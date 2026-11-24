// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include <signal.h>
#include <stdio.h>

#include <sys/wait.h>

#include <iostream>
#include <list>
#include <string>
#include <vector>

#include <medios/medios.hpp>
#include <medios/type_utils.hpp>

#include <process/clock.hpp>
#include <process/defer.hpp>
#include <process/delay.hpp>
#include <process/future.hpp>
#include <process/id.hpp>
#include <process/io.hpp>
#include <process/process.hpp>
#include <process/protobuf.hpp>
#include <process/reap.hpp>
#include <process/subprocess.hpp>
#include <process/time.hpp>
#include <process/timer.hpp>

#include <stout/duration.hpp>
#include <stout/flags.hpp>
#include <stout/json.hpp>
#include <stout/lambda.hpp>
#include <stout/linkedhashmap.hpp>
#include <stout/option.hpp>
#include <stout/os.hpp>
#include <stout/path.hpp>
#include <stout/protobuf.hpp>
#include <stout/stopwatch.hpp>
#include <stout/strings.hpp>

#include <stout/os/environment.hpp>
#include <stout/os/kill.hpp>
#include <stout/os/killtree.hpp>


#include "checks/checker.hpp"
#include "checks/checks_runtime.hpp"
#include "checks/checks_types.hpp"
#include "checks/health_checker.hpp"

#include "common/http.hpp"
#include "common/parse.hpp"
#include "common/protobuf_utils.hpp"
#include "common/status_utils.hpp"

#include "executor/v0_v1executor.hpp"

#include "internal/devolve.hpp"
#include "internal/evolve.hpp"

#include "launcher/finish.hpp"
#include "launcher/observer.hpp"
#include "launcher/progress.hpp"

#ifdef ENABLE_LAUNCHER_SEALING
#include "linux/memfd.hpp"
#endif // ENABLE_LAUNCHER_SEALING

#include "logging/logging.hpp"

#include "messages/messages.hpp"

#include "node/constants.hpp"
#include "node/containerizer/medios/constants.hpp"
#include "node/containerizer/medios/launch.hpp"

using namespace medios::internal::node;

using std::cout;
using std::cerr;
using std::endl;
using std::queue;
using std::string;
using std::vector;

using process::Clock;
using process::Future;
using process::Owned;
using process::Subprocess;
using process::Time;
using process::Timer;

using medios::Environment;

using medios::executor::Call;
using medios::executor::Event;

using medios::node::ContainerLaunchInfo;

using medios::v1::executor::Medios;
using medios::v1::executor::MediosBase;
using medios::v1::executor::V0ToV1Adapter;

namespace medios {
namespace internal {

class CommandExecutor: public ProtobufProcess<CommandExecutor>
{
public:
  CommandExecutor(
      const string& _launcherDir,
      const Option<string>& _rootfs,
      const Option<string>& _sandboxDirectory,
      const Option<string>& _workingDirectory,
      const Option<string>& _user,
      const Option<string>& _taskCommand,
      const Option<Environment>& _taskEnvironment,
      const Option<CapabilityInfo>& _effectiveCapabilities,
      const Option<CapabilityInfo>& _boundingCapabilities,
      const Option<string>& _ttyNodePath,
      const Option<ContainerLaunchInfo>& _taskLaunchInfo,
      const Option<vector<gid_t>> _taskSupplementaryGroups,
      const FrameworkID& _frameworkId,
      const ExecutorID& _executorId,
      const Duration& _shutdownGracePeriod)
    : ProcessBase(process::ID::generate("command-executor")),
      state(DISCONNECTED),
      taskData(None()),
      launched(false),
      killed(false),
      killedByHealthCheck(false),
      killedByMaxCompletionTimer(false),
      killedByProgressStall(false),
      stallReported(false),
      terminated(false),
      terminalForwarded(false),
      pid(None()),
      shutdownGracePeriod(_shutdownGracePeriod),
      frameworkInfo(None()),
      taskId(None()),
      launcherDir(_launcherDir),
      rootfs(_rootfs),
      sandboxDirectory(_sandboxDirectory),
      workingDirectory(_workingDirectory),
      user(_user),
      taskCommand(_taskCommand),
      taskEnvironment(_taskEnvironment),
      effectiveCapabilities(_effectiveCapabilities),
      boundingCapabilities(_boundingCapabilities),
      ttyNodePath(_ttyNodePath),
      taskLaunchInfo(_taskLaunchInfo),
      taskSupplementaryGroups(_taskSupplementaryGroups),
      frameworkId(_frameworkId),
      executorId(_executorId),
      lastTaskStatus(None()) {}

  ~CommandExecutor() override = default;

  void connected()
  {
    state = CONNECTED;

    doReliableRegistration();
  }

  void disconnected()
  {
    state = DISCONNECTED;
  }

  void received(const Event& event)
  {
    LOG(INFO) << "Received " << event.type() << " event";

    switch (event.type()) {
      case Event::SUBSCRIBED: {
        LOG(INFO) << "Subscribed executor on "
                  << event.subscribed().node_info().hostname();

        frameworkInfo = event.subscribed().framework_info();
        state = SUBSCRIBED;
        break;
      }

      case Event::LAUNCH: {
        launch(event.launch().task());
        break;
      }

      case Event::LAUNCH_GROUP: {
        LOG(ERROR) << "LAUNCH_GROUP event is not supported";
        // Shut down because this is unexpected; `LAUNCH_GROUP` event
        // should only ever go to a group-capable default executor and
        // not the command executor.
        shutdown();
        break;
      }

      case Event::KILL: {
        Option<KillPolicy> override = event.kill().has_kill_policy()
          ? Option<KillPolicy>(event.kill().kill_policy())
          : None();

        kill(event.kill().task_id(), override);
        break;
      }

      case Event::ACKNOWLEDGED: {
        const id::UUID uuid =
          id::UUID::fromBytes(event.acknowledged().uuid()).get();

        if (!unacknowledgedUpdates.contains(uuid)) {
          LOG(WARNING) << "Received acknowledgement " << uuid
                       << " for unknown status update";
          return;
        }

        // Terminate if we receive the ACK for the terminal status update.
        // NOTE: The executor receives an ACK iff it uses the HTTP library.
        // No ACK will be received if V0ToV1Adapter is used.
        if (medios::internal::protobuf::isTerminalState(
            unacknowledgedUpdates[uuid].status().state())) {
          terminate(self());
        }

        // Remove the corresponding update.
        unacknowledgedUpdates.erase(uuid);

        // Mark task as acknowledged.
        CHECK(taskData.isSome());
        taskData->acknowledged = true;

        break;
      }

      case Event::SHUTDOWN: {
        shutdown();
        break;
      }

      case Event::MESSAGE: {
        break;
      }

      case Event::ERROR: {
        LOG(ERROR) << "Error: " << event.error().message();
        break;
      }

      case Event::HEARTBEAT: {
        break;
      }

      case Event::UNKNOWN: {
        LOG(WARNING) << "Received an UNKNOWN event and ignored";
        break;
      }
    }
  }

protected:
  void initialize() override
  {
    Option<string> value = os::getenv("MEDIOS_HTTP_COMMAND_EXECUTOR");

    // We initialize the library here to ensure that callbacks are only invoked
    // after the process has spawned.
    if (value.isSome() && value.get() == "1") {
      medios.reset(new Medios(
          ContentType::PROTOBUF,
          defer(self(), &Self::connected),
          defer(self(), &Self::disconnected),
          defer(self(), [this](queue<v1::executor::Event> events) {
            while(!events.empty()) {
              const v1::executor::Event& event = events.front();
              received(devolve(event));

              events.pop();
            }
          })));
    } else {
      medios.reset(new V0ToV1Adapter(
          defer(self(), &Self::connected),
          defer(self(), &Self::disconnected),
          defer(self(), [this](queue<v1::executor::Event> events) {
            while(!events.empty()) {
              const v1::executor::Event& event = events.front();
              received(devolve(event));

              events.pop();
            }
          })));
    }
  }

  void taskCheckUpdated(
      const TaskID& _taskId,
      const CheckStatusInfo& checkStatus)
  {
    CHECK_SOME(taskId);
    CHECK_EQ(taskId.get(), _taskId);

    // This prevents us from sending check updates after a terminal
    // status update, because we may receive an update from a check
    // scheduled before the task has been reaped.
    //
    // TODO(alexr): Consider sending check updates after TASK_KILLING.
    if (killed || terminated) {
      return;
    }

    LOG(INFO) << "Received check update '" << checkStatus << "'";

    // Use the previous task status to preserve all attached information.
    CHECK_SOME(lastTaskStatus);
    TaskStatus status = protobuf::createTaskStatus(
        lastTaskStatus.get(),
        id::UUID::random(),
        Clock::now().secs(),
        None(),
        None(),
        None(),
        TaskStatus::REASON_TASK_CHECK_STATUS_UPDATED,
        None(),
        None(),
        checkStatus);

    forward(status);
  }

  void taskHealthUpdated(const TaskHealthStatus& healthStatus)
  {
    CHECK_SOME(taskId);
    CHECK_EQ(taskId.get(), healthStatus.task_id());

    // This prevents us from sending health updates after a terminal
    // status update, because we may receive an update from a health
    // check scheduled before the task has been reaped.
    //
    // TODO(alexr): Consider sending health updates after TASK_KILLING.
    if (killed || terminated) {
      return;
    }

    LOG(INFO) << "Received task health update, healthy: "
              << stringify(healthStatus.healthy());

    // Use the previous task status to preserve all attached information.
    CHECK_SOME(lastTaskStatus);
    TaskStatus status = protobuf::createTaskStatus(
        lastTaskStatus.get(),
        id::UUID::random(),
        Clock::now().secs(),
        None(),
        None(),
        None(),
        TaskStatus::REASON_TASK_HEALTH_CHECK_STATUS_UPDATED,
        None(),
        healthStatus.healthy());

    forward(status);

    if (healthStatus.kill_task()) {
      killedByHealthCheck = true;
      kill(healthStatus.task_id());
    }
  }

  void doReliableRegistration()
  {
    if (state == SUBSCRIBED || state == DISCONNECTED) {
      return;
    }

    Call call;
    call.set_type(Call::SUBSCRIBE);

    call.mutable_framework_id()->CopyFrom(frameworkId);
    call.mutable_executor_id()->CopyFrom(executorId);

    Call::Subscribe* subscribe = call.mutable_subscribe();

    // Send all unacknowledged updates.
    foreachvalue (const Call::Update& update, unacknowledgedUpdates) {
      subscribe->add_unacknowledged_updates()->MergeFrom(update);
    }

    // Send the unacknowledged task.
    if (taskData.isSome() && !taskData->acknowledged) {
      subscribe->add_unacknowledged_tasks()->MergeFrom(
          taskData->taskInfo);
    }

    medios->send(evolve(call));

    delay(Seconds(1), self(), &Self::doReliableRegistration);
  }

  // Forks a command through `medios-containerizer launch` the way the work
  // command is forked: same rootfs, user, capabilities, mounts and
  // namespaces. The work command inherits this executor's stdout and
  // stderr; a finish hook is given sandbox files instead (DR-042).
  static Try<Subprocess> launchTaskSubprocess(
      const CommandInfo& command,
      const string& launcherDir,
      const Environment& environment,
      const Option<string>& user,
      const Option<string>& rootfs,
      const Option<string>& sandboxDirectory,
      const Option<string>& workingDirectory,
      const Option<CapabilityInfo>& effectiveCapabilities,
      const Option<CapabilityInfo>& boundingCapabilities,
      const Option<string>& ttyNodePath,
      const Option<ContainerLaunchInfo>& taskLaunchInfo,
      const Option<vector<gid_t>>& taskSupplementaryGroups,
      const Subprocess::IO& out,
      const Subprocess::IO& err)
  {
    // Prepare the flags to pass to the launch process.
    node::MediosContainerizerLaunch::Flags launchFlags;

    ContainerLaunchInfo launchInfo;
    launchInfo.mutable_command()->CopyFrom(command);

    if (rootfs.isSome()) {
      // The command executor is responsible for chrooting into the
      // root filesystem and changing the user before exec-ing the
      // user process.
#ifdef __linux__
      if (geteuid() != 0) {
        ABORT("The command executor requires root with rootfs");
      }

      // Ensure that mount namespace of the executor is not affected by
      // changes in its task's namespace induced by calling `pivot_root`
      // as part of the task setup in medios-containerizer binary.
      launchFlags.unshare_namespace_mnt = true;
#else
      ABORT("Not expecting root volume with non-linux platform");
#endif // __linux__

      launchInfo.set_rootfs(rootfs.get());

      CHECK_SOME(sandboxDirectory);

      launchInfo.set_working_directory(workingDirectory.isSome()
        ? workingDirectory.get()
        : sandboxDirectory.get());

      // TODO(jieyu): If the task has a rootfs, the executor itself will
      // be running as root. Its sandbox is owned by root as well. In
      // order for the task to be able to access to its sandbox, we need
      // to make sure the owner of the sandbox is 'user'.
      if (user.isSome()) {
        // NOTE: We only chown the sandbox directory (non-recursively).
        Try<Nothing> chown = os::chown(user.get(), os::getcwd(), false);
        if (chown.isError()) {
          ABORT("Failed to chown sandbox to user " +
                user.get() + ": " + chown.error());
        }
      }
    }

    launchInfo.mutable_environment()->CopyFrom(environment);

    if (user.isSome()) {
      launchInfo.set_user(user.get());
    }

    if (effectiveCapabilities.isSome()) {
      launchInfo.mutable_effective_capabilities()->CopyFrom(
          effectiveCapabilities.get());
    }

    if (boundingCapabilities.isSome()) {
      launchInfo.mutable_bounding_capabilities()->CopyFrom(
          boundingCapabilities.get());
    }

    if (taskLaunchInfo.isSome()) {
      launchInfo.mutable_mounts()->CopyFrom(taskLaunchInfo->mounts());
      launchInfo.mutable_file_operations()->CopyFrom(
          taskLaunchInfo->file_operations());

      launchInfo.mutable_pre_exec_commands()->CopyFrom(
          taskLaunchInfo->pre_exec_commands());

      launchInfo.mutable_clone_namespaces()->CopyFrom(
          taskLaunchInfo->clone_namespaces());
    }

    if (taskSupplementaryGroups.isSome()) {
      foreach (gid_t gid, taskSupplementaryGroups.get()) {
        launchInfo.add_supplementary_groups(gid);
      }
    }

    launchFlags.launch_info = JSON::protobuf(launchInfo);

    // Determine the medios containerizer binary depends on whether we
    // need to clone and seal it on linux.
    string initPath = path::join(launcherDir, MEDIOS_CONTAINERIZER);
#ifdef ENABLE_LAUNCHER_SEALING
    // Clone the launcher binary in memory for security concerns.
    Try<int_fd> memFd = memfd::cloneSealedFile(initPath);
    if (memFd.isError()) {
      ABORT(
          "Failed to clone a sealed file '" + initPath + "' in memory: " +
          memFd.error());
    }

    initPath = "/proc/self/fd/" + stringify(memFd.get());
#endif // ENABLE_LAUNCHER_SEALING

    // Fork the child using launcher.
    vector<string> argv(2);
    argv[0] = MEDIOS_CONTAINERIZER;
    argv[1] = MediosContainerizerLaunch::NAME;

    vector<process::Subprocess::ParentHook> parentHooks;

    vector<process::Subprocess::ChildHook> childHooks;
    if (ttyNodePath.isNone()) {
      childHooks.emplace_back(Subprocess::ChildHook::SETSID());
    }

    return subprocess(
        initPath,
        argv,
        Subprocess::FD(STDIN_FILENO),
        out,
        err,
        &launchFlags,
        None(),
        None(),
        parentHooks,
        childHooks);
  }


  // An observer runs through the same launcher the task does, so it reaches
  // the task's uid the way the task reaches it and through no second
  // mechanism. It is given no rootfs and no task environment:
  // `medios-containerizer launch` inherits this executor's environment when
  // the launch info names none, which is what an observer is given
  // (DR-041).
  static Try<Subprocess> launchObserverSubprocess(
      const TaskObserver& observer,
      const string& stderrPath,
      const string& launcherDir,
      const Option<string>& user,
      const string& sandboxDirectory)
  {
    MediosContainerizerLaunch::Flags launchFlags;

    ContainerLaunchInfo launchInfo;
    launchInfo.mutable_command()->CopyFrom(observer.command());
    launchInfo.set_working_directory(sandboxDirectory);

    if (user.isSome()) {
      launchInfo.set_user(user.get());
    }

    launchFlags.launch_info = JSON::protobuf(launchInfo);

    vector<string> argv(2);
    argv[0] = MEDIOS_CONTAINERIZER;
    argv[1] = MediosContainerizerLaunch::NAME;

    // An observer reports by writing, so it is given no stdin. Its stderr
    // is a sandbox file rather than the executor's, which would land in the
    // task's own `stderr` and read as the task's.
    return subprocess(
        path::join(launcherDir, MEDIOS_CONTAINERIZER),
        argv,
        Subprocess::PATH(os::DEV_NULL),
        Subprocess::PIPE(),
        Subprocess::PATH(stderrPath),
        &launchFlags,
        None(),
        None(),
        {},
        {Subprocess::ChildHook::SETSID()});
  }

  void launch(const TaskInfo& task)
  {
    CHECK_EQ(SUBSCRIBED, state);

    if (launched) {
      TaskStatus status = createTaskStatus(
          task.task_id(),
          TASK_FAILED,
          None(),
          "Attempted to run multiple tasks using a \"command\" executor");

      forward(status);
      return;
    }

    // Capture `TaskInfo` and `TaskID` of the task.
    CHECK(taskData.isNone());
    taskData = TaskData(task);
    taskId = task.task_id();

    // Read the progress policy before anything else runs, so a task that
    // names an unusable setting is refused rather than run with progress
    // silently off.
    Try<Option<progress::ProgressPolicy>> policy =
      progress::progressPolicyFrom(task);

    if (policy.isError()) {
      TaskStatus status = createTaskStatus(
          task.task_id(),
          TASK_ERROR,
          TaskStatus::REASON_TASK_INVALID,
          "Invalid progress settings: " + policy.error());

      forward(status);
      return;
    }

    progressPolicy = policy.get();

    if (progressPolicy.isSome()) {
      // The executor's working directory is the task's sandbox, which is
      // also what the launch path below chowns when a rootfs is in play.
      Try<Owned<progress::ProgressReader>> reader =
        progress::ProgressReader::create(progressPolicy.get(), os::getcwd());

      if (reader.isError()) {
        TaskStatus status = createTaskStatus(
            task.task_id(),
            TASK_ERROR,
            TaskStatus::REASON_TASK_INVALID,
            "Invalid progress settings: " + reader.error());

        forward(status);
        return;
      }

      progressReader = reader.get();
    }

    // Send initial TASK_STARTING update.
    TaskStatus starting = createTaskStatus(taskId.get(), TASK_STARTING);
    forward(starting);

    // Capture the kill policy.
    if (task.has_kill_policy()) {
      killPolicy = task.kill_policy();
    }

    // Determine the command to launch the task.
    CommandInfo command;

    if (taskCommand.isSome()) {
      // Get CommandInfo from a JSON string.
      Try<JSON::Object> object = JSON::parse<JSON::Object>(taskCommand.get());
      if (object.isError()) {
        ABORT("Failed to parse JSON: " + object.error());
      }

      Try<CommandInfo> parse = ::protobuf::parse<CommandInfo>(object.get());
      if (parse.isError()) {
        ABORT("Failed to parse protobuf: " + parse.error());
      }

      command = parse.get();
    } else if (task.has_command()) {
      command = task.command();
    } else {
      LOG(FATAL) << "Expecting task '" << taskId.get() << "' to have a command";
    }

    // TODO(jieyu): For now, we just fail the executor if the task's
    // CommandInfo is not valid. The framework will receive
    // TASK_FAILED for the task, and will most likely find out the
    // cause with some debugging. This is a temporary solution. A more
    // correct solution is to perform this validation at controller side.
    if (command.shell()) {
      CHECK(command.has_value())
        << "Shell command of task '" << taskId.get() << "' is not specified";
    } else {
      CHECK(command.has_value())
        << "Executable of task '" << taskId.get() << "' is not specified";
    }

    // Determine the environment for the command to be launched.
    // The priority of the environment should be:
    //  1) User specified environment in CommandInfo.
    //  2) Environment returned by isolators (`taskEnvironment`).
    //  3) Environment passed from node (`os::environment`).
    //
    // ever uses the `--task_environment` flag.
    //
    // TODO(tillt): Consider logging in detail the original environment
    // variable source and overwriting source.
    //
    // TODO(tillt): Consider implementing a generic, reusable solution
    // for merging these environments. See MESOS-7299.
    //
    // Note that we can not use protobuf message merging as that could
    // cause duplicate keys in the resulting environment.
    hashmap<string, Environment::Variable> environment;

    foreachpair (const string& name, const string& value, os::environment()) {
      // Do not propagate the executor's TLS private key path or password into
      // the user task's environment (#636).
      if (name == "LIBPROCESS_SSL_KEY_FILE" ||
          name == "LIBPROCESS_SSL_KEY_PASSWORD" ||
          name == "SSL_KEY_FILE" ||
          name == "SSL_KEY_PASSWORD") {
        continue;
      }

      Environment::Variable variable;
      variable.set_name(name);
      variable.set_type(Environment::Variable::VALUE);
      variable.set_value(value);
      environment[name] = variable;
    }

    if (taskEnvironment.isSome()) {
      foreach (const Environment::Variable& variable,
               taskEnvironment->variables()) {
        // Skip overwriting if the variable is unresolved secret.
        if (variable.type() == Environment::Variable::SECRET) {
          continue;
        }
        const string& name = variable.name();
        if (environment.contains(name) &&
            environment[name].value() != variable.value()) {
          LOG(INFO) << "Overwriting environment variable '" << name << "'";
        }
        environment[name] = variable;
      }
    }

    if (command.has_environment()) {
      foreach (const Environment::Variable& variable,
               command.environment().variables()) {
        // Skip overwriting if the variable is unresolved secret.
        if (variable.type() == Environment::Variable::SECRET) {
          continue;
        }
        const string& name = variable.name();
        if (environment.contains(name) &&
            environment[name].value() != variable.value()) {
          LOG(INFO) << "Overwriting environment variable '" << name << "'";
        }
        environment[name] = variable;
      }
    }

    // Set the `MEDIOS_ALLOCATION_ROLE` environment variable for the task.
    // Please note that tasks are not allowed to mix resources allocated
    // to different roles, see MESOS-6636.
    Environment::Variable variable;
    variable.set_name("MEDIOS_ALLOCATION_ROLE");
    variable.set_type(Environment::Variable::VALUE);
    variable.set_value(task.resources().begin()->allocation_info().role());
    environment["MEDIOS_ALLOCATION_ROLE"] = variable;

    Environment launchEnvironment;
    foreachvalue (const Environment::Variable& variable, environment) {
      launchEnvironment.add_variables()->CopyFrom(variable);
    }

    // The finish hook runs after the work command exits, inside the task's
    // ceiling; the ceiling's start is kept so what it has left can be
    // read when the work exits (DR-042).
    if (task.has_finish()) {
      finish = task.finish();
    }

    // Setup timer for max_completion_time.
    if (task.max_completion_time().nanoseconds() > 0) {
      Duration duration = Nanoseconds(task.max_completion_time().nanoseconds());

      LOG(INFO) << "Task " << taskId.get() << " has a max completion time of "
                << duration;

      ceiling = duration;
      ceilingStart = Clock::now();

      taskCompletionTimer = delay(
          duration,
          self(),
          &Self::taskCompletionTimeout,
          task.task_id(),
          duration);
    }

    LOG(INFO) << "Starting task " << taskId.get();

    Try<Subprocess> forked = launchTaskSubprocess(
        command,
        launcherDir,
        launchEnvironment,
        user,
        rootfs,
        sandboxDirectory,
        workingDirectory,
        effectiveCapabilities,
        boundingCapabilities,
        ttyNodePath,
        taskLaunchInfo,
        taskSupplementaryGroups,
        Subprocess::FD(STDOUT_FILENO),
        Subprocess::FD(STDERR_FILENO));

    if (forked.isError()) {
      ABORT("Failed to launch task subprocess: " + forked.error());
    }

    Subprocess subprocess = forked.get();

    pid = subprocess.pid();

    LOG(INFO) << "Forked command at " << pid.get();

    if (task.has_check()) {
      vector<string> namespaces;
      if (rootfs.isSome() &&
          task.check().type() == CheckInfo::COMMAND) {
        // Make sure command checks are run from the task's mount namespace.
        // Otherwise if rootfs is specified the command binary may not be
        // available in the executor.
        //
        // NOTE: The command executor shares the network namespace
        // with its task, hence no need to enter it explicitly.
        namespaces.push_back("mnt");
      }

      const checks::runtime::Plain plainRuntime{namespaces, pid.get()};
      Try<Owned<checks::Checker>> _checker =
        checks::Checker::create(
            task.check(),
            launcherDir,
            defer(self(), &Self::taskCheckUpdated, taskId.get(), lambda::_1),
            taskId.get(),
            plainRuntime);

      if (_checker.isError()) {
        // TODO(alexr): Consider ABORT and return a TASK_FAILED here.
        LOG(ERROR) << "Failed to create checker: " << _checker.error();
      } else {
        checker = _checker.get();
      }
    }

    if (task.has_health_check()) {
      vector<string> namespaces;
      if (rootfs.isSome() &&
          task.health_check().type() == HealthCheck::COMMAND) {
        // Make sure command health checks are run from the task's mount
        // namespace. Otherwise if rootfs is specified the command binary
        // may not be available in the executor.
        //
        // NOTE: The command executor shares the network namespace
        // with its task, hence no need to enter it explicitly.
        namespaces.push_back("mnt");
      }

      const checks::runtime::Plain plainRuntime{namespaces, pid.get()};
      Try<Owned<checks::HealthChecker>> _healthChecker =
        checks::HealthChecker::create(
            task.health_check(),
            launcherDir,
            defer(self(), &Self::taskHealthUpdated, lambda::_1),
            taskId.get(),
            plainRuntime);

      if (_healthChecker.isError()) {
        // TODO(gilbert): Consider ABORT and return a TASK_FAILED here.
        LOG(ERROR) << "Failed to create health checker: "
                   << _healthChecker.error();
      } else {
        healthChecker = _healthChecker.get();
      }
    }

    if (!task.observers().empty()) {
      // The task's process exists now. An observer is started after it and
      // the task never waits for one: an observer that does not start is a
      // line in the record and nothing else (DR-041).
      observers = observer::ObserverSet::create(task.observers(), os::getcwd());

      const string launcher = launcherDir;
      const Option<string> taskUser = user;
      const string sandbox = os::getcwd();

      observers->launch(
          [launcher, taskUser, sandbox](
              const TaskObserver& declaration,
              const string& stderrPath) {
            return launchObserverSubprocess(
                declaration, stderrPath, launcher, taskUser, sandbox);
          });

      LOG(INFO) << "Started " << observers->running().size() << " of "
                << task.observers().size() << " observers of task "
                << taskId.get();

      observerDrainTimer = delay(
          observer::DEFAULT_DRAIN_INTERVAL, self(), &Self::drainObservers);
    }

    // Monitor this process.
    subprocess.status()
      .onAny(defer(self(), &Self::reaped, pid.get(), lambda::_1));

    if (progressReader.get() != nullptr) {
      CHECK_SOME(progressPolicy);

      LOG(INFO) << "Reading task progress every "
                << progressPolicy->sampleInterval
                << (progressPolicy->stallWindow.isSome()
                      ? ", stall window " +
                          stringify(progressPolicy->stallWindow.get())
                      : ", no stall window");

      // The task is running now. Everything before this point, the fetch
      // and the fork, is not the task failing to advance.
      progressReader->arm();

      progressSampleTimer = delay(
          progressPolicy->sampleInterval, self(), &Self::sampleProgress);
    }

    TaskStatus status = createTaskStatus(taskId.get(), TASK_RUNNING);

    forward(status);
    launched = true;
  }

  // The grace period between SIGTERM and SIGKILL for the work command, its
  // observers and its finish hook alike: the task's kill policy, with the
  // executor's 3 s default when the task names none. One helper, so the
  // three cannot drift apart.
  Duration killPolicyGracePeriod() const
  {
    if (killPolicy.isSome() && killPolicy->has_grace_period()) {
      return Nanoseconds(killPolicy->grace_period().nanoseconds());
    }

    return Seconds(3);
  }


  // Starts the observers' escalation: SIGTERM now, SIGKILL when the grace
  // period is up. The timer is the task's deadline and not this executor's
  // exit, which a withheld acknowledgement can defer for a minute.
  void terminateObservers(const Duration& gracePeriod)
  {
    if (observers.get() == nullptr) {
      return;
    }

    const Time deadline = Clock::now() + gracePeriod;

    // A second kill may shorten the task's deadline and never lengthen it,
    // and the observers are held to the deadline the task is held to. A
    // deadline no earlier than the one already armed is a no-op, which is
    // what a retried kill must be.
    if (observersSignalled) {
      if (observerKillTimer.isNone() ||
          observerKillTimer->timeout().time() <= deadline) {
        return;
      }

      LOG(INFO) << "Rescheduling the observers' SIGKILL to " << gracePeriod
                << " from now";

      Clock::cancel(observerKillTimer.get());
      observerKillTimer = delay(gracePeriod, self(), &Self::killObservers);
      return;
    }

    observersSignalled = true;

    if (observerDrainTimer.isSome()) {
      Clock::cancel(observerDrainTimer.get());
      observerDrainTimer = None();
    }

    // Whatever the observers managed to say before this.
    observers->drain();
    observers->signalTerm();

    LOG(INFO) << "Sent SIGTERM to the observers of task "
              << (taskId.isSome() ? taskId->value() : "")
              << ", escalating to SIGKILL in " << gracePeriod;

    observerKillTimer = delay(gracePeriod, self(), &Self::killObservers);
  }


  void killObservers()
  {
    // Cancelled before the handle goes, so a timer this did not come from
    // cannot fire later with nothing left to cancel it.
    if (observerKillTimer.isSome()) {
      Clock::cancel(observerKillTimer.get());
      observerKillTimer = None();
    }

    if (observers.get() != nullptr) {
      observers->signalKill();
    }
  }


  // Reads the observers' pipes and sends a progress-only `TASK_RUNNING`
  // update when one of them has said something new. An observation is a
  // headline, so a line that arrives between drains is overtaken by the
  // next one rather than queued.
  void drainObservers()
  {
    observerDrainTimer = None();

    if (terminated || observers.get() == nullptr) {
      return;
    }

    observers->drain();

    if (launched && !terminalForwarded && observers->hasUnreported()) {
      CHECK_SOME(taskId);
      forward(createTaskStatus(taskId.get(), TASK_RUNNING));
    }

    observerDrainTimer = delay(
        observer::DEFAULT_DRAIN_INTERVAL, self(), &Self::drainObservers);
  }


  // Reads the progress source, decides the stall, and sends a
  // progress-only `TASK_RUNNING` update at the report cadence. Runs on this
  // process's event queue, which is what keeps a progress report from
  // overtaking a lifecycle update.
  void sampleProgress()
  {
    progressSampleTimer = None();

    if (terminated) {
      return;
    }

    CHECK_SOME(progressPolicy);
    CHECK_SOME(taskId);
    CHECK_NOTNULL(progressReader.get());

    Try<Nothing> sample = progressReader->sample();

    // An observation failure is the executor's, not the task's: it is
    // logged and sampling continues.
    if (sample.isError()) {
      LOG(WARNING) << "Failed to observe progress of task " << taskId.get()
                   << ": " << sample.error();
    }

    // Nothing is reported once a kill is under way, for the reason check
    // and health updates are not: it would move a killing task back to
    // `TASK_RUNNING`.
    if (killed) {
      return;
    }

    if (progressReader->stalled()) {
      stallDuration = progressReader->sinceAdvance();
      stallLastValue = progressReader->fraction();

      const string explanation =
        "REASON_PROGRESS_STALLED: no progress for " +
        stringify(stallDuration) + "; last reported fraction " +
        (stallLastValue.isSome() ? stringify(stallLastValue.get()) : "none");

      if (progressPolicy->action == progress::ProgressPolicy::KILL) {
        LOG(INFO) << "Killing task " << taskId.get() << ": " << explanation;

        killedByProgressStall = true;

        // The kill-policy escalation: SIGTERM, the policy's grace period,
        // then SIGKILL. The terminal status comes from `reaped`.
        kill(taskId.get());
        return;
      }

      if (!stallReported) {
        stallReported = true;

        LOG(INFO) << "Reporting a stall of task " << taskId.get() << ": "
                  << explanation;

        TaskStatus status = createTaskStatus(
            taskId.get(), TASK_RUNNING, None(), explanation);

        forward(status);
        lastProgressReport = Clock::now();
      }
    } else {
      // The task advanced, so the next stall is a new episode.
      stallReported = false;

      if (progressReader->hasReport() &&
          (lastProgressReport.isNone() ||
           Clock::now() - lastProgressReport.get() >=
             progress::REPORT_CADENCE)) {
        forward(createTaskStatus(taskId.get(), TASK_RUNNING));
        lastProgressReport = Clock::now();
      }
    }

    progressSampleTimer = delay(
        progressPolicy->sampleInterval, self(), &Self::sampleProgress);
  }

  void kill(const TaskID& _taskId, const Option<KillPolicy>& override = None())
  {
    // Cancel the taskCompletionTimer if it is set and ongoing.
    if (taskCompletionTimer.isSome()) {
      Clock::cancel(taskCompletionTimer.get());
      taskCompletionTimer = None();
    }

    // The task's kill-policy grace, as the observers and the finish hook
    // get it; a kill policy provided in the `Kill` event takes precedence
    // over the one the task was launched with.
    Duration gracePeriod = killPolicyGracePeriod();

    if (override.isSome() && override->has_grace_period()) {
      gracePeriod = Nanoseconds(override->grace_period().nanoseconds());
    }

    LOG(INFO) << "Received kill for task " << _taskId.value()
              << " with grace period of " << gracePeriod;

    kill(_taskId, gracePeriod);
  }

  void shutdown()
  {
    LOG(INFO) << "Shutting down";

    // A finish hook that has not started by now does not start: the node
    // destroys the container after the shutdown grace period (DR-042).
    shuttingDown = true;

    // NOTE: We leave a small buffer of time to do the forced kill, otherwise
    // the node may destroy the container before we can send `TASK_KILLED`.
    //
    // TODO(alexr): Remove `MAX_REAP_INTERVAL` once the reaper signals
    // immediately after the watched process has exited.
    Duration gracePeriod =
      shutdownGracePeriod - process::MAX_REAP_INTERVAL() - Seconds(1);

    // Since the command executor manages a single task,
    // shutdown boils down to killing this task.
    //
    // TODO(bmahler): If a shutdown arrives after a kill task within
    // the grace period of the `KillPolicy`, we may need to escalate
    // more quickly (e.g. the shutdown grace period allotted by the
    // node is smaller than the kill grace period).
    if (launched) {
      CHECK_SOME(taskId);
      kill(taskId.get(), gracePeriod);
    } else {
      terminate(self());
    }
  }

private:
  void kill(const TaskID& _taskId, const Duration& gracePeriod)
  {
    // A kill while the finish hook runs is applied to the hook, by the
    // same escalation the work command gets; the work's state is already
    // settled (DR-042).
    if (finishing) {
      finishKilled = true;
      terminateFinish(gracePeriod);
      return;
    }

    if (terminated) {
      return;
    }

    // Terminate if a kill task request is received before the task is launched.
    // This can happen, for example, if `RunTaskMessage` has not been delivered.
    // See MESOS-8297.
    CHECK(launched) << "Terminating because kill task message has been"
                    << " received before the task has been launched";

    // If the task is being killed but has not terminated yet and
    // we receive another kill request. Check if we need to adjust
    // the remaining grace period.
    if (killed && !terminated) {
      // When a kill request arrives on the executor, we cannot simply
      // restart the escalation timer, because the scheduler may retry
      // and this must be a no-op.
      //
      // The escalation grace period can be only decreased. We disallow
      // increasing the total grace period for the terminating task in
      // order to avoid possible confusion when a subsequent kill overrides
      // the previous one and gives the task _more_ time to clean up. Other
      // systems may not allow this.
      //
      // Here are some examples to illustrate:
      //
      // 20, 30 -> Increased grace period is a no-op, grace period remains 20.
      // 20, 20 -> Retries are a no-op, grace period remains 20.
      // 20, 5  -> if `elapsed` >= 5:
      //             SIGKILL immediately, total grace period is `elapsed`.
      //           if `elapsed` < 5:
      //             SIGKILL in (5 - `elapsed`), total grace period is 5.

      CHECK_SOME(killGracePeriodStart);
      CHECK_SOME(killGracePeriodTimer);

      if (killGracePeriodStart.get() + gracePeriod >
          killGracePeriodTimer->timeout().time()) {
        return;
      }

      Duration elapsed = Clock::now() - killGracePeriodStart.get();
      Duration remaining = gracePeriod > elapsed
        ? gracePeriod - elapsed
        : Duration::zero();

      LOG(INFO) << "Rescheduling escalation to SIGKILL in " << remaining
                << " from now";

      Clock::cancel(killGracePeriodTimer.get());
      killGracePeriodTimer = delay(
          remaining, self(), &Self::escalated, gracePeriod);
    }

    // Issue the kill signal if the task has been launched
    // and this is the first time we've received the kill.
    if (launched && !killed) {
      // Send TASK_KILLING if the framework can handle it.
      CHECK_SOME(frameworkInfo);
      CHECK_SOME(taskId);
      CHECK(taskId.get() == _taskId);

      if (!killedByMaxCompletionTimer &&
          protobuf::frameworkHasCapability(
              frameworkInfo.get(),
              FrameworkInfo::Capability::TASK_KILLING_STATE)) {
        TaskStatus status =
          createTaskStatus(taskId.get(), TASK_KILLING);

        forward(status);
      }

      // Stop checking the task.
      if (checker.get() != nullptr) {
        checker->pause();
      }

      // Stop health checking the task.
      if (healthChecker.get() != nullptr) {
        healthChecker->pause();
      }

      // An observer is killed with the task it observes and by the same
      // escalation: SIGTERM here, SIGKILL when this kill's grace period is
      // up (DR-041).
      terminateObservers(gracePeriod);

      // Now perform signal escalation to begin killing the task.
      CHECK_SOME(pid);

      LOG(INFO) << "Sending SIGTERM to process tree at pid " << pid.get();

      Stopwatch stopwatch;
      stopwatch.start();

      Try<std::list<os::ProcessTree>> trees =
        os::killtree(pid.get(), SIGTERM, true, true);

      if (trees.isError()) {
        LOG(ERROR) << "Failed to kill the process tree rooted at pid "
                   << pid.get() << ": " << trees.error();

        // Send SIGTERM directly to process 'pid' as it may not have
        // received signal before os::killtree() failed.
        os::kill(pid.get(), SIGTERM);
      } else {
        LOG(INFO) << "Sent SIGTERM to process tree at pid " << pid.get()
                  << " in " << stopwatch.elapsed();
        VLOG(1) << "Sent SIGTERM to the following process trees:\n"
                << stringify(trees.get());
      }

      LOG(INFO) << "Scheduling escalation to SIGKILL in " << gracePeriod
                << " from now";

      killGracePeriodTimer =
        delay(gracePeriod, self(), &Self::escalated, gracePeriod);

      killGracePeriodStart = Clock::now();
      killed = true;
    }
  }

  void reaped(pid_t _pid, const Future<Option<int>>& status_)
  {
    terminated = true;

    // Stop checking the task.
    if (checker.get() != nullptr) {
      checker->pause();
    }

    // Stop health checking the task.
    if (healthChecker.get() != nullptr) {
      healthChecker->pause();
    }

    // The task is gone, so an observer of it is observing nothing. It gets
    // the task's own grace period to go, then SIGKILL, on a timer that
    // does not depend on when this executor exits.
    terminateObservers(killPolicyGracePeriod());

    TaskState taskState;
    string message;

    if (killGracePeriodTimer.isSome()) {
      Clock::cancel(killGracePeriodTimer.get());
    }

    if (taskCompletionTimer.isSome()) {
      Clock::cancel(taskCompletionTimer.get());
      taskCompletionTimer = None();
    }

    if (progressSampleTimer.isSome()) {
      Clock::cancel(progressSampleTimer.get());
      progressSampleTimer = None();
    }

    // One last observation, so the terminal status says how far the task
    // got rather than how far it had got one sample interval ago.
    if (progressReader.get() != nullptr) {
      Try<Nothing> sample = progressReader->sample();
      if (sample.isError()) {
        LOG(WARNING) << "Failed to observe progress: " << sample.error();
      }
    }

    Option<TaskStatus::Reason> reason = None();

    if (!status_.isReady()) {
      taskState = TASK_FAILED;
      message =
        "Failed to get exit status for Command: " +
        (status_.isFailed() ? status_.failure() : "future discarded");
    } else if (status_->isNone()) {
      taskState = TASK_FAILED;
      message = "Failed to get exit status for Command";
    } else {
      int status = status_->get();
      CHECK(WIFEXITED(status) || WIFSIGNALED(status))
        << "Unexpected wait status " << status;

      if (killedByProgressStall) {
        taskState = TASK_FAILED;
        reason = TaskStatus::REASON_PROGRESS_STALLED;
      } else if (killedByMaxCompletionTimer) {
        taskState = TASK_FAILED;
        reason = TaskStatus::REASON_MAX_COMPLETION_TIME_REACHED;
      } else if (killed) {
        // Send TASK_KILLED if the task was killed as a result of
        // kill() or shutdown().
        taskState = TASK_KILLED;
      } else if (WSUCCEEDED(status)) {
        taskState = TASK_FINISHED;
      } else {
        taskState = TASK_FAILED;
      }

      message = "Command " + WSTRINGIFY(status);
    }

    if (killedByProgressStall) {
      message =
        "REASON_PROGRESS_STALLED: no progress for " +
        stringify(stallDuration) + "; last reported fraction " +
        (stallLastValue.isSome() ? stringify(stallLastValue.get()) : "none") +
        "; " + message;
    }

    LOG(INFO) << message << " (pid: " << _pid << ")";

    CHECK_SOME(taskId);

    TaskStatus status = createTaskStatus(
        taskId.get(),
        taskState,
        reason,
        message);

    // Indicate that a kill occurred due to a failing health check.
    if (killed && killedByHealthCheck) {
      // TODO(abudnik): Consider specifying appropriate status update reason,
      // saying that the task was killed due to a failing health check.
      status.set_healthy(false);
    }

    // The work's state, reason and message are settled here, before the
    // finish hook runs, so nothing the hook does can change them; the
    // hook's outcome is added to this status when it is known (DR-042).
    if (finish.isSome()) {
      startFinish(status, status_);
      return;
    }

    forwardTerminal(status);
  }


  // Sends the terminal status and schedules the executor's own exit.
  void forwardTerminal(const TaskStatus& status)
  {
    forward(status);

    Option<string> value = os::getenv("MEDIOS_HTTP_COMMAND_EXECUTOR");
    if (value.isSome() && value.get() == "1") {
      // For HTTP based executor, this is a fail safe in case the node
      // doesn't send an ACK for the terminal update for some reason.
      delay(Seconds(60), self(), &Self::selfTerminate);
    } else {
      // For adapter based executor, this is a hack to ensure the status
      // update is sent to the node before we exit the process. Without
      // this we may exit before libprocess has sent the data over the
      // socket. See MESOS-4111 for more details.
      delay(Seconds(1), self(), &Self::selfTerminate);
    }
  }


  // Starts the finish hook after the work command was reaped, or records
  // why it does not start. `terminal` is the work's status, complete; the
  // hook's outcome is added to it before it is sent.
  void startFinish(
      const TaskStatus& terminal,
      const Future<Option<int>>& workStatus)
  {
    CHECK_SOME(finish);
    CHECK_SOME(taskId);

    pendingTerminal = terminal;

    const Duration cap = Nanoseconds(finish->cap().nanoseconds());

    // The remaining ceiling is measured once, here, and the limit timer is
    // armed against this instant after the fork, so the fork's own latency
    // is inside the bound rather than added past it.
    const Time measuredAt = Clock::now();

    Option<Duration> ceilingRemaining = None();
    if (ceiling.isSome()) {
      CHECK_SOME(ceilingStart);
      ceilingRemaining = ceiling.get() - (measuredAt - ceilingStart.get());
    }

    Option<string> reason = None();

    if (killedByMaxCompletionTimer) {
      reason = "the work command was killed at the task's max_completion_time"
               " of " + stringify(ceiling.get()) + ", so nothing of the"
               " task's ceiling is left for the finish";
    } else if (shuttingDown) {
      reason = "the executor is shutting down";
    } else if (!workStatus.isReady() || workStatus->isNone()) {
      reason = "the work command's exit status is unknown";
    }

    // The grace period is taken out of a ceiling-bound limit so the
    // SIGKILL that follows SIGTERM lands no later than the deadline.
    Option<Duration> limit = None();
    if (reason.isNone()) {
      limit = finish::limitFor(cap, ceilingRemaining, killPolicyGracePeriod());
      if (limit.isNone()) {
        reason = "ceiling spent: the task's max_completion_time of " +
                 stringify(ceiling.get()) + " has nothing left for the"
                 " finish once the kill grace period of " +
                 stringify(killPolicyGracePeriod()) + " is taken from it";
      }
    }

    if (reason.isSome()) {
      LOG(INFO) << "Not starting the finish of task " << taskId.get() << ": "
                << reason.get();
      completeFinish(finish::didNotStart(reason.get()));
      return;
    }

    // The executor's own environment, as the work command is given it
    // (the node's, with the ids), plus what the hook is told about the
    // work command.
    Environment executorEnvironment;
    foreachpair (const string& name, const string& value, os::environment()) {
      if (name == "LIBPROCESS_SSL_KEY_FILE" ||
          name == "LIBPROCESS_SSL_KEY_PASSWORD" ||
          name == "SSL_KEY_FILE" ||
          name == "SSL_KEY_PASSWORD") {
        continue;
      }

      Environment::Variable* variable = executorEnvironment.add_variables();
      variable->set_name(name);
      variable->set_type(Environment::Variable::VALUE);
      variable->set_value(value);
    }

    // The node sets MEDIOS_NODE_ID for every executor it starts; identity
    // is never defaulted, so its absence is a refusal with its name.
    const Option<string> nodeId = os::getenv("MEDIOS_NODE_ID");
    if (nodeId.isNone()) {
      completeFinish(finish::didNotStart(
          "MEDIOS_NODE_ID is not set in the executor's environment, so the"
          " finish cannot be told its node"));
      return;
    }

    const Environment environment = finish::hookEnvironment(
        executorEnvironment,
        finish->command().environment(),
        workStatus->get(),
        taskId.get(),
        frameworkId,
        nodeId.get());

    // The hook's output is the sandbox's, beside the work command's.
    const string stdoutPath = path::join(os::getcwd(), finish::STDOUT_FILE);
    const string stderrPath = path::join(os::getcwd(), finish::STDERR_FILE);

    // The hook's own mount namespace is rebuilt from the task's mounts,
    // clone namespaces and file operations (symlinks into filesystems
    // that namespace mounts fresh); an isolator's pre-exec commands are
    // its preparation of the work command and are not run again
    // (DR-042).
    Option<ContainerLaunchInfo> hookLaunchInfo = taskLaunchInfo;
    if (hookLaunchInfo.isSome()) {
      hookLaunchInfo->clear_pre_exec_commands();
    }

    Try<Subprocess> forked = launchTaskSubprocess(
        finish->command(),
        launcherDir,
        environment,
        user,
        rootfs,
        sandboxDirectory,
        workingDirectory,
        effectiveCapabilities,
        boundingCapabilities,
        None(),
        hookLaunchInfo,
        taskSupplementaryGroups,
        Subprocess::PATH(stdoutPath),
        Subprocess::PATH(stderrPath));

    if (forked.isError()) {
      LOG(WARNING) << "Failed to start the finish of task " << taskId.get()
                   << ": " << forked.error();
      completeFinish(finish::didNotStart(
          "the finish command could not be forked: " + forked.error()));
      return;
    }

    finishing = true;
    finishPid = forked->pid();
    finishLimit = limit.get();
    finishStart = Clock::now();

    LOG(INFO) << "Started the finish of task " << taskId.get() << " at pid "
              << finishPid.get() << " with " << limit.get() << " to run";

    const Duration sinceMeasured = Clock::now() - measuredAt;
    finishLimitTimer = delay(
        limit.get() > sinceMeasured ? limit.get() - sinceMeasured
                                    : Duration::zero(),
        self(),
        &Self::finishTimedOut);

    forked->status()
      .onAny(defer(self(), &Self::finishReaped, lambda::_1));
  }


  // The hook reached its limit: SIGTERM now, SIGKILL after the task's
  // kill-policy grace period, the same escalation the work command gets.
  void finishTimedOut()
  {
    finishLimitTimer = None();

    if (!finishing || finishSignalled) {
      return;
    }

    LOG(INFO) << "The finish of task " << taskId.get()
              << " did not end within " << finishLimit.get();

    // The task's own grace period, as the work command and the observers
    // get it.
    finishTimedOutAtLimit = true;
    terminateFinish(killPolicyGracePeriod());
  }


  void terminateFinish(const Duration& gracePeriod)
  {
    CHECK(finishing);
    CHECK_SOME(finishPid);

    if (finishSignalled) {
      return;
    }

    finishSignalled = true;

    LOG(INFO) << "Sending SIGTERM to the finish of task " << taskId.get()
              << " at pid " << finishPid.get() << ", escalating to SIGKILL in "
              << gracePeriod;

    Try<std::list<os::ProcessTree>> trees =
      os::killtree(finishPid.get(), SIGTERM, true, true);

    if (trees.isError()) {
      os::kill(finishPid.get(), SIGTERM);
    }

    finishKillTimer = delay(gracePeriod, self(), &Self::killFinish);
  }


  void killFinish()
  {
    finishKillTimer = None();

    if (!finishing) {
      return;
    }

    CHECK_SOME(finishPid);

    LOG(INFO) << "Sending SIGKILL to the finish of task " << taskId.get()
              << " at pid " << finishPid.get();

    Try<std::list<os::ProcessTree>> trees =
      os::killtree(finishPid.get(), SIGKILL, true, true);

    if (trees.isError()) {
      os::kill(finishPid.get(), SIGKILL);
    }
  }


  void finishReaped(const Future<Option<int>>& status)
  {
    CHECK(finishing);
    CHECK_SOME(finishLimit);
    CHECK_SOME(finishStart);

    finishing = false;

    if (finishLimitTimer.isSome()) {
      Clock::cancel(finishLimitTimer.get());
      finishLimitTimer = None();
    }

    if (finishKillTimer.isSome()) {
      Clock::cancel(finishKillTimer.get());
      finishKillTimer = None();
    }

    const Duration duration = Clock::now() - finishStart.get();

    FinishStatus outcome;
    if (finishTimedOutAtLimit) {
      outcome = finish::timedOut(finishLimit.get(), duration);
    } else if (finishKilled) {
      outcome = finish::killed(finishLimit.get(), duration);
    } else if (!status.isReady() || status->isNone()) {
      // It ran; what failed is reading how it ended.
      outcome = finish::lost(
          "the finish command's exit status could not be read: " +
          (status.isFailed() ? status.failure() : "no status"),
          finishLimit.get(),
          duration);
    } else {
      outcome = finish::ran(status->get(), finishLimit.get(), duration);
    }

    LOG(INFO) << "The finish of task " << taskId.get() << " ended as "
              << FinishStatus::State_Name(outcome.state()) << " after "
              << duration;

    completeFinish(outcome);
  }


  // Attaches the hook's outcome to the work's terminal status and sends it.
  void completeFinish(const FinishStatus& outcome)
  {
    CHECK_SOME(pendingTerminal);

    TaskStatus status = pendingTerminal.get();
    status.mutable_finish()->CopyFrom(outcome);
    pendingTerminal = None();

    forwardTerminal(status);
  }

  void escalated(const Duration& timeout)
  {
    if (terminated) {
      return;
    }

    CHECK_SOME(pid);

    killObservers();

    LOG(INFO) << "Process " << pid.get() << " did not terminate after "
              << timeout << ", sending SIGKILL to process tree at "
              << pid.get();

    // TODO(nnielsen): Sending SIGTERM in the first stage of the
    // shutdown may leave orphan processes hanging off init. This
    // scenario will be handled when PID namespace encapsulated
    // execution is in place.
    Stopwatch stopwatch;
    stopwatch.start();

    Try<std::list<os::ProcessTree>> trees =
      os::killtree(pid.get(), SIGKILL, true, true);

    if (trees.isError()) {
      LOG(ERROR) << "Failed to kill the process tree rooted at pid "
                 << pid.get() << ": " << trees.error();

      // Process 'pid' may not have received signal before
      // os::killtree() failed. To make sure process 'pid' is reaped
      // we send SIGKILL directly.
      os::kill(pid.get(), SIGKILL);
    } else {
      LOG(INFO) << "Sent SIGKILL to process tree at pid " << pid.get()
                << " in " << stopwatch.elapsed();
      VLOG(1) << "Killed the following process trees:\n"
              << stringify(trees.get());
    }
  }


  void taskCompletionTimeout(const TaskID& taskId, const Duration& duration)
  {
    CHECK(!terminated);
    CHECK(!killed);

    LOG(INFO) << "Killing task " << taskId
              << " which exceeded its maximum completion time of " << duration;

    taskCompletionTimer = None();
    killedByMaxCompletionTimer = true;

    // Use a zero gracePeriod to kill the task.
    kill(taskId, Duration::zero());
  }


  // Use this helper to create a status update from scratch, i.e., without
  // previously attached extra information like `data` or `check_status`.
  TaskStatus createTaskStatus(
      const TaskID& _taskId,
      const TaskState& state,
      const Option<TaskStatus::Reason>& reason = None(),
      const Option<string>& message = None())
  {
    TaskStatus status = protobuf::createTaskStatus(
        _taskId,
        state,
        id::UUID::random(),
        Clock::now().secs());

    status.mutable_executor_id()->CopyFrom(executorId);
    status.set_source(TaskStatus::SOURCE_EXECUTOR);

    if (reason.isSome()) {
      status.set_reason(reason.get());
    }

    if (message.isSome()) {
      status.set_message(message.get());
    }

    // TODO(alexr): Augment health information in a way similar to
    // `CheckStatusInfo`. See MESOS-6417 for more details.

    // If a check for the task has been defined, `check_status` field in each
    // task status must be set to a valid `CheckStatusInfo` message even if
    // there is no check status available yet.
    if (taskData->taskInfo.has_check()) {
      CheckStatusInfo checkStatusInfo;
      checkStatusInfo.set_type(taskData->taskInfo.check().type());
      switch (taskData->taskInfo.check().type()) {
        case CheckInfo::COMMAND: {
          checkStatusInfo.mutable_command();
          break;
        }
        case CheckInfo::HTTP: {
          checkStatusInfo.mutable_http();
          break;
        }
        case CheckInfo::TCP: {
          checkStatusInfo.mutable_tcp();
          break;
        }
        case CheckInfo::UNKNOWN: {
          LOG(FATAL) << "UNKNOWN check type is invalid";
          break;
        }
      }

      status.mutable_check_status()->CopyFrom(checkStatusInfo);
    }

    return status;
  }

  void forward(TaskStatus status)
  {
    // The single sender. A status after a terminal one cannot be delivered
    // in order by definition, so it is refused here rather than racing the
    // terminal update at the node.
    if (terminalForwarded) {
      LOG(WARNING) << "Not sending " << status.state()
                   << " after a terminal status update";
      return;
    }

    if (medios::internal::protobuf::isTerminalState(status.state())) {
      terminalForwarded = true;
    }

    // Every status the executor sends carries the newest report, so a
    // lifecycle update does not wait for the report cadence. The report is
    // taken here rather than where the status is built, so a status this
    // refuses does not consume a pending discontinuity note.
    if (progressReader.get() != nullptr) {
      Option<TaskStatus::Progress> progress = progressReader->report();
      if (progress.isSome()) {
        status.mutable_progress()->CopyFrom(progress.get());
        progressReader->reported();
      }
    }

    // An observation reaches the record only as the progress message, and
    // only after the reader has set the work value, so nothing an observer
    // says moves the value or the stall clock (DR-041). A line already sent
    // leaves the reader's own message in place.
    Option<double> observedAt = None();

    if (observers.get() != nullptr) {
      observers->drain();

      Option<string> readerMessage = None();
      if (status.has_progress() && status.progress().has_message()) {
        readerMessage = status.progress().message();
      }

      Option<string> line = observers->fold(readerMessage);
      if (line.isSome()) {
        status.mutable_progress()->set_message(line.get());
        observedAt = observers->observedAt();
      }
    }

    // One counter for the task's reports, whether the reader produced this
    // one or an observer did, so `sequence` advances across both and a
    // consumer folding by it never sees a number twice or a gap it cannot
    // explain.
    if (status.has_progress()) {
      observer::stampReport(
          status.mutable_progress(), ++reportSequence, observedAt);
    }

    Call call;
    call.set_type(Call::UPDATE);

    call.mutable_framework_id()->CopyFrom(frameworkId);
    call.mutable_executor_id()->CopyFrom(executorId);

    call.mutable_update()->mutable_status()->CopyFrom(status);

    // Capture the status update.
    unacknowledgedUpdates[id::UUID::fromBytes(status.uuid()).get()] =
      call.update();

    // Overwrite the last task status.
    lastTaskStatus = status;

    medios->send(evolve(call));
  }

  void selfTerminate()
  {
    // Nothing of this task's survives this executor, so an observer that
    // did not take the SIGTERM at the task's exit is killed here. `EXIT`
    // below leaves nothing to run a destructor.
    if (observers.get() != nullptr) {
      observers->signalKill();
    }

    Option<string> value = os::getenv("MEDIOS_HTTP_COMMAND_EXECUTOR");
    if (value.isSome() && value.get() == "1") {
      // If we get here, that means HTTP based command executor does
      // not get the ACK for the terminal status update, let's exit
      // with non-zero status since this should not happen.
      EXIT(EXIT_FAILURE)
        << "Did not receive ACK for the terminal status update from the node";
    } else {
      // For adapter based executor, the terminal status update should
      // have already been sent to the node at this point, so we can
      // safely self terminate.
      terminate(self());
    }
  }

  enum State
  {
    CONNECTED,
    DISCONNECTED,
    SUBSCRIBED
  } state;

  struct TaskData
  {
    explicit TaskData(const TaskInfo& _taskInfo)
      : taskInfo(_taskInfo), acknowledged(false) {}

    TaskInfo taskInfo;

    // Indicates whether a status update acknowledgement
    // has been received for any status update.
    bool acknowledged;
  };

  // Once `TaskInfo` is received, it is cached for later access.
  Option<TaskData> taskData;

  // TODO(alexr): Introduce a state enum and document transitions,
  // see MESOS-5252.
  bool launched;
  bool killed;
  bool killedByHealthCheck;
  bool killedByMaxCompletionTimer;
  bool killedByProgressStall;

  // Whether the current stall episode has already been reported, so
  // `report` sends one update per episode rather than one per sample.
  bool stallReported;

  bool terminated;

  // Whether a terminal status has been handed to the sender, which is what
  // keeps a progress report from following one.
  bool terminalForwarded;

  // Set when the stall fired, for the message on the terminal status.
  Duration stallDuration = Duration::zero();
  Option<double> stallLastValue;

  Option<progress::ProgressPolicy> progressPolicy;
  Owned<progress::ProgressReader> progressReader;
  Option<Timer> progressSampleTimer;
  Option<Time> lastProgressReport;

  Owned<observer::ObserverSet> observers;
  Option<Timer> observerDrainTimer;

  // The observers' escalation: set once SIGTERM has been sent, and the
  // timer that sends SIGKILL on the task's deadline.
  bool observersSignalled = false;
  Option<Timer> observerKillTimer;

  // The task's report counter, advanced once per status that carries a
  // progress report.
  uint64_t reportSequence = 0u;

  Option<Time> killGracePeriodStart;
  Option<Timer> killGracePeriodTimer;
  Option<Timer> taskCompletionTimer;

  // The task's ceiling and when it started, so what it has left can be
  // read when the work command exits (DR-042).
  Option<Duration> ceiling;
  Option<Time> ceilingStart;

  // The finish hook: declared on the task, running, and its outcome's
  // inputs. `pendingTerminal` is the work's terminal status, held until
  // the hook's outcome can be added to it.
  Option<TaskFinish> finish;
  bool finishing = false;
  bool finishSignalled = false;
  bool finishTimedOutAtLimit = false;
  bool finishKilled = false;
  bool shuttingDown = false;
  Option<pid_t> finishPid;
  Option<Duration> finishLimit;
  Option<Time> finishStart;
  Option<Timer> finishLimitTimer;
  Option<Timer> finishKillTimer;
  Option<TaskStatus> pendingTerminal;

  Option<pid_t> pid;
  Duration shutdownGracePeriod;
  Option<KillPolicy> killPolicy;
  Option<FrameworkInfo> frameworkInfo;
  Option<TaskID> taskId;
  string launcherDir;
  Option<string> rootfs;
  Option<string> sandboxDirectory;
  Option<string> workingDirectory;
  Option<string> user;
  Option<string> taskCommand;
  Option<Environment> taskEnvironment;
  Option<CapabilityInfo> effectiveCapabilities;
  Option<CapabilityInfo> boundingCapabilities;
  Option<string> ttyNodePath;
  Option<ContainerLaunchInfo> taskLaunchInfo;
  Option<vector<gid_t>> taskSupplementaryGroups;
  const FrameworkID frameworkId;
  const ExecutorID executorId;
  Owned<MediosBase> medios;

  LinkedHashMap<id::UUID, Call::Update> unacknowledgedUpdates;

  Option<TaskStatus> lastTaskStatus;

  Owned<checks::Checker> checker;
  Owned<checks::HealthChecker> healthChecker;
};

} // namespace internal {
} // namespace medios {


class Flags : public virtual medios::internal::logging::Flags
{
public:
  Flags()
  {
    add(&Flags::rootfs,
        "rootfs",
        "The path to the root filesystem for the task");

    // The following flags are only applicable when a rootfs is
    // provisioned for this command.
    add(&Flags::sandbox_directory,
        "sandbox_directory",
        "The absolute path for the directory in the container where the\n"
        "sandbox is mapped to");

    add(&Flags::working_directory,
        "working_directory",
        "The working directory for the task in the container.");

    add(&Flags::user,
        "user",
        "The user that the task should be running as.");

    add(&Flags::task_command,
        "task_command",
        "If specified, this is the overrided command for launching the\n"
        "task (instead of the command from TaskInfo).");

    add(&Flags::task_environment,
        "task_environment",
        "If specified, this is a JSON-ified `Environment` protobuf that\n"
        "should be added to the executor's environment before launching\n"
        "the task.");

    add(&Flags::effective_capabilities,
        "effective_capabilities",
        "Capabilities granted to the command at launch.");

    add(&Flags::bounding_capabilities,
        "bounding_capabilities",
        "The bounding set of capabilities the command can use.");

    add(&Flags::task_launch_info,
        "task_launch_info",
        "The launch info to run the task.");

    add(&Flags::tty_node_path,
        "tty_node_path",
        "A path to the node end of the attached TTY if there is one.");

    add(&Flags::task_supplementary_groups,
        "task_supplementary_groups",
        "Comma-separated list of supplementary groups to run the task with.");

    add(&Flags::launcher_dir,
        "launcher_dir",
        "Directory path of Medios binaries.",
        PKGLIBEXECDIR);

    // TODO(nnielsen): Add 'prefix' option to enable replacing
    // 'sh -c' with user specified wrapper.
  }

  Option<string> rootfs;
  Option<string> sandbox_directory;
  Option<string> working_directory;
  Option<string> user;
  Option<string> task_command;
  Option<Environment> task_environment;
  Option<medios::CapabilityInfo> effective_capabilities;
  Option<medios::CapabilityInfo> bounding_capabilities;
  Option<string> tty_node_path;
  Option<JSON::Object> task_launch_info;
  Option<vector<gid_t>> task_supplementary_groups;
  string launcher_dir;
};


int main(int argc, char** argv)
{
  medios::FrameworkID frameworkId;
  medios::ExecutorID executorId;

  Flags flags;

  // Load flags from command line.
  Try<flags::Warnings> load = flags.load(None(), &argc, &argv);

  if (flags.help) {
    cout << flags.usage() << endl;
    return EXIT_SUCCESS;
  }

  if (load.isError()) {
    cerr << flags.usage(load.error()) << endl;
    return EXIT_FAILURE;
  }

  medios::internal::logging::initialize(argv[0], true, flags); // Catch signals.

  // Log any flag warnings (after logging is initialized).
  foreach (const flags::Warning& warning, load->warnings) {
    LOG(WARNING) << warning.message;
  }

  Option<string> value = os::getenv("MEDIOS_FRAMEWORK_ID");
  if (value.isNone()) {
    EXIT(EXIT_FAILURE)
      << "Expecting 'MEDIOS_FRAMEWORK_ID' to be set in the environment";
  }
  frameworkId.set_value(value.get());

  value = os::getenv("MEDIOS_EXECUTOR_ID");
  if (value.isNone()) {
    EXIT(EXIT_FAILURE)
      << "Expecting 'MEDIOS_EXECUTOR_ID' to be set in the environment";
  }
  executorId.set_value(value.get());

  // Get executor shutdown grace period from the environment.
  //
  // NOTE: We avoided introducing a command executor flag for this
  // because the command executor exits if it sees an unknown flag.
  // This makes it difficult to add or remove command executor flags
  // that are unconditionally set by the node.
  Duration shutdownGracePeriod = DEFAULT_EXECUTOR_SHUTDOWN_GRACE_PERIOD;
  value = os::getenv("MEDIOS_EXECUTOR_SHUTDOWN_GRACE_PERIOD");
  if (value.isSome()) {
    Try<Duration> parse = Duration::parse(value.get());
    if (parse.isError()) {
      EXIT(EXIT_FAILURE)
        << "Failed to parse value '" << value.get() << "'"
        << " of 'MEDIOS_EXECUTOR_SHUTDOWN_GRACE_PERIOD': " << parse.error();
    }

    shutdownGracePeriod = parse.get();
  }

  Option<ContainerLaunchInfo> task_launch_info;
  if (flags.task_launch_info.isSome()) {
    Try<ContainerLaunchInfo> parse =
      protobuf::parse<ContainerLaunchInfo>(flags.task_launch_info.get());

    if (parse.isError()) {
      EXIT(EXIT_FAILURE)
        << "Failed to parse task launch info: " << parse.error();
    }

    task_launch_info = parse.get();
  }

  process::initialize();

  Owned<medios::internal::CommandExecutor> executor(
      new medios::internal::CommandExecutor(
          flags.launcher_dir,
          flags.rootfs,
          flags.sandbox_directory,
          flags.working_directory,
          flags.user,
          flags.task_command,
          flags.task_environment,
          flags.effective_capabilities,
          flags.bounding_capabilities,
          flags.tty_node_path,
          task_launch_info,
          flags.task_supplementary_groups,
          frameworkId,
          executorId,
          shutdownGracePeriod));

  process::spawn(executor.get());
  process::wait(executor.get());
  executor.reset();

  // also clean up the networking stack before exiting.
  process::finalize(true);
  return EXIT_SUCCESS;
}
