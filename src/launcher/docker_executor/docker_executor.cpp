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

// medios-docker-executor: a standard Medios Executor that launches a
// Docker container on behalf of a task, monitors it, and reports
// TASK_STARTING / TASK_RUNNING / TASK_FINISHED / TASK_FAILED via the
// MediosExecutorDriver protocol.
//
// Architecture: medios's DockerShellContainerizer forks this binary as a
// host process via process::Subprocess. The user's container has no
// medios awareness — this executor wraps it from outside.
//
// Status mapping:
//   medios issued kill           → TASK_KILLED  (out-of-band flag)
//   else docker exit code == 0   → TASK_FINISHED
//   else                         → TASK_FAILED  (exit code in message)
//
// Documented contracts deliberately NOT covered by this base
// implementation, each tracked as its own R-issue under DR-030:
//   R1 / #145  kill semantics (long-running container + framework kill)
//   R2 / #146  sandbox bind mount
//   R3 / #147  environment propagation (basic shape is here; needs test)
//   R4 / #148  stdout/stderr capture into sandbox logs
//
// Deferred non-goal: D1 / #149 recovery after node restart.

#include <atomic>
#include <chrono>
#include <cstdlib>
#include <iostream>
#include <map>
#include <mutex>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include <medios/executor.hpp>
#include <medios/medios.hpp>

#include <process/owned.hpp>

#include <stout/duration.hpp>
#include <stout/lambda.hpp>
#include <stout/nothing.hpp>
#include <stout/os.hpp>
#include <stout/option.hpp>
#include <stout/stringify.hpp>
#include <stout/try.hpp>

#include "launcher/docker_executor/docker_exec.hpp"

#include "launcher/observer.hpp"
#include "launcher/progress.hpp"

#include "node/stores.hpp"

namespace medios {
namespace internal {
namespace docker {

// How long the progress sampling thread waits between checks that it should
// still be running.
//
// Parameter, owned by this executor, 50 ms, provisional: it bounds how long
// a finished container keeps a sampling thread alive, and is far below the
// 1000 ms default sample interval so it does not change the sampling rate.
constexpr Duration SAMPLER_WAKEUP = Milliseconds(50);


// The grace period `docker stop` gives the container between SIGTERM and
// SIGKILL when the task's kill policy sets none.
//
// Default, owned by this executor, 10 s, provisional: Docker's own
// default. A task's `kill_policy.grace_period` replaces it (#315).
constexpr int STOP_GRACE_SECONDS = 10;


// The grace period an observer of a docker task gets between SIGTERM and
// SIGKILL.
//
// Parameter, owned by this executor, default 3 s, provisional: the command
// executor's default kill grace, which an observer gets there. It is not
// the container's stop grace, which is the task's kill policy or docker's
// default for a workload with state to flush; an observer has none. The
// escalation runs to its end before this executor exits, because a process
// this executor leaves behind is reparented and keeps running.
constexpr Duration OBSERVER_KILL_GRACE = Seconds(3);


class DockerExecutor : public medios::Executor
{
public:
  void registered(
      medios::ExecutorDriver* /*driver*/,
      const medios::ExecutorInfo& executorInfo,
      const medios::FrameworkInfo& /*frameworkInfo*/,
      const medios::NodeInfo& /*nodeInfo*/) override
  {
    std::cerr << "[medios-docker-executor] registered executor='"
              << executorInfo.executor_id().value() << "'\n";
  }

  void reregistered(
      medios::ExecutorDriver* /*driver*/,
      const medios::NodeInfo& /*nodeInfo*/) override {}

  void disconnected(medios::ExecutorDriver* /*driver*/) override {}

  void launchTask(
      medios::ExecutorDriver* driver,
      const medios::TaskInfo& task) override
  {
    std::cerr << "[medios-docker-executor] launchTask id='"
              << task.task_id().value() << "'\n";

    // Spike-1 finding 6: for Docker tasks, the user's actual command lives
    // in task.command(), not in any executor_info wrapping. The node-side
    // command-executor wrapping that produced 'sh -c .../medios-executor' in
    // spike 1 is bypassed because this binary IS the executor.
    if (!task.has_container() ||
        task.container().type() != medios::ContainerInfo::DOCKER ||
        !task.container().docker().has_image()) {
      sendFailed(driver, task.task_id(),
                 "Task has no Docker image in container_info");
      return;
    }
    const std::string image = task.container().docker().image();

    stopGrace = stopGraceSeconds(task, STOP_GRACE_SECONDS);

    // Read the progress policy before the container starts, so a task that
    // names an unusable setting is refused rather than run with progress
    // silently off.
    Try<Option<progress::ProgressPolicy>> policy =
      progress::progressPolicyFrom(task);

    if (policy.isError()) {
      sendInvalidTask(
          driver, task.task_id(), "Invalid progress settings: " + policy.error());
      return;
    }

    progressPolicy = policy.get();

    if (progressPolicy.isSome()) {
      // The node hands the sandbox path down as MEDIOS_DIRECTORY, and the
      // container's stdout is redirected into a file there. Without it
      // there is nowhere to read progress from, and reading some other
      // directory would report another task's numbers as this one's.
      const char* sandboxEnv = ::getenv("MEDIOS_DIRECTORY");
      if (sandboxEnv == nullptr || *sandboxEnv == '\0') {
        sendInvalidTask(
            driver,
            task.task_id(),
            "Invalid progress settings: the task carries progress settings"
            " but MEDIOS_DIRECTORY is not set, so the sandbox the progress"
            " source lives in is unknown");
        return;
      }

      Try<process::Owned<progress::ProgressReader>> reader =
        progress::ProgressReader::create(progressPolicy.get(), sandboxEnv);

      if (reader.isError()) {
        sendInvalidTask(
            driver,
            task.task_id(),
            "Invalid progress settings: " + reader.error());
        return;
      }

      progressReader = reader.get();
    }

    if (!task.observers().empty()) {
      // An observer runs in the task's sandbox, and the node hands that
      // path down as MEDIOS_DIRECTORY. Without it there is nowhere to run
      // one, which the set records as every observer failing to launch.
      // That is one line in the record and nothing else: an observer never
      // decides whether the task runs (DR-041).
      const char* sandboxEnv = ::getenv("MEDIOS_DIRECTORY");

      observers = observer::ObserverSet::create(
          task.observers(), sandboxEnv == nullptr ? "" : sandboxEnv);
    }

    // Run the lifecycle on a worker thread so we don't block the driver.
    std::thread([this, driver, task, image]() {
      runDockerTask(driver, task, image);
    }).detach();
  }

  void killTask(
      medios::ExecutorDriver* driver,
      const medios::TaskID& taskId) override
  {
    std::cerr << "[medios-docker-executor] killTask id='"
              << taskId.value() << "'\n";

    // Set killIssued FIRST so a concurrent runDockerTask sees it (the
    // worker thread checks killIssued after docker run exits).
    killIssued.store(true);

    // A container being stopped stops advancing, so leaving the sampler
    // running would turn the framework's kill into a stall.
    sampling.store(false);

    // An observer is killed with the task it observes (DR-041). `docker
    // stop` below gives the container its own escalation; the observers get
    // theirs on their own timer, which the worker thread waits out once
    // `docker run` has returned.
    terminateObservers();

    if (containerName.empty()) {
      // Kill arrived before runDockerTask had a chance to set
      // containerName. The worker thread will see killIssued, skip the
      // docker run entirely, and emit TASK_KILLED. R1: prevents the
      // race where a kill is lost before docker create.
      std::cerr << "[medios-docker-executor] kill received before launch; "
                << "worker thread will short-circuit\n";
      return;
    }

    stopContainer();
  }

  void frameworkMessage(
      medios::ExecutorDriver* /*driver*/,
      const std::string& /*data*/) override {}

  void shutdown(medios::ExecutorDriver* driver) override
  {
    killIssued.store(true);
    terminateObservers();
    if (!containerName.empty()) {
      dockerCapture({"docker", "kill", containerName});
    }

    // Nothing of this task's survives this executor, so the observers'
    // escalation finishes before the driver stops.
    awaitObserverTeardown();

    driver->stop();
  }

  void error(
      medios::ExecutorDriver* /*driver*/,
      const std::string& message) override
  {
    std::cerr << "[medios-docker-executor] driver error: " << message << "\n";
  }

private:
  // `docker stop` sends SIGTERM, waits the grace period, then SIGKILL:
  // the escalation this executor has for the kill policy.
  void stopContainer()
  {
    // Whatever stops the container stops its observers: a stall kill
    // reaches here without going through `killTask`.
    terminateObservers();

    std::cerr << "[medios-docker-executor] docker stop --time="
              << stopGrace << " " << containerName << "\n";
    dockerCapture({"docker", "stop",
                   "--time=" + stringify(stopGrace),
                   containerName});
  }


  // Forks one observer as a host-side child of this executor, with the
  // task's sandbox as its working directory and this executor's own
  // environment. The task's uid lives inside the container, so an observer
  // here runs as this executor does and its command must be a host command
  // (DR-041).
  static Try<process::Subprocess> forkObserver(
      const medios::TaskObserver& declaration,
      const std::string& stderrPath,
      const std::string& sandboxDir)
  {
    std::string path;
    std::vector<std::string> argv;

    if (declaration.command().shell()) {
      path = "/bin/sh";
      argv = {"sh", "-c", declaration.command().value()};
    } else {
      path = declaration.command().value();
      argv.push_back(path);
      for (const auto& argument : declaration.command().arguments()) {
        argv.push_back(argument);
      }
    }

    // An observer reports by writing, so it is given no stdin. Its stderr
    // is a sandbox file rather than this executor's, which would land in
    // the task's own `stderr` and read as the container's.
    return process::subprocess(
        path,
        argv,
        process::Subprocess::PATH(os::DEV_NULL),
        process::Subprocess::PIPE(),
        process::Subprocess::PATH(stderrPath),
        nullptr,
        None(),
        None(),
        {},
        {process::Subprocess::ChildHook::CHDIR(sandboxDir),
         process::Subprocess::ChildHook::SETSID()});
  }


  // Launches the observers, unless a teardown has already run: an observer
  // started after one would never be signalled. Holds `updateMutex`, which
  // is what orders this against the teardown and against `send`'s reads of
  // the same set.
  void launchObservers(
      medios::ExecutorDriver* driver,
      const medios::TaskID& taskId,
      const std::string& sandboxDir)
  {
    {
      std::lock_guard<std::mutex> lock(updateMutex);

      if (observers.get() == nullptr || observersSignalled) {
        return;
      }

      const std::string sandbox = sandboxDir;

      observers->launch(
          [sandbox](
              const medios::TaskObserver& declaration,
              const std::string& stderrPath) {
            return forkObserver(declaration, stderrPath, sandbox);
          });

      observing.store(true);

      // Started under the lock, so a teardown cannot pass between the
      // launch and the thread and leave the thread unjoined.
      observerDrainer = std::thread([this, driver, taskId]() {
        drainObservers(driver, taskId);
      });
    }
  }


  // Sends SIGTERM to every observer and arms the escalation, which sends
  // SIGKILL when `OBSERVER_KILL_GRACE` is up. Returns without waiting:
  // every path that stops the container calls this, and a kill arriving on
  // the driver thread must not wait out a grace period.
  //
  // The SIGKILL goes to every observer that was forked, not to the ones
  // still running: an observer that exited can have left a child in its
  // group, and that child is what the signal is for.
  void terminateObservers()
  {
    std::lock_guard<std::mutex> lock(updateMutex);

    if (observers.get() == nullptr || observersSignalled) {
      return;
    }

    observersSignalled = true;
    observing.store(false);

    // Whatever the observers managed to say before this.
    observers->drain();
    observers->signalTerm();

    if (!observers->launched()) {
      // Nothing was forked, so there is no group to escalate against.
      return;
    }

    const std::chrono::steady_clock::time_point deadline =
      std::chrono::steady_clock::now() +
        std::chrono::nanoseconds(OBSERVER_KILL_GRACE.ns());

    observerEscalator = std::thread([this, deadline]() {
      // SIGKILL goes out as soon as every observer has been reaped, which
      // is when the signal is for whatever they left in their groups, and
      // otherwise when the deadline is up. Waiting out the grace period
      // against processes that have already gone would hold the task's
      // terminal status for nothing.
      while (std::chrono::steady_clock::now() < deadline) {
        {
          std::lock_guard<std::mutex> lock(updateMutex);
          if (observers->running().empty()) {
            break;
          }
        }

        std::this_thread::sleep_for(
            std::chrono::nanoseconds(SAMPLER_WAKEUP.ns()));
      }

      std::lock_guard<std::mutex> lock(updateMutex);
      observers->signalKill();
    });
  }


  // Waits for the observers' teardown to finish: the draining thread
  // stopped and the escalation run. This executor's exit would reparent an
  // observer rather than end it, so every path to `driver->stop()` goes
  // through here.
  //
  // `call_once` so the worker thread and a shutdown on the driver thread
  // can both reach it: the second caller waits for the first rather than
  // joining a thread twice or passing a teardown that has not finished.
  void awaitObserverTeardown()
  {
    std::call_once(observerTeardown, [this]() {
      if (observerDrainer.joinable()) {
        observerDrainer.join();
      }

      if (observerEscalator.joinable()) {
        observerEscalator.join();
      }

      std::lock_guard<std::mutex> lock(updateMutex);
      observersFinished = true;
    });
  }


  // Reads the observers' pipes until the container exits and sends a
  // progress-only `TASK_RUNNING` when one of them has said something new.
  // One thread, so the set is touched from here and from `send` only, both
  // under `updateMutex`.
  void drainObservers(
      medios::ExecutorDriver* driver,
      const medios::TaskID& taskId)
  {
    Duration untilDrain = observer::DEFAULT_DRAIN_INTERVAL;

    while (observing.load()) {
      std::this_thread::sleep_for(
          std::chrono::nanoseconds(SAMPLER_WAKEUP.ns()));

      if (!observing.load()) {
        return;
      }

      untilDrain -= SAMPLER_WAKEUP;
      if (untilDrain > Duration::zero()) {
        continue;
      }
      untilDrain = observer::DEFAULT_DRAIN_INTERVAL;

      bool fresh = false;
      {
        std::lock_guard<std::mutex> lock(updateMutex);
        observers->drain();
        fresh = observers->hasUnreported();
      }

      if (fresh) {
        sendUpdate(driver, taskId, medios::TASK_RUNNING, "RUNNING");
      }
    }
  }


  // Samples the task's progress source until the container exits. One
  // thread, so the reader is touched from here and from `sendUpdate`'s
  // stamping only, both under `updateMutex`.
  void sampleProgress(
      medios::ExecutorDriver* driver,
      const medios::TaskID& taskId)
  {
    Duration untilSample = progressPolicy->sampleInterval;

    while (sampling.load()) {
      std::this_thread::sleep_for(
          std::chrono::nanoseconds(SAMPLER_WAKEUP.ns()));

      if (!sampling.load() || killIssued.load()) {
        return;
      }

      untilSample -= SAMPLER_WAKEUP;
      if (untilSample > Duration::zero()) {
        continue;
      }
      untilSample = progressPolicy->sampleInterval;

      bool stalled = false;
      std::string explanation;
      {
        std::lock_guard<std::mutex> lock(updateMutex);

        Try<Nothing> sample = progressReader->sample();

        // An observation failure is this executor's, not the task's.
        if (sample.isError()) {
          std::cerr << "[medios-docker-executor] failed to observe progress"
                    << " of task '" << taskId.value() << "': "
                    << sample.error() << "\n";
        }

        stalled = progressReader->stalled();

        if (stalled) {
          explanation =
            "REASON_PROGRESS_STALLED: no progress for " +
            stringify(progressReader->sinceAdvance()) +
            "; last reported fraction " +
            (progressReader->fraction().isSome()
               ? stringify(progressReader->fraction().get())
               : "none");

          stallExplanation = explanation;
        } else {
          // The task advanced, so the next stall is a new episode.
          stallReported = false;
        }
      }

      if (stalled) {
        if (progressPolicy->action == progress::ProgressPolicy::KILL) {
          std::cerr << "[medios-docker-executor] killing task '"
                    << taskId.value() << "': " << explanation << "\n";

          killedByStall.store(true);
          stopContainer();
          return;
        }

        if (!stallReported) {
          stallReported = true;
          std::cerr << "[medios-docker-executor] reporting a stall of task '"
                    << taskId.value() << "': " << explanation << "\n";
          sendUpdate(driver, taskId, medios::TASK_RUNNING, explanation);
        }

        continue;
      }

      bool reportable = false;
      {
        std::lock_guard<std::mutex> lock(updateMutex);
        reportable = progressReader->hasReport();
      }

      if (reportable &&
          (lastReport.isNone() ||
           lastReport.get() + progress::REPORT_CADENCE <= sinceStart())) {
        lastReport = sinceStart();
        sendUpdate(driver, taskId, medios::TASK_RUNNING, "RUNNING");
      }
    }
  }


  // Monotonic time since this executor started, for the report cadence.
  Duration sinceStart() const
  {
    return Nanoseconds(
        std::chrono::duration_cast<std::chrono::nanoseconds>(
            std::chrono::steady_clock::now() - started).count());
  }


  void runDockerTask(
      medios::ExecutorDriver* driver,
      const medios::TaskInfo& task,
      const std::string& image)
  {
    // R1 / #145: handle "kill before run" — if killTask arrived before
    // we started, short-circuit. Otherwise the kill is silently dropped
    // and the task hangs as TASK_RUNNING with no container.
    if (killIssued.load()) {
      std::cerr << "[medios-docker-executor] kill seen before launch; "
                << "emitting TASK_KILLED without starting container\n";
      sendUpdate(driver, task.task_id(), medios::TASK_KILLED,
                 "Killed before container launch");
      std::this_thread::sleep_for(std::chrono::milliseconds(300));
      driver->stop();
      return;
    }

    containerName = "medios-" + task.task_id().value();

    // Every volume is a STORE volume resolved against the stores file the
    // node declared (DR-036), reached here as MEDIOS_STORES in the
    // environment. Resolved before docker pull so a bad task shape does
    // not waste a network round trip; each refusal names what was wrong.
    std::vector<std::string> volumeArguments;
    {
      std::map<std::string, medios::internal::node::StoreDeclaration> stores;

      const char* storesEnv = ::getenv("MEDIOS_STORES");
      if (storesEnv != nullptr && *storesEnv != '\0') {
        Try<std::map<std::string, medios::internal::node::StoreDeclaration>>
          read = medios::internal::node::readStores(storesEnv);
        if (read.isError()) {
          sendInvalidTask(driver, task.task_id(), read.error());
          return;
        }
        stores = read.get();
      }

      // The sandbox `docker run` binds below, where a relative container
      // path places its store.
      const char* directory = ::getenv("MEDIOS_DIRECTORY");

      Try<std::vector<std::string>> arguments =
        medios::internal::docker::storeVolumeArguments(
            task.container().volumes(),
            stores,
            directory != nullptr ? directory : "");
      if (arguments.isError()) {
        sendInvalidTask(driver, task.task_id(), arguments.error());
        return;
      }
      volumeArguments = arguments.get();
    }

    // The GPUs the node assigned this task, refused when they do not match
    // its grant; a task granted none gets none, whatever the image asks.
    double grantedGpus = 0;
    for (const auto& resource : task.resources()) {
      if (resource.name() == "gpus" &&
          resource.type() == medios::Value::SCALAR) {
        grantedGpus += resource.scalar().value();
      }
    }

    const char* devicesEnv = ::getenv(GPU_DEVICES_ENVIRONMENT_VARIABLE);
    Try<std::vector<std::string>> gpus = gpuArguments(
        grantedGpus,
        devicesEnv == nullptr ? Option<std::string>::none()
                              : Option<std::string>(devicesEnv));
    if (gpus.isError()) {
      sendInvalidTask(driver, task.task_id(), gpus.error());
      return;
    }

    // The task's IPC namespace and /dev/shm size, refused here when the
    // medios containerizer would refuse them too, so a task that asks for a
    // larger /dev/shm never runs with Docker's 64 MB default (#648).
    Try<std::vector<std::string>> ipc =
      medios::internal::docker::ipcArguments(task.container());
    if (ipc.isError()) {
      sendInvalidTask(driver, task.task_id(), ipc.error());
      return;
    }

    // The network the task asked for, refused here rather than run on the
    // default bridge when docker_shell cannot give it that network.
    Try<std::vector<std::string>> network =
      medios::internal::docker::networkArguments(task);
    if (network.isError()) {
      sendInvalidTask(driver, task.task_id(), network.error());
      return;
    }

    // Extra `docker run` flags from `ContainerInfo.docker.parameters` (#574),
    // such as `--entrypoint`.
    Try<std::vector<std::string>> parameters =
      medios::internal::docker::parameterArguments(task.container().docker());
    if (parameters.isError()) {
      sendInvalidTask(driver, task.task_id(), parameters.error());
      return;
    }

    sendUpdate(driver, task.task_id(), medios::TASK_STARTING, "STARTING");

    // Honor ContainerInfo.DockerInfo.force_pull_image per the proto contract
    // (medios.proto: "...even if the image is already downloaded on the
    // node"). Default false: use the local image if present; pull only on
    // cache miss. true: always pull, ignoring local cache.
    const bool forcePull = task.container().docker().force_pull_image();
    const std::pair<Option<int>, std::string> inspect = dockerCapture(
        {"docker", "image", "inspect", image, "--format", "{{.Id}}"});
    const bool inLocalCache =
        inspect.first.isSome() && inspect.first.get() == 0;

    if (forcePull || !inLocalCache) {
      const std::pair<Option<int>, std::string> pull =
          dockerCapture({"docker", "pull", image});
      if (pull.first.isNone() || pull.first.get() != 0) {
        // Name the step that was tried so the message describes the whole
        // chain (cache miss, then failed pull), not just the pull error.
        const std::string cacheContext = inLocalCache
            ? ""
            : "image '" + image + "' not in local cache; ";
        const std::string msg =
            cacheContext +
            "failed to run 'docker pull " + image + "': " +
            describeStatus(pull.first) +
            (pull.second.empty() ? "" : "; stderr='" + pull.second + "'");
        sendUpdate(driver, task.task_id(), medios::TASK_FAILED, msg,
                   medios::TaskStatus::REASON_CONTAINER_LAUNCH_FAILED);
        std::this_thread::sleep_for(std::chrono::milliseconds(300));
        driver->stop();
        return;
      }
    }

    // Build `docker run` argv. --rm so docker cleans up the container after
    // it exits. --name to identify the container for kill/inspect later.
    std::vector<std::string> argv = {
      "docker", "run", "--rm",
      "--name", containerName,
    };

    // R2 / #146: bind-mount the task sandbox into the container at the
    // same path. The node sets MEDIOS_DIRECTORY when it forks us; that's
    // the sandbox root for this task. Bind-mounting at the same path
    // means absolute references inside the container resolve identically
    // on the host (frameworks can write checkpoints to a known location
    // and read them back).
    const char* sandboxEnv = ::getenv("MEDIOS_DIRECTORY");
    const std::string sandboxDir = sandboxEnv ? sandboxEnv : "";
    if (!sandboxDir.empty()) {
      argv.push_back("--volume");
      argv.push_back(sandboxDir + ":" + sandboxDir);
      argv.push_back("--workdir");
      argv.push_back(sandboxDir);
      // Surface the sandbox path inside the container under the well-known
      // env var name so user commands don't need to hardcode it.
      argv.push_back("--env");
      argv.push_back("MEDIOS_SANDBOX=" + sandboxDir);
    }

    // The task's stores, resolved above.
    argv.insert(argv.end(), volumeArguments.begin(), volumeArguments.end());

    // The task's IPC namespace and /dev/shm, resolved above.
    argv.insert(argv.end(), ipc->begin(), ipc->end());

    // R3 / #147: propagate user-provided env from command_info.environment.
    if (task.command().has_environment()) {
      for (const auto& var : task.command().environment().variables()) {
        if (var.type() == medios::Environment::Variable::VALUE &&
            var.has_value()) {
          argv.push_back("--env");
          argv.push_back(var.name() + "=" + var.value());
        }
      }
    }

    // Extra `docker run` parameters (#574), before `gpus` so a parameter
    // cannot override `NVIDIA_VISIBLE_DEVICES`.
    argv.insert(argv.end(), parameters->begin(), parameters->end());

    // After the task's environment and parameters, so the task cannot widen
    // its devices.
    argv.insert(argv.end(), gpus->begin(), gpus->end());

    // The task's network and published ports, resolved above.
    argv.insert(argv.end(), network->begin(), network->end());

    argv.push_back(image);

    // User command.
    if (task.command().shell()) {
      argv.push_back("sh");
      argv.push_back("-c");
      argv.push_back(task.command().value());
    } else {
      argv.push_back(task.command().value());
      for (const auto& a : task.command().arguments()) {
        argv.push_back(a);
      }
    }

    // TASK_RUNNING fires BEFORE the blocking docker run because a fast-exit
    // task (echo) would otherwise skip RUNNING entirely (spike-1 finding 7).
    sendUpdate(driver, task.task_id(), medios::TASK_RUNNING, "RUNNING");

    std::thread sampler;
    if (progressReader.get() != nullptr) {
      // The container is running now. The image pull above is not the task
      // failing to advance.
      progressReader->arm();

      sampling.store(true);
      sampler = std::thread([this, driver, task]() {
        sampleProgress(driver, task.task_id());
      });
    }

    std::cerr << "[medios-docker-executor] docker run image=" << image
              << " name=" << containerName << "\n";

    // R4 / #148: container stdout/stderr go to the sandbox files (subprocess
    // creates/appends them); without this, container output would land on the
    // node's streams, not where tooling expects.
    // The observers are launched from the hook, once `docker run` exists
    // and the sandbox `stdout` an observer may read has been created.
    // Launching before this left an observer opening a file that was not
    // there yet, and it is never restarted.
    const Option<int> runStatus = dockerRun(
        argv,
        sandboxDir,
        observers.get() == nullptr
          ? Option<lambda::function<void()>>(None())
          : Option<lambda::function<void()>>(
                [this, driver, task, sandboxDir]() {
                  launchObservers(driver, task.task_id(), sandboxDir);
                }));

    // The container is gone, so an observer of it is observing nothing.
    // The teardown is awaited here, on the thread that owns the lifecycle,
    // so the escalation has run before the terminal status and the
    // driver's stop below.
    terminateObservers();
    awaitObserverTeardown();

    if (sampler.joinable()) {
      sampling.store(false);
      sampler.join();

      // One last observation, so the terminal status says how far the task
      // got rather than how far it had got one sample interval ago.
      std::lock_guard<std::mutex> lock(updateMutex);
      Try<Nothing> sample = progressReader->sample();
      if (sample.isError()) {
        std::cerr << "[medios-docker-executor] failed to observe progress: "
                  << sample.error() << "\n";
      }
    }

    std::cerr << "[medios-docker-executor] docker run " << describeStatus(runStatus)
              << " killIssued=" << killIssued.load()
              << " observerTeardownFinished=" << observersFinished << "\n";

    // Spike-1 finding 8 (corrected): exit-code-only detection is unreliable
    // (BusyBox sleep handles SIGTERM cleanly and exits 0). The killIssued
    // flag is the source of truth for whether the framework killed this.
    const ContainerOutcome outcome = containerOutcome(
        runStatus,
        killIssued.load(),
        killedByStall.load(),
        stallExplanation);

    if (outcome.reason.isSome()) {
      sendUpdate(
          driver,
          task.task_id(),
          outcome.state,
          outcome.message,
          outcome.reason.get());
    } else {
      sendUpdate(driver, task.task_id(), outcome.state, outcome.message);
    }

    // Brief settle to flush the terminal status before the driver tears down.
    std::this_thread::sleep_for(std::chrono::milliseconds(300));
    driver->stop();
  }

  // The single sender. Lifecycle updates and progress reports come from
  // two threads, so one lock orders them, and a status after a terminal one
  // is refused rather than left to race the terminal update at the node.
  void send(
      medios::ExecutorDriver* driver,
      const medios::TaskStatus& status)
  {
    std::lock_guard<std::mutex> lock(updateMutex);

    if (terminalSent) {
      std::cerr << "[medios-docker-executor] not sending state "
                << status.state() << " after a terminal status update\n";
      return;
    }

    if (status.state() == medios::TASK_FINISHED ||
        status.state() == medios::TASK_FAILED ||
        status.state() == medios::TASK_KILLED ||
        status.state() == medios::TASK_LOST ||
        status.state() == medios::TASK_ERROR ||
        status.state() == medios::TASK_DROPPED ||
        status.state() == medios::TASK_GONE) {
      terminalSent = true;
    }

    medios::TaskStatus stamped = status;

    // Every status carries the newest report, so a lifecycle update does
    // not wait for the report cadence.
    if (progressReader.get() != nullptr) {
      Option<medios::TaskStatus::Progress> report = progressReader->report();
      if (report.isSome()) {
        stamped.mutable_progress()->CopyFrom(report.get());

        // Taken as sent, so a discontinuity note is not carried into the
        // next report as well.
        progressReader->reported();
      }
    }

    // An observation reaches the record only as the progress message, and
    // only after the reader has set the work value, so nothing an observer
    // says moves the value or the stall clock (DR-041).
    Option<double> observedAt = None();

    if (observers.get() != nullptr) {
      observers->drain();

      Option<std::string> readerMessage = None();
      if (stamped.has_progress() && stamped.progress().has_message()) {
        readerMessage = stamped.progress().message();
      }

      Option<std::string> line = observers->fold(readerMessage);
      if (line.isSome()) {
        stamped.mutable_progress()->set_message(line.get());
        observedAt = observers->observedAt();
      }
    }

    // One counter for the task's reports, whether the reader produced this
    // one or an observer did, so `sequence` advances across both.
    if (stamped.has_progress()) {
      observer::stampReport(
          stamped.mutable_progress(), ++reportSequence, observedAt);
    }

    driver->sendStatusUpdate(stamped);
  }

  void sendUpdate(
      medios::ExecutorDriver* driver,
      const medios::TaskID& taskId,
      medios::TaskState state,
      const std::string& message)
  {
    medios::TaskStatus status;
    *status.mutable_task_id() = taskId;
    status.set_state(state);
    status.set_source(medios::TaskStatus::SOURCE_EXECUTOR);
    status.set_message(message);
    send(driver, status);
  }

  // Overload that also carries a reason, so a launch/pull failure reaches the
  // framework as a diagnostic rather than a status with a null reason (#298).
  void sendUpdate(
      medios::ExecutorDriver* driver,
      const medios::TaskID& taskId,
      medios::TaskState state,
      const std::string& message,
      medios::TaskStatus::Reason reason)
  {
    medios::TaskStatus status;
    *status.mutable_task_id() = taskId;
    status.set_state(state);
    status.set_source(medios::TaskStatus::SOURCE_EXECUTOR);
    status.set_reason(reason);
    status.set_message(message);
    send(driver, status);
  }

  void sendFailed(
      medios::ExecutorDriver* driver,
      const medios::TaskID& taskId,
      const std::string& message)
  {
    sendUpdate(driver, taskId, medios::TASK_FAILED, message);
    std::this_thread::sleep_for(std::chrono::milliseconds(200));
    driver->stop();
  }

  // Reject a task before launch with TASK_ERROR + REASON_TASK_INVALID and
  // a structured message. Used when the TaskInfo violates a contract the
  // executor enforces (e.g. unsupported Volume shape). Per STYLE.md §5.1
  // TASK_ERROR is the right state — the task never ran and won't on retry.
  void sendInvalidTask(
      medios::ExecutorDriver* driver,
      const medios::TaskID& taskId,
      const std::string& message)
  {
    medios::TaskStatus status;
    *status.mutable_task_id() = taskId;
    status.set_state(medios::TASK_ERROR);
    status.set_source(medios::TaskStatus::SOURCE_EXECUTOR);
    status.set_reason(medios::TaskStatus::REASON_TASK_INVALID);
    status.set_message(message);
    send(driver, status);
    std::this_thread::sleep_for(std::chrono::milliseconds(300));
    driver->stop();
  }

  std::string containerName;
  std::atomic<bool> killIssued{false};

  // The `docker stop --time` this task's container gets, set from its kill
  // policy before the container starts.
  int stopGrace = STOP_GRACE_SECONDS;

  Option<progress::ProgressPolicy> progressPolicy;
  process::Owned<progress::ProgressReader> progressReader;

  process::Owned<observer::ObserverSet> observers;

  // Whether the observer draining thread should keep running, and the
  // thread itself, joined by the teardown.
  std::atomic<bool> observing{false};
  std::thread observerDrainer;

  // Guarded by `updateMutex`: whether the observers have been signalled,
  // which every path that stops the container does, and whether the
  // teardown has finished, which only `awaitObserverTeardown` decides.
  bool observersSignalled = false;
  bool observersFinished = false;

  // The escalation from SIGTERM to SIGKILL, joined by the teardown, which
  // runs once however many threads reach it.
  std::thread observerEscalator;
  std::once_flag observerTeardown;

  // The task's report counter, advanced once per status that carries a
  // progress report.
  uint64_t reportSequence = 0u;

  // Orders the lifecycle updates from the worker thread against the
  // progress reports from the sampling thread, and guards the reader.
  std::mutex updateMutex;
  bool terminalSent = false;

  // Whether the sampling thread should keep running.
  std::atomic<bool> sampling{false};

  // Whether this executor stopped the container because it stopped
  // advancing, which is a different terminal state from a framework kill.
  std::atomic<bool> killedByStall{false};

  // Touched by the sampling thread only.
  bool stallReported = false;
  Option<Duration> lastReport;

  // Written under `updateMutex` by the sampling thread, read by the worker
  // thread after it has joined that thread.
  std::string stallExplanation;

  const std::chrono::steady_clock::time_point started =
    std::chrono::steady_clock::now();
};

} // namespace docker
} // namespace internal
} // namespace medios


int main(int /*argc*/, char** /*argv*/)
{
  // The executor is forked by the node with a stripped environment; without
  // PATH, execvp("docker", ...) returns 127. Prepend common docker
  // locations so the binary is findable on macOS+Homebrew (Colima, Docker
  // Desktop), Linux (apt/yum default install path), and standard system
  // dirs.
  //
  // Production should accept a --docker_path flag or honor a node-level
  // containerizer.docker flag; this static prefix is adequate for alpha.
  const char* path = ::getenv("PATH");
  std::string newPath =
      "/opt/homebrew/bin:/usr/local/bin:/usr/bin:/bin";
  if (path != nullptr && *path != '\0') {
    newPath += ":";
    newPath += path;
  }
  ::setenv("PATH", newPath.c_str(), 1);

  medios::internal::docker::DockerExecutor executor;
  medios::MediosExecutorDriver driver(&executor);
  return driver.run() == medios::DRIVER_STOPPED ? 0 : 1;
}
