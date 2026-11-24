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

#include <errno.h>
#include <stdlib.h> // For random().

#include <algorithm>
#include <cmath>
#include <deque>
#include <iomanip>
#include <list>
#include <map>
#include <memory>
#include <set>
#include <sstream>
#include <string>
#include <utility>
#include <vector>

#include <glog/logging.h>

#include <medios/type_utils.hpp>

#include <medios/authentication/secret_generator.hpp>

#include <medios/module/authenticatee.hpp>

#include <medios/state/leveldb.hpp>
#include <medios/state/in_memory.hpp>

#include <process/after.hpp>
#include <process/async.hpp>
#include <process/check.hpp>
#include <process/collect.hpp>
#include <process/defer.hpp>
#include <process/delay.hpp>
#include <process/dispatch.hpp>
#include <process/http.hpp>
#include <process/id.hpp>
#include <process/loop.hpp>
#include <process/reap.hpp>
#include <process/time.hpp>

#include <process/ssl/flags.hpp>

#include <stout/bytes.hpp>
#include <stout/check.hpp>
#include <stout/duration.hpp>
#include <stout/exit.hpp>
#include <stout/fs.hpp>
#include <stout/json.hpp>
#include <stout/lambda.hpp>
#include <stout/net.hpp>
#include <stout/numify.hpp>
#include <stout/option.hpp>
#include <stout/os.hpp>
#include <stout/path.hpp>
#include <stout/protobuf.hpp>
#include <stout/stringify.hpp>
#include <stout/strings.hpp>
#include <stout/try.hpp>
#include <stout/utils.hpp>
#include <stout/uuid.hpp>

#include <stout/os/realpath.hpp>

#include "authentication/identity_assertion/identity_assertion_authenticator.hpp"

#include "common/authorization.hpp"
#include "common/protobuf_utils.hpp"
#include "common/resources_utils.hpp"
#include "common/status_utils.hpp"
#include "common/validation.hpp"

#include "credentials/credentials.hpp"

#include "hook/manager.hpp"

#ifdef __linux__
#include "linux/fs.hpp"
#endif // __linux__

#include "logging/logging.hpp"

#include "controller/detector/standalone.hpp"

#include "module/manager.hpp"

#include "node/compatibility.hpp"
#include "node/constants.hpp"
#include "node/flags.hpp"
#include "node/paths.hpp"
#include "node/node.hpp"
#include "node/state.pb.h"
#include "node/task_status_update_manager.hpp"

namespace http = process::http;

using google::protobuf::RepeatedPtrField;

using medios::SecretGenerator;

using medios::authorization::ACCESS_SANDBOX;

using medios::executor::Call;

using medios::controller::detector::ControllerDetector;

using medios::node::ContainerConfig;
using medios::node::ContainerTermination;
using medios::node::QoSController;
using medios::node::QoSCorrection;
using medios::node::ResourceEstimator;

using std::deque;
using std::list;
using std::map;
using std::ostream;
using std::ostringstream;
using std::set;
using std::shared_ptr;
using std::string;
using std::vector;

using process::after;
using process::async;
using process::wait; // Necessary on some OS's to disambiguate.
using process::Break;
using process::Clock;
using process::Continue;
using process::ControlFlow;
using process::Failure;
using process::Future;
using process::Owned;
using process::PID;
using process::Promise;
using process::Time;
using process::UPID;

using process::http::authentication::Principal;

namespace medios {
namespace internal {
namespace node {

using namespace state;

Framework::Framework(
    Node* _node,
    const Flags& nodeFlags,
    const FrameworkInfo& _info,
    const Option<UPID>& _pid)
  : state(RUNNING),
    node(_node),
    info(_info),
    capabilities(_info.capabilities()),
    pid(_pid),
    completedExecutors(nodeFlags.max_completed_executors_per_framework) {}


Framework::~Framework()
{
  // We own the non-completed executor pointers, so they need to be deleted.
  foreachvalue (Executor* executor, executors) {
    delete executor;
  }
}


bool Framework::idle() const
{
  return executors.empty() && pendingTasks.empty();
}


void Framework::checkpointFramework() const
{
  // Checkpoint the framework info.
  string path = paths::getFrameworkInfoPath(
      node->metaDir, node->info.id(), id());

  VLOG(1) << "Checkpointing FrameworkInfo to '" << path << "'";

  CHECK_SOME(state::checkpoint(path, info));

  // Checkpoint the framework pid, note that we checkpoint a
  // UPID() when it is None (for HTTP schedulers) because
  // 0.23.x nodes consider a missing pid file to be an
  // error.
  path = paths::getFrameworkPidPath(
      node->metaDir, node->info.id(), id());

  VLOG(1) << "Checkpointing framework pid"
          << " '" << pid.getOrElse(UPID()) << "'"
          << " to '" << path << "'";

  CHECK_SOME(state::checkpoint(path, pid.getOrElse(UPID())));
}


Try<Executor*> Framework::addExecutor(
  const ExecutorInfo& executorInfo,
  bool isGeneratedForCommandTask)
{
  // Verify that Resource.AllocationInfo is set, if coming
  // from a MULTI_ROLE controller this will be set, otherwise
  // the node will inject it when receiving the executor.
  foreach (const Resource& resource, executorInfo.resources()) {
    CHECK(resource.has_allocation_info());
  }

  // Generate an ID for the executor's container.
  // TODO(idownes) This should be done by the containerizer but we need the
  // ContainerID to create the executor's directory and generate the secret.
  // Consider fixing this since 'launchExecutor()' is handled asynchronously.
  ContainerID containerId;
  containerId.set_value(id::UUID::random().toString());

  Option<string> user = None();

  if (node->flags.switch_user) {
    // The command (either in form of task or executor command) can
    // define a specific user to run as. If present, this precedes the
    // framework user value. The selected user will have been verified by
    // the controller at this point through the active ACLs.
    // NOTE: The global invariant is that the executor info at this
    // point is (1) the user provided task.executor() or (2) a command
    // executor constructed by the node from the task.command().
    // If this changes, we need to check the user in both
    // task.command() and task.executor().command() below.
    user = info.user();
    if (executorInfo.command().has_user()) {
      user = executorInfo.command().user();
    }

    if (user.isSome()) {
      Result<uid_t> uid = os::getuid(user.get());

      if (uid.isError()) {
        return Error(
            "Failed to look up user '" + user.get() + "' on this node: " +
            uid.error());
      }

      if (uid.isNone()) {
        return Error(
            "User '" + user.get() + "' does not exist on this node");
      }
    }
  }

  // Create a directory for the executor.
  Try<string> directory = paths::createExecutorDirectory(
      node->flags.work_dir,
      node->info.id(),
      id(),
      executorInfo.executor_id(),
      containerId,
      user);

  if (directory.isError()) {
    return Error(directory.error());
  }

  Executor* executor = new Executor(
      node,
      id(),
      executorInfo,
      containerId,
      directory.get(),
      user,
      info.checkpoint(),
      isGeneratedForCommandTask);

  if (executor->checkpoint) {
    executor->checkpointExecutor();
  }

  CHECK(!executors.contains(executorInfo.executor_id()))
    << "Unknown executor '" << executorInfo.executor_id() << "'";

  executors[executorInfo.executor_id()] = executor;

  LOG(INFO) << "Launching executor '" << executorInfo.executor_id()
            << "' of framework " << id()
            << " with resources " << executorInfo.resources()
            << " in work directory '" << directory.get() << "'";

  const ExecutorID& executorId = executorInfo.executor_id();
  FrameworkID frameworkId = id();

  const PID<Node> nodePid = node->self();

  auto authorize =
    [nodePid, executorId, frameworkId](const Option<Principal>& principal) {
      return dispatch(
          nodePid,
          &Node::authorizeSandboxAccess,
          principal,
          frameworkId,
          executorId);
    };

  // We expose the executor's sandbox in the /files endpoint
  // via the following paths:
  //
  //  (1) /node_workdir/frameworks/FID/executors/EID/runs/CID
  //  (2) /node_workdir/frameworks/FID/executors/EID/runs/latest
  //  (3) /frameworks/FID/executors/EID/runs/latest
  //
  // Originally we just exposed the real path (1) and later
  // exposed the 'latest' symlink (2) since it's not easy for
  // users to know the run's container ID. We deprecated
  // (1) and (2) by exposing a virtual path (3) since we do not
  // want to expose the node's work directory and it's not
  // something users care about in this context.
  //
  // TODO(zhitao): Remove (1) and (2) per MESOS-7960 once we
  // pass 2.0. They remain now for backwards compatibility.
  const string latestPath = paths::getExecutorLatestRunPath(
      node->flags.work_dir,
      node->info.id(),
      id(),
      executorInfo.executor_id());

  const string virtualLatestPath = paths::getExecutorVirtualPath(
      id(),
      executorInfo.executor_id());

  node->files->attach(executor->directory, latestPath, authorize)
    .onAny(defer(
        node,
        &Node::fileAttached,
        lambda::_1,
        executor->directory,
        latestPath));

  node->files->attach(executor->directory, virtualLatestPath, authorize)
    .onAny(defer(
        node,
        &Node::fileAttached,
        lambda::_1,
        executor->directory,
        virtualLatestPath));

  node->files->attach(executor->directory, executor->directory, authorize)
    .onAny(defer(
        node,
        &Node::fileAttached,
        lambda::_1,
        executor->directory,
        executor->directory));

  return executor;
}


Executor* Framework::getExecutor(const ExecutorID& executorId) const
{
  if (executors.contains(executorId)) {
    return executors.at(executorId);
  }

  return nullptr;
}


Executor* Framework::getExecutor(const UPID& pid) const
{
  foreachvalue (Executor* executor, executors) {
    if (executor->pid.isSome() && executor->pid.get() == pid) {
      return executor;
    }
  }
  return nullptr;
}


Executor* Framework::getExecutorHoldingTask(const TaskID& taskId) const
{
  foreachvalue (Executor* executor, executors) {
    if (executor->hasTask(taskId)) {
      return executor;
    }
  }
  return nullptr;
}


Executor* Framework::getExecutor(const TaskID& taskId) const
{
  foreachvalue (Executor* executor, executors) {
    if (executor->queuedTasks.contains(taskId) ||
        executor->launchedTasks.contains(taskId) ||
        executor->terminatedTasks.contains(taskId)) {
      return executor;
    }
  }
  return nullptr;
}


void Framework::destroyExecutor(const ExecutorID& executorId)
{
  if (executors.contains(executorId)) {
    Executor* executor = executors[executorId];
    executors.erase(executorId);

    // See the declaration of `taskLaunchSequences` regarding its
    // lifecycle management.
    taskLaunchSequences.erase(executorId);

    // Pass ownership of the executor pointer.
    completedExecutors.push_back(Owned<Executor>(executor));
  }
}


void Framework::recoverExecutor(
    const ExecutorState& state,
    bool recheckpointExecutor,
    const hashset<TaskID>& tasksToRecheckpoint)
{
  LOG(INFO) << "Recovering executor '" << state.id
            << "' of framework " << id();

  CHECK_NOTNULL(node);

  if (state.runs.empty() || state.latest.isNone() || state.info.isNone()) {
    LOG(WARNING) << "Skipping recovery of executor '" << state.id
                 << "' of framework " << id()
                 << " because its latest run or executor info"
                 << " cannot be recovered";

    // GC the top level executor work directory.
    node->garbageCollect(paths::getExecutorPath(
        node->flags.work_dir, node->info.id(), id(), state.id));

    // GC the top level executor meta directory.
    node->garbageCollect(paths::getExecutorPath(
        node->metaDir, node->info.id(), id(), state.id));

    return;
  }

  // Verify that Resource.AllocationInfo is set, this should
  // be injected by the node when recovering.
  foreach (const Resource& resource, state.info->resources()) {
    CHECK(resource.has_allocation_info());
  }

  // We are only interested in the latest run of the executor!
  // So, we GC all the old runs.
  // NOTE: We don't schedule the top level executor work and meta
  // directories for GC here, because they will be scheduled when
  // the latest executor run terminates.
  const ContainerID& latest = state.latest.get();
  foreachvalue (const RunState& run, state.runs) {
    CHECK_SOME(run.id);
    const ContainerID& runId = run.id.get();
    if (latest != runId) {
      // GC the executor run's work directory.
      // TODO(vinod): Expose this directory to webui by recovering the
      // tasks and doing a 'files->attach()'.
      node->garbageCollect(paths::getExecutorRunPath(
          node->flags.work_dir, node->info.id(), id(), state.id, runId));

      // GC the executor run's meta directory.
      node->garbageCollect(paths::getExecutorRunPath(
          node->metaDir, node->info.id(), id(), state.id, runId));
    }
  }

  Option<RunState> run = state.runs.get(latest);
  CHECK_SOME(run)
      << "Cannot find latest run " << latest << " for executor " << state.id
      << " of framework " << id();

  // Create executor.
  const string directory = paths::getExecutorRunPath(
      node->flags.work_dir, node->info.id(), id(), state.id, latest);

  Executor* executor = new Executor(
      node,
      id(),
      state.info.get(),
      latest,
      directory,
      info.user(),
      info.checkpoint(),
      state.generatedForCommandTask);

  // Recover the libprocess PID if possible for PID based executors.
  if (run->http.isSome()) {
    if (!run->http.get()) {
      // When recovering in non-strict mode, the assumption is that the
      // node can die after checkpointing the forked pid but before the
      // libprocess pid. So, it is not possible for the libprocess pid
      // to exist but not the forked pid. If so, it is a really bad
      // situation (e.g., disk corruption).
      CHECK_SOME(run->forkedPid)
        << "Failed to get forked pid for executor " << state.id
        << " of framework " << id();

      executor->pid = run->libprocessPid.get();
    } else {
      // We set the PID to None() to signify that this is a HTTP based
      // executor.
      executor->pid = None();
    }
  } else {
    // We set the PID to UPID() to signify that the connection type for this
    // executor is unknown.
    executor->pid = UPID();
  }

  // And finally recover all the executor's tasks.
  foreachvalue (const TaskState& taskState, run->tasks) {
    executor->recoverTask(
        taskState,
        tasksToRecheckpoint.contains(taskState.id));
  }

  ExecutorID executorId = state.id;
  FrameworkID frameworkId = id();

  const PID<Node> nodePid = node->self();

  auto authorize =
    [nodePid, executorId, frameworkId](const Option<Principal>& principal) {
      return dispatch(
          nodePid,
          &Node::authorizeSandboxAccess,
          principal,
          frameworkId,
          executorId);
    };

  // We expose the executor's sandbox in the /files endpoint
  // via the following paths:
  //
  //  (1) /node_workdir/frameworks/FID/executors/EID/runs/CID
  //  (2) /node_workdir/frameworks/FID/executors/EID/runs/latest
  //  (3) /frameworks/FID/executors/EID/runs/latest
  //
  // Originally we just exposed the real path (1) and later
  // exposed the 'latest' symlink (2) since it's not easy for
  // users to know the run's container ID. We deprecated
  // (1) and (2) by exposing a virtual path (3) since we do not
  // want to expose the node's work directory and it's not
  // something users care about in this context.
  //
  // TODO(zhitao): Remove (1) and (2) per MESOS-7960 once we
  // pass 2.0. They remain now for backwards compatibility.
  const string latestPath = paths::getExecutorLatestRunPath(
      node->flags.work_dir,
      node->info.id(),
      id(),
      state.id);

  const string virtualLatestPath = paths::getExecutorVirtualPath(
      id(),
      state.id);

  node->files->attach(executor->directory, latestPath, authorize)
    .onAny(defer(
        node,
        &Node::fileAttached,
        lambda::_1,
        executor->directory,
        latestPath));

  node->files->attach(executor->directory, virtualLatestPath, authorize)
    .onAny(defer(
        node,
        &Node::fileAttached,
        lambda::_1,
        executor->directory,
        virtualLatestPath));

  // Expose the executor's files.
  node->files->attach(executor->directory, executor->directory, authorize)
    .onAny(defer(
        node,
        &Node::fileAttached,
        lambda::_1,
        executor->directory,
        executor->directory));

  // Add the executor to the framework.
  executors[executor->id] = executor;
  if (recheckpointExecutor) {
    executor->checkpointExecutor();
  }

  // If the latest run of the executor was completed (i.e., terminated
  // and all updates are acknowledged) in the previous run, we
  // transition its state to 'TERMINATED' and gc the directories.
  if (run->completed) {
    ++node->metrics.executors_terminated;

    executor->state = Executor::TERMINATED;

    CHECK_SOME(run->id);
    const ContainerID& runId = run->id.get();

    // GC the executor run's work directory.
    const string path = paths::getExecutorRunPath(
        node->flags.work_dir, node->info.id(), id(), state.id, runId);

    // NOTE: We keep a list of default executor tasks here to for
    // detaching task volume directories, since the executor may be
    // already destroyed when the GC completes (MESOS-8460).
    vector<Task> defaultExecutorTasks;
    if (executor->info.has_type() &&
        executor->info.type() == ExecutorInfo::DEFAULT) {
      foreachvalue (const Task* task, executor->launchedTasks) {
        defaultExecutorTasks.push_back(*task);
      }

      foreachvalue (const Task* task, executor->terminatedTasks) {
        defaultExecutorTasks.push_back(*task);
      }

      foreach (const shared_ptr<Task>& task, executor->completedTasks) {
        defaultExecutorTasks.push_back(*task);
      }
    }

    node->garbageCollect(path)
      .onAny(defer(node, &Node::detachFile, path))
      .onAny(defer(
          node,
          &Node::detachTaskVolumeDirectories,
          executor->info,
          executor->containerId,
          defaultExecutorTasks));

    // Make sure there are no "launched tasks" on a recovered completed
    // executor. We can only encounter these non-terminal terminated tasks
    // when recovering a checkpointed executor that is missing a terminal
    // status update. See MESOS-9750 for a one way to enter this state.
    foreachpair (
        const TaskID& taskId,
        Task* task,
        utils::copy(executor->launchedTasks)) {
      executor->launchedTasks.erase(taskId);
      executor->terminatedTasks[taskId] = task;
    }

    // GC the executor run's meta directory.
    node->garbageCollect(paths::getExecutorRunPath(
        node->metaDir, node->info.id(), id(), state.id, runId));

    // GC the top level executor work directory.
    node->garbageCollect(paths::getExecutorPath(
        node->flags.work_dir, node->info.id(), id(), state.id))
        .onAny(defer(node, &Node::detachFile, latestPath))
        .onAny(defer(node, &Node::detachFile, virtualLatestPath));

    // GC the top level executor meta directory.
    node->garbageCollect(paths::getExecutorPath(
        node->metaDir, node->info.id(), id(), state.id));

    // Move the executor to 'completedExecutors'.
    destroyExecutor(executor->id);
  }
}


void Framework::addPendingTask(
    const ExecutorID& executorId,
    const TaskInfo& task)
{
  pendingTasks[executorId][task.task_id()] = task;
}


void Framework::addPendingTaskGroup(
    const ExecutorID& executorId,
    const TaskGroupInfo& taskGroup)
{
  foreach (const TaskInfo& task, taskGroup.tasks()) {
    pendingTasks[executorId][task.task_id()] = task;
  }

  pendingTaskGroups.push_back(taskGroup);
}


bool Framework::hasTask(const TaskID& taskId) const
{
  foreachkey (const ExecutorID& executorId, pendingTasks) {
    if (pendingTasks.at(executorId).contains(taskId)) {
      return true;
    }
  }

  foreachvalue (Executor* executor, executors) {
    if (executor->queuedTasks.contains(taskId) ||
        executor->launchedTasks.contains(taskId) ||
        executor->terminatedTasks.contains(taskId)) {
      return true;
    }
  }

  return false;
}


bool Framework::isPending(const TaskID& taskId) const
{
  foreachkey (const ExecutorID& executorId, pendingTasks) {
    if (pendingTasks.at(executorId).contains(taskId)) {
      return true;
    }
  }

  return false;
}


Option<TaskGroupInfo> Framework::getTaskGroupForPendingTask(
    const TaskID& taskId)
{
  foreach (const TaskGroupInfo& taskGroup, pendingTaskGroups) {
    foreach (const TaskInfo& taskInfo, taskGroup.tasks()) {
      if (taskInfo.task_id() == taskId) {
        return taskGroup;
      }
    }
  }

  return None();
}


bool Framework::removePendingTask(const TaskID& taskId)
{
  bool removed = false;

  foreachkey (const ExecutorID& executorId, pendingTasks) {
    if (pendingTasks.at(executorId).contains(taskId)) {
      pendingTasks.at(executorId).erase(taskId);
      if (pendingTasks.at(executorId).empty()) {
        pendingTasks.erase(executorId);
      }

      removed = true;
      break;
    }
  }

  // We also remove the pending task group if all of its
  // tasks have been removed.
  for (auto it = pendingTaskGroups.begin();
       it != pendingTaskGroups.end();
       ++it) {
    foreach (const TaskInfo& t, it->tasks()) {
      if (t.task_id() == taskId) {
        // Found its task group, check if all tasks within
        // the group have been removed.
        bool allRemoved = true;

        foreach (const TaskInfo& t_, it->tasks()) {
          if (hasTask(t_.task_id())) {
            allRemoved = false;
            break;
          }
        }

        if (allRemoved) {
          pendingTaskGroups.erase(it);
        }

        return removed;
      }
    }
  }

  return removed;
}


Option<ExecutorID> Framework::getExecutorIdForPendingTask(
    const TaskID& taskId) const
{
  foreachkey (const ExecutorID& executorId, pendingTasks) {
    if (pendingTasks.at(executorId).contains(taskId)) {
      return executorId;
    }
  }

  return None();
}


Resources Framework::allocatedResources() const
{
  Resources allocated;

  foreachvalue (const Executor* executor, executors) {
    allocated += executor->allocatedResources();
  }

  hashset<ExecutorID> pendingExecutors;

  typedef hashmap<TaskID, TaskInfo> TaskMap;
  foreachvalue (const TaskMap& pendingTasks, pendingTasks) {
    foreachvalue (const TaskInfo& task, pendingTasks) {
      allocated += task.resources();

      ExecutorInfo executorInfo = node->getExecutorInfo(info, task);
      const ExecutorID& executorId = executorInfo.executor_id();

      if (!executors.contains(executorId) &&
          !pendingExecutors.contains(executorId)) {
        allocated += executorInfo.resources();
        pendingExecutors.insert(executorId);
      }
    }
  }

  return allocated;
}

ostream& operator<<(ostream& stream, Framework::State state)
{
  switch (state) {
    case Framework::RUNNING:     return stream << "RUNNING";
    case Framework::TERMINATING: return stream << "TERMINATING";
    default:                     return stream << "UNKNOWN";
  }
}

} // namespace node {
} // namespace internal {
} // namespace medios {
