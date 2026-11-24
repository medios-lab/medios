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
#include "common/validation.hpp"
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

Executor::Executor(
    Node* _node,
    const FrameworkID& _frameworkId,
    const ExecutorInfo& _info,
    const ContainerID& _containerId,
    const string& _directory,
    const Option<string>& _user,
    bool _checkpoint,
    bool isGeneratedForCommandTask)
  : state(REGISTERING),
    node(_node),
    id(_info.executor_id()),
    info(_info),
    frameworkId(_frameworkId),
    containerId(_containerId),
    directory(_directory),
    user(_user),
    checkpoint(_checkpoint),
    http(None()),
    pid(None()),
    isGeneratedForCommandTask_(isGeneratedForCommandTask)
{
  CHECK_NOTNULL(node);

  // NOTE: This should be greater than zero because the node looks
  // for completed tasks to determine (with false positives) whether
  // an executor ever received tasks. See MESOS-8411.
  //
  // TODO(mzhu): Remove this check once we can determine whether an
  // executor ever received tasks without looking through the
  // completed tasks.
  static_assert(
      MAX_COMPLETED_TASKS_PER_EXECUTOR > 0,
      "Max completed tasks per executor should be greater than zero");

  completedTasks =
    circular_buffer<shared_ptr<Task>>(MAX_COMPLETED_TASKS_PER_EXECUTOR);
}


Executor::~Executor()
{
  if (http.isSome()) {
    closeHttpConnection();
  }

  // Delete the tasks.
  foreachvalue (Task* task, launchedTasks) {
    delete task;
  }
  foreachvalue (Task* task, terminatedTasks) {
    delete task;
  }
}


void Executor::enqueueTask(const TaskInfo& task)
{
  queuedTasks[task.task_id()] = task;
}


void Executor::enqueueTaskGroup(const TaskGroupInfo& taskGroup)
{
  foreach (const TaskInfo& task, taskGroup.tasks()) {
    queuedTasks[task.task_id()] = task;
  }

  queuedTaskGroups.push_back(taskGroup);
}


Option<TaskInfo> Executor::dequeueTask(const TaskID& taskId)
{
  Option<TaskInfo> taskInfo = queuedTasks.get(taskId);

  queuedTasks.erase(taskId);

  // Remove the task group if all of its tasks have been dequeued.
  for (auto it = queuedTaskGroups.begin(); it != queuedTaskGroups.end(); ++it) {
    foreach (const TaskInfo& t, it->tasks()) {
      if (t.task_id() == taskId) {
        // Found its task group, check if all tasks within
        // the group have been removed.
        bool allRemoved = true;

        foreach (const TaskInfo& t_, it->tasks()) {
          if (queuedTasks.contains(t_.task_id())) {
            allRemoved = false;
            break;
          }
        }

        if (allRemoved) {
          queuedTaskGroups.erase(it);
        }

        return taskInfo;
      }
    }
  }

  return taskInfo;
}


Task* Executor::addLaunchedTask(const TaskInfo& task)
{
  CHECK(!queuedTasks.contains(task.task_id()))
    << "Task " << task.task_id() << " was not dequeued";

  // The controller should enforce unique task IDs, but just in case
  // maybe we shouldn't make this a fatal error.
  CHECK(!launchedTasks.contains(task.task_id()))
    << "Duplicate task " << task.task_id();

  // Verify that Resource.AllocationInfo is set, if coming
  // from a MULTI_ROLE controller this will be set, otherwise
  // the node will inject it when receiving the task.
  foreach (const Resource& resource, task.resources()) {
    CHECK(resource.has_allocation_info());
  }

  // The node records no framework principal: the controller is the only
  // stamper of the on-behalf-of pair (DR-037), and a node-side Task is
  // the node's own bookkeeping, not the record.
  Task* t = new Task(
      protobuf::createTask(task, TASK_STAGING, frameworkId, None()));

  launchedTasks[task.task_id()] = t;

  if (info.has_type() && info.type() == ExecutorInfo::DEFAULT) {
    node->attachTaskVolumeDirectory(info, containerId, *t);
  }

  return t;
}


void Executor::completeTask(const TaskID& taskId)
{
  VLOG(1) << "Completing task " << taskId;

  CHECK(terminatedTasks.contains(taskId))
    << "Failed to find terminated task " << taskId;

  // If `completedTasks` is full and this is a default executor, we need
  // to detach the volume directory for the first task in `completedTasks`
  // before pushing a task into it, otherwise, we will never have chance
  // to do the detach for that task which would be a leak.
  if (info.has_type() &&
      info.type() == ExecutorInfo::DEFAULT &&
      completedTasks.full()) {
    const shared_ptr<Task>& firstTask = completedTasks.front();
    node->detachTaskVolumeDirectories(info, containerId, {*firstTask});
  }

  // Mark the task metadata (TaskInfo and status updates) for garbage
  // collection. This is important for keeping the metadata of long-lived,
  // multi-task executors within reasonable levels.
  if (checkpoint) {
    node->garbageCollect(paths::getTaskPath(
        node->metaDir,
        node->info.id(),
        frameworkId,
        id,
        containerId,
        taskId));
  }

  Task* task = terminatedTasks[taskId];
  completedTasks.push_back(shared_ptr<Task>(task));
  terminatedTasks.erase(taskId);
}


void Executor::checkpointExecutor()
{
  CHECK(checkpoint);

  // Checkpoint the executor info.
  const string path = paths::getExecutorInfoPath(
      node->metaDir, node->info.id(), frameworkId, id);

  VLOG(1) << "Checkpointing ExecutorInfo to '" << path << "'";

  CHECK_SOME(state::checkpoint(path, info));

  // Sync state of sentinel indicating whether the executor was
  // generated by the node.
  CHECK_SOME(state::checkpoint(
    paths::getExecutorGeneratedForCommandTaskPath(
        node->metaDir, node->info.id(), frameworkId, id),
    stringify(static_cast<int>(isGeneratedForCommandTask_)),
    true));


  // Create the meta executor directory.
  // NOTE: This creates the 'latest' symlink in the meta directory.
  Try<string> mkdir = paths::createExecutorDirectory(
      node->metaDir, node->info.id(), frameworkId, id, containerId);

  CHECK_SOME(mkdir);
}


void Executor::checkpointTask(const TaskInfo& task)
{
  checkpointTask(
      protobuf::createTask(task, TASK_STAGING, frameworkId, None()));
}


void Executor::checkpointTask(const Task& task)
{
  CHECK(checkpoint);

  const string path = paths::getTaskInfoPath(
      node->metaDir,
      node->info.id(),
      frameworkId,
      id,
      containerId,
      task.task_id());

  VLOG(1) << "Checkpointing TaskInfo to '" << path << "'";

  CHECK_SOME(state::checkpoint(path, task));
}


void Executor::recoverTask(const TaskState& state, bool recheckpointTask)
{
  if (state.info.isNone()) {
    LOG(WARNING) << "Skipping recovery of task " << state.id
                 << " because its info cannot be recovered";
    return;
  }

  // Verify that Resource.AllocationInfo is set, the node
  // should inject it during recovery.
  foreach (const Resource& resource, state.info->resources()) {
    CHECK(resource.has_allocation_info());
  }

  Task* task = new Task(state.info.get());
  if (recheckpointTask) {
    checkpointTask(*task);
  }

  launchedTasks[state.id] = task;

  if (info.has_type() && info.type() == ExecutorInfo::DEFAULT) {
    node->attachTaskVolumeDirectory(info, containerId, *task);
  }

  // Read updates to get the latest state of the task.
  foreach (const StatusUpdate& update, state.updates) {
    Try<Nothing> updated = updateTaskState(update.status());

    // TODO(bmahler): We only log this error because we used to
    // allow multiple terminal updates and so we may encounter
    // this when recovering an old executor. We can hard-CHECK
    // this 6 months from 1.1.0.
    if (updated.isError()) {
      LOG(ERROR) << "Failed to update state of recovered task"
                 << " '" << state.id << "' to " << update.status().state()
                 << ": " << updated.error();

      // The only case that should be possible here is when the
      // task had multiple terminal updates persisted.
      continue;
    }

    // Complete the task if it is terminal and
    // the update has been acknowledged.
    if (protobuf::isTerminalState(update.status().state())) {
      CHECK(update.has_uuid())
        << "Expecting updates without 'uuid' to have been rejected";

      if (state.acks.contains(id::UUID::fromBytes(update.uuid()).get())) {
        completeTask(state.id);
      }
      break;
    }
  }
}


void Executor::addPendingTaskStatus(const TaskStatus& status)
{
  auto uuid = id::UUID::fromBytes(status.uuid()).get();
  pendingStatusUpdates[status.task_id()][uuid] = status;
}


void Executor::removePendingTaskStatus(const TaskStatus& status)
{
  const TaskID& taskId = status.task_id();

  auto uuid = id::UUID::fromBytes(status.uuid()).get();

  if (!pendingStatusUpdates.contains(taskId) ||
      !pendingStatusUpdates[taskId].contains(uuid)) {
    LOG(WARNING) << "Unknown pending status update (uuid: " << uuid << ")";
    return;
  }

  pendingStatusUpdates[taskId].erase(uuid);

  if (pendingStatusUpdates[taskId].empty()) {
    pendingStatusUpdates.erase(taskId);
  }
}


Try<Nothing> Executor::updateTaskState(const TaskStatus& status)
{
  bool terminal = protobuf::isTerminalState(status.state());

  const TaskID& taskId = status.task_id();

  Task* task = nullptr;

  if (queuedTasks.contains(taskId)) {
    if (!terminal) {
      return Error("Cannot send non-terminal update for queued task");
    }

    TaskInfo taskInfo = CHECK_NOTNONE(dequeueTask(taskId));

    task = new Task(protobuf::createTask(
        taskInfo,
        status.state(),
        frameworkId,
        None()));
  } else if (launchedTasks.contains(taskId)) {
    task = launchedTasks.at(status.task_id());

    if (terminal) {
      if (pendingStatusUpdates.contains(status.task_id())) {
        auto statusUpdates = pendingStatusUpdates[status.task_id()].values();

        auto firstTerminal = std::find_if(
            statusUpdates.begin(),
            statusUpdates.end(),
            [](const TaskStatus& status) {
              return protobuf::isTerminalState(status.state());
            });

        CHECK(firstTerminal != statusUpdates.end());

        if (firstTerminal->uuid() != status.uuid()) {
          return Error("Unexpected terminal status update after first status"
                       " update " + stringify(firstTerminal->state()));
        }
      }

      launchedTasks.erase(taskId);
    }
  } else if (terminatedTasks.contains(taskId)) {
    return Error("Task is already terminated with state"
                 " " + stringify(terminatedTasks.at(taskId)->state()));
  } else {
    return Error("Task is unknown");
  }

  CHECK_NOTNULL(task);

  // Progress is validated here, before it is retained, because this record
  // is checkpointed and is what the node reports when it reregisters. A
  // value kept here without checking would reach the controller by that
  // route whatever the status-update path refuses.
  //
  // A report that fails, and a status that carries none at all, both leave
  // the last report that passed in place. The collapse below drops the
  // previous status of the same state, and a health or check update is
  // exactly that shape: the same state, a health reason, no progress of its
  // own, so without this the recorded value would go with it.
  bool keepProgress = false;
  if (status.has_progress()) {
    Option<Error> error =
      common::validation::validateTaskProgress(status.progress());

    if (error.isSome()) {
      LOG(WARNING) << "Dropping task progress reported for task " << taskId
                   << ": " << error->message;
    } else {
      keepProgress = true;
    }
  }

  Option<TaskStatus::Progress> recordedProgress = None();
  if (!keepProgress) {
    recordedProgress = protobuf::getTaskProgress(*task);
  }

  // TODO(brenden): Consider wiping the `data` and `message` fields?
  if (task->statuses_size() > 0 &&
      task->statuses(task->statuses_size() - 1).state() == status.state()) {
    task->mutable_statuses()->RemoveLast();
  }

  task->add_statuses()->CopyFrom(status);

  if (!keepProgress) {
    TaskStatus* retained = task->mutable_statuses(task->statuses_size() - 1);
    retained->clear_progress();

    if (recordedProgress.isSome()) {
      retained->mutable_progress()->CopyFrom(recordedProgress.get());
    }
  }

  task->set_state(status.state());

  // TODO(bmahler): This only increments the state when the update
  // can be handled. Should we always increment the state?
  if (terminal) {
    terminatedTasks[task->task_id()] = task;

    switch (status.state()) {
      case TASK_FINISHED: ++node->metrics.tasks_finished; break;
      case TASK_FAILED:   ++node->metrics.tasks_failed;   break;
      case TASK_KILLED:   ++node->metrics.tasks_killed;   break;
      case TASK_LOST:     ++node->metrics.tasks_lost;     break;
      case TASK_GONE:     ++node->metrics.tasks_gone;     break;
      default:
        LOG(ERROR) << "Unexpected terminal task state " << status.state();
        break;
    }
  }

  return Nothing();
}


bool Executor::incompleteTasks()
{
  return !queuedTasks.empty() ||
         !launchedTasks.empty() ||
         !terminatedTasks.empty();
}


bool Executor::hasTask(const TaskID& taskId) const
{
  if (queuedTasks.contains(taskId) ||
      launchedTasks.contains(taskId) ||
      terminatedTasks.contains(taskId)) {
    return true;
  }

  foreach (const shared_ptr<Task>& task, completedTasks) {
    if (task.get() != nullptr && task->task_id() == taskId) {
      return true;
    }
  }

  return false;
}


bool Executor::everSentTask() const
{
  if (!launchedTasks.empty()) {
    return true;
  }

  foreachvalue (Task* task, terminatedTasks) {
    foreach (const TaskStatus& status, task->statuses()) {
      if (status.source() == TaskStatus::SOURCE_EXECUTOR) {
        return true;
      }
    }
  }

  foreach (const shared_ptr<Task>& task, completedTasks) {
    foreach (const TaskStatus& status, task->statuses()) {
      if (status.source() == TaskStatus::SOURCE_EXECUTOR) {
        return true;
      }
    }
  }

  return false;
}


bool Executor::isGeneratedForCommandTask() const
{
  return isGeneratedForCommandTask_;
}


void Executor::closeHttpConnection()
{
  CHECK_SOME(http);

  if (!http->close()) {
    LOG(WARNING) << "Failed to close HTTP pipe for " << *this;
  }

  http = None();
}


Option<TaskGroupInfo> Executor::getQueuedTaskGroup(const TaskID& taskId)
{
  foreach (const TaskGroupInfo& taskGroup, queuedTaskGroups) {
    foreach (const TaskInfo& taskInfo, taskGroup.tasks()) {
      if (taskInfo.task_id() == taskId) {
        return taskGroup;
      }
    }
  }

  return None();
}


Resources Executor::allocatedResources() const
{
  Resources allocatedResources = info.resources();

  foreachvalue (const TaskInfo& task, queuedTasks) {
    allocatedResources += task.resources();
  }

  foreachvalue (const Task* task, launchedTasks) {
    allocatedResources += task->resources();
  }

  return allocatedResources;
}

ostream& operator<<(ostream& stream, const Executor& executor)
{
  stream << "'" << executor.id << "' of framework " << executor.frameworkId;

  if (executor.pid.isSome() && executor.pid.get()) {
    stream << " at " << executor.pid.get();
  } else if (executor.http.isSome() ||
             (executor.node->state == Node::RECOVERING &&
              executor.state == Executor::REGISTERING &&
              executor.http.isNone() && executor.pid.isNone())) {
    stream << " (via HTTP)";
  }

  return stream;
}

ostream& operator<<(ostream& stream, Executor::State state)
{
  switch (state) {
    case Executor::REGISTERING: return stream << "REGISTERING";
    case Executor::RUNNING:     return stream << "RUNNING";
    case Executor::TERMINATING: return stream << "TERMINATING";
    case Executor::TERMINATED:  return stream << "TERMINATED";
    default:                    return stream << "UNKNOWN";
  }
}

} // namespace node {
} // namespace internal {
} // namespace medios {
