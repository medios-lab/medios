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

#ifndef __TASK_STATUS_UPDATE_MANAGER_HPP__
#define __TASK_STATUS_UPDATE_MANAGER_HPP__

#include <queue>
#include <string>

#include <medios/medios.hpp>

#include <process/future.hpp>
#include <process/pid.hpp>
#include <process/timeout.hpp>

#include <stout/hashset.hpp>
#include <stout/lambda.hpp>
#include <stout/option.hpp>
#include <stout/try.hpp>
#include <stout/uuid.hpp>

#include "messages/messages.hpp"

#include "node/flags.hpp"

namespace medios {
namespace internal {
namespace node {

// Forward declarations.

namespace state {
struct NodeState;
}

class TaskStatusUpdateManagerProcess;
struct TaskStatusUpdateStream;


// TaskStatusUpdateManager is responsible for
// 1) Reliably sending status updates to the controller.
// 2) Checkpointing the update to disk (optional).
// 3) Sending ACKs to the executor (optional).
// 4) Receiving ACKs from the scheduler.
class TaskStatusUpdateManager
{
public:
  TaskStatusUpdateManager(const Flags& flags);
  virtual ~TaskStatusUpdateManager();

  // Expects a callback 'forward' which gets called whenever there is
  // a new status update that needs to be forwarded to the controller.
  void initialize(const lambda::function<void(StatusUpdate)>& forward);

  // TODO(vinod): Come up with better names/signatures for the
  // checkpointing and non-checkpointing 'update()' functions.
  // Currently, it is not obvious that one version of 'update()'
  // does checkpointing while the other doesn't.

  // Checkpoints the status update and reliably sends the
  // update to the controller (and hence the scheduler).
  // @return Whether the update is handled successfully
  // (e.g. checkpointed).
  process::Future<Nothing> update(
      const StatusUpdate& update,
      const NodeID& nodeId,
      const ExecutorID& executorId,
      const ContainerID& containerId);

  // Retries the update to the controller (as long as the node is
  // alive), but does not checkpoint the update.
  // @return Whether the update is handled successfully.
  process::Future<Nothing> update(
      const StatusUpdate& update,
      const NodeID& nodeId);

  // Checkpoints the status update to disk if necessary.
  // Also, sends the next pending status update, if any.
  // @return True if the ACK is handled successfully (e.g., checkpointed)
  //              and the task's status update stream is not terminated.
  //         False same as above except the status update stream is terminated.
  //         Failed if there are any errors (e.g., duplicate, checkpointing).
  process::Future<bool> acknowledgement(
      const TaskID& taskId,
      const FrameworkID& frameworkId,
      const id::UUID& uuid);

  // Recover status updates.
  process::Future<Nothing> recover(
      const std::string& rootDir,
      const Option<state::NodeState>& state);


  // Pause sending updates.
  // This is useful when the node is disconnected because a
  // disconnected node will drop the updates.
  void pause();

  // Unpause and resend all the pending updates right away.
  // This is useful when the updates were pending because there was
  // no controller elected (e.g., during recovery) or framework failed over.
  void resume();

  // Closes all the status update streams corresponding to this framework.
  // NOTE: This stops retrying any pending status updates for this framework.
  void cleanup(const FrameworkID& frameworkId);

private:
  TaskStatusUpdateManagerProcess* process;
};


// TaskStatusUpdateStream handles the status updates and acknowledgements
// of a task, checkpointing them if necessary. It also holds the information
// about received, acknowledged and pending status updates.
// NOTE: A task is expected to have a globally unique ID across the lifetime
// of a framework. In other words the tuple (taskId, frameworkId) should be
// always unique.
struct TaskStatusUpdateStream
{
  TaskStatusUpdateStream(const TaskID& _taskId,
                     const FrameworkID& _frameworkId,
                     const NodeID& _nodeId,
                     const Flags& _flags,
                     bool _checkpoint,
                     const Option<ExecutorID>& executorId,
                     const Option<ContainerID>& containerId);

  ~TaskStatusUpdateStream();

  // This function handles the update, checkpointing if necessary.
  // @return   True if the update is successfully handled.
  //           False if the update is a duplicate.
  //           Error Any errors (e.g., checkpointing).
  Try<bool> update(const StatusUpdate& update);

  // This function handles the ACK, checkpointing if necessary.
  // @return   True if the acknowledgement is successfully handled.
  //           False if the acknowledgement is a duplicate.
  //           Error Any errors (e.g., checkpointing).
  Try<bool> acknowledgement(
      const TaskID& taskId,
      const FrameworkID& frameworkId,
      const id::UUID& uuid,
      const StatusUpdate& update);

  // Returns the next update (or none, if empty) in the queue.
  Result<StatusUpdate> next();

  // Replays the stream by sequentially handling an update and its
  // corresponding ACK, if present.
  Try<Nothing> replay(
      const std::vector<StatusUpdate>& updates,
      const hashset<id::UUID>& acks);

  // TODO(vinod): Explore semantics to make these private.
  const bool checkpoint;
  bool terminated;
  Option<process::Timeout> timeout; // Timeout for resending status update.
  std::queue<StatusUpdate> pending;

private:
  // Handles the status update and writes it to disk, if necessary.
  // TODO(vinod): The write has to be asynchronous to avoid status updates that
  // are being checkpointed, blocking the processing of other updates.
  // One solution is to wrap the protobuf::write inside async, but its probably
  // too much of an overhead to spin up a new libprocess per status update?
  // A better solution might be to be have async write capability for file io.
  Try<Nothing> handle(
      const StatusUpdate& update,
      const StatusUpdateRecord::Type& type);

  void _handle(
      const StatusUpdate& update,
      const StatusUpdateRecord::Type& type);

  const TaskID taskId;
  const FrameworkID frameworkId;
  const NodeID nodeId;

  const Flags flags;

  hashset<id::UUID> received;
  hashset<id::UUID> acknowledged;

  Option<std::string> path; // File path of the update stream.
  Option<int_fd> fd; // File descriptor to the update stream.

  Option<std::string> error; // Potential non-retryable error.
};

} // namespace node {
} // namespace internal {
} // namespace medios {


#endif // __TASK_STATUS_UPDATE_MANAGER_HPP__
