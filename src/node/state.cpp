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
// limitations under the License

#include <glog/logging.h>

#include <iostream>

#include <process/pid.hpp>

#include <stout/check.hpp>
#include <stout/error.hpp>
#include <stout/foreach.hpp>
#include <stout/none.hpp>
#include <stout/nothing.hpp>
#include <stout/numify.hpp>
#include <stout/path.hpp>
#include <stout/protobuf.hpp>
#include <stout/strings.hpp>
#include <stout/try.hpp>

#include <stout/os/bootid.hpp>
#include <stout/os/close.hpp>
#include <stout/os/exists.hpp>
#include <stout/os/ftruncate.hpp>
#include <stout/os/int_fd.hpp>
#include <stout/os/ls.hpp>
#include <stout/os/lseek.hpp>
#include <stout/os/read.hpp>
#include <stout/os/realpath.hpp>
#include <stout/os/stat.hpp>

#include <stout/os/realpath.hpp>

#include "common/resources_utils.hpp"

#include "messages/messages.hpp"

#include "node/constants.hpp"
#include "node/paths.hpp"
#include "node/state.hpp"
#include "node/state.pb.h"

namespace medios {
namespace internal {
namespace node {
namespace state {

using std::list;
using std::max;
using std::string;


Try<State> recover(const string& rootDir, bool strict)
{
  LOG(INFO) << "Recovering state from '" << rootDir << "'";

  State state;

  // We consider the absence of 'rootDir' to mean that this is either
  // the first time this node was started or this node was started after
  // an upgrade (--recover=cleanup).
  if (!os::exists(rootDir)) {
    return state;
  }

  // Recover resources regardless whether the host has rebooted.
  Try<ResourcesState> resources = ResourcesState::recover(rootDir, strict);
  if (resources.isError()) {
    return Error(resources.error());
  }

  // TODO(jieyu): Do not set 'state.resources' if we cannot find the
  // resources checkpoint file.
  state.resources = resources.get();

  const string bootIdPath = paths::getBootIdPath(rootDir);
  if (os::exists(bootIdPath)) {
    Result<string> read = state::read<string>(bootIdPath);
    if (read.isError()) {
      LOG(WARNING) << "Failed to read '"
                   << bootIdPath << "': " << read.error();
    } else {
      Try<string> id = os::bootId();
      CHECK_SOME(id);

      if (id.get() != strings::trim(read.get())) {
        LOG(INFO) << "Node host rebooted";
        state.rebooted = true;
      }
    }
  }

  const string latest = paths::getLatestNodePath(rootDir);

  // Check if the "latest" symlink to a node directory exists.
  if (!os::exists(latest)) {
    // The node was asked to shutdown or died before it registered
    // and had a chance to create the "latest" symlink.
    LOG(INFO) << "Failed to find the latest node from '" << rootDir << "'";
    return state;
  }

  // Get the latest node id.
  Result<string> directory = os::realpath(latest);
  if (!directory.isSome()) {
    return Error("Failed to find latest node: " +
                 (directory.isError()
                  ? directory.error()
                  : "No such file or directory"));
  }

  NodeID nodeId;
  nodeId.set_value(Path(directory.get()).basename());

  Try<NodeState> node =
    NodeState::recover(rootDir, nodeId, strict, state.rebooted);

  if (node.isError()) {
    return Error(node.error());
  }

  state.node = node.get();

  return state;
}


Try<NodeState> NodeState::recover(
    const string& rootDir,
    const NodeID& nodeId,
    bool strict,
    bool rebooted)
{
  NodeState state;
  state.id = nodeId;

  // Read the node info.
  const string path = paths::getNodeInfoPath(rootDir, nodeId);
  if (!os::exists(path)) {
    // This could happen if the node died before it registered with
    // the controller.
    LOG(WARNING) << "Failed to find node info file '" << path << "'";
    return state;
  }

  Result<NodeInfo> nodeInfo = state::read<NodeInfo>(path);

  if (nodeInfo.isError()) {
    const string message = "Failed to read node info from '" + path + "': " +
                            nodeInfo.error();
    if (strict) {
      return Error(message);
    } else {
      LOG(WARNING) << message;
      state.errors++;
      return state;
    }
  }

  if (nodeInfo.isNone()) {
    // This could happen if the node is hard rebooted after the file is created
    // but before the data is synced on disk.
    LOG(WARNING) << "Found empty node info file '" << path << "'";
    return state;
  }

  state.info = nodeInfo.get();

  // Find the frameworks.
  Try<list<string>> frameworks = paths::getFrameworkPaths(rootDir, nodeId);

  if (frameworks.isError()) {
    return Error("Failed to find frameworks for node " + nodeId.value() +
                 ": " + frameworks.error());
  }

  // Recover each of the frameworks.
  foreach (const string& path, frameworks.get()) {
    FrameworkID frameworkId;
    frameworkId.set_value(Path(path).basename());

    Try<FrameworkState> framework =
      FrameworkState::recover(rootDir, nodeId, frameworkId, strict, rebooted);

    if (framework.isError()) {
      return Error("Failed to recover framework " + frameworkId.value() +
                   ": " + framework.error());
    }

    state.frameworks[frameworkId] = framework.get();
    state.errors += framework->errors;
  }

  // Recover any drain state.
  const string drainConfigPath = paths::getDrainConfigPath(rootDir, nodeId);
  if (os::exists(drainConfigPath)) {
    Result<DrainConfig> drainConfig = state::read<DrainConfig>(drainConfigPath);
    if (drainConfig.isError()) {
      string message = "Failed to read node state file '"
                       + drainConfigPath + "': " + drainConfig.error();

      LOG(WARNING) << message;
      state.errors++;
    }
    if (drainConfig.isSome()) {
      state.drainConfig = *drainConfig;
    }
  }

  // Operations might be checkpointed in either the target resource state file
  // or in the final resource state checkpoint location. If the target file
  // exists, then the node must have crashed while attempting to sync those
  // target resources to disk. Since node recovery guarantees that the node
  // will not successfully recover until the target resources are synced to disk
  // and moved to the final checkpoint location, here we can safely recover
  // operations from the target file if it exists.

  const string targetPath = paths::getResourceStateTargetPath(rootDir);
  const string resourceStatePath = paths::getResourceStatePath(rootDir);

  if (os::exists(targetPath)) {
    Result<ResourceState> target = state::read<ResourceState>(targetPath);
    if (target.isError()) {
      string message = "Failed to read resources and operations target file '" +
                       targetPath + "': " + target.error();

      if (strict) {
        return Error(message);
      } else {
        LOG(WARNING) << message;
        state.errors++;
        return state;
      }
    }

    if (target.isSome()) {
      state.operations = std::vector<Operation>();
      foreach (const Operation& operation, target->operations()) {
        state.operations->push_back(operation);
      }
    }
  } else if (os::exists(resourceStatePath)) {
    Result<ResourceState> resourceState =
      state::read<ResourceState>(resourceStatePath);
    if (resourceState.isError()) {
      string message = "Failed to read resource and operations file '" +
                       resourceStatePath + "': " + resourceState.error();

      if (strict) {
        return Error(message);
      } else {
        LOG(WARNING) << message;
        state.errors++;
        return state;
      }
    }

    if (resourceState.isSome()) {
      state.operations = std::vector<Operation>();
      foreach (const Operation& operation, resourceState->operations()) {
        state.operations->push_back(operation);
      }
    }
  }

  return state;
}


Try<FrameworkState> FrameworkState::recover(
    const string& rootDir,
    const NodeID& nodeId,
    const FrameworkID& frameworkId,
    bool strict,
    bool rebooted)
{
  FrameworkState state;
  state.id = frameworkId;
  string message;

  // Read the framework info.
  string path = paths::getFrameworkInfoPath(rootDir, nodeId, frameworkId);
  if (!os::exists(path)) {
    // This could happen if the node died after creating the
    // framework directory but before it checkpointed the framework
    // info.
    LOG(WARNING) << "Failed to find framework info file '" << path << "'";
    return state;
  }

  const Result<FrameworkInfo> frameworkInfo = state::read<FrameworkInfo>(path);

  if (frameworkInfo.isError()) {
    message = "Failed to read framework info from '" + path + "': " +
              frameworkInfo.error();

    if (strict) {
      return Error(message);
    } else {
      LOG(WARNING) << message;
      state.errors++;
      return state;
    }
  }

  if (frameworkInfo.isNone()) {
    // This could happen if the node is hard rebooted after the file is created
    // but before the data is synced on disk.
    LOG(WARNING) << "Found empty framework info file '" << path << "'";
    return state;
  }

  state.info = frameworkInfo.get();

  // Read the framework pid.
  path = paths::getFrameworkPidPath(rootDir, nodeId, frameworkId);
  if (!os::exists(path)) {
    // This could happen if the node died after creating the
    // framework info but before it checkpointed the framework pid.
    LOG(WARNING) << "Failed to framework pid file '" << path << "'";
    return state;
  }

  Result<string> pid = state::read<string>(path);

  if (pid.isError()) {
    message =
      "Failed to read framework pid from '" + path + "': " + pid.error();

    if (strict) {
      return Error(message);
    } else {
      LOG(WARNING) << message;
      state.errors++;
      return state;
    }
  }

  if (pid->empty()) {
    // This could happen if the node is hard rebooted after the file is created
    // but before the data is synced on disk.
    LOG(WARNING) << "Found empty framework pid file '" << path << "'";
    return state;
  }

  state.pid = process::UPID(pid.get());

  // Find the executors.
  Try<list<string>> executors =
    paths::getExecutorPaths(rootDir, nodeId, frameworkId);

  if (executors.isError()) {
    return Error(
        "Failed to find executors for framework " + frameworkId.value() +
        ": " + executors.error());
  }

  // Recover the executors.
  foreach (const string& path, executors.get()) {
    ExecutorID executorId;
    executorId.set_value(Path(path).basename());

    Try<ExecutorState> executor = ExecutorState::recover(
        rootDir, nodeId, frameworkId, executorId, strict, rebooted);

    if (executor.isError()) {
      return Error("Failed to recover executor '" + executorId.value() +
                   "': " + executor.error());
    }

    state.executors[executorId] = executor.get();
    state.errors += executor->errors;
  }

  return state;
}


Try<ExecutorState> ExecutorState::recover(
    const string& rootDir,
    const NodeID& nodeId,
    const FrameworkID& frameworkId,
    const ExecutorID& executorId,
    bool strict,
    bool rebooted)
{
  ExecutorState state;
  state.id = executorId;
  string message;

  // Find the runs.
  Try<list<string>> runs = paths::getExecutorRunPaths(
      rootDir,
      nodeId,
      frameworkId,
      executorId);

  if (runs.isError()) {
    return Error("Failed to find runs for executor '" + executorId.value() +
                 "': " + runs.error());
  }

  // Recover the runs.
  foreach (const string& path, runs.get()) {
    if (Path(path).basename() == paths::LATEST_SYMLINK) {
      const Result<string> latest = os::realpath(path);
      if (latest.isNone()) {
        // This can happen if the node died between garbage collecting the
        // executor latest run and garbage collecting the top level executor
        // meta directory, containing the "latest" symlink.
        LOG(WARNING) << "Dangling 'latest' run symlink of executor '"
                     << executorId << "'";
        continue;
      } else if (latest.isError()) {
        return Error(
            "Failed to find latest run of executor '" +
            executorId.value() + "': " + latest.error());
      }

      // Store the ContainerID of the latest executor run.
      ContainerID containerId;
      containerId.set_value(Path(latest.get()).basename());
      state.latest = containerId;
    } else {
      ContainerID containerId;
      containerId.set_value(Path(path).basename());

      Try<RunState> run = RunState::recover(
          rootDir,
          nodeId,
          frameworkId,
          executorId,
          containerId,
          strict,
          rebooted);

      if (run.isError()) {
        return Error(
            "Failed to recover run " + containerId.value() +
            " of executor '" + executorId.value() +
            "': " + run.error());
      }

      state.runs[containerId] = run.get();
      state.errors += run->errors;
    }
  }

  // Find the latest executor.
  // It is possible that we cannot find the "latest" executor if the
  // node died before it created the "latest" symlink.
  if (state.latest.isNone()) {
    LOG(WARNING) << "Failed to find the latest run of executor '"
                 << executorId << "' of framework " << frameworkId;
    return state;
  }

  // Read the executor info.
  const string path =
    paths::getExecutorInfoPath(rootDir, nodeId, frameworkId, executorId);
  if (!os::exists(path)) {
    // This could happen if the node died after creating the executor
    // directory but before it checkpointed the executor info.
    LOG(WARNING) << "Failed to find executor info file '" << path << "'";
    return state;
  }

  Result<ExecutorInfo> executorInfo = state::read<ExecutorInfo>(path);

  if (executorInfo.isError()) {
    message = "Failed to read executor info from '" + path + "': " +
              executorInfo.error();

    if (strict) {
      return Error(message);
    } else {
      LOG(WARNING) << message;
      state.errors++;
      return state;
    }
  }

  if (executorInfo.isNone()) {
    // This could happen if the node is hard rebooted after the file is created
    // but before the data is synced on disk.
    LOG(WARNING) << "Found empty executor info file '" << path << "'";
    return state;
  }

  state.info = executorInfo.get();

  const string executorGeneratedForCommandTaskPath =
    paths::getExecutorGeneratedForCommandTaskPath(
        rootDir, nodeId, frameworkId, executorId);

  if (os::exists(executorGeneratedForCommandTaskPath)) {
    Try<string> read = os::read(executorGeneratedForCommandTaskPath);

    if (read.isError()) {
      return Error(
          "Could not read '" + executorGeneratedForCommandTaskPath + "': " +
          read.error());
    }

    Try<int> generatedForCommandTask = numify<int>(*read);

    if (generatedForCommandTask.isError()) {
      return Error(
          "Could not parse '" + executorGeneratedForCommandTaskPath + "': " +
          generatedForCommandTask.error());
    }

    state.generatedForCommandTask = *generatedForCommandTask;
  } else {
    // If we did not find persisted information on whether this executor was
    // generated by the node, use a heuristic.
    //
    // TODO(bbannier): Remove this code once we do not need to support
    // versions anymore which do not persist this information.
    // TODO(jieyu): The way we determine if an executor is generated for
    // a command task is really hacky. We rely on the fact that the command
    // executor launch command is set by the containerizer so that this check is
    // still valid
    // in the node.
    state.generatedForCommandTask =
      strings::endsWith(executorInfo->command().value(), MEDIOS_EXECUTOR) &&
      strings::startsWith(executorInfo->name(), "Command Executor");
  }

  return state;
}


Try<RunState> RunState::recover(
    const string& rootDir,
    const NodeID& nodeId,
    const FrameworkID& frameworkId,
    const ExecutorID& executorId,
    const ContainerID& containerId,
    bool strict,
    bool rebooted)
{
  RunState state;
  state.id = containerId;
  string message;

  // See if the sentinel file exists. This is done first so it is
  // known even if partial state is returned, e.g., if the libprocess
  // pid file is not recovered. It indicates the node removed the
  // executor.
  string path = paths::getExecutorSentinelPath(
      rootDir, nodeId, frameworkId, executorId, containerId);

  state.completed = os::exists(path);

  // Find the tasks.
  Try<list<string>> tasks = paths::getTaskPaths(
      rootDir,
      nodeId,
      frameworkId,
      executorId,
      containerId);

  if (tasks.isError()) {
    return Error(
        "Failed to find tasks for executor run " + containerId.value() +
        ": " + tasks.error());
  }

  // Recover tasks.
  foreach (const string& path, tasks.get()) {
    TaskID taskId;
    taskId.set_value(Path(path).basename());

    Try<TaskState> task = TaskState::recover(
        rootDir, nodeId, frameworkId, executorId, containerId, taskId, strict);

    if (task.isError()) {
      return Error(
          "Failed to recover task " + taskId.value() + ": " + task.error());
    }

    state.tasks[taskId] = task.get();
    state.errors += task->errors;
  }

  path = paths::getForkedPidPath(
      rootDir, nodeId, frameworkId, executorId, containerId);

  // If node host is rebooted, we do not read the forked pid and libprocess pid
  // since those two pids are obsolete after reboot. And we remove the forked
  // pid file to make sure we will not read it in the case the node process is
  // restarted after we checkpoint the new boot ID in `Node::__recover` (i.e.,
  // node recovery is done after the reboot).
  if (rebooted) {
    if (os::exists(path)) {
      Try<Nothing> rm = os::rm(path);
      if (rm.isError()) {
        return Error(
            "Failed to remove executor forked pid file '" + path + "': " +
            rm.error());
      }
    }

    return state;
  }

  if (!os::exists(path)) {
    // This could happen if the node died before the containerizer checkpointed
    // the forked pid or node process is restarted after node host is rebooted
    // since we remove this file in the above code.
    LOG(WARNING) << "Failed to find executor forked pid file '" << path << "'";
    return state;
  }

  // Read the forked pid.
  Result<string> pid = state::read<string>(path);
  if (pid.isError()) {
    message = "Failed to read executor forked pid from '" + path +
              "': " + pid.error();

    if (strict) {
      return Error(message);
    } else {
      LOG(WARNING) << message;
      state.errors++;
      return state;
    }
  }

  if (pid->empty()) {
    // This could happen if the node is hard rebooted after the file is created
    // but before the data is synced on disk.
    LOG(WARNING) << "Found empty executor forked pid file '" << path << "'";
    return state;
  }

  Try<pid_t> forkedPid = numify<pid_t>(pid.get());
  if (forkedPid.isError()) {
    return Error("Failed to parse forked pid '" + pid.get() + "' "
                 "from pid file '" + path + "': " +
                 forkedPid.error());
  }

  state.forkedPid = forkedPid.get();

  // Read the libprocess pid.
  path = paths::getLibprocessPidPath(
      rootDir, nodeId, frameworkId, executorId, containerId);

  if (os::exists(path)) {
    pid = state::read<string>(path);

    if (pid.isError()) {
      message = "Failed to read executor libprocess pid from '" + path +
                "': " + pid.error();

      if (strict) {
        return Error(message);
      } else {
        LOG(WARNING) << message;
        state.errors++;
        return state;
      }
    }

    if (pid->empty()) {
      // This could happen if the node is hard rebooted after the file is
      // created but before the data is synced on disk.
      LOG(WARNING) << "Found empty executor libprocess pid file '" << path
                   << "'";
      return state;
    }

    state.libprocessPid = process::UPID(pid.get());
    state.http = false;

    return state;
  }

  path = paths::getExecutorHttpMarkerPath(
      rootDir, nodeId, frameworkId, executorId, containerId);

  // The marker could be absent if the node died before the executor
  // registered with the node.
  if (!os::exists(path)) {
    LOG(WARNING) << "Failed to find '" << paths::LIBPROCESS_PID_FILE
                 << "' or '" << paths::HTTP_MARKER_FILE
                 << "' for container " << containerId
                 << " of executor '" << executorId
                 << "' of framework " << frameworkId;
    return state;
  }

  state.http = true;
  return state;
}


Try<TaskState> TaskState::recover(
    const string& rootDir,
    const NodeID& nodeId,
    const FrameworkID& frameworkId,
    const ExecutorID& executorId,
    const ContainerID& containerId,
    const TaskID& taskId,
    bool strict)
{
  TaskState state;
  state.id = taskId;
  string message;

  // Read the task info.
  string path = paths::getTaskInfoPath(
      rootDir, nodeId, frameworkId, executorId, containerId, taskId);
  if (!os::exists(path)) {
    // This could happen if the node died after creating the task
    // directory but before it checkpointed the task info.
    LOG(WARNING) << "Failed to find task info file '" << path << "'";
    return state;
  }

  Result<Task> task = state::read<Task>(path);

  if (task.isError()) {
    message = "Failed to read task info from '" + path + "': " + task.error();

    if (strict) {
      return Error(message);
    } else {
      LOG(WARNING) << message;
      state.errors++;
      return state;
    }
  }

  if (task.isNone()) {
    // This could happen if the node is hard rebooted after the file is created
    // but before the data is synced on disk.
    LOG(WARNING) << "Found empty task info file '" << path << "'";
    return state;
  }

  state.info = task.get();

  // Read the status updates.
  path = paths::getTaskUpdatesPath(
      rootDir, nodeId, frameworkId, executorId, containerId, taskId);
  if (!os::exists(path)) {
    // This could happen if the node died before it checkpointed any
    // status updates for this task.
    LOG(WARNING) << "Failed to find status updates file '" << path << "'";
    return state;
  }

  // Open the status updates file for reading and writing (for
  // truncating).
  Try<int_fd> fd = os::open(path, O_RDWR | O_CLOEXEC);
  if (fd.isError()) {
    message = "Failed to open status updates file '" + path +
              "': " + fd.error();

    if (strict) {
      return Error(message);
    } else {
      LOG(WARNING) << message;
      state.errors++;
      return state;
    }
  }

  // Now, read the updates.
  Result<StatusUpdateRecord> record = None();
  while (true) {
    // Ignore errors due to partial protobuf read and enable undoing
    // failed reads by reverting to the previous seek position.
    record = ::protobuf::read<StatusUpdateRecord>(fd.get(), true, true);

    if (!record.isSome()) {
      break;
    }

    if (record->type() == StatusUpdateRecord::UPDATE) {
      state.updates.push_back(record->update());
    } else {
      state.acks.insert(id::UUID::fromBytes(record->uuid()).get());
    }
  }

  Try<off_t> lseek = os::lseek(fd.get(), 0, SEEK_CUR);
  if (lseek.isError()) {
    os::close(fd.get());
    return Error(
        "Failed to lseek status updates file '" + path + "':" + lseek.error());
  }

  off_t offset = lseek.get();

  // Always truncate the file to contain only valid updates.
  // NOTE: This is safe even though we ignore partial protobuf read
  // errors above, because the 'fd' is properly set to the end of the
  // last valid update by 'protobuf::read()'.
  Try<Nothing> truncated = os::ftruncate(fd.get(), offset);

  if (truncated.isError()) {
    os::close(fd.get());
    return Error(
        "Failed to truncate status updates file '" + path +
        "': " + truncated.error());
  }

  // After reading a non-corrupted updates file, 'record' should be
  // 'none'.
  if (record.isError()) {
    message = "Failed to read status updates file  '" + path +
              "': " + record.error();

    os::close(fd.get());

    if (strict) {
      return Error(message);
    } else {
      LOG(WARNING) << message;
      state.errors++;
      return state;
    }
  }

  // Close the updates file.
  os::close(fd.get());

  return state;
}


Try<ResourcesState> ResourcesState::recover(
    const string& rootDir,
    bool strict)
{
  ResourcesState state;

  // The checkpointed resources may exist in one of two different formats:
  // 1) Pre-operation-feedback, where only resources are written to a target
  //    file, then moved to the final checkpoint location once any persistent
  //    volumes have been committed to disk.
  // 2) Post-operation-feedback, where both resources and operations are written
  //    to a target file, then moved to the final checkpoint location once any
  //    persistent volumes have been committed to disk.
  //
  // The post-operation-feedback node writes both of these formats to disk to
  // enable node downgrades.

  const string resourceStatePath = paths::getResourceStatePath(rootDir);
  if (os::exists(resourceStatePath)) {
    // The post-operation-feedback format was detected.
    Result<ResourceState> resourceState =
      state::read<ResourceState>(resourceStatePath);
    if (resourceState.isError()) {
      string message = "Failed to read resource and operations file '" +
                       resourceStatePath + "': " + resourceState.error();

      if (strict) {
        return Error(message);
      } else {
        LOG(WARNING) << message;
        state.errors++;
        return state;
      }
    }

    if (resourceState.isSome()) {
      state.resources = resourceState->resources();
    }

    const string targetPath = paths::getResourceStateTargetPath(rootDir);
    if (!os::exists(targetPath)) {
      return state;
    }

    Result<ResourceState> target = state::read<ResourceState>(targetPath);
    if (target.isError()) {
      string message =
        "Failed to read resources and operations target file '" +
        targetPath + "': " + target.error();

      if (strict) {
        return Error(message);
      } else {
        LOG(WARNING) << message;
        state.errors++;
        return state;
      }
    }

    if (target.isSome()) {
      state.target = target->resources();
    }

    return state;
  }

  // Falling back to the pre-operation-feedback format.

  LOG(INFO) << "No committed checkpointed resources and operations found at '"
            << resourceStatePath << "'";

  // Process the committed resources.
  const string infoPath = paths::getResourcesInfoPath(rootDir);
  if (!os::exists(infoPath)) {
    LOG(INFO) << "No committed checkpointed resources found at '"
              << infoPath << "'";
    return state;
  }

  Result<Resources> info = state::read<Resources>(infoPath);
  if (info.isError()) {
    string message =
      "Failed to read resources file '" + infoPath + "': " + info.error();

    if (strict) {
      return Error(message);
    } else {
      LOG(WARNING) << message;
      state.errors++;
      return state;
    }
  }

  if (info.isSome()) {
    state.resources = info.get();
  }

  // Process the target resources.
  const string targetPath = paths::getResourcesTargetPath(rootDir);
  if (!os::exists(targetPath)) {
    return state;
  }

  Result<Resources> target = state::read<Resources>(targetPath);
  if (target.isError()) {
    string message =
      "Failed to read resources file '" + targetPath + "': " + target.error();

    if (strict) {
      return Error(message);
    } else {
      LOG(WARNING) << message;
      state.errors++;
      return state;
    }
  }

  if (target.isSome()) {
    state.target = target.get();
  }

  return state;
}

} // namespace state {
} // namespace node {
} // namespace internal {
} // namespace medios {
