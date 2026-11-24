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

#ifndef __LAUNCHER_DOCKER_EXECUTOR_DOCKER_EXEC_HPP__
#define __LAUNCHER_DOCKER_EXECUTOR_DOCKER_EXEC_HPP__

#include <map>
#include <string>
#include <utility>
#include <vector>

#include <google/protobuf/repeated_field.h>

#include <medios/medios.hpp>

#include <stout/lambda.hpp>
#include <stout/option.hpp>
#include <stout/try.hpp>

#include "node/stores.hpp"

namespace medios {
namespace internal {
namespace docker {

// Run an argv via libprocess subprocess, blocking the calling (worker) thread
// until the process exits. The docker executor already runs libprocess (the
// ExecutorDriver is a libprocess Process), so awaiting subprocess Futures from
// a worker thread adds no dependency and is the upstream pattern (cf.
// Docker::create's `version.await`). `os::execvpe` resolves the program on
// PATH. Adapted from upstream src/docker/docker.cpp (checkError/failure). #298.

// stdout -> /dev/null, stderr captured for diagnostics. Returns the exit status
// (None if the subprocess could not be started or produced no status) and the
// captured stderr.
std::pair<Option<int>, std::string> dockerCapture(
    const std::vector<std::string>& argv);

// stdout/stderr -> the task sandbox files (created if absent, appended). The
// stderr redirection means it cannot also be captured for the status message
// (the trade-off upstream documents in docker.cpp). Returns the exit status
// (None if it could not start).
//
// `forked` is called once the process exists and its sandbox files have
// been created, before this blocks on the exit status. A caller that has to
// act on the running container, rather than on the intent to run one, acts
// there.
Option<int> dockerRun(
    const std::vector<std::string>& argv,
    const std::string& sandboxDir,
    const Option<lambda::function<void()>>& forked = None());

// Render a subprocess exit status for a task status message. None means the
// process never produced a status — typically the program was not found on
// PATH.
std::string describeStatus(const Option<int>& status);


// The terminal status of a finished container run.
struct ContainerOutcome
{
  TaskState state;
  Option<TaskStatus::Reason> reason;
  std::string message;
};


// Decides the terminal status of a container whose `docker run` has
// returned `runStatus`.
//
// A framework kill is decided ahead of a stall. Once the framework has
// asked for the container to stop, the work value stops advancing because
// of that, so a stall seen afterwards is a consequence of the kill and not
// the reason the task ended. A kill of either kind is decided ahead of the
// exit status, because a container that handles SIGTERM and exits 0 did
// not finish its work.
ContainerOutcome containerOutcome(
    const Option<int>& runStatus,
    bool killIssued,
    bool killedByStall,
    const std::string& stallExplanation);

// The `--volume` arguments for a task's volumes, each a STORE volume
// resolved against the node's stores to `<path>:<target>[:ro]` (DR-036).
// The target is the container path when it is absolute, and
// `<sandboxDir>/<container_path>` when it is one name inside the sandbox
// (DR-036 revision). Fails, naming the entry, on a volume of any other
// source type or with the bare `host_path` field, an image volume, a
// store the node does not declare, any other relative path, a relative
// path with `sandboxDir` empty, and a relative target that already exists
// in the sandbox. The node's declared path was checked at its start;
// nothing here creates one.
Try<std::vector<std::string>> storeVolumeArguments(
    const google::protobuf::RepeatedPtrField<Volume>& volumes,
    const std::map<std::string, internal::node::StoreDeclaration>& stores,
    const std::string& sandboxDir);

// The `docker run` arguments for a task's `LinuxInfo.ipc_mode` and
// `LinuxInfo.shm_size`, under the rules the medios containerizer's
// `namespaces/ipc` isolator applies, so one task envelope means the same
// thing on both containerizers:
//
//   * neither field set: no arguments, and the container gets Docker's
//     own IPC namespace and /dev/shm (64 MB unless the daemon says
//     otherwise);
//   * `ipc_mode: PRIVATE`: `--ipc=private`;
//   * `shm_size: N` with `ipc_mode: PRIVATE`: also `--shm-size=Nm`.
//
// Fails, naming the field, on `shm_size` without `ipc_mode: PRIVATE`, as
// the medios containerizer does; on `shm_size: 0`; and on
// `ipc_mode: SHARE_PARENT`, which for a top-level task means the node's own
// IPC namespace (`--ipc=host`) and is refused rather than granted.
Try<std::vector<std::string>> ipcArguments(const ContainerInfo& container);


// The `docker run` arguments for a task's network, from its
// `ContainerInfo.docker.network` and its `ports` resources:
//
//   * `network` unset or `BRIDGE`: the container is on Docker's default
//     bridge, and each reserved port is published to the same host port
//     (`--publish p:p`);
//   * `network: HOST`: `--network=host` and no `--publish`, so the task
//     runs in the node's network namespace and binds its reserved ports
//     there, as under the medios containerizer. The ports stay the task's
//     resources; nothing stops it binding others.
//
// Presence is read rather than the value, because the proto's default for
// `network` is `HOST` and an unset field means the default bridge here.
//
// Fails, naming the value, on `NONE` and `USER`.
Try<std::vector<std::string>> networkArguments(const TaskInfo& task);


// The `docker stop --time` value, in whole seconds, for a task: its
// `kill_policy.grace_period` rounded up, so the container never gets less
// than the grace the framework asked for, or `defaultSeconds` when the task
// sets none. The controller has already refused a negative grace period.
int stopGraceSeconds(const TaskInfo& task, int defaultSeconds);


// The environment variable the docker_shell containerizer sets on this
// executor: the comma-separated Docker device indices it assigned to the
// task's `gpus`, empty when the task has none.
constexpr char GPU_DEVICES_ENVIRONMENT_VARIABLE[] =
  "MEDIOS_DOCKER_GPU_DEVICES";


// The `docker run` arguments that give a container exactly the GPUs the
// node assigned to it (`devices`, the value of
// `GPU_DEVICES_ENVIRONMENT_VARIABLE`), and no GPU when it assigned none.
//
// Both `--gpus` and `NVIDIA_VISIBLE_DEVICES` are set. A daemon whose default
// runtime is `nvidia` reads the variable, and CUDA images set it to `all`,
// so `--gpus` alone would leave every device visible there; the variable
// goes after the task's own environment so the task cannot widen it. With
// no devices it is `void`, which the NVIDIA runtime treats as a container
// without GPUs.
//
// Fails when the task asks for GPUs and the node assigned none (a node that
// does not set the variable), or when the number assigned is not the number
// the task was granted.
Try<std::vector<std::string>> gpuArguments(
    double grantedGpus,
    const Option<std::string>& devices);


// The `docker run` arguments for `ContainerInfo.docker.parameters` (#574),
// each rendered as `--<key>=<value>` before the image argument so a task
// can override `--entrypoint`, `--user`, etc. Only an explicit allowlist of
// safe task-level flags is permitted; any key outside the allowlist (such as
// `volume`, `gpus`, `device`, `cap-add`, `network`, `ipc`, `pid`, `detach`,
// `privileged`, or `name`) is refused.
Try<std::vector<std::string>> parameterArguments(
    const ContainerInfo::DockerInfo& docker);

} // namespace docker {
} // namespace internal {
} // namespace medios {

#endif // __LAUNCHER_DOCKER_EXECUTOR_DOCKER_EXEC_HPP__
