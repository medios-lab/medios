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

#include "node/containerizer/docker_shell/docker_shell_containerizer.hpp"

#include <signal.h>

#include <iterator>
#include <memory>
#include <set>
#include <utility>
#include <vector>

#include <process/dispatch.hpp>
#include <process/future.hpp>
#include <process/id.hpp>
#include <process/process.hpp>
#include <process/reap.hpp>
#include <process/subprocess.hpp>

#include <stout/foreach.hpp>
#include <stout/path.hpp>
#include <stout/stringify.hpp>
#include <stout/strings.hpp>

#include "launcher/docker_executor/docker_exec.hpp"

using std::map;
using std::string;
using std::vector;

using process::Failure;
using process::Future;
using process::Owned;
using process::Promise;
using process::Subprocess;

namespace medios {
namespace internal {
namespace node {

namespace {

constexpr const char MEDIOS_DOCKER_EXECUTOR[] = "medios-docker-executor";

struct Container
{
  ContainerID id;
  pid_t pid = -1;
  // Docker device indices assigned to this container, returned when its
  // executor is reaped.
  std::vector<unsigned> gpus;
  Promise<Option<medios::node::ContainerTermination>> termination;
};

} // namespace


class DockerShellContainerizerProcess
  : public process::Process<DockerShellContainerizerProcess>
{
public:
  DockerShellContainerizerProcess(const Flags& _flags, unsigned gpuCount)
    : ProcessBase(process::ID::generate("docker-shell-containerizer")),
      flags(_flags)
  {
    for (unsigned device = 0; device < gpuCount; ++device) {
      freeGpus.insert(device);
    }
  }

  Future<Nothing> recover(const Option<state::NodeState>& /*state*/)
  {
    // D1 / #149: deferred. Framework-level recovery is the primary model
    // for alpha; this containerizer does not re-attach to running Docker
    // containers across node restarts.
    LOG(INFO) << "[docker_shell] recover() (no orphan reconciliation)";
    return Nothing();
  }

  Future<Containerizer::LaunchResult> launch(
      const ContainerID& containerId,
      const medios::node::ContainerConfig& containerConfig,
      const map<string, string>& environment,
      const Option<string>& /*pidCheckpointPath*/)
  {
    if (containerId.has_parent()) {
      return Failure("Nested containers are not supported by docker_shell");
    }
    if (containers_.contains(containerId)) {
      return Failure("Container already started: " + containerId.value());
    }

    // Only launch Docker tasks. If a task arrives without container.docker,
    // the node should have routed it to a different containerizer (we don't
    // currently support composing containerizers — single backend per node).
    if (!containerConfig.has_container_info() ||
        containerConfig.container_info().type() !=
            ContainerInfo::DOCKER) {
      return Containerizer::LaunchResult::NOT_SUPPORTED;
    }

    // The node fills executor_info.command with the host-side command
    // executor (medios-executor). We ignore it: medios-docker-executor IS
    // the executor for Docker tasks. The user's actual task command lives
    // in containerConfig.task_info().command() and is read by
    // medios-docker-executor itself when it receives launchTask().
    // The task's `gpus` grant becomes specific devices here, because only
    // the node knows which of its devices are in use. The controller never
    // grants more than the node advertises, so a shortfall means this
    // ledger and the controller disagree, and the launch fails rather than
    // sharing a device.
    const double granted = Resources(containerConfig.resources())
      .get<Value::Scalar>("gpus")
      .getOrElse(Value::Scalar())
      .value();

    if (granted != static_cast<double>(static_cast<unsigned>(granted))) {
      return Failure(
          "docker_shell assigns whole GPUs; the task was granted " +
          stringify(granted));
    }

    const unsigned requested = static_cast<unsigned>(granted);
    if (requested > freeGpus.size()) {
      return Failure(
          "The task was granted " + stringify(requested) + " gpus but only " +
          stringify(freeGpus.size()) + " of this node's devices are free");
    }

    // Lowest free indices first.
    vector<unsigned> gpus(
        freeGpus.begin(), std::next(freeGpus.begin(), requested));

    vector<string> devices;
    foreach (unsigned device, gpus) {
      devices.push_back(stringify(device));
    }

    map<string, string> executorEnvironment = environment;
    executorEnvironment[
        medios::internal::docker::GPU_DEVICES_ENVIRONMENT_VARIABLE] =
      strings::join(",", devices);

    const string path =
        ::path::join(flags.launcher_dir, MEDIOS_DOCKER_EXECUTOR);
    vector<string> argv = {MEDIOS_DOCKER_EXECUTOR};

    LOG(INFO) << "[docker_shell] launch " << containerId.value()
              << " — forking " << path;

    Try<Subprocess> s = process::subprocess(
        path,
        argv,
        Subprocess::FD(STDIN_FILENO),
        Subprocess::FD(STDOUT_FILENO),
        Subprocess::FD(STDERR_FILENO),
        nullptr,
        executorEnvironment);

    if (s.isError()) {
      return Failure("Failed to fork medios-docker-executor: " + s.error());
    }

    auto container = std::make_shared<Container>();
    container->id = containerId;
    container->pid = s->pid();
    container->gpus = gpus;
    containers_[containerId] = container;

    foreach (unsigned device, gpus) {
      freeGpus.erase(device);
    }

    LOG(INFO) << "[docker_shell] forked executor pid=" << container->pid
              << " for container " << containerId.value();

    // Reap the executor process. When it exits, fulfill the termination
    // promise. The executor having exited is medios's signal that the
    // task has reached a terminal state (the executor sends the final
    // status update before driver.stop() returns and main() exits).
    process::reap(container->pid)
      .onAny(process::defer(
          self(),
          [this, containerId](const Future<Option<int>>& exitCode) {
            onReaped(containerId, exitCode);
          }));

    return Containerizer::LaunchResult::SUCCESS;
  }

  Future<Nothing> update(
      const ContainerID& /*containerId*/,
      const Resources& /*resourceRequests*/,
      const google::protobuf::Map<string, Value::Scalar>& /*resourceLimits*/)
  {
    // medios does not currently update Docker resource limits dynamically.
    // The framework's resource asks at launch are honored at `docker run`
    // time (when R3 / #147 lands).
    return Nothing();
  }

  Future<ResourceStatistics> usage(const ContainerID& /*containerId*/)
  {
    // No live container stats in alpha. Future: `docker stats --no-stream`.
    return ResourceStatistics();
  }

  Future<ContainerStatus> status(const ContainerID& containerId)
  {
    auto it = containers_.find(containerId);
    if (it == containers_.end()) {
      // The container id is the only identity the node can attach a status
      // update to. Reporting a status for a container this containerizer
      // never launched, or has already reaped, would attach the wrong one.
      return Failure("Unknown container: " + stringify(containerId));
    }

    ContainerStatus result;
    result.mutable_container_id()->CopyFrom(containerId);
    if (it->second->pid > 0) {
      result.set_executor_pid(it->second->pid);
    }
    return result;
  }

  Future<Option<medios::node::ContainerTermination>> waitContainer(
      const ContainerID& containerId)
  {
    auto it = containers_.find(containerId);
    if (it == containers_.end()) {
      return None();
    }
    return it->second->termination.future();
  }

  Future<Option<medios::node::ContainerTermination>> destroy(
      const ContainerID& containerId)
  {
    auto it = containers_.find(containerId);
    if (it == containers_.end()) {
      return None();
    }
    auto container = it->second;
    LOG(INFO) << "[docker_shell] destroy " << containerId.value()
              << " pid=" << container->pid;
    if (container->pid > 0) {
      ::kill(container->pid, SIGTERM);
    }
    return container->termination.future();
  }

  Future<bool> killSignal(const ContainerID& containerId, int signal)
  {
    // R1 / #145: full kill semantics. This routes the signal to the
    // executor process; the executor's killTask handler then runs
    // `docker kill` on the user container.
    auto it = containers_.find(containerId);
    if (it == containers_.end()) {
      return false;
    }
    auto container = it->second;
    LOG(INFO) << "[docker_shell] kill " << containerId.value()
              << " signal=" << signal;
    if (container->pid > 0 && ::kill(container->pid, signal) == 0) {
      return true;
    }
    return false;
  }

  Future<hashset<ContainerID>> listContainers()
  {
    hashset<ContainerID> result;
    for (const auto& [id, _] : containers_) {
      result.insert(id);
    }
    return result;
  }

  Future<Nothing> pruneImages(const vector<Image>& /*excludedImages*/)
  {
    return Nothing();
  }

private:
  void onReaped(const ContainerID& containerId,
                const Future<Option<int>>& exitCode)
  {
    auto it = containers_.find(containerId);
    if (it == containers_.end()) {
      return;
    }
    auto container = it->second;

    medios::node::ContainerTermination termination;
    if (exitCode.isReady() && exitCode->isSome()) {
      termination.set_status(exitCode->get());
      LOG(INFO) << "[docker_shell] container " << containerId.value()
                << " reaped, status=" << exitCode->get();
    } else {
      LOG(WARNING) << "[docker_shell] container " << containerId.value()
                   << " reap produced no status";
    }

    // The executor has exited, and it does not exit before its container
    // has stopped, so the devices are free again.
    foreach (unsigned device, container->gpus) {
      freeGpus.insert(device);
    }

    container->termination.set(
        Option<medios::node::ContainerTermination>(termination));
    containers_.erase(it);
  }

  Flags flags;

  // Docker device indices not assigned to a running container. A node
  // restart starts with all of them free, because this containerizer does
  // not recover containers (#149).
  std::set<unsigned> freeGpus;
  hashmap<ContainerID, std::shared_ptr<Container>> containers_;
};


// ----- Outer DockerShellContainerizer (dispatch shim) -----

Try<DockerShellContainerizer*> DockerShellContainerizer::create(
    const Flags& flags)
{
  // The node's devices are Docker indices 0..N-1, N being its declared
  // `gpus`. A fraction has no device to stand for, so it is refused here
  // rather than at the first launch.
  Try<Resources> declared = Resources::parse(
      flags.resources.getOrElse(""), flags.default_role);
  if (declared.isError()) {
    return Error("Failed to parse --resources: " + declared.error());
  }

  const double gpus = declared->get<Value::Scalar>("gpus")
    .getOrElse(Value::Scalar())
    .value();

  if (gpus < 0 || gpus != static_cast<double>(static_cast<unsigned>(gpus))) {
    return Error(
        "docker_shell needs a whole number of gpus in --resources, got " +
        stringify(gpus));
  }

  Owned<DockerShellContainerizerProcess> proc(
      new DockerShellContainerizerProcess(
          flags, static_cast<unsigned>(gpus)));
  return new DockerShellContainerizer(std::move(proc));
}


DockerShellContainerizer::DockerShellContainerizer(
    Owned<DockerShellContainerizerProcess> _process)
  : process(std::move(_process))
{
  spawn(process.get());
}


DockerShellContainerizer::~DockerShellContainerizer()
{
  process::terminate(process.get());
  process::wait(process.get());
}


Future<Nothing> DockerShellContainerizer::recover(
    const Option<state::NodeState>& state)
{
  return dispatch(process.get(),
                  &DockerShellContainerizerProcess::recover, state);
}


Future<Containerizer::LaunchResult> DockerShellContainerizer::launch(
    const ContainerID& containerId,
    const medios::node::ContainerConfig& containerConfig,
    const map<string, string>& environment,
    const Option<string>& pidCheckpointPath)
{
  return dispatch(process.get(),
                  &DockerShellContainerizerProcess::launch,
                  containerId, containerConfig, environment, pidCheckpointPath);
}


Future<Nothing> DockerShellContainerizer::update(
    const ContainerID& containerId,
    const Resources& resourceRequests,
    const google::protobuf::Map<string, Value::Scalar>& resourceLimits)
{
  return dispatch(process.get(),
                  &DockerShellContainerizerProcess::update,
                  containerId, resourceRequests, resourceLimits);
}


Future<ResourceStatistics> DockerShellContainerizer::usage(
    const ContainerID& containerId)
{
  return dispatch(process.get(),
                  &DockerShellContainerizerProcess::usage, containerId);
}


Future<ContainerStatus> DockerShellContainerizer::status(
    const ContainerID& containerId)
{
  return dispatch(process.get(),
                  &DockerShellContainerizerProcess::status, containerId);
}


Future<Option<medios::node::ContainerTermination>>
DockerShellContainerizer::wait(const ContainerID& containerId)
{
  return dispatch(process.get(),
                  &DockerShellContainerizerProcess::waitContainer,
                  containerId);
}


Future<Option<medios::node::ContainerTermination>>
DockerShellContainerizer::destroy(const ContainerID& containerId)
{
  return dispatch(process.get(),
                  &DockerShellContainerizerProcess::destroy, containerId);
}


Future<bool> DockerShellContainerizer::kill(
    const ContainerID& containerId, int signal)
{
  return dispatch(process.get(),
                  &DockerShellContainerizerProcess::killSignal,
                  containerId, signal);
}


Future<hashset<ContainerID>> DockerShellContainerizer::containers()
{
  return dispatch(process.get(),
                  &DockerShellContainerizerProcess::listContainers);
}


Future<Nothing> DockerShellContainerizer::pruneImages(
    const vector<Image>& excludedImages)
{
  return dispatch(process.get(),
                  &DockerShellContainerizerProcess::pruneImages,
                  excludedImages);
}

} // namespace node
} // namespace internal
} // namespace medios
