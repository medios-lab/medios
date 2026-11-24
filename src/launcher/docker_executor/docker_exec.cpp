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

#include "launcher/docker_executor/docker_exec.hpp"

#include <sys/wait.h>
#include <unistd.h>

#include <process/future.hpp>
#include <process/io.hpp>
#include <process/subprocess.hpp>

#include <stout/duration.hpp>
#include <stout/error.hpp>
#include <stout/foreach.hpp>
#include <stout/hashset.hpp>
#include <stout/os.hpp>
#include <stout/path.hpp>
#include <stout/strings.hpp>
#include <stout/stringify.hpp>
#include <stout/try.hpp>

namespace medios {
namespace internal {
namespace docker {

std::pair<Option<int>, std::string> dockerCapture(
    const std::vector<std::string>& argv)
{
  Try<process::Subprocess> s = process::subprocess(
      argv[0],
      argv,
      process::Subprocess::PATH(os::DEV_NULL),
      process::Subprocess::PATH(os::DEV_NULL),
      process::Subprocess::PIPE());

  if (s.isError()) {
    return {None(),
            "failed to start '" + strings::join(" ", argv) + "': " + s.error()};
  }

  // Drain stderr to EOF (async on the libprocess event loop, so no deadlock),
  // then reap.
  std::string captured;
  if (s->err().isSome()) {
    process::Future<std::string> read = process::io::read(s->err().get());
    read.await();
    if (read.isReady()) {
      captured = read.get();
    }
  }

  process::Future<Option<int>> status = s->status();
  status.await();

  return {status.isReady() ? status.get() : Option<int>(None()), captured};
}


Option<int> dockerRun(
    const std::vector<std::string>& argv,
    const std::string& sandboxDir,
    const Option<lambda::function<void()>>& forked)
{
  const process::Subprocess::IO out = sandboxDir.empty()
      ? process::Subprocess::FD(STDOUT_FILENO)
      : process::Subprocess::PATH(sandboxDir + "/stdout");
  const process::Subprocess::IO err = sandboxDir.empty()
      ? process::Subprocess::FD(STDERR_FILENO)
      : process::Subprocess::PATH(sandboxDir + "/stderr");

  Try<process::Subprocess> s = process::subprocess(
      argv[0], argv, process::Subprocess::PATH(os::DEV_NULL), out, err);

  if (s.isError()) {
    return None();
  }

  if (forked.isSome()) {
    forked.get()();
  }

  process::Future<Option<int>> status = s->status();
  status.await();
  return status.isReady() ? status.get() : Option<int>(None());
}


std::string describeStatus(const Option<int>& status)
{
  // `status` is the raw waitpid status from libprocess (see Subprocess::status),
  // so decode it rather than printing it verbatim.
  if (status.isNone()) {
    return "no exit status (the subprocess did not run)";
  }
  const int raw = status.get();
  if (WIFEXITED(raw)) {
    return "exit " + stringify(WEXITSTATUS(raw));
  }
  if (WIFSIGNALED(raw)) {
    return "killed by signal " + stringify(WTERMSIG(raw));
  }
  return "wait status " + stringify(raw);
}


ContainerOutcome containerOutcome(
    const Option<int>& runStatus,
    bool killIssued,
    bool killedByStall,
    const std::string& stallExplanation)
{
  if (killIssued) {
    return {TASK_KILLED, None(), "Killed by framework"};
  }

  if (killedByStall) {
    return {
      TASK_FAILED,
      TaskStatus::REASON_PROGRESS_STALLED,
      stallExplanation + "; container " + describeStatus(runStatus)};
  }

  if (runStatus.isNone()) {
    // `docker run` never produced a status — the launch itself failed.
    return {
      TASK_FAILED,
      TaskStatus::REASON_CONTAINER_LAUNCH_FAILED,
      "Failed to run 'docker run': " + describeStatus(runStatus)};
  }

  if (runStatus.get() == 0) {
    return {TASK_FINISHED, None(), "Container exited cleanly"};
  }

  return {
    TASK_FAILED,
    None(),
    "Container 'docker run' " + describeStatus(runStatus) +
      "; see the task sandbox 'stderr' for docker output"};
}

Try<std::vector<std::string>> storeVolumeArguments(
    const google::protobuf::RepeatedPtrField<Volume>& volumes,
    const std::map<std::string, node::StoreDeclaration>& stores,
    const std::string& sandboxDir)
{
  std::vector<std::string> arguments;

  for (int i = 0; i < volumes.size(); ++i) {
    const Volume& volume = volumes.Get(i);
    const std::string prefix =
      "container.volumes[" + stringify(i) + "]: ";

    if (volume.has_image()) {
      return Error(
          prefix + "an image volume is not delivered by the docker_shell "
          "containerizer; a volume names a store");
    }

    Try<std::string> declared = node::storePathOf(volume, stores);
    if (declared.isError()) {
      return Error(prefix + declared.error());
    }

    const std::string& name = volume.source().store().name();
    const std::string& containerPath = volume.container_path();

    std::string target = containerPath;

    // A relative path is a name inside the sandbox, which `docker run`
    // binds at its own path (DR-036 revision, medios#602).
    if (containerPath.empty() || containerPath.front() != '/') {
      if (!node::isSandboxName(containerPath)) {
        return Error(
            prefix + "container_path '" + containerPath + "' is neither "
            "absolute nor one name inside the sandbox, such as 'data'");
      }

      if (sandboxDir.empty()) {
        return Error(
            prefix + "container_path '" + containerPath + "' is relative "
            "to the sandbox, and MEDIOS_DIRECTORY is not set, so there is "
            "no sandbox to place store '" + name + "' in");
      }

      target = path::join(sandboxDir, containerPath);

      // The fetcher ran before this executor; a fetched file at the same
      // name would be covered by the bind.
      if (os::exists(target)) {
        return Error(
            prefix + "Volume for store '" + name + "' would be placed at '" +
            target + "', which already exists in the sandbox; rename the "
            "volume or the fetched file");
      }
    }

    std::string entry = declared.get() + ":" + target;
    if (volume.mode() == Volume::RO) {
      entry += ":ro";
    }

    arguments.push_back("--volume");
    arguments.push_back(entry);
  }

  return arguments;
}


Try<std::vector<std::string>> ipcArguments(const ContainerInfo& container)
{
  std::vector<std::string> arguments;

  if (!container.has_linux_info()) {
    return arguments;
  }

  const LinuxInfo& linuxInfo = container.linux_info();

  if (linuxInfo.has_ipc_mode()) {
    if (linuxInfo.ipc_mode() != LinuxInfo::PRIVATE) {
      return Error(
          "LinuxInfo.ipc_mode " +
          LinuxInfo::IpcMode_Name(linuxInfo.ipc_mode()) +
          " is not supported by the docker_shell containerizer; only"
          " PRIVATE is");
    }

    arguments.push_back("--ipc=private");
  }

  if (linuxInfo.has_shm_size()) {
    if (!linuxInfo.has_ipc_mode()) {
      return Error(
          "LinuxInfo.shm_size is only supported when LinuxInfo.ipc_mode is"
          " PRIVATE");
    }

    if (linuxInfo.shm_size() == 0) {
      return Error("LinuxInfo.shm_size must be at least 1 (MB)");
    }

    arguments.push_back(
        "--shm-size=" + stringify(linuxInfo.shm_size()) + "m");
  }

  return arguments;
}


Try<std::vector<std::string>> networkArguments(const TaskInfo& task)
{
  std::vector<std::string> arguments;

  const ContainerInfo::DockerInfo& docker = task.container().docker();

  if (docker.has_network()) {
    switch (docker.network()) {
      case ContainerInfo::DockerInfo::HOST:
        arguments.push_back("--network=host");
        return arguments;
      case ContainerInfo::DockerInfo::BRIDGE:
        break;
      default:
        return Error(
            "ContainerInfo.docker.network " +
            ContainerInfo::DockerInfo::Network_Name(docker.network()) +
            " is not supported by the docker_shell containerizer; only"
            " HOST and BRIDGE are");
    }
  }

  // On the bridge, a process binding a reserved port inside the container
  // is unreachable from the host unless the port is published.
  foreach (const Resource& resource, task.resources()) {
    if (resource.name() != "ports" || resource.type() != Value::RANGES) {
      continue;
    }
    foreach (const Value::Range& range, resource.ranges().range()) {
      for (uint64_t port = range.begin(); port <= range.end(); ++port) {
        arguments.push_back("--publish");
        arguments.push_back(stringify(port) + ":" + stringify(port));
      }
    }
  }

  return arguments;
}


int stopGraceSeconds(const TaskInfo& task, int defaultSeconds)
{
  if (!task.has_kill_policy() || !task.kill_policy().has_grace_period()) {
    return defaultSeconds;
  }

  const int64_t nanoseconds = task.kill_policy().grace_period().nanoseconds();
  const int64_t perSecond = Seconds(1).ns();

  if (nanoseconds <= 0) {
    return 0;
  }

  return static_cast<int>((nanoseconds + perSecond - 1) / perSecond);
}


Try<std::vector<std::string>> gpuArguments(
    double grantedGpus,
    const Option<std::string>& devices)
{
  std::vector<std::string> assigned;
  if (devices.isSome()) {
    foreach (const std::string& device, strings::tokenize(devices.get(), ",")) {
      assigned.push_back(device);
    }
  }

  if (grantedGpus > 0 && devices.isNone()) {
    return Error(
        "The task was granted " + stringify(grantedGpus) + " gpus but the"
        " node assigned it no devices (" +
        std::string(GPU_DEVICES_ENVIRONMENT_VARIABLE) + " is not set)");
  }

  if (static_cast<double>(assigned.size()) != grantedGpus) {
    return Error(
        "The task was granted " + stringify(grantedGpus) + " gpus but the"
        " node assigned " + stringify(assigned.size()) + " devices ('" +
        devices.getOrElse("") + "')");
  }

  if (assigned.empty()) {
    return std::vector<std::string>{"--env", "NVIDIA_VISIBLE_DEVICES=void"};
  }

  const std::string list = strings::join(",", assigned);

  // Docker splits `--gpus` on commas, so a list of devices is quoted.
  return std::vector<std::string>{
    "--gpus", "\"device=" + list + "\"",
    "--env", "NVIDIA_VISIBLE_DEVICES=" + list,
  };
}


Try<std::vector<std::string>> parameterArguments(
    const ContainerInfo::DockerInfo& docker)
{
  static const hashset<std::string> allowedKeys = {
    "entrypoint",
    "user",
    "workdir",
    "hostname",
    "stop-signal",
    "ulimit",
    "init",
    "read-only",
    "tmpfs",
    "add-host",
    "dns",
    "dns-search",
    "dns-option",
    "label",
    "log-opt",
  };

  std::vector<std::string> arguments;

  for (int i = 0; i < docker.parameters_size(); ++i) {
    const Parameter& parameter = docker.parameters(i);
    const std::string prefix =
      "container.docker.parameters[" + stringify(i) + "]: ";

    if (!parameter.has_key() || parameter.key().empty()) {
      return Error(prefix + "parameter key must not be empty");
    }

    const std::string& key = parameter.key();
    if (!allowedKeys.contains(key)) {
      return Error(
          prefix + "DockerInfo.parameters key '" + key + "' is not allowed");
    }

    arguments.push_back("--" + key + "=" + parameter.value());
  }

  return arguments;
}

} // namespace docker {
} // namespace internal {
} // namespace medios {
