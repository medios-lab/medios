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

#include <sys/wait.h>

#include <map>
#include <string>
#include <utility>
#include <vector>

#include <gtest/gtest.h>

#include <stout/duration.hpp>
#include <stout/gtest.hpp>
#include <stout/option.hpp>
#include <stout/os.hpp>
#include <stout/path.hpp>
#include <stout/strings.hpp>

#include <medios/medios.hpp>

#include <stout/try.hpp>

#include "launcher/docker_executor/docker_exec.hpp"

#include "node/stores.hpp"

using medios::internal::docker::containerOutcome;
using medios::internal::docker::describeStatus;
using medios::internal::docker::dockerCapture;
using medios::internal::docker::gpuArguments;
using medios::internal::docker::ipcArguments;
using medios::internal::docker::networkArguments;
using medios::internal::docker::parameterArguments;
using medios::internal::docker::stopGraceSeconds;
using medios::internal::docker::storeVolumeArguments;

using medios::internal::node::StoreDeclaration;

using std::string;
using std::vector;

// Hermetic: these drive the capture/decode mechanism with `sh -c`, not docker,
// so the stderr + exit-status capture is deterministic and independent of
// whether docker is installed. The mechanism does not care that the program is
// docker. #298.

TEST(DockerExecTest, CapturesStderrAndExitStatus)
{
  const std::pair<Option<int>, string> r =
      dockerCapture({"sh", "-c", "echo boom >&2; exit 3"});

  ASSERT_SOME(r.first);
  ASSERT_TRUE(WIFEXITED(r.first.get()));
  EXPECT_EQ(3, WEXITSTATUS(r.first.get()));
  EXPECT_TRUE(strings::contains(r.second, "boom"));
}


TEST(DockerExecTest, CleanExitHasNoStderr)
{
  const std::pair<Option<int>, string> r =
      dockerCapture({"sh", "-c", "exit 0"});

  ASSERT_SOME(r.first);
  EXPECT_EQ(0, r.first.get()); // raw wait status 0 == clean exit 0.
  EXPECT_EQ("", r.second);
}


TEST(DockerExecTest, MissingBinaryIsNeverACleanExit)
{
  // execvpe fails in the child, which exits non-zero; never a clean exit 0.
  const std::pair<Option<int>, string> r =
      dockerCapture({"medios-no-such-binary-xyzzy"});

  EXPECT_FALSE(r.first.isSome() && r.first.get() == 0);
}


TEST(DockerExecTest, DescribeStatusDecodesExitCode)
{
  const std::pair<Option<int>, string> r =
      dockerCapture({"sh", "-c", "exit 5"});

  // Decodes the raw waitpid status, not the verbatim integer.
  EXPECT_EQ("exit 5", describeStatus(r.first));
}


// The terminal status of a container run. The docker executor has no
// end-to-end test, so the decision is checked here directly.

// A framework kill is decided ahead of a stall. The container stops
// advancing because it is being stopped, so a stall seen after the kill is
// a consequence of it and not the reason the task ended.
TEST(DockerExecTest, AFrameworkKillOutranksAStall)
{
  const medios::internal::docker::ContainerOutcome outcome = containerOutcome(
      Option<int>(0),
      true,
      true,
      "REASON_PROGRESS_STALLED: no progress for 10secs");

  EXPECT_EQ(medios::TASK_KILLED, outcome.state);
  EXPECT_NE(medios::TASK_FAILED, outcome.state);
  EXPECT_NONE(outcome.reason);
}


TEST(DockerExecTest, AStallIsFailedWithTheReasonAndTheExplanation)
{
  const medios::internal::docker::ContainerOutcome outcome = containerOutcome(
      Option<int>(0),
      false,
      true,
      "REASON_PROGRESS_STALLED: no progress for 10secs");

  EXPECT_EQ(medios::TASK_FAILED, outcome.state);
  ASSERT_SOME(outcome.reason);
  EXPECT_EQ(
      medios::TaskStatus::REASON_PROGRESS_STALLED, outcome.reason.get());
  EXPECT_TRUE(
      strings::contains(outcome.message, "REASON_PROGRESS_STALLED"));
}


// A container that handles SIGTERM and exits 0 did not finish its work, so
// neither kind of kill is reported as TASK_FINISHED.
TEST(DockerExecTest, AKilledContainerThatExitsZeroIsNotFinished)
{
  EXPECT_NE(
      medios::TASK_FINISHED,
      containerOutcome(Option<int>(0), true, false, "").state);

  EXPECT_NE(
      medios::TASK_FINISHED,
      containerOutcome(Option<int>(0), false, true, "stalled").state);
}


TEST(DockerExecTest, ACleanExitIsFinished)
{
  const medios::internal::docker::ContainerOutcome outcome =
      containerOutcome(Option<int>(0), false, false, "");

  EXPECT_EQ(medios::TASK_FINISHED, outcome.state);
  EXPECT_NONE(outcome.reason);
}


TEST(DockerExecTest, NoExitStatusIsALaunchFailure)
{
  const medios::internal::docker::ContainerOutcome outcome =
      containerOutcome(None(), false, false, "");

  EXPECT_EQ(medios::TASK_FAILED, outcome.state);
  ASSERT_SOME(outcome.reason);
  EXPECT_EQ(
      medios::TaskStatus::REASON_CONTAINER_LAUNCH_FAILED,
      outcome.reason.get());
}


TEST(DockerExecTest, ANonZeroExitIsFailedWithoutAReason)
{
  const medios::internal::docker::ContainerOutcome outcome =
      containerOutcome(Option<int>(256), false, false, "");

  EXPECT_EQ(medios::TASK_FAILED, outcome.state);
  EXPECT_NONE(outcome.reason);
}


TEST(DockerExecTest, DescribeStatusNone)
{
  EXPECT_EQ("no exit status (the subprocess did not run)",
            describeStatus(None()));
}


static std::map<string, StoreDeclaration> twoStores()
{
  StoreDeclaration tables;
  tables.name = "tables";
  tables.path = "/var/lib/medios-stores/tabicl";
  tables.storeClass = "local";
  tables.locality = "near";
  tables.readers = 4;
  tables.writers = 0;

  StoreDeclaration results;
  results.name = "results";
  results.path = "/var/lib/medios-results";
  results.storeClass = "shared";
  results.locality = "near";
  results.readers = 10;
  results.writers = 10;

  return {{"tables", tables}, {"results", results}};
}


static medios::Volume storeVolume(
    const string& store,
    const string& containerPath,
    medios::Volume::Mode mode)
{
  medios::Volume volume;
  volume.set_container_path(containerPath);
  volume.set_mode(mode);
  volume.mutable_source()->set_type(medios::Volume::Source::STORE);
  volume.mutable_source()->mutable_store()->set_name(store);
  return volume;
}


// A STORE volume resolves to the declared path, read-only as `:ro`.
TEST(DockerExecTest, StoreVolumesResolveToDeclaredPaths)
{
  google::protobuf::RepeatedPtrField<medios::Volume> volumes;
  *volumes.Add() = storeVolume("tables", "/data", medios::Volume::RO);
  *volumes.Add() = storeVolume("results", "/out", medios::Volume::RW);

  Try<vector<string>> arguments =
    storeVolumeArguments(volumes, twoStores(), "");
  ASSERT_SOME(arguments);

  const vector<string> expected = {
    "--volume", "/var/lib/medios-stores/tabicl:/data:ro",
    "--volume", "/var/lib/medios-results:/out",
  };
  EXPECT_EQ(expected, arguments.get());
}


// Every other shape is refused with the entry and the reason named.
TEST(DockerExecTest, NonStoreVolumesAreRefusedByName)
{
  google::protobuf::RepeatedPtrField<medios::Volume> volumes;
  medios::Volume* bare = volumes.Add();
  bare->set_container_path("/data");
  bare->set_host_path("/var/lib/data");
  bare->set_mode(medios::Volume::RW);

  Try<vector<string>> arguments =
    storeVolumeArguments(volumes, twoStores(), "");
  ASSERT_ERROR(arguments);
  EXPECT_TRUE(strings::contains(arguments.error(), "volumes[0]"));
  EXPECT_TRUE(strings::contains(arguments.error(), "HOST_PATH"));

  volumes.Clear();
  medios::Volume* source = volumes.Add();
  source->set_container_path("/data");
  source->set_mode(medios::Volume::RW);
  source->mutable_source()->set_type(medios::Volume::Source::HOST_PATH);
  source->mutable_source()->mutable_host_path()->set_path("/var/lib/data");

  arguments = storeVolumeArguments(volumes, twoStores(), "");
  ASSERT_ERROR(arguments);
  EXPECT_TRUE(strings::contains(arguments.error(), "HOST_PATH"));

  volumes.Clear();
  medios::Volume* image = volumes.Add();
  image->set_container_path("/image");
  image->set_mode(medios::Volume::RO);
  image->mutable_image()->set_type(medios::Image::DOCKER);
  image->mutable_image()->mutable_docker()->set_name("busybox");

  arguments = storeVolumeArguments(volumes, twoStores(), "");
  ASSERT_ERROR(arguments);
  EXPECT_TRUE(strings::contains(arguments.error(), "image volume"));

  volumes.Clear();
  *volumes.Add() = storeVolume("tables", "/data", medios::Volume::RO);
  *volumes.Add() = storeVolume("weights", "/weights", medios::Volume::RO);

  arguments = storeVolumeArguments(volumes, twoStores(), "");
  ASSERT_ERROR(arguments);
  EXPECT_TRUE(strings::contains(arguments.error(), "volumes[1]"));
  EXPECT_TRUE(strings::contains(arguments.error(), "'weights'"));
  EXPECT_TRUE(strings::contains(arguments.error(), "does not declare"));

  volumes.Clear();
  *volumes.Add() = storeVolume("tables", "data/tables", medios::Volume::RO);

  arguments = storeVolumeArguments(volumes, twoStores(), "/sandbox");
  ASSERT_ERROR(arguments);
  EXPECT_TRUE(strings::contains(arguments.error(), "'data/tables'"));
  EXPECT_TRUE(strings::contains(
      arguments.error(), "one name inside the sandbox"));

  // A node that declares no stores says so.
  volumes.Clear();
  *volumes.Add() = storeVolume("tables", "/data", medios::Volume::RO);

  arguments = storeVolumeArguments(volumes, {}, "");
  ASSERT_ERROR(arguments);
  EXPECT_TRUE(strings::contains(arguments.error(), "declares no stores"));
}


// A relative container path is one name inside the sandbox, which
// `docker run` binds at its own path (DR-036 revision, medios#602).
TEST(DockerExecTest, ARelativeStoreIsPlacedInTheSandbox)
{
  Try<string> sandboxDir = os::mkdtemp();
  ASSERT_SOME(sandboxDir);

  google::protobuf::RepeatedPtrField<medios::Volume> volumes;
  *volumes.Add() = storeVolume("tables", "tables", medios::Volume::RO);
  *volumes.Add() = storeVolume("results", "/out", medios::Volume::RW);

  Try<vector<string>> arguments =
    storeVolumeArguments(volumes, twoStores(), sandboxDir.get());
  ASSERT_SOME(arguments);

  const vector<string> expected = {
    "--volume",
    "/var/lib/medios-stores/tabicl:" +
      path::join(sandboxDir.get(), "tables") + ":ro",
    "--volume", "/var/lib/medios-results:/out",
  };
  EXPECT_EQ(expected, arguments.get());

  // With nothing at the name: the bind would cover a fetched file there.
  ASSERT_SOME(os::write(path::join(sandboxDir.get(), "tables"), "fetched"));

  arguments = storeVolumeArguments(volumes, twoStores(), sandboxDir.get());
  ASSERT_ERROR(arguments);
  EXPECT_TRUE(strings::contains(
      arguments.error(),
      "Volume for store 'tables' would be placed at '" +
      path::join(sandboxDir.get(), "tables") + "', which already exists"))
    << arguments.error();

  ASSERT_SOME(os::rmdir(sandboxDir.get()));
}


// Without MEDIOS_DIRECTORY there is no sandbox to place a relative store
// in, and the refusal names the variable. An absolute path still works.
TEST(DockerExecTest, ARelativeStoreWithNoSandboxIsRefused)
{
  google::protobuf::RepeatedPtrField<medios::Volume> volumes;
  *volumes.Add() = storeVolume("tables", "tables", medios::Volume::RO);

  Try<vector<string>> arguments =
    storeVolumeArguments(volumes, twoStores(), "");
  ASSERT_ERROR(arguments);
  EXPECT_TRUE(strings::contains(arguments.error(), "MEDIOS_DIRECTORY"))
    << arguments.error();

  volumes.Clear();
  *volumes.Add() = storeVolume("tables", "/data", medios::Volume::RO);

  EXPECT_SOME(storeVolumeArguments(volumes, twoStores(), ""));
}


// No volumes, no arguments.
TEST(DockerExecTest, NoVolumesNoArguments)
{
  google::protobuf::RepeatedPtrField<medios::Volume> volumes;

  Try<vector<string>> arguments = storeVolumeArguments(volumes, {}, "");
  ASSERT_SOME(arguments);
  EXPECT_TRUE(arguments->empty());
}


// No LinuxInfo leaves IPC to Docker: no arguments.
TEST(DockerExecTest, NoLinuxInfoNoIpcArguments)
{
  medios::ContainerInfo container;
  container.set_type(medios::ContainerInfo::DOCKER);

  Try<vector<string>> arguments = ipcArguments(container);
  ASSERT_SOME(arguments);
  EXPECT_TRUE(arguments->empty());
}


// A private IPC namespace with a /dev/shm size is passed to `docker run`,
// the size in MB as the proto field states (#648).
TEST(DockerExecTest, PrivateIpcWithShmSizeIsPassedToDocker)
{
  medios::ContainerInfo container;
  container.mutable_linux_info()->set_ipc_mode(medios::LinuxInfo::PRIVATE);
  container.mutable_linux_info()->set_shm_size(8192);

  Try<vector<string>> arguments = ipcArguments(container);
  ASSERT_SOME(arguments);
  EXPECT_EQ(
      vector<string>({"--ipc=private", "--shm-size=8192m"}),
      arguments.get());
}


TEST(DockerExecTest, PrivateIpcAloneIsPassedToDocker)
{
  medios::ContainerInfo container;
  container.mutable_linux_info()->set_ipc_mode(medios::LinuxInfo::PRIVATE);

  Try<vector<string>> arguments = ipcArguments(container);
  ASSERT_SOME(arguments);
  EXPECT_EQ(vector<string>({"--ipc=private"}), arguments.get());
}


// The medios containerizer refuses shm_size without a private IPC
// namespace; docker_shell refuses it the same way.
TEST(DockerExecTest, ShmSizeWithoutPrivateIpcIsRefused)
{
  medios::ContainerInfo container;
  container.mutable_linux_info()->set_shm_size(1024);

  Try<vector<string>> arguments = ipcArguments(container);
  ASSERT_ERROR(arguments);
  EXPECT_TRUE(strings::contains(arguments.error(), "shm_size"))
    << arguments.error();
}


TEST(DockerExecTest, ZeroShmSizeIsRefused)
{
  medios::ContainerInfo container;
  container.mutable_linux_info()->set_ipc_mode(medios::LinuxInfo::PRIVATE);
  container.mutable_linux_info()->set_shm_size(0);

  ASSERT_ERROR(ipcArguments(container));
}


// SHARE_PARENT for a top-level task is the node's own IPC namespace, which
// docker_shell does not grant.
TEST(DockerExecTest, SharedParentIpcIsRefused)
{
  medios::ContainerInfo container;
  container.mutable_linux_info()->set_ipc_mode(
      medios::LinuxInfo::SHARE_PARENT);

  Try<vector<string>> arguments = ipcArguments(container);
  ASSERT_ERROR(arguments);
  EXPECT_TRUE(strings::contains(arguments.error(), "SHARE_PARENT"))
    << arguments.error();
}


// A Docker task reserving ports 31000-31001 and 31005, with `network` set
// to `network`, or left unset when `network` is None.
static medios::TaskInfo portsTask(
    const Option<medios::ContainerInfo::DockerInfo::Network>& network)
{
  medios::TaskInfo task;
  task.mutable_container()->set_type(medios::ContainerInfo::DOCKER);
  task.mutable_container()->mutable_docker()->set_image("alpine:latest");
  if (network.isSome()) {
    task.mutable_container()->mutable_docker()->set_network(network.get());
  }

  medios::Resource* ports = task.add_resources();
  ports->set_name("ports");
  ports->set_type(medios::Value::RANGES);
  medios::Value::Range* range = ports->mutable_ranges()->add_range();
  range->set_begin(31000);
  range->set_end(31001);
  range = ports->mutable_ranges()->add_range();
  range->set_begin(31005);
  range->set_end(31005);

  return task;
}


// An unset network is the default bridge, although the proto's default
// value for the field is HOST.
TEST(DockerExecTest, UnsetNetworkPublishesReservedPorts)
{
  Try<vector<string>> arguments = networkArguments(portsTask(None()));
  ASSERT_SOME(arguments);
  EXPECT_EQ(
      vector<string>({
          "--publish", "31000:31000",
          "--publish", "31001:31001",
          "--publish", "31005:31005"}),
      arguments.get());
}


TEST(DockerExecTest, BridgeNetworkPublishesReservedPorts)
{
  Try<vector<string>> arguments =
    networkArguments(portsTask(medios::ContainerInfo::DockerInfo::BRIDGE));
  ASSERT_SOME(arguments);
  EXPECT_EQ(
      vector<string>({
          "--publish", "31000:31000",
          "--publish", "31001:31001",
          "--publish", "31005:31005"}),
      arguments.get());
}


// HOST puts the task in the node's network namespace, where it binds its
// reserved ports directly, so none are published (#659).
TEST(DockerExecTest, HostNetworkPublishesNothing)
{
  Try<vector<string>> arguments =
    networkArguments(portsTask(medios::ContainerInfo::DockerInfo::HOST));
  ASSERT_SOME(arguments);
  EXPECT_EQ(vector<string>({"--network=host"}), arguments.get());
}


TEST(DockerExecTest, NoneAndUserNetworksAreRefused)
{
  Try<vector<string>> arguments =
    networkArguments(portsTask(medios::ContainerInfo::DockerInfo::NONE));
  ASSERT_ERROR(arguments);
  EXPECT_TRUE(strings::contains(arguments.error(), "NONE"))
    << arguments.error();

  arguments =
    networkArguments(portsTask(medios::ContainerInfo::DockerInfo::USER));
  ASSERT_ERROR(arguments);
  EXPECT_TRUE(strings::contains(arguments.error(), "USER"))
    << arguments.error();
}


// A task without a kill policy gets the executor's default stop grace.
TEST(DockerExecTest, NoKillPolicyUsesTheDefaultStopGrace)
{
  medios::TaskInfo task;
  EXPECT_EQ(10, stopGraceSeconds(task, 10));

  task.mutable_kill_policy();
  EXPECT_EQ(10, stopGraceSeconds(task, 10));
}


// The kill policy's grace replaces the default, rounded up to whole
// seconds so the container never gets less than was asked for (#315).
TEST(DockerExecTest, KillPolicyGraceSetsTheStopGrace)
{
  medios::TaskInfo task;
  auto* grace = task.mutable_kill_policy()->mutable_grace_period();

  grace->set_nanoseconds(Seconds(60).ns());
  EXPECT_EQ(60, stopGraceSeconds(task, 10));

  grace->set_nanoseconds(Milliseconds(1500).ns());
  EXPECT_EQ(2, stopGraceSeconds(task, 10));

  grace->set_nanoseconds(1);
  EXPECT_EQ(1, stopGraceSeconds(task, 10));

  grace->set_nanoseconds(0);
  EXPECT_EQ(0, stopGraceSeconds(task, 10));
}


// A container granted no GPU sees none, even on a daemon whose default
// runtime attaches GPUs to every container (#648).
TEST(DockerExecTest, NoGrantHidesEveryGpu)
{
  Try<vector<string>> arguments = gpuArguments(0, string(""));
  ASSERT_SOME(arguments);
  EXPECT_EQ(
      vector<string>({"--env", "NVIDIA_VISIBLE_DEVICES=void"}),
      arguments.get());

  // A node that does not assign devices still hides them.
  arguments = gpuArguments(0, None());
  ASSERT_SOME(arguments);
  EXPECT_EQ(
      vector<string>({"--env", "NVIDIA_VISIBLE_DEVICES=void"}),
      arguments.get());
}


// The assigned devices go to `--gpus`, quoted because Docker splits that
// flag on commas, and to the runtime's variable.
TEST(DockerExecTest, AssignedDevicesAreTheOnlyOnesPassed)
{
  Try<vector<string>> arguments = gpuArguments(2, string("1,3"));
  ASSERT_SOME(arguments);
  EXPECT_EQ(
      vector<string>({
          "--gpus", "\"device=1,3\"",
          "--env", "NVIDIA_VISIBLE_DEVICES=1,3"}),
      arguments.get());
}


TEST(DockerExecTest, AGrantWithoutAssignedDevicesIsRefused)
{
  ASSERT_ERROR(gpuArguments(1, None()));
  ASSERT_ERROR(gpuArguments(1, string("")));
}


TEST(DockerExecTest, AssignedDevicesMustMatchTheGrant)
{
  ASSERT_ERROR(gpuArguments(1, string("0,1")));
  ASSERT_ERROR(gpuArguments(0, string("0")));
}


// DockerInfo.parameters are rendered as `--<key>=<value>` so tasks can
// override an image's entrypoint (#574).
TEST(DockerExecTest, DockerParametersArePassedToDockerRun)
{
  medios::ContainerInfo::DockerInfo docker;
  docker.set_image("alpine/git:latest");

  Try<vector<string>> arguments = parameterArguments(docker);
  ASSERT_SOME(arguments);
  EXPECT_TRUE(arguments->empty());

  medios::Parameter* entrypoint = docker.add_parameters();
  entrypoint->set_key("entrypoint");
  entrypoint->set_value("");

  medios::Parameter* user = docker.add_parameters();
  user->set_key("user");
  user->set_value("1000:1000");

  arguments = parameterArguments(docker);
  ASSERT_SOME(arguments);
  EXPECT_EQ(
      vector<string>({"--entrypoint=", "--user=1000:1000"}),
      arguments.get());
}


TEST(DockerExecTest, InvalidDockerParametersAreRefused)
{
  {
    medios::ContainerInfo::DockerInfo docker;
    docker.set_image("alpine:latest");
    medios::Parameter* p = docker.add_parameters();
    p->set_key("");
    p->set_value("val");
    ASSERT_ERROR(parameterArguments(docker));
  }

  const vector<string> disallowedKeys = {
    "--entrypoint",
    "name",
    "privileged",
    "volume",
    "v",
    "mount",
    "gpus",
    "device",
    "cap-add",
    "security-opt",
    "network",
    "ipc",
    "pid",
    "detach",
    "d",
  };

  for (const string& key : disallowedKeys) {
    medios::ContainerInfo::DockerInfo docker;
    docker.set_image("alpine:latest");
    medios::Parameter* p = docker.add_parameters();
    p->set_key(key);
    p->set_value("val");

    Try<vector<string>> result = parameterArguments(docker);
    ASSERT_ERROR(result) << "unexpectedly accepted key '" << key << "'";
    EXPECT_TRUE(strings::contains(result.error(), "is not allowed"))
      << result.error();
  }
}
