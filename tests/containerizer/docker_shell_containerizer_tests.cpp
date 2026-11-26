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

#include <string>

#include <gtest/gtest.h>

#include <process/future.hpp>
#include <process/gtest.hpp>
#include <process/owned.hpp>

#include <stout/duration.hpp>
#include <stout/gtest.hpp>
#include <stout/option.hpp>
#include <stout/os.hpp>
#include <stout/path.hpp>
#include <stout/stringify.hpp>
#include <stout/try.hpp>

#include "node/containerizer/docker_shell/docker_shell_containerizer.hpp"
#include "node/flags.hpp"

using medios::internal::node::Containerizer;
using medios::internal::node::DockerShellContainerizer;
using medios::internal::node::Flags;

using process::Future;
using process::Owned;

using std::string;

namespace medios {
namespace internal {
namespace tests {

// No docker daemon is involved: the containerizer's job at launch is to fork
// the executor binary out of `launcher_dir` and remember the container it
// forked it for. A stub binary under that name exercises the same bookkeeping
// that `status()` reads.
class DockerShellContainerizerStatusTest : public ::testing::Test
{
protected:
  void SetUp() override
  {
    Try<string> dir = os::mkdtemp();
    ASSERT_SOME(dir);
    launcherDir = dir.get();

    const string stub = path::join(launcherDir, "medios-docker-executor");
    ASSERT_SOME(os::write(stub, "#!/bin/sh\nexec sleep 300\n"));
    ASSERT_SOME(os::chmod(stub, 0755));
  }

  void TearDown() override
  {
    if (!launcherDir.empty()) {
      os::rmdir(launcherDir);
    }
  }

  ContainerID containerId(const string& value) const
  {
    ContainerID id;
    id.set_value(value);
    return id;
  }

  medios::node::ContainerConfig dockerConfig() const
  {
    medios::node::ContainerConfig config;
    config.mutable_container_info()->set_type(ContainerInfo::DOCKER);
    config.mutable_container_info()->mutable_docker()->set_image(
        "alpine:latest");
    return config;
  }

  string launcherDir;
};


// The container id the node uses to key a status update must come back from
// `status()`; without it the node has nothing to put in the task status's
// `container_status`.
TEST_F(DockerShellContainerizerStatusTest, StatusCarriesLaunchedContainerId)
{
  Flags flags;
  flags.launcher_dir = launcherDir;

  Try<DockerShellContainerizer*> create =
      DockerShellContainerizer::create(flags);
  ASSERT_SOME(create);

  Owned<DockerShellContainerizer> containerizer(create.get());

  const ContainerID id = containerId("docker-shell-status-001");

  Future<Containerizer::LaunchResult> launched = containerizer->launch(
      id, dockerConfig(), {}, None());

  AWAIT_READY(launched);
  ASSERT_EQ(Containerizer::LaunchResult::SUCCESS, launched.get());

  Future<ContainerStatus> status = containerizer->status(id);

  AWAIT_READY(status);
  ASSERT_TRUE(status->has_container_id());
  EXPECT_EQ(id.value(), status->container_id().value());
  EXPECT_TRUE(status->has_executor_pid());

  AWAIT_READY(containerizer->destroy(id));
}


// A container this containerizer did not launch has no id to report. It fails
// with the reason rather than returning an empty or borrowed status.
TEST_F(DockerShellContainerizerStatusTest, StatusForUnknownContainerFails)
{
  Flags flags;
  flags.launcher_dir = launcherDir;

  Try<DockerShellContainerizer*> create =
      DockerShellContainerizer::create(flags);
  ASSERT_SOME(create);

  Owned<DockerShellContainerizer> containerizer(create.get());

  Future<ContainerStatus> status =
      containerizer->status(containerId("never-launched"));

  AWAIT_FAILED(status);
  EXPECT_TRUE(status.failure().find("Unknown container") != string::npos);
}

// The node's `gpus` are Docker device indices. Each launch is given the
// lowest free ones, a device is never given to two running containers, and
// a reaped container's devices are given out again (#648).
class DockerShellContainerizerGpuTest
  : public DockerShellContainerizerStatusTest
{
protected:
  void SetUp() override
  {
    DockerShellContainerizerStatusTest::SetUp();

    // The stub records the devices it was assigned, under its own pid.
    const string stub = path::join(launcherDir, "medios-docker-executor");
    ASSERT_SOME(os::write(
        stub,
        "#!/bin/sh\n"
        "printf '%s' \"$MEDIOS_DOCKER_GPU_DEVICES\" > \"" + launcherDir +
        "/devices.$$\"\n"
        "exec sleep 300\n"));
  }

  medios::node::ContainerConfig gpuConfig(double gpus) const
  {
    medios::node::ContainerConfig config = dockerConfig();
    if (gpus > 0) {
      Resource* resource = config.add_resources();
      resource->set_name("gpus");
      resource->set_type(Value::SCALAR);
      resource->mutable_scalar()->set_value(gpus);
    }
    return config;
  }

  // The devices the stub for `id` was started with.
  Option<string> devices(
      DockerShellContainerizer* containerizer,
      const ContainerID& id) const
  {
    Future<ContainerStatus> status = containerizer->status(id);
    status.await();
    if (!status.isReady() || !status->has_executor_pid()) {
      return None();
    }

    const string file = path::join(
        launcherDir, "devices." + stringify(status->executor_pid()));

    for (int i = 0; i < 100 && !os::exists(file); ++i) {
      os::sleep(Milliseconds(20));
    }

    Try<string> read = os::read(file);
    if (read.isError()) {
      return None();
    }
    return read.get();
  }
};


TEST_F(DockerShellContainerizerGpuTest, DevicesAreAssignedOnceAndReturned)
{
  Flags flags;
  flags.launcher_dir = launcherDir;
  flags.resources = "gpus:2";

  Try<DockerShellContainerizer*> create =
      DockerShellContainerizer::create(flags);
  ASSERT_SOME(create);

  Owned<DockerShellContainerizer> containerizer(create.get());

  const ContainerID first = containerId("docker-shell-gpu-001");
  const ContainerID second = containerId("docker-shell-gpu-002");
  const ContainerID none = containerId("docker-shell-gpu-003");
  const ContainerID third = containerId("docker-shell-gpu-004");

  AWAIT_ASSERT_EQ(
      Containerizer::LaunchResult::SUCCESS,
      containerizer->launch(first, gpuConfig(1), {}, None()));
  AWAIT_ASSERT_EQ(
      Containerizer::LaunchResult::SUCCESS,
      containerizer->launch(second, gpuConfig(1), {}, None()));
  AWAIT_ASSERT_EQ(
      Containerizer::LaunchResult::SUCCESS,
      containerizer->launch(none, gpuConfig(0), {}, None()));

  EXPECT_SOME_EQ("0", devices(containerizer.get(), first));
  EXPECT_SOME_EQ("1", devices(containerizer.get(), second));
  EXPECT_SOME_EQ("", devices(containerizer.get(), none));

  // Both devices are in use, so a third grant is a disagreement with the
  // controller and fails rather than sharing one.
  AWAIT_FAILED(containerizer->launch(third, gpuConfig(1), {}, None()));

  // Device 0 comes back once its container's executor is reaped.
  AWAIT_READY(containerizer->destroy(first));

  AWAIT_ASSERT_EQ(
      Containerizer::LaunchResult::SUCCESS,
      containerizer->launch(third, gpuConfig(1), {}, None()));
  EXPECT_SOME_EQ("0", devices(containerizer.get(), third));

  AWAIT_READY(containerizer->destroy(second));
  AWAIT_READY(containerizer->destroy(none));
  AWAIT_READY(containerizer->destroy(third));
}


TEST_F(DockerShellContainerizerGpuTest, FractionalGpusAreRefused)
{
  Flags flags;
  flags.launcher_dir = launcherDir;
  flags.resources = "gpus:1.5";

  EXPECT_ERROR(DockerShellContainerizer::create(flags));

  flags.resources = "gpus:2";
  Try<DockerShellContainerizer*> create =
      DockerShellContainerizer::create(flags);
  ASSERT_SOME(create);

  Owned<DockerShellContainerizer> containerizer(create.get());
  AWAIT_FAILED(containerizer->launch(
      containerId("docker-shell-gpu-fraction"), gpuConfig(0.5), {}, None()));
}


} // namespace tests
} // namespace internal
} // namespace medios
