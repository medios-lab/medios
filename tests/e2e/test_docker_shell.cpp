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

// DR-030 R1-R4 end-to-end coverage for the docker_shell containerizer:
//
//   R1 / #145  KillTask        — long-running task killed → TASK_KILLED
//   R2 / #146  BindMount       — container writes to sandbox; host reads it
//   R3 / #147  EnvPropagation  — task env reaches container, value matches
//   R4 / #148  StdoutStderr    — container output lands in sandbox files
//
// Subprocess / log / lifecycle helpers live in cluster_harness.hpp.
// Auto-skipped on hosts without a reachable Docker daemon (SetUp() probes
// `docker info` and calls GTEST_SKIP() if unavailable).

#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <string>
#include <vector>

#include <unistd.h>

#include <gtest/gtest.h>

#include "cluster_harness.hpp"

using std::string;
using std::vector;

using namespace medios::tests::e2e;

namespace {

// Shared base: helpers + per-test JSON cleanup. No containerizer-specific
// setup lives here; subclasses add it (e.g. the Docker-daemon probe below).
class TaskJsonClusterFixture : public ClusterFixture
{
protected:
  void TearDown() override
  {
    ClusterFixture::TearDown();
    if (!taskJsonPath.empty()) {
      std::filesystem::remove(taskJsonPath);
    }
  }

  std::string launcherDir() const
  {
    return std::filesystem::path(MEDIOS_NODE_BINARY)
               .parent_path().parent_path().string();
  }

  std::filesystem::path writeTaskJson(const string& json, const string& tag)
  {
    auto path = testRoot() /
        ("task-" + tag + "-" + std::to_string(::getpid()) + ".json");
    std::filesystem::remove(path);
    std::ofstream f(path);
    f << json;
    taskJsonPath = path;
    return path;
  }

  std::filesystem::path taskJsonPath;
};


class DockerShellTest : public TaskJsonClusterFixture
{
protected:
  void SetUp() override
  {
    // Self-gate: hosts without Docker get a clear SKIPPED rather than
    // a confusing TASK_FAILED. (This test binary uses gtest_main, not
    // the medios test infra with its DockerFilter.)
    if (std::system("docker info >/dev/null 2>&1") != 0) {
      GTEST_SKIP() << "Docker daemon not reachable; install a Docker "
                   << "backend (e.g. `brew install colima && colima start`) "
                   << "and rerun.";
    }
  }

  void TearDown() override
  {
    TaskJsonClusterFixture::TearDown();
    // Best-effort cleanup of any test-tagged containers that survived.
    // `docker rm -f` is a no-op if the container doesn't exist.
    std::system("docker rm -f medios-r1-kill-001 "
                "medios-r2-bind-001 "
                "medios-r3-env-001 "
                "medios-r4-stdio-001 "
                ">/dev/null 2>&1");
  }
};


// ----------------- R1 / #145: Kill semantics -----------------

// DISABLED pending #166: a docker-side kill races between the executor's
// killTask (which sets killIssued) and the node containerizer's destroy()
// SIGTERM to the executor process (which doesn't), so the kill is sometimes
// reported as TASK_FAILED instead of TASK_KILLED. Re-enable with the fix.
TEST_F(DockerShellTest, DISABLED_DOCKER_KillTaskProducesTaskKilled)
{
  const int controllerPort = startController();
  ASSERT_GT(controllerPort, 0) << controllerLogs;
  ASSERT_TRUE(startNode(controllerPort, {
        "--containerizers=docker_shell",
        "--launcher_dir=" + launcherDir()
      })) << nodeLogs;

  auto task = writeTaskJson(R"({
  "name": "r1-kill-task",
  "task_id": {"value": "r1-kill-001"},
  "node_id": {"value": ""},
  "resources": [
    {"name": "cpus", "type": "SCALAR", "scalar": {"value": 0.5}},
    {"name": "mem",  "type": "SCALAR", "scalar": {"value": 128}}
  ],
  "command": {"shell": true, "value": "sleep 60"},
  "container": {"type": "DOCKER", "docker": {"image": "alpine:latest"}}
})", "r1-kill");

  string output;
  const int exitCode = runMediosExecute(controllerPort, {
        "--task=file://" + task.string(),
        "--kill_after=2secs"
      }, output);

  const auto diag = [&]() {
    return "--- medios-execute (exit " + std::to_string(exitCode) + ") ---\n"
         + output + "\n--- node ---\n" + nodeLogs;
  };

  EXPECT_NE(string::npos, output.find("TASK_STARTING")) << diag();
  EXPECT_NE(string::npos, output.find("TASK_RUNNING")) << diag();
  EXPECT_NE(string::npos, output.find("TASK_KILLED")) << diag();
  EXPECT_EQ(string::npos, output.find("TASK_FINISHED"))
      << "killed task should not reach FINISHED:\n" << diag();
  EXPECT_EQ(string::npos, output.find("TASK_FAILED")) << diag();

  int rc = std::system(
      "docker ps -a --filter name=medios-r1-kill-001 "
      "--format '{{.ID}}' 2>/dev/null | grep -q .");
  EXPECT_NE(0, rc) << "medios-r1-kill-001 leaked:\n" << diag();
}


// ----------------- R2 / #146: Sandbox bind mount -----------------

TEST_F(DockerShellTest, DOCKER_SandboxBindMountReceivesContainerWrites)
{
  const int controllerPort = startController();
  ASSERT_GT(controllerPort, 0) << controllerLogs;
  ASSERT_TRUE(startNode(controllerPort, {
        "--containerizers=docker_shell",
        "--launcher_dir=" + launcherDir()
      })) << nodeLogs;

  auto task = writeTaskJson(R"({
  "name": "r2-bind-mount",
  "task_id": {"value": "r2-bind-001"},
  "node_id": {"value": ""},
  "resources": [
    {"name": "cpus", "type": "SCALAR", "scalar": {"value": 0.5}},
    {"name": "mem",  "type": "SCALAR", "scalar": {"value": 128}}
  ],
  "command": {"shell": true,
              "value": "echo OUTPUT-FROM-CONTAINER > $MEDIOS_SANDBOX/result.txt"},
  "container": {"type": "DOCKER", "docker": {"image": "alpine:latest"}}
})", "r2-bind");

  string output;
  const int exitCode = runMediosExecute(controllerPort, {
        "--task=file://" + task.string()
      }, output);

  const auto diag = [&]() {
    return "--- medios-execute (exit " + std::to_string(exitCode) + ") ---\n"
         + output + "\n--- node ---\n" + nodeLogs;
  };

  ASSERT_NE(string::npos, output.find("TASK_FINISHED")) << diag();

  std::filesystem::path found;
  for (auto it = std::filesystem::recursive_directory_iterator(nodeWorkDir);
       it != std::filesystem::recursive_directory_iterator(); ++it) {
    if (it->is_regular_file() && it->path().filename() == "result.txt") {
      found = it->path();
      break;
    }
  }
  ASSERT_FALSE(found.empty())
      << "Container wrote result.txt to $MEDIOS_SANDBOX but file not found "
      << "under " << nodeWorkDir << ":\n" << diag();

  std::ifstream f(found);
  std::string contents((std::istreambuf_iterator<char>(f)),
                       std::istreambuf_iterator<char>());
  EXPECT_NE(string::npos, contents.find("OUTPUT-FROM-CONTAINER"))
      << "result.txt: '" << contents << "'\n" << diag();
}


// ----------------- R3 / #147: Environment propagation -----------------

TEST_F(DockerShellTest, DOCKER_EnvironmentPropagatesToContainer)
{
  const int controllerPort = startController();
  ASSERT_GT(controllerPort, 0) << controllerLogs;
  ASSERT_TRUE(startNode(controllerPort, {
        "--containerizers=docker_shell",
        "--launcher_dir=" + launcherDir()
      })) << nodeLogs;

  auto task = writeTaskJson(R"({
  "name": "r3-env",
  "task_id": {"value": "r3-env-001"},
  "node_id": {"value": ""},
  "resources": [
    {"name": "cpus", "type": "SCALAR", "scalar": {"value": 0.5}},
    {"name": "mem",  "type": "SCALAR", "scalar": {"value": 128}}
  ],
  "command": {
    "shell": true,
    "value": "echo MEDIOS_TEST_ENV=$MEDIOS_TEST_ENV > $MEDIOS_SANDBOX/env.out",
    "environment": {
      "variables": [
        {"name": "MEDIOS_TEST_ENV", "value": "spike-r3-value"}
      ]
    }
  },
  "container": {"type": "DOCKER", "docker": {"image": "alpine:latest"}}
})", "r3-env");

  string output;
  const int exitCode = runMediosExecute(controllerPort, {
        "--task=file://" + task.string()
      }, output);

  const auto diag = [&]() {
    return "--- medios-execute (exit " + std::to_string(exitCode) + ") ---\n"
         + output + "\n--- node ---\n" + nodeLogs;
  };

  ASSERT_NE(string::npos, output.find("TASK_FINISHED")) << diag();

  std::filesystem::path found;
  for (auto it = std::filesystem::recursive_directory_iterator(nodeWorkDir);
       it != std::filesystem::recursive_directory_iterator(); ++it) {
    if (it->is_regular_file() && it->path().filename() == "env.out") {
      found = it->path();
      break;
    }
  }
  ASSERT_FALSE(found.empty()) << "env.out not found:\n" << diag();

  std::ifstream f(found);
  std::string contents((std::istreambuf_iterator<char>(f)),
                       std::istreambuf_iterator<char>());
  EXPECT_NE(string::npos, contents.find("MEDIOS_TEST_ENV=spike-r3-value"))
      << "env.out: '" << contents << "'\n" << diag();
}


// ----------------- R4 / #148: Stdout/stderr capture -----------------

TEST_F(DockerShellTest, DOCKER_StdoutAndStderrCapturedToSandbox)
{
  const int controllerPort = startController();
  ASSERT_GT(controllerPort, 0) << controllerLogs;
  ASSERT_TRUE(startNode(controllerPort, {
        "--containerizers=docker_shell",
        "--launcher_dir=" + launcherDir()
      })) << nodeLogs;

  auto task = writeTaskJson(R"({
  "name": "r4-stdio",
  "task_id": {"value": "r4-stdio-001"},
  "node_id": {"value": ""},
  "resources": [
    {"name": "cpus", "type": "SCALAR", "scalar": {"value": 0.5}},
    {"name": "mem",  "type": "SCALAR", "scalar": {"value": 128}}
  ],
  "command": {"shell": true,
              "value": "echo OUT-LINE; echo ERR-LINE 1>&2"},
  "container": {"type": "DOCKER", "docker": {"image": "alpine:latest"}}
})", "r4-stdio");

  string output;
  const int exitCode = runMediosExecute(controllerPort, {
        "--task=file://" + task.string()
      }, output);

  const auto diag = [&]() {
    return "--- medios-execute (exit " + std::to_string(exitCode) + ") ---\n"
         + output + "\n--- node ---\n" + nodeLogs;
  };

  ASSERT_NE(string::npos, output.find("TASK_FINISHED")) << diag();

  // Find sandbox stdout/stderr files containing our marker lines. Multiple
  // stdout files may exist under nodeWorkDir (executor + task); the one
  // we want is whichever contains OUT-LINE.
  std::filesystem::path stdoutPath, stderrPath;
  for (auto it = std::filesystem::recursive_directory_iterator(nodeWorkDir);
       it != std::filesystem::recursive_directory_iterator(); ++it) {
    if (!it->is_regular_file()) continue;
    if (it->path().filename() == "stdout") {
      std::ifstream f(it->path());
      std::string c((std::istreambuf_iterator<char>(f)),
                    std::istreambuf_iterator<char>());
      if (c.find("OUT-LINE") != string::npos) stdoutPath = it->path();
    } else if (it->path().filename() == "stderr") {
      std::ifstream f(it->path());
      std::string c((std::istreambuf_iterator<char>(f)),
                    std::istreambuf_iterator<char>());
      if (c.find("ERR-LINE") != string::npos) stderrPath = it->path();
    }
  }

  EXPECT_FALSE(stdoutPath.empty())
      << "Container stdout not captured to any sandbox stdout file:\n" << diag();
  EXPECT_FALSE(stderrPath.empty())
      << "Container stderr not captured to any sandbox stderr file:\n" << diag();
}


// ----------------- Out-of-contract task rejection -----------------
//
// The rejection happens upstream of any containerizer launch, so no Docker
// daemon is needed. This fixture inherits from TaskJsonClusterFixture
// directly to skip DockerShellTest's daemon probe.
class ContainerizerContractTest : public TaskJsonClusterFixture
{
};


TEST_F(ContainerizerContractTest, BareShellTaskUnderDockerShellRejectedAsTaskError)
{
  const int controllerPort = startController();
  ASSERT_GT(controllerPort, 0) << controllerLogs;
  ASSERT_TRUE(startNode(controllerPort, {
        "--containerizers=docker_shell",
        "--launcher_dir=" + launcherDir()
      })) << nodeLogs;

  // CommandInfo but no container field: no configured containerizer can
  // launch this. The node MUST reject with TASK_ERROR + REASON_TASK_INVALID
  // and a status message naming the configured containerizers.
  auto task = writeTaskJson(R"({
  "name": "bare-shell",
  "task_id": {"value": "bare-shell-001"},
  "node_id": {"value": ""},
  "resources": [
    {"name": "cpus", "type": "SCALAR", "scalar": {"value": 0.5}},
    {"name": "mem",  "type": "SCALAR", "scalar": {"value": 128}}
  ],
  "command": {"shell": true, "value": "echo hello"}
})", "bare-shell");

  string output;
  const int exitCode = runMediosExecute(controllerPort, {
        "--task=file://" + task.string()
      }, output);

  const auto diag = [&]() {
    return "--- medios-execute (exit " + std::to_string(exitCode) + ") ---\n"
         + output + "\n--- node ---\n" + nodeLogs;
  };

  EXPECT_NE(string::npos, output.find("TASK_ERROR")) << diag();
  EXPECT_NE(string::npos, output.find("REASON_TASK_INVALID")) << diag();

  // Message must name the configured containerizers so the framework can
  // act on the diagnostic.
  EXPECT_NE(string::npos, output.find("docker_shell"))
      << "rejection message should name the configured containerizer:\n"
      << diag();

  // Negative assertions: the failure must not masquerade as a run failure,
  // and the task must not progress past rejection.
  EXPECT_EQ(string::npos, output.find("TASK_FAILED"))
      << "rejection must not surface as TASK_FAILED:\n" << diag();
  EXPECT_EQ(string::npos, output.find("TASK_STARTING"))
      << "rejected task must not start:\n" << diag();
  EXPECT_EQ(string::npos, output.find("TASK_RUNNING"))
      << "rejected task must not run:\n" << diag();
}


// ----------------- #215: ContainerInfo.volumes bind mount -----------------

// Container reads a file the host wrote into a bind-mounted host directory,
// and writes its own file back. Host verifies both the read and the write.
// This is the canonical bind-mount use case (workspace shared across stages
// in a pipeline framework).
TEST_F(DockerShellTest, DOCKER_VolumeHostPathBindMountReadAndWrite)
{
  const int controllerPort = startController();
  ASSERT_GT(controllerPort, 0) << controllerLogs;
  ASSERT_TRUE(startNode(controllerPort, {
        "--containerizers=docker_shell",
        "--launcher_dir=" + launcherDir()
      })) << nodeLogs;

  // Pre-stage a host directory the container will see.
  const auto hostShare = testRoot() / "vol-share";
  std::filesystem::remove_all(hostShare);
  std::filesystem::create_directories(hostShare);
  {
    std::ofstream f(hostShare / "from-host.txt");
    f << "HELLO-FROM-HOST";
  }

  const std::string hostPath = hostShare.string();
  const std::string containerPath = "/work";

  auto task = writeTaskJson(std::string(R"({
  "name": "vol-rw",
  "task_id": {"value": "vol-rw-001"},
  "node_id": {"value": ""},
  "resources": [
    {"name": "cpus", "type": "SCALAR", "scalar": {"value": 0.5}},
    {"name": "mem",  "type": "SCALAR", "scalar": {"value": 128}}
  ],
  "command": {"shell": true,
              "value": "cat /work/from-host.txt && echo HELLO-FROM-CONTAINER > /work/from-container.txt"},
  "container": {
    "type": "DOCKER",
    "docker": {"image": "alpine:latest"},
    "volumes": [
      {"host_path": ")") + hostPath + R"(", "container_path": "/work", "mode": "RW"}
    ]
  }
})", "vol-rw");

  string output;
  const int exitCode = runMediosExecute(controllerPort, {
        "--task=file://" + task.string()
      }, output);

  const auto diag = [&]() {
    return "--- medios-execute (exit " + std::to_string(exitCode) + ") ---\n"
         + output + "\n--- node ---\n" + nodeLogs;
  };

  ASSERT_NE(string::npos, output.find("TASK_FINISHED")) << diag();

  // Host can read what the container wrote.
  const auto containerWrote = hostShare / "from-container.txt";
  ASSERT_TRUE(std::filesystem::exists(containerWrote))
      << "container did not write to bind-mounted volume:\n" << diag();
  std::ifstream f(containerWrote);
  std::string contents((std::istreambuf_iterator<char>(f)),
                       std::istreambuf_iterator<char>());
  EXPECT_NE(string::npos, contents.find("HELLO-FROM-CONTAINER"))
      << "unexpected content in container-written file: '" << contents
      << "'\n" << diag();

  std::filesystem::remove_all(hostShare);
}


// Read-only mode: container can read but writes fail.
TEST_F(DockerShellTest, DOCKER_VolumeReadOnlyModeBlocksContainerWrites)
{
  const int controllerPort = startController();
  ASSERT_GT(controllerPort, 0) << controllerLogs;
  ASSERT_TRUE(startNode(controllerPort, {
        "--containerizers=docker_shell",
        "--launcher_dir=" + launcherDir()
      })) << nodeLogs;

  const auto hostShare = testRoot() / "vol-share-ro";
  std::filesystem::remove_all(hostShare);
  std::filesystem::create_directories(hostShare);
  {
    std::ofstream f(hostShare / "readable.txt");
    f << "READ-ONLY-CONTENT";
  }

  auto task = writeTaskJson(std::string(R"json({
  "name": "vol-ro",
  "task_id": {"value": "vol-ro-001"},
  "node_id": {"value": ""},
  "resources": [
    {"name": "cpus", "type": "SCALAR", "scalar": {"value": 0.5}},
    {"name": "mem",  "type": "SCALAR", "scalar": {"value": 128}}
  ],
  "command": {"shell": true,
              "value": "cat /work/readable.txt && (echo nope > /work/forbidden.txt && exit 100 || exit 0)"},
  "container": {
    "type": "DOCKER",
    "docker": {"image": "alpine:latest"},
    "volumes": [
      {"host_path": ")json") + hostShare.string() + R"json(", "container_path": "/work", "mode": "RO"}
    ]
  }
})json", "vol-ro");

  string output;
  const int exitCode = runMediosExecute(controllerPort, {
        "--task=file://" + task.string()
      }, output);

  const auto diag = [&]() {
    return "--- medios-execute (exit " + std::to_string(exitCode) + ") ---\n"
         + output + "\n--- node ---\n" + nodeLogs;
  };

  ASSERT_NE(string::npos, output.find("TASK_FINISHED")) << diag();

  // Host file was not modified — container's write attempt was blocked.
  EXPECT_FALSE(std::filesystem::exists(hostShare / "forbidden.txt"))
      << "RO mount allowed container write:\n" << diag();

  std::filesystem::remove_all(hostShare);
}


// Relative host_path is out of contract — executor rejects with TASK_ERROR
// + REASON_TASK_INVALID + a message naming the constraint, before any
// docker invocation. No silent normalization to sandbox-relative.
TEST_F(DockerShellTest, DOCKER_VolumeRelativeHostPathRejectedAsTaskError)
{
  const int controllerPort = startController();
  ASSERT_GT(controllerPort, 0) << controllerLogs;
  ASSERT_TRUE(startNode(controllerPort, {
        "--containerizers=docker_shell",
        "--launcher_dir=" + launcherDir()
      })) << nodeLogs;

  auto task = writeTaskJson(R"({
  "name": "vol-relative",
  "task_id": {"value": "vol-relative-001"},
  "node_id": {"value": ""},
  "resources": [
    {"name": "cpus", "type": "SCALAR", "scalar": {"value": 0.5}},
    {"name": "mem",  "type": "SCALAR", "scalar": {"value": 128}}
  ],
  "command": {"shell": true, "value": "echo unreachable"},
  "container": {
    "type": "DOCKER",
    "docker": {"image": "alpine:latest"},
    "volumes": [
      {"host_path": "relative/path", "container_path": "/work", "mode": "RW"}
    ]
  }
})", "vol-relative");

  string output;
  const int exitCode = runMediosExecute(controllerPort, {
        "--task=file://" + task.string()
      }, output);

  const auto diag = [&]() {
    return "--- medios-execute (exit " + std::to_string(exitCode) + ") ---\n"
         + output + "\n--- node ---\n" + nodeLogs;
  };

  EXPECT_NE(string::npos, output.find("TASK_ERROR")) << diag();
  EXPECT_NE(string::npos, output.find("REASON_TASK_INVALID")) << diag();
  EXPECT_NE(string::npos, output.find("absolute"))
      << "message should name the absolute-path requirement:\n" << diag();

  EXPECT_EQ(string::npos, output.find("TASK_FINISHED"))
      << "out-of-contract task must not run to FINISHED:\n" << diag();
  EXPECT_EQ(string::npos, output.find("unreachable"))
      << "the task command must not execute:\n" << diag();
}


// ----------------- #225: force_pull_image semantics -----------------

// Default (force_pull_image unset): use the local image if present, skip
// pull. Tag alpine:latest as a name no registry resolves; task must still
// run via the local cache.
TEST_F(DockerShellTest, DOCKER_DefaultUsesLocalImageWithoutPull)
{
  const std::string localTag = "medios-local-only-test:v1";
  ASSERT_EQ(0, std::system(
      ("docker tag alpine:latest " + localTag + " 2>/dev/null").c_str()))
      << "failed to tag alpine:latest -> " << localTag;

  const int controllerPort = startController();
  ASSERT_GT(controllerPort, 0) << controllerLogs;
  ASSERT_TRUE(startNode(controllerPort, {
        "--containerizers=docker_shell",
        "--launcher_dir=" + launcherDir()
      })) << nodeLogs;

  auto task = writeTaskJson(std::string(R"({
  "name": "local-default",
  "task_id": {"value": "local-default-001"},
  "node_id": {"value": ""},
  "resources": [
    {"name": "cpus", "type": "SCALAR", "scalar": {"value": 0.5}},
    {"name": "mem",  "type": "SCALAR", "scalar": {"value": 128}}
  ],
  "command": {"shell": true, "value": "echo LOCAL-IMAGE-OK"},
  "container": {"type": "DOCKER", "docker": {"image": ")") + localTag + R"("}}
})", "local-default");

  string output;
  const int exitCode = runMediosExecute(controllerPort, {
        "--task=file://" + task.string()
      }, output);

  const auto diag = [&]() {
    return "--- medios-execute (exit " + std::to_string(exitCode) + ") ---\n"
         + output + "\n--- node ---\n" + nodeLogs;
  };

  EXPECT_NE(string::npos, output.find("TASK_FINISHED")) << diag();
  EXPECT_EQ(string::npos, output.find("TASK_FAILED"))
      << "default behavior must use local cache when image is present:\n"
      << diag();

  std::system(("docker rmi " + localTag + " >/dev/null 2>&1").c_str());
}


// Image is neither in local cache nor pullable: TASK_FAILED with a message
// that names the local-cache miss explicitly.
TEST_F(DockerShellTest, DOCKER_MissingImagePullAndLocalBothFailedTaskFailed)
{
  const std::string ghostImage =
      "medios-ghost-" + std::to_string(::getpid()) + ":v1";
  std::system(("docker rmi " + ghostImage + " >/dev/null 2>&1").c_str());

  const int controllerPort = startController();
  ASSERT_GT(controllerPort, 0) << controllerLogs;
  ASSERT_TRUE(startNode(controllerPort, {
        "--containerizers=docker_shell",
        "--launcher_dir=" + launcherDir()
      })) << nodeLogs;

  auto task = writeTaskJson(std::string(R"({
  "name": "ghost",
  "task_id": {"value": "ghost-001"},
  "node_id": {"value": ""},
  "resources": [
    {"name": "cpus", "type": "SCALAR", "scalar": {"value": 0.5}},
    {"name": "mem",  "type": "SCALAR", "scalar": {"value": 128}}
  ],
  "command": {"shell": true, "value": "echo unreachable"},
  "container": {"type": "DOCKER", "docker": {"image": ")") + ghostImage + R"("}}
})", "ghost");

  string output;
  const int exitCode = runMediosExecute(controllerPort, {
        "--task=file://" + task.string()
      }, output);

  const auto diag = [&]() {
    return "--- medios-execute (exit " + std::to_string(exitCode) + ") ---\n"
         + output + "\n--- node ---\n" + nodeLogs;
  };

  EXPECT_NE(string::npos, output.find("TASK_FAILED")) << diag();
  EXPECT_NE(string::npos, output.find("not in local cache"))
      << "message must name the cache-miss path, not just the pull:\n"
      << diag();

  EXPECT_EQ(string::npos, output.find("TASK_FINISHED"))
      << "missing-image task must not run to FINISHED:\n" << diag();
  EXPECT_EQ(string::npos, output.find("unreachable"))
      << "the task command must not execute:\n" << diag();
}
// ----------------- #511: container id on the task status -----------------

// The controller's /state is where a framework reads a task's
// container_status. It only carries a container_id if the node's
// containerizer reported one for the container it launched.
TEST_F(DockerShellTest, DOCKER_TaskStatusCarriesContainerIdInControllerState)
{
  const int controllerPort = startController();
  ASSERT_GT(controllerPort, 0) << controllerLogs;
  ASSERT_TRUE(startNode(controllerPort, {
        "--containerizers=docker_shell",
        "--launcher_dir=" + launcherDir()
      })) << nodeLogs;

  auto task = writeTaskJson(R"({
  "name": "c6-container-id",
  "task_id": {"value": "c6-container-id-001"},
  "node_id": {"value": ""},
  "resources": [
    {"name": "cpus", "type": "SCALAR", "scalar": {"value": 0.5}},
    {"name": "mem",  "type": "SCALAR", "scalar": {"value": 128}}
  ],
  "command": {"shell": true, "value": "echo container-id-probe"},
  "container": {"type": "DOCKER", "docker": {"image": "alpine:latest"}}
})", "c6-container-id");

  string output;
  const int exitCode = runMediosExecute(controllerPort, {
        "--task=file://" + task.string()
      }, output);

  const auto diag = [&]() {
    return "--- medios-execute (exit " + std::to_string(exitCode) + ") ---\n"
         + output + "\n--- node ---\n" + nodeLogs;
  };

  ASSERT_NE(string::npos, output.find("TASK_FINISHED")) << diag();

  const string state = httpGet(controllerPort, "/state");
  ASSERT_FALSE(state.empty())
      << "could not read the controller's /state:\n" << diag();

  const auto taskAt = state.find("c6-container-id-001");
  ASSERT_NE(string::npos, taskAt)
      << "task absent from /state:\n" << state << "\n" << diag();

  const auto statusAt = state.find("\"container_status\"", taskAt);
  ASSERT_NE(string::npos, statusAt)
      << "no container_status on the task's statuses:\n"
      << state.substr(taskAt) << "\n" << diag();

  const auto idAt = state.find("\"container_id\"", statusAt);
  ASSERT_NE(string::npos, idAt)
      << "container_status carries no container_id:\n"
      << state.substr(statusAt) << "\n" << diag();

  // The id must be a value, not an empty placeholder.
  const auto valueAt = state.find("\"value\"", idAt);
  ASSERT_NE(string::npos, valueAt) << state.substr(idAt);
  const auto openQuote = state.find('"', state.find(':', valueAt));
  ASSERT_NE(string::npos, openQuote) << state.substr(idAt);
  const auto closeQuote = state.find('"', openQuote + 1);
  ASSERT_NE(string::npos, closeQuote) << state.substr(idAt);
  EXPECT_GT(closeQuote - openQuote, 1u)
      << "container_id.value is empty:\n" << state.substr(idAt);
}

} // namespace
