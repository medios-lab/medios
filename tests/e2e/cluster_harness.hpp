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

#ifndef __MEDIOS_TESTS_E2E_CLUSTER_HARNESS_HPP__
#define __MEDIOS_TESTS_E2E_CLUSTER_HARNESS_HPP__

// Shared subprocess + lifecycle helpers for e2e tests that spawn
// medios-controller + medios-node + medios-execute as real processes
// and assert on their output. Used by tests/e2e/test_hello_world.cpp and
// tests/e2e/test_docker_shell.cpp.

#include <chrono>
#include <filesystem>
#include <functional>
#include <optional>
#include <string>
#include <vector>

#include <sys/types.h>

#include <gtest/gtest.h>

namespace medios::tests::e2e {

constexpr std::chrono::seconds DAEMON_STARTUP_TIMEOUT{15};
constexpr std::chrono::seconds EXECUTE_TIMEOUT{30};


// --- Subprocess helpers ---

// Spawn a subprocess. Stdout + stderr are merged into a pipe whose read
// end is returned via *logFd (caller closes). Returns child pid, -1 on
// fork failure. `env` entries ("NAME=value") are added to the inherited
// environment; a daemon's TLS settings travel this way, since libprocess
// reads LIBPROCESS_SSL_* from the environment rather than from flags.
//
// Inherited LIBPROCESS_* entries are dropped first. A medios executor
// exports LIBPROCESS_IP, LIBPROCESS_ADVERTISE_IP and LIBPROCESS_PORT to
// every task, so a test running as a task would otherwise spawn daemons
// that advertise the host's address instead of the loopback address
// their flags name; under TLS the peer then dials an address the test
// certificates do not cover.
pid_t spawnSubprocess(
    const std::vector<std::string>& args,
    int* logFd,
    const std::vector<std::string>& env = {});

// Drain available bytes from a non-blocking fd into a string.
std::string readAvailable(int fd);

// Set O_NONBLOCK on an fd. Returns false on failure.
bool setNonBlock(int fd);

// Match libprocess's "started on <ip>:<port>" startup line in a log
// buffer. Returns the bound port or nullopt.
std::optional<int> parseBoundPort(const std::string& logs);

// Read into `logs` until `predicate(logs)` is true or `deadline` passes.
// Returns true if predicate fired; false on timeout or child exit. A
// child that exits is reaped here, so its status is handed back through
// `exitStatus` (128 + signal for a signalled child); nothing else can
// wait for it afterwards.
bool waitForLog(
    pid_t childPid,
    int logFd,
    std::string& logs,
    const std::function<bool(const std::string&)>& predicate,
    const std::chrono::steady_clock::time_point& deadline,
    std::optional<int>& exitStatus);

// Read into `logs` until the child exits or `deadline` passes. Returns
// the exit status (128 + signal for a signalled child), or nullopt if the
// child is still running at the deadline.
std::optional<int> waitForExit(
    pid_t childPid,
    int logFd,
    std::string& logs,
    const std::chrono::steady_clock::time_point& deadline);

// Stop a child process: SIGTERM with deadline, then SIGKILL.
void stopChild(pid_t pid);


// --- ClusterFixture: controller + node lifecycle ---

// Base fixture for e2e tests. Spawns medios-controller (kernel-assigned
// port) and medios-node, holds their pids, and tears everything down in
// TearDown. Subclasses fill in test bodies.
//
// Configure the node with `setExtraNodeArgs()` before calling startNode().
// Defaults (medios containerizer + posix launcher + no isolation) match
// the historical test_hello_world dev flow.
//
// Test work dirs live under testRoot() (default ~/.medios-e2e-test) so
// they're accessible to Docker backends that only share $HOME (Colima).
class ClusterFixture : public ::testing::Test
{
protected:
  void TearDown() override;

  // Returns the controller's bound port, or 0 on failure. `env` is
  // added to the controller's environment (see spawnSubprocess). A
  // controller that exits before reporting a port leaves its status in
  // controllerExitStatus and controllerPid at -1.
  int startController(const std::vector<std::string>& env = {});

  // Returns true once the node has registered with the controller.
  // `extraNodeArgs` are appended to the medios-node argv after the
  // standard defaults. Use to swap containerizer mode, add
  // --launcher_dir, etc. `env` is added to the node's environment.
  bool startNode(int controllerPort,
                 const std::vector<std::string>& extraNodeArgs = {},
                 const std::vector<std::string>& env = {});

  // Run medios-execute against the cluster with the given CLI args
  // (excluding --controller, which is added automatically). Captures
  // combined output and returns the exit code.
  int runMediosExecute(int controllerPort,
                      const std::vector<std::string>& executeArgs,
                      std::string& output);

  // GET `path` from the controller over HTTP and return the response
  // body. Empty on failure.
  std::string httpGet(int controllerPort, const std::string& path);

  // Root dir for test work directories. $HOME-relative on macOS so
  // Docker backends (Colima, Docker Desktop) can share it into the VM.
  std::filesystem::path testRoot() const;

  pid_t controllerPid = -1;
  pid_t nodePid = -1;
  // Set when the daemon exited while startController/startNode waited.
  std::optional<int> controllerExitStatus;
  std::optional<int> nodeExitStatus;
  int controllerLogFd = -1;
  int nodeLogFd = -1;
  std::string controllerLogs;
  std::string nodeLogs;
  std::filesystem::path controllerWorkDir;
  std::filesystem::path nodeWorkDir;
};

} // namespace medios::tests::e2e

#endif // __MEDIOS_TESTS_E2E_CLUSTER_HARNESS_HPP__
