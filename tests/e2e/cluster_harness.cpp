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

#include "cluster_harness.hpp"

#include <fcntl.h>
#include <signal.h>
#include <sys/wait.h>
#include <unistd.h>

#include <cerrno>
#include <cstdlib>
#include <cstdio>
#include <cstring>
#include <regex>
#include <thread>

// Not declared by <unistd.h> on every platform medios builds on.
extern char** environ;

using std::chrono::steady_clock;
using std::chrono::milliseconds;
using std::chrono::seconds;
using std::optional;
using std::string;
using std::vector;

namespace medios::tests::e2e {

bool setNonBlock(int fd)
{
  int flags = ::fcntl(fd, F_GETFL, 0);
  if (flags == -1) return false;
  return ::fcntl(fd, F_SETFL, flags | O_NONBLOCK) != -1;
}


string readAvailable(int fd)
{
  string out;
  char buf[4096];
  while (true) {
    ssize_t n = ::read(fd, buf, sizeof(buf));
    if (n > 0) {
      out.append(buf, static_cast<size_t>(n));
      continue;
    }
    if (n == 0) break;                     // EOF
    if (errno == EAGAIN || errno == EWOULDBLOCK) break;
    if (errno == EINTR) continue;
    break;
  }
  return out;
}


optional<int> parseBoundPort(const string& logs)
{
  std::smatch match;
  const std::regex pattern("started on .*:([0-9]+)");
  if (std::regex_search(logs, match, pattern)) {
    return std::stoi(match[1].str());
  }
  return std::nullopt;
}


pid_t spawnSubprocess(
    const vector<string>& args,
    int* logFd,
    const vector<string>& env)
{
  // Built before fork so the child does nothing that allocates.
  vector<string> childEnv;
  for (char** e = environ; *e != nullptr; ++e) {
    if (strncmp(*e, "LIBPROCESS_", 11) == 0) continue;
    childEnv.emplace_back(*e);
  }
  childEnv.insert(childEnv.end(), env.begin(), env.end());
  vector<char*> envp;
  envp.reserve(childEnv.size() + 1);
  for (const string& e : childEnv) {
    envp.push_back(const_cast<char*>(e.c_str()));
  }
  envp.push_back(nullptr);

  int pipeFds[2];
  if (::pipe(pipeFds) != 0) {
    return -1;
  }
  if (!setNonBlock(pipeFds[0])) {
    ::close(pipeFds[0]);
    ::close(pipeFds[1]);
    return -1;
  }

  pid_t pid = ::fork();
  if (pid < 0) {
    ::close(pipeFds[0]);
    ::close(pipeFds[1]);
    return -1;
  }

  if (pid == 0) {
    // Child: stdout + stderr → write end of the pipe.
    ::close(pipeFds[0]);
    ::dup2(pipeFds[1], STDOUT_FILENO);
    ::dup2(pipeFds[1], STDERR_FILENO);
    ::close(pipeFds[1]);

    vector<char*> argv;
    argv.reserve(args.size() + 1);
    for (const string& arg : args) {
      argv.push_back(const_cast<char*>(arg.c_str()));
    }
    argv.push_back(nullptr);

    ::execve(args.front().c_str(), argv.data(), envp.data());
    ::_exit(127);
  }

  ::close(pipeFds[1]);
  *logFd = pipeFds[0];
  return pid;
}


// Non-blocking reap. Returns the child's status once it has exited,
// nullopt while it runs.
static optional<int> reapIfExited(pid_t childPid)
{
  int status;
  pid_t result = ::waitpid(childPid, &status, WNOHANG);
  if (result != childPid) return std::nullopt;
  if (WIFEXITED(status))   return WEXITSTATUS(status);
  if (WIFSIGNALED(status)) return 128 + WTERMSIG(status);
  return -1;
}


bool waitForLog(
    pid_t childPid,
    int logFd,
    string& logs,
    const std::function<bool(const string&)>& predicate,
    const steady_clock::time_point& deadline,
    optional<int>& exitStatus)
{
  while (steady_clock::now() < deadline) {
    logs += readAvailable(logFd);
    if (predicate(logs)) return true;

    exitStatus = reapIfExited(childPid);
    if (exitStatus.has_value()) {
      logs += readAvailable(logFd);
      return false;
    }
    std::this_thread::sleep_for(milliseconds(50));
  }
  return false;
}


optional<int> waitForExit(
    pid_t childPid,
    int logFd,
    string& logs,
    const steady_clock::time_point& deadline)
{
  while (steady_clock::now() < deadline) {
    logs += readAvailable(logFd);

    optional<int> status = reapIfExited(childPid);
    if (status.has_value()) {
      logs += readAvailable(logFd);
      return status;
    }
    std::this_thread::sleep_for(milliseconds(50));
  }
  return std::nullopt;
}


void stopChild(pid_t pid)
{
  if (pid <= 0) return;

  ::kill(pid, SIGTERM);
  const steady_clock::time_point deadline =
      steady_clock::now() + seconds(3);

  while (steady_clock::now() < deadline) {
    int status;
    pid_t result = ::waitpid(pid, &status, WNOHANG);
    if (result == pid) return;
    std::this_thread::sleep_for(milliseconds(50));
  }

  ::kill(pid, SIGKILL);
  ::waitpid(pid, nullptr, 0);
}


// --- ClusterFixture ---

std::filesystem::path ClusterFixture::testRoot() const
{
  const char* home = ::getenv("HOME");
  std::filesystem::path root =
      (home && *home) ? std::filesystem::path(home) / ".medios-e2e-test"
                      : std::filesystem::path("/tmp/medios-e2e-test");
  std::filesystem::create_directories(root);
  return root;
}


void ClusterFixture::TearDown()
{
  stopChild(nodePid);
  nodePid = -1;
  stopChild(controllerPid);
  controllerPid = -1;

  if (nodeLogFd != -1)       { ::close(nodeLogFd);       nodeLogFd = -1; }
  if (controllerLogFd != -1) { ::close(controllerLogFd); controllerLogFd = -1; }

  if (!nodeWorkDir.empty())       std::filesystem::remove_all(nodeWorkDir);
  if (!controllerWorkDir.empty()) std::filesystem::remove_all(controllerWorkDir);
}


int ClusterFixture::startController(const vector<string>& env)
{
  controllerWorkDir = testRoot() /
      ("controller-" + std::to_string(::getpid()));
  std::filesystem::remove_all(controllerWorkDir);
  std::filesystem::create_directories(controllerWorkDir);

  vector<string> args = {
    MEDIOS_CONTROLLER_BINARY,
    "--ip=127.0.0.1",
    "--port=0",
    "--registry=in_memory",
    "--authenticate_frameworks=false",
    "--authenticate_nodes=false",
    "--work_dir=" + controllerWorkDir.string(),
    "--logbufsecs=0"
  };

  controllerExitStatus.reset();
  controllerPid = spawnSubprocess(args, &controllerLogFd, env);
  if (controllerPid < 0) return 0;

  const steady_clock::time_point deadline =
      steady_clock::now() + DAEMON_STARTUP_TIMEOUT;

  int port = 0;
  bool ok = waitForLog(
      controllerPid, controllerLogFd, controllerLogs,
      [&port](const string& logs) {
        optional<int> p = parseBoundPort(logs);
        if (p.has_value()) { port = *p; return true; }
        return false;
      },
      deadline,
      controllerExitStatus);

  if (controllerExitStatus.has_value()) controllerPid = -1;
  return ok ? port : 0;
}


bool ClusterFixture::startNode(
    int controllerPort,
    const vector<string>& extraNodeArgs,
    const vector<string>& env)
{
  nodeWorkDir = testRoot() / ("node-" + std::to_string(::getpid()));
  std::filesystem::remove_all(nodeWorkDir);
  std::filesystem::create_directories(nodeWorkDir);

  const string controllerAddr =
      "127.0.0.1:" + std::to_string(controllerPort);

  bool hasRuntimeDir = false;
  for (const string& a : extraNodeArgs) {
    if (a.find("--runtime_dir=") == 0) {
      hasRuntimeDir = true;
      break;
    }
  }

  vector<string> args = {
    MEDIOS_NODE_BINARY,
    "--controller=" + controllerAddr,
    "--ip=127.0.0.1",
    "--port=0",
    "--work_dir=" + nodeWorkDir.string(),
    "--resources=cpus:2;mem:1024;disk:1024",
#ifdef __linux__
    // The node tries to create a systemd slice on Linux by default;
    // unprivileged tests can't. The flag is only compiled in on Linux.
    "--no-systemd_enable_support",
#endif
    "--logbufsecs=0"
  };
  if (!hasRuntimeDir) {
    args.push_back("--runtime_dir=" + (nodeWorkDir / "runtime").string());
  }
  for (const string& a : extraNodeArgs) {
    args.push_back(a);
  }

  nodeExitStatus.reset();
  nodePid = spawnSubprocess(args, &nodeLogFd, env);
  if (nodePid < 0) return false;

  const steady_clock::time_point deadline =
      steady_clock::now() + DAEMON_STARTUP_TIMEOUT;

  bool ok = waitForLog(
      nodePid, nodeLogFd, nodeLogs,
      [](const string& logs) {
        return logs.find("Registered with controller") != string::npos;
      },
      deadline,
      nodeExitStatus);

  if (nodeExitStatus.has_value()) nodePid = -1;
  return ok;
}


string ClusterFixture::httpGet(int controllerPort, const string& path)
{
  const string command =
      "curl -sS --fail --max-time 10 http://127.0.0.1:" +
      std::to_string(controllerPort) + path;

  FILE* pipe = ::popen(command.c_str(), "r");
  if (pipe == nullptr) return "";

  string body;
  char buf[4096];
  while (::fgets(buf, sizeof(buf), pipe) != nullptr) {
    body += buf;
  }

  if (::pclose(pipe) != 0) return "";
  return body;
}


int ClusterFixture::runMediosExecute(
    int controllerPort,
    const vector<string>& executeArgs,
    string& output)
{
  const string controllerAddr =
      "127.0.0.1:" + std::to_string(controllerPort);

  vector<string> args = {
    MEDIOS_EXECUTE_BINARY,
    "--controller=" + controllerAddr
  };
  for (const string& a : executeArgs) {
    args.push_back(a);
  }

  int logFd = -1;
  pid_t pid = spawnSubprocess(args, &logFd);
  if (pid < 0) return -1;

  const steady_clock::time_point deadline =
      steady_clock::now() + EXECUTE_TIMEOUT;

  while (steady_clock::now() < deadline) {
    output += readAvailable(logFd);

    int status;
    pid_t result = ::waitpid(pid, &status, WNOHANG);
    if (result == pid) {
      output += readAvailable(logFd);
      ::close(logFd);
      if (WIFEXITED(status))   return WEXITSTATUS(status);
      if (WIFSIGNALED(status)) return 128 + WTERMSIG(status);
      return -1;
    }
    std::this_thread::sleep_for(milliseconds(100));
  }

  ::kill(pid, SIGKILL);
  ::waitpid(pid, nullptr, 0);
  ::close(logFd);
  return -1;
}

} // namespace medios::tests::e2e
