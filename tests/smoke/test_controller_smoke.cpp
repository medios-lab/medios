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

#include <arpa/inet.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <signal.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>

#include <chrono>
#include <cerrno>
#include <cstring>
#include <filesystem>
#include <optional>
#include <regex>
#include <string>
#include <thread>
#include <vector>

#include <gtest/gtest.h>

#include <medios/version.hpp>

using std::chrono::steady_clock;
using std::chrono::milliseconds;
using std::chrono::seconds;
using std::optional;
using std::string;
using std::vector;


namespace {

constexpr seconds TEST_TIMEOUT(10);


string errnoMessage()
{
  return std::strerror(errno);
}


bool setNonBlock(int fd)
{
  int flags = fcntl(fd, F_GETFL, 0);
  if (flags == -1) {
    return false;
  }

  return fcntl(fd, F_SETFL, flags | O_NONBLOCK) != -1;
}


string readAvailable(int fd)
{
  string output;
  char buffer[4096];

  while (true) {
    ssize_t bytes = read(fd, buffer, sizeof(buffer));

    if (bytes > 0) {
      output.append(buffer, static_cast<size_t>(bytes));
      continue;
    }

    if (bytes == 0 || errno == EAGAIN || errno == EWOULDBLOCK) {
      return output;
    }

    if (errno == EINTR) {
      continue;
    }

    return output;
  }
}


optional<int> parseControllerPort(const string& logs)
{
  std::smatch match;
  const std::regex pattern("started on .*:([0-9]+)");

  if (std::regex_search(logs, match, pattern)) {
    return std::stoi(match[1].str());
  }

  return std::nullopt;
}


optional<string> httpGetVersion(int port)
{
  int fd = socket(AF_INET, SOCK_STREAM, 0);
  if (fd == -1) {
    return std::nullopt;
  }

  timeval timeout;
  timeout.tv_sec = 1;
  timeout.tv_usec = 0;
  setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout));
  setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO, &timeout, sizeof(timeout));

  sockaddr_in address;
  std::memset(&address, 0, sizeof(address));
  address.sin_family = AF_INET;
  address.sin_port = htons(static_cast<uint16_t>(port));

  if (inet_pton(AF_INET, "127.0.0.1", &address.sin_addr) != 1) {
    close(fd);
    return std::nullopt;
  }

  if (connect(fd, reinterpret_cast<sockaddr*>(&address), sizeof(address)) == -1) {
    close(fd);
    return std::nullopt;
  }

  const string request =
    "GET /version HTTP/1.1\r\n"
    "Host: 127.0.0.1\r\n"
    "Connection: close\r\n"
    "\r\n";

  ssize_t written = send(fd, request.data(), request.size(), 0);
  if (written < 0 || static_cast<size_t>(written) != request.size()) {
    close(fd);
    return std::nullopt;
  }

  string response;
  char buffer[4096];

  while (true) {
    ssize_t bytes = recv(fd, buffer, sizeof(buffer), 0);

    if (bytes > 0) {
      response.append(buffer, static_cast<size_t>(bytes));
      continue;
    }

    if (bytes == 0) {
      close(fd);
      return response;
    }

    if (errno == EINTR) {
      continue;
    }

    close(fd);
    return std::nullopt;
  }
}


class ControllerSmokeTest : public ::testing::Test
{
protected:
  void TearDown() override
  {
    if (controllerPid > 0) {
      kill(controllerPid, SIGKILL);
      waitpid(controllerPid, nullptr, 0);
      controllerPid = -1;
    }

    if (logFd != -1) {
      close(logFd);
      logFd = -1;
    }

    if (!workDir.empty()) {
      std::filesystem::remove_all(workDir);
    }
  }

  void startController()
  {
    int pipeFds[2];
    ASSERT_EQ(0, pipe(pipeFds)) << errnoMessage();

    ASSERT_TRUE(setNonBlock(pipeFds[0])) << errnoMessage();

    workDir = std::filesystem::temp_directory_path() /
      ("medios-ai-controller-smoke-" + std::to_string(getpid()));

    std::filesystem::remove_all(workDir);
    std::filesystem::create_directories(workDir);

    vector<string> args = {
      MEDIOS_CONTROLLER_BINARY,
      "--ip=127.0.0.1",
      "--port=0",
      "--registry=in_memory",
      "--work_dir=" + workDir.string(),
      "--logbufsecs=0"
    };

    controllerPid = fork();
    ASSERT_GE(controllerPid, 0) << errnoMessage();

    if (controllerPid == 0) {
      close(pipeFds[0]);
      dup2(pipeFds[1], STDOUT_FILENO);
      dup2(pipeFds[1], STDERR_FILENO);
      close(pipeFds[1]);

      vector<char*> argv;
      for (string& arg : args) {
        argv.push_back(arg.data());
      }
      argv.push_back(nullptr);

      execv(args.front().c_str(), argv.data());
      _exit(127);
    }

    close(pipeFds[1]);
    logFd = pipeFds[0];
  }

  optional<int> waitForPort(const steady_clock::time_point& deadline)
  {
    while (steady_clock::now() < deadline) {
      logs += readAvailable(logFd);

      optional<int> port = parseControllerPort(logs);
      if (port.has_value()) {
        return port;
      }

      int status;
      pid_t result = waitpid(controllerPid, &status, WNOHANG);
      if (result == controllerPid) {
        controllerPid = -1;
        return std::nullopt;
      }

      std::this_thread::sleep_for(milliseconds(50));
    }

    return std::nullopt;
  }

  optional<string> waitForVersion(
      int port,
      const steady_clock::time_point& deadline)
  {
    while (steady_clock::now() < deadline) {
      optional<string> response = httpGetVersion(port);
      if (response.has_value() && response->find(" 200 ") != string::npos) {
        return response;
      }

      std::this_thread::sleep_for(milliseconds(50));
    }

    return std::nullopt;
  }

  bool stopController()
  {
    if (controllerPid <= 0) {
      return true;
    }

    kill(controllerPid, SIGTERM);
    const steady_clock::time_point deadline = steady_clock::now() + seconds(2);

    while (steady_clock::now() < deadline) {
      int status;
      pid_t result = waitpid(controllerPid, &status, WNOHANG);

      if (result == controllerPid) {
        controllerPid = -1;
        return true;
      }

      std::this_thread::sleep_for(milliseconds(50));
    }

    return false;
  }

  pid_t controllerPid = -1;
  int logFd = -1;
  std::filesystem::path workDir;
  string logs;
};


TEST_F(ControllerSmokeTest, StartsAndServesVersion)
{
  startController();

  const steady_clock::time_point deadline = steady_clock::now() + TEST_TIMEOUT;

  optional<int> port = waitForPort(deadline);
  ASSERT_TRUE(port.has_value()) << logs;

  optional<string> response = waitForVersion(port.value(), deadline);
  ASSERT_TRUE(response.has_value()) << logs;
  EXPECT_NE(string::npos, response->find("\"version\":\"" MEDIOS_VERSION "\""))
    << response.value();

  EXPECT_TRUE(stopController()) << logs;
}

} // namespace
