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

// PHASE-A acceptance test: hello-world end-to-end (BUILD-006 / #56).
//
// Spawns medios-controller + medios-node, runs medios-execute with an
// `echo` command, asserts the TASK_STARTING → TASK_RUNNING → TASK_FINISHED
// status sequence and a clean exit.
//
// Subprocess / log / lifecycle helpers live in cluster_harness.hpp.

#include <string>

#include <gtest/gtest.h>

#include "cluster_harness.hpp"

using std::string;

using namespace medios::tests::e2e;

namespace {

class HelloWorldTest : public ClusterFixture {};


TEST_F(HelloWorldTest, TaskFinishesSuccessfully)
{
  const int controllerPort = startController();
  ASSERT_GT(controllerPort, 0) << "controller failed to bind a port:\n"
                                << controllerLogs;

  // Hello-world uses the legacy medios containerizer with posix launcher
  // and no isolation (PHASE-A dev shape).
  ASSERT_TRUE(startNode(controllerPort, {
        "--containerizers=medios",
        "--launcher=posix",
        "--isolation="
      }))
      << "node failed to register with controller:\n"
      << "--- node logs ---\n" << nodeLogs
      << "--- controller logs ---\n" << controllerLogs;

  string executeOutput;
  const int exitCode = runMediosExecute(controllerPort, {
        "--name=hello-medios",
        "--command=echo hello-medios"
      }, executeOutput);

  const auto diagnostics = [&]() {
    return "--- medios-execute (exit " + std::to_string(exitCode) + ") ---\n"
         + executeOutput
         + "\n--- node logs ---\n" + nodeLogs
         + "\n--- controller logs ---\n" + controllerLogs;
  };

  EXPECT_EQ(0, exitCode) << diagnostics();
  EXPECT_NE(string::npos, executeOutput.find("TASK_STARTING")) << diagnostics();
  EXPECT_NE(string::npos, executeOutput.find("TASK_RUNNING")) << diagnostics();
  EXPECT_NE(string::npos, executeOutput.find("TASK_FINISHED")) << diagnostics();
}

} // namespace
