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

#ifndef __TEST_CONTAINERIZER_HPP__
#define __TEST_CONTAINERIZER_HPP__

#include <unistd.h>

#include <map>
#include <memory>
#include <string>

#include <medios/executor.hpp>
#include <medios/medios.hpp>
#include <medios/resources.hpp>
#include <medios/type_utils.hpp>

#include <medios/v1/executor.hpp>

#include <process/dispatch.hpp>
#include <process/future.hpp>
#include <process/gmock.hpp>
#include <process/http.hpp>
#include <process/pid.hpp>

#include <stout/hashmap.hpp>
#include <stout/os.hpp>
#include <stout/try.hpp>
#include <stout/uuid.hpp>

#include "node/containerizer/containerizer.hpp"

#include "node/node.hpp"
#include "node/state.hpp"

#include "tests/medios.hpp"

namespace medios {
namespace internal {
namespace tests {

// Forward declaration.
class MockExecutor;
class TestContainerizerProcess;


class TestContainerizer : public node::Containerizer
{
public:
  // TODO(bmahler): These constructors assume that ExecutorIDs are
  // unique across FrameworkIDs, which is not the case.
  TestContainerizer(
      const ExecutorID& executorId,
      const std::shared_ptr<v1::MockHTTPExecutor>& executor);

  TestContainerizer(const hashmap<ExecutorID, Executor*>& executors);

  TestContainerizer(const ExecutorID& executorId, Executor* executor);

  explicit TestContainerizer(MockExecutor* executor);

  TestContainerizer();

  ~TestContainerizer() override;

  process::Future<hashset<ContainerID>> containers() override;

  MOCK_METHOD1(
      recover,
      process::Future<Nothing>(const Option<node::state::NodeState>&));

  MOCK_METHOD4(
      launch,
      process::Future<node::Containerizer::LaunchResult>(
          const ContainerID&,
          const medios::node::ContainerConfig&,
          const std::map<std::string, std::string>&,
          const Option<std::string>&));

  MOCK_METHOD1(
      attach,
      process::Future<process::http::Connection>(
          const ContainerID& containerId));

  MOCK_METHOD3(
      update,
      process::Future<Nothing>(
          const ContainerID&,
          const Resources&,
          const google::protobuf::Map<std::string, Value::Scalar>&));

  MOCK_METHOD1(
      usage,
      process::Future<ResourceStatistics>(const ContainerID&));

  MOCK_METHOD1(
      status,
      process::Future<ContainerStatus>(const ContainerID&));

  MOCK_METHOD1(
      wait,
      process::Future<Option<medios::node::ContainerTermination>>(
          const ContainerID&));

  MOCK_METHOD1(
      destroy,
      process::Future<Option<medios::node::ContainerTermination>>(
          const ContainerID&));

  MOCK_METHOD2(
      kill,
      process::Future<bool>(const ContainerID&, int));

  MOCK_METHOD1(
      pruneImages,
      process::Future<Nothing>(const std::vector<Image>&));

  // Additional destroy method for testing because we won't know the
  // ContainerID created for each container.
  process::Future<Option<medios::node::ContainerTermination>> destroy(
      const FrameworkID& frameworkId,
      const ExecutorID& executorId);

private:
  void setup();

  // The following functions act as a level of indirection to
  // perform the dispatch while still allowing the above to be
  // mock functions.

  process::Future<Nothing> _recover(
      const Option<node::state::NodeState>& state);

  process::Future<node::Containerizer::LaunchResult> _launch(
      const ContainerID& containerId,
      const medios::node::ContainerConfig& containerConfig,
      const std::map<std::string, std::string>& environment,
      const Option<std::string>& pidCheckpointPath);

  process::Future<process::http::Connection> _attach(
      const ContainerID& containerId);

  process::Future<Nothing> _update(
      const ContainerID& containerId,
      const Resources& resourceRequests,
      const google::protobuf::Map<std::string, Value::Scalar>& resourceLimits);

  process::Future<ResourceStatistics> _usage(
      const ContainerID& containerId);

  process::Future<ContainerStatus> _status(
      const ContainerID& containerId);

  process::Future<Option<medios::node::ContainerTermination>> _wait(
      const ContainerID& containerId);

  process::Future<Option<medios::node::ContainerTermination>> _destroy(
      const ContainerID& containerId);

  process::Future<bool> _kill(
      const ContainerID& containerId,
      int status);

  process::Future<Nothing> _pruneImages(
      const std::vector<Image>& excludedImages);

  process::Owned<TestContainerizerProcess> process;
};

} // namespace tests {
} // namespace internal {
} // namespace medios {

#endif // __TEST_CONTAINERIZER_HPP__
