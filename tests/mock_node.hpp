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

#ifndef __TESTS_MOCKNODE_HPP__
#define __TESTS_MOCKNODE_HPP__

#include <list>
#include <string>
#include <vector>

#include <gmock/gmock.h>

#include <medios/authentication/secret_generator.hpp>

#include <medios/controller/detector.hpp>

#include <medios/node/qos_controller.hpp>
#include <medios/node/resource_estimator.hpp>

#include <process/future.hpp>
#include <process/owned.hpp>
#include <process/pid.hpp>

#include <stout/duration.hpp>
#include <stout/none.hpp>
#include <stout/option.hpp>
#include <stout/try.hpp>

#include "messages/messages.hpp"

#include "node/node.hpp"

using ::testing::_;
using ::testing::DoDefault;
using ::testing::Return;

namespace medios {
namespace internal {
namespace tests {

class MockResourceEstimator : public medios::node::ResourceEstimator
{
public:
  MockResourceEstimator();
  ~MockResourceEstimator() override;

  MOCK_METHOD1(
      initialize,
      Try<Nothing>(const lambda::function<process::Future<ResourceUsage>()>&));

  MOCK_METHOD0(
      oversubscribable,
      process::Future<Resources>());
};


// The MockQoSController is a stub which lets tests fill the
// correction queue for a node.
class MockQoSController : public medios::node::QoSController
{
public:
  MockQoSController();
  ~MockQoSController() override;

  MOCK_METHOD1(
      initialize,
      Try<Nothing>(const lambda::function<process::Future<ResourceUsage>()>&));

  MOCK_METHOD0(
      corrections, process::Future<std::list<medios::node::QoSCorrection>>());
};


// Definition of a mock Node to be used in tests with gmock, covering
// potential races between runTask and killTask.
class MockNode : public node::Node
{
public:
  MockNode(
      const std::string& id,
      const node::Flags& flags,
      medios::controller::detector::ControllerDetector* detector,
      node::Containerizer* containerizer,
      Files* files,
      node::GarbageCollector* gc,
      node::TaskStatusUpdateManager* taskStatusUpdateManager,
      medios::node::ResourceEstimator* resourceEstimator,
      medios::node::QoSController* qosController,
      SecretGenerator* secretGenerator,
      node::VolumeGidManager* volumeGidManager,
      PendingFutureTracker* futureTracker,
      const Option<Authorizer*>& authorizer);

  MOCK_METHOD6(___run, void(
      const process::Future<Nothing>& future,
      const FrameworkID& frameworkId,
      const ExecutorID& executorId,
      const ContainerID& containerId,
      const std::vector<TaskInfo>& tasks,
      const std::vector<TaskGroupInfo>& taskGroups));

  void unmocked____run(
      const process::Future<Nothing>& future,
      const FrameworkID& frameworkId,
      const ExecutorID& executorId,
      const ContainerID& containerId,
      const std::vector<TaskInfo>& tasks,
      const std::vector<TaskGroupInfo>& taskGroups);

  MOCK_METHOD7(runTask, void(
      const process::UPID& from,
      const FrameworkInfo& frameworkInfo,
      const FrameworkID& frameworkId,
      const process::UPID& pid,
      const TaskInfo& task,
      const std::vector<ResourceVersionUUID>& resourceVersionUuids,
      const Option<bool>& launchExecutor));

  void unmocked_runTask(
      const process::UPID& from,
      const FrameworkInfo& frameworkInfo,
      const FrameworkID& frameworkId,
      const process::UPID& pid,
      const TaskInfo& task,
      const std::vector<ResourceVersionUUID>& resourceVersionUuids,
      const Option<bool>& launchExecutor);

  MOCK_METHOD6(_run, process::Future<Nothing>(
      const FrameworkInfo& frameworkInfo,
      const ExecutorInfo& executorInfo,
      const Option<TaskInfo>& task,
      const Option<TaskGroupInfo>& taskGroup,
      const std::vector<ResourceVersionUUID>& resourceVersionUuids,
      const Option<bool>& launchExecutor));

  process::Future<Nothing> unmocked__run(
      const FrameworkInfo& frameworkInfo,
      const ExecutorInfo& executorInfo,
      const Option<TaskInfo>& task,
      const Option<TaskGroupInfo>& taskGroup,
      const std::vector<ResourceVersionUUID>& resourceVersionUuids,
      const Option<bool>& launchExecutor);

  MOCK_METHOD7(__run, void(
      const FrameworkInfo& frameworkInfo,
      const ExecutorInfo& executorInfo,
      const Option<TaskInfo>& task,
      const Option<TaskGroupInfo>& taskGroup,
      const std::vector<ResourceVersionUUID>& resourceVersionUuids,
      const Option<bool>& launchExecutor,
      bool executorGeneratedForCommandTask));

  void unmocked___run(
      const FrameworkInfo& frameworkInfo,
      const ExecutorInfo& executorInfo,
      const Option<TaskInfo>& task,
      const Option<TaskGroupInfo>& taskGroup,
      const std::vector<ResourceVersionUUID>& resourceVersionUuids,
      const Option<bool>& launchExecutor,
      bool executorGeneratedForCommandTask);

  MOCK_METHOD6(runTaskGroup, void(
      const process::UPID& from,
      const FrameworkInfo& frameworkInfo,
      const ExecutorInfo& executorInfo,
      const TaskGroupInfo& taskGroup,
      const std::vector<ResourceVersionUUID>& resourceVersionUuids,
      const Option<bool>& launchExecutor));

  void unmocked_runTaskGroup(
      const process::UPID& from,
      const FrameworkInfo& frameworkInfo,
      const ExecutorInfo& executorInfo,
      const TaskGroupInfo& taskGroup,
      const std::vector<ResourceVersionUUID>& resourceVersionUuids,
      const Option<bool>& launchExecutor);

  MOCK_METHOD2(killTask, void(
      const process::UPID& from,
      const KillTaskMessage& killTaskMessage));

  void unmocked_killTask(
      const process::UPID& from,
      const KillTaskMessage& killTaskMessage);

  MOCK_METHOD2(authenticate, void(
      Duration minTimeout,
      Duration maxTimeout));

  void unmocked_authenticate(
      Duration minTimeout,
      Duration maxTimeout);

  MOCK_METHOD1(removeFramework, void(
      node::Framework* framework));

  void unmocked_removeFramework(
      node::Framework* framework);

  MOCK_METHOD1(__recover, void(
      const process::Future<Nothing>& future));

  void unmocked___recover(
      const process::Future<Nothing>& future);

  MOCK_METHOD0(qosCorrections, void());

  void unmocked_qosCorrections();

  MOCK_METHOD1(_qosCorrections, void(
      const process::Future<std::list<
          medios::node::QoSCorrection>>& correction));

  MOCK_METHOD0(usage, process::Future<ResourceUsage>());

  process::Future<ResourceUsage> unmocked_usage();

  MOCK_METHOD3(executorTerminated, void(
      const FrameworkID& frameworkId,
      const ExecutorID& executorId,
      const process::Future<Option<
          medios::node::ContainerTermination>>& termination));

  void unmocked_executorTerminated(
      const FrameworkID& frameworkId,
      const ExecutorID& executorId,
      const process::Future<Option<
          medios::node::ContainerTermination>>& termination);

  MOCK_METHOD3(shutdownExecutor, void(
      const process::UPID& from,
      const FrameworkID& frameworkId,
      const ExecutorID& executorId));

  void unmocked_shutdownExecutor(
      const process::UPID& from,
      const FrameworkID& frameworkId,
      const ExecutorID& executorId);

  MOCK_METHOD2(_shutdownExecutor, void(
      node::Framework* framework,
      node::Executor* executor));

  void unmocked__shutdownExecutor(
      node::Framework* framework,
      node::Executor* executor);

  MOCK_METHOD1(applyOperation, void(
      const ApplyOperationMessage& message));

  void unmocked_applyOperation(
      const ApplyOperationMessage& message);
};

} // namespace tests {
} // namespace internal {
} // namespace medios {

#endif // __TESTS_MOCKNODE_HPP__
