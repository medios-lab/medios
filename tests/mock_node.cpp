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

#include <utility>

#include <gmock/gmock.h>

#include <medios/authentication/secret_generator.hpp>

#include <medios/node/qos_controller.hpp>
#include <medios/node/resource_estimator.hpp>

#include <process/future.hpp>
#include <process/owned.hpp>
#include <process/pid.hpp>

#include <stout/option.hpp>

#include "node/node.hpp"
#include "node/task_status_update_manager.hpp"

#include "tests/mock_node.hpp"

using medios::controller::detector::ControllerDetector;

using medios::internal::node::Containerizer;
using medios::internal::node::GarbageCollector;
using medios::internal::node::TaskStatusUpdateManager;
using medios::internal::node::VolumeGidManager;

using medios::node::ContainerTermination;
using medios::node::ResourceEstimator;
using medios::node::QoSController;

using std::list;
using std::string;
using std::vector;

using process::Future;
using process::Owned;
using process::UPID;

using testing::_;
using testing::Invoke;

namespace medios {
namespace internal {
namespace tests {

MockResourceEstimator::MockResourceEstimator()
{
  ON_CALL(*this, initialize(_))
    .WillByDefault(Return(Nothing()));
  EXPECT_CALL(*this, initialize(_))
    .WillRepeatedly(DoDefault());

  ON_CALL(*this, oversubscribable())
    .WillByDefault(Return(Future<Resources>()));
  EXPECT_CALL(*this, oversubscribable())
    .WillRepeatedly(DoDefault());
}


MockResourceEstimator::~MockResourceEstimator() {}


MockQoSController::MockQoSController()
{
  ON_CALL(*this, initialize(_))
    .WillByDefault(Return(Nothing()));
  EXPECT_CALL(*this, initialize(_))
    .WillRepeatedly(DoDefault());

  ON_CALL(*this, corrections())
    .WillByDefault(
        Return(Future<list<medios::node::QoSCorrection>>()));
  EXPECT_CALL(*this, corrections())
    .WillRepeatedly(DoDefault());
}


MockQoSController::~MockQoSController() {}


MockNode::MockNode(
    const string& id,
    const node::Flags& flags,
    ControllerDetector* detector,
    Containerizer* containerizer,
    Files* files,
    GarbageCollector* gc,
    TaskStatusUpdateManager* taskStatusUpdateManager,
    ResourceEstimator* resourceEstimator,
    QoSController* qosController,
    SecretGenerator* secretGenerator,
    VolumeGidManager* volumeGidManager,
    PendingFutureTracker* futureTracker,
    const Option<Authorizer*>& authorizer)
  // It is necessary to explicitly call `ProcessBase` constructor here even
  // though the direct parent `Node` already does this. This is because
  // `ProcessBase` being a virtual base class, if not being explicitly
  // constructed here by passing `id`, will be constructed implicitly first with
  // a default constructor, resulting in lost argument `id`.
  : ProcessBase(id),
    node::Node(
        id,
        flags,
        detector,
        containerizer,
        files,
        gc,
        taskStatusUpdateManager,
        resourceEstimator,
        qosController,
        secretGenerator,
        volumeGidManager,
        futureTracker,
        None(),
        authorizer)
{
  // Set up default behaviors, calling the original methods.
  EXPECT_CALL(*this, ___run(_, _, _, _, _, _))
    .WillRepeatedly(Invoke(this, &MockNode::unmocked____run));
  EXPECT_CALL(*this, runTask(_, _, _, _, _, _, _))
    .WillRepeatedly(Invoke(this, &MockNode::unmocked_runTask));
  EXPECT_CALL(*this, _run(_, _, _, _, _, _))
    .WillRepeatedly(Invoke(this, &MockNode::unmocked__run));
  EXPECT_CALL(*this, __run(_, _, _, _, _, _, _))
    .WillRepeatedly(Invoke(this, &MockNode::unmocked___run));
  EXPECT_CALL(*this, runTaskGroup(_, _, _, _, _, _))
    .WillRepeatedly(Invoke(this, &MockNode::unmocked_runTaskGroup));
  EXPECT_CALL(*this, killTask(_, _))
    .WillRepeatedly(Invoke(this, &MockNode::unmocked_killTask));
  EXPECT_CALL(*this, authenticate(_, _))
    .WillRepeatedly(Invoke(this, &MockNode::unmocked_authenticate));
  EXPECT_CALL(*this, removeFramework(_))
    .WillRepeatedly(Invoke(this, &MockNode::unmocked_removeFramework));
  EXPECT_CALL(*this, __recover(_))
    .WillRepeatedly(Invoke(this, &MockNode::unmocked___recover));
  EXPECT_CALL(*this, qosCorrections())
    .WillRepeatedly(Invoke(this, &MockNode::unmocked_qosCorrections));
  EXPECT_CALL(*this, usage())
    .WillRepeatedly(Invoke(this, &MockNode::unmocked_usage));
  EXPECT_CALL(*this, executorTerminated(_, _, _))
    .WillRepeatedly(Invoke(this, &MockNode::unmocked_executorTerminated));
  EXPECT_CALL(*this, shutdownExecutor(_, _, _))
    .WillRepeatedly(Invoke(this, &MockNode::unmocked_shutdownExecutor));
  EXPECT_CALL(*this, _shutdownExecutor(_, _))
    .WillRepeatedly(Invoke(this, &MockNode::unmocked__shutdownExecutor));
  EXPECT_CALL(*this, applyOperation(_))
    .WillRepeatedly(Invoke(this, &MockNode::unmocked_applyOperation));
}


void MockNode::unmocked____run(
    const Future<Nothing>& future,
    const FrameworkID& frameworkId,
    const ExecutorID& executorId,
    const ContainerID& containerId,
    const vector<TaskInfo>& tasks,
    const vector<TaskGroupInfo>& taskGroups)
{
  node::Node::___run(
      future,
      frameworkId,
      executorId,
      containerId,
      tasks,
      taskGroups);
}


void MockNode::unmocked_runTask(
    const UPID& from,
    const FrameworkInfo& frameworkInfo,
    const FrameworkID& frameworkId,
    const UPID& pid,
    const TaskInfo& task,
    const vector<ResourceVersionUUID>& resourceVersionUuids,
    const Option<bool>& launchExecutor)
{
  node::Node::runTask(
      from,
      frameworkInfo,
      frameworkInfo.id(),
      pid,
      task,
      resourceVersionUuids,
      launchExecutor);
}


Future<Nothing> MockNode::unmocked__run(
    const FrameworkInfo& frameworkInfo,
    const ExecutorInfo& executorInfo,
    const Option<TaskInfo>& taskInfo,
    const Option<TaskGroupInfo>& taskGroup,
    const std::vector<ResourceVersionUUID>& resourceVersionUuids,
    const Option<bool>& launchExecutor)
{
  return node::Node::_run(
      frameworkInfo,
      executorInfo,
      taskInfo,
      taskGroup,
      resourceVersionUuids,
      launchExecutor);
}


void MockNode::unmocked___run(
    const FrameworkInfo& frameworkInfo,
    const ExecutorInfo& executorInfo,
    const Option<TaskInfo>& task,
    const Option<TaskGroupInfo>& taskGroup,
    const std::vector<ResourceVersionUUID>& resourceVersionUuids,
    const Option<bool>& launchExecutor,
    bool executorGeneratedForCommandTask)
{
  node::Node::__run(
      frameworkInfo,
      executorInfo,
      task,
      taskGroup,
      resourceVersionUuids,
      launchExecutor,
      executorGeneratedForCommandTask);
}


void MockNode::unmocked_runTaskGroup(
    const UPID& from,
    const FrameworkInfo& frameworkInfo,
    const ExecutorInfo& executorInfo,
    const TaskGroupInfo& taskGroup,
    const vector<ResourceVersionUUID>& resourceVersionUuids,
    const Option<bool>& launchExecutor)
{
  node::Node::runTaskGroup(
      from,
      frameworkInfo,
      executorInfo,
      taskGroup,
      resourceVersionUuids,
      launchExecutor);
}


void MockNode::unmocked_killTask(
    const UPID& from,
    const KillTaskMessage& killTaskMessage)
{
  node::Node::killTask(from, killTaskMessage);
}


void MockNode::unmocked_authenticate(
    Duration minTimeout,
    Duration maxTimeout)
{
  node::Node::authenticate(minTimeout, maxTimeout);
}


void MockNode::unmocked_removeFramework(node::Framework* framework)
{
  node::Node::removeFramework(framework);
}


void MockNode::unmocked___recover(const Future<Nothing>& future)
{
  node::Node::__recover(future);
}


void MockNode::unmocked_qosCorrections()
{
  node::Node::qosCorrections();
}


Future<ResourceUsage> MockNode::unmocked_usage()
{
  return node::Node::usage();
}


void MockNode::unmocked_executorTerminated(
    const FrameworkID& frameworkId,
    const ExecutorID& executorId,
    const Future<Option<ContainerTermination>>& termination)
{
  node::Node::executorTerminated(frameworkId, executorId, termination);
}


void MockNode::unmocked_shutdownExecutor(
    const UPID& from,
    const FrameworkID& frameworkId,
    const ExecutorID& executorId)
{
  node::Node::shutdownExecutor(from, frameworkId, executorId);
}


void MockNode::unmocked__shutdownExecutor(
    node::Framework* framework,
    node::Executor* executor)
{
  node::Node::_shutdownExecutor(framework, executor);
}


void MockNode::unmocked_applyOperation(const ApplyOperationMessage& message)
{
  node::Node::applyOperation(message);
}

} // namespace tests {
} // namespace internal {
} // namespace medios {
