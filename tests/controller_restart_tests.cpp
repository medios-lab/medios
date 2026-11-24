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

// The controller restart contract (DR-010): a controller restart is a
// reconnection flow, not a recovery-of-state flow. These tests restart
// the controller under live cluster state and assert what survives:
// the node reregisters, the framework resubscribes, a running task is
// still running, and node-checkpointed dynamic reservations reappear
// in offers. Outstanding offers — including gang grants (#323) — die
// with the controller by design; frameworks re-request.

#include <string>
#include <vector>

#include <gmock/gmock.h>

#include <medios/resources.hpp>
#include <medios/scheduler.hpp>

#include <process/clock.hpp>
#include <process/future.hpp>
#include <process/gmock.hpp>
#include <process/http.hpp>
#include <process/owned.hpp>
#include <process/pid.hpp>

#include <stout/json.hpp>
#include <stout/option.hpp>
#include <stout/try.hpp>

#include "controller/flags.hpp"
#include "controller/controller.hpp"

#include "controller/detector/standalone.hpp"

#include "tests/containerizer.hpp"
#include "tests/medios.hpp"
#include "tests/resources_utils.hpp"

using medios::internal::controller::Controller;

using medios::internal::node::Node;

using medios::controller::detector::StandaloneControllerDetector;

using process::Clock;
using process::Future;
using process::Owned;

using process::http::OK;
using process::http::Response;

using std::vector;

using testing::_;
using testing::AtMost;
using testing::Return;
using testing::SaveArg;

namespace medios {
namespace internal {
namespace tests {

class ControllerRestartTest : public MediosTest {};


// A running task survives a controller restart: the node reregisters
// with the new controller instance, the framework resubscribes, and
// /state reports the task still running.
TEST_F(ControllerRestartTest, TaskSurvivesControllerRestart)
{
  Clock::pause();

  controller::Flags controllerFlags = CreateControllerFlags();
  Try<Owned<cluster::Controller>> controller =
    StartController(controllerFlags);
  ASSERT_SOME(controller);

  MockExecutor exec(DEFAULT_EXECUTOR_ID);
  TestContainerizer containerizer(&exec);

  // Shared detector so both the node and the scheduler can be pointed
  // at the restarted controller.
  StandaloneControllerDetector detector(controller.get()->pid);

  node::Flags nodeFlags = CreateNodeFlags();
  Try<Owned<cluster::Node>> node =
    StartNode(&detector, &containerizer, nodeFlags);
  ASSERT_SOME(node);

  FrameworkInfo frameworkInfo = DEFAULT_FRAMEWORK_INFO;

  MockScheduler sched;
  TestingMediosSchedulerDriver driver(&sched, &detector, frameworkInfo);

  // Initial registration plus the post-restart re-registration.
  EXPECT_CALL(sched, registered(_, _, _))
    .Times(2);

  EXPECT_CALL(exec, registered(_, _, _, _));

  ExecutorInfo executorInfo =
    createExecutorInfo("default", "sleep 1000", "cpus:0.1");

  EXPECT_CALL(sched, resourceOffers(&driver, _))
    .WillOnce(LaunchTasks(executorInfo, 1, 1, 64, frameworkInfo.roles(0)))
    .WillRepeatedly(Return()); // Ignore subsequent offers.

  EXPECT_CALL(exec, launchTask(_, _))
    .WillOnce(SendStatusUpdateFromTask(TASK_RUNNING));

  EXPECT_CALL(exec, shutdown(_))
    .Times(AtMost(1));

  // Node reregistration may re-affirm the task with a second
  // TASK_RUNNING (REASON_NODE_REREGISTERED); its delivery depends on
  // framework connectivity timing, so it is allowed but not required.
  Future<TaskStatus> status;
  EXPECT_CALL(sched, statusUpdate(&driver, _))
    .WillOnce(FutureArg<1>(&status))
    .WillRepeatedly(Return());

  driver.start();

  Clock::settle();
  Clock::advance(controllerFlags.allocation_interval);

  AWAIT_READY(status);
  EXPECT_EQ(TASK_RUNNING, status->state());

  // Restart the controller.
  Future<Nothing> disconnected;
  EXPECT_CALL(sched, disconnected(&driver))
    .WillOnce(FutureSatisfy(&disconnected));

  controller->reset();
  controller = StartController(controllerFlags);
  ASSERT_SOME(controller);

  Future<NodeReregisteredMessage> nodeReregistered =
    FUTURE_PROTOBUF(NodeReregisteredMessage(), _, node.get()->pid);

  // Point the node and the scheduler at the new controller instance.
  detector.appoint(controller.get()->pid);

  Clock::settle();
  AWAIT_READY(disconnected);

  Clock::advance(nodeFlags.authentication_backoff_factor);
  Clock::advance(nodeFlags.registration_backoff_factor);

  AWAIT_READY(nodeReregistered);

  Clock::settle();

  // The restarted controller reports the task still running.
  Future<Response> response = process::http::get(
      controller.get()->pid,
      "state",
      None(),
      createBasicAuthHeaders(DEFAULT_CREDENTIAL));

  AWAIT_EXPECT_RESPONSE_STATUS_EQ(OK().status, response);

  Try<JSON::Object> parse = JSON::parse<JSON::Object>(response->body);
  ASSERT_SOME(parse);

  JSON::Array frameworks = parse->at<JSON::Array>("frameworks").get();
  ASSERT_EQ(1u, frameworks.values.size());

  JSON::Object framework = frameworks.values.front().as<JSON::Object>();
  JSON::Array tasks = framework.at<JSON::Array>("tasks").get();
  ASSERT_EQ(1u, tasks.values.size());

  JSON::Object task = tasks.values.front().as<JSON::Object>();
  EXPECT_EQ(JSON::String("TASK_RUNNING"), task.values["state"]);

  driver.stop();
  driver.join();
}


// A dynamic reservation survives a controller restart via the node's
// checkpointed resources: after the node reregisters with the new
// controller instance, the role's reservation is offered again.
TEST_F(ControllerRestartTest, ReservationSurvivesControllerRestart)
{
  Clock::pause();

  controller::Flags controllerFlags = CreateControllerFlags();
  controllerFlags.allocation_interval = Milliseconds(50);

  Try<Owned<cluster::Controller>> controller =
    StartController(controllerFlags);
  ASSERT_SOME(controller);

  StandaloneControllerDetector detector(controller.get()->pid);

  node::Flags nodeFlags = CreateNodeFlags();
  nodeFlags.resources = "cpus:4;mem:1024;disk:512";

  Try<Owned<cluster::Node>> node = StartNode(&detector, nodeFlags);
  ASSERT_SOME(node);

  FrameworkInfo frameworkInfo = DEFAULT_FRAMEWORK_INFO;
  frameworkInfo.set_roles(0, DEFAULT_TEST_ROLE);

  MockScheduler sched;
  TestingMediosSchedulerDriver driver(&sched, &detector, frameworkInfo);

  EXPECT_CALL(sched, registered(_, _, _))
    .Times(2);

  Filters filters;
  filters.set_refuse_seconds(0);

  Resources unreserved = Resources::parse("cpus:2;mem:512").get();
  Resources reserved = unreserved.pushReservation(createDynamicReservationInfo(
      frameworkInfo.roles(0), frameworkInfo.principal()));

  // First offer: unreserved resources.
  Future<vector<Offer>> offers;
  EXPECT_CALL(sched, resourceOffers(&driver, _))
    .WillOnce(FutureArg<1>(&offers));

  driver.start();

  Clock::settle();
  Clock::advance(controllerFlags.allocation_interval);

  AWAIT_READY(offers);
  ASSERT_FALSE(offers->empty());
  Offer offer = offers->front();

  // Reserve, and wait for the operation to reach the node (which
  // checkpoints the reservation on receipt).
  Future<ApplyOperationMessage> applyOperation =
    FUTURE_PROTOBUF(ApplyOperationMessage(), _, node.get()->pid);

  // Second offer: the reservation.
  EXPECT_CALL(sched, resourceOffers(&driver, _))
    .WillOnce(FutureArg<1>(&offers));

  Offer::Operation reserve;
  reserve.set_type(Offer::Operation::RESERVE);
  reserve.mutable_reserve()->mutable_resources()->CopyFrom(
      static_cast<const google::protobuf::RepeatedPtrField<Resource>&>(
          reserved));

  driver.acceptOffers({offer.id()}, {reserve}, filters);

  AWAIT_READY(applyOperation);

  Clock::settle();
  Clock::advance(controllerFlags.allocation_interval);

  AWAIT_READY(offers);
  ASSERT_FALSE(offers->empty());
  EXPECT_TRUE(Resources(offers->front().resources())
                .contains(allocatedResources(
                    reserved, frameworkInfo.roles(0))));

  // Restart the controller.
  Future<Nothing> disconnected;
  EXPECT_CALL(sched, disconnected(&driver))
    .WillOnce(FutureSatisfy(&disconnected));

  controller->reset();
  controller = StartController(controllerFlags);
  ASSERT_SOME(controller);

  Future<NodeReregisteredMessage> nodeReregistered =
    FUTURE_PROTOBUF(NodeReregisteredMessage(), _, node.get()->pid);

  // Post-restart offer: the checkpointed reservation comes back.
  Future<vector<Offer>> postRestartOffers;
  EXPECT_CALL(sched, resourceOffers(&driver, _))
    .WillOnce(FutureArg<1>(&postRestartOffers))
    .WillRepeatedly(Return());

  detector.appoint(controller.get()->pid);

  Clock::settle();
  AWAIT_READY(disconnected);

  Clock::advance(nodeFlags.authentication_backoff_factor);
  Clock::advance(nodeFlags.registration_backoff_factor);

  AWAIT_READY(nodeReregistered);

  Clock::settle();
  Clock::advance(controllerFlags.allocation_interval);

  AWAIT_READY(postRestartOffers);
  ASSERT_FALSE(postRestartOffers->empty());

  EXPECT_TRUE(Resources(postRestartOffers->front().resources())
                .contains(allocatedResources(
                    reserved, frameworkInfo.roles(0))))
    << "reservation missing from post-restart offer: "
    << Resources(postRestartOffers->front().resources());

  driver.stop();
  driver.join();
}

} // namespace tests {
} // namespace internal {
} // namespace medios {
