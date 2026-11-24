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

#include <vector>

#include <gmock/gmock.h>

#include <medios/executor.hpp>
#include <medios/scheduler.hpp>

#include <process/future.hpp>
#include <process/owned.hpp>
#include <process/pid.hpp>

#include <stout/strings.hpp>

#include "controller/controller.hpp"

#include "controller/allocator/medios/hierarchical.hpp"

#include "node/node.hpp"

#include "tests/allocator.hpp"
#include "tests/medios.hpp"

using medios::internal::controller::Controller;

using medios::internal::node::Node;

using medios::controller::detector::ControllerDetector;

using process::Future;
using process::Owned;
using process::PID;

using std::vector;

using testing::_;

namespace medios {
namespace internal {
namespace tests {


class ResourceOffersTest : public MediosTest {};


TEST_F(
    ResourceOffersTest,
    CGROUPS_ResourceOfferWithMultipleNodes)
{
  Try<Owned<cluster::Controller>> controller = StartController();
  ASSERT_SOME(controller);

  Owned<ControllerDetector> detector = controller.get()->createDetector();
  vector<Owned<cluster::Node>> nodes;

  // Start 10 nodes.
  for (int i = 0; i < 10; i++) {
    node::Flags flags = CreateNodeFlags();
    flags.launcher = "posix";

    flags.resources = Option<std::string>("cpus:2;mem:1024");

    Try<Owned<cluster::Node>> node = StartNode(detector.get(), flags);
    ASSERT_SOME(node);
    nodes.push_back(node.get());
  }

  MockScheduler sched;
  MediosSchedulerDriver driver(
      &sched, DEFAULT_FRAMEWORK_INFO, controller.get()->pid, DEFAULT_CREDENTIAL);

  EXPECT_CALL(sched, registered(&driver, _, _));

  Future<vector<Offer>> offers;
  EXPECT_CALL(sched, resourceOffers(&driver, _))
    .WillOnce(FutureArg<1>(&offers))
    .WillRepeatedly(Return()); // All 10 nodes might not be in first offer.

  driver.start();

  AWAIT_READY(offers);
  ASSERT_FALSE(offers->empty());
  EXPECT_GE(10u, offers->size());

  Resources resources(offers.get()[0].resources());
  EXPECT_EQ(2, resources.get<Value::Scalar>("cpus")->value());
  EXPECT_EQ(1024, resources.get<Value::Scalar>("mem")->value());

  driver.stop();
  driver.join();
}


TEST_F(ResourceOffersTest, ResourcesGetReofferedAfterFrameworkStops)
{
  Try<Owned<cluster::Controller>> controller = StartController();
  ASSERT_SOME(controller);

  Owned<ControllerDetector> detector = controller.get()->createDetector();
  Try<Owned<cluster::Node>> node = StartNode(detector.get());
  ASSERT_SOME(node);

  MockScheduler sched1;
  MediosSchedulerDriver driver1(
      &sched1, DEFAULT_FRAMEWORK_INFO, controller.get()->pid, DEFAULT_CREDENTIAL);

  EXPECT_CALL(sched1, registered(&driver1, _, _));

  Future<vector<Offer>> offers;
  EXPECT_CALL(sched1, resourceOffers(&driver1, _))
    .WillOnce(FutureArg<1>(&offers));

  driver1.start();

  AWAIT_READY(offers);
  EXPECT_FALSE(offers->empty());

  driver1.stop();
  driver1.join();

  MockScheduler sched2;
  MediosSchedulerDriver driver2(
      &sched2, DEFAULT_FRAMEWORK_INFO, controller.get()->pid, DEFAULT_CREDENTIAL);

  EXPECT_CALL(sched2, registered(&driver2, _, _));

  EXPECT_CALL(sched2, resourceOffers(&driver2, _))
    .WillOnce(FutureArg<1>(&offers));

  driver2.start();

  AWAIT_READY(offers);

  driver2.stop();
  driver2.join();
}


TEST_F(ResourceOffersTest, ResourcesGetReofferedWhenUnused)
{
  Try<Owned<cluster::Controller>> controller = StartController();
  ASSERT_SOME(controller);

  Owned<ControllerDetector> detector = controller.get()->createDetector();
  Try<Owned<cluster::Node>> node = StartNode(detector.get());
  ASSERT_SOME(node);

  MockScheduler sched1;
  MediosSchedulerDriver driver1(
      &sched1, DEFAULT_FRAMEWORK_INFO, controller.get()->pid, DEFAULT_CREDENTIAL);

  EXPECT_CALL(sched1, registered(&driver1, _, _));

  Future<vector<Offer>> offers;
  EXPECT_CALL(sched1, resourceOffers(&driver1, _))
    .WillOnce(FutureArg<1>(&offers));

  driver1.start();

  AWAIT_READY(offers);
  ASSERT_FALSE(offers->empty());

  vector<TaskInfo> tasks; // Use nothing!
  driver1.launchTasks(offers.get()[0].id(), tasks);

  MockScheduler sched2;
  MediosSchedulerDriver driver2(
      &sched2, DEFAULT_FRAMEWORK_INFO, controller.get()->pid, DEFAULT_CREDENTIAL);

  EXPECT_CALL(sched2, registered(&driver2, _, _));

  EXPECT_CALL(sched2, resourceOffers(&driver2, _))
    .WillOnce(FutureArg<1>(&offers));

  driver2.start();

  AWAIT_READY(offers);

  // Stop first framework before second so no offers are sent.
  driver1.stop();
  driver1.join();

  driver2.stop();
  driver2.join();
}


TEST_F(ResourceOffersTest, ResourcesGetReofferedAfterTaskInfoError)
{
  Try<Owned<cluster::Controller>> controller = StartController();
  ASSERT_SOME(controller);

  Owned<ControllerDetector> detector = controller.get()->createDetector();
  Try<Owned<cluster::Node>> node = StartNode(detector.get());
  ASSERT_SOME(node);

  MockScheduler sched1;
  MediosSchedulerDriver driver1(
      &sched1, DEFAULT_FRAMEWORK_INFO, controller.get()->pid, DEFAULT_CREDENTIAL);

  EXPECT_CALL(sched1, registered(&driver1, _, _));

  Future<vector<Offer>> offers;
  EXPECT_CALL(sched1, resourceOffers(&driver1, _))
    .WillOnce(FutureArg<1>(&offers))
    .WillRepeatedly(Return()); // Ignore subsequent offers.

  driver1.start();

  AWAIT_READY(offers);
  ASSERT_FALSE(offers->empty());

  TaskInfo task;
  task.set_name("");
  task.set_principal(DEFAULT_TASK_PRINCIPAL);
  task.mutable_task_id()->set_value("1");
  task.mutable_node_id()->MergeFrom(offers.get()[0].node_id());
  task.mutable_executor()->MergeFrom(DEFAULT_EXECUTOR_INFO);

  Resource* cpus = task.add_resources();
  cpus->set_name("cpus");
  cpus->set_type(Value::SCALAR);
  cpus->mutable_scalar()->set_value(-1);

  Resource* mem = task.add_resources();
  mem->set_name("mem");
  mem->set_type(Value::SCALAR);
  mem->mutable_scalar()->set_value(static_cast<double>(Gigabytes(1).bytes()));

  vector<TaskInfo> tasks;
  tasks.push_back(task);

  Future<TaskStatus> status;
  EXPECT_CALL(sched1, statusUpdate(&driver1, _))
    .WillOnce(FutureArg<1>(&status));

  driver1.launchTasks(offers.get()[0].id(), tasks);

  AWAIT_READY(status);
  EXPECT_EQ(task.task_id(), status->task_id());
  EXPECT_EQ(TASK_ERROR, status->state());
  EXPECT_EQ(TaskStatus::REASON_TASK_INVALID, status->reason());
  EXPECT_TRUE(status->has_message());
  EXPECT_TRUE(strings::contains(status->message(), "Invalid scalar resource"))
    << status->message();

  MockScheduler sched2;
  MediosSchedulerDriver driver2(
      &sched2, DEFAULT_FRAMEWORK_INFO, controller.get()->pid, DEFAULT_CREDENTIAL);

  EXPECT_CALL(sched2, registered(&driver2, _, _));

  EXPECT_CALL(sched2, resourceOffers(&driver2, _))
    .WillOnce(FutureArg<1>(&offers))
    .WillRepeatedly(Return()); // Ignore subsequent offers.

  driver2.start();

  AWAIT_READY(offers);

  driver1.stop();
  driver1.join();

  driver2.stop();
  driver2.join();
}


TEST_F(ResourceOffersTest, Request)
{
  TestAllocator<controller::allocator::HierarchicalDRFAllocator> allocator;

  EXPECT_CALL(allocator, initialize(_, _, _));

  Try<Owned<cluster::Controller>> controller = StartController(&allocator);
  ASSERT_SOME(controller);

  MockScheduler sched;
  MediosSchedulerDriver driver(
      &sched, DEFAULT_FRAMEWORK_INFO, controller.get()->pid, DEFAULT_CREDENTIAL);

  EXPECT_CALL(allocator, addFramework_(_, _, _, _, _));

  Future<Nothing> registered;
  EXPECT_CALL(sched, registered(&driver, _, _))
    .WillOnce(FutureSatisfy(&registered));

  driver.start();

  AWAIT_READY(registered);

  vector<Request> sent;
  Request request;
  request.mutable_node_id()->set_value("test");
  sent.push_back(request);

  Future<vector<Request>> received;
  EXPECT_CALL(allocator, requestResources(_, _))
    .WillOnce(FutureArg<1>(&received));

  driver.requestResources(sent);

  AWAIT_READY(received);
  EXPECT_EQ(sent.size(), received->size());
  EXPECT_FALSE(received->empty());
  EXPECT_EQ(request.node_id(), received.get()[0].node_id());

  driver.stop();
  driver.join();
}

} // namespace tests {
} // namespace internal {
} // namespace medios {
