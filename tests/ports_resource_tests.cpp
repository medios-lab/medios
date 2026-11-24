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

// Regression guard for the `ports` RANGES resource path: a node advertises a
// declared ports range, and a task that reserves a sub-range removes exactly
// those ports from what the node re-offers. Exercises the generic RANGES
// arithmetic through offer -> accept -> re-offer; no sockets, no real binding.
// Underpins kernel-per-task frameworks that reserve ports (#335).

#include <vector>

#include <gmock/gmock.h>

#include <medios/executor.hpp>
#include <medios/resources.hpp>
#include <medios/scheduler.hpp>

#include <process/future.hpp>
#include <process/gmock.hpp>
#include <process/owned.hpp>

#include <stout/try.hpp>

#include "controller/controller.hpp"

#include "node/flags.hpp"
#include "node/node.hpp"

#include "tests/containerizer.hpp"
#include "tests/medios.hpp"
#include "tests/resources_utils.hpp"

using medios::internal::controller::Controller;
using medios::internal::node::Node;
using medios::controller::detector::ControllerDetector;

using process::Future;
using process::Owned;

using std::vector;

using testing::_;
using testing::DoAll;
using testing::Return;

namespace medios {
namespace internal {
namespace tests {

class PortsResourceTest : public MediosTest {};


// Whether the offered ports cover every port in [begin, end].
static bool offerCoversPorts(const Offer& offer, uint64_t begin, uint64_t end)
{
  Option<Value::Ranges> ports = Resources(offer.resources()).ports();
  if (ports.isNone()) {
    return false;
  }
  for (uint64_t p = begin; p <= end; p++) {
    bool found = false;
    foreach (const Value::Range& range, ports->range()) {
      if (range.begin() <= p && p <= range.end()) {
        found = true;
        break;
      }
    }
    if (!found) {
      return false;
    }
  }
  return true;
}


// Whether the offered ports include port p.
static bool offerHasPort(const Offer& offer, uint64_t p)
{
  return offerCoversPorts(offer, p, p);
}


// A node that declares a ports range advertises it in the offer.
TEST_F(PortsResourceTest, OfferAdvertisesDeclaredPorts)
{
  Try<Owned<cluster::Controller>> controller = StartController();
  ASSERT_SOME(controller);

  node::Flags nodeFlags = CreateNodeFlags();
  nodeFlags.resources = "cpus:2;mem:512;ports:[20000-20005]";

  Owned<ControllerDetector> detector = controller.get()->createDetector();
  Try<Owned<cluster::Node>> node = StartNode(detector.get(), nodeFlags);
  ASSERT_SOME(node);

  MockScheduler sched;
  MediosSchedulerDriver driver(
      &sched, DEFAULT_FRAMEWORK_INFO, controller.get()->pid, DEFAULT_CREDENTIAL);

  EXPECT_CALL(sched, registered(&driver, _, _));

  Future<vector<Offer>> offers;
  EXPECT_CALL(sched, resourceOffers(&driver, _))
    .WillOnce(FutureArg<1>(&offers))
    .WillRepeatedly(Return());

  driver.start();

  AWAIT_READY(offers);
  ASSERT_FALSE(offers->empty());

  EXPECT_TRUE(offerCoversPorts(offers->front(), 20000, 20005))
    << "offer did not advertise the declared ports: "
    << Resources(offers->front().resources());

  driver.stop();
  driver.join();
}


// A task that reserves a ports sub-range removes exactly those ports from the
// node's next offer; the rest stay available.
TEST_F(PortsResourceTest, TaskLaunchAccountsPortsSubRange)
{
  controller::Flags controllerFlags = CreateControllerFlags();
  controllerFlags.allocation_interval = Milliseconds(50);

  Try<Owned<cluster::Controller>> controller = StartController(controllerFlags);
  ASSERT_SOME(controller);

  MockExecutor exec(DEFAULT_EXECUTOR_ID);
  TestContainerizer containerizer(&exec);

  node::Flags nodeFlags = CreateNodeFlags();
  nodeFlags.resources = "cpus:2;mem:512;ports:[20000-20005]";

  Owned<ControllerDetector> detector = controller.get()->createDetector();
  Try<Owned<cluster::Node>> node =
    StartNode(detector.get(), &containerizer, nodeFlags);
  ASSERT_SOME(node);

  MockScheduler sched;
  MediosSchedulerDriver driver(
      &sched, DEFAULT_FRAMEWORK_INFO, controller.get()->pid, DEFAULT_CREDENTIAL);

  EXPECT_CALL(sched, registered(&driver, _, _));

  Future<vector<Offer>> offers;
  EXPECT_CALL(sched, resourceOffers(&driver, _))
    .WillOnce(FutureArg<1>(&offers))
    .WillRepeatedly(Return());

  driver.start();

  AWAIT_READY(offers);
  ASSERT_FALSE(offers->empty());
  Offer offer = offers->front();
  ASSERT_TRUE(offerCoversPorts(offer, 20000, 20005));

  // Reserve ports [20000-20002] (plus a little cpus/mem) for the task.
  Resources taskResources = allocatedResources(
      Resources::parse("cpus:1;mem:128;ports:[20000-20002]").get(),
      DEFAULT_FRAMEWORK_INFO.roles(0));

  TaskInfo task;
  task.set_name("ports-task");
  task.set_principal(DEFAULT_TASK_PRINCIPAL);
  task.mutable_task_id()->set_value("1");
  task.mutable_node_id()->MergeFrom(offer.node_id());
  task.mutable_resources()->MergeFrom(taskResources);
  task.mutable_executor()->MergeFrom(DEFAULT_EXECUTOR_INFO);

  EXPECT_CALL(exec, registered(_, _, _, _));
  EXPECT_CALL(exec, launchTask(_, _))
    .WillOnce(SendStatusUpdateFromTask(TASK_RUNNING));

  Future<Nothing> update;
  EXPECT_CALL(containerizer, update(_, _, _))
    .WillRepeatedly(DoAll(FutureSatisfy(&update), Return(Nothing())));

  Future<TaskStatus> status;
  EXPECT_CALL(sched, statusUpdate(&driver, _))
    .WillOnce(FutureArg<1>(&status));

  // Capture the re-offer of the node's remaining resources.
  Future<vector<Offer>> reoffer;
  EXPECT_CALL(sched, resourceOffers(&driver, _))
    .WillOnce(FutureArg<1>(&reoffer))
    .WillRepeatedly(Return());

  driver.launchTasks(offer.id(), {task});

  AWAIT_READY(status);
  ASSERT_EQ(TASK_RUNNING, status->state());

  AWAIT_READY(reoffer);
  ASSERT_FALSE(reoffer->empty());
  Offer remaining = reoffer->front();

  // The reserved ports are gone; the rest remain.
  EXPECT_FALSE(offerHasPort(remaining, 20000))
    << "reserved port 20000 was re-offered: "
    << Resources(remaining.resources());
  EXPECT_FALSE(offerHasPort(remaining, 20002));
  EXPECT_TRUE(offerCoversPorts(remaining, 20003, 20005))
    << "unreserved ports 20003-20005 missing from re-offer: "
    << Resources(remaining.resources());

  EXPECT_CALL(exec, shutdown(_))
    .Times(testing::AtMost(1));

  driver.stop();
  driver.join();
}

} // namespace tests {
} // namespace internal {
} // namespace medios {
