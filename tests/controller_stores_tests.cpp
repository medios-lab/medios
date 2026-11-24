// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

// The controller's side of a store (DR-036, medios#529): a task's volumes
// are STORE volumes on a store the offering node declares, a mode takes a
// unit the node has free, the unit comes back at the task's end, and the
// record of what was given rides on /state and every status update.

#include <string>
#include <vector>

#include <gmock/gmock.h>

#include <medios/executor.hpp>
#include <medios/scheduler.hpp>

#include <process/future.hpp>
#include <process/gmock.hpp>
#include <process/http.hpp>
#include <process/owned.hpp>

#include <stout/foreach.hpp>
#include <stout/gtest.hpp>
#include <stout/json.hpp>
#include <stout/os.hpp>
#include <stout/path.hpp>
#include <stout/strings.hpp>
#include <stout/try.hpp>

#include "controller/validation.hpp"

#include "node/flags.hpp"

#include "tests/containerizer.hpp"
#include "tests/medios.hpp"
#include "tests/mock_node.hpp"

using medios::controller::detector::ControllerDetector;

using medios::internal::node::Flags;

using medios::internal::tests::TestContainerizer;

using process::Future;
using process::Owned;

using process::http::OK;
using process::http::Response;

using std::string;
using std::vector;

using testing::_;
using testing::AtMost;
using testing::Return;

namespace validation = medios::internal::controller::validation;

namespace medios {
namespace internal {
namespace tests {

class ControllerStoresTest : public MediosTest
{
protected:
  // The flags of a node that delivers stores: the linux launcher and,
  // beside whatever isolation the test flags chose, the filesystem
  // isolator that binds a store. The containerizer in these tests is a
  // mock, so the strings are what the node reads to know it can deliver,
  // and nothing is loaded from them.
  static Flags deliversStores(Flags flags)
  {
    flags.launcher = "linux";
    flags.isolation = flags.isolation.empty()
      ? "filesystem/linux"
      : flags.isolation + ",filesystem/linux";
    return flags;
  }

  // A stores file declaring one store with the given counts, at a
  // directory under the test sandbox that exists.
  string storesFile(const string& name, size_t readers, size_t writers)
  {
    const string directory = path::join(sandbox.get(), name);
    CHECK_SOME(os::mkdir(directory));

    const string file = path::join(sandbox.get(), name + "-stores.json");
    CHECK_SOME(os::write(file,
        "{ \"stores\": [ { \"name\": \"" + name + "\", \"path\": \"" +
        directory + "\", \"class\": \"local\", \"locality\": \"near\", "
        "\"readers\": " + stringify(readers) + ", \"writers\": " +
        stringify(writers) + " } ] }"));

    return file;
  }

  // A task on the offer that asks for one store, with a small claim so
  // several fit on the node, listing the store's shared resource from the
  // offer as the controller requires.
  TaskInfo storeTask(
      const Offer& offer,
      const string& id,
      const string& store,
      const Volume::Mode& mode,
      bool exclusive = false,
      bool listResource = true)
  {
    // The claim is allocated as the offer was; the store's copy comes from
    // the offer itself, already allocated.
    Resources claim = Resources::parse("cpus:0.1;mem:32").get();
    claim.allocate(offer.resources(0).allocation_info().role());

    if (listResource) {
      foreach (const Resource& resource, offer.resources()) {
        if (resource.name() == "store/" + store) {
          claim += resource;
        }
      }
    }

    TaskInfo task = createTask(
        offer.node_id(), claim, "sleep 1000", DEFAULT_EXECUTOR_ID);
    task.mutable_task_id()->set_value(id);

    ContainerInfo* container = task.mutable_container();
    container->set_type(ContainerInfo::MEDIOS);

    Volume* volume = container->add_volumes();
    volume->set_container_path("/data");
    volume->set_mode(mode);
    volume->mutable_source()->set_type(Volume::Source::STORE);
    volume->mutable_source()->mutable_store()->set_name(store);
    if (exclusive) {
      volume->mutable_source()->mutable_store()->set_exclusive(true);
    }

    return task;
  }
};


// A task with STORE volumes at the given container paths; the rest of the
// task is unset, since `validateStorePaths` reads nothing else.
static TaskInfo taskWithPaths(const vector<string>& paths)
{
  TaskInfo task;
  task.mutable_container()->set_type(ContainerInfo::MEDIOS);

  foreach (const string& path, paths) {
    Volume* volume = task.mutable_container()->add_volumes();
    volume->set_container_path(path);
    volume->set_mode(Volume::RW);
    volume->mutable_source()->set_type(Volume::Source::STORE);
    volume->mutable_source()->mutable_store()->set_name(path + "-store");
  }

  return task;
}


// An absolute path and one name inside the sandbox are both accepted, on
// one task (DR-036 revision, medios#602).
TEST(ControllerStorePathsTest, AbsoluteAndOneRelativeNameAreAccepted)
{
  EXPECT_NONE(validation::task::internal::validateStorePaths(
      taskWithPaths({"/data", "tables", ".cache"})));

  EXPECT_NONE(validation::task::internal::validateStorePaths(TaskInfo()));
}


TEST(ControllerStorePathsTest, AnEmptyPathIsRefused)
{
  Option<Error> error =
    validation::task::internal::validateStorePaths(taskWithPaths({""}));
  ASSERT_SOME(error);
  EXPECT_TRUE(strings::contains(error->message, "empty container path"))
    << error->message;
}


TEST(ControllerStorePathsTest, ANestedRelativePathIsRefused)
{
  Option<Error> error = validation::task::internal::validateStorePaths(
      taskWithPaths({"data/tables"}));
  ASSERT_SOME(error);
  EXPECT_TRUE(strings::contains(error->message, "'data/tables'"))
    << error->message;
  EXPECT_TRUE(strings::contains(error->message, "more than one component"))
    << error->message;

  // A trailing slash is a `/` too.
  error = validation::task::internal::validateStorePaths(
      taskWithPaths({"data/"}));
  ASSERT_SOME(error);
  EXPECT_TRUE(strings::contains(error->message, "more than one component"))
    << error->message;
}


TEST(ControllerStorePathsTest, TheSandboxAndItsParentAreRefused)
{
  foreach (const string& path, vector<string>({".", ".."})) {
    Option<Error> error =
      validation::task::internal::validateStorePaths(taskWithPaths({path}));
    ASSERT_SOME(error) << path;
    EXPECT_TRUE(strings::contains(error->message, "the sandbox or its parent"))
      << error->message;
  }
}


TEST(ControllerStorePathsTest, ThePathTwiceIsRefused)
{
  Option<Error> error = validation::task::internal::validateStorePaths(
      taskWithPaths({"tables", "/data", "tables"}));
  ASSERT_SOME(error);
  EXPECT_TRUE(strings::contains(error->message, "Two volumes"))
    << error->message;
  EXPECT_TRUE(strings::contains(error->message, "'tables'"))
    << error->message;

  error = validation::task::internal::validateStorePaths(
      taskWithPaths({"/data", "/data"}));
  ASSERT_SOME(error);
  EXPECT_TRUE(strings::contains(error->message, "'/data'"))
    << error->message;
}


// A volume that is not a STORE volume is refused at the controller with
// its source type named, before anything reaches a node.
TEST_F(ControllerStoresTest, HostPathVolumeIsRefused)
{
  Try<Owned<cluster::Controller>> controller = StartController();
  ASSERT_SOME(controller);

  Owned<ControllerDetector> detector = controller.get()->createDetector();
  Try<Owned<cluster::Node>> node = StartNode(detector.get());
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

  TaskInfo task = createTask(offers->at(0), "sleep 1000");
  task.mutable_container()->set_type(ContainerInfo::MEDIOS);
  task.mutable_container()->add_volumes()->CopyFrom(
      createVolumeHostPath("/data", "/var/lib/data", Volume::RW));

  Future<TaskStatus> status;
  EXPECT_CALL(sched, statusUpdate(&driver, _))
    .WillOnce(FutureArg<1>(&status));

  // The refused launch returns the offer's resources; the next offer is
  // the one the second launch uses.
  Future<vector<Offer>> offers2;
  EXPECT_CALL(sched, resourceOffers(&driver, _))
    .WillOnce(FutureArg<1>(&offers2))
    .WillRepeatedly(Return());

  driver.launchTasks(offers->at(0).id(), {task});

  AWAIT_READY(status);
  EXPECT_EQ(TASK_ERROR, status->state());
  EXPECT_EQ(TaskStatus::SOURCE_CONTROLLER, status->source());
  EXPECT_EQ(TaskStatus::REASON_TASK_INVALID, status->reason());
  EXPECT_TRUE(strings::contains(status->message(), "HOST_PATH"))
    << status->message();
  EXPECT_TRUE(strings::contains(status->message(), "only STORE volumes"))
    << status->message();
  EXPECT_EQ(0, status->stores_size());

  AWAIT_READY(offers2);
  ASSERT_FALSE(offers2->empty());

  // The bare field, with no source, is a host path too.
  TaskInfo bare = createTask(offers2->at(0), "sleep 1000");
  bare.mutable_task_id()->set_value("bare");
  bare.mutable_container()->set_type(ContainerInfo::MEDIOS);
  Volume* volume = bare.mutable_container()->add_volumes();
  volume->set_container_path("/data");
  volume->set_host_path("/var/lib/data");
  volume->set_mode(Volume::RO);

  EXPECT_CALL(sched, statusUpdate(&driver, _))
    .WillOnce(FutureArg<1>(&status));

  driver.launchTasks(offers2->at(0).id(), {bare});

  AWAIT_READY(status);
  EXPECT_EQ(TASK_ERROR, status->state());
  EXPECT_TRUE(strings::contains(status->message(), "HOST_PATH"))
    << status->message();

  driver.stop();
  driver.join();
}


// A store the offering node does not declare is refused by name, and so
// is a request that mounts a declared store without listing its resource.
TEST_F(ControllerStoresTest, UndeclaredStoreAndMissingResourceAreRefused)
{
  Try<Owned<cluster::Controller>> controller = StartController();
  ASSERT_SOME(controller);

  Flags flags = CreateNodeFlags();
  flags.stores = storesFile("tables", 2, 0);
  flags = deliversStores(flags);

  Owned<ControllerDetector> detector = controller.get()->createDetector();
  Try<Owned<cluster::Node>> node = StartNode(detector.get(), flags);
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

  Future<TaskStatus> status;
  EXPECT_CALL(sched, statusUpdate(&driver, _))
    .WillOnce(FutureArg<1>(&status));

  Future<vector<Offer>> offers2;
  EXPECT_CALL(sched, resourceOffers(&driver, _))
    .WillOnce(FutureArg<1>(&offers2))
    .WillRepeatedly(Return());

  driver.launchTasks(
      offers->at(0).id(),
      {storeTask(offers->at(0), "undeclared", "results", Volume::RO)});

  AWAIT_READY(status);
  EXPECT_EQ(TASK_ERROR, status->state());
  EXPECT_EQ(TaskStatus::REASON_TASK_INVALID, status->reason());
  EXPECT_TRUE(strings::contains(
      status->message(), "Store 'results' is not declared on node"))
    << status->message();

  AWAIT_READY(offers2);
  ASSERT_FALSE(offers2->empty());

  EXPECT_CALL(sched, statusUpdate(&driver, _))
    .WillOnce(FutureArg<1>(&status));

  driver.launchTasks(
      offers2->at(0).id(),
      {storeTask(offers2->at(0), "unlisted", "tables", Volume::RO, false,
                 false)});

  AWAIT_READY(status);
  EXPECT_EQ(TASK_ERROR, status->state());
  EXPECT_TRUE(strings::contains(status->message(), "does not list"))
    << status->message();
  EXPECT_TRUE(strings::contains(status->message(), "store/tables"))
    << status->message();

  driver.stop();
  driver.join();
}


// The path checks run at submission: a nested relative path is refused
// before anything reaches the node, and one name inside the sandbox runs,
// reported in the status update exactly as the task gave it.
TEST_F(ControllerStoresTest, ARelativePathIsCheckedAtSubmission)
{
  Try<Owned<cluster::Controller>> controller = StartController();
  ASSERT_SOME(controller);

  MockExecutor exec(DEFAULT_EXECUTOR_ID);
  TestContainerizer containerizer(&exec);

  Flags flags = CreateNodeFlags();
  flags.stores = storesFile("tables", 0, 1);
  flags = deliversStores(flags);

  Owned<ControllerDetector> detector = controller.get()->createDetector();
  Try<Owned<cluster::Node>> node =
    StartNode(detector.get(), &containerizer, flags);
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

  TaskInfo nested = storeTask(offers->at(0), "nested", "tables", Volume::RW);
  nested.mutable_container()->mutable_volumes(0)->set_container_path(
      "data/tables");

  Future<TaskStatus> status;
  EXPECT_CALL(sched, statusUpdate(&driver, _))
    .WillOnce(FutureArg<1>(&status));

  Future<vector<Offer>> offers2;
  EXPECT_CALL(sched, resourceOffers(&driver, _))
    .WillOnce(FutureArg<1>(&offers2))
    .WillRepeatedly(Return());

  driver.launchTasks(offers->at(0).id(), {nested});

  AWAIT_READY(status);
  EXPECT_EQ(TASK_ERROR, status->state());
  EXPECT_EQ(TaskStatus::SOURCE_CONTROLLER, status->source());
  EXPECT_EQ(TaskStatus::REASON_TASK_INVALID, status->reason());
  EXPECT_TRUE(strings::contains(status->message(), "more than one component"))
    << status->message();

  AWAIT_READY(offers2);
  ASSERT_FALSE(offers2->empty());

  TaskInfo relative =
    storeTask(offers2->at(0), "relative", "tables", Volume::RW);
  relative.mutable_container()->mutable_volumes(0)->set_container_path(
      "tables");

  EXPECT_CALL(exec, registered(_, _, _, _));

  EXPECT_CALL(exec, launchTask(_, _))
    .WillOnce(SendStatusUpdateFromTask(TASK_RUNNING));

  Future<TaskStatus> running;
  EXPECT_CALL(sched, statusUpdate(&driver, _))
    .WillOnce(FutureArg<1>(&running));

  driver.launchTasks(offers2->at(0).id(), {relative});

  AWAIT_READY(running);
  EXPECT_EQ(TASK_RUNNING, running->state());
  ASSERT_EQ(1, running->stores_size());
  EXPECT_EQ("tables", running->stores(0).container_path());

  EXPECT_CALL(exec, shutdown(_))
    .Times(AtMost(1));

  driver.stop();
  driver.join();
}


// A store declared readers 0, as every store on a node with
// filesystem/posix is, is refused read-only at submission by name and
// runs read-write at one name inside the sandbox (medios#602). The node
// has the flags of a Mac: the posix launcher and no isolation named.
TEST_F(ControllerStoresTest, AStoreWithNoReadersRefusesReadOnly)
{
  Try<Owned<cluster::Controller>> controller = StartController();
  ASSERT_SOME(controller);

  MockExecutor exec(DEFAULT_EXECUTOR_ID);
  TestContainerizer containerizer(&exec);

  Flags flags = CreateNodeFlags();
  flags.stores = storesFile("tables", 0, 1);
  flags.containerizers = "medios";
  flags.launcher = "posix";
  flags.isolation = "";

  Owned<ControllerDetector> detector = controller.get()->createDetector();
  Try<Owned<cluster::Node>> node =
    StartNode(detector.get(), &containerizer, flags);
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

  TaskInfo reader = storeTask(offers->at(0), "reader", "tables", Volume::RO);
  reader.mutable_container()->mutable_volumes(0)->set_container_path(
      "tables");

  Future<TaskStatus> refused;
  EXPECT_CALL(sched, statusUpdate(&driver, _))
    .WillOnce(FutureArg<1>(&refused));

  Future<vector<Offer>> offers2;
  EXPECT_CALL(sched, resourceOffers(&driver, _))
    .WillOnce(FutureArg<1>(&offers2))
    .WillRepeatedly(Return());

  driver.launchTasks(offers->at(0).id(), {reader});

  AWAIT_READY(refused);
  EXPECT_EQ(TASK_ERROR, refused->state());
  EXPECT_EQ(TaskStatus::SOURCE_CONTROLLER, refused->source());
  EXPECT_EQ(TaskStatus::REASON_TASK_INVALID, refused->reason());
  EXPECT_TRUE(strings::contains(refused->message(), "(readers 0)"))
    << refused->message();
  EXPECT_TRUE(strings::contains(refused->message(), "request it RW"))
    << refused->message();

  AWAIT_READY(offers2);
  ASSERT_FALSE(offers2->empty());

  TaskInfo writer = storeTask(offers2->at(0), "writer", "tables", Volume::RW);
  writer.mutable_container()->mutable_volumes(0)->set_container_path(
      "tables");

  EXPECT_CALL(exec, registered(_, _, _, _));

  EXPECT_CALL(exec, launchTask(_, _))
    .WillOnce(SendStatusUpdateFromTask(TASK_RUNNING));

  Future<TaskStatus> running;
  EXPECT_CALL(sched, statusUpdate(&driver, _))
    .WillOnce(FutureArg<1>(&running));

  driver.launchTasks(offers2->at(0).id(), {writer});

  AWAIT_READY(running);
  EXPECT_EQ(TASK_RUNNING, running->state());

  EXPECT_CALL(exec, shutdown(_))
    .Times(AtMost(1));

  driver.stop();
  driver.join();
}


// readers is a count of holders: the first reader runs and the status
// update carries what it was given; the second is refused while the first
// holds; a writer is refused on a store declared writers 0; once the first
// reader ends its unit is free again. /state shows the held counts.
TEST_F(ControllerStoresTest, ReaderUnitsAreCountedAndReleased)
{
  Try<Owned<cluster::Controller>> controller = StartController();
  ASSERT_SOME(controller);

  MockExecutor exec(DEFAULT_EXECUTOR_ID);
  TestContainerizer containerizer(&exec);

  Flags flags = CreateNodeFlags();
  flags.stores = storesFile("tables", 1, 0);
  flags = deliversStores(flags);

  Owned<ControllerDetector> detector = controller.get()->createDetector();
  Try<Owned<cluster::Node>> node =
    StartNode(detector.get(), &containerizer, flags);
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

  EXPECT_CALL(exec, registered(_, _, _, _));

  EXPECT_CALL(exec, launchTask(_, _))
    .WillOnce(SendStatusUpdateFromTask(TASK_RUNNING));

  Future<TaskStatus> running;
  EXPECT_CALL(sched, statusUpdate(&driver, _))
    .WillOnce(FutureArg<1>(&running));

  // Keep the remainder of the offer for the next launches: decline is not
  // needed, the controller re-offers what the first task leaves.
  Future<vector<Offer>> moreOffers;
  EXPECT_CALL(sched, resourceOffers(&driver, _))
    .WillOnce(FutureArg<1>(&moreOffers))
    .WillRepeatedly(Return());

  driver.launchTasks(
      offers->at(0).id(),
      {storeTask(offers->at(0), "reader-1", "tables", Volume::RO)});

  AWAIT_READY(running);
  EXPECT_EQ(TASK_RUNNING, running->state());
  ASSERT_EQ(1, running->stores_size());
  EXPECT_EQ("tables", running->stores(0).store());
  EXPECT_EQ("/data", running->stores(0).container_path());
  EXPECT_EQ(Volume::RO, running->stores(0).mode());
  EXPECT_FALSE(running->stores(0).exclusive());
  EXPECT_EQ(offers->at(0).node_id(), running->stores(0).node_id());

  AWAIT_READY(moreOffers);
  ASSERT_FALSE(moreOffers->empty());

  // The store is offered again while held: it is shared.
  bool offeredAgain = false;
  foreach (const Resource& resource, moreOffers->at(0).resources()) {
    if (resource.name() == "store/tables") {
      offeredAgain = true;
    }
  }
  EXPECT_TRUE(offeredAgain) << moreOffers->at(0).resources();

  // /state: the task's stores and the node's held count.
  {
    Future<Response> response = process::http::get(
        controller.get()->pid,
        "state",
        None(),
        createBasicAuthHeaders(DEFAULT_CREDENTIAL));

    AWAIT_EXPECT_RESPONSE_STATUS_EQ(OK().status, response);

    Try<JSON::Object> parse = JSON::parse<JSON::Object>(response->body);
    ASSERT_SOME(parse);

    Result<JSON::String> store = parse->find<JSON::String>(
        "frameworks[0].tasks[0].stores[0].store");
    EXPECT_SOME_EQ(string("tables"), store);

    Result<JSON::String> mode = parse->find<JSON::String>(
        "frameworks[0].tasks[0].stores[0].mode");
    EXPECT_SOME_EQ(string("RO"), mode);

    Result<JSON::String> name =
      parse->find<JSON::String>("nodes[0].stores[0].name");
    EXPECT_SOME_EQ(string("tables"), name);

    Result<JSON::Number> readers =
      parse->find<JSON::Number>("nodes[0].stores[0].readers");
    ASSERT_SOME(readers);
    EXPECT_EQ(1, readers->as<int>());

    Result<JSON::Number> heldReaders =
      parse->find<JSON::Number>("nodes[0].stores[0].held_readers");
    ASSERT_SOME(heldReaders);
    EXPECT_EQ(1, heldReaders->as<int>());

    Result<JSON::Number> heldWriters =
      parse->find<JSON::Number>("nodes[0].stores[0].held_writers");
    ASSERT_SOME(heldWriters);
    EXPECT_EQ(0, heldWriters->as<int>());
  }

  // A second reader: the one unit is held.
  Future<TaskStatus> refused;
  EXPECT_CALL(sched, statusUpdate(&driver, _))
    .WillOnce(FutureArg<1>(&refused));

  driver.launchTasks(
      moreOffers->at(0).id(),
      {storeTask(moreOffers->at(0), "reader-2", "tables", Volume::RO)});

  AWAIT_READY(refused);
  EXPECT_EQ(TASK_ERROR, refused->state());
  EXPECT_EQ(TaskStatus::REASON_TASK_INVALID, refused->reason());
  EXPECT_TRUE(strings::contains(
      refused->message(), "no free readers unit"))
    << refused->message();
  EXPECT_TRUE(strings::contains(refused->message(), "1 of 1 held"))
    << refused->message();

  // A writer on a store declared writers 0.
  Future<vector<Offer>> offersAfterRefusal;
  EXPECT_CALL(sched, resourceOffers(&driver, _))
    .WillOnce(FutureArg<1>(&offersAfterRefusal))
    .WillRepeatedly(Return());

  AWAIT_READY(offersAfterRefusal);
  ASSERT_FALSE(offersAfterRefusal->empty());

  EXPECT_CALL(sched, statusUpdate(&driver, _))
    .WillOnce(FutureArg<1>(&refused));

  driver.launchTasks(
      offersAfterRefusal->at(0).id(),
      {storeTask(offersAfterRefusal->at(0), "writer", "tables", Volume::RW)});

  AWAIT_READY(refused);
  EXPECT_EQ(TASK_ERROR, refused->state());
  EXPECT_TRUE(strings::contains(
      refused->message(), "read-only by declaration"))
    << refused->message();

  // The first reader ends; its unit comes back.
  Future<vector<Offer>> offersAfterEnd;
  EXPECT_CALL(sched, resourceOffers(&driver, _))
    .WillOnce(FutureArg<1>(&offersAfterEnd))
    .WillRepeatedly(Return());

  Future<TaskStatus> killed;
  EXPECT_CALL(sched, statusUpdate(&driver, _))
    .WillOnce(FutureArg<1>(&killed));

  EXPECT_CALL(exec, killTask(_, _))
    .WillOnce(SendStatusUpdateFromTaskID(TASK_KILLED));

  driver.killTask(running->task_id());

  AWAIT_READY(killed);
  EXPECT_EQ(TASK_KILLED, killed->state());
  ASSERT_EQ(1, killed->stores_size());
  EXPECT_EQ("tables", killed->stores(0).store());

  AWAIT_READY(offersAfterEnd);
  ASSERT_FALSE(offersAfterEnd->empty());

  EXPECT_CALL(exec, launchTask(_, _))
    .WillOnce(SendStatusUpdateFromTask(TASK_RUNNING));

  Future<TaskStatus> runningAgain;
  EXPECT_CALL(sched, statusUpdate(&driver, _))
    .WillOnce(FutureArg<1>(&runningAgain));

  driver.launchTasks(
      offersAfterEnd->at(0).id(),
      {storeTask(offersAfterEnd->at(0), "reader-3", "tables", Volume::RO)});

  AWAIT_READY(runningAgain);
  EXPECT_EQ(TASK_RUNNING, runningAgain->state());

  EXPECT_CALL(exec, shutdown(_))
    .Times(AtMost(1));

  driver.stop();
  driver.join();
}


// exclusive on a read-write request takes every writer unit: nothing
// else writes beside it, and it is refused while anything does. On a
// read-only request it is refused by name.
TEST_F(ControllerStoresTest, ExclusiveTakesEveryWriterUnit)
{
  Try<Owned<cluster::Controller>> controller = StartController();
  ASSERT_SOME(controller);

  MockExecutor exec(DEFAULT_EXECUTOR_ID);
  TestContainerizer containerizer(&exec);

  Flags flags = CreateNodeFlags();
  flags.stores = storesFile("results", 2, 2);
  flags = deliversStores(flags);

  Owned<ControllerDetector> detector = controller.get()->createDetector();
  Try<Owned<cluster::Node>> node =
    StartNode(detector.get(), &containerizer, flags);
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

  Future<TaskStatus> refused;
  EXPECT_CALL(sched, statusUpdate(&driver, _))
    .WillOnce(FutureArg<1>(&refused));

  driver.launchTasks(
      offers->at(0).id(),
      {storeTask(offers->at(0), "ro-exclusive", "results", Volume::RO, true)});

  AWAIT_READY(refused);
  EXPECT_EQ(TASK_ERROR, refused->state());
  EXPECT_TRUE(strings::contains(
      refused->message(), "read-only and exclusive"))
    << refused->message();

  Future<vector<Offer>> offers2;
  EXPECT_CALL(sched, resourceOffers(&driver, _))
    .WillOnce(FutureArg<1>(&offers2))
    .WillRepeatedly(Return());

  AWAIT_READY(offers2);
  ASSERT_FALSE(offers2->empty());

  EXPECT_CALL(exec, registered(_, _, _, _));

  EXPECT_CALL(exec, launchTask(_, _))
    .WillOnce(SendStatusUpdateFromTask(TASK_RUNNING));

  Future<TaskStatus> running;
  EXPECT_CALL(sched, statusUpdate(&driver, _))
    .WillOnce(FutureArg<1>(&running));

  Future<vector<Offer>> offers3;
  EXPECT_CALL(sched, resourceOffers(&driver, _))
    .WillOnce(FutureArg<1>(&offers3))
    .WillRepeatedly(Return());

  driver.launchTasks(
      offers2->at(0).id(),
      {storeTask(offers2->at(0), "exclusive", "results", Volume::RW, true)});

  AWAIT_READY(running);
  EXPECT_EQ(TASK_RUNNING, running->state());
  ASSERT_EQ(1, running->stores_size());
  EXPECT_TRUE(running->stores(0).exclusive());

  AWAIT_READY(offers3);
  ASSERT_FALSE(offers3->empty());

  // Every writer unit is held by the exclusive holder; a reader still fits.
  {
    Future<Response> response = process::http::get(
        controller.get()->pid,
        "state",
        None(),
        createBasicAuthHeaders(DEFAULT_CREDENTIAL));

    AWAIT_EXPECT_RESPONSE_STATUS_EQ(OK().status, response);

    Try<JSON::Object> parse = JSON::parse<JSON::Object>(response->body);
    ASSERT_SOME(parse);

    Result<JSON::Number> heldWriters =
      parse->find<JSON::Number>("nodes[0].stores[0].held_writers");
    ASSERT_SOME(heldWriters);
    EXPECT_EQ(2, heldWriters->as<int>());
  }

  EXPECT_CALL(sched, statusUpdate(&driver, _))
    .WillOnce(FutureArg<1>(&refused));

  driver.launchTasks(
      offers3->at(0).id(),
      {storeTask(offers3->at(0), "second-writer", "results", Volume::RW)});

  AWAIT_READY(refused);
  EXPECT_EQ(TASK_ERROR, refused->state());
  EXPECT_TRUE(strings::contains(refused->message(), "no free writers unit"))
    << refused->message();
  EXPECT_TRUE(strings::contains(refused->message(), "2 of 2 held"))
    << refused->message();

  EXPECT_CALL(exec, shutdown(_))
    .Times(AtMost(1));

  driver.stop();
  driver.join();
}


// A node whose medios containerizer cannot deliver a store (the posix
// launcher with filesystem/linux, a pairing that neither binds nor
// links one) refuses a MEDIOS task with a
// STORE volume before launching anything, as TASK_DROPPED from the node
// naming the store and what the node lacks. The node starts because its
// docker containerizer delivers stores to docker tasks; the controller
// accepted the task: the node declared the store, and the task claims it.
TEST_F(ControllerStoresTest, ANodeThatCannotDeliverAStoreRefusesTheTask)
{
  Try<Owned<cluster::Controller>> controller = StartController();
  ASSERT_SOME(controller);

  MockExecutor exec(DEFAULT_EXECUTOR_ID);
  TestContainerizer containerizer(&exec);

  Flags flags = CreateNodeFlags();
  flags.stores = storesFile("tables", 2, 0);
  flags.containerizers = "docker_shell,medios";
  flags.launcher = "posix";
  flags.isolation = "filesystem/linux";

  Owned<ControllerDetector> detector = controller.get()->createDetector();
  Try<Owned<cluster::Node>> node =
    StartNode(detector.get(), &containerizer, flags);
  ASSERT_SOME(node);

  // Partition-aware, so the drop is reported as TASK_DROPPED and not as
  // the older TASK_LOST.
  FrameworkInfo frameworkInfo = DEFAULT_FRAMEWORK_INFO;
  frameworkInfo.add_capabilities()->set_type(
      FrameworkInfo::Capability::PARTITION_AWARE);

  MockScheduler sched;
  MediosSchedulerDriver driver(
      &sched, frameworkInfo, controller.get()->pid, DEFAULT_CREDENTIAL);

  EXPECT_CALL(sched, registered(&driver, _, _));

  Future<vector<Offer>> offers;
  EXPECT_CALL(sched, resourceOffers(&driver, _))
    .WillOnce(FutureArg<1>(&offers))
    .WillRepeatedly(Return());

  driver.start();

  AWAIT_READY(offers);
  ASSERT_FALSE(offers->empty());

  // Never launched: the executor is not asked for anything.
  EXPECT_CALL(exec, registered(_, _, _, _)).Times(0);
  EXPECT_CALL(exec, launchTask(_, _)).Times(0);

  TaskInfo task = storeTask(offers->at(0), "t1", "tables", Volume::RO);

  Future<TaskStatus> status;
  EXPECT_CALL(sched, statusUpdate(&driver, _))
    .WillOnce(FutureArg<1>(&status))
    .WillRepeatedly(Return());

  driver.launchTasks(offers->at(0).id(), {task});

  AWAIT_READY(status);
  EXPECT_EQ(task.task_id(), status->task_id());
  EXPECT_EQ(TASK_DROPPED, status->state());
  EXPECT_EQ(TaskStatus::SOURCE_NODE, status->source());
  EXPECT_FALSE(status->has_reason());
  EXPECT_TRUE(strings::contains(status->message(), "store 'tables'"))
    << status->message();
  EXPECT_TRUE(strings::contains(status->message(), "'/data'"))
    << status->message();
  EXPECT_TRUE(strings::contains(status->message(), "cannot deliver"))
    << status->message();
  EXPECT_TRUE(strings::contains(status->message(), "launcher is 'posix'"))
    << status->message();

  driver.stop();
  driver.join();
}

} // namespace tests {
} // namespace internal {
} // namespace medios {
