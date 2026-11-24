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

#include <algorithm>
#include <iostream>
#include <random>
#include <map>
#include <set>
#include <string>
#include <vector>

#include <google/protobuf/util/message_differencer.h>

#include <medios/attributes.hpp>
#include <medios/type_utils.hpp>

#include <medios/authentication/http/basic_authenticator_factory.hpp>

#include <medios/state/in_memory.hpp>
#include <medios/state/leveldb.hpp>
#include <medios/state/state.hpp>
#include <medios/state/storage.hpp>

#include <process/clock.hpp>
#include <process/gmock.hpp>
#include <process/gtest.hpp>
#include <process/pid.hpp>
#include <process/process.hpp>

#include <stout/bytes.hpp>
#include <stout/stopwatch.hpp>
#include <stout/uuid.hpp>

#include <stout/tests/utils.hpp>

#include "common/protobuf_utils.hpp"

#include "controller/flags.hpp"
#include "controller/maintenance.hpp"
#include "controller/controller.hpp"
#include "controller/quota.hpp"
#include "controller/registrar.hpp"
#include "controller/registry_operations.hpp"
#include "controller/weights.hpp"

#include "tests/medios.hpp"

using namespace medios::internal::controller;

using namespace process;

using std::cout;
using std::endl;
using std::map;
using std::pair;
using std::set;
using std::string;
using std::vector;

using process::Clock;
using process::Owned;

using process::http::OK;
using process::http::Response;
using process::http::Unauthorized;

using google::protobuf::Map;
using google::protobuf::RepeatedPtrField;
using google::protobuf::util::MessageDifferencer;

using medios::internal::protobuf::maintenance::createMachineList;
using medios::internal::protobuf::maintenance::createSchedule;
using medios::internal::protobuf::maintenance::createUnavailability;
using medios::internal::protobuf::maintenance::createWindow;

using testing::_;
using testing::DoAll;
using testing::Eq;
using testing::Return;

using ::testing::WithParamInterface;

namespace medios {
namespace internal {
namespace tests {

namespace quota = medios::internal::controller::quota;

namespace authentication = process::http::authentication;

using namespace medios::maintenance;
using namespace medios::quota;

using namespace medios::internal::controller::maintenance;
using namespace medios::internal::controller::quota;
using namespace medios::internal::controller::weights;

using medios::http::authentication::BasicAuthenticatorFactory;

using medios::state::State;
using medios::state::Storage;

using state::Entry;


static vector<WeightInfo> getWeightInfos(
    const hashmap<string, double>& weights) {
  vector<WeightInfo> weightInfos;

  foreachpair (const string& role, double weight, weights) {
    WeightInfo weightInfo;
    weightInfo.set_role(role);
    weightInfo.set_weight(weight);
    weightInfos.push_back(weightInfo);
  }

  return weightInfos;
}


class RegistrarTestBase : public TemporaryDirectoryTest
{
public:
  RegistrarTestBase()
    : storage(nullptr),
      state(nullptr) {}

protected:
  void SetUp() override
  {
    TemporaryDirectoryTest::SetUp();

    storage = new medios::state::InMemoryStorage();
    state = new State(storage);

    // Compensate for slow CI machines / VMs.
    flags.registry_store_timeout = process::TEST_AWAIT_TIMEOUT;

    controller.CopyFrom(protobuf::createControllerInfo(UPID("controller@127.0.0.1:5050")));

    NodeID id;
    id.set_value("1");

    NodeInfo info;
    info.set_hostname("localhost");
    info.mutable_id()->CopyFrom(id);

    node.CopyFrom(info);
  }

  void TearDown() override
  {
    delete state;
    delete storage;

    TemporaryDirectoryTest::TearDown();
  }

  Storage* storage;
  State* state;

  ControllerInfo controller;
  NodeInfo node;
  Flags flags;
};


class RegistrarTest : public RegistrarTestBase {};


TEST_F(RegistrarTest, Recover)
{
  Registrar registrar(flags, state);

  // Operations preceding recovery will fail.
  AWAIT_EXPECT_FAILED(registrar.apply(Owned<RegistryOperation>(
      new AdmitNode(node))));
  AWAIT_EXPECT_FAILED(registrar.apply(Owned<RegistryOperation>(
      new MarkNodeUnreachable(node, protobuf::getCurrentTime()))));
  AWAIT_EXPECT_FAILED(registrar.apply(Owned<RegistryOperation>(
      new MarkNodeReachable(node))));
  AWAIT_EXPECT_FAILED(registrar.apply(Owned<RegistryOperation>(
      new RemoveNode(node))));

  Future<Registry> registry = registrar.recover(controller);

  // Before waiting for the recovery to complete, invoke some
  // operations to ensure they do not fail.
  Future<bool> admit = registrar.apply(Owned<RegistryOperation>(
      new AdmitNode(node)));
  Future<bool> unreachable = registrar.apply(Owned<RegistryOperation>(
      new MarkNodeUnreachable(node, protobuf::getCurrentTime())));
  Future<bool> reachable = registrar.apply(Owned<RegistryOperation>(
      new MarkNodeReachable(node)));
  Future<bool> remove = registrar.apply(Owned<RegistryOperation>(
      new RemoveNode(node)));

  AWAIT_READY(registry);
  EXPECT_EQ(controller, registry->controller().info());

  AWAIT_TRUE(admit);
  AWAIT_TRUE(unreachable);
  AWAIT_TRUE(reachable);
  AWAIT_TRUE(remove);
}


TEST_F(RegistrarTest, Admit)
{
  Registrar registrar(flags, state);
  AWAIT_READY(registrar.recover(controller));

  AWAIT_TRUE(registrar.apply(Owned<RegistryOperation>(new AdmitNode(node))));
  AWAIT_FALSE(registrar.apply(Owned<RegistryOperation>(new AdmitNode(node))));
}


// The LevelDB storage backend persists registry state across registrar
// generations on the same path — the single-controller durability
// contract behind `--registry=leveldb`.
TEST_F(RegistrarTest, LevelDBStorageSurvivesRegistrarRestart)
{
  const std::string path = path::join(os::getcwd(), "registry-leveldb");

  // First registrar generation: recover and admit a node.
  {
    medios::state::LevelDBStorage leveldbStorage(path);
    State leveldbState(&leveldbStorage);
    Registrar registrar(flags, &leveldbState);

    AWAIT_READY(registrar.recover(controller));
    AWAIT_TRUE(registrar.apply(Owned<RegistryOperation>(new AdmitNode(node))));
  }

  // Second generation over the same path: the admitted node is present
  // after recovery, exactly as if the controller had restarted.
  {
    medios::state::LevelDBStorage leveldbStorage(path);
    State leveldbState(&leveldbStorage);
    Registrar registrar(flags, &leveldbState);

    Future<Registry> registry = registrar.recover(controller);
    AWAIT_READY(registry);

    ASSERT_EQ(1, registry->nodes().nodes().size());
    EXPECT_EQ(node.id(), registry->nodes().nodes(0).info().id());

    // The recovered node is a known node: re-admission is refused.
    AWAIT_FALSE(registrar.apply(Owned<RegistryOperation>(new AdmitNode(node))));
  }
}


TEST_F(RegistrarTest, UpdateNode)
{
  // Add a new node to the registry.
  {
    Registrar registrar(flags, state);
    AWAIT_READY(registrar.recover(controller));

    node.set_hostname("original");
    AWAIT_TRUE(registrar.apply(Owned<RegistryOperation>(
        new AdmitNode(node))));
  }


  // Verify that the node is present, and update its hostname.
  {
    Registrar registrar(flags, state);
    Future<Registry> registry = registrar.recover(controller);
    AWAIT_READY(registry);

    ASSERT_EQ(1, registry->nodes().nodes().size());
    EXPECT_EQ("original", registry->nodes().nodes(0).info().hostname());

    node.set_hostname("changed");
    AWAIT_TRUE(registrar.apply(Owned<RegistryOperation>(
        new UpdateNode(node))));
  }

  // Verify that the hostname indeed changed, and do one additional update
  // to check that the operation is idempotent.
  {
    Registrar registrar(flags, state);
    Future<Registry> registry = registrar.recover(controller);
    AWAIT_READY(registry);

    ASSERT_EQ(1, registry->nodes().nodes().size());
    EXPECT_EQ("changed", registry->nodes().nodes(0).info().hostname());

    AWAIT_TRUE(registrar.apply(Owned<RegistryOperation>(
        new UpdateNode(node))));
  }

  // Verify that nothing changed from the second update.
  {
    Registrar registrar(flags, state);
    Future<Registry> registry = registrar.recover(controller);
    AWAIT_READY(registry);

    ASSERT_EQ(1, registry->nodes().nodes().size());
    EXPECT_EQ("changed", registry->nodes().nodes(0).info().hostname());
  }
}


TEST_F(RegistrarTest, MarkReachable)
{
  Registrar registrar(flags, state);
  AWAIT_READY(registrar.recover(controller));

  NodeID id1;
  id1.set_value("1");

  NodeInfo info1;
  info1.set_hostname("localhost");
  info1.mutable_id()->CopyFrom(id1);

  NodeID id2;
  id2.set_value("2");

  NodeInfo info2;
  info2.set_hostname("localhost");
  info2.mutable_id()->CopyFrom(id2);

  AWAIT_TRUE(registrar.apply(Owned<RegistryOperation>(
      new AdmitNode(info1))));

  // Attempting to mark a node as reachable that is already reachable
  // should not result in an error.
  AWAIT_TRUE(registrar.apply(Owned<RegistryOperation>(
      new MarkNodeReachable(info1))));
  AWAIT_TRUE(registrar.apply(Owned<RegistryOperation>(
      new MarkNodeReachable(info1))));

  // Attempting to mark a node as reachable that is not in the
  // unreachable list should not result in error.
  AWAIT_TRUE(registrar.apply(Owned<RegistryOperation>(
      new MarkNodeReachable(info2))));
}


TEST_F(RegistrarTest, MarkUnreachable)
{
  Registrar registrar(flags, state);
  AWAIT_READY(registrar.recover(controller));

  NodeID id1;
  id1.set_value("1");

  NodeInfo info1;
  info1.set_hostname("localhost");
  info1.mutable_id()->CopyFrom(id1);

  NodeID id2;
  id2.set_value("2");

  NodeInfo info2;
  info2.set_hostname("localhost");
  info2.mutable_id()->CopyFrom(id2);

  AWAIT_TRUE(registrar.apply(Owned<RegistryOperation>(
      new AdmitNode(info1))));

  AWAIT_TRUE(registrar.apply(Owned<RegistryOperation>(
      new MarkNodeUnreachable(info1, protobuf::getCurrentTime()))));

  AWAIT_TRUE(registrar.apply(Owned<RegistryOperation>(
      new MarkNodeReachable(info1))));

  AWAIT_TRUE(registrar.apply(Owned<RegistryOperation>(
      new MarkNodeUnreachable(info1, protobuf::getCurrentTime()))));

  // If a node is already unreachable, trying to mark it unreachable
  // again should fail.
  AWAIT_FALSE(registrar.apply(Owned<RegistryOperation>(
      new MarkNodeUnreachable(info1, protobuf::getCurrentTime()))));
}


// Verify that an admitted node can be marked as gone.
TEST_F(RegistrarTest, MarkGone)
{
  Registrar registrar(flags, state);
  AWAIT_READY(registrar.recover(controller));

  NodeID id1;
  id1.set_value("1");

  NodeInfo info1;
  info1.set_hostname("localhost");
  info1.mutable_id()->CopyFrom(id1);

  AWAIT_TRUE(registrar.apply(Owned<RegistryOperation>(
      new AdmitNode(info1))));

  AWAIT_TRUE(registrar.apply(Owned<RegistryOperation>(
      new MarkNodeGone(info1.id(), protobuf::getCurrentTime()))));
}


// Verify that an unreachable node can be marked as gone.
TEST_F(RegistrarTest, MarkUnreachableGone)
{
  Registrar registrar(flags, state);
  AWAIT_READY(registrar.recover(controller));

  NodeID id1;
  id1.set_value("1");

  NodeInfo info1;
  info1.set_hostname("localhost");
  info1.mutable_id()->CopyFrom(id1);

  AWAIT_TRUE(registrar.apply(Owned<RegistryOperation>(
      new AdmitNode(info1))));

  AWAIT_TRUE(registrar.apply(Owned<RegistryOperation>(
      new MarkNodeUnreachable(info1, protobuf::getCurrentTime()))));

  AWAIT_TRUE(registrar.apply(Owned<RegistryOperation>(
      new MarkNodeGone(info1.id(), protobuf::getCurrentTime()))));

  // If a node is already gone, trying to mark it gone again should fail.
  AWAIT_FALSE(registrar.apply(Owned<RegistryOperation>(
      new MarkNodeGone(info1.id(), protobuf::getCurrentTime()))));

  // If a node is already gone, trying to mark it unreachable
  // again should fail.
  AWAIT_FALSE(registrar.apply(Owned<RegistryOperation>(
      new MarkNodeUnreachable(info1, protobuf::getCurrentTime()))));
}


TEST_F(RegistrarTest, Prune)
{
  Registrar registrar(flags, state);
  AWAIT_READY(registrar.recover(controller));

  NodeID id1;
  id1.set_value("1");

  NodeInfo info1;
  info1.set_hostname("localhost");
  info1.mutable_id()->CopyFrom(id1);

  NodeID id2;
  id2.set_value("2");

  NodeInfo info2;
  info2.set_hostname("localhost");
  info2.mutable_id()->CopyFrom(id2);

  NodeID id3;
  id3.set_value("3");

  NodeInfo info3;
  info3.set_hostname("localhost");
  info3.mutable_id()->CopyFrom(id3);

  AWAIT_TRUE(registrar.apply(Owned<RegistryOperation>(
      new AdmitNode(info1))));
  AWAIT_TRUE(registrar.apply(Owned<RegistryOperation>(
      new AdmitNode(info2))));
  AWAIT_TRUE(registrar.apply(Owned<RegistryOperation>(
      new AdmitNode(info3))));

  AWAIT_TRUE(registrar.apply(Owned<RegistryOperation>(
      new MarkNodeUnreachable(info1, protobuf::getCurrentTime()))));

  AWAIT_TRUE(registrar.apply(Owned<RegistryOperation>(
      new MarkNodeUnreachable(info2, protobuf::getCurrentTime()))));

  AWAIT_TRUE(registrar.apply(Owned<RegistryOperation>(
      new MarkNodeGone(info3.id(), protobuf::getCurrentTime()))));

  AWAIT_TRUE(registrar.apply(Owned<RegistryOperation>(
      new Prune({id1}, {}))));
  AWAIT_TRUE(registrar.apply(Owned<RegistryOperation>(
      new Prune({id2}, {}))));
  AWAIT_TRUE(registrar.apply(Owned<RegistryOperation>(
      new Prune({}, {id3}))));

  AWAIT_TRUE(registrar.apply(Owned<RegistryOperation>(
      new AdmitNode(info1))));
  AWAIT_TRUE(registrar.apply(Owned<RegistryOperation>(
      new AdmitNode(info2))));
  AWAIT_TRUE(registrar.apply(Owned<RegistryOperation>(
      new AdmitNode(info3))));

  AWAIT_TRUE(registrar.apply(Owned<RegistryOperation>(
      new MarkNodeUnreachable(info1, protobuf::getCurrentTime()))));

  AWAIT_TRUE(registrar.apply(Owned<RegistryOperation>(
      new MarkNodeUnreachable(info2, protobuf::getCurrentTime()))));

  AWAIT_TRUE(registrar.apply(Owned<RegistryOperation>(
      new MarkNodeGone(info3.id(), protobuf::getCurrentTime()))));

  AWAIT_TRUE(registrar.apply(Owned<RegistryOperation>(
      new Prune({id1}, {}))));
  AWAIT_TRUE(registrar.apply(Owned<RegistryOperation>(
      new Prune({id2}, {}))));
  AWAIT_TRUE(registrar.apply(Owned<RegistryOperation>(
      new Prune({}, {id3}))));
}


TEST_F(RegistrarTest, Remove)
{
  Registrar registrar(flags, state);
  AWAIT_READY(registrar.recover(controller));

  NodeID id1;
  id1.set_value("1");

  NodeInfo info1;
  info1.set_hostname("localhost");
  info1.mutable_id()->CopyFrom(id1);

  NodeID id2;
  id2.set_value("2");

  NodeInfo info2;
  info2.set_hostname("localhost");
  info2.mutable_id()->CopyFrom(id2);

  NodeID id3;
  id3.set_value("3");

  NodeInfo info3;
  info3.set_hostname("localhost");
  info3.mutable_id()->CopyFrom(id3);

  AWAIT_TRUE(registrar.apply(Owned<RegistryOperation>(
      new AdmitNode(info1))));
  AWAIT_TRUE(registrar.apply(Owned<RegistryOperation>(
      new AdmitNode(info2))));
  AWAIT_TRUE(registrar.apply(Owned<RegistryOperation>(
      new AdmitNode(info3))));
  AWAIT_TRUE(registrar.apply(Owned<RegistryOperation>(
      new RemoveNode(info1))));
  AWAIT_FALSE(registrar.apply(Owned<RegistryOperation>(
      new RemoveNode(info1))));
  AWAIT_TRUE(registrar.apply(Owned<RegistryOperation>(
      new AdmitNode(info1))));
  AWAIT_TRUE(registrar.apply(Owned<RegistryOperation>(
      new RemoveNode(info2))));
  AWAIT_FALSE(registrar.apply(Owned<RegistryOperation>(
      new RemoveNode(info2))));
  AWAIT_TRUE(registrar.apply(Owned<RegistryOperation>(
      new RemoveNode(info3))));
  AWAIT_FALSE(registrar.apply(Owned<RegistryOperation>(
      new RemoveNode(info3))));
}


// NOTE: For the following tests, the state of the registrar can
// only be viewed once per instantiation of the registrar.
// To check the result of each operation, we must re-construct
// the registrar, which is done by putting the code into scoped blocks.

// TODO(josephw): Consider refactoring these maintenance operation tests
// to use a helper function for each un-named scoped block.
// For example:
//   MaintenanceTest(flags, state, [=](const Registry& registry) {
//     // Checks and operations.  i.e.:
//     EXPECT_EQ(1, registry->schedules().size());
//   });

// Adds maintenance schedules to the registry, one machine at a time.
// Then removes machines from the schedule.
TEST_F(RegistrarTest, UpdateMaintenanceSchedule)
{
  // Machine definitions used in this test.
  MachineID machine1;
  machine1.set_ip("0.0.0.1");

  MachineID machine2;
  machine2.set_hostname("2");

  MachineID machine3;
  machine3.set_hostname("3");
  machine3.set_ip("0.0.0.3");

  Unavailability unavailability = createUnavailability(Clock::now());

  {
    // Prepare the registrar.
    Registrar registrar(flags, state);
    AWAIT_READY(registrar.recover(controller));

    // Schedule one machine for maintenance.
    maintenance::Schedule schedule = createSchedule(
        {createWindow({machine1}, unavailability)});

    AWAIT_READY(registrar.apply(Owned<RegistryOperation>(
        new UpdateSchedule(schedule))));
  }

  {
    // Check that one schedule and one machine info was made.
    Registrar registrar(flags, state);
    Future<Registry> registry = registrar.recover(controller);
    AWAIT_READY(registry);

    EXPECT_EQ(1, registry->schedules().size());
    EXPECT_EQ(1, registry->schedules(0).windows().size());
    EXPECT_EQ(1, registry->schedules(0).windows(0).machine_ids().size());
    EXPECT_EQ(1, registry->machines().machines().size());
    EXPECT_EQ(
        MachineInfo::DRAINING,
        registry->machines().machines(0).info().mode());

    // Extend the schedule by one machine (in a different window).
    maintenance::Schedule schedule = createSchedule({
        createWindow({machine1}, unavailability),
        createWindow({machine2}, unavailability)});

    AWAIT_READY(registrar.apply(Owned<RegistryOperation>(
        new UpdateSchedule(schedule))));
  }

  {
    // Check that both machines are part of maintenance.
    Registrar registrar(flags, state);
    Future<Registry> registry = registrar.recover(controller);
    AWAIT_READY(registry);

    EXPECT_EQ(1, registry->schedules().size());
    EXPECT_EQ(2, registry->schedules(0).windows().size());
    EXPECT_EQ(1, registry->schedules(0).windows(0).machine_ids().size());
    EXPECT_EQ(1, registry->schedules(0).windows(1).machine_ids().size());
    EXPECT_EQ(2, registry->machines().machines().size());
    EXPECT_EQ(
        MachineInfo::DRAINING,
        registry->machines().machines(1).info().mode());

    // Extend a window by one machine.
    maintenance::Schedule schedule = createSchedule({
        createWindow({machine1}, unavailability),
        createWindow({machine2, machine3}, unavailability)});

    AWAIT_READY(registrar.apply(Owned<RegistryOperation>(
        new UpdateSchedule(schedule))));
  }

  {
    // Check that all three machines are included.
    Registrar registrar(flags, state);
    Future<Registry> registry = registrar.recover(controller);
    AWAIT_READY(registry);

    EXPECT_EQ(1, registry->schedules().size());
    EXPECT_EQ(2, registry->schedules(0).windows().size());
    EXPECT_EQ(1, registry->schedules(0).windows(0).machine_ids().size());
    EXPECT_EQ(2, registry->schedules(0).windows(1).machine_ids().size());
    EXPECT_EQ(3, registry->machines().machines().size());
    EXPECT_EQ(
        MachineInfo::DRAINING,
        registry->machines().machines(2).info().mode());

    // Rearrange the schedule into one window.
    maintenance::Schedule schedule = createSchedule(
        {createWindow({machine1, machine2, machine3}, unavailability)});

    AWAIT_READY(registrar.apply(Owned<RegistryOperation>(
        new UpdateSchedule(schedule))));
  }

  {
    // Check that the machine infos are unchanged, but the schedule is.
    Registrar registrar(flags, state);
    Future<Registry> registry = registrar.recover(controller);
    AWAIT_READY(registry);

    EXPECT_EQ(1, registry->schedules().size());
    EXPECT_EQ(1, registry->schedules(0).windows().size());
    EXPECT_EQ(3, registry->schedules(0).windows(0).machine_ids().size());
    EXPECT_EQ(3, registry->machines().machines().size());
    EXPECT_EQ(
        MachineInfo::DRAINING,
        registry->machines().machines(0).info().mode());

    EXPECT_EQ(
        MachineInfo::DRAINING,
        registry->machines().machines(1).info().mode());

    EXPECT_EQ(
        MachineInfo::DRAINING,
        registry->machines().machines(2).info().mode());

    // Delete one machine from the schedule.
    maintenance::Schedule schedule = createSchedule(
        {createWindow({machine2, machine3}, unavailability)});

    AWAIT_READY(registrar.apply(Owned<RegistryOperation>(
        new UpdateSchedule(schedule))));
  }

  {
    // Check that one machine info is removed.
    Registrar registrar(flags, state);
    Future<Registry> registry = registrar.recover(controller);
    AWAIT_READY(registry);

    EXPECT_EQ(1, registry->schedules().size());
    EXPECT_EQ(1, registry->schedules(0).windows().size());
    EXPECT_EQ(2, registry->schedules(0).windows(0).machine_ids().size());
    EXPECT_EQ(2, registry->machines().machines().size());

    // Delete all machines from the schedule.
    maintenance::Schedule schedule;
    AWAIT_READY(registrar.apply(Owned<RegistryOperation>(
        new UpdateSchedule(schedule))));
  }

  {
    // Check that all statuses are removed.
    Registrar registrar(flags, state);
    Future<Registry> registry = registrar.recover(controller);
    AWAIT_READY(registry);

    EXPECT_EQ(1, registry->schedules().size());
    EXPECT_TRUE(registry->schedules(0).windows().empty());
    EXPECT_TRUE(registry->machines().machines().empty());
  }
}


// Creates a schedule and properly starts maintenance.
TEST_F(RegistrarTest, StartMaintenance)
{
  // Machine definitions used in this test.
  MachineID machine1;
  machine1.set_ip("0.0.0.1");

  MachineID machine2;
  machine2.set_hostname("2");

  MachineID machine3;
  machine3.set_hostname("3");
  machine3.set_ip("0.0.0.3");

  Unavailability unavailability = createUnavailability(Clock::now());

  {
    // Prepare the registrar.
    Registrar registrar(flags, state);
    AWAIT_READY(registrar.recover(controller));

    // Schedule two machines for maintenance.
    maintenance::Schedule schedule = createSchedule(
        {createWindow({machine1, machine2}, unavailability)});

    AWAIT_READY(registrar.apply(Owned<RegistryOperation>(
        new UpdateSchedule(schedule))));

    // Transition machine two into `DOWN` mode.
    RepeatedPtrField<MachineID> machines = createMachineList({machine2});

    AWAIT_READY(registrar.apply(Owned<RegistryOperation>(
        new StartMaintenance(machines))));
  }

  {
    // Check that machine two is down.
    Registrar registrar(flags, state);
    Future<Registry> registry = registrar.recover(controller);
    AWAIT_READY(registry);

    EXPECT_EQ(2, registry->machines().machines().size());
    EXPECT_EQ(
        MachineInfo::DRAINING,
        registry->machines().machines(0).info().mode());

    EXPECT_EQ(
        MachineInfo::DOWN,
        registry->machines().machines(1).info().mode());

    // Schedule three machines for maintenance.
    maintenance::Schedule schedule = createSchedule(
        {createWindow({machine1, machine2, machine3}, unavailability)});

    AWAIT_READY(registrar.apply(Owned<RegistryOperation>(
        new UpdateSchedule(schedule))));

    // Deactivate the two `DRAINING` machines.
    RepeatedPtrField<MachineID> machines =
      createMachineList({machine1, machine3});

    AWAIT_READY(registrar.apply(Owned<RegistryOperation>(
        new StartMaintenance(machines))));
  }

  {
    // Check that all machines are down.
    Registrar registrar(flags, state);
    Future<Registry> registry = registrar.recover(controller);
    AWAIT_READY(registry);

    EXPECT_EQ(3, registry->machines().machines().size());
    EXPECT_EQ(
        MachineInfo::DOWN,
        registry->machines().machines(0).info().mode());

    EXPECT_EQ(
        MachineInfo::DOWN,
        registry->machines().machines(1).info().mode());

    EXPECT_EQ(
        MachineInfo::DOWN,
        registry->machines().machines(2).info().mode());
  }
}


// Creates a schedule and properly starts and stops maintenance.
TEST_F(RegistrarTest, StopMaintenance)
{
  // Machine definitions used in this test.
  MachineID machine1;
  machine1.set_ip("0.0.0.1");

  MachineID machine2;
  machine2.set_hostname("2");

  MachineID machine3;
  machine3.set_hostname("3");
  machine3.set_ip("0.0.0.3");

  Unavailability unavailability = createUnavailability(Clock::now());

  {
    // Prepare the registrar.
    Registrar registrar(flags, state);
    AWAIT_READY(registrar.recover(controller));

    // Schdule three machines for maintenance.
    maintenance::Schedule schedule = createSchedule({
        createWindow({machine1, machine2}, unavailability),
        createWindow({machine3}, unavailability)});

    AWAIT_READY(registrar.apply(Owned<RegistryOperation>(
        new UpdateSchedule(schedule))));

    // Transition machine three into `DOWN` mode.
    RepeatedPtrField<MachineID> machines = createMachineList({machine3});

    AWAIT_READY(registrar.apply(Owned<RegistryOperation>(
        new StartMaintenance(machines))));

    // Transition machine three into `UP` mode.
    AWAIT_READY(registrar.apply(Owned<RegistryOperation>(
        new StopMaintenance(machines))));
  }

  {
    // Check that machine three and the window were removed.
    Registrar registrar(flags, state);
    Future<Registry> registry = registrar.recover(controller);
    AWAIT_READY(registry);

    EXPECT_EQ(1, registry->schedules().size());
    EXPECT_EQ(1, registry->schedules(0).windows().size());
    EXPECT_EQ(2, registry->schedules(0).windows(0).machine_ids().size());
    EXPECT_EQ(2, registry->machines().machines().size());
    EXPECT_EQ(
        MachineInfo::DRAINING,
        registry->machines().machines(0).info().mode());

    EXPECT_EQ(
        MachineInfo::DRAINING,
        registry->machines().machines(1).info().mode());

    // Transition machine one and two into `DOWN` mode.
    RepeatedPtrField<MachineID> machines =
      createMachineList({machine1, machine2});

    AWAIT_READY(registrar.apply(Owned<RegistryOperation>(
        new StartMaintenance(machines))));

    // Transition all machines into `UP` mode.
    AWAIT_READY(registrar.apply(Owned<RegistryOperation>(
        new StopMaintenance(machines))));
  }

  {
    // Check that the schedule is now empty.
    Registrar registrar(flags, state);
    Future<Registry> registry = registrar.recover(controller);
    AWAIT_READY(registry);

    EXPECT_TRUE(registry->schedules().empty());
    EXPECT_TRUE(registry->machines().machines().empty());
  }
}


// Marks a node for draining and checks for the appropriate data.
TEST_F(RegistrarTest, DrainNode)
{
  NodeID notAdmittedID;
  notAdmittedID.set_value("not-admitted");

  {
    // Prepare the registrar.
    Registrar registrar(flags, state);
    AWAIT_READY(registrar.recover(controller));

    // Add a node to be marked by other operations.
    AWAIT_TRUE(registrar.apply(Owned<RegistryOperation>(
        new AdmitNode(node))));

    // Try to mark an unknown node for draining.
    AWAIT_TRUE(registrar.apply(Owned<RegistryOperation>(
        new DrainNode(notAdmittedID, None(), false))));
  }

  {
    // Check that the node is admitted, but has no DrainConfig.
    Registrar registrar(flags, state);
    Future<Registry> registry = registrar.recover(controller);
    AWAIT_READY(registry);

    ASSERT_EQ(1, registry->nodes().nodes().size());
    EXPECT_EQ(node, registry->nodes().nodes(0).info());
    EXPECT_FALSE(registry->nodes().nodes(0).has_drain_info());
    EXPECT_FALSE(registry->nodes().nodes(0).deactivated());

    // No minimum capability should be added when the operation does
    // not mutate anything.
    EXPECT_EQ(0, registry->minimum_capabilities().size());

    // Drain an admitted node.
    AWAIT_TRUE(registrar.apply(Owned<RegistryOperation>(
        new DrainNode(node.id(), None(), true))));
  }

  {
    // Check that node is now marked for draining.
    Registrar registrar(flags, state);
    Future<Registry> registry = registrar.recover(controller);
    AWAIT_READY(registry);

    ASSERT_EQ(1, registry->nodes().nodes().size());
    ASSERT_TRUE(registry->nodes().nodes(0).has_drain_info());
    EXPECT_EQ(DRAINING, registry->nodes().nodes(0).drain_info().state());
    EXPECT_FALSE(registry->nodes().nodes(0)
      .drain_info().config().has_max_grace_period());
    EXPECT_TRUE(registry->nodes().nodes(0).drain_info().config().mark_gone());
    EXPECT_TRUE(registry->nodes().nodes(0).deactivated());

    // Minimum capability should be added now.
    ASSERT_EQ(1, registry->minimum_capabilities().size());
    EXPECT_EQ(
        ControllerInfo_Capability_Type_Name(ControllerInfo::Capability::NODE_DRAINING),
        registry->minimum_capabilities(0).capability());

    // Mark the node unreachable.
    AWAIT_TRUE(registrar.apply(Owned<RegistryOperation>(
        new MarkNodeUnreachable(node, protobuf::getCurrentTime()))));
  }

  {
    // Check that unreachable node retains the draining.
    Registrar registrar(flags, state);
    Future<Registry> registry = registrar.recover(controller);
    AWAIT_READY(registry);

    EXPECT_EQ(0, registry->nodes().nodes().size());
    ASSERT_EQ(1, registry->unreachable().nodes().size());
    ASSERT_TRUE(registry->unreachable().nodes(0).has_drain_info());
    EXPECT_FALSE(
        registry->unreachable().nodes(0)
          .drain_info().config().has_max_grace_period());
    EXPECT_TRUE(
        registry->unreachable().nodes(0).drain_info().config().mark_gone());
    EXPECT_TRUE(registry->unreachable().nodes(0).deactivated());

    // Mark the node reachable.
    AWAIT_TRUE(registrar.apply(Owned<RegistryOperation>(
        new MarkNodeReachable(node))));
  }

  {
    // Check that reachable node retains the draining.
    Registrar registrar(flags, state);
    Future<Registry> registry = registrar.recover(controller);
    AWAIT_READY(registry);

    ASSERT_EQ(1, registry->nodes().nodes().size());
    EXPECT_EQ(0, registry->unreachable().nodes().size());
    ASSERT_TRUE(registry->nodes().nodes(0).has_drain_info());
    EXPECT_FALSE(
        registry->nodes().nodes(0)
          .drain_info().config().has_max_grace_period());
    EXPECT_TRUE(registry->nodes().nodes(0).drain_info().config().mark_gone());
    EXPECT_TRUE(registry->nodes().nodes(0).deactivated());
  }
}


TEST_F(RegistrarTest, MarkNodeDrained)
{
  {
    // Prepare the registrar.
    Registrar registrar(flags, state);
    AWAIT_READY(registrar.recover(controller));

    // Add a node to be marked for draining.
    AWAIT_TRUE(registrar.apply(Owned<RegistryOperation>(
        new AdmitNode(node))));

    // Try to mark a non-draining node as drained. This should fail.
    AWAIT_FALSE(registrar.apply(Owned<RegistryOperation>(
        new MarkNodeDrained(node.id()))));

    // Drain the admitted node.
    AWAIT_TRUE(registrar.apply(Owned<RegistryOperation>(
        new DrainNode(node.id(), None(), true))));
  }

  {
    // Check that node is now marked for draining.
    Registrar registrar(flags, state);
    Future<Registry> registry = registrar.recover(controller);
    AWAIT_READY(registry);

    ASSERT_EQ(1, registry->nodes().nodes().size());
    EXPECT_TRUE(registry->nodes().nodes(0).has_drain_info());
    EXPECT_EQ(DRAINING, registry->nodes().nodes(0).drain_info().state());

    // Transition from draining to drained.
    AWAIT_TRUE(registrar.apply(Owned<RegistryOperation>(
        new MarkNodeDrained(node.id()))));
  }

  {
    // Check that node is now marked drained.
    Registrar registrar(flags, state);
    Future<Registry> registry = registrar.recover(controller);
    AWAIT_READY(registry);

    ASSERT_EQ(1, registry->nodes().nodes().size());
    ASSERT_TRUE(registry->nodes().nodes(0).has_drain_info());
    EXPECT_EQ(DRAINED, registry->nodes().nodes(0).drain_info().state());

    // Try the same sequence of operations for an unreachable node.
    // First remove the node.
    AWAIT_TRUE(registrar.apply(Owned<RegistryOperation>(
        new RemoveNode(node))));

    // Add the node back, anew.
    AWAIT_TRUE(registrar.apply(Owned<RegistryOperation>(
        new AdmitNode(node))));

    // Mark it unreachable.
    AWAIT_TRUE(registrar.apply(Owned<RegistryOperation>(
        new MarkNodeUnreachable(node, protobuf::getCurrentTime()))));

    // Try to mark the node drained prematurely.
    AWAIT_FALSE(registrar.apply(Owned<RegistryOperation>(
        new MarkNodeDrained(node.id()))));

    // Now drain properly.
    AWAIT_TRUE(registrar.apply(Owned<RegistryOperation>(
        new DrainNode(node.id(), None(), true))));

    // And finish draining.
    AWAIT_TRUE(registrar.apply(Owned<RegistryOperation>(
        new MarkNodeDrained(node.id()))));
  }

  {
    // Check that unreachable node is now marked drained.
    Registrar registrar(flags, state);
    Future<Registry> registry = registrar.recover(controller);
    AWAIT_READY(registry);

    ASSERT_EQ(1, registry->unreachable().nodes().size());
    ASSERT_TRUE(registry->unreachable().nodes(0).has_drain_info());
    EXPECT_EQ(DRAINED, registry->unreachable().nodes(0).drain_info().state());
  }
}


TEST_F(RegistrarTest, DeactivateNode)
{
  NodeID notAdmittedID;
  notAdmittedID.set_value("not-admitted");

  {
    // Prepare the registrar.
    Registrar registrar(flags, state);
    AWAIT_READY(registrar.recover(controller));

    // Add a node to be marked by other operations.
    AWAIT_TRUE(registrar.apply(Owned<RegistryOperation>(
        new AdmitNode(node))));

    // Try to mark an unknown node for draining.
    AWAIT_TRUE(registrar.apply(Owned<RegistryOperation>(
        new DeactivateNode(notAdmittedID))));
  }

  {
    // Check that the node is admitted and is not deactivated.
    Registrar registrar(flags, state);
    Future<Registry> registry = registrar.recover(controller);
    AWAIT_READY(registry);

    ASSERT_EQ(1, registry->nodes().nodes().size());
    EXPECT_EQ(node, registry->nodes().nodes(0).info());
    EXPECT_FALSE(registry->nodes().nodes(0).has_drain_info());
    EXPECT_FALSE(registry->nodes().nodes(0).deactivated());

    // No minimum capability should be added when the operation does
    // not mutate anything.
    EXPECT_EQ(0, registry->minimum_capabilities().size());

    // Deactivate the admitted node this time.
    AWAIT_TRUE(registrar.apply(Owned<RegistryOperation>(
        new DeactivateNode(node.id()))));
  }

  {
    // Check that node is now deactivated.
    Registrar registrar(flags, state);
    Future<Registry> registry = registrar.recover(controller);
    AWAIT_READY(registry);

    ASSERT_EQ(1, registry->nodes().nodes().size());
    EXPECT_FALSE(registry->nodes().nodes(0).has_drain_info());
    EXPECT_TRUE(registry->nodes().nodes(0).deactivated());

    // Minimum capability should be added now.
    ASSERT_EQ(1, registry->minimum_capabilities().size());
    EXPECT_EQ(
        ControllerInfo_Capability_Type_Name(ControllerInfo::Capability::NODE_DRAINING),
        registry->minimum_capabilities(0).capability());

    // Mark the node unreachable.
    AWAIT_TRUE(registrar.apply(Owned<RegistryOperation>(
        new MarkNodeUnreachable(node, protobuf::getCurrentTime()))));
  }

  {
    // Check that unreachable node retains the deactivation.
    Registrar registrar(flags, state);
    Future<Registry> registry = registrar.recover(controller);
    AWAIT_READY(registry);

    EXPECT_EQ(0, registry->nodes().nodes().size());
    ASSERT_EQ(1, registry->unreachable().nodes().size());
    EXPECT_FALSE(registry->unreachable().nodes(0).has_drain_info());
    EXPECT_TRUE(registry->unreachable().nodes(0).deactivated());

    // Mark the node reachable.
    AWAIT_TRUE(registrar.apply(Owned<RegistryOperation>(
        new MarkNodeReachable(node))));
  }

  {
    // Check that reachable node retains the deactivation.
    Registrar registrar(flags, state);
    Future<Registry> registry = registrar.recover(controller);
    AWAIT_READY(registry);

    ASSERT_EQ(1, registry->nodes().nodes().size());
    EXPECT_EQ(0, registry->unreachable().nodes().size());
    EXPECT_FALSE(registry->nodes().nodes(0).has_drain_info());
    EXPECT_TRUE(registry->nodes().nodes(0).deactivated());
  }
}


// Checks that reactivating nodes will remove the draining/deactivated
// metadata and the NODE_DRAINING minimum capability correctly.
TEST_F(RegistrarTest, ReactivateNode)
{
  NodeID reachable1;
  reachable1.set_value("reachable1");

  NodeID reachable2;
  reachable2.set_value("reachable2");

  NodeID unreachable1;
  unreachable1.set_value("unreachable1");

  NodeID unreachable2;
  unreachable2.set_value("unreachable2");

  NodeInfo info1;
  info1.set_hostname("localhost");
  info1.mutable_id()->CopyFrom(reachable1);

  NodeInfo info2;
  info2.set_hostname("localhost");
  info2.mutable_id()->CopyFrom(reachable2);

  NodeInfo info3;
  info3.set_hostname("localhost");
  info3.mutable_id()->CopyFrom(unreachable1);

  NodeInfo info4;
  info4.set_hostname("localhost");
  info4.mutable_id()->CopyFrom(unreachable2);

  {
    // Prepare the registrar.
    Registrar registrar(flags, state);
    AWAIT_READY(registrar.recover(controller));

    // Add all the nodes.
    AWAIT_TRUE(registrar.apply(Owned<RegistryOperation>(
        new AdmitNode(info1))));
    AWAIT_TRUE(registrar.apply(Owned<RegistryOperation>(
        new AdmitNode(info2))));
    AWAIT_TRUE(registrar.apply(Owned<RegistryOperation>(
        new AdmitNode(info3))));
    AWAIT_TRUE(registrar.apply(Owned<RegistryOperation>(
        new AdmitNode(info4))));

    // Mark two nodes as unreachable.
    AWAIT_TRUE(registrar.apply(Owned<RegistryOperation>(
        new MarkNodeUnreachable(info3, protobuf::getCurrentTime()))));
    AWAIT_TRUE(registrar.apply(Owned<RegistryOperation>(
        new MarkNodeUnreachable(info4, protobuf::getCurrentTime()))));

    // Two reachable deactivated nodes.
    AWAIT_TRUE(registrar.apply(Owned<RegistryOperation>(
        new DeactivateNode(reachable1))));
    AWAIT_TRUE(registrar.apply(Owned<RegistryOperation>(
        new DeactivateNode(reachable2))));
  }

  {
    // Check for two deactivated nodes and the minimum capability.
    Registrar registrar(flags, state);
    Future<Registry> registry = registrar.recover(controller);
    AWAIT_READY(registry);

    ASSERT_EQ(2, registry->nodes().nodes().size());
    ASSERT_EQ(2, registry->unreachable().nodes().size());

    EXPECT_TRUE(registry->nodes().nodes(0).deactivated());
    EXPECT_TRUE(registry->nodes().nodes(1).deactivated());

    ASSERT_EQ(1, registry->minimum_capabilities().size());
    EXPECT_EQ(
        ControllerInfo_Capability_Type_Name(ControllerInfo::Capability::NODE_DRAINING),
        registry->minimum_capabilities(0).capability());

    // Reactivate one node.
    AWAIT_TRUE(registrar.apply(Owned<RegistryOperation>(
        new ReactivateNode(reachable1))));
  }

  {
    // Check for one deactivated node and
    // that the minimum capability is still present.
    Registrar registrar(flags, state);
    Future<Registry> registry = registrar.recover(controller);
    AWAIT_READY(registry);

    ASSERT_EQ(2, registry->nodes().nodes().size());
    ASSERT_EQ(2, registry->unreachable().nodes().size());

    EXPECT_FALSE(registry->nodes().nodes(0).deactivated());
    EXPECT_TRUE(registry->nodes().nodes(1).deactivated());

    ASSERT_EQ(1, registry->minimum_capabilities().size());
    EXPECT_EQ(
        ControllerInfo_Capability_Type_Name(ControllerInfo::Capability::NODE_DRAINING),
        registry->minimum_capabilities(0).capability());

    // Reactivate the other node.
    AWAIT_TRUE(registrar.apply(Owned<RegistryOperation>(
        new ReactivateNode(reachable2))));
  }

  {
    // Check for one deactivated node and
    // that the minimum capability is still present.
    Registrar registrar(flags, state);
    Future<Registry> registry = registrar.recover(controller);
    AWAIT_READY(registry);

    ASSERT_EQ(2, registry->nodes().nodes().size());
    ASSERT_EQ(2, registry->unreachable().nodes().size());

    EXPECT_FALSE(registry->nodes().nodes(0).deactivated());
    EXPECT_FALSE(registry->nodes().nodes(1).deactivated());

    ASSERT_EQ(0, registry->minimum_capabilities().size());

    // Two unreachable deactivated nodes.
    AWAIT_TRUE(registrar.apply(Owned<RegistryOperation>(
        new DeactivateNode(unreachable1))));
    AWAIT_TRUE(registrar.apply(Owned<RegistryOperation>(
        new DeactivateNode(unreachable2))));

    // Try reactivating a node that is already active.
    // This should not result in a change.
    AWAIT_TRUE(registrar.apply(Owned<RegistryOperation>(
        new ReactivateNode(reachable1))));
  }

  {
    // Again, check for two deactivated nodes and the minimum capability.
    Registrar registrar(flags, state);
    Future<Registry> registry = registrar.recover(controller);
    AWAIT_READY(registry);

    ASSERT_EQ(2, registry->nodes().nodes().size());
    ASSERT_EQ(2, registry->unreachable().nodes().size());

    EXPECT_TRUE(registry->unreachable().nodes(0).deactivated());
    EXPECT_TRUE(registry->unreachable().nodes(1).deactivated());

    ASSERT_EQ(1, registry->minimum_capabilities().size());
    EXPECT_EQ(
        ControllerInfo_Capability_Type_Name(ControllerInfo::Capability::NODE_DRAINING),
        registry->minimum_capabilities(0).capability());

    // Reactivate one. This time, in the opposite order.
    AWAIT_TRUE(registrar.apply(Owned<RegistryOperation>(
        new ReactivateNode(unreachable2))));
  }

  {
    // Should be one deactivated node, with minimum capability still present.
    Registrar registrar(flags, state);
    Future<Registry> registry = registrar.recover(controller);
    AWAIT_READY(registry);

    ASSERT_EQ(2, registry->nodes().nodes().size());
    ASSERT_EQ(2, registry->unreachable().nodes().size());

    EXPECT_TRUE(registry->unreachable().nodes(0).deactivated());
    EXPECT_FALSE(registry->unreachable().nodes(1).deactivated());

    ASSERT_EQ(1, registry->minimum_capabilities().size());
    EXPECT_EQ(
        ControllerInfo_Capability_Type_Name(ControllerInfo::Capability::NODE_DRAINING),
        registry->minimum_capabilities(0).capability());

    // Reactivate the other one.
    AWAIT_TRUE(registrar.apply(Owned<RegistryOperation>(
        new ReactivateNode(unreachable1))));
  }

  {
    // No deactivated nodes, with no minimum capability.
    Registrar registrar(flags, state);
    Future<Registry> registry = registrar.recover(controller);
    AWAIT_READY(registry);

    ASSERT_EQ(2, registry->nodes().nodes().size());
    ASSERT_EQ(2, registry->unreachable().nodes().size());

    EXPECT_FALSE(registry->unreachable().nodes(0).deactivated());
    EXPECT_FALSE(registry->unreachable().nodes(1).deactivated());

    ASSERT_EQ(0, registry->minimum_capabilities().size());

    // Now try deactivate reachable and unreachable together.
    AWAIT_TRUE(registrar.apply(Owned<RegistryOperation>(
        new DeactivateNode(reachable1))));
    AWAIT_TRUE(registrar.apply(Owned<RegistryOperation>(
        new DeactivateNode(unreachable1))));

    // We'll skip a validation step here and reactivate one right away.
    AWAIT_TRUE(registrar.apply(Owned<RegistryOperation>(
        new ReactivateNode(reachable1))));
  }

  {
    // Minimum capability should not be removed if an unreachable node
    // is deactivated.
    Registrar registrar(flags, state);
    Future<Registry> registry = registrar.recover(controller);
    AWAIT_READY(registry);

    ASSERT_EQ(2, registry->nodes().nodes().size());
    ASSERT_EQ(2, registry->unreachable().nodes().size());

    EXPECT_FALSE(registry->nodes().nodes(0).deactivated());
    EXPECT_TRUE(registry->unreachable().nodes(0).deactivated());

    ASSERT_EQ(1, registry->minimum_capabilities().size());
    EXPECT_EQ(
        ControllerInfo_Capability_Type_Name(ControllerInfo::Capability::NODE_DRAINING),
        registry->minimum_capabilities(0).capability());
  }
}


// Tests that adding and updating quotas in the registry works properly.
TEST_F(RegistrarTest, UpdateQuota)
{
  // Helper to construct `QuotaConfig`.
  auto createQuotaConfig = [](const string& role,
                              const string& quantitiesString,
                              const string& limitsString) {
    QuotaConfig config;
    config.set_role(role);

    google::protobuf::Map<string, Value::Scalar> guarantees_;
    ResourceQuantities quantities =
      CHECK_NOTERROR(ResourceQuantities::fromString(quantitiesString));
    foreachpair (const string& name, const Value::Scalar& scalar, quantities) {
      guarantees_[name] = scalar;
    }

    google::protobuf::Map<string, Value::Scalar> limits_;
    ResourceLimits limits =
      CHECK_NOTERROR(ResourceLimits::fromString(limitsString));
    foreachpair (const string& name, const Value::Scalar& scalar, limits) {
      limits_[name] = scalar;
    }

    *config.mutable_guarantees() = std::move(guarantees_);
    *config.mutable_limits() = std::move(limits_);

    return config;
  };

  RepeatedPtrField<QuotaConfig> configs;

  {
    // Initially no quota and minimum capabilities are recorded.

    Registrar registrar(flags, state);
    Future<Registry> registry = registrar.recover(controller);
    AWAIT_READY(registry);

    EXPECT_EQ(0, registry->quota_configs().size());
    EXPECT_EQ(0, registry->minimum_capabilities().size());

    // Store quota for a role with default quota.
    *configs.Add() = createQuotaConfig("role1", "", "");

    AWAIT_TRUE(
        registrar.apply(Owned<RegistryOperation>(new UpdateQuota(configs))));
  }

  {
    Registrar registrar(flags, state);
    Future<Registry> registry = registrar.recover(controller);
    AWAIT_READY(registry);

    // Default quota is not persisted into the registry.
    EXPECT_EQ(0, registry->quota_configs().size());
    EXPECT_EQ(0, registry->minimum_capabilities().size());

    // Update quota for `role1`.
    configs.Clear();
    *configs.Add() =
      createQuotaConfig("role1", "cpus:1;mem:1024", "cpus:2;mem:2048");

    AWAIT_TRUE(
        registrar.apply(Owned<RegistryOperation>(new UpdateQuota(configs))));
  }

  {
    Registrar registrar(flags, state);
    Future<Registry> registry = registrar.recover(controller);
    AWAIT_READY(registry);

    EXPECT_EQ(1, registry->quota_configs().size());

    Try<JSON::Array> expected = JSON::parse<JSON::Array>(
      R"~(
      [
        {
          "guarantees": {
            "cpus": {
              "value": 1
            },
            "mem": {
              "value": 1024
            }
          },
          "limits": {
            "cpus": {
              "value": 2
            },
            "mem": {
              "value": 2048
            }
          },
          "role": "role1"
        }
      ])~");

    EXPECT_EQ(
        CHECK_NOTERROR(expected), JSON::protobuf(registry->quota_configs()));

    // The `QUOTA_V2` capability is added to the registry.
    //
    // TODO(mzhu): This assumes the the registry starts empty which might not
    // be in the future. Just check the presence of `QUOTA_V2`.
    EXPECT_EQ(1, registry->minimum_capabilities().size());
    EXPECT_EQ("QUOTA_V2", registry->minimum_capabilities(0).capability());

    // Update quota for "role2".
    configs.Clear();
    *configs.Add() =
      createQuotaConfig("role2", "cpus:1;mem:1024", "cpus:2;mem:2048");

    AWAIT_TRUE(
        registrar.apply(Owned<RegistryOperation>(new UpdateQuota(configs))));
  }

  {
    Registrar registrar(flags, state);
    Future<Registry> registry = registrar.recover(controller);
    AWAIT_READY(registry);

    // Check that the recovered quotas match those we stored previously.
    // NOTE: We assume quota messages are stored in order they have
    // been added.
    // TODO(alexr): Consider removing dependency on the order.
    Try<JSON::Array> expected = JSON::parse<JSON::Array>(
      R"~(
      [
        {
          "guarantees": {
            "cpus": {
              "value": 1
            },
            "mem": {
              "value": 1024
            }
          },
          "limits": {
            "cpus": {
              "value": 2
            },
            "mem": {
              "value": 2048
            }
          },
          "role": "role1"
        },
        {
          "guarantees": {
            "cpus": {
              "value": 1
            },
            "mem": {
              "value": 1024
            }
          },
          "limits": {
            "cpus": {
              "value": 2
            },
            "mem": {
              "value": 2048
            }
          },
          "role": "role2"
        }
      ])~");

    EXPECT_EQ(
        CHECK_NOTERROR(expected), JSON::protobuf(registry->quota_configs()));

    // TODO(mzhu): This assumes the the registry starts empty which might not
    // be in the future. Just check the presence of `QUOTA_V2`.
    EXPECT_EQ(1, registry->minimum_capabilities().size());
    EXPECT_EQ("QUOTA_V2", registry->minimum_capabilities(0).capability());

    // Change quota for "role1"` and "role2"` in a single call.
    configs.Clear();
    *configs.Add() = createQuotaConfig("role1", "cpus:2", "cpus:4");
    *configs.Add() = createQuotaConfig("role2", "cpus:2", "cpus:4");

    AWAIT_TRUE(
        registrar.apply(Owned<RegistryOperation>(new UpdateQuota(configs))));
  }

  {
    Registrar registrar(flags, state);
    Future<Registry> registry = registrar.recover(controller);
    AWAIT_READY(registry);

    // Check that the recovered quotas match those we stored previously.
    // NOTE: We assume quota messages are stored in order they have
    // been added.
    // TODO(alexr): Consider removing dependency on the order.
    Try<JSON::Array> expected = JSON::parse<JSON::Array>(
      R"~(
      [
        {
          "guarantees": {
            "cpus": {
              "value": 2
            }
          },
          "limits": {
            "cpus": {
              "value": 4
            }
          },
          "role": "role1"
        },
        {
          "guarantees": {
            "cpus": {
              "value": 2
            }
          },
          "limits": {
            "cpus": {
              "value": 4
            }
          },
          "role": "role2"
        }
      ])~");

    EXPECT_EQ(
        CHECK_NOTERROR(expected), JSON::protobuf(registry->quota_configs()));

    // TODO(mzhu): This assumes the the registry starts empty which might not
    // be in the future. Just check the presence of `QUOTA_V2`.
    EXPECT_EQ(1, registry->minimum_capabilities().size());
    EXPECT_EQ("QUOTA_V2", registry->minimum_capabilities(0).capability());

    // Reset "role2"` quota to default.
    configs.Clear();
    *configs.Add() = createQuotaConfig("role2", "", "");

    AWAIT_TRUE(
        registrar.apply(Owned<RegistryOperation>(new UpdateQuota(configs))));
  }

  {
    Registrar registrar(flags, state);
    Future<Registry> registry = registrar.recover(controller);
    AWAIT_READY(registry);

    configs.Clear();
    *configs.Add() = createQuotaConfig("role1", "cpus:2", "cpus:4");

    Try<JSON::Array> expected = JSON::parse<JSON::Array>(
      R"~(
      [
        {
          "guarantees": {
            "cpus": {
              "value": 2
            }
          },
          "limits": {
            "cpus": {
              "value": 4
            }
          },
          "role": "role1"
        }
      ])~");

    EXPECT_EQ(
        CHECK_NOTERROR(expected), JSON::protobuf(registry->quota_configs()));

    // TODO(mzhu): This assumes the the registry starts empty which might not
    // be in the future. Just check the presence of `QUOTA_V2`.
    EXPECT_EQ(1, registry->minimum_capabilities().size());
    EXPECT_EQ("QUOTA_V2", registry->minimum_capabilities(0).capability());

    // Reset "role1"` quota to default.
    configs.Clear();
    *configs.Add() = createQuotaConfig("role1", "", "");

    AWAIT_TRUE(
        registrar.apply(Owned<RegistryOperation>(new UpdateQuota(configs))));
  }

  {
    Registrar registrar(flags, state);
    Future<Registry> registry = registrar.recover(controller);
    AWAIT_READY(registry);

    EXPECT_EQ(0, registry->quota_configs().size());
    // The `QUOTA_V2` capability is removed because `quota_configs` is empty.
    EXPECT_EQ(0, registry->minimum_capabilities().size());
  }
}


// Tests that updating quota with an invalid config fails.
TEST_F(RegistrarTest, UpdateQuotaInvalid)
{
  QuotaConfig config;
  config.set_role("role1");

  auto resourceMap = [](const vector<pair<string, double>>& vector)
    -> Map<string, Value::Scalar> {
    Map<string, Value::Scalar> result;

    foreachpair (const string& name, double value, vector) {
      Value::Scalar scalar;
      scalar.set_value(value);
      result[name] = scalar;
    }

    return result;
  };

  // The quota endpoint only allows memory / disk up to
  // 1 exabyte (in megabytes) or 1 trillion cores/ports/other.
  // For this test, we just check 1 invalid case via mem.
  double largestMegabytes = 1024.0 * 1024.0 * 1024.0 * 1024.0;

  *config.mutable_limits() = resourceMap({{"mem", largestMegabytes + 1.0}});

  Registrar registrar(flags, state);
  Future<Registry> registry = registrar.recover(controller);
  AWAIT_READY(registry);

  EXPECT_EQ(0, registry->quota_configs().size());
  EXPECT_EQ(0, registry->minimum_capabilities().size());

  // Store quota for a role with default quota.
  RepeatedPtrField<QuotaConfig> configs;
  *configs.Add() = config;

  AWAIT_FALSE(
      registrar.apply(Owned<RegistryOperation>(new UpdateQuota(configs))));
}


// Tests that updating weights in the registry works properly.
TEST_F(RegistrarTest, UpdateWeights)
{
  const string ROLE1 = "role1";
  double WEIGHT1 = 2.0;
  double UPDATED_WEIGHT1 = 1.0;

  const string ROLE2 = "role2";
  double WEIGHT2 = 3.5;

  {
    // Prepare the registrar.
    Registrar registrar(flags, state);
    Future<Registry> registry = registrar.recover(controller);
    AWAIT_READY(registry);

    ASSERT_TRUE(registry->weights().empty());

    // Store the weight for 'ROLE1' previously without weight.
    hashmap<string, double> weights;
    weights[ROLE1] = WEIGHT1;
    vector<WeightInfo> weightInfos = getWeightInfos(weights);

    AWAIT_TRUE(registrar.apply(Owned<RegistryOperation>(
        new UpdateWeights(weightInfos))));
  }

  {
    Registrar registrar(flags, state);
    Future<Registry> registry = registrar.recover(controller);
    AWAIT_READY(registry);

    // Check that the recovered weights matches the weights we stored
    // previously.
    ASSERT_EQ(1, registry->weights_size());
    EXPECT_EQ(ROLE1, registry->weights(0).info().role());
    ASSERT_EQ(WEIGHT1, registry->weights(0).info().weight());

    // Change weight for 'ROLE1', and store the weight for 'ROLE2'.
    hashmap<string, double> weights;
    weights[ROLE1] = UPDATED_WEIGHT1;
    weights[ROLE2] = WEIGHT2;
    vector<WeightInfo> weightInfos = getWeightInfos(weights);

    AWAIT_TRUE(registrar.apply(Owned<RegistryOperation>(
        new UpdateWeights(weightInfos))));
  }

  {
    Registrar registrar(flags, state);
    Future<Registry> registry = registrar.recover(controller);
    AWAIT_READY(registry);

    // Check that the recovered weights matches the weights we updated
    // previously.
    ASSERT_EQ(2, registry->weights_size());
    EXPECT_EQ(ROLE1, registry->weights(0).info().role());
    ASSERT_EQ(UPDATED_WEIGHT1, registry->weights(0).info().weight());
    EXPECT_EQ(ROLE2, registry->weights(1).info().role());
    ASSERT_EQ(WEIGHT2, registry->weights(1).info().weight());
  }
}


TEST_F(RegistrarTest, Bootstrap)
{
  // Run 1 simulates the reregistration of a node that is not present
  // in the registry.
  {
    Registrar registrar(flags, state);
    AWAIT_READY(registrar.recover(controller));

    AWAIT_TRUE(registrar.apply(Owned<RegistryOperation>(
        new MarkNodeReachable(node))));
  }

  // Run 2 should see the node.
  {
    Registrar registrar(flags, state);

    Future<Registry> registry = registrar.recover(controller);

    AWAIT_READY(registry);

    ASSERT_EQ(1, registry->nodes().nodes().size());
    EXPECT_EQ(node, registry->nodes().nodes(0).info());
  }
}


class MockStorage : public Storage
{
public:
  MOCK_METHOD1(get, Future<Option<Entry>>(const string&));
  MOCK_METHOD2(set, Future<bool>(const Entry&, const id::UUID&));
  MOCK_METHOD1(expunge, Future<bool>(const Entry&));
  MOCK_METHOD0(names, Future<std::set<string>>());
};


TEST_F(RegistrarTest, FetchTimeout)
{
  Clock::pause();

  MockStorage storage;
  State state(&storage);

  Future<Nothing> get;
  EXPECT_CALL(storage, get(_))
    .WillOnce(DoAll(FutureSatisfy(&get),
                    Return(Future<Option<Entry>>())));

  Registrar registrar(flags, &state);

  Future<Registry> recover = registrar.recover(controller);

  AWAIT_READY(get);

  Clock::advance(flags.registry_fetch_timeout);

  AWAIT_FAILED(recover);

  Clock::resume();

  // Ensure the registrar fails subsequent operations.
  AWAIT_FAILED(registrar.apply(Owned<RegistryOperation>(
      new AdmitNode(node))));
}


TEST_F(RegistrarTest, StoreTimeout)
{
  Clock::pause();

  MockStorage storage;
  State state(&storage);

  Registrar registrar(flags, &state);

  EXPECT_CALL(storage, get(_))
    .WillOnce(Return(None()));

  Future<Nothing> set;
  EXPECT_CALL(storage, set(_, _))
    .WillOnce(DoAll(FutureSatisfy(&set),
                    Return(Future<bool>())));

  Future<Registry> recover = registrar.recover(controller);

  AWAIT_READY(set);

  Clock::advance(flags.registry_store_timeout);

  AWAIT_FAILED(recover);

  Clock::resume();

  // Ensure the registrar fails subsequent operations.
  AWAIT_FAILED(registrar.apply(Owned<RegistryOperation>(
      new AdmitNode(node))));
}


TEST_F(RegistrarTest, Abort)
{
  MockStorage storage;
  State state(&storage);

  Registrar registrar(flags, &state);

  EXPECT_CALL(storage, get(_))
    .WillOnce(Return(None()));

  EXPECT_CALL(storage, set(_, _))
    .WillOnce(Return(Future<bool>(true)))              // Recovery.
    .WillOnce(Return(Future<bool>::failed("failure"))) // Failure.
    .WillRepeatedly(Return(Future<bool>(true)));       // Success.

  AWAIT_READY(registrar.recover(controller));

  // Storage failure.
  AWAIT_FAILED(registrar.apply(Owned<RegistryOperation>(
      new AdmitNode(node))));

  // The registrar should now be aborted!
  AWAIT_FAILED(registrar.apply(Owned<RegistryOperation>(
      new AdmitNode(node))));
}


// Tests that requests to the '/registry' endpoint are authenticated when HTTP
// authentication is enabled.
TEST_F(RegistrarTest, Authentication)
{
  const string AUTHENTICATION_REALM = "realm";

  Credentials credentials;
  credentials.add_credentials()->CopyFrom(DEFAULT_CREDENTIAL);

  Try<authentication::Authenticator*> authenticator =
    BasicAuthenticatorFactory::create(AUTHENTICATION_REALM, credentials);

  ASSERT_SOME(authenticator);

  AWAIT_READY(authentication::setAuthenticator(
      AUTHENTICATION_REALM,
      Owned<authentication::Authenticator>(authenticator.get())));

  Registrar registrar(flags, state, AUTHENTICATION_REALM);

  // Requests without credentials should be rejected.
  Future<Response> response = process::http::get(registrar.pid(), "registry");
  AWAIT_EXPECT_RESPONSE_STATUS_EQ(Unauthorized({}).status, response);

  Credential badCredential;
  badCredential.set_principal("bad-principal");
  badCredential.set_secret("bad-secret");

  // Requests with bad credentials should be rejected.
  response = process::http::get(
      registrar.pid(),
      "registry",
      None(),
      createBasicAuthHeaders(badCredential));
  AWAIT_EXPECT_RESPONSE_STATUS_EQ(Unauthorized({}).status, response);

  // Requests with good credentials should be permitted.
  response = process::http::get(
      registrar.pid(),
      "registry",
      None(),
      createBasicAuthHeaders(DEFAULT_CREDENTIAL));
  AWAIT_EXPECT_RESPONSE_STATUS_EQ(OK().status, response);

  AWAIT_READY(authentication::unsetAuthenticator(AUTHENTICATION_REALM));
}


class Registrar_BENCHMARK_Test
  : public RegistrarTestBase,
    public WithParamInterface<size_t> {};


// The Registrar benchmark tests are parameterized by the number of nodes.
INSTANTIATE_TEST_CASE_P(
    NodeCount,
    Registrar_BENCHMARK_Test,
    ::testing::Values(10000U, 20000U, 30000U, 50000U));


TEST_P(Registrar_BENCHMARK_Test, Performance)
{
  Registrar registrar(flags, state);
  AWAIT_READY(registrar.recover(controller));

  Attributes attributes = Attributes::parse("foo:bar;baz:quux");
  Resources resources =
    Resources::parse("cpus(*):1.0;mem(*):512;disk(*):2048").get();

  size_t nodeCount = GetParam();

  // Create nodes.
  vector<NodeInfo> infos;
  for (size_t i = 0; i < nodeCount; ++i) {
    // Simulate real node information.
    NodeInfo info;
    info.set_hostname("localhost");
    info.mutable_id()->set_value(
        string("201310101658-2280333834-5050-48574-") + stringify(i));
    info.mutable_resources()->MergeFrom(resources);
    info.mutable_attributes()->MergeFrom(attributes);
    infos.push_back(info);
  }

  // Admit nodes.
  Stopwatch watch;
  watch.start();
  Future<bool> result;
  foreach (const NodeInfo& info, infos) {
    result = registrar.apply(Owned<RegistryOperation>(new AdmitNode(info)));
  }
  AWAIT_READY_FOR(result, Minutes(5));
  LOG(INFO) << "Admitted " << nodeCount << " nodes in " << watch.elapsed();

  // Shuffle the nodes so we are readmitting them in random order (
  // same as in production).
  std::shuffle(infos.begin(), infos.end(), std::mt19937(std::random_device{}()));

  // Mark nodes reachable again. This simulates the controller failing
  // over, and then the previously registered nodes reregistering
  // with the new controller.
  watch.start();
  foreach (const NodeInfo& info, infos) {
    result = registrar.apply(Owned<RegistryOperation>(
        new MarkNodeReachable(info)));
  }
  AWAIT_READY_FOR(result, Minutes(5));
  LOG(INFO) << "Marked " << nodeCount
            << " nodes reachable in " << watch.elapsed();

  // Recover nodes.
  Registrar registrar2(flags, state);
  watch.start();
  ControllerInfo info;
  info.set_id("controller");
  info.set_ip(10000000);
  info.set_port(5050);
  Future<Registry> registry = registrar2.recover(info);
  AWAIT_READY(registry);
  LOG(INFO) << "Recovered " << nodeCount << " nodes ("
            << Bytes(registry->ByteSize()) << ") in " << watch.elapsed();

  // Shuffle the nodes so we are removing them in random order (same
  // as in production).
  std::shuffle(infos.begin(), infos.end(), std::mt19937(std::random_device{}()));

  // Remove nodes.
  watch.start();
  foreach (const NodeInfo& info, infos) {
    result = registrar2.apply(Owned<RegistryOperation>(new RemoveNode(info)));
  }
  AWAIT_READY_FOR(result, Minutes(5));
  cout << "Removed " << nodeCount << " nodes in " << watch.elapsed() << endl;
}


// Test the performance of marking all registered nodes unreachable,
// then marking them reachable again. This might occur if there is a
// network partition and then the partition heals.
TEST_P(Registrar_BENCHMARK_Test, MarkUnreachableThenReachable)
{
  Registrar registrar(flags, state);
  AWAIT_READY(registrar.recover(controller));

  Attributes attributes = Attributes::parse("foo:bar;baz:quux");
  Resources resources =
    Resources::parse("cpus(*):1.0;mem(*):512;disk(*):2048").get();

  size_t nodeCount = GetParam();

  // Create nodes.
  vector<NodeInfo> infos;
  for (size_t i = 0; i < nodeCount; ++i) {
    // Simulate real node information.
    NodeInfo info;
    info.set_hostname("localhost");
    info.mutable_id()->set_value(
        string("201310101658-2280333834-5050-48574-") + stringify(i));
    info.mutable_resources()->MergeFrom(resources);
    info.mutable_attributes()->MergeFrom(attributes);
    infos.push_back(info);
  }

  // Admit nodes.
  Stopwatch watch;
  watch.start();
  Future<bool> result;
  foreach (const NodeInfo& info, infos) {
    result = registrar.apply(Owned<RegistryOperation>(new AdmitNode(info)));
  }
  AWAIT_READY_FOR(result, Minutes(5));
  LOG(INFO) << "Admitted " << nodeCount << " nodes in " << watch.elapsed();

  // Shuffle the nodes so that we mark them unreachable in random
  // order (same as in production).
  std::shuffle(infos.begin(), infos.end(), std::mt19937(std::random_device{}()));

  // Mark all nodes unreachable.
  TimeInfo unreachableTime = protobuf::getCurrentTime();

  watch.start();
  foreach (const NodeInfo& info, infos) {
    result = registrar.apply(Owned<RegistryOperation>(
        new MarkNodeUnreachable(info, unreachableTime)));
  }
  AWAIT_READY_FOR(result, Minutes(5));
  cout << "Marked " << nodeCount << " nodes unreachable in "
       << watch.elapsed() << endl;

  // Shuffles the nodes again so that we mark them reachable in
  // random order (same as in production).
  std::shuffle(infos.begin(), infos.end(), std::mt19937(std::random_device{}()));

  // Mark all nodes reachable.
  watch.start();
  foreach (const NodeInfo& info, infos) {
    result = registrar.apply(Owned<RegistryOperation>(
        new MarkNodeReachable(info)));
  }
  AWAIT_READY_FOR(result, Minutes(5));
  cout << "Marked " << nodeCount << " nodes reachable in "
       << watch.elapsed() << endl;
}


// Test the performance of garbage collecting a large portion of the
// unreachable list in a single operation. We use a fixed percentage
// at the moment (50%).
TEST_P(Registrar_BENCHMARK_Test, GcManyNodes)
{
  Registrar registrar(flags, state);
  AWAIT_READY(registrar.recover(controller));

  Attributes attributes = Attributes::parse("foo:bar;baz:quux");
  Resources resources =
    Resources::parse("cpus(*):1.0;mem(*):512;disk(*):2048").get();

  size_t nodeCount = GetParam();

  // Create nodes.
  vector<NodeInfo> infos;
  for (size_t i = 0; i < nodeCount; ++i) {
    // Simulate real node information.
    NodeInfo info;
    info.set_hostname("localhost");
    info.mutable_id()->set_value(
        string("201310101658-2280333834-5050-48574-") + stringify(i));
    info.mutable_resources()->MergeFrom(resources);
    info.mutable_attributes()->MergeFrom(attributes);
    infos.push_back(info);
  }

  // Admit nodes.
  Stopwatch watch;
  watch.start();
  Future<bool> result;
  foreach (const NodeInfo& info, infos) {
    result = registrar.apply(Owned<RegistryOperation>(new AdmitNode(info)));
  }
  AWAIT_READY_FOR(result, Minutes(5));
  LOG(INFO) << "Admitted " << nodeCount << " nodes in " << watch.elapsed();

  // Shuffle the nodes so that we mark them unreachable in random
  // order (same as in production).
  std::shuffle(infos.begin(), infos.end(), std::mt19937(std::random_device{}()));

  // Mark all nodes unreachable.
  TimeInfo unreachableTime = protobuf::getCurrentTime();

  watch.start();
  foreach (const NodeInfo& info, infos) {
    result = registrar.apply(Owned<RegistryOperation>(
        new MarkNodeUnreachable(info, unreachableTime)));
  }
  AWAIT_READY_FOR(result, Minutes(5));
  LOG(INFO) << "Marked " << nodeCount << " nodes unreachable in "
            << watch.elapsed() << endl;

  // Prepare to GC the first half of the unreachable list.
  hashset<NodeID> toRemove;
  for (size_t i = 0; (i * 2) < nodeCount; i++) {
    const NodeInfo& info = infos[i];
    toRemove.insert(info.id());
  }

  // Do GC.
  watch.start();
  result = registrar.apply(Owned<RegistryOperation>(
      new Prune(toRemove, hashset<NodeID>())));

  AWAIT_READY_FOR(result, Minutes(5));
  cout << "Garbage collected " << toRemove.size() << " nodes in "
       << watch.elapsed() << endl;
}

} // namespace tests {
} // namespace internal {
} // namespace medios {
