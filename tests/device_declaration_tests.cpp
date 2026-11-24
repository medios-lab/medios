// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

// The device-declaration seam: isolators declare device access in their
// launch info, the containerizer collects and validates the declarations
// after every isolator has prepared, and on Linux grants them through the
// node's device manager. The collection and validation are platform
// neutral; the conversion of a path to a cgroup entry needs a device node;
// the grant itself needs cgroup v2 and root and runs only there.

#include <string>
#include <vector>

#include <gtest/gtest.h>

#include <medios/medios.hpp>

#include <medios/node/containerizer.hpp>

#include <stout/gtest.hpp>
#include <stout/duration.hpp>
#include <stout/os.hpp>
#include <stout/option.hpp>
#include <stout/strings.hpp>
#include <stout/try.hpp>

#include "node/containerizer/medios/containerizer.hpp"

#ifdef __linux__
#include "node/containerizer/device_manager/device_manager.hpp"
#endif

#include <process/future.hpp>
#include <process/gmock.hpp>
#include <process/gtest.hpp>
#include <process/owned.hpp>

#include "node/flags.hpp"

#include "tests/cluster.hpp"
#include "tests/medios.hpp"

using medios::controller::detector::ControllerDetector;
using medios::internal::node::MediosContainerizer;
using medios::node::ContainerLaunchInfo;

using process::Future;
using process::Owned;

using testing::_;
using testing::Return;

using std::string;
using std::vector;

namespace medios {
namespace internal {
namespace tests {

static DeviceAccess declare(const string& path, bool read, bool write)
{
  DeviceAccess access;
  access.mutable_device()->set_path(path);
  access.mutable_access()->set_read(read);
  access.mutable_access()->set_write(write);
  return access;
}


// Declarations from several isolators combine; an isolator that returned
// nothing contributes nothing.
TEST(DeviceDeclarationTest, CollectsAcrossIsolators)
{
  ContainerLaunchInfo first;
  first.add_devices()->CopyFrom(declare("/dev/null", true, false));

  ContainerLaunchInfo second;
  second.add_devices()->CopyFrom(declare("/dev/zero", true, true));
  second.add_devices()->CopyFrom(declare("/dev/urandom", true, false));

  vector<Option<ContainerLaunchInfo>> launchInfos = {first, None(), second};

  Try<vector<DeviceAccess>> devices =
    node::collectDeclaredDevices(launchInfos);

  ASSERT_SOME(devices);
  ASSERT_EQ(3u, devices->size());
  EXPECT_EQ("/dev/null", devices->at(0).device().path());
  EXPECT_EQ("/dev/zero", devices->at(1).device().path());
  EXPECT_EQ("/dev/urandom", devices->at(2).device().path());
}


// The same device declared by two isolators with the same access is one
// grant, not two; a different access to the same path is a second entry.
TEST(DeviceDeclarationTest, IdenticalDeclarationsCollapse)
{
  ContainerLaunchInfo first;
  first.add_devices()->CopyFrom(declare("/dev/nvidiactl", true, true));

  ContainerLaunchInfo second;
  second.add_devices()->CopyFrom(declare("/dev/nvidiactl", true, true));
  second.add_devices()->CopyFrom(declare("/dev/nvidiactl", true, false));

  Try<vector<DeviceAccess>> devices =
    node::collectDeclaredDevices({first, second});

  ASSERT_SOME(devices);
  ASSERT_EQ(2u, devices->size());
  EXPECT_TRUE(devices->at(0).access().write());
  EXPECT_FALSE(devices->at(1).access().write());
}


TEST(DeviceDeclarationTest, NoDeclarationsIsEmptyNotAnError)
{
  ContainerLaunchInfo plain;
  plain.mutable_environment();

  Try<vector<DeviceAccess>> devices =
    node::collectDeclaredDevices({plain, None()});

  ASSERT_SOME(devices);
  EXPECT_TRUE(devices->empty());
}


// A declaration without a path cannot name a device; one without any
// access bit grants nothing and is almost certainly a mistake. Both are
// refused rather than dropped, so a broken isolator fails its launch.
TEST(DeviceDeclarationTest, RefusesMissingPathAndNoAccess)
{
  ContainerLaunchInfo noPath;
  noPath.add_devices()->mutable_access()->set_read(true);

  Try<vector<DeviceAccess>> devices = node::collectDeclaredDevices({noPath});
  ASSERT_ERROR(devices);
  EXPECT_TRUE(strings::contains(devices.error(), "no path")) << devices.error();

  ContainerLaunchInfo noAccess;
  noAccess.add_devices()->CopyFrom(declare("/dev/null", false, false));

  devices = node::collectDeclaredDevices({noAccess});
  ASSERT_ERROR(devices);
  EXPECT_TRUE(strings::contains(devices.error(), "grants no access"))
    << devices.error();
}


#ifdef __linux__
// /dev/null is character device 1:3 on every Linux system.
TEST(DeviceDeclarationTest, EntryFromDeviceAccessReadsTheDeviceNode)
{
  Try<cgroups::devices::Entry> entry =
    node::DeviceManager::entryFromDeviceAccess(declare("/dev/null", true, true));

  ASSERT_SOME(entry);
  EXPECT_EQ(cgroups::devices::Entry::Selector::Type::CHARACTER,
            entry->selector.type);
  ASSERT_SOME(entry->selector.major);
  ASSERT_SOME(entry->selector.minor);
  EXPECT_EQ(1u, entry->selector.major.get());
  EXPECT_EQ(3u, entry->selector.minor.get());
  EXPECT_TRUE(entry->access.read);
  EXPECT_TRUE(entry->access.write);
  EXPECT_FALSE(entry->access.mknod);
}


TEST(DeviceDeclarationTest, EntryFromDeviceAccessRefusesNonDevices)
{
  Try<string> file = os::mktemp();
  ASSERT_SOME(file);

  Try<cgroups::devices::Entry> entry =
    node::DeviceManager::entryFromDeviceAccess(declare(file.get(), true, false));

  // Refused at the device-id read ("not a special file") or at the mode
  // check; either way the refusal names the path.
  ASSERT_ERROR(entry);
  EXPECT_TRUE(strings::contains(entry.error(), file.get())) << entry.error();

  entry = node::DeviceManager::entryFromDeviceAccess(
      declare("/dev/definitely-not-here", true, false));
  ASSERT_ERROR(entry);

  os::rm(file.get());
}


// End to end, on a cgroup v2 host as root: a node running the Linux
// launcher, the cgroups devices controller and the device-declaring test
// isolator launches a command task that opens the declared device. The
// control is the same node without the declaring isolator: the default
// allowlist does not contain the device, so the same task fails. /dev/fuse
// (character 10:229) is present on every host with FUSE and is not in the
// default allowlist, which is what makes the control a control.
// The containerizer fixture, not the plain one: on a cgroup v2 host as
// root its flags point the node at the test cgroup root and its setup
// creates that root with every controller enabled. The plain fixture
// leaves the default root, which nothing creates, and the cgroup isolator
// refuses the first launch.
class DeviceDeclarationNodeTest
  : public ContainerizerTest<node::MediosContainerizer>
{
protected:
  // Runs one command task that opens `device` on a node with `flags` and
  // returns its terminal state. Plain returns rather than the await
  // macros, which only work in a void test body.
  Try<TaskState> runOpenDeviceTask(
      const node::Flags& flags,
      const string& device);
};


Try<TaskState> DeviceDeclarationNodeTest::runOpenDeviceTask(
    const node::Flags& flags,
    const string& device)
{
  Try<Owned<cluster::Controller>> controller = StartController();
  if (controller.isError()) {
    return Error("controller: " + controller.error());
  }

  Try<MediosContainerizer*> _containerizer =
    MediosContainerizer::create(flags, false);
  if (_containerizer.isError()) {
    return Error("containerizer: " + _containerizer.error());
  }
  Owned<MediosContainerizer> containerizer(_containerizer.get());

  Owned<ControllerDetector> detector = controller.get()->createDetector();
  Try<Owned<cluster::Node>> node =
    StartNode(detector.get(), containerizer.get(), flags);
  if (node.isError()) {
    return Error("node: " + node.error());
  }

  MockScheduler sched;
  MediosSchedulerDriver driver(
      &sched, DEFAULT_FRAMEWORK_INFO, controller.get()->pid, DEFAULT_CREDENTIAL);

  EXPECT_CALL(sched, registered(&driver, _, _));

  Future<vector<Offer>> offers;
  EXPECT_CALL(sched, resourceOffers(&driver, _))
    .WillOnce(FutureArg<1>(&offers))
    .WillRepeatedly(Return());

  driver.start();

  if (!offers.await(Seconds(60)) || !offers.isReady() || offers->empty()) {
    return Error("no offers within 60 seconds");
  }

  TaskInfo task = createTask(
      offers.get()[0], "sh -c 'exec 3<" + device + "'");

  Future<TaskStatus> starting;
  Future<TaskStatus> running;
  Future<TaskStatus> terminal;
  EXPECT_CALL(sched, statusUpdate(&driver, _))
    .WillOnce(FutureArg<1>(&starting))
    .WillOnce(FutureArg<1>(&running))
    .WillOnce(FutureArg<1>(&terminal));

  driver.launchTasks(offers.get()[0].id(), {task});

  if (!terminal.await(Seconds(120)) || !terminal.isReady()) {
    return Error("no terminal status within 120 seconds");
  }
  TaskState state = terminal->state();

  driver.stop();
  driver.join();

  return state;
}


TEST_F(DeviceDeclarationNodeTest, ROOT_CGROUPS2_DeclaredDeviceIsGranted)
{
  if (!os::exists("/dev/fuse")) {
    return;
  }

  os::setenv("MEDIOS_TEST_DEVICE_PATH", "/dev/fuse");

  node::Flags flags = CreateNodeFlags();
  flags.launcher = "linux";
  flags.isolation = "cgroups/devices,org_apache_medios_TestDeviceIsolator";

  Try<TaskState> state = runOpenDeviceTask(flags, "/dev/fuse");
  ASSERT_SOME(state);
  EXPECT_EQ(TASK_FINISHED, state.get());

  os::unsetenv("MEDIOS_TEST_DEVICE_PATH");
}


TEST_F(DeviceDeclarationNodeTest, ROOT_CGROUPS2_UndeclaredDeviceIsDenied)
{
  if (!os::exists("/dev/fuse")) {
    return;
  }

  node::Flags flags = CreateNodeFlags();
  flags.launcher = "linux";
  flags.isolation = "cgroups/devices";

  Try<TaskState> state = runOpenDeviceTask(flags, "/dev/fuse");
  ASSERT_SOME(state);
  EXPECT_EQ(TASK_FAILED, state.get());
}
#endif // __linux__

} // namespace tests {
} // namespace internal {
} // namespace medios {
