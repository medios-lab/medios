// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

// The GPU isolator module (src/modules/gpu) is a shared library the node
// loads at runtime. These tests dlopen it through the module manager, the
// same path `--modules` takes, and start a node that selects it by name.
// MEDIOS_GPU_MODULE_PATH is the built library, supplied by CMake.

#ifdef __linux__
#include <sys/mount.h>
#endif // __linux__

#include <string>

#include <gmock/gmock.h>

#include <gtest/gtest.h>

#include <medios/medios.hpp>
#include <medios/module.hpp>
#include <medios/resources.hpp>

#include <medios/module/isolator.hpp>
#include <medios/node/isolator.hpp>

#include <process/future.hpp>
#include <process/gmock.hpp>
#include <process/gtest.hpp>
#include <process/owned.hpp>

#include <stout/gtest.hpp>
#include <stout/hashmap.hpp>
#include <stout/os.hpp>
#include <stout/path.hpp>
#include <set>
#include <functional>
#include <stout/nothing.hpp>
#include <stout/strings.hpp>
#include <stout/try.hpp>

#include "messages/messages.hpp"
#include "modules/gpu/checkpoint.hpp"
#include "modules/gpu/delivery.hpp"
#include "modules/gpu/discovery.hpp"
#include "modules/gpu/nvml.hpp"
#include "modules/gpu/units.hpp"
#include "module/manager.hpp"
#include "node/flags.hpp"

#include "tests/cluster.hpp"
#include "tests/medios.hpp"

using medios::controller::detector::ControllerDetector;
using medios::internal::node::Flags;
using medios::modules::ModuleManager;
using medios::node::Isolator;

using process::Future;
using process::Owned;

using std::string;
using std::vector;

using testing::_;

namespace medios {
namespace internal {
namespace tests {

// The isolator names the GPU module library registers. The assignment
// isolator needs NVML and a units file but no cgroup hierarchy, so a case that
// tests assignment loads it alone. A case that tests device declarations loads
// the device isolator as well.
static const string GPU_MODULE_NAME = "com_medioslab_medios_gpu_assign";
static const string GPU_DEVICES_MODULE_NAME = "com_medioslab_medios_gpu_devices";


// A units file for whatever this host actually has, written into the test's
// sandbox. None when there is no driver or no device: the module requires a
// declaration it can honour, so loading it at all needs a real GPU, and a
// host without one has nothing to declare.
//
// Built from discovery rather than from a fixed UUID because the identity is
// the host's. That makes these two tests a GPU-host check; every other test in
// this file runs anywhere.
static Option<string> writeUnitsFileForThisHost()
{
  std::unique_ptr<medios::modules::gpu::DeviceSource> source =
    medios::modules::gpu::nvmlDeviceSource();

  Try<vector<medios::modules::gpu::Device>> devices =
    medios::modules::gpu::discover(*source);

  if (devices.isError() || devices->empty()) {
    return None();
  }

  const string path = path::join(os::getcwd(), "units.json");

  Try<Nothing> written = os::write(
      path,
      "{\"units\": [{\"resource\": \"gpus\", \"per_device\": 1, "
      "\"devices\": [\"" + devices->at(0).uuid + "\"]}]}");

  if (written.isError()) {
    return None();
  }

  return path;
}


// Builds the manifest that medios-node reads from --modules: one library
// entry for the GPU module, with one module entry for each name in `names`.
// Every module entry gets a `units` parameter set to `units`. An operator
// gives both GPU isolators the same units file, because the device isolator
// looks up each assigned UUID in its own copy of the file.
static Modules gpuModules(
    const string& units,
    const vector<string>& names = {GPU_MODULE_NAME})
{
  Modules modules;
  Modules::Library* library = modules.add_libraries();
  library->set_file(MEDIOS_GPU_MODULE_PATH);

  foreach (const string& name, names) {
    Modules::Library::Module* module = library->add_modules();
    module->set_name(name);

    Parameter* parameter = module->add_parameters();
    parameter->set_key("units");
    parameter->set_value(units);
  }

  return modules;
}


// Every case below declares its own units, and each declaration arrives through
// `ModuleManager::load`, because a node building its containerizer calls
// `create()` with no parameters of its own and so reads the ones the load
// recorded (`src/node/containerizer/medios/containerizer.cpp:403`, the only
// caller). A second load of the same module name with different parameters is
// refused (`verifyIdenticalModule`, src/module/manager.cpp:212), so without
// releasing the module between cases the second case in a process fails at its
// own load rather than at what it set out to test. `unload()` exists for this.
//
// The failure is visible only when the test binary is run directly:
// `gtest_discover_tests` gives every case its own process, so `ctest` passes
// either way (medios#472).
class GpuModuleTest : public MediosTest
{
protected:
  void TearDown() override
  {
    for (const string& name : {GPU_MODULE_NAME, GPU_DEVICES_MODULE_NAME}) {
      if (ModuleManager::contains<Isolator>(name)) {
        ASSERT_SOME(ModuleManager::unload(name));
      }
    }

    MediosTest::TearDown();
  }
};


// The module library opens, the manifest verifies against this build's
// module API and version, and the manager can construct the isolator.
TEST_F(GpuModuleTest, LoadsThroughModuleManager)
{
  const Option<string> units = writeUnitsFileForThisHost();
  if (units.isNone()) {
    GTEST_SKIP() << "No GPU on this host; the module has nothing it could be "
                 << "declared over. Covered by a run on a GPU host.";
  }

  ASSERT_SOME(ModuleManager::load(
      gpuModules(*units, {GPU_MODULE_NAME, GPU_DEVICES_MODULE_NAME})));

  for (const string& name : {GPU_MODULE_NAME, GPU_DEVICES_MODULE_NAME}) {
    ASSERT_TRUE(ModuleManager::contains<Isolator>(name)) << name;

    Try<Isolator*> isolator = ModuleManager::create<Isolator>(name);
    ASSERT_SOME(isolator) << name;
    delete isolator.get();
  }
}


// Prepares a GPU container with the assignment isolator and then the device
// isolator, the order --isolation lists them in. The assignment isolator's
// launch info must set CUDA_VISIBLE_DEVICES to the device's UUID. The device
// isolator's launch info must declare that device's file, read from the
// checkpoint the assignment isolator wrote.
//
// A second container is prepared by the device isolator with no assignment
// before it, which is what a node gets when --isolation lists the two in the
// wrong order. Its prepare() must fail with a message naming the right order.
//
// Skipped without a GPU, because the isolators are created only over a units
// file whose devices NVML reports on this host.
TEST_F(GpuModuleTest, AssignThenDevicesHandsTheAssignmentAcross)
{
  const Option<string> units = writeUnitsFileForThisHost();
  if (units.isNone()) {
    GTEST_SKIP() << "No GPU on this host; covered by a run on a GPU host.";
  }

  std::unique_ptr<medios::modules::gpu::DeviceSource> source =
    medios::modules::gpu::nvmlDeviceSource();
  Try<vector<medios::modules::gpu::Device>> host =
    medios::modules::gpu::discover(*source);
  ASSERT_SOME(host);
  const medios::modules::gpu::Device& device = host->at(0);

  ASSERT_SOME(ModuleManager::load(
      gpuModules(*units, {GPU_MODULE_NAME, GPU_DEVICES_MODULE_NAME})));

  Try<Isolator*> assignCreated = ModuleManager::create<Isolator>(GPU_MODULE_NAME);
  ASSERT_SOME(assignCreated);
  Owned<Isolator> assign(assignCreated.get());

  Try<Isolator*> devicesCreated =
    ModuleManager::create<Isolator>(GPU_DEVICES_MODULE_NAME);
  ASSERT_SOME(devicesCreated);
  Owned<Isolator> devices(devicesCreated.get());

  // A container with a GPU claim and no task, so the variable belongs in the
  // container environment.
  const string directory = path::join(os::getcwd(), "container");
  ASSERT_SOME(os::mkdir(directory));

  medios::node::ContainerConfig config;
  config.set_directory(directory);
  config.mutable_command_info()->set_value("true");
  config.mutable_resources()->CopyFrom(
      medios::Resources::parse("cpus:1;gpus:1").get());

  medios::ContainerID containerId;
  containerId.set_value("gpu-container");

  Future<Option<medios::node::ContainerLaunchInfo>> assigned =
    assign->prepare(containerId, config);
  AWAIT_READY(assigned);
  ASSERT_SOME(assigned.get());

  const medios::node::ContainerLaunchInfo& assignInfo = assigned->get();
  EXPECT_FALSE(assignInfo.has_task_environment());
  ASSERT_EQ(1, assignInfo.environment().variables_size());
  EXPECT_EQ("CUDA_VISIBLE_DEVICES", assignInfo.environment().variables(0).name());
  EXPECT_EQ(device.uuid, assignInfo.environment().variables(0).value());
  EXPECT_EQ(0, assignInfo.devices_size());

  Future<Option<medios::node::ContainerLaunchInfo>> declared =
    devices->prepare(containerId, config);
  AWAIT_READY(declared);
  ASSERT_SOME(declared.get());

  std::set<string> paths;
  foreach (const medios::DeviceAccess& access, declared->get().devices()) {
    paths.insert(access.device().path());
  }
  EXPECT_EQ(1u, paths.count(medios::modules::gpu::devicePath(device)));
  EXPECT_EQ(1u, paths.count("/dev/nvidiactl"));
  // The variable is assign's alone; devices sets no environment for a
  // container without a rootfs, since no driver volume is injected.
  EXPECT_EQ(0, declared->get().environment().variables_size());

  // Devices without assign before it: a fresh directory with no checkpoint.
  const string unassigned = path::join(os::getcwd(), "unassigned");
  ASSERT_SOME(os::mkdir(unassigned));

  medios::node::ContainerConfig backwards = config;
  backwards.set_directory(unassigned);

  medios::ContainerID backwardsId;
  backwardsId.set_value("backwards-container");

  Future<Option<medios::node::ContainerLaunchInfo>> refused =
    devices->prepare(backwardsId, backwards);
  AWAIT_FAILED(refused);
  EXPECT_TRUE(strings::contains(
      refused.failure(), "List " + GPU_MODULE_NAME + " before")) << refused.failure();

  // A container with no GPU claim and no checkpoint is untouched by both.
  medios::node::ContainerConfig plain = backwards;
  plain.mutable_resources()->CopyFrom(medios::Resources::parse("cpus:1").get());

  Future<Option<medios::node::ContainerLaunchInfo>> untouched =
    devices->prepare(backwardsId, plain);
  AWAIT_READY(untouched);
  EXPECT_NONE(untouched.get());

  AWAIT_READY(assign->cleanup(containerId));
}


// The command executor reads the task environment, not the container
// environment. For a container that carries a task, the assignment isolator
// must put CUDA_VISIBLE_DEVICES in the task environment.
TEST_F(GpuModuleTest, AssignPutsTheVariableInTheTaskEnvironmentOfACommandTask)
{
  const Option<string> units = writeUnitsFileForThisHost();
  if (units.isNone()) {
    GTEST_SKIP() << "No GPU on this host; covered by a run on a GPU host.";
  }

  ASSERT_SOME(ModuleManager::load(gpuModules(*units)));

  Try<Isolator*> created = ModuleManager::create<Isolator>(GPU_MODULE_NAME);
  ASSERT_SOME(created);
  Owned<Isolator> assign(created.get());

  const string directory = path::join(os::getcwd(), "task-container");
  ASSERT_SOME(os::mkdir(directory));

  medios::node::ContainerConfig config;
  config.set_directory(directory);
  config.mutable_command_info()->set_value("true");
  config.mutable_resources()->CopyFrom(
      medios::Resources::parse("cpus:1;gpus:1").get());
  config.mutable_task_info()->set_name("task");
  config.mutable_task_info()->mutable_task_id()->set_value("task");
  config.mutable_task_info()->mutable_node_id()->set_value("node");

  medios::ContainerID containerId;
  containerId.set_value("task-container");

  Future<Option<medios::node::ContainerLaunchInfo>> assigned =
    assign->prepare(containerId, config);
  AWAIT_READY(assigned);
  ASSERT_SOME(assigned.get());

  EXPECT_EQ(0, assigned->get().environment().variables_size());
  ASSERT_EQ(1, assigned->get().task_environment().variables_size());
  EXPECT_EQ("CUDA_VISIBLE_DEVICES",
            assigned->get().task_environment().variables(0).name());

  AWAIT_READY(assign->cleanup(containerId));
}


// The other half of the load: a units file naming a device this host does not
// have refuses to construct the isolator (DR-034 hypothesis 2, through the
// module manager rather than against resolveUnits directly). On a host with
// no driver the same refusal arrives from discovery instead, so only a host
// with a driver exercises the unknown-UUID path; both are a refusal and
// neither is a load.
TEST_F(GpuModuleTest, AUnitsFileThisHostCannotHonourRefusesTheIsolator)
{
  const string path = path::join(os::getcwd(), "absent.json");

  ASSERT_SOME(os::write(
      path,
      "{\"units\": [{\"resource\": \"gpus\", \"per_device\": 1, "
      "\"devices\": [\"GPU-not-on-any-host\"]}]}"));

  ASSERT_SOME(ModuleManager::load(gpuModules(path)));
  ASSERT_TRUE(ModuleManager::contains<Isolator>(GPU_MODULE_NAME));

  EXPECT_ERROR(ModuleManager::create<Isolator>(GPU_MODULE_NAME));
}


// A manifest with one library entry that names both isolators loads both
// names. Each isolator's create() checks the units file against the devices
// NVML reports and refuses a file naming a device this host does not have.
// The test runs without a GPU, because its units file names a UUID that no
// host has.
TEST_F(GpuModuleTest, BothIsolatorsLoadFromOneLibraryAndCheckTheirUnits)
{
  const string path = path::join(os::getcwd(), "absent.json");

  ASSERT_SOME(os::write(
      path,
      "{\"units\": [{\"resource\": \"gpus\", \"per_device\": 1, "
      "\"devices\": [\"GPU-not-on-any-host\"]}]}"));

  ASSERT_SOME(ModuleManager::load(
      gpuModules(path, {GPU_MODULE_NAME, GPU_DEVICES_MODULE_NAME})));

  ASSERT_TRUE(ModuleManager::contains<Isolator>(GPU_MODULE_NAME));
  ASSERT_TRUE(ModuleManager::contains<Isolator>(GPU_DEVICES_MODULE_NAME));

  EXPECT_ERROR(ModuleManager::create<Isolator>(GPU_MODULE_NAME));
  EXPECT_ERROR(ModuleManager::create<Isolator>(GPU_DEVICES_MODULE_NAME));
}


// The library no longer registers `com_medioslab_medios_gpu`. A manifest that
// names it fails to load, so a node configured with the old name fails at
// start instead of running without the GPU isolator its --isolation lists.
TEST_F(GpuModuleTest, TheCombinedIsolatorNameIsNoLongerRegistered)
{
  Modules modules;
  Modules::Library* library = modules.add_libraries();
  library->set_file(MEDIOS_GPU_MODULE_PATH);
  library->add_modules()->set_name("com_medioslab_medios_gpu");

  EXPECT_ERROR(ModuleManager::load(modules));
}


// A module with no units parameter has no declaration, and inferring one from
// whatever the driver reports is what this milestone removed.
TEST_F(GpuModuleTest, NoUnitsParameterRefusesTheIsolator)
{
  Modules modules;
  Modules::Library* library = modules.add_libraries();
  library->set_file(MEDIOS_GPU_MODULE_PATH);
  library->add_modules()->set_name(GPU_MODULE_NAME);

  ASSERT_SOME(ModuleManager::load(modules));
  ASSERT_TRUE(ModuleManager::contains<Isolator>(GPU_MODULE_NAME));

  EXPECT_ERROR(ModuleManager::create<Isolator>(GPU_MODULE_NAME));
}


// Two cases' worth of declarations in one process, which is the mechanism the
// rest of the suite now depends on: released, the module takes a second
// declaration; still loaded, the manager refuses it and whichever case carried
// it fails at its own load rather than at what it set out to test. Both halves
// here refuse to construct the isolator, so this case needs no GPU.
//
// It proves the release works, not that the fixture performs it. What catches a
// fixture that stops releasing is the `gpu_module_tests_in_one_process` entry in
// tests/CMakeLists.txt (medios#472).
TEST_F(GpuModuleTest, ASecondDeclarationLoadsOnceTheModuleIsReleased)
{
  const string first = path::join(os::getcwd(), "first.json");
  const string second = path::join(os::getcwd(), "second.json");

  for (const string& file : {first, second}) {
    ASSERT_SOME(os::write(
        file,
        "{\"units\": [{\"resource\": \"gpus\", \"per_device\": 1, "
        "\"devices\": [\"GPU-not-on-any-host\"]}]}"));
  }

  ASSERT_SOME(ModuleManager::load(gpuModules(first)));
  EXPECT_ERROR(ModuleManager::create<Isolator>(GPU_MODULE_NAME));

  ASSERT_SOME(ModuleManager::unload(GPU_MODULE_NAME));

  // The parameters differ from the first load only in the units path, which is
  // enough for the manager to refuse a module it still holds.
  ASSERT_SOME(ModuleManager::load(gpuModules(second)));
  EXPECT_ERROR(ModuleManager::create<Isolator>(GPU_MODULE_NAME));
}


// Control: an isolation name that no module provides refuses node start.
// Without this, the registration test below would pass for any name.
TEST_F(GpuModuleTest, UnknownIsolationRefusesNodeStart)
{
  Try<Owned<cluster::Controller>> controller = StartController();
  ASSERT_SOME(controller);

  Flags flags = CreateNodeFlags();
  flags.isolation = "com_medioslab_not_a_module";

  Owned<ControllerDetector> detector = controller.get()->createDetector();
  Try<Owned<cluster::Node>> node = StartNode(detector.get(), flags);
  ASSERT_ERROR(node);
  EXPECT_TRUE(strings::contains(node.error(), "Unknown or unsupported isolator"))
    << node.error();
}


// A node that selects the module by name builds its containerizer with the
// module's isolator and registers with the controller.
TEST_F(GpuModuleTest, NodeRegistersWithModuleIsolation)
{
  const Option<string> units = writeUnitsFileForThisHost();
  if (units.isNone()) {
    GTEST_SKIP() << "No GPU on this host; the module has nothing it could be "
                 << "declared over. Covered by a run on a GPU host.";
  }

  ASSERT_SOME(ModuleManager::load(gpuModules(*units)));

  Try<Owned<cluster::Controller>> controller = StartController();
  ASSERT_SOME(controller);

  Future<NodeRegisteredMessage> registered =
    FUTURE_PROTOBUF(NodeRegisteredMessage(), _, _);

  Flags flags = CreateNodeFlags();
  flags.isolation = GPU_MODULE_NAME;

  Owned<ControllerDetector> detector = controller.get()->createDetector();
  Try<Owned<cluster::Node>> node = StartNode(detector.get(), flags);
  ASSERT_SOME(node);

  AWAIT_READY(registered);
}


// --- device discovery -------------------------------------------------------
//
// `discover()` is written against an injectable source because `nvml` is a
// namespace of free functions over a dlopen'd library with process-global
// state, which a test cannot substitute. The cases that matter are the error
// paths, and none of them need a GPU.

namespace {

// A scripted driver. `counts` and `minors` are what the source returns; an
// Error entry makes that call fail.
class FakeDeviceSource : public medios::modules::gpu::DeviceSource
{
public:
  FakeDeviceSource(
      const Try<unsigned int>& _count,
      const hashmap<unsigned int, Try<unsigned int>>& _minors)
    : countResult(_count), minors(_minors) {}

  FakeDeviceSource(
      const Try<unsigned int>& _count,
      const hashmap<unsigned int, Try<unsigned int>>& _minors,
      const hashmap<unsigned int, Try<std::string>>& _uuids)
    : countResult(_count), minors(_minors), uuids(_uuids) {}

  Try<unsigned int> count() override
  {
    countCalls++;
    return countResult;
  }

  Try<unsigned int> minorNumber(unsigned int index) override
  {
    queried.push_back(index);
    if (!minors.contains(index)) {
      return Error("no such device " + stringify(index));
    }
    return minors.at(index);
  }

  // A synthesised identity when the case under test does not care about it,
  // so the tests written before devices had one keep saying what they said.
  // The cases that do care pass `uuids` and get exactly what they passed.
  Try<std::string> uuid(unsigned int index) override
  {
    if (uuids.contains(index)) {
      return uuids.at(index);
    }
    return "GPU-fake-" + stringify(index);
  }

  Try<unsigned int> countResult;
  hashmap<unsigned int, Try<unsigned int>> minors;
  hashmap<unsigned int, Try<std::string>> uuids;
  int countCalls = 0;
  vector<unsigned int> queried;
};

} // namespace {


// The ordinary case: every device is described, in index order.
TEST(GpuDiscoveryTest, ReportsEveryDeviceInIndexOrder)
{
  FakeDeviceSource source(3u, {{0u, 0u}, {1u, 1u}, {2u, 4u}});

  Try<vector<medios::modules::gpu::Device>> devices =
    medios::modules::gpu::discover(source);

  ASSERT_SOME(devices);
  ASSERT_EQ(3u, devices->size());
  EXPECT_EQ(0u, devices->at(0).index);
  EXPECT_EQ(0u, devices->at(0).minorNumber);
  EXPECT_EQ(2u, devices->at(2).index);

  // The minor number is not the index: /dev/nvidia4 is device 2 here, and a
  // caller that assumes they are the same delivers the wrong device file.
  EXPECT_EQ(4u, devices->at(2).minorNumber);
}


TEST(GpuDiscoveryTest, NoDevicesIsNotAnError)
{
  FakeDeviceSource source(0u, {});

  Try<vector<medios::modules::gpu::Device>> devices =
    medios::modules::gpu::discover(source);

  ASSERT_SOME(devices);
  EXPECT_TRUE(devices->empty());

  // A host with no GPUs is a fact, not a failure, and nothing should have
  // been asked about a device.
  EXPECT_TRUE(source.queried.empty());
}


TEST(GpuDiscoveryTest, CountFailureIsReported)
{
  FakeDeviceSource source(Error("driver says no"), {{0u, 0u}});

  Try<vector<medios::modules::gpu::Device>> devices =
    medios::modules::gpu::discover(source);

  ASSERT_ERROR(devices);
  EXPECT_TRUE(strings::contains(devices.error(), "driver says no"))
    << devices.error();

  // No device was described, since the count that would bound the loop failed.
  EXPECT_TRUE(source.queried.empty());
}


// The case this interface exists for. A partial inventory is worse than a
// refusal: a caller cannot tell one GPU from four where three could not be
// read, and under-reporting hardware looks exactly like not having it.
TEST(GpuDiscoveryTest, OneUndescribableDeviceFailsTheWholeDiscovery)
{
  FakeDeviceSource source(
      4u, {{0u, 0u}, {1u, 1u}, {3u, 3u}}); // device 2 cannot be described

  Try<vector<medios::modules::gpu::Device>> devices =
    medios::modules::gpu::discover(source);

  ASSERT_ERROR(devices);

  // The refusal names which device of how many, so an operator can act on it.
  EXPECT_TRUE(strings::contains(devices.error(), "device 2")) << devices.error();
  EXPECT_TRUE(strings::contains(devices.error(), "of 4")) << devices.error();
}


// A count larger than the devices the driver will describe is the same
// failure, and must not silently truncate.
TEST(GpuDiscoveryTest, CountLargerThanDescribableDevicesFails)
{
  FakeDeviceSource source(8u, {{0u, 0u}});

  Try<vector<medios::modules::gpu::Device>> devices =
    medios::modules::gpu::discover(source);

  ASSERT_ERROR(devices);
  EXPECT_TRUE(strings::contains(devices.error(), "device 1")) << devices.error();
}


// A device carries the driver's identity, which is what a declaration names.
// The index and the minor number are host- and boot-local; the UUID is not.
TEST(GpuDiscoveryTest, ReportsTheDriverIdentityOfEveryDevice)
{
  FakeDeviceSource source(
      2u,
      {{0u, 0u}, {1u, 1u}},
      {{0u, std::string("GPU-804c560c")}, {1u, std::string("GPU-11f0aa31")}});

  Try<vector<medios::modules::gpu::Device>> devices =
    medios::modules::gpu::discover(source);

  ASSERT_SOME(devices);
  ASSERT_EQ(2u, devices->size());
  EXPECT_EQ("GPU-804c560c", devices->at(0).uuid);
  EXPECT_EQ("GPU-11f0aa31", devices->at(1).uuid);
}


// Same rule as the minor number: a device that cannot be fully described
// fails the inventory rather than joining it without an identity.
TEST(GpuDiscoveryTest, AnUnreadableIdentityFailsTheWholeDiscovery)
{
  FakeDeviceSource source(
      2u,
      {{0u, 0u}, {1u, 1u}},
      {{0u, std::string("GPU-804c560c")}, {1u, Error("driver refused")}});

  Try<vector<medios::modules::gpu::Device>> devices =
    medios::modules::gpu::discover(source);

  ASSERT_ERROR(devices);
  EXPECT_TRUE(strings::contains(devices.error(), "device 1")) << devices.error();
  EXPECT_TRUE(strings::contains(devices.error(), "driver refused"))
    << devices.error();
}


// An empty identity is refused rather than carried. Two devices reporting
// nothing would be indistinguishable, and a declaration naming one of them
// would resolve to whichever came first.
TEST(GpuDiscoveryTest, AnEmptyIdentityIsRefused)
{
  FakeDeviceSource source(
      1u, {{0u, 0u}}, {{0u, std::string("")}});

  Try<vector<medios::modules::gpu::Device>> devices =
    medios::modules::gpu::discover(source);

  ASSERT_ERROR(devices);
  EXPECT_TRUE(strings::contains(devices.error(), "empty UUID"))
    << devices.error();
}


// The NVML-backed source must fail rather than report an empty inventory when
// the driver library is absent, which is every host without it installed.
// "No NVML" and "no GPUs" are different answers and only one of them is safe
// to act on.
TEST(GpuDiscoveryTest, NvmlSourceFailsWhenNvmlIsUnavailable)
{
  if (nvml::isAvailable()) {
    GTEST_SKIP() << "This host has the driver, so the absent-library path "
                 << "cannot be reached here.";
  }

  std::unique_ptr<medios::modules::gpu::DeviceSource> source =
    medios::modules::gpu::nvmlDeviceSource();

  Try<unsigned int> count = source->count();

  ASSERT_ERROR(count);
  EXPECT_TRUE(strings::contains(count.error(), "not available")) << count.error();
}



// --- device declarations ----------------------------------------------------
//
// What a container granted GPUs is allowed to open. The containerizer resolves
// a declaration by stat'ing the path (#455), so a declaration for an absent
// device fails the launch; these cases pin which devices are declared and
// which absences are errors rather than omissions.

namespace {

// A host whose /dev contains exactly these paths.
std::function<bool(const std::string&)> hostWith(const std::set<std::string>& paths)
{
  return [paths](const std::string& path) { return paths.count(path) > 0; };
}

std::set<std::string> declaredPaths(const vector<medios::DeviceAccess>& ds)
{
  std::set<std::string> paths;
  foreach (const medios::DeviceAccess& d, ds) { paths.insert(d.device().path()); }
  return paths;
}

} // namespace {


// The ordinary case: the granted GPUs, the control device, and unified memory.
TEST(GpuDeliveryTest, DeclaresGrantedDevicesAndControlDevices)
{
  const vector<medios::modules::gpu::Device> granted = {{0u, 0u}};

  Try<vector<medios::DeviceAccess>> declarations =
    medios::modules::gpu::deviceDeclarations(
        granted,
        hostWith({"/dev/nvidia0", "/dev/nvidiactl",
                  "/dev/nvidia-uvm", "/dev/nvidia-uvm-tools"}));

  ASSERT_SOME(declarations);
  EXPECT_EQ(
      std::set<std::string>({"/dev/nvidia0", "/dev/nvidiactl",
                             "/dev/nvidia-uvm", "/dev/nvidia-uvm-tools"}),
      declaredPaths(*declarations));

  foreach (const medios::DeviceAccess& d, *declarations) {
    EXPECT_TRUE(d.access().read()) << d.device().path();
    EXPECT_TRUE(d.access().write()) << d.device().path();
  }
}


// A container granted one GPU must not be able to open the other. This is the
// whole point of the milestone, so it is pinned rather than assumed.
TEST(GpuDeliveryTest, GrantsOnlyTheDevicesInTheAllocation)
{
  const vector<medios::modules::gpu::Device> granted = {{1u, 1u}};

  Try<vector<medios::DeviceAccess>> declarations =
    medios::modules::gpu::deviceDeclarations(
        granted,
        hostWith({"/dev/nvidia0", "/dev/nvidia1", "/dev/nvidiactl"}));

  ASSERT_SOME(declarations);
  EXPECT_TRUE(declaredPaths(*declarations).count("/dev/nvidia1") == 1);
  EXPECT_TRUE(declaredPaths(*declarations).count("/dev/nvidia0") == 0)
    << "declared a device this container was not granted";
}


// The minor number names the device file, not the index. A host that
// enumerates device 0 as /dev/nvidia3 must get /dev/nvidia3.
TEST(GpuDeliveryTest, UsesTheMinorNumberNotTheIndex)
{
  const vector<medios::modules::gpu::Device> granted = {{0u, 3u}};

  Try<vector<medios::DeviceAccess>> declarations =
    medios::modules::gpu::deviceDeclarations(
        granted, hostWith({"/dev/nvidia3", "/dev/nvidiactl"}));

  ASSERT_SOME(declarations);
  EXPECT_TRUE(declaredPaths(*declarations).count("/dev/nvidia3") == 1);
}


// Unified memory is created on first use by nvidia-modprobe, so a host
// without it is normal and must not fail the launch.
TEST(GpuDeliveryTest, UnifiedMemoryDevicesAreOptional)
{
  const vector<medios::modules::gpu::Device> granted = {{0u, 0u}};

  Try<vector<medios::DeviceAccess>> declarations =
    medios::modules::gpu::deviceDeclarations(
        granted, hostWith({"/dev/nvidia0", "/dev/nvidiactl"}));

  ASSERT_SOME(declarations);
  EXPECT_EQ(
      std::set<std::string>({"/dev/nvidia0", "/dev/nvidiactl"}),
      declaredPaths(*declarations));
}


// Without the control device nothing works, so its absence is a failure and
// not a device to skip.
TEST(GpuDeliveryTest, MissingControlDeviceIsAnError)
{
  const vector<medios::modules::gpu::Device> granted = {{0u, 0u}};

  Try<vector<medios::DeviceAccess>> declarations =
    medios::modules::gpu::deviceDeclarations(
        granted, hostWith({"/dev/nvidia0"}));

  ASSERT_ERROR(declarations);
  EXPECT_TRUE(strings::contains(declarations.error(), "nvidiactl"))
    << declarations.error();
}


// A granted device with no device file means the ledger and the host
// disagree. Declaring the rest would start a container that believes it has
// a GPU it cannot open.
TEST(GpuDeliveryTest, GrantedDeviceWithNoDeviceFileIsAnError)
{
  const vector<medios::modules::gpu::Device> granted = {{0u, 0u}, {1u, 1u}};

  Try<vector<medios::DeviceAccess>> declarations =
    medios::modules::gpu::deviceDeclarations(
        granted, hostWith({"/dev/nvidia0", "/dev/nvidiactl"}));

  ASSERT_ERROR(declarations);
  EXPECT_TRUE(strings::contains(declarations.error(), "/dev/nvidia1"))
    << declarations.error();
}


// A container granted nothing gets no GPU devices. It still gets no
// declarations at all, so the containerizer's empty short-circuit applies and
// a non-GPU container is untouched.
TEST(GpuDeliveryTest, NoGrantDeclaresNothingAtAll)
{
  Try<vector<medios::DeviceAccess>> declarations =
    medios::modules::gpu::deviceDeclarations({}, hostWith({"/dev/nvidiactl"}));

  ASSERT_SOME(declarations);
  EXPECT_TRUE(declarations->empty())
    << "a container granted no GPUs must not receive driver access either";
}


namespace {

// The declarations a host produces, as the input to the mount decision. Built
// through deviceDeclarations rather than written by hand so the two cannot
// drift apart.
vector<medios::DeviceAccess> declarationsFor(
    const vector<medios::modules::gpu::Device>& granted,
    const std::set<std::string>& devicesOnHost)
{
  Try<vector<medios::DeviceAccess>> declarations =
    medios::modules::gpu::deviceDeclarations(granted, hostWith(devicesOnHost));

  CHECK_SOME(declarations);
  return *declarations;
}


std::set<std::string> mountSources(
    const vector<medios::modules::gpu::DeviceMount>& mounts)
{
  std::set<std::string> sources;
  foreach (const medios::modules::gpu::DeviceMount& m, mounts) {
    sources.insert(m.source);
  }
  return sources;
}

} // namespace {


// A declaration grants the right to open a device; under a private /dev the
// file has to be there as well. Every declared device is bound in at the path
// it was declared under, because that is the name CUDA opens.
TEST(GpuDeviceMountTest, BindsEveryDeclaredDeviceUnderTheRootfs)
{
  const vector<medios::DeviceAccess> declarations =
    declarationsFor({{0u, 0u}}, {"/dev/nvidia0", "/dev/nvidiactl"});

  const vector<medios::modules::gpu::DeviceMount> mounts =
    medios::modules::gpu::deviceMounts(declarations, "/rootfs");

  EXPECT_EQ(
      std::set<std::string>({"/dev/nvidia0", "/dev/nvidiactl"}),
      mountSources(mounts));

  foreach (const medios::modules::gpu::DeviceMount& m, mounts) {
    EXPECT_EQ("/rootfs" + m.source, m.target)
      << "a device must keep its path inside the container";
  }
}


// The isolation this milestone exists to provide has to survive the mounts. A
// container granted one GPU must not find the other in its /dev, which is what
// globbing /dev/nvidia* would have given it.
TEST(GpuDeviceMountTest, MountsOnlyTheGrantedDevice)
{
  const vector<medios::DeviceAccess> declarations =
    declarationsFor(
        {{1u, 1u}},
        {"/dev/nvidia0", "/dev/nvidia1", "/dev/nvidiactl"});

  const std::set<std::string> sources =
    mountSources(medios::modules::gpu::deviceMounts(declarations, "/rootfs"));

  EXPECT_TRUE(sources.count("/dev/nvidia1") == 1);
  EXPECT_TRUE(sources.count("/dev/nvidia0") == 0)
    << "bound a device this container was not granted";
}


// The source is the host's own node. A copy would have to live in the runtime
// directory, which is under /run on standard hosts and mounted nodev, where a
// device node cannot be opened at all.
TEST(GpuDeviceMountTest, BindsTheHostNodeRatherThanACopy)
{
  const vector<medios::DeviceAccess> declarations =
    declarationsFor({{0u, 0u}}, {"/dev/nvidia0", "/dev/nvidiactl"});

  foreach (
      const medios::modules::gpu::DeviceMount& m,
      medios::modules::gpu::deviceMounts(declarations, "/rootfs")) {
    EXPECT_TRUE(strings::startsWith(m.source, "/dev/"))
      << "source must be the host's device node, not a staged copy: "
      << m.source;
  }
}


// A container granted no GPUs declares nothing, so there is nothing to bind.
// Without this the empty case would put the control device in every container
// on the node.
TEST(GpuDeviceMountTest, NoDeclarationsMountNothing)
{
  EXPECT_TRUE(medios::modules::gpu::deviceMounts({}, "/rootfs").empty());
}


// --- the driver volume ------------------------------------------------------
//
// A container whose image replaced the filesystem gets the host's driver
// libraries bound in. It must not be able to write to them: they are the
// host's own files, and a container that overwrote them would break every
// later GPU container on the node (medios#481).

// The kernel ignores MS_RDONLY on a bind, so read-only is a second call
// carrying MS_REMOUNT. Asserted as a sequence rather than a set: the remount
// changes the mount the bind just made, so it cannot come first.
TEST(GpuDriverVolumeTest, TheVolumeIsBoundAndThenRemountedReadOnly)
{
  const vector<medios::modules::gpu::VolumeMount> mounts =
    medios::modules::gpu::driverVolumeMounts("/host/nvidia", "/rootfs/nvidia");

  ASSERT_EQ(2u, mounts.size());

  EXPECT_EQ(medios::modules::gpu::VolumeMountMode::BIND, mounts[0].mode);
  EXPECT_EQ(
      medios::modules::gpu::VolumeMountMode::REMOUNT_READ_ONLY,
      mounts[1].mode)
    << "a bind alone is writable; the remount is what makes it read-only";

  foreach (const medios::modules::gpu::VolumeMount& m, mounts) {
    EXPECT_EQ("/host/nvidia", m.source);
    EXPECT_EQ("/rootfs/nvidia", m.target)
      << "the remount has to name the mount the bind made";
  }
}


// Two mounts and no more. A third mount of the same target, or a second bind
// after the remount, would leave the volume writable again, and the order here
// is the only thing that says which mount the remount changes.
TEST(GpuDriverVolumeTest, TheVolumeIsMountedTwiceAndNoMoreThanTwice)
{
  const vector<medios::modules::gpu::VolumeMount> mounts =
    medios::modules::gpu::driverVolumeMounts("/host/nvidia", "/rootfs/nvidia");

  size_t binds = 0;
  size_t remounts = 0;
  foreach (const medios::modules::gpu::VolumeMount& m, mounts) {
    if (m.mode == medios::modules::gpu::VolumeMountMode::BIND) {
      ++binds;
    } else {
      ++remounts;
    }
  }

  EXPECT_EQ(1u, binds);
  EXPECT_EQ(1u, remounts);
  EXPECT_EQ(2u, mounts.size());
}


#ifdef __linux__
// The flag word each mode becomes, which is the whole of what medios#481 asked
// to be held in place. The tests above are one level up and would pass with
// MS_REC put back or MS_REMOUNT taken away, so they say nothing about
// read-only; these are the assertions that do.
TEST(GpuDriverVolumeTest, TheFlagsCarryReadOnlyAndNeverRecurse)
{
  const unsigned long bind =
    medios::modules::gpu::volumeMountFlags(
        medios::modules::gpu::VolumeMountMode::BIND);

  EXPECT_EQ(static_cast<unsigned long>(MS_BIND), bind);

  const unsigned long readOnly =
    medios::modules::gpu::volumeMountFlags(
        medios::modules::gpu::VolumeMountMode::REMOUNT_READ_ONLY);

  EXPECT_EQ(
      static_cast<unsigned long>(MS_BIND | MS_REMOUNT | MS_RDONLY),
      readOnly);

  // Named one at a time as well, so a failure says which property was lost.
  EXPECT_TRUE((readOnly & MS_REMOUNT) != 0)
    << "the kernel ignores MS_RDONLY on a bind; without MS_REMOUNT this "
       "second mount does not make the volume read-only";
  EXPECT_TRUE((readOnly & MS_RDONLY) != 0);

  EXPECT_TRUE((bind & MS_REC) == 0)
    << "a recursive bind carries a filesystem mounted beneath the volume "
       "into the container, and the read-only remount does not reach it";
  EXPECT_TRUE((readOnly & MS_REC) == 0);
}
#endif // __linux__


// --- the units file ---------------------------------------------------------
//
// What the operator declared, and what the module refuses at start rather than
// discovering later (DR-035, FAC3-0028). These run against a fake host so the
// rules are pinned without a GPU, and again on a GPU host against the real
// driver.

namespace {

using medios::modules::gpu::Device;
using medios::modules::gpu::Placement;
using medios::modules::gpu::UnitSet;
using medios::modules::gpu::UnitDeclaration;

// A units file declaring one resource over these UUIDs, each device providing
// `perDevice` of it.
string unitsFile(
    const string& resource, size_t perDevice, const vector<string>& uuids)
{
  string devices;
  for (size_t i = 0; i < uuids.size(); i++) {
    devices += (i == 0 ? "\"" : ", \"") + uuids.at(i) + "\"";
  }

  return "{\"units\": [{\"resource\": \"" + resource + "\", "
         "\"per_device\": " + stringify(perDevice) +
         ", \"devices\": [" + devices + "]}]}";
}


// A host reporting these devices, in index order, with these identities.
vector<Device> hostOf(const vector<string>& uuids)
{
  vector<Device> devices;
  for (size_t i = 0; i < uuids.size(); i++) {
    devices.push_back(
        Device{static_cast<unsigned int>(i),
               static_cast<unsigned int>(i),
               uuids.at(i)});
  }
  return devices;
}


// One resolved unit, without going through a file.
UnitSet unitOf(
    const string& resource, size_t perDevice, const vector<string>& uuids)
{
  return UnitSet{resource, perDevice, hostOf(uuids)};
}

} // namespace {


// The ordinary case: a declaration this host can honour resolves to one unit
// carrying the devices discovery reported, in the operator's order.
TEST(GpuUnitsTest, ResolvesEveryDeclaredDeviceToItsDiscoveredDevice)
{
  Try<vector<UnitDeclaration>> declarations =
    medios::modules::gpu::parseUnits(unitsFile("gpus", 1, {"GPU-b", "GPU-a"}));

  ASSERT_SOME(declarations);
  ASSERT_EQ(1u, declarations->size());
  EXPECT_EQ("gpus", declarations->at(0).resource);
  EXPECT_EQ(1u, declarations->at(0).perDevice);

  Try<vector<UnitSet>> units = medios::modules::gpu::resolveUnits(
      *declarations, hostOf({"GPU-a", "GPU-b"}));

  ASSERT_SOME(units);
  ASSERT_EQ(1u, units->size());
  EXPECT_EQ("gpus", units->at(0).resource);
  EXPECT_EQ(1u, units->at(0).perDevice);
  ASSERT_EQ(2u, units->at(0).devices.size());

  // Declaration order, not discovery order: the operator's file is the list,
  // and the device carried is the one the driver reported for that UUID.
  EXPECT_EQ("GPU-b", units->at(0).devices.at(0).uuid);
  EXPECT_EQ(1u, units->at(0).devices.at(0).minorNumber);
  EXPECT_EQ("GPU-a", units->at(0).devices.at(1).uuid);
  EXPECT_EQ(0u, units->at(0).devices.at(1).minorNumber);
}


// A seat unit is the same file with a larger per_device, and it resolves the
// same way. Nothing about the shared device is a different shape.
TEST(GpuUnitsTest, ASeatUnitCarriesWhatOneDeviceProvides)
{
  Try<vector<UnitDeclaration>> declarations =
    medios::modules::gpu::parseUnits(unitsFile("gpu_replica", 10, {"GPU-a"}));

  ASSERT_SOME(declarations);
  ASSERT_EQ(1u, declarations->size());
  EXPECT_EQ(10u, declarations->at(0).perDevice);

  Try<vector<UnitSet>> units =
    medios::modules::gpu::resolveUnits(*declarations, hostOf({"GPU-a"}));

  ASSERT_SOME(units);
  ASSERT_EQ(1u, units->size());
  EXPECT_EQ(10u, units->at(0).perDevice);
  ASSERT_EQ(1u, units->at(0).devices.size());
  EXPECT_EQ("GPU-a", units->at(0).devices.at(0).uuid);
}


// FAC3-0028. A declared device this host does not have refuses the start,
// naming the UUID, rather than starting over fewer devices: the node would
// then offer a share of hardware no container could ever be given.
TEST(GpuUnitsTest, ADeclaredDeviceTheHostDoesNotHaveIsRefusedByName)
{
  Try<vector<UnitDeclaration>> declarations =
    medios::modules::gpu::parseUnits(
        unitsFile("gpus", 1, {"GPU-present", "GPU-absent"}));

  ASSERT_SOME(declarations);

  Try<vector<UnitSet>> units = medios::modules::gpu::resolveUnits(
      *declarations, hostOf({"GPU-present"}));

  ASSERT_ERROR(units);
  EXPECT_TRUE(strings::contains(units.error(), "GPU-absent")) << units.error();
  EXPECT_TRUE(strings::contains(units.error(), "gpus")) << units.error();
}


// One device under two resource names is refused at the declaration, where
// the operator can fix it, rather than at run time, where the same hardware
// has already been given out under two totals core counts separately.
TEST(GpuUnitsTest, ADeviceDeclaredUnderTwoResourcesIsRefused)
{
  Try<vector<UnitDeclaration>> declarations =
    medios::modules::gpu::parseUnits(
        "{\"units\": ["
        "{\"resource\": \"gpus\", \"per_device\": 1, "
        "\"devices\": [\"GPU-a\"]},"
        "{\"resource\": \"gpu_replica\", \"per_device\": 10, "
        "\"devices\": [\"GPU-a\"]}]}");

  ASSERT_SOME(declarations);

  Try<vector<UnitSet>> units = medios::modules::gpu::resolveUnits(
      *declarations, hostOf({"GPU-a"}));

  ASSERT_ERROR(units);
  EXPECT_TRUE(strings::contains(units.error(), "gpus")) << units.error();
  EXPECT_TRUE(strings::contains(units.error(), "gpu_replica")) << units.error();
}


// The same device twice under one name would double what the resource
// provides while the node's --resources counted the device once.
TEST(GpuUnitsTest, ADeviceListedTwiceUnderOneResourceIsRefused)
{
  Try<vector<UnitDeclaration>> declarations =
    medios::modules::gpu::parseUnits(
        "{\"units\": [{\"resource\": \"gpus\", \"per_device\": 1, "
        "\"devices\": [\"GPU-a\", \"GPU-a\"]}]}");

  ASSERT_ERROR(declarations);
  EXPECT_TRUE(strings::contains(declarations.error(), "twice"))
    << declarations.error();
}


// DR-034's fields are refused by name rather than ignored. A file carrying
// both `count` and `per_device` is an operator who believes one of the two,
// and reading the one this module now understands drops the other silently.
TEST(GpuUnitsTest, TheFieldsDR035ReplacedAreRefusedByName)
{
  Try<vector<UnitDeclaration>> mode =
    medios::modules::gpu::parseUnits(
        "{\"units\": [{\"resource\": \"gpus\", \"mode\": \"exclusive\", "
        "\"per_device\": 1, \"devices\": [\"GPU-a\"]}]}");

  ASSERT_ERROR(mode);
  EXPECT_TRUE(strings::contains(mode.error(), "mode")) << mode.error();
  EXPECT_TRUE(strings::contains(mode.error(), "per_device")) << mode.error();

  Try<vector<UnitDeclaration>> count =
    medios::modules::gpu::parseUnits(
        "{\"units\": [{\"resource\": \"gpus\", \"count\": 1, "
        "\"per_device\": 1, \"devices\": [\"GPU-a\"]}]}");

  ASSERT_ERROR(count);
  EXPECT_TRUE(strings::contains(count.error(), "count")) << count.error();
  EXPECT_TRUE(strings::contains(count.error(), "--resources")) << count.error();

  Try<vector<UnitDeclaration>> occupancy =
    medios::modules::gpu::parseUnits(
        "{\"units\": [{\"resource\": \"gpu_replica\", \"occupancy\": 10, "
        "\"per_device\": 10, \"devices\": [\"GPU-a\"]}]}");

  ASSERT_ERROR(occupancy);
  EXPECT_TRUE(strings::contains(occupancy.error(), "occupancy"))
    << occupancy.error();
}


// A missing per_device is refused rather than defaulted to one. It is what
// says how many holders a device takes, and defaulting it would turn a
// time-sliced device into an exclusive one without saying so.
TEST(GpuUnitsTest, AUnitWithNoPerDeviceIsRefused)
{
  Try<vector<UnitDeclaration>> declarations =
    medios::modules::gpu::parseUnits(
        "{\"units\": [{\"resource\": \"gpus\", \"devices\": [\"GPU-a\"]}]}");

  ASSERT_ERROR(declarations);
  EXPECT_TRUE(strings::contains(declarations.error(), "per_device"))
    << declarations.error();
}


// A per_device below one, fractional, or too large to convert is refused
// before the conversion rather than after it.
TEST(GpuUnitsTest, APerDeviceThatIsNotAWholeNumberOfAtLeastOneIsRefused)
{
  EXPECT_ERROR(medios::modules::gpu::parseUnits(unitsFile("gpus", 0, {"GPU-a"})));

  EXPECT_ERROR(medios::modules::gpu::parseUnits(
      "{\"units\": [{\"resource\": \"gpus\", \"per_device\": 2.5, "
      "\"devices\": [\"GPU-a\"]}]}"));

  EXPECT_ERROR(medios::modules::gpu::parseUnits(
      "{\"units\": [{\"resource\": \"gpus\", \"per_device\": -1, "
      "\"devices\": [\"GPU-a\"]}]}"));

  EXPECT_ERROR(medios::modules::gpu::parseUnits(
      "{\"units\": [{\"resource\": \"gpus\", \"per_device\": 1e30, "
      "\"devices\": [\"GPU-a\"]}]}"));
}


// A resource name with no device behind it would have the node offering
// per_device times zero of it. That is a half-edited file, not a host with
// no GPUs.
TEST(GpuUnitsTest, AUnitWithNoDevicesIsRefused)
{
  Try<vector<UnitDeclaration>> declarations =
    medios::modules::gpu::parseUnits(
        "{\"units\": [{\"resource\": \"gpus\", \"per_device\": 1, "
        "\"devices\": []}]}");

  ASSERT_ERROR(declarations);
  EXPECT_TRUE(strings::contains(declarations.error(), "no devices"))
    << declarations.error();
}


// A resource declared twice would look whole on each half while the node
// offered the sum of both.
TEST(GpuUnitsTest, AResourceDeclaredTwiceIsRefused)
{
  Try<vector<UnitDeclaration>> declarations =
    medios::modules::gpu::parseUnits(
        "{\"units\": ["
        "{\"resource\": \"gpus\", \"per_device\": 1, "
        "\"devices\": [\"GPU-a\"]},"
        "{\"resource\": \"gpus\", \"per_device\": 1, "
        "\"devices\": [\"GPU-b\"]}]}");

  ASSERT_ERROR(declarations);
  EXPECT_TRUE(strings::contains(declarations.error(), "more than once"))
    << declarations.error();
}


// A file that is not JSON, or not the shape, is an error naming the file
// rather than an empty declaration treated as "this host has no GPUs".
TEST(GpuUnitsTest, AFileThatIsNotTheExpectedShapeIsRefused)
{
  EXPECT_ERROR(medios::modules::gpu::parseUnits("not json at all"));
  EXPECT_ERROR(medios::modules::gpu::parseUnits("{}"));
  EXPECT_ERROR(medios::modules::gpu::parseUnits("{\"units\": []}"));
}


// --- placing a claim on devices ---------------------------------------------
//
// `place()` is the whole of what the module decides (DR-035). Quantity is
// core's: by the time a claim reaches here core has already subtracted it from
// the node's declared total, so what is decided is which devices serve it.
//
// It is a free function rather than a method because the isolator itself is
// Linux-only and defined inside the module's translation unit, so no test
// reaches its prepare() (medios#472). Pulling the decision out is what makes
// these cases testable at all on any host; the isolator around it is covered
// by a node run on a GPU host.

// per_device 1: a claim takes whole devices, and only ones nothing holds.
TEST(GpuPlacementTest, AWholeDeviceClaimTakesDevicesNothingHolds)
{
  const UnitSet unit = unitOf("gpus", 1, {"GPU-a", "GPU-b"});

  Try<Placement> first = medios::modules::gpu::place(unit, 1.0, {});
  ASSERT_SOME(first);
  EXPECT_TRUE(first->whole);
  ASSERT_EQ(1u, first->devices.size());
  EXPECT_EQ("GPU-a", first->devices.at(0).uuid);

  // With the first held, the second claim gets the other device rather than
  // the same one again.
  Try<Placement> second =
    medios::modules::gpu::place(unit, 1.0, {"GPU-a"});
  ASSERT_SOME(second);
  ASSERT_EQ(1u, second->devices.size());
  EXPECT_EQ("GPU-b", second->devices.at(0).uuid);

  // And with both held there is nothing left to give, which is a refusal
  // naming how many are free rather than a device handed out twice.
  Try<Placement> third =
    medios::modules::gpu::place(unit, 1.0, {"GPU-a", "GPU-b"});
  ASSERT_ERROR(third);
  EXPECT_TRUE(strings::contains(third.error(), "free")) << third.error();
}


// A claim of two whole devices takes two, and is refused when only one is
// free rather than served with one.
TEST(GpuPlacementTest, AClaimOfSeveralWholeDevicesIsServedWholeOrRefused)
{
  const UnitSet unit = unitOf("gpus", 1, {"GPU-a", "GPU-b"});

  Try<Placement> both = medios::modules::gpu::place(unit, 2.0, {});
  ASSERT_SOME(both);
  ASSERT_EQ(2u, both->devices.size());

  Try<Placement> partial =
    medios::modules::gpu::place(unit, 2.0, {"GPU-a"});
  ASSERT_ERROR(partial);
  EXPECT_TRUE(strings::contains(partial.error(), "gpus")) << partial.error();
}


// DR-035 worked example 1. Two models each hold a seat on the one device and
// get the same device, and holding a seat binds nothing, so the device is
// still there for the next holder. What refuses the third model in the
// record is memory, which core subtracts and this module never sees; the
// eight free seats it describes are what is asserted here.
TEST(GpuPlacementTest, EverySeatHolderGetsTheSameDevice)
{
  const UnitSet unit = unitOf("gpu_replica", 10, {"GPU-a"});

  Try<Placement> first = medios::modules::gpu::place(unit, 1.0, {});
  ASSERT_SOME(first);
  ASSERT_EQ(1u, first->devices.size());
  EXPECT_EQ("GPU-a", first->devices.at(0).uuid);

  // Not whole, so the caller records nothing and the device stays available
  // to the holders it is meant to be shared with.
  EXPECT_FALSE(first->whole);

  Try<Placement> second = medios::modules::gpu::place(unit, 1.0, {});
  ASSERT_SOME(second);
  ASSERT_EQ(1u, second->devices.size());
  EXPECT_EQ("GPU-a", second->devices.at(0).uuid);

  // A third holder, on the same device again. Nothing in the module stops
  // this; the ceiling is the ten seats core counts.
  Try<Placement> third = medios::modules::gpu::place(unit, 8.0, {});
  ASSERT_SOME(third);
  ASSERT_EQ(1u, third->devices.size());
  EXPECT_EQ("GPU-a", third->devices.at(0).uuid);
}


// DR-035 worked examples 2 and 3. A task that wants the device to itself asks
// for all of its seats, which is a whole-device claim and is served as one.
// What refuses the next model is core, which now has no seats left to offer;
// the module's part is that the whole claim is served by the one device.
TEST(GpuPlacementTest, AClaimOfEverySeatIsTheWholeDevice)
{
  const UnitSet unit = unitOf("gpu_replica", 10, {"GPU-a"});

  Try<Placement> all = medios::modules::gpu::place(unit, 10.0, {});
  ASSERT_SOME(all);
  ASSERT_EQ(1u, all->devices.size());
  EXPECT_EQ("GPU-a", all->devices.at(0).uuid);

  // Whole, so the caller records it, and a second container asking for all
  // ten seats is refused rather than handed the same card.
  EXPECT_TRUE(all->whole);

  Try<Placement> again = medios::modules::gpu::place(unit, 10.0, {"GPU-a"});
  ASSERT_ERROR(again);
  EXPECT_TRUE(strings::contains(again.error(), "free")) << again.error();
}


// The case the review of #480 found. On a resource whose devices provide more
// than one of it, a claim of exactly what one device provides is still a
// whole-device claim, and saying otherwise granted the same card twice: two
// containers each asking for ten of a two-device, ten-seat resource both took
// the first device and the second was never handed out.
//
// Whether a placement is whole is decided here and reported, so the caller
// cannot hold a second opinion about it.
TEST(GpuPlacementTest, AWholeDeviceClaimOnASeatResourceIsReportedAsWhole)
{
  const UnitSet unit = unitOf("gpu_replica", 10, {"GPU-a", "GPU-b"});

  Try<Placement> first = medios::modules::gpu::place(unit, 10.0, {});
  ASSERT_SOME(first);
  EXPECT_TRUE(first->whole);
  ASSERT_EQ(1u, first->devices.size());
  EXPECT_EQ("GPU-a", first->devices.at(0).uuid);

  // With the first recorded, the second container gets the other device.
  Try<Placement> second =
    medios::modules::gpu::place(unit, 10.0, {"GPU-a"});
  ASSERT_SOME(second);
  EXPECT_TRUE(second->whole);
  ASSERT_EQ(1u, second->devices.size());
  EXPECT_EQ("GPU-b", second->devices.at(0).uuid);

  // And the third is refused rather than given a card someone holds.
  Try<Placement> third =
    medios::modules::gpu::place(unit, 10.0, {"GPU-a", "GPU-b"});
  ASSERT_ERROR(third);
  EXPECT_TRUE(strings::contains(third.error(), "free")) << third.error();
}


// DR-035 worked example 4, the half of it this module answers. A claim over
// what the declared devices provide, and a fractional seat, are refused by
// name; a claim over the memory pool is core's refusal and never arrives
// here.
TEST(GpuPlacementTest, AClaimOverTheSeatsOrBetweenThemIsRefusedByName)
{
  const UnitSet unit = unitOf("gpu_replica", 10, {"GPU-a"});

  Try<Placement> over = medios::modules::gpu::place(unit, 11.0, {});
  ASSERT_ERROR(over);
  EXPECT_TRUE(strings::contains(over.error(), "gpu_replica")) << over.error();
  EXPECT_TRUE(strings::contains(over.error(), "11")) << over.error();

  Try<Placement> fraction = medios::modules::gpu::place(unit, 0.5, {});
  ASSERT_ERROR(fraction);
  EXPECT_TRUE(strings::contains(fraction.error(), "whole number"))
    << fraction.error();

  // The same rule on an exclusive unit, where half a device has never been
  // something this module could deliver.
  Try<Placement> half =
    medios::modules::gpu::place(unitOf("gpus", 1, {"GPU-a"}), 0.5, {});
  ASSERT_ERROR(half);
  EXPECT_TRUE(strings::contains(half.error(), "whole number")) << half.error();
}


// A claim of nothing is not placed. prepare() filters these out before they
// reach here, and answering one with a device would grant hardware to a
// container that asked for none.
TEST(GpuPlacementTest, AClaimOfNothingIsNotPlaced)
{
  EXPECT_ERROR(
      medios::modules::gpu::place(unitOf("gpus", 1, {"GPU-a"}), 0.0, {}));
}


// A claim far larger than any double can carry as a whole number is refused
// for what is wrong with it, which is that no device on this host provides
// it, rather than for its shape. The size test itself is bounded, so no
// claim reaches a conversion that is not defined for it.
TEST(GpuPlacementTest, AClaimTooLargeToBeAWholeNumberIsRefusedForItsSize)
{
  Try<Placement> huge =
    medios::modules::gpu::place(unitOf("gpus", 1, {"GPU-a"}), 1e30, {});

  ASSERT_ERROR(huge);
  EXPECT_TRUE(strings::contains(huge.error(), "provide")) << huge.error();

  EXPECT_TRUE(medios::modules::gpu::isWholeClaim(1.0));
  EXPECT_TRUE(medios::modules::gpu::isWholeClaim(10.0));
  EXPECT_FALSE(medios::modules::gpu::isWholeClaim(0.5));
  EXPECT_FALSE(medios::modules::gpu::isWholeClaim(1.001));
  EXPECT_FALSE(medios::modules::gpu::isWholeClaim(-1.0));
  EXPECT_FALSE(medios::modules::gpu::isWholeClaim(1e30));
}


// A share of one device, on a resource declared over several, is refused
// until DR-035's span rule says which device it lands on (medios#479). The
// module records no seat, so it cannot spread holders, and sending every one
// of them to the first device would leave the rest idle while core counted
// those seats as given out.
TEST(GpuPlacementTest, APartialSeatOverSeveralDevicesIsRefusedRatherThanGuessed)
{
  const UnitSet unit = unitOf("gpu_replica", 10, {"GPU-a", "GPU-b"});

  Try<Placement> partial = medios::modules::gpu::place(unit, 4.0, {});
  ASSERT_ERROR(partial);
  EXPECT_TRUE(strings::contains(partial.error(), "not decided"))
    << partial.error();

  // A whole multiple of what one device provides is decided, and is served.
  Try<Placement> whole = medios::modules::gpu::place(unit, 20.0, {});
  ASSERT_SOME(whole);
  EXPECT_TRUE(whole->whole);
  EXPECT_EQ(2u, whole->devices.size());
}


// --- reading an assignment back by identity ----------------------------------
//
// A checkpoint holds device UUIDs. Both isolators turn those UUIDs back into
// devices with `devicesByUuid()`, which looks up this process's index and minor
// number for each one: the assignment isolator in recover() after a node
// restart (medios#471), and the device isolator in every prepare().

// The devices come back in the order the UUIDs were written, which is the
// order they were assigned, not the order the units file lists them.
TEST(GpuUnitsTest, DevicesByUuidKeepsTheAssignedOrder)
{
  const UnitSet unit = unitOf("gpus", 1, {"GPU-a", "GPU-b", "GPU-c"});

  Try<vector<Device>> devices =
    medios::modules::gpu::devicesByUuid(unit, {"GPU-c", "GPU-a"});

  ASSERT_SOME(devices);
  ASSERT_EQ(2u, devices->size());
  EXPECT_EQ("GPU-c", devices->at(0).uuid);
  EXPECT_EQ(2u, devices->at(0).minorNumber);
  EXPECT_EQ("GPU-a", devices->at(1).uuid);
  EXPECT_EQ(0u, devices->at(1).minorNumber);
}


// A UUID the unit does not declare refuses the whole set, naming the UUID and
// the resource, rather than returning the devices it did find.
TEST(GpuUnitsTest, DevicesByUuidRefusesAnUndeclaredDeviceByName)
{
  const UnitSet unit = unitOf("gpus", 1, {"GPU-a"});

  Try<vector<Device>> devices =
    medios::modules::gpu::devicesByUuid(unit, {"GPU-a", "GPU-gone"});

  ASSERT_ERROR(devices);
  EXPECT_TRUE(strings::contains(devices.error(), "GPU-gone")) << devices.error();
  EXPECT_TRUE(strings::contains(devices.error(), "'gpus'")) << devices.error();
}


// --- CUDA_VISIBLE_DEVICES ----------------------------------------------------

// The assignment isolator names devices by UUID, comma-separated, in the
// order they were assigned. Indices would be wrong wherever CUDA's default
// fastest-first order differs from NVML's.
TEST(GpuDeliveryTest, VisibleDevicesNamesTheAssignedUuidsInOrder)
{
  EXPECT_EQ(
      "GPU-b",
      medios::modules::gpu::visibleDevices(hostOf({"GPU-b"})));

  EXPECT_EQ(
      "GPU-b,GPU-a",
      medios::modules::gpu::visibleDevices(hostOf({"GPU-b", "GPU-a"})));
}


// An empty list of devices gives an empty value. The assignment isolator does
// not set the variable for a container with no GPU claim, so this case only
// fixes the helper's output for an empty list.
TEST(GpuDeliveryTest, VisibleDevicesOfNothingIsEmpty)
{
  EXPECT_EQ("", medios::modules::gpu::visibleDevices({}));
}


// --- the allocation checkpoint -----------------------------------------------
//
// The assignment isolator writes a checkpoint for each container it assigns.
// Its recover() reads the checkpoints to rebuild the ledger after a node
// restart (medios#471), and the device isolator reads a container's
// checkpoint in prepare() to learn which devices to declare.
//
// The read and write functions are free functions, as `place()` is, because
// both isolators are Linux-only classes defined inside the module's
// translation unit. A test reaches the isolators only through the module
// manager on a GPU host. These cases write to a real scratch directory rather
// than a fake filesystem, because what they check is a round trip through disk
// between a writer and a later reader.

namespace {

using medios::modules::gpu::AllocationCheckpoint;

// A fresh directory nothing else in this process has touched, standing in
// for one container's sandbox.
std::string scratchContainerDirectory()
{
  Try<std::string> dir =
    os::mkdtemp(path::join(os::getcwd(), "XXXXXX"));
  CHECK_SOME(dir);
  return *dir;
}

} // namespace {


// The ordinary case: what prepare() writes is what recover() reads back,
// exactly.
TEST(GpuCheckpointTest, WhatIsWrittenIsWhatIsReadBack)
{
  const std::string directory = scratchContainerDirectory();

  const AllocationCheckpoint written{
      "gpus", 2.0, {"GPU-a", "GPU-b"}, true};

  ASSERT_SOME(medios::modules::gpu::writeAllocationCheckpoint(
      directory, written));

  Try<Option<AllocationCheckpoint>> read =
    medios::modules::gpu::readAllocationCheckpoint(directory);

  ASSERT_SOME(read);
  ASSERT_SOME(*read);
  EXPECT_EQ("gpus", (*read)->resource);
  EXPECT_EQ(2.0, (*read)->claim);
  EXPECT_EQ(
      (std::vector<std::string>{"GPU-a", "GPU-b"}), (*read)->deviceUuids);
  EXPECT_TRUE((*read)->whole);
}


// A seat grant round-trips the same way, with `whole` false: recover() has
// to know not to bind a device a seat holder never bound in the first place.
TEST(GpuCheckpointTest, ASeatGrantRoundTripsAsNotWhole)
{
  const std::string directory = scratchContainerDirectory();

  ASSERT_SOME(medios::modules::gpu::writeAllocationCheckpoint(
      directory, AllocationCheckpoint{"gpu_replica", 3.0, {"GPU-a"}, false}));

  Try<Option<AllocationCheckpoint>> read =
    medios::modules::gpu::readAllocationCheckpoint(directory);

  ASSERT_SOME(read);
  ASSERT_SOME(*read);
  EXPECT_FALSE((*read)->whole);
}


// A container that never held a GPU claim has nothing checkpointed. `None`,
// not an error: this is the ordinary case for every non-GPU container on a
// GPU node, and for one this module declined at prepare.
TEST(GpuCheckpointTest, NoCheckpointIsNoneRatherThanAnError)
{
  const std::string directory = scratchContainerDirectory();

  Try<Option<AllocationCheckpoint>> read =
    medios::modules::gpu::readAllocationCheckpoint(directory);

  ASSERT_SOME(read);
  EXPECT_NONE(read.get());
}


// A file that exists but is not the shape this module writes is a checkpoint
// disagreeing with the code that reads it, and is refused by name rather than
// treated as "nothing checkpointed".
TEST(GpuCheckpointTest, AFileThatIsNotTheExpectedShapeIsRefused)
{
  const std::string directory = scratchContainerDirectory();

  ASSERT_SOME(os::write(
      medios::modules::gpu::allocationCheckpointPath(directory),
      "not json at all"));

  Try<Option<AllocationCheckpoint>> notJson =
    medios::modules::gpu::readAllocationCheckpoint(directory);
  ASSERT_ERROR(notJson);

  ASSERT_SOME(os::write(
      medios::modules::gpu::allocationCheckpointPath(directory),
      "{\"resource\": \"gpus\", \"claim\": 1.0, \"whole\": true}"));

  Try<Option<AllocationCheckpoint>> noDevices =
    medios::modules::gpu::readAllocationCheckpoint(directory);
  ASSERT_ERROR(noDevices);
  EXPECT_TRUE(strings::contains(noDevices.error(), "device_uuids"))
    << noDevices.error();
}


// A whole-device grant checkpointed with no devices is not a shape this
// module ever writes, and is refused rather than read back as a survivor
// holding nothing.
TEST(GpuCheckpointTest, AnEmptyDeviceListIsRefused)
{
  const std::string directory = scratchContainerDirectory();

  ASSERT_SOME(os::write(
      medios::modules::gpu::allocationCheckpointPath(directory),
      "{\"resource\": \"gpus\", \"claim\": 1.0, \"device_uuids\": [], "
      "\"whole\": true}"));

  Try<Option<AllocationCheckpoint>> read =
    medios::modules::gpu::readAllocationCheckpoint(directory);

  ASSERT_ERROR(read);
  EXPECT_TRUE(strings::contains(read.error(), "no devices")) << read.error();
}


// cleanup() removes the checkpoint; a second removal (the ordinary case for a
// container this module declined, which never wrote one) is not an error.
TEST(GpuCheckpointTest, RemoveIsIdempotentAndMissingIsNotAnError)
{
  const std::string directory = scratchContainerDirectory();

  ASSERT_SOME(medios::modules::gpu::removeAllocationCheckpoint(directory));

  ASSERT_SOME(medios::modules::gpu::writeAllocationCheckpoint(
      directory, AllocationCheckpoint{"gpus", 1.0, {"GPU-a"}, true}));

  ASSERT_SOME(medios::modules::gpu::removeAllocationCheckpoint(directory));

  Try<Option<AllocationCheckpoint>> read =
    medios::modules::gpu::readAllocationCheckpoint(directory);
  ASSERT_SOME(read);
  EXPECT_NONE(read.get());

  // Removing what is already gone is the cleanup() case for a container this
  // module never granted anything to, and must not be refused.
  ASSERT_SOME(medios::modules::gpu::removeAllocationCheckpoint(directory));
}


} // namespace tests {
} // namespace internal {
} // namespace medios {
