// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

// filesystem/posix delivers a STORE volume as a symlink at one name in the
// sandbox (DR-036, medios#602). The declaration and the isolator's own
// state are checked against a synthetic config; the launches run the
// posix launcher with medios-containerizer and medios-fetcher built beside
// the test, on Linux and macOS alike, and need no root.

#include <functional>
#include <map>
#include <string>
#include <utility>
#include <vector>

#include <gmock/gmock.h>

#include <gtest/gtest.h>

#include <medios/medios.hpp>
#include <medios/scheduler.hpp>

#include <medios/controller/detector.hpp>

#include <medios/node/containerizer.hpp>
#include <medios/node/isolator.hpp>

#include <process/clock.hpp>
#include <process/future.hpp>
#include <process/gmock.hpp>
#include <process/gtest.hpp>
#include <process/owned.hpp>

#include <stout/foreach.hpp>
#include <stout/fs.hpp>
#include <stout/gtest.hpp>
#include <stout/hashset.hpp>
#include <stout/os.hpp>
#include <stout/os/realpath.hpp>
#include <stout/os/rmdir.hpp>
#include <stout/os/stat.hpp>
#include <stout/path.hpp>
#include <stout/strings.hpp>
#include <stout/try.hpp>
#include <stout/uri.hpp>
#include <stout/uuid.hpp>

#include <stout/tests/utils.hpp>

#include "node/flags.hpp"
#include "node/paths.hpp"

#include "node/containerizer/medios/containerizer.hpp"
#include "node/containerizer/medios/isolators/filesystem/posix.hpp"

#include "tests/medios.hpp"

using medios::controller::detector::ControllerDetector;

using medios::node::ContainerConfig;
using medios::node::ContainerFileOperation;
using medios::node::ContainerLaunchInfo;
using medios::node::ContainerLimitation;
using medios::node::ContainerState;
using medios::node::Isolator;

using process::Clock;
using process::Future;
using process::Owned;

using std::string;
using std::vector;

using testing::_;
using testing::Return;

namespace medios {
namespace internal {
namespace tests {

class PosixFilesystemStoreTest : public TemporaryDirectoryTest
{
protected:
  void SetUp() override
  {
    TemporaryDirectoryTest::SetUp();

    // Compared with what the isolator resolved: on macOS the temporary
    // directory is under /tmp, a link to /private/tmp.
    Result<string> resolved = os::realpath(sandbox.get());
    ASSERT_SOME(resolved);
    root = resolved.get();

    tablesPath = path::join(root, "tables");
    ASSERT_SOME(os::mkdir(tablesPath));
    ASSERT_SOME(os::write(path::join(tablesPath, "canary"), "canary"));

    directory = path::join(root, "sandbox");
    ASSERT_SOME(os::mkdir(directory));

    writeStores(tablesPath, 0);
    createIsolator();

    containerId.set_value("with-store");

    config.set_directory(directory);
    config.mutable_command_info()->set_value("true");
    config.mutable_container_info()->set_type(ContainerInfo::MEDIOS);
  }

  void writeStores(const string& path, size_t readers)
  {
    const string file = path::join(root, "stores.json");
    ASSERT_SOME(os::write(file,
        "{ \"stores\": [ { \"name\": \"tables\", \"path\": \"" + path +
        "\", \"class\": \"local\", \"locality\": \"near\", \"readers\": " +
        stringify(readers) + ", \"writers\": 1 } ] }"));
    flags.stores = file;
  }

  void createIsolator()
  {
    Try<Isolator*> created =
      node::PosixFilesystemIsolatorProcess::create(flags, nullptr);
    ASSERT_SOME(created);
    isolator.reset(created.get());
  }

  Volume* addVolume(const string& containerPath, Volume::Mode mode)
  {
    Volume* volume = config.mutable_container_info()->add_volumes();
    volume->set_container_path(containerPath);
    volume->set_mode(mode);
    volume->mutable_source()->set_type(Volume::Source::STORE);
    volume->mutable_source()->mutable_store()->set_name("tables");
    return volume;
  }

  // The state the node checkpoints for a top-level container launched
  // with `config`.
  ContainerState stateOf() const
  {
    ContainerState state;
    state.mutable_executor_info()->mutable_container()->CopyFrom(
        config.container_info());
    state.mutable_container_id()->CopyFrom(containerId);
    state.set_pid(1);
    state.set_directory(directory);
    return state;
  }

  node::Flags flags;
  string root;
  string tablesPath;
  string directory;
  Owned<Isolator> isolator;
  ContainerID containerId;
  ContainerConfig config;
};


// What the launcher does with the isolator's declaration, which runs
// after the fetcher.
static void applyLinks(const ContainerLaunchInfo& launchInfo)
{
  foreach (const ContainerFileOperation& op, launchInfo.file_operations()) {
    ASSERT_EQ(ContainerFileOperation::SYMLINK, op.operation());
    ASSERT_SOME(::fs::symlink(op.symlink().source(), op.symlink().target()));
  }
}


// One name inside the sandbox is a link to the store, made by the
// launcher, and listed for it so a fetched file at the name refuses the
// launch.
TEST_F(PosixFilesystemStoreTest, LinksARelativeStoreInTheSandbox)
{
  addVolume("tables", Volume::RW);

  Future<Option<ContainerLaunchInfo>> launchInfo =
    isolator->prepare(containerId, config);
  AWAIT_READY(launchInfo);
  ASSERT_SOME(launchInfo.get());

  ASSERT_EQ(1, launchInfo->get().file_operations_size());
  const ContainerFileOperation& op = launchInfo->get().file_operations(0);
  EXPECT_EQ(ContainerFileOperation::SYMLINK, op.operation());
  EXPECT_EQ(tablesPath, op.symlink().source());
  EXPECT_EQ(path::join(directory, "tables"), op.symlink().target());

  ASSERT_EQ(1, launchInfo->get().placements_size());
  EXPECT_EQ(
      path::join(directory, "tables"), launchInfo->get().placements(0).path());
  EXPECT_EQ(
      "Volume for store 'tables'", launchInfo->get().placements(0).what());

  // Nothing is made at prepare; the fetcher has not run yet.
  EXPECT_FALSE(os::exists(path::join(directory, "tables")));
}


// The link points where the declared path resolved when the node
// started, so a store declared through a symlink is linked to the
// directory itself.
TEST_F(PosixFilesystemStoreTest, TheLinkPointsWhereTheStoreResolvedAtStart)
{
  const string alias = path::join(root, "tables-alias");
  ASSERT_SOME(::fs::symlink(tablesPath, alias));

  writeStores(alias, 0);
  createIsolator();
  addVolume("tables", Volume::RW);

  Future<Option<ContainerLaunchInfo>> launchInfo =
    isolator->prepare(containerId, config);
  AWAIT_READY(launchInfo);
  ASSERT_SOME(launchInfo.get());
  ASSERT_EQ(1, launchInfo->get().file_operations_size());
  EXPECT_EQ(tablesPath, launchInfo->get().file_operations(0).symlink().source());
}


// A declared path that resolves somewhere else at launch than at start
// is refused, naming both places.
TEST_F(PosixFilesystemStoreTest, RefusesAStoreThatResolvesElsewhereNow)
{
  const string alias = path::join(root, "tables-alias");
  ASSERT_SOME(::fs::symlink(tablesPath, alias));

  writeStores(alias, 0);
  createIsolator();

  const string other = path::join(root, "other");
  ASSERT_SOME(os::mkdir(other));
  ASSERT_SOME(os::rm(alias));
  ASSERT_SOME(::fs::symlink(other, alias));

  addVolume("tables", Volume::RW);

  Future<Option<ContainerLaunchInfo>> launchInfo =
    isolator->prepare(containerId, config);
  AWAIT_FAILED(launchInfo);
  EXPECT_TRUE(strings::contains(
      launchInfo.failure(),
      "resolved to '" + tablesPath + "' when the node started and resolves "
      "to '" + other + "' now"))
    << launchInfo.failure();
}


TEST_F(PosixFilesystemStoreTest, RefusesAStoreThatNoLongerResolves)
{
  const string alias = path::join(root, "tables-alias");
  ASSERT_SOME(::fs::symlink(tablesPath, alias));

  writeStores(alias, 0);
  createIsolator();
  ASSERT_SOME(os::rm(alias));

  addVolume("tables", Volume::RW);

  Future<Option<ContainerLaunchInfo>> launchInfo =
    isolator->prepare(containerId, config);
  AWAIT_FAILED(launchInfo);
  EXPECT_TRUE(strings::contains(launchInfo.failure(), "no longer resolves"))
    << launchInfo.failure();
}


// Without a mount namespace there is nowhere to put a store at an
// absolute path but the node's own filesystem.
TEST_F(PosixFilesystemStoreTest, RefusesAnAbsolutePath)
{
  addVolume("/data", Volume::RW);

  Future<Option<ContainerLaunchInfo>> launchInfo =
    isolator->prepare(containerId, config);
  AWAIT_FAILED(launchInfo);
  EXPECT_TRUE(strings::contains(
      launchInfo.failure(),
      "asks for absolute container path '/data'; this node places stores "
      "only inside the sandbox"))
    << launchInfo.failure();
}


TEST_F(PosixFilesystemStoreTest, RefusesARelativePathThatIsNotOneName)
{
  foreach (const string& containerPath,
           vector<string>({"data/tables", "..", "."})) {
    config.mutable_container_info()->clear_volumes();
    addVolume(containerPath, Volume::RW);

    ContainerID id;
    id.set_value("not-one-name-" + stringify(containerPath.size()));

    Future<Option<ContainerLaunchInfo>> launchInfo =
      isolator->prepare(id, config);
    AWAIT_FAILED(launchInfo);
    EXPECT_TRUE(strings::contains(
        launchInfo.failure(), "is one name inside the sandbox"))
      << containerPath << ": " << launchInfo.failure();
  }
}


// A link cannot make a store read-only, so a read-only volume is refused
// rather than quietly handed out writable.
TEST_F(PosixFilesystemStoreTest, RefusesReadOnly)
{
  addVolume("tables", Volume::RO);

  Future<Option<ContainerLaunchInfo>> launchInfo =
    isolator->prepare(containerId, config);
  AWAIT_FAILED(launchInfo);
  EXPECT_TRUE(strings::contains(
      launchInfo.failure(), "asks for read-only access"))
    << launchInfo.failure();
}


TEST_F(PosixFilesystemStoreTest, RefusesAnUndeclaredStoreAndAHostPath)
{
  Volume* undeclared = addVolume("other", Volume::RW);
  undeclared->mutable_source()->mutable_store()->set_name("other");

  Future<Option<ContainerLaunchInfo>> launchInfo =
    isolator->prepare(containerId, config);
  AWAIT_FAILED(launchInfo);
  EXPECT_TRUE(strings::contains(
      launchInfo.failure(), "names store 'other', which this node does not "
      "declare"))
    << launchInfo.failure();

  config.mutable_container_info()->clear_volumes();
  Volume* host = config.mutable_container_info()->add_volumes();
  host->set_container_path("tables");
  host->set_host_path(tablesPath);
  host->set_mode(Volume::RW);

  ContainerID hostId;
  hostId.set_value("host-path");

  launchInfo = isolator->prepare(hostId, config);
  AWAIT_FAILED(launchInfo);
  EXPECT_TRUE(strings::contains(launchInfo.failure(), "source type HOST_PATH"))
    << launchInfo.failure();
}


TEST_F(PosixFilesystemStoreTest, RefusesARootfs)
{
  config.set_rootfs(path::join(root, "rootfs"));

  Future<Option<ContainerLaunchInfo>> launchInfo =
    isolator->prepare(containerId, config);
  AWAIT_FAILED(launchInfo);
  EXPECT_TRUE(strings::contains(
      launchInfo.failure(), "Container root filesystems not supported"))
    << launchInfo.failure();
}


// A node with this isolator cannot honour a read-only claim, so a store
// declaring readers is refused when the node starts.
TEST_F(PosixFilesystemStoreTest, AStoreWithReadersRefusesTheIsolator)
{
  writeStores(tablesPath, 2);

  Try<Isolator*> created =
    node::PosixFilesystemIsolatorProcess::create(flags, nullptr);
  ASSERT_ERROR(created);
  EXPECT_EQ(
      "Store 'tables' declares readers: 2; filesystem/posix cannot make a "
      "store read-only, so a store on this node declares readers: 0",
      created.error());
}


TEST_F(PosixFilesystemStoreTest, NoVolumesDeclaresNoLinks)
{
  Future<Option<ContainerLaunchInfo>> launchInfo =
    isolator->prepare(containerId, config);
  AWAIT_READY(launchInfo);
  ASSERT_SOME(launchInfo.get());
  EXPECT_EQ(0, launchInfo->get().file_operations_size());
  EXPECT_EQ(0, launchInfo->get().placements_size());
}


// A nested container is prepared too: skipped, its store would go
// undelivered with nothing said.
TEST_F(PosixFilesystemStoreTest, LinksAStoreForANestedContainer)
{
  ContainerID nested;
  nested.set_value("nested");
  nested.mutable_parent()->CopyFrom(containerId);

  addVolume("tables", Volume::RW);

  Future<Option<ContainerLaunchInfo>> launchInfo =
    isolator->prepare(nested, config);
  AWAIT_READY(launchInfo);
  ASSERT_SOME(launchInfo.get());
  ASSERT_EQ(1, launchInfo->get().file_operations_size());
  EXPECT_EQ(
      path::join(directory, "tables"),
      launchInfo->get().file_operations(0).symlink().target());
}


// Cleanup removes the link it made and nothing behind it.
TEST_F(PosixFilesystemStoreTest, CleanupRemovesTheLinkAndLeavesTheStore)
{
  addVolume("tables", Volume::RW);

  Future<Option<ContainerLaunchInfo>> launchInfo =
    isolator->prepare(containerId, config);
  AWAIT_READY(launchInfo);
  ASSERT_SOME(launchInfo.get());
  applyLinks(launchInfo->get());

  const string link = path::join(directory, "tables");
  ASSERT_TRUE(os::stat::islink(link));

  AWAIT_READY(isolator->cleanup(containerId));

  EXPECT_FALSE(os::exists(link));
  EXPECT_SOME_EQ("canary", os::read(path::join(tablesPath, "canary")));
}


// A file the fetcher left at the name, which refused the launch, is the
// task's and stays.
TEST_F(PosixFilesystemStoreTest, CleanupLeavesAFileAtTheName)
{
  addVolume("tables", Volume::RW);

  Future<Option<ContainerLaunchInfo>> launchInfo =
    isolator->prepare(containerId, config);
  AWAIT_READY(launchInfo);

  ASSERT_SOME(os::write(path::join(directory, "tables"), "fetched"));

  AWAIT_READY(isolator->cleanup(containerId));

  EXPECT_SOME_EQ("fetched", os::read(path::join(directory, "tables")));
}


// Removing a sandbox removes the link without following it: the sandbox
// GC is what removes an orphan's links.
TEST_F(PosixFilesystemStoreTest, RemovingTheSandboxLeavesTheStore)
{
  ASSERT_SOME(::fs::symlink(tablesPath, path::join(directory, "tables")));

  ASSERT_SOME(os::rmdir(directory));

  EXPECT_FALSE(os::exists(directory));
  EXPECT_SOME_EQ("canary", os::read(path::join(tablesPath, "canary")));
}


// A container recovered with its link still pointing where the store
// resolves keeps running.
TEST_F(PosixFilesystemStoreTest, RecoveryKeepsAContainerWhoseLinkMatches)
{
  addVolume("tables", Volume::RW);
  ASSERT_SOME(::fs::symlink(tablesPath, path::join(directory, "tables")));

  AWAIT_READY(isolator->recover({stateOf()}, hashset<ContainerID>()));

  Future<ContainerLimitation> limitation = isolator->watch(containerId);

  // Settled, so the watch has been answered if it is going to be.
  Clock::pause();
  Clock::settle();
  EXPECT_TRUE(limitation.isPending());
  Clock::resume();

  AWAIT_READY(isolator->cleanup(containerId));
  EXPECT_FALSE(os::exists(path::join(directory, "tables")));
}


// A link made before the node restarted, to where the store resolved
// then, is a limitation once the store resolves elsewhere: the container
// is stopped rather than left writing to the old place.
TEST_F(PosixFilesystemStoreTest, RecoveryStopsAContainerWhoseStoreMoved)
{
  const string old = path::join(root, "old-tables");
  ASSERT_SOME(os::mkdir(old));

  addVolume("tables", Volume::RW);
  ASSERT_SOME(::fs::symlink(old, path::join(directory, "tables")));

  AWAIT_READY(isolator->recover({stateOf()}, hashset<ContainerID>()));

  Future<ContainerLimitation> limitation = isolator->watch(containerId);
  AWAIT_READY(limitation);
  EXPECT_EQ(TaskStatus::REASON_CONTAINER_LIMITATION, limitation->reason());
  EXPECT_TRUE(strings::contains(limitation->message(), "points at '" + old))
    << limitation->message();
  EXPECT_TRUE(strings::contains(
      limitation->message(), "now resolves to '" + tablesPath + "'"))
    << limitation->message();

  // The link the container ran with is still the one removed.
  AWAIT_READY(isolator->cleanup(containerId));
  EXPECT_FALSE(os::exists(path::join(directory, "tables")));
  EXPECT_TRUE(os::stat::isdir(old));
}


TEST_F(PosixFilesystemStoreTest, RecoveryStopsAContainerWhoseStoreIsGone)
{
  addVolume("tables", Volume::RW);
  ASSERT_SOME(::fs::symlink(tablesPath, path::join(directory, "tables")));

  // A node declares at least one store, so the restarted node declares
  // another one in place of `tables`.
  const string other = path::join(root, "other");
  ASSERT_SOME(os::mkdir(other));
  ASSERT_SOME(os::write(flags.stores.get(),
      "{ \"stores\": [ { \"name\": \"other\", \"path\": \"" + other +
      "\", \"class\": \"local\", \"locality\": \"near\", "
      "\"readers\": 0, \"writers\": 1 } ] }"));
  createIsolator();

  AWAIT_READY(isolator->recover({stateOf()}, hashset<ContainerID>()));

  Future<ContainerLimitation> limitation = isolator->watch(containerId);
  AWAIT_READY(limitation);
  EXPECT_TRUE(strings::contains(
      limitation->message(), "this node no longer declares the store"))
    << limitation->message();
}


// The launches: the posix launcher makes the link after the fetcher, a
// write through it lands in the store, and the link is gone once the
// container is.
class PosixFilesystemStoreLaunchTest
  : public ContainerizerTest<node::MediosContainerizer>
{
protected:
  void SetUp() override
  {
    ContainerizerTest<node::MediosContainerizer>::SetUp();

    Result<string> resolved = os::realpath(sandbox.get());
    ASSERT_SOME(resolved);

    storePath = path::join(resolved.get(), "tables-store");
    ASSERT_SOME(os::mkdir(storePath));
    ASSERT_SOME(os::write(path::join(storePath, "canary"), "canary"));
  }

  node::Flags CreateNodeFlags() override
  {
    node::Flags flags =
      ContainerizerTest<node::MediosContainerizer>::CreateNodeFlags();
    flags.launcher = "posix";
    flags.isolation = "filesystem/posix";

    const string file = path::join(sandbox.get(), "stores.json");
    CHECK_SOME(os::write(file,
        "{ \"stores\": [ { \"name\": \"tables\", \"path\": \"" +
        storePath + "\", \"class\": \"local\", \"locality\": \"near\", "
        "\"readers\": 0, \"writers\": 1 } ] }"));
    flags.stores = file;

    return flags;
  }

  static Volume storeVolume(const string& containerPath)
  {
    Volume volume;
    volume.set_container_path(containerPath);
    volume.set_mode(Volume::RW);
    volume.mutable_source()->set_type(Volume::Source::STORE);
    volume.mutable_source()->mutable_store()->set_name("tables");
    return volume;
  }

  // Launches `executor` in a fresh sandbox, the sandbox directory in
  // `directory`, and waits for it; its exit status goes in `status`. Not
  // local, as on a node: the launcher writes to the sandbox's stderr.
  void run(
      const ExecutorInfo& executor,
      string* directory,
      Option<int>* status,
      const std::function<void(const string&)>& before = nullptr)
  {
    const node::Flags flags = CreateNodeFlags();

    Try<node::MediosContainerizer*> create =
      node::MediosContainerizer::create(flags, false);
    ASSERT_SOME(create);

    Owned<node::Containerizer> containerizer(create.get());

    ContainerID containerId;
    containerId.set_value(id::UUID::random().toString());

    *directory = path::join(flags.work_dir, containerId.value());
    ASSERT_SOME(os::mkdir(*directory));

    // Whatever the test puts in the sandbox before the launch.
    if (before) {
      before(*directory);
    }

    Future<node::Containerizer::LaunchResult> launch = containerizer->launch(
        containerId,
        createContainerConfig(None(), executor, *directory),
        std::map<string, string>(),
        None());

    AWAIT_ASSERT_EQ(node::Containerizer::LaunchResult::SUCCESS, launch);

    Future<Option<medios::node::ContainerTermination>> wait =
      containerizer->wait(containerId);

    AWAIT_READY(wait);
    ASSERT_SOME(wait.get());
    ASSERT_TRUE(wait->get().has_status());

    *status = wait->get().status();
  }

  string storePath;
};


TEST_F(PosixFilesystemStoreLaunchTest, AStoreIsLinkedInTheSandbox)
{
  ExecutorInfo executor = createExecutorInfo(
      "test_executor", "cat tables/canary && echo written > tables/marker");
  *executor.mutable_container()->add_volumes() = storeVolume("tables");
  executor.mutable_container()->set_type(ContainerInfo::MEDIOS);

  string directory;
  Option<int> status;
  run(executor, &directory, &status);
  ASSERT_SOME(status);
  EXPECT_WEXITSTATUS_EQ(0, status.get());

  EXPECT_SOME_EQ("written\n", os::read(path::join(storePath, "marker")));
  EXPECT_FALSE(os::exists(path::join(directory, "tables")));
}


// The fetcher runs before the launcher, so a URI whose file lands at the
// store's name refuses the launch with the sentence in the sandbox's
// stderr; the fetched file stays and the store gets no write.
TEST_F(PosixFilesystemStoreLaunchTest, AFetchedFileAtTheNameRefuses)
{
  const string from = path::join(sandbox.get(), "from");
  ASSERT_SOME(os::mkdir(from));
  ASSERT_SOME(os::write(path::join(from, "tables"), "fetched"));

  ExecutorInfo executor = createExecutorInfo(
      "test_executor", "echo written > tables/marker");
  CommandInfo::URI* uri = executor.mutable_command()->add_uris();
  uri->set_value(uri::from_path(path::join(from, "tables")));
  uri->set_extract(false);
  *executor.mutable_container()->add_volumes() = storeVolume("tables");
  executor.mutable_container()->set_type(ContainerInfo::MEDIOS);

  string directory;
  Option<int> status;
  run(executor, &directory, &status);
  ASSERT_SOME(status);
  EXPECT_WEXITSTATUS_NE(0, status.get());

  EXPECT_SOME_EQ("fetched", os::read(path::join(directory, "tables")));
  EXPECT_FALSE(os::exists(path::join(storePath, "marker")));

  Try<string> output = os::read(path::join(directory, "stderr"));
  ASSERT_SOME(output);
  EXPECT_TRUE(strings::contains(
      output.get(),
      "Volume for store 'tables' would be placed at '" +
      path::join(directory, "tables") + "', which already exists in the "
      "sandbox"))
    << output.get();
}


// lstat, not stat: a dangling symlink at the name is taken, and the
// launcher refuses rather than writing through it.
TEST_F(PosixFilesystemStoreLaunchTest, ADanglingLinkAtTheNameRefuses)
{
  ExecutorInfo executor = createExecutorInfo(
      "test_executor", "echo written > tables/marker");
  *executor.mutable_container()->add_volumes() = storeVolume("tables");
  executor.mutable_container()->set_type(ContainerInfo::MEDIOS);

  const string nowhere = path::join(sandbox.get(), "nowhere");

  string directory;
  Option<int> status;
  run(executor, &directory, &status, [&](const string& sandbox) {
    ASSERT_SOME(::fs::symlink(nowhere, path::join(sandbox, "tables")));
  });
  ASSERT_SOME(status);
  EXPECT_WEXITSTATUS_NE(0, status.get());

  EXPECT_TRUE(os::stat::islink(path::join(directory, "tables")));
  EXPECT_FALSE(os::exists(nowhere));
  EXPECT_FALSE(os::exists(path::join(storePath, "marker")));

  Try<string> output = os::read(path::join(directory, "stderr"));
  ASSERT_SOME(output);
  EXPECT_TRUE(strings::contains(
      output.get(),
      "Volume for store 'tables' would be placed at '" +
      path::join(directory, "tables") + "', which already exists in the "
      "sandbox"))
    << output.get();
}


// What a framework and an operator see when the fetcher takes the
// store's name on a node with the posix launcher: the task ends
// TASK_FAILED, and the launcher's sentence, naming the store and the
// path, is in the sandbox's stderr.
TEST_F(PosixFilesystemStoreLaunchTest, AFetchedFileAtTheNameFailsTheTask)
{
  Try<Owned<cluster::Controller>> controller = StartController();
  ASSERT_SOME(controller);

  const node::Flags flags = CreateNodeFlags();

  // Not local, as on a node: in local mode the container shares this
  // process's stderr and the sandbox's file stays empty.
  Try<node::MediosContainerizer*> _containerizer =
    node::MediosContainerizer::create(flags, false);
  ASSERT_SOME(_containerizer);
  Owned<node::MediosContainerizer> containerizer(_containerizer.get());

  Owned<ControllerDetector> detector = controller.get()->createDetector();
  Try<Owned<cluster::Node>> node =
    StartNode(detector.get(), containerizer.get(), flags);
  ASSERT_SOME(node);

  MockScheduler sched;
  MediosSchedulerDriver driver(
      &sched, DEFAULT_FRAMEWORK_INFO, controller.get()->pid, DEFAULT_CREDENTIAL);

  Future<FrameworkID> frameworkId;
  EXPECT_CALL(sched, registered(&driver, _, _))
    .WillOnce(FutureArg<1>(&frameworkId));

  Future<vector<Offer>> offers;
  EXPECT_CALL(sched, resourceOffers(&driver, _))
    .WillOnce(FutureArg<1>(&offers))
    .WillRepeatedly(Return());

  driver.start();

  AWAIT_READY(frameworkId);
  AWAIT_READY(offers);
  ASSERT_FALSE(offers->empty());
  const Offer& offer = offers->at(0);

  Resources claim = Resources::parse("cpus:0.1;mem:32").get();
  claim.allocate(offer.resources(0).allocation_info().role());
  foreach (const Resource& resource, offer.resources()) {
    if (resource.name() == "store/tables") {
      claim += resource;
    }
  }

  const string from = path::join(sandbox.get(), "from");
  ASSERT_SOME(os::mkdir(from));
  ASSERT_SOME(os::write(path::join(from, "tables"), "fetched"));

  TaskInfo task =
    createTask(offer.node_id(), claim, "echo written > tables/marker");
  CommandInfo::URI* uri = task.mutable_command()->add_uris();
  uri->set_value(uri::from_path(path::join(from, "tables")));
  uri->set_extract(false);
  task.mutable_container()->set_type(ContainerInfo::MEDIOS);
  *task.mutable_container()->add_volumes() = storeVolume("tables");

  Future<TaskStatus> failed;
  EXPECT_CALL(sched, statusUpdate(&driver, _))
    .WillRepeatedly(Return());
  EXPECT_CALL(sched, statusUpdate(&driver, TaskStatusStateEq(TASK_FAILED)))
    .WillOnce(FutureArg<1>(&failed));

  driver.launchTasks(offer.id(), {task});

  AWAIT_READY(failed);
  EXPECT_EQ(task.task_id(), failed->task_id());
  EXPECT_FALSE(os::exists(path::join(storePath, "marker")));

  // A command task's executor has the task's id.
  ExecutorID executorId;
  executorId.set_value(task.task_id().value());

  const string latest = node::paths::getExecutorLatestRunPath(
      flags.work_dir, offer.node_id(), frameworkId.get(), executorId);

  // The sentence names the run directory, which `latest` links to.
  Result<string> run = os::realpath(latest);
  ASSERT_SOME(run);
  const string containerId = Path(run.get()).basename();

  Try<string> output = os::read(path::join(latest, "stderr"));
  ASSERT_SOME(output);
  EXPECT_TRUE(strings::contains(
      output.get(), "Volume for store 'tables' would be placed at '"))
    << output.get();
  EXPECT_TRUE(strings::contains(
      output.get(),
      "/" + containerId + "/tables', which already exists in the sandbox, "
      "most likely fetched from the task's URIs; rename the volume or the "
      "fetched file"))
    << output.get();

  driver.stop();
  driver.join();
}

} // namespace tests {
} // namespace internal {
} // namespace medios {
