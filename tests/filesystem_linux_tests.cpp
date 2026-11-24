// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

// filesystem/linux prepares a provisioned rootfs: kernel filesystems, a
// private /dev with the standard nodes, and the sandbox. The declaration
// is checked on every platform against a synthetic config; the launch
// itself needs a real rootfs, cgroup v2 and root, and runs only there,
// with medios-containerizer and medios-fetcher built beside the test.

#include <functional>
#include <map>
#include <string>
#include <utility>
#include <vector>

#include <gmock/gmock.h>

#include <gtest/gtest.h>

#include <medios/resources.hpp>
#include <medios/scheduler.hpp>

#include <medios/node/containerizer.hpp>
#include <medios/node/isolator.hpp>

#include <process/future.hpp>
#include <process/gmock.hpp>
#include <process/gtest.hpp>
#include <process/owned.hpp>

#include <stout/gtest.hpp>
#include <stout/foreach.hpp>
#include <stout/fs.hpp>
#include <stout/os.hpp>
#include <stout/os/realpath.hpp>
#include <stout/path.hpp>
#include <stout/result.hpp>
#include <stout/strings.hpp>
#include <stout/try.hpp>
#include <stout/uri.hpp>

#include <stout/tests/utils.hpp>

#include "node/flags.hpp"
#include "node/paths.hpp"

#include "tests/medios.hpp"

#ifdef __linux__
#include <sched.h>
#include <sys/mount.h>

#include "node/containerizer/medios/containerizer.hpp"
#include "node/containerizer/medios/isolators/filesystem/linux.hpp"

#include "tests/containerizer/docker_archive.hpp"
#endif

using medios::controller::detector::ControllerDetector;

using medios::node::ContainerConfig;
using medios::node::ContainerLaunchInfo;
using medios::node::ContainerMountInfo;
using medios::node::ContainerPlacement;
using medios::node::Isolator;

using process::Future;
using process::Owned;

using std::pair;
using std::string;
using std::vector;

using testing::_;
using testing::Return;

namespace medios {
namespace internal {
namespace tests {

#ifdef __linux__

class LinuxFilesystemIsolatorTest : public TemporaryDirectoryTest {};


static bool hasMount(
    const ContainerLaunchInfo& launchInfo,
    const string& target,
    const Option<string>& type = None())
{
  foreach (const ContainerMountInfo& mount, launchInfo.mounts()) {
    if (mount.target() == target &&
        (type.isNone() || mount.type() == type.get())) {
      return true;
    }
  }
  return false;
}


// A container without a rootfs sees the node's filesystem and gets no
// declaration at all: no namespace, no mounts, no device nodes.
TEST_F(LinuxFilesystemIsolatorTest, NoRootfsDeclaresNothing)
{
  node::Flags flags;
  flags.runtime_dir = sandbox.get();

  Try<Isolator*> isolator = node::LinuxFilesystemIsolatorProcess::create(flags);
  ASSERT_SOME(isolator);
  Owned<Isolator> owned(isolator.get());

  ContainerID containerId;
  containerId.set_value("no-rootfs");

  ContainerConfig config;
  config.set_directory(sandbox.get());
  config.mutable_command_info()->set_value("true");

  Future<Option<ContainerLaunchInfo>> launchInfo =
    owned->prepare(containerId, config);

  AWAIT_READY(launchInfo);
  EXPECT_NONE(launchInfo.get());
}


// A container with a rootfs gets the mount namespace, /proc, /sys, a
// tmpfs /dev, devpts, /dev/shm, the host's standard device nodes bound
// in, the masked paths, and the sandbox.
TEST_F(LinuxFilesystemIsolatorTest, RootfsGetsSpecialMountsAndDevices)
{
  node::Flags flags;
  flags.runtime_dir = path::join(sandbox.get(), "runtime");
  flags.sandbox_directory = "/mnt/medios/sandbox";

  const string rootfs = path::join(sandbox.get(), "rootfs");
  ASSERT_SOME(os::mkdir(rootfs));

  Try<Isolator*> isolator = node::LinuxFilesystemIsolatorProcess::create(flags);
  ASSERT_SOME(isolator);
  Owned<Isolator> owned(isolator.get());

  ContainerID containerId;
  containerId.set_value("with-rootfs");

  ContainerConfig config;
  config.set_directory(path::join(sandbox.get(), "sandbox"));
  config.set_rootfs(rootfs);
  config.mutable_command_info()->set_value("true");

  Future<Option<ContainerLaunchInfo>> launchInfo =
    owned->prepare(containerId, config);

  AWAIT_READY(launchInfo);
  ASSERT_SOME(launchInfo.get());
  const ContainerLaunchInfo& info = launchInfo->get();

  bool clonesMountNamespace = false;
  foreach (int ns, info.clone_namespaces()) {
    if (ns == CLONE_NEWNS) {
      clonesMountNamespace = true;
    }
  }
  EXPECT_TRUE(clonesMountNamespace);

  EXPECT_TRUE(hasMount(info, path::join(rootfs, "/proc"), "proc"));
  EXPECT_TRUE(hasMount(info, path::join(rootfs, "/sys"), "sysfs"));
  EXPECT_TRUE(hasMount(info, path::join(rootfs, "/dev"), "tmpfs"));
  EXPECT_TRUE(hasMount(info, path::join(rootfs, "/dev/pts"), "devpts"));
  EXPECT_TRUE(hasMount(info, path::join(rootfs, "/dev/shm"), "tmpfs"));
  EXPECT_TRUE(hasMount(info, path::join(rootfs, "/mnt/medios/sandbox")));

  // The read-only binds inside /proc are sourced from the rootfs itself.
  EXPECT_TRUE(hasMount(info, path::join(rootfs, "/proc/sys")));

  const vector<string> devices = {"null", "zero", "urandom"};
  foreach (const string& device, devices) {
    EXPECT_TRUE(hasMount(info, path::join(rootfs, "dev", device)))
      << device;
  }

  // The device nodes are the host's own, never the image's and never a
  // copy on the runtime directory, which may sit on a nodev mount.
  bool bindsHostNull = false;
  foreach (const ContainerMountInfo& mount, info.mounts()) {
    if (mount.target() == path::join(rootfs, "dev", "null")) {
      bindsHostNull = mount.source() == "/dev/null";
    }
  }
  EXPECT_TRUE(bindsHostNull);

  bool masksKcore = false;
  foreach (const string& masked, info.masked_paths()) {
    if (masked == path::join(rootfs, "/proc/kcore")) {
      masksKcore = true;
    }
  }
  EXPECT_TRUE(masksKcore);

  AWAIT_READY(owned->cleanup(containerId));
}


// With `namespaces/ipc` enabled, that isolator owns the container's
// /dev/shm, so this isolator does not mount one.
TEST_F(LinuxFilesystemIsolatorTest, RootfsLeavesShmToNamespacesIPC)
{
  node::Flags flags;
  flags.runtime_dir = path::join(sandbox.get(), "runtime");
  flags.sandbox_directory = "/mnt/medios/sandbox";
  flags.isolation = "filesystem/linux,namespaces/ipc";

  const string rootfs = path::join(sandbox.get(), "rootfs");
  ASSERT_SOME(os::mkdir(rootfs));

  Try<Isolator*> isolator = node::LinuxFilesystemIsolatorProcess::create(flags);
  ASSERT_SOME(isolator);
  Owned<Isolator> owned(isolator.get());

  ContainerID containerId;
  containerId.set_value("ipc-owns-shm");

  ContainerConfig config;
  config.set_directory(path::join(sandbox.get(), "sandbox"));
  config.set_rootfs(rootfs);
  config.mutable_command_info()->set_value("true");

  Future<Option<ContainerLaunchInfo>> launchInfo =
    owned->prepare(containerId, config);

  AWAIT_READY(launchInfo);
  ASSERT_SOME(launchInfo.get());

  EXPECT_TRUE(hasMount(
      launchInfo->get(), path::join(rootfs, "/dev/pts"), "devpts"));
  EXPECT_FALSE(hasMount(launchInfo->get(), path::join(rootfs, "/dev/shm")));

  AWAIT_READY(owned->cleanup(containerId));
}

// The mounts a prepared container declares onto one target, in order.
static vector<ContainerMountInfo> mountsOnto(
    const ContainerLaunchInfo& launchInfo,
    const string& target)
{
  vector<ContainerMountInfo> result;
  foreach (const ContainerMountInfo& mount, launchInfo.mounts()) {
    if (mount.target() == target) {
      result.push_back(mount);
    }
  }
  return result;
}


// What the volume tests share: an isolator, a rootfs, and a container
// config that names the rootfs.
class LinuxFilesystemVolumeTest : public LinuxFilesystemIsolatorTest
{
protected:
  void SetUp() override
  {
    LinuxFilesystemIsolatorTest::SetUp();

    flags.runtime_dir = path::join(sandbox.get(), "runtime");
    flags.sandbox_directory = "/mnt/medios/sandbox";

    rootfs = path::join(sandbox.get(), "rootfs");
    ASSERT_SOME(os::mkdir(rootfs));

    // The node's stores: `models` and `weights`, each a directory here.
    modelsPath = path::join(sandbox.get(), "models");
    ASSERT_SOME(os::mkdir(modelsPath));
    weightsPath = path::join(sandbox.get(), "weights");
    ASSERT_SOME(os::mkdir(weightsPath));

    storesFile = path::join(sandbox.get(), "stores.json");
    ASSERT_SOME(os::write(storesFile, storesJson()));
    flags.stores = storesFile;

    createIsolator();

    containerId.set_value("with-volumes");

    config.set_directory(path::join(sandbox.get(), "sandbox"));
    config.set_rootfs(rootfs);
    config.mutable_command_info()->set_value("true");
    config.mutable_container_info()->set_type(ContainerInfo::MEDIOS);
  }

  string storesJson() const
  {
    return "{ \"stores\": [ "
           "{ \"name\": \"models\", \"path\": \"" + modelsPath + "\", "
           "\"class\": \"local\", \"locality\": \"near\", "
           "\"readers\": 2, \"writers\": 1 }, "
           "{ \"name\": \"weights\", \"path\": \"" + weightsPath + "\", "
           "\"class\": \"local\", \"locality\": \"near\", "
           "\"readers\": 2, \"writers\": 0 } ] }";
  }

  void createIsolator()
  {
    Try<Isolator*> created =
      node::LinuxFilesystemIsolatorProcess::create(flags);
    ASSERT_SOME(created);
    isolator.reset(created.get());
  }

  Volume* addVolume(
      const string& store,
      const string& containerPath,
      Volume::Mode mode)
  {
    Volume* volume = config.mutable_container_info()->add_volumes();
    volume->set_container_path(containerPath);
    volume->set_mode(mode);
    volume->mutable_source()->set_type(Volume::Source::STORE);
    volume->mutable_source()->mutable_store()->set_name(store);
    return volume;
  }

  node::Flags flags;
  string rootfs;
  string modelsPath;
  string weightsPath;
  string storesFile;
  Owned<Isolator> isolator;
  ContainerID containerId;
  ContainerConfig config;
};


// The host's resolver and hosts table are bound over the image's, since
// the container runs on the host's network, and read-only: what is bound
// is the host's own file, not a copy of it, so a writable bind lets a
// container rewrite the node's resolver for every container after it.
TEST_F(LinuxFilesystemVolumeTest, BindsTheHostsNameResolutionFilesReadOnly)
{
  Future<Option<ContainerLaunchInfo>> launchInfo =
    isolator->prepare(containerId, config);

  AWAIT_READY(launchInfo);
  ASSERT_SOME(launchInfo.get());

  const vector<string> files = {"/etc/resolv.conf", "/etc/hosts"};
  foreach (const string& file, files) {
    const vector<ContainerMountInfo> mounts =
      mountsOnto(launchInfo->get(), path::join(rootfs, file));

    ASSERT_EQ(2u, mounts.size()) << file;
    EXPECT_EQ(file, mounts[0].source()) << file;
    EXPECT_EQ(file, mounts[1].source()) << file;

    // The flag word itself rather than the bits in it. Asserting that
    // MS_RDONLY is present would pass with MS_REC added, and asserting the
    // pair of mounts would pass with the remount reduced to a second bind;
    // neither would arrive read-only.
    EXPECT_EQ(static_cast<unsigned>(MS_BIND), mounts[0].flags()) << file;
    EXPECT_EQ(
        static_cast<unsigned>(MS_BIND | MS_REMOUNT | MS_RDONLY),
        mounts[1].flags()) << file;
  }
}


// A read-write store is a recursive bind of the declared path.
TEST_F(LinuxFilesystemVolumeTest, BindsAReadWriteStore)
{
  addVolume("models", "/root/.ollama", Volume::RW);

  Future<Option<ContainerLaunchInfo>> launchInfo =
    isolator->prepare(containerId, config);

  AWAIT_READY(launchInfo);
  ASSERT_SOME(launchInfo.get());

  const vector<ContainerMountInfo> mounts =
    mountsOnto(launchInfo->get(), path::join(rootfs, "/root/.ollama"));

  ASSERT_EQ(1u, mounts.size());
  EXPECT_EQ(modelsPath, mounts[0].source());
  EXPECT_EQ(static_cast<unsigned>(MS_BIND | MS_REC), mounts[0].flags());
}


// A read-only store is a bind followed by a non-recursive read-only
// remount: the kernel ignores MS_RDONLY on the bind itself.
TEST_F(LinuxFilesystemVolumeTest, RemountsAReadOnlyStore)
{
  addVolume("weights", "/weights", Volume::RO);

  Future<Option<ContainerLaunchInfo>> launchInfo =
    isolator->prepare(containerId, config);

  AWAIT_READY(launchInfo);
  ASSERT_SOME(launchInfo.get());

  const vector<ContainerMountInfo> mounts =
    mountsOnto(launchInfo->get(), path::join(rootfs, "/weights"));

  ASSERT_EQ(2u, mounts.size());
  EXPECT_EQ(weightsPath, mounts[0].source());
  EXPECT_EQ(static_cast<unsigned>(MS_BIND), mounts[0].flags());
  EXPECT_EQ(weightsPath, mounts[1].source());
  EXPECT_EQ(
      static_cast<unsigned>(MS_BIND | MS_REMOUNT | MS_RDONLY),
      mounts[1].flags());
}


// A store whose declared directory has gone since the node started is
// refused by name, not created.
TEST_F(LinuxFilesystemVolumeTest, RefusesAStoreWhosePathIsGone)
{
  ASSERT_SOME(os::rmdir(modelsPath));

  addVolume("models", "/data", Volume::RW);

  Future<Option<ContainerLaunchInfo>> launchInfo =
    isolator->prepare(containerId, config);

  AWAIT_FAILED(launchInfo);
  EXPECT_TRUE(strings::contains(launchInfo.failure(), "'models'"));
  EXPECT_TRUE(strings::contains(launchInfo.failure(), "no longer exists"));
}


// A store the node does not declare is refused by name.
TEST_F(LinuxFilesystemVolumeTest, RefusesAnUndeclaredStore)
{
  addVolume("results", "/out", Volume::RW);

  Future<Option<ContainerLaunchInfo>> launchInfo =
    isolator->prepare(containerId, config);

  AWAIT_FAILED(launchInfo);
  EXPECT_TRUE(strings::contains(launchInfo.failure(), "'results'"));
  EXPECT_TRUE(strings::contains(launchInfo.failure(), "does not declare"));
}


// A node with no stores file declares no store and refuses every volume,
// saying so.
TEST_F(LinuxFilesystemVolumeTest, ANodeWithNoStoresRefusesEveryVolume)
{
  flags.stores = None();
  createIsolator();

  addVolume("models", "/data", Volume::RW);

  Future<Option<ContainerLaunchInfo>> launchInfo =
    isolator->prepare(containerId, config);

  AWAIT_FAILED(launchInfo);
  EXPECT_TRUE(strings::contains(launchInfo.failure(), "declares no stores"));
}


// A stores file the node would have refused refuses the isolator too.
TEST_F(LinuxFilesystemVolumeTest, ARefusedStoresFileRefusesTheIsolator)
{
  ASSERT_SOME(os::write(storesFile, "{ \"stores\": [] }"));

  Try<Isolator*> created =
    node::LinuxFilesystemIsolatorProcess::create(flags);
  ASSERT_ERROR(created);
  EXPECT_TRUE(strings::contains(created.error(), "declares no stores"));
}


// The placements a prepared container asks the launcher to find empty.
static vector<pair<string, string>> placementsOf(
    const ContainerLaunchInfo& launchInfo)
{
  vector<pair<string, string>> result;
  foreach (const ContainerPlacement& placement, launchInfo.placements()) {
    result.emplace_back(placement.path(), placement.what());
  }
  return result;
}


// A relative container path is one name inside the sandbox (DR-036
// revision, medios#602): bound in the image at the sandbox directory, after
// the sandbox's own bind so it lands on top of it, and listed for the
// launcher at its path on the node, so a fetched file there refuses the
// launch.
TEST_F(LinuxFilesystemVolumeTest, BindsARelativeStoreInsideTheSandbox)
{
  addVolume("models", "tables", Volume::RW);

  Future<Option<ContainerLaunchInfo>> launchInfo =
    isolator->prepare(containerId, config);

  AWAIT_READY(launchInfo);
  ASSERT_SOME(launchInfo.get());

  const string target = path::join(rootfs, "/mnt/medios/sandbox", "tables");

  const vector<ContainerMountInfo> mounts =
    mountsOnto(launchInfo->get(), target);

  ASSERT_EQ(1u, mounts.size());
  EXPECT_EQ(modelsPath, mounts[0].source());
  EXPECT_EQ(static_cast<unsigned>(MS_BIND | MS_REC), mounts[0].flags());

  int sandboxMount = -1, volume = -1;
  for (int i = 0; i < launchInfo->get().mounts_size(); i++) {
    const string& mountTarget = launchInfo->get().mounts(i).target();
    if (mountTarget == path::join(rootfs, "/mnt/medios/sandbox")) {
      sandboxMount = i;
    }
    if (mountTarget == target) { volume = i; }
  }

  ASSERT_NE(-1, sandboxMount);
  EXPECT_LT(sandboxMount, volume);

  const vector<pair<string, string>> expected = {
    {path::join(config.directory(), "tables"), "Volume for store 'models'"}};
  EXPECT_EQ(expected, placementsOf(launchInfo->get()));
}


// Read-only inside the sandbox is the same bind and remount as anywhere.
TEST_F(LinuxFilesystemVolumeTest, RemountsARelativeReadOnlyStore)
{
  addVolume("weights", "weights", Volume::RO);

  Future<Option<ContainerLaunchInfo>> launchInfo =
    isolator->prepare(containerId, config);

  AWAIT_READY(launchInfo);
  ASSERT_SOME(launchInfo.get());

  const vector<ContainerMountInfo> mounts = mountsOnto(
      launchInfo->get(),
      path::join(rootfs, "/mnt/medios/sandbox", "weights"));

  ASSERT_EQ(2u, mounts.size());
  EXPECT_EQ(static_cast<unsigned>(MS_BIND), mounts[0].flags());
  EXPECT_EQ(
      static_cast<unsigned>(MS_BIND | MS_REMOUNT | MS_RDONLY),
      mounts[1].flags());
}


// An absolute path places nothing in the sandbox, so the launcher is
// asked to find nothing empty.
TEST_F(LinuxFilesystemVolumeTest, AnAbsolutePathListsNoPlacement)
{
  addVolume("models", "/models", Volume::RW);

  Future<Option<ContainerLaunchInfo>> launchInfo =
    isolator->prepare(containerId, config);

  AWAIT_READY(launchInfo);
  ASSERT_SOME(launchInfo.get());
  EXPECT_EQ(0, launchInfo->get().placements_size());
}


// Any other relative path is refused by the node too, though the
// controller refuses it first: the node joins the path to a directory on
// its own disk.
TEST_F(LinuxFilesystemVolumeTest, RefusesARelativePathThatIsNotOneName)
{
  foreach (const string& path, vector<string>({"relative/target", ".."})) {
    config.mutable_container_info()->clear_volumes();
    addVolume("models", path, Volume::RW);

    Future<Option<ContainerLaunchInfo>> launchInfo =
      isolator->prepare(containerId, config);

    AWAIT_FAILED(launchInfo);
    EXPECT_TRUE(strings::contains(
        launchInfo.failure(), "one name inside the sandbox"))
      << launchInfo.failure();
    EXPECT_TRUE(strings::contains(launchInfo.failure(), "'" + path + "'"))
      << launchInfo.failure();
  }
}


// A volume backed by an image belongs to volume/image and is left alone.
TEST_F(LinuxFilesystemVolumeTest, LeavesAnImageVolumeAlone)
{
  Volume* volume = config.mutable_container_info()->add_volumes();
  volume->set_container_path("/image-volume");
  volume->set_mode(Volume::RO);
  volume->mutable_image()->set_type(Image::DOCKER);
  volume->mutable_image()->mutable_docker()->set_name("busybox");

  Future<Option<ContainerLaunchInfo>> launchInfo =
    isolator->prepare(containerId, config);

  AWAIT_READY(launchInfo);
  ASSERT_SOME(launchInfo.get());

  EXPECT_TRUE(
      mountsOnto(launchInfo->get(), path::join(rootfs, "/image-volume"))
        .empty());
}


// A host path, in either form, is refused with its source type named:
// nothing a framework sends names a path on this node.
TEST_F(LinuxFilesystemVolumeTest, RefusesAHostPathInEitherForm)
{
  Volume* source = config.mutable_container_info()->add_volumes();
  source->set_container_path("/models");
  source->set_mode(Volume::RW);
  source->mutable_source()->set_type(Volume::Source::HOST_PATH);
  source->mutable_source()->mutable_host_path()->set_path(modelsPath);

  Future<Option<ContainerLaunchInfo>> launchInfo =
    isolator->prepare(containerId, config);

  AWAIT_FAILED(launchInfo);
  EXPECT_TRUE(strings::contains(launchInfo.failure(), "HOST_PATH"));
  EXPECT_TRUE(strings::contains(launchInfo.failure(), "only a STORE volume"));

  config.mutable_container_info()->clear_volumes();

  Volume* bare = config.mutable_container_info()->add_volumes();
  bare->set_container_path("/models");
  bare->set_mode(Volume::RW);
  bare->set_host_path(modelsPath);

  launchInfo = isolator->prepare(containerId, config);

  AWAIT_FAILED(launchInfo);
  EXPECT_TRUE(strings::contains(launchInfo.failure(), "HOST_PATH"));
}


// A sandbox-path source has no isolator here and is refused, not skipped.
TEST_F(LinuxFilesystemVolumeTest, RefusesASandboxPathSource)
{
  Volume* volume = config.mutable_container_info()->add_volumes();
  volume->set_container_path("/shared");
  volume->set_mode(Volume::RW);
  volume->mutable_source()->set_type(Volume::Source::SANDBOX_PATH);
  volume->mutable_source()->mutable_sandbox_path()->set_path("shared");

  Future<Option<ContainerLaunchInfo>> launchInfo =
    isolator->prepare(containerId, config);

  AWAIT_FAILED(launchInfo);
  EXPECT_TRUE(strings::contains(launchInfo.failure(), "SANDBOX_PATH"));
}


// A container path that climbs out of the rootfs with `..` is refused.
TEST_F(LinuxFilesystemVolumeTest, RefusesAContainerPathThatClimbsOut)
{
  addVolume("models", "/a/../../escaped", Volume::RW);

  Future<Option<ContainerLaunchInfo>> launchInfo =
    isolator->prepare(containerId, config);

  AWAIT_FAILED(launchInfo);
  EXPECT_TRUE(strings::contains(launchInfo.failure(), ".."));
}


// Mounts are made before the pivot, so an absolute symlink in the image
// would send the bind to a host path. Refused rather than followed.
TEST_F(LinuxFilesystemVolumeTest, RefusesATargetThatLeavesTheRootfsByASymlink)
{
  const string outside = path::join(sandbox.get(), "outside");
  ASSERT_SOME(os::mkdir(outside));
  ASSERT_SOME(os::mkdir(path::join(rootfs, "root")));
  ASSERT_SOME(fs::symlink(outside, path::join(rootfs, "root", ".ollama")));

  addVolume("models", "/root/.ollama", Volume::RW);

  Future<Option<ContainerLaunchInfo>> launchInfo =
    isolator->prepare(containerId, config);

  AWAIT_FAILED(launchInfo);
  EXPECT_TRUE(strings::contains(launchInfo.failure(), "outside the container"));
}


// The same guard covers the resolver files: an image whose resolv.conf is
// a symlink to a host path is refused by name instead of starting with a
// resolver that dangles after the pivot.
TEST_F(LinuxFilesystemVolumeTest, RefusesAnImageResolverThatIsAHostSymlink)
{
  ASSERT_SOME(os::mkdir(path::join(rootfs, "etc")));
  ASSERT_SOME(fs::symlink(
      "/run/systemd/resolve/stub-resolv.conf",
      path::join(rootfs, "etc", "resolv.conf")));

  Future<Option<ContainerLaunchInfo>> launchInfo =
    isolator->prepare(containerId, config);

  AWAIT_FAILED(launchInfo);
  EXPECT_TRUE(strings::contains(launchInfo.failure(), "/etc/resolv.conf"));
}


// A volume over a path the containerizer mounts itself would cover the
// container's devices or sandbox.
TEST_F(LinuxFilesystemVolumeTest, RefusesAVolumeOverAContainerizerMount)
{
  addVolume("models", "/dev", Volume::RW);

  Future<Option<ContainerLaunchInfo>> launchInfo =
    isolator->prepare(containerId, config);

  AWAIT_FAILED(launchInfo);
  EXPECT_TRUE(strings::contains(launchInfo.failure(), "/dev"));
}


// What the tests of a command with no image share: no rootfs, a sandbox
// that exists, and a directory on the node for a store to land on.
class LinuxFilesystemNoImageTest : public LinuxFilesystemVolumeTest
{
protected:
  void SetUp() override
  {
    LinuxFilesystemVolumeTest::SetUp();

    config.clear_rootfs();
    ASSERT_SOME(os::mkdir(config.directory()));

    mountPoint = path::join(sandbox.get(), "mnt", "tables");
    ASSERT_SOME(os::mkdir(mountPoint));
  }

  static bool clonesMountNamespace(const ContainerLaunchInfo& info)
  {
    foreach (int ns, info.clone_namespaces()) {
      if (ns == CLONE_NEWNS) {
        return true;
      }
    }
    return false;
  }

  string mountPoint;
};


// A command with no image gets a private mount namespace and its
// read-only store bound at the container path itself, with the same
// non-recursive read-only remount an image gets.
TEST_F(LinuxFilesystemNoImageTest, BindsAReadOnlyStoreAtItsPath)
{
  addVolume("weights", mountPoint, Volume::RO);

  Future<Option<ContainerLaunchInfo>> launchInfo =
    isolator->prepare(containerId, config);

  AWAIT_READY(launchInfo);
  ASSERT_SOME(launchInfo.get());
  EXPECT_TRUE(clonesMountNamespace(launchInfo->get()));

  const vector<ContainerMountInfo> mounts =
    mountsOnto(launchInfo->get(), mountPoint);

  ASSERT_EQ(2u, mounts.size());
  EXPECT_EQ(weightsPath, mounts[0].source());
  EXPECT_EQ(static_cast<unsigned>(MS_BIND), mounts[0].flags());
  EXPECT_EQ(weightsPath, mounts[1].source());
  EXPECT_EQ(
      static_cast<unsigned>(MS_BIND | MS_REMOUNT | MS_RDONLY),
      mounts[1].flags());

  // Only the stores: no rootfs mounts, no resolver files, no devices.
  EXPECT_EQ(2, launchInfo->get().mounts_size());
  EXPECT_EQ(0, launchInfo->get().masked_paths_size());
}


// A read-write store is a recursive bind, as for an image.
TEST_F(LinuxFilesystemNoImageTest, BindsAReadWriteStoreAtItsPath)
{
  addVolume("models", mountPoint, Volume::RW);

  Future<Option<ContainerLaunchInfo>> launchInfo =
    isolator->prepare(containerId, config);

  AWAIT_READY(launchInfo);
  ASSERT_SOME(launchInfo.get());
  EXPECT_TRUE(clonesMountNamespace(launchInfo->get()));

  const vector<ContainerMountInfo> mounts =
    mountsOnto(launchInfo->get(), mountPoint);

  ASSERT_EQ(1u, mounts.size());
  EXPECT_EQ(modelsPath, mounts[0].source());
  EXPECT_EQ(static_cast<unsigned>(MS_BIND | MS_REC), mounts[0].flags());
}


// A relative path is a name in the command's sandbox, where the launcher
// creates the target: it need not exist beforehand, unlike an absolute
// path on the node, and it is listed for the launcher to find empty.
TEST_F(LinuxFilesystemNoImageTest, BindsARelativeStoreInTheSandbox)
{
  addVolume("weights", "weights", Volume::RO);

  Future<Option<ContainerLaunchInfo>> launchInfo =
    isolator->prepare(containerId, config);

  AWAIT_READY(launchInfo);
  ASSERT_SOME(launchInfo.get());
  EXPECT_TRUE(clonesMountNamespace(launchInfo->get()));

  Result<string> directory = os::realpath(config.directory());
  ASSERT_SOME(directory);

  const string target = path::join(directory.get(), "weights");

  const vector<ContainerMountInfo> mounts =
    mountsOnto(launchInfo->get(), target);

  ASSERT_EQ(2u, mounts.size());
  EXPECT_EQ(weightsPath, mounts[0].source());
  EXPECT_EQ(static_cast<unsigned>(MS_BIND), mounts[0].flags());
  EXPECT_EQ(
      static_cast<unsigned>(MS_BIND | MS_REMOUNT | MS_RDONLY),
      mounts[1].flags());

  const vector<pair<string, string>> expected = {
    {target, "Volume for store 'weights'"}};
  EXPECT_EQ(expected, placementsOf(launchInfo->get()));
}


// A command with no image and no store gets no namespace: it sees the
// node's filesystem as it is.
TEST_F(LinuxFilesystemNoImageTest, NoStoreDeclaresNothing)
{
  Future<Option<ContainerLaunchInfo>> launchInfo =
    isolator->prepare(containerId, config);

  AWAIT_READY(launchInfo);
  EXPECT_NONE(launchInfo.get());
}


// The container path is a path on this node and must already be a
// directory there: the launcher would create a missing target on the
// node's filesystem, and that outlives the namespace.
TEST_F(LinuxFilesystemNoImageTest, RefusesAPathThatIsNotADirectoryOnTheNode)
{
  const string missing = path::join(sandbox.get(), "mnt", "missing");
  addVolume("models", missing, Volume::RW);

  Future<Option<ContainerLaunchInfo>> launchInfo =
    isolator->prepare(containerId, config);

  AWAIT_FAILED(launchInfo);
  EXPECT_TRUE(strings::contains(launchInfo.failure(), "'" + missing + "'"))
    << launchInfo.failure();
  EXPECT_TRUE(strings::contains(
      launchInfo.failure(), "not a directory on this node"))
    << launchInfo.failure();
}


// The root, the kernel filesystems and the command's own sandbox are
// refused as targets, each named in the sentence.
TEST_F(LinuxFilesystemNoImageTest, RefusesTheRootKernelPathsAndTheSandbox)
{
  const vector<pair<string, string>> refused = {
    {"/", "over the root"},
    {"/dev/tables", "'/dev' is a kernel filesystem"},
    {"/proc", "'/proc' is a kernel filesystem"},
    {"/sys/fs", "'/sys' is a kernel filesystem"},
    {config.directory(), "the command's sandbox"},
    {path::join(config.directory(), "tables"), "the command's sandbox"},
  };

  foreach (const auto& target, refused) {
    config.mutable_container_info()->clear_volumes();
    addVolume("models", target.first, Volume::RW);

    Future<Option<ContainerLaunchInfo>> launchInfo =
      isolator->prepare(containerId, config);

    AWAIT_FAILED(launchInfo);
    EXPECT_TRUE(strings::contains(launchInfo.failure(), target.second))
      << target.first << ": " << launchInfo.failure();
  }
}


// A store over an ancestor of the sandbox would cover the sandbox and
// the command would run from inside the store. Refused in that direction
// too, naming both.
TEST_F(LinuxFilesystemNoImageTest, RefusesAnAncestorOfTheSandbox)
{
  const string parent = Path(config.directory()).dirname();
  addVolume("models", parent, Volume::RW);

  Future<Option<ContainerLaunchInfo>> launchInfo =
    isolator->prepare(containerId, config);

  AWAIT_FAILED(launchInfo);
  EXPECT_TRUE(strings::contains(
      launchInfo.failure(),
      "'" + parent + "' contains '" + config.directory() + "'"))
    << launchInfo.failure();
  EXPECT_TRUE(strings::contains(launchInfo.failure(), "the command's sandbox"))
    << launchInfo.failure();
}


// The container path is checked as it resolves on the node, so a symlink
// to a reserved path, or to the root, is refused as the path it names.
TEST_F(LinuxFilesystemNoImageTest, ResolvesASymlinkBeforeTheChecks)
{
  const string toDev = path::join(sandbox.get(), "to-dev");
  ASSERT_SOME(fs::symlink("/dev", toDev));
  addVolume("models", toDev, Volume::RW);

  Future<Option<ContainerLaunchInfo>> launchInfo =
    isolator->prepare(containerId, config);

  AWAIT_FAILED(launchInfo);
  EXPECT_TRUE(strings::contains(launchInfo.failure(), "'/dev' is a kernel"))
    << launchInfo.failure();

  const string toRoot = path::join(sandbox.get(), "to-root");
  ASSERT_SOME(fs::symlink("/", toRoot));
  config.mutable_container_info()->clear_volumes();
  addVolume("models", toRoot, Volume::RW);

  launchInfo = isolator->prepare(containerId, config);

  AWAIT_FAILED(launchInfo);
  EXPECT_TRUE(strings::contains(launchInfo.failure(), "over the root"))
    << launchInfo.failure();

  // A symlink to an ordinary directory is bound at the directory.
  const string toTables = path::join(sandbox.get(), "to-tables");
  ASSERT_SOME(fs::symlink(mountPoint, toTables));
  config.mutable_container_info()->clear_volumes();
  addVolume("models", toTables, Volume::RW);

  launchInfo = isolator->prepare(containerId, config);

  AWAIT_READY(launchInfo);
  ASSERT_SOME(launchInfo.get());
  EXPECT_EQ(1u, mountsOnto(launchInfo->get(), mountPoint).size());
}


// A relative path, a path with `..`, and a store the node does not
// declare are refused for a command with no image as for an image.
TEST_F(LinuxFilesystemNoImageTest, RefusesRelativeClimbingAndUndeclared)
{
  addVolume("models", "mnt/tables", Volume::RW);

  Future<Option<ContainerLaunchInfo>> launchInfo =
    isolator->prepare(containerId, config);

  AWAIT_FAILED(launchInfo);
  EXPECT_TRUE(strings::contains(
      launchInfo.failure(), "one name inside the sandbox"))
    << launchInfo.failure();

  config.mutable_container_info()->clear_volumes();
  addVolume("models", path::join(mountPoint, "..", "tables"), Volume::RW);

  launchInfo = isolator->prepare(containerId, config);

  AWAIT_FAILED(launchInfo);
  EXPECT_TRUE(strings::contains(launchInfo.failure(), ".."))
    << launchInfo.failure();

  config.mutable_container_info()->clear_volumes();
  addVolume("results", mountPoint, Volume::RW);

  launchInfo = isolator->prepare(containerId, config);

  AWAIT_FAILED(launchInfo);
  EXPECT_TRUE(strings::contains(launchInfo.failure(), "'results'"))
    << launchInfo.failure();
  EXPECT_TRUE(strings::contains(launchInfo.failure(), "does not declare"))
    << launchInfo.failure();

  // A store whose declared directory has gone since the node started.
  ASSERT_SOME(os::rmdir(weightsPath));
  config.mutable_container_info()->clear_volumes();
  addVolume("weights", mountPoint, Volume::RO);

  launchInfo = isolator->prepare(containerId, config);

  AWAIT_FAILED(launchInfo);
  EXPECT_TRUE(strings::contains(launchInfo.failure(), "'weights'"))
    << launchInfo.failure();
  EXPECT_TRUE(strings::contains(launchInfo.failure(), "no longer exists"))
    << launchInfo.failure();
}


// The resolver and volume binds come after the tmpfs over /dev and the
// sandbox, so nothing this isolator mounts later covers them.
TEST_F(LinuxFilesystemVolumeTest, DeclaresVolumesAfterTheRootfsMounts)
{
  addVolume("models", "/models", Volume::RW);

  Future<Option<ContainerLaunchInfo>> launchInfo =
    isolator->prepare(containerId, config);

  AWAIT_READY(launchInfo);
  ASSERT_SOME(launchInfo.get());

  int dev = -1, sandboxMount = -1, resolver = -1, volume = -1;
  for (int i = 0; i < launchInfo->get().mounts_size(); i++) {
    const string& target = launchInfo->get().mounts(i).target();
    if (target == path::join(rootfs, "/dev")) { dev = i; }
    if (target == path::join(rootfs, "/mnt/medios/sandbox")) {
      sandboxMount = i;
    }
    if (target == path::join(rootfs, "/etc/resolv.conf")) { resolver = i; }
    if (target == path::join(rootfs, "/models")) { volume = i; }
  }

  ASSERT_NE(-1, dev);
  ASSERT_NE(-1, sandboxMount);
  EXPECT_LT(dev, resolver);
  EXPECT_LT(sandboxMount, resolver);
  EXPECT_LT(resolver, volume);
}


// A store volume the node's isolator refuses reaches the framework as
// TASK_FAILED from the node, and the status message is the isolator's
// sentence, verbatim: the controller accepted the task (it claims a store
// the node declares), so the refusal is the node's, and a framework must
// be able to show its caller the reason without parsing around a prefix.
TEST_F(MediosTest, ARefusedStoreVolumeOnACommandCarriesTheIsolatorsSentence)
{
  Try<Owned<cluster::Controller>> controller = StartController();
  ASSERT_SOME(controller);

  const string tables = path::join(sandbox.get(), "tables");
  ASSERT_SOME(os::mkdir(tables));

  const string storesFile = path::join(sandbox.get(), "stores.json");
  ASSERT_SOME(os::write(storesFile,
      "{ \"stores\": [ { \"name\": \"tables\", \"path\": \"" + tables +
      "\", \"class\": \"local\", \"locality\": \"near\", "
      "\"readers\": 1, \"writers\": 1 } ] }"));

  node::Flags flags = CreateNodeFlags();
  flags.isolation = "filesystem/linux";
  flags.stores = storesFile;

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
  const Offer& offer = offers->at(0);

  // The claim lists the store's resource from the offer, as the controller
  // requires, so the launch reaches the node; the target is under /dev,
  // which the node's isolator refuses.
  Resources claim = Resources::parse("cpus:0.1;mem:32").get();
  claim.allocate(offer.resources(0).allocation_info().role());
  foreach (const Resource& resource, offer.resources()) {
    if (resource.name() == "store/tables") {
      claim += resource;
    }
  }

  TaskInfo task = createTask(offer.node_id(), claim, "sleep 1000");
  task.mutable_container()->set_type(ContainerInfo::MEDIOS);

  Volume* volume = task.mutable_container()->add_volumes();
  volume->set_container_path("/dev/tables");
  volume->set_mode(Volume::RO);
  volume->mutable_source()->set_type(Volume::Source::STORE);
  volume->mutable_source()->mutable_store()->set_name("tables");

  Future<TaskStatus> status;
  EXPECT_CALL(sched, statusUpdate(&driver, _))
    .WillOnce(FutureArg<1>(&status))
    .WillRepeatedly(Return());

  driver.launchTasks(offer.id(), {task});

  AWAIT_READY(status);
  EXPECT_EQ(task.task_id(), status->task_id());
  EXPECT_EQ(TASK_FAILED, status->state());
  EXPECT_EQ(TaskStatus::SOURCE_NODE, status->source());
  EXPECT_EQ(TaskStatus::REASON_CONTAINER_LAUNCH_FAILED, status->reason());
  EXPECT_EQ(
      "Volume '" + tables + "' -> '/dev/tables' is refused: '/dev' is a "
      "kernel filesystem of this node",
      status->message());

  driver.stop();
  driver.join();
}


// A real launch of a STORE volume at a relative container path, through
// the medios containerizer with the linux launcher: the store is where the
// task looks for it, read-only is the kernel's, and a file the fetcher
// put at the same name refuses the launch with the store untouched. The
// declaration tests above check the shape; these check what the launcher
// does with it.
class LinuxFilesystemStoreLaunchTest
  : public ContainerizerTest<node::MediosContainerizer>
{
protected:
  void SetUp() override
  {
    ContainerizerTest<node::MediosContainerizer>::SetUp();

    storePath = path::join(sandbox.get(), "tables-store");
    ASSERT_SOME(os::mkdir(storePath));
    ASSERT_SOME(os::write(path::join(storePath, "canary"), "canary"));
  }

  node::Flags CreateNodeFlags() override
  {
    node::Flags flags =
      ContainerizerTest<node::MediosContainerizer>::CreateNodeFlags();
    flags.isolation = "filesystem/linux,docker/runtime";
    flags.docker_registry = path::join(sandbox.get(), "registry");
    flags.docker_store_dir = path::join(sandbox.get(), "store");
    flags.image_providers = "docker";
    flags.launcher = "linux";

    const string file = path::join(sandbox.get(), "stores.json");
    CHECK_SOME(os::write(file,
        "{ \"stores\": [ { \"name\": \"tables\", \"path\": \"" +
        storePath + "\", \"class\": \"local\", \"locality\": \"near\", "
        "\"readers\": 1, \"writers\": 1 } ] }"));
    flags.stores = file;

    return flags;
  }

  static Volume storeVolume(const string& containerPath, Volume::Mode mode)
  {
    Volume volume;
    volume.set_container_path(containerPath);
    volume.set_mode(mode);
    volume.mutable_source()->set_type(Volume::Source::STORE);
    volume.mutable_source()->mutable_store()->set_name("tables");
    return volume;
  }

  // Launches `executor` in a fresh sandbox, the sandbox directory in
  // `directory`, and waits for it; its exit status goes in `status`. Not
  // local, as on a node: the launcher writes to the sandbox's stderr.
  void run(
      const node::Flags& flags,
      const ExecutorInfo& executor,
      string* directory,
      Option<int>* status,
      const std::function<void(const string&)>& before = nullptr)
  {
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


// A command with no image writes through `tables` in its working
// directory, and the write lands in the store. The bind was the command's
// own: on the node the sandbox holds only the empty mount point.
TEST_F(LinuxFilesystemStoreLaunchTest, ROOT_ARelativeStoreIsInTheSandbox)
{
  ExecutorInfo executor = createExecutorInfo(
      "test_executor", "cat tables/canary && echo written > tables/marker");
  *executor.mutable_container()->add_volumes() =
    storeVolume("tables", Volume::RW);
  executor.mutable_container()->set_type(ContainerInfo::MEDIOS);

  string directory;
  Option<int> status;
  run(CreateNodeFlags(), executor, &directory, &status);
  ASSERT_SOME(status);
  EXPECT_WEXITSTATUS_EQ(0, status.get());

  EXPECT_SOME_EQ("written\n", os::read(path::join(storePath, "marker")));
  EXPECT_TRUE(os::stat::isdir(path::join(directory, "tables")));
  EXPECT_FALSE(os::exists(path::join(directory, "tables", "marker")));
}


// Read-only by the kernel: the command reads the canary and its write is
// refused, and the store gains nothing.
TEST_F(LinuxFilesystemStoreLaunchTest, ROOT_AReadOnlyRelativeStoreRefusesWrites)
{
  ExecutorInfo executor = createExecutorInfo(
      "test_executor",
      "cat tables/canary && if touch tables/new; then exit 2; fi");
  *executor.mutable_container()->add_volumes() =
    storeVolume("tables", Volume::RO);
  executor.mutable_container()->set_type(ContainerInfo::MEDIOS);

  string directory;
  Option<int> status;
  run(CreateNodeFlags(), executor, &directory, &status);
  ASSERT_SOME(status);
  EXPECT_WEXITSTATUS_EQ(0, status.get());

  EXPECT_FALSE(os::exists(path::join(storePath, "new")));
}


// An image container finds the store in its sandbox directory, on top of
// the sandbox bind.
TEST_F(LinuxFilesystemStoreLaunchTest, ROOT_AnImageFindsARelativeStore)
{
  const node::Flags flags = CreateNodeFlags();

  AWAIT_READY(DockerArchive::create(flags.docker_registry, "test_image"));

  const string inside = path::join(flags.sandbox_directory, "tables");

  ExecutorInfo executor = createExecutorInfo(
      "test_executor",
      "cat " + inside + "/canary && echo written > " + inside + "/marker");
  executor.mutable_container()->CopyFrom(createContainerInfo("test_image"));
  *executor.mutable_container()->add_volumes() =
    storeVolume("tables", Volume::RW);

  string directory;
  Option<int> status;
  run(flags, executor, &directory, &status);
  ASSERT_SOME(status);
  EXPECT_WEXITSTATUS_EQ(0, status.get());

  EXPECT_SOME_EQ("written\n", os::read(path::join(storePath, "marker")));
}


// The fetcher runs after the isolators, so a URI whose file lands at the
// store's name is only seen by the launcher. It refuses the launch with
// the sentence in the sandbox's stderr; the fetched file is not covered
// and the store gets no write.
TEST_F(LinuxFilesystemStoreLaunchTest, ROOT_AFetchedFileAtTheTargetRefuses)
{
  const string from = path::join(sandbox.get(), "from");
  ASSERT_SOME(os::mkdir(from));
  ASSERT_SOME(os::write(path::join(from, "tables"), "fetched"));

  ExecutorInfo executor = createExecutorInfo(
      "test_executor", "echo written > tables/marker");
  CommandInfo::URI* uri = executor.mutable_command()->add_uris();
  uri->set_value(uri::from_path(path::join(from, "tables")));
  uri->set_extract(false);
  *executor.mutable_container()->add_volumes() =
    storeVolume("tables", Volume::RW);
  executor.mutable_container()->set_type(ContainerInfo::MEDIOS);

  string directory;
  Option<int> status;
  run(CreateNodeFlags(), executor, &directory, &status);
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
// launcher refuses rather than binding over it or following it.
TEST_F(LinuxFilesystemStoreLaunchTest, ROOT_ADanglingLinkAtTheTargetRefuses)
{
  ExecutorInfo executor = createExecutorInfo(
      "test_executor", "echo written > tables/marker");
  *executor.mutable_container()->add_volumes() =
    storeVolume("tables", Volume::RW);
  executor.mutable_container()->set_type(ContainerInfo::MEDIOS);

  const node::Flags flags = CreateNodeFlags();
  const string nowhere = path::join(sandbox.get(), "nowhere");

  string directory;
  Option<int> status;
  run(flags, executor, &directory, &status, [&](const string& sandbox) {
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
// store's name: the task ends TASK_FAILED, and the launcher's sentence,
// naming the store and the path, is in the sandbox's stderr.
TEST_F(LinuxFilesystemStoreLaunchTest, ROOT_AFetchedFileAtTheTargetFailsTheTask)
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
  *task.mutable_container()->add_volumes() =
    storeVolume("tables", Volume::RW);

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

#endif // __linux__

} // namespace tests {
} // namespace internal {
} // namespace medios {
