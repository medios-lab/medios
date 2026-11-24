// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

#include <sys/mount.h>

#include <map>
#include <string>
#include <utility>
#include <vector>

#include <glog/logging.h>

#include <process/id.hpp>

#include <stout/foreach.hpp>
#include <stout/os.hpp>
#include <stout/os/realpath.hpp>
#include <stout/os/stat.hpp>
#include <stout/path.hpp>
#include <stout/result.hpp>
#include <stout/strings.hpp>

#include "common/protobuf_utils.hpp"

#include "node/containerizer/medios/utils.hpp"

#include "node/containerizer/medios/isolators/filesystem/linux.hpp"

using medios::node::ContainerClass;
using medios::node::ContainerConfig;
using medios::node::ContainerLaunchInfo;
using medios::node::ContainerMountInfo;
using medios::node::ContainerState;
using medios::node::Isolator;

using medios::internal::protobuf::node::containerSymlinkOperation;
using medios::internal::protobuf::node::createContainerMount;

using process::Failure;
using process::Future;
using process::Owned;

using std::pair;
using std::string;
using std::vector;

namespace medios {
namespace internal {
namespace node {

namespace {

// Kernel filesystems every rootfs needs, with the parts of /proc that
// would let a container reach into the host masked read-only. Targets
// are relative to the rootfs and joined at prepare time; a source that is
// an absolute path is a bind mount from the same rootfs. The launcher
// creates a missing target directory before mounting.
const ContainerMountInfo ROOTFS_MOUNTS[] = {
  createContainerMount(
      "proc", "/proc", "proc",
      MS_NOSUID | MS_NOEXEC | MS_NODEV),
  createContainerMount(
      "/proc/bus", "/proc/bus",
      MS_BIND | MS_RDONLY | MS_NOSUID | MS_NOEXEC | MS_NODEV),
  createContainerMount(
      "/proc/fs", "/proc/fs",
      MS_BIND | MS_RDONLY | MS_NOSUID | MS_NOEXEC | MS_NODEV),
  createContainerMount(
      "/proc/irq", "/proc/irq",
      MS_BIND | MS_RDONLY | MS_NOSUID | MS_NOEXEC | MS_NODEV),
  createContainerMount(
      "/proc/sys", "/proc/sys",
      MS_BIND | MS_RDONLY | MS_NOSUID | MS_NOEXEC | MS_NODEV),
  createContainerMount(
      "/proc/sysrq-trigger", "/proc/sysrq-trigger",
      MS_BIND | MS_RDONLY | MS_NOSUID | MS_NOEXEC | MS_NODEV),
  createContainerMount(
      "sysfs", "/sys", "sysfs",
      MS_RDONLY | MS_NOSUID | MS_NOEXEC | MS_NODEV),
  createContainerMount(
      "tmpfs", "/dev", "tmpfs", "mode=755",
      MS_NOSUID | MS_NOEXEC | MS_STRICTATIME),
  // gid=5 is the tty group on every standard distribution; grantpt(3)
  // then needs no privileged helper inside the container.
  createContainerMount(
      "devpts", "/dev/pts", "devpts",
      "newinstance,ptmxmode=0666,mode=0620,gid=5",
      MS_NOSUID | MS_NOEXEC),
};


const vector<string> ROOTFS_MASKED_PATHS = {
  "/proc/acpi",
  "/proc/asound",
  "/proc/kcore",
  "/proc/keys",
  "/proc/key-users",
  "/proc/latency_stats",
  "/proc/sched_debug",
  "/proc/scsi",
  "/proc/timer_list",
  "/proc/timer_stats",
  "/sys/firmware",
};


// The device nodes a chroot needs. Each is the host's own node,
// bind-mounted over the private /dev, so the image's /dev is never
// trusted for device numbers. A copy would have to live on a filesystem
// mounted without nodev; the runtime directory is not one (it is under
// /run on standard hosts), and a node on a nodev mount cannot be opened.
void makeStandardDevices(
    const string& rootfs,
    ContainerLaunchInfo* launchInfo)
{
  const vector<string> devices = {
    "full", "null", "random", "tty", "urandom", "zero"
  };

  foreach (const string& device, devices) {
    *launchInfo->add_mounts() = createContainerMount(
        path::join("/", "dev", device),
        path::join(rootfs, "dev", device),
        MS_BIND);
  }

  const vector<pair<string, string>> symlinks = {
    {"/proc/self/fd",   path::join(rootfs, "dev", "fd")},
    {"/proc/self/fd/0", path::join(rootfs, "dev", "stdin")},
    {"/proc/self/fd/1", path::join(rootfs, "dev", "stdout")},
    {"/proc/self/fd/2", path::join(rootfs, "dev", "stderr")},
    {"pts/ptmx",        path::join(rootfs, "dev", "ptmx")},
  };

  foreach (const auto& symlink, symlinks) {
    *launchInfo->add_file_operations() =
      containerSymlinkOperation(symlink.first, symlink.second);
  }
}


// The host's name-resolution files, bound over the image's own. This
// containerizer gives a container no network namespace, so the host's
// resolver and hosts table are the ones that are true for it; an image
// ships its own from wherever it was built, or empty ones. A network
// isolator that gives a container its own namespace would own these
// files instead.
const vector<string> HOST_NETWORK_FILES = {
  "/etc/resolv.conf",
  "/etc/hosts",
};


// The declared path a volume resolves to, or none for a volume backed by
// an image, which belongs to volume/image and is left alone.
Try<Option<string>> declaredPathOf(
    const Volume& volume,
    const std::map<string, StoreDeclaration>& stores)
{
  if (volume.has_image()) {
    return None();
  }

  Try<string> path = node::storePathOf(volume, stores);
  if (path.isError()) {
    return Error(path.error());
  }

  return Option<string>(path.get());
}


// Whether a path exists, following symlinks. `os::exists` does not, so a
// dangling link, the usual shape of /etc/resolv.conf on a host whose
// resolver service is stopped, would pass it and fail later in the
// launcher without the reason.
bool existsFollowingLinks(const string& path)
{
  return os::stat::isdir(path) || os::stat::isfile(path);
}


// The checks every store volume passes before it is placed, whatever the
// form of its container path. The declared path was checked when the
// node started; a store whose directory has gone since is refused, not
// created on the framework's say-so as dockerd would. A relative path
// must be one name inside the sandbox; the controller refuses the rest
// at submission and this is the node's own check before joining it to a
// sandbox on disk.
Try<Nothing> placeableStore(const Volume& volume, const string& hostPath)
{
  const string& containerPath = volume.container_path();

  if (!path::is_absolute(hostPath)) {
    return Error(
        "Volume '" + hostPath + "' -> '" + containerPath + "' is refused: "
        "the store's declared path is not absolute");
  }

  if (!existsFollowingLinks(hostPath)) {
    return Error(
        "Store '" + volume.source().store().name() + "' is declared at '" +
        hostPath + "', which no longer exists on this node");
  }

  if (!path::is_absolute(containerPath) && !isSandboxName(containerPath)) {
    return Error(
        "Volume for store '" + volume.source().store().name() + "' asks "
        "for container path '" + containerPath + "'; a relative container "
        "path is one name inside the sandbox, such as 'data'");
  }

  return Nothing();
}


// A mount target must land inside the rootfs. Mounts are made before the
// pivot, so an absolute symlink in the image resolves against the host,
// and `path::join` does not normalize `..`. Either would bind the source
// somewhere on the host and leave the container's path empty, quietly.
// The part of the target that already exists is resolved and must be the
// rootfs or under it; the launcher creates the rest as plain directories.
Try<Nothing> insideRootfs(const string& rootfs, const string& target)
{
  foreach (const string& component, strings::tokenize(target, "/")) {
    if (component == "..") {
      return Error("'" + target + "' contains '..'");
    }
  }

  string existing = target;
  while (!os::exists(existing) && existing != "/") {
    existing = Path(existing).dirname();
  }

  Result<string> resolved = os::realpath(existing);
  if (!resolved.isSome()) {
    return Error(
        "'" + existing + "' in the image cannot be resolved; it is most "
        "likely a symlink that points nowhere");
  }

  Result<string> root = os::realpath(rootfs);
  if (!root.isSome()) {
    return Error("The rootfs '" + rootfs + "' cannot be resolved");
  }

  if (resolved.get() != root.get() &&
      !strings::startsWith(resolved.get(), root.get() + "/")) {
    return Error(
        "'" + existing + "' in the image is a symlink to '" +
        resolved.get() + "', outside the container");
  }

  return Nothing();
}


// The mounts that deliver one store at `target`. A read-only store is a
// bind followed by a non-recursive read-only remount: the kernel ignores
// MS_RDONLY on the bind itself, and a read-only remount reaches only the
// top mount, so a filesystem mounted beneath the host path would stay
// writable through a recursive bind. Without MS_REC it is not visible in
// the container at all, which is the side that cannot write through. The
// same shape for an image and for a command with no image.
void bindStore(
    const string& hostPath,
    const string& target,
    Volume::Mode mode,
    ContainerLaunchInfo* launchInfo)
{
  if (mode == Volume::RO) {
    *launchInfo->add_mounts() =
      createContainerMount(hostPath, target, MS_BIND);
    *launchInfo->add_mounts() = createContainerMount(
        hostPath, target, MS_BIND | MS_REMOUNT | MS_RDONLY);
  } else {
    *launchInfo->add_mounts() =
      createContainerMount(hostPath, target, MS_BIND | MS_REC);
  }
}


// Whether `path` is `reserved` or beneath it.
bool isUnder(const string& path, const string& reserved)
{
  return path == reserved || strings::startsWith(path, reserved + "/");
}


// A command with no image sees the node's own filesystem. Its stores are
// delivered the way an image's are: the command gets a private mount
// namespace and each store is bound at its container path in that view,
// read-only by the kernel when asked. The launcher marks the root of the
// new namespace recursively node-private before mounting, so the binds are the
// command's alone and nothing reaches the node's own mount table.
//
// A relative container path is a name inside the command's sandbox, and
// the launcher creates the target there. An absolute container path is a
// path on this node, and the launcher creates a missing mount target on
// the node's filesystem, which outlives the namespace. So an absolute path
// must already exist here as a directory: a directory on a node is the
// operator's to create, as the store's own directory is, and not made on
// a framework's say-so.
Future<Option<ContainerLaunchInfo>> prepareCommandWithNoImage(
    const ContainerConfig& containerConfig,
    const std::map<string, StoreDeclaration>& stores)
{
  ContainerLaunchInfo launchInfo;

  // The command's sandbox is its working directory on this node, and a
  // store over it or over the kernel filesystems would cover what the
  // command runs from.
  const string sandboxDirectory = [&containerConfig]() {
    // As it resolves, like the container path it is compared with.
    Result<string> resolved = os::realpath(containerConfig.directory());
    return resolved.isSome() ? resolved.get() : containerConfig.directory();
  }();

  const vector<string> reserved = sandboxDirectory.empty()
    ? vector<string>{"/dev", "/proc", "/sys"}
    : vector<string>{"/dev", "/proc", "/sys", sandboxDirectory};

  foreach (const Volume& volume, containerConfig.container_info().volumes()) {
    Try<Option<string>> declared = declaredPathOf(volume, stores);
    if (declared.isError()) {
      return Failure(declared.error());
    }

    if (declared->isNone()) {
      continue; // An image volume, volume/image's.
    }

    const string& hostPath = declared->get();
    const string& containerPath = volume.container_path();

    Try<Nothing> placeable = placeableStore(volume, hostPath);
    if (placeable.isError()) {
      return Failure(placeable.error());
    }

    // A relative path names a place in the sandbox, which is the task's
    // own directory, so the launcher creating the target there creates
    // nothing on the node outside it. The launcher refuses the launch if
    // the fetcher left something at that name.
    if (!path::is_absolute(containerPath)) {
      if (sandboxDirectory.empty()) {
        return Failure(
            "Volume for store '" + volume.source().store().name() + "' "
            "asks for relative container path '" + containerPath + "', and "
            "the container has no sandbox to place it in");
      }

      const string target = path::join(sandboxDirectory, containerPath);
      bindStore(hostPath, target, volume.mode(), &launchInfo);
      addStorePlacement(volume, target, &launchInfo);
      continue;
    }

    foreach (const string& component, strings::tokenize(containerPath, "/")) {
      if (component == "..") {
        return Failure(
            "Volume '" + hostPath + "' -> '" + containerPath + "' is "
            "refused: '" + containerPath + "' contains '..'");
      }
    }

    // Checked and mounted as the path resolves on the node, so a symlink
    // at the container path cannot stand in for a reserved one. The part
    // that exists is resolved and the rest appended, so a path under a
    // reserved one is named as such even when it does not exist.
    string existing = containerPath;
    string rest;
    while (!os::exists(existing) && existing != "/") {
      rest = rest.empty()
        ? Path(existing).basename()
        : path::join(Path(existing).basename(), rest);
      existing = Path(existing).dirname();
    }

    Result<string> resolved = os::realpath(existing);
    if (!resolved.isSome()) {
      return Failure(
          "Volume '" + hostPath + "' -> '" + containerPath + "' is refused: "
          "'" + existing + "' cannot be resolved on this node" +
          (resolved.isError() ? ": " + resolved.error() : ""));
    }

    const string target =
      rest.empty() ? resolved.get() : path::join(resolved.get(), rest);

    if (target == "/") {
      return Failure(
          "Volume '" + hostPath + "' -> '" + containerPath + "' is refused: "
          "a store cannot be bound over the root of a command that has no "
          "image");
    }

    foreach (const string& path, reserved) {
      if (isUnder(target, path)) {
        return Failure(
            "Volume '" + hostPath + "' -> '" + containerPath + "' is "
            "refused: '" + path + "' is " +
            (path == sandboxDirectory
               ? "the command's sandbox"
               : "a kernel filesystem of this node"));
      }

      // The other direction: a store bound over an ancestor of the
      // sandbox would cover the sandbox, and the command would run from
      // inside the store.
      if (isUnder(path, target)) {
        return Failure(
            "Volume '" + hostPath + "' -> '" + containerPath + "' is "
            "refused: '" + target + "' contains '" + path + "', " +
            (path == sandboxDirectory
               ? "the command's sandbox"
               : "a kernel filesystem of this node"));
      }
    }

    if (!os::stat::isdir(target)) {
      return Failure(
          "Volume '" + hostPath + "' -> '" + containerPath + "' is refused: "
          "'" + containerPath + "' is not a directory on this node; a "
          "command with no image is given its stores at directories that "
          "exist on the node, created once by the operator as the store's "
          "own directory is, and the node creates none for a task");
    }

    bindStore(hostPath, target, volume.mode(), &launchInfo);
  }

  if (launchInfo.mounts().empty()) {
    // Nothing to deliver: the command sees the node's filesystem as it
    // is, with no namespace of its own.
    return None();
  }

  launchInfo.add_clone_namespaces(CLONE_NEWNS);

  return launchInfo;
}

} // namespace {


Try<Isolator*> LinuxFilesystemIsolatorProcess::create(const Flags& flags)
{
  // Read once at start, as the node did for its total: the file the node
  // advertised from is the file volumes resolve against.
  std::map<string, StoreDeclaration> stores;
  if (flags.stores.isSome()) {
    Try<std::map<string, StoreDeclaration>> read =
      readStores(flags.stores.get());
    if (read.isError()) {
      return Error(read.error());
    }
    stores = read.get();
  }

  Owned<MediosIsolatorProcess> process(
      new LinuxFilesystemIsolatorProcess(flags, stores));

  return new MediosIsolator(process);
}


LinuxFilesystemIsolatorProcess::LinuxFilesystemIsolatorProcess(
    const Flags& _flags,
    const std::map<string, StoreDeclaration>& _stores)
  : ProcessBase(process::ID::generate("linux-filesystem-isolator")),
    flags(_flags),
    stores(_stores) {}


bool LinuxFilesystemIsolatorProcess::supportsNesting()
{
  return true;
}


bool LinuxFilesystemIsolatorProcess::supportsStandalone()
{
  return true;
}


Future<Nothing> LinuxFilesystemIsolatorProcess::recover(
    const vector<ContainerState>& states,
    const hashset<ContainerID>& orphans)
{
  // Mounts live in the container's own mount namespace and vanish with
  // it. Nothing to reconcile.
  return Nothing();
}


Future<Option<ContainerLaunchInfo>> LinuxFilesystemIsolatorProcess::prepare(
    const ContainerID& containerId,
    const ContainerConfig& containerConfig)
{
  // If we are a nested container in the `DEBUG` class, then we only
  // use this isolator to indicate that we should enter our parent's
  // MOUNT namespace. We don't want to clone a new MOUNT namespace or
  // run any new pre-exec commands in it. For now, we also don't
  // support provisioning a new filesystem or setting a `rootfs` for
  // the container. We also don't support mounting any volumes.
  if (containerId.has_parent() &&
      containerConfig.has_container_class() &&
      containerConfig.container_class() == ContainerClass::DEBUG) {
    if (containerConfig.has_rootfs()) {
      return Failure("A 'rootfs' cannot be set for DEBUG containers");
    }

    if (containerConfig.has_container_info() &&
        containerConfig.container_info().volumes().size() > 0) {
      return Failure("Volumes not supported for DEBUG containers");
    }

    ContainerLaunchInfo launchInfo;
    launchInfo.add_enter_namespaces(CLONE_NEWNS);
    return launchInfo;
  }

  if (!containerConfig.has_rootfs()) {
    return prepareCommandWithNoImage(containerConfig, stores);
  }

  const string& rootfs = containerConfig.rootfs();

  ContainerLaunchInfo launchInfo;
  launchInfo.add_clone_namespaces(CLONE_NEWNS);

  // Mounting the rootfs over itself gives pivot_root a mount point that
  // is not the process's current root.
  *launchInfo.add_mounts() = createContainerMount(rootfs, rootfs, MS_BIND);

  foreach (const ContainerMountInfo& mount, ROOTFS_MOUNTS) {
    ContainerMountInfo* info = launchInfo.add_mounts();
    info->CopyFrom(mount);
    info->set_target(path::join(rootfs, mount.target()));

    if (strings::startsWith(mount.source(), "/")) {
      info->set_source(path::join(rootfs, mount.source()));
    }
  }

  // With `namespaces/ipc` enabled, that isolator mounts /dev/shm: it
  // applies the requested size and decides whether a nested container
  // shares its parent's.
  if (!strings::contains(flags.isolation, "namespaces/ipc")) {
    *launchInfo.add_mounts() = createContainerMount(
        "tmpfs",
        path::join(rootfs, "/dev/shm"),
        "tmpfs",
        "mode=1777",
        MS_NOSUID | MS_NODEV | MS_STRICTATIME);
  }

  foreach (const string& path, ROOTFS_MASKED_PATHS) {
    launchInfo.add_masked_paths(path::join(rootfs, path));
  }

  makeStandardDevices(rootfs, &launchInfo);

  // The sandbox, at the same path inside the image that the executor's
  // working directory and MEDIOS_SANDBOX name.
  *launchInfo.add_mounts() = createContainerMount(
      containerConfig.directory(),
      path::join(rootfs, flags.sandbox_directory),
      MS_BIND | MS_REC);

  foreach (const string& file, HOST_NETWORK_FILES) {
    // Refused rather than skipped: a container that starts without a
    // resolver fails later, at its first lookup, with nothing tying the
    // failure back to the missing file.
    if (!existsFollowingLinks(file)) {
      return Failure(
          "The host has no '" + file + "' to give the container; this "
          "containerizer runs containers on the host's network and binds "
          "the host's name-resolution files into the image");
    }

    const string target = path::join(rootfs, file);

    Try<Nothing> inside = insideRootfs(rootfs, target);
    if (inside.isError()) {
      return Failure(
          "Cannot bind the host's '" + file + "' into the image: " +
          inside.error());
    }

    // Read-only, because these are binds of the host's own files rather
    // than per-container copies: a container that rewrote its resolver
    // would rewrite the node's, for every container after it and for the
    // node itself. Proven on a GPU node, in a private mount namespace: a
    // plain bind let a write through to the host file; with the remount
    // the write is refused and the file still reads.
    //
    // Two calls because the kernel ignores MS_RDONLY on the bind itself,
    // and not recursive because a single file has nothing mounted beneath
    // it. Same shape as a read-only store.
    //
    // This is narrower than `docker run`, which binds a copy it manages
    // and leaves that copy writable, so a container that edits its own
    // resolv.conf works under docker and not here. Recorded on medios#476
    // as the docker-parity alternative; nothing in this workload edits
    // these files, and the hazard is the shared host file.
    *launchInfo.add_mounts() = createContainerMount(file, target, MS_BIND);
    *launchInfo.add_mounts() = createContainerMount(
        file, target, MS_BIND | MS_REMOUNT | MS_RDONLY);
  }

  // Paths this isolator has already mounted. A volume on one of them
  // would cover the container's devices or its sandbox.
  const vector<string> reserved = {
    "/dev", "/proc", "/sys", flags.sandbox_directory
  };

  foreach (const Volume& volume, containerConfig.container_info().volumes()) {
    Try<Option<string>> declared = declaredPathOf(volume, stores);
    if (declared.isError()) {
      return Failure(declared.error());
    }

    if (declared->isNone()) {
      continue; // An image volume, volume/image's.
    }

    const string& hostPath = declared->get();
    const string& containerPath = volume.container_path();

    Try<Nothing> placeable = placeableStore(volume, hostPath);
    if (placeable.isError()) {
      return Failure(placeable.error());
    }

    // A relative path is inside the sandbox. The bind is added after the
    // sandbox's, so it lands on top of it, and the target the launcher
    // creates is in the task's own directory on the node. The launcher
    // refuses the launch if the fetcher left something at that name.
    if (!path::is_absolute(containerPath)) {
      bindStore(
          hostPath,
          path::join(rootfs, flags.sandbox_directory, containerPath),
          volume.mode(),
          &launchInfo);
      addStorePlacement(
          volume,
          path::join(containerConfig.directory(), containerPath),
          &launchInfo);
      continue;
    }

    foreach (const string& path, reserved) {
      if (isUnder(containerPath, path)) {
        return Failure(
            "Volume '" + hostPath + "' -> '" + containerPath + "' is "
            "refused: '" + path + "' is mounted by the containerizer");
      }
    }

    const string target = path::join(rootfs, containerPath);

    Try<Nothing> inside = insideRootfs(rootfs, target);
    if (inside.isError()) {
      return Failure(
          "Volume '" + hostPath + "' -> '" + containerPath + "' is "
          "refused: " + inside.error());
    }

    bindStore(hostPath, target, volume.mode(), &launchInfo);
  }

  return launchInfo;
}


Future<Nothing> LinuxFilesystemIsolatorProcess::cleanup(
    const ContainerID& containerId)
{
  return Nothing();
}

} // namespace node {
} // namespace internal {
} // namespace medios {
