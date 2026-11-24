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

#include <limits.h>
#include <unistd.h>

#include <map>
#include <string>
#include <utility>
#include <vector>

#include <process/collect.hpp>
#include <process/id.hpp>

#include <stout/fs.hpp>
#include <stout/os.hpp>
#include <stout/path.hpp>

#include <stout/os/realpath.hpp>
#include <stout/os/stat.hpp>

#include "common/protobuf_utils.hpp"

#include "node/paths.hpp"

#include "node/containerizer/medios/utils.hpp"

#include "node/containerizer/medios/isolators/filesystem/posix.hpp"

using namespace process;

using std::map;
using std::pair;
using std::string;
using std::vector;

using medios::node::ContainerConfig;
using medios::node::ContainerLaunchInfo;
using medios::node::ContainerLimitation;
using medios::node::ContainerState;
using medios::node::Isolator;

namespace medios {
namespace internal {
namespace node {

namespace {

// Where a link at `link` points, if it is a symlink. Read without
// following it, so a fetched file or directory at the name is not taken
// for the link the isolator made.
Option<string> linkTarget(const string& link)
{
  if (!os::stat::islink(link)) {
    return None();
  }

  char buffer[PATH_MAX];
  const ssize_t length = ::readlink(link.c_str(), buffer, sizeof(buffer));
  if (length < 0 || static_cast<size_t>(length) >= sizeof(buffer)) {
    return None();
  }

  return string(buffer, static_cast<size_t>(length));
}


// The volumes a recovered container was launched with: a top-level
// container's are its executor's, a nested or standalone container's
// are its own.
const ContainerInfo* containerInfoOf(const ContainerState& state)
{
  if (state.has_container_info()) {
    return &state.container_info();
  }

  if (state.has_executor_info() && state.executor_info().has_container()) {
    return &state.executor_info().container();
  }

  return nullptr;
}

} // namespace {


PosixFilesystemIsolatorProcess::PosixFilesystemIsolatorProcess(
    const Flags& _flags,
    VolumeGidManager* _volumeGidManager,
    const map<string, StoreDeclaration>& _stores,
    const map<string, string>& _storeRealpaths)
  : ProcessBase(process::ID::generate("posix-filesystem-isolator")),
    flags(_flags),
    volumeGidManager(_volumeGidManager),
    stores(_stores),
    storeRealpaths(_storeRealpaths) {}


PosixFilesystemIsolatorProcess::~PosixFilesystemIsolatorProcess() {}


Try<Isolator*> PosixFilesystemIsolatorProcess::create(
    const Flags& flags,
    VolumeGidManager* volumeGidManager)
{
  // Read once at start, as the node did for its total: the file the node
  // advertised from is the file volumes resolve against.
  map<string, StoreDeclaration> stores;
  if (flags.stores.isSome()) {
    Try<map<string, StoreDeclaration>> read = readStores(flags.stores.get());
    if (read.isError()) {
      return Error(read.error());
    }
    stores = read.get();
  }

  map<string, string> storeRealpaths;
  foreachpair (const string& name, const StoreDeclaration& store, stores) {
    // A symlink grants the container the store read-write whatever the
    // volume asks for, so a store this node could hand out read-only is
    // refused here rather than at every launch (DR-036, medios#602).
    if (store.readers > 0) {
      return Error(
          "Store '" + name + "' declares readers: " +
          stringify(store.readers) + "; filesystem/posix cannot make a "
          "store read-only, so a store on this node declares readers: 0");
    }

    Result<string> realpath = os::realpath(store.path);
    if (!realpath.isSome()) {
      return Error(
          "Store '" + name + "' is declared at '" + store.path + "', which "
          "does not resolve on this node" +
          (realpath.isError() ? ": " + realpath.error() : ""));
    }

    storeRealpaths[name] = realpath.get();
  }

  process::Owned<MediosIsolatorProcess> process(
      new PosixFilesystemIsolatorProcess(
          flags, volumeGidManager, stores, storeRealpaths));

  return new MediosIsolator(process);
}


bool PosixFilesystemIsolatorProcess::supportsNesting()
{
  return true;
}


bool PosixFilesystemIsolatorProcess::supportsStandalone()
{
  return true;
}


Future<Nothing> PosixFilesystemIsolatorProcess::recover(
    const vector<ContainerState>& states,
    const hashset<ContainerID>& orphans)
{
  foreach (const ContainerState& state, states) {
    Owned<Info> info(new Info(state.directory()));

    const ContainerInfo* containerInfo = containerInfoOf(state);
    const vector<Volume> volumes = containerInfo == nullptr
      ? vector<Volume>()
      : vector<Volume>(
            containerInfo->volumes().begin(), containerInfo->volumes().end());

    // A link made before the node restarted points where its store
    // resolved then. If the declaration has moved or gone since, the
    // container is stopped rather than left writing to the old place.
    foreach (const Volume& volume, volumes) {
      if (!volume.has_source() ||
          volume.source().type() != Volume::Source::STORE ||
          !isSandboxName(volume.container_path()) ||
          state.directory().empty()) {
        continue;
      }

      const string& name = volume.source().store().name();
      const string link = path::join(state.directory(), volume.container_path());

      // No link at the name: the container stopped before the launcher
      // made it, or the launch was refused because the name was taken.
      Option<string> target = linkTarget(link);
      if (target.isNone()) {
        continue;
      }

      info->storeLinks.push_back({link, target.get()});

      auto current = storeRealpaths.find(name);
      if (current == storeRealpaths.end()) {
        info->limitation.set(protobuf::node::createContainerLimitation(
            Resources(),
            "The link for store '" + name + "' at '" + link + "' points at '" +
            target.get() + "', and this node no longer declares the store",
            TaskStatus::REASON_CONTAINER_LIMITATION));
        continue;
      }

      if (current->second != target.get()) {
        info->limitation.set(protobuf::node::createContainerLimitation(
            Resources(),
            "The link for store '" + name + "' at '" + link + "' points at '" +
            target.get() + "', and the store now resolves to '" +
            current->second + "'",
            TaskStatus::REASON_CONTAINER_LIMITATION));
      }
    }

    infos.put(state.container_id(), info);
  }

  return Nothing();
}


Try<pair<string, string>> PosixFilesystemIsolatorProcess::storeLinkOf(
    const Volume& volume,
    const string& directory) const
{
  if (volume.has_image()) {
    return Error(
        "Volume '" + volume.container_path() + "' is backed by an image; "
        "filesystem/posix has no mount namespace to deliver it");
  }

  Try<string> declared = node::storePathOf(volume, stores);
  if (declared.isError()) {
    return Error(declared.error());
  }

  const string& name = volume.source().store().name();
  const string& containerPath = volume.container_path();

  if (path::is_absolute(containerPath)) {
    return Error(
        "Volume for store '" + name + "' asks for absolute container path '" +
        containerPath + "'; this node places stores only inside the "
        "sandbox; use a relative container path");
  }

  if (!isSandboxName(containerPath)) {
    return Error(
        "Volume for store '" + name + "' asks for container path '" +
        containerPath + "'; a relative container path is one name inside "
        "the sandbox, such as 'data'");
  }

  if (volume.mode() == Volume::RO) {
    return Error(
        "Volume for store '" + name + "' at '" + containerPath + "' asks "
        "for read-only access; filesystem/posix delivers a store as a "
        "symlink, which cannot make it read-only");
  }

  if (directory.empty()) {
    return Error(
        "Volume for store '" + name + "' asks for container path '" +
        containerPath + "', and the container has no sandbox to place it in");
  }

  // The link points where the store resolved at start. A declared path
  // that resolves elsewhere now, through a symlink an operator or another
  // process changed, is refused and both places named.
  const string& atStart = storeRealpaths.at(name);

  Result<string> now = os::realpath(declared.get());
  if (!now.isSome()) {
    return Error(
        "Store '" + name + "' is declared at '" + declared.get() + "', "
        "which resolved to '" + atStart + "' when the node started and no "
        "longer resolves");
  }

  if (now.get() != atStart) {
    return Error(
        "Store '" + name + "' is declared at '" + declared.get() + "', "
        "which resolved to '" + atStart + "' when the node started and "
        "resolves to '" + now.get() + "' now");
  }

  return pair<string, string>(path::join(directory, containerPath), atStart);
}


Future<Option<ContainerLaunchInfo>> PosixFilesystemIsolatorProcess::prepare(
    const ContainerID& containerId,
    const ContainerConfig& containerConfig)
{
  if (infos.contains(containerId)) {
    return Failure("Container has already been prepared");
  }

  // A link made here would point outside a new root.
  if (containerConfig.has_rootfs() ||
      (containerConfig.has_container_info() &&
       containerConfig.container_info().medios().has_image())) {
    return Failure("Container root filesystems not supported");
  }

  ContainerLaunchInfo storeLaunchInfo;
  vector<pair<string, string>> storeLinks;

  if (containerConfig.has_container_info()) {
    foreach (const Volume& volume,
             containerConfig.container_info().volumes()) {
      Try<pair<string, string>> link =
        storeLinkOf(volume, containerConfig.directory());
      if (link.isError()) {
        return Failure(link.error());
      }

      // The launcher makes the link after the fetcher has run, and
      // refuses the launch if the fetcher left something at the name.
      *storeLaunchInfo.add_file_operations() =
        protobuf::node::containerSymlinkOperation(
            link->second, link->first);
      addStorePlacement(volume, link->first, &storeLaunchInfo);
      storeLinks.push_back(link.get());
    }
  }

  infos.put(containerId, Owned<Info>(new Info(containerConfig.directory())));
  infos[containerId]->storeLinks = storeLinks;

  // A persistent volume is an executor's resource, linked by `update`; a
  // nested container holds none, and a DEBUG one has no sandbox to stat.
  if (containerId.has_parent()) {
    return storeLaunchInfo;
  }

  const ExecutorInfo& executorInfo = containerConfig.executor_info();

  return update(containerId, executorInfo.resources())
    .then(defer(
        self(),
        [this, containerId, containerConfig, storeLaunchInfo]() mutable
            -> Future<Option<ContainerLaunchInfo>> {
          if (!infos.contains(containerId)) {
            return Failure("Unknown container");
          }

          ContainerLaunchInfo launchInfo = storeLaunchInfo;
          foreach (gid_t gid, infos[containerId]->gids) {
            // For command task with its own rootfs, the command executor will
            // run as root and the task itself will run as the specified normal
            // user, so here we add the supplementary group for the task and the
            // command executor will set it accordingly when launching the task.
            if (containerConfig.has_task_info() &&
                containerConfig.has_rootfs()) {
              launchInfo.add_task_supplementary_groups(gid);
            } else {
              launchInfo.add_supplementary_groups(gid);
            }
          }

          return launchInfo;
      }));
}


Future<ContainerLimitation> PosixFilesystemIsolatorProcess::watch(
    const ContainerID& containerId)
{
  if (!infos.contains(containerId)) {
    return Failure("Unknown container");
  }

  return infos[containerId]->limitation.future();
}


Future<Nothing> PosixFilesystemIsolatorProcess::update(
    const ContainerID& containerId,
    const Resources& resourceRequests,
    const google::protobuf::Map<string, Value::Scalar>& resourceLimits)
{
  if (!infos.contains(containerId)) {
    return Failure("Unknown container");
  }

  const Owned<Info>& info = infos[containerId];

  // TODO(jieyu): Currently, we only allow non-nested relative
  // container paths for volumes. This is enforced by the controller. For
  // those volumes, we create symlinks in the executor directory.
  Resources current = info->resources;

  // We first remove unneeded persistent volumes.
  foreach (const Resource& resource, current.persistentVolumes()) {
    // This is enforced by the controller.
    CHECK(resource.disk().has_volume());

    // Ignore absolute and nested paths.
    const string& containerPath = resource.disk().volume().container_path();
    if (strings::contains(containerPath, "/")) {
      LOG(WARNING) << "Skipping updating symlink for persistent volume "
                   << resource << " of container " << containerId
                   << " because the container path '" << containerPath
                   << "' contains slash";
      continue;
    }

    if (resourceRequests.contains(resource)) {
      continue;
    }

    string link = path::join(info->directory, containerPath);

    LOG(INFO) << "Removing symlink '" << link << "' for persistent volume "
              << resource << " of container " << containerId;

    Try<Nothing> rm = os::rm(link);
    if (rm.isError()) {
      return Failure(
          "Failed to remove the symlink for the unneeded "
          "persistent volume at '" + link + "'");
    }
  }

  // Get user and group info for this task based on the task's sandbox.
  struct stat s;
  if (::stat(info->directory.c_str(), &s) < 0) {
    return Failure("Failed to get ownership for '" + info->directory +
                   "': " + os::strerror(errno));
  }

  const uid_t uid = s.st_uid;
  const gid_t gid = s.st_gid;

  vector<Future<gid_t>> futures;

  // We then link additional persistent volumes.
  foreach (const Resource& resource, resourceRequests.persistentVolumes()) {
    // This is enforced by the controller.
    CHECK(resource.disk().has_volume());

    // Ignore absolute and nested paths.
    const string& containerPath = resource.disk().volume().container_path();
    if (strings::contains(containerPath, "/")) {
      LOG(WARNING) << "Skipping updating symlink for persistent volume "
                   << resource << " of container " << containerId
                   << " because the container path '" << containerPath
                   << "' contains slash";
      continue;
    }

    if (current.contains(resource)) {
      continue;
    }

    string original = paths::getPersistentVolumePath(flags.work_dir, resource);

    // If the container's user is root (uid == 0), we do not need to do any
    // changes about the volume's ownership since it has the full permissions
    // to access the volume.
    if (uid != 0) {
      // For persistent volumes not from resource providers, if volume gid
      // manager is enabled, call volume gid manager to allocate a gid to
      // make sure the container has the permission to access the volume.
      //
      // TODO(qianzhang): Support gid allocation for persistent volumes from
      // resource providers.
      if (!Resources::hasResourceProvider(resource) &&
          volumeGidManager) {
  #ifndef __WINDOWS__
        LOG(INFO) << "Invoking volume gid manager to allocate gid to the "
                  << "volume path '" << original << "' for container "
                  << containerId;

        futures.push_back(
            volumeGidManager->allocate(original, VolumeGidInfo::PERSISTENT));
  #endif // __WINDOWS__
      } else {
        bool isVolumeInUse = false;

        // Check if the shared persistent volume is currently used by another
        // container. We do not need to do this check for local persistent
        // volume since it can only be used by one container at a time.
        if (resource.has_shared()) {
          foreachpair (const ContainerID& _containerId,
                       const Owned<Info>& info,
                       infos) {
            // Skip self.
            if (_containerId == containerId) {
              continue;
            }

            if (info->resources.contains(resource)) {
              isVolumeInUse = true;
              break;
            }
          }
        }

        // Set the ownership of the persistent volume to match that of the
        // sandbox directory if the volume is not already in use. If the
        // volume is currently in use by other containers, tasks in this
        // container may fail to read from or write to the persistent volume
        // due to incompatible ownership and file system permissions.
        if (!isVolumeInUse) {
          // TODO(hausdorff): (MESOS-5461) Persistent volumes maintain the
          // invariant that they are used by one task at a time. This is
          // currently enforced by `os::chown`. Windows does not support
          // `os::chown`, we will need to revisit this later.
  #ifndef __WINDOWS__
          LOG(INFO) << "Changing the ownership of the persistent volume at '"
                    << original << "' with uid " << uid << " and gid " << gid;

          Try<Nothing> chown = os::chown(uid, gid, original, false);

          if (chown.isError()) {
            return Failure(
                "Failed to change the ownership of the persistent volume at '" +
                original + "' with uid " + stringify(uid) +
                " and gid " + stringify(gid) + ": " + chown.error());
          }
  #endif // __WINDOWS__
        }
      }
    }

    string link = path::join(info->directory, containerPath);

    if (os::exists(link)) {
      // NOTE: This is possible because 'info->resources' will be
      // reset when node restarts and recovers. When the node calls
      // 'containerizer->update' after the executor reregisters,
      // we'll try to relink all the already symlinked volumes.
      Result<string> realpath = os::realpath(link);
      if (!realpath.isSome()) {
        return Failure(
            "Failed to get the realpath of symlink '" + link + "': " +
            (realpath.isError() ? realpath.error() : "No such directory"));
      }

      // A sanity check to make sure the target of the symlink does
      // not change. In fact, this is not supposed to happen.
      // NOTE: Here, we compare the realpaths because 'original' might
      // contain symbolic links.
      Result<string> _original = os::realpath(original);
      if (!_original.isSome()) {
        return Failure(
            "Failed to get the realpath of volume '" + original + "': " +
            (_original.isError() ? _original.error() : "No such directory"));
      }

      if (realpath.get() != _original.get()) {
        return Failure(
            "The existing symlink '" + link + "' points to '" +
            _original.get() + "' and the new target is '" +
            realpath.get() + "'");
      }
    } else {
      LOG(INFO) << "Adding symlink from '" << original << "' to '"
                << link << "' for persistent volume " << resource
                << " of container " << containerId;

      // NOTE: We cannot enforce read-only access given the symlink without
      // changing the source so we just log a warning here.
      if (resource.disk().volume().mode() == Volume::RO) {
        LOG(WARNING) << "Allowing read-write access to read-only volume '"
                     << original << "' of container " << containerId;
      }

      Try<Nothing> symlink = ::fs::symlink(original, link);
      if (symlink.isError()) {
        return Failure(
            "Failed to symlink persistent volume from '" +
            original + "' to '" + link + "'");
      }
    }
  }

  // Store the updated resources.
  info->resources = resourceRequests;

  return collect(futures)
    .then(defer(self(), [this, containerId](const vector<gid_t>& gids)
      -> Future<Nothing> {
      if (!infos.contains(containerId)) {
        return Failure("Unknown container");
      }

      infos[containerId]->gids = gids;

      return Nothing();
    }));
}


Future<Nothing> PosixFilesystemIsolatorProcess::cleanup(
    const ContainerID& containerId)
{
  // An orphan was never recovered, so its links are unknown; they and the
  // links for persistent resources go when the work directory is GC'ed,
  // which removes a link without following it.
  if (!infos.contains(containerId)) {
    return Nothing();
  }

  // A store link is removed while it still points where it was made to;
  // anything else at the name was not made here and is left.
  foreach (const auto& link, infos[containerId]->storeLinks) {
    Option<string> target = linkTarget(link.first);
    if (target.isNone() || target.get() != link.second) {
      continue;
    }

    Try<Nothing> rm = os::rm(link.first);
    if (rm.isError()) {
      LOG(WARNING) << "Failed to remove the link for a store at '"
                   << link.first << "' of container " << containerId
                   << ", leaving it for the sandbox GC: " << rm.error();
    }
  }

  infos.erase(containerId);

  return Nothing();
}

} // namespace node {
} // namespace internal {
} // namespace medios {
