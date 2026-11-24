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

#ifndef __POSIX_FILESYSTEM_ISOLATOR_HPP__
#define __POSIX_FILESYSTEM_ISOLATOR_HPP__

#include <map>
#include <string>
#include <utility>
#include <vector>

#include <medios/resources.hpp>

#include <process/future.hpp>

#include "node/flags.hpp"
#include "node/stores.hpp"

#include "node/containerizer/medios/isolator.hpp"

#include "node/volume_gid_manager/volume_gid_manager.hpp"

namespace medios {
namespace internal {
namespace node {

// The filesystem isolator of a node without mount namespaces, and the
// default when a node names none. A container sees the node's filesystem
// and runs in its sandbox; a container root filesystem is refused.
//
// A STORE volume (DR-036, medios#602) is delivered as a symlink at
// `<sandbox>/<container_path>`, pointing at the store's declared path as
// it resolved when the node started. The launcher makes the link after the fetcher has
// run and refuses the launch if the name is taken. Only one name inside
// the sandbox is placed; an absolute container path is refused, and so is
// a read-only volume, since a symlink cannot make a store read-only. A
// node with this isolator refuses at start a store declaring readers
// above 0. The link is removed at cleanup; removing the sandbox does not
// follow it either.
class PosixFilesystemIsolatorProcess : public MediosIsolatorProcess
{
public:
  static Try<medios::node::Isolator*> create(
      const Flags& flags,
      VolumeGidManager* volumeGidManager);

  ~PosixFilesystemIsolatorProcess() override;

  bool supportsNesting() override;
  bool supportsStandalone() override;

  process::Future<Nothing> recover(
      const std::vector<medios::node::ContainerState>& states,
      const hashset<ContainerID>& orphans) override;

  process::Future<Option<medios::node::ContainerLaunchInfo>> prepare(
      const ContainerID& containerId,
      const medios::node::ContainerConfig& containerConfig) override;

  process::Future<Nothing> update(
      const ContainerID& containerId,
      const Resources& resourceRequests,
      const google::protobuf::Map<
          std::string, Value::Scalar>& resourceLimits = {}) override;

  process::Future<medios::node::ContainerLimitation> watch(
      const ContainerID& containerId) override;

  process::Future<Nothing> cleanup(
      const ContainerID& containerId) override;

protected:
  PosixFilesystemIsolatorProcess(
      const Flags& flags,
      VolumeGidManager* volumeGidManager,
      const std::map<std::string, StoreDeclaration>& stores,
      const std::map<std::string, std::string>& storeRealpaths);

  const Flags flags;
  VolumeGidManager* volumeGidManager;

  // The stores this node declared, by name, and where each declared path
  // resolved when the node started. A link points at the latter; a
  // declared path that resolves elsewhere later is refused rather than
  // followed (DR-036).
  const std::map<std::string, StoreDeclaration> stores;
  const std::map<std::string, std::string> storeRealpaths;

  // The link that delivers a STORE volume, as (link, target), or why the
  // volume cannot be delivered here.
  Try<std::pair<std::string, std::string>> storeLinkOf(
      const Volume& volume,
      const std::string& directory) const;

  struct Info
  {
    explicit Info(const std::string& _directory)
      : directory(_directory) {}

    const std::string directory;

    // Track resources so we can unlink unneeded persistent volumes.
    Resources resources;

    std::vector<gid_t> gids;

    // The store links placed in the sandbox, each with the target it was
    // made with. Cleanup removes a link only while it still points there,
    // so a file the fetcher left at the name stays.
    std::vector<std::pair<std::string, std::string>> storeLinks;

    // Satisfied when a recovered link no longer points where its store
    // resolves; the containerizer then destroys the container.
    process::Promise<medios::node::ContainerLimitation> limitation;
  };

  hashmap<ContainerID, process::Owned<Info>> infos;
};

} // namespace node {
} // namespace internal {
} // namespace medios {

#endif // __POSIX_FILESYSTEM_ISOLATOR_HPP__
