// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

#ifndef __LINUX_FILESYSTEM_ISOLATOR_HPP__
#define __LINUX_FILESYSTEM_ISOLATOR_HPP__

#include <vector>

#include <medios/node/isolator.hpp>

#include <process/future.hpp>

#include <stout/hashset.hpp>
#include <stout/nothing.hpp>
#include <stout/option.hpp>
#include <stout/try.hpp>

#include "node/flags.hpp"
#include "node/stores.hpp"

#include "node/containerizer/medios/isolator.hpp"

namespace medios {
namespace internal {
namespace node {

// Makes a provisioned rootfs enterable. A container image carries no
// /proc, /sys or /dev; the launcher enters the rootfs and then reads its
// own mount table from /proc/mounts, so without these mounts every image
// container fails at launch. This isolator declares, for any container
// that has a rootfs: the kernel filesystems, a private /dev with the
// standard device nodes, and the sandbox at the sandbox directory. A STORE
// volume is resolved against the node's stores and bound into the rootfs.
// A container without a rootfs sees the node's filesystem; with a STORE
// volume it gets a private mount namespace and the store bound at the
// container path in that view, and with none it gets no declaration at
// all. Persistent volumes are not handled here.
class LinuxFilesystemIsolatorProcess : public MediosIsolatorProcess
{
public:
  static Try<medios::node::Isolator*> create(const Flags& flags);

  ~LinuxFilesystemIsolatorProcess() override = default;

  bool supportsNesting() override;
  bool supportsStandalone() override;

  process::Future<Nothing> recover(
      const std::vector<medios::node::ContainerState>& states,
      const hashset<ContainerID>& orphans) override;

  process::Future<Option<medios::node::ContainerLaunchInfo>> prepare(
      const ContainerID& containerId,
      const medios::node::ContainerConfig& containerConfig) override;

  process::Future<Nothing> cleanup(const ContainerID& containerId) override;

private:
  LinuxFilesystemIsolatorProcess(
      const Flags& flags,
      const std::map<std::string, StoreDeclaration>& stores);

  const Flags flags;

  // The stores this node declared, by name: what a STORE volume resolves
  // against (DR-036). Empty on a node that declares none, which then
  // refuses every volume.
  const std::map<std::string, StoreDeclaration> stores;
};

} // namespace node {
} // namespace internal {
} // namespace medios {

#endif // __LINUX_FILESYSTEM_ISOLATOR_HPP__
