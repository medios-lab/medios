// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

// GPU isolator modules.
//
// Built as its own shared library (target `medios_gpu`), never linked into
// `libmedios`, and loaded into the node through `--modules`. Core has no
// reference to it: GPU behaviour reaches the container lifecycle only through
// the Isolator interface, the same seam the cgroups isolators use for cpus
// and mem.
//
// The library registers two isolators:
//
//   com_medioslab_medios_gpu_assign   Decides at prepare() which devices serve
//                                     the container's claim, checkpoints that
//                                     assignment, and sets CUDA_VISIBLE_DEVICES
//                                     to it. For a container with its own
//                                     rootfs, also binds the assigned devices'
//                                     files and the driver libraries into it.
//                                     Releases whole devices at cleanup().
//                                     Needs NVML and a units file, and nothing
//                                     from the cgroup hierarchy.
//
//   com_medioslab_medios_gpu_devices  Reads that assignment back at prepare()
//                                     and declares the device files the
//                                     container may open, which the
//                                     containerizer grants through
//                                     cgroups/devices (medios#455).
//
// A node that cannot create cgroups lists the first alone. A node that can
// lists both, assign first: the containerizer runs isolator prepare() calls
// in --isolation order, and the device isolator refuses a GPU container whose
// assignment it cannot find.

#include <medios/medios.hpp>
#include <medios/module.hpp>

#include <medios/module/isolator.hpp>
#include <medios/node/isolator.hpp>

#include <glog/logging.h>

#include <map>
#include <memory>
#include <mutex>
#include <set>
#include <vector>

#include <process/future.hpp>

#include <stout/foreach.hpp>
#include <stout/hashset.hpp>
#include <stout/os/exists.hpp>
#include <stout/os/read.hpp>
#include <stout/stringify.hpp>
#include <stout/strings.hpp>

#ifdef __linux__
// Device delivery is a Linux concept: there is no cgroup devices controller
// to grant anything elsewhere, and the driver volume is a Linux mount. On
// other platforms these modules still load and still discover, so a developer
// can run the tests, and they deliver nothing.
#include "modules/gpu/volume.hpp"

#include <sys/mount.h>

#include <stout/os/mkdir.hpp>
#include <stout/path.hpp>

#include "common/protobuf_utils.hpp"
#endif // __linux__

#include "modules/gpu/checkpoint.hpp"
#include "modules/gpu/delivery.hpp"
#include "modules/gpu/discovery.hpp"
#include "modules/gpu/units.hpp"

using medios::ContainerID;
using medios::DeviceAccess;
using medios::Environment;
using medios::Parameters;
using medios::Resources;
using medios::Value;
using medios::node::ContainerConfig;
using medios::node::ContainerLaunchInfo;
using medios::node::ContainerState;
using medios::node::Isolator;


namespace mediosgpu {

// The registered names, also used in log lines and refusals so an operator
// reading either sees the string they put in --isolation.
constexpr char ASSIGN_MODULE[] = "com_medioslab_medios_gpu_assign";
constexpr char DEVICES_MODULE[] = "com_medioslab_medios_gpu_devices";

#ifdef __linux__
// The amount of a named scalar in a claim, if any. Core no longer knows the
// GPU resource names; the module does (FAC3-0028).
static Option<double> scalar(const Resources& resources, const std::string& name)
{
  Option<Value::Scalar> value = resources.get<Value::Scalar>(name);
  if (value.isNone()) {
    return None();
  }
  return value->value();
}


// Which declared GPU resource names a claim asks for. Only the names the
// units file declares are looked at: a node's other resources are none of
// this module's business.
template <typename Units>
static std::vector<std::string> askedResources(
    const Units& units,
    const Resources& resources)
{
  std::vector<std::string> asked;
  foreachkey (const std::string& resource, units) {
    const Option<double> amount = scalar(resources, resource);
    if (amount.isSome() && *amount != 0.0) {
      asked.push_back(resource);
    }
  }
  return asked;
}


// Placing a claim reads the bound devices and then adds to them, so two
// containers preparing at once would otherwise both see the same device free
// and both be granted it. One mutex covers the read and the write. The
// in-tree isolator solves the same problem by being a libprocess Process; a
// module that owns two small containers does not need one.
class GpuAssignIsolator : public Isolator
{
public:
  // One unit per declared resource name: the devices behind it and how much
  // of the resource each of them provides (DR-035). The module knows no
  // resource name it was not given, so a node declaring `gpus` and a node
  // declaring something else are the same code with a different units file.
  GpuAssignIsolator(
      const std::map<std::string, medios::modules::gpu::UnitSet>& _units,
      const Option<medios::internal::node::NvidiaVolume>& _volume)
    : units(_units), volume(_volume) {}

  // Containers that outlived the node process. The module keeps what it
  // assigned in memory and checkpoints it under each container's own
  // directory, which survives the restart along with the container
  // (medios#471): rebuilding the ledger from that checkpoint is what lets a
  // whole-device claim stay refused for a device a survivor still holds,
  // instead of being handed out a second time.
  //
  // Recording that a container existed before this process did is still
  // needed independently of the checkpoint: it is what update() uses to tell
  // "prepared without a GPU" apart from "prepared by a node that is gone",
  // and a container that never held a claim (no checkpoint to read) still
  // has to be told apart from one this process never saw at all.
  process::Future<Nothing> recover(
      const std::vector<ContainerState>& states,
      const hashset<ContainerID>& orphans) override
  {
    std::lock_guard<std::mutex> guard(mutex);

    foreach (const ContainerState& state, states) {
      const std::string key = state.container_id().SerializeAsString();
      recovered.insert(key);

      Try<Option<medios::modules::gpu::AllocationCheckpoint>> checkpoint =
        medios::modules::gpu::readAllocationCheckpoint(state.directory());

      if (checkpoint.isError()) {
        return process::Failure(
            "GPU module: the allocation checkpoint for recovered container " +
            stringify(state.container_id()) + " could not be read: " +
            checkpoint.error() + "; the ledger cannot be rebuilt for it "
            "without guessing which devices it holds");
      }

      // No checkpoint means the container was never assigned a GPU claim --
      // declined at prepare, or a non-GPU container on a GPU node -- and
      // there is nothing for this module to rebuild for it.
      if (checkpoint->isNone()) {
        continue;
      }

      const medios::modules::gpu::AllocationCheckpoint& saved = **checkpoint;

      if (units.count(saved.resource) == 0) {
        return process::Failure(
            "GPU module: recovered container " +
            stringify(state.container_id()) + " checkpointed a '" +
            saved.resource + "' allocation, which this node's units file no "
            "longer declares");
      }

      // Re-resolved against this process's own discovery rather than taken
      // from the checkpoint verbatim: the UUID is the only part of a device's
      // identity that outlives a reboot, and the index and minor number the
      // checkpoint might carry are this process's business to look up fresh,
      // not the previous process's to hand down stale.
      Try<std::vector<medios::modules::gpu::Device>> devices =
        medios::modules::gpu::devicesByUuid(
            units.at(saved.resource), saved.deviceUuids);

      if (devices.isError()) {
        return process::Failure(
            "GPU module: recovered container " +
            stringify(state.container_id()) + " checkpointed " +
            devices.error());
      }

      allocations[key] = Allocation{
          saved.resource, saved.claim, *devices, saved.whole, state.directory()};

      // Only a whole-device assignment is bound; a seat assignment was never
      // bound in the first place (see prepare()), so rebuilding it here would
      // refuse a device its other seat-holders are meant to still share.
      if (saved.whole) {
        foreach (const medios::modules::gpu::Device& device, *devices) {
          bound.insert(device.uuid);
        }
      }
    }

    // Orphans are containers the launcher knows and the node does not. They
    // are destroyed rather than updated (and carry no directory to read a
    // checkpoint from), but recording them costs nothing and means the
    // refusal in update() never fires on one in the moment before it dies.
    foreach (const ContainerID& orphan, orphans) {
      recovered.insert(orphan.SerializeAsString());
    }

    LOG(INFO) << "GPU module: recovered " << recovered.size() << " container(s) "
              << "from a previous node; " << allocations.size() << " had a "
              << "checkpointed GPU allocation, now rebuilt into this "
              << "process's ledger";

    return Nothing();
  }

  process::Future<Option<ContainerLaunchInfo>> prepare(
      const ContainerID& containerId,
      const ContainerConfig& containerConfig) override
  {
    const Resources resources = containerConfig.resources();
    const std::vector<std::string> asked = askedResources(units, resources);

    // A container that asked for no GPUs is untouched: no assignment, no
    // checkpoint and no CUDA_VISIBLE_DEVICES.
    if (asked.empty()) {
      return None();
    }

    // Two GPU resource names at once is refused rather than served (DR-034).
    // The units file gives each device to one resource, so honouring both
    // would hand the container a device under one name and a device under
    // another, which is not what either request asked for.
    if (asked.size() > 1) {
      return process::Failure(
          "This container asked for " + stringify(asked.size()) + " GPU "
          "resources at once (" + strings::join(", ", asked) + "). A "
          "container holds devices under one resource name; ask for one.");
    }

    const std::string resource = asked.front();
    const double amount = *scalar(resources, resource);

    // Which devices serve this claim. Quantity is core's: it has already
    // taken the claim out of the node's total, and this module keeps no
    // second count to check it against (DR-035). What is decided here is
    // identity, and every refusal it can make names the resource.
    std::vector<medios::modules::gpu::Device> granted;
    {
      std::lock_guard<std::mutex> guard(mutex);

      Try<medios::modules::gpu::Placement> placed =
        medios::modules::gpu::place(units.at(resource), amount, bound);

      if (placed.isError()) {
        return process::Failure(placed.error());
      }

      granted = placed->devices;

      // Only devices taken whole are recorded, and place() is what says which
      // those are. A claim below what one device provides is a share beside
      // other shares of the same device, and recording it would take that
      // device away from the holders it is meant to be shared with.
      if (placed->whole) {
        foreach (const medios::modules::gpu::Device& device, granted) {
          bound.insert(device.uuid);
        }
      }

      // Recorded before anything that can fail. cleanup() is guaranteed after
      // prepare(), including when prepare() fails, so the entry has to be
      // findable by then: returning a Failure with devices still bound and no
      // record of them leaves them held for the lifetime of the node, and a
      // node that has done that to every device refuses GPU work while
      // nvidia-smi shows an idle GPU.
      allocations[containerId.SerializeAsString()] = Allocation{
          resource, amount, granted, placed->whole, containerConfig.directory()};

      // Checkpointed before returning, for two readers. A restart between
      // here and cleanup() must find the same assignment, or the next
      // process's recover() rebuilds nothing for this container and a later
      // prepare() can give one of its devices away (medios#471). And the
      // device isolator, when it is listed, reads this checkpoint in its own
      // prepare() to learn which device files to declare.
      std::vector<std::string> uuids;
      foreach (const medios::modules::gpu::Device& device, granted) {
        uuids.push_back(device.uuid);
      }

      Try<Nothing> checkpointed = medios::modules::gpu::writeAllocationCheckpoint(
          containerConfig.directory(),
          medios::modules::gpu::AllocationCheckpoint{
              resource, amount, uuids, placed->whole});

      if (checkpointed.isError()) {
        // The assignment is already recorded in `allocations` above (and, if
        // whole, in `bound`), so cleanup() -- guaranteed after this failure
        // -- still releases it. An assignment this module cannot checkpoint
        // is exactly the state medios#471 exists to close, so it is refused
        // rather than handed out uncheckpointed.
        return process::Failure(
            "GPU module: could not checkpoint the assigned device set: " +
            checkpointed.error());
      }
    }

    // CUDA_VISIBLE_DEVICES is what makes the assignment take effect when no
    // device isolator restricts the container: CUDA in the task enumerates
    // only the named devices. It does not stop a task from opening the other
    // device files or from setting the variable itself in its command; only
    // the device isolator does that.
    //
    // A command executor reads the task environment; everything else reads
    // the container environment. Same split injectVolume() makes.
    Environment environment;
    Environment::Variable* variable = environment.add_variables();
    variable->set_name("CUDA_VISIBLE_DEVICES");
    variable->set_type(Environment::Variable::VALUE);
    variable->set_value(medios::modules::gpu::visibleDevices(granted));

    ContainerLaunchInfo launchInfo;
    if (containerConfig.has_task_info()) {
      *launchInfo.mutable_task_environment() = environment;
    } else {
      *launchInfo.mutable_environment() = environment;
    }

    // A container with its own rootfs gets a private /dev that holds only the
    // standard device nodes, and its image's libraries in place of the
    // host's. CUDA in it needs the assigned devices' files and the driver
    // libraries, and the device isolator is not always listed, so both are
    // bound in here. A container without a rootfs sees the host's /dev and
    // libraries and needs neither.
    if (containerConfig.has_rootfs()) {
      Try<Nothing> delivered =
        deliverIntoRootfs(containerConfig, granted, &launchInfo);
      if (delivered.isError()) {
        // The assignment is recorded in `allocations`, so cleanup(), which
        // the containerizer runs after a failed prepare(), releases it.
        return process::Failure(
            std::string(ASSIGN_MODULE) + ": container " +
            stringify(containerId) + ": " + delivered.error());
      }
    }

    // The UUIDs, because the count alone cannot say which device a container
    // is on: seat holders share a device, and on a node with several devices
    // the count is the same whichever one was placed (medios#482).
    std::vector<std::string> held;
    foreach (const medios::modules::gpu::Device& device, granted) {
      held.push_back(
          device.uuid + " (index " + stringify(device.index) + ")");
    }

    LOG(INFO) << "GPU module: container " << containerId << " assigned "
              << granted.size() << " '" << resource << "' device(s) "
              << strings::join(", ", held) << "; CUDA_VISIBLE_DEVICES="
              << variable->value();

    return launchInfo;
  }

  // An assignment is made once, at prepare, so the device set a container
  // holds cannot change after it starts. The base implementation is a no-op,
  // which would accept a change and silently not apply it: a container would
  // be charged for a GPU it cannot use, or keep one it no longer holds.
  // Refused by name instead. Dropping dynamic change is recorded on #450;
  // the case that reaches here is a second task landing in an existing
  // executor, since a container's resources are the sum of its tasks.
  process::Future<Nothing> update(
      const ContainerID& containerId,
      const Resources& resourceRequests,
      const google::protobuf::Map<std::string, Value::Scalar>& resourceLimits)
      override
  {
    const std::string key = containerId.SerializeAsString();

    Option<std::string> resource;
    double held = 0.0;
    bool survived = false;
    {
      std::lock_guard<std::mutex> guard(mutex);
      auto it = allocations.find(key);
      if (it != allocations.end()) {
        resource = it->second.resource;
        held = it->second.claim;
      }
      survived = recovered.count(key) > 0;
    }

    // A container that outlived the node process is left alone. This module
    // has no record of what it holds, so it cannot tell a grown claim from
    // the claim the container launched with, and the node destroys a
    // container whose update fails: refusing here would kill every surviving
    // GPU executor on the first terminal task after a restart.
    if (survived) {
      LOG(INFO) << "GPU module: update() for recovered container "
                << containerId << " left alone (no allocation record "
                << "survived the restart to compare it against)";
      return Nothing();
    }

    // A container this module holds nothing for is still refused if it has
    // grown a GPU claim, which is a GPU task landing in an executor that
    // started without one. prepare() declined that container and assigned
    // nothing, so there is no device it can use; letting the update through
    // would charge it for one anyway. Growth from zero is the same refusal as
    // growth from one, and this is the shape it takes.
    if (resource.isNone()) {
      foreachkey (const std::string& name, units) {
        const Option<double> claim = scalar(resourceRequests, name);
        if (claim.isSome() && *claim != 0.0) {
          return process::Failure(
              "This module cannot add devices to a running container: the "
              "assignment is made once at prepare, and this container was "
              "prepared without one. It has grown a '" + name + "' claim of " +
              stringify(*claim) + ", which no device on this node can serve. "
              "Run the GPU task in its own executor.");
        }
      }

      return Nothing();
    }

    // Every declared name is read, not just the one held. A container's
    // resources are the sum of its tasks, so a second GPU task can put a
    // claim under a name this container was never prepared for; reading only
    // the held name would let that through as an unchanged claim.
    foreachkey (const std::string& name, units) {
      if (name == *resource) {
        continue;
      }

      const Option<double> other = scalar(resourceRequests, name);
      if (other.isSome() && *other != 0.0) {
        return process::Failure(
            "This container holds '" + *resource + "' devices and has grown a "
            "'" + name + "' claim of " + stringify(*other) + ". A container "
            "holds devices under one resource name, and the assignment is made "
            "once at prepare, so the second name cannot be served. Run the "
            "GPU task in its own executor.");
      }
    }

    const double amount = scalar(resourceRequests, *resource).getOrElse(0.0);

    // The same whole-claim rule prepare() applies, so a fractional claim is
    // refused by name here too rather than compared as a number that reads
    // close enough to the one held.
    if (!medios::modules::gpu::isWholeClaim(amount)) {
      return process::Failure(
          "The '" + *resource + "' claim of " + stringify(amount) + " is not "
          "a whole number. One device provides " +
          stringify(units.at(*resource).perDevice) + " of this resource and "
          "cannot be divided further");
    }

    // Shrinking is ordinary and must succeed. The node calls update() with the
    // reduced claim on every terminal task status and destroys the container
    // when the call fails, so refusing a shrink kills an executor on normal
    // task completion — which is what refusing every change did.
    //
    // Nothing is revoked here. The container keeps its assigned devices until
    // cleanup, which over-grants to a container relative to its claim but
    // never assigns a whole device to a second container: the devices are
    // unbound at cleanup, not at shrink. Revoking mid-life would mean
    // changing an environment and a device declaration already in effect,
    // which this seam cannot express.
    if (amount <= held) {
      if (amount < held) {
        LOG(INFO) << "GPU module: container " << containerId << " '"
                  << *resource << "' claim fell from " << held << " to "
                  << amount << "; its assigned devices stay assigned "
                  << "until cleanup";
      }
      return Nothing();
    }

    // Growth cannot be honoured. The assignment is made once at prepare, so
    // anything added now would be charged to the container without being
    // usable by it. The case that reaches here is a second GPU task landing
    // in an existing executor, since a container's resources are the sum of
    // its tasks.
    return process::Failure(
        "This module cannot add devices to a running container: the "
        "assignment is made once at prepare, so a container holding " +
        stringify(held) + " of '" + *resource + "' cannot grow to " +
        stringify(amount) + ". Run one GPU task per executor.");
  }

  process::Future<Nothing> cleanup(const ContainerID& containerId) override
  {
    std::lock_guard<std::mutex> guard(mutex);

    auto it = allocations.find(containerId.SerializeAsString());
    if (it == allocations.end()) {
      // cleanup() is guaranteed after prepare(), including for containers
      // this isolator declined, so an unknown container is ordinary.
      return Nothing();
    }

    // Released here, not at shrink, so a device held whole is never offered
    // to a second container while the first can still use it.
    //
    // Only a container that took its devices whole releases them. A container
    // holding a share of a device recorded nothing, and erasing its UUID here
    // would free a device some other container is still holding whole: DR-035
    // leaves the two able to coexist on one device, with core's subtraction
    // the only thing that keeps them apart.
    if (it->second.whole) {
      foreach (const medios::modules::gpu::Device& device, it->second.devices) {
        bound.erase(device.uuid);
      }
    }

    // Best-effort: the container is gone either way, and a checkpoint left
    // behind for a directory the containerizer is about to remove costs
    // nothing further than the warning.
    Try<Nothing> removed =
      medios::modules::gpu::removeAllocationCheckpoint(it->second.directory);

    if (removed.isError()) {
      LOG(WARNING) << "GPU module: could not remove the allocation "
                   << "checkpoint for container " << containerId << ": "
                   << removed.error();
    }

    allocations.erase(it);

    return Nothing();
  }

private:
  // Binds the files of the assigned devices, the driver control device and
  // the unified-memory devices that exist on the host into the container's
  // rootfs at their host paths, then injects the driver volume. The files
  // are the ones the device isolator declares, so a node listing both
  // isolators grants access to exactly the files bound here.
  Try<Nothing> deliverIntoRootfs(
      const ContainerConfig& containerConfig,
      const std::vector<medios::modules::gpu::Device>& granted,
      ContainerLaunchInfo* launchInfo)
  {
    Try<std::vector<DeviceAccess>> files =
      medios::modules::gpu::deviceDeclarations(granted, [](const std::string& path) {
        return os::exists(path);
      });

    if (files.isError()) {
      return Error(files.error());
    }

    foreach (const medios::modules::gpu::DeviceMount& mount,
             medios::modules::gpu::deviceMounts(*files, containerConfig.rootfs())) {
      *launchInfo->add_mounts() =
        medios::internal::protobuf::node::createContainerMount(
            mount.source, mount.target, MS_BIND);
    }

    return injectVolume(containerConfig, launchInfo);
  }

  // Mounts the driver volume read-only into the container's rootfs and puts
  // its directories on PATH and LD_LIBRARY_PATH. Nothing is injected when the
  // volume could not be created at load, or for an image the volume declines.
  Try<Nothing> injectVolume(
      const ContainerConfig& containerConfig,
      ContainerLaunchInfo* launchInfo)
  {
    if (volume.isNone()) {
      return Nothing();
    }

    // Only a Docker image carries the manifest the volume inspects. A
    // container with a rootfs from elsewhere is left alone rather than
    // refused: its device files are bound in, and whether its own libraries
    // suffice depends on its image.
    if (!containerConfig.has_docker() ||
        !containerConfig.docker().has_manifest()) {
      LOG(INFO) << "GPU module: no Docker manifest for this container; "
                << "driver libraries were not injected";
      return Nothing();
    }

    const ::docker::spec::v1::ImageManifest manifest =
      containerConfig.docker().manifest();

    if (!volume->shouldInject(manifest)) {
      return Nothing();
    }

    const std::string target =
      path::join(containerConfig.rootfs(), volume->CONTAINER_PATH());

    Try<Nothing> mkdir = os::mkdir(target);
    if (mkdir.isError()) {
      return Error(
          "Failed to create the driver volume directory at '" + target +
          "': " + mkdir.error());
    }

    const std::vector<medios::modules::gpu::VolumeMount> mounts =
      medios::modules::gpu::driverVolumeMounts(volume->HOST_PATH(), target);

    foreach (const medios::modules::gpu::VolumeMount& mount, mounts) {
      *launchInfo->add_mounts() =
        medios::internal::protobuf::node::createContainerMount(
            mount.source,
            mount.target,
            medios::modules::gpu::volumeMountFlags(mount.mode));
    }

    // Added to the environment that already holds CUDA_VISIBLE_DEVICES, in
    // the same place prepare() put it: the task environment for a command
    // executor, the container environment otherwise.
    if (containerConfig.has_task_info()) {
      launchInfo->mutable_task_environment()->MergeFrom(volume->ENV(manifest));
    } else {
      launchInfo->mutable_environment()->MergeFrom(volume->ENV(manifest));
    }

    return Nothing();
  }

  // What a container holds: the resource it asked for, how much of it core
  // charged the container, and the devices that claim was placed on. The
  // resource name is kept because update() has to read the same unit
  // prepare() placed against. The directory is kept because cleanup() only
  // receives a ContainerID and has to know where this container's checkpoint
  // lives to remove it.
  struct Allocation
  {
    std::string resource;
    double claim;
    std::vector<medios::modules::gpu::Device> devices;
    bool whole;   // Whether those devices were recorded in `bound`.
    std::string directory;
  };

  const std::map<std::string, medios::modules::gpu::UnitSet> units;
  const Option<medios::internal::node::NvidiaVolume> volume;

  std::mutex mutex;
  // Keyed by the serialised id: ContainerID has no ordering, and a nested
  // container must not collide with its parent.
  std::map<std::string, Allocation> allocations;

  // The devices held whole by a container that has not been cleaned up. A
  // resource whose devices provide more than one of it binds nothing: its
  // holders are meant to share, and core counts how many there can be.
  std::set<std::string> bound;

  // Serialised ids of containers recovered from a previous node process.
  hashset<std::string> recovered;
};


// Restricts a container to the devices the assignment isolator assigned it.
//
// Keeps no state of its own. The assignment isolator checkpoints each
// assignment under the container's directory before its prepare() returns,
// and the containerizer runs prepare() calls in --isolation order, one after
// another, so by the time this prepare() runs the checkpoint is the
// assignment. Reading it instead of sharing the other isolator's memory means
// the two modules hold no reference to each other, and a node restart needs
// no recover() here: nothing in this isolator outlives one prepare() call.
class GpuDevicesIsolator : public Isolator
{
public:
  explicit GpuDevicesIsolator(
      const std::map<std::string, medios::modules::gpu::UnitSet>& _units)
    : units(_units) {}

  process::Future<Option<ContainerLaunchInfo>> prepare(
      const ContainerID& containerId,
      const ContainerConfig& containerConfig) override
  {
    // The checkpoint is read first, for every container. Deciding from this
    // isolator's own units file whether a container asked for a GPU would
    // skip a container the assignment isolator did assign when the two units
    // files disagree, and that container would start with
    // CUDA_VISIBLE_DEVICES naming a device it is not allowed to open.
    Try<Option<medios::modules::gpu::AllocationCheckpoint>> checkpoint =
      medios::modules::gpu::readAllocationCheckpoint(
          containerConfig.directory());

    if (checkpoint.isError()) {
      return process::Failure(
          std::string(DEVICES_MODULE) + ": the assignment checkpoint for "
          "container " + stringify(containerId) + " could not be read: " +
          checkpoint.error());
    }

    if (checkpoint->isNone()) {
      const std::vector<std::string> asked =
        askedResources(units, containerConfig.resources());

      // No assignment and no GPU claim: a non-GPU container on a GPU node,
      // which gets no declarations at all, so the containerizer's empty
      // short-circuit applies.
      if (asked.empty()) {
        return None();
      }

      // A GPU claim with no assignment is a node configured without the
      // assignment isolator, or with it listed after this one. Declaring
      // nothing would start a container the controller charged for a GPU it
      // cannot open, so the launch is refused and the refusal names the fix.
      return process::Failure(
          std::string(DEVICES_MODULE) + ": container " +
          stringify(containerId) + " asked for '" + asked.front() + "' but no "
          "device assignment was recorded for it. List " + ASSIGN_MODULE +
          " before " + DEVICES_MODULE + " in --isolation.");
    }

    const medios::modules::gpu::AllocationCheckpoint& assigned = **checkpoint;

    if (units.count(assigned.resource) == 0) {
      return process::Failure(
          std::string(DEVICES_MODULE) + ": container " +
          stringify(containerId) + " was assigned '" + assigned.resource +
          "' devices, which the units file this module loaded does not "
          "declare. Give both GPU isolators the same units file.");
    }

    Try<std::vector<medios::modules::gpu::Device>> granted =
      medios::modules::gpu::devicesByUuid(
          units.at(assigned.resource), assigned.deviceUuids);

    if (granted.isError()) {
      return process::Failure(
          std::string(DEVICES_MODULE) + ": container " +
          stringify(containerId) + " was assigned " + granted.error() +
          ". Give both GPU isolators the same units file.");
    }

    return declare(containerId, assigned.resource, *granted);
  }

private:
  process::Future<Option<ContainerLaunchInfo>> declare(
      const ContainerID& containerId,
      const std::string& resource,
      const std::vector<medios::modules::gpu::Device>& granted)
  {
    Try<std::vector<DeviceAccess>> declarations =
      medios::modules::gpu::deviceDeclarations(granted, [](const std::string& path) {
        return os::exists(path);
      });

    if (declarations.isError()) {
      return process::Failure(declarations.error());
    }

    ContainerLaunchInfo launchInfo;
    foreach (const DeviceAccess& declaration, *declarations) {
      *launchInfo.add_devices() = declaration;
    }

    // The declarations only grant access. For a container with its own
    // rootfs, the assignment isolator has already bound these files into it.
    LOG(INFO) << "GPU module: container " << containerId << " declared "
              << declarations->size() << " device file(s) for its "
              << granted.size() << " '" << resource << "' device(s)";

    return launchInfo;
  }

  const std::map<std::string, medios::modules::gpu::UnitSet> units;
};
#else
// Elsewhere: load, discover, deliver nothing.
class GpuAssignIsolator : public Isolator
{
public:
  explicit GpuAssignIsolator(const std::vector<medios::modules::gpu::Device>&) {}
};

class GpuDevicesIsolator : public Isolator
{
public:
  explicit GpuDevicesIsolator(const std::vector<medios::modules::gpu::Device>&) {}
};
#endif // __linux__

} // namespace mediosgpu {


// What both isolators need before they can be constructed: the operator's
// units file, read and checked against what the driver reports. None, with
// the reason logged under `module`'s name, when either cannot be had.
struct LoadedUnits
{
  std::vector<medios::modules::gpu::Device> discovered;
  std::map<std::string, medios::modules::gpu::UnitSet> byResource;
};


static Option<LoadedUnits> loadUnits(
    const Parameters& parameters,
    const char* module)
{
  // The operator's declaration of what this host has, and what each GPU
  // resource name means on it (DR-034, FAC3-0028). Required: a module with no
  // declaration has nothing to offer, and inferring one from whatever the
  // driver reports is how a node ends up offering hardware the operator did
  // not mean to give it.
  Option<std::string> path;
  for (int i = 0; i < parameters.parameter_size(); i++) {
    if (parameters.parameter(i).key() == "units") {
      path = parameters.parameter(i).value();
    }
  }

  if (path.isNone()) {
    LOG(ERROR) << module << ": no 'units' parameter. Point it at the units "
               << "file that says which devices each GPU resource name means "
               << "on this host (medios#467)";
    return None();
  }

  Try<std::string> contents = os::read(*path);
  if (contents.isError()) {
    LOG(ERROR) << module << ": cannot read the units file '" << *path
               << "': " << contents.error();
    return None();
  }

  Try<std::vector<medios::modules::gpu::UnitDeclaration>> declarations =
    medios::modules::gpu::parseUnits(*contents);

  if (declarations.isError()) {
    LOG(ERROR) << module << ": the units file '" << *path << "' is not usable: "
               << declarations.error();
    return None();
  }

  // Discovery second, because it is now checked against the declaration
  // rather than being the declaration. A discovery failure refuses the load:
  // every declared device has to be found on this host, and a module that
  // cannot ask the driver cannot find any of them.
  std::unique_ptr<medios::modules::gpu::DeviceSource> source =
    medios::modules::gpu::nvmlDeviceSource();

  Try<std::vector<medios::modules::gpu::Device>> discovered =
    medios::modules::gpu::discover(*source);

  if (discovered.isError()) {
    LOG(ERROR) << module << ": device discovery failed: " << discovered.error()
               << "; the units file declares devices that cannot be checked "
               << "against this host, so the module refuses to load";
    return None();
  }

  foreach (const medios::modules::gpu::Device& device, *discovered) {
    LOG(INFO) << module << ": device " << device.index
              << " is /dev/nvidia" << device.minorNumber
              << " (" << device.uuid << ")";
  }

  Try<std::vector<medios::modules::gpu::UnitSet>> units =
    medios::modules::gpu::resolveUnits(*declarations, *discovered);

  if (units.isError()) {
    LOG(ERROR) << module << ": the units file '" << *path << "' cannot be "
               << "honoured on this host: " << units.error();
    return None();
  }

  LoadedUnits loaded;
  loaded.discovered = *discovered;

  foreach (const medios::modules::gpu::UnitSet& unit, *units) {
    loaded.byResource.emplace(unit.resource, unit);

    LOG(INFO) << module << ": '" << unit.resource << "' is "
              << unit.devices.size() << " device(s) from " << *path
              << ", each providing " << unit.perDevice << "; this node's "
              << "--resources should declare "
              << unit.perDevice * unit.devices.size();
  }

  return loaded;
}


static Isolator* createAssign(const Parameters& parameters)
{
  Option<LoadedUnits> loaded =
    loadUnits(parameters, mediosgpu::ASSIGN_MODULE);

  if (loaded.isNone()) {
    return nullptr;
  }

#ifdef __linux__
  // The driver volume holds the libraries a container with its own image
  // needs. Its absence is logged and does not refuse the load: assignment
  // does not depend on it, and a container without its own rootfs uses the
  // host's libraries.
  Option<medios::internal::node::NvidiaVolume> volume;

  Try<medios::internal::node::NvidiaVolume> created =
    medios::internal::node::NvidiaVolume::create();

  if (created.isError()) {
    LOG(WARNING) << mediosgpu::ASSIGN_MODULE << ": the driver volume is "
                 << "unavailable: " << created.error()
                 << "; containers with their own rootfs will get no driver libraries";
  } else {
    volume = *created;
  }

  return new mediosgpu::GpuAssignIsolator(loaded->byResource, volume);
#else
  return new mediosgpu::GpuAssignIsolator(loaded->discovered);
#endif // __linux__
}


static Isolator* createDevices(const Parameters& parameters)
{
  // The same units file as the assignment isolator. The devices isolator
  // needs it to tell a GPU container from any other and to resolve an
  // assigned UUID to its device file; a mismatch between the two files is
  // refused at prepare, naming the device.
  Option<LoadedUnits> loaded =
    loadUnits(parameters, mediosgpu::DEVICES_MODULE);

  if (loaded.isNone()) {
    return nullptr;
  }

#ifdef __linux__
  return new mediosgpu::GpuDevicesIsolator(loaded->byResource);
#else
  return new mediosgpu::GpuDevicesIsolator(loaded->discovered);
#endif // __linux__
}


medios::modules::Module<Isolator> com_medioslab_medios_gpu_assign(
    MEDIOS_MODULE_API_VERSION,
    MEDIOS_VERSION,
    "medios-lab",
    "https://github.com/medios-lab",
    "GPU assignment isolator: places each claim on devices and sets "
    "CUDA_VISIBLE_DEVICES.",
    nullptr, // No compatibility check.
    createAssign);


medios::modules::Module<Isolator> com_medioslab_medios_gpu_devices(
    MEDIOS_MODULE_API_VERSION,
    MEDIOS_VERSION,
    "medios-lab",
    "https://github.com/medios-lab",
    "GPU device isolator: restricts each container to its assigned devices "
    "through cgroups/devices.",
    nullptr, // No compatibility check.
    createDevices);
