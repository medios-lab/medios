// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

#ifndef __MODULES_GPU_DELIVERY_HPP__
#define __MODULES_GPU_DELIVERY_HPP__

#include <functional>
#include <string>
#include <vector>

#include <medios/medios.hpp>

#include <stout/try.hpp>

#include "modules/gpu/discovery.hpp"

namespace medios {
namespace modules {
namespace gpu {

// What a container granted these devices must be allowed to open.
//
// A CUDA process needs more than its own GPUs. `/dev/nvidiactl` is the
// driver's control device and nothing works without it. `/dev/nvidia-uvm` is
// unified memory, created by `nvidia-modprobe` on first use rather than at
// boot, and `/dev/nvidia-uvm-tools` alongside it; both are declared only when
// they exist, because the containerizer resolves a declaration by `stat`ing
// the path and a declaration for an absent device fails the launch.
//
// Granting a device the container does not need is not a small error here:
// every declaration is a device this container can open for the rest of its
// life, so the list is exactly the granted GPUs plus the control devices, and
// nothing is added because it might be useful.
//
// `exists` is injected so the decision can be tested without the driver.
Try<std::vector<DeviceAccess>> deviceDeclarations(
    const std::vector<Device>& granted,
    const std::function<bool(const std::string&)>& exists);


// One bind mount: the host's own device node, and where it belongs inside a
// container that has a filesystem of its own.
struct DeviceMount
{
  std::string source;
  std::string target;
};


// Where a container's declared device files come from when it has a rootfs.
//
// A declaration grants permission to open a device; it does not create the
// file. `filesystem/linux` mounts a private tmpfs at /dev and populates only
// the standard nodes, so inside an image container a granted GPU is the right
// to open a path that is not there.
//
// The source is the host's own node rather than a copy of it. A copy has to
// live somewhere, and the runtime directory is under /run on standard hosts,
// which is mounted `nodev`; a device node on a nodev mount cannot be opened
// at all.
//
// Only declared devices are mounted. The in-tree isolator globs /dev/nvidia*
// and mounts whatever it finds, which hands every GPU container every device
// on the node and gives back exactly what the declaration seam exists to
// enforce. These mounts are the declarations and nothing else, so a container
// granted one GPU still cannot reach the others.
std::vector<DeviceMount> deviceMounts(
    const std::vector<DeviceAccess>& declarations,
    const std::string& rootfs);


// The device file for a GPU, by its minor number: /dev/nvidia0 is minor 0.
// The index is NOT the minor number — discovery reports both because they
// differ on hosts where devices were enumerated in a different order.
std::string devicePath(const Device& device);


// What one mount of the driver volume is for. The mount flags stay with the
// isolator, which is compiled only for Linux; the sequence lives here, where
// a test can read it on any host.
enum class VolumeMountMode
{
  BIND,                // Carry the volume in. Not recursive.
  REMOUNT_READ_ONLY,   // Make the mount just made read-only.
};


struct VolumeMount
{
  std::string source;
  std::string target;
  VolumeMountMode mode;
};


// How the driver volume is put inside a container: bound, then remounted
// read-only, and never recursively.
//
// Two mounts because the kernel ignores MS_RDONLY on a bind; read-only takes
// a second call carrying MS_REMOUNT. `fs::mount` already adds that remount
// for any read-only bind, so asking for it in one call does arrive read-only
// at the top of the volume. Saying it here is what makes the intent legible
// at the point the mount is declared, and keeps it true if that helper
// changes.
//
// Not recursive because a read-only remount reaches the mount it names and no
// deeper. Under a recursive bind, a filesystem mounted beneath the volume is
// carried into the container keeping the mode it had, so it stays writable
// behind a volume that reports itself read-only. Without MS_REC it is not
// visible in the container at all, which is the side that cannot be written
// through. This module mounts nothing beneath the volume: it mounts at the top
// of it, and only when the filesystem there is noexec, then copies files in.
// What is mounted beneath it on any given host is that host's mount table, not
// something this code decides. Same shape isolators/filesystem/linux.cpp uses
// for a read-only volume, for the same reason (medios#481).
std::vector<VolumeMount> driverVolumeMounts(
    const std::string& hostPath,
    const std::string& target);


#ifdef __linux__
// The mount flags for one mode. Here rather than at the call site so the flag
// word itself is testable: the absence of MS_REC and the presence of
// MS_REMOUNT are the whole of what medios#481 asked to be held in place, and a
// test one level above this, over the modes, would pass without either.
unsigned long volumeMountFlags(VolumeMountMode mode);
#endif // __linux__

} // namespace gpu {
} // namespace modules {
} // namespace medios {

#endif // __MODULES_GPU_DELIVERY_HPP__
