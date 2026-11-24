// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

#include "modules/gpu/delivery.hpp"

#ifdef __linux__
#include <sys/mount.h>
#endif // __linux__

#include <string>
#include <vector>

#include <stout/error.hpp>
#include <stout/path.hpp>
#include <stout/stringify.hpp>

using std::function;
using std::string;
using std::vector;

namespace medios {
namespace modules {
namespace gpu {

namespace {

// The driver's control device. Without it a CUDA process cannot talk to the
// driver at all, so its absence is a failure rather than a device to skip.
constexpr char CONTROL_DEVICE[] = "/dev/nvidiactl";

// Unified memory. Created by `nvidia-modprobe` on first use, so a host with
// working GPUs may legitimately not have these yet.
constexpr char UVM_DEVICE[] = "/dev/nvidia-uvm";
constexpr char UVM_TOOLS_DEVICE[] = "/dev/nvidia-uvm-tools";


DeviceAccess declaration(const string& path)
{
  DeviceAccess access;
  access.mutable_device()->set_path(path);

  // Read and write are what a CUDA process needs. `mknod` is granted because
  // a container may recreate the device node inside its own filesystem; the
  // in-tree isolator grants the same three bits for the same reason.
  access.mutable_access()->set_read(true);
  access.mutable_access()->set_write(true);
  access.mutable_access()->set_mknod(true);

  return access;
}

} // namespace {


string devicePath(const Device& device)
{
  return "/dev/nvidia" + stringify(device.minorNumber);
}


vector<DeviceMount> deviceMounts(
    const vector<DeviceAccess>& declarations,
    const string& rootfs)
{
  vector<DeviceMount> mounts;

  for (const DeviceAccess& declaration : declarations) {
    const string& source = declaration.device().path();

    // The target is the same absolute path, resolved under the rootfs, so the
    // device keeps the name every CUDA library expects. The launcher creates a
    // missing target before mounting, which matters here because the private
    // /dev is a fresh tmpfs and none of these files exist in it yet.
    mounts.push_back({source, path::join(rootfs, source)});
  }

  return mounts;
}


Try<vector<DeviceAccess>> deviceDeclarations(
    const vector<Device>& granted,
    const function<bool(const string&)>& exists)
{
  vector<DeviceAccess> declarations;

  // A container granted no GPUs gets no declarations at all, not even the
  // control device. Returning the control device here would hand driver
  // access to every container on the node and would defeat the
  // containerizer's short-circuit for an empty declaration set.
  if (granted.empty()) {
    return declarations;
  }

  for (const Device& device : granted) {
    const string path = devicePath(device);

    // A granted device that is not present is a disagreement between the
    // ledger and the host, not a device to quietly drop: the container would
    // start believing it has a GPU it cannot open.
    if (!exists(path)) {
      return Error(
          "Granted device " + stringify(device.index) + " has no device file "
          "at '" + path + "'");
    }

    declarations.push_back(declaration(path));
  }

  if (!exists(CONTROL_DEVICE)) {
    return Error(
        "The driver control device '" + string(CONTROL_DEVICE) + "' is "
        "absent; a container cannot use a GPU without it");
  }

  declarations.push_back(declaration(CONTROL_DEVICE));

  for (const char* optional : {UVM_DEVICE, UVM_TOOLS_DEVICE}) {
    if (exists(optional)) {
      declarations.push_back(declaration(optional));
    }
  }

  return declarations;
}


vector<VolumeMount> driverVolumeMounts(
    const string& hostPath,
    const string& target)
{
  return {
    {hostPath, target, VolumeMountMode::BIND},
    {hostPath, target, VolumeMountMode::REMOUNT_READ_ONLY},
  };
}


#ifdef __linux__
unsigned long volumeMountFlags(VolumeMountMode mode)
{
  switch (mode) {
    case VolumeMountMode::BIND:
      return MS_BIND;
    case VolumeMountMode::REMOUNT_READ_ONLY:
      return MS_BIND | MS_REMOUNT | MS_RDONLY;
  }

  // Every mode is handled above. Returning the read-only flags rather than 0
  // keeps a mode added later from mounting the driver libraries writable.
  return MS_BIND | MS_REMOUNT | MS_RDONLY;
}
#endif // __linux__

} // namespace gpu {
} // namespace modules {
} // namespace medios {
