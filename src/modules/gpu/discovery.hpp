// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

#ifndef __MODULES_GPU_DISCOVERY_HPP__
#define __MODULES_GPU_DISCOVERY_HPP__

#include <memory>
#include <string>
#include <vector>

#include <stout/try.hpp>

namespace medios {
namespace modules {
namespace gpu {

// What the driver says exists, and nothing else. No capacity, no
// configuration, no allocation state: discovery answers "which physical
// devices are present", and a caller that wants to know who holds a device
// asks the ledger, which this does not touch.
struct Device
{
  unsigned int index;         // NVML enumeration index.
  unsigned int minorNumber;   // /dev/nvidia<minorNumber>.
  std::string uuid;           // The driver's identity, "GPU-<uuid>".
};


bool operator==(const Device& left, const Device& right);
bool operator!=(const Device& left, const Device& right);


// The driver, behind one narrow interface, so discovery can be exercised
// without a GPU.
//
// `nvml` is a namespace of free functions over a dlopen'd library with
// process-global state, which cannot be substituted in a test. Everything
// interesting about discovery is the handling: an index whose minor number
// fails, a count that disagrees with the handles, a driver that is present
// but refuses. Those paths are the reason this interface exists; a fake
// implements them in a few lines and the real one stays thin enough to read.
class DeviceSource
{
public:
  virtual ~DeviceSource() = default;

  virtual Try<unsigned int> count() = 0;
  virtual Try<unsigned int> minorNumber(unsigned int index) = 0;
  virtual Try<std::string> uuid(unsigned int index) = 0;
};


// Devices the source reports, ordered by index.
//
// Fails rather than returning a short list when any device cannot be
// described. A partial inventory is the more dangerous answer: a caller
// cannot distinguish "this host has one GPU" from "this host has four and
// three could not be read", and the second one silently under-reports
// hardware the operator paid for.
Try<std::vector<Device>> discover(DeviceSource& source);


// A source backed by the real driver through the `nvml` wrapper. Returns an
// error from every call when NVML is unavailable, which is every non-Linux
// host and any Linux host without the driver library installed.
std::unique_ptr<DeviceSource> nvmlDeviceSource();

} // namespace gpu {
} // namespace modules {
} // namespace medios {

#endif // __MODULES_GPU_DISCOVERY_HPP__
