// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

#include "modules/gpu/discovery.hpp"

#include <memory>
#include <string>
#include <vector>

#include <stout/error.hpp>
#include <stout/stringify.hpp>

#include "modules/gpu/nvml.hpp"

using std::string;
using std::unique_ptr;
using std::vector;

namespace medios {
namespace modules {
namespace gpu {

bool operator==(const Device& left, const Device& right)
{
  return left.index == right.index &&
         left.minorNumber == right.minorNumber &&
         left.uuid == right.uuid;
}


bool operator!=(const Device& left, const Device& right)
{
  return !(left == right);
}


Try<vector<Device>> discover(DeviceSource& source)
{
  Try<unsigned int> count = source.count();
  if (count.isError()) {
    return Error("Failed to get the device count: " + count.error());
  }

  vector<Device> devices;
  devices.reserve(*count);

  for (unsigned int index = 0; index < *count; index++) {
    Try<unsigned int> minor = source.minorNumber(index);
    if (minor.isError()) {
      return Error(
          "Failed to get the minor number of device " + stringify(index) +
          " of " + stringify(*count) + ": " + minor.error());
    }

    Try<string> uuid = source.uuid(index);
    if (uuid.isError()) {
      return Error(
          "Failed to get the UUID of device " + stringify(index) +
          " of " + stringify(*count) + ": " + uuid.error());
    }

    // A device the driver reports without an identity cannot be matched
    // against a declaration, and a declaration is how the operator says what
    // this host has. Reported as a failure rather than carried as an empty
    // string, which would match a second such device and grant the wrong one.
    if (uuid->empty()) {
      return Error(
          "Device " + stringify(index) + " of " + stringify(*count) +
          " reported an empty UUID");
    }

    devices.push_back(Device{index, *minor, *uuid});
  }

  return devices;
}


namespace {

class NvmlDeviceSource : public DeviceSource
{
public:
  Try<unsigned int> count() override
  {
    Try<Nothing> initialized = initialize();
    if (initialized.isError()) {
      return Error(initialized.error());
    }

    return nvml::deviceGetCount();
  }

  Try<unsigned int> minorNumber(unsigned int index) override
  {
    Try<Nothing> initialized = initialize();
    if (initialized.isError()) {
      return Error(initialized.error());
    }

    Try<nvmlDevice_t> handle = nvml::deviceGetHandleByIndex(index);
    if (handle.isError()) {
      return Error("Failed to get a handle for device " + stringify(index) +
                   ": " + handle.error());
    }

    return nvml::deviceGetMinorNumber(*handle);
  }

  Try<string> uuid(unsigned int index) override
  {
    Try<Nothing> initialized = initialize();
    if (initialized.isError()) {
      return Error(initialized.error());
    }

    Try<nvmlDevice_t> handle = nvml::deviceGetHandleByIndex(index);
    if (handle.isError()) {
      return Error("Failed to get a handle for device " + stringify(index) +
                   ": " + handle.error());
    }

    return nvml::deviceGetUuid(*handle);
  }

private:
  // `nvml::initialize()` is idempotent and guarded by a `Once`, so calling it
  // per operation is cheap and means a source constructed before the driver
  // is reachable still works afterwards. Availability is checked here rather
  // than in the constructor for the same reason: construction must not decide
  // a question that can change.
  Try<Nothing> initialize()
  {
    if (!nvml::isAvailable()) {
      return Error("NVML is not available on this host");
    }

    return nvml::initialize();
  }
};

} // namespace {


unique_ptr<DeviceSource> nvmlDeviceSource()
{
  return unique_ptr<DeviceSource>(new NvmlDeviceSource());
}

} // namespace gpu {
} // namespace modules {
} // namespace medios {
