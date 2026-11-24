// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

#include "modules/gpu/checkpoint.hpp"

#include <string>
#include <vector>

#include <stout/error.hpp>
#include <stout/foreach.hpp>
#include <stout/json.hpp>
#include <stout/option.hpp>
#include <stout/os/exists.hpp>
#include <stout/os/rm.hpp>
#include <stout/os/write.hpp>
#include <stout/path.hpp>
#include <stout/result.hpp>
#include <stout/stringify.hpp>

using std::string;
using std::vector;

namespace medios {
namespace modules {
namespace gpu {

namespace {

// The 2^53 bound `isWholeClaim` and the units parser both use: every integer
// below it is exactly representable as a double, so a claim of that size
// round-trips through JSON without losing the value that decides how much of
// the resource was granted.
const double CLAIM_BOUND = 9007199254740992.0;

} // namespace {


string allocationCheckpointPath(const string& containerDirectory)
{
  return path::join(containerDirectory, "gpu_allocation.checkpoint");
}


Try<Nothing> writeAllocationCheckpoint(
    const string& containerDirectory,
    const AllocationCheckpoint& allocation)
{
  JSON::Array devices;
  foreach (const string& uuid, allocation.deviceUuids) {
    devices.values.push_back(uuid);
  }

  JSON::Object object;
  object.values["resource"] = allocation.resource;
  object.values["claim"] = allocation.claim;
  object.values["device_uuids"] = devices;
  object.values["whole"] = allocation.whole;

  const string path = allocationCheckpointPath(containerDirectory);

  Try<Nothing> written = os::write(path, stringify(object));
  if (written.isError()) {
    return Error(
        "Failed to write the allocation checkpoint '" + path + "': " +
        written.error());
  }

  return Nothing();
}


Try<Option<AllocationCheckpoint>> readAllocationCheckpoint(
    const string& containerDirectory)
{
  const string path = allocationCheckpointPath(containerDirectory);

  if (!os::exists(path)) {
    return None();
  }

  Try<string> contents = os::read(path);
  if (contents.isError()) {
    return Error(
        "Failed to read the allocation checkpoint '" + path + "': " +
        contents.error());
  }

  Try<JSON::Object> json = JSON::parse<JSON::Object>(*contents);
  if (json.isError()) {
    return Error(
        "The allocation checkpoint '" + path + "' is not a JSON object: " +
        json.error());
  }

  Result<JSON::String> resource = json->at<JSON::String>("resource");
  if (resource.isError()) {
    return Error(
        "The allocation checkpoint '" + path + "' has a 'resource' that is "
        "not a string: " + resource.error());
  }
  if (resource.isNone() || resource->value.empty()) {
    return Error(
        "The allocation checkpoint '" + path + "' has no 'resource'");
  }

  Result<JSON::Number> claim = json->at<JSON::Number>("claim");
  if (claim.isError()) {
    return Error(
        "The allocation checkpoint '" + path + "' has a 'claim' that is not "
        "a number: " + claim.error());
  }
  if (claim.isNone()) {
    return Error("The allocation checkpoint '" + path + "' has no 'claim'");
  }

  const double claimed = claim->as<double>();
  if (claimed < 0.0 || claimed >= CLAIM_BOUND) {
    return Error(
        "The allocation checkpoint '" + path + "' has a 'claim' of " +
        stringify(claimed) + ", which is not a value this module ever "
        "writes");
  }

  Result<JSON::Array> deviceUuids = json->at<JSON::Array>("device_uuids");
  if (deviceUuids.isError()) {
    return Error(
        "The allocation checkpoint '" + path + "' has a 'device_uuids' "
        "that is not an array: " + deviceUuids.error());
  }
  if (deviceUuids.isNone()) {
    return Error(
        "The allocation checkpoint '" + path + "' has no 'device_uuids'");
  }

  Result<JSON::Boolean> whole = json->at<JSON::Boolean>("whole");
  if (whole.isError()) {
    return Error(
        "The allocation checkpoint '" + path + "' has a 'whole' that is not "
        "a boolean: " + whole.error());
  }
  if (whole.isNone()) {
    return Error("The allocation checkpoint '" + path + "' has no 'whole'");
  }

  AllocationCheckpoint result;
  result.resource = resource->value;
  result.claim = claimed;
  result.whole = whole->value;

  foreach (const JSON::Value& value, deviceUuids->values) {
    if (!value.is<JSON::String>()) {
      return Error(
          "The allocation checkpoint '" + path + "' lists a device that is "
          "not a string; devices are checkpointed by driver UUID");
    }

    const string uuid = value.as<JSON::String>().value;
    if (uuid.empty()) {
      return Error(
          "The allocation checkpoint '" + path + "' lists an empty device "
          "UUID");
    }

    result.deviceUuids.push_back(uuid);
  }

  // A whole-device grant with no device, or a seat grant with more than the
  // one device it can ever land on, is not a shape this module writes: it is
  // the checkpoint disagreeing with the code that reads it, not a survivor
  // holding nothing.
  if (result.deviceUuids.empty()) {
    return Error(
        "The allocation checkpoint '" + path + "' lists no devices for a "
        "granted '" + result.resource + "' claim");
  }

  return result;
}


Try<Nothing> removeAllocationCheckpoint(const string& containerDirectory)
{
  const string path = allocationCheckpointPath(containerDirectory);

  if (!os::exists(path)) {
    // cleanup() runs for every container prepare() saw, including ones this
    // module declined; a container that never held a claim never wrote one.
    return Nothing();
  }

  Try<Nothing> removed = os::rm(path);
  if (removed.isError()) {
    return Error(
        "Failed to remove the allocation checkpoint '" + path + "': " +
        removed.error());
  }

  return Nothing();
}

} // namespace gpu {
} // namespace modules {
} // namespace medios {
