// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

#include "modules/gpu/units.hpp"

#include <cmath>
#include <map>
#include <set>
#include <string>
#include <vector>

#include <stout/error.hpp>
#include <stout/foreach.hpp>
#include <stout/json.hpp>
#include <stout/option.hpp>
#include <stout/stringify.hpp>

using std::set;
using std::string;
using std::vector;

namespace medios {
namespace modules {
namespace gpu {

namespace {

// The position of a unit in the file, for a message an operator can act on.
// A resource name would be better, but the name is one of the things that can
// be missing, and "the first unit" beats "a unit".
string nth(size_t index)
{
  return "unit " + stringify(index) + " of the units file";
}


// The fields DR-034 read, refused by name now that DR-035 has replaced them.
// Ignoring one would drop a number the operator wrote: a `count` of 2 beside
// a `per_device` of 10 is an operator who believes one of the two, and which
// one cannot be guessed from the file.
Option<Error> retiredField(
    const JSON::Object& unit, const string& resource, size_t index)
{
  if (unit.values.count("mode") > 0) {
    return Error(nth(index) + " ('" + resource + "') sets 'mode', which is no "
                 "longer read. A device no one shares is \"per_device\": 1 and "
                 "a time-sliced device is \"per_device\": N");
  }

  if (unit.values.count("count") > 0) {
    return Error(nth(index) + " ('" + resource + "') sets 'count', which is no "
                 "longer read. The node's --resources carries the total; the "
                 "units file says what one device provides, as \"per_device\"");
  }

  if (unit.values.count("occupancy") > 0) {
    return Error(nth(index) + " ('" + resource + "') sets 'occupancy', which "
                 "is now \"per_device\": the number of holders one device "
                 "takes is the number of seats it provides");
  }

  return None();
}


Try<UnitDeclaration> parseUnit(const JSON::Object& unit, size_t index)
{
  Result<JSON::String> resource = unit.at<JSON::String>("resource");
  if (resource.isError()) {
    return Error(nth(index) + " has a 'resource' that is not a string: " +
                 resource.error());
  }
  if (resource.isNone() || resource->value.empty()) {
    return Error(nth(index) + " has no 'resource'; a unit says what one "
                 "resource name means and cannot be anonymous");
  }

  Option<Error> retired = retiredField(unit, resource->value, index);
  if (retired.isSome()) {
    return *retired;
  }

  Result<JSON::Number> perDevice = unit.at<JSON::Number>("per_device");
  if (perDevice.isError()) {
    return Error(nth(index) + " ('" + resource->value + "') has a 'per_device' "
                 "that is not a number: " + perDevice.error());
  }
  if (perDevice.isNone()) {
    return Error(nth(index) + " ('" + resource->value + "') has no "
                 "'per_device'; write how much of this resource one device "
                 "provides, which is 1 for a device no one shares");
  }

  // Bounded before the conversion, not after: converting a double that does
  // not fit a size_t is undefined, so it cannot be part of the test that
  // decides whether the value fits. Every integer below the bound is exactly
  // representable as a double, which is what makes the equality meaningful.
  const double declared = perDevice->as<double>();
  const double bound = 9007199254740992.0;  // 2^53.

  if (declared < 1.0 || declared >= bound || declared != std::floor(declared)) {
    return Error(nth(index) + " ('" + resource->value + "') has a 'per_device' "
                 "of " + stringify(declared) + "; it is a whole number of at "
                 "least one, because a device that provides none of a resource "
                 "does not belong to it");
  }

  Result<JSON::Array> devices = unit.at<JSON::Array>("devices");
  if (devices.isError()) {
    return Error(nth(index) + " ('" + resource->value + "') has a 'devices' "
                 "that is not an array: " + devices.error());
  }
  if (devices.isNone()) {
    return Error(nth(index) + " ('" + resource->value + "') has no 'devices'; "
                 "list the driver UUIDs this resource names");
  }

  UnitDeclaration declaration;
  declaration.resource = resource->value;
  declaration.perDevice = static_cast<size_t>(declared);

  set<string> seen;
  foreach (const JSON::Value& device, devices->values) {
    if (!device.is<JSON::String>()) {
      return Error(nth(index) + " ('" + resource->value + "') lists a device "
                   "that is not a string; devices are named by driver UUID");
    }

    const string uuid = device.as<JSON::String>().value;
    if (uuid.empty()) {
      return Error(nth(index) + " ('" + resource->value + "') lists an empty "
                   "device UUID");
    }

    // The same device twice would double what the name provides while the
    // node's --resources counted it once, and for whole devices it would put
    // two holders on the one device.
    if (!seen.insert(uuid).second) {
      return Error(nth(index) + " ('" + resource->value + "') lists '" + uuid +
                   "' twice; one device is listed once");
    }

    declaration.devices.push_back(uuid);
  }

  // A resource with no devices behind it is a name the node would offer
  // nothing under. The --resources total for it is per_device times zero,
  // so this is a file someone half-edited rather than a host with no GPUs.
  if (declaration.devices.empty()) {
    return Error(nth(index) + " ('" + resource->value + "') lists no devices; "
                 "a resource name with no device behind it provides nothing");
  }

  return declaration;
}

} // namespace {


Try<vector<UnitDeclaration>> parseUnits(const string& contents)
{
  Try<JSON::Object> json = JSON::parse<JSON::Object>(contents);
  if (json.isError()) {
    return Error("The units file is not a JSON object: " + json.error());
  }

  Result<JSON::Array> units = json->at<JSON::Array>("units");
  if (units.isError()) {
    return Error("The units file's 'units' is not an array: " + units.error());
  }
  if (units.isNone()) {
    return Error("The units file has no 'units' array");
  }

  // An empty list is refused rather than accepted as "this host offers no
  // GPUs". A node that offers none does not load this module; a units file
  // that declares none is a file someone emptied by mistake.
  if (units->values.empty()) {
    return Error("The units file declares no units; a node with no GPU "
                 "resources does not need this module loaded");
  }

  vector<UnitDeclaration> declarations;
  set<string> names;

  for (size_t index = 0; index < units->values.size(); index++) {
    const JSON::Value& value = units->values.at(index);
    if (!value.is<JSON::Object>()) {
      return Error(nth(index) + " is not a JSON object");
    }

    Try<UnitDeclaration> declaration =
      parseUnit(value.as<JSON::Object>(), index);

    if (declaration.isError()) {
      return Error(declaration.error());
    }

    // One resource name, one declaration. Two entries for a name would each
    // look whole while the node offered the sum of both.
    if (!names.insert(declaration->resource).second) {
      return Error("The units file declares '" + declaration->resource +
                   "' more than once; one resource name has one declaration");
    }

    declarations.push_back(*declaration);
  }

  return declarations;
}


Try<vector<UnitSet>> resolveUnits(
    const vector<UnitDeclaration>& declarations,
    const vector<Device>& discovered)
{
  vector<UnitSet> units;

  // Which resource claimed a device, so the second claim can name the first.
  std::map<string, string> claimedBy;

  foreach (const UnitDeclaration& declaration, declarations) {
    UnitSet unit;
    unit.resource = declaration.resource;
    unit.perDevice = declaration.perDevice;

    foreach (const string& uuid, declaration.devices) {
      const Device* found = nullptr;
      foreach (const Device& device, discovered) {
        if (device.uuid == uuid) {
          found = &device;
          break;
        }
      }

      // FAC3-0028: the declaration is not adjusted to fit the host. A UUID
      // this host does not have is a misconfiguration, and a node that
      // started anyway would offer a share of a device no container can ever
      // be given.
      if (found == nullptr) {
        return Error("The units file declares device '" + uuid + "' under '" +
                     declaration.resource + "', which this host does not "
                     "have. Discovery reported " +
                     stringify(discovered.size()) + " device(s)");
      }

      // Two resource names on one device would offer the same hardware twice
      // under two totals core adds up separately. Enforced on the declaration
      // rather than at run time, where the second grant has already been made.
      auto claim = claimedBy.find(uuid);
      if (claim != claimedBy.end()) {
        return Error("The units file declares device '" + uuid + "' under "
                     "both '" + claim->second + "' and '" +
                     declaration.resource + "'; one device belongs to one "
                     "resource");
      }
      claimedBy[uuid] = declaration.resource;

      unit.devices.push_back(*found);
    }

    units.push_back(unit);
  }

  return units;
}


bool isWholeClaim(double amount)
{
  // Bounded before the comparison for the same reason the parser bounds
  // per_device: every integer below 2^53 is exactly representable as a
  // double, which is what makes the equality mean what it reads as, and a
  // value above it carries no fractional part to detect either way.
  const double bound = 9007199254740992.0;  // 2^53.

  if (amount < 0.0 || amount >= bound) {
    return false;
  }

  return amount == std::floor(amount);
}


Try<Placement> place(
    const UnitSet& unit,
    double amount,
    const set<string>& bound)
{
  if (amount <= 0.0) {
    return Error("The '" + unit.resource + "' claim is " + stringify(amount) +
                 "; a container that holds none of a resource is not placed "
                 "on a device");
  }

  // In double, because per_device times the devices declared can be larger
  // than a size_t holds and the overflow would read as a claim that fits.
  // Checked before the whole-number test so that a claim far too large is
  // refused for the reason it is refusable, rather than for its shape.
  const double total =
    static_cast<double>(unit.perDevice) *
    static_cast<double>(unit.devices.size());

  if (amount > total) {
    return Error("The '" + unit.resource + "' claim of " + stringify(amount) +
                 " is more than this node's " + stringify(unit.devices.size()) +
                 " declared device(s) provide, which is " + stringify(total) +
                 ". Nothing on this host can ever serve it");
  }

  // Refused by name rather than rounded into a grant the requester did not
  // ask for: a device is not divisible below the seat the operator declared,
  // and rounding down grants less than was asked while rounding up grants a
  // share that does not exist.
  if (!isWholeClaim(amount)) {
    return Error("The '" + unit.resource + "' claim of " + stringify(amount) +
                 " is not a whole number. One device provides " +
                 stringify(unit.perDevice) + " of this resource and cannot be "
                 "divided further");
  }

  const size_t claim = static_cast<size_t>(amount);

  // A claim that is not a whole multiple of what one device provides is a
  // share of one device rather than a set of them.
  if (claim % unit.perDevice != 0) {
    // Which device it lands on is not determined for a resource declared over
    // several devices: the module records no seat, so it cannot spread
    // holders, and sending every one of them to the first device would leave
    // the rest of the hardware idle while core counted it as given out.
    // Refused by name until that rule is written (medios#479).
    if (unit.devices.size() > 1) {
      return Error("The '" + unit.resource + "' claim of " + stringify(amount) +
                   " is a share of one device, and this resource is declared "
                   "over " + stringify(unit.devices.size()) + " devices, so "
                   "which one it lands on is not decided. Declare a share of "
                   "one device per resource name, or claim all " +
                   stringify(unit.perDevice) + " of a device");
    }

    return Placement{vector<Device>{unit.devices.front()}, false};
  }

  // Whole devices, and the caller is told so: these are the ones it must
  // record, or the next whole claim is served the same device again.
  const size_t wanted = claim / unit.perDevice;

  vector<Device> free;
  foreach (const Device& device, unit.devices) {
    if (bound.count(device.uuid) == 0) {
      free.push_back(device);
    }
  }

  if (free.size() < wanted) {
    return Error("The '" + unit.resource + "' claim of " + stringify(amount) +
                 " is " + stringify(wanted) + " whole device(s) and only " +
                 stringify(free.size()) + " of this node's " +
                 stringify(unit.devices.size()) + " are free");
  }

  free.resize(wanted);
  return Placement{free, true};
}


Try<vector<Device>> devicesByUuid(
    const UnitSet& unit,
    const vector<string>& uuids)
{
  vector<Device> devices;
  foreach (const string& uuid, uuids) {
    const Device* found = nullptr;
    foreach (const Device& device, unit.devices) {
      if (device.uuid == uuid) {
        found = &device;
        break;
      }
    }

    if (found == nullptr) {
      return Error("device '" + uuid + "' is not declared under '" +
                   unit.resource + "' on this node");
    }

    devices.push_back(*found);
  }

  return devices;
}

} // namespace gpu {
} // namespace modules {
} // namespace medios {
