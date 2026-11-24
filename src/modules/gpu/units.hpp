// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

#ifndef __MODULES_GPU_UNITS_HPP__
#define __MODULES_GPU_UNITS_HPP__

#include <set>
#include <string>
#include <vector>

#include <stout/try.hpp>

#include "modules/gpu/discovery.hpp"

namespace medios {
namespace modules {
namespace gpu {

// The operator's declaration of what a GPU resource name means on this host
// (DR-035, medios#470). It says which physical devices stand behind the name,
// named by the driver's UUID rather than by index or minor number, and how
// much of the resource one of those devices provides.
//
// The file the module parameter `units` points at:
//
//   { "units": [
//     { "resource": "gpus",        "per_device": 1,
//       "devices": ["GPU-804c560c-...", "GPU-11f0aa31-..."] },
//     { "resource": "gpu_replica", "per_device": 10,
//       "devices": ["GPU-804c560c-..."] }
//   ] }
//
// `per_device` is in the resource's own unit. A device no one shares is
// `per_device: 1`. A time-sliced device is `per_device: N` seats, and every
// holder of a seat gets that same device. The node's --resources carries
// `per_device` times the number of devices declared for the name; the module
// cannot read --resources and keeps no count of its own.
struct UnitDeclaration
{
  std::string resource;
  size_t perDevice;                   // Resource per device, in its own unit.
  std::vector<std::string> devices;   // Driver UUIDs, in the operator's order.
};


// One resource name, resolved against what discovery reported for each UUID.
// This is what a claim is placed on.
struct UnitSet
{
  std::string resource;
  size_t perDevice;
  std::vector<Device> devices;
};


// Reads the units file. Rejects a file that is not the shape above, a
// `per_device` that is not a whole number of at least one, a resource name
// declared twice, a unit that lists no devices or the same device twice, and
// the `mode`, `count` and `occupancy` fields that DR-034 used, which are
// refused by name rather than ignored.
Try<std::vector<UnitDeclaration>> parseUnits(const std::string& contents);


// The start-time check (FAC3-0028): every declared device must be on this
// host, and no device may be declared under two resource names. Refuses
// rather than adjusting the declaration to fit the host, naming the unit that
// could not be honoured.
Try<std::vector<UnitSet>> resolveUnits(
    const std::vector<UnitDeclaration>& declarations,
    const std::vector<Device>& discovered);


// Whether a scalar claim is a whole number of the resource.
//
// One definition, because the rule is applied at prepare and again at update
// and the two must not drift. Bounded, so the comparison is defined for any
// double a claim can carry: a claim too large to be a whole number of devices
// is refused by the caller that knows what the declaration provides, not by
// this.
bool isWholeClaim(double amount);


// Which devices serve a claim, and whether the claim took them whole.
//
// The second half is what the caller records. A container holding a device
// whole must keep the next whole claim off it; a container holding a share of
// one must not, because the holders of a shared device are meant to be on it
// together. Reported here rather than worked out again by the caller, because
// the rule that decides it is the one below and two copies of it would drift.
struct Placement
{
  std::vector<Device> devices;
  bool whole;
};


// Which devices serve a claim of `amount` of this resource, given the UUIDs
// of the devices currently held whole.
//
// Quantity is core's: it has already subtracted this claim from the node's
// declared total, and this does not check it against a second book. What is
// decided here is identity, and the two rules are DR-035's:
//
//   - A claim that is a whole multiple of `per_device` takes whole devices.
//     For `per_device: 1` that is every claim, and the devices taken are ones
//     no other container holds.
//   - A smaller claim is a seat, and lands on one device. Seats are not
//     recorded, so every holder of a seat unit gets the same device.
//
// `bound` is consulted only for whole devices. A share of a device is placed
// without regard to it, which is what lets several containers hold the one
// device at once.
//
// Fails, naming the resource, on a fractional claim, a claim of nothing, a
// claim larger than the devices declared can provide, and a whole-device
// claim with too few devices free.
Try<Placement> place(
    const UnitSet& unit,
    double amount,
    const std::set<std::string>& bound);

} // namespace gpu {
} // namespace modules {
} // namespace medios {

#endif // __MODULES_GPU_UNITS_HPP__
