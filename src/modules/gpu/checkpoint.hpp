// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

#ifndef __MODULES_GPU_CHECKPOINT_HPP__
#define __MODULES_GPU_CHECKPOINT_HPP__

#include <string>
#include <vector>

#include <stout/nothing.hpp>
#include <stout/option.hpp>
#include <stout/try.hpp>

namespace medios {
namespace modules {
namespace gpu {

// What prepare() granted a container: the resource it claimed, how much core
// charged it, which devices (named by driver UUID, not index or minor
// number, since those are host- and boot-local) served the claim, and
// whether they were taken whole. Checkpointed under the container's own
// directory so recover() can rebuild what a node restart would otherwise
// lose (medios#471) -- everything else about what a container holds lives
// in the isolator's memory only.
//
// Carries UUIDs rather than resolved Device structs on purpose: a checkpoint
// written by one process is read back by another, and only the reading
// process's own discovery can say what a UUID's current index and minor
// number are. Resolving a UUID against today's devices is the isolator's
// job, once it has this back.
struct AllocationCheckpoint
{
  std::string resource;
  double claim;
  std::vector<std::string> deviceUuids;
  bool whole;
};


// Where the checkpoint for one container lives, given its own directory.
// The same path both times: `ContainerConfig::directory()` at prepare(),
// `ContainerState::directory()` at recover() -- both name the one sandbox.
std::string allocationCheckpointPath(const std::string& containerDirectory);


// Writes the allocation prepare() decided. Called before the containerizer
// can grant anything, so a container never holds a device this checkpoint
// does not also describe.
Try<Nothing> writeAllocationCheckpoint(
    const std::string& containerDirectory,
    const AllocationCheckpoint& allocation);


// Reads the allocation back, if one was checkpointed.
//
// `None` means the container never held a GPU claim -- declined at prepare,
// or a non-GPU container on a GPU node -- which is not a state to report as
// an error. An `Error` means a checkpoint exists but is not the shape this
// wrote, which is the checkpoint disagreeing with the code that wrote it,
// not a state to tolerate: recover() cannot rebuild a ledger entry it cannot
// read, and guessing one would grant a device on no evidence.
Try<Option<AllocationCheckpoint>> readAllocationCheckpoint(
    const std::string& containerDirectory);


// Removes the checkpoint. Called from cleanup(), where a missing file is
// already ordinary: a container this module declined at prepare never had
// one.
Try<Nothing> removeAllocationCheckpoint(const std::string& containerDirectory);

} // namespace gpu {
} // namespace modules {
} // namespace medios {

#endif // __MODULES_GPU_CHECKPOINT_HPP__
