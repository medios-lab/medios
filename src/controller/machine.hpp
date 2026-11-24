// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#ifndef __MEDIOS_CONTROLLER_MACHINE_HPP__
#define __MEDIOS_CONTROLLER_MACHINE_HPP__

#include <medios/medios.hpp>

#include <stout/hashset.hpp>

namespace medios {
namespace internal {
namespace controller {

// A C++ Wrapper object for MachineInfo that also stores some extra information.
struct Machine
{
  // A default constructor to allow use of the `[]` operator.
  Machine() {}

  Machine(const MachineInfo& _info) : info(_info) {}

  // The state of the machine represented as a protobuf.
  MachineInfo info;

  // The set of nodes currently registered from this machine.
  hashset<NodeID> nodes;
};

} // namespace controller {
} // namespace internal {
} // namespace medios {

#endif // __MEDIOS_CONTROLLER_MACHINE_HPP__
