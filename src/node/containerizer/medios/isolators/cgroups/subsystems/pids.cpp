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

#include <process/id.hpp>

#include "node/containerizer/medios/isolators/cgroups/subsystems/pids.hpp"

using process::Owned;

using std::string;

namespace medios {
namespace internal {
namespace node {

Try<Owned<SubsystemProcess>> PidsSubsystemProcess::create(
    const Flags& flags,
    const string& hierarchy)
{
  return Owned<SubsystemProcess>(new PidsSubsystemProcess(flags, hierarchy));
}


PidsSubsystemProcess::PidsSubsystemProcess(
    const Flags& _flags,
    const string& _hierarchy)
  : ProcessBase(process::ID::generate("cgroups-pids-subsystem")),
    SubsystemProcess(_flags, _hierarchy) {}

} // namespace node {
} // namespace internal {
} // namespace medios {
