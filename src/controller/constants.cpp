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

#include "controller/constants.hpp"

namespace medios {
namespace internal {
namespace controller {

std::vector<ControllerInfo::Capability> CONTROLLER_CAPABILITIES()
{
  ControllerInfo::Capability::Type types[] = {
    ControllerInfo::Capability::NODE_UPDATE,
    ControllerInfo::Capability::NODE_DRAINING,
    ControllerInfo::Capability::QUOTA_V2,
  };

  std::vector<ControllerInfo::Capability> result;
  foreach (ControllerInfo::Capability::Type type, types) {
    ControllerInfo::Capability capability;
    capability.set_type(type);
    result.push_back(capability);
  }

  return result;
}

} // namespace controller {
} // namespace internal {
} // namespace medios {
