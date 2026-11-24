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

#include "node/constants.hpp"

#include <vector>

using std::vector;

namespace medios {
namespace internal {
namespace node {

Duration DEFAULT_CONTROLLER_PING_TIMEOUT()
{
  return controller::DEFAULT_NODE_PING_TIMEOUT *
    controller::DEFAULT_MAX_NODE_PING_TIMEOUTS;
}


vector<NodeInfo::Capability> NODE_CAPABILITIES()
{
  NodeInfo::Capability::Type types[] = {
    NodeInfo::Capability::HIERARCHICAL_ROLE,
    NodeInfo::Capability::MULTI_ROLE,
    NodeInfo::Capability::RESERVATION_REFINEMENT,
    NodeInfo::Capability::RESOURCE_PROVIDER,
    NodeInfo::Capability::RESIZE_VOLUME,
    NodeInfo::Capability::NODE_OPERATION_FEEDBACK,
    NodeInfo::Capability::NODE_DRAINING,
    NodeInfo::Capability::TASK_RESOURCE_LIMITS,
  };

  vector<NodeInfo::Capability> result;
  foreach (NodeInfo::Capability::Type type, types) {
    NodeInfo::Capability capability;
    capability.set_type(type);
    result.push_back(capability);
  }

  return result;
}

} // namespace node {
} // namespace internal {
} // namespace medios {
