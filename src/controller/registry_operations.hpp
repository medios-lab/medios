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

#ifndef __CONTROLLER_REGISTRY_OPERATIONS_HPP__
#define __CONTROLLER_REGISTRY_OPERATIONS_HPP__

#include <medios/medios.hpp>
#include <medios/type_utils.hpp>

#include "controller/registrar.hpp"


namespace medios {
namespace internal {
namespace controller {

// Add a new node to the list of admitted nodes.
class AdmitNode : public RegistryOperation
{
public:
  explicit AdmitNode(const NodeInfo& _info);

protected:
  Try<bool> perform(Registry* registry, hashset<NodeID>* nodeIDs) override;

private:
  NodeInfo info;
};


// Update the NodeInfo of an existing admitted node.
class UpdateNode : public RegistryOperation
{
public:
  explicit UpdateNode(const NodeInfo& _info);

protected:
  Try<bool> perform(Registry* registry, hashset<NodeID>* nodeIDs) override;

private:
  NodeInfo info;
};


// Move a node from the list of admitted nodes to the list of
// unreachable nodes.
class MarkNodeUnreachable : public RegistryOperation
{
public:
  MarkNodeUnreachable(
      const NodeInfo& _info,
      const TimeInfo& _unreachableTime);

protected:
  Try<bool> perform(Registry* registry, hashset<NodeID>* nodeIDs) override;

private:
  const NodeInfo info;
  const TimeInfo unreachableTime;
};


// Add a node back to the list of admitted nodes. The node will
// typically be in the "unreachable" list; if so, it is removed from
// that list. The node might also be in the "admitted" list already.
// Finally, the node might be in neither the "unreachable" or
// "admitted" lists, if its metadata has been garbage collected from
// the registry.
class MarkNodeReachable : public RegistryOperation
{
public:
  explicit MarkNodeReachable(const NodeInfo& _info);

protected:
  Try<bool> perform(Registry* registry, hashset<NodeID>* nodeIDs) override;

private:
  NodeInfo info;
};


class Prune : public RegistryOperation
{
public:
  explicit Prune(
      const hashset<NodeID>& _toRemoveUnreachable,
      const hashset<NodeID>& _toRemoveGone);

protected:
  Try<bool> perform(Registry* registry, hashset<NodeID>* /*nodeIDs*/)
    override;

private:
  const hashset<NodeID> toRemoveUnreachable;
  const hashset<NodeID> toRemoveGone;
};


class RemoveNode : public RegistryOperation
{
public:
  explicit RemoveNode(const NodeInfo& _info);

protected:
  Try<bool> perform(Registry* registry, hashset<NodeID>* nodeIDs) override;

private:
  const NodeInfo info;
};


// Move a node from the list of admitted/unreachable nodes
// to the list of gone nodes.
class MarkNodeGone : public RegistryOperation
{
public:
  MarkNodeGone(const NodeID& _id, const TimeInfo& _goneTime);

protected:
  Try<bool> perform(Registry* registry, hashset<NodeID>* nodeIDs) override;

private:
  const NodeID id;
  const TimeInfo goneTime;
};


// Marks an existing node for draining.
// Also adds a minimum capability to the controller for NODE_DRAINING.
class DrainNode : public RegistryOperation
{
public:
  DrainNode(
      const NodeID& _nodeId,
      const Option<DurationInfo>& _maxGracePeriod,
      const bool _markGone);

protected:
  Try<bool> perform(Registry* registry, hashset<NodeID>* nodeIDs) override;

private:
  const NodeID nodeId;
  const Option<DurationInfo> maxGracePeriod;
  const bool markGone;
};


// Transitions a DRAINING node into the DRAINED state.
class MarkNodeDrained : public RegistryOperation
{
public:
  MarkNodeDrained(const NodeID& _nodeId);

protected:
  Try<bool> perform(Registry* registry, hashset<NodeID>* nodeIDs) override;

private:
  const NodeID nodeId;
};


// Marks an existing node as deactivated.
// Also adds a minimum capability to the controller for NODE_DRAINING.
class DeactivateNode : public RegistryOperation
{
public:
  DeactivateNode(const NodeID& _nodeId);

protected:
  Try<bool> perform(Registry* registry, hashset<NodeID>* nodeIDs) override;

private:
  const NodeID nodeId;
};


// Clears draining or deactivation from an existing node.
// If there are no remaining draining or deactivated nodes,
// this also clears the minimum capability for NODE_DRAINING.
class ReactivateNode : public RegistryOperation
{
public:
  ReactivateNode(const NodeID& _nodeId);

protected:
  Try<bool> perform(Registry* registry, hashset<NodeID>* nodeIDs) override;

private:
  const NodeID nodeId;
};

} // namespace controller {
} // namespace internal {
} // namespace medios {

#endif // __CONTROLLER_REGISTRY_OPERATIONS_HPP__
