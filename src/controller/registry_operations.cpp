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

#include <stout/check.hpp>

#include "controller/registry_operations.hpp"

#include "common/protobuf_utils.hpp"
#include "common/resources_utils.hpp"

namespace medios {
namespace internal {
namespace controller {

AdmitNode::AdmitNode(const NodeInfo& _info) : info(_info)
{
  CHECK(info.has_id()) << "NodeInfo is missing the 'id' field";
}


Try<bool> AdmitNode::perform(Registry* registry, hashset<NodeID>* nodeIDs)
{
  // Check if this node is currently admitted. This should only
  // happen if there is a nodeID collision, but that is extremely
  // unlikely in practice: nodeIDs are prefixed with the controller ID,
  // which is a randomly generated UUID.
  if (nodeIDs->contains(info.id())) {
    return Error("Node already admitted");
  }

  // Downgrade the resources such that an older controller can recover from
  // the checkpointed registry state.
  CHECK_SOME(downgradeResources(&info));

  Registry::Node* node = registry->mutable_nodes()->add_nodes();
  node->mutable_info()->CopyFrom(info);
  nodeIDs->insert(info.id());
  return true; // Mutation.
}


UpdateNode::UpdateNode(const NodeInfo& _info) : info(_info)
{
  CHECK(info.has_id()) << "NodeInfo is missing the 'id' field";
}


Try<bool> UpdateNode::perform(Registry* registry, hashset<NodeID>* nodeIDs)
{
  if (!nodeIDs->contains(info.id())) {
    return Error("Node not yet admitted.");
  }

  for (int i = 0; i < registry->nodes().nodes().size(); i++) {
    Registry::Node* node = registry->mutable_nodes()->mutable_nodes(i);

    if (node->info().id() == info.id()) {
      // The NodeInfo in the registry is stored in the
      // `PRE_RESERVATION_REFINEMENT` format, but the equality operator
      // asserts that resources are in `POST_RESERVATION_REFINEMENT` format,
      // so we have to upgrade before we can do the comparison.
      NodeInfo previousInfo(node->info());
      upgradeResources(&previousInfo);

      if (info == previousInfo) {
        return false; // No mutation.
      }

      // Downgrade the resources such that an older controller can recover from
      // the checkpointed registry state.
      CHECK_SOME(downgradeResources(&info));

      node->mutable_info()->CopyFrom(info);
      return true; // Mutation.
    }
  }

  // Shouldn't happen
  return Error("Failed to find node " + stringify(info.id()));
}


// Move a node from the list of admitted nodes to the list of
// unreachable nodes.
MarkNodeUnreachable::MarkNodeUnreachable(
    const NodeInfo& _info,
    const TimeInfo& _unreachableTime)
  : info(_info)
  , unreachableTime(_unreachableTime)
{
  CHECK(info.has_id()) << "NodeInfo is missing the 'id' field";
}


Try<bool> MarkNodeUnreachable::perform(
    Registry* registry,
    hashset<NodeID>* nodeIDs)
{
  // As currently implemented, this should not be possible: the
  // controller will only mark nodes unreachable that are currently
  // admitted.
  if (!nodeIDs->contains(info.id())) {
    return Error("Node not yet admitted");
  }

  for (int i = 0; i < registry->nodes().nodes().size(); i++) {
    const Registry::Node& node = registry->nodes().nodes(i);

    if (node.info().id() == info.id()) {
      Registry::UnreachableNode* unreachable =
        registry->mutable_unreachable()->add_nodes();

      unreachable->mutable_id()->CopyFrom(info.id());
      unreachable->mutable_timestamp()->CopyFrom(unreachableTime);

      // Copy the draining and deactivation states.
      if (node.has_drain_info()) {
        unreachable->mutable_drain_info()->CopyFrom(node.drain_info());
      }

      unreachable->set_deactivated(node.deactivated());

      registry->mutable_nodes()->mutable_nodes()->DeleteSubrange(i, 1);
      nodeIDs->erase(info.id());

      return true; // Mutation.
    }
  }

  // Should not happen.
  return Error("Failed to find node " + stringify(info.id()));
}


// Add a node back to the list of admitted nodes. The node will
// typically be in the "unreachable" list; if so, it is removed from
// that list. The node might also be in the "admitted" list already.
// Finally, the node might be in neither the "unreachable" or
// "admitted" lists, if its metadata has been garbage collected from
// the registry.
MarkNodeReachable::MarkNodeReachable(const NodeInfo& _info)
  : info(_info)
{
  CHECK(info.has_id()) << "NodeInfo is missing the 'id' field";
}


Try<bool> MarkNodeReachable::perform(
    Registry* registry,
    hashset<NodeID>* nodeIDs)
{
  // Always remove any matching entry from the unreachable list first (#591),
  // even if the node already appears in `nodeIDs`.
  bool removedFromUnreachable = false;
  Option<DrainInfo> unreachableDrainInfo;
  bool unreachableDeactivated = false;

  for (int i = 0; i < registry->unreachable().nodes().size(); i++) {
    const Registry::UnreachableNode& node =
      registry->unreachable().nodes(i);

    if (node.id() == info.id()) {
      if (node.has_drain_info()) {
        unreachableDrainInfo = node.drain_info();
      }
      unreachableDeactivated = node.deactivated();

      registry->mutable_unreachable()->mutable_nodes()->DeleteSubrange(i, 1);
      removedFromUnreachable = true;
      break;
    }
  }

  // A node might try to reregister that appears in the list of
  // admitted nodes. This can occur when the controller fails over:
  // nodes will usually attempt to reregister with the new controller
  // before they are marked unreachable. In this situation, the
  // registry is already in the correct state, so no changes are
  // needed unless we cleaned up a stale unreachable entry above.
  if (nodeIDs->contains(info.id())) {
    return removedFromUnreachable;
  }

  Registry::Node reachable;
  reachable.mutable_info()->CopyFrom(info);

  if (removedFromUnreachable) {
    if (unreachableDrainInfo.isSome()) {
      reachable.mutable_drain_info()->CopyFrom(unreachableDrainInfo.get());
    }
    reachable.set_deactivated(unreachableDeactivated);
  } else {
    LOG(WARNING) << "Allowing UNKNOWN node to reregister: " << info;
  }

  // Downgrade the resources such that an older controller can recover from
  // the checkpointed registry state.
  CHECK_SOME(downgradeResources(&info));

  // Add the node to the admitted list, even if we didn't find it
  // in the unreachable list. This accounts for when the node was
  // unreachable for a long time, was GC'd from the unreachable
  // list, but then eventually reregistered.
  registry->mutable_nodes()->add_nodes()->CopyFrom(reachable);
  nodeIDs->insert(info.id());

  return true; // Mutation.
}


Prune::Prune(
    const hashset<NodeID>& _toRemoveUnreachable,
    const hashset<NodeID>& _toRemoveGone)
  : toRemoveUnreachable(_toRemoveUnreachable)
  , toRemoveGone(_toRemoveGone)
{}


Try<bool> Prune::perform(Registry* registry, hashset<NodeID>* /*nodeIDs*/)
{
  // Attempt to remove the NodeIDs in the `toRemoveXXX` from the
  // unreachable/gone list. Some NodeIDs in `toRemoveXXX` might not appear
  // in the registry; this is possible if there was a concurrent
  // registry operation.
  //
  // TODO(neilc): This has quadratic worst-case behavior, because
  // `DeleteSubrange` for a `repeated` object takes linear time.
  bool mutate = false;

  {
    int i = 0;
    while (i < registry->unreachable().nodes().size()) {
      const Registry::UnreachableNode& node =
        registry->unreachable().nodes(i);

      if (toRemoveUnreachable.contains(node.id())) {
        Registry::UnreachableNodes* unreachable =
          registry->mutable_unreachable();

        unreachable->mutable_nodes()->DeleteSubrange(i, 1);
        mutate = true;
        continue;
      }

      i++;
    }
  }

  {
    int i = 0;
    while (i < registry->gone().nodes().size()) {
      const Registry::GoneNode& node = registry->gone().nodes(i);

      if (toRemoveGone.contains(node.id())) {
        Registry::GoneNodes* gone = registry->mutable_gone();

        gone->mutable_nodes()->DeleteSubrange(i, 1);
        mutate = true;
        continue;
      }

      i++;
    }
  }

  return mutate;
}


RemoveNode::RemoveNode(const NodeInfo& _info)
  : info(_info)
{
  CHECK(info.has_id()) << "NodeInfo is missing the 'id' field";
}


Try<bool> RemoveNode::perform(
    Registry* registry,
    hashset<NodeID>* nodeIDs)
{
  for (int i = 0; i < registry->nodes().nodes().size(); i++) {
    const Registry::Node& node = registry->nodes().nodes(i);
    if (node.info().id() == info.id()) {
      registry->mutable_nodes()->mutable_nodes()->DeleteSubrange(i, 1);
      nodeIDs->erase(info.id());
      return true; // Mutation.
    }
  }

  // Should not happen: the controller will only try to remove nodes
  // that are currently admitted.
  return Error("Node not yet admitted");
}


MarkNodeGone::MarkNodeGone(
    const NodeID& _id,
    const TimeInfo& _goneTime)
  : id(_id)
  , goneTime(_goneTime)
{}


Try<bool> MarkNodeGone::perform(Registry* registry, hashset<NodeID>* nodeIDs)
{
  // Check whether the node is already in the gone list. As currently
  // implemented, this should not be possible: the controller will not
  // try to transition an already gone node.
  for (int i = 0; i < registry->gone().nodes().size(); i++) {
    const Registry::GoneNode& node = registry->gone().nodes(i);

    if (node.id() == id) {
      return Error("Node " + stringify(id) + " already marked as gone");
    }
  }

  // Check whether the node is in the admitted/unreachable list.
  bool found = false;
  if (nodeIDs->contains(id)) {
    found = true;
    for (int i = 0; i < registry->nodes().nodes().size(); i++) {
      const Registry::Node& node = registry->nodes().nodes(i);

      if (node.info().id() == id) {
        registry->mutable_nodes()->mutable_nodes()->DeleteSubrange(i, 1);
        nodeIDs->erase(id);
        break;
      }
    }
  }

  if (!found) {
    for (int i = 0; i < registry->unreachable().nodes().size(); i++) {
      const Registry::UnreachableNode& node =
        registry->unreachable().nodes(i);

      if (node.id() == id) {
        registry->mutable_unreachable()->mutable_nodes()->DeleteSubrange(
            i, 1);

        found = true;
        break;
      }
    }
  }

  if (found) {
    Registry::GoneNode* gone = registry->mutable_gone()->add_nodes();

    gone->mutable_id()->CopyFrom(id);
    gone->mutable_timestamp()->CopyFrom(goneTime);

    return true; // Mutation;
  }

  // Should not happen.
  return Error("Failed to find node " + stringify(id));
}


DrainNode::DrainNode(
    const NodeID& _nodeId,
    const Option<DurationInfo>& _maxGracePeriod,
    const bool _markGone)
  : nodeId(_nodeId),
    maxGracePeriod(_maxGracePeriod),
    markGone(_markGone)
{}


Try<bool> DrainNode::perform(Registry* registry, hashset<NodeID>* nodeIDs)
{
  // Check whether the node is in the admitted list.
  bool found = false;
  if (nodeIDs->contains(nodeId)) {
    found = true;

    for (int i = 0; i < registry->nodes().nodes().size(); i++) {
      if (registry->nodes().nodes(i).info().id() == nodeId) {
        Registry::Node* node = registry->mutable_nodes()->mutable_nodes(i);

        node->mutable_drain_info()->set_state(DRAINING);

        // Copy the DrainConfig and ensure the node is deactivated.
        if (maxGracePeriod.isSome()) {
          node->mutable_drain_info()->mutable_config()
            ->mutable_max_grace_period()->CopyFrom(maxGracePeriod.get());
        }

        node->mutable_drain_info()->mutable_config()->set_mark_gone(markGone);
        node->set_deactivated(true);
        break;
      }
    }
  }

  // If not found above, check the unreachable list.
  if (!found) {
    for (int i = 0; i < registry->unreachable().nodes().size(); i++) {
      if (registry->unreachable().nodes(i).id() == nodeId) {
        Registry::UnreachableNode* node =
          registry->mutable_unreachable()->mutable_nodes(i);

        node->mutable_drain_info()->set_state(DRAINING);

        // Copy the DrainConfig and ensure the node is deactivated.
        if (maxGracePeriod.isSome()) {
          node->mutable_drain_info()->mutable_config()
            ->mutable_max_grace_period()->CopyFrom(maxGracePeriod.get());
        }

        node->mutable_drain_info()->mutable_config()->set_mark_gone(markGone);
        node->set_deactivated(true);
        found = true;
        break;
      }
    }
  }

  // Make sure the NODE_DRAINING minimum capability is present or added.
  if (found) {
    protobuf::controller::addMinimumCapability(
        registry->mutable_minimum_capabilities(),
        ControllerInfo::Capability::NODE_DRAINING);
  }

  return found; // Mutation if found.
}


MarkNodeDrained::MarkNodeDrained(
    const NodeID& _nodeId)
  : nodeId(_nodeId)
{}


Try<bool> MarkNodeDrained::perform(
    Registry* registry, hashset<NodeID>* nodeIDs)
{
  // Check whether the node is in the admitted list.
  bool found = false;
  if (nodeIDs->contains(nodeId)) {
    found = true;

    for (int i = 0; i < registry->nodes().nodes().size(); i++) {
      if (registry->nodes().nodes(i).info().id() == nodeId) {
        Registry::Node* node = registry->mutable_nodes()->mutable_nodes(i);

        // NOTE: This should be validated/prevented on the controller side.
        if (!node->has_drain_info() ||
            node->drain_info().state() != DRAINING) {
          return Error("Node " + stringify(nodeId) + " is not DRAINING");
        }

        node->mutable_drain_info()->set_state(DRAINED);
        break;
      }
    }
  }

  // If not found above, check the unreachable list.
  if (!found) {
    for (int i = 0; i < registry->unreachable().nodes().size(); i++) {
      if (registry->unreachable().nodes(i).id() == nodeId) {
        Registry::UnreachableNode* node =
          registry->mutable_unreachable()->mutable_nodes(i);

        // NOTE: This should be validated/prevented on the controller side.
        if (!node->has_drain_info() ||
            node->drain_info().state() != DRAINING) {
          return Error("Node " + stringify(nodeId) + " is not DRAINING");
        }

        node->mutable_drain_info()->set_state(DRAINED);
        found = true;
        break;
      }
    }
  }

  return found; // Mutation if found.
}


DeactivateNode::DeactivateNode(
    const NodeID& _nodeId)
  : nodeId(_nodeId)
{}


Try<bool> DeactivateNode::perform(
    Registry* registry, hashset<NodeID>* nodeIDs)
{
  // Check whether the node is in the admitted list.
  bool found = false;
  if (nodeIDs->contains(nodeId)) {
    found = true;

    for (int i = 0; i < registry->nodes().nodes().size(); i++) {
      if (registry->nodes().nodes(i).info().id() == nodeId) {
        Registry::Node* node = registry->mutable_nodes()->mutable_nodes(i);

        // Set the deactivated boolean.
        node->set_deactivated(true);
        break;
      }
    }
  }

  // If not found above, check the unreachable list.
  if (!found) {
    for (int i = 0; i < registry->unreachable().nodes().size(); i++) {
      if (registry->unreachable().nodes(i).id() == nodeId) {
        Registry::UnreachableNode* node =
          registry->mutable_unreachable()->mutable_nodes(i);

        // Set the deactivated boolean.
        node->set_deactivated(true);
        found = true;
        break;
      }
    }
  }

  // Make sure the NODE_DRAINING minimum capability is present or added.
  if (found) {
    protobuf::controller::addMinimumCapability(
        registry->mutable_minimum_capabilities(),
        ControllerInfo::Capability::NODE_DRAINING);
  }

  return found; // Mutation if found.
}


ReactivateNode::ReactivateNode(
    const NodeID& _nodeId)
  : nodeId(_nodeId)
{}


Try<bool> ReactivateNode::perform(
    Registry* registry, hashset<NodeID>* nodeIDs)
{
  // Check whether the node is in the admitted list.
  bool found = false;
  bool moreThanOneDeactivated = false;

  for (int i = 0; i < registry->nodes().nodes().size(); i++) {
    if (registry->nodes().nodes(i).info().id() == nodeId) {
      Registry::Node* node = registry->mutable_nodes()->mutable_nodes(i);

      // Clear the draining and deactivated states.
      node->clear_drain_info();
      node->clear_deactivated();
      found = true;

      if (moreThanOneDeactivated) {
        break;
      }

      continue;
    }

    if (registry->nodes().nodes(i).deactivated()) {
      moreThanOneDeactivated = true;

      if (found) {
        break;
      }
    }
  }

  // Check the unreachable list too.
  if (!found || !moreThanOneDeactivated) {
    for (int i = 0; i < registry->unreachable().nodes().size(); i++) {
      if (registry->unreachable().nodes(i).id() == nodeId) {
        Registry::UnreachableNode* node =
          registry->mutable_unreachable()->mutable_nodes(i);

        // Clear the draining and deactivated states.
        node->clear_drain_info();
        node->clear_deactivated();
        found = true;

        if (moreThanOneDeactivated) {
          break;
        }

        continue;
      }

      if (registry->unreachable().nodes(i).deactivated()) {
        moreThanOneDeactivated = true;

        if (found) {
          break;
        }
      }
    }
  }

  // If this is the last deactivated node,
  // remove the NODE_DRAINING minimum capability.
  if (found && !moreThanOneDeactivated) {
    protobuf::controller::removeMinimumCapability(
        registry->mutable_minimum_capabilities(),
        ControllerInfo::Capability::NODE_DRAINING);
  }

  return found; // Mutation if found.
}

} // namespace controller {
} // namespace internal {
} // namespace medios {
