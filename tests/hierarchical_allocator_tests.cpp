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

#include <atomic>
#include <iostream>
#include <set>
#include <string>
#include <utility>
#include <vector>

#include <gmock/gmock.h>

#include <medios/allocator/allocator.hpp>

#include <process/clock.hpp>
#include <process/future.hpp>
#include <process/gtest.hpp>
#include <process/queue.hpp>

#include <stout/duration.hpp>
#include <stout/foreach.hpp>
#include <stout/gtest.hpp>
#include <stout/hashmap.hpp>
#include <stout/hashset.hpp>
#include <stout/json.hpp>
#include <stout/os.hpp>
#include <stout/stopwatch.hpp>
#include <stout/utils.hpp>

#include "controller/constants.hpp"
#include "controller/flags.hpp"

#include "controller/allocator/medios/hierarchical.hpp"

#include "node/constants.hpp"

#include "tests/allocator.hpp"
#include "tests/medios.hpp"
#include "tests/resources_utils.hpp"
#include "tests/utils.hpp"

using medios::internal::controller::MIN_CPUS;
using medios::internal::controller::MIN_MEM;

using medios::internal::controller::allocator::HierarchicalDRFAllocator;

using medios::internal::protobuf::createLabel;

using medios::internal::node::NODE_CAPABILITIES;

using medios::allocator::Allocator;

using process::Clock;
using process::Future;

using std::atomic;
using std::cout;
using std::endl;
using std::map;
using std::ostream;
using std::set;
using std::string;
using std::vector;

using testing::WithParamInterface;

namespace medios {
namespace internal {
namespace tests {

struct Allocation
{
  Allocation() = default;

  Allocation(
      const FrameworkID& frameworkId_,
      const hashmap<string, hashmap<NodeID, Resources>>& resources_)
    : frameworkId(frameworkId_),
      resources(resources_)
  {
    // Ensure the resources have the allocation info set.
    foreachkey (const string& role, resources) {
      foreachvalue (Resources& r, resources.at(role)) {
        r.allocate(role);
      }
    }
  }

  FrameworkID frameworkId;
  hashmap<string, hashmap<NodeID, Resources>> resources;
};


bool operator==(const Allocation& left, const Allocation& right)
{
  return left.frameworkId == right.frameworkId &&
      left.resources == right.resources;
}


ostream& operator<<(ostream& stream, const Allocation& allocation)
{
  return stream
    << "FrameworkID: " << allocation.frameworkId
    << " Resource Allocation: " << stringify(allocation.resources);
}


struct Deallocation
{
  FrameworkID frameworkId;
  hashmap<NodeID, UnavailableResources> resources;
};


class HierarchicalAllocatorTestBase : public ::testing::Test
{
protected:
  HierarchicalAllocatorTestBase()
    : allocator(createAllocator<HierarchicalDRFAllocator>()),
      nextNodeId(1),
      nextFrameworkId(1) {}

  ~HierarchicalAllocatorTestBase() override
  {
    delete allocator;
  }

  void initialize(
      const controller::Flags& _flags = controller::Flags(),
      Option<lambda::function<
          void(const FrameworkID&,
               const hashmap<string, hashmap<NodeID, Resources>>&)>>
                 offerCallback = None(),
      Option<lambda::function<
          void(const FrameworkID&,
               const hashmap<NodeID, UnavailableResources>&)>>
                 inverseOfferCallback = None())
  {
    flags = _flags;

    if (offerCallback.isNone()) {
      offerCallback =
        [this](const FrameworkID& frameworkId,
               const hashmap<string, hashmap<NodeID, Resources>>& resources) {
          Allocation allocation;
          allocation.frameworkId = frameworkId;
          allocation.resources = resources;

          allocations.put(allocation);
        };
    }

    if (inverseOfferCallback.isNone()) {
      inverseOfferCallback =
        [this](const FrameworkID& frameworkId,
               const hashmap<NodeID, UnavailableResources>& resources) {
          Deallocation deallocation;
          deallocation.frameworkId = frameworkId;
          deallocation.resources = resources;

          deallocations.put(deallocation);
        };
    }

    vector<ResourceQuantities> minAllocatableResources;
    minAllocatableResources.push_back(CHECK_NOTERROR(
        ResourceQuantities::fromString("cpus:" + stringify(MIN_CPUS))));
    minAllocatableResources.push_back(
        CHECK_NOTERROR(ResourceQuantities::fromString(
            "mem:" + stringify((double)MIN_MEM.bytes() / Bytes::MEGABYTES))));

    Options options;
    options.allocationInterval = flags.allocation_interval;
    options.fairnessExcludeResourceNames =
      flags.fair_sharing_excluded_resource_names;
    options.filterSharedResources = flags.filter_shared_resources;
    options.minAllocatableResources = minAllocatableResources;

    allocator->initialize(
        options,
        offerCallback.get(),
        inverseOfferCallback.get());
  }

  NodeInfo createNodeInfo(const Resources& resources)
  {
    NodeID nodeId;
    nodeId.set_value("node" + stringify(nextNodeId++));

    NodeInfo node;
    *(node.mutable_resources()) = resources;
    *(node.mutable_id()) = nodeId;
    node.set_hostname(nodeId.value());

    return node;
  }

  NodeInfo createNodeInfo(const string& resources)
  {
    const Resources nodeResources = Resources::parse(resources).get();
    return createNodeInfo(nodeResources);
  }

  FrameworkInfo createFrameworkInfo(
      const set<string>& roles,
      const vector<FrameworkInfo::Capability::Type>& capabilities = {})
  {
    FrameworkInfo frameworkInfo;
    frameworkInfo.set_user("user");
    frameworkInfo.set_name("framework" + stringify(nextFrameworkId++));
    frameworkInfo.mutable_id()->set_value(frameworkInfo.name());

    bool multiRole = false;

    foreach (const FrameworkInfo::Capability::Type& capability, capabilities) {
      frameworkInfo.add_capabilities()->set_type(capability);
      if (capability == FrameworkInfo::Capability::MULTI_ROLE) {
        multiRole = true;
      }
    }

    // Inject the MULTI_ROLE capability by default. The allocator ONLY looks
    // at whether the framework is MULTI_ROLE capable to filter out offers
    // from non-MULTI_ROLE nodes (see MESOS-6940). Other than that, the logic
    // between a non-MULTI_ROLE scheduler and a single role MULTI_ROLE scheduler
    // is the same as far as the allocator is concerned.
    if (!multiRole) {
      frameworkInfo.add_capabilities()->set_type(
          FrameworkInfo::Capability::MULTI_ROLE);
    }

    foreach(const string& role, roles) {
      frameworkInfo.add_roles(role);
    }

    return frameworkInfo;
  }

  Resources createRevocableResources(
      const string& name,
      const string& value,
      const string& role = "*")
  {
    Resource resource = Resources::parse(name, value, role).get();
    resource.mutable_revocable();
    return resource;
  }

protected:
  controller::Flags flags;

  Allocator* allocator;

  process::Queue<Allocation> allocations;
  process::Queue<Deallocation> deallocations;

private:
  int nextNodeId;
  int nextFrameworkId;
};


class HierarchicalAllocatorTest : public HierarchicalAllocatorTestBase {};


// TODO(bmahler): These tests were transformed directly from
// integration tests into unit tests. However, these tests
// should be simplified even further to each test a single
// expected behavior, at which point we can have more tests
// that are each very small.


// Checks that the DRF allocator implements the DRF algorithm
// correctly. The test accomplishes this by adding frameworks and
// nodes one at a time to the allocator, making sure that each time
// a new node is added all of its resources are offered to whichever
// framework currently has the smallest share. Checking for proper DRF
// logic when resources are returned, frameworks exit, etc. is handled
// by SorterTest.DRFSorter.
TEST_F(HierarchicalAllocatorTest, UnreservedDRF)
{
  // Pausing the clock is not necessary, but ensures that the test
  // doesn't rely on the batch allocation in the allocator, which
  // would slow down the test.
  Clock::pause();

  initialize();

  // Total cluster resources will become cpus=2, mem=1024.
  NodeInfo node1 = createNodeInfo("cpus:2;mem:1024;disk:0");
  allocator->addNode(
      node1.id(),
      node1,
      NODE_CAPABILITIES(),
      None(),
      node1.resources(),
      {});

  // framework1 will be offered all of node1's resources since it is
  // the only framework running so far.
  FrameworkInfo framework1 = createFrameworkInfo({"role1"});
  allocator->addFramework(framework1.id(), framework1, {}, true, {});

  Allocation expected = Allocation(
      framework1.id(),
      {{"role1", {{node1.id(), node1.resources()}}}});

  AWAIT_EXPECT_EQ(expected, allocations.get());

  // role1 share = 1 (cpus=2, mem=1024)
  //   framework1 share = 1

  FrameworkInfo framework2 = createFrameworkInfo({"role2"});
  allocator->addFramework(framework2.id(), framework2, {}, true, {});

  // Total cluster resources will become cpus=3, mem=1536:
  // role1 share = 0.66 (cpus=2, mem=1024)
  //   framework1 share = 1
  // role2 share = 0
  //   framework2 share = 0
  NodeInfo node2 = createNodeInfo("cpus:1;mem:512;disk:0");
  allocator->addNode(
      node2.id(),
      node2,
      NODE_CAPABILITIES(),
      None(),
      node2.resources(),
      {});

  // framework2 will be offered all of node2's resources since role2
  // has the lowest user share, and framework2 is its only framework.
  expected = Allocation(
      framework2.id(),
      {{"role2", {{node2.id(), node2.resources()}}}});

  AWAIT_EXPECT_EQ(expected, allocations.get());

  // role1 share = 0.67 (cpus=2, mem=1024)
  //   framework1 share = 1
  // role2 share = 0.33 (cpus=1, mem=512)
  //   framework2 share = 1

  // Total cluster resources will become cpus=6, mem=3584:
  // role1 share = 0.33 (cpus=2, mem=1024)
  //   framework1 share = 1
  // role2 share = 0.16 (cpus=1, mem=512)
  //   framework2 share = 1
  NodeInfo node3 = createNodeInfo("cpus:3;mem:2048;disk:0");
  allocator->addNode(
      node3.id(),
      node3,
      NODE_CAPABILITIES(),
      None(),
      node3.resources(),
      {});

  // framework2 will be offered all of node3's resources since role2
  // has the lowest share.
  expected = Allocation(
      framework2.id(),
      {{"role2", {{node3.id(), node3.resources()}}}});

  AWAIT_EXPECT_EQ(expected, allocations.get());

  // role1 share = 0.33 (cpus=2, mem=1024)
  //   framework1 share = 1
  // role2 share = 0.71 (cpus=4, mem=2560)
  //   framework2 share = 1

  FrameworkInfo framework3 = createFrameworkInfo({"role1"});
  allocator->addFramework(framework3.id(), framework3, {}, true, {});

  // Total cluster resources will become cpus=10, mem=7680:
  // role1 share = 0.2 (cpus=2, mem=1024)
  //   framework1 share = 1
  //   framework3 share = 0
  // role2 share = 0.4 (cpus=4, mem=2560)
  //   framework2 share = 1
  NodeInfo node4 = createNodeInfo("cpus:4;mem:4096;disk:0");
  allocator->addNode(
      node4.id(),
      node4,
      NODE_CAPABILITIES(),
      None(),
      node4.resources(),
      {});

  // framework3 will be offered all of node4's resources since role1
  // has the lowest user share, and framework3 has the lowest share of
  // role1's frameworks.
  expected = Allocation(
      framework3.id(),
      {{"role1", {{node4.id(), node4.resources()}}}});

  AWAIT_EXPECT_EQ(expected, allocations.get());

  // role1 share = 0.67 (cpus=6, mem=5120)
  //   framework1 share = 0.33 (cpus=2, mem=1024)
  //   framework3 share = 0.8 (cpus=4, mem=4096)
  // role2 share = 0.4 (cpus=4, mem=2560)
  //   framework2 share = 1

  FrameworkInfo framework4 = createFrameworkInfo({"role1"});
  allocator->addFramework(framework4.id(), framework4, {}, true, {});

  // Total cluster resources will become cpus=11, mem=8192
  // role1 share = 0.63 (cpus=6, mem=5120)
  //   framework1 share = 0.33 (cpus=2, mem=1024)
  //   framework3 share = 0.8 (cpus=4, mem=4096)
  //   framework4 share = 0
  // role2 share = 0.36 (cpus=4, mem=2560)
  //   framework2 share = 1
  NodeInfo node5 = createNodeInfo("cpus:1;mem:512;disk:0");
  allocator->addNode(
      node5.id(),
      node5,
      NODE_CAPABILITIES(),
      None(),
      node5.resources(),
      {});

  // Even though framework4 doesn't have any resources, role2 has a
  // lower share than role1, so framework2 receives node5's resources.
  expected = Allocation(
      framework2.id(),
      {{"role2", {{node5.id(), node5.resources()}}}});

  AWAIT_EXPECT_EQ(expected, allocations.get());
}


// This test ensures that reserved resources do affect the sharing across roles.
TEST_F(HierarchicalAllocatorTest, ReservedDRF)
{
  // Pausing the clock is not necessary, but ensures that the test
  // doesn't rely on the batch allocation in the allocator, which
  // would slow down the test.
  Clock::pause();

  initialize();

  NodeInfo node1 = createNodeInfo(
      "cpus:1;mem:512;disk:0;"
      "cpus(role1):100;mem(role1):1024;disk(role1):0");
  allocator->addNode(
      node1.id(),
      node1,
      NODE_CAPABILITIES(),
      None(),
      node1.resources(),
      {});

  // framework1 will be offered all of the resources.
  FrameworkInfo framework1 = createFrameworkInfo({"role1"});
  allocator->addFramework(framework1.id(), framework1, {}, true, {});

  Allocation expected = Allocation(
      framework1.id(),
      {{"role1", {{node1.id(), node1.resources()}}}});

  AWAIT_EXPECT_EQ(expected, allocations.get());

  FrameworkInfo framework2 = createFrameworkInfo({"role2"});
  allocator->addFramework(framework2.id(), framework2, {}, true, {});

  // framework2 will be allocated the new resources.
  NodeInfo node2 = createNodeInfo("cpus:2;mem:512;disk:0");
  allocator->addNode(
      node2.id(),
      node2,
      NODE_CAPABILITIES(),
      None(),
      node2.resources(),
      {});

  expected = Allocation(
      framework2.id(),
      {{"role2", {{node2.id(), node2.resources()}}}});

  AWAIT_EXPECT_EQ(expected, allocations.get());

  // Since `framework1` has more resources allocated to it than `framework2`,
  // We expect `framework2` to receive this node's resources.
  NodeInfo node3 = createNodeInfo("cpus:2;mem:512;disk:0");
  allocator->addNode(
      node3.id(),
      node3,
      NODE_CAPABILITIES(),
      None(),
      node3.resources(),
      {});

  expected = Allocation(
      framework2.id(),
      {{"role2", {{node3.id(), node3.resources()}}}});

  AWAIT_EXPECT_EQ(expected, allocations.get());

  // Now add another framework in role1. Since the reserved resources
  // should be allocated fairly between frameworks within a role, we
  // expect framework3 to receive the next allocation of role1
  // resources.
  FrameworkInfo framework3 = createFrameworkInfo({"role1"});
  allocator->addFramework(framework3.id(), framework3, {}, true, {});

  NodeInfo node4 = createNodeInfo(
      "cpus(role1):2;mem(role1):1024;disk(role1):0");
  allocator->addNode(
      node4.id(),
      node4,
      NODE_CAPABILITIES(),
      None(),
      node4.resources(),
      {});

  expected = Allocation(
      framework3.id(),
      {{"role1", {{node4.id(), node4.resources()}}}});

  AWAIT_EXPECT_EQ(expected, allocations.get());
}


// Tests that the fairness exclusion list works as expected. The test
// accomplishes this by adding frameworks and nodes one at a time to
// the allocator with exclude resources, making sure that each time a
// new node is added all of its resources are offered to whichever
// framework currently has the smallest share. Checking for proper DRF
// logic when resources are returned, frameworks exit, etc, is handled
// by SorterTest.DRFSorter.
TEST_F(HierarchicalAllocatorTest, DRFWithFairnessExclusion)
{
  // Pausing the clock is not necessary, but ensures that the test
  // doesn't rely on the batch allocation in the allocator, which
  // would slow down the test.
  Clock::pause();

  // Specify that `gpus` should not be fairly shared.
  controller::Flags flags_;
  flags_.fair_sharing_excluded_resource_names = set<string>({"gpus"});

  initialize(flags_);

  // Total cluster resources will become cpus=2, mem=1024, gpus=1.
  NodeInfo node1 = createNodeInfo("cpus:2;mem:1024;disk:0;gpus:1");
  allocator->addNode(
      node1.id(),
      node1,
      NODE_CAPABILITIES(),
      None(),
      node1.resources(),
      {});

  // framework1 will be offered all of node1's resources since it is
  // the only framework running so far.
  FrameworkInfo framework1 = createFrameworkInfo(
      {"role1"}, {FrameworkInfo::Capability::GPU_RESOURCES});

  allocator->addFramework(framework1.id(), framework1, {}, true, {});

  Allocation expected = Allocation(
      framework1.id(),
      {{"role1", {{node1.id(), node1.resources()}}}});

  AWAIT_EXPECT_EQ(expected, allocations.get());

  // role1 share = 1 (cpus=2, mem=1024, (ignored) gpus=1)
  //   framework1 share = 1

  FrameworkInfo framework2 = createFrameworkInfo({"role2"});
  allocator->addFramework(framework2.id(), framework2, {}, true, {});

  // Total cluster resources will become cpus=3, mem=1536, (ignored) gpus=1
  // role1 share = 0.66 (cpus=2, mem=1024, (ignored) gpus=1)
  //   framework1 share = 1
  // role2 share = 0
  //   framework2 share = 0
  NodeInfo node2 = createNodeInfo("cpus:1;mem:512;disk:0");
  allocator->addNode(
      node2.id(),
      node2,
      NODE_CAPABILITIES(),
      None(),
      node2.resources(),
      {});

  // framework2 will be offered all of node2's resources since role2
  // has the lowest user share, and framework2 is its only framework.
  expected = Allocation(
      framework2.id(),
      {{"role2", {{node2.id(), node2.resources()}}}});

  AWAIT_EXPECT_EQ(expected, allocations.get());

  // role1 share = 0.67 (cpus=2, mem=1024, (ignored) gpus=1)
  //   framework1 share = 1
  // role2 share = 0.33 (cpus=1, mem=512)
  //   framework2 share = 1

  // Total cluster resources will become cpus=6, mem=3584, (ignored) gpus=1
  // role1 share = 0.33 (cpus=2, mem=1024, (ignored) gpus=1)
  //   framework1 share = 1
  // role2 share = 0.16 (cpus=1, mem=512)
  //   framework2 share = 1
  NodeInfo node3 = createNodeInfo("cpus:3;mem:2048;disk:0");
  allocator->addNode(
      node3.id(),
      node3,
      NODE_CAPABILITIES(),
      None(),
      node3.resources(),
      {});

  // framework2 will be offered all of node3's resources since role2
  // has the lowest share.
  expected = Allocation(
      framework2.id(),
      {{"role2", {{node3.id(), node3.resources()}}}});

  AWAIT_EXPECT_EQ(expected, allocations.get());

  // role1 share = 0.33 (cpus=2, mem=1024, (ignored)gpus=1)
  //   framework1 share = 1
  // role2 share = 0.71 (cpus=4, mem=2560)
  //   framework2 share = 1

  FrameworkInfo framework3 = createFrameworkInfo({"role1"});
  allocator->addFramework(framework3.id(), framework3, {}, true, {});

  // Total cluster resources will become cpus=10, mem=7680, (ignored) gpus=1
  // role1 share = 0.2 (cpus=2, mem=1024, (ignored) gpus=1)
  //   framework1 share = 1
  //   framework3 share = 0
  // role2 share = 0.4 (cpus=4, mem=2560)
  //   framework2 share = 1
  NodeInfo node4 = createNodeInfo("cpus:4;mem:4096;disk:0");
  allocator->addNode(
      node4.id(),
      node4,
      NODE_CAPABILITIES(),
      None(),
      node4.resources(),
      {});

  // framework3 will be offered all of node4's resources since role1
  // has the lowest user share, and framework3 has the lowest share of
  // role1's frameworks.
  expected = Allocation(
      framework3.id(),
      {{"role1", {{node4.id(), node4.resources()}}}});

  AWAIT_EXPECT_EQ(expected, allocations.get());

  // role1 share = 0.67 (cpus=6, mem=5120, (ignored) gpus=1)
  //   framework1 share = 0.33 (cpus=2, mem=1024, (ignored) gpus=1)
  //   framework3 share = 0.8 (cpus=4, mem=4096)
  // role2 share = 0.4 (cpus=4, mem=2560)
  //   framework2 share = 1

  FrameworkInfo framework4 = createFrameworkInfo({"role1"});
  allocator->addFramework(framework4.id(), framework4, {}, true, {});

  // Total cluster resources will become cpus=11, mem=8192, (ignored) gpus=1
  // role1 share = 0.63 (cpus=6, mem=5120, (ignored) gpus=1)
  //   framework1 share = 0.33 (cpus=2, mem=1024, (ignored) gpus=1)
  //   framework3 share = 0.8 (cpus=4, mem=4096)
  //   framework4 share = 0
  // role2 share = 0.36 (cpus=4, mem=2560)
  //   framework2 share = 1
  NodeInfo node5 = createNodeInfo("cpus:1;mem:512;disk:0");
  allocator->addNode(
      node5.id(),
      node5,
      NODE_CAPABILITIES(),
      None(),
      node5.resources(),
      {});

  // Even though framework4 doesn't have any resources, role2 has a
  // lower share than role1, so framework2 receives node5's resources.
  expected = Allocation(
      framework2.id(),
      {{"role2", {{node5.id(), node5.resources()}}}});

  AWAIT_EXPECT_EQ(expected, allocations.get());
}


// This test checks allocator behavior when offering resources to
// frameworks that register using nested ("hierarchical") roles.
TEST_F(HierarchicalAllocatorTest, NestedRoleDRF)
{
  // Pausing the clock is not necessary, but ensures that the test
  // doesn't rely on the batch allocation in the allocator, which
  // would slow down the test.
  Clock::pause();

  initialize();

  // Total cluster resources will become cpus=2, mem=1024.
  NodeInfo node1 = createNodeInfo("cpus:2;mem:1024;disk:0");
  allocator->addNode(
      node1.id(),
      node1,
      NODE_CAPABILITIES(),
      None(),
      node1.resources(),
      {});

  // framework1 will be offered all of node1's resources since it is
  // the only framework running so far.
  FrameworkInfo framework1 = createFrameworkInfo({"a/b"});
  allocator->addFramework(framework1.id(), framework1, {}, true, {});

  {
    Allocation expected = Allocation(
        framework1.id(),
        {{"a/b", {{node1.id(), node1.resources()}}}});

    AWAIT_EXPECT_EQ(expected, allocations.get());
  }

  // a share = 1 (cpus=2, mem=1024)
  //   a/b share = 1 (cpus=2, mem=1024)
  //     framework1 share = 1

  // Add two new frameworks in roles "a/c" and "d/e".
  FrameworkInfo framework2 = createFrameworkInfo({"a/c"});
  allocator->addFramework(framework2.id(), framework2, {}, true, {});

  FrameworkInfo framework3 = createFrameworkInfo({"d/e"});
  allocator->addFramework(framework3.id(), framework3, {}, true, {});

  // Add a new node. The new node's resources should be offered to
  // the framework in role "d/e" rather than the framework in role
  // "a/c", since the role subtree under "a" has more resources than
  // the "d" subtree.
  //
  // Total cluster resources will become cpus=3, mem=1536.
  NodeInfo node2 = createNodeInfo("cpus:1;mem:512;disk:0");
  allocator->addNode(
      node2.id(),
      node2,
      NODE_CAPABILITIES(),
      None(),
      node2.resources(),
      {});

  {
    Allocation expected = Allocation(
        framework3.id(),
        {{"d/e", {{node2.id(), node2.resources()}}}});

    AWAIT_EXPECT_EQ(expected, allocations.get());
  }

  // a share = 0.666667 (cpus=2, mem=1024)
  //   a/b share = 0.666667 (cpus=2, mem=1024)
  //     framework1 share = 1
  //   a/c share = 0
  //     framework2 share = 0
  // d share = 0.333333 (cpus=1, mem=512)
  //   d/e share = 0.333333 (cpus=1, mem=512)
  //     framework3 share = 1

  // Add a new framework in the role "d/f".
  FrameworkInfo framework4 = createFrameworkInfo({"d/f"});
  allocator->addFramework(framework4.id(), framework4, {}, true, {});

  // Add a new node. The new node's resources should be allocated to
  // the framework in "d/f" (and not the framework in "a/c"), because the
  // "d" subtree has fewer allocated resources than the "a" subtree.
  //
  // Total cluster resources will become cpus=5, mem=2560.
  NodeInfo node3 = createNodeInfo("cpus:2;mem:1024;disk:0");
  allocator->addNode(
      node3.id(),
      node3,
      NODE_CAPABILITIES(),
      None(),
      node3.resources(),
      {});

  {
    Allocation expected = Allocation(
        framework4.id(),
        {{"d/f", {{node3.id(), node3.resources()}}}});

    AWAIT_EXPECT_EQ(expected, allocations.get());
  }

  // a share = 0.4 (cpus=2, mem=1024)
  //   a/b share = 0.4 (cpus=2, mem=1024)
  //     framework1 share = 1
  //   a/c share = 0
  //     framework2 share = 0
  // d share = 0.6 (cpus=3, mem=1536)
  //   d/e share = 0.2 (cpus=1, mem=512)
  //     framework3 share = 1
  //   d/f share = 0.4 (cpus=2, mem=1024)
  //     framework4 share = 1
}


// This test ensures that an offer filter larger than the
// allocation interval effectively filters out resources.
TEST_F(HierarchicalAllocatorTest, OfferFilter)
{
  // Pausing the clock is not necessary, but ensures that the test
  // doesn't rely on the batch allocation in the allocator, which
  // would slow down the test.
  Clock::pause();

  // We put both frameworks into the same role, but we could also
  // have had separate roles; this should not influence the test.
  const string ROLE{"role"};

  initialize();

  FrameworkInfo framework = createFrameworkInfo({ROLE});
  allocator->addFramework(framework.id(), framework, {}, true, {});

  NodeInfo node = createNodeInfo("cpus:1;mem:512;disk:0");
  allocator->addNode(
      node.id(),
      node,
      NODE_CAPABILITIES(),
      None(),
      node.resources(),
      {});

  // `framework` will be offered all of `node` resources
  // because it is the only framework in the cluster.
  Allocation expected = Allocation(
      framework.id(),
      {{ROLE, {{node.id(), node.resources()}}}});

  Future<Allocation> allocation = allocations.get();
  AWAIT_EXPECT_EQ(expected, allocation);

  // Now `framework` declines the offer and sets a filter
  // with the duration greater than the allocation interval.
  Duration filterTimeout = flags.allocation_interval * 2;
  Filters offerFilter;
  offerFilter.set_refuse_seconds(filterTimeout.secs());

  allocator->recoverResources(
      framework.id(),
      node.id(),
      allocation->resources.at(ROLE).at(node.id()),
      offerFilter,
      false);

  // Ensure the offer filter timeout is set before advancing the clock.
  Clock::settle();

  JSON::Object metrics = Metrics();

  string activeOfferFilters =
    "allocator/medios/offer_filters/roles/" + ROLE + "/active";
  EXPECT_EQ(1, metrics.values[activeOfferFilters]);

  // Trigger a batch allocation.
  Clock::advance(flags.allocation_interval);
  Clock::settle();

  // There should be no allocation due to the offer filter.
  allocation = allocations.get();
  EXPECT_TRUE(allocation.isPending());

  // Ensure the offer filter times out (2x the allocation interval)
  // and the next batch allocation occurs.
  Clock::advance(flags.allocation_interval);
  Clock::settle();

  // The next batch allocation should offer resources to `framework1`.
  expected = Allocation(
      framework.id(),
      {{ROLE, {{node.id(), node.resources()}}}});

  AWAIT_EXPECT_EQ(expected, allocation);

  metrics = Metrics();

  EXPECT_EQ(0, metrics.values[activeOfferFilters]);
}


// This test ensures that an offer filter is not removed earlier than
// the next batch allocation. See MESOS-4302 for more information.
//
// NOTE: If we update the code to allocate upon resource recovery
// (MESOS-3078), this test should still pass in that the small offer
// filter timeout should lead to the next allocation for the node
// applying the filter.
TEST_F(HierarchicalAllocatorTest, SmallOfferFilterTimeout)
{
  // Pausing the clock is not necessary, but ensures that the test
  // doesn't rely on the batch allocation in the allocator, which
  // would slow down the test.
  Clock::pause();

  // We put both frameworks into the same role, but we could also
  // have had separate roles; this should not influence the test.
  const string ROLE{"role"};

  // Explicitly set the allocation interval to make sure
  // it is greater than the offer filter timeout.
  controller::Flags flags_;
  flags_.allocation_interval = Minutes(1);

  initialize(flags_);

  FrameworkInfo framework1 = createFrameworkInfo({ROLE});
  allocator->addFramework(framework1.id(), framework1, {}, true, {});

  FrameworkInfo framework2 = createFrameworkInfo({ROLE});
  allocator->addFramework(framework2.id(), framework2, {}, true, {});

  NodeInfo node1 = createNodeInfo("cpus:1;mem:512;disk:0");
  allocator->addNode(
      node1.id(),
      node1,
      NODE_CAPABILITIES(),
      None(),
      node1.resources(),
      {{framework1.id(), allocatedResources(node1.resources(), ROLE)}});

  // Process all triggered allocation events.
  //
  // NOTE: No allocations happen because there are no resources to allocate.
  Clock::settle();

  // Total cluster resources (1 node): cpus=1, mem=512.
  // ROLE1 share = 1 (cpus=1, mem=512)
  //   framework1 share = 1 (cpus=1, mem=512)
  //   framework2 share = 0

  // Add one more node with some free resources.
  NodeInfo node2 = createNodeInfo("cpus:1;mem:512;disk:0");
  allocator->addNode(
      node2.id(),
      node2,
      NODE_CAPABILITIES(),
      None(),
      node2.resources(),
      {});

  // Process the allocation triggered by the node addition.
  Clock::settle();

  // `framework2` will be offered all of `node2` resources
  // because its share (0) is smaller than `framework1`.
  Allocation expected = Allocation(
      framework2.id(),
      {{ROLE, {{node2.id(), node2.resources()}}}});

  Future<Allocation> allocation = allocations.get();
  AWAIT_EXPECT_EQ(expected, allocation);

  // Total cluster resources (2 nodes): cpus=2, mem=1024.
  // ROLE1 share = 1 (cpus=2, mem=1024)
  //   framework1 share = 0.5 (cpus=1, mem=512)
  //   framework2 share = 0.5 (cpus=1, mem=512)

  // Now `framework2` declines the offer and sets a filter
  // for 1 second, which is less than the allocation interval.
  Duration filterTimeout = Seconds(1);
  ASSERT_GT(flags.allocation_interval, filterTimeout);

  Filters offerFilter;
  offerFilter.set_refuse_seconds(filterTimeout.secs());

  allocator->recoverResources(
      framework2.id(),
      node2.id(),
      allocation->resources.at(ROLE).at(node2.id()),
      offerFilter,
      false);

  // Total cluster resources (2 nodes): cpus=2, mem=1024.
  // ROLE1 share = 0.5 (cpus=1, mem=512)
  //   framework1 share = 1 (cpus=1, mem=512)
  //   framework2 share = 0

  // The offer filter times out. Since the allocator ensures that
  // offer filters are removed after at least one batch allocation
  // has occurred, we expect that after the timeout elapses, the
  // filter will remain active for the next allocation and the
  // resources are allocated to `framework1`.
  Clock::advance(filterTimeout);
  Clock::settle();

  // Trigger a batch allocation.
  Clock::advance(flags.allocation_interval);
  Clock::settle();

  // Since the filter is applied, resources are offered to `framework1`
  // even though its share is greater than `framework2`.
  expected = Allocation(
       framework1.id(),
       {{ROLE, {{node2.id(), node2.resources()}}}});

  allocation = allocations.get();
  AWAIT_EXPECT_EQ(expected, allocation);

  // Total cluster resources (2 nodes): cpus=2, mem=1024.
  // ROLE1 share = 1 (cpus=2, mem=1024)
  //   framework1 share = 1 (cpus=2, mem=1024)
  //   framework2 share = 0

  // The filter should be removed now than the batch
  // allocation has occurred!

  // Now `framework1` declines the offer.
  allocator->recoverResources(
      framework1.id(),
      node2.id(),
      allocation->resources.at(ROLE).at(node2.id()),
      None(),
      false);

  // Total cluster resources (2 nodes): cpus=2, mem=1024.
  // ROLE1 share = 0.5 (cpus=1, mem=512)
  //   framework1 share = 1 (cpus=1, mem=512)
  //   framework2 share = 0

  // Trigger a batch allocation.
  Clock::advance(flags.allocation_interval);

  // Since the filter is removed, resources are offered to `framework2`.
  expected = Allocation(
       framework2.id(),
       {{ROLE, {{node2.id(), node2.resources()}}}});

  allocation = allocations.get();
  AWAIT_EXPECT_EQ(expected, allocation);

  // Total cluster resources (2 nodes): cpus=2, mem=1024.
  // ROLE1 share = 1 (cpus=2, mem=1024)
  //   framework1 share = 0.5 (cpus=1, mem=512)
  //   framework2 share = 0.5 (cpus=1, mem=512)
}


// Tests that allocator continues to work after adding a node
// that has both maintenance schedule and resources used by not yet known
// frameworks. This is a regression test for MESOS-10109.
TEST_F(HierarchicalAllocatorTest, AddNodeWithUnknownFrameworkAndMaintenance)
{
  // Pausing the clock is not necessary, but ensures that the test
  // doesn't rely on the batch allocation in the allocator, which
  // would slow down the test.
  Clock::pause();

  initialize();

  // Create a fully used node which is about to enter maintenance
  // in a distant future, with resources used by some unknown framework.
  const NodeInfo node1 = createNodeInfo("cpus:2;mem:1024;disk:0");
  const FrameworkInfo unknown = createFrameworkInfo({"role"});
  const hashmap<FrameworkID, Resources> used{
    {unknown.id(), node1.resources()}};

  allocator->addNode(
      node1.id(),
      node1,
      NODE_CAPABILITIES(),
      protobuf::maintenance::createUnavailability(Clock::now() + Days(365)),
      node1.resources(),
      used);

  const FrameworkInfo framework = createFrameworkInfo({"role"});
  allocator->addFramework(framework.id(), framework, {}, true, {});

  // Create an empty node.
  NodeInfo node2 = createNodeInfo("cpus:2;mem:1024;disk:0");
  allocator->addNode(
      node2.id(),
      node2,
      NODE_CAPABILITIES(),
      None(),
      node2.resources(),
      {});

  // Check that resources of node2 go to the framework.
  const Allocation expected =
    Allocation(framework.id(), {{"role", {{node2.id(), node2.resources()}}}});

  AWAIT_EXPECT_EQ(expected, allocations.get());

  // Add node2 unavailability to trigger inverse offer generation.
  allocator->updateUnavailability(
      node2.id(),
      protobuf::maintenance::createUnavailability(Clock::now() + Seconds(60)));

  // Check that resources of node2 get inverse offered.
  Future<Deallocation> deallocation = deallocations.get();
  AWAIT_READY(deallocation);
  EXPECT_EQ(framework.id(), deallocation->frameworkId);
  EXPECT_TRUE(deallocation->resources.contains(node2.id()));
}


// This test ensures that nodes which are scheduled for maintenance are
// properly sent inverse offers after they have accepted or reserved resources.
// It also verifies that the frameworks declined the offer should get no
// inverse offers.
TEST_F(HierarchicalAllocatorTest, MaintenanceInverseOffers)
{
  // Pausing the clock is not necessary, but ensures that the test
  // doesn't rely on the batch allocation in the allocator, which
  // would slow down the test.
  Clock::pause();

  initialize();

  // Create a node which is about to enter maintenance.
  NodeInfo node1 = createNodeInfo("cpus:2;mem:1024;disk:0");
  allocator->addNode(
      node1.id(),
      node1,
      NODE_CAPABILITIES(),
      None(),
      node1.resources(),
      {});

  // This framework will be offered all of the resources.
  FrameworkInfo framework = createFrameworkInfo({"*"});
  allocator->addFramework(framework.id(), framework, {}, true, {});

  // Check that the resources go to the framework.
  Allocation expected = Allocation(
      framework.id(),
      {{"*", {{node1.id(), node1.resources()}}}});

  AWAIT_EXPECT_EQ(expected, allocations.get());

  // Create another node.
  NodeInfo node2 = createNodeInfo("cpus:2;mem:1024;disk:0");
  allocator->addNode(
      node2.id(),
      node2,
      NODE_CAPABILITIES(),
      None(),
      node2.resources(),
      {});

  // Check that resources of node2 go to the framework too.
  expected =
    Allocation(framework.id(), {{"*", {{node2.id(), node2.resources()}}}});

  AWAIT_EXPECT_EQ(expected, allocations.get());

  // Recover resources of the offer for node2.
  Filters filter1day;
  filter1day.set_refuse_seconds(Days(1).secs());

  allocator->recoverResources(
      framework.id(),
      node2.id(),
      allocatedResources(node2.resources(), "*"),
      filter1day,
      false);

  const process::Time start = Clock::now() + Seconds(60);
  // Give both nodes some unavailability.
  allocator->updateUnavailability(
      node1.id(), protobuf::maintenance::createUnavailability(start));
  allocator->updateUnavailability(
      node2.id(), protobuf::maintenance::createUnavailability(start));

  // Check the resources of node1 get inverse offered.
  Future<Deallocation> deallocation = deallocations.get();
  AWAIT_READY(deallocation);
  EXPECT_EQ(framework.id(), deallocation->frameworkId);
  EXPECT_TRUE(deallocation->resources.contains(node1.id()));

  // Resources of node2 should not be inverse offered.
  EXPECT_FALSE(deallocation->resources.contains(node2.id()));

  foreachvalue (
      const UnavailableResources& unavailableResources,
      deallocation->resources) {
    // The resources in the inverse offer are unspecified.
    // This means everything is being requested back.
    EXPECT_EQ(Resources(), unavailableResources.resources);

    EXPECT_EQ(
        start.duration(),
        Nanoseconds(unavailableResources.unavailability.start().nanoseconds()));
  }

  // Ensure only one offer should be deallocated.
  EXPECT_TRUE(deallocations.get().isPending());
}


// This test ensures that allocation is done per node. This is done
// by having 2 nodes and 2 frameworks and making sure each framework
// gets only one node's resources during an allocation.
TEST_F(HierarchicalAllocatorTest, CoarseGrained)
{
  // Pausing the clock ensures that the batch allocation does not
  // influence this test.
  Clock::pause();

  initialize();

  NodeInfo node1 = createNodeInfo("cpus:2;mem:1024;disk:0");
  allocator->addNode(
      node1.id(),
      node1,
      NODE_CAPABILITIES(),
      None(),
      node1.resources(),
      {});

  NodeInfo node2 = createNodeInfo("cpus:2;mem:1024;disk:0");
  allocator->addNode(
      node2.id(),
      node2,
      NODE_CAPABILITIES(),
      None(),
      node2.resources(),
      {});

  // Once framework1 is added, an allocation will occur. Return the
  // resources so that we can test what happens when there are 2
  // frameworks and 2 nodes to consider during allocation.
  FrameworkInfo framework1 = createFrameworkInfo({"role1"});
  allocator->addFramework(framework1.id(), framework1, {}, true, {});

  Allocation expected = Allocation(
      framework1.id(),
      {{"role1", {
          {node1.id(), node1.resources()},
          {node2.id(), node2.resources()}}
      }});

  Future<Allocation> allocation = allocations.get();
  AWAIT_EXPECT_EQ(expected, allocation);

  allocator->recoverResources(
      framework1.id(),
      node1.id(),
      allocation->resources.at("role1").at(node1.id()),
      None(),
      false);

  allocator->recoverResources(
      framework1.id(),
      node2.id(),
      allocation->resources.at("role1").at(node2.id()),
      None(),
      false);

  // Now add the second framework, we expect there to be 2 subsequent
  // allocations, each framework being allocated a full node.
  FrameworkInfo framework2 = createFrameworkInfo({"role2"});
  allocator->addFramework(framework2.id(), framework2, {}, true, {});

  hashmap<FrameworkID, Allocation> frameworkAllocations;

  allocation = allocations.get();
  AWAIT_READY(allocation);
  frameworkAllocations[allocation->frameworkId] = allocation.get();

  allocation = allocations.get();
  AWAIT_READY(allocation);
  frameworkAllocations[allocation->frameworkId] = allocation.get();

  // NOTE: `node1` and `node2` have the same resources, we don't care
  // which framework received which node, only that they each received one.
  ASSERT_TRUE(frameworkAllocations.contains(framework1.id()));

  allocation = frameworkAllocations.at(framework1.id());

  ASSERT_EQ(1u, allocation->resources.size());
  ASSERT_TRUE(allocation->resources.contains("role1"));
  EXPECT_EQ(allocatedResources(node1.resources(), "role1"),
            Resources::sum(allocation->resources.at("role1")));

  ASSERT_TRUE(frameworkAllocations.contains(framework2.id()));
  allocation = frameworkAllocations.at(framework2.id());

  ASSERT_EQ(1u, allocation->resources.size());
  ASSERT_TRUE(allocation->resources.contains("role2"));
  EXPECT_EQ(allocatedResources(node2.resources(), "role2"),
            Resources::sum(allocation->resources.at("role2")));
}


// This test ensures that frameworks that have the same share get an
// equal number of allocations over time (rather than the same
// framework getting all the allocations because its name is
// lexicographically ordered first).
TEST_F(HierarchicalAllocatorTest, SameShareFairness)
{
  Clock::pause();

  initialize();

  FrameworkInfo framework1 = createFrameworkInfo({"*"});
  allocator->addFramework(framework1.id(), framework1, {}, true, {});

  FrameworkInfo framework2 = createFrameworkInfo({"*"});
  allocator->addFramework(framework2.id(), framework2, {}, true, {});

  NodeInfo node = createNodeInfo("cpus:2;mem:1024;disk:0");
  allocator->addNode(
      node.id(),
      node,
      NODE_CAPABILITIES(),
      None(),
      node.resources(),
      {});

  // Ensure that the node's resources are alternated between both
  // frameworks.
  hashmap<FrameworkID, size_t> counts;

  for (int i = 0; i < 10; i++) {
    Future<Allocation> allocation = allocations.get();
    AWAIT_READY(allocation);

    Allocation expected = Allocation(
        allocation->frameworkId,
        {{"*", {{node.id(), node.resources()}}}});

    EXPECT_EQ(expected, allocation.get());

    counts[allocation->frameworkId]++;

    allocator->recoverResources(
        allocation->frameworkId,
        node.id(),
        allocation->resources.at("*").at(node.id()),
        None(),
        false);

    Clock::advance(flags.allocation_interval);
  }

  EXPECT_EQ(5u, counts[framework1.id()]);
  EXPECT_EQ(5u, counts[framework2.id()]);
}


class HierarchicalAllocatorTestWithReservations
  : public HierarchicalAllocatorTestBase,
    public WithParamInterface<Resource::ReservationInfo::Type> {};


INSTANTIATE_TEST_CASE_P(
    ReservationTypeSwitch,
    HierarchicalAllocatorTestWithReservations,
    testing::Values(
        Resource::ReservationInfo::DYNAMIC,
        Resource::ReservationInfo::STATIC));


// This test verifies that the reservations should be
// accounted towards the quota guarantee/limit even if
// they are currently unallocated.
TEST_P(HierarchicalAllocatorTestWithReservations, ReservationUnallocated)
{
  Clock::pause();

  const string QUOTA_ROLE{"quota-role"};

  initialize();

  Resource::ReservationInfo reservation;
  reservation.set_type(GetParam());
  reservation.set_role(QUOTA_ROLE);

  Resources resources = Resources::parse("cpus:1;mem:1024").get();
  resources = resources.pushReservation(reservation);

  NodeInfo node1 = createNodeInfo(resources);
  allocator->addNode(
      node1.id(),
      node1,
      NODE_CAPABILITIES(),
      None(),
      node1.resources(),
      {});

  Resources reserved = Resources(node1.resources()).reserved(QUOTA_ROLE);

  // Set a quota for 1x node resources.
  const Quota quota = createQuota("cpus:1;mem:1024");
  allocator->updateQuota(QUOTA_ROLE, quota);

  // Create `framework1` and set quota for its role.
  FrameworkInfo framework1 = createFrameworkInfo({QUOTA_ROLE});
  allocator->addFramework(framework1.id(), framework1, {}, true, {});

  // Process all triggered allocation events.
  Clock::settle();

  // `framework1` will be offered the reserved resources at `node1`
  // because those resources are reserved for its role.
  Allocation expected = Allocation(
      framework1.id(),
      {{QUOTA_ROLE, {{node1.id(), reserved}}}});

  AWAIT_EXPECT_EQ(expected, allocations.get());

  // Decline the reservations for the rest of the test.
  Filters filter1day;
  filter1day.set_refuse_seconds(Days(1).secs());
  allocator->recoverResources(
      framework1.id(),
      node1.id(),
      allocatedResources(reserved, QUOTA_ROLE),
      filter1day,
      false);

  // Add another node with unreserved resources.
  // This will trigger a batch allocation.
  NodeInfo node2 = createNodeInfo("cpus:1;mem:1024");
  allocator->addNode(
      node2.id(),
      node2,
      NODE_CAPABILITIES(),
      None(),
      node2.resources(),
      {});

  Clock::settle();

  // Node2's resources should not be allocated to `framework1` under
  // `QUOTA_ROLE` because the reserved resources should be accounted
  // towards the quota and allocating this node will exceed the
  // quota limit.
  Future<Allocation> allocation = allocations.get();
  EXPECT_TRUE(allocation.isPending());
}


// This test verifies that the reservations should be
// accounted towards the quota guarantee/limit if they
// are currently allocated.
TEST_P(HierarchicalAllocatorTestWithReservations, ReservationAllocated)
{
  Clock::pause();

  const string QUOTA_ROLE{"quota-role"};

  initialize();

  Resource::ReservationInfo reservation;
  reservation.set_type(GetParam());
  reservation.set_role(QUOTA_ROLE);

  Resources resources = Resources::parse("cpus:1;mem:1024").get();
  resources = resources.pushReservation(reservation);

  NodeInfo node1 = createNodeInfo(resources);
  allocator->addNode(
      node1.id(),
      node1,
      NODE_CAPABILITIES(),
      None(),
      node1.resources(),
      {});

  Resources reserved = Resources(node1.resources()).reserved(QUOTA_ROLE);

  // Set a quota for 2x node resources.
  const Quota quota = createQuota("cpus:2;mem:2048");
  allocator->updateQuota(QUOTA_ROLE, quota);

  // Create `framework1` and set quota for its role.
  FrameworkInfo framework1 = createFrameworkInfo({QUOTA_ROLE});
  allocator->addFramework(framework1.id(), framework1, {}, true, {});

  // Process all triggered allocation events.
  Clock::settle();

  // `framework1` will be offered the reserved resources at `node1`
  // because those resources are reserved for its role.
  Allocation expected = Allocation(
      framework1.id(),
      {{QUOTA_ROLE, {{node1.id(), reserved}}}});

  AWAIT_EXPECT_EQ(expected, allocations.get());

  // Add another node with unreserved resources.
  // This will trigger a batch allocation.
  NodeInfo node2 = createNodeInfo("cpus:1;mem:1024");
  allocator->addNode(
      node2.id(),
      node2,
      NODE_CAPABILITIES(),
      None(),
      node2.resources(),
      {});

  Clock::settle();

  expected = Allocation(
      framework1.id(),
      {{QUOTA_ROLE, {{node2.id(), node2.resources()}}}});

  // Node2's resources should be allocated to framework1
  // to meet its remaining quota.
  AWAIT_EXPECT_EQ(expected, allocations.get());

  // Add a third node with unreserved resources.
  // This will trigger a batch allocation.
  NodeInfo node3 = createNodeInfo("cpus:1;mem:1024");
  allocator->addNode(
      node3.id(),
      node3,
      NODE_CAPABILITIES(),
      None(),
      node3.resources(),
      {});

  Clock::settle();

  // Node3's resources should not be allocated to `framework1` under
  // `QUOTA_ROLE` because the role is already allocated its quota.
  // Some of this quota is satisfied via an allocated reservation.
  Future<Allocation> allocation = allocations.get();
  EXPECT_TRUE(allocation.isPending());
}


// This test verifies that the non-quota role can get its reservation allocated
// when it co-exists with roles with unsatisfied quota. See MESOS-8293.
TEST_P(HierarchicalAllocatorTestWithReservations,
  NonQuotaRoleReservationWithQuotaRole)
{
  Clock::pause();

  const string QUOTA_ROLE{"quota-role"};
  const string NON_QUOTA_ROLE{"non-quota-role"};

  initialize();

  NodeInfo node1;

  Resources unreserved = Resources::parse("cpus:2;mem:2048").get();

  Resource::ReservationInfo reservation;
  reservation.set_type(GetParam());
  reservation.set_role(NON_QUOTA_ROLE);

  Resources reserved = unreserved.pushReservation(reservation);

  node1 = createNodeInfo(unreserved + reserved);
  allocator->addNode(
      node1.id(),
      node1,
      NODE_CAPABILITIES(),
      None(),
      node1.resources(),
      {});

  const Quota quota = createQuota("cpus:2;mem:2048");
  allocator->updateQuota(QUOTA_ROLE, quota);

  // Create `framework1` and set quota to half the size of node1' resources
  // for its role.
  FrameworkInfo framework1 = createFrameworkInfo({QUOTA_ROLE});
  allocator->addFramework(framework1.id(), framework1, {}, true, {});

  // Process all triggered allocation events.
  Clock::settle();

  // `framework1` will be offered unreserved resources of `node1`
  // because it is the only framework.
  Allocation expected = Allocation(
      framework1.id(),
      {{QUOTA_ROLE, {{node1.id(), unreserved}}}});

  AWAIT_EXPECT_EQ(expected, allocations.get());

  // Decline resources on node1 for the rest of the test.
  Filters filter1day;
  filter1day.set_refuse_seconds(Days(1).secs());
  allocator->recoverResources(
      framework1.id(),
      node1.id(),
      allocatedResources(unreserved, QUOTA_ROLE),
      filter1day,
      false);

  // Create `framework2` which belongs to the `NON_QUOTA_ROLE`
  // and is entitled to its reserved resources.
  // This will trigger a batch allocation.
  FrameworkInfo framework2 = createFrameworkInfo({NON_QUOTA_ROLE});
  allocator->addFramework(framework2.id(), framework2, {}, true, {});

  Clock::settle();

  // `framework2` will be offered its reserved resources on `node1`
  // even though half of node1's resources are set aside for the quota
  // headroom.
  expected = Allocation(
      framework2.id(),
      {{NON_QUOTA_ROLE, {{node1.id(), reserved}}}});

  AWAIT_EXPECT_EQ(expected, allocations.get());
}


// This test verifies that when enforcing quota limit, shared resources
// that are part of a role's reserved-allocated resources are only
// charged once even when they are offered multiple times.
TEST_P(HierarchicalAllocatorTestWithReservations,
  SharedAllocatedResourceQuotaAccounting)
{
  // We test this by creating a quota for 100 disk resources.
  // We then create three nodes:
  //
  //   (1) One node is reserved for the quota role with 50 disk.
  //       On this node, we create a shared volume using the 50
  //       disk, and allocate it twice to the framework.
  //
  //   (2) We create another node with 50 disk. Since the role
  //       only has 50 disk allocated to it (two instances of the
  //       50 disk shared volume), the disk should be allocated
  //       towards the role's quota.
  //
  //   (3) We create another node with 50 disk. This time, the
  //       node should not be allocated to the role since now
  //       the role has 100 disk already allocated to it.
  Clock::pause();

  const string QUOTA_ROLE{"quota-role"};

  initialize();

  const Quota quota = createQuota("cpus:3;mem:2048;disk:100");
  allocator->updateQuota(QUOTA_ROLE, quota);

  Resource::ReservationInfo reservation;
  reservation.set_type(GetParam());
  reservation.set_role(QUOTA_ROLE);

  Resources reserved = Resources::parse("cpus:2;mem:1024;disk:50").get();
  reserved = reserved.pushReservation(reservation);

  NodeInfo node1 = createNodeInfo(reserved);
  allocator->addNode(
      node1.id(),
      node1,
      NODE_CAPABILITIES(),
      None(),
      node1.resources(),
      {});

  FrameworkInfo framework = createFrameworkInfo(
      {QUOTA_ROLE},
      {FrameworkInfo::Capability::SHARED_RESOURCES});

  allocator->addFramework(framework.id(), framework, {}, true, {});

  Allocation expected = Allocation(
    framework.id(),
    {{QUOTA_ROLE, {{node1.id(), reserved}}}});

  Future<Allocation> allocation = allocations.get();
  AWAIT_EXPECT_EQ(expected, allocation);

  // Quota: "cpus:3;mem:2048;disk:100".
  // Allocated quota: "cpus:2;mem:1024;disk:50".

  Resource::AllocationInfo allocationInfo;
  allocationInfo.set_role(QUOTA_ROLE);

  // Create a shared volume.
  Resource volume = createDiskResource(
      "50", QUOTA_ROLE, "id1", None(), None(), true);

  // Inject reservation info.
  volume.add_reservations()->CopyFrom(reservation);

  Offer::Operation create = CREATE(volume);
  protobuf::injectAllocationInfo(&create, allocationInfo);

  Try<vector<ResourceConversion>> conversions = getResourceConversions(create);
  ASSERT_SOME(conversions);

  // Ensure the CREATE operation can be applied.
  Try<Resources> updated =
    allocation->resources.at(QUOTA_ROLE).at(node1.id())
      .apply(conversions.get());

  ASSERT_SOME(updated);

  // Update the allocation in the allocator with a CREATE operation.
  allocator->updateAllocation(
      framework.id(),
      node1.id(),
      allocation->resources.at(QUOTA_ROLE).at(node1.id()),
      conversions.get());

  // Recover part of the resources for the next allocation.
  Resources recover = allocatedResources(
      Resources::parse("cpus:1;mem:512").get(),
      QUOTA_ROLE);

  // Inject reservation info.
  recover = recover.pushReservation(reservation);

  allocator->recoverResources(
      framework.id(),
      node1.id(),
      recover,
      None(),
      false);

  // Quota: "cpus:3;mem:2048;disk:100".
  // Allocated quota: "cpus:1;mem:512;disk:50".

  // Trigger a batch allocation.
  Clock::advance(flags.allocation_interval);

  // The remaining resources (cpus:1;mem:512) of node1 along with
  // the shared volume are offered again.
  expected = Allocation(
      framework.id(),
      {{QUOTA_ROLE, {{node1.id(), recover + create.create().volumes()}}}});

  AWAIT_EXPECT_EQ(expected, allocations.get());

  // Quota: "cpus:3;mem:2048;disk:100".
  // Allocated quota: "cpus:2;mem:1024;disk:50".
  // Note: 50 shared disk is allocated twice but should only count once.

  // Add node2. This will trigger a batch allocation.
  NodeInfo node2 = createNodeInfo("cpus:1;mem:1024;disk:50");
  allocator->addNode(
      node2.id(),
      node2,
      NODE_CAPABILITIES(),
      None(),
      node2.resources(),
      {});

  // `framework` will get all of node2's resources.
  expected = Allocation(
       framework.id(),
       {{QUOTA_ROLE, {{node2.id(), node2.resources()}}}});

  AWAIT_EXPECT_EQ(expected, allocations.get());

  // Quota: "cpus:3;mem:2048;disk:100"
  // Allocated quota: "cpus:3;mem:2048;disk:100"
  // The quota is met.

  // Add node3.
  // This will trigger a batch allocation.
  NodeInfo node3 = createNodeInfo("cpus:0.5;mem:512;disk:50");
  allocator->addNode(
      node3.id(),
      node3,
      NODE_CAPABILITIES(),
      None(),
      node3.resources(),
      {});

  Clock::settle();

  // No more allocations because quota limit has been reached.
  EXPECT_TRUE(allocations.get().isPending());
}

// This test ensures that reserving already allocated resources would not
// affect quota allocation.
TEST_F(HierarchicalAllocatorTest, QuotaAccountingReserveAllocatedResources)
{
  Clock::pause();
  initialize();

  const string QUOTA_ROLE{"quota-role"};

  // Create `framework` and set quota for its role.

  const Quota quota = createQuota("cpus:10;mem:1024");
  allocator->updateQuota(QUOTA_ROLE, quota);

  FrameworkInfo framework = createFrameworkInfo({QUOTA_ROLE});
  allocator->addFramework(framework.id(), framework, {}, true, {});

  Resources nodeResources = CHECK_NOTERROR(Resources::parse("cpus:5;mem:512"));

  // Add `node1`. This will trigger an event-driven allocation.
  NodeInfo node1 = createNodeInfo(nodeResources);
  allocator->addNode(
      node1.id(), node1, NODE_CAPABILITIES(), None(), nodeResources, {});

  // All of node1's resources will be offered to `framework`
  // as part of its quota.
  Allocation expected =
    Allocation(framework.id(), {{QUOTA_ROLE, {{node1.id(), nodeResources}}}});

  Future<Allocation> allocation = allocations.get();
  AWAIT_EXPECT_EQ(expected, allocation);

  // `framework` unsatisfied quota: cpus:5;mem:512

  // Let `framework` reserve its allocated resources on node1.
  Resources reserveResources = nodeResources;
  reserveResources.allocate(QUOTA_ROLE);
  reserveResources = reserveResources.pushReservation(
      createDynamicReservationInfo(QUOTA_ROLE, "principal"));

  Offer::Operation reserve = RESERVE(reserveResources);

  // Update the allocation in the allocator with a `RESERVE` operation.
  allocator->updateAllocation(
      framework.id(),
      node1.id(),
      allocation->resources.at(QUOTA_ROLE).at(node1.id()),
      CHECK_NOTERROR(getResourceConversions(reserve)));

  // Reserving allocated resources does not change unsatisfied quota.
  // `framework` unsatisfied quota: cpus:5;mem:512

  // Add `node2` with twice the resources as that of `node1`.
  // This will trigger an event-driven allocation.
  NodeInfo node2 = createNodeInfo(nodeResources + nodeResources);
  allocator->addNode(
      node2.id(), node2, NODE_CAPABILITIES(), None(), nodeResources, {});

  // `node2` will be chopped and half of its resources will be allocated to
  // `framework` to meet its unsatisfied quota.
  expected =
    Allocation(framework.id(), {{QUOTA_ROLE, {{node2.id(), nodeResources}}}});

  AWAIT_EXPECT_EQ(expected, allocations.get());
}


// This test ensures that unreserving allocated resources would not affect
// quota allocation.
TEST_F(HierarchicalAllocatorTest, QuotaAccountingUnreserveAllocatedResources)
{
  Clock::pause();
  initialize();

  // Create `framework` and set quota for its role.

  const Quota quota = createQuota("cpus:1;mem:1024");
  allocator->updateQuota("quota-role", quota);

  FrameworkInfo framework = createFrameworkInfo({"quota-role"});
  allocator->addFramework(framework.id(), framework, {}, true, {});

  Resources reservedResources =
    CHECK_NOTERROR(Resources::parse("cpus(quota-role):1;mem(quota-role):1024"));

  // Add `node1` with reserved resources for "quota-role" that matches exactly
  // its quota. This will trigger an event-driven allocation.
  NodeInfo node1 = createNodeInfo(reservedResources);
  allocator->addNode(
      node1.id(),
      node1,
      NODE_CAPABILITIES(),
      None(),
      node1.resources(),
      {});

  // All of node1's resources will be offered to `framework` to satisfy
  // all of its quota.
  Allocation expected = Allocation(
      framework.id(), {{"quota-role", {{node1.id(), node1.resources()}}}});

  AWAIT_EXPECT_EQ(expected, allocations.get());

  // Add `node2` with unreserved resources. This will trigger an event-driven
  // allocation.
  NodeInfo node2 = createNodeInfo("cpus:1;mem:1024");
  allocator->addNode(
      node2.id(),
      node2,
      NODE_CAPABILITIES(),
      None(),
      node2.resources(),
      {});

  Clock::settle();

  // No allocation should be made because `framework` is under "quota-role"
  // and its quota is already satisfied by `node1`.
  Future<Allocation> allocation = allocations.get();
  EXPECT_TRUE(allocation.isPending());

  // Unreserve the allocated resources on `node1`.
  reservedResources.allocate("quota-role");
  Offer::Operation unreserve = UNRESERVE(reservedResources);

  allocator->updateAllocation(
      framework.id(),
      node1.id(),
      reservedResources,
      CHECK_NOTERROR(getResourceConversions(unreserve)));

  // Trigger the next allocation cycle.
  Clock::advance(flags.allocation_interval);
  Clock::settle();

  // Still no allocation should be made because un-reserving allocated resources
  // does not affect allocated resource quantities. "quota-role"'s quota is
  // still satisfied by allocated resources on `node1`.
  EXPECT_TRUE(allocation.isPending());
}


// Checks that resources on a node that are statically reserved to
// a role are only offered to frameworks in that role.
TEST_F(HierarchicalAllocatorTest, Reservations)
{
  Clock::pause();

  initialize();

  NodeInfo node1 = createNodeInfo(
      "cpus(role1):2;mem(role1):1024;disk(role1):0");
  allocator->addNode(
      node1.id(),
      node1,
      NODE_CAPABILITIES(),
      None(),
      node1.resources(),
      {});

  NodeInfo node2 = createNodeInfo(
      "cpus(role2):2;mem(role2):1024;cpus:1;mem:1024;disk:0");
  allocator->addNode(
      node2.id(),
      node2,
      NODE_CAPABILITIES(),
      None(),
      node2.resources(),
      {});

  // This node's resources should never be allocated, since there
  // is no framework for role3.
  NodeInfo node3 = createNodeInfo(
      "cpus(role3):1;mem(role3):1024;disk(role3):0");
  allocator->addNode(
      node3.id(),
      node3,
      NODE_CAPABILITIES(),
      None(),
      node3.resources(),
      {});

  // framework1 should get all the resources from node1, and the
  // unreserved resources from node2.
  FrameworkInfo framework1 = createFrameworkInfo({"role1"});
  allocator->addFramework(framework1.id(), framework1, {}, true, {});

  Allocation expected = Allocation(
      framework1.id(),
      {{"role1", {
        {node1.id(), node1.resources()},
        {node2.id(), Resources(node2.resources()).unreserved()}
      }}});

  AWAIT_EXPECT_EQ(expected, allocations.get());

  // framework2 should get all of its reserved resources on node2.
  FrameworkInfo framework2 = createFrameworkInfo({"role2"});
  allocator->addFramework(framework2.id(), framework2, {}, true, {});

  expected = Allocation(
      framework2.id(),
      {{"role2", {
        {node2.id(), Resources(node2.resources()).reserved("role2")}
      }}});

  AWAIT_EXPECT_EQ(expected, allocations.get());
}


// This test verifies that quota headroom is correctly maintained when some
// roles have more reservations than their quotas.
TEST_P(HierarchicalAllocatorTestWithReservations,
  QuotaHeadroomWhenReservationsExceedQuota)
{
  Clock::pause();
  initialize();

  // We start with two roles `QUOTA_ROLE_W_RESERVATION` and
  // `QUOTA_ROLE_NO_RESERVATION` with the same quota (cpus:1;mem:1024) set.
  // `QUOTA_ROLE_W_RESERVATION` will also have reservations twice of its quota.
  // Thus we have its reservation amount equals to the combined quota of both
  // roles.
  const string QUOTA_ROLE_W_RESERVATION{"quota-role-w-reservation"};
  const string QUOTA_ROLE_NO_RESERVATION{"quota-role-no-reservation"};
  const string NON_QUOTA_ROLE{"non-quota-role"};

  const Quota quota1 = createQuota("cpus:1;mem:1024");
  allocator->updateQuota(QUOTA_ROLE_W_RESERVATION, quota1);

  const Quota quota2 = createQuota("cpus:1;mem:1024");
  allocator->updateQuota(QUOTA_ROLE_NO_RESERVATION, quota2);

  // Add `node1` with reserved resources for `QUOTA_ROLE_W_RESERVATION`.
  Resource::ReservationInfo reservation;
  reservation.set_type(GetParam());
  reservation.set_role(QUOTA_ROLE_W_RESERVATION);

  Resources reserved = Resources::parse("cpus:2;mem:2048").get();
  reserved = reserved.pushReservation(reservation);

  NodeInfo node1 = createNodeInfo(reserved);
  allocator->addNode(
      node1.id(),
      node1,
      NODE_CAPABILITIES(),
      None(),
      node1.resources(),
      {});

  // No quota headroom needed for `QUOTA_ROLE_W_RESERVATION`.
  // (cpus:1;mem:1024) quota headroom needed for `QUOTA_ROLE_NO_RESERVATION`.
  // Combined quota headroom: (cpus:1;mem:1024)

  // No allocation will happen because there are no frameworks.

  // Add `node2` with unreserved resources.
  NodeInfo node2 = createNodeInfo("cpus:1;mem:1024");
  allocator->addNode(
      node2.id(),
      node2,
      NODE_CAPABILITIES(),
      None(),
      node2.resources(),
      {});

  // Add `framework` under `NON_QUOTA_ROLE`.
  // This will trigger a batch allocation.
  FrameworkInfo framework = createFrameworkInfo({NON_QUOTA_ROLE});
  allocator->addFramework(framework.id(), framework, {}, true, {});

  Clock::settle();

  // No allocation should happen because resources on node2 are
  // set aside for the quota headroom.
  Future<Allocation> allocation = allocations.get();
  EXPECT_TRUE(allocation.isPending());

  // Add `node3` with unreserved resources.
  // This will trigger an event-driven allocation loop for node3.
  NodeInfo node3 = createNodeInfo("cpus:1;mem:1024");
  allocator->addNode(
      node3.id(),
      node3,
      NODE_CAPABILITIES(),
      None(),
      node3.resources(),
      {});

  Clock::settle();

  // All resources on `node3` are allocated to `framework` because
  // it is the only framework and quota headroom has been met by `node1`
  // and `node2`.
  Allocation expected = Allocation(
      framework.id(),
      {{NON_QUOTA_ROLE, {{node3.id(), node3.resources()}}}});

  AWAIT_EXPECT_EQ(expected, allocation);
}


// Checks that recovered resources are re-allocated correctly.
TEST_F(HierarchicalAllocatorTest, RecoverResources)
{
  Clock::pause();

  initialize();

  NodeInfo node = createNodeInfo(
      "cpus(role1):1;mem(role1):200;"
      "cpus:1;mem:200;disk:0");
  allocator->addNode(
      node.id(),
      node,
      NODE_CAPABILITIES(),
      None(),
      node.resources(),
      {});

  // Initially, all the resources are allocated.
  FrameworkInfo framework = createFrameworkInfo({"role1"});
  allocator->addFramework(framework.id(), framework, {}, true, {});

  Allocation expected = Allocation(
      framework.id(),
      {{"role1", {{node.id(), node.resources()}}}});

  Future<Allocation> allocation = allocations.get();
  AWAIT_EXPECT_EQ(expected, allocation);

  // Recover the reserved resources, expect them to be re-offered.
  Resources reserved = allocation->resources.at("role1").at(node.id())
    .reserved("role1");
  Resources unreserved = allocation->resources.at("role1").at(node.id())
    .unreserved();

  allocator->recoverResources(
      allocation->frameworkId,
      node.id(),
      reserved,
      None(),
      false);

  Clock::advance(flags.allocation_interval);

  expected = Allocation(
      framework.id(),
      {{"role1", {{node.id(), reserved}}}});

  allocation = allocations.get();
  AWAIT_EXPECT_EQ(expected, allocation);

  // Recover the unreserved resources, expect them to be re-offered.
  allocator->recoverResources(
      allocation->frameworkId,
      node.id(),
      unreserved,
      None(),
      false);

  Clock::advance(flags.allocation_interval);

  expected = Allocation(
      framework.id(),
      {{"role1", {{node.id(), unreserved}}}});

  allocation = allocations.get();
  AWAIT_EXPECT_EQ(expected, allocation);
}


// Checks that resource provider resources can be added to a node
// and that the added used resources are correctly taken into account
// when computing fair share.
TEST_F(HierarchicalAllocatorTest, AddResourceProvider)
{
  Clock::pause();

  initialize();

  // Register two deactivated frameworks.
  FrameworkInfo framework1 = createFrameworkInfo({"role1"});
  allocator->addFramework(framework1.id(), framework1, {}, false, {});

  FrameworkInfo framework2 = createFrameworkInfo({"role2"});
  allocator->addFramework(framework2.id(), framework2, {}, false, {});

  // Add a single node with `resources` resources.
  const Resources resources = Resources::parse("cpus:1;mem:100;disk:10").get();

  NodeInfo node1 = createNodeInfo(resources);
  allocator->addNode(
      node1.id(),
      node1,
      NODE_CAPABILITIES(),
      None(),
      node1.resources(),
      {});

  {
    // Add a resource provider with `resources*2` to the node, all in
    // use by `framework1`.
    Resources allocation = resources + resources;
    allocation.allocate("role1");
    allocator->addResourceProvider(
        node1.id(),
        resources + resources,
        {{framework1.id(), allocation}});
  }

  // Activate `framework2`. The next allocation will be to
  // `framework2` which is the only active framework. After that
  // `framework1`'s dominant share is 2/3 and `framework2`'s is 1/3.
  allocator->activateFramework(framework2.id());

  {
    Resources allocation = node1.resources();
    allocation.allocate("role2");
    Allocation expected = Allocation(
        framework2.id(),
        {{"role2", {{node1.id(), allocation}}}});

    AWAIT_EXPECT_EQ(expected, allocations.get());
  }

  // Activate `framework1` so it can receive offers. Currently all
  // available resources are allocated.
  allocator->activateFramework(framework1.id());

  // Add another node with `resources` resources. With that
  // `framework1` no has a dominant share of 2/4 and `framework2` of
  // 1/4.
  NodeInfo node2 = createNodeInfo(resources);
  allocator->addNode(
      node2.id(),
      node2,
      NODE_CAPABILITIES(),
      None(),
      node2.resources(),
      {});

  {
    // The next allocation will be to `framework2` since it is
    // furthest below fair share.
    Resources allocation = node2.resources();
    allocation.allocate("role2");
    Allocation expected = Allocation(
        framework2.id(),
        {{"role2", {{node2.id(), allocation}}}});

    AWAIT_EXPECT_EQ(expected, allocations.get());
  }
}


// Check that even if as an overallocated resource provider is added to an
// node, new allocations are only made for unused node resources.
TEST_F(HierarchicalAllocatorTest, AddResourceProviderOverallocated)
{
  Clock::pause();

  initialize();

  const Resources resources = Resources::parse("cpus:1;mem:100;disk:10").get();

  // Register a node.
  NodeInfo node = createNodeInfo(resources + resources);
  allocator->addNode(
      node.id(),
      node,
      NODE_CAPABILITIES(),
      None(),
      node.resources(),
      {});

  // Register a framework in deactivated state
  // so it initially does not receive offers.
  FrameworkInfo framework = createFrameworkInfo({"role"});
  allocator->addFramework(framework.id(), framework, {}, false, {});

  // Track an allocation to the framework of half the node's resources. We add
  // no new resources to the total, but just increment the used resources.
  Resources allocation = resources;
  allocation.allocate("role");
  allocator->addResourceProvider(
      node.id(),
      Resources(),
      {{framework.id(), allocation}});

  // Activate framework so it receives offers.
  allocator->activateFramework(framework.id());

  // Trigger a batch allocation. In the subsequent offer we expect the
  // framework to receive the other half of the node's resources so
  // that it now has all its resources allocated to it.
  Clock::advance(flags.allocation_interval);
  Clock::settle();

  Allocation expected = Allocation(
      framework.id(),
      {{"role", {{node.id(), allocation}}}});

  AWAIT_EXPECT_EQ(expected, allocations.get());
}


TEST_F(HierarchicalAllocatorTest, Allocatable)
{
  // Pausing the clock is not necessary, but ensures that the test
  // doesn't rely on the batch allocation in the allocator, which
  // would slow down the test.
  Clock::pause();

  initialize();

  FrameworkInfo framework = createFrameworkInfo({"role1"});
  allocator->addFramework(framework.id(), framework, {}, true, {});

  // Not enough memory or cpu to be considered allocatable.
  NodeInfo node1 = createNodeInfo(
      "cpus:" + stringify(MIN_CPUS / 2u) + ";"
      "mem:" + stringify(
          (double) (MIN_MEM / 2u).bytes() / Bytes::MEGABYTES) + ";"
      "disk:128");
  allocator->addNode(
      node1.id(),
      node1,
      NODE_CAPABILITIES(),
      None(),
      node1.resources(),
      {});

  // Enough cpus to be considered allocatable.
  NodeInfo node2 = createNodeInfo(
      "cpus:" + stringify(MIN_CPUS) + ";"
      "mem:" + stringify(
          (double) (MIN_MEM / 2u).bytes() / Bytes::MEGABYTES) + ";"
      "disk:128");
  allocator->addNode(
      node2.id(),
      node2,
      NODE_CAPABILITIES(),
      None(),
      node2.resources(),
      {});

  Allocation expected = Allocation(
      framework.id(),
      {{"role1", {{node2.id(), node2.resources()}}}});

  AWAIT_EXPECT_EQ(expected, allocations.get());

  // Enough memory to be considered allocatable.
  NodeInfo node3 = createNodeInfo(
      "cpus:" + stringify(MIN_CPUS / 2u) + ";"
      "mem:" + stringify((double) (MIN_MEM).bytes() / Bytes::MEGABYTES) + ";"
      "disk:128");
  allocator->addNode(
      node3.id(),
      node3,
      NODE_CAPABILITIES(),
      None(),
      node3.resources(),
      {});

  expected = Allocation(
      framework.id(),
      {{"role1", {{node3.id(), node3.resources()}}}});

  AWAIT_EXPECT_EQ(expected, allocations.get());

  // node4 has enough cpu and memory to be considered allocatable,
  // but it lies across unreserved and reserved resources!
  NodeInfo node4 = createNodeInfo(
      "cpus:" + stringify(MIN_CPUS * 3u / 2u) + ";"
      "mem:" + stringify(
          (double) (MIN_MEM / 2u).bytes() / Bytes::MEGABYTES) + ";"
      "cpus(role1):" + stringify(MIN_CPUS * 3u / 2u) + ";"
      "mem(role1):" + stringify(
          (double) (MIN_MEM / 2u).bytes() / Bytes::MEGABYTES) + ";"
      "disk:128");
  allocator->addNode(
      node4.id(),
      node4,
      NODE_CAPABILITIES(),
      None(),
      node4.resources(),
      {});

  expected = Allocation(
      framework.id(),
      {{"role1", {{node4.id(), node4.resources()}}}});

  AWAIT_EXPECT_EQ(expected, allocations.get());
}


// Check that if a framework specifies minimum allocatable resources, it
// correctly overrides the global minimum. We check this by ensuring it gets an
// offer when the global minimum is not satisfied but the framework's
// role-specific minimum is. This test also confirms that framework minimal
// allocatable resources can be updated in the allocator by checking that the
// framework gets no offers with the default flags and only specify a limit
// later on.
TEST_F(HierarchicalAllocatorTest, FrameworkMinAllocatable)
{
  // Pausing the clock is not necessary, but ensures that the test
  // doesn't rely on the batch allocation in the allocator, which
  // would slow down the test.
  Clock::pause();

  controller::Flags flags = controller::Flags();
  flags.min_allocatable_resources = "cpus:0.01|mem:32";

  initialize(flags);

  // Initially start the framework without any non-standard static
  // offer filters.
  FrameworkInfo framework = createFrameworkInfo({"role1"});
  allocator->addFramework(framework.id(), framework, {}, true, {});

  // Not enough memory or cpu to be considered allocatable with
  // default minimum allocatable resources.
  NodeInfo node = createNodeInfo(
      "cpus:" + stringify(0.01 / 2) + ";"
      "mem:" + stringify(32. / 2) + ";"
      "disk:128");
  allocator->addNode(
      node.id(),
      node,
      NODE_CAPABILITIES(),
      None(),
      node.resources(),
      {});

  // The framework should not get offers as no resources allocatable
  // to it are available.
  Future<Allocation> allocation = allocations.get();

  Clock::settle();
  ASSERT_TRUE(allocation.isPending());

  // Update the framework with custom minimal allocatable resources
  // below the globally configured minimal allocatable resources and
  // statisfiable by the available resources.
  Value::Scalar minCpus;
  minCpus.set_value(0.01 / 3);

  framework.mutable_offer_filters()->insert({framework.roles(0), {}});
  framework.mutable_offer_filters()
    ->at(framework.roles(0))
    .mutable_min_allocatable_resources()
    ->add_quantities()
    ->mutable_quantities()
    ->insert({"cpus", minCpus});
  allocator->updateFramework(framework.id(), framework, {});

  // Advance the clock to trigger allocation of the available resources.
  Clock::advance(flags.allocation_interval);

  Allocation expected =
    Allocation(framework.id(), {{"role1", {{node.id(), node.resources()}}}});

  AWAIT_EXPECT_EQ(expected, allocation);
}


// Check that a framework-specified empty set of minimum allocatable resource
// requirements is interpreted as the framework accepting any resources
// regardless of the global limit.
TEST_F(HierarchicalAllocatorTest, FrameworkEmptyMinAllocatable)
{
  // Pausing the clock is not necessary, but ensures that the test
  // doesn't rely on the batch allocation in the allocator, which
  // would slow down the test.
  Clock::pause();

  controller::Flags flags = controller::Flags();
  flags.min_allocatable_resources = "cpus:0.01|mem:32";

  initialize(flags);

  // Add a framework which specifies minimum allocatable resources
  // with an empty set of quantities.
  FrameworkInfo framework = createFrameworkInfo({"role1"});
  framework.mutable_offer_filters()->insert({framework.roles(0), {}});
  framework.mutable_offer_filters()
    ->at(framework.roles(0))
    .mutable_min_allocatable_resources();
  allocator->addFramework(framework.id(), framework, {}, true, {});

  // Not enough memory or cpu to be considered allocatable with
  // default minimum allocatable resources.
  NodeInfo node = createNodeInfo(
      "cpus:" + stringify(0.01 / 2) + ";"
      "mem:" + stringify(32. / 2) + ";"
      "disk:128");
  allocator->addNode(
      node.id(),
      node,
      NODE_CAPABILITIES(),
      None(),
      node.resources(),
      {});

  Allocation expected =
    Allocation(framework.id(), {{"role1", {{node.id(), node.resources()}}}});

  AWAIT_EXPECT_EQ(expected, allocations.get());
}


// This test ensures that frameworks can apply offer operations (e.g.,
// creating persistent volumes) on their allocations.
TEST_F(HierarchicalAllocatorTest, UpdateAllocation)
{
  Clock::pause();

  initialize();

  NodeInfo node = createNodeInfo("cpus:100;mem:100;disk:100");
  allocator->addNode(
      node.id(),
      node,
      NODE_CAPABILITIES(),
      None(),
      node.resources(),
      {});

  // Initially, all the resources are allocated.
  FrameworkInfo framework = createFrameworkInfo({"role1"});
  allocator->addFramework(framework.id(), framework, {}, true, {});

  Allocation expected = Allocation(
      framework.id(),
      {{"role1", {{node.id(), node.resources()}}}});

  Future<Allocation> allocation = allocations.get();
  AWAIT_EXPECT_EQ(expected, allocation);

  // Construct an offer operation for the framework's allocation.
  Resource volume = Resources::parse("disk", "5", "*").get();
  volume.mutable_disk()->mutable_persistence()->set_id("ID");
  volume.mutable_disk()->mutable_volume()->set_container_path("data");
  volume.mutable_allocation_info()->set_role("role1");

  Offer::Operation create;
  create.set_type(Offer::Operation::CREATE);
  create.mutable_create()->add_volumes()->CopyFrom(volume);

  Try<vector<ResourceConversion>> conversions = getResourceConversions(create);
  ASSERT_SOME(conversions);

  // Ensure the offer operation can be applied.
  Try<Resources> updated =
    allocation->resources.at("role1").at(node.id()).apply(conversions.get());

  ASSERT_SOME(updated);

  // Update the allocation in the allocator.
  allocator->updateAllocation(
      framework.id(),
      node.id(),
      allocation->resources.at("role1").at(node.id()),
      conversions.get());

  // Now recover the resources, and expect the next allocation to
  // contain the updated resources.
  allocator->recoverResources(
      framework.id(),
      node.id(),
      updated.get(),
      None(),
      false);

  Clock::advance(flags.allocation_interval);

  // The allocation should be the node's resources with the offer
  // operation applied.
  expected = Allocation(
      framework.id(),
      {{"role1", {{node.id(), updated.get()}}}});

  allocation = allocations.get();
  AWAIT_EXPECT_EQ(expected, allocation);
}


// This test ensures that frameworks can apply resource conversions that remove
// resources from their allocations and the node's total resources.
TEST_F(HierarchicalAllocatorTest, UpdateAllocationRemoveResources)
{
  Clock::pause();

  initialize();

  NodeInfo node = createNodeInfo("cpus:100;mem:100;disk:100");
  allocator->addNode(
      node.id(),
      node,
      NODE_CAPABILITIES(),
      None(),
      node.resources(),
      {});

  // Initially, all the resources are allocated.
  FrameworkInfo framework = createFrameworkInfo({"role1"});
  allocator->addFramework(framework.id(), framework, {}, true, {});

  Allocation expected = Allocation(
      framework.id(),
      {{"role1", {{node.id(), node.resources()}}}});

  Future<Allocation> allocation = allocations.get();
  AWAIT_EXPECT_EQ(expected, allocation);

  // Construct a resource conversion that removes all disks from the framework's
  // allocation and the total resources.
  Resource removed = Resources::parse("disk", "100", "*").get();
  removed.mutable_allocation_info()->set_role("role1");
  vector<ResourceConversion> conversions{
    ResourceConversion(removed, Resources())
  };

  // Ensure that the resource conversion can be applied.
  Try<Resources> updated =
    allocation->resources.at("role1").at(node.id()).apply(conversions);

  ASSERT_SOME(updated);
  EXPECT_NONE(updated->disk());

  // Update the allocation in the allocator.
  allocator->updateAllocation(
      framework.id(),
      node.id(),
      allocation->resources.at("role1").at(node.id()),
      conversions);

  // Now recover the resources, and expect that the next allocation contains the
  // updated resources.
  allocator->recoverResources(
      framework.id(),
      node.id(),
      updated.get(),
      None(),
      false);

  Clock::advance(flags.allocation_interval);

  // The allocation should be the node's resources with the resource conversion
  // applied.
  expected = Allocation(
      framework.id(),
      {{"role1", {{node.id(), updated.get()}}}});

  allocation = allocations.get();
  AWAIT_EXPECT_EQ(expected, allocation);
}


// This test verifies that `updateAllocation()` supports creating and
// destroying shared persistent volumes.
TEST_F(HierarchicalAllocatorTest, UpdateAllocationSharedPersistentVolume)
{
  Clock::pause();

  initialize();

  NodeInfo node = createNodeInfo("cpus:100;mem:100;disk(role1):100");
  allocator->addNode(
      node.id(),
      node,
      NODE_CAPABILITIES(),
      None(),
      node.resources(),
      {});

  // Initially, all the resources are allocated.
  FrameworkInfo framework = createFrameworkInfo(
      {"role1"},
      {FrameworkInfo::Capability::SHARED_RESOURCES});
  allocator->addFramework(
      framework.id(), framework, hashmap<NodeID, Resources>(), true, {});

  Allocation expected = Allocation(
      framework.id(),
      {{"role1", {{node.id(), node.resources()}}}});

  Future<Allocation> allocation = allocations.get();
  AWAIT_EXPECT_EQ(expected, allocation);

  // Construct an offer operation for the framework's allocation.
  // Create a shared volume.
  Resource volume = createDiskResource(
      "5", "role1", "id1", None(), None(), true);
  volume.mutable_allocation_info()->set_role("role1");

  Offer::Operation create = CREATE(volume);

  Try<vector<ResourceConversion>> conversions = getResourceConversions(create);
  ASSERT_SOME(conversions);

  // Ensure the offer operation can be applied.
  Try<Resources> update =
    allocation->resources.at("role1").at(node.id()).apply(conversions.get());

  ASSERT_SOME(update);

  // Update the allocation in the allocator.
  allocator->updateAllocation(
      framework.id(),
      node.id(),
      allocation->resources.at("role1").at(node.id()),
      conversions.get());

  // Now recover the resources, and expect the next allocation to
  // contain the updated resources.
  allocator->recoverResources(
      framework.id(),
      node.id(),
      update.get(),
      None(),
      false);

  Clock::advance(flags.allocation_interval);

  // The allocation should be the node's resources with the offer
  // operation applied.
  expected = Allocation(
      framework.id(),
      {{"role1", {{node.id(), update.get()}}}});

  allocation = allocations.get();
  AWAIT_EXPECT_EQ(expected, allocation);

  // Construct an offer operation for the framework's allocation to
  // destroy the shared volume.
  Offer::Operation destroy = DESTROY(volume);

  conversions = getResourceConversions(destroy);
  ASSERT_SOME(conversions);

  // Update the allocation in the allocator.
  allocator->updateAllocation(
      framework.id(),
      node.id(),
      allocation->resources.at("role1").at(node.id()),
      conversions.get());

  // The resources to recover should be equal to the node's original
  // resources now that the shared volume is created and then destroyed.
  update = update->apply(conversions.get());
  ASSERT_SOME_EQ(allocatedResources(node.resources(), "role1"), update);

  allocator->recoverResources(
      framework.id(),
      node.id(),
      update.get(),
      None(),
      false);

  Clock::advance(flags.allocation_interval);

  expected = Allocation(
      framework.id(),
      {{"role1", {{node.id(), update.get()}}}});

  allocation = allocations.get();
  AWAIT_EXPECT_EQ(expected, allocation);
}


// Tests that shared resources are only offered to frameworks who have
// opted in for SHARED_RESOURCES.
TEST_F(HierarchicalAllocatorTest, SharedResourcesCapability)
{
  Clock::pause();

  // The gate is cluster policy, off by default (FAC3-0028, DR-036).
  controller::Flags flags = controller::Flags();
  flags.filter_shared_resources = true;
  initialize(flags);

  NodeInfo node = createNodeInfo("cpus:100;mem:100;disk(role1):100");
  allocator->addNode(
      node.id(),
      node,
      NODE_CAPABILITIES(),
      None(),
      node.resources(),
      {});

  // Create `framework1` without opting in for SHARED_RESOURCES.
  FrameworkInfo framework1 = createFrameworkInfo({"role1"});
  allocator->addFramework(framework1.id(), framework1, {}, true, {});

  // Initially, all the resources are allocated to `framework1`.
  Allocation expected = Allocation(
      framework1.id(),
      {{"role1", {{node.id(), node.resources()}}}});

  Future<Allocation> allocation = allocations.get();
  AWAIT_EXPECT_EQ(expected, allocation);

  // Create a shared volume.
  Resource volume = createDiskResource(
      "5", "role1", "id1", None(), None(), true);
  volume.mutable_allocation_info()->set_role("role1");

  Offer::Operation create = CREATE(volume);

  Try<vector<ResourceConversion>> conversions = getResourceConversions(create);
  ASSERT_SOME(conversions);

  // Ensure the offer operation can be applied.
  Try<Resources> update =
    allocation->resources.at("role1").at(node.id()).apply(conversions.get());

  ASSERT_SOME(update);

  // Update the allocation in the allocator.
  allocator->updateAllocation(
      framework1.id(),
      node.id(),
      allocation->resources.at("role1").at(node.id()),
      conversions.get());

  // Now recover the resources, and expect the next allocation to
  // contain the updated resources.
  allocator->recoverResources(
      framework1.id(),
      node.id(),
      update.get(),
      None(),
      false);

  // Shared volume not offered to `framework1` since it has not
  // opted in for SHARED_RESOURCES.
  Clock::advance(flags.allocation_interval);

  expected = Allocation(
      framework1.id(),
      {{"role1", {{node.id(), update.get() - volume}}}});

  allocation = allocations.get();
  AWAIT_EXPECT_EQ(expected, allocation);

  // Recover the resources for the offer in the next allocation cycle.
  allocator->recoverResources(
      framework1.id(),
      node.id(),
      allocation->resources.at("role1").at(node.id()),
      None(),
      false);

  // Create `framework2` with opting in for SHARED_RESOURCES.
  FrameworkInfo framework2 = createFrameworkInfo(
      {"role1"},
      {FrameworkInfo::Capability::SHARED_RESOURCES});
  allocator->addFramework(framework2.id(), framework2, {}, true, {});

  // The offer to 'framework2` should contain the shared volume since it
  // has opted in for SHARED_RESOURCES.
  Clock::advance(flags.allocation_interval);

  expected = Allocation(
      framework2.id(),
      {{"role1", {{node.id(), update.get()}}}});

  allocation = allocations.get();
  AWAIT_EXPECT_EQ(expected, allocation);
}


// This test ensures that a call to 'updateAvailable' succeeds when the
// allocator has sufficient available resources.
TEST_F(HierarchicalAllocatorTest, UpdateAvailableSuccess)
{
  initialize();

  NodeInfo node = createNodeInfo("cpus:100;mem:100;disk:100");
  allocator->addNode(
      node.id(),
      node,
      NODE_CAPABILITIES(),
      None(),
      node.resources(),
      {});

  // Construct an offer operation for the framework's allocation.
  Resources unreserved = Resources::parse("cpus:25;mem:50").get();
  Resources dynamicallyReserved =
    unreserved.pushReservation(createDynamicReservationInfo("role1", "ops"));

  Offer::Operation reserve = RESERVE(dynamicallyReserved);

  Try<Resources> update = Resources(node.resources()).apply(reserve);
  ASSERT_SOME(update);
  EXPECT_NE(Resources(node.resources()), update.get());

  // Update the allocation in the allocator.
  AWAIT_READY(allocator->updateAvailable(node.id(), {reserve}));

  // Expect to receive the updated available resources.
  FrameworkInfo framework = createFrameworkInfo({"role1"});
  allocator->addFramework(framework.id(), framework, {}, true, {});

  // The allocation should be the node's resources with the offer
  // operation applied.
  Allocation expected = Allocation(
      framework.id(),
      {{"role1", {{node.id(), update.get()}}}});

  AWAIT_EXPECT_EQ(expected, allocations.get());
}


// This test ensures that a call to 'updateAvailable' fails when the
// allocator has insufficient available resources.
TEST_F(HierarchicalAllocatorTest, UpdateAvailableFail)
{
  initialize();

  NodeInfo node = createNodeInfo("cpus:100;mem:100;disk:100");
  allocator->addNode(
      node.id(),
      node,
      NODE_CAPABILITIES(),
      None(),
      node.resources(),
      {});

  // Expect to receive the all of the available resources.
  FrameworkInfo framework = createFrameworkInfo({"role1"});
  allocator->addFramework(framework.id(), framework, {}, true, {});

  Allocation expected = Allocation(
      framework.id(),
      {{"role1", {{node.id(), node.resources()}}}});

  AWAIT_EXPECT_EQ(expected, allocations.get());

  // Construct an offer operation for the framework's allocation.
  Resources unreserved = Resources::parse("cpus:25;mem:50").get();
  Resources dynamicallyReserved =
    unreserved.pushReservation(createDynamicReservationInfo("role1", "ops"));

  Offer::Operation reserve = RESERVE(dynamicallyReserved);

  // Update the allocation in the allocator.
  AWAIT_FAILED(allocator->updateAvailable(node.id(), {reserve}));
}


// This test ensures that when oversubscribed resources are updated
// subsequent allocations properly account for that.
TEST_F(HierarchicalAllocatorTest, UpdateNodeOversubscribedResources)
{
  // Pause clock to disable batch allocation.
  Clock::pause();

  initialize();

  NodeInfo node = createNodeInfo("cpus:100;mem:100;disk:100");
  allocator->addNode(
      node.id(),
      node,
      NODE_CAPABILITIES(),
      None(),
      node.resources(),
      {});

  // Add a framework that can accept revocable resources.
  FrameworkInfo framework = createFrameworkInfo(
      {"role1"},
      {FrameworkInfo::Capability::REVOCABLE_RESOURCES});
  allocator->addFramework(framework.id(), framework, {}, true, {});

  // Initially, all the resources are allocated.
  Allocation expected = Allocation(
      framework.id(),
      {{"role1", {{node.id(), node.resources()}}}});

  AWAIT_EXPECT_EQ(expected, allocations.get());

  // Update the node with 10 oversubscribed cpus.
  Resources oversubscribed = createRevocableResources("cpus", "10");
  allocator->updateNode(node.id(), node, node.resources() + oversubscribed);

  // The next allocation should be for 10 oversubscribed resources.
  expected = Allocation(
      framework.id(),
      {{"role1", {{node.id(), oversubscribed}}}});

  AWAIT_EXPECT_EQ(expected, allocations.get());

  // Update the node again with 12 oversubscribed cpus.
  Resources oversubscribed2 = createRevocableResources("cpus", "12");
  allocator->updateNode(
      node.id(), node, node.resources() + oversubscribed2);

  // The next allocation should be for 2 oversubscribed cpus.
  expected = Allocation(
      framework.id(),
      {{"role1", {{node.id(), oversubscribed2 - oversubscribed}}}});

  AWAIT_EXPECT_EQ(expected, allocations.get());

  // Update the node again with 5 oversubscribed cpus.
  Resources oversubscribed3 = createRevocableResources("cpus", "5");
  allocator->updateNode(
      node.id(), node, node.resources() + oversubscribed3);

  // Since there are no more available oversubscribed resources there
  // shouldn't be an allocation.
  Clock::settle();

  Future<Allocation> allocation = allocations.get();
  EXPECT_TRUE(allocation.isPending());
}


// This test ensures that we can update the total of a node. We
// check that we can expand and shrink the resources available on an
// node. Nodes can be overallocated, meaning the amount of allocated
// resources can exceed the total available resources.
TEST_F(HierarchicalAllocatorTest, UpdateNodeTotalResources)
{
  // Pause clock to disable batch allocation.
  Clock::pause();

  initialize();

  // Create a node and a framework. This triggers allocation
  // of the node's resources to the framework.
  const NodeInfo node = createNodeInfo("cpus:100;mem:100;disk:100");

  allocator->addNode(
      node.id(),
      node,
      NODE_CAPABILITIES(),
      None(),
      node.resources(),
      {});

  const FrameworkInfo framework = createFrameworkInfo({"role1"});
  allocator->addFramework(framework.id(), framework, {}, true, {});

  const Allocation expected1 = Allocation(
      framework.id(),
      {{"role1", {{node.id(), node.resources()}}}});

  AWAIT_EXPECT_EQ(expected1, allocations.get());

  // Increase the node's total. The additional
  // resources will be offered to the framework.
  const Resources addedResources = Resources::parse("cpus:12").get();

  allocator->updateNode(
      node.id(),
      node,
      node.resources() + addedResources);

  const Allocation expected2 = Allocation(
      framework.id(),
      {{"role1", {{node.id(), addedResources}}}});

  Future<Allocation> allocation = allocations.get();
  AWAIT_EXPECT_EQ(expected2, allocation);

  // Decrease the node's total to half its original value. The allocated now
  // exceeds to total; nothing will be offered due to this operation.
  const Resources nodeResources2 =
    Resources::parse("cpus:50;mem:50;disk:50").get();

  allocator->updateNode(node.id(), node, nodeResources2);

  // Recover all node resources allocated to the framework in the last two
  // allocations. We will subsequently be offered the complete node which has
  // `nodeResources2` resources.
  allocator->recoverResources(
      framework.id(),
      node.id(),
      expected1.resources.at("role1").at(node.id()) +
        expected2.resources.at("role1").at(node.id()),
      None(),
      false);

  // Advance the clock to trigger allocation of
  // the available `nodeResources2` resources.
  Clock::advance(flags.allocation_interval);

  const Allocation expected3 = Allocation(
      framework.id(),
      {{"role1", {{node.id(), nodeResources2}}}});

  AWAIT_EXPECT_EQ(expected3, allocations.get());

  // Set the node's total resources to its original value. This will trigger
  // allocation of the newly added `nodeResources2` resources now available on
  // the node.
  allocator->updateNode(node.id(), node, node.resources());

  const Allocation expected4 = Allocation(
      framework.id(),
      {{"role1", {{node.id(), nodeResources2}}}});

  AWAIT_EXPECT_EQ(expected4, allocations.get());
}


// This test ensures that when node capabilities are updated
// subsequent allocations properly account for that.
TEST_F(HierarchicalAllocatorTest, UpdateNodeCapabilities)
{
  // Pause clock to disable batch allocation.
  Clock::pause();

  initialize();

  NodeInfo node = createNodeInfo("cpus:1;mem:1;disk:1");
  allocator->addNode(
      node.id(),
      node,
      {},
      None(),
      node.resources(),
      {});

  // Add a MULTI_ROLE framework. We explicitly check the capability here
  // in case `createFrameworkInfo` helper changes in the future.
  FrameworkInfo framework = createFrameworkInfo({"role1"});
  EXPECT_EQ(1, framework.capabilities_size());
  EXPECT_EQ(FrameworkInfo::Capability::MULTI_ROLE,
            framework.capabilities().begin()->type());
  allocator->addFramework(framework.id(), framework, {}, true, {});

  // Initially, we do not expect any allocation since non-MULTI_ROLE node
  // should not be allocated to MULTI_ROLE framework.
  Clock::settle();

  Future<Allocation> allocation = allocations.get();
  ASSERT_TRUE(allocation.isPending());

  // Update the node to be MULTI_ROLE capable.

  allocator->updateNode(node.id(), node, None(), NODE_CAPABILITIES());

  Clock::settle();

  // Resources of node should be allocated to framework now.
  Allocation expected = Allocation(
      framework.id(),
      {{"role1", {{node.id(), node.resources()}}}});

  AWAIT_EXPECT_EQ(expected, allocation);
}


// This is a regression test for MESOS-621. It ensures that when removing
// a node, its resources are fully recovered.
TEST_F(HierarchicalAllocatorTest, RemoveNodeRecoverResources)
{
  // Per MESOS-621, removing a node was a two step process.
  // Resources in the Node struct is immediately recovered.
  // But states in the role tree or the sorter would require
  // an additional recoverResources call.
  // We verify the fix by:
  //  - Allocating a larger node1 to framework1
  //  - Allocating a smaller node2 to framework2
  //  - Remove node1 (without calling recoverResources)
  //  - Add node3
  //
  // Node3 should be allocated to framework1 since it has no resources
  // (verfiying that the sorter states are correctly updated
  // after the node1 is removed).

  Clock::pause();

  initialize();

  NodeInfo node1 = createNodeInfo("cpus:2;mem:200");
  allocator->addNode(
      node1.id(),
      node1,
      NODE_CAPABILITIES(),
      None(),
      node1.resources(),
      {});

  FrameworkInfo framework1 = createFrameworkInfo({"role"});
  allocator->addFramework(framework1.id(), framework1, {}, true, {});

  Allocation expected = Allocation(
      framework1.id(), {{"role", {{node1.id(), node1.resources()}}}});

  AWAIT_EXPECT_EQ(expected, allocations.get());

  FrameworkInfo framework2 = createFrameworkInfo({"role"});
  allocator->addFramework(framework2.id(), framework2, {}, true, {});

  NodeInfo node2 = createNodeInfo("cpus:1;mem:100");
  allocator->addNode(
      node2.id(),
      node2,
      NODE_CAPABILITIES(),
      None(),
      node2.resources(),
      {});

  expected = Allocation(
      framework2.id(), {{"role", {{node2.id(), node2.resources()}}}});

  AWAIT_EXPECT_EQ(expected, allocations.get());

  allocator->removeNode(node1.id());

  NodeInfo node3 = createNodeInfo("cpus:1;mem:100");
  allocator->addNode(
      node3.id(),
      node3,
      NODE_CAPABILITIES(),
      None(),
      node3.resources(),
      {});

  expected = Allocation(
      framework1.id(), {{"role", {{node3.id(), node3.resources()}}}});

  AWAIT_EXPECT_EQ(expected, allocations.get());
}


// This is a white-box test to ensure that MESOS-9554 is fixed.
// It ensures that if a framework is not capable of receiving
// any resources on a node, we still proceed to try allocating
// those resources to other frameworks (previously, the loop
// exited incorrectly in this case). This is done through the
// RESERVATION_REFINEMENT capability.
TEST_F(HierarchicalAllocatorTest, FrameworkLoopMEDIOS_9554)
{
  // Pause clock to disable batch allocation.
  Clock::pause();

  initialize();

  // First, we add a framework and node and ensure the
  // resources get allocated. This makes framework 1 have
  // a non-zero share.
  FrameworkInfo framework1 = createFrameworkInfo(
      {"parent/child"},
      {FrameworkInfo::Capability::RESERVATION_REFINEMENT});

  allocator->addFramework(framework1.id(), framework1, {}, true, {});

  NodeInfo node1 = createNodeInfo("cpus:1;mem:1;disk:1");

  allocator->addNode(
      node1.id(),
      node1,
      NODE_CAPABILITIES(),
      None(),
      node1.resources(),
      {});

  Allocation expected = Allocation(
      framework1.id(),
      {{"parent/child", {{node1.id(), node1.resources()}}}});

  AWAIT_EXPECT_EQ(expected, allocations.get());

  // Now, we add a framework that is not RESERVATION_REFINEMENT
  // capable, it will have a lower share of 0 in the same role.
  FrameworkInfo framework2 = createFrameworkInfo({"parent/child"});

  // We explicitly check to ensure the RESERVATION_REFINEMENT
  // capability is absent, in case the `createFrameworkInfo`
  // helper changes in the future.
  EXPECT_EQ(1, framework2.capabilities_size());
  ASSERT_EQ(FrameworkInfo::Capability::MULTI_ROLE,
            framework2.capabilities().begin()->type());

  allocator->addFramework(framework2.id(), framework2, {}, true, {});

  // There is nothing to be allocated.
  Clock::settle();
  Future<Allocation> allocation = allocations.get();
  ASSERT_TRUE(allocation.isPending());

  // Here's the meat of this test. We'll add a second node with
  // refined reservations. The lower share framework2 is not capable
  // of being offered any resources on this node, and they should
  // be correctly sent to framework1 instead.
  Resources resources =
    CHECK_NOTERROR(Resources::parse("cpus:1;mem:1;disk:1"))
      .pushReservation(createDynamicReservationInfo("parent"))
      .pushReservation(createDynamicReservationInfo("parent/child"));

  foreach (const Resource& r, resources) {
    ASSERT_TRUE(Resources::hasRefinedReservations(r));
  }

  NodeInfo node2 = createNodeInfo(resources);

  allocator->addNode(
      node2.id(),
      node2,
      NODE_CAPABILITIES(),
      None(),
      node2.resources(),
      {});

  expected = Allocation(
      framework1.id(),
      {{"parent/child", {{node2.id(), node2.resources()}}}});

  AWAIT_EXPECT_EQ(expected, allocation);
}


// This is a regression test for MESOS-9555 that ensures that
// the tracking of non-scalar reservations across nodes does
// not lead to a CHECK failure.
TEST_F(HierarchicalAllocatorTest, NonScalarReservationTrackingMEDIOS_9555)
{
  // Pause clock to disable batch allocation.
  Clock::pause();

  initialize();

  // Have only non-scalar (ports) reserved for a role.
  Resources resources = CHECK_NOTERROR(Resources::parse(
      "cpus:1;mem:1;disk:1;ports(role):[1-2]"));

  NodeInfo node1 = createNodeInfo(resources);

  allocator->addNode(
      node1.id(),
      node1,
      NODE_CAPABILITIES(),
      None(),
      node1.resources(),
      {});

  NodeInfo node2 = createNodeInfo(resources);

  allocator->addNode(
      node2.id(),
      node2,
      NODE_CAPABILITIES(),
      None(),
      node2.resources(),
      {});

  allocator->removeNode(node1.id());
  allocator->removeNode(node2.id());

  Clock::settle();
}


// This test verifies that a framework that has not opted in for
// revocable resources do not get allocated oversubscribed resources.
TEST_F(HierarchicalAllocatorTest, OversubscribedNotAllocated)
{
  // Pause clock to disable batch allocation.
  Clock::pause();

  initialize();

  NodeInfo node = createNodeInfo("cpus:100;mem:100;disk:100");
  allocator->addNode(
      node.id(),
      node,
      NODE_CAPABILITIES(),
      None(),
      node.resources(),
      {});

  // Add a framework that does *not* accept revocable resources.
  FrameworkInfo framework = createFrameworkInfo({"role1"});
  allocator->addFramework(framework.id(), framework, {}, true, {});

  // Initially, all the resources are allocated.
  Allocation expected = Allocation(
      framework.id(),
      {{"role1", {{node.id(), node.resources()}}}});

  AWAIT_EXPECT_EQ(expected, allocations.get());

  // Update the node with 10 oversubscribed cpus.
  Resources oversubscribed = createRevocableResources("cpus", "10");
  allocator->updateNode(node.id(), node, node.resources() + oversubscribed);

  // No allocation should be made for oversubscribed resources because
  // the framework has not opted in for them.
  Clock::settle();

  Future<Allocation> allocation = allocations.get();
  EXPECT_TRUE(allocation.isPending());
}


// This test verifies that when oversubscribed resources are partially
// recovered subsequent allocation properly accounts for that.
TEST_F(HierarchicalAllocatorTest, RecoverOversubscribedResources)
{
  // Pause clock to disable batch allocation.
  Clock::pause();

  initialize();

  NodeInfo node = createNodeInfo("cpus:100;mem:100;disk:100");
  allocator->addNode(
      node.id(),
      node,
      NODE_CAPABILITIES(),
      None(),
      node.resources(),
      {});

  // Add a framework that can accept revocable resources.
  FrameworkInfo framework = createFrameworkInfo(
      {"role1"},
      {FrameworkInfo::Capability::REVOCABLE_RESOURCES});
  allocator->addFramework(framework.id(), framework, {}, true, {});

  // Initially, all the resources are allocated.
  Allocation expected = Allocation(
      framework.id(),
      {{"role1", {{node.id(), node.resources()}}}});

  AWAIT_EXPECT_EQ(expected, allocations.get());

  // Update the node with 10 oversubscribed cpus.
  Resources oversubscribed = createRevocableResources("cpus", "10");
  allocator->updateNode(node.id(), node, node.resources() + oversubscribed);

  // The next allocation should be for 10 oversubscribed cpus.
  expected = Allocation(
      framework.id(),
      {{"role1", {{node.id(), oversubscribed}}}});

  AWAIT_EXPECT_EQ(expected, allocations.get());

  // Recover 6 oversubscribed cpus and 2 regular cpus.
  Resources recovered = createRevocableResources("cpus", "6");
  recovered += Resources::parse("cpus:2").get();
  recovered.allocate("role1");

  allocator->recoverResources(
      framework.id(), node.id(), recovered, None(), false);

  Clock::advance(flags.allocation_interval);

  // The next allocation should be for 6 oversubscribed and 2 regular
  // cpus.
  expected = Allocation(
      framework.id(),
      {{"role1", {{node.id(), recovered}}}});

  AWAIT_EXPECT_EQ(expected, allocations.get());
}


// Checks that a node that is not allowlisted will not have its
// resources get offered, and that if the allowlist is updated so
// that it is allowlisted, its resources will then be offered.
TEST_F(HierarchicalAllocatorTest, Allowlist)
{
  Clock::pause();

  initialize();

  hashset<string> allowlist;
  allowlist.insert("dummy-node");

  allocator->updateAllowlist(allowlist);

  NodeInfo node = createNodeInfo("cpus:2;mem:1024");
  allocator->addNode(
      node.id(),
      node,
      NODE_CAPABILITIES(),
      None(),
      node.resources(),
      {});

  FrameworkInfo framework = createFrameworkInfo({"*"});
  allocator->addFramework(framework.id(), framework, {}, true, {});

  Future<Allocation> allocation = allocations.get();

  // Ensure a batch allocation is triggered.
  Clock::advance(flags.allocation_interval);
  Clock::settle();

  // There should be no allocation!
  ASSERT_TRUE(allocation.isPending());

  // Updating the allowlist to include the node should
  // trigger an allocation in the next batch.
  allowlist.insert(node.hostname());
  allocator->updateAllowlist(allowlist);

  Clock::advance(flags.allocation_interval);

  Allocation expected = Allocation(
      framework.id(),
      {{"*", {{node.id(), node.resources()}}}});

  AWAIT_EXPECT_EQ(expected, allocation);
}


// This test checks that the order in which `addFramework()` and `addNode()`
// are called does not influence the bookkeeping. We start with two frameworks
// with identical allocations, but we update the allocator in different order
// for each framework. We expect the fair shares of the frameworks to be
// identical, which we implicitly check by subsequent allocations.
TEST_F(HierarchicalAllocatorTest, NoDoubleAccounting)
{
  // Pausing the clock is not necessary, but ensures that the test
  // doesn't rely on the batch allocation in the allocator, which
  // would slow down the test.
  Clock::pause();

  const string nodeResources{"cpus:1;mem:0;disk:0"};

  initialize();

  // Start with two identical nodes and two frameworks,
  // each having one node allocated to it.
  NodeInfo node1 = createNodeInfo(nodeResources);
  NodeInfo node2 = createNodeInfo(nodeResources);

  const string ROLE1 = "ROLE1";
  FrameworkInfo framework1 = createFrameworkInfo({ROLE1});

  const string ROLE2 = "ROLE2";
  FrameworkInfo framework2 = createFrameworkInfo({ROLE2});

  hashmap<FrameworkID, Resources> node1Allocation =
    {{framework1.id(), allocatedResources(node1.resources(), ROLE1)}};
  hashmap<FrameworkID, Resources> node2Allocation =
    {{framework2.id(), allocatedResources(node2.resources(), ROLE2)}};

  hashmap<NodeID, Resources> framework1Allocation =
    {{node1.id(), allocatedResources(node1.resources(), ROLE1)}};
  hashmap<NodeID, Resources> framework2Allocation =
    {{node2.id(), allocatedResources(node2.resources(), ROLE2)}};

  // Call `addFramework()` and `addNode()` in different order for
  // `framework1` and `framework2`
  allocator->addFramework(
      framework1.id(), framework1, framework1Allocation, true, {});

  allocator->addNode(
      node1.id(),
      node1,
      NODE_CAPABILITIES(),
      None(),
      node1.resources(),
      node1Allocation);

  allocator->addNode(
      node2.id(),
      node2,
      NODE_CAPABILITIES(),
      None(),
      node2.resources(),
      node2Allocation);

  allocator->addFramework(
      framework2.id(), framework2, framework2Allocation, true, {});

  // Process all triggered allocation events.
  Clock::settle();

  // Total cluster resources (2 identical nodes): cpus=2, mem=1024.
  // ROLE1 share = 0.5
  //   framework1 share = 1
  // ROLE2 share = 0.5
  //   framework2 share = 1

  // We expect the frameworks to have identical resource allocations and
  // hence identical dominant shares.
  JSON::Object metrics = Metrics();
  string metric1 = "allocator/medios/roles/" + ROLE1 + "/shares/dominant";
  string metric2 = "allocator/medios/roles/" + ROLE2 + "/shares/dominant";

  double share1 = metrics.values[metric1].as<JSON::Number>().as<double>();
  double share2 = metrics.values[metric2].as<JSON::Number>().as<double>();
  EXPECT_DOUBLE_EQ(share1, share2);
}


// The quota tests that are specific to the built-in Hierarchical DRF
// allocator (i.e. the way quota is satisfied) are in this file.

// TODO(alexr): Additional tests we may want to implement:
//   * A role has running tasks, quota is being set and is less than the
//     current allocation, some tasks finish or are killed, but the role
//     does not get new non-revocable offers (retroactively).
//   * Multiple frameworks in a role with quota set, some nodes fail,
//     frameworks should be deprived fairly.
//   * Multiple quota'ed roles, some nodes fail, roles should be deprived
//     according to their weights.
//   * Oversubscribed resources should not count towards quota.
//   * A role has dynamic reservations, quota is set and is less than total
//     dynamic reservations.
//   * A role has dynamic reservations, quota is set and is greater than
//     total dynamic reservations. Resource math should account them towards
//     quota and do not offer extra resources, offer dynamically reserved
//     resources as part of quota and do not re-offer them afterwards.

// In the presence of quota'ed and non-quota'ed roles, if a framework in
// the quota'ed role declines offers, some resources are laid away for
// the role, so that a greedy framework from a non-quota'ed role cannot
// eat up all free resources.
TEST_F(HierarchicalAllocatorTest, QuotaProvidesGuarantee)
{
  // Pausing the clock is not necessary, but ensures that the test
  // doesn't rely on the batch allocation in the allocator, which
  // would slow down the test.
  Clock::pause();

  const string QUOTA_ROLE{"quota-role"};
  const string NO_QUOTA_ROLE{"no-quota-role"};

  initialize();

  // Create `framework1` and set quota for its role.
  FrameworkInfo framework1 = createFrameworkInfo({QUOTA_ROLE});
  allocator->addFramework(framework1.id(), framework1, {}, true, {});

  const Quota quota = createQuota("cpus:2;mem:1024");
  allocator->updateQuota(QUOTA_ROLE, quota);

  // Create `framework2` in a non-quota'ed role.
  FrameworkInfo framework2 = createFrameworkInfo({NO_QUOTA_ROLE});
  allocator->addFramework(framework2.id(), framework2, {}, true, {});

  // Process all triggered allocation events.
  //
  // NOTE: No allocations happen because there are no resources to allocate.
  Clock::settle();

  NodeInfo node1 = createNodeInfo("cpus:1;mem:512;disk:0");
  allocator->addNode(
      node1.id(),
      node1,
      NODE_CAPABILITIES(),
      None(),
      node1.resources(),
      {});

  // `framework1` will be offered all of `node1`'s resources because it is
  // the only framework in the only role with unsatisfied quota.
  Allocation expected = Allocation(
      framework1.id(),
      {{QUOTA_ROLE, {{node1.id(), node1.resources()}}}});

  AWAIT_EXPECT_EQ(expected, allocations.get());

  // Total cluster resources: cpus=1, mem=512.
  // QUOTA_ROLE share = 1 (cpus=1, mem=512) [quota: cpus=2, mem=1024]
  //   framework1 share = 1
  // NO_QUOTA_ROLE share = 0
  //   framework2 share = 0

  NodeInfo node2 = createNodeInfo("cpus:1;mem:512;disk:0");
  allocator->addNode(
      node2.id(),
      node2,
      NODE_CAPABILITIES(),
      None(),
      node2.resources(),
      {});

  // `framework1` will again be offered all of `node2`'s resources
  // because it is the only framework in the only role with unsatisfied
  // quota. `framework2` has to wait.
  expected = Allocation(
      framework1.id(),
      {{QUOTA_ROLE, {{node2.id(), node2.resources()}}}});

  Future<Allocation> allocation = allocations.get();
  AWAIT_EXPECT_EQ(expected, allocation);

  // Total cluster resources: cpus=2, mem=1024.
  // QUOTA_ROLE share = 1 (cpus=2, mem=1024) [quota: cpus=2, mem=1024]
  //   framework1 share = 1
  // NO_QUOTA_ROLE share = 0
  //   framework2 share = 0

  // Now `framework1` declines the second offer and sets a filter for twice
  // the allocation interval. The declined resources should not be offered
  // to `framework2` because by doing so they may not be available to
  // `framework1` when the filter expires.
  Duration filterTimeout = flags.allocation_interval * 2;
  Filters offerFilter;
  offerFilter.set_refuse_seconds(filterTimeout.secs());

  allocator->recoverResources(
      framework1.id(),
      node2.id(),
      allocation->resources.at(QUOTA_ROLE).at(node2.id()),
      offerFilter,
      false);

  // Total cluster resources: cpus=2, mem=1024.
  // QUOTA_ROLE share = 0.5 (cpus=1, mem=512) [quota: cpus=2, mem=1024]
  //   framework1 share = 1
  // NO_QUOTA_ROLE share = 0
  //   framework2 share = 0

  // Ensure the offer filter timeout is set before advancing the clock.
  Clock::settle();

  // Trigger a batch allocation.
  Clock::advance(flags.allocation_interval);
  Clock::settle();

  // There should be no allocation due to the offer filter.
  allocation = allocations.get();
  EXPECT_TRUE(allocation.isPending());

  // Ensure the offer filter times out (2x the allocation interval)
  // and the next batch allocation occurs.
  Clock::advance(flags.allocation_interval);

  // Previously declined resources should be offered to the quota'ed role.
  expected = Allocation(
      framework1.id(),
      {{QUOTA_ROLE, {{node2.id(), node2.resources()}}}});

  AWAIT_EXPECT_EQ(expected, allocation);

  // Total cluster resources: cpus=2, mem=1024.
  // QUOTA_ROLE share = 1 (cpus=2, mem=1024) [quota: cpus=2, mem=1024]
  //   framework1 share = 1
  // NO_QUOTA_ROLE share = 0
  //   framework2 share = 0
}


// When a role has limits set, its frameworks allocations are restricted based
// on its quota limits.
// We set quota on the default "*" role as a regression test for MESOS-3938.
TEST_F(HierarchicalAllocatorTest, QuotaProvidesLimit)
{
  Clock::pause();

  const string QUOTA_ROLE{"*"};
  const string NO_QUOTA_ROLE{"no-quota-role"};

  initialize();

  // Create `framework1` and set quota for its role.
  FrameworkInfo framework1 = createFrameworkInfo({QUOTA_ROLE});
  allocator->addFramework(framework1.id(), framework1, {}, true, {});

  // Set quota with no guarantees and zero limit.
  allocator->updateQuota(QUOTA_ROLE, createQuota("", "cpus:0;mem:0;disk:0"));

  // Add a node, this will trigger an event-driven allocation.
  NodeInfo node1 = createNodeInfo("cpus:2;mem:1024;disk:1024");
  allocator->addNode(
      node1.id(),
      node1,
      NODE_CAPABILITIES(),
      None(),
      node1.resources(),
      {});

  // Make sure the node is added.
  Clock::settle();

  // There should be no allocation due to `QUOTA_ROLE` quota limits.
  Future<Allocation> allocation = allocations.get();
  EXPECT_TRUE(allocation.isPending());

  // Create `framework2` under `NO_QUOTA_ROLE`.
  // This will trigger an event-driven allocation.
  FrameworkInfo framework2 = createFrameworkInfo({NO_QUOTA_ROLE});
  allocator->addFramework(framework2.id(), framework2, {}, true, {});

  // All resources of `node1` will be offered to `framework2`.
  Allocation expected = Allocation(
      framework2.id(), {{NO_QUOTA_ROLE, {{node1.id(), node1.resources()}}}});

  AWAIT_EXPECT_EQ(expected, allocation);

  // Now raise the limit from 0 to "cpus:1;mem:512;disk:512" which should
  // make the role get resources up to this limits.
  allocator->updateQuota(
      QUOTA_ROLE, createQuota("", "cpus:1;mem:512;disk:512"));

  // Add a 2nd node with same resources.
  // This will trigger an event-driven allocation.
  NodeInfo node2 = createNodeInfo("cpus:2;mem:1024;disk:1024");
  allocator->addNode(
      node2.id(),
      node2,
      NODE_CAPABILITIES(),
      None(),
      node2.resources(),
      {});

  // Half of `node2` resources will be offered to framework1
  // which is restricted by its role's quota limits.
  // The rest of the resources will be "chopped" and
  // offered to framework2.
  Resources offered =
    CHECK_NOTERROR(Resources::parse("cpus:1;mem:512;disk:512"));
  Allocation expected1 =
    Allocation(framework1.id(), {{QUOTA_ROLE, {{node2.id(), offered}}}});
  Allocation expected2 =
    Allocation(framework2.id(), {{NO_QUOTA_ROLE, {{node2.id(), offered}}}});

  Future<Allocation> allocation1 = allocations.get();
  Future<Allocation> allocation2 = allocations.get();

  AWAIT_READY(allocation1);
  AWAIT_READY(allocation2);

  // While currently allocation1 is always allocated to `framework2`.
  // This order may be affected by many factors in between.
  // Since we only care about framework1 and framework2 each gets half
  // the resources, we do a swap here if necessary.
  if (allocation1->frameworkId != framework1.id()) {
    std::swap(allocation1, allocation2);
  }

  EXPECT_EQ(allocation1.get(), expected1);
  EXPECT_EQ(allocation2.get(), expected2);
}


// If quota is removed, fair sharing should be restored in the cluster
// after sufficient number of tasks finish.
TEST_F(HierarchicalAllocatorTest, RemoveQuota)
{
  // Pausing the clock is not necessary, but ensures that the test
  // doesn't rely on the batch allocation in the allocator, which
  // would slow down the test.
  Clock::pause();

  const string QUOTA_ROLE{"quota-role"};
  const string NO_QUOTA_ROLE{"no-quota-role"};

  initialize();

  const Quota quota = createQuota("cpus:2;mem:1024");
  allocator->updateQuota(QUOTA_ROLE, quota);

  FrameworkInfo framework1 = createFrameworkInfo({QUOTA_ROLE});
  allocator->addFramework(framework1.id(), framework1, {}, true, {});

  FrameworkInfo framework2 = createFrameworkInfo({NO_QUOTA_ROLE});
  allocator->addFramework(framework2.id(), framework2, {}, true, {});

  NodeInfo node1 = createNodeInfo("cpus:1;mem:512;disk:0");
  allocator->addNode(
      node1.id(),
      node1,
      NODE_CAPABILITIES(),
      None(),
      node1.resources(),
      {{framework1.id(), allocatedResources(node1.resources(), QUOTA_ROLE)}});

  NodeInfo node2 = createNodeInfo("cpus:1;mem:512;disk:0");
  allocator->addNode(
      node2.id(),
      node2,
      NODE_CAPABILITIES(),
      None(),
      node2.resources(),
      {{framework1.id(), allocatedResources(node2.resources(), QUOTA_ROLE)}});

  // Total cluster resources (2 identical nodes): cpus=2, mem=1024.
  // QUOTA_ROLE share = 1 (cpus=2, mem=1024) [quota: cpus=2, mem=1024]
  //   framework1 share = 1
  // NO_QUOTA_ROLE share = 0
  //   framework2 share = 0

  // All cluster resources are now being used by `framework1` as part of
  // its role quota, no further allocations are expected. However, once the
  // quota is removed, quota guarantee does not apply any more and released
  // resources should be offered to `framework2` to restore fairness.

  allocator->updateQuota(QUOTA_ROLE, controller::DEFAULT_QUOTA);

  // Process all triggered allocation events.
  //
  // NOTE: No allocations happen because there are no resources to allocate.
  Clock::settle();

  allocator->recoverResources(
      framework1.id(),
      node1.id(),
      allocatedResources(node1.resources(), QUOTA_ROLE),
      None(),
      false);

  // Trigger the next batch allocation.
  Clock::advance(flags.allocation_interval);

  Allocation expected = Allocation(
      framework2.id(),
      {{NO_QUOTA_ROLE, {{node1.id(), node1.resources()}}}});

  AWAIT_EXPECT_EQ(expected, allocations.get());

  // Total cluster resources: cpus=2, mem=1024.
  // QUOTA_ROLE share = 0.5 (cpus=1, mem=512)
  //   framework1 share = 1
  // NO_QUOTA_ROLE share = 0.5 (cpus=1, mem=512)
  //   framework2 share = 1

  JSON::Object metrics = Metrics();

  string metric =
    "allocator/medios/quota"
    "/roles/" + QUOTA_ROLE +
    "/resources/cpus"
    "/offered_or_allocated";
  EXPECT_EQ(0u, metrics.values.count(metric));

  metric =
    "allocator/medios/quota"
    "/roles/" + QUOTA_ROLE +
    "/resources/mem"
    "/offered_or_allocated";
  EXPECT_EQ(0u, metrics.values.count(metric));
}


// If a quota'ed role contains multiple frameworks, the resources should
// be distributed fairly between them. However, inside the quota'ed role,
// if one framework declines resources, there is no guarantee the other
// framework in the same role does not consume all role's quota.
TEST_F(HierarchicalAllocatorTest, MultipleFrameworksInRoleWithQuota)
{
  // Pausing the clock is not necessary, but ensures that the test
  // doesn't rely on the batch allocation in the allocator, which
  // would slow down the test.
  Clock::pause();

  const string QUOTA_ROLE{"quota-role"};
  const string NO_QUOTA_ROLE{"no-quota-role"};

  initialize();

  // Create `framework1a` and set quota for its role.
  FrameworkInfo framework1a = createFrameworkInfo({QUOTA_ROLE});
  allocator->addFramework(framework1a.id(), framework1a, {}, true, {});

  const Quota quota = createQuota("cpus:4;mem:2048");
  allocator->updateQuota(QUOTA_ROLE, quota);

  // Create `framework2` in a non-quota'ed role.
  FrameworkInfo framework2 = createFrameworkInfo({NO_QUOTA_ROLE});
  allocator->addFramework(framework2.id(), framework2, {}, true, {});

  // Process all triggered allocation events.
  //
  // NOTE: No allocations happen because there are no resources to allocate.
  Clock::settle();

  NodeInfo node1 = createNodeInfo("cpus:1;mem:512;disk:0");
  allocator->addNode(
      node1.id(),
      node1,
      NODE_CAPABILITIES(),
      None(),
      node1.resources(),
      {});

  // `framework1a` will be offered all of `node1`'s resources because
  // it is the only framework in the only role with unsatisfied quota.
  Allocation expected = Allocation(
      framework1a.id(),
      {{QUOTA_ROLE, {{node1.id(), node1.resources()}}}});

  AWAIT_EXPECT_EQ(expected, allocations.get());

  // Total cluster resources: cpus=1, mem=512.
  // QUOTA_ROLE share = 1 (cpus=1, mem=512) [quota: cpus=2, mem=1024]
  //   framework1a share = 1
  // NO_QUOTA_ROLE share = 0
  //   framework2 share = 0

  // Create `framework1b` in the quota'ed role.
  FrameworkInfo framework1b = createFrameworkInfo({QUOTA_ROLE});
  allocator->addFramework(framework1b.id(), framework1b, {}, true, {});

  NodeInfo node2 = createNodeInfo("cpus:2;mem:1024;disk:0");
  allocator->addNode(
      node2.id(),
      node2,
      NODE_CAPABILITIES(),
      None(),
      node2.resources(),
      {});

  // `framework1b` will be offered all of `node2`'s resources
  // (coarse-grained allocation) because its share is 0 and it belongs
  // to a role with unsatisfied quota.
  expected = Allocation(
      framework1b.id(),
      {{QUOTA_ROLE, {{node2.id(), node2.resources()}}}});

  AWAIT_EXPECT_EQ(expected, allocations.get());

  // Total cluster resources: cpus=3, mem=1536.
  // QUOTA_ROLE share = 1 (cpus=3, mem=1536) [quota: cpus=4, mem=2048]
  //   framework1a share = 0.33 (cpus=1, mem=512)
  //   framework1b share = 0.66 (cpus=2, mem=1024)
  // NO_QUOTA_ROLE share = 0
  //   framework2 share = 0

  NodeInfo node3 = createNodeInfo("cpus:1;mem:512;disk:0");
  allocator->addNode(
      node3.id(),
      node3,
      NODE_CAPABILITIES(),
      None(),
      node3.resources(),
      {});

  // `framework1a` will be offered all of `node3`'s resources because
  // its share is less than `framework1b`'s and `QUOTA_ROLE` still
  // has unsatisfied quota.
  expected = Allocation(
      framework1a.id(),
      {{QUOTA_ROLE, {{node3.id(), node3.resources()}}}});

  AWAIT_EXPECT_EQ(expected, allocations.get());

  // Total cluster resources: cpus=4, mem=2048.
  // QUOTA_ROLE share = 1 (cpus=4, mem=2048) [quota: cpus=4, mem=2048]
  //   framework1a share = 0.5 (cpus=2, mem=1024)
  //   framework1b share = 0.5 (cpus=2, mem=1024)
  // NO_QUOTA_ROLE share = 0
  //   framework2 share = 0

  // If `framework1a` declines offered resources, they will be allocated to
  // `framework1b`.
  Filters filter5s;
  filter5s.set_refuse_seconds(5.);
  allocator->recoverResources(
      framework1a.id(),
      node3.id(),
      allocatedResources(node3.resources(), QUOTA_ROLE),
      filter5s,
      false);

  // Trigger the next batch allocation.
  Clock::advance(flags.allocation_interval);

  expected = Allocation(
      framework1b.id(),
      {{QUOTA_ROLE, {{node3.id(), node3.resources()}}}});

  AWAIT_EXPECT_EQ(expected, allocations.get());

  // Total cluster resources: cpus=4, mem=2048.
  // QUOTA_ROLE share = 1 (cpus=4, mem=2048) [quota: cpus=4, mem=2048]
  //   framework1a share = 0.25 (cpus=1, mem=512)
  //   framework1b share = 0.75 (cpus=3, mem=1536)
  // NO_QUOTA_ROLE share = 0
  //   framework2 share = 0
}


// Quota allocations should be fine-grained. A role should get no more
// resources than its quota even if the node has more resources to offer.
TEST_F(HierarchicalAllocatorTest, QuotaAllocationGranularity)
{
  // Pausing the clock is not necessary, but ensures that the test
  // doesn't rely on the batch allocation in the allocator, which
  // would slow down the test.
  Clock::pause();

  const string QUOTA_ROLE{"quota-role"};
  const string NO_QUOTA_ROLE{"no-quota-role"};

  initialize();

  // Create `framework1` and set quota for its role.
  FrameworkInfo framework1 = createFrameworkInfo({QUOTA_ROLE});
  allocator->addFramework(framework1.id(), framework1, {}, true, {});

  // Set quota to be less than the node resources.
  const string quotaResourcesString = "cpus:0.5;mem:200";
  const Quota quota = createQuota(quotaResourcesString);
  allocator->updateQuota(QUOTA_ROLE, quota);

  NodeInfo node = createNodeInfo("cpus:1;mem:512;disk:0");
  allocator->addNode(
      node.id(),
      node,
      NODE_CAPABILITIES(),
      None(),
      node.resources(),
      {});

  // Due to fine-grained allocation, `framework1` will be offered the
  // exact amount of quota resources on `node` even though the node
  // has more resources to offer.
  Allocation expected = Allocation(
      framework1.id(),
      {{QUOTA_ROLE,
        {{node.id(),
          CHECK_NOTERROR(Resources::parse(quotaResourcesString))}}}});

  AWAIT_EXPECT_EQ(expected, allocations.get());

  // Total cluster resources: cpus=1, mem=512.
  // QUOTA_ROLE share = 1 (cpus=0.5, mem=200) [quota: cpus=0.5, mem=200]
  //   framework1 share = 1

  // Create `framework2` in a non-quota'ed role.
  FrameworkInfo framework2 = createFrameworkInfo({NO_QUOTA_ROLE});
  allocator->addFramework(framework2.id(), framework2, {}, true, {});

  // `framework2` will get the remaining resources of `node`.
  expected = Allocation(
      framework2.id(),
      {{NO_QUOTA_ROLE,
        {{node.id(),
          node.resources() -
            CHECK_NOTERROR(Resources::parse(quotaResourcesString))}}}});

  AWAIT_EXPECT_EQ(expected, allocations.get());

  // Total cluster resources: cpus=1, mem=512.
  // QUOTA_ROLE  (cpus=0.5, mem=200) [quota: cpus=0.5, mem=200]
  // NO_QUOTA_ROLE  (cpus=0.5, mem=312)
}

// While quota allocation should be fine-grained, some resources are
// unchoppable. They have to be offered entirely. This test verifies
// one of the cases: disk resource of type MOUNT.
TEST_F(HierarchicalAllocatorTest, QuotaAllocationGranularityUnchoppableResource)
{
  Clock::pause();
  initialize();

  const string QUOTA_ROLE{"quota-role"};

  const string quotaResourcesString = "cpus:2;mem:2048;disk:150";
  const Quota quota = createQuota(quotaResourcesString);
  allocator->updateQuota(QUOTA_ROLE, quota);

  // Create 100 disk resource of type MOUNT.
  Resources mountDiskResource =
    createDiskResource("100", "*", None(), None(),
      createDiskSourceMount(), false);

  Resources nodeResources =
    Resources::parse("cpus:0.5;mem:512").get() + mountDiskResource;

  NodeInfo node1 = createNodeInfo(nodeResources);
  allocator->addNode(
      node1.id(),
      node1,
      NODE_CAPABILITIES(),
      None(),
      node1.resources(),
      {});

  // Create `framework` under `QUOTA_ROLE`.
  // This will tigger an event-driven allocation loop.
  FrameworkInfo framework = createFrameworkInfo({QUOTA_ROLE});
  allocator->addFramework(framework.id(), framework, {}, true, {});

  Clock::settle();

  // `framework` will get all resources of `node1` because it is the only
  // framework.
  Allocation expected = Allocation(
      framework.id(),
      {{QUOTA_ROLE, {{node1.id(), node1.resources()}}}});

  AWAIT_EXPECT_EQ(expected, allocations.get());

  // Quota: "cpus:2;mem:2048;disk:150".
  // Allocated quota: "cpus:0.5;mem:512;disk:100".

  // Add `node2` with identical resources.
  // This will trigger an event-driven allocation for `node2`.
  NodeInfo node2 = createNodeInfo(nodeResources);
  allocator->addNode(
      node2.id(),
      node2,
      NODE_CAPABILITIES(),
      None(),
      node2.resources(),
      {});

  Clock::settle();

  // `framework` will get all resources of `node2` except the disk.
  // Because the unsatisfied disk quota of `QUOTA_ROLE` is 50 and
  // there is 100 MOUNT disk on `node2` which cannot be chopped.
  expected = Allocation(framework.id(),
      {{QUOTA_ROLE, {{node2.id(), node2.resources() - mountDiskResource}}}});

  AWAIT_EXPECT_EQ(expected, allocations.get());

  // Quota: "cpus:2;mem:2048;disk:150".
  // Allocated quota: "cpus:1;mem:1024;disk:100".

  Resources allocatedQuota =
    node1.resources() + node2.resources() - mountDiskResource;

  // Create `node3` that has enough resources to meet `QUOTA_ROLE` quota.
  // This disk resource of node3 can be chopped to meet `QUOTA_ROLE` quota
  NodeInfo node3 = createNodeInfo("cpus:2;mem:2048;disk:150");
  allocator->addNode(
      node3.id(),
      node3,
      NODE_CAPABILITIES(),
      None(),
      node3.resources(),
      {});

  Clock::settle();

  expected = Allocation(
      framework.id(),
      {{QUOTA_ROLE,
        {{node3.id(),
          CHECK_NOTERROR(Resources::parse(quotaResourcesString)) -
            allocatedQuota.createStrippedScalarQuantity()}}}});

  AWAIT_EXPECT_EQ(expected, allocations.get());

  // Quota: "cpus:2;mem:2048;disk:150".
  // Allocated quota: "cpus:2;mem:2048;disk:150",
  // out of the 150 disk resource, 100 is MOUNT type, 50 is default type.
}


// This test ensures that quota is satisfied in the presence of multiple
// unreserved disk resources. This is a regression test for MESOS-9692.
TEST_F(HierarchicalAllocatorTest, QuotaAllocationMultipleDisk)
{
  // The test sets a quota that contains 100 disk resources and
  // a node with two kinds of disk resources: 50 vanilla disks
  // and 50 mount disks. Both disks (100 in total) should be offered
  // to the framework in a single offer to meet its role's quota.

  Clock::pause();
  initialize();

  const string QUOTA_ROLE{"quota-role"};

  const Quota quota = createQuota("cpus:1;mem:512;disk:100");
  allocator->updateQuota(QUOTA_ROLE, quota);

  // Create 50 disk resource of type `MOUNT`.
  Resources mountDiskResource = createDiskResource(
      "50", "*", None(), None(), createDiskSourceMount(), false);

  Resources nodeResources =
    Resources::parse("cpus:1;mem:512;disk:50").get() + mountDiskResource;

  NodeInfo node1 = createNodeInfo(nodeResources);
  allocator->addNode(
      node1.id(),
      node1,
      NODE_CAPABILITIES(),
      None(),
      node1.resources(),
      {});

  // Create `framework` under `QUOTA_ROLE`.
  // This will tigger an event-driven allocation loop.
  FrameworkInfo framework = createFrameworkInfo({QUOTA_ROLE});
  allocator->addFramework(framework.id(), framework, {}, true, {});

  Clock::settle();

  // `framework` will get all resources (including both disks) of `node1` to
  // satisfy its role's quota.
  Allocation expected = Allocation(
      framework.id(), {{QUOTA_ROLE, {{node1.id(), node1.resources()}}}});

  AWAIT_EXPECT_EQ(expected, allocations.get());
}


// This test verifies the behavior of allocating a resource to quota roles
// that has no quota set for that particular resource (e.g. allocating
// memory to a role with only quota set for CPU). If a role has no quota
// set for a resource, it will get that resource only when two conditions
// are both met:
//
// - It was allocated quota resources on the node;
//
// - It can be allocated without violating the quota headroom.
TEST_F(HierarchicalAllocatorTest, QuotaRoleAllocateNonQuotaResource)
{
  Clock::pause();
  initialize();

  const string QUOTA_ROLE_1{"quota-role-1"};

  const string quotaResourcesString1 = "cpus:2";
  const Quota quota1 = createQuota(quotaResourcesString1);
  allocator->updateQuota(QUOTA_ROLE_1, quota1);

  NodeInfo node1 = createNodeInfo("cpus:1;mem:1024;ports:[31000-32000]");
  allocator->addNode(
      node1.id(),
      node1,
      NODE_CAPABILITIES(),
      None(),
      node1.resources(),
      {});

  // Create `framework` under `QUOTA_ROLE_1`.
  // This will tigger an event-driven allocation loop.
  FrameworkInfo framework = createFrameworkInfo({QUOTA_ROLE_1});
  allocator->addFramework(framework.id(), framework, {}, true, {});

  Clock::settle();

  // `framework` will get all resources of `node1`. Memory resource is
  // allocated because `QUOTA_ROLE_1` does not have any memory quota limit
  // and there is no need to set aside memory for other quota roles. Port
  // resource is allocated due to the same reason (this is further due to
  // the fact that ports are non-scalar resources that no roles can set
  // quota for).
  Allocation expected = Allocation(
      framework.id(),
      {{QUOTA_ROLE_1, {{node1.id(), node1.resources()}}}});

  AWAIT_EXPECT_EQ(expected, allocations.get());

  // QUOTA_ROLE_1:
  // quota: "cpus:2"
  // allocated: "cpus:1;mem:1024;ports:[31000-32000]" (node1)

  const string QUOTA_ROLE_2{"quota-role-2"};

  const string quotaResourcesString2 = "mem:1024";
  const Quota quota2 = createQuota(quotaResourcesString2);
  allocator->updateQuota(QUOTA_ROLE_2, quota2);

  // Add `node2` with identical resources.
  // This will trigger an event-driven allocation on `node2`.
  NodeInfo node2 = createNodeInfo("cpus:1;mem:1024;ports:[31000-32000]");
  allocator->addNode(
      node2.id(),
      node2,
      NODE_CAPABILITIES(),
      None(),
      node2.resources(),
      {});

  // `framework` will only get cpu and port resources. CPU resource is
  // allocated because `QUOTA_ROLE_1` still needs one more CPU to meet its
  // quota. Memory is NOT allocated even though `QUOTA_ROLE_1` has no memory
  // quota limit because the memory needs to be set aside for `QUOTA_ROLE_2`.
  // Port resource is allocated because `QUOTA_ROLE_1` does not have any port
  // quota limit and there is no need to set aside the port resource for other
  // quota roles ((this is further due to the fact that ports are non-scalar
  // resources that no roles can set quota for).
  expected = Allocation(
      framework.id(),
      {{QUOTA_ROLE_1,
        {{node2.id(),
          node2.resources() -
            CHECK_NOTERROR(Resources::parse(quotaResourcesString2))}}}});

  AWAIT_EXPECT_EQ(expected, allocations.get());

  // QUOTA_ROLE_1:
  // quota: "cpus:2"
  // allocated: "cpus:1;mem:1024;ports:[31000-32000]" (node1)
  //            "cpus:1;ports:[31000-32000]" (node2)
  //
  // QUOTA_ROLE_1's quota has been met.

  // Add `node3` with identical resources.
  // This will trigger an event-driven allocation on `node3`.
  NodeInfo node3 = createNodeInfo("cpus:1;mem:1024;ports:[31000-32000]");
  allocator->addNode(
      node3.id(),
      node3,
      NODE_CAPABILITIES(),
      None(),
      node3.resources(),
      {});

  Clock::settle();

  // `QUOTA_ROLE_1` quota has been reached. Only resources with the default
  // quota (i.e. no limits) are allocated.

  Resources nonQuotaResources =
    Resources(node3.resources()).filter([&](const Resource& resource) {
      return quota1.guarantees.get(resource.name()) == Value::Scalar();
    });

  expected = Allocation(
      framework.id(), {{QUOTA_ROLE_1, {{node3.id(), nonQuotaResources}}}});

  AWAIT_EXPECT_EQ(expected, allocations.get());
}


// This test verifies, that the free pool (what is left after all quotas
// are satisfied) is allocated according to the DRF algorithm across the roles
// which do not have quota set.
TEST_F(HierarchicalAllocatorTest, DRFWithQuota)
{
  // Pausing the clock is not necessary, but ensures that the test
  // doesn't rely on the batch allocation in the allocator, which
  // would slow down the test.
  Clock::pause();

  const string QUOTA_ROLE{"quota-role"};
  const string NO_QUOTA_ROLE{"no-quota-role"};

  initialize();

  const string quotaResourcesString = "cpus:0.25;mem:128";
  const Quota quota = createQuota(quotaResourcesString);
  allocator->updateQuota(QUOTA_ROLE, quota);

  FrameworkInfo framework1 = createFrameworkInfo({QUOTA_ROLE});
  allocator->addFramework(framework1.id(), framework1, {}, true, {});

  FrameworkInfo framework2 = createFrameworkInfo({NO_QUOTA_ROLE});
  allocator->addFramework(framework2.id(), framework2, {}, true, {});

  // Process all triggered allocation events.
  //
  // NOTE: No allocations happen because there are no resources to allocate.
  Clock::settle();

  JSON::Object metrics = Metrics();

  string metric =
    "allocator/medios/quota"
    "/roles/" + QUOTA_ROLE +
    "/resources/cpus"
    "/guarantee";
  EXPECT_EQ(0.25, metrics.values[metric]);

  metric =
    "allocator/medios/quota"
    "/roles/" + QUOTA_ROLE +
    "/resources/cpus"
    "/limit";
  EXPECT_EQ(0.25, metrics.values[metric]);

  metric =
    "allocator/medios/quota"
    "/roles/" + QUOTA_ROLE +
    "/resources/mem"
    "/guarantee";
  EXPECT_EQ(128, metrics.values[metric]);

  metric =
    "allocator/medios/quota"
    "/roles/" + QUOTA_ROLE +
    "/resources/mem"
    "/limit";
  EXPECT_EQ(128, metrics.values[metric]);

  // Add a node with some allocated resources.
  NodeInfo node1 = createNodeInfo("cpus:1;mem:512;disk:0");
  allocator->addNode(
      node1.id(),
      node1,
      NODE_CAPABILITIES(),
      None(),
      node1.resources(),
      {{framework1.id(),
        allocatedResources(
            CHECK_NOTERROR(Resources::parse(quotaResourcesString)),
            QUOTA_ROLE)}});

  // Total cluster resources (1 node): cpus=1, mem=512.
  // QUOTA_ROLE share = 0.25 (cpus=0.25, mem=128) [quota: cpus=0.25, mem=128]
  //   framework1 share = 1
  // NO_QUOTA_ROLE share = 0
  //   framework2 share = 0

  // Some resources on `node1` are now being used by `framework1` as part
  // of its role quota. All quotas are satisfied, all available resources
  // should be allocated according to fair shares of roles and frameworks.

  // `framework2` will be offered all of `node1`'s resources because its
  // share is 0.
  Allocation expected = Allocation(
      framework2.id(),
      {{NO_QUOTA_ROLE,
        {{node1.id(),
          Resources(node1.resources()) -
            CHECK_NOTERROR(Resources::parse(quotaResourcesString))}}}});

  AWAIT_EXPECT_EQ(expected, allocations.get());

  metrics = Metrics();

  metric =
    "allocator/medios/quota"
    "/roles/" + QUOTA_ROLE +
    "/resources/cpus"
    "/offered_or_allocated";
  EXPECT_EQ(0.25, metrics.values[metric]);

  metric =
    "allocator/medios/quota"
    "/roles/" + QUOTA_ROLE +
    "/resources/mem"
    "/offered_or_allocated";
  EXPECT_EQ(128, metrics.values[metric]);

  metric =
    "allocator/medios/quota"
    "/roles/" + QUOTA_ROLE +
    "/resources/disk"
    "/offered_or_allocated";
  EXPECT_EQ(0u, metrics.values.count(metric));

  // Total cluster resources (1 node): cpus=1, mem=512.
  // QUOTA_ROLE share = 0.25 (cpus=0.25, mem=128) [quota: cpus=0.25, mem=128]
  //   framework1 share = 1
  // NO_QUOTA_ROLE share = 0.75 (cpus=0.75, mem=384)
  //   framework2 share = 1

  NodeInfo node2 = createNodeInfo("cpus:1;mem:512;disk:0");
  allocator->addNode(
      node2.id(),
      node2,
      NODE_CAPABILITIES(),
      None(),
      node2.resources(),
      {});

  // `framework2` will be offered all of `node2`'s resources (coarse-grained
  // allocation). `framework1` does not receive them even though it has a
  // smaller allocation, since we have already satisfied its role's quota.

  expected = Allocation(
      framework2.id(),
      {{NO_QUOTA_ROLE, {{node2.id(), node2.resources()}}}});

  AWAIT_EXPECT_EQ(expected, allocations.get());
}


// This tests addresses a so-called "starvation" case. Suppose there are
// several frameworks below their fair share: they decline any offers they
// get. There is also a framework which fully utilizes its share and would
// accept more resources if they were offered. However, if there are not
// many free resources available and the decline timeout is small enough,
// free resources may circulate between frameworks underutilizing their fair
// share and might never be offered to the framework that needs them. While
// this behavior corresponds to the way DRF algorithm works, it might not be
// desirable in some cases. Setting quota for a "starving" role can mitigate
// the issue.
TEST_F(HierarchicalAllocatorTest, QuotaAgainstStarvation)
{
  // Pausing the clock is not necessary, but ensures that the test
  // doesn't rely on the batch allocation in the allocator, which
  // would slow down the test.
  Clock::pause();

  const string QUOTA_ROLE{"quota-role"};
  const string NO_QUOTA_ROLE{"no-quota-role"};

  initialize();

  FrameworkInfo framework1 = createFrameworkInfo({QUOTA_ROLE});
  allocator->addFramework(framework1.id(), framework1, {}, true, {});

  FrameworkInfo framework2 = createFrameworkInfo({NO_QUOTA_ROLE});
  allocator->addFramework(framework2.id(), framework2, {}, true, {});

  NodeInfo node1 = createNodeInfo("cpus:1;mem:512;disk:0");
  allocator->addNode(
      node1.id(),
      node1,
      NODE_CAPABILITIES(),
      None(),
      node1.resources(),
      {{framework1.id(), allocatedResources(node1.resources(), QUOTA_ROLE)}});

  // Total cluster resources (1 node): cpus=1, mem=512.
  // QUOTA_ROLE share = 1 (cpus=1, mem=512)
  //   framework1 share = 1
  // NO_QUOTA_ROLE share = 0
  //   framework2 share = 0

  NodeInfo node2 = createNodeInfo("cpus:1;mem:512;disk:0");
  allocator->addNode(
      node2.id(),
      node2,
      NODE_CAPABILITIES(),
      None(),
      node2.resources(),
      {});

  // Free cluster resources on `node2` will be allocated to `framework2`
  // because its share is 0.

  Allocation expected = Allocation(
      framework2.id(),
      {{NO_QUOTA_ROLE, {{node2.id(), node2.resources()}}}});

  AWAIT_EXPECT_EQ(expected, allocations.get());

  // Total cluster resources (2 identical nodes): cpus=2, mem=1024.
  // QUOTA_ROLE share = 0.5 (cpus=1, mem=512)
  //   framework1 share = 1
  // NO_QUOTA_ROLE share = 0.5 (cpus=1, mem=512)
  //   framework2 share = 1

  // If `framework2` declines offered resources with 0 timeout, they will
  // be returned to the free pool and then allocated to `framework2` again,
  // because its share is still 0.
  Filters filter0s;
  filter0s.set_refuse_seconds(0.);
  allocator->recoverResources(
      framework2.id(),
      node2.id(),
      allocatedResources(node2.resources(), NO_QUOTA_ROLE),
      filter0s,
      false);

  // Total cluster resources (2 identical nodes): cpus=2, mem=1024.
  // QUOTA_ROLE share = 0.5 (cpus=1, mem=512)
  //   framework1 share = 1
  // NO_QUOTA_ROLE share = 0
  //   framework2 share = 0

  // Trigger the next batch allocation.
  Clock::advance(flags.allocation_interval);

  expected = Allocation(
      framework2.id(),
      {{NO_QUOTA_ROLE, {{node2.id(), node2.resources()}}}});

  AWAIT_EXPECT_EQ(expected, allocations.get());

  // `framework2` continues declining offers.
  allocator->recoverResources(
      framework2.id(),
      node2.id(),
      allocatedResources(node2.resources(), NO_QUOTA_ROLE),
      filter0s,
      false);

  // We set quota for the "starving" `QUOTA_ROLE` role.
  const Quota quota = createQuota("cpus:2;mem:1024");
  allocator->updateQuota(QUOTA_ROLE, quota);

  // Since `QUOTA_ROLE` is under quota, `node2`'s resources will
  // be allocated to `framework1`.

  // Trigger the next batch allocation.
  Clock::advance(flags.allocation_interval);

  expected = Allocation(
      framework1.id(),
      {{QUOTA_ROLE, {{node2.id(), node2.resources()}}}});

  AWAIT_EXPECT_EQ(expected, allocations.get());

  // Total cluster resources: cpus=2, mem=1024.
  // QUOTA_ROLE share = 1 (cpus=2, mem=1024) [quota: cpus=2, mem=1024]
  //   framework1 share = 1
  // NO_QUOTA_ROLE share = 0
  //   framework2 share = 0
}


// This test checks that quota is respected even for roles that do not
// have any frameworks currently registered. It also ensures an event-
// triggered allocation does not unnecessarily deprive non-quota'ed
// frameworks of resources.
//
// TODO(mzhu): Add a similar test but with only one node. The test
// will then verify the node is "chopped" to maintain the quota headroom.
TEST_F(HierarchicalAllocatorTest, QuotaAbsentFrameworkWholeNode)
{
  // Pausing the clock is not necessary, but ensures that the test
  // doesn't rely on the batch allocation in the allocator, which
  // would slow down the test.
  Clock::pause();

  const string QUOTA_ROLE{"quota-role"};
  const string NO_QUOTA_ROLE{"no-quota-role"};

  initialize();

  // Set quota for the quota'ed role. This role isn't registered with
  // the allocator yet.
  const Quota quota = createQuota("cpus:2;mem:1024");
  allocator->updateQuota(QUOTA_ROLE, quota);

  // Add `framework` in the non-quota'ed role.
  FrameworkInfo framework = createFrameworkInfo({NO_QUOTA_ROLE});
  allocator->addFramework(framework.id(), framework, {}, true, {});

  // Process all triggered allocation events.
  //
  // NOTE: No allocations happen because there are no resources to allocate.
  Clock::settle();

  Resources nodeResources =
    CHECK_NOTERROR(Resources::parse("cpus:2;mem:1024"));

  // Total cluster resources (0 nodes): 0.
  // QUOTA_ROLE share = 0 [quota: cpus=2, mem=1024]
  //   no frameworks
  // NO_QUOTA_ROLE share = 0
  //   framework share = 0

  // Each `addNode()` triggers an event-based allocation.
  NodeInfo node1 = createNodeInfo(nodeResources);
  allocator->addNode(
      node1.id(),
      node1,
      NODE_CAPABILITIES(),
      None(),
      node1.resources(),
      {});

  Future<Allocation> allocation = allocations.get();

  // `framework` won't get any allocation because all of the node1's resources
  // are set aside to satisfy `QUOTA_ROLE` role's quota guarantee (even though
  // there is no framework under the role).
  EXPECT_TRUE(allocation.isPending());

  NodeInfo node2 = createNodeInfo(nodeResources);
  allocator->addNode(
      node2.id(),
      node2,
      NODE_CAPABILITIES(),
      None(),
      node2.resources(),
      {});

  // `node1` and `node2` have identical resources. One of them
  // will be allocated. The other node will be set aside for `QUOTA_ROLE`
  // quota guarantee.
  AWAIT_READY(allocation);

  ASSERT_EQ(allocation->frameworkId, framework.id());
  ASSERT_EQ(1u, allocation->resources.size());
  ASSERT_TRUE(allocation->resources.contains(NO_QUOTA_ROLE));

  nodeResources.allocate(NO_QUOTA_ROLE);
  EXPECT_EQ(
      nodeResources, Resources::sum(allocation->resources.at(NO_QUOTA_ROLE)));
}


// This test checks that if one role with quota has no frameworks in it,
// other roles with quota are still offered resources. Roles without
// frameworks have zero fair share and are always considered first during
// allocation, hence this test actually addresses several scenarios:
//  * Quota'ed roles without frameworks do not prevent other quota'ed roles
//    from getting resources.
//  * Resources are not laid away for quota'ed roles without frameworks if
//    there are other quota'ed roles with not fully satisfied quota.
TEST_F(HierarchicalAllocatorTest, MultiQuotaAbsentFrameworks)
{
  // Pausing the clock is not necessary, but ensures that the test
  // doesn't rely on the batch allocation in the allocator, which
  // would slow down the test.
  Clock::pause();

  const string QUOTA_ROLE1{"quota-role-1"};
  const string QUOTA_ROLE2{"quota-role-2"};

  initialize();

  NodeInfo node = createNodeInfo("cpus:2;mem:2048;disk:0");
  allocator->addNode(
      node.id(),
      node,
      NODE_CAPABILITIES(),
      None(),
      node.resources(),
      {});

  // Set quota for both roles.
  const Quota quota1 = createQuota("cpus:1;mem:1024");
  allocator->updateQuota(QUOTA_ROLE1, quota1);

  const Quota quota2 = createQuota("cpus:2;mem:2048");
  allocator->updateQuota(QUOTA_ROLE2, quota2);

  // Add a framework in the `QUOTA_ROLE2` role.
  FrameworkInfo framework = createFrameworkInfo({QUOTA_ROLE2});
  allocator->addFramework(framework.id(), framework, {}, true, {});

  // Due to the coarse-grained nature of the allocations, `framework` will
  // get all `node`'s resources.
  Allocation expected = Allocation(
      framework.id(),
      {{QUOTA_ROLE2, {{node.id(), node.resources()}}}});

  AWAIT_EXPECT_EQ(expected, allocations.get());
}


// This test checks that if there are multiple roles with quota, all of them
// get enough offers given there are enough resources. Suppose one quota'ed
// role has smaller share and is fully satisfied. Another quota'ed role has
// greater share but its quota is not fully satisfied yet. Though the first
// role is considered before the second because it has smaller share, this
// should not lead to starvation of the second role.
TEST_F(HierarchicalAllocatorTest, MultiQuotaWithFrameworks)
{
  // Pausing the clock is not necessary, but ensures that the test
  // doesn't rely on the batch allocation in the allocator, which
  // would slow down the test.
  Clock::pause();

  const string QUOTA_ROLE1{"quota-role-1"};
  const string QUOTA_ROLE2{"quota-role-2"};

  initialize();

  // Mem Quota for `QUOTA_ROLE1` is 10 times smaller than for `QUOTA_ROLE2`.
  const string quotaResourcesString1 = "cpus:1;mem:200";
  const Quota quota1 = createQuota(quotaResourcesString1);
  allocator->updateQuota(QUOTA_ROLE1, quota1);

  const string quotaResourcesString2 = "cpus:2;mem:2000";
  const Quota quota2 = createQuota(quotaResourcesString2);
  allocator->updateQuota(QUOTA_ROLE2, quota2);

  // Add `framework1` in the `QUOTA_ROLE1` role.
  FrameworkInfo framework1 = createFrameworkInfo({QUOTA_ROLE1});
  allocator->addFramework(framework1.id(), framework1, {}, true, {});

  // Add `framework2` in the `QUOTA_ROLE2` role.
  FrameworkInfo framework2 = createFrameworkInfo({QUOTA_ROLE2});
  allocator->addFramework(framework2.id(), framework2, {}, true, {});

  // Process all triggered allocation events.
  //
  // NOTE: No allocations happen because there are no resources to allocate.
  Clock::settle();

  NodeInfo node1 = createNodeInfo("cpus:1;mem:1024;disk:0");
  allocator->addNode(
      node1.id(),
      node1,
      NODE_CAPABILITIES(),
      None(),
      node1.resources(),
      {{framework1.id(), allocatedResources(node1.resources(), QUOTA_ROLE1)}});

  NodeInfo node2 = createNodeInfo("cpus:1;mem:1024;disk:0");
  allocator->addNode(
      node2.id(),
      node2,
      NODE_CAPABILITIES(),
      None(),
      node2.resources(),
      {{framework2.id(), allocatedResources(node2.resources(), QUOTA_ROLE2)}});

  // TODO(bmahler): Add assertions to test this is accurate!
  //
  // Total cluster resources (2 identical nodes): cpus=2, mem=2048.
  // QUOTA_ROLE1 share = 0.5 (cpus=1, mem=200) [quota: cpus=1, mem=200]
  //   framework1 share = 1
  // QUOTA_ROLE2 share = 0.5 (cpus=1, mem=1024) [quota: cpus=2, mem=2000]
  //   framework2 share = 1

  // Quota for the `QUOTA_ROLE1` role is satisfied, while `QUOTA_ROLE2` is
  // under quota. Hence resources of the newly added node should be offered
  // to the framework in `QUOTA_ROLE2`.

  NodeInfo node3 = createNodeInfo("cpus:2;mem:2048");
  allocator->addNode(
      node3.id(),
      node3,
      NODE_CAPABILITIES(),
      None(),
      node3.resources(),
      {});

  // `framework2` will get some of node3's resources to meet its quota.
  Allocation expected = Allocation(
      framework2.id(),
      {{QUOTA_ROLE2,
        {{node3.id(),
          CHECK_NOTERROR(Resources::parse(quotaResourcesString2)) -
            node2.resources()}}}});

  AWAIT_EXPECT_EQ(expected, allocations.get());

  // Total cluster resources (3 nodes): cpus=4, mem=4096.
  // QUOTA_ROLE1 share = 0.33 (cpus=1, mem=1024) [quota: cpus=1, mem=200]
  //   framework1 share = 1
  // QUOTA_ROLE2 share = 0.66 (cpus=2, mem=3=2000) [quota: cpus=2, mem=2000]
  //   framework2 share = 1
}


// This tests that reserved resources are accounted for in the role's quota.
TEST_F(HierarchicalAllocatorTest, ReservationWithinQuota)
{
  // Pausing the clock is not necessary, but ensures that the test
  // doesn't rely on the batch allocation in the allocator, which
  // would slow down the test.
  Clock::pause();

  const string QUOTA_ROLE{"quota-role"};
  const string NON_QUOTA_ROLE{"non-quota-role"};

  initialize();

  const Quota quota = createQuota("cpus:2;mem:256");
  allocator->updateQuota(QUOTA_ROLE, quota);

  FrameworkInfo framework1 = createFrameworkInfo({QUOTA_ROLE});
  allocator->addFramework(framework1.id(), framework1, {}, true, {});

  FrameworkInfo framework2 = createFrameworkInfo({NON_QUOTA_ROLE});
  allocator->addFramework(framework2.id(), framework2, {}, true, {});

  // Process all triggered allocation events.
  //
  // NOTE: No allocations happen because there are no resources to allocate.
  Clock::settle();

  // Some resources on `node1` are now being used by `framework1` as part
  // of its role quota. `framework2` will be offered the rest of `node1`'s
  // resources since `framework1`'s quota is satisfied, and `framework2` has
  // no resources.
  NodeInfo node1 = createNodeInfo("cpus:8;mem(" + QUOTA_ROLE + "):256");
  allocator->addNode(
      node1.id(),
      node1,
      NODE_CAPABILITIES(),
      None(),
      node1.resources(),
      {{framework1.id(),
        // The `mem` portion is used to test that reserved resources are
        // accounted for, and the `cpus` portion is allocated to show that
        // the result of DRF would be different if `mem` was not accounted.
        allocatedResources(
            Resources::parse("cpus:2;mem(" + QUOTA_ROLE + "):256").get(),
            QUOTA_ROLE)}});

  Allocation expected = Allocation(
      framework2.id(),
      {{NON_QUOTA_ROLE, {{node1.id(), Resources::parse("cpus:6").get()}}}});

  AWAIT_EXPECT_EQ(expected, allocations.get());

  // Since the reserved resources account towards the quota as well as being
  // accounted for DRF, we expect these resources to also be allocated to
  // `framework2`.
  NodeInfo node2 = createNodeInfo("cpus:4");
  allocator->addNode(
      node2.id(),
      node2,
      NODE_CAPABILITIES(),
      None(),
      node2.resources(),
      {});

  expected = Allocation(
      framework2.id(),
      {{NON_QUOTA_ROLE, {{node2.id(), node2.resources()}}}});

  AWAIT_EXPECT_EQ(expected, allocations.get());
}


// This test checks that when setting aside unallocated resources to
// ensure that a quota guarantee can be met, we don't use resources
// that have been reserved for a different role.
//
// We setup a scenario with 8 CPUs, where role X has quota for 4 CPUs
// and role Y has 4 CPUs reserved. All offers are declined; the 4
// unreserved CPUs should not be offered to role Y.
TEST_F(HierarchicalAllocatorTest, QuotaSetAsideReservedResources)
{
  Clock::pause();

  const string QUOTA_ROLE{"quota-role"};
  const string NO_QUOTA_ROLE{"no-quota-role"};

  initialize();

  // Create two nodes.
  NodeInfo node1 = createNodeInfo("cpus:4;mem:512;disk:0");
  allocator->addNode(
      node1.id(),
      node1,
      NODE_CAPABILITIES(),
      None(),
      node1.resources(),
      {});

  NodeInfo node2 = createNodeInfo("cpus:4;mem:512;disk:0");
  allocator->addNode(
      node2.id(),
      node2,
      NODE_CAPABILITIES(),
      None(),
      node2.resources(),
      {});

  // Reserve 4 CPUs and 512MB of memory on `node2` for non-quota'ed role.
  Resources unreserved = Resources::parse("cpus:4;mem:512").get();
  Resources dynamicallyReserved = unreserved.pushReservation(
      createDynamicReservationInfo(NO_QUOTA_ROLE, "ops"));

  Offer::Operation reserve = RESERVE(dynamicallyReserved);

  Future<Nothing> updateNode2 =
    allocator->updateAvailable(node2.id(), {reserve});

  AWAIT_EXPECT_READY(updateNode2);

  // Create `framework1` and set quota for its role.
  FrameworkInfo framework1 = createFrameworkInfo({QUOTA_ROLE});
  allocator->addFramework(framework1.id(), framework1, {}, true, {});

  const string quotaResourcesString = "cpus:4;mem:512";
  const Quota quota = createQuota(quotaResourcesString);
  allocator->updateQuota(QUOTA_ROLE, quota);

  // `framework1` will be offered resources at `node1` up to its quota limit
  // because the resources at `node2` are reserved for a different role.
  Allocation expected = Allocation(
      framework1.id(),
      {{QUOTA_ROLE,
        {{node1.id(),
          CHECK_NOTERROR(Resources::parse(quotaResourcesString))}}}});

  AWAIT_EXPECT_EQ(expected, allocations.get());

  // `framework1` declines the resources on `node1` for the duration
  // of the test.
  Filters longFilter;
  longFilter.set_refuse_seconds(flags.allocation_interval.secs() * 10);

  allocator->recoverResources(
      framework1.id(),
      node1.id(),
      allocatedResources(
          CHECK_NOTERROR(Resources::parse(quotaResourcesString)), QUOTA_ROLE),
      longFilter,
      false);

  // Trigger a batch allocation for good measure, but don't expect any
  // allocations.
  Clock::advance(flags.allocation_interval);
  Clock::settle();

  Future<Allocation> allocation = allocations.get();
  EXPECT_TRUE(allocation.isPending());

  // Create `framework2` in a non-quota'ed role.
  FrameworkInfo framework2 = createFrameworkInfo({NO_QUOTA_ROLE});
  allocator->addFramework(framework2.id(), framework2, {}, true, {});

  // `framework2` will be offered the reserved resources at `node2`
  // because those resources are reserved for its role.
  expected = Allocation(
      framework2.id(),
      {{NO_QUOTA_ROLE, {{node2.id(), dynamicallyReserved}}}});

  AWAIT_EXPECT_EQ(expected, allocation);

  // `framework2` declines the resources on `node2` for the duration
  // of the test.
  allocator->recoverResources(
      framework2.id(),
      node2.id(),
      allocatedResources(dynamicallyReserved, NO_QUOTA_ROLE),
      longFilter,
      false);

  // No more resource offers should be made until the filters expire:
  // `framework1` should not be offered the resources at `node2`
  // (because they are reserved for a different role), and
  // `framework2` should not be offered the resources at `node1`
  // (because this would risk violating quota guarantees).

  // Trigger a batch allocation for good measure, but don't expect any
  // allocations.
  Clock::advance(flags.allocation_interval);
  Clock::settle();

  allocation = allocations.get();
  EXPECT_TRUE(allocation.isPending());
}


// This test checks that if a framework suppresses offers, disconnects and
// reconnects again, it will start receiving resource offers again.
TEST_F(HierarchicalAllocatorTest, DeactivateAndReactivateFramework)
{
  // Pausing the clock is not necessary, but ensures that the test
  // doesn't rely on the batch allocation in the allocator, which
  // would slow down the test.
  Clock::pause();

  initialize();

  // Total cluster resources will become cpus=2, mem=1024.
  NodeInfo node = createNodeInfo("cpus:2;mem:1024;disk:0");
  allocator->addNode(
      node.id(),
      node,
      NODE_CAPABILITIES(),
      None(),
      node.resources(),
      {});

  // Framework will be offered all of the node's resources since it is
  // the only framework running so far.
  FrameworkInfo framework = createFrameworkInfo({"role1"});
  allocator->addFramework(framework.id(), framework, {}, true, {});

  Allocation expected = Allocation(
      framework.id(),
      {{"role1", {{node.id(), node.resources()}}}});

  AWAIT_EXPECT_EQ(expected, allocations.get());

  allocator->recoverResources(
      framework.id(),
      node.id(),
      allocatedResources(node.resources(), "role1"),
      None(),
      false);

  // Suppress offers and disconnect framework.
  allocator->suppressOffers(framework.id(), {});
  allocator->deactivateFramework(framework.id());

  // Advance the clock and trigger a background allocation cycle.
  Clock::advance(flags.allocation_interval);

  // Wait for all the `suppressOffers` and `deactivateFramework`
  // operations to be processed.
  Clock::settle();

  Future<Allocation> allocation = allocations.get();
  EXPECT_TRUE(allocation.isPending());

  // Reconnect the framework again.
  allocator->activateFramework(framework.id());
  allocator->reviveOffers(framework.id(), {});

  // Framework will be offered all of node's resources again
  // after getting activated.
  expected = Allocation(
      framework.id(),
      {{"role1", {{node.id(), node.resources()}}}});

  AWAIT_EXPECT_EQ(expected, allocation);
}


// This test verifies that offer suppression and revival work as intended.
TEST_F(HierarchicalAllocatorTest, SuppressAndReviveOffers)
{
  // Pausing the clock is not necessary, but ensures that the test
  // doesn't rely on the batch allocation in the allocator, which
  // would slow down the test.
  Clock::pause();

  initialize();

  // Total cluster resources will become cpus=2, mem=1024.
  NodeInfo node = createNodeInfo("cpus:2;mem:1024;disk:0");
  allocator->addNode(
      node.id(),
      node,
      NODE_CAPABILITIES(),
      None(),
      node.resources(),
      {});

  // Framework will be offered all of the node's resources since it is
  // the only framework running so far.
  FrameworkInfo framework = createFrameworkInfo({"role1"});
  allocator->addFramework(framework.id(), framework, {}, true, {});

  Allocation expected = Allocation(
      framework.id(),
      {{"role1", {{node.id(), node.resources()}}}});

  AWAIT_EXPECT_EQ(expected, allocations.get());

  // Here the revival is totally unnecessary but we should tolerate the
  // framework's redundant REVIVE calls.
  allocator->reviveOffers(framework.id(), {});

  // Settle to ensure that the dispatched allocation is executed.
  Clock::settle();

  // Nothing is allocated because of no additional resources.
  Future<Allocation> allocation = allocations.get();
  EXPECT_TRUE(allocation.isPending());

  allocator->recoverResources(
      framework.id(),
      node.id(),
      allocatedResources(node.resources(), "role1"),
      None(),
      false);

  allocator->suppressOffers(framework.id(), {});

  // Advance the clock and trigger a background allocation cycle.
  Clock::advance(flags.allocation_interval);
  Clock::settle();

  // Still pending because the framework has suppressed offers.
  EXPECT_TRUE(allocation.isPending());

  // Revive again and this time it should work.
  allocator->reviveOffers(framework.id(), {});

  // Framework will be offered all of node's resources again after
  // reviving offers.
  expected = Allocation(
      framework.id(),
      {{"role1", {{node.id(), node.resources()}}}});

  AWAIT_EXPECT_EQ(expected, allocation);
}


// This test checks that total and allocator resources
// are correctly reflected in the metrics endpoint.
TEST_F(HierarchicalAllocatorTest, ResourceMetrics)
{
  // Pausing the clock is not necessary, but ensures that the test
  // doesn't rely on the batch allocation in the allocator, which
  // would slow down the test.
  Clock::pause();

  initialize();

  NodeInfo node = createNodeInfo("cpus:2;mem:1024;disk:0");
  allocator->addNode(
      node.id(),
      node,
      NODE_CAPABILITIES(),
      None(),
      node.resources(),
      {});

  Clock::settle();

  JSON::Object expected;

  // No frameworks are registered yet, so nothing is allocated.
  expected.values = {
      {"allocator/medios/resources/cpus/total",   2},
      {"allocator/medios/resources/mem/total", 1024},
      {"allocator/medios/resources/disk/total",   0},
      {"allocator/medios/resources/cpus/offered_or_allocated", 0},
      {"allocator/medios/resources/mem/offered_or_allocated",  0},
      {"allocator/medios/resources/disk/offered_or_allocated", 0},
  };

  JSON::Value metrics = Metrics();

  EXPECT_TRUE(metrics.contains(expected));

  FrameworkInfo framework = createFrameworkInfo({"role1"});
  allocator->addFramework(framework.id(), framework, {}, true, {});
  Clock::settle();

  // All of the resources should be offered.
  expected.values = {
      {"allocator/medios/resources/cpus/total",   2},
      {"allocator/medios/resources/mem/total", 1024},
      {"allocator/medios/resources/disk/total",   0},
      {"allocator/medios/resources/cpus/offered_or_allocated",   2},
      {"allocator/medios/resources/mem/offered_or_allocated", 1024},
      {"allocator/medios/resources/disk/offered_or_allocated",   0},
  };

  metrics = Metrics();

  EXPECT_TRUE(metrics.contains(expected));

  allocator->removeNode(node.id());
  Clock::settle();

  // No frameworks are registered yet, so nothing is allocated.
  expected.values = {
      {"allocator/medios/resources/cpus/total", 0},
      {"allocator/medios/resources/mem/total",  0},
      {"allocator/medios/resources/disk/total", 0},
      {"allocator/medios/resources/cpus/offered_or_allocated", 0},
      {"allocator/medios/resources/mem/offered_or_allocated",  0},
      {"allocator/medios/resources/disk/offered_or_allocated", 0},
  };

  metrics = Metrics();

  EXPECT_TRUE(metrics.contains(expected));
}


// Ensures that guarantee and limit metrics are exposed
// and updated correctly.
TEST_F(HierarchicalAllocatorTest, QuotaMetrics)
{
  // Pausing the clock is not necessary, but ensures that the test
  // doesn't rely on the batch allocation in the allocator, which
  // would slow down the test.
  Clock::pause();

  initialize();

  const string cpuGuaranteeKey =
    "allocator/medios/quota/roles/role/resources/cpus/guarantee";
  const string cpuLimitKey =
    "allocator/medios/quota/roles/role/resources/cpus/limit";
  const string memGuaranteeKey =
    "allocator/medios/quota/roles/role/resources/mem/guarantee";
  const string memLimitKey =
    "allocator/medios/quota/roles/role/resources/mem/limit";

  JSON::Object metrics = Metrics();

  EXPECT_TRUE(metrics.values.count(cpuGuaranteeKey) == 0);
  EXPECT_TRUE(metrics.values.count(cpuLimitKey) == 0);
  EXPECT_TRUE(metrics.values.count(memGuaranteeKey) == 0);
  EXPECT_TRUE(metrics.values.count(memLimitKey) == 0);

  // Set quota to have:
  //   * 1 cpu guarantee / no cpu limit and
  //   * no mem guarantee / 1024 mem limit
  Quota quota = createQuota("cpus:1", "mem:1024");
  allocator->updateQuota("role", quota);
  Clock::settle();

  metrics = Metrics();

  EXPECT_EQ(1, metrics.values[cpuGuaranteeKey]);
  EXPECT_TRUE(metrics.values.count(cpuLimitKey) == 0);
  EXPECT_TRUE(metrics.values.count(memGuaranteeKey) == 0);
  EXPECT_EQ(1024, metrics.values[memLimitKey]);

  // Increase the cpu guarantee:
  quota = createQuota("cpus:2", "mem:1024");
  allocator->updateQuota("role", quota);
  Clock::settle();

  metrics = Metrics();

  EXPECT_EQ(2, metrics.values[cpuGuaranteeKey]);

  // Set back to default quota.
  quota = createQuota("", "");
  allocator->updateQuota("role", quota);
  Clock::settle();

  metrics = Metrics();

  EXPECT_TRUE(metrics.values.count(cpuGuaranteeKey) == 0);
  EXPECT_TRUE(metrics.values.count(cpuLimitKey) == 0);
  EXPECT_TRUE(metrics.values.count(memGuaranteeKey) == 0);
  EXPECT_TRUE(metrics.values.count(memLimitKey) == 0);
}


// The allocator is not fully initialized until `allocator->initialize(...)`
// is called (e.g., from `Controller::initialize()` or
// `HierarchicalAllocatorTestBase::initialize(...)`). This test
// verifies that metrics collection works but returns empty results
// when the allocator is uninitialized. In reality this can happen if
// the metrics endpoint is polled before the controller is initialized.
TEST_F(HierarchicalAllocatorTest, ResourceMetricsUninitialized)
{
  JSON::Value metrics = Metrics();

  JSON::Object expected;

  // Nothing is added to the allocator or allocated.
  expected.values = {
      {"allocator/medios/resources/cpus/total", 0},
      {"allocator/medios/resources/mem/total",  0},
      {"allocator/medios/resources/disk/total", 0},
      {"allocator/medios/resources/cpus/offered_or_allocated", 0},
      {"allocator/medios/resources/mem/offered_or_allocated",  0},
      {"allocator/medios/resources/disk/offered_or_allocated", 0},
  };

  EXPECT_TRUE(metrics.contains(expected));
}


// This test checks that the number of times the allocation
// algorithm has run is correctly reflected in the metric.
TEST_F(HierarchicalAllocatorTest, AllocationRunsMetric)
{
  // Pausing the clock is not necessary, but ensures that the test
  // doesn't rely on the batch allocation in the allocator, which
  // would slow down the test.
  Clock::pause();

  initialize();

  size_t allocations = 0;

  JSON::Object expected;

  expected.values = { {"allocator/medios/allocation_runs", allocations} };

  JSON::Value metrics = Metrics();

  EXPECT_TRUE(metrics.contains(expected));

  NodeInfo node = createNodeInfo("cpus:2;mem:1024;disk:0");
  allocator->addNode(
      node.id(),
      node,
      NODE_CAPABILITIES(),
      None(),
      node.resources(),
      {});

  // Wait for the allocation triggered from `addNode()` to complete.
  // Otherwise `addFramework()` below may not trigger a new allocation
  // because the allocator batches them.
  Clock::settle();

  ++allocations; // Adding a node triggers allocations.

  FrameworkInfo framework = createFrameworkInfo({"role"});
  allocator->addFramework(framework.id(), framework, {}, true, {});

  Clock::settle();

  ++allocations; // Adding a framework triggers allocations.

  // Allocation count is set based on adding a node and a framework.
  expected.values = { {"allocator/medios/allocation_runs", allocations} };

  metrics = Metrics();

  EXPECT_TRUE(metrics.contains(expected));
}


// This test checks that the allocation run timer
// metrics are reported in the metrics endpoint.
TEST_F(
    HierarchicalAllocatorTest,
    AllocationRunTimerMetrics)
{
  Clock::pause();

  initialize();

  // These time series statistics will be generated
  // once at least 2 allocation runs occur.
  auto statistics = {
    "allocator/medios/allocation_run_ms/count",
    "allocator/medios/allocation_run_ms/min",
    "allocator/medios/allocation_run_ms/max",
    "allocator/medios/allocation_run_ms/p50",
    "allocator/medios/allocation_run_ms/p95",
    "allocator/medios/allocation_run_ms/p99",
    "allocator/medios/allocation_run_ms/p999",
    "allocator/medios/allocation_run_ms/p9999",
  };

  JSON::Object metrics = Metrics();
  map<string, JSON::Value> values = metrics.values;

  EXPECT_EQ(0u, values.count("allocator/medios/allocation_run_ms"));

  // No allocation timing statistics should appear.
  foreach (const string& statistic, statistics) {
    EXPECT_EQ(0u, values.count(statistic))
      << "Expected " << statistic << " to be absent";
  }

  // Allow the allocation timer to measure time.
  Clock::resume();

  // Trigger at least two calls to allocate occur
  // to generate the window statistics.
  NodeInfo node = createNodeInfo("cpus:2;mem:1024;disk:0");
  allocator->addNode(
      node.id(),
      node,
      NODE_CAPABILITIES(),
      None(),
      node.resources(),
      {});

  // Due to the batching of allocation work, wait for the `allocate()`
  // call and subsequent work triggered by `addNode()` to complete.
  Clock::pause();
  Clock::settle();
  Clock::resume();

  FrameworkInfo framework = createFrameworkInfo({"role1"});
  allocator->addFramework(framework.id(), framework, {}, true, {});

  // Wait for the allocation triggered by `addFramework()` to complete.
  AWAIT_READY(allocations.get());

  // Ensure the timer has been stopped so that
  // the second measurement to be recorded.
  Clock::pause();
  Clock::settle();
  Clock::resume();

  metrics = Metrics();
  values = metrics.values;

  // A non-zero measurement should be present.
  EXPECT_EQ(1u, values.count("allocator/medios/allocation_run_ms"));

  JSON::Value value = metrics.values["allocator/medios/allocation_run_ms"];
  ASSERT_TRUE(value.is<JSON::Number>()) << value.which();

  JSON::Number timing = value.as<JSON::Number>();
  ASSERT_EQ(JSON::Number::FLOATING, timing.type);
  EXPECT_GT(timing.as<double>(), 0.0);

  // The statistics should be generated.
  foreach (const string& statistic, statistics) {
    EXPECT_EQ(1u, values.count(statistic))
      << "Expected " << statistic << " to be present";
  }
}


// This test checks that the allocation run latency
// metrics are reported in the metrics endpoint.
// TODO(xujyan): This test is structurally similar to
// `AllocationRunTimerMetrics` above. Consider a refactor.
TEST_F(
    HierarchicalAllocatorTest,
    AllocationRunLatencyMetrics)
{
  Clock::pause();

  initialize();

  // These time series statistics will be generated
  // once at least 2 allocation runs occur.
  auto statistics = {
    "allocator/medios/allocation_run_latency_ms/count",
    "allocator/medios/allocation_run_latency_ms/min",
    "allocator/medios/allocation_run_latency_ms/max",
    "allocator/medios/allocation_run_latency_ms/p50",
    "allocator/medios/allocation_run_latency_ms/p95",
    "allocator/medios/allocation_run_latency_ms/p99",
    "allocator/medios/allocation_run_latency_ms/p999",
    "allocator/medios/allocation_run_latency_ms/p9999",
  };

  JSON::Object metrics = Metrics();
  map<string, JSON::Value> values = metrics.values;

  EXPECT_EQ(0u, values.count("allocator/medios/allocation_run_latency_ms"));

  // No allocation latency statistics should appear.
  foreach (const string& statistic, statistics) {
    EXPECT_EQ(0u, values.count(statistic))
      << "Expected " << statistic << " to be absent";
  }

  // Allow the allocation timer to measure time.
  Clock::resume();

  // Trigger at least two calls to allocate to generate the window statistics.
  NodeInfo node = createNodeInfo("cpus:2;mem:1024;disk:0");
  allocator->addNode(
      node.id(),
      node,
      NODE_CAPABILITIES(),
      None(),
      node.resources(),
      {});

  // Wait for the allocation triggered by `addNode()` to complete.
  Clock::pause();
  Clock::settle();
  Clock::resume();

  FrameworkInfo framework = createFrameworkInfo({"role1"});
  allocator->addFramework(framework.id(), framework, {}, true, {});

  // Wait for the allocation triggered by `addFramework()` to complete.
  AWAIT_READY(allocations.get());

  metrics = Metrics();
  values = metrics.values;

  // A non-zero measurement should be present.
  EXPECT_EQ(1u, values.count("allocator/medios/allocation_run_latency_ms"));

  JSON::Value value =
    metrics.values["allocator/medios/allocation_run_latency_ms"];
  ASSERT_TRUE(value.is<JSON::Number>()) << value.which();

  JSON::Number timing = value.as<JSON::Number>();
  ASSERT_EQ(JSON::Number::FLOATING, timing.type);
  EXPECT_GE(timing.as<double>(), 0.0);

  // The statistics should be generated.
  foreach (const string& statistic, statistics) {
    EXPECT_EQ(1u, values.count(statistic))
      << "Expected " << statistic << " to be present";
  }
}


// This test checks that per-role active offer filter metrics
// are correctly reported in the metrics endpoint.
TEST_F(
    HierarchicalAllocatorTest,
    ActiveOfferFiltersMetrics)
{
  // Pausing the clock is not necessary, but ensures that the test
  // doesn't rely on the batch allocation in the allocator, which
  // would slow down the test.
  Clock::pause();

  initialize();

  NodeInfo node = createNodeInfo("cpus:2;mem:1024;disk:0");
  allocator->addNode(
      node.id(),
      node,
      NODE_CAPABILITIES(),
      None(),
      node.resources(),
      {});

  // Register three frameworks, two of which are in the same role.
  // For every offer the frameworks install practically indefinite
  // offer filters.
  Duration filterTimeout = flags.allocation_interval * 100;
  Filters offerFilter;
  offerFilter.set_refuse_seconds(filterTimeout.secs());

  FrameworkInfo framework1 = createFrameworkInfo({"roleA"});
  allocator->addFramework(framework1.id(), framework1, {}, true, {});

  Allocation expectedAllocation = Allocation(
      framework1.id(),
      {{"roleA", {{node.id(), node.resources()}}}});

  Future<Allocation> allocation = allocations.get();
  AWAIT_EXPECT_EQ(expectedAllocation, allocation);

  allocator->recoverResources(
      allocation->frameworkId,
      node.id(),
      allocation->resources.at("roleA").at(node.id()),
      offerFilter,
      false);

  JSON::Object expected;
  expected.values = {
      {"allocator/medios/offer_filters/roles/roleA/active", 1},
  };

  JSON::Value metrics = Metrics();

  EXPECT_TRUE(metrics.contains(expected));

  FrameworkInfo framework2 = createFrameworkInfo({"roleB"});
  allocator->addFramework(framework2.id(), framework2, {}, true, {});

  expectedAllocation = Allocation(
      framework2.id(),
      {{"roleB", {{node.id(), node.resources()}}}});

  allocation = allocations.get();
  AWAIT_EXPECT_EQ(expectedAllocation, allocation);

  allocator->recoverResources(
      allocation->frameworkId,
      node.id(),
      allocation->resources.at("roleB").at(node.id()),
      offerFilter,
      false);

  expected.values = {
      {"allocator/medios/offer_filters/roles/roleA/active", 1},
      {"allocator/medios/offer_filters/roles/roleB/active", 1},
  };

  metrics = Metrics();

  EXPECT_TRUE(metrics.contains(expected));

  FrameworkInfo framework3 = createFrameworkInfo({"roleA"});
  allocator->addFramework(framework3.id(), framework3, {}, true, {});

  expectedAllocation = Allocation(
      framework3.id(),
      {{"roleA", {{node.id(), node.resources()}}}});

  allocation = allocations.get();
  AWAIT_EXPECT_EQ(expectedAllocation, allocation);

  allocator->recoverResources(
      allocation->frameworkId,
      node.id(),
      allocation->resources.at("roleA").at(node.id()),
      offerFilter,
      false);

  expected.values = {
      {"allocator/medios/offer_filters/roles/roleA/active", 2},
      {"allocator/medios/offer_filters/roles/roleB/active", 1},
  };

  metrics = Metrics();

  EXPECT_TRUE(metrics.contains(expected));
}


// Verifies that per-role dominant share metrics are correctly reported.
TEST_F(HierarchicalAllocatorTest, DominantShareMetrics)
{
  // Pausing the clock is not necessary, but ensures that the test
  // doesn't rely on the batch allocation in the allocator, which
  // would slow down the test.
  Clock::pause();

  initialize();

  // Register one node and one framework. The framework will
  // immediately receive receive an offer and make it have the
  // maximum possible dominant share.
  NodeInfo node1 = createNodeInfo("cpus:1;mem:1024");
  allocator->addNode(
      node1.id(),
      node1,
      NODE_CAPABILITIES(),
      None(),
      node1.resources(),
      {});

  FrameworkInfo framework1 = createFrameworkInfo({"roleA"});
  allocator->addFramework(framework1.id(), framework1, {}, true, {});
  Clock::settle();

  JSON::Object expected;

  expected.values = {
      {"allocator/medios/roles/roleA/shares/dominant", 1},
  };

  JSON::Value metrics = Metrics();
  EXPECT_TRUE(metrics.contains(expected));

  // Decline the offered resources and expect a zero share.
  Future<Allocation> allocation = allocations.get();
  AWAIT_READY(allocation);
  allocator->recoverResources(
      allocation->frameworkId,
      node1.id(),
      allocation->resources.at("roleA").at(node1.id()),
      None(),
      false);

  Clock::settle();

  expected.values = {
      {"allocator/medios/roles/roleA/shares/dominant", 0},
  };

  metrics = Metrics();
  EXPECT_TRUE(metrics.contains(expected));

  // Register a second framework. This framework will receive
  // offers as `framework1` has just declined an offer and the
  // implicit filter has not yet timed out. The new framework
  // will have the full share.
  FrameworkInfo framework2 = createFrameworkInfo({"roleB"});
  allocator->addFramework(framework2.id(), framework2, {}, true, {});
  Clock::settle();

  expected.values = {
      {"allocator/medios/roles/roleA/shares/dominant", 0},
      {"allocator/medios/roles/roleB/shares/dominant", 1},
  };

  metrics = Metrics();
  EXPECT_TRUE(metrics.contains(expected));

  // Add a second, identical node. Now `framework1` will
  // receive an offer since it has the lowest dominant
  // share. After the offer the dominant shares of
  // `framework1` and `framework2` are equal.
  NodeInfo node2 = createNodeInfo("cpus:1;mem:1024");
  allocator->addNode(
      node2.id(),
      node2,
      NODE_CAPABILITIES(),
      None(),
      node2.resources(),
      {});

  Clock::settle();

  expected.values = {
      {"allocator/medios/roles/roleA/shares/dominant", 0.5},
      {"allocator/medios/roles/roleB/shares/dominant", 0.5},
  };

  metrics = Metrics();
  EXPECT_TRUE(metrics.contains(expected));

  // Removing `framework2` frees up its allocated resources. The
  // corresponding metric is removed when the last framework in
  // the role is removed.
  allocator->removeFramework(framework2.id());
  Clock::settle();

  expected.values = {
      {"allocator/medios/roles/roleA/shares/dominant", 0.5},
  };

  metrics = Metrics();
  EXPECT_TRUE(metrics.contains(expected));

  ASSERT_TRUE(metrics.is<JSON::Object>());
  map<string, JSON::Value> values = metrics.as<JSON::Object>().values;
  EXPECT_EQ(0u, values.count("allocator/medios/roles/roleB/shares/dominant"));
}


// Verifies that per-role dominant share metrics are correctly
// reported when resources are excluded from fair sharing.
TEST_F(
    HierarchicalAllocatorTest,
    DominantShareMetricsWithFairnessExclusion)
{
  // Pausing the clock is not necessary, but ensures that the test
  // doesn't rely on the batch allocation in the allocator, which
  // would slow down the test.
  Clock::pause();

  // Specify that `gpus` should not be fairly shared.
  controller::Flags flags_;
  flags_.fair_sharing_excluded_resource_names = set<string>({"gpus"});

  initialize(flags_);

  // Register one node and one framework. The framework will
  // immediately receive receive an offer and make it have the
  // maximum possible dominant share.
  NodeInfo node1 = createNodeInfo("cpus:1;mem:1024;gpus:1");
  allocator->addNode(
      node1.id(),
      node1,
      NODE_CAPABILITIES(),
      None(),
      node1.resources(),
      {});

  FrameworkInfo framework1 = createFrameworkInfo(
      {"roleA"}, {FrameworkInfo::Capability::GPU_RESOURCES});

  allocator->addFramework(framework1.id(), framework1, {}, true, {});
  Clock::settle();

  JSON::Object expected;

  expected.values = {
      {"allocator/medios/roles/roleA/shares/dominant", 1},
  };

  JSON::Value metrics = Metrics();
  EXPECT_TRUE(metrics.contains(expected));

  FrameworkInfo framework2 = createFrameworkInfo({"roleB"});
  allocator->addFramework(framework2.id(), framework2, {}, true, {});
  Clock::settle();

  // Add a second, identical node. Now `framework2` will
  // receive an offer since it has the lowest dominant share:
  // the 100% of `gpus` allocated to framework1 are excluded!
  NodeInfo node2 = createNodeInfo("cpus:3;mem:3072");
  allocator->addNode(
      node2.id(),
      node2,
      NODE_CAPABILITIES(),
      None(),
      node2.resources(),
      {});

  Clock::settle();

  expected.values = {
      {"allocator/medios/roles/roleA/shares/dominant", 0.25},
      {"allocator/medios/roles/roleB/shares/dominant", 0.75},
  };

  metrics = Metrics();
  EXPECT_TRUE(metrics.contains(expected));
}


// This test ensures that resource allocation is done according to each role's
// weight. This is done by having six nodes and three frameworks and making
// sure each framework gets the appropriate number of resources.
TEST_F(HierarchicalAllocatorTest, UpdateWeight)
{
  // Pausing the clock is not necessary, but ensures that the test
  // doesn't rely on the batch allocation in the allocator, which
  // would slow down the test.
  Clock::pause();

  initialize();

  const Resources SINGLE_RESOURCES = Resources::parse("cpus:2;mem:1024").get();
  const Resources DOUBLE_RESOURCES = SINGLE_RESOURCES + SINGLE_RESOURCES;
  const Resources TRIPLE_RESOURCES = DOUBLE_RESOURCES + SINGLE_RESOURCES;
  const Resources FOURFOLD_RESOURCES = DOUBLE_RESOURCES + DOUBLE_RESOURCES;

  // There will be 6 nodes.
  const Resources TOTAL_RESOURCES = FOURFOLD_RESOURCES + DOUBLE_RESOURCES;

  auto awaitAllocationsAndRecoverResources = [this](
      Resources* totalAllocatedResources,
      hashmap<FrameworkID, Allocation>* frameworkAllocations,
      int allocationsCount,
      bool recoverResources) {
    for (int i = 0; i < allocationsCount; i++) {
      Future<Allocation> allocation = allocations.get();
      AWAIT_READY(allocation);
      ASSERT_EQ(1u, allocation->resources.size());

      (*frameworkAllocations)[allocation->frameworkId] = allocation.get();
      *totalAllocatedResources +=
        Resources::sum(allocation->resources.begin()->second);

      if (recoverResources) {
        // Recover the allocated resources so they can be offered
        // again next time.
        foreachkey (const string& role, allocation->resources) {
          foreachpair (const NodeID& nodeId,
                       const Resources& resources,
                       allocation->resources.at(role)) {
            allocator->recoverResources(
                allocation->frameworkId, nodeId, resources, None(), false);
          }
        }
      }
    }
  };

  // Register six nodes with the same resources (cpus:2;mem:1024).
  vector<NodeInfo> nodes;
  for (size_t i = 0; i < 6; i++) {
    NodeInfo node = createNodeInfo(SINGLE_RESOURCES);
    nodes.push_back(node);
    allocator->addNode(
        node.id(),
        node,
        NODE_CAPABILITIES(),
        None(),
        node.resources(),
        {});
  }

  // Add two frameworks with the same weight, both should receive
  // the same amount of resources once the nodes are added. However,
  // since framework1 is added first, it will receive all of the
  // resources, so we recover them once both frameworks are added.
  FrameworkInfo framework1 = createFrameworkInfo({"role1"});
  allocator->addFramework(framework1.id(), framework1, {}, true, {});

  // Wait for the allocation triggered from `addFramework(framework1)`
  // to complete. Otherwise due to a race between `addFramework(framework2)`
  // and the next allocation (because it's run asynchronously), framework2
  // may or may not be allocated resources. For simplicity here we give
  // all resources to framework1 as all we wanted to achieve in this step
  // is to recover all resources to set up the allocator for the next batch
  // allocation.
  Clock::settle();

  FrameworkInfo framework2 = createFrameworkInfo({"role2"});
  allocator->addFramework(framework2.id(), framework2, {}, true, {});

  // Recover the allocation to framework1 so that the allocator
  // can offer to both frameworks.
  hashmap<FrameworkID, Allocation> frameworkAllocations;
  Resources totalAllocatedResources;
  awaitAllocationsAndRecoverResources(
      &totalAllocatedResources, &frameworkAllocations, 1, true);

  // Total cluster resources (6 nodes): cpus=12, mem=6144.
  {
    // Advance the clock and trigger a batch allocation.
    Clock::advance(flags.allocation_interval);

    // role1 share = 0.5 (cpus=6, mem=3072)
    //   framework1 share = 1
    // role2 share = 0.5 (cpus=6, mem=3072)
    //   framework2 share = 1

    // Ensure that all resources are offered equally between both frameworks,
    // since each framework's role has a weight of 1.0 by default.
    hashmap<FrameworkID, Allocation> frameworkAllocations;
    Resources totalAllocatedResources;
    awaitAllocationsAndRecoverResources(
        &totalAllocatedResources, &frameworkAllocations, 2, true);

    // Both frameworks should get one allocation with three nodes.
    ASSERT_TRUE(frameworkAllocations.contains(framework1.id()));
    ASSERT_TRUE(frameworkAllocations.contains(framework2.id()));

    Allocation allocation1 = frameworkAllocations.at(framework1.id());
    Allocation allocation2 = frameworkAllocations.at(framework2.id());

    EXPECT_EQ(allocatedResources(TRIPLE_RESOURCES, "role1"),
              Resources::sum(allocation1.resources.at("role1")));
    EXPECT_EQ(allocatedResources(TRIPLE_RESOURCES, "role2"),
              Resources::sum(allocation2.resources.at("role2")));

    // Check to ensure that these two allocations sum to the total resources;
    // this check can ensure there are only two allocations in this case.
    EXPECT_EQ(TOTAL_RESOURCES,
              totalAllocatedResources.createStrippedScalarQuantity());
  }

  // Tests whether `framework1` gets 1/3 of the resources and `framework2` gets
  // 2/3 of the resources when their roles' weights are 1:2.
  {
    // Update the weight of framework2's role to 2.0.
    vector<WeightInfo> weightInfos;
    weightInfos.push_back(createWeightInfo({"role2"}, 2.0));
    allocator->updateWeights(weightInfos);

    // Advance the clock and trigger a batch allocation.
    Clock::advance(flags.allocation_interval);

    // role1 share = 0.33 (cpus=4, mem=2048)
    //   framework1 share = 1
    // role2 share = 0.66 (cpus=8, mem=4096)
    //   framework2 share = 1

    // Now that the frameworks's weights are 1:2, ensure that all
    // resources are offered with a ratio of 1:2 between both frameworks.
    hashmap<FrameworkID, Allocation> frameworkAllocations;
    Resources totalAllocatedResources;
    awaitAllocationsAndRecoverResources(
        &totalAllocatedResources, &frameworkAllocations, 2, true);

    ASSERT_TRUE(frameworkAllocations.contains(framework1.id()));
    ASSERT_TRUE(frameworkAllocations.contains(framework2.id()));

    Allocation allocation1 = frameworkAllocations.at(framework1.id());
    Allocation allocation2 = frameworkAllocations.at(framework2.id());

    EXPECT_EQ(allocatedResources(DOUBLE_RESOURCES, "role1"),
              Resources::sum(allocation1.resources.at("role1")));
    EXPECT_EQ(allocatedResources(FOURFOLD_RESOURCES, "role2"),
              Resources::sum(allocation2.resources.at("role2")));

    // Check to ensure that these two allocations sum to the total resources;
    // this check can ensure there are only two allocations in this case.
    EXPECT_EQ(TOTAL_RESOURCES,
              totalAllocatedResources.createStrippedScalarQuantity());
  }

  // Tests whether `framework1` gets 1/6 of the resources, `framework2` gets
  // 2/6 of the resources and `framework3` gets 3/6 of the resources when their
  // roles' weights are 1:2:3.
  {
    // Add a new role with a weight of 3.0.
    vector<WeightInfo> weightInfos;
    weightInfos.push_back(createWeightInfo("role3", 3.0));
    allocator->updateWeights(weightInfos);

    // 'updateWeights' does not trigger an allocation.

    // Framework3 registers with 'role3'.
    FrameworkInfo framework3 = createFrameworkInfo({"role3"});
    allocator->addFramework(framework3.id(), framework3, {}, true, {});

    // 'addFramework' will trigger an allocation.

    // role1 share = 0.166 (cpus=2, mem=1024)
    //   framework1 share = 1
    // role2 share = 0.333 (cpus=4, mem=2048)
    //   framework2 share = 1
    // role3 share = 0.50 (cpus=6, mem=3072)
    //   framework3 share = 1

    // Currently, there are three frameworks and six nodes in this cluster,
    // and the weight ratio of these frameworks is 1:2:3, therefore frameworks
    // will get the proper resource ratio of 1:2:3.
    hashmap<FrameworkID, Allocation> frameworkAllocations;
    Resources totalAllocatedResources;
    awaitAllocationsAndRecoverResources(
        &totalAllocatedResources, &frameworkAllocations, 3, false);

    // Both frameworks should get one allocation with three nodes.
    ASSERT_TRUE(frameworkAllocations.contains(framework1.id()));
    ASSERT_TRUE(frameworkAllocations.contains(framework2.id()));
    ASSERT_TRUE(frameworkAllocations.contains(framework3.id()));

    Allocation allocation1 = frameworkAllocations.at(framework1.id());
    Allocation allocation2 = frameworkAllocations.at(framework2.id());
    Allocation allocation3 = frameworkAllocations.at(framework3.id());

    EXPECT_EQ(allocatedResources(SINGLE_RESOURCES, "role1"),
              Resources::sum(allocation1.resources.at("role1")));
    EXPECT_EQ(allocatedResources(DOUBLE_RESOURCES, "role2"),
              Resources::sum(allocation2.resources.at("role2")));
    EXPECT_EQ(allocatedResources(TRIPLE_RESOURCES, "role3"),
              Resources::sum(allocation3.resources.at("role3")));

    // Check to ensure that these two allocations sum to the total resources;
    // this check can ensure there are only three allocations in this case.
    EXPECT_EQ(TOTAL_RESOURCES,
              totalAllocatedResources.createStrippedScalarQuantity());
  }
}


// This test checks that if a framework declines resources with a
// long filter, it will be offered filtered resources again after
// reviving offers.
TEST_F(HierarchicalAllocatorTest, ReviveOffers)
{
  // Pausing the clock is not necessary, but ensures that the test
  // doesn't rely on the batch allocation in the allocator, which
  // would slow down the test.
  Clock::pause();

  initialize();

  // Total cluster resources will become cpus=2, mem=1024.
  NodeInfo node = createNodeInfo("cpus:2;mem:1024;disk:0");
  allocator->addNode(
      node.id(),
      node,
      NODE_CAPABILITIES(),
      None(),
      node.resources(),
      {});

  // Framework will be offered all of node's resources since it is
  // the only framework running so far.
  FrameworkInfo framework = createFrameworkInfo({"role1"});
  allocator->addFramework(framework.id(), framework, {}, true, {});

  Allocation expected = Allocation(
      framework.id(),
      {{"role1", {{node.id(), node.resources()}}}});

  AWAIT_EXPECT_EQ(expected, allocations.get());

  Filters filter1000s;
  filter1000s.set_refuse_seconds(1000.);
  allocator->recoverResources(
      framework.id(),
      node.id(),
      allocatedResources(node.resources(), "role1"),
      filter1000s,
      false);

  // Advance the clock to trigger a batch allocation.
  Clock::advance(flags.allocation_interval);
  Clock::settle();

  Future<Allocation> allocation = allocations.get();
  EXPECT_TRUE(allocation.isPending());

  allocator->reviveOffers(framework.id(), {});

  // Framework will be offered all of node's resources again
  // after reviving offers.
  AWAIT_EXPECT_EQ(expected, allocation);
}


// This test checks that if a multi-role framework declines resources
// for one role with a long filter, it will be offered filtered resources
// again to another role with some suppress and revive logic.
TEST_F(HierarchicalAllocatorTest, SuppressAndReviveOffersWithMultiRole)
{
  // Pausing the clock is not necessary, but ensures that the test
  // doesn't rely on the batch allocation in the allocator, which
  // would slow down the test.
  Clock::pause();

  initialize();

  // Register a framework with role1 and role2, suppress offer for
  // role1, then role2 in this framework will be offered all of node's
  // resources since it is the only active role in the framework.
  FrameworkInfo framework = createFrameworkInfo({"role1", "role2"});

  allocator->addFramework(framework.id(), framework, {}, true, {});
  allocator->suppressOffers(framework.id(), {"role1"});

  // Total cluster resources will become cpus=2, mem=1024.
  NodeInfo node = createNodeInfo("cpus:2;mem:1024;disk:0");
  allocator->addNode(
      node.id(),
      node,
      NODE_CAPABILITIES(),
      None(),
      node.resources(),
      {});

  Allocation expected = Allocation(
      framework.id(),
      {{"role2", {{node.id(), node.resources()}}}});

  AWAIT_EXPECT_EQ(expected, allocations.get());

  Filters filter1day;
  filter1day.set_refuse_seconds(Days(1).secs());
  allocator->recoverResources(
      framework.id(),
      node.id(),
      allocatedResources(node.resources(), "role2"),
      filter1day,
      false);

  // Advance the clock to trigger a batch allocation.
  Clock::advance(flags.allocation_interval);
  Clock::settle();

  Future<Allocation> allocation = allocations.get();
  EXPECT_TRUE(allocation.isPending());

  // Revive offers for role1. This should NOT affect offer filters of role2.
  allocator->reviveOffers(framework.id(), {"role1"});

  expected = Allocation(
      framework.id(),
      {{"role1", {{node.id(), node.resources()}}}});

  AWAIT_EXPECT_EQ(expected, allocation);

  // Recover offered resources and set a filter for role1 too.
  allocator->recoverResources(
      framework.id(),
      node.id(),
      allocatedResources(node.resources(), "role1"),
      filter1day,
      false);

  // Advance the clock to trigger a batch allocation.
  Clock::advance(flags.allocation_interval);
  Clock::settle();

  // As both roles should have the filters set now, nothing should be allocated.
  allocation = allocations.get();
  EXPECT_TRUE(allocation.isPending());
}


// This test ensures that resources from non-MULTI_ROLE should not be
// allocated to MULTI_ROLE framework.
TEST_F(HierarchicalAllocatorTest, DontOfferOldNodeToMultiRoleFramework)
{
  // Pausing the clock is not necessary, but ensures that the test
  // doesn't rely on the batch allocation in the allocator, which
  // would slow down the test.
  Clock::pause();

  initialize();

  FrameworkInfo framework = createFrameworkInfo(
      {"foo"},
      {FrameworkInfo::Capability::MULTI_ROLE});

  allocator->addFramework(framework.id(), framework, {}, true, {});

  // Total cluster resources will become cpus=2, mem=1024.
  NodeInfo node = createNodeInfo("cpus:2;mem:1024;disk:0");
  allocator->addNode(node.id(), node, {}, None(), node.resources(), {});

  // Advance the clock to trigger a batch allocation.
  Clock::advance(flags.allocation_interval);
  Clock::settle();

  Future<Allocation> allocation = allocations.get();
  EXPECT_TRUE(allocation.isPending());
}


// This tests the behavior of quota when the allocation and
// quota are disproportionate. We always try to allocate
// up to the limit for all resources.
// We test the following example:
//
//        Quota: cpus:4;mem:1024
//   Allocation: cpus:2;mem:1024
//
// Role will only get cpus resources in this case to meet its remaining qutoa.
TEST_F(HierarchicalAllocatorTest, DisproportionateQuotaVsAllocation)
{
  // Pausing the clock is not necessary, but ensures that the test
  // doesn't rely on the batch allocation in the allocator, which
  // would slow down the test.
  Clock::pause();

  initialize();

  const string QUOTA_ROLE{"quota-role"};
  const string NO_QUOTA_ROLE{"no-quota-role"};

  // We use a quota such that mem can be satisfied from a single node,
  // but cpus requires multiple nodes.
  const string nodeResources = "cpus:2;mem:1024";
  const string quotaResources = "cpus:4;mem:1024";

  Quota quota = createQuota(quotaResources);
  allocator->updateQuota(QUOTA_ROLE, quota);

  // Register `framework` under `QUOTA_ROLE`.
  FrameworkInfo framework = createFrameworkInfo({QUOTA_ROLE});
  allocator->addFramework(framework.id(), framework, {}, true, {});

  // Register a node. This triggers an allocation of all of the
  // node's resources to partially satisfy QUOTA_ROLE's quota. After
  // the allocation QUOTA_ROLE's quota for mem will be satisfied while
  // still being below the set quota for cpus. With that framework under
  // QUOTA_ROLE will only receive cpus resources.
  NodeInfo node1 = createNodeInfo(nodeResources);
  allocator->addNode(
      node1.id(),
      node1,
      NODE_CAPABILITIES(),
      None(),
      node1.resources(),
      {});

  Allocation expected = Allocation(
      framework.id(),
      {{QUOTA_ROLE, {{node1.id(), node1.resources()}}}});

  AWAIT_EXPECT_EQ(expected, allocations.get());

  // Register a second node.
  NodeInfo node2 = createNodeInfo(nodeResources);
  allocator->addNode(
      node2.id(),
      node2,
      NODE_CAPABILITIES(),
      None(),
      node2.resources(),
      {});

  Clock::settle();

  // `framework` will get its unsatisfied quota resources (2cpus).
  expected = Allocation(
    framework.id(),
    {{QUOTA_ROLE, {{node2.id(),
      Resources::parse(quotaResources).get() - node1.resources()}}}});

  AWAIT_EXPECT_EQ(expected, allocations.get());
}


// This test ensures that resources reserved to ancestor roles can be offered
// to their descendants.
TEST_F(HierarchicalAllocatorTest, OfferAncestorReservationsToDescendantChild)
{
  // Pausing the clock is not necessary, but ensures that the test
  // doesn't rely on the batch allocation in the allocator, which
  // would slow down the test.
  Clock::pause();

  const string ROLE{"a/b/c"};

  initialize();

  FrameworkInfo framework = createFrameworkInfo({ROLE});
  allocator->addFramework(framework.id(), framework, {}, true, {});

  NodeInfo node1 = createNodeInfo("cpus(a):1;mem(a):512;disk(a):0;");
  allocator->addNode(
      node1.id(),
      node1,
      NODE_CAPABILITIES(),
      None(),
      node1.resources(),
      {});

  {
    // All the resources of node1 are offered.
    Allocation expected = Allocation(
        framework.id(),
        {{ROLE, {{node1.id(), node1.resources()}}}});

    AWAIT_EXPECT_EQ(expected, allocations.get());
  }


  NodeInfo node2 = createNodeInfo(
      "cpus:2;mem:1024;disk:0;"
      "cpus(a/b):4;mem(a/b):2048;disk(a/b):0");
  allocator->addNode(
      node2.id(),
      node2,
      NODE_CAPABILITIES(),
      None(),
      node2.resources(),
      {});

  {
    // We expect all resources of node2 including the reserved resources
    // for the parent of ROLE will be offered.
    Allocation expected = Allocation(
        framework.id(),
        {{ROLE, {{node2.id(), node2.resources()}}}});

    AWAIT_EXPECT_EQ(expected, allocations.get());
  }
}


// This test ensures that resources can be correctly allocated in
// the presence of quota when a parent role's reservation is allocated
// to a child role. Specifically, it checks that a parent role's
// reservations allocated to a child role are correctly considered
// as an allocated reservation by the quota headroom calculation.
// See MESOS-8604.
TEST_F(HierarchicalAllocatorTest, QuotaWithAncestorReservations)
{
  Clock::pause();

  initialize();

  const string QUOTA_ROLE{"quota-role"};
  const string NO_QUOTA_ROLE{"no-quota-role"};
  const string PARENT_ROLE{"a"};
  const string CHILD_ROLE{"a/b"};

  Quota quota = createQuota("cpus:1;mem:1024");
  allocator->updateQuota(QUOTA_ROLE, quota);

  // This node is reserved for the parent role `a`.
  NodeInfo node1 = createNodeInfo("cpus(a):1;mem(a):1024");
  allocator->addNode(
      node1.id(),
      node1,
      NODE_CAPABILITIES(),
      None(),
      node1.resources(),
      {});

  // Add framework1 under the child role `a/b`.
  FrameworkInfo framework1 = createFrameworkInfo({CHILD_ROLE});
  allocator->addFramework(framework1.id(), framework1, {}, true, {});

  // All of node1's resources are allocated to `framework1` under `a/b`
  // because it is reserved by its ancestor `a`.
  Allocation expected = Allocation(
      framework1.id(),
      {{CHILD_ROLE, {{node1.id(), node1.resources()}}}});

  AWAIT_EXPECT_EQ(expected, allocations.get());

  // Add framework2 under `NO_QUOTA_ROLE`.
  FrameworkInfo framework2 = createFrameworkInfo({NO_QUOTA_ROLE});
  allocator->addFramework(framework2.id(), framework2, {}, true, {});

  // No allocations are made because there is no free resources.
  Future<Allocation> allocation = allocations.get();
  EXPECT_TRUE(allocation.isPending());

  // Add node2 and node3 with the same resources as `quota`.

  Resources nodeResources = Resources::parse("cpus:1;mem:1024").get();

  NodeInfo node2 = createNodeInfo("cpus:1;mem:1024");
  allocator->addNode(
      node2.id(),
      node2,
      NODE_CAPABILITIES(),
      None(),
      node2.resources(),
      {});

  NodeInfo node3 = createNodeInfo("cpus:1;mem:1024");
  allocator->addNode(
      node3.id(),
      node3,
      NODE_CAPABILITIES(),
      None(),
      node3.resources(),
      {});

  // Required headroom: quota(cpus:1;mem:1024)
  //
  // Available headroom = total resources (cpus:3;mem:3072) -
  //                      allocated resources (cpus:1;mem:1024) -
  //                      unallocated reservations (cpus:0;mem:0)
  //                    = (cpus:2;mem:2048)
  //
  // Resources that can allocated without breaking quota headroom =
  //   available headroom - required headroom = (cpus:1;mem:1024)

  // Either node2 or node3 will be set aside for the quota headroom for
  // role `QUOTA_ROLE`. The other will be offered to framework2, because
  // it hasn't got any resources and has the lowest share.
  //
  // Node2 and node3 have identical resources. We only care that the
  // framework can get either of them.

  AWAIT_READY(allocation);

  ASSERT_EQ(allocation->frameworkId, framework2.id());
  ASSERT_EQ(1u, allocation->resources.size());
  ASSERT_TRUE(allocation->resources.contains(NO_QUOTA_ROLE));

  nodeResources.allocate(NO_QUOTA_ROLE);
  EXPECT_EQ(nodeResources,
            Resources::sum(allocation->resources.at(NO_QUOTA_ROLE)));
}


// This test verifies that a subrole's reservation is counted towards
// parent's consumed quota. See MESOS-9688.
TEST_F(HierarchicalAllocatorTest, QuotaWithNestedRoleReservation)
{
  // Setup:
  //   Roles: "a"   --> guarantee cpus:2;mem:200
  //          "a/b" --> reservation cpus:1;mem:100
  //
  // Test:
  //   Add a second node with cpus:10;mem:1000
  //   Expect "a" to be allocated cpus:1;mem:100 to reach
  //     its guarantee (since it already has a consumption of
  //     cpus:1;mem:100 from the "a/b" reservation).

  // -------
  // Setup
  // -------

  Clock::pause();

  initialize();

  const string PARENT_ROLE{"a"};
  const string CHILD_ROLE{"a/b"};

  Quota quota = createQuota("cpus:2;mem:200");
  allocator->updateQuota(PARENT_ROLE, quota);

  // This node is reserved for the child role "a/b".
  NodeInfo node1 = createNodeInfo("cpus(a/b):1;mem(a/b):100");
  allocator->addNode(
      node1.id(),
      node1,
      NODE_CAPABILITIES(),
      None(),
      node1.resources(),
      {});

  // -------
  // Test
  // -------

  NodeInfo node2 = createNodeInfo("cpus:10;mem:1000");
  allocator->addNode(
      node2.id(),
      node2,
      NODE_CAPABILITIES(),
      None(),
      node2.resources(),
      {});

  // Add framework1 under the parent role "a".
  FrameworkInfo framework1 = createFrameworkInfo({PARENT_ROLE});
  allocator->addFramework(framework1.id(), framework1, {}, true, {});

  // Process all events.
  Clock::settle();

  Allocation expected = Allocation(
      framework1.id(),
      {{
          PARENT_ROLE,
          {{node2.id(), *Resources::fromSimpleString("cpus:1;mem:100")}}
      }});

  AWAIT_EXPECT_EQ(expected, allocations.get());
}

// This test ensures that nested role's allocation is accounted
// to top-level role's consumed quota.
TEST_F(HierarchicalAllocatorTest, QuotaWithNestedRoleAllocation)
{
  // Setup:
  // node1: R
  // Roles:
  //     "a"   --> guarantee with R w/ no framework
  //     "a/b" --> has `framework1`, allocated R on node1
  //
  // Test:
  //   Add `framework2` under "a"
  //   Add node2 with R
  // Ensure:
  //   `node2` is allocated to `framework1` under `a/b`, even though, role
  // `a` and `framework2` have lower shares. This is because role `a` has
  // reached its quota limit (due to its subrole's allocation). Also,
  // currently, subrole's allocations are not constrained by top-level
  // role's quota (though they are tracked post factum).
  //
  // TODO(mzhu): Once we finish support for hierarchical quota, no allocation
  // should be made since "a/b" will also be bound by the quota of "a".

  // --- SET UP ---

  Clock::pause();

  initialize();

  const string PARENT_ROLE{"a"};
  const string CHILD_ROLE{"a/b"};

  // Add framework1 under the child role "a/b".
  FrameworkInfo framework1 = createFrameworkInfo({CHILD_ROLE});
  allocator->addFramework(framework1.id(), framework1, {}, true, {});

  NodeInfo node1 = createNodeInfo("cpus:1;mem:1024");
  allocator->addNode(
      node1.id(),
      node1,
      NODE_CAPABILITIES(),
      None(),
      node1.resources(),
      {});

  Allocation expected = Allocation(
      framework1.id(), {{CHILD_ROLE, {{node1.id(), node1.resources()}}}});

  AWAIT_EXPECT_EQ(expected, allocations.get());

  Quota quota = createQuota("cpus:1;mem:1024");
  allocator->updateQuota(PARENT_ROLE, quota);

  // --- TEST ---

  // Add framework2 under the parent role "a".
  FrameworkInfo framework2 = createFrameworkInfo({PARENT_ROLE});
  allocator->addFramework(framework2.id(), framework2, {}, true, {});

  NodeInfo node2 = createNodeInfo("cpus:1;mem:1024");
  allocator->addNode(
      node2.id(),
      node2,
      NODE_CAPABILITIES(),
      None(),
      node2.resources(),
      {});

  // Process all events.
  Clock::settle();

  expected = Allocation(
      framework1.id(), {{CHILD_ROLE, {{node2.id(), node2.resources()}}}});

  AWAIT_EXPECT_EQ(expected, allocations.get());
}


// This test ensures that quota headroom is calculated correctly
// in the presence of subrole's allocations.
TEST_F(HierarchicalAllocatorTest, QuotaHeadroomWithNestedRoleAllocation)
{
  // Setup:
  //   nodes: 2 * R
  //    roles: "a"   --> allocated R
  //           "a/b" --> allocated R
  //           "quota-role" --> guarantee R w/ no framework
  //
  // Test: Add 1 more node with R.
  //       Ensure node is held back for "quota-role".
  //       Add 1 more node with R.
  //       Ensure only 1 of the two extra nodes goes to "a" or "a/b"
  //         (since there is enough headroom for "quota-role")

  Clock::pause();

  initialize();

  const string PARENT_ROLE = "a";
  const string CHILD_ROLE = "a/b";

  // Add framework1 under the parent role "a/b".
  FrameworkInfo framework1 = createFrameworkInfo({CHILD_ROLE});
  allocator->addFramework(framework1.id(), framework1, {}, true, {});

  NodeInfo node1 = createNodeInfo("cpus:1;mem:100");
  allocator->addNode(
      node1.id(),
      node1,
      NODE_CAPABILITIES(),
      None(),
      node1.resources(),
      {});

  // All the resources of node1 are offered to framework1.
  Allocation expected = Allocation(
      framework1.id(), {{CHILD_ROLE, {{node1.id(), node1.resources()}}}});

  AWAIT_EXPECT_EQ(expected, allocations.get());

  // Add framework2 under the child role "a".
  FrameworkInfo framework2 = createFrameworkInfo({PARENT_ROLE});
  allocator->addFramework(framework2.id(), framework2, {}, true, {});

  // Add `node2` which will be allocated to `framework2`.
  NodeInfo node2 = createNodeInfo("cpus:1;mem:100");
  allocator->addNode(
      node2.id(),
      node2,
      NODE_CAPABILITIES(),
      None(),
      node2.resources(),
      {});

  // All the resources of node2 are offered to framework2.
  expected = Allocation(
      framework2.id(), {{PARENT_ROLE, {{node2.id(), node2.resources()}}}});

  AWAIT_EXPECT_EQ(expected, allocations.get());

  const string QUOTA_ROLE{"quota-role"};

  Quota quota = createQuota("cpus:1;mem:100");
  allocator->updateQuota(QUOTA_ROLE, quota);

  NodeInfo node3 = createNodeInfo("cpus:1;mem:100");
  allocator->addNode(
      node3.id(),
      node3,
      NODE_CAPABILITIES(),
      None(),
      node3.resources(),
      {});

  Future<Allocation> allocation = allocations.get();
  EXPECT_TRUE(allocation.isPending());

  NodeInfo node4 = createNodeInfo("cpus:1;mem:100");
  allocator->addNode(
      node4.id(),
      node4,
      NODE_CAPABILITIES(),
      None(),
      node4.resources(),
      {});

  Clock::settle();

  // There should be one allocation made. Either node3 or node4 is
  // allocated and the other node is set aside for the quota headroom.
  AWAIT_READY(allocation);
  EXPECT_TRUE(allocations.get().isPending());
}


// This test checks that quota guarantees work as expected when a
// nested role is created as a child of an existing quota'd role.
//
// TODO(bmahler): Re-enable this test once hierarchical quota is
// implemented.
TEST_F(HierarchicalAllocatorTest, DISABLED_NestedRoleQuota)
{
  // Pausing the clock is not necessary, but ensures that the test
  // doesn't rely on the batch allocation in the allocator, which
  // would slow down the test.
  Clock::pause();

  initialize();

  const string PARENT_ROLE = "a/b";
  const string CHILD_ROLE1 = "a/b/c";
  const string CHILD_ROLE2 = "a/b/d";

  // Create `framework1` in PARENT_ROLE and set quota for its role.
  FrameworkInfo framework1 = createFrameworkInfo({PARENT_ROLE});
  allocator->addFramework(framework1.id(), framework1, {}, true, {});

  const Quota parentQuota = createQuota("cpus:2;mem:1024");
  allocator->updateQuota(PARENT_ROLE, parentQuota);

  NodeInfo node = createNodeInfo("cpus:1;mem:512");
  allocator->addNode(
      node.id(),
      node,
      NODE_CAPABILITIES(),
      None(),
      node.resources(),
      {});

  // `framework1` will be offered all the resources on `node` because
  // it is the only framework in the only role with unsatisfied quota.
  {
    Allocation expected = Allocation(
        framework1.id(),
        {{PARENT_ROLE, {{node.id(), node.resources()}}}});

    AWAIT_EXPECT_EQ(expected, allocations.get());
  }

  // `framework1` declines the resources on `node` for the duration
  // of the test.
  Filters longFilter;
  longFilter.set_refuse_seconds(flags.allocation_interval.secs() * 10);

  allocator->recoverResources(
      framework1.id(),
      node.id(),
      allocatedResources(node.resources(), PARENT_ROLE),
      longFilter,
      false);

  // Create `framework2` in CHILD_ROLE1, which is a child role of
  // PARENT_ROLE. CHILD_ROLE1 does not have quota. In the current
  // implementation, because CHILD_ROLE1 does not itself have quota,
  // it will not be offered any of PARENT_ROLE's quota'd resources.
  // This behavior may change in the future (MESOS-7150).
  FrameworkInfo framework2 = createFrameworkInfo({CHILD_ROLE1});
  allocator->addFramework(framework2.id(), framework2, {}, true, {});

  // Trigger a batch allocation for good measure; we do not expect
  // either framework to be offered resources.
  Clock::advance(flags.allocation_interval);
  Clock::settle();

  Future<Allocation> allocation = allocations.get();
  EXPECT_TRUE(allocation.isPending());

  // Set quota for `CHILD_ROLE2`.
  const Quota childQuota = createQuota("cpus:1;mem:512");
  allocator->updateQuota(CHILD_ROLE2, childQuota);

  // Create `framework3` in CHILD_ROLE2, which is a child role of
  // PARENT_ROLE. CHILD_ROLE2 has quota, so in the current
  // implementation, it will be offered resources.
  FrameworkInfo framework3 = createFrameworkInfo({CHILD_ROLE2});
  allocator->addFramework(framework3.id(), framework3, {}, true, {});

  {
    Allocation expected = Allocation(
        framework3.id(),
        {{CHILD_ROLE2, {{node.id(), node.resources()}}}});

    AWAIT_EXPECT_EQ(expected, allocation);
  }
}


// This test checks that quota guarantees work as expected when a
// nested role is created as a child of an existing quota'd role, and
// the parent role has been allocated resources.
//
// TODO(bmahler): Re-enable this test once hierarchical quota is
// implemented.
TEST_F(HierarchicalAllocatorTest, DISABLED_NestedRoleQuotaAllocateToParent)
{
  // Pausing the clock is not necessary, but ensures that the test
  // doesn't rely on the batch allocation in the allocator, which
  // would slow down the test.
  Clock::pause();

  initialize();

  const string PARENT_ROLE = "a/b";
  const string CHILD_ROLE = "a/b/c";

  // Set quota for parent role.
  const Quota parentQuota = createQuota("cpus:4;mem:2048");
  allocator->updateQuota(PARENT_ROLE, parentQuota);

  // Create `framework1` in the parent role.
  FrameworkInfo framework1 = createFrameworkInfo({PARENT_ROLE});
  allocator->addFramework(framework1.id(), framework1, {}, true, {});

  NodeInfo node1 = createNodeInfo("cpus:2;mem:1024");
  allocator->addNode(
      node1.id(),
      node1,
      NODE_CAPABILITIES(),
      None(),
      node1.resources(),
      {});

  // `framework1` will be offered all of the resources on `node1`.
  {
    Allocation expected = Allocation(
        framework1.id(),
        {{PARENT_ROLE, {{node1.id(), node1.resources()}}}});

    AWAIT_EXPECT_EQ(expected, allocations.get());
  }

  // Create `framework2` in the child role.
  FrameworkInfo framework2 = createFrameworkInfo({CHILD_ROLE});
  allocator->addFramework(framework2.id(), framework2, {}, true, {});

  const Quota childQuota = createQuota("cpus:1;mem:512");
  allocator->updateQuota(CHILD_ROLE, childQuota);

  NodeInfo node2 = createNodeInfo("cpus:1;mem:512");
  allocator->addNode(
      node2.id(),
      node2,
      NODE_CAPABILITIES(),
      None(),
      node2.resources(),
      {});

  // `framework2` will be offered all of the resources on `node2`.
  {
    Allocation expected = Allocation(
        framework2.id(),
        {{CHILD_ROLE, {{node2.id(), node2.resources()}}}});

    AWAIT_EXPECT_EQ(expected, allocations.get());
  }

  NodeInfo node3 = createNodeInfo("cpus:1;mem:512");
  allocator->addNode(
      node3.id(),
      node3,
      NODE_CAPABILITIES(),
      None(),
      node3.resources(),
      {});

  // `framework1` will be offered all of the resources on `node3`.
  //
  // NOTE: The quota on PARENT_ROLE actually applies to the entire
  // subtree rooted at PARENT_ROLE, which includes CHILD_ROLE.
  // Therefore, `framework1` and `framework2` should both be
  // candidates to receive the resources at `node3`. In the current
  // implementation, we don't "delegate" the PARENT_ROLE quota to the
  // entire subtree; rather, it can only be used by roles in the
  // subtree that have quota set (MESOS-7150).
  {
    Allocation expected = Allocation(
        framework1.id(),
        {{PARENT_ROLE, {{node3.id(), node3.resources()}}}});

    AWAIT_EXPECT_EQ(expected, allocations.get());
  }
}


// This test checks that when quota resources are allocated to a
// nested role, those resources are also counted against the quota of
// the parent role as well.
//
// TODO(neilc): Re-enable this test when MESOS-7402 is fixed.
TEST_F(HierarchicalAllocatorTest, DISABLED_NestedQuotaAccounting)
{
  Clock::pause();

  initialize();

  const string PARENT_ROLE = "x/b";
  const string CHILD_ROLE = "x/b/c";
  const string NON_QUOTA_ROLE = "aaa";

  // Create `framework1` in the non-quota role.
  FrameworkInfo framework1 = createFrameworkInfo({NON_QUOTA_ROLE});
  allocator->addFramework(framework1.id(), framework1, {}, true, {});

  // Set quota for parent role.
  const Quota parentQuota = createQuota("cpus:3;mem:300");
  allocator->updateQuota(PARENT_ROLE, parentQuota);

  // Create `framework2` in the parent role.
  FrameworkInfo framework2 = createFrameworkInfo({PARENT_ROLE});
  allocator->addFramework(framework2.id(), framework2, {}, true, {});

  NodeInfo node1 = createNodeInfo("cpus:2;mem:200");
  allocator->addNode(
      node1.id(),
      node1,
      NODE_CAPABILITIES(),
      None(),
      node1.resources(),
      {});

  // `framework2` will be offered all of the resources on `node1`.
  {
    Allocation expected = Allocation(
        framework2.id(),
        {{PARENT_ROLE, {{node1.id(), node1.resources()}}}});

    AWAIT_EXPECT_EQ(expected, allocations.get());
  }

  // Set quota for child role.
  const Quota childQuota = createQuota("cpus:1;mem:100");
  allocator->updateQuota(CHILD_ROLE, childQuota);

  // Create `framework3` in the child role.
  FrameworkInfo framework3 = createFrameworkInfo({CHILD_ROLE});
  allocator->addFramework(framework3.id(), framework3, {}, true, {});

  NodeInfo node2 = createNodeInfo("cpus:1;mem:100");
  allocator->addNode(
      node2.id(),
      node2,
      NODE_CAPABILITIES(),
      None(),
      node2.resources(),
      {});

  // `framework3` will be offered all of the resources on `node2`.
  {
    Allocation expected = Allocation(
        framework3.id(),
        {{CHILD_ROLE, {{node2.id(), node2.resources()}}}});

    AWAIT_EXPECT_EQ(expected, allocations.get());
  }

  NodeInfo node3 = createNodeInfo("cpus:1;mem:100");
  allocator->addNode(
      node3.id(),
      node3,
      NODE_CAPABILITIES(),
      None(),
      node3.resources(),
      {});

  // Quota of both frameworks are satisfied at this point, therefore
  // resources of node3 should follow the rule of fair share and be
  // offered to `framework1`.
  {
    Allocation expected = Allocation(
        framework1.id(),
        {{PARENT_ROLE, {{node3.id(), node3.resources()}}}});

    AWAIT_EXPECT_EQ(expected, allocations.get());
  }
}


class HierarchicalAllocatorTestWithParam
  : public HierarchicalAllocatorTestBase,
    public WithParamInterface<bool> {};


// The HierarchicalAllocatorTestWithParam tests are parameterized by a
// flag which indicates if quota is involved (true) or not (false).
// TODO(anindya_sinha): Move over more allocator tests that make sense to run
// both when the role is quota'ed and not.
INSTANTIATE_TEST_CASE_P(
    QuotaSwitch,
    HierarchicalAllocatorTestWithParam,
    ::testing::Bool());


// Tests that shared resources are only offered to frameworks one by one.
// Note that shared resources are offered even if they are in use.
TEST_P(HierarchicalAllocatorTestWithParam, AllocateSharedResources)
{
  Clock::pause();

  initialize();

  // Create 2 frameworks which have opted in for SHARED_RESOURCES.
  FrameworkInfo framework1 = createFrameworkInfo(
      {"role1"},
      {FrameworkInfo::Capability::SHARED_RESOURCES});

  FrameworkInfo framework2 = createFrameworkInfo(
      {"role1"},
      {FrameworkInfo::Capability::SHARED_RESOURCES});

  allocator->addFramework(framework1.id(), framework1, {}, true, {});
  allocator->addFramework(framework2.id(), framework2, {}, true, {});

  if (GetParam()) {
    // Assign a quota.
    const Quota quota = createQuota("cpus:8;mem:2048;disk:4096");
    allocator->updateQuota("role1", quota);
  }

  NodeInfo node = createNodeInfo("cpus:4;mem:1024;disk(role1):2048");
  allocator->addNode(
      node.id(),
      node,
      NODE_CAPABILITIES(),
      None(),
      node.resources(),
      {});

  // Initially, all the resources are allocated to `framework1`.
  Allocation expected = Allocation(
      framework1.id(),
      {{"role1", {{node.id(), node.resources()}}}});

  Future<Allocation> allocation = allocations.get();
  AWAIT_EXPECT_EQ(expected, allocation);

  Resource::AllocationInfo allocationInfo;
  allocationInfo.set_role("role1");

  // Create a shared volume.
  Resource volume = createDiskResource(
      "5", "role1", "id1", None(), None(), true);

  Offer::Operation create = CREATE(volume);

  protobuf::injectAllocationInfo(&create, allocationInfo);

  // Launch a task using the shared volume.
  TaskInfo task = createTask(
      node.id(),
      Resources::parse("cpus:1;mem:5").get() + volume,
      "echo abc > path1/file");

  Try<vector<ResourceConversion>> conversions = getResourceConversions(create);
  ASSERT_SOME(conversions);

  // Ensure the CREATE operation can be applied.
  Try<Resources> updated =
    allocation->resources.at("role1").at(node.id()).apply(conversions.get());

  ASSERT_SOME(updated);

  // Update the allocation in the allocator with a CREATE and a LAUNCH
  // (with one task using the created shared volume) operation.
  allocator->updateAllocation(
      framework1.id(),
      node.id(),
      allocation->resources.at("role1").at(node.id()),
      conversions.get());

  // Now recover the resources, and expect the next allocation to contain
  // the updated resources. Note that the volume is not recovered as it is
  // used by the task (but it is still offerable because it is shared).
  allocator->recoverResources(
      framework1.id(),
      node.id(),
      updated.get() - allocatedResources(task.resources(), "role1"),
      None(),
      false);

  // The offer to 'framework2` should contain the shared volume.
  Clock::advance(flags.allocation_interval);

  expected = Allocation(
      framework2.id(),
      {{"role1", {{node.id(),
          updated.get() -
          allocatedResources(task.resources(), "role1") +
          create.create().volumes()}}}});

  AWAIT_EXPECT_EQ(expected, allocations.get());
}


// Resource sharing types used for the PersistentVolumes benchmark test:
//
// 1. `REGULAR` uses no shared resources.
// 2. `SHARED` only uses shared resources.
// 3. `MIXED` uses half the nodes with shared resources, and the remaining
//    half with regular resources.
enum Sharedness
{
  REGULAR,
  SHARED,
  MIXED
};


ostream& operator<<(ostream& stream, Sharedness type)
{
  switch (type) {
    case REGULAR: stream << "Regular"; break;
    case SHARED: stream << "Shared"; break;
    case MIXED: stream << "Mixed"; break;
    default:
      UNREACHABLE();
  }

  return stream;
}


// --- Gang admission via REQUEST (#323) ---
//
// A REQUEST is a gang request: each entry is one member's per-node
// shape, admitted all-or-none and delivered as a single offers event.
// These tests pause the allocator after adding nodes so that the
// background allocation cycle cannot generate offers; every allocation
// observed below comes from `requestResources` alone.

namespace {

Request createRequest(
    const Resources& resources,
    const Option<NodeID>& nodeId = None())
{
  Request request;

  foreach (const Resource& resource, resources) {
    request.add_resources()->CopyFrom(resource);
  }

  if (nodeId.isSome()) {
    request.mutable_node_id()->CopyFrom(nodeId.get());
  }

  return request;
}


// Builds `gpus:<count>` carrying a single opaque metadata pair
// `class=<klass>`. An empty `klass` leaves the resource untyped.
Resources typedGpus(double count, const string& klass)
{
  Resource resource;
  resource.set_name("gpus");
  resource.set_type(Value::SCALAR);
  resource.mutable_scalar()->set_value(count);

  if (!klass.empty()) {
    Label* label = resource.mutable_metadata()->add_labels();
    label->set_key("class");
    label->set_value(klass);
  }

  return Resources(resource);
}

} // namespace {


class HierarchicalAllocatorGangTest : public HierarchicalAllocatorTestBase
{
protected:
  // Adds `count` nodes of the given resources, pauses the allocator so
  // the allocation cycle stays quiet, and subscribes one framework.
  vector<NodeInfo> setupCluster(
      int count,
      const string& resources,
      const string& role = "train")
  {
    initialize();

    vector<NodeInfo> nodeInfos;
    for (int i = 0; i < count; i++) {
      NodeInfo node = createNodeInfo(resources);
      allocator->addNode(
          node.id(), node, NODE_CAPABILITIES(), None(), node.resources(), {});
      nodeInfos.push_back(node);
    }

    allocator->pause();

    framework = createFrameworkInfo({role});
    allocator->addFramework(framework.id(), framework, {}, true, {});

    return nodeInfos;
  }

  FrameworkInfo framework;
};


// A feasible gang is granted in a single allocation containing every
// member on a distinct node, with exactly the requested shapes.
TEST_F(HierarchicalAllocatorGangTest, GrantedAtomically)
{
  Clock::pause();

  setupCluster(3, "cpus:2;mem:1024;disk:100");

  Future<Allocation> allocation = allocations.get();

  Clock::settle();
  EXPECT_TRUE(allocation.isPending());

  const Resources shape = Resources::parse("cpus:1;mem:512").get();

  allocator->requestResources(
      framework.id(), {createRequest(shape), createRequest(shape)});

  AWAIT_READY(allocation);
  EXPECT_EQ(framework.id(), allocation->frameworkId);
  ASSERT_TRUE(allocation->resources.contains("train"));

  const hashmap<NodeID, Resources>& roster =
    allocation->resources.at("train");

  // Two members on two distinct nodes (distinctness is implied by the
  // map keying), each carrying exactly the requested shape.
  EXPECT_EQ(2u, roster.size());

  Resources allocatedShape = shape;
  allocatedShape.allocate("train");

  foreachvalue (const Resources& member, roster) {
    EXPECT_EQ(allocatedShape, member);
  }
}


// An infeasible gang grants nothing and holds nothing: a subsequent
// feasible gang over the same capacity is granted in full.
TEST_F(HierarchicalAllocatorGangTest, AllOrNone)
{
  Clock::pause();

  setupCluster(2, "cpus:2;mem:1024;disk:100");

  Future<Allocation> allocation = allocations.get();

  const Resources shape = Resources::parse("cpus:2;mem:1024").get();

  // Three members on a two-node cluster: infeasible.
  allocator->requestResources(
      framework.id(),
      {createRequest(shape), createRequest(shape), createRequest(shape)});

  Clock::settle();
  EXPECT_TRUE(allocation.isPending());

  // The full capacity is still grantable: nothing was held back by the
  // failed admission.
  allocator->requestResources(
      framework.id(), {createRequest(shape), createRequest(shape)});

  AWAIT_READY(allocation);
  ASSERT_TRUE(allocation->resources.contains("train"));
  EXPECT_EQ(2u, allocation->resources.at("train").size());
}


// Members seat on distinct nodes: a gang of two does not fit on a
// single node regardless of its capacity.
TEST_F(HierarchicalAllocatorGangTest, DistinctNodes)
{
  Clock::pause();

  setupCluster(1, "cpus:16;mem:8192;disk:1000");

  Future<Allocation> allocation = allocations.get();

  const Resources shape = Resources::parse("cpus:1;mem:512").get();

  allocator->requestResources(
      framework.id(), {createRequest(shape), createRequest(shape)});

  Clock::settle();
  EXPECT_TRUE(allocation.isPending());
}


// A member pinned to a node is granted on exactly that node; a pin
// that cannot be satisfied fails the whole gang.
TEST_F(HierarchicalAllocatorGangTest, PinnedMember)
{
  Clock::pause();

  vector<NodeInfo> nodeInfos = setupCluster(2, "cpus:2;mem:1024;disk:100");

  Future<Allocation> allocation = allocations.get();

  const Resources shape = Resources::parse("cpus:1;mem:512").get();
  const NodeID& pinnedNode = nodeInfos[1].id();

  allocator->requestResources(
      framework.id(), {createRequest(shape, pinnedNode)});

  AWAIT_READY(allocation);
  ASSERT_TRUE(allocation->resources.contains("train"));
  ASSERT_EQ(1u, allocation->resources.at("train").size());
  EXPECT_TRUE(allocation->resources.at("train").contains(pinnedNode));

  // A pin exceeding the pinned node's remaining capacity fails the
  // gang even though the other node could seat the member.
  Future<Allocation> secondAllocation = allocations.get();

  allocator->requestResources(
      framework.id(),
      {createRequest(Resources::parse("cpus:2;mem:1024").get(), pinnedNode)});

  Clock::settle();
  EXPECT_TRUE(secondAllocation.isPending());
}


// Metadata-typed resources seat members only on nodes whose type
// matches: `gpus{class=h100}` is not satisfied by `gpus{class=a100}`.
TEST_F(HierarchicalAllocatorGangTest, TypedGpuMatching)
{
  Clock::pause();

  initialize();

  NodeInfo h100Node =
    createNodeInfo(Resources::parse("cpus:4;mem:2048;disk:100").get() +
                   typedGpus(8, "h100"));
  NodeInfo a100Node =
    createNodeInfo(Resources::parse("cpus:4;mem:2048;disk:100").get() +
                   typedGpus(8, "a100"));

  allocator->addNode(
      h100Node.id(),
      h100Node,
      NODE_CAPABILITIES(),
      None(),
      h100Node.resources(),
      {});
  allocator->addNode(
      a100Node.id(),
      a100Node,
      NODE_CAPABILITIES(),
      None(),
      a100Node.resources(),
      {});

  allocator->pause();

  // GPU-bearing nodes are only offered to frameworks subscribed with
  // the GPU_RESOURCES capability; gang requests inherit that gate.
  framework = createFrameworkInfo(
      {"train"}, {FrameworkInfo::Capability::GPU_RESOURCES});
  allocator->addFramework(framework.id(), framework, {}, true, {});

  Future<Allocation> allocation = allocations.get();

  const Resources shape =
    Resources::parse("cpus:1;mem:512").get() + typedGpus(4, "h100");

  allocator->requestResources(framework.id(), {createRequest(shape)});

  AWAIT_READY(allocation);
  ASSERT_TRUE(allocation->resources.contains("train"));
  ASSERT_EQ(1u, allocation->resources.at("train").size());
  EXPECT_TRUE(allocation->resources.at("train").contains(h100Node.id()));

  // A gang of two h100 members is infeasible: only one h100 node.
  Future<Allocation> secondAllocation = allocations.get();

  allocator->requestResources(
      framework.id(), {createRequest(shape), createRequest(shape)});

  Clock::settle();
  EXPECT_TRUE(secondAllocation.isPending());
}


// A gang whose unreserved scalar demand would eat into another role's
// unsatisfied quota guarantee is refused.
TEST_F(HierarchicalAllocatorGangTest, RespectsQuotaHeadroom)
{
  Clock::pause();

  setupCluster(1, "cpus:2;mem:1024;disk:100");

  const Quota quota = createQuota("cpus:2;mem:1024");
  allocator->updateQuota("quota-role", quota);

  Future<Allocation> allocation = allocations.get();

  allocator->requestResources(
      framework.id(),
      {createRequest(Resources::parse("cpus:1;mem:512").get())});

  Clock::settle();
  EXPECT_TRUE(allocation.isPending());
}


// A request for a suppressed role is refused until the role is
// revived.
TEST_F(HierarchicalAllocatorGangTest, SuppressedRole)
{
  Clock::pause();

  setupCluster(1, "cpus:2;mem:1024;disk:100");

  allocator->suppressOffers(framework.id(), {"train"});

  Future<Allocation> allocation = allocations.get();

  const Resources shape = Resources::parse("cpus:1;mem:512").get();

  allocator->requestResources(framework.id(), {createRequest(shape)});

  Clock::settle();
  EXPECT_TRUE(allocation.isPending());

  allocator->reviveOffers(framework.id(), {"train"});

  allocator->requestResources(framework.id(), {createRequest(shape)});

  AWAIT_READY(allocation);
  ASSERT_TRUE(allocation->resources.contains("train"));
}

} // namespace tests {
} // namespace internal {
} // namespace medios {
