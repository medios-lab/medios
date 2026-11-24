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

#include <string>
#include <vector>

#include <gtest/gtest.h>

#include <stout/gtest.hpp>

#include <medios/resources.hpp>

#include "common/resources_utils.hpp"

#include "tests/medios.hpp"
#include "tests/resources_utils.hpp"

using std::string;
using std::vector;

using medios::internal::protobuf::createLabel;

namespace medios {
namespace internal {
namespace tests {


TEST(ResourcesTest, Reservations)
{
  Resources unreserved = Resources::parse(
      "cpus:1;mem:2;disk:4").get();
  Resources role1 = Resources::parse(
      "cpus(role1):2;mem(role1):4;disk(role1):8").get();
  Resources role2 = Resources::parse(
      "cpus(role2):4;mem(role2):8;disk(role2):6").get();

  Resources resources = unreserved + role1 + role2;

  hashmap<string, Resources> reserved = resources.reservations();

  EXPECT_EQ(2u, reserved.size());
  EXPECT_EQ(role1, reserved["role1"]);
  EXPECT_EQ(role2, reserved["role2"]);

  EXPECT_EQ(role1, resources.reserved("role1"));
  EXPECT_EQ(role2, resources.reserved("role2"));

  // Resources with role "*" are not considered reserved.
  EXPECT_EQ(Resources(), resources.reserved("*"));

  EXPECT_EQ(unreserved, resources.unreserved());

  EXPECT_EQ(role1 + role2, resources.reserved());
}


// This test verifies that we can get all resources allocatable to a role,
// including reservations of itself, its ancestors, and unreserved resources.
TEST(ResourcesTest, HierarchicalReservations)
{
  Resources unreserved = Resources::parse(
      "cpus:1;mem:2;disk:4").get();
  Resources grandfather = Resources::parse(
      "cpus(a):2;mem(a):4;disk(a):8").get();
  Resources father = Resources::parse(
      "cpus(a/bx):4;mem(a/bx):8;disk(a/bx):16").get();
  Resources uncle = Resources::parse(
      "cpus(a/b):4;mem(a/b):8;disk(a/b):16").get();
  Resources child = Resources::parse(
      "cpus(a/bx/c):8;mem(a/bx/c):16;disk(a/bx/c):32").get();

  Resources family = unreserved + grandfather + father + uncle + child;

  EXPECT_EQ(unreserved, family.allocatableTo("*"));

  EXPECT_EQ(grandfather + unreserved, family.allocatableTo("a"));

  EXPECT_EQ(grandfather + father + unreserved, family.allocatableTo("a/bx"));

  EXPECT_EQ(grandfather + father + child + unreserved,
            family.allocatableTo("a/bx/c"));
}




TEST(ResourcesTest, ToUnreserved)
{
  Resource cpus1 = Resources::parse("cpus", "1", "role1").get();
  Resource cpus2 = Resources::parse("cpus", "2", "role2").get();
  Resource mem1 = Resources::parse("mem", "5", "role1").get();

  Resources r;
  r += cpus1;
  r += cpus2;
  r += mem1;

  EXPECT_EQ(r.toUnreserved(), Resources::parse("cpus:3;mem:5").get());
}




TEST(ReservedResourcesTest, Validation)
{
  // Unreserved.
  EXPECT_NONE(Resources::validate(createReservedResource("cpus", "8")));

  // Statically reserved to "role".
  EXPECT_NONE(Resources::validate(createReservedResource(
      "cpus", "8", createStaticReservationInfo("role"))));

  // Dynamically reserved without labels.
  EXPECT_NONE(Resources::validate(createReservedResource(
      "cpus", "8", createDynamicReservationInfo("role", "principal2"))));

  // Dynamically reserved with labels.
  Labels labels;
  labels.add_labels()->CopyFrom(createLabel("foo", "bar"));
  EXPECT_NONE(Resources::validate(createReservedResource(
      "cpus",
      "8",
      createDynamicReservationInfo("role", "principal2", labels))));
}


TEST(ReservedResourcesTest, Equals)
{
  Labels labels1;
  labels1.add_labels()->CopyFrom(createLabel("foo", "bar"));

  Labels labels2;
  labels2.add_labels()->CopyFrom(createLabel("foo", "baz"));

  vector<Resources> unique = {
    // Unreserved.
    createReservedResource("cpus", "8"),
    // Statically reserved for role.
    createReservedResource("cpus", "8", createStaticReservationInfo("role1")),
    createReservedResource("cpus", "8", createStaticReservationInfo("role2")),
    // Dynamically reserved for role.
    createReservedResource(
        "cpus", "8", createDynamicReservationInfo("role1", "principal1")),
    createReservedResource(
        "cpus", "8", createDynamicReservationInfo("role1", "principal2")),
    createReservedResource(
        "cpus", "8", createDynamicReservationInfo("role2", "principal1")),
    createReservedResource(
        "cpus", "8", createDynamicReservationInfo("role2", "principal2")),
    // Dynamically reserved with labels.
    createReservedResource(
        "cpus",
        "8",
        createDynamicReservationInfo("role1", "principal2", labels1)),
    createReservedResource(
        "cpus",
        "8",
        createDynamicReservationInfo("role1", "principal2", labels2))};

  // Test that all resources in 'unique' are considered different.
  foreach (const Resources& left, unique) {
    foreach (const Resources& right, unique) {
      if (&left == &right) {
        continue;
      }
      EXPECT_NE(left, right);
    }
  }
}


TEST(ReservedResourcesTest, AdditionStaticallyReserved)
{
  Resources left =
    createReservedResource("cpus", "8", createStaticReservationInfo("role"));
  Resources right =
    createReservedResource("cpus", "4", createStaticReservationInfo("role"));
  Resources expected =
    createReservedResource("cpus", "12", createStaticReservationInfo("role"));

  EXPECT_EQ(expected, left + right);

  // Test operator+ with rvalue references.
  EXPECT_EQ(expected, Resources(left) + right);
  EXPECT_EQ(expected, left + Resources(right));
  EXPECT_EQ(expected, Resources(left) + Resources(right));
}


TEST(ReservedResourcesTest, AdditionDynamicallyReservedWithoutLabels)
{
  Resource::ReservationInfo reservation =
    createDynamicReservationInfo("role", "principal");

  Resources left = createReservedResource("cpus", "8", reservation);
  Resources right = createReservedResource("cpus", "4", reservation);
  Resources expected = createReservedResource("cpus", "12", reservation);

  EXPECT_EQ(expected, left + right);

  // Test operator+ with rvalue references.
  EXPECT_EQ(expected, left + Resources(right));
  EXPECT_EQ(expected, Resources(left) + right);
  EXPECT_EQ(expected, Resources(left) + Resources(right));
}


TEST(ReservedResourcesTest, AdditionDynamicallyReservedWithSameLabels)
{
  Labels labels;
  labels.add_labels()->CopyFrom(createLabel("foo", "bar"));

  Resource::ReservationInfo reservation =
    createDynamicReservationInfo("role", "principal", labels);

  Resources left = createReservedResource("cpus", "8", reservation);
  Resources right = createReservedResource("cpus", "4", reservation);
  Resources expected = createReservedResource("cpus", "12", reservation);

  EXPECT_EQ(expected, left + right);

  // Test operator+ with rvalue references.
  EXPECT_EQ(expected, left + Resources(right));
  EXPECT_EQ(expected, Resources(left) + right);
  EXPECT_EQ(expected, Resources(left) + Resources(right));
}


TEST(ReservedResourcesTest, AdditionDynamicallyReservedWithDistinctLabels)
{
  Labels labels1;
  Labels labels2;

  labels1.add_labels()->CopyFrom(createLabel("foo", "bar"));
  labels2.add_labels()->CopyFrom(createLabel("foo", "baz"));

  Resource::ReservationInfo reservation1 =
    createDynamicReservationInfo("role", "principal", labels1);
  Resource::ReservationInfo reservation2 =
    createDynamicReservationInfo("role", "principal", labels2);

  Resources r1 = createReservedResource("cpus", "6", reservation1);
  Resources r2 = createReservedResource("cpus", "6", reservation2);
  Resources sum = r1 + r2;

  EXPECT_EQ(2u, sum.size());
  EXPECT_FALSE(sum == r1 + r1);
  EXPECT_FALSE(sum == r2 + r2);

  // Test operator+ with rvalue references.
  Resources sum1 = Resources(r1) + r2;
  Resources sum2 = r1 + Resources(r2);
  Resources sum3 = Resources(r1) + Resources(r2);

  EXPECT_EQ(sum, sum1);
  EXPECT_EQ(sum, sum2);
  EXPECT_EQ(sum, sum3);
}


TEST(ReservedResourcesTest, Subtraction)
{
  Labels labels1;
  Labels labels2;

  labels1.add_labels()->CopyFrom(createLabel("foo", "bar"));
  labels2.add_labels()->CopyFrom(createLabel("foo", "baz"));

  Resource::ReservationInfo reservation1 =
    createDynamicReservationInfo("role", "principal", labels1);

  Resource::ReservationInfo reservation2 =
    createDynamicReservationInfo("role", "principal", labels2);

  Resources r1 =
    createReservedResource("cpus", "8", createStaticReservationInfo("role"));

  Resources r2 = createReservedResource("cpus", "8", reservation1);

  EXPECT_TRUE((r1 - r1).empty());
  EXPECT_TRUE((r2 - r2).empty());
  EXPECT_FALSE((r2 - r1).empty());
  EXPECT_FALSE((r1 - r2).empty());
  EXPECT_EQ(r1, r1 - r2);
  EXPECT_EQ(r2, r2 - r1);

  Resources total = r1 + r2;

  Resources r3 =
    createReservedResource("cpus", "6", createStaticReservationInfo("role"));
  Resources r4 = createReservedResource("cpus", "4", reservation1);

  Resources expected = r3 + r4;

  Resources r5 =
    createReservedResource("cpus", "2", createStaticReservationInfo("role"));
  Resources r6 = createReservedResource("cpus", "4", reservation1);

  EXPECT_EQ(expected, total - r5 - r6);

  // Distinct labels
  Resources r7 = createReservedResource("cpus", "8", reservation1);
  Resources r8 = createReservedResource("cpus", "8", reservation2);

  EXPECT_FALSE((r2 - r1).empty());
  EXPECT_FALSE((r1 - r2).empty());
  EXPECT_EQ(r1, r1 - r2);
  EXPECT_EQ(r2, r2 - r1);
}


TEST(ReservedResourcesTest, Contains)
{
  Resources r1 =
    createReservedResource("cpus", "8", createStaticReservationInfo("role"));

  Resources r2 = createReservedResource(
      "cpus", "12", createDynamicReservationInfo("role", "principal"));

  EXPECT_TRUE(r1.contains(r1));
  EXPECT_TRUE(r2.contains(r2));

  EXPECT_FALSE(r1.contains(r2));
  EXPECT_FALSE(r2.contains(r1));

  EXPECT_FALSE(r1.contains(r1 + r2));
  EXPECT_FALSE(r2.contains(r1 + r2));
  EXPECT_TRUE((r1 + r2).contains(r1));
  EXPECT_TRUE((r1 + r2).contains(r2));
  EXPECT_TRUE((r1 + r2).contains(r1 + r2));
}




TEST(ResourcesOperationTest, ReserveResources)
{
  Resources unreservedCpus = Resources::parse("cpus:1").get();
  Resources unreservedMem = Resources::parse("mem:512").get();

  Resources unreserved = unreservedCpus + unreservedMem;

  Resources reservedCpus1 = unreservedCpus.pushReservation(
      createDynamicReservationInfo("role", "principal"));

  EXPECT_SOME_EQ(unreservedMem + reservedCpus1,
                 unreserved.apply(RESERVE(reservedCpus1)));

  // Check the case of insufficient unreserved resources.
  Resources reservedCpus2 = createReservedResource(
      "cpus", "2", createDynamicReservationInfo("role", "principal"));

  EXPECT_ERROR(unreserved.apply(RESERVE(reservedCpus2)));
}


TEST(ResourcesOperationTest, UnreserveResources)
{
  Resources reservedCpus = createReservedResource(
      "cpus", "1", createDynamicReservationInfo("role", "principal"));

  Resources reservedMem = createReservedResource(
      "mem", "512", createDynamicReservationInfo("role", "principal"));

  Resources reserved = reservedCpus + reservedMem;

  Resources unreservedCpus1 = reservedCpus.toUnreserved();

  EXPECT_SOME_EQ(reservedMem + unreservedCpus1,
                 reserved.apply(UNRESERVE(reservedCpus)));

  // Check the case of insufficient unreserved resources.
  Resource reservedCpus2 = createReservedResource(
      "cpus", "2", createDynamicReservationInfo("role", "principal"));

  EXPECT_ERROR(reserved.apply(UNRESERVE(reservedCpus2)));
}


TEST(ResourcesOperationTest, CreatePersistentVolume)
{
  Resources total = Resources::parse("cpus:1;mem:512;disk(role):1000").get();

  Resource volume1 = createDiskResource("200", "role", "1", "path");

  Offer::Operation create1;
  create1.set_type(Offer::Operation::CREATE);
  create1.mutable_create()->add_volumes()->CopyFrom(volume1);

  EXPECT_SOME_EQ(
      Resources::parse("cpus:1;mem:512;disk(role):800").get() + volume1,
      total.apply(create1));

  // Check the case of insufficient disk resources.
  Resource volume2 = createDiskResource("2000", "role", "1", "path");

  Offer::Operation create2;
  create2.set_type(Offer::Operation::CREATE);
  create2.mutable_create()->add_volumes()->CopyFrom(volume2);

  EXPECT_ERROR(total.apply(create2));
}


TEST(ResourcesOperationTest, StrippedResourcesVolume)
{
  Resources volume = createDiskResource("200", "role", "1", "path");
  Resources stripped = volume.createStrippedScalarQuantity();

  EXPECT_TRUE(stripped.persistentVolumes().empty());
  EXPECT_TRUE(stripped.reserved().empty());
  EXPECT_EQ(Megabytes(200), stripped.disk().get());

  Resource strippedVolume = *(stripped.begin());

  ASSERT_EQ(Value::SCALAR, strippedVolume.type());
  EXPECT_DOUBLE_EQ(200, strippedVolume.scalar().value());
  EXPECT_EQ("disk", strippedVolume.name());
  EXPECT_FALSE(strippedVolume.has_disk());
  EXPECT_FALSE(Resources::isPersistentVolume(strippedVolume));
}


TEST(ResourcesOperationTest, StrippedResourcesAllocated)
{
  Resources allocated = Resources::parse("cpus:1;mem:512").get();
  allocated.allocate("role");

  Resources stripped = allocated.createStrippedScalarQuantity();

  // Allocation info should be stripped when
  // converting to a quantity.
  foreach (const Resource& resource, stripped) {
    EXPECT_FALSE(resource.has_allocation_info());
  }
}


TEST(ResourcesOperationTest, StrippedResourcesReserved)
{
  Resources unreserved = Resources::parse("cpus:1;mem:512").get();
  Resources dynamicallyReserved = unreserved.pushReservation(
      createDynamicReservationInfo("role", "principal"));

  Resources stripped = dynamicallyReserved.createStrippedScalarQuantity();

  EXPECT_TRUE(stripped.reserved("role").empty());

  foreach (const Resource& resource, stripped) {
    EXPECT_FALSE(Resources::isDynamicallyReserved(resource));
    EXPECT_TRUE(Resources::isUnreserved(resource));
  }
}




TEST(ResourceFormatTest, PreReservationRefinement)
{
  Resource resource;
  resource.set_name("cpus");
  resource.set_type(Value::SCALAR);
  resource.mutable_scalar()->set_value(555.5);

  // Unreserved resource.
  EXPECT_NONE(Resources::validate(resource));

  resource.set_role("*");
  EXPECT_NONE(Resources::validate(resource));

  // Statically reserved resource.
  resource.set_role("foo");
  EXPECT_NONE(Resources::validate(resource));

  // Dynamically reserved resource.
  Resource::ReservationInfo* reservation = resource.mutable_reservation();
  reservation->set_principal("principal1");

  EXPECT_NONE(Resources::validate(resource));
}


// This test checks that the resources in the "post-reservation-refinement"
// format are valid. In the "post-reservation-refinement" format,
// the reservation state is represented by the `Resource.reservations` field.
TEST(ResourceFormatTest, PostReservationRefinement)
{
  Resource resource;
  resource.set_name("cpus");
  resource.set_type(Value::SCALAR);
  resource.mutable_scalar()->set_value(555.5);

  // Unreserved resource.
  EXPECT_NONE(Resources::validate(resource));

  Resource::ReservationInfo* reservation = resource.add_reservations();
  reservation->set_role("foo");
  reservation->set_principal("principal1");

  // Statically reserved resource.
  reservation->set_type(Resource::ReservationInfo::STATIC);
  EXPECT_NONE(Resources::validate(resource));

  // Dynamically reserved resource.
  reservation->set_type(Resource::ReservationInfo::DYNAMIC);
  EXPECT_NONE(Resources::validate(resource));

  // Refined static reservation is invalid.
  reservation->set_type(Resource::ReservationInfo::STATIC);
  Resource::ReservationInfo* refinedReservation = resource.add_reservations();
  refinedReservation->set_type(Resource::ReservationInfo::STATIC);
  refinedReservation->set_role("foo/bar");
  refinedReservation->set_principal("principal2");
  EXPECT_SOME(Resources::validate(resource));

  // Refined dynamic reservation on top of static reservation.
  refinedReservation->set_type(Resource::ReservationInfo::DYNAMIC);
  EXPECT_NONE(Resources::validate(resource));

  // Refined dynamic reservation on top of dynamic reservation.
  reservation->set_type(Resource::ReservationInfo::DYNAMIC);
  EXPECT_NONE(Resources::validate(resource));
}


// This test checks that the resources in the "endpoint" format are valid.
// In the "endpoint" format, both the fields for both pre-refinement and
// post-refinement reservations are set, but they must be set to mutually
// equivalent values.
TEST(ResourceFormatTest, Endpoint)
{
  Resource resource;
  resource.set_name("cpus");
  resource.set_type(Value::SCALAR);
  resource.mutable_scalar()->set_value(555.5);
  resource.set_role("r1");

  // Dynamically reserved, pre-refinement format.
  Resource::ReservationInfo* unrefinedReservation =
    resource.mutable_reservation();
  unrefinedReservation->set_principal("principal1");

  // Set `reservations` field as well (post-refinement format).
  Resource::ReservationInfo* refinedReservation = resource.add_reservations();
  refinedReservation->set_type(Resource::ReservationInfo::DYNAMIC);
  refinedReservation->set_role("r1");
  refinedReservation->set_principal("principal1");

  // Should validate now that all fields are set to equivalent values.
  EXPECT_NONE(Resources::validate(resource));

  // Should not validate if pre- and post-refinement fields are inconsistent.
  {
    refinedReservation->set_role("r2");
    EXPECT_SOME(Resources::validate(resource));
    refinedReservation->set_role("r1");

    refinedReservation->set_type(Resource::ReservationInfo::STATIC);
    EXPECT_SOME(Resources::validate(resource));
    refinedReservation->set_type(Resource::ReservationInfo::DYNAMIC);

    unrefinedReservation->set_principal("principal2");
    EXPECT_SOME(Resources::validate(resource));
    unrefinedReservation->set_principal("principal1");

    // Sanity check that the resource is still valid.
    EXPECT_NONE(Resources::validate(resource));
  }

  // Should not validate if the post-refinement format contains a
  // reservation refinement.
  Resource::ReservationInfo* refinedReservation2 = resource.add_reservations();
  refinedReservation2->set_type(Resource::ReservationInfo::DYNAMIC);
  refinedReservation2->set_role("r1/r2");
  refinedReservation2->set_principal("principal1");

  EXPECT_SOME(Resources::validate(resource));
}


TEST(ResourceFormatTest, DowngradeWithoutResources)
{
  FrameworkInfo frameworkInfo = DEFAULT_FRAMEWORK_INFO;
  EXPECT_SOME(downgradeResources(&frameworkInfo));
  EXPECT_EQ(
      DEFAULT_FRAMEWORK_INFO.SerializeAsString(),
      frameworkInfo.SerializeAsString());
}


TEST(ResourceFormatTest, DowngradeWithResourcesWithoutRefinedReservations)
{
  NodeID nodeId;
  nodeId.set_value("node");

  TaskInfo actual;
  {
    actual.set_name("task");
  actual.set_principal(DEFAULT_TASK_PRINCIPAL);
    actual.mutable_task_id()->set_value("task_id");
    actual.mutable_node_id()->CopyFrom(nodeId);

    Resource resource;
    resource.set_name("cpus");
    resource.set_type(Value::SCALAR);
    resource.mutable_scalar()->set_value(555.5);

    // Add "post-reservation-refinement" resources.

    // Unreserved resource.
    actual.add_resources()->CopyFrom(resource);

    Resource::ReservationInfo* reservation = resource.add_reservations();

    // Statically reserved resource.
    reservation->set_type(Resource::ReservationInfo::STATIC);
    reservation->set_role("foo");
    actual.add_resources()->CopyFrom(resource);

    // Dynamically reserved resource.
    reservation->set_type(Resource::ReservationInfo::DYNAMIC);
    reservation->set_role("bar");
    reservation->set_principal("principal1");
    actual.add_resources()->CopyFrom(resource);
  }

  TaskInfo expected;
  {
    expected.set_name("task");
  expected.set_principal(DEFAULT_TASK_PRINCIPAL);
    expected.mutable_task_id()->set_value("task_id");
    expected.mutable_node_id()->CopyFrom(nodeId);

    Resource resource;
    resource.set_name("cpus");
    resource.set_type(Value::SCALAR);
    resource.mutable_scalar()->set_value(555.5);

    // Add "pre-reservation-refinement" resources.

    // Unreserved resource.
    resource.set_role("*");
    expected.add_resources()->CopyFrom(resource);

    // Statically reserved resource.
    resource.set_role("foo");
    expected.add_resources()->CopyFrom(resource);

    // Dynamically reserved resource.
    resource.set_role("bar");
    Resource::ReservationInfo* reservation = resource.mutable_reservation();
    reservation->set_principal("principal1");
    expected.add_resources()->CopyFrom(resource);
  }

  EXPECT_SOME(downgradeResources(&actual));
  EXPECT_EQ(expected, actual);
}


TEST(ResourceFormatTest, DowngradeWithResourcesWithRefinedReservations)
{
  NodeID nodeId;
  nodeId.set_value("node");

  TaskInfo actual;
  {
    actual.set_name("task");
  actual.set_principal(DEFAULT_TASK_PRINCIPAL);
    actual.mutable_task_id()->set_value("task_id");
    actual.mutable_node_id()->CopyFrom(nodeId);

    Resource resource;
    resource.set_name("cpus");
    resource.set_type(Value::SCALAR);
    resource.mutable_scalar()->set_value(555.5);

    // Add "post-reservation-refinement" resources.

    // Unreserved resource.
    actual.add_resources()->CopyFrom(resource);

    Resource::ReservationInfo* reservation = resource.add_reservations();

    // Statically reserved resource.
    reservation->set_type(Resource::ReservationInfo::STATIC);
    reservation->set_role("foo");
    actual.add_resources()->CopyFrom(resource);

    // Dynamically reserved resource.
    reservation->set_type(Resource::ReservationInfo::DYNAMIC);
    reservation->set_role("bar");
    reservation->set_principal("principal1");
    actual.add_resources()->CopyFrom(resource);

    // Dynamically refined reservation on top of dynamic reservation.
    Resource::ReservationInfo* refinedReservation = resource.add_reservations();
    refinedReservation->set_type(Resource::ReservationInfo::DYNAMIC);
    refinedReservation->set_role("bar/baz");
    refinedReservation->set_principal("principal2");
    actual.add_resources()->CopyFrom(resource);
  }

  TaskInfo expected;
  {
    expected.set_name("task");
  expected.set_principal(DEFAULT_TASK_PRINCIPAL);
    expected.mutable_task_id()->set_value("task_id");
    expected.mutable_node_id()->CopyFrom(nodeId);

    Resource resource;
    resource.set_name("cpus");
    resource.set_type(Value::SCALAR);
    resource.mutable_scalar()->set_value(555.5);

    // Add "pre-reservation-refinement" resources.

    // Unreserved resource.
    resource.set_role("*");
    expected.add_resources()->CopyFrom(resource);

    // Statically reserved resource.
    resource.set_role("foo");
    expected.add_resources()->CopyFrom(resource);

    // Dynamically reserved resource.
    resource.set_role("bar");
    Resource::ReservationInfo* reservation = resource.mutable_reservation();
    reservation->set_principal("principal1");
    expected.add_resources()->CopyFrom(resource);

    // Add non-downgradable resources. Note that the non-downgradable
    // resources remain in "post-reservation-refinement" format.

    // Dynamically refined reservation on top of dynamic reservation.
    resource.clear_role();
    resource.clear_reservation();

    Resource::ReservationInfo* dynamicReservation = resource.add_reservations();
    dynamicReservation->set_type(Resource::ReservationInfo::DYNAMIC);
    dynamicReservation->set_role("bar");
    dynamicReservation->set_principal("principal1");

    Resource::ReservationInfo* refinedReservation = resource.add_reservations();
    refinedReservation->set_type(Resource::ReservationInfo::DYNAMIC);
    refinedReservation->set_role("bar/baz");
    refinedReservation->set_principal("principal2");

    expected.add_resources()->CopyFrom(resource);
  }

  EXPECT_ERROR(downgradeResources(&actual));
  EXPECT_EQ(expected, actual);
}




TEST(ResourcesTest, ReservationAncestor)
{
  Resources baseResources = *Resources::parse("cpus:2;mem:10");

  EXPECT_EQ(
      baseResources,
      Resources::getReservationAncestor(baseResources, baseResources));

  Resources resources1 = baseResources.pushReservation(
      createDynamicReservationInfo("foo"));

  EXPECT_EQ(
      resources1,
      Resources::getReservationAncestor(resources1, resources1));

  EXPECT_EQ(
      baseResources,
      Resources::getReservationAncestor(resources1, baseResources));

  EXPECT_EQ(
      baseResources,
      Resources::getReservationAncestor(baseResources, resources1));

  resources1 = resources1.pushReservation(
      createDynamicReservationInfo("foo/bar"));

  EXPECT_EQ(
      resources1,
      Resources::getReservationAncestor(resources1, resources1));

  EXPECT_EQ(
      baseResources,
      Resources::getReservationAncestor(resources1, baseResources));

  EXPECT_EQ(
      baseResources,
      Resources::getReservationAncestor(baseResources, resources1));

  Resources resources2 = baseResources.pushReservation(
      createDynamicReservationInfo("foo"));

  EXPECT_EQ(
      resources2,
      Resources::getReservationAncestor(resources1, resources2));

  EXPECT_EQ(
      resources2,
      Resources::getReservationAncestor(resources2, resources1));

  EXPECT_NE(
      resources1,
      Resources::getReservationAncestor(baseResources, resources2));

  EXPECT_EQ(
      baseResources,
      Resources::getReservationAncestor(baseResources, resources2));

  Resources resources3 = resources2.pushReservation(
      createDynamicReservationInfo("foo/baz"));

  EXPECT_EQ(
      resources2,
      Resources::getReservationAncestor(resources1, resources3));
}

} // namespace tests {
} // namespace internal {
} // namespace medios {
