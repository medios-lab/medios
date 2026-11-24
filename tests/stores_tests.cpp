// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

// The stores file a node declares its stores in (DR-036): what it accepts,
// what it refuses by name, and the resource it becomes.

#include <unistd.h>

#include <map>
#include <string>
#include <vector>

#include <gtest/gtest.h>

#include <medios/medios.hpp>
#include <medios/resources.hpp>

#include <medios/v1/resources.hpp>

#include <stout/foreach.hpp>
#include <stout/gtest.hpp>
#include <stout/os.hpp>
#include <stout/path.hpp>
#include <stout/strings.hpp>
#include <stout/try.hpp>

#include <stout/tests/utils.hpp>

#include "node/compatibility.hpp"
#include "node/stores.hpp"

using std::map;
using std::string;
using std::vector;

namespace medios {
namespace internal {
namespace tests {

using node::StoreDeclaration;


static string storesFile(const string& entries)
{
  return "{ \"stores\": [" + entries + "] }";
}


static string entry(
    const string& name,
    const string& path,
    const string& storeClass,
    const string& locality,
    const string& readers,
    const string& writers)
{
  return "{ \"name\": \"" + name + "\", \"path\": \"" + path +
         "\", \"class\": \"" + storeClass + "\", \"locality\": \"" +
         locality + "\", \"readers\": " + readers + ", \"writers\": " +
         writers + " }";
}


const string TABICL = entry(
    "tabicl-tables", "/var/lib/medios-stores/tabicl", "local", "near",
    "4", "0");

const string RESULTS = entry(
    "results", "/var/lib/medios-results", "shared", "near", "10", "10");


TEST(StoresTest, ParsesStoreDeclarations)
{
  Try<vector<StoreDeclaration>> stores =
    node::parseStores(storesFile(TABICL + "," + RESULTS));

  ASSERT_SOME(stores);
  ASSERT_EQ(2u, stores->size());

  EXPECT_EQ("tabicl-tables", stores->at(0).name);
  EXPECT_EQ("/var/lib/medios-stores/tabicl", stores->at(0).path);
  EXPECT_EQ("local", stores->at(0).storeClass);
  EXPECT_EQ("near", stores->at(0).locality);
  EXPECT_EQ(4u, stores->at(0).readers);
  EXPECT_EQ(0u, stores->at(0).writers);

  EXPECT_EQ("results", stores->at(1).name);
  EXPECT_EQ(10u, stores->at(1).readers);
  EXPECT_EQ(10u, stores->at(1).writers);
}


// Each refusal names what was wrong, so the operator reads the file once.
TEST(StoresTest, RefusesByName)
{
  Try<vector<StoreDeclaration>> stores = node::parseStores("[]");
  ASSERT_ERROR(stores);
  EXPECT_TRUE(strings::contains(stores.error(), "not a JSON object"));

  stores = node::parseStores("{}");
  ASSERT_ERROR(stores);
  EXPECT_TRUE(strings::contains(stores.error(), "no 'stores' array"));

  stores = node::parseStores(storesFile(""));
  ASSERT_ERROR(stores);
  EXPECT_TRUE(strings::contains(stores.error(), "declares no stores"));

  stores = node::parseStores(storesFile(
      entry("a/b", "/x", "local", "near", "1", "0")));
  ASSERT_ERROR(stores);
  EXPECT_TRUE(strings::contains(stores.error(), "A-Za-z0-9._-"));

  stores = node::parseStores(storesFile(
      entry("a", "relative", "local", "near", "1", "0")));
  ASSERT_ERROR(stores);
  EXPECT_TRUE(strings::contains(stores.error(), "absolute"));

  stores = node::parseStores(storesFile(
      entry("a", "/x", "object", "near", "1", "0")));
  ASSERT_ERROR(stores);
  EXPECT_TRUE(strings::contains(stores.error(), "class 'object'"));

  stores = node::parseStores(storesFile(
      entry("a", "/x", "nfs", "near", "1", "0")));
  ASSERT_ERROR(stores);
  EXPECT_TRUE(strings::contains(stores.error(), "'local' or 'shared'"));

  stores = node::parseStores(storesFile(
      entry("a", "/x", "local", "far", "1", "0")));
  ASSERT_ERROR(stores);
  EXPECT_TRUE(strings::contains(stores.error(), "'near', 'same-site'"));

  stores = node::parseStores(storesFile(
      entry("a", "/x", "local", "near", "1.5", "0")));
  ASSERT_ERROR(stores);
  EXPECT_TRUE(strings::contains(stores.error(), "whole number"));

  stores = node::parseStores(storesFile(
      entry("a", "/x", "local", "near", "-1", "0")));
  ASSERT_ERROR(stores);
  EXPECT_TRUE(strings::contains(stores.error(), "whole number"));

  stores = node::parseStores(storesFile(
      entry("a", "/x", "local", "near", "0", "0")));
  ASSERT_ERROR(stores);
  EXPECT_TRUE(strings::contains(stores.error(), "0 readers and 0 writers"));

  stores = node::parseStores(storesFile(TABICL + "," + TABICL));
  ASSERT_ERROR(stores);
  EXPECT_TRUE(strings::contains(stores.error(), "more than once"));

  stores = node::parseStores(storesFile(
      "{ \"name\": \"a\", \"path\": \"/x\", \"class\": \"local\", "
      "\"locality\": \"near\", \"readers\": 1 }"));
  ASSERT_ERROR(stores);
  EXPECT_TRUE(strings::contains(stores.error(), "no 'writers'"));
}


// One shared scalar per store, named store/<name>, carrying the
// declaration as metadata, and accepted by core's resource validation
// as the node's total would be.
TEST(StoresTest, BecomesOneSharedScalarPerStore)
{
  Try<vector<StoreDeclaration>> stores =
    node::parseStores(storesFile(TABICL + "," + RESULTS));
  ASSERT_SOME(stores);

  Resources resources = node::storeResources(*stores, "*");

  EXPECT_NONE(Resources::validate(resources));
  EXPECT_EQ(2u, resources.size());

  Option<Value::Scalar> tabicl =
    resources.get<Value::Scalar>("store/tabicl-tables");
  ASSERT_SOME(tabicl);
  EXPECT_EQ(1, tabicl->value());

  foreach (const Resource& resource, resources) {
    EXPECT_TRUE(Resources::isShared(resource)) << resource;
    EXPECT_TRUE(Resources::isUnreserved(resource)) << resource;

    Try<StoreDeclaration> store = node::storeOf(resource);
    ASSERT_SOME(store);

    if (store->name == "tabicl-tables") {
      EXPECT_EQ("local", store->storeClass);
      EXPECT_EQ("near", store->locality);
      EXPECT_EQ(4u, store->readers);
      EXPECT_EQ(0u, store->writers);
    } else {
      EXPECT_EQ("results", store->name);
      EXPECT_EQ("shared", store->storeClass);
      EXPECT_EQ(10u, store->readers);
      EXPECT_EQ(10u, store->writers);
    }
  }

  // The resource name round-trips to the store name, and a name without
  // the prefix is not a store.
  EXPECT_SOME_EQ(string("results"), node::storeNameOf("store/results"));
  EXPECT_NONE(node::storeNameOf("cpus"));
  EXPECT_NONE(node::storeNameOf("stores/results"));
  EXPECT_EQ("store/results", node::storeResourceName("results"));

  // A plain scalar with metadata is not a store, and a store resource
  // some other code built without the counts is refused.
  Resource cpus = Resources::parse("cpus", "1", "*").get();
  EXPECT_ERROR(node::storeOf(cpus));

  Resource bare;
  bare.set_name("store/x");
  bare.set_type(Value::SCALAR);
  bare.mutable_scalar()->set_value(1);
  bare.mutable_shared();
  EXPECT_ERROR(node::storeOf(bare));
}


// A shared scalar adds as a holder count, never as a quantity: two copies
// of the same store are one resource held twice, which is what lets a
// task's copy be subtracted at its end while the node's copy stays.
TEST(StoresTest, SharedCopiesCountHolders)
{
  Try<vector<StoreDeclaration>> stores = node::parseStores(storesFile(RESULTS));
  ASSERT_SOME(stores);

  Resources one = node::storeResources(*stores, "*");
  Resources two = one + one;

  EXPECT_EQ(1u, two.size());
  EXPECT_EQ(2u, two.count(*one.begin()));
  EXPECT_TRUE(two.contains(one));
  EXPECT_EQ(one, two - one);
  EXPECT_TRUE((one - one).empty());
}


// Core refuses what it refused before for everything that is not a scalar
// or a persistent volume.
TEST(StoresTest, SharedIsForScalarsAndPersistentVolumes)
{
  Resource ports = Resources::parse("ports", "[1-2]", "*").get();
  ports.mutable_shared();
  EXPECT_SOME(Resources::validate(ports));

  Resource disk = Resources::parse("disk", "10", "*").get();
  disk.mutable_shared();
  EXPECT_SOME(Resources::validate(disk));
}


// The v1 API, which C++ frameworks such as medios-execute use, takes a
// store as the internal API does. A v1 `Resources` drops a resource that
// fails validation, so a refused store would vanish from the offer and
// from the task rather than fail.
TEST(StoresTest, TheV1APIKeepsASharedScalar)
{
  medios::v1::Resource store =
    medios::v1::Resources::parse("store/results", "1", "*").get();
  store.mutable_shared();
  EXPECT_NONE(medios::v1::Resources::validate(store));

  const medios::v1::Resources task(store);
  ASSERT_EQ(1u, task.size());

  const medios::v1::Resources offered =
    medios::v1::Resources::parse("cpus:1;mem:64").get() + task;
  EXPECT_TRUE(offered.contains(task));
  EXPECT_EQ(2u, (offered + task).count(store));

  medios::v1::Resource ports =
    medios::v1::Resources::parse("ports", "[1-2]", "*").get();
  ports.mutable_shared();
  EXPECT_SOME(medios::v1::Resources::validate(ports));

  medios::v1::Resource disk =
    medios::v1::Resources::parse("disk", "10", "*").get();
  disk.mutable_shared();
  EXPECT_SOME(medios::v1::Resources::validate(disk));
}


class StoresFileTest : public TemporaryDirectoryTest {};


// The start-time check refuses a declared path the host does not have,
// naming the store; a file whose paths exist is read back by name.
TEST_F(StoresFileTest, ChecksDeclaredPathsAndReadsByName)
{
  const string present = path::join(sandbox.get(), "present");
  ASSERT_SOME(os::mkdir(present));

  const string file = path::join(sandbox.get(), "stores.json");
  ASSERT_SOME(os::write(file, storesFile(
      entry("here", present, "local", "near", "2", "1") + "," +
      entry("gone", path::join(sandbox.get(), "gone"), "shared", "remote",
            "1", "1"))));

  Try<map<string, StoreDeclaration>> stores = node::readStores(file);
  ASSERT_SOME(stores);
  EXPECT_EQ(2u, stores->size());
  EXPECT_EQ(present, stores->at("here").path);

  vector<StoreDeclaration> declared;
  foreachvalue (const StoreDeclaration& store, *stores) {
    declared.push_back(store);
  }

  Try<Nothing> checked = node::checkStores(declared);
  ASSERT_ERROR(checked);
  EXPECT_TRUE(strings::contains(checked.error(), "'gone'"));
  EXPECT_TRUE(strings::contains(checked.error(), "does not exist"));

  EXPECT_SOME(node::checkStores({stores->at("here")}));

  EXPECT_ERROR(node::readStores(path::join(sandbox.get(), "missing.json")));
}


// A node that declares stores must have a containerizer that delivers
// one: the docker containerizer, or the medios containerizer with the
// linux launcher and the filesystem/linux isolator. Keyed on the flags,
// never on the platform.
TEST(StoresTest, DeliveryIsDecidedByTheContainerizerLauncherAndIsolation)
{
  EXPECT_TRUE(node::mediosDeliversStores("linux", "filesystem/linux"));
  EXPECT_TRUE(node::mediosDeliversStores(
      "linux", "cgroups/cpu,filesystem/linux,cgroups/mem"));

  // filesystem/posix links a store into the sandbox (medios#602), named
  // or as the default when no filesystem isolator is named.
  EXPECT_TRUE(node::mediosDeliversStores("posix", ""));
  EXPECT_TRUE(node::mediosDeliversStores("posix", "filesystem/posix"));
  EXPECT_TRUE(node::mediosDeliversStores("posix", "posix/cpu,posix/mem"));

  // A launcher paired with the other's filesystem isolator.
  EXPECT_FALSE(node::mediosDeliversStores("posix", "filesystem/linux"));
  EXPECT_FALSE(node::mediosDeliversStores("linux", "filesystem/posix"));
  EXPECT_FALSE(node::mediosDeliversStores("linux", ""));
  EXPECT_FALSE(node::mediosDeliversStores("linux", "cgroups/cpu,cgroups/mem"));

  // A Linux GPU node, a Docker node, a Mac with the quickstart flags.
  EXPECT_SOME(node::checkStoreDelivery(
      "medios", "linux",
      "filesystem/linux,docker/runtime,cgroups/devices,cgroups/cpu,"
      "cgroups/mem,com_medioslab_medios_gpu"));
  EXPECT_SOME(node::checkStoreDelivery("docker_shell", "posix", ""));
  EXPECT_SOME(node::checkStoreDelivery("docker_shell,medios", "posix", ""));
  EXPECT_SOME(node::checkStoreDelivery("medios", "posix", ""));

  Try<Nothing> refused =
    node::checkStoreDelivery("medios", "posix", "filesystem/linux");
  ASSERT_ERROR(refused);
  EXPECT_TRUE(strings::contains(
      refused.error(), "--containerizers is 'medios'"))
    << refused.error();
  EXPECT_TRUE(strings::contains(refused.error(), "--launcher is 'posix'"))
    << refused.error();
  EXPECT_TRUE(strings::contains(
      refused.error(), "--isolation is 'filesystem/linux'"))
    << refused.error();

  EXPECT_ERROR(node::checkStoreDelivery("medios", "linux", "cgroups/cpu"));
  EXPECT_ERROR(node::checkStoreDelivery("medios", "linux", "filesystem/posix"));
}


// One name inside the sandbox is the only relative container path a node
// places (DR-036 revision, medios#602).
TEST(StoresTest, ASandboxNameIsOneRelativeComponent)
{
  EXPECT_TRUE(node::isSandboxName("tables"));
  EXPECT_TRUE(node::isSandboxName(".cache"));
  EXPECT_TRUE(node::isSandboxName("..data"));

  EXPECT_FALSE(node::isSandboxName(""));
  EXPECT_FALSE(node::isSandboxName("."));
  EXPECT_FALSE(node::isSandboxName(".."));
  EXPECT_FALSE(node::isSandboxName("data/tables"));
  EXPECT_FALSE(node::isSandboxName("data/"));
  EXPECT_FALSE(node::isSandboxName("/data"));
}


// The shared resolver: a STORE volume on a declared store resolves to the
// declared path; any other source and an undeclared store are refused.
TEST(StoresTest, AStoreVolumeResolvesToTheDeclaredPath)
{
  StoreDeclaration tables;
  tables.name = "tables";
  tables.path = "/var/lib/tables";
  const map<string, StoreDeclaration> stores = {{"tables", tables}};

  Volume volume;
  volume.set_container_path("tables");
  volume.set_mode(Volume::RW);
  volume.mutable_source()->set_type(Volume::Source::STORE);
  volume.mutable_source()->mutable_store()->set_name("tables");

  EXPECT_SOME_EQ("/var/lib/tables", node::storePathOf(volume, stores));

  Try<string> undeclared = node::storePathOf(volume, {});
  ASSERT_ERROR(undeclared);
  EXPECT_TRUE(strings::contains(undeclared.error(), "declares no stores"))
    << undeclared.error();

  Volume bare;
  bare.set_container_path("/data");
  bare.set_host_path("/var/lib/data");
  bare.set_mode(Volume::RW);

  Try<string> host = node::storePathOf(bare, stores);
  ASSERT_ERROR(host);
  EXPECT_TRUE(strings::contains(host.error(), "HOST_PATH")) << host.error();
}


// A declared path this node cannot read is refused at start by name, as
// a missing one is: a node that advertised it would answer nothing.
TEST_F(StoresFileTest, RefusesAStoreItCannotRead)
{
  const string unreadable = path::join(sandbox.get(), "unreadable");
  ASSERT_SOME(os::mkdir(unreadable));
  ASSERT_SOME(os::chmod(unreadable, 0));

  StoreDeclaration store;
  store.name = "sealed";
  store.path = unreadable;
  store.storeClass = "local";
  store.locality = "near";
  store.readers = 1;
  store.writers = 0;

  Try<Nothing> checked = node::checkStores({store});

  // Root reads everything; the refusal is observable only without it.
  if (::geteuid() == 0) {
    EXPECT_SOME(checked);
  } else {
    ASSERT_ERROR(checked);
    EXPECT_TRUE(strings::contains(checked.error(), "'sealed'"))
      << checked.error();
    EXPECT_TRUE(strings::contains(checked.error(), unreadable))
      << checked.error();
    EXPECT_TRUE(strings::contains(checked.error(), "cannot read"))
      << checked.error();
  }

  ASSERT_SOME(os::chmod(unreadable, 0755));
}


static NodeInfo nodeWithStore(const string& readers, const string& writers)
{
  Try<vector<StoreDeclaration>> stores = node::parseStores(storesFile(entry(
      "results", "/var/lib/medios-results", "shared", "near",
      readers, writers)));

  CHECK_SOME(stores);

  NodeInfo info;
  info.set_hostname("host");
  info.set_port(5051);
  info.mutable_resources()->CopyFrom(
      Resources::parse("cpus:4;mem:1024").get() +
      node::storeResources(*stores, "*"));

  return info;
}


// Raising a store's counts allows more holders and takes nothing from a
// task that holds the store, so the additive policy accepts it.
TEST(StoresTest, AdditivePolicyAcceptsHigherCounts)
{
  EXPECT_SOME(node::compatibility::additive(
      nodeWithStore("2", "2"), nodeWithStore("2", "2")));

  EXPECT_SOME(node::compatibility::additive(
      nodeWithStore("2", "2"), nodeWithStore("4", "4")));

  EXPECT_SOME(node::compatibility::additive(
      nodeWithStore("2", "2"), nodeWithStore("2", "3")));
}


TEST(StoresTest, AdditivePolicyRefusesLowerCounts)
{
  Try<Nothing> lowered = node::compatibility::additive(
      nodeWithStore("4", "4"), nodeWithStore("4", "2"));

  ASSERT_ERROR(lowered);
  EXPECT_TRUE(strings::contains(lowered.error(), "'results' decreased"))
    << lowered.error();
}


TEST(StoresTest, AdditivePolicyRefusesAChangedClassOrARemovedStore)
{
  NodeInfo local = nodeWithStore("2", "2");

  Try<vector<StoreDeclaration>> stores = node::parseStores(storesFile(entry(
      "results", "/var/lib/medios-results", "local", "near", "4", "4")));
  ASSERT_SOME(stores);

  NodeInfo changed = local;
  changed.mutable_resources()->CopyFrom(
      Resources::parse("cpus:4;mem:1024").get() +
      node::storeResources(*stores, "*"));

  Try<Nothing> reclassed = node::compatibility::additive(local, changed);
  ASSERT_ERROR(reclassed);
  EXPECT_TRUE(strings::contains(reclassed.error(), "more than its"))
    << reclassed.error();

  NodeInfo removed = local;
  removed.mutable_resources()->CopyFrom(
      Resources::parse("cpus:4;mem:1024").get());

  Try<Nothing> gone = node::compatibility::additive(local, removed);
  ASSERT_ERROR(gone);
  EXPECT_TRUE(strings::contains(gone.error(), "Resource not found"))
    << gone.error();
}

} // namespace tests {
} // namespace internal {
} // namespace medios {
