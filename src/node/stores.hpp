// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

#ifndef __NODE_STORES_HPP__
#define __NODE_STORES_HPP__

#include <map>
#include <string>
#include <vector>

#include <medios/medios.hpp>
#include <medios/resources.hpp>

#include <stout/nothing.hpp>
#include <stout/option.hpp>
#include <stout/try.hpp>

namespace medios {
namespace internal {
namespace node {

// A store the operator declared on this node (DR-036, medios#529). The
// declaration is the node's: a task names a store and never a path.
//
// The file the node's `--stores` flag points at:
//
//   { "stores": [
//     { "name": "tabicl-tables", "path": "/var/lib/medios-stores/tabicl",
//       "class": "local", "locality": "near", "readers": 4, "writers": 0 },
//     { "name": "results", "path": "/var/lib/medios-results",
//       "class": "shared", "locality": "near", "readers": 10, "writers": 10 }
//   ] }
//
// `readers` is how many tasks on this node may hold the store read-only at
// once and `writers` how many read-write; both are the operator's statement
// about the disk, in tasks, enforced by the controller. `class` is `local`
// (a directory on this host) or `shared` (a filesystem other nodes mount
// too); `object` is a declaration this release does not deliver and is
// refused at start. `locality` is carried into the offer and read by
// frameworks; the node does not interpret it.
struct StoreDeclaration
{
  std::string name;
  std::string path;
  std::string storeClass;
  std::string locality;
  size_t readers;
  size_t writers;
};


// The name of the resource the node advertises for a store, and the
// store name a resource name carries, if it is one.
std::string storeResourceName(const std::string& store);
Option<std::string> storeNameOf(const std::string& resourceName);


// The metadata keys the store resource carries into the offer.
extern const std::string STORE_METADATA_CLASS;
extern const std::string STORE_METADATA_LOCALITY;
extern const std::string STORE_METADATA_READERS;
extern const std::string STORE_METADATA_WRITERS;


// Reads the stores file. Refuses a file that is not the shape above, a
// name with a character outside `A-Za-z0-9._-`, a name declared twice, a
// class other than `local` or `shared`, a locality other than `near`,
// `same-site` or `remote`, a relative path, a count that is not a whole
// number, and a store whose two counts are both zero.
Try<std::vector<StoreDeclaration>> parseStores(const std::string& contents);


// The start-time check (FAC3-0028): every declared path exists on this
// host. Refuses rather than dropping the store, naming it.
Try<Nothing> checkStores(const std::vector<StoreDeclaration>& stores);


// One resource per store: name `store/<name>`, scalar 1, shared, with the
// class, locality and the two counts as metadata. Shared so that a store
// in use keeps appearing in offers; the counts are metadata and not the
// quantity because every copy of a shared resource must be the same proto
// (DR-036).
Resources storeResources(
    const std::vector<StoreDeclaration>& stores,
    const std::string& role);


// The declaration a resource in a node's total carries, if the resource
// is a store. Fails on a store resource missing a count, which is a
// resource some other node code built.
Try<StoreDeclaration> storeOf(const Resource& resource);


// The stores by name, read from the file the flag names. The one entry
// point for the isolators and the executor that resolve a request.
Try<std::map<std::string, StoreDeclaration>> readStores(const std::string& path);

// The declared path a STORE volume resolves to. Fails on a volume of any
// other source type, the bare `host_path` field included, and on a store
// this node does not declare: a task names a store and never a path on a
// node (DR-036). An image volume is the caller's to set aside first. The
// one resolver for the isolators and the docker executor.
Try<std::string> storePathOf(
    const Volume& volume,
    const std::map<std::string, StoreDeclaration>& stores);


// Whether a relative container path is one name inside the sandbox: not
// empty, not `.` or `..`, and without a `/` (DR-036 revision, medios#602).
// The controller refuses every other relative form at submission; a node
// checks again before joining the path to a sandbox on its own disk.
bool isSandboxName(const std::string& containerPath);


// Whether the medios containerizer on this node delivers a STORE volume.
// Under the linux launcher `filesystem/linux` binds it in a private mount
// namespace; under the posix launcher `filesystem/posix` links it into
// the sandbox. The filesystem isolator is the one `isolation` names, or
// `filesystem/posix` when it names none, as the containerizer picks it.
// A launcher paired with the other's isolator delivers nothing. Keyed on
// the flags, never on the platform.
bool mediosDeliversStores(
    const std::string& launcher,
    const std::string& isolation);


// The start-time check for a node that declares stores: a containerizer
// on it must deliver one, the docker containerizer (which binds a store
// itself) or the medios containerizer with the launcher and isolator
// above. Refuses with a sentence naming the flags.
Try<Nothing> checkStoreDelivery(
    const std::string& containerizers,
    const std::string& launcher,
    const std::string& isolation);


} // namespace node {
} // namespace internal {
} // namespace medios {

#endif // __NODE_STORES_HPP__
