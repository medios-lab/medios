// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

#include "node/stores.hpp"

#include <algorithm>
#include <cmath>
#include <map>
#include <set>
#include <string>
#include <vector>

#include <stout/error.hpp>
#include <stout/foreach.hpp>
#include <stout/json.hpp>
#include <stout/numify.hpp>
#include <stout/option.hpp>
#include <stout/os/access.hpp>
#include <stout/os/exists.hpp>
#include <stout/os/read.hpp>
#include <stout/path.hpp>
#include <stout/stringify.hpp>
#include <stout/strings.hpp>

#include "common/protobuf_utils.hpp"

using std::map;
using std::set;
using std::string;
using std::vector;

namespace medios {
namespace internal {
namespace node {

const string STORE_METADATA_CLASS = "class";
const string STORE_METADATA_LOCALITY = "locality";
const string STORE_METADATA_READERS = "readers";
const string STORE_METADATA_WRITERS = "writers";

namespace {

const string STORE_RESOURCE_PREFIX = "store/";


string nth(size_t index)
{
  return "store " + stringify(index) + " of the stores file";
}


bool validName(const string& name)
{
  if (name.empty()) {
    return false;
  }

  foreach (char c, name) {
    const bool allowed =
      (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') ||
      (c >= '0' && c <= '9') || c == '.' || c == '_' || c == '-';
    if (!allowed) {
      return false;
    }
  }

  return true;
}


Try<string> stringField(
    const JSON::Object& store,
    const string& field,
    size_t index,
    const string& name)
{
  Result<JSON::String> value = store.at<JSON::String>(field);
  if (value.isError()) {
    return Error(nth(index) + " ('" + name + "') has a '" + field +
                 "' that is not a string: " + value.error());
  }
  if (value.isNone() || value->value.empty()) {
    return Error(nth(index) + " ('" + name + "') has no '" + field + "'");
  }
  return value->value;
}


// A count is a whole number of tasks. Bounded before the conversion: a
// double outside size_t is undefined to convert, and every integer below
// 2^53 is exactly representable, which is what makes the equality mean
// what it reads as.
Try<size_t> countField(
    const JSON::Object& store,
    const string& field,
    size_t index,
    const string& name)
{
  Result<JSON::Number> value = store.at<JSON::Number>(field);
  if (value.isError()) {
    return Error(nth(index) + " ('" + name + "') has a '" + field +
                 "' that is not a number: " + value.error());
  }
  if (value.isNone()) {
    return Error(nth(index) + " ('" + name + "') has no '" + field +
                 "'; write how many tasks on this node may hold the store "
                 "in that mode at once, 0 for none");
  }

  const double declared = value->as<double>();
  const double bound = 9007199254740992.0;  // 2^53.

  if (declared < 0.0 || declared >= bound || declared != std::floor(declared)) {
    return Error(nth(index) + " ('" + name + "') has a '" + field + "' of " +
                 stringify(declared) + "; it is a whole number of tasks");
  }

  return static_cast<size_t>(declared);
}


Try<StoreDeclaration> parseStore(const JSON::Object& json, size_t index)
{
  Result<JSON::String> name = json.at<JSON::String>("name");
  if (name.isError()) {
    return Error(nth(index) + " has a 'name' that is not a string: " +
                 name.error());
  }
  if (name.isNone() || name->value.empty()) {
    return Error(nth(index) + " has no 'name'; a task asks for a store by "
                 "name and an anonymous store cannot be asked for");
  }

  // The name becomes the tail of the resource name `store/<name>`, and a
  // second slash or a space in it would make the resource name ambiguous
  // to everything that reads it back.
  if (!validName(name->value)) {
    return Error(nth(index) + " has the name '" + name->value + "'; a store "
                 "name is one or more of A-Za-z0-9._-");
  }

  StoreDeclaration store;
  store.name = name->value;

  Try<string> path = stringField(json, "path", index, store.name);
  if (path.isError()) {
    return Error(path.error());
  }
  if (!path::is_absolute(*path)) {
    return Error(nth(index) + " ('" + store.name + "') has the path '" +
                 *path + "'; a store's path is absolute");
  }
  store.path = *path;

  Try<string> storeClass = stringField(json, "class", index, store.name);
  if (storeClass.isError()) {
    return Error(storeClass.error());
  }
  if (*storeClass == "object") {
    return Error(nth(index) + " ('" + store.name + "') has class 'object', "
                 "which this node does not deliver: an object store is "
                 "reached by endpoint and not mounted, and that delivery is "
                 "not written (DR-036)");
  }
  if (*storeClass != "local" && *storeClass != "shared") {
    return Error(nth(index) + " ('" + store.name + "') has class '" +
                 *storeClass + "'; it is 'local' or 'shared'");
  }
  store.storeClass = *storeClass;

  Try<string> locality = stringField(json, "locality", index, store.name);
  if (locality.isError()) {
    return Error(locality.error());
  }
  if (*locality != "near" && *locality != "same-site" &&
      *locality != "remote") {
    return Error(nth(index) + " ('" + store.name + "') has locality '" +
                 *locality + "'; it is 'near', 'same-site' or 'remote'");
  }
  store.locality = *locality;

  Try<size_t> readers = countField(json, "readers", index, store.name);
  if (readers.isError()) {
    return Error(readers.error());
  }
  store.readers = *readers;

  Try<size_t> writers = countField(json, "writers", index, store.name);
  if (writers.isError()) {
    return Error(writers.error());
  }
  store.writers = *writers;

  // A store no task may hold in either mode would be advertised and refused
  // on every request. That is a file someone half-edited.
  if (store.readers == 0 && store.writers == 0) {
    return Error(nth(index) + " ('" + store.name + "') declares 0 readers "
                 "and 0 writers; no task could ever hold it");
  }

  return store;
}


Try<size_t> countLabel(const Resource& resource, const string& key)
{
  foreach (const Label& label, resource.metadata().labels()) {
    if (label.key() == key) {
      Try<size_t> count = numify<size_t>(label.value());
      if (count.isError()) {
        return Error("Resource '" + resource.name() + "' has a '" + key +
                     "' of '" + label.value() + "', which is not a count");
      }
      return count;
    }
  }

  return Error("Resource '" + resource.name() + "' is a store with no '" +
               key + "' metadata");
}


Try<string> stringLabel(const Resource& resource, const string& key)
{
  foreach (const Label& label, resource.metadata().labels()) {
    if (label.key() == key) {
      return label.value();
    }
  }

  return Error("Resource '" + resource.name() + "' is a store with no '" +
               key + "' metadata");
}

} // namespace {


string storeResourceName(const string& store)
{
  return STORE_RESOURCE_PREFIX + store;
}


Option<string> storeNameOf(const string& resourceName)
{
  if (!strings::startsWith(resourceName, STORE_RESOURCE_PREFIX)) {
    return None();
  }

  return resourceName.substr(STORE_RESOURCE_PREFIX.size());
}


Try<vector<StoreDeclaration>> parseStores(const string& contents)
{
  Try<JSON::Object> json = JSON::parse<JSON::Object>(contents);
  if (json.isError()) {
    return Error("The stores file is not a JSON object: " + json.error());
  }

  Result<JSON::Array> stores = json->at<JSON::Array>("stores");
  if (stores.isError()) {
    return Error("The stores file's 'stores' is not an array: " +
                 stores.error());
  }
  if (stores.isNone()) {
    return Error("The stores file has no 'stores' array");
  }

  // An empty list is refused rather than read as "this node has no
  // stores". A node with none does not pass --stores; a file that declares
  // none is a file someone emptied by mistake.
  if (stores->values.empty()) {
    return Error("The stores file declares no stores; a node with none does "
                 "not pass --stores");
  }

  vector<StoreDeclaration> declarations;
  set<string> names;

  for (size_t index = 0; index < stores->values.size(); index++) {
    const JSON::Value& value = stores->values.at(index);
    if (!value.is<JSON::Object>()) {
      return Error(nth(index) + " is not a JSON object");
    }

    Try<StoreDeclaration> store = parseStore(value.as<JSON::Object>(), index);
    if (store.isError()) {
      return Error(store.error());
    }

    if (!names.insert(store->name).second) {
      return Error("The stores file declares '" + store->name +
                   "' more than once; one store has one declaration");
    }

    declarations.push_back(*store);
  }

  return declarations;
}


Try<Nothing> checkStores(const vector<StoreDeclaration>& stores)
{
  foreach (const StoreDeclaration& store, stores) {
    // Following links: a store may be a link to a mount, and what a task
    // gets is what the link reaches.
    if (!os::exists(store.path)) {
      return Error("Store '" + store.name + "' is declared at '" +
                   store.path + "', which does not exist on this host. "
                   "The declaration is not adjusted to fit the host "
                   "(FAC3-0028)");
    }

    // A store is delivered by this node, so this node can read it; one it
    // cannot would be advertised and then answer nothing.
    Try<bool> readable = os::access(store.path, R_OK);
    if (readable.isError() || !readable.get()) {
      return Error("Store '" + store.name + "' is declared at '" +
                   store.path + "', which this node cannot read" +
                   (readable.isError() ? ": " + readable.error() : "") +
                   ". The declaration is not adjusted to fit the host "
                   "(FAC3-0028)");
    }
  }

  return Nothing();
}


Resources storeResources(const vector<StoreDeclaration>& stores, const string& role)
{
  Resources resources;

  foreach (const StoreDeclaration& store, stores) {
    // Through the parser so the reservation is in the format the rest of
    // the node's total carries, whatever the role.
    Resource resource =
      Resources::parse(storeResourceName(store.name), "1", role).get();
    resource.mutable_shared();

    Labels* metadata = resource.mutable_metadata();
    *metadata->add_labels() =
      protobuf::createLabel(STORE_METADATA_CLASS, store.storeClass);
    *metadata->add_labels() =
      protobuf::createLabel(STORE_METADATA_LOCALITY, store.locality);
    *metadata->add_labels() =
      protobuf::createLabel(STORE_METADATA_READERS, stringify(store.readers));
    *metadata->add_labels() =
      protobuf::createLabel(STORE_METADATA_WRITERS, stringify(store.writers));

    resources += resource;
  }

  return resources;
}


Try<StoreDeclaration> storeOf(const Resource& resource)
{
  Option<string> name = storeNameOf(resource.name());
  if (name.isNone()) {
    return Error("Resource '" + resource.name() + "' is not a store");
  }

  StoreDeclaration store;
  store.name = *name;

  Try<string> storeClass = stringLabel(resource, STORE_METADATA_CLASS);
  if (storeClass.isError()) {
    return Error(storeClass.error());
  }
  store.storeClass = *storeClass;

  Try<string> locality = stringLabel(resource, STORE_METADATA_LOCALITY);
  if (locality.isError()) {
    return Error(locality.error());
  }
  store.locality = *locality;

  Try<size_t> readers = countLabel(resource, STORE_METADATA_READERS);
  if (readers.isError()) {
    return Error(readers.error());
  }
  store.readers = *readers;

  Try<size_t> writers = countLabel(resource, STORE_METADATA_WRITERS);
  if (writers.isError()) {
    return Error(writers.error());
  }
  store.writers = *writers;

  return store;
}


Try<map<string, StoreDeclaration>> readStores(const string& path)
{
  Try<string> contents = os::read(path);
  if (contents.isError()) {
    return Error("Failed to read the stores file '" + path + "': " +
                 contents.error());
  }

  Try<vector<StoreDeclaration>> stores = parseStores(*contents);
  if (stores.isError()) {
    return Error("The stores file '" + path + "' is refused: " +
                 stores.error());
  }

  map<string, StoreDeclaration> byName;
  foreach (const StoreDeclaration& store, *stores) {
    byName[store.name] = store;
  }

  return byName;
}


Try<string> storePathOf(
    const Volume& volume,
    const map<string, StoreDeclaration>& stores)
{
  if (!volume.has_source() ||
      volume.source().type() != Volume::Source::STORE) {
    const string type = volume.has_source()
      ? Volume::Source::Type_Name(volume.source().type())
      : "HOST_PATH";

    return Error(
        "Volume '" + volume.container_path() + "' has source type " + type +
        "; only a STORE volume is delivered, and a host path is not");
  }

  const string& name = volume.source().store().name();

  auto store = stores.find(name);
  if (store == stores.end()) {
    return Error(
        "Volume '" + volume.container_path() + "' names store '" + name +
        "', which this node does not declare" +
        (stores.empty() ? " (it declares no stores)" : ""));
  }

  return store->second.path;
}


bool isSandboxName(const string& containerPath)
{
  return !containerPath.empty() &&
    containerPath != "." &&
    containerPath != ".." &&
    !strings::contains(containerPath, "/");
}


bool mediosDeliversStores(const string& launcher, const string& isolation)
{
  // The containerizer runs filesystem/posix when no filesystem isolator
  // is named.
  string filesystem = "filesystem/posix";
  foreach (const string& isolator, strings::tokenize(isolation, ",")) {
    if (strings::startsWith(isolator, "filesystem/")) {
      filesystem = isolator;
    }
  }

  return (launcher == "linux" && filesystem == "filesystem/linux") ||
    (launcher == "posix" && filesystem == "filesystem/posix");
}


Try<Nothing> checkStoreDelivery(
    const string& containerizers,
    const string& launcher,
    const string& isolation)
{
  const vector<string> types = strings::tokenize(containerizers, ",");

  const bool docker =
    std::find(types.begin(), types.end(), "docker_shell") != types.end();
  const bool medios =
    std::find(types.begin(), types.end(), "medios") != types.end();

  if (docker || (medios && mediosDeliversStores(launcher, isolation))) {
    return Nothing();
  }

  return Error(
      "no containerizer on this node delivers a store: --containerizers is '" +
      containerizers + "', --launcher is '" + launcher + "' and --isolation "
      "is '" + isolation + "'; a store is bound by the docker_shell "
      "containerizer, delivered by the medios containerizer with "
      "--launcher=linux and filesystem/linux in --isolation, or linked by "
      "it with --launcher=posix and filesystem/posix, the default when "
      "--isolation names no filesystem isolator");
}


} // namespace node {
} // namespace internal {
} // namespace medios {
