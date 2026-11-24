// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

#include "controller/stores.hpp"

#include <string>

#include <stout/foreach.hpp>
#include <stout/hashmap.hpp>
#include <stout/try.hpp>

#include "common/protobuf_utils.hpp"

#include "controller/controller.hpp"

#include "node/stores.hpp"

using std::string;

namespace medios {
namespace internal {

hashmap<string, StoreHolders> heldStores(const controller::Node& node)
{
  // What the node declares, so an exclusive holder can be charged the
  // declared writer count rather than one.
  hashmap<string, node::StoreDeclaration> declared;
  foreach (const Resource& resource, node.totalResources) {
    Try<node::StoreDeclaration> store = node::storeOf(resource);
    if (store.isSome()) {
      declared[store->name] = *store;
    }
  }

  hashmap<string, StoreHolders> held;

  foreachvalue (const auto& tasks, node.tasks) {
    foreachvalue (const Task* task, tasks) {
      // The same rule `updateTask` recovers resources by: a task in a
      // terminal state, or unreachable, holds nothing.
      if (protobuf::isTerminalState(task->state()) ||
          task->state() == TASK_UNREACHABLE) {
        continue;
      }

      foreach (const TaskData& data, protobuf::storesOf(*task)) {
        StoreHolders& holders = held[data.store()];

        if (data.mode() == Volume::RO) {
          holders.readers++;
        } else if (data.exclusive() && declared.contains(data.store())) {
          holders.writers += declared.at(data.store()).writers;
        } else {
          holders.writers++;
        }
      }
    }
  }

  return held;
}


} // namespace internal {
} // namespace medios {
