// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

#ifndef __CONTROLLER_STORES_HPP__
#define __CONTROLLER_STORES_HPP__

#include <string>

#include <medios/medios.hpp>

#include <stout/hashmap.hpp>

namespace medios {
namespace internal {
namespace controller {

struct Node;

} // namespace controller {


// How many tasks on a node hold a store in each mode (DR-036). A task
// holds its stores from launch until its terminal or unreachable state,
// when the controller recovers its resources; the tally follows that and
// nothing else, so it needs no book of its own. An exclusive RW holder
// counts as every writer unit the store declares.
struct StoreHolders
{
  size_t readers = 0;
  size_t writers = 0;
};


// The holders of each store on the node, keyed by store name. A store a
// task names that the node does not declare still appears, so the caller
// can see a count for it; validation refuses such a task before it is
// added.
hashmap<std::string, StoreHolders> heldStores(const controller::Node& node);

} // namespace internal {
} // namespace medios {

#endif // __CONTROLLER_STORES_HPP__
