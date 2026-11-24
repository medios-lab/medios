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

#include <medios/allocator/allocator.hpp>

#include <medios/module/allocator.hpp>

#include "controller/constants.hpp"

#include "controller/allocator/medios/hierarchical.hpp"

#include "module/manager.hpp"

using std::string;

using medios::internal::controller::allocator::HierarchicalDRFAllocator;
using medios::internal::controller::allocator::HierarchicalRandomAllocator;

namespace medios {
namespace allocator {

Try<Allocator*> Allocator::create(
    const string& name,
    const string& roleSorter,
    const string& frameworkSorter)
{
  // Create an instance of the default allocator. If other than the
  // default allocator is requested, search for it in loaded modules.
  //
  // NOTE: We do not need an extra not-null check, because both
  // ModuleManager and built-in allocator factory do that already.
  //
  // We also look for "HierarchicalDRF" since that was the
  // previous value for `DEFAULT_ALLOCATOR`.
  if (name == "HierarchicalDRF" ||
      name == medios::internal::controller::DEFAULT_ALLOCATOR) {
    if (roleSorter == "drf" && frameworkSorter == "drf") {
      return HierarchicalDRFAllocator::create();
    }

    if (roleSorter == "random" && frameworkSorter == "random") {
      return HierarchicalRandomAllocator::create();
    }

    return Error("Unsupported combination of 'role_sorter'"
                 " and 'framework_sorter': must be equal (for now)");
  }

  return modules::ModuleManager::create<Allocator>(name);
}

} // namespace allocator {
} // namespace medios {
