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

#ifndef __MEDIOS_CONTAINERIZER_UTILS_HPP__
#define __MEDIOS_CONTAINERIZER_UTILS_HPP__

#include <string>

#include <medios/medios.hpp>

#include <medios/node/containerizer.hpp>

#include <stout/os.hpp>
#include <stout/try.hpp>

namespace medios {
namespace internal {
namespace node {

#ifdef __linux__
Try<pid_t> getMountNamespaceTarget(pid_t parent);
#endif // __linux__


Try<int> calculateOOMScoreAdj(const Bytes& memRequest);


// Tells the launcher where a store volume goes in the sandbox, at its
// path on the node, so it refuses the launch if the fetcher left
// something there. The filesystem isolators place stores; the launcher
// runs after the fetcher and they run before it.
void addStorePlacement(
    const Volume& volume,
    const std::string& path,
    medios::node::ContainerLaunchInfo* launchInfo);

} // namespace node {
} // namespace internal {
} // namespace medios {

#endif // __MEDIOS_CONTAINERIZER_UTILS_HPP__
