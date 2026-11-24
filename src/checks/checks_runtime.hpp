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

#ifndef __CHECKS_RUNTIME_HPP__
#define __CHECKS_RUNTIME_HPP__

#include <string>
#include <vector>

#include <medios/medios.hpp>

#include <process/http.hpp>

#include <stout/option.hpp>

namespace medios {
namespace internal {
namespace checks {
namespace runtime {

// `Plain` contains the runtime information for the regular Medios
// containerizer.
struct Plain
{
  // The namespaces to enter prior to performing the health check.
  std::vector<std::string> namespaces;

  // The target task's pid used to enter the specified namespaces.
  Option<pid_t> taskPid;
};

// `Nested` contains the runtime information for nested containers created by
// the Medios containerizer. In these cases, the command health check is sent to
// and performed by the node. The HTTP and TCP health checks are still
// performed directly by the executor.
struct Nested
{
  ContainerID taskContainerId; // The ContainerID of the target task.
  process::http::URL nodeURL; // The URL of the node.

  // The authorization header the health checker should use to authenticate
  // with the node operator API.
  Option<std::string> authorizationHeader;
};

} // namespace runtime {
} // namespace checks {
} // namespace internal {
} // namespace medios {

#endif // __CHECKS_RUNTIME_HPP__
