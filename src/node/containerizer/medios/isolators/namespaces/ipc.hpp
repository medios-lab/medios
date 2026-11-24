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

#ifndef __NAMESPACES_IPC_ISOLATOR_HPP__
#define __NAMESPACES_IPC_ISOLATOR_HPP__

#include "node/flags.hpp"

#include "node/containerizer/medios/isolator.hpp"

namespace medios {
namespace internal {
namespace node {

class NamespacesIPCIsolatorProcess : public MediosIsolatorProcess
{
public:
  static Try<medios::node::Isolator*> create(const Flags& flags);

  ~NamespacesIPCIsolatorProcess() override {}

  bool supportsNesting() override;
  bool supportsStandalone() override;

  process::Future<Option<medios::node::ContainerLaunchInfo>> prepare(
      const ContainerID& containerId,
      const medios::node::ContainerConfig& containerConfig) override;

  process::Future<Nothing> cleanup(
      const ContainerID& containerId) override;

private:
  NamespacesIPCIsolatorProcess(const Flags& flags);

  const Flags flags;
};

} // namespace node {
} // namespace internal {
} // namespace medios {

#endif // __NAMESPACES_IPC_ISOLATOR_HPP__
