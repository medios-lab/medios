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

#ifndef __MEDIOS_DOCKER_SHELL_CONTAINERIZER_HPP__
#define __MEDIOS_DOCKER_SHELL_CONTAINERIZER_HPP__

// DR-030: medios's Docker runtime backend. Orchestration-only: forks the
// medios-docker-executor binary as a host subprocess and reaps it. All
// Docker work (run, monitor, status updates) lives in the executor.

#include <process/future.hpp>
#include <process/owned.hpp>

#include <stout/try.hpp>

#include "node/containerizer/containerizer.hpp"
#include "node/flags.hpp"
#include "node/state.hpp"

namespace medios {
namespace internal {
namespace node {

class DockerShellContainerizerProcess; // forward

class DockerShellContainerizer : public Containerizer
{
public:
  static Try<DockerShellContainerizer*> create(const Flags& flags);

  ~DockerShellContainerizer() override;

  process::Future<Nothing> recover(
      const Option<state::NodeState>& state) override;

  process::Future<Containerizer::LaunchResult> launch(
      const ContainerID& containerId,
      const medios::node::ContainerConfig& containerConfig,
      const std::map<std::string, std::string>& environment,
      const Option<std::string>& pidCheckpointPath) override;

  process::Future<Nothing> update(
      const ContainerID& containerId,
      const Resources& resourceRequests,
      const google::protobuf::Map<
          std::string, Value::Scalar>& resourceLimits = {}) override;

  process::Future<ResourceStatistics> usage(
      const ContainerID& containerId) override;

  process::Future<ContainerStatus> status(
      const ContainerID& containerId) override;

  process::Future<Option<medios::node::ContainerTermination>> wait(
      const ContainerID& containerId) override;

  process::Future<Option<medios::node::ContainerTermination>> destroy(
      const ContainerID& containerId) override;

  process::Future<bool> kill(
      const ContainerID& containerId,
      int signal) override;

  process::Future<hashset<ContainerID>> containers() override;

  process::Future<Nothing> pruneImages(
      const std::vector<Image>& excludedImages) override;

private:
  explicit DockerShellContainerizer(
      process::Owned<DockerShellContainerizerProcess> _process);

  process::Owned<DockerShellContainerizerProcess> process;
};

} // namespace node
} // namespace internal
} // namespace medios

#endif // __MEDIOS_DOCKER_SHELL_CONTAINERIZER_HPP__
