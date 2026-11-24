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

#ifndef __ISOLATOR_HPP__
#define __ISOLATOR_HPP__

#include <medios/node/isolator.hpp>

#include <process/owned.hpp>
#include <process/process.hpp>

#include <stout/none.hpp>

namespace medios {
namespace internal {
namespace node {

// Forward declaration.
class MediosIsolatorProcess;


// A wrapper class that implements the 'Isolator' interface which is
// backed by an 'MediosIsolatorProcess'. This helper class is useful
// for programmers to write asynchronous isolators.
class MediosIsolator : public medios::node::Isolator
{
public:
  explicit MediosIsolator(process::Owned<MediosIsolatorProcess> process);
  ~MediosIsolator() override;

  bool supportsNesting() override;
  bool supportsStandalone() override;

  process::Future<Nothing> recover(
      const std::vector<medios::node::ContainerState>& states,
      const hashset<ContainerID>& orphans) override;

  process::Future<Option<medios::node::ContainerLaunchInfo>> prepare(
      const ContainerID& containerId,
      const medios::node::ContainerConfig& containerConfig) override;

  process::Future<Nothing> isolate(
      const ContainerID& containerId,
      pid_t pid) override;

  process::Future<medios::node::ContainerLimitation> watch(
      const ContainerID& containerId) override;

  process::Future<Nothing> update(
      const ContainerID& containerId,
      const Resources& resourceRequests,
      const google::protobuf::Map<
          std::string, Value::Scalar>& resourceLimits = {}) override;

  process::Future<ResourceStatistics> usage(
      const ContainerID& containerId) override;

  process::Future<ContainerStatus> status(
      const ContainerID& containerId) override;

  process::Future<Nothing> cleanup(
      const ContainerID& containerId) override;

private:
  process::Owned<MediosIsolatorProcess> process;
};


class MediosIsolatorProcess : public process::Process<MediosIsolatorProcess>
{
public:
  ~MediosIsolatorProcess() override {}

  virtual bool supportsNesting()
  {
    return false;
  }

  virtual bool supportsStandalone()
  {
    return false;
  }

  virtual process::Future<Nothing> recover(
      const std::vector<medios::node::ContainerState>& states,
      const hashset<ContainerID>& orphans)
  {
    return Nothing();
  }

  virtual process::Future<Option<medios::node::ContainerLaunchInfo>> prepare(
      const ContainerID& containerId,
      const medios::node::ContainerConfig& containerConfig)
  {
    return None();
  }

  virtual process::Future<Nothing> isolate(
      const ContainerID& containerId,
      pid_t pid)
  {
    return Nothing();
  }

  virtual process::Future<medios::node::ContainerLimitation> watch(
      const ContainerID& containerId)
  {
    return process::Future<medios::node::ContainerLimitation>();
  }

  virtual process::Future<Nothing> update(
      const ContainerID& containerId,
      const Resources& resourceRequests,
      const google::protobuf::Map<
          std::string, Value::Scalar>& resourceLimits = {})
  {
    return Nothing();
  }

  virtual process::Future<ResourceStatistics> usage(
      const ContainerID& containerId)
  {
    return ResourceStatistics();
  }

  virtual process::Future<ContainerStatus> status(
      const ContainerID& containerId)
  {
    return ContainerStatus();
  };

  virtual process::Future<Nothing> cleanup(
      const ContainerID& containerId)
  {
    return Nothing();
  }
};

} // namespace node {
} // namespace internal {
} // namespace medios {

#endif // __ISOLATOR_HPP__
