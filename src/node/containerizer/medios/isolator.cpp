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

#include <process/dispatch.hpp>

#include "node/containerizer/medios/isolator.hpp"

using namespace process;

using std::string;
using std::vector;

using medios::node::ContainerConfig;
using medios::node::ContainerLaunchInfo;
using medios::node::ContainerLimitation;
using medios::node::ContainerState;

namespace medios {
namespace internal {
namespace node {

MediosIsolator::MediosIsolator(Owned<MediosIsolatorProcess> _process)
  : process(_process)
{
  spawn(CHECK_NOTNULL(process.get()));
}


MediosIsolator::~MediosIsolator()
{
  terminate(process.get());
  wait(process.get());
}


bool MediosIsolator::supportsNesting()
{
  return process->supportsNesting();
}


bool MediosIsolator::supportsStandalone()
{
  return process->supportsStandalone();
}


Future<Nothing> MediosIsolator::recover(
    const vector<ContainerState>& state,
    const hashset<ContainerID>& orphans)
{
  return dispatch(process.get(),
                  &MediosIsolatorProcess::recover,
                  state,
                  orphans);
}


Future<Option<ContainerLaunchInfo>> MediosIsolator::prepare(
    const ContainerID& containerId,
    const ContainerConfig& containerConfig)
{
  return dispatch(process.get(),
                  &MediosIsolatorProcess::prepare,
                  containerId,
                  containerConfig);
}


Future<Nothing> MediosIsolator::isolate(
    const ContainerID& containerId,
    pid_t pid)
{
  return dispatch(process.get(),
                  &MediosIsolatorProcess::isolate,
                  containerId,
                  pid);
}


Future<ContainerLimitation> MediosIsolator::watch(
    const ContainerID& containerId)
{
  return dispatch(process.get(),
                  &MediosIsolatorProcess::watch,
                  containerId);
}


Future<Nothing> MediosIsolator::update(
    const ContainerID& containerId,
    const Resources& resourceRequests,
    const google::protobuf::Map<string, Value::Scalar>& resourceLimits)
{
  return dispatch(process.get(),
                  &MediosIsolatorProcess::update,
                  containerId,
                  resourceRequests,
                  resourceLimits);
}


Future<ResourceStatistics> MediosIsolator::usage(
    const ContainerID& containerId)
{
  return dispatch(process.get(),
                  &MediosIsolatorProcess::usage,
                  containerId);
}


Future<ContainerStatus> MediosIsolator::status(
    const ContainerID& containerId)
{
  return dispatch(process.get(),
                  &MediosIsolatorProcess::status,
                  containerId);
}


Future<Nothing> MediosIsolator::cleanup(
    const ContainerID& containerId)
{
  return dispatch(process.get(),
                  &MediosIsolatorProcess::cleanup,
                  containerId);
}

} // namespace node {
} // namespace internal {
} // namespace medios {
