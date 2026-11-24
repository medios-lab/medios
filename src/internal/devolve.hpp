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

#ifndef __INTERNAL_DEVOLVE_HPP__
#define __INTERNAL_DEVOLVE_HPP__

#include <google/protobuf/message.h>

#include <medios/node/node.hpp>

#include <medios/medios.hpp>
#include <medios/resources.hpp>

#include <medios/executor/executor.hpp>

#include <medios/controller/controller.hpp>

#include <medios/resource_provider/resource_provider.hpp>

#include <medios/scheduler/scheduler.hpp>

#include <medios/v1/medios.hpp>
#include <medios/v1/resources.hpp>

#include <medios/v1/node/node.hpp>

#include <medios/v1/executor/executor.hpp>

#include <medios/v1/controller/controller.hpp>

#include <medios/v1/resource_provider/resource_provider.hpp>

#include <medios/v1/scheduler/scheduler.hpp>

#include <stout/foreach.hpp>

namespace medios {
namespace internal {

// Helpers for devolving types between versions. Please add as necessary!
CommandInfo devolve(const v1::CommandInfo& command);
ContainerID devolve(const v1::ContainerID& containerId);
Credential devolve(const v1::Credential& credential);
DrainConfig devolve(const v1::DrainConfig& drainConfig);
DrainInfo devolve(const v1::DrainInfo& drainInfo);
DurationInfo devolve(const google::protobuf::Duration& duration);
ExecutorID devolve(const v1::ExecutorID& executorId);
FrameworkID devolve(const v1::FrameworkID& frameworkId);
FrameworkInfo devolve(const v1::FrameworkInfo& frameworkInfo);
HealthCheck devolve(const v1::HealthCheck& check);
InverseOffer devolve(const v1::InverseOffer& inverseOffer);
Offer devolve(const v1::Offer& offer);
Offer::Operation devolve(const v1::Offer::Operation& operation);
OperationStatus devolve(const v1::OperationStatus& status);
Resource devolve(const v1::Resource& resource);
ResourceProviderID devolve(const v1::ResourceProviderID& resourceProviderId);
ResourceProviderInfo devolve(
    const v1::ResourceProviderInfo& resourceProviderInfo);
Resources devolve(const v1::Resources& resources);
NodeID devolve(const v1::NodeID& nodeId);
NodeInfo devolve(const v1::NodeInfo& nodeInfo);
TaskID devolve(const v1::TaskID& taskId);
TaskStatus devolve(const v1::TaskStatus& status);

medios::resource_provider::Call devolve(const v1::resource_provider::Call& call);
medios::resource_provider::Event devolve(
    const v1::resource_provider::Event& event);

medios::scheduler::Call devolve(const v1::scheduler::Call& call);
medios::scheduler::Event devolve(const v1::scheduler::Event& event);

executor::Call devolve(const v1::executor::Call& call);
executor::Event devolve(const v1::executor::Event& event);

medios::node::Call devolve(const v1::node::Call& call);
medios::node::Response devolve(const v1::node::Response& response);

medios::controller::Call devolve(const v1::controller::Call& call);

// Helper for repeated field devolving to 'T1' from 'T2'.
template <typename T1, typename T2>
google::protobuf::RepeatedPtrField<T1> devolve(
    google::protobuf::RepeatedPtrField<T2> t2s)
{
  google::protobuf::RepeatedPtrField<T1> t1s;

  foreach (const T2& t2, t2s) {
    t1s.Add()->CopyFrom(devolve(t2));
  }

  return t1s;
}

} // namespace internal {
} // namespace medios {

#endif // __INTERNAL_DEVOLVE_HPP__
