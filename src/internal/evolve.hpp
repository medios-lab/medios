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

#ifndef __INTERNAL_EVOLVE_HPP__
#define __INTERNAL_EVOLVE_HPP__

#include <google/protobuf/message.h>

#include <medios/node/node.hpp>

#include <medios/medios.hpp>
#include <medios/resources.hpp>

#include <medios/controller/controller.hpp>

#include <medios/executor/executor.hpp>

#include <medios/controller/controller.hpp>

#include <medios/maintenance/maintenance.hpp>

#include <medios/resource_provider/resource_provider.hpp>

#include <medios/scheduler/scheduler.hpp>

#include <medios/v1/medios.hpp>
#include <medios/v1/resources.hpp>

#include <medios/v1/node/node.hpp>

#include <medios/v1/executor/executor.hpp>

#include <medios/v1/controller/controller.hpp>

#include <medios/v1/resource_provider/resource_provider.hpp>

#include <medios/v1/maintenance/maintenance.hpp>

#include <medios/v1/scheduler/scheduler.hpp>

#include <stout/foreach.hpp>
#include <stout/json.hpp>

#include "messages/messages.hpp"

namespace medios {
namespace internal {

// Helpers for evolving types between versions. Please add as necessary!
v1::NodeID evolve(const NodeID& nodeId);
v1::NodeInfo evolve(const NodeInfo& nodeInfo);
v1::ContainerInfo evolve(const ContainerInfo& containerInfo);
v1::DomainInfo evolve(const DomainInfo& domainInfo);
v1::DrainInfo evolve(const DrainInfo& drainInfo);
v1::ExecutorID evolve(const ExecutorID& executorId);
v1::ExecutorInfo evolve(const ExecutorInfo& executorInfo);
v1::FileInfo evolve(const FileInfo& fileInfo);
v1::FrameworkID evolve(const FrameworkID& frameworkId);
v1::FrameworkInfo evolve(const FrameworkInfo& frameworkInfo);
v1::InverseOffer evolve(const InverseOffer& inverseOffer);
v1::KillPolicy evolve(const KillPolicy& killPolicy);
v1::MachineID evolve(const MachineID& machineId);
v1::ControllerInfo evolve(const ControllerInfo& controllerInfo);
v1::Offer evolve(const Offer& offer);
v1::OfferID evolve(const OfferID& offerId);
v1::OperationStatus evolve(const OperationStatus& status);
v1::Resource evolve(const Resource& resource);
v1::ResourceProviderID evolve(const ResourceProviderID& resourceProviderId);
v1::Resources evolve(const Resources& resources);
v1::Task evolve(const Task& task);
v1::TaskID evolve(const TaskID& taskId);
v1::TaskInfo evolve(const TaskInfo& taskInfo);
v1::TaskStatus evolve(const TaskStatus& status);
v1::UUID evolve(const UUID& uuid);


v1::node::Call evolve(const medios::node::Call& call);
v1::node::ProcessIO evolve(const medios::node::ProcessIO& processIO);
v1::node::Response evolve(const medios::node::Response& response);


v1::maintenance::ClusterStatus evolve(
    const maintenance::ClusterStatus& cluster);
v1::maintenance::Schedule evolve(const maintenance::Schedule& schedule);


v1::resource_provider::Call evolve(const resource_provider::Call& call);
v1::resource_provider::Event evolve(const resource_provider::Event& event);


// Helper for repeated field evolving to 'T1' from 'T2'.
template <typename T1, typename T2>
google::protobuf::RepeatedPtrField<T1> evolve(
    const google::protobuf::RepeatedPtrField<T2>& t2s)
{
  google::protobuf::RepeatedPtrField<T1> t1s;
  t1s.Reserve(t2s.size());

  foreach (const T2& t2, t2s) {
    *t1s.Add() = evolve(t2);
  }

  return t1s;
}


v1::scheduler::Event evolve(const medios::scheduler::Event& event);
v1::scheduler::Response evolve(const medios::scheduler::Response& response);


// Helper functions that evolve old style internal messages to a
// v1::scheduler::Event.
v1::scheduler::Event evolve(const ExitedExecutorMessage& message);
v1::scheduler::Event evolve(const ExecutorToFrameworkMessage& message);
v1::scheduler::Event evolve(const FrameworkErrorMessage& message);
v1::scheduler::Event evolve(const FrameworkRegisteredMessage& message);
v1::scheduler::Event evolve(const FrameworkReregisteredMessage& message);
v1::scheduler::Event evolve(const InverseOffersMessage& message);
v1::scheduler::Event evolve(const LostNodeMessage& message);
v1::scheduler::Event evolve(const ResourceOffersMessage& message);
v1::scheduler::Event evolve(const RescindInverseOfferMessage& message);
v1::scheduler::Event evolve(const RescindResourceOfferMessage& message);
v1::scheduler::Event evolve(const StatusUpdateMessage& message);
v1::scheduler::Event evolve(const UpdateOperationStatusMessage& message);


v1::executor::Call evolve(const executor::Call& call);
v1::executor::Event evolve(const executor::Event& event);


// Helper functions that evolve old style internal messages to a
// v1::Executor::Event.
v1::executor::Event evolve(const ExecutorRegisteredMessage& message);
v1::executor::Event evolve(const FrameworkToExecutorMessage& message);
v1::executor::Event evolve(const KillTaskMessage& message);
v1::executor::Event evolve(const RunTaskMessage& message);
v1::executor::Event evolve(const ShutdownExecutorMessage& message);
v1::executor::Event evolve(const StatusUpdateAcknowledgementMessage& message);


v1::controller::Event evolve(const medios::controller::Event& event);
v1::controller::Response evolve(const medios::controller::Response& response);


// Before the v1 API we had REST endpoints that returned JSON. The JSON was not
// specified in any formal way, i.e., there were no protobufs which captured the
// structure. As part of the v1 API we introduced the Call/Response protobufs
// (see v1/controller.proto and v1/node.proto). This evolve variant transforms a
// JSON object that would have been returned from a particular REST endpoint to
// a `Response` protobuf suitable for returning from the new v1 API endpoints.

// Declaration of helper functions for evolving JSON objects used in controller's
// REST endpoints pre v1 API.
template <v1::controller::Response::Type T>
v1::controller::Response evolve(const JSON::Object& object);


// Declaration of helper functions for evolving JSON objects used in node's
// REST endpoints pre v1 API.
template <v1::node::Response::Type T>
v1::node::Response evolve(const JSON::Object& object);


template <v1::node::Response::Type T>
v1::node::Response evolve(const JSON::Array& array);

} // namespace internal {
} // namespace medios {

#endif // __INTERNAL_EVOLVE_HPP__
