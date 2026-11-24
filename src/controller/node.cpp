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

#include <stdint.h>

#include <algorithm>
#include <cctype>
#include <functional>
#include <iomanip>
#include <iterator>
#include <list>
#include <memory>
#include <set>
#include <sstream>
#include <utility>

#include <medios/module.hpp>
#include <medios/resource_quantities.hpp>
#include <medios/roles.hpp>

#include <medios/authentication/authenticator.hpp>

#include <medios/authorizer/authorizer.hpp>

#include <medios/allocator/allocator.hpp>
#include <medios/controller/contender.hpp>
#include <medios/controller/detector.hpp>

#include <medios/module/authenticator.hpp>

#include <medios/scheduler/scheduler.hpp>

#include <process/check.hpp>
#include <process/collect.hpp>
#include <process/defer.hpp>
#include <process/delay.hpp>
#include <process/http.hpp>
#include <process/id.hpp>
#include <process/limiter.hpp>
#include <process/owned.hpp>
#include <process/run.hpp>
#include <process/shared.hpp>

#include <process/metrics/metrics.hpp>

#include <stout/check.hpp>
#include <stout/duration.hpp>
#include <stout/error.hpp>
#include <stout/foreach.hpp>
#include <stout/ip.hpp>
#include <stout/lambda.hpp>
#include <stout/multihashmap.hpp>
#include <stout/net.hpp>
#include <stout/nothing.hpp>
#include <stout/numify.hpp>
#include <stout/option.hpp>
#include <stout/path.hpp>
#include <stout/stringify.hpp>
#include <stout/unreachable.hpp>
#include <stout/utils.hpp>
#include <stout/uuid.hpp>

#include "authentication/identity_assertion/identity_assertion_authenticator.hpp"

#include "common/authorization.hpp"
#include "common/http.hpp"
#include "common/protobuf_utils.hpp"
#include "common/status_utils.hpp"

#include "credentials/credentials.hpp"

#include "hook/manager.hpp"

#include "logging/logging.hpp"

#include "controller/authorization.hpp"
#include "controller/flags.hpp"
#include "controller/controller.hpp"
#include "controller/registry_operations.hpp"
#include "controller/weights.hpp"

#include "module/manager.hpp"

#include "watcher/allowlist_watcher.hpp"

using std::make_move_iterator;
using std::reference_wrapper;
using std::set;
using std::shared_ptr;
using std::string;
using std::vector;

using process::wait; // Necessary on some OS's to disambiguate.
using process::Clock;
using process::ExitedEvent;
using process::Failure;
using process::Future;
using process::MessageEvent;
using process::Owned;
using process::PID;
using process::Process;
using process::RateLimiter;
using process::Time;
using process::UPID;

using process::http::authentication::Principal;

using process::metrics::Counter;

using google::protobuf::RepeatedPtrField;

namespace medios {
namespace internal {
namespace controller {

using medios::allocator::Allocator;
using medios::allocator::FrameworkOptions;
using medios::allocator::OfferConstraintsFilter;

using medios::authorization::createSubject;
using medios::authorization::VIEW_ROLE;
using medios::authorization::VIEW_FRAMEWORK;
using medios::authorization::VIEW_TASK;

using medios::authorization::ActionObject;

using medios::controller::contender::ControllerContender;

using medios::controller::detector::ControllerDetector;

using medios::scheduler::OfferConstraints;

Node::Node(
    Controller* const _controller,
    NodeInfo _info,
    const UPID& _pid,
    const MachineID& _machineId,
    const string& _version,
    vector<NodeInfo::Capability> _capabilites,
    const Time& _registeredTime,
    vector<Resource> _checkpointedResources,
    const Option<UUID>& _resourceVersion,
    vector<ExecutorInfo> executorInfos,
    vector<Task> tasks)
  : controller(_controller),
    id(_info.id()),
    info(std::move(_info)),
    machineId(_machineId),
    pid(_pid),
    version(_version),
    capabilities(std::move(_capabilites)),
    registeredTime(_registeredTime),
    connected(true),
    active(true),
    checkpointedResources(std::move(_checkpointedResources)),
    resourceVersion(_resourceVersion),
    observer(nullptr)
{
  CHECK(info.has_id());

  Try<Resources> resources = applyCheckpointedResources(
      info.resources(),
      checkpointedResources);

  // NOTE: This should be validated during node recovery.
  CHECK_SOME(resources);
  totalResources = resources.get();

  foreach (ExecutorInfo& executorInfo, executorInfos) {
    CHECK(executorInfo.has_framework_id());
    addExecutor(executorInfo.framework_id(), std::move(executorInfo));
  }

  foreach (Task& task, tasks) {
    addTask(new Task(std::move(task)));
  }
}


Node::~Node()
{
  if (reregistrationTimer.isSome()) {
    process::Clock::cancel(reregistrationTimer.get());
  }
}


Task* Node::getTask(const FrameworkID& frameworkId, const TaskID& taskId) const
{
  if (tasks.contains(frameworkId) && tasks.at(frameworkId).contains(taskId)) {
    return tasks.at(frameworkId).at(taskId);
  }
  return nullptr;
}


void Node::addTask(Task* task)
{
  const TaskID& taskId = task->task_id();
  const FrameworkID& frameworkId = task->framework_id();

  CHECK(!tasks[frameworkId].contains(taskId))
    << "Duplicate task " << taskId << " of framework " << frameworkId;

  // Verify that Resource.AllocationInfo is set,
  // this should be guaranteed by the controller.
  foreach (const Resource& resource, task->resources()) {
    CHECK(resource.has_allocation_info());
  }

  tasks[frameworkId][taskId] = task;

  // Note that we explicitly convert from protobuf to `Resources` here
  // and then use the result below to avoid performance penalty for multiple
  // conversions and validations implied by conversion.
  // Conversion is safe, as resources have already passed validation.
  const Resources resources = task->resources();

  CHECK(task->state() != TASK_UNREACHABLE)
    << "Task '" << taskId << "' of framework " << frameworkId
    << " added in TASK_UNREACHABLE state";

  if (!protobuf::isTerminalState(task->state())) {
    usedResources[frameworkId] += resources;
  }
}


void Node::recoverResources(Task* task)
{
  const TaskID& taskId = task->task_id();
  const FrameworkID& frameworkId = task->framework_id();

  CHECK(protobuf::isTerminalState(task->state()) ||
        task->state() == TASK_UNREACHABLE)
    << "Task '" << taskId << "' of framework " << frameworkId
    << " is in unexpected state " << task->state();

  CHECK(tasks.at(frameworkId).contains(taskId))
    << "Unknown task " << taskId << " of framework " << frameworkId;

  usedResources[frameworkId] -= task->resources();
  if (usedResources[frameworkId].empty()) {
    usedResources.erase(frameworkId);
  }
}


void Node::removeTask(Task* task)
{
  const TaskID& taskId = task->task_id();
  const FrameworkID& frameworkId = task->framework_id();

  CHECK(tasks.at(frameworkId).contains(taskId))
    << "Unknown task " << taskId << " of framework " << frameworkId;

  // The invariant here is that the controller will have already called
  // `recoverResources()` prior to removing terminal or unreachable tasks.
  //
  // TODO(bmahler): The unreachable case could be avoided if
  // we updated `removeNode` in the allocator to recover the
  // resources (see MESOS-621) so that the controller could just
  // remove the unreachable node from the allocator.
  if (!protobuf::isTerminalState(task->state()) &&
      task->state() != TASK_UNREACHABLE) {
    // We cannot call `Node::recoverResources()` here because
    // it expects the task to be terminal or unreachable.
    usedResources[frameworkId] -= task->resources();
    if (usedResources[frameworkId].empty()) {
      usedResources.erase(frameworkId);
    }
  }

  tasks.at(frameworkId).erase(taskId);
  if (tasks.at(frameworkId).empty()) {
    tasks.erase(frameworkId);
  }

  killedTasks.remove(frameworkId, taskId);
}


void Node::addOperation(Operation* operation)
{
  Result<ResourceProviderID> resourceProviderId =
    getResourceProviderId(operation->info());

  CHECK(!resourceProviderId.isError()) << resourceProviderId.error();

  if (resourceProviderId.isNone()) {
    operations.put(operation->uuid(), operation);
  } else {
    CHECK(resourceProviders.contains(resourceProviderId.get()));

    ResourceProvider& resourceProvider =
      resourceProviders.at(resourceProviderId.get());

    resourceProvider.operations.put(operation->uuid(), operation);
  }

  if (!protobuf::isSpeculativeOperation(operation->info()) &&
      !protobuf::isTerminalState(operation->latest_status().state())) {
    Try<Resources> consumed = protobuf::getConsumedResources(operation->info());

    CHECK_SOME(consumed);

    // There isn't support for non-speculative operations using the
    // operator API. We can assume the framework ID has been set.
    CHECK(operation->has_framework_id());

    usedResources[operation->framework_id()] += consumed.get();
  }
}


void Node::recoverResources(Operation* operation)
{
  // TODO(jieyu): Currently, we do not keep track of used resources
  // for operations that are created by the operator through the
  // operator API endpoint.
  if (!operation->has_framework_id()) {
    return;
  }

  const FrameworkID& frameworkId = operation->framework_id();

  if (protobuf::isSpeculativeOperation(operation->info())) {
    return;
  }

  Try<Resources> consumed = protobuf::getConsumedResources(operation->info());
  CHECK_SOME(consumed);

  CHECK(usedResources[frameworkId].contains(consumed.get()))
    << "Unknown resources " << consumed.get()
    << " of framework " << frameworkId;

  usedResources[frameworkId] -= consumed.get();
  if (usedResources[frameworkId].empty()) {
    usedResources.erase(frameworkId);
  }
}


void Node::removeOperation(Operation* operation)
{
  const UUID& uuid = operation->uuid();

  Result<ResourceProviderID> resourceProviderId =
    getResourceProviderId(operation->info());

  CHECK(!resourceProviderId.isError()) << resourceProviderId.error();

  if (orphanedOperations.contains(uuid)) {
    orphanedOperations.erase(uuid);

    CHECK(!protobuf::isSpeculativeOperation(operation->info()))
      << "Orphaned operations can only be non-speculative";

    // If the orphan is removed before reaching a terminal state,
    // the used resources must be added back into the node's total
    // resources. Terminal orphaned operations are handled by
    // `Controller::updateOperation`.
    if (!protobuf::isTerminalState(operation->latest_status().state())) {
      Try<Resources> consumed =
        protobuf::getConsumedResources(operation->info());

      CHECK_SOME(consumed);

      Resources consumedUnallocated = consumed.get();
      consumedUnallocated.unallocate();

      // NOTE: Non-terminal operations are only removed when the node or
      // resource provider is removed. When the node is removed, the controller
      // will call `allocator->removeNode` in `Controller::_removeNode()`.
      // When a resource provider is removed, the controller will call
      // `allocator->updateNode()` in `Controller::updateNode()`.
      // This means we do not need to update the allocator in this method.
      totalResources += consumedUnallocated;
    }
  } else {
    // Recover the resource used by this operation.
    if (!protobuf::isSpeculativeOperation(operation->info()) &&
        !protobuf::isTerminalState(operation->latest_status().state())) {
      recoverResources(operation);
    }
  }

  // Remove the operation.
  if (resourceProviderId.isNone()) {
    CHECK(operations.contains(uuid))
      << "Unknown operation (uuid: " << uuid << ")"
      << " to node " << *this;

    operations.erase(uuid);
  } else {
    CHECK(resourceProviders.contains(resourceProviderId.get()))
      << "resource provider " << resourceProviderId.get() << " is unknown";

    ResourceProvider& resourceProvider =
      resourceProviders.at(resourceProviderId.get());

    CHECK(resourceProvider.operations.contains(uuid))
      << "Unknown operation (uuid: " << uuid << ")"
      << " to resource provider " << resourceProviderId.get()
      << " on node " << *this;

    resourceProvider.operations.erase(uuid);
  }
}


void Node::markOperationAsOrphan(Operation* operation)
{
  // Only non-speculative operations can be orphaned.
  if (protobuf::isSpeculativeOperation(operation->info())) {
    return;
  }

  LOG(INFO) << "Marking operation " << operation->uuid()
            << (operation->info().has_id()
                ? " (ID: " + operation->info().id().value() + ")"
                : "")
            << (operation->has_node_id()
                ? " (Node: " + operation->node_id().value() + ")"
                : "")
            << (operation->has_framework_id()
                ? " (Framework: " + operation->framework_id().value() + ")"
                : "")
            << " in state " << operation->latest_status().state()
            << " as an orphan";

  orphanedOperations.insert(operation->uuid());

  // Only non-terminal orphans require additional resource math.
  if (protobuf::isTerminalState(operation->latest_status().state())) {
    return;
  }

  // Orphaned operations have no framework, and hence cannot be accounted
  // for within the allocator. Instead, the operation's resources are removed
  // from the node's total resources until the operation terminates.
  recoverResources(operation);

  Try<Resources> consumed = protobuf::getConsumedResources(operation->info());
  CHECK_SOME(consumed);

  Resources consumedUnallocated = consumed.get();
  consumedUnallocated.unallocate();

  CHECK(totalResources.contains(consumedUnallocated))
    << "Unknown resources from orphan operation: " << consumedUnallocated;

  totalResources -= consumedUnallocated;
}


Operation* Node::getOperation(const UUID& uuid) const
{
  if (operations.contains(uuid)) {
    return operations.at(uuid);
  }

  foreachvalue (const ResourceProvider& resourceProvider, resourceProviders) {
    if (resourceProvider.operations.contains(uuid)) {
      return resourceProvider.operations.at(uuid);
    }
  }

  return nullptr;
}


void Node::addOffer(Offer* offer)
{
  CHECK(!offers.contains(offer)) << "Duplicate offer " << offer->id();

  offers.insert(offer);
  offeredResources += offer->resources();
}


void Node::removeOffer(Offer* offer)
{
  CHECK(offers.contains(offer)) << "Unknown offer " << offer->id();

  offeredResources -= offer->resources();
  offers.erase(offer);
}


void Node::addInverseOffer(InverseOffer* inverseOffer)
{
  CHECK(!inverseOffers.contains(inverseOffer))
    << "Duplicate inverse offer " << inverseOffer->id();

  inverseOffers.insert(inverseOffer);
}


void Node::removeInverseOffer(InverseOffer* inverseOffer)
{
  CHECK(inverseOffers.contains(inverseOffer))
    << "Unknown inverse offer " << inverseOffer->id();

  inverseOffers.erase(inverseOffer);
}


bool Node::hasExecutor(const FrameworkID& frameworkId,
                        const ExecutorID& executorId) const
{
  return executors.contains(frameworkId) &&
         executors.at(frameworkId).contains(executorId);
}


void Node::addExecutor(const FrameworkID& frameworkId,
                        const ExecutorInfo& executorInfo)
{
  CHECK(!hasExecutor(frameworkId, executorInfo.executor_id()))
    << "Duplicate executor '" << executorInfo.executor_id()
    << "' of framework " << frameworkId;

  // Verify that Resource.AllocationInfo is set,
  // this should be guaranteed by the controller.
  foreach (const Resource& resource, executorInfo.resources()) {
    CHECK(resource.has_allocation_info());
  }

  executors[frameworkId][executorInfo.executor_id()] = executorInfo;
  usedResources[frameworkId] += executorInfo.resources();
}


void Node::removeExecutor(const FrameworkID& frameworkId,
                           const ExecutorID& executorId)
{
  CHECK(hasExecutor(frameworkId, executorId))
    << "Unknown executor '" << executorId << "' of framework " << frameworkId;

  usedResources[frameworkId] -=
    executors[frameworkId][executorId].resources();
  if (usedResources[frameworkId].empty()) {
    usedResources.erase(frameworkId);
  }

  executors[frameworkId].erase(executorId);
  if (executors[frameworkId].empty()) {
    executors.erase(frameworkId);
  }
}


void Node::apply(const vector<ResourceConversion>& conversions)
{
  Try<Resources> resources = totalResources.apply(conversions);
  CHECK_SOME(resources);

  totalResources = resources.get();

  checkpointedResources = totalResources.filter(needCheckpointing);

  // Also apply the conversion to the explicitly maintained resource
  // provider resources.
  foreach (const ResourceConversion& conversion, conversions) {
    Result<ResourceProviderID> providerId =
      getResourceProviderId(conversion.consumed);

    if (providerId.isNone()) {
      continue;
    }

    CHECK_SOME(providerId);
    CHECK(resourceProviders.contains(providerId.get()));
    ResourceProvider& provider = resourceProviders.at(providerId.get());

    CHECK(provider.totalResources.contains(conversion.consumed));
    provider.totalResources -= conversion.consumed;
    provider.totalResources += conversion.converted;
  }
}


Try<Nothing> Node::update(
    const NodeInfo& _info,
    const string& _version,
    const vector<NodeInfo::Capability>& _capabilities,
    const Resources& _checkpointedResources,
    const Option<UUID>& _resourceVersion)
{
  Try<Resources> resources = applyCheckpointedResources(
      _info.resources(),
      _checkpointedResources);

  // This should be validated during node recovery.
  if (resources.isError()) {
    return Error(resources.error());
  }

  version = _version;
  capabilities = protobuf::node::Capabilities(_capabilities);
  info = _info;
  checkpointedResources = _checkpointedResources;

  // There is a short window here where `totalResources` can have an old value,
  // but it should be relatively short because the node will send
  // an `UpdateNodeMessage` with the new total resources immediately after
  // reregistering in this case.
  totalResources = resources.get();

  resourceVersion = _resourceVersion;

  return Nothing();
}

} // namespace controller {
} // namespace internal {
} // namespace medios {
