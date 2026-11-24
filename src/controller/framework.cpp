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

#include "controller/controller.hpp"

#include "common/heartbeater.hpp"
#include "common/protobuf_utils.hpp"

using process::Failure;
using process::Future;
using process::Owned;
using process::http::authentication::Principal;

using medios::authorization::ActionObject;
using medios::scheduler::OfferConstraints;

namespace medios {
namespace internal {
namespace controller {

Framework::Framework(
    Controller* const controller,
    const Flags& controllerFlags,
    const FrameworkInfo& info,
    OfferConstraints&& offerConstraints,
    const process::UPID& pid,
    const Owned<ObjectApprovers>& approvers,
    const process::Time& time)
  : Framework(
        controller,
        controllerFlags,
        info,
        std::move(offerConstraints),
        CONNECTED,
        true,
        approvers,
        time)
{
  pid_ = pid;
}


Framework::Framework(
    Controller* const controller,
    const Flags& controllerFlags,
    const FrameworkInfo& info,
    OfferConstraints&& offerConstraints,
    const StreamingHttpConnection<v1::scheduler::Event>& http,
    const Owned<ObjectApprovers>& approvers,
    const process::Time& time)
  : Framework(
        controller,
        controllerFlags,
        info,
        std::move(offerConstraints),
        CONNECTED,
        true,
        approvers,
        time)
{
  http_ = http;
}


Framework::Framework(
    Controller* const controller,
    const Flags& controllerFlags,
    const FrameworkInfo& info)
  : Framework(
        controller,
        controllerFlags,
        info,
        OfferConstraints{},
        RECOVERED,
        false,
        nullptr,
        process::Time())
{}


Framework::Framework(
    Controller* const _controller,
    const Flags& controllerFlags,
    const FrameworkInfo& _info,
    OfferConstraints&& offerConstraints,
    State state,
    bool active_,
    const Owned<ObjectApprovers>& approvers,
    const process::Time& time)
  : controller(_controller),
    info(_info),
    roles(protobuf::framework::getRoles(_info)),
    capabilities(_info.capabilities()),
    registeredTime(time),
    reregisteredTime(time),
    completedTasks(controllerFlags.max_completed_tasks_per_framework),
    unreachableTasks(controllerFlags.max_unreachable_tasks_per_framework),
    metrics(_info, controllerFlags.publish_per_framework_metrics),
    active_(active_),
    state(state),
    objectApprovers(approvers),
    offerConstraints_(std::move(offerConstraints))
{
  CHECK(_info.has_id());

  setState(state);

  foreach (const std::string& role, roles) {
    // NOTE: It's possible that we're already being tracked under the role
    // because a framework can unsubscribe from a role while it still has
    // resources allocated to the role.
    if (!isTrackedUnderRole(role)) {
      trackUnderRole(role);
    }
  }
}


Framework::~Framework()
{
  disconnect();
}


Task* Framework::getTask(const TaskID& taskId)
{
  if (tasks.count(taskId) > 0) {
    return tasks[taskId];
  }

  return nullptr;
}


void Framework::addTask(Task* task)
{
  CHECK(!tasks.contains(task->task_id()))
    << "Duplicate task " << task->task_id()
    << " of framework " << task->framework_id();

  // Verify that Resource.AllocationInfo is set,
  // this should be guaranteed by the controller.
  foreach (const Resource& resource, task->resources()) {
    CHECK(resource.has_allocation_info());
  }

  tasks[task->task_id()] = task;

  // A task recovered from a node's re-registration was built by the node,
  // which records no framework principal (the controller is the only
  // stamper, DR-037). Stamp it here from the framework's bound principal
  // so the record is the same before and after a controller failover.
  if (!task->has_framework_principal() && info.has_principal()) {
    task->set_framework_principal(info.principal());
  }

  // Unreachable tasks should be added via `addUnreachableTask`.
  CHECK(task->state() != TASK_UNREACHABLE)
    << "Task '" << task->task_id() << "' of framework " << id()
    << " added in TASK_UNREACHABLE state";

  // Since we track terminal but unacknowledged tasks within
  // `tasks` rather than `completedTasks`, we need to handle
  // them here: don't count them as consuming resources.
  //
  // TODO(bmahler): Users currently get confused because
  // terminal tasks can show up as "active" tasks in the UI and
  // endpoints. Ideally, we show the terminal unacknowledged
  // tasks as "completed" as well.
  if (!protobuf::isTerminalState(task->state())) {
    // Note that we explicitly convert from protobuf to `Resources` once
    // and then use the result for calculations to avoid performance penalty
    // for multiple conversions and validations implied by `+=` with protobuf
    // arguments.
    // Conversion is safe, as resources have already passed validation.
    const Resources resources = task->resources();
    totalUsedResources += resources;
    usedResources[task->node_id()] += resources;

    // It's possible that we're not tracking the task's role for
    // this framework if the role is absent from the framework's
    // set of roles. In this case, we track the role's allocation
    // for this framework.
    CHECK(!task->resources().empty());
    const std::string& role =
      task->resources().begin()->allocation_info().role();

    if (!isTrackedUnderRole(role)) {
      trackUnderRole(role);
    }
  }

  metrics.incrementTaskState(task->state());

  if (!controller->subscribers.subscribed.empty()) {
    controller->subscribers.send(
        protobuf::controller::event::createTaskAdded(*task),
        info);
  }
}


void Framework::recoverResources(Task* task)
{
  CHECK(tasks.contains(task->task_id()))
    << "Unknown task " << task->task_id()
    << " of framework " << task->framework_id();

  totalUsedResources -= task->resources();
  usedResources[task->node_id()] -= task->resources();
  if (usedResources[task->node_id()].empty()) {
    usedResources.erase(task->node_id());
  }

  // If we are no longer subscribed to the role to which these resources are
  // being returned to, and we have no more resources allocated to us for that
  // role, stop tracking the framework under the role.
  CHECK(!task->resources().empty());
  const std::string& role =
    task->resources().begin()->allocation_info().role();

  auto allocatedToRole = [&role](const Resource& resource) {
    return resource.allocation_info().role() == role;
  };

  if (roles.count(role) == 0 &&
      totalUsedResources.filter(allocatedToRole).empty()) {
    CHECK(totalOfferedResources.filter(allocatedToRole).empty());
    untrackUnderRole(role);
  }
}


void Framework::addCompletedTask(Task&& task)
{
  // TODO(neilc): We currently allow frameworks to reuse the task
  // IDs of completed tasks (although this is discouraged). This
  // means that there might be multiple completed tasks with the
  // same task ID. We should consider rejecting attempts to reuse
  // task IDs (MESOS-6779).
  completedTasks.push_back(process::Owned<Task>(new Task(std::move(task))));
}


void Framework::addUnreachableTask(const Task& task)
{
  // TODO(adam-medios): Check if unreachable task already exists.
  unreachableTasks.set(task.task_id(), process::Owned<Task>(new Task(task)));
}


void Framework::removeTask(Task* task, bool unreachable)
{
  CHECK(tasks.contains(task->task_id()))
    << "Unknown task " << task->task_id()
    << " of framework " << task->framework_id();

  // The invariant here is that the controller will have already called
  // `recoverResources()` prior to removing terminal or unreachable tasks.
  if (!protobuf::isTerminalState(task->state()) &&
      task->state() != TASK_UNREACHABLE) {
    recoverResources(task);
  }

  if (unreachable) {
    addUnreachableTask(*task);
  } else {
    CHECK(task->state() != TASK_UNREACHABLE);

    // TODO(bmahler): This moves a potentially non-terminal task into
    // the completed list!
    addCompletedTask(Task(*task));
  }

  tasks.erase(task->task_id());
}


void Framework::addOffer(Offer* offer)
{
  CHECK(!offers.contains(offer)) << "Duplicate offer " << offer->id();
  offers.insert(offer);
  totalOfferedResources += offer->resources();
  offeredResources[offer->node_id()] += offer->resources();
}


void Framework::removeOffer(Offer* offer)
{
  CHECK(offers.find(offer) != offers.end())
    << "Unknown offer " << offer->id();

  totalOfferedResources -= offer->resources();
  offeredResources[offer->node_id()] -= offer->resources();
  if (offeredResources[offer->node_id()].empty()) {
    offeredResources.erase(offer->node_id());
  }

  offers.erase(offer);
}


void Framework::addInverseOffer(InverseOffer* inverseOffer)
{
  CHECK(!inverseOffers.contains(inverseOffer))
    << "Duplicate inverse offer " << inverseOffer->id();
  inverseOffers.insert(inverseOffer);
}


void Framework::removeInverseOffer(InverseOffer* inverseOffer)
{
  CHECK(inverseOffers.contains(inverseOffer))
    << "Unknown inverse offer " << inverseOffer->id();

  inverseOffers.erase(inverseOffer);
}


bool Framework::hasExecutor(
    const NodeID& nodeId,
    const ExecutorID& executorId)
{
  return executors.contains(nodeId) &&
    executors[nodeId].contains(executorId);
}


void Framework::addExecutor(
    const NodeID& nodeId,
    const ExecutorInfo& executorInfo)
{
  CHECK(!hasExecutor(nodeId, executorInfo.executor_id()))
    << "Duplicate executor '" << executorInfo.executor_id()
    << "' on node " << nodeId;

  // Verify that Resource.AllocationInfo is set,
  // this should be guaranteed by the controller.
  foreach (const Resource& resource, executorInfo.resources()) {
    CHECK(resource.has_allocation_info());
  }

  executors[nodeId][executorInfo.executor_id()] = executorInfo;
  totalUsedResources += executorInfo.resources();
  usedResources[nodeId] += executorInfo.resources();

  // It's possible that we're not tracking the task's role for
  // this framework if the role is absent from the framework's
  // set of roles. In this case, we track the role's allocation
  // for this framework.
  if (!executorInfo.resources().empty()) {
    const std::string& role =
      executorInfo.resources().begin()->allocation_info().role();

    if (!isTrackedUnderRole(role)) {
      trackUnderRole(role);
    }
  }
}


void Framework::removeExecutor(
    const NodeID& nodeId,
    const ExecutorID& executorId)
{
  CHECK(hasExecutor(nodeId, executorId))
    << "Unknown executor '" << executorId
    << "' of framework " << id()
    << " of node " << nodeId;

  const ExecutorInfo& executorInfo = executors[nodeId][executorId];

  totalUsedResources -= executorInfo.resources();
  usedResources[nodeId] -= executorInfo.resources();
  if (usedResources[nodeId].empty()) {
    usedResources.erase(nodeId);
  }

  // If we are no longer subscribed to the role to which these resources are
  // being returned to, and we have no more resources allocated to us for that
  // role, stop tracking the framework under the role.
  if (!executorInfo.resources().empty()) {
    const std::string& role =
      executorInfo.resources().begin()->allocation_info().role();

    auto allocatedToRole = [&role](const Resource& resource) {
      return resource.allocation_info().role() == role;
    };

    if (roles.count(role) == 0 &&
        totalUsedResources.filter(allocatedToRole).empty()) {
      CHECK(totalOfferedResources.filter(allocatedToRole).empty());
      untrackUnderRole(role);
    }
  }

  executors[nodeId].erase(executorId);
  if (executors[nodeId].empty()) {
    executors.erase(nodeId);
  }
}


void Framework::addOperation(Operation* operation)
{
  CHECK(operation->has_framework_id());

  const FrameworkID& frameworkId = operation->framework_id();

  const UUID& uuid = operation->uuid();

  CHECK(!operations.contains(uuid))
    << "Duplicate operation '" << operation->info().id()
    << "' (uuid: " << uuid << ") "
    << "of framework " << frameworkId;

  operations.put(uuid, operation);

  if (operation->info().has_id()) {
    operationUUIDs.put(operation->info().id(), uuid);
  }

  if (!protobuf::isSpeculativeOperation(operation->info()) &&
      !protobuf::isTerminalState(operation->latest_status().state())) {
    Try<Resources> consumed =
      protobuf::getConsumedResources(operation->info());
    CHECK_SOME(consumed);

    CHECK(operation->has_node_id())
      << "External resource provider is not supported yet";

    const NodeID& nodeId = operation->node_id();

    totalUsedResources += consumed.get();
    usedResources[nodeId] += consumed.get();

    // It's possible that we're not tracking the role from the
    // resources in the operation for this framework if the role is
    // absent from the framework's set of roles. In this case, we
    // track the role's allocation for this framework.
    foreachkey (const std::string& role, consumed->allocations()) {
      if (!isTrackedUnderRole(role)) {
        trackUnderRole(role);
      }
    }
  }
}


Option<Operation*> Framework::getOperation(const OperationID& id)
{
  Option<UUID> uuid = operationUUIDs.get(id);

  if (uuid.isNone()) {
    return None();
  }

  Option<Operation*> operation = operations.get(uuid.get());

  CHECK_SOME(operation);

  return operation;
}


void Framework::recoverResources(Operation* operation)
{
  CHECK(operation->has_node_id())
    << "External resource provider is not supported yet";

  const NodeID& nodeId = operation->node_id();

  if (protobuf::isSpeculativeOperation(operation->info())) {
    return;
  }

  Try<Resources> consumed = protobuf::getConsumedResources(operation->info());
  CHECK_SOME(consumed);

  CHECK(totalUsedResources.contains(consumed.get()))
    << "Tried to recover resources " << consumed.get()
    << " which do not seem used";

  CHECK(usedResources[nodeId].contains(consumed.get()))
    << "Tried to recover resources " << consumed.get() << " of node "
    << nodeId << " which do not seem used";

  totalUsedResources -= consumed.get();
  usedResources[nodeId] -= consumed.get();
  if (usedResources[nodeId].empty()) {
    usedResources.erase(nodeId);
  }

  // If we are no longer subscribed to the role to which these
  // resources are being returned to, and we have no more resources
  // allocated to us for that role, stop tracking the framework
  // under the role.
  foreachkey (const std::string& role, consumed->allocations()) {
    auto allocatedToRole = [&role](const Resource& resource) {
      return resource.allocation_info().role() == role;
    };

    if (roles.count(role) == 0 &&
        totalUsedResources.filter(allocatedToRole).empty()) {
      CHECK(totalOfferedResources.filter(allocatedToRole).empty());
      untrackUnderRole(role);
    }
  }
}


void Framework::removeOperation(Operation* operation)
{
  const UUID& uuid = operation->uuid();

  CHECK(operations.contains(uuid))
    << "Unknown operation '" << operation->info().id()
    << "' (uuid: " << uuid << ") "
    << "of framework " << operation->framework_id();

  if (!protobuf::isSpeculativeOperation(operation->info()) &&
      !protobuf::isTerminalState(operation->latest_status().state())) {
    recoverResources(operation);
  }

  if (operation->info().has_id()) {
    operationUUIDs.erase(operation->info().id());
  }

  operations.erase(uuid);
}


void Framework::update(
    const FrameworkInfo& newInfo,
    OfferConstraints&& offerConstraints)
{
  // We only merge 'info' from the same framework 'id'.
  CHECK_EQ(info.id(), newInfo.id());

  // Fields 'principal', 'user' and 'update' are immutable.
  // They should be validated before calling this method.
  // A framework recovered from a re-registering node across a pre-#537
  // upgrade may have had no principal until its scheduler re-subscribes (#697).
  //
  // TODO(asekretenko): Make it possible to update 'user'
  // and 'checkpoint' as per design doc in MESOS-703.
  if (info.has_principal() && !info.principal().empty()) {
    CHECK_EQ(info.principal(), newInfo.principal());
  }
  CHECK_EQ(info.user(), newInfo.user());
  CHECK_EQ(info.checkpoint(), newInfo.checkpoint());

  info.CopyFrom(newInfo);
  offerConstraints_ = std::move(offerConstraints);

  // Save the old list of roles for later.
  std::set<std::string> oldRoles = roles;
  roles = protobuf::framework::getRoles(info);
  capabilities = protobuf::framework::Capabilities(info.capabilities());

  const std::set<std::string>& newRoles = roles;

  const std::set<std::string> removedRoles = [&]() {
    std::set<std::string> result = oldRoles;
    foreach (const std::string& role, newRoles) {
      result.erase(role);
    }
    return result;
  }();

  foreach (const std::string& role, removedRoles) {
    auto allocatedToRole = [&role](const Resource& resource) {
      return resource.allocation_info().role() == role;
    };

    // Stop tracking the framework under this role if there are
    // no longer any resources allocated to it.
    if (totalUsedResources.filter(allocatedToRole).empty()) {
      CHECK(totalOfferedResources.filter(allocatedToRole).empty());
      untrackUnderRole(role);
    }
  }

  const std::set<std::string> addedRoles = [&]() {
    std::set<std::string> result = newRoles;
    foreach (const std::string& role, oldRoles) {
      result.erase(role);
    }
    return result;
  }();

  foreach (const std::string& role, addedRoles) {
    // NOTE: It's possible that we're already tracking this framework
    // under the role because a framework can unsubscribe from a role
    // while it still has resources allocated to the role.
    if (!isTrackedUnderRole(role)) {
      trackUnderRole(role);
    }
  }
}


void Framework::updateConnection(
    const process::UPID& newPid,
    const Owned<ObjectApprovers>& objectApprovers_)
{
  // Cleanup the old connection state if exists.
  disconnect();
  CHECK_NONE(http_);

  // TODO(benh): unlink(oldPid);
  pid_ = newPid;
  objectApprovers = objectApprovers_;
  setState(State::CONNECTED);
}


void Framework::updateConnection(
    const StreamingHttpConnection<v1::scheduler::Event>& newHttp,
    const Owned<ObjectApprovers>& objectApprovers_)
{
  // Note that controller creates a new HTTP connection for every
  // subscribe request, so 'newHttp' should always be different
  // from 'http'.
  CHECK(http_.isNone() || newHttp.writer != http_->writer);

  // Cleanup the old connection state if exists.
  disconnect();

  // TODO(benh): unlink(oldPid) if this is an upgrade from PID to HTTP.
  pid_ = None();

  CHECK_NONE(http_);
  http_ = newHttp;
  objectApprovers = objectApprovers_;
  setState(State::CONNECTED);
}


bool Framework::activate()
{
  bool noop = active_;
  active_ = true;
  return !noop;
}


bool Framework::deactivate()
{
  bool noop = !active_;
  active_ = false;
  return !noop;
}


bool Framework::disconnect()
{
  if (state != State::CONNECTED) {
    CHECK(http_.isNone());
    return false;
  }

  if (http_.isSome() && connected() && !http_->close()) {
    LOG(WARNING) << "Failed to close HTTP pipe for " << *this;
  }

  http_ = None();
  heartbeater.reset();

  // `ObjectApprover`s are kept up-to-date by authorizer, which potentially
  // entails continious interaction with an external IAM. Hence, we do not
  // want to keep them alive if there is no subscribed scheduler.
  objectApprovers.reset();

  setState(State::DISCONNECTED);
  return true;
}


void Framework::heartbeat()
{
  CHECK_SOME(http_);

  // TODO(vinod): Make heartbeat interval configurable and include
  // this information in the SUBSCRIBED response.
  scheduler::Event event;
  event.set_type(scheduler::Event::HEARTBEAT);

  heartbeater.reset(
      new ResponseHeartbeater<scheduler::Event, v1::scheduler::Event>(
          "framework " + stringify(info.id()),
          event,
          http_.get(),
          DEFAULT_HEARTBEAT_INTERVAL,
          None(),
          [this, event]() {
            this->metrics.incrementEvent(event);
          }));
}


bool Framework::isTrackedUnderRole(const std::string& role) const
{
  CHECK(controller->isAllowlistedRole(role))
    << "Unknown role '" << role << "'" << " of framework " << *this;

  return controller->roles.contains(role) &&
         controller->roles.at(role)->frameworks.contains(id());
}


void Framework::trackUnderRole(const std::string& role)
{
  CHECK(controller->isAllowlistedRole(role))
    << "Unknown role '" << role << "'" << " of framework " << *this;

  CHECK(!isTrackedUnderRole(role));

  if (!controller->roles.contains(role)) {
    controller->roles[role] = new Role(controller, role);
  }
  controller->roles.at(role)->addFramework(this);
}


void Framework::untrackUnderRole(const std::string& role)
{
  CHECK(controller->isAllowlistedRole(role))
    << "Unknown role '" << role << "'" << " of framework " << *this;

  CHECK(isTrackedUnderRole(role));

  // NOTE: Ideally we would also `CHECK` that we're not currently subscribed
  // to the role. We don't do this currently because this function is used in
  // `Controller::removeFramework` where we're still subscribed to `roles`.

  auto allocatedToRole = [&role](const Resource& resource) {
    return resource.allocation_info().role() == role;
  };

  CHECK(totalUsedResources.filter(allocatedToRole).empty());
  CHECK(totalOfferedResources.filter(allocatedToRole).empty());

  controller->roles.at(role)->removeFramework(this);
  if (controller->roles.at(role)->frameworks.empty()) {
    delete controller->roles.at(role);
    controller->roles.erase(role);
  }
}


void Framework::setState(Framework::State _state)
{
  state = _state;
  metrics.subscribed = state == Framework::State::CONNECTED ? 1 : 0;
}


Try<bool> Framework::approved(const ActionObject& actionObject) const
{
  CHECK(objectApprovers.get() != nullptr)
    << "Framework " << *this << " has no ObjectApprovers"
    << " (attempt to call approved() for a disconnected framework?)";

  return objectApprovers->approved(
      actionObject.action(),
      actionObject.object().getOrElse(authorization::Object()));
}


Future<Owned<ObjectApprovers>> Framework::createObjectApprovers(
    const Option<Authorizer*>& authorizer,
    const FrameworkInfo& frameworkInfo)
{
  return ObjectApprovers::create(
      authorizer,
      frameworkInfo.has_principal()
        ? Option<Principal>(frameworkInfo.principal())
        : Option<Principal>::none(),
      {authorization::REGISTER_FRAMEWORK,
       authorization::RUN_TASK,
       authorization::ACT_FOR_PRINCIPAL,
       authorization::UNRESERVE_RESOURCES,
       authorization::RESERVE_RESOURCES,
       authorization::CREATE_VOLUME,
       authorization::DESTROY_VOLUME,
       authorization::RESIZE_VOLUME,
       authorization::CREATE_MOUNT_DISK,
       authorization::CREATE_BLOCK_DISK,
       authorization::DESTROY_MOUNT_DISK,
       authorization::DESTROY_BLOCK_DISK,
       authorization::DESTROY_RAW_DISK});
}


} // namespace controller {
} // namespace internal {
} // namespace medios {
