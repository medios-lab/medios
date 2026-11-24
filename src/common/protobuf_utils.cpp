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

#include "common/protobuf_utils.hpp"

#include <grp.h>
#include <pwd.h>

#include <ostream>
#include <vector>

#include <medios/node/isolator.hpp>

#include <medios/type_utils.hpp>

#include <process/clock.hpp>
#include <process/pid.hpp>

#include <stout/adaptor.hpp>
#include <stout/duration.hpp>
#include <stout/foreach.hpp>
#include <stout/net.hpp>
#include <stout/stringify.hpp>
#include <stout/unreachable.hpp>
#include <stout/uuid.hpp>

#include <stout/os/permissions.hpp>


#include "common/http.hpp"
#include "common/resources_utils.hpp"

#include "controller/controller.hpp"
#include "controller/constants.hpp"

#include "messages/messages.hpp"

using std::ostream;
using std::set;
using std::string;
using std::vector;

using google::protobuf::Map;
using google::protobuf::RepeatedPtrField;

using medios::authorization::VIEW_ROLE;

using medios::node::ContainerFileOperation;
using medios::node::ContainerLimitation;
using medios::node::ContainerMountInfo;
using medios::node::ContainerState;

using process::Owned;
using process::UPID;

namespace medios {
namespace internal {
namespace protobuf {

// If `descriptor` is not a descriptor of a protobuf union,
// this constructor will abort the process.
UnionValidator::UnionValidator(const google::protobuf::Descriptor* descriptor)
{
  const auto* typeFieldDescriptor = descriptor->FindFieldByName("type");
  CHECK_NOTNULL(typeFieldDescriptor);

  typeDescriptor_ = typeFieldDescriptor->enum_type();
  CHECK_NOTNULL(typeDescriptor_);

  const auto* unknownTypeValueDescriptor =
    typeDescriptor_->FindValueByNumber(0);
  if (unknownTypeValueDescriptor != nullptr) {
    CHECK_EQ(unknownTypeValueDescriptor->name(), "UNKNOWN");
  }

  for (int index = 0; index < typeDescriptor_->value_count(); index++) {
    const auto* typeValueDescriptor = typeDescriptor_->value(index);
    if (typeValueDescriptor->number() == 0) {
      // We are skipping the "UNKNOWN" value of the enum.
      continue;
    }

    const auto* fieldDescriptor =
      descriptor->FindFieldByName(strings::lower(typeValueDescriptor->name()));

    CHECK_NOTNULL(fieldDescriptor);
    unionFieldDescriptors_.emplace_back(
        typeValueDescriptor->number(), fieldDescriptor);
  }
}


Option<Error> UnionValidator::validate(
  const int messageTypeNumber, const google::protobuf::Message& message) const
{
  const auto* reflection = message.GetReflection();
  for (const auto& item : unionFieldDescriptors_) {
    const auto typeNumber = item.first;
    const auto* fieldDescriptor = item.second;
    if (
        messageTypeNumber != typeNumber &&
        reflection->HasField(message, fieldDescriptor)) {
      const auto* descr = typeDescriptor_->FindValueByNumber(messageTypeNumber);
      return Error(
          "Protobuf union `" + message.GetDescriptor()->full_name() +
          "` with `Type == " +
          (descr == nullptr ? string("<UNKNOWN>") : descr->name()) +
          "` should not have the field `" +
          fieldDescriptor->name() + "` set.");
    }
  }
  return None();
}


bool frameworkHasCapability(
    const FrameworkInfo& framework,
    FrameworkInfo::Capability::Type capability)
{
  foreach (const FrameworkInfo::Capability& c,
           framework.capabilities()) {
    if (c.type() == capability) {
      return true;
    }
  }

  return false;
}


bool isTerminalState(const TaskState& state)
{
  switch (state) {
    case TASK_FINISHED:
    case TASK_FAILED:
    case TASK_KILLED:
    case TASK_LOST:
    case TASK_ERROR:
    case TASK_DROPPED:
    case TASK_GONE:
    case TASK_GONE_BY_OPERATOR:
      return true;
    case TASK_KILLING:
    case TASK_STAGING:
    case TASK_STARTING:
    case TASK_RUNNING:
    case TASK_UNREACHABLE:
    case TASK_UNKNOWN:
      return false;
  }

  UNREACHABLE();
}


StatusUpdate createStatusUpdate(
    const FrameworkID& frameworkId,
    const Option<NodeID>& nodeId,
    const TaskID& taskId,
    const TaskState& state,
    const TaskStatus::Source& source,
    const Option<id::UUID>& uuid,
    const string& message,
    const Option<TaskStatus::Reason>& reason,
    const Option<ExecutorID>& executorId,
    const Option<bool>& healthy,
    const Option<CheckStatusInfo>& checkStatus,
    const Option<Labels>& labels,
    const Option<ContainerStatus>& containerStatus,
    const Option<TimeInfo>& unreachableTime,
    const Option<Resources>& limitedResources,
    const Option<TaskStatus::Progress>& progress)
{
  StatusUpdate update;

  update.set_timestamp(process::Clock::now().secs());
  update.mutable_framework_id()->MergeFrom(frameworkId);

  if (nodeId.isSome()) {
    update.mutable_node_id()->MergeFrom(nodeId.get());
  }

  if (executorId.isSome()) {
    update.mutable_executor_id()->MergeFrom(executorId.get());
  }

  // TODO(alexr): Use `createTaskStatus()` instead
  // once `UUID` is required in this function.
  TaskStatus* status = update.mutable_status();
  status->mutable_task_id()->MergeFrom(taskId);

  if (nodeId.isSome()) {
    status->mutable_node_id()->MergeFrom(nodeId.get());
  }

  status->set_state(state);
  status->set_source(source);
  status->set_message(message);
  status->set_timestamp(update.timestamp());

  if (uuid.isSome()) {
    update.set_uuid(uuid->toBytes());
    status->set_uuid(uuid->toBytes());
  }

  if (reason.isSome()) {
    status->set_reason(reason.get());
  }

  if (healthy.isSome()) {
    status->set_healthy(healthy.get());
  }

  if (checkStatus.isSome()) {
    status->mutable_check_status()->CopyFrom(checkStatus.get());
  }

  if (progress.isSome()) {
    status->mutable_progress()->CopyFrom(progress.get());
  }

  if (labels.isSome()) {
    status->mutable_labels()->CopyFrom(labels.get());
  }

  if (containerStatus.isSome()) {
    status->mutable_container_status()->CopyFrom(containerStatus.get());
  }

  if (unreachableTime.isSome()) {
    status->mutable_unreachable_time()->CopyFrom(unreachableTime.get());
  }

  if (limitedResources.isSome()) {
    // Check that we are only sending the `Limitation` field when the
    // reason is a container limitation.
    CHECK_SOME(reason);
    CHECK(
        reason.get() == TaskStatus::REASON_CONTAINER_LIMITATION ||
        reason.get() == TaskStatus::REASON_CONTAINER_LIMITATION_DISK ||
        reason.get() == TaskStatus::REASON_CONTAINER_LIMITATION_MEMORY)
      << reason.get();

    status->mutable_limitation()->mutable_resources()->CopyFrom(
        limitedResources.get());
  }

  return update;
}


StatusUpdate createStatusUpdate(
    const FrameworkID& frameworkId,
    const TaskStatus& status,
    const Option<NodeID>& nodeId)
{
  StatusUpdate update;

  update.mutable_framework_id()->MergeFrom(frameworkId);

  if (status.has_executor_id()) {
    update.mutable_executor_id()->MergeFrom(status.executor_id());
  }

  update.mutable_status()->MergeFrom(status);

  if (nodeId.isSome()) {
    update.mutable_node_id()->MergeFrom(nodeId.get());

    // We also populate `TaskStatus.node_id` if the executor
    // did not set it.
    if (!status.has_node_id()) {
      update.mutable_status()->mutable_node_id()->MergeFrom(nodeId.get());
    }
  }

  if (!status.has_timestamp()) {
    update.set_timestamp(process::Clock::now().secs());
  } else {
    update.set_timestamp(status.timestamp());
  }

  if (status.has_uuid()) {
    update.set_uuid(status.uuid());
  }

  return update;
}


TaskStatus createTaskStatus(
    const TaskID& taskId,
    const TaskState& state,
    const id::UUID& uuid,
    double timestamp)
{
  TaskStatus status;

  status.set_uuid(uuid.toBytes());
  status.set_timestamp(timestamp);
  status.mutable_task_id()->CopyFrom(taskId);
  status.set_state(state);

  return status;
}


TaskStatus createTaskStatus(
    TaskStatus status,
    const id::UUID& uuid,
    double timestamp,
    const Option<TaskState>& state,
    const Option<string>& message,
    const Option<TaskStatus::Source>& source,
    const Option<TaskStatus::Reason>& reason,
    const Option<string>& data,
    const Option<bool>& healthy,
    const Option<CheckStatusInfo>& checkStatus,
    const Option<Labels>& labels,
    const Option<ContainerStatus>& containerStatus,
    const Option<TimeInfo>& unreachableTime)
{
  status.set_uuid(uuid.toBytes());
  status.set_timestamp(timestamp);

  if (state.isSome()) {
    status.set_state(state.get());
  }

  if (message.isSome()) {
    status.set_message(message.get());
  }

  if (source.isSome()) {
    status.set_source(source.get());
  }

  if (reason.isSome()) {
    status.set_reason(reason.get());
  }

  if (data.isSome()) {
    status.set_data(data.get());
  }

  if (healthy.isSome()) {
    status.set_healthy(healthy.get());
  }

  if (checkStatus.isSome()) {
    status.mutable_check_status()->CopyFrom(checkStatus.get());
  }

  if (labels.isSome()) {
    status.mutable_labels()->CopyFrom(labels.get());
  }

  if (containerStatus.isSome()) {
    status.mutable_container_status()->CopyFrom(containerStatus.get());
  }

  if (unreachableTime.isSome()) {
    status.mutable_unreachable_time()->CopyFrom(unreachableTime.get());
  }

  return status;
}


Task createTask(
    const TaskInfo& task,
    const TaskState& state,
    const FrameworkID& frameworkId,
    const Option<string>& frameworkPrincipal)
{
  Task t;
  t.mutable_framework_id()->CopyFrom(frameworkId);

  if (task.has_principal()) {
    t.set_principal(task.principal());
  }

  if (frameworkPrincipal.isSome()) {
    t.set_framework_principal(frameworkPrincipal.get());
  }

  t.set_state(state);
  t.set_name(task.name());
  t.mutable_task_id()->CopyFrom(task.task_id());
  t.mutable_node_id()->CopyFrom(task.node_id());
  t.mutable_resources()->CopyFrom(task.resources());
  *t.mutable_limits() = task.limits();

  if (task.has_executor()) {
    t.mutable_executor_id()->CopyFrom(task.executor().executor_id());
  }

  if (task.has_labels()) {
    t.mutable_labels()->CopyFrom(task.labels());
  }

  if (task.has_discovery()) {
    t.mutable_discovery()->CopyFrom(task.discovery());
  }

  if (task.has_container()) {
    t.mutable_container()->CopyFrom(task.container());
  }

  if (task.has_health_check()) {
    t.mutable_health_check()->CopyFrom(task.health_check());
  }

  if (task.has_kill_policy()) {
    t.mutable_kill_policy()->CopyFrom(task.kill_policy());
  }

  if (task.has_progress_policy()) {
    t.mutable_progress_policy()->CopyFrom(task.progress_policy());
  }

  t.mutable_observers()->CopyFrom(task.observers());

  if (task.has_finish()) {
    t.mutable_finish()->CopyFrom(task.finish());
  }

  // Copy `user` if set.
  if (task.has_command() && task.command().has_user()) {
    t.set_user(task.command().user());
  } else if (task.has_executor() && task.executor().command().has_user()) {
    t.set_user(task.executor().command().user());
  }

  return t;
}


vector<TaskData> storesOf(const Task& task)
{
  vector<TaskData> stores;

  if (!task.has_container()) {
    return stores;
  }

  foreach (const Volume& volume, task.container().volumes()) {
    if (!volume.has_source() ||
        volume.source().type() != Volume::Source::STORE) {
      continue;
    }

    TaskData data;
    data.set_store(volume.source().store().name());
    data.set_container_path(volume.container_path());
    data.set_mode(volume.mode());
    data.set_exclusive(
        volume.source().store().exclusive() && volume.mode() == Volume::RW);
    data.mutable_node_id()->CopyFrom(task.node_id());

    stores.push_back(data);
  }

  return stores;
}


Option<bool> getTaskHealth(const Task& task)
{
  Option<bool> healthy = None();
  if (task.statuses_size() > 0) {
    // The statuses list only keeps the most recent `TaskStatus` for
    // each state, and appends later statuses at the end. Thus the last
    // status is either a terminal state (where health is irrelevant),
    // or the latest TASK_RUNNING status.
    const TaskStatus& lastStatus = task.statuses(task.statuses_size() - 1);
    if (lastStatus.has_healthy()) {
      healthy = lastStatus.healthy();
    }
  }
  return healthy;
}


Option<TaskStatus::Progress> getTaskProgress(const Task& task)
{
  // The statuses list only keeps the most recent `TaskStatus` for each
  // state, and appends later states at the end. Unlike health, progress
  // keeps its meaning after the task is terminal: how far a task got
  // before it failed or was killed for stalling is the whole point of
  // recording it. So this scans back for the most recent status that
  // carried progress, as `getTaskContainerStatus()` does, rather than
  // reading only the last status.
  foreach (const TaskStatus& status, adaptor::reverse(task.statuses())) {
    if (status.has_progress()) {
      return status.progress();
    }
  }
  return None();
}


Option<CheckStatusInfo> getTaskCheckStatus(const Task& task)
{
  Option<CheckStatusInfo> checkStatus = None();
  if (task.statuses_size() > 0) {
    // The statuses list only keeps the most recent `TaskStatus` for
    // each state, and appends later statuses at the end. Thus the last
    // status is either a terminal state (where check is irrelevant),
    // or the latest TASK_RUNNING status.
    const TaskStatus& lastStatus = task.statuses(task.statuses_size() - 1);
    if (lastStatus.has_check_status()) {
      checkStatus = lastStatus.check_status();
    }
  }
  return checkStatus;
}


Option<ContainerStatus> getTaskContainerStatus(const Task& task)
{
  // The statuses list only keeps the most recent TaskStatus for
  // each state, and appends later states at the end. Let's find
  // the most recent TaskStatus with a valid container_status.
  foreach (const TaskStatus& status, adaptor::reverse(task.statuses())) {
    if (status.has_container_status()) {
      return status.container_status();
    }
  }
  return None();
}


bool isTerminalState(const OperationState& state)
{
  switch (state) {
    case OPERATION_DROPPED:
    case OPERATION_ERROR:
    case OPERATION_FAILED:
    case OPERATION_FINISHED:
    case OPERATION_GONE_BY_OPERATOR:
      return true;
    case OPERATION_PENDING:
    case OPERATION_RECOVERING:
    case OPERATION_UNKNOWN:
    case OPERATION_UNREACHABLE:
    case OPERATION_UNSUPPORTED:
      return false;
  }

  UNREACHABLE();
}


OperationStatus createOperationStatus(
    const OperationState& state,
    const Option<OperationID>& operationId,
    const Option<string>& message,
    const Option<Resources>& convertedResources,
    const Option<id::UUID>& uuid,
    const Option<NodeID>& nodeId,
    const Option<ResourceProviderID>& resourceProviderId)
{
  OperationStatus status;
  status.set_state(state);

  if (operationId.isSome()) {
    status.mutable_operation_id()->CopyFrom(operationId.get());
  }

  if (message.isSome()) {
    status.set_message(message.get());
  }

  if (convertedResources.isSome()) {
    status.mutable_converted_resources()->CopyFrom(convertedResources.get());
  }

  if (uuid.isSome()) {
    status.mutable_uuid()->CopyFrom(protobuf::createUUID(uuid.get()));
  }

  if (nodeId.isSome()) {
    status.mutable_node_id()->CopyFrom(nodeId.get());
  }

  if (resourceProviderId.isSome()) {
    status.mutable_resource_provider_id()->CopyFrom(resourceProviderId.get());
  }

  return status;
}


Operation createOperation(
    const Offer::Operation& info,
    const OperationStatus& latestStatus,
    const Option<FrameworkID>& frameworkId,
    const Option<NodeID>& nodeId,
    const Option<UUID>& operationUUID)
{
  Operation operation;
  if (frameworkId.isSome()) {
    operation.mutable_framework_id()->CopyFrom(frameworkId.get());
  }
  if (nodeId.isSome()) {
    operation.mutable_node_id()->CopyFrom(nodeId.get());
  }
  operation.mutable_info()->CopyFrom(info);
  operation.mutable_latest_status()->CopyFrom(latestStatus);
  if (operationUUID.isSome()) {
    operation.mutable_uuid()->CopyFrom(operationUUID.get());
  } else {
    operation.mutable_uuid()->CopyFrom(protobuf::createUUID());
  }

  return operation;
}


UpdateOperationStatusMessage createUpdateOperationStatusMessage(
    const UUID& operationUUID,
    const OperationStatus& status,
    const Option<OperationStatus>& latestStatus,
    const Option<FrameworkID>& frameworkId,
    const Option<NodeID>& nodeId)
{
  UpdateOperationStatusMessage update;
  if (frameworkId.isSome()) {
    update.mutable_framework_id()->CopyFrom(frameworkId.get());
  }
  if (nodeId.isSome()) {
    update.mutable_node_id()->CopyFrom(nodeId.get());
  }
  update.mutable_status()->CopyFrom(status);
  if (latestStatus.isSome()) {
    update.mutable_latest_status()->CopyFrom(latestStatus.get());
  }
  update.mutable_operation_uuid()->CopyFrom(operationUUID);

  return update;
}


UUID createUUID(const Option<id::UUID>& uuid)
{
  UUID uuid_;

  if (uuid.isSome()) {
    uuid_.set_value(uuid->toBytes());
  } else {
    uuid_.set_value(id::UUID::random().toBytes());
  }

  return uuid_;
}


/**
 * Creates a ControllerInfo protobuf from the process's UPID.
 *
 * This is only used by the `StandaloneControllerDetector` (used in tests
 * and outside tests when ZK is not used).
 *
 * For example, when we start a node with
 * `--controller=controller@127.0.0.1:5050`, since the node (and consequently
 * its detector) doesn't have enough information about `ControllerInfo`, it
 * tries to construct it based on the only available information
 * (`UPID`).
 *
 * @param pid The process's assigned untyped PID.
 * @return A fully formed `ControllerInfo` with the IP/hostname information
 *    as derived from the `UPID`.
 */
ControllerInfo createControllerInfo(const UPID& pid)
{
  ControllerInfo info;
  info.set_id(stringify(pid) + "-" + id::UUID::random().toString());

  // NOTE: Currently, we store the ip in network order, which should
  // be fixed. See MESOS-1201 for more details.
  // TODO(marco): `ip` and `port` are deprecated in favor of `address`;
  //     remove them both after the deprecation cycle.
  info.set_ip(pid.address.ip.in()->s_addr);
  info.set_port(pid.address.port);

  info.mutable_address()->set_ip(stringify(pid.address.ip));
  info.mutable_address()->set_port(pid.address.port);

  info.set_pid(pid);

  Try<string> hostname = net::getHostname(pid.address.ip);
  if (hostname.isSome()) {
    // Hostname is deprecated; but we need to update it
    // to maintain backward compatibility.
    // TODO(marco): Remove once we deprecate it.
    info.set_hostname(hostname.get());
    info.mutable_address()->set_hostname(hostname.get());
  }

  foreach (const ControllerInfo::Capability& capability,
           medios::internal::controller::CONTROLLER_CAPABILITIES()) {
    info.add_capabilities()->CopyFrom(capability);
  }

  return info;
}


Label createLabel(const string& key, const Option<string>& value)
{
  Label label;
  label.set_key(key);
  if (value.isSome()) {
    label.set_value(value.get());
  }
  return label;
}


Labels convertStringMapToLabels(const Map<string, string>& map)
{
  Labels labels;

  foreach (const auto& entry, map) {
    Label* label = labels.mutable_labels()->Add();
    label->set_key(entry.first);
    label->set_value(entry.second);
  }

  return labels;
}


Try<Map<string, string>> convertLabelsToStringMap(const Labels& labels)
{
  Map<string, string> map;

  foreach (const Label& label, labels.labels()) {
    if (map.count(label.key())) {
      return Error("Repeated key '" + label.key() + "' in labels");
    }

    if (!label.has_value()) {
      return Error("Missing value for key '" + label.key() + "' in labels");
    }

    map[label.key()] = label.value();
  }

  return map;
}


void injectAllocationInfo(
    Offer::Operation* operation,
    const Resource::AllocationInfo& allocationInfo)
{
  struct Injector
  {
    void operator()(
        Resource& resource, const Resource::AllocationInfo& allocationInfo)
    {
      if (!resource.has_allocation_info()) {
        resource.mutable_allocation_info()->CopyFrom(allocationInfo);
      }
    }

    void operator()(
        RepeatedPtrField<Resource>* resources,
        const Resource::AllocationInfo& allocationInfo)
    {
      foreach (Resource& resource, *resources) {
        operator()(resource, allocationInfo);
      }
    }
  } inject;

  switch (operation->type()) {
    case Offer::Operation::LAUNCH: {
      Offer::Operation::Launch* launch = operation->mutable_launch();

      foreach (TaskInfo& task, *launch->mutable_task_infos()) {
        inject(task.mutable_resources(), allocationInfo);

        if (task.has_executor()) {
          inject(
              task.mutable_executor()->mutable_resources(),
              allocationInfo);
        }
      }
      break;
    }

    case Offer::Operation::LAUNCH_GROUP: {
      Offer::Operation::LaunchGroup* launchGroup =
        operation->mutable_launch_group();

      if (launchGroup->has_executor()) {
        inject(
            launchGroup->mutable_executor()->mutable_resources(),
            allocationInfo);
      }

      TaskGroupInfo* taskGroup = launchGroup->mutable_task_group();

      foreach (TaskInfo& task, *taskGroup->mutable_tasks()) {
        inject(task.mutable_resources(), allocationInfo);

        if (task.has_executor()) {
          inject(
              task.mutable_executor()->mutable_resources(),
              allocationInfo);
        }
      }
      break;
    }

    case Offer::Operation::RESERVE: {
      inject(
          operation->mutable_reserve()->mutable_resources(),
          allocationInfo);

      break;
    }

    case Offer::Operation::UNRESERVE: {
      inject(
          operation->mutable_unreserve()->mutable_resources(),
          allocationInfo);

      break;
    }

    case Offer::Operation::CREATE: {
      inject(
          operation->mutable_create()->mutable_volumes(),
          allocationInfo);

      break;
    }

    case Offer::Operation::DESTROY: {
      inject(
          operation->mutable_destroy()->mutable_volumes(),
          allocationInfo);

      break;
    }

    case Offer::Operation::GROW_VOLUME: {
      inject(
          *operation->mutable_grow_volume()->mutable_volume(),
          allocationInfo);

      inject(
          *operation->mutable_grow_volume()->mutable_addition(),
          allocationInfo);

      break;
    }

    case Offer::Operation::SHRINK_VOLUME: {
      inject(
          *operation->mutable_shrink_volume()->mutable_volume(),
          allocationInfo);

      break;
    }

    case Offer::Operation::CREATE_DISK: {
      inject(
          *operation->mutable_create_disk()->mutable_source(),
          allocationInfo);

      break;
    }

    case Offer::Operation::DESTROY_DISK: {
      inject(
          *operation->mutable_destroy_disk()->mutable_source(),
          allocationInfo);

      break;
    }

    case Offer::Operation::UNKNOWN:
      break; // No-op.
  }
}


void stripAllocationInfo(Offer::Operation* operation)
{
  struct Stripper
  {
    void operator()(Resource& resource)
    {
      if (resource.has_allocation_info()) {
        resource.clear_allocation_info();
      }
    }

    void operator()(RepeatedPtrField<Resource>* resources)
    {
      foreach (Resource& resource, *resources) {
        operator()(resource);
      }
    }
  } strip;

  switch (operation->type()) {
    case Offer::Operation::LAUNCH: {
      Offer::Operation::Launch* launch = operation->mutable_launch();

      foreach (TaskInfo& task, *launch->mutable_task_infos()) {
        strip(task.mutable_resources());

        if (task.has_executor()) {
          strip(task.mutable_executor()->mutable_resources());
        }
      }
      break;
    }

    case Offer::Operation::LAUNCH_GROUP: {
      Offer::Operation::LaunchGroup* launchGroup =
        operation->mutable_launch_group();

      if (launchGroup->has_executor()) {
        strip(launchGroup->mutable_executor()->mutable_resources());
      }

      TaskGroupInfo* taskGroup = launchGroup->mutable_task_group();

      foreach (TaskInfo& task, *taskGroup->mutable_tasks()) {
        strip(task.mutable_resources());

        if (task.has_executor()) {
          strip(task.mutable_executor()->mutable_resources());
        }
      }
      break;
    }

    case Offer::Operation::RESERVE: {
      strip(operation->mutable_reserve()->mutable_resources());

      break;
    }

    case Offer::Operation::UNRESERVE: {
      strip(operation->mutable_unreserve()->mutable_resources());

      break;
    }

    case Offer::Operation::CREATE: {
      strip(operation->mutable_create()->mutable_volumes());

      break;
    }

    case Offer::Operation::DESTROY: {
      strip(operation->mutable_destroy()->mutable_volumes());

      break;
    }

    case Offer::Operation::GROW_VOLUME: {
      strip(*operation->mutable_grow_volume()->mutable_volume());
      strip(*operation->mutable_grow_volume()->mutable_addition());

      break;
    }

    case Offer::Operation::SHRINK_VOLUME: {
      strip(*operation->mutable_shrink_volume()->mutable_volume());

      break;
    }

    case Offer::Operation::CREATE_DISK: {
      strip(*operation->mutable_create_disk()->mutable_source());

      break;
    }

    case Offer::Operation::DESTROY_DISK: {
      strip(*operation->mutable_destroy_disk()->mutable_source());

      break;
    }

    case Offer::Operation::UNKNOWN:
      break; // No-op.
  }
}


bool isSpeculativeOperation(const Offer::Operation& operation)
{
  switch (operation.type()) {
    case Offer::Operation::LAUNCH:
    case Offer::Operation::LAUNCH_GROUP:
    case Offer::Operation::CREATE_DISK:
    case Offer::Operation::DESTROY_DISK:
      return false;
    case Offer::Operation::RESERVE:
    case Offer::Operation::UNRESERVE:
    case Offer::Operation::CREATE:
    case Offer::Operation::DESTROY:
    // TODO(zhitao): Convert `GROW_VOLUME` and `SHRINK_VOLUME` to
    // non-speculative operations once we can support non-speculative operator
    // API.
    case Offer::Operation::GROW_VOLUME:
    case Offer::Operation::SHRINK_VOLUME:
      return true;
    case Offer::Operation::UNKNOWN:
      UNREACHABLE();
  }

  UNREACHABLE();
}


RepeatedPtrField<ResourceVersionUUID> createResourceVersions(
    const hashmap<Option<ResourceProviderID>, UUID>& resourceVersions)
{
  RepeatedPtrField<ResourceVersionUUID> result;

  foreachpair (
      const Option<ResourceProviderID>& resourceProviderId,
      const UUID& uuid,
      resourceVersions) {
    ResourceVersionUUID* entry = result.Add();

    if (resourceProviderId.isSome()) {
      entry->mutable_resource_provider_id()->CopyFrom(resourceProviderId.get());
    }
    entry->mutable_uuid()->CopyFrom(uuid);
  }

  return result;
}


hashmap<Option<ResourceProviderID>, UUID> parseResourceVersions(
    const RepeatedPtrField<ResourceVersionUUID>& resourceVersionUUIDs)
{
  hashmap<Option<ResourceProviderID>, UUID> result;

  foreach (
      const ResourceVersionUUID& resourceVersionUUID,
      resourceVersionUUIDs) {
    const Option<ResourceProviderID> resourceProviderId =
      resourceVersionUUID.has_resource_provider_id()
        ? resourceVersionUUID.resource_provider_id()
        : Option<ResourceProviderID>::none();

    CHECK(!result.contains(resourceProviderId));

    result.insert({std::move(resourceProviderId), resourceVersionUUID.uuid()});
  }

  return result;
}


TimeInfo getCurrentTime()
{
  TimeInfo timeInfo;
  timeInfo.set_nanoseconds(process::Clock::now().duration().ns());
  return timeInfo;
}


FileInfo createFileInfo(const string& path, const struct stat& s)
{
  FileInfo file;
  file.set_path(path);
  file.set_nlink(s.st_nlink);
  file.set_size(s.st_size);
  file.mutable_mtime()->set_nanoseconds(Seconds((s.st_mtime)).ns());
  file.set_mode(s.st_mode);

  passwd* p = getpwuid(s.st_uid);
  if (p != nullptr) {
    file.set_uid(p->pw_name);
  } else {
    file.set_uid(stringify(s.st_uid));
  }

  struct group* g = getgrgid(s.st_gid);
  if (g != nullptr) {
    file.set_gid(g->gr_name);
  } else {
    file.set_gid(stringify(s.st_gid));
  }

  return file;
}


ContainerID getRootContainerId(const ContainerID& containerId)
{
  ContainerID rootContainerId = containerId;
  while (rootContainerId.has_parent()) {
    // NOTE: Looks like protobuf does not handle copying well when
    // nesting message is involved, because the source and the target
    // point to the same object. Therefore, we create a temporary
    // variable and use an extra copy here.
    ContainerID id = rootContainerId.parent();
    rootContainerId = id;
  }

  return rootContainerId;
}


ContainerID parseContainerId(const string& value)
{
  vector<string> tokens = strings::split(value, ".");

  Option<ContainerID> result;
  foreach (const string& token, tokens) {
    ContainerID id;
    id.set_value(token);

    if (result.isSome()) {
      id.mutable_parent()->CopyFrom(result.get());
    }

    result = id;
  }

  CHECK_SOME(result);
  return result.get();
}


Try<Resources> getConsumedResources(const Offer::Operation& operation)
{
  switch (operation.type()) {
    case Offer::Operation::CREATE_DISK:
      return operation.create_disk().source();
    case Offer::Operation::DESTROY_DISK:
      return operation.destroy_disk().source();
    case Offer::Operation::RESERVE:
    case Offer::Operation::UNRESERVE:
    case Offer::Operation::CREATE:
    case Offer::Operation::DESTROY:
    case Offer::Operation::GROW_VOLUME:
    case Offer::Operation::SHRINK_VOLUME: {
      Try<vector<ResourceConversion>> conversions =
        getResourceConversions(operation);

      if (conversions.isError()) {
        return Error(conversions.error());
      }

      Resources consumed;
      foreach (const ResourceConversion& conversion, conversions.get()) {
        consumed += conversion.consumed;
      }

      return consumed;
    }
    case Offer::Operation::LAUNCH:
    case Offer::Operation::LAUNCH_GROUP:
      // TODO(bbannier): Consider adding support for 'LAUNCH' and
      // 'LAUNCH_GROUP' operations.
    case Offer::Operation::UNKNOWN:
      return Error("Unsupported operation");
  }

  UNREACHABLE();
}


namespace node {

bool operator==(const Capabilities& left, const Capabilities& right)
{
  // TODO(bmahler): Use reflection-based equality to avoid breaking
  // as new capabilities are added. Note that it needs to be set-based
  // equality.
  return left.multiRole == right.multiRole &&
         left.hierarchicalRole == right.hierarchicalRole &&
         left.reservationRefinement == right.reservationRefinement &&
         left.resourceProvider == right.resourceProvider &&
         left.resizeVolume == right.resizeVolume &&
         left.nodeOperationFeedback == right.nodeOperationFeedback &&
         left.nodeDraining == right.nodeDraining &&
         left.taskResourceLimits == right.taskResourceLimits;
}


bool operator!=(const Capabilities& left, const Capabilities& right)
{
  return !(left == right);
}


ostream& operator<<(ostream& stream, const Capabilities& c)
{
  set<string> names;

  foreach (const NodeInfo::Capability& capability, c.toRepeatedPtrField()) {
    names.insert(NodeInfo::Capability::Type_Name(capability.type()));
  }

  return stream << stringify(names);
}


ContainerLimitation createContainerLimitation(
    const Resources& resources,
    const string& message,
    const TaskStatus::Reason& reason)
{
  ContainerLimitation limitation;
  foreach (Resource resource, resources) {
    limitation.add_resources()->CopyFrom(resource);
  }
  limitation.set_message(message);
  limitation.set_reason(reason);
  return limitation;
}


ContainerState createContainerState(
    const Option<ExecutorInfo>& executorInfo,
    const Option<ContainerInfo>& containerInfo,
    const ContainerID& containerId,
    pid_t pid,
    const string& directory)
{
  ContainerState state;

  if (executorInfo.isSome()) {
    state.mutable_executor_info()->CopyFrom(executorInfo.get());
  }

  if (containerInfo.isSome()) {
    state.mutable_container_info()->CopyFrom(containerInfo.get());
  }

  state.mutable_container_id()->CopyFrom(containerId);
  state.set_pid(pid);
  state.set_directory(directory);

  return state;
}


ContainerMountInfo createContainerMount(
    const string& source,
    const string& target,
    unsigned long flags)
{
  ContainerMountInfo mnt;

  mnt.set_source(source);
  mnt.set_target(target);
  mnt.set_flags(flags);

  return mnt;
}


ContainerMountInfo createContainerMount(
    const string& source,
    const string& target,
    const string& type,
    unsigned long flags)
{
  ContainerMountInfo mnt;

  mnt.set_source(source);
  mnt.set_target(target);
  mnt.set_type(type);
  mnt.set_flags(flags);

  return mnt;
}


ContainerMountInfo createContainerMount(
    const string& source,
    const string& target,
    const string& type,
    const string& options,
    unsigned long flags)
{
  ContainerMountInfo mnt;

  mnt.set_source(source);
  mnt.set_target(target);
  mnt.set_type(type);
  mnt.set_options(options);
  mnt.set_flags(flags);

  return mnt;
}


medios::node::ContainerFileOperation containerSymlinkOperation(
    const std::string& source,
    const std::string& target)
{
  ContainerFileOperation op;

  op.set_operation(ContainerFileOperation::SYMLINK);
  op.mutable_symlink()->set_source(source);
  op.mutable_symlink()->set_target(target);

  return op;
}


medios::node::ContainerFileOperation containerRenameOperation(
    const std::string& source,
    const std::string& target)
{
  ContainerFileOperation op;

  op.set_operation(ContainerFileOperation::RENAME);
  op.mutable_rename()->set_source(source);
  op.mutable_rename()->set_target(target);

  return op;
}


medios::node::ContainerFileOperation containerMkdirOperation(
    const std::string& target,
    const bool recursive)
{
  ContainerFileOperation op;

  op.set_operation(ContainerFileOperation::MKDIR);
  op.mutable_mkdir()->set_target(target);
  op.mutable_mkdir()->set_recursive(recursive);

  return op;
}


medios::node::ContainerFileOperation containerMountOperation(
    const ContainerMountInfo& mnt)
{
  ContainerFileOperation op;

  op.set_operation(ContainerFileOperation::MOUNT);
  *op.mutable_mount() = mnt;

  return op;
}

} // namespace node {

namespace maintenance {

Unavailability createUnavailability(
    const process::Time& start,
    const Option<Duration>& duration)
{
  Unavailability unavailability;
  unavailability.mutable_start()->set_nanoseconds(start.duration().ns());

  if (duration.isSome()) {
    unavailability.mutable_duration()->set_nanoseconds(duration->ns());
  }

  return unavailability;
}


RepeatedPtrField<MachineID> createMachineList(
    std::initializer_list<MachineID> ids)
{
  RepeatedPtrField<MachineID> array;

  foreach (const MachineID& id, ids) {
    array.Add()->CopyFrom(id);
  }

  return array;
}


medios::maintenance::Window createWindow(
    std::initializer_list<MachineID> ids,
    const Unavailability& unavailability)
{
  medios::maintenance::Window window;
  window.mutable_unavailability()->CopyFrom(unavailability);

  foreach (const MachineID& id, ids) {
    window.add_machine_ids()->CopyFrom(id);
  }

  return window;
}


medios::maintenance::Schedule createSchedule(
    std::initializer_list<medios::maintenance::Window> windows)
{
  medios::maintenance::Schedule schedule;

  foreach (const medios::maintenance::Window& window, windows) {
    schedule.add_windows()->CopyFrom(window);
  }

  return schedule;
}

} // namespace maintenance {

namespace controller {

void addMinimumCapability(
    google::protobuf::RepeatedPtrField<Registry::MinimumCapability>*
      capabilities,
    const ControllerInfo::Capability::Type& capability)
{
  int capabilityIndex =
    std::find_if(
        capabilities->begin(),
        capabilities->end(),
        [&](const Registry::MinimumCapability& mc) {
          return mc.capability() == ControllerInfo_Capability_Type_Name(capability);
        }) -
    capabilities->begin();

  if (capabilityIndex == capabilities->size()) {
    capabilities->Add()->set_capability(
        ControllerInfo_Capability_Type_Name(capability));
  }
}


void removeMinimumCapability(
    google::protobuf::RepeatedPtrField<Registry::MinimumCapability>*
      capabilities,
    const ControllerInfo::Capability::Type& capability)
{
  int capabilityIndex =
    std::find_if(
        capabilities->begin(),
        capabilities->end(),
        [&](const Registry::MinimumCapability& mc) {
          return mc.capability() == ControllerInfo_Capability_Type_Name(capability);
        }) -
    capabilities->begin();

  if (capabilityIndex < capabilities->size()) {
    capabilities->DeleteSubrange(capabilityIndex, 1);
  }
}

namespace event {

medios::controller::Event createTaskUpdated(
    const Task& task,
    const TaskState& state,
    const TaskStatus& status)
{
  medios::controller::Event event;
  event.set_type(medios::controller::Event::TASK_UPDATED);

  medios::controller::Event::TaskUpdated* taskUpdated = event.mutable_task_updated();

  taskUpdated->mutable_framework_id()->CopyFrom(task.framework_id());
  taskUpdated->mutable_status()->CopyFrom(status);
  taskUpdated->set_state(state);

  return event;
}


medios::controller::Event createTaskAdded(const Task& task)
{
  medios::controller::Event event;
  event.set_type(medios::controller::Event::TASK_ADDED);

  event.mutable_task_added()->mutable_task()->CopyFrom(task);

  return event;
}


medios::controller::Event createFrameworkAdded(
    const medios::internal::controller::Framework& _framework)
{
  medios::controller::Event event;
  event.set_type(medios::controller::Event::FRAMEWORK_ADDED);

  medios::controller::Response::GetFrameworks::Framework* framework =
    event.mutable_framework_added()->mutable_framework();

  framework->mutable_framework_info()->CopyFrom(_framework.info);
  framework->set_active(_framework.active());
  framework->set_connected(_framework.connected());
  framework->set_recovered(_framework.recovered());

  framework->mutable_registered_time()->set_nanoseconds(
      _framework.registeredTime.duration().ns());

  framework->mutable_reregistered_time()->set_nanoseconds(
      _framework.reregisteredTime.duration().ns());

  framework->mutable_unregistered_time()->set_nanoseconds(
      _framework.unregisteredTime.duration().ns());

  return event;
}


medios::controller::Event createFrameworkUpdated(
    const medios::internal::controller::Framework& _framework)
{
  medios::controller::Event event;
  event.set_type(medios::controller::Event::FRAMEWORK_UPDATED);

  medios::controller::Response::GetFrameworks::Framework* framework =
    event.mutable_framework_updated()->mutable_framework();

  framework->mutable_framework_info()->CopyFrom(_framework.info);
  framework->set_active(_framework.active());
  framework->set_connected(_framework.connected());
  framework->set_recovered(_framework.recovered());

  framework->mutable_registered_time()->set_nanoseconds(
      _framework.registeredTime.duration().ns());

  framework->mutable_reregistered_time()->set_nanoseconds(
      _framework.reregisteredTime.duration().ns());

  framework->mutable_unregistered_time()->set_nanoseconds(
      _framework.unregisteredTime.duration().ns());

  return event;
}


medios::controller::Event createFrameworkRemoved(const FrameworkInfo& frameworkInfo)
{
  medios::controller::Event event;
  event.set_type(medios::controller::Event::FRAMEWORK_REMOVED);

  event.mutable_framework_removed()->mutable_framework_info()->CopyFrom(
      frameworkInfo);

  return event;
}


medios::controller::Response::GetNodes::Node createNodeResponse(
    const medios::internal::controller::Node& node,
    const Option<DrainInfo>& drainInfo,
    bool deactivated,
    const Option<Owned<ObjectApprovers>>& approvers)
{
  medios::controller::Response::GetNodes::Node result;

  result.mutable_node_info()->CopyFrom(node.info);

  result.set_pid(string(node.pid));
  result.set_active(node.active);
  result.set_deactivated(deactivated);
  result.set_version(node.version);

  result.mutable_registered_time()->set_nanoseconds(
      node.registeredTime.duration().ns());

  if (node.reregisteredTime.isSome()) {
    result.mutable_reregistered_time()->set_nanoseconds(
        node.reregisteredTime->duration().ns());
  }

  result.mutable_node_info()->clear_resources();
  foreach (const Resource& resource, node.info.resources()) {
    if (approvers.isNone() || approvers.get()->approved<VIEW_ROLE>(resource)) {
      result.mutable_node_info()->add_resources()->CopyFrom(resource);
    }
  }

  foreach (Resource resource, node.totalResources) {
    if (approvers.isNone() || approvers.get()->approved<VIEW_ROLE>(resource)) {
      convertResourceFormat(&resource, ENDPOINT);
      result.add_total_resources()->CopyFrom(resource);
    }
  }

  foreach (Resource resource, Resources::sum(node.usedResources)) {
    if (approvers.isNone() || approvers.get()->approved<VIEW_ROLE>(resource)) {
      convertResourceFormat(&resource, ENDPOINT);
      result.add_allocated_resources()->CopyFrom(resource);
    }
  }

  foreach (Resource resource, node.offeredResources) {
    if (approvers.isNone() || approvers.get()->approved<VIEW_ROLE>(resource)) {
      convertResourceFormat(&resource, ENDPOINT);
      result.add_offered_resources()->CopyFrom(resource);
    }
  }

  result.mutable_capabilities()->CopyFrom(
      node.capabilities.toRepeatedPtrField());

  foreachvalue (
      const medios::internal::controller::Node::ResourceProvider& resourceProvider,
      node.resourceProviders) {
    medios::controller::Response::GetNodes::Node::ResourceProvider* provider =
      result.add_resource_providers();

    provider->mutable_resource_provider_info()->CopyFrom(resourceProvider.info);
    provider->mutable_total_resources()->CopyFrom(
        resourceProvider.totalResources);
  }

  if (drainInfo.isSome()) {
    result.mutable_drain_info()->CopyFrom(drainInfo.get());

    if (node.estimatedDrainStartTime.isSome()) {
      result.mutable_estimated_drain_start_time()->set_nanoseconds(
          Seconds(node.estimatedDrainStartTime->secs()).ns());
    }
  }

  return result;
}


medios::controller::Event createNodeAdded(
    const medios::internal::controller::Node& node,
    const Option<DrainInfo>& drainInfo,
    bool deactivated)
{
  medios::controller::Event event;
  event.set_type(medios::controller::Event::NODE_ADDED);

  event.mutable_node_added()->mutable_node()->CopyFrom(
      createNodeResponse(
          node,
          drainInfo,
          deactivated));

  return event;
}


medios::controller::Event createNodeRemoved(const NodeID& nodeId)
{
  medios::controller::Event event;
  event.set_type(medios::controller::Event::NODE_REMOVED);

  event.mutable_node_removed()->mutable_node_id()->CopyFrom(
      nodeId);

  return event;
}

} // namespace event {
} // namespace controller {

namespace framework {

set<string> getRoles(const FrameworkInfo& frameworkInfo)
{
  if (protobuf::frameworkHasCapability(
          frameworkInfo,
          FrameworkInfo::Capability::MULTI_ROLE)) {
    return set<string>(
        frameworkInfo.roles().begin(),
        frameworkInfo.roles().end());
  } else {
    return {frameworkInfo.role()};
  }
}

} // namespace framework {

} // namespace protobuf {
} // namespace internal {
} // namespace medios {
