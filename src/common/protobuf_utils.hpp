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

#ifndef __PROTOBUF_UTILS_HPP__
#define __PROTOBUF_UTILS_HPP__

#include <initializer_list>
#include <ostream>
#include <set>
#include <string>

#include <sys/stat.h>

#include <google/protobuf/map.h>

#include <medios/medios.hpp>

#include <medios/maintenance/maintenance.hpp>

#include <medios/controller/controller.hpp>

#include <medios/node/isolator.hpp>

#include <process/time.hpp>

#include <stout/duration.hpp>
#include <stout/ip.hpp>
#include <stout/none.hpp>
#include <stout/option.hpp>
#include <stout/try.hpp>
#include <stout/uuid.hpp>

#include "controller/registry.hpp"

#include "messages/messages.hpp"

// Forward declaration (in lieu of an include).
namespace process {
struct UPID;
}

namespace medios {

class ObjectApprovers;

namespace internal {

namespace controller {
// Forward declaration (in lieu of an include).
struct Framework;
struct Node;
} // namespace controller {

namespace protobuf {

// Modeled after WireFormatLite from protobuf, but to provide
// missing helpers.
class WireFormatLite2
{
public:
  // This is a wrapper to compute cached sizes before calling into
  // `WireFormatLite::WriteMessage`, which assumes that sizes are
  // already cached.
  static void WriteMessageWithoutCachedSizes(
      int field_number,
      const google::protobuf::MessageLite& value,
      google::protobuf::io::CodedOutputStream* output)
  {
    // Cache the sizes first.
    value.ByteSizeLong();

    google::protobuf::internal::WireFormatLite::WriteMessage(
        field_number, value, output);
  }
};


// Internal helper class for protobuf union validation.
class UnionValidator
{
public:
  UnionValidator(const google::protobuf::Descriptor*);
  Option<Error> validate(
      const int messageTypeNumber, const google::protobuf::Message&) const;

private:
  std::vector<std::pair<int, const google::protobuf::FieldDescriptor*>>
    unionFieldDescriptors_;
  const google::protobuf::EnumDescriptor* typeDescriptor_;
};

//
// A message is a "protobuf union" if, and only if,
// the following requirements are satisfied:
// 1. It has a required field named `type` of an enum type.
// 2. A member of this enum with a number (not index!) of 0
//    either is named "UNKNOWN" or does not exist.
// 3. For each other member of this enum there is an optional field
//    in the message with an exactly matching name in lowercase.
// (Being or not being a protobuf uinion depends on a message declaration only.)
//
// A "protobuf union" is valid if, and only if, all the message fields
// which correspond to members of this enum that do not matching the value
// of the `type` field, are not set.
// (Validity of the protobuf union depends on the message contents.
// Note that it does not depend on whether the matching field is set or not.)
//
// NOTE: If possible, oneof should be used in the new messages instead
// of the "protobuf union".
//
// This function returns None if the protobuf union is valid
// and Error otherwise.
// In case the ProtobufUnion is not a protobuf union,
// this function will abort the process on the first use.
template <class ProtobufUnion>
Option<Error> validateProtobufUnion(const ProtobufUnion& message)
{
  static const UnionValidator validator(ProtobufUnion::descriptor());
  return validator.validate(message.type(), message);
}


bool frameworkHasCapability(
    const FrameworkInfo& framework,
    FrameworkInfo::Capability::Type capability);


// Returns whether the task state is terminal. Terminal states
// mean that the resources are released and the task cannot
// transition back to a non-terminal state. Note that
// `TASK_UNREACHABLE` is not a terminal state, but still
// releases the resources.
bool isTerminalState(const TaskState& state);


// See TaskStatus for more information about these fields. Note
// that the 'uuid' must be provided for updates that need
// acknowledgement. Currently, all node and executor generated
// updates require acknowledgement, whereas controller generated
// and scheduler driver generated updates do not.
StatusUpdate createStatusUpdate(
    const FrameworkID& frameworkId,
    const Option<NodeID>& nodeId,
    const TaskID& taskId,
    const TaskState& state,
    const TaskStatus::Source& source,
    const Option<id::UUID>& uuid,
    const std::string& message = "",
    const Option<TaskStatus::Reason>& reason = None(),
    const Option<ExecutorID>& executorId = None(),
    const Option<bool>& healthy = None(),
    const Option<CheckStatusInfo>& checkStatus = None(),
    const Option<Labels>& labels = None(),
    const Option<ContainerStatus>& containerStatus = None(),
    const Option<TimeInfo>& unreachableTime = None(),
    const Option<Resources>& limitedResources = None(),
    const Option<TaskStatus::Progress>& progress = None());


StatusUpdate createStatusUpdate(
    const FrameworkID& frameworkId,
    const TaskStatus& status,
    const Option<NodeID>& nodeId);


// Helper function that creates a new task status from scratch with
// obligatory fields set.
TaskStatus createTaskStatus(
    const TaskID& taskId,
    const TaskState& state,
    const id::UUID& uuid,
    double timestamp);


// Helper function that creates a new task status from the given task
// status. Specific fields in `status` can be overridden in the new
// status by specifying the appropriate argument. Fields `task_id`,
// `node_id`, `executor_id`, cannot be changed; while `timestamp`
// and `uuid` cannot be preserved.
//
// NOTE: A task status update may be used for guaranteed delivery of
// some task-related information, e.g., task's health update. In this
// case, it is often desirable to preserve specific fields from the
// previous status update to avoid shadowing information that was
// delivered previously.
TaskStatus createTaskStatus(
    TaskStatus status,
    const id::UUID& uuid,
    double timestamp,
    const Option<TaskState>& state = None(),
    const Option<std::string>& message = None(),
    const Option<TaskStatus::Source>& source = None(),
    const Option<TaskStatus::Reason>& reason = None(),
    const Option<std::string>& data = None(),
    const Option<bool>& healthy = None(),
    const Option<CheckStatusInfo>& checkStatus = None(),
    const Option<Labels>& labels = None(),
    const Option<ContainerStatus>& containerStatus = None(),
    const Option<TimeInfo>& unreachableTime = None());


// `frameworkPrincipal` is the principal bound to the launching framework at
// SUBSCRIBE, stamped onto `Task.framework_principal`; None when the framework
// registered without one (DR-037).
Task createTask(
    const TaskInfo& task,
    const TaskState& state,
    const FrameworkID& frameworkId,
    const Option<std::string>& frameworkPrincipal);


Option<bool> getTaskHealth(const Task& task);


// The task's last reported progress, read from the same place health is
// read from: the most recent status the controller kept. None
// when the task has never reported progress.
Option<TaskStatus::Progress> getTaskProgress(const Task& task);


// The stores a task was given, from its stored container and node id
// (DR-036): one `TaskData` per STORE volume, what `/state` and every
// status update the controller forwards carry.
std::vector<TaskData> storesOf(const Task& task);


Option<CheckStatusInfo> getTaskCheckStatus(const Task& task);


Option<ContainerStatus> getTaskContainerStatus(const Task& task);


bool isTerminalState(const OperationState& state);


OperationStatus createOperationStatus(
    const OperationState& state,
    const Option<OperationID>& operationId = None(),
    const Option<std::string>& message = None(),
    const Option<Resources>& convertedResources = None(),
    const Option<id::UUID>& statusUUID = None(),
    const Option<NodeID>& nodeId = None(),
    const Option<ResourceProviderID>& resourceProviderId = None());


Operation createOperation(
    const Offer::Operation& info,
    const OperationStatus& latestStatus,
    const Option<FrameworkID>& frameworkId,
    const Option<NodeID>& nodeId,
    const Option<UUID>& operationUUID = None());


UpdateOperationStatusMessage createUpdateOperationStatusMessage(
    const UUID& operationUUID,
    const OperationStatus& status,
    const Option<OperationStatus>& latestStatus = None(),
    const Option<FrameworkID>& frameworkId = None(),
    const Option<NodeID>& nodeId = None());


// Create a `UUID`. If `uuid` is given it is used to initialize
// the created `UUID`; otherwise a random `UUID` is returned.
UUID createUUID(const Option<id::UUID>& uuid = None());


// Helper function that creates a ControllerInfo from UPID.
ControllerInfo createControllerInfo(const process::UPID& pid);


Label createLabel(
    const std::string& key,
    const Option<std::string>& value = None());


// Helper function to convert a protobuf string map to `Labels`.
Labels convertStringMapToLabels(
    const google::protobuf::Map<std::string, std::string>& map);


// Helper function to convert a `Labels` to a protobuf string map.
Try<google::protobuf::Map<std::string, std::string>> convertLabelsToStringMap(
    const Labels& labels);


// Previously, `Resource` did not contain `AllocationInfo`.
// So for backwards compatibility with old schedulers and
// tooling, we must allow operations to contain `Resource`s
// without an `AllocationInfo`. This allows the controller to
// inject the offer's `AllocationInfo` into the operation's
// resources.
void injectAllocationInfo(
    Offer::Operation* operation,
    const Resource::AllocationInfo& allocationInfo);


// This strips the Resource::AllocationInfo from all
// Resource objects contained within the operation.
void stripAllocationInfo(Offer::Operation* operation);


bool isSpeculativeOperation(const Offer::Operation& operation);


// Helper function to pack a protobuf list of resource versions.
google::protobuf::RepeatedPtrField<ResourceVersionUUID> createResourceVersions(
    const hashmap<Option<ResourceProviderID>, UUID>& resourceVersions);


// Helper function to unpack a protobuf list of resource versions.
hashmap<Option<ResourceProviderID>, UUID> parseResourceVersions(
    const google::protobuf::RepeatedPtrField<ResourceVersionUUID>&
      resourceVersionUUIDs);


// Helper function that fills in a TimeInfo from the current time.
TimeInfo getCurrentTime();


// Helper function that creates a `FileInfo` from data returned by `stat()`.
FileInfo createFileInfo(const std::string& path, const struct stat& s);


ContainerID getRootContainerId(const ContainerID& containerId);


ContainerID parseContainerId(const std::string& value);


Try<Resources> getConsumedResources(const Offer::Operation& operation);

namespace node {

// TODO(bmahler): Store the repeated field within this so that we
// don't drop unknown capabilities.
struct Capabilities
{
  Capabilities() = default;

  template <typename Iterable>
  explicit Capabilities(const Iterable& capabilities)
  {
    foreach (const NodeInfo::Capability& capability, capabilities) {
      switch (capability.type()) {
        case NodeInfo::Capability::UNKNOWN:
          break;
        case NodeInfo::Capability::MULTI_ROLE:
          multiRole = true;
          break;
        case NodeInfo::Capability::HIERARCHICAL_ROLE:
          hierarchicalRole = true;
          break;
        case NodeInfo::Capability::RESERVATION_REFINEMENT:
          reservationRefinement = true;
          break;
        case NodeInfo::Capability::RESOURCE_PROVIDER:
          resourceProvider = true;
          break;
        case NodeInfo::Capability::RESIZE_VOLUME:
          resizeVolume = true;
          break;
        case NodeInfo::Capability::NODE_OPERATION_FEEDBACK:
          nodeOperationFeedback = true;
          break;
        case NodeInfo::Capability::NODE_DRAINING:
          nodeDraining = true;
          break;
        case NodeInfo::Capability::TASK_RESOURCE_LIMITS:
          taskResourceLimits = true;
          break;
        // If adding another case here be sure to update the
        // equality operator.
      }
    }
  }

  // See medios.proto for the meaning of node capabilities.
  bool multiRole = false;
  bool hierarchicalRole = false;
  bool reservationRefinement = false;
  bool resourceProvider = false;
  bool resizeVolume = false;
  bool nodeOperationFeedback = false;
  bool nodeDraining = false;
  bool taskResourceLimits = false;

  google::protobuf::RepeatedPtrField<NodeInfo::Capability>
  toRepeatedPtrField() const
  {
    google::protobuf::RepeatedPtrField<NodeInfo::Capability> result;
    if (multiRole) {
      result.Add()->set_type(NodeInfo::Capability::MULTI_ROLE);
    }
    if (hierarchicalRole) {
      result.Add()->set_type(NodeInfo::Capability::HIERARCHICAL_ROLE);
    }
    if (reservationRefinement) {
      result.Add()->set_type(NodeInfo::Capability::RESERVATION_REFINEMENT);
    }
    if (resourceProvider) {
      result.Add()->set_type(NodeInfo::Capability::RESOURCE_PROVIDER);
    }
    if (resizeVolume) {
      result.Add()->set_type(NodeInfo::Capability::RESIZE_VOLUME);
    }
    if (nodeOperationFeedback) {
      result.Add()->set_type(NodeInfo::Capability::NODE_OPERATION_FEEDBACK);
    }
    if (nodeDraining) {
      result.Add()->set_type(NodeInfo::Capability::NODE_DRAINING);
    }
    if (taskResourceLimits) {
      result.Add()->set_type(NodeInfo::Capability::TASK_RESOURCE_LIMITS);
    }

    return result;
  }
};


bool operator==(const Capabilities& left, const Capabilities& right);
bool operator!=(const Capabilities& left, const Capabilities& right);
std::ostream& operator<<(std::ostream& stream, const Capabilities& c);


medios::node::ContainerLimitation createContainerLimitation(
    const Resources& resources,
    const std::string& message,
    const TaskStatus::Reason& reason);


medios::node::ContainerState createContainerState(
    const Option<ExecutorInfo>& executorInfo,
    const Option<ContainerInfo>& containerInfo,
    const ContainerID& id,
    pid_t pid,
    const std::string& directory);


medios::node::ContainerMountInfo createContainerMount(
    const std::string& source,
    const std::string& target,
    unsigned long flags);


medios::node::ContainerMountInfo createContainerMount(
    const std::string& source,
    const std::string& target,
    const std::string& type,
    unsigned long flags);


medios::node::ContainerMountInfo createContainerMount(
    const std::string& source,
    const std::string& target,
    const std::string& type,
    const std::string& options,
    unsigned long flags);


medios::node::ContainerFileOperation containerSymlinkOperation(
    const std::string& source,
    const std::string& target);


medios::node::ContainerFileOperation containerRenameOperation(
    const std::string& source,
    const std::string& target);


medios::node::ContainerFileOperation containerMkdirOperation(
    const std::string& target,
    const bool recursive);


medios::node::ContainerFileOperation containerMountOperation(
    const medios::node::ContainerMountInfo& mnt);

} // namespace node {

namespace maintenance {

/**
 * Helper for constructing an unavailability from a `Time` and `Duration`.
 */
Unavailability createUnavailability(
    const process::Time& start,
    const Option<Duration>& duration = None());


/**
 * Helper for constructing a list of `MachineID`.
 */
google::protobuf::RepeatedPtrField<MachineID> createMachineList(
    std::initializer_list<MachineID> ids);


/**
 * Helper for constructing a maintenance `Window`.
 * See `createUnavailability` above.
 */
medios::maintenance::Window createWindow(
    std::initializer_list<MachineID> ids,
    const Unavailability& unavailability);


/**
 * Helper for constructing a maintenance `Schedule`.
 * See `createWindow` above.
 */
medios::maintenance::Schedule createSchedule(
    std::initializer_list<medios::maintenance::Window> windows);

} // namespace maintenance {


namespace controller {

// TODO(mzhu): Consolidate these helpers into `struct Capabilities`.
// For example, to add a minimum capability for `QUOTA_V2`, we could do the
// following in the call site:
//
//  Capabilities capabilities = registry->minimum_capabilities();
//  capabilities.quotaV2 = needsV2;
//  *registry->mutable_minimum_capabilities() = capabilities.toStrings();
//
// For this to work, we need to:
//  - Add a constructor from repeated `MinimumCapability`
//  - Add a toStrings() that goes back to repeated string
//  - Note, unknown capabilities need to be carried in the struct.
//
// In addition, we should consolidate the helper
// `Controller::misingMinimumCapabilities` into the struct as well.

// Helper to add a minimum capability, it is a noop if already set.
void addMinimumCapability(
    google::protobuf::RepeatedPtrField<Registry::MinimumCapability>*
      capabilities,
    const ControllerInfo::Capability::Type& capability);


// Helper to remove a minimum capability,
// it is a noop if already absent.
void removeMinimumCapability(
    google::protobuf::RepeatedPtrField<Registry::MinimumCapability>*
      capabilities,
    const ControllerInfo::Capability::Type& capability);


// TODO(bmahler): Store the repeated field within this so that we
// don't drop unknown capabilities.
struct Capabilities
{
  Capabilities() = default;

  template <typename Iterable>
  explicit Capabilities(const Iterable& capabilities)
  {
    foreach (const ControllerInfo::Capability& capability, capabilities) {
      switch (capability.type()) {
        case ControllerInfo::Capability::UNKNOWN:
          break;
        case ControllerInfo::Capability::NODE_UPDATE:
          nodeUpdate = true;
          break;
        case ControllerInfo::Capability::NODE_DRAINING:
          nodeDraining = true;
          break;
        case ControllerInfo::Capability::QUOTA_V2:
          quotaV2 = true;
          break;
      }
    }
  }

  bool nodeUpdate = false;
  bool nodeDraining = false;
  bool quotaV2 = false;
};

namespace event {

// Helper for creating a `TASK_UPDATED` event from a `Task`, its
// latest state according to the node, and its status corresponding
// to the last status update acknowledged from the scheduler.
medios::controller::Event createTaskUpdated(
    const Task& task,
    const TaskState& state,
    const TaskStatus& status);


// Helper for creating a `TASK_ADDED` event from a `Task`.
medios::controller::Event createTaskAdded(const Task& task);


// Helper for creating a 'FRAMEWORK_ADDED' event from a `Framework`.
medios::controller::Event createFrameworkAdded(
    const medios::internal::controller::Framework& framework);


// Helper for creating a 'FRAMEWORK_UPDATED' event from a `Framework`.
medios::controller::Event createFrameworkUpdated(
    const medios::internal::controller::Framework& framework);


// Helper for creating a 'FRAMEWORK_REMOVED' event from a `FrameworkInfo`.
medios::controller::Event createFrameworkRemoved(const FrameworkInfo& frameworkInfo);


// Helper for creating an `Node` response.
medios::controller::Response::GetNodes::Node createNodeResponse(
    const medios::internal::controller::Node& node,
    const Option<DrainInfo>& drainInfo,
    bool deactivated,
    const Option<process::Owned<ObjectApprovers>>& approvers = None());


// Helper for creating an `NODE_ADDED` event from a `Node`.
medios::controller::Event createNodeAdded(
    const medios::internal::controller::Node& node,
    const Option<DrainInfo>& drainInfo,
    bool deactivated);


// Helper for creating an `NODE_REMOVED` event from a `NodeID`.
medios::controller::Event createNodeRemoved(const NodeID& nodeId);

} // namespace event {
} // namespace controller {

namespace framework {

// TODO(bmahler): Store the repeated field within this so that we
// don't drop unknown capabilities.
struct Capabilities
{
  Capabilities() = default;

  template <typename Iterable>
  explicit Capabilities(const Iterable& capabilities)
  {
    foreach (const FrameworkInfo::Capability& capability, capabilities) {
      switch (capability.type()) {
        case FrameworkInfo::Capability::UNKNOWN:
          break;
        case FrameworkInfo::Capability::REVOCABLE_RESOURCES:
          revocableResources = true;
          break;
        case FrameworkInfo::Capability::TASK_KILLING_STATE:
          taskKillingState = true;
          break;
        case FrameworkInfo::Capability::GPU_RESOURCES:
          gpuResources = true;
          break;
        case FrameworkInfo::Capability::SHARED_RESOURCES:
          sharedResources = true;
          break;
        case FrameworkInfo::Capability::PARTITION_AWARE:
          partitionAware = true;
          break;
        case FrameworkInfo::Capability::MULTI_ROLE:
          multiRole = true;
          break;
        case FrameworkInfo::Capability::RESERVATION_REFINEMENT:
          reservationRefinement = true;
          break;
        case FrameworkInfo::Capability::REGION_AWARE:
          regionAware = true;
          break;
      }
    }
  }

  // See medios.proto for the meaning of these capabilities.
  bool revocableResources = false;
  bool taskKillingState = false;
  bool gpuResources = false;
  bool sharedResources = false;
  bool partitionAware = false;
  bool multiRole = false;
  bool reservationRefinement = false;
  bool regionAware = false;
};


// Helper to get roles from FrameworkInfo based on the
// presence of the MULTI_ROLE capability.
std::set<std::string> getRoles(const FrameworkInfo& frameworkInfo);

} // namespace framework {

} // namespace protobuf {
} // namespace internal {
} // namespace medios {

#endif // __PROTOBUF_UTILS_HPP__
