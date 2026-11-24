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

#ifndef __CONTROLLER_VALIDATION_HPP__
#define __CONTROLLER_VALIDATION_HPP__

#include <cstddef>
#include <vector>

#include <google/protobuf/repeated_field.h>

#include <medios/medios.hpp>
#include <medios/resources.hpp>

#include <medios/scheduler/scheduler.hpp>

#include <medios/controller/controller.hpp>

#include <process/authenticator.hpp>

#include <stout/error.hpp>
#include <stout/option.hpp>

#include "common/protobuf_utils.hpp"

namespace medios {
namespace internal {
namespace controller {

class Controller;

struct Framework;
struct Node;

namespace validation {

namespace controller {
namespace call {

// Validates that a controller:Call is well-formed.
//
// TODO(bmahler): Note that this does not validate the fields within
// the nested messages (e.g. `ReserveResources`) which is unintuitive.
// Consider moving all `controller::Call` validation that does not require
// controller state into this function.
//
// TODO(bmahler): Add unit tests.
Option<Error> validate(const medios::controller::Call& call);

} // namespace call {

namespace message {

// Validation helpers for internal Medios protocol messages. This is a
// best-effort validation, intended to prevent trivial attacks on the
// protocol in deployments where the network between controller and nodes
// is not secured. The longer term remedy for this is to make security
// guarantees at the libprocess level that would prevent arbitrary UPID
// impersonation (MESOS-7424).

Option<Error> registerNode(const RegisterNodeMessage& message);
Option<Error> reregisterNode(const ReregisterNodeMessage& message);

} // namespace message {
} // namespace controller {


namespace framework {
namespace internal {

// Validates the roles in given FrameworkInfo. Role, roles and
// MULTI_ROLE should be set according to following matrix. Also,
// roles should not contain duplicate entries.
//
// -- MULTI_ROLE is NOT set --
// +-------+-------+---------+
// |       |Roles  |No Roles |
// +-------+-------+---------+
// |Role   | Error |  None   |
// +-------+-------+---------+
// |No Role| Error |  None   |
// +-------+-------+---------+
//
// ---- MULTI_ROLE is set ----
// +-------+-------+---------+
// |       |Roles  |No Roles |
// +-------+-------+---------+
// |Role   | Error |  Error  |
// +-------+-------+---------+
// |No Role| None  |  None   |
// +-------+-------+---------+
Option<Error> validateRoles(const medios::FrameworkInfo& frameworkInfo);

Option<Error> validateFrameworkId(const medios::FrameworkInfo& frameworkInfo);

Option<Error> validateOfferFilters(const FrameworkInfo& frameworkInfo);

Option<Error> validatePrincipal(const FrameworkInfo& frameworkInfo);

} // namespace internal {

// Validate a FrameworkInfo.
Option<Error> validate(
    const medios::FrameworkInfo& frameworkInfo,
    bool validatePrincipal = true);

// Validate that the immutable fields of two FrameworkInfos are identical.
// Currently these fields are 'principal', 'user' and 'checkpoint'.
Option<Error> validateUpdate(
    const FrameworkInfo& oldInfo,
    const FrameworkInfo& newInfo);

// Adjusts `newInfo` to ensure that the `user` and `checkpoint` fields
// are not modified and logs a warning if they were modified.
//
// NOTE: This is a legacy function used to preserve the behavior of
// re-subscription silently ignoring these fields. It should not be
// used in new code.
//
// TODO(asekretenko): Remove this function (see MESOS-9747).
void preserveImmutableFields(
    const FrameworkInfo& oldInfo,
    FrameworkInfo* newInfo);

Option<Error> validateSuppressedRoles(
    const std::set<std::string>& validFrameworkRoles,
    const std::set<std::string>& suppressedRoles);

Option<Error> validateOfferConstraintsRoles(
    const std::set<std::string>& validFrameworkRoles,
    const scheduler::OfferConstraints& offerConstraints);

} // namespace framework {


namespace scheduler {
namespace call {

// Validates that a scheduler::Call is well-formed.
// TODO(bmahler): Add unit tests.
Option<Error> validate(
    const medios::scheduler::Call& call,
    const Option<process::http::authentication::Principal>& principal = None());

} // namespace call {
} // namespace scheduler {


namespace resource {

// Functions in this namespace are only exposed for testing.
namespace internal {

Option<Error> validateSingleResourceProvider(
    const google::protobuf::RepeatedPtrField<Resource>& resources);

} // namespace internal {

// Returns `true` if there is any overlap between set- or range-valued
// resources in the provided `Resources` objects.
bool detectOverlappingSetAndRangeResources(
    const std::vector<Resources>& resources);

// Validates resources specified by frameworks.
// NOTE: We cannot take 'Resources' here because invalid resources are
// silently ignored within its constructor.
Option<Error> validate(
    const google::protobuf::RepeatedPtrField<Resource>& resources);

} // namespace resource {


namespace executor {

// Functions in this namespace are only exposed for testing.
namespace internal {

Option<Error> validateExecutorID(const ExecutorInfo& executor);

// Validates that fields are properly set depending on the type of the executor.
Option<Error> validateType(const ExecutorInfo& executor);

// Validates resources of the executor.
Option<Error> validateResources(const ExecutorInfo& executor);

} // namespace internal {

Option<Error> validate(const ExecutorInfo& executor);

} // namespace executor {


namespace task {

// Validates a task that a framework attempts to launch within the
// offered resources. Returns an optional error which will cause the
// controller to send a `TASK_ERROR` status update back to the framework.
//
// NOTE: This function must be called sequentially for each task, and
// each task needs to be launched before the next can be validated.
Option<Error> validate(
    const TaskInfo& task,
    Framework* framework,
    Node* node,
    const Resources& offered);


// The longest name the controller accepts after the kind in
// `TaskInfo.principal`, in characters. Parameter: a principal is a label for
// a human or an agent run id, and 64 fits `run:research-164-s5-r3`-shaped ids
// with room while bounding the string that lands in every /state render, log
// line and status update.
constexpr size_t PRINCIPAL_NAME_MAX_LENGTH = 64;


// The most observers the controller accepts on one task, in observers.
//
// Parameter, default 8, provisional. Each observer is a process beside the
// task drawing on the task's own CPU and memory allocation, and only the
// newest line of all of them reaches the record, so a task past a handful
// is paying for observations the framework never sees. No measurement of
// observer cost per task-hour exists yet; raising it is research question 4
// of DR-041.
constexpr size_t MAX_OBSERVERS_PER_TASK = 8;


// The longest `TaskObserver.name` the controller accepts, in characters.
//
// Invariant of the name's job: it prefixes every line the observer
// contributes to a 4096-byte progress message, so a long name would spend
// the message on itself. The check is on bytes, which a name the alphabet
// check accepts has one of per character.
constexpr size_t OBSERVER_NAME_MAX_LENGTH = 32;


// Functions in this namespace are only exposed for testing.
namespace internal {

// Validates `TaskInfo.principal`: present, of the form `kind:name`, name at
// most `PRINCIPAL_NAME_MAX_LENGTH` characters.
Option<Error> validatePrincipal(const TaskInfo& task);

// Validates resources of the task.
Option<Error> validateResources(const TaskInfo& task);

// Validates resources of the task and its executor.
Option<Error> validateTaskAndExecutorResources(const TaskInfo& task);

// Validates the kill policy of the task.
Option<Error> validateKillPolicy(const TaskInfo& task);

// Validates the progress policy of the task: the source it names, the
// durations, and that a stall window and an action come together (DR-040).
Option<Error> validateProgressPolicy(const TaskInfo& task);

// Validates the observers of the task: each carries a name and a command,
// each name is `[a-z0-9-]` within `OBSERVER_NAME_MAX_LENGTH` and unique in
// the task, and there are at most `MAX_OBSERVERS_PER_TASK` of them
// (DR-041).
Option<Error> validateObservers(const TaskInfo& task);

// Validates `TaskInfo.finish`: run by the built-in command executor only,
// a command that names something to run, a positive cap no larger than
// `max_completion_time` when the task has one (DR-042).
Option<Error> validateFinish(const TaskInfo& task);

// Validates `max_completion_time` of the task.
Option<Error> validateMaxCompletionTime(const TaskInfo& task);

// Validates the check of the task.
Option<Error> validateCheck(const TaskInfo& task);

// Validates the health check of the task.
Option<Error> validateHealthCheck(const TaskInfo& task);

// Validates the form of the task's volume container paths: absolute, or
// one relative component inside the sandbox, each path once (DR-036).
Option<Error> validateStorePaths(const TaskInfo& task);

// Validates the stores the task asks for against the node's declarations
// and its current holders (DR-036).
Option<Error> validateStores(const TaskInfo& task, Node* node);

} // namespace internal {

namespace group {

// Validates a task group that a framework attempts to launch within the
// offered resources. Returns an optional error which will cause the
// controller to send a `TASK_ERROR` status updates for *all* the tasks in
// the task group back to the framework.
//
// NOTE: Validation error of *any* task will cause all the tasks in the task
// group to be rejected by the controller.
Option<Error> validate(
    const TaskGroupInfo& taskGroup,
    const ExecutorInfo& executor,
    Framework* framework,
    Node* node,
    const Resources& offered);


// Functions in this namespace are only exposed for testing.
namespace internal {

// Validates that the resources specified by
// the task group and its executor are valid.
//
// TODO(vinod): Consolidate this with `validateTaskAndExecutorResources()`.
Option<Error> validateTaskGroupAndExecutorResources(
    const TaskGroupInfo& taskGroup,
    const ExecutorInfo& executor);

} // namespace internal {

} // namespace group {

} // namespace task {


namespace offer {

// NOTE: These two functions are placed in the header file because we
// need to declare them as friends of Controller.
Offer* getOffer(Controller* controller, const OfferID& offerId);
InverseOffer* getInverseOffer(Controller* controller, const OfferID& offerId);
Node* getNode(Controller* controller, const NodeID& nodeId);


// Validates the given offers.
Option<Error> validate(
    const google::protobuf::RepeatedPtrField<OfferID>& offerIds,
    Controller* controller,
    Framework* framework);


// Validates the given inverse offers.
Option<Error> validateInverseOffers(
    const google::protobuf::RepeatedPtrField<OfferID>& offerIds,
    Controller* controller,
    Framework* framework);

} // namespace offer {


namespace operation {

// Validates the RESERVE operation.
Option<Error> validate(
    const Offer::Operation::Reserve& reserve,
    const Option<process::http::authentication::Principal>& principal,
    const protobuf::node::Capabilities& nodeCapabilities,
    const Option<FrameworkInfo>& frameworkInfo = None());


// Validates the UNRESERVE operation.
Option<Error> validate(
    const Offer::Operation::Unreserve& unreserve,
    const Option<FrameworkInfo>& frameworkInfo = None());


// Validates the CREATE operation. We need node's checkpointed resources so
// that we can validate persistence ID uniqueness, and we need the principal to
// verify that it's equal to the one in `DiskInfo.Persistence.principal`.
// We need the FrameworkInfo (unless the operation is requested by the
// operator) to ensure shared volumes are created by frameworks with the
// appropriate capability.
Option<Error> validate(
    const Offer::Operation::Create& create,
    const Resources& checkpointedResources,
    const Option<process::http::authentication::Principal>& principal,
    const protobuf::node::Capabilities& nodeCapabilities,
    const Option<FrameworkInfo>& frameworkInfo = None());


// Validates the DESTROY operation. We need node's checkpointed
// resources to validate that the volumes to destroy actually exist.
// We also check that the volumes are not being used, or not assigned
// to any pending task.
Option<Error> validate(
    const Offer::Operation::Destroy& destroy,
    const Resources& checkpointedResources,
    const hashmap<FrameworkID, Resources>& usedResources,
    const Option<FrameworkInfo>& frameworkInfo = None());


Option<Error> validate(
    const Offer::Operation::GrowVolume& growVolume,
    const protobuf::node::Capabilities& nodeCapabilities);


Option<Error> validate(
    const Offer::Operation::ShrinkVolume& shrinkVolume,
    const protobuf::node::Capabilities& nodeCapabilities);


Option<Error> validate(const Offer::Operation::CreateDisk& createDisk);


Option<Error> validate(const Offer::Operation::DestroyDisk& destroyDisk);

} // namespace operation {

} // namespace validation {
} // namespace controller {
} // namespace internal {
} // namespace medios {

#endif // __CONTROLLER_VALIDATION_HPP__
