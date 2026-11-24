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

#include "controller/validation.hpp"

#include <algorithm>
#include <cmath>
#include <iterator>
#include <regex>
#include <set>
#include <string>
#include <vector>

#include <glog/logging.h>

#include <medios/resource_quantities.hpp>
#include <medios/roles.hpp>
#include <medios/type_utils.hpp>

#include <process/authenticator.hpp>
#include <process/owned.hpp>

#include <stout/foreach.hpp>
#include <stout/hashmap.hpp>
#include <stout/hashset.hpp>
#include <stout/lambda.hpp>
#include <stout/none.hpp>
#include <stout/set.hpp>
#include <stout/stringify.hpp>
#include <stout/strings.hpp>

#include "checks/checker.hpp"
#include "checks/health_checker.hpp"

#include "common/protobuf_utils.hpp"
#include "common/validation.hpp"

#include "node/stores.hpp"

#include "controller/controller.hpp"
#include "controller/stores.hpp"

using process::Owned;

using process::http::authentication::Principal;

using std::pair;
using std::set;
using std::string;
using std::vector;

using google::protobuf::RepeatedPtrField;

using medios::scheduler::OfferConstraints;

namespace medios {
namespace internal {
namespace controller {
namespace validation {
namespace controller {
namespace call {

Option<Error> validate(const medios::controller::Call& call)
{
  if (!call.IsInitialized()) {
    return Error("Not initialized: " + call.InitializationErrorString());
  }

  if (!call.has_type()) {
    return Error("Expecting 'type' to be present");
  }

  switch (call.type()) {
    case medios::controller::Call::UNKNOWN:
      return None();

    case medios::controller::Call::GET_HEALTH:
      return None();

    case medios::controller::Call::GET_FLAGS:
      return None();

    case medios::controller::Call::GET_VERSION:
      return None();

    case medios::controller::Call::GET_METRICS:
      if (!call.has_get_metrics()) {
        return Error("Expecting 'get_metrics' to be present");
      }
      return None();

    case medios::controller::Call::GET_LOGGING_LEVEL:
      return None();

    case medios::controller::Call::SET_LOGGING_LEVEL:
      if (!call.has_set_logging_level()) {
        return Error("Expecting 'set_logging_level' to be present");
      }
      return None();

    case medios::controller::Call::LIST_FILES:
      if (!call.has_list_files()) {
        return Error("Expecting 'list_files' to be present");
      }
      return None();

    case medios::controller::Call::READ_FILE:
      if (!call.has_read_file()) {
        return Error("Expecting 'read_file' to be present");
      }
      return None();

    case medios::controller::Call::GET_STATE:
      return None();

    case medios::controller::Call::GET_NODES:
      return None();

    case medios::controller::Call::GET_FRAMEWORKS:
      return None();

    case medios::controller::Call::GET_EXECUTORS:
      return None();

    case medios::controller::Call::GET_OPERATIONS:
      return None();

    case medios::controller::Call::GET_TASKS:
      return None();

    case medios::controller::Call::GET_ROLES:
      return None();

    case medios::controller::Call::GET_WEIGHTS:
      return None();

    case medios::controller::Call::UPDATE_WEIGHTS:
      if (!call.has_update_weights()) {
        return Error("Expecting 'update_weights' to be present");
      }
      return None();

    case medios::controller::Call::GET_CONTROLLER:
      return None();

    case medios::controller::Call::SUBSCRIBE:
      return None();

    case medios::controller::Call::RESERVE_RESOURCES: {
      if (!call.has_reserve_resources()) {
        return Error("Expecting 'reserve_resources' to be present");
      }

      Option<Error> error =
        Resources::validate(call.reserve_resources().resources());

      if (error.isSome()) {
        return error;
      }

      return None();
    }

    case medios::controller::Call::UNRESERVE_RESOURCES: {
      if (!call.has_unreserve_resources()) {
        return Error("Expecting 'unreserve_resources' to be present");
      }

      Option<Error> error =
        Resources::validate(call.unreserve_resources().resources());

      if (error.isSome()) {
        return error;
      }

      return None();
    }

    case medios::controller::Call::CREATE_VOLUMES:
      if (!call.has_create_volumes()) {
        return Error("Expecting 'create_volumes' to be present");
      }
      return None();

    case medios::controller::Call::DESTROY_VOLUMES:
      if (!call.has_destroy_volumes()) {
        return Error("Expecting 'destroy_volumes' to be present");
      }
      return None();

    case medios::controller::Call::GROW_VOLUME:
      if (!call.has_grow_volume()) {
        return Error("Expecting 'grow_volume' to be present");
      }

      if (!call.grow_volume().has_node_id()) {
        return Error(
            "Expecting 'node_id' to be present; only node default resources "
            "are supported right now");
      }

      return None();

    case medios::controller::Call::SHRINK_VOLUME:
      if (!call.has_shrink_volume()) {
        return Error("Expecting 'shrink_volume' to be present");
      }

      if (!call.shrink_volume().has_node_id()) {
        return Error(
            "Expecting 'node_id' to be present; only node default resources "
            "are supported right now");
      }

      return None();

    case medios::controller::Call::GET_MAINTENANCE_STATUS:
      return None();

    case medios::controller::Call::GET_MAINTENANCE_SCHEDULE:
      return None();

    case medios::controller::Call::UPDATE_MAINTENANCE_SCHEDULE:
      if (!call.has_update_maintenance_schedule()) {
        return Error("Expecting 'update_maintenance_schedule' to be present");
      }
      return None();

    case medios::controller::Call::START_MAINTENANCE:
      if (!call.has_start_maintenance()) {
        return Error("Expecting 'start_maintenance' to be present");
      }
      return None();

    case medios::controller::Call::STOP_MAINTENANCE:
      if (!call.has_stop_maintenance()) {
        return Error("Expecting 'stop_maintenance' to be present");
      }
      return None();

    case medios::controller::Call::DRAIN_NODE:
      if (!call.has_drain_node()) {
        return Error("Expecting 'drain_node' to be present");
      }
      return None();

    case medios::controller::Call::DEACTIVATE_NODE:
      if (!call.has_deactivate_node()) {
        return Error("Expecting 'deactivate_node' to be present");
      }
      return None();

    case medios::controller::Call::REACTIVATE_NODE:
      if (!call.has_reactivate_node()) {
        return Error("Expecting 'reactivate_node' to be present");
      }
      return None();

    case medios::controller::Call::GET_QUOTA:
      return None();

    case medios::controller::Call::UPDATE_QUOTA:
      if (!call.has_update_quota()) {
        return Error("Expecting 'update_quota' to be present");
      }
      return None();

    // TODO(bmahler): Add this to a deprecated call section
    // at the bottom once deprecated by `UPDATE_QUOTA`.
    case medios::controller::Call::SET_QUOTA:
      if (!call.has_set_quota()) {
        return Error("Expecting 'set_quota' to be present");
      }
      return None();

    // TODO(bmahler): Add this to a deprecated call section
    // at the bottom once deprecated by `UPDATE_QUOTA`.
    case medios::controller::Call::REMOVE_QUOTA:
      if (!call.has_remove_quota()) {
        return Error("Expecting 'remove_quota' to be present");
      }
      return None();

    case medios::controller::Call::TEARDOWN:
      if (!call.has_teardown()) {
        return Error("Expecting 'teardown' to be present");
      }
      return None();

    case medios::controller::Call::MARK_NODE_GONE:
      if (!call.has_mark_node_gone()) {
        return Error("Expecting 'mark_node_gone' to be present");
      }
      return None();
  }

  UNREACHABLE();
}

} // namespace call {

namespace message {

static Option<Error> validateNodeInfo(const NodeInfo& nodeInfo)
{
  if (nodeInfo.has_id()) {
    Option<Error> error = common::validation::validateNodeID(nodeInfo.id());
    if (error.isSome()) {
      return error.get();
    }
  }

  Option<Error> error = Resources::validate(nodeInfo.resources());
  if (error.isSome()) {
    return error.get();
  }

  return None();
}


Option<Error> registerNode(const RegisterNodeMessage& message)
{
  const NodeInfo& nodeInfo = message.node();

  Option<Error> error = validateNodeInfo(nodeInfo);
  if (error.isSome()) {
    return error.get();
  }

  if (!message.checkpointed_resources().empty()) {
    if (!nodeInfo.has_checkpoint() || !nodeInfo.checkpoint()) {
      return Error(
          "Checkpointed resources provided when checkpointing is not enabled");
    }
  }

  foreach (const Resource& resource, message.checkpointed_resources()) {
    error = Resources::validate(resource);
    if (error.isSome()) {
      return error.get();
    }
  }

  return None();
}


Option<Error> reregisterNode(const ReregisterNodeMessage& message)
{
  hashset<FrameworkID> frameworkIDs;
  hashset<pair<FrameworkID, ExecutorID>> executorIDs;

  const NodeInfo& nodeInfo = message.node();
  Option<Error> error = validateNodeInfo(nodeInfo);
  if (error.isSome()) {
    return error.get();
  }

  foreach (const Resource& resource, message.checkpointed_resources()) {
    Option<Error> error = Resources::validate(resource);
    if (error.isSome()) {
      return error.get();
    }
  }

  foreach (const FrameworkInfo& framework, message.frameworks()) {
    // A node re-registering across a controller upgrade may carry a framework
    // that subscribed before `FrameworkInfo.principal` became mandatory (#697).
    Option<Error> error = validation::framework::validate(framework, false);
    if (error.isSome()) {
      return error.get();
    }

    if (frameworkIDs.contains(framework.id())) {
      return Error("Framework has a duplicate FrameworkID: '" +
                  stringify(framework.id()) + "'");
    }

    frameworkIDs.insert(framework.id());
  }

  foreach (const ExecutorInfo& executor, message.executor_infos()) {
    Option<Error> error = validation::executor::validate(executor);
    if (error.isSome()) {
      return error.get();
    }

    // We don't use `internal::validateResources` here because that
    // includes the `validateAllocatedToSingleRole` check, which is
    // not valid for node re-registration.
    //
    // TODO(neilc): Consider refactoring `internal::validateResources`
    // to allow some but not all checks to be applied, or else inject
    // allocation info into executor resources here.
    error = Resources::validate(executor.resources());
    if (error.isSome()) {
      return error.get();
    }

    if (!frameworkIDs.contains(executor.framework_id())) {
      return Error("Executor has an invalid FrameworkID '" +
                   stringify(executor.framework_id()) + "'");
    }

    if (executor.has_executor_id()) {
      auto id = std::make_pair(executor.framework_id(), executor.executor_id());
      if (executorIDs.contains(id)) {
        return Error("Framework '" + stringify(executor.framework_id()) +
                     "' has a duplicate ExecutorID '" +
                     stringify(executor.executor_id()) + "'");
      }

      executorIDs.insert(id);
    }
  }

  foreach (const Task& task, message.tasks()) {
    Option<Error> error = common::validation::validateTaskID(task.task_id());
    if (error.isSome()) {
      return Error("Task has an invalid TaskID: " + error->message);
    }

    if (task.node_id() != nodeInfo.id()) {
      return Error("Task has an invalid NodeID '" +
                   stringify(task.node_id()) + "'");
    }

    if (!frameworkIDs.contains(task.framework_id())) {
      return Error("Task has an invalid FrameworkID '" +
                   stringify(task.framework_id()) + "'");
    }

    // Command Executors don't send the executor ID in the task because it
    // is generated on the node (see Node::doReliableRegistration). Only
    // running tasks ought to have executors.
    if (task.has_executor_id() && task.state() == TASK_RUNNING) {
      auto id = std::make_pair(task.framework_id(), task.executor_id());
      if (!executorIDs.contains(id)) {
        return Error("Task has an invalid ExecutorID '" +
                     stringify(task.executor_id()) + "'");
      }
    }

    // We don't use `resource::validate` here because the task's
    // resources might not be in "post-reservation refinement" format.
    //
    // TODO(neilc): Consider refactoring `resource::validate` to allow
    // some but not all checks to be applied, or else convert the
    // task's resources into post-refinement format here.
    error = Resources::validate(task.resources());
    if (error.isSome()) {
      return Error("Task uses invalid resources: " + error->message);
    }
  }

  return None();
}

} // namespace message {
} // namespace controller {


namespace framework {
namespace internal {

Option<Error> validateRoles(const FrameworkInfo& frameworkInfo)
{
  bool multiRole = protobuf::frameworkHasCapability(
      frameworkInfo,
      FrameworkInfo::Capability::MULTI_ROLE);

  // Ensure that the right fields are used.
  if (multiRole) {
    if (frameworkInfo.has_role()) {
      return Error("'FrameworkInfo.role' must not be set when the"
                   " framework is MULTI_ROLE capable");
     }
  } else {
    if (frameworkInfo.roles_size() > 0) {
      return Error("'FrameworkInfo.roles' must not be set when the"
                   " framework is not MULTI_ROLE capable");
    }
  }

  // Check for duplicate entries.
  //
  // TODO(bmahler): Use a generic duplicate check function.
  if (multiRole) {
    const hashset<string> duplicateRoles = [&]() {
      hashset<string> roles;
      hashset<string> duplicates;

      foreach (const string& role, frameworkInfo.roles()) {
        if (roles.contains(role)) {
          duplicates.insert(role);
        } else {
          roles.insert(role);
        }
      }

      return duplicates;
    }();

    if (!duplicateRoles.empty()) {
      return Error("'FrameworkInfo.roles' contains duplicate items: " +
                   stringify(duplicateRoles));
     }
  }

  // Validate the role(s).
  if (multiRole) {
    foreach (const string& role, frameworkInfo.roles()) {
      Option<Error> error = roles::validate(role);
      if (error.isSome()) {
        return Error("'FrameworkInfo.roles' contains invalid role: " +
                     error->message);
      }
    }
  } else {
    Option<Error> error = roles::validate(frameworkInfo.role());
    if (error.isSome()) {
      return Error("'FrameworkInfo.role' is not a valid role: " +
                   error->message);
    }
  }

  return None();
}


Option<Error> validateFrameworkId(const FrameworkInfo& frameworkInfo)
{
  if (!frameworkInfo.has_id() || frameworkInfo.id().value().empty()) {
    return None();
  }

  return common::validation::validateID(frameworkInfo.id().value());
}


Option<Error> validateOfferFilters(const FrameworkInfo& frameworkInfo)
{
  // Use `auto` in place of `protobuf::MapPair<string, Value::Scalar>`
  // below since `foreach` is a macro and cannot contain angle brackets.
  foreach (auto&& filter, frameworkInfo.offer_filters()) {
    const OfferFilters& offerFilters = filter.second;

    Option<Error> error =
      common::validation::validateOfferFilters(offerFilters);

    if (error.isSome()) {
      return error;
    }
  }

  return None();
}


// #187: an empty FrameworkInfo.user produces silent TASK_LOST on default
// --switch_user=true nodes because the node-side user lookup ('') fails
// post-fork without surfacing a meaningful TaskStatus.message. Reject at
// SUBSCRIBE time so the framework gets a clear ERROR event instead.
Option<Error> validateUser(const FrameworkInfo& frameworkInfo)
{
  if (!frameworkInfo.has_user() || frameworkInfo.user().empty()) {
    return Error(
        "'FrameworkInfo.user' must be set to a non-empty OS user that"
        " exists on every node where this framework's tasks will run."
        " An empty value causes the node to fail at executor launch"
        " without producing meaningful task status updates"
        " (see #187 for context).");
  }

  return None();
}


// #537 (DR-037): every framework must declare a non-empty
// `FrameworkInfo.principal` (or authenticate with a principal that the
// controller binds onto `FrameworkInfo.principal`) so that the controller
// stamps `Task.framework_principal` on every launched task and records both
// halves of the on-behalf-of pair.
Option<Error> validatePrincipal(const FrameworkInfo& frameworkInfo)
{
  if (!frameworkInfo.has_principal() || frameworkInfo.principal().empty()) {
    return Error(
        "'FrameworkInfo.principal' must be set to a non-empty principal"
        " (see #537 / DR-037).");
  }

  return None();
}


Option<Error> validateFailoverTimeout(const FrameworkInfo& frameworkInfo)
{
  if (Duration::create(frameworkInfo.failover_timeout()).isSome()) {
    return None();
  }

  return Error(
      "The framework failover_timeout (" +
      stringify(frameworkInfo.failover_timeout()) + ") is invalid");
}

} // namespace internal {


Option<Error> validate(
    const medios::FrameworkInfo& frameworkInfo,
    bool validatePrincipal)
{
  // TODO(jay_guo): This currently only validates the role(s),
  // framework ID, offer filters, failover timeout, user, and principal,
  // validate more fields!
  Option<Error> error = internal::validateRoles(frameworkInfo);

  if (error.isSome()) {
    return error;
  }

  error = internal::validateFrameworkId(frameworkInfo);

  if (error.isSome()) {
    return error;
  }

  error = internal::validateOfferFilters(frameworkInfo);

  if (error.isSome()) {
    return error;
  }

  error = internal::validateFailoverTimeout(frameworkInfo);

  if(error.isSome()) {
    return error;
  }

  error = internal::validateUser(frameworkInfo);

  if (error.isSome()) {
    return error;
  }

  if (validatePrincipal) {
    error = internal::validatePrincipal(frameworkInfo);

    if (error.isSome()) {
      return error;
    }
  }

  return None();
}


Option<Error> validateUpdate(
    const FrameworkInfo& oldInfo,
    const FrameworkInfo& newInfo)
{
  Option<string> oldPrincipal = None();
  if (oldInfo.has_principal() && !oldInfo.principal().empty()) {
    oldPrincipal = oldInfo.principal();
  }

  Option<string> newPrincipal = None();
  if (newInfo.has_principal() && !newInfo.principal().empty()) {
    newPrincipal = newInfo.principal();
  }

  // Allow a pre-#537 framework recovered from a re-registering node with no
  // principal (`oldPrincipal == None()`) to adopt a non-empty `newPrincipal`
  // when its scheduler reconnects (#697), while still forbidding changing or
  // clearing an existing principal.
  if (oldPrincipal.isSome() && oldPrincipal != newPrincipal) {
    // We should not expose the old principal to the 'scheduler' which tries
    // to subscribe with a known framework ID but another principal.
    // However, it still should be possible for the people having access to
    // the controller to understand what is going on, hence the log message.
    LOG(WARNING)
      << "Framework " << oldInfo.id() << " which had a principal "
      << " '" << oldPrincipal.getOrElse("<NONE>") << "'"
      << " tried to (re)subscribe with a new principal "
      << " '" << newPrincipal.getOrElse("<NONE>") << "'";

    return Error("Changing framework's principal is not allowed.");
  }

  if (newInfo.user() != oldInfo.user()) {
    return Error(
        "Updating 'FrameworkInfo.user' is unsupported"
        "; attempted to update from '" + oldInfo.user() + "'"
        " to '" + newInfo.user() + "'");
  }

  if (newInfo.checkpoint() != oldInfo.checkpoint()) {
    return Error(
        "Updating 'FrameworkInfo.checkpoint' is unsupported"
        "; attempted to update"
        " from '" + stringify(oldInfo.checkpoint()) + "'"
        " to '" + stringify(newInfo.checkpoint()) + "'");
  }

  return None();
}


void preserveImmutableFields(
    const FrameworkInfo& oldInfo,
    FrameworkInfo* newInfo)
{
  if (newInfo->user() != oldInfo.user()) {
    LOG(WARNING)
      << "Cannot update 'FrameworkInfo.user' to '" << newInfo->user() << "'"
      << " for framework " << oldInfo.id() << "; see MESOS-703";

    newInfo->set_user(oldInfo.user());
  }

  if (newInfo->checkpoint() != oldInfo.checkpoint()) {
    LOG(WARNING)
      << "Cannot update FrameworkInfo.checkpoint to"
      << " '" << stringify(newInfo->checkpoint()) << "'"
      << " for framework " << oldInfo.id() << "; see MESOS-703";

    newInfo->set_checkpoint(oldInfo.checkpoint());
  }
}


Option<Error> validateSuppressedRoles(
    const set<string>& validFrameworkRoles,
    const set<string>& suppressedRoles)
{
  // Needed to prevent shadowing of the template '::operator-<std::set<T>>'
  // by a non-template '::medios::operator-'
  using ::operator-;

  set<string> invalidSuppressedRoles = suppressedRoles - validFrameworkRoles;
  if (!invalidSuppressedRoles.empty()) {
    return Error(
        "Suppressed roles " + stringify(invalidSuppressedRoles) +
        " are not contained in the set of roles");
  }

  return None();
}


Option<Error> validateOfferConstraintsRoles(
    const set<string>& validFrameworkRoles,
    const OfferConstraints& offerConstraints)
{
  for (const auto& pair : offerConstraints.role_constraints()) {
    const string& role = pair.first;
    if (validFrameworkRoles.count(role) < 1) {
      return Error(
          "Offer constraints specify `role_constraints` for a role '" + role +
          "'not contained in the set of roles");
    }
  }

  return None();
}


} // namespace framework {


namespace scheduler {
namespace call {

Option<Error> validate(
    const medios::scheduler::Call& call,
    const Option<Principal>& principal)
{
  if (!call.IsInitialized()) {
    return Error("Not initialized: " + call.InitializationErrorString());
  }

  if (!call.has_type()) {
    return Error("Expecting 'type' to be present");
  }

  if (call.type() == medios::scheduler::Call::SUBSCRIBE) {
    if (!call.has_subscribe()) {
      return Error("Expecting 'subscribe' to be present");
    }

    const FrameworkInfo& frameworkInfo = call.subscribe().framework_info();

    if (frameworkInfo.id() != call.framework_id()) {
      return Error("'framework_id' differs from 'subscribe.framework_info.id'");
    }

    if (principal.isSome() &&
        frameworkInfo.has_principal() &&
        !frameworkInfo.principal().empty() &&
        principal != frameworkInfo.principal()) {
      // We assume that `principal->value.isSome()` is true. The controller's HTTP
      // handlers enforce this constraint, and V0 authenticators will only
      // return principals of that form.
      CHECK_SOME(principal->value);

      return Error(
          "Authenticated principal '" + stringify(principal.get()) +
          "' does not match principal '" + frameworkInfo.principal() +
          "' set in `FrameworkInfo`");
    }

    return None();
  }

  // All calls except SUBSCRIBE should have framework id set.
  if (!call.has_framework_id()) {
    return Error("Expecting 'framework_id' to be present");
  }

  switch (call.type()) {
    case medios::scheduler::Call::SUBSCRIBE:
      // SUBSCRIBE call should have been handled above.
      LOG(FATAL) << "Unexpected 'SUBSCRIBE' call";

    case medios::scheduler::Call::TEARDOWN:
      return None();

    case medios::scheduler::Call::ACCEPT:
      if (!call.has_accept()) {
        return Error("Expecting 'accept' to be present");
      }
      return None();

    case medios::scheduler::Call::DECLINE:
      if (!call.has_decline()) {
        return Error("Expecting 'decline' to be present");
      }
      return None();

    case medios::scheduler::Call::ACCEPT_INVERSE_OFFERS:
      if (!call.has_accept_inverse_offers()) {
        return Error("Expecting 'accept_inverse_offers' to be present");
      }
      return None();

    case medios::scheduler::Call::DECLINE_INVERSE_OFFERS:
      if (!call.has_decline_inverse_offers()) {
        return Error("Expecting 'decline_inverse_offers' to be present");
      }
      return None();

    case medios::scheduler::Call::REVIVE:
      return None();

    case medios::scheduler::Call::SUPPRESS:
      return None();

    case medios::scheduler::Call::KILL:
      if (!call.has_kill()) {
        return Error("Expecting 'kill' to be present");
      }
      return None();

    case medios::scheduler::Call::SHUTDOWN:
      if (!call.has_shutdown()) {
        return Error("Expecting 'shutdown' to be present");
      }
      return None();

    case medios::scheduler::Call::ACKNOWLEDGE: {
      if (!call.has_acknowledge()) {
        return Error("Expecting 'acknowledge' to be present");
      }

      Try<id::UUID> uuid = id::UUID::fromBytes(call.acknowledge().uuid());
      if (uuid.isError()) {
        return uuid.error();
      }
      return None();
    }

    case medios::scheduler::Call::ACKNOWLEDGE_OPERATION_STATUS: {
      if (!call.has_acknowledge_operation_status()) {
        return Error("Expecting 'acknowledge_operation_status' to be present");
      }

      const medios::scheduler::Call::AcknowledgeOperationStatus& acknowledge =
        call.acknowledge_operation_status();

      Try<id::UUID> uuid = id::UUID::fromBytes(acknowledge.uuid());
      if (uuid.isError()) {
        return uuid.error();
      }

      // TODO(gkleiman): Revisit this once we introduce support for external
      // resource providers.
      if (!acknowledge.has_node_id()) {
        return Error("Expecting 'node_id' to be present");
      }

      return None();
    }

    case medios::scheduler::Call::RECONCILE:
      if (!call.has_reconcile()) {
        return Error("Expecting 'reconcile' to be present");
      }
      return None();

    case medios::scheduler::Call::RECONCILE_OPERATIONS:
      if (!call.has_reconcile_operations()) {
        return Error("Expecting 'reconcile_operations' to be present");
      }
      return None();

    case medios::scheduler::Call::MESSAGE:
      if (!call.has_message()) {
        return Error("Expecting 'message' to be present");
      }
      return None();

    case medios::scheduler::Call::REQUEST:
      if (!call.has_request()) {
        return Error("Expecting 'request' to be present");
      }
      return None();

    case medios::scheduler::Call::UPDATE_FRAMEWORK: {
      if (!call.has_update_framework()) {
        return Error("Expecting 'update_framework' to be present");
      }

      const FrameworkInfo& frameworkInfo =
        call.update_framework().framework_info();

      if (call.framework_id() != frameworkInfo.id()) {
        return Error(
            "'framework_id' differs from 'update_framework.framework_info.id'");
      }

      if (principal.isSome() &&
          frameworkInfo.has_principal() &&
          !frameworkInfo.principal().empty() &&
          principal != frameworkInfo.principal()) {
        CHECK_SOME(principal->value);

        return Error(
            "Authenticated principal '" + stringify(principal.get()) +
            "' does not match principal '" + frameworkInfo.principal() +
            "' set in `FrameworkInfo`");
      }

      return None();
    }

    case medios::scheduler::Call::UNKNOWN:
      return None();
  }

  UNREACHABLE();
}

} // namespace call {
} // namespace scheduler {


namespace resource {

// Validates the ReservationInfos specified in the given resources (if
// exist). Returns error if any ReservationInfo is found invalid or
// unsupported.
Option<Error> validateDynamicReservationInfo(
    const RepeatedPtrField<Resource>& resources)
{
  foreach (const Resource& resource, resources) {
    if (!Resources::isDynamicallyReserved(resource)) {
      continue;
    }

    if (Resources::isRevocable(resource)) {
      return Error(
          "Dynamically reserved resource " + stringify(resource) +
          " cannot be created from revocable resources");
    }
  }

  return None();
}


// Validates the DiskInfos specified in the given resources (if
// exist). Returns error if any DiskInfo is found invalid or
// unsupported.
Option<Error> validateDiskInfo(const RepeatedPtrField<Resource>& resources)
{
  foreach (const Resource& resource, resources) {
    if (!resource.has_disk()) {
      continue;
    }

    if (resource.disk().has_persistence()) {
      if (Resources::isRevocable(resource)) {
        return Error(
            "Persistent volumes cannot be created from revocable resources");
      }
      if (Resources::isUnreserved(resource)) {
        return Error(
            "Persistent volumes cannot be created from unreserved resources");
      }
      if (!resource.disk().has_volume()) {
        return Error("Expecting 'volume' to be set for persistent volume");
      }
      if (resource.disk().volume().has_host_path()) {
        return Error("Expecting 'host_path' to be unset for persistent volume");
      }

      // Ensure persistence ID meets common ID requirements.
      Option<Error> error =
        common::validation::validateID(resource.disk().persistence().id());
      if (error.isSome()) {
        return Error("Invalid persistence ID for persistent volume: " +
                     error->message);
      }
    } else if (resource.disk().has_volume()) {
      return Error("Non-persistent volume not supported");
    } else if (!resource.disk().has_source()) {
      return Error("DiskInfo is set but empty");
    }
  }

  return None();
}


// Validates the uniqueness of the persistence IDs used in the given
// resources. They need to be unique per role on each node.
Option<Error> validateUniquePersistenceID(
    const Resources& resources)
{
  hashmap<string, hashset<string>> persistenceIds;

  // Check duplicated persistence ID within the given resources.
  Resources volumes = resources.persistentVolumes();

  foreach (const Resource& volume, volumes) {
    const string& role = Resources::reservationRole(volume);
    const string& id = volume.disk().persistence().id();

    if (persistenceIds.contains(role) &&
        persistenceIds[role].contains(id)) {
      return Error("Persistence ID '" + id + "' is not unique");
    }

    persistenceIds[role].insert(id);
  }

  return None();
}


// Validates that revocable and non-revocable
// resources of the same name do not exist.
//
// TODO(vinod): Is this the right place to do this?
Option<Error> validateRevocableAndNonRevocableResources(
    const Resources& _resources)
{
  foreach (const string& name, _resources.names()) {
    Resources resources = _resources.get(name);
    if (!resources.revocable().empty() && resources != resources.revocable()) {
      return Error("Cannot use both revocable and non-revocable '" + name +
                   "' at the same time");
    }
  }

  return None();
}


// Validates that all the given resources are persistent volumes.
Option<Error> validatePersistentVolume(
    const RepeatedPtrField<Resource>& volumes)
{
  foreach (const Resource& volume, volumes) {
    if (!volume.has_disk()) {
      return Error("Resource " + stringify(volume) + " does not have DiskInfo");
    } else if (!volume.disk().has_persistence()) {
      return Error("'persistence' is not set in DiskInfo");
    } else if (!volume.disk().has_volume()) {
      return Error("Expecting 'volume' to be set for persistent volume");
    } else if (volume.disk().volume().mode() == Volume::RO) {
      return Error("Read-only persistent volume not supported");
    }
  }

  return None();
}


// Validates that all the given resources are allocated to same role.
Option<Error> validateAllocatedToSingleRole(const Resources& resources)
{
  Option<string> role;

  foreach (const Resource& resource, resources) {
    // Note that the controller normalizes `Offer::Operation` resources
    // to have allocation info set, so we can validate it here.
    if (!resource.allocation_info().has_role()) {
      return Error("The resources are not allocated to a role");
    }

    string _role = resource.allocation_info().role();

    if (role.isNone()) {
      role = _role;
      continue;
    }

    if (_role != role.get()) {
      return Error("The resources have multiple allocation roles"
                   " ('" + _role + "' and '" + role.get() + "')"
                   " but only one allocation role is allowed");
    }
  }

  return None();
}


bool detectOverlappingSetAndRangeResources(
    const vector<Resources>& resources)
{
  // If the sum of quantities of each `Resources` is not equal to the quantities
  // of the sum of `Resources`, then there is some overlap in ranges or sets.
  ResourceQuantities totalQuantities;
  Resources totalResources;

  foreach (const Resources& resources_, resources) {
    totalQuantities += ResourceQuantities::fromResources(resources_);
    totalResources += resources_;
  }

  return totalQuantities != ResourceQuantities::fromResources(totalResources);
}

namespace internal {

// Validates that all the given resources are from the same resource
// provider. Resources that do not have resource provider ID are
// assumed to be from the node (default resource provider).
Option<Error> validateSingleResourceProvider(
    const RepeatedPtrField<Resource>& resources)
{
  hashset<Option<ResourceProviderID>> resourceProviderIds;

  foreach (const Resource& resource, resources) {
    resourceProviderIds.insert(resource.has_provider_id()
      ? resource.provider_id()
      : Option<ResourceProviderID>::none());
  }

  if (resourceProviderIds.size() != 1) {
    if (resourceProviderIds.contains(None())) {
      return Error("Some resources have a resource provider and some do not");
    }

    vector<ResourceProviderID> ids;
    std::transform(
        resourceProviderIds.begin(),
        resourceProviderIds.end(),
        std::back_inserter(ids),
        [](const Option<ResourceProviderID>& id) {
          return id.get();
        });

    return Error(
        "The resources have multiple resource providers: " +
        strings::join(", ", ids));
  }

  return None();
}

} // namespace internal {


Option<Error> validate(const RepeatedPtrField<Resource>& resources)
{
  Option<Error> error = Resources::validate(resources);
  if (error.isSome()) {
    return Error("Invalid resources: " + error->message);
  }

  error = validateDiskInfo(resources);
  if (error.isSome()) {
    return Error("Invalid DiskInfo: " + error->message);
  }

  error = validateDynamicReservationInfo(resources);
  if (error.isSome()) {
    return Error("Invalid ReservationInfo: " + error->message);
  }

  return None();
}

} // namespace resource {


namespace executor {
namespace internal {

Option<Error> validateExecutorID(const ExecutorInfo& executor)
{
  // Delegate to the common ExecutorID validation. Here we wrap
  // around it to be consistent with other executor validators.
  return common::validation::validateExecutorID(executor.executor_id());
}


Option<Error> validateType(const ExecutorInfo& executor)
{
  if (executor.has_container()) {
    Option<Error> unionError =
      protobuf::validateProtobufUnion(executor.container());
    if (unionError.isSome()) {
      LOG(WARNING)
        << "Executor " << executor.executor_id()
        // NOTE: Validation of FrameworkID is done after this validation.
        << " of framework '"
        << (executor.has_framework_id() ? executor.framework_id().value() : "")
        << "' has an invalid protobuf union: "
        << unionError.get();
    }
  }
  switch (executor.type()) {
    case ExecutorInfo::DEFAULT:
      if (executor.has_command()) {
        return Error(
            "'ExecutorInfo.command' must not be set for 'DEFAULT' executor");
      }

      if (executor.has_container()) {
        if (executor.container().type() != ContainerInfo::MEDIOS) {
          return Error(
              "'ExecutorInfo.container.type' must be 'MEDIOS' for "
              "'DEFAULT' executor");
        }

        if (executor.container().medios().has_image()) {
          return Error(
              "'ExecutorInfo.container.medios.image' must not be set for "
              "'DEFAULT' executor");
        }
      }
      break;

    case ExecutorInfo::CUSTOM:
      if (!executor.has_command()) {
        return Error(
            "'ExecutorInfo.command' must be set for 'CUSTOM' executor");
      }
      break;

    case ExecutorInfo::UNKNOWN:
      // This could happen if a new executor type is introduced in the
      // protos but the  controller doesn't know about it yet (e.g., new
      // scheduler launches new type of executor on an old controller).
      return None();
  }

  return None();
}


Option<Error> validateCompatibleExecutorInfo(
    const ExecutorInfo& executor,
    Framework* framework,
    Node* node)
{
  CHECK_NOTNULL(framework);
  CHECK_NOTNULL(node);

  const ExecutorID& executorId = executor.executor_id();
  Option<ExecutorInfo> executorInfo = None();

  if (node->hasExecutor(framework->id(), executorId)) {
    executorInfo =
      node->executors.at(framework->id()).at(executorId);
  }

  if (executorInfo.isSome() && executor != executorInfo.get()) {
    return Error(
        "ExecutorInfo is not compatible with existing ExecutorInfo"
        " with same ExecutorID.\n"
        "------------------------------------------------------------\n"
        "Existing ExecutorInfo:\n" +
        stringify(executorInfo.get()) + "\n"
        "------------------------------------------------------------\n"
        "ExecutorInfo:\n" +
        stringify(executor) + "\n"
        "------------------------------------------------------------\n");
  }

  return None();
}


Option<Error> validateFrameworkID(
    const ExecutorInfo& executor,
    Framework* framework)
{
  CHECK_NOTNULL(framework);

  // The controller fills in `ExecutorInfo.framework_id` for
  // executors used in Launch operations.
  if (!executor.has_framework_id()) {
    return Error("'ExecutorInfo.framework_id' must be set");
  }

  if (executor.framework_id() != framework->id()) {
    return Error(
        "ExecutorInfo has an invalid FrameworkID"
        " (Actual: " + stringify(executor.framework_id()) +
        " vs Expected: " + stringify(framework->id()) + ")");
  }

  return None();
}


Option<Error> validateShutdownGracePeriod(const ExecutorInfo& executor)
{
  // Make sure provided duration is non-negative.
  if (executor.has_shutdown_grace_period() &&
      Nanoseconds(executor.shutdown_grace_period().nanoseconds()) <
        Duration::zero()) {
    return Error(
        "ExecutorInfo's 'shutdown_grace_period' must be non-negative");
  }

  return None();
}


Option<Error> validateResources(const ExecutorInfo& executor)
{
  Option<Error> error = resource::validate(executor.resources());
  if (error.isSome()) {
    return Error("Executor uses invalid resources: " + error->message);
  }

  const Resources& resources = executor.resources();

  error = resource::validateUniquePersistenceID(resources);
  if (error.isSome()) {
    return Error(
        "Executor uses duplicate persistence ID: " + error->message);
  }

  error = resource::validateAllocatedToSingleRole(resources);
  if (error.isSome()) {
    return Error("Invalid executor resources: " + error->message);
  }

  error = resource::validateRevocableAndNonRevocableResources(resources);
  if (error.isSome()) {
    return Error("Executor mixes revocable and non-revocable resources: " +
                 error->message);
  }

  return None();
}


// Validates the `CommandInfo` contained within an `ExecutorInfo`.
Option<Error> validateCommandInfo(const ExecutorInfo& executor)
{
  if (executor.has_command()) {
    Option<Error> error =
      common::validation::validateCommandInfo(executor.command());
    if (error.isSome()) {
      return Error("Executor's `CommandInfo` is invalid: " + error->message);
    }
  }

  return None();
}


// Validates the `ContainerInfo` contained within a `ExecutorInfo`.
Option<Error> validateContainerInfo(const ExecutorInfo& executor)
{
  if (executor.has_container()) {
    Option<Error> error =
      common::validation::validateContainerInfo(executor.container());
    if (error.isSome()) {
      return Error("Executor's `ContainerInfo` is invalid: " + error->message);
    }
  }

  return None();
}


Option<Error> validate(
    const ExecutorInfo& executor,
    Framework* framework,
    Node* node)
{
  CHECK_NOTNULL(framework);
  CHECK_NOTNULL(node);

  Option<Error> error = executor::validate(executor);
  if (error.isSome()) {
    return error;
  }

  const vector<lambda::function<Option<Error>()>> executorValidators = {
    lambda::bind(internal::validateFrameworkID, executor, framework),
    lambda::bind(internal::validateResources, executor),
    lambda::bind(
        internal::validateCompatibleExecutorInfo, executor, framework, node),
  };

  foreach (const auto& validator, executorValidators) {
    error = validator();
    if (error.isSome()) {
      return error;
    }
  }

  return None();
}

} // namespace internal {

Option<Error> validate(const ExecutorInfo& executor)
{
  const vector<lambda::function<Option<Error>(const ExecutorInfo&)>>
      executorValidators = {
    internal::validateType,
    internal::validateExecutorID,
    internal::validateShutdownGracePeriod,
    internal::validateCommandInfo,
    internal::validateContainerInfo
  };

  foreach (const auto& validator, executorValidators) {
    Option<Error> error = validator(executor);
    if (error.isSome()) {
      return error.get();
    }
  }

  return None();
}

} // namespace executor {


namespace task {

namespace internal {

Option<Error> validateTaskID(const TaskInfo& task)
{
  // Delegate to the common TaskID validation. Here we wrap
  // around it to be consistent with other task validators.
  return common::validation::validateTaskID(task.task_id());
}


// Validates that the TaskID does not collide with the ID of a running
// or unreachable task for this framework.
Option<Error> validateUniqueTaskID(const TaskInfo& task, Framework* framework)
{
  const TaskID& taskId = task.task_id();

  if (framework->tasks.contains(taskId)) {
    return Error("Task has duplicate ID: " + taskId.value());
  }

  // TODO(neilc): `unreachableTasks` is a fixed-size cache and is not
  // preserved across controller failover, so we cannot avoid all possible
  // task ID collisions (MESOS-6785).
  if (framework->unreachableTasks.contains(taskId)) {
    return Error("Task reuses the ID of an unreachable task: " +
                 taskId.value());
  }

  return None();
}


// Validates that the node ID used by a task is correct.
Option<Error> validateNodeID(const TaskInfo& task, Node* node)
{
  if (task.node_id() != node->id) {
    return Error(
        "Task uses invalid node " + task.node_id().value() +
        " while node " + node->id.value() + " is expected");
  }

  return None();
}


Option<Error> validateKillPolicy(const TaskInfo& task)
{
  if (task.has_kill_policy() &&
      task.kill_policy().has_grace_period() &&
      Nanoseconds(task.kill_policy().grace_period().nanoseconds()) <
        Duration::zero()) {
    return Error("Task's 'kill_policy.grace_period' must be non-negative");
  }

  return None();
}


Option<Error> validateProgressPolicy(const TaskInfo& task)
{
  if (!task.has_progress_policy()) {
    return None();
  }

  const ProgressPolicy& policy = task.progress_policy();

  // A guessed source reports another file's numbers as this task's
  // progress, so the source is named or nothing is read.
  if (!policy.has_source()) {
    return Error("Invalid progress_policy: 'source' is required");
  }

  if (policy.source() == ProgressPolicy::FILE) {
    if (!policy.has_path() || policy.path().empty()) {
      return Error(
          "Invalid progress_policy: 'path' is required when 'source' is FILE");
    }

    if (strings::startsWith(policy.path(), "/")) {
      return Error(
          "Invalid progress_policy: 'path' is '" + policy.path() +
          "'; it must be relative to the task's sandbox");
    }

    foreach (const std::string& component,
             strings::tokenize(policy.path(), "/")) {
      if (component == "..") {
        return Error(
            "Invalid progress_policy: 'path' is '" + policy.path() +
            "'; it may not leave the task's sandbox");
      }
    }
  } else if (policy.has_path()) {
    return Error(
        "Invalid progress_policy: 'path' is set with 'source' " +
        ProgressPolicy::Source_Name(policy.source()) +
        "; it is read only when 'source' is FILE");
  }

  if (policy.has_sample_interval() &&
      Nanoseconds(policy.sample_interval().nanoseconds()) <=
        Duration::zero()) {
    return Error("Invalid progress_policy: 'sample_interval' must be positive");
  }

  if (policy.has_stall_window() &&
      Nanoseconds(policy.stall_window().nanoseconds()) <= Duration::zero()) {
    return Error("Invalid progress_policy: 'stall_window' must be positive");
  }

  // Killing a task that stopped advancing and only reporting it are
  // different outcomes for the framework, so neither is inferred from the
  // window, and a window is what there is to act on.
  if (policy.has_stall_window() && !policy.has_action()) {
    return Error(
        "Invalid progress_policy: 'action' is required when 'stall_window'"
        " is set");
  }

  if (policy.has_action() && !policy.has_stall_window()) {
    return Error(
        "Invalid progress_policy: 'action' is set without 'stall_window', so"
        " there is no stall to act on");
  }

  if (policy.has_pattern()) {
    std::regex pattern;
    try {
      pattern = std::regex(policy.pattern(), std::regex::ECMAScript);
    } catch (const std::regex_error& e) {
      return Error(
          "Invalid progress_policy: 'pattern' '" + policy.pattern() +
          "' does not compile: " + e.what());
    }

    if (pattern.mark_count() < 1) {
      return Error(
          "Invalid progress_policy: 'pattern' '" + policy.pattern() +
          "' has no capture group; capture group one is the work value");
    }
  }

  return None();
}


Option<Error> validateObservers(const TaskInfo& task)
{
  if (task.observers().empty()) {
    return None();
  }

  if (static_cast<size_t>(task.observers().size()) >
      MAX_OBSERVERS_PER_TASK) {
    return Error(
        "Invalid observers: the task carries " +
        stringify(task.observers().size()) + " observers; the limit is " +
        stringify(MAX_OBSERVERS_PER_TASK));
  }

  std::set<std::string> names;

  foreach (const TaskObserver& observer, task.observers()) {
    // The name prefixes the observer's lines in the task record, so an
    // unnamed observer's observations could not be told from another's.
    if (!observer.has_name() || observer.name().empty()) {
      return Error("Invalid observers: 'name' is required on each observer");
    }

    const std::string& name = observer.name();

    if (name.size() > OBSERVER_NAME_MAX_LENGTH) {
      return Error(
          "Invalid observers: 'name' '" + name + "' is " +
          stringify(name.size()) + " characters; the limit is " +
          stringify(OBSERVER_NAME_MAX_LENGTH));
    }

    foreach (char c, name) {
      const bool allowed =
        (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') || c == '-';

      if (!allowed) {
        return Error(
            "Invalid observers: 'name' '" + name + "' is outside"
            " [a-z0-9-]");
      }
    }

    if (!names.insert(name).second) {
      return Error(
          "Invalid observers: 'name' '" + name + "' is used by more than one"
          " observer on this task, so their lines could not be told apart");
    }

    if (!observer.has_command()) {
      return Error(
          "Invalid observers: 'command' is required on observer '" + name +
          "'");
    }
  }

  return None();
}


// Validates `TaskInfo.finish` (DR-042). The hook is run by the built-in
// command executor only, so a task that names its own executor or runs
// under the docker executor is refused here, naming the field, rather
// than launched with the hook silently skipped. The cap is required and
// counted inside `max_completion_time`: a cap the ceiling cannot hold
// would be a deadline the executor never honours.
Option<Error> validateFinish(const TaskInfo& task)
{
  if (!task.has_finish()) {
    return None();
  }

  const TaskFinish& finish = task.finish();

  if (task.has_executor()) {
    return Error(
        "Invalid finish: the task names executor '" +
        task.executor().executor_id().value() + "', which does not run a"
        " finish hook; the built-in command executor runs it");
  }

  if (task.has_container() &&
      task.container().type() == ContainerInfo::DOCKER) {
    return Error(
        "Invalid finish: the task is a docker task, and the docker executor"
        " does not run a finish hook; the built-in command executor runs it");
  }

  if (!finish.has_command()) {
    return Error("Invalid finish: 'command' is required");
  }

  // The launcher requires `value` on both the shell and the argv path;
  // `arguments` add to it and never stand in for it.
  if (!finish.command().has_value() || finish.command().value().empty()) {
    return Error("Invalid finish: 'command.value' is required");
  }

  if (!finish.has_cap()) {
    return Error(
        "Invalid finish: 'cap' is required; there is no default, as for"
        " 'stall_window'");
  }

  const Duration cap = Nanoseconds(finish.cap().nanoseconds());
  if (cap <= Duration::zero()) {
    return Error("Invalid finish: 'cap' must be positive");
  }

  // A zero `max_completion_time` is no ceiling, as the executor reads it.
  if (task.has_max_completion_time() &&
      task.max_completion_time().nanoseconds() > 0) {
    const Duration ceiling =
      Nanoseconds(task.max_completion_time().nanoseconds());
    if (cap > ceiling) {
      return Error(
          "Invalid finish: 'cap' " + stringify(cap) + " is larger than the"
          " task's 'max_completion_time' " + stringify(ceiling) + "; the cap"
          " is counted inside the task's ceiling and the executor never"
          " extends a deadline");
    }
  }

  return None();
}


Option<Error> validateMaxCompletionTime(const TaskInfo& task)
{
  if (task.has_max_completion_time() &&
      Nanoseconds(task.max_completion_time().nanoseconds()) <
        Duration::zero()) {
    return Error("Task's `max_completion_time` must be non-negative");
  }

  return None();
}


Option<Error> validateCheck(const TaskInfo& task)
{
  if (task.has_check()) {
    Option<Error> error = common::validation::validateCheckInfo(task.check());
    if (error.isSome()) {
      return Error("Task uses invalid check: " + error->message);
    }
  }

  return None();
}


Option<Error> validateHealthCheck(const TaskInfo& task)
{
  if (task.has_health_check()) {
    Option<Error> error =
      common::validation::validateHealthCheck(task.health_check());
    if (error.isSome()) {
      return Error("Task uses invalid health check: " + error->message);
    }
  }

  return None();
}


Option<Error> validateResources(const TaskInfo& task)
{
  if (task.resources().empty()) {
    return Error("Task uses no resources");
  }

  Option<Error> error = resource::validate(task.resources());
  if (error.isSome()) {
    return Error("Task uses invalid resources: " + error->message);
  }

  const Resources& resources = task.resources();

  error = resource::validateUniquePersistenceID(resources);
  if (error.isSome()) {
    return Error("Task uses duplicate persistence ID: " + error->message);
  }

  error = resource::validateAllocatedToSingleRole(resources);
  if (error.isSome()) {
    return Error("Invalid task resources: " + error->message);
  }

  error = resource::validateRevocableAndNonRevocableResources(resources);
  if (error.isSome()) {
    return Error("Task mixes revocable and non-revocable resources: " +
                 error->message);
  }

  return None();
}


Option<Error> validateTaskAndExecutorResources(const TaskInfo& task)
{
  Resources total = task.resources();
  if (task.has_executor()) {
    total += task.executor().resources();
  }

  Option<Error> error = resource::validate(total);
  if (error.isSome()) {
    return Error(
        "Task and its executor use invalid resources: " + error->message);
  }

  // We perform the check for `has_executor()` again here because we needed to
  // validate the total resources before checking for overlapping ranges/sets.
  if (task.has_executor()) {
    if (resource::detectOverlappingSetAndRangeResources({
            task.resources(),
            task.executor().resources()})) {
      return Error("There are overlapping resources in the task resources " +
                   stringify(task.resources()) + " and executor resources " +
                   stringify(task.executor().resources()));
    }
  }

  error = resource::validateUniquePersistenceID(total);
  if (error.isSome()) {
    return Error("Task and its executor use duplicate persistence ID: " +
                 error->message);
  }

  error = resource::validateRevocableAndNonRevocableResources(total);
  if (error.isSome()) {
    return Error("Task and its executor mix revocable and non-revocable"
                 " resources: " + error->message);
  }

  return None();
}


// Validates `TaskInfo.principal` (DR-037). The field is required and has the
// shape `kind:name`, kind one of `operator`, `team` or `run`, name one or more
// characters from `A-Za-z0-9._-`. The shape is checked here, where a principal
// enters the controller; everywhere else the string is opaque. Nothing here
// verifies that the name is true: that is the assertion level, not the record.
Option<Error> validatePrincipal(const TaskInfo& task)
{
  if (!task.has_principal() || task.principal().empty()) {
    return Error(
        "Task has no `principal`; it is required, as `kind:name` with kind"
        " `operator`, `team` or `run`");
  }

  const string& principal = task.principal();
  const size_t colon = principal.find(':');
  if (colon == string::npos) {
    return Error(
        "Task `principal` '" + principal + "' is not of the form `kind:name`");
  }

  const string kind = principal.substr(0, colon);
  if (kind != "operator" && kind != "team" && kind != "run") {
    return Error(
        "Task `principal` '" + principal + "' has kind '" + kind +
        "'; it must be `operator`, `team` or `run`");
  }

  const string name = principal.substr(colon + 1);
  if (name.empty()) {
    return Error(
        "Task `principal` '" + principal + "' has an empty name after the kind");
  }

  if (name.size() > PRINCIPAL_NAME_MAX_LENGTH) {
    return Error(
        "Task `principal` has a name of " + stringify(name.size()) +
        " characters after the kind; the limit is " +
        stringify(PRINCIPAL_NAME_MAX_LENGTH));
  }

  foreach (char c, name) {
    const bool allowed =
      (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') ||
      (c >= '0' && c <= '9') || c == '.' || c == '_' || c == '-';
    if (!allowed) {
      return Error(
          "Task `principal` '" + principal + "' has a name with a character"
          " outside `A-Za-z0-9._-`");
    }
  }

  return None();
}


// Validates the `CommandInfo` contained within a `TaskInfo`.
Option<Error> validateCommandInfo(const TaskInfo& task)
{
  if (task.has_command()) {
    Option<Error> error =
      common::validation::validateCommandInfo(task.command());
    if (error.isSome()) {
      return Error("Task's `CommandInfo` is invalid: " + error->message);
    }
  }

  return None();
}


// Validates the `ContainerInfo` contained within a `TaskInfo`.
Option<Error> validateContainerInfo(const TaskInfo& task)
{
  if (task.has_container()) {
    Option<Error> error =
      common::validation::validateContainerInfo(task.container());
    if (error.isSome()) {
      return Error("Task's `ContainerInfo` is invalid: " + error->message);
    }
  }

  return None();
}


// A source type's name for a refusal, so the framework reads which kind
// of volume it sent.
static string volumeSourceName(const Volume& volume)
{
  if (volume.has_source()) {
    return Volume::Source::Type_Name(volume.source().type());
  }

  if (volume.has_image()) {
    return "IMAGE";
  }

  return "HOST_PATH";
}


// Validates the container paths of a task's volumes (DR-036 revision,
// medios#602). A path is absolute, or one relative path component, which
// names `<sandbox>/<component>`: the one form every node delivers, Linux
// and darwin alike. Refuses an empty path, a relative path with a `/` in
// it, `.` and `..`, and two volumes at the same path. Whether a node can
// place an absolute path is the node's to say, since the controller does
// not know which filesystem isolator a node runs.
//
// One component is a default, not a requirement of any delivery path: a
// link or bind at a nested relative path needs its parent directories
// made in the sandbox first, and one rule on both platforms is simpler.
// Revisit if a framework needs a nested path such as `data/tables`.
Option<Error> validateStorePaths(const TaskInfo& task)
{
  if (!task.has_container()) {
    return None();
  }

  hashset<string> paths;

  foreach (const Volume& volume, task.container().volumes()) {
    const string& path = volume.container_path();

    if (path.empty()) {
      return Error(
          "A volume has an empty container path; name a directory inside "
          "the sandbox, such as 'data', or an absolute path");
    }

    if (path.front() != '/') {
      if (path == "." || path == "..") {
        return Error(
            "Volume container path '" + path + "' names the sandbox or its "
            "parent; a relative container path is one name inside the "
            "sandbox, such as 'data'");
      }

      if (strings::contains(path, "/")) {
        return Error(
            "Volume container path '" + path + "' is a relative path with "
            "more than one component; a relative container path is one "
            "name inside the sandbox, such as 'data'");
      }
    }

    if (!paths.insert(path).second) {
      return Error(
          "Two volumes on the task have container path '" + path + "'; "
          "each volume needs its own");
    }
  }

  return None();
}


// Validates the stores a task asks for (DR-036, medios#529). Every volume
// on the task is a STORE volume; the store is declared on the offering
// node; the task lists one copy of the store's shared resource; and the
// requested mode has a free unit on that node, where the holders are the
// node's non-terminal tasks (`heldStores`). Refuses, in that order, a
// volume of any other source type, an undeclared store, a store named
// twice, a request missing its resource, `exclusive` on a read-only
// request, a read-write request on a store declared `writers: 0`, and a
// holder beyond the declared count. The last is the one place a second
// writer is refused.
//
// Sequential by the same rule as `validate` as a whole: the controller
// adds each task before validating the next, so a launch of several tasks
// counts the earlier ones.
Option<Error> validateStores(const TaskInfo& task, Node* node)
{
  CHECK_NOTNULL(node);

  // Volumes are declared on the task. An executor's volumes would outlive
  // the tasks whose units they held, and nothing counts them.
  if (task.has_executor() && task.executor().has_container() &&
      task.executor().container().volumes_size() > 0) {
    return Error(
        "Executor '" + task.executor().executor_id().value() + "' declares "
        "volumes; volumes are declared on the task as STORE volumes");
  }

  if (!task.has_container() || task.container().volumes_size() == 0) {
    return None();
  }

  hashmap<string, node::StoreDeclaration> declared;
  foreach (const Resource& resource, node->totalResources) {
    Try<node::StoreDeclaration> store = node::storeOf(resource);
    if (store.isSome()) {
      declared[store->name] = *store;
    }
  }

  const hashmap<string, StoreHolders> held = heldStores(*node);
  const Resources taskResources = task.resources();
  const string nodeId = node->id.value();

  hashset<string> named;

  foreach (const Volume& volume, task.container().volumes()) {
    if (!volume.has_source() ||
        volume.source().type() != Volume::Source::STORE) {
      return Error(
          "Volume '" + volume.container_path() + "' has source type " +
          volumeSourceName(volume) + "; only STORE volumes are accepted, "
          "and a host path is not (DR-036)");
    }

    const Volume::Source::StoreVolume& request = volume.source().store();
    const string& name = request.name();

    if (!declared.contains(name)) {
      return Error(
          "Store '" + name + "' is not declared on node " + nodeId);
    }

    if (!named.insert(name).second) {
      return Error(
          "Store '" + name + "' is named twice on the task; a task holds a "
          "store once");
    }

    // The copy that puts the holder on the shared-resource ledger.
    const string resourceName = node::storeResourceName(name);
    bool listed = false;
    foreach (const Resource& resource, taskResources) {
      if (resource.name() == resourceName && Resources::isShared(resource)) {
        listed = true;
        break;
      }
    }

    if (!listed) {
      return Error(
          "Task mounts store '" + name + "' but does not list the shared "
          "resource '" + resourceName + "' in its resources");
    }

    const node::StoreDeclaration& store = declared.at(name);
    const StoreHolders holders =
      held.get(name).getOrElse(StoreHolders());

    if (volume.mode() == Volume::RO) {
      if (request.exclusive()) {
        return Error(
            "Store '" + name + "' is requested read-only and exclusive; "
            "exclusive takes every writer unit and is for a read-write "
            "request");
      }

      // A node that cannot make a store read-only, filesystem/posix,
      // declares it readers 0 (medios#602).
      if (store.readers == 0) {
        return Error(
            "Store '" + name + "' is read-write only by declaration on "
            "node " + nodeId + " (readers 0); request it RW");
      }

      if (holders.readers >= store.readers) {
        return Error(
            "Store '" + name + "' has no free readers unit on node " +
            nodeId + ": " + stringify(holders.readers) + " of " +
            stringify(store.readers) + " held");
      }
    } else {
      if (store.writers == 0) {
        return Error(
            "Store '" + name + "' is read-only by declaration on node " +
            nodeId + " (writers 0); request it RO");
      }

      if (request.exclusive() && holders.writers > 0) {
        return Error(
            "Store '" + name + "' is requested exclusive and has " +
            stringify(holders.writers) + " of " + stringify(store.writers) +
            " writers units held on node " + nodeId);
      }

      if (holders.writers >= store.writers) {
        return Error(
            "Store '" + name + "' has no free writers unit on node " +
            nodeId + ": " + stringify(holders.writers) + " of " +
            stringify(store.writers) + " held");
      }
    }
  }

  return None();
}


Option<Error> validateResourceLimits(
    const TaskInfo& task,
    Node* node)
{
  auto limits = task.limits();

  if (!limits.empty()) {
    if (!node->capabilities.taskResourceLimits) {
      return Error("Node is not capable of handling task resource limits");
    }

    // Ensure that only "cpus" and "mem" are included.
    const size_t cpuCount = limits.count("cpus");
    const size_t memCount = limits.count("mem");

    if (limits.size() > cpuCount + memCount) {
      return Error(
          "Only cpus and mem may be included in a task's resource limits");
    }

    if (cpuCount) {
      Option<double> taskCpus = Resources(task.resources()).cpus();
      if (taskCpus.isNone()) {
        return Error(
            "When a CPU limit is specified, a CPU request must also be "
            "specified");
      }

      if (limits.at("cpus").value() < taskCpus.get()) {
        return Error(
            "The cpu limit must be greater than or equal to the cpu request");
      }
    }

    if (memCount) {
      Option<Bytes> taskMem = Resources(task.resources()).mem();
      if (taskMem.isNone()) {
        return Error(
            "When a memory limit is specified, a memory request must also be "
            "specified");
      }

      if (!std::isinf(limits.at("mem").value()) &&
          Bytes(limits.at("mem").value(), Bytes::MEGABYTES) < taskMem.get()) {
        return Error(
            "The memory limit must be greater"
            " than or equal to the memory request");
      }
    }
  }

  return None();
}


// This validation function should only be executed for tasks which are launched
// via the LAUNCH operation, not the LAUNCH_GROUP operation.
Option<Error> validateShareCgroups(const TaskInfo& task)
{
  if (task.has_container() &&
      task.container().has_linux_info() &&
      task.container().linux_info().has_share_cgroups() &&
      !task.container().linux_info().share_cgroups()) {
    return Error(
        "Only tasks in a task group may have 'share_cgroups' set to 'false'");
  }

  return None();
}


// Validates task specific fields except its executor (if it exists).
Option<Error> validateTask(
    const TaskInfo& task,
    Framework* framework,
    Node* node)
{
  CHECK_NOTNULL(framework);
  CHECK_NOTNULL(node);

  // NOTE: The order in which the following validate functions are
  // executed does matter!
  vector<lambda::function<Option<Error>()>> validators = {
    lambda::bind(internal::validateTaskID, task),
    lambda::bind(internal::validateUniqueTaskID, task, framework),
    lambda::bind(internal::validateNodeID, task, node),
    lambda::bind(internal::validatePrincipal, task),
    lambda::bind(internal::validateKillPolicy, task),
    lambda::bind(internal::validateMaxCompletionTime, task),
    lambda::bind(internal::validateProgressPolicy, task),
    lambda::bind(internal::validateObservers, task),
    lambda::bind(internal::validateFinish, task),
    lambda::bind(internal::validateCheck, task),
    lambda::bind(internal::validateHealthCheck, task),
    lambda::bind(internal::validateResources, task),
    lambda::bind(internal::validateCommandInfo, task),
    lambda::bind(internal::validateContainerInfo, task),
    lambda::bind(internal::validateStorePaths, task),
    lambda::bind(internal::validateStores, task, node),
    lambda::bind(internal::validateResourceLimits, task, node)
  };

  foreach (const lambda::function<Option<Error>()>& validator, validators) {
    Option<Error> error = validator();
    if (error.isSome()) {
      return error;
    }
  }

  return None();
}


// Validates `Task.executor` if it exists.
Option<Error> validateExecutor(
    const TaskInfo& task,
    Framework* framework,
    Node* node,
    const Resources& offered)
{
  CHECK_NOTNULL(framework);
  CHECK_NOTNULL(node);

  if (task.has_executor() == task.has_command()) {
    return Error(
        "Task should have at least one (but not both) of CommandInfo or "
        "ExecutorInfo present");
  }

  Resources total = task.resources();

  Option<Error> error = None();

  if (task.has_executor()) {
    const ExecutorInfo& executor = task.executor();

    // Do the general validation first.
    error = executor::internal::validate(executor, framework, node);
    if (error.isSome()) {
      return error;
    }

    // Now do specific validation when an executor is specified on `Task`.

    // TODO(vinod): Revisit this when we allow schedulers to explicitly
    // specify 'DEFAULT' executor in the `LAUNCH` operation.

    if (executor.has_type() && executor.type() != ExecutorInfo::CUSTOM) {
      return Error("'ExecutorInfo.type' must be 'CUSTOM'");
    }

    // While `ExecutorInfo.command` is optional in the protobuf,
    // semantically it is still required for backwards compatibility.
    if (!executor.has_command()) {
      return Error("'ExecutorInfo.command' must be set");
    }

    // TODO(martin): MESOS-1807. Return Error instead of logging a
    // warning.
    const Resources& executorResources = executor.resources();

    // Ensure there are no shared resources in the executor resources.
    //
    // TODO(anindya_sinha): For simplicity we currently don't
    // allow shared resources in ExecutorInfo. See comments in
    // `HierarchicalAllocatorProcess::updateAllocation()` for more
    // details. Remove this check once we start supporting it.
    if (!executorResources.shared().empty()) {
      return Error(
          "Executor resources " + stringify(executorResources) +
          " should not contain any shared resources");
    }

    Option<double> cpus = executorResources.cpus();
    if (cpus.isNone() || cpus.get() < MIN_CPUS) {
      LOG(WARNING)
        << "Executor '" << task.executor().executor_id()
        << "' for task '" << task.task_id()
        << "' uses less CPUs ("
        << (cpus.isSome() ? stringify(cpus.get()) : "None")
        << ") than the minimum required (" << MIN_CPUS
        << "). Please update your executor, as this will be mandatory "
        << "in future releases.";
    }

    Option<Bytes> mem = executorResources.mem();
    if (mem.isNone() || mem.get() < MIN_MEM) {
      LOG(WARNING)
        << "Executor '" << task.executor().executor_id()
        << "' for task '" << task.task_id()
        << "' uses less memory ("
        << (mem.isSome() ? stringify(mem.get()) : "None")
        << ") than the minimum required (" << MIN_MEM
        << "). Please update your executor, as this will be mandatory "
        << "in future releases.";
    }

    if (executor.has_container() &&
        executor.container().has_linux_info() &&
        executor.container().linux_info().has_share_cgroups() &&
        executor.container().linux_info().share_cgroups()) {
      return Error(
          "The 'share_cgroups' field cannot be set to 'true'"
          " on executor containers");
    }

    if (!node->hasExecutor(framework->id(), task.executor().executor_id())) {
      total += executorResources;
    }
  }

  // Now validate combined resources of task and executor.

  // NOTE: This is refactored into a separate function
  // so that it can be easily unit tested.
  error = task::internal::validateTaskAndExecutorResources(task);

  if (error.isSome()) {
    return error;
  }

  if (!offered.contains(total)) {
    return Error(
        "Total resources " + stringify(total) + " required by task and its"
        " executor is more than available " + stringify(offered));
  }

  return None();
}

} // namespace internal {


// Validate task and its executor (if it exists).
Option<Error> validate(
    const TaskInfo& task,
    Framework* framework,
    Node* node,
    const Resources& offered)
{
  CHECK_NOTNULL(framework);
  CHECK_NOTNULL(node);

  vector<lambda::function<Option<Error>()>> validators = {
    lambda::bind(internal::validateTask, task, framework, node),
    lambda::bind(internal::validateExecutor, task, framework, node, offered),
    lambda::bind(internal::validateShareCgroups, task)
  };

  foreach (const lambda::function<Option<Error>()>& validator, validators) {
    Option<Error> error = validator();
    if (error.isSome()) {
      return error;
    }
  }

  return None();
}


namespace group {

namespace internal {

Option<Error> validateTask(
    const TaskInfo& task,
    Framework* framework,
    Node* node)
{
  CHECK_NOTNULL(framework);
  CHECK_NOTNULL(node);

  // Before the general validation, which would refuse a finish for the
  // executor the group names rather than for the group: the default
  // executor runs a task group, and it does not run a finish hook (DR-042).
  if (task.has_finish()) {
    return Error(
        "Invalid finish: a task in a task group runs under the default"
        " executor, which does not run a finish hook");
  }

  // Do the general validation first.
  Option<Error> error = task::internal::validateTask(task, framework, node);
  if (error.isSome()) {
    return error;
  }

  // Now do `TaskGroup` specific validation.

  if (!task.has_executor()) {
    return Error("'TaskInfo.executor' must be set");
  }

  if (task.has_container()) {
    if (!task.container().network_infos().empty()) {
      if (task.has_health_check() &&
          (task.health_check().type() == HealthCheck::HTTP ||
           task.health_check().type() == HealthCheck::TCP)) {
        return Error("HTTP and TCP health checks are not supported for "
                     "nested containers not joining parent's network");
      }
    }

    // Group-only constraint: task groups run under the default executor,
    // which supports nested MEDIOS containers only. Plain LAUNCH tasks may
    // carry a DOCKER ContainerInfo (the docker_shell path, DR-030).
    if (task.container().type() == ContainerInfo::DOCKER) {
      return Error(
          "Docker ContainerInfo is not supported on tasks in a task group");
    }
  }

  if (!task.limits().empty() &&
      !(task.has_container() &&
        task.container().has_linux_info() &&
        !task.container().linux_info().share_cgroups())) {
    return Error(
        "Resource limits may only be set for tasks within a task group when "
        "the 'share_cgroups' field is set to 'false'.");
  }

  return None();
}


Option<Error> validateTaskGroupAndExecutorResources(
    const TaskGroupInfo& taskGroup,
    const ExecutorInfo& executor)
{
  Resources total = executor.resources();

  vector<Resources> taskResources;
  foreach (const TaskInfo& task, taskGroup.tasks()) {
    taskResources.push_back(task.resources());
    total += task.resources();
  }

  Option<Error> error = resource::validateUniquePersistenceID(total);
  if (error.isSome()) {
    return Error("Task group and executor use duplicate persistence ID: " +
                 error->message);
  }

  error = resource::validateRevocableAndNonRevocableResources(total);
  if (error.isSome()) {
    return Error("Task group and executor mix revocable and non-revocable"
                 " resources: " + error->message);
  }

  vector<Resources> allResources(taskResources);
  allResources.push_back(executor.resources());

  if (resource::detectOverlappingSetAndRangeResources(allResources)) {
    return Error("There are overlapping resources in the task group's task"
                 " resources " + stringify(taskResources) +
                 " and/or executor resources " +
                 stringify(executor.resources()));
  }

  return None();
}


Option<Error> validateExecutor(
    const TaskGroupInfo& taskGroup,
    const ExecutorInfo& executor,
    Framework* framework,
    Node* node,
    const Resources& offered)
{
  CHECK_NOTNULL(framework);
  CHECK_NOTNULL(node);

  // Do the general validation first.
  Option<Error> error =
    executor::internal::validate(executor, framework, node);

  if (error.isSome()) {
    return error;
  }

  // Now do `TaskGroup` specific validation.

  if (!executor.has_type()) {
    return Error("'ExecutorInfo.type' must be set");
  }

  if (executor.type() == ExecutorInfo::UNKNOWN) {
    return Error("Unknown executor type");
  }

  if (executor.has_container() &&
      executor.container().type() == ContainerInfo::DOCKER) {
    return Error(
        "Docker ContainerInfo is not supported on a task group's executor");
  }

  // Validate the `ExecutorInfo` in all tasks are same.

  foreach (const TaskInfo& task, taskGroup.tasks()) {
    if (task.has_executor() && task.executor() != executor) {
      return Error(
          "The `ExecutorInfo` of "
          "task '" + stringify(task.task_id()) + "' is different from "
          "executor '" + stringify(executor.executor_id()) + "'");
    }
  }

  const Resources& executorResources = executor.resources();

  // Validate minimal cpus and memory resources of executor.
  Option<double> cpus = executorResources.cpus();
  if (cpus.isNone() || cpus.get() < MIN_CPUS) {
    return Error(
      "Executor '" + stringify(executor.executor_id()) +
      "' uses less CPUs (" +
      (cpus.isSome() ? stringify(cpus.get()) : "None") +
      ") than the minimum required (" + stringify(MIN_CPUS) + ")");
  }

  Option<Bytes> mem = executorResources.mem();
  if (mem.isNone() || mem.get() < MIN_MEM) {
    return Error(
      "Executor '" + stringify(executor.executor_id()) +
      "' uses less memory (" +
      (mem.isSome() ? stringify(mem.get()) : "None") +
      ") than the minimum required (" + stringify(MIN_MEM) + ")");
  }

  Option<Bytes> disk = executorResources.disk();
  if (disk.isNone()) {
    return Error(
      "Executor '" + stringify(executor.executor_id()) + "' uses no disk");
  }

  // Validate combined resources of task group and executor.

  // NOTE: This is refactored into a separate function so that it can
  // be easily unit tested.
  error = internal::validateTaskGroupAndExecutorResources(taskGroup, executor);
  if (error.isSome()) {
    return error;
  }

  Resources total;
  foreach (const TaskInfo& task, taskGroup.tasks()) {
    total += task.resources();
  }

  if (!node->hasExecutor(framework->id(), executor.executor_id())) {
    total += executorResources;
  }

  if (!offered.contains(total)) {
    return Error(
        "Total resources " + stringify(total) + " required by task group and"
        " its executor are more than available " + stringify(offered));
  }

  if (executor.has_command()) {
    Option<Error> error =
      common::validation::validateCommandInfo(executor.command());
    if (error.isSome()) {
      return Error(
          "Executor '" + stringify(executor.executor_id()) + "'" +
          "contains an invalid command: " + error->message);
    }
  }

  return None();
}


Option<Error> validateShareCgroups(
    const TaskGroupInfo& taskGroup,
    const ExecutorInfo& executorInfo,
    Framework* framework,
    Node* node)
{
  if (executorInfo.has_container() &&
      executorInfo.container().has_linux_info() &&
      executorInfo.container().linux_info().has_share_cgroups() &&
      executorInfo.container().linux_info().share_cgroups()) {
    return Error(
        "The 'share_cgroups' field cannot be set to 'true' on "
        "executor containers");
  }

  // If any task in a task group has 'share_cgroups' set to 'false', then all
  // tasks in the task group must have it set to 'false'. We use this local
  // variable to track the value.
  Option<bool> shareCgroups;

  // Helper function to determine whether or not we've seen a task in this task
  // group or under this executor with a different value of 'share_cgroups'.
  auto validateShareCgroupsForTask = [](
      Option<bool>& shareCgroups,
      const Option<ContainerInfo>& container) -> Option<Error> {
    // If the task does not have 'LinuxInfo' set, then we treat it as
    // having 'share_cgroups==true' for validation purposes, since that is
    // the default behavior.
    bool taskShareCgroups =
      (container.isSome() &&
       container->has_linux_info() &&
       container->linux_info().has_share_cgroups()) ?
         container->linux_info().share_cgroups() :
         true;

    if (shareCgroups.isNone()) {
      shareCgroups = taskShareCgroups;
    } else if (taskShareCgroups != shareCgroups.get()) {
      return Error(
          "If set, the value of 'share_cgroups' must be the same for all "
          "tasks in each task group and under a single executor");
    }

    return None();
  };

  foreach (const TaskInfo& task, taskGroup.tasks()) {
    Option<Error> error = validateShareCgroupsForTask(
        shareCgroups,
        task.has_container() ?
          task.container() :
          Option<ContainerInfo>::none());

    if (error.isSome()) {
      return error;
    }
  }

  CHECK_NOTNULL(framework);

  // If this executor already exists, ensure that all tasks under it
  // have the same value of 'share_cgroups'.
  if (shareCgroups.isSome() &&
      framework->executors.contains(node->id) &&
      framework->executors.at(node->id)
        .contains(executorInfo.executor_id())) {
    foreachvalue (const Task* task, framework->tasks) {
      CHECK_NOTNULL(task);

      if (task->node_id() == node->id &&
          task->has_executor_id() &&
          task->executor_id() == executorInfo.executor_id()) {
        Option<Error> error = validateShareCgroupsForTask(
            shareCgroups,
            task->has_container() ?
              task->container() :
              Option<ContainerInfo>::none());

        if (error.isSome()) {
          return error;
        }
      }
    }

    // Unreachable tasks are held in the `Framework` struct and in the `Nodes`
    // struct. Rather than passing `Nodes` to this function to find unreachable
    // tasks for only one node, we look through all unreachable tasks via the
    // `Framework` struct, which is already available.
    foreachvalue (const Owned<Task>& task, framework->unreachableTasks) {
      CHECK_NOTNULL(task);

      if (task->node_id() == node->id &&
          task->has_executor_id() &&
          task->executor_id() == executorInfo.executor_id()) {
        Option<Error> error = validateShareCgroupsForTask(
            shareCgroups,
            task->has_container() ?
              task->container() :
              Option<ContainerInfo>::none());

        if (error.isSome()) {
          return error;
        }
      }
    }
  }

  return None();
}

} // namespace internal {


Option<Error> validate(
    const TaskGroupInfo& taskGroup,
    const ExecutorInfo& executor,
    Framework* framework,
    Node* node,
    const Resources& offered)
{
  CHECK_NOTNULL(framework);
  CHECK_NOTNULL(node);

  foreach (const TaskInfo& task, taskGroup.tasks()) {
    Option<Error> error = internal::validateTask(task, framework, node);
    if (error.isSome()) {
      return Error("Task '" + stringify(task.task_id()) + "' is invalid: " +
                   error->message);
    }
  }

  vector<lambda::function<Option<Error>()>> validators = {
    lambda::bind(
        internal::validateExecutor,
        taskGroup,
        executor,
        framework,
        node,
        offered),
    lambda::bind(
        internal::validateShareCgroups, taskGroup, executor, framework, node)
  };

  foreach (const lambda::function<Option<Error>()>& validator, validators) {
    Option<Error> error = validator();
    if (error.isSome()) {
      return error;
    }
  }

  return None();
}

} // namespace group {

} // namespace task {


namespace offer {

Offer* getOffer(Controller* controller, const OfferID& offerId)
{
  CHECK_NOTNULL(controller);
  return controller->getOffer(offerId);
}


InverseOffer* getInverseOffer(Controller* controller, const OfferID& offerId)
{
  CHECK_NOTNULL(controller);
  return controller->getInverseOffer(offerId);
}


Node* getNode(Controller* controller, const NodeID& nodeId)
{
  CHECK_NOTNULL(controller);
  return controller->nodes.registered.get(nodeId);
}


Try<NodeID> getNodeId(Controller* controller, const OfferID& offerId)
{
  // Try as an offer.
  Offer* offer = getOffer(controller, offerId);
  if (offer != nullptr) {
    return offer->node_id();
  }

  InverseOffer* inverseOffer = getInverseOffer(controller, offerId);
  if (inverseOffer != nullptr) {
    return inverseOffer->node_id();
  }

  return Error("Offer " + stringify(offerId) + " is no longer valid");
}


Try<FrameworkID> getFrameworkId(Controller* controller, const OfferID& offerId)
{
  // Try as an offer.
  Offer* offer = getOffer(controller, offerId);
  if (offer != nullptr) {
    return offer->framework_id();
  }

  InverseOffer* inverseOffer = getInverseOffer(controller, offerId);
  if (inverseOffer != nullptr) {
    return inverseOffer->framework_id();
  }

  return Error("Offer " + stringify(offerId) + " is no longer valid");
}


Option<Error> validateOfferIds(
    const RepeatedPtrField<OfferID>& offerIds,
    Controller* controller)
{
  foreach (const OfferID& offerId, offerIds) {
    Offer* offer = getOffer(controller, offerId);
    if (offer == nullptr) {
      return Error("Offer " + stringify(offerId) + " is no longer valid");
    }
  }

  return None();
}


Option<Error> validateInverseOfferIds(
    const RepeatedPtrField<OfferID>& offerIds,
    Controller* controller)
{
  foreach (const OfferID& offerId, offerIds) {
    InverseOffer* inverseOffer = getInverseOffer(controller, offerId);
    if (inverseOffer == nullptr) {
      return Error(
          "Inverse offer " + stringify(offerId) + " is no longer valid");
    }
  }

  return None();
}


// Validates that an offer only appears once in offer list.
Option<Error> validateUniqueOfferID(const RepeatedPtrField<OfferID>& offerIds)
{
  hashset<OfferID> offers;

  foreach (const OfferID& offerId, offerIds) {
    if (offers.contains(offerId)) {
      return Error("Duplicate offer " + stringify(offerId) + " in offer list");
    }

    offers.insert(offerId);
  }

  return None();
}


// Validates that all offers belongs to the expected framework.
Option<Error> validateFramework(
    const RepeatedPtrField<OfferID>& offerIds,
    Controller* controller,
    Framework* framework)
{
  foreach (const OfferID& offerId, offerIds) {
    Try<FrameworkID> offerFrameworkId = getFrameworkId(controller, offerId);
    if (offerFrameworkId.isError()) {
      return offerFrameworkId.error();
    }

    if (framework->id() != offerFrameworkId.get()) {
      return Error(
          "Offer " + stringify(offerId) +
          " has invalid framework " + stringify(offerFrameworkId.get()) +
          " while framework " + stringify(framework->id()) + " is expected");
    }
  }

  return None();
}


// Validate that all offers in one operation must be
// allocated to the same role.
Option<Error> validateAllocationRole(
    const RepeatedPtrField<OfferID>& offerIds,
    Controller* controller)
{
  Option<string> role;
  foreach (const OfferID& offerId, offerIds) {
    Offer* offer = getOffer(controller, offerId);
    if (offer == nullptr) {
      return Error("Offer " + stringify(offerId) + " is no longer valid");
    }

    CHECK(offer->has_allocation_info());

    string _role = offer->allocation_info().role();
    if (role.isNone()) {
      role = _role;
      continue;
    }

    if (_role != role.get()) {
      return Error(
          "Aggregated offers must be allocated to the same role. Offer " +
          stringify(offerId) + " uses role '" + _role + " but another"
          " is using role '" + role.get());
    }
  }

  return None();
}


// Validates that all offers belong to the same valid node.
Option<Error> validateNode(
    const RepeatedPtrField<OfferID>& offerIds,
    Controller* controller)
{
  Option<NodeID> nodeId;

  foreach (const OfferID& offerId, offerIds) {
    Try<NodeID> offerNodeId = getNodeId(controller, offerId);
    if (offerNodeId.isError()) {
      return offerNodeId.error();
    }

    Node* node = getNode(controller, offerNodeId.get());

    // This is not possible because the offer should've been removed.
    CHECK(node != nullptr)
      << "Offer " << offerId
      << " outlived node " << offerNodeId.get();

    // This is not possible because the offer should've been removed.
    CHECK(node->connected)
      << "Offer " << offerId
      << " outlived disconnected node " << *node;

    if (nodeId.isNone()) {
      // Set node id and use as base case for validation.
      nodeId = node->id;
    }

    if (node->id != nodeId.get()) {
      return Error(
          "Aggregated offers must belong to one single node. Offer " +
          stringify(offerId) + " uses node " +
          stringify(node->id) + " and node " +
          stringify(nodeId.get()));
    }
  }

  return None();
}


Option<Error> validate(
    const RepeatedPtrField<OfferID>& offerIds,
    Controller* controller,
    Framework* framework)
{
  CHECK_NOTNULL(controller);
  CHECK_NOTNULL(framework);

  vector<lambda::function<Option<Error>()>> validators = {
    lambda::bind(validateUniqueOfferID, offerIds),
    lambda::bind(validateOfferIds, offerIds, controller),
    lambda::bind(validateFramework, offerIds, controller, framework),
    lambda::bind(validateAllocationRole, offerIds, controller),
    lambda::bind(validateNode, offerIds, controller)
  };

  foreach (const lambda::function<Option<Error>()>& validator, validators) {
    Option<Error> error = validator();
    if (error.isSome()) {
      return error;
    }
  }

  return None();
}


Option<Error> validateInverseOffers(
    const RepeatedPtrField<OfferID>& offerIds,
    Controller* controller,
    Framework* framework)
{
  CHECK_NOTNULL(controller);
  CHECK_NOTNULL(framework);

  vector<lambda::function<Option<Error>()>> validators = {
    lambda::bind(validateUniqueOfferID, offerIds),
    lambda::bind(validateInverseOfferIds, offerIds, controller),
    lambda::bind(validateFramework, offerIds, controller, framework),
    lambda::bind(validateNode, offerIds, controller)
  };

  foreach (const lambda::function<Option<Error>()>& validator, validators) {
    Option<Error> error = validator();
    if (error.isSome()) {
      return error;
    }
  }

  return None();
}

} // namespace offer {


namespace operation {

// TODO(jieyu): Validate that resources in an operation is not empty.


Option<Error> validate(
    const Offer::Operation::Reserve& reserve,
    const Option<Principal>& principal,
    const protobuf::node::Capabilities& nodeCapabilities,
    const Option<FrameworkInfo>& frameworkInfo)
{
  // NOTE: this ensures the reservation is not being made to the "*" role.
  Option<Error> error = resource::validate(reserve.resources());
  if (error.isSome()) {
    return Error("Invalid resources: " + error->message);
  }

  error = resource::validate(reserve.source());
  if (error.isSome()) {
    return Error("Invalid source: " + error->message);
  }

  if (reserve.source_size() > 0) {
    Resources source = reserve.source();
    Resources target = reserve.resources();

    auto validateReservationResources =
      [](const RepeatedPtrField<Resource>& resources) -> Option<Error> {
      // Resources should not be empty.
      if (resources.empty()) {
        return Error("Resource cannot be empty");
      }

      // All passed resources should have identical reservations.
      const RepeatedPtrField<Resource::ReservationInfo>& reservations =
        resources.begin()->reservations();

      if (!std::all_of(
              resources.begin(),
              resources.end(),
              [&](const Resource& resource) {
                return resource.reservations_size() == reservations.size() &&
                       std::equal(
                           resource.reservations().begin(),
                           resource.reservations().end(),
                           reservations.begin());
              })) {
        return Error(
            "Mixed reservations are not supported" + stringify(resources));
      }

      return None();
    };

    error = validateReservationResources(source);
    if (error.isSome()) {
      return Error("Invalid source: " + error->message);
    }

    error = validateReservationResources(target);
    if (error.isSome()) {
      return Error("Invalid resources: " + error->message);
    }

    // Both operands should refer to identical resource quantities.
    if (source.toUnreserved() != target.toUnreserved()) {
      return Error(
          "Source and target resources should refer to identical resource "
          "quantities: " +
          stringify(source.toUnreserved()) + " vs. " +
          stringify(target.toUnreserved()));
    }
  }

  error =
    resource::internal::validateSingleResourceProvider(reserve.resources());
  if (error.isSome()) {
    return Error("Invalid resources: " + error->message);
  }

  Option<hashset<string>> frameworkRoles;
  if (frameworkInfo.isSome()) {
    frameworkRoles = protobuf::framework::getRoles(frameworkInfo.get());
  }

  foreach (const Resource& resource, reserve.resources()) {
    if (!Resources::isDynamicallyReserved(resource)) {
      return Error(
          "Resource " + stringify(resource) + " is not dynamically reserved");
    }

    if (!nodeCapabilities.hierarchicalRole &&
        strings::contains(Resources::reservationRole(resource), "/")) {
      return Error(
          "Resource " + stringify(resource) +
          " with reservation for hierarchical role"
          " '" + Resources::reservationRole(resource) + "'"
          " cannot be reserved on a node without HIERARCHICAL_ROLE"
          " capability");
    }

    if (!nodeCapabilities.reservationRefinement &&
        Resources::hasRefinedReservations(resource)) {
      return Error(
          "Resource " + stringify(resource) +
          " with reservation refinement cannot be reserved on"
          " a node without RESERVATION_REFINEMENT capability");
    }

    if (principal.isSome()) {
      // We assume that `principal->value.isSome()` is true. The controller's HTTP
      // handlers enforce this constraint, and V0 authenticators will only
      // return principals of that form.
      CHECK_SOME(principal->value);

      const Resource::ReservationInfo& reservation =
        *resource.reservations().rbegin();

      if (!reservation.has_principal()) {
        return Error(
            "A reserve operation was attempted by principal '" +
            stringify(principal.get()) + "', but there is a "
            "reserved resource in the request with no principal set");
      }

      if (principal != reservation.principal()) {
        return Error(
            "A reserve operation was attempted by authenticated principal '" +
            stringify(principal.get()) + "', which does not match a "
            "reserved resource in the request with principal '" +
            reservation.principal() + "'");
      }
    }

    if (frameworkRoles.isSome()) {
      // If information on the framework was passed we are dealing
      // with a request over the framework API. In this case we expect
      // that the reserved resources where allocated to the role, and
      // that the allocation role and reservation role match the role
      // of the framework.
      if (!resource.allocation_info().has_role()) {
        return Error(
            "A reserve operation was attempted on unallocated resource"
            " " + stringify(resource) + ", but frameworks can only"
            " perform reservations on allocated resources");
      }

      if (resource.allocation_info().role() !=
          Resources::reservationRole(resource)) {
        return Error(
            "A reserve operation was attempted for a resource with role"
            " '" + Resources::reservationRole(resource) + "', but"
            " the resource was allocated to role"
            " '" + resource.allocation_info().role() + "'");
      }

      if (!frameworkRoles->contains(resource.allocation_info().role())) {
        return Error(
            "A reserve operation was attempted for a resource allocated"
            " to role '" + resource.allocation_info().role() + "',"
            " but the framework only has roles"
            " '" + stringify(frameworkRoles.get()) + "'");
      }

      if (!frameworkRoles->contains(Resources::reservationRole(resource))) {
        return Error(
            "A reserve operation was attempted for a resource with role"
            " '" + Resources::reservationRole(resource) + "',"
            " but the framework can only reserve resources with roles"
            " '" + stringify(frameworkRoles.get()) + "'");
      }
    } else {
      // If no `FrameworkInfo` was passed we are dealing with a
      // request via the operator HTTP API. In this case we expect
      // that the reserved resources have no `AllocationInfo` set
      // because operators can only reserve from the unallocated
      // resources.
      if (resource.has_allocation_info()) {
        return Error(
            "A reserve operation was attempted with an allocated resource,"
            " but the operator API only allows reservations to be made to"
            " unallocated resources");
      }
    }
  }

  if (frameworkInfo.isSome()) {
    // If the operation is being applied by a framework, we also
    // ensure that across all the resources, they are allocated
    // to a single role.
    error = resource::validateAllocatedToSingleRole(reserve.resources());
    if (error.isSome()) {
      return Error("Invalid reservation resources: " + error->message);
    }
  }

  return None();
}


Option<Error> validate(
    const Offer::Operation::Unreserve& unreserve,
    const Option<FrameworkInfo>& frameworkInfo)
{
  Option<Error> error = resource::validate(unreserve.resources());
  if (error.isSome()) {
    return Error("Invalid resources: " + error->message);
  }

  error =
    resource::internal::validateSingleResourceProvider(unreserve.resources());
  if (error.isSome()) {
    return Error("Invalid resources: " + error->message);
  }

  // NOTE: We don't check that 'FrameworkInfo.principal' matches
  // 'Resource.ReservationInfo.principal' here because the authorization
  // depends on the "unreserve" ACL which specifies which 'principal' can
  // unreserve which 'principal's resources. In the absence of an ACL, we allow
  // any 'principal' to unreserve any other 'principal's resources.

  foreach (const Resource& resource, unreserve.resources()) {
    if (!Resources::isDynamicallyReserved(resource)) {
      return Error(
          "Resource " + stringify(resource) + " is not dynamically reserved");
    }

    if (Resources::isPersistentVolume(resource)) {
      return Error(
          "A dynamically reserved persistent volume " +
          stringify(resource) +
          " cannot be unreserved directly. Please destroy the persistent"
          " volume first then unreserve the resource");
    }
  }

  return None();
}


Option<Error> validate(
    const Offer::Operation::Create& create,
    const Resources& checkpointedResources,
    const Option<Principal>& principal,
    const protobuf::node::Capabilities& nodeCapabilities,
    const Option<FrameworkInfo>& frameworkInfo)
{
  Option<Error> error = resource::validate(create.volumes());
  if (error.isSome()) {
    return Error("Invalid resources: " + error->message);
  }

  error = resource::internal::validateSingleResourceProvider(create.volumes());
  if (error.isSome()) {
    return Error("Invalid resources: " + error->message);
  }

  error = resource::validatePersistentVolume(create.volumes());
  if (error.isSome()) {
    return Error("Not a persistent volume: " + error->message);
  }

  error = resource::validateUniquePersistenceID(
      checkpointedResources + create.volumes());

  if (error.isSome()) {
    return error;
  }

  foreach (const Resource& volume, create.volumes()) {
    // If the volume being created is a shared persistent volume, we
    // allow it only if the framework has opted in for SHARED_RESOURCES.
    if (frameworkInfo.isSome() &&
        volume.has_shared() &&
        !protobuf::frameworkHasCapability(
            frameworkInfo.get(),
            FrameworkInfo::Capability::SHARED_RESOURCES)) {
      return Error(
          "Create volume operation for '" + stringify(volume) +
          "' has been attempted by framework '" +
          stringify(frameworkInfo->id()) +
          "' with no SHARED_RESOURCES capability");
    }

    if (!nodeCapabilities.hierarchicalRole &&
        strings::contains(Resources::reservationRole(volume), "/")) {
      return Error(
          "Volume " + stringify(volume) +
          " with reservation for hierarchical role"
          " '" + Resources::reservationRole(volume) + "'"
          " cannot be created on a node without HIERARCHICAL_ROLE"
          " capability");
    }

    if (!nodeCapabilities.reservationRefinement &&
        Resources::hasRefinedReservations(volume)) {
      return Error(
          "Volume " + stringify(volume) +
          " with reservation refinement cannot be created on"
          " a node without RESERVATION_REFINEMENT capability");
    }

    // Ensure that the provided principals match. If `principal` is `None`,
    // we allow `volume.disk.persistence.principal` to take any value.
    if (principal.isSome()) {
      // We assume that `principal->value.isSome()` is true. The controller's HTTP
      // handlers enforce this constraint, and V0 authenticators will only
      // return principals of that form.
      CHECK_SOME(principal->value);

      if (!volume.disk().persistence().has_principal()) {
        return Error(
            "Create volume operation attempted by principal '" +
            stringify(principal.get()) + "', but there is a volume in the "
            "operation with no principal set in 'disk.persistence'");
      }

      if (principal != volume.disk().persistence().principal()) {
        return Error(
            "Create volume operation attempted by authenticated principal '" +
            stringify(principal.get()) + "', which does not match "
            "a volume in the operation with principal '" +
            volume.disk().persistence().principal() +
            "' set in 'disk.persistence'");
      }
    }
  }

  if (frameworkInfo.isSome()) {
    // If the operation is being applied by a framework, we also
    // ensure that across all the resources, they are allocated
    // to a single role.
    error = resource::validateAllocatedToSingleRole(create.volumes());
    if (error.isSome()) {
      return Error("Invalid volume resources: " + error->message);
    }
  }

  return None();
}


Option<Error> validate(
    const Offer::Operation::Destroy& destroy,
    const Resources& checkpointedResources,
    const hashmap<FrameworkID, Resources>& usedResources,
    const Option<FrameworkInfo>& frameworkInfo)
{
  // The operation can either contain allocated resources
  // (in the case of a framework accepting offers), or
  // unallocated resources (in the case of the operator
  // endpoints). To ensure we can check for the presence
  // of the volume in the resources in use by tasks and
  // executors, we unallocate both the volume and the
  // used resources before performing the contains check.
  //
  // TODO(bmahler): This lambda is copied in several places
  // in the code, consider how best to pull this out.
  auto unallocated = [](const Resources& resources) {
    Resources result = resources;
    result.unallocate();
    return result;
  };

  Resources volumes = unallocated(destroy.volumes());

  Option<Error> error = resource::validate(volumes);
  if (error.isSome()) {
    return Error("Invalid resources: " + error->message);
  }

  error = resource::internal::validateSingleResourceProvider(volumes);
  if (error.isSome()) {
    return Error("Invalid resources: " + error->message);
  }

  error = resource::validatePersistentVolume(volumes);
  if (error.isSome()) {
    return Error("Not a persistent volume: " + error->message);
  }

  foreach (const Resource& volume, volumes) {
    if (Resources::hasResourceProvider(volume)) {
      continue;
    }

    if (!checkpointedResources.contains(volume)) {
      return Error("Persistent volumes not found");
    }
  }

  // Ensure the volumes being destroyed are not in use currently.
  // This check is mainly to validate destruction of shared volumes since
  // it is not possible for a non-shared resource to appear in an offer
  // if it is already in use.
  foreachvalue (const Resources& resources, usedResources) {
    foreach (const Resource& volume, volumes) {
      if (unallocated(resources).contains(volume)) {
        return Error("Persistent volumes in use");
      }
    }
  }

  return None();
}


Option<Error> validate(
    const Offer::Operation::GrowVolume& growVolume,
    const protobuf::node::Capabilities& nodeCapabilities)
{
  Option<Error> error = Resources::validate(growVolume.volume());
  if (error.isSome()) {
    return Error(
        "Invalid resource in the 'GrowVolume.volume' field: " +
        error->message);
  }

  error = Resources::validate(growVolume.addition());
  if (error.isSome()) {
    return Error(
        "Invalid resource in the 'GrowVolume.addition' field: " +
        error->message);
  }

  Value::Scalar zero;
  zero.set_value(0);

  // The `Scalar` comparison contains a fixed-point conversion.
  if (growVolume.addition().scalar() <= zero) {
    return Error(
        "The size of 'GrowVolume.addition' field must be greater than zero");
  }

  if (Resources::hasResourceProvider(growVolume.volume())) {
    return Error("Growing a volume from a resource provider is not supported");
  }

  error = resource::validatePersistentVolume(Resources(growVolume.volume()));
  if (error.isSome()) {
    return Error(
        "Invalid persistent volume in the 'GrowVolume.volume' field: " +
        error->message);
  }

  if (growVolume.volume().has_shared()) {
    return Error("Growing a shared persistent volume is not supported");
  }

  // TODO(zhitao): Move this to a helper function
  // `Resources::stripPersistentVolume`.
  Resource stripped = growVolume.volume();

  if (stripped.disk().has_source()) {
    // PATH/MOUNT disk.
    stripped.mutable_disk()->clear_persistence();
    stripped.mutable_disk()->clear_volume();
  } else {
    // ROOT disk.
    stripped.clear_disk();
  }

  if ((Resources(stripped) + growVolume.addition()).size() != 1) {
    return Error(
        "Incompatible resources in the 'GrowVolume.volume' and "
        "'GrowVolume.addition' fields");
  }

  if (!nodeCapabilities.resizeVolume) {
    return Error(
        "Volume " + stringify(growVolume.volume()) +
        " cannot be grown on a node without RESIZE_VOLUME capability");
  }

  return None();
}


Option<Error> validate(
    const Offer::Operation::ShrinkVolume& shrinkVolume,
    const protobuf::node::Capabilities& nodeCapabilities)
{
  Option<Error> error = Resources::validate(shrinkVolume.volume());
  if (error.isSome()) {
    return Error(
        "Invalid resource in the 'ShrinkVolume.volume' field: " +
        error->message);
  }

  Value::Scalar zero;
  zero.set_value(0);

  // The `Scalar` comparison contains a fixed-point conversion.
  if (shrinkVolume.subtract() <= zero) {
    return Error(
        "Value of 'ShrinkVolume.subtract' must be greater than zero");
  }

  if (shrinkVolume.volume().scalar() <= shrinkVolume.subtract()) {
    return Error(
        "Value of 'ShrinkVolume.subtract' must be smaller than the size "
        "of 'ShrinkVolume.volume'");
  }

  if (Resources::hasResourceProvider(shrinkVolume.volume())) {
    return Error(
        "Shrinking a volume from a resource provider is not supported");
  }

  if (shrinkVolume.volume().disk().source().type() ==
      Resource::DiskInfo::Source::MOUNT) {
    return Error(
        "Shrinking a volume on a MOUNT disk is not supported");
  }

  error = resource::validatePersistentVolume(Resources(shrinkVolume.volume()));
  if (error.isSome()) {
    return Error(
        "Invalid persistent volume in the 'ShrinkVolume.volume' field: " +
        error->message);
  }

  if (shrinkVolume.volume().has_shared()) {
    return Error("Shrinking a shared persistent volume is not supported");
  }

  if (!nodeCapabilities.resizeVolume) {
    return Error(
        "Volume " + stringify(shrinkVolume.volume()) +
        " cannot be shrunk on a node without RESIZE_VOLUME capability");
  }

  return None();
}


Option<Error> validate(const Offer::Operation::CreateDisk& createDisk)
{
  const Resource& source = createDisk.source();

  Option<Error> error = resource::validate(Resources(source));
  if (error.isSome()) {
    return Error("Invalid resource: " + error->message);
  }

  if (!Resources::hasResourceProvider(source)) {
    return Error("'source' is not managed by a resource provider");
  }

  if (!Resources::isDisk(source, Resource::DiskInfo::Source::RAW)) {
    return Error("'source' is not a RAW disk resource");
  }

  if (createDisk.target_type() != Resource::DiskInfo::Source::MOUNT &&
      createDisk.target_type() != Resource::DiskInfo::Source::BLOCK) {
    return Error("'target_type' is neither MOUNT or BLOCK");
  }

  if (source.disk().source().has_profile() == createDisk.has_target_profile()) {
    return createDisk.has_target_profile()
      ? Error("'target_profile' must not be set when 'source' has a profile")
      : Error("'target_profile' must be set when 'source' has no profile");
  }

  return None();
}


Option<Error> validate(const Offer::Operation::DestroyDisk& destroyDisk)
{
  const Resource& source = destroyDisk.source();

  Option<Error> error = resource::validate(Resources(source));
  if (error.isSome()) {
    return Error("Invalid resource: " + error->message);
  }

  if (!Resources::hasResourceProvider(source)) {
    return Error("'source' is not managed by a resource provider");
  }

  if (!Resources::isDisk(source, Resource::DiskInfo::Source::MOUNT) &&
      !Resources::isDisk(source, Resource::DiskInfo::Source::BLOCK) &&
      !Resources::isDisk(source, Resource::DiskInfo::Source::RAW)) {
    return Error("'source' is neither a MOUNT, BLOCK or RAW disk resource");
  }

  if (!source.disk().source().has_id()) {
    return Error("'source' is not backed by a CSI volume");
  }

  if (Resources::isPersistentVolume(source)) {
    return Error(
        "A disk resource containing a persistent volume " + stringify(source) +
        " cannot be destroyed directly. Please destroy the persistent volume"
        " first then destroy the disk resource");
  }

  return None();
}

} // namespace operation {

} // namespace validation {
} // namespace controller {
} // namespace internal {
} // namespace medios {
