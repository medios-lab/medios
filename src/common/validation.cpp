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

#include "common/validation.hpp"

#include <limits.h>

#include <algorithm>
#include <cctype>
#include <cmath>

#include <medios/executor/executor.hpp>

#include <medios/resources.hpp>

#include <stout/foreach.hpp>
#include <stout/stringify.hpp>
#include <stout/unreachable.hpp>

#include <stout/os/constants.hpp>

#include "common/protobuf_utils.hpp"

using std::string;

using google::protobuf::RepeatedPtrField;

namespace medios {
namespace internal {
namespace common {
namespace validation {

Option<Error> validateID(const string& id)
{
  if (id.empty()) {
    return Error("ID must not be empty");
  }

  if (id.length() > NAME_MAX) {
    return Error(
        "ID must not be greater than " +
        stringify(NAME_MAX) + " characters");
  }

  // The ID cannot be exactly these special path components.
  if (id == "." || id == "..") {
    return Error("'" + id + "' is disallowed");
  }

  // Rules on invalid characters in the ID:
  // - Control characters are obviously not allowed.
  // - Slashes are disallowed as IDs are likely mapped to directories in Medios.
  auto invalidCharacter = [](char c) {
    return iscntrl(c) ||
           c == os::POSIX_PATH_SEPARATOR;
  };

  if (std::any_of(id.begin(), id.end(), invalidCharacter)) {
    return Error("'" + id + "' contains invalid characters");
  }

  return None();
}


// These IDs are valid as long as they meet the common ID requirements
// enforced by `validateID()` but we define each of them separately to
// be clear which IDs are subject to which rules.
Option<Error> validateTaskID(const TaskID& taskId)
{
  return validateID(taskId.value());
}


Option<Error> validateExecutorID(const ExecutorID& executorId)
{
  return validateID(executorId.value());
}


Option<Error> validateNodeID(const NodeID& nodeId)
{
  return validateID(nodeId.value());
}


Option<Error> validateFrameworkID(const FrameworkID& frameworkId)
{
  return validateID(frameworkId.value());
}


Option<Error> validateSecret(const Secret& secret)
{
  switch (secret.type()) {
    case Secret::REFERENCE:
      if (!secret.has_reference()) {
        return Error(
            "Secret of type REFERENCE must have the 'reference' field set");
      }

      if (secret.has_value()) {
        return Error(
            "Secret '" + secret.reference().name() + "' of type REFERENCE "
            "must not have the 'value' field set");
      }
      break;

    case Secret::VALUE:
      if (!secret.has_value()) {
        return Error("Secret of type VALUE must have the 'value' field set");
      }

      if (secret.has_reference()) {
        return Error(
            "Secret of type VALUE must not have the 'reference' field set");
      }
      break;

    case Secret::UNKNOWN:
      break;

    UNREACHABLE();
  }

  return None();
}


Option<Error> validateEnvironment(const Environment& environment)
{
  foreach (const Environment::Variable& variable, environment.variables()) {
    switch (variable.type()) {
      case Environment::Variable::SECRET: {
        if (!variable.has_secret()) {
          return Error(
              "Environment variable '" + variable.name() +
              "' of type 'SECRET' must have a secret set");
        }

        if (variable.has_value()) {
          return Error(
              "Environment variable '" + variable.name() +
              "' of type 'SECRET' must not have a value set");
        }

        Option<Error> error = validateSecret(variable.secret());
        if (error.isSome()) {
          return Error(
              "Environment variable '" + variable.name() + "' specifies an "
              "invalid secret: " + error->message);
        }

        if (variable.secret().value().data().find('\0') != string::npos) {
            return Error(
                "Environment variable '" + variable.name() + "' specifies a "
                "secret containing null bytes, which is not allowed in the "
                "environment");
        }

        // Nothing in this build resolves a secret into a task's environment
        // (the environment secret isolator is not shipped, #43); the node
        // would start the task without the variable.
        return Error(
            "Environment variable '" + variable.name() + "' is of type "
            "'SECRET', which this build cannot deliver: no isolator resolves "
            "secrets into a task's environment");
      }

      // NOTE: If new variable types are added in the future and an upgraded
      // client/controller sends a new type to an older controller/node, the older
      // controller/node will see VALUE instead of the new type, since VALUE is set
      // as the default type in the protobuf definition.
      case Environment::Variable::VALUE:
        if (!variable.has_value()) {
          return Error(
              "Environment variable '" + variable.name() +
              "' of type 'VALUE' must have a value set");
        }

        if (variable.has_secret()) {
          return Error(
              "Environment variable '" + variable.name() +
              "' of type 'VALUE' must not have a secret set");
        }
        break;

      case Environment::Variable::UNKNOWN:
          return Error("Environment variable of type 'UNKNOWN' is not allowed");

      UNREACHABLE();
    }
  }

  return None();
}


// TODO(greggomann): Do more than just validate the `Environment`.
Option<Error> validateCommandInfo(const CommandInfo& command)
{
  return validateEnvironment(command.environment());
}


Option<Error> validateVolume(const Volume& volume)
{
  // TODO(jieyu): Add a validation for path.

  // Only one of the following fields can be set:
  //   1. host_path
  //   2. image
  //   3. source
  int count = 0;
  if (volume.has_host_path()) { count++; }
  if (volume.has_image()) { count++; }
  if (volume.has_source()) { count++; }

  if (count != 1) {
    return Error(
        "Only one of them should be set: "
        "'host_path', 'image' and 'source'");
  }

  if (volume.has_source()) {
    switch (volume.source().type()) {
      case Volume::Source::DOCKER_VOLUME:
        if (!volume.source().has_docker_volume()) {
          return Error(
              "'source.docker_volume' is not set for DOCKER_VOLUME volume");
        }
        break;
      case Volume::Source::HOST_PATH:
        if (!volume.source().has_host_path()) {
          return Error(
              "'source.host_path' is not set for HOST_PATH volume");
        }
        break;
      case Volume::Source::SANDBOX_PATH:
        if (!volume.source().has_sandbox_path()) {
          return Error(
              "'source.sandbox_path' is not set for SANDBOX_PATH volume");
        }
        break;
      case Volume::Source::SECRET:
        if (!volume.source().has_secret()) {
          return Error(
              "'source.secret' is not set for SECRET volume");
        }

        // Nothing in this build mounts a SECRET volume (the volume/secret
        // isolator is not shipped, #43).
        return Error(
            "Volume '" + volume.container_path() + "' is a SECRET volume, "
            "which this build cannot deliver: no isolator mounts secrets");
      case Volume::Source::STORE:
        if (!volume.source().has_store()) {
          return Error("'source.store' is not set for STORE volume");
        }

        if (volume.source().store().name().empty()) {
          return Error("'source.store.name' is empty for STORE volume");
        }
        break;
      case Volume::Source::CSI_VOLUME:
        if (!volume.source().has_csi_volume()) {
          return Error(
              "'source.csi_volume' is not set for CSI volume");
        }

        if (!volume.source().csi_volume().has_static_provisioning()) {
          return Error(
              "'source.csi_volume.static_provisioning' "
              "is not set for CSI volume");
        }
        break;
      default:
        return Error("'source.type' is unknown");
    }
  }

  return None();
}


Option<Error> validateContainerInfo(const ContainerInfo& containerInfo)
{
  Option<Error> unionError = protobuf::validateProtobufUnion(containerInfo);
  if (unionError.isSome()) {
    // TODO(josephw): There is not enough information in this function to
    // print an actionable warning. Readers of this warning will not know
    // which container (task or executor) has this problem. Printing
    // the entire ContainerInfo is the best we can do.
    LOG(WARNING)
      << "Invalid protobuf union detected in the given ContainerInfo ("
      << containerInfo.DebugString()
      << "): " << unionError.get();
  }

  foreach (const Volume& volume, containerInfo.volumes()) {
    Option<Error> error = validateVolume(volume);
    if (error.isSome()) {
      return Error("Invalid volume: " + error->message);
    }
  }

  if (containerInfo.type() == ContainerInfo::DOCKER) {
    if (!containerInfo.has_docker()) {
      return Error(
          "DockerInfo 'docker' is not set for DOCKER typed ContainerInfo");
    }

    // We do not support setting `name` parameter in Docker info because
    // Docker containerizer has its own way to name the Docker container,
    // otherwise Docker containerizer will not be able to recognize the
    // created container, see MESOS-8497 for details.
    foreach (const Parameter& parameter,
             containerInfo.docker().parameters()) {
      if (parameter.key() == "name") {
        return Error("Parameter in DockerInfo must not be 'name'");
      }
    }
  }

  return None();
}


Option<Error> validateHealthCheck(const HealthCheck& healthCheck)
{
  if (!healthCheck.has_type()) {
    return Error("HealthCheck must specify 'type'");
  }

  switch (healthCheck.type()) {
    case HealthCheck::COMMAND: {
      if (!healthCheck.has_command()) {
        return Error("Expecting 'command' to be set for COMMAND health check");
      }

      const CommandInfo& command = healthCheck.command();

      if (!command.has_value()) {
        string commandType =
          (command.shell() ? "'shell command'" : "'executable path'");

        return Error("Command health check must contain " + commandType);
      }

      Option<Error> error =
        common::validation::validateCommandInfo(command);
      if (error.isSome()) {
        return Error(
            "Health check's `CommandInfo` is invalid: " + error->message);
      }

      break;
    }
    case HealthCheck::HTTP: {
      if (!healthCheck.has_http()) {
        return Error("Expecting 'http' to be set for HTTP health check");
      }

      const HealthCheck::HTTPCheckInfo& http = healthCheck.http();

      if (http.has_scheme() &&
          http.scheme() != "http" &&
          http.scheme() != "https") {
        return Error(
            "Unsupported HTTP health check scheme: '" + http.scheme() + "'");
      }

      if (http.has_path() && !strings::startsWith(http.path(), '/')) {
        return Error(
            "The path '" + http.path() +
            "' of HTTP health check must start with '/'");
      }

      break;
    }
    case HealthCheck::TCP: {
      if (!healthCheck.has_tcp()) {
        return Error("Expecting 'tcp' to be set for TCP health check");
      }

      break;
    }
    case HealthCheck::UNKNOWN: {
      return Error(
          "'" + HealthCheck::Type_Name(healthCheck.type()) + "'"
          " is not a valid health check type");
    }
  }

  if (healthCheck.has_delay_seconds() && healthCheck.delay_seconds() < 0.0) {
    return Error("Expecting 'delay_seconds' to be non-negative");
  }

  if (healthCheck.has_grace_period_seconds() &&
      healthCheck.grace_period_seconds() < 0.0) {
    return Error("Expecting 'grace_period_seconds' to be non-negative");
  }

  if (healthCheck.has_interval_seconds() &&
      healthCheck.interval_seconds() < 0.0) {
    return Error("Expecting 'interval_seconds' to be non-negative");
  }

  if (healthCheck.has_timeout_seconds() &&
      healthCheck.timeout_seconds() < 0.0) {
    return Error("Expecting 'timeout_seconds' to be non-negative");
  }

  return None();
}


Option<Error> validateCheckInfo(const CheckInfo& checkInfo)
{
  if (!checkInfo.has_type()) {
    return Error("CheckInfo must specify 'type'");
  }

  switch (checkInfo.type()) {
    case CheckInfo::COMMAND: {
      if (!checkInfo.has_command()) {
        return Error("Expecting 'command' to be set for COMMAND check");
      }

      const CommandInfo& command = checkInfo.command().command();

      if (!command.has_value()) {
        string commandType =
          (command.shell() ? "'shell command'" : "'executable path'");

        return Error("Command check must contain " + commandType);
      }

      Option<Error> error =
        common::validation::validateCommandInfo(command);
      if (error.isSome()) {
        return Error(
            "Check's `CommandInfo` is invalid: " + error->message);
      }

      break;
    }
    case CheckInfo::HTTP: {
      if (!checkInfo.has_http()) {
        return Error("Expecting 'http' to be set for HTTP check");
      }

      const CheckInfo::Http& http = checkInfo.http();

      if (http.has_path() && !strings::startsWith(http.path(), '/')) {
        return Error(
            "The path '" + http.path() + "' of HTTP check must start with '/'");
      }

      break;
    }
    case CheckInfo::TCP: {
      if (!checkInfo.has_tcp()) {
        return Error("Expecting 'tcp' to be set for TCP check");
      }

      break;
    }
    case CheckInfo::UNKNOWN: {
      return Error(
          "'" + CheckInfo::Type_Name(checkInfo.type()) + "'"
          " is not a valid check type");
    }
  }

  if (checkInfo.has_delay_seconds() && checkInfo.delay_seconds() < 0.0) {
    return Error("Expecting 'delay_seconds' to be non-negative");
  }

  if (checkInfo.has_interval_seconds() && checkInfo.interval_seconds() < 0.0) {
    return Error("Expecting 'interval_seconds' to be non-negative");
  }

  if (checkInfo.has_timeout_seconds() && checkInfo.timeout_seconds() < 0.0) {
    return Error("Expecting 'timeout_seconds' to be non-negative");
  }

  return None();
}


Option<Error> validateCheckStatusInfo(const CheckStatusInfo& checkStatusInfo)
{
  if (!checkStatusInfo.has_type()) {
    return Error("CheckStatusInfo must specify 'type'");
  }

  switch (checkStatusInfo.type()) {
    case CheckInfo::COMMAND: {
      if (!checkStatusInfo.has_command()) {
        return Error(
            "Expecting 'command' to be set for COMMAND check's status");
      }
      break;
    }
    case CheckInfo::HTTP: {
      if (!checkStatusInfo.has_http()) {
        return Error("Expecting 'http' to be set for HTTP check's status");
      }
      break;
    }
    case CheckInfo::TCP: {
      if (!checkStatusInfo.has_tcp()) {
        return Error("Expecting 'tcp' to be set for TCP check's status");
      }
      break;
    }
    case CheckInfo::UNKNOWN: {
      return Error(
          "'" + CheckInfo::Type_Name(checkStatusInfo.type()) + "'"
          " is not a valid check's status type");
    }
  }

  return None();
}

Option<Error> validateExecutorCall(const medios::executor::Call& call)
{
  if (!call.IsInitialized()) {
    return Error("Not initialized: " + call.InitializationErrorString());
  }

  if (!call.has_type()) {
    return Error("Expecting 'type' to be present");
  }

  // All calls should have executor id set.
  if (!call.has_executor_id()) {
    return Error("Expecting 'executor_id' to be present");
  }

  // All calls should have framework id set.
  if (!call.has_framework_id()) {
    return Error("Expecting 'framework_id' to be present");
  }

  switch (call.type()) {
    case medios::executor::Call::SUBSCRIBE: {
      if (!call.has_subscribe()) {
        return Error("Expecting 'subscribe' to be present");
      }
      return None();
    }

    case medios::executor::Call::UPDATE: {
      if (!call.has_update()) {
        return Error("Expecting 'update' to be present");
      }

      const TaskStatus& status = call.update().status();

      if (!status.has_uuid()) {
        return Error("Expecting 'uuid' to be present");
      }

      Try<id::UUID> uuid = id::UUID::fromBytes(status.uuid());
      if (uuid.isError()) {
        return uuid.error();
      }

      if (status.has_executor_id() &&
          status.executor_id().value()
          != call.executor_id().value()) {
        return Error("ExecutorID in Call: " +
                     call.executor_id().value() +
                     " does not match ExecutorID in TaskStatus: " +
                     call.update().status().executor_id().value()
                     );
      }

      if (status.source() != TaskStatus::SOURCE_EXECUTOR) {
        return Error("Received Call from executor " +
                     call.executor_id().value() +
                     " of framework " +
                     call.framework_id().value() +
                     " with invalid source, expecting 'SOURCE_EXECUTOR'"
                     );
      }

      if (status.state() == TASK_STAGING) {
        return Error("Received TASK_STAGING from executor " +
                     call.executor_id().value() +
                     " of framework " +
                     call.framework_id().value() +
                     " which is not allowed"
                     );
      }

      // TODO(alexr): Validate `check_status` is present if
      // the corresponding `TaskInfo.check` has been defined.

      if (status.has_check_status()) {
        Option<Error> validate =
          common::validation::validateCheckStatusInfo(status.check_status());

        if (validate.isSome()) {
          return validate.get();
        }
      }

      return None();
    }

    case medios::executor::Call::MESSAGE: {
      if (!call.has_message()) {
        return Error("Expecting 'message' to be present");
      }
      return None();
    }

    case medios::executor::Call::HEARTBEAT: {
      return None();
    }

    case medios::executor::Call::UNKNOWN: {
      return None();
    }
  }

  UNREACHABLE();
}


Option<Error> validateOfferFilters(const OfferFilters& offerFilters)
{
  if (offerFilters.has_min_allocatable_resources()) {
    foreach (
        const OfferFilters::ResourceQuantities& quantities,
        offerFilters.min_allocatable_resources().quantities()) {
      if (quantities.quantities().empty()) {
        return Error("Resource quantities must contain at least one quantity");
      }

      // Use `auto` instead of `protobuf::MapPair<string, Value::>` since
      // `foreach` is a macro and does not allow angle brackets.
      foreach (auto&& quantity, quantities.quantities()) {
        Option<Error> error = validateInputScalarValue(quantity.second.value());
        if (error.isSome()) {
          return Error(
              "Invalid resource quantity for '" + quantity.first + "': " +
              error->message);
        }
      }
    }
  }

  return None();
}

Option<Error> validateInputScalarValue(double value)
{
  switch (std::fpclassify(value)) {
    case FP_INFINITE:
      return Error("Infinite values not supported");
    case FP_NAN:
      return Error("NaN not supported");
    case FP_SUBNORMAL:
      return Error("Subnormal values not supported");
    case FP_ZERO:
      break;
    case FP_NORMAL:
      if (value < 0) {
        return Error("Negative values not supported");
      }
      break;
    default:
      return Error("Unknown error");
  }

  return None();
}


Option<Error> validateTaskProgress(const TaskStatus::Progress& progress)
{
  if (progress.has_fraction()) {
    const double fraction = progress.fraction();

    if (!std::isfinite(fraction)) {
      return Error("Task progress `fraction` is not a finite number");
    }

    if (fraction < 0.0 || fraction > 1.0) {
      return Error(
          "Task progress `fraction` " + stringify(fraction) +
          " is outside [0, 1]");
    }
  }

  // Not bounded against this process's clock. A skewed executor clock is a
  // framework's problem to notice, and refusing a report for being in the
  // future would turn a clock fault into a lost observation.
  if (progress.has_observed_at() && !std::isfinite(progress.observed_at())) {
    return Error("Task progress `observed_at` is not a finite number");
  }

  if (progress.message().size() > PROGRESS_MESSAGE_MAX_LENGTH) {
    return Error(
        "Task progress `message` is " + stringify(progress.message().size()) +
        " bytes; the limit is " + stringify(PROGRESS_MESSAGE_MAX_LENGTH));
  }

  const size_t serialized = progress.ByteSizeLong();
  if (serialized > PROGRESS_MAX_SERIALIZED_BYTES) {
    return Error(
        "Task progress serializes to " + stringify(serialized) +
        " bytes; the limit is " + stringify(PROGRESS_MAX_SERIALIZED_BYTES) +
        ". Fields this version does not recognize are counted, and are"
        " retained if the report is kept");
  }

  return None();
}


size_t sanitizeTaskFinish(Task* task)
{
  size_t cleared = 0;

  foreach (TaskStatus& status, *task->mutable_statuses()) {
    if (!status.has_finish()) {
      continue;
    }

    Option<Error> error = validateFinishStatus(status.finish());
    if (error.isSome()) {
      LOG(WARNING) << "Dropping the finish outcome from a status of task "
                   << task->task_id() << ": " << error->message;
      status.clear_finish();
      cleared++;
    }
  }

  return cleared;
}


Option<Error> validateFinishStatus(const FinishStatus& finish)
{
  if (!finish.has_state() || !FinishStatus::State_IsValid(finish.state())) {
    return Error("Task finish `state` is required and must be RAN,"
                 " TIMED_OUT, KILLED, DID_NOT_START or LOST");
  }

  if (finish.state() == FinishStatus::RAN &&
      !finish.has_exit_code() && !finish.has_signal()) {
    return Error("Task finish RAN carries neither `exit_code` nor `signal`");
  }

  if ((finish.state() == FinishStatus::DID_NOT_START ||
       finish.state() == FinishStatus::LOST) &&
      (!finish.has_reason() || finish.reason().empty())) {
    return Error(
        "Task finish " + FinishStatus::State_Name(finish.state()) +
        " carries no `reason`");
  }

  return None();
}


void sanitizeTaskProgress(Task* task)
{
  Option<TaskStatus::Progress> lastValid = None();

  foreach (TaskStatus& status, *task->mutable_statuses()) {
    if (status.has_progress() &&
        validateTaskProgress(status.progress()).isNone()) {
      lastValid = status.progress();
      continue;
    }

    status.clear_progress();

    if (lastValid.isSome()) {
      status.mutable_progress()->CopyFrom(lastValid.get());
    }
  }
}

} // namespace validation {
} // namespace common {
} // namespace internal {
} // namespace medios {
