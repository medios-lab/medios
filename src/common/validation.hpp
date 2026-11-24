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

#ifndef __COMMON_VALIDATION_HPP__
#define __COMMON_VALIDATION_HPP__

#include <cstddef>
#include <string>

#include <medios/medios.hpp>

#include <stout/error.hpp>
#include <stout/option.hpp>

namespace medios {

namespace executor {

class Call;

} // namespace executor {

namespace internal {
namespace common {
namespace validation {

// Validates if `id` meets the common requirements for IDs in Medios.
// Note that some IDs in Medios have additional/stricter requirements.
Option<Error> validateID(const std::string& id);

Option<Error> validateTaskID(const TaskID& taskId);

Option<Error> validateExecutorID(const ExecutorID& executorId);

Option<Error> validateNodeID(const NodeID& nodeId);

Option<Error> validateFrameworkID(const FrameworkID& frameworkId);

Option<Error> validateSecret(const Secret& secret);

Option<Error> validateEnvironment(const Environment& environment);

Option<Error> validateCommandInfo(const CommandInfo& command);

Option<Error> validateVolume(const Volume& volume);

Option<Error> validateContainerInfo(const ContainerInfo& containerInfo);

Option<Error> validateHealthCheck(const HealthCheck& healthCheck);

Option<Error> validateCheckInfo(const CheckInfo& checkInfo);

Option<Error> validateCheckStatusInfo(const CheckStatusInfo& checkStatusInfo);

Option<Error> validateExecutorCall(const medios::executor::Call& call);

Option<Error> validateOfferFilters(const OfferFilters& offerFilters);

Option<Error> validateInputScalarValue(double value);


// The longest `TaskStatus.Progress.message` accepted, in bytes.
//
// Parameter, default 4096, provisional. Both the node and the controller
// keep one retained status per task in memory, and the controller clears
// `TaskStatus.data` for exactly that reason, so the typed progress field
// must not recreate the same growth. The bound on the retained bytes is
// tasks times one status times this value. An executor's own report is
// expected to be far shorter, so a compliant executor never meets it.
// No measurement of memory under per-task progress exists yet.
constexpr size_t PROGRESS_MESSAGE_MAX_LENGTH = 4096;


// The largest serialized `TaskStatus.Progress` accepted, in bytes.
//
// Parameter, default 4352, provisional: `PROGRESS_MESSAGE_MAX_LENGTH` plus
// 256 bytes for the known fields and their tags. Two doubles at nine bytes,
// three varints at up to eleven, and the message's own tag and length prefix
// come to under sixty, so the allowance leaves room for fields added later
// without moving the message bound.
//
// Bounding the message alone would not bound the protobuf. Protobuf keeps
// fields it does not recognize, so a report can carry an empty `message` and
// a large unknown field, and every copy, checkpoint and carry-forward would
// keep those bytes. This is checked on the serialized size, which counts
// them.
constexpr size_t PROGRESS_MAX_SERIALIZED_BYTES = 4352;


// Validates a progress report from a task's executor: `fraction`, when set,
// is finite and in [0, 1]; `observed_at`, when set, is finite; `message` is
// within `PROGRESS_MESSAGE_MAX_LENGTH`; and the whole message serializes
// within `PROGRESS_MAX_SERIALIZED_BYTES`.
//
// A failing report is an observation error in the reporting executor, not a
// task failure. Callers drop the field, keep the last report that passed,
// and deliver the rest of the status.
Option<Error> validateTaskProgress(const TaskStatus::Progress& progress);


// Replaces every invalid `TaskStatus.progress` in `task`'s status list with
// the most recent report that passed validation, so a record arriving from
// another component cannot carry a value this process would have refused.
void sanitizeTaskProgress(Task* task);


// Clears every `TaskStatus.finish` in `task`'s status list that fails
// `validateFinishStatus`, so a record arriving from another component
// cannot carry an outcome this process would have refused. Cleared and
// not replaced: a finish outcome has no predecessor to fall back to.
// Returns how many were cleared, for the caller's counter.
size_t sanitizeTaskFinish(Task* task);


// Validates `TaskStatus.finish` (DR-042): `state` is required and one of
// the named values, since no value is zero and an unset state is never
// read as a hook that ran; `RAN` carries `exit_code` or `signal`;
// `DID_NOT_START` and `LOST` carry `reason`. Callers drop the field and deliver
// the rest of the status, as for progress.
Option<Error> validateFinishStatus(const FinishStatus& finish);

} // namespace validation {
} // namespace common {
} // namespace internal {
} // namespace medios {

#endif // __COMMON_VALIDATION_HPP__
