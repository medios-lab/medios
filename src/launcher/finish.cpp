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

#include "launcher/finish.hpp"

#include <sys/wait.h>

#include <algorithm>
#include <string>
#include <vector>

#include <glog/logging.h>

#include <stout/foreach.hpp>
#include <stout/hashmap.hpp>
#include <stout/stringify.hpp>

using std::string;
using std::vector;

namespace medios {
namespace internal {
namespace finish {

const string STDOUT_FILE = "finish.stdout";
const string STDERR_FILE = "finish.stderr";

const string WORK_EXIT_STATUS_VARIABLE = "MEDIOS_WORK_EXIT_STATUS";
const string TASK_ID_VARIABLE = "MEDIOS_TASK_ID";
const string FRAMEWORK_ID_VARIABLE = "MEDIOS_FRAMEWORK_ID";
const string NODE_ID_VARIABLE = "MEDIOS_NODE_ID";


int workExitStatus(int waitStatus)
{
  // A reaped child's status is one or the other, as the work command's
  // own path asserts; a number that is neither means nothing to a hook.
  CHECK(WIFEXITED(waitStatus) || WIFSIGNALED(waitStatus))
    << "Unexpected wait status " << waitStatus;

  if (WIFEXITED(waitStatus)) {
    return WEXITSTATUS(waitStatus);
  }

  return 128 + WTERMSIG(waitStatus);
}


static void set(
    Environment* environment,
    const string& name,
    const string& value)
{
  Environment::Variable* variable = environment->add_variables();
  variable->set_name(name);
  variable->set_type(Environment::Variable::VALUE);
  variable->set_value(value);
}


Environment hookEnvironment(
    const Environment& executorEnvironment,
    const Environment& commandEnvironment,
    int workWaitStatus,
    const TaskID& taskId,
    const FrameworkID& frameworkId,
    const string& nodeId)
{
  // One value per name, the later source winning: the executor's, then
  // the command's, then the four. The order of first appearance is kept
  // so the result reads as the sources did.
  vector<string> order;
  hashmap<string, string> values;

  auto put = [&order, &values](const string& name, const string& value) {
    if (!values.contains(name)) {
      order.push_back(name);
    }
    values[name] = value;
  };

  foreach (const Environment::Variable& variable,
           executorEnvironment.variables()) {
    put(variable.name(), variable.value());
  }

  foreach (const Environment::Variable& variable,
           commandEnvironment.variables()) {
    put(variable.name(), variable.value());
  }

  put(WORK_EXIT_STATUS_VARIABLE, stringify(workExitStatus(workWaitStatus)));
  put(TASK_ID_VARIABLE, taskId.value());
  put(FRAMEWORK_ID_VARIABLE, frameworkId.value());
  put(NODE_ID_VARIABLE, nodeId);

  Environment environment;
  foreach (const string& name, order) {
    set(&environment, name, values[name]);
  }

  return environment;
}


Option<Duration> limitFor(
    const Duration& cap,
    const Option<Duration>& ceilingRemaining,
    const Duration& gracePeriod)
{
  if (ceilingRemaining.isNone()) {
    return cap;
  }

  const Duration bound = ceilingRemaining.get() - gracePeriod;
  if (bound <= Duration::zero()) {
    return None();
  }

  return std::min(cap, bound);
}


static FinishStatus withBounds(
    FinishStatus status,
    const Duration& limit,
    const Duration& duration)
{
  status.mutable_limit()->set_nanoseconds(limit.ns());
  status.mutable_duration()->set_nanoseconds(duration.ns());
  return status;
}


FinishStatus ran(
    int waitStatus,
    const Duration& limit,
    const Duration& duration)
{
  CHECK(WIFEXITED(waitStatus) || WIFSIGNALED(waitStatus))
    << "Unexpected wait status " << waitStatus;

  FinishStatus status;
  status.set_state(FinishStatus::RAN);

  if (WIFEXITED(waitStatus)) {
    status.set_exit_code(WEXITSTATUS(waitStatus));
  } else {
    status.set_signal(WTERMSIG(waitStatus));
  }

  return withBounds(status, limit, duration);
}


FinishStatus timedOut(const Duration& limit, const Duration& duration)
{
  FinishStatus status;
  status.set_state(FinishStatus::TIMED_OUT);
  return withBounds(status, limit, duration);
}


FinishStatus killed(const Duration& limit, const Duration& duration)
{
  FinishStatus status;
  status.set_state(FinishStatus::KILLED);
  return withBounds(status, limit, duration);
}


FinishStatus didNotStart(const string& reason)
{
  FinishStatus status;
  status.set_state(FinishStatus::DID_NOT_START);
  status.set_reason(reason);
  return status;
}


FinishStatus lost(
    const string& reason,
    const Duration& limit,
    const Duration& duration)
{
  FinishStatus status;
  status.set_state(FinishStatus::LOST);
  status.set_reason(reason);
  return withBounds(status, limit, duration);
}

} // namespace finish {
} // namespace internal {
} // namespace medios {
