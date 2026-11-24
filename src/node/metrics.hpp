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

#ifndef __NODE_METRICS_HPP__
#define __NODE_METRICS_HPP__

#include <vector>

#include <process/metrics/counter.hpp>
#include <process/metrics/pull_gauge.hpp>


namespace medios {
namespace internal {
namespace node {

class Node;

struct Metrics
{
  explicit Metrics(const Node& node);

  ~Metrics();

  void setRecoveryTime(const Duration& duration);

  process::metrics::PullGauge uptime_secs;
  process::metrics::PullGauge registered;

  process::metrics::Counter recovery_errors;
  Option<process::metrics::PullGauge> recovery_time_secs;

  process::metrics::PullGauge frameworks_active;

  process::metrics::PullGauge tasks_staging;
  process::metrics::PullGauge tasks_starting;
  process::metrics::PullGauge tasks_running;
  process::metrics::PullGauge tasks_killing;
  process::metrics::Counter tasks_finished;
  process::metrics::Counter tasks_failed;
  process::metrics::Counter tasks_killed;
  process::metrics::Counter tasks_lost;
  process::metrics::Counter tasks_gone;

  process::metrics::PullGauge executors_registering;
  process::metrics::PullGauge executors_running;
  process::metrics::PullGauge executors_terminating;
  process::metrics::Counter executors_terminated;
  process::metrics::Counter executors_preempted;

  process::metrics::Counter valid_status_updates;
  process::metrics::Counter invalid_status_updates;

  // Status updates refused because the executor that sent them does not own
  // the task they name. An executor may report only on its own tasks and the
  // node holds the only executor-to-task map, so this counts a defective or
  // hostile executor rather than anything about the task.
  process::metrics::Counter unowned_task_status_updates;

  // Progress reports stripped from a status update because they failed
  // validation. The update itself is delivered without the field, so this
  // counts a defect in the reporting executor rather than anything about
  // the task. Recovery of a checkpoint written before the node validated
  // reports logs the same event without counting it.
  process::metrics::Counter invalid_task_progress_reports;

  // Finish outcomes dropped from status updates because they were
  // malformed (DR-042).
  process::metrics::Counter invalid_task_finish_outcomes;

  process::metrics::Counter valid_framework_messages;
  process::metrics::Counter invalid_framework_messages;

  process::metrics::PullGauge executor_directory_max_allowed_age_secs;

  process::metrics::Counter container_launch_errors;

  // Non-revocable resources.
  std::vector<process::metrics::PullGauge> resources_total;
  std::vector<process::metrics::PullGauge> resources_used;
  std::vector<process::metrics::PullGauge> resources_percent;

  // Revocable resources.
  std::vector<process::metrics::PullGauge> resources_revocable_total;
  std::vector<process::metrics::PullGauge> resources_revocable_used;
  std::vector<process::metrics::PullGauge> resources_revocable_percent;
};

} // namespace node {
} // namespace internal {
} // namespace medios {

#endif // __NODE_METRICS_HPP__
