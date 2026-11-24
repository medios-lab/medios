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

// Typed task progress on `TaskStatus`, and the node's refusal of a
// status update for a task the sending executor does not own.
//
// Four layers, each proved on its own:
//   - validation: `fraction` finite and in [0, 1], `message` bounded;
//   - `protobuf::getTaskProgress`: reads the newest report the controller
//     kept for a task, including after the task went terminal;
//   - the controller: a value reported in TASK_RUNNING is retained on the
//     task record, reaches `/state`, and comes back on both explicit and
//     implicit reconciliation; an out-of-range fraction is dropped and
//     counted while the rest of the update is delivered;
//   - the node: an executor that reports on a task it does not own is
//     refused and counted rather than forwarded.

#include <limits>
#include <list>
#include <string>
#include <vector>

#include <gmock/gmock.h>

#include <gtest/gtest.h>

#include <medios/medios.hpp>
#include <medios/resources.hpp>

#include <process/clock.hpp>
#include <process/future.hpp>
#include <process/gmock.hpp>
#include <process/gtest.hpp>
#include <process/http.hpp>
#include <process/message.hpp>
#include <process/owned.hpp>
#include <process/pid.hpp>

#include <stout/foreach.hpp>
#include <stout/gtest.hpp>
#include <stout/hashmap.hpp>
#include <stout/json.hpp>
#include <stout/option.hpp>
#include <stout/stringify.hpp>
#include <stout/strings.hpp>
#include <stout/uuid.hpp>

#include <stout/os/find.hpp>
#include <stout/os/read.hpp>

#include "common/protobuf_utils.hpp"

#include "common/validation.hpp"

#include "node/node.hpp"
#include "node/paths.hpp"

#include "tests/cluster.hpp"
#include "tests/containerizer.hpp"
#include "tests/medios.hpp"
#include "tests/utils.hpp"

using medios::controller::detector::ControllerDetector;

using medios::internal::node::Node;

using process::Clock;
using process::Future;
using process::Message;
using process::Owned;
using process::Promise;
using process::UPID;

using process::http::OK;
using process::http::Response;

using std::string;
using std::vector;

using testing::_;
using testing::AtMost;
using testing::DoAll;
using testing::Eq;
using testing::Return;
using testing::SaveArg;

namespace medios {
namespace internal {
namespace tests {

// `tests` has a `common` of its own, so name the shared validator here.
namespace validation = medios::internal::common::validation;

using Progress = TaskStatus::Progress;


// An executor action that reports `TASK_RUNNING` for the launched task with
// `progress` attached, which is how a task's own executor reports its
// advance.
ACTION_P(SendRunningWithProgress, progress)
{
  TaskStatus status;
  status.mutable_task_id()->MergeFrom(arg1.task_id());
  status.set_state(TASK_RUNNING);
  status.mutable_progress()->CopyFrom(progress);
  arg0->sendStatusUpdate(status);
}


// A fixed observation time, well in the past, so a test can tell it apart
// from any timestamp the controller stamps on a reply.
constexpr double OBSERVED_AT = 1700000000.0;


static Progress progressAt(double fraction)
{
  Progress progress;
  progress.set_fraction(fraction);
  progress.set_step(3);
  progress.set_total(10);
  progress.set_message("shard 3 of 10");
  progress.set_sequence(7);
  progress.set_observed_at(OBSERVED_AT);
  return progress;
}


// A `Progress` whose `message` is empty and which still carries a large
// payload, as field 15: nothing in `Progress` claims that number, so
// parsing keeps the bytes as an unknown field and every copy keeps them.
static Progress oversizedProgress()
{
  const size_t size = 8192;

  string wire;
  wire += static_cast<char>((15 << 3) | 2);

  size_t remaining = size;
  while (remaining >= 0x80) {
    wire += static_cast<char>((remaining & 0x7F) | 0x80);
    remaining >>= 7;
  }
  wire += static_cast<char>(remaining);
  wire += string(size, 'x');

  Progress progress;
  CHECK(progress.ParseFromString(wire));
  return progress;
}


// --- Validation ---------------------------------------------------------

TEST(TaskProgressValidationTest, EmptyProgressIsValid)
{
  EXPECT_NONE(validation::validateTaskProgress(Progress()));
}


TEST(TaskProgressValidationTest, FractionAtTheEndsOfTheRangeIsValid)
{
  Progress progress;

  progress.set_fraction(0.0);
  EXPECT_NONE(validation::validateTaskProgress(progress));

  progress.set_fraction(1.0);
  EXPECT_NONE(validation::validateTaskProgress(progress));

  progress.set_fraction(0.5);
  EXPECT_NONE(validation::validateTaskProgress(progress));
}


TEST(TaskProgressValidationTest, FractionBelowZeroIsAnError)
{
  Progress progress;
  progress.set_fraction(-0.01);

  Option<Error> error = validation::validateTaskProgress(progress);

  ASSERT_SOME(error);
  EXPECT_TRUE(strings::contains(error->message, "[0, 1]")) << error->message;
}


TEST(TaskProgressValidationTest, FractionAboveOneIsAnError)
{
  Progress progress;
  progress.set_fraction(1.5);

  Option<Error> error = validation::validateTaskProgress(progress);

  ASSERT_SOME(error);
  EXPECT_TRUE(strings::contains(error->message, "[0, 1]")) << error->message;
}


TEST(TaskProgressValidationTest, NonFiniteFractionIsAnError)
{
  Progress progress;

  progress.set_fraction(std::numeric_limits<double>::quiet_NaN());
  Option<Error> error = validation::validateTaskProgress(progress);
  ASSERT_SOME(error);
  EXPECT_TRUE(strings::contains(error->message, "finite")) << error->message;

  progress.set_fraction(std::numeric_limits<double>::infinity());
  error = validation::validateTaskProgress(progress);
  ASSERT_SOME(error);
  EXPECT_TRUE(strings::contains(error->message, "finite")) << error->message;
}


TEST(TaskProgressValidationTest, NonFiniteObservedAtIsAnError)
{
  Progress progress;
  progress.set_observed_at(std::numeric_limits<double>::infinity());

  Option<Error> error = validation::validateTaskProgress(progress);

  ASSERT_SOME(error);
  EXPECT_TRUE(strings::contains(error->message, "observed_at"))
    << error->message;
}


// An observation time the controller's clock disagrees with is a framework's
// to notice. Refusing it would turn an executor clock fault into a lost
// observation, so only non-finite values are rejected.
TEST(TaskProgressValidationTest, ObservedAtInTheFutureIsValid)
{
  Progress progress;
  progress.set_observed_at(4000000000.0);

  EXPECT_NONE(validation::validateTaskProgress(progress));
}


TEST(TaskProgressValidationTest, MessageAtTheBoundIsValid)
{
  Progress progress;
  progress.set_message(
      string(validation::PROGRESS_MESSAGE_MAX_LENGTH, 'a'));

  EXPECT_NONE(validation::validateTaskProgress(progress));
}


TEST(TaskProgressValidationTest, OverLongMessageIsAnError)
{
  Progress progress;
  progress.set_message(
      string(validation::PROGRESS_MESSAGE_MAX_LENGTH + 1, 'a'));

  Option<Error> error = validation::validateTaskProgress(progress);

  ASSERT_SOME(error);
  EXPECT_TRUE(strings::contains(
      error->message,
      stringify(validation::PROGRESS_MESSAGE_MAX_LENGTH)))
    << error->message;
}


// Protobuf keeps fields it does not recognize, so a report can pass the
// message bound and still carry a large payload. Everything the retention
// path does, copying, checkpointing and carrying forward, keeps those bytes.
TEST(TaskProgressValidationTest, OversizedUnknownFieldIsAnError)
{
  Progress progress = oversizedProgress();

  // The field the message bound covers is empty.
  ASSERT_TRUE(progress.message().empty());
  ASSERT_GT(progress.ByteSizeLong(),
            validation::PROGRESS_MAX_SERIALIZED_BYTES);

  Option<Error> error = validation::validateTaskProgress(progress);

  ASSERT_SOME(error);
  EXPECT_TRUE(strings::contains(error->message, "serializes to"))
    << error->message;
}


// The bound leaves room for every known field at its largest.
TEST(TaskProgressValidationTest, EveryKnownFieldAtItsLargestIsValid)
{
  Progress progress;
  progress.set_fraction(1.0);
  progress.set_step(18446744073709551615u);
  progress.set_total(18446744073709551615u);
  progress.set_sequence(18446744073709551615u);
  progress.set_observed_at(1700000000.0);
  progress.set_message(
      string(validation::PROGRESS_MESSAGE_MAX_LENGTH, 'a'));

  EXPECT_NONE(validation::validateTaskProgress(progress));
}


// --- Sanitizing a record from another component ------------------------

// A task record arriving from a node has not passed through the status
// update path, so each status is checked and an invalid report is replaced
// by the last one that passed.
TEST(TaskProgressRecordTest, SanitizeKeepsThePrecedingValidReport)
{
  Task task;

  TaskStatus* starting = task.add_statuses();
  starting->set_state(TASK_STARTING);

  TaskStatus* running = task.add_statuses();
  running->set_state(TASK_RUNNING);
  running->mutable_progress()->CopyFrom(progressAt(0.3));

  TaskStatus* invalid = task.add_statuses();
  invalid->set_state(TASK_RUNNING);
  invalid->mutable_progress()->CopyFrom(progressAt(1.5));

  TaskStatus* omitting = task.add_statuses();
  omitting->set_state(TASK_FINISHED);

  validation::sanitizeTaskProgress(&task);

  // The status that reported nothing stays that way until a valid report.
  EXPECT_FALSE(task.statuses(0).has_progress());

  EXPECT_DOUBLE_EQ(0.3, task.statuses(1).progress().fraction());

  // The invalid report is replaced, not kept and not merely cleared.
  ASSERT_TRUE(task.statuses(2).has_progress());
  EXPECT_DOUBLE_EQ(0.3, task.statuses(2).progress().fraction());

  ASSERT_TRUE(task.statuses(3).has_progress());
  EXPECT_DOUBLE_EQ(0.3, task.statuses(3).progress().fraction());

  ASSERT_SOME(protobuf::getTaskProgress(task));
  EXPECT_DOUBLE_EQ(0.3, protobuf::getTaskProgress(task)->fraction());
}


// With no valid report anywhere on the record, nothing is invented.
TEST(TaskProgressRecordTest, SanitizeClearsWhenNothingValidPrecedes)
{
  Task task;

  TaskStatus* running = task.add_statuses();
  running->set_state(TASK_RUNNING);
  running->mutable_progress()->CopyFrom(progressAt(1.5));

  validation::sanitizeTaskProgress(&task);

  EXPECT_FALSE(task.statuses(0).has_progress());
  EXPECT_NONE(protobuf::getTaskProgress(task));
}


// --- getTaskProgress ----------------------------------------------------

TEST(TaskProgressRecordTest, NoStatusesMeansNoProgress)
{
  Task task;
  EXPECT_NONE(protobuf::getTaskProgress(task));
}


TEST(TaskProgressRecordTest, ReadsTheNewestReport)
{
  Task task;

  TaskStatus* running = task.add_statuses();
  running->set_state(TASK_RUNNING);
  running->mutable_progress()->CopyFrom(progressAt(0.3));

  EXPECT_SOME(protobuf::getTaskProgress(task));
  EXPECT_DOUBLE_EQ(0.3, protobuf::getTaskProgress(task)->fraction());

  running->mutable_progress()->CopyFrom(progressAt(0.6));

  EXPECT_DOUBLE_EQ(0.6, protobuf::getTaskProgress(task)->fraction());
}


// A terminal status need not repeat the task's progress. How far a task got
// before it failed or was killed for stalling is exactly what a framework
// reads afterwards, so the last report survives the terminal status.
TEST(TaskProgressRecordTest, SurvivesATerminalStatusThatOmitsIt)
{
  Task task;

  TaskStatus* running = task.add_statuses();
  running->set_state(TASK_RUNNING);
  running->mutable_progress()->CopyFrom(progressAt(0.4));

  TaskStatus* failed = task.add_statuses();
  failed->set_state(TASK_FAILED);
  failed->set_reason(TaskStatus::REASON_PROGRESS_STALLED);

  ASSERT_SOME(protobuf::getTaskProgress(task));
  EXPECT_DOUBLE_EQ(0.4, protobuf::getTaskProgress(task)->fraction());
  EXPECT_EQ("shard 3 of 10", protobuf::getTaskProgress(task)->message());
}


// --- The controller -----------------------------------------------------

class TaskProgressControllerTest : public MediosTest {};


// A value an executor reports in a TASK_RUNNING update reaches the
// framework, is retained on the controller's task record where `/state`
// renders it, and comes back on both forms of reconciliation. The
// reconciliation leg is what the invariant rests on: a framework that
// restarts and reconciles learns the task's last reported progress without
// the executor reporting again.
TEST_F(TaskProgressControllerTest, ReportedProgressIsRetainedAndReconciled)
{
  Try<Owned<cluster::Controller>> controller = StartController();
  ASSERT_SOME(controller);

  MockExecutor exec(DEFAULT_EXECUTOR_ID);
  TestContainerizer containerizer(&exec);

  Owned<ControllerDetector> detector = controller.get()->createDetector();
  Try<Owned<cluster::Node>> node = StartNode(detector.get(), &containerizer);
  ASSERT_SOME(node);

  MockScheduler sched;
  MediosSchedulerDriver driver(
      &sched,
      DEFAULT_FRAMEWORK_INFO,
      controller.get()->pid,
      DEFAULT_CREDENTIAL);

  EXPECT_CALL(sched, registered(&driver, _, _));

  Future<vector<Offer>> offers;
  EXPECT_CALL(sched, resourceOffers(&driver, _))
    .WillOnce(FutureArg<1>(&offers))
    .WillRepeatedly(Return());

  driver.start();

  AWAIT_READY(offers);
  ASSERT_FALSE(offers->empty());

  TaskInfo task;
  task.set_name("progress-test");
  task.set_principal(DEFAULT_TASK_PRINCIPAL);
  task.mutable_task_id()->set_value("1");
  task.mutable_node_id()->MergeFrom(offers->at(0).node_id());
  task.mutable_resources()->MergeFrom(offers->at(0).resources());
  task.mutable_executor()->MergeFrom(DEFAULT_EXECUTOR_INFO);

  EXPECT_CALL(exec, registered(_, _, _, _))
    .Times(AtMost(1));

  EXPECT_CALL(exec, launchTask(_, _))
    .WillOnce(SendRunningWithProgress(progressAt(0.3)));

  EXPECT_CALL(containerizer, update(_, _, _))
    .WillRepeatedly(Return(Nothing()));

  Future<TaskStatus> running;
  EXPECT_CALL(sched, statusUpdate(&driver, _))
    .WillOnce(FutureArg<1>(&running));

  driver.launchTasks(offers->at(0).id(), {task});

  AWAIT_READY(running);
  ASSERT_EQ(TASK_RUNNING, running->state()) << running->message();
  ASSERT_TRUE(running->has_progress());
  EXPECT_DOUBLE_EQ(0.3, running->progress().fraction());
  EXPECT_EQ(3u, running->progress().step());
  EXPECT_EQ(10u, running->progress().total());
  EXPECT_EQ("shard 3 of 10", running->progress().message());
  EXPECT_EQ(7u, running->progress().sequence());

  // The controller's own record of the task, which is what survives a
  // framework going away.
  Future<Response> response = process::http::get(
      controller.get()->pid,
      "state",
      None(),
      createBasicAuthHeaders(DEFAULT_CREDENTIAL));

  AWAIT_EXPECT_RESPONSE_STATUS_EQ(OK().status, response);

  Try<JSON::Object> state = JSON::parse<JSON::Object>(response->body);
  ASSERT_SOME(state);

  Result<JSON::Number> fraction = state->find<JSON::Number>(
      "frameworks[0].tasks[0].statuses[0].progress.fraction");
  ASSERT_SOME(fraction);
  EXPECT_DOUBLE_EQ(0.3, fraction->as<double>());

  EXPECT_SOME_EQ(
      JSON::String("shard 3 of 10"),
      state->find<JSON::String>(
          "frameworks[0].tasks[0].statuses[0].progress.message"));

  // Explicit reconciliation.
  Future<TaskStatus> explicitReply;
  EXPECT_CALL(sched, statusUpdate(&driver, _))
    .WillOnce(FutureArg<1>(&explicitReply));

  TaskStatus request;
  request.mutable_task_id()->CopyFrom(task.task_id());
  request.set_state(TASK_RUNNING);
  driver.reconcileTasks({request});

  AWAIT_READY(explicitReply);
  EXPECT_EQ(TaskStatus::REASON_RECONCILIATION, explicitReply->reason());
  ASSERT_TRUE(explicitReply->has_progress());
  EXPECT_DOUBLE_EQ(0.3, explicitReply->progress().fraction());

  // Implicit reconciliation.
  Future<TaskStatus> implicitReply;
  EXPECT_CALL(sched, statusUpdate(&driver, _))
    .WillOnce(FutureArg<1>(&implicitReply));

  driver.reconcileTasks({});

  AWAIT_READY(implicitReply);
  EXPECT_EQ(TaskStatus::REASON_RECONCILIATION, implicitReply->reason());
  ASSERT_TRUE(implicitReply->has_progress());
  EXPECT_DOUBLE_EQ(0.3, implicitReply->progress().fraction());
  EXPECT_EQ("shard 3 of 10", implicitReply->progress().message());

  // The reply's own timestamp is current while the observation is not, which
  // is the only thing that lets a framework tell stale progress from fresh.
  EXPECT_DOUBLE_EQ(OBSERVED_AT, implicitReply->progress().observed_at());
  EXPECT_GT(implicitReply->timestamp(), OBSERVED_AT);

  EXPECT_CALL(exec, shutdown(_))
    .Times(AtMost(1));

  driver.stop();
  driver.join();
}


// A fraction outside [0, 1] is an observation error in the reporting
// executor, not a task failure. The field is dropped and counted where the
// report enters, which is the node; the state the update carried is still
// delivered. The controller's own counter answers for a record arriving
// from a node that did not strip it, such as one running older code.
TEST_F(TaskProgressControllerTest, OutOfRangeFractionIsDroppedAndCounted)
{
  Try<Owned<cluster::Controller>> controller = StartController();
  ASSERT_SOME(controller);

  MockExecutor exec(DEFAULT_EXECUTOR_ID);
  TestContainerizer containerizer(&exec);

  Owned<ControllerDetector> detector = controller.get()->createDetector();
  Try<Owned<cluster::Node>> node = StartNode(detector.get(), &containerizer);
  ASSERT_SOME(node);

  MockScheduler sched;
  MediosSchedulerDriver driver(
      &sched,
      DEFAULT_FRAMEWORK_INFO,
      controller.get()->pid,
      DEFAULT_CREDENTIAL);

  EXPECT_CALL(sched, registered(&driver, _, _));

  Future<vector<Offer>> offers;
  EXPECT_CALL(sched, resourceOffers(&driver, _))
    .WillOnce(FutureArg<1>(&offers))
    .WillRepeatedly(Return());

  driver.start();

  AWAIT_READY(offers);
  ASSERT_FALSE(offers->empty());

  TaskInfo task;
  task.set_name("progress-test");
  task.set_principal(DEFAULT_TASK_PRINCIPAL);
  task.mutable_task_id()->set_value("1");
  task.mutable_node_id()->MergeFrom(offers->at(0).node_id());
  task.mutable_resources()->MergeFrom(offers->at(0).resources());
  task.mutable_executor()->MergeFrom(DEFAULT_EXECUTOR_INFO);

  EXPECT_CALL(exec, registered(_, _, _, _))
    .Times(AtMost(1));

  EXPECT_CALL(exec, launchTask(_, _))
    .WillOnce(SendRunningWithProgress(progressAt(1.5)));

  EXPECT_CALL(containerizer, update(_, _, _))
    .WillRepeatedly(Return(Nothing()));

  Future<TaskStatus> running;
  EXPECT_CALL(sched, statusUpdate(&driver, _))
    .WillOnce(FutureArg<1>(&running));

  driver.launchTasks(offers->at(0).id(), {task});

  AWAIT_READY(running);
  EXPECT_EQ(TASK_RUNNING, running->state()) << running->message();
  EXPECT_FALSE(running->has_progress());

  EXPECT_TRUE(metricEquals("node/invalid_task_progress_reports", 1));

  // The controller never saw it, because the node cleaned the update before
  // forwarding it.
  EXPECT_TRUE(metricEquals("controller/invalid_task_progress_reports", 0));

  EXPECT_CALL(exec, shutdown(_))
    .Times(AtMost(1));

  driver.stop();
  driver.join();
}


// A status update that reports no progress must not erase progress already
// recorded. The controller keeps one status per state, so a second
// TASK_RUNNING update replaces the first; a health or check update has
// exactly that shape and carries no progress of its own. Without the carry
// forward in `Controller::updateTask` the only status holding the value is
// gone and the record reads as if the task never reported.
TEST_F(TaskProgressControllerTest, ProgressSurvivesAnUpdateThatOmitsIt)
{
  Try<Owned<cluster::Controller>> controller = StartController();
  ASSERT_SOME(controller);

  MockExecutor exec(DEFAULT_EXECUTOR_ID);
  TestContainerizer containerizer(&exec);

  Owned<ControllerDetector> detector = controller.get()->createDetector();
  Try<Owned<cluster::Node>> node = StartNode(detector.get(), &containerizer);
  ASSERT_SOME(node);

  MockScheduler sched;
  MediosSchedulerDriver driver(
      &sched,
      DEFAULT_FRAMEWORK_INFO,
      controller.get()->pid,
      DEFAULT_CREDENTIAL);

  EXPECT_CALL(sched, registered(&driver, _, _));

  Future<vector<Offer>> offers;
  EXPECT_CALL(sched, resourceOffers(&driver, _))
    .WillOnce(FutureArg<1>(&offers))
    .WillRepeatedly(Return());

  driver.start();

  AWAIT_READY(offers);
  ASSERT_FALSE(offers->empty());

  TaskInfo task;
  task.set_name("progress-test");
  task.set_principal(DEFAULT_TASK_PRINCIPAL);
  task.mutable_task_id()->set_value("1");
  task.mutable_node_id()->MergeFrom(offers->at(0).node_id());
  task.mutable_resources()->MergeFrom(offers->at(0).resources());
  task.mutable_executor()->MergeFrom(DEFAULT_EXECUTOR_INFO);

  ExecutorDriver* execDriver = nullptr;
  EXPECT_CALL(exec, registered(_, _, _, _))
    .WillOnce(SaveArg<0>(&execDriver));

  EXPECT_CALL(exec, launchTask(_, _))
    .WillOnce(SendRunningWithProgress(progressAt(0.3)));

  EXPECT_CALL(containerizer, update(_, _, _))
    .WillRepeatedly(Return(Nothing()));

  Future<TaskStatus> running;
  Future<TaskStatus> health;
  EXPECT_CALL(sched, statusUpdate(&driver, _))
    .WillOnce(FutureArg<1>(&running))
    .WillOnce(FutureArg<1>(&health));

  driver.launchTasks(offers->at(0).id(), {task});

  AWAIT_READY(running);
  ASSERT_EQ(TASK_RUNNING, running->state()) << running->message();
  ASSERT_TRUE(running->has_progress());
  ASSERT_TRUE(execDriver != nullptr);

  // A health update: same state, a health reason, no progress.
  TaskStatus healthUpdate;
  healthUpdate.mutable_task_id()->CopyFrom(task.task_id());
  healthUpdate.set_state(TASK_RUNNING);
  healthUpdate.set_reason(TaskStatus::REASON_TASK_HEALTH_CHECK_STATUS_UPDATED);
  healthUpdate.set_healthy(true);
  execDriver->sendStatusUpdate(healthUpdate);

  AWAIT_READY(health);
  ASSERT_EQ(TASK_RUNNING, health->state());
  EXPECT_TRUE(health->healthy());

  // What the executor sent is forwarded as it sent it, so the health update
  // itself carries no progress. The controller's record does.
  EXPECT_FALSE(health->has_progress());

  Future<Response> response = process::http::get(
      controller.get()->pid,
      "state",
      None(),
      createBasicAuthHeaders(DEFAULT_CREDENTIAL));

  AWAIT_EXPECT_RESPONSE_STATUS_EQ(OK().status, response);

  Try<JSON::Object> state = JSON::parse<JSON::Object>(response->body);
  ASSERT_SOME(state);

  // One status for TASK_RUNNING, the collapsed one, still carrying progress.
  Result<JSON::Number> fraction = state->find<JSON::Number>(
      "frameworks[0].tasks[0].statuses[0].progress.fraction");
  ASSERT_SOME(fraction);
  EXPECT_DOUBLE_EQ(0.3, fraction->as<double>());

  Future<TaskStatus> reply;
  EXPECT_CALL(sched, statusUpdate(&driver, _))
    .WillOnce(FutureArg<1>(&reply));

  driver.reconcileTasks({});

  AWAIT_READY(reply);
  EXPECT_EQ(TaskStatus::REASON_RECONCILIATION, reply->reason());
  ASSERT_TRUE(reply->has_progress());
  EXPECT_DOUBLE_EQ(0.3, reply->progress().fraction());
  EXPECT_DOUBLE_EQ(OBSERVED_AT, reply->progress().observed_at());

  EXPECT_CALL(exec, shutdown(_))
    .Times(AtMost(1));

  driver.stop();
  driver.join();
}


// --- The node -----------------------------------------------------------

class TaskProgressNodeTest : public MediosTest {};


// An executor may report only on tasks it owns. The node holds the only
// executor-to-task map, so it is the only component that can tell a
// cross-executor report from an update it generated itself. The second
// update below names a task this executor was never given; the node refuses
// it and counts it, and the framework never sees it. The third update, for
// the executor's own task, still goes through, which also proves the refusal
// did not wedge the stream.
TEST_F(TaskProgressNodeTest, UpdateForAnUnownedTaskIsRefused)
{
  Try<Owned<cluster::Controller>> controller = StartController();
  ASSERT_SOME(controller);

  MockExecutor exec(DEFAULT_EXECUTOR_ID);
  TestContainerizer containerizer(&exec);

  Owned<ControllerDetector> detector = controller.get()->createDetector();
  Try<Owned<cluster::Node>> node = StartNode(detector.get(), &containerizer);
  ASSERT_SOME(node);

  MockScheduler sched;
  MediosSchedulerDriver driver(
      &sched,
      DEFAULT_FRAMEWORK_INFO,
      controller.get()->pid,
      DEFAULT_CREDENTIAL);

  EXPECT_CALL(sched, registered(&driver, _, _));

  Future<vector<Offer>> offers;
  EXPECT_CALL(sched, resourceOffers(&driver, _))
    .WillOnce(FutureArg<1>(&offers))
    .WillRepeatedly(Return());

  driver.start();

  AWAIT_READY(offers);
  ASSERT_FALSE(offers->empty());

  TaskInfo task;
  task.set_name("owned");
  task.set_principal(DEFAULT_TASK_PRINCIPAL);
  task.mutable_task_id()->set_value("1");
  task.mutable_node_id()->MergeFrom(offers->at(0).node_id());
  task.mutable_resources()->MergeFrom(offers->at(0).resources());
  task.mutable_executor()->MergeFrom(DEFAULT_EXECUTOR_INFO);

  ExecutorDriver* execDriver = nullptr;
  EXPECT_CALL(exec, registered(_, _, _, _))
    .WillOnce(SaveArg<0>(&execDriver));

  EXPECT_CALL(exec, launchTask(_, _))
    .WillOnce(SendStatusUpdateFromTask(TASK_RUNNING));

  EXPECT_CALL(containerizer, update(_, _, _))
    .WillRepeatedly(Return(Nothing()));

  Future<TaskStatus> running;
  Future<TaskStatus> next;
  EXPECT_CALL(sched, statusUpdate(&driver, _))
    .WillOnce(FutureArg<1>(&running))
    .WillOnce(FutureArg<1>(&next));

  driver.launchTasks(offers->at(0).id(), {task});

  AWAIT_READY(running);
  ASSERT_EQ(TASK_RUNNING, running->state()) << running->message();
  ASSERT_TRUE(execDriver != nullptr);

  // A task id this executor was never given.
  TaskStatus unowned;
  unowned.mutable_task_id()->set_value("2");
  unowned.set_state(TASK_RUNNING);
  execDriver->sendStatusUpdate(unowned);

  // The node handles updates from one executor in order, so once this one
  // reaches the framework the refusal above has already been decided.
  TaskStatus finished;
  finished.mutable_task_id()->CopyFrom(task.task_id());
  finished.set_state(TASK_FINISHED);
  execDriver->sendStatusUpdate(finished);

  AWAIT_READY(next);
  EXPECT_EQ(TASK_FINISHED, next->state());
  EXPECT_EQ(task.task_id(), next->task_id());

  EXPECT_TRUE(metricEquals("node/unowned_task_status_updates", 1));

  EXPECT_CALL(exec, shutdown(_))
    .Times(AtMost(1));

  driver.stop();
  driver.join();
}


// The case the design packet names as the unmet invariant: two executors on
// one node, and one reports on the other's task. The task is owned, so the
// node's executor lookup succeeds and the old code forwarded the update with
// the owner's id substituted for the sender's.
TEST_F(TaskProgressNodeTest, UpdateForAnotherExecutorsTaskIsRefused)
{
  Try<Owned<cluster::Controller>> controller = StartController();
  ASSERT_SOME(controller);

  ExecutorInfo executor1 = createExecutorInfo("executor-1", "exit 1");
  ExecutorInfo executor2 = createExecutorInfo("executor-2", "exit 1");

  MockExecutor exec1(executor1.executor_id());
  MockExecutor exec2(executor2.executor_id());

  hashmap<ExecutorID, Executor*> execs;
  execs[executor1.executor_id()] = &exec1;
  execs[executor2.executor_id()] = &exec2;

  TestContainerizer containerizer(execs);

  Owned<ControllerDetector> detector = controller.get()->createDetector();
  Try<Owned<cluster::Node>> node = StartNode(detector.get(), &containerizer);
  ASSERT_SOME(node);

  MockScheduler sched;
  MediosSchedulerDriver driver(
      &sched,
      DEFAULT_FRAMEWORK_INFO,
      controller.get()->pid,
      DEFAULT_CREDENTIAL);

  EXPECT_CALL(sched, registered(&driver, _, _));

  Future<vector<Offer>> offers;
  EXPECT_CALL(sched, resourceOffers(&driver, _))
    .WillOnce(FutureArg<1>(&offers))
    .WillRepeatedly(Return());

  driver.start();

  AWAIT_READY(offers);
  ASSERT_FALSE(offers->empty());

  vector<TaskInfo> tasks;

  TaskInfo task1;
  task1.set_name("one");
  task1.set_principal(DEFAULT_TASK_PRINCIPAL);
  task1.mutable_task_id()->set_value("1");
  task1.mutable_node_id()->MergeFrom(offers->at(0).node_id());
  task1.mutable_resources()->MergeFrom(
      Resources::parse("cpus:1;mem:512").get());
  task1.mutable_executor()->MergeFrom(executor1);
  tasks.push_back(task1);

  TaskInfo task2;
  task2.set_name("two");
  task2.set_principal(DEFAULT_TASK_PRINCIPAL);
  task2.mutable_task_id()->set_value("2");
  task2.mutable_node_id()->MergeFrom(offers->at(0).node_id());
  task2.mutable_resources()->MergeFrom(
      Resources::parse("cpus:1;mem:512").get());
  task2.mutable_executor()->MergeFrom(executor2);
  tasks.push_back(task2);

  ExecutorDriver* execDriver1 = nullptr;
  EXPECT_CALL(exec1, registered(_, _, _, _))
    .WillOnce(SaveArg<0>(&execDriver1));
  EXPECT_CALL(exec1, launchTask(_, _))
    .WillOnce(SendStatusUpdateFromTask(TASK_RUNNING));

  EXPECT_CALL(exec2, registered(_, _, _, _));
  EXPECT_CALL(exec2, launchTask(_, _))
    .WillOnce(SendStatusUpdateFromTask(TASK_RUNNING));

  EXPECT_CALL(containerizer, update(_, _, _))
    .WillRepeatedly(Return(Nothing()));

  Future<TaskStatus> first;
  Future<TaskStatus> second;
  Future<TaskStatus> third;
  EXPECT_CALL(sched, statusUpdate(&driver, _))
    .WillOnce(FutureArg<1>(&first))
    .WillOnce(FutureArg<1>(&second))
    .WillOnce(FutureArg<1>(&third));

  driver.launchTasks(offers->at(0).id(), tasks);

  AWAIT_READY(first);
  AWAIT_READY(second);
  ASSERT_EQ(TASK_RUNNING, first->state()) << first->message();
  ASSERT_EQ(TASK_RUNNING, second->state()) << second->message();
  ASSERT_TRUE(execDriver1 != nullptr);

  // Executor one reports on executor two's task.
  TaskStatus poached;
  poached.mutable_task_id()->CopyFrom(task2.task_id());
  poached.set_state(TASK_FAILED);
  poached.mutable_progress()->CopyFrom(progressAt(0.9));
  execDriver1->sendStatusUpdate(poached);

  // Then on its own, which the node delivers in order, so by the time this
  // reaches the framework the refusal above has been decided.
  TaskStatus own;
  own.mutable_task_id()->CopyFrom(task1.task_id());
  own.set_state(TASK_FINISHED);
  execDriver1->sendStatusUpdate(own);

  AWAIT_READY(third);
  EXPECT_EQ(TASK_FINISHED, third->state());
  EXPECT_EQ(task1.task_id(), third->task_id());

  EXPECT_TRUE(metricEquals("node/unowned_task_status_updates", 1));

  EXPECT_CALL(exec1, shutdown(_))
    .Times(AtMost(1));
  EXPECT_CALL(exec2, shutdown(_))
    .Times(AtMost(1));

  driver.stop();
  driver.join();
}


// The completed-task arm of `Executor::hasTask`. Once a terminal update is
// acknowledged the task moves out of the executor's live structures into its
// completed-task cache, so the node's lookup by task id no longer finds an
// executor. A replay of that update must still be forwarded, which is the
// sequence the upstream note in `Node::statusUpdate` was written for, and
// the one place the ownership refusal could turn a delivered terminal update
// into a dropped one.
//
// This drives a live executor replaying after the acknowledgement. The full
// node-failover replay, where the executor reregisters after a node restart
// with `--recover=reconnect`, is not covered here: it needs a real executor
// process surviving the restart, which the test containerizer does not give.
// The behaviour under test is decided by whether the sender holds a record
// of the task, which is what this exercises.
TEST_F(TaskProgressNodeTest, ReplayedUpdateForACompletedTaskIsForwarded)
{
  Try<Owned<cluster::Controller>> controller = StartController();
  ASSERT_SOME(controller);

  MockExecutor exec(DEFAULT_EXECUTOR_ID);
  TestContainerizer containerizer(&exec);

  Owned<ControllerDetector> detector = controller.get()->createDetector();
  Try<Owned<cluster::Node>> node = StartNode(detector.get(), &containerizer);
  ASSERT_SOME(node);

  MockScheduler sched;
  MediosSchedulerDriver driver(
      &sched,
      DEFAULT_FRAMEWORK_INFO,
      controller.get()->pid,
      DEFAULT_CREDENTIAL);

  EXPECT_CALL(sched, registered(&driver, _, _));

  Future<vector<Offer>> offers;
  EXPECT_CALL(sched, resourceOffers(&driver, _))
    .WillOnce(FutureArg<1>(&offers))
    .WillRepeatedly(Return());

  driver.start();

  AWAIT_READY(offers);
  ASSERT_FALSE(offers->empty());

  TaskInfo task;
  task.set_name("replayed");
  task.set_principal(DEFAULT_TASK_PRINCIPAL);
  task.mutable_task_id()->set_value("1");
  task.mutable_node_id()->MergeFrom(offers->at(0).node_id());
  task.mutable_resources()->MergeFrom(offers->at(0).resources());
  task.mutable_executor()->MergeFrom(DEFAULT_EXECUTOR_INFO);

  ExecutorDriver* execDriver = nullptr;
  EXPECT_CALL(exec, registered(_, _, _, _))
    .WillOnce(SaveArg<0>(&execDriver));

  EXPECT_CALL(exec, launchTask(_, _))
    .WillOnce(SendRunningWithProgress(progressAt(0.8)));

  EXPECT_CALL(containerizer, update(_, _, _))
    .WillRepeatedly(Return(Nothing()));

  Future<TaskStatus> running;
  Future<TaskStatus> finished;
  EXPECT_CALL(sched, statusUpdate(&driver, _))
    .WillOnce(FutureArg<1>(&running))
    .WillOnce(FutureArg<1>(&finished))
    .WillRepeatedly(Return());

  driver.launchTasks(offers->at(0).id(), {task});

  AWAIT_READY(running);
  ASSERT_EQ(TASK_RUNNING, running->state()) << running->message();
  ASSERT_TRUE(execDriver != nullptr);

  // The acknowledgement of this update is what moves the task into the
  // executor's completed-task cache.
  Future<Nothing> acknowledged =
    FUTURE_DISPATCH(node.get()->pid, &Node::_statusUpdateAcknowledgement);

  TaskStatus terminal;
  terminal.mutable_task_id()->CopyFrom(task.task_id());
  terminal.set_state(TASK_FINISHED);
  terminal.mutable_progress()->CopyFrom(progressAt(1.0));
  execDriver->sendStatusUpdate(terminal);

  AWAIT_READY(finished);
  ASSERT_EQ(TASK_FINISHED, finished->state());
  AWAIT_READY(acknowledged);

  // The acknowledgement dispatch having fired does not mean it has run, and
  // what moves the task into the executor's completed-task cache is the
  // handler. Drain before relying on that state.
  Clock::pause();
  Clock::settle();
  Clock::resume();

  // Replay it. Reaching `___statusUpdate` proves the node took the
  // forwarding path rather than the refusal.
  Future<Nothing> forwarded =
    FUTURE_DISPATCH(node.get()->pid, &Node::___statusUpdate);

  execDriver->sendStatusUpdate(terminal);

  AWAIT_READY(forwarded);

  EXPECT_TRUE(metricEquals("node/unowned_task_status_updates", 0));

  // The controller's record of the completed task still carries the value.
  Future<Response> response = process::http::get(
      controller.get()->pid,
      "state",
      None(),
      createBasicAuthHeaders(DEFAULT_CREDENTIAL));

  AWAIT_EXPECT_RESPONSE_STATUS_EQ(OK().status, response);

  Try<JSON::Object> state = JSON::parse<JSON::Object>(response->body);
  ASSERT_SOME(state);

  Result<JSON::Number> fraction = state->find<JSON::Number>(
      "frameworks[0].completed_tasks[0].statuses[1].progress.fraction");
  ASSERT_SOME(fraction);
  EXPECT_DOUBLE_EQ(1.0, fraction->as<double>());

  EXPECT_CALL(exec, shutdown(_))
    .Times(AtMost(1));

  driver.stop();
  driver.join();
}


// The node keeps its own task record, one status per state, and checkpoints
// it. A status that reports no progress must not erase progress recorded
// there either: a health update would otherwise drop the value the node
// holds, and a reregistration after a node restart would carry the loss to
// the controller.
TEST_F(TaskProgressNodeTest, NodeRecordHoldsProgressAcrossAnOmittingUpdate)
{
  Try<Owned<cluster::Controller>> controller = StartController();
  ASSERT_SOME(controller);

  MockExecutor exec(DEFAULT_EXECUTOR_ID);
  TestContainerizer containerizer(&exec);

  Owned<ControllerDetector> detector = controller.get()->createDetector();
  Try<Owned<cluster::Node>> node = StartNode(detector.get(), &containerizer);
  ASSERT_SOME(node);

  MockScheduler sched;
  MediosSchedulerDriver driver(
      &sched,
      DEFAULT_FRAMEWORK_INFO,
      controller.get()->pid,
      DEFAULT_CREDENTIAL);

  EXPECT_CALL(sched, registered(&driver, _, _));

  Future<vector<Offer>> offers;
  EXPECT_CALL(sched, resourceOffers(&driver, _))
    .WillOnce(FutureArg<1>(&offers))
    .WillRepeatedly(Return());

  driver.start();

  AWAIT_READY(offers);
  ASSERT_FALSE(offers->empty());

  TaskInfo task;
  task.set_name("node-record");
  task.set_principal(DEFAULT_TASK_PRINCIPAL);
  task.mutable_task_id()->set_value("1");
  task.mutable_node_id()->MergeFrom(offers->at(0).node_id());
  task.mutable_resources()->MergeFrom(offers->at(0).resources());
  task.mutable_executor()->MergeFrom(DEFAULT_EXECUTOR_INFO);

  ExecutorDriver* execDriver = nullptr;
  EXPECT_CALL(exec, registered(_, _, _, _))
    .WillOnce(SaveArg<0>(&execDriver));

  EXPECT_CALL(exec, launchTask(_, _))
    .WillOnce(SendRunningWithProgress(progressAt(0.6)));

  EXPECT_CALL(containerizer, update(_, _, _))
    .WillRepeatedly(Return(Nothing()));

  Future<TaskStatus> running;
  Future<TaskStatus> health;
  EXPECT_CALL(sched, statusUpdate(&driver, _))
    .WillOnce(FutureArg<1>(&running))
    .WillOnce(FutureArg<1>(&health));

  driver.launchTasks(offers->at(0).id(), {task});

  AWAIT_READY(running);
  ASSERT_EQ(TASK_RUNNING, running->state()) << running->message();
  ASSERT_TRUE(execDriver != nullptr);

  TaskStatus healthUpdate;
  healthUpdate.mutable_task_id()->CopyFrom(task.task_id());
  healthUpdate.set_state(TASK_RUNNING);
  healthUpdate.set_reason(TaskStatus::REASON_TASK_HEALTH_CHECK_STATUS_UPDATED);
  healthUpdate.set_healthy(true);
  execDriver->sendStatusUpdate(healthUpdate);

  AWAIT_READY(health);
  ASSERT_EQ(TASK_RUNNING, health->state());
  EXPECT_FALSE(health->has_progress());

  // The node's own record, which is what it checkpoints and replays.
  Future<Response> response = process::http::get(
      node.get()->pid,
      "state",
      None(),
      createBasicAuthHeaders(DEFAULT_CREDENTIAL));

  AWAIT_EXPECT_RESPONSE_STATUS_EQ(OK().status, response);

  Try<JSON::Object> state = JSON::parse<JSON::Object>(response->body);
  ASSERT_SOME(state);

  Result<JSON::Number> fraction = state->find<JSON::Number>(
      "frameworks[0].executors[0].tasks[0].statuses[0].progress.fraction");
  ASSERT_SOME(fraction);
  EXPECT_DOUBLE_EQ(0.6, fraction->as<double>());

  Result<JSON::Boolean> healthy = state->find<JSON::Boolean>(
      "frameworks[0].executors[0].tasks[0].statuses[0].healthy");
  EXPECT_SOME_EQ(JSON::Boolean(true), healthy);

  EXPECT_CALL(exec, shutdown(_))
    .Times(AtMost(1));

  driver.stop();
  driver.join();
}


// A task awaiting authorization is in `Framework::pendingTasks` and in no
// executor's structures, and the branch that handles an update for one
// removes it, which prevents its launch. So the ownership check has to run
// before that branch: this is the cross-executor report that does real
// damage rather than merely supplying a wrong value.
TEST_F(TaskProgressNodeTest, UpdateForAnotherExecutorsPendingTaskIsRefused)
{
  Try<Owned<cluster::Controller>> controller = StartController();
  ASSERT_SOME(controller);

  ExecutorInfo executor1 = createExecutorInfo("executor-1", "exit 1");
  ExecutorInfo executor2 = createExecutorInfo("executor-2", "exit 1");

  MockExecutor exec1(executor1.executor_id());
  MockExecutor exec2(executor2.executor_id());

  hashmap<ExecutorID, Executor*> execs;
  execs[executor1.executor_id()] = &exec1;
  execs[executor2.executor_id()] = &exec2;

  TestContainerizer containerizer(execs);

  MockAuthorizer authorizer;

  Owned<ControllerDetector> detector = controller.get()->createDetector();
  Try<Owned<cluster::Node>> node =
    StartNode(detector.get(), &containerizer, &authorizer);
  ASSERT_SOME(node);

  MockScheduler sched;
  MediosSchedulerDriver driver(
      &sched,
      DEFAULT_FRAMEWORK_INFO,
      controller.get()->pid,
      DEFAULT_CREDENTIAL);

  EXPECT_CALL(sched, registered(&driver, _, _));

  Future<vector<Offer>> offers;
  EXPECT_CALL(sched, resourceOffers(&driver, _))
    .WillOnce(FutureArg<1>(&offers))
    .WillRepeatedly(Return());

  driver.start();

  AWAIT_READY(offers);
  ASSERT_FALSE(offers->empty());

  vector<TaskInfo> tasks;

  TaskInfo task1;
  task1.set_name("one");
  task1.set_principal(DEFAULT_TASK_PRINCIPAL);
  task1.mutable_task_id()->set_value("1");
  task1.mutable_node_id()->MergeFrom(offers->at(0).node_id());
  task1.mutable_resources()->MergeFrom(
      Resources::parse("cpus:1;mem:256").get());
  task1.mutable_executor()->MergeFrom(executor1);
  tasks.push_back(task1);

  TaskInfo task2;
  task2.set_name("two");
  task2.set_principal(DEFAULT_TASK_PRINCIPAL);
  task2.mutable_task_id()->set_value("2");
  task2.mutable_node_id()->MergeFrom(offers->at(0).node_id());
  task2.mutable_resources()->MergeFrom(
      Resources::parse("cpus:1;mem:256").get());
  task2.mutable_executor()->MergeFrom(executor2);
  tasks.push_back(task2);

  // Everything is allowed except that the second task's authorization is
  // held, which is what keeps it pending.
  Promise<bool> authorizeTask2;

  EXPECT_CALL(authorizer, authorized(_))
    .WillRepeatedly(Return(true));

  EXPECT_CALL(
      authorizer,
      authorized(AuthorizationRequestHasTaskID(task2.task_id())))
    .WillOnce(Return(authorizeTask2.future()));

  ExecutorDriver* execDriver1 = nullptr;
  EXPECT_CALL(exec1, registered(_, _, _, _))
    .WillOnce(SaveArg<0>(&execDriver1));
  EXPECT_CALL(exec1, launchTask(_, _))
    .WillOnce(SendStatusUpdateFromTask(TASK_RUNNING));

  EXPECT_CALL(exec2, registered(_, _, _, _))
    .Times(AtMost(1));
  EXPECT_CALL(exec2, launchTask(_, _))
    .WillRepeatedly(SendStatusUpdateFromTask(TASK_RUNNING));

  EXPECT_CALL(containerizer, update(_, _, _))
    .WillRepeatedly(Return(Nothing()));

  Future<TaskStatus> firstRunning;
  EXPECT_CALL(sched, statusUpdate(&driver, TaskStatusTaskIdEq(task1.task_id())))
    .WillOnce(FutureArg<1>(&firstRunning))
    .WillRepeatedly(Return());

  Future<TaskStatus> secondRunning;
  EXPECT_CALL(sched, statusUpdate(&driver, TaskStatusTaskIdEq(task2.task_id())))
    .WillOnce(FutureArg<1>(&secondRunning))
    .WillRepeatedly(Return());

  driver.launchTasks(offers->at(0).id(), tasks);

  AWAIT_READY(firstRunning);
  ASSERT_EQ(TASK_RUNNING, firstRunning->state()) << firstRunning->message();
  ASSERT_TRUE(execDriver1 != nullptr);

  // The second task is still waiting on its authorization. Executor one
  // reports a terminal state for it.
  ASSERT_TRUE(secondRunning.isPending());

  TaskStatus poached;
  poached.mutable_task_id()->CopyFrom(task2.task_id());
  poached.set_state(TASK_FAILED);
  poached.mutable_progress()->CopyFrom(progressAt(0.9));
  execDriver1->sendStatusUpdate(poached);

  // Executor one's own update, which the node handles after the refusal.
  TaskStatus own;
  own.mutable_task_id()->CopyFrom(task1.task_id());
  own.set_state(TASK_FINISHED);
  execDriver1->sendStatusUpdate(own);

  Future<Nothing> ownHandled =
    FUTURE_DISPATCH(node.get()->pid, &Node::___statusUpdate);
  AWAIT_READY(ownHandled);

  EXPECT_TRUE(metricEquals("node/unowned_task_status_updates", 1));

  // The launch was not prevented: releasing the authorization still runs it.
  authorizeTask2.set(true);

  AWAIT_READY(secondRunning);
  EXPECT_EQ(TASK_RUNNING, secondRunning->state()) << secondRunning->message();

  EXPECT_CALL(exec1, shutdown(_))
    .Times(AtMost(1));
  EXPECT_CALL(exec2, shutdown(_))
    .Times(AtMost(1));

  driver.stop();
  driver.join();
}


// An executor keeps an update until it is acknowledged, so a refusal has to
// end the exchange. The acknowledgement goes to the executor that sent the
// update, which is not the one the task belongs to.
TEST_F(TaskProgressNodeTest, RefusalAcknowledgesTheSenderAndNobodyElse)
{
  Try<Owned<cluster::Controller>> controller = StartController();
  ASSERT_SOME(controller);

  ExecutorInfo executor1 = createExecutorInfo("executor-1", "exit 1");
  ExecutorInfo executor2 = createExecutorInfo("executor-2", "exit 1");

  MockExecutor exec1(executor1.executor_id());
  MockExecutor exec2(executor2.executor_id());

  hashmap<ExecutorID, Executor*> execs;
  execs[executor1.executor_id()] = &exec1;
  execs[executor2.executor_id()] = &exec2;

  TestContainerizer containerizer(execs);

  Owned<ControllerDetector> detector = controller.get()->createDetector();
  Try<Owned<cluster::Node>> node = StartNode(detector.get(), &containerizer);
  ASSERT_SOME(node);

  MockScheduler sched;
  MediosSchedulerDriver driver(
      &sched,
      DEFAULT_FRAMEWORK_INFO,
      controller.get()->pid,
      DEFAULT_CREDENTIAL);

  EXPECT_CALL(sched, registered(&driver, _, _));

  Future<vector<Offer>> offers;
  EXPECT_CALL(sched, resourceOffers(&driver, _))
    .WillOnce(FutureArg<1>(&offers))
    .WillRepeatedly(Return());

  driver.start();

  AWAIT_READY(offers);
  ASSERT_FALSE(offers->empty());

  vector<TaskInfo> tasks;

  TaskInfo task1;
  task1.set_name("one");
  task1.set_principal(DEFAULT_TASK_PRINCIPAL);
  task1.mutable_task_id()->set_value("1");
  task1.mutable_node_id()->MergeFrom(offers->at(0).node_id());
  task1.mutable_resources()->MergeFrom(
      Resources::parse("cpus:1;mem:256").get());
  task1.mutable_executor()->MergeFrom(executor1);
  tasks.push_back(task1);

  TaskInfo task2;
  task2.set_name("two");
  task2.set_principal(DEFAULT_TASK_PRINCIPAL);
  task2.mutable_task_id()->set_value("2");
  task2.mutable_node_id()->MergeFrom(offers->at(0).node_id());
  task2.mutable_resources()->MergeFrom(
      Resources::parse("cpus:1;mem:256").get());
  task2.mutable_executor()->MergeFrom(executor2);
  tasks.push_back(task2);

  // Neither executor reports anything, so the only acknowledgement in
  // flight below is the one the refusal produces. A task is in its
  // executor's launched tasks from the launch, not from its first report.
  ExecutorDriver* execDriver1 = nullptr;
  Future<Nothing> registeredOne;
  EXPECT_CALL(exec1, registered(_, _, _, _))
    .WillOnce(DoAll(SaveArg<0>(&execDriver1),
                    FutureSatisfy(&registeredOne)));
  EXPECT_CALL(exec1, launchTask(_, _))
    .WillOnce(Return());

  Future<Nothing> registeredTwo;
  EXPECT_CALL(exec2, registered(_, _, _, _))
    .WillOnce(FutureSatisfy(&registeredTwo));
  EXPECT_CALL(exec2, launchTask(_, _))
    .WillOnce(Return());

  EXPECT_CALL(containerizer, update(_, _, _))
    .WillRepeatedly(Return(Nothing()));

  EXPECT_CALL(sched, statusUpdate(&driver, _))
    .WillRepeatedly(Return());

  // Capture the executors' addresses so the acknowledgement's destination
  // can be told apart.
  Future<Message> registrationOne =
    FUTURE_MESSAGE(Eq(RegisterExecutorMessage().GetTypeName()), _, _);
  Future<Message> registrationTwo =
    FUTURE_MESSAGE(Eq(RegisterExecutorMessage().GetTypeName()), _, _);

  driver.launchTasks(offers->at(0).id(), tasks);

  AWAIT_READY(registeredOne);
  AWAIT_READY(registeredTwo);
  AWAIT_READY(registrationOne);
  AWAIT_READY(registrationTwo);
  ASSERT_TRUE(execDriver1 != nullptr);

  // Executor one's address, taken from whichever registration carries its id.
  Option<UPID> senderPid = None();
  Option<UPID> victimPid = None();

  vector<Future<Message>> registrations = {registrationOne, registrationTwo};
  foreach (const Future<Message>& registration, registrations) {
    RegisterExecutorMessage parsed;
    ASSERT_TRUE(parsed.ParseFromString(registration->body));

    if (parsed.executor_id() == executor1.executor_id()) {
      senderPid = registration->from;
    } else {
      victimPid = registration->from;
    }
  }

  ASSERT_SOME(senderPid);
  ASSERT_SOME(victimPid);
  ASSERT_NE(senderPid.get(), victimPid.get());

  Future<Message> acknowledgement = FUTURE_MESSAGE(
      Eq(StatusUpdateAcknowledgementMessage().GetTypeName()), _, _);

  TaskStatus poached;
  poached.mutable_task_id()->CopyFrom(task2.task_id());
  poached.set_state(TASK_FAILED);
  execDriver1->sendStatusUpdate(poached);

  AWAIT_READY(acknowledgement);

  StatusUpdateAcknowledgementMessage acknowledged;
  ASSERT_TRUE(acknowledged.ParseFromString(acknowledgement->body));

  // Addressed to the executor that sent the refused update, not the one
  // that owns the task.
  EXPECT_EQ(senderPid.get(), acknowledgement->to);
  EXPECT_NE(victimPid.get(), acknowledgement->to);
  EXPECT_EQ(task2.task_id(), acknowledged.task_id());

  EXPECT_TRUE(metricEquals("node/unowned_task_status_updates", 1));

  EXPECT_CALL(exec1, shutdown(_))
    .Times(AtMost(1));
  EXPECT_CALL(exec2, shutdown(_))
    .Times(AtMost(1));

  driver.stop();
  driver.join();
}


// A libprocess executor is identified by the address it sends from, which it
// cannot choose. These two updates name a real executor and a real task that
// executor owns, and are still refused, because they do not come from that
// executor's address. The second omits the id entirely.
TEST_F(TaskProgressNodeTest, UpdateFromAnUnregisteredAddressIsRefused)
{
  Try<Owned<cluster::Controller>> controller = StartController();
  ASSERT_SOME(controller);

  MockExecutor exec(DEFAULT_EXECUTOR_ID);
  TestContainerizer containerizer(&exec);

  Owned<ControllerDetector> detector = controller.get()->createDetector();
  Try<Owned<cluster::Node>> node = StartNode(detector.get(), &containerizer);
  ASSERT_SOME(node);

  MockScheduler sched;
  MediosSchedulerDriver driver(
      &sched,
      DEFAULT_FRAMEWORK_INFO,
      controller.get()->pid,
      DEFAULT_CREDENTIAL);

  Future<FrameworkID> frameworkId;
  EXPECT_CALL(sched, registered(&driver, _, _))
    .WillOnce(FutureArg<1>(&frameworkId));

  Future<vector<Offer>> offers;
  EXPECT_CALL(sched, resourceOffers(&driver, _))
    .WillOnce(FutureArg<1>(&offers))
    .WillRepeatedly(Return());

  driver.start();

  AWAIT_READY(frameworkId);
  AWAIT_READY(offers);
  ASSERT_FALSE(offers->empty());

  TaskInfo task;
  task.set_name("owned");
  task.set_principal(DEFAULT_TASK_PRINCIPAL);
  task.mutable_task_id()->set_value("1");
  task.mutable_node_id()->MergeFrom(offers->at(0).node_id());
  task.mutable_resources()->MergeFrom(offers->at(0).resources());
  task.mutable_executor()->MergeFrom(DEFAULT_EXECUTOR_INFO);

  EXPECT_CALL(exec, registered(_, _, _, _))
    .Times(AtMost(1));
  EXPECT_CALL(exec, launchTask(_, _))
    .WillOnce(SendStatusUpdateFromTask(TASK_RUNNING));

  EXPECT_CALL(containerizer, update(_, _, _))
    .WillRepeatedly(Return(Nothing()));

  Future<TaskStatus> running;
  EXPECT_CALL(sched, statusUpdate(&driver, _))
    .WillOnce(FutureArg<1>(&running))
    .WillRepeatedly(Return());

  driver.launchTasks(offers->at(0).id(), {task});

  AWAIT_READY(running);
  ASSERT_EQ(TASK_RUNNING, running->state()) << running->message();

  // Two updates posted from this test's own address, which is not any
  // executor's: one naming the real executor, one naming none.
  for (int i = 0; i < 2; i++) {
    StatusUpdate update;
    update.mutable_framework_id()->CopyFrom(frameworkId.get());
    update.mutable_node_id()->CopyFrom(offers->at(0).node_id());
    update.set_timestamp(0);
    update.set_uuid(id::UUID::random().toBytes());

    if (i == 0) {
      update.mutable_executor_id()->CopyFrom(DEFAULT_EXECUTOR_ID);
    }

    TaskStatus* status = update.mutable_status();
    status->mutable_task_id()->CopyFrom(task.task_id());
    status->mutable_node_id()->CopyFrom(offers->at(0).node_id());
    status->set_state(TASK_FAILED);
    status->set_uuid(update.uuid());
    status->mutable_progress()->set_fraction(0.9);

    const UPID forged("forged@127.0.0.1:1");

    Future<Message> atNode = FUTURE_MESSAGE(
        Eq(StatusUpdateMessage().GetTypeName()), forged, node.get()->pid);

    StatusUpdateMessage message;
    message.mutable_update()->CopyFrom(update);
    message.set_pid(stringify(forged));

    process::post(forged, node.get()->pid, message);

    AWAIT_READY(atNode);
  }

  // The posts come from an address with no process behind it, so there is no
  // reply to wait on. Settle the queues instead.
  Clock::pause();
  Clock::settle();
  Clock::resume();

  EXPECT_TRUE(metricEquals("node/unowned_task_status_updates", 2));

  EXPECT_CALL(exec, shutdown(_))
    .Times(AtMost(1));

  driver.stop();
  driver.join();
}


// The node keeps and checkpoints its own task record, and reports it when it
// reregisters, so a report the controller would refuse must not be kept
// there either. A valid report, then an invalid one, then one that reports
// nothing: the record holds the valid value throughout, and the lifecycle
// state of every update is still delivered.
TEST_F(TaskProgressNodeTest, InvalidProgressIsNotRetainedAtTheNode)
{
  Try<Owned<cluster::Controller>> controller = StartController();
  ASSERT_SOME(controller);

  MockExecutor exec(DEFAULT_EXECUTOR_ID);
  TestContainerizer containerizer(&exec);

  Owned<ControllerDetector> detector = controller.get()->createDetector();
  Try<Owned<cluster::Node>> node = StartNode(detector.get(), &containerizer);
  ASSERT_SOME(node);

  MockScheduler sched;
  MediosSchedulerDriver driver(
      &sched,
      DEFAULT_FRAMEWORK_INFO,
      controller.get()->pid,
      DEFAULT_CREDENTIAL);

  EXPECT_CALL(sched, registered(&driver, _, _));

  Future<vector<Offer>> offers;
  EXPECT_CALL(sched, resourceOffers(&driver, _))
    .WillOnce(FutureArg<1>(&offers))
    .WillRepeatedly(Return());

  driver.start();

  AWAIT_READY(offers);
  ASSERT_FALSE(offers->empty());

  TaskInfo task;
  task.set_name("sanitized");
  task.set_principal(DEFAULT_TASK_PRINCIPAL);
  task.mutable_task_id()->set_value("1");
  task.mutable_node_id()->MergeFrom(offers->at(0).node_id());
  task.mutable_resources()->MergeFrom(offers->at(0).resources());
  task.mutable_executor()->MergeFrom(DEFAULT_EXECUTOR_INFO);

  ExecutorDriver* execDriver = nullptr;
  EXPECT_CALL(exec, registered(_, _, _, _))
    .WillOnce(SaveArg<0>(&execDriver));

  EXPECT_CALL(exec, launchTask(_, _))
    .WillOnce(SendRunningWithProgress(progressAt(0.3)));

  EXPECT_CALL(containerizer, update(_, _, _))
    .WillRepeatedly(Return(Nothing()));

  Future<TaskStatus> running;
  Future<TaskStatus> second;
  Future<TaskStatus> third;
  EXPECT_CALL(sched, statusUpdate(&driver, _))
    .WillOnce(FutureArg<1>(&running))
    .WillOnce(FutureArg<1>(&second))
    .WillOnce(FutureArg<1>(&third));

  driver.launchTasks(offers->at(0).id(), {task});

  AWAIT_READY(running);
  ASSERT_EQ(TASK_RUNNING, running->state()) << running->message();
  ASSERT_TRUE(execDriver != nullptr);

  // An out-of-range fraction.
  TaskStatus invalid;
  invalid.mutable_task_id()->CopyFrom(task.task_id());
  invalid.set_state(TASK_RUNNING);
  invalid.mutable_progress()->CopyFrom(progressAt(1.5));
  execDriver->sendStatusUpdate(invalid);

  AWAIT_READY(second);
  EXPECT_EQ(TASK_RUNNING, second->state());

  // Then a health-shaped update reporting nothing.
  TaskStatus omitting;
  omitting.mutable_task_id()->CopyFrom(task.task_id());
  omitting.set_state(TASK_RUNNING);
  omitting.set_reason(TaskStatus::REASON_TASK_HEALTH_CHECK_STATUS_UPDATED);
  omitting.set_healthy(true);
  execDriver->sendStatusUpdate(omitting);

  AWAIT_READY(third);
  EXPECT_EQ(TASK_RUNNING, third->state());
  EXPECT_TRUE(third->healthy());

  // The node's record.
  Future<Response> nodeState = process::http::get(
      node.get()->pid,
      "state",
      None(),
      createBasicAuthHeaders(DEFAULT_CREDENTIAL));

  AWAIT_EXPECT_RESPONSE_STATUS_EQ(OK().status, nodeState);

  Try<JSON::Object> parsedNodeState =
    JSON::parse<JSON::Object>(nodeState->body);
  ASSERT_SOME(parsedNodeState);

  Result<JSON::Number> atNode = parsedNodeState->find<JSON::Number>(
      "frameworks[0].executors[0].tasks[0].statuses[0].progress.fraction");
  ASSERT_SOME(atNode);
  EXPECT_DOUBLE_EQ(0.3, atNode->as<double>());

  // And the controller's.
  Future<Response> controllerState = process::http::get(
      controller.get()->pid,
      "state",
      None(),
      createBasicAuthHeaders(DEFAULT_CREDENTIAL));

  AWAIT_EXPECT_RESPONSE_STATUS_EQ(OK().status, controllerState);

  Try<JSON::Object> parsedControllerState =
    JSON::parse<JSON::Object>(controllerState->body);
  ASSERT_SOME(parsedControllerState);

  Result<JSON::Number> atController =
    parsedControllerState->find<JSON::Number>(
        "frameworks[0].tasks[0].statuses[0].progress.fraction");
  ASSERT_SOME(atController);
  EXPECT_DOUBLE_EQ(0.3, atController->as<double>());

  EXPECT_CALL(exec, shutdown(_))
    .Times(AtMost(1));

  driver.stop();
  driver.join();
}


// Only the progress is discarded, and a terminal update still terminates the
// task. The report here passes the message bound and fails the serialized
// bound, which is the shape protobuf's unknown fields make possible.
TEST_F(TaskProgressNodeTest, OversizedProgressOnATerminalUpdateLosesOnlyIt)
{
  Try<Owned<cluster::Controller>> controller = StartController();
  ASSERT_SOME(controller);

  MockExecutor exec(DEFAULT_EXECUTOR_ID);
  TestContainerizer containerizer(&exec);

  Owned<ControllerDetector> detector = controller.get()->createDetector();
  Try<Owned<cluster::Node>> node = StartNode(detector.get(), &containerizer);
  ASSERT_SOME(node);

  MockScheduler sched;
  MediosSchedulerDriver driver(
      &sched,
      DEFAULT_FRAMEWORK_INFO,
      controller.get()->pid,
      DEFAULT_CREDENTIAL);

  EXPECT_CALL(sched, registered(&driver, _, _));

  Future<vector<Offer>> offers;
  EXPECT_CALL(sched, resourceOffers(&driver, _))
    .WillOnce(FutureArg<1>(&offers))
    .WillRepeatedly(Return());

  driver.start();

  AWAIT_READY(offers);
  ASSERT_FALSE(offers->empty());

  TaskInfo task;
  task.set_name("oversized");
  task.set_principal(DEFAULT_TASK_PRINCIPAL);
  task.mutable_task_id()->set_value("1");
  task.mutable_node_id()->MergeFrom(offers->at(0).node_id());
  task.mutable_resources()->MergeFrom(offers->at(0).resources());
  task.mutable_executor()->MergeFrom(DEFAULT_EXECUTOR_INFO);

  ExecutorDriver* execDriver = nullptr;
  EXPECT_CALL(exec, registered(_, _, _, _))
    .WillOnce(SaveArg<0>(&execDriver));

  EXPECT_CALL(exec, launchTask(_, _))
    .WillOnce(SendRunningWithProgress(progressAt(0.4)));

  EXPECT_CALL(containerizer, update(_, _, _))
    .WillRepeatedly(Return(Nothing()));

  Future<TaskStatus> running;
  Future<TaskStatus> finished;
  EXPECT_CALL(sched, statusUpdate(&driver, _))
    .WillOnce(FutureArg<1>(&running))
    .WillOnce(FutureArg<1>(&finished));

  driver.launchTasks(offers->at(0).id(), {task});

  AWAIT_READY(running);
  ASSERT_EQ(TASK_RUNNING, running->state()) << running->message();
  ASSERT_TRUE(execDriver != nullptr);

  TaskStatus terminal;
  terminal.mutable_task_id()->CopyFrom(task.task_id());
  terminal.set_state(TASK_FINISHED);
  terminal.mutable_progress()->CopyFrom(oversizedProgress());
  execDriver->sendStatusUpdate(terminal);

  AWAIT_READY(finished);

  // The state arrives; the progress on it does not.
  EXPECT_EQ(TASK_FINISHED, finished->state());
  EXPECT_FALSE(finished->has_progress());

  // The last report that passed is what the record holds.
  Future<Response> response = process::http::get(
      controller.get()->pid,
      "state",
      None(),
      createBasicAuthHeaders(DEFAULT_CREDENTIAL));

  AWAIT_EXPECT_RESPONSE_STATUS_EQ(OK().status, response);

  Try<JSON::Object> state = JSON::parse<JSON::Object>(response->body);
  ASSERT_SOME(state);

  Result<JSON::Number> fraction = state->find<JSON::Number>(
      "frameworks[0].completed_tasks[0].statuses[1].progress.fraction");
  ASSERT_SOME(fraction);
  EXPECT_DOUBLE_EQ(0.4, fraction->as<double>());

  EXPECT_CALL(exec, shutdown(_))
    .Times(AtMost(1));

  driver.stop();
  driver.join();
}


// A task still awaiting authorization has been delivered to nobody, so not
// even the executor it is destined for may report on it. Allowing that
// reaches the partially-killed CHECK in `__run`: the branch that handles an
// update for a pending task removes the one task named, and a task group
// that then launches with a member missing aborts the node.
//
// The group shape is the same code path with a worse consequence. It is not
// tested here: a launch group needs a real default executor, and the node
// tests that use one are in the set this environment excludes.
TEST_F(TaskProgressNodeTest, ExecutorCannotReportOnItsOwnPendingTask)
{
  Try<Owned<cluster::Controller>> controller = StartController();
  ASSERT_SOME(controller);

  ExecutorInfo executorInfo = createExecutorInfo("executor-1", "exit 1");
  MockExecutor exec(executorInfo.executor_id());
  TestContainerizer containerizer(executorInfo.executor_id(), &exec);

  MockAuthorizer authorizer;

  Owned<ControllerDetector> detector = controller.get()->createDetector();
  Try<Owned<cluster::Node>> node =
    StartNode(detector.get(), &containerizer, &authorizer);
  ASSERT_SOME(node);

  MockScheduler sched;
  MediosSchedulerDriver driver(
      &sched,
      DEFAULT_FRAMEWORK_INFO,
      controller.get()->pid,
      DEFAULT_CREDENTIAL);

  EXPECT_CALL(sched, registered(&driver, _, _));

  Future<vector<Offer>> offers;
  EXPECT_CALL(sched, resourceOffers(&driver, _))
    .WillOnce(FutureArg<1>(&offers))
    .WillRepeatedly(Return());

  driver.start();

  AWAIT_READY(offers);
  ASSERT_FALSE(offers->empty());

  // Both tasks go to the same executor. The second is held in
  // authorization, which is where a pending task lives.
  vector<TaskInfo> tasks;

  TaskInfo delivered;
  delivered.set_name("delivered");
  delivered.set_principal(DEFAULT_TASK_PRINCIPAL);
  delivered.mutable_task_id()->set_value("1");
  delivered.mutable_node_id()->MergeFrom(offers->at(0).node_id());
  delivered.mutable_resources()->MergeFrom(
      Resources::parse("cpus:1;mem:256").get());
  delivered.mutable_executor()->MergeFrom(executorInfo);
  tasks.push_back(delivered);

  TaskInfo awaiting;
  awaiting.set_name("awaiting");
  awaiting.set_principal(DEFAULT_TASK_PRINCIPAL);
  awaiting.mutable_task_id()->set_value("2");
  awaiting.mutable_node_id()->MergeFrom(offers->at(0).node_id());
  awaiting.mutable_resources()->MergeFrom(
      Resources::parse("cpus:1;mem:256").get());
  awaiting.mutable_executor()->MergeFrom(executorInfo);
  tasks.push_back(awaiting);

  Promise<bool> authorizeAwaiting;

  EXPECT_CALL(authorizer, authorized(_))
    .WillRepeatedly(Return(true));

  EXPECT_CALL(
      authorizer,
      authorized(AuthorizationRequestHasTaskID(awaiting.task_id())))
    .WillOnce(Return(authorizeAwaiting.future()));

  ExecutorDriver* execDriver = nullptr;
  EXPECT_CALL(exec, registered(_, _, _, _))
    .WillOnce(SaveArg<0>(&execDriver));
  EXPECT_CALL(exec, launchTask(_, _))
    .WillRepeatedly(SendStatusUpdateFromTask(TASK_RUNNING));

  EXPECT_CALL(containerizer, update(_, _, _))
    .WillRepeatedly(Return(Nothing()));

  Future<TaskStatus> deliveredRunning;
  EXPECT_CALL(
      sched, statusUpdate(&driver, TaskStatusTaskIdEq(delivered.task_id())))
    .WillOnce(FutureArg<1>(&deliveredRunning))
    .WillRepeatedly(Return());

  Future<TaskStatus> awaitingRunning;
  EXPECT_CALL(
      sched, statusUpdate(&driver, TaskStatusTaskIdEq(awaiting.task_id())))
    .WillOnce(FutureArg<1>(&awaitingRunning))
    .WillRepeatedly(Return());

  driver.launchTasks(offers->at(0).id(), tasks);

  AWAIT_READY(deliveredRunning);
  ASSERT_EQ(TASK_RUNNING, deliveredRunning->state())
    << deliveredRunning->message();
  ASSERT_TRUE(execDriver != nullptr);
  ASSERT_TRUE(awaitingRunning.isPending());

  // The running executor reports a terminal state for its own task that has
  // not been delivered to it.
  TaskStatus premature;
  premature.mutable_task_id()->CopyFrom(awaiting.task_id());
  premature.set_state(TASK_FAILED);
  execDriver->sendStatusUpdate(premature);

  // Its own delivered task, which the node handles after the refusal.
  TaskStatus own;
  own.mutable_task_id()->CopyFrom(delivered.task_id());
  own.set_state(TASK_FINISHED);
  execDriver->sendStatusUpdate(own);

  Future<Nothing> ownHandled =
    FUTURE_DISPATCH(node.get()->pid, &Node::___statusUpdate);
  AWAIT_READY(ownHandled);

  EXPECT_TRUE(metricEquals("node/unowned_task_status_updates", 1));

  // The pending task was not removed, so its launch still happens.
  authorizeAwaiting.set(true);

  AWAIT_READY(awaitingRunning);
  EXPECT_EQ(TASK_RUNNING, awaitingRunning->state())
    << awaitingRunning->message();

  EXPECT_CALL(exec, shutdown(_))
    .Times(AtMost(1));

  driver.stop();
  driver.join();
}


// An address the node cannot place may report on a task no executor holds,
// because an executor already torn down can still replay a terminal update.
// A task in a live executor's completed-task cache is held, by the same
// four-structure rule a resolved sender is judged by, so that exception does
// not extend to it.
TEST_F(TaskProgressNodeTest, UnplaceableSenderCannotReportOnACompletedTask)
{
  Try<Owned<cluster::Controller>> controller = StartController();
  ASSERT_SOME(controller);

  ExecutorInfo executorInfo = createExecutorInfo("executor-1", "exit 1");
  MockExecutor exec(executorInfo.executor_id());
  TestContainerizer containerizer(executorInfo.executor_id(), &exec);

  Owned<ControllerDetector> detector = controller.get()->createDetector();
  Try<Owned<cluster::Node>> node = StartNode(detector.get(), &containerizer);
  ASSERT_SOME(node);

  MockScheduler sched;
  MediosSchedulerDriver driver(
      &sched,
      DEFAULT_FRAMEWORK_INFO,
      controller.get()->pid,
      DEFAULT_CREDENTIAL);

  Future<FrameworkID> frameworkId;
  EXPECT_CALL(sched, registered(&driver, _, _))
    .WillOnce(FutureArg<1>(&frameworkId));

  Future<vector<Offer>> offers;
  EXPECT_CALL(sched, resourceOffers(&driver, _))
    .WillOnce(FutureArg<1>(&offers))
    .WillRepeatedly(Return());

  driver.start();

  AWAIT_READY(frameworkId);
  AWAIT_READY(offers);
  ASSERT_FALSE(offers->empty());

  // Two tasks under one executor: the first completes, the second keeps the
  // executor alive so the completed task still has an owner.
  vector<TaskInfo> tasks;

  TaskInfo completed;
  completed.set_name("completed");
  completed.set_principal(DEFAULT_TASK_PRINCIPAL);
  completed.mutable_task_id()->set_value("1");
  completed.mutable_node_id()->MergeFrom(offers->at(0).node_id());
  completed.mutable_resources()->MergeFrom(
      Resources::parse("cpus:1;mem:256").get());
  completed.mutable_executor()->MergeFrom(executorInfo);
  tasks.push_back(completed);

  TaskInfo lingering;
  lingering.set_name("lingering");
  lingering.set_principal(DEFAULT_TASK_PRINCIPAL);
  lingering.mutable_task_id()->set_value("2");
  lingering.mutable_node_id()->MergeFrom(offers->at(0).node_id());
  lingering.mutable_resources()->MergeFrom(
      Resources::parse("cpus:1;mem:256").get());
  lingering.mutable_executor()->MergeFrom(executorInfo);
  tasks.push_back(lingering);

  ExecutorDriver* execDriver = nullptr;
  EXPECT_CALL(exec, registered(_, _, _, _))
    .WillOnce(SaveArg<0>(&execDriver));
  EXPECT_CALL(exec, launchTask(_, _))
    .WillRepeatedly(SendStatusUpdateFromTask(TASK_RUNNING));

  EXPECT_CALL(containerizer, update(_, _, _))
    .WillRepeatedly(Return(Nothing()));

  Future<TaskStatus> completedRunning;
  Future<TaskStatus> completedFinished;
  EXPECT_CALL(
      sched, statusUpdate(&driver, TaskStatusTaskIdEq(completed.task_id())))
    .WillOnce(FutureArg<1>(&completedRunning))
    .WillOnce(FutureArg<1>(&completedFinished))
    .WillRepeatedly(Return());

  Future<TaskStatus> lingeringRunning;
  EXPECT_CALL(
      sched, statusUpdate(&driver, TaskStatusTaskIdEq(lingering.task_id())))
    .WillOnce(FutureArg<1>(&lingeringRunning))
    .WillRepeatedly(Return());

  driver.launchTasks(offers->at(0).id(), tasks);

  AWAIT_READY(completedRunning);
  AWAIT_READY(lingeringRunning);
  ASSERT_TRUE(execDriver != nullptr);

  Future<Nothing> acknowledged =
    FUTURE_DISPATCH(node.get()->pid, &Node::_statusUpdateAcknowledgement);

  TaskStatus terminal;
  terminal.mutable_task_id()->CopyFrom(completed.task_id());
  terminal.set_state(TASK_FINISHED);
  execDriver->sendStatusUpdate(terminal);

  AWAIT_READY(completedFinished);
  AWAIT_READY(acknowledged);

  // The task is only in the executor's completed-task cache once the
  // acknowledgement handler has run, which is the state under test.
  Clock::pause();
  Clock::settle();
  Clock::resume();

  const UPID forged("forged@127.0.0.1:1");

  Future<Message> atNode = FUTURE_MESSAGE(
      Eq(StatusUpdateMessage().GetTypeName()), forged, node.get()->pid);

  StatusUpdate update;
  update.mutable_framework_id()->CopyFrom(frameworkId.get());
  update.mutable_node_id()->CopyFrom(offers->at(0).node_id());
  update.mutable_executor_id()->CopyFrom(executorInfo.executor_id());
  update.set_timestamp(0);
  update.set_uuid(id::UUID::random().toBytes());

  TaskStatus* status = update.mutable_status();
  status->mutable_task_id()->CopyFrom(completed.task_id());
  status->mutable_node_id()->CopyFrom(offers->at(0).node_id());
  status->set_state(TASK_FAILED);
  status->set_uuid(update.uuid());
  status->mutable_progress()->set_fraction(0.9);

  StatusUpdateMessage message;
  message.mutable_update()->CopyFrom(update);
  message.set_pid(stringify(forged));

  process::post(forged, node.get()->pid, message);

  AWAIT_READY(atNode);

  Clock::pause();
  Clock::settle();
  Clock::resume();

  EXPECT_TRUE(metricEquals("node/unowned_task_status_updates", 1));

  EXPECT_CALL(exec, shutdown(_))
    .Times(AtMost(1));

  driver.stop();
  driver.join();
}


// The task status update manager writes the update's own protobuf to disk
// and replays it from there, so stripping a failed report only from the task
// record would leave its bytes in the checkpoint and in replay traffic. The
// update itself is cleaned before anything retains or queues it.
TEST_F(TaskProgressNodeTest, InvalidProgressNeverReachesTheCheckpoint)
{
  Try<Owned<cluster::Controller>> controller = StartController();
  ASSERT_SOME(controller);

  MockExecutor exec(DEFAULT_EXECUTOR_ID);
  TestContainerizer containerizer(&exec);

  node::Flags nodeFlags = CreateNodeFlags();

  Owned<ControllerDetector> detector = controller.get()->createDetector();
  Try<Owned<cluster::Node>> node =
    StartNode(detector.get(), &containerizer, nodeFlags);
  ASSERT_SOME(node);

  // Checkpointing is what puts the update on disk.
  FrameworkInfo frameworkInfo = DEFAULT_FRAMEWORK_INFO;
  frameworkInfo.set_checkpoint(true);

  MockScheduler sched;
  MediosSchedulerDriver driver(
      &sched, frameworkInfo, controller.get()->pid, DEFAULT_CREDENTIAL);

  EXPECT_CALL(sched, registered(&driver, _, _));

  Future<vector<Offer>> offers;
  EXPECT_CALL(sched, resourceOffers(&driver, _))
    .WillOnce(FutureArg<1>(&offers))
    .WillRepeatedly(Return());

  driver.start();

  AWAIT_READY(offers);
  ASSERT_FALSE(offers->empty());

  TaskInfo task;
  task.set_name("checkpointed");
  task.set_principal(DEFAULT_TASK_PRINCIPAL);
  task.mutable_task_id()->set_value("1");
  task.mutable_node_id()->MergeFrom(offers->at(0).node_id());
  task.mutable_resources()->MergeFrom(offers->at(0).resources());
  task.mutable_executor()->MergeFrom(DEFAULT_EXECUTOR_INFO);

  ExecutorDriver* execDriver = nullptr;
  EXPECT_CALL(exec, registered(_, _, _, _))
    .WillOnce(SaveArg<0>(&execDriver));

  EXPECT_CALL(exec, launchTask(_, _))
    .WillOnce(SendRunningWithProgress(progressAt(0.5)));

  EXPECT_CALL(containerizer, update(_, _, _))
    .WillRepeatedly(Return(Nothing()));

  Future<TaskStatus> running;
  Future<TaskStatus> second;
  EXPECT_CALL(sched, statusUpdate(&driver, _))
    .WillOnce(FutureArg<1>(&running))
    .WillOnce(FutureArg<1>(&second));

  driver.launchTasks(offers->at(0).id(), {task});

  AWAIT_READY(running);
  ASSERT_EQ(TASK_RUNNING, running->state()) << running->message();
  ASSERT_TRUE(execDriver != nullptr);

  // A report whose bytes hide in a field this version does not know.
  TaskStatus oversized;
  oversized.mutable_task_id()->CopyFrom(task.task_id());
  oversized.set_state(TASK_RUNNING);
  oversized.mutable_progress()->CopyFrom(oversizedProgress());
  execDriver->sendStatusUpdate(oversized);

  AWAIT_READY(second);
  EXPECT_EQ(TASK_RUNNING, second->state());
  EXPECT_FALSE(second->has_progress());

  // Every update file the node wrote, read as bytes.
  const string metaDir = node::paths::getMetaRootDir(nodeFlags.work_dir);

  Try<std::list<string>> files = os::find(metaDir, "task.updates");
  ASSERT_SOME(files);
  ASSERT_FALSE(files->empty()) << "no checkpointed updates under " << metaDir;

  size_t checked = 0;
  foreach (const string& file, files.get()) {
    Try<string> contents = os::read(file);
    ASSERT_SOME(contents);

    EXPECT_FALSE(strings::contains(contents.get(), string(1024, 'x')))
      << "the refused report's bytes are in " << file;

    // The report that passed is there, which also shows this is looking at
    // the right files.
    EXPECT_TRUE(strings::contains(contents.get(), "shard 3 of 10"))
      << "no valid report found in " << file;

    checked++;
  }

  EXPECT_GT(checked, 0u);

  EXPECT_CALL(exec, shutdown(_))
    .Times(AtMost(1));

  driver.stop();
  driver.join();
}

} // namespace tests {
} // namespace internal {
} // namespace medios {
