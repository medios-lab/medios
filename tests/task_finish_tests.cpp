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

// The controller's side of `TaskInfo.finish` (DR-042): validation refuses
// a finish the built-in command executor would not run and a cap the
// task's ceiling cannot hold, the finish is copied onto the `Task` the
// controller keeps, and it is rendered on the task.

#include <sstream>
#include <string>
#include <vector>

#include <gmock/gmock.h>

#include <gtest/gtest.h>

#include <process/future.hpp>
#include <process/gmock.hpp>
#include <process/owned.hpp>

#include <medios/executor.hpp>
#include <medios/medios.hpp>
#include <medios/resources.hpp>
#include <medios/scheduler.hpp>
#include <medios/type_utils.hpp>

#include <stout/duration.hpp>
#include <stout/foreach.hpp>
#include <stout/gtest.hpp>
#include <stout/json.hpp>
#include <stout/jsonify.hpp>
#include <stout/option.hpp>
#include <stout/strings.hpp>
#include <stout/try.hpp>

#include "common/http.hpp"
#include "common/protobuf_utils.hpp"
#include "common/validation.hpp"

#include "controller/validation.hpp"

#include "tests/containerizer.hpp"
#include "tests/medios.hpp"
#include "tests/utils.hpp"

using medios::controller::detector::ControllerDetector;

using medios::internal::tests::TestContainerizer;

using process::Future;
using process::Owned;

using std::string;
using std::vector;

using testing::_;
using testing::Return;

namespace medios {
namespace internal {
namespace tests {

namespace validation = medios::internal::controller::validation;


class TaskFinishTest : public MediosTest
{
protected:
  // The smallest task the controller will validate and build a record
  // from, with a finish that uploads for at most a minute.
  static TaskInfo taskWithFinish(const Option<Duration>& cap = Minutes(1))
  {
    TaskInfo task;
    task.set_name("finished");
    task.mutable_task_id()->set_value("task-1");
    task.mutable_node_id()->set_value("node-1");
    task.mutable_resources()->CopyFrom(
        Resources::parse("cpus:1;mem:128").get());
    task.mutable_command()->set_value("python train.py");
    task.set_principal("run:research-1");

    TaskFinish* finish = task.mutable_finish();
    finish->mutable_command()->set_value("upload.sh");
    if (cap.isSome()) {
      finish->mutable_cap()->set_nanoseconds(cap->ns());
    }
    return task;
  }

  static FrameworkID frameworkId()
  {
    FrameworkID id;
    id.set_value("framework-1");
    return id;
  }

  // The task as `/state` renders it, through the same writer.
  static string render(const Task& task)
  {
    std::ostringstream out;
    out << jsonify([&task](JSON::ObjectWriter* writer) {
      medios::json(writer, task);
    });
    return out.str();
  }
};


// --- Validation ---------------------------------------------------------

TEST_F(TaskFinishTest, NoFinishIsValid)
{
  TaskInfo task = taskWithFinish();
  task.clear_finish();

  EXPECT_NONE(validation::task::internal::validateFinish(task));
}


TEST_F(TaskFinishTest, ACommandWithACapIsValid)
{
  EXPECT_NONE(validation::task::internal::validateFinish(taskWithFinish()));

  // A zero `max_completion_time` is no ceiling, as the executor reads it,
  // so any cap is inside it.
  TaskInfo noCeiling = taskWithFinish(Minutes(1));
  noCeiling.mutable_max_completion_time()->set_nanoseconds(0);
  EXPECT_NONE(validation::task::internal::validateFinish(noCeiling));
}


// The cap is required and positive: there is no default.
TEST_F(TaskFinishTest, ACapIsRequiredAndPositive)
{
  Option<Error> missing =
    validation::task::internal::validateFinish(taskWithFinish(None()));
  ASSERT_SOME(missing);
  EXPECT_TRUE(strings::contains(missing->message, "'cap' is required"))
    << missing->message;

  Option<Error> zero = validation::task::internal::validateFinish(
      taskWithFinish(Duration::zero()));
  ASSERT_SOME(zero);
  EXPECT_TRUE(strings::contains(zero->message, "'cap' must be positive"))
    << zero->message;
}


// The cap is counted inside the task's ceiling: one the ceiling cannot
// hold is refused naming both.
TEST_F(TaskFinishTest, ACapInsideTheCeilingIsValidAndOneOutsideIsNot)
{
  TaskInfo inside = taskWithFinish(Minutes(1));
  inside.mutable_max_completion_time()->set_nanoseconds(Minutes(1).ns());
  EXPECT_NONE(validation::task::internal::validateFinish(inside));

  TaskInfo outside = taskWithFinish(Minutes(2));
  outside.mutable_max_completion_time()->set_nanoseconds(Minutes(1).ns());

  Option<Error> error = validation::task::internal::validateFinish(outside);
  ASSERT_SOME(error);
  EXPECT_TRUE(strings::contains(error->message, "'cap' 2mins"))
    << error->message;
  EXPECT_TRUE(strings::contains(error->message, "'max_completion_time' 1mins"))
    << error->message;
}


// A finish needs a command that names something to run.
TEST_F(TaskFinishTest, ACommandIsRequired)
{
  TaskInfo none = taskWithFinish();
  none.mutable_finish()->clear_command();

  Option<Error> missing = validation::task::internal::validateFinish(none);
  ASSERT_SOME(missing);
  EXPECT_TRUE(strings::contains(missing->message, "'command' is required"))
    << missing->message;

  // `arguments` add to `value` and never stand in for it: the launcher
  // requires `value` on both its paths.
  TaskInfo argv = taskWithFinish();
  argv.mutable_finish()->mutable_command()->clear_value();
  argv.mutable_finish()->mutable_command()->set_shell(false);
  argv.mutable_finish()->mutable_command()->add_arguments("upload");

  Option<Error> nothing = validation::task::internal::validateFinish(argv);
  ASSERT_SOME(nothing);
  EXPECT_TRUE(strings::contains(
      nothing->message, "'command.value' is required"))
    << nothing->message;
}


// Only the built-in command executor runs the hook: a task that names its
// own executor, and a docker task, are refused naming the field.
TEST_F(TaskFinishTest, AnExecutorThatDoesNotRunTheHookIsRefused)
{
  TaskInfo custom = taskWithFinish();
  custom.clear_command();
  custom.mutable_executor()->mutable_executor_id()->set_value("mine");
  custom.mutable_executor()->mutable_command()->set_value("my-executor");

  Option<Error> error = validation::task::internal::validateFinish(custom);
  ASSERT_SOME(error);
  EXPECT_TRUE(strings::contains(error->message, "Invalid finish"))
    << error->message;
  EXPECT_TRUE(strings::contains(error->message, "executor 'mine'"))
    << error->message;

  TaskInfo docker = taskWithFinish();
  docker.mutable_container()->set_type(ContainerInfo::DOCKER);
  docker.mutable_container()->mutable_docker()->set_image("busybox");

  error = validation::task::internal::validateFinish(docker);
  ASSERT_SOME(error);
  EXPECT_TRUE(strings::contains(error->message, "docker task"))
    << error->message;
}


// --- The record ---------------------------------------------------------

TEST_F(TaskFinishTest, NoFinishIsNotCopiedOrRendered)
{
  TaskInfo info = taskWithFinish();
  info.clear_finish();

  Task task = protobuf::createTask(info, TASK_RUNNING, frameworkId(), None());
  EXPECT_FALSE(task.has_finish());

  Try<JSON::Object> rendered = JSON::parse<JSON::Object>(render(task));
  ASSERT_SOME(rendered);
  EXPECT_NONE(rendered->find<JSON::Object>("finish"));
}


// The finish an outcome is read against is the one the task was launched
// with, so the controller keeps it on the record and renders it.
TEST_F(TaskFinishTest, TheFinishIsCopiedAndRendered)
{
  Task task = protobuf::createTask(
      taskWithFinish(Seconds(90)), TASK_RUNNING, frameworkId(), None());

  ASSERT_TRUE(task.has_finish());
  EXPECT_EQ("upload.sh", task.finish().command().value());
  EXPECT_EQ(Seconds(90).ns(), task.finish().cap().nanoseconds());

  Try<JSON::Object> rendered = JSON::parse<JSON::Object>(render(task));
  ASSERT_SOME(rendered);

  Result<JSON::String> command =
    rendered->find<JSON::String>("finish.command.value");
  ASSERT_SOME(command);
  EXPECT_EQ("upload.sh", command->value);

  Result<JSON::Number> cap =
    rendered->find<JSON::Number>("finish.cap.nanoseconds");
  ASSERT_SOME(cap);
  EXPECT_EQ(Seconds(90).ns(), cap->as<int64_t>());
}


// A terminal status carries the finish outcome as a typed field, and the
// status renderer carries it into `/state`.
TEST_F(TaskFinishTest, AFinishOutcomeOnAStatusIsRendered)
{
  TaskStatus status;
  status.mutable_task_id()->set_value("task-1");
  status.set_state(TASK_FINISHED);
  status.set_timestamp(1.0);
  status.mutable_finish()->set_state(FinishStatus::RAN);
  status.mutable_finish()->set_exit_code(3);
  status.mutable_finish()->mutable_limit()->set_nanoseconds(Minutes(1).ns());

  std::ostringstream out;
  out << jsonify([&status](JSON::ObjectWriter* writer) {
    medios::json(writer, status);
  });

  Try<JSON::Object> rendered = JSON::parse<JSON::Object>(out.str());
  ASSERT_SOME(rendered);

  Result<JSON::String> state = rendered->find<JSON::String>("finish.state");
  ASSERT_SOME(state);
  EXPECT_EQ("RAN", state->value);

  Result<JSON::Number> code = rendered->find<JSON::Number>("finish.exit_code");
  ASSERT_SOME(code);
  EXPECT_EQ(3, code->as<int>());
}


// Two records that differ only in the finish they were launched with are
// not equal: the finish is what a finish outcome is read against.
TEST_F(TaskFinishTest, TheFinishIsPartOfTheRecord)
{
  Task one = protobuf::createTask(
      taskWithFinish(Seconds(90)), TASK_RUNNING, frameworkId(), None());
  Task same = protobuf::createTask(
      taskWithFinish(Seconds(90)), TASK_RUNNING, frameworkId(), None());
  Task longer = protobuf::createTask(
      taskWithFinish(Seconds(91)), TASK_RUNNING, frameworkId(), None());

  TaskInfo without = taskWithFinish();
  without.clear_finish();
  Task none = protobuf::createTask(
      without, TASK_RUNNING, frameworkId(), None());

  EXPECT_TRUE(one == same);
  EXPECT_FALSE(one == longer);
  EXPECT_FALSE(one == none);
}


// The object model of a task carries the finish beside its statuses, so
// an outcome on a status can be read against the command and the cap.
TEST_F(TaskFinishTest, TheFinishIsInTheTaskModel)
{
  Task task = protobuf::createTask(
      taskWithFinish(Seconds(90)), TASK_RUNNING, frameworkId(), None());

  JSON::Object model = medios::internal::model(task);

  Result<JSON::String> command =
    model.find<JSON::String>("finish.command.value");
  ASSERT_SOME(command);
  EXPECT_EQ("upload.sh", command->value);
}


// A finish outcome on the way in must say what happened: no state, or a
// RAN with no exit, or a DID_NOT_START with no reason, is refused, so an
// unset field is never read as a hook that ran.
TEST_F(TaskFinishTest, AFinishOutcomeWithoutItsFactsIsRefused)
{
  FinishStatus unset;
  Option<Error> error =
    medios::internal::common::validation::validateFinishStatus(unset);
  ASSERT_SOME(error);
  EXPECT_TRUE(strings::contains(error->message, "`state` is required"))
    << error->message;

  FinishStatus ran;
  ran.set_state(FinishStatus::RAN);
  error = medios::internal::common::validation::validateFinishStatus(ran);
  ASSERT_SOME(error);
  EXPECT_TRUE(strings::contains(error->message, "neither `exit_code`"))
    << error->message;

  ran.set_exit_code(0);
  EXPECT_NONE(medios::internal::common::validation::validateFinishStatus(ran));

  FinishStatus none;
  none.set_state(FinishStatus::DID_NOT_START);
  error = medios::internal::common::validation::validateFinishStatus(none);
  ASSERT_SOME(error);
  EXPECT_TRUE(strings::contains(error->message, "no `reason`"))
    << error->message;

  none.set_reason("the executor is shutting down");
  EXPECT_NONE(medios::internal::common::validation::validateFinishStatus(none));

  FinishStatus timedOut;
  timedOut.set_state(FinishStatus::TIMED_OUT);
  EXPECT_NONE(
      medios::internal::common::validation::validateFinishStatus(timedOut));
}


// A finish on a task in a task group is refused by its own sentence: the
// default executor runs the group and does not run a finish hook. The
// refusal arrives from the controller for the task, before any launch.
TEST_F(TaskFinishTest, AFinishInATaskGroupIsRefusedByItsOwnSentence)
{
  Try<Owned<cluster::Controller>> controller = StartController();
  ASSERT_SOME(controller);

  Owned<ControllerDetector> detector = controller.get()->createDetector();
  Try<Owned<cluster::Node>> node = StartNode(detector.get());
  ASSERT_SOME(node);

  MockScheduler sched;
  MediosSchedulerDriver driver(
      &sched, DEFAULT_FRAMEWORK_INFO, controller.get()->pid, DEFAULT_CREDENTIAL);

  Future<FrameworkID> registered;
  EXPECT_CALL(sched, registered(&driver, _, _))
    .WillOnce(FutureArg<1>(&registered));

  Future<vector<Offer>> offers;
  EXPECT_CALL(sched, resourceOffers(&driver, _))
    .WillOnce(FutureArg<1>(&offers))
    .WillRepeatedly(Return());

  driver.start();

  AWAIT_READY(registered);
  AWAIT_READY(offers);
  ASSERT_FALSE(offers->empty());
  const Offer& offer = offers->at(0);
  const string role = offer.resources(0).allocation_info().role();

  Resources executorResources = Resources::parse("cpus:0.1;mem:32").get();
  executorResources.allocate(role);
  ExecutorInfo executor = createExecutorInfo(
      "default", None(), executorResources, ExecutorInfo::DEFAULT,
      registered.get());

  Resources taskResources = Resources::parse("cpus:0.1;mem:32").get();
  taskResources.allocate(role);
  TaskInfo task = createTask(offer.node_id(), taskResources, "sleep 1000");
  task.set_principal("run:research-1");
  task.mutable_finish()->mutable_command()->set_value("upload.sh");
  task.mutable_finish()->mutable_cap()->set_nanoseconds(Minutes(1).ns());

  Future<TaskStatus> status;
  EXPECT_CALL(sched, statusUpdate(&driver, _))
    .WillOnce(FutureArg<1>(&status))
    .WillRepeatedly(Return());

  driver.acceptOffers(
      {offer.id()}, {LAUNCH_GROUP(executor, createTaskGroupInfo({task}))});

  AWAIT_READY(status);
  EXPECT_EQ(task.task_id(), status->task_id());
  EXPECT_EQ(TASK_ERROR, status->state());
  EXPECT_EQ(TaskStatus::SOURCE_CONTROLLER, status->source());
  EXPECT_TRUE(strings::contains(status->message(), "a task in a task group"))
    << status->message();

  driver.stop();
  driver.join();
}


// A task record arriving whole from a node carries statuses this
// controller never validated: a finish outcome on one that fails
// validation is cleared, not replaced, and the clean ones stay.
TEST_F(TaskFinishTest, AMalformedFinishOutcomeOnARecordIsCleared)
{
  Task task = protobuf::createTask(
      taskWithFinish(), TASK_RUNNING, frameworkId(), None());

  TaskStatus* running = task.add_statuses();
  running->mutable_task_id()->CopyFrom(task.task_id());
  running->set_state(TASK_RUNNING);

  TaskStatus* clean = task.add_statuses();
  clean->mutable_task_id()->CopyFrom(task.task_id());
  clean->set_state(TASK_FINISHED);
  clean->mutable_finish()->set_state(FinishStatus::TIMED_OUT);

  TaskStatus* malformed = task.add_statuses();
  malformed->mutable_task_id()->CopyFrom(task.task_id());
  malformed->set_state(TASK_FINISHED);
  malformed->mutable_finish()->set_exit_code(0); // No state.

  EXPECT_EQ(
      1u, medios::internal::common::validation::sanitizeTaskFinish(&task));

  EXPECT_FALSE(task.statuses(0).has_finish());
  ASSERT_TRUE(task.statuses(1).has_finish());
  EXPECT_EQ(FinishStatus::TIMED_OUT, task.statuses(1).finish().state());
  EXPECT_FALSE(task.statuses(2).has_finish());

  EXPECT_EQ(
      0u, medios::internal::common::validation::sanitizeTaskFinish(&task));
}


// The drop is counted where it happens: an executor that reports a finish
// outcome with no state has it dropped at the node, whose counter moves,
// and the controller then sees a clean status, so its counter stays.
TEST_F(TaskFinishTest, TheDropIsCountedWhereItHappens)
{
  Try<Owned<cluster::Controller>> controller = StartController();
  ASSERT_SOME(controller);

  MockExecutor exec(DEFAULT_EXECUTOR_ID);
  TestContainerizer containerizer(&exec);

  Owned<ControllerDetector> detector = controller.get()->createDetector();
  Try<Owned<cluster::Node>> node =
    StartNode(detector.get(), &containerizer);
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

  TaskInfo task =
    createTask(offers->at(0), "sleep 1000", DEFAULT_EXECUTOR_ID);

  EXPECT_CALL(exec, registered(_, _, _, _));

  // The executor reports a terminal status whose finish has an exit code
  // and no state.
  EXPECT_CALL(exec, launchTask(_, _))
    .WillOnce(testing::Invoke(
        [](ExecutorDriver* executor, const TaskInfo& task) {
          TaskStatus status;
          status.mutable_task_id()->CopyFrom(task.task_id());
          status.set_state(TASK_FINISHED);
          status.mutable_finish()->set_exit_code(0);
          executor->sendStatusUpdate(status);
        }));

  Future<TaskStatus> status;
  EXPECT_CALL(sched, statusUpdate(&driver, _))
    .WillOnce(FutureArg<1>(&status))
    .WillRepeatedly(Return());

  driver.launchTasks(offers->at(0).id(), {task});

  AWAIT_READY(status);
  EXPECT_EQ(TASK_FINISHED, status->state());
  EXPECT_FALSE(status->has_finish());

  JSON::Object metrics = Metrics();

  Result<JSON::Number> onNode =
    metrics.find<JSON::Number>("node/invalid_task_finish_outcomes");
  ASSERT_SOME(onNode);
  EXPECT_EQ(1, onNode->as<int>());

  Result<JSON::Number> onController =
    metrics.find<JSON::Number>("controller/invalid_task_finish_outcomes");
  ASSERT_SOME(onController);
  EXPECT_EQ(0, onController->as<int>());

  EXPECT_CALL(exec, shutdown(_)).Times(testing::AtMost(1));

  driver.stop();
  driver.join();
}

} // namespace tests {
} // namespace internal {
} // namespace medios {
