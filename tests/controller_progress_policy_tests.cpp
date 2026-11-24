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

// The controller's side of `TaskInfo.progress_policy` (DR-040): an
// unusable setting is refused at submission, naming the field, rather
// than reaching an executor that reads something the framework did not
// ask for.

#include <string>
#include <vector>

#include <gmock/gmock.h>

#include <medios/executor.hpp>
#include <medios/scheduler.hpp>

#include <process/future.hpp>
#include <process/gmock.hpp>
#include <process/owned.hpp>

#include <stout/duration.hpp>
#include <stout/gtest.hpp>
#include <stout/lambda.hpp>
#include <stout/strings.hpp>

#include "controller/validation.hpp"

#include "tests/medios.hpp"

using medios::controller::detector::ControllerDetector;

using process::Future;
using process::Owned;

using std::string;
using std::vector;

using testing::_;
using testing::Return;

namespace validation = medios::internal::controller::validation;

namespace medios {
namespace internal {
namespace tests {

class ControllerProgressPolicyTest : public MediosTest
{
protected:
  static void setDuration(DurationInfo* duration, const Duration& value)
  {
    duration->set_nanoseconds(value.ns());
  }

  // A task whose policy the caller shapes. The rest of the task is
  // unset: `validateProgressPolicy` reads nothing else.
  static TaskInfo taskWithPolicy(
      const lambda::function<void(ProgressPolicy*)>& shape)
  {
    TaskInfo task;
    shape(task.mutable_progress_policy());
    return task;
  }

  static Option<Error> validate(
      const lambda::function<void(ProgressPolicy*)>& shape)
  {
    return validation::task::internal::validateProgressPolicy(
        taskWithPolicy(shape));
  }
};


// A task that carries no policy is not a task with an empty one.
TEST_F(ControllerProgressPolicyTest, AbsentPolicyIsValid)
{
  TaskInfo task;
  EXPECT_NONE(validation::task::internal::validateProgressPolicy(task));
}


TEST_F(ControllerProgressPolicyTest, StdoutSourceAloneIsValid)
{
  EXPECT_NONE(validate([](ProgressPolicy* policy) {
    policy->set_source(ProgressPolicy::STDOUT);
  }));
}


TEST_F(ControllerProgressPolicyTest, FullPolicyIsValid)
{
  EXPECT_NONE(validate([](ProgressPolicy* policy) {
    policy->set_source(ProgressPolicy::FILE);
    policy->set_path("logs/progress");
    policy->set_pattern("progress: ([0-9]+), (.*)");
    setDuration(policy->mutable_sample_interval(), Milliseconds(500));
    setDuration(policy->mutable_stall_window(), Seconds(120));
    policy->set_action(ProgressPolicy::KILL);
  }));
}


// A guessed source reports another file's numbers as this task's
// progress, so the source is named or nothing is read.
TEST_F(ControllerProgressPolicyTest, SourceIsRequired)
{
  Option<Error> error = validate([](ProgressPolicy* policy) {
    setDuration(policy->mutable_stall_window(), Seconds(10));
    policy->set_action(ProgressPolicy::REPORT);
  });

  ASSERT_SOME(error);
  EXPECT_TRUE(strings::contains(error->message, "'source' is required"))
    << error->message;
}


TEST_F(ControllerProgressPolicyTest, FileSourceRequiresAPath)
{
  Option<Error> error = validate([](ProgressPolicy* policy) {
    policy->set_source(ProgressPolicy::FILE);
  });

  ASSERT_SOME(error);
  EXPECT_TRUE(strings::contains(error->message, "'path' is required"))
    << error->message;
}


TEST_F(ControllerProgressPolicyTest, EmptyPathIsRefused)
{
  Option<Error> error = validate([](ProgressPolicy* policy) {
    policy->set_source(ProgressPolicy::FILE);
    policy->set_path("");
  });

  ASSERT_SOME(error);
  EXPECT_TRUE(strings::contains(error->message, "'path' is required"))
    << error->message;
}


TEST_F(ControllerProgressPolicyTest, AbsolutePathIsRefused)
{
  Option<Error> error = validate([](ProgressPolicy* policy) {
    policy->set_source(ProgressPolicy::FILE);
    policy->set_path("/var/log/messages");
  });

  ASSERT_SOME(error);
  EXPECT_TRUE(strings::contains(error->message, "relative")) << error->message;
}


TEST_F(ControllerProgressPolicyTest, PathLeavingTheSandboxIsRefused)
{
  Option<Error> error = validate([](ProgressPolicy* policy) {
    policy->set_source(ProgressPolicy::FILE);
    policy->set_path("logs/../../etc/passwd");
  });

  ASSERT_SOME(error);
  EXPECT_TRUE(strings::contains(error->message, "sandbox")) << error->message;
}


// A path the executor will not read is a setting the framework believes
// is in force and is not.
TEST_F(ControllerProgressPolicyTest, PathWithTheStdoutSourceIsRefused)
{
  Option<Error> error = validate([](ProgressPolicy* policy) {
    policy->set_source(ProgressPolicy::STDOUT);
    policy->set_path("logs/progress");
  });

  ASSERT_SOME(error);
  EXPECT_TRUE(strings::contains(error->message, "STDOUT")) << error->message;
}


TEST_F(ControllerProgressPolicyTest, ZeroSampleIntervalIsRefused)
{
  Option<Error> error = validate([](ProgressPolicy* policy) {
    policy->set_source(ProgressPolicy::STDOUT);
    setDuration(policy->mutable_sample_interval(), Duration::zero());
  });

  ASSERT_SOME(error);
  EXPECT_TRUE(
      strings::contains(error->message, "'sample_interval' must be positive"))
    << error->message;
}


TEST_F(ControllerProgressPolicyTest, NegativeSampleIntervalIsRefused)
{
  Option<Error> error = validate([](ProgressPolicy* policy) {
    policy->set_source(ProgressPolicy::STDOUT);
    setDuration(policy->mutable_sample_interval(), Milliseconds(-1));
  });

  ASSERT_SOME(error);
  EXPECT_TRUE(
      strings::contains(error->message, "'sample_interval' must be positive"))
    << error->message;
}


TEST_F(ControllerProgressPolicyTest, ZeroStallWindowIsRefused)
{
  Option<Error> error = validate([](ProgressPolicy* policy) {
    policy->set_source(ProgressPolicy::STDOUT);
    setDuration(policy->mutable_stall_window(), Duration::zero());
    policy->set_action(ProgressPolicy::KILL);
  });

  ASSERT_SOME(error);
  EXPECT_TRUE(
      strings::contains(error->message, "'stall_window' must be positive"))
    << error->message;
}


// Kill-on-stall and report-only are different outcomes for the
// framework, so the window never implies either one.
TEST_F(ControllerProgressPolicyTest, StallWindowRequiresAnAction)
{
  Option<Error> error = validate([](ProgressPolicy* policy) {
    policy->set_source(ProgressPolicy::STDOUT);
    setDuration(policy->mutable_stall_window(), Seconds(120));
  });

  ASSERT_SOME(error);
  EXPECT_TRUE(strings::contains(error->message, "'action' is required"))
    << error->message;
}


TEST_F(ControllerProgressPolicyTest, KillActionRequiresAStallWindow)
{
  Option<Error> error = validate([](ProgressPolicy* policy) {
    policy->set_source(ProgressPolicy::STDOUT);
    policy->set_action(ProgressPolicy::KILL);
  });

  ASSERT_SOME(error);
  EXPECT_TRUE(strings::contains(error->message, "without 'stall_window'"))
    << error->message;
}


TEST_F(ControllerProgressPolicyTest, ReportActionRequiresAStallWindow)
{
  Option<Error> error = validate([](ProgressPolicy* policy) {
    policy->set_source(ProgressPolicy::STDOUT);
    policy->set_action(ProgressPolicy::REPORT);
  });

  ASSERT_SOME(error);
  EXPECT_TRUE(strings::contains(error->message, "without 'stall_window'"))
    << error->message;
}


TEST_F(ControllerProgressPolicyTest, PatternThatDoesNotCompileIsRefused)
{
  Option<Error> error = validate([](ProgressPolicy* policy) {
    policy->set_source(ProgressPolicy::STDOUT);
    policy->set_pattern("progress: ([0-9]+");
  });

  ASSERT_SOME(error);
  EXPECT_TRUE(strings::contains(error->message, "does not compile"))
    << error->message;
}


// Capture group one is the work value, so a pattern without one matches
// lines and reads nothing from them.
TEST_F(ControllerProgressPolicyTest, PatternWithoutACaptureGroupIsRefused)
{
  Option<Error> error = validate([](ProgressPolicy* policy) {
    policy->set_source(ProgressPolicy::STDOUT);
    policy->set_pattern("progress: [0-9]+");
  });

  ASSERT_SOME(error);
  EXPECT_TRUE(strings::contains(error->message, "no capture group"))
    << error->message;
}


// The validator is reached from `validateTask`, so the framework sees
// TASK_ERROR from the controller with the field named.
TEST_F(ControllerProgressPolicyTest, LaunchWithAnUnusablePolicyIsRefused)
{
  Try<Owned<cluster::Controller>> controller = StartController();
  ASSERT_SOME(controller);

  Owned<ControllerDetector> detector = controller.get()->createDetector();
  Try<Owned<cluster::Node>> node = StartNode(detector.get());
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

  TaskInfo task = createTask(offers->at(0), "sleep 1000");
  ProgressPolicy* policy = task.mutable_progress_policy();
  policy->set_source(ProgressPolicy::FILE);
  policy->set_path("../../etc/passwd");

  Future<TaskStatus> status;
  EXPECT_CALL(sched, statusUpdate(&driver, _))
    .WillOnce(FutureArg<1>(&status));

  driver.launchTasks(offers->at(0).id(), {task});

  AWAIT_READY(status);
  EXPECT_EQ(TASK_ERROR, status->state());
  EXPECT_EQ(TaskStatus::SOURCE_CONTROLLER, status->source());
  EXPECT_EQ(TaskStatus::REASON_TASK_INVALID, status->reason());
  EXPECT_TRUE(strings::contains(status->message(), "progress_policy"))
    << status->message();
  EXPECT_TRUE(strings::contains(status->message(), "sandbox"))
    << status->message();

  driver.stop();
  driver.join();
}

} // namespace tests {
} // namespace internal {
} // namespace medios {
