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

// The controller's side of `TaskInfo.observers` (DR-041): an observer the
// executor could not run, or whose name could not tell its lines from
// another's, is refused at submission with the field named.

#include <string>
#include <vector>

#include <gmock/gmock.h>

#include <medios/executor.hpp>
#include <medios/scheduler.hpp>

#include <process/future.hpp>
#include <process/gmock.hpp>
#include <process/owned.hpp>

#include <stout/foreach.hpp>
#include <stout/gtest.hpp>
#include <stout/stringify.hpp>
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

class ControllerTaskObserverTest : public MediosTest
{
protected:
  // An observer with a command the validator never runs: it reads the
  // name and whether a command is there, and nothing else.
  static TaskObserver observer(const string& name)
  {
    TaskObserver declaration;
    declaration.set_name(name);
    declaration.mutable_command()->set_value("true");
    return declaration;
  }

  static Option<Error> validate(const vector<TaskObserver>& observers)
  {
    TaskInfo task;
    foreach (const TaskObserver& declaration, observers) {
      task.add_observers()->CopyFrom(declaration);
    }

    return validation::task::internal::validateObservers(task);
  }
};


// A task that carries no observers is not a task with an empty one.
TEST_F(ControllerTaskObserverTest, NoObserversIsValid)
{
  TaskInfo task;
  EXPECT_NONE(validation::task::internal::validateObservers(task));
}


TEST_F(ControllerTaskObserverTest, OneObserverIsValid)
{
  EXPECT_NONE(validate({observer("haiku")}));
}


TEST_F(ControllerTaskObserverTest, SeveralNamesAreValid)
{
  EXPECT_NONE(validate({observer("haiku"), observer("gpu-watch")}));
}


TEST_F(ControllerTaskObserverTest, DigitsAndDashesInANameAreValid)
{
  EXPECT_NONE(validate({observer("obs-2")}));
}


TEST_F(ControllerTaskObserverTest, NameAtTheLengthLimitIsValid)
{
  EXPECT_NONE(validate(
      {observer(string(validation::task::OBSERVER_NAME_MAX_LENGTH, 'a'))}));
}


TEST_F(ControllerTaskObserverTest, ObserversAtTheLimitAreValid)
{
  vector<TaskObserver> observers;
  for (size_t i = 0u; i < validation::task::MAX_OBSERVERS_PER_TASK; i++) {
    observers.push_back(observer("obs-" + stringify(i)));
  }

  EXPECT_NONE(validate(observers));
}


// The name prefixes the observer's lines in the task record, so an unnamed
// observer's observations could not be told from another's.
TEST_F(ControllerTaskObserverTest, NameIsRequired)
{
  TaskObserver unnamed;
  unnamed.mutable_command()->set_value("true");

  Option<Error> error = validate({unnamed});

  ASSERT_SOME(error);
  EXPECT_TRUE(strings::contains(error->message, "observers"))
    << error->message;
  EXPECT_TRUE(strings::contains(error->message, "'name' is required"))
    << error->message;
}


TEST_F(ControllerTaskObserverTest, EmptyNameIsRefused)
{
  Option<Error> error = validate({observer("")});

  ASSERT_SOME(error);
  EXPECT_TRUE(strings::contains(error->message, "'name' is required"))
    << error->message;
}


TEST_F(ControllerTaskObserverTest, UppercaseInANameIsRefused)
{
  Option<Error> error = validate({observer("Haiku")});

  ASSERT_SOME(error);
  EXPECT_TRUE(strings::contains(error->message, "[a-z0-9-]"))
    << error->message;
}


TEST_F(ControllerTaskObserverTest, PunctuationInANameIsRefused)
{
  Option<Error> error = validate({observer("haiku.watch")});

  ASSERT_SOME(error);
  EXPECT_TRUE(strings::contains(error->message, "[a-z0-9-]"))
    << error->message;
}


TEST_F(ControllerTaskObserverTest, NameOverTheLengthLimitIsRefused)
{
  Option<Error> error = validate(
      {observer(string(validation::task::OBSERVER_NAME_MAX_LENGTH + 1u, 'a'))});

  ASSERT_SOME(error);
  EXPECT_TRUE(strings::contains(error->message, "the limit is"))
    << error->message;
}


// Two observers under one name put their lines in the record under the
// same prefix, so the framework could not tell which one spoke.
TEST_F(ControllerTaskObserverTest, DuplicateNameIsRefused)
{
  Option<Error> error = validate({observer("haiku"), observer("haiku")});

  ASSERT_SOME(error);
  EXPECT_TRUE(strings::contains(error->message, "more than one observer"))
    << error->message;
}


TEST_F(ControllerTaskObserverTest, CommandIsRequired)
{
  TaskObserver commandless;
  commandless.set_name("haiku");

  Option<Error> error = validate({commandless});

  ASSERT_SOME(error);
  EXPECT_TRUE(strings::contains(error->message, "'command' is required"))
    << error->message;
}


// Each observer is a process beside the task drawing on the task's own
// allocation, and only the newest line of all of them reaches the record.
TEST_F(ControllerTaskObserverTest, MoreObserversThanTheLimitAreRefused)
{
  vector<TaskObserver> observers;
  for (size_t i = 0u; i <= validation::task::MAX_OBSERVERS_PER_TASK; i++) {
    observers.push_back(observer("obs-" + stringify(i)));
  }

  Option<Error> error = validate(observers);

  ASSERT_SOME(error);
  EXPECT_TRUE(strings::contains(error->message, "the limit is"))
    << error->message;
}


// The validator is reached from `validateTask`, so the framework sees
// TASK_ERROR from the controller with the field named.
TEST_F(ControllerTaskObserverTest, LaunchWithAnUnusableObserverIsRefused)
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
  task.add_observers()->CopyFrom(observer("haiku"));
  task.add_observers()->CopyFrom(observer("haiku"));

  Future<TaskStatus> status;
  EXPECT_CALL(sched, statusUpdate(&driver, _))
    .WillOnce(FutureArg<1>(&status));

  driver.launchTasks(offers->at(0).id(), {task});

  AWAIT_READY(status);
  EXPECT_EQ(TASK_ERROR, status->state());
  EXPECT_EQ(TaskStatus::SOURCE_CONTROLLER, status->source());
  EXPECT_EQ(TaskStatus::REASON_TASK_INVALID, status->reason());
  EXPECT_TRUE(strings::contains(status->message(), "observers"))
    << status->message();

  driver.stop();
  driver.join();
}

} // namespace tests {
} // namespace internal {
} // namespace medios {
