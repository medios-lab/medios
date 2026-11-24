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

// The task principal (DR-037): the on-behalf-of pair the controller records.
//
// Three layers, each proved on its own:
//   - validation: `TaskInfo.principal` is required and shaped `kind:name`;
//   - the local authorizer: ACT_FOR_PRINCIPAL answers "may this framework
//     principal act for this task principal", and RUN_TASK takes the task
//     principal as its subject when one is set;
//   - the controller: a launch without a principal is refused as
//     TASK_INVALID, a pair an ACL forbids as TASK_UNAUTHORIZED, and an
//     accepted launch carries both principals on status updates and /state.

#include <functional>
#include <memory>
#include <string>
#include <vector>

#include <gmock/gmock.h>

#include <gtest/gtest.h>

#include <medios/medios.hpp>

#include <medios/authorizer/acls.hpp>
#include <medios/authorizer/authorizer.hpp>

#include <process/future.hpp>
#include <process/gmock.hpp>
#include <process/gtest.hpp>
#include <process/http.hpp>
#include <process/owned.hpp>

#include <stout/foreach.hpp>
#include <stout/gtest.hpp>
#include <stout/json.hpp>
#include <stout/option.hpp>
#include <stout/stringify.hpp>
#include <stout/strings.hpp>
#include <stout/try.hpp>

#include "authorizer/local/authorizer.hpp"

#include "controller/validation.hpp"

#include "tests/cluster.hpp"
#include "tests/containerizer.hpp"
#include "tests/medios.hpp"

using medios::controller::detector::ControllerDetector;

using medios::internal::LocalAuthorizer;

using process::Future;
using process::Owned;

using process::http::OK;
using process::http::Response;

using std::shared_ptr;
using std::string;
using std::vector;

using testing::_;
using testing::AtMost;
using testing::DoAll;
using testing::Return;

namespace medios {
namespace internal {
namespace tests {

namespace validation = medios::internal::controller::validation;


// --- Validation ---------------------------------------------------------

static TaskInfo taskWithPrincipal(const Option<string>& principal)
{
  TaskInfo task;
  task.set_name("t");
  task.mutable_task_id()->set_value("1");
  task.mutable_node_id()->set_value("n");
  if (principal.isSome()) {
    task.set_principal(principal.get());
  }
  return task;
}


TEST(TaskPrincipalValidationTest, MissingPrincipalIsAnError)
{
  Option<Error> error =
    validation::task::internal::validatePrincipal(taskWithPrincipal(None()));

  ASSERT_SOME(error);
  EXPECT_TRUE(strings::contains(error->message, "no `principal`"))
    << error->message;
}


TEST(TaskPrincipalValidationTest, EmptyPrincipalIsAnError)
{
  Option<Error> error =
    validation::task::internal::validatePrincipal(taskWithPrincipal(""));

  ASSERT_SOME(error);
  EXPECT_TRUE(strings::contains(error->message, "no `principal`"))
    << error->message;
}


TEST(TaskPrincipalValidationTest, MalformedPrincipalIsAnError)
{
  // One of each way the grammar `^(operator|team|run):[A-Za-z0-9._-]+$`
  // can be missed.
  const vector<string> malformed = {
    "alice",            // no kind
    "user:alice",       // unknown kind
    "Team:research",    // kind is case sensitive
    "team:",            // empty name
    "run:a b",          // space in the name
    "run:a:b",          // a second colon
    "run:caf\xc3\xa9",  // a byte outside ASCII
  };

  foreach (const string& principal, malformed) {
    Option<Error> error = validation::task::internal::validatePrincipal(
        taskWithPrincipal(principal));

    EXPECT_SOME(error) << "accepted '" << principal << "'";
  }
}


TEST(TaskPrincipalValidationTest, WellFormedPrincipalPasses)
{
  const vector<string> wellFormed = {
    "operator:alice",
    "team:research",
    "run:research-164.s5_r3",
    "run:1",
  };

  foreach (const string& principal, wellFormed) {
    Option<Error> error = validation::task::internal::validatePrincipal(
        taskWithPrincipal(principal));

    EXPECT_NONE(error) << "refused '" << principal << "': "
                       << error->message;
  }
}


TEST(TaskPrincipalValidationTest, NameAtTheLengthLimitPasses)
{
  const string name(validation::task::PRINCIPAL_NAME_MAX_LENGTH, 'a');

  Option<Error> error = validation::task::internal::validatePrincipal(
      taskWithPrincipal("run:" + name));

  EXPECT_NONE(error) << error->message;
}


TEST(TaskPrincipalValidationTest, NameOverTheLengthLimitIsAnError)
{
  const string name(validation::task::PRINCIPAL_NAME_MAX_LENGTH + 1, 'a');

  Option<Error> error = validation::task::internal::validatePrincipal(
      taskWithPrincipal("run:" + name));

  ASSERT_SOME(error);
  EXPECT_TRUE(strings::contains(error->message, "`principal`"))
    << error->message;
  EXPECT_TRUE(strings::contains(
      error->message,
      stringify(validation::task::PRINCIPAL_NAME_MAX_LENGTH)))
    << error->message;
}


// #537: FrameworkInfo.principal is required at SUBSCRIBE so the controller can
// stamp Task.framework_principal on every launched task.
TEST(TaskPrincipalValidationTest, MissingFrameworkPrincipalIsAnError)
{
  FrameworkInfo framework = DEFAULT_FRAMEWORK_INFO;
  framework.clear_principal();

  Option<Error> error = validation::framework::validate(framework);
  ASSERT_SOME(error);
  EXPECT_TRUE(strings::contains(error->message, "FrameworkInfo.principal"))
    << error->message;

  framework.set_principal("");
  error = validation::framework::validate(framework);
  ASSERT_SOME(error);
  EXPECT_TRUE(strings::contains(error->message, "FrameworkInfo.principal"))
    << error->message;

  // #697: A node re-registering with a framework that subscribed before
  // `FrameworkInfo.principal` became mandatory must not be refused.
  framework.clear_principal();
  EXPECT_NONE(validation::framework::validate(framework, false));

  ReregisterNodeMessage reregister;
  reregister.mutable_node()->set_hostname("localhost");
  reregister.mutable_node()->mutable_id()->set_value("node-1");
  *reregister.add_frameworks() = framework;
  EXPECT_NONE(validation::controller::message::reregisterNode(reregister));

  // When the recovered principalless framework's scheduler reconnects with a
  // principal, `validateUpdate` allows adopting the principal once (#697),
  // while still rejecting any subsequent principal change.
  FrameworkInfo reconnected = framework;
  reconnected.set_principal("team:cue");
  EXPECT_NONE(validation::framework::validateUpdate(framework, reconnected));

  FrameworkInfo changedAgain = reconnected;
  changedAgain.set_principal("team:other");
  EXPECT_SOME(validation::framework::validateUpdate(reconnected, changedAgain));
}


// --- Local authorizer ---------------------------------------------------

class TaskPrincipalAuthorizerTest : public MediosTest
{
protected:
  // Asks the local authorizer built from `acls` whether `frameworkPrincipal`
  // may perform `action` on `task` launched by `framework`.
  static Try<bool> approved(
      const ACLs& acls,
      const string& frameworkPrincipal,
      const authorization::Action& action,
      const TaskInfo& task,
      const FrameworkInfo& framework)
  {
    Try<Authorizer*> create = LocalAuthorizer::create(acls);
    if (create.isError()) {
      return Error(create.error());
    }
    Owned<Authorizer> authorizer(create.get());

    authorization::Subject subject;
    subject.set_value(frameworkPrincipal);

    Future<shared_ptr<const ObjectApprover>> approver =
      authorizer->getApprover(subject, action);

    approver.await();
    if (!approver.isReady()) {
      return Error("approver not ready: " + approver.failure());
    }

    return approver.get()->approved(ObjectApprover::Object(task, framework));
  }

  static FrameworkInfo frameworkWithPrincipal(const string& principal)
  {
    FrameworkInfo framework = DEFAULT_FRAMEWORK_INFO;
    framework.set_principal(principal);
    return framework;
  }
};


TEST_F(TaskPrincipalAuthorizerTest, ActForAllowedAndDeniedByAcl)
{
  ACLs acls;
  acls.set_permissive(false);

  // "test-framework" may act for "run:r1" and nothing else.
  ACL::ActForPrincipal* acl = acls.add_act_for_principals();
  acl->mutable_principals()->add_values("test-framework");
  acl->mutable_task_principals()->add_values("run:r1");

  const FrameworkInfo framework = frameworkWithPrincipal("test-framework");

  EXPECT_SOME_TRUE(approved(
      acls,
      "test-framework",
      authorization::ACT_FOR_PRINCIPAL,
      taskWithPrincipal("run:r1"),
      framework));

  EXPECT_SOME_FALSE(approved(
      acls,
      "test-framework",
      authorization::ACT_FOR_PRINCIPAL,
      taskWithPrincipal("run:r2"),
      framework));

  EXPECT_SOME_FALSE(approved(
      acls,
      "other-framework",
      authorization::ACT_FOR_PRINCIPAL,
      taskWithPrincipal("run:r1"),
      frameworkWithPrincipal("other-framework")));
}


// With no ACL entries and the default `permissive = true`, every pair is
// allowed. This is the policy of a controller started without `--acls`.
TEST_F(TaskPrincipalAuthorizerTest, ActForOpenByDefault)
{
  const FrameworkInfo framework = frameworkWithPrincipal("test-framework");

  EXPECT_SOME_TRUE(approved(
      ACLs(),
      "test-framework",
      authorization::ACT_FOR_PRINCIPAL,
      taskWithPrincipal("operator:alice"),
      framework));
}


// When the task carries a principal, `run_tasks` is asked of that principal,
// not of the framework's; without one it is asked of the framework's, as
// before.
TEST_F(TaskPrincipalAuthorizerTest, RunTaskSubjectIsTheTaskPrincipal)
{
  ACLs acls;
  acls.set_permissive(false);

  ACL::RunTask* acl = acls.add_run_tasks();
  acl->mutable_principals()->add_values("run:r1");
  acl->mutable_users()->add_values("nobody");

  const FrameworkInfo framework = frameworkWithPrincipal("test-framework");

  TaskInfo task = taskWithPrincipal("run:r1");
  task.mutable_command()->set_value("true");
  task.mutable_command()->set_user("nobody");

  EXPECT_SOME_TRUE(approved(
      acls, "test-framework", authorization::RUN_TASK, task, framework));

  // Another task principal has no run_tasks entry.
  task.set_principal("run:r2");
  EXPECT_SOME_FALSE(approved(
      acls, "test-framework", authorization::RUN_TASK, task, framework));

  // No task principal: the subject is the framework principal, which has
  // no run_tasks entry either.
  task.clear_principal();
  EXPECT_SOME_FALSE(approved(
      acls, "test-framework", authorization::RUN_TASK, task, framework));

  // An entry for the framework principal does not cover a task that names
  // a principal of its own.
  ACL::RunTask* frameworkAcl = acls.add_run_tasks();
  frameworkAcl->mutable_principals()->add_values("test-framework");
  frameworkAcl->mutable_users()->add_values("nobody");

  EXPECT_SOME_TRUE(approved(
      acls, "test-framework", authorization::RUN_TASK, task, framework));

  task.set_principal("run:r3");
  EXPECT_SOME_FALSE(approved(
      acls, "test-framework", authorization::RUN_TASK, task, framework));
}


// --- Controller ---------------------------------------------------------

class TaskPrincipalControllerTest : public MediosTest
{
protected:
  // Launches `task` through a scheduler driver registered as
  // DEFAULT_FRAMEWORK_INFO (principal DEFAULT_CREDENTIAL.principal()) against
  // a controller started with `flags`, and returns the first status update.
  // The node runs a mock executor so an accepted task reaches TASK_RUNNING.
  void launchAndAwaitStatus(
      const controller::Flags& flags,
      const std::function<void(TaskInfo*)>& shape,
      TaskStatus* out,
      bool readState = false)
  {
    Try<Owned<cluster::Controller>> controller = StartController(flags);
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
    task.set_name("principal-test");
    task.mutable_task_id()->set_value("1");
    task.mutable_node_id()->MergeFrom(offers.get()[0].node_id());
    task.mutable_resources()->MergeFrom(offers.get()[0].resources());
    task.mutable_executor()->MergeFrom(DEFAULT_EXECUTOR_INFO);
    shape(&task);

    EXPECT_CALL(exec, registered(_, _, _, _))
      .Times(AtMost(1));

    EXPECT_CALL(exec, launchTask(_, _))
      .WillRepeatedly(SendStatusUpdateFromTask(TASK_RUNNING));

    EXPECT_CALL(containerizer, update(_, _, _))
      .WillRepeatedly(Return(Nothing()));

    Future<TaskStatus> status;
    EXPECT_CALL(sched, statusUpdate(&driver, _))
      .WillOnce(FutureArg<1>(&status))
      .WillRepeatedly(Return());

    driver.launchTasks(offers.get()[0].id(), {task});

    AWAIT_READY(status);
    *out = status.get();

    if (readState && status->state() == TASK_RUNNING) {
      // Read /state while the task is live, then let the driver go.
      Future<Response> response = process::http::get(
          controller.get()->pid,
          "state",
          None(),
          createBasicAuthHeaders(DEFAULT_CREDENTIAL));

      AWAIT_EXPECT_RESPONSE_STATUS_EQ(OK().status, response);

      Try<JSON::Object> parse = JSON::parse<JSON::Object>(response->body);
      ASSERT_SOME(parse);
      stateJson = parse.get();
    }

    EXPECT_CALL(exec, shutdown(_))
      .Times(AtMost(1));

    driver.stop();
    driver.join();
  }

  Option<JSON::Object> stateJson;
};


TEST_F(TaskPrincipalControllerTest, LaunchWithoutPrincipalIsInvalid)
{
  TaskStatus status;
  launchAndAwaitStatus(
      CreateControllerFlags(), [](TaskInfo*) {}, &status);

  EXPECT_EQ(TASK_ERROR, status.state());
  EXPECT_EQ(TaskStatus::SOURCE_CONTROLLER, status.source());
  EXPECT_EQ(TaskStatus::REASON_TASK_INVALID, status.reason());
  EXPECT_TRUE(strings::contains(status.message(), "principal"))
    << status.message();
  EXPECT_FALSE(status.has_principal());
  EXPECT_FALSE(status.has_framework_principal());
}


TEST_F(TaskPrincipalControllerTest, LaunchWithMalformedPrincipalIsInvalid)
{
  TaskStatus status;
  launchAndAwaitStatus(
      CreateControllerFlags(),
      [](TaskInfo* task) { task->set_principal("alice"); },
      &status);

  EXPECT_EQ(TASK_ERROR, status.state());
  EXPECT_EQ(TaskStatus::SOURCE_CONTROLLER, status.source());
  EXPECT_EQ(TaskStatus::REASON_TASK_INVALID, status.reason());
  EXPECT_TRUE(strings::contains(status.message(), "kind:name"))
    << status.message();
}


TEST_F(TaskPrincipalControllerTest, LaunchWithOverLongPrincipalIsInvalid)
{
  const string name(validation::task::PRINCIPAL_NAME_MAX_LENGTH + 1, 'a');

  TaskStatus status;
  launchAndAwaitStatus(
      CreateControllerFlags(),
      [&name](TaskInfo* task) { task->set_principal("run:" + name); },
      &status);

  EXPECT_EQ(TASK_ERROR, status.state());
  EXPECT_EQ(TaskStatus::SOURCE_CONTROLLER, status.source());
  EXPECT_EQ(TaskStatus::REASON_TASK_INVALID, status.reason());
  EXPECT_TRUE(strings::contains(
      status.message(),
      stringify(validation::task::PRINCIPAL_NAME_MAX_LENGTH)))
    << status.message();
}


TEST_F(TaskPrincipalControllerTest, LaunchRecordsBothPrincipals)
{
  TaskStatus status;
  launchAndAwaitStatus(
      CreateControllerFlags(),
      [](TaskInfo* task) { task->set_principal("run:research-1"); },
      &status,
      true);

  ASSERT_EQ(TASK_RUNNING, status.state()) << status.message();
  EXPECT_EQ("run:research-1", status.principal());
  EXPECT_EQ(DEFAULT_CREDENTIAL.principal(), status.framework_principal());

  ASSERT_SOME(stateJson);

  Result<JSON::String> principal = stateJson->find<JSON::String>(
      "frameworks[0].tasks[0].principal");
  EXPECT_SOME_EQ(JSON::String("run:research-1"), principal);

  Result<JSON::String> frameworkPrincipal = stateJson->find<JSON::String>(
      "frameworks[0].tasks[0].framework_principal");
  EXPECT_SOME_EQ(
      JSON::String(DEFAULT_CREDENTIAL.principal()), frameworkPrincipal);
}


// An explicit ActForPrincipal entry that lets the test framework act for
// nobody turns the launch into TASK_UNAUTHORIZED; everything else stays
// permissive, so registration and the RUN_TASK check still pass.
TEST_F(TaskPrincipalControllerTest, LaunchDeniedByActForAcl)
{
  controller::Flags flags = CreateControllerFlags();
  ACL::ActForPrincipal* acl = flags.acls->add_act_for_principals();
  acl->mutable_principals()->add_values(DEFAULT_CREDENTIAL.principal());
  acl->mutable_task_principals()->set_type(ACL::Entity::NONE);

  TaskStatus status;
  launchAndAwaitStatus(
      flags,
      [](TaskInfo* task) { task->set_principal("run:research-1"); },
      &status);

  EXPECT_EQ(TASK_ERROR, status.state());
  EXPECT_EQ(TaskStatus::SOURCE_CONTROLLER, status.source());
  EXPECT_EQ(TaskStatus::REASON_TASK_UNAUTHORIZED, status.reason());
  EXPECT_TRUE(strings::contains(status.message(), "act for principal"))
    << status.message();
}

} // namespace tests {
} // namespace internal {
} // namespace medios {
