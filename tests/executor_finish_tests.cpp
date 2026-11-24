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

// The finish hook (DR-042): what the hook is told, how long it may run
// and how its outcome is reported, on their own; then the command
// executor running one after the work command, through a controller and
// a node, for each outcome the record can carry.

#include <sys/wait.h>

#include <functional>
#include <list>
#include <memory>
#include <string>
#include <vector>

#include <gmock/gmock.h>

#include <gtest/gtest.h>

#include <medios/medios.hpp>

#include <process/clock.hpp>
#include <process/future.hpp>
#include <process/gmock.hpp>
#include <process/gtest.hpp>
#include <process/owned.hpp>
#include <process/reap.hpp>

#include <stout/duration.hpp>
#include <stout/foreach.hpp>
#include <stout/gtest.hpp>
#include <stout/hashmap.hpp>
#include <stout/option.hpp>
#include <stout/os.hpp>
#include <stout/path.hpp>
#include <stout/stringify.hpp>
#include <stout/strings.hpp>
#include <stout/try.hpp>

#include "launcher/finish.hpp"

#include "controller/detector/standalone.hpp"

#include "node/containerizer/medios/containerizer.hpp"
#include "node/flags.hpp"

#include "tests/medios.hpp"

using medios::controller::detector::ControllerDetector;

using medios::internal::node::MediosContainerizer;

using process::Future;
using process::Owned;

using std::string;
using std::vector;

using testing::_;
using testing::Return;
using testing::WithParamInterface;

namespace medios {
namespace internal {
namespace tests {

// --- The facts, without a process ---------------------------------------

// A wait status reaches the hook as the shell's `$?` would report it.
TEST(FinishTest, WorkExitStatusIsTheShellsConvention)
{
  EXPECT_EQ(0, finish::workExitStatus(W_EXITCODE(0, 0)));
  EXPECT_EQ(3, finish::workExitStatus(W_EXITCODE(3, 0)));
  EXPECT_EQ(128 + SIGTERM, finish::workExitStatus(W_EXITCODE(0, SIGTERM)));
  EXPECT_EQ(128 + SIGKILL, finish::workExitStatus(W_EXITCODE(0, SIGKILL)));
}


// The hook gets the executor's environment, then the command's on top of
// it, then the four variables on top of both; an earlier value of one of
// the four, from either source, does not survive.
TEST(FinishTest, TheHookEnvironmentIsTheExecutorsThenTheCommandsThenTheFour)
{
  Environment executors;
  Environment::Variable* path = executors.add_variables();
  path->set_name("PATH");
  path->set_type(Environment::Variable::VALUE);
  path->set_value("/usr/bin");
  Environment::Variable* stale = executors.add_variables();
  stale->set_name("MEDIOS_WORK_EXIT_STATUS");
  stale->set_type(Environment::Variable::VALUE);
  stale->set_value("99");

  TaskID taskId;
  taskId.set_value("t-1");
  FrameworkID frameworkId;
  frameworkId.set_value("f-1");

  // The command's variables override the executor's and the four override
  // the command's.
  Environment commands;
  Environment::Variable* bucket = commands.add_variables();
  bucket->set_name("BUCKET");
  bucket->set_type(Environment::Variable::VALUE);
  bucket->set_value("results");
  Environment::Variable* ownPath = commands.add_variables();
  ownPath->set_name("PATH");
  ownPath->set_type(Environment::Variable::VALUE);
  ownPath->set_value("/opt/hook/bin");
  Environment::Variable* forged = commands.add_variables();
  forged->set_name("MEDIOS_TASK_ID");
  forged->set_type(Environment::Variable::VALUE);
  forged->set_value("not-this");

  Environment hook = finish::hookEnvironment(
      executors, commands, W_EXITCODE(2, 0), taskId, frameworkId, "n-1");

  hashmap<string, string> variables;
  foreach (const Environment::Variable& variable, hook.variables()) {
    EXPECT_FALSE(variables.contains(variable.name())) << variable.name();
    variables[variable.name()] = variable.value();
  }

  EXPECT_EQ("/opt/hook/bin", variables["PATH"]);
  EXPECT_EQ("results", variables["BUCKET"]);
  EXPECT_EQ("2", variables["MEDIOS_WORK_EXIT_STATUS"]);
  EXPECT_EQ("t-1", variables["MEDIOS_TASK_ID"]);
  EXPECT_EQ("f-1", variables["MEDIOS_FRAMEWORK_ID"]);
  EXPECT_EQ("n-1", variables["MEDIOS_NODE_ID"]);
}


// The limit is the cap, or what the ceiling has left minus the kill grace
// period, whichever is smaller, so SIGKILL lands no later than the
// deadline; nothing positive left means no start.
TEST(FinishTest, TheLimitIsInsideTheCeilingLessTheGrace)
{
  const Duration grace = Seconds(3);
  EXPECT_SOME_EQ(Minutes(1), finish::limitFor(Minutes(1), None(), grace));
  EXPECT_SOME_EQ(
      Minutes(1), finish::limitFor(Minutes(1), Minutes(5), grace));
  EXPECT_SOME_EQ(
      Seconds(7), finish::limitFor(Minutes(1), Seconds(10), grace));
  EXPECT_NONE(finish::limitFor(Minutes(1), Seconds(3), grace));
  EXPECT_NONE(finish::limitFor(Minutes(1), Seconds(2), grace));
  EXPECT_NONE(finish::limitFor(Minutes(1), Duration::zero(), grace));
}


// Each outcome is typed: how it ended, under what bound, for how long.
TEST(FinishTest, OutcomesAreTyped)
{
  FinishStatus ran = finish::ran(W_EXITCODE(4, 0), Minutes(1), Seconds(2));
  EXPECT_EQ(FinishStatus::RAN, ran.state());
  EXPECT_EQ(4, ran.exit_code());
  EXPECT_FALSE(ran.has_signal());
  EXPECT_EQ(Minutes(1).ns(), ran.limit().nanoseconds());
  EXPECT_EQ(Seconds(2).ns(), ran.duration().nanoseconds());

  FinishStatus signalled =
    finish::ran(W_EXITCODE(0, SIGTERM), Minutes(1), Seconds(2));
  EXPECT_EQ(FinishStatus::RAN, signalled.state());
  EXPECT_FALSE(signalled.has_exit_code());
  EXPECT_EQ(SIGTERM, signalled.signal());

  EXPECT_EQ(
      FinishStatus::TIMED_OUT,
      finish::timedOut(Minutes(1), Minutes(1)).state());
  EXPECT_EQ(
      FinishStatus::KILLED, finish::killed(Minutes(1), Seconds(3)).state());

  FinishStatus none = finish::didNotStart("the executor is shutting down");
  EXPECT_EQ(FinishStatus::DID_NOT_START, none.state());
  EXPECT_EQ("the executor is shutting down", none.reason());
  EXPECT_FALSE(none.has_limit());

  FinishStatus lost = finish::lost("no status", Minutes(1), Seconds(2));
  EXPECT_EQ(FinishStatus::LOST, lost.state());
  EXPECT_EQ("no status", lost.reason());
  EXPECT_EQ(Minutes(1).ns(), lost.limit().nanoseconds());
  EXPECT_EQ(Seconds(2).ns(), lost.duration().nanoseconds());
}


// --- The command executor ------------------------------------------------

// Each test starts a controller and a node with the medios containerizer,
// the POSIX launcher and no isolator (a forked process and its sandbox are
// all the hook needs), launches one task with a finish, and reads the
// terminal status. A file under the test's own directory carries what the
// hook saw, since the sandbox is the node's.
class CommandExecutorFinishTest
  : public MediosTest,
    public WithParamInterface<bool>
{
protected:
  node::Flags finishNodeFlags()
  {
    node::Flags flags = CreateNodeFlags();
    flags.http_command_executor = GetParam();
    flags.isolation = "";
    flags.launcher = "posix";
    return flags;
  }

  static void setFinish(
      TaskInfo* task,
      const string& command,
      const Duration& cap)
  {
    task->mutable_finish()->mutable_command()->set_shell(true);
    task->mutable_finish()->mutable_command()->set_value(command);
    task->mutable_finish()->mutable_cap()->set_nanoseconds(cap.ns());
  }

  // Runs one task to its terminal status, into `terminal`, with
  // `whileRunning` given the driver and the task once the task is
  // running, for a kill. Void so the awaits can fail the test.
  void runToTerminal(
      const TaskInfo& prototype,
      TaskStatus* terminal,
      const std::function<void(MediosSchedulerDriver&, const TaskInfo&)>&
        whileRunning = nullptr)
  {
    Try<Owned<cluster::Controller>> controller = StartController();
    CHECK_SOME(controller);

    Owned<ControllerDetector> detector = controller.get()->createDetector();

    node::Flags flags = finishNodeFlags();

    Try<MediosContainerizer*> _containerizer =
      MediosContainerizer::create(flags, false);
    CHECK_SOME(_containerizer);
    Owned<MediosContainerizer> containerizer(_containerizer.get());

    Try<Owned<cluster::Node>> node =
      StartNode(detector.get(), containerizer.get(), flags);
    CHECK_SOME(node);

    MockScheduler sched;
    MediosSchedulerDriver driver(
        &sched, DEFAULT_FRAMEWORK_INFO, controller.get()->pid,
        DEFAULT_CREDENTIAL);

    EXPECT_CALL(sched, registered(&driver, _, _));

    Future<vector<Offer>> offers;
    EXPECT_CALL(sched, resourceOffers(&driver, _))
      .WillOnce(FutureArg<1>(&offers))
      .WillRepeatedly(Return());

    driver.start();

    AWAIT_READY(offers);
    CHECK_EQ(1u, offers->size());

    TaskInfo task = prototype;
    task.mutable_node_id()->CopyFrom(offers->front().node_id());
    task.mutable_resources()->CopyFrom(offers->front().resources());

    // Promises rather than reassigned futures: an await copies the future
    // it is given, and a future assigned later is a different one.
    std::shared_ptr<process::Promise<TaskStatus>> running(
        new process::Promise<TaskStatus>());
    std::shared_ptr<process::Promise<TaskStatus>> last(
        new process::Promise<TaskStatus>());
    EXPECT_CALL(sched, statusUpdate(&driver, _))
      .WillRepeatedly(testing::Invoke(
          [running, last](SchedulerDriver*, const TaskStatus& status) {
            if (status.state() == TASK_RUNNING) {
              running->set(status);
            }
            if (protobuf::isTerminalState(status.state())) {
              last->set(status);
            }
          }));

    driver.launchTasks(offers->front().id(), {task});

    if (whileRunning != nullptr) {
      AWAIT_READY(running->future());
      whileRunning(driver, task);
    }

    AWAIT_READY_FOR(last->future(), Seconds(60));

    driver.stop();
    driver.join();

    *terminal = last->future().get();
  }

  TaskInfo task(const string& command)
  {
    TaskInfo task = createTask(NodeID(), Resources(), command);
    task.mutable_task_id()->set_value("finished");
    return task;
  }
};


INSTANTIATE_TEST_CASE_P(
    HTTPCommandExecutor,
    CommandExecutorFinishTest,
    ::testing::Bool());


// The hook runs after a work command that failed, sees that status and
// the ids, writes to the sandbox files, and the task's state is the work's.
TEST_P(CommandExecutorFinishTest, TheHookRunsAfterTheWorkAndSeesItsStatus)
{
  const string seen = path::join(sandbox.get(), "seen");

  TaskInfo prototype = task("exit 3");
  setFinish(
      &prototype,
      "echo \"$MEDIOS_WORK_EXIT_STATUS $MEDIOS_TASK_ID $MEDIOS_FRAMEWORK_ID"
      " $MEDIOS_NODE_ID\" > " + seen + "; echo finished-out; exit 5",
      Seconds(30));

  TaskStatus status;
  ASSERT_NO_FATAL_FAILURE(runToTerminal(prototype, &status));

  EXPECT_EQ(TASK_FAILED, status.state());
  EXPECT_TRUE(strings::contains(status.message(), "exited with status 3"))
    << status.message();

  ASSERT_TRUE(status.has_finish());
  EXPECT_EQ(FinishStatus::RAN, status.finish().state());
  EXPECT_EQ(5, status.finish().exit_code());
  EXPECT_EQ(Seconds(30).ns(), status.finish().limit().nanoseconds());
  EXPECT_TRUE(status.finish().has_duration());

  Try<string> contents = os::read(seen);
  ASSERT_SOME(contents);
  vector<string> fields = strings::tokenize(strings::trim(contents.get()), " ");
  ASSERT_EQ(4u, fields.size());
  EXPECT_EQ("3", fields[0]);
  EXPECT_EQ("finished", fields[1]);
  EXPECT_FALSE(fields[2].empty());
  EXPECT_FALSE(fields[3].empty());
}


// A hook that outlives its cap is ended by the executor and reported as
// such; the work's state stays what it was.
TEST_P(CommandExecutorFinishTest, AHookPastItsCapTimesOut)
{
  TaskInfo prototype = task("true");
  setFinish(&prototype, "sleep 60", Seconds(1));

  TaskStatus status;
  ASSERT_NO_FATAL_FAILURE(runToTerminal(prototype, &status));

  EXPECT_EQ(TASK_FINISHED, status.state());
  ASSERT_TRUE(status.has_finish());
  EXPECT_EQ(FinishStatus::TIMED_OUT, status.finish().state());
  EXPECT_EQ(Seconds(1).ns(), status.finish().limit().nanoseconds());
}


// The limit is what the ceiling has left when the work exits, less the
// kill grace period: a cap of a minute inside a ceiling of three seconds
// with a one-second grace is bounded at two.
TEST_P(CommandExecutorFinishTest, TheCeilingBoundsTheHook)
{
  TaskInfo prototype = task("true");
  prototype.mutable_max_completion_time()->set_nanoseconds(Seconds(3).ns());
  prototype.mutable_kill_policy()->mutable_grace_period()->set_nanoseconds(
      Seconds(1).ns());
  setFinish(&prototype, "sleep 60", Seconds(3));

  TaskStatus status;
  ASSERT_NO_FATAL_FAILURE(runToTerminal(prototype, &status));

  EXPECT_EQ(TASK_FINISHED, status.state());
  ASSERT_TRUE(status.has_finish());
  EXPECT_EQ(FinishStatus::TIMED_OUT, status.finish().state());
  EXPECT_LE(status.finish().limit().nanoseconds(), Seconds(2).ns());
}


// A ceiling that leaves less than the grace period leaves nothing the
// hook could use and end inside the deadline: it does not start, and the
// reason says the ceiling is spent.
TEST_P(CommandExecutorFinishTest, ACeilingInsideTheGraceMeansNoStart)
{
  TaskInfo prototype = task("true");
  prototype.mutable_max_completion_time()->set_nanoseconds(Seconds(2).ns());
  setFinish(&prototype, "true", Seconds(2));

  TaskStatus status;
  ASSERT_NO_FATAL_FAILURE(runToTerminal(prototype, &status));

  EXPECT_EQ(TASK_FINISHED, status.state());
  ASSERT_TRUE(status.has_finish());
  EXPECT_EQ(FinishStatus::DID_NOT_START, status.finish().state());
  EXPECT_TRUE(strings::contains(status.finish().reason(), "ceiling spent"))
    << status.finish().reason();
}


// A work command killed at the ceiling leaves nothing for the hook, which
// does not start and says why; the work's reason is unchanged.
TEST_P(CommandExecutorFinishTest, NothingLeftAtTheCeilingMeansNoStart)
{
  TaskInfo prototype = task("sleep 60");
  prototype.mutable_max_completion_time()->set_nanoseconds(Seconds(1).ns());
  setFinish(&prototype, "true", Seconds(1));

  TaskStatus status;
  ASSERT_NO_FATAL_FAILURE(runToTerminal(prototype, &status));

  EXPECT_EQ(TASK_FAILED, status.state());
  EXPECT_EQ(TaskStatus::REASON_MAX_COMPLETION_TIME_REACHED, status.reason());
  ASSERT_TRUE(status.has_finish());
  EXPECT_EQ(FinishStatus::DID_NOT_START, status.finish().state());
  EXPECT_TRUE(strings::contains(
      status.finish().reason(), "max_completion_time"))
    << status.finish().reason();
}


// A kill of the task while the work runs ends the work, and the hook still
// runs: the task is TASK_KILLED with the hook's outcome beside it.
TEST_P(CommandExecutorFinishTest, TheHookRunsAfterAKilledWork)
{
  const string seen = path::join(sandbox.get(), "seen");

  TaskInfo prototype = task("sleep 60");
  setFinish(
      &prototype,
      "echo $MEDIOS_WORK_EXIT_STATUS > " + seen,
      Seconds(30));

  TaskStatus status;
  ASSERT_NO_FATAL_FAILURE(runToTerminal(
      prototype,
      &status,
      [](MediosSchedulerDriver& driver, const TaskInfo& task) {
        driver.killTask(task.task_id());
      }));

  EXPECT_EQ(TASK_KILLED, status.state());
  ASSERT_TRUE(status.has_finish());
  EXPECT_EQ(FinishStatus::RAN, status.finish().state());
  EXPECT_EQ(0, status.finish().exit_code());

  Try<string> contents = os::read(seen);
  ASSERT_SOME(contents);
  EXPECT_EQ(stringify(128 + SIGTERM), strings::trim(contents.get()));
}


// A kill that arrives while the hook runs is applied to the hook: the work
// finished, the hook is KILLED.
TEST_P(CommandExecutorFinishTest, AKillDuringTheHookKillsTheHook)
{
  const string started = path::join(sandbox.get(), "started");

  TaskInfo prototype = task("true");
  setFinish(&prototype, "touch " + started + "; sleep 60", Seconds(30));

  TaskStatus status;
  ASSERT_NO_FATAL_FAILURE(runToTerminal(
      prototype,
      &status,
      [&started](MediosSchedulerDriver& driver, const TaskInfo& task) {
        for (int i = 0; i < 300 && !os::exists(started); i++) {
          os::sleep(Milliseconds(100));
        }
        ASSERT_TRUE(os::exists(started));
        driver.killTask(task.task_id());
      }));

  EXPECT_EQ(TASK_FINISHED, status.state());
  ASSERT_TRUE(status.has_finish());
  EXPECT_EQ(FinishStatus::KILLED, status.finish().state());
}


// A variable the framework sets on the finish command reaches the hook,
// and one of the four it tries to set does not: the executor's word wins.
TEST_P(CommandExecutorFinishTest, TheCommandsEnvironmentReachesTheHook)
{
  const string seen = path::join(sandbox.get(), "seen");

  TaskInfo prototype = task("true");
  setFinish(
      &prototype,
      "echo \"$BUCKET $MEDIOS_TASK_ID\" > " + seen,
      Seconds(30));

  Environment::Variable* bucket =
    prototype.mutable_finish()->mutable_command()->mutable_environment()
      ->add_variables();
  bucket->set_name("BUCKET");
  bucket->set_type(Environment::Variable::VALUE);
  bucket->set_value("results");

  Environment::Variable* forged =
    prototype.mutable_finish()->mutable_command()->mutable_environment()
      ->add_variables();
  forged->set_name("MEDIOS_TASK_ID");
  forged->set_type(Environment::Variable::VALUE);
  forged->set_value("not-this");

  TaskStatus status;
  ASSERT_NO_FATAL_FAILURE(runToTerminal(prototype, &status));

  EXPECT_EQ(TASK_FINISHED, status.state());
  ASSERT_TRUE(status.has_finish());
  EXPECT_EQ(FinishStatus::RAN, status.finish().state());

  Try<string> contents = os::read(seen);
  ASSERT_SOME(contents);
  EXPECT_EQ("results finished", strings::trim(contents.get()));
}


// A work command killed for a stall still gets its hook: the task is
// TASK_FAILED with REASON_PROGRESS_STALLED as before, the hook RAN.
TEST_P(CommandExecutorFinishTest, TheHookRunsAfterAStallKill)
{
  const string seen = path::join(sandbox.get(), "seen");

  TaskInfo prototype =
    task("echo 'progress: 0.1, started'; sleep 60");
  ProgressPolicy* policy = prototype.mutable_progress_policy();
  policy->set_source(ProgressPolicy::STDOUT);
  policy->mutable_sample_interval()->set_nanoseconds(
      Milliseconds(200).ns());
  policy->mutable_stall_window()->set_nanoseconds(Seconds(1).ns());
  policy->set_action(ProgressPolicy::KILL);
  setFinish(
      &prototype, "echo $MEDIOS_WORK_EXIT_STATUS > " + seen, Seconds(30));

  TaskStatus status;
  ASSERT_NO_FATAL_FAILURE(runToTerminal(prototype, &status));

  EXPECT_EQ(TASK_FAILED, status.state());
  EXPECT_EQ(TaskStatus::REASON_PROGRESS_STALLED, status.reason());
  ASSERT_TRUE(status.has_finish());
  EXPECT_EQ(FinishStatus::RAN, status.finish().state());
  EXPECT_EQ(0, status.finish().exit_code());

  Try<string> contents = os::read(seen);
  ASSERT_SOME(contents);
  EXPECT_EQ(stringify(128 + SIGTERM), strings::trim(contents.get()));
}


// A hook that traps SIGTERM under a ceiling shorter than its cap is
// SIGKILLed no later than the deadline: the limit is the ceiling's
// remainder minus the grace period, and the task ends inside the ceiling.
TEST_P(CommandExecutorFinishTest, AHookThatTrapsSigtermEndsAtTheCeiling)
{
  const Duration ceiling = Seconds(6);

  TaskInfo prototype = task("true");
  prototype.mutable_max_completion_time()->set_nanoseconds(ceiling.ns());
  prototype.mutable_kill_policy()->mutable_grace_period()->set_nanoseconds(
      Seconds(2).ns());
  setFinish(&prototype, "trap '' TERM; sleep 60", Seconds(6));

  TaskStatus status;
  ASSERT_NO_FATAL_FAILURE(runToTerminal(prototype, &status));

  EXPECT_EQ(TASK_FINISHED, status.state());
  ASSERT_TRUE(status.has_finish());
  EXPECT_EQ(FinishStatus::TIMED_OUT, status.finish().state());

  // The limit left room for the two-second grace inside the ceiling, and
  // the hook, which ignored SIGTERM, ran no longer than the limit plus
  // that grace: the SIGKILL landed by the deadline. The duration is
  // measured when the executor reaps the hook, which the reaper notices
  // within its own interval after the kill.
  const Duration limit = Nanoseconds(status.finish().limit().nanoseconds());
  const Duration duration =
    Nanoseconds(status.finish().duration().nanoseconds());
  EXPECT_LE(limit, ceiling - Seconds(2));
  EXPECT_LE(duration, limit + Seconds(2) + process::MAX_REAP_INTERVAL())
    << duration << " > " << limit;
}


// A shutdown while the work runs ends the work, and the hook does not
// start: the executor says so in its log, since the framework that would
// have read the status is gone with the shutdown.
TEST_P(CommandExecutorFinishTest, AShutdownBeforeTheHookMeansNoStart)
{
  Try<Owned<cluster::Controller>> controller = StartController();
  ASSERT_SOME(controller);

  Owned<ControllerDetector> detector = controller.get()->createDetector();

  node::Flags flags = finishNodeFlags();

  Try<MediosContainerizer*> _containerizer =
    MediosContainerizer::create(flags, false);
  ASSERT_SOME(_containerizer);
  Owned<MediosContainerizer> containerizer(_containerizer.get());

  Try<Owned<cluster::Node>> node =
    StartNode(detector.get(), containerizer.get(), flags);
  ASSERT_SOME(node);

  MockScheduler sched;
  MediosSchedulerDriver driver(
      &sched, DEFAULT_FRAMEWORK_INFO, controller.get()->pid,
      DEFAULT_CREDENTIAL);

  EXPECT_CALL(sched, registered(&driver, _, _));

  Future<vector<Offer>> offers;
  EXPECT_CALL(sched, resourceOffers(&driver, _))
    .WillOnce(FutureArg<1>(&offers))
    .WillRepeatedly(Return());

  driver.start();

  AWAIT_READY(offers);
  ASSERT_EQ(1u, offers->size());

  TaskInfo prototype = task("sleep 60");
  setFinish(&prototype, "true", Seconds(30));
  prototype.mutable_node_id()->CopyFrom(offers->front().node_id());
  prototype.mutable_resources()->CopyFrom(offers->front().resources());

  std::shared_ptr<process::Promise<TaskStatus>> running(
      new process::Promise<TaskStatus>());
  EXPECT_CALL(sched, statusUpdate(&driver, _))
    .WillRepeatedly(testing::Invoke(
        [running](SchedulerDriver*, const TaskStatus& status) {
          if (status.state() == TASK_RUNNING) {
            running->set(status);
          }
        }));

  driver.launchTasks(offers->front().id(), {prototype});

  AWAIT_READY(running->future());

  // Tearing the framework down shuts its executor down.
  driver.stop();
  driver.join();

  // The executor's log is the sandbox's stderr.
  Option<string> stderrPath = None();
  for (int i = 0; i < 300 && stderrPath.isNone(); i++) {
    Try<std::list<string>> found = os::find(flags.work_dir, "stderr");
    if (found.isSome()) {
      foreach (const string& path, found.get()) {
        Try<string> contents = os::read(path);
        if (contents.isSome() &&
            strings::contains(
                contents.get(), "Not starting the finish of task")) {
          stderrPath = path;
        }
      }
    }
    if (stderrPath.isNone()) {
      os::sleep(Milliseconds(100));
    }
  }

  ASSERT_SOME(stderrPath);
  Try<string> log = os::read(stderrPath.get());
  ASSERT_SOME(log);
  EXPECT_TRUE(strings::contains(log.get(), "the executor is shutting down"))
    << log.get();
}

} // namespace tests {
} // namespace internal {
} // namespace medios {
