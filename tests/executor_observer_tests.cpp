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

// The executor's side of `TaskInfo.observers` (DR-041): the set that holds
// the observers' processes and their newest line, and the command executor
// end to end, where that line reaches the framework as the task's progress
// message and the observer dies with the task.

#include <signal.h>
#include <unistd.h>

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
#include <process/subprocess.hpp>

#include <stout/duration.hpp>
#include <stout/foreach.hpp>
#include <stout/gtest.hpp>
#include <stout/numify.hpp>
#include <stout/option.hpp>
#include <stout/os.hpp>
#include <stout/path.hpp>
#include <stout/stringify.hpp>
#include <stout/strings.hpp>

#include "common/validation.hpp"

#include "launcher/observer.hpp"

#include "controller/controller.hpp"

#include "controller/detector/standalone.hpp"

#include "node/node.hpp"

#include "node/containerizer/medios/containerizer.hpp"

#include "tests/medios.hpp"
#include "tests/mock_node.hpp"
#include "tests/utils.hpp"

using medios::controller::detector::ControllerDetector;

using medios::internal::node::MediosContainerizer;

using medios::internal::observer::ObserverSet;

using process::Future;
using process::Owned;
using process::Subprocess;

using std::string;
using std::vector;

using testing::_;
using testing::Return;
using testing::WithParamInterface;

namespace observer = medios::internal::observer;
namespace validation = medios::internal::common::validation;

namespace medios {
namespace internal {
namespace tests {

// --- The observer set ---

class ObserverSetTest : public MediosTest
{
protected:
  static TaskObserver declare(const string& name, const string& command)
  {
    TaskObserver declaration;
    declaration.set_name(name);
    declaration.mutable_command()->set_shell(true);
    declaration.mutable_command()->set_value(command);
    return declaration;
  }

  static google::protobuf::RepeatedPtrField<TaskObserver> declarations(
      const vector<TaskObserver>& observers)
  {
    google::protobuf::RepeatedPtrField<TaskObserver> field;
    foreach (const TaskObserver& declaration, observers) {
      field.Add()->CopyFrom(declaration);
    }
    return field;
  }

  // The shape the docker executor forks an observer with: a host-side
  // child in the sandbox, its stdout a pipe and its stderr a file.
  static Try<Subprocess> forkInSandbox(
      const TaskObserver& declaration,
      const string& stderrPath,
      const string& sandbox)
  {
    return process::subprocess(
        "/bin/sh",
        {"sh", "-c", declaration.command().value()},
        Subprocess::PATH(os::DEV_NULL),
        Subprocess::PIPE(),
        Subprocess::PATH(stderrPath),
        nullptr,
        None(),
        None(),
        {},
        {Subprocess::ChildHook::CHDIR(sandbox),
         Subprocess::ChildHook::SETSID()});
  }

  static observer::Fork forker(const string& sandbox)
  {
    return [sandbox](
        const TaskObserver& declaration, const string& stderrPath) {
      return forkInSandbox(declaration, stderrPath, sandbox);
    };
  }

  // Drains and reports each observation as an executor does, until
  // `count` of them have been seen or the wait gives out. An observer
  // writes on its own schedule, so the wait is on the observation and not
  // on a sleep.
  static vector<string> observe(
      const Owned<ObserverSet>& observers,
      size_t count)
  {
    vector<string> seen;

    for (int i = 0; i < 300 && seen.size() < count; i++) {
      observers->drain();

      if (observers->hasUnreported()) {
        seen.push_back(observers->line().get());
        observers->reported();
        continue;
      }

      os::sleep(Milliseconds(50));
    }

    return seen;
  }

  // Whether every byte of `text` belongs to a whole character, which is
  // what a protobuf string and its JSON render require.
  static bool isValidUtf8(const string& text)
  {
    size_t index = 0u;

    while (index < text.size()) {
      const unsigned char lead = static_cast<unsigned char>(text[index]);

      size_t length = 1u;
      if ((lead & 0x80) == 0x00) {
        length = 1u;
      } else if ((lead & 0xe0) == 0xc0) {
        length = 2u;
      } else if ((lead & 0xf0) == 0xe0) {
        length = 3u;
      } else if ((lead & 0xf8) == 0xf0) {
        length = 4u;
      } else {
        return false;
      }

      if (text.size() - index < length) {
        return false;
      }

      for (size_t i = 1u; i < length; i++) {
        if ((static_cast<unsigned char>(text[index + i]) & 0xc0) != 0x80) {
          return false;
        }
      }

      index += length;
    }

    return true;
  }

  // Waits for every observer to exit, then drains once, so what the set
  // holds is everything the observers wrote and nothing of a schedule.
  static void drainAfterExit(const Owned<ObserverSet>& observers)
  {
    for (int i = 0; i < 300 && !observers->running().empty(); i++) {
      os::sleep(Milliseconds(50));
    }

    observers->drain();
  }
};


TEST_F(ObserverSetTest, NoObserversSayNothing)
{
  Owned<ObserverSet> observers =
    ObserverSet::create(declarations({}), os::getcwd());

  observers->launch(forker(os::getcwd()));
  observers->drain();

  EXPECT_NONE(observers->line());
  EXPECT_FALSE(observers->hasUnreported());
  EXPECT_EQ(0u, observers->generation());
  EXPECT_TRUE(observers->running().empty());
}


// Every line is an observation, and the newest one is what the record
// carries: the message is a headline, not a log.
TEST_F(ObserverSetTest, TheNewestLineWins)
{
  Owned<ObserverSet> observers = ObserverSet::create(
      declarations({declare("obs", "echo first; echo second")}),
      os::getcwd());

  observers->launch(forker(os::getcwd()));
  drainAfterExit(observers);

  EXPECT_EQ(2u, observers->generation());
  ASSERT_SOME(observers->line());
  EXPECT_EQ("obs: second", observers->line().get());
}


// Several observers on one task are told apart by the name their lines
// carry.
TEST_F(ObserverSetTest, EachLineCarriesItsObserversName)
{
  Owned<ObserverSet> observers = ObserverSet::create(
      declarations({declare("haiku", "echo OK"),
                    declare("gpu-watch", "sleep 1000")}),
      os::getcwd());

  observers->launch(forker(os::getcwd()));

  const vector<string> seen = observe(observers, 1u);

  ASSERT_EQ(1u, seen.size());
  EXPECT_EQ("haiku: OK", seen[0]);

  // The second observer never exits by itself. Kill it and wait for both
  // to be reaped, so the test leaves no child behind.
  observers->signalKill();
  drainAfterExit(observers);
}


// An observer's exit is an observation like any other, and it comes after
// the last thing the observer said rather than over it.
TEST_F(ObserverSetTest, AnExitComesAfterTheLastObservation)
{
  Owned<ObserverSet> observers = ObserverSet::create(
      declarations({declare("obs", "echo ISSUE verdict; exit 0")}),
      os::getcwd());

  observers->launch(forker(os::getcwd()));

  const vector<string> seen = observe(observers, 2u);

  ASSERT_EQ(2u, seen.size());
  EXPECT_EQ("obs: ISSUE verdict", seen[0]);
  EXPECT_EQ("obs: exited 0", seen[1]);
}


TEST_F(ObserverSetTest, ANonZeroExitIsRecorded)
{
  Owned<ObserverSet> observers = ObserverSet::create(
      declarations({declare("obs", "exit 3")}), os::getcwd());

  observers->launch(forker(os::getcwd()));

  const vector<string> seen = observe(observers, 1u);

  ASSERT_EQ(1u, seen.size());
  EXPECT_EQ("obs: exited 3", seen[0]);
}


// A fork that fails is one line in the record and nothing else: the task
// it was to observe is not affected.
TEST_F(ObserverSetTest, AFailedLaunchIsRecorded)
{
  Owned<ObserverSet> observers = ObserverSet::create(
      declarations({declare("obs", "true")}), os::getcwd());

  observers->launch([](const TaskObserver&, const string&) {
    return Try<Subprocess>(Error("no such file or directory"));
  });

  ASSERT_SOME(observers->line());
  EXPECT_EQ(
      "obs: did not start: no such file or directory",
      observers->line().get());
  EXPECT_TRUE(observers->hasUnreported());
  EXPECT_EQ(1u, observers->generation());
  EXPECT_TRUE(observers->running().empty());
}


// An executor that does not know the task's sandbox has nowhere to run an
// observer. That is every observer failing to launch, not a reason to
// refuse the task.
TEST_F(ObserverSetTest, AnUnknownSandboxIsAFailedLaunch)
{
  Owned<ObserverSet> observers = ObserverSet::create(
      declarations({declare("obs", "echo never")}), "");

  observers->launch(forker(os::getcwd()));

  ASSERT_SOME(observers->line());
  EXPECT_EQ(
      "obs: did not start: no sandbox directory",
      observers->line().get());
  EXPECT_TRUE(observers->running().empty());
}


// The node drops a progress report whose message is over the bound, which
// would lose the observation rather than shorten it.
TEST_F(ObserverSetTest, ALongLineIsTruncatedToTheMessageBound)
{
  Owned<ObserverSet> observers = ObserverSet::create(
      declarations({declare(
          "obs",
          "awk 'BEGIN { while (i++ < 9000) printf \"x\" ; print \"\" }'")}),
      os::getcwd());

  observers->launch(forker(os::getcwd()));

  const vector<string> seen = observe(observers, 1u);

  ASSERT_EQ(1u, seen.size());
  EXPECT_EQ(validation::PROGRESS_MESSAGE_MAX_LENGTH, seen[0].size());
  EXPECT_TRUE(strings::startsWith(seen[0], "obs: xxx")) << seen[0];
}


// A character the bound cuts in half is dropped rather than left half
// written: a progress message is a protobuf string and is rendered into
// JSON, and one byte of a two-byte character is neither. The prefix is five
// bytes, so 4090 ASCII characters put the cut inside the character that
// follows them.
TEST_F(ObserverSetTest, ATruncatedLineKeepsWholeCharacters)
{
  Owned<ObserverSet> observers = ObserverSet::create(
      declarations({declare(
          "obs",
          "awk 'BEGIN { while (i++ < 4090) printf \"x\";"
          " printf \"\\303\\251\\303\\251\"; print \"\" }'")}),
      os::getcwd());

  observers->launch(forker(os::getcwd()));

  const vector<string> seen = observe(observers, 1u);

  ASSERT_EQ(1u, seen.size());

  // "obs: " plus 4090 characters is 4095 bytes, so the first byte of the
  // first two-byte character falls on the bound and the character goes.
  EXPECT_EQ(4095u, seen[0].size());
  EXPECT_EQ('x', seen[0].back());
  EXPECT_TRUE(isValidUtf8(seen[0])) << seen[0].size() << " bytes";
}


// An observer's stderr is a sandbox file, so a misbehaving observer can be
// read without its noise landing in the task's own stderr.
TEST_F(ObserverSetTest, StderrGoesToTheSandbox)
{
  Owned<ObserverSet> observers = ObserverSet::create(
      declarations({declare("obs", "echo trouble >&2; echo fine")}),
      os::getcwd());

  observers->launch(forker(os::getcwd()));

  ASSERT_EQ(1u, observe(observers, 1u).size());

  const string path = path::join(os::getcwd(), "observers", "obs.stderr");

  ASSERT_TRUE(os::exists(path));

  Try<string> contents = os::read(path);
  ASSERT_SOME(contents);
  EXPECT_TRUE(strings::contains(contents.get(), "trouble")) << contents.get();
}


// An observer cannot outlive the set that holds it.
TEST_F(ObserverSetTest, TerminationKillsTheObserver)
{
  Owned<ObserverSet> observers = ObserverSet::create(
      declarations({declare("obs", "echo up; sleep 1000")}), os::getcwd());

  observers->launch(forker(os::getcwd()));

  ASSERT_EQ(1u, observe(observers, 1u).size());

  const vector<pid_t> pids = observers->running();
  ASSERT_EQ(1u, pids.size());

  observers->signalTerm();
  observers->signalKill();

  bool gone = false;
  for (int i = 0; i < 100 && !gone; i++) {
    gone = !os::exists(pids.front());
    if (!gone) {
      os::sleep(Milliseconds(50));
    }
  }

  EXPECT_TRUE(gone) << "observer " << pids.front() << " is still running";
}


// A child an observer backgrounded is in the observer's group and session,
// so it is signalled even after the observer itself has been reaped. The
// observer prints the child's pid and exits.
TEST_F(ObserverSetTest, AChildTheObserverLeftBehindIsKilled)
{
  Owned<ObserverSet> observers = ObserverSet::create(
      declarations({declare("obs", "sleep 300 & echo $!; exit 0")}),
      os::getcwd());

  observers->launch(forker(os::getcwd()));

  const vector<string> seen = observe(observers, 1u);

  ASSERT_EQ(1u, seen.size());

  Try<pid_t> child = numify<pid_t>(
      strings::remove(seen[0], "obs: ", strings::PREFIX));
  ASSERT_SOME(child);
  ASSERT_TRUE(os::exists(child.get()));

  // Wait for the observer itself to be reaped, so the signal goes to a
  // group whose leader has gone.
  for (int i = 0; i < 300 && !observers->running().empty(); i++) {
    os::sleep(Milliseconds(50));
  }
  ASSERT_TRUE(observers->running().empty());

  observers->signalTerm();
  observers->signalKill();

  bool gone = false;
  for (int i = 0; i < 100 && !gone; i++) {
    gone = !os::exists(child.get());
    if (!gone) {
      os::sleep(Milliseconds(50));
    }
  }

  EXPECT_TRUE(gone) << "child " << child.get() << " outlived its observer";
}


// The exit this executor caused is not an observation.
TEST_F(ObserverSetTest, AKilledObserverDoesNotReportItsExit)
{
  Owned<ObserverSet> observers = ObserverSet::create(
      declarations({declare("obs", "echo up; sleep 1000")}), os::getcwd());

  observers->launch(forker(os::getcwd()));

  ASSERT_EQ(1u, observe(observers, 1u).size());
  ASSERT_EQ(1u, observers->generation());

  observers->signalKill();

  for (int i = 0; i < 40; i++) {
    observers->drain();
    os::sleep(Milliseconds(50));
  }

  EXPECT_FALSE(observers->hasUnreported());
  EXPECT_EQ(1u, observers->generation());
  ASSERT_SOME(observers->line());
  EXPECT_EQ("obs: up", observers->line().get());
}


// --- The command executor, end to end ---

class CommandExecutorObserverTest
  : public MediosTest,
    public WithParamInterface<bool>
{
protected:
  // Observers are independent of isolation, and the shapes here only need
  // a forked process and its sandbox. Naming no isolator and the POSIX
  // launcher keeps them off the host's cgroup hierarchy, which on a host
  // running a node belongs to that node.
  node::Flags observerNodeFlags()
  {
    node::Flags flags = CreateNodeFlags();
    flags.http_command_executor = GetParam();
    flags.isolation = "";
    flags.launcher = "posix";
    return flags;
  }

  static TaskObserver declare(const string& name, const string& command)
  {
    TaskObserver declaration;
    declaration.set_name(name);
    declaration.mutable_command()->set_shell(true);
    declaration.mutable_command()->set_value(command);
    return declaration;
  }
};


INSTANTIATE_TEST_CASE_P(
    HTTPCommandExecutor,
    CommandExecutorObserverTest,
    ::testing::Bool());


// An observer beside a sleeping task: each line it writes reaches the
// framework as the task's progress message, in the order it wrote them,
// and the work value stays absent because no observer sets it.
TEST_P(CommandExecutorObserverTest, ObserverLinesBecomeTheProgressMessage)
{
  Try<Owned<cluster::Controller>> controller = StartController();
  ASSERT_SOME(controller);

  Owned<ControllerDetector> detector = controller.get()->createDetector();

  node::Flags flags = observerNodeFlags();

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

  TaskInfo task = createTask(
      offers->front().node_id(), offers->front().resources(), "sleep 60");

  task.add_observers()->CopyFrom(
      declare("obs", "echo OK first; sleep 1; echo ISSUE second"));

  vector<TaskStatus> updates;
  EXPECT_CALL(sched, statusUpdate(_, _))
    .WillRepeatedly(testing::Invoke(
        [&updates](SchedulerDriver*, const TaskStatus& status) {
          updates.push_back(status);
        }));

  driver.launchTasks(offers->front().id(), {task});

  // The two lines reach the framework in the order the observer wrote
  // them. Which update carries the first one depends on how fast the
  // observer gets to its first `echo`, so the sequence of observations is
  // what is asserted, not which update holds each.
  vector<string> observed;
  for (int i = 0; i < 300 && observed.size() < 2u; i++) {
    observed.clear();

    foreach (const TaskStatus& status, updates) {
      if (!status.has_progress() ||
          !strings::startsWith(status.progress().message(), "obs: ")) {
        continue;
      }

      if (observed.empty() ||
          observed.back() != status.progress().message()) {
        observed.push_back(status.progress().message());
      }

      // No observer sets the work value, so a task whose only reporter is
      // an observer has no fraction.
      EXPECT_FALSE(status.progress().has_fraction())
        << status.progress().message();
      EXPECT_EQ(TASK_RUNNING, status.state());
    }

    if (observed.size() < 2u) {
      os::sleep(Milliseconds(100));
    }
  }

  ASSERT_LE(2u, observed.size());
  EXPECT_EQ("obs: OK first", observed[0]);
  EXPECT_EQ("obs: ISSUE second", observed[1]);

  driver.killTask(task.task_id());

  driver.stop();
  driver.join();
}


// A report that is only an observation is the whole report, so it carries
// the ordering and the freshness of one: successive observations advance
// `sequence` and each carries `observed_at`.
TEST_P(CommandExecutorObserverTest, ObservationsAdvanceTheReportSequence)
{
  Try<Owned<cluster::Controller>> controller = StartController();
  ASSERT_SOME(controller);

  Owned<ControllerDetector> detector = controller.get()->createDetector();

  node::Flags flags = observerNodeFlags();

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

  TaskInfo task = createTask(
      offers->front().node_id(), offers->front().resources(), "sleep 60");

  task.add_observers()->CopyFrom(
      declare("obs", "echo one; sleep 1; echo two; sleep 1; echo three"));

  vector<TaskStatus> updates;
  EXPECT_CALL(sched, statusUpdate(_, _))
    .WillRepeatedly(testing::Invoke(
        [&updates](SchedulerDriver*, const TaskStatus& status) {
          updates.push_back(status);
        }));

  driver.launchTasks(offers->front().id(), {task});

  vector<TaskStatus> reports;
  for (int i = 0; i < 300 && reports.size() < 3u; i++) {
    reports.clear();

    foreach (const TaskStatus& status, updates) {
      if (status.has_progress() &&
          strings::startsWith(status.progress().message(), "obs: ")) {
        reports.push_back(status);
      }
    }

    if (reports.size() < 3u) {
      os::sleep(Milliseconds(100));
    }
  }

  ASSERT_LE(3u, reports.size());

  uint64_t previous = 0u;
  foreach (const TaskStatus& status, reports) {
    ASSERT_TRUE(status.progress().has_sequence())
      << status.progress().message();
    EXPECT_LT(previous, status.progress().sequence())
      << status.progress().message();
    previous = status.progress().sequence();

    ASSERT_TRUE(status.progress().has_observed_at())
      << status.progress().message();
    EXPECT_LT(0.0, status.progress().observed_at());
  }

  driver.killTask(task.task_id());

  driver.stop();
  driver.join();
}


// An observer that exits says so, and the task it was observing keeps
// running: an observer has no authority over the task.
TEST_P(CommandExecutorObserverTest, AnObserversExitReachesTheFramework)
{
  Try<Owned<cluster::Controller>> controller = StartController();
  ASSERT_SOME(controller);

  Owned<ControllerDetector> detector = controller.get()->createDetector();

  node::Flags flags = observerNodeFlags();

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

  TaskInfo task = createTask(
      offers->front().node_id(), offers->front().resources(), "sleep 60");

  task.add_observers()->CopyFrom(declare("obs", "echo watching; exit 0"));

  vector<TaskStatus> updates;
  EXPECT_CALL(sched, statusUpdate(_, _))
    .WillRepeatedly(testing::Invoke(
        [&updates](SchedulerDriver*, const TaskStatus& status) {
          updates.push_back(status);
        }));

  driver.launchTasks(offers->front().id(), {task});

  // The exit reaches the framework on its own update, after the line the
  // observer managed to write.
  bool exited = false;
  for (int i = 0; i < 200 && !exited; i++) {
    foreach (const TaskStatus& status, updates) {
      if (status.has_progress() &&
          status.progress().message() == "obs: exited 0") {
        exited = true;
      }
    }

    if (!exited) {
      os::sleep(Milliseconds(100));
    }
  }

  EXPECT_TRUE(exited);

  // The task outlived its observer.
  foreach (const TaskStatus& status, updates) {
    EXPECT_NE(TASK_FAILED, status.state());
    EXPECT_NE(TASK_FINISHED, status.state());
  }

  driver.killTask(task.task_id());

  driver.stop();
  driver.join();
}


// An observer command that is not there is one line in the record. The
// task runs unobserved rather than failing. The command executor launches
// an observer through `medios-containerizer launch`, which exits non-zero
// when it cannot exec the command, so a command that is not there arrives
// as that exit and the reason is in the observer's sandbox stderr file.
TEST_P(CommandExecutorObserverTest, AnObserverThatDoesNotStartIsRecorded)
{
  Try<Owned<cluster::Controller>> controller = StartController();
  ASSERT_SOME(controller);

  Owned<ControllerDetector> detector = controller.get()->createDetector();

  node::Flags flags = observerNodeFlags();

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

  TaskInfo task = createTask(
      offers->front().node_id(), offers->front().resources(), "sleep 60");

  TaskObserver missing = declare("obs", "");
  missing.mutable_command()->set_shell(false);
  missing.mutable_command()->set_value("/nonexistent/observer-binary");
  task.add_observers()->CopyFrom(missing);

  vector<TaskStatus> updates;
  EXPECT_CALL(sched, statusUpdate(_, _))
    .WillRepeatedly(testing::Invoke(
        [&updates](SchedulerDriver*, const TaskStatus& status) {
          updates.push_back(status);
        }));

  driver.launchTasks(offers->front().id(), {task});

  bool recorded = false;
  bool running = false;
  for (int i = 0; i < 200 && !recorded; i++) {
    foreach (const TaskStatus& status, updates) {
      if (status.state() == TASK_RUNNING) {
        running = true;
      }

      if (status.has_progress() &&
          strings::startsWith(status.progress().message(), "obs: exited ") &&
          status.progress().message() != "obs: exited 0") {
        recorded = true;
      }
    }

    if (!recorded) {
      os::sleep(Milliseconds(100));
    }
  }

  EXPECT_TRUE(recorded);
  EXPECT_TRUE(running);

  driver.killTask(task.task_id());

  driver.stop();
  driver.join();
}


// An observer is killed with the task it observes and cannot outlive the
// sandbox. The observer reports its own pid, so the test watches the
// process the executor actually forked.
TEST_P(CommandExecutorObserverTest, KillingTheTaskKillsTheObserver)
{
  Try<Owned<cluster::Controller>> controller = StartController();
  ASSERT_SOME(controller);

  Owned<ControllerDetector> detector = controller.get()->createDetector();

  node::Flags flags = observerNodeFlags();

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

  TaskInfo task = createTask(
      offers->front().node_id(), offers->front().resources(), "sleep 300");

  task.add_observers()->CopyFrom(declare("obs", "echo $$; sleep 300"));

  vector<TaskStatus> updates;
  EXPECT_CALL(sched, statusUpdate(_, _))
    .WillRepeatedly(testing::Invoke(
        [&updates](SchedulerDriver*, const TaskStatus& status) {
          updates.push_back(status);
        }));

  driver.launchTasks(offers->front().id(), {task});

  Option<pid_t> observerPid = None();
  for (int i = 0; i < 200 && observerPid.isNone(); i++) {
    foreach (const TaskStatus& status, updates) {
      if (!status.has_progress() ||
          !strings::startsWith(status.progress().message(), "obs: ")) {
        continue;
      }

      Try<pid_t> parsed = numify<pid_t>(
          strings::remove(
              status.progress().message(), "obs: ", strings::PREFIX));

      if (parsed.isSome()) {
        observerPid = parsed.get();
      }
    }

    if (observerPid.isNone()) {
      os::sleep(Milliseconds(100));
    }
  }

  ASSERT_SOME(observerPid);
  ASSERT_TRUE(os::exists(observerPid.get()));

  driver.killTask(task.task_id());

  bool gone = false;
  for (int i = 0; i < 300 && !gone; i++) {
    gone = !os::exists(observerPid.get());
    if (!gone) {
      os::sleep(Milliseconds(100));
    }
  }

  EXPECT_TRUE(gone)
    << "observer " << observerPid.get() << " outlived its task";

  driver.stop();
  driver.join();
}

} // namespace tests {
} // namespace internal {
} // namespace medios {
