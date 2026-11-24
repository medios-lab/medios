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

#include <string>
#include <vector>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <medios/medios.hpp>

#include <process/future.hpp>
#include <process/gmock.hpp>
#include <process/gtest.hpp>
#include <process/owned.hpp>

#include <stout/duration.hpp>
#include <stout/gtest.hpp>
#include <stout/os.hpp>
#include <stout/path.hpp>
#include <stout/strings.hpp>

#include <stout/tests/utils.hpp>

#include "controller/controller.hpp"

#include "controller/detector/standalone.hpp"

#include "launcher/progress.hpp"

#include "node/node.hpp"

#include "node/containerizer/medios/containerizer.hpp"

#include "tests/medios.hpp"
#include "tests/mock_node.hpp"
#include "tests/utils.hpp"

// `ProgressPolicy` alone names the proto message carried on the task; the
// executor's own struct is `progress::ProgressPolicy`.
namespace progress = medios::internal::progress;
using medios::internal::progress::ProgressReader;
using medios::internal::progress::progressPolicyFrom;

using medios::internal::node::MediosContainerizer;

using medios::controller::detector::ControllerDetector;

using process::Future;
using process::Owned;

using std::string;
using std::vector;

using ::testing::_;
using ::testing::Return;
using ::testing::WithParamInterface;

namespace medios {
namespace internal {
namespace tests {

// --- The policy carrier ---

class ProgressPolicyTest : public ::testing::Test {};


// Sets a `DurationInfo` from a stout `Duration`.
static void setDuration(DurationInfo* duration, const Duration& value)
{
  duration->set_nanoseconds(value.ns());
}


// A task that carries no `progress_policy` gets no progress reading and
// no stall kill.
TEST_F(ProgressPolicyTest, AbsentWithoutAPolicy)
{
  TaskInfo task;

  Try<Option<progress::ProgressPolicy>> policy = progressPolicyFrom(task);

  ASSERT_SOME(policy);
  EXPECT_NONE(policy.get());
}


// The source is never guessed: reading the wrong file would report another
// task's numbers as this task's progress.
TEST_F(ProgressPolicyTest, SourceIsRequired)
{
  TaskInfo task;
  setDuration(
      task.mutable_progress_policy()->mutable_stall_window(), Seconds(10));

  Try<Option<progress::ProgressPolicy>> policy = progressPolicyFrom(task);

  ASSERT_ERROR(policy);
  EXPECT_TRUE(strings::contains(policy.error(), "source"));
}


TEST_F(ProgressPolicyTest, DefaultsWithStdoutSource)
{
  TaskInfo task;
  task.mutable_progress_policy()->set_source(
      ::medios::ProgressPolicy::STDOUT);

  Try<Option<progress::ProgressPolicy>> policy = progressPolicyFrom(task);

  ASSERT_SOME(policy);
  ASSERT_SOME(policy.get());
  EXPECT_EQ(progress::ProgressPolicy::STDOUT, policy->get().source);
  EXPECT_EQ(Milliseconds(1000), policy->get().sampleInterval);
  EXPECT_NONE(policy->get().stallWindow);
}


TEST_F(ProgressPolicyTest, CarriesAFileSource)
{
  TaskInfo task;
  ::medios::ProgressPolicy* declared = task.mutable_progress_policy();
  declared->set_source(::medios::ProgressPolicy::FILE);
  declared->set_path("logs/progress");
  declared->set_pattern("step ([0-9]+)");
  setDuration(declared->mutable_sample_interval(), Milliseconds(250));
  setDuration(declared->mutable_stall_window(), Seconds(45));
  declared->set_action(::medios::ProgressPolicy::KILL);

  Try<Option<progress::ProgressPolicy>> policy = progressPolicyFrom(task);

  ASSERT_SOME(policy);
  ASSERT_SOME(policy.get());
  EXPECT_EQ(progress::ProgressPolicy::PATH, policy->get().source);
  ASSERT_SOME(policy->get().path);
  EXPECT_EQ("logs/progress", policy->get().path.get());
  EXPECT_EQ("step ([0-9]+)", policy->get().pattern);
  EXPECT_EQ(Milliseconds(250), policy->get().sampleInterval);
  ASSERT_SOME(policy->get().stallWindow);
  EXPECT_EQ(Seconds(45), policy->get().stallWindow.get());
  EXPECT_EQ(progress::ProgressPolicy::KILL, policy->get().action);
}


TEST_F(ProgressPolicyTest, RejectsAFileSourceWithoutAPath)
{
  TaskInfo task;
  task.mutable_progress_policy()->set_source(::medios::ProgressPolicy::FILE);

  Try<Option<progress::ProgressPolicy>> policy = progressPolicyFrom(task);

  ASSERT_ERROR(policy);
  EXPECT_TRUE(strings::contains(policy.error(), "path"));
}


// A path alongside the stdout source would be read by neither, so it is a
// setting the framework believes is in force and is not.
TEST_F(ProgressPolicyTest, RejectsAPathWithTheStdoutSource)
{
  TaskInfo task;
  ::medios::ProgressPolicy* declared = task.mutable_progress_policy();
  declared->set_source(::medios::ProgressPolicy::STDOUT);
  declared->set_path("logs/progress");

  Try<Option<progress::ProgressPolicy>> policy = progressPolicyFrom(task);

  ASSERT_ERROR(policy);
  EXPECT_TRUE(strings::contains(policy.error(), "STDOUT"));
}


// Kill-on-stall and report-only are different outcomes for the framework,
// so the window never implies either one.
TEST_F(ProgressPolicyTest, ActionIsRequiredWithAStallWindow)
{
  TaskInfo task;
  ::medios::ProgressPolicy* declared = task.mutable_progress_policy();
  declared->set_source(::medios::ProgressPolicy::STDOUT);
  setDuration(declared->mutable_stall_window(), Seconds(30));

  Try<Option<progress::ProgressPolicy>> policy = progressPolicyFrom(task);

  ASSERT_ERROR(policy);
  EXPECT_TRUE(strings::contains(policy.error(), "action"));
}


TEST_F(ProgressPolicyTest, StallWindowIsRequiredWithAnAction)
{
  TaskInfo task;
  ::medios::ProgressPolicy* declared = task.mutable_progress_policy();
  declared->set_source(::medios::ProgressPolicy::STDOUT);
  declared->set_action(::medios::ProgressPolicy::KILL);

  Try<Option<progress::ProgressPolicy>> policy = progressPolicyFrom(task);

  ASSERT_ERROR(policy);
  EXPECT_TRUE(strings::contains(policy.error(), "stall_window"));
}


TEST_F(ProgressPolicyTest, RejectsAPathOutsideTheSandbox)
{
  TaskInfo task;
  ::medios::ProgressPolicy* declared = task.mutable_progress_policy();
  declared->set_source(::medios::ProgressPolicy::FILE);
  declared->set_path("../../etc/passwd");

  Try<Option<progress::ProgressPolicy>> policy = progressPolicyFrom(task);

  ASSERT_ERROR(policy);
  EXPECT_TRUE(strings::contains(policy.error(), "sandbox"));
}


TEST_F(ProgressPolicyTest, RejectsAnAbsolutePath)
{
  TaskInfo task;
  ::medios::ProgressPolicy* declared = task.mutable_progress_policy();
  declared->set_source(::medios::ProgressPolicy::FILE);
  declared->set_path("/var/log/messages");

  Try<Option<progress::ProgressPolicy>> policy = progressPolicyFrom(task);

  ASSERT_ERROR(policy);
  EXPECT_TRUE(strings::contains(policy.error(), "relative"));
}


TEST_F(ProgressPolicyTest, RejectsANonPositiveSampleInterval)
{
  TaskInfo task;
  ::medios::ProgressPolicy* declared = task.mutable_progress_policy();
  declared->set_source(::medios::ProgressPolicy::STDOUT);
  setDuration(declared->mutable_sample_interval(), Duration::zero());

  Try<Option<progress::ProgressPolicy>> policy = progressPolicyFrom(task);

  ASSERT_ERROR(policy);
  EXPECT_TRUE(strings::contains(policy.error(), "sample_interval"));
}


TEST_F(ProgressPolicyTest, RejectsANonPositiveStallWindow)
{
  TaskInfo task;
  ::medios::ProgressPolicy* declared = task.mutable_progress_policy();
  declared->set_source(::medios::ProgressPolicy::STDOUT);
  setDuration(declared->mutable_stall_window(), Duration::zero());
  declared->set_action(::medios::ProgressPolicy::KILL);

  Try<Option<progress::ProgressPolicy>> policy = progressPolicyFrom(task);

  ASSERT_ERROR(policy);
  EXPECT_TRUE(strings::contains(policy.error(), "stall_window"));
}


// --- The reader ---

class ProgressReaderTest : public TemporaryDirectoryTest
{
protected:
  progress::ProgressPolicy makePolicy(
      const Option<Duration>& stallWindow = None(),
      progress::ProgressPolicy::Action action = progress::ProgressPolicy::KILL)
  {
    progress::ProgressPolicy made;
    made.source = progress::ProgressPolicy::PATH;
    made.path = "progress.log";
    made.pattern = medios::internal::progress::DEFAULT_PATTERN;
    made.sampleInterval = Milliseconds(10);
    made.stallWindow = stallWindow;
    made.action = action;
    return made;
  }

  string source() const
  {
    return path::join(os::getcwd(), "progress.log");
  }

  void append(const string& text)
  {
    ASSERT_SOME(os::write(source(), read() + text));
  }

  string read() const
  {
    if (!os::exists(source())) {
      return "";
    }
    Try<string> contents = os::read(source());
    return contents.isSome() ? contents.get() : "";
  }
};


// A source the task has not created yet is not an error: a task that has
// written nothing has reported nothing.
TEST_F(ProgressReaderTest, MissingSourceReportsNothing)
{
  Try<Owned<ProgressReader>> reader =
    ProgressReader::create(makePolicy(), os::getcwd());

  ASSERT_SOME(reader);
  ASSERT_SOME(reader.get()->sample());
  EXPECT_NONE(reader.get()->fraction());
  EXPECT_NONE(reader.get()->report());
}


TEST_F(ProgressReaderTest, ReadsAnAdvancingValue)
{
  Try<Owned<ProgressReader>> reader =
    ProgressReader::create(makePolicy(), os::getcwd());
  ASSERT_SOME(reader);

  append("progress: 25, loading shard\n");
  ASSERT_SOME(reader.get()->sample());

  Option<TaskStatus::Progress> first = reader.get()->report();
  ASSERT_SOME(first);
  EXPECT_DOUBLE_EQ(0.25, first->fraction());
  EXPECT_EQ("loading shard", first->message());
  EXPECT_EQ(1u, first->sequence());
  EXPECT_TRUE(first->has_observed_at());
  reader.get()->reported();

  append("progress: 75, training\n");
  ASSERT_SOME(reader.get()->sample());

  Option<TaskStatus::Progress> second = reader.get()->report();
  ASSERT_SOME(second);
  EXPECT_DOUBLE_EQ(0.75, second->fraction());
  EXPECT_EQ("training", second->message());
  EXPECT_EQ(2u, second->sequence());
}


// A partial line is held until the task finishes writing it.
TEST_F(ProgressReaderTest, HoldsAPartialLine)
{
  Try<Owned<ProgressReader>> reader =
    ProgressReader::create(makePolicy(), os::getcwd());
  ASSERT_SOME(reader);

  append("progress: 40, half w");
  ASSERT_SOME(reader.get()->sample());
  EXPECT_NONE(reader.get()->fraction());

  append("ritten\n");
  ASSERT_SOME(reader.get()->sample());

  ASSERT_SOME(reader.get()->fraction());
  EXPECT_DOUBLE_EQ(0.4, reader.get()->fraction().get());
}


TEST_F(ProgressReaderTest, DropsValuesOutsideTheDomainAndBackwards)
{
  Try<Owned<ProgressReader>> reader =
    ProgressReader::create(makePolicy(), os::getcwd());
  ASSERT_SOME(reader);

  append("progress: 50, half\n");
  append("progress: 120, more than all of it\n");
  append("progress: 10, backwards\n");
  ASSERT_SOME(reader.get()->sample());

  ASSERT_SOME(reader.get()->fraction());
  EXPECT_DOUBLE_EQ(0.5, reader.get()->fraction().get());
  EXPECT_EQ(2u, reader.get()->dropped());
}


// Truncation is detected by the file shrinking, and the gap it leaves is
// recorded on the report rather than passed off as a continuous series.
TEST_F(ProgressReaderTest, ReportsTruncationAsADiscontinuity)
{
  Try<Owned<ProgressReader>> reader =
    ProgressReader::create(makePolicy(), os::getcwd());
  ASSERT_SOME(reader);

  append("progress: 30, before\n");
  ASSERT_SOME(reader.get()->sample());
  ASSERT_SOME(reader.get()->report());
  reader.get()->reported();

  ASSERT_SOME(os::write(source(), "progress: 60, after\n"));
  ASSERT_SOME(reader.get()->sample());

  Option<TaskStatus::Progress> report = reader.get()->report();
  ASSERT_SOME(report);
  EXPECT_DOUBLE_EQ(0.6, report->fraction());
  EXPECT_TRUE(strings::contains(report->message(), "truncated"));
  EXPECT_TRUE(strings::contains(report->message(), "after"));
  reader.get()->reported();

  // The note is reported once, not on every later report.
  append("progress: 90, later\n");
  ASSERT_SOME(reader.get()->sample());

  Option<TaskStatus::Progress> later = reader.get()->report();
  ASSERT_SOME(later);
  EXPECT_FALSE(strings::contains(later->message(), "truncated"));
}


// Rotation leaves the old path's size unchanged, so it is detected by the
// inode rather than by the size.
TEST_F(ProgressReaderTest, ReportsRotationAsADiscontinuity)
{
  Try<Owned<ProgressReader>> reader =
    ProgressReader::create(makePolicy(), os::getcwd());
  ASSERT_SOME(reader);

  append("progress: 30, before\n");
  ASSERT_SOME(reader.get()->sample());
  ASSERT_SOME(reader.get()->report());
  reader.get()->reported();

  ASSERT_SOME(os::rename(source(), path::join(os::getcwd(), "rotated.log")));
  ASSERT_SOME(os::write(source(), "progress: 35, after rotation\n"));
  ASSERT_SOME(reader.get()->sample());

  Option<TaskStatus::Progress> report = reader.get()->report();
  ASSERT_SOME(report);
  EXPECT_DOUBLE_EQ(0.35, report->fraction());
  EXPECT_TRUE(strings::contains(report->message(), "replaced"));
}


TEST_F(ProgressReaderTest, RefusesASourceThatIsNotARegularFile)
{
  ASSERT_SOME(os::mkdir(source()));

  Try<Owned<ProgressReader>> reader =
    ProgressReader::create(makePolicy(), os::getcwd());
  ASSERT_SOME(reader);

  Try<Nothing> sample = reader.get()->sample();
  ASSERT_ERROR(sample);
  EXPECT_TRUE(strings::contains(sample.error(), "regular file"));
}


// The stall clock measures time since the work value last advanced. A
// repeated value with a changed message is a heartbeat, and a heartbeat is
// not progress.
TEST_F(ProgressReaderTest, AHeartbeatDoesNotRearmTheStallClock)
{
  Try<Owned<ProgressReader>> reader =
    ProgressReader::create(makePolicy(Milliseconds(300)), os::getcwd());
  ASSERT_SOME(reader);
  reader.get()->arm();

  append("progress: 20, working\n");
  ASSERT_SOME(reader.get()->sample());
  EXPECT_FALSE(reader.get()->stalled());

  os::sleep(Milliseconds(200));
  append("progress: 20, still working\n");
  ASSERT_SOME(reader.get()->sample());

  os::sleep(Milliseconds(200));
  ASSERT_SOME(reader.get()->sample());

  EXPECT_TRUE(reader.get()->stalled());
  EXPECT_GE(reader.get()->sinceAdvance(), Milliseconds(300));
}


TEST_F(ProgressReaderTest, AnAdvancingValueRearmsTheStallClock)
{
  Try<Owned<ProgressReader>> reader =
    ProgressReader::create(makePolicy(Milliseconds(300)), os::getcwd());
  ASSERT_SOME(reader);
  reader.get()->arm();

  append("progress: 20, working\n");
  ASSERT_SOME(reader.get()->sample());

  os::sleep(Milliseconds(200));
  append("progress: 40, working\n");
  ASSERT_SOME(reader.get()->sample());

  os::sleep(Milliseconds(200));
  ASSERT_SOME(reader.get()->sample());

  EXPECT_FALSE(reader.get()->stalled());
}


// With no window named the stall clock is not armed, whatever the task
// does or fails to do.
TEST_F(ProgressReaderTest, NoWindowMeansNoStall)
{
  Try<Owned<ProgressReader>> reader =
    ProgressReader::create(makePolicy(), os::getcwd());
  ASSERT_SOME(reader);
  reader.get()->arm();

  os::sleep(Milliseconds(100));
  ASSERT_SOME(reader.get()->sample());

  EXPECT_FALSE(reader.get()->stalled());
}


// The stall window measures the task's own advance. The node's fetch and
// an image pull happen between this reader being built and the task
// running, and a task must not be killed for time it never had.
TEST_F(ProgressReaderTest, ADelayedStartDoesNotStall)
{
  Try<Owned<ProgressReader>> reader =
    ProgressReader::create(makePolicy(Milliseconds(200)), os::getcwd());
  ASSERT_SOME(reader);

  os::sleep(Milliseconds(500));

  reader.get()->arm();
  ASSERT_SOME(reader.get()->sample());

  EXPECT_FALSE(reader.get()->stalled());

  os::sleep(Milliseconds(300));
  ASSERT_SOME(reader.get()->sample());

  EXPECT_TRUE(reader.get()->stalled());
}


TEST_F(ProgressReaderTest, AnUnarmedReaderNeverStalls)
{
  Try<Owned<ProgressReader>> reader =
    ProgressReader::create(makePolicy(Milliseconds(100)), os::getcwd());
  ASSERT_SOME(reader);

  os::sleep(Milliseconds(300));
  ASSERT_SOME(reader.get()->sample());

  EXPECT_FALSE(reader.get()->stalled());
  EXPECT_EQ(Duration::zero(), reader.get()->sinceAdvance());
}


// A report the sender refuses must not take the discontinuity note or a
// sequence number with it, so nothing is consumed until `reported`.
TEST_F(ProgressReaderTest, KeepsADiscontinuityUntilItIsReported)
{
  Try<Owned<ProgressReader>> reader =
    ProgressReader::create(makePolicy(), os::getcwd());
  ASSERT_SOME(reader);

  append("progress: 30, before\n");
  ASSERT_SOME(reader.get()->sample());
  ASSERT_SOME(reader.get()->report());
  reader.get()->reported();

  ASSERT_SOME(os::write(source(), "progress: 60, after\n"));
  ASSERT_SOME(reader.get()->sample());

  Option<TaskStatus::Progress> refused = reader.get()->report();
  ASSERT_SOME(refused);
  EXPECT_TRUE(strings::contains(refused->message(), "truncated"));
  EXPECT_EQ(2u, refused->sequence());

  // Not accepted, so the same report is still pending.
  Option<TaskStatus::Progress> retried = reader.get()->report();
  ASSERT_SOME(retried);
  EXPECT_TRUE(strings::contains(retried->message(), "truncated"));
  EXPECT_EQ(2u, retried->sequence());

  reader.get()->reported();

  Option<TaskStatus::Progress> after = reader.get()->report();
  ASSERT_SOME(after);
  EXPECT_FALSE(strings::contains(after->message(), "truncated"));
  EXPECT_EQ(3u, after->sequence());
}


// A source that grows by more than one sample's worth is skipped forward,
// and the new offset can sit inside a line. What precedes the next newline
// is a fragment, and reading it as a line reports a value the task never
// printed.
TEST_F(ProgressReaderTest, DiscardsTheFragmentAfterASkipForward)
{
  progress::ProgressPolicy anchored = makePolicy();

  // Anchored at both ends, so the pattern matches the fragment
  // `progress: 99, bogus` and not the whole line it was cut from.
  anchored.pattern = "^progress: ([0-9]*\\.?[0-9]+), (.*)$";

  Try<Owned<ProgressReader>> reader =
    ProgressReader::create(anchored, os::getcwd());
  ASSERT_SOME(reader);
  reader.get()->arm();

  ASSERT_SOME(os::write(source(), ""));
  ASSERT_SOME(reader.get()->sample());

  // The straddling line is 21 bytes; the filler after it is one byte short
  // of a full sample, so the new offset lands one byte into it.
  const string head = "progress: 1, head\n";
  const string straddling = "xprogress: 99, bogus\n";
  const string filler =
    string(medios::internal::progress::MAX_SAMPLE_BYTES - 21u, 'f') + "\n";

  ASSERT_SOME(os::write(source(), head + straddling + filler));
  ASSERT_SOME(reader.get()->sample());

  EXPECT_NONE(reader.get()->fraction());

  Option<TaskStatus::Progress> report = reader.get()->report();
  ASSERT_SOME(report);
  EXPECT_FALSE(report->has_fraction());
  EXPECT_TRUE(strings::contains(report->message(), "were not read"));
}


TEST_F(ProgressReaderTest, RefusesAPatternThatDoesNotCompile)
{
  progress::ProgressPolicy broken = makePolicy();
  broken.pattern = "progress: ([0-9";

  Try<Owned<ProgressReader>> reader =
    ProgressReader::create(broken, os::getcwd());

  ASSERT_ERROR(reader);
  EXPECT_TRUE(strings::contains(reader.error(), "does not compile"));
}


TEST_F(ProgressReaderTest, RefusesAPatternWithoutACaptureGroup)
{
  progress::ProgressPolicy broken = makePolicy();
  broken.pattern = "progress: [0-9]+";

  Try<Owned<ProgressReader>> reader =
    ProgressReader::create(broken, os::getcwd());

  ASSERT_ERROR(reader);
  EXPECT_TRUE(strings::contains(reader.error(), "capture group"));
}


// --- The command executor, end to end ---
//
// The shapes are the smoke shapes of the per-task executor design: a task
// that exits without ever reporting, a task whose reported value advances
// while it runs, and a task that reports and then stops advancing.

class CommandExecutorProgressTest
  : public MediosTest,
    public WithParamInterface<bool>
{
protected:
  // Progress reading is independent of isolation, and the shapes here only
  // need a forked process and its sandbox. Naming no isolator and the
  // POSIX launcher keeps them off the host's cgroup hierarchy, which on a
  // host running a node belongs to that node.
  node::Flags progressNodeFlags()
  {
    node::Flags flags = CreateNodeFlags();
    flags.http_command_executor = GetParam();
    flags.isolation = "";
    flags.launcher = "posix";
    return flags;
  }
};


INSTANTIATE_TEST_CASE_P(
    HTTPCommandExecutor,
    CommandExecutorProgressTest,
    ::testing::Bool());


// A task that exits within a second, with no progress policy: the executor
// reports the lifecycle and never claims an observation it did not make.
TEST_P(CommandExecutorProgressTest, FastExitWithoutProgress)
{
  Try<Owned<cluster::Controller>> controller = StartController();
  ASSERT_SOME(controller);

  Owned<ControllerDetector> detector = controller.get()->createDetector();

  node::Flags flags = progressNodeFlags();

  // The container logger only writes the sandbox `stdout` file for a
  // containerizer that is not in local mode; in local mode the container
  // inherits this process's stdio and the file stays empty.
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
      offers->front().node_id(), offers->front().resources(), "exit 0");

  Future<TaskStatus> statusStarting, statusRunning, statusFinished;
  EXPECT_CALL(sched, statusUpdate(_, _))
    .WillOnce(FutureArg<1>(&statusStarting))
    .WillOnce(FutureArg<1>(&statusRunning))
    .WillOnce(FutureArg<1>(&statusFinished))
    .WillRepeatedly(Return());

  driver.launchTasks(offers->front().id(), {task});

  AWAIT_READY(statusStarting);
  EXPECT_EQ(TASK_STARTING, statusStarting->state());
  EXPECT_FALSE(statusStarting->has_progress());

  AWAIT_READY(statusRunning);
  EXPECT_EQ(TASK_RUNNING, statusRunning->state());
  EXPECT_FALSE(statusRunning->has_progress());

  AWAIT_READY(statusFinished);
  EXPECT_EQ(TASK_FINISHED, statusFinished->state());
  EXPECT_NE(TASK_FAILED, statusFinished->state());
  EXPECT_FALSE(statusFinished->has_progress());

  driver.stop();
  driver.join();
}


// A task printing the convention while it runs: the value reaches the
// framework on a TASK_RUNNING update and advances between reports.
TEST_P(CommandExecutorProgressTest, ProgressAdvancesWhileRunning)
{
  Try<Owned<cluster::Controller>> controller = StartController();
  ASSERT_SOME(controller);

  Owned<ControllerDetector> detector = controller.get()->createDetector();

  node::Flags flags = progressNodeFlags();

  // The container logger only writes the sandbox `stdout` file for a
  // containerizer that is not in local mode; in local mode the container
  // inherits this process's stdio and the file stays empty.
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
      offers->front().node_id(),
      offers->front().resources(),
      "for i in 10 30 50 70 90; do echo \"progress: $i, step $i\";"
      " sleep 2; done; sleep 600");

  task.mutable_progress_policy()->set_source(
      ::medios::ProgressPolicy::STDOUT);

  Future<TaskStatus> statusStarting, statusRunning;
  Future<TaskStatus> firstReport, secondReport;
  EXPECT_CALL(sched, statusUpdate(_, _))
    .WillOnce(FutureArg<1>(&statusStarting))
    .WillOnce(FutureArg<1>(&statusRunning))
    .WillOnce(FutureArg<1>(&firstReport))
    .WillOnce(FutureArg<1>(&secondReport))
    .WillRepeatedly(Return());

  driver.launchTasks(offers->front().id(), {task});

  AWAIT_READY(statusStarting);
  EXPECT_EQ(TASK_STARTING, statusStarting->state());

  AWAIT_READY(statusRunning);
  EXPECT_EQ(TASK_RUNNING, statusRunning->state());

  // The report cadence is 5 s, so two reports take upwards of ten
  // seconds of a task that runs for ten minutes.
  AWAIT_READY_FOR(firstReport, Seconds(60));
  EXPECT_EQ(TASK_RUNNING, firstReport->state());
  ASSERT_TRUE(firstReport->has_progress());
  ASSERT_TRUE(firstReport->progress().has_fraction());
  EXPECT_GT(firstReport->progress().fraction(), 0.0);
  EXPECT_TRUE(firstReport->progress().has_observed_at());
  EXPECT_TRUE(strings::startsWith(firstReport->progress().message(), "step"));

  AWAIT_READY_FOR(secondReport, Seconds(60));
  EXPECT_EQ(TASK_RUNNING, secondReport->state());
  ASSERT_TRUE(secondReport->has_progress());
  EXPECT_GT(
      secondReport->progress().fraction(),
      firstReport->progress().fraction());
  EXPECT_GT(
      secondReport->progress().sequence(),
      firstReport->progress().sequence());

  driver.killTask(task.task_id());

  driver.stop();
  driver.join();
}


// A task that reports and then wedges: the executor kills it through the
// kill-policy escalation and names the reason, the last value and the
// stall duration.
TEST_P(CommandExecutorProgressTest, StallKillReportsTheReason)
{
  Try<Owned<cluster::Controller>> controller = StartController();
  ASSERT_SOME(controller);

  Owned<ControllerDetector> detector = controller.get()->createDetector();

  node::Flags flags = progressNodeFlags();

  // The container logger only writes the sandbox `stdout` file for a
  // containerizer that is not in local mode; in local mode the container
  // inherits this process's stdio and the file stays empty.
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
      offers->front().node_id(),
      offers->front().resources(),
      "echo \"progress: 25, quarter\"; sleep 600");

  ::medios::ProgressPolicy* policy = task.mutable_progress_policy();
  policy->set_source(::medios::ProgressPolicy::STDOUT);
  setDuration(policy->mutable_sample_interval(), Milliseconds(200));
  setDuration(policy->mutable_stall_window(), Seconds(2));
  policy->set_action(::medios::ProgressPolicy::KILL);

  // The value is read a sample interval after the launch and reported then,
  // so the report precedes the stall by the length of the stall window.
  Future<TaskStatus> statusStarting, statusRunning;
  Future<TaskStatus> progressReport, statusFailed;
  EXPECT_CALL(sched, statusUpdate(_, _))
    .WillOnce(FutureArg<1>(&statusStarting))
    .WillOnce(FutureArg<1>(&statusRunning))
    .WillOnce(FutureArg<1>(&progressReport))
    .WillOnce(FutureArg<1>(&statusFailed))
    .WillRepeatedly(Return());

  driver.launchTasks(offers->front().id(), {task});

  AWAIT_READY(statusStarting);
  EXPECT_EQ(TASK_STARTING, statusStarting->state());

  AWAIT_READY(statusRunning);
  EXPECT_EQ(TASK_RUNNING, statusRunning->state());

  AWAIT_READY_FOR(progressReport, Seconds(60));
  EXPECT_EQ(TASK_RUNNING, progressReport->state());
  ASSERT_TRUE(progressReport->has_progress());
  EXPECT_DOUBLE_EQ(0.25, progressReport->progress().fraction());

  AWAIT_READY_FOR(statusFailed, Seconds(60));
  EXPECT_EQ(TASK_FAILED, statusFailed->state());
  EXPECT_NE(TASK_FINISHED, statusFailed->state());
  EXPECT_NE(TASK_KILLED, statusFailed->state());
  EXPECT_EQ(TaskStatus::REASON_PROGRESS_STALLED, statusFailed->reason());
  EXPECT_TRUE(
      strings::contains(statusFailed->message(), "REASON_PROGRESS_STALLED"));
  EXPECT_TRUE(strings::contains(statusFailed->message(), "0.25"));

  ASSERT_TRUE(statusFailed->has_progress());
  EXPECT_DOUBLE_EQ(0.25, statusFailed->progress().fraction());

  driver.stop();
  driver.join();
}


// The same shape with a task that exits 0 when it is signalled: the kill
// decides the terminal state, so a stall kill is never TASK_FINISHED.
TEST_P(CommandExecutorProgressTest, StallKillIsNotFinished)
{
  Try<Owned<cluster::Controller>> controller = StartController();
  ASSERT_SOME(controller);

  Owned<ControllerDetector> detector = controller.get()->createDetector();

  node::Flags flags = progressNodeFlags();

  // The container logger only writes the sandbox `stdout` file for a
  // containerizer that is not in local mode; in local mode the container
  // inherits this process's stdio and the file stays empty.
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
      offers->front().node_id(),
      offers->front().resources(),
      "trap 'exit 0' TERM; echo \"progress: 50, half\"; sleep 600");

  ::medios::ProgressPolicy* policy = task.mutable_progress_policy();
  policy->set_source(::medios::ProgressPolicy::STDOUT);
  setDuration(policy->mutable_sample_interval(), Milliseconds(200));
  setDuration(policy->mutable_stall_window(), Seconds(2));
  policy->set_action(::medios::ProgressPolicy::KILL);

  Future<TaskStatus> statusStarting, statusRunning;
  Future<TaskStatus> progressReport, statusTerminal;
  EXPECT_CALL(sched, statusUpdate(_, _))
    .WillOnce(FutureArg<1>(&statusStarting))
    .WillOnce(FutureArg<1>(&statusRunning))
    .WillOnce(FutureArg<1>(&progressReport))
    .WillOnce(FutureArg<1>(&statusTerminal))
    .WillRepeatedly(Return());

  driver.launchTasks(offers->front().id(), {task});

  AWAIT_READY(statusStarting);
  AWAIT_READY(statusRunning);
  AWAIT_READY_FOR(progressReport, Seconds(60));

  AWAIT_READY_FOR(statusTerminal, Seconds(60));
  EXPECT_NE(TASK_FINISHED, statusTerminal->state());
  EXPECT_EQ(TASK_FAILED, statusTerminal->state());
  EXPECT_EQ(TaskStatus::REASON_PROGRESS_STALLED, statusTerminal->reason());

  driver.stop();
  driver.join();
}


// A framework kill of a task that exits 0 when it is signalled is
// TASK_KILLED, not TASK_FINISHED.
TEST_P(CommandExecutorProgressTest, FrameworkKillIsNotFinished)
{
  Try<Owned<cluster::Controller>> controller = StartController();
  ASSERT_SOME(controller);

  Owned<ControllerDetector> detector = controller.get()->createDetector();

  node::Flags flags = progressNodeFlags();

  // The container logger only writes the sandbox `stdout` file for a
  // containerizer that is not in local mode; in local mode the container
  // inherits this process's stdio and the file stays empty.
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
      offers->front().node_id(),
      offers->front().resources(),
      "trap 'exit 0' TERM; sleep 600");

  Future<TaskStatus> statusStarting, statusRunning;
  EXPECT_CALL(sched, statusUpdate(_, _))
    .WillOnce(FutureArg<1>(&statusStarting))
    .WillOnce(FutureArg<1>(&statusRunning));

  driver.launchTasks(offers->front().id(), {task});

  AWAIT_READY(statusStarting);
  AWAIT_READY(statusRunning);
  EXPECT_EQ(TASK_RUNNING, statusRunning->state());

  Future<TaskStatus> statusTerminal;
  EXPECT_CALL(sched, statusUpdate(_, _))
    .WillOnce(FutureArg<1>(&statusTerminal))
    .WillRepeatedly(Return());

  driver.killTask(task.task_id());

  AWAIT_READY_FOR(statusTerminal, Seconds(60));
  EXPECT_NE(TASK_FINISHED, statusTerminal->state());
  EXPECT_EQ(TASK_KILLED, statusTerminal->state());

  driver.stop();
  driver.join();
}


// Report-only: the stall reaches the framework on a TASK_RUNNING update
// and the task keeps running, which the framework kill at the end proves.
TEST_P(CommandExecutorProgressTest, StallReportDoesNotKill)
{
  Try<Owned<cluster::Controller>> controller = StartController();
  ASSERT_SOME(controller);

  Owned<ControllerDetector> detector = controller.get()->createDetector();

  node::Flags flags = progressNodeFlags();

  // The container logger only writes the sandbox `stdout` file for a
  // containerizer that is not in local mode; in local mode the container
  // inherits this process's stdio and the file stays empty.
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
      offers->front().node_id(),
      offers->front().resources(),
      "echo \"progress: 10, start\"; sleep 600");

  ::medios::ProgressPolicy* policy = task.mutable_progress_policy();
  policy->set_source(::medios::ProgressPolicy::STDOUT);
  setDuration(policy->mutable_sample_interval(), Milliseconds(200));
  setDuration(policy->mutable_stall_window(), Seconds(2));
  policy->set_action(::medios::ProgressPolicy::REPORT);

  Future<TaskStatus> statusStarting, statusRunning;
  Future<TaskStatus> progressReport, stallReport;
  EXPECT_CALL(sched, statusUpdate(_, _))
    .WillOnce(FutureArg<1>(&statusStarting))
    .WillOnce(FutureArg<1>(&statusRunning))
    .WillOnce(FutureArg<1>(&progressReport))
    .WillOnce(FutureArg<1>(&stallReport));

  driver.launchTasks(offers->front().id(), {task});

  AWAIT_READY(statusStarting);
  AWAIT_READY(statusRunning);

  AWAIT_READY_FOR(progressReport, Seconds(60));
  EXPECT_EQ(TASK_RUNNING, progressReport->state());

  AWAIT_READY_FOR(stallReport, Seconds(60));
  EXPECT_EQ(TASK_RUNNING, stallReport->state());
  EXPECT_NE(TASK_FAILED, stallReport->state());
  EXPECT_TRUE(
      strings::contains(stallReport->message(), "REASON_PROGRESS_STALLED"));
  ASSERT_TRUE(stallReport->has_progress());
  EXPECT_DOUBLE_EQ(0.1, stallReport->progress().fraction());

  Future<TaskStatus> statusKilled;
  EXPECT_CALL(sched, statusUpdate(_, _))
    .WillOnce(FutureArg<1>(&statusKilled))
    .WillRepeatedly(Return());

  driver.killTask(task.task_id());

  AWAIT_READY_FOR(statusKilled, Seconds(60));
  EXPECT_EQ(TASK_KILLED, statusKilled->state());

  driver.stop();
  driver.join();
}

} // namespace tests {
} // namespace internal {
} // namespace medios {
