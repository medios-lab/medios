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

#ifndef __LAUNCHER_OBSERVER_HPP__
#define __LAUNCHER_OBSERVER_HPP__

#include <sys/types.h>

#include <string>
#include <vector>

#include <google/protobuf/repeated_field.h>

#include <medios/medios.hpp>

#include <process/future.hpp>
#include <process/owned.hpp>
#include <process/subprocess.hpp>

#include <stout/duration.hpp>
#include <stout/lambda.hpp>
#include <stout/nothing.hpp>
#include <stout/option.hpp>
#include <stout/try.hpp>

#include "common/validation.hpp"

namespace medios {
namespace internal {
namespace observer {

// How often the executor reads new bytes from its observers' stdout pipes.
//
// Parameter, owned by the executor, default 1000 ms, provisional. It is
// also the executor's update rate while an observer is printing: a new line
// is a reason to send a progress-only `TASK_RUNNING`, and a line that
// arrives between drains is overtaken by the next one rather than sent. The
// progress sampler's default interval, so an observer line and a progress
// line reach a status update on the same cadence. The update rate under
// many observed tasks is unmeasured; it is research question 4 of DR-041
// together with the cost per task-hour.
constexpr Duration DEFAULT_DRAIN_INTERVAL = Milliseconds(1000);


// The most bytes read from one observer's pipe in one drain.
//
// Parameter, owned by the executor, default 64 KiB, provisional. Only the
// newest line of a drain reaches the record, so reading more would buy
// nothing but the executor's own memory. An observer that outruns this
// keeps the rest in the pipe for the next drain, and the kernel's pipe
// buffer backpressures it rather than the executor.
constexpr size_t MAX_DRAIN_BYTES = 65536;


// The most bytes held for one observer line.
//
// Invariant of the message bound: a line is carried as the progress
// message, which DR-038 bounds at 4096 bytes, so bytes past this one
// cannot reach the framework. They are read and dropped rather than
// buffered, and the line keeps its beginning: an observer puts its verdict
// first. The name prefix is counted again when the message is built, so
// the bytes of a line that reach the framework are this bound less the
// name and two more.
constexpr size_t MAX_LINE_BYTES =
  common::validation::PROGRESS_MESSAGE_MAX_LENGTH;


// Forks one observer and returns its subprocess, whose stdout must be a
// pipe and whose stderr must be the file at `stderrPath`.
//
// The two built-in executors reach the task's uid differently, so neither
// path belongs here: the command executor launches an observer through
// `medios-containerizer launch` with the task's user, the same lifecycle
// that starts the task itself, while the docker executor forks it on the
// host, where the task's own uid lives inside the container.
using Fork = lambda::function<Try<process::Subprocess>(
    const TaskObserver& observer,
    const std::string& stderrPath)>;


// Stamps the ordering on a report an executor is about to send, from one
// counter per task attempt.
//
// `TaskStatus.Progress.sequence` is what tells a report that arrives after
// a newer one from the newer one, so a task's reports carry one sequence
// whatever produced them. The progress reader counts only its own reports
// and an observation can be the whole report, so the executor owns the
// counter and this writes it, over whatever the reader put there.
//
// `observedAt` is left alone when the report already carries one: the
// reader's is the time it read the source, which is what the report is
// about. An observation-only report takes the time its line was read.
void stampReport(
    TaskStatus::Progress* progress,
    uint64_t sequence,
    const Option<double>& observedAt);


// The observers of one task: their processes, their stdout pipes, and the
// newest line any of them has written.
//
// An observer's output reaches the task record only as a progress message
// (DR-041). This holds no work value, no sequence and no stall clock, so
// nothing an observer says can mask a stall or create one.
//
// Not thread safe. The command executor drives it from its libprocess
// event queue; the docker executor drives it under `updateMutex`.
class ObserverSet
{
public:
  // Creates the sandbox `observers` directory the observers' stderr files
  // live in. A sandbox this executor does not know, or a directory that
  // cannot be created, is every observer failing to launch, which `launch`
  // records as a line each, so building this never fails: an unobservable
  // task still runs (DR-041).
  static process::Owned<ObserverSet> create(
      const google::protobuf::RepeatedPtrField<TaskObserver>& observers,
      const std::string& sandboxDirectory);

  ~ObserverSet();

  // Forks every observer. The task never waits for this: an observer that
  // does not start is one line in the record and nothing else.
  void launch(const Fork& fork);

  // Reads whatever the observers have written since the last drain and
  // keeps the newest complete line. An observer that has reached the end of
  // its pipe and exited contributes its exit as a line, held back until the
  // last thing it observed has been reported: an exit says less than the
  // verdict it would otherwise hide. The exit of an observer this executor
  // signalled is not recorded at all, being the executor's doing and not an
  // observation.
  void drain();

  // The newest observation, prefixed with its observer's name and bounded
  // by the progress message length, or `None` when no observer has written
  // a complete line.
  Option<std::string> line() const;

  // Whether `line` holds an observation the caller has not yet reported.
  bool hasUnreported() const;

  // The wall clock in seconds since the epoch at which the newest line was
  // read, which a report carrying that observation takes as its
  // `observed_at`.
  Option<double> observedAt() const;

  // The message a report carries, given the reader's own message on it:
  // the observers' newest line for as long as the reader's message has not
  // changed since that line was read, and `None` (the reader's) once it
  // has. Newest wins between the two, so a verdict stays on the record
  // until the task itself says something newer. Consumes the pending line.
  Option<std::string> fold(const Option<std::string>& readerMessage);

  // Accepts the last `line` as sent. The caller calls this on a status it
  // has handed to its sender, and nothing is consumed until then: a status
  // the sender refuses would otherwise take an observation with it.
  void reported();

  // Increases with every line accepted, so a caller can tell a line it has
  // already sent from a newer one. Zero until the first line.
  uint64_t generation() const;

  // Signals every observer's process group and session with SIGTERM, then
  // with SIGKILL. The caller runs the escalation on the task's own deadline:
  // the observers get the same escalation the task gets. After the first of
  // these, an observer's exit is no longer recorded as an observation.
  //
  // Both signal the departed leader's group and session as well as the
  // process, so a child an observer left behind is reached after the
  // observer itself has gone.
  void signalTerm();
  void signalKill();

  // The observers that were forked and have not been seen to exit, for
  // tests that assert an observer does not outlive its task.
  //
  // An empty result does not mean nothing of the observers is left: an
  // observer that exited can have left a child in its group, which is why
  // the escalation signals rather than consults this.
  std::vector<pid_t> running() const;

  // Whether any observer was forked, so a caller can tell a set that needs
  // an escalation from one whose observers all failed to launch.
  bool launched() const;

private:
  struct Observer
  {
    // What the framework asked for.
    TaskObserver declaration;

    // `None` when the fork failed, which is recorded as a line instead.
    Option<process::Subprocess> process;

    // Whether this observer's pipe still has bytes to come.
    bool reading = false;

    // A line the observer has not finished writing, bounded by
    // `MAX_LINE_BYTES`.
    std::string partial;

    // Whether the rest of the line being written is past that bound, and
    // so is read and dropped until the next newline.
    bool skipping = false;

    bool exitRecorded = false;
  };

  ObserverSet(
      std::vector<Observer>&& _observers,
      const std::string& _directory,
      const Option<std::string>& _directoryError);

  // Takes one complete line from one observer as the newest observation.
  void accept(const std::string& name, const std::string& text);

  // Reads one observer's pipe up to `MAX_DRAIN_BYTES`, accepting each
  // complete line.
  void read(Observer* observer);

  // Signals every launched observer's process, group and session.
  void signal(int signal);

  std::vector<Observer> observers;

  // The sandbox `observers` directory the stderr files live in, and why it
  // is not there when it is not.
  const std::string directory;
  const Option<std::string> directoryError;

  Option<std::string> newest;
  Option<double> newestAt;
  uint64_t lines = 0u;
  uint64_t reportedLines = 0u;

  // Set by the first signal, after which an exit is the executor's doing.
  bool terminating = false;

  // The reader's message as of the last fold, and whether an observer line
  // was read after it last changed.
  Option<std::string> readerSeen;
  bool observerNewer = false;
};

} // namespace observer {
} // namespace internal {
} // namespace medios {

#endif // __LAUNCHER_OBSERVER_HPP__
