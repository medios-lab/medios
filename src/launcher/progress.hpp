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

#ifndef __LAUNCHER_PROGRESS_HPP__
#define __LAUNCHER_PROGRESS_HPP__

#include <sys/types.h>

#include <chrono>
#include <regex>
#include <string>

#include <medios/medios.hpp>

#include <process/owned.hpp>

#include <stout/duration.hpp>
#include <stout/nothing.hpp>
#include <stout/option.hpp>
#include <stout/try.hpp>

namespace medios {
namespace internal {
namespace progress {

// How often the executor reads new bytes from the progress source.
//
// Parameter, per task, default 1000 ms, provisional. Cook's environment
// default, the only sampled-progress interval with a deployment behind it.
// A task names its own in `TaskInfo.progress_policy.sample_interval`.
constexpr Duration DEFAULT_SAMPLE_INTERVAL = Milliseconds(1000);


// How often a progress-only `TASK_RUNNING` update is sent while the task
// runs. Lifecycle updates carry the newest report regardless of this.
//
// Parameter, owned by the executor, default 5000 ms, provisional. Five
// samples per report at the default interval, about twelve updates per
// minute per task. Unmeasured against controller load; research question 2
// of the per-task executor design.
constexpr Duration REPORT_CADENCE = Milliseconds(5000);


// The expression applied to each line of the progress source. Capture group
// one is the work value as a percent; capture group two, when the pattern
// has one, is the message.
//
// Parameter, per task, provisional. Cook's default convention, which is
// what the first jobs to use this already print. A task names its own in
// `TaskInfo.progress_policy.pattern`.
constexpr char DEFAULT_PATTERN[] = "progress: ([0-9]*\\.?[0-9]+), (.*)";


// The domain of capture group one, in percent. A value outside it is
// dropped as an observation error rather than clamped, because a job
// printing 120 is miscounting and clamping would hide that.
//
// Invariant of the convention.
constexpr double VALUE_MIN_PERCENT = 0.0;
constexpr double VALUE_MAX_PERCENT = 100.0;


// The most bytes read from the progress source in one sample.
//
// Parameter, owned by the executor, default 1 MiB, provisional. It bounds
// the executor's own memory per sample and covers a task writing 1 MB/s at
// the default interval. A tail longer than this is skipped forward to its
// last MiB and the skip is recorded as a discontinuity, because falling
// permanently behind a chatty task would be worse than losing lines.
constexpr size_t MAX_SAMPLE_BYTES = 1048576;


// The most bytes held for a line the task has not finished writing.
//
// Parameter, owned by the executor, default 64 KiB, provisional. A single
// line longer than this cannot be a progress line under any plausible
// convention, so it is discarded and recorded as a discontinuity.
constexpr size_t MAX_PARTIAL_LINE_BYTES = 65536;


// Where the executor reads the task's progress from, how it reads it, and
// what it does when the work value stops advancing.
//
// `progressPolicyFrom` is the only reader of `TaskInfo.progress_policy`
// (DR-040), so the carrier changes in one place.
struct ProgressPolicy
{
  enum Source
  {
    // The task's `stdout` file in the sandbox.
    STDOUT,

    // The file named by `path`.
    PATH
  };

  enum Action
  {
    // Kill the task through the kill-policy escalation and report
    // `TASK_FAILED` with `REASON_PROGRESS_STALLED`.
    KILL,

    // Report the stall on a `TASK_RUNNING` update and leave the task alone.
    REPORT
  };

  Source source;

  // Set when `source` is `PATH`: the progress file, relative to the
  // sandbox.
  Option<std::string> path;

  std::string pattern;

  Duration sampleInterval;

  // Absent means the stall clock is not armed: no stall kill and no stall
  // report. There is no default, because a shard and a half-hour research
  // job differ by orders of magnitude and a wrong window kills working
  // tasks.
  Option<Duration> stallWindow;

  // Meaningful only when `stallWindow` is set, and required then.
  Action action;
};


// The task's progress policy, or `None` when the task carries none, which
// means no progress reading and no stall kill.
//
// An error is a task-shape error: a setting is present and unusable. The
// executor fails the task rather than reading something the framework did
// not ask for.
Try<Option<ProgressPolicy>> progressPolicyFrom(const TaskInfo& task);


// Reads a task's progress convention from a regular file in its sandbox and
// holds the stall clock.
//
// Not thread safe. The command executor drives it from its libprocess
// event queue; the docker executor drives it from one sampling thread.
class ProgressReader
{
public:
  // Fails when the pattern does not compile, when it has no capture group
  // for the work value, or when the named path is not inside the sandbox.
  // The source file need not exist yet: a task that has written nothing has
  // reported nothing, which is not an error.
  static Try<process::Owned<ProgressReader>> create(
      const ProgressPolicy& policy,
      const std::string& sandboxDirectory);

  // Starts the stall clock. The caller calls this when the task itself
  // starts running, which is later than when this reader is built: the
  // node's fetch and an image pull happen in between, and the stall window
  // measures the task's own advance, not the time to get it running. An
  // unarmed reader never reports a stall.
  void arm();

  // Reads whatever the task has written since the last sample and folds it
  // into the reader's state. Truncation and rotation of the source are
  // detected by inode and size, the file is reopened, and the gap is
  // recorded as a discontinuity on the next report.
  //
  // An error is an observation failure in the executor, never a task
  // failure: the caller logs it and keeps sampling.
  Try<Nothing> sample();

  // Whether the work value has not advanced for longer than the policy's
  // stall window. Always false when the policy names no window.
  bool stalled() const;

  // Monotonic time since the work value last advanced, or since this reader
  // was created when it has never advanced. A task that has printed nothing
  // has not advanced, so its stall clock runs from the launch.
  Duration sinceAdvance() const;

  // The newest accepted work value, as a fraction in [0, 1].
  Option<double> fraction() const;

  // The report to put on a status update, or `None` when the task has
  // reported nothing and no discontinuity is pending: an absent field
  // means unknown, and an empty report would claim an observation never
  // made.
  //
  // The caller calls this on a status it is about to send, and nothing is
  // consumed until `reported`. A status the sender refuses would otherwise
  // take a pending discontinuity note and a sequence number with it.
  Option<TaskStatus::Progress> report() const;

  // Accepts the last `report` as sent: the sequence advances and a pending
  // discontinuity note is cleared.
  void reported();

  // Whether `report` would return a report.
  bool hasReport() const;

  // Observations dropped because the value did not parse, fell outside the
  // percent domain, or went backwards.
  uint64_t dropped() const;

private:
  ProgressReader(
      const ProgressPolicy& _policy,
      const std::string& _path,
      const std::regex& _pattern,
      size_t _valueGroup,
      Option<size_t> _messageGroup);

  // Opens the source at `offset`, or reports that it is not there yet.
  Try<Nothing> readTail();

  // Folds one complete line into the reader's state.
  void consume(const std::string& line);

  const ProgressPolicy policy;

  // The progress source, absolute on the host.
  const std::string path;

  const std::regex pattern;
  const size_t valueGroup;
  const Option<size_t> messageGroup;

  // Where the next read starts, and what the file looked like at the last
  // read, so a shrink or a new inode is recognized as a discontinuity.
  uint64_t offset;
  Option<ino_t> inode;

  // A line the task has not finished writing.
  std::string partial;

  // Set when the offset was moved forward past unread bytes, so it may sit
  // inside a line. Everything up to the next newline is then a fragment of
  // a line, not a line.
  bool awaitingLineStart;

  Option<double> value;
  std::string message;

  uint64_t sequence;
  uint64_t droppedObservations;

  // Set when the source was truncated, rotated or skipped forward; carried
  // into the next report's message and cleared there.
  Option<std::string> discontinuity;

  // When the work value last advanced, or when the clock was armed if it
  // never has. Monotonic, from `std::chrono::steady_clock`: the stall
  // clock must not move when the system clock is stepped. `None` until
  // `arm`.
  Option<std::chrono::steady_clock::time_point> advancedAt;

  // Wall clock, seconds since the epoch, of the last completed sample.
  Option<double> observedAt;
};

} // namespace progress {
} // namespace internal {
} // namespace medios {

#endif // __LAUNCHER_PROGRESS_HPP__
