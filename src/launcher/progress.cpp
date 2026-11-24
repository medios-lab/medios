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

#include "launcher/progress.hpp"

#include <fcntl.h>

#include <algorithm>
#include <cmath>

#include <glog/logging.h>

#include <stout/bytes.hpp>
#include <stout/foreach.hpp>
#include <stout/numify.hpp>
#include <stout/os.hpp>
#include <stout/path.hpp>
#include <stout/stringify.hpp>
#include <stout/strings.hpp>

#include <stout/os/close.hpp>
#include <stout/os/lseek.hpp>
#include <stout/os/open.hpp>
#include <stout/os/read.hpp>
#include <stout/os/stat.hpp>

#include "common/validation.hpp"

using process::Owned;

using std::string;

namespace medios {
namespace internal {
namespace progress {

namespace {

// The sandbox file the node redirects a task's standard output to.
constexpr char STDOUT_FILENAME[] = "stdout";


double now()
{
  return std::chrono::duration_cast<std::chrono::duration<double>>(
      std::chrono::system_clock::now().time_since_epoch()).count();
}

} // namespace {


Try<Option<ProgressPolicy>> progressPolicyFrom(const TaskInfo& task)
{
  if (!task.has_progress_policy()) {
    return None();
  }

  const ::medios::ProgressPolicy& declared = task.progress_policy();

  ProgressPolicy policy;

  // The controller refuses each of the shapes below at submission
  // (DR-040). The executor checks them again rather than reading a file
  // the framework did not name: a task record can reach an executor
  // without passing this controller's validation.
  if (!declared.has_source()) {
    return Error("Task's 'progress_policy' names no 'source'");
  }

  if (declared.source() == ::medios::ProgressPolicy::STDOUT) {
    policy.source = ProgressPolicy::STDOUT;

    if (declared.has_path()) {
      return Error(
          "Task's 'progress_policy.path' is set with 'source' STDOUT");
    }
  } else {
    policy.source = ProgressPolicy::PATH;

    if (!declared.has_path() || declared.path().empty()) {
      return Error(
          "Task's 'progress_policy.source' is FILE without a 'path'");
    }

    if (strings::startsWith(declared.path(), "/")) {
      return Error(
          "Task's 'progress_policy.path' is '" + declared.path() +
          "'; a progress path is relative to the task's sandbox");
    }

    foreach (const string& component,
             strings::tokenize(declared.path(), "/")) {
      if (component == "..") {
        return Error(
            "Task's 'progress_policy.path' is '" + declared.path() +
            "'; a progress path may not leave the task's sandbox");
      }
    }

    policy.path = declared.path();
  }

  policy.pattern = declared.has_pattern()
    ? declared.pattern()
    : string(DEFAULT_PATTERN);

  policy.sampleInterval = DEFAULT_SAMPLE_INTERVAL;
  if (declared.has_sample_interval()) {
    const Duration interval =
      Nanoseconds(declared.sample_interval().nanoseconds());

    if (interval <= Duration::zero()) {
      return Error(
          "Task's 'progress_policy.sample_interval' is " +
          stringify(interval) + "; it must be positive");
    }

    policy.sampleInterval = interval;
  }

  if (declared.has_stall_window()) {
    const Duration window =
      Nanoseconds(declared.stall_window().nanoseconds());

    if (window <= Duration::zero()) {
      return Error(
          "Task's 'progress_policy.stall_window' is " + stringify(window) +
          "; it must be positive");
    }

    policy.stallWindow = window;
  }

  // Killing a task that stopped advancing and only reporting it are
  // different outcomes for the framework, so the choice is made by the
  // task and never inferred from the window being set.
  if (policy.stallWindow.isNone()) {
    if (declared.has_action()) {
      return Error(
          "Task's 'progress_policy.action' is set without a 'stall_window',"
          " so there is no stall to act on");
    }

    // Unread when no window is set; the stall clock is not armed.
    policy.action = ProgressPolicy::REPORT;
  } else if (!declared.has_action()) {
    return Error(
        "Task's 'progress_policy.stall_window' is set without an 'action'");
  } else {
    policy.action = declared.action() == ::medios::ProgressPolicy::KILL
      ? ProgressPolicy::KILL
      : ProgressPolicy::REPORT;
  }

  return policy;
}


Try<Owned<ProgressReader>> ProgressReader::create(
    const ProgressPolicy& policy,
    const string& sandboxDirectory)
{
  std::regex pattern;
  try {
    pattern = std::regex(policy.pattern, std::regex::ECMAScript);
  } catch (const std::regex_error& e) {
    return Error(
        "Progress pattern '" + policy.pattern + "' does not compile: " +
        e.what());
  }

  const size_t groups = pattern.mark_count();
  if (groups < 1) {
    return Error(
        "Progress pattern '" + policy.pattern + "' has no capture group;"
        " capture group one is the work value");
  }

  // Capture group two is the message when the pattern has one. A pattern
  // with only the value reports no message.
  const Option<size_t> messageGroup =
    groups >= 2 ? Option<size_t>(2u) : None();

  const string path = policy.source == ProgressPolicy::STDOUT
    ? path::join(sandboxDirectory, STDOUT_FILENAME)
    : path::join(sandboxDirectory, policy.path.get());

  return Owned<ProgressReader>(
      new ProgressReader(policy, path, pattern, 1u, messageGroup));
}


ProgressReader::ProgressReader(
    const ProgressPolicy& _policy,
    const string& _path,
    const std::regex& _pattern,
    size_t _valueGroup,
    Option<size_t> _messageGroup)
  : policy(_policy),
    path(_path),
    pattern(_pattern),
    valueGroup(_valueGroup),
    messageGroup(_messageGroup),
    offset(0u),
    inode(None()),
    awaitingLineStart(false),
    value(None()),
    sequence(0u),
    droppedObservations(0u),
    discontinuity(None()),
    advancedAt(None()),
    observedAt(None()) {}


void ProgressReader::arm()
{
  advancedAt = std::chrono::steady_clock::now();
}


Try<Nothing> ProgressReader::sample()
{
  Try<Nothing> read = readTail();

  // The sample time is the read time whether or not anything was there to
  // read: it dates the observation, including the observation that the
  // value has not moved.
  observedAt = now();

  return read;
}


Try<Nothing> ProgressReader::readTail()
{
  if (!os::exists(path)) {
    // A task that has written nothing has reported nothing.
    return Nothing();
  }

  if (!os::stat::isfile(path)) {
    return Error(
        "Progress source '" + path + "' is not a regular file");
  }

  Try<ino_t> currentInode = os::stat::inode(path);
  if (currentInode.isError()) {
    return Error(
        "Failed to stat progress source '" + path + "': " +
        currentInode.error());
  }

  Try<Bytes> currentSize = os::stat::size(path);
  if (currentSize.isError()) {
    return Error(
        "Failed to size progress source '" + path + "': " +
        currentSize.error());
  }

  const uint64_t size = currentSize->bytes();

  if (inode.isSome() && inode.get() != currentInode.get()) {
    discontinuity =
      "progress source was replaced after " + stringify(offset) +
      " bytes; reading resumed from the start of the new file";
    offset = 0u;
    partial.clear();
  } else if (size < offset) {
    discontinuity =
      "progress source was truncated from " + stringify(offset) + " to " +
      stringify(size) + " bytes; reading resumed from the start";
    offset = 0u;
    partial.clear();
  }

  inode = currentInode.get();

  if (size <= offset) {
    return Nothing();
  }

  if (size - offset > MAX_SAMPLE_BYTES) {
    const uint64_t skipped = size - offset - MAX_SAMPLE_BYTES;
    discontinuity =
      "progress source grew by more than " + stringify(MAX_SAMPLE_BYTES) +
      " bytes in one sample; " + stringify(skipped) +
      " bytes were not read";
    offset = size - MAX_SAMPLE_BYTES;
    partial.clear();
    awaitingLineStart = true;
  }

  Try<int_fd> fd = os::open(path, O_RDONLY | O_CLOEXEC);
  if (fd.isError()) {
    return Error(
        "Failed to open progress source '" + path + "': " + fd.error());
  }

  Try<off_t> seek = os::lseek(fd.get(), offset, SEEK_SET);
  if (seek.isError()) {
    os::close(fd.get());
    return Error(
        "Failed to seek progress source '" + path + "' to " +
        stringify(offset) + ": " + seek.error());
  }

  Result<string> bytes = os::read(fd.get(), size - offset);
  os::close(fd.get());

  if (bytes.isError()) {
    return Error(
        "Failed to read progress source '" + path + "': " + bytes.error());
  }

  if (bytes.isNone()) {
    return Nothing();
  }

  offset += bytes->size();

  partial += bytes.get();

  // The new offset may sit inside a line, so what precedes the first
  // newline is a fragment. Reading it as a line would report a value the
  // task never printed.
  if (awaitingLineStart) {
    const size_t newline = partial.find('\n');
    if (newline == std::string::npos) {
      partial.clear();
      return Nothing();
    }

    partial = partial.substr(newline + 1u);
    awaitingLineStart = false;
  }

  size_t start = 0u;
  for (size_t i = 0u; i < partial.size(); ++i) {
    if (partial[i] == '\n') {
      consume(partial.substr(start, i - start));
      start = i + 1u;
    }
  }

  partial = partial.substr(start);

  if (partial.size() > MAX_PARTIAL_LINE_BYTES) {
    discontinuity =
      "a progress source line exceeded " +
      stringify(MAX_PARTIAL_LINE_BYTES) + " bytes and was discarded";
    partial.clear();
  }

  return Nothing();
}


void ProgressReader::consume(const string& line)
{
  std::smatch match;
  if (!std::regex_search(line, match, pattern)) {
    return;
  }

  Try<double> percent = numify<double>(match[valueGroup].str());
  if (percent.isError() || !std::isfinite(percent.get())) {
    ++droppedObservations;
    LOG(WARNING) << "Dropped a progress observation: '"
                 << match[valueGroup].str() << "' is not a number";
    return;
  }

  if (percent.get() < VALUE_MIN_PERCENT ||
      percent.get() > VALUE_MAX_PERCENT) {
    ++droppedObservations;
    LOG(WARNING) << "Dropped a progress observation: " << percent.get()
                 << " is outside the convention's " << VALUE_MIN_PERCENT
                 << " to " << VALUE_MAX_PERCENT << " percent domain";
    return;
  }

  const double fraction = percent.get() / VALUE_MAX_PERCENT;

  // A work value that goes backwards is invalid evidence, not progress: it
  // neither replaces the value nor rearms the stall clock.
  if (value.isSome() && fraction < value.get()) {
    ++droppedObservations;
    LOG(WARNING) << "Dropped a progress observation: " << fraction
                 << " is behind the last accepted value " << value.get();
    return;
  }

  const bool advanced = value.isNone() || fraction > value.get();

  value = fraction;

  if (messageGroup.isSome() && match.size() > messageGroup.get()) {
    message = match[messageGroup.get()].str();
  }

  // Only the work value rearms the stall clock. A repeated value with a
  // changed message is a heartbeat, and a heartbeat is not progress.
  if (advanced) {
    advancedAt = std::chrono::steady_clock::now();
  }
}


bool ProgressReader::stalled() const
{
  if (policy.stallWindow.isNone() || advancedAt.isNone()) {
    return false;
  }

  return sinceAdvance() >= policy.stallWindow.get();
}


Duration ProgressReader::sinceAdvance() const
{
  if (advancedAt.isNone()) {
    return Duration::zero();
  }

  const std::chrono::nanoseconds elapsed =
    std::chrono::duration_cast<std::chrono::nanoseconds>(
        std::chrono::steady_clock::now() - advancedAt.get());

  return Nanoseconds(elapsed.count());
}


Option<double> ProgressReader::fraction() const
{
  return value;
}


uint64_t ProgressReader::dropped() const
{
  return droppedObservations;
}


bool ProgressReader::hasReport() const
{
  return value.isSome() || discontinuity.isSome();
}


Option<TaskStatus::Progress> ProgressReader::report() const
{
  if (!hasReport()) {
    return None();
  }

  TaskStatus::Progress progress;

  if (value.isSome()) {
    progress.set_fraction(value.get());
  }

  string text = message;
  if (discontinuity.isSome()) {
    text = text.empty()
      ? discontinuity.get()
      : discontinuity.get() + "; " + text;
  }

  if (!text.empty()) {
    progress.set_message(
        text.substr(
            0u,
            std::min(
                text.size(),
                common::validation::PROGRESS_MESSAGE_MAX_LENGTH)));
  }

  progress.set_sequence(sequence + 1u);

  if (observedAt.isSome()) {
    progress.set_observed_at(observedAt.get());
  }

  return progress;
}


void ProgressReader::reported()
{
  ++sequence;
  discontinuity = None();
}

} // namespace progress {
} // namespace internal {
} // namespace medios {
