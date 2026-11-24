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

#include "launcher/observer.hpp"

#include <errno.h>
#include <string.h>
#include <signal.h>
#include <sys/wait.h>
#include <unistd.h>

#include <algorithm>
#include <chrono>
#include <string>
#include <utility>
#include <vector>

#include <stout/os.hpp>
#include <stout/path.hpp>
#include <stout/stringify.hpp>

#include "common/validation.hpp"

using process::Owned;
using process::Subprocess;

using std::string;
using std::vector;

namespace medios {
namespace internal {
namespace observer {

namespace {

// The name of the sandbox directory holding the observers' stderr files.
constexpr char DIRECTORY[] = "observers";


// Wall clock, seconds since the epoch, as `TaskStatus.Progress.observed_at`
// carries it.
double now()
{
  return std::chrono::duration_cast<std::chrono::duration<double>>(
      std::chrono::system_clock::now().time_since_epoch()).count();
}


// Drops a character the byte bound cut in half. A progress message is a
// protobuf string and is rendered into JSON, and half a character is
// neither.
void trimPartialCharacter(string* text)
{
  size_t index = text->size();

  // Walk back over the continuation bytes of the last character to the
  // byte that starts it. A character is at most four bytes, so a longer
  // run of continuation bytes is not one.
  size_t continuations = 0u;
  while (index > 0u && continuations <= 3u) {
    const unsigned char byte = static_cast<unsigned char>((*text)[index - 1u]);

    --index;

    if ((byte & 0xc0) != 0x80) {
      break;
    }

    ++continuations;
  }

  if (index >= text->size()) {
    return;
  }

  const unsigned char lead = static_cast<unsigned char>((*text)[index]);

  if ((lead & 0x80) == 0x00) {
    return;
  }

  size_t length = 0u;
  if ((lead & 0xe0) == 0xc0) {
    length = 2u;
  } else if ((lead & 0xf0) == 0xe0) {
    length = 3u;
  } else if ((lead & 0xf8) == 0xf0) {
    length = 4u;
  }

  // A run with no byte that starts a character, or a character the bound
  // cut short.
  if (length == 0u || text->size() - index < length) {
    text->resize(index);
  }
}


string describe(const process::Future<Option<int>>& status)
{
  if (!status.isReady()) {
    return "exited with an unknown status: " +
      (status.isFailed() ? status.failure() : string("future discarded"));
  }

  if (status->isNone()) {
    return "exited with an unknown status";
  }

  const int value = status->get();

  if (WIFEXITED(value)) {
    return "exited " + stringify(WEXITSTATUS(value));
  }

  if (WIFSIGNALED(value)) {
    return "killed by signal " + stringify(WTERMSIG(value));
  }

  return "exited with wait status " + stringify(value);
}

} // namespace {


void stampReport(
    TaskStatus::Progress* progress,
    uint64_t sequence,
    const Option<double>& observedAt)
{
  progress->set_sequence(sequence);

  if (!progress->has_observed_at() && observedAt.isSome()) {
    progress->set_observed_at(observedAt.get());
  }
}


Owned<ObserverSet> ObserverSet::create(
    const google::protobuf::RepeatedPtrField<TaskObserver>& observers,
    const string& sandboxDirectory)
{
  const string directory = path::join(sandboxDirectory, DIRECTORY);

  Option<string> directoryError = None();

  if (sandboxDirectory.empty()) {
    // An observer runs in the task's sandbox. Without one there is nothing
    // to run it in, which is every observer failing to launch rather than
    // a reason to refuse the task.
    directoryError = "no sandbox directory";
  } else {
    Try<Nothing> mkdir = os::mkdir(directory);
    if (mkdir.isError()) {
      directoryError =
        "its stderr file cannot be created under '" + directory + "': " +
        mkdir.error();
    }
  }

  vector<Observer> state;
  foreach (const TaskObserver& observer, observers) {
    Observer one;
    one.declaration = observer;
    state.push_back(std::move(one));
  }

  return Owned<ObserverSet>(
      new ObserverSet(std::move(state), directory, directoryError));
}


ObserverSet::ObserverSet(
    vector<Observer>&& _observers,
    const string& _directory,
    const Option<string>& _directoryError)
  : observers(std::move(_observers)),
    directory(_directory),
    directoryError(_directoryError) {}


ObserverSet::~ObserverSet()
{
  // An observer cannot outlive the sandbox it runs in, so it cannot
  // outlive this.
  signalKill();
}


void ObserverSet::launch(const Fork& fork)
{
  foreach (Observer& observer, observers) {
    const string& name = observer.declaration.name();

    if (directoryError.isSome()) {
      accept(name, "did not start: " + directoryError.get());
      continue;
    }

    Try<Subprocess> process =
      fork(observer.declaration, path::join(directory, name + ".stderr"));

    if (process.isError()) {
      accept(name, "did not start: " + process.error());
      continue;
    }

    // A fork that succeeded leaves a live process whatever happens next,
    // so it is retained and killed here rather than abandoned: dropping the
    // object neither closes the pipe, which libprocess's reaper holds until
    // the process exits, nor stops the observer. Its exit is this
    // executor's doing, so it is not recorded as an observation.
    Option<string> setupError = None();

    if (process->out().isNone()) {
      setupError = "its stdout is not a pipe";
    } else {
      // Read without blocking the executor: an observer that writes nothing
      // must not stop the executor from reading the next one, or from
      // reaching its own event queue.
      Try<Nothing> nonblock = os::nonblock(process->out().get());
      if (nonblock.isError()) {
        setupError =
          "failed to set its stdout non-blocking: " + nonblock.error();
      }
    }

    observer.process = process.get();

    if (setupError.isSome()) {
      observer.exitRecorded = true;
      os::killtree(process->pid(), SIGKILL, true, true);
      accept(name, "did not start: " + setupError.get());
      continue;
    }

    observer.reading = true;
  }
}


void ObserverSet::drain()
{
  foreach (Observer& observer, observers) {
    if (observer.process.isNone()) {
      continue;
    }

    if (observer.reading) {
      read(&observer);
    }

    // The exit is recorded after the pipe is drained and after the last
    // thing the observer said has reached a status update, so an
    // observer's last words are not overtaken by the note that it stopped
    // speaking.
    if (!observer.reading &&
        !observer.exitRecorded &&
        !hasUnreported() &&
        observer.process->status().isReady()) {
      observer.exitRecorded = true;

      // An observer this executor signalled did not observe anything by
      // dying, so its exit is not an observation.
      if (!terminating) {
        accept(
            observer.declaration.name(),
            describe(observer.process->status()));
      }
    }
  }
}


void ObserverSet::read(Observer* observer)
{
  char buffer[4096];
  size_t total = 0u;

  while (total < MAX_DRAIN_BYTES) {
    const ssize_t length = ::read(
        observer->process->out().get(),
        buffer,
        std::min(sizeof(buffer), MAX_DRAIN_BYTES - total));

    if (length < 0) {
      if (errno == EINTR) {
        continue;
      }

      if (errno == EAGAIN || errno == EWOULDBLOCK) {
        return;
      }

      // The pipe is unreadable, so this observer has nothing more to say.
      observer->reading = false;
      return;
    }

    if (length == 0) {
      // End of the pipe: the observer closed its stdout or exited. A line
      // it did not finish is all it managed to say, so it is taken whole.
      observer->reading = false;

      if (!observer->partial.empty()) {
        const string last = observer->partial;
        observer->partial.clear();
        observer->skipping = false;
        accept(observer->declaration.name(), last);
      }

      return;
    }

    total += static_cast<size_t>(length);

    size_t position = 0u;
    const size_t available = static_cast<size_t>(length);

    while (position < available) {
      const void* found =
        ::memchr(buffer + position, '\n', available - position);

      const size_t segment = found == nullptr
        ? available
        : static_cast<size_t>(static_cast<const char*>(found) - buffer);

      if (!observer->skipping) {
        const size_t room = MAX_LINE_BYTES - observer->partial.size();

        if (segment - position > room) {
          observer->partial.append(buffer + position, room);
          observer->skipping = true;
        } else {
          observer->partial.append(buffer + position, segment - position);
        }
      }

      if (found == nullptr) {
        break;
      }

      string line = observer->partial;
      observer->partial.clear();
      observer->skipping = false;
      position = segment + 1u;

      if (!line.empty() && line.back() == '\r') {
        line.pop_back();
      }

      if (!line.empty()) {
        accept(observer->declaration.name(), line);
      }
    }
  }
}


void ObserverSet::accept(const string& name, const string& text)
{
  // The name prefixes the line so several observers on one task are told
  // apart, and the whole message is bounded: the node drops a progress
  // report whose message is over the bound, which would lose the
  // observation rather than shorten it.
  string message = name + ": " + text;

  if (message.size() > common::validation::PROGRESS_MESSAGE_MAX_LENGTH) {
    message.resize(common::validation::PROGRESS_MESSAGE_MAX_LENGTH);
    trimPartialCharacter(&message);
  }

  newest = message;
  newestAt = now();
  ++lines;
}


Option<string> ObserverSet::line() const
{
  return newest;
}


bool ObserverSet::hasUnreported() const
{
  return lines > reportedLines;
}


Option<string> ObserverSet::fold(const Option<string>& readerMessage)
{
  if (readerMessage != readerSeen) {
    readerSeen = readerMessage;
    observerNewer = false;
  }

  if (hasUnreported()) {
    observerNewer = true;
    reported();
  }

  if (observerNewer) {
    return line();
  }

  return None();
}


void ObserverSet::reported()
{
  reportedLines = lines;
}


Option<double> ObserverSet::observedAt() const
{
  return newestAt;
}


uint64_t ObserverSet::generation() const
{
  return lines;
}


void ObserverSet::signal(int signal)
{
  terminating = true;

  foreach (Observer& observer, observers) {
    if (observer.process.isNone()) {
      continue;
    }

    // Signalled whether or not the observer itself has been reaped. An
    // observer that backgrounded a child and exited leaves that child in
    // its group and session, and `os::killtree` finds a departed leader's
    // group and session for exactly this case; skipping the call on a
    // ready status would leave the child running past the sandbox.
    os::killtree(observer.process->pid(), signal, true, true);
  }
}


void ObserverSet::signalTerm()
{
  signal(SIGTERM);
}


void ObserverSet::signalKill()
{
  signal(SIGKILL);
}


vector<pid_t> ObserverSet::running() const
{
  vector<pid_t> pids;

  foreach (const Observer& observer, observers) {
    if (observer.process.isSome() &&
        !observer.process->status().isReady()) {
      pids.push_back(observer.process->pid());
    }
  }

  return pids;
}


bool ObserverSet::launched() const
{
  foreach (const Observer& observer, observers) {
    if (observer.process.isSome()) {
      return true;
    }
  }

  return false;
}

} // namespace observer {
} // namespace internal {
} // namespace medios {
