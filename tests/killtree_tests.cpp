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

#include <errno.h>
#include <poll.h>
#include <signal.h>
#include <unistd.h>

#include <sys/types.h>
#include <sys/wait.h>

#include <functional>
#include <iostream>
#include <list>
#include <set>
#include <vector>

#include <gtest/gtest.h>

#include <stout/duration.hpp>
#include <stout/gtest.hpp>
#include <stout/os.hpp>
#include <stout/stopwatch.hpp>
#include <stout/try.hpp>

#include <stout/os/killtree.hpp>
#include <stout/os/process.hpp>

#ifdef __APPLE__
#include <sys/sysctl.h>

#include <stout/os/sysctl.hpp>
#else
#include <stout/proc.hpp>
#endif // __APPLE__

using std::list;
using std::set;
using std::vector;

using os::Process;
using os::ProcessTree;

namespace {

// How long a test waits for a forked process to report in, to take a
// signal, or to exit. A bound on a hung test, not a measurement: every
// wait it guards finishes in milliseconds on an idle host.
const Duration PROCESS_WAIT = Seconds(10);


// The write end the SIGUSR1 handler reports through, set in the child
// before the handler is installed.
int signalReport = -1;


void reportSignal(int)
{
  const char byte = 's';
  (void) ::write(signalReport, &byte, 1);
}


void writePid(int fd)
{
  const pid_t pid = ::getpid();
  (void) ::write(fd, &pid, sizeof(pid));
}


[[noreturn]] void pauseForever()
{
  while (true) {
    ::pause();
  }
}


// Reads `count` pids written by `writePid`, failing the test on a short
// read or a timeout.
vector<pid_t> readPids(int fd, size_t count)
{
  vector<pid_t> pids;

  while (pids.size() < count) {
    struct pollfd ready = {fd, POLLIN, 0};
    if (::poll(&ready, 1, PROCESS_WAIT.ms()) != 1) {
      ADD_FAILURE() << "Timed out waiting for " << count << " processes";
      return pids;
    }

    pid_t pid;
    if (::read(fd, &pid, sizeof(pid)) != sizeof(pid)) {
      ADD_FAILURE() << "Short read of a pid";
      return pids;
    }

    pids.push_back(pid);
  }

  return pids;
}


// Whether `pid` has exited: absent, or a zombie awaiting its parent.
bool gone(pid_t pid)
{
#ifdef __APPLE__
  // Read from the kernel directly, so that the check does not depend on
  // the `os::Process` conversion these tests exercise.
  const Try<vector<kinfo_proc>> process =
    os::sysctl(CTL_KERN, KERN_PROC, KERN_PROC_PID, pid).table(1);

  return process.isError() || process->size() != 1 ||
         process->front().kp_proc.p_stat == SZOMB;
#else
  const Result<os::Process> process = os::process(pid);
  return process.isNone() || (process.isSome() && process->zombie);
#endif // __APPLE__
}


// Whether `pid` is stopped by a signal.
bool isStopped(pid_t pid)
{
#ifdef __APPLE__
  const Try<vector<kinfo_proc>> process =
    os::sysctl(CTL_KERN, KERN_PROC, KERN_PROC_PID, pid).table(1);

  return process.isSome() && process->size() == 1 &&
         process->front().kp_proc.p_stat == SSTOP;
#else
  const Result<proc::ProcessStatus> status = proc::status(pid);
  return status.isSome() && status->state == 'T';
#endif // __APPLE__
}


// Whether `pid` is stopped within PROCESS_WAIT. On Linux kill(2) returns
// before the target stops, so a check right after a SIGSTOP polls.
bool waitStopped(pid_t pid)
{
  Stopwatch stopwatch;
  stopwatch.start();

  while (stopwatch.elapsed() < PROCESS_WAIT) {
    if (isStopped(pid)) {
      return true;
    }
    os::sleep(Milliseconds(1));
  }

  return false;
}


bool waitGone(const set<pid_t>& pids)
{
  Stopwatch stopwatch;
  stopwatch.start();

  while (stopwatch.elapsed() < PROCESS_WAIT) {
    bool all = true;
    foreach (pid_t pid, pids) {
      all = all && gone(pid);
    }

    if (all) {
      return true;
    }

    os::sleep(Milliseconds(5));
  }

  return false;
}


// A pipe whose ends are closed when the test returns, so that repeated
// runs do not exhaust the descriptor limit.
struct Pipe
{
  Pipe()
  {
    if (::pipe(fds) != 0) {
      fds[0] = fds[1] = -1;
    }
  }

  ~Pipe()
  {
    close(0);
    close(1);
  }

  void close(int end)
  {
    if (fds[end] != -1) {
      ::close(fds[end]);
      fds[end] = -1;
    }
  }

  int operator[](int end) const { return fds[end]; }

  int fds[2];
};


// Kills whatever a failed test left running, so a failure does not leak
// paused processes past the test.
void killSurvivors(const set<pid_t>& pids)
{
  foreach (pid_t pid, pids) {
    if (!gone(pid)) {
      ::kill(pid, SIGKILL);
    }
  }
}


// Kills and reaps a forked root left running by a test.
void reap(pid_t root)
{
  ::kill(root, SIGKILL);
  ::waitpid(root, nullptr, 0);
}



// Whether any of `trees` contains `pid`.
bool signalled(const list<ProcessTree>& trees, pid_t pid)
{
  foreach (const ProcessTree& tree, trees) {
    if (tree.contains(pid)) {
      return true;
    }
  }
  return false;
}


// `os::killtree` with its table reads counted.
Try<list<ProcessTree>> killtree(
    pid_t pid,
    int signal,
    size_t* reads,
    const std::function<void()>& beforeRead = []() {},
    const std::function<void()>& afterRead = []() {},
    size_t maxReads = os::internal::KILLTREE_MAX_TABLE_READS)
{
  return os::internal::killtree(
      pid,
      signal,
      true,
      true,
      [&]() {
        ++*reads;
        beforeRead();
        const Try<list<Process>> table = os::processTable();
        afterRead();
        return table;
      },
      maxReads);
}

} // namespace {


// The group is stopped before the process table is read and signalled
// after it: while the table is read, a member of the root's group is
// stopped and has not yet run its handler; once the call returns, it has.
TEST(KilltreeTest, StopsGroupBeforeReadingTable)
{
  Pipe ready;
  Pipe report;
  ASSERT_NE(-1, ready[0]);
  ASSERT_NE(-1, report[0]);

  const pid_t root = ::fork();
  ASSERT_NE(-1, root);

  if (root == 0) {
    ::setsid();

    // The root ignores the signal, so only the member reports it.
    ::signal(SIGUSR1, SIG_IGN);

    if (::fork() == 0) {
      signalReport = report[1];
      ::signal(SIGUSR1, reportSignal);
      writePid(ready[1]);
      pauseForever();
    }

    pauseForever();
  }

  const vector<pid_t> pids = readPids(ready[0], 1);
  ASSERT_EQ(1u, pids.size());

  const pid_t member = pids[0];
  ASSERT_EQ(root, ::getpgid(member));

  bool stoppedAtRead = false;
  bool signalledAtRead = true;
  size_t reads = 0;

  const Try<list<ProcessTree>> trees = killtree(root, SIGUSR1, &reads, [&]() {
    stoppedAtRead = waitStopped(member);

    struct pollfd delivered = {report[0], POLLIN, 0};
    signalledAtRead = ::poll(&delivered, 1, 0) == 1;
  });

  struct pollfd delivered = {report[0], POLLIN, 0};
  const bool signalledAfter = ::poll(&delivered, 1, PROCESS_WAIT.ms()) == 1;

  killSurvivors({member});
  reap(root);

  ASSERT_SOME(trees);
  EXPECT_TRUE(signalled(trees.get(), member));
  EXPECT_EQ(1u, reads);
  EXPECT_TRUE(stoppedAtRead);
  EXPECT_FALSE(signalledAtRead);
  EXPECT_TRUE(signalledAfter);
}


// A stopped process is not a zombie.
TEST(KilltreeTest, StoppedProcessIsNotZombie)
{
  const pid_t child = ::fork();
  ASSERT_NE(-1, child);

  if (child == 0) {
    pauseForever();
  }

  ASSERT_EQ(0, ::kill(child, SIGSTOP));

  int status = 0;
  ASSERT_EQ(child, ::waitpid(child, &status, WUNTRACED));
  ASSERT_TRUE(WIFSTOPPED(status));

  const Result<Process> process = os::process(child);

  bool inTable = false;
  bool zombieInTable = true;
  const Try<list<Process>> table = os::processTable();
  if (table.isSome()) {
    foreach (const Process& entry, table.get()) {
      if (entry.pid == child) {
        inTable = true;
        zombieInTable = entry.zombie;
      }
    }
  }

  reap(child);

  ASSERT_SOME(process);
  EXPECT_FALSE(process->zombie);
  ASSERT_SOME(table);
  EXPECT_TRUE(inTable);
  EXPECT_FALSE(zombieInTable);
}


// A descendant that called setsid(2) is outside the group and the session
// the root leads, and is found through its parent and signalled on its
// own.
TEST(KilltreeTest, SignalsDescendantThatCalledSetsid)
{
  Pipe ready;
  ASSERT_NE(-1, ready[0]);

  const pid_t root = ::fork();
  ASSERT_NE(-1, root);

  if (root == 0) {
    ::setsid();

    // The root outlives the SIGTERM, so the descendant keeps its parent
    // link while the table is read.
    ::signal(SIGTERM, SIG_IGN);

    if (::fork() == 0) {
      ::signal(SIGTERM, SIG_DFL);
      ::setsid();
      writePid(ready[1]);
      pauseForever();
    }

    writePid(ready[1]);
    pauseForever();
  }

  const vector<pid_t> pids = readPids(ready[0], 2);
  ASSERT_EQ(2u, pids.size());

  const pid_t descendant = pids[0] == root ? pids[1] : pids[0];
  ASSERT_EQ(descendant, ::getsid(descendant));

  size_t reads = 0;
  const Try<list<ProcessTree>> trees = killtree(root, SIGTERM, &reads);

  EXPECT_TRUE(waitGone({descendant}));
  EXPECT_FALSE(gone(root));

  killSurvivors({descendant});
  reap(root);

  ASSERT_SOME(trees);
  EXPECT_TRUE(signalled(trees.get(), descendant));

  // The second read finds the descendant, stopped after the first, with
  // no child the first read missed.
  EXPECT_EQ(2u, reads);
}


// A child forked by a process outside the stopped group while the table
// is read is found by the next read, and that process, stopped after the
// first read, forks nothing during the second.
TEST(KilltreeTest, FindsChildForkedDuringRead)
{
  Pipe ready;
  Pipe forkNow;
  ASSERT_NE(-1, ready[0]);
  ASSERT_NE(-1, forkNow[0]);

  const pid_t root = ::fork();
  ASSERT_NE(-1, root);

  if (root == 0) {
    ::setsid();

    if (::fork() == 0) {
      // Outside the root's group, and forks once when told to.
      ::setpgid(0, 0);
      writePid(ready[1]);

      char byte;
      while (::read(forkNow[0], &byte, 1) == 1) {
        if (::fork() == 0) {
          writePid(ready[1]);
          pauseForever();
        }
      }
      pauseForever();
    }

    writePid(ready[1]);
    pauseForever();
  }

  const vector<pid_t> pids = readPids(ready[0], 2);
  ASSERT_EQ(2u, pids.size());

  const pid_t outside = pids[0] == root ? pids[1] : pids[0];
  ASSERT_EQ(outside, ::getpgid(outside));

  // How long the second read waits for a child that a stopped process
  // cannot fork. A bound on the wait, not a measurement.
  const Duration LATE_FORK_WAIT = Milliseconds(200);

  // The table is read before the outside process is told to fork, so a
  // read does not list the child forked during it.
  pid_t forked = -1;
  pid_t forkedLate = -1;
  size_t reads = 0;
  const Try<list<ProcessTree>> trees = os::internal::killtree(
      root, SIGKILL, true, true, [&]() -> Try<list<Process>> {
        ++reads;
        const Try<list<Process>> table = os::processTable();
        const char byte = 'f';
        if (reads == 1) {
          (void) ::write(forkNow[1], &byte, 1);
          const vector<pid_t> child = readPids(ready[0], 1);
          if (child.size() == 1) {
            forked = child[0];
          }
        } else if (reads == 2) {
          (void) ::write(forkNow[1], &byte, 1);
          struct pollfd reported = {ready[0], POLLIN, 0};
          if (::poll(&reported, 1, LATE_FORK_WAIT.ms()) == 1) {
            (void) ::read(ready[0], &forkedLate, sizeof(forkedLate));
          }
        }
        return table;
      });

  ASSERT_NE(-1, forked);
  EXPECT_EQ(-1, forkedLate);

  EXPECT_TRUE(waitGone({root, outside, forked}));

  killSurvivors({outside, forked});
  if (forkedLate != -1) {
    killSurvivors({forkedLate});
  }
  ::waitpid(root, nullptr, 0);

  ASSERT_SOME(trees);
  EXPECT_TRUE(signalled(trees.get(), forked));
  EXPECT_EQ(3u, reads);
}


// A grandchild that called setsid(2) is reachable only through its
// parent, and that parent exits on SIGTERM. The group is stopped while the
// table is read, so the parent is still there to lead to the grandchild,
// however long the read takes.
TEST(KilltreeTest, SignalsSetsidGrandchildWhoseParentExits)
{
  // A table read slower than the parent's exit on SIGTERM, so that a
  // parent left running would be gone by the time the table is read.
  const Duration SLOW_READ = Milliseconds(200);

  Pipe ready;
  ASSERT_NE(-1, ready[0]);

  const pid_t root = ::fork();
  ASSERT_NE(-1, root);

  if (root == 0) {
    ::setsid();

    if (::fork() == 0) {
      if (::fork() == 0) {
        ::setsid();
        writePid(ready[1]);
        pauseForever();
      }

      writePid(ready[1]);
      pauseForever();
    }

    writePid(ready[1]);
    pauseForever();
  }

  const vector<pid_t> pids = readPids(ready[0], 3);
  ASSERT_EQ(3u, pids.size());

  pid_t parent = -1;
  pid_t grandchild = -1;
  foreach (pid_t pid, pids) {
    if (pid == root) {
      continue;
    } else if (::getsid(pid) == pid) {
      grandchild = pid;
    } else {
      parent = pid;
    }
  }

  ASSERT_NE(-1, parent);
  ASSERT_NE(-1, grandchild);
  ASSERT_EQ(root, ::getpgid(parent));

  size_t reads = 0;
  const Try<list<ProcessTree>> trees =
    killtree(root, SIGTERM, &reads, [&]() { os::sleep(SLOW_READ); });

  EXPECT_TRUE(waitGone({root, parent, grandchild}));

  killSurvivors({parent, grandchild});
  reap(root);

  ASSERT_SOME(trees);
  EXPECT_TRUE(signalled(trees.get(), grandchild));
}


// A group of 30 processes is signalled with one read of the table.
TEST(KilltreeTest, SignalsThirtyProcessGroupWithOneTableRead)
{
  const size_t CHILDREN = 29;

  Pipe ready;
  ASSERT_NE(-1, ready[0]);

  const pid_t root = ::fork();
  ASSERT_NE(-1, root);

  if (root == 0) {
    ::setsid();

    for (size_t i = 0; i < CHILDREN; ++i) {
      if (::fork() == 0) {
        writePid(ready[1]);
        pauseForever();
      }
    }

    writePid(ready[1]);
    pauseForever();
  }

  const vector<pid_t> pids = readPids(ready[0], CHILDREN + 1);
  ASSERT_EQ(CHILDREN + 1, pids.size());

  size_t reads = 0;

  Stopwatch stopwatch;
  stopwatch.start();

  const Try<list<ProcessTree>> trees = killtree(root, SIGKILL, &reads);

  const Duration elapsed = stopwatch.elapsed();

  EXPECT_TRUE(waitGone(set<pid_t>(pids.begin(), pids.end())));

  killSurvivors(set<pid_t>(pids.begin(), pids.end()));
  ::waitpid(root, nullptr, 0);

  ASSERT_SOME(trees);
  foreach (pid_t pid, pids) {
    EXPECT_TRUE(signalled(trees.get(), pid)) << pid;
  }
  EXPECT_EQ(1u, reads);

  // An observation for the log, not an assertion.
  RecordProperty("killtree_ms", stringify(elapsed.ms()));
  std::cout << "Signalled " << pids.size() << " processes in " << elapsed
            << std::endl;
}


// When a third of a 30-process tree has left the group, the table is read
// a second time, after those processes are stopped.
TEST(KilltreeTest, SignalsThirtyProcessTreeWithTwoTableReads)
{
  // The tree's size and how many of its children leave the group.
  const size_t CHILDREN = 29;
  const size_t LEAVE_GROUP_EVERY = 3;

  Pipe ready;
  ASSERT_NE(-1, ready[0]);

  const pid_t root = ::fork();
  ASSERT_NE(-1, root);

  if (root == 0) {
    ::setsid();

    for (size_t i = 0; i < CHILDREN; ++i) {
      if (::fork() == 0) {
        if (i % LEAVE_GROUP_EVERY == 0) {
          ::setpgid(0, 0);
        }
        writePid(ready[1]);
        pauseForever();
      }
    }

    writePid(ready[1]);
    pauseForever();
  }

  const vector<pid_t> pids = readPids(ready[0], CHILDREN + 1);
  ASSERT_EQ(CHILDREN + 1, pids.size());

  size_t reads = 0;
  const Try<list<ProcessTree>> trees = killtree(root, SIGKILL, &reads);

  EXPECT_TRUE(waitGone(set<pid_t>(pids.begin(), pids.end())));

  killSurvivors(set<pid_t>(pids.begin(), pids.end()));
  ::waitpid(root, nullptr, 0);

  ASSERT_SOME(trees);
  foreach (pid_t pid, pids) {
    EXPECT_TRUE(signalled(trees.get(), pid)) << pid;
  }
  EXPECT_EQ(2u, reads);
}


// A root that has exited and been reaped leaves its group behind, and the
// group is signalled.
TEST(KilltreeTest, SignalsGroupOfExitedRoot)
{
  Pipe ready;
  ASSERT_NE(-1, ready[0]);

  const pid_t root = ::fork();
  ASSERT_NE(-1, root);

  if (root == 0) {
    ::setsid();

    if (::fork() == 0) {
      writePid(ready[1]);
      pauseForever();
    }

    // Exits, leaving the child in the group.
    ::_exit(0);
  }

  const vector<pid_t> pids = readPids(ready[0], 1);
  ASSERT_EQ(1u, pids.size());

  const pid_t child = pids[0];
  ASSERT_EQ(root, ::waitpid(root, nullptr, 0));
  ASSERT_EQ(root, ::getpgid(child));

  size_t reads = 0;
  const Try<list<ProcessTree>> trees = killtree(root, SIGKILL, &reads);

  EXPECT_TRUE(waitGone({child}));

  killSurvivors({child});

  ASSERT_SOME(trees);
  EXPECT_TRUE(signalled(trees.get(), child));
  EXPECT_EQ(1u, reads);
}


// A signal that neither terminates nor continues a process leaves no
// process of the tree stopped, inside the group or outside it.
TEST(KilltreeTest, ContinuesEveryStoppedProcess)
{
  Pipe ready;
  ASSERT_NE(-1, ready[0]);

  const pid_t root = ::fork();
  ASSERT_NE(-1, root);

  if (root == 0) {
    ::setsid();
    ::signal(SIGUSR1, SIG_IGN);

    if (::fork() == 0) {
      ::setsid();
      writePid(ready[1]);
      pauseForever();
    }

    writePid(ready[1]);
    pauseForever();
  }

  const vector<pid_t> pids = readPids(ready[0], 2);
  ASSERT_EQ(2u, pids.size());

  const pid_t outside = pids[0] == root ? pids[1] : pids[0];

  const Try<list<ProcessTree>> trees = os::killtree(root, SIGUSR1, true, true);

  const bool rootStopped = isStopped(root);
  const bool outsideStopped = isStopped(outside);

  killSurvivors({outside});
  reap(root);

  ASSERT_SOME(trees);
  EXPECT_TRUE(signalled(trees.get(), outside));
  EXPECT_FALSE(rootStopped);
  EXPECT_FALSE(outsideStopped);
}


// Without groups, the root alone is stopped before the table is read, so
// a root with no children takes one read.
TEST(KilltreeTest, StopsRootBeforeReadingTableWithoutGroups)
{
  Pipe ready;
  ASSERT_NE(-1, ready[0]);

  const pid_t root = ::fork();
  ASSERT_NE(-1, root);

  if (root == 0) {
    writePid(ready[1]);
    pauseForever();
  }

  ASSERT_EQ(vector<pid_t>{root}, readPids(ready[0], 1));

  bool stoppedAtRead = false;
  size_t reads = 0;
  const Try<list<ProcessTree>> trees = os::internal::killtree(
      root, SIGKILL, false, false, [&]() {
        ++reads;
        stoppedAtRead = waitStopped(root);
        return os::processTable();
      });

  reap(root);

  ASSERT_SOME(trees);
  EXPECT_TRUE(signalled(trees.get(), root));
  EXPECT_TRUE(stoppedAtRead);
  EXPECT_EQ(1u, reads);
}


// A chain that forks a new process outside the stopped group after every
// read is read `maxReads` times. The processes the last read found are
// signalled, and the child forked after it is not.
TEST(KilltreeTest, StopsReadingTableAtMaxReads)
{
  // Small, so that the chain is short; the cap's behavior does not
  // depend on its value.
  const size_t maxReads = 3;

  Pipe ready;
  Pipe forkNow;
  ASSERT_NE(-1, ready[0]);
  ASSERT_NE(-1, forkNow[0]);

  const pid_t root = ::fork();
  ASSERT_NE(-1, root);

  if (root == 0) {
    ::setsid();
    forkNow.close(1);

    // Each link is in its own group, reports its pid, and forks the next
    // link for each byte it reads. It exits when the test closes the pipe.
    while (::fork() == 0) {
      ::setpgid(0, 0);
      writePid(ready[1]);

      char byte;
      if (::read(forkNow[0], &byte, 1) != 1) {
        ::_exit(0);
      }
    }

    pauseForever();
  }

  const vector<pid_t> first = readPids(ready[0], 1);
  ASSERT_EQ(1u, first.size());

  vector<pid_t> links = first;
  size_t reads = 0;

  // After each read the newest link, found running by that read, forks
  // the next one before it is stopped.
  const Try<list<ProcessTree>> trees = killtree(
      root,
      SIGKILL,
      &reads,
      []() {},
      [&]() {
        const char byte = 'f';
        if (::write(forkNow[1], &byte, 1) == 1) {
          const vector<pid_t> next = readPids(ready[0], 1);
          links.insert(links.end(), next.begin(), next.end());
        }
      },
      maxReads);

  const set<pid_t> found(links.begin(), links.end() - 1);
  const pid_t missed = links.back();

  EXPECT_TRUE(waitGone(found));
  const bool missedRunning = !gone(missed);

  forkNow.close(1);

  killSurvivors(set<pid_t>(links.begin(), links.end()));
  reap(root);

  ASSERT_SOME(trees);
  EXPECT_EQ(maxReads, reads);
  ASSERT_EQ(maxReads + 1, links.size());
  foreach (pid_t pid, found) {
    EXPECT_TRUE(signalled(trees.get(), pid)) << pid;
  }
  EXPECT_FALSE(signalled(trees.get(), missed));
  EXPECT_TRUE(missedRunning);
}


// When the root leaves the table between reads and nothing is left in its
// group or session, the processes stopped by the earlier read are
// continued before killtree returns.
TEST(KilltreeTest, ContinuesStoppedProcessesWhenRootVanishes)
{
  Pipe ready;
  ASSERT_NE(-1, ready[0]);

  const pid_t root = ::fork();
  ASSERT_NE(-1, root);

  if (root == 0) {
    ::setsid();

    if (::fork() == 0) {
      // Outside the root's group and session, so the root's exit leaves
      // nothing to find through either.
      ::setsid();
      writePid(ready[1]);
      pauseForever();
    }

    pauseForever();
  }

  const vector<pid_t> pids = readPids(ready[0], 1);
  ASSERT_EQ(1u, pids.size());

  const pid_t child = pids[0];

  bool stoppedAtSecondRead = false;
  size_t reads = 0;

  const Try<list<ProcessTree>> trees = killtree(root, SIGKILL, &reads, [&]() {
    if (reads == 2) {
      stoppedAtSecondRead = waitStopped(child);
      reap(root);
    }
  });

  const bool stoppedAfter = isStopped(child);

  killSurvivors({child});

  ASSERT_SOME(trees);
  EXPECT_TRUE(trees->empty());
  EXPECT_EQ(2u, reads);
  EXPECT_TRUE(stoppedAtSecondRead);
  EXPECT_FALSE(stoppedAfter);
}


// A member of the root's group that the table does not list is stopped
// with the group and continued with it, though it is never visited.
TEST(KilltreeTest, ContinuesGroupMemberMissingFromTable)
{
  Pipe ready;
  ASSERT_NE(-1, ready[0]);

  const pid_t root = ::fork();
  ASSERT_NE(-1, root);

  if (root == 0) {
    ::setsid();
    ::signal(SIGUSR1, SIG_IGN);

    if (::fork() == 0) {
      writePid(ready[1]);
      pauseForever();
    }

    pauseForever();
  }

  const vector<pid_t> pids = readPids(ready[0], 1);
  ASSERT_EQ(1u, pids.size());

  const pid_t member = pids[0];
  ASSERT_EQ(root, ::getpgid(member));

  bool stoppedAtRead = false;

  const Try<list<ProcessTree>> trees = os::internal::killtree(
      root, SIGUSR1, true, true, [&]() -> Try<list<Process>> {
        stoppedAtRead = waitStopped(member);

        Try<list<Process>> table = os::processTable();
        if (table.isSome()) {
          table->remove_if(
              [&](const Process& process) { return process.pid == member; });
        }
        return table;
      });

  const bool stoppedAfter = isStopped(member);

  killSurvivors({member});
  reap(root);

  ASSERT_SOME(trees);
  EXPECT_FALSE(signalled(trees.get(), member));
  EXPECT_TRUE(stoppedAtRead);
  EXPECT_FALSE(stoppedAfter);
}


TEST(KilltreeTest, RefusesNonPositiveRoot)
{
  EXPECT_ERROR(os::killtree(0, SIGTERM, true, true));
  EXPECT_ERROR(os::killtree(-1, SIGTERM, true, true));
}
