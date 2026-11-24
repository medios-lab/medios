// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#ifndef __STOUT_OS_POSIX_KILLTREE_HPP__
#define __STOUT_OS_POSIX_KILLTREE_HPP__

#include <stdlib.h>
#include <unistd.h>

#include <functional>
#include <list>
#include <queue>
#include <set>

#include <stout/check.hpp>
#include <stout/os.hpp>
#include <stout/stringify.hpp>

#include <stout/os/pstree.hpp>


namespace os {

// Forward declarations from os.hpp.
inline std::set<pid_t> children(pid_t, const std::list<Process>&, bool);
inline Option<Process> process(pid_t, const std::list<Process>&);
inline Try<std::list<Process>> processTable();
inline Try<std::list<ProcessTree>> pstrees(
    const std::set<pid_t>&,
    const std::list<Process>&);


namespace internal {

// Heuristic: the most process table reads one `os::killtree` makes. A
// tree whose processes are all in the root's group takes 1 read, a tree
// with a process outside it takes 2, and each process that forks between
// a read and its own stop adds 1 (3 in the tests). The cap only bounds a
// tree that keeps forking outside the stopped group faster than it is
// stopped; at the cap the tree from the last read is signalled.
constexpr size_t KILLTREE_MAX_TABLE_READS = 8;


// `os::killtree`, reading the process table through `table` at most
// `maxReads` times.
inline Try<std::list<ProcessTree>> killtree(
    pid_t pid,
    int signal,
    bool groups,
    bool sessions,
    const std::function<Try<std::list<Process>>()>& table,
    size_t maxReads = KILLTREE_MAX_TABLE_READS)
{
  // kill(2) and killpg(2) read 0 and negative pids as the caller's group
  // or as every process the caller may signal.
  if (pid <= 0) {
    return Error("Invalid pid " + stringify(pid));
  }

  // Each process is stopped before the table read that lists its
  // children, so that none can fork a child the read misses or exit and
  // leave a child re-parented by init.
  std::set<pid_t> stopped;

  if (kill(pid, SIGSTOP) == 0) {
    stopped.insert(pid);
  }

  // The group the root leads, or led before it exited, is stopped with
  // one killpg(2). When the root leads no group there is no group `pid`
  // and killpg(2) fails with ESRCH.
  const bool groupStopped =
    groups && pid != getpgrp() && killpg(pid, SIGSTOP) == 0;

  // Continues every process stopped here, and the group.
  auto resume = [&]() {
    foreach (pid_t pid_, stopped) {
      kill(pid_, SIGCONT);
    }
    if (groupStopped) {
      killpg(pid, SIGCONT);
    }
  };

  struct {
    std::set<pid_t> pids;
    std::set<pid_t> groups;
    std::set<pid_t> sessions;
    std::list<Process> processes;
  } visited;

  // The table is read until a read finds no process that was running
  // while it was read, or `maxReads` times. With the group stopped that
  // is usually one read when every process is in the group, and two
  // otherwise.
  for (size_t reads = 1; ; ++reads) {
    const Try<std::list<Process>> processes = table();

    if (processes.isError()) {
      resume();
      return Error(processes.error());
    }

    visited.pids.clear();
    visited.groups.clear();
    visited.sessions.clear();
    visited.processes.clear();

    Option<Process> process = os::process(pid, processes.get());

    std::queue<pid_t> queue;

    // If the root process has already terminated we'll add in any pids
    // that are in the process group originally led by pid or in the
    // session originally led by pid, if instructed.
    if (process.isNone()) {
      foreach (const Process& _process, processes.get()) {
        if (groups && _process.group == pid) {
          queue.push(_process.pid);
        } else if (sessions &&
                   _process.session.isSome() &&
                   _process.session.get() == pid) {
          queue.push(_process.pid);
        }
      }

      // Root process is not running and no processes found in the
      // process group or session so nothing we can do.
      if (queue.empty()) {
        resume();
        return std::list<ProcessTree>();
      }
    } else {
      // Start the traversal from pid as the root.
      queue.push(pid);
    }

    // If we are following groups and/or sessions then we try and make
    // the group and session of the parent process "already visited" so
    // that we don't kill "up the tree". This can only be done if the
    // process is present.
    if (process.isSome() && (groups || sessions)) {
      Option<Process> parent = os::process(process->parent, processes.get());

      if (parent.isSome()) {
        if (groups) {
          visited.groups.insert(parent->group);
        }
        if (sessions && parent->session.isSome()) {
          visited.sessions.insert(parent->session.get());
        }
      }
    }

    // Visited processes that were running while the table was read.
    std::set<pid_t> running;

    while (!queue.empty()) {
      pid_t pid_ = queue.front();
      queue.pop();

      if (visited.pids.count(pid_) != 0) {
        continue;
      }

      process = os::process(pid_, processes.get());

      if (process.isNone()) {
        continue;
      }

      visited.pids.insert(pid_);
      visited.processes.push_back(process.get());

      if (stopped.count(pid_) == 0 &&
          !(groupStopped && process->group == pid)) {
        running.insert(pid_);
      }

      // Enqueue the children for visiting.
      foreach (pid_t child, os::children(pid_, processes.get(), false)) {
        queue.push(child);
      }

      // Now "visit" the group and/or session of the current process.
      if (groups) {
        pid_t group = process->group;
        if (visited.groups.count(group) == 0) {
          foreach (const Process& process, processes.get()) {
            if (process.group == group) {
              queue.push(process.pid);
            }
          }
          visited.groups.insert(group);
        }
      }

      // If we do not have a session for the process, it's likely
      // because the process is a zombie on OS X. This implies it has
      // not been reaped and thus is located somewhere in the tree we
      // are trying to kill. Therefore, we should discover it from our
      // tree traversal, or through its group (which is always present).
      if (sessions && process->session.isSome()) {
        pid_t session = process->session.get();
        if (visited.sessions.count(session) == 0) {
          foreach (const Process& process, processes.get()) {
            if (process.session.isSome() &&
                process.session.get() == session) {
              queue.push(process.pid);
            }
          }
          visited.sessions.insert(session);
        }
      }
    }

    // Stop them to keep them from forking while we are killing them,
    // then read the table again for any child forked before the stop.
    // At `maxReads` the visited processes are signalled as they are,
    // and a child forked since the last read is not.
    foreach (pid_t pid_, running) {
      kill(pid_, SIGSTOP);
      stopped.insert(pid_);
    }

    if (running.empty() || reads >= maxReads) {
      break;
    }
  }

  // Now that all processes are stopped, we send the signal.
  foreach (pid_t pid_, visited.pids) {
    kill(pid_, signal);
  }

  // There is a concern that even though some process is stopped,
  // sending a signal to any of its children may cause a SIGCLD to
  // be delivered to it which wakes it up (or any other signal maybe
  // delivered). However, from the Open Group standards on "Signal
  // Concepts":
  //
  //   "While a process is stopped, any additional signals that are
  //    sent to the process shall not be delivered until the process
  //    is continued, except SIGKILL which always terminates the
  //    receiving process."
  //
  // In practice, this is not what has been witnessed. Rather, a
  // process that has been stopped will respond to SIGTERM, SIGINT,
  // etc. That being said, we still continue the process below in the
  // event that it doesn't terminate from the sending signal but it
  // also doesn't get continued (as per the specifications above).

  // Try and continue the processes in case the signal is
  // non-terminating but doesn't continue the process.
  foreach (pid_t pid_, visited.pids) {
    kill(pid_, SIGCONT);
  }
  resume();

  // Return the process trees representing the visited pids.
  return pstrees(visited.pids, visited.processes);
}

} // namespace internal {


// Sends a signal to a process tree rooted at the specified pid.
// If groups is true, this also sends the signal to all encountered
// process groups.
// If sessions is true, this also sends the signal to all encountered
// process sessions.
// Note that processes of the group and session of the parent of the
// root process is not included unless they are part of the root
// process tree.
// Note that if the process 'pid' has exited we'll signal the process
// tree(s) rooted at pids in the group or session led by the process
// if groups = true or sessions = true, respectively.
// Every process is stopped before it is signalled. If groups is true,
// the group the root leads is stopped with one killpg(2) before the
// process table is first read; any other process is stopped when a
// read finds it, and the table is read again until a read finds only
// processes that were already stopped, at most
// `internal::KILLTREE_MAX_TABLE_READS` times.
// The table is `os::processTable()`: the returned processes carry the
// executable's name (OS X `p_comm`, Linux `comm`) as their command and
// no rss or CPU times.
// Returns the process trees that were successfully or unsuccessfully
// signaled. Note that the process trees can be stringified.
// TODO(benh): Allow excluding the root pid from stopping, killing,
// and continuing so as to provide a means for expressing "kill all of
// my children". This is non-trivial because of the current
// implementation.
inline Try<std::list<ProcessTree>> killtree(
    pid_t pid,
    int signal,
    bool groups = false,
    bool sessions = false)
{
  return internal::killtree(pid, signal, groups, sessions, processTable);
}

} // namespace os {

#endif // __STOUT_OS_POSIX_KILLTREE_HPP__
