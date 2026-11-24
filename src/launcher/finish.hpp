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

#ifndef __LAUNCHER_FINISH_HPP__
#define __LAUNCHER_FINISH_HPP__

#include <string>

#include <medios/medios.hpp>

#include <stout/duration.hpp>
#include <stout/option.hpp>

// The parts of a task's finish hook (DR-042) that are decided without a
// process: what the hook is told about the work command, how long it may
// run, and how its outcome is reported. The command executor owns the
// process; these are the facts it hands over and reads back.
namespace medios {
namespace internal {
namespace finish {

// The sandbox files the hook's stdout and stderr land in.
extern const std::string STDOUT_FILE;
extern const std::string STDERR_FILE;

// The variables the hook receives beside the executor's environment.
extern const std::string WORK_EXIT_STATUS_VARIABLE;
extern const std::string TASK_ID_VARIABLE;
extern const std::string FRAMEWORK_ID_VARIABLE;
extern const std::string NODE_ID_VARIABLE;


// What `MEDIOS_WORK_EXIT_STATUS` says about a wait status: the exit code
// when the work command exited, and 128 plus the signal when it was
// signalled, which is the shell's `$?` convention and what a hook written
// as a shell script expects.
int workExitStatus(int waitStatus);


// The hook's environment, in order of precedence: the executor's own, then
// the command's (`TaskFinish.command.environment`, the framework's
// variables for the hook, which override the executor's), then the four
// variables on top, so nothing a framework sets overrides what the
// executor knows about the work command.
Environment hookEnvironment(
    const Environment& executorEnvironment,
    const Environment& commandEnvironment,
    int workWaitStatus,
    const TaskID& taskId,
    const FrameworkID& frameworkId,
    const std::string& nodeId);


// The bound the hook runs under: the cap, or what the task's ceiling has
// left minus the kill grace period, whichever is smaller, so that when the
// ceiling is the bound the SIGKILL that follows SIGTERM by the grace
// period lands no later than the deadline. None when that leaves nothing
// positive, in which case the hook does not start (DR-042).
Option<Duration> limitFor(
    const Duration& cap,
    const Option<Duration>& ceilingRemaining,
    const Duration& gracePeriod);


// The outcomes, each as the typed field the terminal status carries.
FinishStatus ran(
    int waitStatus,
    const Duration& limit,
    const Duration& duration);

FinishStatus timedOut(const Duration& limit, const Duration& duration);

FinishStatus killed(const Duration& limit, const Duration& duration);

FinishStatus didNotStart(const std::string& reason);

// Ran, and the outcome could not be read.
FinishStatus lost(
    const std::string& reason,
    const Duration& limit,
    const Duration& duration);

} // namespace finish {
} // namespace internal {
} // namespace medios {

#endif // __LAUNCHER_FINISH_HPP__
