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

#ifndef __HOOK_MANAGER_HPP__
#define __HOOK_MANAGER_HPP__

#include <string>

#include <medios/medios.hpp>
#include <medios/hook.hpp>

#include <process/future.hpp>

#include <stout/try.hpp>

namespace medios {
namespace internal {

// Loads operator-supplied Hook modules and invokes them at the fixed
// controller/node lifecycle points (see include/medios/hook.hpp for the points
// themselves). Every method below fans out to all loaded hooks; nothing here
// holds task or node policy of its own.
//
// Dispatch contract, shared by all the methods below:
//   - Hooks run in the order named in --hooks (the backing store is a
//     LinkedHashMap).
//   - The decorator methods chain: each hook sees the previous hook's result on
//     one mutable copy, and the combined value is returned. A hook returning
//     None() leaves the value unchanged; a hook returning a value overwrites the
//     whole field (labels/resources/environment/...), so a well-behaved hook
//     extends the incoming value rather than discarding it.
//   - A hook that returns an error is logged at WARNING and skipped. Hook
//     failures never abort a task launch or node startup.
//
// All methods are static and synchronized on a single process-wide lock.
// Callers gate invocation on hooksAvailable() so the common no-hooks path costs
// nothing. See src/hook/README.md for the operator-facing view and an example.
class HookManager
{
public:
  // Instantiates the Hook modules named in `hookList` (the --hooks flag, a
  // comma-separated list) from the already-loaded modules. Errors if a name is
  // unknown to the ModuleManager or listed twice.
  static Try<Nothing> initialize(const std::string& hookList);

  // Exposed just for testing so that we can unload a given
  // hook and remove it from the list of available hooks.
  static Try<Nothing> unload(const std::string& hookName);

  // True once at least one hook module is loaded. Call sites check this before
  // dispatching so a cluster with no hooks does no extra work.
  static bool hooksAvailable();

  // The hook points. Each runs every loaded hook per the dispatch contract
  // above; see include/medios/hook.hpp for what each point means and when the
  // controller or node calls it.
  static Labels controllerLaunchTaskLabelDecorator(
      const TaskInfo& taskInfo,
      const FrameworkInfo& frameworkInfo,
      const NodeInfo& nodeInfo);

  static Resources controllerLaunchTaskResourceDecorator(
      const TaskInfo& taskInfo,
      const Resources& nodeResources);

  static void controllerNodeLostHook(const NodeInfo& nodeInfo);

  static Labels nodeRunTaskLabelDecorator(
      const TaskInfo& taskInfo,
      const ExecutorInfo& executorInfo,
      const FrameworkInfo& frameworkInfo,
      const NodeInfo& nodeInfo);

  static Environment nodeExecutorEnvironmentDecorator(
      ExecutorInfo executorInfo);

  static void nodePostFetchHook(
      const ContainerID& containerId,
      const std::string& directory);

  static void nodeRemoveExecutorHook(
      const FrameworkInfo& frameworkInfo,
      const ExecutorInfo& executorInfo);

  static TaskStatus nodeTaskStatusDecorator(
      const FrameworkID& frameworkId,
      TaskStatus status);

  static Resources nodeResourcesDecorator(
      const NodeInfo& nodeInfo);

  static Attributes nodeAttributesDecorator(
      const NodeInfo& nodeInfo);
};

} // namespace internal {
} // namespace medios {

#endif // __HOOK_MANAGER_HPP__
