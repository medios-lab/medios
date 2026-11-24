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

#include <mutex>
#include <string>
#include <vector>

#include <medios/hook.hpp>

#include <medios/module/hook.hpp>

#include <process/collect.hpp>
#include <process/future.hpp>

#include <stout/check.hpp>
#include <stout/foreach.hpp>
#include <stout/linkedhashmap.hpp>
#include <stout/nothing.hpp>
#include <stout/strings.hpp>
#include <stout/try.hpp>

#include "hook/manager.hpp"
#include "module/manager.hpp"

using std::map;
using std::string;
using std::vector;

using process::collect;
using process::Future;

using medios::modules::ModuleManager;

namespace medios {
namespace internal {

static std::mutex* mutex = new std::mutex();
static LinkedHashMap<string, Hook*>* availableHooks =
  new LinkedHashMap<string, Hook*>();


Try<Nothing> HookManager::initialize(const string& hookList)
{
  synchronized (mutex) {
    const vector<string> hooks = strings::split(hookList, ",");
    foreach (const string& hook, hooks) {
      if (availableHooks->contains(hook)) {
        return Error("Hook module '" + hook + "' already loaded");
      }

      if (!ModuleManager::contains<Hook>(hook)) {
        return Error("No hook module named '" + hook + "' available");
      }

      // Let's create an instance of the hook module.
      Try<Hook*> module = ModuleManager::create<Hook>(hook);
      if (module.isError()) {
        return Error(
            "Failed to instantiate hook module '" + hook + "': " +
            module.error());
      }

      // Add the hook module to the list of available hooks.
      (*availableHooks)[hook] = module.get();
    }
  }

  return Nothing();
}


Try<Nothing> HookManager::unload(const string& hookName)
{
  synchronized (mutex) {
    if (!availableHooks->contains(hookName)) {
      return Error(
          "Error unloading hook module '" + hookName + "': module not loaded");
    }

    // Now remove the hook from the list of available hooks.
    availableHooks->erase(hookName);
  }

  return Nothing();
}


bool HookManager::hooksAvailable()
{
  synchronized (mutex) {
    return !availableHooks->empty();
  }
}


Labels HookManager::controllerLaunchTaskLabelDecorator(
    const TaskInfo& taskInfo,
    const FrameworkInfo& frameworkInfo,
    const NodeInfo& nodeInfo)
{
  // We need a mutable copy of the task info and set the new
  // labels after each hook invocation. Otherwise, the last hook
  // will be the only effective hook setting the labels.
  TaskInfo taskInfo_ = taskInfo;

  synchronized (mutex) {
    foreachpair (const string& name, Hook* hook, *availableHooks) {
      Result<Labels> result =
        hook->controllerLaunchTaskLabelDecorator(
            taskInfo_,
            frameworkInfo,
            nodeInfo);

      // NOTE: If the hook returns None(), the task labels won't be
      // changed.
      if (result.isSome()) {
        *taskInfo_.mutable_labels() = std::move(result.get());
      } else if (result.isError()) {
        LOG(WARNING) << "Controller label decorator hook failed for module '"
                    << name << "': " << result.error();
      }
    }
  }

  return std::move(*taskInfo_.mutable_labels());
}


Resources HookManager::controllerLaunchTaskResourceDecorator(
    const TaskInfo& taskInfo,
    const Resources& nodeResources)
{
  TaskInfo taskInfo_ = taskInfo;

  synchronized (mutex) {
    foreachpair (const string& name, Hook* hook, *availableHooks) {
      Result<Resources> result =
        hook->controllerLaunchTaskResourceDecorator(
          taskInfo_,
          nodeResources);

      if (result.isSome()) {
        *taskInfo_.mutable_resources() = std::move(result.get());
      } else if (result.isError()) {
        LOG(WARNING) << "Controller resource decorator hook failed for module '"
                     << name << "': " << result.error();
      }
    }
  }
  return std::move(*taskInfo_.mutable_resources());
}


void HookManager::controllerNodeLostHook(const NodeInfo& nodeInfo)
{
  synchronized (mutex) {
    foreachpair (const string& name, Hook* hook, *availableHooks) {
      Try<Nothing> result = hook->controllerNodeLostHook(nodeInfo);
      if (result.isError()) {
        LOG(WARNING) << "Controller node-lost hook failed for module '"
                     << name << "': " << result.error();
      }
    }
  }
}


Labels HookManager::nodeRunTaskLabelDecorator(
    const TaskInfo& taskInfo,
    const ExecutorInfo& executorInfo,
    const FrameworkInfo& frameworkInfo,
    const NodeInfo& nodeInfo)
{
  synchronized (mutex) {
    TaskInfo taskInfo_ = taskInfo;

    foreachpair (const string& name, Hook* hook, *availableHooks) {
      const Result<Labels> result = hook->nodeRunTaskLabelDecorator(
          taskInfo_, executorInfo, frameworkInfo, nodeInfo);

      // NOTE: If the hook returns None(), the task labels won't be
      // changed.
      if (result.isSome()) {
        taskInfo_.mutable_labels()->CopyFrom(result.get());
      } else if (result.isError()) {
        LOG(WARNING) << "Node label decorator hook failed for module '"
                    << name << "': " << result.error();
      }
    }

    return taskInfo_.labels();
  }
}


Environment HookManager::nodeExecutorEnvironmentDecorator(
    ExecutorInfo executorInfo)
{
  synchronized (mutex) {
    foreachpair (const string& name, Hook* hook, *availableHooks) {
      const Result<Environment> result =
        hook->nodeExecutorEnvironmentDecorator(executorInfo);

      // NOTE: If the hook returns None(), the environment won't be
      // changed.
      if (result.isSome()) {
        executorInfo.mutable_command()->mutable_environment()->CopyFrom(
            result.get());
      } else if (result.isError()) {
        LOG(WARNING) << "Node environment decorator hook failed for module '"
                    << name << "': " << result.error();
      }
    }

    return executorInfo.command().environment();
  }
}


void HookManager::nodePostFetchHook(
    const ContainerID& containerId,
    const string& directory)
{
  synchronized (mutex) {
    foreachpair (const string& name, Hook* hook, *availableHooks) {
      Try<Nothing> result = hook->nodePostFetchHook(containerId, directory);
      if (result.isError()) {
        LOG(WARNING) << "Node post fetch hook failed for module "
                     << "'" << name << "': " << result.error();
      }
    }
  }
}


void HookManager::nodeRemoveExecutorHook(
    const FrameworkInfo& frameworkInfo,
    const ExecutorInfo& executorInfo)
{
  synchronized (mutex) {
    foreachpair (const string& name, Hook* hook, *availableHooks) {
      const Try<Nothing> result =
        hook->nodeRemoveExecutorHook(frameworkInfo, executorInfo);
      if (result.isError()) {
        LOG(WARNING) << "Node remove executor hook failed for module '"
                     << name << "': " << result.error();
      }
    }
  }
}


TaskStatus HookManager::nodeTaskStatusDecorator(
    const FrameworkID& frameworkId,
    TaskStatus status)
{
  synchronized (mutex) {
    foreachpair (const string& name, Hook* hook, *availableHooks) {
      const Result<TaskStatus> result =
        hook->nodeTaskStatusDecorator(frameworkId, status);

      // NOTE: Labels/ContainerStatus remain unchanged if the hook returns
      // None().
      if (result.isSome()) {
        if (result->has_labels()) {
          status.mutable_labels()->CopyFrom(result->labels());
        }

        if (result->has_container_status()) {
          status.mutable_container_status()->CopyFrom(
              result->container_status());
        }
      } else if (result.isError()) {
        LOG(WARNING) << "Node TaskStatus decorator hook failed for "
                     << "module '" << name << "': " << result.error();
      }
    }

    return status;
  }
}


Resources HookManager::nodeResourcesDecorator(
    const NodeInfo& nodeInfo)
{
  // We need a mutable copy of the Resources object. Each hook will
  // see the changes made by previous hooks, so the order of execution
  // matters. Hooks are executed in the order they are specified by
  // the user (note that `availableHooks` is a LinkedHashMap).
  NodeInfo nodeInfo_ = nodeInfo;

  synchronized (mutex) {
    foreachpair (const string& name, Hook* hook, *availableHooks) {
      const Result<Resources> result =
        hook->nodeResourcesDecorator(nodeInfo_);

      // NOTE: Resources remain unchanged if the hook returns None().
      if (result.isSome()) {
        nodeInfo_.mutable_resources()->CopyFrom(result.get());
      } else if (result.isError()) {
        LOG(WARNING) << "Node Resources decorator hook failed for "
                     << "module '" << name << "': " << result.error();
      }
    }

    return nodeInfo_.resources();
  }
}


Attributes HookManager::nodeAttributesDecorator(
    const NodeInfo& nodeInfo)
{
  // We need a mutable copy of the Attributes object. Each hook will
  // see the changes made by previous hooks, so the order of execution
  // matters. Hooks are executed in the order they are specified by
  // the user (note that `availableHooks` is a LinkedHashMap).
  NodeInfo nodeInfo_ = nodeInfo;

  synchronized (mutex) {
    foreachpair (const string& name, Hook* hook, *availableHooks) {
      const Result<Attributes> result =
        hook->nodeAttributesDecorator(nodeInfo_);

      // NOTE: Attributes remain unchanged if the hook returns None().
      if (result.isSome()) {
        nodeInfo_.mutable_attributes()->CopyFrom(result.get());
      } else if (result.isError()) {
        LOG(WARNING) << "Node Attributes decorator hook failed for "
                     << "module '" << name << "': " << result.error();
      }
    }

    return nodeInfo_.attributes();
  }
}

} // namespace internal {
} // namespace medios {
