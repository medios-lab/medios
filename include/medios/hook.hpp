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

#ifndef __MEDIOS_HOOK_HPP__
#define __MEDIOS_HOOK_HPP__

#include <map>
#include <string>

#include <medios/attributes.hpp>
#include <medios/medios.hpp>
#include <medios/resources.hpp>

#include <process/future.hpp>

#include <stout/none.hpp>
#include <stout/nothing.hpp>
#include <stout/result.hpp>
#include <stout/try.hpp>

// ONLY USEFUL AFTER RUNNING PROTOC.
#include <medios/module/hook.pb.h>

namespace medios {

// Interface for an operator-supplied extension module. A hook module subclasses
// Hook and overrides only the points it cares about; the rest keep the no-op
// defaults below. At each point the controller or node hands the hook the
// relevant state and uses what it returns:
//   - the decorator methods (Result<T>) return None() to leave the value
//     unchanged, or a value to overwrite that field;
//   - the side-effect methods (Try<Nothing>) return an error to signal failure,
//     which the dispatcher logs but does not treat as fatal.
// Modules are loaded with --modules and activated by name with --hooks; the
// HookManager (src/hook/manager.hpp) loads them and runs every loaded hook at
// each point. medios ships no hook modules by default. See src/hook/README.md.
class Hook
{
public:
  virtual ~Hook() {}

  // This label decorator hook is called from within controller during
  // the launchTask routine. A module implementing the hook creates
  // and returns a set of labels. These labels overwrite the existing
  // labels on the task info.
  virtual Result<Labels> controllerLaunchTaskLabelDecorator(
      const TaskInfo& taskInfo,
      const FrameworkInfo& frameworkInfo,
      const NodeInfo& nodeInfo)
  {
    return None();
  }

  // This resource decorator hook is called from within the controller during
  // the launchTask routine. An implementation of this hook allows the user
  // to, for example, allocate a default amount of a certain resource if it is
  // not provided by the framework. This hook aims to provide default values for
  // resources that must be accounted. Implementing this hook is useful when one
  // a new resource is introduced and at least one framework does not support it
  // yet. Note that the original resources from the TaskInfo will be overwritten
  // by the ones returned by the hook.
  virtual Result<Resources> controllerLaunchTaskResourceDecorator(
      const TaskInfo& task,
      const Resources& nodeResources)
  {
    return None();
  }

  // This label decorator hook is called from within the node when
  // receiving a run task request from the controller. A module
  // implementing the hook creates and returns a set of labels. These
  // labels overwrite the existing labels on the task info.
  virtual Result<Labels> nodeRunTaskLabelDecorator(
      const TaskInfo& taskInfo,
      const ExecutorInfo& executorInfo,
      const FrameworkInfo& frameworkInfo,
      const NodeInfo& nodeInfo)
  {
    return None();
  }


  // This hook is called when an Node is removed i.e. deemed lost by the
  // controller. The hook is invoked after all frameworks have been informed about
  // the loss.
  virtual Try<Nothing> controllerNodeLostHook(const NodeInfo& nodeInfo)
  {
    return Nothing();
  }


  // This environment decorator hook is called from within node when
  // launching a new executor. A module implementing the hook creates
  // and returns a set of environment variables. These environment
  // variables then become part of the executor's environment.
  // Ideally, a hook module will also look at the existing environment
  // variables in executorInfo and extend the values as needed in case
  // of a conflict.
  virtual Result<Environment> nodeExecutorEnvironmentDecorator(
      const ExecutorInfo& executorInfo)
  {
    return None();
  }

  // This hook is called from within node after container image provisioning.
  // A typical module implementing this hook will perform setup work in the
  // container sandbox before the command starts.
  virtual Try<Nothing> nodePostFetchHook(
      const ContainerID& containerId,
      const std::string& directory)
  {
    return Nothing();
  }


  // This hook is called from within node when an executor is being
  // removed. A typical module implementing the hook will perform some
  // cleanup as required.
  virtual Try<Nothing> nodeRemoveExecutorHook(
      const FrameworkInfo& frameworkInfo,
      const ExecutorInfo& executorInfo)
  {
    return Nothing();
  }

  // This hook is called from within node when it receives a status update from
  // the executor. A module implementing the hook creates and returns a
  // TaskStatus with a set of labels and container_status. These labels and
  // container status overwrite the existing labels on the TaskStatus. Remaining
  // fields from the returned TaskStatus are discarded.
  virtual Result<TaskStatus> nodeTaskStatusDecorator(
      const FrameworkID& frameworkId,
      const TaskStatus& status)
  {
    return None();
  }

  // This hook is called from within the node when it initializes. A module
  // implementing the hook creates and returns a Resources object with the new
  // list of resources available on the node before they are advertised to the
  // controller. These new resources overwrite the previous ones in NodeInfo.
  virtual Result<Resources> nodeResourcesDecorator(
      const NodeInfo& nodeInfo)
  {
    return None();
  }

  // This hook is called from within the node when it initializes. A module
  // implementing the hook creates and returns an Attributes object with the
  // new list of attributes for the node before they are advertised to the
  // controller. These new attributes overwrite the previous ones in NodeInfo.
  virtual Result<Attributes> nodeAttributesDecorator(
      const NodeInfo& nodeInfo)
  {
    return None();
  }
};

} // namespace medios {

#endif // __MEDIOS_HOOK_HPP__
