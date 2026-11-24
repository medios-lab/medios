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

#include <medios/hook.hpp>
#include <medios/medios.hpp>
#include <medios/module.hpp>

#include <medios/module/hook.hpp>

#include <process/future.hpp>
#include <process/id.hpp>
#include <process/process.hpp>
#include <process/protobuf.hpp>

#include <stout/foreach.hpp>
#include <stout/os.hpp>
#include <stout/try.hpp>

#include "messages/messages.hpp"

using namespace medios;

using std::map;
using std::string;

using process::Failure;
using process::Future;

// Must be kept in sync with variables of the same name in
// tests/hook_tests.cpp.
const char* testLabelKey = "MEDIOS_Test_Label";
const char* testLabelValue = "ApacheMedios";
const char* testRemoveLabelKey = "MEDIOS_Test_Remove_Label";
const char* testErrorLabelKey = "MEDIOS_Test_Error_Label";

class HookProcess : public ProtobufProcess<HookProcess>
{
public:
  HookProcess() : ProcessBase(process::ID::generate("example-hook")) {}

  void initialize() override
  {
    install<internal::HookExecuted>(
        &HookProcess::handler,
        &internal::HookExecuted::module);
  }

  void signal()
  {
    LOG(INFO) << "HookProcess emitting signal";

    internal::HookExecuted message;
    message.set_module("org_apache_medios_TestHook");
    send(self(), message);
  }

  void handler(const process::UPID& from, const string& module)
  {
    LOG(INFO) << "HookProcess caught signal: " << module;

    promise.set(Nothing());
  }

  Future<Nothing> await()
  {
    return promise.future();
  }

private:
  process::Promise<Nothing> promise;
};


class TestHook : public Hook
{
public:
  Result<Labels> controllerLaunchTaskLabelDecorator(
      const TaskInfo& taskInfo,
      const FrameworkInfo& frameworkInfo,
      const NodeInfo& nodeInfo) override
  {
    LOG(INFO) << "Executing 'controllerLaunchTaskLabelDecorator' hook";

    Labels labels;

    // Set one known label.
    Label* newLabel = labels.add_labels();
    newLabel->set_key(testLabelKey);
    newLabel->set_value(testLabelValue);

    // Remove the 'testRemoveLabelKey' label which was set by the test.
    foreach (const Label& oldLabel, taskInfo.labels().labels()) {
      if (oldLabel.key() != testRemoveLabelKey) {
        labels.add_labels()->CopyFrom(oldLabel);
      }
    }

    return labels;
  }

  Result<Resources> controllerLaunchTaskResourceDecorator(
      const TaskInfo& taskInfo,
      const Resources& nodeResources) override
  {
    LOG(INFO) << "Executing 'controllerLaunchTaskResourceDecorator' hook";

    // If node does not declare network bandwidth resource,
    // don't set a default value for it.
    if (nodeResources.names().count("network_bandwidth") == 0) {
      return taskInfo.resources();
    }

    Resources taskResources = taskInfo.resources();

    // Add a default value for network bandwidth if absent.
    if (taskResources.names().count("network_bandwidth") == 0) {
      Resources bandwidth =
        CHECK_NOTERROR(Resources::parse("network_bandwidth:10"));

      CHECK(!taskResources.allocations().empty());
      bandwidth.allocate(taskResources.allocations().begin()->first);

      taskResources += bandwidth;
    }

    return taskResources;
  }

  Try<Nothing> controllerNodeLostHook(const NodeInfo& nodeInfo) override
  {
    LOG(INFO) << "Executing 'controllerNodeLostHook' in node '"
              << nodeInfo.id() << "'";

    // TODO(nnielsen): Add argument to signal(), so we can filter messages from
    // the `controllerNodeLostHook` from `nodeRemoveExecutorHook`.
    // NOTE: Will not be a problem **as long as** the test doesn't start any
    // tasks.
    HookProcess hookProcess;
    process::spawn(&hookProcess);
    Future<Nothing> future =
      process::dispatch(hookProcess, &HookProcess::await);

    process::dispatch(hookProcess, &HookProcess::signal);

    // Make sure we don't terminate the process before the message self-send has
    // completed.
    future.await();

    process::terminate(hookProcess);
    process::wait(hookProcess);

    return Nothing();
  }


  // TODO(nnielsen): Split hook tests into multiple modules to avoid
  // interference.
  Result<Labels> nodeRunTaskLabelDecorator(
      const TaskInfo& taskInfo,
      const ExecutorInfo& executorInfo,
      const FrameworkInfo& frameworkInfo,
      const NodeInfo& nodeInfo) override
  {
    LOG(INFO) << "Executing 'nodeRunTaskLabelDecorator' hook";

    Labels labels;

    // Set one known label.
    Label* newLabel = labels.add_labels();
    newLabel->set_key("baz");
    newLabel->set_value("qux");

    // Remove label which was set by test.
    foreach (const Label& oldLabel, taskInfo.labels().labels()) {
      if (oldLabel.key() != "foo") {
        labels.add_labels()->CopyFrom(oldLabel);
      }
    }

    return labels;
  }


  // In this hook, we create a new environment variable "FOO" and set
  // it's value to "bar".
  Result<Environment> nodeExecutorEnvironmentDecorator(
      const ExecutorInfo& executorInfo) override
  {
    LOG(INFO) << "Executing 'nodeExecutorEnvironmentDecorator' hook";

    Environment environment;

    if (executorInfo.command().has_environment()) {
      environment.CopyFrom(executorInfo.command().environment());
    }

    Environment::Variable* variable = environment.add_variables();
    variable->set_name("FOO");
    variable->set_value("bar");

    return environment;
  }


  Try<Nothing> nodePostFetchHook(
      const ContainerID& containerId,
      const string& directory) override
  {
    LOG(INFO) << "Executing 'nodePostFetchHook'";

    const string path = path::join(directory, "post_fetch_hook");

    if (os::exists(path)) {
      return os::rm(path);
    } else {
      return Nothing();
    }
  }


  // This hook locates the file created by environment decorator hook
  // and deletes it.
  Try<Nothing> nodeRemoveExecutorHook(
      const FrameworkInfo& frameworkInfo,
      const ExecutorInfo& executorInfo) override
  {
    LOG(INFO) << "Executing 'nodeRemoveExecutorHook'";

    // Send a HookExecuted message to ourself. The hook test
    // "VerifyNodeLaunchExecutorHook" will wait for the testing
    // infrastructure to intercept this message. The intercepted message
    // indicates successful execution of this hook.
    HookProcess hookProcess;
    process::spawn(&hookProcess);
    Future<Nothing> future =
      process::dispatch(hookProcess, &HookProcess::await);

    process::dispatch(hookProcess, &HookProcess::signal);

    // Make sure we don't terminate the process before the message self-send has
    // completed.
    future.await();

    process::terminate(hookProcess);
    process::wait(hookProcess);

    return Nothing();
  }


  Result<TaskStatus> nodeTaskStatusDecorator(
      const FrameworkID& frameworkId,
      const TaskStatus& status) override
  {
    LOG(INFO) << "Executing 'nodeTaskStatusDecorator' hook";

    Labels labels;

    // Set one known label.
    Label* newLabel = labels.add_labels();
    newLabel->set_key("bar");
    newLabel->set_value("qux");

    // Remove label which was set by test.
    foreach (const Label& oldLabel, status.labels().labels()) {
      if (oldLabel.key() != "foo") {
        labels.add_labels()->CopyFrom(oldLabel);
      }
    }

    TaskStatus result;
    result.mutable_labels()->CopyFrom(labels);

    // Set an IP address, a network isolation group, and a known label
    // in network info. This data is later validated by the
    // 'HookTest.VerifyNodeTaskStatusDecorator' test.
    NetworkInfo* networkInfo =
      result.mutable_container_status()->add_network_infos();

    NetworkInfo::IPAddress* ipAddress = networkInfo->add_ip_addresses();
    ipAddress->set_ip_address("4.3.2.1");
    networkInfo->add_groups("public");

    Label* networkInfoLabel = networkInfo->mutable_labels()->add_labels();
    networkInfoLabel->set_key("net_foo");
    networkInfoLabel->set_value("net_bar");

    return result;
  }


  Result<Resources> nodeResourcesDecorator(
      const NodeInfo& nodeInfo) override
  {
    LOG(INFO) << "Executing 'nodeResourcesDecorator' hook";

    Resources resources;
    // Remove the existing "cpus" resource, it will be overwritten by the
    // current hook. Keep other resources unchanged.
    foreach (const Resource& resource, nodeInfo.resources()) {
      if (resource.name() != "cpus") {
        resources += resource;
      }
    }

    // Force the value of "cpus" to 4 and add a new custom resource named "foo"
    // of type set.
    resources += Resources::parse("cpus:4;foo:{bar,baz}").get();

    return resources;
  }


  Result<Attributes> nodeAttributesDecorator(
      const NodeInfo& nodeInfo) override
  {
    LOG(INFO) << "Executing 'nodeAttributesDecorator' hook";

    Attributes attributes = nodeInfo.attributes();
    attributes.add(Attributes::parse("rack", "rack1"));

    return attributes;
  }
};


static Hook* createHook(const Parameters& parameters)
{
  return new TestHook();
}


// Declares a Hook module named 'org_apache_medios_TestHook'.
medios::modules::Module<Hook> org_apache_medios_TestHook(
    MEDIOS_MODULE_API_VERSION,
    MEDIOS_VERSION,
    "Apache Mesos",
    "modules@mesos.apache.org",
    "Test Hook module.",
    nullptr,
    createHook);
