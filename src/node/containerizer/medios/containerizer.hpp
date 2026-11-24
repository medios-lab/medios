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

#ifndef __MEDIOS_CONTAINERIZER_HPP__
#define __MEDIOS_CONTAINERIZER_HPP__

#include <vector>

#include <medios/secret/resolver.hpp>

#include <medios/node/isolator.hpp>

#include <process/clock.hpp>
#include <process/http.hpp>
#include <process/id.hpp>
#include <process/sequence.hpp>
#include <process/shared.hpp>
#include <process/time.hpp>

#include <process/metrics/counter.hpp>

#include <stout/hashmap.hpp>
#include <stout/lambda.hpp>
#include <stout/multihashmap.hpp>
#include <stout/try.hpp>
#include <stout/os/int_fd.hpp>

#include "node/gc.hpp"
#include "node/state.hpp"

#include "node/containerizer/containerizer.hpp"
#include "node/containerizer/fetcher.hpp"

#include "node/containerizer/medios/launcher.hpp"

#include "node/containerizer/medios/io/switchboard.hpp"


#include "node/containerizer/medios/provisioner/provisioner.hpp"

namespace medios {
namespace internal {
namespace node {

// Grants a container the device access its isolators declared in their
// launch info. Supplied by the factory on hosts with a device manager;
// absent elsewhere, in which case any declaration fails the launch.
using DeviceGranter = lambda::function<process::Future<Nothing>(
    const ContainerID&, const std::vector<DeviceAccess>&)>;


// Every device access declared across the isolators' launch infos, with
// each entry checked for a path and at least one access bit.
Try<std::vector<DeviceAccess>> collectDeclaredDevices(
    const std::vector<Option<medios::node::ContainerLaunchInfo>>& launchInfos);

// If the container class is not of type `DEBUG` (i.e., it is not set or
// `DEFAULT`), we log the line at the INFO level. Otherwise, we use VLOG(1).
// The purpose of this macro is to avoid polluting node logs with information
// related to `DEBUG` containers as this type of container can run periodically.
#define LOG_BASED_ON_CLASS(containerClass) \
  LOG_IF(INFO, (containerClass != ContainerClass::DEBUG) || VLOG_IS_ON(1))

// Forward declaration.
class MediosContainerizerProcess;


class MediosContainerizer : public Containerizer
{
public:
  static Try<MediosContainerizer*> create(
      const Flags& flags,
      bool local,
      GarbageCollector* gc = nullptr,
      SecretResolver* secretResolver = nullptr,
      VolumeGidManager* volumeGidManager = nullptr,
      PendingFutureTracker* futureTracker = nullptr);

  static Try<MediosContainerizer*> create(
      const Flags& flags,
      bool local,
      GarbageCollector* gc,
      const process::Owned<Launcher>& launcher,
      const process::Shared<Provisioner>& provisioner,
      const std::vector<process::Owned<medios::node::Isolator>>& isolators,
      VolumeGidManager* volumeGidManager = nullptr,
      const Option<DeviceGranter>& deviceGranter = None());

  ~MediosContainerizer() override;

  process::Future<Nothing> recover(
      const Option<state::NodeState>& state) override;

  process::Future<Containerizer::LaunchResult> launch(
      const ContainerID& containerId,
      const medios::node::ContainerConfig& containerConfig,
      const std::map<std::string, std::string>& environment,
      const Option<std::string>& pidCheckpointPath) override;

  process::Future<process::http::Connection> attach(
      const ContainerID& containerId) override;

  process::Future<Nothing> update(
      const ContainerID& containerId,
      const Resources& resourceRequests,
      const google::protobuf::Map<
          std::string, Value::Scalar>& resourceLimits = {}) override;

  process::Future<ResourceStatistics> usage(
      const ContainerID& containerId) override;

  process::Future<ContainerStatus> status(
      const ContainerID& containerId) override;

  process::Future<Option<medios::node::ContainerTermination>> wait(
      const ContainerID& containerId) override;

  process::Future<Option<medios::node::ContainerTermination>> destroy(
      const ContainerID& containerId) override;

  process::Future<bool> kill(
      const ContainerID& containerId,
      int signal) override;

  process::Future<hashset<ContainerID>> containers() override;

  process::Future<Nothing> remove(const ContainerID& containerId) override;

  process::Future<Nothing> pruneImages(
      const std::vector<Image>& excludedImages) override;

private:
  explicit MediosContainerizer(
      const process::Owned<MediosContainerizerProcess>& process);

  process::Owned<MediosContainerizerProcess> process;
};


class MediosContainerizerProcess
  : public process::Process<MediosContainerizerProcess>
{
public:
  MediosContainerizerProcess(
      const Flags& _flags,
      GarbageCollector* _gc,
      IOSwitchboard* _ioSwitchboard,
      const process::Owned<Launcher>& _launcher,
      const process::Owned<Fetcher>& _fetcher,
      const process::Shared<Provisioner>& _provisioner,
      const std::vector<process::Owned<medios::node::Isolator>>& _isolators,
      VolumeGidManager* _volumeGidManager,
      const Option<DeviceGranter>& _deviceGranter,
      const Option<int_fd>& _initMemFd,
      const Option<int_fd>& _commandExecutorMemFd)
    : ProcessBase(process::ID::generate("medios-containerizer")),
      flags(_flags),
      gc(_gc),
      ioSwitchboard(_ioSwitchboard),
      launcher(_launcher),
      fetcher(_fetcher),
      provisioner(_provisioner),
      isolators(_isolators),
      volumeGidManager(_volumeGidManager),
      deviceGranter(_deviceGranter),
      initMemFd(_initMemFd),
      commandExecutorMemFd(_commandExecutorMemFd) {}

  ~MediosContainerizerProcess() override
  {
    if (initMemFd.isSome()) {
      Try<Nothing> close = os::close(initMemFd.get());
      if (close.isError()) {
        LOG(WARNING) << "Failed to close memfd '" << stringify(initMemFd.get())
                     << "': " << close.error();
      }
    }

    if (commandExecutorMemFd.isSome()) {
      Try<Nothing> close = os::close(commandExecutorMemFd.get());
      if (close.isError()) {
        LOG(WARNING) << "Failed to close memfd '"
                     << stringify(commandExecutorMemFd.get())
                     << "': " << close.error();
      }
    }
  }

  virtual process::Future<Nothing> recover(
      const Option<state::NodeState>& state);

  virtual process::Future<Containerizer::LaunchResult> launch(
      const ContainerID& containerId,
      const medios::node::ContainerConfig& containerConfig,
      const std::map<std::string, std::string>& environment,
      const Option<std::string>& pidCheckpointPath);

  virtual process::Future<process::http::Connection> attach(
      const ContainerID& containerId);

  virtual process::Future<Nothing> update(
      const ContainerID& containerId,
      const Resources& resourceRequests,
      const google::protobuf::Map<
          std::string, Value::Scalar>& resourceLimits = {});

  virtual process::Future<ResourceStatistics> usage(
      const ContainerID& containerId);

  virtual process::Future<ContainerStatus> status(
      const ContainerID& containerId);

  virtual process::Future<Option<medios::node::ContainerTermination>> wait(
      const ContainerID& containerId);

  virtual process::Future<Containerizer::LaunchResult> exec(
      const ContainerID& containerId,
      int_fd pipeWrite);

  virtual process::Future<Option<medios::node::ContainerTermination>> destroy(
      const ContainerID& containerId,
      const Option<medios::node::ContainerTermination>& termination);

  virtual process::Future<bool> kill(
      const ContainerID& containerId,
      int signal);

  virtual process::Future<Nothing> remove(const ContainerID& containerId);

  virtual process::Future<hashset<ContainerID>> containers();

  virtual process::Future<Nothing> pruneImages(
      const std::vector<Image>& excludedImages);

private:
  enum State
  {
    STARTING,
    PROVISIONING,
    PREPARING,
    ISOLATING,
    FETCHING,
    RUNNING,
    DESTROYING
  };

  friend std::ostream& operator<<(std::ostream& stream, const State& state);

  process::Future<Nothing> _recover(
      const std::vector<medios::node::ContainerState>& recoverable,
      const hashset<ContainerID>& orphans);

  process::Future<std::vector<Nothing>> recoverIsolators(
      const std::vector<medios::node::ContainerState>& recoverable,
      const hashset<ContainerID>& orphans);

  process::Future<Nothing> recoverProvisioner(
      const std::vector<medios::node::ContainerState>& recoverable,
      const hashset<ContainerID>& orphans);

  process::Future<Nothing> __recover(
      const std::vector<medios::node::ContainerState>& recovered,
      const hashset<ContainerID>& orphans);

  // Applies the devices the isolators declared, after every isolator has
  // prepared, and passes the launch infos through unchanged.
  process::Future<std::vector<Option<medios::node::ContainerLaunchInfo>>>
    grantDeclaredDevices(
        const ContainerID& containerId,
        const std::vector<Option<medios::node::ContainerLaunchInfo>>&
          launchInfos);

  process::Future<Nothing> prepare(
      const ContainerID& containerId,
      const Option<ProvisionInfo>& provisionInfo);

  process::Future<Nothing> fetch(
      const ContainerID& containerId);

  process::Future<Containerizer::LaunchResult> _launch(
      const ContainerID& containerId,
      const Option<medios::node::ContainerIO>& containerIO,
      const std::map<std::string, std::string>& environment,
      const Option<std::string>& pidCheckpointPath);

  process::Future<Nothing> isolate(
      const ContainerID& containerId,
      pid_t _pid);

  // Continues 'destroy()' once nested containers are handled.
  void _destroy(
      const ContainerID& containerId,
      const Option<medios::node::ContainerTermination>& termination,
      const State& previousState,
      const std::vector<
        process::Future<Option<medios::node::ContainerTermination>>>& destroys);

  // Continues '_destroy()' once isolators has completed.
  void __destroy(
      const ContainerID& containerId,
      const Option<medios::node::ContainerTermination>& termination);

  // Continues '__destroy()' once all processes have been killed
  // by the launcher.
  void ___destroy(
      const ContainerID& containerId,
      const Option<medios::node::ContainerTermination>& termination,
      const process::Future<Nothing>& future);

  // Continues '___destroy()' once we get the exit status of the container.
  void ____destroy(
      const ContainerID& containerId,
      const Option<medios::node::ContainerTermination>& termination);

  // Continues '____destroy()' once all isolators have completed
  // cleanup.
  void _____destroy(
      const ContainerID& containerId,
      const Option<medios::node::ContainerTermination>& termination,
      const process::Future<std::vector<process::Future<Nothing>>>& cleanups);

  // Continues '_____destroy()' once provisioner have completed destroy.
  void ______destroy(
      const ContainerID& containerId,
      const Option<medios::node::ContainerTermination>& termination,
      const process::Future<bool>& destroy);

  // Schedules a path for garbage collection based on its modification time.
  // Equivalent to the `Node::garbageCollect` method.
  process::Future<Nothing> garbageCollect(const std::string& path);

  // Call back for when an isolator limits a container and impacts the
  // processes. This will trigger container destruction.
  void limited(
      const ContainerID& containerId,
      const process::Future<medios::node::ContainerLimitation>& future);

  // Helper for reaping the 'init' process of a container.
  process::Future<Option<int>> reap(
      const ContainerID& containerId,
      pid_t pid);

  // Call back for when the executor exits. This will trigger container
  // destroy.
  void reaped(const ContainerID& containerId);

  // TODO(jieyu): Consider introducing an Isolators struct and moving
  // all isolator related operations to that struct.
  process::Future<std::vector<process::Future<Nothing>>> cleanupIsolators(
      const ContainerID& containerId);

  const Flags flags;

  // NOTE: This actor may be nullptr in tests, as not all tests need to
  // share this actor with the node.
  GarbageCollector* gc;

  IOSwitchboard* ioSwitchboard;
  const process::Owned<Launcher> launcher;
  const process::Owned<Fetcher> fetcher;
  const process::Shared<Provisioner> provisioner;
  const std::vector<process::Owned<medios::node::Isolator>> isolators;
  VolumeGidManager* volumeGidManager;
  const Option<DeviceGranter> deviceGranter;
  const Option<int_fd> initMemFd;
  const Option<int_fd> commandExecutorMemFd;

  struct Container
  {
    Container()
      : state(STARTING),
        lastStateTransition(process::Clock::now()),
        sequence("medios-container-status-updates") {}

    // Promise for futures returned from wait().
    process::Promise<medios::node::ContainerTermination> termination;

    // NOTE: this represents 'PID 1', i.e., the "init" of the
    // container that we created (it may be for an executor, or any
    // arbitrary process that has been launched in the event of nested
    // containers).
    Option<pid_t> pid;

    // Sandbox directory for the container. It is optional here because
    // we don't keep track of sandbox directory for orphan containers.
    // It is not checkpointed explicitly; on recovery, it is reconstructed
    // from executor's directory and hierarchy of containers.
    //
    // NOTE: This holds the sandbox path in the host mount namespace,
    // while MEDIOS_SANDBOX is the path in the container mount namespace.
    Option<std::string> directory;

    // We keep track of the future exit status for the container if it
    // has been launched. If the container has not been launched yet,
    // 'status' will be set to None().
    //
    // NOTE: A container has an exit status does not mean that it has
    // been properly destroyed. We need to perform cleanup on
    // isolators and provisioner after that.
    Option<process::Future<Option<int>>> status;

    // We keep track of the future for 'provisioner->provision' so
    // that we can discard the provisioning for the container which
    // is destroyed when it is being provisioned.
    process::Future<ProvisionInfo> provisioning;

    // We keep track of the future that is waiting for all the
    // 'isolator->prepare' to finish so that destroy will only start
    // calling cleanup after all isolators have finished preparing.
    process::Future<std::vector<Option<medios::node::ContainerLaunchInfo>>>
      launchInfos;

    // We keep track of the future that is waiting for all the
    // 'isolator->isolate' futures so that destroy will only start
    // calling cleanup after all isolators have finished isolating.
    process::Future<std::vector<Nothing>> isolation;

    // We keep track of the resource requests and limits for each container so
    // we can set the ResourceStatistics limits in usage().
    Resources resourceRequests;
    google::protobuf::Map<std::string, Value::Scalar> resourceLimits;

    // The configuration for the container to be launched.
    // This can only be None if the underlying container is launched
    // before we checkpoint `ContainerConfig` in MESOS-6894.
    // TODO(zhitao): Drop the `Option` part at the end of deprecation
    // cycle.
    Option<medios::node::ContainerConfig> config;

    // The container class that can be `DEFAULT` or `DEBUG`.
    // Returns `DEFAULT` even if the container class is not defined.
    medios::node::ContainerClass containerClass();

    // Container's information at the moment it was launched. For example,
    // used to bootstrap the launch information of future child DEBUG
    // containers. Checkpointed and restored on recovery. Optional because
    // it is not set for orphan containers.
    //
    // NOTE: Some of these data, may change during the container lifetime,
    // e.g., the working directory. Such changes are not be captured here,
    // which might be problematic, e.g., for DEBUG containers relying on
    // some data in parent working directory.
    Option<medios::node::ContainerLaunchInfo> launchInfo;

    State state;
    process::Time lastStateTransition;

    // Used when `status` needs to be collected from isolators
    // associated with this container. `Sequence` allows us to
    // maintain the order of `status` requests for a given container.
    process::Sequence sequence;

    // Child containers nested under this container.
    hashset<ContainerID> children;
  };

  hashmap<ContainerID, process::Owned<Container>> containers_;

  // Helper to transition container state.
  void transition(const ContainerID& containerId, const State& state);

  // Helper to determine if a container is supported by an isolator.
  bool isSupportedByIsolator(
      const ContainerID& containerId,
      bool isolatorSupportsNesting,
      bool isolatorSupportsStandalone);

  struct Metrics
  {
    Metrics();
    ~Metrics();

    process::metrics::Counter container_destroy_errors;
  } metrics;
};


std::ostream& operator<<(
    std::ostream& stream,
    const MediosContainerizerProcess::State& state);

} // namespace node {
} // namespace internal {
} // namespace medios {

#endif // __MEDIOS_CONTAINERIZER_HPP__
