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

#ifndef __TESTS_CLUSTER_HPP__
#define __TESTS_CLUSTER_HPP__

#include <memory>
#include <string>

#include <medios/medios.hpp>

#include <medios/authentication/secret_generator.hpp>

#include <medios/authorizer/authorizer.hpp>

#include <medios/allocator/allocator.hpp>
#include <medios/controller/contender.hpp>
#include <medios/controller/detector.hpp>

#include <medios/node/resource_estimator.hpp>

#include <medios/state/in_memory.hpp>
#include <medios/state/state.hpp>
#include <medios/state/storage.hpp>

#include <process/owned.hpp>
#include <process/pid.hpp>

#include <stout/none.hpp>
#include <stout/option.hpp>
#include <stout/try.hpp>

#include "authorizer/local/authorizer.hpp"

#include "files/files.hpp"

#include "controller/constants.hpp"
#include "controller/flags.hpp"
#include "controller/controller.hpp"

#include "node/constants.hpp"
#include "node/flags.hpp"
#include "node/gc.hpp"
#include "node/node.hpp"
#include "node/task_status_update_manager.hpp"

#include "node/containerizer/containerizer.hpp"

#include "tests/mock_registrar.hpp"
#include "tests/mock_node.hpp"

namespace medios {
namespace internal {
namespace tests {
namespace cluster {

class Controller
{
public:
  // Factory method that starts a new controller with the provided flags and
  // injections. The destructor of this object will cleanup some, but not
  // all of its state by calling the injections. Cleanup includes:
  //   * All destructors of each controller dependency created in this factory.
  //   * Unsetting the libprocess authenticator.
  //   * Terminating the controller process.
  static Try<process::Owned<Controller>> start(
      const controller::Flags& flags = controller::Flags(),
      const Option<medios::allocator::Allocator*>& allocator = None(),
      const Option<Authorizer*>& authorizer = None(),
      const Option<std::shared_ptr<process::RateLimiter>>&
        nodeRemovalLimiter = None());

  ~Controller();

  // Returns a new controller detector for this instance of controller.
  process::Owned<medios::controller::detector::ControllerDetector> createDetector();

  // Returns the `ControllerInfo` associated with the underlying controller process.
  ControllerInfo getControllerInfo();

  // The underlying controller process.
  process::PID<controller::Controller> pid;

  // Sets authorization callbacks in libprocess.
  void setAuthorizationCallbacks(Authorizer* authorizer);

private:
  Controller() : files(controller::READONLY_HTTP_AUTHENTICATION_REALM) {};

  // Not copyable, not assignable.
  Controller(const Controller&) = delete;
  Controller& operator=(const Controller&) = delete;

  Files files;

  // Dependencies that are created by the factory method. The order in
  // which these fields are declared should match the order in which
  // they are initialized by the constructor; this ensures that
  // dependencies between these fields are handled correctly during
  // destruction.

  process::Owned<medios::allocator::Allocator> allocator;
  process::Owned<Authorizer> authorizer;
  process::Owned<medios::controller::contender::ControllerContender> contender;
  process::Owned<medios::controller::detector::ControllerDetector> detector;
  process::Owned<medios::state::Storage> storage;
  process::Owned<medios::state::State> state;
public:
  // Exposed for testing and mocking purposes. We always use a
  // `MockRegistrar` in case the test case wants to inspect how the
  // controller interacts with the registrar; by default, the mock
  // registrar behaves identically to the normal registrar.
  process::Owned<MockRegistrar> registrar;

  // The underlying controller object.
  process::Owned<controller::Controller> controller;

private:
  Option<std::shared_ptr<process::RateLimiter>> nodeRemovalLimiter;

  // Indicates whether or not authorization callbacks were set when this controller
  // was constructed.
  bool authorizationCallbacksSet;
};


class Node
{
public:
  // Factory method that creates a new node with the provided flags and
  // injections. The destructor of this object will cleanup some, but not
  // all of its state by calling the injections. Cleanup includes:
  //   * All destructors of each node dependency created in this factory.
  //   * If neither `terminate` nor `shutdown` are called, all containers
  //     will be destroyed before termination.
  //   * Terminating the node process.
  //   * On Linux, we will simulate an OS process exiting.
  static Try<process::Owned<Node>> create(
      medios::controller::detector::ControllerDetector* detector,
      const node::Flags& flags = node::Flags(),
      const Option<std::string>& id = None(),
      const Option<node::Containerizer*>& containerizer = None(),
      const Option<node::GarbageCollector*>& gc = None(),
      const Option<node::TaskStatusUpdateManager*>& taskStatusUpdateManager =
        None(),
      const Option<medios::node::ResourceEstimator*>& resourceEstimator =
        None(),
      const Option<medios::node::QoSController*>& qosController = None(),
      const Option<medios::SecretGenerator*>& secretGenerator = None(),
      const Option<Authorizer*>& authorizer = None(),
      const Option<PendingFutureTracker*>& futureTracker = None(),
      bool mock = false);

  ~Node();

  // Stops this node by either dispatching a shutdown call to the underlying
  // node process or terminating it. If either of these methods are called,
  // this wrapper object will not clean up containers during its destruction.
  // NOTE: Destroying the containerizer does not clean up containers.
  //
  // These methods are useful if the test wants to emulate the node's
  // shutdown/termination logic. For example, the node-recovery tests do
  // not want to destroy all containers when restarting the node.
  void shutdown();
  void terminate();

  // Returns the pointer to the mock node object. Returns nullptr if
  // this is not a mock node.
  MockNode* mock();

  // Start the mock node as it is not auto started. This is a no-op
  // if it is a real node.
  void start();

  // The underlying node process.
  process::PID<node::Node> pid;

  // Sets authorization callbacks in libprocess.
  void setAuthorizationCallbacks(Authorizer* authorizer);

private:
  Node() : files(node::READONLY_HTTP_AUTHENTICATION_REALM) {};

  // Not copyable, not assignable.
  Node(const Node&) = delete;
  Node& operator=(const Node&) = delete;

  // Helper for `shutdown` and `terminate`.
  // Waits for the underlying node process to finish and then
  // (Linux-only) simulates an OS process exiting.
  void wait();

  node::Flags flags;
  Files files;

  // This is set to `false` if either `shutdown()` or `terminate()` are called.
  // If false, the destructor of this `Node` will not clean up containers.
  bool cleanUpContainersInDestructor = true;

  // Controller detector that is not managed by this object.
  medios::controller::detector::ControllerDetector* detector = nullptr;

  // Containerizer that is either owned outside of this `Node` object
  // or by `ownedContainerizer`.  We keep a copy of this pointer
  // because the cleanup logic acts upon the containerizer (regardless
  // of who created it).
  node::Containerizer* containerizer = nullptr;

  // Pending future tracker must be destroyed last since there may be
  // pending requests related to the dependant objects declared below.
  process::Owned<PendingFutureTracker> futureTracker;

  // Dependencies that are created by the factory method.
  process::Owned<Authorizer> authorizer;
  process::Owned<node::Containerizer> ownedContainerizer;
  process::Owned<node::GarbageCollector> gc;
  process::Owned<medios::node::QoSController> qosController;
  process::Owned<medios::node::ResourceEstimator> resourceEstimator;
  process::Owned<medios::SecretGenerator> secretGenerator;
  process::Owned<node::TaskStatusUpdateManager> taskStatusUpdateManager;

  // Indicates whether or not authorization callbacks were set when this node
  // was constructed.
  bool authorizationCallbacksSet;

  // The underlying node object.
  process::Owned<node::Node> node;
};

} // namespace cluster {
} // namespace tests {
} // namespace internal {
} // namespace medios {

#endif // __TESTS_CLUSTER_HPP__
