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

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include <medios/medios.hpp>

#include <medios/authorizer/authorizer.hpp>

#include <medios/allocator/allocator.hpp>

#include <medios/node/resource_estimator.hpp>

#include <medios/state/in_memory.hpp>
#include <medios/state/leveldb.hpp>
#include <medios/state/protobuf.hpp>
#include <medios/state/storage.hpp>

#include <process/clock.hpp>
#include <process/future.hpp>
#include <process/gmock.hpp>
#include <process/gtest.hpp>
#include <process/http.hpp>
#include <process/id.hpp>
#include <process/limiter.hpp>
#include <process/owned.hpp>
#include <process/pid.hpp>
#include <process/process.hpp>

#include <process/ssl/flags.hpp>

#include <stout/duration.hpp>
#include <stout/error.hpp>
#include <stout/gtest.hpp>
#include <stout/none.hpp>
#include <stout/nothing.hpp>
#include <stout/option.hpp>
#include <stout/os.hpp>
#include <stout/path.hpp>
#include <stout/strings.hpp>
#include <stout/try.hpp>

#include <stout/os/permissions.hpp>

#ifdef USE_SSL_SOCKET
#include "authentication/executor/jwt_secret_generator.hpp"
#endif // USE_SSL_SOCKET

#include "authorizer/local/authorizer.hpp"

#include "common/authorization.hpp"

#include "common/domain_sockets.hpp"

#include "common/future_tracker.hpp"
#include "common/http.hpp"

#include "files/files.hpp"

#include "controller/constants.hpp"
#include "controller/flags.hpp"
#include "controller/controller.hpp"
#include "controller/registrar.hpp"

#include "controller/allocator/medios/hierarchical.hpp"

#include "controller/contender/standalone.hpp"

#include "controller/detector/standalone.hpp"

#include "node/flags.hpp"
#include "node/gc.hpp"
#include "node/node.hpp"
#include "node/task_status_update_manager.hpp"

#include "node/containerizer/containerizer.hpp"

#include "node/volume_gid_manager/volume_gid_manager.hpp"

#include "tests/cluster.hpp"
#include "tests/mock_registrar.hpp"

using std::string;
using std::vector;

#ifdef USE_SSL_SOCKET
using medios::authentication::executor::JWTSecretGenerator;
#endif // USE_SSL_SOCKET

using medios::controller::contender::StandaloneControllerContender;

using medios::controller::detector::ControllerDetector;
using medios::controller::detector::StandaloneControllerDetector;

using medios::node::ContainerTermination;

using net::IP;

using process::Owned;

using process::network::unix::Socket;

namespace medios {
namespace internal {
namespace tests {
namespace cluster {

Try<process::Owned<Controller>> Controller::start(
    const controller::Flags& flags,
    const Option<medios::allocator::Allocator*>& allocator,
    const Option<Authorizer*>& authorizer,
    const Option<std::shared_ptr<process::RateLimiter>>& nodeRemovalLimiter)
{
  process::Owned<Controller> controller(new Controller());

  // If the allocator is not provided, create a default one.
  if (allocator.isNone()) {
    Try<medios::allocator::Allocator*> _allocator =
      controller::allocator::HierarchicalDRFAllocator::create();

    if (_allocator.isError()) {
      return Error(
          "Failed to create an instance of HierarchicalDRFAllocator: " +
          _allocator.error());
    }

    controller->allocator.reset(_allocator.get());
  }

  // If the authorizer is not provided, create a default one.
  if (authorizer.isNone()) {
    // Indicates whether or not the caller explicitly specified the
    // authorization configuration for this controller.
    bool authorizationSpecified = true;

    vector<string> authorizerNames = strings::split(flags.authorizers, ",");

    if (authorizerNames.empty()) {
      return Error("No authorizer specified");
    }

    if (authorizerNames.size() > 1) {
      return Error("Multiple authorizers not supported");
    }

    string authorizerName = authorizerNames[0];

    Result<Authorizer*> authorizer((None()));
    if (authorizerName != controller::DEFAULT_AUTHORIZER) {
      LOG(INFO) << "Creating '" << authorizerName << "' authorizer";

      authorizer = Authorizer::create(authorizerName);
    } else {
      // `authorizerName` is `DEFAULT_AUTHORIZER` at this point.
      if (flags.acls.isSome()) {
        LOG(INFO) << "Creating default '" << authorizerName << "' authorizer";

        authorizer = Authorizer::create(flags.acls.get());
        CHECK_SOME(authorizer);
      } else {
        authorizationSpecified = false;
      }
    }

    if (authorizer.isError()) {
      return Error(
          "Could not create '" + authorizerName + "' authorizer: " +
          authorizer.error());
    } else if (authorizer.isSome()) {
      controller->authorizer.reset(authorizer.get());

      if (authorizationSpecified) {
        // Authorization config was explicitly provided, so set authorization
        // callbacks in libprocess.
        controller->setAuthorizationCallbacks(authorizer.get());
      }
    }
  } else {
    // An authorizer was provided, so set authorization callbacks in libprocess.
    controller->setAuthorizationCallbacks(authorizer.get());
  }

  controller->contender.reset(new StandaloneControllerContender());
  controller->detector.reset(new StandaloneControllerDetector());

  // Check for some invalid flag combinations.
  if (flags.registry_strict) {
    return Error("Support for '--registry_strict' has been removed");
  }

  // Create the registry's storage backend.
  if (flags.registry == "in_memory") {
    controller->storage.reset(new medios::state::InMemoryStorage());
  } else if (flags.registry == "leveldb") {
    // Persistent across cluster::Controller restart so long as the test
    // reuses the same controllerFlags (and therefore work_dir). Used by
    // tests that exercise controller-restart recovery semantics (#138).
    if (flags.work_dir.isNone()) {
      return Error("--work_dir is required for registry=leveldb");
    }
    controller->storage.reset(new medios::state::LevelDBStorage(
        path::join(flags.work_dir.get(), "registry-leveldb")));
  } else {
    return Error(
        "Unsupported option for registry persistence: " + flags.registry);
  }

  // Instantiate some other controller dependencies.
  controller->state.reset(new medios::state::State(controller->storage.get()));
  controller->registrar.reset(new MockRegistrar(
      flags, controller->state.get(), controller::READONLY_HTTP_AUTHENTICATION_REALM));

  if (nodeRemovalLimiter.isNone() && flags.node_removal_rate_limit.isSome()) {
    // Parse the flag value.
    // TODO(vinod): Move this parsing logic to flags once we have a
    // 'Rate' abstraction in stout.
    vector<string> tokens =
      strings::tokenize(flags.node_removal_rate_limit.get(), "/");

    if (tokens.size() != 2) {
      return Error(
          "Invalid node_removal_rate_limit: " +
          flags.node_removal_rate_limit.get() +
          ". Format is <Number of nodes>/<Duration>");
    }

    Try<int> permits = numify<int>(tokens[0]);
    if (permits.isError()) {
      return Error(
          "Invalid node_removal_rate_limit: " +
          flags.node_removal_rate_limit.get() +
          ". Format is <Number of nodes>/<Duration>: " + permits.error());
    }

    Try<Duration> duration = Duration::parse(tokens[1]);
    if (duration.isError()) {
      return Error(
          "Invalid node_removal_rate_limit: " +
          flags.node_removal_rate_limit.get() +
          ". Format is <Number of nodes>/<Duration>: " + duration.error());
    }

    controller->nodeRemovalLimiter =
      std::make_shared<process::RateLimiter>(permits.get(), duration.get());
  }

  // Inject all the dependencies.
  controller->controller.reset(new controller::Controller(
      allocator.getOrElse(controller->allocator.get()),
      controller->registrar.get(),
      &controller->files,
      controller->contender.get(),
      controller->detector.get(),
      authorizer.getOrElse(controller->authorizer.get()),
      nodeRemovalLimiter.isSome() ? nodeRemovalLimiter
                                   : controller->nodeRemovalLimiter,
      flags));

  StandaloneControllerDetector* _detector = CHECK_NOTNULL(
      dynamic_cast<StandaloneControllerDetector*>(controller->detector.get()));

  _detector->appoint(controller->controller->info());

  process::Future<Nothing> _recover =
    FUTURE_DISPATCH(controller->controller->self(), &controller::Controller::_recover);

  controller->pid = process::spawn(controller->controller.get());

  // Speed up the tests by ensuring that the Controller is recovered
  // before the test proceeds. Otherwise, authentication and
  // registration messages may be dropped, causing delayed retries.
  // NOTE: We use process::internal::await() to avoid awaiting a
  // Future forever when the Clock is paused.
  if (!process::internal::await(
          _recover,
          flags.registry_fetch_timeout + flags.registry_store_timeout)) {
    return Error("Failed to wait for Controller::_recover");
  }

  bool paused = process::Clock::paused();

  // Need to settle the Clock to ensure that the Controller finishes
  // executing _recover() before we return.
  process::Clock::pause();
  process::Clock::settle();

  // Return the Clock to its original state.
  if (!paused) {
    process::Clock::resume();
  }

  return controller;
}


Controller::~Controller()
{
  // Remove any libprocess authorization callbacks that were installed.
  if (authorizationCallbacksSet) {
    process::http::authorization::unsetCallbacks();
  }

  // NOTE: Authenticators' lifetimes are tied to libprocess's lifetime.
  // This means that multiple controllers in tests are not supported.
  process::http::authentication::unsetAuthenticator(
      controller::READONLY_HTTP_AUTHENTICATION_REALM);
  process::http::authentication::unsetAuthenticator(
      controller::READWRITE_HTTP_AUTHENTICATION_REALM);

  process::terminate(pid);
  process::wait(pid);
}


process::Owned<ControllerDetector> Controller::createDetector()
{
  return process::Owned<ControllerDetector>(new StandaloneControllerDetector(pid));
}


ControllerInfo Controller::getControllerInfo()
{
  return controller->info();
}


void Controller::setAuthorizationCallbacks(Authorizer* authorizer)
{
  process::http::authorization::setCallbacks(
      authorization::createAuthorizationCallbacks(authorizer));

  authorizationCallbacksSet = true;
}


Try<process::Owned<Node>> Node::create(
    ControllerDetector* detector,
    const node::Flags& flags,
    const Option<string>& id,
    const Option<node::Containerizer*>& containerizer,
    const Option<node::GarbageCollector*>& gc,
    const Option<node::TaskStatusUpdateManager*>& taskStatusUpdateManager,
    const Option<medios::node::ResourceEstimator*>& resourceEstimator,
    const Option<medios::node::QoSController*>& qosController,
    const Option<medios::SecretGenerator*>& secretGenerator,
    const Option<Authorizer*>& providedAuthorizer,
    const Option<PendingFutureTracker*>& futureTracker,
    bool mock)
{
  process::Owned<Node> node(new Node());

  // TODO(benh): Create a work directory if using the default.

  node->flags = flags;
  node->detector = detector;

  // If the garbage collector is not provided, create a default one.
  if (gc.isNone()) {
    node->gc.reset(new node::GarbageCollector(flags.work_dir));
  }

  // If the flag `--volume_gid_range` is specified, create a volume gid manager.
  node::VolumeGidManager* volumeGidManager = nullptr;

  if (flags.volume_gid_range.isSome()) {
    Try<node::VolumeGidManager*> _volumeGidManager =
      node::VolumeGidManager::create(flags);

    if (_volumeGidManager.isError()) {
      return Error(
          "Failed to create volume gid manager: " + _volumeGidManager.error());
    }

    volumeGidManager = _volumeGidManager.get();
  }

  // If the future tracker is not provided, create a default one.
  if (futureTracker.isNone()) {
    Try<PendingFutureTracker*> _futureTracker = PendingFutureTracker::create();
    if (_futureTracker.isError()) {
      return Error(
          "Failed to create pending future tracker: " + _futureTracker.error());
    }

    node->futureTracker.reset(_futureTracker.get());
  }

  // If the secret generator is not provided, create a default one.
  if (secretGenerator.isNone()) {
    SecretGenerator* _secretGenerator = nullptr;

#ifdef USE_SSL_SOCKET
    if (flags.jwt_secret_key.isSome()) {
      Try<string> jwtSecretKey = os::read(flags.jwt_secret_key.get());
      if (jwtSecretKey.isError()) {
        return Error("Failed to read the file specified by --jwt_secret_key");
      }

      // TODO(greggomann): Factor the following code out into a common helper,
      // since we also do this when loading credentials.
      Try<os::Permissions> permissions =
        os::permissions(flags.jwt_secret_key.get());
      if (permissions.isError()) {
        LOG(WARNING) << "Failed to stat jwt secret key file '"
                     << flags.jwt_secret_key.get()
                     << "': " << permissions.error();
      } else if (permissions->others.rwx) {
        LOG(WARNING) << "Permissions on executor secret key file '"
                     << flags.jwt_secret_key.get()
                     << "' are too open; it is recommended that your"
                     << " key file is NOT accessible by others";
      }

      _secretGenerator = new JWTSecretGenerator(jwtSecretKey.get());
    }
#endif // USE_SSL_SOCKET

    node->secretGenerator.reset(_secretGenerator);
  }

  const string processId =
    id.isSome() ? id.get() : process::ID::generate("node");

  // If the containerizer is not provided, create a default one.
  if (containerizer.isSome()) {
    node->containerizer = containerizer.get();
  } else {
    Try<node::Containerizer*> _containerizer =
      node::Containerizer::create(
          flags,
          true,
          gc.getOrElse(node->gc.get()),
          nullptr,
          volumeGidManager,
          futureTracker.getOrElse(node->futureTracker.get()));

    if (_containerizer.isError()) {
      return Error("Failed to create containerizer: " + _containerizer.error());
    }

    node->containerizer = _containerizer.get();
  }

  if (containerizer.isNone()) {
    node->ownedContainerizer.reset(node->containerizer);
  }

  Option<Authorizer*> authorizer = providedAuthorizer;

  // If the authorizer is not provided, create a default one.
  if (providedAuthorizer.isNone()) {
    // Indicates whether or not the caller explicitly specified the
    // authorization configuration for this node.
    bool authorizationSpecified = true;

    string authorizerName = flags.authorizer;

    Result<Authorizer*> createdAuthorizer((None()));
    if (authorizerName != node::DEFAULT_AUTHORIZER) {
      LOG(INFO) << "Creating '" << authorizerName << "' authorizer";

      // NOTE: The contents of --acls will be ignored.
      createdAuthorizer = Authorizer::create(authorizerName);
    } else {
      // `authorizerName` is `DEFAULT_AUTHORIZER` at this point.
      if (flags.acls.isSome()) {
        LOG(INFO) << "Creating default '" << authorizerName << "' authorizer";

        createdAuthorizer = Authorizer::create(flags.acls.get());
      } else {
        // Neither a non-default authorizer nor a set of ACLs were specified.
        authorizationSpecified = false;
      }
    }

    if (createdAuthorizer.isError()) {
      EXIT(EXIT_FAILURE) << "Could not create '" << authorizerName
                         << "' authorizer: " << createdAuthorizer.error();
    } else if (createdAuthorizer.isSome()) {
      node->authorizer.reset(createdAuthorizer.get());
      authorizer = createdAuthorizer.get();

      if (authorizationSpecified) {
        // Authorization config was explicitly provided, so set authorization
        // callbacks in libprocess.
        node->setAuthorizationCallbacks(authorizer.get());
      }
    }
  } else {
    // An authorizer was provided, so set authorization callbacks in libprocess.
    node->setAuthorizationCallbacks(providedAuthorizer.get());
  }

  // If the resource estimator is not provided, create a default one.
  if (resourceEstimator.isNone()) {
    Try<medios::node::ResourceEstimator*> _resourceEstimator =
      medios::node::ResourceEstimator::create(flags.resource_estimator);

    if (_resourceEstimator.isError()) {
      return Error(
          "Failed to create resource estimator: " + _resourceEstimator.error());
    }

    node->resourceEstimator.reset(_resourceEstimator.get());
  }

  // If the QoS controller is not provided, create a default one.
  if (qosController.isNone()) {
    Try<medios::node::QoSController*> _qosController =
      medios::node::QoSController::create(flags.qos_controller);

    if (_qosController.isError()) {
      return Error(
          "Failed to create QoS controller: " + _qosController.error());
    }

    node->qosController.reset(_qosController.get());
  }

  Option<Socket> executorSocket = None();
  if (flags.http_executor_domain_sockets) {
    // If `http_executor_domain_sockets` is true, then the location should have
    // been set by the user or automatically during startup.
    CHECK_SOME(flags.domain_socket_location);

    LOG(INFO) << "Creating domain socket at " << *flags.domain_socket_location;
    const int mode = flags.switch_user
      ? common::DOMAIN_SOCKET_SWITCH_USER_MODE
      : common::DOMAIN_SOCKET_DEFAULT_MODE;
    Try<Socket> socket =
      common::createDomainSocket(*flags.domain_socket_location, mode);

    if (socket.isError()) {
      EXIT(EXIT_FAILURE)
        << "Failed to create domain socket: " << socket.error();
    }

    executorSocket = socket.get();
  }

  // If the task status update manager is not provided, create a default one.
  if (taskStatusUpdateManager.isNone()) {
    node->taskStatusUpdateManager.reset(
        new node::TaskStatusUpdateManager(flags));
  }

  // Inject all the dependencies.
  if (mock) {
    node->node.reset(new MockNode(
        processId,
        flags,
        detector,
        node->containerizer,
        &node->files,
        gc.getOrElse(node->gc.get()),
        taskStatusUpdateManager.getOrElse(node->taskStatusUpdateManager.get()),
        resourceEstimator.getOrElse(node->resourceEstimator.get()),
        qosController.getOrElse(node->qosController.get()),
        secretGenerator.getOrElse(node->secretGenerator.get()),
        volumeGidManager,
        futureTracker.getOrElse(node->futureTracker.get()),
        authorizer));
  } else {
    node->node.reset(new node::Node(
        processId,
        flags,
        detector,
        node->containerizer,
        &node->files,
        gc.getOrElse(node->gc.get()),
        taskStatusUpdateManager.getOrElse(node->taskStatusUpdateManager.get()),
        resourceEstimator.getOrElse(node->resourceEstimator.get()),
        qosController.getOrElse(node->qosController.get()),
        secretGenerator.getOrElse(node->secretGenerator.get()),
        volumeGidManager,
        futureTracker.getOrElse(node->futureTracker.get()),
        executorSocket,
        authorizer));
  }

  return node;
}


Node::~Node()
{
  // Remove any libprocess authorization callbacks that were installed.
  if (authorizationCallbacksSet) {
    process::http::authorization::unsetCallbacks();
  }

  process::http::authentication::unsetAuthenticator(
      node::READONLY_HTTP_AUTHENTICATION_REALM);
  process::http::authentication::unsetAuthenticator(
      node::READWRITE_HTTP_AUTHENTICATION_REALM);
  process::http::authentication::unsetAuthenticator(
      node::EXECUTOR_HTTP_AUTHENTICATION_REALM);
  process::http::authentication::unsetAuthenticator(
      node::RESOURCE_PROVIDER_HTTP_AUTHENTICATION_REALM);

  // If either `shutdown()` or `terminate()` were called already,
  // skip the below container cleanup logic.  Additionally, we can skip
  // termination, as the shutdown/terminate will do this too.
  if (!cleanUpContainersInDestructor) {
    return;
  }

  // Startup didn't complete so don't try to do the full shutdown.
  if (!containerizer) {
    return;
  }

  // We should wait until node recovery completes to prevent a potential race
  // between containerizer recovery process and the following code that invokes
  // methods of the containerizer, e.g. a test can start a node that in turn
  // triggers containerizer recovery of orphaned containers, then immediately
  // destroys the node. Thus, the containerizer might return a different set of
  // containers, depending on whether containerizer recovery has been finished.
  //
  // NOTE: This wait is omitted if a pointer to a containerizer object was
  // passed to the node's constructor, as it might be a mock containerizer,
  // thereby node recovery will never be finished.
  if (ownedContainerizer.get() != nullptr) {
    node->recoveryInfo.recovered.future().await();
  }

  terminate();

  // This extra closure is necessary in order to use `AWAIT` and `ASSERT_*`,
  // as these macros require a void return type.
  [this]() {
    // Destroy the existing containers on the node. Note that some
    // containers may terminate while we are doing this, so we ignore
    // any 'wait' failures and ensure that there are no containers
    // when we're done destroying.
    process::Future<hashset<ContainerID>> containers =
      containerizer->containers();

    AWAIT_READY(containers);

    // Because the `cgroups::destroy()` code path makes use of `delay()`, the
    // clock must not be paused in order to reliably destroy all remaining
    // containers. If necessary, we resume the clock here and then pause it
    // again when we're done destroying containers.
    bool paused = process::Clock::paused();

    if (paused) {
      process::Clock::resume();
    }

    foreach (const ContainerID& containerId, containers.get()) {
      process::Future<Option<ContainerTermination>> termination =
        containerizer->destroy(containerId);

      AWAIT(termination);

      if (!termination.isReady()) {
        LOG(ERROR) << "Failed to destroy container " << containerId << ": "
                   << (termination.isFailed() ?
                       termination.failure() :
                       "discarded");
      }
    }

    // When using the composing containerizer, the assertion checking
    // `containers->empty()` below is racy, since the containers are
    // not yet removed from that containerizer's internal data structures
    // when the future becomes ready. Instead, an internal function to clean
    // up these internal data structures is dispatched when the future
    // becomes ready.
    // To work around this, we wait for the clock to settle here. This can
    // be removed once MESOS-9413 is resolved.
    process::Clock::pause();
    process::Clock::settle();

    if (!paused) {
      process::Clock::resume();
    }

    containers = containerizer->containers();
    AWAIT_READY(containers);

    ASSERT_TRUE(containers->empty())
      << "Failed to destroy containers: " << stringify(containers.get());
  }();
}


void Node::setAuthorizationCallbacks(Authorizer* authorizer)
{
  process::http::authorization::setCallbacks(
      authorization::createAuthorizationCallbacks(authorizer));

  authorizationCallbacksSet = true;
}


void Node::shutdown()
{
  cleanUpContainersInDestructor = false;

  bool paused = process::Clock::paused();

  if (paused) {
    process::Clock::resume();
  }

  process::dispatch(node.get(), &node::Node::shutdown, process::UPID(), "");
  wait();

  if (paused) {
    process::Clock::pause();
  }
}


void Node::terminate()
{
  cleanUpContainersInDestructor = false;

  process::terminate(pid);
  wait();
}


MockNode* Node::mock()
{
  return dynamic_cast<MockNode*>(node.get());
}


void Node::start()
{
  pid = process::spawn(node.get());
}


void Node::wait()
{
  process::wait(pid);
}

} // namespace cluster {
} // namespace tests {
} // namespace internal {
} // namespace medios {
