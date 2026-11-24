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
#include <stdlib.h> // For random().

#include <algorithm>
#include <cmath>
#include <deque>
#include <iomanip>
#include <list>
#include <map>
#include <memory>
#include <set>
#include <sstream>
#include <string>
#include <utility>
#include <vector>

#include <glog/logging.h>

#include <medios/type_utils.hpp>

#include <medios/authentication/secret_generator.hpp>

#include <medios/module/authenticatee.hpp>

#include <medios/state/leveldb.hpp>
#include <medios/state/in_memory.hpp>

#include <process/after.hpp>
#include <process/async.hpp>
#include <process/check.hpp>
#include <process/collect.hpp>
#include <process/defer.hpp>
#include <process/delay.hpp>
#include <process/dispatch.hpp>
#include <process/http.hpp>
#include <process/id.hpp>
#include <process/loop.hpp>
#include <process/reap.hpp>
#include <process/time.hpp>

#include <process/ssl/flags.hpp>

#include <stout/bytes.hpp>
#include <stout/check.hpp>
#include <stout/duration.hpp>
#include <stout/exit.hpp>
#include <stout/fs.hpp>
#include <stout/json.hpp>
#include <stout/lambda.hpp>
#include <stout/net.hpp>
#include <stout/numify.hpp>
#include <stout/option.hpp>
#include <stout/os.hpp>
#include <stout/path.hpp>
#include <stout/protobuf.hpp>
#include <stout/stringify.hpp>
#include <stout/strings.hpp>
#include <stout/try.hpp>
#include <stout/utils.hpp>
#include <stout/uuid.hpp>

#include <stout/os/realpath.hpp>

#include "authentication/identity_assertion/identity_assertion_authenticator.hpp"

#include "common/authorization.hpp"
#include "common/openapi_embedded.hpp"
#include "common/protobuf_utils.hpp"
#include "common/resources_utils.hpp"
#include "common/status_utils.hpp"
#include "common/validation.hpp"

#include "credentials/credentials.hpp"

#include "hook/manager.hpp"

#ifdef __linux__
#include "linux/fs.hpp"
#endif // __linux__

#include "logging/logging.hpp"

#include "controller/detector/standalone.hpp"

#include "module/manager.hpp"

#include "node/compatibility.hpp"
#include "node/constants.hpp"
#include "node/flags.hpp"
#include "node/paths.hpp"
#include "node/stores.hpp"
#include "node/node.hpp"
#include "node/state.pb.h"
#include "node/task_status_update_manager.hpp"

// Used to install a handler for POSIX signal.
// http://pubs.opengroup.org/onlinepubs/009695399/functions/sigaction.html
#include <node/posix_signalhandler.hpp>

namespace http = process::http;

using google::protobuf::RepeatedPtrField;

using medios::SecretGenerator;

using medios::authorization::ACCESS_SANDBOX;

using medios::executor::Call;

using medios::controller::detector::ControllerDetector;

using medios::node::ContainerConfig;
using medios::node::ContainerTermination;
using medios::node::QoSController;
using medios::node::QoSCorrection;
using medios::node::ResourceEstimator;

using std::deque;
using std::list;
using std::map;
using std::ostream;
using std::ostringstream;
using std::set;
using std::shared_ptr;
using std::string;
using std::vector;

using process::after;
using process::async;
using process::wait; // Necessary on some OS's to disambiguate.
using process::Break;
using process::Clock;
using process::Continue;
using process::ControlFlow;
using process::Failure;
using process::Future;
using process::Owned;
using process::PID;
using process::Promise;
using process::Time;
using process::UPID;

using process::http::authentication::Principal;

namespace medios {
namespace internal {
namespace node {

using namespace state;

// Forward declarations.

// Needed for logging task/task group.
static string taskOrTaskGroup(
    const Option<TaskInfo>& task,
    const Option<TaskGroupInfo>& taskGroup);


// Returns the command info for default executor.
static CommandInfo defaultExecutorCommandInfo(
    const std::string& launcherDir,
    const Option<std::string>& user);


// Sets the executor resource limit (the `limit` parameter) based on the
// resource passed in (the `value` parameter).
static void setLimit(Option<Value::Scalar>& limit, const Value::Scalar& value);


Node::Node(const string& id,
             const node::Flags& _flags,
             ControllerDetector* _detector,
             Containerizer* _containerizer,
             Files* _files,
             GarbageCollector* _gc,
             TaskStatusUpdateManager* _taskStatusUpdateManager,
             ResourceEstimator* _resourceEstimator,
             QoSController* _qosController,
             SecretGenerator* _secretGenerator,
             VolumeGidManager* _volumeGidManager,
             PendingFutureTracker* _futureTracker,
             const Option<process::network::unix::Socket>& _executorSocket,
             const Option<Authorizer*>& _authorizer)
  : ProcessBase(id),
    state(RECOVERING),
    flags(_flags),
    http(this),
    capabilities(
        _flags.node_features.isNone()
          ? protobuf::node::Capabilities(NODE_CAPABILITIES())
          : protobuf::node::Capabilities(
                _flags.node_features->capabilities())),
    completedFrameworks(MAX_COMPLETED_FRAMEWORKS),
    detector(_detector),
    containerizer(_containerizer),
    files(_files),
    metrics(*this),
    gc(_gc),
    taskStatusUpdateManager(_taskStatusUpdateManager),
    controllerPingTimeout(DEFAULT_CONTROLLER_PING_TIMEOUT()),
    metaDir(paths::getMetaRootDir(flags.work_dir)),
    recoveryErrors(0),
    credential(None()),
    authenticatee(nullptr),
    authenticating(None()),
    authenticated(false),
    reauthenticate(false),
    executorDirectoryMaxAllowedAge(age(0)),
    resourceEstimator(_resourceEstimator),
    qosController(_qosController),
    secretGenerator(_secretGenerator),
    volumeGidManager(_volumeGidManager),
    futureTracker(_futureTracker),
    executorSocket(_executorSocket),
    authorizer(_authorizer),
    resourceVersion(protobuf::createUUID()) {}


Node::~Node()
{
  // TODO(benh): Shut down frameworks?

  // TODO(benh): Shut down executors? The executor should get an "exited"
  // event and initiate a shut down itself.

  Clock::cancel(pingTimer);

  foreachvalue (Framework* framework, frameworks) {
    delete framework;
  }

  delete authenticatee;
}


void Node::signaled(int signal, int uid)
{
  if (signal == SIGUSR1) {
    Result<string> user = os::user(uid);

    shutdown(
        UPID(),
        "Received SIGUSR1 signal" +
        (user.isSome() ? " from user " + user.get() : ""));
  }
}


void Node::initialize()
{
  LOG(INFO) << "Medios node started on " << self().address;
  LOG(INFO) << "Flags at startup: " << flags;

  if (self().address.ip.isLoopback()) {
    LOG(WARNING) << "\n**************************************************\n"
                 << "Node bound to loopback interface!"
                 << " Cannot communicate with remote controller(s)."
                 << " You might want to set '--ip' flag to a routable"
                 << " IP address.\n"
                 << "**************************************************";
  }

  if (flags.registration_backoff_factor > REGISTER_RETRY_INTERVAL_MAX) {
    EXIT(EXIT_FAILURE)
      << "Invalid value '" << flags.registration_backoff_factor << "'"
      << " for --registration_backoff_factor:"
      << " Must be less than " << REGISTER_RETRY_INTERVAL_MAX;
  }

  authenticateeName = flags.authenticatee;

  // Load credential for node authentication with the controller.
  if (flags.credential.isSome()) {
    Result<Credential> _credential =
      credentials::readCredential(flags.credential.get());
    if (_credential.isError()) {
      EXIT(EXIT_FAILURE) << _credential.error() << " (see --credential flag)";
    } else if (_credential.isNone()) {
      EXIT(EXIT_FAILURE)
        << "Empty credential file '" << flags.credential.get() << "'"
        << " (see --credential flag)";
    } else {
      credential = _credential.get();
      LOG(INFO) << "Node using credential for: "
                << credential->principal();
    }
  }

  Option<Credentials> httpCredentials;
  if (flags.http_credentials.isSome()) {
    Result<Credentials> credentials =
      credentials::read(flags.http_credentials.get());
    if (credentials.isError()) {
       EXIT(EXIT_FAILURE)
         << credentials.error() << " (see --http_credentials flag)";
    } else if (credentials.isNone()) {
       EXIT(EXIT_FAILURE)
         << "Credentials file must contain at least one credential"
         << " (see --http_credentials flag)";
    }
    httpCredentials = credentials.get();
  }

  string httpAuthenticators;
  if (flags.http_authenticators.isSome()) {
    httpAuthenticators = flags.http_authenticators.get();
#ifdef USE_SSL_SOCKET
  } else if (flags.authenticate_http_executors) {
    httpAuthenticators =
      string(DEFAULT_BASIC_HTTP_AUTHENTICATOR) + "," +
      string(DEFAULT_JWT_HTTP_AUTHENTICATOR);
#endif // USE_SSL_SOCKET
  } else {
    httpAuthenticators = DEFAULT_BASIC_HTTP_AUTHENTICATOR;
  }

  Option<string> jwtSecretKey;
#ifdef USE_SSL_SOCKET
  if (flags.jwt_secret_key.isSome()) {
    Try<string> jwtSecretKey_ = os::read(flags.jwt_secret_key.get());
    if (jwtSecretKey_.isError()) {
      EXIT(EXIT_FAILURE) << "Failed to read the file specified by "
                         << "--jwt_secret_key";
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

    jwtSecretKey = jwtSecretKey_.get();
  }

  if (flags.authenticate_http_executors) {
    if (flags.jwt_secret_key.isNone()) {
      EXIT(EXIT_FAILURE) << "--jwt_secret_key must be specified when "
                         << "--authenticate_http_executors is set to true";
    }

    Try<Nothing> result = initializeHttpAuthenticators(
        EXECUTOR_HTTP_AUTHENTICATION_REALM,
        strings::split(httpAuthenticators, ","),
        httpCredentials,
        jwtSecretKey);

    if (result.isError()) {
      EXIT(EXIT_FAILURE) << result.error();
    }
  }
#endif // USE_SSL_SOCKET

  if (flags.authenticate_http_readonly) {
    Try<Nothing> result = initializeHttpAuthenticators(
        READONLY_HTTP_AUTHENTICATION_REALM,
        strings::split(httpAuthenticators, ","),
        httpCredentials,
        jwtSecretKey);

    if (result.isError()) {
      EXIT(EXIT_FAILURE) << result.error();
    }
  }

  if (flags.authenticate_http_readwrite) {
    Try<Nothing> result = initializeHttpAuthenticators(
        READWRITE_HTTP_AUTHENTICATION_REALM,
        strings::split(httpAuthenticators, ","),
        httpCredentials,
        jwtSecretKey);

    if (result.isError()) {
      EXIT(EXIT_FAILURE) << result.error();
    }
  }

  if ((flags.gc_disk_headroom < 0) || (flags.gc_disk_headroom > 1)) {
    EXIT(EXIT_FAILURE)
      << "Invalid value '" << flags.gc_disk_headroom << "'"
      << " for --gc_disk_headroom. Must be between 0.0 and 1.0";
  }

  Try<Nothing> initialize =
    resourceEstimator->initialize(defer(self(), &Self::usage));

  if (initialize.isError()) {
    EXIT(EXIT_FAILURE)
      << "Failed to initialize the resource estimator: " << initialize.error();
  }

  initialize = qosController->initialize(defer(self(), &Self::usage));

  if (initialize.isError()) {
    EXIT(EXIT_FAILURE)
      << "Failed to initialize the QoS Controller: " << initialize.error();
  }

  // Ensure node work directory exists.
  Try<Nothing> mkdir = os::mkdir(flags.work_dir);
  if (mkdir.isError()) {
    EXIT(EXIT_FAILURE)
      << "Failed to create node work directory '" << flags.work_dir << "': "
      << mkdir.error();
  }

  Try<Resources> resources = Containerizer::resources(flags);
  if (resources.isError()) {
    EXIT(EXIT_FAILURE)
      << "Failed to determine node resources: " << resources.error();
  }

  // Stores (DR-036). --resources may not carry a store: the declaration
  // lives in the stores file, where the counts and the path are checked.
  foreach (const Resource& resource, resources.get()) {
    if (storeNameOf(resource.name()).isSome()) {
      EXIT(EXIT_FAILURE)
        << "--resources names '" << resource.name() << "'; a store is "
        << "declared in the stores file (--stores), not in --resources";
    }
  }

  if (flags.stores.isSome()) {
    Try<string> contents = os::read(flags.stores.get());
    if (contents.isError()) {
      EXIT(EXIT_FAILURE)
        << "Failed to read the stores file '" << flags.stores.get()
        << "': " << contents.error();
    }

    Try<vector<StoreDeclaration>> stores = parseStores(contents.get());
    if (stores.isError()) {
      EXIT(EXIT_FAILURE)
        << "The stores file '" << flags.stores.get() << "' is refused: "
        << stores.error();
    }

    Try<Nothing> checked = checkStores(stores.get());
    if (checked.isError()) {
      EXIT(EXIT_FAILURE) << checked.error();
    }

    // A node that declares stores must be able to deliver one, or every
    // task placed on it for a store ends refused at launch.
    Try<Nothing> delivery = checkStoreDelivery(
        flags.containerizers, flags.launcher, flags.isolation);
    if (delivery.isError()) {
      EXIT(EXIT_FAILURE)
        << "The stores file '" << flags.stores.get() << "' declares "
        << stores->size() << " store(s), and " << delivery.error();
    }

    resources =
      resources.get() + storeResources(stores.get(), flags.default_role);

    foreach (const StoreDeclaration& store, stores.get()) {
      LOG(INFO) << "Declared store '" << store.name << "' at '"
                << store.path << "' (" << store.storeClass << ", "
                << store.locality << ", readers " << store.readers
                << ", writers " << store.writers << ")";
    }

    // A store is delivered by this node, so this node can read it: each
    // one is served read-only through the files API at /stores/<name>,
    // the way a sandbox is, so a framework can read what a task left in
    // a store from the node that ran the task, without a host path
    // (DR-042, medios#585). The files API resolves every path inside
    // the attached directory and refuses one that escapes it.
    Option<Authorizer*> _authorizer = authorizer;

    foreach (const StoreDeclaration& store, stores.get()) {
      const string virtualPath = path::join(STORES_VIRTUAL_PATH, store.name);

      auto authorizeStore =
        [_authorizer, virtualPath](
            const Option<Principal>& principal) -> Future<bool> {
          if (_authorizer.isNone()) {
            return true;
          }

          authorization::Request request;
          request.set_action(authorization::GET_ENDPOINT_WITH_PATH);

          Option<authorization::Subject> subject =
            authorization::createSubject(principal);
          if (subject.isSome()) {
            request.mutable_subject()->CopyFrom(subject.get());
          }

          request.mutable_object()->set_value(virtualPath);
          return _authorizer.get()->authorized(request);
        };

      // Refused, not logged: a node that cannot read a store it declares
      // would advertise it and answer 404, with the premise broken.
      files->attach(store.path, virtualPath, authorizeStore)
        .onAny(defer(
            self(),
            [=](const Future<Nothing>& attached) {
              if (!attached.isReady()) {
                EXIT(EXIT_FAILURE)
                  << "Store '" << store.name << "' at '" << store.path
                  << "' cannot be served through the files API at '"
                  << virtualPath << "': "
                  << (attached.isFailed() ? attached.failure() : "discarded");
              }

              VLOG(1) << "Serving store '" << store.name << "' at '"
                      << virtualPath << "'";
            }));
    }
  }

  // Ensure disk `source`s are accessible.
  foreach (
      const Resource& resource,
      resources->filter([](const Resource& _resource) {
        return _resource.has_disk() && _resource.disk().has_source();
      })) {
    const Resource::DiskInfo::Source& source = resource.disk().source();
    switch (source.type()) {
      case Resource::DiskInfo::Source::PATH: {
        // For `PATH` sources we create them if they do not exist.
        CHECK(source.has_path());

        if (!source.path().has_root()) {
          EXIT(EXIT_FAILURE)
            << "PATH disk root directory is not specified "
            << "'" << resource << "'";
        }

        Try<Nothing> mkdir = os::mkdir(source.path().root(), true);

        if (mkdir.isError()) {
          EXIT(EXIT_FAILURE)
            << "Failed to create DiskInfo path directory "
            << "'" << source.path().root() << "': " << mkdir.error();
        }
        break;
      }
      case Resource::DiskInfo::Source::MOUNT: {
        CHECK(source.has_mount());

        if (!source.mount().has_root()) {
          EXIT(EXIT_FAILURE)
            << "MOUNT disk root directory is not specified "
            << "'" << resource << "'";
        }

        // For `MOUNT` sources we fail if they don't exist.
        // On Linux we test the mount table for existence.
#ifdef __linux__
        // Get the `realpath` of the `root` to verify it against the
        // mount table entries.
        // TODO(jmlvanre): Consider enforcing allowing only real paths
        // as opposed to symlinks. This would prevent the ability for
        // an operator to change the underlying data while the node
        // checkpointed `root` had the same value. We could also check
        // the UUID of the underlying block device to catch this case.
        Result<string> realpath = os::realpath(source.mount().root());

        if (!realpath.isSome()) {
          EXIT(EXIT_FAILURE)
            << "Failed to determine `realpath` for DiskInfo mount in resource '"
            << resource << "' with path '" << source.mount().root() << "': "
            << (realpath.isError() ? realpath.error() : "no such path");
        }

        // TODO(jmlvanre): Consider moving this out of the for loop.
        Try<fs::MountTable> mountTable = fs::MountTable::read("/proc/mounts");
        if (mountTable.isError()) {
          EXIT(EXIT_FAILURE)
            << "Failed to open mount table to verify mounts: "
            << mountTable.error();
        }

        bool foundEntry = false;
        foreach (const fs::MountTable::Entry& entry, mountTable->entries) {
          if (entry.dir == realpath.get()) {
            foundEntry = true;
            break;
          }
        }

        if (!foundEntry) {
          EXIT(EXIT_FAILURE)
            << "Failed to find mount '" << realpath.get()
            << "' in /proc/mounts";
        }
#else // __linux__
        // On other platforms we test whether that provided `root` exists.
        if (!os::exists(source.mount().root())) {
          EXIT(EXIT_FAILURE)
            << "Failed to find mount point '" << source.mount().root() << "'";
        }
#endif // __linux__
        break;
      }
      case Resource::DiskInfo::Source::BLOCK:
      case Resource::DiskInfo::Source::RAW:
      case Resource::DiskInfo::Source::UNKNOWN: {
        EXIT(EXIT_FAILURE)
          << "Unsupported 'DiskInfo.Source.Type' in '" << resource << "'";
      }
    }
  }

  Attributes attributes;
  if (flags.attributes.isSome()) {
    attributes = Attributes::parse(flags.attributes.get());
  }

  // Determine our hostname or use the hostname provided.
  string hostname;

  if (flags.hostname.isNone()) {
    if (flags.hostname_lookup) {
      Try<string> result = net::getHostname(self().address.ip);

      if (result.isError()) {
        EXIT(EXIT_FAILURE) << "Failed to get hostname: " << result.error();
      }

      hostname = result.get();
    } else {
      // We use the IP address for hostname if the user requested us
      // NOT to look it up, and it wasn't explicitly set via --hostname:
      hostname = stringify(self().address.ip);
    }
  } else {
    hostname = flags.hostname.get();
  }

  // Initialize node info.
  info.set_hostname(hostname);
  info.set_port(self().address.port);

  info.mutable_resources()->CopyFrom(resources.get());
  if (HookManager::hooksAvailable()) {
    info.mutable_resources()->CopyFrom(
        HookManager::nodeResourcesDecorator(info));
  }

  // Initialize `totalResources` with `info.resources`, checkpointed
  // resources will be applied later during recovery.
  totalResources = info.resources();

  LOG(INFO) << "Node resources: " << info.resources();

  info.mutable_attributes()->CopyFrom(attributes);
  if (HookManager::hooksAvailable()) {
    info.mutable_attributes()->CopyFrom(
        HookManager::nodeAttributesDecorator(info));
  }

  LOG(INFO) << "Node attributes: " << info.attributes();

  // Checkpointing of nodes is always enabled.
  info.set_checkpoint(true);

  if (flags.domain.isSome()) {
    info.mutable_domain()->CopyFrom(flags.domain.get());
  }

  LOG(INFO) << "Node hostname: " << info.hostname();

  taskStatusUpdateManager->initialize(defer(self(), &Node::forward, lambda::_1)
    .operator std::function<void(StatusUpdate)>());

  // We pause the status update managers so that they don't forward any updates
  // while the node is still recovering. They are unpaused/resumed when the
  // node (re-)registers with the controller.
  taskStatusUpdateManager->pause();
  operationStatusUpdateManager.pause();

  // Start disk monitoring.
  // NOTE: We send a delayed message here instead of directly calling
  // checkDiskUsage, to make disabling this feature easy (e.g by specifying
  // a very large disk_watch_interval).
  delay(flags.disk_watch_interval, self(), &Node::checkDiskUsage);

  // Start image store disk monitoring. Please note that image layers
  // garbage collection is only enabled if the node flag `--image_gc_config`
  // is set.
  // TODO(gilbert): Consider move the image auto GC logic to containerizers
  // respectively. For now, it is only enabled for the Medios Containerizer.
  if (flags.image_gc_config.isSome() &&
      flags.image_providers.isSome() &&
      strings::contains(flags.containerizers, "medios")) {
    delay(
        Nanoseconds(
            flags.image_gc_config->image_disk_watch_interval().nanoseconds()),
        self(),
        &Node::checkImageDiskUsage);
  }

  startTime = Clock::now();

  // Install protobuf handlers.
  install<NodeRegisteredMessage>(
      &Node::registered,
      &NodeRegisteredMessage::node_id,
      &NodeRegisteredMessage::connection);

  install<NodeReregisteredMessage>(
      &Node::reregistered,
      &NodeReregisteredMessage::node_id,
      &NodeReregisteredMessage::reconciliations,
      &NodeReregisteredMessage::connection);

  install<RunTaskMessage>(
      &Node::handleRunTaskMessage);

  install<RunTaskGroupMessage>(
      &Node::handleRunTaskGroupMessage);

  install<KillTaskMessage>(
      &Node::killTask);

  install<ShutdownExecutorMessage>(
      &Node::shutdownExecutor,
      &ShutdownExecutorMessage::framework_id,
      &ShutdownExecutorMessage::executor_id);

  install<ShutdownFrameworkMessage>(
      &Node::shutdownFramework,
      &ShutdownFrameworkMessage::framework_id);

  install<FrameworkToExecutorMessage>(
      &Node::schedulerMessage,
      &FrameworkToExecutorMessage::node_id,
      &FrameworkToExecutorMessage::framework_id,
      &FrameworkToExecutorMessage::executor_id,
      &FrameworkToExecutorMessage::data);

  install<UpdateFrameworkMessage>(
      &Node::updateFramework);

  install<CheckpointResourcesMessage>(
      &Node::checkpointResourcesMessage,
      &CheckpointResourcesMessage::resources);

  install<ApplyOperationMessage>(
      &Node::applyOperation);

  install<ReconcileOperationsMessage>(
      &Node::reconcileOperations);

  install<StatusUpdateAcknowledgementMessage>(
      &Node::statusUpdateAcknowledgement,
      &StatusUpdateAcknowledgementMessage::node_id,
      &StatusUpdateAcknowledgementMessage::framework_id,
      &StatusUpdateAcknowledgementMessage::task_id,
      &StatusUpdateAcknowledgementMessage::uuid);

  install<AcknowledgeOperationStatusMessage>(
      &Node::operationStatusAcknowledgement);

  install<RegisterExecutorMessage>(
      &Node::registerExecutor,
      &RegisterExecutorMessage::framework_id,
      &RegisterExecutorMessage::executor_id);

  install<ReregisterExecutorMessage>(
      &Node::reregisterExecutor,
      &ReregisterExecutorMessage::framework_id,
      &ReregisterExecutorMessage::executor_id,
      &ReregisterExecutorMessage::tasks,
      &ReregisterExecutorMessage::updates);

  install<StatusUpdateMessage>(
      &Node::statusUpdate,
      &StatusUpdateMessage::update,
      &StatusUpdateMessage::pid);

  install<ExecutorToFrameworkMessage>(
      &Node::executorMessage,
      &ExecutorToFrameworkMessage::node_id,
      &ExecutorToFrameworkMessage::framework_id,
      &ExecutorToFrameworkMessage::executor_id,
      &ExecutorToFrameworkMessage::data);

  install<ShutdownMessage>(
      &Node::shutdown,
      &ShutdownMessage::message);

  install<DrainNodeMessage>(&Node::drain);

  install<PingNodeMessage>(
      &Node::ping,
      &PingNodeMessage::connected);

  // Setup the '/api/v1' handler for streaming requests.
  RouteOptions options;
  options.requestStreaming = true;
  route("/api/v1",
        // TODO(benh): Is this authentication realm sufficient or do
        // we need some kind of hybrid if we expect both executors
        // and operators/tooling to use this endpoint?
        READWRITE_HTTP_AUTHENTICATION_REALM,
        Http::API_HELP(),
        [this](const http::Request& request,
               const Option<Principal>& principal) {
          logRequest(request);
          return http.api(request, principal);
        },
        options);

  if (executorSocket.isSome()) {
    // We use `http::Server` to manage the communication channel.
    // Since `http::Server` currently doesn't offer support for
    // authentication we then inject the request received by the
    // server into normal node rounting logic.
    Try<http::Server> server = http::Server::create(
        *executorSocket,
        process::defer(
            self(),
            [this](const process::network::Socket&, http::Request request)
              -> Future<http::Response> {
              // Restrict access to only allow `/node(N)/api/v1/executor`
              // and `/node(N)/api/v1`. Executors need to be able to
              // access the first to subscribe and the latter to e.g.,
              // launch containers or perform other operator API calls.
              string selfPrefix = "/" + self().id;
              if (request.url.path != selfPrefix + "/api/v1/executor" &&
                  request.url.path != selfPrefix + "/api/v1") {
                LOG(INFO)
                  << "Blocking request for " << request.url.path
                  << " over executor socket";
                return http::Forbidden();
              }

              // Create an `HttpEvent` with the needed information which we can
              // be consumed by the node. The event contains e.g., the
              // requested path so the expected route `/api/v1/executor` is
              // routed when consuming the event.
              std::unique_ptr<Promise<http::Response>> promise(
                  new Promise<http::Response>());

              Future<http::Response> response = promise->future();

              process::HttpEvent event(
                  std::unique_ptr<http::Request>(new http::Request(request)),
                  std::move(promise));

              std::move(event).consume(this);

              return response;
            }),
        {
          /* .scheme =*/process::http::Scheme::HTTP_UNIX,
          /* .backlog =*/16384,
        });

    if (server.isError()) {
      LOG(FATAL) << "Could not start listening on executor socket: "
                 << server.error();
    } else {
      executorSocketServer = std::move(*server);

      Future<Nothing> executorSocketServerTerminated =
        executorSocketServer->run();

      if (executorSocketServerTerminated.isFailed()) {
        LOG(FATAL) << "Could not start listening on executor socket: "
                   << executorSocketServerTerminated.failure();
      }
    }
  }

  route("/api/v1/executor",
        EXECUTOR_HTTP_AUTHENTICATION_REALM,
        Http::EXECUTOR_HELP(),
        [this](const http::Request& request,
               const Option<Principal>& principal) {
          logRequest(request);
          return http.executor(request, principal);
        });
  route(
      "/api/v1/resource_provider",
      RESOURCE_PROVIDER_HTTP_AUTHENTICATION_REALM,
      Http::RESOURCE_PROVIDER_HELP(),
      [this](const http::Request& request, const Option<Principal>& principal)
        -> Future<http::Response> {
        logRequest(request);

        if (resourceProviderManager.get() == nullptr) {
          return http::ServiceUnavailable();
        }

        return resourceProviderManager->api(request, principal);
      });
  route("/state",
        READONLY_HTTP_AUTHENTICATION_REALM,
        Http::STATE_HELP(),
        [this](const http::Request& request,
               const Option<Principal>& principal) {
          logRequest(request);
          return http.state(request, principal)
            .onReady([request](const process::http::Response& response) {
              logResponse(request, response);
            });
        });
  route("/flags",
        READONLY_HTTP_AUTHENTICATION_REALM,
        Http::FLAGS_HELP(),
        [this](const http::Request& request,
               const Option<Principal>& principal) {
          logRequest(request);
          return http.flags(request, principal);
        });
  route("/health",
        Http::HEALTH_HELP(),
        [this](const http::Request& request) {
          return http.health(request);
        });
  {
    // OpenAPI 3.x specification discovery (#261). Served unauthenticated
    // because the spec is public surface, the same as /help.
    auto serveOpenapi = [](const http::Request&)
        -> Future<http::Response> {
      http::Response response = http::OK(medios::openapi::nodeSpec);
      response.headers["Content-Type"] = "application/openapi+json";
      return response;
    };
    route("/openapi.json", Http::OPENAPI_HELP(), serveOpenapi);
    route("/.well-known/openapi.json", Http::OPENAPI_HELP(), serveOpenapi);
  }
  route("/monitor/statistics",
        READONLY_HTTP_AUTHENTICATION_REALM,
        Http::STATISTICS_HELP(),
        [this](const http::Request& request,
               const Option<Principal>& principal) {
          logRequest(request);
          return http.statistics(request, principal);
        });
  route("/containers",
        READONLY_HTTP_AUTHENTICATION_REALM,
        Http::CONTAINERS_HELP(),
        [this](const http::Request& request,
               const Option<Principal>& principal) {
          logRequest(request);
          return http.containers(request, principal)
            .onReady([request](const process::http::Response& response) {
              logResponse(request, response);
            });
        });
  route("/containerizer/debug",
        READONLY_HTTP_AUTHENTICATION_REALM,
        Http::CONTAINERIZER_DEBUG_HELP(),
        [this](const http::Request& request,
               const Option<Principal>& principal) {
          logRequest(request);
          return http.containerizerDebug(request, principal);
        });

  // TODO(tillt): Use generalized lambda capture once we adopt C++14.
  Option<Authorizer*> _authorizer = authorizer;

  auto authorize = [_authorizer](const Option<Principal>& principal) {
    return authorization::authorizeLogAccess(_authorizer, principal);
  };

  // Expose the log file for the webui. Fall back to 'log_dir' if
  // an explicit file was not specified.
  if (flags.external_log_file.isSome()) {
    files->attach(
        flags.external_log_file.get(), NODE_LOG_VIRTUAL_PATH, authorize)
      .onAny(defer(self(),
                   &Self::fileAttached,
                   lambda::_1,
                   flags.external_log_file.get(),
                   NODE_LOG_VIRTUAL_PATH));
  } else if (flags.log_dir.isSome()) {
    Try<string> log =
      logging::getLogFile(logging::getLogSeverity(flags.logging_level));

    if (log.isError()) {
      LOG(ERROR) << "Node log file cannot be found: " << log.error();
    } else {
      files->attach(log.get(), NODE_LOG_VIRTUAL_PATH, authorize)
        .onAny(defer(self(),
                     &Self::fileAttached,
                     lambda::_1,
                     log.get(),
                     NODE_LOG_VIRTUAL_PATH));
    }
  }

  // Check that the reconfiguration_policy flag is valid.
  if (flags.reconfiguration_policy != "equal" &&
      flags.reconfiguration_policy != "additive") {
    EXIT(EXIT_FAILURE)
      << "Unknown option for 'reconfiguration_policy' flag "
      << flags.reconfiguration_policy << "."
      << " Please run the node with '--help' to see the valid options.";
  }

  // Check that the recover flag is valid.
  if (flags.recover != "reconnect" && flags.recover != "cleanup") {
    EXIT(EXIT_FAILURE)
      << "Unknown option for 'recover' flag " << flags.recover << "."
      << " Please run the node with '--help' to see the valid options";
  }

  auto signalHandler = defer(self(), &Node::signaled, lambda::_1, lambda::_2)
    .operator std::function<void(int, int)>();

  if (os::internal::configureSignal(&signalHandler) < 0) {
    EXIT(EXIT_FAILURE)
      << "Failed to configure signal handlers: " << os::strerror(errno);
  }

  // Do recovery.
  async(&state::recover, metaDir, flags.strict)
    .then(defer(self(), &Node::recover, lambda::_1))
    .then(defer(self(), &Node::_recover))
    .onAny(defer(self(), &Node::__recover, lambda::_1));
}


void Node::finalize()
{
  LOG(INFO) << "Node terminating";

  // NOTE: We use 'frameworks.keys()' here because 'shutdownFramework'
  // can potentially remove a framework from 'frameworks'.
  foreach (const FrameworkID& frameworkId, frameworks.keys()) {
    // TODO(benh): Because a shut down isn't instantaneous (but has
    // a shut down/kill phases) we might not actually propagate all
    // the status updates appropriately here. Consider providing
    // an alternative function which skips the shut down phase and
    // simply does a kill (sending all status updates
    // immediately). Of course, this still isn't sufficient
    // because those status updates might get lost and we won't
    // resend them unless we build that into the system.
    // NOTE: We shut down the framework only if it has disabled
    // checkpointing. This is because node recovery tests terminate
    // the node to simulate node restart.
    if (!frameworks[frameworkId]->info.checkpoint()) {
      shutdownFramework(UPID(), frameworkId);
    }
  }

  // Explicitly tear down the resource provider manager to ensure that the
  // wrapped process is terminated and releases the underlying storage.
  resourceProviderManager.reset();
}


void Node::shutdown(const UPID& from, const string& message)
{
  if (from && controller != from) {
    LOG(WARNING) << "Ignoring shutdown message from " << from
                 << " because it is not from the registered controller: "
                 << (controller.isSome() ? stringify(controller.get()) : "None");
    return;
  }

  if (from) {
    LOG(INFO) << "Node asked to shut down by " << from
              << (message.empty() ? "" : " because '" + message + "'");
  } else if (info.has_id()) {
    if (message.empty()) {
      LOG(INFO) << "Unregistering and shutting down";
    } else {
      LOG(INFO) << message << "; unregistering and shutting down";
    }

    UnregisterNodeMessage message_;
    message_.mutable_node_id()->MergeFrom(info.id());
    send(controller.get(), message_);
  } else {
    if (message.empty()) {
      LOG(INFO) << "Shutting down";
    } else {
      LOG(INFO) << message << "; shutting down";
    }
  }

  state = TERMINATING;

  if (frameworks.empty()) { // Terminate node if there are no frameworks.
    terminate(self());
  } else {
    // NOTE: The node will terminate after all the executors have
    // terminated.
    // NOTE: We use 'frameworks.keys()' here because 'shutdownFramework'
    // can potentially remove a framework from 'frameworks'.
    foreach (const FrameworkID& frameworkId, frameworks.keys()) {
      shutdownFramework(from, frameworkId);
    }
  }
}


void Node::drain(
    const UPID& from,
    DrainNodeMessage&& drainNodeMessage)
{
  if (operations.empty() && frameworks.empty()) {
    LOG(INFO)
      << "Received DrainConfig " << drainNodeMessage.config()
      << (drainConfig.isSome()
          ? "; previously stored DrainConfig " + stringify(*drainConfig)
          : "")
      << "; node has no stored frameworks, tasks, or operations,"
         " so draining is already complete";

    return;
  }

  hashmap<FrameworkID, hashset<TaskID>> pendingTaskIds;
  foreachvalue (Framework* framework, frameworks) {
    foreachvalue (const auto& taskMap, framework->pendingTasks) {
      pendingTaskIds[framework->id()] = taskMap.keys();
    }
  }

  hashmap<FrameworkID, hashset<TaskID>> queuedTaskIds;
  foreachvalue (Framework* framework, frameworks) {
    foreachvalue (Executor* executor, framework->executors) {
      foreachkey (const TaskID& taskId, executor->queuedTasks) {
        queuedTaskIds[framework->id()].insert(taskId);
      }
    }
  }

  hashmap<FrameworkID, hashset<TaskID>> launchedTaskIds;
  foreachvalue (Framework* framework, frameworks) {
    foreachvalue (Executor* executor, framework->executors) {
      foreachkey (const TaskID& taskId, executor->launchedTasks) {
        launchedTaskIds[framework->id()].insert(taskId);
      }
    }
  }

  LOG(INFO)
    << "Initiating drain with DrainConfig " << drainNodeMessage.config()
    << (drainConfig.isSome()
        ? "; overwriting previous DrainConfig " + stringify(*drainConfig)
        : "")
    << "; node has (pending tasks, queued tasks, launched tasks, operations)"
    << " == ("
    << stringify(pendingTaskIds) << ", "
    << stringify(queuedTaskIds) << ", "
    << stringify(launchedTaskIds) << ", "
    << stringify(operations.keys()) << ")";

  CHECK_SOME(state::checkpoint(
      paths::getDrainConfigPath(metaDir, info.id()),
      drainNodeMessage.config()))
    << "Failed to checkpoint DrainConfig";

  drainConfig = drainNodeMessage.config();
  estimatedDrainStartTime = Clock::now();

  const Option<DurationInfo> maxGracePeriod =
    drainConfig->has_max_grace_period()
      ? drainConfig->max_grace_period()
      : Option<DurationInfo>::none();

  auto calculateKillPolicy =
    [&](const Option<KillPolicy>& killPolicy) -> Option<KillPolicy> {
      if (maxGracePeriod.isNone()) {
        return None();
      }

      KillPolicy killPolicyOverride;
      killPolicyOverride.mutable_grace_period()->CopyFrom(maxGracePeriod.get());

      // Task kill policy is not set or unknown.
      if (killPolicy.isNone() || !killPolicy->has_grace_period()) {
        return killPolicyOverride;
      }

      // Task kill policy is greater than the override.
      if (maxGracePeriod.get() < killPolicy->grace_period()) {
        return killPolicyOverride;
      }

      return None();
    };

  // Frameworks may be removed within `kill()` or `killPendingTask()` below,
  // so we must copy them and their members before looping.
  foreachvalue (Framework* framework, utils::copy(frameworks)) {
    typedef hashmap<TaskID, TaskInfo> TaskMap;
    foreachvalue (const TaskMap& tasks, utils::copy(framework->pendingTasks)) {
      foreachvalue (const TaskInfo& task, tasks) {
        killPendingTask(framework->id(), framework, task.task_id());
      }
    }

    foreachvalue (Executor* executor, utils::copy(framework->executors)) {
      foreachvalue (Task* task, executor->launchedTasks) {
        kill(framework->id(),
             framework,
             executor,
             task->task_id(),
             calculateKillPolicy(
                task->has_kill_policy()
                  ? task->kill_policy()
                  : Option<KillPolicy>::none()));
      }

      foreachvalue (const TaskInfo& task, utils::copy(executor->queuedTasks)) {
        kill(framework->id(),
             framework,
             executor,
             task.task_id(),
             calculateKillPolicy(
                task.has_kill_policy()
                  ? task.kill_policy()
                  : Option<KillPolicy>::none()));
      }
    }
  }
}


void Node::fileAttached(
    const Future<Nothing>& result,
    const string& path,
    const string& virtualPath)
{
  if (result.isReady()) {
    VLOG(1) << "Successfully attached '" << path << "'"
            << " to virtual path '" << virtualPath << "'";
  } else {
    LOG(ERROR) << "Failed to attach '" << path << "'"
               << " to virtual path '" << virtualPath << "': "
               << (result.isFailed() ? result.failure() : "discarded");
  }
}


// TODO(vinod/bmahler): Get rid of this helper.
Nothing Node::detachFile(const string& path)
{
  files->detach(path);
  return Nothing();
}


void Node::attachTaskVolumeDirectory(
    const ExecutorInfo& executorInfo,
    const ContainerID& executorContainerId,
    const Task& task)
{
  CHECK(executorInfo.has_type() &&
        executorInfo.type() == ExecutorInfo::DEFAULT);

  CHECK_EQ(task.executor_id(), executorInfo.executor_id());

  const PID<Node> nodePid = self();
  const FrameworkID frameworkId = task.framework_id();
  const ExecutorID executorId = task.executor_id();

  auto authorize =
    [nodePid, frameworkId, executorId](const Option<Principal>& principal) {
      return dispatch(
          nodePid,
          &Node::authorizeSandboxAccess,
          principal,
          frameworkId,
          executorId);
    };

  // This is the case that the task has disk resources specified.
  foreach (const Resource& resource, task.resources()) {
    // Ignore if there are no disk resources or if the
    // disk resources did not specify a volume mapping.
    if (!resource.has_disk() || !resource.disk().has_volume()) {
      continue;
    }

    const Volume& volume = resource.disk().volume();

    const string executorRunPath = paths::getExecutorRunPath(
        flags.work_dir,
        info.id(),
        task.framework_id(),
        task.executor_id(),
        executorContainerId);

    const string executorDirectoryPath =
      path::join(executorRunPath, volume.container_path());

    const string taskPath = paths::getTaskPath(
        flags.work_dir,
        info.id(),
        task.framework_id(),
        task.executor_id(),
        executorContainerId,
        task.task_id());

    const string taskDirectoryPath =
      path::join(taskPath, volume.container_path());

    files->attach(executorDirectoryPath, taskDirectoryPath, authorize)
      .onAny(defer(
          self(),
          &Self::fileAttached,
          lambda::_1,
          executorDirectoryPath,
          taskDirectoryPath));
  }

  // This is the case that the executor has disk resources specified
  // and the task's ContainerInfo has a `SANDBOX_PATH` volume with type
  // `PARENT` to share the executor's disk volume.
  hashset<string> executorContainerPaths;
  foreach (const Resource& resource, executorInfo.resources()) {
    // Ignore if there are no disk resources or if the
    // disk resources did not specify a volume mapping.
    if (!resource.has_disk() || !resource.disk().has_volume()) {
      continue;
    }

    const Volume& volume = resource.disk().volume();
    executorContainerPaths.insert(volume.container_path());
  }

  if (executorContainerPaths.empty()) {
    return;
  }

  if (task.has_container()) {
    foreach (const Volume& volume, task.container().volumes()) {
      if (!volume.has_source() ||
          volume.source().type() != Volume::Source::SANDBOX_PATH) {
        continue;
      }

      CHECK(volume.source().has_sandbox_path());

      const Volume::Source::SandboxPath& sandboxPath =
        volume.source().sandbox_path();

      if (sandboxPath.type() != Volume::Source::SandboxPath::PARENT) {
        continue;
      }

      if (!executorContainerPaths.contains(sandboxPath.path())) {
        continue;
      }

      const string executorRunPath = paths::getExecutorRunPath(
          flags.work_dir,
          info.id(),
          task.framework_id(),
          task.executor_id(),
          executorContainerId);

      const string executorDirectoryPath =
        path::join(executorRunPath, sandboxPath.path());

      const string taskPath = paths::getTaskPath(
          flags.work_dir,
          info.id(),
          task.framework_id(),
          task.executor_id(),
          executorContainerId,
          task.task_id());

      const string taskDirectoryPath =
        path::join(taskPath, volume.container_path());

      files->attach(executorDirectoryPath, taskDirectoryPath, authorize)
        .onAny(defer(
            self(),
            &Self::fileAttached,
            lambda::_1,
            executorDirectoryPath,
            taskDirectoryPath));
    }
  }
}


void Node::detachTaskVolumeDirectories(
    const ExecutorInfo& executorInfo,
    const ContainerID& executorContainerId,
    const vector<Task>& tasks)
{
  // NOTE: If the executor is not a default executor, this function will
  // still be called but with an empty list of tasks.
  CHECK(tasks.empty() ||
        (executorInfo.has_type() &&
         executorInfo.type() == ExecutorInfo::DEFAULT));

  hashset<string> executorContainerPaths;
  foreach (const Resource& resource, executorInfo.resources()) {
    // Ignore if there are no disk resources or if the
    // disk resources did not specify a volume mapping.
    if (!resource.has_disk() || !resource.disk().has_volume()) {
      continue;
    }

    const Volume& volume = resource.disk().volume();
    executorContainerPaths.insert(volume.container_path());
  }

  foreach (const Task& task, tasks) {
    CHECK_EQ(task.executor_id(), executorInfo.executor_id());

    // This is the case that the task has disk resources specified.
    foreach (const Resource& resource, task.resources()) {
      // Ignore if there are no disk resources or if the
      // disk resources did not specify a volume mapping.
      if (!resource.has_disk() || !resource.disk().has_volume()) {
        continue;
      }

      const Volume& volume = resource.disk().volume();

      const string taskPath = paths::getTaskPath(
          flags.work_dir,
          info.id(),
          task.framework_id(),
          task.executor_id(),
          executorContainerId,
          task.task_id());

      const string taskDirectoryPath =
        path::join(taskPath, volume.container_path());

      files->detach(taskDirectoryPath);
    }

    if (executorContainerPaths.empty()) {
      continue;
    }

    // This is the case that the executor has disk resources specified
    // and the task's ContainerInfo has a `SANDBOX_PATH` volume with type
    // `PARENT` to share the executor's disk volume.
    if (task.has_container()) {
      foreach (const Volume& volume, task.container().volumes()) {
        if (!volume.has_source() ||
            volume.source().type() != Volume::Source::SANDBOX_PATH) {
          continue;
        }

        CHECK(volume.source().has_sandbox_path());

        const Volume::Source::SandboxPath& sandboxPath =
          volume.source().sandbox_path();

        if (sandboxPath.type() != Volume::Source::SandboxPath::PARENT) {
          continue;
        }

        if (!executorContainerPaths.contains(sandboxPath.path())) {
          continue;
        }

        const string taskPath = paths::getTaskPath(
            flags.work_dir,
            info.id(),
            task.framework_id(),
            task.executor_id(),
            executorContainerId,
            task.task_id());

        const string taskDirectoryPath =
          path::join(taskPath, volume.container_path());

        files->detach(taskDirectoryPath);
      }
    }
  }
}


void Node::detected(const Future<Option<ControllerInfo>>& _controller)
{
  CHECK(state == DISCONNECTED ||
        state == RUNNING ||
        state == TERMINATING) << state;

  if (state != TERMINATING) {
    state = DISCONNECTED;
  }

  // Pause the status updates.
  taskStatusUpdateManager->pause();
  operationStatusUpdateManager.pause();

  if (_controller.isFailed()) {
    EXIT(EXIT_FAILURE) << "Failed to detect a controller: " << _controller.failure();
  }

  Option<ControllerInfo> latest;

  if (_controller.isDiscarded()) {
    LOG(INFO) << "Re-detecting controller";
    latest = None();
    controller = None();
  } else if (_controller->isNone()) {
    LOG(INFO) << "Lost leading controller";
    latest = None();
    controller = None();
  } else {
    latest = _controller.get();
    controller = UPID(latest->pid());

    LOG(INFO) << "New controller detected at " << controller.get();

    // Cancel the pending registration timer to avoid spurious attempts
    // at reregistration. `Clock::cancel` is idempotent, so this call
    // is safe even if no timer is active or pending.
    Clock::cancel(nodeRegistrationTimer);

    if (state == TERMINATING) {
      LOG(INFO) << "Skipping registration because node is terminating";
      return;
    }

    if (requiredControllerCapabilities.nodeUpdate) {
      protobuf::controller::Capabilities controllerCapabilities(
          latest->capabilities());

      if (!controllerCapabilities.nodeUpdate) {
        EXIT(EXIT_FAILURE) <<
          "Node state changed on restart, but the detected controller lacks the "
          "NODE_UPDATE capability. Refusing to connect.";
        return;
      }

      if (dynamic_cast<medios::controller::detector::StandaloneControllerDetector*>(
          detector)) {
        LOG(WARNING) <<
          "The NODE_UPDATE controller capability is required, "
          "but the current controller detector did not provide controller "
          "capabilities.";
      }
    }

    // Wait for a random amount of time before authentication or
    // registration.
    //
    // TODO(mzhu): Specialize this for authentication.
    Duration duration =
      flags.registration_backoff_factor * ((double) os::random() / RAND_MAX);

    if (credential.isSome()) {
      // Authenticate with the controller.
      // TODO(vinod): Consider adding an "AUTHENTICATED" state to the
      // node instead of "authenticate" variable.
      Duration maxTimeout = flags.authentication_timeout_min +
                            flags.authentication_backoff_factor * 2;

      delay(
          duration,
          self(),
          &Node::authenticate,
          flags.authentication_timeout_min,
          std::min(maxTimeout, flags.authentication_timeout_max));
    } else {
      // Proceed with registration without authentication.
      LOG(INFO) << "No credentials provided."
                << " Attempting to register without authentication";

      delay(duration,
            self(),
            &Node::doReliableRegistration,
            flags.registration_backoff_factor * 2); // Backoff.
    }
  }

  // Keep detecting controllers.
  LOG(INFO) << "Detecting new controller";
  detection = detector->detect(latest)
    .onAny(defer(self(), &Node::detected, lambda::_1));
}


void Node::authenticate(Duration minTimeout, Duration maxTimeout)
{
  authenticated = false;

  if (controller.isNone()) {
    return;
  }

  if (authenticating.isSome()) {
    // Authentication is in progress. Try to cancel it.
    // Note that it is possible that 'authenticating' is ready
    // and the dispatch to '_authenticate' is enqueued when we
    // are here, making the 'discard' here a no-op. This is ok
    // because we set 'reauthenticate' here which enforces a retry
    // in '_authenticate'.
    Future<bool> authenticating_ = authenticating.get();
    authenticating_.discard();
    reauthenticate = true;
    return;
  }

  LOG(INFO) << "Authenticating with controller " << controller.get();

  // Ensure there is a link to the controller before we start
  // communicating with it.
  link(controller.get());

  CHECK(authenticatee == nullptr);

  if (authenticateeName == DEFAULT_AUTHENTICATEE) {
    LOG(INFO) << "Using default identity_assertion authenticatee";
    authenticatee = new authentication::IdentityAssertionAuthenticatee();
  }

  if (authenticatee == nullptr) {
    Try<Authenticatee*> module =
      modules::ModuleManager::create<Authenticatee>(authenticateeName);
    if (module.isError()) {
      EXIT(EXIT_FAILURE)
        << "Could not create authenticatee module '"
        << authenticateeName << "': " << module.error();
    }
    LOG(INFO) << "Using '" << authenticateeName << "' authenticatee";
    authenticatee = module.get();
  }

  CHECK_SOME(credential);

  // We pick a random duration between `minTimeout` and `maxTimeout`.
  Duration timeout =
    minTimeout + (maxTimeout - minTimeout) * ((double)os::random() / RAND_MAX);

  authenticating =
    authenticatee->authenticate(controller.get(), self(), credential.get())
      .onAny(defer(self(), &Self::_authenticate, minTimeout, maxTimeout))
      .after(timeout, [](Future<bool> future) {
        // NOTE: Discarded future results in a retry in '_authenticate()'.
        // This is a no-op if the future is already ready.
        if (future.discard()) {
          LOG(WARNING) << "Authentication timed out";
        }

        return future;
      });
}


void Node::_authenticate(
    Duration currentMinTimeout, Duration currentMaxTimeout)
{
  delete CHECK_NOTNULL(authenticatee);
  authenticatee = nullptr;

  CHECK_SOME(authenticating);
  const Future<bool>& future = authenticating.get();

  if (controller.isNone()) {
    LOG(INFO) << "Ignoring _authenticate because the controller is lost";
    authenticating = None();
    // Set it to false because we do not want further retries until
    // a new controller is detected.
    // We obviously do not need to reauthenticate either even if
    // 'reauthenticate' is currently true because the controller is
    // lost.
    reauthenticate = false;
    return;
  }

  if (reauthenticate || !future.isReady()) {
    LOG(WARNING)
      << "Failed to authenticate with controller " << controller.get() << ": "
      << (reauthenticate ? "controller changed" :
         (future.isFailed() ? future.failure() : "future discarded"));

    authenticating = None();
    reauthenticate = false;

    // Grow the timeout range using exponential backoff:
    //
    //   [min, min + factor * 2^0]
    //   [min, min + factor * 2^1]
    //   ...
    //   [min, min + factor * 2^N]
    //   ...
    //   [min, max] // Stop at max.
    Duration maxTimeout =
      currentMinTimeout + (currentMaxTimeout - currentMinTimeout) * 2;

    authenticate(
        currentMinTimeout,
        std::min(maxTimeout, flags.authentication_timeout_max));
    return;
  }

  if (!future.get()) {
    // For refused authentication, we exit instead of doing a shutdown
    // to keep possibly active executors running.
    EXIT(EXIT_FAILURE)
      << "Controller " << controller.get() << " refused authentication";
  }

  LOG(INFO) << "Successfully authenticated with controller " << controller.get();

  authenticated = true;
  authenticating = None();

  // Proceed with registration.
  doReliableRegistration(flags.registration_backoff_factor * 2);
}


void Node::registered(
    const UPID& from,
    const NodeID& nodeId,
    const ControllerNodeConnection& connection)
{
  if (controller != from) {
    LOG(WARNING) << "Ignoring registration message from " << from
                 << " because it is not the expected controller: "
                 << (controller.isSome() ? stringify(controller.get()) : "None");
    return;
  }

  CHECK_SOME(controller);

  if (connection.has_total_ping_timeout_seconds()) {
    controllerPingTimeout =
      Seconds(static_cast<int64_t>(connection.total_ping_timeout_seconds()));
  } else {
    controllerPingTimeout = DEFAULT_CONTROLLER_PING_TIMEOUT();
  }

  switch (state) {
    case DISCONNECTED: {
      LOG(INFO) << "Registered with controller " << controller.get()
                << "; given node ID " << nodeId;

      state = RUNNING;

      // Cancel the pending registration timer to avoid spurious attempts
      // at reregistration. `Clock::cancel` is idempotent, so this call
      // is safe even if no timer is active or pending.
      Clock::cancel(nodeRegistrationTimer);

      taskStatusUpdateManager->resume(); // Resume status updates.

      info.mutable_id()->CopyFrom(nodeId); // Store the node id.

      // Create the node meta directory.
      paths::createNodeDirectory(metaDir, nodeId);

      // Initialize and resume the operation status update manager.
      //
      // NOTE: There is no need to recover the operation status update manager,
      // because its streams are checkpointed within the node meta directory
      // which was just created.
      operationStatusUpdateManager.initialize(
          defer(self(), &Self::sendOperationStatusUpdate, lambda::_1),
          std::bind(
              &node::paths::getNodeOperationUpdatesPath,
              metaDir,
              info.id(),
              lambda::_1));

      operationStatusUpdateManager.resume();

      // Checkpoint node info.
      const string path = paths::getNodeInfoPath(metaDir, nodeId);

      VLOG(1) << "Checkpointing NodeInfo to '" << path << "'";

      CHECK_SOME(state::checkpoint(path, info));

      // If we registered with this node ID for the first time initialize
      // the resource provider manager with it; if the manager was already
      // initialized with a recovered node ID this is a no-op.
      initializeResourceProviderManager(flags, info.id());

      // Setup a timer so that the node attempts to reregister if it
      // doesn't receive a ping from the controller for an extended period
      // of time. This needs to be done once registered, in case we
      // never receive an initial ping.
      Clock::cancel(pingTimer);

      pingTimer = delay(
          controllerPingTimeout,
          self(),
          &Node::pingTimeout,
          detection);

      break;
    }
    case RUNNING:
      // Already registered!
      if (info.id() != nodeId) {
       EXIT(EXIT_FAILURE)
         << "Registered but got wrong id: " << nodeId
         << " (expected: " << info.id() << "). Committing suicide";
      }
      LOG(WARNING) << "Already registered with controller " << controller.get();

      break;
    case TERMINATING:
      LOG(WARNING) << "Ignoring registration because node is terminating";
      break;
    case RECOVERING:
    default:
      LOG(FATAL) << "Unexpected node state " << state;
      break;
  }

  // If this node can support resource providers or has had any oversubscribed
  // resources set, send an `UpdateNodeMessage` to the controller to inform it of a
  // possible changes between completion of recovery and node registration.
  if (capabilities.resourceProvider || oversubscribedResources.isSome()) {
    UpdateNodeMessage message = generateUpdateNodeMessage();

    LOG(INFO) << "Forwarding node update " << JSON::protobuf(message);

    send(controller.get(), message);
  }
}


void Node::reregistered(
    const UPID& from,
    const NodeID& nodeId,
    const vector<ReconcileTasksMessage>& reconciliations,
    const ControllerNodeConnection& connection)
{
  if (controller != from) {
    LOG(WARNING) << "Ignoring re-registration message from " << from
                 << " because it is not the expected controller: "
                 << (controller.isSome() ? stringify(controller.get()) : "None");
    return;
  }

  CHECK_SOME(controller);

  if (info.id() != nodeId) {
    EXIT(EXIT_FAILURE)
      << "Re-registered but got wrong id: " << nodeId
      << " (expected: " << info.id() << "). Committing suicide";
  }

  if (connection.has_total_ping_timeout_seconds()) {
    controllerPingTimeout =
      Seconds(static_cast<int64_t>(connection.total_ping_timeout_seconds()));
  } else {
    controllerPingTimeout = DEFAULT_CONTROLLER_PING_TIMEOUT();
  }

  switch (state) {
    case DISCONNECTED:
      LOG(INFO) << "Re-registered with controller " << controller.get();
      state = RUNNING;
      taskStatusUpdateManager->resume(); // Resume status updates.
      operationStatusUpdateManager.resume();

      // Setup a timer so that the node attempts to reregister if it
      // doesn't receive a ping from the controller for an extended period
      // of time. This needs to be done once reregistered, in case we
      // never receive an initial ping.
      Clock::cancel(pingTimer);

      pingTimer = delay(
          controllerPingTimeout,
          self(),
          &Node::pingTimeout,
          detection);

      break;
    case RUNNING:
      LOG(WARNING) << "Already reregistered with controller " << controller.get();
      break;
    case TERMINATING:
      LOG(WARNING) << "Ignoring re-registration because node is terminating";
      return;
    case RECOVERING:
      // It's possible to receive a message intended for the previous
      // run of the node here. Short term we can leave this as is and
      // crash in this case. Ideally responses can be tied to a
      // particular run of the node, see:
      // https://issues.apache.org/jira/browse/MESOS-676
      // https://issues.apache.org/jira/browse/MESOS-677
    default:
      LOG(FATAL) << "Unexpected node state " << state;
      return;
  }

  // If this node can support resource providers or has had any oversubscribed
  // resources set, send an `UpdateNodeMessage` to the controller to inform it of a
  // possible changes between completion of recovery and node registration.
  if (capabilities.resourceProvider || oversubscribedResources.isSome()) {
    UpdateNodeMessage message = generateUpdateNodeMessage();

    LOG(INFO) << "Forwarding node update " << JSON::protobuf(message);
    send(controller.get(), message);
  }

  // Reconcile any tasks per the controller's request.
  foreach (const ReconcileTasksMessage& reconcile, reconciliations) {
    Framework* framework = getFramework(reconcile.framework_id());

    foreach (const TaskStatus& status, reconcile.statuses()) {
      const TaskID& taskId = status.task_id();

      bool known = false;
      if (framework != nullptr) {
        known = framework->hasTask(taskId);
      }

      // Send a terminal status update for each task that is known to
      // the controller but not known to the node. This ensures that the
      // controller will cleanup any state associated with the task, which
      // is not running. We send TASK_DROPPED to partition-aware
      // frameworks; frameworks that are not partition-aware are sent
      // TASK_LOST for backward compatibility.
      //
      // If the task is known to the node, we don't need to send a
      // status update to the controller: because the controller already knows
      // about the task, any subsequent status updates will be
      // propagated correctly.
      if (!known) {
        // NOTE: The `framework` field of the `ReconcileTasksMessage`
        // is only set by controllers running Medios 1.1.0 or later. If the
        // field is unset, we assume the framework is not partition-aware.
        medios::TaskState taskState = TASK_LOST;

        if (reconcile.has_framework() &&
            protobuf::frameworkHasCapability(
                reconcile.framework(),
                FrameworkInfo::Capability::PARTITION_AWARE)) {
          taskState = TASK_DROPPED;
        }

        LOG(WARNING) << "Node reconciling task " << taskId
                     << " of framework " << reconcile.framework_id()
                     << " in state " << taskState
                     << ": task unknown to the node";

        const StatusUpdate update = protobuf::createStatusUpdate(
            reconcile.framework_id(),
            info.id(),
            taskId,
            taskState,
            TaskStatus::SOURCE_NODE,
            id::UUID::random(),
            "Reconciliation: task unknown to the node",
            TaskStatus::REASON_RECONCILIATION);

        // NOTE: We can't use statusUpdate() here because it drops
        // updates for unknown frameworks.
        taskStatusUpdateManager->update(update, info.id())
          .onAny(defer(self(),
                       &Node::___statusUpdate,
                       lambda::_1,
                       update,
                       UPID()));
      }
    }
  }
}


void Node::doReliableRegistration(Duration maxBackoff)
{
  if (controller.isNone()) {
    LOG(INFO) << "Skipping registration because no controller present";
    return;
  }

  if (credential.isSome() && !authenticated) {
    LOG(INFO) << "Skipping registration because not authenticated";
    return;
  }

  if (state == RUNNING) { // Node (re-)registered with the controller.
    return;
  }

  if (state == TERMINATING) {
    LOG(INFO) << "Skipping registration because node is terminating";
    return;
  }

  CHECK(state == DISCONNECTED) << state;

  CHECK_NE("cleanup", flags.recover);

  // Ensure there is a link to the controller before we start
  // communicating with it. We want to link after the initial
  // registration backoff in order to avoid all of the nodes
  // establishing connections with the controller at once.
  // See MESOS-5330.
  link(controller.get());

  if (!info.has_id()) {
    // Registering for the first time.
    RegisterNodeMessage message;
    message.set_version(MEDIOS_VERSION);
    message.mutable_node()->CopyFrom(info);

    message.mutable_node_capabilities()->CopyFrom(
        capabilities.toRepeatedPtrField());

    // Include checkpointed resources.
    message.mutable_checkpointed_resources()->CopyFrom(checkpointedResources);

    message.mutable_resource_version_uuid()->CopyFrom(resourceVersion);

    // If the `Try` from `downgradeResources` returns an `Error`, we currently
    // continue to send the resources to the controller in a partially downgraded
    // state. This implies that an node with refined reservations cannot work
    // with versions of controller before reservation refinement support, which was
    // introduced in 1.4.0.
    //
    // TODO(mpark): Do something smarter with the result once something
    //              like a controller capability is introduced.
    downgradeResources(&message);

    send(controller.get(), message);
  } else {
    // Re-registering, so send tasks running.
    ReregisterNodeMessage message;
    message.set_version(MEDIOS_VERSION);

    message.mutable_node_capabilities()->CopyFrom(
        capabilities.toRepeatedPtrField());

    // Include checkpointed resources.
    message.mutable_checkpointed_resources()->CopyFrom(checkpointedResources);

    message.mutable_resource_version_uuid()->CopyFrom(resourceVersion);
    message.mutable_node()->CopyFrom(info);

    foreachvalue (Framework* framework, frameworks) {
      message.add_frameworks()->CopyFrom(framework->info);

      // TODO(bmahler): We need to send the executors for these
      // pending tasks, and we need to send exited events if they
      // cannot be launched, see MESOS-1715, MESOS-1720, MESOS-1800.
      typedef hashmap<TaskID, TaskInfo> TaskMap;
      foreachvalue (const TaskMap& tasks, framework->pendingTasks) {
        foreachvalue (const TaskInfo& task, tasks) {
          message.add_tasks()->CopyFrom(protobuf::createTask(
              task, TASK_STAGING, framework->id(), None()));
        }
      }

      foreachvalue (Executor* executor, framework->executors) {
        // Add launched, terminated, and queued tasks.
        // Note that terminated executors will only have terminated
        // unacknowledged tasks.
        // Note that for each task the latest state and status update
        // state (if any) is also included.
        foreachvalue (Task* task, executor->launchedTasks) {
          message.add_tasks()->CopyFrom(*task);
        }

        foreachvalue (Task* task, executor->terminatedTasks) {
          message.add_tasks()->CopyFrom(*task);
        }

        foreachvalue (const TaskInfo& task, executor->queuedTasks) {
          message.add_tasks()->CopyFrom(protobuf::createTask(
              task, TASK_STAGING, framework->id(), None()));
        }

        // Do not reregister with Command (or Docker) Executors
        // because the controller doesn't store them; they are generated
        // by the node.
        if (!executor->isGeneratedForCommandTask()) {
          // Ignore terminated executors because they do not consume
          // any resources.
          if (executor->state != Executor::TERMINATED) {
            ExecutorInfo* executorInfo = message.add_executor_infos();
            executorInfo->MergeFrom(executor->info);

            // Scheduler Driver will ensure the framework id is set in
            // ExecutorInfo, effectively making it a required field.
            CHECK(executorInfo->has_framework_id());
          }
        }
      }
    }

    // Add completed frameworks.
    foreachvalue (const Owned<Framework>& completedFramework,
                  completedFrameworks) {
      VLOG(1) << "Reregistering completed framework "
                << completedFramework->id();

      Archive::Framework* completedFramework_ =
        message.add_completed_frameworks();

      completedFramework_->mutable_framework_info()->CopyFrom(
          completedFramework->info);

      if (completedFramework->pid.isSome()) {
        completedFramework_->set_pid(completedFramework->pid.get());
      }

      foreach (const Owned<Executor>& executor,
               completedFramework->completedExecutors) {
        VLOG(2) << "Reregistering completed executor '" << executor->id
                << "' with " << executor->terminatedTasks.size()
                << " terminated tasks, " << executor->completedTasks.size()
                << " completed tasks";

        foreachvalue (const Task* task, executor->terminatedTasks) {
          VLOG(2) << "Reregistering terminated task " << task->task_id();
          completedFramework_->add_tasks()->CopyFrom(*task);
        }

        foreach (const shared_ptr<Task>& task, executor->completedTasks) {
          VLOG(2) << "Reregistering completed task " << task->task_id();
          completedFramework_->add_tasks()->CopyFrom(*task);
        }
      }
    }

    // If the `Try` from `downgradeResources` returns an `Error`, we currently
    // continue to send the resources to the controller in a partially downgraded
    // state. This implies that an node with refined reservations cannot work
    // with versions of controller before reservation refinement support, which was
    // introduced in 1.4.0.
    //
    // TODO(mpark): Do something smarter with the result once something
    // like a controller capability is introduced.
    downgradeResources(&message);

    CHECK_SOME(controller);
    send(controller.get(), message);
  }

  // Bound the maximum backoff by 'REGISTER_RETRY_INTERVAL_MAX'.
  maxBackoff = std::min(maxBackoff, REGISTER_RETRY_INTERVAL_MAX);

  // Determine the delay for next attempt by picking a random
  // duration between 0 and 'maxBackoff'.
  Duration delay = maxBackoff * ((double) os::random() / RAND_MAX);

  VLOG(1) << "Will retry registration in " << delay << " if necessary";

  // Backoff.
  nodeRegistrationTimer = process::delay(
      delay,
      self(),
      &Node::doReliableRegistration,
      maxBackoff * 2);
}


void Node::handleRunTaskMessage(
    const UPID& from,
    RunTaskMessage&& runTaskMessage)
{
  runTask(
      from,
      runTaskMessage.framework(),
      runTaskMessage.framework_id(),
      runTaskMessage.pid(),
      runTaskMessage.task(),
      google::protobuf::convert(runTaskMessage.resource_version_uuids()),
      runTaskMessage.has_launch_executor() ?
          Option<bool>(runTaskMessage.launch_executor()) : None());
}


// TODO(vinod): Instead of crashing the node on checkpoint errors,
// send TASK_LOST to the framework.
void Node::runTask(
    const UPID& from,
    const FrameworkInfo& frameworkInfo,
    const FrameworkID& frameworkId,
    const UPID& pid,
    const TaskInfo& task,
    const vector<ResourceVersionUUID>& resourceVersionUuids,
    const Option<bool>& launchExecutor)
{
  CHECK_NE(task.has_executor(), task.has_command())
    << "Task " << task.task_id()
    << " should have either CommandInfo or ExecutorInfo set but not both";

  if (controller != from) {
    LOG(WARNING) << "Ignoring run task message from " << from
                 << " because it is not the expected controller: "
                 << (controller.isSome() ? stringify(controller.get()) : "None");
    return;
  }

  if (!frameworkInfo.has_id()) {
    LOG(ERROR) << "Ignoring run task message from " << from
               << " because it does not have a framework ID";
    return;
  }

  const ExecutorInfo executorInfo = getExecutorInfo(frameworkInfo, task);

  bool executorGeneratedForCommandTask = !task.has_executor();

  run(frameworkInfo,
      executorInfo,
      task,
      None(),
      resourceVersionUuids,
      pid,
      launchExecutor,
      executorGeneratedForCommandTask);
}


Option<Error> Node::validateResourceLimitsAndIsolators(
    const vector<TaskInfo>& tasks)
{
  foreach (const TaskInfo& task, tasks) {
    if (!(task.has_container() &&
          task.container().type() == ContainerInfo::DOCKER)) {
      if (task.limits().count("cpus") &&
          !(strings::contains(flags.isolation, "cgroups/cpu") ||
            strings::contains(flags.isolation, "cgroups/all"))) {
        return Error(
            "CPU limits can only be set on tasks launched in Medios containers"
            " when the node has loaded the 'cgroups/cpu' isolator");
      }

      if (task.limits().count("mem") &&
          !(strings::contains(flags.isolation, "cgroups/mem") ||
            strings::contains(flags.isolation, "cgroups/all"))) {
        return Error(
            "Memory limits can only be set on tasks launched in Medios"
            " containers when the node has loaded the 'cgroups/mem' isolator");
      }
    }
  }

  return None();
}


// A STORE volume is delivered by a bind in a private mount namespace,
// the `filesystem/linux` isolator's, or by a link in the sandbox, the
// `filesystem/posix` isolator's (see `mediosDeliversStores`). A
// docker task is the docker executor's, which binds stores itself. A
// node that has neither would run the command with nothing at the path
// and report it finished, so the task is refused here, before anything
// is launched, naming the store and what this node lacks.
Option<Error> Node::validateStoreVolumes(const vector<TaskInfo>& tasks)
{
  const bool deliversStores =
    mediosDeliversStores(flags.launcher, flags.isolation);

  foreach (const TaskInfo& task, tasks) {
    if (!task.has_container() ||
        task.container().type() == ContainerInfo::DOCKER) {
      continue;
    }

    foreach (const Volume& volume, task.container().volumes()) {
      if (!volume.has_source() ||
          volume.source().type() != Volume::Source::STORE) {
        continue;
      }

      if (deliversStores) {
        continue;
      }

      return Error(
          "Task '" + task.task_id().value() + "' has a STORE volume naming "
          "store '" + volume.source().store().name() + "' at '" +
          volume.container_path() + "', and this node cannot deliver it: "
          "its launcher is '" + flags.launcher + "' and its isolation is '" +
          flags.isolation + "', and a store is bound by filesystem/linux "
          "under the linux launcher or linked by filesystem/posix under "
          "the posix launcher");
    }
  }

  return None();
}


void Node::run(
    const FrameworkInfo& frameworkInfo,
    ExecutorInfo executorInfo,
    Option<TaskInfo> task,
    Option<TaskGroupInfo> taskGroup,
    const vector<ResourceVersionUUID>& resourceVersionUuids,
    const UPID& pid,
    const Option<bool>& launchExecutor,
    bool executorGeneratedForCommandTask)
{
  CHECK_NE(task.isSome(), taskGroup.isSome())
    << "Either task or task group should be set but not both";

  auto injectAllocationInfo = [](
      RepeatedPtrField<Resource>* resources,
      const FrameworkInfo& frameworkInfo) {
    set<string> roles = protobuf::framework::getRoles(frameworkInfo);

    foreach (Resource& resource, *resources) {
      if (!resource.has_allocation_info()) {
        if (roles.size() != 1) {
          LOG(FATAL) << "Missing 'Resource.AllocationInfo' for resources"
                     << " allocated to MULTI_ROLE framework"
                     << " '" << frameworkInfo.name() << "'";
        }

        resource.mutable_allocation_info()->set_role(*roles.begin());
      }
    }
  };

  injectAllocationInfo(executorInfo.mutable_resources(), frameworkInfo);
  upgradeResources(&executorInfo);

  if (task.isSome()) {
    injectAllocationInfo(task->mutable_resources(), frameworkInfo);

    if (task->has_executor()) {
      injectAllocationInfo(
          task->mutable_executor()->mutable_resources(),
          frameworkInfo);
    }

    upgradeResources(&task.get());
  }

  if (taskGroup.isSome()) {
    foreach (TaskInfo& task, *taskGroup->mutable_tasks()) {
      injectAllocationInfo(task.mutable_resources(), frameworkInfo);

      if (task.has_executor()) {
        injectAllocationInfo(
            task.mutable_executor()->mutable_resources(),
            frameworkInfo);
      }
    }

    upgradeResources(&taskGroup.get());
  }

  vector<TaskInfo> tasks;
  if (task.isSome()) {
    tasks.push_back(task.get());
  } else {
    foreach (const TaskInfo& task, taskGroup->tasks()) {
      tasks.push_back(task);
    }
  }

  const FrameworkID& frameworkId = frameworkInfo.id();

  LOG(INFO) << "Got assigned " << taskOrTaskGroup(task, taskGroup)
            << " for framework " << frameworkId;

  foreach (const TaskInfo& _task, tasks) {
    if (_task.node_id() != info.id()) {
      LOG(WARNING)
        << "Node " << info.id() << " ignoring running "
        << taskOrTaskGroup(_task, taskGroup) << " because "
        << "it was intended for old node " << _task.node_id();
      return;
    }
  }

  CHECK(state == RECOVERING || state == DISCONNECTED ||
        state == RUNNING || state == TERMINATING)
    << state;

  // TODO(bmahler): Also ignore if we're DISCONNECTED.
  if (state == RECOVERING || state == TERMINATING) {
    LOG(WARNING) << "Ignoring running " << taskOrTaskGroup(task, taskGroup)
                 << " because the node is " << state;

    // We do not send `ExitedExecutorMessage` here because the disconnected
    // node is expected to (eventually) reregister and reconcile the executor
    // states with the controller.

    // TODO(vinod): Consider sending a TASK_LOST here.
    // Currently it is tricky because 'statusUpdate()'
    // ignores updates for unknown frameworks.
    return;
  }

  vector<Future<bool>> unschedules;

  // If we are about to create a new framework, unschedule the work
  // and meta directories from getting gc'ed.
  Framework* framework = getFramework(frameworkId);
  if (framework == nullptr) {
    // Unschedule framework work directory.
    string path = paths::getFrameworkPath(
        flags.work_dir, info.id(), frameworkId);

    if (os::exists(path)) {
      unschedules.push_back(gc->unschedule(path));
    }

    // Unschedule framework meta directory.
    path = paths::getFrameworkPath(metaDir, info.id(), frameworkId);
    if (os::exists(path)) {
      unschedules.push_back(gc->unschedule(path));
    }

    Option<UPID> frameworkPid = None();

    if (pid != UPID()) {
      frameworkPid = pid;
    }

    framework = new Framework(
        this,
        flags,
        frameworkInfo,
        frameworkPid);

    frameworks[frameworkId] = framework;
    if (frameworkInfo.checkpoint()) {
      framework->checkpointFramework();
    }

    // Does this framework ID already exist in `completedFrameworks`?
    // If so, move the completed executors of the old framework to
    // this new framework and remove the old completed framework.
    if (completedFrameworks.contains(frameworkId)) {
      Owned<Framework>& completedFramework =
        completedFrameworks.at(frameworkId);

      framework->completedExecutors = completedFramework->completedExecutors;
      completedFrameworks.erase(frameworkId);
    }
  }

  CHECK_NOTNULL(framework);

  Option<Error> undeliverable = validateStoreVolumes(tasks);
  if (undeliverable.isSome()) {
    // Dropped, not invalid: the task is well formed and runs on a node
    // that delivers stores; this node cannot carry it out, and it was
    // never launched. TASK_LOST for a framework without PARTITION_AWARE,
    // as the validator below reports. The sentence is the whole message
    // and no reason is claimed, since none names a node's capability.
    medios::TaskState taskState = TASK_DROPPED;
    if (!protobuf::frameworkHasCapability(
            frameworkInfo, FrameworkInfo::Capability::PARTITION_AWARE)) {
      taskState = TASK_LOST;
    }

    foreach (const TaskInfo& _task, tasks) {
      const StatusUpdate update = protobuf::createStatusUpdate(
          frameworkId,
          info.id(),
          _task.task_id(),
          taskState,
          TaskStatus::SOURCE_NODE,
          id::UUID::random(),
          undeliverable->message,
          None());

      statusUpdate(update, UPID());
    }

    if (framework->idle()) {
      removeFramework(framework);
    }

    return;
  }

  Option<Error> error = validateResourceLimitsAndIsolators(tasks);
  if (error.isSome()) {
    // We report TASK_DROPPED to the framework because the task was
    // never launched. For non-partition-aware frameworks, we report
    // TASK_LOST for backward compatibility.
    medios::TaskState taskState = TASK_DROPPED;
    if (!protobuf::frameworkHasCapability(
            frameworkInfo, FrameworkInfo::Capability::PARTITION_AWARE)) {
      taskState = TASK_LOST;
    }

    foreach (const TaskInfo& _task, tasks) {
      const StatusUpdate update = protobuf::createStatusUpdate(
          frameworkId,
          info.id(),
          _task.task_id(),
          taskState,
          TaskStatus::SOURCE_NODE,
          id::UUID::random(),
          error->message,
          TaskStatus::REASON_GC_ERROR);

      statusUpdate(update, UPID());
    }

    if (framework->idle()) {
      removeFramework(framework);
    }

    return;
  }

  const ExecutorID& executorId = executorInfo.executor_id();

  if (HookManager::hooksAvailable()) {
    // Set task labels from run task label decorator.
    for (auto it = tasks.begin(); it != tasks.end(); ++it) {
      (*it).mutable_labels()->CopyFrom(
          HookManager::nodeRunTaskLabelDecorator(
              (*it), executorInfo, frameworkInfo, info));
    }

    // Update `task`/`taskGroup` to reflect the task label updates.
    if (task.isSome()) {
      CHECK_EQ(1u, tasks.size());
      task->mutable_labels()->CopyFrom(tasks[0].labels());
    } else {
      for (int i = 0; i < taskGroup->tasks().size(); ++i) {
        taskGroup->mutable_tasks(i)->mutable_labels()->
          CopyFrom(tasks[i].labels());
      }
    }
  }

  // Track the pending task / task group to ensure the framework is
  // not removed and the framework and top level executor directories
  // are not scheduled for deletion before '_run()' is called.
  //
  // TODO(bmahler): Can we instead track pending tasks within the
  // `Executor` struct by creating it earlier?
  if (task.isSome()) {
    framework->addPendingTask(executorId, task.get());
  } else {
    framework->addPendingTaskGroup(executorId, taskGroup.get());
  }

  // If we are about to create a new executor, unschedule the top
  // level work and meta directories from getting gc'ed.
  Executor* executor = framework->getExecutor(executorId);
  if (executor == nullptr) {
    // Unschedule executor work directory.
    string path = paths::getExecutorPath(
        flags.work_dir, info.id(), frameworkId, executorId);

    if (os::exists(path)) {
      unschedules.push_back(gc->unschedule(path));
    }

    // Unschedule executor meta directory.
    path = paths::getExecutorPath(metaDir, info.id(), frameworkId, executorId);

    if (os::exists(path)) {
      unschedules.push_back(gc->unschedule(path));
    }
  }

  auto onUnscheduleGCFailure =
    [=](const Future<vector<bool>>& unschedules) -> Future<vector<bool>> {
      LOG(ERROR) << "Failed to unschedule directories scheduled for gc: "
                 << unschedules.failure();

      Framework* _framework = getFramework(frameworkId);
      if (_framework == nullptr) {
        const string error =
          "Cannot handle unschedule GC failure for " +
          taskOrTaskGroup(task, taskGroup) + " because the framework " +
          stringify(frameworkId) + " does not exist";

        LOG(WARNING) << error;

        return Failure(error);
      }

      // We report TASK_DROPPED to the framework because the task was
      // never launched. For non-partition-aware frameworks, we report
      // TASK_LOST for backward compatibility.
      medios::TaskState taskState = TASK_DROPPED;
      if (!protobuf::frameworkHasCapability(
              frameworkInfo, FrameworkInfo::Capability::PARTITION_AWARE)) {
        taskState = TASK_LOST;
      }

      foreach (const TaskInfo& _task, tasks) {
        _framework->removePendingTask(_task.task_id());

        const StatusUpdate update = protobuf::createStatusUpdate(
            frameworkId,
            info.id(),
            _task.task_id(),
            taskState,
            TaskStatus::SOURCE_NODE,
            id::UUID::random(),
            "Could not launch the task because we failed to unschedule"
            " directories scheduled for gc",
            TaskStatus::REASON_GC_ERROR);

        // TODO(vinod): Ensure that the task status update manager
        // reliably delivers this update. Currently, we don't guarantee
        // this because removal of the framework causes the status
        // update manager to stop retrying for its un-acked updates.
        statusUpdate(update, UPID());
      }

      if (_framework->idle()) {
        removeFramework(_framework);
      }

      return unschedules;
  };

  // `taskLaunch` encapsulates each task's launch steps from this point
  // to the end of `_run` (the completion of task authorization).
  Future<Nothing> taskLaunch = collect(unschedules)
    // Handle the failure iff unschedule GC fails.
    .repair(defer(self(), onUnscheduleGCFailure))
    // If unschedule GC succeeds, trigger the next continuation.
    .then(defer(
        self(),
        &Self::_run,
        frameworkInfo,
        executorInfo,
        task,
        taskGroup,
        resourceVersionUuids,
        launchExecutor));

  // Use a sequence to ensure that task launch order is preserved.
  framework->taskLaunchSequences[executorId]
    .add<Nothing>([taskLaunch]() -> Future<Nothing> {
      // We use this sequence only to maintain the task launching order. If the
      // sequence is deleted, we do not want the resulting discard event to
      // propagate up the chain, which would prevent the previous `.then()` or
      // `.repair()` callbacks from being invoked. Thus, we use `undiscardable`
      // to protect each `taskLaunch`.
      return undiscardable(taskLaunch);
    })
    // We register `onAny` on the future returned by the sequence (referred to
    // as `seqFuture` below). The following scenarios could happen:
    //
    // (1) `seqFuture` becomes ready. This happens when all previous tasks'
    // `taskLaunch` futures are in non-pending state AND this task's own
    // `taskLaunch` future is in ready state. The `onReady` call registered
    // below will be triggered and continue the success path.
    //
    // (2) `seqFuture` becomes failed. This happens when all previous tasks'
    // `taskLaunch` futures are in non-pending state AND this task's own
    // `taskLaunch` future is in failed state (e.g. due to unschedule GC
    // failure or some other failure). The `onFailed` call registered below
    // will be triggered to handle the failure.
    //
    // (3) `seqFuture` becomes discarded. This happens when the sequence is
    // destructed (see declaration of `taskLaunchSequences` on its lifecycle)
    // while the `seqFuture` is still pending. In this case, we wait until
    // this task's own `taskLaunch` future becomes non-pending and trigger
    // callbacks accordingly.
    //
    // TODO(mzhu): In case (3), the destruction of the sequence means that the
    // node will eventually discover that the executor is absent and drop
    // the task. While `__run` is capable of handling this case, it is more
    // optimal to handle the failure earlier here rather than waiting for
    // the `taskLaunch` transition and directing control to `__run`.
    .onAny(defer(self(), [=](const Future<Nothing>&) {
      // We only want to execute the following callbacks once the work performed
      // in the `taskLaunch` chain is complete. Thus, we add them onto the
      // `taskLaunch` chain rather than dispatching directly.
      taskLaunch
        .onReady(defer(
            self(),
            &Self::__run,
            frameworkInfo,
            executorInfo,
            task,
            taskGroup,
            resourceVersionUuids,
            launchExecutor,
            executorGeneratedForCommandTask))
        .onFailed(defer(self(), [=](const string& failure) {
          Framework* _framework = getFramework(frameworkId);
          if (_framework == nullptr) {
            LOG(WARNING) << "Ignoring running "
                         << taskOrTaskGroup(task, taskGroup)
                         << " because the framework " << stringify(frameworkId)
                         << " does not exist";
          }

          if (launchExecutor.isSome() && launchExecutor.get()) {
            // Controller expects a new executor to be launched for this task(s).
            // To keep the controller executor entries updated, the node needs to
            // send `ExitedExecutorMessage` even though no executor launched.
            sendExitedExecutorMessage(frameworkId, executorInfo.executor_id());

            // See the declaration of `taskLaunchSequences` regarding its
            // lifecycle management.
            if (_framework != nullptr) {
              _framework->taskLaunchSequences.erase(executorInfo.executor_id());
            }
          }
        }));
    }));

  // TODO(mzhu): Consolidate error handling code in `__run` here.
}


Future<Nothing> Node::_run(
    const FrameworkInfo& frameworkInfo,
    const ExecutorInfo& executorInfo,
    const Option<TaskInfo>& task,
    const Option<TaskGroupInfo>& taskGroup,
    const std::vector<ResourceVersionUUID>& resourceVersionUuids,
    const Option<bool>& launchExecutor)
{
  // TODO(anindya_sinha): Consider refactoring the initial steps common
  // to `_run()` and `__run()`.
  CHECK_NE(task.isSome(), taskGroup.isSome())
    << "Either task or task group should be set but not both";

  vector<TaskInfo> tasks;
  if (task.isSome()) {
    tasks.push_back(task.get());
  } else {
    foreach (const TaskInfo& _task, taskGroup->tasks()) {
      tasks.push_back(_task);
    }
  }

  const FrameworkID& frameworkId = frameworkInfo.id();
  Framework* framework = getFramework(frameworkId);
  if (framework == nullptr) {
    const string error =
      "Ignoring running " + taskOrTaskGroup(task, taskGroup) +
      " because the framework " + stringify(frameworkId) + " does not exist";

    LOG(WARNING) << error;

    return Failure(error);
  }

  // We don't send a status update here because a terminating
  // framework cannot send acknowledgements.
  if (framework->state == Framework::TERMINATING) {
    const string error = "Ignoring running " +
                         taskOrTaskGroup(task, taskGroup) + " of framework " +
                         stringify(frameworkId) +
                         " because the framework is terminating";

    LOG(WARNING) << error;

    // Although we cannot send a status update in this case, we remove
    // the affected tasks from the pending tasks.
    foreach (const TaskInfo& _task, tasks) {
      framework->removePendingTask(_task.task_id());
    }

    if (framework->idle()) {
      removeFramework(framework);
    }

    return Failure(error);
  }

  // Ignore the launch if killed in the interim. The invariant here
  // is that all tasks in the group are still pending, or all were
  // removed due to a kill arriving for one of the tasks in the group.
  bool allPending = true;
  bool allRemoved = true;
  foreach (const TaskInfo& _task, tasks) {
    if (framework->isPending(_task.task_id())) {
      allRemoved = false;
    } else {
      allPending = false;
    }
  }

  CHECK(allPending != allRemoved)
    << "BUG: The " << taskOrTaskGroup(task, taskGroup)
    << " was partially killed";

  if (allRemoved) {
    const string error = "Ignoring running " +
                         taskOrTaskGroup(task, taskGroup) + " of framework " +
                         stringify(frameworkId) +
                         " because it has been killed in the meantime";

    LOG(WARNING) << error;

    return Failure(error);
  }

  // Authorize the task or tasks (as in a task group) to ensure that the
  // task user is allowed to launch tasks on the node. If authorization
  // fails, the task (or all tasks in a task group) are not launched.
  vector<Future<bool>> authorizations;

  LOG(INFO) << "Authorizing " << taskOrTaskGroup(task, taskGroup)
            << " for framework " << frameworkId;

  foreach (const TaskInfo& _task, tasks) {
    authorizations.push_back(authorizeTask(_task, frameworkInfo));
  }

  auto onTaskAuthorizationFailure =
    [=](const string& error, Framework* _framework) {
      CHECK_NOTNULL(_framework);

      // For failed authorization, we send a TASK_ERROR status update
      // for all tasks.
      const TaskStatus::Reason reason = task.isSome()
        ? TaskStatus::REASON_TASK_UNAUTHORIZED
        : TaskStatus::REASON_TASK_GROUP_UNAUTHORIZED;

      LOG(ERROR) << "Authorization failed for "
                 << taskOrTaskGroup(task, taskGroup) << " of framework "
                 << frameworkId << ": " << error;

      foreach (const TaskInfo& _task, tasks) {
        _framework->removePendingTask(_task.task_id());

        const StatusUpdate update = protobuf::createStatusUpdate(
            frameworkId,
            info.id(),
            _task.task_id(),
            TASK_ERROR,
            TaskStatus::SOURCE_NODE,
            id::UUID::random(),
            error,
            reason);

        statusUpdate(update, UPID());
      }

      if (_framework->idle()) {
        removeFramework(_framework);
      }
  };

  return collect(authorizations)
    .repair(defer(self(),
      [=](const Future<vector<bool>>& future) -> Future<vector<bool>> {
        Framework* _framework = getFramework(frameworkId);
        if (_framework == nullptr) {
          const string error =
            "Authorization failed for " + taskOrTaskGroup(task, taskGroup) +
            " because the framework " + stringify(frameworkId) +
            " does not exist";

            LOG(WARNING) << error;

          return Failure(error);
        }

        const string error =
          "Failed to authorize " + taskOrTaskGroup(task, taskGroup) +
          ": " + future.failure();

        onTaskAuthorizationFailure(error, _framework);

        return future;
      }
    ))
    .then(defer(self(),
      [=](const Future<vector<bool>>& future) -> Future<Nothing> {
        Framework* _framework = getFramework(frameworkId);
        if (_framework == nullptr) {
          const string error =
            "Ignoring running " + taskOrTaskGroup(task, taskGroup) +
            " because the framework " + stringify(frameworkId) +
            " does not exist";

            LOG(WARNING) << error;

          return Failure(error);
        }

        deque<bool> authorizations(future->begin(), future->end());

        foreach (const TaskInfo& _task, tasks) {
          bool authorized = authorizations.front();
          authorizations.pop_front();

          // If authorization for this task fails, we fail all tasks (in case
          // of a task group) with this specific error.
          if (!authorized) {
            const string error =
              "Framework " + stringify(frameworkId) +
              " is not authorized to launch task " + stringify(_task);

            onTaskAuthorizationFailure(error, _framework);

            return Failure(error);
          }
        }

        return Nothing();
      }
    ));
}


void Node::__run(
    const FrameworkInfo& frameworkInfo,
    const ExecutorInfo& executorInfo,
    const Option<TaskInfo>& task,
    const Option<TaskGroupInfo>& taskGroup,
    const vector<ResourceVersionUUID>& resourceVersionUuids,
    const Option<bool>& launchExecutor,
    bool executorGeneratedForCommandTask)
{
  CHECK_NE(task.isSome(), taskGroup.isSome())
    << "Either task or task group should be set but not both";

  vector<TaskInfo> tasks;
  if (task.isSome()) {
    tasks.push_back(task.get());
  } else {
    foreach (const TaskInfo& _task, taskGroup->tasks()) {
      tasks.push_back(_task);
    }
  }

  const FrameworkID& frameworkId = frameworkInfo.id();
  Framework* framework = getFramework(frameworkId);
  if (framework == nullptr) {
    LOG(WARNING) << "Ignoring running " << taskOrTaskGroup(task, taskGroup)
                 << " because the framework " << frameworkId
                 << " does not exist";

    if (launchExecutor.isSome() && launchExecutor.get()) {
      // Controller expects a new executor to be launched for this task(s).
      // To keep the controller executor entries updated, the node needs to send
      // `ExitedExecutorMessage` even though no executor launched.
      sendExitedExecutorMessage(frameworkId, executorInfo.executor_id());

      // There is no need to clean up the task launch sequence here since
      // the framework (along with the sequence) no longer exists.
    }

    return;
  }

  const ExecutorID& executorId = executorInfo.executor_id();

  // We report TASK_DROPPED to the framework because the task was
  // never launched. For non-partition-aware frameworks, we report
  // TASK_LOST for backward compatibility.
  auto sendTaskDroppedUpdate =
    [&](TaskStatus::Reason reason, const string& message) {
      medios::TaskState taskState = TASK_DROPPED;

      if (!protobuf::frameworkHasCapability(
              frameworkInfo, FrameworkInfo::Capability::PARTITION_AWARE)) {
        taskState = TASK_LOST;
      }

      foreach (const TaskInfo& _task, tasks) {
        const StatusUpdate update = protobuf::createStatusUpdate(
            frameworkId,
            info.id(),
            _task.task_id(),
            taskState,
            TaskStatus::SOURCE_NODE,
            id::UUID::random(),
            message,
            reason,
            executorId);

        statusUpdate(update, UPID());
      }
    };

  // We don't send a status update here because a terminating
  // framework cannot send acknowledgements.
  if (framework->state == Framework::TERMINATING) {
    LOG(WARNING) << "Ignoring running " << taskOrTaskGroup(task, taskGroup)
                 << " of framework " << frameworkId
                 << " because the framework is terminating";

    // Although we cannot send a status update in this case, we remove
    // the affected tasks from the list of pending tasks.
    foreach (const TaskInfo& _task, tasks) {
      framework->removePendingTask(_task.task_id());
    }

    if (framework->idle()) {
      removeFramework(framework);
    }

    if (launchExecutor.isSome() && launchExecutor.get()) {
      // Controller expects a new executor to be launched for this task(s).
      // To keep the controller executor entries updated, the node needs to send
      // `ExitedExecutorMessage` even though no executor launched.
      sendExitedExecutorMessage(frameworkId, executorInfo.executor_id());

      // See the declaration of `taskLaunchSequences` regarding its lifecycle
      // management.
      framework->taskLaunchSequences.erase(executorInfo.executor_id());
    }

    return;
  }

  // Ignore the launch if killed in the interim. The invariant here
  // is that all tasks in the group are still pending, or all were
  // removed due to a kill arriving for one of the tasks in the group.
  bool allPending = true;
  bool allRemoved = true;
  foreach (const TaskInfo& _task, tasks) {
    if (framework->isPending(_task.task_id())) {
      allRemoved = false;
    } else {
      allPending = false;
    }
  }

  CHECK(allPending != allRemoved)
    << "BUG: The " << taskOrTaskGroup(task, taskGroup)
    << " was partially killed";

  if (allRemoved) {
    LOG(WARNING) << "Ignoring running " << taskOrTaskGroup(task, taskGroup)
                 << " of framework " << frameworkId
                 << " because it has been killed in the meantime";

    if (launchExecutor.isSome() && launchExecutor.get()) {
      // Controller expects a new executor to be launched for this task(s).
      // To keep the controller executor entries updated, the node needs to send
      // `ExitedExecutorMessage` even though no executor launched.
      sendExitedExecutorMessage(frameworkId, executorInfo.executor_id());

      // See the declaration of `taskLaunchSequences` regarding its lifecycle
      // management.
      framework->taskLaunchSequences.erase(executorInfo.executor_id());
    }

    return;
  }

  foreach (const TaskInfo& _task, tasks) {
    CHECK(framework->removePendingTask(_task.task_id()));
  }

  // Check task launch invariants.
  //
  // TODO(bbannier): Instead of copy-pasting identical code to deal
  // with cases where tasks need to be terminated, consolidate code
  // below to decouple checking from terminating.
  Option<string> kill = None();

  // Fail the launch if the node is draining.
  if (drainConfig.isSome()) {
    LOG(WARNING) << "Ignoring running " << taskOrTaskGroup(task, taskGroup)
                 << " of framework " << frameworkId
                 << " because the node is draining";

    kill = "Task was received while node was already draining";
  }

  if (kill.isSome()) {
    sendTaskDroppedUpdate(TaskStatus::REASON_NODE_DRAINING, *kill);

    // Refer to the comment after 'framework->removePendingTask' above
    // for why we need this.
    if (framework->idle()) {
      removeFramework(framework);
    }

    if (launchExecutor.isSome() && launchExecutor.get()) {
      // Controller expects a new executor to be launched for this task(s).
      // To keep the controller executor entries updated, the node needs to send
      // `ExitedExecutorMessage` even though no executor launched.
      sendExitedExecutorMessage(frameworkId, executorInfo.executor_id());

      // See the declaration of `taskLaunchSequences` regarding its lifecycle
      // management.
      framework->taskLaunchSequences.erase(executorInfo.executor_id());
    }

    return;
  }

  CHECK_NONE(kill);

  // If the controller sent resource versions, perform a best-effort check
  // that they are consistent with the resources the task uses.
  //
  // TODO(bbannier): Also check executor resources.
  if (!resourceVersionUuids.empty()) {
    hashset<Option<ResourceProviderID>> usedResourceProviderIds;
    foreach (const TaskInfo& _task, tasks) {
      foreach (const Resource& resource, _task.resources()) {
        usedResourceProviderIds.insert(resource.has_provider_id()
           ? Option<ResourceProviderID>(resource.provider_id())
           : None());
      }
    }

    const hashmap<Option<ResourceProviderID>, UUID>
      receivedResourceVersions = protobuf::parseResourceVersions(
          {resourceVersionUuids.begin(), resourceVersionUuids.end()});

    foreach (const Option<ResourceProviderID>& resourceProviderId,
             usedResourceProviderIds) {
      if (resourceProviderId.isNone()) {
        CHECK(receivedResourceVersions.contains(None()));

        if (resourceVersion != receivedResourceVersions.at(None())) {
          kill = "Task assumes outdated resource state";
        }
      } else {
        ResourceProvider* resourceProvider =
          getResourceProvider(resourceProviderId.get());

        if (resourceProvider == nullptr ||
            resourceProvider->resourceVersion !=
              receivedResourceVersions.at(resourceProviderId.get())) {
          kill = "Task assumes outdated resource state";
        }
      }
    }
  }

  if (kill.isSome()) {
    sendTaskDroppedUpdate(TaskStatus::REASON_INVALID_OFFERS, *kill);

    // Refer to the comment after 'framework->removePendingTask' above
    // for why we need this.
    if (framework->idle()) {
      removeFramework(framework);
    }

    if (launchExecutor.isSome() && launchExecutor.get()) {
      // Controller expects a new executor to be launched for this task(s).
      // To keep the controller executor entries updated, the node needs to send
      // `ExitedExecutorMessage` even though no executor launched.
      sendExitedExecutorMessage(frameworkId, executorInfo.executor_id());

      // See the declaration of `taskLaunchSequences` regarding its lifecycle
      // management.
      framework->taskLaunchSequences.erase(executorInfo.executor_id());
    }

    return;
  }

  auto unallocated = [](const Resources& resources) {
    Resources result = resources;
    result.unallocate();
    return result;
  };

  CHECK_NONE(kill);

  // NOTE: If the task/task group or executor uses resources that are
  // checkpointed on the node (e.g. persistent volumes), we should
  // already know about it. If the node doesn't know about them (e.g.
  // CheckpointResourcesMessage was dropped or came out of order), we
  // send TASK_DROPPED status updates here since restarting the task
  // may succeed in the event that CheckpointResourcesMessage arrives
  // out of order.
  foreach (const TaskInfo& _task, tasks) {
    // We must unallocate the resources to check whether they are
    // contained in the unallocated total checkpointed resources.
    Resources checkpointedTaskResources =
      unallocated(_task.resources()).filter(needCheckpointing);

    foreach (const Resource& resource, checkpointedTaskResources) {
      if (!checkpointedResources.contains(resource)) {
        LOG(WARNING) << "Unknown checkpointed resource " << resource
                     << " for task " << _task
                     << " of framework " << frameworkId;

        kill =
          "The checkpointed resources being used by the task or task group are "
          "unknown to the node";
        break;
      }
    }
  }

  if (kill.isSome()) {
    sendTaskDroppedUpdate(TaskStatus::REASON_RESOURCES_UNKNOWN, *kill);

    // Refer to the comment after 'framework->removePendingTask' above
    // for why we need this.
    if (framework->idle()) {
      removeFramework(framework);
    }

    if (launchExecutor.isSome() && launchExecutor.get()) {
      // Controller expects a new executor to be launched for this task(s).
      // To keep the controller executor entries updated, the node needs to send
      // `ExitedExecutorMessage` even though no executor launched.
      sendExitedExecutorMessage(frameworkId, executorInfo.executor_id());

      // See the declaration of `taskLaunchSequences` regarding its lifecycle
      // management.
      framework->taskLaunchSequences.erase(executorInfo.executor_id());
    }

    return;
  }

  CHECK_NONE(kill);

  // Refer to the comment above when looping across tasks on
  // why we need to unallocate resources.
  Resources checkpointedExecutorResources =
    unallocated(executorInfo.resources()).filter(needCheckpointing);

  foreach (const Resource& resource, checkpointedExecutorResources) {
    if (!checkpointedResources.contains(resource)) {
      LOG(WARNING) << "Unknown checkpointed resource " << resource
                   << " for executor '" << executorId
                   << "' of framework " << frameworkId;

      kill =
        "The checkpointed resources being used by the executor are unknown "
        "to the node";
      break;
    }
  }

  if (kill.isSome()) {
    sendTaskDroppedUpdate(TaskStatus::REASON_RESOURCES_UNKNOWN, *kill);

    // Refer to the comment after 'framework->removePendingTask' above
    // for why we need this.
    if (framework->idle()) {
      removeFramework(framework);
    }

    if (launchExecutor.isSome() && launchExecutor.get()) {
      // Controller expects a new executor to be launched for this task(s).
      // To keep the controller executor entries updated, the node needs to send
      // `ExitedExecutorMessage` even though no executor launched.
      sendExitedExecutorMessage(frameworkId, executorInfo.executor_id());

      // See the declaration of `taskLaunchSequences` regarding its lifecycle
      // management.
      framework->taskLaunchSequences.erase(executorInfo.executor_id());
    }

    return;
  }

  // NOTE: The node cannot be in 'RECOVERING' because the task would
  // have been rejected in 'run()' in that case.
  CHECK(state == DISCONNECTED || state == RUNNING || state == TERMINATING)
    << state;

  if (state == TERMINATING) {
    LOG(WARNING) << "Ignoring running " << taskOrTaskGroup(task, taskGroup)
                 << " of framework " << frameworkId
                 << " because the node is terminating";

    // Refer to the comment after 'framework->removePendingTask' above
    // for why we need this.
    if (framework->idle()) {
      removeFramework(framework);
    }

    // We don't send TASK_LOST or ExitedExecutorMessage here because the node
    // is terminating.
    return;
  }

  CHECK(framework->state == Framework::RUNNING) << framework->state;

  LOG(INFO) << "Launching " << taskOrTaskGroup(task, taskGroup)
            << " for framework " << frameworkId;

  Executor* executor = framework->getExecutor(executorId);

  // If launchExecutor is NONE, this is the legacy case where the controller
  // did not set the `launch_executor` flag. Executor will be launched if
  // there is none.

  if (launchExecutor.isSome()) {
    if (taskGroup.isNone() && task->has_command()) {
      // We are dealing with command task; a new command executor will be
      // launched.
      // It is possible for an executor with this ID to already exist, if the
      // TaskID was re-used - see MESOS-9657. If this happens, we have no
      // choice but to drop the task.
      if (executor != nullptr) {
        sendTaskDroppedUpdate(
            TaskStatus::REASON_TASK_INVALID,
            "Cannot reuse an already existing executor for a command task");

        return;
      }
    } else {
      // Controller set the `launch_executor` flag and this is not a command task.
      if (launchExecutor.get() && executor != nullptr) {
        // Controller requests launching executor but an executor still exits
        // on the node. In this case we will drop tasks. This could happen if
        // the executor is already terminated on the node (and node has sent
        // out the `ExitedExecutorMessage` and it was received by the controller)
        // but the node is still waiting for all the status updates to be
        // acked before removing the executor struct.

        sendTaskDroppedUpdate(
            TaskStatus::REASON_EXECUTOR_TERMINATED,
            "Controller wants to launch executor, but one already exists");

        // Controller expects a new executor to be launched for this task(s).
        // To keep the controller executor entries updated, the node needs to
        // send `ExitedExecutorMessage` even though no executor launched.
        if (executor->state == Executor::TERMINATED) {
          sendExitedExecutorMessage(frameworkId, executorInfo.executor_id());
        } else {
          // This could happen if the following sequence of events happen:
          //
          //  (1) Controller sends `runTaskMessage` to node with
          //      `launch_executor = true`;
          //
          //  (2) Before the node got the `runTaskMessage`, it reconnects and
          //      reconciles with the controller. Controller then removes the executor
          //      entry it asked the node to launch in step (1);
          //
          //  (3) Node got the `runTaskMessage` sent in step (1), launches
          //      the task and the executor (that the controller does not know
          //      about).
          //
          //  (4) Controller now sends another `runTaskMessage` for the same
          //      executor id with `launch_executor = true`.
          //
          // The node ends up with a lingering executor that the controller does
          // not know about. We will shutdown the executor.
          //
          // TODO(mzhu): This could be avoided if the node can
          // tell whether the controller's message was sent before or after the
          // reconnection and discard the message in the former case.
          //
          // TODO(mzhu): Controller needs to do proper executor reconciliation
          // with the node to avoid this from happening.
          _shutdownExecutor(framework, executor);
        }

        return;
      }

      if (!launchExecutor.get() && executor == nullptr) {
        // Controller wants no new executor launched and there is none running on
        // the node. This could happen if the task expects some previous
        // tasks to launch the executor. However, the earlier task got killed
        // or dropped hence did not launch the executor but the controller doesn't
        // know about it yet because the `ExitedExecutorMessage` is still in
        // flight. In this case, we will drop the task.

        sendTaskDroppedUpdate(
            TaskStatus::REASON_EXECUTOR_TERMINATED,
            "No executor is expected to launch and there is none running");

        // We do not send `ExitedExecutorMessage` here because the expectation
        // is that there is already one on the fly to controller. If the message
        // gets dropped, we will hopefully reconcile with the controller later.

        return;
      }
    }
  }

  // Either the controller explicitly requests launching a new executor
  // or we are in the legacy case of launching one if there wasn't
  // one already. Either way, let's launch executor now.
  if (executor == nullptr) {
    Try<Executor*> added =
      framework->addExecutor(executorInfo, executorGeneratedForCommandTask);

    if (added.isError()) {
      CHECK(framework->getExecutor(executorId) == nullptr);

      sendTaskDroppedUpdate(
          TaskStatus::REASON_EXECUTOR_TERMINATED,
          added.error());

      // Refer to the comment after 'framework->removePendingTask' above
      // for why we need this.
      if (framework->idle()) {
        removeFramework(framework);
      }

      if (launchExecutor.isSome() && launchExecutor.get()) {
        // Controller expects a new executor to be launched for this task(s).
        // To keep the controller executor entries updated, the node needs to send
        // `ExitedExecutorMessage` even though no executor launched.
        sendExitedExecutorMessage(frameworkId, executorInfo.executor_id());

        // See the declaration of `taskLaunchSequences` regarding its lifecycle
        // management.
        framework->taskLaunchSequences.erase(executorInfo.executor_id());
      }

      return;
    }

    executor = added.get();

    // NOTE: We make a copy of the executor info because we may mutate it with
    // some default fields and resources.
    ExecutorInfo executorInfo_ = executorInfo;

    // Populate the command info for default executor. We modify the executor
    // info to avoid resetting command info upon reregistering with the controller
    // since the controller doesn't store them; they are generated by the node.
    if (executorInfo_.has_type() &&
        executorInfo_.type() == ExecutorInfo::DEFAULT) {
      CHECK(!executorInfo_.has_command());

      *executorInfo_.mutable_command() =
        defaultExecutorCommandInfo(flags.launcher_dir, executor->user);
    }

    // We modify the ExecutorInfo to include the task/task group's resources
    // when launching the executor so that the containerizer has non-zero
    // resources to work with when the executor has no resources. And this is
    // also helpful for the executor to have enough resources to start because
    // usually the resources that framework gives to executor are pretty small
    // (e.g., Marathon/medios-execute give 0.1 CPUs to the default executor) so
    // the executor may be throttled by CFS, see MESOS-9925 for details.
    Resources tasksResources;
    foreach (const TaskInfo& _task, tasks) {
      tasksResources += _task.resources();
    }

    *executorInfo_.mutable_resources() =
      Resources(executorInfo.resources()) + tasksResources;

    // Add the default container info to the executor info.
    // TODO(jieyu): Rename the flag to be default_medios_container_info.
    if (!executorInfo_.has_container() &&
        flags.default_container_info.isSome()) {
      *executorInfo_.mutable_container() = flags.default_container_info.get();
    }

    publishResources(executor->containerId, executorInfo_.resources())
      .then(defer(
          self(),
          &Self::generateSecret,
          frameworkId,
          executorId,
          executor->containerId))
      .onAny(defer(
          self(),
          &Self::launchExecutor,
          lambda::_1,
          frameworkId,
          executorInfo_,
          computeExecutorLimits(executorInfo.resources(), tasks),
          taskGroup.isNone() ? task.get() : Option<TaskInfo>::none()));
  }

  CHECK_NOTNULL(executor);

  switch (executor->state) {
    case Executor::TERMINATING:
    case Executor::TERMINATED: {
      string executorState;

      if (executor->state == Executor::TERMINATING) {
        executorState = "terminating";
      } else {
        executorState = "terminated";
      }

      LOG(WARNING) << "Asked to run " << taskOrTaskGroup(task, taskGroup)
                   << "' for framework " << frameworkId
                   << " with executor '" << executorId
                   << "' which is " << executorState;

      // We report TASK_DROPPED to the framework because the task was
      // never launched. For non-partition-aware frameworks, we report
      // TASK_LOST for backward compatibility.
      medios::TaskState taskState = TASK_DROPPED;
      if (!protobuf::frameworkHasCapability(
              frameworkInfo, FrameworkInfo::Capability::PARTITION_AWARE)) {
        taskState = TASK_LOST;
      }

      foreach (const TaskInfo& _task, tasks) {
        const StatusUpdate update = protobuf::createStatusUpdate(
            frameworkId,
            info.id(),
            _task.task_id(),
            taskState,
            TaskStatus::SOURCE_NODE,
            id::UUID::random(),
            "Executor " + executorState,
            TaskStatus::REASON_EXECUTOR_TERMINATED);

        statusUpdate(update, UPID());
      }

      break;
    }
    case Executor::REGISTERING:
      if (executor->checkpoint) {
        foreach (const TaskInfo& _task, tasks) {
          executor->checkpointTask(_task);
        }
      }

      if (taskGroup.isSome()) {
        executor->enqueueTaskGroup(taskGroup.get());
      } else {
        foreach (const TaskInfo& _task, tasks) {
          executor->enqueueTask(_task);
        }
      }

      LOG(INFO) << "Queued " << taskOrTaskGroup(task, taskGroup)
                << " for executor " << *executor;

      break;
    case Executor::RUNNING: {
      if (executor->checkpoint) {
        foreach (const TaskInfo& _task, tasks) {
          executor->checkpointTask(_task);
        }
      }

      // Queue tasks until the containerizer is updated
      // with new resource limits (MESOS-998).
      if (taskGroup.isSome()) {
        executor->enqueueTaskGroup(taskGroup.get());
      } else {
        foreach (const TaskInfo& _task, tasks) {
          executor->enqueueTask(_task);
        }
      }

      LOG(INFO) << "Queued " << taskOrTaskGroup(task, taskGroup)
                << " for executor " << *executor;

      const ContainerID& containerId = executor->containerId;
      const Resources& resourceRequests = executor->allocatedResources();
      const google::protobuf::Map<string, Value::Scalar>& resourceLimits =
        computeExecutorLimits(
            executor->info.resources(),
            executor->queuedTasks.values(),
            executor->launchedTasks.values());

      publishResources(containerId, resourceRequests)
        .then(defer(
            self(),
            [this, containerId, resourceRequests, resourceLimits] {
              // NOTE: The executor struct could have been removed before
              // containerizer update, so we use the captured container ID,
              // resource requests and limits here. If this happens, the
              // containerizer would simply skip updating a destroyed container.
              return containerizer->update(
                  containerId, resourceRequests, resourceLimits);
        }))
        .onAny(defer(self(),
                     &Self::___run,
                     lambda::_1,
                     frameworkId,
                     executorId,
                     executor->containerId,
                     task.isSome()
                       ? vector<TaskInfo>({task.get()})
                       : vector<TaskInfo>(),
                     taskGroup.isSome()
                       ? vector<TaskGroupInfo>({taskGroup.get()})
                       : vector<TaskGroupInfo>()));

      break;
    }
    default:
      LOG(FATAL) << "Executor " << *executor << " is in unexpected state "
                 << executor->state;
      break;
  }

  // We don't perform the checks for 'removeFramework' here since
  // we're guaranteed by 'addExecutor' that 'framework->executors'
  // will be non-empty.
  CHECK(!framework->executors.empty());
}


void Node::___run(
    const Future<Nothing>& future,
    const FrameworkID& frameworkId,
    const ExecutorID& executorId,
    const ContainerID& containerId,
    const vector<TaskInfo>& tasks,
    const vector<TaskGroupInfo>& taskGroups)
{
  if (!future.isReady()) {
    LOG(ERROR) << "Failed to update resources for container " << containerId
               << " of executor '" << executorId
               << "' of framework " << frameworkId
               << ", destroying container: "
               << (future.isFailed() ? future.failure() : "discarded");

    containerizer->destroy(containerId);

    Executor* executor = getExecutor(frameworkId, executorId);
    if (executor != nullptr) {
      Framework* framework = getFramework(frameworkId);
      CHECK_NOTNULL(framework);

      // Send TASK_GONE because the task was started but has now
      // been terminated. If the framework is not partition-aware,
      // we send TASK_LOST instead for backward compatibility.
      medios::TaskState taskState = TASK_GONE;
      if (!framework->capabilities.partitionAware) {
        taskState = TASK_LOST;
      }

      ContainerTermination termination;
      termination.set_state(taskState);
      termination.set_reason(TaskStatus::REASON_CONTAINER_UPDATE_FAILED);
      termination.set_message(
          "Failed to update resources for container: " +
          (future.isFailed() ? future.failure() : "discarded"));

      executor->pendingTermination = termination;

      // TODO(jieyu): Set executor->state to be TERMINATING.
    }

    return;
  }

  // Needed for logging.
  auto tasksAndTaskGroups = [&tasks, &taskGroups]() {
    ostringstream out;
    if (!tasks.empty()) {
      vector<TaskID> taskIds;
      foreach (const TaskInfo& task, tasks) {
        taskIds.push_back(task.task_id());
      }
      out << "tasks " << stringify(taskIds);
    }

    if (!taskGroups.empty()) {
      if (!tasks.empty()) {
        out << " and ";
      }

      out << "task groups ";

      vector<vector<TaskID>> taskIds;
      for (auto it = taskGroups.begin(); it != taskGroups.end(); it++) {
        vector<TaskID> taskIds_;
        foreach (const TaskInfo& task, (*it).tasks()) {
          taskIds_.push_back(task.task_id());
        }
        taskIds.push_back(taskIds_);
      }

      out << stringify(taskIds);
    }

    return out.str();
  };

  Framework* framework = getFramework(frameworkId);
  if (framework == nullptr) {
    LOG(WARNING) << "Ignoring sending queued " << tasksAndTaskGroups()
                 << " to executor '" << executorId
                 << "' of framework " << frameworkId
                 << " because the framework does not exist";
    return;
  }

  // No need to send the task to the executor because the framework is
  // being shutdown. No need to send status update for the task as
  // well because the framework is terminating!
  if (framework->state == Framework::TERMINATING) {
    LOG(WARNING) << "Ignoring sending queued " << tasksAndTaskGroups()
                 << " to executor '" << executorId
                 << "' of framework " << frameworkId
                 << " because the framework is terminating";
    return;
  }

  Executor* executor = framework->getExecutor(executorId);
  if (executor == nullptr) {
    LOG(WARNING) << "Ignoring sending queued " << tasksAndTaskGroups()
                 << " to executor '" << executorId
                 << "' of framework " << frameworkId
                 << " because the executor does not exist";
    return;
  }

  // This is the case where the original instance of the executor has
  // been shutdown and a new instance is brought up. No need to send
  // status update as well because it should have already been sent
  // when the original instance of the executor was shutting down.
  if (executor->containerId != containerId) {
    LOG(WARNING) << "Ignoring sending queued " << tasksAndTaskGroups()
                 << "' to executor " << *executor
                 << " because the target container " << containerId
                 << " has exited";
    return;
  }

  CHECK(executor->state == Executor::RUNNING ||
        executor->state == Executor::TERMINATING ||
        executor->state == Executor::TERMINATED)
    << executor->state;

  // No need to send the task to the executor because the executor is
  // terminating or has been terminated. No need to send status update
  // for the task as well because it will be properly handled by
  // 'executorTerminated'.
  if (executor->state != Executor::RUNNING) {
    LOG(WARNING) << "Ignoring sending queued " << tasksAndTaskGroups()
                 << " to executor " << *executor
                 << " because the executor is in "
                 << executor->state << " state";
    return;
  }

  // At this point, we must have either sent some tasks to the running
  // executor or there are queued tasks that need to be delivered.
  // Otherwise, the executor state would have been synchronously
  // transitioned to TERMINATING when the queued tasks were killed.
  CHECK(executor->everSentTask() || !executor->queuedTasks.empty());

  foreach (const TaskInfo& task, tasks) {
    // This is the case where the task is killed. No need to send
    // status update because it should be handled in 'killTask'.
    if (!executor->queuedTasks.contains(task.task_id())) {
      LOG(WARNING) << "Ignoring sending queued task '" << task.task_id()
                   << "' to executor " << *executor
                   << " because the task has been killed";
      continue;
    }

    CHECK_SOME(executor->dequeueTask(task.task_id()));
    executor->addLaunchedTask(task);

    LOG(INFO) << "Sending queued task '" << task.task_id()
              << "' to executor " << *executor;

    RunTaskMessage message;
    message.mutable_framework()->MergeFrom(framework->info);
    message.mutable_task()->MergeFrom(task);

    // Note that 0.23.x executors require the 'pid' to be set
    // to decode the message, but do not use the field.
    message.set_pid(framework->pid.getOrElse(UPID()));

    executor->send(message);
  }

  foreach (const TaskGroupInfo& taskGroup, taskGroups) {
    // The invariant here is that all queued tasks in the group
    // are still queued, or all were removed due to a kill arriving
    // for one of the tasks in the group.
    bool allQueued = true;
    bool allRemoved = true;
    foreach (const TaskInfo& task, taskGroup.tasks()) {
      if (executor->queuedTasks.contains(task.task_id())) {
        allRemoved = false;
      } else {
        allQueued = false;
      }
    }

    CHECK(allQueued != allRemoved)
      << "BUG: The " << taskOrTaskGroup(None(), taskGroup)
      << " was partially killed";

    if (allRemoved) {
      // This is the case where the task group is killed. No need to send
      // status update because it should be handled in 'killTask'.
      LOG(WARNING) << "Ignoring sending queued "
                   << taskOrTaskGroup(None(), taskGroup) << " to executor "
                   << *executor << " because the task group has been killed";
      continue;
    }

    LOG(INFO) << "Sending queued " << taskOrTaskGroup(None(), taskGroup)
              << " to executor " << *executor;

    foreach (const TaskInfo& task, taskGroup.tasks()) {
      CHECK_SOME(executor->dequeueTask(task.task_id()));
      executor->addLaunchedTask(task);
    }

    executor::Event event;
    event.set_type(executor::Event::LAUNCH_GROUP);

    executor::Event::LaunchGroup* launchGroup = event.mutable_launch_group();
    launchGroup->mutable_task_group()->CopyFrom(taskGroup);

    executor->send(event);
  }
}


Future<Option<Secret>> Node::generateSecret(
    const FrameworkID& frameworkId,
    const ExecutorID& executorId,
    const ContainerID& containerId)
{
  if (!secretGenerator) {
    return None();
  }

  Principal principal(
      Option<string>::none(),
      {
        {"fid", frameworkId.value()},
        {"eid", executorId.value()},
        {"cid", containerId.value()}
      });

  return secretGenerator->generate(principal)
    .then([](const Secret& secret) -> Future<Option<Secret>> {
      Option<Error> error = common::validation::validateSecret(secret);

      if (error.isSome()) {
        return Failure(
            "Failed to validate generated secret: " + error->message);
      } else if (secret.type() != Secret::VALUE) {
        return Failure(
            "Expecting generated secret to be of VALUE type instead of " +
            stringify(secret.type()) + " type; " +
            "only VALUE type secrets are supported at this time");
      }

      return secret;
    });
}


// Launches an executor which was previously created.
void Node::launchExecutor(
    const Future<Option<Secret>>& authenticationToken,
    const FrameworkID& frameworkId,
    const ExecutorInfo& executorInfo,
    const google::protobuf::Map<string, Value::Scalar>& executorLimits,
    const Option<TaskInfo>& taskInfo)
{
  Framework* framework = getFramework(frameworkId);
  if (framework == nullptr) {
    LOG(WARNING) << "Ignoring launching executor '"
                 << executorInfo.executor_id() << "' because the framework "
                 << frameworkId << " does not exist";
    return;
  }

  if (framework->state == Framework::TERMINATING) {
    LOG(WARNING) << "Ignoring launching executor '"
                 << executorInfo.executor_id() << "' of framework "
                 << frameworkId << " because the framework is terminating";
    return;
  }

  Executor* executor = framework->getExecutor(executorInfo.executor_id());
  if (executor == nullptr) {
    LOG(WARNING) << "Ignoring launching executor '"
                 << executorInfo.executor_id() << "' of framework "
                 << frameworkId << " because the executor does not exist";
    return;
  }

  if (executor->state == Executor::TERMINATING ||
      executor->state == Executor::TERMINATED) {
    string executorState;
    if (executor->state == Executor::TERMINATING) {
      executorState = "terminating";
    } else {
      executorState = "terminated";
    }

    LOG(WARNING) << "Ignoring launching executor " << *executor
                 << " in container " << executor->containerId
                 << " because the executor is " << executorState;

    // The framework may have shutdown this executor already, transitioning it
    // to the TERMINATING/TERMINATED state. However, the executor still exists
    // in the node's map, so we must send status updates for any queued tasks
    // and perform cleanup via `executorTerminated`.
    ContainerTermination termination;
    termination.set_state(TASK_FAILED);
    termination.set_reason(TaskStatus::REASON_CONTAINER_LAUNCH_FAILED);
    termination.set_message("Executor " + executorState);

    executorTerminated(frameworkId, executor->id, termination);

    return;
  }

  CHECK_EQ(Executor::REGISTERING, executor->state);

  if (!authenticationToken.isReady()) {
    const string message = "Secret generation failed: " +
      (authenticationToken.isFailed()
         ? authenticationToken.failure() : "future discarded");

    LOG(ERROR) << "Failed to launch executor " << *executor << " in container "
               << executor->containerId << ": " << message;

    ContainerTermination termination;
    termination.set_state(TASK_FAILED);
    termination.set_reason(TaskStatus::REASON_CONTAINER_LAUNCH_FAILED);
    termination.set_message(message);

    executorTerminated(frameworkId, executor->id, termination);

    return;
  }

  // Tell the containerizer to launch the executor.

  // Bundle all the container launch fields together.
  ContainerConfig containerConfig;
  *containerConfig.mutable_executor_info() = executorInfo;
  *containerConfig.mutable_command_info() = executorInfo.command();
  *containerConfig.mutable_resources() = executorInfo.resources();
  containerConfig.set_directory(executor->directory);

  if (!executorLimits.empty()) {
    *containerConfig.mutable_limits() = executorLimits;
  }

  if (executor->user.isSome()) {
    containerConfig.set_user(executor->user.get());
  }

  // For both of the following cases, `ExecutorInfo.container` is what
  // we want to tell the containerizer about the container to be
  // launched:
  // (1) If this is a command task case (i.e., the framework specifies
  //     the `TaskInfo` but not `ExecutorInfo`), the
  //     `ExecutorInfo.container` is already copied from
  //     `TaskInfo.container` in `Node::getExecutorInfo`. As a
  //     result, we should just inform the containerizer about
  //     `ExecutorInfo.container`.
  // (2) If this is a non command task (e.g., default executor, custom
  //     executor), the `ExecutorInfo.container` is what we want to
  //     tell the containerizer anyway.
  if (executorInfo.has_container()) {
    *containerConfig.mutable_container_info() = executorInfo.container();
  }

  if (executor->isGeneratedForCommandTask()) {
    CHECK_SOME(taskInfo)
      << "Command (or Docker) executor does not support task group";

    containerConfig.mutable_task_info()->CopyFrom(taskInfo.get());
  }

  // Prepare environment variables for the executor.
  map<string, string> environment = executorEnvironment(
      flags,
      executorInfo,
      executor->directory,
      info.id(),
      self(),
      authenticationToken.get(),
      framework->info.checkpoint());

  // Prepare the filename of the pidfile, for checkpoint-enabled frameworks.
  Option<string> pidCheckpointPath = None();
  if (framework->info.checkpoint()){
    pidCheckpointPath = node::paths::getForkedPidPath(
        node::paths::getMetaRootDir(flags.work_dir),
        info.id(),
        framework->id(),
        executor->id,
        executor->containerId);
  }

  LOG(INFO) << "Launching container " << executor->containerId
            << " for executor '" << executor->id
            << "' of framework " << framework->id();

  // Launch the container.
  //
  // NOTE: This must be called synchronously to avoid launching a container for
  // a removed executor.
  containerizer->launch(
      executor->containerId, containerConfig, environment, pidCheckpointPath)
    .onAny(defer(
        self(),
        &Self::executorLaunched,
        frameworkId,
        executor->id,
        executor->containerId,
        lambda::_1));

  // Make sure the executor registers within the given timeout.
  delay(flags.executor_registration_timeout,
        self(),
        &Self::registerExecutorTimeout,
        frameworkId,
        executor->id,
        executor->containerId);

  return;
}


void Node::handleRunTaskGroupMessage(
    const UPID& from,
    RunTaskGroupMessage&& runTaskGroupMessage)
{
  runTaskGroup(
      from,
      runTaskGroupMessage.framework(),
      runTaskGroupMessage.executor(),
      runTaskGroupMessage.task_group(),
      google::protobuf::convert(runTaskGroupMessage.resource_version_uuids()),
      runTaskGroupMessage.has_launch_executor() ?
          Option<bool>(runTaskGroupMessage.launch_executor()) : None());
}


void Node::runTaskGroup(
    const UPID& from,
    const FrameworkInfo& frameworkInfo,
    const ExecutorInfo& executorInfo,
    const TaskGroupInfo& taskGroupInfo,
    const vector<ResourceVersionUUID>& resourceVersionUuids,
    const Option<bool>& launchExecutor)
{
  if (controller != from) {
    LOG(WARNING) << "Ignoring run task group message from " << from
                 << " because it is not the expected controller: "
                 << (controller.isSome() ? stringify(controller.get()) : "None");
    return;
  }

  if (!frameworkInfo.has_id()) {
    LOG(ERROR) << "Ignoring run task group message from " << from
               << " because it does not have a framework ID";
    return;
  }

  // TODO(mzhu): Consider doing a `CHECK` here since this shouldn't be possible.
  if (taskGroupInfo.tasks().empty()) {
    LOG(ERROR) << "Ignoring run task group message from " << from
               << " for framework " << frameworkInfo.id()
               << " because it has no tasks";

    return;
  }

  // Executors for task groups are injected by the controller, not the node.
  constexpr bool executorGeneratedForCommandTask = false;

  run(frameworkInfo,
      executorInfo,
      None(),
      taskGroupInfo,
      resourceVersionUuids,
      UPID(),
      launchExecutor,
      executorGeneratedForCommandTask);
}


void Node::killTask(
    const UPID& from,
    const KillTaskMessage& killTaskMessage)
{
  if (controller != from) {
    LOG(WARNING) << "Ignoring kill task message from " << from
                 << " because it is not the expected controller: "
                 << (controller.isSome() ? stringify(controller.get()) : "None");
    return;
  }

  const FrameworkID& frameworkId = killTaskMessage.framework_id();
  const TaskID& taskId = killTaskMessage.task_id();

  LOG(INFO) << "Asked to kill task " << taskId
            << " of framework " << frameworkId;

  CHECK(state == RECOVERING || state == DISCONNECTED ||
        state == RUNNING || state == TERMINATING)
    << state;

  // TODO(bmahler): Also ignore if we're DISCONNECTED.
  if (state == RECOVERING || state == TERMINATING) {
    LOG(WARNING) << "Cannot kill task " << taskId
                 << " of framework " << frameworkId
                 << " because the node is " << state;
    // TODO(vinod): Consider sending a TASK_LOST here.
    // Currently it is tricky because 'statusUpdate()'
    // ignores updates for unknown frameworks.
    return;
  }

  Framework* framework = getFramework(frameworkId);
  if (framework == nullptr) {
    LOG(WARNING) << "Ignoring kill task " << taskId
                 << " of framework " << frameworkId
                 << " because no such framework is running";
    return;
  }

  // We don't send a status update here because a terminating
  // framework cannot send acknowledgements.
  if (framework->state == Framework::TERMINATING) {
    LOG(WARNING) << "Ignoring kill task " << taskId
                 << " of framework " << frameworkId
                 << " because the framework is terminating";
    return;
  }

  CHECK(framework->state == Framework::RUNNING) << framework->state;

  if (framework->isPending(taskId)) {
    killPendingTask(frameworkId, framework, taskId);

    return;
  }

  Executor* executor = framework->getExecutor(taskId);
  if (executor == nullptr) {
    LOG(WARNING) << "Cannot kill task " << taskId
                 << " of framework " << frameworkId
                 << " because no corresponding executor is running";

    // We send a TASK_DROPPED update because this task has never been
    // launched on this node. If the framework is not partition-aware,
    // we send TASK_LOST for backward compatibility.
    medios::TaskState taskState = TASK_DROPPED;
    if (!framework->capabilities.partitionAware) {
      taskState = TASK_LOST;
    }

    const StatusUpdate update = protobuf::createStatusUpdate(
        frameworkId,
        info.id(),
        taskId,
        taskState,
        TaskStatus::SOURCE_NODE,
        id::UUID::random(),
        "Cannot find executor",
        TaskStatus::REASON_EXECUTOR_TERMINATED);

    statusUpdate(update, UPID());
    return;
  }

  kill(frameworkId,
       framework,
       executor,
       taskId,
       (killTaskMessage.has_kill_policy()
          ? killTaskMessage.kill_policy()
          : Option<KillPolicy>::none()));
}


void Node::killPendingTask(
    const FrameworkID& frameworkId,
    Framework* framework,
    const TaskID& taskId)
{
  LOG(WARNING) << "Killing task " << taskId
               << " of framework " << frameworkId
               << " before it was launched";

  Option<TaskGroupInfo> taskGroup =
    framework->getTaskGroupForPendingTask(taskId);

  vector<StatusUpdate> updates;
  if (taskGroup.isSome()) {
    foreach (const TaskInfo& task, taskGroup->tasks()) {
      updates.push_back(protobuf::createStatusUpdate(
          frameworkId,
          info.id(),
          task.task_id(),
          TASK_KILLED,
          TaskStatus::SOURCE_NODE,
          id::UUID::random(),
          "A task within the task group was killed before"
          " delivery to the executor",
          TaskStatus::REASON_TASK_KILLED_DURING_LAUNCH,
          CHECK_NOTNONE(
              framework->getExecutorIdForPendingTask(task.task_id()))));
    }
  } else {
    updates.push_back(protobuf::createStatusUpdate(
        frameworkId,
        info.id(),
        taskId,
        TASK_KILLED,
        TaskStatus::SOURCE_NODE,
        id::UUID::random(),
        "Killed before delivery to the executor",
        TaskStatus::REASON_TASK_KILLED_DURING_LAUNCH,
        CHECK_NOTNONE(
            framework->getExecutorIdForPendingTask(taskId))));
  }

  foreach (const StatusUpdate& update, updates) {
    // NOTE: Sending a terminal update (TASK_KILLED) synchronously
    // removes the task/task group from 'framework->pendingTasks'
    // and 'framework->pendingTaskGroups', so that it will not be
    // launched.
    statusUpdate(update, UPID());
  }
}


void Node::kill(
    const FrameworkID& frameworkId,
    Framework* framework,
    Executor* executor,
    const TaskID& taskId,
    const Option<KillPolicy>& killPolicy)
{
  // This function should only be called on tasks which are queued or launched,
  // so both the framework and executor should always exist.
  CHECK_NOTNULL(framework);
  CHECK_NOTNULL(executor);

  switch (executor->state) {
    case Executor::REGISTERING: {
      LOG(WARNING) << "Transitioning the state of task " << taskId
                   << " of framework " << frameworkId
                   << " to TASK_KILLED because the executor is not registered";

      // This task might be part of a task group. If so, we need to
      // send a TASK_KILLED update for all tasks in the group.
      Option<TaskGroupInfo> taskGroup = executor->getQueuedTaskGroup(taskId);

      vector<StatusUpdate> updates;
      if (taskGroup.isSome()) {
        foreach (const TaskInfo& task, taskGroup->tasks()) {
          updates.push_back(protobuf::createStatusUpdate(
              frameworkId,
              info.id(),
              task.task_id(),
              TASK_KILLED,
              TaskStatus::SOURCE_NODE,
              id::UUID::random(),
              "A task within the task group was killed before"
              " delivery to the executor",
              TaskStatus::REASON_TASK_KILLED_DURING_LAUNCH,
              executor->id));
        }
      } else {
        updates.push_back(protobuf::createStatusUpdate(
            frameworkId,
            info.id(),
            taskId,
            TASK_KILLED,
            TaskStatus::SOURCE_NODE,
            id::UUID::random(),
            "Killed before delivery to the executor",
            TaskStatus::REASON_TASK_KILLED_DURING_LAUNCH,
            executor->id));
      }

      foreach (const StatusUpdate& update, updates) {
        // NOTE: Sending a terminal update (TASK_KILLED) removes the
        // task/task group from 'executor->queuedTasks' and
        // 'executor->queuedTaskGroup', so that if the executor registers at
        // a later point in time, it won't get this task or task group.
        statusUpdate(update, UPID());
      }

      // TODO(mzhu): Consider shutting down the executor here
      // if all of its initial tasks are killed rather than
      // waiting for it to register.

      break;
    }
    case Executor::TERMINATING:
      LOG(WARNING) << "Ignoring kill task " << taskId
                   << " because the executor " << *executor
                   << " is terminating";
      break;
    case Executor::TERMINATED:
      LOG(WARNING) << "Ignoring kill task " << taskId
                   << " because the executor " << *executor
                   << " is terminated";
      break;
    case Executor::RUNNING: {
      if (executor->queuedTasks.contains(taskId)) {
        // This is the case where the task has not yet been sent to
        // the executor (e.g., waiting for containerizer update to
        // finish).

        // This task might be part of a task group. If so, we need to
        // send a TASK_KILLED update for all the other tasks.
        Option<TaskGroupInfo> taskGroup = executor->getQueuedTaskGroup(taskId);

        vector<StatusUpdate> updates;
        if (taskGroup.isSome()) {
          foreach (const TaskInfo& task, taskGroup->tasks()) {
            updates.push_back(protobuf::createStatusUpdate(
                frameworkId,
                info.id(),
                task.task_id(),
                TASK_KILLED,
                TaskStatus::SOURCE_NODE,
                id::UUID::random(),
                "Killed before delivery to the executor",
                TaskStatus::REASON_TASK_KILLED_DURING_LAUNCH,
                executor->id));
          }
        } else {
          updates.push_back(protobuf::createStatusUpdate(
              frameworkId,
              info.id(),
              taskId,
              TASK_KILLED,
              TaskStatus::SOURCE_NODE,
              id::UUID::random(),
              "Killed before delivery to the executor",
              TaskStatus::REASON_TASK_KILLED_DURING_LAUNCH,
              executor->id));
        }

        foreach (const StatusUpdate& update, updates) {
          // NOTE: Sending a terminal update (TASK_KILLED) removes the
          // task/task group from 'executor->queuedTasks' and
          // 'executor->queuedTaskGroup', so that if the executor registers at
          // a later point in time, it won't get this task.
          statusUpdate(update, UPID());
        }

        // Shutdown the executor if all of its initial tasks are killed.
        // See MESOS-8411. This is a workaround for those executors (e.g.,
        // command executor, default executor) that do not have a proper
        // self terminating logic when they haven't received the task or
        // task group within a timeout.
        if (!executor->everSentTask() && executor->queuedTasks.empty()) {
          LOG(WARNING) << "Shutting down executor " << *executor
                       << " because it has never been sent a task and all of"
                       << " its queued tasks have been killed before delivery";

          _shutdownExecutor(framework, executor);
        }
      } else {
        // Send a message to the executor and wait for
        // it to send us a status update.
        KillTaskMessage message;
        message.mutable_framework_id()->MergeFrom(frameworkId);
        message.mutable_task_id()->MergeFrom(taskId);
        if (killPolicy.isSome()) {
          message.mutable_kill_policy()->MergeFrom(killPolicy.get());
        }

        executor->send(message);
      }
      break;
    }
    default:
      LOG(FATAL) << "Executor " << *executor << " is in unexpected state "
                 << executor->state;
      break;
  }
}


// TODO(benh): Consider sending a boolean that specifies if the
// shut down should be graceful or immediate. Likewise, consider
// sending back a shut down acknowledgement, because otherwise you
// could get into a state where a shut down was sent, dropped, and
// therefore never processed.
void Node::shutdownFramework(
    const UPID& from,
    const FrameworkID& frameworkId)
{
  // Allow shutdownFramework() only if
  // its called directly (e.g. Node::finalize()) or
  // its a message from the currently registered controller.
  if (from && controller != from) {
    LOG(WARNING) << "Ignoring shutdown framework message for " << frameworkId
                 << " from " << from
                 << " because it is not from the registered controller ("
                 << (controller.isSome() ? stringify(controller.get()) : "None") << ")";
    return;
  }

  VLOG(1) << "Asked to shut down framework " << frameworkId
          << " by " << from;

  CHECK(state == RECOVERING || state == DISCONNECTED ||
        state == RUNNING || state == TERMINATING)
    << state;

  if (state == RECOVERING || state == DISCONNECTED) {
    LOG(WARNING) << "Ignoring shutdown framework message for " << frameworkId
                 << " because the node has not yet registered with the controller";
    return;
  }

  Framework* framework = getFramework(frameworkId);
  if (framework == nullptr) {
    VLOG(1) << "Cannot shut down unknown framework " << frameworkId;
    return;
  }

  switch (framework->state) {
    case Framework::TERMINATING:
      LOG(WARNING) << "Ignoring shutdown framework " << framework->id()
                   << " because it is terminating";
      break;
    case Framework::RUNNING:
      LOG(INFO) << "Shutting down framework " << framework->id();

      framework->state = Framework::TERMINATING;

      // Shut down all executors of this framework.
      // NOTE: We use 'executors.keys()' here because 'shutdownExecutor'
      // and 'removeExecutor' can remove an executor from 'executors'.
      foreach (const ExecutorID& executorId, framework->executors.keys()) {
        Executor* executor = framework->executors[executorId];
        CHECK(executor->state == Executor::REGISTERING ||
              executor->state == Executor::RUNNING ||
              executor->state == Executor::TERMINATING ||
              executor->state == Executor::TERMINATED)
          << executor->state;

        if (executor->state == Executor::REGISTERING ||
            executor->state == Executor::RUNNING) {
          _shutdownExecutor(framework, executor);
        } else if (executor->state == Executor::TERMINATED) {
          // NOTE: We call remove here to ensure we can remove an
          // executor (of a terminating framework) that is terminated
          // but waiting for acknowledgements.
          removeExecutor(framework, executor);
        } else {
          // Executor is terminating. Ignore.
        }
      }

      // Remove this framework if it has no pending executors and tasks.
      if (framework->idle()) {
        removeFramework(framework);
      }
      break;
    default:
      LOG(FATAL) << "Framework " << frameworkId
                 << " is in unexpected state " << framework->state;
      break;
  }
}


void Node::schedulerMessage(
    const NodeID& nodeId,
    const FrameworkID& frameworkId,
    const ExecutorID& executorId,
    const string& data)
{
  CHECK(state == RECOVERING || state == DISCONNECTED ||
        state == RUNNING || state == TERMINATING)
    << state;

  if (state != RUNNING) {
    LOG(WARNING) << "Dropping message from framework " << frameworkId
                 << " because the node is in " << state << " state";
    metrics.invalid_framework_messages++;
    return;
  }


  Framework* framework = getFramework(frameworkId);
  if (framework == nullptr) {
    LOG(WARNING) << "Dropping message from framework " << frameworkId
                 << " because framework does not exist";
    metrics.invalid_framework_messages++;
    return;
  }

  CHECK(framework->state == Framework::RUNNING ||
        framework->state == Framework::TERMINATING)
    << framework->state;

  if (framework->state == Framework::TERMINATING) {
    LOG(WARNING) << "Dropping message from framework " << frameworkId
                 << " because framework is terminating";
    metrics.invalid_framework_messages++;
    return;
  }

  Executor* executor = framework->getExecutor(executorId);
  if (executor == nullptr) {
    LOG(WARNING) << "Dropping message for executor " << executorId
                 << " because executor does not exist";
    metrics.invalid_framework_messages++;
    return;
  }

  switch (executor->state) {
    case Executor::REGISTERING:
    case Executor::TERMINATING:
    case Executor::TERMINATED:
      // TODO(*): If executor is not yet registered, queue framework
      // message? It's probably okay to just drop it since frameworks
      // can have the executor send a message to the controller to say when
      // it's ready.
      LOG(WARNING) << "Dropping message for executor " << *executor
                   << " because executor is not running";
      metrics.invalid_framework_messages++;
      break;
    case Executor::RUNNING: {
      FrameworkToExecutorMessage message;
      message.mutable_node_id()->MergeFrom(nodeId);
      message.mutable_framework_id()->MergeFrom(frameworkId);
      message.mutable_executor_id()->MergeFrom(executorId);
      message.set_data(data);
      executor->send(message);
      metrics.valid_framework_messages++;
      break;
    }
    default:
      LOG(FATAL) << "Executor " << *executor << " is in unexpected state "
                 << executor->state;
      break;
  }
}


void Node::updateFramework(
    const UpdateFrameworkMessage& message)
{
  CHECK(state == RECOVERING || state == DISCONNECTED ||
        state == RUNNING || state == TERMINATING)
    << state;

  const FrameworkID& frameworkId = message.framework_id();
  const UPID& pid = message.pid();

  if (state != RUNNING) {
    LOG(WARNING) << "Dropping updateFramework message for " << frameworkId
                 << " because the node is in " << state << " state";
    metrics.invalid_framework_messages++;
    return;
  }

  Framework* framework = getFramework(frameworkId);
  if (framework == nullptr) {
    LOG(INFO) << "Ignoring info update for framework " << frameworkId
              << " because it does not exist";
    return;
  }

  switch (framework->state) {
    case Framework::TERMINATING:
      LOG(WARNING) << "Ignoring info update for framework " << frameworkId
                   << " because it is terminating";
      break;
    case Framework::RUNNING: {
      LOG(INFO) << "Updating info for framework " << frameworkId
                << (pid != UPID() ? " with pid updated to " + stringify(pid)
                                  : "");

      // The framework info was added in 1.3, so it will not be set
      // if from a controller older than 1.3.
      if (message.has_framework_info()) {
        framework->info.CopyFrom(message.framework_info());
        framework->capabilities = protobuf::framework::Capabilities(
            message.framework_info().capabilities());
      }

      if (pid == UPID()) {
        framework->pid = None();
      } else {
        framework->pid = pid;
      }

      if (framework->info.checkpoint()) {
        framework->checkpointFramework();
      }

      // Inform task status update manager to immediately resend any pending
      // updates.
      taskStatusUpdateManager->resume();

      break;
    }
    default:
      LOG(FATAL) << "Framework " << framework->id()
                << " is in unexpected state " << framework->state;
      break;
  }
}


// TODO(nfnt): Have this function return a `Result`.
void Node::checkpointResourceState(
    vector<Resource> resources,
    bool changeTotal)
{
  // TODO(jieyu): Here we assume that CheckpointResourcesMessages are
  // ordered (i.e., node receives them in the same order controller sends
  // them). This should be true in most of the cases because TCP
  // enforces in order delivery per connection. However, the ordering
  // is technically not guaranteed because controller creates multiple
  // connections to the node in some cases (e.g., persistent socket
  // to node breaks and controller uses ephemeral socket). This could
  // potentially be solved by using a version number and rejecting
  // stale messages according to the version number.
  //
  // If CheckpointResourcesMessages are delivered out-of-order, there
  // are two cases to consider:
  //  (1) If controller does not fail over, it will reconcile the state
  //      with the node if the framework later changes the
  //      checkpointed resources. Since controller is the source of truth
  //      for reservations, the inconsistency is not exposed to
  //      frameworks.
  //  (2) If controller does fail over, the node will inform the new
  //      controller about the incorrect checkpointed resources. When that
  //      happens, we expect framework to reconcile based on the
  //      offers they get.

  // An node with resource providers requires an operation feedback protocol
  // instead of simply checkpointing results by the controller. Fail hard here
  // instead of applying an incompatible message.
  const bool checkpointingResourceProviderResources = std::any_of(
      resources.begin(),
      resources.end(),
      [](const Resource& resource) { return resource.has_provider_id(); });

  CHECK(!checkpointingResourceProviderResources)
    << "Resource providers must perform their own checkpointing";

  upgradeResources(&resources);

  Resources resourcesToCheckpoint = resources;

  // Tests if the given Operation needs to be checkpointed on the node.
  //
  // The node checkpoints pending CREATE/DESTROY operations on node default
  // resources and terminal operations on node default resources that have
  // unacknowledged status updates.
  auto operationNeedsCheckpointing = [](const Operation& operation) {
    Result<ResourceProviderID> resourceProviderId =
      getResourceProviderId(operation.info());

    CHECK(!resourceProviderId.isError())
      << "Failed to get resource provider ID: "
      << resourceProviderId.error();

    if (resourceProviderId.isSome()) {
      return false;
    }

    const OperationStatus& status(operation.latest_status());

    // Creating and destroying a persistent volume isn't atomic, so non-terminal
    // CREATE/DESTROY operations on node default resources have to be
    // checkpointed to retry the creation/removal of persistent volumes.
    if (!protobuf::isTerminalState(status.state())) {
      Offer::Operation::Type type = operation.info().type();

      return type == Offer::Operation::CREATE ||
             type == Offer::Operation::DESTROY;
    }

    return status.has_uuid();
  };

  hashmap<UUID, Operation> operationsToCheckpoint;

  foreachpair (const UUID& uuid, Operation* operation, operations) {
    if (operationNeedsCheckpointing(*operation)) {
      operationsToCheckpoint.put(uuid, *operation);
    }
  }

  if (resourcesToCheckpoint == checkpointedResources &&
      operationsToCheckpoint == checkpointedOperations) {
    VLOG(1) << "Ignoring new checkpointed resources and operations identical "
            << "to the current version";
    return;
  }

  ResourceState resourceState;

  foreach (const Resource& resource, resourcesToCheckpoint) {
    resourceState.add_resources()->CopyFrom(resource);
  }

  foreach (const Operation& operation, operationsToCheckpoint.values()) {
    resourceState.add_operations()->CopyFrom(operation);
  }

  // This is a sanity check to verify that the new checkpointed
  // resources are compatible with the node resources specified
  // through the '--resources' command line flag. The resources
  // should be guaranteed compatible by the controller.
  Try<Resources> _totalResources = applyCheckpointedResources(
      info.resources(),
      resourcesToCheckpoint);

  CHECK_SOME(_totalResources)
    << "Failed to apply checkpointed resources "
    << resourcesToCheckpoint << " to node's resources "
    << info.resources();

  if (changeTotal) {
    totalResources = _totalResources.get();
  }

  // Store the target checkpoint resources. We commit the checkpoint by renaming
  // the target file only after all operations are successful. If any of the
  // operations fail, the node exits and the update to checkpointed resources
  // is re-attempted after the node restarts before node reregistration.
  //
  // Since we commit the checkpoint after all operations are successful,
  // we avoid a case of inconsistency between the controller and the node if
  // the node restarts during handling of `CheckpointResourcesMessage`.
  //
  // NOTE: Since the addition of operation feedback on the node, the resources
  // are checkpointed in two formats:
  // 1) Pre-operation-feedback, where only resources are written to a target
  //    file, then moved to the final checkpoint location once any persistent
  //    volumes have been committed to disk.
  // 2) Post-operation-feedback, where both resources and operations are written
  //    to a target file, then moved to the final checkpoint location once any
  //    persistent volumes have been committed to disk.
  //
  // Both of these formats continue to be written to disk in order to permit
  // node downgrades.

  CHECK_SOME(state::checkpoint(
      paths::getResourceStateTargetPath(metaDir),
      resourceState,
      false,
      false))
    << "Failed to checkpoint resources " << resourceState.resources()
    << " and operations " << resourceState.operations() << "to target file";

  if (resourcesToCheckpoint != checkpointedResources) {
    CHECK_SOME(state::checkpoint(
        paths::getResourcesTargetPath(metaDir),
        resourcesToCheckpoint))
      << "Failed to checkpoint resources target " << resourcesToCheckpoint;

    Try<Nothing> syncResult = syncCheckpointedResources(resourcesToCheckpoint);

    if (syncResult.isError()) {
      // Exit the node (without committing the checkpoint) on failure.
      EXIT(EXIT_FAILURE)
        << "Failed to sync checkpointed resources: "
        << syncResult.error();
    }

    // Rename the target checkpoint to the committed checkpoint.
    Try<Nothing> renameResult = os::rename(
        paths::getResourcesTargetPath(metaDir),
        paths::getResourcesInfoPath(metaDir));

    if (renameResult.isError()) {
      // Exit the node since the checkpoint could not be committed.
      EXIT(EXIT_FAILURE)
        << "Failed to checkpoint resources " << resourcesToCheckpoint
        << ": " << renameResult.error();
    }

    LOG(INFO) << "Updated checkpointed resources from "
              << checkpointedResources << " to "
              << resourcesToCheckpoint;

    checkpointedResources = std::move(resourcesToCheckpoint);
  }

  // At this point, `syncCheckpointedResources()` has ensured that any change in
  // checkpointed resources (e.g. persistent volumes) is now reflected on disk.
  // We rename the target resource state file to the actual resource state file,
  // which is our source of truth for the current state of the node resources.
  Try<Nothing> renameResult = os::rename(
      paths::getResourceStateTargetPath(metaDir),
      paths::getResourceStatePath(metaDir));

  if (renameResult.isError()) {
    // Exit the node since the checkpoint could not be committed.
    EXIT(EXIT_FAILURE)
      << "Failed to move target resources " << resourceState.resources()
      << " and operations " << resourceState.operations()
      << ": " << renameResult.error();
  }

  if (operationsToCheckpoint != checkpointedOperations) {
    LOG(INFO) << "Updated checkpointed operations from "
              << checkpointedOperations.values() << " to "
              << operationsToCheckpoint.values();

    checkpointedOperations = std::move(operationsToCheckpoint);
  }
}


void Node::checkpointResourceState(
    const Resources& resources,
    bool changeTotal)
{
  checkpointResourceState({resources.begin(), resources.end()}, changeTotal);
}


void Node::checkpointResourcesMessage(
    const vector<Resource>& resources)
{
  checkpointResourceState(resources, true);
}


Try<Nothing> Node::syncCheckpointedResources(
    const Resources& newCheckpointedResources)
{
  auto toPathMap = [](const string& workDir, const Resources& resources) {
    hashmap<string, Resource> pathMap;
    const Resources& persistentVolumes = resources.persistentVolumes();

    foreach (const Resource& volume, persistentVolumes) {
      // This is validated in controller.
      CHECK(Resources::isReserved(volume));
      string path = paths::getPersistentVolumePath(workDir, volume);
      pathMap[path] = volume;
    }

    return pathMap;
  };

  const hashmap<string, Resource> oldPathMap =
    toPathMap(flags.work_dir, checkpointedResources);

  const hashmap<string, Resource> newPathMap =
    toPathMap(flags.work_dir, newCheckpointedResources);

  const hashset<string> oldPaths = oldPathMap.keys();
  const hashset<string> newPaths = newPathMap.keys();

  const hashset<string> createPaths = newPaths - oldPaths;
  const hashset<string> deletePaths = oldPaths - newPaths;

  // Create persistent volumes that do not already exist.
  //
  // TODO(jieyu): Consider introducing a volume manager once we start
  // to support multiple disks, or raw disks. Depending on the
  // DiskInfo, we may want to create either directories under a root
  // directory, or LVM volumes from a given device.
  foreach (const string& path, createPaths) {
    const Resource& volume = newPathMap.at(path);

    // If creation of persistent volume fails, the node exits.
    string volumeDescription = "persistent volume " +
      volume.disk().persistence().id() + " at '" + path + "'";

    // We don't take any action if the directory already exists.
    // If the volume is on a MOUNT disk then the directory would
    // be a mount point that already exists. Otherwise it is possible
    // that pre-existing data exists at this path before it's managed
    // by Medios node. In any case because we make sure volume destroy
    // is retried until successful, here we are not concerned about
    // them being leaked from previous persistent volumes.
    if (!os::exists(path)) {
      // If the directory does not exist, we should proceed only if the
      // target directory is successfully created.
      Try<Nothing> result = os::mkdir(path, true);
      if (result.isError()) {
        return Error("Failed to create the " +
            volumeDescription + ": " + result.error());
      }
    }
  }

  // If a persistent volume that in the node's previous checkpointed
  // resources doesn't appear in the new checkpointed resources, this
  // implies the volume has been explicitly destroyed. We immediately
  // remove the filesystem objects for the removed volume. Note that
  // for MOUNT disks, we don't remove the root directory (mount point)
  // of the volume.
  foreach (const string& path, deletePaths) {
    const Resource& volume = oldPathMap.at(path);

    LOG(INFO) << "Deleting persistent volume '"
              << volume.disk().persistence().id()
              << "' at '" << path << "'";

    if (!os::exists(path)) {
      LOG(WARNING) << "Failed to find persistent volume '"
                   << volume.disk().persistence().id()
                   << "' at '" << path << "'";
    } else {
      const Resource::DiskInfo::Source& source = volume.disk().source();

      bool removeRoot = true;
      if (source.type() == Resource::DiskInfo::Source::MOUNT) {
        removeRoot = false;
      }

      // We should proceed only if the directory is removed.
      Try<Nothing> result = os::rmdir(path, true, removeRoot);
      if (result.isError()) {
        return Error(
            "Failed to remove persistent volume '" +
            stringify(volume.disk().persistence().id()) +
            "' at '" + path + "': " + result.error());
      }
    }

    // Deallocate the shared persistent volume's gid. Please note that the
    // gid is allocated when the shared persistent volume is first used by
    // a container rather than when it is created.
    if (volumeGidManager) {
      volumeGidManager->deallocate(path);
    }
  }

  return Nothing();
}


void Node::applyOperation(const ApplyOperationMessage& message)
{
  // The operation might be from an operator API call, thus the framework ID
  // here is optional.
  Option<FrameworkID> frameworkId = message.has_framework_id()
    ? message.framework_id()
    : Option<FrameworkID>::none();

  Option<OperationID> operationId = message.operation_info().has_id()
    ? message.operation_info().id()
    : Option<OperationID>::none();

  Result<ResourceProviderID> resourceProviderId =
    getResourceProviderId(message.operation_info());

  const UUID& uuid = message.operation_uuid();

  if (resourceProviderId.isError()) {
    LOG(ERROR) << "Failed to get the resource provider ID of operation "
               << "'" << message.operation_info().id() << "' "
               << "(uuid: " << uuid << ") from "
               << (frameworkId.isSome()
                     ? "framework " + stringify(frameworkId.get())
                     : "an operator API call")
               << ": " << resourceProviderId.error();
    return;
  }

  Operation* operation = new Operation(protobuf::createOperation(
      message.operation_info(),
      protobuf::createOperationStatus(
          OPERATION_PENDING,
          operationId,
          None(),
          None(),
          None(),
          info.id(),
          resourceProviderId.isSome()
            ? resourceProviderId.get() : Option<ResourceProviderID>::none()),
      frameworkId,
      info.id(),
      uuid));

  addOperation(operation);

  // TODO(jieyu): We should drop the operation if the resource version
  // uuid in the operation does not match that of the node. This is
  // currently not possible because if any speculative operation for
  // node default resources fails, the node will crash. We might
  // want to change that behavior in the future. Revisit this once we
  // change that behavior.
  checkpointResourceState(
      totalResources.filter(medios::needCheckpointing), false);

  if (protobuf::isSpeculativeOperation(message.operation_info())) {
    apply(operation);
  }

  if (resourceProviderId.isSome()) {
    CHECK_NOTNULL(resourceProviderManager.get())->applyOperation(message);
    return;
  }

  CHECK(protobuf::isSpeculativeOperation(message.operation_info()));

  UpdateOperationStatusMessage update =
    protobuf::createUpdateOperationStatusMessage(
        uuid,
        protobuf::createOperationStatus(
            OPERATION_FINISHED,
            operationId,
            None(),
            None(),
            id::UUID::random(),
            info.id(),
            resourceProviderId.isSome()
              ? resourceProviderId.get() : Option<ResourceProviderID>::none()),
        None(),
        frameworkId,
        info.id());

  updateOperation(operation, update);

  checkpointResourceState(
      totalResources.filter(medios::needCheckpointing), false);

  operationStatusUpdateManager.update(update);
}


void Node::reconcileOperations(const ReconcileOperationsMessage& message)
{
  // If the `framework_id` field in the message is set, then this reconciliation
  // request was initiated by the framework. This means the operations in this
  // message were not known to the controller at the time of reconciliation. If the
  // resource provider manager doesn't recognize the operation either, then we
  // will return OPERATION_UNKNOWN.
  if (message.has_framework_id()) {
    foreach (
        const ReconcileOperationsMessage::Operation& operation,
        message.operations()) {
      Option<UUID> operationUuid;
      if (operation.has_operation_uuid()) {
        operationUuid = operation.operation_uuid();
      } else if (operation.has_operation_id()) {
        auto key = std::make_pair(
            message.framework_id(), operation.operation_id());
        if (operationIds.contains(key)) {
          operationUuid = operationIds.at(key);
        }
      }

      if (operationUuid.isSome()) {
        Operation* storedOperation = getOperation(operationUuid.get());

        // If the node knows this operation, then the reconciliation request
        // must have raced with an `UpdateNodeMessage` from the node. We
        // satisfy this reconciliation request with the latest stored state of
        // the operation.
        if (storedOperation != nullptr) {
          // Clear the status UUID from the latest status since this update is
          // not sent reliably and thus does not require acknowledgement.
          OperationStatus status = storedOperation->latest_status();
          status.clear_uuid();

          UpdateOperationStatusMessage update =
            protobuf::createUpdateOperationStatusMessage(
                operationUuid.get(),
                status,
                None(),
                message.framework_id(),
                info.id());

          send(controller.get(), update);

          continue;
        }
      }

      // If the node doesn't know this operation and the operation includes a
      // resource provider ID, then we forward the reconciliation to the
      // resource provider manager to satisfy it based on whether or not the
      // specified resource provider is known.
      CHECK_NOTNULL(resourceProviderManager.get())
        ->reconcileOperations(message);
    }
  // If the `framework_id` field in the message is not set, then this
  // reconciliation was initiated by the controller. We help the controller reconcile
  // its in-memory state below. If operations known by the controller are not known
  // by the node/RP, then we return OPERATION_DROPPED to indicate that the
  // operation never made it to the node.
  } else {
    bool forwardToResourceProvider = false;

    foreach (
        const ReconcileOperationsMessage::Operation& operation,
        message.operations()) {
      // The `operation_uuid` field should always be set for
      // controller-initiated reconciliations.
      CHECK(operation.has_operation_uuid());

      if (operation.has_resource_provider_id()) {
        forwardToResourceProvider = true;
        continue;
      }

      // The controller reconciles when it notices that an operation is missing from
      // an `UpdateNodeMessage`. If we cannot find an operation in the node
      // state, we send an update to inform the controller. If we do find the
      // operation, then the controller and node state are consistent and we do not
      // need to do anything.
      Operation* storedOperation = getOperation(operation.operation_uuid());
      if (storedOperation == nullptr) {
        // For node default resources, we send best-effort operation status
        // updates to the controller. This is satisfactory because a dropped message
        // would imply a subsequent node reregistration, after which an
        // `UpdateNodeMessage` would be sent with pending operations.
        UpdateOperationStatusMessage update =
          protobuf::createUpdateOperationStatusMessage(
              operation.operation_uuid(),
              protobuf::createOperationStatus(
                  OPERATION_DROPPED,
                  None(),
                  None(),
                  None(),
                  None(),
                  info.id()),
              None(),
              None(),
              info.id());

        send(controller.get(), update);
      }
    }

    if (forwardToResourceProvider) {
      CHECK_NOTNULL(resourceProviderManager.get())
        ->reconcileOperations(message);
    }
  }
}


void Node::statusUpdateAcknowledgement(
    const UPID& from,
    const NodeID& nodeId,
    const FrameworkID& frameworkId,
    const TaskID& taskId,
    const string& uuid)
{
  // Originally, all status update acknowledgements were sent from the
  // scheduler driver. We'd like to have all acknowledgements sent by
  // the controller instead. See: MESOS-1389.
  // For now, we handle acknowledgements from the leading controller and
  // from the scheduler driver, for backwards compatibility.
  // TODO(bmahler): Aim to have the scheduler driver no longer
  // sending acknowledgements in 0.20.0. Stop handling those messages
  // here in 0.21.0.
  // NOTE: We must reject those acknowledgements coming from
  // non-leading controllers because we may have already sent the terminal
  // un-acknowledged task to the leading controller! Unfortunately, the
  // controller's pid will not change across runs on the same machine, so
  // we may process a message from the old controller on the same machine,
  // but this is a more general problem!
  if (strings::startsWith(from.id, "controller")) {
    if (state != RUNNING) {
      LOG(WARNING) << "Dropping status update acknowledgement message for "
                   << frameworkId << " because the node is in "
                   << state << " state";
      return;
    }

    if (controller != from) {
      LOG(WARNING) << "Ignoring status update acknowledgement message from "
                   << from << " because it is not the expected controller: "
                   << (controller.isSome() ? stringify(controller.get()) : "None");
      return;
    }
  }

  UUID uuid_;
  uuid_.set_value(uuid);

  taskStatusUpdateManager->acknowledgement(
      taskId, frameworkId, id::UUID::fromBytes(uuid).get())
    .onAny(defer(self(),
                 &Node::_statusUpdateAcknowledgement,
                 lambda::_1,
                 taskId,
                 frameworkId,
                 uuid_));
}


void Node::_statusUpdateAcknowledgement(
    const Future<bool>& future,
    const TaskID& taskId,
    const FrameworkID& frameworkId,
    const UUID& uuid)
{
  // The future could fail if this is a duplicate status update acknowledgement.
  if (!future.isReady()) {
    LOG(ERROR) << "Failed to handle status update acknowledgement (UUID: "
               << uuid << ") for task " << taskId
               << " of framework " << frameworkId << ": "
               << (future.isFailed() ? future.failure() : "future discarded");
    return;
  }

  VLOG(1) << "Task status update manager successfully handled status update"
          << " acknowledgement (UUID: " << uuid
          << ") for task " << taskId
          << " of framework " << frameworkId;

  CHECK(state == RECOVERING || state == DISCONNECTED ||
        state == RUNNING || state == TERMINATING)
    << state;

  Framework* framework = getFramework(frameworkId);
  if (framework == nullptr) {
    LOG(ERROR) << "Status update acknowledgement (UUID: " << uuid
               << ") for task " << taskId
               << " of unknown framework " << frameworkId;
    return;
  }

  CHECK(framework->state == Framework::RUNNING ||
        framework->state == Framework::TERMINATING)
    << framework->state;

  // Find the executor that has this update.
  Executor* executor = framework->getExecutor(taskId);
  if (executor == nullptr) {
    LOG(ERROR) << "Status update acknowledgement (UUID: " << uuid
               << ") for task " << taskId
               << " of unknown executor";
    return;
  }

  CHECK(executor->state == Executor::REGISTERING ||
        executor->state == Executor::RUNNING ||
        executor->state == Executor::TERMINATING ||
        executor->state == Executor::TERMINATED)
    << executor->state;

  // If the task has reached terminal state and all its updates have
  // been acknowledged, mark it completed.
  if (executor->terminatedTasks.contains(taskId) && !future.get()) {
    executor->completeTask(taskId);
  }

  // Remove the executor if it has terminated and there are no more
  // incomplete tasks.
  if (executor->state == Executor::TERMINATED && !executor->incompleteTasks()) {
    removeExecutor(framework, executor);
  }

  // Remove this framework if it has no pending executors and tasks.
  if (framework->idle()) {
    removeFramework(framework);
  }
}


void Node::operationStatusAcknowledgement(
    const UPID& from,
    const AcknowledgeOperationStatusMessage& acknowledgement)
{
  Operation* operation = getOperation(acknowledgement.operation_uuid());

  if (operation == nullptr) {
    LOG(WARNING) << "Dropping operation update acknowledgement with"
      << " status_uuid " << acknowledgement.status_uuid() << " and"
      << " operation_uuid " << acknowledgement.operation_uuid()
      << " because the operation was not found";

    return;
  }

  // If the operation was on resource provider resources forward the
  // acknowledgement to the resource provider manager as well.
  Result<ResourceProviderID> resourceProviderId =
    getResourceProviderId(operation->info());

  CHECK(!resourceProviderId.isError())
    << "Could not determine resource provider of operation " << operation
    << ": " << resourceProviderId.error();

  if (resourceProviderId.isSome()) {
    CHECK_NOTNULL(resourceProviderManager.get())
      ->acknowledgeOperationStatus(acknowledgement);

    CHECK(operation->statuses_size() > 0);
    if (protobuf::isTerminalState(
            operation->statuses(operation->statuses_size() - 1).state())) {
      // Note that if this acknowledgement is dropped due to resource provider
      // disconnection, the resource provider will inform the node about the
      // operation via an UPDATE_STATE call after it reregisters, which will
      // cause the node to add the operation back.
      removeOperation(operation);
    }

    return;
  }

  // Acknowledgement was for an operation on the node's default resources.
  auto statusUuid = id::UUID::fromBytes(
      acknowledgement.status_uuid().value());

  auto operationUuid = id::UUID::fromBytes(
      acknowledgement.operation_uuid().value());

  if (operationUuid.isError() || statusUuid.isError()) {
    LOG(WARNING) << "Dropping acknowledgement for operation " << operation
      << " with provided operation uuid "
      << acknowledgement.operation_uuid().value()
      << " and status uuid "
      << acknowledgement.status_uuid().value() << ".";
    return;
  }

  auto err = [](const id::UUID& uuid, const string& message) {
    LOG(ERROR)
      << "Failed to acknowledge status update for operation (uuid: " << uuid
      << "): " << message;
  };

  // NOTE: It is possible that an incoming acknowledgement races with an
  // outgoing retry of status update, and then a duplicated
  // acknowledgement will be received. In this case, the following call
  // will fail, so we just leave an error log.
  operationStatusUpdateManager
    .acknowledgement(operationUuid.get(), statusUuid.get())
    .then(defer(self(), [=](bool continuation) {
      if (!continuation) {
        removeOperation(operation);

        // Garbage collect the status update stream.

        const string path = node::paths::getNodeOperationPath(
            metaDir,
            info.id(),
            operationUuid.get());

        // NOTE: We check if the path exists since we do not checkpoint some
        // status updates, such as OPERATION_DROPPED.
        if (os::exists(path)) {
          Try<Nothing> rmdir = os::rmdir(path);
          if (rmdir.isError()) {
            LOG(ERROR) << "Failed to remove operation status update stream "
                       << "directory '" << path << "': " << rmdir.error();
          }
        }
      }

      return Nothing();
    }))
    .onFailed(std::bind(err, operationUuid.get(), lambda::_1))
    .onDiscarded(std::bind(err, operationUuid.get(), "future discarded"));
}


void Node::subscribe(
    StreamingHttpConnection<v1::executor::Event> http,
    const Call::Subscribe& subscribe,
    Framework* framework,
    Executor* executor)
{
  CHECK_NOTNULL(framework);
  CHECK_NOTNULL(executor);

  LOG(INFO) << "Received Subscribe request for HTTP executor " << *executor;

  CHECK(state == RECOVERING || state == DISCONNECTED ||
        state == RUNNING || state == TERMINATING)
    << state;

  if (state == TERMINATING) {
    LOG(WARNING) << "Shutting down executor " << *executor << " as the node "
                 << "is terminating";
    http.send(ShutdownExecutorMessage());
    http.close();
    return;
  }

  CHECK(framework->state == Framework::RUNNING ||
        framework->state == Framework::TERMINATING)
    << framework->state;

  if (framework->state == Framework::TERMINATING) {
    LOG(WARNING) << "Shutting down executor " << *executor << " as the "
                 << "framework is terminating";
    http.send(ShutdownExecutorMessage());
    http.close();
    return;
  }

  switch (executor->state) {
    case Executor::TERMINATING:
    case Executor::TERMINATED:
      // TERMINATED is possible if the executor forks, the parent process
      // terminates and the child process (driver) tries to register!
      LOG(WARNING) << "Shutting down executor " << *executor
                   << " because it is in unexpected state " << executor->state;
      http.send(ShutdownExecutorMessage());
      http.close();
      break;
    case Executor::RUNNING:
    case Executor::REGISTERING: {
      // Close the earlier connection if one existed. This can even
      // be a retried Subscribe request from an already connected
      // executor.
      if (executor->http.isSome()) {
        LOG(WARNING) << "Closing already existing HTTP connection from "
                     << "executor " << *executor;
        executor->http->close();
      }

      executor->state = Executor::RUNNING;

      // Save the connection for the executor.
      executor->http = http;
      executor->pid = None();

      // Create a heartbeater for HTTP executors.
      executor::Event heartbeatEvent;
      heartbeatEvent.set_type(executor::Event::HEARTBEAT);

      executor->heartbeater.reset(
          new ResponseHeartbeater<executor::Event, v1::executor::Event>(
              "executor " + stringify(executor->id),
              heartbeatEvent,
              http,
              DEFAULT_EXECUTOR_HEARTBEAT_INTERVAL,
              DEFAULT_EXECUTOR_HEARTBEAT_INTERVAL));

      if (framework->info.checkpoint()) {
        // Write a marker file to indicate that this executor
        // is HTTP based.
        const string path = paths::getExecutorHttpMarkerPath(
            metaDir,
            info.id(),
            framework->id(),
            executor->id,
            executor->containerId);

        LOG(INFO) << "Creating a marker file for HTTP based executor "
                  << *executor << " at path '" << path << "'";
        CHECK_SOME(os::touch(path));
      }

      // Handle all the pending updates.
      // The task status update manager might have already checkpointed
      // some of these pending updates (for example, if the node died
      // right after it checkpointed the update but before it could send
      // the ACK to the executor). This is ok because the status update
      // manager correctly handles duplicate updates.
      foreach (const Call::Update& update, subscribe.unacknowledged_updates()) {
        // The subscribed connection is what identifies the sender, so stamp
        // it onto the replay. Without it a replayed update carries no
        // executor id unless the executor put one in the status, and an
        // executor may report only on tasks it owns.
        StatusUpdate replay = protobuf::createStatusUpdate(
            framework->id(),
            update.status(),
            info.id());

        replay.mutable_executor_id()->CopyFrom(executor->id);

        // NOTE: This also updates the executor's resources!
        statusUpdate(std::move(replay), None());
      }

      hashmap<TaskID, TaskInfo> unackedTasks;
      foreach (const TaskInfo& task, subscribe.unacknowledged_tasks()) {
        unackedTasks[task.task_id()] = task;
      }

      // Now, if there is any task still in STAGING state and not in
      // unacknowledged 'tasks' known to the executor, the node must
      // have died before the executor received the task! We should
      // transition it to TASK_DROPPED. We only consider/store
      // unacknowledged 'tasks' at the executor driver because if a
      // task has been acknowledged, the node must have received an
      // update for that task and transitioned it out of STAGING!
      //
      // TODO(vinod): Consider checkpointing 'TaskInfo' instead of
      // 'Task' so that we can relaunch such tasks! Currently we don't
      // do it because 'TaskInfo.data' could be huge.
      foreach (Task* task, executor->launchedTasks.values()) {
        if (task->state() == TASK_STAGING &&
            !unackedTasks.contains(task->task_id())) {
          medios::TaskState newTaskState = TASK_DROPPED;
          if (!protobuf::frameworkHasCapability(
                  framework->info,
                  FrameworkInfo::Capability::PARTITION_AWARE)) {
            newTaskState = TASK_LOST;
          }

          LOG(INFO) << "Transitioning STAGED task " << task->task_id()
                    << " to " << newTaskState
                    << " because it is unknown to the executor "
                    << executor->id;

          const StatusUpdate update = protobuf::createStatusUpdate(
              framework->id(),
              info.id(),
              task->task_id(),
              newTaskState,
              TaskStatus::SOURCE_NODE,
              id::UUID::random(),
              "Task launched during node restart",
              TaskStatus::REASON_NODE_RESTARTED,
              executor->id);

          statusUpdate(update, UPID());
        }
      }

      // Shutdown the executor if all of its initial tasks are killed.
      // See MESOS-8411. This is a workaround for those executors (e.g.,
      // command executor, default executor) that do not have a proper
      // self terminating logic when they haven't received the task or
      // task group within a timeout.
      if (!executor->everSentTask() && executor->queuedTasks.empty()) {
        LOG(WARNING) << "Shutting down executor " << *executor
                     << " because it has never been sent a task and all of"
                     << " its queued tasks have been killed before delivery";

        _shutdownExecutor(framework, executor);

        return;
      }

      // Tell executor it's registered and give it any queued tasks
      // or task groups.
      executor::Event event;
      event.set_type(executor::Event::SUBSCRIBED);

      executor::Event::Subscribed* subscribed = event.mutable_subscribed();
      subscribed->mutable_executor_info()->CopyFrom(executor->info);
      subscribed->mutable_framework_info()->MergeFrom(framework->info);
      subscribed->mutable_node_info()->CopyFrom(info);
      subscribed->mutable_container_id()->CopyFrom(executor->containerId);

      executor->send(event);

      // Split the queued tasks between the task groups and tasks.
      LinkedHashMap<TaskID, TaskInfo> queuedTasks = executor->queuedTasks;

      foreach (const TaskGroupInfo& taskGroup, executor->queuedTaskGroups) {
        foreach (const TaskInfo& task, taskGroup.tasks()) {
          queuedTasks.erase(task.task_id());
        }
      }

      const ContainerID& containerId = executor->containerId;
      const Resources& resourceRequests = executor->allocatedResources();
      const google::protobuf::Map<string, Value::Scalar>& resourceLimits =
        computeExecutorLimits(
            executor->info.resources(),
            executor->queuedTasks.values(),
            executor->launchedTasks.values());

      Future<Nothing> resourcesPublished;
      if (executor->queuedTasks.empty()) {
        // Since no task is queued, all resources should have been published
        // before, so we skip resource publishing here. This avoids failures due
        // to unregistered resource providers during recovery (see MESOS-9711).
        //
        // NOTE: It is safe to not update the published resources when the
        // executor reduces its resource consumption (e.g., due to task
        // completion) because we don't require resources to be unpublished
        // after use. See comments in `publishResources` for details.
        resourcesPublished = Nothing();
      } else {
        resourcesPublished = publishResources(containerId, resourceRequests);
      }

      resourcesPublished
        .then(defer(
            self(),
            [this, containerId, resourceRequests, resourceLimits] {
              // NOTE: The executor struct could have been removed before
              // containerizer update, so we use the captured container ID,
              // resource requests and limits here. If this happens, the
              // containerizer would simply skip updating a destroyed container.
              return containerizer->update(
                  containerId, resourceRequests, resourceLimits);
        }))
        .onAny(defer(self(),
                     &Self::___run,
                     lambda::_1,
                     framework->id(),
                     executor->id,
                     executor->containerId,
                     queuedTasks.values(),
                     executor->queuedTaskGroups));

      break;
    }
    default:
      LOG(FATAL) << "Executor " << *executor << " is in unexpected state "
                 << executor->state;
      break;
  }
}


void Node::registerExecutor(
    const UPID& from,
    const FrameworkID& frameworkId,
    const ExecutorID& executorId)
{
  LOG(INFO) << "Got registration for executor '" << executorId
            << "' of framework " << frameworkId << " from "
            << stringify(from);

  CHECK(state == RECOVERING || state == DISCONNECTED ||
        state == RUNNING || state == TERMINATING)
    << state;

  if (state == RECOVERING) {
    LOG(WARNING) << "Shutting down executor '" << executorId
                 << "' of framework " << frameworkId
                 << " because the node is still recovering";
    reply(ShutdownExecutorMessage());
    return;
  }

  if (state == TERMINATING) {
    LOG(WARNING) << "Shutting down executor '" << executorId
                 << "' of framework " << frameworkId
                 << " because the node is terminating";
    reply(ShutdownExecutorMessage());
    return;
  }

  Framework* framework = getFramework(frameworkId);
  if (framework == nullptr) {
    LOG(WARNING) << "Shutting down executor '" << executorId
                 << "' as the framework " << frameworkId
                 << " does not exist";

    reply(ShutdownExecutorMessage());
    return;
  }

  CHECK(framework->state == Framework::RUNNING ||
        framework->state == Framework::TERMINATING)
    << framework->state;

  if (framework->state == Framework::TERMINATING) {
    LOG(WARNING) << "Shutting down executor '" << executorId
                 << "' as the framework " << frameworkId
                 << " is terminating";

    reply(ShutdownExecutorMessage());
    return;
  }

  Executor* executor = framework->getExecutor(executorId);

  // Check the status of the executor.
  if (executor == nullptr) {
    LOG(WARNING) << "Unexpected executor '" << executorId
                 << "' registering for framework " << frameworkId;
    reply(ShutdownExecutorMessage());
    return;
  }

  switch (executor->state) {
    case Executor::TERMINATING:
    case Executor::TERMINATED:
      // TERMINATED is possible if the executor forks, the parent process
      // terminates and the child process (driver) tries to register!
    case Executor::RUNNING:
      LOG(WARNING) << "Shutting down executor " << *executor
                   << " because it is in unexpected state " << executor->state;
      reply(ShutdownExecutorMessage());
      break;
    case Executor::REGISTERING: {
      executor->state = Executor::RUNNING;

      // Save the pid for the executor.
      executor->pid = from;
      link(from);

      if (framework->info.checkpoint()) {
        // TODO(vinod): This checkpointing should be done
        // asynchronously as it is in the fast path of the node!

        // Checkpoint the libprocess pid.
        string path = paths::getLibprocessPidPath(
            metaDir,
            info.id(),
            executor->frameworkId,
            executor->id,
            executor->containerId);

        VLOG(1) << "Checkpointing executor pid '"
                << executor->pid.get() << "' to '" << path << "'";
        CHECK_SOME(state::checkpoint(path, executor->pid.get()));
      }

      // Here, we kill the executor if it no longer has any task to run
      // (e.g., framework sent a `killTask()`). This is a workaround for those
      // single task executors (e.g., command executor) that do not have a
      // proper self terminating logic when they haven't received the task
      // within a timeout. Also note even if the node restarts before sending
      // this shutdown message, it is safe because the executor driver shuts
      // down the executor if it gets disconnected from the node before
      // registration.
      if (!executor->everSentTask() && executor->queuedTasks.empty()) {
        LOG(WARNING) << "Shutting down registering executor " << *executor
                     << " because it has no tasks to run";

        _shutdownExecutor(framework, executor);

        return;
      }

      // Tell executor it's registered and give it any queued tasks
      // or task groups.
      ExecutorRegisteredMessage message;
      message.mutable_executor_info()->MergeFrom(executor->info);
      message.mutable_framework_id()->MergeFrom(framework->id());
      message.mutable_framework_info()->MergeFrom(framework->info);
      message.mutable_node_id()->MergeFrom(info.id());
      message.mutable_node_info()->MergeFrom(info);
      executor->send(message);

      // Split the queued tasks between the task groups and tasks.
      LinkedHashMap<TaskID, TaskInfo> queuedTasks = executor->queuedTasks;

      foreach (const TaskGroupInfo& taskGroup, executor->queuedTaskGroups) {
        foreach (const TaskInfo& task, taskGroup.tasks()) {
          queuedTasks.erase(task.task_id());
        }
      }

      const ContainerID& containerId = executor->containerId;
      const Resources& resourceRequests = executor->allocatedResources();
      const google::protobuf::Map<string, Value::Scalar>& resourceLimits =
        computeExecutorLimits(
            executor->info.resources(),
            executor->queuedTasks.values(),
            executor->launchedTasks.values());

      publishResources(containerId, resourceRequests)
        .then(defer(
            self(),
            [this, containerId, resourceRequests, resourceLimits] {
              // NOTE: The executor struct could have been removed before
              // containerizer update, so we use the captured container ID,
              // resource requests and limits here. If this happens, the
              // containerizer would simply skip updating a destroyed container.
              return containerizer->update(
                  containerId, resourceRequests, resourceLimits);
        }))
        .onAny(defer(self(),
                     &Self::___run,
                     lambda::_1,
                     frameworkId,
                     executorId,
                     executor->containerId,
                     queuedTasks.values(),
                     executor->queuedTaskGroups));

      break;
    }
    default:
      LOG(FATAL) << "Executor " << *executor << " is in unexpected state "
                 << executor->state;
      break;
  }
}


void Node::reregisterExecutor(
    const UPID& from,
    const FrameworkID& frameworkId,
    const ExecutorID& executorId,
    const vector<TaskInfo>& tasks,
    const vector<StatusUpdate>& updates)
{
  CHECK(state == RECOVERING || state == DISCONNECTED ||
        state == RUNNING || state == TERMINATING)
    << state;

  LOG(INFO) << "Received re-registration message from"
            << " executor '" << executorId << "'"
            << " of framework " << frameworkId;

  if (state == TERMINATING) {
    LOG(WARNING) << "Shutting down executor '" << executorId << "'"
                 << " of framework " << frameworkId
                 << " because the node is terminating";

    reply(ShutdownExecutorMessage());
    return;
  }

  if (!frameworks.contains(frameworkId)) {
    LOG(WARNING) << "Shutting down executor '" << executorId << "'"
                 << " of framework " << frameworkId
                 << " because the framework is unknown";

    reply(ShutdownExecutorMessage());
    return;
  }

  Framework* framework = frameworks.at(frameworkId);

  CHECK(framework->state == Framework::RUNNING ||
        framework->state == Framework::TERMINATING)
    << framework->state;

  if (framework->state == Framework::TERMINATING) {
    LOG(WARNING) << "Shutting down executor '" << executorId << "'"
                 << " of framework " << frameworkId
                 << " because the framework is terminating";

    reply(ShutdownExecutorMessage());
    return;
  }

  Executor* executor = framework->getExecutor(executorId);

  if (executor == nullptr) {
    LOG(WARNING) << "Shutting down unknown executor '" << executorId << "'"
                 << " of framework " << frameworkId;

    reply(ShutdownExecutorMessage());
    return;
  }

  switch (executor->state) {
    case Executor::TERMINATING:
    case Executor::TERMINATED:
      // TERMINATED is possible if the executor forks, the parent process
      // terminates and the child process (driver) tries to register!
      LOG(WARNING) << "Shutting down executor " << *executor
                   << " because it is in unexpected state " << executor->state;
      reply(ShutdownExecutorMessage());
      break;

    case Executor::RUNNING:
      if (flags.executor_reregistration_retry_interval.isNone()) {
        // Previously, when an executor sends a re-registration while
        // in the RUNNING state, we would shut the executor down. We
        // preserve that behavior when the optional reconnect retry
        // is not enabled.
        LOG(WARNING) << "Shutting down executor " << *executor
                     << " because it is in unexpected state "
                     << executor->state;
        reply(ShutdownExecutorMessage());
      } else {
        // When the node is configured to retry the reconnect requests
        // to executors, we ignore any further re-registrations. This
        // is because we can't easily handle reregistering libprocess
        // based executors in the steady state, and we plan to move to
        // only allowing v1 HTTP executors (where re-subscription in
        // the steady state is supported). Also, ignoring this message
        // ensures that any executors mimicking the libprocess protocol
        // do not have any illusion of being able to reregister without
        // an node restart (hopefully they will commit suicide if they
        // fail to reregister).
        LOG(WARNING) << "Ignoring executor re-registration message from "
                     << *executor << " because it is already registered";
      }
      break;

    case Executor::REGISTERING: {
      executor->state = Executor::RUNNING;

      executor->pid = from; // Update the pid.
      link(from);

      // Send re-registration message to the executor.
      ExecutorReregisteredMessage message;
      message.mutable_node_id()->MergeFrom(info.id());
      message.mutable_node_info()->MergeFrom(info);
      send(executor->pid.get(), message);

      // Handle all the pending updates.
      // The task status update manager might have already checkpointed
      // some of these pending updates (for example, if the node died
      // right after it checkpointed the update but before it could send
      // the ACK to the executor). This is ok because the status update
      // manager correctly handles duplicate updates.
      foreach (const StatusUpdate& update, updates) {
        // The identity inside a replayed update is whatever the executor
        // put there, and it is optional, so stamp the identity this node
        // resolved from the re-registration message instead. Without it an
        // executor could report on another's task, or route the update to
        // another framework, from inside its own replay.
        StatusUpdate replay = update;
        replay.mutable_framework_id()->CopyFrom(framework->id());
        replay.mutable_executor_id()->CopyFrom(executor->id);

        // NOTE: This also updates the executor's resources!
        statusUpdate(std::move(replay), executor->pid.get());
      }

      // Tell the containerizer to update the resources.
      containerizer->update(
          executor->containerId,
          executor->allocatedResources(),
          computeExecutorLimits(
              executor->info.resources(),
              executor->queuedTasks.values(),
              executor->launchedTasks.values()))
        .onAny(defer(self(),
                     &Self::_reregisterExecutor,
                     lambda::_1,
                     frameworkId,
                     executorId,
                     executor->containerId));

      hashmap<TaskID, TaskInfo> unackedTasks;
      foreach (const TaskInfo& task, tasks) {
        unackedTasks[task.task_id()] = task;
      }

      // Now, if there is any task still in STAGING state and not in
      // unacknowledged 'tasks' known to the executor, the node must
      // have died before the executor received the task! We should
      // transition it to TASK_DROPPED. We only consider/store
      // unacknowledged 'tasks' at the executor driver because if a
      // task has been acknowledged, the node must have received
      // an update for that task and transitioned it out of STAGING!
      //
      // TODO(vinod): Consider checkpointing 'TaskInfo' instead of
      // 'Task' so that we can relaunch such tasks! Currently we
      // don't do it because 'TaskInfo.data' could be huge.
      foreach (Task* task, executor->launchedTasks.values()) {
        if (task->state() == TASK_STAGING &&
            !unackedTasks.contains(task->task_id())) {
          medios::TaskState newTaskState = TASK_DROPPED;
          if (!protobuf::frameworkHasCapability(
                  framework->info,
                  FrameworkInfo::Capability::PARTITION_AWARE)) {
            newTaskState = TASK_LOST;
          }

          LOG(INFO) << "Transitioning STAGED task " << task->task_id()
                    << " to " << newTaskState
                    << " because it is unknown to the executor '"
                    << executorId << "'";

          const StatusUpdate update = protobuf::createStatusUpdate(
              frameworkId,
              info.id(),
              task->task_id(),
              newTaskState,
              TaskStatus::SOURCE_NODE,
              id::UUID::random(),
              "Task launched during node restart",
              TaskStatus::REASON_NODE_RESTARTED,
              executorId);

          statusUpdate(update, UPID());
        }
      }

      // Shutdown the executor if all of its initial tasks are killed.
      // This is a workaround for those executors (e.g.,
      // command executor, default executor) that do not have a proper
      // self terminating logic when they haven't received the task or
      // task group within a timeout.
      if (!executor->everSentTask() && executor->queuedTasks.empty()) {
        LOG(WARNING) << "Shutting down reregistering executor " << *executor
                     << " because it has no tasks to run and"
                     << " has never been sent a task";

        _shutdownExecutor(framework, executor);

        return;
      }

      break;
    }
  }
}


void Node::_reregisterExecutor(
    const Future<Nothing>& future,
    const FrameworkID& frameworkId,
    const ExecutorID& executorId,
    const ContainerID& containerId)
{
  if (!future.isReady()) {
    LOG(ERROR) << "Failed to update resources for container " << containerId
               << " of executor '" << executorId
               << "' of framework " << frameworkId
               << ", destroying container: "
               << (future.isFailed() ? future.failure() : "discarded");

    containerizer->destroy(containerId);

    Executor* executor = getExecutor(frameworkId, executorId);
    if (executor != nullptr) {
      Framework* framework = getFramework(frameworkId);
      CHECK_NOTNULL(framework);

      // Send TASK_GONE because the task was started but has now
      // been terminated. If the framework is not partition-aware,
      // we send TASK_LOST instead for backward compatibility.
      medios::TaskState taskState = TASK_GONE;
      if (!framework->capabilities.partitionAware) {
        taskState = TASK_LOST;
      }

      ContainerTermination termination;
      termination.set_state(taskState);
      termination.set_reason(TaskStatus::REASON_CONTAINER_UPDATE_FAILED);
      termination.set_message(
          "Failed to update resources for container: " +
          (future.isFailed() ? future.failure() : "discarded"));

      executor->pendingTermination = termination;

      // TODO(jieyu): Set executor->state to be TERMINATING.
    }
  }
}


void Node::reregisterExecutorTimeout()
{
  CHECK(state == RECOVERING || state == TERMINATING) << state;

  LOG(INFO) << "Cleaning up un-reregistered executors";

  foreachvalue (Framework* framework, frameworks) {
    CHECK(framework->state == Framework::RUNNING ||
          framework->state == Framework::TERMINATING)
      << framework->state;

    foreachvalue (Executor* executor, framework->executors) {
      switch (executor->state) {
        case Executor::RUNNING:     // Executor reregistered.
        case Executor::TERMINATING:
        case Executor::TERMINATED:
          break;
        case Executor::REGISTERING: {
          // If we are here, the executor must have been hung and not
          // exited! This is because if the executor properly exited,
          // it should have already been identified by the isolator
          // (via the reaper) and cleaned up!
          LOG(INFO) << "Killing un-reregistered executor " << *executor;

          containerizer->destroy(executor->containerId);

          executor->state = Executor::TERMINATING;

          // Send TASK_GONE because the task was started but has now
          // been terminated. If the framework is not partition-aware,
          // we send TASK_LOST instead for backward compatibility.
          medios::TaskState taskState = TASK_GONE;
          if (!protobuf::frameworkHasCapability(
                  framework->info,
                  FrameworkInfo::Capability::PARTITION_AWARE)) {
            taskState = TASK_LOST;
          }

          ContainerTermination termination;
          termination.set_state(taskState);
          termination.set_reason(
              TaskStatus::REASON_EXECUTOR_REREGISTRATION_TIMEOUT);
          termination.set_message(
              "Executor did not reregister within " +
              stringify(flags.executor_reregistration_timeout));

          executor->pendingTermination = termination;
          break;
        }
        default:
          LOG(FATAL) << "Executor " << *executor << " is in unexpected state "
                     << executor->state;
          break;
      }
    }
  }

  // Replay any active draining.
  if (drainConfig.isSome()) {
    DrainNodeMessage drainNodeMessage;
    *drainNodeMessage.mutable_config() = *drainConfig;

    LOG(INFO) << "Replaying in-process node draining";

    drain(self(), std::move(drainNodeMessage));
  }

  // Signal the end of recovery.
  // TODO(greggomann): Allow the node to complete recovery before the executor
  // re-registration timeout has elapsed. See MESOS-7539
  recoveryInfo.recovered.set(Nothing());
}


// This can be called in two ways:
// 1) When a status update from the executor is received.
// 2) When node generates task updates (e.g LOST/KILLED/FAILED).
// NOTE: We set the pid in 'Node::___statusUpdate()' to 'pid' so that
// whoever sent this update will get an ACK. This is important because
// we allow executors to send updates for tasks that belong to other
// executors. Currently we allow this because we cannot guarantee
// reliable delivery of status updates. Since executor driver caches
// unacked updates it is important that whoever sent the update gets
// acknowledgement for it.
void Node::statusUpdate(StatusUpdate update, const Option<UPID>& pid)
{
  LOG(INFO) << "Handling status update " << update
            << (pid.isSome() ? " from " + stringify(pid.get()) : "");

  CHECK(state == RECOVERING || state == DISCONNECTED ||
        state == RUNNING || state == TERMINATING)
    << state;

  if (!update.has_uuid()) {
    LOG(WARNING) << "Ignoring status update " << update << " without 'uuid'";
    metrics.invalid_status_updates++;
    return;
  }

  if (update.node_id() != info.id()) {
    LOG(WARNING) << "Ignoring status update " << update << " due to "
                 << "Node ID mismatch; expected '" << info.id()
                 << "', received '" << update.node_id() << "'";
    metrics.invalid_status_updates++;
    return;
  }

  if (update.status().node_id() != info.id()) {
    LOG(WARNING) << "Ignoring status update " << update << " due to "
                 << "Node ID mismatch; expected '" << info.id()
                 << "', received '" << update.status().node_id() << "'";
    metrics.invalid_status_updates++;
    return;
  }

  // TODO(bmahler): With the HTTP API, we must validate the UUID
  // inside the TaskStatus. For now, we ensure that the uuid of task
  // status matches the update's uuid, in case the executor is using
  // pre 0.23.x driver.
  update.mutable_status()->set_uuid(update.uuid());

  // Set the source and UUID before forwarding the status update.
  update.mutable_status()->set_source(
      pid == UPID() ? TaskStatus::SOURCE_NODE : TaskStatus::SOURCE_EXECUTOR);

  // Set TaskStatus.executor_id if not already set; overwrite existing
  // value if already set.
  if (update.has_executor_id()) {
    if (update.status().has_executor_id() &&
        update.status().executor_id() != update.executor_id()) {
      LOG(WARNING) << "Executor ID mismatch in status update"
                   << (pid.isSome() ? " from " + stringify(pid.get()) : "")
                   << "; overwriting received '"
                   << update.status().executor_id() << "' with expected'"
                   << update.executor_id() << "'";
    }
    update.mutable_status()->mutable_executor_id()->CopyFrom(
        update.executor_id());
  }

  Framework* framework = getFramework(update.framework_id());
  if (framework == nullptr) {
    LOG(WARNING) << "Ignoring status update " << update
                 << " for unknown framework " << update.framework_id();
    metrics.invalid_status_updates++;
    return;
  }

  CHECK(framework->state == Framework::RUNNING ||
        framework->state == Framework::TERMINATING)
    << framework->state;

  // We don't send update when a framework is terminating because
  // it cannot send acknowledgements.
  if (framework->state == Framework::TERMINATING) {
    LOG(WARNING) << "Ignoring status update " << update
                 << " for terminating framework " << framework->id();
    metrics.invalid_status_updates++;
    return;
  }

  // If the node is draining we provide additional
  // information for KILLING or KILLED states.
  if (drainConfig.isSome()) {
    switch (update.status().state()) {
      case TASK_STAGING:
      case TASK_STARTING:
      case TASK_RUNNING:
      case TASK_FAILED:
      case TASK_FINISHED:
      case TASK_ERROR:
      case TASK_LOST:
      case TASK_DROPPED:
      case TASK_UNREACHABLE:
      case TASK_GONE:
      case TASK_GONE_BY_OPERATOR:
      case TASK_UNKNOWN: {
        break;
      }
      case TASK_KILLING:
      case TASK_KILLED: {
        // We unconditionally overwrite any previous reason to provide a
        // consistent signal that this task went away during draining.
        update.mutable_status()->set_reason(TaskStatus::REASON_NODE_DRAINING);

        // If the draining marks the node as gone report tasks as
        // gone by operator.
        if (drainConfig->mark_gone() &&
            framework->capabilities.partitionAware) {
          update.mutable_status()->set_state(TASK_GONE_BY_OPERATOR);
        }
        break;
      }
    }
  }

  if (HookManager::hooksAvailable()) {
    // Even though the hook(s) return a TaskStatus, we only use two fields:
    // container_status and labels. Remaining fields are discarded.
    TaskStatus statusFromHooks =
      HookManager::nodeTaskStatusDecorator(
          update.framework_id(), update.status());
    if (statusFromHooks.has_labels()) {
      update.mutable_status()->mutable_labels()->CopyFrom(
          statusFromHooks.labels());
    }

    if (statusFromHooks.has_container_status()) {
      update.mutable_status()->mutable_container_status()->CopyFrom(
          statusFromHooks.container_status());
    }
  }

  const TaskStatus& status = update.status();

  // An executor may report only on tasks it owns. The node holds the only
  // executor-to-task map, so this is the only place the refusal can be made,
  // and it has to come before anything that acts on the update: the pending
  // branch below removes a task and so prevents its launch, which is itself
  // the damage a cross-executor report can do.
  //
  // The sender is resolved to an executor this node knows: by the address a
  // libprocess executor gave, or by the id stamped onto an HTTP executor's
  // update from the call it arrived on. A resolved id replaces whatever the
  // message carried, so the rest of the path and the record a framework
  // reads name the executor the node resolved.
  //
  // An update the node generated itself carries `SOURCE_NODE` and is not
  // judged, which includes the cancellation of a pending task.
  //
  // The refusal defends against a mistaken executor, not a hostile one,
  // until executor identity is verified (medios#551). Both of those
  // identities come from the sender: the address is a field in the message
  // rather than the libprocess envelope's own, and the HTTP id is the call's
  // own unless authenticated claims are in use. So this check is only as
  // strong as the node's knowledge of who sent the update, and that is
  // currently weak.
  const Option<ExecutorID> pendingOwner =
    framework->getExecutorIdForPendingTask(status.task_id());

  if (status.source() == TaskStatus::SOURCE_EXECUTOR) {
    Executor* sender = nullptr;
    if (pid.isSome() && pid.get() != UPID()) {
      sender = framework->getExecutor(pid.get());
    } else if (update.has_executor_id()) {
      sender = framework->getExecutor(update.executor_id());
    }

    bool entitled;
    if (sender != nullptr) {
      update.mutable_executor_id()->CopyFrom(sender->id);
      update.mutable_status()->mutable_executor_id()->CopyFrom(sender->id);

      // Holding the task is the whole test, and a task still awaiting
      // authorization has been delivered to nobody, so not even the executor
      // it is destined for may report on it. Allowing that reaches the
      // partially-killed CHECK in `__run`: the branch below removes the one
      // task named, and a task group that then launches with a member
      // missing aborts the node.
      entitled = sender->hasTask(status.task_id());
    } else {
      // The node cannot say who sent this. It is judged only when the task
      // does have an owner here, because then the sender is not that owner.
      // Otherwise the update is forwarded: an executor the node has already
      // torn down can still replay a terminal update for a task that no
      // longer appears in any executor's structures, and refusing that
      // would drop a lifecycle update.
      //
      // Ownership here is the four-structure rule a resolved sender is held
      // to, completed tasks included, so a task a live executor still holds
      // cannot be reported on by an address the node cannot place.
      entitled = framework->getExecutorHoldingTask(status.task_id()) ==
                   nullptr &&
                 pendingOwner.isNone();
    }

    if (!entitled) {
      LOG(WARNING) << "Refusing status update " << update
                   << " for task " << status.task_id()
                   << " of framework " << framework->id()
                   << ": it did not come from the executor that owns the task";

      metrics.unowned_task_status_updates++;

      // The sender holds an update until it is acknowledged and replays it
      // on every subscription, so a silent refusal becomes an endless retry.
      // The acknowledgement goes to whoever sent this update, and only to
      // them: the path taken for a handled update resolves the executor from
      // the task, which here is a different executor or none.
      //
      // It is dispatched rather than sent here because this is reached while
      // handling a subscription's replayed updates, before the executor has
      // been told it is subscribed.
      if (pid.isSome() && pid.get() != UPID()) {
        dispatch(
            self(),
            &Node::sendStatusUpdateAcknowledgement,
            update.framework_id(),
            None(),
            status.task_id(),
            update.uuid(),
            pid);
      } else if (sender != nullptr) {
        dispatch(
            self(),
            &Node::sendStatusUpdateAcknowledgement,
            update.framework_id(),
            Option<ExecutorID>(sender->id),
            status.task_id(),
            update.uuid(),
            None());
      }

      return;
    }
  }

  // A progress report that fails validation is stripped from the update
  // itself, before anything retains, queues or checkpoints it. The task
  // status update manager writes this protobuf to disk and replays it from
  // there, so clearing the field only on the task record would leave the
  // bytes in the checkpoint and on the wire. The task record keeps the last
  // report that passed, which `Executor::updateTaskState` does for any
  // status that carries no progress.
  if (update.status().has_progress()) {
    Option<Error> progressError =
      common::validation::validateTaskProgress(update.status().progress());

    if (progressError.isSome()) {
      LOG(WARNING) << "Dropping task progress from status update for task "
                   << status.task_id() << " of framework " << framework->id()
                   << ": " << progressError->message;

      update.mutable_status()->clear_progress();
      metrics.invalid_task_progress_reports++;
    }
  }

  // A finish outcome with no state or an impossible one is dropped the
  // same way (DR-042).
  if (update.status().has_finish()) {
    Option<Error> finishError =
      common::validation::validateFinishStatus(update.status().finish());

    if (finishError.isSome()) {
      LOG(WARNING) << "Dropping the finish outcome from status update for task "
                   << status.task_id() << " of framework " << framework->id()
                   << ": " << finishError->message;

      update.mutable_status()->clear_finish();
      metrics.invalid_task_finish_outcomes++;
    }
  }

  // For pending tasks, we must synchronously remove them
  // to guarantee that the launch is prevented.
  //
  // TODO(bmahler): Ideally we store this task as terminated
  // but with unacknowledged updates (same as the `Executor`
  // struct does).
  if (framework->isPending(status.task_id())) {
    CHECK(framework->removePendingTask(status.task_id()));

    if (framework->idle()) {
      removeFramework(framework);
    }

    metrics.valid_status_updates++;

    taskStatusUpdateManager->update(update, info.id())
      .onAny(defer(self(), &Node::___statusUpdate, lambda::_1, update, pid));

    return;
  }

  Executor* executor = framework->getExecutor(status.task_id());
  if (executor == nullptr) {
    LOG(WARNING) << "Could not find the executor for "
                 << "status update " << update;
    metrics.valid_status_updates++;

    // NOTE: We forward the update here because this update could be
    // generated by the node when the executor is unknown to it
    // (e.g., killTask(), _run()) or sent by an executor for a
    // task that belongs to another executor.
    // We also end up here if 1) the previous node died after
    // checkpointing a _terminal_ update but before it could send an
    // ACK to the executor AND 2) after recovery the status update
    // manager successfully retried the update, got the ACK from the
    // scheduler and cleaned up the stream before the executor
    // reregistered. In this case, the node cannot find the executor
    // corresponding to this task because the task has been moved to
    // 'Executor::completedTasks'.
    //
    // NOTE: We do not set the `ContainerStatus` (including the
    // `NetworkInfo` within the `ContainerStatus)  for this case,
    // because the container is unknown. We cannot use the node IP
    // address here (for the `NetworkInfo`) since we do not know the
    // type of network isolation used for this container.
    taskStatusUpdateManager->update(update, info.id())
      .onAny(defer(self(), &Node::___statusUpdate, lambda::_1, update, pid));

    return;
  }

  CHECK(executor->state == Executor::REGISTERING ||
        executor->state == Executor::RUNNING ||
        executor->state == Executor::TERMINATING ||
        executor->state == Executor::TERMINATED)
    << executor->state;

  // Failing this validation on the executor driver used to cause the
  // driver to abort. Now that the validation is done by the node, it
  // should shutdown the executor to be consistent.
  //
  // TODO(arojas): Once the HTTP API is the default, return a
  // 400 Bad Request response, indicating the reason in the body.
  if (status.source() == TaskStatus::SOURCE_EXECUTOR &&
      status.state() == TASK_STAGING) {
    LOG(ERROR) << "Received TASK_STAGING from executor " << *executor
               << " which is not allowed. Shutting down the executor";

    _shutdownExecutor(framework, executor);
    return;
  }

  // An update from a known executor for a task it does not own is refused
  // above, so what reaches here on behalf of a different executor comes from
  // an executor id this node does not know and cannot judge.
  if (pid.isSome() &&
      pid != UPID() &&
      executor->pid.isSome() &&
      executor->pid != pid) {
    LOG(WARNING) << "Received status update " << update << " from " << pid.get()
                 << " on behalf of a different executor '" << executor->id
                 << "' (" << executor->pid.get() << ")";
  }

  metrics.valid_status_updates++;

  executor->addPendingTaskStatus(status);

  // Before sending update, we need to retrieve the container status
  // if the task reached the executor. For tasks that are queued, we
  // do not need to send the container status and we must
  // synchronously transition the task to ensure that it is removed
  // from the queued tasks before the run task path continues.
  //
  // Also if the task is in `launchedTasks` but was dropped by the
  // node, we know that the task did not reach the executor. We
  // will synchronously transition the task to ensure that the
  // node re-registration logic can call `everSentTask()` after
  // dropping tasks.
  if (executor->queuedTasks.contains(status.task_id())) {
    CHECK(protobuf::isTerminalState(status.state()))
        << "Queued tasks can only be transitioned to terminal states";

    _statusUpdate(update, pid, executor->id, None());
  } else if (executor->launchedTasks.contains(status.task_id()) &&
            (status.state() == TASK_DROPPED || status.state() == TASK_LOST) &&
            status.source() == TaskStatus::SOURCE_NODE) {
    _statusUpdate(update, pid, executor->id, None());
  } else {
    // NOTE: If the executor sets the ContainerID inside the
    // ContainerStatus, that indicates that the Task this status update
    // is associated with is tied to that container (could be nested).
    // Therefore, we need to get the status of that container, instead
    // of the top level executor container.
    ContainerID containerId = executor->containerId;
    if (update.status().has_container_status() &&
        update.status().container_status().has_container_id()) {
      containerId = update.status().container_status().container_id();
    }

    containerizer->status(containerId)
      .onAny(defer(self(),
                   &Node::_statusUpdate,
                   update,
                   pid,
                   executor->id,
                   lambda::_1));
  }
}


void Node::_statusUpdate(
    StatusUpdate update,
    const Option<process::UPID>& pid,
    const ExecutorID& executorId,
    const Option<Future<ContainerStatus>>& containerStatus)
{
  // There can be cases where a container is already removed from the
  // containerizer before the `status` call is dispatched to the
  // containerizer, leading to the failure of the returned `Future`.
  // In such a case we should simply not update the `ContainerStatus`
  // with the return `Future` but continue processing the
  // `StatusUpdate`.
  if (containerStatus.isSome() && containerStatus->isReady()) {
    ContainerStatus* status =
      update.mutable_status()->mutable_container_status();

    status->MergeFrom(containerStatus->get());

    // Fill in the container IP address with the IP from the node
    // PID, if not already filled in.
    //
    // TODO(karya): Fill in the IP address by looking up the executor PID.
    if (status->network_infos().size() == 0) {
      NetworkInfo* networkInfo = status->add_network_infos();
      NetworkInfo::IPAddress* ipAddress = networkInfo->add_ip_addresses();

      // Set up IPv4 address.
      //
      // NOTE: By default the protocol is set to IPv4 and therefore we
      // don't explicitly set the protocol here.
      ipAddress->set_ip_address(stringify(self().address.ip));

      // Set up IPv6 address.
      if (self().addresses.v6.isSome()) {
        ipAddress = networkInfo->add_ip_addresses();
        ipAddress->set_ip_address(stringify(self().addresses.v6->ip));
        ipAddress->set_protocol(NetworkInfo::IPv6);
      }
    }
  }

  const TaskStatus& status = update.status();

  Executor* executor = getExecutor(update.framework_id(), executorId);
  if (executor == nullptr) {
    LOG(WARNING) << "Ignoring container status update for framework "
                 << update.framework_id()
                 << "for a non-existent executor";
    return;
  }

  // We set the latest state of the task here so that the node can
  // inform the controller about the latest state (via status update or
  // ReregisterNodeMessage message) as soon as possible. Controller can use
  // this information, for example, to release resources as soon as the
  // latest state of the task reaches a terminal state. This is
  // important because task status update manager queues updates and
  // only sends one update per task at a time; the next update for a
  // task is sent only after the acknowledgement for the previous one is
  // received, which could take a long time if the framework is backed
  // up or is down.
  Try<Nothing> updated = executor->updateTaskState(status);

  // If we fail to update the task state, drop the update. Note that
  // we have to acknowledge the executor so that it does not retry.
  if (updated.isError()) {
    LOG(ERROR) << "Failed to update state of task '" << status.task_id() << "'"
               << " to " << status.state() << ": " << updated.error();

    // NOTE: This may lead to out-of-order acknowledgements since other
    // update acknowledgements may be waiting for the containerizer or
    // task status update manager.
    ___statusUpdate(Nothing(), update, pid);
    return;
  }

  if (protobuf::isTerminalState(status.state())) {
    // If the task terminated, wait until the container's resources
    // have been updated before sending the status update. Note that
    // duplicate terminal updates are not possible here because they
    // lead to an error from `Executor::updateTaskState`.
    containerizer->update(
        executor->containerId,
        executor->allocatedResources(),
        computeExecutorLimits(
            executor->info.resources(),
            executor->queuedTasks.values(),
            executor->launchedTasks.values()))
      .onAny(defer(self(),
                   &Node::__statusUpdate,
                   lambda::_1,
                   update,
                   pid,
                   executor->id,
                   executor->containerId,
                   executor->checkpoint));
  } else {
    // Immediately send the status update.
    __statusUpdate(None(),
                   update,
                   pid,
                   executor->id,
                   executor->containerId,
                   executor->checkpoint);
  }
}


void Node::__statusUpdate(
    const Option<Future<Nothing>>& future,
    const StatusUpdate& update,
    const Option<UPID>& pid,
    const ExecutorID& executorId,
    const ContainerID& containerId,
    bool checkpoint)
{
  if (future.isSome() && !future->isReady()) {
    LOG(ERROR) << "Failed to update resources for container " << containerId
               << " of executor '" << executorId
               << "' running task " << update.status().task_id()
               << " on status update for terminal task, destroying container: "
               << (future->isFailed() ? future->failure() : "discarded");

    containerizer->destroy(containerId);

    Executor* executor = getExecutor(update.framework_id(), executorId);
    if (executor != nullptr) {
      Framework* framework = getFramework(update.framework_id());
      CHECK_NOTNULL(framework);

      // Send TASK_GONE because the task was started but has now
      // been terminated. If the framework is not partition-aware,
      // we send TASK_LOST instead for backward compatibility.
      medios::TaskState taskState = TASK_GONE;
      if (!framework->capabilities.partitionAware) {
        taskState = TASK_LOST;
      }

      ContainerTermination termination;
      termination.set_state(taskState);
      termination.set_reason(TaskStatus::REASON_CONTAINER_UPDATE_FAILED);
      termination.set_message(
          "Failed to update resources for container: " +
          (future->isFailed() ? future->failure() : "discarded"));

      executor->pendingTermination = termination;

      // TODO(jieyu): Set executor->state to be TERMINATING.
    }
  }

  if (checkpoint) {
    // Ask the task status update manager to checkpoint and reliably send the
    // update.
    taskStatusUpdateManager->update(update, info.id(), executorId, containerId)
      .onAny(defer(self(), &Node::___statusUpdate, lambda::_1, update, pid));
  } else {
    // Ask the task status update manager to just retry the update.
    taskStatusUpdateManager->update(update, info.id())
      .onAny(defer(self(), &Node::___statusUpdate, lambda::_1, update, pid));
  }
}


void Node::___statusUpdate(
    const Future<Nothing>& future,
    const StatusUpdate& update,
    const Option<UPID>& pid)
{
  CHECK_READY(future) << "Failed to handle status update " << update;

  VLOG(1) << "Task status update manager successfully handled status update "
          << update;

  const TaskStatus& status = update.status();

  Executor* executor = nullptr;
  Framework* framework = getFramework(update.framework_id());
  if (framework != nullptr) {
    executor = framework->getExecutor(status.task_id());
    if (executor != nullptr) {
      executor->removePendingTaskStatus(status);
    }
  }

  if (pid == UPID()) {
    return;
  }

  StatusUpdateAcknowledgementMessage message;
  message.mutable_framework_id()->MergeFrom(update.framework_id());
  message.mutable_node_id()->MergeFrom(update.node_id());
  message.mutable_task_id()->MergeFrom(status.task_id());
  message.set_uuid(update.uuid());

  // Task status update manager successfully handled the status update.
  // Acknowledge the executor, if we have a valid pid.
  if (pid.isSome()) {
    LOG(INFO) << "Sending acknowledgement for status update " << update
              << " to " << pid.get();

    send(pid.get(), message);
  } else {
    // Acknowledge the HTTP based executor.
    if (framework == nullptr) {
      LOG(WARNING) << "Ignoring sending acknowledgement for status update "
                   << update << " of unknown framework";
      return;
    }

    if (executor == nullptr) {
      // Refer to the comments in 'statusUpdate()' on when this can
      // happen.
      LOG(WARNING) << "Ignoring sending acknowledgement for status update "
                   << update << " of unknown executor";
      return;
    }

    executor->send(message);
  }
}


void Node::sendStatusUpdateAcknowledgement(
    const FrameworkID& frameworkId,
    const Option<ExecutorID>& executorId,
    const TaskID& taskId,
    const string& uuid,
    const Option<UPID>& pid)
{
  StatusUpdateAcknowledgementMessage message;
  message.mutable_framework_id()->CopyFrom(frameworkId);
  message.mutable_node_id()->CopyFrom(info.id());
  message.mutable_task_id()->CopyFrom(taskId);
  message.set_uuid(uuid);

  if (pid.isSome()) {
    send(pid.get(), message);
    return;
  }

  CHECK_SOME(executorId);

  Framework* framework = getFramework(frameworkId);
  if (framework == nullptr) {
    LOG(WARNING) << "Not acknowledging status update for task " << taskId
                 << ": framework " << frameworkId << " is gone";
    return;
  }

  Executor* executor = framework->getExecutor(executorId.get());
  if (executor == nullptr) {
    LOG(WARNING) << "Not acknowledging status update for task " << taskId
                 << ": executor " << executorId.get() << " is gone";
    return;
  }

  executor->send(message);
}


// NOTE: An acknowledgement for this update might have already been
// processed by the node but not the task status update manager.
void Node::forward(StatusUpdate update)
{
  CHECK(state == RECOVERING || state == DISCONNECTED ||
        state == RUNNING || state == TERMINATING)
    << state;

  if (state != RUNNING) {
    LOG(WARNING) << "Dropping status update " << update
                 << " sent by task status update manager because the node"
                 << " is in " << state << " state";
    return;
  }

  // Ensure that task status uuid is set even if this update was sent by
  // the task status update manager after recovering a pre 0.23.x
  // node/executor driver's updates. This allows us to simplify the
  // controller code (in >= 0.27.0) to assume the uuid is always set for
  // retryable updates.
  CHECK(update.has_uuid())
    << "Expecting updates without 'uuid' to have been rejected";

  update.mutable_status()->set_uuid(update.uuid());

  // Update the status update state of the task and include the latest
  // state of the task in the status update.
  Framework* framework = getFramework(update.framework_id());
  if (framework != nullptr) {
    const TaskID& taskId = update.status().task_id();
    Executor* executor = framework->getExecutor(taskId);
    if (executor != nullptr) {
      // NOTE: We do not look for the task in queued tasks because
      // no update is expected for it until it's launched. Similarly,
      // we do not look for completed tasks because the state for a
      // completed task shouldn't be changed.
      Task* task = nullptr;
      if (executor->launchedTasks.contains(taskId)) {
        task = executor->launchedTasks[taskId];
      } else if (executor->terminatedTasks.contains(taskId)) {
        task = executor->terminatedTasks[taskId];
      }

      if (task != nullptr) {
        // We set the status update state of the task here because in
        // steady state controller updates the status update state of the
        // task when it receives this update. If the controller fails over,
        // node reregisters with this task in this status update
        // state. Note that an acknowledgement for this update might be
        // enqueued on task status update manager when we are here. But
        // that is ok because the status update state will be updated
        // when the next update is forwarded to the node.
        task->set_status_update_state(update.status().state());
        task->set_status_update_uuid(update.uuid());

        // Include the latest state of task in the update. See the
        // comments in 'statusUpdate()' on why informing the controller
        // about the latest state of the task is important.
        update.set_latest_state(task->state());
      }
    }
  }

  CHECK_SOME(controller);
  LOG(INFO) << "Forwarding the update " << update << " to " << controller.get();

  // NOTE: We forward the update even if framework/executor/task doesn't
  // exist because the task status update manager will be expecting an
  // acknowledgement for the update. This could happen for example if
  // this is a retried terminal update and before we are here the node
  // has already processed the acknowledgement of the original update
  // and removed the framework/executor/task. Also, node
  // re-registration can generate updates when framework/executor/task
  // are unknown.

  // Forward the update to controller.
  StatusUpdateMessage message;
  message.mutable_update()->MergeFrom(update);
  message.set_pid(self()); // The ACK will be first received by the node.

  send(controller.get(), message);
}


void Node::sendOperationStatusUpdate(
    const UpdateOperationStatusMessage& update)
{
  const UUID& operationUUID = update.operation_uuid();

  Operation* operation = getOperation(operationUUID);

  // TODO(greggomann): Make a note here of which cases may lead to
  // the operation being unknown by the node.
  if (operation != nullptr) {
    updateOperation(operation, update);
  }

  switch (state) {
    case RECOVERING:
    case DISCONNECTED:
    case TERMINATING: {
      LOG(WARNING)
        << "Dropping status update of operation"
        << (update.status().has_operation_id()
             ? " '" + stringify(update.status().operation_id()) + "'"
             : " with no ID")
        << " (operation_uuid: " << operationUUID << ")"
        << (update.has_framework_id()
             ? " for framework " + stringify(update.framework_id())
             : " for an operator API call")
        << " because node is in " << state << " state";
      break;
    }
    case RUNNING: {
      LOG(INFO)
        << "Forwarding status update of"
        << (operation == nullptr ? " unknown" : "") << " operation"
        << (update.status().has_operation_id()
             ? " '" + stringify(update.status().operation_id()) + "'"
             : " with no ID")
        << " (operation_uuid: " << operationUUID << ")"
        << (update.has_framework_id()
             ? " for framework " + stringify(update.framework_id())
             : " for an operator API call");

      send(controller.get(), update);
      break;
    }
  }
}


void Node::executorMessage(
    const NodeID& nodeId,
    const FrameworkID& frameworkId,
    const ExecutorID& executorId,
    const string& data)
{
  CHECK(state == RECOVERING || state == DISCONNECTED ||
        state == RUNNING || state == TERMINATING)
    << state;

  if (state != RUNNING) {
    LOG(WARNING) << "Dropping framework message from executor '"
                 << executorId << "' to framework " << frameworkId
                 << " because the node is in " << state << " state";
    metrics.invalid_framework_messages++;
    return;
  }

  Framework* framework = getFramework(frameworkId);
  if (framework == nullptr) {
    LOG(WARNING) << "Cannot send framework message from executor '"
                 << executorId << "' to framework " << frameworkId
                 << " because framework does not exist";
    metrics.invalid_framework_messages++;
    return;
  }

  CHECK(framework->state == Framework::RUNNING ||
        framework->state == Framework::TERMINATING)
    << framework->state;

  if (framework->state == Framework::TERMINATING) {
    LOG(WARNING) << "Ignoring framework message from executor '"
                 << executorId << "' to framework " << frameworkId
                 << " because framework is terminating";
    metrics.invalid_framework_messages++;
    return;
  }

  ExecutorToFrameworkMessage message;
  message.mutable_node_id()->MergeFrom(nodeId);
  message.mutable_framework_id()->MergeFrom(frameworkId);
  message.mutable_executor_id()->MergeFrom(executorId);
  message.set_data(data);

  CHECK_SOME(controller);

  if (framework->pid.isSome()) {
    LOG(INFO) << "Sending message for framework " << frameworkId
              << " to " << framework->pid.get();
    send(framework->pid.get(), message);
  } else {
    LOG(INFO) << "Sending message for framework " << frameworkId
              << " through the controller " << controller.get();
    send(controller.get(), message);
  }

  metrics.valid_framework_messages++;
}


// NOTE: The node will respond to pings from the controller even if it is
// not in the RUNNING state. This is because node recovery might take
// longer than the controller's ping timeout. We don't want to cause
// cluster churn by marking such nodes unreachable. If the controller
// sees a broken node socket, it waits `node_reregister_timeout` for
// the node to reregister, which implies that recovery should finish
// within that (more generous) timeout.
void Node::ping(const UPID& from, bool connected)
{
  VLOG(2) << "Received ping from " << from;

  if (!connected && state == RUNNING) {
    // This could happen if there is a one-way partition between
    // the controller and node, causing the controller to get an exited
    // event and marking the node disconnected but the node
    // thinking it is still connected. Force a re-registration with
    // the controller to reconcile.
    LOG(INFO) << "Controller marked the node as disconnected but the node"
              << " considers itself registered! Forcing re-registration.";
    detection.discard();
  }

  // We just received a ping from the controller, so reset the ping timer.
  Clock::cancel(pingTimer);

  pingTimer = delay(
      controllerPingTimeout,
      self(),
      &Node::pingTimeout,
      detection);

  send(from, PongNodeMessage());
}


void Node::pingTimeout(Future<Option<ControllerInfo>> future)
{
  // It's possible that a new ping arrived since the timeout fired
  // and we were unable to cancel this timeout. If this occurs, don't
  // bother trying to re-detect.
  if (pingTimer.timeout().expired()) {
    LOG(INFO) << "No pings from controller received within "
              << controllerPingTimeout;

    future.discard();
  }
}


void Node::exited(const UPID& pid)
{
  LOG(INFO) << "Got exited event for " << pid;

  if (controller.isNone() || controller.get() == pid) {
    // TODO(neilc): Try to re-link to the controller (MESOS-1963).
    // TODO(benh): After so long waiting for a controller, commit suicide.
    LOG(WARNING) << "Controller disconnected!"
                 << " Waiting for a new controller to be elected";
  }
}


Framework* Node::getFramework(const FrameworkID& frameworkId) const
{
  if (frameworks.count(frameworkId) > 0) {
    return frameworks.at(frameworkId);
  }

  return nullptr;
}


Executor* Node::getExecutor(
    const FrameworkID& frameworkId,
    const ExecutorID& executorId) const
{
  Framework* framework = getFramework(frameworkId);
  if (framework != nullptr) {
    return framework->getExecutor(executorId);
  }

  return nullptr;
}


Executor* Node::getExecutor(const ContainerID& containerId) const
{
  const ContainerID rootContainerId = protobuf::getRootContainerId(containerId);

  // Locate the executor (for now we just loop since we don't
  // index based on container id and this likely won't have a
  // significant performance impact due to the low number of
  // executors per-node).
  foreachvalue (Framework* framework, frameworks) {
    foreachvalue (Executor* executor, framework->executors) {
      if (rootContainerId == executor->containerId) {
        return executor;
      }
    }
  }

  return nullptr;
}


ExecutorInfo Node::getExecutorInfo(
    const FrameworkInfo& frameworkInfo,
    const TaskInfo& task) const
{
  // In the case of tasks launched as part of a task group, the task group's
  // ExecutorInfo is injected into each TaskInfo by the controller and we return
  // it here.
  if (task.has_executor()) {
    return task.executor();
  }

  ExecutorInfo executor;

  // Command executors share the same id as the task.
  executor.mutable_executor_id()->set_value(task.task_id().value());
  executor.mutable_framework_id()->CopyFrom(frameworkInfo.id());

  if (task.has_container()) {
    // Store the container info in the executor info so it will
    // be checkpointed. This allows the correct containerizer to
    // recover this task on restart.
    executor.mutable_container()->CopyFrom(task.container());
  }

  // Prepare an executor name which includes information on the
  // command being launched.
  string name = "(Task: " + task.task_id().value() + ") ";

  if (task.command().shell()) {
    if (!task.command().has_value()) {
      name += "(Command: NO COMMAND)";
    } else {
      name += "(Command: sh -c '";
      if (task.command().value().length() > 15) {
        name += task.command().value().substr(0, 12) + "...')";
      } else {
        name += task.command().value() + "')";
      }
    }
  } else {
    if (!task.command().has_value()) {
      name += "(Command: NO EXECUTABLE)";
    } else {
      string args =
        task.command().value() + ", " +
        strings::join(", ", task.command().arguments());

      if (args.length() > 15) {
        name += "(Command: [" + args.substr(0, 12) + "...])";
      } else {
        name += "(Command: [" + args + "])";
      }
    }
  }

  executor.set_name("Command Executor " + name);
  executor.set_source(task.task_id().value());

  // Copy the [uris, environment, container, user] fields from the CommandInfo
  // to get the URIs we need to download, the environment variables that should
  // get set, the necessary container information, and the user to run the
  // executor as but nothing else because we need to set up the rest of the
  // executor command ourselves in order to invoke 'medios-executor'.

  executor.mutable_command()->mutable_uris()->MergeFrom(task.command().uris());

  if (task.command().has_environment()) {
    executor.mutable_command()->mutable_environment()->MergeFrom(
        task.command().environment());
  }

  // Add fields which can be relevant (depending on Authorizer) for
  // authorization.

  if (task.has_labels()) {
    executor.mutable_labels()->MergeFrom(task.labels());
  }

  if (task.has_discovery()) {
    executor.mutable_discovery()->MergeFrom(task.discovery());
  }

  // Adjust the executor shutdown grace period if the kill policy is
  // set. We add a small buffer of time to avoid destroying the
  // container before `TASK_KILLED` is sent by the executor.
  //
  // TODO(alexr): Remove `MAX_REAP_INTERVAL` once the reaper signals
  // immediately after the watched process has exited.
  if (task.has_kill_policy() &&
      task.kill_policy().has_grace_period()) {
    Duration gracePeriod =
      Nanoseconds(task.kill_policy().grace_period().nanoseconds()) +
      process::MAX_REAP_INTERVAL() +
      Seconds(1);

    executor.mutable_shutdown_grace_period()->set_nanoseconds(
        gracePeriod.ns());
  }

  if (task.command().has_user()) {
    executor.mutable_command()->set_user(task.command().user());
  }

  Result<string> path = os::realpath(
      path::join(flags.launcher_dir, MEDIOS_EXECUTOR));

  if (path.isSome()) {
    executor.mutable_command()->set_shell(false);
    executor.mutable_command()->set_value(path.get());
    executor.mutable_command()->add_arguments(MEDIOS_EXECUTOR);
    executor.mutable_command()->add_arguments(
        "--launcher_dir=" + flags.launcher_dir);

    // TODO(jieyu): We should move those Medios containerizer specific
    // logic (e.g., 'hasRootfs') to Medios containerizer.
    bool hasRootfs = task.has_container() &&
                     task.container().type() == ContainerInfo::MEDIOS &&
                     task.container().medios().has_image();

    if (hasRootfs) {
      executor.mutable_command()->add_arguments(
          "--sandbox_directory=" + flags.sandbox_directory);

      // NOTE: if switch_user flag is false and the node runs under
      // a non-root user, the task will be rejected by the Posix
      // filesystem isolator. Linux filesystem isolator requires node
      // to have root permission.
      if (flags.switch_user) {
        string user;
        if (task.command().has_user()) {
          user = task.command().user();
        } else {
          user = frameworkInfo.user();
        }

        executor.mutable_command()->add_arguments("--user=" + user);
      }
    }
  } else {
    executor.mutable_command()->set_shell(true);
    executor.mutable_command()->set_value(
        "echo '" +
        (path.isError() ? path.error() : "No such file or directory") +
        "'; exit 1");
  }

  // Add an allowance for the command (or docker) executor. This does
  // lead to a small overcommit of resources.
  //
  // NOTE: The size of the memory is truncated here to preserve the
  // existing behavior for backward compatibility.
  // TODO(vinod): If a task is using revocable resources, mark the
  // corresponding executor resource (e.g., cpus) to be also
  // revocable. Currently, it is OK because the containerizer is
  // given task + executor resources on task launch resulting in
  // the container being correctly marked as revocable.
  Resources executorOverhead = Resources::parse(
      "cpus:" + stringify(DEFAULT_EXECUTOR_CPUS) + ";" +
      "mem:" + stringify(
          DEFAULT_EXECUTOR_MEM.bytes() / Bytes::MEGABYTES)).get();

  // If the task has an allocation role, we inject it into
  // the executor as well. Note that old controllers will not
  // ensure the allocation info is set, and the node will
  // inject this later, when storing the task/executor.
  Option<string> role = None();
  foreach (const Resource& resource, task.resources()) {
    if (role.isNone() && resource.has_allocation_info()) {
      role = resource.allocation_info().role();
    }

    // Check that the roles are consistent.
    Option<string> otherRole = resource.has_allocation_info()
        ? Option<string>(resource.allocation_info().role()) : None();

    CHECK(role == otherRole)
      << (role.isSome() ? role.get() : "None")
      << " vs " << (otherRole.isSome() ? otherRole.get() : "None");
  }

  if (role.isSome()) {
    executorOverhead.allocate(role.get());
  }

  executor.mutable_resources()->CopyFrom(executorOverhead);

  return executor;
}


void Node::executorLaunched(
    const FrameworkID& frameworkId,
    const ExecutorID& executorId,
    const ContainerID& containerId,
    const Future<Containerizer::LaunchResult>& future)
{
  // Set up callback for executor termination. Note that we do this
  // regardless of whether or not we have successfully launched the
  // executor because even if we failed to launch the executor the
  // result of calling 'wait' will make sure everything gets properly
  // cleaned up. Note that we do this here instead of where we do
  // Containerizer::launch because we want to guarantee the contract
  // with the Containerizer that we won't call 'wait' until after the
  // launch has completed.
  containerizer->wait(containerId)
    .onAny(defer(self(),
                 &Self::executorTerminated,
                 frameworkId,
                 executorId,
                 lambda::_1));

  if (!future.isReady()) {
    LOG(ERROR) << "Container '" << containerId
               << "' for executor '" << executorId
               << "' of framework " << frameworkId
               << " failed to start: "
               << (future.isFailed() ? future.failure() : "future discarded");

    ++metrics.container_launch_errors;

    containerizer->destroy(containerId);

    Executor* executor = getExecutor(frameworkId, executorId);
    if (executor != nullptr) {
      // The containerizer's sentence, verbatim: it names the volume, the
      // store or the path a framework can act on, and the reason already
      // says the container failed to launch. A prefix here would be the
      // one thing a framework had to strip to show its caller the cause.
      ContainerTermination termination;
      termination.set_state(TASK_FAILED);
      termination.set_reason(TaskStatus::REASON_CONTAINER_LAUNCH_FAILED);
      termination.set_message(
          future.isFailed()
            ? future.failure()
            : "The container launch was discarded before it completed");

      executor->pendingTermination = termination;

      // TODO(jieyu): Set executor->state to be TERMINATING.
    }

    return;
  } else if (future.get() == Containerizer::LaunchResult::NOT_SUPPORTED) {
    const string message =
        "None of the configured containerizers (" + flags.containerizers +
        ") can launch the provided TaskInfo. The configured containerizer(s) "
        "rejected the task shape (typically a missing or incompatible "
        "ContainerInfo).";

    LOG(ERROR) << "Container '" << containerId
               << "' for executor '" << executorId
               << "' of framework " << frameworkId
               << " failed to start: " << message;

    ++metrics.container_launch_errors;

    // Carry the rejection into pendingTermination; otherwise the per-task
    // status updates emitted downstream fall through to TASK_FAILED +
    // REASON_EXECUTOR_TERMINATED defaults, hiding the real reason.
    Executor* executor = getExecutor(frameworkId, executorId);
    if (executor != nullptr) {
      ContainerTermination termination;
      termination.set_state(TASK_ERROR);
      termination.set_reason(TaskStatus::REASON_TASK_INVALID);
      termination.set_message(message);
      executor->pendingTermination = termination;
    }

    return;
  } else if (future.get() == Containerizer::LaunchResult::ALREADY_LAUNCHED) {
    // This should be extremely rare, as the user would need to launch a
    // standalone container with a user-specified UUID that happens to
    // collide with the Node-generated ContainerID for this launch.
    LOG(ERROR) << "Container '" << containerId
               << "' for executor '" << executorId
               << "' of framework " << frameworkId
               << " has already been launched.";
    return;
  }

  Framework* framework = getFramework(frameworkId);
  if (framework == nullptr) {
    LOG(WARNING) << "Framework '" << frameworkId
                 << "' for executor '" << executorId
                 << "' is no longer valid";
    return;
  }

  CHECK(framework->state == Framework::RUNNING ||
        framework->state == Framework::TERMINATING)
    << framework->state;

  if (framework->state == Framework::TERMINATING) {
    LOG(WARNING) << "Killing executor '" << executorId
                 << "' of framework " << frameworkId
                 << " because the framework is terminating";
    containerizer->destroy(containerId);
    return;
  }

  Executor* executor = framework->getExecutor(executorId);
  if (executor == nullptr) {
    LOG(WARNING) << "Killing unknown executor '" << executorId
                 << "' of framework " << frameworkId;
    containerizer->destroy(containerId);
    return;
  }

  switch (executor->state) {
    case Executor::TERMINATING:
      LOG(WARNING) << "Killing executor " << *executor
                   << " because the executor is terminating";

      containerizer->destroy(containerId);
      break;
    case Executor::REGISTERING:
    case Executor::RUNNING:
      break;
    case Executor::TERMINATED:
    default:
      LOG(FATAL) << "Executor " << *executor << " is in an unexpected state "
                 << executor->state;

      break;
  }
}


void Node::executorTerminated(
    const FrameworkID& frameworkId,
    const ExecutorID& executorId,
    const Future<Option<ContainerTermination>>& termination)
{
  int status;
  // A termination failure indicates the containerizer could not destroy a
  // container.
  // TODO(idownes): This is a serious error so consider aborting the node if
  // this occurs.
  if (!termination.isReady()) {
    LOG(ERROR) << "Termination of executor '" << executorId
               << "' of framework " << frameworkId
               << " failed: "
               << (termination.isFailed()
                   ? termination.failure()
                   : "discarded");
    // Set a special status for failure.
    status = -1;
  } else if (termination->isNone()) {
    LOG(ERROR) << "Termination of executor '" << executorId
               << "' of framework " << frameworkId
               << " failed: unknown container";
    // Set a special status for failure.
    status = -1;
  } else if (!termination->get().has_status()) {
    LOG(INFO) << "Executor '" << executorId
              << "' of framework " << frameworkId
              << " has terminated with unknown status";
    // Set a special status for None.
    status = -1;
  } else {
    status = termination->get().status();
    LOG(INFO) << "Executor '" << executorId
              << "' of framework " << frameworkId << " "
              << WSTRINGIFY(status);
  }

  Framework* framework = getFramework(frameworkId);
  if (framework == nullptr) {
    LOG(WARNING) << "Framework " << frameworkId
                 << " for executor '" << executorId
                 << "' does not exist";
    return;
  }

  CHECK(framework->state == Framework::RUNNING ||
        framework->state == Framework::TERMINATING)
    << framework->state;

  Executor* executor = framework->getExecutor(executorId);
  if (executor == nullptr) {
    LOG(WARNING) << "Executor '" << executorId
                 << "' of framework " << frameworkId
                 << " does not exist";
    return;
  }

  switch (executor->state) {
    case Executor::REGISTERING:
    case Executor::RUNNING:
    case Executor::TERMINATING: {
      ++metrics.executors_terminated;

      executor->state = Executor::TERMINATED;

      // Transition all live tasks to TASK_GONE/TASK_FAILED.
      // If the containerizer killed the executor (e.g., due to OOM event)
      // or if this is a command executor, we send TASK_FAILED status updates
      // instead of TASK_GONE.
      // NOTE: We don't send updates if the framework is terminating because we
      // don't want the task status update manager to keep retrying these
      // updates since it won't receive ACKs from the scheduler.  Also, the task
      // status update manager should have already cleaned up all the status
      // update streams for a framework that is terminating.
      if (framework->state != Framework::TERMINATING) {
        // Transition all live launched tasks. Note that the map is
        // removed from within the loop due terminal status updates.
        foreach (const TaskID& taskId, executor->launchedTasks.keys()) {
          Task* task = executor->launchedTasks.at(taskId);

          if (!protobuf::isTerminalState(task->state())) {
            sendExecutorTerminatedStatusUpdate(
                taskId, termination, frameworkId, executor);
          }
        }

        // Transition all queued tasks. Note that the map is removed
        // from within the loop due terminal status updates.
        foreach (const TaskID& taskId, executor->queuedTasks.keys()) {
          sendExecutorTerminatedStatusUpdate(
              taskId, termination, frameworkId, executor);
        }
      } else {
        // When the framework is TERMINATING, we cannot send status updates
        // for "launched tasks", but these tasks no longer belong in the
        // `launchedTasks` structure. These tasks will continue to show
        // in the node's state (as a completed executor) for some time
        // after the framework/executor terminates.
        foreachpair (
            const TaskID& taskId,
            Task* task,
            utils::copy(executor->launchedTasks)) {
          executor->launchedTasks.erase(taskId);
          executor->terminatedTasks[taskId] = task;
        }
      }

      // Only send ExitedExecutorMessage if it is not a Command (or
      // Docker) Executor because the controller doesn't store them; they
      // are generated by the node.
      // TODO(vinod): Reliably forward this message to the controller.
      if (!executor->isGeneratedForCommandTask()) {
        sendExitedExecutorMessage(frameworkId, executorId, status);
      }

      // Remove the executor if either the node or framework is
      // terminating or there are no incomplete tasks.
      if (state == TERMINATING ||
          framework->state == Framework::TERMINATING ||
          !executor->incompleteTasks()) {
        removeExecutor(framework, executor);
      }

      // Remove this framework if it has no pending executors and tasks.
      if (framework->idle()) {
        removeFramework(framework);
      }
      break;
    }
    default:
      LOG(FATAL) << "Executor '" << executor->id
                 << "' of framework " << framework->id()
                 << " in unexpected state " << executor->state;
      break;
  }
}


void Node::removeExecutor(Framework* framework, Executor* executor)
{
  CHECK_NOTNULL(framework);
  CHECK_NOTNULL(executor);

  LOG(INFO) << "Cleaning up executor " << *executor;

  CHECK(framework->state == Framework::RUNNING ||
        framework->state == Framework::TERMINATING)
    << framework->state;

  // Check that this executor has terminated.
  CHECK(executor->state == Executor::TERMINATED) << executor->state;

  // Check that either 1) the executor has no tasks with pending
  // updates or 2) the node/framework is terminating, because no
  // acknowledgements might be received.
  CHECK(!executor->incompleteTasks() ||
        state == TERMINATING ||
        framework->state == Framework::TERMINATING);

  // Write a sentinel file to indicate that this executor
  // is completed.
  if (executor->checkpoint) {
    const string path = paths::getExecutorSentinelPath(
        metaDir,
        info.id(),
        framework->id(),
        executor->id,
        executor->containerId);
    CHECK_SOME(os::touch(path));
  }

  // TODO(vinod): Move the responsibility of gc'ing to the
  // Executor struct.

  // Schedule the executor run work directory to get garbage collected.
  const string path = paths::getExecutorRunPath(
      flags.work_dir,
      info.id(),
      framework->id(),
      executor->id,
      executor->containerId);

  // NOTE: We keep a list of default executor tasks here to for
  // detaching task volume directories, since the executor may be
  // already destroyed when the GC completes (MESOS-8460).
  vector<Task> defaultExecutorTasks;
  if (executor->info.has_type() &&
      executor->info.type() == ExecutorInfo::DEFAULT) {
    foreachvalue (const Task* task, executor->launchedTasks) {
      defaultExecutorTasks.push_back(*task);
    }

    foreachvalue (const Task* task, executor->terminatedTasks) {
      defaultExecutorTasks.push_back(*task);
    }

    foreach (const shared_ptr<Task>& task, executor->completedTasks) {
      defaultExecutorTasks.push_back(*task);
    }
  }

  os::utime(path); // Update the modification time.
  garbageCollect(path)
    .onAny(defer(self(), &Self::detachFile, path))
    .onAny(defer(
        self(),
        &Self::detachTaskVolumeDirectories,
        executor->info,
        executor->containerId,
        defaultExecutorTasks));

  // Schedule the top level executor work directory, only if the
  // framework doesn't have any 'pending' tasks for this executor.
  if (!framework->pendingTasks.contains(executor->id)) {
    const string path = paths::getExecutorPath(
        flags.work_dir, info.id(), framework->id(), executor->id);

    // Make sure we detach both real and virtual paths for "latest"
    // symlink. We prefer users to use the virtual paths because
    // they do not expose the `work_dir` and node ID, but the real
    // paths remains for compatibility reason.
    const string latestPath = paths::getExecutorLatestRunPath(
        flags.work_dir,
        info.id(),
        framework->id(),
        executor->id);

    const string virtualLatestPath = paths::getExecutorVirtualPath(
        framework->id(),
        executor->id);

    os::utime(path); // Update the modification time.
    garbageCollect(path)
      .onAny(defer(self(), &Self::detachFile, latestPath))
      .onAny(defer(self(), &Self::detachFile, virtualLatestPath));
  }

  if (executor->checkpoint) {
    // Schedule the executor run meta directory to get garbage collected.
    const string path = paths::getExecutorRunPath(
        metaDir,
        info.id(),
        framework->id(),
        executor->id,
        executor->containerId);

    os::utime(path); // Update the modification time.
    garbageCollect(path);

    // Schedule the top level executor meta directory, only if the
    // framework doesn't have any 'pending' tasks for this executor.
    if (!framework->pendingTasks.contains(executor->id)) {
      const string path = paths::getExecutorPath(
          metaDir, info.id(), framework->id(), executor->id);

      os::utime(path); // Update the modification time.
      garbageCollect(path);
    }
  }

  if (HookManager::hooksAvailable()) {
    HookManager::nodeRemoveExecutorHook(framework->info, executor->info);
  }

  framework->destroyExecutor(executor->id);
}


void Node::removeFramework(Framework* framework)
{
  CHECK_NOTNULL(framework);

  LOG(INFO)<< "Cleaning up framework " << framework->id();

  CHECK(framework->state == Framework::RUNNING ||
        framework->state == Framework::TERMINATING);

  // We only remove frameworks once they become idle.
  CHECK(framework->idle());

  // Close all task status update streams for this framework.
  taskStatusUpdateManager->cleanup(framework->id());

  // Schedule the framework work and meta directories for garbage
  // collection.
  // TODO(vinod): Move the responsibility of gc'ing to the
  // Framework struct.

  const string path = paths::getFrameworkPath(
      flags.work_dir, info.id(), framework->id());

  os::utime(path); // Update the modification time.
  garbageCollect(path);

  if (framework->info.checkpoint()) {
    // Schedule the framework meta directory to get garbage collected.
    const string path = paths::getFrameworkPath(
        metaDir, info.id(), framework->id());

    os::utime(path); // Update the modification time.
    garbageCollect(path);
  }

  frameworks.erase(framework->id());

  // Pass ownership of the framework pointer.
  completedFrameworks.set(framework->id(), Owned<Framework>(framework));

  updateDrainStatus();

  if (state == TERMINATING && frameworks.empty()) {
    terminate(self());
  }
}


void Node::shutdownExecutor(
    const UPID& from,
    const FrameworkID& frameworkId,
    const ExecutorID& executorId)
{
  if (from && controller != from) {
    LOG(WARNING) << "Ignoring shutdown executor message for executor '"
                 << executorId << "' of framework " << frameworkId
                 << " from " << from << " because it is not from the"
                 << " registered controller ("
                 << (controller.isSome() ? stringify(controller.get()) : "None") << ")";
    return;
  }

  LOG(INFO) << "Asked to shut down executor '" << executorId
            << "' of framework " << frameworkId << " by " << from;

  CHECK(state == RECOVERING || state == DISCONNECTED ||
        state == RUNNING || state == TERMINATING)
    << state;

  if (state == RECOVERING || state == DISCONNECTED) {
    LOG(WARNING) << "Ignoring shutdown executor message for executor '"
                 << executorId << "' of framework " << frameworkId
                 << " because the node has not yet registered with the controller";
    return;
  }

  Framework* framework = getFramework(frameworkId);
  if (framework == nullptr) {
    LOG(WARNING) << "Cannot shut down executor '" << executorId
                 << "' of unknown framework " << frameworkId;
    return;
  }

  CHECK(framework->state == Framework::RUNNING ||
        framework->state == Framework::TERMINATING)
    << framework->state;

  if (framework->state == Framework::TERMINATING) {
    LOG(WARNING) << "Ignoring shutdown executor '" << executorId
                 << "' of framework " << frameworkId
                 << " because the framework is terminating";
    return;
  }

  if (!framework->executors.contains(executorId)) {
    LOG(WARNING) << "Ignoring shutdown of unknown executor '" << executorId
                 << "' of framework " << frameworkId;
    return;
  }

  Executor* executor = framework->executors[executorId];
  CHECK(executor->state == Executor::REGISTERING ||
        executor->state == Executor::RUNNING ||
        executor->state == Executor::TERMINATING ||
        executor->state == Executor::TERMINATED)
    << executor->state;

  if (executor->state == Executor::TERMINATING) {
    LOG(WARNING) << "Ignoring shutdown executor '" << executorId
                 << "' of framework " << frameworkId
                 << " because the executor is terminating";
    return;
  }

  if (executor->state == Executor::TERMINATED) {
    LOG(WARNING) << "Ignoring shutdown executor '" << executorId
                 << "' of framework " << frameworkId
                 << " because the executor is terminated";
    return;
  }

  _shutdownExecutor(framework, executor);
}


void Node::_shutdownExecutor(Framework* framework, Executor* executor)
{
  CHECK_NOTNULL(framework);
  CHECK_NOTNULL(executor);

  LOG(INFO) << "Shutting down executor " << *executor;

  CHECK(framework->state == Framework::RUNNING ||
        framework->state == Framework::TERMINATING)
    << framework->state;

  CHECK(executor->state == Executor::REGISTERING ||
        executor->state == Executor::RUNNING)
    << executor->state;

  executor->state = Executor::TERMINATING;

  // If the executor hasn't yet registered, this message
  // will be dropped to the floor!
  executor->send(ShutdownExecutorMessage());

  // If the executor specifies shutdown grace period,
  // pass it instead of the default.
  Duration shutdownTimeout = flags.executor_shutdown_grace_period;
  if (executor->info.has_shutdown_grace_period()) {
    shutdownTimeout = Nanoseconds(
        executor->info.shutdown_grace_period().nanoseconds());
  }

  // Prepare for sending a kill if the executor doesn't comply.
  delay(shutdownTimeout,
        self(),
        &Node::shutdownExecutorTimeout,
        framework->id(),
        executor->id,
        executor->containerId);
}


void Node::shutdownExecutorTimeout(
    const FrameworkID& frameworkId,
    const ExecutorID& executorId,
    const ContainerID& containerId)
{
  Framework* framework = getFramework(frameworkId);
  if (framework == nullptr) {
    LOG(INFO) << "Framework " << frameworkId
              << " seems to have exited. Ignoring shutdown timeout"
              << " for executor '" << executorId << "'";
    return;
  }

  CHECK(framework->state == Framework::RUNNING ||
        framework->state == Framework::TERMINATING)
    << framework->state;

  Executor* executor = framework->getExecutor(executorId);
  if (executor == nullptr) {
    VLOG(1) << "Executor '" << executorId
            << "' of framework " << frameworkId
            << " seems to have exited. Ignoring its shutdown timeout";
    return;
  }

  // Make sure this timeout is valid.
  if (executor->containerId != containerId) {
    LOG(INFO) << "A new executor " << *executor
              << " with run " << executor->containerId
              << " seems to be active. Ignoring the shutdown timeout"
              << " for the old executor run " << containerId;
    return;
  }

  switch (executor->state) {
    case Executor::TERMINATED:
      LOG(INFO) << "Executor " << *executor << " has already terminated";
      break;
    case Executor::TERMINATING:
      LOG(INFO) << "Killing executor " << *executor;

      containerizer->destroy(executor->containerId);
      break;
    default:
      LOG(FATAL) << "Executor " << *executor << " is in unexpected state "
                 << executor->state;
      break;
  }
}


void Node::registerExecutorTimeout(
    const FrameworkID& frameworkId,
    const ExecutorID& executorId,
    const ContainerID& containerId)
{
  Framework* framework = getFramework(frameworkId);
  if (framework == nullptr) {
    LOG(INFO) << "Framework " << frameworkId
              << " seems to have exited. Ignoring registration timeout"
              << " for executor '" << executorId << "'";
    return;
  }

  CHECK(framework->state == Framework::RUNNING ||
        framework->state == Framework::TERMINATING)
    << framework->state;

  if (framework->state == Framework::TERMINATING) {
    LOG(INFO) << "Ignoring registration timeout for executor '" << executorId
              << "' because the  framework " << frameworkId
              << " is terminating";
    return;
  }

  Executor* executor = framework->getExecutor(executorId);
  if (executor == nullptr) {
    VLOG(1) << "Executor '" << executorId
            << "' of framework " << frameworkId
            << " seems to have exited. Ignoring its registration timeout";
    return;
  }

  if (executor->containerId != containerId) {
    LOG(INFO) << "A new executor " << *executor
              << " with run " << executor->containerId
              << " seems to be active. Ignoring the registration timeout"
              << " for the old executor run " << containerId;
    return;
  }

  switch (executor->state) {
    case Executor::RUNNING:
    case Executor::TERMINATING:
    case Executor::TERMINATED:
      // Ignore the registration timeout.
      break;
    case Executor::REGISTERING: {
      LOG(INFO) << "Terminating executor " << *executor
                << " because it did not register within "
                << flags.executor_registration_timeout;

      // Immediately kill the executor.
      containerizer->destroy(containerId);

      executor->state = Executor::TERMINATING;

      ContainerTermination termination;
      termination.set_state(TASK_FAILED);
      termination.set_reason(TaskStatus::REASON_EXECUTOR_REGISTRATION_TIMEOUT);
      termination.set_message(
          "Executor did not register within " +
          stringify(flags.executor_registration_timeout));

      executor->pendingTermination = termination;
      break;
    }
    default:
      LOG(FATAL) << "Executor " << *executor << " is in unexpected state "
                 << executor->state;
      break;
  }
}


// TODO(vinod): Figure out a way to express this function via cmd line.
Duration Node::age(double usage)
{
  return flags.gc_delay * std::max(0.0, (1.0 - flags.gc_disk_headroom - usage));
}


void Node::checkDiskUsage()
{
  // TODO(vinod): We are making usage a Future, so that we can plug in
  // fs::usage() into async.
  // NOTE: We calculate disk usage of the file system on which the
  // node work directory is mounted.
  Future<double>(::fs::usage(flags.work_dir))
    .onAny(defer(self(), &Node::_checkDiskUsage, lambda::_1));
}


void Node::_checkDiskUsage(const Future<double>& usage)
{
  if (!usage.isReady()) {
    LOG(ERROR) << "Failed to get disk usage: "
               << (usage.isFailed() ? usage.failure() : "future discarded");
  } else {
    executorDirectoryMaxAllowedAge = age(usage.get());
    LOG(INFO) << "Current disk usage " << std::setiosflags(std::ios::fixed)
              << std::setprecision(2) << 100 * usage.get() << "%."
              << " Max allowed age: " << executorDirectoryMaxAllowedAge;

    // We prune all directories whose deletion time is within
    // the next 'gc_delay - age'. Since a directory is always
    // scheduled for deletion 'gc_delay' into the future, only directories
    // that are at least 'age' old are deleted.
    gc->prune(flags.gc_delay - executorDirectoryMaxAllowedAge);
  }
  delay(flags.disk_watch_interval, self(), &Node::checkDiskUsage);
}


Try<Nothing> Node::compatible(
  const NodeInfo& previous,
  const NodeInfo& current) const
{
  // TODO(vinod): Also check for version compatibility.

  if (flags.reconfiguration_policy == "equal") {
    return compatibility::equal(previous, current);
  }

  if (flags.reconfiguration_policy == "additive") {
    return compatibility::additive(previous, current);
  }

  // Should have been validated during startup.
  UNREACHABLE();
}


// TODO(gilbert): Consider to read the Image GC config dynamically.
// For now, the Image GC config can only be updated after the node
// restarts.
void Node::checkImageDiskUsage()
{
  // TODO(gilbert): Container image gc is supported for docker image
  // in Medios Containerizer for now. Add more image store gc supports
  // if necessary.
  Future<double>(::fs::usage(flags.docker_store_dir))
    .onAny(defer(self(), &Node::_checkImageDiskUsage, lambda::_1));
}


void Node::_checkImageDiskUsage(const Future<double>& usage)
{
  CHECK(flags.image_gc_config.isSome());

  if (!usage.isReady()) {
    LOG(ERROR) << "Failed to get image store disk usage: "
               << (usage.isFailed() ? usage.failure() : "future discarded");
  } else {
    LOG(INFO) << "Current docker image store disk usage: "
              << std::setiosflags(std::ios::fixed) << std::setprecision(2)
              << 100 * usage.get() << "%.";

    if ((flags.image_gc_config->image_disk_headroom() + usage.get()) > 1.0) {
      LOG(INFO) << "Image store disk usage exceeds the threshold '"
                << 100 * (1.0 - flags.image_gc_config->image_disk_headroom())
                << "%'. Container Image GC is triggered.";

      vector<Image> excludedImages(
          flags.image_gc_config->excluded_images().begin(),
          flags.image_gc_config->excluded_images().end());

      containerizer->pruneImages(excludedImages);
    }
  }

  delay(
      Nanoseconds(
          flags.image_gc_config->image_disk_watch_interval().nanoseconds()),
      self(),
      &Node::checkImageDiskUsage);
}


Future<Nothing> Node::recover(const Try<state::State>& state)
{
  if (state.isError()) {
    return Failure(state.error());
  }

  LOG(INFO) << "Finished recovering checkpointed state from '" << metaDir
            << "', beginning node recovery";

  Option<ResourcesState> resourcesState = state->resources;
  Option<NodeState> nodeState = state->node;

  // With the addition of frameworks with multiple roles, we
  // need to inject the allocated role into each allocated
  // `Resource` object that we've persisted. Note that we
  // also must do this for MULTI_ROLE frameworks since they
  // may have tasks that were present before the framework
  // upgraded into MULTI_ROLE.
  auto injectAllocationInfo = [](
      RepeatedPtrField<Resource>* resources,
      const FrameworkInfo& frameworkInfo) {
    set<string> roles = protobuf::framework::getRoles(frameworkInfo);

    bool injectedAllocationInfo = false;
    foreach (Resource& resource, *resources) {
      if (!resource.has_allocation_info()) {
        if (roles.size() != 1) {
          LOG(FATAL) << "Missing 'Resource.AllocationInfo' for resources"
                     << " allocated to MULTI_ROLE framework"
                     << " '" << frameworkInfo.name() << "'";
        }

        resource.mutable_allocation_info()->set_role(*roles.begin());
        injectedAllocationInfo = true;
      }
    }

    return injectedAllocationInfo;
  };

  // In order to allow frameworks to change their role(s), we need to keep
  // track of the fact that the resources used to be implicitly allocated to
  // `FrameworkInfo.role` before the node upgrade. To this end, we inject
  // the `AllocationInfo` to the resources in `ExecutorState` and `TaskState`,
  // and re-checkpoint them if necessary.

  hashset<ExecutorID> injectedExecutors;
  hashmap<ExecutorID, hashset<TaskID>> injectedTasks;

  if (nodeState.isSome()) {
    foreachvalue (FrameworkState& frameworkState, nodeState->frameworks) {
      if (!frameworkState.info.isSome()) {
        continue;
      }

      foreachvalue (ExecutorState& executorState, frameworkState.executors) {
        if (!executorState.info.isSome()) {
          continue;
        }

        if (injectAllocationInfo(
                executorState.info->mutable_resources(),
                frameworkState.info.get())) {
          injectedExecutors.insert(executorState.id);
        }

        foreachvalue (RunState& runState, executorState.runs) {
          foreachvalue (TaskState& taskState, runState.tasks) {
            if (!taskState.info.isSome()) {
              continue;
            }

            if (injectAllocationInfo(
                    taskState.info->mutable_resources(),
                    frameworkState.info.get())) {
              injectedTasks[executorState.id].insert(taskState.id);
            }
          }
        }
      }
    }
  }

  // Recover checkpointed resources.
  // NOTE: 'resourcesState' is None if the node rootDir does not
  // exist or the resources checkpoint file cannot be found.
  if (resourcesState.isSome()) {
    if (resourcesState->errors > 0) {
      LOG(WARNING) << "Errors encountered during resources recovery: "
                   << resourcesState->errors;

      metrics.recovery_errors += resourcesState->errors;
    }

    checkpointedResources = resourcesState->resources;

    if (resourcesState->target.isSome()) {
      Resources targetResources = resourcesState->target.get();

      // Sync the checkpointed resources from the target (which was
      // only created when there are pending changes in the
      // checkpointed resources). If there is any failure, the
      // checkpoint is not committed and the node exits. In that
      // case, sync of checkpoints will be reattempted on the next
      // node restart (before node reregistration).
      Try<Nothing> syncResult = syncCheckpointedResources(targetResources);

      if (syncResult.isError()) {
        return Failure(
            "Target checkpointed resources " +
            stringify(targetResources) +
            " failed to sync from current checkpointed resources " +
            stringify(checkpointedResources) + ": " +
            syncResult.error());
      }

      // At this point, `syncCheckpointedResources()` has ensured that any
      // change in checkpointed resources (e.g. persistent volumes) is now
      // reflected on disk. We rename the target resource state file to the
      // actual resource state file, which is our source of truth for the
      // current state of the node resources.
      Try<Nothing> renameResult = os::rename(
          paths::getResourceStateTargetPath(metaDir),
          paths::getResourceStatePath(metaDir));

      if (renameResult.isError()) {
        return Failure(
            "Failed to rename target resources " +
            stringify(targetResources) + " and associated operations: " +
            renameResult.error());
      }

      // The following rename call handles the pre-operation-feedback
      // checkpoint format for backward compatibility.
      renameResult = os::rename(
          paths::getResourcesTargetPath(metaDir),
          paths::getResourcesInfoPath(metaDir));

      if (renameResult.isError()) {
        return Failure(
            "Failed to rename target resources " +
            stringify(targetResources) + ": " +
            renameResult.error());
      }

      // Since we synced the target resources to the committed resources, we
      // check resource compatibility with `--resources` command line flag
      // based on the committed checkpoint.
      checkpointedResources = targetResources;
    }

    // This is to verify that the checkpointed resources are
    // compatible with the node resources specified through the
    // '--resources' command line flag. The compatibility has been
    // verified by the old node but the flag may have changed during
    // node restart in an incompatible way and the operator may need
    // to either fix the flag or the checkpointed resources.
    Try<Resources> _totalResources = applyCheckpointedResources(
        info.resources(), checkpointedResources);

    if (_totalResources.isError()) {
      return Failure(
          "Checkpointed resources " +
          stringify(checkpointedResources) +
          " are incompatible with node resources " +
          stringify(info.resources()) + ": " +
          _totalResources.error());
    }

    totalResources = _totalResources.get();
  }

  if (nodeState.isSome() && nodeState->info.isSome()) {
    if (nodeState->errors > 0) {
      LOG(WARNING) << "Errors encountered during node recovery: "
                   << nodeState->errors;

      metrics.recovery_errors += nodeState->errors;
    }

    // Save the previous id into the current `NodeInfo`, so we can compare
    // both of them for equality. This is safe because if it turned out that
    // we can not reuse the id, we will either crash or erase it again.
    info.mutable_id()->CopyFrom(nodeState->info->id());

    drainConfig = nodeState->drainConfig;

    // Check for NodeInfo compatibility.
    Try<Nothing> _compatible =
      compatible(nodeState->info.get(), info);

    if (_compatible.isSome()) {
      // Permitted change, so we reuse the recovered node id and reconnect
      // to running executors.

      // Prior to Medios 1.5, the controller expected that an node would never
      // change its `NodeInfo` and keep the same node id, and therefore would
      // not update it's internal data structures on node re-registration.
      if (!(nodeState->info.get() == info)) {
        requiredControllerCapabilities.nodeUpdate = true;
      }

      // If we restarted the node process and will reuse the same node ID
      // we can immediately start the resource provider manager. This allows
      // executors recovered later on to resubscribe immediately.
      initializeResourceProviderManager(flags, info.id());

      // Recover the frameworks.
      foreachvalue (const FrameworkState& frameworkState,
                    nodeState->frameworks) {
        recoverFramework(frameworkState, injectedExecutors, injectedTasks);
      }
    } else if (state->rebooted) {
      // Prior to Medios 1.4 we directly bypass the state recovery and
      // start as a new node upon reboot (introduced in MESOS-844).
      // This unnecessarily discards the existing node ID (MESOS-6223).
      // Starting in Medios 1.4 we'll attempt to recover the node state
      // even after reboot but in case of an incompatible node info change
      // we'll fall back to recovering as a new node (existing behavior).
      // Prior to Medios 1.5, an incompatible change would be any node info
      // mismatch.
      // This prevents the node from flapping if the node info (resources,
      // attributes, etc.) change is due to host maintenance associated
      // with the reboot.

      LOG(WARNING) << "Falling back to recover as a new node due to error: "
                   << _compatible.error();

      // Cleaning up the node state to avoid any state recovery for the
      // old node.
      info.clear_id();
      nodeState = None();

      // Remove the "latest" symlink if it exists to "checkpoint" the
      // decision to recover as a new node.
      const string& latest = paths::getLatestNodePath(metaDir);
      if (os::exists(latest)) {
        CHECK_SOME(os::rm(latest))
          << "Failed to remove latest symlink '" << latest << "'";
      }
    } else {
      return Failure(_compatible.error());
    }
  }

  return _recoverVolumeGidManager(state->rebooted)
    .then(defer(self(), &Node::_recoverTaskStatusUpdates, nodeState))
    .then(defer(self(), &Node::_recoverContainerizer, nodeState))
    .then(defer(self(), &Node::_recoverOperations, nodeState));
}


Future<Nothing> Node::_recoverVolumeGidManager(bool rebooted)
{
  if (volumeGidManager) {
    return volumeGidManager->recover(rebooted);
  }
  return Nothing();
}


Future<Option<NodeState>> Node::_recoverTaskStatusUpdates(
    const Option<NodeState>& state)
{
  return taskStatusUpdateManager->recover(metaDir, state)
    .then([state]() -> Future<Option<NodeState>> {
      return state;
    });
}


Future<Nothing> Node::_recoverContainerizer(
    const Option<NodeState>& state)
{
  return containerizer->recover(state);
}


Future<Nothing> Node::_recoverOperations(
    const Option<state::NodeState>& state)
{
  if (state.isNone()) {
    return Nothing();
  }

  operationStatusUpdateManager.initialize(
      defer(self(), &Self::sendOperationStatusUpdate, lambda::_1),
      std::bind(
          &node::paths::getNodeOperationUpdatesPath,
          metaDir,
          info.id(),
          lambda::_1));

  if (state->operations.isSome()) {
    foreach (const Operation& operation, state->operations.get()) {
      Result<ResourceProviderID> resourceProviderId =
        getResourceProviderId(operation.info());

      // Only operations affecting node default resources are checkpointed.
      CHECK(resourceProviderId.isNone());

      addOperation(new Operation(operation));
    }
  }

  // Walk the operation status update streams directories in order to generate
  // the list of streams to recover.
  //
  // NOTE: It is possible for the node to fail over right after having
  // checkpointed an operation in a `ResourceState` message, but before having
  // created the corresponding stream.
  //
  // In that case the checkpointed message will contain the operation, but the
  // corresponding directory for the operation status update stream will not
  // exist.
  //
  // Since the SUM recovery process will return an error if invoked with an
  // operation ID for which a stream hasn't been created, we can't extract the
  // list of streams to recover from the content of the checkpointed
  // `ResourceState` message.
  Try<list<string>> operationPaths =
    node::paths::getNodeOperationPaths(metaDir, info.id());

  if (operationPaths.isError()) {
    return Failure(
        "Failed to find operation status update streams: " +
        operationPaths.error());
  }

  list<id::UUID> operationUuids;
  foreach (const string& path, operationPaths.get()) {
    Try<id::UUID> uuid =
      node::paths::parseNodeOperationPath(metaDir, info.id(), path);

    if (uuid.isError()) {
      return Failure(
          "Failed to parse operation status update stream path '" + path +
          "': " + uuid.error());
    }

    UUID uuid_;
    uuid_.set_value(uuid->toBytes());

    // NOTE: This could happen if we failed to remove the operation path before.
    if (!operations.contains(uuid_)) {
      LOG(WARNING)
        << "Garbage collecting status update stream for unknown operation"
        << " (uuid: " << uuid.get() << ")";

      Try<Nothing> rmdir = os::rmdir(path);
      if (rmdir.isError()) {
        LOG(ERROR)
          << "Failed to remove directory '" << path << "': " << rmdir.error();
      }

      continue;
    }

    operationUuids.emplace_back(std::move(uuid.get()));
  }

  return operationStatusUpdateManager.recover(operationUuids, flags.strict)
    .then(defer(self(), &Node::__recoverOperations, lambda::_1));
}


Future<Nothing> Node::__recoverOperations(
  const Future<OperationStatusUpdateManagerState>& state)
{
  if (!state.isReady()) {
    EXIT(EXIT_FAILURE)
      << "Failed to recover operation status update manager: "
      << (state.isFailed() ? state.failure() : "future discarded") << "\n";
  }

  if (state->errors > 0) {
    LOG(WARNING)
      << "Errors encountered during operation status update manager recovery: "
      << state->errors;

    metrics.recovery_errors += state->errors;
  }

  // Clean up operations with terminated streams.
  //
  // These are operations with terminal updates that have already been
  // acknowledged. They could still be checkpointed if the node failed
  // over just before removing them from its state.
  using StreamState = typename OperationStatusUpdateManagerState::StreamState;
  vector<id::UUID> completedOperations;
  foreachpair (const id::UUID& uuid,
               const Option<StreamState>& stream,
               state->streams) {
    if (stream.isSome() && stream->terminated) {
      UUID operationUuid;
      operationUuid.set_value(uuid.toBytes());

      Operation* operation = getOperation(operationUuid);
      if (operation != nullptr) {
        removeOperation(operation);
        completedOperations.push_back(uuid);
      }
    }
  }

  // Garbage collect the operation streams.
  foreach (const id::UUID& uuid, completedOperations) {
    const string path =
      node::paths::getNodeOperationPath(metaDir, info.id(), uuid);

    Try<Nothing> rmdir = os::rmdir(path);
    if (rmdir.isError()) {
      LOG(ERROR) << "Failed to remove operation status update stream "
                 << "directory '" << path << "': " << rmdir.error();
    }
  }

  foreachpair (const UUID& uuid, Operation* operation, operations) {
    const id::UUID operationUuid(
        CHECK_NOTERROR(id::UUID::fromBytes(uuid.value())));

    // The operation might be from an operator API call, thus the framework
    // ID here is optional.
    Option<FrameworkID> frameworkId =
      operation->has_framework_id()
        ? operation->framework_id()
        : Option<FrameworkID>::none();

    if (operation->latest_status().state() == OPERATION_PENDING) {
      // The node failed over before the checkpoint of the
      // `OPERATION_FINISHED` update completed.
      CHECK(
          !state->streams.contains(operationUuid) ||
          state->streams.at(operationUuid).isNone());

      Option<OperationID> operationId =
        operation->info().has_id()
          ? operation->info().id()
          : Option<OperationID>::none();

      UpdateOperationStatusMessage update =
        protobuf::createUpdateOperationStatusMessage(
            operation->uuid(),
            protobuf::createOperationStatus(
                OPERATION_FINISHED,
                operationId,
                None(),
                None(),
                id::UUID::random(),
                info.id(),
                Option<ResourceProviderID>::none()),
            None(),
            frameworkId,
            info.id());

      updateOperation(operation, update);

      CHECK(protobuf::isSpeculativeOperation(operation->info()));
      apply(operation);

      checkpointResourceState(
          totalResources.filter(medios::needCheckpointing), false);

      operationStatusUpdateManager.update(update);
    } else if (!state->streams.contains(operationUuid) ||
               state->streams.get(operationUuid)->isNone()) {
      // The node failed over after creating the `OPERATION_FINISHED` update,
      // but before the operation status update manager checkpointed it.
      operationStatusUpdateManager.update(
          protobuf::createUpdateOperationStatusMessage(
              operation->uuid(),
              operation->latest_status(),
              None(),
              frameworkId,
              info.id()));
    }
  }

  return Nothing();
}


Future<Nothing> Node::_recover()
{
  LOG(INFO) << "Recovering executors";

  // Alow HTTP based executors to subscribe after the
  // containerizer recovery is complete.
  recoveryInfo.reconnect = true;

  foreachvalue (Framework* framework, frameworks) {
    foreachvalue (Executor* executor, framework->executors) {
      // Set up callback for executor termination.
      containerizer->wait(executor->containerId)
        .onAny(defer(self(),
                     &Self::executorTerminated,
                     framework->id(),
                     executor->id,
                     lambda::_1));

      if (flags.recover == "reconnect") {
        // We send a reconnect message for PID based executors
        // as we can initiate communication with them. Recovered
        // HTTP executors, on the other hand, are responsible for
        // subscribing back with the node using a retry interval.
        // Note that recovered http executors are marked with
        // http.isNone and pid.isNone (see comments in the header).
        if (executor->pid.isSome() && executor->pid.get()) {
          LOG(INFO)
            << "Sending reconnect request to executor " << *executor;

          ReconnectExecutorMessage message;
          message.mutable_node_id()->MergeFrom(info.id());
          send(executor->pid.get(), message);

          // PID-based executors using Medios libraries >= 1.1.2 always
          // re-link with the node upon receiving the reconnect message.
          // This avoids the executor replying on a half-open TCP
          // connection to the old node (possible if netfilter is
          // dropping packets, see: MESOS-7057). However, PID-based
          // executors using Medios libraries < 1.1.2 do not re-link
          // and are therefore prone to replying on a half-open connection
          // after the node restarts. If we only send a single reconnect
          // message, these "old" executors will reply on their half-open
          // connection and receive a RST; without any retries, they will
          // fail to reconnect and be killed by the node once the executor
          // re-registration timeout elapses. To ensure these "old"
          // executors can reconnect in the presence of netfilter dropping
          // packets, we introduced optional retries of the reconnect
          // message. This results in "old" executors correctly establishing
          // a link when processing the second reconnect message.
          if (flags.executor_reregistration_retry_interval.isSome()) {
            const Duration& retryInterval =
              flags.executor_reregistration_retry_interval.get();

            const FrameworkID& frameworkId = framework->id();
            const ExecutorID& executorId = executor->id;

            process::loop(
                self(),
                [retryInterval]() {
                  return after(retryInterval);
                },
                [this, frameworkId, executorId, message](Nothing)
                    -> ControlFlow<Nothing> {
                  if (state != RECOVERING) {
                    return Break();
                  }

                  Framework* framework = getFramework(frameworkId);
                  if (framework == nullptr) {
                    return Break();
                  }

                  Executor* executor = framework->getExecutor(executorId);
                  if (executor == nullptr) {
                    return Break();
                  }

                  if (executor->state != Executor::REGISTERING) {
                    return Break();
                  }

                  LOG(INFO) << "Re-sending reconnect request to executor "
                            << *executor;

                  send(executor->pid.get(), message);
                  return Continue();
                });
          }
        } else if (executor->pid.isNone()) {
          LOG(INFO) << "Waiting for executor " << *executor
                    << " to subscribe";
        } else {
          LOG(INFO) << "Unable to reconnect to executor " << *executor
                    << " because no pid or http checkpoint file was found";
        }
      } else {
        // For PID-based executors, we ask the executor to shut
        // down and give it time to terminate. For HTTP executors,
        // we do the same, however, the shutdown will only be sent
        // when the executor subscribes.
        if ((executor->pid.isSome() && executor->pid.get()) ||
            executor->pid.isNone()) {
          LOG(INFO) << "Sending shutdown to executor " << *executor;
          _shutdownExecutor(framework, executor);
        } else {
          LOG(INFO) << "Killing executor " << *executor
                    << " because no pid or http checkpoint file was found";

          containerizer->destroy(executor->containerId);
        }
      }
    }
  }

  if (!frameworks.empty() && flags.recover == "reconnect") {
    // Cleanup unregistered executors after a delay.
    delay(flags.executor_reregistration_timeout,
          self(),
          &Node::reregisterExecutorTimeout);

    // We set 'recovered' flag inside reregisterExecutorTimeout(),
    // so that when the node reregisters with controller it can
    // correctly inform the controller about the launched tasks.
    return recoveryInfo.recovered.future();
  }

  return Nothing();
}


void Node::__recover(const Future<Nothing>& future)
{
  if (!future.isReady()) {
    EXIT(EXIT_FAILURE)
      << "Failed to perform recovery: "
      << (future.isFailed() ? future.failure() : "future discarded") << "\n"
      << "If recovery failed due to a change in configuration and you want to\n"
      << "keep the current node id, you might want to change the\n"
      << "`--reconfiguration_policy` flag to a more permissive value.\n"
      << "\n"
      << "To restart this node with a new node id instead, do as follows:\n"
      << "rm -f " << paths::getLatestNodePath(metaDir) << "\n"
      << "This ensures that the node does not recover old live executors.\n"
      << "\n"
      << "If you use the Docker containerizer and think that the Docker\n"
      << "daemon state is broken, you can try to clear it. But be careful:\n"
      << "these commands will erase all containers and images from this host,\n"
      << "not just those started by Medios!\n"
      << "docker kill $(docker ps -q)\n"
      << "docker rm $(docker ps -a -q)\n"
      << "docker rmi $(docker images -q)\n"
      << "\n"
      << "Finally, restart the node.";
  }

  LOG(INFO) << "Finished recovery";

  CHECK_EQ(RECOVERING, state);

  // Checkpoint boot ID.
  Try<string> bootId = os::bootId();
  if (bootId.isError()) {
    LOG(ERROR) << "Could not retrieve boot id: " << bootId.error();
  } else {
    const string path = paths::getBootIdPath(metaDir);
    CHECK_SOME(state::checkpoint(path, bootId.get()));
  }

  // Schedule all old node directories for garbage collection.
  // TODO(vinod): Do this as part of recovery. This needs a fix
  // in the recovery code, to recover all nodes instead of only
  // the latest node.
  const string directory = path::join(flags.work_dir, "nodes");
  Try<list<string>> entries = os::ls(directory);
  if (entries.isSome()) {
    foreach (const string& entry, entries.get()) {
      string path = path::join(directory, entry);
      // Ignore non-directory entries.
      if (!os::stat::isdir(path)) {
        continue;
      }

      // We garbage collect a directory if either the node has not
      // recovered its id (hence going to get a new id when it
      // registers with the controller) or if it is an old work directory.
      NodeID nodeId;
      nodeId.set_value(entry);
      if (!info.has_id() || nodeId != info.id()) {
        LOG(INFO) << "Garbage collecting old node " << nodeId;

        // NOTE: We update the modification time of the node work/meta
        // directories even though these are old because these
        // directories might not have been scheduled for gc before.

        // GC the node work directory.
        os::utime(path); // Update the modification time.
        garbageCollect(path);

        // GC the node meta directory.
        path = paths::getNodePath(metaDir, nodeId);
        if (os::exists(path)) {
          os::utime(path); // Update the modification time.
          garbageCollect(path);
        }
      }
    }
  }

  if (flags.recover == "reconnect") {
    state = DISCONNECTED;

    // Start detecting controllers.
    detection = detector->detect()
      .onAny(defer(self(), &Node::detected, lambda::_1));

    if (info.has_id()) {
      initializeResourceProviderManager(flags, info.id());
    }

    // Forward oversubscribed resources.
    forwardOversubscribed();

    // Start acting on correction from QoS Controller.
    qosCorrections();
  } else {
    // Node started in cleanup mode.
    CHECK_EQ("cleanup", flags.recover);
    state = TERMINATING;

    if (frameworks.empty()) {
      terminate(self());
    }

    // If there are active executors/frameworks, the node will
    // shutdown when all the executors are terminated. Note that
    // the executors are guaranteed to terminate because they
    // are sent shutdown signal in '_recover()' which results in
    // 'Containerizer::destroy()' being called if the termination
    // doesn't happen within a timeout.
  }

  recoveryInfo.recovered.set(Nothing()); // Signal recovery.

  metrics.setRecoveryTime(process::Clock::now() - startTime);
}


void Node::recoverFramework(
    const FrameworkState& state,
    const hashset<ExecutorID>& executorsToRecheckpoint,
    const hashmap<ExecutorID, hashset<TaskID>>& tasksToRecheckpoint)
{
  LOG(INFO) << "Recovering framework " << state.id;

  if (state.executors.empty()) {
    // GC the framework work directory.
    garbageCollect(
        paths::getFrameworkPath(flags.work_dir, info.id(), state.id));

    // GC the framework meta directory.
    garbageCollect(
        paths::getFrameworkPath(metaDir, info.id(), state.id));

    return;
  }

  CHECK(!frameworks.contains(state.id));

  CHECK_SOME(state.info);
  FrameworkInfo frameworkInfo = state.info.get();

  // Medios 0.22 and earlier didn't write the FrameworkID into the FrameworkInfo.
  // In this case, we we update FrameworkInfo.framework_id from directory name,
  // and rewrite the new format when we are done.
  bool recheckpoint = false;
  if (!frameworkInfo.has_id()) {
    frameworkInfo.mutable_id()->CopyFrom(state.id);
    recheckpoint = true;
  }

  CHECK(frameworkInfo.has_id());
  CHECK(frameworkInfo.checkpoint());

  // In 0.24.0, HTTP schedulers are supported and these do not
  // have a 'pid'. In this case, the node will checkpoint UPID().
  CHECK_SOME(state.pid);

  Option<UPID> pid = state.pid.get();

  if (pid.get() == UPID()) {
    pid = None();
  }

  Framework* framework = new Framework(
      this, flags, frameworkInfo, pid);

  frameworks[framework->id()] = framework;

  if (recheckpoint) {
    framework->checkpointFramework();
  }

  // Now recover the executors for this framework.
  foreachvalue (const ExecutorState& executorState, state.executors) {
    framework->recoverExecutor(
        executorState,
        executorsToRecheckpoint.contains(executorState.id),
        tasksToRecheckpoint.contains(executorState.id)
            ? tasksToRecheckpoint.at(executorState.id)
            : hashset<TaskID>{});
  }

  // Remove the framework in case we didn't recover any executors.
  if (framework->executors.empty()) {
    removeFramework(framework);
  }
}


Future<Nothing> Node::garbageCollect(const string& path)
{
  Try<long> mtime = os::stat::mtime(path);
  if (mtime.isError()) {
    LOG(ERROR) << "Failed to find the mtime of '" << path
               << "': " << mtime.error();
    return Failure(mtime.error());
  }

  // It is unsafe for testing to use unix time directly, we must use
  // Time::create to convert into a Time object that reflects the
  // possibly advanced state of the libprocess Clock.
  Try<Time> time = Time::create(mtime.get());
  CHECK_SOME(time);

  // GC based on the modification time.
  Duration delay = flags.gc_delay - (Clock::now() - time.get());

  return gc->schedule(delay, path);
}


void Node::forwardOversubscribed()
{
  VLOG(2) << "Querying resource estimator for oversubscribable resources";

  resourceEstimator->oversubscribable()
    .onAny(defer(self(), &Self::_forwardOversubscribed, lambda::_1));
}


void Node::_forwardOversubscribed(const Future<Resources>& oversubscribable)
{
  if (!oversubscribable.isReady()) {
    LOG(ERROR) << "Failed to get oversubscribable resources: "
               << (oversubscribable.isFailed()
                   ? oversubscribable.failure() : "future discarded");
  } else {
    VLOG(2) << "Received oversubscribable resources "
            << oversubscribable.get() << " from the resource estimator";

    // Oversubscribable resources must be tagged as revocable.
    //
    // TODO(bmahler): Consider tagging input as revocable
    // rather than rejecting and crashing here.
    CHECK_EQ(oversubscribable.get(), oversubscribable->revocable());

    auto unallocated = [](const Resources& resources) {
      Resources result = resources;
      result.unallocate();
      return result;
    };

    // Calculate the latest allocation of oversubscribed resources.
    // Note that this allocation value might be different from the
    // controller's view because new task/executor might be in flight from
    // the controller or pending on the node etc. This is ok because the
    // allocator only considers the node's view of allocation when
    // calculating the available oversubscribed resources to offer.
    Resources oversubscribed;
    foreachvalue (Framework* framework, frameworks) {
      oversubscribed += unallocated(
          framework->allocatedResources().revocable());
    }

    // Add oversubscribable resources to the total.
    oversubscribed += oversubscribable.get();

    // Only forward the estimate if it's different from the previous
    // estimate. We also send this whenever we get (re-)registered
    // (i.e. whenever we transition into the RUNNING state).
    if (state == RUNNING && oversubscribedResources != oversubscribed) {
      LOG(INFO) << "Forwarding total oversubscribed resources "
                << oversubscribed;

      // We do not update the node's resource version since
      // oversubscribed resources cannot be used for any operations
      // but launches. Since oversubscription is run at regular
      // intervals updating the version could cause a lot of
      // operation churn.
      //
      // TODO(bbannier): Revisit this if we modify the operations
      // possible on oversubscribed resources.

      UpdateNodeMessage message;
      message.mutable_node_id()->CopyFrom(info.id());
      message.set_update_oversubscribed_resources(true);
      message.mutable_oversubscribed_resources()->CopyFrom(oversubscribed);

      CHECK_SOME(controller);
      send(controller.get(), message);
    }

    // Update the estimate.
    oversubscribedResources = oversubscribed;
  }

  delay(flags.oversubscribed_resources_interval,
        self(),
        &Self::forwardOversubscribed);
}


UpdateNodeMessage Node::generateResourceProviderUpdate() const
{
  // Node information (total resources, operations, resource
  // versions) is not passed as part of some `ResourceProvider`, but
  // globally in `UpdateStateMessage`.
  //
  // TODO(bbannier): Pass node information as a resource provider.
  UpdateNodeMessage message;
  message.mutable_node_id()->CopyFrom(info.id());
  message.set_update_oversubscribed_resources(false);
  message.mutable_resource_version_uuid()->CopyFrom(resourceVersion);
  message.mutable_operations();

  foreachvalue (const Operation* operation, operations) {
    Result<ResourceProviderID> resourceProviderId =
      getResourceProviderId(operation->info());

    if (resourceProviderId.isNone()) {
      message.mutable_operations()
        ->add_operations()->CopyFrom(*operation);
    }
  }

  // Always add a `resource_providers` field so we can distinguish the
  // empty and unset case.
  UpdateNodeMessage::ResourceProviders* providers =
    message.mutable_resource_providers();

  foreachvalue (ResourceProvider* resourceProvider, resourceProviders) {
    // If the resource provider has not updated its state we do not
    // need to and cannot include its information in an
    // `UpdateNodeMessage` since it requires a resource version.
    if (resourceProvider->resourceVersion.isNone()) {
      continue;
    }

    UpdateNodeMessage::ResourceProvider* provider = providers->add_providers();

    provider->mutable_info()->CopyFrom(
        resourceProvider->info);
    provider->mutable_total_resources()->CopyFrom(
        resourceProvider->totalResources);
    provider->mutable_resource_version_uuid()->CopyFrom(
        resourceProvider->resourceVersion.get());

    provider->mutable_operations();

    foreachvalue (const Operation* operation,
                  resourceProvider->operations) {
      provider->mutable_operations()
        ->add_operations()->CopyFrom(*operation);
    }
  }

  return message;
}


UpdateNodeMessage Node::generateUpdateNodeMessage() const
{
  UpdateNodeMessage message = generateResourceProviderUpdate();

  if (oversubscribedResources.isSome()) {
    message.set_update_oversubscribed_resources(true);
    message.mutable_oversubscribed_resources()->CopyFrom(
        oversubscribedResources.get());
  }

  return message;
}


void Node::handleResourceProviderMessage(
    const Future<ResourceProviderMessage>& message)
{
  // Ignore terminal messages which are not ready. These
  // can arise e.g., if the `Future` was discarded.
  if (!message.isReady()) {
    LOG(ERROR) << "Last resource provider message became terminal before "
                  "becoming ready: "
               << (message.isFailed() ? message.failure() : "future discarded");

    // Wait for the next message.
    CHECK_NOTNULL(resourceProviderManager.get())->messages().get()
      .onAny(defer(self(), &Self::handleResourceProviderMessage, lambda::_1));

    return;
  }

  LOG(INFO) << "Handling resource provider message '" << message.get() << "'";

  switch(message->type) {
    case ResourceProviderMessage::Type::SUBSCRIBE: {
      CHECK_SOME(message->subscribe);

      const ResourceProviderMessage::Subscribe& subscribe =
        message->subscribe.get();

      CHECK(subscribe.info.has_id());

      ResourceProvider* resourceProvider =
        getResourceProvider(subscribe.info.id());

      if (resourceProvider == nullptr) {
        resourceProvider = new ResourceProvider(subscribe.info, {}, None());

        addResourceProvider(resourceProvider);
      } else {
        // Always update the resource provider info.
        resourceProvider->info = subscribe.info;
      }
      break;
    }
    case ResourceProviderMessage::Type::UPDATE_STATE: {
      CHECK_SOME(message->updateState);

      const ResourceProviderMessage::UpdateState& updateState =
        message->updateState.get();

      ResourceProvider* resourceProvider =
        getResourceProvider(updateState.resourceProviderId);

      CHECK(resourceProvider);

      if (resourceProvider->totalResources != updateState.totalResources) {
        // Update the 'total' in the Node.
        CHECK(totalResources.contains(resourceProvider->totalResources));
        totalResources -= resourceProvider->totalResources;
        totalResources += updateState.totalResources;

        // Update the 'total' in the resource provider.
        resourceProvider->totalResources = updateState.totalResources;
      }

      // Update operation state.
      //
      // We only update operations which are not contained in both
      // the known and just received sets. All other operations will
      // be updated via relayed operation status updates.
      const hashset<UUID> knownUuids = resourceProvider->operations.keys();
      const hashset<UUID> receivedUuids = updateState.operations.keys();

      // Handle operations known to the node but not reported by
      // the resource provider. These could be operations where the
      // node has started tracking an operation, but the resource
      // provider failed over before it could bookkeep the
      // operation.
      //
      // NOTE: We do not mutate operations statuses here; this would
      // be the responsibility of an operation status update handler.
      hashset<UUID> disappearedUuids = knownUuids - receivedUuids;
      foreach (const UUID& uuid, disappearedUuids) {
        // TODO(bbannier): Instead of simply dropping an operation
        // with `removeOperation` here we should instead send a
        // `Reconcile` message with a failed state to the resource
        // provider so its status update manager can reliably
        // deliver the operation status to the framework.
        removeOperation(resourceProvider->operations.at(uuid));
      }

      // Handle operations known to the resource provider but not
      // the node. This can happen if the node failed over and the
      // resource provider reregistered.
      hashset<UUID> reappearedUuids = receivedUuids - knownUuids;
      foreach (const UUID& uuid, reappearedUuids) {
        // Start tracking this operation.
        //
        // NOTE: We do not need to update total resources here as its
        // state was sync explicitly with the received total above.
        addOperation(new Operation(updateState.operations.at(uuid)));
      }

      // Handle operations known to both the node and the resource provider.
      //
      // If an operation became terminal, its result is already reflected in
      // the total resources reported by the resource provider, and thus it
      // should not be applied again in an operation status update handler
      // when its terminal status update arrives. So we set the terminal
      // `latest_status` here to prevent resource conversions elsewhere.
      //
      // NOTE: We only update the `latest_status` of a known operation if it
      // is not terminal yet here; its `statuses` would be updated by an
      // operation status update handler.
      hashset<UUID> matchedUuids = knownUuids - disappearedUuids;
      foreach (const UUID& uuid, matchedUuids) {
        const Operation& operation = updateState.operations.at(uuid);
        if (operation.has_latest_status() &&
            protobuf::isTerminalState(operation.latest_status().state())) {
          updateOperationLatestStatus(
              getOperation(uuid),
              operation.latest_status());
        }
      }

      // Update resource version of this resource provider.
      resourceProvider->resourceVersion = updateState.resourceVersion;

      // Send the updated resources to the controller if the node is running. Note
      // that since we have already updated our copy of the latest resource
      // provider resources, it is safe to consume this message and wait for the
      // next one; even if we do not send the update to the controller right now, an
      // update will be send once the node reregisters.
      switch (state) {
        case RECOVERING:
        case DISCONNECTED:
        case TERMINATING: {
          break;
        }
        case RUNNING: {
          LOG(INFO) << "Forwarding new total resources " << totalResources;

          // Inform the controller about the update from the resource provider.
          send(controller.get(), generateResourceProviderUpdate());

          break;
        }
      }
      break;
    }
    case ResourceProviderMessage::Type::UPDATE_OPERATION_STATUS: {
      CHECK_SOME(message->updateOperationStatus);

      // The status update from the resource provider didn't provide
      // the node ID (because the resource provider doesn't know it),
      // hence we inject it here.
      UpdateOperationStatusMessage update =
        message->updateOperationStatus->update;

      update.mutable_node_id()->CopyFrom(info.id());
      update.mutable_status()->mutable_node_id()->CopyFrom(info.id());
      if (update.has_latest_status()) {
        update.mutable_latest_status()->mutable_node_id()->CopyFrom(info.id());
      }

      const UUID& operationUUID = update.operation_uuid();

      Operation* operation = getOperation(operationUUID);

      if (operation != nullptr) {
        // It is possible for the resource provider to forget or incorrectly
        // copy the OperationID in its status update. We make sure the ID
        // is filled in with the correct value before proceeding.
        if (operation->info().has_id()) {
          update.mutable_status()->mutable_operation_id()
            ->CopyFrom(operation->info().id());

          if (update.has_latest_status()) {
            update.mutable_latest_status()->mutable_operation_id()
              ->CopyFrom(operation->info().id());
          }
        }

        // The node might not know about the operation in the
        // following cases:
        //
        // Case 1:
        // (1) The node sends to a resource provder an ACK for a
        //     terminal operation status update and removes the
        //     operation.
        // (2) The resource provider doesn't get the ACK.
        // (3) The resource provider's status update manager resends
        //     the operation status update.
        //
        // Case 2:
        // (1) The controller knows an operation that the node doesn't
        //     know, because an ApplyOperationMessage was dropped.
        // (2) The controller sends a ReconcileOperationsMessage
        //     message to the node, who forwards it to a resource
        //     provider.
        // (3) The resource provider doesn't know the operation, so it
        //     sends an operation status update with the state
        //     OPERATION_DROPPED.
        //
        // In both cases the node should not update it's internal
        // state, but it should still forward the operation status
        // update.
        updateOperation(operation, update);
      }

      switch (state) {
        case RECOVERING:
        case DISCONNECTED:
        case TERMINATING: {
          LOG(WARNING)
            << "Dropping status update of operation"
            << (update.status().has_operation_id()
                 ? " '" + stringify(update.status().operation_id()) + "'"
                 : " with no ID")
            << " (operation_uuid: " << operationUUID << ")"
            << (update.has_framework_id()
                 ? " for framework " + stringify(update.framework_id())
                 : " for an operator API call")
            << " because node is in " << state << " state";
          break;
        }
        case RUNNING: {
          LOG(INFO)
            << "Forwarding status update of"
            << (operation == nullptr ? " unknown" : "") << " operation"
            << (update.status().has_operation_id()
                 ? " '" + stringify(update.status().operation_id()) + "'"
                 : " with no ID")
            << " (operation_uuid: " << operationUUID << ")"
            << (update.has_framework_id()
                 ? " for framework " + stringify(update.framework_id())
                 : " for an operator API call");

          send(controller.get(), update);
          break;
        }
      }
      break;
    }
    case ResourceProviderMessage::Type::DISCONNECT: {
      CHECK_SOME(message->disconnect);

      const ResourceProviderID& resourceProviderId =
        message->disconnect->resourceProviderId;

      ResourceProvider* resourceProvider =
        getResourceProvider(resourceProviderId);

      if (resourceProvider == nullptr) {
        LOG(ERROR) << "Failed to find the disconnected resource provider "
                   << resourceProviderId << ", ignoring the message";
        break;
      }

      // Remove the resource provider's resources from the node's
      // total resources and remove it from our internal tracking.
      CHECK(totalResources.contains(resourceProvider->totalResources));
      totalResources -= resourceProvider->totalResources;

      resourceProviders.erase(resourceProviderId);

      // Send the updated resources to the controller if the node is running. Note
      // that since we have already updated our copy of the latest resource
      // provider resources, it is safe to consume this message and wait for the
      // next one; even if we do not send the update to the controller right now, an
      // update will be send once the node reregisters.
      switch (state) {
        case RECOVERING:
        case DISCONNECTED:
        case TERMINATING: {
          break;
        }
        case RUNNING: {
          LOG(INFO) << "Forwarding new total resources " << totalResources;

          // Inform the controller about the update from the resource provider.
          send(controller.get(), generateResourceProviderUpdate());

          break;
        }
      }
      break;
    }
    case ResourceProviderMessage::Type::REMOVE: {
      CHECK_SOME(message->remove);

      const ResourceProviderID& resourceProviderId =
        message->remove->resourceProviderId;

      if (!resourceProviders.contains(resourceProviderId)) {
        break;
      }

      const ResourceProvider* resourceProvider =
        resourceProviders.at(resourceProviderId);

      CHECK_NOTNULL(resourceProvider);

      // Transition all non-terminal operations on the resource provider to a
      // terminal state.
      //
      // NOTE: We operate on a copy of the operations container since we trigger
      // removal of current operation in below loop. This invalidates the loop
      // iterator so it cannot be safely incremented after the loop body.
      const hashmap<UUID, Operation*> operations = resourceProvider->operations;
      foreachpair (const UUID& uuid, Operation * operation, operations) {
        CHECK_NOTNULL(operation);

        if (protobuf::isTerminalState(operation->latest_status().state())) {
          continue;
        }

        // The operation might be from an operator API call, thus the framework
        // ID here is optional.
        Option<FrameworkID> frameworkId =
          operation->has_framework_id()
            ? operation->framework_id()
            : Option<FrameworkID>::none();

        Option<OperationID> operationId =
          operation->info().has_id()
            ? operation->info().id()
            : Option<OperationID>::none();

        UpdateOperationStatusMessage update =
          protobuf::createUpdateOperationStatusMessage(
              uuid,
              protobuf::createOperationStatus(
                  OPERATION_GONE_BY_OPERATOR,
                  operationId,
                  "The resource provider was removed before a terminal "
                  "operation status update was received",
                  None(),
                  None(),
                  info.id()),
              None(),
              frameworkId);

        updateOperation(operation, update);

        removeOperation(operation);

        // Forward the operation status update to the controller.
        //
        // The status update from the resource provider does not
        // provide the node ID (because the resource provider doesn't
        // know it), so we inject it here.
        UpdateOperationStatusMessage _update;
        _update.CopyFrom(update);
        _update.mutable_node_id()->CopyFrom(info.id());
        send(controller.get(), _update);
      };

      // TODO(bbannier): Consider transitioning all tasks using resources from
      // this resource provider to e.g., `TASK_GONE_BY_OPERATOR` and terminating
      // them.

      // Remove the resources of the resource provider from the node's total.
      // This needs to be done after triggering the operation status update so
      // that controller does not receive a operations status update for an unknown
      // operation (gone from `UpdateNodeMessage`).
      totalResources -= resourceProvider->totalResources;

      resourceProviders.erase(resourceProviderId);

      switch (state) {
        case RECOVERING:
        case DISCONNECTED:
        case TERMINATING: {
          break;
        }
        case RUNNING: {
          LOG(INFO) << "Forwarding new total resources " << totalResources;

          // Inform the controller about the updated resources.
          send(controller.get(), generateResourceProviderUpdate());

          break;
        }
      }

      LOG(INFO) << "Removed resource provider '" << resourceProviderId << "'";
      break;
    }
  }

  // Wait for the next message.
  CHECK_NOTNULL(resourceProviderManager.get())->messages().get()
    .onAny(defer(self(), &Self::handleResourceProviderMessage, lambda::_1));
}


void Node::addOperation(Operation* operation)
{
  operations.put(operation->uuid(), operation);

  if (operation->info().has_id() && operation->has_framework_id()) {
    operationIds.put(
        std::make_pair(operation->framework_id(), operation->info().id()),
        operation->uuid());
  }

  Result<ResourceProviderID> resourceProviderId =
    getResourceProviderId(operation->info());

  CHECK(!resourceProviderId.isError())
    << "Failed to get resource provider ID: "
    << resourceProviderId.error();

  if (resourceProviderId.isSome()) {
    ResourceProvider* resourceProvider =
      getResourceProvider(resourceProviderId.get());

    CHECK_NOTNULL(resourceProvider);

    resourceProvider->addOperation(operation);
  }
}


void Node::updateOperation(
    Operation* operation,
    const UpdateOperationStatusMessage& update)
{
  CHECK_NOTNULL(operation);

  const OperationStatus& status = update.status();

  Option<OperationStatus> latestStatus;
  if (update.has_latest_status()) {
    latestStatus = update.latest_status();
  }

  // Whether the operation has just become terminated.
  Option<bool> terminated;

  if (latestStatus.isSome()) {
    terminated =
      !protobuf::isTerminalState(operation->latest_status().state()) &&
      protobuf::isTerminalState(latestStatus->state());

    updateOperationLatestStatus(operation, latestStatus.get());
  } else {
    terminated =
      !protobuf::isTerminalState(operation->latest_status().state()) &&
      protobuf::isTerminalState(status.state());

    updateOperationLatestStatus(operation, status);
  }

  // Adding the update's status to the stored operation below is the one place
  // in this function where we mutate the operation state irrespective of the
  // value of `terminated`. We check to see if this status update is a retry;
  // if so, we do nothing.
  bool isRetry = false;
  if (status.has_uuid()) {
    foreach (const OperationStatus& storedStatus, operation->statuses()) {
      if (storedStatus.has_uuid() && storedStatus.uuid() == status.uuid()) {
        isRetry = true;
        break;
      }
    }
  }

  if (!isRetry) {
    operation->add_statuses()->CopyFrom(status);
  }

  LOG(INFO) << "Updating the state of operation"
            << (operation->info().has_id()
                 ? " '" + stringify(operation->info().id()) + "'"
                 : " with no ID")
            << " (uuid: " << operation->uuid() << ")"
            << (operation->has_framework_id()
                 ? " for framework " + stringify(operation->framework_id())
                 : " for an operation API call")
            << " (latest state: " << operation->latest_status().state()
            << ", status update state: " << status.state() << ")";

  CHECK_SOME(terminated);

  if (!terminated.get()) {
    return;
  }

  if (protobuf::isSpeculativeOperation(operation->info())) {
    return;
  }

  switch (operation->latest_status().state()) {
    // Terminal state, and the conversion is successful.
    case OPERATION_FINISHED: {
      apply(operation);
      break;
    }

    // Terminal state, and the conversion has failed.
    case OPERATION_DROPPED:
    case OPERATION_ERROR:
    case OPERATION_FAILED:
    case OPERATION_GONE_BY_OPERATOR: {
      break;
    }

    // Non-terminal or not sent by resource providers. This shouldn't happen.
    case OPERATION_UNSUPPORTED:
    case OPERATION_PENDING:
    case OPERATION_UNREACHABLE:
    case OPERATION_RECOVERING:
    case OPERATION_UNKNOWN: {
      LOG(FATAL)
        << "Unexpected operation state " << operation->latest_status().state();
    }
  }
}


void Node::updateOperationLatestStatus(
    Operation* operation,
    const OperationStatus& status)
{
  CHECK_NOTNULL(operation);

  if (!protobuf::isTerminalState(operation->latest_status().state())) {
    operation->mutable_latest_status()->CopyFrom(status);
  }
}


void Node::removeOperation(Operation* operation)
{
  const UUID& uuid = operation->uuid();

  Result<ResourceProviderID> resourceProviderId =
    getResourceProviderId(operation->info());

  CHECK(!resourceProviderId.isError())
    << "Failed to get resource provider ID: "
    << resourceProviderId.error();

  if (resourceProviderId.isSome()) {
    ResourceProvider* resourceProvider =
      getResourceProvider(resourceProviderId.get());

    CHECK_NOTNULL(resourceProvider);

    resourceProvider->removeOperation(operation);
  }

  CHECK(operations.contains(uuid))
    << "Unknown operation (uuid: " << uuid << ")";

  if (operation->info().has_id() && operation->has_framework_id()) {
    operationIds.erase(
        std::make_pair(operation->framework_id(), operation->info().id()));
  }

  operations.erase(uuid);
  delete operation;

  checkpointResourceState(
      totalResources.filter(medios::needCheckpointing), false);

  updateDrainStatus();
}


Operation* Node::getOperation(const UUID& uuid) const
{
  if (operations.contains(uuid)) {
    return operations.at(uuid);
  }
  return nullptr;
}


void Node::addResourceProvider(ResourceProvider* resourceProvider)
{
  CHECK(resourceProvider->info.has_id());
  CHECK(!resourceProviders.contains(resourceProvider->info.id()));

  resourceProviders.put(
      resourceProvider->info.id(),
      resourceProvider);
}


ResourceProvider* Node::getResourceProvider(const ResourceProviderID& id) const
{
  if (resourceProviders.contains(id)) {
    return resourceProviders.at(id);
  }
  return nullptr;
}


Future<Nothing> Node::markResourceProviderGone(
    const ResourceProviderID& resourceProviderId) const
{
  auto message = [&resourceProviderId](const string& reason) {
    return
      "Could not mark resource provider '" + stringify(resourceProviderId) +
      "' as gone: " + reason;
  };

  if (!resourceProviderManager.get()) {
    return Failure(message("Node has not registered yet"));
  }

  if (resourceProviders.contains(resourceProviderId) &&
      !resourceProviders.at(resourceProviderId)->totalResources.empty()) {
    return Failure(message("Resource provider has resources"));
  }

  return resourceProviderManager->removeResourceProvider(resourceProviderId);
}

void Node::apply(Operation* operation)
{
  vector<ResourceConversion> conversions;

  // NOTE: 'totalResources' don't have allocations set, we need to
  // remove them from the conversions.

  if (protobuf::isSpeculativeOperation(operation->info())) {
    Offer::Operation strippedOperation = operation->info();
    protobuf::stripAllocationInfo(&strippedOperation);

    Try<vector<ResourceConversion>> _conversions =
      getResourceConversions(strippedOperation);

    CHECK_SOME(_conversions);

    conversions = _conversions.get();
  } else {
    // For non-speculative operations, we only apply the conversion
    // once it becomes terminal. Before that, we don't know the
    // converted resources of the conversion.
    CHECK_EQ(OPERATION_FINISHED, operation->latest_status().state());

    Try<Resources> consumed = protobuf::getConsumedResources(operation->info());
    CHECK_SOME(consumed);

    Resources converted = operation->latest_status().converted_resources();

    consumed->unallocate();
    converted.unallocate();

    conversions.emplace_back(consumed.get(), converted);
  }

  // Now, actually apply the operation.
  Try<Resources> resources = totalResources.apply(conversions);
  CHECK_SOME(resources);

  totalResources = resources.get();

  Result<ResourceProviderID> resourceProviderId =
    getResourceProviderId(operation->info());

  CHECK(!resourceProviderId.isError())
    << "Failed to get resource provider ID: "
    << resourceProviderId.error();

  // Besides updating the node's `totalResources`, we also need to
  // update the resource provider's `totalResources`.
  if (resourceProviderId.isSome()) {
    ResourceProvider* resourceProvider =
      getResourceProvider(resourceProviderId.get());

    CHECK_NOTNULL(resourceProvider);

    Try<Resources> resources =
      resourceProvider->totalResources.apply(conversions);

    CHECK_SOME(resources);

    resourceProvider->totalResources = resources.get();
  }
}


Future<Nothing> Node::publishResources(
    const ContainerID& containerId, const Resources& resources)
{
  hashset<ResourceProviderID> resourceProviderIds;
  foreach (const Resource& resource, resources) {
    if (resource.has_provider_id()) {
      resourceProviderIds.insert(resource.provider_id());
    }
  }

  vector<Future<Nothing>> futures;
  foreach (const ResourceProviderID& resourceProviderId, resourceProviderIds) {
    auto hasResourceProviderId = [&](const Resource& resource) {
      return resource.has_provider_id() &&
             resource.provider_id() == resourceProviderId;
    };

    // NOTE: For resources providers that serve quantity-based resources without
    // identifier (such as cpus and mem), we cannot achieve idempotency with
    // diff-based resource publishing, so we have to implement the "ensure-all"
    // semantics, and always calculate the total resources to publish.
    Option<Resources> containerResources;
    Resources complementaryResources;
    foreachvalue (const Framework* framework, frameworks) {
      foreachvalue (const Executor* executor, framework->executors) {
        if (executor->containerId == containerId) {
          containerResources = resources.filter(hasResourceProviderId);
        } else {
          complementaryResources +=
            executor->allocatedResources().filter(hasResourceProviderId);
        }
      }
    }

    if (containerResources.isNone()) {
      // NOTE: This actually should not happen, as the callers have already
      // ensured the existence of the executor before calling this function
      // synchronously. However we still treat this as a nonfatal error since
      // this might change in the future.
      LOG(WARNING) << "Ignoring publishing resources for container "
                   << containerId << ": Executor cannot be found";

      return Nothing();
    }

    // Since we already have resources from any resource provider in the
    // resource pool, the resource provider manager must have been created.
    futures.push_back(
        CHECK_NOTNULL(resourceProviderManager.get())
          ->publishResources(containerResources.get() + complementaryResources)
          .repair([=](const Future<Nothing>& future) -> Future<Nothing> {
            // TODO(chhsiao): Consider surfacing the set of published resources
            // and only fail if `published - complementaryResources` does not
            // contain `containerResources`.
            return Failure(
                "Failed to publish resources '" +
                stringify(containerResources.get()) + "' for container " +
                stringify(containerId) + ": " + future.failure());
          }));
  }

  // NOTE: Resource cleanups (e.g., unpublishing) are not performed at task
  // completion, but rather done __lazily__ when necessary. This is not just an
  // optimization but required because resource allocations are tied to task
  // lifecycles. As a result, no cleanup is needed here if any future fails.
  return collect(futures).then([] { return Nothing(); });
}


void Node::qosCorrections()
{
  qosController->corrections()
    .onAny(defer(self(), &Self::_qosCorrections, lambda::_1));
}


void Node::_qosCorrections(const Future<list<QoSCorrection>>& future)
{
  // Make sure correction handler is scheduled again.
  delay(flags.qos_correction_interval_min, self(), &Self::qosCorrections);

  // Verify node state.
  CHECK(state == RECOVERING || state == DISCONNECTED ||
        state == RUNNING || state == TERMINATING)
    << state;

  if (state == RECOVERING || state == TERMINATING) {
    LOG(WARNING) << "Cannot perform QoS corrections because the node is "
                 << state;
    return;
  }

  if (!future.isReady()) {
    LOG(WARNING) << "Failed to get corrections from QoS Controller: "
                  << (future.isFailed() ? future.failure() : "discarded");
    return;
  }

  const list<QoSCorrection>& corrections = future.get();

  VLOG(1) << "Received " << corrections.size() << " QoS corrections";

  foreach (const QoSCorrection& correction, corrections) {
    // TODO(nnielsen): Print correction, once the operator overload
    // for QoSCorrection has been implemented.
    if (correction.type() == QoSCorrection::KILL) {
      const QoSCorrection::Kill& kill = correction.kill();

      if (!kill.has_framework_id()) {
        LOG(WARNING) << "Ignoring QoS correction KILL: "
                     << "framework id not specified.";
        continue;
      }

      const FrameworkID& frameworkId = kill.framework_id();

      if (!kill.has_executor_id()) {
        // TODO(nnielsen): For now, only executor killing is supported. Check
        // can be removed when task killing is supported as well.
        LOG(WARNING) << "Ignoring QoS correction KILL on framework "
                     << frameworkId << ": executor id not specified";
        continue;
      }

      const ExecutorID& executorId = kill.executor_id();

      Framework* framework = getFramework(frameworkId);
      if (framework == nullptr) {
        LOG(WARNING) << "Ignoring QoS correction KILL on framework "
                     << frameworkId << ": framework cannot be found";
        continue;
      }

      // Verify framework state.
      CHECK(framework->state == Framework::RUNNING ||
            framework->state == Framework::TERMINATING)
        << framework->state;

      if (framework->state == Framework::TERMINATING) {
        LOG(WARNING) << "Ignoring QoS correction KILL on framework "
                     << frameworkId << ": framework is terminating.";
        continue;
      }

      Executor* executor = framework->getExecutor(executorId);
      if (executor == nullptr) {
        LOG(WARNING) << "Ignoring QoS correction KILL on executor '"
                     << executorId << "' of framework " << frameworkId
                     << ": executor cannot be found";
        continue;
      }

      const ContainerID containerId =
          kill.has_container_id() ? kill.container_id() : executor->containerId;
      if (containerId != executor->containerId) {
        LOG(WARNING) << "Ignoring QoS correction KILL on container '"
                     << containerId << "' for executor " << *executor
                     << ": container cannot be found";
        continue;
      }

      switch (executor->state) {
        case Executor::REGISTERING:
        case Executor::RUNNING: {
          LOG(INFO) << "Killing container '" << containerId
                    << "' for executor " << *executor
                    << " as QoS correction";

          containerizer->destroy(containerId);

          // TODO(nnielsen): We should ensure that we are addressing
          // the _container_ which the QoS controller intended to
          // kill. Without this check, we may run into a scenario
          // where the executor has terminated and one with the same
          // id has started in the interim i.e. running in a different
          // container than the one the QoS controller targeted
          // (MESOS-2875).
          executor->state = Executor::TERMINATING;

          // Send TASK_GONE because the task was started but has now
          // been terminated. If the framework is not partition-aware,
          // we send TASK_LOST instead for backward compatibility.
          medios::TaskState taskState = TASK_GONE;
          if (!protobuf::frameworkHasCapability(
                  framework->info,
                  FrameworkInfo::Capability::PARTITION_AWARE)) {
            taskState = TASK_LOST;
          }

          ContainerTermination termination;
          termination.set_state(taskState);
          termination.set_reason(TaskStatus::REASON_CONTAINER_PREEMPTED);
          termination.set_message("Container preempted by QoS correction");

          executor->pendingTermination = termination;

          ++metrics.executors_preempted;
          break;
        }
        case Executor::TERMINATING:
        case Executor::TERMINATED:
          LOG(WARNING) << "Ignoring QoS correction KILL on executor "
                       << *executor << " because the executor is in "
                       << executor->state << " state";
          break;
        default:
          LOG(FATAL) << "Executor " << *executor << " is in unexpected state "
                     << executor->state;
          break;
      }
    } else {
      LOG(WARNING) << "QoS correction type " << correction.type()
                   << " is not supported";
    }
  }
}


Future<ResourceUsage> Node::usage()
{
  // NOTE: We use 'Owned' here trying to avoid the expensive copy.
  // C++11 lambda only supports capturing variables that have copy
  // constructors. Revisit once we remove the copy constructor for
  // Owned (or C++14 lambda generalized capture is supported).
  Owned<ResourceUsage> usage(new ResourceUsage());
  vector<Future<ResourceStatistics>> futures;

  foreachvalue (const Framework* framework, frameworks) {
    foreachvalue (const Executor* executor, framework->executors) {
      // No need to get statistics and status if we know that the
      // executor has already terminated.
      if (executor->state == Executor::TERMINATED) {
        continue;
      }

      ResourceUsage::Executor* entry = usage->add_executors();
      entry->mutable_executor_info()->CopyFrom(executor->info);
      entry->mutable_allocated()->CopyFrom(executor->allocatedResources());
      entry->mutable_container_id()->CopyFrom(executor->containerId);

      // We include non-terminal tasks in ResourceUsage.
      foreachvalue (const Task* task, executor->launchedTasks) {
        ResourceUsage::Executor::Task* t = entry->add_tasks();
        t->set_name(task->name());
        t->mutable_id()->CopyFrom(task->task_id());
        t->mutable_resources()->CopyFrom(task->resources());

        if (task->has_labels()) {
          t->mutable_labels()->CopyFrom(task->labels());
        }
      }

      futures.push_back(containerizer->usage(executor->containerId));
    }
  }

  usage->mutable_total()->CopyFrom(totalResources);

  return await(futures).then(
      [usage](const vector<Future<ResourceStatistics>>& futures) {
        // NOTE: We add ResourceUsage::Executor to 'usage' the same
        // order as we push future to 'futures'. So the variables
        // 'future' and 'executor' below should be in sync.
        CHECK_EQ(futures.size(), (size_t) usage->executors_size());

        int i = 0;
        foreach (const Future<ResourceStatistics>& future, futures) {
          ResourceUsage::Executor* executor = usage->mutable_executors(i++);

          if (future.isReady()) {
            executor->mutable_statistics()->CopyFrom(future.get());
          } else {
            LOG(WARNING) << "Failed to get resource statistics for executor '"
                         << executor->executor_info().executor_id() << "'"
                         << " of framework "
                         << executor->executor_info().framework_id() << ": "
                         << (future.isFailed() ? future.failure()
                                               : "discarded");
          }
        }

        return Future<ResourceUsage>(*usage);
      });
}


// As a principle, we do not need to re-authorize actions that have already
// been authorized by the controller. However, we re-authorize the RUN_TASK action
// on the node even though the controller has already authorized it because:
// a) in cases where hosts have heterogeneous user-account configurations,
//    it makes sense to set the ACL on the node instead of on the controller
// b) compared to other actions such as killing a task and shutting down a
//    framework, it's a greater security risk if malicious tasks are launched
//    as a superuser on the node.
Future<bool> Node::authorizeTask(
    const TaskInfo& task,
    const FrameworkInfo& frameworkInfo)
{
  if (authorizer.isNone()) {
    return true;
  }

  // Authorize the task.
  authorization::Request request;

  if (frameworkInfo.has_principal()) {
    request.mutable_subject()->set_value(frameworkInfo.principal());
  }

  request.set_action(authorization::RUN_TASK);

  authorization::Object* object = request.mutable_object();

  object->mutable_task_info()->CopyFrom(task);
  object->mutable_framework_info()->CopyFrom(frameworkInfo);

  LOG(INFO)
    << "Authorizing framework principal '"
    << (frameworkInfo.has_principal() ? frameworkInfo.principal() : "ANY")
    << "' to launch task " << task.task_id();

  return authorizer.get()->authorized(request);
}


Future<bool> Node::authorizeSandboxAccess(
    const Option<Principal>& principal,
    const FrameworkID& frameworkId,
    const ExecutorID& executorId)
{
  if (authorizer.isNone()) {
    return true;
  }

  return ObjectApprovers::create(
      authorizer,
      principal,
      {ACCESS_SANDBOX})
    .then(defer(
        self(),
        [=](const Owned<ObjectApprovers>& approvers) -> Future<bool> {
          // Construct authorization object.
          ObjectApprover::Object object;

          if (frameworks.contains(frameworkId)) {
            Framework* framework = frameworks.at(frameworkId);

            object.framework_info = &(framework->info);

            if (framework->executors.contains(executorId)) {
              object.executor_info =
                &(framework->executors.at(executorId)->info);
            }
          }

          return approvers->approved<ACCESS_SANDBOX>(object);
        }));
}


void Node::sendExecutorTerminatedStatusUpdate(
    const TaskID& taskId,
    const Future<Option<ContainerTermination>>& termination,
    const FrameworkID& frameworkId,
    const Executor* executor)
{
  CHECK_NOTNULL(executor);

  medios::TaskState state;
  TaskStatus::Reason reason;
  string message;

  const bool haveTermination = termination.isReady() && termination->isSome();

  // Determine the task state for the status update.
  if (haveTermination && termination->get().has_state()) {
    state = termination->get().state();
  } else if (executor->pendingTermination.isSome() &&
             executor->pendingTermination->has_state()) {
    state = executor->pendingTermination->state();
  } else {
    state = TASK_FAILED;
  }

  // Determine the task reason for the status update.
  if (haveTermination && termination->get().has_reason()) {
    reason = termination->get().reason();
  } else if (executor->pendingTermination.isSome() &&
             executor->pendingTermination->has_reason()) {
    reason = executor->pendingTermination->reason();
  } else {
    reason = TaskStatus::REASON_EXECUTOR_TERMINATED;
  }

  // Determine the message for the status update.
  vector<string> messages;

  if (executor->pendingTermination.isSome() &&
      executor->pendingTermination->has_message()) {
    messages.push_back(executor->pendingTermination->message());
  }

  if (!termination.isReady()) {
    messages.push_back(
        "Abnormal executor termination: " +
        (termination.isFailed() ? termination.failure() : "discarded future"));
  } else if (termination->isNone()) {
    messages.push_back("Abnormal executor termination: unknown container");
  } else if (termination->get().has_message()) {
    messages.push_back(termination->get().message());
  }

  if (messages.empty()) {
    message = "Executor terminated";
  } else {
    message = strings::join("; ", messages);
  }

  Option<Resources> limitedResources;

  if (haveTermination && !termination->get().limited_resources().empty()) {
    limitedResources = termination->get().limited_resources();
  }

  statusUpdate(
      protobuf::createStatusUpdate(
          frameworkId,
          info.id(),
          taskId,
          state,
          TaskStatus::SOURCE_NODE,
          id::UUID::random(),
          message,
          reason,
          executor->id,
          None(),
          None(),
          None(),
          None(),
          None(),
          limitedResources),
      UPID());
}


void Node::sendExitedExecutorMessage(
    const FrameworkID& frameworkId,
    const ExecutorID& executorId,
    const Option<int>& status)
{
  ExitedExecutorMessage message;
  message.mutable_node_id()->MergeFrom(info.id());
  message.mutable_framework_id()->MergeFrom(frameworkId);
  message.mutable_executor_id()->MergeFrom(executorId);
  message.set_status(status.getOrElse(-1));

  if (controller.isSome()) {
    send(controller.get(), message);
  }
}


// TODO(dhamon): Move these to their own metrics.hpp|cpp.
double Node::_tasks_staging()
{
  double count = 0.0;
  foreachvalue (Framework* framework, frameworks) {
    typedef hashmap<TaskID, TaskInfo> TaskMap;
    foreachvalue (const TaskMap& tasks, framework->pendingTasks) {
      count += tasks.size();
    }

    foreachvalue (Executor* executor, framework->executors) {
      count += executor->queuedTasks.size();

      foreachvalue (Task* task, executor->launchedTasks) {
        if (task->state() == TASK_STAGING) {
          count++;
        }
      }
    }
  }
  return count;
}


double Node::_tasks_starting()
{
  double count = 0.0;
  foreachvalue (Framework* framework, frameworks) {
    foreachvalue (Executor* executor, framework->executors) {
      foreachvalue (Task* task, executor->launchedTasks) {
        if (task->state() == TASK_STARTING) {
          count++;
        }
      }
    }
  }
  return count;
}


double Node::_tasks_running()
{
  double count = 0.0;
  foreachvalue (Framework* framework, frameworks) {
    foreachvalue (Executor* executor, framework->executors) {
      foreachvalue (Task* task, executor->launchedTasks) {
        if (task->state() == TASK_RUNNING) {
          count++;
        }
      }
    }
  }
  return count;
}


double Node::_tasks_killing()
{
  double count = 0.0;
  foreachvalue (Framework* framework, frameworks) {
    foreachvalue (Executor* executor, framework->executors) {
      foreachvalue (Task* task, executor->launchedTasks) {
        if (task->state() == TASK_KILLING) {
          count++;
        }
      }
    }
  }
  return count;
}


double Node::_executors_registering()
{
  double count = 0.0;
  foreachvalue (Framework* framework, frameworks) {
    foreachvalue (Executor* executor, framework->executors) {
      if (executor->state == Executor::REGISTERING) {
        count++;
      }
    }
  }
  return count;
}


double Node::_executors_running()
{
  double count = 0.0;
  foreachvalue (Framework* framework, frameworks) {
    foreachvalue (Executor* executor, framework->executors) {
      if (executor->state == Executor::RUNNING) {
        count++;
      }
    }
  }
  return count;
}


double Node::_executors_terminating()
{
  double count = 0.0;
  foreachvalue (Framework* framework, frameworks) {
    foreachvalue (Executor* executor, framework->executors) {
      if (executor->state == Executor::TERMINATING) {
        count++;
      }
    }
  }
  return count;
}


double Node::_executor_directory_max_allowed_age_secs()
{
  return executorDirectoryMaxAllowedAge.secs();
}


double Node::_resources_total(const string& name)
{
  double total = 0.0;

  foreach (const Resource& resource, info.resources()) {
    if (resource.name() == name && resource.type() == Value::SCALAR) {
      total += resource.scalar().value();
    }
  }

  return total;
}


double Node::_resources_used(const string& name)
{
  // We use `Resources` arithmetic to accummulate the resources since the
  // `+=` operator de-duplicates the same shared resources across executors.
  Resources used;

  foreachvalue (Framework* framework, frameworks) {
    used += framework->allocatedResources().nonRevocable();
  }

  return used.get<Value::Scalar>(name).getOrElse(Value::Scalar()).value();
}


double Node::_resources_percent(const string& name)
{
  double total = _resources_total(name);

  if (total == 0.0) {
    return 0.0;
  }

  return _resources_used(name) / total;
}


double Node::_resources_revocable_total(const string& name)
{
  double total = 0.0;

  if (oversubscribedResources.isSome()) {
    foreach (const Resource& resource, oversubscribedResources.get()) {
      if (resource.name() == name && resource.type() == Value::SCALAR) {
        total += resource.scalar().value();
      }
    }
  }

  return total;
}


double Node::_resources_revocable_used(const string& name)
{
  // We use `Resources` arithmetic to accummulate the resources since the
  // `+=` operator de-duplicates the same shared resources across executors.
  Resources used;

  foreachvalue (Framework* framework, frameworks) {
    used += framework->allocatedResources().revocable();
  }

  return used.get<Value::Scalar>(name).getOrElse(Value::Scalar()).value();
}


double Node::_resources_revocable_percent(const string& name)
{
  double total = _resources_revocable_total(name);

  if (total == 0.0) {
    return 0.0;
  }

  return _resources_revocable_used(name) / total;
}


void Node::initializeResourceProviderManager(
    const Flags& flags,
    const NodeID& nodeId)
{
  // To simplify reasoning about lifetimes we do not allow
  // reinitialization of the resource provider manager.
  if (resourceProviderManager.get() != nullptr) {
    return;
  }

  // The registrar uses LevelDB as underlying storage. Since LevelDB
  // back to in-memory storage there.
  //
  Owned<medios::state::Storage> storage(new medios::state::LevelDBStorage(
      paths::getResourceProviderRegistryPath(flags.work_dir, nodeId)));

  Try<Owned<resource_provider::Registrar>> resourceProviderRegistrar =
    resource_provider::Registrar::create(std::move(storage));

  CHECK_SOME(resourceProviderRegistrar)
    << "Could not construct resource provider registrar: "
    << resourceProviderRegistrar.error();

  resourceProviderManager.reset(
      new ResourceProviderManager(std::move(resourceProviderRegistrar.get())));

  if (capabilities.resourceProvider) {
    // Start listening for messages from the resource provider manager.
    resourceProviderManager->messages().get().onAny(
        defer(self(), &Self::handleResourceProviderMessage, lambda::_1));
  }
}


google::protobuf::Map<string, Value::Scalar> Node::computeExecutorLimits(
    const Resources& executorResources,
    const vector<TaskInfo>& taskInfos,
    const vector<Task*>& tasks) const
{
  Option<Value::Scalar> executorCpuLimit, executorMemLimit;
  Value::Scalar cpuRequest, memRequest;
  foreach (const TaskInfo& taskInfo, taskInfos) {
    // Count the task's CPU limit into the executor's CPU limit.
    if (taskInfo.limits().count("cpus")) {
      setLimit(executorCpuLimit, taskInfo.limits().at("cpus"));
    } else {
      Option<Value::Scalar> taskCpus =
        Resources(taskInfo.resources()).get<Value::Scalar>("cpus");

      if (taskCpus.isSome()) {
        cpuRequest += taskCpus.get();
      }
    }

    // Count the task's memory limit into the executor's memory limit.
    if (taskInfo.limits().count("mem")) {
      setLimit(executorMemLimit, taskInfo.limits().at("mem"));
    } else {
      Option<Value::Scalar> taskMem =
        Resources(taskInfo.resources()).get<Value::Scalar>("mem");

      if (taskMem.isSome()) {
        memRequest += taskMem.get();
      }
    }
  }

  foreach (const Task* task, tasks) {
    CHECK_NOTNULL(task);

    // Count the task's CPU limit into the executor's CPU limit.
    if (task->limits().count("cpus")) {
      setLimit(executorCpuLimit, task->limits().at("cpus"));
    } else {
      Option<Value::Scalar> taskCpus =
        Resources(task->resources()).get<Value::Scalar>("cpus");

      if (taskCpus.isSome()) {
        cpuRequest += taskCpus.get();
      }
    }

    // Count the task's memory limit into the executor's memory limit.
    if (task->limits().count("mem")) {
      setLimit(executorMemLimit, task->limits().at("mem"));
    } else {
      Option<Value::Scalar> taskMem =
        Resources(task->resources()).get<Value::Scalar>("mem");

      if (taskMem.isSome()) {
        memRequest += taskMem.get();
      }
    }
  }

  if (executorCpuLimit.isSome()) {
    // Count the executor's CPU request into its CPU limit as well, this is to
    // ensure the executor's CPU limit is always greater than its CPU request.
    Option<Value::Scalar> executorCpus =
      executorResources.get<Value::Scalar>("cpus");

    if (executorCpus.isSome()) {
      setLimit(executorCpuLimit, executorCpus.get());
    }

    // For the tasks which do not have CPU limit, count their CPU requests
    // into the executor's CPU limit as well, this is also to ensure the
    // executor's CPU limit is always greater than its CPU request. Please
    // note that if the flag `cgroups_enable_cfs` is not enabled, we should
    // not set the executor's CPU limit, otherwise the tasks which do not
    // have CPU limit will be throttled implicitly by the executor's CPU limit.
    if (cpuRequest.value() > 0) {
#ifdef __linux__
      if (flags.cgroups_enable_cfs) {
        setLimit(executorCpuLimit, cpuRequest);
      } else {
        executorCpuLimit = None();
      }
#else
      setLimit(executorCpuLimit, cpuRequest);
#endif // __linux__
    }
  }

  if (executorMemLimit.isSome()) {
    // Count the executor's memory request into its memory limit as well,
    // this is to ensure the executor's memory limit is always greater
    // than its memory request.
    Option<Value::Scalar> executorMem =
      executorResources.get<Value::Scalar>("mem");

    if (executorMem.isSome()) {
      setLimit(executorMemLimit, executorMem.get());
    }

    // For the tasks which do not have memory limit, count their memory
    // requests into the executor's memory limit as well, this is also
    // to ensure the executor's memory limit is always greater than its
    // memory request.
    if (memRequest.value() > 0) {
      setLimit(executorMemLimit, memRequest);
    }
  }

  google::protobuf::Map<string, Value::Scalar> executorLimits;
  if (executorCpuLimit.isSome()) {
    executorLimits.insert({"cpus", executorCpuLimit.get()});
  }

  if (executorMemLimit.isSome()) {
    executorLimits.insert({"mem", executorMemLimit.get()});
  }

  return executorLimits;
}


void Node::updateDrainStatus()
{
  if (drainConfig.isNone()) {
    return;
  }

  bool drained = operations.empty() && frameworks.empty();

  if (!drained) {
    return;
  }

  LOG(INFO) << "Node finished draining";

  const string drainConfigPath = paths::getDrainConfigPath(metaDir, info.id());

  Try<Nothing> rm = os::rm(drainConfigPath);

  if (rm.isError()) {
    EXIT(EXIT_FAILURE) << "Could not remove persisted drain configuration "
                       << "'" << drainConfigPath << "': " << rm.error();
  }

  drainConfig = None();
  estimatedDrainStartTime = None();
}


map<string, string> executorEnvironment(
    const Flags& flags,
    const ExecutorInfo& executorInfo,
    const string& directory,
    const NodeID& nodeId,
    const PID<Node>& nodePid,
    const Option<Secret>& authenticationToken,
    bool checkpoint)
{
  map<string, string> environment;

  // In cases where DNS is not available on the node, the absence of
  // LIBPROCESS_IP in the executor's environment will cause an error when the
  // new executor process attempts a hostname lookup. Thus, we pass the node's
  // LIBPROCESS_IP through here, even if the executor environment is specified
  // explicitly. Note that a LIBPROCESS_IP present in the provided flags will
  // override this value.
  Option<string> libprocessIP = os::getenv("LIBPROCESS_IP");
  if (libprocessIP.isSome()) {
    environment["LIBPROCESS_IP"] = libprocessIP.get();
  }

  if (flags.executor_environment_variables.isSome()) {
    foreachpair (const string& key,
                 const JSON::Value& value,
                 flags.executor_environment_variables->values) {
      // See node/flags.cpp where we validate each value is a string.
      CHECK(value.is<JSON::String>());
      environment[key] = value.as<JSON::String>().value;
    }
  }

  // Set LIBPROCESS_PORT so that we bind to a random free port (since
  // this might have been set via --port option). We do this before
  // the environment variables below in case it is included.
  environment["LIBPROCESS_PORT"] = "0";

  // Also add MEDIOS_NATIVE_JAVA_LIBRARY if it's not already present (and
  // like above, we do this before the environment variables below in
  // case the framework wants to override).
  // TODO(tillt): Adapt library towards JNI specific name once libmedios
  // has been split.
  if (environment.count("MEDIOS_NATIVE_JAVA_LIBRARY") == 0) {
    const string path =
      path::join(LIBDIR, os::libraries::expandName("medios-" VERSION));
    if (os::exists(path)) {
      environment["MEDIOS_NATIVE_JAVA_LIBRARY"] = path;
    }
  }

  // Also add MEDIOS_NATIVE_LIBRARY if it's not already present.
  // This environment variable is kept for offering non JVM-based
  // frameworks a more compact and JNI independent library.
  if (environment.count("MEDIOS_NATIVE_LIBRARY") == 0) {
    const string path =
      path::join(LIBDIR, os::libraries::expandName("medios-" VERSION));
    if (os::exists(path)) {
      environment["MEDIOS_NATIVE_LIBRARY"] = path;
    }
  }

  environment["MEDIOS_FRAMEWORK_ID"] = executorInfo.framework_id().value();
  environment["MEDIOS_EXECUTOR_ID"] = executorInfo.executor_id().value();
  environment["MEDIOS_DIRECTORY"] = directory;
  environment["MEDIOS_NODE_ID"] = nodeId.value();

  // The docker executor is a separate process and resolves a STORE volume
  // itself; it reads the same stores file this node declared (DR-036).
  if (flags.stores.isSome()) {
    environment["MEDIOS_STORES"] = flags.stores.get();
  }
  // An executor runs on the node's host, so it reaches the node at the
  // address the node bound. The advertised address can be a NAT mapping
  // that is unreachable from inside the host.
  UPID executorNodePid = nodePid;
  if ((flags.advertise_ip.isSome() || flags.advertise_port.isSome()) &&
      libprocessIP.isSome()) {
    Try<net::IP> ip = net::IP::parse(libprocessIP.get());
    CHECK_SOME(ip) << "Failed to parse LIBPROCESS_IP '"
                   << libprocessIP.get() << "'";
    executorNodePid.address =
      process::network::inet::Address(ip.get(), flags.port);
  }

  environment["MEDIOS_NODE_PID"] = stringify(executorNodePid);
  environment["MEDIOS_NODE_ENDPOINT"] = stringify(executorNodePid.address);
  environment["MEDIOS_CHECKPOINT"] = checkpoint ? "1" : "0";
  environment["MEDIOS_HTTP_COMMAND_EXECUTOR"] =
    flags.http_command_executor ? "1" : "0";

  if (flags.http_executor_domain_sockets) {
    // If `http_executor_domain_sockets` is true, the location should have
    // been set either by the user or automatically during node startup.
    CHECK(flags.domain_socket_location.isSome());
    environment["MEDIOS_DOMAIN_SOCKET"] = *flags.domain_socket_location;
  }

  // Set executor's shutdown grace period. If set, the customized value
  // from `ExecutorInfo` overrides the default from node flags.
  Duration executorShutdownGracePeriod = flags.executor_shutdown_grace_period;
  if (executorInfo.has_shutdown_grace_period()) {
    executorShutdownGracePeriod =
      Nanoseconds(executorInfo.shutdown_grace_period().nanoseconds());
  }

  environment["MEDIOS_EXECUTOR_SHUTDOWN_GRACE_PERIOD"] =
    stringify(executorShutdownGracePeriod);

  if (checkpoint) {
    environment["MEDIOS_RECOVERY_TIMEOUT"] = stringify(flags.recovery_timeout);

    // The maximum backoff duration to be used by an executor between two
    // retries when disconnected.
    environment["MEDIOS_SUBSCRIPTION_BACKOFF_MAX"] =
      stringify(flags.executor_reregistration_timeout);
  }

  if (authenticationToken.isSome()) {
    CHECK(authenticationToken->has_value());

    environment["MEDIOS_EXECUTOR_AUTHENTICATION_TOKEN"] =
      authenticationToken->value().data();
  }

  if (HookManager::hooksAvailable()) {
    // Include any environment variables from Hooks.
    // TODO(karya): Call environment decorator hook _after_ putting all
    // variables from executorInfo into 'env'. This would prevent the
    // ones provided by hooks from being overwritten by the ones in
    // executorInfo in case of a conflict. The overwriting takes places
    // at the callsites of executorEnvironment (e.g., ___launch function
    // in the containerizer implementation)
    // TODO(karya): Provide a mechanism to pass the new environment
    // variables created above (MEDIOS_*) on to the hook modules.
    const Environment& hooksEnvironment =
      HookManager::nodeExecutorEnvironmentDecorator(executorInfo);

    foreach (const Environment::Variable& variable,
             hooksEnvironment.variables()) {
      environment[variable.name()] = variable.value();
    }
  }

  return environment;
}


ostream& operator<<(ostream& stream, Node::State state)
{
  switch (state) {
    case Node::RECOVERING:   return stream << "RECOVERING";
    case Node::DISCONNECTED: return stream << "DISCONNECTED";
    case Node::RUNNING:      return stream << "RUNNING";
    case Node::TERMINATING:  return stream << "TERMINATING";
    default:                  return stream << "UNKNOWN";
  }
}


static string taskOrTaskGroup(
    const Option<TaskInfo>& task,
    const Option<TaskGroupInfo>& taskGroup)
{
  ostringstream out;
  if (task.isSome()) {
    out << "task '" << task->task_id() << "'";
  } else {
    CHECK_SOME(taskGroup);

    vector<TaskID> taskIds;
    foreach (const TaskInfo& task, taskGroup->tasks()) {
      taskIds.push_back(task.task_id());
    }
    out << "task group containing tasks " << taskIds;
  }

  return out.str();
}


static CommandInfo defaultExecutorCommandInfo(
    const string& launcherDir,
    const Option<string>& user)
{
  Result<string> path = os::realpath(
      path::join(launcherDir, MEDIOS_DEFAULT_EXECUTOR));

  CommandInfo commandInfo;
  if (path.isSome()) {
    commandInfo.set_shell(false);
    commandInfo.set_value(path.get());
    commandInfo.add_arguments(MEDIOS_DEFAULT_EXECUTOR);
    commandInfo.add_arguments("--launcher_dir=" + launcherDir);
  } else {
    commandInfo.set_shell(true);
    commandInfo.set_value(
        "echo '" +
        (path.isError() ? path.error() : "No such file or directory") +
        "'; exit 1");
  }

  if (user.isSome()) {
    commandInfo.set_user(user.get());
  }

  return commandInfo;
}


static void setLimit(Option<Value::Scalar>& limit, const Value::Scalar& delta)
{
  if (limit.isSome() && std::isinf(limit->value())) {
    // Just return if the limit is already infinite.
    return;
  }

  Value::Scalar scalar;
  if (limit.isNone() || std::isinf(delta.value())) {
    // Set limit directly if it is the first time or the value to be
    // added is infinite.
    scalar.set_value(delta.value());
  } else {
    // Add the value into the limit.
    scalar.set_value(limit->value() + delta.value());
  }

  limit = scalar;
};

} // namespace node {
} // namespace internal {
} // namespace medios {
