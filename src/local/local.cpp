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

#include <map>
#include <memory>
#include <set>
#include <sstream>
#include <string>
#include <vector>

#include <medios/authentication/secret_generator.hpp>

#include <medios/authorizer/authorizer.hpp>

#include <medios/allocator/allocator.hpp>

#include <medios/module/anonymous.hpp>
#include <medios/module/authorizer.hpp>
#include <medios/module/contender.hpp>
#include <medios/module/detector.hpp>
#include <medios/module/secret_resolver.hpp>

#include <medios/secret/resolver.hpp>

#include <medios/node/resource_estimator.hpp>

#include <medios/state/in_memory.hpp>
#include <medios/state/protobuf.hpp>
#include <medios/state/storage.hpp>

#include <process/limiter.hpp>
#include <process/owned.hpp>
#include <process/pid.hpp>

#include <stout/duration.hpp>
#include <stout/exit.hpp>
#include <stout/foreach.hpp>
#include <stout/os.hpp>
#include <stout/path.hpp>
#include <stout/try.hpp>
#include <stout/strings.hpp>

#ifdef USE_SSL_SOCKET
#include <stout/os/permissions.hpp>
#endif // USE_SSL_SOCKET

#ifdef USE_SSL_SOCKET
#include "authentication/executor/jwt_secret_generator.hpp"
#endif // USE_SSL_SOCKET

#include "common/protobuf_utils.hpp"

#include "local.hpp"

#include "logging/flags.hpp"
#include "logging/logging.hpp"

#include "controller/controller.hpp"
#include "controller/registrar.hpp"

#include "controller/allocator/medios/hierarchical.hpp"
#include "controller/allocator/medios/sorter/drf/sorter.hpp"

#include "controller/contender/standalone.hpp"

#include "controller/detector/standalone.hpp"

#include "module/manager.hpp"

#include "node/gc.hpp"
#include "node/node.hpp"
#include "node/task_status_update_manager.hpp"

#include "node/containerizer/containerizer.hpp"

using namespace medios::internal;

using medios::SecretGenerator;

using medios::allocator::Allocator;

#ifdef USE_SSL_SOCKET
using medios::authentication::executor::JWTSecretGenerator;
#endif // USE_SSL_SOCKET

using medios::controller::contender::ControllerContender;
using medios::controller::contender::StandaloneControllerContender;

using medios::controller::detector::ControllerDetector;
using medios::controller::detector::StandaloneControllerDetector;

using medios::internal::controller::allocator::HierarchicalDRFAllocator;

using medios::internal::controller::Controller;
using medios::internal::controller::Registrar;

using medios::internal::node::Containerizer;
using medios::internal::node::GarbageCollector;
using medios::internal::node::Node;
using medios::internal::node::TaskStatusUpdateManager;

using medios::modules::Anonymous;
using medios::modules::ModuleManager;

using medios::node::QoSController;
using medios::node::ResourceEstimator;

using process::Owned;
using process::PID;
using process::RateLimiter;
using process::UPID;

using std::map;
using std::set;
using std::shared_ptr;
using std::string;
using std::stringstream;
using std::vector;


namespace medios {
namespace internal {
namespace local {

static Allocator* allocator = nullptr;
static medios::state::Storage* storage = nullptr;
static medios::state::protobuf::State* state = nullptr;
static Registrar* registrar = nullptr;
static Controller* controller = nullptr;
static map<Containerizer*, Node*> nodes;
static StandaloneControllerDetector* detector = nullptr;
static ControllerContender* contender = nullptr;
static Option<Authorizer*> authorizer_ = None();
static Files* files = nullptr;
static vector<GarbageCollector*>* garbageCollectors = nullptr;
static vector<TaskStatusUpdateManager*>* taskStatusUpdateManagers = nullptr;
static vector<ResourceEstimator*>* resourceEstimators = nullptr;
static vector<QoSController*>* qosControllers = nullptr;
static vector<SecretGenerator*>* secretGenerators = nullptr;
static vector<SecretResolver*>* secretResolvers = nullptr;


PID<Controller> launch(const Flags& flags, Allocator* _allocator)
{
  if (controller != nullptr) {
    LOG(FATAL) << "Can only launch one local cluster at a time (for now)";
  }

  if (_allocator == nullptr) {
    // Create a default allocator.
    Try<Allocator*> defaultAllocator = HierarchicalDRFAllocator::create();
    if (defaultAllocator.isError()) {
      EXIT(EXIT_FAILURE)
        << "Failed to create an instance of HierarchicalDRFAllocator: "
        << defaultAllocator.error();
    }

    // Update caller's instance.
    _allocator = defaultAllocator.get();

    // Save the instance for deleting later.
    allocator = defaultAllocator.get();
  } else {
    // TODO(benh): Figure out the behavior of allocator pointer and
    // remove the else block.
    allocator = nullptr;
  }

  files = new Files();

  {
    // Use to propagate necessary flags to controller. Without this, the
    // load call will spit out an error unless their corresponding
    // environment variables explicitly set.
    map<string, string> propagatedFlags;
    propagatedFlags["work_dir"] = path::join(flags.work_dir, "controller");

    controller::Flags controllerFlags;
    Try<flags::Warnings> load = controllerFlags.load(
        propagatedFlags,
        false,
        "MEDIOS_");

    if (load.isError()) {
      EXIT(EXIT_FAILURE)
        << "Failed to start a local cluster while loading"
        << " controller flags from the environment: " << load.error();
    }

    // Log any flag warnings.
    foreach (const flags::Warning& warning, load->warnings) {
      LOG(WARNING) << warning.message;
    }

    // Attempt to create the `work_dir` for controller as a convenience.
    Try<Nothing> mkdir = os::mkdir(controllerFlags.work_dir.get());
    if (mkdir.isError()) {
      EXIT(EXIT_FAILURE)
        << "Failed to create the work_dir for controller '"
        << controllerFlags.work_dir.get() << "': " << mkdir.error();
    }

    // Load modules. Note that this covers both, controller and node
    // specific modules as both use the same flag (--modules).
    if (controllerFlags.modules.isSome()) {
      Try<Nothing> result = ModuleManager::load(controllerFlags.modules.get());
      if (result.isError()) {
        EXIT(EXIT_FAILURE) << "Error loading modules: " << result.error();
      }
    }

    if (controllerFlags.registry == "in_memory") {
      storage = new medios::state::InMemoryStorage();
    } else {
      EXIT(EXIT_FAILURE)
        << "'" << controllerFlags.registry << "' is not a supported"
        << " option for registry persistence";
    }

    CHECK_NOTNULL(storage);

    state = new medios::state::protobuf::State(storage);
    registrar = new Registrar(
        controllerFlags,
        state,
        controller::READONLY_HTTP_AUTHENTICATION_REALM);

    contender = new StandaloneControllerContender();
    detector = new StandaloneControllerDetector();

    auto authorizerNames = strings::split(controllerFlags.authorizers, ",");
    if (authorizerNames.empty()) {
      EXIT(EXIT_FAILURE) << "No authorizer specified";
    }
    if (authorizerNames.size() > 1) {
      EXIT(EXIT_FAILURE) << "Multiple authorizers not supported";
    }
    string authorizerName = authorizerNames[0];

    // NOTE: The flag --authorizers overrides the flag --acls, i.e. if
    // a non default authorizer is requested, it will be used and
    // the contents of --acls will be ignored.
    // TODO(arojas): Consider adding support for multiple authorizers.
    Result<Authorizer*> authorizer((None()));
    if (authorizerName != controller::DEFAULT_AUTHORIZER) {
      LOG(INFO) << "Creating '" << authorizerName << "' authorizer";

      authorizer = Authorizer::create(authorizerName);
    } else {
      // `authorizerName` is `DEFAULT_AUTHORIZER` at this point.
      if (controllerFlags.acls.isSome()) {
        LOG(INFO) << "Creating default '" << authorizerName << "' authorizer";

        authorizer = Authorizer::create(controllerFlags.acls.get());
      }
    }

    if (authorizer.isError()) {
      EXIT(EXIT_FAILURE) << "Could not create '" << authorizerName
                         << "' authorizer: " << authorizer.error();
    } else if (authorizer.isSome()) {
      authorizer_ = authorizer.get();
    }

    Option<shared_ptr<RateLimiter>> nodeRemovalLimiter = None();
    if (controllerFlags.node_removal_rate_limit.isSome()) {
      // Parse the flag value.
      // TODO(vinod): Move this parsing logic to flags once we have a
      // 'Rate' abstraction in stout.
      vector<string> tokens =
        strings::tokenize(controllerFlags.node_removal_rate_limit.get(), "/");

      if (tokens.size() != 2) {
        EXIT(EXIT_FAILURE)
          << "Invalid node_removal_rate_limit: "
          << controllerFlags.node_removal_rate_limit.get()
          << ". Format is <Number of nodes>/<Duration>";
      }

      Try<int> permits = numify<int>(tokens[0]);
      if (permits.isError()) {
        EXIT(EXIT_FAILURE)
          << "Invalid node_removal_rate_limit: "
          << controllerFlags.node_removal_rate_limit.get()
          << ". Format is <Number of nodes>/<Duration>"
          << ": " << permits.error();
      }

      Try<Duration> duration = Duration::parse(tokens[1]);
      if (duration.isError()) {
        EXIT(EXIT_FAILURE)
          << "Invalid node_removal_rate_limit: "
          << controllerFlags.node_removal_rate_limit.get()
          << ". Format is <Number of nodes>/<Duration>"
          << ": " << duration.error();
      }

      nodeRemovalLimiter = new RateLimiter(permits.get(), duration.get());
    }

    // Create anonymous modules.
    foreach (const string& name, ModuleManager::find<Anonymous>()) {
      Try<Anonymous*> create = ModuleManager::create<Anonymous>(name);
      if (create.isError()) {
        EXIT(EXIT_FAILURE)
          << "Failed to create anonymous module named '" << name << "'";
      }

      // We don't bother keeping around the pointer to this anonymous
      // module, when we exit that will effectively free its memory.
      //
      // TODO(benh): We might want to add explicit finalization (and
      // maybe explicit initialization too) in order to let the module
      // do any housekeeping necessary when the controller is cleanly
      // terminating.
    }

    controller = new Controller(
        _allocator,
        registrar,
        files,
        contender,
        detector,
        authorizer_,
        nodeRemovalLimiter,
        controllerFlags);

    detector->appoint(controller->info());
  }

  PID<Controller> pid = process::spawn(controller);

  garbageCollectors = new vector<GarbageCollector*>();
  taskStatusUpdateManagers = new vector<TaskStatusUpdateManager*>();
  resourceEstimators = new vector<ResourceEstimator*>();
  qosControllers = new vector<QoSController*>();
  secretGenerators = new vector<SecretGenerator*>();
  secretResolvers = new vector<SecretResolver*>();

  vector<UPID> pids;

  for (int i = 0; i < flags.num_nodes; i++) {
    // Use to propagate necessary flags to node. Without this, the
    // load call will spit out an error unless their corresponding
    // environment variables explicitly set.
    map<string, string> propagatedFlags;

    // Use a different work/runtime directory for each node.
    propagatedFlags["work_dir"] =
      path::join(flags.work_dir, "nodes", stringify(i), "work");

    propagatedFlags["runtime_dir"] =
      path::join(flags.work_dir, "nodes", stringify(i), "run");

    node::Flags nodeFlags;
    Try<flags::Warnings> load = nodeFlags.load(
        propagatedFlags,
        false,
        "MEDIOS_");

    if (load.isError()) {
      EXIT(EXIT_FAILURE)
        << "Failed to start a local cluster while loading"
        << " node flags from the environment: " << load.error();
    }

    // Log any flag warnings (after logging is initialized).
    foreach (const flags::Warning& warning, load->warnings) {
      LOG(WARNING) << warning.message;
    }

    // Attempt to create the `work_dir` for node as a convenience.
    Try<Nothing> mkdir = os::mkdir(nodeFlags.work_dir);
    if (mkdir.isError()) {
      EXIT(EXIT_FAILURE)
        << "Failed to create the work_dir for node '"
        << nodeFlags.work_dir << "': " << mkdir.error();
    }

    // Attempt to create the `runtime_dir` for node as a convenience.
    mkdir = os::mkdir(nodeFlags.runtime_dir);
    if (mkdir.isError()) {
      EXIT(EXIT_FAILURE)
        << "Failed to create the runtime_dir for node '"
        << nodeFlags.runtime_dir << "': " << mkdir.error();
    }

    garbageCollectors->push_back(new GarbageCollector(nodeFlags.work_dir));
    taskStatusUpdateManagers->push_back(
        new TaskStatusUpdateManager(nodeFlags));

    Try<ResourceEstimator*> resourceEstimator =
      ResourceEstimator::create(nodeFlags.resource_estimator);

    if (resourceEstimator.isError()) {
      EXIT(EXIT_FAILURE)
        << "Failed to create resource estimator: " << resourceEstimator.error();
    }

    resourceEstimators->push_back(resourceEstimator.get());

    Try<QoSController*> qosController =
      QoSController::create(nodeFlags.qos_controller);

    if (qosController.isError()) {
      EXIT(EXIT_FAILURE)
        << "Failed to create QoS Controller: " << qosController.error();
    }

    qosControllers->push_back(qosController.get());

    SecretGenerator* secretGenerator = nullptr;

#ifdef USE_SSL_SOCKET
    if (nodeFlags.jwt_secret_key.isSome()) {
      Try<string> jwtSecretKey = os::read(nodeFlags.jwt_secret_key.get());
      if (jwtSecretKey.isError()) {
        EXIT(EXIT_FAILURE) << "Failed to read the file specified by "
                           << "--jwt_secret_key";
      }

      // TODO(greggomann): Factor the following code out into a common helper,
      // since we also do this when loading credentials.
      Try<os::Permissions> permissions =
        os::permissions(nodeFlags.jwt_secret_key.get());
      if (permissions.isError()) {
        LOG(WARNING) << "Failed to stat jwt secret key file '"
                     << nodeFlags.jwt_secret_key.get()
                     << "': " << permissions.error();
      } else if (permissions->others.rwx) {
        LOG(WARNING) << "Permissions on executor secret key file '"
                     << nodeFlags.jwt_secret_key.get()
                     << "' are too open; it is recommended that your"
                     << " key file is NOT accessible by others";
      }

      secretGenerator = new JWTSecretGenerator(jwtSecretKey.get());
    }
#endif // USE_SSL_SOCKET

    secretGenerators->push_back(secretGenerator);

    // Override the default launcher that gets created per node to
    // 'posix' if we're creating multiple nodes because the
    // LinuxLauncher does not support multiple nodes on the same host
    // (see MESOS-3793).
    if (flags.num_nodes > 1 && nodeFlags.launcher != "posix") {
      // TODO(benh): This log line will print out for each node!
      LOG(WARNING) << "Using the 'posix' launcher instead of '"
                   << nodeFlags.launcher << "' since currently only the "
                   << "'posix' launcher supports multiple nodes per host";
      nodeFlags.launcher = "posix";
    }

    // Initialize SecretResolver.
    Try<SecretResolver*> secretResolver =
      medios::SecretResolver::create(nodeFlags.secret_resolver);

    if (secretResolver.isError()) {
      EXIT(EXIT_FAILURE)
        << "Failed to initialize secret resolver: " << secretResolver.error();
    }

    secretResolvers->push_back(secretResolver.get());

    Try<Containerizer*> containerizer = Containerizer::create(
        nodeFlags,
        true,
        garbageCollectors->back(),
        secretResolver.get());

    if (containerizer.isError()) {
      EXIT(EXIT_FAILURE)
        << "Failed to create a containerizer: " << containerizer.error();
    }

    // NOTE: At this point detector is already initialized by the
    // Controller.
    Node* node = new Node(
        process::ID::generate("node"),
        nodeFlags,
        detector,
        containerizer.get(),
        files,
        garbageCollectors->back(),
        taskStatusUpdateManagers->back(),
        resourceEstimators->back(),
        qosControllers->back(),
        secretGenerators->back(),
        nullptr,
        nullptr,
        None(),
        authorizer_); // Same authorizer as controller.

    nodes[containerizer.get()] = node;

    pids.push_back(process::spawn(node));
  }

  return pid;
}


void shutdown()
{
  if (controller != nullptr) {
    process::terminate(controller->self());
    process::wait(controller->self());
    delete controller;
    delete allocator;
    controller = nullptr;

    // TODO(benh): Ugh! Because the isolator calls back into the node
    // (not the best design) we can't delete the node until we have
    // deleted the isolator. But since the node calls into the
    // isolator, we can't delete the isolator until we have stopped
    // the node.

    foreachpair (Containerizer* containerizer, Node* node, nodes) {
      process::terminate(node->self());
      process::wait(node->self());
      delete containerizer;
      delete node;
    }

    nodes.clear();

    if (authorizer_.isSome()) {
      delete authorizer_.get();
      authorizer_ = None();
    }

    delete detector;
    detector = nullptr;

    delete contender;
    contender = nullptr;

    delete files;
    files = nullptr;

    foreach (GarbageCollector* gc, *garbageCollectors) {
      delete gc;
    }

    delete garbageCollectors;
    garbageCollectors = nullptr;

    foreach (
        TaskStatusUpdateManager* taskStatusUpdateManager,
        *taskStatusUpdateManagers) {
      delete taskStatusUpdateManager;
    }

    delete taskStatusUpdateManagers;
    taskStatusUpdateManagers = nullptr;

    foreach (SecretResolver* secretResolver, *secretResolvers) {
      delete secretResolver;
    }

    foreach (SecretGenerator* secretGenerator, *secretGenerators) {
      delete secretGenerator;
    }

    delete secretGenerators;
    secretGenerators = nullptr;

    foreach (ResourceEstimator* estimator, *resourceEstimators) {
      delete estimator;
    }

    delete resourceEstimators;
    resourceEstimators = nullptr;

    foreach (QoSController* controller, *qosControllers) {
      delete controller;
    }

    delete qosControllers;
    qosControllers = nullptr;

    delete registrar;
    registrar = nullptr;

    delete state;
    state = nullptr;

    delete storage;
    storage = nullptr;

  }
}

} // namespace local {
} // namespace internal {
} // namespace medios {
