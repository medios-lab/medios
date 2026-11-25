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

#include <stdint.h>

#include <memory>
#include <set>
#include <string>
#include <utility>
#include <vector>

#include <medios/medios.hpp>

#include <medios/authorizer/authorizer.hpp>

#include <medios/allocator/allocator.hpp>
#include <medios/controller/contender.hpp>
#include <medios/controller/detector.hpp>

#include <medios/module/anonymous.hpp>
#include <medios/module/authorizer.hpp>

#include <medios/state/in_memory.hpp>
#include <medios/state/leveldb.hpp>
#include <medios/state/state.hpp>
#include <medios/state/storage.hpp>

#include <process/limiter.hpp>
#include <process/owned.hpp>
#include <process/pid.hpp>

#include <stout/check.hpp>
#include <stout/duration.hpp>
#include <stout/exit.hpp>
#include <stout/flags.hpp>
#include <stout/nothing.hpp>
#include <stout/option.hpp>
#include <stout/os.hpp>
#include <stout/path.hpp>
#include <stout/stringify.hpp>
#include <stout/strings.hpp>
#include <stout/try.hpp>
#include <stout/version.hpp>

#include "common/authorization.hpp"
#include "common/build.hpp"
#include "common/http.hpp"
#include "common/protobuf_utils.hpp"

#include "hook/manager.hpp"

#include "logging/flags.hpp"
#include "logging/logging.hpp"

#include "controller/controller.hpp"
#include "controller/registrar.hpp"

#include "controller/allocator/medios/hierarchical.hpp"

#include "controller/detector/standalone.hpp"

#include "module/manager.hpp"

#include "version/version.hpp"

using namespace medios::internal;
using namespace medios::internal::controller;

using medios::Authorizer;
using medios::ControllerInfo;
using medios::Parameter;
using medios::Parameters;

using medios::allocator::Allocator;

using medios::controller::contender::ControllerContender;

using medios::controller::detector::ControllerDetector;
using medios::controller::detector::StandaloneControllerDetector;

using medios::modules::Anonymous;
using medios::modules::ModuleManager;

using medios::state::InMemoryStorage;
using medios::state::Storage;

using process::Owned;
using process::RateLimiter;
using process::UPID;

using process::firewall::DisabledEndpointsFirewallRule;
using process::firewall::FirewallRule;

using std::cerr;
using std::cout;
using std::endl;
using std::move;
using std::ostringstream;
using std::set;
using std::shared_ptr;
using std::string;
using std::vector;


int main(int argc, char** argv)
{
  // The order of initialization of various controller components is as follows:
  // * Validate flags.
  // * Logging.
  // * Emit build information.
  // * Libprocess.
  // * Version process.
  // * Firewall rules: should be initialized before initializing HTTP endpoints.
  // * Modules: Load module libraries and manifests before they
  //   can be instantiated.
  // * Anonymous modules: Later components such as Allocators, and controller
  //   contender/detector might depend upon anonymous modules.
  // * Hooks.
  // * Allocator.
  // * Registry storage.
  // * State.
  // * Controller contender.
  // * Controller detector.
  // * Authorizer.
  // * Node removal rate limiter.
  // * `Controller` process.
  //
  // TODO(avinash): Add more comments discussing the rationale behind for this
  // particular component ordering.

  GOOGLE_PROTOBUF_VERIFY_VERSION;

  controller::Flags flags;

  Try<flags::Warnings> load = flags.load("MEDIOS_", argc, argv);

  if (flags.help) {
    cout << flags.usage() << endl;
    return EXIT_SUCCESS;
  }

  if (flags.version) {
    cout << "medios" << " " << MEDIOS_VERSION << endl;
    return EXIT_SUCCESS;
  }

  if (load.isError()) {
    cerr << load.error() << "\n\n"
         << "See `medios-controller --help` for a list of supported flags." << endl;
    return EXIT_FAILURE;
  }

  logging::initialize(argv[0], true, flags); // Catch signals.

  // Emit any flag warnings after logging is initialized.
  foreach (const flags::Warning& warning, load->warnings) {
    LOG(WARNING) << warning.message;
  }

  // Check that controller's version has the expected format (SemVer).
  {
    Try<Version> version = Version::parse(MEDIOS_VERSION);
    if (version.isError()) {
      EXIT(EXIT_FAILURE)
        << "Failed to parse Medios version '" << MEDIOS_VERSION << "': "
        << version.error();
    }
  }

  if (flags.ip_discovery_command.isSome() && flags.ip.isSome()) {
    EXIT(EXIT_FAILURE)
      << "Only one of `--ip` or `--ip_discovery_command` should be specified";
  }

  if (flags.ip_discovery_command.isSome()) {
    Try<string> ipAddress = os::shell(flags.ip_discovery_command.get());

    if (ipAddress.isError()) {
      EXIT(EXIT_FAILURE) << ipAddress.error();
    }

    os::setenv("LIBPROCESS_IP", strings::trim(ipAddress.get()));
  } else if (flags.ip.isSome()) {
    os::setenv("LIBPROCESS_IP", flags.ip.get());
  }

  os::setenv("LIBPROCESS_PORT", stringify(flags.port));

  if (flags.advertise_ip.isSome()) {
    os::setenv("LIBPROCESS_ADVERTISE_IP", flags.advertise_ip.get());
  }

  if (flags.advertise_port.isSome()) {
    os::setenv("LIBPROCESS_ADVERTISE_PORT", flags.advertise_port.get());
  }

  if (flags.controller_contender.isSome() ^ flags.controller_detector.isSome()) {
    EXIT(EXIT_FAILURE)
      << "Both --controller_contender and --controller_detector should "
         "be specified or omitted.";
  }

  os::setenv("LIBPROCESS_MEMORY_PROFILING", stringify(flags.memory_profiling));

  // Emit build information.
  LOG(INFO) << "Build: " << build::DATE << " by " << build::USER;
  LOG(INFO) << "Version: " << MEDIOS_VERSION;

  if (build::GIT_TAG.isSome()) {
    LOG(INFO) << "Git tag: " << build::GIT_TAG.get();
  }

  if (build::GIT_SHA.isSome()) {
    LOG(INFO) << "Git SHA: " << build::GIT_SHA.get();
  }

  // This should be the first invocation of `process::initialize`. If it returns
  // `false`, then it has already been called, which means that the
  // authentication realm for libprocess-level HTTP endpoints was not set to the
  // correct value for the controller.
  if (!process::initialize(
          "controller",
          READWRITE_HTTP_AUTHENTICATION_REALM,
          READONLY_HTTP_AUTHENTICATION_REALM)) {
    EXIT(EXIT_FAILURE) << "The call to `process::initialize()` in the controller's "
                       << "`main()` was not the function's first invocation";
  }

  spawn(new VersionProcess(), true);

  // Initialize firewall rules.
  if (flags.firewall_rules.isSome()) {
    vector<Owned<FirewallRule>> rules;

    const Firewall firewall = flags.firewall_rules.get();

    if (firewall.has_disabled_endpoints()) {
      hashset<string> paths;

      foreach (const string& path, firewall.disabled_endpoints().paths()) {
        paths.insert(path);
      }

      rules.emplace_back(new DisabledEndpointsFirewallRule(paths));
    }

    process::firewall::install(std::move(rules));
  }

  // Initialize modules.
  if (flags.modules.isSome() && flags.modulesDir.isSome()) {
    EXIT(EXIT_FAILURE) <<
      "Only one of --modules or --modules_dir should be specified";
  }

  if (flags.modulesDir.isSome()) {
    Try<Nothing> result = ModuleManager::load(flags.modulesDir.get());
    if (result.isError()) {
      EXIT(EXIT_FAILURE) << "Error loading modules: " << result.error();
    }
  }

  if (flags.modules.isSome()) {
    Try<Nothing> result = ModuleManager::load(flags.modules.get());
    if (result.isError()) {
      EXIT(EXIT_FAILURE) << "Error loading modules: " << result.error();
    }
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

  // Initialize hooks.
  if (flags.hooks.isSome()) {
    Try<Nothing> result = HookManager::initialize(flags.hooks.get());
    if (result.isError()) {
      EXIT(EXIT_FAILURE) << "Error installing hooks: " << result.error();
    }
  }

  // Create an instance of allocator.
  Try<Allocator*> allocator = Allocator::create(
      flags.allocator,
      flags.role_sorter,
      flags.framework_sorter);

  if (allocator.isError()) {
    EXIT(EXIT_FAILURE)
      << "Failed to create allocator '" << flags.allocator << "'"
      << ": " << allocator.error();
  }

  CHECK_NOTNULL(allocator.get());
  LOG(INFO) << "Using '" << flags.allocator << "' allocator";

  Storage* storage = nullptr;

  if (flags.registry == "in_memory") {
    storage = new InMemoryStorage();
  } else if (flags.registry == "leveldb") {
    if (flags.work_dir.isNone()) {
      EXIT(EXIT_FAILURE)
        << "Flag '--work_dir' is required for '--registry=leveldb'";
    }

    storage = new medios::state::LevelDBStorage(
        path::join(flags.work_dir.get(), "registry-leveldb"));
  } else {
    EXIT(EXIT_FAILURE)
      << "'" << flags.registry << "' is not a supported"
      << " option for registry persistence";
  }

  CHECK_NOTNULL(storage);

  medios::state::State* state = new medios::state::State(storage);
  Registrar* registrar =
    new Registrar(flags, state, READONLY_HTTP_AUTHENTICATION_REALM);

  ControllerContender* contender;
  ControllerDetector* detector;

  if (flags.etcd.isSome() &&
      (flags.controller_contender.isSome() ||
       flags.controller_detector.isSome())) {
    EXIT(EXIT_FAILURE)
      << "'--etcd' is mutually exclusive with"
      << " '--controller_contender'/'--controller_detector'";
  }

  Try<ControllerContender*> contender_ = ControllerContender::create(
      flags.etcd,
      flags.controller_contender,
      None());

  if (contender_.isError()) {
    EXIT(EXIT_FAILURE)
      << "Failed to create a controller contender: " << contender_.error();
  }

  contender = contender_.get();

  Try<ControllerDetector*> detector_ = ControllerDetector::create(
      flags.etcd,
      flags.controller_detector,
      None());

  if (detector_.isError()) {
    EXIT(EXIT_FAILURE)
      << "Failed to create a controller detector: " << detector_.error();
  }

  detector = detector_.get();

  Option<Authorizer*> authorizer_ = None();

  auto authorizerNames = strings::split(flags.authorizers, ",");
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
    if (flags.acls.isSome()) {
      LOG(INFO) << "Creating default '" << authorizerName << "' authorizer";

      authorizer = Authorizer::create(flags.acls.get());
    }
  }

  if (authorizer.isError()) {
    EXIT(EXIT_FAILURE) << "Could not create '" << authorizerName
                       << "' authorizer: " << authorizer.error();
  } else if (authorizer.isSome()) {
    authorizer_ = authorizer.get();

    // Set the authorization callbacks for libprocess HTTP endpoints.
    // Note that these callbacks capture `authorizer_.get()`, but the controller
    // creates a copy of the authorizer during construction. Thus, if in the
    // future it becomes possible to dynamically set the authorizer, this would
    // break.
    process::http::authorization::setCallbacks(
        medios::authorization::createAuthorizationCallbacks(authorizer_.get()));
  }

  Files files(READONLY_HTTP_AUTHENTICATION_REALM, authorizer_);

  Option<shared_ptr<RateLimiter>> nodeRemovalLimiter = None();
  if (flags.node_removal_rate_limit.isSome()) {
    // Parse the flag value.
    // TODO(vinod): Move this parsing logic to flags once we have a
    // 'Rate' abstraction in stout.
    vector<string> tokens =
      strings::tokenize(flags.node_removal_rate_limit.get(), "/");

    if (tokens.size() != 2) {
      EXIT(EXIT_FAILURE)
        << "Invalid node_removal_rate_limit: "
        << flags.node_removal_rate_limit.get()
        << ". Format is <Number of nodes>/<Duration>";
    }

    Try<int> permits = numify<int>(tokens[0]);
    if (permits.isError()) {
      EXIT(EXIT_FAILURE)
        << "Invalid node_removal_rate_limit: "
        << flags.node_removal_rate_limit.get()
        << ". Format is <Number of nodes>/<Duration>"
        << ": " << permits.error();
    }

    Try<Duration> duration = Duration::parse(tokens[1]);
    if (duration.isError()) {
      EXIT(EXIT_FAILURE)
        << "Invalid node_removal_rate_limit: "
        << flags.node_removal_rate_limit.get()
        << ". Format is <Number of nodes>/<Duration>"
        << ": " << duration.error();
    }

    nodeRemovalLimiter = new RateLimiter(permits.get(), duration.get());
  }

  Controller* controller =
    new Controller(
      allocator.get(),
      registrar,
      &files,
      contender,
      detector,
      authorizer_,
      nodeRemovalLimiter,
      flags);

  // Only the standalone detector needs this controller appointed as leader;
  // an etcd or module detector learns the leader from its election.
  StandaloneControllerDetector* standalone =
    dynamic_cast<StandaloneControllerDetector*>(detector);
  if (standalone != nullptr) {
    standalone->appoint(controller->info());
  }

  process::spawn(controller);
  process::wait(controller->self());

  delete controller;
  delete allocator.get();

  delete registrar;
  delete state;
  delete storage;
  delete contender;
  delete detector;

  if (authorizer_.isSome()) {
    delete authorizer_.get();
  }

  return EXIT_SUCCESS;
}
