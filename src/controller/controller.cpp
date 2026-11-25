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

#include <algorithm>
#include <cctype>
#include <functional>
#include <iomanip>
#include <iterator>
#include <list>
#include <memory>
#include <set>
#include <sstream>
#include <utility>

#include <medios/module.hpp>
#include <medios/resource_quantities.hpp>
#include <medios/roles.hpp>

#include <medios/authentication/authenticator.hpp>

#include <medios/authorizer/authorizer.hpp>

#include <medios/allocator/allocator.hpp>
#include <medios/controller/contender.hpp>
#include <medios/controller/detector.hpp>

#include <medios/module/authenticator.hpp>

#include <medios/scheduler/scheduler.hpp>

#include <process/check.hpp>
#include <process/collect.hpp>
#include <process/defer.hpp>
#include <process/delay.hpp>
#include <process/http.hpp>
#include <process/id.hpp>
#include <process/limiter.hpp>
#include <process/owned.hpp>
#include <process/run.hpp>
#include <process/shared.hpp>

#include <process/metrics/metrics.hpp>

#include <stout/check.hpp>
#include <stout/duration.hpp>
#include <stout/error.hpp>
#include <stout/foreach.hpp>
#include <stout/ip.hpp>
#include <stout/lambda.hpp>
#include <stout/multihashmap.hpp>
#include <stout/net.hpp>
#include <stout/nothing.hpp>
#include <stout/numify.hpp>
#include <stout/option.hpp>
#include <stout/path.hpp>
#include <stout/stringify.hpp>
#include <stout/unreachable.hpp>
#include <stout/utils.hpp>
#include <stout/uuid.hpp>

#include "authentication/identity_assertion/identity_assertion_authenticator.hpp"

#include "common/authorization.hpp"
#include "common/http.hpp"
#include "common/openapi_embedded.hpp"
#include "common/protobuf_utils.hpp"
#include "common/status_utils.hpp"
#include "common/validation.hpp"

#include "credentials/credentials.hpp"

#include "hook/manager.hpp"

#include "logging/logging.hpp"

#include "controller/authorization.hpp"
#include "controller/flags.hpp"
#include "controller/controller.hpp"
#include "controller/registry_operations.hpp"
#include "controller/stores.hpp"
#include "controller/weights.hpp"

#include "module/manager.hpp"

#include "watcher/allowlist_watcher.hpp"

using std::make_move_iterator;
using std::reference_wrapper;
using std::set;
using std::shared_ptr;
using std::string;
using std::vector;

using process::wait; // Necessary on some OS's to disambiguate.
using process::Clock;
using process::ExitedEvent;
using process::Failure;
using process::Future;
using process::MessageEvent;
using process::Owned;
using process::PID;
using process::Process;
using process::RateLimiter;
using process::Time;
using process::UPID;

using process::http::authentication::Principal;

using process::metrics::Counter;

using google::protobuf::RepeatedPtrField;

namespace medios {
namespace internal {
namespace controller {

using medios::allocator::Allocator;
using medios::allocator::FrameworkOptions;
using medios::allocator::OfferConstraintsFilter;

using medios::authorization::createSubject;
using medios::authorization::VIEW_ROLE;
using medios::authorization::VIEW_FRAMEWORK;
using medios::authorization::VIEW_TASK;

using medios::authorization::ActionObject;

using medios::controller::contender::ControllerContender;

using medios::controller::detector::ControllerDetector;

using medios::scheduler::OfferConstraints;


class NodeObserver : public ProtobufProcess<NodeObserver>
{
public:
  NodeObserver(const UPID& _node,
                const NodeInfo& _nodeInfo,
                const NodeID& _nodeId,
                const PID<Controller>& _controller,
                const Option<shared_ptr<RateLimiter>>& _limiter,
                const shared_ptr<Metrics>& _metrics,
                const Duration& _nodePingTimeout,
                const size_t _maxNodePingTimeouts)
    : ProcessBase(process::ID::generate("node-observer")),
      node(_node),
      nodeInfo(_nodeInfo),
      nodeId(_nodeId),
      controller(_controller),
      limiter(_limiter),
      metrics(_metrics),
      nodePingTimeout(_nodePingTimeout),
      maxNodePingTimeouts(_maxNodePingTimeouts),
      timeouts(0),
      pinged(false),
      connected(true)
  {
    install<PongNodeMessage>(&NodeObserver::pong);
  }

  void reconnect()
  {
    connected = true;
  }

  void disconnect()
  {
    connected = false;
  }

protected:
  void initialize() override
  {
    ping();
  }

  void ping()
  {
    PingNodeMessage message;
    message.set_connected(connected);
    send(node, message);

    pinged = true;
    delay(nodePingTimeout, self(), &NodeObserver::timeout);
  }

  void pong()
  {
    timeouts = 0;
    pinged = false;

    // Cancel any pending unreachable transitions.
    if (markingUnreachable.isSome()) {
      // Need a copy for non-const access.
      Future<Nothing> future = markingUnreachable.get();
      future.discard();
    }
  }

  void timeout()
  {
    if (pinged) {
      timeouts++; // No pong has been received before the timeout.
      if (timeouts >= maxNodePingTimeouts) {
        // No pong has been received for the last
        // 'maxNodePingTimeouts' pings.
        markUnreachable();
      }
    }

    // NOTE: We keep pinging even if we schedule a transition to
    // UNREACHABLE. This is because if the node eventually responds
    // to a ping, we can cancel the UNREACHABLE transition.
    ping();
  }

  // Marking nodes unreachable is rate-limited and can be canceled if
  // a pong is received before `_markUnreachable` is called.
  //
  // TODO(neilc): Using a rate-limit when marking nodes unreachable
  // is only necessary for frameworks that are not PARTITION_AWARE.
  // For such frameworks, we shutdown their tasks when an unreachable
  // node reregisters, so a rate-limit is a useful safety
  // precaution. Once all frameworks are PARTITION_AWARE, we can
  // likely remove the rate-limit (MESOS-5948).
  void markUnreachable()
  {
    if (markingUnreachable.isSome()) {
      return; // Unreachable transition is already in progress.
    }

    Future<Nothing> acquire = Nothing();

    if (limiter.isSome()) {
      LOG(INFO) << "Scheduling transition of node " << nodeId
                << " to UNREACHABLE because of health check timeout";

      acquire = limiter.get()->acquire();
    }

    markingUnreachable = acquire.onAny(defer(self(), &Self::_markUnreachable));
    ++metrics->node_unreachable_scheduled;
  }

  void _markUnreachable()
  {
    CHECK_SOME(markingUnreachable);

    const Future<Nothing>& future = markingUnreachable.get();

    CHECK(!future.isFailed());

    if (future.isReady()) {
      ++metrics->node_unreachable_completed;

      dispatch(controller,
               &Controller::markUnreachable,
               nodeInfo,
               false,
               "health check timed out");
    } else if (future.isDiscarded()) {
      LOG(INFO) << "Canceling transition of node " << nodeId
                << " to UNREACHABLE because a pong was received!";

      ++metrics->node_unreachable_canceled;
    }

    markingUnreachable = None();
  }

private:
  const UPID node;
  const NodeInfo nodeInfo;
  const NodeID nodeId;
  const PID<Controller> controller;
  const Option<shared_ptr<RateLimiter>> limiter;
  shared_ptr<Metrics> metrics;
  Option<Future<Nothing>> markingUnreachable;
  const Duration nodePingTimeout;
  const size_t maxNodePingTimeouts;
  uint32_t timeouts;
  bool pinged;
  bool connected;
};


Controller::Controller(
    Allocator* _allocator,
    Registrar* _registrar,
    Files* _files,
    ControllerContender* _contender,
    ControllerDetector* _detector,
    const Option<Authorizer*>& _authorizer,
    const Option<shared_ptr<RateLimiter>>& _nodeRemovalLimiter,
    const Flags& _flags)
  : ProcessBase("controller"),
    flags(_flags),
    http(this),
    allocator(_allocator),
    registrar(_registrar),
    files(_files),
    contender(_contender),
    detector(_detector),
    authorizer(_authorizer),
    frameworks(flags),
    subscribers(this, flags.max_operator_event_stream_subscribers),
    authenticator(None()),
    metrics(new Metrics(*this)),
    electedTime(None()),
    offerConstraintsFilterOptions(
        {{flags.offer_constraints_re2_max_mem,
          flags.offer_constraints_re2_max_program_size}})
{
  nodes.limiter = _nodeRemovalLimiter;

  // NOTE: We populate 'info_' here instead of inside 'initialize()'
  // because 'StandaloneControllerDetector' needs access to the info.

  // Controller ID is generated randomly based on UUID.
  info_.set_id(id::UUID::random().toString());

  // NOTE: Currently, we store ip in ControllerInfo in network order,
  // which should be fixed. See MESOS-1201 for details.
  // TODO(marco): The ip, port, hostname fields above are
  //     being deprecated; the code should be removed once
  //     the deprecation cycle is complete.
  info_.set_ip(self().address.ip.in()->s_addr);

  info_.set_port(self().address.port);
  info_.set_pid(self());
  info_.set_version(MEDIOS_VERSION);

  for (const ControllerInfo::Capability& capability : CONTROLLER_CAPABILITIES()) {
    info_.add_capabilities()->CopyFrom(capability);
  }

  // Determine our hostname or use the hostname provided.
  string hostname;

  if (flags.hostname.isNone()) {
    if (flags.hostname_lookup) {
      Try<string> result = net::getHostname(self().address.ip);

      if (result.isError()) {
        LOG(FATAL) << "Failed to get hostname: " << result.error();
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

  info_.set_hostname(hostname);

  // This uses the new `Address` message in `ControllerInfo`.
  info_.mutable_address()->set_ip(stringify(self().address.ip));
  info_.mutable_address()->set_port(self().address.port);
  info_.mutable_address()->set_hostname(hostname);

  if (flags.domain.isSome()) {
    info_.mutable_domain()->CopyFrom(flags.domain.get());
  }
}


Controller::~Controller() {}


hashset<string> Controller::missingMinimumCapabilities(
    const ControllerInfo& controllerInfo, const Registry& registry)
{
  if (registry.minimum_capabilities().size() == 0) {
    return hashset<string>();
  }

  hashset<string> minimumCapabilities, controllerCapabilities;

  foreach (
      const Registry::MinimumCapability& minimumCapability,
      registry.minimum_capabilities()) {
    minimumCapabilities.insert(minimumCapability.capability());
  }

  foreach (
      const ControllerInfo::Capability& controllerCapability,
      controllerInfo.capabilities()) {
    controllerCapabilities.insert(
        ControllerInfo::Capability::Type_Name(controllerCapability.type()));
  }

  return minimumCapabilities - controllerCapabilities;
}


// TODO(vinod): Update this interface to return failed futures when
// capacity is reached.
struct BoundedRateLimiter
{
  BoundedRateLimiter(double qps, Option<uint64_t> _capacity)
    : limiter(new process::RateLimiter(qps)),
      capacity(_capacity),
      messages(0) {}

  process::Owned<process::RateLimiter> limiter;
  const Option<uint64_t> capacity;

  // Number of outstanding messages for this RateLimiter.
  // NOTE: ExitedEvents are throttled but not counted towards
  // the capacity here.
  uint64_t messages;
};


void Controller::initialize()
{
  LOG(INFO) << "Controller " << info_.id() << " (" << info_.hostname() << ")"
            << " started on " << self().address;

  LOG(INFO) << "Flags at startup: " << flags;

  if (process::address().ip.isLoopback()) {
    LOG(WARNING) << "\n**************************************************\n"
                 << "Controller bound to loopback interface!"
                 << " Cannot communicate with remote schedulers or nodes."
                 << " You might want to set '--ip' flag to a routable"
                 << " IP address.\n"
                 << "**************************************************";
  }

  // NOTE: We enforce a minimum node reregister timeout because the
  // node bounds its (re-)registration retries based on the minimum.
  if (flags.node_reregister_timeout < MIN_NODE_REREGISTER_TIMEOUT) {
    EXIT(EXIT_FAILURE)
      << "Invalid value '" << flags.node_reregister_timeout << "'"
      << " for --node_reregister_timeout:"
      << " Must be at least " << MIN_NODE_REREGISTER_TIMEOUT;
  }

  // Parse the percentage for the node removal limit.
  // TODO(bmahler): Add a 'Percentage' abstraction.
  if (!strings::endsWith(flags.recovery_node_removal_limit, "%")) {
    EXIT(EXIT_FAILURE)
      << "Invalid value '" << flags.recovery_node_removal_limit << "'"
      << " for --recovery_node_removal_percent_limit: " << "missing '%'";
  }

  Try<double> limit = numify<double>(
      strings::remove(
          flags.recovery_node_removal_limit,
          "%",
          strings::SUFFIX));

  if (limit.isError()) {
    EXIT(EXIT_FAILURE)
      << "Invalid value '" << flags.recovery_node_removal_limit << "'"
      << " for --recovery_node_removal_percent_limit: " << limit.error();
  }

  if (limit.get() < 0.0 || limit.get() > 100.0) {
    EXIT(EXIT_FAILURE)
      << "Invalid value '" << flags.recovery_node_removal_limit << "'"
      << " for --recovery_node_removal_percent_limit:"
      << " Must be within [0%-100%]";
  }

  // Log authentication state.
  if (flags.authenticate_frameworks) {
    LOG(INFO) << "Controller only allowing authenticated frameworks to register";
  } else {
    LOG(INFO) << "Controller allowing unauthenticated frameworks to register";
  }

  if (flags.authenticate_nodes) {
    LOG(INFO) << "Controller only allowing authenticated nodes to register";
  } else {
    LOG(INFO) << "Controller allowing unauthenticated nodes to register";
  }

  if (flags.authenticate_http_frameworks) {
    LOG(INFO) << "Controller only allowing authenticated HTTP frameworks to "
              << "register";
  } else {
    LOG(INFO) << "Controller allowing HTTP frameworks to register without "
              << "authentication";
  }

  // Load credentials.
  Option<Credentials> credentials;
  if (flags.credentials.isSome()) {
    Result<Credentials> _credentials =
      credentials::read(flags.credentials.get());
    if (_credentials.isError()) {
      EXIT(EXIT_FAILURE) << _credentials.error() << " (see --credentials flag)";
    } else if (_credentials.isNone()) {
      EXIT(EXIT_FAILURE)
        << "Credentials file must contain at least one credential"
        << " (see --credentials flag)";
    }
    // Store credentials in controller to use them in routes.
    credentials = _credentials.get();
  }

  // Extract authenticator names and validate them.
  authenticatorNames = strings::split(flags.authenticators, ",");
  if (authenticatorNames.empty()) {
    EXIT(EXIT_FAILURE) << "No authenticator specified";
  }
  if (authenticatorNames.size() > 1) {
    EXIT(EXIT_FAILURE) << "Multiple authenticators not supported";
  }
  if (authenticatorNames[0] != DEFAULT_AUTHENTICATOR &&
      !modules::ModuleManager::contains<Authenticator>(
          authenticatorNames[0])) {
    EXIT(EXIT_FAILURE)
      << "Authenticator '" << authenticatorNames[0] << "' not found."
      << " Check the spelling (compare to '" << DEFAULT_AUTHENTICATOR << "')"
      << " or verify that the authenticator was loaded successfully"
      << " (see --modules)";
  }

  // TODO(tillt): Allow multiple authenticators to be loaded and enable
  // the authenticatee to select the appropriate one. See MESOS-1939.
  if (authenticatorNames[0] == DEFAULT_AUTHENTICATOR) {
    LOG(INFO) << "Using default '" << DEFAULT_AUTHENTICATOR
              << "' authenticator";

    authenticator = new authentication::IdentityAssertionAuthenticator();
  } else {
    Try<Authenticator*> module =
      modules::ModuleManager::create<Authenticator>(authenticatorNames[0]);
    if (module.isError()) {
      EXIT(EXIT_FAILURE)
        << "Could not create authenticator module '"
        << authenticatorNames[0] << "': " << module.error();
    }
    LOG(INFO) << "Using '" << authenticatorNames[0] << "' authenticator";
    authenticator = module.get();
  }

  // Give Authenticator access to credentials when needed.
  CHECK_SOME(authenticator);
  Try<Nothing> initialize = authenticator.get()->initialize(credentials);
  if (initialize.isError()) {
    const string error =
      "Failed to initialize authenticator '" + authenticatorNames[0] +
      "': " + initialize.error();
    if (flags.authenticate_frameworks || flags.authenticate_nodes) {
      EXIT(EXIT_FAILURE)
        << "Failed to start controller with authentication enabled: " << error;
    } else {
      // A failure to initialize the authenticator does lead to
      // unusable authentication but still allows non authenticating
      // frameworks and nodes to connect.
      LOG(WARNING) << "Only non-authenticating frameworks and nodes are "
                   << "allowed to connect. "
                   << "Authentication is disabled: " << error;

      delete authenticator.get();
      authenticator = None();
    }
  }

  if (flags.authenticate_http_readonly) {
    Try<Nothing> result = initializeHttpAuthenticators(
        READONLY_HTTP_AUTHENTICATION_REALM,
        strings::split(flags.http_authenticators, ","),
        credentials);

    if (result.isError()) {
      EXIT(EXIT_FAILURE) << result.error();
    }
  }

  if (flags.authenticate_http_readwrite) {
    Try<Nothing> result = initializeHttpAuthenticators(
        READWRITE_HTTP_AUTHENTICATION_REALM,
        strings::split(flags.http_authenticators, ","),
        credentials);

    if (result.isError()) {
      EXIT(EXIT_FAILURE) << result.error();
    }
  }

  if (flags.authenticate_http_frameworks) {
    // The `--http_framework_authenticators` flag should always be set when HTTP
    // framework authentication is enabled.
    if (flags.http_framework_authenticators.isNone()) {
      EXIT(EXIT_FAILURE)
        << "Missing `--http_framework_authenticators` flag. This must be used "
        << "in conjunction with `--authenticate_http_frameworks`";
    }

    Try<Nothing> result = initializeHttpAuthenticators(
        DEFAULT_HTTP_FRAMEWORK_AUTHENTICATION_REALM,
        strings::split(flags.http_framework_authenticators.get(), ","),
        credentials);

    if (result.isError()) {
      EXIT(EXIT_FAILURE) << result.error();
    }
  }

  if (authorizer.isSome()) {
    LOG(INFO) << "Authorization enabled";
  }

  if (flags.rate_limits.isSome()) {
    // Add framework rate limiters.
    foreach (const RateLimit& limit_, flags.rate_limits->limits()) {
      if (frameworks.limiters.contains(limit_.principal())) {
        EXIT(EXIT_FAILURE)
          << "Duplicate principal " << limit_.principal()
          << " found in RateLimits configuration";
      }

      if (limit_.has_qps() && limit_.qps() <= 0) {
        EXIT(EXIT_FAILURE)
          << "Invalid qps: " << limit_.qps()
          << ". It must be a positive number";
      }

      if (limit_.has_qps()) {
        Option<uint64_t> capacity;
        if (limit_.has_capacity()) {
          capacity = limit_.capacity();
        }
        frameworks.limiters.put(
            limit_.principal(),
            Owned<BoundedRateLimiter>(
                new BoundedRateLimiter(limit_.qps(), capacity)));
      } else {
        frameworks.limiters.put(limit_.principal(), None());
      }
    }

    if (flags.rate_limits->has_aggregate_default_qps() &&
        flags.rate_limits->aggregate_default_qps() <= 0) {
      EXIT(EXIT_FAILURE)
        << "Invalid aggregate_default_qps: "
        << flags.rate_limits->aggregate_default_qps()
        << ". It must be a positive number";
    }

    if (flags.rate_limits->has_aggregate_default_qps()) {
      Option<uint64_t> capacity;
      if (flags.rate_limits->has_aggregate_default_capacity()) {
        capacity = flags.rate_limits->aggregate_default_capacity();
      }
      frameworks.defaultLimiter =
        Owned<BoundedRateLimiter>(new BoundedRateLimiter(
            flags.rate_limits->aggregate_default_qps(), capacity));
    }

    LOG(INFO) << "Framework rate limiting enabled";
  }

  // If the rate limiter is injected for testing,
  // the flag may not be set.
  if (nodes.limiter.isSome() && flags.node_removal_rate_limit.isSome()) {
    LOG(INFO) << "Node removal is rate limited to "
              << flags.node_removal_rate_limit.get();
  }

  // If "--roles" is set, configure the role allowlist.
  // TODO(neilc): Remove support for explicit roles in ~Medios 0.32.
  if (flags.roles.isSome()) {
    LOG(WARNING) << "The '--roles' flag is deprecated. This flag will be "
                 << "removed in the future. See the Medios 0.27 upgrade "
                 << "notes for more information";

    Try<vector<string>> roles = roles::parse(flags.roles.get());
    if (roles.isError()) {
      EXIT(EXIT_FAILURE) << "Failed to parse roles: " << roles.error();
    }

    roleAllowlist = hashset<string>();
    foreach (const string& role, roles.get()) {
      roleAllowlist->insert(role);
    }

    if (roleAllowlist->size() < roles->size()) {
      LOG(WARNING) << "Duplicate values in '--roles': " << flags.roles.get();
    }

    // The default role is always allowed.
    roleAllowlist->insert("*");
  }

  // Add role weights.
  if (flags.weights.isSome()) {
    vector<string> tokens = strings::tokenize(flags.weights.get(), ",");

    foreach (const string& token, tokens) {
      vector<string> pair = strings::tokenize(token, "=");
      if (pair.size() != 2) {
        EXIT(EXIT_FAILURE)
          << "Invalid weight: '" << token << "'. --weights should"
          << " be of the form 'role=weight,role=weight'";
      } else if (!isAllowlistedRole(pair[0])) {
        EXIT(EXIT_FAILURE)
          << "Invalid weight: '" << token << "'. " << pair[0]
          << " is not a valid role";
      }

      double weight = atof(pair[1].c_str());
      if (weight <= 0) {
        EXIT(EXIT_FAILURE)
          << "Invalid weight: '" << token << "'. Weights must be positive";
      }

      weights[pair[0]] = weight;
    }
  }

  // Verify the timeout is greater than zero.
  if (flags.offer_timeout.isSome() &&
      flags.offer_timeout.get() <= Duration::zero()) {
    EXIT(EXIT_FAILURE)
      << "Invalid value '" << flags.offer_timeout.get() << "'"
      << " for --offer_timeout: Must be greater than zero";
  }

  // Parse min_allocatable_resources.
  vector<ResourceQuantities> minAllocatableResources;
  foreach (
      const string& token,
      strings::tokenize(flags.min_allocatable_resources, "|")) {
    Try<ResourceQuantities> resourceQuantities =
      ResourceQuantities::fromString(token);

    if (resourceQuantities.isError()) {
      EXIT(EXIT_FAILURE) << "Error parsing min_allocatable_resources '"
                         << flags.min_allocatable_resources
                         << "': " << resourceQuantities.error();
    }

    // We check the configuration against first-class resources and warn
    // against possible mis-configuration (e.g. typo).
    set<string> firstClassResources = {"cpus", "mem", "disk", "ports", "gpus"};
    for (auto it = resourceQuantities->begin(); it != resourceQuantities->end();
         ++it) {
      if (firstClassResources.count(it->first) == 0) {
        LOG(WARNING) << "Non-first-class resource '" << it->first
                     << "' is configured as part of min_allocatable_resources";
      }
    }

    minAllocatableResources.push_back(resourceQuantities.get());
  }

  // Initialize the allocator options.
  medios::allocator::Options options;

  options.allocationInterval = flags.allocation_interval;
  options.fairnessExcludeResourceNames =
    flags.fair_sharing_excluded_resource_names;
  options.filterGpuResources = flags.filter_gpu_resources;
  options.filterSharedResources = flags.filter_shared_resources;
  options.domain = flags.domain;
  options.minAllocatableResources = minAllocatableResources;
  options.maxCompletedFrameworks = flags.max_completed_frameworks;
  options.publishPerFrameworkMetrics = flags.publish_per_framework_metrics;
  options.readonlyHttpAuthenticationRealm = READONLY_HTTP_AUTHENTICATION_REALM;
  options.authorizer = authorizer;
  options.recoveryTimeout = flags.allocator_recovery_timeout;
  options.nodeRecoveryFactor = flags.allocator_node_recovery_factor;

  // Initialize the allocator.
  allocator->initialize(
      options,
      defer(self(), &Controller::offer, lambda::_1, lambda::_2),
      defer(self(), &Controller::inverseOffer, lambda::_1, lambda::_2));

  // Parse the allowlist. Passing Allocator::updateAllowlist()
  // callback is safe because we shut down the allowlistWatcher in
  // Controller::finalize(), while allocator lifetime is greater than
  // controllers. Therefore there is no risk of calling into an allocator
  // that has been cleaned up.
  allowlistWatcher = new AllowlistWatcher(
      flags.allowlist,
      ALLOWLIST_WATCH_INTERVAL,
      [this](const Option<hashset<string>>& allowlist) {
        return allocator->updateAllowlist(allowlist);
      });
  spawn(allowlistWatcher);

  nextFrameworkId = 0;
  nextNodeId = 0;
  nextOfferId = 0;

  startTime = Clock::now();

  install<scheduler::Call>(&Controller::receive);

  // Install handler functions for certain messages.
  install<SubmitSchedulerRequest>(
      &Controller::submitScheduler,
      &SubmitSchedulerRequest::name);

  install<RegisterFrameworkMessage>(
      &Controller::registerFramework);

  install<ReregisterFrameworkMessage>(
      &Controller::reregisterFramework);

  install<UnregisterFrameworkMessage>(
      &Controller::unregisterFramework,
      &UnregisterFrameworkMessage::framework_id);

  install<DeactivateFrameworkMessage>(
      &Controller::deactivateFramework,
      &DeactivateFrameworkMessage::framework_id);

  install<ResourceRequestMessage>(
      &Controller::resourceRequest,
      &ResourceRequestMessage::framework_id,
      &ResourceRequestMessage::requests);

  install<LaunchTasksMessage>(
      &Controller::launchTasks);

  install<ReviveOffersMessage>(
      &Controller::reviveOffers,
      &ReviveOffersMessage::framework_id,
      &ReviveOffersMessage::roles);

  install<KillTaskMessage>(
      &Controller::killTask,
      &KillTaskMessage::framework_id,
      &KillTaskMessage::task_id);

  install<StatusUpdateAcknowledgementMessage>(
      &Controller::statusUpdateAcknowledgement);

  install<FrameworkToExecutorMessage>(
      &Controller::schedulerMessage);

  install<RegisterNodeMessage>(
      &Controller::registerNode);

  install<ReregisterNodeMessage>(
      &Controller::reregisterNode);

  install<UnregisterNodeMessage>(
      &Controller::unregisterNode,
      &UnregisterNodeMessage::node_id);

  install<StatusUpdateMessage>(
      &Controller::statusUpdate);

  // Added in 0.24.0 to support HTTP schedulers. Since
  // these do not have a pid, the node must forward
  // messages through the controller.
  install<ExecutorToFrameworkMessage>(
      &Controller::executorMessage);

  install<ReconcileTasksMessage>(
      &Controller::reconcileTasks);

  install<UpdateOperationStatusMessage>(
      &Controller::updateOperationStatus);

  install<ExitedExecutorMessage>(
      &Controller::exitedExecutor,
      &ExitedExecutorMessage::node_id,
      &ExitedExecutorMessage::framework_id,
      &ExitedExecutorMessage::executor_id,
      &ExitedExecutorMessage::status);

  install<UpdateNodeMessage>(&Controller::updateNode);

  install<AuthenticateMessage>(
      &Controller::authenticate,
      &AuthenticateMessage::pid,
      &AuthenticateMessage::principal);

  // Setup HTTP routes.
  route("/api/v1",
        // TODO(benh): Is this authentication realm sufficient or do
        // we need some kind of hybrid if we expect both schedulers
        // and operators/tooling to use this endpoint?
        READWRITE_HTTP_AUTHENTICATION_REALM,
        Http::API_HELP(),
        [this](const process::http::Request& request,
               const Option<Principal>& principal) {
          logRequest(request);
          return http.api(request, principal);
        });
  route("/api/v1/scheduler",
        DEFAULT_HTTP_FRAMEWORK_AUTHENTICATION_REALM,
        Http::SCHEDULER_HELP(),
        [this](const process::http::Request& request,
               const Option<Principal>& principal) {
          logRequest(request);
          return http.scheduler(request, principal);
        });
  route("/create-volumes",
        READWRITE_HTTP_AUTHENTICATION_REALM,
        Http::CREATE_VOLUMES_HELP(),
        [this](const process::http::Request& request,
               const Option<Principal>& principal) {
          logRequest(request);
          return http.createVolumes(request, principal);
        });
  route("/destroy-volumes",
        READWRITE_HTTP_AUTHENTICATION_REALM,
        Http::DESTROY_VOLUMES_HELP(),
        [this](const process::http::Request& request,
               const Option<Principal>& principal) {
          logRequest(request);
          return http.destroyVolumes(request, principal);
        });
  route("/frameworks",
        READONLY_HTTP_AUTHENTICATION_REALM,
        Http::FRAMEWORKS_HELP(),
        [this](const process::http::Request& request,
               const Option<Principal>& principal) {
          logRequest(request);
          return http.frameworks(request, principal)
            .onReady([request](const process::http::Response& response) {
              logResponse(request, response);
            });
        });
  route("/flags",
        READONLY_HTTP_AUTHENTICATION_REALM,
        Http::FLAGS_HELP(),
        [this](const process::http::Request& request,
               const Option<Principal>& principal) {
          logRequest(request);
          return http.flags(request, principal);
        });
  route("/health",
        Http::HEALTH_HELP(),
        [this](const process::http::Request& request) {
          return http.health(request);
        });
  {
    // OpenAPI 3.x specification discovery (#261). Served unauthenticated
    // because the spec is public surface, the same as /help.
    auto serveOpenapi = [](const process::http::Request&)
        -> process::Future<process::http::Response> {
      process::http::Response response =
          process::http::OK(medios::openapi::controllerSpec);
      response.headers["Content-Type"] = "application/openapi+json";
      return response;
    };
    route("/openapi.json", Http::OPENAPI_HELP(), serveOpenapi);
    route("/.well-known/openapi.json", Http::OPENAPI_HELP(), serveOpenapi);
  }
  route("/redirect",
        Http::REDIRECT_HELP(),
        [this](const process::http::Request& request) {
          return http.redirect(request);
        });
  route("/reserve",
        READWRITE_HTTP_AUTHENTICATION_REALM,
        Http::RESERVE_HELP(),
        [this](const process::http::Request& request,
               const Option<Principal>& principal) {
          logRequest(request);
          return http.reserve(request, principal);
        });
  route("/roles",
        READONLY_HTTP_AUTHENTICATION_REALM,
        Http::ROLES_HELP(),
        [this](const process::http::Request& request,
               const Option<Principal>& principal) {
          logRequest(request);
          return http.roles(request, principal);
        });
  route("/teardown",
        READWRITE_HTTP_AUTHENTICATION_REALM,
        Http::TEARDOWN_HELP(),
        [this](const process::http::Request& request,
               const Option<Principal>& principal) {
          logRequest(request);
          return http.teardown(request, principal);
        });
  route("/nodes",
        READONLY_HTTP_AUTHENTICATION_REALM,
        Http::NODES_HELP(),
        [this](const process::http::Request& request,
               const Option<Principal>& principal) {
          logRequest(request);
          return http.nodes(request, principal)
            .onReady([request](const process::http::Response& response) {
              logResponse(request, response);
            });
        });
  route("/state",
        READONLY_HTTP_AUTHENTICATION_REALM,
        Http::STATE_HELP(),
        [this](const process::http::Request& request,
               const Option<Principal>& principal) {
          logRequest(request);
          return http.state(request, principal)
            .onReady([request](const process::http::Response& response) {
              logResponse(request, response);
            });
        });
  route("/state-summary",
        READONLY_HTTP_AUTHENTICATION_REALM,
        Http::STATESUMMARY_HELP(),
        [this](const process::http::Request& request,
               const Option<Principal>& principal) {
          logRequest(request);
          return http.stateSummary(request, principal)
            .onReady([request](const process::http::Response& response) {
              logResponse(request, response);
            });
        });
  route("/tasks",
        READONLY_HTTP_AUTHENTICATION_REALM,
        Http::TASKS_HELP(),
        [this](const process::http::Request& request,
               const Option<Principal>& principal) {
          logRequest(request);
          return http.tasks(request, principal)
            .onReady([request](const process::http::Response& response) {
              logResponse(request, response);
            });
        });
  route("/maintenance/schedule",
        READWRITE_HTTP_AUTHENTICATION_REALM,
        Http::MAINTENANCE_SCHEDULE_HELP(),
        [this](const process::http::Request& request,
               const Option<Principal>& principal) {
          logRequest(request);
          return http.maintenanceSchedule(request, principal);
        });
  route("/maintenance/status",
        READONLY_HTTP_AUTHENTICATION_REALM,
        Http::MAINTENANCE_STATUS_HELP(),
        [this](const process::http::Request& request,
               const Option<Principal>& principal) {
          logRequest(request);
          return http.maintenanceStatus(request, principal);
        });
  route("/machine/down",
        READWRITE_HTTP_AUTHENTICATION_REALM,
        Http::MACHINE_DOWN_HELP(),
        [this](const process::http::Request& request,
               const Option<Principal>& principal) {
          logRequest(request);
          return http.machineDown(request, principal);
        });
  route("/machine/up",
        READWRITE_HTTP_AUTHENTICATION_REALM,
        Http::MACHINE_UP_HELP(),
        [this](const process::http::Request& request,
               const Option<Principal>& principal) {
          logRequest(request);
          return http.machineUp(request, principal);
        });
  route("/unreserve",
        READWRITE_HTTP_AUTHENTICATION_REALM,
        Http::UNRESERVE_HELP(),
        [this](const process::http::Request& request,
               const Option<Principal>& principal) {
          logRequest(request);
          return http.unreserve(request, principal);
        });
  route("/weights",
        READWRITE_HTTP_AUTHENTICATION_REALM,
        Http::WEIGHTS_HELP(),
        [this](const process::http::Request& request,
               const Option<Principal>& principal) {
          logRequest(request);
          return http.weights(request, principal);
        });

  // Deprecated routes:
  route("/quota",
        READWRITE_HTTP_AUTHENTICATION_REALM,
        Http::QUOTA_HELP(),
        [this](const process::http::Request& request,
               const Option<Principal>& principal) {
          logRequest(request);
          return http.quota(request, principal);
        });

  // TODO(tillt): Use generalized lambda capture once we adopt C++14.
  Option<Authorizer*> _authorizer = authorizer;

  auto authorize = [_authorizer](const Option<Principal>& principal) {
    return authorization::authorizeLogAccess(_authorizer, principal);
  };

  // Expose the log file for the webui. Fall back to 'log_dir' if
  // an explicit file was not specified.
  if (flags.external_log_file.isSome()) {
    files->attach(flags.external_log_file.get(), "/controller/log", authorize)
      .onAny(defer(self(),
                   &Self::fileAttached,
                   lambda::_1,
                   flags.external_log_file.get()));
  } else if (flags.log_dir.isSome()) {
    Try<string> log = logging::getLogFile(
        logging::getLogSeverity(flags.logging_level));

    if (log.isError()) {
      LOG(ERROR) << "Controller log file cannot be found: " << log.error();
    } else {
      files->attach(log.get(), "/controller/log", authorize)
        .onAny(defer(self(), &Self::fileAttached, lambda::_1, log.get()));
    }
  }

  contender->initialize(info_);

  // Start contending to be a leading controller and detecting the current
  // leader.
  contend();
  detector->detect()
    .onAny(defer(self(), &Controller::detected, lambda::_1));
}


void Controller::finalize()
{
  LOG(INFO) << "Controller terminating";

  // NOTE: Even though we remove the node and framework from the
  // allocator, it is possible that offers are already dispatched to
  // this controller. In tests, if a new controller (with the same PID) is
  // started, it might process the offers from the old controller's
  // allocator.
  // TODO(vinod): Fix the above race by changing the allocator
  // interface to return a stream of offer events.

  // Remove the nodes.
  foreachvalue (Node* node, nodes.registered) {
    // We first remove the node from the allocator so that any
    // recovered resources below are not reoffered.
    allocator->removeNode(node->id);

    foreachkey (const FrameworkID& frameworkId, utils::copy(node->tasks)) {
      foreachvalue (Task* task, utils::copy(node->tasks[frameworkId])) {
        removeTask(task);
      }
    }

    // Remove executors.
    foreachkey (const FrameworkID& frameworkId, utils::copy(node->executors)) {
      foreachkey (const ExecutorID& executorId,
                  utils::copy(node->executors[frameworkId])) {
        removeExecutor(node, frameworkId, executorId);
      }
    }

    foreach (Offer* offer, utils::copy(node->offers)) {
      discardOffer(offer);
    }

    // Remove inverse offers.
    foreach (InverseOffer* inverseOffer, utils::copy(node->inverseOffers)) {
      // We don't need to update the allocator because the node has already
      // been removed.
      removeInverseOffer(inverseOffer);
    }

    // Terminate the node observer.
    terminate(node->observer);
    wait(node->observer);

    delete node->observer;
    delete node;
  }
  nodes.registered.clear();

  // Remove the frameworks.
  // Note we are not deleting the pointers to the frameworks from the
  // roles because it is unnecessary bookkeeping at this point since
  // we are shutting down.
  foreachvalue (Framework* framework, frameworks.registered) {
    allocator->removeFramework(framework->id());

    // No tasks/executors/offers should remain since the nodes
    // have been removed.
    CHECK(framework->tasks.empty());
    CHECK(framework->executors.empty());
    CHECK(framework->offers.empty());
    CHECK(framework->inverseOffers.empty());

    delete framework;
  }
  frameworks.registered.clear();

  CHECK(offers.empty());
  CHECK(inverseOffers.empty());

  foreachvalue (Future<Option<string>> future, authenticating) {
    // NOTE: This is necessary during tests because a copy of
    // this future is used to setup authentication timeout. If a
    // test doesn't discard this future, authentication timeout might
    // fire in a different test and any associated callbacks
    // (e.g., '_authenticate()') would be called. This is because the
    // controller pid doesn't change across the tests.
    // TODO(vinod): This seems to be a bug in libprocess or the
    // testing infrastructure.
    future.discard();
  }

  foreachvalue (Role* role, roles) {
    delete role;
  }
  roles.clear();

  // NOTE: This is necessary during tests because we don't want the
  // timer to fire in a different test and invoke the callback.
  // The callback would be invoked because the controller pid doesn't
  // change across the tests.
  // TODO(vinod): This seems to be a bug in libprocess or the
  // testing infrastructure.
  if (nodes.recoveredTimer.isSome()) {
    Clock::cancel(nodes.recoveredTimer.get());
  }

  if (registryGcTimer.isSome()) {
    Clock::cancel(registryGcTimer.get());
  }

  terminate(allowlistWatcher);
  wait(allowlistWatcher);
  delete allowlistWatcher;

  if (authenticator.isSome()) {
    delete authenticator.get();
  }
}


void Controller::exited(
    const FrameworkID& frameworkId,
    const StreamingHttpConnection<v1::scheduler::Event>& http)
{
  foreachvalue (Framework* framework, frameworks.registered) {
    if (framework->http().isSome() &&
        framework->http()->writer == http.writer) {
      CHECK_EQ(frameworkId, framework->id());
      _exited(framework);
      return;
    }

    // If the framework has reconnected, the writer will not match
    // above, and we will have a framework with a matching id.
    if (frameworkId == framework->id()) {
      LOG(INFO) << "Ignoring disconnection for framework "
                << *framework << " as it has already reconnected";

      return;
    }
  }
}


void Controller::exited(const UPID& pid)
{
  foreachvalue (Framework* framework, frameworks.registered) {
    if (framework->pid() == pid) {
      // See comments in `receive()` on why we send an error message
      // to the framework upon detecting a disconnection.
      FrameworkErrorMessage message;
      message.set_message("Framework disconnected");
      framework->send(message);

      _exited(framework);
      return;
    }
  }

  if (Node* node = nodes.registered.get(pid)) {
    LOG(INFO) << "Node " << *node << " disconnected";

    if (node->connected) {
      disconnect(node);

      // The semantics when a registered node gets disconnected are as
      // follows for each framework running on that node:
      //
      // 1) If the framework is checkpointing: No immediate action is
      //    taken. The node is given a chance to reconnect until the
      //    node observer times out (75s) and removes the node.
      //
      // 2) If the framework is not-checkpointing: The node is not
      //    removed but the framework is removed from the node's
      //    structs, its tasks transitioned to LOST and resources
      //    recovered.
      hashset<FrameworkID> frameworkIds =
        node->tasks.keys() | node->executors.keys();

      foreach (const FrameworkID& frameworkId, frameworkIds) {
        Framework* framework = getFramework(frameworkId);
        CHECK_NOTNULL(framework);

        if (!framework->info.checkpoint()) {
          LOG(INFO) << "Removing framework " << *framework
                    << " from disconnected node " << *node
                    << " because the framework is not checkpointing";

          removeFramework(node, framework);
        }
      }

      // If the controller -> node socket breaks, we expect that either
      // (a) the node will fail to respond to pings and be marked
      // unreachable, or (b) the node will receive a ping, notice the
      // controller thinks it is disconnected, and then reregister. There
      // is a third possibility: if the node restarts but hangs
      // during node recovery, it will respond to pings but never
      // attempt to reregister (MESOS-6286).
      //
      // To handle this case, we expect that a node whose socket has
      // broken will reregister within `node_reregister_timeout`. If
      // the node doesn't reregister, it is marked unreachable.
      node->reregistrationTimer =
        delay(flags.node_reregister_timeout,
              self(),
              &Controller::nodeReregisterTimeout,
              node->id);
    } else {
      // NOTE: A duplicate exited() event is possible for a node
      // because its PID doesn't change on restart. See MESOS-675
      // for details.
      LOG(WARNING) << "Ignoring duplicate exited() notification for "
                   << "node " << *node;
    }
  }
}


void Controller::nodeReregisterTimeout(const NodeID& nodeId)
{
  Node* node = nodes.registered.get(nodeId);

  // The node might have been removed or reregistered concurrently
  // with the timeout expiring.
  if (node == nullptr || node->connected) {
    return;
  }

  // Remove the node in a rate limited manner, similar to how the
  // NodeObserver removes nodes.
  Future<Nothing> acquire = Nothing();

  if (nodes.limiter.isSome()) {
      LOG(INFO) << "Scheduling removal of node "
                << *node
                << "; did not reregister within "
                << flags.node_reregister_timeout << " after disconnecting";

      acquire = nodes.limiter.get()->acquire();
  }

  acquire
    .then(defer(self(), &Self::_nodeReregisterTimeout, nodeId));

  ++metrics->node_unreachable_scheduled;
}


Nothing Controller::_nodeReregisterTimeout(const NodeID& nodeId)
{
  Node* node = nodes.registered.get(nodeId);

  // The node might have been removed or reregistered while we were
  // waiting to acquire the rate limit.
  if (node == nullptr || node->connected) {
    ++metrics->node_unreachable_canceled;
    return Nothing();
  }

  ++metrics->node_unreachable_completed;

  markUnreachable(
      node->info,
      false,
      "node did not reregister within " +
      stringify(flags.node_reregister_timeout) +
      " after disconnecting");

  return Nothing();
}


void Controller::_exited(Framework* framework)
{
  LOG(INFO) << "Framework " << *framework << " disconnected";

  // Disconnect the framework.
  if (framework->connected()) {
    disconnect(framework);
  }

  // We can assume framework's failover_timeout is valid
  // because it has been validated in framework subscription.
  Try<Duration> failoverTimeout_ =
    Duration::create(framework->info.failover_timeout());

  CHECK_SOME(failoverTimeout_);
  Duration failoverTimeout = failoverTimeout_.get();

  LOG(INFO) << "Giving framework " << *framework << " "
            << failoverTimeout << " to failover";

  // Delay dispatching a message to ourselves for the timeout.
  delay(failoverTimeout,
        self(),
        &Controller::frameworkFailoverTimeout,
        framework->id(),
        framework->reregisteredTime);
}


void Controller::consume(MessageEvent&& event)
{
  // There are three cases about the message's UPID with respect to
  // 'frameworks.principals':
  // 1) if a <UPID, principal> pair exists and the principal is Some,
  //    it's a framework with its principal specified.
  // 2) if a <UPID, principal> pair exists and the principal is None,
  //    it's a framework without a principal.
  // 3) if a <UPID, principal> pair does not exist in the map, it's
  //    either an unregistered framework or not a framework.
  // The logic for framework message counters and rate limiting
  // mainly concerns with whether the UPID is a *registered*
  // framework and whether the framework has a principal so we use
  // these two temp variables to simplify the condition checks below.
  bool isRegisteredFramework =
    frameworks.principals.contains(event.message.from);
  const Option<string> principal = isRegisteredFramework
    ? frameworks.principals[event.message.from]
    : Option<string>::none();

  // Increment the "message_received" counter if the message is from
  // a framework and such a counter is configured for it.
  // See comments for 'Controller::Metrics::Frameworks' and
  // 'Controller::Frameworks::principals' for details.
  if (principal.isSome()) {
    // If the framework has a principal, the counter must exist.
    CHECK(metrics->frameworks.contains(principal.get()));
    Counter messages_received =
      metrics->frameworks.at(principal.get())->messages_received;
    ++messages_received;
  }

  // All messages are filtered when non-leading.
  if (!elected()) {
    VLOG(1) << "Dropping '" << event.message.name << "' message since "
            << "not elected yet";

    ++metrics->dropped_messages;
    return;
  }

  CHECK_SOME(recovered);

  // All messages are filtered while recovering.
  // TODO(bmahler): Consider instead re-enqueing *all* messages
  // through recover(). What are the performance implications of
  // the additional queueing delay and the accumulated backlog
  // of messages post-recovery?
  if (!recovered->isReady()) {
    VLOG(1) << "Dropping '" << event.message.name << "' message since "
            << "not recovered yet";

    ++metrics->dropped_messages;
    return;
  }

  // Throttle the message if it's a framework message and a
  // RateLimiter is configured for the framework's principal.
  // The framework is throttled by the default RateLimiter if:
  // 1) the default RateLimiter is configured (and)
  // 2) the framework doesn't have a principal or its principal is
  //    not specified in 'flags.rate_limits'.
  // The framework is not throttled if:
  // 1) the default RateLimiter is not configured to handle case 2)
  //    above. (or)
  // 2) the principal exists in RateLimits but 'qps' is not set.
  if (principal.isSome() &&
      frameworks.limiters.contains(principal.get()) &&
      frameworks.limiters[principal.get()].isSome()) {
    const Owned<BoundedRateLimiter>& limiter =
      frameworks.limiters[principal.get()].get();

    if (limiter->capacity.isNone() ||
        limiter->messages < limiter->capacity.get()) {
      limiter->messages++;
      limiter->limiter->acquire()
        .onReady(defer(self(), &Self::throttled, std::move(event), principal));
    } else {
      exceededCapacity(
          event,
          principal,
          limiter->capacity.get());
    }
  } else if ((principal.isNone() ||
              !frameworks.limiters.contains(principal.get())) &&
             isRegisteredFramework &&
             frameworks.defaultLimiter.isSome()) {
    if (frameworks.defaultLimiter.get()->capacity.isNone() ||
        frameworks.defaultLimiter.get()->messages <
          frameworks.defaultLimiter.get()->capacity.get()) {
      frameworks.defaultLimiter.get()->messages++;
      frameworks.defaultLimiter.get()->limiter->acquire()
        .onReady(defer(self(), &Self::throttled, std::move(event), None()));
    } else {
      exceededCapacity(
          event,
          principal,
          frameworks.defaultLimiter.get()->capacity.get());
    }
  } else {
    _consume(std::move(event));
  }
}


void Controller::consume(ExitedEvent&& event)
{
  // See comments in 'consume(MessageEvent&& event)' for which
  // RateLimiter is used to throttle this UPID and when it is not
  // throttled.
  // Note that throttling ExitedEvent is necessary so the order
  // between MessageEvents and ExitedEvents from the same PID is
  // maintained. Also ExitedEvents are not subject to the capacity.
  bool isRegisteredFramework = frameworks.principals.contains(event.pid);
  const Option<string> principal = isRegisteredFramework
    ? frameworks.principals[event.pid]
    : Option<string>::none();

  // Necessary to disambiguate below.
  typedef void(Self::*F)(ExitedEvent&&);

  if (principal.isSome() &&
      frameworks.limiters.contains(principal.get()) &&
      frameworks.limiters[principal.get()].isSome()) {
    frameworks.limiters[principal.get()].get()->limiter->acquire().onReady(
        defer(self(), static_cast<F>(&Self::_consume), std::move(event)));
  } else if ((principal.isNone() ||
              !frameworks.limiters.contains(principal.get())) &&
             isRegisteredFramework &&
             frameworks.defaultLimiter.isSome()) {
    frameworks.defaultLimiter.get()->limiter->acquire().onReady(
        defer(self(), static_cast<F>(&Self::_consume), std::move(event)));
  } else {
    _consume(std::move(event));
  }
}


// TODO(greggomann): Change this to accept an `Option<Principal>`
// when MESOS-7202 is resolved.
void Controller::throttled(
    MessageEvent&& event,
    const Option<string>& principal)
{
  // We already know a RateLimiter is used to throttle this event so
  // here we only need to determine which.
  if (principal.isSome()) {
    CHECK_SOME(frameworks.limiters[principal.get()]);
    frameworks.limiters[principal.get()].get()->messages--;
  } else {
    CHECK_SOME(frameworks.defaultLimiter);
    frameworks.defaultLimiter.get()->messages--;
  }

  _consume(std::move(event));
}


void Controller::_consume(MessageEvent&& event)
{
  // Obtain the principal before processing the Message because the
  // mapping may be deleted in handling 'UnregisterFrameworkMessage'
  // but its counter still needs to be incremented for this message.
  const Option<string> principal =
    frameworks.principals.contains(event.message.from)
      ? frameworks.principals[event.message.from]
      : Option<string>::none();

  ProtobufProcess<Controller>::consume(std::move(event));

  // Increment 'messages_processed' counter if it still exists.
  // Note that it could be removed in handling
  // 'UnregisterFrameworkMessage' if it's the last framework with
  // this principal.
  if (principal.isSome() && metrics->frameworks.contains(principal.get())) {
    Counter messages_processed =
      metrics->frameworks.at(principal.get())->messages_processed;
    ++messages_processed;
  }
}


// TODO(greggomann): Change this to accept an `Option<Principal>`
// when MESOS-7202 is resolved.
void Controller::exceededCapacity(
    const MessageEvent& event,
    const Option<string>& principal,
    uint64_t capacity)
{
  LOG(WARNING) << "Dropping message " << event.message.name << " from "
               << event.message.from
               << (principal.isSome() ? "(" + principal.get() + ")" : "")
               << ": capacity(" << capacity << ") exceeded";

  // Send an error to the framework which will abort the scheduler
  // driver.
  // NOTE: The scheduler driver will send back a
  // DeactivateFrameworkMessage which may be dropped as well but this
  // should be fine because the scheduler is already informed of an
  // unrecoverable error and should take action to recover.
  FrameworkErrorMessage message;
  message.set_message(
      "Message " + event.message.name +
      " dropped: capacity(" + stringify(capacity) + ") exceeded");
  send(event.message.from, message);
}


void Controller::_consume(ExitedEvent&& event)
{
  Process<Controller>::consume(std::move(event));
}


void fail(const string& message, const string& failure)
{
  LOG(FATAL) << message << ": " << failure;
}


Future<Nothing> Controller::recover()
{
  if (!elected()) {
    return Failure("Not elected as leading controller");
  }

  if (recovered.isNone()) {
    LOG(INFO) << "Recovering from registrar";

    recovered = registrar->recover(info_)
      .then(defer(self(), &Self::_recover, lambda::_1));
  }

  return recovered.get();
}


Future<Nothing> Controller::_recover(const Registry& registry)
{
  hashset<string> missingCapabilities =
    missingMinimumCapabilities(info_, registry);

  if (!missingCapabilities.empty()) {
    LOG(ERROR) << "Controller is missing the following minimum capabilities: "
               << strings::join<hashset<string>>(", ", missingCapabilities)
               << ". See the following documentation for steps to safely "
               << "recover from this state: "
               << "http://mesos.apache.org/documentation/latest/downgrades";
    EXIT(EXIT_FAILURE);
  }

  foreach (const Registry::Node& node, registry.nodes().nodes()) {
    NodeInfo nodeInfo = node.info();

    // We store the `NodeInfo`'s resources in the `pre-reservation-refinement`
    // in order to support downgrades. We convert them back to `post-` format
    // here so that we can keep our invariant of working with `post-` format
    // resources within controller memory.
    upgradeResources(&nodeInfo);

    nodes.recovered.put(nodeInfo.id(), nodeInfo);

    // Recover the draining and deactivation states.
    if (node.has_drain_info()) {
      nodes.draining[nodeInfo.id()] = node.drain_info();
    }

    if (node.has_deactivated() && node.deactivated()) {
      nodes.deactivated.insert(nodeInfo.id());
    }
  }

  foreach (const Registry::UnreachableNode& unreachable,
           registry.unreachable().nodes()) {
    if (nodes.recovered.contains(unreachable.id())) {
      LOG(WARNING) << "Ignoring unreachable registry entry for node "
                   << unreachable.id()
                   << " because it is already present in the admitted nodes list";
      continue;
    }

    CHECK(!nodes.unreachable.contains(unreachable.id()));
    nodes.unreachable[unreachable.id()] = unreachable.timestamp();

    // Recover the draining and deactivation states.
    if (unreachable.has_drain_info()) {
      nodes.draining[unreachable.id()] = unreachable.drain_info();
    }

    if (unreachable.has_deactivated() && unreachable.deactivated()) {
      nodes.deactivated.insert(unreachable.id());
    }
  }

  foreach (const Registry::GoneNode& gone,
           registry.gone().nodes()) {
    nodes.gone[gone.id()] = gone.timestamp();
  }

  // Set up a timer for age-based registry GC.
  scheduleRegistryGc();

  // Set up a timeout for nodes to reregister.
  nodes.recoveredTimer =
    delay(flags.node_reregister_timeout,
          self(),
          &Self::recoveredNodesTimeout,
          registry);

  // Save the maintenance schedule.
  foreach (const medios::maintenance::Schedule& schedule, registry.schedules()) {
    maintenance.schedules.push_back(schedule);
  }

  // Save the machine info for each machine.
  foreach (const Registry::Machine& machine, registry.machines().machines()) {
    machines[machine.info().id()] = Machine(machine.info());
  }

  // Save the quotas for each role.

  // First recover from the legacy quota entries.
  foreach (const Registry::Quota& quota, registry.quotas()) {
    quotas[quota.info().role()] = Quota{quota.info()};
  }

  // Then the new ones.
  foreach (const quota::QuotaConfig& config, registry.quota_configs()) {
    CHECK_NOT_CONTAINS(quotas, config.role());
    quotas[config.role()] = Quota{config};
  }

  // We notify the allocator via the `recover()` call. This has to be
  // done before the first node reregisters and makes its resources
  // available for allocation. This is necessary because at this point
  // the allocator is already initialized and ready to perform
  // allocations. An allocator may decide to hold off with allocation
  // until after it restores a view of the cluster state.
  int expectedNodeCount = registry.nodes().nodes().size();

  allocator->recover(expectedNodeCount, quotas);

  // TODO(alexr): Consider adding a sanity check: whether quotas are
  // satisfiable given all recovering nodes reregister. We may want
  // to notify operators early if total quota cannot be met.

  // Recover weights, and update the allocator accordingly. If we
  // recovered weights from the registry, any weights specified on the
  // command-line are ignored. If no weights were recovered from the
  // registry, any weights specified on the command-line are used and
  // then stored in the registry.
  vector<WeightInfo> weightInfos;

  if (registry.weights_size() != 0) {
    // TODO(Yongqiao Wang): After the Medios controller quorum is achieved,
    // operator can send an update weights request to do a batch
    // configuration for weights, so the `--weights` flag can be
    // deprecated and this check can eventually be removed.
    if (!weights.empty()) {
      LOG(WARNING) << "Ignoring --weights flag '" << flags.weights.get()
                   << "' and recovering the weights from registry";

      weights.clear();
    }

    foreach (const Registry::Weight& weight, registry.weights()) {
      WeightInfo weightInfo;
      weightInfo.set_role(weight.info().role());
      weightInfo.set_weight(weight.info().weight());
      weightInfos.push_back(weightInfo);

      weights[weight.info().role()] = weight.info().weight();
    }
  } else if (!weights.empty()) {
    foreachpair (const string& role, double weight, weights) {
      WeightInfo weightInfo;
      weightInfo.set_role(role);
      weightInfo.set_weight(weight);
      weightInfos.push_back(weightInfo);
    }
    registrar->apply(Owned<RegistryOperation>(
        new weights::UpdateWeights(weightInfos)));
  }

  allocator->updateWeights(weightInfos);

  // Recovery is now complete!
  LOG(INFO) << "Recovered " << registry.nodes().nodes().size() << " nodes"
            << " from the registry (" << Bytes(registry.ByteSize()) << ")"
            << "; allowing " << flags.node_reregister_timeout
            << " for nodes to reregister";

  return Nothing();
}


void Controller::scheduleRegistryGc()
{
  registryGcTimer = delay(flags.registry_gc_interval,
                          self(),
                          &Self::doRegistryGc);
}


void Controller::doRegistryGc()
{
  // Schedule next periodic GC.
  scheduleRegistryGc();

  // Determine which unreachable nodes to GC from the registry, if
  // any. We do this by examining the controller's in-memory copy of the
  // unreachable list and checking two criteria, "age" and "count". To
  // check the "count" criteria, we remove elements from the beginning
  // of the list until it contains at most "registry_max_node_count"
  // elements (note that `nodes.unreachable` is a `LinkedHashMap`,
  // which provides iteration over keys in insertion-order). To check
  // the "age" criteria, we remove any element in the list whose age
  // is more than "registry_max_node_age". Note that for the latter,
  // we check the entire list, not just the beginning: this avoids
  // requiring that the list be kept sorted by timestamp.
  //
  // We build a candidate list of NodeIDs to remove. We then try to
  // remove this list from the registry. Note that all the nodeIDs we
  // want to remove might not be found in the registrar's copy of the
  // unreachable list; this can occur if there is a concurrent write
  // (e.g., an unreachable node we want to GC reregisters
  // concurrently). In this situation, we skip removing any elements
  // we don't find.

  auto prune = [this](const LinkedHashMap<NodeID, TimeInfo>& nodes) {
    size_t count = nodes.size();
    TimeInfo currentTime = protobuf::getCurrentTime();
    hashset<NodeID> toRemove;

    foreachpair (const NodeID& nodeId,
                 const TimeInfo& removalTime,
                 nodes) {
      // Count-based GC.
      CHECK(toRemove.size() <= count);

      size_t liveCount = count - toRemove.size();
      if (liveCount > flags.registry_max_node_count) {
        toRemove.insert(nodeId);
        continue;
      }

      // Age-based GC.
      Duration age = Nanoseconds(
          currentTime.nanoseconds() - removalTime.nanoseconds());

      if (age > flags.registry_max_node_age) {
        toRemove.insert(nodeId);
      }
    }

    return toRemove;
  };

  hashset<NodeID> toRemoveUnreachable = prune(nodes.unreachable);
  hashset<NodeID> toRemoveGone = prune(nodes.gone);

  if (toRemoveUnreachable.empty() && toRemoveGone.empty()) {
    VLOG(1) << "Skipping periodic registry garbage collection: "
            << "no nodes qualify for removal";

    return;
  }

  VLOG(1) << "Attempting to remove " << toRemoveUnreachable.size()
          << " unreachable and " << toRemoveGone.size()
          << " gone nodes from the registry";

  registrar->apply(Owned<RegistryOperation>(
      new Prune(toRemoveUnreachable, toRemoveGone)))
    .onAny(defer(self(),
                 &Self::_doRegistryGc,
                 toRemoveUnreachable,
                 toRemoveGone,
                 lambda::_1));
}


void Controller::_doRegistryGc(
    const hashset<NodeID>& toRemoveUnreachable,
    const hashset<NodeID>& toRemoveGone,
    const Future<bool>& registrarResult)
{
  CHECK(!registrarResult.isDiscarded());
  CHECK(!registrarResult.isFailed());

  // `Prune` registry operation should never fail.
  CHECK(registrarResult.get());

  // Update in-memory state to be consistent with registry changes. If
  // there was a concurrent registry operation that also modified the
  // unreachable/gone list (e.g., a node in `toRemoveXXX` concurrently
  // reregistered), entries in `toRemove` might not appear in
  // `nodes.unreachable` or `nodes.gone`.
  //
  // TODO(neilc): It would be nice to verify that the effect of these
  // in-memory updates is equivalent to the changes made by the registry
  // operation, but there isn't an easy way to do that.

  size_t numRemovedUnreachable = 0;
  foreach (const NodeID& nodeId, toRemoveUnreachable) {
    if (!nodes.unreachable.contains(nodeId)) {
      LOG(WARNING) << "Failed to garbage collect " << nodeId
                   << " from the unreachable list";

      continue;
    }

    nodes.unreachable.erase(nodeId);

    // TODO(vinod): Consider moving these tasks into `completedTasks` by
    // transitioning them to a terminal state and sending status updates.
    // But it's not clear what this state should be. If a framework
    // reconciles these tasks after this point it would get `TASK_UNKNOWN`
    // which seems appropriate but we don't keep tasks in this state in-memory.
    if (nodes.unreachableTasks.contains(nodeId)) {
      foreachkey (const FrameworkID& frameworkId,
                  nodes.unreachableTasks.at(nodeId)) {
        Framework* framework = getFramework(frameworkId);
        if (framework != nullptr) {
          foreach (const TaskID& taskId,
                   nodes.unreachableTasks.at(nodeId).at(frameworkId)) {
            framework->unreachableTasks.erase(taskId);
          }
        }
      }
    }

    nodes.unreachableTasks.erase(nodeId);

    numRemovedUnreachable++;
  }

  size_t numRemovedGone = 0;
  foreach (const NodeID& nodeId, toRemoveGone) {
    if (!nodes.gone.contains(nodeId)) {
      LOG(WARNING) << "Failed to garbage collect " << nodeId
                   << " from the gone list";

      continue;
    }

    nodes.gone.erase(nodeId);
    numRemovedGone++;
  }

  // TODO(neilc): Add a metric for # of nodes discarded from the registry?
  LOG(INFO) << "Garbage collected " << numRemovedUnreachable
            << " unreachable and " << numRemovedGone
            << " gone nodes from the registry";
}


void Controller::recoveredNodesTimeout(const Registry& registry)
{
  CHECK(elected());

  // TODO(bmahler): Add a 'Percentage' abstraction.
  Try<double> limit_ = numify<double>(
      strings::remove(
          flags.recovery_node_removal_limit,
          "%",
          strings::SUFFIX));

  CHECK_SOME(limit_);

  double limit = limit_.get() / 100.0;

  // Compute the percentage of nodes to be removed, if it exceeds the
  // safety-net limit, bail!
  double removalPercentage =
    (1.0 * nodes.recovered.size()) /
    (1.0 * registry.nodes().nodes().size());

  if (removalPercentage > limit) {
    EXIT(EXIT_FAILURE)
      << "Post-recovery node removal limit exceeded! After "
      << flags.node_reregister_timeout
      << " there were " << nodes.recovered.size()
      << " (" << removalPercentage * 100 << "%) nodes recovered from the"
      << " registry that did not reregister: \n"
      << stringify(nodes.recovered.keys()) << "\n "
      << " The configured removal limit is " << limit * 100 << "%. Please"
      << " investigate or increase this limit to proceed further";
  }

  // Remove the nodes in a rate limited manner, similar to how the
  // NodeObserver removes nodes.
  foreach (const Registry::Node& node, registry.nodes().nodes()) {
    // The node is removed from `recovered` when it completes the
    // re-registration process. If the node is in `reregistering`, it
    // has started but not yet finished reregistering. In either
    // case, we don't want to try to remove it.
    if (!nodes.recovered.contains(node.info().id()) ||
        nodes.reregistering.contains(node.info().id())) {
      continue;
    }

    Future<Nothing> acquire = Nothing();

    if (nodes.limiter.isSome()) {
      LOG(INFO) << "Scheduling removal of node "
                << node.info().id() << " (" << node.info().hostname() << ")"
                << "; did not reregister within "
                << flags.node_reregister_timeout << " after controller failover";

      acquire = nodes.limiter.get()->acquire();
    }

    const string failure = "Node removal rate limit acquisition failed";

    // TODO(bmahler): Cancelation currently occurs within by returning
    // early from `markUnreachable` *without* the "discarder" having
    // discarded the rate limit token. This approach means that if
    // nodes reregister while many of the marking unreachable
    // operations are in progress, the rate that we mark unreachable
    // will "slow down" rather than stay constant. We should instead
    // discard the rate limit token when the node reregisters and
    // handle the discard here. See MESOS-8386.
    acquire
      .onFailed(lambda::bind(fail, failure, lambda::_1))
      .onDiscarded(lambda::bind(fail, failure, "discarded"))
      .then(defer(self(),
                  &Self::markUnreachable,
                  node.info(),
                  true,
                  "did not reregister within"
                  " " + stringify(flags.node_reregister_timeout) +
                  " after controller failover"))
      .then(defer(self(), [=](bool marked) {
        if (marked) {
          ++metrics->node_unreachable_completed;
        } else {
          ++metrics->node_unreachable_canceled;
        }

        return Nothing();
      }));

    ++metrics->node_unreachable_scheduled;
  }
}


void Controller::sendNodeLost(const NodeInfo& nodeInfo)
{
  foreachvalue (Framework* framework, frameworks.registered) {
    if (!framework->connected()) {
      continue;
    }

    LOG(INFO) << "Notifying framework " << *framework << " of lost node "
              << nodeInfo.id() << " (" << nodeInfo.hostname() << ")";

    LostNodeMessage message;
    message.mutable_node_id()->MergeFrom(nodeInfo.id());
    framework->send(message);
  }

  if (HookManager::hooksAvailable()) {
    HookManager::controllerNodeLostHook(nodeInfo);
  }
}


void Controller::fileAttached(const Future<Nothing>& result, const string& path)
{
  if (result.isReady()) {
    LOG(INFO) << "Successfully attached file '" << path << "'";
  } else {
    LOG(ERROR) << "Failed to attach file '" << path << "': "
               << (result.isFailed() ? result.failure() : "discarded");
  }
}


void Controller::submitScheduler(const string& name)
{
  LOG(INFO) << "Scheduler submit request for " << name;
  SubmitSchedulerResponse response;
  response.set_okay(false);
  reply(response);
}


void Controller::contend()
{
  contender->contend()
    .onAny(defer(self(), &Controller::contended, lambda::_1));
}


void Controller::contended(const Future<Future<Nothing>>& candidacy)
{
  CHECK(!candidacy.isDiscarded());

  if (candidacy.isFailed()) {
    // A failed contend means the election store was unreachable. This
    // controller is not leading (a leader that loses its candidacy exits
    // in `lostCandidacy`), so it holds no leadership to give up and
    // contends again rather than exiting. It rejoins the election as soon
    // as the store is back, with no supervisor restart.
    LOG(WARNING) << "Failed to contend: " << candidacy.failure()
                 << "; contending again in " << CONTEND_RETRY_INTERVAL;
    delay(CONTEND_RETRY_INTERVAL, self(), &Controller::contend);
    return;
  }

  // Watch for candidacy change.
  candidacy
    ->onAny(defer(self(), &Controller::lostCandidacy, lambda::_1));
}


void Controller::lostCandidacy(const Future<Nothing>& lost)
{
  CHECK(!lost.isDiscarded());

  if (lost.isFailed()) {
    EXIT(EXIT_FAILURE) << "Failed to watch for candidacy: " << lost.failure();
  }

  if (elected()) {
    EXIT(EXIT_FAILURE) << "Lost candidacy as a leader... committing suicide!";
  }

  LOG(INFO) << "Lost candidacy as a follower... Contend again";
  contend();
}


void Controller::detected(const Future<Option<ControllerInfo>>& _leader)
{
  CHECK(!_leader.isDiscarded());

  if (_leader.isFailed()) {
    EXIT(EXIT_FAILURE)
      << "Failed to detect the leading controller: " << _leader.failure()
      << "; committing suicide!";
  }

  bool wasElected = elected();
  leader = _leader.get();

  if (elected()) {
    electedTime = Clock::now();

    if (!wasElected) {
      LOG(INFO) << "Elected as the leading controller!";

      // Begin the recovery process, bail if it fails or is discarded.
      recover()
        .onFailed(lambda::bind(fail, "Recovery failed", lambda::_1))
        .onDiscarded(lambda::bind(fail, "Recovery failed", "discarded"));
    } else {
      // This happens if there is a ZK blip that causes a re-election
      // but the same leading controller is elected as leader.
      LOG(INFO) << "Re-elected as the leading controller";
    }
  } else if (leader.isSome()) {
    // A different node has been elected as the leading controller.
    LOG(INFO) << "The newly elected leader is " << leader->pid()
              << " with id " << leader->id();

    if (wasElected) {
      EXIT(EXIT_FAILURE) << "Conceded leadership to another controller..."
                         << " committing suicide!";
    }

    // If this controller and the current leader both have a configured
    // domain and the current leader is located in a different region,
    // exit with an error message: this indicates a configuration
    // error, since all controllers must be in the same region.
    if (leader->has_domain() && info_.has_domain()) {
      const DomainInfo& leaderDomain = leader->domain();
      const DomainInfo& selfDomain = info_.domain();

      // We currently reject configured domains without fault domains,
      // but that might change in the future. For compatibility with
      // future versions of Medios, we treat a controller with a configured
      // domain but no fault domain as equivalent to a controller with no
      // configured domain.
      if (leaderDomain.has_fault_domain() && selfDomain.has_fault_domain()) {
        const DomainInfo::FaultDomain::RegionInfo& leaderRegion =
          leaderDomain.fault_domain().region();
        const DomainInfo::FaultDomain::RegionInfo& selfRegion =
          selfDomain.fault_domain().region();

        if (leaderRegion != selfRegion) {
          EXIT(EXIT_FAILURE) << "Leading controller uses domain "
                             << leaderDomain << "; this controller is "
                             << "configured to use domain "
                             << selfDomain << "; all controllers in the "
                             << "same cluster must use the same region";
        }
      }
    }
  } else {
    // If an election occured and no leader was elected, `None` is returned.
    LOG(INFO) << "No controller was elected.";

    if (wasElected) {
      EXIT(EXIT_FAILURE) << "Lost leadership after indecisive election..."
                         << " committing suicide!";
    }
  }

  // Keep detecting.
  detector->detect(leader)
    .onAny(defer(self(), &Controller::detected, lambda::_1));
}


Option<Error> Controller::validateFrameworkAuthentication(
    const FrameworkInfo& frameworkInfo,
    const UPID& from)
{
  if (authenticating.contains(from)) {
    return Error("Re-authentication in progress");
  }

  if (flags.authenticate_frameworks && !authenticated.contains(from)) {
    // This could happen if another authentication request came
    // through before we are here or if a framework tried to
    // (re-)register without authentication.
    return Error("Framework at " + stringify(from) + " is not authenticated");
  }

  // TODO(bmahler): Currently the scheduler driver does not
  // set 'principal', so we allow frameworks to omit it.
  if (frameworkInfo.has_principal() &&
      !frameworkInfo.principal().empty() &&
      authenticated.contains(from) &&
      frameworkInfo.principal() != authenticated[from]) {
    return Error("Framework principal '" + frameworkInfo.principal() + "'"
                 " does not match authenticated principal"
                 " '" + authenticated[from]  + "'");
  }

  return None();
}


void Controller::drop(
    const UPID& from,
    const scheduler::Call& call,
    const string& message)
{
  // TODO(bmahler): Increment a metric.

  LOG(WARNING) << "Dropping " << call.type() << " call"
               << " from framework " << call.framework_id()
               << " at " << from << ": " << message;
}


void Controller::drop(
    Framework* framework,
    const Offer::Operation& operation,
    const string& message)
{
  CHECK_NOTNULL(framework);

  LOG(WARNING) << "Dropping " << Offer::Operation::Type_Name(operation.type())
               << " operation from framework " << *framework
               << ": " << message;

  // NOTE: Despite the suggestive name of this method, it is called
  // as part of validation so the correct updated state is `OPERATION_ERROR`,
  // not `OPERATION_DROPPED`.
  metrics->incrementOperationState(operation.type(), OPERATION_ERROR);

  // NOTE: The operation validation code should be refactored. Due to the order
  // of validation, it's possible that this function will be called before the
  // controller validates that operations from v0 frameworks should not have their
  // ID set.
  if (operation.has_id() && framework->http().isSome()) {
    scheduler::Event update;
    update.set_type(scheduler::Event::UPDATE_OPERATION_STATUS);

    // NOTE: We do not attempt to set the node or resource provider IDs for
    // dropped operations as we cannot guarantee to always know their values.
    //
    // TODO(bbannier): Set node or resource provider ID if we know
    // for certain that the operation was valid.
    *update.mutable_update_operation_status()->mutable_status() =
      protobuf::createOperationStatus(
          OperationState::OPERATION_ERROR,
          operation.id(),
          message);

    framework->send(update);
  }
}


void Controller::drop(
    Framework* framework,
    const scheduler::Call& call,
    const string& message)
{
    CHECK_NOTNULL(framework);

    // TODO(gyliu513): Increment a metric.

    LOG(WARNING) << "Dropping " << call.type() << " call"
                 << " from framework " << *framework
                 << ": " << message;
}


void Controller::drop(
    Framework* framework,
    const scheduler::Call::Suppress& suppress,
    const string& message)
{
  scheduler::Call call;
  call.set_type(scheduler::Call::SUPPRESS);
  call.mutable_suppress()->CopyFrom(suppress);

  drop(framework, call, message);
}


void Controller::drop(
    Framework* framework,
    const scheduler::Call::Revive& revive,
    const string& message)
{
  scheduler::Call call;
  call.set_type(scheduler::Call::REVIVE);
  call.mutable_revive()->CopyFrom(revive);

  drop(framework, call, message);
}


void Controller::receive(
    const UPID& from,
    scheduler::Call&& call)
{
  // TODO(vinod): Add metrics for calls.

  Option<Error> error = validation::scheduler::call::validate(call);

  if (error.isSome()) {
    metrics->incrementInvalidSchedulerCalls(call);
    drop(from, call, error->message);
    return;
  }

  if (call.type() == scheduler::Call::SUBSCRIBE) {
    subscribe(from, std::move(*call.mutable_subscribe()));
    return;
  }

  // We consolidate the framework lookup and pid validation logic here
  // because they are common for all the call handlers.
  Framework* framework = getFramework(call.framework_id());

  if (framework == nullptr) {
    drop(from, call, "Framework cannot be found");
    return;
  }

  if (framework->pid() != from) {
    drop(from, call, "Call is not from registered framework");
    return;
  }

  framework->metrics.incrementCall(call.type());

  // This is possible when controller --> framework link is broken (i.e., one
  // way network partition) and the framework is not aware of it. There
  // is no way for driver based frameworks to detect this in the absence
  // of periodic heartbeat events. We send an error message to the framework
  // causing the scheduler driver to abort when this happens.
  if (!framework->connected()) {
    const string error = "Framework disconnected";

    LOG(INFO) << "Refusing " << call.type() << " call from framework "
              << *framework << ": " << error;

    FrameworkErrorMessage message;
    message.set_message(error);
    send(from, message);
    return;
  }

  switch (call.type()) {
    case scheduler::Call::SUBSCRIBE:
      // SUBSCRIBE call should have been handled above.
      LOG(FATAL) << "Unexpected 'SUBSCRIBE' call";

    case scheduler::Call::TEARDOWN:
      teardown(framework);
      break;

    case scheduler::Call::ACCEPT:
      accept(framework, std::move(*call.mutable_accept()));
      break;

    case scheduler::Call::DECLINE:
      decline(framework, std::move(*call.mutable_decline()));
      break;

    case scheduler::Call::ACCEPT_INVERSE_OFFERS:
      acceptInverseOffers(framework, call.accept_inverse_offers());
      break;

    case scheduler::Call::DECLINE_INVERSE_OFFERS:
      declineInverseOffers(framework, call.decline_inverse_offers());
      break;

    case scheduler::Call::REVIVE:
      revive(framework, call.revive());
      break;

    case scheduler::Call::KILL:
      kill(framework, call.kill());
      break;

    case scheduler::Call::SHUTDOWN:
      shutdown(framework, call.shutdown());
      break;

    case scheduler::Call::ACKNOWLEDGE: {
      acknowledge(framework, std::move(*call.mutable_acknowledge()));
      break;
    }

    case scheduler::Call::ACKNOWLEDGE_OPERATION_STATUS: {
      drop(
          from,
          call,
          "'ACKNOWLEDGE_OPERATION_STATUS' is not supported by the v0 API");
      break;
    }

    case scheduler::Call::RECONCILE:
      reconcile(framework, std::move(*call.mutable_reconcile()));
      break;

    case scheduler::Call::RECONCILE_OPERATIONS:
      drop(
          from,
          call,
          "'RECONCILE_OPERATIONS' is not supported by the v0 API");
      break;

    case scheduler::Call::MESSAGE:
      message(framework, std::move(*call.mutable_message()));
      break;

    case scheduler::Call::REQUEST:
      request(framework, call.request());
      break;

    case scheduler::Call::SUPPRESS:
      suppress(framework, call.suppress());
      break;

    case scheduler::Call::UPDATE_FRAMEWORK:
      updateFramework(from, std::move(*call.mutable_update_framework()));
      break;

    case scheduler::Call::UNKNOWN:
      LOG(WARNING) << "'UNKNOWN' call";
      break;
  }
}


void Controller::registerFramework(
    const UPID& from,
    RegisterFrameworkMessage&& registerFrameworkMessage)
{
  FrameworkInfo frameworkInfo =
    std::move(*registerFrameworkMessage.mutable_framework());

  if (frameworkInfo.has_id() && !frameworkInfo.id().value().empty()) {
    const string error = "Registering with 'id' already set";

    LOG(INFO) << "Refusing registration request of framework"
              << " '" << frameworkInfo.name() << "' at " << from
              << ": " << error;

    FrameworkErrorMessage message;
    message.set_message(error);
    send(from, message);
    return;
  }

  scheduler::Call::Subscribe call;
  *call.mutable_framework_info() = std::move(frameworkInfo);

  subscribe(from, std::move(call));
}


void Controller::reregisterFramework(
    const UPID& from,
    ReregisterFrameworkMessage&& reregisterFrameworkMessage)
{
  FrameworkInfo frameworkInfo =
    std::move(*reregisterFrameworkMessage.mutable_framework());

  if (!frameworkInfo.has_id() || frameworkInfo.id().value().empty()) {
    const string error = "Re-registering without an 'id'";

    LOG(INFO) << "Refusing re-registration request of framework"
              << " '" << frameworkInfo.name() << "' at " << from
              << ": " << error;

    FrameworkErrorMessage message;
    message.set_message(error);
    send(from, message);
    return;
  }

  scheduler::Call::Subscribe call;
  *call.mutable_framework_info() = std::move(frameworkInfo);
  call.set_force(reregisterFrameworkMessage.failover());

  subscribe(from, std::move(call));
}


Option<Error> Controller::validateFramework(
    const FrameworkInfo& frameworkInfo) const
{
  Option<Error> validationError =
    validation::framework::validate(frameworkInfo);

  if (validationError.isSome()) {
    return validationError;
  }

  // Check the framework's role(s) against the allowlist.
  set<string> invalidRoles;

  if (protobuf::frameworkHasCapability(
          frameworkInfo,
          FrameworkInfo::Capability::MULTI_ROLE)) {
    foreach (const string& role, frameworkInfo.roles()) {
      if (!isAllowlistedRole(role)) {
        invalidRoles.insert(role);
      }
    }
  } else {
    if (!isAllowlistedRole(frameworkInfo.role())) {
      invalidRoles.insert(frameworkInfo.role());
    }
  }

  if (!invalidRoles.empty()) {
    return Error("Roles " + stringify(invalidRoles) +
                 " are not present in the controller's --roles");
  }

  // TODO(vinod): Deprecate this in favor of authorization.
  if (frameworkInfo.user() == "root" && !flags.root_submissions) {
    return Error("User 'root' is not allowed to run frameworks"
                 " without --root_submissions set");
  }

  if (frameworkInfo.has_id() && isCompletedFramework(frameworkInfo.id())) {
    // This could happen if a framework tries to subscribe after its failover
    // timeout has elapsed, or it has been torn down via the operator API,
    // or it unregistered itself by calling 'stop()' on the scheduler driver.
    //
    // TODO(vinod): Controller should persist admitted frameworks to the
    // registry and remove them from it after failover timeout.
    return Error("Framework has been removed");
  }

  return Option<Error>::none();
}


static Try<allocator::FrameworkOptions> createAllocatorFrameworkOptions(
    const set<string>& validFrameworkRoles,
    const OfferConstraintsFilter::Options filterOptions,
    google::protobuf::RepeatedPtrField<std::string>&& suppressedRoles,
    OfferConstraints offerConstraints)
{
  set<string> suppressedRolesSet(
      make_move_iterator(suppressedRoles.begin()),
      make_move_iterator(suppressedRoles.end()));

  Option<Error> error = validation::framework::validateSuppressedRoles(
      validFrameworkRoles, suppressedRolesSet);

  if (error.isSome()) {
    return *error;
  }

  error = validation::framework::validateOfferConstraintsRoles(
      validFrameworkRoles, offerConstraints);

  if (error.isSome()) {
    return *error;
  }

  Try<OfferConstraintsFilter> filter = OfferConstraintsFilter::create(
      filterOptions, std::move(offerConstraints));

  if (filter.isError()) {
    return Error(
        "Offer constraints are not valid: " + std::move(filter.error()));
  }

  return allocator::FrameworkOptions{
    std::move(suppressedRolesSet), std::move(*filter)};
}


// Returns None if the framework object approvers are ready and the scheduler
// trying to SUBSCRIBE is authorized to do so with provided framework info.
// Otherwise, returns an error to be sent to the scheduler trying to subscribe.
static Option<Error> checkSubscribeAuthorization(
    const Future<Owned<ObjectApprovers>>& frameworkObjectApprovers,
    const FrameworkInfo& frameworkInfo)
{
  if (frameworkObjectApprovers.isFailed()) {
    return Error(
        "Authorization failure: could not create ObjectApprovers for a "
        "framework: " +
        frameworkObjectApprovers.failure());
  }

  auto actionObject = ActionObject::frameworkRegistration(frameworkInfo);

  CHECK(frameworkObjectApprovers.isReady());
  Try<bool> approved = frameworkObjectApprovers.get()->approved(
      actionObject.action(),
      actionObject.object().getOrElse(authorization::Object()));

  if (approved.isError()) {
    return Error("Authorization failure: " + approved.error());
  }

  if (!*approved) {
    return Error("Not authorized to " + stringify(actionObject));
  }

  return None();
};


void Controller::subscribe(
    StreamingHttpConnection<v1::scheduler::Event> http,
    scheduler::Call::Subscribe&& subscribe)
{
  // TODO(anand): Authenticate the framework.

  FrameworkInfo& frameworkInfo = *subscribe.mutable_framework_info();

  // Update messages_{re}register_framework accordingly.
  if (!frameworkInfo.has_id() || frameworkInfo.id() == "") {
    ++metrics->messages_register_framework;
  } else {
    ++metrics->messages_reregister_framework;
  }

  LOG(INFO) << "Received subscription request for"
            << " HTTP framework '" << frameworkInfo.name() << "'";

  auto refuseSubscription = [&](const string& error) {
    LOG(INFO) << "Refusing subscription of framework"
              << " '" << frameworkInfo.name() << "': " << error;

    FrameworkErrorMessage message;
    message.set_message(error);

    http.send(message);
    http.close();
  };

  const Option<Error> validationError = validateFramework(frameworkInfo);
  if (validationError.isSome()) {
    refuseSubscription(validationError->message);
    return;
  }

  Try<allocator::FrameworkOptions> allocatorOptions =
    createAllocatorFrameworkOptions(
        protobuf::framework::getRoles(frameworkInfo),
        offerConstraintsFilterOptions,
        std::move(*subscribe.mutable_suppressed_roles()),
        subscribe.offer_constraints());

  if (allocatorOptions.isError()) {
    refuseSubscription(allocatorOptions.error());
    return;
  }

  // Need to disambiguate for the compiler.
  void (Controller::*_subscribe)(
      StreamingHttpConnection<v1::scheduler::Event>,
      FrameworkInfo&&,
      OfferConstraints&&,
      bool,
      FrameworkOptions&&,
      const Future<Owned<ObjectApprovers>>&) = &Self::_subscribe;

  Future<Owned<ObjectApprovers>> objectApprovers =
    Framework::createObjectApprovers(authorizer, frameworkInfo);

  objectApprovers.onAny(defer(
      self(),
      _subscribe,
      http,
      std::move(frameworkInfo),
      std::move(*subscribe.mutable_offer_constraints()),
      subscribe.force(),
      std::move(*allocatorOptions),
      lambda::_1));
}


void Controller::_subscribe(
    StreamingHttpConnection<v1::scheduler::Event> http,
    FrameworkInfo&& frameworkInfo,
    OfferConstraints&& offerConstraints,
    bool force,
    ::medios::allocator::FrameworkOptions&& options,
    const Future<Owned<ObjectApprovers>>& objectApprovers)
{
  CHECK(!objectApprovers.isDiscarded());

  Option<Error> authorizationError =
    checkSubscribeAuthorization(objectApprovers, frameworkInfo);
  if (authorizationError.isSome()) {
    LOG(INFO) << "Refusing subscription of framework"
              << " '" << frameworkInfo.name() << "'"
              << ": " << authorizationError->message;

    FrameworkErrorMessage message;
    message.set_message(authorizationError->message);
    http.send(message);
    http.close();
    return;
  }

  CHECK(objectApprovers.isReady());

  LOG(INFO) << "Subscribing framework '" << frameworkInfo.name()
            << "' with checkpointing "
            << (frameworkInfo.checkpoint() ? "enabled" : "disabled")
            << " and capabilities " << frameworkInfo.capabilities();


  if (!frameworkInfo.has_id() || frameworkInfo.id() == "") {
    // If we are here the framework is subscribing for the first time.
    // Assign a new FrameworkID.
    FrameworkInfo frameworkInfo_ = frameworkInfo;
    frameworkInfo_.mutable_id()->CopyFrom(newFrameworkId());

    Framework* framework = new Framework(
        this,
        flags,
        frameworkInfo_,
        std::move(offerConstraints),
        http,
        objectApprovers.get());

    addFramework(framework, std::move(options));

    framework->metrics.incrementCall(scheduler::Call::SUBSCRIBE);

    FrameworkRegisteredMessage message;
    message.mutable_framework_id()->MergeFrom(framework->id());
    message.mutable_controller_info()->MergeFrom(info_);
    framework->send(message);

    // Start the heartbeat after sending SUBSCRIBED event.
    framework->heartbeat();

    if (!subscribers.subscribed.empty()) {
      subscribers.send(
          protobuf::controller::event::createFrameworkAdded(*framework));
    }

    return;
  }

  // If we are here the framework has already been assigned an id.
  CHECK(!frameworkInfo.id().value().empty());

  Framework* framework = getFramework(frameworkInfo.id());

  if (framework == nullptr) {
    // The framework has not yet reregistered after controller failover.
    // Furthermore, no nodes have reregistered running one of this
    // framework's tasks. Reconstruct a `Framework` object from the
    // supplied `FrameworkInfo`.
    //
    // NOTE: allocatorOptions will be fed into the allocator later in this
    // method.
    recoverFramework(frameworkInfo);

    framework = getFramework(frameworkInfo.id());
  }

  CHECK_NOTNULL(framework);

  validation::framework::preserveImmutableFields(
    framework->info, &frameworkInfo);

  // The new framework info cannot be validated against the current one
  // before authorization because the current framework info might have been
  // modified by another concurrent SUBSCRIBE call.
  // Therefore, we have to do this validation here.
  Option<Error> updateValidationError = validation::framework::validateUpdate(
      framework->info, frameworkInfo);

  if (updateValidationError.isSome()) {
    FrameworkErrorMessage message;
    message.set_message(updateValidationError->message);
    http.send(message);
    http.close();
    return;
  }

  framework->metrics.incrementCall(scheduler::Call::SUBSCRIBE);

  updateFramework(
      framework,
      frameworkInfo,
      std::move(offerConstraints),
      std::move(options));

  if (!framework->recovered()) {
    // The framework has previously been registered with this controller;
    // it may or may not currently be connected.
    framework->reregisteredTime = Clock::now();

    // Always failover the old framework connection. See MESOS-4712 for details.
    failoverFramework(framework, http, objectApprovers.get());
  } else {
    // The framework has not yet reregistered after controller failover.
    connectAndActivateRecoveredFramework(
        framework, None(), http, objectApprovers.get());
  }

  // TODO(asekretenko): Consider avoiding to broadcast `FrameworkInfo` to nodes
  // when it has not changed. Note that the API event FRAMEWORK_UPDATED needs
  // to be sent to the subscribers regardless of that, and that nodes must
  // be notified about the scheduler PID removal when this call performs
  // a V0->V1 upgrade.
  sendFrameworkUpdates(*framework);
}


void Controller::sendFrameworkUpdates(const Framework& framework)
{
  LOG(INFO) << "Sending a FRAMEWORK_UPDATED event for framework " << framework
            << " to all subscribers and broadcasting its up-to-date"
            << " FrameworkInfo and PID to all registered nodes";

  if (!subscribers.subscribed.empty()) {
    subscribers.send(
      protobuf::controller::event::createFrameworkUpdated(framework));
  }

  // Broadcast the new framework info and pid to all the nodes. We have to do
  // this upon frameworkInfo/pid updates because an executor might be running
  // on a node but it currently isn't running any tasks.
  foreachvalue (Node* node, nodes.registered) {
    UpdateFrameworkMessage message;
    message.mutable_framework_id()->CopyFrom(framework.id());

    // TODO(anand): We set 'pid' to UPID() for http frameworks
    // as 'pid' was made optional in 0.24.0. In 0.25.0, we
    // no longer have to set pid here for http frameworks.
    message.set_pid(framework.pid().getOrElse(UPID()));
    message.mutable_framework_info()->CopyFrom(framework.info);
    send(node->pid, message);
  }
}


void Controller::subscribe(
    const UPID& from,
    scheduler::Call::Subscribe&& subscribe)
{
  FrameworkInfo& frameworkInfo = *subscribe.mutable_framework_info();

  // Update messages_{re}register_framework accordingly.
  if (!frameworkInfo.has_id() || frameworkInfo.id() == "") {
    ++metrics->messages_register_framework;
  } else {
    ++metrics->messages_reregister_framework;
  }

  if (authenticating.contains(from)) {
    // TODO(vinod): Consider dropping this request and fix the tests
    // to deal with the drop. Currently there is a race between controller
    // realizing the framework is authenticated and framework sending
    // a subscribe call. Dropping this message will cause the
    // framework to retry slowing down the tests.
    LOG(INFO) << "Queuing up SUBSCRIBE call for"
              << " framework '" << frameworkInfo.name() << "' at " << from
              << " because authentication is still in progress";

    // Need to disambiguate for the compiler.
    void (Controller::*f)(const UPID&, scheduler::Call::Subscribe&&)
      = &Self::subscribe;

    authenticating[from]
      .onReady(defer(self(), f, from, std::move(subscribe)));
    return;
  }

  auto refuseSubscription = [&](const string& error) {
    LOG(INFO) << "Refusing subscription of framework"
              << " '" << frameworkInfo.name() << "' at " << from << ": "
              << error;

    FrameworkErrorMessage message;
    message.set_message(error);
    send(from, message);
  };

  // Note that re-authentication errors are already handled above.
  Option<Error> validationError =
    validateFrameworkAuthentication(frameworkInfo, from);

  if (validationError.isSome()) {
    refuseSubscription(validationError->message);
    return;
  }

  // We allow an authenticated framework to not specify a principal
  // in `FrameworkInfo` but we'd prefer to log a WARNING here. We also
  // set `FrameworkInfo.principal` to the value of authenticated principal
  // before `validateFramework` (#537) and use it for authorization later.
  if ((!frameworkInfo.has_principal() || frameworkInfo.principal().empty()) &&
      authenticated.contains(from)) {
    LOG(WARNING)
      << "Setting 'principal' in FrameworkInfo to '" << authenticated[from]
      << "' because the framework authenticated with that principal but did "
      << "not set it in FrameworkInfo";

    frameworkInfo.set_principal(authenticated[from]);
  }

  // #409: Populate `FrameworkInfo.hostname` from the peer UPID IP address when
  // omitted by the framework scheduler (avoiding synchronous reverse-DNS
  // `lookup_hostname()` on the single-threaded Controller actor).
  if ((!frameworkInfo.has_hostname() || frameworkInfo.hostname().empty()) &&
      from != UPID()) {
    frameworkInfo.set_hostname(stringify(from.address.ip));
  }

  validationError = validateFramework(frameworkInfo);

  if (validationError.isSome()) {
    refuseSubscription(validationError->message);
    return;
  }

  Try<allocator::FrameworkOptions> allocatorOptions =
    createAllocatorFrameworkOptions(
        protobuf::framework::getRoles(frameworkInfo),
        offerConstraintsFilterOptions,
        std::move(*subscribe.mutable_suppressed_roles()),
        subscribe.offer_constraints());

  if (allocatorOptions.isError()) {
    refuseSubscription(allocatorOptions.error());
    return;
  }

  LOG(INFO) << "Received SUBSCRIBE call for"
            << " framework '" << frameworkInfo.name() << "' at " << from;

  // Need to disambiguate for the compiler.
  void (Controller::*_subscribe)(
      const UPID&,
      FrameworkInfo&&,
      OfferConstraints&&,
      bool,
      ::medios::allocator::FrameworkOptions&&,
      const Future<Owned<ObjectApprovers>>&) = &Self::_subscribe;

  Future<Owned<ObjectApprovers>> objectApprovers =
    Framework::createObjectApprovers(authorizer, frameworkInfo);

  objectApprovers.onAny(defer(
      self(),
      _subscribe,
      from,
      std::move(frameworkInfo),
      std::move(*subscribe.mutable_offer_constraints()),
      subscribe.force(),
      std::move(*allocatorOptions),
      lambda::_1));
}


void Controller::_subscribe(
    const UPID& from,
    FrameworkInfo&& frameworkInfo,
    OfferConstraints&& offerConstraints,
    bool force,
    ::medios::allocator::FrameworkOptions&& options,
    const Future<Owned<ObjectApprovers>>& objectApprovers)
{
  CHECK(!objectApprovers.isDiscarded());

  Option<Error> authorizationError =
    checkSubscribeAuthorization(objectApprovers, frameworkInfo);

  if (authorizationError.isSome()) {
    LOG(INFO) << "Refusing subscription of framework"
              << " '" << frameworkInfo.name() << "' at " << from
              << ": " << authorizationError->message;

    FrameworkErrorMessage message;
    message.set_message(authorizationError->message);

    send(from, message);
    return;
  }

  CHECK(objectApprovers.isReady());

  // At this point, authentications errors will be due to
  // re-authentication during the authorization process,
  // so we drop the subscription.
  Option<Error> authenticationError =
    validateFrameworkAuthentication(frameworkInfo, from);

  if (authenticationError.isSome()) {
    LOG(INFO) << "Dropping SUBSCRIBE call for framework"
              << " '" << frameworkInfo.name() << "' at " << from
              << ": " << authenticationError->message;

    return;
  }


  LOG(INFO) << "Subscribing framework " << frameworkInfo.name()
            << " with checkpointing "
            << (frameworkInfo.checkpoint() ? "enabled" : "disabled")
            << " and capabilities " << frameworkInfo.capabilities();

  if (!frameworkInfo.has_id() || frameworkInfo.id().value().empty()) {
    // If we are here the framework is subscribing for the first time.
    // Check if this framework is already subscribed (because it retries).
    foreachvalue (Framework* framework, frameworks.registered) {
      if (framework->pid() == from) {
        LOG(INFO) << "Framework " << *framework
                  << " already subscribed, resending acknowledgement";

        FrameworkRegisteredMessage message;
        message.mutable_framework_id()->MergeFrom(framework->id());
        message.mutable_controller_info()->MergeFrom(info_);
        framework->send(message);
        return;
      }
    }

    CHECK(!frameworks.principals.contains(from));

    // Assign a new FrameworkID.
    frameworkInfo.mutable_id()->CopyFrom(newFrameworkId());

    Framework* framework = new Framework(
        this,
        flags,
        frameworkInfo,
        std::move(offerConstraints),
        from,
        objectApprovers.get());

    addFramework(framework, std::move(options));

    FrameworkRegisteredMessage message;
    message.mutable_framework_id()->MergeFrom(framework->id());
    message.mutable_controller_info()->MergeFrom(info_);
    framework->send(message);

    if (!subscribers.subscribed.empty()) {
      subscribers.send(
          protobuf::controller::event::createFrameworkAdded(*framework));
    }

    return;
  }

  // If we are here the framework has already been assigned an id.
  CHECK(!frameworkInfo.id().value().empty());

  // Check whether we got a subscribe from a framework whose UPID duplicates
  // a framework that is already connected. Note that we don't send an error
  // response because that would go to the framework that is already connected.
  if (frameworks.principals.contains(from)) {
    foreachvalue (Framework* framework, frameworks.registered) {
      if (framework->pid() == from && framework->id() != frameworkInfo.id()) {
        LOG(ERROR) << "Dropping SUBSCRIBE call for framework '"
                   << frameworkInfo.name() << "': " << *framework
                   << " already connected at " << from;

        return;
      }
    }
  }

  Framework* framework = getFramework(frameworkInfo.id());

  if (framework == nullptr) {
    // The framework has not yet reregistered after controller failover.
    // Furthermore, no nodes have reregistered running one of this
    // framework's tasks. Reconstruct a `Framework` object from the
    // supplied `FrameworkInfo`.
    recoverFramework(frameworkInfo);

    framework = getFramework(frameworkInfo.id());
  }

  CHECK_NOTNULL(framework);

  validation::framework::preserveImmutableFields(
    framework->info, &frameworkInfo);

  // The new framework info cannot be validated against the current one
  // before authorization because the current framework info might have been
  // modified by another concurrent SUBSCRIBE call.
  // Therefore, we have to do this validation here.
  Option<Error> updateValidationError = validation::framework::validateUpdate(
      framework->info, frameworkInfo);

  if (updateValidationError.isSome()) {
    FrameworkErrorMessage message;
    message.set_message(updateValidationError->message);
    send(from, message);
    return;
  }

  // Using the "force" field of the scheduler allows us to reject a
  // scheduler that got partitioned but didn't die (in ZooKeeper
  // speak this means didn't lose their session) and then
  // eventually tried to connect to this controller even though
  // another instance of their scheduler has reconnected.

  // Test that the scheduler trying to subscribe with a new PID sets `force`.
  if (!framework->recovered() && framework->pid() != from && !force) {
    LOG(ERROR) << "Disallowing subscription attempt of"
               << " framework " << *framework
               << " because it is not expected from " << from;

    FrameworkErrorMessage message;
    message.set_message("Framework failed over");
    send(from, message);
    return;
  }

  // It is now safe to update the framework fields since the request is now
  // guaranteed to be successful. We use the fields passed in during
  // re-registration.
  updateFramework(
      framework,
      frameworkInfo,
      std::move(offerConstraints),
      std::move(options));

  if (!framework->recovered()) {
    // The framework has previously been registered with this controller;
    // it may or may not currently be connected.

    framework->reregisteredTime = Clock::now();

    if (force) {
      // TODO(vinod): Now that the scheduler pid is unique we don't
      // need to call 'failoverFramework()' if the pid hasn't changed
      // (i.e., duplicate message). Instead we can just send the
      // FrameworkReregisteredMessage back and activate the framework
      // if necesssary.
      LOG(INFO) << "Framework " << *framework << " failed over";
      failoverFramework(framework, from, objectApprovers.get());
    } else {
      LOG(INFO) << "Allowing framework " << *framework
                << " to subscribe with an already used id";

      // Rescind any offers sent to this framework.
      // NOTE: We need to do this because the scheduler might have
      // replied to the offers but the driver might have dropped
      // those messages since it wasn't connected to the controller.
      foreach (Offer* offer, utils::copy(framework->offers)) {
        rescindOffer(offer);
      }

      // Also remove inverse offers.
      foreach (InverseOffer* inverseOffer,
               utils::copy(framework->inverseOffers)) {
        allocator->updateInverseOffer(
            inverseOffer->node_id(),
            inverseOffer->framework_id(),
            UnavailableResources{
                inverseOffer->resources(),
                inverseOffer->unavailability()},
            None());

        removeInverseOffer(inverseOffer, true); // Rescind.
      }

      // Relink to the framework. This might be necessary if the
      // framework link previously broke.
      link(framework->pid().get());

      framework->updateConnection(*(framework->pid()), objectApprovers.get());
      if (framework->activate()) {
        // The framework was not active and needs to be activated in allocator.
        //
        // NOTE: We do this after recovering resources (above) so that
        // the allocator has the correct view of the framework's share.
        allocator->activateFramework(framework->id());
      }

      FrameworkReregisteredMessage message;
      message.mutable_framework_id()->MergeFrom(frameworkInfo.id());
      message.mutable_controller_info()->MergeFrom(info_);
      framework->send(message);
    }
  } else {
    // The framework has not yet reregistered after controller failover.
    connectAndActivateRecoveredFramework(
        framework, from, None(), objectApprovers.get());
  }

  // TODO(asekretenko): Consider avoiding to broadcast `FrameworkInfo` and
  // the V0 framework PID to nodes when they have not changed. Note that the
  // API event FRAMEWORK_UPDATED needs to be sent regardless regardless of that.
  sendFrameworkUpdates(*framework);
}


void Controller::updateFramework(
    const process::UPID& from,
    medios::scheduler::Call::UpdateFramework&& call)
{
  FrameworkID frameworkId = call.framework_info().id();

  if ((!call.framework_info().has_principal() ||
       call.framework_info().principal().empty()) &&
      authenticated.contains(from)) {
    LOG(WARNING)
      << "Setting 'principal' in FrameworkInfo to '" << authenticated[from]
      << "' because the framework authenticated with that principal but did "
      << "not set it in FrameworkInfo";

    call.mutable_framework_info()->set_principal(authenticated[from]);
  }

  updateFramework(std::move(call))
    .onAny(defer(self(), [this, from, frameworkId](
             const Future<process::http::Response>& response) {
      if (response->code != process::http::Status::OK) {
        CHECK_EQ(response->type, process::http::Response::BODY);
        FrameworkErrorMessage message;
        message.set_message(response->body);
        send(from, message);
      }
    }));
}


Future<process::http::Response> Controller::updateFramework(
    medios::scheduler::Call::UpdateFramework&& call)
{
  Framework* const framework =
    CHECK_NOTNULL(getFramework(call.framework_info().id()));

  LOG(INFO) << "Processing UPDATE_FRAMEWORK call for framework "
            << call.framework_info().id();

  Option<Error> error = validateFramework(call.framework_info());

  if (error.isSome()) {
    return process::http::BadRequest(
        "Supplied FrameworkInfo is not valid: " + error->message);
  }

  error = validation::framework::validateUpdate(
      framework->info, call.framework_info());

  if (error.isSome()) {
    return process::http::BadRequest(
        "FrameworkInfo update is not valid: " + error->message);
  }

  const bool frameworkInfoChanged =
    !typeutils::equivalent(framework->info, call.framework_info());

  Try<allocator::FrameworkOptions> allocatorOptions =
    createAllocatorFrameworkOptions(
        protobuf::framework::getRoles(call.framework_info()),
        offerConstraintsFilterOptions,
        std::move(*call.mutable_suppressed_roles()),
        call.offer_constraints());

  if (allocatorOptions.isError()) {
    return process::http::BadRequest(
        "'UpdateFramework' call is not valid: " + allocatorOptions.error());
  }

  ActionObject actionObject =
    ActionObject::frameworkRegistration(call.framework_info());

  Try<bool> approved = framework->approved(actionObject);
  if (approved.isError()) {
    return process::http::BadRequest(
        "Authorization failure: " + approved.error());
  }

  if (!*approved) {
    // TODO(asekretenko): We should make it possible for the objectApprover to
    // return the exact cause of declined authorization, after which we can
    // simply forward it here.
    return process::http::Forbidden(
        "Not authorized to " + stringify(actionObject));
  }

  updateFramework(
      framework,
      call.framework_info(),
      std::move(*call.mutable_offer_constraints()),
      std::move(*allocatorOptions));

  if (frameworkInfoChanged) {
    // NOTE: Among the framework properties that can be changed by this call
    // (`FrameworkInfo`, suppressed roles and offer constraints),
    // only the `FrameworkInfo` change needs to be forwarded to the nodes
    // and the API subscribers. This call changes neither the V0 framework PID
    // nor the framework activeness or state (RECOVERED/CONNECTED/DISCONNECTED).
    sendFrameworkUpdates(*framework);
  }

  return process::http::OK();
}


void Controller::unregisterFramework(
    const UPID& from,
    const FrameworkID& frameworkId)
{
  LOG(INFO) << "Asked to unregister framework " << frameworkId;

  Framework* framework = getFramework(frameworkId);
  if (framework != nullptr) {
    if (framework->pid() == from) {
      teardown(framework);
    } else {
      LOG(WARNING)
        << "Ignoring unregister framework message for framework " << *framework
        << " because it is not expected from " << from;
    }
  }
}


void Controller::deactivateFramework(
    const UPID& from,
    const FrameworkID& frameworkId)
{
  ++metrics->messages_deactivate_framework;

  Framework* framework = getFramework(frameworkId);

  if (framework == nullptr) {
    LOG(WARNING)
      << "Ignoring deactivate framework message for framework " << frameworkId
      << " because the framework cannot be found";

    return;
  }

  if (framework->pid() != from) {
    LOG(WARNING)
      << "Ignoring deactivate framework message for framework " << *framework
      << " because it is not expected from " << from;

    return;
  }

  if (!framework->connected()) {
    LOG(INFO)
      << "Ignoring deactivate framework message for framework" << *framework
      << " because it is disconnected";

    return;
  }

  if (framework->active()) {
    deactivate(framework, true);
  }
}


void Controller::disconnect(Framework* framework)
{
  CHECK_NOTNULL(framework);
  CHECK(framework->connected());

  if (framework->active()) {
    deactivate(framework, true);
  }

  LOG(INFO) << "Disconnecting framework " << *framework;

  if (framework->pid().isSome()) {
    // Remove the framework from authenticated. This is safe because
    // a framework will always reauthenticate before (re-)registering.
    authenticated.erase(framework->pid().get());
  }

  CHECK(framework->disconnect());
}


void Controller::deactivate(Framework* framework, bool rescind)
{
  CHECK_NOTNULL(framework);

  LOG(INFO) << "Deactivating framework " << *framework;

  CHECK(framework->deactivate());

  // Tell the allocator to stop allocating resources to this framework.
  allocator->deactivateFramework(framework->id());

  // Remove the framework's offers.
  foreach (Offer* offer, utils::copy(framework->offers)) {
    if (rescind) {
      rescindOffer(offer);
    } else {
      discardOffer(offer);
    }
  }

  // Remove the framework's inverse offers.
  foreach (InverseOffer* inverseOffer, utils::copy(framework->inverseOffers)) {
    allocator->updateInverseOffer(
        inverseOffer->node_id(),
        inverseOffer->framework_id(),
        UnavailableResources{
            inverseOffer->resources(),
            inverseOffer->unavailability()},
        None());

    removeInverseOffer(inverseOffer, rescind);
  }
}


void Controller::disconnect(Node* node)
{
  CHECK_NOTNULL(node);

  LOG(INFO) << "Disconnecting node " << *node;

  node->connected = false;

  // Inform the node observer.
  dispatch(node->observer, &NodeObserver::disconnect);

  // Remove the node from authenticated. This is safe because
  // a node will always reauthenticate before (re-)registering.
  authenticated.erase(node->pid);

  deactivate(node);
}


void Controller::deactivate(Node* node)
{
  CHECK_NOTNULL(node);

  LOG(INFO) << "Deactivating node " << *node;

  node->active = false;

  allocator->deactivateNode(node->id);

  foreach (Offer* offer, utils::copy(node->offers)) {
    rescindOffer(offer);
  }

  // Remove and rescind inverse offers.
  foreach (InverseOffer* inverseOffer, utils::copy(node->inverseOffers)) {
    allocator->updateInverseOffer(
        node->id,
        inverseOffer->framework_id(),
        UnavailableResources{
            inverseOffer->resources(),
            inverseOffer->unavailability()},
        None());

    removeInverseOffer(inverseOffer, true); // Rescind!
  }
}


void Controller::resourceRequest(
    const UPID& from,
    const FrameworkID& frameworkId,
    const vector<Request>& requests)
{
  Framework* framework = getFramework(frameworkId);

  if (framework == nullptr) {
    LOG(WARNING)
      << "Ignoring resource request message from framework " << frameworkId
      << " because the framework cannot be found";

    return;
  }

  if (framework->pid() != from) {
    LOG(WARNING)
      << "Ignoring resource request message from framework " << *framework
      << " because it is not expected from " << from;

    return;
  }

  scheduler::Call::Request call;
  foreach (const Request& request, requests) {
    call.add_requests()->CopyFrom(request);
  }

  request(framework, call);
}


void Controller::request(
    Framework* framework,
    const scheduler::Call::Request& request)
{
  CHECK_NOTNULL(framework);

  LOG(INFO) << "Processing REQUEST call for framework " << *framework;

  ++metrics->messages_resource_request;

  allocator->requestResources(
      framework->id(),
      google::protobuf::convert(request.requests()));
}


void Controller::suppress(
    Framework* framework,
    const scheduler::Call::Suppress& suppress)
{
  CHECK_NOTNULL(framework);

  LOG(INFO) << "Processing SUPPRESS call for framework " << *framework;

  ++metrics->messages_suppress_offers;

  set<string> roles;

  // Validate the roles, if provided. We need to make sure the
  // roles is valid and also contained within the framework roles.
  // Note that if a single role is invalid, we drop the entire
  // call and do not suppress the valid roles.
  foreach (const string& role, suppress.roles()) {
    Option<Error> roleError = roles::validate(role);
    if (roleError.isSome()) {
      drop(framework,
           suppress,
           "suppression role '" + role + "' is invalid: " + roleError->message);
      return;
    }

    if (framework->roles.count(role) == 0) {
      drop(framework,
           suppress,
           "suppression role '" + role + "' is not one"
           " of the frameworks's subscribed roles");
      return;
    }

    roles.insert(role);
  }

  allocator->suppressOffers(framework->id(), roles);
}


vector<string> Controller::knownRoles() const
{
  // NOTE: we use a `std::set` to store the role names to ensure a
  // deterministic output order.
  set<string> roleList;

  auto insertAncestors = [&roleList](const string& role) {
    foreach (const string& ancestor, roles::ancestors(role)) {
      bool inserted = roleList.insert(ancestor).second;

      // We can break here as an optimization since the ancestor
      // will have had its ancestors inserted already.
      if (!inserted) break;
    }
  };

  if (roleAllowlist.isSome()) {
    foreach (const string& role, *this->roleAllowlist) {
      roleList.insert(role);
      insertAncestors(role);
    }
  } else {
    // In terms of building a complete set of known roles, we have to visit:
    //   (1) all entries of `roles` (which means there are frameworks
    //       subscribed to a role or have allocations to a role)
    //   (2) all reservation roles
    //   (3) all roles with configured weights or quotas
    //   (4) all ancestor roles of (1), (2), and (3).

    foreachkey (const string& role, this->roles) {
      roleList.insert(role);
      insertAncestors(role);
    }

    foreachvalue (Node* node, this->nodes.registered) {
      foreachkey (const string& role, node->totalResources.reservations()) {
        roleList.insert(role);
        insertAncestors(role);
      }
    }

    foreachkey (const string& role, this->weights) {
      roleList.insert(role);
      insertAncestors(role);
    }

    foreachkey (const string& role, this->quotas) {
      roleList.insert(role);
      insertAncestors(role);
    }
  }

  return vector<string>(roleList.begin(), roleList.end());
}


bool Controller::isAllowlistedRole(const string& name) const
{
  if (roleAllowlist.isNone()) {
    return true;
  }

  return roleAllowlist->contains(name);
}


ResourceQuantities Controller::RoleResourceBreakdown::offered() const
{
  ResourceQuantities result;

  foreachvalue (Framework* framework, controller->frameworks.registered) {
    result += ResourceQuantities::fromResources(
        framework->totalOfferedResources.allocatedToRoleSubtree(role));
  }

  return result;
}


ResourceQuantities Controller::RoleResourceBreakdown::allocated() const
{
  ResourceQuantities result;

  foreachvalue (Framework* framework, controller->frameworks.registered) {
    result += ResourceQuantities::fromResources(
        framework->totalUsedResources.allocatedToRoleSubtree(role));
  }

  return result;
}


ResourceQuantities Controller::RoleResourceBreakdown::reserved() const
{
  ResourceQuantities result;

  foreachvalue (Node* node, controller->nodes.registered) {
    result += ResourceQuantities::fromResources(
        node->totalResources.reservedToRoleSubtree(role));
  }

  return result;
}


ResourceQuantities Controller::RoleResourceBreakdown::consumedQuota() const
{
  ResourceQuantities unallocatedReservation;

  foreachvalue (Node* node, controller->nodes.registered) {
    ResourceQuantities totalReservation = ResourceQuantities::fromResources(
        node->totalResources.reservedToRoleSubtree(role));

    ResourceQuantities usedReservation;
    foreachvalue (const Resources& r, node->usedResources) {
      usedReservation += ResourceQuantities::fromResources(
          r.reservedToRoleSubtree(role));
    }

    unallocatedReservation += totalReservation - usedReservation;
  }

  return allocated() + unallocatedReservation;
}


void Controller::launchTasks(
    const UPID& from,
    LaunchTasksMessage&& launchTasksMessage)
{
  Framework* framework = getFramework(launchTasksMessage.framework_id());

  if (framework == nullptr) {
    LOG(WARNING)
      << "Ignoring launch tasks message for offers "
      << stringify(launchTasksMessage.offer_ids())
      << " of framework " << launchTasksMessage.framework_id()
      << " because the framework cannot be found";

    return;
  }

  if (framework->pid() != from) {
    LOG(WARNING)
      << "Ignoring launch tasks message for offers "
      << stringify(launchTasksMessage.offer_ids())
      << " from '" << from << "' because it is not from the"
      << " registered framework " << *framework;

    return;
  }

  // Currently when no tasks are specified in the launchTasks message
  // it is implicitly considered a decline of the offers.
  if (!launchTasksMessage.tasks().empty()) {
    scheduler::Call::Accept message;

    *message.mutable_filters() =
      std::move(*launchTasksMessage.mutable_filters());

    *message.mutable_offer_ids() =
      std::move(*launchTasksMessage.mutable_offer_ids());

    Offer::Operation* operation = message.add_operations();
    operation->set_type(Offer::Operation::LAUNCH);

    *operation->mutable_launch()->mutable_task_infos() =
      std::move(*launchTasksMessage.mutable_tasks());

    accept(framework, std::move(message));
  } else {
    scheduler::Call::Decline message;

    *message.mutable_filters() =
      std::move(*launchTasksMessage.mutable_filters());

    *message.mutable_offer_ids() =
      std::move(*launchTasksMessage.mutable_offer_ids());

    decline(framework, std::move(message));
  }
}


Future<bool> Controller::authorize(
    const Option<Principal>& principal,
    ActionObject&& actionObject)
{
  if (authorizer.isNone()) {
    return true;
  }

  const Option<authorization::Subject> subject = createSubject(principal);

  authorization::Request request;

  if (subject.isSome()) {
    *request.mutable_subject() = *subject;
  }

  LOG(INFO) << "Authorizing"
            << (principal.isSome()
                  ? " principal '" + stringify(*principal) + "'"
                  : " ANY principal")
            << " to " << actionObject;

  request.set_action(actionObject.action());
  if (actionObject.object().isSome()) {
    *request.mutable_object() = *(std::move(actionObject).object());
  }

  return authorizer.get()->authorized(request);
}


Future<bool> Controller::authorize(
    const Option<Principal>& principal,
    vector<ActionObject>&& actionObjects)
{
  // NOTE: In some cases (example: RESERVE with empty resources or with source
  // identical to target) there is no need to authorize any action-object pair
  // (and no meaningful ActionObject can be composed anyway). Here, we treat
  // authorization as PASSED in these cases and expect these cases to be
  // handled by validation afterwards.
  //
  // Some of these cases are invalid; ideally, they should be filtered by
  // validation before being fed into ActionObject-composing code (see
  // MESOS-10083).
  if (actionObjects.empty()) {
    return true;
  }

  vector<Future<bool>> authorizations;
  authorizations.reserve(actionObjects.size());
  for (ActionObject& actionObject : actionObjects) {
    authorizations.push_back(authorize(principal, std::move(actionObject)));
  }

  return authorization::collectAuthorizations(authorizations);
}


bool Controller::isLaunchExecutor(
    const ExecutorID& executorId,
    Framework* framework,
    Node* node) const
{
  CHECK_NOTNULL(framework);
  CHECK_NOTNULL(node);

  if (!node->hasExecutor(framework->id(), executorId)) {
    CHECK(!framework->hasExecutor(node->id, executorId))
      << "Executor '" << executorId
      << "' known to the framework " << *framework
      << " but unknown to the node " << *node;

    return true;
  }

  return false;
}


void Controller::addExecutor(
    const ExecutorInfo& executorInfo,
    Framework* framework,
    Node* node)
{
  CHECK_NOTNULL(framework);
  CHECK_NOTNULL(node);
  CHECK(node->connected) << "Adding executor " << executorInfo.executor_id()
                          << " to disconnected node " << *node;

  // Note that we explicitly convert from protobuf to `Resources` here
  // and then use the result below to avoid performance penalty for multiple
  // conversions and validations implied by conversion.
  // Conversion is safe, as resources have already passed validation.
  const Resources resources = executorInfo.resources();

  LOG(INFO) << "Adding executor '" << executorInfo.executor_id()
            << "' with resources " << resources
            << " of framework " << *framework
            << " on node " << *node;

  node->addExecutor(framework->id(), executorInfo);
  framework->addExecutor(node->id, executorInfo);
}


void Controller::addTask(
    const TaskInfo& task,
    Framework* framework,
    Node* node)
{
  CHECK_NOTNULL(framework);
  CHECK_NOTNULL(node);
  CHECK(node->connected) << "Adding task " << task.task_id()
                          << " to disconnected node " << *node;

  // Note that we explicitly convert from protobuf to `Resources` here
  // and then use the result below to avoid performance penalty for multiple
  // conversions and validations implied by conversion.
  // Conversion is safe, as resources have already passed validation.
  const Resources resources = task.resources();

  LOG(INFO) << "Adding task " << task.task_id()
            << " with resources " << resources
            << " of framework " << *framework
            << " on node " << *node;

  // Add the task to the framework and node.
  Task* t = new Task(protobuf::createTask(
      task,
      TASK_STAGING,
      framework->id(),
      framework->info.has_principal()
        ? Option<string>(framework->info.principal())
        : Option<string>::none()));

  node->addTask(t);
  framework->addTask(t);
}



void Controller::accept(
    Framework* framework,
    scheduler::Call::Accept&& accept)
{
  CHECK_NOTNULL(framework);

  // Bump metrics.
  foreach (const Offer::Operation& operation, accept.operations()) {
    if (operation.type() == Offer::Operation::LAUNCH) {
      if (operation.launch().task_infos().size() > 0) {
        ++metrics->messages_launch_tasks;
      } else {
        ++metrics->messages_decline_offers;
        LOG(WARNING) << "Implicitly declining offers: " << accept.offer_ids()
                     << " in ACCEPT call for framework " << framework->id()
                     << " as the launch operation specified no tasks";
      }
    }

    // TODO(mpark): Add metrics for LAUNCH_GROUP operation.
    // TODO(jieyu): Add metrics for non launch operations.
  }

  Option<Error> error = None();

  if (accept.offer_ids().size() == 0) {
    error = Error("No offers specified");
  } else {
    // Validate the offers.
    error = validation::offer::validate(accept.offer_ids(), this, framework);
  }

  if (error.isSome()) {
    // TODO(jieyu): Consider adding a 'drop' overload for ACCEPT call to
    // consistently handle message dropping. It would be ideal if the
    // 'drop' overload can handle both resource recovery and lost task
    // notifications.

    // Discard existing offers.
    foreach (const OfferID& offerId, accept.offer_ids()) {
      Offer* offer = getOffer(offerId);
      if (offer != nullptr) {
        discardOffer(offer);
      } else {
        // If the offer was not in our offer set, then this offer is no
        // longer valid.
        LOG(WARNING) << "Ignoring accept of offer " << offerId
                     << " since it is no longer valid";
      }
    }

    LOG(WARNING) << "ACCEPT call used invalid offers '" << accept.offer_ids()
                 << "': " << error->message;

    const TaskState newTaskState =
      framework->capabilities.partitionAware ? TASK_DROPPED : TASK_LOST;

    foreach (const Offer::Operation& operation, accept.operations()) {
      // Send OPERATION_ERROR for non-LAUNCH operations
      if (operation.type() != Offer::Operation::LAUNCH &&
          operation.type() != Offer::Operation::LAUNCH_GROUP) {
        drop(framework,
             operation,
             "Operation attempted with invalid offers: " + error->message);
        continue;
      }

      // Send task status updates for launch attempts.
      const RepeatedPtrField<TaskInfo>& tasks = [&]() {
        if (operation.type() == Offer::Operation::LAUNCH) {
          return operation.launch().task_infos();
        } else if (operation.type() == Offer::Operation::LAUNCH_GROUP) {
          return operation.launch_group().task_group().tasks();
        }
        UNREACHABLE();
      }();

      foreach (const TaskInfo& task, tasks) {
        const StatusUpdate& update = protobuf::createStatusUpdate(
            framework->id(),
            task.node_id(),
            task.task_id(),
            newTaskState,
            TaskStatus::SOURCE_CONTROLLER,
            None(),
            "Task launched with invalid offers: " + error->message,
            TaskStatus::REASON_INVALID_OFFERS);

        if (framework->capabilities.partitionAware) {
          metrics->tasks_dropped++;
        } else {
          metrics->tasks_lost++;
        }

        metrics->incrementTasksStates(
            newTaskState,
            TaskStatus::SOURCE_CONTROLLER,
            TaskStatus::REASON_INVALID_OFFERS);

        forward(update, UPID(), framework);
      }
    }

    return;
  }

  // From now on, we are handling the valid offers case.

  // Get node id and allocation info from some existing offer
  // (as they are valid, they must have the same node id and allocation info).
  const Offer* existingOffer = ([this](const RepeatedPtrField<OfferID>& ids) {
    for (const OfferID& id : ids) {
      const Offer* offer = getOffer(id);
      if (offer != nullptr) {
        return offer;
      }
    }

    LOG(FATAL) << "No validated offer_ids correspond to existing offers";
  })(accept.offer_ids());

  // TODO(bmahler): We currently only support using multiple offers
  // for a single node.
  NodeID nodeId = existingOffer->node_id();

  // TODO(asekretenko): The code below is copying AllocationInfo (and later
  // injecting it into operations) as a whole, but only the 'role' field is
  // subject to offer validation. As for now, this works fine, because
  // AllocationInfo has no other fields. However, this is fragile and can
  // silently break if more fields are added to AllocationInfo.
  Resource::AllocationInfo allocationInfo = existingOffer->allocation_info();

  Node* node = nodes.registered.get(nodeId);
  CHECK(node != nullptr) << nodeId;

  // Validate and upgrade all of the resources in `accept.operations`:
  //
  // For an operation except LAUNCH and LAUNCH_GROUP which contains invalid
  // resources,
  //   - if the framework has elected to receive feedback by setting the `id`
  //     field, then we send an offer operation status update with a state of
  //     OPERATION_ERROR.
  //   - if the framework has not set the `id` field, then we simply drop the
  //     operation.
  //
  // If a LAUNCH or LAUNCH_GROUP operation contains invalid resources, we send
  // a TASK_ERROR status update per task.
  //
  //
  // If the framework is requesting offer operation status updates by setting
  // the `id` field in an operation, then also verify that the relevant node
  // has the RESOURCE_PROVIDER capability. If it does not, then send an offer
  // operation status update with a state of OPERATION_ERROR.
  //
  // LAUNCH and LAUNCH_GROUP operations cannot receive offer operation status,
  // updates, so we send a TASK_ERROR status update per task when these
  // operations set the `id` field.
  {
    // Used to send TASK_ERROR status updates for tasks in invalid LAUNCH
    // and LAUNCH_GROUP operations. Note that we don't need to recover
    // the resources here because we always continue onto `_accept`
    // which recovers the unused resources at the end.
    //
    // TODO(mpark): Consider pulling this out in a more reusable manner.
    auto sendStatusUpdates = [&](
        const RepeatedPtrField<TaskInfo>& tasks,
        TaskStatus::Reason reason,
        const string& message) {
      foreach (const TaskInfo& task, tasks) {
        const StatusUpdate& update = protobuf::createStatusUpdate(
            framework->id(),
            task.node_id(),
            task.task_id(),
            TASK_ERROR,
            TaskStatus::SOURCE_CONTROLLER,
            None(),
            message,
            reason);

        metrics->tasks_error++;

        metrics->incrementTasksStates(
            TASK_ERROR, TaskStatus::SOURCE_CONTROLLER, reason);

        forward(update, UPID(), framework);
      }
    };

    // We move out the `accept.operations`, and re-insert the operations
    // with the resources validated and upgraded.
    RepeatedPtrField<Offer::Operation> operations = accept.operations();
    accept.clear_operations();

    foreach (Offer::Operation& operation, operations) {
      Option<Error> error = validateAndUpgradeResources(&operation);

      // Additional operation-specific validation.
      if (error.isNone()) {
        switch (operation.type()) {
          case Offer::Operation::RESERVE:
            if (!operation.reserve().source().empty()) {
              error = Error("Reservation updates are not yet implemented"
                            " for the scheduler API.");
            }
            break;
          case Offer::Operation::UNRESERVE:
          case Offer::Operation::CREATE:
          case Offer::Operation::DESTROY:
          case Offer::Operation::GROW_VOLUME:
          case Offer::Operation::SHRINK_VOLUME:
          case Offer::Operation::CREATE_DISK:
          case Offer::Operation::DESTROY_DISK:
          case Offer::Operation::LAUNCH:
          case Offer::Operation::LAUNCH_GROUP:
          case Offer::Operation::UNKNOWN:
            break;
        }
      }

      if (error.isSome()) {
        switch (operation.type()) {
          case Offer::Operation::RESERVE:
          case Offer::Operation::UNRESERVE:
          case Offer::Operation::CREATE:
          case Offer::Operation::DESTROY:
          case Offer::Operation::GROW_VOLUME:
          case Offer::Operation::SHRINK_VOLUME:
          case Offer::Operation::CREATE_DISK:
          case Offer::Operation::DESTROY_DISK: {
            drop(framework,
                 operation,
                 "Operation attempted with invalid resources: " +
                 error->message);
            break;
          }
          case Offer::Operation::LAUNCH: {
            sendStatusUpdates(
                operation.launch().task_infos(),
                TaskStatus::REASON_TASK_INVALID,
                error->message);

            break;
          }
          case Offer::Operation::LAUNCH_GROUP: {
            sendStatusUpdates(
                operation.launch_group().task_group().tasks(),
                TaskStatus::REASON_TASK_GROUP_INVALID,
                error->message);

            break;
          }
          case Offer::Operation::UNKNOWN: {
            LOG(WARNING) << "Ignoring unknown operation";
            break;
          }
        }
      } else if (operation.has_id()) {
        // The `id` field is set, which means operation feedback is requested.
        //
        // Operation feedback is not supported for LAUNCH or LAUNCH_GROUP
        // operations, so we drop them and send TASK_ERROR status updates.
        //
        // For other operations, verify that they have been sent by an HTTP
        // framework and that they are destined for a node with the
        // RESOURCE_PROVIDER capability.
        switch (operation.type()) {
          case Offer::Operation::LAUNCH: {
            sendStatusUpdates(
                operation.launch().task_infos(),
                TaskStatus::REASON_TASK_INVALID,
                "The `id` field cannot be set on LAUNCH operations");

            break;
          }
          case Offer::Operation::LAUNCH_GROUP: {
            sendStatusUpdates(
                operation.launch_group().task_group().tasks(),
                TaskStatus::REASON_TASK_GROUP_INVALID,
                "The `id` field cannot be set on LAUNCH_GROUP operations");

            break;
          }
          case Offer::Operation::RESERVE:
          case Offer::Operation::UNRESERVE:
          case Offer::Operation::CREATE:
          case Offer::Operation::DESTROY:
          case Offer::Operation::GROW_VOLUME:
          case Offer::Operation::SHRINK_VOLUME:
          case Offer::Operation::CREATE_DISK:
          case Offer::Operation::DESTROY_DISK: {
            if (framework->http().isNone()) {
              const string message =
                "The 'id' field was set in an offer operation, but operation"
                " feedback is not supported for the SchedulerDriver API";

              LOG(WARNING) << "Dropping "
                           << Offer::Operation::Type_Name(operation.type())
                           << " operation from framework " << *framework << ": "
                           << message;

              // Send an error which will cause the scheduler driver to abort.
              FrameworkErrorMessage frameworkError;
              frameworkError.set_message(
                  message +
                  "; please use the HTTP scheduler API for this feature");
              framework->send(frameworkError);

              break;
            }

            if (!node->capabilities.resourceProvider) {
              drop(framework,
                   operation,
                   "Operation requested feedback, but node " +
                   stringify(nodeId) +
                   " does not have the required RESOURCE_PROVIDER capability");
              break;
            }

            if (getResourceProviderId(operation).isNone() &&
                !(node->capabilities.nodeOperationFeedback &&
                  node->capabilities.resourceProvider)) {
              drop(framework,
                   operation,
                   "Operation on node default resources requested feedback,"
                   " but node " + stringify(nodeId) +
                   " does not have the required NODE_OPERATION_FEEDBACK and"
                   " RESOURCE_PROVIDER capabilities");
              break;
            }

            accept.add_operations()->CopyFrom(operation);
            break;
          }
          case Offer::Operation::UNKNOWN: {
            LOG(WARNING) << "Ignoring unknown operation";
            break;
          }
        }
      } else {
        // Resource validation succeeded and feedback is not requested,
        // so add the operation.
        accept.add_operations()->CopyFrom(operation);
      }
    }
  }

  // We make various adjustments to the `Offer::Operation`s,
  // typically for backward/forward compatibility.
  // TODO(mpark): Pull this out to a controller normalization utility.
  foreach (Offer::Operation& operation, *accept.mutable_operations()) {
    // With the addition of the MULTI_ROLE capability, the resources
    // within an offer now contain an `AllocationInfo`. We therefore
    // inject the offer's allocation info into the operation's
    // resources if the scheduler has not done so already.
    protobuf::injectAllocationInfo(&operation, allocationInfo);

    switch (operation.type()) {
      case Offer::Operation::RESERVE:
      case Offer::Operation::UNRESERVE:
      case Offer::Operation::CREATE:
      case Offer::Operation::DESTROY:
      case Offer::Operation::GROW_VOLUME:
      case Offer::Operation::SHRINK_VOLUME:
      case Offer::Operation::CREATE_DISK:
      case Offer::Operation::DESTROY_DISK: {
        // No-op.
        break;
      }
      case Offer::Operation::LAUNCH: {
        foreach (
            TaskInfo& task, *operation.mutable_launch()->mutable_task_infos()) {
          // TODO(haosdent): Once we have internal `TaskInfo` separate from
          // the v0 `TaskInfo` (see MESOS-6268), consider extracting the
          // following adaptation code into devolve methods from v0 and v1
          // `TaskInfo` to internal `TaskInfo`.
          //
          // Make a copy of the original task so that we can fill the missing
          // `framework_id` in `ExecutorInfo` if needed. This field was added
          // to the API later and thus was made optional.
          if (task.has_executor() && !task.executor().has_framework_id()) {
            task.mutable_executor()->mutable_framework_id()->CopyFrom(
                framework->id());
          }

          // For backwards compatibility with the v0 and v1 API, when
          // the type of the health check is not specified, determine
          // its type from the `http` and `command` fields.
          //
          // TODO(haosdent): Remove this after the deprecation cycle which
          // starts in 2.0.
          if (task.has_health_check() && !task.health_check().has_type()) {
            LOG(WARNING) << "The type of health check is not set; use of "
                         << "'HealthCheck' without specifying 'type' will be "
                         << "deprecated in Medios 2.0";

            const HealthCheck& healthCheck = task.health_check();
            if (healthCheck.has_command() && !healthCheck.has_http()) {
              task.mutable_health_check()->set_type(HealthCheck::COMMAND);
            } else if (healthCheck.has_http() && !healthCheck.has_command()) {
              task.mutable_health_check()->set_type(HealthCheck::HTTP);
            }
          }

          if (HookManager::hooksAvailable()) {
            *task.mutable_resources() =
              HookManager::controllerLaunchTaskResourceDecorator(task,
                node->totalResources);
          }
        }

        break;
      }
      case Offer::Operation::LAUNCH_GROUP: {
        const ExecutorInfo& executor = operation.launch_group().executor();

        TaskGroupInfo* taskGroup =
          operation.mutable_launch_group()->mutable_task_group();

        // Mutate `TaskInfo` to include `ExecutorInfo` to make it easy
        // for operator API and WebUI to get access to the corresponding
        // executor for tasks in the task group.
        foreach (TaskInfo& task, *taskGroup->mutable_tasks()) {
          if (!task.has_executor()) {
            task.mutable_executor()->CopyFrom(executor);
          }

          if (HookManager::hooksAvailable()) {
            *task.mutable_resources() =
              HookManager::controllerLaunchTaskResourceDecorator(task,
                node->totalResources);
          }
        }

        break;
      }
      case Offer::Operation::UNKNOWN: {
        // No-op.
        break;
      }
    }
  }

  LOG(INFO) << "Processing ACCEPT call for offers: " << accept.offer_ids()
            << " on node " << *node << " for framework " << *framework;

  // TODO(asekretenko): Dismantle `_accept(...)` (which, before synchronous
  // authorization was introduced, used to be a deferred continuation of ACCEPT
  // call processing, but now is kept only for limiting variable scopes) and
  // handle operations one-by-one.
  _accept(framework->id(), nodeId, std::move(accept));
}


void Controller::_accept(
    const FrameworkID& frameworkId,
    const NodeID& nodeId,
    scheduler::Call::Accept&& accept)
{
  auto discardOffers = [this](const RepeatedPtrField<OfferID>& ids) {
    for (const OfferID& offerId : ids) {
      Offer* offer = getOffer(offerId);
      if (offer != nullptr) {
        discardOffer(offer);
      }
    }
  };

  Framework* framework = getFramework(frameworkId);

  // TODO(jieyu): Consider using the 'drop' overload mentioned in
  // 'accept' to consistently handle dropping ACCEPT calls.
  if (framework == nullptr) {
    LOG(WARNING)
      << "Ignoring ACCEPT call for framework " << frameworkId
      << " because the framework cannot be found";

    // TODO(asekretenko): consider replacing this with a CHECK that there
    // never are any offers for a non-active (inactive/completed/...) framework.
    discardOffers(accept.offer_ids());
    return;
  }

  Node* node = nodes.registered.get(nodeId);

  if (node == nullptr || !node->connected) {
    TaskState newTaskState = TASK_DROPPED;
    if (!framework->capabilities.partitionAware) {
      newTaskState = TASK_LOST;
    }

    foreach (const Offer::Operation& operation, accept.operations()) {
      if (operation.type() != Offer::Operation::LAUNCH &&
          operation.type() != Offer::Operation::LAUNCH_GROUP) {
        continue;
      }

      const RepeatedPtrField<TaskInfo>& tasks = [&]() {
        if (operation.type() == Offer::Operation::LAUNCH) {
          return operation.launch().task_infos();
        } else {
          CHECK_EQ(Offer::Operation::LAUNCH_GROUP, operation.type());
          return operation.launch_group().task_group().tasks();
        }
      }();

      foreach (const TaskInfo& task, tasks) {
        const TaskStatus::Reason reason =
            node == nullptr ? TaskStatus::REASON_NODE_REMOVED
                             : TaskStatus::REASON_NODE_DISCONNECTED;
        const StatusUpdate& update = protobuf::createStatusUpdate(
            framework->id(),
            task.node_id(),
            task.task_id(),
            newTaskState,
            TaskStatus::SOURCE_CONTROLLER,
            None(),
            node == nullptr ? "Node removed" : "Node disconnected",
            reason);

        if (framework->capabilities.partitionAware) {
          metrics->tasks_dropped++;
        } else {
          metrics->tasks_lost++;
        }

        metrics->incrementTasksStates(
            newTaskState,
            TaskStatus::SOURCE_CONTROLLER,
            reason);

        forward(update, UPID(), framework);
      }
    }

    // TODO(asekretenko): consider replacing this with a CHECK that there
    // never are any offers for a removed/disconnected node.
    discardOffers(accept.offer_ids());
    return;
  }

  Resources offeredResources;
  size_t offersAccepted = 0;

  foreach (const OfferID& offerId, accept.offer_ids()) {
    Offer* offer = getOffer(offerId);
    if (offer == nullptr) {
      LOG(WARNING) << "Ignoring accept of offer " << offerId
                   << " since it is no longer valid";
      continue;
    }
    offeredResources += offer->resources();
    ++offersAccepted;

    _removeOffer(framework, offer);
  }

  framework->metrics.offers_accepted += offersAccepted;

  // We maintain the "running remaining" resources here to support pipelining of
  // speculative operations (e.g., RESERVE), which would modify the remaining
  // resources. Resources consumed by non-speculative operations (e.g., LAUNCH)
  // are removed from the remaining resources.
  Resources remainingResources = offeredResources;

  // Allocator should be informed about resources allocated for tasks
  /// and executors by LAUNCH/LAUNCH_GROUP operations.
  Resources launchResources;

  // Converted resources from volume resizes. These converted resources are not
  // put into `remainingResources`, so no other operations can consume them.
  // TODO(zhitao): This will be unnecessary once `GROW_VOLUME` and
  // `SHRINK_VOLUME` become non-speculative.
  Resources resizedResources;

  // We keep track of the "running remaining" shared resources from the offers
  // separately. `remainingSharedResources` can be modified by CREATE/DESTROY
  // but we don't remove from it when a task is successfully launched so this
  // variable always tracks the *total* amount. We do this to support validation
  // of tasks involving shared resources. See comments in the LAUNCH case below.
  Resources remainingSharedResources = offeredResources.shared();

  // Maintain a list of resource conversions to pass to the allocator
  // as a result of operations. Note that:
  // 1) We drop invalid operations.
  // 2) For LAUNCH operations, we drop invalid tasks. LAUNCH operation
  //    will result in resource conversions because of shared
  //    resources.
  // 3) Currently, LAUNCH_GROUP won't result in resource conversions
  //    because shared resources are not supported yet if the
  //    framework uses LAUNCH_GROUP operation.
  //
  // The order of the conversions is important and preserved.
  vector<ResourceConversion> conversions;

  foreach (const Offer::Operation& operation, accept.operations()) {
    auto authorized_ =
      [&framework, &operation](const ActionObject& actionObject)
        -> Option<Error> {
      const Try<bool> authorized = framework->approved(actionObject);
      if (authorized.isError()) {
        return Error(
            "Failed to authorize principal '" + framework->info.principal() +
            "' to perform " + Offer::Operation::Type_Name(operation.type()) +
            ": " + authorized.error());
      }

      if (!*authorized) {
        return Error(
            "Principal '" + framework->info.principal() +
            "' no authorized to " + stringify(actionObject));
      }

      return None();
    };

    auto authorized = overload(
        authorized_,
        [&authorized_](const vector<ActionObject>& actionObjects) {
          for (const ActionObject& actionObject : actionObjects) {
            const Option<Error> error = authorized_(actionObject);
            if (error.isSome()) {
              return error;
            }
          }

          return Option<Error>::none();
        });


    switch (operation.type()) {
      // The RESERVE operation allows a principal to reserve resources.
      case Offer::Operation::RESERVE: {
        Option<Principal> principal = framework->info.has_principal()
                                        ? Principal(framework->info.principal())
                                        : Option<Principal>::none();

        Option<Error> error = validation::operation::validate(
            operation.reserve(),
            principal,
            node->capabilities,
            framework->info);

        error = error.isSome()
          ? Error(error->message + "; on node " + stringify(*node))
          : authorized(ActionObject::reserve(operation.reserve()));

        if (error.isSome()) {
          drop(framework, operation, error->message);
          continue;
        }

        // Test the given operation on the included resources.
        Try<vector<ResourceConversion>> _conversions =
          getResourceConversions(operation);

        if (_conversions.isError()) {
          drop(framework, operation, _conversions.error());
          continue;
        }

        Try<Resources> resources = remainingResources.apply(_conversions.get());
        if (resources.isError()) {
          drop(framework, operation, resources.error());
          continue;
        }

        remainingResources = resources.get();

        LOG(INFO) << "Applying RESERVE operation for resources "
                  << operation.reserve().resources() << " from framework "
                  << *framework << " to node " << *node;

        _apply(node, framework, operation);

        conversions.insert(
            conversions.end(),
            _conversions->begin(),
            _conversions->end());

        break;
      }

      // The UNRESERVE operation allows a principal to unreserve resources.
      case Offer::Operation::UNRESERVE: {
        Option<Error> error =
          validation::operation::validate(operation.unreserve());

        error = error.isSome()
          ? Error(error->message + "; on node " + stringify(*node))
          : authorized(ActionObject::unreserve(operation.unreserve()));

        if (error.isSome()) {
          drop(framework, operation, error->message);
          continue;
        }

        // Test the given operation on the included resources.
        Try<vector<ResourceConversion>> _conversions =
          getResourceConversions(operation);

        if (_conversions.isError()) {
          drop(framework, operation, _conversions.error());
          continue;
        }

        Try<Resources> resources = remainingResources.apply(_conversions.get());
        if (resources.isError()) {
          drop(framework, operation, resources.error());
          continue;
        }

        remainingResources = resources.get();

        LOG(INFO) << "Applying UNRESERVE operation for resources "
                  << operation.unreserve().resources() << " from framework "
                  << *framework << " to node " << *node;

        _apply(node, framework, operation);

        conversions.insert(
            conversions.end(),
            _conversions->begin(),
            _conversions->end());

        break;
      }

      case Offer::Operation::CREATE: {
        Option<Principal> principal = framework->info.has_principal()
                                        ? Principal(framework->info.principal())
                                        : Option<Principal>::none();

        // Make sure this create operation is valid.
        Option<Error> error = validation::operation::validate(
            operation.create(),
            node->checkpointedResources,
            principal,
            node->capabilities,
            framework->info);

        error = error.isSome()
          ? Error(error->message + "; on node " + stringify(*node))
          : authorized(ActionObject::createVolume(operation.create()));

        if (error.isSome()) {
          drop(framework, operation, error->message);
          continue;
        }

        // Test the given operation on the included resources.
        Try<vector<ResourceConversion>> _conversions =
          getResourceConversions(operation);

        if (_conversions.isError()) {
          drop(framework, operation, _conversions.error());
          continue;
        }

        Try<Resources> resources = remainingResources.apply(_conversions.get());
        if (resources.isError()) {
          drop(framework, operation, resources.error());
          continue;
        }

        remainingResources = resources.get();
        remainingSharedResources = remainingResources.shared();

        LOG(INFO) << "Applying CREATE operation for volumes "
                  << operation.create().volumes() << " from framework "
                  << *framework << " to node " << *node;

        _apply(node, framework, operation);

        conversions.insert(
            conversions.end(),
            _conversions->begin(),
            _conversions->end());

        break;
      }

      case Offer::Operation::DESTROY: {
        Option<Error> error = validation::operation::validate(
            operation.destroy(),
            node->checkpointedResources,
            node->usedResources);

        error = error.isSome()
          ? Error(error->message + "; on node " + stringify(*node))
          : authorized(ActionObject::destroyVolume(operation.destroy()));

        if (error.isSome()) {
          drop(framework, operation, error->message);
          continue;
        }

        // If any offer from this node contains a volume that needs
        // to be destroyed, we should process it, but we should also
        // rescind those offers.
        foreach (Offer* offer, utils::copy(node->offers)) {
          const Resources& offered = offer->resources();

          foreach (const Resource& volume, operation.destroy().volumes()) {
            if (offered.contains(volume)) {
              rescindOffer(offer);

              // This offer may contain other volumes that are being destroyed.
              // However, we have already rescinded it, so we should move on
              // to the next offer.
              break;
            }
          }
        }

        // Test the given operation on the included resources.
        Try<vector<ResourceConversion>> _conversions =
          getResourceConversions(operation);

        if (_conversions.isError()) {
          drop(framework, operation, _conversions.error());
          continue;
        }

        Try<Resources> resources = remainingResources.apply(_conversions.get());
        if (resources.isError()) {
          drop(framework, operation, resources.error());
          continue;
        }

        remainingResources = resources.get();
        remainingSharedResources = remainingResources.shared();

        LOG(INFO) << "Applying DESTROY operation for volumes "
                  << operation.destroy().volumes() << " from framework "
                  << *framework << " to node " << *node;

        _apply(node, framework, operation);

        conversions.insert(
            conversions.end(),
            _conversions->begin(),
            _conversions->end());

        break;
      }

      case Offer::Operation::GROW_VOLUME: {
        Option<Error> error = validation::operation::validate(
            operation.grow_volume(), node->capabilities);

        error = error.isSome() ?
          Error(error->message + "; on node " + stringify(*node))
          : authorized(ActionObject::growVolume(operation.grow_volume()));

        if (error.isSome()) {
          drop(framework, operation, error->message);
          continue;
        }

        // TODO(zhitao): Convert this operation to non-speculative once we can
        // support that in the operator API.
        Try<vector<ResourceConversion>> _conversions =
          getResourceConversions(operation);

        if (_conversions.isError()) {
          drop(framework, operation, _conversions.error());
          continue;
        }

        CHECK_EQ(1u, _conversions->size());
        const Resources& consumed = _conversions->at(0).consumed;
        const Resources& converted = _conversions->at(0).converted;

        if (!remainingResources.contains(consumed)) {
          drop(
              framework,
              operation,
              "Invalid GROW_VOLUME operation: " +
              stringify(remainingResources) + " does not contain " +
              stringify(consumed));

          continue;
        }

        remainingResources -= consumed;
        resizedResources += converted;

        LOG(INFO) << "Processing GROW_VOLUME operation for volume "
                  << operation.grow_volume().volume()
                  << " with additional resource "
                  << operation.grow_volume().addition()
                  << " from framework "
                  << *framework << " on node " << *node;

        _apply(node, framework, operation);

        conversions.insert(
            conversions.end(),
            _conversions->begin(),
            _conversions->end());

        break;
      }

      case Offer::Operation::SHRINK_VOLUME: {
        Option<Error> error = validation::operation::validate(
            operation.shrink_volume(), node->capabilities);

        error = error.isSome()
          ? Error(error->message + "; on node " + stringify(*node))
          : authorized(ActionObject::shrinkVolume(operation.shrink_volume()));

        if (error.isSome()) {
          drop(framework, operation, error->message);
          continue;
        }

        // TODO(zhitao): Convert this operation to non-speculative once we can
        // support that in the operator API.
        Try<vector<ResourceConversion>> _conversions =
          getResourceConversions(operation);

        if (_conversions.isError()) {
          drop(framework, operation, _conversions.error());
          continue;
        }

        CHECK_EQ(1u, _conversions->size());
        const Resources& consumed = _conversions->at(0).consumed;
        const Resources& converted = _conversions->at(0).converted;

        if (!remainingResources.contains(consumed)) {
          drop(
              framework,
              operation,
              "Invalid SHRINK_VOLUME operation: " +
              stringify(remainingResources) + " does not contain " +
              stringify(consumed));

          continue;
        }

        remainingResources -= consumed;
        resizedResources += converted;

        LOG(INFO) << "Processing SHRINK_VOLUME operation for volume "
                  << operation.shrink_volume().volume()
                  << " subtracting scalar value "
                  << operation.shrink_volume().subtract()
                  << " from framework "
                  << *framework << " on node " << *node;

        _apply(node, framework, operation);

        conversions.insert(
            conversions.end(),
            _conversions->begin(),
            _conversions->end());

        break;
      }

      case Offer::Operation::LAUNCH: {
        foreach (const TaskInfo& task, operation.launch().task_infos()) {
          vector<ActionObject> actionObjects{
              ActionObject::taskLaunch(task, framework->info)};

          // The framework-to-task principal pair is authorized only when
          // there is a pair; a task without a principal is refused by
          // validation below as TASK_INVALID (DR-037).
          if (task.has_principal()) {
            actionObjects.push_back(
                ActionObject::actForPrincipal(task, framework->info));
          }

          const Option<Error> authorizationError = authorized(actionObjects);

          if (authorizationError.isSome()) {
            const StatusUpdate& update = protobuf::createStatusUpdate(
                framework->id(),
                task.node_id(),
                task.task_id(),
                TASK_ERROR,
                TaskStatus::SOURCE_CONTROLLER,
                None(),
                authorizationError->message,
                TaskStatus::REASON_TASK_UNAUTHORIZED);

            metrics->tasks_error++;

            metrics->incrementTasksStates(
                TASK_ERROR,
                TaskStatus::SOURCE_CONTROLLER,
                TaskStatus::REASON_TASK_UNAUTHORIZED);

            forward(update, UPID(), framework);

            continue; // Continue to the next task.
          }

          // Validate the task.

          // We add back offered shared resources for validation even if they
          // are already consumed by other tasks in the same ACCEPT call. This
          // allows these tasks to use more copies of the same shared resource
          // than those being offered. e.g., 2 tasks can be launched on 1 copy
          // of a shared persistent volume from the offer; 3 tasks can be
          // launched on 2 copies of a shared persistent volume from 2 offers.
          Resources available =
            remainingResources.nonShared() + remainingSharedResources;

          Option<Error> error =
            validation::task::validate(task, framework, node, available);

          if (error.isSome()) {
            const StatusUpdate& update = protobuf::createStatusUpdate(
                framework->id(),
                task.node_id(),
                task.task_id(),
                TASK_ERROR,
                TaskStatus::SOURCE_CONTROLLER,
                None(),
                error->message,
                TaskStatus::REASON_TASK_INVALID);

            metrics->tasks_error++;

            metrics->incrementTasksStates(
                TASK_ERROR,
                TaskStatus::SOURCE_CONTROLLER,
                TaskStatus::REASON_TASK_INVALID);

            forward(update, UPID(), framework);

            continue; // Continue to the next task.
          }

          // Add task.
          {
            Resources consumed;

            bool launchExecutor = true;
            if (task.has_executor()) {
              launchExecutor = isLaunchExecutor(
                  task.executor().executor_id(), framework, node);

              // Controller tracks the new executor only if the task is not a
              // command task.
              if (launchExecutor) {
                addExecutor(task.executor(), framework, node);
                consumed += task.executor().resources();
              }
            }

            addTask(task, framework, node);
            consumed += task.resources();

            CHECK(available.contains(consumed))
              << available << " does not contain " << consumed;

            // Determine the additional instances of shared resources
            // needed to be added to the allocations since we support
            // tasks requesting more instances of shared resources
            // than those being offered.
            const Resources& consumedShared = consumed.shared();

            // Check that offered resources contain at least one copy
            // of each consumed shared resource (guaranteed by controller
            // validation).
            foreach (const Resource& resource, consumedShared) {
              CHECK(remainingSharedResources.contains(resource));
            }

            Resources additional = consumedShared - remainingResources.shared();
            if (!additional.empty()) {
              LOG(INFO) << "Allocating additional resources " << additional
                        << " for task " << task.task_id()
                        << " of framework " << *framework
                        << " on node " << *node;

              conversions.emplace_back(Resources(), additional);
            }

            remainingResources -= consumed;
            launchResources += consumed;

            RunTaskMessage message;
            message.mutable_framework()->MergeFrom(framework->info);

            hashmap<Option<ResourceProviderID>, UUID> resourceVersions;
            if (node->resourceVersion.isSome()) {
              resourceVersions.put(None(), node->resourceVersion.get());
            }

            foreachpair (
                const ResourceProviderID& resourceProviderId,
                const Node::ResourceProvider& resourceProvider,
                node->resourceProviders) {
              resourceVersions.put(
                  resourceProviderId, resourceProvider.resourceVersion);
            }

            message.mutable_resource_version_uuids()->CopyFrom(
                protobuf::createResourceVersions(resourceVersions));

            // TODO(anand): We set 'pid' to UPID() for http frameworks
            // as 'pid' was made optional in 0.24.0. In 0.25.0, we
            // no longer have to set pid here for http frameworks.
            message.set_pid(framework->pid().getOrElse(UPID()));
            message.mutable_task()->MergeFrom(task);

            message.set_launch_executor(launchExecutor);

            if (HookManager::hooksAvailable()) {
              // Set labels retrieved from label-decorator hooks.
              *message.mutable_task()->mutable_labels() =
                  HookManager::controllerLaunchTaskLabelDecorator(
                      task,
                      framework->info,
                      node->info);
            }

            // If the node does not support reservation refinement, downgrade
            // the task / executor resources to the "pre-reservation-refinement"
            // format. This cannot contain any refined reservations since
            // the controller rejects attempts to create refined reservations
            // on non-capable nodes.
            if (!node->capabilities.reservationRefinement) {
              CHECK_SOME(downgradeResources(&message));
            }

            LOG(INFO) << "Launching task " << task.task_id() << " of framework "
                      << *framework << " with resources " << task.resources()
                      << " on node " << *node << " on "
                      << (launchExecutor ?
                          " new executor" : " existing executor");

            // Increment this metric here for LAUNCH since it
            // does not make use of the `_apply()` function.
            framework->metrics.incrementOperation(operation);

            send(node->pid, message);
          }
        }

        break;
      }

      case Offer::Operation::LAUNCH_GROUP: {
        // We must ensure that the entire group can be launched. This
        // means all tasks in the group must be authorized and valid.
        const ExecutorInfo& executor = operation.launch_group().executor();
        const TaskGroupInfo& taskGroup = operation.launch_group().task_group();

        // Note that we do not fill in the `ExecutorInfo.framework_id`
        // since we do not have to support backwards compatibility like
        // in the `Launch` operation case.

        // TODO(bmahler): Consider injecting some default (cpus, mem, disk)
        // resources when the framework omits the executor resources.

        // See if there are any authorization or validation errors.
        // Note that we'll only report the first error we encounter
        // for the group.
        //
        // TODO(anindya_sinha): If task group uses shared resources, this
        // validation needs to be enhanced to accommodate multiple copies
        // of shared resources across tasks within the task group.
        Option<Error> error;
        Option<TaskStatus::Reason> reason;

        foreach (const TaskInfo& task, taskGroup.tasks()) {
          vector<ActionObject> actionObjects{
              ActionObject::taskLaunch(task, framework->info)};

          // As for LAUNCH: the pair is authorized only when there is one;
          // group validation below refuses a task without a principal.
          if (task.has_principal()) {
            actionObjects.push_back(
                ActionObject::actForPrincipal(task, framework->info));
          }

          foreach (const ActionObject& actionObject, actionObjects) {
            const Try<bool> approval = framework->approved(actionObject);

            if (approval.isError()) {
              error = Error("Failed to authorize task"
                            " '" + stringify(task.task_id()) + "'"
                            ": " + approval.error());
            } else if (!*approval) {
              error = Error("Task '" + stringify(task.task_id()) + "'"
                            " is not authorized to" + stringify(actionObject));
            }
          }
        }

        if (error.isSome()) {
          reason = TaskStatus::REASON_TASK_GROUP_UNAUTHORIZED;
        } else {
          error = validation::task::group::validate(
              taskGroup, executor, framework, node, remainingResources);

          if (error.isSome()) {
            reason = TaskStatus::REASON_TASK_GROUP_INVALID;
          }
        }

        if (error.isSome()) {
          CHECK_SOME(reason);
          foreach (const TaskInfo& task, taskGroup.tasks()) {
            const StatusUpdate& update = protobuf::createStatusUpdate(
                framework->id(),
                task.node_id(),
                task.task_id(),
                TASK_ERROR,
                TaskStatus::SOURCE_CONTROLLER,
                None(),
                error->message,
                reason.get());

            metrics->tasks_error++;

            metrics->incrementTasksStates(
                TASK_ERROR, TaskStatus::SOURCE_CONTROLLER, reason.get());

            forward(update, UPID(), framework);
          }

          continue;
        }

        // Now launch the task group!
        RunTaskGroupMessage message;
        message.mutable_framework()->CopyFrom(framework->info);
        message.mutable_executor()->CopyFrom(executor);
        message.mutable_task_group()->CopyFrom(taskGroup);

        hashmap<Option<ResourceProviderID>, UUID> resourceVersions;
        if (node->resourceVersion.isSome()) {
          resourceVersions.put(None(), node->resourceVersion.get());
        }

        foreachpair (
            const ResourceProviderID& resourceProviderId,
            const Node::ResourceProvider& resourceProvider,
            node->resourceProviders) {
          resourceVersions.put(
              resourceProviderId, resourceProvider.resourceVersion);
        }

        message.mutable_resource_version_uuids()->CopyFrom(
            protobuf::createResourceVersions(resourceVersions));

        set<TaskID> taskIds;
        Resources totalResources;
        Resources executorResources;

        bool launchExecutor =
          isLaunchExecutor(executor.executor_id(), framework, node);

        if (launchExecutor) {
          addExecutor(executor, framework, node);
          executorResources = executor.resources();
          totalResources += executorResources;
        }

        message.set_launch_executor(launchExecutor);

        foreach (
            TaskInfo& task, *message.mutable_task_group()->mutable_tasks()) {
          taskIds.insert(task.task_id());
          totalResources += task.resources();

          addTask(task, framework, node);

          if (HookManager::hooksAvailable()) {
            // Set labels retrieved from label-decorator hooks.
            *task.mutable_labels() =
                HookManager::controllerLaunchTaskLabelDecorator(
                    task,
                    framework->info,
                    node->info);
          }
        }

        CHECK(remainingResources.contains(totalResources))
          << remainingResources << " does not contain " << totalResources;

        remainingResources -= totalResources;
        launchResources += totalResources;

        // If the node does not support reservation refinement, downgrade
        // the task and executor resources to the "pre-reservation-refinement"
        // format. This cannot contain any refined reservations since
        // the controller rejects attempts to create refined reservations
        // on non-capable nodes.
        if (!node->capabilities.reservationRefinement) {
          CHECK_SOME(downgradeResources(&message));
        }

        LOG(INFO) << "Launching task group " << stringify(taskIds)
                  << " of framework " << *framework << " with resources "
                  << totalResources -  executorResources << " on node "
                  << *node << " on "
                  << (launchExecutor ? " new executor" : " existing executor");

        // Increment this metric here for LAUNCH_GROUP since it
        // does not make use of the `_apply()` function.
        framework->metrics.incrementOperation(operation);

        send(node->pid, message);

        break;
      }

      case Offer::Operation::CREATE_DISK: {
        if (!node->capabilities.resourceProvider) {
          drop(framework,
               operation,
               "Not supported on node " + stringify(*node) +
               " because it does not have RESOURCE_PROVIDER capability");
          continue;
        }

        Option<Error> error = validation::operation::validate(
            operation.create_disk());

        error = error.isSome()
          ? Error(error->message + "; on node " + stringify(*node))
          : authorized(CHECK_NOTERROR(
              ActionObject::createDisk(operation.create_disk())));

        if (error.isSome()) {
          drop(framework, operation, error->message);
          continue;
        }

        const Resource& consumed = operation.create_disk().source();

        if (!remainingResources.contains(consumed)) {
          drop(framework,
               operation,
               "Invalid CREATE_DISK Operation: " +
                 stringify(remainingResources) + " does not contain " +
                 stringify(consumed));
          continue;
        }

        remainingResources -= consumed;

        LOG(INFO) << "Processing CREATE_DISK operation with source "
                  << operation.create_disk().source() << " from framework "
                  << *framework << " to node " << *node;

        _apply(node, framework, operation);

        break;
      }

      case Offer::Operation::DESTROY_DISK: {
        if (!node->capabilities.resourceProvider) {
          drop(framework,
               operation,
               "Not supported on node " + stringify(*node) +
               " because it does not have RESOURCE_PROVIDER capability");
          continue;
        }

        Option<Error> error = validation::operation::validate(
            operation.destroy_disk());

        error = error.isSome()
          ? Error(error->message + "; on node " + stringify(*node))
          : authorized(CHECK_NOTERROR(
              ActionObject::destroyDisk(operation.destroy_disk())));


        if (error.isSome()) {
          drop(framework, operation, error->message);
          continue;
        }

        const Resource& consumed = operation.destroy_disk().source();

        if (!remainingResources.contains(consumed)) {
          drop(framework,
               operation,
               "Invalid DESTROY_DISK Operation: " +
                 stringify(remainingResources) + " does not contain " +
                 stringify(consumed));
          continue;
        }

        remainingResources -= consumed;

        LOG(INFO) << "Processing DESTROY_DISK operation for volume "
                  << operation.destroy_disk().source() << " from framework "
                  << *framework << " to node " << *node;

        _apply(node, framework, operation);

        break;
      }

      case Offer::Operation::UNKNOWN: {
        LOG(WARNING) << "Ignoring unknown operation";
        break;
      }
    }
  }

  // Update the allocator based on the operations.
  if (!conversions.empty()) {
    allocator->updateAllocation(
        frameworkId,
        nodeId,
        offeredResources,
        conversions);
  }

  // We now need to compute the amounts of remaining (1) speculatively converted
  // resources to recover without a filter and (2) resources that are implicitly
  // declined with the filter:
  //
  // Speculatively converted resources
  //   = (offered resources).apply(speculative operations)
  //       - resources consumed by non-speculative operations
  //       - offered resources not consumed by any operation
  //   = remaining resources - offered resources not consumed by any operation
  //   = remaining resources - offered resources
  //
  // (The last equality holds because resource subtraction yields no negatives.)
  //
  // Implicitly declined resources
  //   = (offered resources).apply(speculative operations)
  //       - resources consumed by non-speculative operations
  //       - speculatively converted resources
  //   = remaining resources - speculatively converted resources
  //
  // TODO(zhitao): Right now `GROW_VOLUME` and `SHRINK_VOLUME` are implemented
  // as speculative operations. Since the plan is to make them non-speculative
  // in the future, their results are not in the remaining resources, so we add
  // them back here. Remove this once the operations become non-speculative.
  Resources speculativelyConverted =
    remainingResources + resizedResources - offeredResources;
  Resources implicitlyDeclined = remainingResources - speculativelyConverted;

  // Prevent any allocations from occurring during resource recovery below.
  //
  // TODO(asekretenko): Ideally, we should be able to inform the allocator about
  // all the resource state transitions in one call. This will obviate the need
  // to pause/resume the allocator.
  allocator->pause();

  // Tell the allocator about the net speculatively converted resources. These
  // resources should not be implicitly declined.
  if (!speculativelyConverted.empty()) {
    allocator->recoverResources(
        frameworkId, nodeId, speculativelyConverted, None(), false);
  }

  // Tell the allocator about the implicitly declined resources.
  if (!implicitlyDeclined.empty()) {
    allocator->recoverResources(
        frameworkId, nodeId, implicitlyDeclined, accept.filters(), false);
  }

  if (!launchResources.empty()) {
    allocator->transitionOfferedToAllocated(nodeId, launchResources);
  }

  allocator->resume();
}


void Controller::acceptInverseOffers(
    Framework* framework,
    const scheduler::Call::AcceptInverseOffers& accept)
{
  CHECK_NOTNULL(framework);

  Option<Error> error;

  if (accept.inverse_offer_ids().size() == 0) {
    error = Error("No inverse offers specified");
  } else {
    LOG(INFO) << "Processing ACCEPT_INVERSE_OFFERS call for inverse offers: "
              << accept.inverse_offer_ids() << " for framework " << *framework;

    // Validate the inverse offers.
    error = validation::offer::validateInverseOffers(
        accept.inverse_offer_ids(),
        this,
        framework);

    // Update each inverse offer in the allocator with the accept and
    // filter.
    // TODO(anand): Notify the framework if some of the offers were invalid.
    foreach (const OfferID& offerId, accept.inverse_offer_ids()) {
      InverseOffer* inverseOffer = getInverseOffer(offerId);
      if (inverseOffer != nullptr) {
        medios::allocator::InverseOfferStatus status;
        status.set_status(medios::allocator::InverseOfferStatus::ACCEPT);
        status.mutable_framework_id()->CopyFrom(inverseOffer->framework_id());
        status.mutable_timestamp()->CopyFrom(protobuf::getCurrentTime());

        allocator->updateInverseOffer(
            inverseOffer->node_id(),
            inverseOffer->framework_id(),
            UnavailableResources{
                inverseOffer->resources(),
                inverseOffer->unavailability()},
            status,
            accept.filters());

        removeInverseOffer(inverseOffer);
        continue;
      }

      // If the offer was not in our inverse offer set, then this
      // offer is no longer valid.
      LOG(WARNING) << "Ignoring accept of inverse offer " << offerId
                   << " since it is no longer valid";
    }
  }

  if (error.isSome()) {
    LOG(WARNING) << "ACCEPT_INVERSE_OFFERS call used invalid offers '"
                 << accept.inverse_offer_ids() << "': " << error->message;
  }
}


void Controller::decline(
    Framework* framework,
    scheduler::Call::Decline&& decline)
{
  CHECK_NOTNULL(framework);

  LOG(INFO) << "Processing DECLINE call for offers: " << decline.offer_ids()
            << " for framework " << *framework << " with "
            << decline.filters().refuse_seconds() << " seconds filter";

  ++metrics->messages_decline_offers;

  size_t offersDeclined = 0;

  foreach (const OfferID& offerId, decline.offer_ids()) {
    Offer* offer = getOffer(offerId);
    if (offer != nullptr) {
      discardOffer(offer, decline.filters());
      offersDeclined++;
      continue;
    }

    // If the offer was not in our offer set, then this offer is no
    // longer valid.
    LOG(WARNING) << "Ignoring decline of offer " << offerId
                 << " since it is no longer valid";
  }

  framework->metrics.offers_declined += offersDeclined;
}


void Controller::declineInverseOffers(
    Framework* framework,
    const scheduler::Call::DeclineInverseOffers& decline)
{
  CHECK_NOTNULL(framework);

  LOG(INFO) << "Processing DECLINE_INVERSE_OFFERS call for inverse offers: "
            << decline.inverse_offer_ids() << " for framework " << *framework;

  // Update each inverse offer in the allocator with the decline and
  // filter.
  foreach (const OfferID& offerId, decline.inverse_offer_ids()) {
    // Try it as an inverse offer. If this fails then the offer is no
    // longer valid.
    InverseOffer* inverseOffer = getInverseOffer(offerId);
    if (inverseOffer != nullptr) { // If this is an inverse offer.
      medios::allocator::InverseOfferStatus status;
      status.set_status(medios::allocator::InverseOfferStatus::DECLINE);
      status.mutable_framework_id()->CopyFrom(inverseOffer->framework_id());
      status.mutable_timestamp()->CopyFrom(protobuf::getCurrentTime());

      allocator->updateInverseOffer(
          inverseOffer->node_id(),
          inverseOffer->framework_id(),
          UnavailableResources{
              inverseOffer->resources(),
              inverseOffer->unavailability()},
          status,
          decline.filters());

      removeInverseOffer(inverseOffer);
      continue;
    }

    // If the offer was not in our inverse offer set, then this
    // offer is no longer valid.
    LOG(WARNING) << "Ignoring decline of inverse offer " << offerId
                 << " since it is no longer valid";
  }
}


void Controller::checkAndTransitionDrainingNode(Node* node)
{
  CHECK_NOTNULL(node);

  const NodeID& nodeId = node->id;

  if (!nodes.draining.contains(nodeId) ||
      nodes.draining.at(nodeId).state() == DRAINED) {
    // Nothing to do for non-draining or already drained nodes.
    return;
  }

  // Check if the node has any tasks running or operations pending.
  if (!node->tasks.empty() ||
      !node->operations.empty()) {
    size_t numTasks = 0u;
    foreachvalue (const auto& frameworkTasks, node->tasks) {
      numTasks += frameworkTasks.size();
    }

    VLOG(1)
      << "DRAINING Node " << nodeId << " has "
      << numTasks << " tasks, and "
      << node->operations.size() << " operations";
    return;
  }

  if (nodes.markingGone.contains(nodeId)) {
    LOG(INFO)
      << "Ignoring transition of node " << nodeId << " to the DRAINED"
      << " state because node is being marked gone";
    return;
  }

  // If the node will be marked gone afterwards, we do not need to mark
  // the node as DRAINED. Simply marking gone will suffice.
  if (nodes.draining.at(nodeId).config().mark_gone()) {
    LOG(INFO) << "Marking node " << nodeId << " in the DRAINED state as gone";

    nodes.markingGone.insert(nodeId);

    TimeInfo goneTime = protobuf::getCurrentTime();

    registrar->apply(Owned<RegistryOperation>(
        new MarkNodeGone(nodeId, goneTime)))
      .onAny(defer(
          self(),
          [this, nodeId, goneTime](const Future<bool>& result) {
            CHECK_READY(result)
              << "Failed to mark node gone in the registry";

            markGone(nodeId, goneTime);
          }));
  } else {
    LOG(INFO) << "Transitioning node " << nodeId << " to the DRAINED state";

    registrar->apply(Owned<RegistryOperation>(
        new MarkNodeDrained(nodeId)))
      .onAny(defer(
          self(),
          [this, nodeId](const Future<bool>& result) {
            CHECK_READY(result)
              << "Failed to update draining info in the registry";

            // This can happen if the node sends an UnregisterNodeMessage
            // right before this method is called.
            if (!nodes.draining.contains(nodeId)) {
              LOG(INFO)
                << "Node " << nodeId << " was removed while being"
                << " marked as DRAINED";
              return;
            }

            nodes.draining[nodeId].set_state(DRAINED);

            LOG(INFO)
              << "Node " << nodeId << " successfully marked as DRAINED";
          }));
  }
}


void Controller::reviveOffers(
    const UPID& from,
    const FrameworkID& frameworkId,
    const vector<string>& roles)
{
  Framework* framework = getFramework(frameworkId);

  if (framework == nullptr) {
    LOG(WARNING)
      << "Ignoring revive offers message for framework " << frameworkId
      << " because the framework cannot be found";

    return;
  }

  if (framework->pid() != from) {
    LOG(WARNING)
      << "Ignoring revive offers message for framework " << *framework
      << " because it is not expected from " << from;

    return;
  }

  scheduler::Call::Revive call;
  foreach (const string& role, roles) {
    call.add_roles(role);
  }

  revive(framework, call);
}


void Controller::revive(
    Framework* framework,
    const scheduler::Call::Revive& revive)
{
  CHECK_NOTNULL(framework);

  LOG(INFO) << "Processing REVIVE call for framework " << *framework;

  ++metrics->messages_revive_offers;

  set<string> roles;

  // Validate the roles, if provided. We need to make sure the
  // roles is valid and also contained within the framework roles.
  // Note that if a single role is invalid, we drop the entire
  // call and do not suppress the valid roles.
  foreach (const string& role, revive.roles()) {
    Option<Error> roleError = roles::validate(role);
    if (roleError.isSome()) {
      drop(framework,
           revive,
           "revive role '" + role + "' is invalid: " + roleError->message);
      return;
    }

    if (framework->roles.count(role) == 0) {
      drop(framework,
           revive,
           "revive role '" + role + "' is not one"
           " of the frameworks's subscribed roles");
      return;
    }

    roles.insert(role);
  }

  allocator->reviveOffers(framework->id(), roles);
}


void Controller::killTask(
    const UPID& from,
    const FrameworkID& frameworkId,
    const TaskID& taskId)
{
  Framework* framework = getFramework(frameworkId);

  if (framework == nullptr) {
    LOG(WARNING)
      << "Ignoring kill task message for task " << taskId << " of framework "
      << frameworkId << " because the framework cannot be found";

    return;
  }

  if (framework->pid() != from) {
    LOG(WARNING)
      << "Ignoring kill task message for task " << taskId << " of framework "
      << *framework << " because it is not expected from " << from;

    return;
  }

  scheduler::Call::Kill call;
  call.mutable_task_id()->CopyFrom(taskId);
  // NOTE: Kill policy in kill task is not supported for schedulers
  // sending `KillTaskMessage` instead of `scheduler::Call::Kill`.

  kill(framework, call);
}


void Controller::kill(Framework* framework, const scheduler::Call::Kill& kill)
{
  CHECK_NOTNULL(framework);

  const TaskID& taskId = kill.task_id();
  const Option<NodeID> nodeId =
    kill.has_node_id() ? Option<NodeID>(kill.node_id()) : None();

  LOG(INFO) << "Processing KILL call for task '" << taskId << "'"
            << " of framework " << *framework;

  ++metrics->messages_kill_task;

  Task* task = framework->getTask(taskId);
  if (task == nullptr) {
    LOG(WARNING) << "Cannot kill task " << taskId
                 << " of framework " << *framework
                 << " because it is unknown; performing reconciliation";

    scheduler::Call::Reconcile message;
    scheduler::Call::Reconcile::Task* t = message.add_tasks();

    *t->mutable_task_id() = taskId;

    if (nodeId.isSome()) {
      *t->mutable_node_id() = nodeId.get();
    }

    reconcile(framework, std::move(message));
    return;
  }

  if (nodeId.isSome() && nodeId.get() != task->node_id()) {
    LOG(WARNING) << "Cannot kill task " << taskId << " of node "
                 << nodeId.get() << " of framework " << *framework
                 << " because it belongs to different node "
                 << task->node_id();

    // TODO(vinod): Return a "Bad Request" when using HTTP API.
    return;
  }

  Node* node = nodes.registered.get(task->node_id());
  CHECK(node != nullptr) << "Unknown node " << task->node_id();

  // We add the task to 'killedTasks' here because the node
  // might be partitioned or disconnected but the controller
  // doesn't know it yet.
  node->killedTasks.put(framework->id(), taskId);

  // NOTE: This task will be properly reconciled when the disconnected node
  // reregisters with the controller.
  // We send the KillTaskMessage even if we have already sent one, just in case
  // the previous one was dropped by the network but it didn't trigger a node
  // re-registration (and hence reconciliation).
  if (node->connected) {
    LOG(INFO) << "Telling node " << *node
              << " to kill task " << taskId
              << " of framework " << *framework;

    KillTaskMessage message;
    message.mutable_framework_id()->MergeFrom(framework->id());
    message.mutable_task_id()->MergeFrom(taskId);
    if (kill.has_kill_policy()) {
      message.mutable_kill_policy()->MergeFrom(kill.kill_policy());
    }

    send(node->pid, message);
  } else {
    LOG(WARNING) << "Cannot kill task " << taskId
                 << " of framework " << *framework
                 << " because the node " << *node << " is disconnected."
                 << " Kill will be retried if the node reregisters";
  }
}


void Controller::statusUpdateAcknowledgement(
    const UPID& from,
    StatusUpdateAcknowledgementMessage&& statusUpdateAcknowledgementMessage)
{
  const NodeID& nodeId =
    statusUpdateAcknowledgementMessage.node_id();
  const FrameworkID& frameworkId =
    statusUpdateAcknowledgementMessage.framework_id();
  const TaskID& taskId =
    statusUpdateAcknowledgementMessage.task_id();
  const string& uuid =
    statusUpdateAcknowledgementMessage.uuid();

  // TODO(bmahler): Consider adding a message validator abstraction
  // for the controller that takes care of all this boilerplate. Ideally
  // by the time we process messages in the critical controller code, we
  // can assume that they are valid. This will become especially
  // important as validation logic is moved out of the scheduler
  // driver and into the controller.

  Try<id::UUID> uuid_ = id::UUID::fromBytes(uuid);
  if (uuid_.isError()) {
    LOG(WARNING)
      << "Ignoring status update acknowledgement "
      << " for task " << taskId << " of framework " << frameworkId
      << " on node " << nodeId << " due to: " << uuid_.error();

    metrics->invalid_status_update_acknowledgements++;
    return;
  }

  Framework* framework = getFramework(frameworkId);

  if (framework == nullptr) {
    LOG(WARNING)
      << "Ignoring status update acknowledgement for status "
      << uuid_.get() << " of task " << taskId << " of framework "
      << frameworkId << " on node " << nodeId << " because the framework "
      << "cannot be found";

    metrics->invalid_status_update_acknowledgements++;
    return;
  }

  if (framework->pid() != from) {
    LOG(WARNING)
      << "Ignoring status update acknowledgement for status "
      << uuid_.get() << " of task " << taskId << " of framework "
      << *framework << " on node " << nodeId << " because it is not "
      << "expected from " << from;

    metrics->invalid_status_update_acknowledgements++;
    return;
  }

  scheduler::Call::Acknowledge message;

  *message.mutable_node_id() =
    std::move(*statusUpdateAcknowledgementMessage.mutable_node_id());
  *message.mutable_task_id() =
    std::move(*statusUpdateAcknowledgementMessage.mutable_task_id());
  *message.mutable_uuid() =
    std::move(*statusUpdateAcknowledgementMessage.mutable_uuid());

  acknowledge(framework, std::move(message));
}


void Controller::acknowledge(
    Framework* framework,
    scheduler::Call::Acknowledge&& acknowledge)
{
  CHECK_NOTNULL(framework);

  metrics->messages_status_update_acknowledgement++;

  const NodeID& nodeId = acknowledge.node_id();
  const TaskID& taskId = acknowledge.task_id();

  Try<id::UUID> uuid_ = id::UUID::fromBytes(acknowledge.uuid());
  CHECK_SOME(uuid_);
  const id::UUID uuid = uuid_.get();

  Node* node = nodes.registered.get(nodeId);

  if (node == nullptr) {
    LOG(WARNING)
      << "Cannot send status update acknowledgement for status " << uuid
      << " of task " << taskId << " of framework " << *framework
      << " to node " << nodeId << " because node is not registered";

    metrics->invalid_status_update_acknowledgements++;
    return;
  }

  if (!node->connected) {
    LOG(WARNING)
      << "Cannot send status update acknowledgement for status " << uuid
      << " of task " << taskId << " of framework " << *framework
      << " to node " << *node << " because node is disconnected";

    metrics->invalid_status_update_acknowledgements++;
    return;
  }

  LOG(INFO)
    << "Processing ACKNOWLEDGE call for status " << uuid
    << " for task " << taskId
    << " of framework " << *framework
    << " on node " << nodeId;

  Task* task = node->getTask(framework->id(), taskId);

  if (task != nullptr) {
    // Status update state and uuid should be either set or unset
    // together.
    CHECK_EQ(task->has_status_update_uuid(), task->has_status_update_state());

    if (!task->has_status_update_state()) {
      // Task should have status update state set because it must have
      // been set when the update corresponding to this
      // acknowledgement was processed by the controller. But in case this
      // acknowledgement was intended for the old run of the controller
      // and the task belongs to a 0.20.0 node, we could be here.
      // Dropping the acknowledgement is safe because the node will
      // retry the update, at which point the controller will set the
      // status update state.
      LOG(WARNING)
        << "Ignoring status update acknowledgement for status " << uuid
        << " of task " << taskId << " of framework " << *framework
        << " to node " << *node << " because the update was not"
        << " sent by this controller";

      metrics->invalid_status_update_acknowledgements++;
      return;
    }

    // Remove the task once the terminal update is acknowledged.
    if (protobuf::isTerminalState(task->status_update_state()) &&
        id::UUID::fromBytes(task->status_update_uuid()).get() == uuid) {
      removeTask(task);
    }
  }

  StatusUpdateAcknowledgementMessage message;
  *message.mutable_node_id() = std::move(*acknowledge.mutable_node_id());
  *message.mutable_framework_id() = framework->id();
  *message.mutable_task_id() = std::move(*acknowledge.mutable_task_id());
  *message.mutable_uuid() = std::move(*acknowledge.mutable_uuid());

  send(node->pid, message);

  metrics->valid_status_update_acknowledgements++;

  checkAndTransitionDrainingNode(node);
}


void Controller::acknowledgeOperationStatus(
    Framework* framework,
    scheduler::Call::AcknowledgeOperationStatus&& acknowledge)
{
  CHECK_NOTNULL(framework);

  metrics->messages_operation_status_update_acknowledgement++;

  const OperationID& operationId = acknowledge.operation_id();

  Try<id::UUID> statusUuid_ = id::UUID::fromBytes(acknowledge.uuid());

  CHECK_SOME(statusUuid_);
  const id::UUID statusUuid = statusUuid_.get();

  CHECK(acknowledge.has_node_id());
  const NodeID& nodeId = acknowledge.node_id();

  Node* node = nodes.registered.get(nodeId);
  if (node == nullptr) {
    LOG(WARNING)
      << "Cannot send operation status update acknowledgement for status "
      << statusUuid << " of operation '" << operationId << "'"
      << " of framework " << *framework << " to node " << nodeId
      << " because node is not registered";

    metrics->invalid_operation_status_update_acknowledgements++;
    return;
  }

  if (!node->connected) {
    LOG(WARNING)
      << "Cannot send operation status update acknowledgement for status "
      << statusUuid << " of operation '" << operationId << "'"
      << " of framework " << *framework << " to node " << nodeId
      << " because node is disconnected";

    metrics->invalid_operation_status_update_acknowledgements++;
    return;
  }

  if (acknowledge.has_resource_provider_id() &&
      !node->capabilities.resourceProvider) {
    LOG(WARNING)
      << "Cannot send operation status update acknowledgement for status "
      << statusUuid << " of operation '" << operationId << "'"
      << " of framework " << *framework << " to node " << nodeId
      << " because the node does not have the RESOURCE_PROVIDER"
      << " capability";

    metrics->invalid_operation_status_update_acknowledgements++;
    return;
  }

  if (!acknowledge.has_resource_provider_id() &&
      !node->capabilities.nodeOperationFeedback) {
    LOG(WARNING)
      << "Cannot send operation status update acknowledgement for status "
      << statusUuid << " of operation '" << operationId << "'"
      << " of framework " << *framework << " to node " << nodeId
      << " because the node does not support operation feedback"
      << " on node default resources";

    metrics->invalid_operation_status_update_acknowledgements++;
    return;
  }

  const Option<UUID> operationUuid_ =
    framework->operationUUIDs.get(operationId);

  if (operationUuid_.isNone()) {
    LOG(WARNING)
      << "Cannot send operation status update acknowledgement for status "
      << statusUuid << " of operation '" << operationId << "'"
      << " of framework" << *framework << " to node " << nodeId
      << " because the operation is unknown";

    metrics->invalid_operation_status_update_acknowledgements++;
    return;
  }
  const UUID operationUuid = operationUuid_.get();

  Operation* operation = node->getOperation(operationUuid);
  CHECK_NOTNULL(operation);

  auto it = std::find_if(
      operation->statuses().begin(),
      operation->statuses().end(),
      [&statusUuid](const OperationStatus& operationStatus) {
        return operationStatus.has_uuid() &&
               operationStatus.uuid().value() == statusUuid.toBytes();
      });

  if (it == operation->statuses().end()) {
    LOG(WARNING)
      << "Ignoring operation status acknowledgement for status " << statusUuid
      << " of operation '" << operationId << "'"
      << " (uuid " << operationUuid << ")"
      << " of framework" << *framework
      << " because the operation status is unknown";

    metrics->invalid_status_update_acknowledgements++;
    return;
  }

  const OperationStatus& acknowledgedStatus = *it;

  LOG(INFO) << "Processing ACKNOWLEDGE_OPERATION_STATUS call for status "
            << statusUuid << " of operation '" << operationId << "'"
            << " (uuid " << operationUuid << ")"
            << " of framework " << *framework << " on node " << nodeId;

  // If the acknowledged status update is terminal, remove the operation.
  if (protobuf::isTerminalState(acknowledgedStatus.state())) {
    removeOperation(operation);
  }

  AcknowledgeOperationStatusMessage message;
  message.mutable_status_uuid()->set_value(statusUuid.toBytes());
  *message.mutable_operation_uuid() = std::move(operationUuid);
  if (acknowledge.has_resource_provider_id()) {
    *message.mutable_resource_provider_id() =
      std::move(*acknowledge.mutable_resource_provider_id());
  }

  send(node->pid, message);

  metrics->valid_operation_status_update_acknowledgements++;

  checkAndTransitionDrainingNode(node);
}


void Controller::schedulerMessage(
    const UPID& from,
    FrameworkToExecutorMessage&& frameworkToExecutorMessage)
{
  const FrameworkID& frameworkId = frameworkToExecutorMessage.framework_id();
  const ExecutorID& executorId = frameworkToExecutorMessage.executor_id();

  Framework* framework = getFramework(frameworkId);

  if (framework == nullptr) {
    LOG(WARNING) << "Ignoring framework message"
                 << " for executor '" << executorId << "'"
                 << " of framework " << frameworkId
                 << " because the framework cannot be found";

    metrics->invalid_framework_to_executor_messages++;
    return;
  }

  if (framework->pid() != from) {
    LOG(WARNING)
      << "Ignoring framework message for executor '" << executorId
      << "' of framework " << *framework
      << " because it is not expected from " << from;

    metrics->invalid_framework_to_executor_messages++;
    return;
  }

  scheduler::Call::Message message_;
  *message_.mutable_node_id() =
    std::move(*frameworkToExecutorMessage.mutable_node_id());
  *message_.mutable_executor_id() =
    std::move(*frameworkToExecutorMessage.mutable_executor_id());
  *message_.mutable_data() =
    std::move(*frameworkToExecutorMessage.mutable_data());

  message(framework, std::move(message_));
}


void Controller::executorMessage(
    const UPID& from,
    ExecutorToFrameworkMessage&& executorToFrameworkMessage)
{
  const NodeID& nodeId = executorToFrameworkMessage.node_id();
  const FrameworkID& frameworkId = executorToFrameworkMessage.framework_id();
  const ExecutorID& executorId = executorToFrameworkMessage.executor_id();

  metrics->messages_executor_to_framework++;

  if (nodes.removed.get(nodeId).isSome()) {
    // If the node has been removed, drop the executor message. The
    // controller is no longer trying to health check this node; when the
    // node realizes it hasn't received any pings from the controller, it
    // will eventually try to reregister.
    LOG(WARNING) << "Ignoring executor message"
                 << " from executor" << " '" << executorId << "'"
                 << " of framework " << frameworkId
                 << " on removed node " << nodeId;

    metrics->invalid_executor_to_framework_messages++;
    return;
  }

  // The node should (re-)register with the controller before
  // forwarding executor messages.
  Node* node = nodes.registered.get(nodeId);

  if (node == nullptr) {
    LOG(WARNING) << "Ignoring executor message"
                 << " from executor '" << executorId << "'"
                 << " of framework " << frameworkId
                 << " on unknown node " << nodeId;

    metrics->invalid_executor_to_framework_messages++;
    return;
  }

  Framework* framework = getFramework(frameworkId);

  if (framework == nullptr) {
    LOG(WARNING) << "Not forwarding executor message"
                 << " for executor '" << executorId << "'"
                 << " of framework " << frameworkId
                 << " on node " << *node
                 << " because the framework is unknown";

    metrics->invalid_executor_to_framework_messages++;
    return;
  }

  if (!framework->connected()) {
    LOG(WARNING) << "Not forwarding executor message for executor '"
                 << executorId << "' of framework " << frameworkId
                 << " on node " << *node
                 << " because the framework is disconnected";

    metrics->invalid_executor_to_framework_messages++;
    return;
  }

  ExecutorToFrameworkMessage message;
  *message.mutable_node_id() =
    std::move(*executorToFrameworkMessage.mutable_node_id());
  *message.mutable_framework_id() =
    std::move(*executorToFrameworkMessage.mutable_framework_id());
  *message.mutable_executor_id() =
    std::move(*executorToFrameworkMessage.mutable_executor_id());
  *message.mutable_data() =
    std::move(*executorToFrameworkMessage.mutable_data());

  framework->send(message);

  metrics->valid_executor_to_framework_messages++;
}


void Controller::message(
    Framework* framework,
    scheduler::Call::Message&& message)
{
  CHECK_NOTNULL(framework);

  metrics->messages_framework_to_executor++;

  Node* node = nodes.registered.get(message.node_id());

  if (node == nullptr) {
    LOG(WARNING) << "Cannot send framework message for framework "
                 << *framework << " to node " << message.node_id()
                 << " because node is not registered";

    metrics->invalid_framework_to_executor_messages++;
    return;
  }

  if (!node->connected) {
    LOG(WARNING) << "Cannot send framework message for framework "
                 << *framework << " to node " << *node
                 << " because node is disconnected";

    metrics->invalid_framework_to_executor_messages++;
    return;
  }

  LOG(INFO) << "Processing MESSAGE call from framework "
            << *framework << " to node " << *node;

  FrameworkToExecutorMessage message_;
  *message_.mutable_node_id() = std::move(*message.mutable_node_id());
  *message_.mutable_framework_id() = framework->id();
  *message_.mutable_executor_id() = std::move(*message.mutable_executor_id());
  *message_.mutable_data() = std::move(*message.mutable_data());

  send(node->pid, message_);

  metrics->valid_framework_to_executor_messages++;
}


void Controller::registerNode(
    const UPID& from,
    RegisterNodeMessage&& registerNodeMessage)
{
  ++metrics->messages_register_node;

  if (authenticating.contains(from)) {
    LOG(INFO) << "Queuing up registration request from " << from
              << " because authentication is still in progress";

    authenticating[from]
      .onReady(defer(self(),
                     &Self::registerNode,
                     from,
                     std::move(registerNodeMessage)));
    return;
  }

  if (flags.authenticate_nodes && !authenticated.contains(from)) {
    // This could happen if another authentication request came
    // through before we are here or if a node tried to register
    // without authentication.
    LOG(WARNING) << "Refusing registration of node at " << from
                 << " because it is not authenticated";
    return;
  }

  Option<Error> error =
    validation::controller::message::registerNode(registerNodeMessage);

  if (error.isSome()) {
    LOG(WARNING) << "Dropping registration of node at " << from
                 << " because it sent an invalid registration: "
                 << error->message;

    return;
  }

  if (nodes.registering.contains(from)) {
    LOG(INFO) << "Ignoring register node message from " << from
              << " (" << registerNodeMessage.node().hostname()
              << ") as registration is already in progress";

    return;
  }

  LOG(INFO) << "Received register node message from " << from
            << " (" << registerNodeMessage.node().hostname() << ")";

  nodes.registering.insert(from);

  // Update all resources passed by the node to `POST_RESERVATION_REFINEMENT`
  // format. We do this as early as possible so that we only use a single
  // format inside controller, and downgrade again if necessary when they leave the
  // controller (e.g. when writing to the registry).
  upgradeResources(&registerNodeMessage);

  // Note that the principal may be empty if authentication is not
  // required. Also it is passed along because it may be removed from
  // `authenticated` while the authorization is pending.
  Option<Principal> principal = authenticated.contains(from)
      ? Principal(authenticated.at(from))
      : Option<Principal>::none();

  // Calling the `onAny` continuation below separately so we can move
  // `registerNodeMessage` without it being evaluated before it's used
  // by `authorizeNode`.
  Future<bool> authorization = authorize(
      principal, ActionObject::nodeRegistration(registerNodeMessage.node()));

  authorization
    .onAny(defer(self(),
                 &Self::_registerNode,
                 from,
                 std::move(registerNodeMessage),
                 principal,
                 lambda::_1));
}


void Controller::_registerNode(
    const UPID& pid,
    RegisterNodeMessage&& registerNodeMessage,
    const Option<Principal>& principal,
    const Future<bool>& authorized)
{
  CHECK(!authorized.isDiscarded());
  CHECK(nodes.registering.contains(pid));

  const NodeInfo& nodeInfo = registerNodeMessage.node();

  Option<string> authorizationError = None();

  if (authorized.isFailed()) {
    authorizationError = "Authorization failure: " + authorized.failure();
  } else if (!authorized.get()) {
    authorizationError =
      "Not authorized to register node providing resources "
      "'" + stringify(Resources(nodeInfo.resources())) + "' " +
      (principal.isSome()
       ? "with principal '" + stringify(principal.get()) + "'"
       : "without a principal");
  }

  if (authorizationError.isSome()) {
    LOG(WARNING) << "Refusing registration of node at " << pid
                 << " (" << nodeInfo.hostname() << ")"
                 << ": " << authorizationError.get();

    nodes.registering.erase(pid);
    return;
  }

  VLOG(1) << "Authorized registration of node at " << pid
          << " (" << nodeInfo.hostname() << ")";

  MachineID machineId;
  machineId.set_hostname(nodeInfo.hostname());
  machineId.set_ip(stringify(pid.address.ip));

  // Nodes are not allowed to register while the machine they are on is in
  // `DOWN` mode.
  if (machines.contains(machineId) &&
      machines[machineId].info.mode() == MachineInfo::DOWN) {
    LOG(WARNING) << "Refusing registration of node at " << pid
                 << " because the machine '" << machineId << "' that it is "
                 << "running on is `DOWN`";

    ShutdownMessage message;
    message.set_message("Machine is `DOWN`");
    send(pid, message);

    nodes.registering.erase(pid);
    return;
  }

  // Ignore registration attempts by nodes running old Medios versions.
  // We expect that the node's version is in SemVer format; if the
  // version cannot be parsed, the registration attempt is ignored.
  const string& version = registerNodeMessage.version();
  Try<Version> parsedVersion = Version::parse(version);

  if (parsedVersion.isError()) {
    LOG(WARNING) << "Failed to parse version '" << version << "'"
                 << " of node at " << pid << ": " << parsedVersion.error()
                 << "; ignoring node registration attempt";

    nodes.registering.erase(pid);
    return;
  } else if (parsedVersion.get() < MINIMUM_NODE_VERSION) {
    LOG(WARNING) << "Ignoring registration attempt from old node at "
                 << pid << ": node version is " << parsedVersion.get()
                 << ", minimum supported node version is "
                 << MINIMUM_NODE_VERSION;

    nodes.registering.erase(pid);
    return;
  }

  // If the node is configured with a domain but the controller is not,
  // we can't determine whether the node is remote. To be safe, we
  // don't allow the node to register. We don't shutdown the node so
  // that any tasks on the node can continue to run.
  //
  // TODO(neilc): Consider sending a warning to node (MESOS-7615).
  if (nodeInfo.has_domain() && !info_.has_domain()) {
    LOG(WARNING) << "Node at " << pid << " is configured with "
                 << "domain " << nodeInfo.domain() << " "
                 << "but the controller has no configured domain. "
                 << "Ignoring node registration attempt";

    nodes.registering.erase(pid);
    return;
  }

  // Don't allow nodes without domain if domains are required.
  // We don't shutdown the node to allow it to restart itself with
  // the correct domain and without losing tasks.
  if (flags.require_node_domain && !nodeInfo.has_domain()) {
    LOG(WARNING) << "Node at " << pid << " attempted to register without "
                 << "a domain, but this controller is configured to require node "
                 << "domains. Ignoring node registration attempt";

    nodes.registering.erase(pid);
    return;
  }

  // Check if this node is already registered (because it retries).
  if (Node* node = nodes.registered.get(pid)) {
    if (!node->connected) {
      // The node was previously disconnected but it is now trying
      // to register as a new node.
      // There are several possible reasons for this to happen:
      // - If the node failed recovery and hence registering as a new
      //   node before the controller removed the old node from its map.
      // - If the node was shutting down while it had a registration
      //   retry scheduled. See MESOS-8463.
      LOG(INFO) << "Removing old disconnected node " << *node
                << " because a registration attempt occurred";

      removeNode(node,
                  "a new node registered at the same address",
                  metrics->node_removals_reason_registered);
    } else {
      LOG(INFO) << "Node " << *node << " already registered,"
                << " resending acknowledgement";

      Duration pingTimeout =
        flags.node_ping_timeout * flags.max_node_ping_timeouts;
      ControllerNodeConnection connection;
      connection.set_total_ping_timeout_seconds(pingTimeout.secs());

      NodeRegisteredMessage message;
      message.mutable_node_id()->CopyFrom(node->id);
      message.mutable_connection()->CopyFrom(connection);
      send(pid, message);

      nodes.registering.erase(pid);
      return;
    }
  }

  // Create and add the node id.
  NodeID nodeId = newNodeId();

  LOG(INFO) << "Registering node at " << pid << " ("
            << nodeInfo.hostname() << ") with id " << nodeId;

  NodeInfo nodeInfo_ = nodeInfo;
  nodeInfo_.mutable_id()->CopyFrom(nodeId);

  registerNodeMessage.mutable_node()->mutable_id()->CopyFrom(nodeId);

  registrar->apply(Owned<RegistryOperation>(new AdmitNode(nodeInfo_)))
    .onAny(defer(self(),
                 &Self::__registerNode,
                 pid,
                 std::move(registerNodeMessage),
                 lambda::_1));
}


void Controller::__registerNode(
    const UPID& pid,
    RegisterNodeMessage&& registerNodeMessage,
    const Future<bool>& admit)
{
  CHECK(nodes.registering.contains(pid));

  CHECK(!admit.isDiscarded());

  const NodeInfo& nodeInfo = registerNodeMessage.node();

  if (admit.isFailed()) {
    LOG(FATAL) << "Failed to admit node " << nodeInfo.id() << " at " << pid
               << " (" << nodeInfo.hostname() << "): " << admit.failure();
  }

  if (!admit.get()) {
    // This should only happen if there is a nodeID collision, but that
    // is extremely unlikely in practice: nodeIDs are prefixed with the
    // controller ID, which is a randomly generated UUID. In this situation,
    // we ignore the registration attempt. The node will eventually try
    // to register again and be assigned a new node ID.
    LOG(WARNING) << "Node " << nodeInfo.id() << " at " << pid
                 << " (" << nodeInfo.hostname() << ") was assigned"
                 << " a node ID that already appears in the registry;"
                 << " ignoring registration attempt";

    nodes.registering.erase(pid);
    return;
  }

  VLOG(1) << "Admitted node " << nodeInfo.id() << " at " << pid
          << " (" << nodeInfo.hostname() << ")";

  MachineID machineId;
  machineId.set_hostname(nodeInfo.hostname());
  machineId.set_ip(stringify(pid.address.ip));

  vector<NodeInfo::Capability> nodeCapabilities = google::protobuf::convert(
      std::move(*registerNodeMessage.mutable_node_capabilities()));
  vector<Resource> checkpointedResources = google::protobuf::convert(
      std::move(*registerNodeMessage.mutable_checkpointed_resources()));

  Option<UUID> resourceVersion;
  if (registerNodeMessage.has_resource_version_uuid()) {
    resourceVersion = registerNodeMessage.resource_version_uuid();
  }

  Node* node = new Node(
      this,
      nodeInfo,
      pid,
      machineId,
      registerNodeMessage.version(),
      std::move(nodeCapabilities),
      Clock::now(),
      std::move(checkpointedResources),
      resourceVersion);

  ++metrics->node_registrations;

  addNode(node, {});

  Duration pingTimeout =
    flags.node_ping_timeout * flags.max_node_ping_timeouts;
  ControllerNodeConnection connection;
  connection.set_total_ping_timeout_seconds(pingTimeout.secs());

  NodeRegisteredMessage message;
  message.mutable_node_id()->CopyFrom(node->id);
  message.mutable_connection()->CopyFrom(connection);
  send(node->pid, message);

  // Note that we convert to `Resources` for output as it's faster than
  // logging raw protobuf data. Conversion is safe, as resources have
  // already passed validation.
  LOG(INFO) << "Registered node " << *node
            << " with " << Resources(node->info.resources());

  nodes.registering.erase(pid);
}


void Controller::reregisterNode(
    const UPID& from,
    ReregisterNodeMessage&& reregisterNodeMessage)
{
  ++metrics->messages_reregister_node;

  if (authenticating.contains(from)) {
    LOG(INFO) << "Queuing up re-registration request from " << from
              << " because authentication is still in progress";

    authenticating[from]
      .onReady(defer(self(),
                     &Self::reregisterNode,
                     from,
                     std::move(reregisterNodeMessage)));
    return;
  }

  if (flags.authenticate_nodes && !authenticated.contains(from)) {
    // This could happen if another authentication request came
    // through before we are here or if a node tried to
    // reregister without authentication.
    LOG(WARNING) << "Refusing re-registration of node at " << from
                 << " because it is not authenticated";
    return;
  }

  // TODO(bevers): Technically this behaviour seems to be incorrect, since we
  // discard the newer re-registration attempt, which might have additional
  // capabilities or a higher version (or a changed NodeInfo, after Medios 1.5).
  // However, this should very rarely happen in practice, and nobody seems to
  // have complained about it so far.
  const NodeInfo& nodeInfo = reregisterNodeMessage.node();
  if (nodes.reregistering.contains(nodeInfo.id())) {
    LOG(INFO)
      << "Ignoring reregister node message from node "
      << nodeInfo.id() << " at " << from << " ("
      << nodeInfo.hostname() << ") as re-registration is already in progress";

    return;
  }

  if (nodes.markingUnreachable.contains(nodeInfo.id())) {
    LOG(INFO)
      << "Ignoring reregister node message from node "
      << nodeInfo.id() << " at " << from << " ("
      << nodeInfo.hostname() << ") as a mark unreachable operation is "
      << "already in progress";

    return;
  }

  if (nodes.markingGone.contains(nodeInfo.id())) {
    LOG(INFO)
      << "Ignoring reregister node message from node "
      << nodeInfo.id() << " at " << from << " ("
      << nodeInfo.hostname() << ") as a gone operation is already in progress";

    return;
  }

  if (nodes.gone.contains(nodeInfo.id())) {
    LOG(WARNING) << "Refusing re-registration of node at " << from
                 << " because it is already marked gone";

    ShutdownMessage message;
    message.set_message("Node has been marked gone");
    send(from, message);
    return;
  }

  Option<Error> error =
    validation::controller::message::reregisterNode(reregisterNodeMessage);

  if (error.isSome()) {
    LOG(WARNING) << "Dropping re-registration of node at " << from
                 << " because it sent an invalid re-registration: "
                 << error->message;

    return;
  }

  LOG(INFO) << "Received reregister node message from node "
            << nodeInfo.id() << " at " << from << " ("
            << nodeInfo.hostname() << ")";

  // TODO(bevers): Create a guard object calling `insert()` in its constructor
  // and `erase()` in its destructor, to avoid the manual bookkeeping.
  nodes.reregistering.insert(nodeInfo.id());

  // Update all resources passed by the node to `POST_RESERVATION_REFINEMENT`
  // format. We do this as early as possible so that we only use a single
  // format inside controller, and downgrade again if necessary when they leave the
  // controller (e.g. when writing to the registry).
  upgradeResources(&reregisterNodeMessage);

  // Note that the principal may be empty if authentication is not
  // required. Also it is passed along because it may be removed from
  // `authenticated` while the authorization is pending.
  Option<Principal> principal = authenticated.contains(from)
      ? Principal(authenticated.at(from))
      : Option<Principal>::none();

  // Calling the `onAny` continuation below separately so we can move
  // `reregisterNodeMessage` without it being evaluated before it's used
  // by `authorizeNode`.
  Future<bool> authorization = authorize(
      principal,
      ActionObject::nodeRegistration(reregisterNodeMessage.node()));

  authorization
    .onAny(defer(self(),
                 &Self::_reregisterNode,
                 from,
                 std::move(reregisterNodeMessage),
                 principal,
                 lambda::_1));
}


void Controller::_reregisterNode(
    const UPID& pid,
    ReregisterNodeMessage&& reregisterNodeMessage,
    const Option<Principal>& principal,
    const Future<bool>& authorized)
{
  CHECK(!authorized.isDiscarded());

  const NodeInfo& nodeInfo = reregisterNodeMessage.node();
  CHECK(nodes.reregistering.contains(nodeInfo.id()));

  Option<string> authorizationError = None();

  if (authorized.isFailed()) {
    authorizationError = "Authorization failure: " + authorized.failure();
  } else if (!authorized.get()) {
    authorizationError =
      "Not authorized to reregister node providing resources "
      "'" + stringify(Resources(nodeInfo.resources())) + "' " +
      (principal.isSome()
       ? "with principal '" + stringify(principal.get()) + "'"
       : "without a principal");
  }

  if (authorizationError.isSome()) {
    LOG(WARNING) << "Refusing re-registration of node " << nodeInfo.id()
                 << " at " << pid << " (" << nodeInfo.hostname() << ")"
                 << ": " << authorizationError.get();

    nodes.reregistering.erase(nodeInfo.id());
    return;
  }

  if (nodes.markingGone.contains(nodeInfo.id())) {
    LOG(INFO)
      << "Ignoring reregister node message from node "
      << nodeInfo.id() << " at " << pid << " ("
      << nodeInfo.hostname() << ") as a gone operation is already in progress";

    nodes.reregistering.erase(nodeInfo.id());
    return;
  }

  if (nodes.gone.contains(nodeInfo.id())) {
    LOG(WARNING) << "Refusing re-registration of node at " << pid
                 << " because it is already marked gone";

    ShutdownMessage message;
    message.set_message("Node has been marked gone");
    send(pid, message);

    nodes.reregistering.erase(nodeInfo.id());
    return;
  }

  VLOG(1) << "Authorized re-registration of node " << nodeInfo.id()
          << " at " << pid << " (" << nodeInfo.hostname() << ")";

  MachineID machineId;
  machineId.set_hostname(nodeInfo.hostname());
  machineId.set_ip(stringify(pid.address.ip));

  // Nodes are not allowed to reregister while the machine they are on is in
  // 'DOWN` mode.
  if (machines.contains(machineId) &&
      machines[machineId].info.mode() == MachineInfo::DOWN) {
    LOG(WARNING) << "Refusing re-registration of node at " << pid
                 << " because the machine '" << machineId << "' that it is "
                 << "running on is `DOWN`";

    ShutdownMessage message;
    message.set_message("Machine is `DOWN`");
    send(pid, message);

    nodes.reregistering.erase(nodeInfo.id());
    return;
  }

  // Ignore re-registration attempts by nodes running old Medios versions.
  // We expect that the node's version is in SemVer format; if the
  // version cannot be parsed, the re-registration attempt is ignored.
  const string& version = reregisterNodeMessage.version();
  Try<Version> parsedVersion = Version::parse(version);

  if (parsedVersion.isError()) {
    LOG(WARNING) << "Failed to parse version '" << version << "'"
                 << " of node at " << pid << ": " << parsedVersion.error()
                 << "; ignoring node re-registration attempt";

    nodes.reregistering.erase(nodeInfo.id());
    return;
  } else if (parsedVersion.get() < MINIMUM_NODE_VERSION) {
    LOG(WARNING) << "Ignoring re-registration attempt from old node at "
                 << pid << ": node version is " << parsedVersion.get()
                 << ", minimum supported node version is "
                 << MINIMUM_NODE_VERSION;

    nodes.reregistering.erase(nodeInfo.id());
    return;
  }

  // If the node is configured with a domain but the controller is not,
  // we can't determine whether the node is remote. To be safe, we
  // don't allow the node to reregister. We don't shutdown the node
  // so that any tasks on the node can continue to run.
  //
  // TODO(neilc): Consider sending a warning to node (MESOS-7615).
  if (nodeInfo.has_domain() && !info_.has_domain()) {
    LOG(WARNING) << "Node at " << pid << " is configured with "
                 << "domain " << nodeInfo.domain() << " "
                 << "but the controller has no configured domain."
                 << "Ignoring node re-registration attempt";

    nodes.reregistering.erase(nodeInfo.id());
    return;
  }

  // Don't allow nodes without domain if domains are required.
  // We don't shutdown the node to allow it to restart itself with
  // the correct domain and without losing tasks.
  if (flags.require_node_domain && !nodeInfo.has_domain()) {
    LOG(WARNING) << "Node at " << pid << " attempted to register without "
                 << "a domain, but this controller is configured to require node "
                 << "domains. Ignoring node re-registration attempt";

    nodes.reregistering.erase(nodeInfo.id());
    return;
  }

  if (Node* node = nodes.registered.get(nodeInfo.id())) {
    CHECK(!nodes.recovered.contains(nodeInfo.id()));

    // NOTE: This handles the case where a node tries to
    // reregister with an existing controller (e.g. because of a
    // spurious Zookeeper session expiration or after the node
    // recovers after a restart).
    // For now, we assume this node is not nefarious (eventually
    // this will be handled by orthogonal security measures like key
    // based authentication).
    VLOG(1) << "Node is already marked as registered: " << nodeInfo.id()
            << " at " << pid << " (" << nodeInfo.hostname() << ")";

    // We don't allow reregistering this way with a different IP or
    // hostname. This is because maintenance is scheduled at the
    // machine level; so we would need to re-validate the node's
    // unavailability if the machine it is running on changed.
    if (node->pid.address.ip != pid.address.ip ||
        node->info.hostname() != nodeInfo.hostname()) {
      LOG(WARNING) << "Node " << nodeInfo.id() << " at " << pid
                   << " (" << nodeInfo.hostname() << ") attempted to "
                   << "reregister with different IP / hostname; expected "
                   << node->pid.address.ip << " (" << node->info.hostname()
                   << ") shutting it down";

      ShutdownMessage message;
      message.set_message(
          "Node attempted to reregister with different IP / hostname");

      send(pid, message);

      nodes.reregistering.erase(nodeInfo.id());
      return;
    }

    // If this node has been downgraded from NODE_OPERATION_FEEDBACK-capable
    // to a version which does not have this capability, then we clean up
    // terminal-but-unACKed operations on node default resources.
    vector<NodeInfo::Capability> nodeCapabilities_ =
      google::protobuf::convert(reregisterNodeMessage.node_capabilities());

    protobuf::node::Capabilities nodeCapabilities(nodeCapabilities_);

    if (!nodeCapabilities.nodeOperationFeedback &&
        node->capabilities.nodeOperationFeedback) {
      foreachvalue (Operation* operation, utils::copy(node->operations)) {
        if (!operation->latest_status().has_resource_provider_id() &&
            operation->info().has_id() &&
            protobuf::isTerminalState(operation->latest_status().state())) {
          removeOperation(operation);
        }
      }
    }

    // Skip updating the registry if `nodeInfo` did not change from its
    // previously known state.
    if (nodeInfo == node->info) {
      ___reregisterNode(
          pid,
          std::move(reregisterNodeMessage),
          true);
    } else {
      registrar->apply(Owned<RegistryOperation>(new UpdateNode(nodeInfo)))
        .onAny(defer(self(),
            &Self::___reregisterNode,
            pid,
            std::move(reregisterNodeMessage),
            lambda::_1));
    }
  } else if (nodes.recovered.contains(nodeInfo.id())) {
    // The node likely is reregistering after a controller failover as it
    // is in the list recovered from the registry.
    VLOG(1) << "Re-admitting recovered node " << nodeInfo.id()
            << " at " << pid << "(" << nodeInfo.hostname() << ")";

    NodeInfo recoveredInfo = nodes.recovered.at(nodeInfo.id());

    // Skip updating the registry if `nodeInfo` did not change from its
    // previously known state (see also MESOS-7711).
    if (nodeInfo == recoveredInfo) {
      __reregisterNode(
          pid,
          std::move(reregisterNodeMessage),
          true);
    } else {
      registrar->apply(Owned<RegistryOperation>(new UpdateNode(nodeInfo)))
        .onAny(defer(self(),
            &Self::__reregisterNode,
            pid,
            std::move(reregisterNodeMessage),
            lambda::_1));
    }
  } else {
    // In the common case, the node has been marked unreachable
    // by the controller, so we move the node to the reachable list and
    // readmit it. If the node isn't in the unreachable list (which
    // might occur if the node's entry in the unreachable list is
    // GC'd), we admit the node anyway.
    VLOG(1) << "Consulting registry about node " << nodeInfo.id()
            << " at " << pid << "(" << nodeInfo.hostname() << ")";

    registrar->apply(Owned<RegistryOperation>(
        new MarkNodeReachable(nodeInfo)))
      .onAny(defer(self(),
          &Self::__reregisterNode,
          pid,
          std::move(reregisterNodeMessage),
          lambda::_1));
  }
}


void Controller::__reregisterNode(
    const UPID& pid,
    ReregisterNodeMessage&& reregisterNodeMessage,
    const Future<bool>& future)
{
  const NodeInfo& nodeInfo = reregisterNodeMessage.node();
  CHECK(nodes.reregistering.contains(nodeInfo.id()));

  if (future.isFailed()) {
    LOG(FATAL) << "Failed to update registry for node " << nodeInfo.id()
               << " at " << pid << " (" << nodeInfo.hostname() << "): "
               << future.failure();
  }

  CHECK(!future.isDiscarded());

  // Neither the `UpdateNode` nor `MarkNodeReachable` registry operations
  // should ever fail.
  CHECK(future.get());

  if (nodes.markingGone.contains(nodeInfo.id())) {
    LOG(INFO)
      << "Ignoring reregister node message from node "
      << nodeInfo.id() << " at " << pid << " ("
      << nodeInfo.hostname() << ") as a gone operation is already in progress";

    nodes.reregistering.erase(nodeInfo.id());
    return;
  }

  if (nodes.gone.contains(nodeInfo.id())) {
    LOG(WARNING) << "Refusing re-registration of node at " << pid
                 << " because it is already marked gone";

    ShutdownMessage message;
    message.set_message("Node has been marked gone");
    send(pid, message);

    nodes.reregistering.erase(nodeInfo.id());
    return;
  }

  VLOG(1) << "Re-admitted node " << nodeInfo.id() << " at " << pid
          << " (" << nodeInfo.hostname() << ")";

  // For nodes without the MULTI_ROLE capability,
  // we need to inject the allocation role inside
  // the task and executor resources;
  auto injectAllocationInfo = [](
      RepeatedPtrField<Resource>* resources,
      const FrameworkInfo& frameworkInfo)
  {
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

  vector<NodeInfo::Capability> nodeCapabilities =
    google::protobuf::convert(reregisterNodeMessage.node_capabilities());

  // Adjust the node's task and executor infos to ensure
  // compatibility with old nodes without certain capabilities.
  protobuf::node::Capabilities capabilities(nodeCapabilities);

  // If the node is not multi-role capable, inject allocation info.
  if (!capabilities.multiRole) {
    hashmap<FrameworkID, reference_wrapper<const FrameworkInfo>> frameworks;

    foreach (const FrameworkInfo& framework,
             reregisterNodeMessage.frameworks()) {
      frameworks.emplace(framework.id(), framework);
    }

    foreach (Task& task, *reregisterNodeMessage.mutable_tasks()) {
      CHECK(frameworks.contains(task.framework_id()));

      injectAllocationInfo(
          task.mutable_resources(),
          frameworks.at(task.framework_id()));
    }

    foreach (ExecutorInfo& executor,
             *reregisterNodeMessage.mutable_executor_infos()) {
      CHECK(frameworks.contains(executor.framework_id()));

      injectAllocationInfo(
          executor.mutable_resources(),
          frameworks.at(executor.framework_id()));
    }
  }

  MachineID machineId;
  machineId.set_hostname(nodeInfo.hostname());
  machineId.set_ip(stringify(pid.address.ip));

  // For easy lookup, first determine the set of FrameworkIDs on the
  // reregistering node that are partition-aware.
  hashset<FrameworkID> partitionAwareFrameworks;

  foreach (const FrameworkInfo& framework,
           reregisterNodeMessage.frameworks()) {
    if (protobuf::frameworkHasCapability(
            framework, FrameworkInfo::Capability::PARTITION_AWARE)) {
      partitionAwareFrameworks.insert(framework.id());
    }
  }

  // All tasks except the ones from completed frameworks are re-added to the
  // controller (those tasks were previously marked "unreachable", so they
  // should be removed from that collection).
  vector<Task> recoveredTasks;
  foreach (Task& task, *reregisterNodeMessage.mutable_tasks()) {
    // A task record arriving from a node has not passed through the
    // status-update path that validates a progress report, so a value this
    // controller would have refused can enter here and then be served from
    // `/state` and carried on reconciliation. Replace any such value with
    // the last report on the record that passes.
    common::validation::sanitizeTaskProgress(&task);

    // A finish outcome in such a record has the same property; a malformed
    // one is cleared, not replaced, and counted (DR-042).
    metrics->invalid_task_finish_outcomes += static_cast<int64_t>(
        common::validation::sanitizeTaskFinish(&task));

    const FrameworkID& frameworkId = task.framework_id();

    // Don't re-add tasks whose framework has been shutdown at the
    // controller. Such frameworks will be shutdown on the node below.
    if (isCompletedFramework(frameworkId)) {
      continue;
    }

    if (!nodes.recovered.contains(nodeInfo.id())) {
      Framework* framework = getFramework(frameworkId);
      if (framework != nullptr) {
        framework->unreachableTasks.erase(task.task_id());

        // The controller transitions task to terminal state on its own in certain
        // scenarios (e.g., framework or node teardown) before instructing the
        // node to remove it. However, we are not guaranteed that the message
        // reaches the node and is processed by it. If the node fails to act
        // on the message, tasks the controller has declared terminal might reappear
        // from the node as non-terminal, see e.g., MESOS-9940.
        //
        // Avoid tracking a task as both terminal and non-terminal by
        // garbage-collected completed tasks which come back as running.
        framework->completedTasks.erase(
            std::remove_if(
                framework->completedTasks.begin(),
                framework->completedTasks.end(),
                [&](const Owned<Task>& task_) {
                  return task_.get() && task_->task_id() == task.task_id();
                }),
            framework->completedTasks.end());
      }

      const string message = nodes.unreachable.contains(nodeInfo.id())
          ? "Unreachable node re-reregistered"
          : "Unknown node reregistered";

      const StatusUpdate& update = protobuf::createStatusUpdate(
          task.framework_id(),
          task.node_id(),
          task.task_id(),
          task.state(),
          TaskStatus::SOURCE_CONTROLLER,
          None(),
          message,
          TaskStatus::REASON_NODE_REREGISTERED,
          (task.has_executor_id()
              ? Option<ExecutorID>(task.executor_id()) : None()),
          protobuf::getTaskHealth(task),
          protobuf::getTaskCheckStatus(task),
          None(),  // `labels`.
          protobuf::getTaskContainerStatus(task),
          None(),  // `unreachable_time`.
          None(),  // `limitation`.
          protobuf::getTaskProgress(task));

      if (framework == nullptr || !framework->connected()) {
        LOG(WARNING) << "Dropping update " << update
                     << (update.status().has_message()
                         ? " '" + update.status().message() + "'"
                         : "")
                     << " for "
                     << (framework == nullptr ? "unknown" : "disconnected")
                     << " framework " << frameworkId;
      } else {
        forward(update, UPID(), framework);
      }
    }

    recoveredTasks.push_back(std::move(task));
  }

  // All tasks from this node are now reachable so clean them up from
  // the controller's unreachable task records.
  if (nodes.unreachableTasks.contains(nodeInfo.id())) {
    foreachkey (const FrameworkID& frameworkId,
                nodes.unreachableTasks.at(nodeInfo.id())) {
      Framework* framework = getFramework(frameworkId);
      if (framework != nullptr) {
        foreach (const TaskID& taskId,
                 nodes.unreachableTasks.at(nodeInfo.id()).at(frameworkId)) {
          framework->unreachableTasks.erase(taskId);
        }
      }
    }
  }

  nodes.unreachableTasks.erase(nodeInfo.id());

  vector<Resource> checkpointedResources = google::protobuf::convert(
      std::move(*reregisterNodeMessage.mutable_checkpointed_resources()));
  vector<ExecutorInfo> executorInfos = google::protobuf::convert(
      std::move(*reregisterNodeMessage.mutable_executor_infos()));

  Option<UUID> resourceVersion;
  if (reregisterNodeMessage.has_resource_version_uuid()) {
    resourceVersion = reregisterNodeMessage.resource_version_uuid();
  }

  nodes.recovered.erase(nodeInfo.id());

  Node* node = new Node(
      this,
      nodeInfo,
      pid,
      machineId,
      reregisterNodeMessage.version(),
      std::move(nodeCapabilities),
      Clock::now(),
      std::move(checkpointedResources),
      resourceVersion,
      std::move(executorInfos),
      std::move(recoveredTasks));

  node->reregisteredTime = Clock::now();

  ++metrics->node_reregistrations;

  nodes.removed.erase(node->id);
  nodes.unreachable.erase(node->id);

  vector<Archive::Framework> completedFrameworks = google::protobuf::convert(
      std::move(*reregisterNodeMessage.mutable_completed_frameworks()));

  addNode(node, std::move(completedFrameworks));

  // If this node was deactivated, make sure to deactivate it again,
  // now that it has reregistered.
  if (nodes.deactivated.contains(nodeInfo.id())) {
    deactivate(node);
  }

  // If this is a draining node, send it the drain message.
  // We do this regardless of the draining state (DRAINING or DRAINED),
  // because the node is expected to handle the message in either state.
  if (nodes.draining.contains(nodeInfo.id())) {
    DrainNodeMessage message;
    message.mutable_config()->CopyFrom(
        nodes.draining.at(nodeInfo.id()).config());

    send(node->pid, message);

    // NOTE: If the node supports resource providers, the node will not
    // report pending operations until the node's first UpdateNodeMessage.
    // Therefore, we cannot check the node's drain state here.
    if (!capabilities.resourceProvider) {
      checkAndTransitionDrainingNode(node);
    }
  }

  Duration pingTimeout =
    flags.node_ping_timeout * flags.max_node_ping_timeouts;
  ControllerNodeConnection connection;
  connection.set_total_ping_timeout_seconds(pingTimeout.secs());

  NodeReregisteredMessage message;
  message.mutable_node_id()->CopyFrom(node->id);
  message.mutable_connection()->CopyFrom(connection);
  send(node->pid, message);

  // Note that we convert to `Resources` for output as it's faster than
  // logging raw protobuf data. Conversion is safe, as resources have
  // already passed validation.
  LOG(INFO) << "Re-registered node " << *node
            << " with " << Resources(node->info.resources());

  // Any framework that is completed at the controller but still running
  // at the node is shutdown. This can occur if the framework was
  // removed when the node was partitioned. NOTE: This is just a
  // short-term hack because information about completed frameworks is
  // lost when the controller fails over. Also, we only store a limited
  // number of completed frameworks. A proper fix likely involves
  // storing framework information in the registry (MESOS-1719).
  foreach (const FrameworkInfo& framework,
           reregisterNodeMessage.frameworks()) {
    if (isCompletedFramework(framework.id())) {
      LOG(INFO) << "Shutting down framework " << framework.id()
                << " at reregistered node " << *node
                << " because the framework has been shutdown at the controller";

      ShutdownFrameworkMessage message;
      message.mutable_framework_id()->MergeFrom(framework.id());
      send(node->pid, message);
    }
  }

  // TODO(bmahler): Consider moving this in to `updateNodeFrameworks`,
  // would be helpful when there are a large total number of frameworks
  // in the cluster.
  const vector<FrameworkInfo> frameworks = google::protobuf::convert(
      std::move(*reregisterNodeMessage.mutable_frameworks()));

  updateNodeFrameworks(node, frameworks);

  nodes.reregistering.erase(nodeInfo.id());
}


void Controller::___reregisterNode(
    const process::UPID& pid,
    ReregisterNodeMessage&& reregisterNodeMessage,
    const process::Future<bool>& updated)
{
  const NodeInfo& nodeInfo = reregisterNodeMessage.node();
  CHECK(nodes.reregistering.contains(nodeInfo.id()));

  CHECK_READY(updated);
  CHECK(updated.get());

  VLOG(1) << "Registry updated for node " << nodeInfo.id() << " at " << pid
          << "(" << nodeInfo.hostname() << ")";

  if (nodes.markingGone.contains(nodeInfo.id())) {
    LOG(INFO)
      << "Ignoring reregister node message from node "
      << nodeInfo.id() << " at " << pid << " ("
      << nodeInfo.hostname() << ") as a gone operation is already in progress";

    nodes.reregistering.erase(nodeInfo.id());
    return;
  }

  if (nodes.gone.contains(nodeInfo.id())) {
    LOG(WARNING) << "Refusing re-registration of node at " << pid
                 << " because it is already marked gone";

    ShutdownMessage message;
    message.set_message("Node has been marked gone");
    send(pid, message);

    nodes.reregistering.erase(nodeInfo.id());
    return;
  }

  if (!nodes.registered.contains(nodeInfo.id())) {
    LOG(WARNING)
      << "Dropping ongoing re-registration attempt of node " << nodeInfo.id()
      << " at " << pid << "(" << nodeInfo.hostname() << ") "
      << "because the re-registration timeout was reached.";

    nodes.reregistering.erase(nodeInfo.id());
    // Don't send a ShutdownMessage here because tasks from partition-aware
    // frameworks running on this host might still be recovered when the node
    // retries the re-registration.
    return;
  }

  Node* node = nodes.registered.get(nodeInfo.id());
  nodes.unreachable.erase(nodeInfo.id());

  // Update the node pid and relink to it.
  // NOTE: Re-linking the node here always rather than only when
  // the node is disconnected can lead to multiple exited events
  // in succession for a disconnected node. As a result, we
  // ignore duplicate exited events for disconnected nodes.
  // See: https://issues.apache.org/jira/browse/MESOS-675
  node->pid = pid;
  link(node->pid);

  const string& version = reregisterNodeMessage.version();
  const vector<NodeInfo::Capability> nodeCapabilities =
    google::protobuf::convert(reregisterNodeMessage.node_capabilities());
  protobuf::node::Capabilities capabilities(nodeCapabilities);

  Option<UUID> resourceVersion;
  if (reregisterNodeMessage.has_resource_version_uuid()) {
    resourceVersion = reregisterNodeMessage.resource_version_uuid();
  }

  // Update our view of checkpointed node resources for resource
  // provider-capable nodes; for other nodes the controller will resend
  // checkpointed resources after reregistration.
  const Resources checkpointedResources =
    node->capabilities.resourceProvider
      ? Resources(reregisterNodeMessage.checkpointed_resources())
      : node->checkpointedResources;

  Try<Nothing> stateUpdated = node->update(
      nodeInfo,
      version,
      nodeCapabilities,
      checkpointedResources,
      resourceVersion);

  // As of now, the only way `node->update()` can fail is if the node sent
  // different checkpointed resources than it had before. A well-behaving
  // node shouldn't do this, so this one is either malicious or buggy. Either
  // way, we refuse the re-registration attempt.
  if (stateUpdated.isError()) {
    LOG(WARNING) << "Refusing re-registration of node " << nodeInfo.id()
                 << " at " << pid << " (" << nodeInfo.hostname() << ")"
                 << " because state update failed: " << stateUpdated.error();

    ShutdownMessage message;
    message.set_message(stateUpdated.error());
    send(pid, message);

    nodes.reregistering.erase(nodeInfo.id());
    return;
  }

  node->reregisteredTime = Clock::now();

  allocator->updateNode(
    node->id,
    node->info,
    node->totalResources,
    nodeCapabilities);

  const vector<ExecutorInfo> executorInfos =
    google::protobuf::convert(reregisterNodeMessage.executor_infos());
  const vector<Task> tasks =
    google::protobuf::convert(reregisterNodeMessage.tasks());
  const vector<FrameworkInfo> frameworks =
    google::protobuf::convert(reregisterNodeMessage.frameworks());

  // Reconcile tasks between controller and node, and send the
  // `NodeReregisteredMessage`.
  reconcileKnownNode(node, executorInfos, tasks);

  // If this is a disconnected node, add it back to the allocator.
  // This is done after reconciliation to ensure the allocator's
  // offers include the recovered resources initially on this
  // node.
  if (!node->connected) {
    CHECK(node->reregistrationTimer.isSome());
    Clock::cancel(node->reregistrationTimer.get());

    node->connected = true;
    dispatch(node->observer, &NodeObserver::reconnect);

    if (!nodes.deactivated.contains(node->id)) {
      LOG(INFO) << "Reactivating re-registered node " << *node;

      node->active = true;
      allocator->activateNode(node->id);
    }
  }

  // If this is a draining node, send it the drain message.
  // We do this regardless of the draining state (DRAINING or DRAINED),
  // because the node is expected to handle the message in either state.
  if (nodes.draining.contains(nodeInfo.id())) {
    DrainNodeMessage message;
    message.mutable_config()->CopyFrom(
        nodes.draining.at(nodeInfo.id()).config());

    send(node->pid, message);

    // NOTE: If the node supports resource providers, the node will not
    // report pending operations until the node's first UpdateNodeMessage.
    // Therefore, we cannot check the node's drain state here.
    if (!capabilities.resourceProvider) {
      checkAndTransitionDrainingNode(node);
    }
  }

  // Inform the node of the new framework pids for its tasks, and
  // recover any unknown frameworks from the node info.
  updateNodeFrameworks(node, frameworks);

  nodes.reregistering.erase(nodeInfo.id());

  // If the node is not resource provider capable (legacy node),
  // send checkpointed resources to the node. This is important for
  // the cases where the controller didn't fail over. In that case, the
  // controller might have already applied an operation that the node
  // didn't see (e.g., due to a breaking connection). This message
  // will sync the state between the controller and the node about
  // checkpointed resources.
  //
  // New nodes that are resource provider capable will always
  // update the controller with total resources during re-registration.
  // Therefore, no need to send checkpointed resources to the new
  // node in this case.
  if (!node->capabilities.resourceProvider) {
    CheckpointResourcesMessage message;

    message.mutable_resources()->CopyFrom(node->checkpointedResources);

    if (!node->capabilities.reservationRefinement) {
      // If the node is not refinement-capable, don't send it
      // checkpointed resources that contain refined reservations. This
      // might occur if a reservation refinement is created but never
      // reaches the node (e.g., due to network partition), and then
      // the node is downgraded before the partition heals.
      //
      // TODO(neilc): It would probably be better to prevent the node
      // from reregistering in this scenario.
      Try<Nothing> result = downgradeResources(&message);
      if (result.isError()) {
        LOG(WARNING) << "Not sending updated checkpointed resources "
                     << node->checkpointedResources
                     << " with refined reservations, since node " << *node
                     << " is not RESERVATION_REFINEMENT-capable.";

        return;
      }
    }

    LOG(INFO) << "Sending updated checkpointed resources "
              << node->checkpointedResources
              << " to node " << *node;

    send(node->pid, message);
  }
}


void Controller::updateNodeFrameworks(
    Node* node,
    const vector<FrameworkInfo>& frameworks)
{
  CHECK_NOTNULL(node);

  // Send the latest framework pids to the node.
  foreach (const FrameworkInfo& frameworkInfo, frameworks) {
    CHECK(frameworkInfo.has_id());
    Framework* framework = getFramework(frameworkInfo.id());

    if (framework != nullptr) {
      // TODO(bmahler): Copying the framework info here can be
      // expensive, consider only sending this message when
      // there has been a change vs what the node reported.
      UpdateFrameworkMessage message;
      message.mutable_framework_id()->CopyFrom(framework->id());
      message.mutable_framework_info()->CopyFrom(framework->info);

      // TODO(anand): We set 'pid' to UPID() for http frameworks
      // as 'pid' was made optional in 0.24.0. In 0.25.0, we
      // no longer have to set pid here for http frameworks.
      message.set_pid(framework->pid().getOrElse(UPID()));

      send(node->pid, message);
    } else {
      // The node is running a framework that the controller doesn't know
      // about. Recover the framework using the `FrameworkInfo`
      // supplied by the node.

      // We skip recovering the framework if it has already been
      // marked completed at the controller. In this situation, the controller
      // has already told the node to shutdown the framework in
      // `__reregisterNode`.
      if (isCompletedFramework(frameworkInfo.id())) {
        continue;
      }

      LOG(INFO) << "Recovering framework " << frameworkInfo.id()
                << " from reregistering node " << *node;

      recoverFramework(frameworkInfo);
    }
  }
}


void Controller::unregisterNode(const UPID& from, const NodeID& nodeId)
{
  ++metrics->messages_unregister_node;

  Node* node = nodes.registered.get(nodeId);

  if (node == nullptr) {
    LOG(WARNING) << "Ignoring unregister node message from " << from
                 << " for unknown node";

    return;
  }

  if (node->pid != from) {
    LOG(WARNING) << "Ignoring unregister node message from " << from
                 << " because it is not the node " << node->pid;

    return;
  }

  removeNode(node,
              "the node unregistered",
              metrics->node_removals_reason_unregistered);
}


void Controller::updateFramework(
    Framework* framework,
    const FrameworkInfo& frameworkInfo,
    OfferConstraints&& offerConstraints,
    ::medios::allocator::FrameworkOptions&& allocatorOptions)
{
  LOG(INFO) << "Updating framework " << *framework << " with roles "
            << stringify(allocatorOptions.suppressedRoles) << " suppressed";

  // NOTE: The allocator takes care of activating/deactivating
  // the frameworks from the added/removed roles, respectively.
  allocator->updateFramework(
      framework->id(), frameworkInfo, std::move(allocatorOptions));

  // Rescind offers allocated to the roles that were removed.
  const set<string> newRoles = protobuf::framework::getRoles(frameworkInfo);
  foreach (Offer* offer, utils::copy(framework->offers)) {
    if (newRoles.count(offer->allocation_info().role()) == 0) {
      rescindOffer(offer);
    }
  }

  // #697: If a framework was recovered from a re-registering node without a
  // principal and adopts one when its scheduler re-subscribes, ensure its
  // per-principal metrics entry exists before `connectAndActivateRecoveredFramework`.
  if ((!framework->info.has_principal() ||
       framework->info.principal().empty()) &&
      frameworkInfo.has_principal() &&
      !frameworkInfo.principal().empty()) {
    const string& newPrincipal = frameworkInfo.principal();
    if (!metrics->frameworks.contains(newPrincipal)) {
      metrics->frameworks.put(
          newPrincipal,
          Owned<Metrics::Frameworks>(new Metrics::Frameworks(newPrincipal)));
    }
  }

  framework->update(frameworkInfo, std::move(offerConstraints));
}


void Controller::updateNode(UpdateNodeMessage&& message)
{
  ++metrics->messages_update_node;

  upgradeResources(&message);

  const NodeID& nodeId = message.node_id();

  if (nodes.removed.get(nodeId).isSome()) {
    // If the node has been removed, drop the status update. The
    // controller is no longer trying to health check this node; when the
    // node realizes it hasn't received any pings from the controller, it
    // will eventually try to reregister.
    LOG(WARNING) << "Ignoring update on removed node " << nodeId;
    return;
  }

  Node* node = nodes.registered.get(nodeId);

  if (node == nullptr) {
    LOG(WARNING) << "Ignoring update on removed node " << nodeId;
    return;
  }

  // NOTE: We must *first* update the node's resources before we
  // recover the resources. If we recovered the resources first,
  // an allocation could trigger between recovering resources and
  // updating the node in the allocator. This would lead us to
  // re-send out the stale oversubscribed resources!

  // If node does not specify the `update_oversubscribed_resources`
  // field, we assume we should set `oversubscribedResources` to be
  // backwards-compatibility with older nodes (version < 1.5).
  const bool hasOversubscribed =
    !message.has_update_oversubscribed_resources() ||
     message.update_oversubscribed_resources();

  Option<Resources> newOversubscribed;

  if (hasOversubscribed) {
    const Resources& oversubscribedResources =
      message.oversubscribed_resources();

    LOG(INFO) << "Received update of node " << *node << " with total"
              << " oversubscribed resources " << oversubscribedResources;

    newOversubscribed = oversubscribedResources;
  }

  Resources newResourceProviderResources;
  if (message.has_resource_providers()) {
    foreach (
        const UpdateNodeMessage::ResourceProvider& resourceProvider,
        message.resource_providers().providers()) {
      newResourceProviderResources += resourceProvider.total_resources();
    }
  }

  auto nodeResources = [](const Resource& resource) {
    return !resource.has_provider_id();
  };

  const Resources newNodeResources =
    node->totalResources.nonRevocable().filter(nodeResources) +
    newOversubscribed.getOrElse(
        node->totalResources.revocable().filter(nodeResources)) +
    newResourceProviderResources;

  // TODO(bbannier): We only need to update if any changes from
  // resource providers are reported.
  bool updated = node->totalResources != newNodeResources;

  // Check if the node's resource version changed.
  if (!updated && message.has_resource_version_uuid() &&
      (node->resourceVersion.isNone() ||
       (node->resourceVersion.isSome() &&
        message.resource_version_uuid() != node->resourceVersion.get()))) {
    updated = true;
  }

  // Check if the known operations for this node changed.
  if (!updated) {
    // Below we loop over all received operations and check whether
    // they are known to the controller; operations can be unknown to the
    // controller after a controller failover. To handle dropped operations on
    // node failover we explicitly track the received operations and
    // compare them against the operations known to the controller.
    hashset<UUID> receivedOperations;

    foreach (const Operation& operation, message.operations().operations()) {
      if (!node->operations.contains(operation.uuid())) {
        updated = true;
        break;
      }

      if (*node->operations.at(operation.uuid()) != operation) {
        updated = true;
        break;
      }

      receivedOperations.insert(operation.uuid());
    }

    if (receivedOperations.size() != node->operations.size()) {
      updated = true;
    }
  }

  // Check if resource provider information changed.
  if (!updated && message.has_resource_providers()) {
    hashset<ResourceProviderID> receivedResourceProviders;

    foreach (
        const UpdateNodeMessage::ResourceProvider& receivedProvider,
        message.resource_providers().providers()) {
      CHECK(receivedProvider.has_info());
      CHECK(receivedProvider.info().has_id());

      const ResourceProviderID& resourceProviderId =
        receivedProvider.info().id();

      receivedResourceProviders.insert(resourceProviderId);

      if (!node->resourceProviders.contains(resourceProviderId)) {
        updated = true;
        break;
      }

      const Node::ResourceProvider& storedProvider =
        node->resourceProviders.at(resourceProviderId);

      if (storedProvider.info != receivedProvider.info() ||
          storedProvider.totalResources != receivedProvider.total_resources() ||
          storedProvider.resourceVersion !=
            receivedProvider.resource_version_uuid()) {
        updated = true;
        break;
      }

      foreach (
          const Operation& operation,
          receivedProvider.operations().operations()) {
        if (!storedProvider.operations.contains(operation.uuid())) {
          updated = true;
          break;
        }

        if (*storedProvider.operations.at(operation.uuid()) != operation) {
          updated = true;
          break;
        }
      }
    }

    if (node->resourceProviders.keys() != receivedResourceProviders) {
      updated = true;
    }
  }

  if (!updated) {
    LOG(INFO) << "Ignoring update on node " << *node
              << " as it reports no changes";

    // NOTE: This is necessary to catch draining nodes with the
    // RESOURCE_PROVIDER capability, since the controller will not know about
    // any pending operations until the first UpdateNodeMessage has been
    // sent after reregistration.
    checkAndTransitionDrainingNode(node);
    return;
  }

  // Check invariants of the received update.
  {
    foreach (
        const UpdateNodeMessage::ResourceProvider& resourceProvider,
        message.resource_providers().providers()) {
      CHECK(resourceProvider.has_info());
      CHECK(resourceProvider.info().has_id());
      const ResourceProviderID& providerId = resourceProvider.info().id();

      const Option<Node::ResourceProvider>& oldProvider =
        node->resourceProviders.get(providerId);

      if (oldProvider.isSome()) {
        // For known resource providers the controller should always know at least
        // as many non-terminal operations as the node. While an
        // operation might get lost on the way to the node or resource
        // provider, or become terminal inside the node, the controller would never
        // make an operation known to the node terminal without the node
        // doing that first.
        //
        // NOTE: We only consider non-terminal operations here as there is an
        // edge case where the controller removes a terminal operation from
        // its own state when it passes on an acknowledgement from a framework
        // to the node, but the node fails over before it can process the
        // acknowledgement, or the node initiates an unrelated
        // `UpdateNodeMessage`.
        foreach (
            const Operation& operation,
            resourceProvider.operations().operations()) {
          if (!protobuf::isTerminalState(operation.latest_status().state())) {
            CHECK(oldProvider->operations.contains(operation.uuid()))
              << "Node tried to reconcile unknown non-terminal operation "
              << operation.uuid();
          }
        }
      }
    }
  }

  // Update controller and allocator state.

  if (hasOversubscribed) {
    node->totalResources -= node->totalResources.revocable();
    node->totalResources += message.oversubscribed_resources();

    // TODO(bbannier): Track oversubscribed resources for resource
    // providers as well.
  }

  // We don't expect the node's resource version to change, but above we
  // do have a check to see if it's changed and therefore set `updated`
  // to true, so we might as well assign the new value here rather than
  // ignore it.
  //
  // Now that we have resource versions in the node, the lack of the
  // update to the version here was causing MESOS-7187 to be triggered
  // when the controller receives a re-registration message from the old run
  // of a node and then ignores the new re-registration message from a
  // new run of the node. When the new node sends the update message,
  // the controller was seeing the different resource uuid and setting
  // `updated`, but wasn't actually setting the new version.
  if (message.has_resource_version_uuid()) {
    node->resourceVersion = message.resource_version_uuid();
  }

  ReconcileOperationsMessage reconcile;

  // Reconcile operations on node-default resources.
  hashset<UUID> newOperations;
  foreach (const Operation& operation, message.operations().operations()) {
    newOperations.insert(operation.uuid());
  }

  foreachkey (const UUID& uuid, node->operations) {
    if (!message.has_operations() || !newOperations.contains(uuid)) {
      LOG(WARNING) << "Performing explicit reconciliation with node for"
                   << " known operation " << uuid
                   << " since it was not present in original"
                   << " reconciliation message from node";

      ReconcileOperationsMessage::Operation* reconcileOperation =
        reconcile.add_operations();

      reconcileOperation->mutable_operation_uuid()->CopyFrom(uuid);
    }
  }

  // Add new operations reported by the node which the controller isn't aware of.
  // This could happen, for example, in the case of controller failover.
  foreach (const Operation& operation, message.operations().operations()) {
    if (!node->operations.contains(operation.uuid())) {
      Framework* framework = nullptr;
      if (operation.has_framework_id()) {
        framework = getFramework(operation.framework_id());
      }

      addOperation(framework, node, new Operation(operation));
    }
  }

  if (message.has_resource_providers()) {
    hashset<ResourceProviderID> receivedResourceProviders;

    foreach (
        const UpdateNodeMessage::ResourceProvider& resourceProvider,
        message.resource_providers().providers()) {
      CHECK(resourceProvider.has_info());
      CHECK(resourceProvider.info().has_id());
      const ResourceProviderID& providerId = resourceProvider.info().id();

      receivedResourceProviders.insert(providerId);

      // Below we only add operations to our state from resource
      // providers which are unknown, or possibly remove them for known
      // resource providers.  This works since the controller should always
      // know more operations of known resource providers than any
      // resource provider itself.
      //
      // NOTE: We do not mutate operation statuses here; that is the
      // responsibility of the `updateOperationStatus` handler.
      //
      // There still exists an edge case where the controller might remove a
      // terminal operation from its state when passing an
      // acknowledgement from a framework on to the node, with the
      // node failing over before the acknowledgement can be processed.
      // In that case the node would track an operation unknown to the
      // controller.
      //
      // TODO(bbannier): We might want to consider to also learn about
      // new (terminal) operations when observing messages from status
      // update managers to frameworks.
      if (!node->resourceProviders.contains(providerId)) {
        // If this is a not previously seen resource provider we had a controller
        // failover. Add the resources and operations to our state.
        CHECK(
            resourceProvider.total_resources().empty() ||
            !node->totalResources.contains(
                resourceProvider.total_resources()));

        // We add the resource provider to the controller first so
        // that it can be found when e.g., adding operations.
        node->resourceProviders.put(
            providerId,
            {resourceProvider.info(),
             resourceProvider.total_resources(),
             resourceProvider.resource_version_uuid(),
             {}});

        // NOTE: We must add the resource provider's resources to the total
        // before adding any operations, because orphan operations will
        // subsequently subtract from this total.
        node->totalResources += resourceProvider.total_resources();

        hashmap<FrameworkID, Resources> usedByOperations;

        foreach (
            const Operation& operation,
            resourceProvider.operations().operations()) {
          // Update to bookkeeping of operations.
          Framework* framework = nullptr;
          if (operation.has_framework_id()) {
            framework = getFramework(operation.framework_id());
          }

          addOperation(framework, node, new Operation(operation));

          if (!protobuf::isTerminalState(operation.latest_status().state()) &&
              operation.has_framework_id()) {
            // If we do not yet know the `FrameworkInfo` of the framework the
            // operation originated from, the operation is an orphan, and
            // will not be accounted for by the allocator.
            //
            // TODO(bbannier): Consider introducing ways of making sure a node
            // always knows the `FrameworkInfo` of operations triggered on its
            // resources, e.g., by adding an explicit `FrameworkInfo` to
            // operations like is already done for `RunTaskMessage`, see
            // MESOS-8582.
            if (framework == nullptr) {
              continue;
            }

            Try<Resources> consumedResources =
              protobuf::getConsumedResources(operation.info());

            CHECK_SOME(consumedResources)
              << "Could not determine resources consumed by operation "
              << operation.uuid();

            usedByOperations[operation.framework_id()] +=
              consumedResources.get();
          }
        }

        allocator->addResourceProvider(
            nodeId, resourceProvider.total_resources(), usedByOperations);
      } else {
        // If this is a known resource provider its total capacity cannot have
        // changed, and it would not know about any non-terminal operations not
        // already known to the controller.  However, it might not have received an
        // operation for a couple different reasons:
        //
        //   - The resource provider or node could have failed over
        //     before the operation's `ApplyOperationMessage` could be
        //     received.
        //   - The operation's `ApplyOperationMessage` could have raced
        //     with this `UpdateNodeMessage`.
        //
        // In both of these cases, we need to reconcile such operations
        // explicitly with the node. For operations which the node or resource
        // provider does not recognize, an OPERATION_DROPPED status update will
        // be generated and the controller will remove the operation from its state
        // upon receipt of that update.
        CHECK(node->resourceProviders.contains(providerId));

        Node::ResourceProvider& oldProvider =
          node->resourceProviders.at(providerId);

        hashmap<UUID, const Operation*> newOperations;
        foreach (
            const Operation& operation,
            resourceProvider.operations().operations()) {
          newOperations.put(operation.uuid(), &operation);
        }

        foreachpair (
            const UUID& uuid,
            Operation * oldOperation,
            oldProvider.operations) {
          if (!newOperations.contains(uuid)) {
            LOG(WARNING) << "Performing explicit reconciliation with node for"
                         << " known operation " << uuid
                         << " since it was not present in original"
                         << " reconciliation message from node";

            ReconcileOperationsMessage::Operation* reconcileOperation =
              reconcile.add_operations();

            reconcileOperation->mutable_operation_uuid()->CopyFrom(uuid);
            reconcileOperation->mutable_resource_provider_id()->CopyFrom(
                providerId);
          } else {
            // If a known operation became terminal between any previous offer
            // operation status update and this `UpdateNodeMessage`, the total
            // resources we were sent already had the operation applied. We need
            // to update the state of the operation to terminal here so that any
            // update sent by the node later does not cause us to apply the
            // operation again.

            const Operation* newOperation = newOperations.at(uuid);

            if (!protobuf::isTerminalState(
                    oldOperation->latest_status().state()) &&
                protobuf::isTerminalState(
                    newOperation->latest_status().state())) {
              Operation* operation = node->getOperation(uuid);
              CHECK(operation != nullptr) << uuid;

              UpdateOperationStatusMessage update =
                protobuf::createUpdateOperationStatusMessage(
                    uuid,
                    newOperation->latest_status(),
                    newOperation->latest_status(),
                    operation->framework_id(),
                    operation->node_id());

              updateOperation(
                  operation, update, false); // Do not update resources.
            }
          }
        }

        // Reconcile the total resources. This includes undoing
        // speculated operations which are only visible in the total,
        // but never in the used resources. We explicitly allow for
        // resource providers to change from or to zero capacity.
        const Resources oldResources =
          node->totalResources.filter([&providerId](const Resource& resource) {
            return resource.provider_id() == providerId;
          });

        node->totalResources -= oldResources;
        node->totalResources += resourceProvider.total_resources();

        oldProvider.totalResources = resourceProvider.total_resources();

        // Reconcile resource versions.
        oldProvider.resourceVersion = resourceProvider.resource_version_uuid();
      }
    }

    // Garbage-collect disappeared resource providers disappeared from
    // the node. We only perform the cleanup if the node sent some set
    // of resource providers as absence of a set resource_providers
    // field does not communicate absence of resource providers.
    const hashset<ResourceProviderID> disappearedResourceProviders =
      node->resourceProviders.keys() - receivedResourceProviders;

    foreach (
        const ResourceProviderID& resourceProviderId,
        disappearedResourceProviders) {
      LOG(INFO) << "Garbage collecting resource provider " << resourceProviderId
                << " since it disappeared from the reported node state";

      // Remove the resource provider resources from the total resources. This
      // includes undoing speculated operations which are only visible in the
      // total, but never in the used resources.
      CHECK(node->resourceProviders.contains(resourceProviderId));

      // Clean up any associated operations belonging to the removed
      // resource provider.
      foreachvalue (
          Operation* operation,
          utils::copy(node->resourceProviders.at(resourceProviderId)
            .operations)) {
        removeOperation(operation);
      }

      node->totalResources -=
        node->resourceProviders.at(resourceProviderId).totalResources;

      node->resourceProviders.erase(resourceProviderId);
    }
  }

  if (reconcile.operations_size() > 0) {
    send(node->pid, reconcile);
  }

  // Now update the node's state and total resources in the allocator.
  allocator->updateNode(nodeId, node->info, node->totalResources);

  // Then rescind outstanding offers affected by the update.
  // NOTE: Need a copy of offers because the offers are removed inside the loop.
  foreach (Offer* offer, utils::copy(node->offers)) {
    const Resources& offered = offer->resources();
    // Since updates of the node's oversubscribed resources are sent at regular
    // intervals, we only rescind offers containing revocable resources to
    // reduce churn.
    if (hasOversubscribed && !offered.revocable().empty()) {
      LOG(INFO) << "Rescinding offer " << offer->id()
                << " with revocable resources " << offered << " on node "
                << *node;

      rescindOffer(offer);
      continue;
    }

    // Updates on resource providers can change the node total
    // resources, so we rescind all offers.
    //
    // TODO(bbannier): Only rescind offers possibly containing
    // affected resources.
    const Resources offeredResourceProviderResources = offered.filter(
        [](const Resource& resource) { return resource.has_provider_id(); });
    if (message.has_resource_providers() &&
        !offeredResourceProviderResources.empty()) {
      LOG(INFO)
        << "Rescinding offer " << offer->id()
        << " with resources " << offered << " on node " << *node;

      rescindOffer(offer);
    }
  }

  // NOTE: We don't need to rescind inverse offers here as they are unrelated to
  // oversubscription.

  // Now that we have the node's operations in controller memory, we can check
  // if the node is drained and transition it appropriately if so.
  checkAndTransitionDrainingNode(node);
}


void Controller::updateUnavailability(
    const MachineID& machineId,
    const Option<Unavailability>& unavailability)
{
  if (unavailability.isSome()) {
    machines[machineId].info.mutable_unavailability()->CopyFrom(
        unavailability.get());
  } else {
    machines[machineId].info.clear_unavailability();
  }

  // TODO(jmlvanre): Only update allocator and rescind offers if the
  // unavailability has actually changed.
  if (machines.contains(machineId)) {
    // For every node on this machine, update the allocator.
    foreach (const NodeID& nodeId, machines[machineId].nodes) {
      // The node should not be in the machines mapping if it is removed.
      CHECK(nodes.removed.get(nodeId).isNone());

      // The node should be registered if it is in the machines mapping.
      CHECK(nodes.registered.contains(nodeId));

      Node* node = nodes.registered.get(nodeId);

      if (unavailability.isSome()) {
        // TODO(jmlvanre): Add stream operator for unavailability.
        LOG(INFO) << "Updating unavailability of node " << *node
                  << ", starting at "
                  << Nanoseconds(unavailability->start().nanoseconds());
      } else {
        LOG(INFO) << "Removing unavailability of node " << *node;
      }

      // Rescind offers since we want to inform frameworks of the
      // unavailability change as soon as possible.
      foreach (Offer* offer, utils::copy(node->offers)) {
        rescindOffer(offer);
      }

      // Remove and rescind inverse offers since the allocator will send new
      // inverse offers for the updated unavailability.
      foreach (InverseOffer* inverseOffer, utils::copy(node->inverseOffers)) {
        allocator->updateInverseOffer(
            node->id,
            inverseOffer->framework_id(),
            UnavailableResources{
                inverseOffer->resources(),
                inverseOffer->unavailability()},
            None());

        removeInverseOffer(inverseOffer, true); // Rescind!
      }

      // We remove / rescind all the offers first so that any calls to the
      // allocator to modify its internal state are queued before the update of
      // the unavailability in the allocator. We do this so that the allocator's
      // state can start from a "clean slate" for the new unavailability.
      // NOTE: Any calls from the Allocator back into the controller, for example
      // `offer()`, are guaranteed to happen after this function exits due to
      // the Actor pattern.

      allocator->updateUnavailability(nodeId, unavailability);
    }
  }
}


// TODO(vinod): Since 0.22.0, we can use 'from' instead of 'pid'
// because the status updates will be sent by the node.
//
// TODO(vinod): Add a benchmark test for status update handling.
void Controller::statusUpdate(StatusUpdateMessage&& statusUpdateMessage)
{
  // Progress is what a task says about itself, so the controller validates
  // it before recording it or forwarding it. A bad report is an
  // observation error, not a task failure: the field is dropped and counted
  // and the rest of the update, including a terminal state, is delivered.
  if (statusUpdateMessage.update().status().has_progress()) {
    Option<Error> error = common::validation::validateTaskProgress(
        statusUpdateMessage.update().status().progress());

    if (error.isSome()) {
      LOG(WARNING) << "Dropping task progress from status update for task "
                   << statusUpdateMessage.update().status().task_id()
                   << " of framework "
                   << statusUpdateMessage.update().framework_id() << ": "
                   << error->message;

      statusUpdateMessage.mutable_update()->mutable_status()
        ->clear_progress();

      ++metrics->invalid_task_progress_reports;
    }
  }

  // The finish outcome is the executor's word on its hook; one with no
  // state, or an impossible one, is dropped the same way and the rest of
  // the update is delivered (DR-042).
  if (statusUpdateMessage.update().status().has_finish()) {
    Option<Error> error = common::validation::validateFinishStatus(
        statusUpdateMessage.update().status().finish());

    if (error.isSome()) {
      LOG(WARNING) << "Dropping the finish outcome from status update for task "
                   << statusUpdateMessage.update().status().task_id()
                   << " of framework "
                   << statusUpdateMessage.update().framework_id() << ": "
                   << error->message;

      statusUpdateMessage.mutable_update()->mutable_status()->clear_finish();

      ++metrics->invalid_task_finish_outcomes;
    }
  }

  const StatusUpdate& update = statusUpdateMessage.update();
  const UPID& pid = statusUpdateMessage.pid();

  CHECK_NE(pid, UPID());

  ++metrics->messages_status_update;

  if (nodes.removed.get(update.node_id()).isSome()) {
    // If the node has been removed, drop the status update. The
    // controller is no longer trying to health check this node; when the
    // node realizes it hasn't received any pings from the controller, it
    // will eventually try to reregister.
    LOG(WARNING) << "Ignoring status update " << update
                 << " from removed node " << pid
                 << " with id " << update.node_id();

    metrics->invalid_status_updates++;
    return;
  }

  Node* node = nodes.registered.get(update.node_id());

  if (node == nullptr) {
    LOG(WARNING) << "Ignoring status update " << update
                 << " from unknown node " << pid
                 << " with id " << update.node_id();

    metrics->invalid_status_updates++;
    return;
  }

  Try<id::UUID> uuid = id::UUID::fromBytes(update.uuid());
  if (uuid.isError()) {
    LOG(WARNING) << "Ignoring status update "
                 << " from node " << *node
                 << ": " << uuid.error();

    ++metrics->invalid_status_updates;
    return;
  }

  LOG(INFO) << "Status update " << update << " from node " << *node;

  // Nodes >= 0.26 should always correctly set task status uuid.
  CHECK(update.status().has_uuid());

  bool validStatusUpdate = true;

  Framework* framework = getFramework(update.framework_id());

  // A framework might not have reregistered upon a controller failover or
  // got disconnected.
  if (framework != nullptr && framework->connected()) {
    forward(update, pid, framework);
  } else {
    validStatusUpdate = false;
    LOG(WARNING) << "Received status update " << update << " from node "
                 << *node << " for "
                 << (framework == nullptr ? "an unknown " : "a disconnected ")
                 << "framework";
  }

  // Lookup the task and see if we need to update anything locally.
  Task* task = node->getTask(update.framework_id(), update.status().task_id());
  if (task == nullptr) {
    // TODO(neilc): We might see status updates for non-partition
    // aware tasks running on a partitioned node that has
    // reregistered with the controller. The controller marks such tasks
    // completed when the node partitions; it will shutdown the
    // framework when the node-reregisters, but we may see a number
    // of status updates before the framework is shutdown.
    LOG(WARNING) << "Could not lookup task for status update " << update
                 << " from node " << *node;

    metrics->invalid_status_updates++;
    return;
  }

  updateTask(task, update);

  validStatusUpdate
    ? metrics->valid_status_updates++ : metrics->invalid_status_updates++;
}


void Controller::forward(
    const StatusUpdate& update,
    const UPID& acknowledgee,
    Framework* framework)
{
  CHECK_NOTNULL(framework);

  if (!acknowledgee) {
    LOG(INFO) << "Sending status update " << update
              << (update.status().has_message()
                  ? " '" + update.status().message() + "'"
                  : "");
  } else {
    LOG(INFO) << "Forwarding status update " << update;
  }

  // The task might not exist in controller's memory (e.g., failed task validation).
  Task* task = framework->getTask(update.status().task_id());
  if (task != nullptr) {
    // Set the status update state and uuid for the task. Note that
    // controller-generated updates are terminal and do not have a uuid
    // (in which case the controller also calls `removeTask()`).
    if (update.has_uuid()) {
      task->set_status_update_state(update.status().state());
      task->set_status_update_uuid(update.status().uuid());
    }
  }

  StatusUpdateMessage message;
  message.mutable_update()->MergeFrom(update);

  // Every update a framework sees carries the controller's record of the
  // on-behalf-of pair (DR-037) and of the stores the task was given
  // (DR-036), so a framework's store mirrors the controller's. An update
  // for a task the controller never recorded, such as a refused launch,
  // carries none of them.
  if (task != nullptr) {
    TaskStatus* status = message.mutable_update()->mutable_status();
    if (task->has_principal()) {
      status->set_principal(task->principal());
    }
    if (task->has_framework_principal()) {
      status->set_framework_principal(task->framework_principal());
    }
    status->clear_stores();
    foreach (const TaskData& data, protobuf::storesOf(*task)) {
      status->add_stores()->CopyFrom(data);
    }
  }

  message.set_pid(acknowledgee);
  framework->send(message);
}


void Controller::updateOperationStatus(UpdateOperationStatusMessage&& update)
{
  CHECK(update.has_node_id())
    << "External resource provider is not supported yet";

  ++metrics->messages_operation_status_update;

  const NodeID& nodeId = update.node_id();

  // While currently only frameworks can initiate operations which request
  // feedback, in some cases such as controller/node reconciliation, the framework
  // ID will not be set in the update message.
  Option<FrameworkID> frameworkId = update.has_framework_id()
    ? update.framework_id()
    : Option<FrameworkID>::none();

  // If the operation UUID is not set, then this must be an update sent as a
  // result of a framework-inititated reconciliation which was forwarded to the
  // node. Since the operation UUID is not known, there is nothing for the
  // controller to do but forward the update to the framework and return.
  if (!update.has_operation_uuid()) {
    CHECK_SOME(frameworkId);

    // Forward the status update to the framework.
    Framework* framework = getFramework(frameworkId.get());

    if (framework == nullptr || !framework->connected()) {
      LOG(WARNING) << "Received operation status update " << update
                   << ", but the framework is "
                   << (framework == nullptr ? "unknown" : "disconnected");
    } else {
      LOG(INFO) << "Forwarding operation status update " << update;
      framework->send(update);
    }

    return;
  }

  Node* node = nodes.registered.get(nodeId);

  const UUID& uuid = update.operation_uuid();

  // This is possible if the node is marked as unreachable or gone,
  // or has initiated a graceful shutdown. In either of those cases,
  // ignore the operation status update.
  //
  // TODO(jieyu): If the node is unreachable or has initiated a
  // graceful shutdown, we can still forward the update to the
  // framework so that the framework can get notified about the offer
  // operation early. However, the acknowledgement of the update won't
  // be able to reach the node in those cases. If the node is gone,
  // we cannot forward the update because the controller might already
  // tell the framework that the operation is gone.
  if (node == nullptr) {
    LOG(WARNING) << "Ignoring status update for operation '"
                 << update.status().operation_id()
                 << "' (uuid: " << uuid << ") for "
                 << (frameworkId.isSome()
                       ? "framework " + stringify(frameworkId.get())
                       : "an operator API call")
                 << ": Node " << nodeId << " is not registered";

    ++metrics->invalid_operation_status_updates;
    return;
  }

  Operation* operation = node->getOperation(update.operation_uuid());
  if (operation == nullptr) {
    // If the operation cannot be found and no framework ID was set, then this
    // must be a duplicate update as a result of controller/node reconciliation. In
    // this case, a terminal reconciliation update has already been received by
    // the controller, so we simply return.
    if (frameworkId.isNone()) {
      return;
    }

    // If the operation cannot be found and a framework ID was set, then this
    // must be an update sent as a result of a framework-inititated
    // reconciliation which was forwarded to the node and raced with an
    // `UpdateNodeMessage` which removed the operation. Since the operation is
    // not known to the controller, there is nothing for the controller to do but
    // forward the update to the framework and return.
    Framework* framework = getFramework(frameworkId.get());

    if (framework == nullptr || !framework->connected()) {
      LOG(WARNING) << "Received operation status update " << update
                   << ", but the framework is "
                   << (framework == nullptr ? "unknown" : "disconnected");
    } else {
      LOG(INFO) << "Forwarding operation status update " << update;
      framework->send(update);
    }

    ++metrics->invalid_operation_status_updates;
    return;
  }

  // TODO(bevers): Most of the `CHECK()`s below could probably be turned
  // into validation steps that would just reject the message as opposed
  // to crashing the controller.
  ++metrics->valid_operation_status_updates;

  if (operation->info().has_id()) {
    // Nodes don't include the framework and operation IDs when sending
    // operation status updates for dropped operations in response to a
    // `ReconcileOperationsMessage`, but they can be deduced from the operation
    // info kept on the controller.

    // Currently, only operations done via the scheduler API can have an ID.
    CHECK(operation->has_framework_id());

    frameworkId = operation->framework_id();

    update.mutable_status()->mutable_operation_id()->CopyFrom(
        operation->info().id());
  }

  updateOperation(operation, update);

  CHECK(operation->statuses_size() > 0);

  const OperationStatus& latestStatus = *operation->statuses().rbegin();

  // Frameworks are sent operation status updates when the operation has
  // a framework-specified ID and the framework is still running.
  // Orphaned operations have no framework to send updates to.
  bool frameworkWillAcknowledge =
    operation->info().has_id() &&
    frameworkId.isSome() &&
    !isCompletedFramework(frameworkId.get()) &&
    !node->orphanedOperations.contains(operation->uuid());

  if (frameworkWillAcknowledge) {
    CHECK_SOME(frameworkId);

    // Forward the status update to the framework.
    Framework* framework = getFramework(frameworkId.get());

    if (framework == nullptr || !framework->connected()) {
      LOG(WARNING) << "Received operation status update " << update
                   << ", but the framework is "
                   << (framework == nullptr ? "unknown" : "disconnected");
    } else {
      LOG(INFO) << "Forwarding operation status update " << update;
      framework->send(update);
    }

    if (protobuf::isTerminalState(latestStatus.state()) &&
        !latestStatus.has_uuid()) {
      // Remove the operation if the update is terminal and it is not
      // reliably sent.
      removeOperation(operation);
    }
  } else {
    if (latestStatus.has_uuid()) {
      // This update is being sent reliably, and either doesn't have
      // an operation ID or the associated framework terminated, so
      // the controller has to send an acknowledgement.

      // If an orphan operation belongs to a framework that is not
      // marked "completed", there is a chance the framework will
      // reregister in future. The controller will drop these status
      // updates until the framework reregisters or a certain amount
      // of time has passed since the associated node has reregistered.
      //
      // This behavior prevents the controller from acknowledging operations
      // directly after controller failover, while both nodes and frameworks
      // reregister. If a node with pending operations reregisters first,
      // the operations may be considered orphans until the framework
      // reregisters.
      //
      // NOTE: Frameworks rotated out of the controller's completed frameworks
      // buffer may also be affected by this wait.
      if (operation->info().has_id() &&
          node->orphanedOperations.contains(operation->uuid()) &&
          frameworkId.isSome() &&
          !isCompletedFramework(frameworkId.get())) {
        if (node->reregisteredTime.isSome() &&
            (Clock::now() - node->reregisteredTime.get()) <
              MIN_WAIT_BEFORE_ORPHAN_OPERATION_ADOPTION) {
          return;
        }
      }

      Result<ResourceProviderID> resourceProviderId =
        getResourceProviderId(operation->info());

      CHECK(!resourceProviderId.isError())
        << "Could not determine resource provider of operation with no ID"
        << (frameworkId.isSome()
              ? " from framework " + stringify(frameworkId.get())
              : " from an operator")
        << ": " << resourceProviderId.error();

      AcknowledgeOperationStatusMessage acknowledgement;
      acknowledgement.mutable_status_uuid()->CopyFrom(latestStatus.uuid());
      acknowledgement.mutable_operation_uuid()->CopyFrom(operation->uuid());

      if (resourceProviderId.isSome()) {
        acknowledgement.mutable_resource_provider_id()->CopyFrom(
            resourceProviderId.get());
      }

      CHECK(node->capabilities.resourceProvider ||
            node->capabilities.nodeOperationFeedback);

      send(node->pid, acknowledgement);
    }

    if (protobuf::isTerminalState(latestStatus.state())) {
      removeOperation(operation);
    }
  }
}


void Controller::exitedExecutor(
    const UPID& from,
    const NodeID& nodeId,
    const FrameworkID& frameworkId,
    const ExecutorID& executorId,
    int32_t status)
{
  ++metrics->messages_exited_executor;

  if (nodes.removed.get(nodeId).isSome()) {
    // If the node has been removed, drop the executor message. The
    // controller is no longer trying to health check this node; when the
    // node realizes it hasn't received any pings from the controller, it
    // will eventually try to reregister.
    LOG(WARNING) << "Ignoring exited executor '" << executorId
                 << "' of framework " << frameworkId
                 << " on removed node " << nodeId;

    return;
  }

  Node* node = nodes.registered.get(nodeId);

  if (node == nullptr) {
    LOG(WARNING) << "Ignoring exited executor '" << executorId
                 << "' of framework " << frameworkId
                 << " on unknown node " << nodeId;

    return;
  }

  // Only update controller's internal data structures here for proper
  // accounting. The TASK_LOST updates are handled by the node.

  if (!node->hasExecutor(frameworkId, executorId)) {
    LOG(WARNING) << "Ignoring unknown exited executor '" << executorId
                 << "' of framework " << frameworkId
                 << " on node " << *node;

    return;
  }

  LOG(INFO) << "Executor '" << executorId
            << "' of framework " << frameworkId
            << " on node " << *node << ": "
            << WSTRINGIFY(status);

  removeExecutor(node, frameworkId, executorId);

  // TODO(vinod): Reliably forward this message to the scheduler.
  Framework* framework = getFramework(frameworkId);
  if (framework == nullptr || !framework->connected()) {
    string status = (framework == nullptr ? "unknown" : "disconnected");

    LOG(WARNING)
      << "Not forwarding exited executor message for executor '" << executorId
      << "' of framework " << frameworkId << " on node " << *node
      << " because the framework is " << status;

    return;
  }

  ExitedExecutorMessage message;
  message.mutable_executor_id()->CopyFrom(executorId);
  message.mutable_framework_id()->CopyFrom(frameworkId);
  message.mutable_node_id()->CopyFrom(nodeId);
  message.set_status(status);

  framework->send(message);
}


void Controller::shutdown(
    Framework* framework,
    const scheduler::Call::Shutdown& shutdown)
{
  CHECK_NOTNULL(framework);

  // TODO(vinod): Add a metric for executor shutdowns.

  const NodeID& nodeId = shutdown.node_id();
  const ExecutorID& executorId = shutdown.executor_id();
  const FrameworkID& frameworkId = framework->id();

  Node* node = nodes.registered.get(nodeId);

  if (node == nullptr) {
    LOG(WARNING) << "Unable to shutdown executor '" << executorId
                 << "' of framework " << frameworkId
                 << " of unknown node " << nodeId;

    return;
  }

  LOG(INFO) << "Processing SHUTDOWN call for executor '" << executorId
            << "' of framework " << *framework << " on node " << nodeId;

  ShutdownExecutorMessage message;
  message.mutable_executor_id()->CopyFrom(executorId);
  message.mutable_framework_id()->CopyFrom(frameworkId);
  send(node->pid, message);
}


Future<bool> Controller::markUnreachable(
    const NodeInfo& node,
    bool duringControllerFailover,
    const string& message)
{
  if (duringControllerFailover && !nodes.recovered.contains(node.id())) {
    LOG(INFO) << "Skipping transition of node"
              << " " << node.id() << " (" << node.hostname() << ")"
              << " to unreachable because it reregistered in the interim";

    return false;
  }

  if (!duringControllerFailover && !nodes.registered.contains(node.id())) {
    // Possible when the `NodeObserver` dispatches a message to
    // mark an unhealthy node as unreachable, but the node is
    // concurrently removed for another reason (e.g.,
    // `UnregisterNodeMessage` is received).
    LOG(WARNING) << "Skipping transition of node"
                 << " " << node.id() << " (" << node.hostname() << ")"
                 << " to unreachable because it has already been removed"
                 << " or marked unreachable";

    return false;
  }

  // The node might be in the process of reregistering without
  // the marking unreachable having been canceled.
  if (nodes.reregistering.contains(node.id())) {
    LOG(INFO) << "Skipping transition of node"
              << " " << node.id() << " (" << node.hostname() << ")"
              << " to unreachable because it is reregistering";

    return false;
  }

  if (nodes.markingUnreachable.contains(node.id())) {
    // We might already be marking this node unreachable. This is
    // possible if marking the node unreachable in the registry takes
    // a long time. While the registry operation is in progress, the
    // `NodeObserver` will continue to ping the node; if the node
    // fails another health check, the `NodeObserver` will trigger
    // another attempt to mark it unreachable. Also possible if
    // `nodeReregisterTimeout` marks the node unreachable
    // concurrently with the node observer doing so.
    LOG(WARNING) << "Skipping transition of node"
                 << " " << node.id() << " (" << node.hostname() << ")"
                 << " to unreachable because another unreachable"
                 << " transition is already in progress";

    return false;
  }

  if (nodes.removing.contains(node.id())) {
    LOG(WARNING) << "Skipping transition of node"
                 << " " << node.id() << " (" << node.hostname() << ")"
                 << " to unreachable because it is being removed";

    return false;
  }

  if (nodes.removed.get(node.id()).isSome()) {
    LOG(WARNING) << "Skipping transition of node"
                 << " " << node.id() << " (" << node.hostname() << ")"
                 << " to unreachable because it has been removed";

    return false;
  }

  if (nodes.markingGone.contains(node.id())) {
    LOG(WARNING) << "Skipping transition of node"
                 << " " << node.id() << " (" << node.hostname() << ")"
                 << " to unreachable because it is being marked as gone";

    return false;
  }

  if (nodes.gone.contains(node.id())) {
    LOG(WARNING) << "Skipping transition of node"
                 << " " << node.id() << " (" << node.hostname() << ")"
                 << " to unreachable because it has been marked as gone";

    return false;
  }

  LOG(INFO) << "Marking node " << node.id() << " (" << node.hostname() << ")"
            << " unreachable: " << message;

  CHECK(!nodes.unreachable.contains(node.id()));
  nodes.markingUnreachable.insert(node.id());

  // Use the same timestamp for all status updates sent below;
  // we also use this timestamp when updating the registry.
  TimeInfo unreachableTime = protobuf::getCurrentTime();

  const string failure = "Failed to mark node " + stringify(node.id()) +
    " (" + node.hostname() + ") as unreachable in the registry";

  // Update the registry to move this node from the list of admitted
  // nodes to the list of unreachable nodes. After this is complete,
  // we can remove the node from the controller's in-memory state and
  // send TASK_UNREACHABLE / TASK_LOST updates to the frameworks.
  return undiscardable(
      registrar->apply(Owned<RegistryOperation>(
          new MarkNodeUnreachable(node, unreachableTime)))
      .onFailed(lambda::bind(fail, failure, lambda::_1))
      .onDiscarded(lambda::bind(fail, failure, "discarded"))
      .then(defer(self(), [=](bool result) {
        _markUnreachable(
            node, unreachableTime, duringControllerFailover, message, result);
        return true;
      })));
}


void Controller::_markUnreachable(
    const NodeInfo& node,
    const TimeInfo& unreachableTime,
    bool duringControllerFailover,
    const string& message,
    bool registrarResult)
{
  // `MarkNodeUnreachable` registry operation should never fail.
  CHECK(registrarResult);

  CHECK(nodes.markingUnreachable.contains(node.id()));
  nodes.markingUnreachable.erase(node.id());

  LOG(INFO) << "Marked node"
            << " " << node.id() << " (" << node.hostname() << ")"
            << " unreachable: " << message;

  ++metrics->node_removals;
  ++metrics->node_removals_reason_unhealthy;

  CHECK(!nodes.unreachable.contains(node.id()));
  nodes.unreachable[node.id()] = unreachableTime;

  if (duringControllerFailover) {
    CHECK(nodes.recovered.contains(node.id()));
    nodes.recovered.erase(node.id());

    ++metrics->recovery_node_removals;

    // TODO(bmahler): Tell partition aware frameworks that the
    // node is unreachable rather than lost. This requires a
    // new capability.
    sendNodeLost(node);
  } else {
    CHECK(nodes.registered.contains(node.id()));

    // Notify frameworks that their operations have been transitioned
    // to `OPERATION_UNREACHABLE`.
    sendBulkOperationFeedback(
        nodes.registered.get(node.id()),
        OperationState::OPERATION_UNREACHABLE,
        "Node was marked unreachable");

    __removeNode(nodes.registered.get(node.id()), message, unreachableTime);
  }
}


void Controller::markGone(const NodeID& nodeId, const TimeInfo& goneTime)
{
  CHECK(nodes.markingGone.contains(nodeId));

  nodes.markingGone.erase(nodeId);

  nodes.gone[nodeId] = goneTime;

  const string message = "Node has been marked gone";

  Node* node = nodes.registered.get(nodeId);

  // If the `Node` struct does not exist, then the node
  // must be either recovered or unreachable.
  if (node == nullptr) {
    CHECK(nodes.recovered.contains(nodeId) ||
          nodes.unreachable.contains(nodeId));

    // When a recovered node is marked gone, we have no task metadata to use in
    // order to send task status updates. We could retain this node ID and send
    // updates upon reregistration but do not currently do this. See MESOS-9739.
    if (nodes.recovered.contains(nodeId)) {
      return;
    }

    nodes.unreachable.erase(nodeId);
    nodes.draining.erase(nodeId);
    nodes.deactivated.erase(nodeId);

    // TODO(vinod): Consider moving these tasks into `completedTasks` by
    // transitioning them to a terminal state and sending status updates.
    // But it's not clear what this state should be. If a framework
    // reconciles these tasks after this point it would get `TASK_UNKNOWN`
    // which seems appropriate but we don't keep tasks in this state in-memory.
    if (nodes.unreachableTasks.contains(nodeId)) {
      foreachkey (const FrameworkID& frameworkId,
                  nodes.unreachableTasks.at(nodeId)) {
        Framework* framework = getFramework(frameworkId);
        if (framework == nullptr) {
          continue;
        }

        TaskState newTaskState = TASK_GONE_BY_OPERATOR;
        TaskStatus::Reason newTaskReason =
          TaskStatus::REASON_NODE_REMOVED_BY_OPERATOR;

        if (!framework->capabilities.partitionAware) {
          newTaskState = TASK_LOST;
          newTaskReason = TaskStatus::REASON_NODE_REMOVED;
        }

        foreach (const TaskID& taskId,
                 nodes.unreachableTasks.at(nodeId).at(frameworkId)) {
          if (framework->unreachableTasks.contains(taskId)) {
            const Owned<Task>& task = framework->unreachableTasks.at(taskId);

            const StatusUpdate& update = protobuf::createStatusUpdate(
                task->framework_id(),
                task->node_id(),
                task->task_id(),
                newTaskState,
                TaskStatus::SOURCE_CONTROLLER,
                None(),
                message,
                newTaskReason,
                (task->has_executor_id()
                   ? Option<ExecutorID>(task->executor_id())
                   : None()));

            updateTask(task.get(), update);

            if (!framework->connected()) {
              LOG(WARNING) << "Dropping update " << update
                           << " for disconnected "
                           << " framework " << frameworkId;
            } else {
              forward(update, UPID(), framework);
            }

            // Move task from unreachable map to completed map.
            framework->addCompletedTask(std::move(*task));
            framework->unreachableTasks.erase(taskId);
          }
        }
      }

      nodes.unreachableTasks.erase(nodeId);
    }

    return;
  }

  // Shutdown the node if it transitioned to gone.
  ShutdownMessage shutdownMessage;
  shutdownMessage.set_message(message);
  send(node->pid, shutdownMessage);

  // Notify frameworks that their operations have been transitioned
  // to status `OPERATION_GONE_BY_OPERATOR`.
  sendBulkOperationFeedback(
    node,
    OperationState::OPERATION_GONE_BY_OPERATOR,
    "Node has been marked gone");

  __removeNode(node, message, None());
}


// Send an update for every operation on a given node.
//
// NOTE: This is currently purely for the frameworks' convenience, as there
// are no retries or other mechanisms to ensure reliability. Frameworks who
// miss this update can (and must) get the latest status via the reconciliation
// process. This might be changed in the future so clients are not
// forced to use operation reconciliation.
void Controller::sendBulkOperationFeedback(
    Node* node,
    OperationState operationState,
    const std::string& message)
{
  hashmap<UUID, const Operation*> operations;
  operations.insert(
      node->operations.begin(),
      node->operations.end());

  foreachvalue (
      const Node::ResourceProvider& provider,
      node->resourceProviders) {
    operations.insert(provider.operations.begin(), provider.operations.end());
  }

  foreachvalue (const Operation* operation, operations) {
    // Frameworks signal that they want to receive feedback
    // on the operation status by setting the `id` field.
    if (!operation->info().has_id()) {
      continue;
    }

    // Offer operations made through the operator API might not have
    // an associated framework id.
    if (!operation->has_framework_id()) {
      continue;
    }

    // Frameworks that are not currently registered will not receive
    // a notification here and need to rely on the reconciliation process.
    Option<Framework*> framework =
      frameworks.registered.get(operation->framework_id());

    if (!framework.isSome() || !framework.get()->http().isSome()) {
      continue;
    }

    Result<ResourceProviderID> resourceProviderId =
      getResourceProviderId(operation->info());

    CHECK(!resourceProviderId.isError());

    scheduler::Event update;
    update.set_type(scheduler::Event::UPDATE_OPERATION_STATUS);
    *update.mutable_update_operation_status()->mutable_status() =
      protobuf::createOperationStatus(
          operationState,
          operation->info().id(),
          message,
          None(),
          None(),
          node->id,
          resourceProviderId.isSome()
            ? Some(resourceProviderId.get())
            : Option<ResourceProviderID>::none());


    framework.get()->send(update);
  }
}


void Controller::reconcileTasks(
    const UPID& from,
    ReconcileTasksMessage&& reconcileTasksMessage)
{
  const FrameworkID& frameworkId = reconcileTasksMessage.framework_id();

  Framework* framework = getFramework(frameworkId);
  if (framework == nullptr) {
    LOG(WARNING) << "Unknown framework " << frameworkId << " at " << from
                 << " attempted to reconcile tasks";

    return;
  }

  if (framework->pid() != from) {
    LOG(WARNING)
      << "Ignoring reconcile tasks message for framework " << *framework
      << " because it is not expected from " << from;

    return;
  }

  scheduler::Call::Reconcile message;
  message.mutable_tasks()->Reserve(reconcileTasksMessage.statuses_size());

  foreach (TaskStatus& status, *reconcileTasksMessage.mutable_statuses()) {
    scheduler::Call::Reconcile::Task* t = message.add_tasks();

    *t->mutable_task_id() = std::move(status.task_id());

    if (status.has_node_id()) {
      *t->mutable_node_id() = std::move(status.node_id());
    }
  }

  reconcile(framework, std::move(message));
}


void Controller::reconcile(
    Framework* framework,
    scheduler::Call::Reconcile&& reconcile)
{
  CHECK_NOTNULL(framework);

  ++metrics->messages_reconcile_tasks;

  if (reconcile.tasks().empty()) {
    // Implicit reconciliation.
    LOG(INFO) << "Performing implicit task state reconciliation"
                 " for framework " << *framework;

    foreachvalue (Task* task, framework->tasks) {
      const TaskState& state = task->has_status_update_state()
          ? task->status_update_state()
          : task->state();

      const Option<ExecutorID>& executorId = task->has_executor_id()
          ? Option<ExecutorID>(task->executor_id())
          : None();

      StatusUpdate update = protobuf::createStatusUpdate(
          framework->id(),
          task->node_id(),
          task->task_id(),
          state,
          TaskStatus::SOURCE_CONTROLLER,
          None(),
          "Reconciliation: Latest task state",
          TaskStatus::REASON_RECONCILIATION,
          executorId,
          protobuf::getTaskHealth(*task),
          protobuf::getTaskCheckStatus(*task),
          None(),  // `labels`.
          protobuf::getTaskContainerStatus(*task),
          None(),  // `unreachable_time`.
          None(),  // `limitation`.
          protobuf::getTaskProgress(*task));

      VLOG(1) << "Sending implicit reconciliation state "
              << update.status().state()
              << " for task " << update.status().task_id()
              << " of framework " << *framework;

      // TODO(bmahler): Consider using forward(); might lead to too
      // much logging.
      StatusUpdateMessage message;
      *message.mutable_update() = std::move(update);
      framework->send(message);
    }

    return;
  }

  // Explicit reconciliation.
  LOG(INFO) << "Performing explicit task state reconciliation"
            << " for " << reconcile.tasks().size() << " tasks"
            << " of framework " << *framework;

  // Explicit reconciliation occurs for the following cases:
  //   (1) Task is known: send the latest state.
  //   (2) Task is unknown, node is recovered: no-op.
  //   (3) Task is unknown, node is registered: TASK_GONE.
  //   (4) Task is unknown, node is unreachable: TASK_UNREACHABLE.
  //   (5) Task is unknown, node is gone: TASK_GONE_BY_OPERATOR.
  //   (6) Task is unknown, node is unknown: TASK_UNKNOWN.
  //
  // For case (2), if the node ID is not provided, we err on the
  // side of caution and do not reply if there are *any* recovered
  // nodes that haven't reregistered, since the task could reside
  // on one of these nodes.
  //
  // For cases (3), (4), (5) and (6) TASK_LOST is sent instead if the
  // framework has not opted-in to the PARTITION_AWARE capability.
  foreach (const scheduler::Call::Reconcile::Task& t, reconcile.tasks()) {
    Option<NodeID> nodeId = None();
    if (t.has_node_id()) {
      nodeId = t.node_id();
    }

    Option<StatusUpdate> update = None();
    Task* task = framework->getTask(t.task_id());

    if (task != nullptr) {
      // (1) Task is known: send the latest status update state.
      const TaskState& state = task->has_status_update_state()
          ? task->status_update_state()
          : task->state();

      const Option<ExecutorID> executorId = task->has_executor_id()
          ? Option<ExecutorID>(task->executor_id())
          : None();

      update = protobuf::createStatusUpdate(
          framework->id(),
          task->node_id(),
          task->task_id(),
          state,
          TaskStatus::SOURCE_CONTROLLER,
          None(),
          "Reconciliation: Latest task state",
          TaskStatus::REASON_RECONCILIATION,
          executorId,
          protobuf::getTaskHealth(*task),
          protobuf::getTaskCheckStatus(*task),
          None(),  // `labels`.
          protobuf::getTaskContainerStatus(*task),
          None(),  // `unreachable_time`.
          None(),  // `limitation`.
          protobuf::getTaskProgress(*task));
    } else if ((nodeId.isSome() && nodes.recovered.contains(nodeId.get())) ||
               (nodeId.isNone() && !nodes.recovered.empty())) {
      // (2) Task is unknown, node is recovered: no-op. The framework
      // will have to retry this and will not receive a response until
      // the node either registers, or is marked unreachable after the
      // timeout.
      LOG(INFO) << "Dropping reconciliation of task " << t.task_id()
                << " for framework " << *framework << " because "
                << (nodeId.isSome() ?
                      "node " + stringify(nodeId.get()) + " has" :
                      "some nodes have")
                << " not yet reregistered with the controller";
    } else if (nodeId.isSome() && nodes.registered.contains(nodeId.get())) {
      // (3) Task is unknown, node is registered: TASK_GONE. If the
      // framework does not have the PARTITION_AWARE capability, send
      // TASK_LOST for backward compatibility.
      TaskState taskState = TASK_GONE;
      if (!framework->capabilities.partitionAware) {
        taskState = TASK_LOST;
      }

      update = protobuf::createStatusUpdate(
          framework->id(),
          nodeId.get(),
          t.task_id(),
          taskState,
          TaskStatus::SOURCE_CONTROLLER,
          None(),
          "Reconciliation: Task is unknown to the node",
          TaskStatus::REASON_RECONCILIATION);
    } else if (nodeId.isSome() && nodes.unreachable.contains(nodeId.get())) {
      // (4) Node is unreachable: TASK_UNREACHABLE. If the framework
      // does not have the PARTITION_AWARE capability, send TASK_LOST
      // for backward compatibility. In either case, the status update
      // also includes the time when the node was marked unreachable.
      const TimeInfo& unreachableTime = nodes.unreachable.at(nodeId.get());

      TaskState taskState = TASK_UNREACHABLE;
      if (!framework->capabilities.partitionAware) {
        taskState = TASK_LOST;
      }

      update = protobuf::createStatusUpdate(
          framework->id(),
          nodeId.get(),
          t.task_id(),
          taskState,
          TaskStatus::SOURCE_CONTROLLER,
          None(),
          "Reconciliation: Task is unreachable",
          TaskStatus::REASON_RECONCILIATION,
          None(),
          None(),
          None(),
          None(),
          None(),
          unreachableTime);
    } else if (nodeId.isSome() && nodes.gone.contains(nodeId.get())) {
      // (5) Node is gone: TASK_GONE_BY_OPERATOR. If the framework
      // does not have the PARTITION_AWARE capability, send TASK_LOST
      // for backward compatibility.
      TaskState taskState = TASK_GONE_BY_OPERATOR;
      if (!framework->capabilities.partitionAware) {
        taskState = TASK_LOST;
      }

      update = protobuf::createStatusUpdate(
          framework->id(),
          nodeId.get(),
          t.task_id(),
          taskState,
          TaskStatus::SOURCE_CONTROLLER,
          None(),
          "Reconciliation: Task is gone",
          TaskStatus::REASON_RECONCILIATION);
    } else {
      // (6) Task is unknown, node is unknown: TASK_UNKNOWN. If the
      // framework does not have the PARTITION_AWARE capability, send
      // TASK_LOST for backward compatibility.
      TaskState taskState = TASK_UNKNOWN;
      if (!framework->capabilities.partitionAware) {
        taskState = TASK_LOST;
      }

      update = protobuf::createStatusUpdate(
          framework->id(),
          nodeId,
          t.task_id(),
          taskState,
          TaskStatus::SOURCE_CONTROLLER,
          None(),
          "Reconciliation: Task is unknown",
          TaskStatus::REASON_RECONCILIATION);
    }

    if (update.isSome()) {
      VLOG(1) << "Sending explicit reconciliation state "
              << update->status().state()
              << " for task " << update->status().task_id()
              << " of framework " << *framework;

      // TODO(bmahler): Consider using forward(); might lead to too
      // much logging.
      StatusUpdateMessage message;
      *message.mutable_update() = std::move(update.get());
      framework->send(message);
    }
  }
}


void Controller::reconcileOperations(
    Framework* framework,
    scheduler::Call::ReconcileOperations&& reconcile)
{
  CHECK_NOTNULL(framework);

  ++metrics->messages_reconcile_operations;

  // We declare the following `scheduler::Event` outside the following lambda so
  // that we construct it only once and use it for all updates sent below.
  scheduler::Event update;
  update.set_type(scheduler::Event::UPDATE_OPERATION_STATUS);

  auto sendOperationUpdate = [&update, framework](OperationStatus&& status) {
    *update.mutable_update_operation_status()->mutable_status() =
      std::move(status);

    framework->send(update);
  };

  if (reconcile.operations_size() == 0) {
    // Implicit reconciliation.
    LOG(INFO) << "Performing implicit operation state reconciliation"
                 " for framework " << *framework;

    foreachvalue (Operation* operation, framework->operations) {
      OperationStatus status;
      if (operation->statuses().empty()) {
        // This can happen if the operation is pending.
        status = operation->latest_status();
      } else {
        status = *operation->statuses().rbegin();
      }

      // This update is not sent reliably, so we unset the uuid.
      status.clear_uuid();

      sendOperationUpdate(std::move(status));
    }
  }

  // Explicit reconciliation.
  LOG(INFO) << "Performing explicit operation state reconciliation for "
            << reconcile.operations_size() << " operations of framework "
            << *framework;

  // Explicit reconciliation occurs for the following cases:
  //   (1) Operation is known: the latest status sent to the framework.
  //   (2) Operation is unknown, node is recovered: OPERATION_RECOVERING.
  //   (3) Operation is unknown, node is registered: see #3 below.
  //   (4) Operation is unknown, node is unreachable: OPERATION_UNREACHABLE.
  //   (5) Operation is unknown, node is gone: OPERATION_GONE_BY_OPERATOR.
  //   (6) Operation is unknown, node is unknown: OPERATION_UNKNOWN.
  //   (7) Operation is unknown, node ID is not specified: OPERATION_UNKNOWN.

  // For #3 above, we forward reconciliation requests to the node. This
  // container is used to build up those forwarded reconciliation messages.
  // For more information, see #3 below.
  hashmap<NodeID, ReconcileOperationsMessage> forwardedReconciliations;

  foreach (const scheduler::Call::ReconcileOperations::Operation& operation,
           reconcile.operations()) {
    Option<NodeID> nodeId = None();
    if (operation.has_node_id()) {
      nodeId = operation.node_id();
    }

    Option<ResourceProviderID> resourceProviderId = None();
    if (operation.has_resource_provider_id()) {
      resourceProviderId = operation.resource_provider_id();
    }

    Option<Operation*> frameworkOperation =
      framework->getOperation(operation.operation_id());

    OperationStatus status;
    if (frameworkOperation.isSome()) {
      // (1) Operation is known: resend the latest status sent to the framework.
      if (frameworkOperation.get()->statuses().empty()) {
        // This can happen if the operation is pending.
        status = frameworkOperation.get()->latest_status();
      } else {
        status = *frameworkOperation.get()->statuses().rbegin();
      }

      // This update is not sent reliably, so we unset the uuid.
      status.clear_uuid();
    } else if (nodeId.isSome() && nodes.recovered.contains(nodeId.get())) {
      // (2) Operation is unknown, node is recovered: OPERATION_RECOVERING.
      status = protobuf::createOperationStatus(
          OperationState::OPERATION_RECOVERING,
          operation.operation_id(),
          "Reconciliation: Node is recovered but has not re-registered",
          None(),
          None(),
          nodeId,
          resourceProviderId);
    } else if (nodeId.isSome() && nodes.registered.contains(nodeId.get())) {
      // (3) Operation is unknown, node is registered: if the operation has a
      //     resource provider ID and that resource provider is not currently
      //     subscribed and the node has the NODE_OPERATION_FEEDBACK
      //     capability, then we forward the reconciliation request to the node
      //     to respond based on whether or not this resource provider has been
      //     seen before. Otherwise, we respond with OPERATION_UNKNOWN.
      Node* node = nodes.registered.get(nodeId.get());
      CHECK(node != nullptr) << nodeId.get();

      if (resourceProviderId.isSome() &&
          !node->resourceProviders.contains(resourceProviderId.get()) &&
          node->capabilities.nodeOperationFeedback) {
        // NOTE: it is intentional that we implicitly initialize the
        // `reconciliationMessage` via `operator[]` here.
        ReconcileOperationsMessage& reconciliationMessage =
          forwardedReconciliations[nodeId.get()];
        if (!reconciliationMessage.has_framework_id()) {
          reconciliationMessage.mutable_framework_id()
            ->CopyFrom(framework->id());
        }

        ReconcileOperationsMessage::Operation* forwardedOperation =
          reconciliationMessage.add_operations();

        forwardedOperation->mutable_operation_id()
          ->CopyFrom(operation.operation_id());
        if (resourceProviderId.isSome()) {
          forwardedOperation->mutable_resource_provider_id()
            ->CopyFrom(resourceProviderId.get());
        }

        // Defer sending the `reconciliationMessage` until this loop is
        // complete, as we aggregate multiple operations into the message
        // during the loop.
        continue;
      } else {
        status = protobuf::createOperationStatus(
            OperationState::OPERATION_UNKNOWN,
            operation.operation_id(),
            "Reconciliation: Operation is unknown",
            None(),
            None(),
            nodeId,
            resourceProviderId);
      }
    } else if (nodeId.isSome() && nodes.unreachable.contains(nodeId.get())) {
      // (4) Operation is unknown, node is unreachable: OPERATION_UNREACHABLE.
      status = protobuf::createOperationStatus(
          OperationState::OPERATION_UNREACHABLE,
          operation.operation_id(),
          "Reconciliation: Node is unreachable",
          None(),
          None(),
          nodeId,
          resourceProviderId);
    } else if (nodeId.isSome() && nodes.gone.contains(nodeId.get())) {
      // (5) Operation is unknown, node is gone: OPERATION_GONE_BY_OPERATOR.
      status = protobuf::createOperationStatus(
          OperationState::OPERATION_GONE_BY_OPERATOR,
          operation.operation_id(),
          "Reconciliation: Node marked gone by operator",
          None(),
          None(),
          nodeId,
          resourceProviderId);
    } else if (nodeId.isSome()) {
      // (6) Operation is unknown, node is unknown: OPERATION_UNKNOWN.
      status = protobuf::createOperationStatus(
          OperationState::OPERATION_UNKNOWN,
          operation.operation_id(),
          "Reconciliation: Both operation and node are unknown",
          None(),
          None(),
          nodeId,
          resourceProviderId);
    } else {
      // (7) Operation is unknown, node is unknown: OPERATION_UNKNOWN.
      status = protobuf::createOperationStatus(
          OperationState::OPERATION_UNKNOWN,
          operation.operation_id(),
          "Reconciliation: Operation is unknown and no 'node_id' was"
          " provided",
          None(),
          None(),
          nodeId,
          resourceProviderId);
    }

    sendOperationUpdate(std::move(status));
  }

  foreachpair (
      const NodeID& nodeId,
      const ReconcileOperationsMessage& message,
      forwardedReconciliations) {
    CHECK(nodes.registered.contains(nodeId));
    send(nodes.registered.get(nodeId)->pid, message);
  }
}


void Controller::frameworkFailoverTimeout(const FrameworkID& frameworkId,
                                      const Time& reregisteredTime)
{
  Framework* framework = getFramework(frameworkId);

  if (framework != nullptr && !framework->connected()) {
    // If the re-registration time has not changed, then the framework
    // has not reregistered within the failover timeout.
    if (framework->reregisteredTime == reregisteredTime) {
      LOG(INFO) << "Framework failover timeout, removing framework "
                << *framework;

      removeFramework(framework);
    }
  }
}


void Controller::offer(
    const FrameworkID& frameworkId,
    const hashmap<string, hashmap<NodeID, Resources>>& resources)
{
  Framework* framework = getFramework(frameworkId);

  if (framework == nullptr ||
      !framework->connected() ||
      !framework->active()) {
    LOG(WARNING) << "Controller returning resources offered to framework "
                 << frameworkId << " because the framework"
                 << " has terminated, is not connected, or is inactive";

    foreachkey (const string& role, resources) {
      foreachpair (const NodeID& nodeId,
                   const Resources& offered,
                   resources.at(role)) {
        allocator->recoverResources(
            frameworkId, nodeId, offered, None(), false);
      }
    }
    return;
  }

  size_t offersEstimate = 0u;
  foreachvalue (const auto& nodes, resources) {
    offersEstimate += nodes.size();
  }

  // Each offer we create is tied to a single node
  // and a single allocation role.
  ResourceOffersMessage message;
  message.mutable_offers()->Reserve(offersEstimate);
  message.mutable_pids()->Reserve(offersEstimate);

  // We keep track of the offer IDs so that we can log them.
  vector<OfferID> offerIds;
  offerIds.reserve(offersEstimate);

  foreachkey (const string& role, resources) {
    foreachpair (const NodeID& nodeId,
                 const Resources& offered,
                 resources.at(role)) {
      Node* node = nodes.registered.get(nodeId);

      if (node == nullptr) {
        LOG(WARNING)
          << "Controller returning resources offered to framework " << *framework
          << " because node " << nodeId << " is not valid";

        allocator->recoverResources(
            frameworkId, nodeId, offered, None(), false);

        continue;
      }

      // This could happen if the allocator dispatched 'Controller::offer' before
      // the node was deactivated in the allocator.
      if (!node->active) {
        LOG(WARNING)
          << "Controller returning resources offered because node " << *node
          << " is " << (node->connected ? "deactivated" : "disconnected");

        allocator->recoverResources(
            frameworkId, nodeId, offered, None(), false);

        continue;
      }

  #ifdef ENABLE_PORT_MAPPING_ISOLATOR
      // TODO(dhamon): This flag is required as the static allocation of
      // ephemeral ports leads to a maximum number of containers that can
      // be created on each node. Once MESOS-1654 is fixed and ephemeral
      // ports are a first class resource, this can be removed.
      if (flags.max_executors_per_node.isSome()) {
        // Check that we haven't hit the executor limit.
        size_t numExecutors = 0;
        foreachkey (const FrameworkID& frameworkId, node->executors) {
          numExecutors += node->executors[frameworkId].keys().size();
        }

        if (numExecutors >= flags.max_executors_per_node.get()) {
          LOG(WARNING) << "Controller returning resources offered because node "
                       << *node << " has reached the maximum number of "
                       << "executors";

          // Pass a default filter to avoid getting this same offer immediately
          // from the allocator. Note that a default-constructed `Filters`
          // object has its `refuse_seconds` offer filter set to 5 seconds.
          allocator->recoverResources(
              frameworkId, nodeId, offered, Filters(), false);
          continue;
        }
      }
  #endif // ENABLE_PORT_MAPPING_ISOLATOR

      // TODO(vinod): Split regular and revocable resources into
      // separate offers, so that rescinding offers with revocable
      // resources does not affect offers with regular resources.

      // TODO(bmahler): Set "https" if only "https" is supported.
      medios::URL url;
      url.set_scheme("http");
      url.mutable_address()->set_hostname(node->info.hostname());
      url.mutable_address()->set_ip(stringify(node->pid.address.ip));
      url.mutable_address()->set_port(node->pid.address.port);
      url.set_path("/" + node->pid.id);

      Offer* offer = new Offer();
      offer->mutable_id()->MergeFrom(newOfferId());
      offer->mutable_framework_id()->MergeFrom(framework->id());
      offer->mutable_node_id()->MergeFrom(node->id);
      offer->set_hostname(node->info.hostname());
      offer->mutable_url()->MergeFrom(url);
      offer->mutable_resources()->MergeFrom(offered);
      offer->mutable_attributes()->MergeFrom(node->info.attributes());
      offer->mutable_allocation_info()->set_role(role);

      if (node->info.has_domain()) {
        offer->mutable_domain()->MergeFrom(node->info.domain());
      }

      // Add all framework's executors running on this node.
      if (node->executors.contains(framework->id())) {
        const hashmap<ExecutorID, ExecutorInfo>& executors =
          node->executors[framework->id()];
        foreachkey (const ExecutorID& executorId, executors) {
          offer->add_executor_ids()->MergeFrom(executorId);
        }
      }

      // If the node in this offer is planned to be unavailable due to
      // maintenance in the future, then set the Unavailability.
      CHECK(machines.contains(node->machineId));
      if (machines[node->machineId].info.has_unavailability()) {
        offer->mutable_unavailability()->CopyFrom(
            machines[node->machineId].info.unavailability());
      }

      offers[offer->id()] = offer;

      framework->addOffer(offer);
      node->addOffer(offer);

      if (flags.offer_timeout.isSome()) {
        // Rescind the offer after the timeout elapses.
        offerTimers[offer->id()] =
          delay(flags.offer_timeout.get(),
                self(),
                &Self::offerTimeout,
                offer->id());
      }

      // TODO(jieyu): For now, we strip 'ephemeral_ports' resource from
      // offers so that frameworks do not see this resource. This is a
      // short term workaround. Revisit this once we resolve MESOS-1654.
      Offer offer_ = *offer;

      for (int i = 0; i < offer_.resources_size();) {
        if (offer_.resources(i).name() == "ephemeral_ports") {
          offer_.mutable_resources()->SwapElements(
              i, offer_.resources_size() - 1);
          offer_.mutable_resources()->RemoveLast();
        } else {
          ++i;
        }
      }

      // Per MESOS-8237, it is problematic to show the
      // `Resource.allocation_info` for pre-MULTI_ROLE schedulers.
      // Pre-MULTI_ROLE schedulers are not `AllocationInfo` aware,
      // and since they may be performing operations that
      // implicitly uses all of Resource's state (e.g. equality
      // comparison), we strip the `AllocationInfo` from `Resource`,
      // as well as Offer. The idea here is that since the
      // information doesn't provide any value to a pre-MULTI_ROLE
      // scheduler, we preserve the old `Offer` format for them.
      if (!framework->capabilities.multiRole) {
        offer_.clear_allocation_info();

        foreach (Resource& resource, *offer_.mutable_resources()) {
          resource.clear_allocation_info();
        }
      }

      if (!framework->capabilities.reservationRefinement) {
        convertResourceFormat(
            offer_.mutable_resources(), PRE_RESERVATION_REFINEMENT);
      }

      VLOG(2) << "Sending offer " << offer_.id()
              << " containing resources " << offered
              << " on node " << *node
              << " to framework " << *framework;

      offerIds.push_back(offer_.id());

      // Add the offer *AND* the corresponding node's PID.
      *message.add_offers() = std::move(offer_);
      message.add_pids(node->pid);
    }
  }

  if (message.offers().size() == 0) {
    return;
  }

  LOG(INFO) << "Sending offers " << offerIds << " to framework " << *framework;

  framework->metrics.offers_sent += message.offers().size();
  framework->send(message);
}


void Controller::inverseOffer(
    const FrameworkID& frameworkId,
    const hashmap<NodeID, UnavailableResources>& resources)
{
  if (!frameworks.registered.contains(frameworkId)) {
    LOG(INFO) << "Controller ignoring inverse offers to framework " << frameworkId
              << " because the framework has terminated";
    return;
  }

  Framework* framework = CHECK_NOTNULL(frameworks.registered.at(frameworkId));

  if (!framework->connected() || !framework->active()) {
    LOG(INFO) << "Controller ignoring inverse offers to framework " << frameworkId
              << " because the framework is "
              << (framework->active() ? "not connected" : "inactive");
    return;
  }


  // Create an inverse offer for each node and add it to the message.
  InverseOffersMessage message;
  foreachpair (const NodeID& nodeId,
               const UnavailableResources& unavailableResources,
               resources) {
    Node* node = nodes.registered.get(nodeId);

    if (node == nullptr) {
      LOG(INFO)
        << "Controller ignoring inverse offers to framework " << *framework
        << " because node " << nodeId << " is not valid";

      continue;
    }

    // This could happen if the allocator dispatched 'Controller::inverseOffer'
    // before the node was deactivated in the allocator.
    if (!node->active) {
      LOG(INFO)
        << "Controller ignoring inverse offers to framework " << *framework
        << " because node " << *node << " is "
        << (node->connected ? "deactivated" : "disconnected");

      continue;
    }

    // This could happen if the allocator dispatched `Controller::inverseOffer`
    // before the unavailability was removed in the controller.
    if (!machines.contains(node->machineId) ||
        !machines.at(node->machineId).info.has_unavailability()) {
      LOG(INFO)
        << "Controller dropping inverse offers to framework " << *framework
        << " because node " << *node << " had its unavailability revoked.";

      continue;
    }

    // TODO(bmahler): Set "https" if only "https" is supported.
    medios::URL url;
    url.set_scheme("http");
    url.mutable_address()->set_hostname(node->info.hostname());
    url.mutable_address()->set_ip(stringify(node->pid.address.ip));
    url.mutable_address()->set_port(node->pid.address.port);
    url.set_path("/" + node->pid.id);

    InverseOffer* inverseOffer = new InverseOffer();

    // We use the same id generator as regular offers so that we can
    // have unique ids across both. This way we can re-use some of the
    // `OfferID` only messages.
    inverseOffer->mutable_id()->CopyFrom(newOfferId());
    inverseOffer->mutable_framework_id()->CopyFrom(framework->id());
    inverseOffer->mutable_node_id()->CopyFrom(node->id);
    inverseOffer->mutable_url()->CopyFrom(url);
    inverseOffer->mutable_unavailability()->CopyFrom(
        unavailableResources.unavailability);

    inverseOffers[inverseOffer->id()] = inverseOffer;

    framework->addInverseOffer(inverseOffer);
    node->addInverseOffer(inverseOffer);

    // TODO(jmlvanre): Do we want a separate flag for inverse offer
    // timeout?
    if (flags.offer_timeout.isSome()) {
      // Rescind the inverse offer after the timeout elapses.
      inverseOfferTimers[inverseOffer->id()] =
        delay(flags.offer_timeout.get(),
              self(),
              &Self::inverseOfferTimeout,
              inverseOffer->id());
    }

    // Add the inverse offer *AND* the corresponding node's PID.
    message.add_inverse_offers()->CopyFrom(*inverseOffer);
    message.add_pids(node->pid);
  }

  if (message.inverse_offers().size() == 0) {
    return;
  }

  vector<OfferID> inverseOfferIds;
  foreach (const InverseOffer& inverseOffer, message.inverse_offers()) {
    inverseOfferIds.push_back(inverseOffer.id());
  }

  LOG(INFO) << "Sending inverse offers " << inverseOfferIds << " to framework "
            << *framework;

  framework->send(message);
}


// TODO(vinod): If due to network partition there are two instances
// of the framework that think they are leaders and try to
// authenticate with controller they would be stepping on each other's
// toes. Currently it is tricky to detect this case because the
// 'authenticate' message doesn't contain the 'FrameworkID'.
// 'from' is the authenticatee process with which to communicate.
// 'pid' is the framework/node process being authenticated.
void Controller::authenticate(
    const UPID& from,
    const UPID& pid,
    const string& assertedPrincipal)
{
  ++metrics->messages_authenticate;

  // An authentication request is sent by a client (node/framework)
  // in the following cases:
  //
  // 1. First time the client is connecting.
  //    This is straightforward; just proceed with authentication.
  //
  // 2. Client retried because of ZK expiration / authentication timeout.
  //    If the client is already authenticated, it will be removed from
  //    the 'authenticated' map and authentication is retried.
  //
  // 3. Client restarted.
  //   3.1. We are here after receiving 'exited()' from old client.
  //        This is safe because the client will be first marked as
  //        disconnected and then when it reregisters it will be
  //        marked as connected.
  //
  //  3.2. We are here before receiving 'exited()' from old client.
  //       This is tricky only if the PID of the client doesn't change
  //       after restart; true for node but not for framework.
  //       If the PID doesn't change the controller might mark the client
  //       disconnected *after* the client reregisters.
  //       This is safe because the client (node) will be informed
  //       about this discrepancy via ping messages so that it can
  //       reregister.

  bool erased = authenticated.erase(pid) > 0;

  if (authenticator.isNone()) {
    // Authentication is optional at the controller level. If no
    // authenticator is loaded, registration may still proceed when
    // authentication is not required, but explicit authentication requests
    // must fail.

    // TODO(tillt): We need to make sure this does not cause retries.
    // See MESOS-2379.
    LOG(ERROR) << "Received authentication request from " << pid
               << " but authenticator is not loaded";

    AuthenticationErrorMessage message;
    message.set_error("No authenticator loaded");
    send(from, message);

    return;
  }

  // If a new authentication is occurring for a client that already
  // has an authentication in progress, we discard the old one
  // (since the client is no longer interested in it) and
  // immediately proceed with the new authentication.
  if (authenticating.contains(pid)) {
    authenticating.at(pid).discard();
    authenticating.erase(pid);

    LOG(INFO) << "Re-authenticating " << pid << ";"
              << " discarding outstanding authentication";
  } else {
    LOG(INFO) << "Authenticating " << pid
              << (erased ? "; clearing previous authentication" : "");
  }

  // Start authentication. Pass any asserted principal through to the
  // authenticator (identity_assertion uses it; cryptographic authenticators
  // ignore it per DR-029).
  Option<string> asserted = assertedPrincipal.empty()
    ? Option<string>::none()
    : Option<string>(assertedPrincipal);

  const Future<Option<string>> future =
    authenticator.get()->authenticate(from, asserted);

  // Save our state.
  authenticating[pid] = future;

  future.onAny(defer(self(), &Self::_authenticate, pid, future));

  // Don't wait for authentication to complete forever.
  delay(flags.authentication_v0_timeout,
        self(),
        &Self::authenticationTimeout,
        future);
}


void Controller::_authenticate(
    const UPID& pid,
    const Future<Option<string>>& future)
{
  // Ignore stale authentication results (if the authentication
  // future has been overwritten).
  if (authenticating.get(pid) != future) {
    LOG(INFO) << "Ignoring stale authentication result of " << pid;
    return;
  }

  if (future.isReady() && future->isSome()) {
    LOG(INFO) << "Successfully authenticated principal '" << future->get()
              << "' at " << pid;

    authenticated.put(pid, future->get());
  } else if (future.isReady() && future->isNone()) {
    LOG(INFO) << "Authentication of " << pid << " was unsuccessful:"
              << " Invalid credentials";
  } else if (future.isFailed()) {
    LOG(WARNING) << "An error ocurred while attempting to authenticate " << pid
                 << ": " << future.failure();
  } else {
    LOG(INFO) << "Authentication of " << pid << " was discarded";
  }

  authenticating.erase(pid);
}


void Controller::authenticationTimeout(Future<Option<string>> future)
{
  // Note that a 'discard' here is safe even if another
  // authenticator is in progress because this copy of the future
  // corresponds to the original authenticator that started the timer.
  if (future.discard()) { // This is a no-op if the future is already ready.
    LOG(WARNING) << "Authentication timed out";
  }
}


void Controller::reconcileKnownNode(
    Node* node,
    const vector<ExecutorInfo>& executors,
    const vector<Task>& tasks)
{
  CHECK_NOTNULL(node);

  // TODO(bmahler): There's an implicit assumption here the node
  // cannot have tasks unknown to the controller. This _should_ be the
  // case since the causal relationship is:
  //   node removes task -> controller removes task
  // Add error logging for any violations of this assumption!

  // We convert the 'tasks' into a map for easier lookup below.
  multihashmap<FrameworkID, TaskID> nodeTasks;
  foreach (const Task& task, tasks) {
    nodeTasks.put(task.framework_id(), task.task_id());
  }

  // Look for tasks missing in the node's re-registration message.
  // This can occur when:
  //   (1) a launch message was dropped (e.g. node failed over), or
  //   (2) the node re-registration raced with a launch message, in
  //       which case the node actually received the task.
  // To resolve both cases correctly, we must reconcile through the
  // node. For nodes that do not support reconciliation, we keep
  // the old semantics and cover only case (1) via TASK_LOST.
  Duration pingTimeout =
    flags.node_ping_timeout * flags.max_node_ping_timeouts;
  ControllerNodeConnection connection;
  connection.set_total_ping_timeout_seconds(pingTimeout.secs());

  NodeReregisteredMessage reregistered;
  reregistered.mutable_node_id()->CopyFrom(node->id);
  reregistered.mutable_connection()->CopyFrom(connection);

  foreachkey (const FrameworkID& frameworkId, node->tasks) {
    ReconcileTasksMessage reconcile;

    foreachvalue (Task* task, node->tasks[frameworkId]) {
      if (!nodeTasks.contains(task->framework_id(), task->task_id())) {
        LOG(WARNING) << "Task " << task->task_id()
                     << " of framework " << task->framework_id()
                     << " unknown to the node " << *node
                     << " during re-registration: reconciling with the node";

        // NOTE: The node doesn't look at the task state when it
        // reconciles the task. We send the controller's view of the
        // current task state since it might be useful in the future.
        const TaskState& state = task->has_status_update_state()
            ? task->status_update_state()
            : task->state();

        TaskStatus* status = reconcile.add_statuses();
        status->mutable_task_id()->CopyFrom(task->task_id());
        status->mutable_node_id()->CopyFrom(node->id);
        status->set_state(state);
        status->set_source(TaskStatus::SOURCE_CONTROLLER);
        status->set_message("Reconciliation request");
        status->set_reason(TaskStatus::REASON_RECONCILIATION);
        status->set_timestamp(Clock::now().secs());
      }
    }

    if (reconcile.statuses_size() > 0) {
      // NOTE: This function is only invoked when a node reregisters
      // with a controller that previously knew about the node and has
      // not marked it unreachable. If the controller has any tasks for
      // the node that are not known to the node itself, it MUST
      // have the FrameworkInfo for those tasks. This is because if a
      // controller has a task that the node doesn't know about, the
      // framework must have reregistered with this controller since the
      // last controller failover.
      Framework* framework = CHECK_NOTNULL(getFramework(frameworkId));
      CHECK(!framework->recovered());

      reconcile.mutable_framework_id()->CopyFrom(frameworkId);
      reconcile.mutable_framework()->CopyFrom(framework->info);

      reregistered.add_reconciliations()->CopyFrom(reconcile);
    }
  }

  // Re-register the node.
  send(node->pid, reregistered);

  // Likewise, any executors that are present in the controller but
  // not present in the node must be removed to correctly account
  // for resources. First we index the executors for fast lookup below.
  multihashmap<FrameworkID, ExecutorID> nodeExecutors;
  foreach (const ExecutorInfo& executor, executors) {
    // Controller validates that `framework_id` is set during task launch.
    CHECK(executor.has_framework_id());
    nodeExecutors.put(executor.framework_id(), executor.executor_id());
  }

  // Now that we have the index for lookup, remove all the executors
  // in the controller that are not known to the node.
  //
  // NOTE: A copy is needed because removeExecutor modifies
  // node->executors.
  foreachkey (const FrameworkID& frameworkId, utils::copy(node->executors)) {
    foreachkey (const ExecutorID& executorId,
                utils::copy(node->executors[frameworkId])) {
      if (!nodeExecutors.contains(frameworkId, executorId)) {
        // TODO(bmahler): Reconcile executors correctly between the
        // controller and the node, see:
        // MESOS-1466, MESOS-1800, and MESOS-1720.
        LOG(WARNING) << "Executor '" << executorId
                     << "' of framework " << frameworkId
                     << " possibly unknown to the node " << *node;

        removeExecutor(node, frameworkId, executorId);
      }
    }
  }

  // Send KillTaskMessages for tasks in 'killedTasks' that are
  // still alive on the node. This could happen if the node
  // did not receive KillTaskMessage because of a partition or
  // disconnection.
  foreach (const Task& task, tasks) {
    if (!protobuf::isTerminalState(task.state()) &&
        node->killedTasks.contains(task.framework_id(), task.task_id())) {
      LOG(WARNING) << " Node " << *node
                   << " has non-terminal task " << task.task_id()
                   << " that is supposed to be killed. Killing it now!";

      KillTaskMessage message;
      message.mutable_framework_id()->MergeFrom(task.framework_id());
      message.mutable_task_id()->MergeFrom(task.task_id());
      send(node->pid, message);
    }
  }

  // Send ShutdownFrameworkMessages for frameworks that are completed.
  // This could happen if the message wasn't received by the node
  // (e.g., node was down, partitioned).
  //
  // NOTE: This is a short-term hack because this information is lost
  // when the controller fails over. Also, we only store a limited number
  // of completed frameworks.
  //
  // TODO(vinod): Revisit this when registrar is in place. It would
  // likely involve storing this information in the registrar.
  foreachvalue (const Owned<Framework>& framework,
                frameworks.completed) {
    if (nodeTasks.contains(framework->id())) {
      LOG(WARNING) << "Node " << *node
                   << " reregistered with completed framework " << *framework
                   << ". Shutting down the framework on the node";

      ShutdownFrameworkMessage message;
      message.mutable_framework_id()->MergeFrom(framework->id());
      send(node->pid, message);
    }
  }
}


void Controller::addFramework(
    Framework* framework,
    ::medios::allocator::FrameworkOptions&& allocatorOptions)
{
  CHECK_NOTNULL(framework);

  CHECK(!frameworks.registered.contains(framework->id()))
    << "Framework " << *framework << " already exists!";

  // TODO(asekretenko): Print some information about the OfferConstraintsFilter.
  LOG(INFO) << "Adding framework " << *framework << " with roles "
            << stringify(allocatorOptions.suppressedRoles) << " suppressed";

  frameworks.registered[framework->id()] = framework;

  if (framework->connected()) {
    if (framework->pid().isSome()) {
      link(framework->pid().get());
    } else {
      CHECK_SOME(framework->http());

      const StreamingHttpConnection<v1::scheduler::Event>& http =
        framework->http().get();

      http.closed()
        .onAny(defer(self(), &Self::exited, framework->id(), http));
    }
  }

  // There should be no offered resources yet!
  CHECK_EQ(Resources(), framework->totalOfferedResources);

  allocator->addFramework(
      framework->id(),
      framework->info,
      framework->usedResources,
      framework->active(),
      std::move(allocatorOptions));

  // Export framework metrics if a principal is specified in `FrameworkInfo`.

  Option<string> principal = framework->info.has_principal()
      ? Option<string>(framework->info.principal())
      : None();

  if (framework->pid().isSome()) {
    CHECK(!frameworks.principals.contains(framework->pid().get()));
    frameworks.principals.put(framework->pid().get(), principal);
  }

  if (principal.isSome()) {
    // Create new framework metrics if this framework is the first
    // one of this principal. Otherwise existing metrics are reused.
    if (!metrics->frameworks.contains(principal.get())) {
      metrics->frameworks.put(
          principal.get(),
          Owned<Metrics::Frameworks>(
            new Metrics::Frameworks(principal.get())));
    }
  }
}


void Controller::recoverFramework(const FrameworkInfo& info)
{
  CHECK(!frameworks.registered.contains(info.id()));

  Framework* framework = new Framework(this, flags, info);

  // Send a `FRAMEWORK_ADDED` event to subscribers before adding recovered tasks
  // so the framework ID referred by any succeeding `TASK_ADDED` event will be
  // known to subscribers.
  if (!subscribers.subscribed.empty()) {
    subscribers.send(protobuf::controller::event::createFrameworkAdded(*framework));
  }

  // Add active operations, tasks, and executors to the framework.
  foreachvalue (Node* node, nodes.registered) {
    if (node->tasks.contains(framework->id())) {
      foreachvalue (Task* task, node->tasks.at(framework->id())) {
        framework->addTask(task);
      }
    }

    if (node->executors.contains(framework->id())) {
      foreachvalue (const ExecutorInfo& executor,
                    node->executors.at(framework->id())) {
        framework->addExecutor(node->id, executor);
      }
    }

    // Combine all the operations of the node into one list
    // so they can be processed the same way.
    vector<Operation*> allOperations = node->operations.values();
    foreachvalue (const Node::ResourceProvider& resourceProvider,
                  node->resourceProviders) {
      foreachvalue (Operation* operation, resourceProvider.operations) {
        allOperations.push_back(operation);
      }
    }

    foreach (Operation* operation, allOperations) {
      if (operation->has_framework_id() &&
          operation->framework_id() == framework->id()) {
        framework->addOperation(operation);

        // If this is an orphaned operation, the orphan's resources
        // must be added back to the node's total, and the allocator
        // will need to be updated with the new total and allocation.
        if (node->orphanedOperations.contains(operation->uuid())) {
          LOG(INFO)
            << "Recovered orphan operation " << operation->uuid()
            << (operation->info().has_id()
                ? " (ID: " + operation->info().id().value() + ")"
                : "")
            << " on node " << operation->node_id()
            << " belonging to framework " << operation->framework_id()
            << " in state " << operation->latest_status().state();

          node->orphanedOperations.erase(operation->uuid());

          // A terminal orphan operation is one whose resources are no longer
          // allocated, but the terminal status has yet to be acknowledged.
          // The operation will be removed once this framework acknowledges it.
          if (protobuf::isTerminalState(operation->latest_status().state())) {
            continue;
          }

          Try<Resources> consumed =
            protobuf::getConsumedResources(operation->info());

          CHECK_SOME(consumed);

          Resources consumedUnallocated = consumed.get();
          consumedUnallocated.unallocate();

          node->totalResources += consumedUnallocated;
          node->usedResources[framework->id()] += consumed.get();

          hashmap<FrameworkID, Resources> usedResources;
          usedResources.put(framework->id(), consumed.get());

          // This call to `addResourceProvider()` adds orphan operation
          // resources back to the node's total and used resources. This
          // prevents these resources from being offered while the operation is
          // still pending.
          //
          // NOTE: We intentionally call `addResourceProvider()` before we call
          // `addFramework()` below because if the order were reversed, these
          // resources would be incorrectly tracked twice in the allocator.
          allocator->addResourceProvider(
              node->id,
              consumedUnallocated,
              usedResources);
        }
      }
    }
  }

  // NOTE: We intentionally call `addFramework()` after we called
  // `addResourceProvider()` above because if the order were reversed, the
  // resources of orphan operations would be incorrectly tracked twice in the
  // allocator.
  addFramework(framework, {});
}


void Controller::connectAndActivateRecoveredFramework(
    Framework* framework,
    const Option<UPID>& pid,
    const Option<StreamingHttpConnection<v1::scheduler::Event>>& http,
    const Owned<ObjectApprovers>& objectApprovers)
{
  // Exactly one of `pid` or `http` must be provided.
  CHECK(pid.isSome() != http.isSome());

  CHECK_NOTNULL(framework);
  CHECK(framework->recovered());
  CHECK(framework->offers.empty());
  CHECK(framework->inverseOffers.empty());
  CHECK(framework->pid().isNone());
  CHECK(framework->http().isNone());

  // Updating `registeredTime` here is debatable: ideally,
  // `registeredTime` would be the time at which the framework first
  // registered with the controller. However, we cannot determine this
  // because the time at which a framework first registered is not
  // persisted across controller failover.
  framework->registeredTime = Clock::now();
  framework->reregisteredTime = Clock::now();

  // Update the framework's connection state.
  if (pid.isSome()) {
    framework->updateConnection(pid.get(), objectApprovers);
    link(pid.get());
  } else {
    framework->updateConnection(http.get(), objectApprovers);
    http->closed()
      .onAny(defer(self(), &Self::exited, framework->id(), http.get()));
  }

  CHECK(framework->activate())
    << "RECOVERED framework is expected not to be active";

  allocator->activateFramework(framework->id());

  // Export framework metrics if a principal is specified in `FrameworkInfo`.
  Option<string> principal = framework->info.has_principal()
    ? Option<string>(framework->info.principal())
    : None();

  if (framework->pid().isSome()) {
    CHECK(!frameworks.principals.contains(framework->pid().get()));
    frameworks.principals.put(framework->pid().get(), principal);
  }

  // We expect the framework metrics for this principal to be created
  // when the framework is recovered. This implies that the framework
  // principal cannot change on re-registration, which is currently
  // the case (MESOS-2842).
  if (principal.isSome()) {
    CHECK(metrics->frameworks.contains(principal.get()));
  }

  if (pid.isSome()) {
    // TODO(bmahler): We have to send a registered message here for
    // the reregistering framework, per the API contract. Send
    // reregister here per MESOS-786; requires deprecation or it
    // will break frameworks.
    FrameworkRegisteredMessage message;
    message.mutable_framework_id()->MergeFrom(framework->id());
    message.mutable_controller_info()->MergeFrom(info_);
    framework->send(message);
  } else {
    FrameworkReregisteredMessage message;
    message.mutable_framework_id()->MergeFrom(framework->id());
    message.mutable_controller_info()->MergeFrom(info_);
    framework->send(message);

    // Start the heartbeat after sending SUBSCRIBED event.
    framework->heartbeat();
  }
}


void Controller::failoverFramework(
    Framework* framework,
    const StreamingHttpConnection<v1::scheduler::Event>& http,
    const Owned<ObjectApprovers>& objectApprovers)
{
  CHECK_NOTNULL(framework);

  // Notify the old connected framework that it has failed over.
  // This is safe to do even if it is a retry because the framework is expected
  // to close the old connection (and hence not receive any more responses)
  // before sending subscription request on a new connection.
  if (framework->connected()) {
    FrameworkErrorMessage message;
    message.set_message("Framework failed over");
    framework->send(message);
  }

  // If this is an upgrade, clear the authentication related data.
  if (framework->pid().isSome()) {
    authenticated.erase(framework->pid().get());

    CHECK(frameworks.principals.contains(framework->pid().get()));
    Option<string> principal = frameworks.principals[framework->pid().get()];

    frameworks.principals.erase(framework->pid().get());
  }

  framework->updateConnection(http, objectApprovers);

  http.closed()
    .onAny(defer(self(), &Self::exited, framework->id(), http));

  _failoverFramework(framework);

  // Start the heartbeat after sending SUBSCRIBED event.
  framework->heartbeat();
}


// Replace the scheduler for a framework with a new process ID, in the
// event of a scheduler failover.
void Controller::failoverFramework(
    Framework* framework,
    const UPID& newPid,
    const Owned<ObjectApprovers>& objectApprovers)
{
  CHECK_NOTNULL(framework);

  const Option<UPID> oldPid = framework->pid();

  // There are a few failover cases to consider:
  //   1. The pid has changed or it was previously a HTTP based scheduler.
  //      In these cases we definitely want to send a FrameworkErrorMessage to
  //      shut down the older scheduler.
  //   2. The pid has not changed.
  //      2.1 The old scheduler on that pid failed over to a new
  //          instance on the same pid. No need to shut down the old
  //          scheduler as it is necessarily dead.
  //      2.2 This is a duplicate message. In this case, the scheduler
  //          has not failed over, so we do not want to shut it down.
  if (oldPid != newPid && framework->connected()) {
    FrameworkErrorMessage message;
    message.set_message("Framework failed over");
    framework->send(message);
  }

  framework->updateConnection(newPid, objectApprovers);
  link(newPid);

  _failoverFramework(framework);

  CHECK_SOME(framework->pid());

  // Update the principal mapping for this framework, which is
  // needed to keep the per-principal framework metrics accurate.
  if (oldPid.isSome() && frameworks.principals.contains(oldPid.get())) {
    frameworks.principals.erase(oldPid.get());
  }

  frameworks.principals[newPid] = authenticated.get(newPid);
}


void Controller::_failoverFramework(Framework* framework)
{
  // Discard the framework's offers, if they weren't removed before.
  foreach (Offer* offer, utils::copy(framework->offers)) {
    discardOffer(offer);
  }

  // Also remove the inverse offers.
  foreach (InverseOffer* inverseOffer, utils::copy(framework->inverseOffers)) {
    allocator->updateInverseOffer(
        inverseOffer->node_id(),
        inverseOffer->framework_id(),
        UnavailableResources{
            inverseOffer->resources(),
            inverseOffer->unavailability()},
        None());

    removeInverseOffer(inverseOffer);
  }

  CHECK(!framework->recovered());

  if (framework->activate()) {
    // The framework was inactive and needs to be activated in the allocator.
    //
    // NOTE: We do this after recovering resources (above) so that
    // the allocator has the correct view of the framework's share.
    allocator->activateFramework(framework->id());
  }

  // The scheduler driver safely ignores any duplicate registration
  // messages, so we don't need to compare the old and new pids here.
  FrameworkRegisteredMessage message;
  message.mutable_framework_id()->MergeFrom(framework->id());
  message.mutable_controller_info()->MergeFrom(info_);
  framework->send(message);
}


void Controller::teardown(Framework* framework)
{
  CHECK_NOTNULL(framework);

  LOG(INFO) << "Processing TEARDOWN call for framework " << *framework;

  ++metrics->messages_unregister_framework;

  removeFramework(framework);
}


void Controller::removeFramework(Framework* framework)
{
  CHECK_NOTNULL(framework);

  LOG(INFO) << "Removing framework " << *framework;

  if (framework->active()) {
    // Deactivate framework, but don't bother rescinding offers
    // because the framework is being removed.
    deactivate(framework, false);
  }

  // The framework's offers should have been removed when the
  // framework was deactivated.
  CHECK(framework->offers.empty());
  CHECK(framework->inverseOffers.empty());

  foreachvalue (Node* node, nodes.registered) {
    // Tell nodes to shutdown the framework.
    ShutdownFrameworkMessage message;
    message.mutable_framework_id()->MergeFrom(framework->id());
    send(node->pid, message);
  }

  // Remove pointers to the framework's tasks in nodes and mark those
  // tasks as completed.
  foreachvalue (Task* task, utils::copy(framework->tasks)) {
    Node* node = nodes.registered.get(task->node_id());

    // Since we only find out about tasks when the node reregisters,
    // it must be the case that the node exists!
    CHECK(node != nullptr)
      << "Unknown node " << task->node_id()
      << " for task " << task->task_id();

    // The task is implicitly killed, and TASK_KILLED is the closest
    // state we have by now. We mark the task and remove it, without
    // sending the update. However, a task may finish during the
    // executor graceful shutdown period. By marking such task as
    // killed and moving it to completed, we lose the opportunity to
    // collect the possible finished status. We tolerate this,
    // because we expect that if the framework has been asked to shut
    // down, its user is not interested in results anymore.
    //
    // TODO(alex): Consider a more descriptive state, e.g. TASK_ABANDONED.
    //
    // TODO(neilc): Marking the task KILLED before it has actually
    // terminated is misleading. Instead, we should consider leaving
    // the task in its current state at the controller; if/when the node
    // shuts down the framework, we should arrange for a terminal
    // status update to be delivered to the controller and update the
    // state of the task at that time (MESOS-6608).
    const StatusUpdate& update = protobuf::createStatusUpdate(
        task->framework_id(),
        task->node_id(),
        task->task_id(),
        TASK_KILLED,
        TaskStatus::SOURCE_CONTROLLER,
        None(),
        "Framework " + framework->id().value() + " removed",
        TaskStatus::REASON_FRAMEWORK_REMOVED,
        (task->has_executor_id()
         ? Option<ExecutorID>(task->executor_id())
         : None()));

    updateTask(task, update);
    removeTask(task);
  }

  // Mark the framework's unreachable tasks as completed.
  foreach (const TaskID& taskId, framework->unreachableTasks.keys()) {
    const Owned<Task>& task = framework->unreachableTasks.at(taskId);

    // TODO(neilc): Per comment above, using TASK_KILLED here is not
    // ideal. It would be better to use TASK_UNREACHABLE here and only
    // transition it to a terminal state when the node reregisters
    // and the task is shutdown (MESOS-6608).
    const StatusUpdate& update = protobuf::createStatusUpdate(
        task->framework_id(),
        task->node_id(),
        task->task_id(),
        TASK_KILLED,
        TaskStatus::SOURCE_CONTROLLER,
        None(),
        "Framework " + framework->id().value() + " removed",
        TaskStatus::REASON_FRAMEWORK_REMOVED,
        (task->has_executor_id()
         ? Option<ExecutorID>(task->executor_id())
         : None()));

    updateTask(task.get(), update);

    // We don't need to remove the task from the node, because the
    // task was removed when the node was marked unreachable.
    CHECK(!nodes.registered.contains(task->node_id()))
      << "Unreachable task " << task->task_id()
      << " of framework " << task->framework_id()
      << " was found on registered node " << task->node_id();

    // Move task from unreachable map to completed map.
    framework->addCompletedTask(std::move(*task));
    framework->unreachableTasks.erase(taskId);
  }

  // Remove the framework's executors for correct resource accounting.
  foreachkey (const NodeID& nodeId, utils::copy(framework->executors)) {
    Node* node = nodes.registered.get(nodeId);

    if (node != nullptr) {
      foreachkey (const ExecutorID& executorId,
                  utils::copy(framework->executors.at(nodeId))) {
        removeExecutor(node, framework->id(), executorId);
      }
    }
  }

  hashset<Node*> nodesWithOrphanOperations;
  foreachvalue (Operation* operation, utils::copy(framework->operations)) {
    // Non-speculative operations are considered "orphaned" once the
    // originating framework is removed. The resources used by the
    // operation will remain allocated until a terminal operation
    // status update is received.
    if (!protobuf::isSpeculativeOperation(operation->info())) {
      CHECK(operation->has_node_id())
        << "External resource provider is not supported yet";

      Node* node = nodes.registered.get(operation->node_id());
      CHECK(node != nullptr) << operation->node_id();

      node->markOperationAsOrphan(operation);

      // We defer the required updates to the allocator until after the
      // framework has been removed from the allocator.
      nodesWithOrphanOperations.insert(node);
    }

    framework->removeOperation(operation);
  }

  // TODO(benh): Similar code between removeFramework and
  // failoverFramework needs to be shared!

  // TODO(benh): unlink(framework->pid);

  framework->disconnect();

  framework->unregisteredTime = Clock::now();

  foreach (const string& role, framework->roles) {
    framework->untrackUnderRole(role);
  }

  // TODO(anand): This only works for pid based frameworks. We would
  // need similar authentication logic for http frameworks.
  if (framework->pid().isSome()) {
    authenticated.erase(framework->pid().get());

    CHECK(frameworks.principals.contains(framework->pid().get()));
    Option<string> principal = frameworks.principals[framework->pid().get()];

    frameworks.principals.erase(framework->pid().get());

    // Remove the metrics for the principal if this framework is the
    // last one with this principal.
    if (principal.isSome() &&
        !frameworks.principals.contains_value(principal.get())) {
      CHECK(metrics->frameworks.contains(principal.get()));
      metrics->frameworks.erase(principal.get());
    }
  }

  // Prevent any allocations from occurring between the multiple resource
  // changes below. Removal of a framework removes allocation, while orphan
  // operations will reduce total resources.
  allocator->pause();

  // Remove the framework.
  frameworks.registered.erase(framework->id());
  allocator->removeFramework(framework->id());

  // For any pending operations, we temporarily remove the operations'
  // resources from the allocator, because these resources are technically
  // still in use by the (now removed) framework.
  foreach (Node* node, nodesWithOrphanOperations) {
    allocator->updateNode(node->id, node->info, node->totalResources);

    // NOTE: Even though we are modifying the node's total resources, we
    // do not need to rescind any offers because the resources removed cannot
    // be offered between the `removeFramework()` and `updateNode()` calls.
  }

  allocator->resume();

  const FrameworkInfo frameworkInfo = framework->info;

  // The framework pointer is now owned by `frameworks.completed`.
  frameworks.completed.set(framework->id(), Owned<Framework>(framework));

  if (!subscribers.subscribed.empty()) {
    subscribers.send(
        protobuf::controller::event::createFrameworkRemoved(frameworkInfo));
  }
}


void Controller::removeFramework(Node* node, Framework* framework)
{
  CHECK_NOTNULL(node);
  CHECK_NOTNULL(framework);

  LOG(INFO) << "Removing framework " << *framework
            << " from node " << *node;

  // Remove pointers to framework's tasks in nodes, and send status
  // updates.
  if (node->tasks.contains(framework->id())) {
    // NOTE: A copy is needed because removeTask modifies node->tasks.
    foreachvalue (Task* task, utils::copy(node->tasks.at(framework->id()))) {
      // Remove tasks that belong to this framework.
      if (task->framework_id() == framework->id()) {
        // A framework might not actually exist because the controller failed
        // over and the framework hasn't reconnected yet. For more info
        // please see the comments in 'removeFramework(Framework*)'.
        const StatusUpdate& update = protobuf::createStatusUpdate(
          task->framework_id(),
          task->node_id(),
          task->task_id(),
          TASK_LOST,
          TaskStatus::SOURCE_CONTROLLER,
          None(),
          "Node " + node->info.hostname() + " disconnected",
          TaskStatus::REASON_NODE_DISCONNECTED,
          (task->has_executor_id()
              ? Option<ExecutorID>(task->executor_id()) : None()));

        updateTask(task, update);
        removeTask(task);

        if (framework->connected()) {
          forward(update, UPID(), framework);
        }
      }
    }
  }

  // Remove the framework's executors from the node and framework
  // for proper resource accounting.
  if (node->executors.contains(framework->id())) {
    foreachkey (const ExecutorID& executorId,
                utils::copy(node->executors.at(framework->id()))) {
      removeExecutor(node, framework->id(), executorId);
    }
  }
}


void Controller::addNode(
    Node* node,
    vector<Archive::Framework>&& completedFrameworks)
{
  CHECK_NOTNULL(node);
  CHECK(!nodes.registered.contains(node->id));
  CHECK(!nodes.unreachable.contains(node->id));
  CHECK(nodes.removed.get(node->id).isNone());

  nodes.registered.put(node);

  link(node->pid);

  // Map the node to the machine it is running on.
  CHECK(!machines[node->machineId].nodes.contains(node->id));
  machines[node->machineId].nodes.insert(node->id);

  // Set up an observer for the node.
  node->observer = new NodeObserver(
      node->pid,
      node->info,
      node->id,
      self(),
      nodes.limiter,
      metrics,
      flags.node_ping_timeout,
      flags.max_node_ping_timeouts);

  spawn(node->observer);

  // Add the node's executors to the frameworks.
  foreachkey (const FrameworkID& frameworkId, node->executors) {
    Framework* framework = getFramework(frameworkId);

    // If the framework has not reregistered yet and this is the
    // first node to reregister that is running the framework, we
    // skip adding the framework's executors here. Instead, the
    // framework will be recovered in `__reregisterNode` and its
    // executors will be added by `recoverFramework`.
    if (framework == nullptr) {
      continue;
    }

    foreachvalue (const ExecutorInfo& executorInfo,
                  node->executors[frameworkId]) {
      framework->addExecutor(node->id, executorInfo);
    }
  }

  // Add the node's tasks to the frameworks.
  foreachkey (const FrameworkID& frameworkId, node->tasks) {
    Framework* framework = getFramework(frameworkId);

    // If the framework has not reregistered yet and this is the
    // first node to reregister that is running the framework, we
    // skip adding the framework's tasks here. Instead, the framework
    // will be recovered in `__reregisterNode` and its tasks will be
    // added by `recoverFramework`.
    if (framework == nullptr) {
      continue;
    }

    foreachvalue (Task* task, node->tasks[frameworkId]) {
      framework->addTask(task);
    }
  }

  // Re-add completed tasks reported by the node.
  //
  // Note that a node considers a framework completed when it has no
  // tasks/executors running for that framework. But a controller
  // considers a framework completed when the framework is removed
  // after a failover timeout.
  //
  // TODO(vinod): Reconcile the notion of a completed framework across
  // the controller and node.
  foreach (Archive::Framework& completedFramework, completedFrameworks) {
    Framework* framework = getFramework(
        completedFramework.framework_info().id());

    foreach (Task& task, *completedFramework.mutable_tasks()) {
      // As in `__reregisterNode`: a record from a node carries whatever
      // progress the node held, which this controller never validated.
      common::validation::sanitizeTaskProgress(&task);
      metrics->invalid_task_finish_outcomes += static_cast<int64_t>(
          common::validation::sanitizeTaskFinish(&task));

      if (framework != nullptr) {
        VLOG(2) << "Re-adding completed task " << task.task_id()
                << " of framework " << *framework
                << " that ran on node " << *node;

        framework->addCompletedTask(std::move(task));
      } else {
        // The framework might not be reregistered yet.
        //
        // TODO(vinod): Revisit these semantics when we store frameworks'
        // information in the registrar.
        LOG(WARNING) << "Possibly orphaned completed task " << task.task_id()
                     << " of framework " << task.framework_id()
                     << " that ran on node " << *node;
      }
    }
  }

  CHECK(machines.contains(node->machineId));

  // Only set unavailability if the protobuf has one set.
  Option<Unavailability> unavailability = None();
  if (machines[node->machineId].info.has_unavailability()) {
    unavailability = machines[node->machineId].info.unavailability();
  }

  allocator->addNode(
      node->id,
      node->info,
      google::protobuf::convert(node->capabilities.toRepeatedPtrField()),
      unavailability,
      node->totalResources,
      node->usedResources);

  if (!subscribers.subscribed.empty()) {
    subscribers.send(protobuf::controller::event::createNodeAdded(
        *node,
        nodes.draining.get(node->id),
        nodes.deactivated.contains(node->id)));
  }
}


void Controller::removeNode(
    Node* node,
    const string& message,
    Option<Counter> reason)
{
  CHECK_NOTNULL(node);

  // It would be better to remove the node here instead of continuing
  // to mark it unreachable, but probably not worth the complexity.
  if (nodes.markingUnreachable.contains(node->id)) {
    LOG(WARNING) << "Ignoring removal of node " << *node
                 << " that is in the process of being marked unreachable";

    return;
  }

  if (nodes.markingGone.contains(node->id)) {
    LOG(WARNING) << "Ignoring removal of node " << *node
                 << " that is in the process of being marked gone";

    return;
  }

  // This should not be possible, but we protect against it anyway for
  // the sake of paranoia.
  if (nodes.removing.contains(node->id)) {
    LOG(WARNING) << "Ignoring removal of node " << *node
                 << " that is in the process of being removed";

    return;
  }

  nodes.removing.insert(node->id);

  LOG(INFO) << "Removing node " << *node << ": " << message;

  // Remove this node from the registrar. Note that we update the
  // registry BEFORE we update the controller's in-memory state; this
  // means that until the registry operation has completed, the node
  // is not considered to be removed (so we might offer its resources
  // to frameworks, etc.). Ensuring that the registry update succeeds
  // before we modify in-memory state ensures that external clients
  // see consistent behavior if the controller fails over.
  registrar->apply(Owned<RegistryOperation>(new RemoveNode(node->info)))
    .onAny(defer(self(),
                 &Self::_removeNode,
                 node,
                 lambda::_1,
                 message,
                 reason));
}


void Controller::_removeNode(
    Node* node,
    const Future<bool>& registrarResult,
    const string& removalCause,
    Option<Counter> reason)
{
  CHECK_NOTNULL(node);
  CHECK(nodes.removing.contains(node->info.id()));
  nodes.removing.erase(node->info.id());

  CHECK(!registrarResult.isDiscarded());

  if (registrarResult.isFailed()) {
    LOG(FATAL) << "Failed to remove node " << *node
               << " from the registrar: " << registrarResult.failure();
  }

  // Should not happen: the controller will only try to remove nodes that
  // are currently admitted.
  CHECK(registrarResult.get())
    << "Node " << *node
    << "already removed from the registrar";

  LOG(INFO) << "Removed node " << *node << ": " << removalCause;

  ++metrics->node_removals;
  if (reason.isSome()) {
    ++utils::copy(reason.get()); // Remove const.
  }

  // We want to remove the node first, to avoid the allocator
  // re-allocating the recovered resources.
  allocator->removeNode(node->id);

  // Transition the tasks to lost and remove them.
  foreachkey (const FrameworkID& frameworkId, utils::copy(node->tasks)) {
    Framework* framework = getFramework(frameworkId);
    CHECK(framework != nullptr)
      << "Framework " << frameworkId << " not found while removing node "
      << *node << "; node tasks: " << node->tasks;

    foreachvalue (Task* task, utils::copy(node->tasks.at(frameworkId))) {
      // TODO(bmahler): Differentiate between node removal reasons
      // (e.g. unhealthy vs. unregistered for maintenance).
      const StatusUpdate& update = protobuf::createStatusUpdate(
          task->framework_id(),
          task->node_id(),
          task->task_id(),
          TASK_LOST,
          TaskStatus::SOURCE_CONTROLLER,
          None(),
          "Node " + node->info.hostname() + " removed: " + removalCause,
          TaskStatus::REASON_NODE_REMOVED,
          (task->has_executor_id() ?
              Option<ExecutorID>(task->executor_id()) : None()));

      updateTask(task, update);
      removeTask(task);

      if (!framework->connected()) {
        LOG(WARNING) << "Dropping update " << update
                     << " for unknown framework " << frameworkId;
      } else {
        forward(update, UPID(), framework);
      }
    }
  }

  // Remove executors from the node for proper resource accounting.
  foreachkey (const FrameworkID& frameworkId, utils::copy(node->executors)) {
    foreachkey (const ExecutorID& executorId,
                utils::copy(node->executors.at(frameworkId))) {
      removeExecutor(node, frameworkId, executorId);
    }
  }

  foreach (Offer* offer, utils::copy(node->offers)) {
    rescindOffer(offer);
  }

  // Remove inverse offers because sending them for a node that is
  // gone doesn't make sense.
  foreach (InverseOffer* inverseOffer, utils::copy(node->inverseOffers)) {
    // We don't need to update the allocator because we've already called
    // `RemoveNode()`.
    // Remove and rescind inverse offers.
    removeInverseOffer(inverseOffer, true); // Rescind!
  }

  // Usually, operations are removed when the framework acknowledges
  // a terminal operation status update. However, currently only operations
  // on registered nodes can be acknowledged. Since we're about to remove
  // this node from the list of registered nodes, clean out all outstanding
  // operations to prevent leaks.
  //
  // NOTE: If the node comes back, there will be a brief window between
  // the `ReregisterNodeMessage` and the first `UpdateNodeMessage` where
  // where the controller will not be able to give correct answers to operation
  // reconciliation requests. However, since the same thing happens during
  // controller failover, the scheduler must be able to handle this scenario
  // anyway so we allow it to happen here.
  //
  // TODO(bevers): The operations removed here are implicitly transitioned
  // to `OPERATION_UNKNOWN` state, but we don't have a corresponding metric
  // for that, nor is it the correct state.
  foreachvalue (Operation* operation, utils::copy(node->operations)) {
    removeOperation(operation);
  }

  foreachvalue (
      const Node::ResourceProvider& provider,
      node->resourceProviders) {
    foreachvalue (
        Operation* operation,
        utils::copy(provider.operations)) {
      removeOperation(operation);
    }
  }

  // Mark the node as being removed.
  nodes.registered.remove(node);
  nodes.removed.put(node->id, Nothing());
  authenticated.erase(node->pid);

  // Remove the node from the `machines` mapping.
  CHECK(machines.contains(node->machineId));
  CHECK(machines[node->machineId].nodes.contains(node->id));
  machines[node->machineId].nodes.erase(node->id);

  // Remove any draining information about the node.
  // NOTE: This should not be mirrored by `__removeNode` because that
  // method handles both unreachable and gone nodes. Unreachable nodes
  // will retain their draining information, but gone nodes will not.
  nodes.draining.erase(node->id);
  nodes.deactivated.erase(node->id);

  // Kill the node observer.
  terminate(node->observer);
  wait(node->observer);
  delete node->observer;

  // TODO(benh): unlink(node->pid);

  sendNodeLost(node->info);

  if (!subscribers.subscribed.empty()) {
    subscribers.send(protobuf::controller::event::createNodeRemoved(node->id));
  }

  delete node;
}


void Controller::__removeNode(
    Node* node,
    const string& message,
    const Option<TimeInfo>& unreachableTime)
{
  // We want to remove the node first, to avoid the allocator
  // re-allocating the recovered resources.
  allocator->removeNode(node->id);

  // Transition tasks to TASK_UNREACHABLE/TASK_GONE_BY_OPERATOR/TASK_LOST
  // and remove them. We only use TASK_UNREACHABLE/TASK_GONE_BY_OPERATOR if
  // the framework has opted in to the PARTITION_AWARE capability.
  foreachkey (const FrameworkID& frameworkId, utils::copy(node->tasks)) {
    Framework* framework = getFramework(frameworkId);
    CHECK(framework != nullptr)
      << "Framework " << frameworkId << " not found while removing node "
      << *node << "; node tasks: " << node->tasks;

    TaskState newTaskState = TASK_UNREACHABLE;
    TaskStatus::Reason newTaskReason = TaskStatus::REASON_NODE_REMOVED;

    // Needed to convey task unreachability because we lose this
    // information from the task state if `TASK_LOST` is used.
    bool unreachable = true;

    if (!framework->capabilities.partitionAware) {
      newTaskState = TASK_LOST;
    } else if (unreachableTime.isNone()) {
      unreachable = false;
      newTaskState = TASK_GONE_BY_OPERATOR;
      newTaskReason = TaskStatus::REASON_NODE_REMOVED_BY_OPERATOR;
    }

    foreachvalue (Task* task, utils::copy(node->tasks.at(frameworkId))) {
      const StatusUpdate& update = protobuf::createStatusUpdate(
          task->framework_id(),
          task->node_id(),
          task->task_id(),
          newTaskState,
          TaskStatus::SOURCE_CONTROLLER,
          None(),
          message,
          newTaskReason,
          (task->has_executor_id() ?
              Option<ExecutorID>(task->executor_id()) : None()),
          None(),
          None(),
          None(),
          None(),
          unreachableTime.isSome() ? unreachableTime : None());

      updateTask(task, update);
      removeTask(task, unreachable);

      if (!framework->connected()) {
        LOG(WARNING) << "Dropping update " << update
                     << " for disconnected "
                     << " framework " << frameworkId;
      } else {
        forward(update, UPID(), framework);
      }
    }
  }

  // Remove executors from the node for proper resource accounting.
  foreachkey (const FrameworkID& frameworkId, utils::copy(node->executors)) {
    foreachkey (const ExecutorID& executorId,
                utils::copy(node->executors.at(frameworkId))) {
      removeExecutor(node, frameworkId, executorId);
    }
  }

  foreach (Offer* offer, utils::copy(node->offers)) {
    rescindOffer(offer);
  }

  // Remove inverse offers because sending them for a node that is
  // unreachable doesn't make sense.
  foreach (InverseOffer* inverseOffer, utils::copy(node->inverseOffers)) {
    // We don't need to update the allocator because we've already called
    // `RemoveNode()`.
    // Remove and rescind inverse offers.
    removeInverseOffer(inverseOffer, true); // Rescind!
  }

  // Usually, operations are removed when the framework acknowledges
  // a terminal operation status update. However, currently only operations
  // on registered nodes can be acknowledged. Since we're about to remove
  // this node from the list of registered nodes, clean out all outstanding
  // operations to prevent leaks.
  //
  // NOTE: If the node comes back, there will be a brief window between
  // the `ReregisterNodeMessage` and the first `UpdateNodeMessage` where
  // where the controller will not be able to give correct answers to operation
  // reconciliation requests. However, since the same thing happens during
  // controller failover, the scheduler must be able to handle this scenario
  // anyway so we allow it to happen here.
  OperationState transitionState = unreachableTime.isSome() ?
    OPERATION_UNREACHABLE :
    OPERATION_GONE_BY_OPERATOR;

  foreachvalue (Operation* operation, utils::copy(node->operations)) {
    metrics->incrementOperationState(
        operation->info().type(),
        transitionState);
    removeOperation(operation);
  }

  foreachvalue (
      const Node::ResourceProvider& provider,
      node->resourceProviders) {
    foreachvalue (
        Operation* operation,
        utils::copy(provider.operations)) {
      metrics->incrementOperationState(
          operation->info().type(),
          transitionState);
      removeOperation(operation);
    }
  }

  // Mark the node as being removed.
  nodes.registered.remove(node);
  nodes.removed.put(node->id, Nothing());
  authenticated.erase(node->pid);

  // Remove the node from the `machines` mapping.
  CHECK(machines.contains(node->machineId));
  CHECK(machines[node->machineId].nodes.contains(node->id));
  machines[node->machineId].nodes.erase(node->id);

  // Kill the node observer.
  terminate(node->observer);
  wait(node->observer);
  delete node->observer;

  // TODO(benh): unlink(node->pid);

  // TODO(bmahler): Tell partition aware frameworks that the
  // node is unreachable rather than lost, if applicable.
  // This requires a new capability.
  sendNodeLost(node->info);

  delete node;
}


void Controller::updateTask(Task* task, const StatusUpdate& update)
{
  CHECK_NOTNULL(task);

  // Get the unacknowledged status.
  const TaskStatus& status = update.status();

  // NOTE: Refer to comments on `StatusUpdate` message in messages.proto for
  // the difference between `update.latest_state()` and `status.state()`.

  // Updates from the node have 'latest_state' set.
  Option<TaskState> latestState;
  if (update.has_latest_state()) {
    latestState = update.latest_state();
  }

  const TaskState updateState = latestState.getOrElse(status.state());

  // Determine whether the task transitioned to terminal or
  // unreachable prior to changing the task state.
  auto isTerminalOrUnreachableState = [](const TaskState& state) {
    return protobuf::isTerminalState(state) || state == TASK_UNREACHABLE;
  };

  bool transitionedToTerminalOrUnreachable =
    !isTerminalOrUnreachableState(task->state()) &&
    isTerminalOrUnreachableState(updateState);

  // Indicates whether we should send a notification to subscribers,
  // set if the task transitioned to a new state.
  bool sendSubscribersUpdate = false;

  Framework* framework = getFramework(task->framework_id());

  // If the task has already transitioned to a terminal state,
  // do not update its state. Note that we are being defensive
  // here because this should not happen unless there is a bug
  // in the controller code.
  //
  // TODO(bmahler): Check that we're not transitioning from
  // TASK_UNREACHABLE to another state.
  if (!protobuf::isTerminalState(task->state())) {
    if (task->state() != updateState && framework != nullptr) {
      // When we observe a transition away from a non-terminal state,
      // decrement the relevant metric.
      framework->metrics.decrementActiveTaskState(task->state());

      framework->metrics.incrementTaskState(updateState);
    }

    task->set_state(updateState);
  }

  // If this is a (health) check status update, always forward it to
  // subscribers.
  if (status.reason() == TaskStatus::REASON_TASK_CHECK_STATUS_UPDATED ||
      status.reason() == TaskStatus::REASON_TASK_HEALTH_CHECK_STATUS_UPDATED) {
    sendSubscribersUpdate = true;
  }

  // The controller holds the task's last reported progress, so a status
  // that reports none must not erase one already recorded. The
  // collapse below drops the previous status of the same state, and a health
  // or check update is exactly that shape: state TASK_RUNNING, reason
  // `REASON_TASK_HEALTH_CHECK_STATUS_UPDATED`, no progress of its own. Read
  // the recorded value before the collapse and stamp it onto the appended
  // status.
  //
  // This is the controller's record, not the update it forwards: a framework
  // watching live sees each update as the executor sent it, and reads the
  // retained value through reconciliation or `/state`.
  Option<TaskStatus::Progress> recordedProgress = None();
  if (!status.has_progress()) {
    recordedProgress = protobuf::getTaskProgress(*task);
  }

  // TODO(brenden): Consider wiping the `message` field?
  if (task->statuses_size() > 0 &&
      task->statuses(task->statuses_size() - 1).state() == status.state()) {
    task->mutable_statuses()->RemoveLast();
  } else {
    // Send a `TASK_UPDATED` event for every new task state.
    sendSubscribersUpdate = true;
  }
  task->add_statuses()->CopyFrom(status);

  if (recordedProgress.isSome()) {
    task->mutable_statuses(task->statuses_size() - 1)
      ->mutable_progress()->CopyFrom(recordedProgress.get());
  }

  // Delete data (maybe very large since it's stored by on-top framework) we
  // are not interested in to avoid OOM.
  // For example: medios-controller is running on a machine with 4GB free memory,
  // if every task stores 10MB data into TaskStatus, then medios-controller will be
  // killed by OOM killer after 400 tasks have finished.
  // MESOS-1746.
  task->mutable_statuses(task->statuses_size() - 1)->clear_data();

  if (sendSubscribersUpdate && !subscribers.subscribed.empty()) {
    // If the framework has been removed, the task would have already
    // transitioned to `TASK_KILLED` by `removeFramework()`, thus
    // `sendSubscribersUpdate` shouldn't have been set to true.
    // TODO(chhsiao): This may be changed after MESOS-6608 is resolved.
    CHECK_NOTNULL(framework);

    subscribers.send(
        protobuf::controller::event::createTaskUpdated(
            *task, task->state(), status),
        framework->info,
        *task);
  }

  LOG(INFO) << "Updating the state of task " << task->task_id()
            << " of framework " << task->framework_id()
            << " (latest state: " << task->state()
            << ", status update state: " << status.state() << ")";

  // Once the task transitioned to terminal or unreachable,
  // recover the resources.
  if (transitionedToTerminalOrUnreachable) {
    allocator->recoverResources(
        task->framework_id(),
        task->node_id(),
        task->resources(),
        None(),
        true);

    // The node owns the Task object and cannot be nullptr.
    Node* node = nodes.registered.get(task->node_id());
    CHECK(node != nullptr) << task->node_id();

    node->recoverResources(task);

    if (framework != nullptr) {
      framework->recoverResources(task);
    }

    switch (status.state()) {
      case TASK_FINISHED:         ++metrics->tasks_finished;         break;
      case TASK_FAILED:           ++metrics->tasks_failed;           break;
      case TASK_KILLED:           ++metrics->tasks_killed;           break;
      case TASK_LOST:             ++metrics->tasks_lost;             break;
      case TASK_ERROR:            ++metrics->tasks_error;            break;
      case TASK_DROPPED:          ++metrics->tasks_dropped;          break;
      case TASK_GONE:             ++metrics->tasks_gone;             break;
      case TASK_GONE_BY_OPERATOR: ++metrics->tasks_gone_by_operator; break;

      // The following are non-terminal and use gauge based metrics.
      case TASK_STARTING:    break;
      case TASK_STAGING:     break;
      case TASK_RUNNING:     break;
      case TASK_KILLING:     break;
      case TASK_UNREACHABLE: break;

      // Should not happen.
      case TASK_UNKNOWN:
        LOG(FATAL) << "Unexpected TASK_UNKNOWN for in-memory task";
        break;
    }

    if (status.has_reason()) {
      metrics->incrementTasksStates(
          status.state(),
          status.source(),
          status.reason());
    }
  }
}


void Controller::removeTask(Task* task, bool unreachable)
{
  CHECK_NOTNULL(task);

  // The node owns the Task object and cannot be nullptr.
  Node* node = nodes.registered.get(task->node_id());
  CHECK(node != nullptr) << task->node_id();

  // Note that we explicitly convert from protobuf to `Resources` here
  // and then use the result below to avoid performance penalty for multiple
  // conversions and validations implied by conversion.
  // Conversion is safe, as resources have already passed validation.
  const Resources resources = task->resources();

  // The invariant here is that the controller will recover the resources
  // prior to removing terminal or unreachable tasks. If the task is
  // not terminal or unreachable, we must recover the resources here.
  //
  // TODO(bmahler): Currently, only `Controller::finalize()` will call
  // `removeTask()` with a non-terminal task. Consider fixing this
  // and instead CHECKing here to simplify the logic.
  if (!protobuf::isTerminalState(task->state()) &&
      task->state() != TASK_UNREACHABLE) {
    CHECK(!unreachable) << task->task_id();

    // Note that we use `Resources` for output as it's faster than
    // logging raw protobuf data.
    LOG(WARNING) << "Removing task " << task->task_id()
                 << " with resources " << resources
                 << " of framework " << task->framework_id()
                 << " on node " << *node
                 << " in non-terminal state " << task->state();

    allocator->recoverResources(
        task->framework_id(),
        task->node_id(),
        resources,
        None(),
        true);
  } else {
    // Note that we use `Resources` for output as it's faster than
    // logging raw protobuf data.
    LOG(INFO) << "Removing task " << task->task_id()
              << " with resources " << resources
              << " of framework " << task->framework_id()
              << " on node " << *node;
  }

  if (unreachable) {
    nodes.unreachableTasks[node->id][task->framework_id()]
      .push_back(task->task_id());
  }

  // Remove from framework.
  Framework* framework = getFramework(task->framework_id());
  if (framework != nullptr) { // A framework might not be reregistered yet.
    framework->removeTask(task, unreachable);
  }

  // Remove from node.
  node->removeTask(task);

  delete task;
}


void Controller::removeExecutor(
    Node* node,
    const FrameworkID& frameworkId,
    const ExecutorID& executorId)
{
  CHECK_NOTNULL(node);
  CHECK(node->hasExecutor(frameworkId, executorId));

  ExecutorInfo executor = node->executors[frameworkId][executorId];

  // Note that we explicitly convert from protobuf to `Resources` here
  // and then use the result below to avoid performance penalty for multiple
  // conversions and validations implied by conversion.
  // Conversion is safe, as resources have already passed validation.
  const Resources resources = executor.resources();

  LOG(INFO) << "Removing executor '" << executorId
            << "' with resources " << resources
            << " of framework " << frameworkId << " on node " << *node;

  allocator->recoverResources(frameworkId, node->id, resources, None(), true);

  Framework* framework = getFramework(frameworkId);
  if (framework != nullptr) { // The framework might not be reregistered yet.
    framework->removeExecutor(node->id, executorId);
  }

  node->removeExecutor(frameworkId, executorId);
}


void Controller::addOperation(
    Framework* framework,
    Node* node,
    Operation* operation)
{
  CHECK_NOTNULL(operation);
  CHECK_NOTNULL(node);

  metrics->incrementOperationState(
      operation->info().type(), operation->latest_status().state());

  node->addOperation(operation);

  if (framework != nullptr) {
    framework->addOperation(operation);
  } else {
    // When the framework is not known by the controller, this means either:
    //   * The framework has been completed.
    //   * The framework has no known tasks and has yet to reregister.
    // The controller cannot always differentiate these cases, because completed
    // frameworks are only kept in memory, in a circular buffer.
    //
    // TODO(josephw): Once MESOS-8582 is resolved, operations may include
    // enough information to add a framework entry, which means only
    // completed frameworks would result in orphans.
    //
    // These operations will be preemptively considered "orphans" and
    // will be given a grace period before the controller adopts them.
    // After which, the controller will acknowledge any associated operation
    // status updates.
    node->markOperationAsOrphan(operation);
  }
}


void Controller::updateOperation(
    Operation* operation,
    const UpdateOperationStatusMessage& update,
    bool convertResources)
{
  CHECK_NOTNULL(operation);

  const OperationStatus& status =
    update.has_latest_status() ? update.latest_status() : update.status();

  LOG(INFO) << "Updating the state of operation '" << operation->info().id()
            << "' (uuid: " << update.operation_uuid() << ") for"
            << (operation->has_framework_id()
                  ? " framework " + stringify(operation->framework_id())
                  : " an operator API call")
            << " (latest state: " << operation->latest_status().state()
            << ", status update state: " << status.state() << ")";

  metrics->transitionOperationState(
      operation->info().type(),
      operation->latest_status().state(),
      status.state());

  // Whether the operation has just become terminated.
  const bool terminated =
    !protobuf::isTerminalState(operation->latest_status().state()) &&
    protobuf::isTerminalState(status.state());

  // If the operation has already transitioned to a terminal state,
  // do not update its state.
  if (!protobuf::isTerminalState(operation->latest_status().state())) {
    operation->mutable_latest_status()->CopyFrom(status);
  }

  // TODO(gkleiman): Revisit the de-duplication logic (MESOS-8441) - if two
  // different terminal statuses arrive, we could end up with different states
  // in `latest_status` and the front of statuses list.
  if (operation->statuses().empty() ||
      *(operation->statuses().rbegin()) != status) {
    operation->add_statuses()->CopyFrom(status);
  }

  if (!terminated) {
    return;
  }

  // Update resource accounting in the controller and in the allocator.
  // NOTE: For the "old" operations (RESERVE, UNRESERVE, CREATE,
  // DESTROY), the controller speculatively assumes that the operation
  // will be successful when it accepts the operations. Therefore, we
  // don't need to update the resource accounting for those types of
  // operations in the controller and in the allocator states upon
  // receiving a terminal status update.
  if (protobuf::isSpeculativeOperation(operation->info())) {
    return;
  }

  // We currently do not support non-speculated operations not
  // triggered by a framework (e.g., over the operator API).
  CHECK(operation->has_framework_id());

  Try<Resources> consumed = protobuf::getConsumedResources(operation->info());
  CHECK_SOME(consumed);

  CHECK(operation->has_node_id())
    << "External resource provider is not supported yet";

  // The node owns the Operation object and cannot be nullptr.
  // TODO(jieyu): Revisit this once we introduce support for external
  // resource provider.
  Node* node = nodes.registered.get(operation->node_id());
  CHECK(node != nullptr) << operation->node_id();

  // Orphaned operations are handled differently, because the allocator
  // has no knowledge of resources consumed by these operations;
  // and any resource consumption is accounted for in the node's total
  // resources.
  if (node->orphanedOperations.contains(operation->uuid())) {
    bool updated = false;

    switch (operation->latest_status().state()) {
      // Terminal state, and the conversion is successful.
      case OPERATION_FINISHED: {
        const Resources converted =
          operation->latest_status().converted_resources();

        if (convertResources) {
          Resources convertedUnallocated = converted;
          convertedUnallocated.unallocate();

          node->totalResources += convertedUnallocated;
          updated = true;
        } else {
          // NOTE: This is only reachable when an existing orphan operation
          // is transitioned to a terminal status via an UpdateNodeMessage.
          // When this happens, the `node->totalResources` already contains
          // the converted resources.
          // The handling for normal operations must recover the consumed
          // resources from the allocator. We cannot mirror this resource
          // recovery (i.e. `node->totalResources += consumed.get()`) because
          // the resource has already been converted and no longer exists.
        }

        break;
      }

      // Terminal state, and the conversion has failed.
      case OPERATION_DROPPED:
      case OPERATION_ERROR:
      case OPERATION_FAILED:
      case OPERATION_GONE_BY_OPERATOR: {
        Resources consumedUnallocated = consumed.get();
        consumedUnallocated.unallocate();

        node->totalResources += consumedUnallocated;
        updated = true;
        break;
      }

      // Non-terminal or not expected from a node. This shouldn't happen.
      case OPERATION_UNSUPPORTED:
      case OPERATION_PENDING:
      case OPERATION_UNREACHABLE:
      case OPERATION_RECOVERING:
      case OPERATION_UNKNOWN: {
        LOG(FATAL) << "Unexpected operation state "
                   << operation->latest_status().state();

        break;
      }
    }

    // If we've added resources to the node's total, the allocator
    // must be informed about the new totals.
    if (updated) {
      allocator->updateNode(node->id, node->info, node->totalResources);
    }

  } else {
    switch (operation->latest_status().state()) {
      // Terminal state, and the conversion is successful.
      case OPERATION_FINISHED: {
        const Resources converted =
          operation->latest_status().converted_resources();

        if (convertResources) {
          allocator->updateAllocation(
              operation->framework_id(),
              operation->node_id(),
              consumed.get(),
              {ResourceConversion(consumed.get(), converted)});

          allocator->recoverResources(
              operation->framework_id(),
              operation->node_id(),
              converted,
              None(),
              false);

          Resources consumedUnallocated = consumed.get();
          consumedUnallocated.unallocate();

          Resources convertedUnallocated = converted;
          convertedUnallocated.unallocate();

          node->apply(
              {ResourceConversion(consumedUnallocated, convertedUnallocated)});
        } else {
          allocator->recoverResources(
              operation->framework_id(),
              operation->node_id(),
              consumed.get(),
              None(),
              false);
        }

        break;
      }

      // Terminal state, and the conversion has failed.
      case OPERATION_DROPPED:
      case OPERATION_ERROR:
      case OPERATION_FAILED:
      case OPERATION_GONE_BY_OPERATOR: {
        allocator->recoverResources(
            operation->framework_id(),
            operation->node_id(),
            consumed.get(),
            None(),
            false);

        break;
      }

      // Non-terminal or not expected from a node. This shouldn't happen.
      case OPERATION_UNSUPPORTED:
      case OPERATION_PENDING:
      case OPERATION_UNREACHABLE:
      case OPERATION_RECOVERING:
      case OPERATION_UNKNOWN: {
        LOG(FATAL) << "Unexpected operation state "
                   << operation->latest_status().state();

        break;
      }
    }

    node->recoverResources(operation);

    Framework* framework = getFramework(operation->framework_id());

    if (framework != nullptr) {
      framework->recoverResources(operation);
    }
  }
}


void Controller::removeOperation(Operation* operation)
{
  CHECK_NOTNULL(operation);

  // Remove from framework.
  Framework* framework = operation->has_framework_id()
    ? getFramework(operation->framework_id())
    : nullptr;

  if (framework != nullptr) {
    framework->removeOperation(operation);
  }

  // Remove from node.
  CHECK(operation->has_node_id())
    << "External resource provider is not supported yet";

  Node* node = nodes.registered.get(operation->node_id());
  CHECK(node != nullptr) << operation->node_id();

  node->removeOperation(operation);

  OperationState state = operation->latest_status().state();

  // The common case is that an operation is removed after a terminal status
  // update has been acknowledged, in thase we have nothing to do here because
  // the counters for terminal operations represent lifetime totals.
  // However, it can happen that we need to remove non-terminal operations,
  // e.g. when a node is marked gone or a resource provider on a node
  // disappears. In this case we need to adjust the metrics to reflect the
  // current numbers.
  if (!protobuf::isTerminalState(state)) {
    metrics->decrementOperationState(
        operation->info().type(),
        state);
  }

  // If the operation was not speculated and is not terminal we
  // need to also recover its used resources in the allocator.
  // If the operation is an orphan, the resources have already been
  // recovered from the allocator.
  if (!protobuf::isSpeculativeOperation(operation->info()) &&
      !protobuf::isTerminalState(state) &&
      !node->orphanedOperations.contains(operation->uuid())) {
    Try<Resources> consumed = protobuf::getConsumedResources(operation->info());
    CHECK_SOME(consumed);

    allocator->recoverResources(
        operation->framework_id(),
        operation->node_id(),
        consumed.get(),
        None(),
        false);
  }

  delete operation;
}


Future<Nothing> Controller::apply(Node* node, const Offer::Operation& operation)
{
  CHECK_NOTNULL(node);

  return allocator->updateAvailable(node->id, {operation})
    .onReady(defer(self(), &Controller::_apply, node, nullptr, operation));
}


void Controller::_apply(
    Node* node,
    Framework* framework,
    const Offer::Operation& operationInfo)
{
  CHECK_NOTNULL(node);

  if (node->capabilities.resourceProvider) {
    Result<ResourceProviderID> resourceProviderId =
      getResourceProviderId(operationInfo);

    // This must have been validated by the caller.
    CHECK(!resourceProviderId.isError());

    CHECK(
        resourceProviderId.isNone() ||
        node->resourceProviders.contains(resourceProviderId.get()))
      << "Resource provider " + stringify(resourceProviderId.get()) +
           " is unknown";

    CHECK_SOME(node->resourceVersion);

    const UUID resourceVersion = resourceProviderId.isNone()
      ? node->resourceVersion.get()
      : node->resourceProviders.at(resourceProviderId.get()).resourceVersion;

    Operation* operation = new Operation(protobuf::createOperation(
        operationInfo,
        protobuf::createOperationStatus(
            OPERATION_PENDING,
            operationInfo.has_id()
              ? operationInfo.id()
              : Option<OperationID>::none(),
            None(),
            None(),
            None(),
            node->id,
            resourceProviderId.isSome()
              ? Some(resourceProviderId.get())
              : Option<ResourceProviderID>::none()),
        framework != nullptr ? framework->id() : Option<FrameworkID>::none(),
        node->id));

    addOperation(framework, node, operation);

    if (protobuf::isSpeculativeOperation(operation->info())) {
      Offer::Operation strippedOperationInfo = operation->info();
      protobuf::stripAllocationInfo(&strippedOperationInfo);

      Try<vector<ResourceConversion>> conversions =
        getResourceConversions(strippedOperationInfo);

      CHECK_SOME(conversions);

      node->apply(conversions.get());
    }

    ApplyOperationMessage message;
    if (framework != nullptr) {
      message.mutable_framework_id()->CopyFrom(framework->id());
    }
    message.mutable_operation_info()->CopyFrom(operation->info());
    message.mutable_operation_uuid()->CopyFrom(operation->uuid());
    if (resourceProviderId.isSome()) {
      message.mutable_resource_version_uuid()
        ->mutable_resource_provider_id()
        ->CopyFrom(resourceProviderId.get());
    }

    message.mutable_resource_version_uuid()->mutable_uuid()->CopyFrom(
        resourceVersion);

    LOG(INFO) << "Sending operation '" << operation->info().id()
              << "' (uuid: " << operation->uuid() << ") "
              << "to node " << *node;

    send(node->pid, message);
  } else {
    if (!protobuf::isSpeculativeOperation(operationInfo)) {
      LOG(FATAL) << "Unexpected operation to apply on node " << *node;
    }

    // We need to strip the allocation info from the operation's
    // resources in order to apply the operation successfully
    // since the node's total is stored as unallocated resources.
    Offer::Operation strippedOperationInfo = operationInfo;
    protobuf::stripAllocationInfo(&strippedOperationInfo);

    Try<vector<ResourceConversion>> conversions =
      getResourceConversions(strippedOperationInfo);

    CHECK_SOME(conversions);

    node->apply(conversions.get());

    CheckpointResourcesMessage message;

    message.mutable_resources()->CopyFrom(node->checkpointedResources);

    if (!node->capabilities.reservationRefinement) {
      // If the node is not refinement-capable, don't send it
      // checkpointed resources that contain refined reservations. This
      // might occur if a reservation refinement is created but never
      // reaches the node (e.g., due to network partition), and then
      // the node is downgraded before the partition heals.
      //
      // TODO(neilc): It would probably be better to prevent the node
      // from reregistering in this scenario.
      Try<Nothing> result = downgradeResources(&message);
      if (result.isError()) {
        LOG(WARNING) << "Not sending updated checkpointed resources "
                     << node->checkpointedResources
                     << " with refined reservations, since node " << *node
                     << " is not RESERVATION_REFINEMENT-capable.";

        return;
      }
    }

    LOG(INFO) << "Sending updated checkpointed resources "
              << node->checkpointedResources
              << " to node " << *node;

    send(node->pid, message);
  }

  if (framework != nullptr) {
    // We increment per-framework operation metrics for all operations except
    // LAUNCH and LAUNCH_GROUP here.
    framework->metrics.incrementOperation(operationInfo);
  }
}


void Controller::offerTimeout(const OfferID& offerId)
{
  Offer* offer = getOffer(offerId);
  if (offer != nullptr) {
    rescindOffer(offer);
  }
}


void Controller::rescindOffer(Offer* offer, const Option<Filters>& filters)
{
  Framework* framework = getFramework(offer->framework_id());
  CHECK(framework != nullptr)
    << "Unknown framework " << offer->framework_id()
    << " in the offer " << offer->id();

  RescindResourceOfferMessage message;
  message.mutable_offer_id()->MergeFrom(offer->id());

  framework->metrics.offers_rescinded++;
  framework->send(message);

  allocator->recoverResources(
      offer->framework_id(),
      offer->node_id(),
      offer->resources(),
      filters,
      false);

  _removeOffer(framework, offer);
}


void Controller::discardOffer(Offer* offer, const Option<Filters>& filters)
{
  Framework* framework = getFramework(offer->framework_id());
  CHECK(framework != nullptr)
    << "Unknown framework " << offer->framework_id()
    << " in the offer " << offer->id();

  allocator->recoverResources(
      offer->framework_id(),
      offer->node_id(),
      offer->resources(),
      filters,
      false);

  _removeOffer(framework, offer);
}


void Controller::_removeOffer(Framework* framework, Offer* offer)
{
  CHECK_EQ(framework->id(), offer->framework_id());
  framework->removeOffer(offer);

  // Remove from node.
  Node* node = nodes.registered.get(offer->node_id());

  CHECK(node != nullptr)
    << "Unknown node " << offer->node_id()
    << " in the offer " << offer->id();

  node->removeOffer(offer);

  // Remove and cancel offer removal timers. Canceling the Timers is
  // only done to avoid having too many active Timers in libprocess.
  if (offerTimers.contains(offer->id())) {
    Clock::cancel(offerTimers[offer->id()]);
    offerTimers.erase(offer->id());
  }

  // Delete it.
  LOG(INFO) << "Removing offer " << offer->id();
  offers.erase(offer->id());
  delete offer;
}


void Controller::inverseOfferTimeout(const OfferID& inverseOfferId)
{
  InverseOffer* inverseOffer = getInverseOffer(inverseOfferId);
  if (inverseOffer != nullptr) {
    allocator->updateInverseOffer(
        inverseOffer->node_id(),
        inverseOffer->framework_id(),
        UnavailableResources{
            inverseOffer->resources(),
            inverseOffer->unavailability()},
        None());

    removeInverseOffer(inverseOffer, true);
  }
}


void Controller::removeInverseOffer(InverseOffer* inverseOffer, bool rescind)
{
  // Remove from framework.
  Framework* framework = getFramework(inverseOffer->framework_id());
  CHECK(framework != nullptr)
    << "Unknown framework " << inverseOffer->framework_id()
    << " in the inverse offer " << inverseOffer->id();

  framework->removeInverseOffer(inverseOffer);

  // Remove from node.
  Node* node = nodes.registered.get(inverseOffer->node_id());

  CHECK(node != nullptr)
    << "Unknown node " << inverseOffer->node_id()
    << " in the inverse offer " << inverseOffer->id();

  node->removeInverseOffer(inverseOffer);

  if (rescind) {
    RescindInverseOfferMessage message;
    message.mutable_inverse_offer_id()->CopyFrom(inverseOffer->id());
    framework->send(message);
  }

  // Remove and cancel inverse offer removal timers. Canceling the Timers is
  // only done to avoid having too many active Timers in libprocess.
  if (inverseOfferTimers.contains(inverseOffer->id())) {
    Clock::cancel(inverseOfferTimers[inverseOffer->id()]);
    inverseOfferTimers.erase(inverseOffer->id());
  }

  // Delete it.
  inverseOffers.erase(inverseOffer->id());
  delete inverseOffer;
}


bool Controller::isCompletedFramework(const FrameworkID& frameworkId) const
{
  return frameworks.completed.contains(frameworkId);
}


// TODO(bmahler): Consider killing this.
Framework* Controller::getFramework(const FrameworkID& frameworkId) const
{
  return frameworks.registered.get(frameworkId).getOrElse(nullptr);
}


// TODO(bmahler): Consider killing this.
Offer* Controller::getOffer(const OfferID& offerId) const
{
  return offers.contains(offerId) ? offers.at(offerId) : nullptr;
}


// TODO(bmahler): Consider killing this.
InverseOffer* Controller::getInverseOffer(const OfferID& inverseOfferId) const
{
  return inverseOffers.contains(inverseOfferId)
           ? inverseOffers.at(inverseOfferId)
           : nullptr;
}


// Create a new framework ID. We format the ID as CONTROLLERID-FWID, where
// CONTROLLERID is the ID of the controller (randomly generated UUID) and FWID
// is an increasing integer.
FrameworkID Controller::newFrameworkId()
{
  std::ostringstream out;

  out << info_.id() << "-" << std::setw(4)
      << std::setfill('0') << nextFrameworkId++;

  FrameworkID frameworkId;
  frameworkId.set_value(out.str());

  return frameworkId;
}


OfferID Controller::newOfferId()
{
  OfferID offerId;
  offerId.set_value(info_.id() + "-O" + stringify(nextOfferId++));
  return offerId;
}


NodeID Controller::newNodeId()
{
  NodeID nodeId;
  nodeId.set_value(info_.id() + "-S" + stringify(nextNodeId++));
  return nodeId;
}


double Controller::_const_nodes_connected() const
{
  double count = 0.0;
  foreachvalue (Node* node, nodes.registered) {
    if (node->connected) {
      count++;
    }
  }
  return count;
}

double Controller::_nodes_connected()
{
  return _const_nodes_connected();
}


double Controller::_const_nodes_disconnected() const
{
  double count = 0.0;
  foreachvalue (Node* node, nodes.registered) {
    if (!node->connected) {
      count++;
    }
  }
  return count;
}


double Controller::_nodes_disconnected()
{
  return _const_nodes_disconnected();
}


double Controller::_const_nodes_active() const
{
  double count = 0.0;
  foreachvalue (Node* node, nodes.registered) {
    if (node->active) {
      count++;
    }
  }
  return count;
}


double Controller::_nodes_active()
{
  return _const_nodes_active();
}


double Controller::_const_nodes_inactive() const
{
  double count = 0.0;
  foreachvalue (Node* node, nodes.registered) {
    if (!node->active) {
      count++;
    }
  }
  return count;
}


double Controller::_nodes_inactive()
{
  return _const_nodes_inactive();
}


double Controller::_const_nodes_unreachable() const
{
  double count = 0.0;
  foreachkey (const NodeID& nodeId, nodes.unreachable) {
    if (!nodes.registered.contains(nodeId)) {
      count++;
    }
  }
  return count;
}


double Controller::_nodes_unreachable()
{
  return _const_nodes_unreachable();
}


double Controller::_frameworks_connected()
{
  double count = 0.0;
  foreachvalue (Framework* framework, frameworks.registered) {
    if (framework->connected()) {
      count++;
    }
  }
  return count;
}


double Controller::_frameworks_disconnected()
{
  double count = 0.0;
  foreachvalue (Framework* framework, frameworks.registered) {
    if (!framework->connected()) {
      count++;
    }
  }
  return count;
}


double Controller::_frameworks_active()
{
  double count = 0.0;
  foreachvalue (Framework* framework, frameworks.registered) {
    if (framework->active()) {
      count++;
    }
  }
  return count;
}


double Controller::_frameworks_inactive()
{
  double count = 0.0;
  foreachvalue (Framework* framework, frameworks.registered) {
    if (!framework->active()) {
      count++;
    }
  }
  return count;
}


double Controller::_tasks_staging()
{
  double count = 0.0;

  foreachvalue (Node* node, nodes.registered) {
    typedef hashmap<TaskID, Task*> TaskMap;
    foreachvalue (const TaskMap& tasks, node->tasks) {
      foreachvalue (const Task* task, tasks) {
        if (task->state() == TASK_STAGING) {
          count++;
        }
      }
    }
  }

  return count;
}


double Controller::_tasks_starting()
{
  double count = 0.0;

  foreachvalue (Node* node, nodes.registered) {
    typedef hashmap<TaskID, Task*> TaskMap;
    foreachvalue (const TaskMap& tasks, node->tasks) {
      foreachvalue (const Task* task, tasks) {
        if (task->state() == TASK_STARTING) {
          count++;
        }
      }
    }
  }

  return count;
}


double Controller::_tasks_running()
{
  double count = 0.0;

  foreachvalue (Node* node, nodes.registered) {
    typedef hashmap<TaskID, Task*> TaskMap;
    foreachvalue (const TaskMap& tasks, node->tasks) {
      foreachvalue (const Task* task, tasks) {
        if (task->state() == TASK_RUNNING) {
          count++;
        }
      }
    }
  }

  return count;
}


double Controller::_tasks_unreachable()
{
  double count = 0.0;

  foreachvalue (Framework* framework, frameworks.registered) {
    foreachvalue (const Owned<Task>& task, framework->unreachableTasks) {
      if (task->state() == TASK_UNREACHABLE) {
        count++;
      }
    }
  }

  return count;
}


double Controller::_tasks_killing()
{
  double count = 0.0;

  foreachvalue (Node* node, nodes.registered) {
    typedef hashmap<TaskID, Task*> TaskMap;
    foreachvalue (const TaskMap& tasks, node->tasks) {
      foreachvalue (const Task* task, tasks) {
        if (task->state() == TASK_KILLING) {
          count++;
        }
      }
    }
  }

  return count;
}


double Controller::_resources_total(const string& name)
{
  double total = 0.0;

  foreachvalue (Node* node, nodes.registered) {
    foreach (const Resource& resource, node->info.resources()) {
      if (resource.name() == name && resource.type() == Value::SCALAR) {
        total += resource.scalar().value();
      }
    }
  }

  return total;
}


double Controller::_resources_used(const string& name)
{
  double used = 0.0;

  foreachvalue (Node* node, nodes.registered) {
    // We use `Resources` arithmetic to accummulate the resources since the
    // `+=` operator de-duplicates the same shared resources across frameworks.
    Resources nodeUsed;

    foreachvalue (const Resources& resources, node->usedResources) {
      nodeUsed += resources.nonRevocable();
    }

    used +=
      nodeUsed.get<Value::Scalar>(name).getOrElse(Value::Scalar()).value();
  }

  return used;
}


double Controller::_resources_percent(const string& name)
{
  double total = _resources_total(name);

  if (total == 0.0) {
    return 0.0;
  }

  return _resources_used(name) / total;
}


double Controller::_resources_revocable_total(const string& name)
{
  double total = 0.0;

  foreachvalue (Node* node, nodes.registered) {
    foreach (const Resource& resource, node->totalResources.revocable()) {
      if (resource.name() == name && resource.type() == Value::SCALAR) {
        total += resource.scalar().value();
      }
    }
  }

  return total;
}


double Controller::_resources_revocable_used(const string& name)
{
  double used = 0.0;

  foreachvalue (Node* node, nodes.registered) {
    // We use `Resources` arithmetic to accummulate the resources since the
    // `+=` operator de-duplicates the same shared resources across frameworks.
    Resources nodeUsed;

    foreachvalue (const Resources& resources, node->usedResources) {
      nodeUsed += resources.revocable();
    }

    used +=
      nodeUsed.get<Value::Scalar>(name).getOrElse(Value::Scalar()).value();
  }

  return used;
}


double Controller::_resources_revocable_percent(const string& name)
{
  double total = _resources_revocable_total(name);

  if (total == 0.0) {
    return 0.0;
  }

  return _resources_revocable_used(name) / total;
}


void Controller::Subscribers::send(
    const medios::controller::Event& event,
    const Option<FrameworkInfo>& frameworkInfo,
    const Option<Task>& task)
{
  VLOG(1) << "Notifying all active subscribers about " << event.type()
          << " event";

  foreachvalue (const Owned<Subscriber>& subscriber, subscribed) {
    subscriber->send(event, frameworkInfo, task);
  }
}


void Controller::Subscribers::Subscriber::send(
    const medios::controller::Event& event,
    const Option<FrameworkInfo>& frameworkInfo,
    const Option<Task>& task)
{
  switch (event.type()) {
    case medios::controller::Event::TASK_ADDED: {
      CHECK_SOME(frameworkInfo);

      if (approvers->approved<VIEW_TASK>(
              event.task_added().task(), *frameworkInfo) &&
          approvers->approved<VIEW_FRAMEWORK>(*frameworkInfo)) {
        http.send(event);
      }
      break;
    }
    case medios::controller::Event::TASK_UPDATED: {
      CHECK_SOME(frameworkInfo);
      CHECK_SOME(task);

      if (approvers->approved<VIEW_TASK>(*task, *frameworkInfo) &&
          approvers->approved<VIEW_FRAMEWORK>(*frameworkInfo)) {
        http.send(event);
      }
      break;
    }
    case medios::controller::Event::FRAMEWORK_ADDED: {
      if (approvers->approved<VIEW_FRAMEWORK>(
              event.framework_added().framework().framework_info())) {
        medios::controller::Event event_(event);
        event_.mutable_framework_added()->mutable_framework()->
            mutable_allocated_resources()->Clear();
        event_.mutable_framework_added()->mutable_framework()->
            mutable_offered_resources()->Clear();

        foreach(
            const Resource& resource,
            event.framework_added().framework().allocated_resources()) {
          if (approvers->approved<VIEW_ROLE>(resource)) {
            event_.mutable_framework_added()->mutable_framework()->
              add_allocated_resources()->CopyFrom(resource);
          }
        }

        foreach(
            const Resource& resource,
            event.framework_added().framework().offered_resources()) {
          if (approvers->approved<VIEW_ROLE>(resource)) {
            event_.mutable_framework_added()->mutable_framework()->
              add_offered_resources()->CopyFrom(resource);
          }
        }

        http.send(event_);
      }
      break;
    }
    case medios::controller::Event::FRAMEWORK_UPDATED: {
      if (approvers->approved<VIEW_FRAMEWORK>(
              event.framework_updated().framework().framework_info())) {
        medios::controller::Event event_(event);
        event_.mutable_framework_updated()->mutable_framework()->
          mutable_allocated_resources()->Clear();
        event_.mutable_framework_updated()->mutable_framework()->
          mutable_offered_resources()->Clear();

        foreach(
            const Resource& resource,
            event.framework_updated().framework().allocated_resources()) {
          if (approvers->approved<VIEW_ROLE>(resource)) {
            event_.mutable_framework_updated()->mutable_framework()->
              add_allocated_resources()->CopyFrom(resource);
          }
        }

        foreach(
            const Resource& resource,
            event.framework_updated().framework().offered_resources()) {
          if (approvers->approved<VIEW_ROLE>(resource)) {
            event_.mutable_framework_updated()->mutable_framework()->
              add_offered_resources()->CopyFrom(resource);
          }
        }

        http.send(event_);
      }
      break;
    }
    case medios::controller::Event::FRAMEWORK_REMOVED: {
      if (approvers->approved<VIEW_FRAMEWORK>(
              event.framework_removed().framework_info())) {
        http.send(event);
      }
      break;
    }
    case medios::controller::Event::NODE_ADDED: {
      medios::controller::Event event_(event);
      event_.mutable_node_added()->mutable_node()->
        mutable_total_resources()->Clear();

      foreach(
          const Resource& resource,
          event.node_added().node().total_resources()) {
        if (approvers->approved<VIEW_ROLE>(resource)) {
          event_.mutable_node_added()->mutable_node()->add_total_resources()
            ->CopyFrom(resource);
        }
      }

      http.send(event_);
      break;
    }
    case medios::controller::Event::NODE_REMOVED:
    case medios::controller::Event::SUBSCRIBED:
    case medios::controller::Event::HEARTBEAT:
    case medios::controller::Event::UNKNOWN:
      http.send(event);
      break;
  }
}


void Controller::exited(const id::UUID& id)
{
  if (!subscribers.subscribed.contains(id)) {
    // NOTE: This is only possible when the controller closes an event stream
    // by deleting the subscriber from the `subscribed` map. There will
    // be separate logging when that happens.
    return;
  }

  LOG(INFO) << "Removed subscriber " << id
            << " from the list of active subscribers";

  subscribers.subscribed.erase(id);

  metrics->operator_event_stream_subscribers =
    subscribers.subscribed.size();
}


void Controller::subscribe(
    const StreamingHttpConnection<v1::controller::Event>& http,
    const Owned<ObjectApprovers>& approvers)
{
  LOG(INFO) << "Added subscriber " << http.streamId
            << " to the list of active subscribers";

  http.closed()
    .onAny(defer(self(),
           [this, http](const Future<Nothing>&) {
             exited(http.streamId);
           }));

  if (subscribers.subscribed.size() >=
      flags.max_operator_event_stream_subscribers) {
    LOG(INFO)
      << "Reached the maximum number of operator event stream subscribers ("
      << flags.max_operator_event_stream_subscribers << ") so the oldest "
      << "connection (" << std::get<0>(*subscribers.subscribed.begin())
      << ") will be closed";
  }

  subscribers.subscribed.set(
      http.streamId,
      Owned<Subscribers::Subscriber>(
          new Subscribers::Subscriber{http, approvers}));

  metrics->operator_event_stream_subscribers =
    subscribers.subscribed.size();
}

} // namespace controller {
} // namespace internal {
} // namespace medios {
