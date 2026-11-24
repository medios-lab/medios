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

#include <algorithm>
#include <map>
#include <memory>
#include <string>
#include <tuple>
#include <utility>
#include <vector>

#include <google/protobuf/descriptor.h>
#include <google/protobuf/wire_format_lite.h>

#include <google/protobuf/io/coded_stream.h>
#include <google/protobuf/io/zero_copy_stream_impl_lite.h>

#include <medios/attributes.hpp>
#include <medios/type_utils.hpp>

#include <medios/authorizer/authorizer.hpp>

#include <medios/maintenance/maintenance.hpp>

#include <medios/controller/controller.hpp>

#include <medios/v1/controller/controller.hpp>

#include <process/async.hpp>
#include <process/collect.hpp>
#include <process/defer.hpp>
#include <process/future.hpp>
#include <process/help.hpp>
#include <process/logging.hpp>

#include <process/metrics/metrics.hpp>

#include <stout/base64.hpp>
#include <stout/errorbase.hpp>
#include <stout/foreach.hpp>
#include <stout/hashmap.hpp>
#include <stout/hashset.hpp>
#include <stout/json.hpp>
#include <stout/jsonify.hpp>
#include <stout/lambda.hpp>
#include <stout/net.hpp>
#include <stout/none.hpp>
#include <stout/nothing.hpp>
#include <stout/numify.hpp>
#include <stout/option.hpp>
#include <stout/os.hpp>
#include <stout/protobuf.hpp>
#include <stout/representation.hpp>
#include <stout/result.hpp>
#include <stout/strings.hpp>
#include <stout/try.hpp>
#include <stout/unreachable.hpp>
#include <stout/utils.hpp>
#include <stout/uuid.hpp>

#include "common/authorization.hpp"
#include "common/http.hpp"
#include "common/protobuf_utils.hpp"
#include "common/resources_utils.hpp"

#include "internal/devolve.hpp"

#include "controller/authorization.hpp"
#include "controller/machine.hpp"
#include "controller/maintenance.hpp"
#include "controller/controller.hpp"
#include "controller/registry_operations.hpp"
#include "controller/validation.hpp"

#include "medios/resources.hpp"

#include "version/version.hpp"

using google::protobuf::RepeatedPtrField;

using google::protobuf::internal::WireFormatLite;

using process::AUTHENTICATION;
using process::AUTHORIZATION;
using process::Clock;
using process::DESCRIPTION;
using process::Future;
using process::HELP;
using process::Logging;
using process::Promise;
using process::TLDR;

using process::http::Accepted;
using process::http::BadRequest;
using process::http::Conflict;
using process::http::Forbidden;
using process::http::InternalServerError;
using process::http::MethodNotAllowed;
using process::http::NotFound;
using process::http::NotImplemented;
using process::http::NotAcceptable;
using process::http::OK;
using process::http::Pipe;
using process::http::ServiceUnavailable;
using process::http::TemporaryRedirect;
using process::http::UnsupportedMediaType;
using process::http::URL;

using process::http::authentication::Principal;

using std::list;
using std::map;
using std::pair;
using std::string;
using std::tuple;
using std::vector;

using medios::authorization::ActionObject;
using medios::authorization::createSubject;
using medios::authorization::DEACTIVATE_NODE;
using medios::authorization::DRAIN_NODE;
using medios::authorization::GET_MAINTENANCE_SCHEDULE;
using medios::authorization::GET_MAINTENANCE_STATUS;
using medios::authorization::MARK_NODE_GONE;
using medios::authorization::REACTIVATE_NODE;
using medios::authorization::SET_LOG_LEVEL;
using medios::authorization::START_MAINTENANCE;
using medios::authorization::STOP_MAINTENANCE;
using medios::authorization::UPDATE_MAINTENANCE_SCHEDULE;
using medios::authorization::VIEW_EXECUTOR;
using medios::authorization::VIEW_FLAGS;
using medios::authorization::VIEW_FRAMEWORK;
using medios::authorization::VIEW_ROLE;
using medios::authorization::VIEW_TASK;

namespace medios {
namespace internal {
namespace controller {

// Pull in definitions from process.
using process::http::Response;
using process::http::Request;
using process::Owned;


string Controller::Http::API_HELP()
{
  return HELP(
    TLDR(
        "Endpoint for API calls against the controller."),
    DESCRIPTION(
        "Returns 200 OK when the request was processed successfully.",
        "",
        "Returns 307 TEMPORARY_REDIRECT redirect to the leading controller when",
        "current controller is not the leader.",
        "",
        "Returns 503 SERVICE_UNAVAILABLE if the leading controller cannot be",
        "found."),
    AUTHENTICATION(true),
    AUTHORIZATION(
        "The information returned by this endpoint for certain calls",
        "might be filtered based on the user accessing it.",
        "For example a user might only see the subset of frameworks,",
        "tasks, and executors they are allowed to view.",
        "See the authorization documentation for details."));
}


Future<Response> Controller::Http::api(
    const Request& request,
    const Option<Principal>& principal) const
{
  // TODO(greggomann): Remove this check once the `Principal` type is used in
  // `ReservationInfo`, `DiskInfo`, and within the controller's `principals` map.
  // See MESOS-7202.
  if (principal.isSome() && principal->value.isNone()) {
    return Forbidden(
        "The request's authenticated principal contains claims, but no value "
        "string. The controller currently requires that principals have a value");
  }

  // TODO(vinod): Add metrics for rejected requests.

  // TODO(vinod): Add support for rate limiting.

  // When current controller is not the leader, redirect to the leading controller.
  // Note that this could happen when an operator, or some other
  // service, including a scheduler realizes this is the leading
  // controller before the controller itself realizes it, e.g., due to a
  // ZooKeeper watch delay.
  if (!controller->elected()) {
    return redirect(request);
  }

  CHECK_SOME(controller->recovered);

  if (!controller->recovered->isReady()) {
    return ServiceUnavailable("Controller has not finished recovery");
  }

  if (request.method != "POST") {
    return MethodNotAllowed({"POST"}, request.method);
  }

  v1::controller::Call v1Call;

  // TODO(anand): Content type values are case-insensitive.
  Option<string> contentType = request.headers.get("Content-Type");

  if (contentType.isNone()) {
    return BadRequest("Expecting 'Content-Type' to be present");
  }

  if (contentType.get() == APPLICATION_PROTOBUF) {
    if (!v1Call.ParseFromString(request.body)) {
      return BadRequest("Failed to parse body into Call protobuf");
    }
  } else if (contentType.get() == APPLICATION_JSON) {
    Try<JSON::Value> value = JSON::parse(request.body);

    if (value.isError()) {
      return BadRequest("Failed to parse body into JSON: " + value.error());
    }

    Try<v1::controller::Call> parse =
      ::protobuf::parse<v1::controller::Call>(value.get());

    if (parse.isError()) {
      return BadRequest("Failed to convert JSON into Call protobuf: " +
                        parse.error());
    }

    v1Call = parse.get();
  } else {
    return UnsupportedMediaType(
        string("Expecting 'Content-Type' of ") +
        APPLICATION_JSON + " or " + APPLICATION_PROTOBUF);
  }

  medios::controller::Call call = devolve(v1Call);

  Option<Error> error = validation::controller::call::validate(call);

  if (error.isSome()) {
    return BadRequest("Failed to validate controller::Call: " + error->message);
  }

  LOG(INFO) << "Processing call " << call.type();

  ContentType acceptType;
  if (request.acceptsMediaType(APPLICATION_JSON)) {
    acceptType = ContentType::JSON;
  } else if (request.acceptsMediaType(APPLICATION_PROTOBUF)) {
    acceptType = ContentType::PROTOBUF;
  } else {
    return NotAcceptable(
        string("Expecting 'Accept' to allow ") +
        "'" + APPLICATION_PROTOBUF + "' or '" + APPLICATION_JSON + "'");
  }

  switch (call.type()) {
    case medios::controller::Call::UNKNOWN:
      return NotImplemented();

    case medios::controller::Call::GET_HEALTH:
      return getHealth(call, principal, acceptType);

    case medios::controller::Call::GET_FLAGS:
      return getFlags(call, principal, acceptType);

    case medios::controller::Call::GET_VERSION:
      return getVersion(call, principal, acceptType);

    case medios::controller::Call::GET_METRICS:
      return getMetrics(call, principal, acceptType);

    case medios::controller::Call::GET_LOGGING_LEVEL:
      return getLoggingLevel(call, principal, acceptType);

    case medios::controller::Call::SET_LOGGING_LEVEL:
      return setLoggingLevel(call, principal, acceptType);

    case medios::controller::Call::LIST_FILES:
      return listFiles(call, principal, acceptType);

    case medios::controller::Call::READ_FILE:
      return readFile(call, principal, acceptType);

    case medios::controller::Call::GET_STATE:
      return getState(call, principal, acceptType);

    case medios::controller::Call::GET_NODES:
      return getNodes(call, principal, acceptType);

    case medios::controller::Call::GET_FRAMEWORKS:
      return getFrameworks(call, principal, acceptType);

    case medios::controller::Call::GET_EXECUTORS:
      return getExecutors(call, principal, acceptType);

    case medios::controller::Call::GET_OPERATIONS:
      return getOperations(call, principal, acceptType);

    case medios::controller::Call::GET_TASKS:
      return getTasks(call, principal, acceptType);

    case medios::controller::Call::GET_ROLES:
      return getRoles(call, principal, acceptType);

    case medios::controller::Call::GET_WEIGHTS:
      return weightsHandler.get(call, principal, acceptType);

    case medios::controller::Call::UPDATE_WEIGHTS:
      return weightsHandler.update(call, principal, acceptType);

    case medios::controller::Call::GET_CONTROLLER:
      return getController(call, principal, acceptType);

    case medios::controller::Call::SUBSCRIBE:
      return subscribe(call, principal, acceptType);

    case medios::controller::Call::RESERVE_RESOURCES:
      return reserveResources(call, principal, acceptType);

    case medios::controller::Call::UNRESERVE_RESOURCES:
      return unreserveResources(call, principal, acceptType);

    case medios::controller::Call::CREATE_VOLUMES:
      return createVolumes(call, principal, acceptType);

    case medios::controller::Call::DESTROY_VOLUMES:
      return destroyVolumes(call, principal, acceptType);

    case medios::controller::Call::GROW_VOLUME:
      return growVolume(call, principal, acceptType);

    case medios::controller::Call::SHRINK_VOLUME:
      return shrinkVolume(call, principal, acceptType);

    case medios::controller::Call::GET_MAINTENANCE_STATUS:
      return getMaintenanceStatus(call, principal, acceptType);

    case medios::controller::Call::GET_MAINTENANCE_SCHEDULE:
      return getMaintenanceSchedule(call, principal, acceptType);

    case medios::controller::Call::UPDATE_MAINTENANCE_SCHEDULE:
      return updateMaintenanceSchedule(call, principal, acceptType);

    case medios::controller::Call::START_MAINTENANCE:
      return startMaintenance(call, principal, acceptType);

    case medios::controller::Call::STOP_MAINTENANCE:
      return stopMaintenance(call, principal, acceptType);

    case medios::controller::Call::DRAIN_NODE:
      return drainNode(call, principal, acceptType);

    case medios::controller::Call::DEACTIVATE_NODE:
      return deactivateNode(call, principal, acceptType);

    case medios::controller::Call::REACTIVATE_NODE:
      return reactivateNode(call, principal, acceptType);

    case medios::controller::Call::GET_QUOTA:
      return quotaHandler.status(call, principal, acceptType);

    case medios::controller::Call::UPDATE_QUOTA:
      return quotaHandler.update(call, principal);

    // TODO(bmahler): Add this to a deprecated call section
    // at the bottom once deprecated by `UPDATE_QUOTA`.
    case medios::controller::Call::SET_QUOTA:
      return quotaHandler.set(call, principal);

    // TODO(bmahler): Add this to a deprecated call section
    // at the bottom once deprecated by `UPDATE_QUOTA`.
    case medios::controller::Call::REMOVE_QUOTA:
      return quotaHandler.remove(call, principal);

    case medios::controller::Call::TEARDOWN:
      return teardown(call, principal, acceptType);

    case medios::controller::Call::MARK_NODE_GONE:
      return markNodeGone(call, principal, acceptType);
  }

  UNREACHABLE();
}


Future<Response> Controller::Http::subscribe(
    const medios::controller::Call& call,
    const Option<Principal>& principal,
    ContentType outputContentType) const
{
  CHECK_EQ(medios::controller::Call::SUBSCRIBE, call.type());

  return ObjectApprovers::create(
      controller->authorizer,
      principal,
      {VIEW_FRAMEWORK, VIEW_TASK, VIEW_EXECUTOR, VIEW_ROLE})
    .then(defer(
          controller->self(),
          [this, principal, outputContentType](
              const Owned<ObjectApprovers>& approvers) {
            return deferBatchedRequest(
                &Controller::ReadOnlyHandler::subscribe,
                principal,
                outputContentType,
                {},
                approvers);
          }));
}


// TODO(ijimenez): Add some information or pointers to help
// users understand the HTTP Event/Call API.
string Controller::Http::SCHEDULER_HELP()
{
  return HELP(
    TLDR(
        "Endpoint for schedulers to make calls against the controller."),
    DESCRIPTION(
        "Returns 202 Accepted iff the request is accepted.",
        "",
        "Returns 307 TEMPORARY_REDIRECT redirect to the leading controller when",
        "current controller is not the leader.",
        "",
        "Returns 503 SERVICE_UNAVAILABLE if the leading controller cannot be",
        "found."),
    AUTHENTICATION(true),
    AUTHORIZATION(
        "The returned frameworks information might be filtered based on the",
        "users authorization.",
        "See the authorization documentation for details."));
}


Future<Response> Controller::Http::scheduler(
    const Request& request,
    const Option<Principal>& principal) const
{
  // TODO(greggomann): Remove this check once the `Principal` type is used in
  // `ReservationInfo`, `DiskInfo`, and within the controller's `principals` map.
  // See MESOS-7202.
  if (principal.isSome() && principal->value.isNone()) {
    return Forbidden(
        "The request's authenticated principal contains claims, but no value "
        "string. The controller currently requires that principals have a value");
  }

  // TODO(vinod): Add metrics for rejected requests.

  // TODO(vinod): Add support for rate limiting.

  // When current controller is not the leader, redirect to the leading controller.
  // Note that this could happen if the scheduler realizes this is the
  // leading controller before the controller itself realizes it, e.g., due to
  // a ZooKeeper watch delay.
  if (!controller->elected()) {
    return redirect(request);
  }

  CHECK_SOME(controller->recovered);

  if (!controller->recovered->isReady()) {
    return ServiceUnavailable("Controller has not finished recovery");
  }

  if (request.method != "POST") {
    return MethodNotAllowed({"POST"}, request.method);
  }

  v1::scheduler::Call v1Call;

  // TODO(anand): Content type values are case-insensitive.
  Option<string> contentType = request.headers.get("Content-Type");

  if (contentType.isNone()) {
    return BadRequest("Expecting 'Content-Type' to be present");
  }

  if (contentType.get() == APPLICATION_PROTOBUF) {
    if (!v1Call.ParseFromString(request.body)) {
      return BadRequest("Failed to parse body into Call protobuf");
    }
  } else if (contentType.get() == APPLICATION_JSON) {
    Try<JSON::Value> value = JSON::parse(request.body);

    if (value.isError()) {
      return BadRequest("Failed to parse body into JSON: " + value.error());
    }

    Try<v1::scheduler::Call> parse =
      ::protobuf::parse<v1::scheduler::Call>(value.get());

    if (parse.isError()) {
      return BadRequest("Failed to convert JSON into Call protobuf: " +
                        parse.error());
    }

    v1Call = parse.get();
  } else {
    return UnsupportedMediaType(
        string("Expecting 'Content-Type' of ") +
        APPLICATION_JSON + " or " + APPLICATION_PROTOBUF);
  }

  scheduler::Call call = devolve(v1Call);

  Option<Error> error = validation::scheduler::call::validate(call, principal);

  if (error.isSome()) {
    controller->metrics->incrementInvalidSchedulerCalls(call);
    return BadRequest("Failed to validate scheduler::Call: " + error->message);
  }

  ContentType acceptType;

  // Ideally this handler would be consistent with the Operator API handler
  // and determine the accept type regardless of the type of request.
  // However, to maintain backwards compatibility, it determines the accept
  // type only if the response will not be empty.
  if (call.type() == scheduler::Call::SUBSCRIBE ||
      call.type() == scheduler::Call::RECONCILE_OPERATIONS) {
    if (request.acceptsMediaType(APPLICATION_JSON)) {
      acceptType = ContentType::JSON;
    } else if (request.acceptsMediaType(APPLICATION_PROTOBUF)) {
      acceptType = ContentType::PROTOBUF;
    } else {
      return NotAcceptable(
          string("Expecting 'Accept' to allow ") +
          "'" + APPLICATION_PROTOBUF + "' or '" + APPLICATION_JSON + "'");
    }
  }

  if (call.type() == scheduler::Call::SUBSCRIBE) {
    // Make sure that a stream ID was not included in the request headers.
    if (request.headers.contains("Medios-Stream-Id")) {
      return BadRequest(
          "Subscribe calls should not include the 'Medios-Stream-Id' header");
    }

    const FrameworkInfo& frameworkInfo = call.subscribe().framework_info();

    // We allow an authenticated framework to not specify a principal in
    // `FrameworkInfo`, but in that case we log a WARNING here. We also set
    // `FrameworkInfo.principal` to the value of the authenticated principal
    // and use it for authorization later.
    //
    // NOTE: Common validation code, called previously, verifies that the
    // authenticated principal is the same as `FrameworkInfo.principal`,
    // if present.
    if (principal.isSome() &&
        (!frameworkInfo.has_principal() || frameworkInfo.principal().empty())) {
      CHECK_SOME(principal->value);

      LOG(WARNING)
        << "Setting 'principal' in FrameworkInfo to '" << principal->value.get()
        << "' because the framework authenticated with that principal but "
        << "did not set it in FrameworkInfo";

      call.mutable_subscribe()->mutable_framework_info()->set_principal(
          principal->value.get());
    }

    // #409: HTTP frameworks (such as `medios-sdk-go`) often omit
    // `FrameworkInfo.hostname`, leaving `hostname: ""` on `/state`.
    // Populate it from the peer client IP address when empty (avoiding
    // synchronous reverse-DNS `lookup_hostname()` on the Controller actor).
    if ((!frameworkInfo.has_hostname() || frameworkInfo.hostname().empty()) &&
        request.client.isSome()) {
      Try<process::network::inet::Address> clientAddress =
        process::network::convert<process::network::inet::Address>(
            request.client.get());
      if (clientAddress.isSome()) {
        call.mutable_subscribe()->mutable_framework_info()->set_hostname(
            stringify(clientAddress->ip));
      }
    }

    Pipe pipe;
    OK ok;
    ok.headers["Content-Type"] = stringify(acceptType);

    ok.type = Response::PIPE;
    ok.reader = pipe.reader();

    // Generate a stream ID and return it in the response.
    id::UUID streamId = id::UUID::random();
    ok.headers["Medios-Stream-Id"] = streamId.toString();

    StreamingHttpConnection<v1::scheduler::Event> http(
        pipe.writer(), acceptType, streamId);

    controller->subscribe(http, std::move(*call.mutable_subscribe()));

    return ok;
  }

  // We consolidate the framework lookup logic here because it is
  // common for all the call handlers.
  Framework* framework = controller->getFramework(call.framework_id());

  if (framework == nullptr) {
    return BadRequest("Framework cannot be found");
  }

  framework->metrics.incrementCall(call.type());

  // TODO(greggomann): Move this implicit scheduler authorization
  // into the authorizer. See MESOS-7399.
  if (principal.isSome() && principal != framework->info.principal()) {
    return BadRequest(
        "Authenticated principal '" + stringify(principal.get()) + "' does not "
        "match principal '" + framework->info.principal() + "' set in "
        "`FrameworkInfo`");
  }

  if (!framework->connected()) {
    return Forbidden("Framework is not subscribed");
  }

  if (framework->http().isNone()) {
    return Forbidden("Framework is not connected via HTTP");
  }

  // This isn't a `SUBSCRIBE` call, so the request should include a stream ID.
  if (!request.headers.contains("Medios-Stream-Id")) {
    return BadRequest(
        "All non-subscribe calls should include the 'Medios-Stream-Id' header");
  }

  const string& streamId = request.headers.at("Medios-Stream-Id");
  if (streamId != framework->http()->streamId.toString()) {
    return BadRequest(
        "The stream ID '" + streamId + "' included in this request "
        "didn't match the stream ID currently associated with framework ID "
        + framework->id().value());
  }

  switch (call.type()) {
    case scheduler::Call::SUBSCRIBE:
      // SUBSCRIBE call should have been handled above.
      LOG(FATAL) << "Unexpected 'SUBSCRIBE' call";

    case scheduler::Call::TEARDOWN:
      controller->removeFramework(framework);
      return Accepted();

    case scheduler::Call::ACCEPT:
      controller->accept(framework, std::move(*call.mutable_accept()));
      return Accepted();

    case scheduler::Call::DECLINE:
      controller->decline(framework, std::move(*call.mutable_decline()));
      return Accepted();

    case scheduler::Call::ACCEPT_INVERSE_OFFERS:
      controller->acceptInverseOffers(framework, call.accept_inverse_offers());
      return Accepted();

    case scheduler::Call::DECLINE_INVERSE_OFFERS:
      controller->declineInverseOffers(framework, call.decline_inverse_offers());
      return Accepted();

    case scheduler::Call::REVIVE:
      controller->revive(framework, call.revive());
      return Accepted();

    case scheduler::Call::SUPPRESS:
      controller->suppress(framework, call.suppress());
      return Accepted();

    case scheduler::Call::KILL:
      controller->kill(framework, call.kill());
      return Accepted();

    case scheduler::Call::SHUTDOWN:
      controller->shutdown(framework, call.shutdown());
      return Accepted();

    case scheduler::Call::ACKNOWLEDGE:
      controller->acknowledge(framework, std::move(*call.mutable_acknowledge()));
      return Accepted();

    case scheduler::Call::ACKNOWLEDGE_OPERATION_STATUS:
      controller->acknowledgeOperationStatus(
          framework, std::move(*call.mutable_acknowledge_operation_status()));
      return Accepted();

    case scheduler::Call::RECONCILE:
      controller->reconcile(framework, std::move(*call.mutable_reconcile()));
      return Accepted();

    case scheduler::Call::RECONCILE_OPERATIONS:
      controller->reconcileOperations(
          framework, std::move(*call.mutable_reconcile_operations()));
      return Accepted();

    case scheduler::Call::MESSAGE:
      controller->message(framework, std::move(*call.mutable_message()));
      return Accepted();

    case scheduler::Call::REQUEST:
      controller->request(framework, call.request());
      return Accepted();

    case scheduler::Call::UPDATE_FRAMEWORK: {
      const FrameworkInfo& updateInfo =
        call.update_framework().framework_info();

      if (principal.isSome() &&
          (!updateInfo.has_principal() || updateInfo.principal().empty())) {
        CHECK_SOME(principal->value);

        LOG(WARNING)
          << "Setting 'principal' in FrameworkInfo to '"
          << principal->value.get()
          << "' because the framework authenticated with that principal but "
          << "did not set it in FrameworkInfo";

        call.mutable_update_framework()->mutable_framework_info()
          ->set_principal(principal->value.get());
      }

      return controller->updateFramework(
          std::move(*call.mutable_update_framework()));
    }

    case scheduler::Call::UNKNOWN:
      LOG(WARNING) << "Received 'UNKNOWN' call";
      return NotImplemented();
  }

  return NotImplemented();
}


string Controller::Http::CREATE_VOLUMES_HELP()
{
  return HELP(
    TLDR(
        "Create persistent volumes on reserved resources."),
    DESCRIPTION(
        "Returns 202 ACCEPTED which indicates that the create",
        "operation has been validated successfully by the controller.",
        "",
        "Returns 307 TEMPORARY_REDIRECT redirect to the leading controller when",
        "current controller is not the leader.",
        "",
        "Returns 503 SERVICE_UNAVAILABLE if the leading controller cannot be",
        "found.",
        "",
        "The request is then forwarded asynchronously to the Medios",
        "node where the reserved resources are located.",
        "That asynchronous message may not be delivered or",
        "creating the volumes at the node might fail.",
        "",
        "Please provide \"nodeId\" and \"volumes\" values describing",
        "the volumes to be created."),
    AUTHENTICATION(true),
    AUTHORIZATION(
        "Using this endpoint to create persistent volumes requires that",
        "the current principal is authorized to create volumes for the",
        "specific role.",
        "See the authorization documentation for details."));
}


Future<Response> Controller::Http::createVolumes(
    const Request& request,
    const Option<Principal>& principal) const
{
  // TODO(greggomann): Remove this check once the `Principal` type is used in
  // `ReservationInfo`, `DiskInfo`, and within the controller's `principals` map.
  // See MESOS-7202.
  if (principal.isSome() && principal->value.isNone()) {
    return Forbidden(
        "The request's authenticated principal contains claims, but no value "
        "string. The controller currently requires that principals have a value");
  }

  // When current controller is not the leader, redirect to the leading controller.
  if (!controller->elected()) {
    return redirect(request);
  }

  if (request.method != "POST") {
    return MethodNotAllowed({"POST"}, request.method);
  }

  // Parse the query string in the request body.
  Try<hashmap<string, string>> decode =
    process::http::query::decode(request.body);

  if (decode.isError()) {
    return BadRequest("Unable to decode query string: " + decode.error());
  }

  const hashmap<string, string>& values = decode.get();

  Option<string> value;

  value = values.get("nodeId");
  if (value.isNone()) {
    return BadRequest("Missing 'nodeId' query parameter in the request body");
  }

  NodeID nodeId;
  nodeId.set_value(value.get());

  value = values.get("volumes");
  if (value.isNone()) {
    return BadRequest("Missing 'volumes' query parameter in the request body");
  }

  Try<JSON::Array> parse =
    JSON::parse<JSON::Array>(value.get());

  if (parse.isError()) {
    return BadRequest(
        "Error in parsing 'volumes' query parameter in the request body: " +
        parse.error());
  }

  RepeatedPtrField<Resource> volumes;
  foreach (const JSON::Value& value, parse->values) {
    Try<Resource> volume = ::protobuf::parse<Resource>(value);
    if (volume.isError()) {
      return BadRequest(
          "Error in parsing 'volumes' query parameter in the request body: " +
          volume.error());
    }

    volumes.Add()->CopyFrom(volume.get());
  }

  return _createVolumes(nodeId, volumes, principal);
}


Future<Response> Controller::Http::_createVolumes(
    const NodeID& nodeId,
    const RepeatedPtrField<Resource>& volumes,
    const Option<Principal>& principal) const
{
  Node* node = controller->nodes.registered.get(nodeId);
  if (node == nullptr) {
    return BadRequest("No node found with specified ID");
  }

  // Create an operation.
  Offer::Operation operation;
  operation.set_type(Offer::Operation::CREATE);
  operation.mutable_create()->mutable_volumes()->CopyFrom(volumes);

  Option<Error> error = validateAndUpgradeResources(&operation);
  if (error.isSome()) {
    return BadRequest(error->message);
  }

  error = validation::operation::validate(
      operation.create(),
      node->checkpointedResources,
      principal,
      node->capabilities);

  if (error.isSome()) {
    return BadRequest(
        "Invalid CREATE operation on node " + stringify(*node) + ": " +
        error->message);
  }

  return controller->authorize(
      principal, ActionObject::createVolume(operation.create()))
    .then(defer(controller->self(), [=](bool authorized) -> Future<Response> {
      if (!authorized) {
        return Forbidden();
      }

      return _operation(nodeId, operation);
    }));
}


Future<Response> Controller::Http::createVolumes(
    const medios::controller::Call& call,
    const Option<Principal>& principal,
    ContentType /*contentType*/) const
{
  // TODO(greggomann): Remove this check once the `Principal` type is used in
  // `ReservationInfo`, `DiskInfo`, and within the controller's `principals` map.
  // See MESOS-7202.
  if (principal.isSome() && principal->value.isNone()) {
    return Forbidden(
        "The request's authenticated principal contains claims, but no value "
        "string. The controller currently requires that principals have a value");
  }

  CHECK_EQ(medios::controller::Call::CREATE_VOLUMES, call.type());
  CHECK(call.has_create_volumes());

  const NodeID& nodeId = call.create_volumes().node_id();
  const RepeatedPtrField<Resource>& volumes = call.create_volumes().volumes();

  return _createVolumes(nodeId, volumes, principal);
}


string Controller::Http::DESTROY_VOLUMES_HELP()
{
  return HELP(
    TLDR(
        "Destroy persistent volumes."),
    DESCRIPTION(
        "Returns 202 ACCEPTED which indicates that the destroy",
        "operation has been validated successfully by the controller.",
        "",
        "Returns 307 TEMPORARY_REDIRECT redirect to the leading controller when",
        "current controller is not the leader.",
        "",
        "Returns 503 SERVICE_UNAVAILABLE if the leading controller cannot be",
        "found.",
        "",
        "The request is then forwarded asynchronously to the Medios",
        "node where the reserved resources are located.",
        "That asynchronous message may not be delivered or",
        "destroying the volumes at the node might fail.",
        "",
        "Please provide \"nodeId\" and \"volumes\" values describing",
        "the volumes to be destroyed."),
    AUTHENTICATION(true),
    AUTHORIZATION(
        "Using this endpoint to destroy persistent volumes requires that",
        "the current principal is authorized to destroy volumes created",
        "by the principal who created the volume.",
        "See the authorization documentation for details."));
}


Future<Response> Controller::Http::destroyVolumes(
    const Request& request,
    const Option<Principal>& principal) const
{
  // TODO(greggomann): Remove this check once the `Principal` type is used in
  // `ReservationInfo`, `DiskInfo`, and within the controller's `principals` map.
  // See MESOS-7202.
  if (principal.isSome() && principal->value.isNone()) {
    return Forbidden(
        "The request's authenticated principal contains claims, but no value "
        "string. The controller currently requires that principals have a value");
  }

  // When current controller is not the leader, redirect to the leading controller.
  if (!controller->elected()) {
    return redirect(request);
  }

  if (request.method != "POST") {
    return MethodNotAllowed({"POST"}, request.method);
  }

  // Parse the query string in the request body.
  Try<hashmap<string, string>> decode =
    process::http::query::decode(request.body);

  if (decode.isError()) {
    return BadRequest("Unable to decode query string: " + decode.error());
  }

  const hashmap<string, string>& values = decode.get();

  Option<string> value;

  value = values.get("nodeId");
  if (value.isNone()) {
    return BadRequest("Missing 'nodeId' query parameter in the request body");
  }

  NodeID nodeId;
  nodeId.set_value(value.get());

  value = values.get("volumes");
  if (value.isNone()) {
    return BadRequest("Missing 'volumes' query parameter in the request body");
  }

  Try<JSON::Array> parse =
    JSON::parse<JSON::Array>(value.get());

  if (parse.isError()) {
    return BadRequest(
        "Error in parsing 'volumes' query parameter in the request body: " +
        parse.error());
  }

  RepeatedPtrField<Resource> volumes;
  foreach (const JSON::Value& value, parse->values) {
    Try<Resource> volume = ::protobuf::parse<Resource>(value);
    if (volume.isError()) {
      return BadRequest(
          "Error in parsing 'volumes' query parameter in the request body: " +
          volume.error());
    }

    volumes.Add()->CopyFrom(volume.get());
  }

  return _destroyVolumes(nodeId, volumes, principal);
}


Future<Response> Controller::Http::_destroyVolumes(
    const NodeID& nodeId,
    const RepeatedPtrField<Resource>& volumes,
    const Option<Principal>& principal) const
{
  Node* node = controller->nodes.registered.get(nodeId);
  if (node == nullptr) {
    return BadRequest("No node found with specified ID");
  }

  // Create an operation.
  Offer::Operation operation;
  operation.set_type(Offer::Operation::DESTROY);
  operation.mutable_destroy()->mutable_volumes()->CopyFrom(volumes);

  Option<Error> error = validateAndUpgradeResources(&operation);
  if (error.isSome()) {
    return BadRequest(error->message);
  }

  error = validation::operation::validate(
      operation.destroy(),
      node->checkpointedResources,
      node->usedResources);

  if (error.isSome()) {
    return BadRequest("Invalid DESTROY operation: " + error->message);
  }

  return controller->authorize(
      principal, ActionObject::destroyVolume(operation.destroy()))
    .then(defer(controller->self(), [=](bool authorized) -> Future<Response> {
      if (!authorized) {
        return Forbidden();
      }

      return _operation(nodeId, operation);
    }));
}


Future<Response> Controller::Http::destroyVolumes(
    const medios::controller::Call& call,
    const Option<Principal>& principal,
    ContentType /*contentType*/) const
{
  // TODO(greggomann): Remove this check once the `Principal` type is used in
  // `ReservationInfo`, `DiskInfo`, and within the controller's `principals` map.
  // See MESOS-7202.
  if (principal.isSome() && principal->value.isNone()) {
    return Forbidden(
        "The request's authenticated principal contains claims, but no value "
        "string. The controller currently requires that principals have a value");
  }

  CHECK_EQ(medios::controller::Call::DESTROY_VOLUMES, call.type());
  CHECK(call.has_destroy_volumes());

  const NodeID& nodeId = call.destroy_volumes().node_id();
  const RepeatedPtrField<Resource>& volumes = call.destroy_volumes().volumes();

  return _destroyVolumes(nodeId, volumes, principal);
}


Future<Response> Controller::Http::growVolume(
    const medios::controller::Call& call,
    const Option<Principal>& principal,
    ContentType /*contentType*/) const
{
  // TODO(greggomann): Remove this check once the `Principal` type is used in
  // `ReservationInfo`, `DiskInfo`, and within the controller's `principals` map.
  // See MESOS-7202.
  if (principal.isSome() && principal->value.isNone()) {
    return Forbidden(
        "The request's authenticated principal contains claims, but no value "
        "string. The controller currently requires that principals have a value");
  }

  CHECK_EQ(medios::controller::Call::GROW_VOLUME, call.type());
  CHECK(call.has_grow_volume());

  // Only node default resources are supported right now.
  CHECK(call.grow_volume().has_node_id());

  const NodeID& nodeId = call.grow_volume().node_id();

  Node* node = controller->nodes.registered.get(nodeId);
  if (node == nullptr) {
    return BadRequest("No node found with specified ID");
  }

  // Create an operation.
  Offer::Operation operation;
  operation.set_type(Offer::Operation::GROW_VOLUME);

  operation.mutable_grow_volume()->mutable_volume()->CopyFrom(
      call.grow_volume().volume());

  operation.mutable_grow_volume()->mutable_addition()->CopyFrom(
      call.grow_volume().addition());

  Option<Error> error = validateAndUpgradeResources(&operation);
  if (error.isSome()) {
    return BadRequest(error->message);
  }

  error = validation::operation::validate(
      operation.grow_volume(), node->capabilities);

  if (error.isSome()) {
    return BadRequest(
        "Invalid GROW_VOLUME operation on node " +
        stringify(*node) + ": " + error->message);
  }

  return controller->authorize(
      principal, ActionObject::growVolume(operation.grow_volume()))
    .then(defer(controller->self(), [=](bool authorized) -> Future<Response> {
      if (!authorized) {
        return Forbidden();
      }

      return _operation(nodeId, operation);
    }));
}


Future<Response> Controller::Http::shrinkVolume(
    const medios::controller::Call& call,
    const Option<Principal>& principal,
    ContentType /*contentType*/) const
{
  // TODO(greggomann): Remove this check once the `Principal` type is used in
  // `ReservationInfo`, `DiskInfo`, and within the controller's `principals` map.
  // See MESOS-7202.
  if (principal.isSome() && principal->value.isNone()) {
    return Forbidden(
        "The request's authenticated principal contains claims, but no value "
        "string. The controller currently requires that principals have a value");
  }

  CHECK_EQ(medios::controller::Call::SHRINK_VOLUME, call.type());
  CHECK(call.has_shrink_volume());

  // Only persistent volumes are supported right now.
  CHECK(call.shrink_volume().has_node_id());

  const NodeID& nodeId = call.shrink_volume().node_id();

  Node* node = controller->nodes.registered.get(nodeId);
  if (node == nullptr) {
    return BadRequest("No node found with specified ID");
  }

  // Create an operation.
  Offer::Operation operation;
  operation.set_type(Offer::Operation::SHRINK_VOLUME);

  operation.mutable_shrink_volume()->mutable_volume()->CopyFrom(
      call.shrink_volume().volume());

  operation.mutable_shrink_volume()->mutable_subtract()->CopyFrom(
      call.shrink_volume().subtract());

  Option<Error> error = validateAndUpgradeResources(&operation);
  if (error.isSome()) {
    return BadRequest(error->message);
  }

  error = validation::operation::validate(
      operation.shrink_volume(), node->capabilities);

  if (error.isSome()) {
    return BadRequest(
        "Invalid SHRINK_VOLUME operation on node " +
        stringify(*node) + ": " + error->message);
  }

  return controller->authorize(
      principal, ActionObject::shrinkVolume(operation.shrink_volume()))
    .then(defer(controller->self(), [=](bool authorized) -> Future<Response> {
      if (!authorized) {
        return Forbidden();
      }

      return _operation(nodeId, operation);
    }));
}


string Controller::Http::FRAMEWORKS_HELP()
{
  return HELP(
    TLDR("Exposes the frameworks info."),
    DESCRIPTION(
        "Returns 200 OK when the frameworks info was queried successfully.",
        "",
        "Returns 307 TEMPORARY_REDIRECT redirect to the leading controller when",
        "current controller is not the leader.",
        "",
        "Returns 503 SERVICE_UNAVAILABLE if the leading controller cannot be",
        "found.",
        "",
        "Query parameters:",
        ">        framework_id=VALUE   The ID of the framework returned "
        "(if no framework ID is specified, all frameworks will be returned)."),
    AUTHENTICATION(true),
    AUTHORIZATION(
        "This endpoint might be filtered based on the user accessing it.",
        "See the authorization documentation for details."));
}


Future<Response> Controller::Http::frameworks(
    const Request& request,
    const Option<Principal>& principal) const
{
  // TODO(greggomann): Remove this check once the `Principal` type is used in
  // `ReservationInfo`, `DiskInfo`, and within the controller's `principals` map.
  // See MESOS-7202.
  if (principal.isSome() && principal->value.isNone()) {
    return Forbidden(
        "The request's authenticated principal contains claims, but no value "
        "string. The controller currently requires that principals have a value");
  }

  // When current controller is not the leader, redirect to the leading controller.
  if (!controller->elected()) {
    return redirect(request);
  }

  return ObjectApprovers::create(
      controller->authorizer,
      principal,
      {VIEW_FRAMEWORK, VIEW_TASK, VIEW_EXECUTOR})
    .then(defer(
        controller->self(),
        [this, request, principal](const Owned<ObjectApprovers>& approvers) {
          return deferBatchedRequest(
              &Controller::ReadOnlyHandler::frameworks,
              principal,
              ContentType::JSON,
              request.url.query,
              approvers);
        }));
}


medios::controller::Response::GetFrameworks::Framework model(
    const Framework& framework)
{
  medios::controller::Response::GetFrameworks::Framework _framework;

  _framework.mutable_framework_info()->CopyFrom(framework.info);

  _framework.set_active(framework.active());
  _framework.set_connected(framework.connected());
  _framework.set_recovered(framework.recovered());

  int64_t time = framework.registeredTime.duration().ns();
  if (time != 0) {
    _framework.mutable_registered_time()->set_nanoseconds(time);
  }

  time = framework.unregisteredTime.duration().ns();
  if (time != 0) {
    _framework.mutable_unregistered_time()->set_nanoseconds(time);
  }

  time = framework.reregisteredTime.duration().ns();
  if (time != 0) {
    _framework.mutable_reregistered_time()->set_nanoseconds(time);
  }

  foreach (const Offer* offer, framework.offers) {
    _framework.mutable_offers()->Add()->CopyFrom(*offer);
  }

  foreach (const InverseOffer* offer, framework.inverseOffers) {
    _framework.mutable_inverse_offers()->Add()->CopyFrom(*offer);
  }

  foreach (Resource resource, framework.totalUsedResources) {
    convertResourceFormat(&resource, ENDPOINT);

    _framework.mutable_allocated_resources()->Add()->CopyFrom(resource);
  }

  foreach (Resource resource, framework.totalOfferedResources) {
    convertResourceFormat(&resource, ENDPOINT);

    _framework.mutable_offered_resources()->Add()->CopyFrom(resource);
  }

  *_framework.mutable_offer_constraints() = framework.offerConstraints();

  return _framework;
}


Future<Response> Controller::Http::getFrameworks(
    const medios::controller::Call& call,
    const Option<Principal>& principal,
    ContentType outputContentType) const
{
  CHECK_EQ(medios::controller::Call::GET_FRAMEWORKS, call.type());

  return ObjectApprovers::create(
      controller->authorizer,
      principal,
      {VIEW_FRAMEWORK})
    .then(defer(
          controller->self(),
          [this, principal, outputContentType](
              const Owned<ObjectApprovers>& approvers) {
            return deferBatchedRequest(
                &Controller::ReadOnlyHandler::getFrameworks,
                principal,
                outputContentType,
                {},
                approvers);
          }));
}


Future<Response> Controller::Http::getExecutors(
    const medios::controller::Call& call,
    const Option<Principal>& principal,
    ContentType outputContentType) const
{
  CHECK_EQ(medios::controller::Call::GET_EXECUTORS, call.type());

  return ObjectApprovers::create(
      controller->authorizer,
      principal,
      {VIEW_FRAMEWORK, VIEW_EXECUTOR})
    .then(defer(
          controller->self(),
          [this, principal, outputContentType](
             const Owned<ObjectApprovers>& approvers) {
            return deferBatchedRequest(
                &Controller::ReadOnlyHandler::getExecutors,
                principal,
                outputContentType,
                {},
                approvers);
          }));
}


Future<Response> Controller::Http::getState(
    const medios::controller::Call& call,
    const Option<Principal>& principal,
    ContentType outputContentType) const
{
  CHECK_EQ(medios::controller::Call::GET_STATE, call.type());

  return ObjectApprovers::create(
      controller->authorizer,
      principal,
      {VIEW_FRAMEWORK, VIEW_TASK, VIEW_EXECUTOR, VIEW_ROLE})
    .then(defer(
          controller->self(),
          [this, principal, outputContentType](
              const Owned<ObjectApprovers>& approvers) {
            return deferBatchedRequest(
                &Controller::ReadOnlyHandler::getState,
                principal,
                outputContentType,
                {},
                approvers);
          }));
}


class Controller::Http::FlagsError : public Error
{
public:
  enum Type
  {
    UNAUTHORIZED
  };

  // TODO(arojas): Provide a proper string representation of the enum.
  explicit FlagsError(Type _type)
    : Error(stringify(_type)), type(_type) {}

  FlagsError(Type _type, const string& _message)
    : Error(stringify(_type)), type(_type), message(_message) {}

  const Type type;
  const string message;
};


string Controller::Http::FLAGS_HELP()
{
  return HELP(
    TLDR("Exposes the controller's flag configuration."),
    None(),
    AUTHENTICATION(true),
    AUTHORIZATION(
        "Querying this endpoint requires that the current principal",
        "is authorized to view all flags.",
        "See the authorization documentation for details."));
}


Future<Response> Controller::Http::flags(
    const Request& request,
    const Option<Principal>& principal) const
{
  // TODO(greggomann): Remove this check once the `Principal` type is used in
  // `ReservationInfo`, `DiskInfo`, and within the controller's `principals` map.
  // See MESOS-7202.
  if (principal.isSome() && principal->value.isNone()) {
    return Forbidden(
        "The request's authenticated principal contains claims, but no value "
        "string. The controller currently requires that principals have a value");
  }

  // TODO(nfnt): Remove check for enabled
  // authorization as part of MESOS-5346.
  if (request.method != "GET" && controller->authorizer.isSome()) {
    return MethodNotAllowed({"GET"}, request.method);
  }

  Option<string> jsonp = request.url.query.get("jsonp");

  return _flags(principal)
    .then([jsonp](const Try<JSON::Object, FlagsError>& flags)
          -> Future<Response> {
      if (flags.isError()) {
        switch (flags.error().type) {
          case FlagsError::Type::UNAUTHORIZED:
            return Forbidden();
        }

        return InternalServerError(flags.error().message);
      }

      return OK(flags.get(), jsonp);
    });
}


Future<Try<JSON::Object, Controller::Http::FlagsError>> Controller::Http::_flags(
    const Option<Principal>& principal) const
{
  if (controller->authorizer.isNone()) {
    return __flags();
  }

  authorization::Request authRequest;
  authRequest.set_action(authorization::VIEW_FLAGS);

  Option<authorization::Subject> subject = createSubject(principal);
  if (subject.isSome()) {
    authRequest.mutable_subject()->CopyFrom(subject.get());
  }

  return controller->authorizer.get()->authorized(authRequest)
    .then(defer(
        controller->self(),
        [this](bool authorized) -> Future<Try<JSON::Object, FlagsError>> {
      if (authorized) {
        return __flags();
      } else {
        return FlagsError(FlagsError::Type::UNAUTHORIZED);
      }
    }));
}


JSON::Object Controller::Http::__flags() const
{
  JSON::Object object;

  {
    JSON::Object flags;
    foreachvalue (const flags::Flag& flag, controller->flags) {
      Option<string> value = flag.stringify(controller->flags);
      if (value.isSome()) {
        const string& name = flag.effective_name().value;
        flags.values[name] = redactFlagValue(name, value.get());
      }
    }
    object.values["flags"] = std::move(flags);
  }

  return object;
}


Future<Response> Controller::Http::getFlags(
    const medios::controller::Call& call,
    const Option<Principal>& principal,
    ContentType contentType) const
{
  CHECK_EQ(medios::controller::Call::GET_FLAGS, call.type());

  return _flags(principal)
    .then([contentType](const Try<JSON::Object, FlagsError>& flags)
          -> Future<Response> {
      if (flags.isError()) {
        switch (flags.error().type) {
          case FlagsError::Type::UNAUTHORIZED:
            return Forbidden();
        }

        return InternalServerError(flags.error().message);
      }

      return OK(
          serialize(contentType,
                    evolve<v1::controller::Response::GET_FLAGS>(flags.get())),
          stringify(contentType));
    });
}


string Controller::Http::OPENAPI_HELP()
{
  return HELP(
    TLDR(
        "OpenAPI 3.x specification for this daemon."),
    DESCRIPTION(
        "Returns the OpenAPI 3.x JSON specification describing every",
        "HTTP endpoint this daemon exposes. Also served at",
        "/.well-known/openapi.json per RFC 8615."),
    AUTHENTICATION(false));
}


string Controller::Http::HEALTH_HELP()
{
  return HELP(
    TLDR(
        "Health status of the Controller."),
    DESCRIPTION(
        "Returns 200 OK iff the Controller is healthy.",
        "Delayed responses are also indicative of poor health."),
    AUTHENTICATION(false));
}


Future<Response> Controller::Http::health(const Request& request) const
{
  return OK();
}


Future<Response> Controller::Http::getHealth(
    const medios::controller::Call& call,
    const Option<Principal>& principal,
    ContentType contentType) const
{
  CHECK_EQ(medios::controller::Call::GET_HEALTH, call.type());

  medios::controller::Response response;
  response.set_type(medios::controller::Response::GET_HEALTH);
  response.mutable_get_health()->set_healthy(true);

  return OK(serialize(contentType, evolve(response)),
            stringify(contentType));
}


Future<Response> Controller::Http::getVersion(
    const medios::controller::Call& call,
    const Option<Principal>& principal,
    ContentType contentType) const
{
  CHECK_EQ(medios::controller::Call::GET_VERSION, call.type());

  return OK(serialize(contentType,
                      evolve<v1::controller::Response::GET_VERSION>(version())),
            stringify(contentType));
}


Future<Response> Controller::Http::getMetrics(
    const medios::controller::Call& call,
    const Option<Principal>& principal,
    ContentType contentType) const
{
  CHECK_EQ(medios::controller::Call::GET_METRICS, call.type());
  CHECK(call.has_get_metrics());

  Option<Duration> timeout;
  if (call.get_metrics().has_timeout()) {
    timeout = Nanoseconds(call.get_metrics().timeout().nanoseconds());
  }

  return process::metrics::snapshot(timeout)
    .then([contentType](const map<string, double>& metrics) -> Response {
      // Serialize the following message:
      //
      //   medios::controller::Response response;
      //   response.set_type(medios::controller::Response::GET_METRICS);
      //   medios::controller::Response::GetMetrics* getMetrics = ...;

      switch (contentType) {
        case ContentType::PROTOBUF: {
          string output;
          google::protobuf::io::StringOutputStream stream(&output);
          google::protobuf::io::CodedOutputStream writer(&stream);

          WireFormatLite::WriteEnum(
              medios::v1::controller::Response::kTypeFieldNumber,
              medios::v1::controller::Response::GET_METRICS,
              &writer);

          WireFormatLite::WriteBytes(
              medios::v1::controller::Response::kGetMetricsFieldNumber,
              serializeGetMetrics<medios::v1::controller::Response::GetMetrics>(
                  metrics),
              &writer);

          // We must manually trim the unused buffer space since
          // we use the string before the coded output stream is
          // destructed.
          writer.Trim();

          return OK(std::move(output), stringify(contentType));
        }

        case ContentType::JSON: {
          string body = jsonify([&](JSON::ObjectWriter* writer) {
            const google::protobuf::Descriptor* descriptor =
              v1::controller::Response::descriptor();

            int field;

            field = v1::controller::Response::kTypeFieldNumber;
            writer->field(
                descriptor->FindFieldByNumber(field)->name(),
                v1::controller::Response::Type_Name(
                    v1::controller::Response::GET_METRICS));

            field = v1::controller::Response::kGetMetricsFieldNumber;
            writer->field(
                descriptor->FindFieldByNumber(field)->name(),
                jsonifyGetMetrics<medios::v1::controller::Response::GetMetrics>(
                    metrics));
          });

          // TODO(bmahler): Pass jsonp query parameter through here.
          return OK(std::move(body), stringify(contentType));
        }

        default:
          return NotAcceptable("Request must accept json or protobuf");
      }
    });
}


Future<Response> Controller::Http::getLoggingLevel(
    const medios::controller::Call& call,
    const Option<Principal>& principal,
    ContentType contentType) const
{
  CHECK_EQ(medios::controller::Call::GET_LOGGING_LEVEL, call.type());

  medios::controller::Response response;
  response.set_type(medios::controller::Response::GET_LOGGING_LEVEL);
  response.mutable_get_logging_level()->set_level(FLAGS_v);

  return OK(serialize(contentType, evolve(response)),
            stringify(contentType));
}


Future<Response> Controller::Http::setLoggingLevel(
    const medios::controller::Call& call,
    const Option<Principal>& principal,
    ContentType /*contentType*/) const
{
  CHECK_EQ(medios::controller::Call::SET_LOGGING_LEVEL, call.type());
  CHECK(call.has_set_logging_level());

  uint32_t level = call.set_logging_level().level();
  Duration duration =
    Nanoseconds(call.set_logging_level().duration().nanoseconds());

  return ObjectApprovers::create(controller->authorizer, principal, {SET_LOG_LEVEL})
    .then([level, duration](const Owned<ObjectApprovers>& approvers)
        -> Future<Response> {
       if (!approvers->approved<SET_LOG_LEVEL>()) {
        return Forbidden();
      }

      return dispatch(process::logging(), &Logging::set_level, level, duration)
        .then([]() -> Response {
          return OK();
        });
    });
}


Future<Response> Controller::Http::getController(
    const medios::controller::Call& call,
    const Option<Principal>& principal,
    ContentType contentType) const
{
  CHECK_EQ(medios::controller::Call::GET_CONTROLLER, call.type());

  medios::controller::Response response;
  response.set_type(medios::controller::Response::GET_CONTROLLER);

  // It is guaranteed that this controller has been elected as the leader.
  CHECK(controller->elected());

  medios::controller::Response::GetController* getController = response.mutable_get_controller();

  getController->mutable_controller_info()->CopyFrom(controller->info());

  getController->set_start_time(controller->startTime.secs());
  if (controller->electedTime.isSome()) {
    getController->set_elected_time(controller->electedTime->secs());
  }

  return OK(serialize(contentType, evolve(response)),
            stringify(contentType));
}


string Controller::Http::REDIRECT_HELP()
{
  return HELP(
    TLDR(
        "Redirects to the leading Controller."),
    DESCRIPTION(
        "Returns 307 TEMPORARY_REDIRECT redirect to the leading controller when",
        "current controller is not the leader.",
        "",
        "Returns 503 SERVICE_UNAVAILABLE if the leading controller cannot be",
        "found.",
        "",
        "**NOTES:**",
        "1. This is broken currently \"on the cloud\" (e.g., EC2) as "
        "this will attempt to redirect to the private IP address, unless "
        "`advertise_ip` points to an externally accessible IP"),
    AUTHENTICATION(false));
}


Future<Response> Controller::Http::redirect(const Request& request) const
{
  // If there's no leader, return `ServiceUnavailable`.
  if (controller->leader.isNone()) {
    LOG(WARNING) << "Current controller is not elected as leader, and leader "
                 << "information is unavailable. Failed to redirect the "
                 << "request url: " << request.url;
    return ServiceUnavailable("No leader elected");
  }

  ControllerInfo info = controller->leader.get();

  // NOTE: Currently, 'info.ip()' stores ip in network order, which
  // should be fixed. See MESOS-1201 for details.
  Try<string> hostname = info.has_hostname()
    ? info.hostname()
    : net::getHostname(net::IP(ntohl(info.ip())));

  if (hostname.isError()) {
    return InternalServerError(hostname.error());
  }

  LOG(INFO) << "Redirecting request for " << request.url
            << " to the leading controller " << hostname.get();

  // NOTE: We can use a protocol-relative URL here in order to allow
  // the browser (or other HTTP client) to prefix with 'http:' or
  // 'https:' depending on the original request. See
  // https://tools.ietf.org/html/rfc7231#section-7.1.2 as well as
  // http://stackoverflow.com/questions/12436669/using-protocol-relative-uris-within-location-headers
  // which discusses this.
  string basePath = "//" + hostname.get() + ":" + stringify(info.port());

  string redirectPath = "/redirect";
  string controllerRedirectPath = "/" + controller->self().id + "/redirect";

  if (request.url.path == redirectPath ||
      request.url.path == controllerRedirectPath) {
    // When request url is '/redirect' or '/controller/redirect', redirect to the
    // base url of leading controller to avoid infinite redirect loop.
    return TemporaryRedirect(basePath);
  } else if (strings::startsWith(request.url.path, redirectPath + "/") ||
             strings::startsWith(request.url.path, controllerRedirectPath + "/")) {
    // Prevent redirection loop.
    return NotFound();
  } else {
    // `request.url` is not absolute so we can safely append it to
    // `basePath`. See https://tools.ietf.org/html/rfc2616#section-5.1.2
    // for details.
    CHECK(!request.url.isAbsolute());
    return TemporaryRedirect(basePath + stringify(request.url));
  }
}


string Controller::Http::RESERVE_HELP()
{
  return HELP(
    TLDR(
        "Reserve resources dynamically on a specific node."),
    DESCRIPTION(
        "Returns 202 ACCEPTED which indicates that the reserve",
        "operation has been validated successfully by the controller.",
        "",
        "Returns 307 TEMPORARY_REDIRECT redirect to the leading controller when",
        "current controller is not the leader.",
        "",
        "Returns 503 SERVICE_UNAVAILABLE if the leading controller cannot be",
        "found.",
        "",
        "The request is then forwarded asynchronously to the Medios",
        "node where the reserved resources are located.",
        "That asynchronous message may not be delivered or",
        "reserving resources at the node might fail.",
        "",
        "Please provide \"nodeId\" and \"resources\" values describing",
        "the resources to be reserved."),
    AUTHENTICATION(true),
    AUTHORIZATION(
        "Using this endpoint to reserve resources requires that the",
        "current principal is authorized to reserve resources for the",
        "specific role.",
        "See the authorization documentation for details."));
}


Future<Response> Controller::Http::reserve(
    const Request& request,
    const Option<Principal>& principal) const
{
  // TODO(greggomann): Remove this check once the `Principal` type is used in
  // `ReservationInfo`, `DiskInfo`, and within the controller's `principals` map.
  // See MESOS-7202.
  if (principal.isSome() && principal->value.isNone()) {
    return Forbidden(
        "The request's authenticated principal contains claims, but no value "
        "string. The controller currently requires that principals have a value");
  }

  // When current controller is not the leader, redirect to the leading controller.
  if (!controller->elected()) {
    return redirect(request);
  }

  if (request.method != "POST") {
    return MethodNotAllowed({"POST"}, request.method);
  }

  // Parse the query string in the request body.
  Try<hashmap<string, string>> decode =
    process::http::query::decode(request.body);

  if (decode.isError()) {
    return BadRequest("Unable to decode query string: " + decode.error());
  }

  // Helper function to parse a list of resource from query parameters.
  auto parseResourcesList = [](
      const std::string& key,
      const hashmap<string, string>& queryParameters)
    -> Try<RepeatedPtrField<Resource>>
    {
      Option<string> value = queryParameters.get(key);
      if (value.isNone()) {
        return Error(
          "Missing '" + key + "' query parameter in the request body");
      }

      Try<JSON::Array> parse =
        JSON::parse<JSON::Array>(value.get());

      if (parse.isError()) {
        return Error(
            "Error parsing '" + key +
            "' query parameter in the request body: " + parse.error());
      }

      RepeatedPtrField<Resource> resources;
      foreach (const JSON::Value& value, parse->values) {
        Try<Resource> resource = ::protobuf::parse<Resource>(value);
        if (resource.isError()) {
          return Error(
              "Error parsing '" + key +
              "' query parameter in the request body: " + resource.error());
        }

        resources.Add()->CopyFrom(resource.get());
      }

      return resources;
    };

  const hashmap<string, string>& values = decode.get();

  Option<string> value = values.get("nodeId");
  if (value.isNone()) {
    return BadRequest("Missing 'nodeId' query parameter in the request body");
  }

  NodeID nodeId;
  nodeId.set_value(value.get());

  Try<RepeatedPtrField<Resource>> resources =
    parseResourcesList("resources", values);

  if (resources.isError()) {
    return BadRequest(resources.error());
  }

  RepeatedPtrField<Resource> source;
  if (values.contains("source")) {
    Try<RepeatedPtrField<Resource>> parsedSource =
      parseResourcesList("source", values);

    if (parsedSource.isError()) {
      return BadRequest(parsedSource.error());
    }

    source = parsedSource.get();
  }

  return _reserve(nodeId, source, resources.get(), principal);
}


Future<Response> Controller::Http::_reserve(
    const NodeID& nodeId,
    const RepeatedPtrField<Resource>& source,
    const RepeatedPtrField<Resource>& resources,
    const Option<Principal>& principal) const
{
  Node* node = controller->nodes.registered.get(nodeId);
  if (node == nullptr) {
    return BadRequest("No node found with specified ID");
  }

  // Create an operation.
  Offer::Operation operation;
  operation.set_type(Offer::Operation::RESERVE);
  operation.mutable_reserve()->mutable_source()->CopyFrom(source);
  operation.mutable_reserve()->mutable_resources()->CopyFrom(resources);

  Option<Error> error = validateAndUpgradeResources(&operation);
  if (error.isSome()) {
    return BadRequest(error->message);
  }

  error = validation::operation::validate(
      operation.reserve(), principal, node->capabilities);

  if (error.isSome()) {
    return BadRequest(
        "Invalid RESERVE operation on node " + stringify(*node) + ": " +
        error->message);
  }

  return controller->authorize(
      principal, ActionObject::reserve(operation.reserve()))
    .then(defer(controller->self(), [=](bool authorized) -> Future<Response> {
      if (!authorized) {
        return Forbidden();
      }

      return _operation(nodeId, operation);
    }));
}


Future<Response> Controller::Http::reserveResources(
    const medios::controller::Call& call,
    const Option<Principal>& principal,
    ContentType contentType) const
{
  CHECK_EQ(medios::controller::Call::RESERVE_RESOURCES, call.type());

  const NodeID& nodeId = call.reserve_resources().node_id();
  const RepeatedPtrField<Resource>& source = call.reserve_resources().source();
  const RepeatedPtrField<Resource>& resources =
    call.reserve_resources().resources();

  return _reserve(nodeId, source, resources, principal);
}


string Controller::Http::NODES_HELP()
{
  return HELP(
    TLDR(
        "Information about nodes."),
    DESCRIPTION(
        "Returns 200 OK when the request was processed successfully.",
        "",
        "Returns 307 TEMPORARY_REDIRECT redirect to the leading controller when",
        "current controller is not the leader.",
        "",
        "Returns 503 SERVICE_UNAVAILABLE if the leading controller cannot be",
        "found.",
        "",
        "This endpoint shows information about the nodes which are registered",
        "in this controller or recovered from registry, formatted as a JSON",
        "object.",
        "",
        "Query parameters:",
        ">        node_id=VALUE       The ID of the node returned "
        "(when no node_id is specified, all nodes will be returned)."),
    AUTHENTICATION(true));
}


Future<Response> Controller::Http::nodes(
    const Request& request,
    const Option<Principal>& principal) const
{
  // When current controller is not the leader, redirect to the leading controller.
  if (!controller->elected()) {
    return redirect(request);
  }

  return ObjectApprovers::create(controller->authorizer, principal, {VIEW_ROLE})
    .then(defer(
        controller->self(),
        [this, request, principal](const Owned<ObjectApprovers>& approvers) {
          return deferBatchedRequest(
              &Controller::ReadOnlyHandler::nodes,
              principal,
              ContentType::JSON,
              request.url.query,
              approvers);
        }));
}


Future<Response> Controller::Http::getNodes(
    const medios::controller::Call& call,
    const Option<Principal>& principal,
    ContentType outputContentType) const
{
  CHECK_EQ(medios::controller::Call::GET_NODES, call.type());

  return ObjectApprovers::create(controller->authorizer, principal, {VIEW_ROLE})
    .then(defer(
          controller->self(),
          [this, principal, outputContentType](
              const Owned<ObjectApprovers>& approvers) {
            return deferBatchedRequest(
                &Controller::ReadOnlyHandler::getNodes,
                principal,
                outputContentType,
                {},
                approvers);
          }));
}


string Controller::Http::QUOTA_HELP()
{
  return HELP(
    TLDR(
        "(Deprecated) Gets or updates quota for roles."),
    DESCRIPTION(
        "NOTE: This endpoint is deprecated in favor of using the v1 controller",
        "calls: UPDATE_QUOTA and GET_QUOTA.",
        "",
        "Returns 200 OK when the quota was queried or updated successfully.",
        "",
        "Returns 307 TEMPORARY_REDIRECT redirect to the leading controller when",
        "current controller is not the leader.",
        "",
        "Returns 503 SERVICE_UNAVAILABLE if the leading controller cannot be",
        "found.",
        "",
        "GET: Returns the currently set quotas as JSON.",
        "",
        "POST: Validates the request body as JSON",
        " and sets quota for a role.",
        "",
        "DELETE: Validates the request body as JSON",
        " and removes quota for a role."),
    AUTHENTICATION(true),
    AUTHORIZATION(
        "Using this endpoint to set a quota for a certain role requires that",
        "the current principal is authorized to set quota for the target role.",
        "Similarly, removing quota requires that the principal is authorized",
        "to remove quota created by the quota_principal.",
        "Getting quota information for a certain role requires that the",
        "current principal is authorized to get quota for the target role,",
        "otherwise the entry for the target role could be silently filtered.",
        "See the authorization documentation for details."));
}


// Deprecated in favor of v1 UPDATE_QUOTA and GET_QUOTA.
Future<Response> Controller::Http::quota(
    const Request& request,
    const Option<Principal>& principal) const
{
  // TODO(greggomann): Remove this check once the `Principal` type is used in
  // `ReservationInfo`, `DiskInfo`, and within the controller's `principals` map.
  // See MESOS-7202.
  if (principal.isSome() && principal->value.isNone()) {
    return Forbidden(
        "The request's authenticated principal contains claims, but no value "
        "string. The controller currently requires that principals have a value");
  }

  // When current controller is not the leader, redirect to the leading controller.
  if (!controller->elected()) {
    return redirect(request);
  }

  // Dispatch based on HTTP method to separate `QuotaHandler`.
  if (request.method == "GET") {
    return quotaHandler.status(request, principal);
  }

  if (request.method == "POST") {
    return quotaHandler.set(request, principal);
  }

  if (request.method == "DELETE") {
    return quotaHandler.remove(request, principal);
  }

  // TODO(joerg84): Add update logic for PUT requests
  // once Quota supports updates.

  return MethodNotAllowed({"GET", "POST", "DELETE"}, request.method);
}


string Controller::Http::WEIGHTS_HELP()
{
  return HELP(
    TLDR(
        "Updates weights for the specified roles."),
    DESCRIPTION(
        "Returns 200 OK when the weights update was successful.",
        "",
        "Returns 307 TEMPORARY_REDIRECT redirect to the leading controller when",
        "current controller is not the leader.",
        "",
        "Returns 503 SERVICE_UNAVAILABLE if the leading controller cannot be",
        "found.",
        "",
        "PUT: Validates the request body as JSON",
        "and updates the weights for the specified roles."),
    AUTHENTICATION(true),
    AUTHORIZATION(
        "Getting weight information for a role requires that the current",
        "principal is authorized to get weights for the target role,",
        "otherwise the entry for the target role could be silently filtered.",
        "See the authorization documentation for details."));
}


Future<Response> Controller::Http::weights(
    const Request& request,
    const Option<Principal>& principal) const
{
  // TODO(greggomann): Remove this check once the `Principal` type is used in
  // `ReservationInfo`, `DiskInfo`, and within the controller's `principals` map.
  // See MESOS-7202.
  if (principal.isSome() && principal->value.isNone()) {
    return Forbidden(
        "The request's authenticated principal contains claims, but no value "
        "string. The controller currently requires that principals have a value");
  }

  // When current controller is not the leader, redirect to the leading controller.
  if (!controller->elected()) {
    return redirect(request);
  }

  if (request.method == "GET") {
    return weightsHandler.get(request, principal);
  }

  // Dispatch based on HTTP method to separate `WeightsHandler`.
  if (request.method == "PUT") {
    return weightsHandler.update(request, principal);
  }

  return MethodNotAllowed({"GET", "PUT"}, request.method);
}


string Controller::Http::STATE_HELP()
{
  return HELP(
    TLDR(
        "Information about state of controller."),
    DESCRIPTION(
        "Returns 200 OK when the state of the controller was queried successfully.",
        "",
        "Returns 307 TEMPORARY_REDIRECT redirect to the leading controller when",
        "current controller is not the leader.",
        "",
        "Returns 503 SERVICE_UNAVAILABLE if the leading controller cannot be",
        "found.",
        "",
        "This endpoint shows information about the frameworks, tasks,",
        "executors, and nodes running in the cluster as a JSON object.",
        "The information shown might be filtered based on the user",
        "accessing the endpoint.",
        "",
        "Example (**Note**: this is not exhaustive):",
        "",
        "```",
        "{",
        "    \"version\" : \"0.28.0\",",
        "    \"git_sha\" : \"9d5889b5a265849886a533965f4aefefd1fbd103\",",
        "    \"git_branch\" : \"refs/heads/controller\",",
        "    \"git_tag\" : \"0.28.0\",",
        "    \"build_date\" : \"2016-02-15 10:00:28\",",
        "    \"build_time\" : 1455559228,",
        "    \"build_user\" : \"medios-user\",",
        "    \"start_time\" : 1455643643.42422,",
        "    \"elected_time\" : 1455643643.43457,",
        "    \"id\" : \"b5eac2c5-609b-4ca1-a352-61941702fc9e\",",
        "    \"pid\" : \"controller@127.0.0.1:5050\",",
        "    \"hostname\" : \"localhost\",",
        "    \"activated_nodes\" : 0,",
        "    \"deactivated_nodes\" : 0,",
        "    \"cluster\" : \"test-cluster\",",
        "    \"leader\" : \"controller@127.0.0.1:5050\",",
        "    \"log_dir\" : \"/var/log\",",
        "    \"external_log_file\" : \"medios.log\",",
        "    \"flags\" : {",
        "         \"framework_sorter\" : \"drf\",",
        "         \"authenticate\" : \"false\",",
        "         \"logbufsecs\" : \"0\",",
        "         \"initialize_driver_logging\" : \"true\",",
        "         \"work_dir\" : \"/var/lib/medios\",",
        "         \"http_authenticators\" : \"basic\",",
        "         \"authorizers\" : \"local\",",
        "         \"node_reregister_timeout\" : \"10mins\",",
        "         \"logging_level\" : \"INFO\",",
        "         \"help\" : \"false\",",
        "         \"root_submissions\" : \"true\",",
        "         \"ip\" : \"127.0.0.1\",",
        "         \"user_sorter\" : \"drf\",",
        "         \"version\" : \"false\",",
        "         \"max_node_ping_timeouts\" : \"5\",",
        "         \"node_ping_timeout\" : \"15secs\",",
        "         \"registry_store_timeout\" : \"20secs\",",
        "         \"max_completed_frameworks\" : \"50\",",
        "         \"quiet\" : \"false\",",
        "         \"allocator\" : \"HierarchicalDRF\",",
        "         \"hostname_lookup\" : \"true\",",
        "         \"authenticators\" : \"identity_assertion\",",
        "         \"max_completed_tasks_per_framework\" : \"1000\",",
        "         \"registry\" : \"in_memory\",",
        "         \"registry_strict\" : \"false\",",
        "         \"authenticate_nodes\" : \"false\",",
        "         \"registry_fetch_timeout\" : \"1mins\",",
        "         \"allocation_interval\" : \"1secs\",",
        "         \"authenticate_http\" : \"false\",",
        "         \"port\" : \"5050\",",
        "         \"recovery_node_removal_limit\" : \"100%\",",
        "         \"cluster\" : \"mycluster\",",
        "         \"leader\" : \"controller@127.0.0.1:5050\",",
        "         \"log_dir\" : \"/var/log\",",
        "         \"external_log_file\" : \"medios.log\"",
        "    },",
        "    \"nodes\" : [],",
        "    \"frameworks\" : [],",
        "    \"completed_frameworks\" : [],",
        "    \"orphan_tasks\" : [],",
        "    \"unregistered_frameworks\" : []",
        "}",
        "```"),
    AUTHENTICATION(true),
    AUTHORIZATION(
        "This endpoint might be filtered based on the user accessing it.",
        "For example a user might only see the subset of frameworks,",
        "tasks, and executors they are allowed to view.",
        "See the authorization documentation for details."));
}


Future<Response> Controller::Http::state(
    const Request& request,
    const Option<Principal>& principal) const
{
  // TODO(greggomann): Remove this check once the `Principal` type is used in
  // `ReservationInfo`, `DiskInfo`, and within the controller's `principals` map.
  // See MESOS-7202.
  if (principal.isSome() && principal->value.isNone()) {
    return Forbidden(
        "The request's authenticated principal contains claims, but no value "
        "string. The controller currently requires that principals have a value");
  }

  // When current controller is not the leader, redirect to the leading controller.
  if (!controller->elected()) {
    return redirect(request);
  }

  return ObjectApprovers::create(
      controller->authorizer,
      principal,
      {VIEW_ROLE, VIEW_FRAMEWORK, VIEW_TASK, VIEW_EXECUTOR, VIEW_FLAGS})
    .then(defer(
        controller->self(),
        [this, request, principal](const Owned<ObjectApprovers>& approvers) {
          return deferBatchedRequest(
              &Controller::ReadOnlyHandler::state,
              principal,
              ContentType::JSON,
              request.url.query,
              approvers);
        }));
}


Future<Response> Controller::Http::deferBatchedRequest(
    ReadOnlyRequestHandler handler,
    const Option<Principal>& principal,
    ContentType outputContentType,
    const hashmap<std::string, std::string>& queryParameters,
    const Owned<ObjectApprovers>& approvers) const
{
  bool scheduleBatch = batchedRequests.empty();

  auto it = std::find_if(batchedRequests.begin(), batchedRequests.end(),
      [handler, &principal, &queryParameters, &outputContentType](
          const BatchedRequest& batchedRequest) {
        // NOTE: This is not a general-purpose request comparison, but
        // specific to the batched requests which are always members of
        // `ReadOnlyHandler`, since we rely on the response only depending
        // on query parameters and the current controller state.
        //
        // TODO(asekretenko): We should consider replacing `approvers` with
        // taking the set of authorization actions and creating
        // `ObjectApprovers` inside of this method, because the code below
        // implicilty relies on the fact that `approvers` for a given handler
        // always contain `ObjectApprover`s for the same set of actions. Note
        // that together with the fact that permissions in all `ObjectApprover`s
        // for a given principal are equally up-to-date (regardless of when they
        // were created), this allows us to compare `handler` and `principal`
        // instead of comparing `approvers`.
        return handler == batchedRequest.handler &&
               principal == batchedRequest.principal &&
               outputContentType == batchedRequest.outputContentType &&
               queryParameters == batchedRequest.queryParameters;
      });

  Future<Response> future;

  // Note that we do not de-duplicate the SUBSCRIBE responses,
  // since the http server in libprocess assumes there's only
  // 1 reader of the pipe.
  if (handler == &Controller::ReadOnlyHandler::subscribe ||
      it == batchedRequests.end()) {
    // Add an element to the batched state requests.
    Promise<Response> promise;
    future = promise.future();
    batchedRequests.push_back(BatchedRequest{
        handler,
        outputContentType,
        queryParameters,
        principal,
        approvers,
        std::move(promise)});
  } else {
    // Return the existing future if we have a matching request.
    future = it->promise.future();
    ++controller->metrics->http_cache_hits;

    // NOTE: The returned response should be either of type
    // `BODY` or `PATH`, since `PIPE`-type responses cannot
    // be de-duplicated currently.
    it->promise.future()
      .onReady([](const Response& r) {
        CHECK_NE(r.type, Response::PIPE);
      });
  }

  // Schedule processing of batched requests if not yet scheduled.
  if (scheduleBatch) {
    dispatch(controller->self(), [this]() {
      processRequestsBatch();
    });
  }

  return future;
}


void Controller::Http::processRequestsBatch() const
{
  CHECK(!batchedRequests.empty())
    << "Bug in state batching logic: No requests to process";

  vector<Future<pair<Response, Option<ReadOnlyHandler::PostProcessing>>>>
    results;

  // Produce the responses in parallel.
  //
  // TODO(alexr): Consider abstracting this into `parallel_async` or
  // `foreach_parallel`, see MESOS-8587.
  //
  // TODO(alexr): Consider moving `BatchedStateRequest`'s fields into
  // `process::async` once it supports moving.
  foreach (BatchedRequest& request, batchedRequests) {
    Future<pair<Response, Option<ReadOnlyHandler::PostProcessing>>>
      f = process::async(
          [this](ReadOnlyRequestHandler handler,
                 ContentType outputContentType,
                 const hashmap<std::string, std::string>& queryParameters,
                 const process::Owned<ObjectApprovers>& approvers) {
            return (readonlyHandler.*handler)(
                outputContentType,
                queryParameters,
                approvers);
          },
          request.handler,
          request.outputContentType,
          request.queryParameters,
          request.approvers);

    request.promise.associate(
      f.then([](const pair<
          Response,
          Option<ReadOnlyHandler::PostProcessing>>& result) {
        return result.first;
      }));

    results.push_back(f);
  }

  // Block the controller actor until all workers have generated state responses.
  // It is crucial not to allow the controller actor to continue and possibly
  // modify its state while a worker is reading it.
  //
  // NOTE: There is the potential for deadlock since we are blocking 1 working
  // thread here, see MESOS-8256.
  process::await(results).await();

  batchedRequests.clear();

  // Now perform the post-processing "writes" synchronously.
  for (const auto& result : results) {
    CHECK(!result.isPending()) << result;

    // Response failed or was discarded.
    if (!result.isReady()) continue;

    // No post-processing needed.
    if (result->second.isNone()) continue;

    const ReadOnlyHandler::PostProcessing& postProcessing = *result->second;

    postProcessing.state.visit(
        [&](const ReadOnlyHandler::PostProcessing::Subscribe& s) {
          controller->subscribe(s.connection, s.approvers);
        });
  }
}


Future<Response> Controller::Http::readFile(
    const medios::controller::Call& call,
    const Option<Principal>& principal,
    ContentType contentType) const
{
  CHECK_EQ(medios::controller::Call::READ_FILE, call.type());

  const size_t offset = call.read_file().offset();
  const string& path = call.read_file().path();

  Option<size_t> length;
  if (call.read_file().has_length()) {
    length = call.read_file().length();
  }

  return controller->files->read(offset, length, path, principal)
    .then([contentType](const Try<tuple<size_t, string>, FilesError>& result)
        -> Future<Response> {
      if (result.isError()) {
        const FilesError& error = result.error();

        switch (error.type) {
          case FilesError::Type::INVALID:
            return BadRequest(error.message);

          case FilesError::Type::UNAUTHORIZED:
            return Forbidden(error.message);

          case FilesError::Type::NOT_FOUND:
            return NotFound(error.message);

          case FilesError::Type::UNKNOWN:
            return InternalServerError(error.message);
        }

        UNREACHABLE();
      }

      medios::controller::Response response;
      response.set_type(medios::controller::Response::READ_FILE);

      response.mutable_read_file()->set_size(std::get<0>(result.get()));
      response.mutable_read_file()->set_data(std::get<1>(result.get()));

      return OK(serialize(contentType, evolve(response)),
                stringify(contentType));
    });
}


string Controller::Http::STATESUMMARY_HELP()
{
  return HELP(
    TLDR(
        "Summary of nodes, tasks, and registered frameworks in cluster."),
    DESCRIPTION(
        "Returns 200 OK when a summary of the controller's state was queried",
        "successfully.",
        "",
        "Returns 307 TEMPORARY_REDIRECT redirect to the leading controller when",
        "current controller is not the leader.",
        "",
        "Returns 503 SERVICE_UNAVAILABLE if the leading controller cannot be",
        "found.",
        "",
        "This endpoint gives a summary of the nodes, tasks, and",
        "registered frameworks in the cluster as a JSON object.",
        "The information shown might be filtered based on the user",
        "accessing the endpoint."),
    AUTHENTICATION(true),
    AUTHORIZATION(
        "This endpoint might be filtered based on the user accessing it.",
        "For example a user might only see the subset of frameworks",
        "they are allowed to view.",
        "See the authorization documentation for details."));
}


Future<Response> Controller::Http::stateSummary(
    const Request& request,
    const Option<Principal>& principal) const
{
  // TODO(greggomann): Remove this check once the `Principal` type is used in
  // `ReservationInfo`, `DiskInfo`, and within the controller's `principals` map.
  // See MESOS-7202.
  if (principal.isSome() && principal->value.isNone()) {
    return Forbidden(
        "The request's authenticated principal contains claims, but no value "
        "string. The controller currently requires that principals have a value");
  }

  // When current controller is not the leader, redirect to the leading controller.
  if (!controller->elected()) {
    return redirect(request);
  }

  return ObjectApprovers::create(
      controller->authorizer,
      principal,
      {VIEW_ROLE, VIEW_FRAMEWORK})
    .then(defer(
        controller->self(),
        [this, request, principal](const Owned<ObjectApprovers>& approvers) {
          return deferBatchedRequest(
              &Controller::ReadOnlyHandler::stateSummary,
              principal,
              ContentType::JSON,
              request.url.query,
              approvers);
        }));
}


string Controller::Http::ROLES_HELP()
{
  return HELP(
    TLDR(
        "Information about roles."),
    DESCRIPTION(
        "Returns 200 OK when information about roles was queried successfully.",
        "",
        "Returns 307 TEMPORARY_REDIRECT redirect to the leading controller when",
        "current controller is not the leader.",
        "",
        "Returns 503 SERVICE_UNAVAILABLE if the leading controller cannot be",
        "found.",
        "",
        "This endpoint provides information about roles as a JSON object.",
        "It returns information about every role that is on the role",
        "allowlist (if enabled), has one or more registered frameworks,",
        "or has a non-default weight or quota. For each role, it returns",
        "the weight, total allocated resources, and registered frameworks."),
    AUTHENTICATION(true));
}


Future<Response> Controller::Http::roles(
    const Request& request,
    const Option<Principal>& principal) const
{
  // TODO(greggomann): Remove this check once the `Principal` type is used in
  // `ReservationInfo`, `DiskInfo`, and within the controller's `principals` map.
  // See MESOS-7202.
  if (principal.isSome() && principal->value.isNone()) {
    return Forbidden(
        "The request's authenticated principal contains claims, but no value "
        "string. The controller currently requires that principals have a value");
  }

  // When current controller is not the leader, redirect to the leading controller.
  if (!controller->elected()) {
    return redirect(request);
  }

  return ObjectApprovers::create(controller->authorizer, principal, {VIEW_ROLE})
    .then(defer(controller->self(),
        [this, request, principal](const Owned<ObjectApprovers>& approvers) {
            return deferBatchedRequest(
                &Controller::ReadOnlyHandler::roles,
                principal,
                ContentType::JSON,
                request.url.query,
                approvers);
          }));
}


Future<Response> Controller::Http::listFiles(
    const medios::controller::Call& call,
    const Option<Principal>& principal,
    ContentType contentType) const
{
  CHECK_EQ(medios::controller::Call::LIST_FILES, call.type());

  const string& path = call.list_files().path();

  return controller->files->browse(path, principal)
    .then([contentType](const Try<list<FileInfo>, FilesError>& result)
      -> Future<Response> {
      if (result.isError()) {
        const FilesError& error = result.error();

        switch (error.type) {
          case FilesError::Type::INVALID:
            return BadRequest(error.message);

          case FilesError::Type::UNAUTHORIZED:
            return Forbidden(error.message);

          case FilesError::Type::NOT_FOUND:
            return NotFound(error.message);

          case FilesError::Type::UNKNOWN:
            return InternalServerError(error.message);
        }

        UNREACHABLE();
      }

      medios::controller::Response response;
      response.set_type(medios::controller::Response::LIST_FILES);

      medios::controller::Response::ListFiles* listFiles =
        response.mutable_list_files();

      foreach (const FileInfo& fileInfo, result.get()) {
        listFiles->add_file_infos()->CopyFrom(fileInfo);
      }

      return OK(serialize(contentType, evolve(response)),
                stringify(contentType));
    });
}


// This duplicates the functionality offered by `roles()`. This was necessary
// as the JSON object returned by `roles()` was not specified in a formal way
// i.e. via a corresponding protobuf object and would have been very hard to
// convert back into a `Resource` object.
Future<Response> Controller::Http::getRoles(
    const medios::controller::Call& call,
    const Option<Principal>& principal,
    ContentType outputContentType) const
{
  CHECK_EQ(medios::controller::Call::GET_ROLES, call.type());

  return ObjectApprovers::create(controller->authorizer, principal, {VIEW_ROLE})
    .then(defer(
          controller->self(),
          [this, principal, outputContentType](
              const Owned<ObjectApprovers>& approvers) {
            return deferBatchedRequest(
                &Controller::ReadOnlyHandler::getRoles,
                principal,
                outputContentType,
                {},
                approvers);
          }));
}


string Controller::Http::TEARDOWN_HELP()
{
  return HELP(
    TLDR(
        "Tears down a running framework by shutting down all tasks/executors "
        "and removing the framework."),
    DESCRIPTION(
        "Returns 200 OK if the framework was torn down successfully.",
        "",
        "Returns 307 TEMPORARY_REDIRECT redirect to the leading controller when",
        "current controller is not the leader.",
        "",
        "Returns 503 SERVICE_UNAVAILABLE if the leading controller cannot be",
        "found.",
        "",
        "Please provide a \"frameworkId\" value designating the running",
        "framework to tear down."),
    AUTHENTICATION(true),
    AUTHORIZATION(
        "Using this endpoint to teardown frameworks requires that the",
        "current principal is authorized to teardown frameworks created",
        "by the principal who created the framework.",
        "See the authorization documentation for details."));
}


Future<Response> Controller::Http::teardown(
    const Request& request,
    const Option<Principal>& principal) const
{
  // TODO(greggomann): Remove this check once the `Principal` type is used in
  // `ReservationInfo`, `DiskInfo`, and within the controller's `principals` map.
  // See MESOS-7202.
  if (principal.isSome() && principal->value.isNone()) {
    return Forbidden(
        "The request's authenticated principal contains claims, but no value "
        "string. The controller currently requires that principals have a value");
  }

  // When current controller is not the leader, redirect to the leading controller.
  if (!controller->elected()) {
    return redirect(request);
  }

  if (request.method != "POST") {
    return MethodNotAllowed({"POST"}, request.method);
  }

  // Parse the query string in the request body (since this is a POST)
  // in order to determine the framework ID to shutdown.
  Try<hashmap<string, string>> decode =
    process::http::query::decode(request.body);

  if (decode.isError()) {
    return BadRequest("Unable to decode query string: " + decode.error());
  }

  const hashmap<string, string>& values = decode.get();

  Option<string> value = values.get("frameworkId");
  if (value.isNone()) {
    return BadRequest(
        "Missing 'frameworkId' query parameter in the request body");
  }

  FrameworkID id;
  id.set_value(value.get());

  return _teardown(id, principal);
}


Future<Response> Controller::Http::_teardown(
    const FrameworkID& id,
    const Option<Principal>& principal) const
{
  Framework* framework = controller->getFramework(id);

  if (framework == nullptr) {
    return BadRequest("No framework found with specified ID");
  }

  // Skip authorization if no ACLs were provided to the controller.
  if (controller->authorizer.isNone()) {
    return __teardown(id);
  }

  authorization::Request teardown;
  teardown.set_action(authorization::TEARDOWN_FRAMEWORK);

  Option<authorization::Subject> subject = createSubject(principal);
  if (subject.isSome()) {
    teardown.mutable_subject()->CopyFrom(subject.get());
  }

  if (framework->info.has_principal()) {
    teardown.mutable_object()->mutable_framework_info()->CopyFrom(
        framework->info);
    teardown.mutable_object()->set_value(framework->info.principal());
  }

  return controller->authorizer.get()->authorized(teardown)
    .then(defer(controller->self(), [=](bool authorized) -> Future<Response> {
      if (!authorized) {
        return Forbidden();
      }

      return __teardown(id);
    }));
}


Future<Response> Controller::Http::__teardown(const FrameworkID& id) const
{
  Framework* framework = controller->getFramework(id);

  if (framework == nullptr) {
    return BadRequest("No framework found with ID " + stringify(id));
  }

  // TODO(ijimenez): Do 'removeFramework' asynchronously.
  controller->removeFramework(framework);

  return OK();
}


Future<Response> Controller::Http::teardown(
    const medios::controller::Call& call,
    const Option<Principal>& principal,
    ContentType contentType) const
{
  CHECK_EQ(medios::controller::Call::TEARDOWN, call.type());

  return _teardown(call.teardown().framework_id(), principal);
}


Future<Response> Controller::Http::getOperations(
    const medios::controller::Call& call,
    const Option<Principal>& principal,
    ContentType outputContentType) const
{
  CHECK_EQ(medios::controller::Call::GET_OPERATIONS, call.type());

  return ObjectApprovers::create(controller->authorizer, principal, {VIEW_ROLE})
    .then(defer(
          controller->self(),
          [this, principal, outputContentType](
             const Owned<ObjectApprovers>& approvers) {
            return deferBatchedRequest(
                &Controller::ReadOnlyHandler::getOperations,
                principal,
                outputContentType,
                {},
                approvers);
          }));
}


string Controller::Http::TASKS_HELP()
{
  return HELP(
    TLDR(
        "Lists tasks from all active frameworks."),
    DESCRIPTION(
        "Returns 200 OK when task information was queried successfully.",
        "",
        "Returns 307 TEMPORARY_REDIRECT redirect to the leading controller when",
        "current controller is not the leader.",
        "",
        "Returns 503 SERVICE_UNAVAILABLE if the leading controller cannot be",
        "found.",
        "",
        "Lists known tasks.",
        "The information shown might be filtered based on the user",
        "accessing the endpoint.",
        "",
        "Query parameters:",
        "",
        ">        framework_id=VALUE   Only return tasks belonging to the "
        "framework with this ID.",
        ">        limit=VALUE          Maximum number of tasks returned "
        "(default is " + stringify(TASK_LIMIT) + ").",
        ">        offset=VALUE         Starts task list at offset.",
        ">        order=(asc|desc)     Ascending or descending sort order "
        "(default is descending).",
        ">        task_id=VALUE        Only return tasks with this ID "
        "(should be used together with parameter 'framework_id')."
        ""),
    AUTHENTICATION(true),
    AUTHORIZATION(
        "This endpoint might be filtered based on the user accessing it.",
        "For example a user might only see the subset of tasks they are",
        "allowed to view.",
        "See the authorization documentation for details."));
}


Future<Response> Controller::Http::tasks(
    const Request& request,
    const Option<Principal>& principal) const
{
  // TODO(greggomann): Remove this check once the `Principal` type is used in
  // `ReservationInfo`, `DiskInfo`, and within the controller's `principals` map.
  // See MESOS-7202.
  if (principal.isSome() && principal->value.isNone()) {
    return Forbidden(
        "The request's authenticated principal contains claims, but no value "
        "string. The controller currently requires that principals have a value");
  }

  // When current controller is not the leader, redirect to the leading controller.
  if (!controller->elected()) {
    return redirect(request);
  }

  return ObjectApprovers::create(
      controller->authorizer,
      principal,
      {VIEW_FRAMEWORK, VIEW_TASK})
    .then(defer(
        controller->self(),
        [this, request, principal](const Owned<ObjectApprovers>& approvers) {
          return deferBatchedRequest(
              &Controller::ReadOnlyHandler::tasks,
              principal,
              ContentType::JSON,
              request.url.query,
              approvers);
        }));
}


Future<Response> Controller::Http::getTasks(
    const medios::controller::Call& call,
    const Option<Principal>& principal,
    ContentType outputContentType) const
{
  CHECK_EQ(medios::controller::Call::GET_TASKS, call.type());

  return ObjectApprovers::create(
      controller->authorizer,
      principal,
      {VIEW_FRAMEWORK, VIEW_TASK})
    .then(defer(
          controller->self(),
          [this, principal, outputContentType](
             const Owned<ObjectApprovers>& approvers) {
            return deferBatchedRequest(
                &Controller::ReadOnlyHandler::getTasks,
                principal,
                outputContentType,
                {},
                approvers);
          }));
}


// /controller/maintenance/schedule endpoint help.
string Controller::Http::MAINTENANCE_SCHEDULE_HELP()
{
  return HELP(
    TLDR(
        "Returns or updates the cluster's maintenance schedule."),
    DESCRIPTION(
        "Returns 200 OK when the requested maintenance operation was performed",
        "successfully.",
        "",
        "Returns 307 TEMPORARY_REDIRECT redirect to the leading controller when",
        "current controller is not the leader.",
        "",
        "Returns 503 SERVICE_UNAVAILABLE if the leading controller cannot be",
        "found.",
        "",
        "GET: Returns the current maintenance schedule as JSON.",
        "",
        "POST: Validates the request body as JSON",
        "and updates the maintenance schedule."),
    AUTHENTICATION(true),
    AUTHORIZATION(
        "GET: The response will contain only the maintenance schedule for",
        "those machines the current principal is allowed to see. If none",
        "an empty response will be returned.",
        "",
        "POST: The current principal must be authorized to modify the",
        "maintenance schedule of all the machines in the request. If the",
        "principal is unauthorized to modify the schedule for at least one",
        "machine, the whole request will fail."));
}


// /controller/maintenance/schedule endpoint handler.
Future<Response> Controller::Http::maintenanceSchedule(
    const Request& request,
    const Option<Principal>& principal) const
{
  // When current controller is not the leader, redirect to the leading controller.
  if (!controller->elected()) {
    return redirect(request);
  }

  if (request.method != "GET" && request.method != "POST") {
    return MethodNotAllowed({"GET", "POST"}, request.method);
  }

  // JSON-ify and return the current maintenance schedule.
  if (request.method == "GET") {
    Option<string> jsonp = request.url.query.get("jsonp");

    return ObjectApprovers::create(
        controller->authorizer,
        principal,
        {GET_MAINTENANCE_SCHEDULE})
      .then(defer(
        controller->self(),
        [this, jsonp](const Owned<ObjectApprovers>& approvers) -> Response {
          const medios::maintenance::Schedule schedule =
            _getMaintenanceSchedule(approvers);
          return OK(JSON::protobuf(schedule), jsonp);
        }));
  }

  // Parse the POST body as JSON.
  Try<JSON::Object> jsonSchedule = JSON::parse<JSON::Object>(request.body);
  if (jsonSchedule.isError()) {
    return BadRequest(jsonSchedule.error());
  }

  // Convert the schedule to a protobuf.
  Try<medios::maintenance::Schedule> protoSchedule =
    ::protobuf::parse<medios::maintenance::Schedule>(jsonSchedule.get());

  if (protoSchedule.isError()) {
    return BadRequest(protoSchedule.error());
  }

  return _updateMaintenanceSchedule(protoSchedule.get(), principal);
}


medios::maintenance::Schedule Controller::Http::_getMaintenanceSchedule(
    const Owned<ObjectApprovers>& approvers) const
{
  // TODO(josephw): Return more than one schedule.
  if (controller->maintenance.schedules.empty()) {
    return medios::maintenance::Schedule();
  }

  medios::maintenance::Schedule schedule;

  foreach (const medios::maintenance::Window& window,
           controller->maintenance.schedules.front().windows()) {
    medios::maintenance::Window window_;

    foreach (const MachineID& machine_id, window.machine_ids()) {
      if (!approvers->approved<GET_MAINTENANCE_SCHEDULE>(machine_id)) {
        continue;
      }

      window_.add_machine_ids()->CopyFrom(machine_id);
    }

    if (window_.machine_ids_size() > 0) {
      window_.mutable_unavailability()->CopyFrom(window.unavailability());
      schedule.add_windows()->CopyFrom(window_);
    }
  }

  return schedule;
}


Future<Response> Controller::Http::_updateMaintenanceSchedule(
    const medios::maintenance::Schedule& schedule,
    const Option<process::http::authentication::Principal>& principal) const
{
  // Validate that the schedule only transitions machines between
  // `UP` and `DRAINING` modes.
  Try<Nothing> isValid = maintenance::validation::schedule(
      schedule,
      controller->machines);

  if (isValid.isError()) {
    return BadRequest(isValid.error());
  }

  // TODO(alexr): Consider pulling this higher above before we even start
  // parsing request body.
  return ObjectApprovers::create(
      controller->authorizer,
      principal,
      {UPDATE_MAINTENANCE_SCHEDULE})
    .then(defer(
      controller->self(),
      [this, schedule](const Owned<ObjectApprovers>& approvers) {
        return __updateMaintenanceSchedule(schedule, approvers);
      }));
}

Future<Response> Controller::Http::__updateMaintenanceSchedule(
    const medios::maintenance::Schedule& schedule,
    const Owned<ObjectApprovers>& approvers) const
{
  foreach (const medios::maintenance::Window& window, schedule.windows()) {
    foreach (const MachineID& machine, window.machine_ids()) {
      if (!approvers->approved<UPDATE_MAINTENANCE_SCHEDULE>(machine)) {
        return Forbidden();
      }
    }
  }

  return controller->registrar->apply(Owned<RegistryOperation>(
      new maintenance::UpdateSchedule(schedule)))
    .onAny([](const Future<bool>& result) {
      // TODO(fiu): Consider changing/refactoring the registrar itself
      // so the individual call sites don't need to handle this separately.
      // All registrar failures that cause it to abort should instead
      // abort the process.
      CHECK_READY(result)
        << "Failed to update maintenance schedule in the registry";
    })
    .then(defer(controller->self(), [this, schedule](bool result) {
      return ___updateMaintenanceSchedule(schedule, result);
    }));
}

Future<Response> Controller::Http::___updateMaintenanceSchedule(
    const medios::maintenance::Schedule& schedule,
    bool applied) const
{
  // See the top comment in "controller/maintenance.hpp" for why this check
  // is here, and is appropriate.
  CHECK(applied);

  // Update the controller's local state with the new schedule.
  //
  // NOTE: We only add or remove differences between the current schedule and
  // the new schedule.  This is because the `MachineInfo` struct holds more
  // information than a maintenance schedule. For example, the `mode` field is
  // not part of a maintenance schedule.
  //
  // TODO(josephw): allow more than one schedule.

  // Put the machines in the updated schedule into a set.
  // Save the unavailability, to help with updating some machines.
  hashmap<MachineID, Unavailability> unavailabilities;
  foreach (const medios::maintenance::Window& window, schedule.windows()) {
    foreach (const MachineID& id, window.machine_ids()) {
      unavailabilities[id] = window.unavailability();
    }
  }

  // NOTE: Copies are needed because `updateUnavailability()` in this loop
  // modifies the container.
  foreachkey (const MachineID& id, utils::copy(controller->machines)) {
    // Update the `unavailability` for each existing machine, except for
    // machines going from `UP` to `DRAINING` (handled in the next loop).
    // Each machine will only be touched by 1 of the 2 loops here to
    // avoid sending inverse offer twice for a single machine since
    // `updateUnavailability` will trigger an inverse offer.
    // TODO(gyliu513): Merge this logic with `Controller::updateUnavailability`,
    // having it in two places results in more conditionals to handle.
    if (unavailabilities.contains(id)) {
      if (controller->machines[id].info.mode() == MachineInfo::UP) {
        continue;
      }

      controller->updateUnavailability(id, unavailabilities[id]);
      continue;
    }

    // Transition each removed machine back to the `UP` mode and remove the
    // unavailability.
    controller->machines[id].info.set_mode(MachineInfo::UP);
    controller->updateUnavailability(id, None());
  }

  // Save each new machine, with the unavailability
  // and starting in `DRAINING` mode.
  foreach (const medios::maintenance::Window& window, schedule.windows()) {
    foreach (const MachineID& id, window.machine_ids()) {
      if (controller->machines.contains(id) &&
          controller->machines[id].info.mode() != MachineInfo::UP) {
        continue;
      }

      MachineInfo info;
      info.mutable_id()->CopyFrom(id);
      info.set_mode(MachineInfo::DRAINING);

      controller->machines[id].info.CopyFrom(info);

      controller->updateUnavailability(id, window.unavailability());
    }
  }

  // Replace the old schedule(s) with the new schedule.
  controller->maintenance.schedules.clear();
  controller->maintenance.schedules.push_back(schedule);

  return OK();
}


Future<Response> Controller::Http::getMaintenanceSchedule(
    const medios::controller::Call& call,
    const Option<Principal>& principal,
    ContentType contentType) const
{
  CHECK_EQ(medios::controller::Call::GET_MAINTENANCE_SCHEDULE, call.type());

  return ObjectApprovers::create(
      controller->authorizer,
      principal,
      {GET_MAINTENANCE_SCHEDULE})
    .then(defer(
      controller->self(),
      [this, contentType](const Owned<ObjectApprovers>& approvers) -> Response {
        medios::controller::Response response;

        response.set_type(medios::controller::Response::GET_MAINTENANCE_SCHEDULE);

        *response.mutable_get_maintenance_schedule()->mutable_schedule() =
          _getMaintenanceSchedule(approvers);

        return OK(serialize(contentType, evolve(response)),
                  stringify(contentType));
      }));
}


Future<Response> Controller::Http::updateMaintenanceSchedule(
    const medios::controller::Call& call,
    const Option<Principal>& principal,
    ContentType /*contentType*/) const
{
  CHECK_EQ(medios::controller::Call::UPDATE_MAINTENANCE_SCHEDULE, call.type());
  CHECK(call.has_update_maintenance_schedule());

  medios::maintenance::Schedule schedule =
    call.update_maintenance_schedule().schedule();

  return _updateMaintenanceSchedule(schedule, principal);
}


// /controller/machine/down endpoint help.
string Controller::Http::MACHINE_DOWN_HELP()
{
  return HELP(
    TLDR(
        "Brings a set of machines down."),
    DESCRIPTION(
        "Returns 200 OK when the operation was successful.",
        "",
        "Returns 307 TEMPORARY_REDIRECT redirect to the leading controller when",
        "current controller is not the leader.",
        "",
        "Returns 503 SERVICE_UNAVAILABLE if the leading controller cannot be",
        "found.",
        "",
        "POST: Validates the request body as JSON and transitions",
        "  the list of machines into DOWN mode.  Currently, only",
        "  machines in DRAINING mode are allowed to be brought down."),
    AUTHENTICATION(true),
    AUTHORIZATION(
        "The current principal must be allowed to bring down all the machines",
        "in the request, otherwise the request will fail."));
}


// /controller/machine/down endpoint handler.
Future<Response> Controller::Http::machineDown(
    const Request& request,
    const Option<Principal>& principal) const
{
  // When current controller is not the leader, redirect to the leading controller.
  if (!controller->elected()) {
    return redirect(request);
  }

  if (request.method != "POST") {
    return MethodNotAllowed({"POST"}, request.method);
  }

  // Parse the POST body as JSON.
  Try<JSON::Array> jsonIds = JSON::parse<JSON::Array>(request.body);
  if (jsonIds.isError()) {
    return BadRequest(jsonIds.error());
  }

  // Convert the machines to a protobuf.
  auto ids = ::protobuf::parse<RepeatedPtrField<MachineID>>(jsonIds.get());
  if (ids.isError()) {
    return BadRequest(ids.error());
  }

  return ObjectApprovers::create(
      controller->authorizer,
      principal,
      {START_MAINTENANCE})
    .then(defer(
      controller->self(),
      [this, ids](const Owned<ObjectApprovers>& approvers) {
        return _startMaintenance(ids.get(), approvers);
      }));
}


Future<Response> Controller::Http::_startMaintenance(
    const RepeatedPtrField<MachineID>& machineIds,
    const Owned<ObjectApprovers>& approvers) const
{
  // Validate every machine in the list.
  Try<Nothing> isValid = maintenance::validation::machines(machineIds);
  if (isValid.isError()) {
    return BadRequest(isValid.error());
  }

  // Check that all machines are part of a maintenance schedule.
  // TODO(josephw): Allow a transition from `UP` to `DOWN`.
  foreach (const MachineID& id, machineIds) {
    if (!controller->machines.contains(id)) {
      return BadRequest(
          "Machine '" + stringify(JSON::protobuf(id)) +
            "' is not part of a maintenance schedule");
    }

    if (controller->machines[id].info.mode() != MachineInfo::DRAINING) {
      return BadRequest(
          "Machine '" + stringify(JSON::protobuf(id)) +
            "' is not in DRAINING mode and cannot be brought down");
    }

    if (!approvers->approved<START_MAINTENANCE>(id)) {
      return Forbidden();
    }
  }

  return controller->registrar->apply(Owned<RegistryOperation>(
      new maintenance::StartMaintenance(machineIds)))
    .then(defer(controller->self(), [=](bool result) -> Response {
      // See the top comment in "controller/maintenance.hpp" for why this check
      // is here, and is appropriate.
      CHECK(result);

      // We currently send a `ShutdownMessage` to each node. This terminates
      // all the executors for all the frameworks running on that node.
      // We also manually remove the node to force sending TASK_LOST updates
      // for all the tasks that were running on the node and `LostNodeMessage`
      // messages to the framework. This guards against the node having dropped
      // the `ShutdownMessage`.
      foreach (const MachineID& machineId, machineIds) {
        // The machine may not be in machines. This means no nodes are
        // currently registered on that machine so this is a no-op.
        if (controller->machines.contains(machineId)) {
          // NOTE: Copies are needed because removeNode modifies
          // controller->machines.
          foreach (
              const NodeID& nodeId,
              utils::copy(controller->machines[machineId].nodes)) {
            Node* node = controller->nodes.registered.get(nodeId);
            CHECK_NOTNULL(node);

            // Tell the node to shut down.
            ShutdownMessage shutdownMessage;
            shutdownMessage.set_message("Operator initiated 'Machine DOWN'");
            controller->send(node->pid, shutdownMessage);

            // Immediately remove the node to force sending `TASK_LOST` status
            // updates as well as `LostNodeMessage` messages to the frameworks.
            // See comment above.
            controller->removeNode(node, "Operator initiated 'Machine DOWN'");
          }
        }
      }

      // Update the controller's local state with the downed machines.
      foreach (const MachineID& id, machineIds) {
        controller->machines[id].info.set_mode(MachineInfo::DOWN);
      }

      return OK();
    }));
}


Future<Response> Controller::Http::startMaintenance(
    const medios::controller::Call& call,
    const Option<Principal>& principal,
    ContentType /*contentType*/) const
{
  CHECK_EQ(medios::controller::Call::START_MAINTENANCE, call.type());
  CHECK(call.has_start_maintenance());

  RepeatedPtrField<MachineID> machineIds = call.start_maintenance().machines();

  return ObjectApprovers::create(
      controller->authorizer,
      principal,
      {START_MAINTENANCE})
    .then(defer(
      controller->self(),
      [this, machineIds](const Owned<ObjectApprovers>& approvers) {
        return _startMaintenance(machineIds, approvers);
      }));
}


// /controller/machine/up endpoint help.
string Controller::Http::MACHINE_UP_HELP()
{
  return HELP(
    TLDR(
        "Brings a set of machines back up."),
    DESCRIPTION(
        "Returns 200 OK when the operation was successful.",
        "",
        "Returns 307 TEMPORARY_REDIRECT redirect to the leading controller when",
        "current controller is not the leader.",
        "",
        "Returns 503 SERVICE_UNAVAILABLE if the leading controller cannot be",
        "found.",
        "",
        "POST: Validates the request body as JSON and transitions",
        "  the list of machines into UP mode.  This also removes",
        "  the list of machines from the maintenance schedule."),
    AUTHENTICATION(true),
    AUTHORIZATION(
        "The current principal must be allowed to bring up all the machines",
        "in the request, otherwise the request will fail."));
}


// /controller/machine/up endpoint handler.
Future<Response> Controller::Http::machineUp(
    const Request& request,
    const Option<Principal>& principal) const
{
  // When current controller is not the leader, redirect to the leading controller.
  if (!controller->elected()) {
    return redirect(request);
  }

  if (request.method != "POST") {
    return MethodNotAllowed({"POST"}, request.method);
  }

  // Parse the POST body as JSON.
  Try<JSON::Array> jsonIds = JSON::parse<JSON::Array>(request.body);
  if (jsonIds.isError()) {
    return BadRequest(jsonIds.error());
  }

  // Convert the machines to a protobuf.
  auto ids = ::protobuf::parse<RepeatedPtrField<MachineID>>(jsonIds.get());
  if (ids.isError()) {
    return BadRequest(ids.error());
  }

  return ObjectApprovers::create(
      controller->authorizer,
      principal,
      {STOP_MAINTENANCE})
    .then(defer(
      controller->self(),
      [this, ids](const Owned<ObjectApprovers>& approvers) {
        return _stopMaintenance(ids.get(), approvers);
      }));
}


Future<Response> Controller::Http::_stopMaintenance(
    const RepeatedPtrField<MachineID>& machineIds,
    const Owned<ObjectApprovers>& approvers) const
{
  // Validate every machine in the list.
  Try<Nothing> isValid = maintenance::validation::machines(machineIds);
  if (isValid.isError()) {
    return BadRequest(isValid.error());
  }

  // Check that all machines are part of a maintenance schedule.
  foreach (const MachineID& id, machineIds) {
    if (!controller->machines.contains(id)) {
      return BadRequest(
          "Machine '" + stringify(JSON::protobuf(id)) +
            "' is not part of a maintenance schedule");
    }

    if (controller->machines[id].info.mode() != MachineInfo::DOWN) {
      return BadRequest(
          "Machine '" + stringify(JSON::protobuf(id)) +
            "' is not in DOWN mode and cannot be brought up");
    }

    if (!approvers->approved<STOP_MAINTENANCE>(id)) {
      return Forbidden();
    }
  }

  return controller->registrar->apply(Owned<RegistryOperation>(
      new maintenance::StopMaintenance(machineIds)))
    .then(defer(controller->self(), [=](bool result) -> Future<Response> {
      // See the top comment in "controller/maintenance.hpp" for why this check
      // is here, and is appropriate.
      CHECK(result);

      // Update the controller's local state with the reactivated machines.
      hashset<MachineID> updated;
      foreach (const MachineID& id, machineIds) {
        controller->machines[id].info.set_mode(MachineInfo::UP);
        controller->machines[id].info.clear_unavailability();
        updated.insert(id);
      }

      // Delete the machines from the schedule.
      for (list<medios::maintenance::Schedule>::iterator schedule =
          controller->maintenance.schedules.begin();
          schedule != controller->maintenance.schedules.end();) {
        for (int j = schedule->windows().size() - 1; j >= 0; j--) {
          medios::maintenance::Window* window = schedule->mutable_windows(j);

          // Delete individual machines.
          for (int k = window->machine_ids().size() - 1; k >= 0; k--) {
            if (updated.contains(window->machine_ids(k))) {
              window->mutable_machine_ids()->DeleteSubrange(k, 1);
            }
          }

          // If the resulting window is empty, delete it.
          if (window->machine_ids().size() == 0) {
            schedule->mutable_windows()->DeleteSubrange(j, 1);
          }
        }

        // If the resulting schedule is empty, delete it.
        if (schedule->windows().size() == 0) {
          schedule = controller->maintenance.schedules.erase(schedule);
        } else {
          ++schedule;
        }
      }

      return OK();
    }));
}


Future<Response> Controller::Http::stopMaintenance(
    const medios::controller::Call& call,
    const Option<Principal>& principal,
    ContentType /*contentType*/) const
{
  CHECK_EQ(medios::controller::Call::STOP_MAINTENANCE, call.type());
  CHECK(call.has_stop_maintenance());

  RepeatedPtrField<MachineID> machineIds = call.stop_maintenance().machines();

  return ObjectApprovers::create(
      controller->authorizer,
      principal,
      {STOP_MAINTENANCE})
    .then(defer(
      controller->self(),
      [this, machineIds](const Owned<ObjectApprovers>& approvers) {
        return _stopMaintenance(machineIds, approvers);
      }));
}


// /controller/maintenance/status endpoint help.
string Controller::Http::MAINTENANCE_STATUS_HELP()
{
  return HELP(
    TLDR(
        "Retrieves the maintenance status of the cluster."),
    DESCRIPTION(
        "Returns 200 OK when the maintenance status was queried successfully.",
        "",
        "Returns 307 TEMPORARY_REDIRECT redirect to the leading controller when",
        "current controller is not the leader.",
        "",
        "Returns 503 SERVICE_UNAVAILABLE if the leading controller cannot be",
        "found.",
        "",
        "Returns an object with one list of machines per machine mode.",
        "For draining machines, this list includes the frameworks' responses",
        "to inverse offers.",
        "**NOTE**:",
        "Inverse offer responses are cleared if the controller fails over.",
        "However, new inverse offers will be sent once the controller recovers."),
    AUTHENTICATION(true),
    AUTHORIZATION(
        "The response will contain only the maintenance status for those",
        "machines the current principal is allowed to see. If none, an empty",
        "response will be returned."));
}


// /controller/maintenance/status endpoint handler.
Future<Response> Controller::Http::maintenanceStatus(
    const Request& request,
    const Option<Principal>& principal) const
{
  // When current controller is not the leader, redirect to the leading controller.
  if (!controller->elected()) {
    return redirect(request);
  }

  if (request.method != "GET") {
    return MethodNotAllowed({"GET"}, request.method);
  }

  Option<string> jsonp = request.url.query.get("jsonp");

  return ObjectApprovers::create(
      controller->authorizer,
      principal,
      {GET_MAINTENANCE_STATUS})
    .then(defer(
      controller->self(),
      [this](const Owned<ObjectApprovers>& approvers) {
        return _getMaintenanceStatus(approvers);
      }))
    .then([jsonp](const medios::maintenance::ClusterStatus& status) -> Response {
      return OK(JSON::protobuf(status), jsonp);
    });
}


Future<medios::maintenance::ClusterStatus> Controller::Http::_getMaintenanceStatus(
    const Owned<ObjectApprovers>& approvers) const
{
  return controller->allocator->getInverseOfferStatuses()
    .then(defer(
        controller->self(),
        [=](hashmap<
            NodeID,
            hashmap<FrameworkID, medios::allocator::InverseOfferStatus>> result)
          -> Future<medios::maintenance::ClusterStatus> {
    // Unwrap the controller's machine information into two arrays of machines.
    // The data is coming from the allocator and therefore could be stale.
    // Also, if the controller fails over, this data is cleared.
    medios::maintenance::ClusterStatus status;
    foreachpair (
        const MachineID& id,
        const Machine& machine,
        controller->machines) {
      if (!approvers->approved<GET_MAINTENANCE_STATUS>(id)) {
        continue;
      }

      switch (machine.info.mode()) {
        case MachineInfo::DRAINING: {
          medios::maintenance::ClusterStatus::DrainingMachine* drainingMachine =
            status.add_draining_machines();

          drainingMachine->mutable_id()->CopyFrom(id);

          // Unwrap inverse offer status information from the allocator.
          foreach (const NodeID& node, machine.nodes) {
            if (result.contains(node)) {
              foreachvalue (
                  const medios::allocator::InverseOfferStatus& status,
                  result[node]) {
                drainingMachine->add_statuses()->CopyFrom(status);
              }
            }
          }
          break;
        }

        case MachineInfo::DOWN: {
          status.add_down_machines()->CopyFrom(id);
          break;
        }

        // Currently, `UP` machines are not specifically tracked in the controller.
        case MachineInfo::UP: {}
        default: {
          break;
        }
      }
    }

    return status;
  }));
}


Future<Response> Controller::Http::getMaintenanceStatus(
    const medios::controller::Call& call,
    const Option<Principal>& principal,
    ContentType contentType) const
{
  CHECK_EQ(medios::controller::Call::GET_MAINTENANCE_STATUS, call.type());

  return ObjectApprovers::create(
      controller->authorizer,
      principal,
      {GET_MAINTENANCE_STATUS})
    .then(defer(
      controller->self(),
      [this](const Owned<ObjectApprovers>& approvers) {
        return _getMaintenanceStatus(approvers);
      }))
    .then([contentType](const medios::maintenance::ClusterStatus& status)
          -> Response {
        medios::controller::Response response;
        response.set_type(medios::controller::Response::GET_MAINTENANCE_STATUS);
        response.mutable_get_maintenance_status()->mutable_status()
            ->CopyFrom(status);

        return OK(serialize(contentType, evolve(response)),
                  stringify(contentType));
      });
}


Future<Response> Controller::Http::_drainNode(
    const NodeID& nodeId,
    const Option<DurationInfo>& maxGracePeriod,
    const bool markGone,
    const Owned<ObjectApprovers>& approvers) const
{
  if (!approvers->approved<DRAIN_NODE>()) {
    return Forbidden();
  }

  if (markGone && !approvers->approved<MARK_NODE_GONE>()) {
    return Forbidden();
  }

  // Check that the node is either recovering, registered, or unreachable.
  if (!controller->nodes.recovered.contains(nodeId) &&
      !controller->nodes.registered.contains(nodeId) &&
      !controller->nodes.unreachable.contains(nodeId)) {
    return BadRequest("Unknown node");
  }

  // If this node is being marked gone, then no draining can be performed.
  if (controller->nodes.markingGone.contains(nodeId)) {
    return Conflict("Node is currently being marked gone");
  }

  // Save the draining info to the registry.
  return controller->registrar->apply(Owned<RegistryOperation>(
      new DrainNode(nodeId, maxGracePeriod, markGone)))
    .onAny([](const Future<bool>& result) {
      CHECK_READY(result)
        << "Failed to update draining info in the registry";
    })
    .then(defer(
        controller->self(),
        [this, nodeId, maxGracePeriod, markGone](bool result) -> Response {
          // Update the in-memory state.
          DrainConfig drainConfig;
          drainConfig.set_mark_gone(markGone);

          if (maxGracePeriod.isSome()) {
            drainConfig.mutable_max_grace_period()
              ->CopyFrom(maxGracePeriod.get());
          }

          DrainInfo drainInfo;
          drainInfo.set_state(DRAINING);
          drainInfo.mutable_config()->CopyFrom(drainConfig);

          controller->nodes.draining[nodeId] = drainInfo;

          // Deactivate the node.
          controller->nodes.deactivated.insert(nodeId);

          Node* node = controller->nodes.registered.get(nodeId);

          // It's possible for the node to be removed in the interim
          // if it is marked unreachable.
          if (node != nullptr) {
            hashmap<FrameworkID, hashset<TaskID>> taskIds;
            foreachpair (const FrameworkID& frameworkId,
                         const auto& tasks,
                         node->tasks) {
              taskIds[frameworkId] = tasks.keys();
            }

            LOG(INFO)
              << "Transitioning node " << nodeId << " to the DRAINING state"
              << "; node has (tasks, operations) == ("
              << stringify(taskIds) << ", "
              << stringify(node->operations.keys()) << ")";

            controller->deactivate(node);

            // Tell the node to start draining.
            DrainNodeMessage message;
            message.mutable_config()->CopyFrom(drainConfig);
            controller->send(node->pid, message);

            node->estimatedDrainStartTime = Clock::now();

            // Check if the node is already drained and transition it
            // appropriately if so.
            controller->checkAndTransitionDrainingNode(node);
          }

          return OK();
        }));
}


Future<Response> Controller::Http::drainNode(
    const medios::controller::Call& call,
    const Option<Principal>& principal,
    ContentType /*contentType*/) const
{
  CHECK_EQ(medios::controller::Call::DRAIN_NODE, call.type());
  CHECK(call.has_drain_node());

  NodeID nodeId = call.drain_node().node_id();
  Node* node = controller->nodes.registered.get(nodeId);

  if (node != nullptr) {
    // Check that the targeted node is not part of a maintenance schedule.
    // NOTE: This is a best-effort check, because it is possible to drain
    // a node, and then change the node's hostname/IP into a MachineID
    // in a maintenance schedule. Also, the MachineID of unreachable nodes
    // is unknown until they reregister.
    //
    // TODO(josephw): Reconsider this check once the maintenance and node
    // draining features are integrated.
    //
    // TODO(josephw): Check this condition against unreachable nodes
    // once MESOS-9884 is resolved.
    if (!controller->maintenance.schedules.empty()) {
      foreach (
          const medios::maintenance::Window& window,
          controller->maintenance.schedules.front().windows()) {
        foreach (const MachineID& machineId, window.machine_ids()) {
          if (machineId == node->machineId) {
            return BadRequest(
                "Node " + stringify(nodeId) + " is part of a maintenance"
                " schedule under Machine " + stringify(machineId));
          }
        }
      }
    }

    // Check that the targeted node is capable of `NODE_DRAINING`.
    // NOTE: This is a best-effort check, because it is possible to drain
    // a node, and then downgrade the node to a version that does not
    // support draining. Also, the capabilities of unreachable nodes
    // are unknown until they reregister.
    //
    // TODO(josephw): Check this condition against unreachable nodes
    // once MESOS-9884 is resolved.
    if (!node->capabilities.nodeDraining) {
      return BadRequest(
          "Node " + stringify(nodeId) + " is not capable of draining");
    }
  }

  Option<DurationInfo> maxGracePeriod;
  if (call.drain_node().has_max_grace_period()) {
    maxGracePeriod = call.drain_node().max_grace_period();
  }

  bool markGone = call.drain_node().mark_gone();

  return ObjectApprovers::create(
      controller->authorizer,
      principal,
      {DRAIN_NODE, MARK_NODE_GONE})
    .then(defer(
      controller->self(),
      [this, nodeId, maxGracePeriod, markGone](
          const Owned<ObjectApprovers>& approvers) {
        return _drainNode(nodeId, maxGracePeriod, markGone, approvers);
      }));
}


Future<Response> Controller::Http::_deactivateNode(
    const NodeID& nodeId,
    const Owned<ObjectApprovers>& approvers) const
{
  if (!approvers->approved<DEACTIVATE_NODE>()) {
    return Forbidden();
  }

  // Check that the node is either recovering, registered, or unreachable.
  if (!controller->nodes.recovered.contains(nodeId) &&
      !controller->nodes.registered.contains(nodeId) &&
      !controller->nodes.unreachable.contains(nodeId)) {
    return BadRequest("Unknown node");
  }

  // Save the deactivation to the registry.
  return controller->registrar->apply(Owned<RegistryOperation>(
      new DeactivateNode(nodeId)))
    .onAny([](const Future<bool>& result) {
      CHECK_READY(result)
        << "Failed to deactivate node in the registry";
    })
    .then(defer(controller->self(), [this, nodeId](bool result) -> Response {
      // Deactivate the node.
      controller->nodes.deactivated.insert(nodeId);

      Node* node = controller->nodes.registered.get(nodeId);
      if (node != nullptr) {
        controller->deactivate(node);
      }

      return OK();
    }));
}


Future<Response> Controller::Http::deactivateNode(
    const medios::controller::Call& call,
    const Option<Principal>& principal,
    ContentType /*contentType*/) const
{
  CHECK_EQ(medios::controller::Call::DEACTIVATE_NODE, call.type());
  CHECK(call.has_deactivate_node());

  NodeID nodeId = call.deactivate_node().node_id();

  return ObjectApprovers::create(
      controller->authorizer,
      principal,
      {DEACTIVATE_NODE})
    .then(defer(
      controller->self(),
      [this, nodeId](
          const Owned<ObjectApprovers>& approvers) {
        return _deactivateNode(nodeId, approvers);
      }));
}


Future<Response> Controller::Http::_reactivateNode(
    const NodeID& nodeId,
    const Owned<ObjectApprovers>& approvers) const
{
  if (!approvers->approved<REACTIVATE_NODE>()) {
    return Forbidden();
  }

  // Check that the node is deactivated.
  if (!controller->nodes.deactivated.contains(nodeId)) {
    return BadRequest("Node is not deactivated");
  }

  if (controller->nodes.draining.contains(nodeId) &&
      controller->nodes.draining.at(nodeId).state() == DRAINING) {
    return BadRequest("Node is still in the DRAINING state");
  }

  // Save the reactivation to the registry.
  return controller->registrar->apply(Owned<RegistryOperation>(
      new ReactivateNode(nodeId)))
    .onAny([](const Future<bool>& result) {
      CHECK_READY(result)
        << "Failed to reactivate node in the registry";
    })
    .then(defer(controller->self(), [this, nodeId](bool result) -> Response {
      // Reactivate the node.
      controller->nodes.draining.erase(nodeId);
      controller->nodes.deactivated.erase(nodeId);

      Node* node = controller->nodes.registered.get(nodeId);
      if (node == nullptr) {
        return Conflict("Node removed while processing the call");
      }

      if (node->connected) {
        LOG(INFO) << "Reactivating node " << *node;

        node->active = true;
        controller->allocator->activateNode(nodeId);
      } else {
        LOG(INFO) << "Disconnected node " << *node
                  << " will be reactivated upon reregistration.";
      }

      node->estimatedDrainStartTime = None();

      return OK();
    }));
}


Future<Response> Controller::Http::reactivateNode(
    const medios::controller::Call& call,
    const Option<Principal>& principal,
    ContentType /*contentType*/) const
{
  CHECK_EQ(medios::controller::Call::REACTIVATE_NODE, call.type());
  CHECK(call.has_reactivate_node());

  NodeID nodeId = call.reactivate_node().node_id();

  return ObjectApprovers::create(
      controller->authorizer,
      principal,
      {REACTIVATE_NODE})
    .then(defer(
      controller->self(),
      [this, nodeId](
          const Owned<ObjectApprovers>& approvers) {
        return _reactivateNode(nodeId, approvers);
      }));
}


string Controller::Http::UNRESERVE_HELP()
{
  return HELP(
    TLDR(
        "Unreserve resources dynamically on a specific node."),
    DESCRIPTION(
        "Returns 202 ACCEPTED which indicates that the unreserve",
        "operation has been validated successfully by the controller.",
        "",
        "Returns 307 TEMPORARY_REDIRECT redirect to the leading controller when",
        "current controller is not the leader.",
        "",
        "Returns 503 SERVICE_UNAVAILABLE if the leading controller cannot be",
        "found.",
        "",
        "The request is then forwarded asynchronously to the Medios",
        "node where the reserved resources are located.",
        "That asynchronous message may not be delivered or",
        "unreserving resources at the node might fail.",
        "",
        "Please provide \"nodeId\" and \"resources\" values describing",
        "the resources to be unreserved."),
    AUTHENTICATION(true),
    AUTHORIZATION(
        "Using this endpoint to unreserve resources requires that the",
        "current principal is authorized to unreserve resources created",
        "by the principal who reserved the resources.",
        "See the authorization documentation for details."));
}


Future<Response> Controller::Http::unreserve(
    const Request& request,
    const Option<Principal>& principal) const
{
  // TODO(greggomann): Remove this check once the `Principal` type is used in
  // `ReservationInfo`, `DiskInfo`, and within the controller's `principals` map.
  // See MESOS-7202.
  if (principal.isSome() && principal->value.isNone()) {
    return Forbidden(
        "The request's authenticated principal contains claims, but no value "
        "string. The controller currently requires that principals have a value");
  }

  // When current controller is not the leader, redirect to the leading controller.
  if (!controller->elected()) {
    return redirect(request);
  }

  if (request.method != "POST") {
    return MethodNotAllowed({"POST"}, request.method);
  }

  // Parse the query string in the request body.
  Try<hashmap<string, string>> decode =
    process::http::query::decode(request.body);

  if (decode.isError()) {
    return BadRequest("Unable to decode query string: " + decode.error());
  }

  const hashmap<string, string>& values = decode.get();

  Option<string> value;

  value = values.get("nodeId");
  if (value.isNone()) {
    return BadRequest("Missing 'nodeId' query parameter in the request body");
  }

  NodeID nodeId;
  nodeId.set_value(value.get());

  value = values.get("resources");
  if (value.isNone()) {
    return BadRequest(
        "Missing 'resources' query parameter in the request body");
  }

  Try<JSON::Array> parse =
    JSON::parse<JSON::Array>(value.get());

  if (parse.isError()) {
    return BadRequest(
        "Error in parsing 'resources' query parameter in the request body: " +
        parse.error());
  }

  RepeatedPtrField<Resource> resources;
  foreach (const JSON::Value& value, parse->values) {
    Try<Resource> resource = ::protobuf::parse<Resource>(value);
    if (resource.isError()) {
      return BadRequest(
          "Error in parsing 'resources' query parameter in the request body: " +
          resource.error());
    }

    resources.Add()->CopyFrom(resource.get());
  }

  return _unreserve(nodeId, resources, principal);
}


Future<Response> Controller::Http::_unreserve(
    const NodeID& nodeId,
    const RepeatedPtrField<Resource>& resources,
    const Option<Principal>& principal) const
{
  Node* node = controller->nodes.registered.get(nodeId);
  if (node == nullptr) {
    return BadRequest("No node found with specified ID");
  }

  // Create an operation.
  Offer::Operation operation;
  operation.set_type(Offer::Operation::UNRESERVE);
  operation.mutable_unreserve()->mutable_resources()->CopyFrom(resources);

  Option<Error> error = validateAndUpgradeResources(&operation);
  if (error.isSome()) {
    return BadRequest(error->message);
  }

  error = validation::operation::validate(operation.unreserve());
  if (error.isSome()) {
    return BadRequest("Invalid UNRESERVE operation: " + error->message);
  }

  return controller->authorize(
      principal, ActionObject::unreserve(operation.unreserve()))
    .then(defer(controller->self(), [=](bool authorized) -> Future<Response> {
      if (!authorized) {
        return Forbidden();
      }

      return _operation(nodeId, operation);
    }));
}


Future<Response> Controller::Http::_operation(
    const NodeID& nodeId,
    const Offer::Operation& operation) const
{
  Try<Resources> required = protobuf::getConsumedResources(operation);

  if (required.isError()) {
    return BadRequest(
        "Invalid " + stringify(operation.type()) + " operation: " +
        required.error());
  }

  Node* node = controller->nodes.registered.get(nodeId);
  if (node == nullptr) {
    return BadRequest("No node found with specified ID");
  }

  // The resources recovered by rescinding outstanding offers.
  Resources totalRecovered;

  // We pessimistically assume that what seems like "available"
  // resources in the allocator will be gone. This can happen due to
  // the race between the allocator scheduling an 'allocate' call to
  // itself vs controller's request to schedule 'updateAvailable'.
  // We greedily rescind one offer at time until we've rescinded
  // enough offers to cover 'operation'.
  foreach (Offer* offer, utils::copy(node->offers)) {
    // If rescinding the offer would not contribute to satisfying
    // the required resources, skip it.
    Resources recovered = offer->resources();
    recovered.unallocate();

    if (required.get() == required.get() - recovered) {
      continue;
    }

    totalRecovered += recovered;
    required.get() -= recovered;

    // We explicitly pass 'Filters()' which has a default 'refuse_seconds'
    // of 5 seconds rather than 'None()' here, so that we can virtually
    // always win the race against 'allocate' if these resources are to
    // be offered back to these frameworks.
    // NOTE: However it's entirely possible that these resources are
    // offered to other frameworks in the next 'allocate' and the filter
    // cannot prevent it.
    controller->rescindOffer(offer, Filters());

    // If we've rescinded enough offers to cover 'operation', we're done.
    Try<Resources> updatedRecovered = totalRecovered.apply(operation);
    if (updatedRecovered.isSome()) {
      break;
    }
  }

  // Propagate the 'Future<Nothing>' as 'Future<Response>' where
  // 'Nothing' -> 'Accepted' and Failed -> 'Conflict'.
  return controller->apply(node, operation)
    .then([]() -> Response { return Accepted(); })
    .repair([](const Future<Response>& result) {
       return Conflict(result.failure());
    });
}


Future<Response> Controller::Http::unreserveResources(
    const medios::controller::Call& call,
    const Option<Principal>& principal,
    ContentType contentType) const
{
  CHECK_EQ(medios::controller::Call::UNRESERVE_RESOURCES, call.type());

  const NodeID& nodeId = call.unreserve_resources().node_id();
  const RepeatedPtrField<Resource>& resources =
    call.unreserve_resources().resources();

  return _unreserve(nodeId, resources, principal);
}


Future<Response> Controller::Http::markNodeGone(
    const medios::controller::Call& call,
    const Option<Principal>& principal,
    ContentType contentType) const
{
  CHECK_EQ(medios::controller::Call::MARK_NODE_GONE, call.type());

  const NodeID& nodeId = call.mark_node_gone().node_id();

  return ObjectApprovers::create(
      controller->authorizer,
      principal,
      {MARK_NODE_GONE})
    .then(defer(
      controller->self(),
      [this, nodeId](const Owned<ObjectApprovers>& approvers)
          -> Future<Response> {
        if (!approvers->approved<MARK_NODE_GONE>()) {
          return Forbidden();
        }

        return _markNodeGone(nodeId);
      }));
}


Future<Response> Controller::Http::_markNodeGone(const NodeID& nodeId) const
{
  LOG(INFO) << "Marking node '" << nodeId << "' as gone";

  if (controller->nodes.gone.contains(nodeId)) {
    LOG(WARNING) << "Not marking node '" << nodeId
                 << "' as gone because it has already transitioned to gone";
    return OK();
  }

  // We return a `ServiceUnavailable` (retryable error) if there is
  // an ongoing registry transition to gone/removed/unreachable.
  if (controller->nodes.markingGone.contains(nodeId)) {
    LOG(WARNING) << "Not marking node '" << nodeId
                 << "' as gone because another gone transition"
                 << " is already in progress";

    return ServiceUnavailable(
        "Node '" + stringify(nodeId) + "' is already being transitioned"
        + " to gone");
  }

  if (controller->nodes.removing.contains(nodeId)) {
    LOG(WARNING) << "Not marking node '" << nodeId
                 << "' as gone because another remove transition"
                 << " is already in progress";

    return ServiceUnavailable(
        "Node '" + stringify(nodeId) + "' is being transitioned to removed");
  }

  if (controller->nodes.markingUnreachable.contains(nodeId)) {
    LOG(WARNING) << "Not marking node '" << nodeId
                 << "' as gone because another unreachable transition"
                 << " is already in progress";

    return ServiceUnavailable(
        "Node '" + stringify(nodeId) + "' is being transitioned to"
        + " unreachable");
  }

  // We currently support marking a node gone if the node
  // is present in the list of active, unreachable or recovered nodes.
  bool found = false;

  if (controller->nodes.registered.contains(nodeId)) {
    found = true;
  } else if(controller->nodes.recovered.contains(nodeId)) {
    found = true;
  } else if (controller->nodes.unreachable.contains(nodeId)) {
    found = true;
  }

  if (!found) {
    return NotFound("Node '" + stringify(nodeId) + "' not found");
  }

  controller->nodes.markingGone.insert(nodeId);

  TimeInfo goneTime = protobuf::getCurrentTime();

  Future<bool> gone = controller->registrar->apply(Owned<RegistryOperation>(
      new MarkNodeGone(nodeId, goneTime)));

  gone.onAny(defer(
      controller->self(), [this, nodeId, goneTime](Future<bool> registrarResult) {
    CHECK(!registrarResult.isDiscarded());

    if (registrarResult.isFailed()) {
      LOG(FATAL) << "Failed to mark node " << nodeId
                 << " as gone in the registry: "
                 << registrarResult.failure();
    }

    controller->markGone(nodeId, goneTime);
  }));

  return gone.then([]() -> Future<Response> {
    return OK();
  });
}

} // namespace controller {
} // namespace internal {
} // namespace medios {
