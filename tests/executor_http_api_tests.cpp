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

#include <string>
#include <vector>

#include <medios/v1/executor/executor.hpp>

#include <medios/http.hpp>

#include <process/clock.hpp>
#include <process/future.hpp>
#include <process/gtest.hpp>
#include <process/http.hpp>
#include <process/message.hpp>
#include <process/owned.hpp>
#include <process/pid.hpp>

#include <stout/lambda.hpp>
#include <stout/uuid.hpp>

#include "common/http.hpp"
#include "common/recordio.hpp"

#include "controller/controller.hpp"

#include "controller/detector/standalone.hpp"

#include "tests/containerizer.hpp"
#include "tests/medios.hpp"
#include "tests/utils.hpp"

#include "tests/containerizer/mock_containerizer.hpp"

using medios::internal::controller::Controller;

using medios::internal::recordio::Reader;

using medios::internal::node::Node;

using medios::controller::detector::ControllerDetector;
using medios::controller::detector::StandaloneControllerDetector;

using medios::v1::executor::Call;
using medios::v1::executor::Event;

using process::Clock;
using process::Future;
using process::Message;
using process::Owned;
using process::PID;
using process::Promise;

using process::http::BadRequest;
using process::http::MethodNotAllowed;
using process::http::NotAcceptable;
using process::http::OK;
using process::http::Pipe;
using process::http::Response;
using process::http::ServiceUnavailable;
using process::http::UnsupportedMediaType;

using recordio::Decoder;

using std::string;
using std::vector;

using testing::AtMost;
using testing::DoAll;
using testing::Eq;
using testing::WithParamInterface;

namespace medios {
namespace v1 {
namespace executor {

// Forward defined constant found in `executor/executor.cpp`.
// TODO(josephw): Remove this when this constant is moved into a header.
extern const Duration DEFAULT_HEARTBEAT_CALL_INTERVAL;

} // namespace executor {
} // namespace v1 {

namespace internal {
namespace tests {


class ExecutorHttpApiTest
  : public MediosTest,
    public WithParamInterface<ContentType>
{
protected:
  node::Flags CreateNodeFlags() override
  {
    node::Flags flags = MediosTest::CreateNodeFlags();

#ifdef USE_SSL_SOCKET
    // Disable executor authentication on the node. Executor authentication
    // currently has SSL as a dependency, so this is only necessary if Medios was
    // built with SSL support.
    flags.authenticate_http_executors = false;
#endif // USE_SSL_SOCKET

    return flags;
  }
};


// The tests are parameterized by the content type of the request.
INSTANTIATE_TEST_CASE_P(
    ContentType,
    ExecutorHttpApiTest,
    ::testing::Values(ContentType::PROTOBUF, ContentType::JSON));


// TODO(anand): Add more validation tests for:
// - If the node is still recovering, the call should return
//   ServiceUnavailable.
// - If Executor is not found, the call should return
//   BadRequest.
// - If Executor has not registered and sends a Call message other
//   than Subscribe, the call should return Forbidden.


// This test expects a BadRequest when 'Content-Type' is omitted.
TEST_F(ExecutorHttpApiTest, NoContentType)
{
  Try<Owned<cluster::Controller>> controller = StartController();
  ASSERT_SOME(controller);

  Future<Nothing> __recover = FUTURE_DISPATCH(_, &Node::__recover);

  Owned<ControllerDetector> detector = controller.get()->createDetector();

  node::Flags flags = CreateNodeFlags();

  Try<Owned<cluster::Node>> node = StartNode(detector.get(), flags);
  ASSERT_SOME(node);

  AWAIT_READY(__recover);

  // Wait for recovery to be complete.
  Clock::pause();
  Clock::settle();

  Call call;
  call.mutable_framework_id()->set_value("dummy_framework_id");
  call.mutable_executor_id()->set_value("dummy_executor_id");
  call.set_type(Call::MESSAGE);

  call.mutable_message()->set_data("hello world");

  Future<Response> response = process::http::post(
      node.get()->pid,
      "api/v1/executor",
      None(),
      serialize(ContentType::JSON, call),
      None());

  AWAIT_EXPECT_RESPONSE_STATUS_EQ(BadRequest().status, response);
}


// This test sends a valid JSON blob that cannot be deserialized
// into a valid protobuf resulting in a BadRequest.
TEST_F(ExecutorHttpApiTest, ValidJsonButInvalidProtobuf)
{
  Try<Owned<cluster::Controller>> controller = StartController();
  ASSERT_SOME(controller);

  Future<Nothing> __recover = FUTURE_DISPATCH(_, &Node::__recover);

  Owned<ControllerDetector> detector = controller.get()->createDetector();

  node::Flags flags = CreateNodeFlags();

  Try<Owned<cluster::Node>> node = StartNode(detector.get(), flags);
  ASSERT_SOME(node);

  AWAIT_READY(__recover);

  // Wait for recovery to be complete.
  Clock::pause();
  Clock::settle();

  JSON::Object object;
  object.values["string"] = "valid_json";

  process::http::Headers headers;
  headers["Accept"] = APPLICATION_JSON;

  Future<Response> response = process::http::post(
      node.get()->pid,
      "api/v1/executor",
      headers,
      stringify(object),
      APPLICATION_JSON);

  AWAIT_EXPECT_RESPONSE_STATUS_EQ(BadRequest().status, response);
}


// This test sends a malformed body that cannot be deserialized
// into a valid protobuf resulting in a BadRequest.
TEST_P(ExecutorHttpApiTest, MalformedContent)
{
  Try<Owned<cluster::Controller>> controller = StartController();
  ASSERT_SOME(controller);

  Future<Nothing> __recover = FUTURE_DISPATCH(_, &Node::__recover);

  Owned<ControllerDetector> detector = controller.get()->createDetector();

  node::Flags flags = CreateNodeFlags();

  Try<Owned<cluster::Node>> node = StartNode(detector.get(), flags);
  ASSERT_SOME(node);

  AWAIT_READY(__recover);

  // Wait for recovery to be complete.
  Clock::pause();
  Clock::settle();

  const string body = "MALFORMED_CONTENT";

  const ContentType contentType = GetParam();
  process::http::Headers headers;
  headers["Accept"] = stringify(contentType);

  Future<Response> response = process::http::post(
      node.get()->pid,
      "api/v1/executor",
      headers,
      body,
      stringify(contentType));

  AWAIT_EXPECT_RESPONSE_STATUS_EQ(BadRequest().status, response);
}


// This test sets an unsupported media type as Content-Type. This
// should result in a 415 (UnsupportedMediaType) response.
TEST_P(ExecutorHttpApiTest, UnsupportedContentMediaType)
{
  Try<Owned<cluster::Controller>> controller = StartController();
  ASSERT_SOME(controller);

  Future<Nothing> __recover = FUTURE_DISPATCH(_, &Node::__recover);

  Owned<ControllerDetector> detector = controller.get()->createDetector();

  node::Flags flags = CreateNodeFlags();

  Try<Owned<cluster::Node>> node = StartNode(detector.get(), flags);
  ASSERT_SOME(node);

  AWAIT_READY(__recover);

  // Wait for recovery to be complete.
  Clock::pause();
  Clock::settle();

  ContentType contentType = GetParam();
  process::http::Headers headers;
  headers["Accept"] = stringify(contentType);

  Call call;
  call.mutable_framework_id()->set_value("dummy_framework_id");
  call.mutable_executor_id()->set_value("dummy_executor_id");
  call.set_type(Call::SUBSCRIBE);

  call.mutable_subscribe();

  const string unknownMediaType = "application/unknown-media-type";

  Future<Response> response = process::http::post(
      node.get()->pid,
      "api/v1/executor",
      headers,
      serialize(contentType, call),
      unknownMediaType);

  AWAIT_EXPECT_RESPONSE_STATUS_EQ(UnsupportedMediaType().status, response);
}


// This test sends a Call from an unknown FrameworkID. The call
// should return a BadRequest.
TEST_P(ExecutorHttpApiTest, MessageFromUnknownFramework)
{
  Try<Owned<cluster::Controller>> controller = StartController();
  ASSERT_SOME(controller);

  Future<Nothing> __recover = FUTURE_DISPATCH(_, &Node::__recover);

  Owned<ControllerDetector> detector = controller.get()->createDetector();

  node::Flags flags = CreateNodeFlags();

  Try<Owned<cluster::Node>> node = StartNode(detector.get(), flags);
  ASSERT_SOME(node);

  AWAIT_READY(__recover);

  // Wait for recovery to be complete.
  Clock::pause();
  Clock::settle();

  ContentType contentType = GetParam();
  process::http::Headers headers;
  headers["Accept"] = stringify(contentType);

  Call call;
  call.mutable_framework_id()->set_value("dummy_framework_id");
  call.mutable_executor_id()->set_value("dummy_executor_id");
  call.set_type(Call::MESSAGE);

  call.mutable_message()->set_data("hello world");

  Future<Response> response = process::http::post(
      node.get()->pid,
      "api/v1/executor",
      headers,
      serialize(contentType, call),
      stringify(contentType));

  AWAIT_EXPECT_RESPONSE_STATUS_EQ(BadRequest().status, response);
}


// This test sends a GET request to the executor HTTP endpoint instead
// of a POST. The call should return a MethodNotAllowed response.
TEST_F(ExecutorHttpApiTest, GetRequest)
{
  Try<Owned<cluster::Controller>> controller = StartController();
  ASSERT_SOME(controller);

  Future<Nothing> __recover = FUTURE_DISPATCH(_, &Node::__recover);

  Owned<ControllerDetector> detector = controller.get()->createDetector();

  node::Flags flags = CreateNodeFlags();

  Try<Owned<cluster::Node>> node = StartNode(detector.get(), flags);
  ASSERT_SOME(node);

  AWAIT_READY(__recover);

  // Wait for recovery to be complete.
  Clock::pause();
  Clock::settle();

  Future<Response> response = process::http::get(
      node.get()->pid,
      "api/v1/executor");

  AWAIT_READY(response);
  AWAIT_EXPECT_RESPONSE_STATUS_EQ(MethodNotAllowed({"POST"}).status, response);
}


// This test sends in an Accept:*/* header meaning it would
// accept any media type in the response. We expect the
// default "application/json" media type.
TEST_P(ExecutorHttpApiTest, DefaultAccept)
{
  Try<Owned<cluster::Controller>> controller = StartController();
  ASSERT_SOME(controller);

  auto executor = std::make_shared<v1::MockHTTPExecutor>();

  ExecutorID executorId = DEFAULT_EXECUTOR_ID;
  TestContainerizer containerizer(executorId, executor);

  Owned<ControllerDetector> detector = controller.get()->createDetector();

  node::Flags flags = CreateNodeFlags();

  Try<Owned<cluster::Node>> node = StartNode(
      detector.get(),
      &containerizer,
      flags);
  ASSERT_SOME(node);

  MockScheduler sched;
  MediosSchedulerDriver driver(
      &sched, DEFAULT_FRAMEWORK_INFO, controller.get()->pid, DEFAULT_CREDENTIAL);

  Future<FrameworkID> frameworkId;
  EXPECT_CALL(sched, registered(&driver, _, _))
    .WillOnce(FutureArg<1>(&frameworkId));

  Future<vector<Offer>> offers;
  EXPECT_CALL(sched, resourceOffers(&driver, _))
    .WillOnce(FutureArg<1>(&offers));

  driver.start();

  AWAIT_READY(frameworkId);
  AWAIT_READY(offers);

  ASSERT_EQ(1u, offers->size());

  Future<v1::executor::Medios*> executorLib;
  EXPECT_CALL(*executor, connected(_))
    .WillOnce(FutureArg<0>(&executorLib));

  TaskInfo taskInfo = createTask(offers.get()[0], "", executorId);
  driver.launchTasks(offers.get()[0].id(), {taskInfo});

  // Wait for the executor to be launched before sending
  // an executor subscribe request.
  AWAIT_READY(executorLib);

  // Only subscribe needs to 'Accept' JSON or protobuf.
  Call call;
  call.mutable_framework_id()->CopyFrom(evolve(frameworkId.get()));
  call.mutable_executor_id()->CopyFrom(evolve(executorId));

  call.set_type(Call::SUBSCRIBE);

  call.mutable_subscribe();

  // Retrieve the parameter passed as content type to this test.
  const ContentType contentType = GetParam();

  process::http::Headers headers;
  headers["Accept"] = "*/*";

  Future<Response> response = process::http::streaming::post(
      node.get()->pid,
      "api/v1/executor",
      headers,
      serialize(contentType, call),
      stringify(contentType));

  AWAIT_EXPECT_RESPONSE_STATUS_EQ(OK().status, response);
  AWAIT_EXPECT_RESPONSE_HEADER_EQ(APPLICATION_JSON, "Content-Type", response);

  driver.stop();
  driver.join();
}


// This test does not set any Accept header for the subscribe call.
// The default response media type should be "application/json" in
// this case.
TEST_P(ExecutorHttpApiTest, NoAcceptHeader)
{
  Try<Owned<cluster::Controller>> controller = StartController();
  ASSERT_SOME(controller);

  auto executor = std::make_shared<v1::MockHTTPExecutor>();

  ExecutorID executorId = DEFAULT_EXECUTOR_ID;
  TestContainerizer containerizer(executorId, executor);

  Owned<ControllerDetector> detector = controller.get()->createDetector();

  node::Flags flags = CreateNodeFlags();

  Try<Owned<cluster::Node>> node = StartNode(
      detector.get(),
      &containerizer,
      flags);
  ASSERT_SOME(node);

  MockScheduler sched;
  MediosSchedulerDriver driver(
      &sched, DEFAULT_FRAMEWORK_INFO, controller.get()->pid, DEFAULT_CREDENTIAL);

  Future<FrameworkID> frameworkId;
  EXPECT_CALL(sched, registered(&driver, _, _))
    .WillOnce(FutureArg<1>(&frameworkId));

  Future<vector<Offer>> offers;
  EXPECT_CALL(sched, resourceOffers(&driver, _))
    .WillOnce(FutureArg<1>(&offers));

  driver.start();

  AWAIT_READY(frameworkId);
  AWAIT_READY(offers);

  ASSERT_EQ(1u, offers->size());

  Future<v1::executor::Medios*> executorLib;
  EXPECT_CALL(*executor, connected(_))
    .WillOnce(FutureArg<0>(&executorLib));

  TaskInfo taskInfo = createTask(offers.get()[0], "", executorId);
  driver.launchTasks(offers.get()[0].id(), {taskInfo});

  // Wait for the executor to be launched before sending
  // an executor subscribe request.
  AWAIT_READY(executorLib);

  // Only subscribe needs to 'Accept' JSON or protobuf.
  Call call;
  call.mutable_framework_id()->CopyFrom(evolve(frameworkId.get()));
  call.mutable_executor_id()->CopyFrom(evolve(executorId));

  call.set_type(Call::SUBSCRIBE);

  call.mutable_subscribe();

  // Retrieve the parameter passed as content type to this test.
  const ContentType contentType = GetParam();

  // No 'Accept' header leads to all media types considered
  // acceptable. JSON will be chosen by default.
  process::http::Headers headers;

  Future<Response> response = process::http::streaming::post(
      node.get()->pid,
      "api/v1/executor",
      headers,
      serialize(contentType, call),
      stringify(contentType));

  AWAIT_EXPECT_RESPONSE_STATUS_EQ(OK().status, response);
  AWAIT_EXPECT_RESPONSE_HEADER_EQ(APPLICATION_JSON, "Content-Type", response);

  driver.stop();
  driver.join();
}


// This test sends a unsupported Accept media type for the Accept
// header. The response should be NotAcceptable in this case.
TEST_P(ExecutorHttpApiTest, NotAcceptable)
{
  Try<Owned<cluster::Controller>> controller = StartController();
  ASSERT_SOME(controller);

  Future<Nothing> __recover = FUTURE_DISPATCH(_, &Node::__recover);

  Owned<ControllerDetector> detector = controller.get()->createDetector();

  node::Flags flags = CreateNodeFlags();

  Try<Owned<cluster::Node>> node = StartNode(detector.get(), flags);
  ASSERT_SOME(node);

  AWAIT_READY(__recover);

  // Wait for recovery to be complete.
  Clock::pause();
  Clock::settle();

  // Retrieve the parameter passed as content type to this test.
  const ContentType contentType = GetParam();

  process::http::Headers headers;
  headers["Accept"] = "foo";

  // Only subscribe needs to 'Accept' JSON or protobuf.
  Call call;
  call.mutable_framework_id()->set_value("dummy_framework_id");
  call.mutable_executor_id()->set_value("dummy_executor_id");

  call.set_type(Call::SUBSCRIBE);

  call.mutable_subscribe();

  Future<Response> response = process::http::streaming::post(
      node.get()->pid,
      "api/v1/executor",
      headers,
      serialize(contentType, call),
      stringify(contentType));

  AWAIT_EXPECT_RESPONSE_STATUS_EQ(NotAcceptable().status, response);
}


TEST_P(ExecutorHttpApiTest, ValidProtobufInvalidCall)
{
  Try<Owned<cluster::Controller>> controller = StartController();
  ASSERT_SOME(controller);

  Future<Nothing> __recover = FUTURE_DISPATCH(_, &Node::__recover);

  Owned<ControllerDetector> detector = controller.get()->createDetector();

  node::Flags flags = CreateNodeFlags();

  Try<Owned<cluster::Node>> node = StartNode(detector.get(), flags);
  ASSERT_SOME(node);

  AWAIT_READY(__recover);

  // Wait for recovery to be complete.
  Clock::pause();
  Clock::settle();

  // We send a Call protobuf message with missing
  // required message per type.
  {
    Call call;
    call.set_type(Call::SUBSCRIBE);
    call.mutable_framework_id()->set_value("dummy_framework_id");
    call.mutable_executor_id()->set_value("dummy_executor_id");

    process::http::Headers headers;
    headers["Accept"] = APPLICATION_JSON;

    Future<Response> response = process::http::post(
        node.get()->pid,
        "api/v1/executor",
        headers,
        serialize(ContentType::PROTOBUF, call),
        APPLICATION_PROTOBUF);

    AWAIT_EXPECT_RESPONSE_STATUS_EQ(BadRequest().status, response);
  }

  {
    Call call;
    call.set_type(Call::UPDATE);
    call.mutable_framework_id()->set_value("dummy_framework_id");
    call.mutable_executor_id()->set_value("dummy_executor_id");

    process::http::Headers headers;
    headers["Accept"] = APPLICATION_JSON;

    Future<Response> response = process::http::post(
        node.get()->pid,
        "api/v1/executor",
        headers,
        serialize(ContentType::PROTOBUF, call),
        APPLICATION_PROTOBUF);

    AWAIT_EXPECT_RESPONSE_STATUS_EQ(BadRequest().status, response);
  }

  {
    Call call;
    call.set_type(Call::MESSAGE);
    call.mutable_framework_id()->set_value("dummy_framework_id");
    call.mutable_executor_id()->set_value("dummy_executor_id");

    process::http::Headers headers;
    headers["Accept"] = APPLICATION_JSON;

    Future<Response> response = process::http::post(
        node.get()->pid,
        "api/v1/executor",
        headers,
        serialize(ContentType::PROTOBUF, call),
        APPLICATION_PROTOBUF);

    AWAIT_EXPECT_RESPONSE_STATUS_EQ(BadRequest().status, response);
  }
}


TEST_P(ExecutorHttpApiTest, StatusUpdateCallFailedValidation)
{
  Try<Owned<cluster::Controller>> controller = StartController();
  ASSERT_SOME(controller);

  Future<Nothing> __recover = FUTURE_DISPATCH(_, &Node::__recover);

  Owned<ControllerDetector> detector = controller.get()->createDetector();

  node::Flags flags = CreateNodeFlags();

  Try<Owned<cluster::Node>> node = StartNode(detector.get(), flags);
  ASSERT_SOME(node);

  AWAIT_READY(__recover);

  // Wait for recovery to be complete.
  Clock::pause();
  Clock::settle();

  // We send a Call::Update message with inconsistent executor id between
  // Call::executor_id and Call::Update::TaskInfo::executor_id.
  // This should result in failed validation.
  {
    Call call;
    call.set_type(Call::UPDATE);
    call.mutable_framework_id()->set_value("dummy_framework_id");
    call.mutable_executor_id()->set_value("call_level_executor_id");

    v1::TaskStatus* status = call.mutable_update()->mutable_status();

    status->mutable_executor_id()->set_value("update_level_executor_id");
    status->set_state(medios::v1::TaskState::TASK_STARTING);
    status->mutable_task_id()->set_value("dummy_task_id");

    process::http::Headers headers;
    headers["Accept"] = APPLICATION_JSON;

    Future<Response> response = process::http::post(
        node.get()->pid,
        "api/v1/executor",
        headers,
        serialize(ContentType::PROTOBUF, call),
        APPLICATION_PROTOBUF);

    AWAIT_EXPECT_RESPONSE_STATUS_EQ(BadRequest().status, response);
  }

  // We send a Call Update message with an invalid UUID.
  // This should result in failed validation.
  {
    Call call;
    call.set_type(Call::UPDATE);
    call.mutable_framework_id()->set_value("dummy_framework_id");
    call.mutable_executor_id()->set_value("call_level_executor_id");

    v1::TaskStatus* status = call.mutable_update()->mutable_status();

    status->set_uuid("dummy_uuid");
    status->mutable_task_id()->set_value("dummy_task_id");
    status->set_state(medios::v1::TaskState::TASK_STARTING);
    status->set_source(medios::v1::TaskStatus::SOURCE_EXECUTOR);

    process::http::Headers headers;
    headers["Accept"] = APPLICATION_JSON;

    Future<Response> response = process::http::post(
        node.get()->pid,
        "api/v1/executor",
        headers,
        serialize(ContentType::PROTOBUF, call),
        APPLICATION_PROTOBUF);

    AWAIT_EXPECT_RESPONSE_STATUS_EQ(BadRequest().status, response);
  }

  // We send a Call Update message with a TASK_STAGING
  // status update. This should fail validation.
  {
    Call call;
    call.set_type(Call::UPDATE);
    call.mutable_framework_id()->set_value("dummy_framework_id");
    call.mutable_executor_id()->set_value("call_level_executor_id");

    v1::TaskStatus* status = call.mutable_update()->mutable_status();

    status->mutable_executor_id()->set_value("call_level_executor_id");
    status->mutable_task_id()->set_value("dummy_task_id");
    status->set_state(medios::v1::TaskState::TASK_STAGING);

    process::http::Headers headers;
    headers["Accept"] = APPLICATION_JSON;

    Future<Response> responseStatusUpdate = process::http::post(
        node.get()->pid,
        "api/v1/executor",
        headers,
        serialize(ContentType::PROTOBUF, call),
        APPLICATION_PROTOBUF);

    AWAIT_EXPECT_RESPONSE_STATUS_EQ(BadRequest().status, responseStatusUpdate);
  }

  // We send a Call Update message with a different source than
  // SOURCE_EXECUTOR in the status update. This should fail validation.
  {
    Call call;
    call.set_type(Call::UPDATE);
    call.mutable_framework_id()->set_value("dummy_framework_id");
    call.mutable_executor_id()->set_value("call_level_executor_id");

    v1::TaskStatus* status = call.mutable_update()->mutable_status();

    status->mutable_executor_id()->set_value("call_level_executor_id");
    status->mutable_task_id()->set_value("dummy_task_id");
    status->set_state(medios::v1::TaskState::TASK_STARTING);
    status->set_source(medios::v1::TaskStatus::SOURCE_CONTROLLER);

    process::http::Headers headers;
    headers["Accept"] = APPLICATION_JSON;

    Future<Response> responseStatusUpdate = process::http::post(
        node.get()->pid,
        "api/v1/executor",
        headers,
        serialize(ContentType::PROTOBUF, call),
        APPLICATION_PROTOBUF);

    AWAIT_EXPECT_RESPONSE_STATUS_EQ(BadRequest().status, responseStatusUpdate);
  }
}


// This test verifies that the executor cannot subscribe with the node
// before it recovers the containerizer.
TEST_F(ExecutorHttpApiTest, SubscribeBeforeContainerizerRecovery)
{
  Try<Owned<cluster::Controller>> controller = StartController();
  ASSERT_SOME(controller);

  MockContainerizer mockContainerizer;
  StandaloneControllerDetector detector;

  Future<Nothing> recover = FUTURE_DISPATCH(_, &Node::recover);

  Promise<Nothing> recoveryPromise;
  EXPECT_CALL(mockContainerizer, recover(_))
    .WillOnce(Return(recoveryPromise.future()));

  node::Flags flags = CreateNodeFlags();

  Try<Owned<cluster::Node>> node = StartNode(
      &detector,
      &mockContainerizer,
      flags);
  ASSERT_SOME(node);

  // Ensure that the node has atleast set up HTTP routes upon startup.
  AWAIT_READY(recover);

  // Send a subscribe call. This should fail with a '503 Service Unavailable'
  // since the node hasn't finished recovering the containerizer.

  Call call;
  call.mutable_framework_id()->CopyFrom(v1::DEFAULT_FRAMEWORK_INFO.id());
  call.mutable_executor_id()->CopyFrom(v1::DEFAULT_EXECUTOR_ID);

  call.set_type(Call::SUBSCRIBE);

  call.mutable_subscribe();

  process::http::Headers headers;
  headers["Accept"] = APPLICATION_JSON;

  Future<Response> response = process::http::post(
        node.get()->pid,
        "api/v1/executor",
        headers,
        serialize(ContentType::JSON, call),
        APPLICATION_JSON);

  AWAIT_EXPECT_RESPONSE_STATUS_EQ(ServiceUnavailable().status, response);

  // The destructor of `cluster::Node` will try to clean up any
  // remaining containers by inspecting the result of `containers()`.
  EXPECT_CALL(mockContainerizer, containers())
    .WillRepeatedly(Return(hashset<ContainerID>()));
}


// This test verifies if the executor is able to receive a Subscribed
// event in response to a Subscribe call request.
TEST_P(ExecutorHttpApiTest, Subscribe)
{
  Try<Owned<cluster::Controller>> controller = StartController();
  ASSERT_SOME(controller);

  ExecutorID executorId = DEFAULT_EXECUTOR_ID;
  MockExecutor exec(executorId);
  TestContainerizer containerizer(&exec);

  Owned<ControllerDetector> detector = controller.get()->createDetector();

  node::Flags flags = CreateNodeFlags();

  Try<Owned<cluster::Node>> node = StartNode(
      detector.get(),
      &containerizer,
      flags);
  ASSERT_SOME(node);

  MockScheduler sched;
  MediosSchedulerDriver driver(
      &sched, DEFAULT_FRAMEWORK_INFO, controller.get()->pid, DEFAULT_CREDENTIAL);

  Future<FrameworkID> frameworkId;
  EXPECT_CALL(sched, registered(&driver, _, _))
    .WillOnce(FutureArg<1>(&frameworkId));

  Future<vector<Offer>> offers;
  EXPECT_CALL(sched, resourceOffers(&driver, _))
    .WillOnce(FutureArg<1>(&offers));

  driver.start();

  AWAIT_READY(frameworkId);
  AWAIT_READY(offers);

  ASSERT_EQ(1u, offers->size());

  Future<Message> registerExecutorMessage =
    DROP_MESSAGE(Eq(RegisterExecutorMessage().GetTypeName()), _, _);

  TaskInfo taskInfo = createTask(offers.get()[0], "", executorId);
  driver.launchTasks(offers.get()[0].id(), {taskInfo});

  // Drop the `RegisterExecutorMessage` and then send a `Subscribe` request
  // from the HTTP based executor.
  AWAIT_READY(registerExecutorMessage);

  Call call;
  call.mutable_framework_id()->CopyFrom(evolve(frameworkId.get()));
  call.mutable_executor_id()->CopyFrom(evolve(executorId));

  call.set_type(Call::SUBSCRIBE);

  call.mutable_subscribe();

  // Retrieve the parameter passed as content type to this test.
  const ContentType contentType = GetParam();
  const string contentTypeString = stringify(contentType);

  process::http::Headers headers;
  headers["Accept"] = contentTypeString;

  Future<Response> response = process::http::streaming::post(
      node.get()->pid,
      "api/v1/executor",
      headers,
      serialize(contentType, call),
      contentTypeString);

  AWAIT_EXPECT_RESPONSE_STATUS_EQ(OK().status, response);
  AWAIT_EXPECT_RESPONSE_HEADER_EQ("chunked", "Transfer-Encoding", response);
  AWAIT_EXPECT_RESPONSE_HEADER_EQ(contentTypeString, "Content-Type", response);

  ASSERT_EQ(Response::PIPE, response->type);

  Option<Pipe::Reader> reader = response->reader;
  ASSERT_SOME(reader);

  Reader<Event> responseDecoder(
      lambda::bind(deserialize<Event>, contentType, lambda::_1),
      reader.get());

  Future<Result<Event>> event = responseDecoder.read();
  AWAIT_READY(event);
  ASSERT_SOME(event.get());

  // Check event type is subscribed and if the ExecutorID matches.
  ASSERT_EQ(Event::SUBSCRIBED, event->get().type());
  ASSERT_EQ(event->get().subscribed().executor_info().executor_id(),
            call.executor_id());
  ASSERT_TRUE(event->get().subscribed().has_container_id());

  reader->close();

  driver.stop();
  driver.join();
}


// This test verifies that heartbeats are sent from the node to the executor.
TEST_P(ExecutorHttpApiTest, HeartbeatEvents)
{
  Clock::pause();

  const ContentType contentType = GetParam();
  const string contentTypeString = stringify(contentType);

  Resources resources = Resources::parse("cpus:0.1;mem:32;disk:32").get();

  controller::Flags controllerFlags = CreateControllerFlags();
  Try<Owned<cluster::Controller>> controller = StartController(controllerFlags);
  ASSERT_SOME(controller);

  ExecutorID executorId = DEFAULT_EXECUTOR_ID;
  MockExecutor exec(executorId);
  TestContainerizer containerizer(&exec);

  Owned<ControllerDetector> detector = controller.get()->createDetector();

  node::Flags nodeFlags = CreateNodeFlags();
  nodeFlags.registration_backoff_factor = Seconds(0);

  Try<Owned<cluster::Node>> node = StartNode(
      detector.get(),
      &containerizer,
      nodeFlags);
  ASSERT_SOME(node);

  MockScheduler sched;
  MediosSchedulerDriver driver(
      &sched, DEFAULT_FRAMEWORK_INFO, controller.get()->pid, DEFAULT_CREDENTIAL);

  Future<FrameworkID> frameworkId;
  EXPECT_CALL(sched, registered(&driver, _, _))
    .WillOnce(FutureArg<1>(&frameworkId));

  Future<vector<Offer>> offers;
  EXPECT_CALL(sched, resourceOffers(&driver, _))
    .WillOnce(FutureArg<1>(&offers));

  driver.start();

  AWAIT_READY(frameworkId);

  AWAIT_READY(offers);
  ASSERT_EQ(1u, offers->size());

  // Drop the `RegisterExecutorMessage` so the test body can
  // pretend to be the executor.
  Future<Message> registerExecutorMessage =
    DROP_MESSAGE(Eq(RegisterExecutorMessage().GetTypeName()), _, _);

  TaskInfo taskInfo1 = createTask(
      offers.get()[0].node_id(), resources, "", executorId);
  driver.launchTasks(offers.get()[0].id(), {taskInfo1});

  AWAIT_READY(registerExecutorMessage);

  // The test body will pretend to be the executor, which gives us
  // straightforward access to any events that are sent to the executor.
  Call call;
  call.mutable_framework_id()->CopyFrom(evolve(frameworkId.get()));
  call.mutable_executor_id()->CopyFrom(evolve(executorId));

  call.set_type(Call::SUBSCRIBE);
  call.mutable_subscribe();

  process::http::Headers headers;
  headers["Accept"] = contentTypeString;

  Future<Response> response = process::http::streaming::post(
      node.get()->pid,
      "api/v1/executor",
      headers,
      serialize(contentType, call),
      contentTypeString);

  AWAIT_EXPECT_RESPONSE_STATUS_EQ(OK().status, response);
  AWAIT_EXPECT_RESPONSE_HEADER_EQ("chunked", "Transfer-Encoding", response);
  AWAIT_EXPECT_RESPONSE_HEADER_EQ(
      contentTypeString, "Content-Type", response);

  ASSERT_EQ(Response::PIPE, response->type);
  Option<Pipe::Reader> reader = response->reader;
  ASSERT_SOME(reader);

  Reader<Event> responseDecoder(
      lambda::bind(deserialize<Event>, contentType, lambda::_1),
      reader.get());

  Future<Result<Event>> event = responseDecoder.read();
  AWAIT_READY(event);
  ASSERT_SOME(event.get());

  // Check event type is subscribed and if the ExecutorID matches.
  ASSERT_EQ(Event::SUBSCRIBED, event->get().type());
  ASSERT_EQ(event->get().subscribed().executor_info().executor_id(),
            call.executor_id());
  ASSERT_TRUE(event->get().subscribed().has_container_id());

  // Wait for the node to send the first task, so we don't race against
  // this later on. This (pretend) executor drops the task though.
  event = responseDecoder.read();
  AWAIT_READY(event);
  ASSERT_SOME(event.get());
  ASSERT_EQ(Event::LAUNCH, event->get().type());

  // The next event should be the heartbeat.
  event = responseDecoder.read();
  ASSERT_TRUE(event.isPending());

  Clock::advance(node::DEFAULT_EXECUTOR_HEARTBEAT_INTERVAL);
  AWAIT_READY(event);
  ASSERT_SOME(event.get());
  ASSERT_EQ(Event::HEARTBEAT, event->get().type());

  EXPECT_CALL(exec, shutdown(_))
    .Times(AtMost(1));

  driver.stop();
  driver.join();

  Clock::resume();
}


// Mock node for intercepting messages from HTTP executors.
class BareBonesNodeProcess : public process::Process<BareBonesNodeProcess>
{
public:
  BareBonesNodeProcess()
    : process::ProcessBase(process::ID::generate("bare-bones-node")) {}

  MOCK_METHOD1(
      executor,
      Future<process::http::Response>(const process::http::Request&));

protected:
  void initialize() override
  {
    route(
        "/api/v1/executor",
        None(),
        &BareBonesNodeProcess::executor);
  }
};


class BareBonesNode
{
public:
  BareBonesNode() : process(new BareBonesNodeProcess())
  {
    process::spawn(process.get());
  }

  ~BareBonesNode()
  {
    process::terminate(process.get());
    process::wait(process.get());
  }

  Owned<BareBonesNodeProcess> process;
};


// This test verifies that heartbeats are sent from the executor driver
// to the node.
TEST_F(ExecutorHttpApiTest, HeartbeatCalls)
{
  Clock::pause();

  // Launch an HTTP server that pretends to be the node in this test.
  BareBonesNode node;

  // Pre-fill a response pipe with a SUBSCRIBED response.
  process::http::Pipe pipe;

  {
    v1::executor::Event event;
    event.set_type(v1::executor::Event::SUBSCRIBED);

    v1::ExecutorInfo* executorInfo =
      event.mutable_subscribed()->mutable_executor_info();
    executorInfo->set_type(v1::ExecutorInfo::DEFAULT);
    executorInfo->mutable_executor_id()->set_value("empty");

    v1::FrameworkInfo* frameworkInfo =
      event.mutable_subscribed()->mutable_framework_info();
    frameworkInfo->mutable_id()->set_value("fake");
    frameworkInfo->set_user("whoever");
    frameworkInfo->set_name("anonymous");

    event.mutable_subscribed()->mutable_node_info()->set_hostname(":P");

    event.mutable_subscribed()->mutable_container_id()->set_value(":P");

    pipe.writer().write(
        ::recordio::encode(serialize(ContentType::PROTOBUF, event)));
  }

  // Set the expectation for an executor to register with the fake node.
  process::http::Response subscribed = process::http::OK();
  subscribed.type = process::http::Response::PIPE;
  subscribed.reader = pipe.reader();

  Future<process::http::Request> expectedSubscribe;
  Promise<Nothing> eventSubscribed;

  EXPECT_CALL(*node.process, executor(_))
    .WillOnce(DoAll(
        FutureArg<0>(&expectedSubscribe),
        Return(subscribed)));

  // Start the executor driver.
  // NOTE: We use an Owned pointer here because the lambdas passed to the
  // executor driver's constructor reference the executor driver itself,
  // which is not allowed on some compilers if the driver is stack allocated.
  Owned<medios::v1::executor::Medios> executor;

  // Since this test only cares about the calls sent to the node,
  // any events and callbacks fired by this driver are used purely for
  // synchronization purposes.
  executor.reset(new medios::v1::executor::Medios(
      ContentType::PROTOBUF,
      [&executor]() mutable {
        v1::executor::Call v1Call;
        v1Call.set_type(v1::executor::Call::SUBSCRIBE);
        v1Call.mutable_executor_id()->set_value("empty");
        v1Call.mutable_framework_id()->set_value("fake");
        v1Call.mutable_subscribe();

        executor->send(v1Call);
      },
      []() {},
      [&eventSubscribed](std::queue<v1::executor::Event> queue) {
        while(!queue.empty()) {
          const v1::executor::Event& event = queue.front();
          if (event.type() == v1::executor::Event::SUBSCRIBED) {
            eventSubscribed.set(Nothing());
          }
          queue.pop();
        }
      },
      {
        { "MEDIOS_NODE_PID",  stringify(node.process->self()) },
        { "MEDIOS_EXECUTOR_SHUTDOWN_GRACE_PERIOD",
          stringify(node::DEFAULT_EXECUTOR_SHUTDOWN_GRACE_PERIOD) }
      }));

  // Wait for the call to arrive at our fake node.
  AWAIT_READY(expectedSubscribe);

  Option<string> contentType = expectedSubscribe->headers.get("Content-Type");
  ASSERT_SOME(contentType);
  ASSERT_EQ(APPLICATION_PROTOBUF, contentType.get());

  {
    v1::executor::Call v1Call;
    ASSERT_TRUE(v1Call.ParseFromString(expectedSubscribe->body));

    ASSERT_EQ(v1::executor::Call::SUBSCRIBE, v1Call.type());
  }

  // Wait for the executor to receive the response from our fake node.
  AWAIT_READY(eventSubscribed.future());

  // Advance time and expect to get a heartbeat.
  Future<process::http::Request> expectedHeartbeat;

  EXPECT_CALL(*node.process, executor(_))
    .WillOnce(DoAll(
        FutureArg<0>(&expectedHeartbeat),
        Return(process::http::Accepted())));

  Clock::advance(medios::v1::executor::DEFAULT_HEARTBEAT_CALL_INTERVAL);
  AWAIT_READY(expectedHeartbeat);

  contentType = expectedHeartbeat->headers.get("Content-Type");
  ASSERT_SOME(contentType);
  ASSERT_EQ(APPLICATION_PROTOBUF, contentType.get());

  {
    v1::executor::Call v1Call;
    ASSERT_TRUE(v1Call.ParseFromString(expectedHeartbeat->body));

    ASSERT_EQ(v1::executor::Call::HEARTBEAT, v1Call.type());
  }

  Clock::resume();
}


// A subscribing executor replays the updates it has not had acknowledged.
// The node has to decide those the same way it decides a live report: an
// executor may report only on tasks it owns, and the subscribed connection
// is what names the sender. This replays an update for a task id the
// executor was never given, which must be refused and counted.
TEST_F(ExecutorHttpApiTest, SubscribeReplayForAnUnownedTaskIsRefused)
{
  Try<Owned<cluster::Controller>> controller = StartController();
  ASSERT_SOME(controller);

  ExecutorID executorId = DEFAULT_EXECUTOR_ID;
  MockExecutor exec(executorId);
  TestContainerizer containerizer(&exec);

  Owned<ControllerDetector> detector = controller.get()->createDetector();

  node::Flags flags = CreateNodeFlags();

  Try<Owned<cluster::Node>> node = StartNode(
      detector.get(),
      &containerizer,
      flags);
  ASSERT_SOME(node);

  MockScheduler sched;
  MediosSchedulerDriver driver(
      &sched, DEFAULT_FRAMEWORK_INFO, controller.get()->pid, DEFAULT_CREDENTIAL);

  Future<FrameworkID> frameworkId;
  EXPECT_CALL(sched, registered(&driver, _, _))
    .WillOnce(FutureArg<1>(&frameworkId));

  Future<vector<Offer>> offers;
  EXPECT_CALL(sched, resourceOffers(&driver, _))
    .WillOnce(FutureArg<1>(&offers));

  driver.start();

  AWAIT_READY(frameworkId);
  AWAIT_READY(offers);

  ASSERT_EQ(1u, offers->size());

  Future<Message> registerExecutorMessage =
    DROP_MESSAGE(Eq(RegisterExecutorMessage().GetTypeName()), _, _);

  TaskInfo taskInfo = createTask(offers.get()[0], "", executorId);
  driver.launchTasks(offers.get()[0].id(), {taskInfo});

  AWAIT_READY(registerExecutorMessage);

  Call call;
  call.mutable_framework_id()->CopyFrom(evolve(frameworkId.get()));
  call.mutable_executor_id()->CopyFrom(evolve(executorId));

  call.set_type(Call::SUBSCRIBE);

  Call::Update* replay =
    call.mutable_subscribe()->add_unacknowledged_updates();

  v1::TaskStatus* status = replay->mutable_status();
  status->mutable_task_id()->set_value("never-launched");
  status->set_state(v1::TASK_RUNNING);
  status->set_uuid(id::UUID::random().toBytes());
  status->mutable_progress()->set_fraction(0.5);

  const ContentType contentType = ContentType::PROTOBUF;
  const string contentTypeString = stringify(contentType);

  process::http::Headers headers;
  headers["Accept"] = contentTypeString;

  Future<Response> response = process::http::streaming::post(
      node.get()->pid,
      "api/v1/executor",
      headers,
      serialize(contentType, call),
      contentTypeString);

  AWAIT_EXPECT_RESPONSE_STATUS_EQ(OK().status, response);
  ASSERT_EQ(Response::PIPE, response->type);

  Option<Pipe::Reader> reader = response->reader;
  ASSERT_SOME(reader);

  Reader<Event> responseDecoder(
      lambda::bind(deserialize<Event>, contentType, lambda::_1),
      reader.get());

  // The subscribe is answered first, as it must be: an executor library is
  // told it is subscribed before it is told anything else.
  Future<Result<Event>> event = responseDecoder.read();
  AWAIT_READY(event);
  ASSERT_SOME(event.get());
  ASSERT_EQ(Event::SUBSCRIBED, event->get().type());

  // Then the refused update is acknowledged, so the executor stops replaying
  // it rather than carrying it into every later subscription.
  event = responseDecoder.read();
  AWAIT_READY(event);
  ASSERT_SOME(event.get());
  ASSERT_EQ(Event::ACKNOWLEDGED, event->get().type());
  EXPECT_EQ("never-launched", event->get().acknowledged().task_id().value());

  EXPECT_TRUE(metricEquals("node/unowned_task_status_updates", 1));

  reader->close();

  EXPECT_CALL(exec, shutdown(_))
    .Times(AtMost(1));

  driver.stop();
  driver.join();
}

} // namespace tests {
} // namespace internal {
} // namespace medios {
