// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with *this work for additional information
// regarding copyright ownership.  The ASF licenses *this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use *this file except in compliance
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

#include <process/loop.hpp>

#include "tests/medios.hpp"

#include "tests/controller/mock_controller_api_subscriber.hpp"

using medios::v1::controller::Call;
using medios::v1::controller::Event;

using medios::internal::controller::Controller;
using medios::internal::recordio::Reader;

using process::Future;
using process::Failure;
using process::Promise;

using testing::_;
using testing::Return;

namespace medios {
namespace internal {
namespace tests {
namespace v1 {

class MockControllerAPISubscriberProcess
  : public process::Process<MockControllerAPISubscriberProcess>
{
public:
  MockControllerAPISubscriberProcess(MockControllerAPISubscriber* subscriber_)
    : subscriber(subscriber_) {};

  Future<Nothing> subscribe(
    const process::PID<Controller>& controllerPid, ContentType contentType)
  {
    Call call;
    call.set_type(Call::SUBSCRIBE);

    process::http::Headers headers = createBasicAuthHeaders(DEFAULT_CREDENTIAL);
    headers["Accept"] = stringify(contentType);

    return process::http::streaming::post(
        controllerPid,
        "api/v1",
        headers,
        serialize(contentType, call),
        stringify(contentType))
      .then(defer(self(), &Self::_subscribe, lambda::_1, contentType));
  }

  Future<Nothing> getReceiveLoopResult()
  {
    return receiveLoopPromise.future();
  }

protected:
  void finalize() override
  {
    receiveLoop.discard();
  }

private:
  Future<Nothing> _subscribe(
    const process::Future<process::http::Response>& response,
    ContentType contentType)
  {
    if (response.isFailed()) {
      return Failure(
        "Failed to subscribe to controller API events: " + response.failure());
    }

    if (process::http::OK().status != response->status) {
      return Failure(
        "SUBSCRIBE call returned bad HTTP status code: " +
        stringify(response->status));
    }

    if (response->type != process::http::Response::PIPE) {
      return Failure("Response type is not PIPE");
    }

    if (response->reader.isNone()) {
      return Failure("Response reader is set to None");
    }

    auto deserializer = lambda::bind(
        deserialize<Event>, contentType, lambda::_1);

    std::unique_ptr<Reader<Event>> reader(new Reader<Event>(
        deserializer, response->reader.get()));

    auto decode = lambda::bind(
        [](std::unique_ptr<Reader<Event>>& d) { return d->read(); },
        std::move(reader));

    receiveLoop = process::loop(
        self(),
        std::move(decode),
        [this](const Result<Event>& event) -> process::ControlFlow<Nothing> {
          if (event.isError()) {
            // medios change (#133): upstream calls LOG(FATAL) here, which
            // aborts the test binary on a single bad event and prevents
            // diagnosing other failures. Surface the error as observable
            // Future state instead so tests can AWAIT_FAILED the receive
            // loop and the rest of the binary keeps running.
            const std::string error =
                "Failed to read controller streaming API event: " +
                event.error();
            LOG(ERROR) << error;
            receiveLoopPromise.fail(error);
            return process::Break();
          }

          if (event.isNone()) {
            LOG(INFO) << "Received EOF from controller streaming API";
            receiveLoopPromise.set(Nothing());
            return process::Break();
          }

          LOG(INFO) << "Received " << event->type()
                    << " event from controller streaming API";

          subscriber->handleEvent(event.get());
          return process::Continue();
        });

    LOG(INFO) << "Subscribed to controller streaming API events";

    receiveLoop.onAny([]() {
      LOG(INFO) << "Stopped controller streaming API receive loop";
    });

    return Nothing();
  }

  MockControllerAPISubscriber* subscriber;
  Future<Nothing> receiveLoop;
  Promise<Nothing> receiveLoopPromise;
};


MockControllerAPISubscriber::MockControllerAPISubscriber()
{
  EXPECT_CALL(*this, subscribed(_))
    .WillRepeatedly(Return());

  EXPECT_CALL(*this, taskAdded(_))
    .WillRepeatedly(Return());

  EXPECT_CALL(*this, taskUpdated(_))
    .WillRepeatedly(Return());

  EXPECT_CALL(*this, nodeAdded(_))
    .WillRepeatedly(Return());

  EXPECT_CALL(*this, nodeRemoved(_))
    .WillRepeatedly(Return());

  EXPECT_CALL(*this, frameworkAdded(_))
    .WillRepeatedly(Return());

  EXPECT_CALL(*this, frameworkUpdated(_))
    .WillRepeatedly(Return());

  EXPECT_CALL(*this, frameworkRemoved(_))
    .WillRepeatedly(Return());

  EXPECT_CALL(*this, heartbeat())
    .WillRepeatedly(Return());

  subscribeCalled = false;

  pid = spawn(new MockControllerAPISubscriberProcess(this), true);
}


MockControllerAPISubscriber::~MockControllerAPISubscriber()
{
  process::terminate(pid);

  // The process holds a pointer to this object, and so
  // we must ensure it won't access the pointer before
  // we exit the destructor.
  //
  // TODO(asekretenko): Figure out a way to avoid blocking.
  process::wait(pid);
}


Future<Nothing> MockControllerAPISubscriber::subscribe(
    const process::PID<Controller>& controllerPid,
    ContentType contentType)
{
  if (subscribeCalled) {
    return Failure(
      "MockControllerAPISubscriber::subscribe should be called at most once");
  }

  subscribeCalled = true;

  return dispatch(
      pid,
      &MockControllerAPISubscriberProcess::subscribe,
      controllerPid,
      contentType);
}


Future<Nothing> MockControllerAPISubscriber::receiveLoopResult()
{
  return dispatch(pid, &MockControllerAPISubscriberProcess::getReceiveLoopResult);
}


void MockControllerAPISubscriber::handleEvent(const Event& event)
{
  switch (event.type()) {
    case Event::SUBSCRIBED:
      subscribed(event.subscribed());
      break;
    case Event::TASK_ADDED:
      taskAdded(event.task_added());
      break;
    case Event::TASK_UPDATED:
      taskUpdated(event.task_updated());
      break;
    case Event::NODE_ADDED:
      nodeAdded(event.node_added());
      break;
    case Event::NODE_REMOVED:
      nodeRemoved(event.node_removed());
      break;
    case Event::FRAMEWORK_ADDED:
      frameworkAdded(event.framework_added());
      break;
    case Event::FRAMEWORK_UPDATED:
      frameworkUpdated(event.framework_updated());
      break;
    case Event::FRAMEWORK_REMOVED:
      frameworkRemoved(event.framework_removed());
      break;
    case Event::HEARTBEAT:
      heartbeat();
      break;
    case Event::UNKNOWN:
      LOG(FATAL) << "Received event of a type UNKNOWN";
  }
}


} // namespace v1 {
} // namespace tests {
} // namespace internal {
} // namespace medios {
