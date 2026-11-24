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

#ifndef __TESTS_CONTROLLER_MOCK_API_SUBSCRIBER__
#define __TESTS_CONTROLLER_MOCK_API_SUBSCRIBER__

#include <memory>

#include <gmock/gmock.h>

#include <process/future.hpp>
#include <process/pid.hpp>

#include <medios/v1/controller/controller.hpp>
#include <medios/http.hpp>

#include "controller/controller.hpp"

namespace medios {
namespace internal {
namespace tests {
namespace v1 {

class MockControllerAPISubscriberProcess;

// This class performs subscribing to controller's V1 API events
// and provides mock methods for setting expectations for these events
// similarly to MockHTTPScheduler.
class MockControllerAPISubscriber
{
public:
  // Mock methods which are called by this class when an event is received.
  MOCK_METHOD1(subscribed, void(const ::medios::v1::controller::Event::Subscribed&));

  MOCK_METHOD1(taskAdded, void(const ::medios::v1::controller::Event::TaskAdded&));

  MOCK_METHOD1(taskUpdated,
               void(const ::medios::v1::controller::Event::TaskUpdated&));

  MOCK_METHOD1(nodeAdded,
               void(const ::medios::v1::controller::Event::NodeAdded&));

  MOCK_METHOD1(nodeRemoved,
               void(const ::medios::v1::controller::Event::NodeRemoved&));

  MOCK_METHOD1(frameworkAdded,
               void(const ::medios::v1::controller::Event::FrameworkAdded&));

  MOCK_METHOD1(frameworkUpdated,
               void(const ::medios::v1::controller::Event::FrameworkUpdated&));

  MOCK_METHOD1(frameworkRemoved,
               void(const ::medios::v1::controller::Event::FrameworkRemoved&));

  MOCK_METHOD0(heartbeat, void());

  MockControllerAPISubscriber();
  virtual ~MockControllerAPISubscriber();

  // Subscribes to the controller's V1 API event stream.
  // The returned Future becomes ready after the SUBSCRIBE call returns a
  // 200 OK response.
  //
  // NOTE: This method should be called at most once per the lifetime of the
  // mock.
  //
  // NOTE: All expectations on the mock methods should be set before calling
  // this method.
  process::Future<Nothing> subscribe(
    const process::PID<medios::internal::controller::Controller>& controllerPid,
    ContentType contentType = ContentType::PROTOBUF);

  // Future tracking the receive loop. Becomes ready on graceful EOF, or
  // fails with the deserialization error message if the controller emits an
  // event the helper cannot decode. Tests can AWAIT_FAILED this to assert
  // strict protocol conformance, or ignore it to let event-driven
  // EXPECT_CALL expectations time out naturally on the per-test deadline.
  //
  // Replaces upstream LOG(FATAL)-on-decode-error so a single bad event no
  // longer aborts the entire test binary (#133).
  process::Future<Nothing> receiveLoopResult();

private:
  friend class MockControllerAPISubscriberProcess;
  void handleEvent(const ::medios::v1::controller::Event& event);

  bool subscribeCalled;
  process::PID<MockControllerAPISubscriberProcess> pid;
};


} // namespace v1 {
} // namespace tests {
} // namespace internal {
} // namespace medios {

#endif //  __TESTS_CONTROLLER_MOCK_API_SUBSCRIBER__
