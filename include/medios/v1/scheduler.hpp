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

#ifndef __MEDIOS_V1_SCHEDULER_HPP__
#define __MEDIOS_V1_SCHEDULER_HPP__

#include <functional>
#include <memory>
#include <queue>
#include <string>

#include <medios/http.hpp>

#include <medios/v1/medios.hpp>

#include <medios/v1/scheduler/scheduler.hpp>

#include <process/future.hpp>

#include <stout/option.hpp>

namespace medios {

namespace controller {
namespace detector {
class ControllerDetector;
} // namespace detector {
} // namespace controller {

namespace v1 {
namespace scheduler {

class MediosProcess; // Forward declaration.

// Abstract interface for connecting a scheduler to Medios.
class MediosBase
{
public:
  // Empty virtual destructor (necessary to instantiate subclasses).
  virtual ~MediosBase() {}
  virtual void send(const Call& call) = 0;
  virtual process::Future<APIResult> call(const Call& callMessage) = 0;
  virtual void reconnect() = 0;
};


// Concrete implementation that connects a scheduler to a Medios controller.
// Abstracts controller detection (connection and disconnection).
//
// Expects three callbacks, 'connected', 'disconnected', and
// 'received' which will get invoked _serially_ when it's determined
// that we've connected (i.e., detected controller), disconnected
// (i.e, detected no controller), or received events from the controller.
// The library reconnects with the controller upon a disconnection.
//
// NOTE: All calls and events are dropped while disconnected.
class Medios : public MediosBase
{
public:
  // The credential will be used for authenticating with the controller. Currently,
  // only HTTP basic authentication is supported.
  Medios(const std::string& controller,
        ContentType contentType,
        const std::function<void()>& connected,
        const std::function<void()>& disconnected,
        const std::function<void(const std::queue<Event>&)>& received,
        const Option<Credential>& credential);

  // Delete copy constructor.
  Medios(const Medios& other) = delete;

  // Delete assignment operator.
  Medios& operator=(const Medios& other) = delete;

  ~Medios() override;

  // Attempts to send a call to the controller.
  //
  // The scheduler should only invoke this method once it has received the
  // 'connected' callback. Otherwise, all calls would be dropped while
  // disconnected.
  //
  // Some local validation of calls is performed which may generate
  // events without ever being sent to the controller. This includes when
  // calls are sent but no controller is currently detected (i.e., we're
  // disconnected).
  void send(const Call& call) override;

  // Attempts to send a call to the controller, returning the response.
  //
  // The scheduler should only invoke this method once it has received the
  // 'connected' callback. Otherwise, a `Failure` will be returned.
  //
  // Some local validation of calls is performed, and the request will not be
  // sent to the controller if the validation fails.
  //
  // A `Failure` will be returned on validation failures or if an error happens
  // when sending the request to the controller, e.g., a controller disconnection, or a
  // deserialization error.
  //
  // If it was possible to receive a response from the server, the returned
  // object will contain the HTTP response status code.
  //
  // There are three cases to consider depending on the HTTP response status
  // code:
  //
  //  (1) '202 ACCEPTED': Indicates the call was accepted for processing and
  //      neither `APIResult::response` nor `APIResult::error` will be set.
  //
  //  (2) '200 OK': Indicates the call completed successfully.
  //      `APIResult::response` will be set if the `scheduler::Call::Type`
  //      has a corresponding `scheduler::Response::Type`, `APIResult::error`
  //      will not be set.
  //
  //  (3) For all other HTTP status codes, the `APIResult::response` field will
  //      not be set and the `APIResult::error` field may be set to provide more
  //      information.
  //
  // Note: This method cannot be used to send `SUBSCRIBE` calls, use `send()`
  // instead.
  process::Future<APIResult> call(const Call& callMessage) override;

  // Force a reconnection with the controller.
  //
  // In the case of a one-way network partition, the connection between the
  // scheduler and controller might not necessarily break. If the scheduler detects
  // a partition, due to lack of `HEARTBEAT` events (e.g., 5) within a time
  // window, it can explicitly ask the library to force a reconnection with
  // the controller.
  //
  // This call would be ignored if the scheduler is already disconnected with
  // the controller (e.g., no new controller has been elected). Otherwise, the scheduler
  // would get a 'disconnected' callback followed by a 'connected' callback.
  void reconnect() override;

protected:
  // NOTE: This constructor is used for testing.
  Medios(
      const std::string& controller,
      ContentType contentType,
      const std::function<void()>& connected,
      const std::function<void()>& disconnected,
      const std::function<void(const std::queue<Event>&)>& received,
      const Option<Credential>& credential,
      const Option<std::shared_ptr<medios::controller::detector::ControllerDetector>>&
        detector);

  // Stops the library so that:
  //   - No more calls can be sent to the controller.
  //   - No more callbacks can be made to the scheduler. In some cases, there
  //     may be one additional callback if the library was in the middle of
  //     processing an event.
  //
  // NOTE: This is used for testing.
  virtual void stop();

private:
  MediosProcess* process;
};

} // namespace scheduler {
} // namespace v1 {
} // namespace medios {

#endif // __MEDIOS_V1_SCHEDULER_HPP__
