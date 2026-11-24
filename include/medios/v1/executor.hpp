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

#ifndef __MEDIOS_V1_EXECUTOR_HPP__
#define __MEDIOS_V1_EXECUTOR_HPP__

#include <functional>
#include <queue>
#include <map>
#include <string>

#include <medios/http.hpp>

#include <medios/v1/medios.hpp>

#include <medios/v1/executor/executor.hpp>

#include <process/owned.hpp>

namespace medios {
namespace v1 {
namespace executor {

class MediosProcess; // Forward declaration.

class MediosBase
{
public:
  // Empty virtual destructor (necessary to instantiate subclasses).
  virtual ~MediosBase() {}
  virtual void send(const Call& call) = 0;
};


// Interface to Medios for an executor.
//
// Expects three callbacks, 'connected', 'disconnected', and
// 'received' which will get invoked _serially_ when it's determined
// that we've connected (i.e. established TCP connection), disconnected
// (i.e, connection is broken), or received events from the node.
// Note that we drop events while disconnected.
class Medios : public MediosBase
{
public:
  // The other constructor overload that accepts `environment`
  // argument is preferable to this one in a multithreaded environment,
  // because the implementation of this one accesses global environment
  // which is unsafe due to a potential concurrent modification of the
  // environment by another thread.
  Medios(ContentType contentType,
        const std::function<void(void)>& connected,
        const std::function<void(void)>& disconnected,
        const std::function<void(const std::queue<Event>&)>& received);

  Medios(ContentType contentType,
        const std::function<void(void)>& connected,
        const std::function<void(void)>& disconnected,
        const std::function<void(const std::queue<Event>&)>& received,
        const std::map<std::string, std::string>& environment);

  // Delete copy constructor.
  Medios(const Medios& other) = delete;

  // Delete assignment operator.
  Medios& operator=(const Medios& other) = delete;

  ~Medios() override;

  // Attempts to send a call to the node.
  //
  // Some local validation of calls is performed which may result in dropped
  // events without ever being sent to the node.
  void send(const Call& call) override;

private:
  process::Owned<MediosProcess> process;
};

} // namespace executor {
} // namespace v1 {
} // namespace medios {

#endif // __MEDIOS_V1_EXECUTOR_HPP__
