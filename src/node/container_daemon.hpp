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

#ifndef __NODE_CONTAINER_DAEMON_HPP__
#define __NODE_CONTAINER_DAEMON_HPP__

#include <functional>

#include <medios/medios.hpp>
#include <medios/resources.hpp>

#include <process/future.hpp>
#include <process/http.hpp>
#include <process/owned.hpp>

#include <stout/duration.hpp>
#include <stout/option.hpp>
#include <stout/try.hpp>

namespace medios {
namespace internal {
namespace node {

// Forward declarations.
class ContainerDaemonProcess;


// A daemon that launches and monitors a service running in a standalone
// container, and automatically restarts the container when it exits.
//
// NOTE: The `ContainerDaemon` itself is not responsible to manage the
// lifecycle of the service container it monitors.
class ContainerDaemon
{
public:
  // Creates a container daemon that idempotently launches the container
  // and then run the `postStartHook` function. Upon container exits, it
  // executes the `postStopHook` function, then restarts the launch
  // cycle again. Any failed or discarded future returned by the hook
  // functions will be reflected by the `wait()` method.
  static Try<process::Owned<ContainerDaemon>> create(
      const process::http::URL& nodeUrl,
      const Option<std::string>& authToken,
      const ContainerID& containerId,
      const Option<CommandInfo>& commandInfo,
      const Option<Resources>& resources,
      const Option<ContainerInfo>& containerInfo,
      const Option<std::function<process::Future<Nothing>()>>& postStartHook =
        None(),
      const Option<std::function<process::Future<Nothing>()>>& postStopHook =
        None());

  ~ContainerDaemon();

  // Returns a future that only reaches a terminal state when a failure
  // or a discarded future occurs during the launch cycle. This is
  // intended to capture any loop-breaking error, and the caller should
  // reconstruct a new daemon instance if they want to retry.
  process::Future<Nothing> wait();

private:
  explicit ContainerDaemon(
      const process::http::URL& nodeUrl,
      const Option<std::string>& authToken,
      const ContainerID& containerId,
      const Option<CommandInfo>& commandInfo,
      const Option<Resources>& resources,
      const Option<ContainerInfo>& containerInfo,
      const Option<std::function<process::Future<Nothing>()>>& postStartHook,
      const Option<std::function<process::Future<Nothing>()>>& postStopHook);

  process::Owned<ContainerDaemonProcess> process;
};

} // namespace node {
} // namespace internal {
} // namespace medios {

#endif // __NODE_CONTAINER_DAEMON_HPP__
