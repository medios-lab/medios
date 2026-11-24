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

#ifndef __NODE_CONTAINER_DAEMON_PROCESS_HPP__
#define __NODE_CONTAINER_DAEMON_PROCESS_HPP__

#include <functional>
#include <string>

#include <medios/medios.hpp>
#include <medios/node/node.hpp>

#include <process/future.hpp>
#include <process/http.hpp>
#include <process/process.hpp>

#include <stout/option.hpp>

#include "common/http.hpp"

namespace medios {
namespace internal {
namespace node {

class ContainerDaemonProcess : public process::Process<ContainerDaemonProcess>
{
public:
  explicit ContainerDaemonProcess(
      const process::http::URL& _nodeUrl,
      const Option<std::string>& _authToken,
      const ContainerID& containerId,
      const Option<CommandInfo>& commandInfo,
      const Option<Resources>& resources,
      const Option<ContainerInfo>& containerInfo,
      const Option<std::function<process::Future<Nothing>()>>& _postStartHook,
      const Option<std::function<process::Future<Nothing>()>>& _postStopHook);

  ContainerDaemonProcess(const ContainerDaemonProcess& other) = delete;

  ContainerDaemonProcess& operator=(
      const ContainerDaemonProcess& other) = delete;

  process::Future<Nothing> wait();

  // Made public for testing purpose.
  void launchContainer();
  void waitContainer();

protected:
  void initialize() override;

private:
  const process::http::URL nodeUrl;
  const Option<std::string> authToken;
  const ContentType contentType;
  const Option<std::function<process::Future<Nothing>()>> postStartHook;
  const Option<std::function<process::Future<Nothing>()>> postStopHook;

  medios::node::Call launchCall;
  medios::node::Call waitCall;

  process::Promise<Nothing> terminated;
};

} // namespace node {
} // namespace internal {
} // namespace medios {

#endif // __NODE_CONTAINER_DAEMON_PROCESS_HPP__
