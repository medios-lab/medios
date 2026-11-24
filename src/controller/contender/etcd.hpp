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

#ifndef __CONTROLLER_CONTENDER_ETCD_HPP__
#define __CONTROLLER_CONTENDER_ETCD_HPP__

#include <medios/controller/contender.hpp>

#include <process/future.hpp>

#include <stout/nothing.hpp>

#include "controller/etcd/client.hpp"

namespace medios {
namespace controller {
namespace contender {

class EtcdControllerContenderProcess;

// Contends for leadership through the etcd v3 election service: a
// lease-bound campaign whose value is the serialized ControllerInfo.
// Candidacy is lost when the lease cannot be kept alive — the caller
// (the controller) treats lost candidacy as fatal, which is the
// abdication guarantee: a leader that cannot renew its lease stops
// leading before a rival's campaign can win.
class EtcdControllerContender : public ControllerContender
{
public:
  explicit EtcdControllerContender(const etcd::EtcdURL& url);

  ~EtcdControllerContender() override;

  void initialize(const ControllerInfo& controllerInfo) override;

  process::Future<process::Future<Nothing>> contend() override;

private:
  EtcdControllerContenderProcess* process;
};

} // namespace contender {
} // namespace controller {
} // namespace medios {

#endif // __CONTROLLER_CONTENDER_ETCD_HPP__
