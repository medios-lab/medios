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

#include <medios/controller/contender.hpp>

#include <medios/module/contender.hpp>

#include <stout/strings.hpp>

#include "controller/contender/etcd.hpp"
#include "controller/contender/standalone.hpp"

#include "controller/etcd/client.hpp"

#include "module/manager.hpp"

using std::string;

namespace medios {
namespace controller {
namespace contender {

Try<ControllerContender*> ControllerContender::create(
    const Option<string>& zk,
    const Option<string>& controllerContenderModule_,
    const Option<Duration>&)
{
  if (controllerContenderModule_.isSome()) {
    return modules::ModuleManager::create<ControllerContender>(
        controllerContenderModule_.get());
  }

  if (zk.isSome()) {
    if (strings::startsWith(zk.get(), "etcd://")) {
      Try<etcd::EtcdURL> url = etcd::EtcdURL::parse(zk.get());
      if (url.isError()) {
        return Error(url.error());
      }

      return new EtcdControllerContender(url.get());
    }

    return Error(
        "ZooKeeper controller contention has been removed; use an "
        "'etcd://host:port/<election-name>' URL, a controller contender "
        "module, or standalone mode");
  }

  return new StandaloneControllerContender();
}


ControllerContender::~ControllerContender() {}

} // namespace contender {
} // namespace controller {
} // namespace medios {
