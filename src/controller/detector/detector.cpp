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

#include <medios/controller/detector.hpp>

#include <medios/module/detector.hpp>

#include <process/pid.hpp>
#include <process/process.hpp>

#include <stout/strings.hpp>

#include "common/protobuf_utils.hpp"

#include "controller/detector/etcd.hpp"
#include "controller/detector/standalone.hpp"

#include "controller/etcd/client.hpp"

#include "module/manager.hpp"

using namespace process;

using std::string;

namespace medios {
namespace controller {
namespace detector {

Try<ControllerDetector*> ControllerDetector::create(
    const Option<string>& zk_,
    const Option<string>& controllerDetectorModule_,
    const Option<Duration>&)
{
  if (controllerDetectorModule_.isSome()) {
    return modules::ModuleManager::create<ControllerDetector>(
        controllerDetectorModule_.get());
  }

  if (zk_.isNone()) {
    return new StandaloneControllerDetector();
  }

  const string& zk = zk_.get();

  if (strings::startsWith(zk, "etcd://")) {
    Try<etcd::EtcdURL> url = etcd::EtcdURL::parse(zk);
    if (url.isError()) {
      return Error(url.error());
    }

    return new EtcdControllerDetector(url.get());
  }

  if (strings::startsWith(zk, "zk://") || strings::startsWith(zk, "file://")) {
    return Error(
        "ZooKeeper controller detection has been removed; use an "
        "'etcd://host:port/<election-name>' URL, a controller detector "
        "module, standalone mode, or a direct controller PID");
  }

  // Okay, try and parse what we got as a PID.
  UPID pid = zk.find("controller@") == 0
             ? UPID(zk)
             : UPID("controller@" + zk);

  if (!pid) {
    return Error("Failed to parse '" + zk + "'");
  }

  return new StandaloneControllerDetector(
      internal::protobuf::createControllerInfo(pid));
}


ControllerDetector::~ControllerDetector() {}

} // namespace detector {
} // namespace controller {
} // namespace medios {
