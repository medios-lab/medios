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

#ifndef __CONTROLLER_DETECTOR_ETCD_HPP__
#define __CONTROLLER_DETECTOR_ETCD_HPP__

#include <medios/controller/detector.hpp>

#include <process/future.hpp>

#include <stout/option.hpp>

#include "controller/etcd/client.hpp"

namespace medios {
namespace controller {
namespace detector {

class EtcdControllerDetectorProcess;

// Detects the leading controller by polling the etcd v3 election
// service; each observed leader value decodes to a ControllerInfo.
// Used by standby controllers, nodes, and driver-based schedulers.
class EtcdControllerDetector : public ControllerDetector
{
public:
  explicit EtcdControllerDetector(const etcd::EtcdURL& url);

  ~EtcdControllerDetector() override;

  process::Future<Option<ControllerInfo>> detect(
      const Option<ControllerInfo>& previous = None()) override;

private:
  EtcdControllerDetectorProcess* process;
};

} // namespace detector {
} // namespace controller {
} // namespace medios {

#endif // __CONTROLLER_DETECTOR_ETCD_HPP__
