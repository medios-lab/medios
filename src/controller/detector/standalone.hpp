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

#ifndef __CONTROLLER_DETECTOR_STANDALONE_HPP__
#define __CONTROLLER_DETECTOR_STANDALONE_HPP__

#include <string>

#include <medios/medios.hpp>

#include <medios/controller/detector.hpp>

#include <process/future.hpp>

#include <stout/option.hpp>

namespace medios {
namespace controller {
namespace detector {

// Forward declarations.
class StandaloneControllerDetectorProcess;

// A standalone implementation of the ControllerDetector with no external
// discovery mechanism so the user has to manually appoint a leader
// to the detector for it to be detected.
class StandaloneControllerDetector : public ControllerDetector
{
public:
  StandaloneControllerDetector();
  // Use this constructor if the leader is known beforehand so it is
  // unnecessary to call 'appoint()' separately.
  explicit StandaloneControllerDetector(const ControllerInfo& leader);

  // Same as above but takes UPID as the parameter.
  explicit StandaloneControllerDetector(const process::UPID& leader);

  ~StandaloneControllerDetector() override;

  // Appoint the leading controller so it can be *detected*.
  void appoint(const Option<ControllerInfo>& leader);

  // Same as above but takes 'UPID' as the parameter.
  void appoint(const process::UPID& leader);

  process::Future<Option<ControllerInfo>> detect(
      const Option<ControllerInfo>& previous = None()) override;

private:
  StandaloneControllerDetectorProcess* process;
};

} // namespace detector {
} // namespace controller {
} // namespace medios {

#endif // __CONTROLLER_DETECTOR_STANDALONE_HPP__
