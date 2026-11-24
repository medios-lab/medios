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

#ifndef __MEDIOS_CONTROLLER_DETECTOR_HPP__
#define __MEDIOS_CONTROLLER_DETECTOR_HPP__

#include <string>

#include <medios/medios.hpp>

#include <process/future.hpp>

#include <stout/duration.hpp>
#include <stout/option.hpp>
#include <stout/try.hpp>

namespace medios {
namespace controller {
namespace detector {

/**
 * An abstraction of a Controller detector which can be used to
 * detect the leading controller from a group.
 */
class ControllerDetector
{
public:
  /**
   * Creates a controller detector. If `controllerDetectorModule` contains a valid
   * module name (that is, a name that matches the name specified in a JSON
   * file/string passed into the command-line invocation using the `--modules`
   * flag), the result is the `ControllerDetector` returned by the module.
   * Additional parameters required to create an object of the detector type are
   * expected to be specified in the JSON's `parameter` object. In command-line
   * invocations, the value of `controllerDetectorModule` is expected to come from
   * the `--controller_detector` flag.
   *
   * If `controllerDetectorModule` is `None`, a direct controller PID may be
   * provided for clients that connect to a known controller. If both arguments
   * are `None`, `StandaloneControllerDetector` is returned.
   */
  static Try<ControllerDetector*> create(
      const Option<std::string>& zk,
      const Option<std::string>& controllerDetectorModule = None(),
      const Option<Duration>& zkSessionTimeout = None());

  virtual ~ControllerDetector() = 0;

  /**
   * Returns ControllerInfo after an election has occurred and the elected
   * controller is different than that specified (if any), or NONE if an
   * election occurs and no controller is elected (e.g., all controllers are
   * lost). A failed future is returned if the detector is unable to
   * detect the leading controller due to a non-retryable error.
   * Note that the detector transparently tries to recover from
   * retryable errors.
   * The future is never discarded unless it stays pending when the
   * detector destructs.
   *
   * The 'previous' result (if any) should be passed back if this
   * method is called repeatedly so the detector only returns when it
   * gets a different result.
   */
  virtual process::Future<Option<ControllerInfo>> detect(
      const Option<ControllerInfo>& previous = None()) = 0;
};

} // namespace detector {
} // namespace controller {
} // namespace medios {

#endif // __MEDIOS_CONTROLLER_DETECTOR_HPP__
