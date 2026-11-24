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

#ifndef __MEDIOS_CONTROLLER_CONTENDER_HPP__
#define __MEDIOS_CONTROLLER_CONTENDER_HPP__

#include <string>

#include <medios/medios.hpp>

#include <process/future.hpp>

#include <stout/duration.hpp>
#include <stout/nothing.hpp>
#include <stout/option.hpp>
#include <stout/try.hpp>

namespace medios {
namespace controller {
namespace contender {

/**
 * An abstraction for contending to be a leading controller.
 *
 * TODO(benh): Support contending with a v1::ControllerInfo.
 */
class ControllerContender
{
public:
  /**
   * Creates a controller contender. If `controllerContenderModule` contains a valid
   * module name (that is, a name that matches the name specified in a JSON
   * file/string passed into the command-line invocations using the `--modules`
   * flag). Additional parameters required to create an object of the contender
   * type are expected to be specified in the JSON's `parameter` object. In
   * command-line invocations, the value of `controllerContenderModule` is expected
   * to come from the `--controller_contender` flag.
   *
   * If both arguments are `None`, `StandaloneControllerContender` is returned.
   *
   * Note that the returned contender still needs to be `initialize()`d.
   */
  static Try<ControllerContender*> create(
      const Option<std::string>& zk,
      const Option<std::string>& controllerContenderModule = None(),
      const Option<Duration>& zkSessionTimeout = None());

  /**
   * Note that the contender's membership, if obtained, is scheduled
   * to be cancelled during destruction.
   */
  virtual ~ControllerContender() = 0;

  /**
   * Initializes the contender with the ControllerInfo of the controller it
   * contends on behalf of.
   */
  virtual void initialize(const ControllerInfo& controllerInfo) = 0;

  /**
   * Returns a Future<Nothing> once the contender has entered the
   * contest (by obtaining a membership) and an error otherwise.
   * A failed future is returned if this method is called before
   * initialize().
   * The inner Future returns Nothing when the contender is out of
   * the contest (i.e. its membership is lost).
   * Controller implementations treat inner future resolution as lost
   * candidacy and exit the process; external contender modules such as
   * future etcd-backed implementations must preserve this contract.
   *
   * This method can be used to contend again after candidacy is
   * obtained (the outer future satisfied), otherwise the future for
   * the pending election is returned.
   * Recontending after candidacy is obtained causes the previous
   * candidacy to be withdrawn.
   */
  virtual process::Future<process::Future<Nothing>> contend() = 0;
};

} // namespace contender {
} // namespace controller {
} // namespace medios {

#endif // __MEDIOS_CONTROLLER_CONTENDER_HPP__
