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

#ifndef __CONTROLLER_CONTENDER_STANDALONE_HPP__
#define __CONTROLLER_CONTENDER_STANDALONE_HPP__

#include <medios/medios.hpp>

#include <medios/controller/contender.hpp>

#include <process/future.hpp>

#include <stout/nothing.hpp>

namespace medios {
namespace controller {
namespace contender {

// A basic implementation which assumes only one controller is
// contending.
class StandaloneControllerContender : public ControllerContender
{
public:
  StandaloneControllerContender()
    : initialized(false),
      promise(nullptr) {}

  ~StandaloneControllerContender() override;

  // ControllerContender implementation.
  void initialize(const ControllerInfo& controllerInfo) override;

  // In this basic implementation the outer Future directly returns
  // and inner Future stays pending because there is only one
  // contender in the contest.
  process::Future<process::Future<Nothing>> contend() override;

private:
  bool initialized;
  process::Promise<Nothing>* promise;
};

} // namespace contender {
} // namespace controller {
} // namespace medios {

#endif // __CONTROLLER_CONTENDER_STANDALONE_HPP__
