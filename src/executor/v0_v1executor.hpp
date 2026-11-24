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

#ifndef __V0_V1EXECUTOR_HPP__
#define __V0_V1EXECUTOR_HPP__

#include <functional>
#include <string>

#include <medios/executor.hpp>
#include <medios/medios.hpp>

#include <medios/v1/executor.hpp>

#include <process/owned.hpp>

namespace medios {
namespace v1 {
namespace executor {

class V0ToV1AdapterProcess; // Forward declaration.

// This interface acts as an adapter from the v0 (driver + executor) to the
// v1 Medios executor.
class V0ToV1Adapter : public medios::Executor, public MediosBase
{
public:
  V0ToV1Adapter(
      const std::function<void(void)>& connected,
      const std::function<void(void)>& disconnected,
      const std::function<void(const std::queue<Event>&)>& received);

  ~V0ToV1Adapter() override;

  void registered(
      ExecutorDriver* driver,
      const medios::ExecutorInfo& executorInfo,
      const medios::FrameworkInfo& frameworkInfo,
      const medios::NodeInfo& nodeInfo) override;

  void reregistered(
      ExecutorDriver* driver,
      const medios::NodeInfo& nodeInfo) override;

  void launchTask(
      ExecutorDriver* driver,
      const medios::TaskInfo& task) override;

  void disconnected(ExecutorDriver* driver) override;

  void killTask(
      ExecutorDriver* driver,
      const medios::TaskID& taskId) override;

  void frameworkMessage(
      ExecutorDriver* driver,
      const std::string& data) override;

  void shutdown(ExecutorDriver* driver) override;

  void error(
      ExecutorDriver* driver,
      const std::string& message) override;

  void send(const Call& call) override;

private:
  process::Owned<V0ToV1AdapterProcess> process;
  MediosExecutorDriver driver;
};

} // namespace executor {
} // namespace v1 {
} // namespace medios {

#endif // __V0_V1EXECUTOR_HPP__
