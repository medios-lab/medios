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

#ifndef __NODE_CONTAINERIZER_FETCHER_PROCESS_HPP__
#define __NODE_CONTAINERIZER_FETCHER_PROCESS_HPP__

#include <list>
#include <memory>
#include <string>

#include <medios/medios.hpp>
#include <medios/type_utils.hpp>

#include <process/future.hpp>
#include <process/process.hpp>

#include <process/metrics/counter.hpp>
#include <process/metrics/pull_gauge.hpp>

#include <stout/hashmap.hpp>

#include "node/flags.hpp"

namespace medios {
namespace internal {
namespace node {

class FetcherProcess : public process::Process<FetcherProcess>
{
public:
  explicit FetcherProcess(const Flags& _flags);
  ~FetcherProcess() override;

  process::Future<Nothing> fetch(
      const ContainerID& containerId,
      const CommandInfo& commandInfo,
      const std::string& sandboxDirectory,
      const Option<std::string>& user);

  // Runs the medios-fetcher, creating a "stdout" and "stderr" file
  // in the given directory, using these for trace output.
  virtual process::Future<Nothing> run(
      const ContainerID& containerId,
      const std::string& sandboxDirectory,
      const Option<std::string>& user,
      const medios::fetcher::FetcherInfo& info);

  // Best effort attempt to kill the external medios-fetcher process
  // running on behalf of the given container ID, if any.
  void kill(const ContainerID& containerId);

private:

  struct Metrics
  {
    explicit Metrics(FetcherProcess *fetcher);
    ~Metrics();

    // NOTE: These metrics will increment at most once per task. Even if
    // a single task asks for multiple artifacts, the total number of
    // fetches will only go up by one. And if any of those artifacts
    // fail to fetch, the failure count will only increase by one.
    process::metrics::Counter task_fetches_succeeded;
    process::metrics::Counter task_fetches_failed;

  } metrics;

  const Flags flags;

  hashmap<ContainerID, pid_t> subprocessPids;
};


} // namespace node {
} // namespace internal {
} // namespace medios {

#endif // __NODE_CONTAINERIZER_FETCHER_PROCESS_HPP__
