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

#include "controller/detector/standalone.hpp"

#include <set>

#include <medios/controller/detector.hpp>

#include <process/defer.hpp>
#include <process/dispatch.hpp>
#include <process/future.hpp>
#include <process/id.hpp>
#include <process/process.hpp>

#include "common/protobuf_utils.hpp"

using namespace process;

using std::set;

namespace medios {
namespace controller {
namespace detector {

class StandaloneControllerDetectorProcess
  : public Process<StandaloneControllerDetectorProcess>
{
public:
  StandaloneControllerDetectorProcess()
    : ProcessBase(ID::generate("standalone-controller-detector")) {}
  explicit StandaloneControllerDetectorProcess(const ControllerInfo& _leader)
    : ProcessBase(ID::generate("standalone-controller-detector")),
      leader(_leader) {}

  ~StandaloneControllerDetectorProcess() override
  {
    discardPromises(&promises);
  }

  void appoint(const Option<ControllerInfo>& leader_)
  {
    leader = leader_;

    setPromises(&promises, leader);
  }

  Future<Option<ControllerInfo>> detect(
      const Option<ControllerInfo>& previous = None())
  {
    if (leader != previous) {
      return leader;
    }

    Promise<Option<ControllerInfo>>* promise = new Promise<Option<ControllerInfo>>();

    promise->future()
      .onDiscard(defer(self(), &Self::discard, promise->future()));

    promises.insert(promise);
    return promise->future();
  }

private:
  void discard(const Future<Option<ControllerInfo>>& future)
  {
    // Discard the promise holding this future.
    discardPromises(&promises, future);
  }

  Option<ControllerInfo> leader; // The appointed controller.
  set<Promise<Option<ControllerInfo>>*> promises;
};


StandaloneControllerDetector::StandaloneControllerDetector()
{
  process = new StandaloneControllerDetectorProcess();
  spawn(process);
}


StandaloneControllerDetector::StandaloneControllerDetector(const ControllerInfo& leader)
{
  process = new StandaloneControllerDetectorProcess(leader);
  spawn(process);
}


StandaloneControllerDetector::StandaloneControllerDetector(const UPID& leader)
{
  process = new StandaloneControllerDetectorProcess(
      medios::internal::protobuf::createControllerInfo(leader));

  spawn(process);
}


StandaloneControllerDetector::~StandaloneControllerDetector()
{
  terminate(process);
  process::wait(process);
  delete process;
}


void StandaloneControllerDetector::appoint(const Option<ControllerInfo>& leader)
{
  dispatch(process, &StandaloneControllerDetectorProcess::appoint, leader);
}


void StandaloneControllerDetector::appoint(const UPID& leader)
{
  dispatch(process,
           &StandaloneControllerDetectorProcess::appoint,
           medios::internal::protobuf::createControllerInfo(leader));
}


Future<Option<ControllerInfo>> StandaloneControllerDetector::detect(
    const Option<ControllerInfo>& previous)
{
  return dispatch(process, &StandaloneControllerDetectorProcess::detect, previous);
}

} // namespace detector {
} // namespace controller {
} // namespace medios {
