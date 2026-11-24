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

#include "controller/detector/etcd.hpp"

#include <set>

#include <medios/type_utils.hpp>

#include <process/defer.hpp>
#include <process/delay.hpp>
#include <process/dispatch.hpp>
#include <process/id.hpp>
#include <process/process.hpp>

#include <stout/json.hpp>
#include <stout/protobuf.hpp>

using process::Future;
using process::Promise;

using std::set;
using std::string;

namespace medios {
namespace controller {
namespace detector {

// Leadership-change detection latency. Bounded below by taste, above
// by failover time; the lease TTL (contender side) dominates failover
// anyway.
constexpr Duration ETCD_LEADER_POLL_INTERVAL = Seconds(1);


class EtcdControllerDetectorProcess
  : public process::Process<EtcdControllerDetectorProcess>
{
public:
  explicit EtcdControllerDetectorProcess(const etcd::EtcdURL& url)
    : ProcessBase(process::ID::generate("etcd-controller-detector")),
      client(url),
      polled(false) {}

  ~EtcdControllerDetectorProcess() override
  {
    foreach (Promise<Option<ControllerInfo>>* promise, promises) {
      promise->discard();
      delete promise;
    }
    promises.clear();
  }

  void initialize() override
  {
    poll();
  }

  Future<Option<ControllerInfo>> detect(const Option<ControllerInfo>& previous)
  {
    // If the current leader is already known and differs from what the
    // caller has, answer immediately; otherwise wait for a change.
    if (polled && leader != previous) {
      return leader;
    }

    Promise<Option<ControllerInfo>>* promise =
      new Promise<Option<ControllerInfo>>();
    promises.insert(promise);
    return promise->future();
  }

private:
  void poll()
  {
    client.leader()
      .onAny(process::defer(self(), &Self::_poll, lambda::_1));
  }

  void _poll(const Future<Option<string>>& result)
  {
    if (result.isReady()) {
      Option<ControllerInfo> current = None();

      if (result->isSome()) {
        Try<ControllerInfo> info = parse(result->get());
        if (info.isSome()) {
          current = info.get();
        } else {
          LOG(ERROR) << "Ignoring unparsable leader value: " << info.error();
        }
      }

      if (!polled || current != leader) {
        polled = true;
        leader = current;

        LOG(INFO) << "Detected leading controller: "
                  << (leader.isSome() ? leader->pid() : "(none)");

        foreach (Promise<Option<ControllerInfo>>* promise, promises) {
          promise->set(leader);
          delete promise;
        }
        promises.clear();
      }

      polled = true;
    } else {
      // Transient etcd unavailability: keep the last-known leader.
      // Silence from the detector means "no change", which lets the
      // cluster ride out etcd blips without flapping leadership.
      LOG(WARNING) << "Failed to query the etcd election leader: "
                   << (result.isFailed() ? result.failure() : "discarded");
    }

    process::delay(ETCD_LEADER_POLL_INTERVAL, self(), &Self::poll);
  }

  static Try<ControllerInfo> parse(const string& value)
  {
    Try<JSON::Object> object = JSON::parse<JSON::Object>(value);
    if (object.isError()) {
      return Error("Failed to parse JSON: " + object.error());
    }

    Try<ControllerInfo> info = ::protobuf::parse<ControllerInfo>(object.get());
    if (info.isError()) {
      return Error("Failed to parse ControllerInfo: " + info.error());
    }

    return info;
  }

  etcd::EtcdClient client;

  bool polled;
  Option<ControllerInfo> leader;
  set<Promise<Option<ControllerInfo>>*> promises;
};


EtcdControllerDetector::EtcdControllerDetector(const etcd::EtcdURL& url)
{
  process = new EtcdControllerDetectorProcess(url);
  process::spawn(process);
}


EtcdControllerDetector::~EtcdControllerDetector()
{
  process::terminate(process);
  process::wait(process);
  delete process;
}


Future<Option<ControllerInfo>> EtcdControllerDetector::detect(
    const Option<ControllerInfo>& previous)
{
  return process::dispatch(
      process, &EtcdControllerDetectorProcess::detect, previous);
}

} // namespace detector {
} // namespace controller {
} // namespace medios {
