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

#include "controller/contender/etcd.hpp"

#include <process/defer.hpp>
#include <process/delay.hpp>
#include <process/dispatch.hpp>
#include <process/id.hpp>
#include <process/process.hpp>

#include <stout/json.hpp>
#include <stout/protobuf.hpp>

using process::Failure;
using process::Future;
using process::Promise;

using std::string;

namespace medios {
namespace controller {
namespace contender {

// The lease TTL bounds failover time: a dead leader's lease expires
// after at most this long, letting a standby's campaign win. The
// keepalive cadence (TTL/3) gives a live leader two retries before
// its lease can lapse.
constexpr Duration ETCD_LEASE_TTL = Seconds(10);


class EtcdControllerContenderProcess
  : public process::Process<EtcdControllerContenderProcess>
{
public:
  explicit EtcdControllerContenderProcess(const etcd::EtcdURL& url)
    : ProcessBase(process::ID::generate("etcd-controller-contender")),
      client(url) {}

  using process::ProcessBase::initialize;

  void initialize(const ControllerInfo& _controllerInfo)
  {
    controllerInfo = _controllerInfo;
  }

  Future<Future<Nothing>> contend()
  {
    if (controllerInfo.isNone()) {
      return Failure("Initialize the contender first");
    }

    // Should not recontend if the last candidacy is still live.
    if (lost.get() != nullptr && lost->future().isPending() &&
        candidacy.isSome() && !candidacy->isFailed()) {
      return candidacy.get();
    }

    // Withdraw any previous candidacy before recontending.
    campaignFuture.discard();
    if (leaseId.isSome()) {
      client.leaseRevoke(leaseId.get());
      leaseId = None();
    }

    lost.reset(new Promise<Nothing>());

    const string value = stringify(JSON::protobuf(controllerInfo.get()));

    candidacy = client.leaseGrant(ETCD_LEASE_TTL)
      .then(process::defer(self(), [=, this](const string& lease) {
        leaseId = lease;

        // The campaign long-polls until leadership is acquired; the
        // detector is what observes the result. Campaign failure is
        // candidacy loss.
        campaignFuture = client.campaign(lease, value);
        campaignFuture
          .onFailed(process::defer(self(), &Self::lose, lambda::_1))
          .onReady([]() {
            LOG(INFO) << "Won the etcd controller election";
          });

        keepalive();

        return Future<Future<Nothing>>(lost->future());
      }));

    return candidacy.get();
  }

private:
  void keepalive()
  {
    if (leaseId.isNone() || lost->future().isReady()) {
      return;
    }

    client.leaseKeepalive(leaseId.get())
      .onAny(process::defer(self(), &Self::_keepalive, lambda::_1));
  }

  void _keepalive(const Future<Nothing>& result)
  {
    if (!result.isReady()) {
      lose("Failed to keep the election lease alive: " +
           (result.isFailed() ? result.failure() : "discarded"));
      return;
    }

    process::delay(ETCD_LEASE_TTL / 3, self(), &Self::keepalive);
  }

  void lose(const string& message)
  {
    LOG(WARNING) << "Etcd candidacy lost: " << message;

    leaseId = None();
    lost->set(Nothing());
  }

  etcd::EtcdClient client;

  Option<ControllerInfo> controllerInfo;
  Option<string> leaseId;
  Future<Nothing> campaignFuture;
  process::Owned<Promise<Nothing>> lost;
  Option<Future<Future<Nothing>>> candidacy;
};


EtcdControllerContender::EtcdControllerContender(const etcd::EtcdURL& url)
{
  process = new EtcdControllerContenderProcess(url);
  process::spawn(process);
}


EtcdControllerContender::~EtcdControllerContender()
{
  process::terminate(process);
  process::wait(process);
  delete process;
}


void EtcdControllerContender::initialize(const ControllerInfo& controllerInfo)
{
  process->initialize(controllerInfo);
}


Future<Future<Nothing>> EtcdControllerContender::contend()
{
  return process::dispatch(
      process, &EtcdControllerContenderProcess::contend);
}

} // namespace contender {
} // namespace controller {
} // namespace medios {
