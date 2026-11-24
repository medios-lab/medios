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

// Leader election over the etcd v3 gRPC gateway (#359): contract
// tests against an in-process fake gateway. The fake is a libprocess
// process named "v3", so its routes appear at the same /v3/... paths
// the real gateway serves.

#include <string>

#include <gmock/gmock.h>

#include <medios/type_utils.hpp>

#include <process/clock.hpp>
#include <process/defer.hpp>
#include <process/dispatch.hpp>
#include <process/future.hpp>
#include <process/gtest.hpp>
#include <process/http.hpp>
#include <process/owned.hpp>
#include <process/process.hpp>

#include <stout/base64.hpp>
#include <stout/json.hpp>
#include <stout/option.hpp>
#include <stout/protobuf.hpp>
#include <stout/stringify.hpp>
#include <stout/try.hpp>

#include "common/protobuf_utils.hpp"

#include "controller/contender/etcd.hpp"
#include "controller/detector/etcd.hpp"
#include "controller/etcd/client.hpp"

#include "tests/medios.hpp"

using medios::controller::contender::EtcdControllerContender;
using medios::controller::detector::EtcdControllerDetector;
using medios::controller::etcd::EtcdURL;

using process::Clock;
using process::Future;
using process::Owned;
using process::Promise;
using process::UPID;

using process::http::OK;
using process::http::NotFound;

using std::string;

namespace medios {
namespace internal {
namespace tests {

// Serves the subset of the etcd v3 gateway that leader election uses.
// Leadership, keepalive health, and campaign outcomes are test-driven.
class FakeEtcdGatewayProcess : public process::Process<FakeEtcdGatewayProcess>
{
public:
  FakeEtcdGatewayProcess()
    : ProcessBase("v3"),
      keepaliveFails(false),
      campaignWins(true) {}

  void setLeader(const Option<string>& value) { leaderValue = value; }
  void setKeepaliveFails(bool fails) { keepaliveFails = fails; }
  void setCampaignWins(bool wins) { campaignWins = wins; }

protected:
  void initialize() override
  {
    route("/lease/grant", None(), &FakeEtcdGatewayProcess::grant);
    route("/lease/keepalive", None(), &FakeEtcdGatewayProcess::keepalive);
    route("/lease/revoke", None(), &FakeEtcdGatewayProcess::revoke);
    route("/election/campaign", None(), &FakeEtcdGatewayProcess::campaign);
    route("/election/leader", None(), &FakeEtcdGatewayProcess::leader);
  }

private:
  Future<process::http::Response> grant(const process::http::Request&)
  {
    JSON::Object body;
    body.values["ID"] = "12345";
    return OK(stringify(body));
  }

  Future<process::http::Response> keepalive(const process::http::Request&)
  {
    JSON::Object result;
    result.values["ID"] = "12345";

    if (!keepaliveFails) {
      result.values["TTL"] = "10";
    }

    JSON::Object body;
    body.values["result"] = std::move(result);
    return OK(stringify(body));
  }

  Future<process::http::Response> revoke(const process::http::Request&)
  {
    return OK("{}");
  }

  Future<process::http::Response> campaign(const process::http::Request&)
  {
    if (campaignWins) {
      return OK("{\"leader\": {}}");
    }

    // Long-poll forever (a candidate that never wins).
    pending.reset(new Promise<process::http::Response>());
    return pending->future();
  }

  Future<process::http::Response> leader(const process::http::Request&)
  {
    if (leaderValue.isNone()) {
      JSON::Object body;
      body.values["code"] = 2;
      body.values["message"] = "election: no leader";
      return NotFound(stringify(body));
    }

    JSON::Object kv;
    kv.values["value"] = base64::encode(leaderValue.get());

    JSON::Object body;
    body.values["kv"] = std::move(kv);
    return OK(stringify(body));
  }

  Option<string> leaderValue;
  bool keepaliveFails;
  bool campaignWins;
  Owned<Promise<process::http::Response>> pending;
};


class EtcdElectionTest : public MediosTest
{
protected:
  void SetUp() override
  {
    MediosTest::SetUp();

    gateway = new FakeEtcdGatewayProcess();
    process::spawn(gateway);
  }

  void TearDown() override
  {
    process::terminate(gateway);
    process::wait(gateway);
    delete gateway;

    MediosTest::TearDown();
  }

  string url() const
  {
    return "etcd://" + stringify(process::address().ip) + ":" +
           stringify(process::address().port) + "/medios";
  }

  static string controllerJson(const string& pid)
  {
    return stringify(
        JSON::protobuf(protobuf::createControllerInfo(UPID(pid))));
  }

  FakeEtcdGatewayProcess* gateway;
};


TEST_F(EtcdElectionTest, URLParse)
{
  Try<EtcdURL> good = EtcdURL::parse("etcd://10.0.0.1:2379/medios");
  ASSERT_SOME(good);
  EXPECT_EQ("10.0.0.1", good->host);
  EXPECT_EQ(2379, good->port);
  EXPECT_EQ("medios", good->election);

  EXPECT_ERROR(EtcdURL::parse("etcd://10.0.0.1:2379"));   // No election.
  EXPECT_ERROR(EtcdURL::parse("etcd://10.0.0.1:2379/"));  // Empty election.
  EXPECT_ERROR(EtcdURL::parse("etcd://10.0.0.1/medios")); // No port.
  EXPECT_ERROR(EtcdURL::parse("zk://10.0.0.1:2181/medios"));
}


// The detector reports the leader, leader changes, and no-leader.
TEST_F(EtcdElectionTest, DetectorObservesLeadershipChanges)
{
  Try<EtcdURL> parsed = EtcdURL::parse(url());
  ASSERT_SOME(parsed);

  EtcdControllerDetector detector(parsed.get());

  // No leader yet.
  Future<Option<ControllerInfo>> noLeader = detector.detect();
  AWAIT_READY(noLeader);
  EXPECT_NONE(noLeader.get());

  // A leader appears.
  process::dispatch(
      gateway,
      &FakeEtcdGatewayProcess::setLeader,
      Option<string>(controllerJson("controller@127.0.0.1:5050")));

  Future<Option<ControllerInfo>> first = detector.detect(noLeader.get());
  AWAIT_READY(first);
  ASSERT_SOME(first.get());
  EXPECT_EQ("controller@127.0.0.1:5050", first->get().pid());

  // Leadership changes.
  process::dispatch(
      gateway,
      &FakeEtcdGatewayProcess::setLeader,
      Option<string>(controllerJson("controller@127.0.0.1:5051")));

  Future<Option<ControllerInfo>> second = detector.detect(first.get());
  AWAIT_READY(second);
  ASSERT_SOME(second.get());
  EXPECT_EQ("controller@127.0.0.1:5051", second->get().pid());

  // The leader goes away.
  process::dispatch(
      gateway,
      &FakeEtcdGatewayProcess::setLeader,
      Option<string>::none());

  Future<Option<ControllerInfo>> gone = detector.detect(second.get());
  AWAIT_READY(gone);
  EXPECT_NONE(gone.get());
}


// Candidacy is established once a lease is granted, survives healthy
// keepalives, and is lost when the lease cannot be kept alive.
TEST_F(EtcdElectionTest, ContenderLosesCandidacyOnKeepaliveFailure)
{
  Try<EtcdURL> parsed = EtcdURL::parse(url());
  ASSERT_SOME(parsed);

  EtcdControllerContender contender(parsed.get());
  contender.initialize(
      protobuf::createControllerInfo(UPID("controller@127.0.0.1:5050")));

  Future<Future<Nothing>> candidacy = contender.contend();
  AWAIT_READY(candidacy);

  Future<Nothing> lost = candidacy.get();
  EXPECT_TRUE(lost.isPending());

  // Break the lease: the next keepalive fails and candidacy is lost.
  process::dispatch(
      gateway, &FakeEtcdGatewayProcess::setKeepaliveFails, true);

  AWAIT_READY(lost);
}


// An uninitialized contender refuses to contend.
TEST_F(EtcdElectionTest, ContenderRequiresInitialization)
{
  Try<EtcdURL> parsed = EtcdURL::parse(url());
  ASSERT_SOME(parsed);

  EtcdControllerContender contender(parsed.get());

  AWAIT_FAILED(contender.contend());
}

} // namespace tests {
} // namespace internal {
} // namespace medios {
