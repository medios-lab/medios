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

#ifndef __CONTROLLER_ETCD_CLIENT_HPP__
#define __CONTROLLER_ETCD_CLIENT_HPP__

#include <string>

#include <process/future.hpp>
#include <process/http.hpp>

#include <stout/duration.hpp>
#include <stout/nothing.hpp>
#include <stout/option.hpp>
#include <stout/try.hpp>

namespace medios {
namespace controller {
namespace etcd {

// An election coordinate: `etcd://host:port/<election-name>`. The
// election name is required — there is no default.
struct EtcdURL
{
  static Try<EtcdURL> parse(const std::string& url);

  std::string host;
  uint16_t port;
  std::string election;
};


// A minimal client for the etcd v3 gRPC gateway (HTTP/JSON): leases
// and the election service, which is all leader election needs. Keys
// and values travel base64-encoded per the gateway's JSON mapping;
// int64 fields travel as strings.
class EtcdClient
{
public:
  explicit EtcdClient(const EtcdURL& url);

  // Grants a lease; returns the lease id.
  process::Future<std::string> leaseGrant(const Duration& ttl) const;

  // One-shot keepalive. Fails if the lease no longer exists.
  process::Future<Nothing> leaseKeepalive(const std::string& leaseId) const;

  process::Future<Nothing> leaseRevoke(const std::string& leaseId) const;

  // Campaigns for leadership of the election with the given value
  // bound to the lease. The returned future stays pending until
  // leadership is acquired (the gateway long-polls).
  process::Future<Nothing> campaign(
      const std::string& leaseId,
      const std::string& value) const;

  // The current leader's value, or none if the election has no leader.
  process::Future<Option<std::string>> leader() const;

private:
  process::http::URL endpoint(const std::string& path) const;

  EtcdURL url;
};

} // namespace etcd {
} // namespace controller {
} // namespace medios {

#endif // __CONTROLLER_ETCD_CLIENT_HPP__
