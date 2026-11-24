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

#include "controller/etcd/client.hpp"

#include <process/http.hpp>

#include <stout/base64.hpp>
#include <stout/json.hpp>
#include <stout/numify.hpp>
#include <stout/strings.hpp>

using process::Future;

using process::http::Response;

using std::string;

namespace medios {
namespace controller {
namespace etcd {

Try<EtcdURL> EtcdURL::parse(const string& url)
{
  if (!strings::startsWith(url, "etcd://")) {
    return Error("Expected an 'etcd://host:port/<election-name>' URL");
  }

  const string rest = url.substr(strlen("etcd://"));

  const size_t slash = rest.find('/');
  if (slash == string::npos || slash == rest.size() - 1) {
    return Error(
        "Missing election name: expected 'etcd://host:port/<election-name>'");
  }

  const string authority = rest.substr(0, slash);
  const string election = rest.substr(slash + 1);

  const size_t colon = authority.find(':');
  if (colon == string::npos || colon == 0 || colon == authority.size() - 1) {
    return Error(
        "Missing host or port: expected 'etcd://host:port/<election-name>'");
  }

  Try<uint16_t> port = numify<uint16_t>(authority.substr(colon + 1));
  if (port.isError()) {
    return Error("Failed to parse port: " + port.error());
  }

  EtcdURL result;
  result.host = authority.substr(0, colon);
  result.port = port.get();
  result.election = election;

  return result;
}


EtcdClient::EtcdClient(const EtcdURL& _url) : url(_url) {}


process::http::URL EtcdClient::endpoint(const string& path) const
{
  return process::http::URL("http", url.host, url.port, path);
}


// Issues a gateway call and returns the parsed JSON response body.
// Gateway errors surface as non-2xx statuses with a JSON error body.
static Future<JSON::Object> post(
    const process::http::URL& endpoint,
    const JSON::Object& body)
{
  return process::http::post(
      endpoint, None(), stringify(body), "application/json")
    .then([=](const Response& response) -> Future<JSON::Object> {
      Try<JSON::Object> json = JSON::parse<JSON::Object>(response.body);

      if (response.code != 200) {
        Option<JSON::String> message = json.isSome()
          ? json->find<JSON::String>("message").get()
          : Option<JSON::String>::none();

        return process::Failure(
            "etcd gateway returned " + response.status +
            (message.isSome() ? ": " + message->value : ""));
      }

      if (json.isError()) {
        return process::Failure(
            "Failed to parse etcd gateway response: " + json.error());
      }

      return json.get();
    });
}


Future<string> EtcdClient::leaseGrant(const Duration& ttl) const
{
  JSON::Object body;
  body.values["TTL"] = stringify(static_cast<int64_t>(ttl.secs()));

  return post(endpoint("/v3/lease/grant"), body)
    .then([](const JSON::Object& response) -> Future<string> {
      Result<JSON::String> id = response.find<JSON::String>("ID");
      if (!id.isSome()) {
        return process::Failure("Lease grant response carried no 'ID'");
      }

      return id->value;
    });
}


Future<Nothing> EtcdClient::leaseKeepalive(const string& leaseId) const
{
  JSON::Object body;
  body.values["ID"] = leaseId;

  return post(endpoint("/v3/lease/keepalive"), body)
    .then([](const JSON::Object& response) -> Future<Nothing> {
      // A keepalive for a vanished lease succeeds at the HTTP layer
      // but returns a result without a (positive) TTL.
      Result<JSON::Object> result = response.find<JSON::Object>("result");
      if (!result.isSome()) {
        return process::Failure("Lease keepalive response carried no result");
      }

      Result<JSON::String> ttl = result->find<JSON::String>("TTL");
      if (!ttl.isSome() || ttl->value == "0") {
        return process::Failure("Lease no longer exists");
      }

      return Nothing();
    });
}


Future<Nothing> EtcdClient::leaseRevoke(const string& leaseId) const
{
  JSON::Object body;
  body.values["ID"] = leaseId;

  return post(endpoint("/v3/lease/revoke"), body)
    .then([]() { return Nothing(); });
}


Future<Nothing> EtcdClient::campaign(
    const string& leaseId,
    const string& value) const
{
  JSON::Object body;
  body.values["name"] = base64::encode(url.election);
  body.values["lease"] = leaseId;
  body.values["value"] = base64::encode(value);

  return post(endpoint("/v3/election/campaign"), body)
    .then([]() { return Nothing(); });
}


Future<Option<string>> EtcdClient::leader() const
{
  JSON::Object body;
  body.values["name"] = base64::encode(url.election);

  return process::http::post(
      endpoint("/v3/election/leader"),
      None(),
      stringify(body),
      "application/json")
    .then([](const Response& response) -> Future<Option<string>> {
      Try<JSON::Object> json = JSON::parse<JSON::Object>(response.body);

      if (response.code != 200) {
        // "election: no leader" is the no-leader signal, not an error.
        if (json.isSome()) {
          Result<JSON::String> message = json->find<JSON::String>("message");
          if (message.isSome() &&
              strings::contains(message->value, "no leader")) {
            return Option<string>::none();
          }
        }

        return process::Failure(
            "etcd gateway returned " + response.status + ": " + response.body);
      }

      if (json.isError()) {
        return process::Failure(
            "Failed to parse etcd gateway response: " + json.error());
      }

      Result<JSON::Object> kv = json->find<JSON::Object>("kv");
      if (!kv.isSome()) {
        return Option<string>::none();
      }

      Result<JSON::String> value = kv->find<JSON::String>("value");
      if (!value.isSome()) {
        return process::Failure("Leader kv carried no value");
      }

      Try<string> decoded = base64::decode(value->value);
      if (decoded.isError()) {
        return process::Failure(
            "Failed to decode leader value: " + decoded.error());
      }

      return Option<string>(decoded.get());
    });
}

} // namespace etcd {
} // namespace controller {
} // namespace medios {
