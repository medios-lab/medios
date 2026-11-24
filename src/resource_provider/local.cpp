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

#include "resource_provider/local.hpp"

namespace http = process::http;

using std::string;

using process::Owned;

using process::http::authentication::Principal;

namespace medios {
namespace internal {

// Built-in CSI and Storage Local Resource Providers (SLRP) were stripped in
// STRIP-009 (PR #46). Shared dataset and storage orchestration in Medios is
// implemented via node-declared `store/<name>` shared resources (DR-036,
// `src/node/stores.hpp`) rather than Local Resource Providers, and
// `LocalResourceProvider` / `LocalResourceProviderDaemon` are retained solely
// as no-op compatibility stubs.
Try<Owned<LocalResourceProvider>> LocalResourceProvider::create(
    const http::URL&,
    const string&,
    const ResourceProviderInfo& info,
    const NodeID&,
    const Option<string>&,
    bool)
{
  return Error("Unknown local resource provider type '" + info.type() + "'");
}


Option<Error> LocalResourceProvider::validate(const ResourceProviderInfo& info)
{
  return Error("Unknown local resource provider type '" + info.type() + "'");
}


Principal LocalResourceProvider::principal(const ResourceProviderInfo& info)
{
  // The 'cid_prefix' for the resource provider is of the following format:
  //     <rp_type>-<rp_name>--
  // where <rp_type> and <rp_name> are the type and name of the resource
  // provider, with dots replaced by dashes. We use a double-dash at the end to
  // explicitly mark the end of the prefix.
  const string cidPrefix = strings::join(
      "-",
      strings::replace(info.type(), ".", "-"),
      info.name(),
      "-");

  return Principal(
      Option<string>::none(),
      {{"cid_prefix", cidPrefix}});
}

} // namespace internal {
} // namespace medios {
