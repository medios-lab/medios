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

#ifndef __URI_UTILS_HPP__
#define __URI_UTILS_HPP__

#include <string>

#include <stout/error.hpp>
#include <stout/none.hpp>
#include <stout/option.hpp>

#include <medios/uri/uri.hpp>

namespace medios {
namespace uri {

/**
 * Construct an URI with the given parameters. No validation will be
 * performed in this function.
 */
URI construct(
    const std::string& scheme,
    const std::string& path = "",
    const Option<std::string>& host = None(),
    const Option<int>& port = None(),
    const Option<std::string>& query = None(),
    const Option<std::string>& fragment = None(),
    const Option<std::string>& user = None(),
    const Option<std::string>& password = None());


// Returns a copy of `uri` with `password` replaced by `"***"` for safe
// logging and error reporting without affecting `stringify(uri)` callers that
// pass the full URI to `curl` (#397).
URI redact(const URI& uri);


// Validates that a network `URI` or URL string does not target cloud instance
// metadata services (169.254.0.0/16, 100.100.100.200, fd00:ec2::254,
// metadata.google.internal, etc.), link-local addresses (fe80::/10), or
// unspecified addresses (0.0.0.0, ::), and when
// `MEDIOS_FETCHER_DISALLOW_PRIVATE_IPS=true` is set in the environment, also
// rejects loopback and RFC1918 private IP literals (#386).
Option<Error> validateNetworkTarget(const URI& uri);
Option<Error> validateUrlTarget(const std::string& url);


// Validates that a local filesystem URI path is permitted by
// `--fetcher_allow_local_uris` (`MEDIOS_FETCHER_ALLOW_LOCAL_URIS`) and resides
// within the canonical (`realpath`) directory prefix configured by
// `--fetcher_allowed_local_uri_prefix` (`MEDIOS_FETCHER_ALLOWED_LOCAL_PREFIX`)
// (#399).
Option<Error> validateLocalSourcePath(
    const std::string& sourcePath,
    bool allowLocalUris,
    const Option<std::string>& allowedPrefix);

Option<Error> validateLocalSourcePath(const std::string& sourcePath);

} // namespace uri {
} // namespace medios {

#endif // __URI_UTILS_HPP__
