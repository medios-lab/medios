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

#include <sys/wait.h>

#include <string>
#include <tuple>
#include <vector>

#include <process/check.hpp>
#include <process/collect.hpp>
#include <process/io.hpp>
#include <process/subprocess.hpp>

#include <stout/path.hpp>
#include <stout/strings.hpp>

#include <stout/os/constants.hpp>
#include <stout/os/mkdir.hpp>

#include "uri/utils.hpp"

#include "uri/fetchers/curl.hpp"

namespace http = process::http;
namespace io = process::io;

using std::set;
using std::string;
using std::tuple;
using std::vector;

using process::await;
using process::subprocess;

using process::Failure;
using process::Future;
using process::Owned;
using process::Subprocess;

namespace medios {
namespace uri {

CurlFetcherPlugin::Flags::Flags()
{
  add(&Flags::curl_stall_timeout,
      "curl_stall_timeout",
      "Amount of time for the fetcher to wait before considering a download\n"
      "being too slow and abort it when the download stalls (i.e., the speed\n"
      "keeps below one byte per second).\n");
}


const char CurlFetcherPlugin::NAME[] = "curl";


Try<Owned<Fetcher::Plugin>> CurlFetcherPlugin::create(const Flags& flags)
{
  // TODO(jieyu): Make sure curl is available.

  return Owned<Fetcher::Plugin>(new CurlFetcherPlugin(flags));
}


set<string> CurlFetcherPlugin::schemes() const
{
  return {"http", "https"};
}


string CurlFetcherPlugin::name() const
{
  return NAME;
}


namespace {

constexpr int kMaxRedirects = 10;

Future<Nothing> fetchWithRedirects(
    const URI& originalUri,
    const string& currentUrl,
    const string& output,
    const Option<Duration>& stallTimeout,
    int redirectsRemaining)
{
  Option<Error> targetError = uri::validateUrlTarget(currentUrl);
  if (targetError.isSome()) {
    return Failure(targetError->message);
  }

  const string curl = "curl";

  vector<string> argv = {
    curl,
    "-s",                 // Don't show progress meter or error messages.
    "-S",                 // Makes curl show an error message if it fails.
    "--proto", "=http,https",
    "--proto-redir", "=http,https",
    "-w", "%{http_code}\n%{redirect_url}",
    "-o", output,         // Write output to the file.
    currentUrl
  };

  // Add a timeout for curl to abort when the download speed keeps low
  // (1 byte per second by default) for the specified duration. See:
  // https://curl.haxx.se/docs/manpage.html#-y
  if (stallTimeout.isSome()) {
    argv.push_back("-y");
    argv.push_back(
        std::to_string(static_cast<long>(stallTimeout->secs())));
  }

  Try<Subprocess> s = subprocess(
      curl,
      argv,
      Subprocess::PATH(os::DEV_NULL),
      Subprocess::PIPE(),
      Subprocess::PIPE());

  if (s.isError()) {
    return Failure("Failed to exec the curl subprocess: " + s.error());
  }

  return await(
      s->status(),
      io::read(s->out().get()),
      io::read(s->err().get()))
    .then([=](const tuple<
        Future<Option<int>>,
        Future<string>,
        Future<string>>& t) -> Future<Nothing> {
      const string redactedUri = stringify(uri::redact(originalUri));
      const Future<Option<int>>& status = std::get<0>(t);
      if (!status.isReady()) {
        return Failure(
            "Failed to get the exit status of the curl subprocess for '" +
            redactedUri + "': " +
            (status.isFailed() ? status.failure() : "discarded"));
      }

      if (status->isNone()) {
        return Failure("Failed to reap the curl subprocess for '" +
                       redactedUri + "'");
      }

      if (status->get() != 0) {
        const Future<string>& error = std::get<2>(t);
        if (!error.isReady()) {
          return Failure(
              "Failed to perform 'curl' for '" + redactedUri +
              "'. Reading stderr failed: " +
              (error.isFailed() ? error.failure() : "discarded"));
        }

        return Failure("Failed to perform 'curl' for '" + redactedUri +
                       "': " + error.get());
      }

      const Future<string>& stdoutData = std::get<1>(t);
      if (!stdoutData.isReady()) {
        return Failure(
            "Failed to read stdout from 'curl' for '" + redactedUri +
            "': " +
            (stdoutData.isFailed() ? stdoutData.failure() : "discarded"));
      }

      const vector<string> tokens = strings::tokenize(stdoutData.get(), "\n", 2);
      if (tokens.empty()) {
        return Failure("Unexpected output from 'curl' for '" + redactedUri +
                       "': " + stdoutData.get());
      }

      // Parse the output and get the HTTP response code.
      Try<int> code = numify<int>(tokens[0]);
      if (code.isError()) {
        return Failure("Unexpected output from 'curl' for '" + redactedUri +
                       "': " + stdoutData.get());
      }

      if (tokens.size() == 2 && !strings::trim(tokens[1]).empty()) {
        if (redirectsRemaining <= 0) {
          return Failure(
              "Exceeded maximum redirect depth (" +
              stringify(kMaxRedirects) + ") for '" + redactedUri + "' (#386)");
        }

        return fetchWithRedirects(
            originalUri,
            strings::trim(tokens[1]),
            output,
            stallTimeout,
            redirectsRemaining - 1);
      }

      if (code.get() != http::Status::OK) {
        return Failure("Unexpected HTTP response code for '" + redactedUri +
                       "': " + http::Status::string(code.get()));
      }

      return Nothing();
    });
}

} // namespace {


Future<Nothing> CurlFetcherPlugin::fetch(
    const URI& uri,
    const string& directory,
    const Option<string>& data,
    const Option<string>& outputFileName) const
{
  // TODO(jieyu): Validate the given URI.

  if (!uri.has_path()) {
    return Failure("URI path is not specified");
  }

  Option<Error> targetError = uri::validateNetworkTarget(uri);
  if (targetError.isSome()) {
    return Failure(targetError->message);
  }

  Try<Nothing> mkdir = os::mkdir(directory);
  if (mkdir.isError()) {
    return Failure(
        "Failed to create directory '" +
        directory + "': " + mkdir.error());
  }

  string output;
  if (outputFileName.isSome()) {
    output = path::join(directory, outputFileName.get());
  } else {
    output = path::join(directory, Path(path::from_uri(uri.path())).basename());
  }

  return fetchWithRedirects(
      uri,
      strings::trim(stringify(uri)),
      output,
      flags.curl_stall_timeout,
      kMaxRedirects);
}

} // namespace uri {
} // namespace medios {
