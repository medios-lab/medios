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

#include <string>
#include <tuple>
#include <vector>

#include <process/collect.hpp>
#include <process/dispatch.hpp>
#include <process/http.hpp>
#include <process/id.hpp>
#include <process/io.hpp>
#include <process/once.hpp>
#include <process/process.hpp>
#include <process/subprocess.hpp>

#include <stout/none.hpp>
#include <stout/option.hpp>
#include <stout/strings.hpp>

#include <stout/os/constants.hpp>
#include <stout/os/exec.hpp>
#include <stout/os/getenv.hpp>
#include <stout/os/kill.hpp>
#include <stout/os/mkdir.hpp>
#include <stout/os/write.hpp>
#include <stout/os/os.hpp>

#include <medios/docker/spec.hpp>

#include "uri/utils.hpp"

#include "uri/fetchers/docker.hpp"

#include "uri/schemes/docker.hpp"
#include "uri/schemes/http.hpp"

namespace http = process::http;
namespace io = process::io;
namespace spec = docker::spec;

using std::set;
using std::string;
using std::tuple;
using std::vector;

using process::dispatch;
using process::spawn;
using process::terminate;
using process::wait;

using process::Failure;
using process::Future;
using process::Owned;
using process::Process;
using process::Subprocess;

namespace medios {
namespace uri {

//-------------------------------------------------------------------
// Helper and utility functions.
//-------------------------------------------------------------------

// Returns the set of schemes supported by this URI fetcher plugin.
static set<string> schemes()
{
  return {
    "docker",           // Fetch image manifest and blobs.
    "docker-manifest",  // Fetch image manifest only.
    "docker-blob"       // Fetch a single image blob.
  };
}

void commandDiscarded(const Subprocess& s, const string& cmd)
{
  if (s.status().isPending()) {
    VLOG(1) << "'" << cmd << "' is being discarded";
    os::kill(s.pid(), SIGKILL);
  }
}


// TODO(jieyu): Move the following curl based utility functions to a
// command utils common directory.

constexpr int kMaxRedirects = 10;

// Uses the curl command to send an HTTP request to the given URL and
// returns the HTTP response it received. HTTP 3xx redirects are followed
// hop-by-hop so every redirect target is validated against
// `uri::validateUrlTarget` (#386). The returned HTTP response will have
// the type 'BODY' (no streaming).
static Future<http::Response> curl(
    const string& uri,
    const http::Headers& headers,
    const Option<Duration>& stallTimeout,
    int redirectsRemaining = kMaxRedirects)
{
  Option<Error> targetError = uri::validateUrlTarget(uri);
  if (targetError.isSome()) {
    return Failure(targetError->message);
  }

  static process::Once* initialized = new process::Once();
  static bool http11 = false;

  if (!initialized->once()) {
    // Test if curl supports locking into HTTP 1.1. We do this as
    // HTTP 1.1 is more likely than HTTP 1.0 to function accross all
    // infrastructures. The '--http1.1' flag got added to curl with
    // with version 7.33.0. Some supported distributions do still come
    // with curl version 7.19.0. See MESOS-8907.
    http11 = os::system("curl --http1.1 -V > /dev/null 2>&1") == 0;
    VLOG(1) << "Curl accepts --http1.1 flag: " << stringify(http11);
    initialized->done();
  }

  vector<string> argv = {
    "curl",
    "-s",       // Don't show progress meter or error messages.
    "-S",       // Make curl show an error message if it fails.
    "--proto", "=http,https",
    "--proto-redir", "=http,https",
    "-i",       // Include the HTTP-header in the output.
    "--raw"     // Disable HTTP decoding of content or transfer encodings.
  };

  // Make sure curl does not enforce HTTP 2 as our HTTP parser does
  // currently not support that. See MESOS-8368.
  // Older curl versions do not support the HTTP 1.1 flag, but these
  // versions are also old enough to not default to HTTP/2.
  if (http11) {
    argv.push_back("--http1.1");
  }

  // Add additional headers.
  foreachpair (const string& key, const string& value, headers) {
    argv.push_back("-H");
    argv.push_back(key + ": " + value);
  }

  // Add a timeout for curl to abort when the download speed keeps low
  // (1 byte per second by default) for the specified duration. See:
  // https://curl.haxx.se/docs/manpage.html#-y
  if (stallTimeout.isSome()) {
    argv.push_back("-y");
    argv.push_back(std::to_string(static_cast<long>(stallTimeout->secs())));
  }

  argv.push_back(strings::trim(uri));

  string cmd = strings::join(" ", argv);

  Try<Subprocess> s = subprocess(
      "curl",
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
        Future<string>>& t) -> Future<http::Response> {
      const Future<Option<int>>& status = std::get<0>(t);
      if (!status.isReady()) {
        return Failure(
            "Failed to get the exit status of the curl subprocess for '" +
            uri + "': " + (status.isFailed() ? status.failure() : "discarded"));
      }

      if (status->isNone()) {
        return Failure("Failed to reap the curl subprocess for '" + uri + "'");
      }

      if (status->get() != 0) {
        const Future<string>& error = std::get<2>(t);
        if (!error.isReady()) {
          return Failure(
              "Failed to perform 'curl' for '" + uri +
              "'. Reading stderr failed: " +
              (error.isFailed() ? error.failure() : "discarded"));
        }

        return Failure("Failed to perform 'curl' for '" + uri +
                       "': " + error.get());
      }

      const Future<string>& output = std::get<1>(t);
      if (!output.isReady()) {
        return Failure(
            "Failed to read stdout from 'curl' for '" + uri + "': " +
            (output.isFailed() ? output.failure() : "discarded"));
      }

      // Decode HTTP responses.
      Try<vector<http::Response>> responses =
        http::decodeResponses(output.get());

      // TODO(nfnt): If we're behing a proxy, curl will use 'HTTP
      // CONNECT tunneling' to access HTTPS. The HTTP parser will
      // put the actual response(s) to the body of the 'CONNECT'
      // response. Therefore, in that case, we'll parse the body of
      // 'CONNECT' response again. See MESOS-6010 for more details.
      bool hasProxy =
        os::getenv("https_proxy").isSome() ||
        os::getenv("HTTPS_PROXY").isSome();

      if (hasProxy && responses.isSome() && responses->size() == 1) {
        const http::Response& response = responses->back();

        if (response.code == 200 &&
            !response.headers.contains("Content-Length") &&
            response.headers.get("Transfer-Encoding") != Some("chunked")) {
          responses = http::decodeResponses(response.body);
        }
      }

      if (responses.isError()) {
        return Failure(
            "Failed to decode HTTP responses: " + responses.error() +
            "\n" + output.get());
      }

      const http::Response& response = responses->back();
      if (response.code >= 300 && response.code < 400 &&
          response.headers.contains("Location")) {
        if (redirectsRemaining <= 0) {
          return Failure(
              "Exceeded maximum redirect depth (" +
              stringify(kMaxRedirects) + ") for '" + uri + "' (#386)");
        }

        string redirectUrl = strings::trim(response.headers.at("Location"));
        if (strings::startsWith(redirectUrl, "/")) {
          const string trimmedUri = strings::trim(uri);
          const size_t schemeEnd = trimmedUri.find("://");
          if (schemeEnd != string::npos) {
            const size_t pathStart =
              trimmedUri.find_first_of("/?#", schemeEnd + 3);
            const string origin = pathStart == string::npos
              ? trimmedUri
              : trimmedUri.substr(0, pathStart);
            redirectUrl = origin + redirectUrl;
          }
        }

        // Preserve non-credential headers (such as `Accept`), and only keep
        // `Authorization` when redirecting within the same origin.
        http::Headers redirectHeaders = headers;
        const string trimmedUri = strings::trim(uri);
        const size_t origSchemeEnd = trimmedUri.find("://");
        const size_t redirSchemeEnd = redirectUrl.find("://");
        if (origSchemeEnd != string::npos && redirSchemeEnd != string::npos) {
          const size_t origPathStart =
            trimmedUri.find_first_of("/?#", origSchemeEnd + 3);
          const size_t redirPathStart =
            redirectUrl.find_first_of("/?#", redirSchemeEnd + 3);
          const string origOrigin = origPathStart == string::npos
            ? trimmedUri
            : trimmedUri.substr(0, origPathStart);
          const string redirOrigin = redirPathStart == string::npos
            ? redirectUrl
            : redirectUrl.substr(0, redirPathStart);
          if (strings::lower(origOrigin) != strings::lower(redirOrigin)) {
            redirectHeaders.erase("Authorization");
          }
        } else {
          redirectHeaders.erase("Authorization");
        }

        return curl(
            redirectUrl,
            redirectHeaders,
            stallTimeout,
            redirectsRemaining - 1);
      }

      return response;
    })
    .onDiscard(lambda::bind(&commandDiscarded, s.get(), cmd));
}


static Future<http::Response> curl(
    const URI& uri,
    const http::Headers& headers,
    const Option<Duration>& stallTimeout)
{
  return curl(stringify(uri), headers, stallTimeout);
}


// TODO(jieyu): Add a comment here.
static Future<int> download(
    const string& uri,
    const string& blobPath,
    const http::Headers& headers,
    const Option<Duration>& stallTimeout,
    int redirectsRemaining = kMaxRedirects)
{
  Option<Error> targetError = uri::validateUrlTarget(uri);
  if (targetError.isSome()) {
    return Failure(targetError->message);
  }

  vector<string> argv = {
    "curl",
    "-s",                 // Don't show progress meter or error messages.
    "-S",                 // Make curl show an error message if it fails.
    "--proto", "=http,https",
    "--proto-redir", "=http,https",
    "-w", "%{http_code}\n%{redirect_url}", // Display HTTP response code and the redirected URL. // NOLINT(whitespace/line_length)
    "-o", blobPath        // Write output to the file.
  };

  // Add additional headers.
  foreachpair (const string& key, const string& value, headers) {
    argv.push_back("-H");
    argv.push_back(key + ": " + value);
  }

  // Add a timeout for curl to abort when the download speed keeps below
  // 1 byte per second. See: https://curl.haxx.se/docs/manpage.html#-y
  if (stallTimeout.isSome()) {
    argv.push_back("-y");
    argv.push_back(std::to_string(static_cast<long>(stallTimeout->secs())));
  }

  argv.push_back(uri);

  string cmd = strings::join(" ", argv);

  Try<Subprocess> s = subprocess(
      "curl",
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
        Future<string>>& t) -> Future<int> {
      const Future<Option<int>>& status = std::get<0>(t);
      if (!status.isReady()) {
        return Failure(
            "Failed to get the exit status of the curl subprocess for '" + uri +
            "': " + (status.isFailed() ? status.failure() : "discarded"));
      }

      if (status->isNone()) {
        return Failure("Failed to reap the curl subprocess for '" + uri + "'");
      }

      if (status->get() != 0) {
        const Future<string>& error = std::get<2>(t);
        if (!error.isReady()) {
          return Failure(
              "Failed to perform 'curl' for '" + uri +
              "'. Reading stderr failed: " +
              (error.isFailed() ? error.failure() : "discarded"));
        }

        return Failure("Failed to perform 'curl' for '" + uri +
                       "': " + error.get());
      }

      const Future<string>& output = std::get<1>(t);
      if (!output.isReady()) {
        return Failure(
            "Failed to read stdout from 'curl' for '" + uri + "': " +
            (output.isFailed() ? output.failure() : "discarded"));
      }

      vector<string> tokens = strings::tokenize(output.get(), "\n", 2);
      if (tokens.empty()) {
        return Failure("Unexpected 'curl' output for '" + uri +
                       "': " + output.get());
      }

      // Parse the output and get the HTTP response code.
      Try<int> code = numify<int>(tokens[0]);
      if (code.isError()) {
        return Failure("Unexpected HTTP response code from 'curl' for '" + uri +
                       "': " + tokens[0]);
      }

      // If there are two tokens, it means that the redirect url
      // exists in the stdout and the request to download the blob
      // is already authenticated.
      if (tokens.size() == 2 && !strings::trim(tokens[1]).empty()) {
        if (redirectsRemaining <= 0) {
          return Failure(
              "Exceeded maximum redirect depth (" +
              stringify(kMaxRedirects) + ") for '" + uri + "' (#386)");
        }

        // Headers are not attached because the request is already
        // authenticated.
        return download(
            strings::trim(tokens[1]),
            blobPath,
            http::Headers(),
            stallTimeout,
            redirectsRemaining - 1);
      }

      return code.get();
    })
    .onDiscard(lambda::bind(&commandDiscarded, s.get(), cmd));
}


static Future<int> download(
    const URI& uri,
    const string& url,
    const string& directory,
    const http::Headers& headers,
    const Option<Duration>& stallTimeout)
{
  string blobSum;

  auto lastSlash = uri.path().find_last_of('/');
  if (lastSlash == string::npos) {
    blobSum = uri.path();
  } else {
    blobSum = uri.path().substr(lastSlash + 1);
  }

  return download(
      url,
      DockerFetcherPlugin::getBlobPath(directory, blobSum),
      headers,
      stallTimeout);
}


// Returns the 'Basic' credential as a header for pulling an image
// from a registry, if the host of the image's repository exists in
// the docker config file, or empty if there is none.
static http::Headers getAuthHeaderBasic(
    const URI& uri,
    const hashmap<string, spec::Config::Auth>& auths)
{
  http::Headers headers;

  // NOTE: The host field of uri can be either domain or IP
  // address, which is merged in docker registry puller.
  const string registry = uri.has_port()
    ? uri.host() + ":" + stringify(uri.port())
    : uri.host();

  foreachpair (const string& key, const spec::Config::Auth& value, auths) {
    // Handle domains including 'docker.io' as a special case,
    // because the url is set differently for different version
    // of docker default registry, but all of them should depend
    // on the same default namespace 'docker.io'. Please see:
    // https://github.com/docker/docker/blob/master/registry/config.go#L34
    const bool isDocker =
      strings::contains(uri.host(), "docker.io") &&
      strings::contains(key, "docker.io");

    // Should not use 'http::URL::parse()' here, since many
    // registry domain recorded in docker config file does
    // not start with 'https://' or 'http://'. They are pure
    // domain only (e.g., 'quay.io', 'localhost:5000').
    // Please see 'ResolveAuthConfig()' in:
    // https://github.com/docker/docker/blob/master/registry/auth.go
    if (isDocker || (registry == spec::parseAuthUrl(key))) {
      if (value.has_auth()) {
        headers["Authorization"] = "Basic " + value.auth();
        break;
      }
    }
  }

  return headers;
}


static http::Headers getAuthHeaderBearer(
    const Option<string>& authToken)
{
  http::Headers headers;

  if (authToken.isSome()) {
    headers["Authorization"] = "Bearer " + authToken.get();
  }

  return headers;
}


static URI constructRegistryUri(const URI& imageUri, string&& path)
{
  const string scheme = imageUri.has_fragment() ? imageUri.fragment() : "https";
  return uri::construct(
      scheme,
      std::move(path),
      imageUri.host(),
      (imageUri.has_port() ? Option<int>(imageUri.port()) : None()));
}


static URI getRegistryRootUri(const URI& imageUri)
{
  return constructRegistryUri(imageUri, "/v2");
}


static URI getManifestUri(const URI& imageUri)
{
  return constructRegistryUri(
      imageUri,
      strings::join(
          "/", "/v2", imageUri.path(), "manifests", imageUri.query()));
}


static URI getBlobUri(const URI& imageUri)
{
  return constructRegistryUri(
      imageUri,
      strings::join("/", "/v2", imageUri.path(), "blobs", imageUri.query()));
}


// Validates that the response contains WWW-Authenticate header with
// a scheme BEARER and, if so, extracts parameters from the header.
// Otherwise, returns Error.
static Try<hashmap<string, string>> getBearerAuthParam(
    const URI& uri,
    const http::Response& response)
{
  Result<http::header::WWWAuthenticate> header =
    response.headers.get<http::header::WWWAuthenticate>();

  if (header.isError()) {
    return Error(
        "Failed to get WWW-Authenticate header from " + stringify(uri) +
        ": " + header.error());
  } else if (header.isNone()) {
    return Error(
        "Got unexpected empty WWW-Authenticate header from " + stringify(uri));
  }

  // According to RFC, auth scheme should be case insensitive.
  const string authScheme = strings::upper(header->authScheme());
  if (authScheme == "BASIC"){
    return Error(
        "Got unexpected BASIC Authorization response status: " +
        response.status + " from " + stringify(uri));
  }

  if (authScheme != "BEARER") {
    return Error(
        "Got unsupported auth-scheme: " + authScheme +
        " from " + stringify(uri));
  }

  return header->authParam();
}


//-------------------------------------------------------------------
// DockerFetcherPlugin implementation.
//-------------------------------------------------------------------

class DockerFetcherPluginProcess : public Process<DockerFetcherPluginProcess>
{
public:
  DockerFetcherPluginProcess(
      const hashmap<string, spec::Config::Auth>& _auths,
      const Option<Duration>& _stallTimeout,
      bool _enableAuthServiceUriFallback)
    : ProcessBase(process::ID::generate("docker-fetcher-plugin")),
      auths(_auths),
      stallTimeout(_stallTimeout),
      enableAuthServiceUriFallback(_enableAuthServiceUriFallback) {}

  Future<Nothing> fetch(
      const URI& uri,
      const string& directory,
      const Option<string>& data);

private:
  Future<Nothing> _fetch(
      const URI& uri,
      const string& directory,
      const URI& manifestUri,
      const http::Headers& manifestHeaders,
      const http::Headers& basicAuthHeaders,
      const http::Response& response);

  Future<Nothing> __fetch(
      const URI& uri,
      const string& directory,
      const http::Headers& authHeaders,
      const http::Response& response);

  // Picks the entry for this node's platform out of a manifest list or
  // OCI index and fetches that manifest by digest.
  Future<Nothing> fetchFromManifestList(
      const URI& uri,
      const string& directory,
      const http::Headers& authHeaders,
      const string& body);

  Future<Nothing> fetchBlobs(
      const URI& uri,
      const string& directory,
      const hashset<string>& digests,
      const http::Headers& authHeaders);

  Future<Nothing> fetchBlob(
      const URI& uri,
      const string& directory,
      const http::Headers& authHeaders);

  Future<Nothing> _fetchBlob(
      const URI& uri,
      const string& directory,
      const URI& blobUri,
      const http::Headers& basicAuthHeaders);


  Future<string> getAuthServiceUri(
      const string& repository,
      const URI& initialUri,
      const http::Response& initialResponse,
      const http::Headers& basicAuthHeaders) const;

  // Returns a token-based authorization header. Basic authorization
  // header may be required to get a proper authorization token.
  Future<http::Headers> getAuthHeader(
      const string& repository,
      const URI& uri,
      const http::Headers& basicAuthHeaders,
      const http::Response& response);

  // This is a lookup table for credentials in docker config file,
  // keyed by registry URL.
  // For example, "https://index.docker.io/v1/" -> spec::Config::Auth
  hashmap<string, spec::Config::Auth> auths;

  // Timeout for curl to wait when a net download stalls.
  const Option<Duration> stallTimeout;

  // Disables auth server URI generation (see MESOS-10092).
  // Used for tests only.
  const bool enableAuthServiceUriFallback;
};


DockerFetcherPlugin::Flags::Flags()
{
  add(&Flags::docker_config,
      "docker_config",
      "The default docker config file.");

  add(&Flags::docker_stall_timeout,
      "docker_stall_timeout",
      "Amount of time for the fetcher to wait before considering a download\n"
      "being too slow and abort it when the download stalls (i.e., the speed\n"
      "keeps below one byte per second).");
}


const char DockerFetcherPlugin::NAME[] = "docker";


Try<Owned<Fetcher::Plugin>> DockerFetcherPlugin::create(
    const Flags& flags,
    bool enableAuthServiceUriFallback)
{
  // TODO(jieyu): Make sure curl is available.

  hashmap<string, spec::Config::Auth> auths;
  if (flags.docker_config.isSome()) {
    Try<hashmap<string, spec::Config::Auth>> cachedAuths =
      spec::parseAuthConfig(flags.docker_config.get());

    if (cachedAuths.isError()) {
      return Error("Failed to parse docker config: " + cachedAuths.error());
    }

    auths = cachedAuths.get();
  }

  Owned<DockerFetcherPluginProcess> process(new DockerFetcherPluginProcess(
      hashmap<string, spec::Config::Auth>(auths),
      flags.docker_stall_timeout,
      enableAuthServiceUriFallback));

  return Owned<Fetcher::Plugin>(new DockerFetcherPlugin(process));
}


string DockerFetcherPlugin::getBlobPath(
    const string& directory,
    const string& blobSum)
{
  return path::join(directory, blobSum);
}


DockerFetcherPlugin::DockerFetcherPlugin(
    Owned<DockerFetcherPluginProcess> _process)
  : process(_process)
{
  spawn(CHECK_NOTNULL(process.get()));
}


DockerFetcherPlugin::~DockerFetcherPlugin()
{
  terminate(process.get());
  wait(process.get());
}


set<string> DockerFetcherPlugin::schemes() const
{
  // Use uri:: prefix to disambiguate.
  return uri::schemes();
}


string DockerFetcherPlugin::name() const
{
  return NAME;
}


Future<Nothing> DockerFetcherPlugin::fetch(
    const URI& uri,
    const string& directory,
    const Option<string>& data,
    const Option<string>& outputFileName) const
{
  return dispatch(
      process.get(),
      &DockerFetcherPluginProcess::fetch,
      uri,
      directory,
      data);
}


Future<Nothing> DockerFetcherPluginProcess::fetch(
    const URI& uri,
    const string& directory,
    const Option<string>& data)
{
  // TODO(gilbert): Convert the `uri` to ::docker::spec::ImageReference
  // and pass it all the way down to avoid the complicated URI conversion
  // and make the code more readable.
  if (schemes().count(uri.scheme()) == 0) {
    return Failure(
        "Docker fetcher plugin does not support "
        "'" + uri.scheme() + "' URI scheme");
  }

  if (!uri.has_host()) {
    return Failure("Registry host (uri.host) is not specified");
  }

  Option<Error> targetError = uri::validateNetworkTarget(uri);
  if (targetError.isSome()) {
    return Failure(targetError->message);
  }

  if (!uri.has_query()) {
    return Failure("Image tag/digest (uri.query) is not specified");
  }

  Try<Nothing> mkdir = os::mkdir(directory);
  if (mkdir.isError()) {
    return Failure(
        "Failed to create directory '" +
        directory + "': " + mkdir.error());
  }

  hashmap<string, spec::Config::Auth> _auths;

  // 'data' is expected as a docker config in JSON format.
  if (data.isSome()) {
    Try<hashmap<string, spec::Config::Auth>> secretAuths =
      spec::parseAuthConfig(data.get());

    if (secretAuths.isError()) {
      return Failure("Failed to parse docker config: " + secretAuths.error());
    }

    _auths = secretAuths.get();
  }

  // The 'secretAuths' takes the precedence over the default auths.
  _auths.insert(auths.begin(), auths.end());

  // Use the 'Basic' credential to pull the manifest/blob by default.
  http::Headers basicAuthHeaders = getAuthHeaderBasic(uri, _auths);

  if (uri.scheme() == "docker-blob") {
    return fetchBlob(uri, directory, basicAuthHeaders);
  }

  URI manifestUri = getManifestUri(uri);

  // Both docker manifest v2s1 and v2s2 are supported. We put all
  // accept headers to the curl request for manifest because:
  // 1. v2+json is needed since some registries start to deprecate
  //    schema 1 support.
  // 2. Some registries only support one schema type.
  http::Headers manifestHeaders = {
    {"Accept",
     "application/vnd.docker.distribution.manifest.list.v2+json,"
     "application/vnd.oci.image.index.v1+json,"
     "application/vnd.docker.distribution.manifest.v2+json,"
     "application/vnd.oci.image.manifest.v1+json,"
     "application/vnd.docker.distribution.manifest.v1+json,"
     "application/vnd.docker.distribution.manifest.v1+prettyjws"
    }
  };

  return curl(manifestUri, manifestHeaders + basicAuthHeaders, stallTimeout)
    .then(defer(self(),
                &Self::_fetch,
                uri,
                directory,
                manifestUri,
                manifestHeaders,
                basicAuthHeaders,
                lambda::_1));
}


Future<Nothing> DockerFetcherPluginProcess::_fetch(
    const URI& uri,
    const string& directory,
    const URI& manifestUri,
    const http::Headers& manifestHeaders,
    const http::Headers& basicAuthHeaders,
    const http::Response& response)
{
  if (response.code == http::Status::UNAUTHORIZED) {
    // Use the 'Basic' credential to request an auth token by default.
    return getAuthHeader(uri.path(), manifestUri, basicAuthHeaders, response)
      .then(defer(self(), [=](
          const http::Headers& authHeaders) -> Future<Nothing> {
        return curl(manifestUri, manifestHeaders + authHeaders, stallTimeout)
          .then(defer(self(),
                      &Self::__fetch,
                      uri,
                      directory,
                      authHeaders,
                      lambda::_1));
      }));
  }

  return __fetch(uri, directory, basicAuthHeaders, response);
}


Future<Nothing> DockerFetcherPluginProcess::__fetch(
    const URI& uri,
    const string& directory,
    const http::Headers& authHeaders,
    const http::Response& response)
{
  if (response.code != http::Status::OK) {
    return Failure(
        "Unexpected HTTP response '" + response.status + "' "
        "when trying to get the manifest");
  }

  CHECK_EQ(response.type, http::Response::BODY);

  Option<string> contentType = response.headers.get("Content-Type");
  if (contentType.isNone()) {
    return Failure("No Content-Type present");
  }

  // NOTE: Docker supports the following five media types.
  //
  // V2 schema 1 manifest:
  // 1. application/vnd.docker.distribution.manifest.v1+json
  // 2. application/vnd.docker.distribution.manifest.v1+prettyjws
  // 3. application/json
  //
  // For more details, see:
  // https://docs.docker.com/registry/spec/manifest-v2-1/
  //
  // V2 schema 2 manifest:
  // 1. application/vnd.docker.distribution.manifest.v2+json
  // 2. application/vnd.docker.distribution.manifest.list.v2+json
  //
  // OCI equivalents of the two above:
  // 1. application/vnd.oci.image.manifest.v1+json
  // 2. application/vnd.oci.image.index.v1+json
  //
  // A list or index is resolved to the single manifest for this node's
  // platform, which is then handled as schema 2.
  //
  // For more details, see:
  // https://docs.docker.com/registry/spec/manifest-v2-2/
  bool isV2Schema1 =
    strings::startsWith(
        contentType.get(),
        "application/vnd.docker.distribution.manifest.v1") ||
    strings::startsWith(
        contentType.get(),
        "application/json");

  bool isV2Schema2 =
    contentType.get() ==
      "application/vnd.docker.distribution.manifest.v2+json" ||
    contentType.get() == "application/vnd.oci.image.manifest.v1+json";

  bool isManifestList =
    contentType.get() ==
      "application/vnd.docker.distribution.manifest.list.v2+json" ||
    contentType.get() == "application/vnd.oci.image.index.v1+json";

  if (isManifestList) {
    return fetchFromManifestList(uri, directory, authHeaders, response.body);
  }

  if (isV2Schema1) {
    // Parse V2 schema 1 image manifest.
    Try<spec::v2::ImageManifest> manifest = spec::v2::parse(response.body);
    if (manifest.isError()) {
      return Failure(
          "Failed to parse the V2 Schema 1 image manifest: " +
          manifest.error());
    }

    // Save manifest to 'directory'.
    Try<Nothing> write = os::write(
        path::join(directory, "manifest"), response.body);

    if (write.isError()) {
      return Failure(
          "Failed to write the V2 Schema 1 image manifest to "
          "'" + directory + "': " + write.error());
    }

    // No need to proceed if we only want manifest.
    if (uri.scheme() == "docker-manifest") {
      return Nothing();
    }

    hashset<string> digests;
    for (int i = 0; i < manifest->fslayers_size(); i++) {
      digests.insert(manifest->fslayers(i).blobsum());
    }

    return fetchBlobs(uri, directory, digests, authHeaders);
  } else if (isV2Schema2) {
    // Parse V2 schema 2 manifest.
    Try<spec::v2_2::ImageManifest> manifest =
      spec::v2_2::parse(response.body);

    if (manifest.isError()) {
      return Failure(
          "Failed to parse the V2 Schema 2 image manifest: " +
          manifest.error());
    }

    // Save manifest to 'directory'.
    Try<Nothing> write = os::write(
        path::join(directory, "manifest"), response.body);

    if (write.isError()) {
      return Failure(
          "Failed to write the V2 Schema 2 image manifest to "
          "'" + directory + "': " + write.error());
    }

    // No need to proceed if we only want manifest.
    if (uri.scheme() == "docker-manifest") {
      return Nothing();
    }

    hashset<string> digests{manifest->config().digest()};
    for (int i = 0; i < manifest->layers_size(); i++) {
      digests.insert(manifest->layers(i).digest());
    }

    // TODO(gilbert): Verify the digest after contents are fetched.
    return fetchBlobs(uri, directory, digests, authHeaders);
  }

  return Failure("Unsupported manifest MIME type: " + contentType.get());
}


Future<Nothing> DockerFetcherPluginProcess::fetchFromManifestList(
    const URI& uri,
    const string& directory,
    const http::Headers& authHeaders,
    const string& body)
{
  Try<spec::v2_2::ManifestList> list = spec::v2_2::parseManifestList(body);
  if (list.isError()) {
    return Failure("Failed to parse the manifest list: " + list.error());
  }

  Try<os::UTSInfo> uts = os::uname();
  if (uts.isError()) {
    return Failure("Failed to read the node's machine name: " + uts.error());
  }

  Try<string> architecture =
    spec::v2_2::architectureFromMachine(uts->machine);

  if (architecture.isError()) {
    return Failure(
        "Cannot select a platform from the manifest list of '" +
        stringify(uri) + "': " + architecture.error());
  }

  Try<string> digest =
    spec::v2_2::selectManifest(list.get(), "linux", architecture.get());

  if (digest.isError()) {
    return Failure(
        "Image '" + stringify(uri) + "' has no manifest for this node: " +
        digest.error());
  }

  LOG(INFO) << "Selected manifest " << digest.get() << " for linux/"
            << architecture.get() << " from the manifest list of '"
            << uri << "'";

  // The same image URI with the digest in place of the tag.
  URI manifestUri = uri;
  manifestUri.set_query(digest.get());
  manifestUri = getManifestUri(manifestUri);

  http::Headers manifestHeaders = {
    {"Accept",
     "application/vnd.docker.distribution.manifest.v2+json,"
     "application/vnd.oci.image.manifest.v1+json"
    }
  };

  return curl(manifestUri, manifestHeaders + authHeaders, stallTimeout)
    .then(defer(self(),
                &Self::__fetch,
                uri,
                directory,
                authHeaders,
                lambda::_1));
}


Future<Nothing> DockerFetcherPluginProcess::fetchBlobs(
    const URI& uri,
    const string& directory,
    const hashset<string>& digests,
    const http::Headers& authHeaders)
{
  vector<Future<Nothing>> futures;

  foreach (const string& digest, digests) {
    URI blob = uri::docker::blob(
        uri.path(),                         // The 'repository'.
        digest,                             // The 'digest'.
        uri.host(),                         // The 'registry'.
        (uri.has_fragment()                 // The 'scheme'.
          ? Option<string>(uri.fragment())
          : None()),
        (uri.has_port()                     // The 'port'.
          ? Option<int>(uri.port())
          : None()));

    futures.push_back(fetchBlob(blob, directory, authHeaders));
  }

  return collect(futures)
    .then([]() -> Future<Nothing> { return Nothing(); });
}


Future<Nothing> DockerFetcherPluginProcess::fetchBlob(
    const URI& uri,
    const string& directory,
    const http::Headers& authHeaders)
{
  URI blobUri = getBlobUri(uri);

  return download(
      blobUri,
      strings::trim(stringify(blobUri)),
      directory,
      authHeaders,
      stallTimeout)
    .then(defer(self(), [=](int code) -> Future<Nothing> {
      if (code == http::Status::UNAUTHORIZED) {
        // If we get a '401 Unauthorized', we assume that 'authHeaders'
        // is either empty or contains the 'Basic' credential, and we
        // can use it to request an auth token.
        // TODO(chhsiao): What if 'authHeaders' has an expired token?
        return _fetchBlob(uri, directory, blobUri, authHeaders);
      }

      if (code == http::Status::OK) {
        return Nothing();
      }

      return Failure(
          "Unexpected HTTP response '" + http::Status::string(code) + "' "
          "when trying to download the blob");
    }));
}


Future<Nothing> DockerFetcherPluginProcess::_fetchBlob(
    const URI& uri,
    const string& directory,
    const URI& blobUri,
    const http::Headers& basicAuthHeaders)
{
  // TODO(jieyu): This extra 'curl' call can be avoided if we can get
  // HTTP headers from 'download'. Currently, 'download' only returns
  // the HTTP response code because we don't support parsing HTTP
  // headers alone. Revisit this once that's supported.
  return curl(blobUri, basicAuthHeaders, stallTimeout)
    .then(defer(self(), [=](const http::Response& response) -> Future<Nothing> {
      // We expect a '401 Unauthorized' response here since the
      // 'download' with the same URI returns a '401 Unauthorized'.
      if (response.code != http::Status::UNAUTHORIZED) {
        return Failure(
          "Expecting a '401 Unauthorized' response when fetching a blob, "
          "but get '" + response.status + "' instead");
      }

      return getAuthHeader(uri.path(), blobUri, basicAuthHeaders, response)
        .then(defer(self(), [=](
            const http::Headers& authHeaders) -> Future<Nothing> {
          return download(
              blobUri,
              strings::trim(stringify(blobUri)),
              directory,
              authHeaders,
              stallTimeout)
            .then(defer(self(), [=](int code) -> Future<Nothing> {
              if (code == http::Status::OK) {
                return Nothing();
              }

              return Failure(
                  "Unexpected HTTP response '" + http::Status::string(code) +
                  "' when trying to download blob '" +
                  strings::trim(stringify(blobUri)) +
                  "' with schema 1 manifest");
            }));
        }));
    }));
}



// Tries to obtain URI of the V2 authorization service based on the
// "realm", "scope" and "service" auth params from the
// initial "401 unauthorized" response of the Docker V2 registry
// (see details in https://docs.docker.com/registry/spec/auth/token).
//
// If any of the params are missing from the "WWW-Authenticate" header, this
// function falls back to the scheme implemented in Docker image puller (see
// MESOS-10092): it queries the registry root URI (as opposed to manifest/blob
// URI) to get the "WWW-Authenticate" header with "realm", and composes the
// repository scope on its own. Scope grammar and semantics are documented in
// https://docs.docker.com/registry/spec/auth/scope .
Future<string> DockerFetcherPluginProcess::getAuthServiceUri(
    const string& repository,
    const URI& initialUri,
    const http::Response& initialResponse,
    const http::Headers& basicAuthHeaders) const
{
  const Try<hashmap<string, string>> authParam =
    getBearerAuthParam(initialUri, initialResponse);

  if (authParam.isError()) {
    LOG(WARNING) << authParam.error();
    return Failure(authParam.error());
  }

  // `authParam` is supposed to contain the 'realm', 'service'
  // and 'scope' information for bearer authentication.
  if (authParam->contains("realm") &&
      authParam->contains("service") &&
      authParam->contains("scope")) {
    Option<Error> realmError = uri::validateUrlTarget(authParam->at("realm"));
    if (realmError.isSome()) {
      return Failure(realmError->message);
    }

    // TODO(jieyu): Currently, we don't expect the auth server to return
    // a service or a scope that needs encoding.
    return authParam->at("realm") + "?" +
      "service=" + authParam->at("service") + "&" +
      "scope=" + authParam->at("scope");
  }

  const string msg =
    "Missing 'realm', 'service' or 'scope' in header WWW-Authenticate: " +
    initialResponse.headers.at("WWW-Authenticate");

  if (!enableAuthServiceUriFallback) {
    return Failure(msg);
  }

  LOG(WARNING) << msg;

  const URI registryRootUri = getRegistryRootUri(initialUri);
  return curl(registryRootUri, basicAuthHeaders, stallTimeout)
    .then([repository, registryRootUri](const http::Response& rootResponse)
      -> Future<string> {
      const Try<hashmap<string, string>> authParam =
        getBearerAuthParam(registryRootUri, rootResponse);

      if (authParam.isError()) {
        LOG(WARNING) << authParam.error();
        return Failure(authParam.error());
      }

      if (!authParam->contains("realm")) {
        return Failure(
            "Missing 'realm' in WWW-Authenticate header obtained from " +
            stringify(registryRootUri));
      }

      Option<Error> realmError = uri::validateUrlTarget(authParam->at("realm"));
      if (realmError.isSome()) {
        return Failure(realmError->message);
      }

      return authParam->at("realm") + "?scope=repository:" + repository +
             ":pull";
    });
}


// If a '401 Unauthorized' response is received and the auth-scheme
// is 'Bearer', we expect a header 'Www-Authenticate' containing the
// auth server information. We extract the auth server information
// from the auth-param, and then contacts the auth server to get the
// token. The token will then be placed in the subsequent HTTP
// requests as a header.
//
// See details here:
// https://docs.docker.com/registry/spec/auth/token/
Future<http::Headers> DockerFetcherPluginProcess::getAuthHeader(
    const string& repository,
    const URI& uri,
    const http::Headers& basicAuthHeaders,
    const http::Response& response)
{
  const auto stallTimeout = this->stallTimeout;

  return getAuthServiceUri(repository, uri, response, basicAuthHeaders)
    .then([basicAuthHeaders, stallTimeout](const string& authServiceUri) {
      return curl(authServiceUri, basicAuthHeaders, stallTimeout)
        .then([authServiceUri](const http::Response& response)
        -> Future<http::Headers> {
          if (response.code != http::Status::OK) {
            return Failure(
                "Unexpected HTTP response '" + response.status + "' "
                "when trying to GET '" + authServiceUri + "'");
          }

          CHECK_EQ(response.type, http::Response::BODY);

          Try<JSON::Object> object = JSON::parse<JSON::Object>(response.body);
          if (object.isError()) {
            return Failure("Parsing the JSON object failed: " + object.error());
          }

          Result<JSON::String> token = object->find<JSON::String>("token");
          if (token.isError()) {
            return Failure(
                "Finding token in JSON object failed: " + token.error());
          } else if (token.isNone()) {
            return Failure("Failed to find token in JSON object");
          }

          return getAuthHeaderBearer(token->value);
        });
    });
}


} // namespace uri {
} // namespace medios {
