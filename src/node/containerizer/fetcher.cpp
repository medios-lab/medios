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

#include "node/containerizer/fetcher.hpp"

#include <process/async.hpp>
#include <process/check.hpp>
#include <process/collect.hpp>
#include <process/dispatch.hpp>
#include <process/id.hpp>
#include <process/owned.hpp>
#include <process/subprocess.hpp>

#include <process/metrics/metrics.hpp>

#include <stout/hashmap.hpp>
#include <stout/hashset.hpp>
#include <stout/net.hpp>
#include <stout/path.hpp>
#include <stout/strings.hpp>
#include <stout/uri.hpp>

#include <stout/os/exists.hpp>
#include <stout/os/find.hpp>
#include <stout/os/killtree.hpp>
#include <stout/os/realpath.hpp>
#include <stout/os/read.hpp>
#include <stout/os/rmdir.hpp>

#include "common/status_utils.hpp"

#include "node/containerizer/fetcher_process.hpp"

#include "uri/utils.hpp"

using std::list;
using std::map;
using std::shared_ptr;
using std::string;
using std::transform;
using std::vector;

using strings::startsWith;

using medios::fetcher::FetcherInfo;

using process::async;

using process::Failure;
using process::Future;
using process::Owned;
using process::Subprocess;

namespace medios {
namespace internal {
namespace node {

static const string CACHE_FILE_NAME_PREFIX = "c";


Fetcher::Fetcher(const Flags& flags) : process(new FetcherProcess(flags))
{
  spawn(process.get());
}


Fetcher::Fetcher(const process::Owned<FetcherProcess>& process)
  : process(process)
{
  spawn(process.get());
}


Fetcher::~Fetcher()
{
  terminate(process.get());
  process::wait(process.get());
}


Try<string> Fetcher::basename(const string& uri)
{
  // TODO(bernd-medios): full URI parsing, then move this to stout.
  // There is a bug (or is it a feature?) in the original fetcher
  // code without caching that remains in effect here. URIs are
  // treated like file paths, looking for occurrences of "/",
  // but ignoring other separators that can show up
  // (e.g. "?", "=" in HTTP URLs).

  if (uri.find_first_of('\\') != string::npos ||
      uri.find_first_of('\'') != string::npos ||
      uri.find_first_of('\0') != string::npos) {
      return Error("Illegal characters in URI");
  }

  size_t index = uri.find("://");
  if (index != string::npos && 1 < index) {
    // URI starts with protocol specifier, e.g., http://, https://,
    // ftp://, ftps://.

    string path = uri.substr(index + 3);
    if (!strings::contains(path, "/") || path.size() <= path.find('/') + 1) {
      return Error("Malformed URI (missing path): " + uri);
    }

    return path.substr(path.find_last_of('/') + 1);
  }
  return Path(uri).basename();
}


Try<Nothing> Fetcher::validateUri(const string& uri)
{
  // A store is delivered as a STORE volume on the task, never staged in
  // through the fetcher; an object store's stage-in is not written
  // (DR-036). Refused here, before a fetcher process is forked, so the
  // task's failure carries the sentence.
  if (strings::startsWith(uri, "store://")) {
    return Error(
        "URI '" + uri + "' names a store; a store is mounted as a STORE "
        "volume on the task and is not fetched");
  }

  if (strings::startsWith(uri, "ftp://") ||
      strings::startsWith(uri, "ftps://")) {
    return Error(
        "URI '" + uri + "' uses an unsupported FTP scheme; only http and "
        "https network URIs are allowed (#386)");
  }

  if (isNetUri(uri)) {
    Option<Error> targetError = medios::uri::validateUrlTarget(uri);
    if (targetError.isSome()) {
      return Error(targetError->message);
    }
  }

  Try<string> result = basename(uri);
  if (result.isError()) {
    return Error(result.error());
  }

  return Nothing();
}


Try<Nothing> Fetcher::validateOutputFile(const string& path)
{
  Try<string> result = Path(path).basename();
  if (result.isError()) {
    return Error(result.error());
  }

  if (path.size() == 0) {
    return Error("URI output file path is empty");
  }

  if (strings::startsWith(path, '/') || strings::endsWith(path, '/')) {
    return Error("URI output file must be within the sandbox directory");
  }

  foreach (const string& component, strings::tokenize(path, "/")) {
    if (component == "..") {
      return Error(
          "URI output file '" + path +
          "' must not contain '..' path traversal components");
    }
  }

  return Nothing();
}


Try<Nothing> Fetcher::validateSha256(const string& digest)
{
  if (digest.size() != 64 ||
      digest.find_first_not_of("0123456789abcdef") != string::npos) {
    return Error(
        "URI sha256 '" + digest + "' is not 64 lowercase hex characters");
  }

  return Nothing();
}


static Try<Nothing> validateUris(const CommandInfo& commandInfo)
{
  foreach (const CommandInfo::URI& uri, commandInfo.uris()) {
    Try<Nothing> uriValidation = Fetcher::validateUri(uri.value());
    if (uriValidation.isError()) {
      return Error(uriValidation.error());
    }

    if (uri.has_output_file()) {
      Try<Nothing> outputFileValidation =
        Fetcher::validateOutputFile(uri.output_file());
      if (outputFileValidation.isError()) {
        return Error(outputFileValidation.error());
      }
    }

    if (uri.has_sha256()) {
      Try<Nothing> sha256Validation = Fetcher::validateSha256(uri.sha256());
      if (sha256Validation.isError()) {
        return Error(sha256Validation.error());
      }
    }
  }

  return Nothing();
}


Result<string> Fetcher::uriToLocalPath(
    const string& uri,
    const Option<string>& frameworksHome)
{
  const bool fileUri = strings::startsWith(uri, ::uri::FILE_PREFIX);

  if (!fileUri && strings::contains(uri, "://")) {
    return None();
  }

  // TODO(andschwa): Fix `path::from_uri` to remove hostname component, which it
  // currently does not do, so we remove `localhost` manually here.
  string path =
    strings::remove(path::from_uri(uri), "localhost", strings::PREFIX);

  if (!path::is_absolute(path)) {
    if (fileUri) {
      return Error("File URI only supports absolute paths");
    }

    if (frameworksHome.isNone() || frameworksHome->empty()) {
      return Error("A relative path was passed for the resource but the "
                   "Medios framework home was not specified. "
                   "Please either provide this config option "
                   "or avoid using a relative path");
    } else {
      path = path::join(frameworksHome.get(), path);
      LOG(INFO) << "Prepended Medios frameworks home to relative path, "
                << "making it: '" << path << "'";
    }
  }

  return path;
}


bool Fetcher::isNetUri(const string& uri)
{
  return strings::startsWith(uri, "http://") ||
         strings::startsWith(uri, "https://");
}


Future<Nothing> Fetcher::fetch(
    const ContainerID& containerId,
    const CommandInfo& commandInfo,
    const string& sandboxDirectory,
    const Option<string>& user)
{
  return dispatch(process.get(),
                  &FetcherProcess::fetch,
                  containerId,
                  commandInfo,
                  sandboxDirectory,
                  user);
}


void Fetcher::kill(const ContainerID& containerId)
{
  dispatch(process.get(), &FetcherProcess::kill, containerId);
}


FetcherProcess::Metrics::Metrics(FetcherProcess *fetcher)
  : task_fetches_succeeded("containerizer/fetcher/task_fetches_succeeded"),
    task_fetches_failed("containerizer/fetcher/task_fetches_failed")
{
  process::metrics::add(task_fetches_succeeded);
  process::metrics::add(task_fetches_failed);
}


FetcherProcess::Metrics::~Metrics()
{
  process::metrics::remove(task_fetches_succeeded);
  process::metrics::remove(task_fetches_failed);
}


FetcherProcess::FetcherProcess(const Flags& _flags)
    : ProcessBase(process::ID::generate("fetcher")),
      metrics(this),
      flags(_flags)
{
}


FetcherProcess::~FetcherProcess()
{
  foreachkey (const ContainerID& containerId, subprocessPids) {
    kill(containerId);
  }
}


Future<Nothing> FetcherProcess::fetch(
    const ContainerID& containerId,
    const CommandInfo& commandInfo,
    const string& sandboxDirectory,
    const Option<string>& user)
{
  VLOG(1) << "Starting to fetch URIs for container: " << containerId
          << ", directory: " << sandboxDirectory;

  Try<Nothing> validated = validateUris(commandInfo);
  if (validated.isError()) {
    ++metrics.task_fetches_failed;
    return Failure("Could not fetch: " + validated.error());
  }

  foreach (const CommandInfo::URI& uri, commandInfo.uris()) {
    const string trimmedUri = strings::trim(uri.value(), strings::PREFIX);
    // The node has no frameworks home, so a relative path stays an
    // error here and the fetcher refuses it.
    Result<string> localPath = Fetcher::uriToLocalPath(trimmedUri, None());
    if (localPath.isSome()) {
      Option<Error> localError = medios::uri::validateLocalSourcePath(
          localPath.get(),
          flags.fetcher_allow_local_uris,
          flags.fetcher_allowed_local_uri_prefix);
      if (localError.isSome()) {
        ++metrics.task_fetches_failed;
        return Failure("Could not fetch: " + localError->message);
      }
    }
  }

  Option<string> commandUser = user;
  if (commandInfo.has_user()) {
    commandUser = commandInfo.user();
  }

  // Build a FetcherInfo with every URI downloading directly into the
  // sandbox.
  FetcherInfo info;
  foreach (const CommandInfo::URI& uri, commandInfo.uris()) {
    FetcherInfo::Item* item = info.add_items();
    item->mutable_uri()->CopyFrom(uri);
    item->set_action(FetcherInfo::Item::BYPASS_CACHE);
  }

  info.set_sandbox_directory(sandboxDirectory);

  if (commandUser.isSome()) {
    info.set_user(commandUser.get());
  }

  info.mutable_stall_timeout()
    ->set_nanoseconds(flags.fetcher_stall_timeout.ns());

  return run(containerId, sandboxDirectory, commandUser, info)
    .repair(defer(self(), [=](const Future<Nothing>& future) {
      ++metrics.task_fetches_failed;
      LOG(ERROR) << "Failed to run medios-fetcher: " << future.failure();
      return future;
    }))
    .then(defer(self(), [=]() {
      ++metrics.task_fetches_succeeded;
      return Nothing();
    }));
}


Future<Nothing> FetcherProcess::run(
    const ContainerID& containerId,
    const string& sandboxDirectory,
    const Option<string>& user,
    const FetcherInfo& info)
{
  // Before we fetch let's make sure we create 'stdout' and 'stderr'
  // files into which we can redirect the output of the medios-fetcher
  // (and later redirect the child's stdout/stderr).

  // TODO(tillt): Considering updating fetcher::run to take paths
  // instead of file descriptors and then use Subprocess::PATH()
  // instead of Subprocess::FD(). The reason this can't easily be done
  // today is because we not only need to open the files but also
  // chown them.
  const string stdoutPath = path::join(info.sandbox_directory(), "stdout");

  Try<int_fd> out = os::open(
      stdoutPath,
      O_WRONLY | O_CREAT | O_TRUNC | O_NONBLOCK | O_CLOEXEC,
      S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH);

  if (out.isError()) {
    return Failure("Failed to create 'stdout' file: " + out.error());
  }

  string stderrPath = path::join(info.sandbox_directory(), "stderr");
  Try<int_fd> err = os::open(
      stderrPath,
      O_WRONLY | O_CREAT | O_TRUNC | O_NONBLOCK | O_CLOEXEC,
      S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH);

  if (err.isError()) {
    os::close(out.get());
    return Failure("Failed to create 'stderr' file: " + err.error());
  }

  if (user.isSome()) {
    // TODO(megha.sharma): Fetcher should not create separate stdout/stderr
    // files but rather use FDs prepared by the container logger.
    // See MESOS-6271 for more details.
    Try<Nothing> chownOut = os::chown(
        user.get(),
        stdoutPath,
        false);

    if (chownOut.isError()) {
      os::close(out.get());
      os::close(err.get());
      return Failure(
          "Failed to chown '" +
          stdoutPath +
          "' to user '" + user.get() + "' : " +
          chownOut.error());
    }

    Try<Nothing> chownErr = os::chown(
        user.get(),
        stderrPath,
        false);

    if (chownErr.isError()) {
      os::close(out.get());
      os::close(err.get());
      return Failure(
          "Failed to chown '" +
          stderrPath +
          "' to user '" + user.get() + "' : " +
          chownErr.error());
    }
  }

  // Return early if there are no URIs to fetch.
  if (info.items_size() == 0) {
      os::close(out.get());
      os::close(err.get());
      return Nothing();
  }

  string fetcherPath = path::join(flags.launcher_dir, "medios-fetcher");

  Result<string> realpath = os::realpath(fetcherPath);

  if (!realpath.isSome()) {
    LOG(ERROR) << "Failed to determine the canonical path "
               << "for the medios-fetcher '"
               << fetcherPath
               << "': "
               << (realpath.isError() ? realpath.error()
                                      : "No such file or directory");

    os::close(out.get());
    os::close(err.get());

    return Failure("Could not fetch URIs: failed to find medios-fetcher");
  }

  // Now the actual medios-fetcher command.
  string command = realpath.get();

  // We pass arguments to the fetcher program by means of an
  // environment variable.
  // For assuring that we pass on variables that may be consumed by
  // the medios-fetcher, we allowlist them before masking out any
  // unwanted node->fetcher environment spillover.
  // TODO(tillt): Consider using the `medios::internal::logging::Flags`
  // to determine the allowlist.
  const hashset<string> allowlist = {
    "MEDIOS_EXTERNAL_LOG_FILE",
    "MEDIOS_FETCHER_ALLOW_LOCAL_URIS",
    "MEDIOS_FETCHER_ALLOWED_LOCAL_PREFIX",
    "MEDIOS_FETCHER_DISALLOW_PRIVATE_IPS",
    "MEDIOS_INITIALIZE_DRIVER_LOGGING",
    "MEDIOS_LOG_DIR",
    "MEDIOS_LOGBUFSECS",
    "MEDIOS_LOGGING_LEVEL",
    "MEDIOS_QUIET"
  };

  map<string, string> environment;
  foreachpair (const string& key, const string& value, os::environment()) {
    if (allowlist.contains(strings::upper(key)) ||
        (!startsWith(key, "LIBPROCESS_") && !startsWith(key, "MEDIOS_"))) {
      environment.emplace(key, value);
    }
  }

  environment["MEDIOS_FETCHER_INFO"] = stringify(JSON::protobuf(info));
  environment["MEDIOS_FETCHER_ALLOW_LOCAL_URIS"] =
    flags.fetcher_allow_local_uris ? "true" : "false";
  if (flags.fetcher_allowed_local_uri_prefix.isSome()) {
    environment["MEDIOS_FETCHER_ALLOWED_LOCAL_PREFIX"] =
      flags.fetcher_allowed_local_uri_prefix.get();
  }

  // TODO(jieyu): This is to make sure the libprocess of the fetcher
  // can properly initialize and find the IP. Since we don't need to
  // use the TCP socket for communication, it's OK to use a local
  // address. Consider disable TCP socket in libprocess if libprocess
  // supports that.
  environment.emplace("LIBPROCESS_IP", "127.0.0.1");

  VLOG(1) << "Fetching URIs using command '" << command << "'";

  Try<Subprocess> fetcherSubprocess = subprocess(
      command,
      Subprocess::PIPE(),
      Subprocess::FD(out.get(), Subprocess::IO::OWNED),
      Subprocess::FD(err.get(), Subprocess::IO::OWNED),
      environment);

  if (fetcherSubprocess.isError()) {
    return Failure("Failed to execute medios-fetcher: " +
                   fetcherSubprocess.error());
  }

  // Remember this PID in case we need to kill the subprocess. See
  // FetcherProcess::kill(). This value gets removed after we wait on
  // the subprocess.
  subprocessPids[containerId] = fetcherSubprocess->pid();

  return fetcherSubprocess->status()
    .then(defer(self(), [=](const Option<int>& status) -> Future<Nothing> {
      if (status.isNone()) {
        return Failure("No status available from medios-fetcher");
      }

      if (!WSUCCEEDED(status.get())) {
        return Failure("Failed to fetch all URIs for container '" +
                       stringify(containerId) + "': " +
                       WSTRINGIFY(status.get()));
      }

      return Nothing();
    }))
    .onFailed(defer(self(), [=](const string&) {
      // To aid debugging what went wrong when attempting to fetch, grab the
      // fetcher's local log output from the sandbox and log it here.
      Try<string> text = os::read(stderrPath);
      if (text.isSome()) {
        LOG(WARNING) << "Begin fetcher log (stderr in sandbox) for container "
                     << containerId << " from running command: " << command
                     << "\n" << text.get() << "\n"
                     << "End fetcher log for container " << containerId;
      } else {
        LOG(ERROR) << "Fetcher log (stderr in sandbox) for container "
                   << containerId << " not readable: " << text.error();
      }
    }))
    .onAny(defer(self(), [=](const Future<Nothing>&) {
      // Clear the subprocess PID remembered from running medios-fetcher.
      subprocessPids.erase(containerId);
    }));
}


void FetcherProcess::kill(const ContainerID& containerId)
{
  if (subprocessPids.contains(containerId)) {
    VLOG(1) << "Killing the fetcher for container '" << containerId << "'";
    // Best effort kill the entire fetcher tree.
    os::killtree(subprocessPids.at(containerId), SIGKILL);

    subprocessPids.erase(containerId);
  }
}


} // namespace node {
} // namespace internal {
} // namespace medios {
