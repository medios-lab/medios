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

#include <unistd.h>

#include <map>
#include <set>
#include <string>

#include <curl/curl.h>

#include <process/future.hpp>
#include <process/gmock.hpp>
#include <process/gtest.hpp>
#include <process/http.hpp>
#include <process/subprocess.hpp>

#include <stout/base64.hpp>
#include <stout/fs.hpp>
#include <stout/gtest.hpp>
#include <stout/net.hpp>
#include <stout/option.hpp>
#include <stout/os.hpp>
#include <stout/path.hpp>
#include <stout/strings.hpp>
#include <stout/try.hpp>
#include <stout/uri.hpp>

#include <stout/os/permissions.hpp>

#include <medios/fetcher/fetcher.hpp>
#include <medios/type_utils.hpp>

#include "common/command_utils.hpp"

#include "node/containerizer/fetcher.hpp"
#include "node/flags.hpp"

#include "uri/utils.hpp"

#include "tests/environment.hpp"
#include "tests/flags.hpp"
#include "tests/medios.hpp"
#include "tests/utils.hpp"

using namespace medios::node;

using namespace process;

using medios::fetcher::FetcherInfo;

using medios::internal::node::Fetcher;

using process::Subprocess;
using process::Future;

using std::map;
using std::set;
using std::string;


namespace medios {
namespace internal {
namespace tests {

class FetcherTest : public MediosTest
{
public:
  static void verifyMetrics(unsigned successCount, unsigned errorCount)
  {
    JSON::Object metrics = Metrics();

    // First verify that each metric is present.
    ASSERT_EQ(
        1u,
        metrics.values.count("containerizer/fetcher/task_fetches_succeeded"));
    ASSERT_EQ(
        1u,
        metrics.values.count("containerizer/fetcher/task_fetches_failed"));

    // Next verify the actual values.
    EXPECT_SOME_EQ(
      successCount,
      metrics.at<JSON::Number>("containerizer/fetcher/task_fetches_succeeded"));
    EXPECT_SOME_EQ(
      errorCount,
      metrics.at<JSON::Number>("containerizer/fetcher/task_fetches_failed"));
  }
};


TEST_F(FetcherTest, FileURI)
{
  string fromDir = path::join(os::getcwd(), "from");
  ASSERT_SOME(os::mkdir(fromDir));
  string testFile = path::join(fromDir, "test");
  EXPECT_SOME(os::write(testFile, "data"));

  string localFile = path::join(os::getcwd(), "test");
  EXPECT_FALSE(os::exists(localFile));

  node::Flags flags = CreateNodeFlags();

  ContainerID containerId;
  containerId.set_value(id::UUID::random().toString());

  CommandInfo commandInfo;
  CommandInfo::URI* uri = commandInfo.add_uris();
  uri->set_value(::uri::from_path(testFile));

  Fetcher fetcher(flags);

  Future<Nothing> fetch = fetcher.fetch(
      containerId, commandInfo, os::getcwd(), None());
  AWAIT_READY(fetch);

  EXPECT_TRUE(os::exists(localFile));

  verifyMetrics(1, 0);
}


TEST_F(FetcherTest, LogSuccessToStderr)
{
  // Valid test file with data.
  string fromDir = path::join(os::getcwd(), "from");
  ASSERT_SOME(os::mkdir(fromDir));
  string testFile = path::join(fromDir, "test");
  EXPECT_SOME(os::write(testFile, "data"));

  string localFile = path::join(os::getcwd(), "test");
  EXPECT_FALSE(os::exists(localFile));

  // Fetcher stderr file.
  string stderrFile = path::join(os::getcwd(), "stderr");
  EXPECT_FALSE(os::exists(stderrFile));

  node::Flags flags = CreateNodeFlags();

  ContainerID containerId;
  containerId.set_value(id::UUID::random().toString());

  CommandInfo commandInfo;
  CommandInfo::URI* uri = commandInfo.add_uris();
  uri->set_value(::uri::from_path(testFile));

  Fetcher fetcher(flags);

  Future<Nothing> fetch =
    fetcher.fetch(containerId, commandInfo, os::getcwd(), None());
  AWAIT_READY(fetch);

  EXPECT_TRUE(os::exists(localFile));

  // Fetcher created log file.
  EXPECT_TRUE(os::exists(stderrFile));

  // Fetcher logged success to stderr.
  const Try<string> stderrContent = os::read(stderrFile);
  EXPECT_SOME(stderrContent);
  EXPECT_TRUE(strings::contains(
      stderrContent.get(), "directly into the sandbox directory"));
  EXPECT_TRUE(strings::contains(
      stderrContent.get(), "Successfully fetched all URIs into"));
}


TEST_F(FetcherTest, LogFailureToStderr)
{
  // Purposefully invalid file to fetch.
  string testFile = path::join(os::getcwd(), "test");
  EXPECT_FALSE(os::exists(testFile));

  string localFile = path::join(os::getcwd(), "test");
  EXPECT_FALSE(os::exists(localFile));

  // Fetcher stderr file.
  string stderrFile = path::join(os::getcwd(), "stderr");
  EXPECT_FALSE(os::exists(stderrFile));

  node::Flags flags = CreateNodeFlags();

  ContainerID containerId;
  containerId.set_value(id::UUID::random().toString());

  CommandInfo commandInfo;
  CommandInfo::URI* uri = commandInfo.add_uris();
  uri->set_value(::uri::from_path(testFile));

  Fetcher fetcher(flags);

  Future<Nothing> fetch =
    fetcher.fetch(containerId, commandInfo, os::getcwd(), None());
  AWAIT_FAILED(fetch);

  // Failed to fetch.
  EXPECT_FALSE(os::exists(localFile));

  // Fetcher created log file.
  EXPECT_TRUE(os::exists(stderrFile));

  // Fetcher logged failure to stderr.
  Try<string> stderrContent = os::read(stderrFile);
  EXPECT_SOME(stderrContent);
  EXPECT_TRUE(strings::contains(stderrContent.get(), "Failed to fetch"));
}


// Tests that non-root users are unable to fetch root-protected files on the
// local filesystem.
TEST_F(FetcherTest, ROOT_UNPRIVILEGED_USER_RootProtectedFileURI)
{
  Option<string> user = os::getenv("SUDO_USER");
  ASSERT_SOME(user);

  ASSERT_SOME(os::getuid(user.get()));

  string fromDir = path::join(os::getcwd(), "from");
  ASSERT_SOME(os::mkdir(fromDir));
  string testFile = path::join(fromDir, "test");
  EXPECT_SOME(os::write(testFile, "data"));
  EXPECT_SOME(os::chmod(testFile, 600));

  node::Flags flags = CreateNodeFlags();

  ContainerID containerId;
  containerId.set_value(id::UUID::random().toString());

  CommandInfo commandInfo;
  commandInfo.set_user(user.get());

  CommandInfo::URI* uri = commandInfo.add_uris();
  uri->set_value(::uri::from_path(testFile));

  Fetcher fetcher(flags);

  AWAIT_FAILED(fetcher.fetch(
      containerId,
      commandInfo,
      os::getcwd(),
      None()));
}


TEST_F(FetcherTest, CustomOutputFileSubdirectory)
{
  string testFile = path::join(os::getcwd(), "test");
  EXPECT_SOME(os::write(testFile, "data"));

  string customOutputFile = path::join("subdir", "custom.txt");
  string localFile = path::join(os::getcwd(), customOutputFile);
  EXPECT_FALSE(os::exists(localFile));

  node::Flags flags = CreateNodeFlags();

  ContainerID containerId;
  containerId.set_value(id::UUID::random().toString());

  CommandInfo commandInfo;
  CommandInfo::URI* uri = commandInfo.add_uris();
  uri->set_value(::uri::from_path(testFile));
  uri->set_output_file(customOutputFile);

  Fetcher fetcher(flags);

  Future<Nothing> fetch = fetcher.fetch(
      containerId, commandInfo, os::getcwd(), None());
  AWAIT_READY(fetch);

  EXPECT_TRUE(os::exists(localFile));

  verifyMetrics(1, 0);
}


// Negative test: invalid custom URI output file. If the user specifies a
// path for the file saved in the sandbox that has a directory component,
// it must be a relative path.
TEST_F(FetcherTest, AbsoluteCustomSubdirectoryFails)
{
  string fromDir = path::join(os::getcwd(), "from");
  ASSERT_SOME(os::mkdir(fromDir));
  string testFile = path::join(fromDir, "test");
  EXPECT_SOME(os::write(testFile, "data"));

  string customOutputFile = "/subdir/custom.txt";
  string localFile = path::join(os::getcwd(), customOutputFile);
  EXPECT_FALSE(os::exists(localFile));

  node::Flags flags = CreateNodeFlags();

  ContainerID containerId;
  containerId.set_value(id::UUID::random().toString());

  CommandInfo commandInfo;
  CommandInfo::URI* uri = commandInfo.add_uris();
  uri->set_value(::uri::from_path(testFile));
  uri->set_output_file(customOutputFile);

  Fetcher fetcher(flags);

  Future<Nothing> fetch = fetcher.fetch(
      containerId, commandInfo, os::getcwd(), None());
  AWAIT_FAILED(fetch);

  EXPECT_FALSE(os::exists(localFile));

  verifyMetrics(0, 1);
}


// Negative test: relative `output_file` containing `..` traversal components
// is rejected before escaping the sandbox directory (#385).
TEST_F(FetcherTest, RelativeParentTraversalOutputFileFails)
{
  string fromDir = path::join(os::getcwd(), "from");
  ASSERT_SOME(os::mkdir(fromDir));
  string testFile = path::join(fromDir, "test");
  EXPECT_SOME(os::write(testFile, "data"));

  string escapedFile = path::join(os::getcwd(), "from", "escaped.txt");
  EXPECT_FALSE(os::exists(escapedFile));

  node::Flags flags = CreateNodeFlags();

  ContainerID containerId;
  containerId.set_value(id::UUID::random().toString());

  CommandInfo commandInfo;
  CommandInfo::URI* uri = commandInfo.add_uris();
  uri->set_value(::uri::from_path(testFile));
  uri->set_output_file("subdir/../../from/escaped.txt");

  Fetcher fetcher(flags);

  Future<Nothing> fetch = fetcher.fetch(
      containerId, commandInfo, os::getcwd(), None());
  AWAIT_FAILED(fetch);
  EXPECT_TRUE(strings::contains(fetch.failure(), "..")) << fetch.failure();
  EXPECT_FALSE(os::exists(escapedFile));

  verifyMetrics(0, 1);
}


// Negative test: invalid user name. Copied from FileTest, so this
// normally would succeed, but here a bogus user name is specified.
// So we check for fetch failure.
//
// support isn't implemented, see MESOS-3176.
TEST_F(FetcherTest, InvalidUser)
{
  string fromDir = path::join(os::getcwd(), "from");
  ASSERT_SOME(os::mkdir(fromDir));
  string testFile = path::join(fromDir, "test");
  EXPECT_SOME(os::write(testFile, "data"));

  string localFile = path::join(os::getcwd(), "test");
  EXPECT_FALSE(os::exists(localFile));

  node::Flags flags = CreateNodeFlags();

  ContainerID containerId;
  containerId.set_value(id::UUID::random().toString());

  CommandInfo commandInfo;
  commandInfo.set_user(id::UUID::random().toString());

  CommandInfo::URI* uri = commandInfo.add_uris();
  uri->set_value(::uri::from_path(testFile));

  Fetcher fetcher(flags);

  Future<Nothing> fetch = fetcher.fetch(
      containerId, commandInfo, os::getcwd(), None());
  AWAIT_FAILED(fetch);

  // See FetcherProcess::fetch(), the message must mention "chown" in
  // this case.
  EXPECT_TRUE(strings::contains(fetch.failure(), "chown"));

  EXPECT_FALSE(os::exists(localFile));

  verifyMetrics(0, 1);
}


// Negative test: URI leading to non-existing file. Copied from FileTest,
// but here the resource is missing. So we check for fetch failure.
TEST_F(FetcherTest, NonExistingFile)
{
  string fromDir = path::join(os::getcwd(), "from");
  ASSERT_SOME(os::mkdir(fromDir));
  string testFile = path::join(fromDir, "nonExistingFile");

  node::Flags flags = CreateNodeFlags();

  ContainerID containerId;
  containerId.set_value(id::UUID::random().toString());

  CommandInfo commandInfo;
  CommandInfo::URI* uri = commandInfo.add_uris();
  uri->set_value(::uri::from_path(testFile));

  Fetcher fetcher(flags);

  Future<Nothing> fetch = fetcher.fetch(
      containerId, commandInfo, os::getcwd(), None());
  AWAIT_FAILED(fetch);

  // See FetcherProcess::run().
  EXPECT_TRUE(strings::contains(fetch.failure(), "Failed to fetch"));

  verifyMetrics(0, 1);
}


// A store:// URI is refused with a sentence: a store is a STORE volume on
// the task, not something the fetcher stages in (DR-036).
TEST_F(FetcherTest, StoreURIIsRefused)
{
  node::Flags flags = CreateNodeFlags();

  ContainerID containerId;
  containerId.set_value(id::UUID::random().toString());

  CommandInfo commandInfo;
  CommandInfo::URI* uri = commandInfo.add_uris();
  uri->set_value("store://tabicl-tables/part-0.npz");

  Fetcher fetcher(flags);

  Future<Nothing> fetch = fetcher.fetch(
      containerId, commandInfo, os::getcwd(), None());
  AWAIT_FAILED(fetch);

  EXPECT_TRUE(strings::contains(fetch.failure(), "names a store"))
    << fetch.failure();
  EXPECT_TRUE(strings::contains(fetch.failure(), "STORE volume"))
    << fetch.failure();

  verifyMetrics(0, 1);
}


// SHA-256 of the four bytes "data".
static const char DATA_SHA256[] =
  "3a6eb0790f39ac87c94f3856b2dd2c5d110e6811602261a9a923d3bb23adc8b7";


TEST_F(FetcherTest, Sha256Match)
{
  string fromDir = path::join(os::getcwd(), "from");
  ASSERT_SOME(os::mkdir(fromDir));
  string testFile = path::join(fromDir, "test");
  ASSERT_SOME(os::write(testFile, "data"));

  node::Flags flags = CreateNodeFlags();

  ContainerID containerId;
  containerId.set_value(id::UUID::random().toString());

  CommandInfo commandInfo;
  CommandInfo::URI* uri = commandInfo.add_uris();
  uri->set_value(::uri::from_path(testFile));
  uri->set_sha256(DATA_SHA256);

  Fetcher fetcher(flags);

  Future<Nothing> fetch = fetcher.fetch(
      containerId, commandInfo, os::getcwd(), None());
  AWAIT_READY(fetch);

  EXPECT_SOME_EQ("data", os::read(path::join(os::getcwd(), "test")));

  verifyMetrics(1, 0);
}


// A file whose digest differs fails the fetch and is not left in the
// sandbox.
TEST_F(FetcherTest, Sha256Mismatch)
{
  string fromDir = path::join(os::getcwd(), "from");
  ASSERT_SOME(os::mkdir(fromDir));
  string testFile = path::join(fromDir, "test");
  ASSERT_SOME(os::write(testFile, "other"));

  node::Flags flags = CreateNodeFlags();

  ContainerID containerId;
  containerId.set_value(id::UUID::random().toString());

  CommandInfo commandInfo;
  CommandInfo::URI* uri = commandInfo.add_uris();
  uri->set_value(::uri::from_path(testFile));
  uri->set_sha256(DATA_SHA256);

  Fetcher fetcher(flags);

  Future<Nothing> fetch = fetcher.fetch(
      containerId, commandInfo, os::getcwd(), None());
  AWAIT_FAILED(fetch);

  Try<string> log = os::read(path::join(os::getcwd(), "stderr"));
  ASSERT_SOME(log);
  EXPECT_TRUE(strings::contains(log.get(), "sha256 mismatch")) << log.get();
  EXPECT_TRUE(strings::contains(log.get(), DATA_SHA256)) << log.get();
  EXPECT_FALSE(os::exists(path::join(os::getcwd(), "test")));

  verifyMetrics(0, 1);
}


// A digest that is not 64 lowercase hex characters is refused before a
// fetcher process is forked.
TEST_F(FetcherTest, Sha256Malformed)
{
  string fromDir = path::join(os::getcwd(), "from");
  ASSERT_SOME(os::mkdir(fromDir));
  string testFile = path::join(fromDir, "test");
  ASSERT_SOME(os::write(testFile, "data"));

  node::Flags flags = CreateNodeFlags();

  ContainerID containerId;
  containerId.set_value(id::UUID::random().toString());

  CommandInfo commandInfo;
  CommandInfo::URI* uri = commandInfo.add_uris();
  uri->set_value(::uri::from_path(testFile));
  uri->set_sha256(strings::upper(DATA_SHA256));

  Fetcher fetcher(flags);

  Future<Nothing> fetch = fetcher.fetch(
      containerId, commandInfo, os::getcwd(), None());
  AWAIT_FAILED(fetch);

  EXPECT_TRUE(strings::contains(fetch.failure(), "lowercase hex"))
    << fetch.failure();
  EXPECT_FALSE(os::exists(path::join(os::getcwd(), "test")));

  verifyMetrics(0, 1);
}


// The digest covers the archive as fetched; the archive is then extracted.
// Sha256Match pins command::sha256 to a known digest, so the test can use it
// to name the archive's.
TEST_F(FetcherTest, Sha256Extract)
{
  Try<string> dir = os::mkdtemp(path::join(os::getcwd(), "XXXXXX"));
  ASSERT_SOME(dir);

  Try<string> path = os::mktemp(path::join(dir.get(), "XXXXXX"));
  ASSERT_SOME(path);

  ASSERT_SOME(os::write(path.get(), "hello tar"));
  ASSERT_SOME(os::shell(
      "tar cf '" + path.get() + ".tar' '" + path.get() + "' 2>&1"));

  Future<string> digest = command::sha256(Path(path.get() + ".tar"));
  AWAIT_READY(digest);

  ContainerID containerId;
  containerId.set_value(id::UUID::random().toString());

  CommandInfo commandInfo;
  CommandInfo::URI* uri = commandInfo.add_uris();
  uri->set_value(::uri::from_path(path.get() + ".tar"));
  uri->set_extract(true);
  uri->set_sha256(digest.get());

  node::Flags flags = CreateNodeFlags();

  Fetcher fetcher(flags);

  Future<Nothing> fetch = fetcher.fetch(
      containerId, commandInfo, os::getcwd(), None());
  AWAIT_READY(fetch);

  ASSERT_SOME_EQ("hello tar", os::read(path::join(os::getcwd(), path.get())));

  verifyMetrics(1, 0);
}


// Negative test: malformed URI, missing path.
TEST_F(FetcherTest, MalformedURI)
{
  node::Flags flags = CreateNodeFlags();

  ContainerID containerId;
  containerId.set_value(id::UUID::random().toString());

  CommandInfo commandInfo;
  CommandInfo::URI* uri = commandInfo.add_uris();
  uri->set_value("lala://nopath");

  Fetcher fetcher(flags);

  Future<Nothing> fetch = fetcher.fetch(
      containerId, commandInfo, os::getcwd(), None());
  AWAIT_FAILED(fetch);

  // See Fetcher::basename().
  EXPECT_TRUE(strings::contains(fetch.failure(), "Malformed"));

  verifyMetrics(0, 1);
}


TEST_F(FetcherTest, AbsoluteFilePath)
{
  string fromDir = path::join(os::getcwd(), "from");
  ASSERT_SOME(os::mkdir(fromDir));
  string testPath = path::join(fromDir, "test");
  EXPECT_SOME(os::write(testPath, "data"));

  string localFile = path::join(os::getcwd(), "test");
  EXPECT_FALSE(os::exists(localFile));

  node::Flags flags = CreateNodeFlags();

  ContainerID containerId;
  containerId.set_value(id::UUID::random().toString());

  CommandInfo commandInfo;
  CommandInfo::URI* uri = commandInfo.add_uris();
  uri->set_value(::uri::from_path(testPath));

  Fetcher fetcher(flags);

  Future<Nothing> fetch = fetcher.fetch(
      containerId, commandInfo, os::getcwd(), None());
  AWAIT_READY(fetch);

  EXPECT_TRUE(os::exists(localFile));

  verifyMetrics(1, 0);
}


class HttpProcess : public Process<HttpProcess>
{
public:
  HttpProcess() {}

  MOCK_METHOD1(test, Future<http::Response>(const http::Request&));

protected:
  void initialize() override
  {
    route("/test", None(), &HttpProcess::test);
  }
};


class Http
{
public:
  Http() : process(new HttpProcess())
  {
    spawn(process.get());
  }

  ~Http()
  {
    terminate(process.get());
    wait(process.get());
  }

  Owned<HttpProcess> process;
};


TEST_F(FetcherTest, OSNetUriTest)
{
  Http http;

  const network::inet::Address& address = http.process->self().address;

  process::http::URL url(
      "http",
      address.ip,
      address.port,
      strings::join("/", http.process->self().id, "test"));

  string localFile = path::join(os::getcwd(), "test");
  EXPECT_FALSE(os::exists(localFile));

  node::Flags flags = CreateNodeFlags();

  ContainerID containerId;
  containerId.set_value(id::UUID::random().toString());

  CommandInfo commandInfo;
  CommandInfo::URI* uri = commandInfo.add_uris();
  uri->set_value(stringify(url));

  Fetcher fetcher(flags);

  EXPECT_CALL(*http.process, test(_))
    .WillOnce(Return(http::OK()));

  Future<Nothing> fetch = fetcher.fetch(
      containerId, commandInfo, os::getcwd(), None());

  AWAIT_READY(fetch);

  EXPECT_TRUE(os::exists(localFile));

  verifyMetrics(1, 0);
}


// Tests whether fetcher can process URIs that contain leading whitespace
// characters. This was added as a verification for MESOS-2862.
//
// TODO(hartem): This test case should be merged with the previous one.
TEST_F(FetcherTest, OSNetUriSpaceTest)
{
  Http http;

  const network::inet::Address& address = http.process->self().address;

  process::http::URL url(
      "http",
      address.ip,
      address.port,
      strings::join("/", http.process->self().id, "test"));

  string localFile = path::join(os::getcwd(), "test");
  EXPECT_FALSE(os::exists(localFile));

  node::Flags flags = CreateNodeFlags();

  ContainerID containerId;
  containerId.set_value(id::UUID::random().toString());

  CommandInfo commandInfo;
  CommandInfo::URI* uri = commandInfo.add_uris();

  // Add whitespace characters to the beginning of the URL.
  uri->set_value("\r\n\t " + stringify(url));

  Fetcher fetcher(flags);

  // Verify that the intended endpoint is hit.
  EXPECT_CALL(*http.process, test(_))
    .WillOnce(Return(http::OK()));

  Future<Nothing> fetch = fetcher.fetch(
      containerId, commandInfo, os::getcwd(), None());

  AWAIT_READY(fetch);

  EXPECT_TRUE(os::exists(localFile));

  verifyMetrics(1, 0);
}


// The fetcher hands http and https URIs to the linked libcurl. It must have
// a TLS backend, and no other protocol compiled in (#386).
TEST_F(FetcherTest, CurlProtocolsAreHttpAndHttps)
{
  const curl_version_info_data* info = curl_version_info(CURLVERSION_NOW);
  ASSERT_NE(nullptr, info);
  EXPECT_NE(0, info->features & CURL_VERSION_SSL);
  ASSERT_NE(nullptr, info->ssl_version);
  EXPECT_TRUE(strings::startsWith(info->ssl_version, "OpenSSL/3"))
    << info->ssl_version;

  set<string> protocols;
  for (const char* const* protocol = info->protocols;
       *protocol != nullptr;
       ++protocol) {
    protocols.insert(*protocol);
  }

  EXPECT_EQ(set<string>({"http", "https"}), protocols);
}


TEST_F(FetcherTest, FileLocalhostURI)
{
  string fromDir = path::join(os::getcwd(), "from");
  ASSERT_SOME(os::mkdir(fromDir));
  string testFile = path::join(fromDir, "test");
  EXPECT_SOME(os::write(testFile, "data"));

  string localFile = path::join(os::getcwd(), "test");
  EXPECT_FALSE(os::exists(localFile));

  node::Flags flags = CreateNodeFlags();

  ContainerID containerId;
  containerId.set_value(id::UUID::random().toString());

  CommandInfo commandInfo;
  CommandInfo::URI* uri = commandInfo.add_uris();
  uri->set_value(::uri::from_path(testFile));

  Fetcher fetcher(flags);

  Future<Nothing> fetch = fetcher.fetch(
      containerId, commandInfo, os::getcwd(), None());
  AWAIT_READY(fetch);

  EXPECT_TRUE(os::exists(localFile));

  verifyMetrics(1, 0);
}


// see MESOS-3176.
TEST_F(FetcherTest, NoExtractNotExecutable)
{
  // First construct a temporary file that can be fetched.
  Try<string> dir = os::mkdtemp(path::join(os::getcwd(), "XXXXXX"));
  ASSERT_SOME(dir);

  Try<string> path = os::mktemp(path::join(dir.get(), "XXXXXX"));
  ASSERT_SOME(path);

  ContainerID containerId;
  containerId.set_value(id::UUID::random().toString());

  CommandInfo commandInfo;
  CommandInfo::URI* uri = commandInfo.add_uris();
  uri->set_value(path.get());
  uri->set_executable(false);
  uri->set_extract(false);

  node::Flags flags = CreateNodeFlags();

  Fetcher fetcher(flags);

  Future<Nothing> fetch = fetcher.fetch(
      containerId, commandInfo, os::getcwd(), None());
  AWAIT_READY(fetch);

  string basename = Path(path.get()).basename();

  Try<os::Permissions> permissions = os::permissions(basename);

  ASSERT_SOME(permissions);
  EXPECT_FALSE(permissions->owner.x);
  EXPECT_FALSE(permissions->group.x);
  EXPECT_FALSE(permissions->others.x);

  verifyMetrics(1, 0);
}


// see MESOS-3176.
TEST_F(FetcherTest, NoExtractExecutable)
{
  // First construct a temporary file that can be fetched.
  Try<string> dir = os::mkdtemp(path::join(os::getcwd(), "XXXXXX"));
  ASSERT_SOME(dir);

  Try<string> path = os::mktemp(path::join(dir.get(), "XXXXXX"));
  ASSERT_SOME(path);

  ContainerID containerId;
  containerId.set_value(id::UUID::random().toString());

  CommandInfo commandInfo;
  CommandInfo::URI* uri = commandInfo.add_uris();
  uri->set_value(path.get());
  uri->set_executable(true);
  uri->set_extract(false);

  node::Flags flags = CreateNodeFlags();

  Fetcher fetcher(flags);

  Future<Nothing> fetch = fetcher.fetch(
      containerId, commandInfo, os::getcwd(), None());

  AWAIT_READY(fetch);

  string basename = Path(path.get()).basename();

  Try<os::Permissions> permissions = os::permissions(basename);

  ASSERT_SOME(permissions);
  EXPECT_TRUE(permissions->owner.x);
  EXPECT_TRUE(permissions->group.x);
  EXPECT_TRUE(permissions->others.x);

  verifyMetrics(1, 0);
}


// NOTE: Permissions handling needs to be worked out, see MESOS-3176.
TEST_F(FetcherTest, ExtractNotExecutable)
{
  // First construct a temporary file that can be fetched and archived with tar
  // gzip.
  Try<string> dir = os::mkdtemp(path::join(os::getcwd(), "XXXXXX"));
  ASSERT_SOME(dir);

  Try<string> path = os::mktemp(path::join(dir.get(), "XXXXXX"));
  ASSERT_SOME(path);

  ASSERT_SOME(os::write(path.get(), "hello world"));

  // TODO(benh): Update os::tar so that we can capture or ignore
  // stdout/stderr output.

  // Create an uncompressed archive (see MESOS-3579), but with
  // extension `.tar.gz` to verify we can unpack files such names.
  ASSERT_SOME(os::shell(
      "tar cf '" + path.get() + ".tar.gz' '" + path.get() + "' 2>&1"));

  ContainerID containerId;
  containerId.set_value(id::UUID::random().toString());

  CommandInfo commandInfo;
  CommandInfo::URI* uri = commandInfo.add_uris();
  uri->set_value(path.get() + ".tar.gz");
  uri->set_executable(false);
  uri->set_extract(true);

  node::Flags flags = CreateNodeFlags();

  Fetcher fetcher(flags);

  Future<Nothing> fetch = fetcher.fetch(
      containerId, commandInfo, os::getcwd(), None());

  AWAIT_READY(fetch);

  ASSERT_TRUE(os::exists(path::join(os::getcwd(), path.get())));

  ASSERT_SOME_EQ("hello world", os::read(path::join(os::getcwd(), path.get())));

  Try<os::Permissions> permissions = os::permissions(path.get());

  ASSERT_SOME(permissions);
  EXPECT_FALSE(permissions->owner.x);
  EXPECT_FALSE(permissions->group.x);
  EXPECT_FALSE(permissions->others.x);

  verifyMetrics(1, 0);
}


// Tests extracting tar file with extension .tar.
//
// use a code library to provide 'tar' functionality programmatically,
// see MESOS-8064.
TEST_F(FetcherTest, ExtractTar)
{
  // First construct a temporary file that can be fetched and archived with
  // tar.
  Try<string> dir = os::mkdtemp(path::join(os::getcwd(), "XXXXXX"));
  ASSERT_SOME(dir);

  Try<string> path = os::mktemp(path::join(dir.get(), "XXXXXX"));
  ASSERT_SOME(path);

  ASSERT_SOME(os::write(path.get(), "hello tar"));

  // TODO(benh): Update os::tar so that we can capture or ignore
  // stdout/stderr output.

  // Create an uncompressed archive (see MESOS-3579).
  ASSERT_SOME(os::shell(
      "tar cf '" + path.get() + ".tar' '" + path.get() + "' 2>&1"));

  ContainerID containerId;
  containerId.set_value(id::UUID::random().toString());

  CommandInfo commandInfo;
  CommandInfo::URI* uri = commandInfo.add_uris();
  uri->set_value(::uri::from_path(path.get() + ".tar"));
  uri->set_extract(true);

  node::Flags flags = CreateNodeFlags();

  Fetcher fetcher(flags);

  Future<Nothing> fetch = fetcher.fetch(
      containerId, commandInfo, os::getcwd(), None());

  AWAIT_READY(fetch);

  ASSERT_TRUE(os::exists(path::join(os::getcwd(), path.get())));

  ASSERT_SOME_EQ("hello tar", os::read(path::join(os::getcwd(), path.get())));

  verifyMetrics(1, 0);
}


// use a code library to provide 'gzip' functionality programmatically,
// see MESOS-8064.
TEST_F(FetcherTest, ExtractGzipFile)
{
  // First construct a temporary file that can be fetched and archived with
  // gzip.
  Try<string> dir = os::mkdtemp(path::join(os::getcwd(), "XXXXXX"));
  ASSERT_SOME(dir);

  Try<string> path = os::mktemp(path::join(dir.get(), "XXXXXX"));
  ASSERT_SOME(path);

  ASSERT_SOME(os::write(path.get(), "hello world"));
  ASSERT_SOME(os::shell("gzip " + path.get()));

  ContainerID containerId;
  containerId.set_value(id::UUID::random().toString());

  CommandInfo commandInfo;
  CommandInfo::URI* uri = commandInfo.add_uris();
  uri->set_value(path.get() + ".gz");
  uri->set_extract(true);

  node::Flags flags = CreateNodeFlags();

  Fetcher fetcher(flags);

  Future<Nothing> fetch = fetcher.fetch(
      containerId, commandInfo, os::getcwd(), None());

  AWAIT_READY(fetch);

  string extractedFile = path::join(os::getcwd(), Path(path.get()).basename());
  ASSERT_TRUE(os::exists(extractedFile));

  ASSERT_SOME_EQ("hello world", os::read(extractedFile));

  verifyMetrics(1, 0);
}


TEST_F(FetcherTest, UNZIP_ExtractFile)
{
  // Construct a tmp file that can be fetched and archived with zip.
  string fromDir = path::join(os::getcwd(), "from");
  ASSERT_SOME(os::mkdir(fromDir));

  Try<string> path = os::mktemp(path::join(fromDir, "XXXXXX"));
  ASSERT_SOME(path);

  //  Length     Size  Cmpr    Date    Time   CRC-32   Name    Content
  // --------  ------- ---- ---------- ----- --------  ----    ------
  //       12       12   0% 2016-03-19 10:08 af083b2d  hello   hello world\n
  // --------  -------  ---                            ------- ------
  //       12       12   0%                            1 file
  ASSERT_SOME(os::write(path.get(), base64::decode(
      "UEsDBAoAAAAAABBRc0gtOwivDAAAAAwAAAAFABwAaGVsbG9VVAkAAxAX7VYQ"
      "F+1WdXgLAAEE6AMAAARkAAAAaGVsbG8gd29ybGQKUEsBAh4DCgAAAAAAEFFz"
      "SC07CK8MAAAADAAAAAUAGAAAAAAAAQAAAKSBAAAAAGhlbGxvVVQFAAMQF+1W"
      "dXgLAAEE6AMAAARkAAAAUEsFBgAAAAABAAEASwAAAEsAAAAAAA==").get()));

  ASSERT_SOME(os::rename(path.get(), path.get() + ".zip"));

  ContainerID containerId;
  containerId.set_value(id::UUID::random().toString());

  CommandInfo commandInfo;
  CommandInfo::URI* uri = commandInfo.add_uris();
  uri->set_value(::uri::from_path(path.get() + ".zip"));
  uri->set_extract(true);

  node::Flags flags = CreateNodeFlags();

  Fetcher fetcher(flags);

  Future<Nothing> fetch = fetcher.fetch(
      containerId,
      commandInfo,
      os::getcwd(),
      None());

  AWAIT_READY(fetch);

  string extractedFile = path::join(os::getcwd(), "hello");
  ASSERT_TRUE(os::exists(extractedFile));

  ASSERT_SOME_EQ("hello world\n", os::read(extractedFile));

  verifyMetrics(1, 0);
}


TEST_F(FetcherTest, UNZIP_ExtractInvalidFile)
{
  // Construct a tmp file that can be filled with broken zip.
  string fromDir = path::join(os::getcwd(), "from");
  ASSERT_SOME(os::mkdir(fromDir));

  Try<string> path = os::mktemp(path::join(fromDir, "XXXXXX"));
  ASSERT_SOME(path);

  // Write broken zip to file [bad CRC 440a6aa5  (should be af083b2d)].
  //  Length     Date    Time  CRC expected  CRC actual  Name    Content
  // -------- ---------- ----- ------------  ----------  ----    ------
  //       12 2016-03-19 10:08  af083b2d     440a6aa5    world   hello hello\n
  // --------                                            ------- ------
  //       12                                            1 file
  ASSERT_SOME(os::write(path.get(), base64::decode(
      "UEsDBAoAAAAAABBRc0gtOwivDAAAAAwAAAAFABwAd29ybG9VVAkAAxAX7VYQ"
      "F+1WdXgLAAEE6AMAAARkAAAAaGVsbG8gaGVsbG8KUEsBAh4DCgAAAAAAEFFz"
      "SC07CK8MAAAADAAAAAUAGAAAAAAAAQAAAKSBAAAAAHdvcmxkVVQFAAMQF+1W"
      "dXgLAAEE6AMAAARkAAAAUEsFBgAAAAABAAEASwAAAEsAAAAAAA==").get()));

  ASSERT_SOME(os::rename(path.get(), path.get() + ".zip"));

  ContainerID containerId;
  containerId.set_value(id::UUID::random().toString());

  CommandInfo commandInfo;
  CommandInfo::URI* uri = commandInfo.add_uris();
  uri->set_value(::uri::from_path(path.get() + ".zip"));
  uri->set_extract(true);

  node::Flags flags = CreateNodeFlags();

  Fetcher fetcher(flags);

  Future<Nothing> fetch = fetcher.fetch(
      containerId,
      commandInfo,
      os::getcwd(),
      None());

  AWAIT_FAILED(fetch);
}


TEST_F(FetcherTest, UNZIP_ExtractFileWithDuplicatedEntries)
{
  // Construct a tmp file that can be filled with zip containing
  // duplicates.
  string fromDir = path::join(os::getcwd(), "from");
  ASSERT_SOME(os::mkdir(fromDir));

  Try<string> path = os::mktemp(path::join(fromDir, "XXXXXX"));
  ASSERT_SOME(path);

  // Create zip file with duplicates.
  //   Length  Method    Size  Cmpr    Date    Time   CRC-32   Name   Content
  // --------  ------  ------- ---- ---------- ----- --------  ----   -------
  //       1   Stored        1   0% 2016-03-18 22:49 83dcefb7  A          1
  //       1   Stored        1   0% 2016-03-18 22:49 1ad5be0d  A          2
  // --------          -------  ---                           ------- -------
  //       2                2   0%                            2 files
  ASSERT_SOME(os::write(path.get(), base64::decode(
      "UEsDBBQAAAAAADC2cki379yDAQAAAAEAAAABAAAAQTFQSwMEFAAAAAAAMrZy"
      "SA2+1RoBAAAAAQAAAAEAAABBMlBLAQIUAxQAAAAAADC2cki379yDAQAAAAEA"
      "AAABAAAAAAAAAAAAAACAAQAAAABBUEsBAhQDFAAAAAAAMrZySA2+1RoBAAAA"
      "AQAAAAEAAAAAAAAAAAAAAIABIAAAAEFQSwUGAAAAAAIAAgBeAAAAQAAAAAAA").get()));

  ASSERT_SOME(os::rename(path.get(), path.get() + ".zip"));

  ContainerID containerId;
  containerId.set_value(id::UUID::random().toString());

  CommandInfo commandInfo;
  CommandInfo::URI* uri = commandInfo.add_uris();
  uri->set_value(::uri::from_path(path.get() + ".zip"));
  uri->set_extract(true);

  node::Flags flags = CreateNodeFlags();

  Fetcher fetcher(flags);

  Future<Nothing> fetch = fetcher.fetch(
      containerId,
      commandInfo,
      os::getcwd(),
      None());

  AWAIT_READY(fetch);

  string extractedFile = path::join(os::getcwd(), "A");
  ASSERT_TRUE(os::exists(extractedFile));

  ASSERT_SOME_EQ("2", os::read(extractedFile));

  verifyMetrics(1, 0);
}


TEST_F(FetcherTest, UseCustomOutputFile)
{
  // First construct a temporary file that can be fetched.
  Try<string> dir = os::mkdtemp(path::join(os::getcwd(), "XXXXXX"));
  ASSERT_SOME(dir);

  Try<string> path = os::mktemp(path::join(dir.get(), "XXXXXX"));
  ASSERT_SOME(path);

  ASSERT_SOME(os::write(path.get(), "hello renamed file"));

  ContainerID containerId;
  containerId.set_value(id::UUID::random().toString());

  const string customOutputFile = "custom.txt";
  CommandInfo commandInfo;
  CommandInfo::URI* uri = commandInfo.add_uris();
  uri->set_value(::uri::from_path(path.get()));
  uri->set_extract(true);
  uri->set_output_file(customOutputFile);

  node::Flags flags = CreateNodeFlags();

  Fetcher fetcher(flags);

  Future<Nothing> fetch = fetcher.fetch(
      containerId, commandInfo, os::getcwd(), None());

  AWAIT_READY(fetch);

  ASSERT_TRUE(os::exists(path::join(".", customOutputFile)));

  ASSERT_SOME_EQ(
      "hello renamed file", os::read(path::join(".", customOutputFile)));

  verifyMetrics(1, 0);
}


TEST_F(FetcherTest, CustomGzipOutputFile)
{
  // First construct a temporary file that can be fetched.
  Try<string> dir = os::mkdtemp(path::join(os::getcwd(), "XXXXXX"));
  ASSERT_SOME(dir);

  Try<string> path = os::mktemp(path::join(dir.get(), "XXXXXX"));
  ASSERT_SOME(path);

  ASSERT_SOME(os::write(path.get(), "hello renamed gzip file"));
  ASSERT_SOME(os::shell("gzip " + path.get()));

  ContainerID containerId;
  containerId.set_value(id::UUID::random().toString());

  const string customOutputFile = "custom";
  CommandInfo commandInfo;
  CommandInfo::URI* uri = commandInfo.add_uris();
  uri->set_value(path.get() + ".gz");
  uri->set_extract(true);
  uri->set_output_file(customOutputFile + ".gz");

  node::Flags flags = CreateNodeFlags();

  Fetcher fetcher(flags);

  Future<Nothing> fetch = fetcher.fetch(
      containerId, commandInfo, os::getcwd(), None());

  AWAIT_READY(fetch);

  string extractFile = path::join(".", customOutputFile);
  ASSERT_TRUE(os::exists(extractFile));

  ASSERT_SOME_EQ("hello renamed gzip file", os::read(extractFile));

  verifyMetrics(1, 0);
}

// Regression test against unwanted environment inheritance from the
// node towards the fetcher. By supplying an invalid SSL setup, we
// force the fetcher to fail if the parent process does not filter
// them out.
TEST_F(FetcherTest, SSLEnvironmentSpillover)
{
  // Patch some critical libprocess environment variables into the
  // parent process of the medios-fetcher. We expect this test to fail
  // when the code path triggered does not filter them.
  char* enabled = getenv("LIBPROCESS_SSL_ENABLED");
  char* key = getenv("LIBPROCESS_SSL_KEY_FILE");

  os::setenv("LIBPROCESS_SSL_ENABLED", "true");
  os::unsetenv("LIBPROCESS_SSL_KEY_FILE");

  // First construct a temporary file that can be fetched and archived with
  // gzip.
  Try<string> dir = os::mkdtemp(path::join(os::getcwd(), "XXXXXX"));
  ASSERT_SOME(dir);

  Try<string> path = os::mktemp(path::join(dir.get(), "XXXXXX"));
  ASSERT_SOME(path);

  ASSERT_SOME(os::write(path.get(), "hello world"));
  ASSERT_SOME(os::shell("gzip " + path.get()));

  ContainerID containerId;
  containerId.set_value(id::UUID::random().toString());

  CommandInfo commandInfo;
  CommandInfo::URI* uri = commandInfo.add_uris();
  uri->set_value(path.get() + ".gz");
  uri->set_extract(true);

  node::Flags flags = CreateNodeFlags();

  Fetcher fetcher(flags);

  Future<Nothing> fetch = fetcher.fetch(
      containerId, commandInfo, os::getcwd(), None());

  // The medios-fetcher runnable will fail initializing libprocess if
  // the SSL environment spilled over. Such failure would cause it to
  // abort and exit and that in turn would fail the `fetch` returned
  // future.
  AWAIT_READY(fetch);

  if (enabled != nullptr) {
    os::setenv("LIBPROCESS_SSL_ENABLED", enabled);
  } else {
    os::unsetenv("LIBPROCESS_SSL_ENABLED");
  }

  if (key != nullptr) {
    os::setenv("LIBPROCESS_SSL_KEY_FILE", key);
  }
}


// Verifies that cloud metadata, link-local, and non-HTTP(S) targets are
// rejected before `curl` runs, and that `MEDIOS_FETCHER_DISALLOW_PRIVATE_IPS`
// additionally rejects loopback and RFC1918 targets (#386).
TEST_F(FetcherTest, RejectCloudMetadataAndLinkLocalTargets)
{
  EXPECT_SOME(medios::uri::validateUrlTarget(
      "http://169.254.169.254/latest/meta-data/"));
  EXPECT_SOME(medios::uri::validateUrlTarget(
      "http://2852039166/latest/meta-data/"));
  EXPECT_SOME(medios::uri::validateUrlTarget(
      "http://0xa9fea9fe/latest/meta-data/"));
  EXPECT_SOME(medios::uri::validateUrlTarget(
      "http://0251.0376.0251.0376/latest/meta-data/"));
  EXPECT_SOME(medios::uri::validateUrlTarget(
      "http://100.100.100.200/latest/meta-data/"));
  EXPECT_SOME(medios::uri::validateUrlTarget(
      "http://metadata.google.internal/computeMetadata/v1/"));
  EXPECT_SOME(medios::uri::validateUrlTarget(
      "http://[fe80::1]/latest/meta-data/"));
  EXPECT_SOME(medios::uri::validateUrlTarget(
      "http://[fd00:ec2::254]/latest/meta-data/"));
  EXPECT_SOME(medios::uri::validateUrlTarget(
      "http://[::ffff:169.254.169.254]/latest/meta-data/"));
  EXPECT_SOME(medios::uri::validateUrlTarget(
      "http://%31%36%39.254.169.254/latest/meta-data/"));
  EXPECT_SOME(medios::uri::validateUrlTarget(
      "http://metadata.google.%69nternal/computeMetadata/v1/"));
  EXPECT_SOME(medios::uri::validateUrlTarget(
      "http://169.254.169.254\\@example.com/latest/meta-data/"));
  EXPECT_SOME(medios::uri::validateUrlTarget(
      "http://[::1]@169.254.169.254/latest/meta-data/"));
  EXPECT_SOME(medios::uri::validateUrlTarget(
      "ftp://example.com/artifact.tar.gz"));

  EXPECT_ERROR(Fetcher::validateUri(
      "http://169.254.169.254/latest/meta-data/iam"));
  EXPECT_ERROR(Fetcher::validateUri(
      "http://metadata.google.internal/computeMetadata/v1/instance"));

  // Loopback is allowed by default for local dev and unit tests.
  EXPECT_NONE(medios::uri::validateUrlTarget("http://127.0.0.1:5050/artifact"));

  // Mode-3 strict cloud hardening blocks loopback and RFC1918 literals.
  os::setenv("MEDIOS_FETCHER_DISALLOW_PRIVATE_IPS", "true");
  EXPECT_SOME(medios::uri::validateUrlTarget("http://127.0.0.1:5050/artifact"));
  EXPECT_SOME(medios::uri::validateUrlTarget("http://localhost:5050/artifact"));
  EXPECT_SOME(medios::uri::validateUrlTarget("http://10.0.0.1/artifact"));
  EXPECT_SOME(medios::uri::validateUrlTarget("http://172.16.0.1/artifact"));
  EXPECT_SOME(medios::uri::validateUrlTarget("http://192.168.1.1/artifact"));
  EXPECT_SOME(medios::uri::validateUrlTarget("http://[::1]:5050/artifact"));
  EXPECT_NONE(medios::uri::validateUrlTarget("https://example.com/artifact"));
  os::unsetenv("MEDIOS_FETCHER_DISALLOW_PRIVATE_IPS");
}


// Verifies that `--fetcher_allow_local_uris=false` rejects local `file://`
// URIs and `--fetcher_allowed_local_uri_prefix` confines local fetches to
// canonical paths inside the configured directory (#399).
TEST_F(FetcherTest, LocalUriAllowlistAndPrefixConfinement)
{
  const string allowedDir = path::join(os::getcwd(), "allowed");
  const string outsideDir = path::join(os::getcwd(), "allowed-outside");
  ASSERT_SOME(os::mkdir(allowedDir));
  ASSERT_SOME(os::mkdir(outsideDir));

  const string allowedFile = path::join(allowedDir, "ok.txt");
  const string outsideFile = path::join(outsideDir, "secret.txt");
  const string symlinkEscape = path::join(allowedDir, "escape.txt");
  ASSERT_SOME(os::write(allowedFile, "allowed"));
  ASSERT_SOME(os::write(outsideFile, "secret"));
  ASSERT_SOME(fs::symlink(outsideFile, symlinkEscape));

  // 1. Disallowing local URIs rejects even `allowedFile`.
  {
    node::Flags flags = CreateNodeFlags();
    flags.fetcher_allow_local_uris = false;

    Fetcher fetcher(flags);
    ContainerID containerId;
    containerId.set_value(id::UUID::random().toString());

    CommandInfo commandInfo;
    commandInfo.add_uris()->set_value(::uri::from_path(allowedFile));

    AWAIT_FAILED(fetcher.fetch(containerId, commandInfo, os::getcwd(), None()));
  }

  // 2. Prefix confinement allows `allowedFile` and rejects `outsideFile` and
  //    `symlinkEscape`.
  {
    node::Flags flags = CreateNodeFlags();
    flags.fetcher_allow_local_uris = true;
    flags.fetcher_allowed_local_uri_prefix = allowedDir;

    Fetcher fetcher(flags);

    ContainerID containerId1;
    containerId1.set_value(id::UUID::random().toString());
    CommandInfo okCmd;
    okCmd.add_uris()->set_value(::uri::from_path(allowedFile));
    AWAIT_READY(fetcher.fetch(containerId1, okCmd, os::getcwd(), None()));

    ContainerID containerId2;
    containerId2.set_value(id::UUID::random().toString());
    CommandInfo outsideCmd;
    outsideCmd.add_uris()->set_value(::uri::from_path(outsideFile));
    AWAIT_FAILED(fetcher.fetch(containerId2, outsideCmd, os::getcwd(), None()));

    ContainerID containerId3;
    containerId3.set_value(id::UUID::random().toString());
    CommandInfo symlinkCmd;
    symlinkCmd.add_uris()->set_value(::uri::from_path(symlinkEscape));
    AWAIT_FAILED(fetcher.fetch(containerId3, symlinkCmd, os::getcwd(), None()));
  }
}

} // namespace tests {
} // namespace internal {
} // namespace medios {
