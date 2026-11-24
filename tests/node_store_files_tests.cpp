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

// A node serves every store it declares read-only through its files API
// at /stores/<name> (DR-042, medios#585): a store is delivered by the
// node, so the node can read it, and a framework can read what a task
// left in a store from the node that ran it, without a host path.

#include <string>

#include <gmock/gmock.h>

#include <gtest/gtest.h>

#include <medios/executor.hpp>
#include <medios/medios.hpp>

#include <process/future.hpp>
#include <process/gtest.hpp>
#include <process/http.hpp>
#include <process/owned.hpp>
#include <process/pid.hpp>

#include <stout/foreach.hpp>
#include <stout/fs.hpp>
#include <stout/gtest.hpp>
#include <stout/json.hpp>
#include <stout/os.hpp>
#include <stout/path.hpp>
#include <stout/strings.hpp>
#include <stout/try.hpp>

#include "node/constants.hpp"
#include "node/flags.hpp"

#include "tests/containerizer.hpp"
#include "tests/medios.hpp"

using medios::controller::detector::ControllerDetector;

using process::Future;
using process::Owned;
using process::UPID;

using medios::internal::tests::TestContainerizer;

using process::http::BadRequest;
using process::http::NotFound;
using process::http::OK;
using process::http::Response;

using std::string;

namespace medios {
namespace internal {
namespace tests {

class NodeStoreFilesTest : public MediosTest
{
protected:
  // One store, `tables`, holding a manifest and a symlink that points
  // outside the store; a stores file declaring it.
  void SetUp() override
  {
    MediosTest::SetUp();

    tables = path::join(sandbox.get(), "tables");
    ASSERT_SOME(os::mkdir(tables));
    ASSERT_SOME(os::write(
        path::join(tables, "manifest.json"), "{\"attempt\": 7}\n"));

    // A sibling whose name extends the store's, so a prefix comparison
    // would serve it; the guard compares at a path boundary.
    outside = path::join(sandbox.get(), "tables-backup");
    ASSERT_SOME(os::mkdir(outside));
    ASSERT_SOME(os::write(path::join(outside, "secret"), "not served\n"));
    ASSERT_SOME(fs::symlink(outside, path::join(tables, "escape")));

    storesFile = path::join(sandbox.get(), "stores.json");
    ASSERT_SOME(os::write(storesFile,
        "{ \"stores\": [ { \"name\": \"tables\", \"path\": \"" + tables +
        "\", \"class\": \"local\", \"locality\": \"near\", "
        "\"readers\": 2, \"writers\": 1 } ] }"));
  }

  // Starts a controller and a node, with the stores file unless told not
  // to, and returns the node's files process.
  UPID startNode(bool withStores)
  {
    Try<Owned<cluster::Controller>> _controller = StartController();
    CHECK_SOME(_controller);
    controller = _controller.get();

    // A node that declares stores must deliver them: the linux launcher
    // and the filesystem isolator by name, with a mock containerizer so
    // nothing is loaded from them; the files API needs neither.
    node::Flags flags = CreateNodeFlags();
    if (withStores) {
      flags.stores = storesFile;
      flags.launcher = "linux";
      flags.isolation = "filesystem/linux";
    }

    detector = controller->createDetector();
    Try<Owned<cluster::Node>> _node =
      StartNode(detector.get(), &containerizer, flags);
    CHECK_SOME(_node);
    node = _node.get();

    return UPID("files", node->pid.address);
  }

  Future<Response> files(
      const UPID& files, const string& endpoint, const string& query)
  {
    return process::http::get(
        files, endpoint, query, createBasicAuthHeaders(DEFAULT_CREDENTIAL));
  }

  string tables;
  string outside;
  string storesFile;
  MockExecutor exec{DEFAULT_EXECUTOR_ID};
  TestContainerizer containerizer{&exec};
  Owned<cluster::Controller> controller;
  Owned<ControllerDetector> detector;
  Owned<cluster::Node> node;
};


// The store's directory is browsable and its files readable under
// /stores/<name>, through the same endpoints a sandbox is read through.
TEST_F(NodeStoreFilesTest, BrowseAndReadUnderTheStore)
{
  UPID filesPid = startNode(true);

  Future<Response> browse =
    files(filesPid, "browse", "path=/stores/tables");
  AWAIT_ASSERT_RESPONSE_STATUS_EQ(OK().status, browse);

  Try<JSON::Array> listing = JSON::parse<JSON::Array>(browse->body);
  ASSERT_SOME(listing);

  bool manifestListed = false;
  foreach (const JSON::Value& entry, listing->values) {
    ASSERT_TRUE(entry.is<JSON::Object>());
    Result<JSON::String> path =
      entry.as<JSON::Object>().find<JSON::String>("path");
    ASSERT_SOME(path);
    if (path->value == "/stores/tables/manifest.json") {
      manifestListed = true;
    }
  }
  EXPECT_TRUE(manifestListed) << browse->body;

  Future<Response> read =
    files(filesPid, "read", "path=/stores/tables/manifest.json&offset=0");
  AWAIT_ASSERT_RESPONSE_STATUS_EQ(OK().status, read);

  Try<JSON::Object> contents = JSON::parse<JSON::Object>(read->body);
  ASSERT_SOME(contents);
  Result<JSON::String> data = contents->find<JSON::String>("data");
  ASSERT_SOME(data);
  EXPECT_EQ("{\"attempt\": 7}\n", data->value);
}


// Download is the route for an artifact larger than a read window; it
// serves the file whole under /stores/<name> and refuses an escape as
// the other two routes do.
TEST_F(NodeStoreFilesTest, DownloadUnderTheStore)
{
  UPID filesPid = startNode(true);

  Future<Response> download =
    files(filesPid, "download", "path=/stores/tables/manifest.json");
  AWAIT_ASSERT_RESPONSE_STATUS_EQ(OK().status, download);
  EXPECT_EQ("{\"attempt\": 7}\n", download->body);

  Future<Response> escape =
    files(filesPid, "download", "path=/stores/tables/escape/secret");
  AWAIT_ASSERT_RESPONSE_STATUS_EQ(BadRequest().status, escape);
  EXPECT_TRUE(strings::contains(escape->body, "inaccessible"))
    << escape->body;
}


// A path that leaves the store, through a symlink or `..`, is refused
// with a sentence; the file it points at is not served.
TEST_F(NodeStoreFilesTest, AnEscapeFromTheStoreIsRefused)
{
  UPID filesPid = startNode(true);

  Future<Response> symlink =
    files(filesPid, "read", "path=/stores/tables/escape/secret&offset=0");
  AWAIT_ASSERT_RESPONSE_STATUS_EQ(BadRequest().status, symlink);
  EXPECT_TRUE(strings::contains(symlink->body, "inaccessible"))
    << symlink->body;

  Future<Response> dotdot =
    files(filesPid, "browse", "path=/stores/tables/../tables-backup");
  AWAIT_ASSERT_RESPONSE_STATUS_EQ(BadRequest().status, dotdot);
  EXPECT_TRUE(strings::contains(dotdot->body, "inaccessible"))
    << dotdot->body;
}


// A node with no stores file attaches nothing under /stores.
TEST_F(NodeStoreFilesTest, NothingIsAttachedWithoutStores)
{
  UPID filesPid = startNode(false);

  Future<Response> browse =
    files(filesPid, "browse", "path=/stores/tables");
  AWAIT_ASSERT_RESPONSE_STATUS_EQ(NotFound().status, browse);
}

} // namespace tests {
} // namespace internal {
} // namespace medios {
