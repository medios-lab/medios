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

#include <list>
#include <set>
#include <string>
#include <vector>

#include <gmock/gmock.h>

#include <medios/medios.hpp>
#include <medios/type_utils.hpp>

#include <medios/state/in_memory.hpp>
#include <medios/state/leveldb.hpp>
#include <medios/state/protobuf.hpp>
#include <medios/state/storage.hpp>

#include <process/future.hpp>
#include <process/gtest.hpp>
#include <process/protobuf.hpp>
#include <process/pid.hpp>

#include <stout/gtest.hpp>
#include <stout/option.hpp>
#include <stout/os.hpp>
#include <stout/try.hpp>

#include <stout/tests/utils.hpp>

#include "controller/registry.hpp"

#include "messages/state.hpp"

using namespace process;

using std::list;
using std::set;
using std::string;
using std::vector;

using medios::state::Storage;
using medios::state::LevelDBStorage;

using medios::state::protobuf::State;
using medios::state::protobuf::Variable;

namespace medios {
namespace internal {
namespace tests {

typedef medios::internal::Registry::Nodes Nodes;
typedef medios::internal::Registry::Node Node;

// We declare this here to avoid collision with the top-level
// `medios::Operation` protobuf message
using medios::internal::state::Operation;

void FetchAndStoreAndFetch(State* state)
{
  Future<Variable<Nodes>> future1 = state->fetch<Nodes>("nodes");
  AWAIT_READY(future1);

  Variable<Nodes> variable = future1.get();

  Nodes nodes1 = variable.get();
  ASSERT_TRUE(nodes1.nodes().empty());

  Node* node = nodes1.add_nodes();
  node->mutable_info()->set_hostname("localhost");

  variable = variable.mutate(nodes1);

  Future<Option<Variable<Nodes>>> future2 = state->store(variable);
  AWAIT_READY(future2);
  ASSERT_SOME(future2.get());

  future1 = state->fetch<Nodes>("nodes");
  AWAIT_READY(future1);

  variable = future1.get();

  Nodes nodes2 = variable.get();
  ASSERT_EQ(1, nodes2.nodes().size());
  EXPECT_EQ("localhost", nodes2.nodes(0).info().hostname());
}


void FetchAndStoreAndStoreAndFetch(State* state)
{
  Future<Variable<Nodes>> future1 = state->fetch<Nodes>("nodes");
  AWAIT_READY(future1);

  Variable<Nodes> variable = future1.get();

  Nodes nodes1 = variable.get();
  ASSERT_TRUE(nodes1.nodes().empty());

  Node* node = nodes1.add_nodes();
  node->mutable_info()->set_hostname("localhost");

  variable = variable.mutate(nodes1);

  Future<Option<Variable<Nodes>>> future2 = state->store(variable);
  AWAIT_READY(future2);
  ASSERT_SOME(future2.get());

  variable = future2->get();

  future2 = state->store(variable);
  AWAIT_READY(future2);
  ASSERT_SOME(future2.get());

  future1 = state->fetch<Nodes>("nodes");
  AWAIT_READY(future1);

  variable = future1.get();

  Nodes nodes2 = variable.get();
  ASSERT_EQ(1, nodes2.nodes().size());
  EXPECT_EQ("localhost", nodes2.nodes(0).info().hostname());
}


void FetchAndStoreAndStoreFailAndFetch(State* state)
{
  Future<Variable<Nodes>> future1 = state->fetch<Nodes>("nodes");
  AWAIT_READY(future1);

  Variable<Nodes> variable1 = future1.get();

  Nodes nodes1 = variable1.get();
  ASSERT_TRUE(nodes1.nodes().empty());

  Node* node1 = nodes1.add_nodes();
  node1->mutable_info()->set_hostname("localhost1");

  Variable<Nodes> variable2 = variable1.mutate(nodes1);

  Future<Option<Variable<Nodes>>> future2 = state->store(variable2);
  AWAIT_READY(future2);
  ASSERT_SOME(future2.get());

  Nodes nodes2 = variable1.get();
  ASSERT_TRUE(nodes2.nodes().empty());

  Node* node2 = nodes2.add_nodes();
  node2->mutable_info()->set_hostname("localhost2");

  variable2 = variable1.mutate(nodes2);

  future2 = state->store(variable2);
  AWAIT_READY(future2);
  EXPECT_NONE(future2.get());

  future1 = state->fetch<Nodes>("nodes");
  AWAIT_READY(future1);

  variable1 = future1.get();

  nodes1 = variable1.get();
  ASSERT_EQ(1, nodes1.nodes().size());
  EXPECT_EQ("localhost1", nodes1.nodes(0).info().hostname());
}


void FetchAndStoreAndExpungeAndFetch(State* state)
{
  Future<Variable<Nodes>> future1 = state->fetch<Nodes>("nodes");
  AWAIT_READY(future1);

  Variable<Nodes> variable = future1.get();

  Nodes nodes1 = variable.get();
  ASSERT_TRUE(nodes1.nodes().empty());

  Node* node = nodes1.add_nodes();
  node->mutable_info()->set_hostname("localhost");

  variable = variable.mutate(nodes1);

  Future<Option<Variable<Nodes>>> future2 = state->store(variable);
  AWAIT_READY(future2);
  ASSERT_SOME(future2.get());

  variable = future2->get();

  Future<bool> future3 = state->expunge(variable);
  AWAIT_READY(future3);
  ASSERT_TRUE(future3.get());

  future1 = state->fetch<Nodes>("nodes");
  AWAIT_READY(future1);

  variable = future1.get();

  Nodes nodes2 = variable.get();
  ASSERT_TRUE(nodes2.nodes().empty());
}


void FetchAndStoreAndExpungeAndExpunge(State* state)
{
  Future<Variable<Nodes>> future1 = state->fetch<Nodes>("nodes");
  AWAIT_READY(future1);

  Variable<Nodes> variable = future1.get();

  Nodes nodes1 = variable.get();
  ASSERT_TRUE(nodes1.nodes().empty());

  Node* node = nodes1.add_nodes();
  node->mutable_info()->set_hostname("localhost");

  variable = variable.mutate(nodes1);

  Future<Option<Variable<Nodes>>> future2 = state->store(variable);
  AWAIT_READY(future2);
  ASSERT_SOME(future2.get());

  variable = future2->get();

  Future<bool> future3 = state->expunge(variable);
  AWAIT_READY(future3);
  ASSERT_TRUE(future3.get());

  future3 = state->expunge(variable);
  AWAIT_READY(future3);
  ASSERT_FALSE(future3.get());
}


void FetchAndStoreAndExpungeAndStoreAndFetch(State* state)
{
  Future<Variable<Nodes>> future1 = state->fetch<Nodes>("nodes");
  AWAIT_READY(future1);

  Variable<Nodes> variable = future1.get();

  Nodes nodes1 = variable.get();
  ASSERT_TRUE(nodes1.nodes().empty());

  Node* node = nodes1.add_nodes();
  node->mutable_info()->set_hostname("localhost");

  variable = variable.mutate(nodes1);

  Future<Option<Variable<Nodes>>> future2 = state->store(variable);
  AWAIT_READY(future2);
  ASSERT_SOME(future2.get());

  variable = future2->get();

  Future<bool> future3 = state->expunge(variable);
  AWAIT_READY(future3);
  ASSERT_TRUE(future3.get());

  future2 = state->store(variable);
  AWAIT_READY(future2);
  ASSERT_SOME(future2.get());

  future1 = state->fetch<Nodes>("nodes");
  AWAIT_READY(future1);

  variable = future1.get();

  Nodes nodes2 = variable.get();
  ASSERT_EQ(1, nodes2.nodes().size());
  EXPECT_EQ("localhost", nodes2.nodes(0).info().hostname());
}


void Names(State* state)
{
  Future<Variable<Nodes>> future1 = state->fetch<Nodes>("nodes");
  AWAIT_READY(future1);

  Variable<Nodes> variable = future1.get();

  Nodes nodes1 = variable.get();
  ASSERT_TRUE(nodes1.nodes().empty());

  Node* node = nodes1.add_nodes();
  node->mutable_info()->set_hostname("localhost");

  variable = variable.mutate(nodes1);

  Future<Option<Variable<Nodes>>> future2 = state->store(variable);
  AWAIT_READY(future2);
  ASSERT_SOME(future2.get());

  Future<set<string>> names = state->names();
  AWAIT_READY(names);
  ASSERT_EQ(1u, names->size());
  EXPECT_NE(names->find("nodes"), names->end());
}


class InMemoryStateTest : public ::testing::Test
{
public:
  InMemoryStateTest()
    : storage(nullptr),
      state(nullptr) {}

protected:
  void SetUp() override
  {
    storage = new medios::state::InMemoryStorage();
    state = new State(storage);
  }

  void TearDown() override
  {
    delete state;
    delete storage;
  }

  medios::state::Storage* storage;
  State* state;
};


TEST_F(InMemoryStateTest, FetchAndStoreAndFetch)
{
  FetchAndStoreAndFetch(state);
}


TEST_F(InMemoryStateTest, FetchAndStoreAndStoreAndFetch)
{
  FetchAndStoreAndStoreAndFetch(state);
}


TEST_F(InMemoryStateTest, FetchAndStoreAndStoreFailAndFetch)
{
  FetchAndStoreAndStoreFailAndFetch(state);
}


TEST_F(InMemoryStateTest, FetchAndStoreAndExpungeAndFetch)
{
  FetchAndStoreAndExpungeAndFetch(state);
}


TEST_F(InMemoryStateTest, FetchAndStoreAndExpungeAndExpunge)
{
  FetchAndStoreAndExpungeAndExpunge(state);
}


TEST_F(InMemoryStateTest, FetchAndStoreAndExpungeAndStoreAndFetch)
{
  FetchAndStoreAndExpungeAndStoreAndFetch(state);
}


TEST_F(InMemoryStateTest, Names)
{
  Names(state);
}


class LevelDBStateTest : public TemporaryDirectoryTest
{
public:
  LevelDBStateTest()
    : storage(nullptr),
      state(nullptr) {}

protected:
  void SetUp() override
  {
    TemporaryDirectoryTest::SetUp();

    ASSERT_SOME(sandbox);
    path = sandbox.get() + "/.state";

    storage = new medios::state::LevelDBStorage(path);
    state = new State(storage);
  }

  void TearDown() override
  {
    delete state;
    delete storage;

    TemporaryDirectoryTest::TearDown();
  }

  medios::state::Storage* storage;
  State* state;

private:
  string path;
};


TEST_F(LevelDBStateTest, FetchAndStoreAndFetch)
{
  FetchAndStoreAndFetch(state);
}


TEST_F(LevelDBStateTest, FetchAndStoreAndStoreAndFetch)
{
  FetchAndStoreAndStoreAndFetch(state);
}


TEST_F(LevelDBStateTest, FetchAndStoreAndStoreFailAndFetch)
{
  FetchAndStoreAndStoreFailAndFetch(state);
}


TEST_F(LevelDBStateTest, FetchAndStoreAndExpungeAndFetch)
{
  FetchAndStoreAndExpungeAndFetch(state);
}


TEST_F(LevelDBStateTest, FetchAndStoreAndExpungeAndExpunge)
{
  FetchAndStoreAndExpungeAndExpunge(state);
}


TEST_F(LevelDBStateTest, FetchAndStoreAndExpungeAndStoreAndFetch)
{
  FetchAndStoreAndExpungeAndStoreAndFetch(state);
}


TEST_F(LevelDBStateTest, Names)
{
  Names(state);
}


} // namespace tests {
} // namespace internal {
} // namespace medios {
