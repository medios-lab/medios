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

// The task record's side of `TaskInfo.observers` (DR-041): the controller
// copies the observers onto the `Task` it keeps, they are part of what
// makes two task records equal, and they are rendered on the task.

#include <sstream>
#include <string>

#include <gmock/gmock.h>

#include <gtest/gtest.h>

#include <medios/medios.hpp>
#include <medios/resources.hpp>
#include <medios/type_utils.hpp>

#include <stout/gtest.hpp>
#include <stout/json.hpp>
#include <stout/jsonify.hpp>
#include <stout/stringify.hpp>

#include "common/http.hpp"
#include "common/protobuf_utils.hpp"

#include "tests/medios.hpp"

using std::string;

namespace medios {
namespace internal {
namespace tests {

class TaskObserverRecordTest : public MediosTest
{
protected:
  static TaskObserver observer(const string& name, const string& command)
  {
    TaskObserver declaration;
    declaration.set_name(name);
    declaration.mutable_command()->set_value(command);
    return declaration;
  }

  // The smallest task the controller will build a record from.
  static TaskInfo taskInfo()
  {
    TaskInfo task;
    task.set_name("observed");
    task.mutable_task_id()->set_value("task-1");
    task.mutable_node_id()->set_value("node-1");
    task.mutable_resources()->CopyFrom(
        Resources::parse("cpus:1;mem:128").get());
    task.mutable_command()->set_value("sleep 1000");
    return task;
  }

  static FrameworkID frameworkId()
  {
    FrameworkID id;
    id.set_value("framework-1");
    return id;
  }

  // The task as `/state` renders it, through the same writer.
  static string render(const Task& task)
  {
    std::ostringstream out;
    out << jsonify([&task](JSON::ObjectWriter* writer) {
      medios::json(writer, task);
    });
    return out.str();
  }
};


// A record of a task that carried no observers claims none.
TEST_F(TaskObserverRecordTest, NoObserversAreCopied)
{
  Task task = protobuf::createTask(
      taskInfo(), TASK_RUNNING, frameworkId(), None());

  EXPECT_TRUE(task.observers().empty());
}


// The observers a stall or an observation is read against are the ones the
// task was launched with, so the controller keeps them on the record.
TEST_F(TaskObserverRecordTest, ObserversAreCopiedInOrder)
{
  TaskInfo info = taskInfo();
  info.add_observers()->CopyFrom(observer("haiku", "medios-observer-haiku"));
  info.add_observers()->CopyFrom(observer("gpu-watch", "nvidia-smi -l 10"));

  Task task = protobuf::createTask(
      info, TASK_RUNNING, frameworkId(), None());

  ASSERT_EQ(2, task.observers().size());
  EXPECT_EQ("haiku", task.observers().Get(0).name());
  EXPECT_EQ(
      "medios-observer-haiku", task.observers().Get(0).command().value());
  EXPECT_EQ("gpu-watch", task.observers().Get(1).name());
  EXPECT_EQ("nvidia-smi -l 10", task.observers().Get(1).command().value());
}


TEST_F(TaskObserverRecordTest, RecordsWithTheSameObserversAreEqual)
{
  TaskInfo info = taskInfo();
  info.add_observers()->CopyFrom(observer("haiku", "medios-observer-haiku"));

  Task left = protobuf::createTask(
      info, TASK_RUNNING, frameworkId(), None());
  Task right = protobuf::createTask(
      info, TASK_RUNNING, frameworkId(), None());

  EXPECT_EQ(left, right);
}


TEST_F(TaskObserverRecordTest, RecordsWithDifferentObserversAreNotEqual)
{
  TaskInfo observed = taskInfo();
  observed.add_observers()->CopyFrom(
      observer("haiku", "medios-observer-haiku"));

  Task left = protobuf::createTask(
      observed, TASK_RUNNING, frameworkId(), None());
  Task right = protobuf::createTask(
      taskInfo(), TASK_RUNNING, frameworkId(), None());

  EXPECT_FALSE(left == right);
}


TEST_F(TaskObserverRecordTest, RecordsWithRenamedObserversAreNotEqual)
{
  TaskInfo one = taskInfo();
  one.add_observers()->CopyFrom(observer("haiku", "watch"));

  TaskInfo other = taskInfo();
  other.add_observers()->CopyFrom(observer("sonnet", "watch"));

  Task left = protobuf::createTask(
      one, TASK_RUNNING, frameworkId(), None());
  Task right = protobuf::createTask(
      other, TASK_RUNNING, frameworkId(), None());

  EXPECT_FALSE(left == right);
}


// `/state` is where an observation is read against the command that
// produced it, so the command is on the task there.
TEST_F(TaskObserverRecordTest, ObserversAreRendered)
{
  TaskInfo info = taskInfo();
  info.add_observers()->CopyFrom(observer("haiku", "medios-observer-haiku"));

  Task task = protobuf::createTask(
      info, TASK_RUNNING, frameworkId(), None());

  Try<JSON::Object> rendered = JSON::parse<JSON::Object>(render(task));
  ASSERT_SOME(rendered);

  Result<JSON::Array> observers = rendered->find<JSON::Array>("observers");
  ASSERT_SOME(observers);
  ASSERT_EQ(1u, observers->values.size());

  ASSERT_TRUE(observers->values[0].is<JSON::Object>());
  const JSON::Object one = observers->values[0].as<JSON::Object>();

  Result<JSON::String> name = one.find<JSON::String>("name");
  ASSERT_SOME(name);
  EXPECT_EQ("haiku", name->value);

  Result<JSON::String> command = one.find<JSON::String>("command.value");
  ASSERT_SOME(command);
  EXPECT_EQ("medios-observer-haiku", command->value);
}


TEST_F(TaskObserverRecordTest, NoObserversAreNotRendered)
{
  Task task = protobuf::createTask(
      taskInfo(), TASK_RUNNING, frameworkId(), None());

  Try<JSON::Object> rendered = JSON::parse<JSON::Object>(render(task));
  ASSERT_SOME(rendered);

  EXPECT_NONE(rendered->find<JSON::Array>("observers"));
}

} // namespace tests {
} // namespace internal {
} // namespace medios {
