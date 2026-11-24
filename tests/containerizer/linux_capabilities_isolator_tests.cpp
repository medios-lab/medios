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

#include <ostream>
#include <set>
#include <string>
#include <vector>

#include <gmock/gmock.h>

#include <process/future.hpp>
#include <process/gmock.hpp>
#include <process/gtest.hpp>
#include <process/owned.hpp>
#include <process/queue.hpp>

#include <stout/foreach.hpp>
#include <stout/gtest.hpp>
#include <stout/none.hpp>
#include <stout/nothing.hpp>
#include <stout/option.hpp>
#include <stout/protobuf.hpp>
#include <stout/stringify.hpp>
#include <stout/try.hpp>

#include <medios/medios.hpp>
#include <medios/scheduler.hpp>

#include <medios/controller/detector.hpp>

#include <medios/resources.hpp>

#include "linux/capabilities.hpp"

#include "controller/detector/standalone.hpp"

#include "node/flags.hpp"

#include "tests/cluster.hpp"
#include "tests/medios.hpp"

#include "tests/containerizer/docker_archive.hpp"

using medios::internal::capabilities::Capability;
using medios::internal::capabilities::CHOWN;
using medios::internal::capabilities::DAC_READ_SEARCH;
using medios::internal::capabilities::NET_ADMIN;
using medios::internal::capabilities::NET_RAW;


using medios::controller::detector::ControllerDetector;
using medios::controller::detector::StandaloneControllerDetector;

using process::Future;
using process::Owned;
using process::Queue;

using std::initializer_list;
using std::ostream;
using std::set;
using std::string;
using std::vector;

namespace medios {
namespace internal {
namespace tests {

// Param for the tests:
//   'framework_effective'
//            Framework specified effective capabilities for the container.
//   'operator_effective'
//            Default effective capabilities configured by the operator.
//   'operator_bounding'
//            Default bounding capabilities configured by the  operator
//   'success'
//            True if the task should finish normally.
struct TestParam
{
  enum Result
  {
    FAILURE = 0,
    SUCCESS = 1
  };

  enum UseImage
  {
    WITHOUT_IMAGE = 0,
    WITH_IMAGE = 1
  };

  TestParam(
      const Option<set<Capability>>& _framework_effective,
      const Option<set<Capability>>& _framework_bounding,
      const Option<set<Capability>>& _operator_effective,
      const Option<set<Capability>>& _operator_bounding,
      UseImage _useImage,
      Result _result)
    : framework_effective(convert(_framework_effective)),
      framework_bounding(convert(_framework_bounding)),
      operator_effective(convert(_operator_effective)),
      operator_bounding(convert(_operator_bounding)),
      useImage(_useImage),
      result(_result) {}

  static const Option<CapabilityInfo> convert(
      const Option<set<Capability>>& caps)
  {
    return caps.isSome()
      ? capabilities::convert(caps.get())
      : Option<CapabilityInfo>::none();
  }

  const Option<CapabilityInfo> framework_effective;
  const Option<CapabilityInfo> framework_bounding;
  const Option<CapabilityInfo> operator_effective;
  const Option<CapabilityInfo> operator_bounding;

  const UseImage useImage;
  const Result result;
};


ostream& operator<<(ostream& stream, const TestParam& param)
{
  if (param.framework_effective.isSome()) {
    stream << "framework_effective='"
           << JSON::protobuf(param.framework_effective.get()) << "', ";
  } else {
    stream << "framework_effective='none', ";
  }

  if (param.framework_bounding.isSome()) {
    stream << "framework_bounding='"
           << JSON::protobuf(param.framework_bounding.get()) << "', ";
  } else {
    stream << "framework_bounding='none', ";
  }

  if (param.operator_effective.isSome()) {
    stream << "operator_effective='"
           << JSON::protobuf(param.operator_effective.get()) << "', ";
  } else {
    stream << "operator_effective='none', ";
  }

  if (param.operator_bounding.isSome()) {
    stream << "operator_bounding='"
           << JSON::protobuf(param.operator_bounding.get()) << "', ";
  } else {
    stream << "operator_bounding='none', ";
  }

  switch (param.useImage) {
    case TestParam::WITHOUT_IMAGE:
      stream << "use_image=false, ";
      break;
    case TestParam::WITH_IMAGE:
      stream << "use_image=true, ";
      break;
  }

  switch (param.result) {
    case TestParam::FAILURE:
      stream << "result=failure'";
      break;
    case TestParam::SUCCESS:
      stream << "result=success'";
  }

  return stream;
}


// ContainerizerTest creates the cgroups v2 root that the cgroups isolator,
// which the linux launcher always loads, requires at prepare().
class LinuxCapabilitiesIsolatorTest
  : public ContainerizerTest<node::MediosContainerizer>,
    public ::testing::WithParamInterface<TestParam>
{
public:
  LinuxCapabilitiesIsolatorTest()
    : param(GetParam()) {}

protected:
  TestParam param;
};


ACTION_TEMPLATE(PushTaskStatus,
                HAS_1_TEMPLATE_PARAMS(int, k),
                AND_1_VALUE_PARAMS(statuses))
{
  statuses->put(std::get<k>(args));
}


// Parameterized test confirming the behavior of the capabilities
// isolator. We here use the fact has `ping` has `NET_RAW` and
// `NET_ADMIN` in its file capabilities. This test should be
// instantiated with above `TestParam` struct.
TEST_P(LinuxCapabilitiesIsolatorTest, ROOT_Ping)
{
  Try<Owned<cluster::Controller>> controller = StartController();
  ASSERT_SOME(controller);

  node::Flags flags = CreateNodeFlags();
  flags.isolation = "linux/capabilities";
  flags.effective_capabilities = param.operator_effective;
  flags.bounding_capabilities = param.operator_bounding;

  if (param.useImage == TestParam::WITH_IMAGE) {
    const string registry = path::join(sandbox.get(), "registry");
    AWAIT_READY(DockerArchive::create(registry, "test_image"));

    flags.docker_registry = registry;
    flags.docker_store_dir = path::join(os::getcwd(), "store");
    flags.image_providers = "docker";
    flags.isolation += ",docker/runtime,filesystem/linux";
  }

  Owned<ControllerDetector> detector = controller.get()->createDetector();

  Try<Owned<cluster::Node>> node = StartNode(detector.get(), flags);
  ASSERT_SOME(node);

  MockScheduler sched;

  MediosSchedulerDriver driver(
      &sched,
      DEFAULT_FRAMEWORK_INFO,
      controller.get()->pid,
      DEFAULT_CREDENTIAL);

  EXPECT_CALL(sched, registered(_, _, _));

  Future<vector<Offer>> offers;
  EXPECT_CALL(sched, resourceOffers(_, _))
    .WillOnce(FutureArg<1>(&offers))
    .WillRepeatedly(Return()); // Ignore subsequent offers.

  driver.start();

  AWAIT_READY(offers);
  ASSERT_FALSE(offers->empty());

  // We use 'ping' as the command since it has file capabilities
  // (`NET_RAW` and `NET_ADMIN` in permitted set). This allows us to
  // test if capabilities are properly set.
  CommandInfo command;
  command.set_shell(false);
  command.set_value("/bin/ping");
  command.add_arguments("ping");
  command.add_arguments("-c");
  command.add_arguments("1");
  command.add_arguments("127.0.0.1");

  TaskInfo task = createTask(
      offers.get()[0].node_id(),
      offers.get()[0].resources(),
      command);

  if (param.framework_effective.isSome() ||
      param.framework_bounding.isSome()) {
    ContainerInfo* container = task.mutable_container();
    container->set_type(ContainerInfo::MEDIOS);

    LinuxInfo* linux = container->mutable_linux_info();

    if (param.framework_effective.isSome()) {
      CapabilityInfo* capabilities = linux->mutable_effective_capabilities();
      capabilities->CopyFrom(param.framework_effective.get());
    }

    if (param.framework_bounding.isSome()) {
      CapabilityInfo* capabilities = linux->mutable_bounding_capabilities();
      capabilities->CopyFrom(param.framework_bounding.get());
    }
  }

  if (param.useImage == TestParam::WITH_IMAGE) {
    ContainerInfo* container = task.mutable_container();
    container->set_type(ContainerInfo::MEDIOS);

    Image* image = container->mutable_medios()->mutable_image();
    image->set_type(Image::DOCKER);
    image->mutable_docker()->set_name("test_image");
  }

  Queue<TaskStatus> statuses;
  EXPECT_CALL(sched, statusUpdate(_, _))
    .WillRepeatedly(PushTaskStatus<1>(&statuses));

  driver.launchTasks(offers.get()[0].id(), {task});

  // Wait for the terminal status update.
  for (;;) {
    Future<TaskStatus> status = statuses.get();
    AWAIT_READY(status);

    TaskState state = status->state();
    if (protobuf::isTerminalState(state)) {
      switch (param.result) {
        case TestParam::SUCCESS:
          EXPECT_EQ(TASK_FINISHED, state);
          break;
        case TestParam::FAILURE:
          EXPECT_EQ(TASK_FAILED, state);
          break;
      }
      break;
    }
  }

  driver.stop();
  driver.join();
}


// Parameterized test confirming the behavior of the capabilities
// isolator with nested containers. We here use the fact has `ping`
// has `NET_RAW` and `NET_ADMIN` in its file capabilities. This test
// should be instantiated with above `TestParam` struct.
TEST_P(LinuxCapabilitiesIsolatorTest, ROOT_NestedPing)
{
  Try<Owned<cluster::Controller>> controller = StartController();
  ASSERT_SOME(controller);

  node::Flags flags = CreateNodeFlags();
  flags.isolation = "linux/capabilities";
  flags.effective_capabilities = param.operator_effective;
  flags.bounding_capabilities = param.operator_bounding;

  if (param.useImage == TestParam::WITH_IMAGE) {
    const string registry = path::join(sandbox.get(), "registry");
    AWAIT_READY(DockerArchive::create(registry, "test_image"));

    flags.docker_registry = registry;
    flags.docker_store_dir = path::join(os::getcwd(), "store");
    flags.image_providers = "docker";
    flags.isolation += ",docker/runtime,filesystem/linux";
  }

  Owned<ControllerDetector> detector = controller.get()->createDetector();

  Try<Owned<cluster::Node>> node = StartNode(detector.get(), flags);
  ASSERT_SOME(node);

  MockScheduler sched;

  MediosSchedulerDriver driver(
      &sched,
      DEFAULT_FRAMEWORK_INFO,
      controller.get()->pid,
      DEFAULT_CREDENTIAL);

  Future<FrameworkID> frameworkId;
  EXPECT_CALL(sched, registered(&driver, _, _))
    .WillOnce(FutureArg<1>(&frameworkId));

  Future<vector<Offer>> offers;

  EXPECT_CALL(sched, resourceOffers(_, _))
    .WillOnce(FutureArg<1>(&offers))
    .WillRepeatedly(Return()); // Ignore subsequent offers.

  driver.start();

  AWAIT_READY(frameworkId);

  AWAIT_READY(offers);
  ASSERT_FALSE(offers->empty());

  Resources resources = Resources::parse("cpus:0.1;mem:32;disk:32").get();

  ExecutorInfo executorInfo;
  executorInfo.set_type(ExecutorInfo::DEFAULT);
  executorInfo.mutable_executor_id()->CopyFrom(DEFAULT_EXECUTOR_ID);
  executorInfo.mutable_framework_id()->CopyFrom(frameworkId.get());
  executorInfo.mutable_resources()->CopyFrom(resources);

  const Offer& offer = offers->front();
  const NodeID& nodeId = offer.node_id();

  // We use 'ping' as the command since it has file capabilities
  // (`NET_RAW` and `NET_ADMIN` in permitted set). This allows us to
  // test if capabilities are properly set.
  CommandInfo command;
  command.set_shell(false);
  command.set_value("/bin/ping");
  command.add_arguments("ping");
  command.add_arguments("-c");
  command.add_arguments("1");
  command.add_arguments("127.0.0.1");

  TaskInfo task = createTask(nodeId, resources, command);

  if (param.framework_effective.isSome() ||
      param.framework_bounding.isSome()) {
    ContainerInfo* container = task.mutable_container();
    container->set_type(ContainerInfo::MEDIOS);

    LinuxInfo* linux = container->mutable_linux_info();

    if (param.framework_effective.isSome()) {
      CapabilityInfo* capabilities = linux->mutable_effective_capabilities();
      capabilities->CopyFrom(param.framework_effective.get());
    }

    if (param.framework_bounding.isSome()) {
      CapabilityInfo* capabilities = linux->mutable_bounding_capabilities();
      capabilities->CopyFrom(param.framework_bounding.get());
    }
  }

  if (param.useImage == TestParam::WITH_IMAGE) {
    ContainerInfo* container = task.mutable_container();
    container->set_type(ContainerInfo::MEDIOS);

    Image* image = container->mutable_medios()->mutable_image();
    image->set_type(Image::DOCKER);
    image->mutable_docker()->set_name("test_image");
  }

  Queue<TaskStatus> statuses;
  EXPECT_CALL(sched, statusUpdate(_, _))
    .WillRepeatedly(PushTaskStatus<1>(&statuses));

  TaskGroupInfo taskGroup = createTaskGroupInfo({task});

  driver.acceptOffers({offer.id()}, {LAUNCH_GROUP(executorInfo, taskGroup)});

  // Wait for the terminal status update.
  for (;;) {
    Future<TaskStatus> status = statuses.get();
    AWAIT_READY(status);

    TaskState state = status->state();
    if (protobuf::isTerminalState(state)) {
      switch (param.result) {
        case TestParam::SUCCESS:
          EXPECT_EQ(TASK_FINISHED, state) << status->DebugString();
          break;
        case TestParam::FAILURE:
          EXPECT_EQ(TASK_FAILED, state) << status->DebugString();
          break;
      }
      break;
    }
  }

  driver.stop();
  driver.join();
}


// TODO(jieyu): We used DAC_READ_SEARCH capability below so that test
// results won't be affected even if the executable (e.g., command
// executor) to launch is not accessible (e.g., under someone's home
// directory). Without that, even ROOT user will receive EACCESS if
// DAC_READ_SEARCH is dropped.
INSTANTIATE_TEST_CASE_P(
    TestParam,
    LinuxCapabilitiesIsolatorTest,
    ::testing::Values(
        // Dropped all relevant capabilities, thus ping will fail.
        TestParam(
            set<Capability>(),
            None(),
            None(),
            None(),
            TestParam::WITHOUT_IMAGE,
            TestParam::FAILURE),
        TestParam(
            set<Capability>(),
            None(),
            None(),
            None(),
            TestParam::WITH_IMAGE,
            TestParam::FAILURE),
        TestParam(
            set<Capability>({DAC_READ_SEARCH}),
            None(),
            set<Capability>({CHOWN, DAC_READ_SEARCH}),
            set<Capability>({CHOWN, DAC_READ_SEARCH}),
            TestParam::WITHOUT_IMAGE,
            TestParam::FAILURE),
        TestParam(
            set<Capability>({DAC_READ_SEARCH}),
            None(),
            set<Capability>({CHOWN, DAC_READ_SEARCH}),
            set<Capability>({CHOWN, DAC_READ_SEARCH}),
            TestParam::WITH_IMAGE,
            TestParam::FAILURE),

        // The framework effective set is outside the bounding set
        // so the task will be failed by the isolator.
        TestParam(
            set<Capability>({NET_RAW, NET_ADMIN, DAC_READ_SEARCH}),
            set<Capability>(),
            None(),
            None(),
            TestParam::WITH_IMAGE,
            TestParam::FAILURE),
        TestParam(
            set<Capability>({NET_RAW, NET_ADMIN, DAC_READ_SEARCH}),
            set<Capability>(),
            None(),
            None(),
            TestParam::WITHOUT_IMAGE,
            TestParam::FAILURE),
        TestParam(
            set<Capability>({NET_RAW, NET_ADMIN, DAC_READ_SEARCH}),
            set<Capability>({CHOWN}),
            None(),
            None(),
            TestParam::WITH_IMAGE,
            TestParam::FAILURE),
        TestParam(
            set<Capability>({NET_RAW, NET_ADMIN, DAC_READ_SEARCH}),
            set<Capability>({CHOWN}),
            None(),
            None(),
            TestParam::WITHOUT_IMAGE,
            TestParam::FAILURE),

        // Effective capabilities do not contain that ping needs, thus
        // ping will fail.
        TestParam(
            None(),
            None(),
            set<Capability>({CHOWN, DAC_READ_SEARCH}),
            set<Capability>({CHOWN, DAC_READ_SEARCH}),
            TestParam::WITHOUT_IMAGE,
            TestParam::FAILURE),
        TestParam(
            None(),
            None(),
            set<Capability>({CHOWN, DAC_READ_SEARCH}),
            set<Capability>({CHOWN, DAC_READ_SEARCH}),
            TestParam::WITH_IMAGE,
            TestParam::FAILURE),
        TestParam(
            None(),
            None(),
            None(),
            set<Capability>({CHOWN, DAC_READ_SEARCH}),
            TestParam::WITH_IMAGE,
            TestParam::FAILURE),
        TestParam(
            None(),
            None(),
            None(),
            set<Capability>({CHOWN, DAC_READ_SEARCH}),
            TestParam::WITHOUT_IMAGE,
            TestParam::FAILURE),
        TestParam(
            set<Capability>({CHOWN, DAC_READ_SEARCH}),
            None(),
            None(),
            None(),
            TestParam::WITHOUT_IMAGE,
            TestParam::FAILURE),
        TestParam(
            set<Capability>({CHOWN, DAC_READ_SEARCH}),
            None(),
            None(),
            None(),
            TestParam::WITH_IMAGE,
            TestParam::FAILURE),
        TestParam(
            set<Capability>({CHOWN, DAC_READ_SEARCH}),
            set<Capability>({CHOWN, DAC_READ_SEARCH}),
            None(),
            None(),
            TestParam::WITH_IMAGE,
            TestParam::FAILURE),
        TestParam(
            set<Capability>({CHOWN, DAC_READ_SEARCH}),
            set<Capability>({CHOWN, DAC_READ_SEARCH}),
            None(),
            None(),
            TestParam::WITHOUT_IMAGE,
            TestParam::FAILURE),

        // Framework effective capabilities are not allowed, task will fail.
        TestParam(
            set<Capability>({NET_RAW, NET_ADMIN}),
            None(),
            set<Capability>({CHOWN}),
            set<Capability>({CHOWN}),
            TestParam::WITHOUT_IMAGE,
            TestParam::FAILURE),
        TestParam(
            set<Capability>({NET_RAW, NET_ADMIN}),
            None(),
            set<Capability>({CHOWN}),
            set<Capability>({CHOWN}),
            TestParam::WITH_IMAGE,
            TestParam::FAILURE),

        // Dropped all capabilities but those that ping needs, thus
        // ping will finish normally.
        TestParam(
            set<Capability>({DAC_READ_SEARCH}),
            set<Capability>({NET_RAW, NET_ADMIN, DAC_READ_SEARCH}),
            None(),
            None(),
            TestParam::WITHOUT_IMAGE,
            TestParam::SUCCESS),
        TestParam(
            set<Capability>(),
            set<Capability>({NET_RAW, NET_ADMIN, DAC_READ_SEARCH}),
            None(),
            None(),
            TestParam::WITH_IMAGE,
            TestParam::SUCCESS),
        TestParam(
            set<Capability>({NET_RAW, NET_ADMIN, DAC_READ_SEARCH}),
            None(),
            None(),
            None(),
            TestParam::WITHOUT_IMAGE,
            TestParam::SUCCESS),
        TestParam(
            set<Capability>({NET_RAW, NET_ADMIN, DAC_READ_SEARCH}),
            None(),
            None(),
            None(),
            TestParam::WITH_IMAGE,
            TestParam::SUCCESS),
        TestParam(
            set<Capability>({NET_RAW, NET_ADMIN, DAC_READ_SEARCH}),
            set<Capability>({NET_RAW, NET_ADMIN, DAC_READ_SEARCH}),
            None(),
            None(),
            TestParam::WITHOUT_IMAGE,
            TestParam::SUCCESS),
        TestParam(
            set<Capability>({NET_RAW, NET_ADMIN, DAC_READ_SEARCH}),
            set<Capability>({NET_RAW, NET_ADMIN, DAC_READ_SEARCH}),
            None(),
            None(),
            TestParam::WITH_IMAGE,
            TestParam::SUCCESS),
        TestParam(
            None(),
            None(),
            set<Capability>({NET_RAW, NET_ADMIN, DAC_READ_SEARCH}),
            set<Capability>({NET_RAW, NET_ADMIN, DAC_READ_SEARCH}),
            TestParam::WITHOUT_IMAGE,
            TestParam::SUCCESS),
        TestParam(
            None(),
            None(),
            set<Capability>({NET_RAW, NET_ADMIN, DAC_READ_SEARCH}),
            set<Capability>({NET_RAW, NET_ADMIN, DAC_READ_SEARCH}),
            TestParam::WITH_IMAGE,
            TestParam::SUCCESS),
        TestParam(
            None(),
            None(),
            None(),
            set<Capability>({NET_RAW, NET_ADMIN, DAC_READ_SEARCH}),
            TestParam::WITHOUT_IMAGE,
            TestParam::SUCCESS),
        TestParam(
            None(),
            None(),
            None(),
            set<Capability>({NET_RAW, NET_ADMIN, DAC_READ_SEARCH}),
            TestParam::WITH_IMAGE,
            TestParam::SUCCESS),
        TestParam(
            None(),
            set<Capability>({NET_RAW, NET_ADMIN, DAC_READ_SEARCH}),
            None(),
            None(),
            TestParam::WITHOUT_IMAGE,
            TestParam::SUCCESS),
        TestParam(
            None(),
            set<Capability>({NET_RAW, NET_ADMIN, DAC_READ_SEARCH}),
            None(),
            None(),
            TestParam::WITH_IMAGE,
            TestParam::SUCCESS),
        TestParam(
            set<Capability>({NET_RAW, NET_ADMIN, DAC_READ_SEARCH}),
            None(),
            set<Capability>({NET_RAW, NET_ADMIN, DAC_READ_SEARCH}),
            set<Capability>({NET_RAW, NET_ADMIN, DAC_READ_SEARCH}),
            TestParam::WITHOUT_IMAGE,
            TestParam::SUCCESS),
        TestParam(
            set<Capability>({NET_RAW, NET_ADMIN, DAC_READ_SEARCH}),
            None(),
            set<Capability>({NET_RAW, NET_ADMIN, DAC_READ_SEARCH}),
            set<Capability>({NET_RAW, NET_ADMIN, DAC_READ_SEARCH}),
            TestParam::WITH_IMAGE,
            TestParam::SUCCESS)));


// TODO(bbannier): Add test cases for running the container as non-root.


// TODO(bbannier): Reject these tasks that specify capabilities if
// capabilities isolator is not enabled.

class LinuxCapabilitiesIsolatorFlagsTest : public MediosTest {};


TEST_F(LinuxCapabilitiesIsolatorFlagsTest, ROOT_IsolatorFlags)
{
  StandaloneControllerDetector detector;

  node::Flags flags = CreateNodeFlags();
  flags.isolation = "linux/capabilities";

  Try<Owned<cluster::Node>> node = Owned<cluster::Node>();

  // Allowed is not a subset of bounding, so this should fail.
  flags.effective_capabilities = convert(set<Capability>({NET_RAW, NET_ADMIN}));
  flags.bounding_capabilities = convert(set<Capability>({NET_RAW}));
  node = StartNode(&detector, flags);
  ASSERT_ERROR(node);

  // Allowed is the same as bounding, which is OK.
  flags.effective_capabilities = convert(set<Capability>({NET_RAW, NET_ADMIN}));
  flags.bounding_capabilities = convert(set<Capability>({NET_RAW, NET_ADMIN}));
  node = StartNode(&detector, flags);
  ASSERT_SOME(node);

  node->reset();

  // Allowed is a subset of bounding, which is OK.
  flags.effective_capabilities = convert(set<Capability>({NET_RAW}));
  flags.bounding_capabilities = convert(set<Capability>({NET_RAW, NET_ADMIN}));
  node = StartNode(&detector, flags);
  ASSERT_SOME(node);

  node->reset();

  // Both sets are allowed to be missing.
  flags.effective_capabilities = None();
  flags.bounding_capabilities = None();
  node = StartNode(&detector, flags);
  ASSERT_SOME(node);

  node->reset();

  // Bounding capabilities are allowed to be missing.
  flags.effective_capabilities = convert(set<Capability>({NET_RAW}));
  flags.bounding_capabilities = None();
  node = StartNode(&detector, flags);
  ASSERT_SOME(node);

  node->reset();

  // Effective capabilities are allowed to be missing.
  flags.effective_capabilities = None();
  flags.bounding_capabilities = convert(set<Capability>({NET_RAW}));
  node = StartNode(&detector, flags);
  ASSERT_SOME(node);
}

} // namespace tests {
} // namespace internal {
} // namespace medios {
