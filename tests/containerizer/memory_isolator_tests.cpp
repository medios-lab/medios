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
#include <vector>

#include <process/gtest.hpp>

#include <stout/gtest.hpp>

#include "node/containerizer/medios/containerizer.hpp"

#include "tests/medios.hpp"

#ifdef __linux__
#include "linux/cgroups.hpp"
#include "linux/cgroups2.hpp"

#include "node/containerizer/medios/paths.hpp"
#endif // __linux__

using medios::internal::controller::Controller;

using medios::internal::node::MediosContainerizer;

using medios::controller::detector::ControllerDetector;

using process::Future;
using process::Owned;

using std::string;
using std::vector;

using testing::WithParamInterface;

namespace medios {
namespace internal {
namespace tests {

class MemoryIsolatorTest
  : public ContainerizerTest<MediosContainerizer>,
    public WithParamInterface<string> {};


// These tests are parameterized by the isolation flag.
static vector<string>* isolators = new vector<string>({
  "posix/mem",
#ifdef __linux__
  "cgroups/mem",
#endif // __linux__
});


INSTANTIATE_TEST_CASE_P(
    IsolationFlag,
    MemoryIsolatorTest,
    ::testing::ValuesIn(*isolators));


TEST_P(MemoryIsolatorTest, ROOT_MemUsage)
{
  Try<Owned<cluster::Controller>> controller = StartController();
  ASSERT_SOME(controller);

  node::Flags flags = CreateNodeFlags();
  flags.isolation = GetParam();

  Try<MediosContainerizer*> _containerizer =
    MediosContainerizer::create(flags, true);

  ASSERT_SOME(_containerizer);

  Owned<MediosContainerizer> containerizer(_containerizer.get());

  Owned<ControllerDetector> detector = controller.get()->createDetector();

  Try<Owned<cluster::Node>> node = StartNode(
      detector.get(),
      containerizer.get());

  ASSERT_SOME(node);

  MockScheduler sched;
  MediosSchedulerDriver driver(
      &sched,
      DEFAULT_FRAMEWORK_INFO,
      controller.get()->pid,
      DEFAULT_CREDENTIAL);

  EXPECT_CALL(sched, registered(&driver, _, _));

  Future<vector<Offer>> offers;
  EXPECT_CALL(sched, resourceOffers(&driver, _))
    .WillOnce(FutureArg<1>(&offers))
    .WillRepeatedly(Return()); // Ignore subsequent offers.

  driver.start();

  AWAIT_READY(offers);
  ASSERT_FALSE(offers->empty());

  TaskInfo task = createTask(offers.get()[0], SLEEP_COMMAND(120));

  Future<TaskStatus> statusStarting;
  Future<TaskStatus> statusRunning;
  EXPECT_CALL(sched, statusUpdate(&driver, _))
    .WillOnce(FutureArg<1>(&statusStarting))
    .WillOnce(FutureArg<1>(&statusRunning));

  driver.launchTasks(offers.get()[0].id(), {task});

  AWAIT_READY(statusStarting);
  EXPECT_EQ(TASK_STARTING, statusStarting->state());

  AWAIT_READY(statusRunning);
  EXPECT_EQ(TASK_RUNNING, statusRunning->state());

  Future<hashset<ContainerID>> containers = containerizer->containers();
  AWAIT_READY(containers);
  ASSERT_EQ(1u, containers->size());

  ContainerID containerId = *(containers->begin());

  Future<ResourceStatistics> usage = containerizer->usage(containerId);
  AWAIT_READY(usage);

  // TODO(jieyu): Consider using a program that predictably increases
  // RSS so that we can set more meaningful expectation here.
#ifdef __WINDOWS__
  // NOTE: Windows reports `mem_total_bytes` instead of `mem_rss_bytes` because
  // of the change in meaning in 0.23.0. The `mem_rss_bytes` represents only
  // anonymous memory usage, but the "working set size" reported by Windows
  // corresponds to the total memory of a process in RAM.
  EXPECT_LT(0u, usage->mem_total_bytes());
#else
  // TODO(andschwa): Consider testing `mem_total_bytes` for other platforms.
  EXPECT_LT(0u, usage->mem_rss_bytes());
#endif // __WINDOWS__

#ifdef __linux__
  // Metrics for kmem are only enabled with memory isolation.
  if (GetParam() == "cgroups/mem") {
    // Check that at least one page of kernel memory is used. Each page is
    // 4096 bytes so we expect at least that much memory to be used.
    if (cgroupsV2()) {
      ASSERT_LT(4096u, usage->mem_kmem_usage_bytes());
    } else {
      Result<std::string> hierarchy = cgroups::hierarchy("memory");
      ASSERT_SOME(hierarchy);

      Try<bool> kmemExists =
        cgroups::exists(hierarchy.get(), "memory.kmem.usage_in_bytes");
      ASSERT_SOME(kmemExists);

      if (kmemExists.get()) {
        ASSERT_LT(4096u, usage->mem_kmem_usage_bytes());
      }
    }
 }
#endif // __linux__

  driver.stop();
  driver.join();
}

#ifdef __linux__
class MemorySwapLimitTest : public ContainerizerTest<MediosContainerizer> {};


// With `--cgroups_limit_swap` on a cgroups v2 host, a container's swap
// limit is zero, so its memory limit is the whole of what it can hold.
TEST_F(MemorySwapLimitTest, ROOT_CGROUPS_SwapLimitedToZero)
{
  if (!cgroupsV2()) {
    return;
  }

  Try<Owned<cluster::Controller>> controller = StartController();
  ASSERT_SOME(controller);

  node::Flags flags = CreateNodeFlags();
  flags.isolation = "cgroups/mem";
  flags.cgroups_limit_swap = true;

  Try<MediosContainerizer*> _containerizer =
    MediosContainerizer::create(flags, true);

  ASSERT_SOME(_containerizer);

  Owned<MediosContainerizer> containerizer(_containerizer.get());

  Owned<ControllerDetector> detector = controller.get()->createDetector();

  Try<Owned<cluster::Node>> node = StartNode(
      detector.get(),
      containerizer.get(),
      flags);

  ASSERT_SOME(node);

  MockScheduler sched;
  MediosSchedulerDriver driver(
      &sched,
      DEFAULT_FRAMEWORK_INFO,
      controller.get()->pid,
      DEFAULT_CREDENTIAL);

  EXPECT_CALL(sched, registered(&driver, _, _));

  Future<vector<Offer>> offers;
  EXPECT_CALL(sched, resourceOffers(&driver, _))
    .WillOnce(FutureArg<1>(&offers))
    .WillRepeatedly(Return()); // Ignore subsequent offers.

  driver.start();

  AWAIT_READY(offers);
  ASSERT_FALSE(offers->empty());

  TaskInfo task = createTask(offers.get()[0], SLEEP_COMMAND(120));

  Future<TaskStatus> statusStarting;
  Future<TaskStatus> statusRunning;
  EXPECT_CALL(sched, statusUpdate(&driver, _))
    .WillOnce(FutureArg<1>(&statusStarting))
    .WillOnce(FutureArg<1>(&statusRunning));

  driver.launchTasks(offers.get()[0].id(), {task});

  AWAIT_READY(statusStarting);
  EXPECT_EQ(TASK_STARTING, statusStarting->state());

  AWAIT_READY(statusRunning);
  EXPECT_EQ(TASK_RUNNING, statusRunning->state());

  Future<hashset<ContainerID>> containers = containerizer->containers();
  AWAIT_READY(containers);
  ASSERT_EQ(1u, containers->size());

  const string cgroup = node::containerizer::paths::cgroups2::container(
      flags.cgroups_root, *(containers->begin()));

  Result<Bytes> swapMax = cgroups2::memory::swap_max(cgroup);
  ASSERT_SOME(swapMax);
  EXPECT_EQ(Bytes(0), swapMax.get());

  driver.stop();
  driver.join();
}
#endif // __linux__

} // namespace tests {
} // namespace internal {
} // namespace medios {
