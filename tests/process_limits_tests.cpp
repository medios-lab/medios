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

#include <gtest/gtest.h>

#include <stout/bytes.hpp>
#include <stout/gtest.hpp>

#include "node/containerizer/process_limits.hpp"

using medios::internal::node::ProcessLimits;
using medios::internal::node::boundCpus;
using medios::internal::node::boundMemory;
using medios::internal::node::unifiedCgroup;


TEST(ProcessLimitsTest, NoLimitsKeepsHostFigures)
{
  ProcessLimits limits;
  EXPECT_EQ(192.0, boundCpus(192, limits));
  EXPECT_EQ(Gigabytes(1132), boundMemory(Gigabytes(1132), limits));
}


// The RunPod CPU pod: a 2-CPU affinity mask, no CPU quota and a memory
// limit of 3999997952 bytes (cgroup v1 memory.limit_in_bytes) on a
// 192-CPU host.
TEST(ProcessLimitsTest, AffinityMaskAndMemoryMaxBound)
{
  ProcessLimits limits;
  limits.affinityCpus = 2;
  limits.memoryMax = Bytes(3999997952);

  EXPECT_EQ(2.0, boundCpus(192, limits));
  EXPECT_EQ(Bytes(3999997952), boundMemory(Gigabytes(1132), limits));
}


TEST(ProcessLimitsTest, FractionalQuotaBelowAffinity)
{
  ProcessLimits limits;
  limits.affinityCpus = 4;
  limits.cpuQuota = 1.5;

  EXPECT_EQ(1.5, boundCpus(16, limits));
}


TEST(ProcessLimitsTest, LimitsAboveHostDoNotRaiseIt)
{
  ProcessLimits limits;
  limits.affinityCpus = 64;
  limits.cpuQuota = 32.0;
  limits.memoryMax = Gigabytes(512);

  EXPECT_EQ(8.0, boundCpus(8, limits));
  EXPECT_EQ(Gigabytes(16), boundMemory(Gigabytes(16), limits));
}


TEST(ProcessLimitsTest, UnifiedCgroupOnV2Host)
{
  EXPECT_SOME_EQ("user.slice/session-3.scope",
                 unifiedCgroup("0::/user.slice/session-3.scope\n"));
  EXPECT_SOME_EQ("", unifiedCgroup("0::/\n"));
  EXPECT_SOME_EQ("medios/abc", unifiedCgroup("0::/medios/abc (deleted)\n"));
}


// A host that mounts v1 hierarchies beside cgroup v2 lists one line per
// hierarchy, with the v2 line first or last.
TEST(ProcessLimitsTest, UnifiedCgroupBesideV1Hierarchies)
{
  EXPECT_SOME_EQ(
      "system.slice/medios-node.service",
      unifiedCgroup(
          "2:net_cls,net_prio:/\n"
          "1:name=systemd:/system.slice/medios-node.service\n"
          "0::/system.slice/medios-node.service\n"));

  EXPECT_SOME_EQ(
      "kubepods/pod1",
      unifiedCgroup(
          "0::/kubepods/pod1\n"
          "1:net_cls:/kubepods/pod1\n"));
}


TEST(ProcessLimitsTest, UnifiedCgroupAbsentOnV1Host)
{
  EXPECT_ERROR(unifiedCgroup(
      "5:memory:/docker/abc\n"
      "4:cpu,cpuacct:/docker/abc\n"));
  EXPECT_ERROR(unifiedCgroup(""));
}
