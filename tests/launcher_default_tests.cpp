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

#include <gtest/gtest.h>

#include "node/flags.hpp"

using medios::internal::node::Flags;


// medios#35: on a cgroup v2 host LinuxLauncher::available() is true for
// any user, and a non-root node that took the Linux launcher by default
// failed its first task in the cgroups isolator. A non-root node now
// defaults to the posix launcher on every host.
TEST(LauncherDefaultTest, NonRootDefaultsToPosix)
{
  if (::geteuid() == 0) {
    GTEST_SKIP() << "runs as root; the non-root default is not observable";
  }

  Flags flags;
  EXPECT_EQ("posix", flags.launcher);
}
