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

#include <errno.h>
#include <sched.h>
#include <unistd.h>

#include <string>
#include <vector>

#include <glog/logging.h>

#include <stout/foreach.hpp>
#include <stout/os.hpp>
#include <stout/path.hpp>

#include "linux/cgroups2.hpp"

#include "node/containerizer/process_limits.hpp"

using std::string;
using std::vector;

namespace medios {
namespace internal {
namespace node {

namespace {

// The cgroup followed by each ancestor up to the root of this process's view
// of the hierarchy. A limit on an ancestor binds every descendant.
vector<string> lineage(const string& cgroup)
{
  vector<string> result;
  string current = cgroup;
  while (true) {
    result.push_back(current);
    if (current.empty()) {
      break;
    }
    size_t slash = current.rfind('/');
    current = slash == string::npos ? "" : current.substr(0, slash);
  }
  return result;
}


// A control file is absent on the true root cgroup and on cgroups whose
// parent has not enabled the controller; absence means no limit there.
bool hasControl(const string& cgroup, const string& control)
{
  return os::exists(path::join(cgroups2::path(cgroup), control));
}

} // namespace {


ProcessLimits processLimits()
{
  ProcessLimits limits;

  cpu_set_t set;
  CPU_ZERO(&set);
  if (::sched_getaffinity(0, sizeof(set), &set) == 0) {
    limits.affinityCpus = CPU_COUNT(&set);
  } else {
    LOG(WARNING) << "Failed to read the CPU affinity mask: "
                 << os::strerror(errno);
  }

  if (!cgroups2::enabled() || !cgroups2::exists(cgroups2::ROOT_CGROUP)) {
    LOG(INFO) << "cgroup v2 is not available; node resources ignore"
              << " cgroup limits";
    return limits;
  }

  // Inside a cgroup namespace this is "" (the container's own cgroup is the
  // root of the view). Without one it is the host path, which may not exist
  // under this mount; such entries are skipped.
  Try<string> self = cgroups2::cgroup(::getpid());
  if (self.isError()) {
    LOG(INFO) << "Not reading cgroup limits: " << self.error();
    return limits;
  }

  foreach (const string& cgroup, lineage(self.get())) {
    if (!cgroups2::exists(cgroup)) {
      continue;
    }

    if (hasControl(cgroup, "cpu.max")) {
      Try<cgroups2::cpu::BandwidthLimit> bandwidth =
        cgroups2::cpu::max(cgroup);
      if (bandwidth.isError()) {
        LOG(WARNING) << bandwidth.error();
      } else if (bandwidth->limit.isSome() && bandwidth->period.isSome()) {
        double quota =
          bandwidth->limit->ns() / static_cast<double>(bandwidth->period->ns());
        if (limits.cpuQuota.isNone() || quota < limits.cpuQuota.get()) {
          limits.cpuQuota = quota;
        }
      }
    }

    if (hasControl(cgroup, "memory.max")) {
      Result<Bytes> memory = cgroups2::memory::max(cgroup);
      if (memory.isError()) {
        LOG(WARNING) << memory.error();
      } else if (memory.isSome()) {
        if (limits.memoryMax.isNone() || memory.get() < limits.memoryMax.get()) {
          limits.memoryMax = memory.get();
        }
      }
    }
  }

  return limits;
}

} // namespace node {
} // namespace internal {
} // namespace medios {
