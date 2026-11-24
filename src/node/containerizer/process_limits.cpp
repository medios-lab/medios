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
#include <stout/numify.hpp>
#include <stout/os.hpp>
#include <stout/path.hpp>
#include <stout/strings.hpp>

#include "linux/cgroups.hpp"
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


void lowerQuota(ProcessLimits* limits, double quota)
{
  if (limits->cpuQuota.isNone() || quota < limits->cpuQuota.get()) {
    limits->cpuQuota = quota;
  }
}


void lowerMemory(ProcessLimits* limits, const Bytes& memory)
{
  if (limits->memoryMax.isNone() || memory < limits->memoryMax.get()) {
    limits->memoryMax = memory;
  }
}


void readCgroups2(ProcessLimits* limits)
{
  if (!cgroups2::enabled() || !cgroups2::exists(cgroups2::ROOT_CGROUP)) {
    return;
  }

  // Inside a cgroup namespace this is "" (the container's own cgroup is the
  // root of the view). Without one it is the host path, which may not exist
  // under this mount; such entries are skipped.
  Try<string> self = cgroups2::cgroup(::getpid());
  if (self.isError()) {
    LOG(INFO) << "Not reading cgroup v2 limits: " << self.error();
    return;
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
        lowerQuota(
            limits,
            bandwidth->limit->ns() /
              static_cast<double>(bandwidth->period->ns()));
      }
    }

    if (hasControl(cgroup, "memory.max")) {
      Result<Bytes> memory = cgroups2::memory::max(cgroup);
      if (memory.isError()) {
        LOG(WARNING) << memory.error();
      } else if (memory.isSome()) {
        lowerMemory(limits, memory.get());
      }
    }
  }
}


// cgroup v1 keeps each controller in its own hierarchy. A container often
// sees its own cgroup mounted as the hierarchy root while /proc/self/cgroup
// still names the host path (e.g. "/docker/<id>"); the walk below then finds
// the limit on the root entry "".
void readCgroups1(ProcessLimits* limits)
{
  if (!cgroups::enabled()) {
    return;
  }

  // An unlimited v1 cgroup reports a quota of -1 and a memory limit near
  // 2^63; both fall out of the minimum against the host figures.
  Result<string> cpuHierarchy = cgroups::hierarchy("cpu");
  Result<string> cpuCgroup = cgroups::cpu::cgroup(::getpid());
  if (cpuHierarchy.isSome() && cpuCgroup.isSome()) {
    foreach (const string& cgroup, lineage(cpuCgroup.get())) {
      const string dir = path::join(cpuHierarchy.get(), cgroup);
      if (!os::exists(path::join(dir, "cpu.cfs_quota_us")) ||
          !os::exists(path::join(dir, "cpu.cfs_period_us"))) {
        continue;
      }
      Try<string> quota =
        cgroups::read(cpuHierarchy.get(), cgroup, "cpu.cfs_quota_us");
      Try<string> period =
        cgroups::read(cpuHierarchy.get(), cgroup, "cpu.cfs_period_us");
      if (quota.isError() || period.isError()) {
        LOG(WARNING) << "Failed to read the cfs quota in " << dir << ": "
                     << (quota.isError() ? quota.error() : period.error());
        continue;
      }
      Try<long long> quotaUs = numify<long long>(strings::trim(quota.get()));
      Try<long long> periodUs = numify<long long>(strings::trim(period.get()));
      if (quotaUs.isSome() && periodUs.isSome() &&
          quotaUs.get() > 0 && periodUs.get() > 0) {
        lowerQuota(
            limits, quotaUs.get() / static_cast<double>(periodUs.get()));
      }
    }
  }

  Result<string> memoryHierarchy = cgroups::hierarchy("memory");
  Result<string> memoryCgroup = cgroups::memory::cgroup(::getpid());
  if (memoryHierarchy.isSome() && memoryCgroup.isSome()) {
    foreach (const string& cgroup, lineage(memoryCgroup.get())) {
      const string dir = path::join(memoryHierarchy.get(), cgroup);
      if (!os::exists(path::join(dir, "memory.limit_in_bytes"))) {
        continue;
      }
      Try<Bytes> memory =
        cgroups::memory::limit_in_bytes(memoryHierarchy.get(), cgroup);
      if (memory.isError()) {
        LOG(WARNING) << memory.error();
      } else {
        lowerMemory(limits, memory.get());
      }
    }
  }
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

  // A hybrid host mounts both; a limit set through either binds the process.
  readCgroups2(&limits);
  readCgroups1(&limits);

  return limits;
}

} // namespace node {
} // namespace internal {
} // namespace medios {
