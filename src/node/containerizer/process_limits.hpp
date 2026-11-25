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

#ifndef __NODE_CONTAINERIZER_PROCESS_LIMITS_HPP__
#define __NODE_CONTAINERIZER_PROCESS_LIMITS_HPP__

#include <algorithm>

#include <stout/bytes.hpp>
#include <stout/option.hpp>

namespace medios {
namespace internal {
namespace node {

// The CPU and memory this process may use. Inside a container the kernel is
// the host's, so sysconf(_SC_NPROCESSORS_ONLN) and sysinfo() report the
// host's totals; the CPU affinity mask and the cgroup limits (v2, or v1 on
// older hosts) are what the container is actually allowed. None means no limit was found.
struct ProcessLimits
{
  // CPUs in this process's affinity mask (sched_getaffinity).
  Option<long> affinityCpus;

  // Tightest CPU quota divided by its period, in CPUs, over this process's
  // cgroup and its ancestors: `cpu.max` on v2, `cpu.cfs_quota_us` over
  // `cpu.cfs_period_us` on v1. May be fractional.
  Option<double> cpuQuota;

  // Tightest memory limit over this process's cgroup and its ancestors:
  // `memory.max` on v2, `memory.limit_in_bytes` on v1.
  Option<Bytes> memoryMax;
};


inline double boundCpus(double host, const ProcessLimits& limits)
{
  double cpus = host;
  if (limits.affinityCpus.isSome()) {
    cpus = std::min(cpus, static_cast<double>(limits.affinityCpus.get()));
  }
  if (limits.cpuQuota.isSome()) {
    cpus = std::min(cpus, limits.cpuQuota.get());
  }
  return cpus;
}


inline Bytes boundMemory(const Bytes& host, const ProcessLimits& limits)
{
  return limits.memoryMax.isSome()
    ? std::min(host, limits.memoryMax.get())
    : host;
}


#ifdef __linux__
// Reads the limits for the calling process. A limit that cannot be read is
// logged and left as None, so the caller falls back to the host totals.
ProcessLimits processLimits();
#endif // __linux__

} // namespace node {
} // namespace internal {
} // namespace medios {

#endif // __NODE_CONTAINERIZER_PROCESS_LIMITS_HPP__
