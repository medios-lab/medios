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

#ifndef __CONTROLLER_FLAGS_HPP__
#define __CONTROLLER_FLAGS_HPP__

#include <cstdint>

#include <string>

#include <stout/duration.hpp>
#include <stout/option.hpp>
#include <stout/path.hpp>

#include <medios/medios.hpp>

#include <medios/authorizer/acls.hpp>

#include <medios/module/module.hpp>

#include "logging/flags.hpp"

#include "messages/flags.hpp"

namespace medios {
namespace internal {
namespace controller {

class Flags : public virtual logging::Flags
{
public:
  Flags();
  bool version;
  Option<std::string> hostname;
  bool hostname_lookup;
  bool root_submissions;
  Option<std::string> work_dir;
  std::string registry;
  bool registry_strict;
  Duration registry_fetch_timeout;
  Duration registry_store_timeout;
  Duration node_reregister_timeout;
  std::string recovery_node_removal_limit;
  Option<std::string> node_removal_rate_limit;
  Option<Path> allowlist;
  std::string role_sorter;
  std::string framework_sorter;
  Duration allocation_interval;
  Option<std::string> cluster;
  Option<std::string> roles;
  Option<std::string> weights;
  bool authenticate_frameworks;
  bool authenticate_nodes;
  Duration authentication_v0_timeout;
  bool authenticate_http_readonly;
  bool authenticate_http_readwrite;
  bool authenticate_http_frameworks;
  Option<Path> credentials;
  Option<ACLs> acls;
  Option<Firewall> firewall_rules;
  Option<RateLimits> rate_limits;
  Option<Duration> offer_timeout;
  Option<Modules> modules;
  Option<std::string> modulesDir;
  std::string authenticators;
  std::string allocator;
  double allocator_node_recovery_factor;
  Duration allocator_recovery_timeout;
  Option<std::set<std::string>> fair_sharing_excluded_resource_names;
  bool filter_gpu_resources;
  bool filter_shared_resources;
  std::string min_allocatable_resources;
  Option<std::string> hooks;
  Duration node_ping_timeout;
  size_t max_node_ping_timeouts;
  std::string authorizers;
  std::string http_authenticators;
  Option<std::string> http_framework_authenticators;
  size_t max_operator_event_stream_subscribers;
  size_t max_completed_frameworks;
  size_t max_completed_tasks_per_framework;
  size_t max_unreachable_tasks_per_framework;
  Option<std::string> controller_contender;
  Option<std::string> controller_detector;
  Option<std::string> etcd;
  Duration registry_gc_interval;
  Duration registry_max_node_age;
  size_t registry_max_node_count;
  bool require_node_domain;
  bool publish_per_framework_metrics;
  Option<DomainInfo> domain;

  // The following flags are executable specific (e.g., since we only
  // have one instance of libprocess per execution, we only want to
  // advertise the IP and port option once, here).

  Option<std::string> ip;
  uint16_t port;
  Option<std::string> advertise_ip;
  Option<std::string> advertise_port;
  bool memory_profiling;

  // Optional IP discover script that will set the Controller IP.
  // If set, its output is expected to be a valid parseable IP string.
  Option<std::string> ip_discovery_command;

#ifdef ENABLE_PORT_MAPPING_ISOLATOR
  Option<size_t> max_executors_per_node;
#endif  // ENABLE_PORT_MAPPING_ISOLATOR

  Bytes offer_constraints_re2_max_mem;
  int offer_constraints_re2_max_program_size;
};

} // namespace controller {
} // namespace internal {
} // namespace medios {

#endif // __CONTROLLER_FLAGS_HPP__
