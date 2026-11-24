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

#ifndef __NODE_CONSTANTS_HPP__
#define __NODE_CONSTANTS_HPP__

#include <stdint.h>
#include <vector>

#include <medios/medios.hpp>

#include <stout/bytes.hpp>
#include <stout/duration.hpp>

namespace medios {
namespace internal {
namespace node {

// TODO(jieyu): Use static functions for all the constants. See more
// details in MESOS-1023.

constexpr Duration EXECUTOR_REGISTRATION_TIMEOUT = Minutes(1);
constexpr Duration EXECUTOR_REREGISTRATION_TIMEOUT = Seconds(2);

// The maximum timeout within which an executor can reregister.
// Note that this value has to be << 'MIN_NODE_REREGISTER_TIMEOUT'
// declared in 'controller/constants.hpp'; since node recovery will only
// complete after this timeout has elapsed, this ensures that the
// node can reregister with the controller before it is marked
// unreachable and its tasks are transitioned to TASK_UNREACHABLE or
// TASK_LOST.
constexpr Duration MAX_EXECUTOR_REREGISTRATION_TIMEOUT = Seconds(15);

// The default amount of time to wait for the executor to
// shut down before destroying the container.
constexpr Duration DEFAULT_EXECUTOR_SHUTDOWN_GRACE_PERIOD = Seconds(5);

// The default amount of time between heartbeats sent to HTTP executors.
constexpr Duration DEFAULT_EXECUTOR_HEARTBEAT_INTERVAL = Minutes(30);

constexpr Duration RECOVERY_TIMEOUT = Minutes(15);

// TODO(gkleiman): Move this to a different file once `TaskStatusUpdateManager`
// uses `StatusUpdateManagerProcess`. See MESOS-8296.
constexpr Duration STATUS_UPDATE_RETRY_INTERVAL_MIN = Seconds(10);
constexpr Duration STATUS_UPDATE_RETRY_INTERVAL_MAX = Minutes(10);

// Default backoff interval used by the node to wait before registration.
constexpr Duration DEFAULT_REGISTRATION_BACKOFF_FACTOR = Seconds(1);

// The maximum interval the node waits before retrying registration.
// Note that this value has to be << 'MIN_NODE_REREGISTER_TIMEOUT'
// declared in 'controller/constants.hpp'. This helps the node to retry
// (re-)registration multiple times between when the controller finishes
// recovery and when it times out node re-registration.
constexpr Duration REGISTER_RETRY_INTERVAL_MAX = Minutes(1);

// Default value for `--authentication_backoff_factor`. The backoff interval
// factor affects the node timeout interval after failed authentications.
constexpr Duration DEFAULT_AUTHENTICATION_BACKOFF_FACTOR = Seconds(1);

// Default value for `--authentication_timeout_min`. The minimum timeout
// interval the node waits before retrying authentication.
constexpr Duration DEFAULT_AUTHENTICATION_TIMEOUT_MIN = Seconds(5);

// Default value for `--authentication_timeout_max`. The maximum timeout
// interval the node waits before retrying authentication.
constexpr Duration DEFAULT_AUTHENTICATION_TIMEOUT_MAX = Minutes(1);

constexpr Duration GC_DELAY = Weeks(1);
constexpr Duration DISK_WATCH_INTERVAL = Minutes(1);

// Minimum free disk capacity enforced by the garbage collector.
constexpr double GC_DISK_HEADROOM = 0.1;

// Maximum number of completed frameworks to store in memory.
constexpr size_t MAX_COMPLETED_FRAMEWORKS = 50;

// Default maximum number of completed executors per framework
// to store in memory.
constexpr size_t DEFAULT_MAX_COMPLETED_EXECUTORS_PER_FRAMEWORK = 150;

// Maximum number of a container id length, according to the
// max entry length for directory names on AUFS.
constexpr size_t MAX_CONTAINER_ID_LENGTH = 242;

// Maximum number of completed tasks per executor to store in memory.
//
// NOTE: This should be greater than zero because the node looks
// for completed tasks to determine (with false positives) whether
// an executor ever received tasks. See MESOS-8411.
//
// TODO(mzhu): Remove this note once we can determine whether an
// executor ever received tasks without looking through the
// completed tasks.
constexpr size_t MAX_COMPLETED_TASKS_PER_EXECUTOR = 200;

// Default cpus offered by the node.
constexpr double DEFAULT_CPUS = 1;

// Default memory offered by the node.
constexpr Bytes DEFAULT_MEM = Gigabytes(1);

// Default disk space offered by the node.
constexpr Bytes DEFAULT_DISK = Gigabytes(10);

// Default ports range offered by the node.
constexpr char DEFAULT_PORTS[] = "[31000-32000]";

// Default cpu resource given to a command executor.
constexpr double DEFAULT_EXECUTOR_CPUS = 0.1;

// Default memory resource given to a command executor.
constexpr Bytes DEFAULT_EXECUTOR_MEM = Megabytes(32);

#ifdef ENABLE_PORT_MAPPING_ISOLATOR
// Default number of ephemeral ports allocated to a container by the
// network isolator.
constexpr uint16_t DEFAULT_EPHEMERAL_PORTS_PER_CONTAINER = 1024;
#endif

// Default filename used for domain socket-based communication between
// node and executors, if that is enabled.
constexpr char NODE_EXECUTORS_SOCKET_FILENAME[] = "node.sock";

// Name of the default identity assertion authenticatee.
constexpr char DEFAULT_AUTHENTICATEE[] = "identity_assertion";

// Name of the default, local authorizer.
constexpr char DEFAULT_AUTHORIZER[] = "local";

// Name of the node HTTP authentication realm for read-only endpoints.
constexpr char READONLY_HTTP_AUTHENTICATION_REALM[] = "medios-node-readonly";

// Name of the node HTTP authentication realm for read-write endpoints.
constexpr char READWRITE_HTTP_AUTHENTICATION_REALM[] = "medios-node-readwrite";

// Name of the node HTTP authentication realm for HTTP executors.
constexpr char EXECUTOR_HTTP_AUTHENTICATION_REALM[] = "medios-node-executor";

// Name of the node HTTP authentication realm for HTTP resource providers.
constexpr char RESOURCE_PROVIDER_HTTP_AUTHENTICATION_REALM[] =
  "medios-node-resource-provider";

// Default timeout for image/provisioning downloads to wait when a net download
// stalls.
constexpr Duration DEFAULT_FETCHER_STALL_TIMEOUT = Minutes(1);

// If no pings received within this timeout, then the node will
// trigger a re-detection of the controller to cause a re-registration.
Duration DEFAULT_CONTROLLER_PING_TIMEOUT();

// Name of the executable for default executor.
constexpr char MEDIOS_DEFAULT_EXECUTOR[] = "medios-default-executor";
constexpr char MEDIOS_EXECUTOR[] = "medios-executor";

// Name of the component used for describing pending futures.
constexpr char COMPONENT_NAME_CONTAINERIZER[] = "containerizer";


// Virtual path on which node logs are mounted in `/files/` endpoint.
constexpr char NODE_LOG_VIRTUAL_PATH[] = "/node/log";

// Where a declared store is served read-only through the files API, under
// its name (DR-042, medios#585).
constexpr char STORES_VIRTUAL_PATH[] = "/stores";

std::vector<NodeInfo::Capability> NODE_CAPABILITIES();

} // namespace node {
} // namespace internal {
} // namespace medios {

#endif // __NODE_CONSTANTS_HPP__
