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

#include "node/flags.hpp"

#include <stout/error.hpp>
#include <stout/flags.hpp>
#include <stout/json.hpp>
#include <stout/option.hpp>
#include <stout/os.hpp>
#include <stout/path.hpp>

#include <medios/type_utils.hpp>

#include "common/domain_sockets.hpp"

#include "common/http.hpp"
#include "common/parse.hpp"
#include "common/protobuf_utils.hpp"

#include "node/constants.hpp"

#ifdef __linux__
#include <unistd.h>

#include "node/containerizer/medios/linux_launcher.hpp"

#include "linux/cgroups2.hpp"

#endif // __linux__

#include "node/containerizer/medios/provisioner/constants.hpp"

using std::string;

medios::internal::node::Flags::Flags()
{
  add(&Flags::hostname,
      "hostname",
      "The hostname the node should report.\n"
      "If left unset, the hostname is resolved from the IP address\n"
      "that the node advertises; unless the user explicitly prevents\n"
      "that, using `--no-hostname_lookup`, in which case the IP itself\n"
      "is used.");

  add(&Flags::hostname_lookup,
      "hostname_lookup",
      "Whether we should execute a lookup to find out the server's hostname,\n"
      "if not explicitly set (via, e.g., `--hostname`).\n"
      "True by default; if set to `false` it will cause Medios\n"
      "to use the IP address, unless the hostname is explicitly set.",
      true);

  add(&Flags::version,
      "version",
      "Show version and exit.",
      false);

  add(&Flags::stores,
      "stores",
      "Path to the stores file: the directories this node declares as\n"
      "stores (DR-036). One shared resource `store/<name>` is added to\n"
      "the node's total per store, carrying the declared class, locality\n"
      "and reader and writer counts. The node refuses to start when a\n"
      "declared path does not exist. A node that declares no store does\n"
      "not set this and refuses every volume.");

  // TODO(benh): Is there a way to specify units for the resources?
  add(&Flags::resources,
      "resources",
      "Total consumable resources per node. Can be provided in JSON format\n"
      "or as a semicolon-delimited list of key:value pairs, with the role\n"
      "optionally specified.\n"
      "\n"
      "As a key:value list:\n"
      "`name(role):value;name:value...`\n"
      "\n"
      "To use JSON, pass a JSON-formatted string or use\n"
      "`--resources=filepath` to specify the resources via a file containing\n"
      "a JSON-formatted string. `filepath` can only be of the form\n"
      "`file:///path/to/file`.\n"
      "\n"
      "Example JSON:\n"
      "[\n"
      "  {\n"
      "    \"name\": \"cpus\",\n"
      "    \"type\": \"SCALAR\",\n"
      "    \"scalar\": {\n"
      "      \"value\": 24\n"
      "    }\n"
      "  },\n"
      "  {\n"
      "    \"name\": \"mem\",\n"
      "    \"type\": \"SCALAR\",\n"
      "    \"scalar\": {\n"
      "      \"value\": 24576\n"
      "    }\n"
      "  }\n"
      "]");

  add(&Flags::resource_provider_config_dir,
      "resource_provider_config_dir",
      "Path to a directory that contains local resource provider configs.\n"
      "Each file in the config dir should contain a JSON object representing\n"
      "a `ResourceProviderInfo` object. Each local resource provider provides\n"
      "resources that are local to the node. It is also responsible for\n"
      "handling operations on the resources it provides. Please note that\n"
      "`resources` field might not need to be specified if the resource\n"
      "provider determines the resources automatically.\n"
      "\n"
      "Example config file in this directory:\n"
      "{\n"
      "  \"type\": \"org.medios.apache.rp.local.storage\",\n"
      "  \"name\": \"lvm\"\n"
      "}");

  add(&Flags::isolation,
      "isolation",
      "Isolation mechanisms to use, e.g., `posix/cpu,posix/mem`, or\n"
      "`cgroups/cpu,cgroups/mem`, or `cgroups/all`,\n"
      "or load an alternate isolator module using the `--modules`\n"
      "flag. if `cgroups/all` is specified, any other cgroups related\n"
      "isolation options (e.g., `cgroups/cpu`) will be ignored, and all\n"
      "the local enabled cgroups subsystems on the node host will be\n"
      "automatically loaded by the cgroups isolator. Note that this flag\n"
      "is only relevant for the Medios Containerizer.",
      "posix/cpu,posix/mem");

  add(&Flags::launcher,
      "launcher",
      "The launcher to be used for Medios containerizer. It could either be\n"
      "`linux` or `posix`. The Linux launcher is required for cgroups\n"
      "isolation. If unspecified, the node will choose the Linux launcher\n"
      "if it's running as root on Linux with cgroups available, and the\n"
      "posix launcher otherwise, including for a non-root user on a\n"
      "cgroup v2 host.",
#ifdef __linux__
      ::geteuid() == 0 && LinuxLauncher::available() ? "linux" : "posix"
#else
      "posix"
#endif // __linux__
      );

  add(&Flags::image_providers,
      "image_providers",
      "Comma-separated list of supported image providers,\n"
      "e.g., `DOCKER`.");

  add(&Flags::image_provisioner_backend,
      "image_provisioner_backend",
      "Strategy for provisioning container rootfs from images,\n"
      "e.g., `aufs`, `bind`, `copy`, `overlay`.");

  add(&Flags::image_gc_config,
      "image_gc_config",
      "JSON-formatted configuration for automatic container image garbage\n"
      "collection. This is an optional flag. If it is not set, it means\n"
      "the automatic container image gc is not enabled. Users have to\n"
      "trigger image gc manually via the operator API. If it is set, the\n"
      "auto image gc is enabled. This image gc config can be provided either\n"
      "as a path pointing to a local file, or as a JSON-formatted string.\n"
      "Please note that the image garbage collection only work with Medios\n"
      "Containerizer for now.\n"
      "\n"
      "See the ImageGcConfig message in `flags.proto` for the expected\n"
      "format.\n"
      "\n"
      "In the following example, image garbage collection is configured to\n"
      "sample disk usage every hour, and will attempt to maintain at least\n"
      "10 percent of free space on the container image filesystem:\n"
      "{\n"
      "  \"image_disk_headroom\": 0.1,\n"
      "  \"image_disk_watch_interval\": {\n"
      "    \"nanoseconds\": 3600000000000\n"
      "  },\n"
      "  \"excluded_images\": []\n"
      "}");

  add(&Flags::docker_registry,
      "docker_registry",
      "The default url for Medios containerizer to pull Docker images. It\n"
      "could either be a Docker registry server url (e.g., `https://registry.docker.io`),\n" // NOLINT(whitespace/line_length)
      "or a source that Docker image archives (result of `docker save`) are\n"
      "stored. The Docker archive source must be specified as a local\n"
      "path (e.g., `/tmp/docker/images`). Note that this option won't\n"
      "change the default registry server for Docker containerizer.",
      "https://registry-1.docker.io");

  add(&Flags::docker_store_dir,
      "docker_store_dir",
      "Directory the Docker provisioner will store images in",
      path::join(os::temp(), "medios", "store", "docker"));

  add(&Flags::docker_ignore_runtime,
      "docker_ignore_runtime",
      "Ignore any runtime configuration specified in the Docker image. The\n"
      "Medios containerizer will not propagate Docker runtime specifications\n"
      "such as `WORKDIR`, `ENV` and `CMD` to the container.\n",
      false);

  add(&Flags::default_role,
      "default_role",
      "Any resources in the `--resources` flag that\n"
      "omit a role, as well as any resources that\n"
      "are not present in `--resources` but that are\n"
      "automatically detected, will be assigned to\n"
      "this role.",
      "*");

  add(&Flags::attributes,
      "attributes",
      "Attributes of the node machine, in the form:\n"
      "`rack:2` or `rack:2;u:1`");

  add(&Flags::fetcher_stall_timeout,
      "fetcher_stall_timeout",
      "Amount of time for image/provisioning downloads to wait before\n"
      "considering a download too slow and aborting it when the download\n"
      "stalls (i.e., the speed keeps below one byte per second).",
      DEFAULT_FETCHER_STALL_TIMEOUT);

  add(&Flags::fetcher_allow_local_uris,
      "fetcher_allow_local_uris",
      "Whether the fetcher is allowed to copy local host filesystem URIs\n"
      "(`file://` or bare file paths) into task sandboxes. Set to `false`\n"
      "in production cloud deployments to prevent tasks from staging\n"
      "arbitrary host files (#399).",
      true);

  add(&Flags::fetcher_allowed_local_uri_prefix,
      "fetcher_allowed_local_uri_prefix",
      "When set, local host filesystem URIs (`file://` or bare file paths)\n"
      "may only be fetched if their canonical (`realpath`) path resides\n"
      "within this canonical directory prefix (#399).");

  add(&Flags::work_dir,
      "work_dir",
      "Path of the node work directory. This is where executor sandboxes\n"
      "will be placed, as well as the node's checkpointed state in case of\n"
      "failover. Note that locations like `/tmp` which are cleaned\n"
      "automatically are not suitable for the work directory when running in\n"
      "production, since long-running nodes could lose data when cleanup\n"
      "occurs. (Example: `/var/lib/medios/node`)");

  add(&Flags::runtime_dir,
      "runtime_dir",
      "Path of the node runtime directory. This is where runtime data\n"
      "is stored by a node that it needs to persist across crashes (but\n"
      "not across reboots). This directory will be cleared on reboot.\n"
      "(Example: `/var/run/medios`)",
      []() -> string {
        Try<string> var = os::var();
        if (var.isSome()) {
          const string prefix(path::join(var.get(), "run"));

          // We check for access on the prefix because the remainder
          // of the directory structure is created by the node later.
          Try<bool> access = os::access(prefix, R_OK | W_OK);
          if (access.isSome() && access.get()) {
            return path::join(prefix, "medios");
          }
        }

        // We provide a fallback path for ease of use in case `os::var()`
        // errors or if the directory is not accessible.
        return path::join(os::temp(), "medios", "runtime");
      }());

  add(&Flags::launcher_dir, // TODO(benh): This needs a better name.
      "launcher_dir",
      "Directory path of Medios binaries. Medios looks for the containerizer\n"
      "and executor binary files under this directory.",
      PKGLIBEXECDIR);

  add(&Flags::switch_user,
      "switch_user",
      "If set to `true`, the node will attempt to run tasks as\n"
      "the `user` who submitted them (as defined in `FrameworkInfo`)\n"
      "(this requires `setuid` permission and that the given `user`\n"
      "exists on the node).\n"
      "If the user does not exist, an error occurs and the task will fail.\n"
      "If set to `false`, tasks will be run as the same user as the Medios\n"
      "node process.",
      true);

  add(&Flags::volume_gid_range,
      "volume_gid_range",
      "When this flag is specified, if a task running as non-root user uses a\n"
      "shared persistent volume or a PARENT type SANDBOX_PATH volume, the\n"
      "volume will be owned by a gid allocated from this range and have the\n"
      "`setgid` bit set, and the task process will be launched with the gid\n"
      "as its supplementary group to make sure it can access the volume.\n"
      "(Example: `[10000-20000]`)");

  add(&Flags::http_heartbeat_interval,
      "http_heartbeat_interval",
      "This flag sets a heartbeat interval (e.g. '5secs', '10mins') for\n"
      "messages to be sent over persistent connections made against\n"
      "the node HTTP API. Currently, this only applies to the\n"
      "'LAUNCH_NESTED_CONTAINER_SESSION' and 'ATTACH_CONTAINER_OUTPUT' calls.",
      Seconds(30));

  add(&Flags::registration_backoff_factor,
      "registration_backoff_factor",
      "Node initially picks a random amount of time between `[0, b]`, where\n"
      "`b = registration_backoff_factor`, to (re-)register with a new controller.\n"
      "Subsequent retries are exponentially backed off based on this\n"
      "interval (e.g., 1st retry uses a random value between `[0, b * 2^1]`,\n"
      "2nd retry between `[0, b * 2^2]`, 3rd retry between `[0, b * 2^3]`,\n"
      "etc) up to a maximum of " + stringify(REGISTER_RETRY_INTERVAL_MAX),
      DEFAULT_REGISTRATION_BACKOFF_FACTOR);

  add(&Flags::authentication_backoff_factor,
      "authentication_backoff_factor",
      "The node will time out its authentication with the controller based on\n"
      "exponential backoff. The timeout will be randomly chosen within the\n"
      "range `[min, min + factor*2^n]` where `n` is the number of failed\n"
      "attempts. To tune these parameters, set the\n"
      "`--authentication_timeout_[min|max|factor]` flags.\n",
      DEFAULT_AUTHENTICATION_BACKOFF_FACTOR);

  add(&Flags::authentication_timeout_min,
      "authentication_timeout_min",
      "The minimum amount of time the node waits before retrying\n"
      "authenticating with the controller. See `authentication_backoff_factor`\n"
      "for more details. NOTE that since authentication retry cancels the\n"
      "previous authentication request, one should consider what is the\n"
      "normal authentication delay when setting this flag to prevent\n"
      "premature retry.",
      DEFAULT_AUTHENTICATION_TIMEOUT_MIN);

  add(&Flags::authentication_timeout_max,
      "authentication_timeout_max",
      "The maximum amount of time the node waits before retrying\n"
      "authenticating with the controller. See `authentication_backoff_factor`\n"
      "for more details.",
      DEFAULT_AUTHENTICATION_TIMEOUT_MAX);

  add(&Flags::executor_environment_variables,
      "executor_environment_variables",
      "JSON object representing the environment variables that should be\n"
      "passed to the executor, and thus subsequently task(s). By default this\n"
      "flag is none. Users have to define executor environment explicitly.\n"
      "Example:\n"
      "{\n"
      "  \"PATH\": \"/bin:/usr/bin\",\n"
      "  \"LD_LIBRARY_PATH\": \"/usr/local/lib\"\n"
      "}",
      [](const Option<JSON::Object>& object) -> Option<Error> {
        if (object.isSome()) {
          foreachvalue (const JSON::Value& value, object->values) {
            if (!value.is<JSON::String>()) {
              return Error("`executor_environment_variables` must "
                           "only contain string values");
            }
          }
        }
        return None();
      });

  add(&Flags::executor_registration_timeout,
      "executor_registration_timeout",
      "Amount of time to wait for an executor\n"
      "to register with the node before considering it hung and\n"
      "shutting it down (e.g., 60secs, 3mins, etc)",
      EXECUTOR_REGISTRATION_TIMEOUT);

  add(&Flags::executor_reregistration_timeout,
      "executor_reregistration_timeout",
      "The timeout within which an executor is expected to reregister after\n"
      "the node has restarted, before the node considers it gone and shuts\n"
      "it down. Note that currently, the node will not reregister with the\n"
      "controller until this timeout has elapsed (see MESOS-7539).",
      EXECUTOR_REREGISTRATION_TIMEOUT,
      [](const Duration& value) -> Option<Error> {
        if (value > MAX_EXECUTOR_REREGISTRATION_TIMEOUT) {
          return Error("Expected `--executor_reregistration_timeout` "
                       "to be not more than " +
                       stringify(MAX_EXECUTOR_REREGISTRATION_TIMEOUT));
        }
        return None();
      });

  // TODO(bmahler): Remove this once v0 executors are no longer supported.
  add(&Flags::executor_reregistration_retry_interval,
      "executor_reregistration_retry_interval",
      "For PID-based executors, how long the node waits before retrying\n"
      "the reconnect message sent to the executor during recovery.\n"
      "NOTE: Do not use this unless you understand the following\n"
      "(see MESOS-5332): PID-based executors using Medios libraries >= 1.1.2\n"
      "always re-link with the node upon receiving the reconnect message.\n"
      "This avoids the executor replying on a half-open TCP connection to\n"
      "the old node (possible if netfilter is dropping packets,\n"
      "see: MESOS-7057). However, PID-based executors using Medios\n"
      "libraries < 1.1.2 do not re-link and are therefore prone to\n"
      "replying on a half-open connection after the node restarts. If we\n"
      "only send a single reconnect message, these \"old\" executors will\n"
      "reply on their half-open connection and receive a RST; without any\n"
      "retries, they will fail to reconnect and be killed by the node once\n"
      "the executor re-registration timeout elapses. To ensure these \"old\"\n"
      "executors can reconnect in the presence of netfilter dropping\n"
      "packets, we introduced optional retries of the reconnect message.\n"
      "This results in \"old\" executors correctly establishing a link\n"
      "when processing the second reconnect message.");

  add(&Flags::executor_shutdown_grace_period,
      "executor_shutdown_grace_period",
      "Default amount of time to wait for an executor to shut down\n"
      "(e.g. 60secs, 3mins, etc). ExecutorInfo.shutdown_grace_period\n"
      "overrides this default. Note that the executor must not assume\n"
      "that it will always be allotted the full grace period, as the\n"
      "node may decide to allot a shorter period, and failures / forcible\n"
      "terminations may occur.",
      DEFAULT_EXECUTOR_SHUTDOWN_GRACE_PERIOD);

#ifdef USE_SSL_SOCKET
  add(&Flags::jwt_secret_key,
      "jwt_secret_key",
      flags::DeprecatedName("executor_secret_key"),
      "Path to a file containing the key used when generating JWT secrets.\n"
      "This flag is only available when Medios is built with SSL support.");
#endif // USE_SSL_SOCKET

  add(&Flags::gc_delay,
      "gc_delay",
      "Maximum amount of time to wait before cleaning up\n"
      "executor directories (e.g., 3days, 2weeks, etc).\n"
      "Note that this delay may be shorter depending on\n"
      "the available disk usage.",
      GC_DELAY);

  add(&Flags::gc_disk_headroom,
      "gc_disk_headroom",
      "Adjust disk headroom used to calculate maximum executor\n"
      "directory age. Age is calculated by:\n"
      "`gc_delay * max(0.0, (1.0 - gc_disk_headroom - disk usage))`\n"
      "every `--disk_watch_interval` duration. `gc_disk_headroom` must\n"
      "be a value between 0.0 and 1.0",
      GC_DISK_HEADROOM);

  add(&Flags::gc_non_executor_container_sandboxes,
      "gc_non_executor_container_sandboxes",
      "Determines whether nested container sandboxes created via the\n"
      "LAUNCH_CONTAINER and LAUNCH_NESTED_CONTAINER APIs will be\n"
      "automatically garbage collected by the node upon termination.\n"
      "The REMOVE_(NESTED_)CONTAINER API is unaffected by this flag\n"
      "and can still be used.",
      false);

  add(&Flags::disk_watch_interval,
      "disk_watch_interval",
      "Periodic time interval (e.g., 10secs, 2mins, etc)\n"
      "to check the overall disk usage managed by the node.\n"
      "This drives the garbage collection of archived\n"
      "information and sandboxes.",
      DISK_WATCH_INTERVAL);

  add(&Flags::container_logger,
      "container_logger",
      "The name of the container logger to use for logging container\n"
      "(i.e., executor and task) stdout and stderr. The default\n"
      "container logger writes to `stdout` and `stderr` files\n"
      "in the sandbox directory. Medios ships no in-tree rotating\n"
      "container logger; log rotation, retention, and collection are\n"
      "delegated to host, runtime, or platform logging.");

  add(&Flags::recover,
      "recover",
      "Whether to recover status updates and reconnect with old executors.\n"
      "Valid values for `recover` are\n"
      "reconnect: Reconnect with any old live executors.\n"
      "cleanup  : Kill any old live executors and exit.\n"
      "           Use this option when doing an incompatible node\n"
      "           or executor upgrade.",
      "reconnect");

  add(&Flags::recovery_timeout,
      "recovery_timeout",
      "Amount of time allotted for the node to recover. If the node takes\n"
      "longer than recovery_timeout to recover, any executors that are\n"
      "waiting to reconnect to the node will self-terminate.\n"
      "The best value of this flag depends on the frameworks being run.\n"
      "For non-partition-aware frameworks, it makes sense to set this\n"
      "close to the `node_reregister_timeout` on the controller.\n"
      "For partition-aware frameworks, it makes sense to set this higher\n"
      "than the timeout that the framework uses to give up on the task,\n"
      "otherwise the executor might terminate even if the task could still\n"
      "successfully reconnect to the framework.",
      RECOVERY_TIMEOUT);

  add(&Flags::reconfiguration_policy,
      "reconfiguration_policy",
      "This flag controls which node configuration changes are considered\n"
      "acceptable when recovering the previous node state. Possible values:\n"
      "equal:    The old and the new state must match exactly.\n"
      "additive: The new state must be a superset of the old state:\n"
      "          it is permitted to add additional resources, attributes\n"
      "          and domains but not to remove or to modify existing ones.\n"
      "Note that this only affects the checking done on the node itself,\n"
      "the controller may still reject the node if it detects a change that it\n"
      "considers unacceptable, which, e.g., currently happens when port or\n"
      "hostname are changed.",
      "equal");

  add(&Flags::strict,
      "strict",
      "If `strict=true`, any and all recovery errors are considered fatal.\n"
      "If `strict=false`, any expected errors (e.g., node cannot recover\n"
      "information about an executor, because the node died right before\n"
      "the executor registered.) during recovery are ignored and as much\n"
      "state as possible is recovered.\n",
      true);

  add(&Flags::max_completed_executors_per_framework,
      "max_completed_executors_per_framework",
      "Maximum number of completed executors per framework to store\n"
      "in memory.\n",
      DEFAULT_MAX_COMPLETED_EXECUTORS_PER_FRAMEWORK);

#ifdef __linux__
  add(&Flags::cgroups_destroy_timeout,
      "cgroups_destroy_timeout",
      "Amount of time allowed to destroy a cgroup hierarchy. If the cgroup\n"
      "hierarchy is not destroyed within the timeout, the corresponding\n"
      "container destroy is considered failed.",
      Seconds(60));

  add(&Flags::cgroups_hierarchy,
      "cgroups_hierarchy",
      "The path to the cgroups hierarchy root\n", "/sys/fs/cgroup");

  add(&Flags::cgroups_root,
      "cgroups_root",
      "Name of the root cgroup\n",
      "medios");

  add(&Flags::cgroups_enable_cfs,
      "cgroups_enable_cfs",
      "Cgroups feature flag to enable hard limits on CPU resources\n"
      "via the CFS bandwidth limiting subfeature.\n",
      false);

  // TODO(antonl): Set default to true in future releases.
  add(&Flags::cgroups_limit_swap,
      "cgroups_limit_swap",
      "Cgroups feature flag to enable memory limits on both memory and\n"
      "swap instead of just memory. On cgroups v2 a container's swap is\n"
      "limited to zero, so a container at its memory limit is OOM killed\n"
      "rather than continuing in swap.\n",
      false);

  add(&Flags::cgroups_cpu_enable_pids_and_tids_count,
      "cgroups_cpu_enable_pids_and_tids_count",
      "Cgroups feature flag to enable counting of processes and threads\n"
      "inside a container.\n",
      false);

  add(&Flags::cgroups_net_cls_primary_handle,
      "cgroups_net_cls_primary_handle",
      "A non-zero, 16-bit handle of the form `0xAAAA`. This will be \n"
      "used as the primary handle for the net_cls cgroup.");

  add(&Flags::cgroups_net_cls_secondary_handles,
      "cgroups_net_cls_secondary_handles",
      "A range of the form 0xAAAA,0xBBBB, specifying the valid secondary\n"
      "handles that can be used with the primary handle. This will take\n"
      "effect only when the `--cgroups_net_cls_primary_handle is set.");

  add(&Flags::allowed_devices,
      "allowed_devices",
      "JSON array representing the devices that will be additionally\n"
      "allowlisted by the cgroups devices subsystem. The following\n"
      "devices are always allowlisted by default:\n"
      "  * /dev/console\n"
      "  * /dev/tty0\n"
      "  * /dev/tty1\n"
      "  * /dev/pts/*\n"
      "  * /dev/ptmx\n"
      "  * /dev/net/tun\n"
      "  * /dev/null\n"
      "  * /dev/zero\n"
      "  * /dev/full\n"
      "  * /dev/tty\n"
      "  * /dev/urandom\n"
      "  * /dev/random\n"
      "This flag will take effect only when `cgroups/devices` is set in\n"
      "`--isolation` flag.\n"
      "Example:\n"
      "{\n"
      "  \"allowed_devices\": [\n"
      "    {\n"
      "      \"device\": {\n"
      "        \"path\": \"/path/to/device\"\n"
      "      },\n"
      "      \"access\": {\n"
      "        \"read\": true,\n"
      "        \"write\": false,\n"
      "        \"mknod\": false\n"
      "      }\n"
      "    }\n"
      "  ]\n"
      "}\n");

  add(&Flags::node_subsystems,
      "node_subsystems",
      "List of comma-separated cgroup subsystems to run the node binary\n"
      "in, e.g., `memory,cpuacct`. The default is none.\n"
      "Present functionality is intended for resource monitoring and\n"
      "no cgroup limits are set, they are inherited from the root medios\n"
      "cgroup.");

  add(&Flags::perf_events,
      "perf_events",
      "List of command-separated perf events to sample for each container\n"
      "when using the perf_event isolator. Default is none.\n"
      "Run command `perf list` to see all events. Event names are\n"
      "sanitized by downcasing and replacing hyphens with underscores\n"
      "when reported in the PerfStatistics protobuf, e.g., `cpu-cycles`\n"
      "becomes `cpu_cycles`; see the PerfStatistics protobuf for all names.");

  add(&Flags::perf_interval,
      "perf_interval",
      "Interval between the start of perf stat samples. Perf samples are\n"
      "obtained periodically according to `perf_interval` and the most\n"
      "recently obtained sample is returned rather than sampling on\n"
      "demand. For this reason, `perf_interval` is independent of the\n"
      "resource monitoring interval",
      Seconds(60));

  add(&Flags::perf_duration,
      "perf_duration",
      "Duration of a perf stat sample. The duration must be less\n"
      "than the `perf_interval`.",
      Seconds(10));

  add(&Flags::revocable_cpu_low_priority,
      "revocable_cpu_low_priority",
      "Run containers with revocable CPU at a lower priority than\n"
      "normal containers (non-revocable cpu). Currently only\n"
      "supported by the cgroups/cpu isolator.",
      true);

  add(&Flags::systemd_enable_support,
      "systemd_enable_support",
      "Top level control of systemd support. When enabled, features such as\n"
      "executor life-time extension are enabled unless there is an explicit\n"
      "flag to disable these (see other flags). This should be enabled when\n"
      "the node is launched as a systemd unit.",
      true);

  add(&Flags::systemd_runtime_directory,
      "systemd_runtime_directory",
      "The path to the systemd system run time directory\n",
      "/run/systemd/system");

  add(&Flags::effective_capabilities,
      "effective_capabilities",
      flags::DeprecatedName("allowed_capabilities"),
      "JSON representation of the Linux capabilities that the node will\n"
      "grant to a task that will be run in containers launched by the\n"
      "containerizer (currently only supported by the Medios Containerizer).\n"
      "This set overrides the default capabilities for the user but not\n"
      "the capabilities requested by the framework.\n"
      "\n"
      "To set capabilities the node should have the `SETPCAP` capability.\n"
      "\n"
      "This flag is effective iff `linux/capabilities` isolation is enabled.\n"
      "When `linux/capabilities` isolation is enabled, the absence of this\n"
      "flag implies that the operator intends to allow ALL capabilities.\n"
      "\n"
      "Example:\n"
      "{\n"
      "   \"capabilities\": [\n"
      "       \"NET_RAW\",\n"
      "       \"SYS_ADMIN\"\n"
      "     ]\n"
      "}");

  add(&Flags::bounding_capabilities,
      "bounding_capabilities",
      "JSON representation of the Linux capabilities that the operator\n"
      "will allow as the maximum level of privilege that a task launched\n"
      "by the containerizer may acquire (currently only supported by the\n"
      "Medios Containerizer).\n"
      "\n"
      "This flag is effective iff `linux/capabilities` isolation is enabled.\n"
      "When `linux/capabilities` isolation is enabled, the absence of this\n"
      "flag implies that the operator allows ALL capabilities.\n"
      "\n"
      "This flag has the same syntax as `--effective_capabilities`."
     );

  add(&Flags::default_container_shm_size,
      "default_container_shm_size",
      "The default size of the /dev/shm for the container which has its own\n"
      "/dev/shm but does not specify the `shm_size` field in its `LinuxInfo`.\n"
      "The format is [number][unit], number must be a positive integer and\n"
      "unit can be B (bytes), KB (kilobytes), MB (megabytes), GB (gigabytes)\n"
      "or TB (terabytes). Note that this flag is only relevant for the Medios\n"
      "Containerizer and it will be ignored if the `namespaces/ipc` isolator\n"
      "is not enabled."
      );

  add(&Flags::disallow_sharing_node_ipc_namespace,
      "disallow_sharing_node_ipc_namespace",
      "If set to `true`, each top-level container will have its own IPC\n"
      "namespace and /dev/shm, and if the framework requests to share the\n"
      "node IPC namespace and /dev/shm for the top level container, the\n"
      "container launch will be rejected. If set to `false`, the top-level\n"
      "containers will share the IPC namespace and /dev/shm with node if\n"
      "the framework requests it. This flag will be ignored if the\n"
      "`namespaces/ipc` isolator is not enabled.\n",
      false);

  add(&Flags::disallow_sharing_node_pid_namespace,
      "disallow_sharing_node_pid_namespace",
      "If set to `true`, each top-level container will have its own pid\n"
      "namespace, and if the framework requests to share the node pid\n"
      "namespace for the top level container, the container launch will be\n"
      "rejected. If set to `false`, the top-level containers will share the\n"
      "pid namespace with node if the framework requests it. This flag will\n"
      "be ignored if the `namespaces/pid` isolator is not enabled.\n",
      false);
#endif

  add(&Flags::node_features,
      "node_features",
      "JSON representation of node features to allowlist. We always require\n"
      "'MULTI_ROLE', 'HIERARCHICAL_ROLE', 'RESERVATION_REFINEMENT',\n"
      "'NODE_OPERATION_FEEDBACK', 'RESOURCE_PROVIDER', 'NODE_DRAINING', and\n"
      "'TASK_RESOURCE_LIMITS'.\n"
      "\n"
      "Example:\n"
      "{\n"
      "    \"capabilities\": [\n"
      "        {\"type\": \"MULTI_ROLE\"},\n"
      "        {\"type\": \"HIERARCHICAL_ROLE\"},\n"
      "        {\"type\": \"RESERVATION_REFINEMENT\"},\n"
      "        {\"type\": \"NODE_OPERATION_FEEDBACK\"},\n"
      "        {\"type\": \"RESOURCE_PROVIDER\"},\n"
      "        {\"type\": \"NODE_DRAINING\"},\n"
      "        {\"type\": \"TASK_RESOURCE_LIMITS\"}\n"
      "    ]\n"
      "}\n",
      [](const Option<NodeCapabilities>& nodeFeatures) -> Option<Error> {
        // Check all required capabilities are enabled.
        if (nodeFeatures.isSome()) {
          protobuf::node::Capabilities capabilities(
              nodeFeatures->capabilities());

          if (!capabilities.multiRole ||
              !capabilities.hierarchicalRole ||
              !capabilities.reservationRefinement ||
              !capabilities.nodeOperationFeedback ||
              !capabilities.resourceProvider ||
              !capabilities.nodeDraining ||
              !capabilities.taskResourceLimits) {
            return Error(
                "At least the following node features need to be enabled:"
                " MULTI_ROLE, HIERARCHICAL_ROLE, RESERVATION_REFINEMENT,"
                " NODE_OPERATION_FEEDBACK, RESOURCE_PROVIDER, NODE_DRAINING,"
                " and TASK_RESOURCE_LIMITS");
          }
        }

        return None();
      });

  add(&Flags::firewall_rules,
      "firewall_rules",
      "The value could be a JSON-formatted string of rules or a\n"
      "file path containing the JSON-formatted rules used in the endpoints\n"
      "firewall. Path must be of the form `file:///path/to/file`\n"
      "or `/path/to/file`.\n"
      "\n"
      "See the `Firewall` message in `flags.proto` for the expected format.\n"
      "\n"
      "Example:\n"
      "{\n"
      "  \"disabled_endpoints\": {\n"
      "    \"paths\": [\n"
      "      \"/files/browse\",\n"
      "      \"/metrics/snapshot\"\n"
      "    ]\n"
      "  }\n"
      "}");

  add(&Flags::credential,
      "credential",
      "Path to a JSON-formatted file containing the credential\n"
      "to use to authenticate with the controller.\n"
      "Path could be of the form `file:///path/to/file` or `/path/to/file`."
      "\n"
      "Example:\n"
      "{\n"
      "  \"principal\": \"username\",\n"
      "  \"secret\": \"secret\"\n"
      "}");

  add(&Flags::acls,
      "acls",
      "The value could be a JSON-formatted string of ACLs\n"
      "or a file path containing the JSON-formatted ACLs used\n"
      "for authorization. Path could be of the form `file:///path/to/file`\n"
      "or `/path/to/file`.\n"
      "\n"
      "Note that if the `--authorizer` flag is provided with a value\n"
      "other than `" + string(DEFAULT_AUTHORIZER) + "`, the ACLs contents\n"
      "will be ignored.\n"
      "\n"
      "See the ACLs protobuf in acls.proto for the expected format.\n"
      "\n"
      "Example:\n"
      "{\n"
      "  \"get_endpoints\": [\n"
      "    {\n"
      "      \"principals\": { \"values\": [\"a\"] },\n"
      "      \"paths\": { \"values\": [\"/flags\", \"/monitor/statistics\"] }\n"
      "    }\n"
      "  ]\n"
      "}");

  add(&Flags::containerizers,
      "containerizers",
      "Comma-separated list of containerizer implementations\n"
      "to compose in order to provide containerization.\n"
      "Available options are `medios` and `docker` (on Linux).\n"
      "The order the containerizers are specified is the order\n"
      "they are tried.\n",
      "medios");

  add(&Flags::docker_config,
      "docker_config",
      "The default docker config file for node. Can be provided either as an\n"
      "absolute path pointing to the node local docker config file, or as a\n"
      "JSON-formatted string. The format of the docker config file should be\n"
      "identical to docker's default one (e.g., either\n"
      "`$HOME/.docker/config.json` or `$HOME/.dockercfg`).\n"
      "Example JSON (`$HOME/.docker/config.json`):\n"
      "{\n"
      "  \"auths\": {\n"
      "    \"https://index.docker.io/v1/\": {\n"
      "      \"auth\": \"xXxXxXxXxXx=\",\n"
      "      \"email\": \"username@example.com\"\n"
      "    }\n"
      "  }\n"
      "}");

  add(&Flags::sandbox_directory,
      "sandbox_directory",
      "The absolute path for the directory in the container where the\n"
      "sandbox is mapped to.\n",
      "/mnt/medios/sandbox"
      );

  add(&Flags::domain_socket_location,
      "domain_socket_location",
      "Location on the host filesystem of the domain socket used for\n"
      "communication with executors.\n Alternatively, this can be set to"
      "'systemd:<identifier>' to use the domain socket with the given\n"
      "identifier, which is expected to be passed by systemd.\n"
      "This flag will be ignored unless the '--http_executor_domain_sockets'\n"
      "flag is also set to true. Total path length must be less than 108\n"
      "characters.\n Will be set to <runtime_dir>/node.sock by default.",
      [](const Option<string>& location) -> Option<Error> {
        if (location.isSome() &&
            location->size() >= common::DOMAIN_SOCKET_MAX_PATH_LENGTH) {
          return Error(
              "Domain socket location cannot be longer than 108 characters.");
        }

        return None();
      });

  add(&Flags::default_container_info,
      "default_container_info",
      "JSON-formatted ContainerInfo that will be included into\n"
      "any ExecutorInfo that does not specify a ContainerInfo.\n"
      "\n"
      "See the ContainerInfo protobuf in medios.proto for\n"
      "the expected format.\n"
      "\n"
      "Example:\n"
      "{\n"
      "  \"type\": \"MEDIOS\",\n"
      "  \"volumes\": [\n"
      "    {\n"
      "      \"host_path\": \".private/tmp\",\n"
      "      \"container_path\": \"/tmp\",\n"
      "      \"mode\": \"RW\"\n"
      "    }\n"
      "  ]\n"
      "}");

  add(&Flags::container_disk_watch_interval,
      "container_disk_watch_interval",
      "The interval between disk quota checks for containers. This flag is\n"
      "used by the `disk/du` isolator.",
      Seconds(15));

  // TODO(jieyu): Consider enabling this flag by default. Remember
  // to update the user doc if we decide to do so.
  add(&Flags::enforce_container_disk_quota,
      "enforce_container_disk_quota",
      "Whether to enable disk quota enforcement for containers. This flag\n"
      "is used by the `disk/du` isolator.",
      false);

  // This help message for --modules flag is the same for
  // {controller,node,sched,tests}/flags.[ch]pp and should always be kept in
  // sync.
  // TODO(karya): Remove the JSON example and add reference to the
  // doc file explaining the --modules flag.
  add(&Flags::modules,
      "modules",
      "List of modules to be loaded and be available to the internal\n"
      "subsystems.\n"
      "\n"
      "Use `--modules=filepath` to specify the list of modules via a\n"
      "file containing a JSON-formatted string. `filepath` can be\n"
      "of the form `file:///path/to/file` or `/path/to/file`.\n"
      "\n"
      "Use `--modules=\"{...}\"` to specify the list of modules inline.\n"
      "\n"
      "Example:\n"
      "{\n"
      "  \"libraries\": [\n"
      "    {\n"
      "      \"file\": \"/path/to/libfoo.so\",\n"
      "      \"modules\": [\n"
      "        {\n"
      "          \"name\": \"org_apache_medios_bar\",\n"
      "          \"parameters\": [\n"
      "            {\n"
      "              \"key\": \"X\",\n"
      "              \"value\": \"Y\"\n"
      "            }\n"
      "          ]\n"
      "        },\n"
      "        {\n"
      "          \"name\": \"org_apache_medios_baz\"\n"
      "        }\n"
      "      ]\n"
      "    },\n"
      "    {\n"
      "      \"name\": \"qux\",\n"
      "      \"modules\": [\n"
      "        {\n"
      "          \"name\": \"org_apache_medios_norf\"\n"
      "        }\n"
      "      ]\n"
      "    }\n"
      "  ]\n"
      "}");

  // This help message for --modules_dir flag is the same for
  // {controller,node,sched,tests}/flags.[ch]pp and should always be kept in
  // sync.
  add(&Flags::modulesDir,
      "modules_dir",
      "Directory path of the module manifest files.\n"
      "The manifest files are processed in alphabetical order.\n"
      "(See --modules for more information on module manifest files).\n"
      "Cannot be used in conjunction with --modules.\n");

  add(&Flags::authenticatee,
      "authenticatee",
      "Authenticatee implementation to use when authenticating against the\n"
      "controller. Use the default `" + string(DEFAULT_AUTHENTICATEE) + "`, or\n"
      "load an alternate authenticatee module using `--modules`.",
      DEFAULT_AUTHENTICATEE);

  add(&Flags::authorizer,
      "authorizer",
      "Authorizer implementation to use when authorizing actions that\n"
      "require it.\n"
      "Use the default `" + string(DEFAULT_AUTHORIZER) + "`, or\n"
      "load an alternate authorizer module using `--modules`.\n"
      "\n"
      "Note that if the `--authorizer` flag is provided with a value\n"
      "other than the default `" + string(DEFAULT_AUTHORIZER) + "`, the\n"
      "ACLs passed through the `--acls` flag will be ignored.",
      DEFAULT_AUTHORIZER);

  add(&Flags::http_authenticators,
      "http_authenticators",
      "HTTP authenticator implementation to use when handling requests to\n"
      "authenticated endpoints. Use the default "
      "`" + string(DEFAULT_BASIC_HTTP_AUTHENTICATOR) + "`, or load an\n"
      "alternate HTTP authenticator module using `--modules`.");

  add(&Flags::authenticate_http_readwrite,
      "authenticate_http_readwrite",
      "If `true`, only authenticated requests for read-write HTTP endpoints\n"
      "supporting authentication are allowed. If `false`, unauthenticated\n"
      "requests to such HTTP endpoints are also allowed.",
      false);

  add(&Flags::authenticate_http_readonly,
      "authenticate_http_readonly",
      "If `true`, only authenticated requests for read-only HTTP endpoints\n"
      "supporting authentication are allowed. If `false`, unauthenticated\n"
      "requests to such HTTP endpoints are also allowed.",
      false);

#ifdef USE_SSL_SOCKET
  add(&Flags::authenticate_http_executors,
      "authenticate_http_executors",
      "If `true`, only authenticated requests for the HTTP executor API are\n"
      "allowed. If `false`, unauthenticated requests are also allowed. This\n"
      "flag is only available when Medios is built with SSL support.",
      false);
#endif // USE_SSL_SOCKET

  add(&Flags::http_executor_domain_sockets,
      "http_executor_domain_sockets",
      "If true, the node will provide a unix domain sockets that the\n"
      "executor can use to connect to the node, instead of relying on\n"
      "a TCP connection.",
      false);

  add(&Flags::http_credentials,
      "http_credentials",
      "Path to a JSON-formatted file containing credentials used to\n"
      "authenticate HTTP endpoints on the node.\n"
      "Path can be of the form `file:///path/to/file` or `/path/to/file`.\n"
      "Example:\n"
      "{\n"
      "  \"credentials\": [\n"
      "    {\n"
      "      \"principal\": \"yoda\",\n"
      "      \"secret\": \"usetheforce\"\n"
      "    }\n"
      "  ]\n"
      "}");

  add(&Flags::hooks,
      "hooks",
      "A comma-separated list of hook modules to be\n"
      "installed inside the node.");

  add(&Flags::secret_resolver,
      "secret_resolver",
      "The name of the secret resolver module to use for resolving\n"
      "environment and file-based secrets. If this flag is not specified,\n"
      "the default behavior is to resolve value-based secrets and error on\n"
      "reference-based secrets.");

  add(&Flags::resource_estimator,
      "resource_estimator",
      "The name of the resource estimator to use for oversubscription.");

  add(&Flags::qos_controller,
      "qos_controller",
      "The name of the QoS Controller to use for oversubscription.");

  add(&Flags::qos_correction_interval_min,
      "qos_correction_interval_min",
      "The node polls and carries out QoS corrections from the QoS\n"
      "Controller based on its observed performance of running tasks.\n"
      "The smallest interval between these corrections is controlled by\n"
      "this flag.",
      Seconds(0));

  add(&Flags::oversubscribed_resources_interval,
      "oversubscribed_resources_interval",
      "The node periodically updates the controller with the current estimation\n"
      "about the total amount of oversubscribed resources that are allocated\n"
      "and available. The interval between updates is controlled by this\n"
      "flag.",
      Seconds(15));

  add(&Flags::controller_detector,
      "controller_detector",
      "The symbol name of the controller detector to use. This symbol\n"
      "should exist in a module specified through the --modules flag.\n"
      "Cannot be used in conjunction with --controller.");

#if ENABLE_SECCOMP_ISOLATOR
  add(&Flags::seccomp_config_dir,
      "seccomp_config_dir",
      "Directory path of the Seccomp profiles.\n"
      "If a container is launched with a specified Seccomp profile name,\n"
      "the `linux/seccomp` isolator will try to locate a Seccomp profile\n"
      "in the specified directory.");

  add(&Flags::seccomp_profile_name,
      "seccomp_profile_name",
      "Path of the default Seccomp profile relative to the"
      "`seccomp_config_dir`.\n"
      "If this flag is specified, the `linux/seccomp` isolator\n"
      "applies the Seccomp profile by default when launching\n"
      "a new Medios container.\n"
      "NOTE: A Seccomp profile must be compatible with the\n"
      "Docker Seccomp profile format (e.g., "
      "https://github.com/moby/moby/blob/master/profiles/seccomp/default.json"
      ").");
#endif

  add(&Flags::http_command_executor,
      "http_command_executor",
      "The underlying executor library to be used for the command executor.\n"
      "If set to `true`, the command executor would use the HTTP based\n"
      "executor library to interact with the Medios node. If set to `false`,\n"
      "the driver based implementation would be used.\n"
      "NOTE: This flag is *experimental* and should not be used in\n"
      "production yet.",
      false);

  add(&Flags::ip,
      "ip",
      "IP address to listen on. This cannot be used in conjunction\n"
      "with `--ip_discovery_command`.");

  add(&Flags::ip6,
      "ip6",
      "IPv6 address to listen on. This cannot be used in conjunction\n"
      "with '--ip6_discovery_command'.\n"
      "\n"
      "NOTE: Currently Medios doesn't listen on IPv6 sockets and hence\n"
      "this IPv6 address is only used to advertise IPv6 addresses for\n"
      "containers running on the host network.\n",
      [](const Option<string>& ip6) -> Option<Error> {
        if (ip6.isSome()) {
          LOG(WARNING) << "Currently Medios doesn't listen on IPv6 sockets"
                       << "and hence the IPv6 address " << ip6.get() << " "
                       << "will only be used to advertise IPv6 addresses"
                       << "for containers running on the host network";
        }

        return None();
      });

  add(&Flags::port, "port", "Port to listen on.", NodeInfo().port());

  add(&Flags::advertise_ip,
      "advertise_ip",
      "IP address advertised to reach this Medios node.\n"
      "The node does not bind to this IP address.\n"
      "However, this IP address may be used to access this node.");

  add(&Flags::advertise_port,
      "advertise_port",
      "Port advertised to reach this Medios node (along with\n"
      "`advertise_ip`). The node does not bind to this port.\n"
      "However, this port (along with `advertise_ip`) may be used to\n"
      "access this node.");

  add(&Flags::controller,
      "controller",
      "May be one of:\n"
      "  `host:port`\n"
      "  `etcd://host:port/<election-name>` (detect the leading\n"
      "  controller through the etcd v3 election service)");

  add(&Flags::ip_discovery_command,
      "ip_discovery_command",
      "Optional IP discovery binary: if set, it is expected to emit\n"
      "the IP address which the node will try to bind to.\n"
      "Cannot be used in conjunction with `--ip`.");

  add(&Flags::ip6_discovery_command,
      "ip6_discovery_command",
      "Optional IPv6 discovery binary: if set, it is expected to emit\n"
      "the IPv6 address on which Medios will try to bind when IPv6 socket\n"
      "support is enabled in Medios.\n"
      "\n"
      "NOTE: Currently Medios doesn't listen on IPv6 sockets and hence\n"
      "this IPv6 address is only used to advertise IPv6 addresses for\n"
      "containers running on the host network.\n");

  // TODO(bevers): Switch the default to `true` after gathering
  // some real-world experience.
  add(&Flags::memory_profiling,
      "memory_profiling",
      "This setting controls whether the memory profiling functionality of\n"
      "libprocess should be exposed when jemalloc is detected.\n"
      "NOTE: Even if set to true, memory profiling will not work unless\n"
      "jemalloc is loaded into the address space of the binary, either by\n"
      "linking against it at compile-time or using `LD_PRELOAD`.",
      false);

  add(&Flags::domain,
      "domain",
      "Domain that the node belongs to. Medios currently only supports\n"
      "fault domains, which identify groups of hosts with similar failure\n"
      "characteristics. A fault domain consists of a region and a zone.\n"
      "If this node is placed in a different region than the controller, it\n"
      "will not appear in resource offers to frameworks that have not\n"
      "enabled the REGION_AWARE capability. This value can be specified\n"
      "as either a JSON-formatted string or a file path containing JSON.\n"
      "\n"
      "Example:\n"
      "{\n"
      "  \"fault_domain\":\n"
      "    {\n"
      "      \"region\":\n"
      "        {\n"
      "          \"name\": \"aws-us-east-1\"\n"
      "        },\n"
      "      \"zone\":\n"
      "        {\n"
      "          \"name\": \"aws-us-east-1a\"\n"
      "        }\n"
      "    }\n"
      "}",
      [](const Option<DomainInfo>& domain) -> Option<Error> {
        if (domain.isSome()) {
          // Don't let the user specify a domain without a fault
          // domain. This is allowed by the protobuf spec (for forward
          // compatibility with possible future changes), but is not a
          // useful configuration right now.
          if (!domain->has_fault_domain()) {
            return Error("`domain` must define `fault_domain`");
          }
        }
        return None();
      });
}
