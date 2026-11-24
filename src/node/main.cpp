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

#include <stdint.h>

#ifndef __WINDOWS__
#include <sys/file.h>
#endif // __WINDOWS__

#include <set>
#include <vector>
#include <utility>

#include <medios/authorizer/authorizer.hpp>

#include <medios/controller/detector.hpp>

#include <medios/medios.hpp>

#include <medios/module/anonymous.hpp>
#include <medios/module/secret_resolver.hpp>

#include <medios/secret/resolver.hpp>

#include <medios/node/resource_estimator.hpp>

#include <process/network.hpp>
#include <process/owned.hpp>
#include <process/process.hpp>

#include <process/ssl/flags.hpp>

#include <stout/check.hpp>
#include <stout/flags.hpp>
#include <stout/hashset.hpp>
#include <stout/nothing.hpp>
#include <stout/os.hpp>
#include <stout/strings.hpp>

#include <stout/os/permissions.hpp>

#ifdef __linux__
#include <linux/systemd.hpp>

#include <stout/proc.hpp>
#endif // __linux__

#include <stout/stringify.hpp>
#include <stout/try.hpp>
#include <stout/version.hpp>

#ifdef USE_SSL_SOCKET
#include "authentication/executor/jwt_secret_generator.hpp"
#endif // USE_SSL_SOCKET

#include "common/authorization.hpp"
#include "common/build.hpp"

#include "common/domain_sockets.hpp"

#include "common/http.hpp"

#include "hook/manager.hpp"

#ifdef __linux__
#include "linux/cgroups.hpp"
#include "linux/systemd.hpp"

#include "linux/cgroups2.hpp"
#include "node/containerizer/medios/isolators/cgroups2/cgroups2.hpp"

#endif // __linux__

#include "logging/logging.hpp"

#include "messages/flags.hpp"
#include "messages/messages.hpp"

#include "module/manager.hpp"

#include "node/containerizer/medios/paths.hpp"
#include "node/constants.hpp"
#include "node/gc.hpp"
#include "node/node.hpp"
#include "node/task_status_update_manager.hpp"

#include "version/version.hpp"

using namespace medios::internal;
using namespace medios::internal::node;

using medios::SecretGenerator;

#ifdef USE_SSL_SOCKET
using medios::authentication::executor::JWTSecretGenerator;
#endif // USE_SSL_SOCKET

using medios::controller::detector::ControllerDetector;

using medios::modules::Anonymous;
using medios::modules::ModuleManager;

using medios::node::QoSController;
using medios::node::ResourceEstimator;

using medios::Authorizer;
using medios::SecretResolver;
using medios::NodeInfo;

using net::IP;

using process::Owned;

using process::firewall::DisabledEndpointsFirewallRule;
using process::firewall::FirewallRule;

using process::network::unix::Socket;

using std::cerr;
using std::cout;
using std::endl;
using std::move;
using std::set;
using std::string;
using std::vector;


#ifdef ENABLE_JEMALLOC_ALLOCATOR
// Reduce the number of arenas jemalloc uses. On multi-core systems
// jemalloc creates multiple allocation arenas to reduce lock
// contention between threads using a single arena. The default high
// number of arenas (nproc*4) causes high RSS for the node use case.
// The new number was selected pretty much arbitrarily. It helped
// reduce the node's RSS (better than 8, same as 2) and existing
// benchmarks didn't reveal any performance hits caused by it.
const char* malloc_conf = "narenas:4";
#endif


#ifdef __linux__

// Log any processes inside of a cgroup.
static Try<Nothing> logProcesses(const string& cgroup)
{
  Try<set<pid_t>> processes = cgroups2::processes(cgroup);
  if (processes.isError()) {
    return Error("Failed to check for existing processes"
                 " in cgroup '" + cgroup + "': " + processes.error());
  }

  if (!processes->empty()) {
    vector<string> infos;
    foreach (pid_t pid, *processes) {
      Result<os::Process> proc = os::process(pid);

      // Print the command if it's available.
      if (proc.isSome()) {
        infos.push_back(stringify(pid) + " '" + proc->command + "'");
      } else {
        infos.push_back(stringify(pid));
      }
    }

    LOG(INFO) << "Found process(es) in the cgroup '" << cgroup << "'. "
              << "Consider checking the following process(es) listed in "
              << path::join("/sys/fs/cgroup", cgroup, "cgroup.procs")
              << ":\n" << strings::join("\n", infos);
  }

  return Nothing();
}


// Initialize Medios cgroups for cgroups v2.
//
//
// Ensures that cgroups v2 is available, correctly mounted, and all of the
// requested controllers are available. If correctly setup, the requested
// controllers are enabled in the root cgroup, and the Medios Node is moved
// into its own cgroup.
//
// Any processes found in the cgroups that are created are logged. This helps
// in debugging, in case the processes are from a previous run of Medios that
// wasn't correctly cleaned up.
//
// Creates cgroups:
// /<root>              Top-level cgroup for the Medios node. Has all of the
//                      requested controllers enabled.
// /<root>/node/leaf   Cgroup for the Medios node. The `/leaf` suffix is not
//                      strictly necessary but is consistent with keeping all
//                      processes inside of `/leaf` folders.
static Try<Nothing> initializeCgroups2(const node::Flags& flags)
{
  namespace containerizer = medios::internal::node::containerizer;
  CHECK_SOME(flags.node_subsystems);

  if (!cgroups2::enabled()) {
    return Error("cgroups v2 is not available on this system");
  }

  Try<bool> mounted = cgroups2::mounted();
  if (mounted.isError()) {
    return Error("Failed to check if cgroups v2 is mounted: "
                 + mounted.error());
  }
  if (!*mounted) {
    return Error("The cgroup2 file system is not mounted at '/sys/fs/cgroup'");
  }

  // Refuse to configure the cgroups2 isolator when the hierarchy is
  // mounted read-only (typical inside unprivileged containers and
  // hardened runtimes). Without this gate, the node starts cleanly and
  // then `Cgroups2IsolatorProcess::prepare` aborts the process at the
  // first task launch (see medios#247). Fail loudly here at startup so
  // operators can choose to run with --isolation=none, or on a host
  // that satisfies the isolator's substrate requirement.
  if (!cgroups2::writable()) {
    return Error(
        "The cgroup2 file system at '/sys/fs/cgroup' is not writable "
        "in this process's context. The cgroups2 isolator requires a "
        "writable cgroups2 hierarchy. Run with --isolation=none, or "
        "on a host with writable /sys/fs/cgroup (privileged container, "
        "or a runtime that does not strip cgroup mutation).");
  }

  if (!cgroups2::exists(flags.cgroups_root)) {
    Try<Nothing> create = cgroups2::create(flags.cgroups_root);
    if (create.isError()) {
      return Error("Failed to create cgroup '" + flags.cgroups_root + "': "
                   + create.error());
    }
  }

  const string& node =
    containerizer::paths::cgroups2::node(flags.cgroups_root);
  if (!cgroups2::exists(node)) {
    Try<Nothing> create = cgroups2::create(node);
    if (create.isError()) {
      return Error("Failed to create cgroup '" + node + "': "
                   + create.error());
    }
  }

  const string nodeLeaf =
    containerizer::paths::cgroups2::node(flags.cgroups_root, true);
  if (!cgroups2::exists(nodeLeaf)) {
    Try<Nothing> create = cgroups2::create(nodeLeaf);
    if (create.isError()) {
      return Error("Failed to create cgroup '" + nodeLeaf + "': "
                   + create.error());
    }
  }

  Try<Nothing> processes = logProcesses(flags.cgroups_root);
  if (processes.isError()) { return Error(processes.error()); }

  processes = logProcesses(node);
  if (processes.isError()) { return Error(processes.error()); }

  processes = logProcesses(nodeLeaf);
  if (processes.isError()) { return Error(processes.error()); }

  // `cgroups2::ROOT_CGROUP` is the default cgroup all processes belong to when
  // the cgroup2 hierarchy is mounted. `flags.cgroups_root`, conversely, is the
  // root cgroup for Medios. Here we make all the requested controllers
  // available to the Medios root cgroup by enabling them in the
  // `cgroups2::ROOT_CGROUP`, its parent.
  Try<set<string>> availableControllers = cgroups2::controllers::available(
      cgroups2::ROOT_CGROUP);
  if (availableControllers.isError()) {
    return Error("Failed to determine all available controllers: "
                 + availableControllers.error());
  }

  const vector<string> requestedControllers = strings::tokenize(
      *flags.node_subsystems, ",");

  const set<string> requestedControllersSet(
      requestedControllers.begin(), requestedControllers.end());


  Try<Nothing> enable = cgroups2::controllers::enable(
      cgroups2::ROOT_CGROUP, requestedControllersSet);
  if (enable.isError()) {
    return Error("Failed to enable the requested cgroup v2 controllers: "
                 + enable.error());
  }

  // Move the node process into its own cgroup.
  Try<Nothing> assign = cgroups2::assign(nodeLeaf, getpid());
  if (assign.isError()) {
    return Error("Failed to move the Medios Node into"
                 " cgroup '" + nodeLeaf + "': " + assign.error());
  }

  return Nothing();
}


// Move the node into its own cgroup for each of the specified
// subsystems.
//
// NOTE: Any subsystem configuration is inherited from the medios
// root cgroup for that subsystem, e.g., by default the memory
// cgroup will be unlimited.
//
// TODO(jieyu): Make sure the corresponding cgroup isolator is
// enabled so that the container processes are moved to different
// cgroups than the node cgroup.
static Try<Nothing> initializeCgroups(const node::Flags& flags)
{
  CHECK_SOME(flags.node_subsystems);

  foreach (const string& subsystem,
           strings::tokenize(flags.node_subsystems.get(), ",")) {
    LOG(INFO) << "Moving node process into its own cgroup for"
              << " subsystem: " << subsystem;

    // Ensure the subsystem is mounted and the Medios root cgroup is
    // present.
    Try<string> hierarchy = cgroups::prepare(
        flags.cgroups_hierarchy,
        subsystem,
        flags.cgroups_root);

    if (hierarchy.isError()) {
      return Error(
          "Failed to prepare cgroup " + flags.cgroups_root +
          " for subsystem " + subsystem +
          ": " + hierarchy.error());
    }

    // Create a cgroup for the node.
    string cgroup = path::join(flags.cgroups_root, "node");

    if (!cgroups::exists(hierarchy.get(), cgroup)) {
      Try<Nothing> create = cgroups::create(hierarchy.get(), cgroup);
      if (create.isError()) {
        return Error(
            "Failed to create cgroup " + cgroup +
            " for subsystem " + subsystem +
            " under hierarchy " + hierarchy.get() +
            " for node: " + create.error());
      }
    }

    // Exit if there are processes running inside the cgroup - this
    // indicates a prior node (or child process) is still running.
    Try<set<pid_t>> processes = cgroups::processes(hierarchy.get(), cgroup);
    if (processes.isError()) {
      return Error(
          "Failed to check for existing threads in cgroup " + cgroup +
          " for subsystem " + subsystem +
          " under hierarchy " + hierarchy.get() +
          " for node: " + processes.error());
    }

    // Log if there are any processes in the node's cgroup. They
    // may be transient helper processes like 'perf' or 'du',
    // ancillary log processes or possibly a stuck
    // node.
    // TODO(idownes): Generally, it's not a problem if there are
    // processes running in the node's cgroup, though any resources
    // consumed by those processes are accounted to the node. Where
    // applicable, transient processes should be configured to
    // terminate if the node exits; see example usage for perf in
    // isolators/cgroups/perf.cpp. Consider moving ancillary
    // processes to a different cgroup, e.g., moving log streaming to
    // the container's cgroup.
    if (!processes->empty()) {
      // For each process, we print its pid as well as its command
      // to help triaging.
      vector<string> infos;
      foreach (pid_t pid, processes.get()) {
        Result<os::Process> proc = os::process(pid);

        // Only print the command if available.
        if (proc.isSome()) {
          infos.push_back(stringify(pid) + " '" + proc->command + "'");
        } else {
          infos.push_back(stringify(pid));
        }
      }

      LOG(INFO) << "A node (or child process) is still running, please"
                << " consider checking the following process(es) listed in "
                << path::join(hierarchy.get(), cgroup, "cgroup.procs")
                << ":\n" << strings::join("\n", infos);
    }

    // Move all of our threads into the cgroup.
    Try<Nothing> assign = cgroups::assign(hierarchy.get(), cgroup, getpid());
    if (assign.isError()) {
      return Error(
          "Failed to move node into cgroup " + cgroup +
          " for subsystem " + subsystem +
          " under hierarchy " + hierarchy.get() +
          " for node: " + assign.error());
    }
  }

  return Nothing();
}
#endif // __linux__


int main(int argc, char** argv)
{
  // The order of initialization is as follows:
  // * Validate flags.
  // * Logging
  // * Log build information.
  // * Libprocess
  // * Version process
  // * Firewall rules: should be initialized before initializing HTTP endpoints.
  // * Modules: Load module libraries and manifests before they
  //   can be instantiated.
  // * Anonymous modules: Later components such as Allocators, and controller
  //   contender/detector might depend upon anonymous modules.
  // * Hooks.
  // * Systemd support (if it exists).
  // * SecretResolver and Containerizer.
  // * Controller detector.
  // * Authorizer.
  // * Garbage collector.
  // * Task status update manager.
  // * Resource estimator.
  // * QoS controller.
  // * `Node` process.
  //
  // TODO(avinash): Add more comments discussing the rationale behind for this
  // particular component ordering.

  GOOGLE_PROTOBUF_VERIFY_VERSION;

  node::Flags flags;

  Try<flags::Warnings> load = flags.load("MEDIOS_", argc, argv);

  if (flags.help) {
    cout << flags.usage() << endl;
    return EXIT_SUCCESS;
  }

  if (flags.version) {
    cout << "medios" << " " << MEDIOS_VERSION << endl;
    return EXIT_SUCCESS;
  }

  if (load.isError()) {
    cerr << load.error() << "\n\n"
         << "See `medios-node --help` for a list of supported flags." << endl;
    return EXIT_FAILURE;
  }

  logging::initialize(argv[0], true, flags); // Catch signals.

  // Log any flag warnings (after logging is initialized).
  foreach (const flags::Warning& warning, load->warnings) {
    LOG(WARNING) << warning.message;
  }

  // Check that node's version has the expected format (SemVer).
  {
    Try<Version> version = Version::parse(MEDIOS_VERSION);
    if (version.isError()) {
      EXIT(EXIT_FAILURE)
        << "Failed to parse Medios version '" << MEDIOS_VERSION << "': "
        << version.error();
    }
  }

  if (flags.controller.isNone() && flags.controller_detector.isNone()) {
    EXIT(EXIT_FAILURE)
      << "Missing required option `--controller` or `--controller_detector`";
  }

  if (flags.controller.isSome() && flags.controller_detector.isSome()) {
    EXIT(EXIT_FAILURE)
      << "Only one of `--controller` or `--controller_detector` should be specified";
  }

  // Initialize libprocess.
  if (flags.ip_discovery_command.isSome() && flags.ip.isSome()) {
    EXIT(EXIT_FAILURE)
      << "Only one of `--ip` or `--ip_discovery_command` should be specified";
  }

  if (flags.ip6_discovery_command.isSome() && flags.ip6.isSome()) {
    EXIT(EXIT_FAILURE)
      << "Only one of `--ip6` or `--ip6_discovery_command` should be specified";
  }

  if (flags.ip_discovery_command.isSome()) {
    Try<string> ipAddress = os::shell(flags.ip_discovery_command.get());

    if (ipAddress.isError()) {
      EXIT(EXIT_FAILURE) << ipAddress.error();
    }

    os::setenv("LIBPROCESS_IP", strings::trim(ipAddress.get()));
  } else if (flags.ip.isSome()) {
    os::setenv("LIBPROCESS_IP", flags.ip.get());
  }

  if (flags.ip6_discovery_command.isSome()) {
    Try<string> ip6Address = os::shell(flags.ip6_discovery_command.get());
    if (ip6Address.isError()) {
      EXIT(EXIT_FAILURE) << ip6Address.error();
    }

    os::setenv("LIBPROCESS_IP6", strings::trim(ip6Address.get()));
  } else if (flags.ip6.isSome()) {
    os::setenv("LIBPROCESS_IP6", flags.ip6.get());
  }

  os::setenv("LIBPROCESS_PORT", stringify(flags.port));

  if (flags.advertise_ip.isSome()) {
    os::setenv("LIBPROCESS_ADVERTISE_IP", flags.advertise_ip.get());
  }

  if (flags.advertise_port.isSome()) {
    os::setenv("LIBPROCESS_ADVERTISE_PORT", flags.advertise_port.get());
  }

  if (flags.http_executor_domain_sockets) {
    if (flags.domain_socket_location.isNone()) {
      flags.domain_socket_location =
        flags.runtime_dir + "/" + NODE_EXECUTORS_SOCKET_FILENAME;
    }

    if (flags.domain_socket_location->size() >=
        common::DOMAIN_SOCKET_MAX_PATH_LENGTH) {
      EXIT(EXIT_FAILURE)
        << "Domain socket location '" << *flags.domain_socket_location << "'"
        << " must have less than 108 characters.";
    }
  }

  os::setenv("LIBPROCESS_MEMORY_PROFILING", stringify(flags.memory_profiling));

  // Log build information.
  LOG(INFO) << "Build: " << build::DATE << " by " << build::USER;
  LOG(INFO) << "Version: " << MEDIOS_VERSION;

  if (build::GIT_TAG.isSome()) {
    LOG(INFO) << "Git tag: " << build::GIT_TAG.get();
  }

  if (build::GIT_SHA.isSome()) {
    LOG(INFO) << "Git SHA: " << build::GIT_SHA.get();
  }

#ifdef __linux__
  if (flags.node_subsystems.isSome()) {
    // Use the cgroups v2 isolator if it is supported. Otherwise, use
    // the cgroups v1 isolator.
    [&flags] () {
      Try<bool> mounted = cgroups2::mounted();
      if (mounted.isError()) {
        EXIT(EXIT_FAILURE) << mounted.error();
      }

      // To use cgroups v2, the host must have a cgroup2 filesystem mounted
      // at `/sys/fs/cgroup`.
      if (*mounted) {
        Try<Nothing> initialize = initializeCgroups2(flags);
        if (initialize.isError()) {
          EXIT(EXIT_FAILURE) << initialize.error();
        }
        return;
      }

      // Initialize a cgroups hierarchy for each of the controllers that
      // are requested, create the root Medios Node's cgroup, and move the
      // node processes into the new cgroup.
      Try<Nothing> initialize = initializeCgroups(flags);
      if (initialize.isError()) {
        EXIT(EXIT_FAILURE) << initialize.error();
      }
    }();
  }
#endif // __linux__

  const string id = process::ID::generate("node"); // Process ID.

  // If `process::initialize()` returns `false`, then it was called before this
  // invocation, meaning the authentication realm for libprocess-level HTTP
  // endpoints was set incorrectly. This should be the first invocation.
  if (!process::initialize(
          id,
          READWRITE_HTTP_AUTHENTICATION_REALM,
          READONLY_HTTP_AUTHENTICATION_REALM)) {
    EXIT(EXIT_FAILURE) << "The call to `process::initialize()` in the node's "
                       << "`main()` was not the function's first invocation";
  }

  spawn(new VersionProcess(), true);

  if (flags.firewall_rules.isSome()) {
    vector<Owned<FirewallRule>> rules;

    const Firewall firewall = flags.firewall_rules.get();

    if (firewall.has_disabled_endpoints()) {
      hashset<string> paths;

      foreach (const string& path, firewall.disabled_endpoints().paths()) {
        paths.insert(path);
      }

      rules.emplace_back(new DisabledEndpointsFirewallRule(paths));
    }

    process::firewall::install(std::move(rules));
  }

  // Initialize modules.
  if (flags.modules.isSome() && flags.modulesDir.isSome()) {
    EXIT(EXIT_FAILURE) <<
      "Only one of --modules or --modules_dir should be specified";
  }

  if (flags.modulesDir.isSome()) {
    Try<Nothing> result = ModuleManager::load(flags.modulesDir.get());
    if (result.isError()) {
      EXIT(EXIT_FAILURE) << "Error loading modules: " << result.error();
    }
  }

  if (flags.modules.isSome()) {
    Try<Nothing> result = ModuleManager::load(flags.modules.get());
    if (result.isError()) {
      EXIT(EXIT_FAILURE) << "Error loading modules: " << result.error();
    }
  }

  // Create anonymous modules.
  foreach (const string& name, ModuleManager::find<Anonymous>()) {
    Try<Anonymous*> create = ModuleManager::create<Anonymous>(name);
    if (create.isError()) {
      EXIT(EXIT_FAILURE)
        << "Failed to create anonymous module named '" << name << "'";
    }

    // We don't bother keeping around the pointer to this anonymous
    // module, when we exit that will effectively free its memory.
    //
    // TODO(benh): We might want to add explicit finalization (and
    // maybe explicit initialization too) in order to let the module
    // do any housekeeping necessary when the node is cleanly
    // terminating.
  }

  // Initialize hooks.
  if (flags.hooks.isSome()) {
    Try<Nothing> result = HookManager::initialize(flags.hooks.get());
    if (result.isError()) {
      EXIT(EXIT_FAILURE) << "Error installing hooks: " << result.error();
    }
  }

#ifdef __linux__
  // Initialize systemd if it exists.
  if (flags.systemd_enable_support && systemd::exists()) {
    LOG(INFO) << "Initializing systemd state";

    systemd::Flags systemdFlags;
    systemdFlags.enabled = flags.systemd_enable_support;
    systemdFlags.runtime_directory = flags.systemd_runtime_directory;
    systemdFlags.cgroups_hierarchy = flags.cgroups_hierarchy;

    Try<Nothing> initialize = systemd::initialize(systemdFlags);
    if (initialize.isError()) {
      EXIT(EXIT_FAILURE)
        << "Failed to initialize systemd: " + initialize.error();
    }
  }
#endif // __linux__

  GarbageCollector* gc = new GarbageCollector(flags.work_dir);

  // Initialize SecretResolver.
  Try<SecretResolver*> secretResolver =
    medios::SecretResolver::create(flags.secret_resolver);

  if (secretResolver.isError()) {
    EXIT(EXIT_FAILURE)
        << "Failed to initialize secret resolver: " << secretResolver.error();
  }

  VolumeGidManager* volumeGidManager = nullptr;

  if (flags.volume_gid_range.isSome()) {
    Try<VolumeGidManager*> _volumeGidManager = VolumeGidManager::create(flags);
    if (_volumeGidManager.isError()) {
      EXIT(EXIT_FAILURE) << "Failed to initialize volume gid manager: "
                         << _volumeGidManager.error();
    }

    volumeGidManager = _volumeGidManager.get();
  }

  // Initialize PendingFutureTracker.
  Try<PendingFutureTracker*> futureTracker = PendingFutureTracker::create();
  if (futureTracker.isError()) {
    EXIT(EXIT_FAILURE) << "Failed to initialize pending future tracker: "
                       << futureTracker.error();
  }

  SecretGenerator* secretGenerator = nullptr;

#ifdef USE_SSL_SOCKET
  if (flags.jwt_secret_key.isSome()) {
    Try<string> jwtSecretKey = os::read(flags.jwt_secret_key.get());
    if (jwtSecretKey.isError()) {
      EXIT(EXIT_FAILURE) << "Failed to read the file specified by "
                         << "--jwt_secret_key";
    }

    // TODO(greggomann): Factor the following code out into a common helper,
    // since we also do this when loading credentials.
    Try<os::Permissions> permissions =
      os::permissions(flags.jwt_secret_key.get());
    if (permissions.isError()) {
      LOG(WARNING) << "Failed to stat jwt secret key file '"
                   << flags.jwt_secret_key.get()
                   << "': " << permissions.error();
    } else if (permissions->others.rwx) {
      LOG(WARNING) << "Permissions on executor secret key file '"
                   << flags.jwt_secret_key.get()
                   << "' are too open; it is recommended that your"
                   << " key file is NOT accessible by others";
    }

    secretGenerator = new JWTSecretGenerator(jwtSecretKey.get());
  }
#endif // USE_SSL_SOCKET

#ifndef __WINDOWS__
  // Acquire a non-blocking exclusive lock on `<runtime_dir>/node.lock` before
  // creating the containerizer or running orphan recovery (#645). If a second
  // `medios-node` shares the same `--runtime_dir`, fail immediately rather
  // than enumerating and destroying the first node's live containers as
  // orphans. `runtimeLockFd` stays open for the lifetime of `main()` and uses
  // `O_CLOEXEC` so child executors do not inherit the descriptor.
  Try<Nothing> runtimeDir = os::mkdir(flags.runtime_dir);
  if (runtimeDir.isError()) {
    EXIT(EXIT_FAILURE)
      << "Failed to create runtime_dir '" << flags.runtime_dir << "': "
      << runtimeDir.error();
  }

  const string runtimeLockPath = path::join(flags.runtime_dir, "node.lock");
  Try<int_fd> runtimeLockFd = os::open(
      runtimeLockPath,
      O_CREAT | O_RDWR | O_CLOEXEC,
      S_IRUSR | S_IWUSR);
  if (runtimeLockFd.isError()) {
    EXIT(EXIT_FAILURE)
      << "Failed to open runtime_dir lock file '" << runtimeLockPath << "': "
      << runtimeLockFd.error();
  }

  if (::flock(runtimeLockFd.get(), LOCK_EX | LOCK_NB) != 0) {
    if (errno == EWOULDBLOCK || errno == EAGAIN) {
      EXIT(EXIT_FAILURE)
        << "Another medios-node holds the lock on runtime_dir '"
        << flags.runtime_dir << "' (" << runtimeLockPath << ")";
    }
    EXIT(EXIT_FAILURE)
      << "Failed to lock runtime_dir lock file '" << runtimeLockPath << "': "
      << os::strerror(errno);
  }
#endif // __WINDOWS__

  Try<Containerizer*> containerizer = Containerizer::create(
      flags,
      false,
      gc,
      secretResolver.get(),
      volumeGidManager,
      futureTracker.get());

  if (containerizer.isError()) {
    EXIT(EXIT_FAILURE)
      << "Failed to create a containerizer: " << containerizer.error();
  }

  Try<ControllerDetector*> detector_ = ControllerDetector::create(
      flags.controller.isSome() ? flags.controller->value : Option<string>::none(),
      flags.controller_detector);

  if (detector_.isError()) {
    EXIT(EXIT_FAILURE)
      << "Failed to create a controller detector: " << detector_.error();
  }

  ControllerDetector* detector = detector_.get();

  Option<Authorizer*> authorizer_ = None();

  string authorizerName = flags.authorizer;

  Result<Authorizer*> authorizer((None()));
  if (authorizerName != node::DEFAULT_AUTHORIZER) {
    LOG(INFO) << "Creating '" << authorizerName << "' authorizer";

    // NOTE: The contents of --acls will be ignored.
    authorizer = Authorizer::create(authorizerName);
  } else {
    // `authorizerName` is `DEFAULT_AUTHORIZER` at this point.
    if (flags.acls.isSome()) {
      LOG(INFO) << "Creating default '" << authorizerName << "' authorizer";

      authorizer = Authorizer::create(flags.acls.get());
    }
  }

  if (authorizer.isError()) {
    EXIT(EXIT_FAILURE) << "Could not create '" << authorizerName
                       << "' authorizer: " << authorizer.error();
  } else if (authorizer.isSome()) {
    authorizer_ = authorizer.get();

    // Set the authorization callbacks for libprocess HTTP endpoints.
    // Note that these callbacks capture `authorizer_.get()`, but the node
    // creates a copy of the authorizer during construction. Thus, if in the
    // future it becomes possible to dynamically set the authorizer, this would
    // break.
    process::http::authorization::setCallbacks(
        medios::authorization::createAuthorizationCallbacks(authorizer_.get()));
  }

  Files* files = new Files(READONLY_HTTP_AUTHENTICATION_REALM, authorizer_);
  TaskStatusUpdateManager* taskStatusUpdateManager =
    new TaskStatusUpdateManager(flags);

  Try<ResourceEstimator*> resourceEstimator =
    ResourceEstimator::create(flags.resource_estimator);

  if (resourceEstimator.isError()) {
    EXIT(EXIT_FAILURE) << "Failed to create resource estimator: "
                       << resourceEstimator.error();
  }

  Try<QoSController*> qosController =
    QoSController::create(flags.qos_controller);

  if (qosController.isError()) {
    EXIT(EXIT_FAILURE) << "Failed to create QoS Controller: "
                       << qosController.error();
  }

  // Create executor domain socket if the user so desires.
  Option<Socket> executorSocket = None();
  if (flags.http_executor_domain_sockets) {
    // If `http_executor_domain_sockets` is true, then the location should have
    // been set by the user or automatically during startup.
    CHECK_SOME(flags.domain_socket_location);

    if (strings::startsWith(*flags.domain_socket_location, "systemd:")) {
      LOG(INFO) << "Expecting domain socket to be passed by systemd";

      // Chop off `systemd:` prefix.
      std::string name = flags.domain_socket_location->substr(8);
#ifdef __linux__
      // NOTE: Some systemd versions do not support FileDescriptorName,
      // thus we also need to listen on descriptors with name "unknown",
      // which is used for sockets where no name could be determined.
      Try<std::vector<int>> socketFds =
        systemd::socket_activation::listenFdsWithNames({name, "unknown"});
#else
      Try<std::vector<int>> socketFds =
        Try<std::vector<int>>({}); // Dummy to avoid compile errors.
      EXIT(EXIT_FAILURE)
        << "Systemd socket passing is only supported on linux.";
#endif

      if (socketFds.isError()) {
        EXIT(EXIT_FAILURE)
          << "Could not get passed file descriptors from systemd: "
          << socketFds.error();
      }

      if (socketFds->size() != 1u) {
        EXIT(EXIT_FAILURE)
          << "Expected exactly one socket with name " << name
          << ", got " << socketFds->size() << " instead.";
      }

      int sockfd = socketFds->at(0);

      // Don't use SSLSocketImpl for unix domain sockets.
      Try<Socket> socket = Socket::create(
          sockfd, process::network::internal::SocketImpl::Kind::POLL);

      if (socket.isError()) {
        EXIT(EXIT_FAILURE)
          << "Failed to create domain socket: " << socket.error();
      }

      executorSocket = socket.get();

      // Adjust socket location to point to the *path*, not the systemd
      // identifier.
      auto addr = process::network::convert<process::network::unix::Address>(
          process::network::address(sockfd).get()).get();

      flags.domain_socket_location = addr.path();
    } else {
      const int mode = flags.switch_user
        ? common::DOMAIN_SOCKET_SWITCH_USER_MODE
        : common::DOMAIN_SOCKET_DEFAULT_MODE;
      Try<Socket> socket =
        common::createDomainSocket(*flags.domain_socket_location, mode);

      if (socket.isError()) {
        EXIT(EXIT_FAILURE)
          << "Failed to create domain socket: " << socket.error();
      }

      executorSocket = socket.get();
    }

    LOG(INFO) << "Using domain socket at " << *flags.domain_socket_location;
  }

  Node* node = new Node(
      id,
      flags,
      detector,
      containerizer.get(),
      files,
      gc,
      taskStatusUpdateManager,
      resourceEstimator.get(),
      qosController.get(),
      secretGenerator,
      volumeGidManager,
      futureTracker.get(),
      executorSocket,
      authorizer_);

  process::spawn(node);
  process::wait(node->self());

  delete node;

  delete secretGenerator;

  delete qosController.get();

  delete resourceEstimator.get();

  delete taskStatusUpdateManager;

  delete files;

  if (authorizer_.isSome()) {
    delete authorizer_.get();
  }

  delete detector;

  delete containerizer.get();

  delete futureTracker.get();

  delete volumeGidManager;

  delete secretResolver.get();

  delete gc;

  // also clean up the networking stack before exiting.
  process::finalize(true);
  return EXIT_SUCCESS;
}
