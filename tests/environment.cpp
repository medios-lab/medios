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

#include <string.h>
#include <unistd.h>

#include <list>
#include <set>
#include <string>
#include <vector>

#include <gtest/gtest.h>

#include <process/gmock.hpp>
#include <process/gtest.hpp>
#include <process/owned.hpp>

#include <stout/check.hpp>
#include <stout/error.hpp>
#include <stout/exit.hpp>
#include <stout/foreach.hpp>
#include <stout/none.hpp>
#include <stout/option.hpp>
#include <stout/os.hpp>
#include <stout/path.hpp>
#include <stout/result.hpp>
#include <stout/stringify.hpp>
#include <stout/strings.hpp>

#include <stout/os/exists.hpp>
#include <stout/os/pstree.hpp>
#include <stout/os/shell.hpp>
#include <stout/os/temp.hpp>
#include <stout/os/which.hpp>

#ifdef __linux__
#include "linux/cgroups2.hpp"
#endif

#include "logging/logging.hpp"

#include "tests/environment.hpp"
#include "tests/flags.hpp"
#include "tests/utils.hpp"

using std::list;
using std::set;
using std::string;
using std::vector;

using process::Owned;

using stout::internal::tests::TestFilter;


namespace medios {
namespace internal {
namespace tests {

// Storage for the global environment instance.
Environment* environment;


class BenchmarkFilter : public TestFilter
{
public:
  bool disable(const ::testing::TestInfo* test) const override
  {
    return matches(test, "BENCHMARK_") && !flags.benchmark;
  }
};


// CFS bandwidth control is a Linux kernel feature. With Linux probes
// stripped, the predicate is preserved but always reports the feature
// as unavailable so any reintroduced CFS_ tests are disabled.
class CfsFilter : public TestFilter
{
public:
  bool disable(const ::testing::TestInfo* test) const override
  {
    return matches(test, "CFS_");
  }
};


// CGROUPS_/Cgroups tests are disabled on platforms without cgroups, and on
// Linux hosts where the cgroups2 hierarchy isn't writable from this process
// (unprivileged containers, hardened runtimes). Per DR-027 v1 plumbing is
// stripped; v2 isolators back the cgroups/cpu, cgroups/mem flag values the
// tests use, so writability of /sys/fs/cgroup is the precondition. The
// writability probe runs once per process via the static local.
class CgroupsFilter : public TestFilter
{
public:
  bool disable(const ::testing::TestInfo* test) const override
  {
    const bool matches_cgroups_v1 =
        matches(test, "CGROUPS_") ||
        (matches(test, "Cgroups") && !matches(test, "Cgroups2"));
    if (!matches_cgroups_v1) {
      return false;
    }
#ifdef __linux__
    static const bool writable = cgroups2::writable();
    return !writable;
#else
    return true;
#endif
  }
};


class Cgroups2Filter : public TestFilter
{
public:
  bool disable(const ::testing::TestInfo* test) const override
  {
    const bool matches_cgroups_v2 =
        matches(test, "CGROUPS2_") || matches(test, "Cgroups2");
    if (!matches_cgroups_v2) {
      return false;
    }
#ifdef __linux__
    static const bool writable = cgroups2::writable();
    return !writable;
#else
    return true;
#endif
  }
};


class CurlFilter : public TestFilter
{
public:
  CurlFilter()
  {
    curlError = os::which("curl").isNone();
    if (curlError) {
      std::cerr
        << "-------------------------------------------------------------\n"
        << "No 'curl' command found so no 'curl' tests will be run\n"
        << "-------------------------------------------------------------"
        << std::endl;
    }
  }

  bool disable(const ::testing::TestInfo* test) const override
  {
    return matches(test, "CURL_") && curlError;
  }

private:
  bool curlError;
};


// NVML support is compile-time-conditional on ENABLE_NVML which is not
// defined in this build. Predicate kept; always disables NVIDIA_GPU_ tests.
class NvidiaGpuFilter : public TestFilter
{
public:
  bool disable(const ::testing::TestInfo* test) const override
  {
    return matches(test, "NVIDIA_GPU_");
  }
};


// DOCKER_* tests require a working `docker` CLI with a reachable daemon
// (Docker Desktop, Colima, OrbStack, Linux dockerd, etc.; backend choice
// is intentionally open per #144). We probe `docker info` once at filter
// construction. If the daemon is not reachable, all DOCKER_* tests
// auto-skip with a one-line banner, like the other environment gates.
class DockerFilter : public TestFilter
{
public:
  DockerFilter()
  {
    int status = std::system("docker info >/dev/null 2>&1");
    dockerAvailable = (status == 0);
    if (!dockerAvailable) {
      std::cerr
        << "-------------------------------------------------------------\n"
        << "No reachable Docker daemon — DOCKER_* tests will not run.\n"
        << "Install a Docker backend (e.g. `brew install colima &&"
        << " colima start`)\nand rerun to exercise them.\n"
        << "-------------------------------------------------------------\n";
    }
  }

  bool disable(const ::testing::TestInfo* test) const override
  {
    return matches(test, "DOCKER_") && !dockerAvailable;
  }

private:
  bool dockerAvailable = false;
};


// `d_type` probing is Linux-only. Predicate kept; always reports the
// attribute as unavailable.
class DtypeFilter : public TestFilter
{
public:
  bool disable(const ::testing::TestInfo* test) const override
  {
    return matches(test, "DTYPE_");
  }
};


class InternetFilter : public TestFilter
{
public:
  InternetFilter()
  {
    error = os::system("ping -c 1 -W 1 google.com") != 0;
    if (error) {
      std::cerr
        << "-------------------------------------------------------------\n"
        << "We cannot run any INTERNET tests because no internet access\n"
        << "-------------------------------------------------------------"
        << std::endl;
    }
  }

  bool disable(const ::testing::TestInfo* test) const override
  {
    return matches(test, "INTERNET_") && error;
  }

private:
  bool error;
};


// iptables is Linux-only. Predicate kept; always disables IPTABLES_ tests.
class IPTablesFilter : public TestFilter
{
public:
  bool disable(const ::testing::TestInfo* test) const override
  {
    return matches(test, "IPTABLES_");
  }
};


class NetcatFilter : public TestFilter
{
public:
  NetcatFilter()
  {
    netcatError = os::which("nc").isNone();
    if (netcatError) {
      std::cerr
        << "-------------------------------------------------------------\n"
        << "No 'nc' command found so no tests depending on 'nc' will run\n"
        << "-------------------------------------------------------------"
        << std::endl;
    }
  }

  bool disable(const ::testing::TestInfo* test) const override
  {
    return matches(test, "NC_") && netcatError;
  }

private:
  bool netcatError;
};


// net_cls is a Linux cgroup subsystem. Predicate kept; always disables.
class NetClsCgroupsFilter : public TestFilter
{
public:
  bool disable(const ::testing::TestInfo* test) const override
  {
    return matches(test, "NET_CLS_");
  }
};


// PortMapping isolator is ENABLE_PORT_MAPPING_ISOLATOR-guarded (Linux).
// Predicate kept; always disables.
class NetworkIsolatorTestFilter : public TestFilter
{
public:
  bool disable(const ::testing::TestInfo* test) const override
  {
    return matches(test, "PortMappingIsolatorTest") ||
           matches(test, "PortMappingMediosTest");
  }
};


// Supported-filesystem probes are Linux-only (require linux/fs.hpp).
// Predicate base kept; always reports unsupported.
class SupportedFilesystemTestFilter : public TestFilter
{
public:
  explicit SupportedFilesystemTestFilter(const string& fsname)
  {
    fsSupportError = Error(fsname + " is only supported on Linux");
  }

  Option<Error> fsSupportError;
};


class AufsFilter : public SupportedFilesystemTestFilter
{
public:
  AufsFilter() : SupportedFilesystemTestFilter("aufs") {}

  bool disable(const ::testing::TestInfo* test) const override
  {
    return fsSupportError.isSome() && matches(test, "AUFS_");
  }
};


class OverlayFSFilter : public SupportedFilesystemTestFilter
{
public:
  OverlayFSFilter() : SupportedFilesystemTestFilter("overlayfs") {}

  bool disable(const ::testing::TestInfo* test) const override
  {
    return fsSupportError.isSome() && matches(test, "OVERLAYFS_");
  }
};


class XfsFilter : public SupportedFilesystemTestFilter
{
public:
  XfsFilter() : SupportedFilesystemTestFilter("xfs") {}

  bool disable(const ::testing::TestInfo* test) const override
  {
    return fsSupportError.isSome() && matches(test, "XFS_");
  }
};


// `perf` is Linux-only. Predicate kept; always disables.
class PerfCPUCyclesFilter : public TestFilter
{
public:
  bool disable(const ::testing::TestInfo* test) const override
  {
    return matches(test, "ROOT_CGROUPS_PERF_PerfTest") ||
           matches(test, "ROOT_CGROUPS_PERF_UserCgroup") ||
           matches(test, "ROOT_CGROUPS_PERF_RollForward") ||
           matches(test, "ROOT_CGROUPS_PERF_Sample");
  }
};


class PerfFilter : public TestFilter
{
public:
  bool disable(const ::testing::TestInfo* test) const override
  {
    return matches(test, "PERF_");
  }
};


class RootFilter : public TestFilter
{
public:
  bool disable(const ::testing::TestInfo* test) const override
  {
    Result<string> user = os::user();
    CHECK_SOME(user);

    return matches(test, "ROOT_") && user.get() != "root";
  }
};


class SeccompFilter : public TestFilter
{
public:
  SeccompFilter()
  {
    // Seccomp parser depends on CPU architecture; tests are deterministic
    // only on x86_64.
    Try<string> arch = os::shell("arch");
    if (arch.isError()) {
      seccompError = arch.error();
    } else if (strings::trim(arch.get()) != "x86_64") {
      seccompError = Error("Seccomp tests cannot be run on non-x86_64 systems");
    }

    if (seccompError.isSome()) {
      std::cerr
        << "-------------------------------------------------------------\n"
        << "We can't run any SECCOMP tests:\n"
        << seccompError.get() << "\n"
        << "-------------------------------------------------------------"
        << std::endl;
    }
  }

  bool disable(const ::testing::TestInfo* test) const override
  {
    return matches(test, "SECCOMP_") && seccompError.isSome();
  }

private:
  Option<Error> seccompError;
};


class UnprivilegedUserFilter : public TestFilter
{
public:
  UnprivilegedUserFilter()
  {
    Option<string> user = os::getenv("SUDO_USER");
    if (user.isNone() || user.get() == "root") {
      unprivilegedUserFound = false;
    } else {
      unprivilegedUserFound = true;
    }

    if (!unprivilegedUserFound) {
      std::cerr
        << "-------------------------------------------------------------\n"
        << "No usable unprivileged user found from the 'SUDO_USER'\n"
        << "environment variable. So tests that rely on an unprivileged\n"
        << "user will not run\n"
        << "-------------------------------------------------------------"
        << std::endl;
    }
  }

  bool disable(const ::testing::TestInfo* test) const override
  {
    return matches(test, "UNPRIVILEGED_USER_") && !unprivilegedUserFound;
  }

private:
  bool unprivilegedUserFound;
};


class UnzipFilter : public TestFilter
{
public:
  UnzipFilter()
  {
    unzipError = os::which("unzip").isNone();
    if (unzipError) {
      std::cerr
        << "-------------------------------------------------------------\n"
        << "No 'unzip' command found so no 'unzip' tests will be run\n"
        << "-------------------------------------------------------------"
        << std::endl;
    }
  }

  bool disable(const ::testing::TestInfo* test) const override
  {
    return matches(test, "UNZIP_") && unzipError;
  }

private:
  bool unzipError;
};


// veth CNI plugin is Linux-only (iproute2 + network namespaces). Predicate
// kept; always disables.
class VEthFilter : public TestFilter
{
public:
  bool disable(const ::testing::TestInfo* test) const override
  {
    return matches(test, "VETH_");
  }
};


Environment::Environment(const Flags& _flags)
  : stout::internal::tests::Environment(
        std::vector<std::shared_ptr<TestFilter>>{
            std::make_shared<AufsFilter>(),
            std::make_shared<BenchmarkFilter>(),
            std::make_shared<CfsFilter>(),
            std::make_shared<CgroupsFilter>(),
            std::make_shared<Cgroups2Filter>(),
            std::make_shared<CurlFilter>(),
            std::make_shared<DockerFilter>(),
            std::make_shared<DtypeFilter>(),
            std::make_shared<InternetFilter>(),
            std::make_shared<IPTablesFilter>(),
            std::make_shared<NetcatFilter>(),
            std::make_shared<NetClsCgroupsFilter>(),
            std::make_shared<NetworkIsolatorTestFilter>(),
            std::make_shared<NvidiaGpuFilter>(),
            std::make_shared<OverlayFSFilter>(),
            std::make_shared<PerfCPUCyclesFilter>(),
            std::make_shared<PerfFilter>(),
            std::make_shared<RootFilter>(),
            std::make_shared<SeccompFilter>(),
            std::make_shared<UnprivilegedUserFilter>(),
            std::make_shared<UnzipFilter>(),
            std::make_shared<VEthFilter>(),
            std::make_shared<XfsFilter>()}),
    flags(_flags)
{
  // Add our test event listeners.
  ::testing::TestEventListeners& listeners =
    ::testing::UnitTest::GetInstance()->listeners();

  listeners.Append(process::FilterTestEventListener::instance());
  listeners.Append(process::ClockTestEventListener::instance());

  // Add the temporary directory event listener which will clean up
  // any directories created after each test finishes.
  temporaryDirectoryEventListener = new TemporaryDirectoryEventListener();
  listeners.Append(temporaryDirectoryEventListener);
}


void Environment::SetUp()
{
  // Clear any MEDIOS_ environment variables so they don't affect our tests.
  foreachkey (const string& key, os::environment()) {
    if (key.find("MEDIOS_") == 0) {
      os::unsetenv(key);
    }
  }

  // Set the path to the native JNI library for running JVM tests.
  // TODO(tillt): Adapt library towards JNI specific name once libmedios
  // has been split.
  if (os::getenv("MEDIOS_NATIVE_JAVA_LIBRARY").isNone()) {
    string path = getLibMediosPath();
    os::setenv("MEDIOS_NATIVE_JAVA_LIBRARY", path);
  }

#if !GTEST_IS_THREADSAFE
  EXIT(EXIT_FAILURE) << "Testing environment is not thread safe, bailing!";
#endif // !GTEST_IS_THREADSAFE
}


void Environment::TearDown()
{
  // Make sure we haven't left any child processes lying around.
  // TODO(benh): Look for processes in the same group or session that
  // might have been reparented.
  // TODO(jmlvanre): Consider doing this `OnTestEnd` in a listener so
  // that we can identify leaked processes more precisely.
  Try<os::ProcessTree> pstree = os::pstree(0);

  if (pstree.isSome() && !pstree->children.empty()) {
    FAIL() << "Tests completed with child processes remaining:\n"
           << pstree.get();
  }
}


Try<string> Environment::mkdtemp()
{
  return temporaryDirectoryEventListener->mkdtemp();
}


void tests::Environment::TemporaryDirectoryEventListener::OnTestEnd(
    const testing::TestInfo&)
{
  foreach (const string& directory, directories) {
    Try<Nothing> rmdir = os::rmdir(directory);
    if (rmdir.isError()) {
      LOG(ERROR) << "Failed to remove '" << directory
                 << "': " << rmdir.error();
    }
  }

  directories.clear();
}


Try<string> Environment::TemporaryDirectoryEventListener::mkdtemp()
{
  const ::testing::TestInfo* const testInfo =
    ::testing::UnitTest::GetInstance()->current_test_info();

  if (testInfo == nullptr) {
    return Error("Failed to determine the current test information");
  }

  // We replace any slashes present in the test names (e.g. TYPED_TEST),
  // to make sure the temporary directory resides under '/tmp/'.
  const string& testCase =
    strings::replace(testInfo->test_case_name(), "/", "_");

  string testName = strings::replace(testInfo->name(), "/", "_");

  // Adjust the test name to remove any 'DISABLED_' prefix (to make
  // things easier to read). While this might seem alarming, if we are
  // "running" a disabled test it must be the case that the test was
  // explicitly enabled (e.g., via 'gtest_filter').
  if (strings::startsWith(testName, "DISABLED_")) {
    testName = strings::remove(testName, "DISABLED_", strings::PREFIX);
  }

  const string tmpdir = os::temp();

  const string& path =
    path::join(tmpdir, strings::join("_", testCase, testName, "XXXXXX"));

  Try<string> mkdtemp = os::mkdtemp(path);
  if (mkdtemp.isSome()) {
    directories.push_back(mkdtemp.get());
  }
  return mkdtemp;
}

} // namespace tests {
} // namespace internal {
} // namespace medios {
