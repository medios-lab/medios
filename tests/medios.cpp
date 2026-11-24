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

#include <memory>
#include <set>
#include <string>

#include <medios/authorizer/authorizer.hpp>

#include <medios/controller/detector.hpp>

#include <process/clock.hpp>

#include <stout/check.hpp>
#include <stout/foreach.hpp>
#include <stout/json.hpp>
#include <stout/os.hpp>
#include <stout/path.hpp>
#include <stout/result.hpp>
#include <stout/stringify.hpp>
#include <stout/uuid.hpp>

#include "common/http.hpp"

#ifdef __linux__
#include "linux/cgroups2.hpp"
#endif

#include "node/constants.hpp"
#include "node/containerizer/containerizer.hpp"

#include "node/containerizer/medios/containerizer.hpp"

#include "tests/containerizer.hpp"
#include "tests/environment.hpp"
#include "tests/flags.hpp"
#include "tests/medios.hpp"

using medios::controller::detector::ControllerDetector;

using std::list;
using std::set;
using std::shared_ptr;
using std::string;
using std::vector;

using testing::_;

using namespace process;

namespace medios {
namespace internal {
namespace tests {


void MediosTest::SetUpTestCase()
{
  // We set the connection delay used by the scheduler library to 0.
  // This is done to speed up the tests.
  os::setenv("MEDIOS_CONNECTION_DELAY_MAX", "0ms");
}


void MediosTest::TearDownTestCase()
{
  os::unsetenv("MEDIOS_CONNECTION_DELAY_MAX");

  SSLTemporaryDirectoryTest::TearDownTestCase();
}


MediosTest::MediosTest() {}


controller::Flags MediosTest::CreateControllerFlags()
{
  controller::Flags flags;

  // We use the current working directory from TempDirectoryTest
  // to ensure the work directory remains the same within a test.
  flags.work_dir = path::join(os::getcwd(), "controller");

  CHECK_SOME(os::mkdir(flags.work_dir.get()));

  flags.authenticate_http_readonly = true;
  flags.authenticate_http_readwrite = true;
  flags.authenticate_frameworks = true;
  flags.authenticate_nodes = true;

  flags.authenticate_http_frameworks = true;
  flags.http_framework_authenticators = "basic";

  // Create a default credentials file.
  const string& path = path::join(os::getcwd(), "credentials");

  Try<int_fd> fd = os::open(
      path,
      O_WRONLY | O_CREAT | O_TRUNC | O_CLOEXEC,
      S_IRUSR | S_IWUSR | S_IRGRP);

  CHECK_SOME(fd);

  // JSON default format for credentials.
  Credentials credentials;

  Credential* credential = credentials.add_credentials();
  credential->set_principal(DEFAULT_CREDENTIAL.principal());
  credential->set_secret(DEFAULT_CREDENTIAL.secret());

  credential = credentials.add_credentials();
  credential->set_principal(DEFAULT_CREDENTIAL_2.principal());
  credential->set_secret(DEFAULT_CREDENTIAL_2.secret());

  CHECK_SOME(os::write(fd.get(), stringify(JSON::protobuf(credentials))))
    << "Failed to write credentials to '" << path << "'";
  CHECK_SOME(os::close(fd.get()));

  flags.credentials = path;

  // Set default ACLs.
  flags.acls = ACLs();

  flags.registry = "in_memory";

  // On many test VMs, this default is too small.
  flags.registry_store_timeout = flags.registry_store_timeout * 5;

  flags.authenticators = tests::flags.authenticators;

  return flags;
}


node::Flags MediosTest::CreateNodeFlags()
{
  node::Flags flags;

  // Create a temporary work directory (removed by Environment).
  Try<string> workDir = environment->mkdtemp();
  CHECK_SOME(workDir) << "Failed to create temporary directory";
  flags.work_dir = workDir.get();

  // Create a temporary runtime directory (removed by Environment).
  Try<string> runtimeDir = environment->mkdtemp();
  CHECK_SOME(runtimeDir) << "Failed to create temporary directory";
  flags.runtime_dir = runtimeDir.get();

  Try<string> nodeDir = os::mkdtemp(path::join(sandbox.get(), "XXXXXX"));
  CHECK_SOME(nodeDir) << "Failed to create temporary directory";

  flags.launcher_dir = getLauncherDir();

  flags.docker_store_dir = path::join(nodeDir.get(), "store", "docker");

  {
    // Create a default credential file for controller/node authentication.
    const string& path = path::join(nodeDir.get(), "credential");

    Try<int_fd> fd = os::open(
        path,
        O_WRONLY | O_CREAT | O_TRUNC | O_CLOEXEC,
        S_IRUSR | S_IWUSR | S_IRGRP);

    CHECK_SOME(fd);

    Credential credential;
    credential.set_principal(DEFAULT_CREDENTIAL.principal());
    credential.set_secret(DEFAULT_CREDENTIAL.secret());

    CHECK_SOME(os::write(fd.get(), stringify(JSON::protobuf(credential))))
      << "Failed to write node credential to '" << path << "'";

    CHECK_SOME(os::close(fd.get()));

    flags.credential = path;

    // Set default (permissive) ACLs.
    flags.acls = ACLs();
  }

  flags.authenticate_http_readonly = true;
  flags.authenticate_http_readwrite = true;

#ifdef USE_SSL_SOCKET
  // Executor authentication currently has SSL as a dependency, so we
  // cannot enable it if Medios was not built with SSL support.
  flags.authenticate_http_executors = true;

  {
    // Create a secret key for executor authentication.
    const string path = path::join(nodeDir.get(), "jwt_secret_key");

    Try<int_fd> fd = os::open(
        path,
        O_WRONLY | O_CREAT | O_TRUNC | O_CLOEXEC,
        S_IRUSR | S_IWUSR | S_IRGRP);

    CHECK_SOME(fd);

    CHECK_SOME(os::write(fd.get(), DEFAULT_JWT_SECRET_KEY))
      << "Failed to write executor secret key to '" << path << "'";

    CHECK_SOME(os::close(fd.get()));

    flags.jwt_secret_key = path;
  }
#else // USE_SSL_SOCKET
  // Disable operator API authentication for the default executor. Executor
  // authentication currently has SSL as a dependency, so we cannot require
  // executors to authenticate with the node operator API if Medios was not
  // built with SSL support.
  flags.authenticate_http_readwrite = false;
#endif // USE_SSL_SOCKET

  {
    // Create a default HTTP credentials file.
    const string& path = path::join(nodeDir.get(), "http_credentials");

    Try<int_fd> fd = os::open(
        path,
        O_WRONLY | O_CREAT | O_TRUNC | O_CLOEXEC,
        S_IRUSR | S_IWUSR | S_IRGRP);

    CHECK_SOME(fd);

    Credentials httpCredentials;

    Credential* httpCredential = httpCredentials.add_credentials();
    httpCredential->set_principal(DEFAULT_CREDENTIAL.principal());
    httpCredential->set_secret(DEFAULT_CREDENTIAL.secret());

    httpCredential = httpCredentials.add_credentials();
    httpCredential->set_principal(DEFAULT_CREDENTIAL_2.principal());
    httpCredential->set_secret(DEFAULT_CREDENTIAL_2.secret());

    CHECK_SOME(os::write(fd.get(), stringify(JSON::protobuf(httpCredentials))))
      << "Failed to write HTTP credentials to '" << path << "'";

    CHECK_SOME(os::close(fd.get()));

    flags.http_credentials = path;
  }

  flags.resources = defaultNodeResourcesString;

  flags.registration_backoff_factor = Milliseconds(10);

  if (tests::flags.isolation.isSome()) {
    flags.isolation = tests::flags.isolation.get();
  }

  return flags;
}


Try<Owned<cluster::Controller>> MediosTest::StartController(
    const Option<controller::Flags>& flags)
{
  return cluster::Controller::start(
      flags.isNone() ? CreateControllerFlags() : flags.get());
}


Try<Owned<cluster::Controller>> MediosTest::StartController(
    medios::allocator::Allocator* allocator,
    const Option<controller::Flags>& flags)
{
  return cluster::Controller::start(
      flags.isNone() ? CreateControllerFlags() : flags.get(),
      allocator);
}


Try<Owned<cluster::Controller>> MediosTest::StartController(
    Authorizer* authorizer,
    const Option<controller::Flags>& flags)
{
  return cluster::Controller::start(
      flags.isNone() ? CreateControllerFlags() : flags.get(),
      None(),
      authorizer);
}


Try<Owned<cluster::Controller>> MediosTest::StartController(
    const shared_ptr<MockRateLimiter>& nodeRemovalLimiter,
    const Option<controller::Flags>& flags)
{
  return cluster::Controller::start(
      flags.isNone() ? CreateControllerFlags() : flags.get(),
      None(),
      None(),
      nodeRemovalLimiter);
}


Try<Owned<cluster::Node>> MediosTest::StartNode(const NodeOptions& options)
{
  Try<Owned<cluster::Node>> node = cluster::Node::create(
      options.detector,
      options.flags.isNone() ? CreateNodeFlags() : options.flags.get(),
      options.id,
      options.containerizer,
      options.gc,
      options.taskStatusUpdateManager,
      options.resourceEstimator,
      options.qosController,
      options.secretGenerator,
      options.authorizer,
      options.futureTracker,
      options.mock);

  if (node.isSome() && !options.mock) {
    node.get()->start();
  }

  return node;
}


Try<Owned<cluster::Node>> MediosTest::StartNode(
    ControllerDetector* detector,
    const Option<node::Flags>& flags,
    bool mock)
{
  return StartNode(NodeOptions(detector, mock)
    .withFlags(flags));
}


Try<Owned<cluster::Node>> MediosTest::StartNode(
    ControllerDetector* detector,
    node::Containerizer* containerizer,
    const Option<node::Flags>& flags,
    bool mock)
{
  return StartNode(NodeOptions(detector, mock)
    .withFlags(flags)
    .withContainerizer(containerizer));
}


Try<Owned<cluster::Node>> MediosTest::StartNode(
    ControllerDetector* detector,
    const string& id,
    const Option<node::Flags>& flags,
    bool mock)
{
  return StartNode(NodeOptions(detector, mock)
    .withFlags(flags)
    .withId(id));
}


Try<Owned<cluster::Node>> MediosTest::StartNode(
    ControllerDetector* detector,
    node::Containerizer* containerizer,
    const string& id,
    const Option<node::Flags>& flags)
{
  return StartNode(NodeOptions(detector)
    .withFlags(flags)
    .withId(id)
    .withContainerizer(containerizer));
}


Try<Owned<cluster::Node>> MediosTest::StartNode(
    ControllerDetector* detector,
    node::GarbageCollector* gc,
    const Option<node::Flags>& flags,
    bool mock)
{
  return StartNode(NodeOptions(detector, mock)
    .withFlags(flags)
    .withGc(gc));
}


Try<Owned<cluster::Node>> MediosTest::StartNode(
    ControllerDetector* detector,
    medios::node::ResourceEstimator* resourceEstimator,
    const Option<node::Flags>& flags)
{
  return StartNode(NodeOptions(detector)
    .withFlags(flags)
    .withResourceEstimator(resourceEstimator));
}


Try<Owned<cluster::Node>> MediosTest::StartNode(
    ControllerDetector* detector,
    node::Containerizer* containerizer,
    medios::node::ResourceEstimator* resourceEstimator,
    const Option<node::Flags>& flags)
{
  return StartNode(NodeOptions(detector)
    .withFlags(flags)
    .withContainerizer(containerizer)
    .withResourceEstimator(resourceEstimator));
}


Try<Owned<cluster::Node>> MediosTest::StartNode(
    ControllerDetector* detector,
    medios::node::QoSController* qosController,
    const Option<node::Flags>& flags)
{
  return StartNode(NodeOptions(detector)
    .withFlags(flags)
    .withQosController(qosController));
}


Try<Owned<cluster::Node>> MediosTest::StartNode(
    ControllerDetector* detector,
    node::Containerizer* containerizer,
    medios::node::QoSController* qosController,
    const Option<node::Flags>& flags,
    bool mock)
{
  return StartNode(NodeOptions(detector, mock)
    .withFlags(flags)
    .withContainerizer(containerizer)
    .withQosController(qosController));
}


Try<Owned<cluster::Node>> MediosTest::StartNode(
    medios::controller::detector::ControllerDetector* detector,
    medios::Authorizer* authorizer,
    const Option<node::Flags>& flags,
    bool mock)
{
  return StartNode(NodeOptions(detector, mock)
    .withFlags(flags)
    .withAuthorizer(authorizer));
}


Try<Owned<cluster::Node>> MediosTest::StartNode(
    medios::controller::detector::ControllerDetector* detector,
    node::Containerizer* containerizer,
    medios::Authorizer* authorizer,
    const Option<node::Flags>& flags,
    bool mock)
{
  return StartNode(NodeOptions(detector, mock)
    .withFlags(flags)
    .withContainerizer(containerizer)
    .withAuthorizer(authorizer));
}


Try<Owned<cluster::Node>> MediosTest::StartNode(
    medios::controller::detector::ControllerDetector* detector,
    node::Containerizer* containerizer,
    medios::SecretGenerator* secretGenerator,
    const Option<medios::Authorizer*>& authorizer,
    const Option<node::Flags>& flags,
    bool mock)
{
  return StartNode(NodeOptions(detector, mock)
    .withFlags(flags)
    .withContainerizer(containerizer)
    .withSecretGenerator(secretGenerator)
    .withAuthorizer(authorizer));
}


Try<Owned<cluster::Node>> MediosTest::StartNode(
    medios::controller::detector::ControllerDetector* detector,
    medios::SecretGenerator* secretGenerator,
    const Option<node::Flags>& flags)
{
  return StartNode(NodeOptions(detector)
    .withFlags(flags)
    .withSecretGenerator(secretGenerator));
}


// Although the constructors and destructors for mock classes are
// often trivial, defining them out-of-line (in a separate compilation
// unit) improves compilation time: see MESOS-3827.

MockScheduler::MockScheduler() {}


MockScheduler::~MockScheduler() {}


MockExecutor::MockExecutor(const ExecutorID& _id) : id(_id) {}


MockExecutor::~MockExecutor() {}


MockAuthorizer::MockAuthorizer()
{
  // NOTE: We use 'EXPECT_CALL' and 'WillRepeatedly' here instead of
  // 'ON_CALL' and 'WillByDefault'. See 'TestContainerizer::SetUp()'
  // for more details.
  EXPECT_CALL(*this, authorized(_))
    .WillRepeatedly(Return(true));

  EXPECT_CALL(*this, getApprover(_, _))
    .WillRepeatedly(Return(std::make_shared<AcceptingObjectApprover>()));
}


MockAuthorizer::~MockAuthorizer() {}


MockGarbageCollector::MockGarbageCollector(const string& workDir)
    : node::GarbageCollector(workDir)
{
  EXPECT_CALL(*this, unschedule(_)).WillRepeatedly(Return(true));
}


MockGarbageCollector::~MockGarbageCollector() {}


node::Flags ContainerizerTest<node::MediosContainerizer>::CreateNodeFlags()
{
  node::Flags flags = MediosTest::CreateNodeFlags();

  // If the user has specified isolation on command-line, we better
  // use it.
  if (tests::flags.isolation.isSome()) {
    flags.isolation = tests::flags.isolation.get();
    return flags;
  }

#ifdef __linux__
  // Use cgroups v2 isolators if available and we're root (DR-027 / #116).
  // cgroups v1 plumbing is stripped per the same DR; only the v2 path remains.
  Result<string> user = os::user();
  if (cgroupsV2() && user.isSome() && *user == "root") {
    flags.isolation = "cgroups/cpu,cgroups/mem";
    flags.cgroups_hierarchy = "/sys/fs/cgroup";
    flags.cgroups_root = TEST_CGROUPS_ROOT;
  } else {
    flags.isolation = "posix/cpu,posix/mem";
  }
#else
  flags.isolation = "posix/cpu,posix/mem";
#endif

  return flags;
}


#ifdef __linux__
void ContainerizerTest<node::MediosContainerizer>::SetUpTestCase()
{
  MediosTest::SetUpTestCase();

  Result<string> user = os::user();
  EXPECT_SOME(user);

  if (cgroupsV2() && user.isSome() && *user == "root") {
    // Clean up any leftover test cgroups from a previous run.
    if (cgroups2::exists(TEST_CGROUPS_ROOT)) {
      AWAIT_ASSERT_READY(cgroups2::destroy(TEST_CGROUPS_ROOT));
    }
  }
}


void ContainerizerTest<node::MediosContainerizer>::TearDownTestCase()
{
  MediosTest::TearDownTestCase();

  Result<string> user = os::user();
  EXPECT_SOME(user);

  if (cgroupsV2() && user.isSome() && *user == "root") {
    if (cgroups2::exists(TEST_CGROUPS_ROOT)) {
      AWAIT_ASSERT_READY(cgroups2::destroy(TEST_CGROUPS_ROOT));
    }
  }
}


void ContainerizerTest<node::MediosContainerizer>::SetUp()
{
  MediosTest::SetUp();

  Result<string> user = os::user();
  EXPECT_SOME(user);

  // The earlier (kernel-supports-cgroupsv2 + we're root) check passes
  // inside unprivileged Docker containers and hardened runtimes that
  // mount /sys/fs/cgroup read-only. cgroups2::writable() probes the
  // filesystem with a small mkdir/rmdir to distinguish "the kernel
  // advertises cgroupsv2" from "we can actually mutate it here." When
  // we can't, skip the fixture cleanly so the ~100 NodeTests that don't
  // genuinely require cgroups can still run. (See medios#246.)
  if (cgroupsV2() && user.isSome() && *user == "root" && cgroups2::writable()) {
    SetUpCgroupsV2();
  }
}


void ContainerizerTest<node::MediosContainerizer>::TearDown()
{
  MediosTest::TearDown();

  Result<string> user = os::user();
  EXPECT_SOME(user);

  if (cgroupsV2() && user.isSome() && *user == "root" && cgroups2::writable()) {
    TearDownCgroupsV2();
  }
}


void ContainerizerTest<node::MediosContainerizer>::SetUpCgroups() {}


void ContainerizerTest<node::MediosContainerizer>::SetUpCgroupsV2()
{
  // The node binary initializes cgroups inside `node/main.cpp`. This
  // setup is done in place of that initialization for the test fixture.
  if (!cgroups2::exists(TEST_CGROUPS_ROOT)) {
    ASSERT_SOME(cgroups2::create(TEST_CGROUPS_ROOT));
  }

  Try<set<string>> controllers = cgroups2::controllers::available(
      cgroups2::ROOT_CGROUP);
  ASSERT_SOME(controllers);
  subsystems = *controllers;

  // Enable all controllers inside the test root cgroup so they are
  // accessible from child container cgroups.
  ASSERT_TRUE(cgroups2::exists(TEST_CGROUPS_ROOT));
  ASSERT_SOME(
      cgroups2::controllers::enable(cgroups2::ROOT_CGROUP, *controllers));
  ASSERT_SOME(cgroups2::controllers::enable(TEST_CGROUPS_ROOT, *controllers));
}


void ContainerizerTest<node::MediosContainerizer>::TearDownCgroups() {}


void ContainerizerTest<node::MediosContainerizer>::TearDownCgroupsV2()
{
  // cgroups2::destroy() retries rmdir on a libprocess timer. A test that
  // pauses the clock leaves it paused here, because the gtest listener
  // resumes it only after TearDown(). The destroy would then never finish
  // and would remove the next test's TEST_CGROUPS_ROOT once the clock
  // resumes.
  if (Clock::paused()) {
    Clock::resume();
  }

  AWAIT_ASSERT_READY(cgroups2::destroy(TEST_CGROUPS_ROOT));
}


bool ContainerizerTest<node::MediosContainerizer>::cgroupsV2()
{
  Try<bool> mounted = cgroups2::mounted();
  if (mounted.isError()) {
    return false;
  }
  return *mounted;
}
#endif // __linux__


string ParamDiskQuota::Printer::operator()(
  const ::testing::TestParamInfo<ParamDiskQuota::Type>& info) const
{
  switch (info.param) {
    case SANDBOX:
      return "Sandbox";
    case ROOTFS:
      return "Rootfs";
    default:
      UNREACHABLE();
  }
}


vector<ParamDiskQuota::Type> ParamDiskQuota::parameters()
{
  return vector<Type>{SANDBOX};
}

} // namespace tests {
} // namespace internal {
} // namespace medios {
