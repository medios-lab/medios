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

#include <medios/medios.hpp>
#include <medios/module.hpp>

#include <medios/module/isolator.hpp>

#include <medios/node/isolator.hpp>

#include <stout/foreach.hpp>
#include <stout/option.hpp>
#include <stout/os.hpp>
#include <stout/try.hpp>

#include <medios/node/containerizer.hpp>

#include "node/containerizer/medios/isolators/posix.hpp"
#include "node/flags.hpp"

using namespace medios;

using medios::internal::node::Flags;
using medios::internal::node::PosixCpuIsolatorProcess;
using medios::internal::node::PosixMemIsolatorProcess;

using medios::node::Isolator;


// The sole purpose of this function is just to exercise the
// compatibility logic.
static bool compatible()
{
  return true;
}


static Isolator* createCpuIsolator(const Parameters& parameters)
{
  Flags flags;
  Try<Isolator*> result = PosixCpuIsolatorProcess::create(flags);
  if (result.isError()) {
    return nullptr;
  }
  return result.get();
}


static Isolator* createMemIsolator(const Parameters& parameters)
{
  Flags flags;
  Try<Isolator*> result = PosixMemIsolatorProcess::create(flags);
  if (result.isError()) {
    return nullptr;
  }
  return result.get();
}


// Declares a CPU Isolator module named 'org_apache_medios_TestCpuIsolator'.
medios::modules::Module<Isolator> org_apache_medios_TestCpuIsolator(
    MEDIOS_MODULE_API_VERSION,
    MEDIOS_VERSION,
    "Apache Mesos",
    "modules@mesos.apache.org",
    "Test CPU Isolator module.",
    compatible,
    createCpuIsolator);


// Declares a Memory Isolator module named 'org_apache_medios_TestMemIsolator'.
medios::modules::Module<Isolator> org_apache_medios_TestMemIsolator(
    MEDIOS_MODULE_API_VERSION,
    MEDIOS_VERSION,
    "Apache Mesos",
    "modules@mesos.apache.org",
    "Test Memory Isolator module.",
    nullptr, // Do not perform any compatibility check.
    createMemIsolator);


// An isolator that declares device access in its launch info and does
// nothing else. The device path comes from the module parameter `path`,
// default /dev/null, read and write. Exercises the containerizer's
// device-declaration seam without any GPU.
class DeviceDeclaringIsolator : public Isolator
{
public:
  explicit DeviceDeclaringIsolator(const std::string& _path) : path(_path) {}

  process::Future<Option<medios::node::ContainerLaunchInfo>> prepare(
      const ContainerID& containerId,
      const medios::node::ContainerConfig& containerConfig) override
  {
    medios::node::ContainerLaunchInfo launchInfo;
    DeviceAccess* access = launchInfo.add_devices();
    access->mutable_device()->set_path(path);
    access->mutable_access()->set_read(true);
    access->mutable_access()->set_write(true);
    return launchInfo;
  }

private:
  const std::string path;
};


// The path comes from the module parameter `path`, overridden by the
// environment variable MEDIOS_TEST_DEVICE_PATH so a test can pick a device
// after the module library has been loaded with a fixed manifest.
static Isolator* createDeviceIsolator(const Parameters& parameters)
{
  std::string path = "/dev/null";
  foreach (const Parameter& parameter, parameters.parameter()) {
    if (parameter.key() == "path") {
      path = parameter.value();
    }
  }
  Option<std::string> fromEnvironment = os::getenv("MEDIOS_TEST_DEVICE_PATH");
  if (fromEnvironment.isSome()) {
    path = fromEnvironment.get();
  }
  return new DeviceDeclaringIsolator(path);
}


// Declares an Isolator module named 'org_apache_medios_TestDeviceIsolator'.
medios::modules::Module<Isolator> org_apache_medios_TestDeviceIsolator(
    MEDIOS_MODULE_API_VERSION,
    MEDIOS_VERSION,
    "Apache Mesos",
    "modules@mesos.apache.org",
    "Test device-declaring Isolator module.",
    nullptr, // Do not perform any compatibility check.
    createDeviceIsolator);
