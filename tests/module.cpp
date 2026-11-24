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

#include <string>

#include <stout/hashmap.hpp>

#include "messages/messages.hpp"
#include "module/manager.hpp"

#include "tests/flags.hpp"
#include "tests/module.hpp"

using std::string;

using namespace medios::modules;

namespace medios {
namespace internal {
namespace tests {

static hashmap<ModuleID, string> moduleNames;


static void addModule(
    Modules::Library* library,
    ModuleID moduleId,
    string moduleName)
{
  moduleNames[moduleId] = moduleName;
  Modules::Library::Module* module = library->add_modules();
  module->set_name(moduleNames[moduleId]);
}


// Add available Isolator modules.
static void addIsolatorModules(Modules* modules)
{
  CHECK_NOTNULL(modules);

  // Now add our test CPU and Memory isolator modules.
  Modules::Library* library = modules->add_libraries();
  library->set_file(getModulePath("testisolator"));

  // To add a new module from this library, create a new ModuleID enum
  // and tie it with a module name.
  addModule(library, TestCpuIsolator, "org_apache_medios_TestCpuIsolator");
  addModule(library, TestMemIsolator, "org_apache_medios_TestMemIsolator");
  addModule(
      library, TestDeviceIsolator, "org_apache_medios_TestDeviceIsolator");
}


// Add available ContainerLogger modules.
static void addContainerLoggerModules(Modules* modules)
{
  CHECK_NOTNULL(modules);

  // Add our test container logger module.
  Modules::Library* library = modules->add_libraries();
  library->set_file(getModulePath("testcontainer_logger"));

  // To add a new module from this library, create a new ModuleID enum
  // and tie it with a module name.
  addModule(library,
            TestSandboxContainerLogger,
            "org_apache_medios_TestSandboxContainerLogger");
}


static void addHookModules(Modules* modules)
{
  CHECK_NOTNULL(modules);

  // Now add our test hook module.
  Modules::Library* library = modules->add_libraries();
  library->set_file(getModulePath("testhook"));

  // To add a new module from this library, create a new ModuleID enum
  // and tie it with a module name.
  addModule(library, TestHook, "org_apache_medios_TestHook");
}


static void addAnonymousModules(Modules* modules)
{
  CHECK_NOTNULL(modules);

  // Now add our test anonymous module.
  Modules::Library* library = modules->add_libraries();
  library->set_file(getModulePath("testanonymous"));

  // To add a new module from this library, create a new ModuleID enum
  // and tie it with a module name.
  addModule(
      library, TestAnonymous, "org_apache_medios_TestAnonymous");
}


// Add available Allocator modules.
static void addAllocatorModules(Modules* modules)
{
  CHECK_NOTNULL(modules);

  // Now add our allocator module.
  Modules::Library* library = modules->add_libraries();
  library->set_file(getModulePath("testallocator"));

  // To add a new module from this library, create a new ModuleID enum
  // and tie it with a module name.
  addModule(library, TestDRFAllocator, "org_apache_medios_TestDRFAllocator");
}


// Add available ResourceEstimator modules.
static void addResourceEstimatorModules(Modules* modules)
{
  CHECK_NOTNULL(modules);

  // Now add our resource_estimator module.
  Modules::Library* library = modules->add_libraries();
  library->set_file(getModulePath("testresource_estimator"));

  // To add a new module from this library, create a new ModuleID enum
  // and tie it with a module name.
  addModule(
      library,
      TestNoopResourceEstimator,
      "org_apache_medios_TestNoopResourceEstimator");
}


static void addAuthorizerModules(Modules* modules)
{
  CHECK_NOTNULL(modules);

  // Now add our test authorizer module.
  Modules::Library* library = modules->add_libraries();
  library->set_file(getModulePath("testauthorizer"));

  // To add a new module from this library, create a new ModuleID enum
  // and tie it with a module name.
  addModule(
      library, TestLocalAuthorizer, "org_apache_medios_TestLocalAuthorizer");
}


static void addHttpAuthenticatorModules(Modules* modules)
{
  CHECK_NOTNULL(modules);

  // Now add our test HTTP authenticator module.
  Modules::Library* library = modules->add_libraries();
  library->set_file(getModulePath("testhttpauthenticator"));

  // To add a new module from this library, create a new ModuleID enum
  // and tie it with a module name.
  addModule(
      library,
      TestHttpBasicAuthenticator,
      "org_apache_medios_TestHttpBasicAuthenticator");
}


static void addControllerContenderModules(Modules* modules)
{
  CHECK_NOTNULL(modules);

  // Now add our test anonymous module.
  Modules::Library* library = modules->add_libraries();
  library->set_file(getModulePath("testcontrollercontender"));

  // To add a new module from this library, create a new ModuleID enum
  // and tie it with a module name.
  addModule(
      library,
      TestControllerContender,
      "org_apache_medios_TestControllerContender");
}


static void addControllerDetectorModules(Modules* modules)
{
  CHECK_NOTNULL(modules);

  // Now add our test anonymous module.
  Modules::Library* library = modules->add_libraries();
  library->set_file(getModulePath("testcontrollerdetector"));

  // To add a new module from this library, create a new ModuleID enum
  // and tie it with a module name.
  addModule(
      library,
      TestControllerDetector,
      "org_apache_medios_TestControllerDetector");
}


Try<Nothing> initModules(const Option<Modules>& modules)
{
  // First get the user provided modules.
  Modules mergedModules;
  if (modules.isSome()) {
    mergedModules = modules.get();
  }

  // Add isolator modules from testisolator library.
  addIsolatorModules(&mergedModules);

  // Add container logger modules from testcontainer_logger library.
  addContainerLoggerModules(&mergedModules);

  // Add hook modules from testhook library.
  addHookModules(&mergedModules);

  // Add anonymous modules from testanonymous library.
  addAnonymousModules(&mergedModules);

  // Add allocator modules from testallocator library.
  addAllocatorModules(&mergedModules);

  // Add resource estimator modules from testresource_estimator library.
  addResourceEstimatorModules(&mergedModules);

  // Add authorizer modules from testauthorizer library.
  addAuthorizerModules(&mergedModules);

  // Add HTTP authenticator modules from testhttpauthenticator library.
  addHttpAuthenticatorModules(&mergedModules);

  // Add ControllerContender module from testcontrollercontender library.
  addControllerContenderModules(&mergedModules);

  // Add ControllerDetector module from testcontrollerdetector library.
  addControllerDetectorModules(&mergedModules);

  return ModuleManager::load(mergedModules);
}


// Mapping from module ID to the actual module name.
Try<string> getModuleName(ModuleID id)
{
  if (!moduleNames.contains(id)) {
    return Error("Module '" + stringify(id) + "' not found");
  }
  return moduleNames[id];
}

} // namespace tests {
} // namespace internal {
} // namespace medios {
