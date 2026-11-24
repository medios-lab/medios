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

#ifndef __MEDIOS_MODULE_HPP__
#define __MEDIOS_MODULE_HPP__

// Medios Module API (MESOS-1384).
//
// A Medios module is an instance of a module 'kind' known to Medios,
// implemented in a dynamically loaded library. A module library can
// hold multiple modules. When a Medios controller or node is started, it
// takes a list of libraries with contained modules as command line
// argument (in JSON format or a path to a JSON file).
//
// JSON := {"libraries": [library, ...]}
// library := {"file": <library path>,
//             "modules": [<module name>, ...]}
//
// How to write a module library:
// 1. Define a create() function that returns a pointer to an object
//    of 'kind' type.
// 2. If you want to indicate backwards compatibility for a module,
//    create:
//      'bool compatible() { return true; }
//    (You can replace compatible with any other name).
//    If you want custom compatibility checks, replace the return
//    statement above with them.
// 3. Define a variable of Module<KIND> struct type and populate the
//    fields (including pointers to 'create' and 'compatible').  The
//    variable name thus becomes the module name.

#include <medios/version.hpp>

#define MEDIOS_MODULE_API_VERSION "2"

namespace medios {
namespace modules {

// Internal utilities, not part of the module API:

// Declare a loadable module library.
// This also provides handshakes with Medios for version checking.
struct ModuleBase
{
  ModuleBase(
      const char* _moduleApiVersion,
      const char* _mediosVersion,
      const char* _kind,
      const char* _authorName,
      const char* _authorEmail,
      const char* _description,
      bool (*_compatible)())
    : moduleApiVersion(_moduleApiVersion),
      mediosVersion(_mediosVersion),
      kind(_kind),
      authorName(_authorName),
      authorEmail(_authorEmail),
      description(_description),
      compatible(_compatible) {}

  const char* moduleApiVersion;
  const char* mediosVersion;

  // String representation of module 'kind' returned from 'create'.
  const char* kind;
  const char* authorName;
  const char* authorEmail;
  const char* description;

  // Callback invoked to check version compatibility.  If this field
  // is `nullptr`, backwards compatibility is disabled and the module
  // version must match the Medios release version exactly. If the
  // macro is used, Medios first checks backwards compatibility against
  // its own internal table maintained by Medios developers, then your
  // implementation that follows the macro gets a say. If you return
  // true, the module is admitted.
  bool (*compatible)();
};


// These declarations are neeed only for later specializations.

template <typename T>
struct Module;

template <typename T>
const char* kind();

// Each module "kind" specialization extends ModuleBase to provide a `create()`
// method that returns a pointer to an object of the given module "kind".
//   template <> struct Module<medios::SecretResolver> : ModuleBase {
//     Module(..., T* (*_create)(const Parameters&))
//       : ModuleBase(...), create(_create) {...}
//     T* (*create)(const Parameters&);
//   };

// TODO(kapil): Update module interface to return a managed pointer instead of
// returning raw pointers. This would allow the caller to manage the lifecycle
// of the dynamically-allocated object. We should also allow passing
// controller/node flags during module initialization. E.g.,
//   unique_ptr<T> (*create)(const Parameters&, const Flags&);

} // namespace modules {
} // namespace medios {

#endif // __MEDIOS_MODULE_HPP__
