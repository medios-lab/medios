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

#ifndef __TEST_MODULE_HPP__
#define __TEST_MODULE_HPP__

#include <medios/medios.hpp>
#include <medios/module.hpp>

#include <stout/nothing.hpp>
#include <stout/try.hpp>

// Each module "kind" has a base class associated with it that is
// inherited by the module instances.  Medios core uses the base
// class interface to bind with the module instances.
// TestModule is a base class for the "TestModule" kind.
class TestModule
{
public:
  TestModule() {}

  // Medios core will use the base class pointer to delete the module
  // instance (which is a derived object).  The virtual destructor
  // here ensures that the derived destructor is called for any
  // cleanup that may be required for the derived object.
  virtual ~TestModule() {}

  virtual Try<Nothing> initialize(const medios::Parameters& parameters) = 0;

  virtual int foo(char a, long b) = 0;

  virtual int bar(float a, double b) = 0;

  virtual int baz(int a, int b) = 0;

  virtual medios::Parameters parameters() const = 0;
};


namespace medios {
namespace modules {

template <>
inline const char* kind<TestModule>()
{
  return "TestModule";
}


template <>
struct Module<TestModule> : ModuleBase
{
  Module(
      const char* _moduleApiVersion,
      const char* _mediosVersion,
      const char* _authorName,
      const char* _authorEmail,
      const char* _description,
      bool (*_compatible)(),
      TestModule* (*_create)(const Parameters& parameters))
    : ModuleBase(
        _moduleApiVersion,
        _mediosVersion,
        medios::modules::kind<TestModule>(),
        _authorName,
        _authorEmail,
        _description,
        _compatible),
      create(_create) {}

  TestModule* (*create)(const Parameters& parameters);
};

} // namespace modules {
} // namespace medios {

#endif // __TEST_MODULE_HPP__
