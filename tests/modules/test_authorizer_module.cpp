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

#include <medios/medios.hpp>
#include <medios/module.hpp>

#include <medios/authorizer/acls.hpp>
#include <medios/authorizer/authorizer.hpp>

#include <medios/module/authorizer.hpp>

#include <stout/foreach.hpp>
#include <stout/option.hpp>
#include <stout/path.hpp>
#include <stout/try.hpp>

#include "authorizer/local/authorizer.hpp"

#include "common/parse.hpp"

using std::string;

using namespace medios;

static Authorizer* createLocalAuthorizer(const Parameters& parameters)
{
  Try<Authorizer*> local = medios::internal::LocalAuthorizer::create(parameters);
  if (local.isError()) {
    return nullptr;
  }

  return local.get();
}


// Declares an Authorizer module named
// 'org_apache_medios_TestLocalAuthorizer'.
medios::modules::Module<Authorizer> org_apache_medios_TestLocalAuthorizer(
    MEDIOS_MODULE_API_VERSION,
    MEDIOS_VERSION,
    "Apache Mesos",
    "modules@mesos.apache.org",
    "Test Authorizer module.",
    nullptr,
    createLocalAuthorizer);
