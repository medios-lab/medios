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

#include <medios/controller/detector.hpp>

#include <medios/module/detector.hpp>

#include <stout/try.hpp>

#include "controller/detector/standalone.hpp"

using namespace medios;
using namespace medios::controller;
using namespace medios::controller::detector;

// Declares a ControllerDetector module named
// 'org_apache_medios_TestControllerDetector'.
medios::modules::Module<ControllerDetector> org_apache_medios_TestControllerDetector(
    MEDIOS_MODULE_API_VERSION,
    MEDIOS_VERSION,
    "Apache Mesos",
    "modules@mesos.apache.org",
    "Test ControllerDetector module.",
    nullptr,
    [](const Parameters& parameters) -> ControllerDetector* {
      return new StandaloneControllerDetector();
    });
