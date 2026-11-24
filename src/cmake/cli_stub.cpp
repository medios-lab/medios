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

#include <cstring>
#include <iostream>

#include <medios/version.hpp>

#ifndef MEDIOS_COMPONENT
#define MEDIOS_COMPONENT "medios-ai"
#endif

namespace {

void usage()
{
  std::cout
    << "Usage: " << MEDIOS_COMPONENT << " [--help] [--version]\n"
    << "\n"
    << MEDIOS_COMPONENT
    << " is linked as a PHASE-A build-system stub.\n";
}

} // namespace

int main(int argc, char** argv)
{
  for (int i = 1; i < argc; ++i) {
    if (std::strcmp(argv[i], "--help") == 0 ||
        std::strcmp(argv[i], "-h") == 0) {
      usage();
      return 0;
    }

    if (std::strcmp(argv[i], "--version") == 0 ||
        std::strcmp(argv[i], "-v") == 0) {
      std::cout << MEDIOS_COMPONENT << " " << MEDIOS_VERSION << "\n";
      return 0;
    }

    std::cerr << MEDIOS_COMPONENT << ": unknown option '" << argv[i] << "'\n";
    return 1;
  }

  usage();
  return 0;
}
