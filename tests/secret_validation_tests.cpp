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

#include <gtest/gtest.h>

#include <medios/medios.hpp>

#include <stout/error.hpp>
#include <stout/gtest.hpp>
#include <stout/option.hpp>
#include <stout/strings.hpp>

#include "common/validation.hpp"

using medios::internal::common::validation::validateEnvironment;
using medios::internal::common::validation::validateVolume;

namespace medios {
namespace internal {
namespace tests {

// No shipped isolator turns a SECRET variable into an environment variable,
// so validation refuses it rather than letting the task start without it
// (#42).
TEST(SecretValidationTest, SecretEnvironmentVariableIsRefused)
{
  Environment environment;
  Environment::Variable* variable = environment.add_variables();
  variable->set_name("MY_TOKEN");
  variable->set_type(Environment::Variable::SECRET);
  variable->mutable_secret()->set_type(Secret::VALUE);
  variable->mutable_secret()->mutable_value()->set_data("unused");

  Option<Error> error = validateEnvironment(environment);
  ASSERT_SOME(error);
  EXPECT_TRUE(strings::contains(error->message, "'MY_TOKEN'"))
    << error->message;
  EXPECT_TRUE(strings::contains(error->message, "cannot deliver"))
    << error->message;
  EXPECT_FALSE(strings::contains(error->message, "unused"))
    << error->message;
}


TEST(SecretValidationTest, ReferenceSecretEnvironmentVariableIsRefused)
{
  Environment environment;
  Environment::Variable* variable = environment.add_variables();
  variable->set_name("MY_TOKEN");
  variable->set_type(Environment::Variable::SECRET);
  variable->mutable_secret()->set_type(Secret::REFERENCE);
  variable->mutable_secret()->mutable_reference()->set_name("/team/token");

  Option<Error> error = validateEnvironment(environment);
  ASSERT_SOME(error);
  EXPECT_TRUE(strings::contains(error->message, "'MY_TOKEN'"))
    << error->message;
  EXPECT_TRUE(strings::contains(error->message, "cannot deliver"))
    << error->message;
}


TEST(SecretValidationTest, ValueEnvironmentVariableIsAccepted)
{
  Environment environment;
  Environment::Variable* variable = environment.add_variables();
  variable->set_name("PLAIN");
  variable->set_type(Environment::Variable::VALUE);
  variable->set_value("1");

  EXPECT_NONE(validateEnvironment(environment));
}


// No shipped isolator mounts a SECRET volume (#42).
TEST(SecretValidationTest, SecretVolumeIsRefused)
{
  Volume volume;
  volume.set_container_path("secrets/token");
  volume.set_mode(Volume::RO);
  volume.mutable_source()->set_type(Volume::Source::SECRET);
  Secret* secret = volume.mutable_source()->mutable_secret();
  secret->set_type(Secret::VALUE);
  secret->mutable_value()->set_data("unused");

  Option<Error> error = validateVolume(volume);
  ASSERT_SOME(error);
  EXPECT_TRUE(strings::contains(error->message, "'secrets/token'"))
    << error->message;
  EXPECT_TRUE(strings::contains(error->message, "cannot deliver"))
    << error->message;
  EXPECT_FALSE(strings::contains(error->message, "unused"))
    << error->message;
}

} // namespace tests {
} // namespace internal {
} // namespace medios {
