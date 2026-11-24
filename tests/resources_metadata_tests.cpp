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

#include <gtest/gtest.h>

#include <medios/resources.hpp>

using medios::Label;
using medios::Resource;
using medios::Resources;
using medios::Value;

namespace {

// Builds `gpus:<count>` carrying a single opaque metadata pair `class=<klass>`.
// An empty `klass` leaves the resource untyped (no metadata).
Resources gpus(double count, const std::string& klass)
{
  Resource resource;
  resource.set_name("gpus");
  resource.set_type(Value::SCALAR);
  resource.mutable_scalar()->set_value(count);

  if (!klass.empty()) {
    Label* label = resource.mutable_metadata()->add_labels();
    label->set_key("class");
    label->set_value(klass);
  }

  return Resources(resource);
}

} // namespace {


// Resources carrying different opaque metadata are distinct: they do not add,
// and one does not satisfy a request for the other. Same metadata behaves like
// an ordinary scalar. Medios compares the metadata by equality only and never
// interprets it (the strings here happen to name GPU families, but core does
// not know that).
TEST(ResourceMetadataTest, MatchingRespectsMetadata)
{
  // Same metadata: sums and contains like a normal scalar.
  EXPECT_EQ(gpus(8, "h100"), gpus(4, "h100") + gpus(4, "h100"));
  EXPECT_TRUE(gpus(8, "h100").contains(gpus(4, "h100")));

  // Different metadata: not contained.
  EXPECT_FALSE(gpus(8, "h100").contains(gpus(4, "a100")));

  // A mixed node holds both classes distinctly.
  Resources node = gpus(4, "h100") + gpus(4, "a100");
  EXPECT_TRUE(node.contains(gpus(4, "h100")));
  EXPECT_TRUE(node.contains(gpus(4, "a100")));
  EXPECT_FALSE(node.contains(gpus(8, "h100"))); // only 4 h100 present

  // Exact matching (option (a)): an untyped request is distinct from a typed
  // one and does not cross-satisfy in either direction.
  EXPECT_FALSE(gpus(8, "h100").contains(gpus(4, "")));
  EXPECT_FALSE(gpus(8, "").contains(gpus(4, "h100")));
}


// `find` extracts a concrete subset satisfying a metadata-typed target,
// and refuses a target whose type is not present.
TEST(ResourceMetadataTest, FindRespectsMetadata)
{
  Resources node =
    Resources::parse("cpus:4;mem:2048").get() + gpus(8, "h100");

  Option<Resources> found =
    node.find(Resources::parse("cpus:1;mem:512").get() + gpus(4, "h100"));

  ASSERT_TRUE(found.isSome());
  EXPECT_TRUE(found->contains(gpus(4, "h100")));

  EXPECT_TRUE(
      node.find(Resources::parse("cpus:1").get() + gpus(4, "a100")).isNone());
}
