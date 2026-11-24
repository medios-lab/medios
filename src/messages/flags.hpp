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

#ifndef __MESSAGES_FLAGS_HPP__
#define __MESSAGES_FLAGS_HPP__

#include <string>
#include <ostream>

#include <stout/error.hpp>
#include <stout/json.hpp>
#include <stout/protobuf.hpp>
#include <stout/try.hpp>

#include <stout/flags/parse.hpp>

#include "common/parse.hpp"

#include "messages/flags.pb.h"

namespace flags {

template <>
inline Try<medios::internal::ImageGcConfig> parse(const std::string& value)
{
  // Convert from string or file to JSON.
  Try<JSON::Object> json = parse<JSON::Object>(value);
  if (json.isError()) {
    return Error(json.error());
  }

  return protobuf::parse<medios::internal::ImageGcConfig>(json.get());
}


template <>
inline Try<medios::internal::Firewall> parse(const std::string& value)
{
  // Convert from string or file to JSON.
  Try<JSON::Object> json = parse<JSON::Object>(value);
  if (json.isError()) {
    return Error(json.error());
  }

  return protobuf::parse<medios::internal::Firewall>(json.get());
}


template <>
inline Try<medios::internal::NodeCapabilities> parse(const std::string& value)
{
  // Convert from string or file to JSON.
  Try<JSON::Object> json = parse<JSON::Object>(value);
  if (json.isError()) {
    return Error(json.error());
  }

  return protobuf::parse<medios::internal::NodeCapabilities>(json.get());
}

} // namespace flags {

namespace medios {
namespace internal {

inline std::ostream& operator<<(
    std::ostream& stream,
    const ImageGcConfig& imageGcConfig)
{
  return stream << imageGcConfig.DebugString();
}


inline std::ostream& operator<<(
    std::ostream& stream,
    const Firewall& rules)
{
  return stream << rules.DebugString();
}


inline std::ostream& operator<<(
    std::ostream& stream,
    const NodeCapabilities& nodeCapabilities)
{
  return stream << nodeCapabilities.DebugString();
}

} // namespace internal {
} // namespace medios {

#endif // __MESSAGES_FLAGS_HPP__
