// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#ifndef __COMMON_PARSE_HPP__
#define __COMMON_PARSE_HPP__

#include <medios/medios.hpp>

#include <medios/authorizer/acls.hpp>

#include <medios/module/module.hpp>

#include <stout/error.hpp>
#include <stout/hashmap.hpp>
#include <stout/json.hpp>
#include <stout/protobuf.hpp>
#include <stout/stringify.hpp>
#include <stout/try.hpp>

#include <stout/flags/parse.hpp>

#include "messages/messages.hpp"

namespace flags {

template <>
inline Try<medios::ACLs> parse(const std::string& value)
{
  // Convert from string or file to JSON.
  Try<JSON::Object> json = parse<JSON::Object>(value);
  if (json.isError()) {
    return Error(json.error());
  }

  // Convert from JSON to Protobuf.
  return protobuf::parse<medios::ACLs>(json.get());
}


template <>
inline Try<medios::RateLimits> parse(const std::string& value)
{
  // Convert from string or file to JSON.
  Try<JSON::Object> json = parse<JSON::Object>(value);
  if (json.isError()) {
    return Error(json.error());
  }

  // Convert from JSON to Protobuf.
  return protobuf::parse<medios::RateLimits>(json.get());
}


template <>
inline Try<medios::Modules> parse(const std::string& value)
{
  // Convert from string or file to JSON.
  Try<JSON::Object> json = parse<JSON::Object>(value);
  if (json.isError()) {
    return Error(json.error());
  }

  // Convert from JSON to Protobuf.
  return protobuf::parse<medios::Modules>(json.get());
}


template <>
inline Try<medios::ContainerInfo> parse(const std::string& value)
{
  // Convert from string or file to JSON.
  Try<JSON::Object> json = parse<JSON::Object>(value);
  if (json.isError()) {
    return Error(json.error());
  }

  // Convert from JSON to Protobuf.
  return protobuf::parse<medios::ContainerInfo>(json.get());
}


template <>
inline Try<medios::DeviceAllowlist> parse(const std::string& value)
{
  // Convert from string or file to JSON.
  Try<JSON::Object> json = parse<JSON::Object>(value);
  if (json.isError()) {
    return Error(json.error());
  }

  // Convert from JSON to Protobuf.
  return protobuf::parse<medios::DeviceAllowlist>(json.get());
}


// When the same variable is listed multiple times,
// uses only the last value.
template <>
inline Try<hashmap<std::string, std::string>> parse(const std::string& value)
{
  // Convert from string or file to JSON.
  Try<JSON::Object> json = parse<JSON::Object>(value);
  if (json.isError()) {
    return Error(json.error());
  }

  // Convert from JSON to Hashmap.
  hashmap<std::string, std::string> map;
  foreachpair (const std::string& key,
               const JSON::Value& value,
               json->values) {
    if (!value.is<JSON::String>()) {
      return Error(
          "The value of key '" + key + "' in '" + stringify(json.get()) + "'"
          " is not a string");
    }

    map[key] = value.as<JSON::String>().value;
  }
  return map;
}


template <>
inline Try<medios::CapabilityInfo> parse(const std::string& value)
{
  Try<JSON::Object> json = parse<JSON::Object>(value);
  if (json.isError()) {
    return Error(json.error());
  }

  return protobuf::parse<medios::CapabilityInfo>(json.get());
}


template <>
inline Try<medios::Environment> parse(const std::string& value)
{
  Try<JSON::Object> json = parse<JSON::Object>(value);
  if (json.isError()) {
    return Error(json.error());
  }

  return protobuf::parse<medios::Environment>(json.get());
}


template <>
inline Try<medios::RLimitInfo> parse(const std::string& value)
{
  Try<JSON::Object> json = parse<JSON::Object>(value);
  if (json.isError()) {
    return Error(json.error());
  }

  return protobuf::parse<medios::RLimitInfo>(json.get());
}


template <>
inline Try<medios::DomainInfo> parse(const std::string& value)
{
  Try<JSON::Object> json = parse<JSON::Object>(value);
  if (json.isError()) {
    return Error(json.error());
  }

  return protobuf::parse<medios::DomainInfo>(json.get());
}


template <>
inline Try<medios::FrameworkID> parse(const std::string& value)
{
  medios::FrameworkID frameworkId;
  frameworkId.set_value(value);

  return frameworkId;
}


template <>
inline Try<medios::ExecutorID> parse(const std::string& value)
{
  medios::ExecutorID executorId;
  executorId.set_value(value);

  return executorId;
}


template <>
inline Try<medios::NodeID> parse(const std::string& value)
{
  medios::NodeID nodeId;
  nodeId.set_value(value);

  return nodeId;
}

} // namespace flags {

#endif // __COMMON_PARSE_HPP__
