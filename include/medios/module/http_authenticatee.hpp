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

#ifndef __MEDIOS_MODULE_HTTP_AUTHENTICATEE_HPP__
#define __MEDIOS_MODULE_HTTP_AUTHENTICATEE_HPP__

#include <medios/medios.hpp>
#include <medios/module.hpp>

#include <medios/authentication/http/authenticatee.hpp>

namespace medios {
namespace modules {

template <>
inline const char* kind<medios::http::authentication::Authenticatee>()
{
  return "HttpAuthenticatee";
}


template <>
struct Module<medios::http::authentication::Authenticatee> : ModuleBase
{
  Module(
      const char* _moduleApiVersion,
      const char* _mediosVersion,
      const char* _authorName,
      const char* _authorEmail,
      const char* _description,
      bool (*_compatible)(),
      medios::http::authentication::Authenticatee* (*_create)(
          const Parameters& parameters))
    : ModuleBase(
        _moduleApiVersion,
        _mediosVersion,
        medios::modules::kind<medios::http::authentication::Authenticatee>(),
        _authorName,
        _authorEmail,
        _description,
        _compatible),
      create(_create) {}

  medios::http::authentication::Authenticatee* (*create)(
      const Parameters& parameters);
};

} // namespace modules {
} // namespace medios {

#endif // __MEDIOS_MODULE_HTTP_AUTHENTICATEE_HPP__
