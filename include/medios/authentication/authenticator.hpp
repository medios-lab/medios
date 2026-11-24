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

#ifndef __MEDIOS_AUTHENTICATION_AUTHENTICATOR_HPP__
#define __MEDIOS_AUTHENTICATION_AUTHENTICATOR_HPP__

#include <string>

#include <medios/medios.hpp>

#include <medios/authentication/authentication.hpp>

#include <process/future.hpp>
#include <process/pid.hpp>

#include <stout/nothing.hpp>
#include <stout/option.hpp>

namespace medios {

class Authenticator
{
public:
  Authenticator() {}

  virtual ~Authenticator() {}

  virtual Try<Nothing> initialize(const Option<Credentials>& credentials) = 0;

  // Returns the principal of the Authenticatee if successfully
  // authenticated otherwise None or an error. Note that we
  // distinguish authentication failure (None) from a failed future
  // in the event the future failed due to a transient error and
  // authentication can (should) be retried. Discarding the future
  // will cause the future to fail if it hasn't already completed
  // since we have already started the authentication procedure and
  // can't reliably cancel.
  // `assertedPrincipal` is an out-of-band hint from the controller's
  // AuthenticateMessage handler used by the identity_assertion mode (DR-029):
  // the peer self-claims this principal and the authenticator records it
  // without cryptographic verification. Cryptographic authenticators MUST
  // ignore this field; it never substitutes for verified identity.
  virtual process::Future<Option<std::string>> authenticate(
      const process::UPID& pid,
      const Option<std::string>& assertedPrincipal = None()) = 0;
};

} // namespace medios {

#endif // __MEDIOS_AUTHENTICATION_AUTHENTICATOR_HPP__
