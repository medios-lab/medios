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

#ifndef __AUTHENTICATION_IDENTITY_ASSERTION_AUTHENTICATOR_HPP__
#define __AUTHENTICATION_IDENTITY_ASSERTION_AUTHENTICATOR_HPP__

#include <string>

#include <medios/module/authenticatee.hpp>
#include <medios/module/authenticator.hpp>

#include <process/future.hpp>
#include <process/pid.hpp>

#include <stout/nothing.hpp>
#include <stout/option.hpp>
#include <stout/try.hpp>

namespace medios {
namespace internal {
namespace authentication {

class IdentityAssertionAuthenticatorProcess;
class IdentityAssertionAuthenticateeProcess;


// IdentityAssertionAuthenticator (DR-029) trusts the principal the peer
// asserts in its AuthenticateMessage. No cryptographic verification.
// Missing or empty asserted principals fail authentication; there is no
// UPID or frameworkInfo.principal fallback.
class IdentityAssertionAuthenticator : public Authenticator
{
public:
  IdentityAssertionAuthenticator() = default;
  ~IdentityAssertionAuthenticator() override;

  Try<Nothing> initialize(const Option<Credentials>& credentials) override;

  process::Future<Option<std::string>> authenticate(
      const process::UPID& pid,
      const Option<std::string>& assertedPrincipal = None()) override;

private:
  IdentityAssertionAuthenticatorProcess* process = nullptr;
  process::PID<IdentityAssertionAuthenticatorProcess> pid;
};


// IdentityAssertionAuthenticatee sends an AuthenticateMessage carrying the
// framework/node's claimed principal (from its Credential) and waits for
// AuthenticationCompletedMessage.
class IdentityAssertionAuthenticatee : public Authenticatee
{
public:
  IdentityAssertionAuthenticatee();
  ~IdentityAssertionAuthenticatee() override;

  process::Future<bool> authenticate(
      const process::UPID& pid,
      const process::UPID& client,
      const Credential& credential) override;

private:
  IdentityAssertionAuthenticateeProcess* process = nullptr;
  process::PID<IdentityAssertionAuthenticateeProcess> pid;
};

} // namespace authentication {
} // namespace internal {
} // namespace medios {

#endif // __AUTHENTICATION_IDENTITY_ASSERTION_AUTHENTICATOR_HPP__
