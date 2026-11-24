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

#include "authentication/identity_assertion/identity_assertion_authenticator.hpp"

#include <medios/authentication/authentication.hpp>

#include <process/dispatch.hpp>
#include <process/id.hpp>
#include <process/process.hpp>
#include <process/protobuf.hpp>

#include <stout/stringify.hpp>

using process::Future;
using process::Promise;
using process::UPID;

using std::string;

namespace medios {
namespace internal {
namespace authentication {

// ---- Authenticator (controller side) ----
//
// Backed by a libprocess actor so we can send the AuthenticationCompletedMessage
// back to the peer (otherwise the framework/node authenticatee times out
// waiting for the completion signal). The actor owns its lifetime via the
// Authenticator's destructor.

class IdentityAssertionAuthenticatorProcess
  : public ::ProtobufProcess<IdentityAssertionAuthenticatorProcess>
{
public:
  IdentityAssertionAuthenticatorProcess()
    : ProcessBase(process::ID::generate("identity-assertion-authenticator")) {}

  Future<Option<string>> doAuthenticate(
      const UPID& peer,
      const Option<string>& assertedPrincipal)
  {
    // DR-029: identity_assertion requires an explicit non-empty principal.
    // No UPID fallback, no frameworkInfo.principal fallback, no quiet
    // substitution. A misconfigured peer fails loudly.
    if (assertedPrincipal.isNone() || assertedPrincipal->empty()) {
      LOG(WARNING) << "identity_assertion rejected peer:"
                   << " peer=" << peer
                   << " reason=missing or empty asserted principal";

      send(peer, AuthenticationFailedMessage());
      return Option<string>::none();
    }

    LOG(INFO) << "Authenticated peer via identity_assertion:"
              << " principal=" << assertedPrincipal.get()
              << " peer=" << peer
              << " authenticator=identity_assertion"
              << " verified=false";

    // Tell the peer authentication completed. The authenticatee is
    // waiting for this message before resolving its own future.
    send(peer, AuthenticationCompletedMessage());

    return Option<string>(assertedPrincipal.get());
  }
};


Try<Nothing> IdentityAssertionAuthenticator::initialize(
    const Option<Credentials>& /*credentials*/)
{
  process = new IdentityAssertionAuthenticatorProcess();
  pid = process::spawn(process);
  return Nothing();
}


IdentityAssertionAuthenticator::~IdentityAssertionAuthenticator()
{
  if (process != nullptr) {
    process::terminate(pid);
    process::wait(pid);
    delete process;
  }
}


Future<Option<string>> IdentityAssertionAuthenticator::authenticate(
    const UPID& peer,
    const Option<string>& assertedPrincipal)
{
  return process::dispatch(
      pid,
      &IdentityAssertionAuthenticatorProcess::doAuthenticate,
      peer,
      assertedPrincipal);
}


// ---- Authenticatee (framework / node side) ----
//
// Sends an AuthenticateMessage carrying the credential's principal (asserted,
// not verified) and waits for the controller's AuthenticationCompletedMessage
// or a failure/error response.
//
// Lifetime: the actor is spawned once at construction and torn down in the
// destructor. wait() from the destructor runs in user code (not on a
// libprocess thread), so it is safe.

class IdentityAssertionAuthenticateeProcess
  : public ::ProtobufProcess<IdentityAssertionAuthenticateeProcess>
{
public:
  IdentityAssertionAuthenticateeProcess()
    : ProcessBase(process::ID::generate("identity-assertion-authenticatee")) {}

  Future<bool> doAuthenticate(
      const UPID& server,
      const UPID& client,
      const Credential& credential)
  {
    AuthenticateMessage message;
    message.set_pid(stringify(client));
    if (!credential.principal().empty()) {
      message.set_principal(credential.principal());
    }
    send(server, message);

    return promise.future();
  }

protected:
  void initialize() override
  {
    install<AuthenticationCompletedMessage>(
        &IdentityAssertionAuthenticateeProcess::completed);

    install<AuthenticationFailedMessage>(
        &IdentityAssertionAuthenticateeProcess::failed);

    install<AuthenticationErrorMessage>(
        &IdentityAssertionAuthenticateeProcess::errored,
        &AuthenticationErrorMessage::error);
  }

private:
  void completed(const UPID& /*from*/)
  {
    if (promise.future().isPending()) {
      promise.set(true);
    }
  }

  void failed(const UPID& /*from*/)
  {
    if (promise.future().isPending()) {
      promise.set(false);
    }
  }

  void errored(const UPID& /*from*/, const string& error)
  {
    if (promise.future().isPending()) {
      promise.fail(error);
    }
  }

  Promise<bool> promise;
};


IdentityAssertionAuthenticatee::IdentityAssertionAuthenticatee()
  : process(new IdentityAssertionAuthenticateeProcess()),
    pid(process::spawn(process)) {}


IdentityAssertionAuthenticatee::~IdentityAssertionAuthenticatee()
{
  if (process != nullptr) {
    process::terminate(pid);
    process::wait(pid);
    delete process;
  }
}


Future<bool> IdentityAssertionAuthenticatee::authenticate(
    const UPID& server,
    const UPID& client,
    const Credential& credential)
{
  return process::dispatch(
      pid,
      &IdentityAssertionAuthenticateeProcess::doAuthenticate,
      server,
      client,
      credential);
}

} // namespace authentication {
} // namespace internal {
} // namespace medios {
