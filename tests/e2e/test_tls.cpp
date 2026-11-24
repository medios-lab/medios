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

// Mutual TLS on the libprocess transport, end to end.
//
// Spawns medios-controller + medios-node with LIBPROCESS_SSL_* in their
// environment and asserts that a node registers over mutual TLS, that a
// node whose certificate chains to a different CA is refused, and that a
// daemon refuses to start when TLS is on but peer verification is not.
//
// Keys and certificates are generated per test under the fixture's work
// root and removed in TearDown. Nothing is printed from them.

#include <unistd.h>

#include <chrono>
#include <filesystem>
#include <optional>
#include <string>
#include <vector>

#include <openssl/evp.h>
#include <openssl/x509.h>

#include <gtest/gtest.h>

#include <process/ssl/utilities.hpp>

#include <stout/ip.hpp>
#include <stout/path.hpp>

#include "cluster_harness.hpp"

namespace fs = std::filesystem;

using std::string;
using std::vector;

using namespace medios::tests::e2e;

namespace {

// Validity of the generated certificates. A test runs for seconds;
// one day keeps a slow clock on either side from mattering.
constexpr int CERTIFICATE_DAYS = 1;

// Long enough for a daemon that is going to refuse to start to exit
// and flush its message through the pipe.
constexpr std::chrono::seconds REFUSAL_TIMEOUT{10};

// Each daemon binds and connects on 127.0.0.1, so every certificate
// names that address. Verification matches it against the iPAddress
// subject alternative name (LIBPROCESS_SSL_VERIFY_IPADD).
const string LOOPBACK = "127.0.0.1";


// A certificate authority. The key lives only in memory, for signing.
struct Authority
{
  EVP_PKEY* key = nullptr;
  X509* certificate = nullptr;
  fs::path certificateFile;
};


// A key and certificate on disk, for one daemon.
struct Identity
{
  fs::path keyFile;
  fs::path certificateFile;
};


void freeAuthority(Authority& authority)
{
  if (authority.certificate != nullptr) X509_free(authority.certificate);
  if (authority.key != nullptr) EVP_PKEY_free(authority.key);
  authority.certificate = nullptr;
  authority.key = nullptr;
}


// Generates a self-signed CA and writes its certificate to `dir`.
void makeAuthority(
    const fs::path& dir,
    const string& name,
    int serial,
    Authority* authority)
{
  Try<EVP_PKEY*> key = process::network::openssl::generate_private_rsa_key();
  ASSERT_FALSE(key.isError()) << key.error();
  authority->key = key.get();

  Try<X509*> certificate = process::network::openssl::generate_x509(
      authority->key,
      authority->key,
      None(),
      serial,
      CERTIFICATE_DAYS,
      name,
      None(),
      true);
  ASSERT_FALSE(certificate.isError()) << certificate.error();
  authority->certificate = certificate.get();

  authority->certificateFile = dir / (name + ".crt");
  Try<Nothing> written = process::network::openssl::write_certificate_file(
      authority->certificate, Path(authority->certificateFile.string()));
  ASSERT_FALSE(written.isError()) << written.error();
}


// Generates a key and a certificate signed by `authority`, both on disk.
void makeIdentity(
    const fs::path& dir,
    const Authority& authority,
    const string& name,
    int serial,
    Identity* identity)
{
  Try<EVP_PKEY*> key = process::network::openssl::generate_private_rsa_key();
  ASSERT_FALSE(key.isError()) << key.error();

  Try<net::IP> ip = net::IP::parse(LOOPBACK, AF_INET);
  ASSERT_FALSE(ip.isError()) << ip.error();

  Try<X509*> certificate = process::network::openssl::generate_x509(
      key.get(),
      authority.key,
      authority.certificate,
      serial,
      CERTIFICATE_DAYS,
      name,
      ip.get());
  if (certificate.isError()) {
    EVP_PKEY_free(key.get());
    FAIL() << certificate.error();
  }

  identity->keyFile = dir / (name + ".key");
  identity->certificateFile = dir / (name + ".crt");

  Try<Nothing> keyWritten = process::network::openssl::write_key_file(
      key.get(), Path(identity->keyFile.string()));
  Try<Nothing> certificateWritten =
    process::network::openssl::write_certificate_file(
        certificate.get(), Path(identity->certificateFile.string()));

  EVP_PKEY_free(key.get());
  X509_free(certificate.get());

  ASSERT_FALSE(keyWritten.isError()) << keyWritten.error();
  ASSERT_FALSE(certificateWritten.isError()) << certificateWritten.error();
}


class TlsTest : public ClusterFixture
{
protected:
  void SetUp() override
  {
    tlsDir = testRoot() / ("tls-" + std::to_string(::getpid()));
    fs::remove_all(tlsDir);
    fs::create_directories(tlsDir);

    ASSERT_NO_FATAL_FAILURE(makeAuthority(tlsDir, "cluster-ca", 1, &ca));
    ASSERT_NO_FATAL_FAILURE(
        makeIdentity(tlsDir, ca, "controller", 2, &controller));
    ASSERT_NO_FATAL_FAILURE(makeIdentity(tlsDir, ca, "node", 3, &node));
  }

  void TearDown() override
  {
    ClusterFixture::TearDown();
    freeAuthority(ca);
    fs::remove_all(tlsDir);
  }

  // The environment that turns on TLS for one daemon. `caFile` is what
  // the daemon verifies its peers against; `verify` false leaves both
  // verification flags unset, which is the configuration a daemon must
  // refuse.
  static vector<string> tlsEnvironment(
      const Identity& identity,
      const fs::path& caFile,
      bool verify = true)
  {
    vector<string> env = {
      "LIBPROCESS_SSL_ENABLED=true",
      "LIBPROCESS_SSL_KEY_FILE=" + identity.keyFile.string(),
      "LIBPROCESS_SSL_CERT_FILE=" + identity.certificateFile.string(),
      "LIBPROCESS_SSL_CA_FILE=" + caFile.string(),
      "LIBPROCESS_SSL_VERIFY_IPADD=true"
    };
    if (verify) {
      env.push_back("LIBPROCESS_SSL_VERIFY_SERVER_CERT=true");
      env.push_back("LIBPROCESS_SSL_REQUIRE_CLIENT_CERT=true");
    }
    return env;
  }

  // Returns the exit status of a controller that never reported a port:
  // the one startController already reaped, or else waits for it.
  // Clears the pid so TearDown does not stop a process that is gone.
  std::optional<int> awaitControllerRefusal()
  {
    if (controllerExitStatus.has_value()) return controllerExitStatus;
    if (controllerPid < 0) return std::nullopt;

    std::optional<int> status = waitForExit(
        controllerPid,
        controllerLogFd,
        controllerLogs,
        std::chrono::steady_clock::now() + REFUSAL_TIMEOUT);
    if (status.has_value()) {
      controllerPid = -1;
    }
    return status;
  }

  string diagnostics() const
  {
    return "--- node logs ---\n" + nodeLogs
         + "\n--- controller logs ---\n" + controllerLogs;
  }

  fs::path tlsDir;
  Authority ca;
  Identity controller;
  Identity node;
};


TEST_F(TlsTest, NodeRegistersOverMutualTls)
{
  const int controllerPort =
    startController(tlsEnvironment(controller, ca.certificateFile));
  ASSERT_GT(controllerPort, 0) << "controller failed to bind a port:\n"
                                << controllerLogs;

  ASSERT_TRUE(startNode(controllerPort, {
        "--containerizers=medios",
        "--launcher=posix",
        "--isolation="
      }, tlsEnvironment(node, ca.certificateFile)))
      << "node failed to register with controller:\n" << diagnostics();

  // Both daemons log this line only once TLS is on with client
  // certificates required, so registration happened over mutual TLS
  // rather than over the plaintext transport.
  const string requiring = "Will require client certificates";
  EXPECT_NE(string::npos, controllerLogs.find(requiring)) << diagnostics();
  EXPECT_NE(string::npos, nodeLogs.find(requiring)) << diagnostics();
  EXPECT_EQ(string::npos, nodeLogs.find("Failed to link")) << diagnostics();
}


TEST_F(TlsTest, NodeFromForeignAuthorityIsRefused)
{
  Authority foreign;
  Identity impostor;
  ASSERT_NO_FATAL_FAILURE(makeAuthority(tlsDir, "foreign-ca", 1, &foreign));
  ASSERT_NO_FATAL_FAILURE(
      makeIdentity(tlsDir, foreign, "impostor", 2, &impostor));
  freeAuthority(foreign);

  const int controllerPort =
    startController(tlsEnvironment(controller, ca.certificateFile));
  ASSERT_GT(controllerPort, 0) << "controller failed to bind a port:\n"
                                << controllerLogs;

  // The node trusts the cluster CA, so it accepts the controller; the
  // controller must reject the node's certificate.
  EXPECT_FALSE(startNode(controllerPort, {
        "--containerizers=medios",
        "--launcher=posix",
        "--isolation="
      }, tlsEnvironment(impostor, ca.certificateFile)))
      << "node with a foreign certificate registered:\n" << diagnostics();

  EXPECT_EQ(string::npos, nodeLogs.find("Registered with controller"))
      << diagnostics();
  EXPECT_EQ(string::npos, controllerLogs.find("Registering node"))
      << diagnostics();
}


TEST_F(TlsTest, RefusesToStartWithoutPeerVerification)
{
  const int controllerPort = startController(
      tlsEnvironment(controller, ca.certificateFile, /* verify */ false));
  EXPECT_EQ(0, controllerPort)
      << "controller started without verification:\n" << controllerLogs;

  std::optional<int> status = awaitControllerRefusal();
  ASSERT_TRUE(status.has_value()) << "controller still running:\n"
                                  << controllerLogs;
  EXPECT_NE(0, *status) << controllerLogs;
  EXPECT_NE(string::npos,
            controllerLogs.find("requires peer verification on both sides"))
      << controllerLogs;
  EXPECT_EQ(string::npos, controllerLogs.find("started on")) << controllerLogs;
}


TEST_F(TlsTest, RefusesToStartWithDowngrade)
{
  vector<string> env = tlsEnvironment(controller, ca.certificateFile);
  env.push_back("LIBPROCESS_SSL_SUPPORT_DOWNGRADE=true");

  const int controllerPort = startController(env);
  EXPECT_EQ(0, controllerPort) << "controller started with downgrade:\n"
                               << controllerLogs;

  std::optional<int> status = awaitControllerRefusal();
  ASSERT_TRUE(status.has_value()) << "controller still running:\n"
                                  << controllerLogs;
  EXPECT_NE(0, *status) << controllerLogs;
  EXPECT_NE(string::npos,
            controllerLogs.find("LIBPROCESS_SSL_SUPPORT_DOWNGRADE"))
      << controllerLogs;
  EXPECT_EQ(string::npos, controllerLogs.find("started on")) << controllerLogs;
}

} // namespace
