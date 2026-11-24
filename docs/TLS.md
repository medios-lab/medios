# TLS on the libprocess transport

The controller, each node, and the executors and frameworks that connect to
them all communicate over libprocess. This page calls each of them a
process. libprocess can run every connection between processes over mutual
TLS: the controller presents a certificate to each node, each node presents
one back, and both sides check the other's against a CA the operator chose.
A node whose certificate was not signed by that CA cannot register; a
controller whose certificate the node cannot verify is never trusted with a
registration.

## Build

TLS is a build option, off by default:

```
cmake -B build -G Ninja -DMEDIOS_ENABLE_SSL=ON
```

It needs OpenSSL headers and libraries on the build host, found with CMake's
`find_package(OpenSSL)`. If CMake cannot find them, pass
`-DOPENSSL_ROOT_DIR=<prefix>` (on a Mac with Homebrew,
`/opt/homebrew/opt/openssl@3`). Configure fails rather than producing a
build without TLS.

A build with the option off has no OpenSSL dependency and ignores every
`LIBPROCESS_SSL_*` variable.

## Runtime

Each process reads its TLS settings from the environment, like every other
libprocess setting (`LIBPROCESS_IP`, `LIBPROCESS_PORT`), not from its own
command line flags. The same variables apply to `medios-controller`,
`medios-node` and `medios-execute`.

| Variable | Meaning |
|---|---|
| `LIBPROCESS_SSL_ENABLED` | `true` turns TLS on for every libprocess connection the process makes or accepts. |
| `LIBPROCESS_SSL_KEY_FILE` | PEM private key for this process. |
| `LIBPROCESS_SSL_CERT_FILE` | PEM certificate (or chain) for this process. |
| `LIBPROCESS_SSL_CA_FILE` | PEM bundle of CA certificates peers are verified against. |
| `LIBPROCESS_SSL_CA_DIR` | A directory of hashed CA certificates, as an alternative to `CA_FILE`. |
| `LIBPROCESS_SSL_VERIFY_SERVER_CERT` | `true`: verify the certificate of every peer this process connects to. |
| `LIBPROCESS_SSL_REQUIRE_CLIENT_CERT` | `true`: require and verify a certificate from every peer that connects to this process. |
| `LIBPROCESS_SSL_VERIFY_IPADD` | `true`: accept a peer certificate whose iPAddress subject alternative name matches the peer's address. Without it, verification matches the hostname from a reverse DNS lookup of the peer. |
| `LIBPROCESS_SSL_HOSTNAME_VALIDATION_SCHEME` | `legacy` (default) or `openssl`. When `VERIFY_IPADD=true`, set `openssl` in production to verify `iPAddress` SANs directly during the OpenSSL handshake without a blocking `getnameinfo()` reverse-DNS lookup on the event-loop thread (#638, #640). |
| `LIBPROCESS_SSL_VERIFICATION_DEPTH` | Maximum chain length accepted, default 4. |
| `LIBPROCESS_SSL_CIPHERS` | OpenSSL cipher list for TLS 1.2 (defaults to forward-secret ECDHE-GCM / CHACHA20-POLY1305 and AES-GCM AEAD suites, #641). |
| `LIBPROCESS_SSL_ENABLE_TLS_V1_2`, `..._V1_3` | Protocol versions offered; both 1.2 and 1.3 are on by default (#641). |

With `ENABLED` set, a process refuses to start unless all of the following
hold, and says which one failed:

- `VERIFY_SERVER_CERT` and `REQUIRE_CLIENT_CERT` are both `true`. TLS
  without peer verification would come up as a working cluster that
  accepts any peer.
- `CA_FILE` or `CA_DIR` is set. Verification against the host's system
  trust store would accept any certificate from a public CA.
- `LIBPROCESS_SSL_SUPPORT_DOWNGRADE` is unset. It lets a peer that fails
  the handshake fall back to plaintext, which skips verification.
- `KEY_FILE` and `CERT_FILE` are set and readable (restrict `KEY_FILE` to
  mode `0600`; libprocess logs a warning at startup if the key file is group-
  or world-readable, #636).

Everything in a process shares the one transport, so the HTTP APIs
(`/api/v1/scheduler`, `/api/v1/operator`, the node's executor API) are
served over HTTPS as well, with the same client certificate requirement.
A client without a certificate the CA signed, `curl` included, is refused
at the handshake.

## Quickstart with a self-signed CA

Suppose a controller at `10.0.0.1` and one node at `203.0.113.7`. The
certificates below carry the controller's and the node's address as an
iPAddress subject alternative name, and both run with `VERIFY_IPADD=true`
and `HOSTNAME_VALIDATION_SCHEME=openssl`, so no reverse DNS is involved.

Make the CA once, somewhere that is not a node:

```
openssl req -x509 -newkey rsa:2048 -sha256 -days 3650 -nodes \
  -subj "/CN=medios-ca" \
  -addext "basicConstraints=critical,CA:TRUE" \
  -keyout ca.key -out ca.crt
chmod 0600 ca.key
```

Make a key and certificate for the controller and one per node, each signed
by the CA. Because libprocess does not poll CRL or OCSP endpoints, issue
short-lived node and controller leaf certificates (e.g. `-days 7`) and
restrict private keys to `0600`:

```
openssl req -newkey rsa:2048 -sha256 -nodes \
  -subj "/CN=controller" -keyout controller.key -out controller.csr
chmod 0600 controller.key
openssl x509 -req -in controller.csr -CA ca.crt -CAkey ca.key -CAcreateserial \
  -days 7 -sha256 -extfile <(printf "subjectAltName=IP:10.0.0.1") \
  -out controller.crt

openssl req -newkey rsa:2048 -sha256 -nodes \
  -subj "/CN=node-1" -keyout node-1.key -out node-1.csr
chmod 0600 node-1.key
openssl x509 -req -in node-1.csr -CA ca.crt -CAkey ca.key -CAcreateserial \
  -days 7 -sha256 -extfile <(printf "subjectAltName=IP:203.0.113.7") \
  -out node-1.crt
```

Copy `ca.crt` to every host, and each controller or node key and
certificate to its own host only (`chmod 0600`). `ca.key` stays where the CA
was made.

Start the controller:

```
LIBPROCESS_SSL_ENABLED=true \
LIBPROCESS_SSL_KEY_FILE=/etc/medios/tls/controller.key \
LIBPROCESS_SSL_CERT_FILE=/etc/medios/tls/controller.crt \
LIBPROCESS_SSL_CA_FILE=/etc/medios/tls/ca.crt \
LIBPROCESS_SSL_VERIFY_SERVER_CERT=true \
LIBPROCESS_SSL_REQUIRE_CLIENT_CERT=true \
LIBPROCESS_SSL_VERIFY_IPADD=true \
LIBPROCESS_SSL_HOSTNAME_VALIDATION_SCHEME=openssl \
medios-controller --ip=10.0.0.1 --port=5050 --registry=in_memory \
  --work_dir=/var/lib/medios/controller
```

Start the node with the same variables pointing at `node-1.key` and
`node-1.crt`.

- **Recommended executor transport (`--http_executor_domain_sockets=true`, #636):**
  When HTTP executors talk to the local node over a UNIX domain socket
  (`--http_executor_domain_sockets=true`), they do not open a TCP/TLS
  connection and **do not need `LIBPROCESS_SSL_KEY_FILE`** in
  `--executor_environment_variables`. Keep `node-1.key` mode `0600` owned by
  the node daemon user so task processes cannot read the node's TLS private
  key.
- **TCP executor transport fallback:** If executors connect to the node over
  TCP (`--http_executor_domain_sockets=false`), pass TLS variables through
  `--executor_environment_variables` using a dedicated localhost-only
  executor certificate (`subjectAltName=IP:127.0.0.1`, or `node-1.key` only on
  single-tenant nodes). `medios-executor` automatically strips
  `LIBPROCESS_SSL_KEY_FILE` and `LIBPROCESS_SSL_KEY_PASSWORD` before forking
  task commands and finish hooks (#636).

```
LIBPROCESS_SSL_ENABLED=true \
LIBPROCESS_SSL_KEY_FILE=/etc/medios/tls/node-1.key \
LIBPROCESS_SSL_CERT_FILE=/etc/medios/tls/node-1.crt \
LIBPROCESS_SSL_CA_FILE=/etc/medios/tls/ca.crt \
LIBPROCESS_SSL_VERIFY_SERVER_CERT=true \
LIBPROCESS_SSL_REQUIRE_CLIENT_CERT=true \
LIBPROCESS_SSL_VERIFY_IPADD=true \
LIBPROCESS_SSL_HOSTNAME_VALIDATION_SCHEME=openssl \
medios-node --controller=10.0.0.1:5050 --ip=203.0.113.7 --port=5051 \
  --work_dir=/var/lib/medios/node \
  --http_executor_domain_sockets=true
```

Both logs carry `Will require client certificates for incoming TLS
connections.` once the transport is up, and the node's `Registered with
controller` line means the registration went over mutual TLS.

A node whose certificate the CA did not sign never registers. Its log
repeats

```
Failed to link to '10.0.0.1:5050', connect: Protocol error
```

and the controller, at default verbosity, warns

```
Error with certificate at depth: 0
Error (20): unable to get local issuer certificate
Failed to accept socket: Failed to SSL handshake with 203.0.113.7:5051: Protocol error
```

Each side verifies the peer's certificate against the address the
connection carries, so a certificate's `subjectAltName` must cover both:

1. **The address the daemon advertises and listens on** (`--ip`, and
   `--advertise_ip` when they differ).
2. **The outbound egress IP seen by peers when the daemon initiates a
   connection (#642).** In libprocess, message passing can open a new
   controller-to-node TCP connection when no reusable socket exists; the node
   acts as the TLS server on that connection and verifies the controller's
   certificate against the controller's source IP. If the controller sits
   behind NAT or a multi-homed routing table, include both its advertised IP
   and its outbound egress IP in `controller.crt`'s `subjectAltName`
   (e.g. `subjectAltName=IP:10.0.0.1,IP:198.51.100.10`). Likewise, a node
   behind NAT needs both its private `--ip` and its public `--advertise_ip`
   in `node-1.crt`.

`medios-execute` and any other framework process need the same variables
and a certificate from the same CA whose `iPAddress` SAN matches the source IP
the controller sees.

## Production hardening checklist

- **Use `LIBPROCESS_SSL_HOSTNAME_VALIDATION_SCHEME=openssl` with
  `LIBPROCESS_SSL_VERIFY_IPADD=true` (#638, #640).** The default `legacy`
  scheme calls `getnameinfo()` synchronously on the libprocess event-loop
  thread for every incoming connection before checking `iPAddress` SANs, so
  slow or unresponsive reverse DNS can stall the event loop. The `openssl`
  scheme verifies `iPAddress` SANs in both client and server mode inside
  OpenSSL without any DNS lookup.
- **Gate libprocess ports at the network edge (#638).** Restrict port `5050`
  (controller) and `5051` (node) with security groups or host firewall rules
  (`nftables` / `iptables`) to cluster subnets or an authenticating gateway so
  untrusted networks cannot hold open half-completed TLS handshakes.
- **Enable application-layer authentication and ACLs alongside mTLS
  (#635, #637, #643).** Mutual TLS verifies that a peer holds a certificate
  signed by the cluster CA and matching its IP; it does not bind a certificate
  to a specific framework principal, node ID, or operator role. Production
  deployments must also enable `--authenticate_frameworks=true`,
  `--authenticate_http_frameworks=true`, `--authenticate_nodes=true`,
  `--authenticate_http_readwrite=true`, `--authenticate_http_readonly=true`,
  `--authenticate_http_executors=true`, and `--acls` (with `permissive: false`
  and `ACT_FOR_PRINCIPAL` rules per `DR-037`).

## What is not provided

- Certificate issuance and rotation. The CA, the certificates
  and their renewal are the operator's, by hand or through whatever PKI
  they run. A process reads its files at startup only; a new certificate
  needs a restart.
- Revocation. There is no CRL or OCSP check; issue short-lived certificates
  (e.g. 7 days) and withdraw a compromised certificate by replacing the CA
  and reissuing.
- Identity beyond the CA. Verification establishes that a peer holds a
  certificate the CA signed and that it names the peer's address; it does
  not tie a certificate to a particular node id or framework principal.
  Framework and node authentication are separate mechanisms with their own
  flags (see the production hardening checklist above).
- Partial TLS. A process is either all TLS or all plaintext; it cannot serve
  plaintext HTTP to one client and TLS to another.
