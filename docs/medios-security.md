# Medios Security & Deployment Modes

Medios is a resource broker and task execution substrate designed to scale from a single developer laptop on loopback up to a multi-host cloud deployment. Out-of-the-box defaults prioritize zero-ceremony local development (`identity_assertion`, unprivileged `--switch_user=false` fallback, and open read-only inspection endpoints), while production controls (mutual TLS, HTTP Basic / `trusted_header` authentication, `LocalAuthorizer` ACLs, user switching, and Linux security isolators) are activated by configuration flags as the deployment crosses trust boundaries.

This document defines the **three supported deployment modes**, provides the concrete flag and environment configurations for each mode, and maps security review requirements to their corresponding Medios product controls.

---

## 1. Deployment Modes

### Mode 1: Standalone Laptop (User-Space on Loopback)

- **Topology:** `medios-controller` and `medios-node` (or `medios-local`) run on the same workstation under a single unprivileged user account, bound strictly to `127.0.0.1`.
- **Trust Boundary:** The host OS user boundary. No remote network interfaces are bound.
- **Recommended Configuration:**
  ```bash
  export LIBPROCESS_IP=127.0.0.1

  medios-controller \
    --ip=127.0.0.1 \
    --port=5050 \
    --work_dir=$HOME/.medios/controller \
    --registry=in_memory

  medios-node \
    --ip=127.0.0.1 \
    --port=5051 \
    --controller=127.0.0.1:5050 \
    --work_dir=$HOME/.medios/node \
    --runtime_dir=$HOME/.medios/run \
    --switch_user=false \
    --containerizers=medios \
    --launcher=posix \
    --isolation=filesystem/posix,posix/cpu,posix/mem \
    --http_executor_domain_sockets=true
  ```
  *(Note: the `environment/secret` isolator is created and wired automatically by `MediosContainerizer::create` and does not need to be passed in `--isolation`.)*

---

### Mode 2: Private Lab / Overlay Cluster (WireGuard / Tailscale)

- **Topology:** One controller and multiple Linux/macOS worker nodes communicating exclusively over an authenticated, encrypted mesh or private VLAN (for example Tailscale or WireGuard). All frameworks (`cue`, `fennel`, `ray-medios`) are operated by the same team.
- **Trust Boundary:** The encrypted overlay network (`100.64.0.0/10` or private RFC1918 subnet). Host firewalls drop all traffic to controller/node ports (`5050`, `5051`, ephemeral `LIBPROCESS_PORT`) from outside the overlay interface.
- **Recommended Configuration:**
  - Bind `--ip` and `LIBPROCESS_IP` to the node's overlay address.
  - Enable `--registry=leveldb` on the controller for restart durability.
  - Enable `--switch_user=true` on Linux worker nodes so tasks execute under the non-root OS account specified in `FrameworkInfo.user` / `CommandInfo.user`.
  - Enable `--http_executor_domain_sockets=true` so executors communicate with the local `medios-node` over a `0600` (or `0666` when `switch_user=true`) UNIX domain socket inside `--runtime_dir` rather than TCP.
  - Require explicit `FrameworkInfo.principal` and `TaskInfo.principal` (`operator:<name>`, `team:<name>`, `run:<name>`) for audit attribution (`DR-037`).

---

### Mode 3: Cloud / Multi-Host Production Deployment

- **Topology:** Controller and worker nodes deployed across cloud VMs or bare-metal providers where the underlay network is untrusted or where HTTP endpoints are reachable through an edge gateway.
- **Trust Boundary:** Explicit cryptographic transport verification (mTLS) between daemons, authenticated HTTP and framework APIs, non-permissive `LocalAuthorizer` ACLs, and Linux kernel namespace/capability/seccomp isolation.
- **Required Configuration:**
  1. **Transport Mutual TLS (see [TLS.md](TLS.md) for certificate generation and SAN rules):**
     ```bash
     export LIBPROCESS_SSL_ENABLED=true
     export LIBPROCESS_SSL_VERIFY_SERVER_CERT=true
     export LIBPROCESS_SSL_REQUIRE_CLIENT_CERT=true
     export LIBPROCESS_SSL_VERIFY_IPADD=true
     export LIBPROCESS_SSL_HOSTNAME_VALIDATION_SCHEME=openssl
     export LIBPROCESS_SSL_CA_FILE=/etc/medios/tls/ca.crt
     export LIBPROCESS_SSL_CERT_FILE=/etc/medios/tls/host.crt
     export LIBPROCESS_SSL_KEY_FILE=/etc/medios/tls/host.key   # chmod 0600
     export MEDIOS_FETCHER_DISALLOW_PRIVATE_IPS=true
     ```
  2. **Controller Authentication & Non-Permissive ACLs:**
     ```bash
     medios-controller \
       --ip=<bind-ip> \
       --advertise_ip=<egress-ip> \
       --port=5050 \
       --work_dir=/var/lib/medios/controller \
       --registry=leveldb \
       --authenticate_http_readwrite=true \
       --authenticate_http_readonly=true \
       --authenticate_http_frameworks=true \
       --http_authenticators=basic \
       --http_framework_authenticators=basic \
       --credentials=file:///etc/medios/credentials.json \
       --authorizers=local \
       --acls=file:///etc/medios/acls.json
     ```
     *(When running behind an authenticating reverse proxy at the cloud edge, `--http_framework_authenticators=trusted_header` can be used in place of `basic` inside the private VPC.)*
  3. **Node Authentication, User Switching & Linux Hardening Isolators:**
     ```bash
     medios-node \
       --ip=<bind-ip> \
       --advertise_ip=<egress-ip> \
       --port=5051 \
       --controller=<controller-ip>:5050 \
       --work_dir=/var/lib/medios/node \
       --runtime_dir=/var/run/medios \
       --switch_user=true \
       --http_executor_domain_sockets=true \
       --authenticate_http_readwrite=true \
       --authenticate_http_readonly=true \
       --http_authenticators=basic \
       --http_credentials=file:///etc/medios/http_credentials.json \
       --authorizer=local \
       --acls=file:///etc/medios/node_acls.json \
       --containerizers=medios \
       --launcher=linux \
       --isolation=cgroups/cpu,cgroups/mem,filesystem/linux,linux/capabilities,linux/nnp,linux/seccomp,namespaces/ipc,namespaces/pid,disk/du \
       --effective_capabilities='{"capabilities":[]}' \
       --bounding_capabilities='{"capabilities":[]}' \
       --fetcher_allow_local_uris=false
     ```
     *(The `environment/secret` isolator is loaded automatically by `MediosContainerizer::create`. If local host artifact staging is used on the node, set `--fetcher_allow_local_uris=true --fetcher_allowed_local_uri_prefix=/var/lib/medios/staging` so `file://` URIs are confined to that canonical directory.)*

---

## 2. Security Requirement Mapping Across Deployment Modes

The table below maps the ten deployment-posture findings from the security review to the concrete Medios configuration and runtime enforcement controls across Modes 1, 2, and 3.

| Issue | Security Requirement | Mode 1 (Laptop) | Mode 2 (Overlay Lab) | Mode 3 (Cloud / Production) Configuration | Product Status |
|---|---|---|---|---|---|
| **#375** (R1), **#376** (R2), **#637** | Control plane authentication & authorization off by default; `permissive=true` ACL default | Off (`127.0.0.1` loopback only) | Optional inside private overlay; principal attribution required (`DR-037`) | Set `--authenticate_http_readwrite=true`, `--authenticate_http_readonly=true`, `--authenticate_http_frameworks=true`, `--http_authenticators=basic`, `--http_framework_authenticators=basic` (or `trusted_header`), `--credentials=file://...`, `--authorizers=local`, and `--acls=file://...` with `"permissive": false` | **Supported today** (Closed) |
| **#378** (R4) | Prevent tasks from executing as unconfined host `root` | Tasks run as the desktop user (`--switch_user=false`) | `--switch_user=true` + dedicated task OS user (`FrameworkInfo.user`) | `--switch_user=true` on `medios-node` + controller `--acls` (`"permissive": false`) with `run_tasks` restricting framework principals to non-root OS users (`"users": {"values": ["medios-task"]}`) + Linux isolators (`linux/capabilities`, `linux/nnp`, `linux/seccomp`, `namespaces/pid`, `namespaces/ipc`) | **Supported today** (Closed) |
| **#379** (R5) | `identity_assertion` trusts self-asserted principal unless transport/HTTP auth is enabled | Accepted on `127.0.0.1` loopback | Accepted across encrypted WireGuard/Tailscale overlay | Enable `--authenticate_http_frameworks=true --http_framework_authenticators=basic` (or `trusted_header` behind an authenticating edge proxy) and mTLS (`LIBPROCESS_SSL_ENABLED=true`, `VERIFY_SERVER_CERT=true`, `REQUIRE_CLIENT_CERT=true`) | **Supported today** (Closed) |
| **#383** (R9) | Sensitive endpoints (`/state`, `/teardown`, `/files/read`, `/files/download`, `/containers`) open by default | Off (`127.0.0.1` loopback only) | Restricted to overlay peers via host firewall | Enable `--authenticate_http_readonly=true --authenticate_http_readwrite=true --http_authenticators=basic --authorizers=local --acls=file://...` on both `medios-controller` and `medios-node` | **Supported today** (Closed) |
| **#386** (R11) | Prevent SSRF to cloud metadata (`169.254.169.254`) or link-local/loopback services via URI & OCI registry fetchers | Cloud metadata & link-local targets blocked by default; `127.0.0.1` allowed for local artifact servers | Cloud metadata & link-local targets blocked by default; `http`/`https`-only libcurl (`#681`) | `uri::validateNetworkTarget` / `uri::validateUrlTarget` always blocks cloud metadata (`169.254.0.0/16`, `100.100.100.200`, `fd00:ec2::254`, `metadata.google.internal`, integer/hex/octal IPv4 encodings) and link-local (`fe80::/10`) across `medios-fetcher`, `CurlFetcherPlugin`, and `DockerFetcherPlugin` (including redirect hops and `WWW-Authenticate` realms); set `MEDIOS_FETCHER_DISALLOW_PRIVATE_IPS=true` in Mode 3 to also block loopback and RFC1918 targets | **Supported today** (Closed) |
| **#396** (R21) | `FilesProcess` default-allows attached paths that were registered without an authorization callback | Off (`127.0.0.1` loopback only) | Restricted to overlay peers via host firewall | Every attached virtual path in `medios-controller` and `medios-node` (including task volume directories via `authorizeSandboxAccess` and `/stores/<name>` via `GET_ENDPOINT_WITH_PATH` / `get_endpoints`) registers an explicit authorization callback, and `FilesProcess::authorize()` fails closed (`403 Forbidden`) for any attached path missing a callback when an authorizer is configured | **Supported today** (Closed) |
| **#399** (R24) | `file://` fetcher (`CopyFetcherPlugin`) copies any host path readable by the task user into the sandbox | Allowed (`--fetcher_allow_local_uris=true`) for local dev artifact staging | Allowed or scoped to a staging directory via `--fetcher_allowed_local_uri_prefix=<dir>` | Set `--fetcher_allow_local_uris=false` on `medios-node` to disable all `file://` and local path fetches, or set `--fetcher_allowed_local_uri_prefix=<dir>` to enforce canonical `realpath()` confinement inside the staging directory | **Supported today** (Closed) |
| **#635** | Prevent a compromised node certificate from spoofing `Libprocess-From: controller@...` to sibling nodes over mTLS | N/A (single process / loopback) | Host firewall restricts node-to-node traffic on `LIBPROCESS_PORT` | Automatically enforced when `LIBPROCESS_SSL_ENABLED=true`, `LIBPROCESS_SSL_REQUIRE_CLIENT_CERT=true`, and `LIBPROCESS_SSL_VERIFY_IPADD=true`: `libprocess` verifies in `parse()` that the sender UPID IP in `Libprocess-From` matches the mTLS-verified peer socket IP | **Supported today** (Closed) |

---

## 3. Example Non-Permissive Mode-3 `acls.json`

```json
{
  "permissive": false,
  "register_nodes": [
    {
      "principals": { "type": "ANY" },
      "nodes": { "type": "ANY" }
    }
  ],
  "register_frameworks": [
    {
      "principals": { "values": ["team:cue", "team:fennel", "team:ray"] },
      "roles": { "type": "ANY" }
    }
  ],
  "run_tasks": [
    {
      "principals": { "values": ["team:cue", "team:fennel", "team:ray"] },
      "users": { "values": ["medios-task"] }
    }
  ],
  "act_for_principals": [
    {
      "principals": { "values": ["team:cue", "team:fennel", "team:ray"] },
      "task_principals": { "type": "ANY" }
    }
  ],
  "view_frameworks": [
    {
      "principals": { "values": ["operator:admin", "team:cue"] },
      "users": { "type": "ANY" }
    }
  ],
  "view_tasks": [
    {
      "principals": { "values": ["operator:admin", "team:cue"] },
      "users": { "type": "ANY" }
    }
  ],
  "view_executors": [
    {
      "principals": { "values": ["operator:admin", "team:cue"] },
      "users": { "type": "ANY" }
    }
  ],
  "view_roles": [
    {
      "principals": { "values": ["operator:admin", "team:cue"] },
      "roles": { "type": "ANY" }
    }
  ],
  "view_flags": [
    {
      "principals": { "values": ["operator:admin", "team:cue"] },
      "flags": { "type": "ANY" }
    }
  ],
  "access_sandboxes": [
    {
      "principals": { "values": ["operator:admin", "team:cue"] },
      "users": { "type": "ANY" }
    }
  ],
  "get_endpoints": [
    {
      "principals": { "values": ["operator:admin", "team:cue"] },
      "paths": { "values": ["/metrics/snapshot", "/containers", "/monitor/statistics", "/stores/tables"] }
    }
  ]
}
```

## See Also

- [TLS.md](TLS.md) — Certificate authority setup, IP/DNS SAN rules, and `LIBPROCESS_SSL_*` reference
- [THIRD_PARTY_LOCK.md](../THIRD_PARTY_LOCK.md) — Immutable dependency SHA/SHA-256 lockfile and CVE audit notes
- [containerizer-fields.md](containerizer-fields.md) — Containerizer isolation fields, `SECRET` environment delivery, and `docker_shell` parameter allowlist
