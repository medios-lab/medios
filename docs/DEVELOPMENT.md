# Development

How to build, run, and test medios-ai locally and on CI.

## Prerequisites

- **Operating system**: Linux/POSIX (per DR-020). Native paths supported and verified: Linux (x86_64), macOS (arm64, Apple Silicon). Docker-on-Mac per DR-003 is also supported and is the recommended path for cross-platform consistency.
- **CMake**: 3.24 or later. `OVERRIDE_FIND_PACKAGE` is a 3.24 feature (BUILD-010). Ubuntu 22.04 ships 3.22 and won't work without manual install; Ubuntu 24.04 ships 3.28.
- **Compiler**: g++ 13+ or clang 18+ (C++20 required). Apple Clang from Xcode 15+ works.
- **Ninja**: 1.10 or later.
- **OpenSSL 3** development files: the node's fetcher downloads https URIs through the vendored cURL, which links the system OpenSSL. `libssl-dev` on Ubuntu, `openssl@3` from Homebrew on macOS. Configure fails on any other major version; Homebrew links OpenSSL 4 by default, so on macOS add `-DOPENSSL_ROOT_DIR=$(brew --prefix openssl@3)`. Hosts that run a release need the OpenSSL 3 runtime library.
- **ccache** (optional, recommended): wraps the compiler for fast incremental rebuilds.

On macOS: `brew install cmake ninja ccache openssl@3`.

## Local build

```
# On macOS also pass -DOPENSSL_ROOT_DIR=$(brew --prefix openssl@3).
cmake -B build -G Ninja \
  -DCMAKE_BUILD_TYPE=RelWithDebInfo \
  -DMEDIOS_BUILD_TESTS=ON \
  -DCMAKE_C_COMPILER_LAUNCHER=ccache \
  -DCMAKE_CXX_COMPILER_LAUNCHER=ccache
cmake --build build -j <N>
```

Add `-DMEDIOS_ENABLE_SSL=ON` for a build in which the controller, its nodes
and the executors and frameworks that connect to them can use mutual TLS;
see [TLS.md](TLS.md) for the OpenSSL prerequisite and the runtime
variables.

> **Fast local rebuild iteration (`#333`):** Always pass `-DCMAKE_C_COMPILER_LAUNCHER=ccache -DCMAKE_CXX_COMPILER_LAUNCHER=ccache` when configuring local build trees so incremental rebuilds hit the compiler cache, and keep `FETCHCONTENT_UPDATES_DISCONNECTED=ON` (default in `cmake/Dependencies.cmake`) so populated vendored dependencies are not re-fetched on reconfigure.

### Picking `-j`

The build is parallelism-limited at low `-j`; pick the largest `N` your RAM allows. Each C++ TU peaks around 1-2 GB.

| Machine | Recommended `-j` |
|---|---|
| 16 GB Mac (M1/M2/M3) | `-j 2` |
| 32 GB Linux or Mac | `-j 4` to `-j 8` |
| Build farm / 64 GB+ | `-j $(nproc)` |

At `-j 2` a cold build is ~13 minutes on Apple Silicon; at `-j 8` on a 32-core build farm it's under 4 minutes. With ccache populated, incremental builds touching only medios-ai source compile in seconds.

### Verifying ccache is actually hitting

```
ccache -z                      # zero stats
cmake --build build -j 2       # build
ccache -s                      # show hits / misses
```

A warm rebuild after `ccache -z` should show `Hits` near the TU count and `Misses` near zero. If `Cacheable calls` is zero, the launcher flags didn't take — re-run `cmake -B build ...` with the `_LAUNCHER` flags set.

`-DMEDIOS_BUILD_TESTS=OFF` skips GoogleTest fetch and the test targets if you only need the binaries.

## Running a cluster

Two terminals, both at the repo root.

Terminal 1 (controller):

```
./build/src/controller/medios-controller \
  --ip=127.0.0.1 --port=5050 \
  --registry=in_memory \
  --work_dir=/tmp/medios-controller
```

Terminal 2 (node with the medios containerizer):

```
./build/src/node/medios-node \
  --controller=127.0.0.1:5050 \
  --ip=127.0.0.1 --port=5051 \
  --work_dir=/tmp/medios-node \
  --containerizers=medios \
  --launcher=posix \
  --isolation= \
  --launcher_dir=$(pwd)/build/src \
  --resources='cpus:2;mem:1024;disk:1024'
```

With the flags above a STORE volume is a symlink at one name in the sandbox, made by `filesystem/posix`, the filesystem isolator the containerizer runs when `--isolation` names none. A link cannot make a store read-only, so every store in this node's `--stores` file declares `readers: 0` (the node refuses to start otherwise), and a task asks for it `RW` at a relative container path such as `tables`. An absolute container path or a read-only store needs `--launcher=linux --isolation=filesystem/linux` (root on Linux), which binds the store in a private mount namespace.

`--launcher_dir` is where the node finds the default command executor binary (`medios-executor`). Required for any task using `command` without an explicit `executor` — the README example, `medios-execute`, and most simple HTTP frameworks all go through that path.

For a Docker-task node, swap the containerizer args:

```
./build/src/node/medios-node \
  --controller=127.0.0.1:5050 \
  --ip=127.0.0.1 --port=5051 \
  --work_dir=/tmp/medios-node \
  --containerizers=docker_shell \
  --launcher_dir=$(pwd)/build/src \
  --resources='cpus:2;mem:1024;disk:1024'
```

`--launcher_dir` tells the node where to find `medios-docker-executor`. A reachable Docker daemon is required (Docker Desktop, Colima, OrbStack, or Linux dockerd).

Launch a task once the cluster is up:

```
./build/src/medios-execute \
  --controller=127.0.0.1:5050 \
  --name=hello \
  --command='echo hello-medios'
```

Expected status sequence: `TASK_STARTING → TASK_RUNNING → TASK_FINISHED`, then exit code 0.

Task `stdout` and `stderr` are captured under the node work directory. The host
path layout is documented in [Task Sandbox Layout](executor-http-api.md#task-sandbox-layout).
Medios writes these sandbox log files directly through the default sandbox
container logger. It does not ship an in-tree log rotation logger; retention,
rotation, and collection should be configured in the host, container runtime, or
platform logging pipeline.

**macOS hostname caveat.** If `medios-execute` exits immediately with `Failed to obtain the IP address for '<hostname>'; the DNS service may not be able to resolve it`, your hostname isn't resolvable via DNS or `/etc/hosts`. This is the default state on most Macs that have never been put into a managed domain. The workaround is to set `LIBPROCESS_IP=127.0.0.1`:

```
LIBPROCESS_IP=127.0.0.1 ./build/src/medios-execute \
  --controller=127.0.0.1:5050 \
  --name=hello \
  --command='echo hello-medios'
```

libprocess (which `medios-execute` and the daemons use for their own listening sockets) resolves the host hostname at startup. Pure-HTTP framework clients are unaffected; they only need to reach the controller's IP.

## Running tests

```
ctest --test-dir build --output-on-failure
```

Tests live in `tests/`:
- `tests/smoke/test_controller_smoke.cpp` — controller subprocess starts, serves `/version` (BUILD-007)
- `tests/e2e/test_hello_world.cpp` — controller + node + medios-execute end-to-end (BUILD-006)

Add a single test:

```
ctest --test-dir build -R HelloWorld --output-on-failure
```

## OpenAPI

The OpenAPI specs live in `api/openapi/`.

They are split by daemon because the controller and node intentionally expose
overlapping paths such as `/api/v1`, `/state`, `/flags`, and `/health`.
`controller.openapi.json` documents controller-hosted routes, including the
scheduler API, and `node.openapi.json` documents node-hosted routes, including
executor and resource provider APIs.

Both specs also include shared libprocess/Medios utility routes such as
`/files/*`, `/help`, `/logging/toggle`, `/metrics/snapshot`, `/version`, and
profiling/system endpoints.

### Runtime discovery (#261)

Both specs are embedded into the daemon binaries at build time and served at
standard discovery paths on the running daemon:

```
curl http://<controller-host>:5050/openapi.json
curl http://<controller-host>:5050/.well-known/openapi.json
curl http://<node-host>:5151/openapi.json
curl http://<node-host>:5151/.well-known/openapi.json
```

The `/.well-known/openapi.json` form follows IETF RFC 8615; the unprefixed form
matches the de facto convention used by most OpenAPI tooling. Both return the
same content with `Content-Type: application/openapi+json`.

OpenAPI validation is intentionally not part of the default local CMake build;
see [FAC3-0024](design/principles/FAC3-0024-openapi-validation-ci.md).

CI validation is tracked in #202. When editing these specs, run the same pinned
containerized linter command used by that workflow. The command should lint both
daemon specs and use a pinned Redocly CLI image or digest, not `latest`.

This first slice keeps the descriptions hand-authored. Route/protobuf drift
checks and generated ProtoJSON schemas should be added with the eventual
OpenAPI generation toolchain.

## Docker-on-Mac (per DR-003)

The repo ships a `Dockerfile` (Ubuntu 24.04 LTS + build toolchain) and `docker-compose.yml` (controller + node services networked). The source tree is mounted from the host; build artifacts persist.

One-shot bring-up:

```
docker compose build              # one-time image build
docker compose up                 # controller + node, networked
docker compose up controller      # controller alone (for log watching)
docker compose down               # tear down
```

Inside the dev container:

```
docker compose run --rm controller bash
# now at /workspace prompt
cmake -B build -G Ninja -DMEDIOS_BUILD_TESTS=ON
cmake --build build
```

**Caveat per DR-003**: cgroups inside Docker-on-Mac (Linux VM inside macOS) are lossy. Sufficient to prove the API loop end-to-end; not production-fidelity for isolation testing. Isolation work needs a Linux host.

## CI

Two GitHub Actions workflows run on GitHub-hosted runners:

- `.github/workflows/build-test.yml` — `g++ / cmake build + ctest`, a full
  RelWithDebInfo build with tests on, then `ctest`. Runs on pull requests and
  pushes to master. Skipped while the repository is private.
- `.github/workflows/cmake-configure.yml` — configure-only on g++ and
  clang++, on a schedule and on manual dispatch.

Vendored dep artifacts are cached in `build/_deps` keyed on `cmake/Dependencies.cmake` hash (BUILD-009, BUILD-011), and build-test.yml keeps ccache across runs. Both caches are saved even when a build fails (`if: always()`), so iteration cost is bounded.

## Architecture decisions referenced above

- **DR-003** (#4) — Docker on Mac → remote Linux dev path
- **DR-008** (#9) — Authenticator/Authorizer abstract; concrete impls stub
- **DR-010** (#11) — In-memory registrar default
- **DR-020** (#35) — Linux/POSIX only
- **BUILD-008** (#72) — SHA pinning + URL hashes
- **BUILD-009** (#73) — Vendored dep cache in CI
- **BUILD-010** (#76) — `OVERRIDE_FIND_PACKAGE` on boost + protobuf

## Common issues

**Configure fails with "FetchContent: failed to download"**: network. Retry. If it persists, check `cmake/Dependencies.cmake` pinned SHAs against upstream availability.

**Build OOM-kills on low-RAM machine**: drop parallelism (`cmake --build build -j 2`) or build on a larger machine. 8 GB Mac is not sufficient for `-j 8` parallel C++20.

**Node fails to register**: check the controller is actually listening on the address the node was given (`MEDIOS_CONTROLLER`). Default is `127.0.0.1:5050`. If running in Docker compose, the controller hostname is `controller` (see `docker-compose.yml`).

**`medios-execute` hangs at "Subscribed with framework ID"**: no offer received. Check the node started and registered (look at its logs). Check resources advertised (`--resources` flag) match the task's requirements (`medios-execute` default cpus:1;mem:128).
