# Architecture

Two daemons, two HTTP APIs. That's the platform.

## Daemons

`medios-controller` is the cluster coordinator. It receives node registrations, generates resource offers, accepts framework launch decisions, and tracks task lifecycle.

`medios-node` is the machine-side daemon. It advertises available resources, launches executors, and reports task status.

A single controller plus one or more nodes is a complete cluster. For automatic failover, controllers can contend for leadership through an etcd v3 election (below); ZooKeeper-based election and the replicated log remain out.

### Leader election (etcd)

Start two or more controllers with the same `--etcd=etcd://host:port/<election-name>` URL: they campaign through etcd's election service on a short-TTL lease, one wins, and the rest stand by. Nodes and driver-based schedulers point at the same URL (`--controller=etcd://...`) and follow the leader. When the leader's lease lapses — crash, partition, shutdown — a standby's campaign wins and the cluster reconnects to it; a deposed leader that cannot renew its lease exits rather than split-brain.

Election coordinates leadership only; it does not replicate state. The new leader starts from its own registry (empty, or its local `leveldb`) and rebuilds live state through the restart contract below — node reregistration and framework resubscription carry the running tasks and reservations. The etcd endpoint is spoken over its HTTP/JSON gateway; no gRPC dependency.

### Controller restart and persistence

A controller restart is a reconnection flow, not a recovery-of-state flow (DR-010). What survives a restart:

- **Running tasks** — nodes reregister with the new controller instance and report their tasks; frameworks resubscribe. Verified by `tests/controller_restart_tests.cpp`.
- **Dynamic reservations** — checkpointed on the node, re-reported at reregistration, and re-offered to the role.
- **Registry state** (node admissions, quota, weights, maintenance schedules) — only with `--registry=leveldb`, which persists under `<work_dir>/registry-leveldb`. The default `in_memory` registry starts empty on every boot.

What does not survive, by design: outstanding offers — including gang grants (#323). Frameworks re-request; a framework that recovers by re-requesting and resuming from its own checkpoint relies on exactly this.

Single-controller durability via `leveldb` plus a supervisor (systemd restart, cloud instance auto-restart) is the simplest deployment posture; standby controllers behind an etcd election (above) add automatic failover on top of the same contract. Custom election schemes plug in via the `--controller_contender`/`--controller_detector` module seam.

## Scheduler API

`/api/v1/scheduler` is the HTTP scheduler API. Frameworks subscribe, receive `OFFERS` events, and respond with `ACCEPT` or `DECLINE`. Documented in [scheduler-http-api.md](scheduler-http-api.md).

A reference implementation in under 100 lines of Python is sketched in [design/principles/FAC3-0014-pure-scheduler.md](design/principles/FAC3-0014-pure-scheduler.md).

The C++ driver-based scheduler API (`MediosSchedulerDriver`) is also supported per DR-028 — both the HTTP API and the driver wrap the same wire protocol.

For LLM agents writing frameworks against medios, see [../AGENTS.md](../AGENTS.md).

## Operator API

`/api/v1/operator` is limited to `GET_STATE`, `GET_NODES`, `GET_FRAMEWORKS`, and a small essential subset. The full upstream operator API is intentionally not implemented; downstream operator tooling builds externally on this minimal surface. Documented in [operator-http-api.md](operator-http-api.md).

## Containerizers

Two containerizer paths ship:

- `--containerizers=medios`: OCI image execution via the in-tree Medios containerizer. The provisioner pulls from Docker registries directly. No `dockerd` install required on nodes.
- `--containerizers=docker_shell` (per DR-030): forks `medios-docker-executor` which shells out to the `docker` CLI (`docker run`, `docker stop`, `docker kill`). Works under any Docker-compatible backend the host provides (Docker Desktop, Colima, OrbStack, Linux dockerd). The contract is "a working `docker` CLI with a reachable daemon" rather than coupling to a specific backend. Requires `TaskInfo.container.type = DOCKER`; tasks without a container are out of contract.

The image format is shared; the task envelope is not. A scheduler submits the container shape the node's containerizer accepts (`DOCKER` for `docker_shell`, `MEDIOS` for `medios`; FAC3-0025), so it has to know which one a node runs.

The `DOCKER` container type is valid on plain `LAUNCH` tasks only. Task groups (`LAUNCH_GROUP`) run under the default executor, which supports nested `MEDIOS` containers exclusively; the controller rejects a task group carrying `type = DOCKER` at validation with `TASK_ERROR` + `REASON_TASK_GROUP_INVALID`.

When a `TaskInfo` does not match any configured containerizer's contract, the node rejects it with `TASK_ERROR` + `TaskStatus::REASON_TASK_INVALID` and a status message naming the configured containerizers. There is no silent fallback; the framework gets a diagnostic, not a `TASK_FAILED` with null fields.

### Volumes (`docker_shell`)

`docker_shell` honors `ContainerInfo.volumes` for `STORE` volumes only (DR-036): `source.store.name` names a store the node declared in its stores file (`--stores`), `container_path` is an absolute path or one name inside the sandbox (such as `tables`), and `mode` is `RW` (default) or `RO`. The executor resolves the name to the declared path and passes `--volume <path>:<container_path>[:ro]`, or `--volume <path>:<sandbox>/<container_path>[:ro]` for a name inside the sandbox; a task never names a path on a node. A task finds a store named inside the sandbox at `$MEDIOS_SANDBOX/<container_path>`. The sandbox is always bind-mounted automatically; store volumes are additive.

Out of contract under `docker_shell`: every other volume source (`HOST_PATH`, the bare `host_path` field, named docker volumes, sandbox paths, secrets, CSI), `Volume.image` (image-as-volume), a store the node does not declare, a relative container path other than one name (`data/tables`, `.`, `..`), a name inside the sandbox when `MEDIOS_DIRECTORY` is unset, and a name the fetcher already wrote a file at. Each rejects the task at executor launch with `TASK_ERROR` + `REASON_TASK_INVALID` + a message naming the violating entry. The controller refuses the same sources and path forms before they reach a node; the last two only the node can see.

### Volumes (`medios`)

The medios containerizer honors the same `STORE` volumes and refuses the same other sources. How it delivers one depends on its filesystem isolator:

- `filesystem/linux` under `--launcher=linux` binds the store in the container's private mount namespace, at an absolute path or at one name inside the sandbox, read-only when the volume asks for `RO`.
- `filesystem/posix` under `--launcher=posix`, the default filesystem isolator when `--isolation` names none, makes a symlink at one name inside the sandbox, pointing where the store's declared path resolved when the node started. It refuses an absolute container path and a read-only volume, since a link cannot make a store read-only, and the node refuses to start with a store declaring `readers` above 0. A declared path that resolves somewhere else at launch is refused; a container recovered with a link to the old place is stopped with `TASK_FAILED`. The link is removed when the container is.

Either way the launcher places the store after the fetcher has run and refuses the launch if the fetcher left something at the name. A node whose launcher and filesystem isolator are the other pairing delivers no store and refuses a store task before launching it.

## Isolators

cgroups (v1 + v2) and the Nvidia GPU isolator ship per DR-027. Other upstream isolators (cpushare, port mapping, mem soft limit, etc.) are stripped — the design assumes single-tenant or trusted-tenant workloads, so fine-grained fairness primitives aren't load-bearing.

## Process limits (POSIX rlimits)

POSIX `setrlimit`-style per-process limits are a kept launch surface, orthogonal to cgroups. cgroups limit aggregate resource usage across a container; rlimits constrain individual process behavior at exec time (file size, CPU seconds, core dumps, open file descriptors, locked memory, stack size).

The contract is in the protobuf surface as `ContainerInfo.rlimit_info` (v0 and v1 `medios.proto`). Frameworks set it directly on the task's container info, or end users pass it through `medios-execute --rlimits=<JSON>`.

Supported limit types reflect the platform's `<sys/resource.h>` definitions. Always-available limits: `RLMT_AS`, `RLMT_CORE`, `RLMT_CPU`, `RLMT_DATA`, `RLMT_FSIZE`, `RLMT_NOFILE`, `RLMT_STACK`. Platform-conditional: `RLMT_MEMLOCK`, `RLMT_NPROC`, `RLMT_RSS`, `RLMT_LOCKS`, `RLMT_MSGQUEUE`, `RLMT_NICE`, `RLMT_RTPRIO`, `RLMT_RTTIME`, `RLMT_SIGPENDING`. Unknown values return an explicit error; invalid soft > hard is rejected.

Implementation: `src/posix/rlimits.{hpp,cpp}` provides `rlimits::convert()`, `rlimits::set()`, and `rlimits::get()`. Launch flow applies limits via `installResourceLimits()` immediately before `execvp`; any failure surfaces as a clear launch error.

## Vocabulary

Medios uses `controller` for the cluster coordinator and `node` for the machine-side daemon. The word `agent` is reserved for AI-agentic concepts: LLM-driven schedulers, autonomous frameworks, agent-orchestrated workloads. See [design/principles/FAC3-0021-controller-node-terminology.md](design/principles/FAC3-0021-controller-node-terminology.md).

## References

- [FAC3-0001](design/principles/FAC3-0001-core.md) — core scope and tier-1 inclusion list
- [FAC3-0014](design/principles/FAC3-0014-pure-scheduler.md) — pure-scheduler thesis
- [FAC3-0017](design/principles/FAC3-0017-offer-model.md) — the resource-offer model
- [FAC3-0021](design/principles/FAC3-0021-controller-node-terminology.md) — vocabulary
- GitHub issues, label `decision record` — locked architectural decisions
