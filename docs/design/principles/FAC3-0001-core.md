```
CATALOG   FAC3-0001
TITLE     Core Specification: Mesos-AI Thin Fork
EDITION   001
STATUS    Draft
```

## 1. Scope

Create a thin fork of Apache Mesos that preserves original semantics and components required for tier-1 scheduling only.

Target Linux; single-node and small multi-node local clusters for validation.

No language/runtime changes; retain C++/libprocess stack.

## 2. Values (Operationalized)

Minimalism → Keep only controller, node, allocator (DRF), resource offers, command executor, Linux isolation, HTTP Scheduler v1.

Authenticity → No semantic changes; disable features via build profile, not redesign.

Legibility → Small, reviewable diffs; reproducible runs with 3–4 commands.

Scope Control → If not required for the baseline loop, exclude from build.

## 3. In/Out

### 3.1 Included (Tier-1)
- medios-controller, medios-node
- Allocator with DRF
- Resource Offers (roles, attributes, filters)
- Command Executor
- Linux isolation: cgroups/*, namespaces/*
- HTTP Scheduler API v1
- External minimal scheduler client for "hello world" only

### 3.2 Excluded (Phase-0)
- Marathon, Aurora, DC/OS, dashboards, CRDs, operators
- Hadoop/Spark/Jenkins integrations and example frameworks in-tree
- Non-Linux targets; bindings (Java/Python) – disabled at build
- Optional storage/volume/auth plugin sets beyond minimal auth path

## 4. Ontology (Terms)

Resource: a named scalar or range the node's operator declares; cpu, mem, disk and gpus are the usual names, not a bound (FAC3-0028)

Offer: time-bound availability of resources on a node

Task: atomic execution unit

Job: user intent; may map to one or more tasks (handled by client)

Framework: external scheduler using HTTP v1 to convert Job→Task(s)

Policy: DRF + (optional) role quotas; no new policy primitives

## 5. Interfaces

Scheduler ↔ Controller: HTTP v1 (subscribe, receive events, accept offers, launch, acknowledge updates).

Controller ↔ Node: existing Mesos internal protocols (unchanged).

Executor: Command executor only.

## 6. Build Profile (Minimal Linux)

Disable: --disable-java, --disable-python, --disable-tests (or cmake equivalents).

Optional modules/plugins OFF unless required by items in §3.1.

Output artifacts: medios-controller, medios-node, command executor; logs to local filesystem.

## 7. Repository Layout

```bash
mesos-ai/
  fac3/
    FAC3-0001-core.md
    FAC3-0002-minimal-fork.md
    FAC3-0003-hello-world.md
  upstream/mesos/                # git submodule pinned to selected tag
  overlay/
    patches/                     # minimal patchset only if needed
    profiles/minimal-linux/      # build knobs/scripts
  tools/
    hello/                       # external minimal HTTP v1 client (single file)
  README.md
```

## 8. Execution Flow (Baseline Loop)

1. Scheduler SUBSCRIBE to the controller (HTTP v1).
2. Controller emits OFFERS.
3. Scheduler ACCEPT with LAUNCH(TaskInfo) using command executor.
4. Node executes; scheduler receives UPDATE (TASK_FINISHED).
5. Scheduler ACK; terminate.

## 9. Acceptance Criteria (Phase-0)

Hello World: single task prints fixed string and completes via command executor.

Start-to-finish with the controller, node, and a minimal HTTP v1 scheduler client running concurrently; the scheduler client connects to the controller and submits a single command-executor task.

No excluded modules loaded; logs show clean offer→launch→finish path.

## 10. Metrics & Limits

Time-to-first-run (TTV): ≤ 10 minutes from fresh clone on Linux.

p95 offer→accept latency (sim): < 150 ms at 200 queued jobs (no code changes required; spec target).

Overlay diff size: minimal; single-screen reviewable per change set.

## 11. Risks & Mitigations

Dependency rot → Pin submodule to last stable release tag; document build prerequisites.

Scope creep → Changes must reference a FAC3 item; non-referenced changes are out of scope.

Hidden feature coupling → Prefer disabling via build flags; if patching is required, isolate in overlay/patches/ with clear revert path.

## 12. Decision Log (To Be Finalized in FAC3-0002)

Release tag to pin for upstream/mesos.

Exact configure/cmake flags for minimal-linux profile.

Minimal auth path (if any) to keep HTTP v1 working without extra modules.
