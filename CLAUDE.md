# CLAUDE.md

Project-level instructions for Claude Code on `medios`.

## What Medios is

A thin fork of Apache Mesos restructured for agentic workloads. Target deployment: H100 cluster scheduling AI/agentic compute. Most of the upstream Mesos surface is preserved as interface; concrete implementations are stripped or stubbed.

The fork's first job is to be the substrate for one demonstration: an LLM agent declares resource needs, the broker schedules without human-in-the-loop provisioning.

## Phases

- **PHASE-A** — build infrastructure + first runnable target. Closed at `v0.1.0-runnable`.
- **PHASE-B** (#40) — test safety net against runnable baseline.
- **PHASE-C** (#41) — parked. Its mandate, DR-019, was not taken (revised 2026-09-15); disposition pending.

Tip moves; `git log --oneline -10` for current state.

## Directory map

- `include/mesos/` — public Mesos headers and protobuf API surfaces. Higher blast radius than `src/`.
- `src/` — implementation. Kept subsystems are interface-preserved.
- `stout/`, `libprocess/` — Mesos's own libraries, kept in-tree (DR-005; DR-019 not taken).
- `docs/design/principles/` — `FAC3-*` design principles. Authoritative product direction.
- `docs/` — HTTP API docs + design records.
- `cmake/` — `Dependencies.cmake` (vendored FetchContent deps, SHA-pinned).
- `tests/` — smoke + e2e tests gating the runnable surface.
- `.github/workflows/` — CI workflows.

## Design records that bind code decisions

| DR | Topic | Implication |
|---|---|---|
| DR-007 (#8) | Lineage preservation | Keep interfaces and call sites; stub the impls. |
| DR-013 (#14) | Test suite | Upstream tests stripped wholesale. Targeted edits in strip PRs are a judgment call. |
| DR-018 (#19) | Docs | `docs/` winnowed. Fresh docs only; CHANGELOG starts at 0.1. |
| DR-019 (#20) | `stout` + `libprocess` | Not taken (2026-09-15). Both stay in-tree. Interfaces typed in them are stable surfaces. |
| DR-021 (#36) | Docker | Keep OCI image format; strip dockerd. |
| DR-022 (#85) | PR-event CI | Superseded: build-test.yml runs on pull requests once the repository is public. |

`gh issue list --label "decision record"` is the authoritative list.

## Terminology — FAC3-0021

- `controller` for the cluster coordinator
- `node` for the machine-side daemon
- `agent` is reserved for AI/agentic concepts. Never use `agent` to mean a node-side daemon.

## Architectural principles

- **Orthodoxy A** — preserve Mesos resource-offer semantics. Don't redesign scheduler primitives.
- **In-place fork, modern CMake, Docker-first** (DR-001/002/003).
- **Stub-the-impls** — when stripping, stub rather than excise (DR-007).
- **Dependency parsimony** (FAC3-0022) — no new runtime deps without explicit approval. Vendored deps go through `cmake/Dependencies.cmake` with SHA pinning.
- **Isolation = agentic guardrails**, not multi-tenant fairness.

## Working discipline

### Branch-state preflight

Before any non-trivial change, run:

```
git rev-parse --abbrev-ref HEAD
git log --oneline @{upstream}..HEAD
git status --short
```

### Anti-workaround

If a failure reveals a missing upstream lifecycle (broken handshake, dangling reference, silent-no-op), stop and write the design seam before coding. Workarounds that hide a broken lifecycle compound — future iterations build on the lie. Examples to refuse on sight: disabling auth in a fixture to make a test pass, special-casing a code path to bypass a missing component, returning local success without performing the documented operation.

### No fallbacks

Required identity / config / state must be explicit. Missing required state fails loudly with a specific reason. Do not invent fallback values. Fallbacks let misconfigured callers succeed silently; the cost surfaces later, somewhere else, with no log line tying back.

## Coding rules

- C++20. Two-space indent. Wrapped function signatures. Explicit namespace blocks. Local `using` declarations near includes.
- Preserve Apache license headers.
- `.proto` changes are protocol changes. Escalate unless explicitly assigned.
- `stout` and `libprocess` types are the project's types; use them where the surrounding code does. Stdlib replacements only as a bounded, reviewed change, never as a wholesale port (DR-019 not taken).
- No new runtime dependencies without explicit approval.
- Comments sparse; explain non-obvious constraints, not behavior. Don't reference task/fix/callers in comments.

## Commit message convention

```
[<scope>] <Short imperative>

<Body — rationale, references to DR / issue, behavior notes.>

Co-Authored-By: <name> <email>
```

Scope prefixes: `[build]`, `[strip]`, `[ci]`, `[docs]`, `[cleanup]`, `[BUILD-NNN]`. Body should reference the issue number and any binding DRs.

## Build + test workflow

- Local on Mac: `cmake -B build -G Ninja -DMEDIOS_BUILD_TESTS=ON && cmake --build build && ctest --test-dir build --output-on-failure`.
- CI: `.github/workflows/build-test.yml` builds and runs ctest on pull requests and pushes to master, on a GitHub-hosted runner; it is skipped while the repository is private. `.github/workflows/cmake-configure.yml` runs a configure-only check on a schedule and on manual dispatch.
- Evidence a merge carries: the author builds the change and runs the affected tests on a Linux host and states the commit and the counts in the pull request; an independent reviewer reads the diff.

## Strip plan

Reorganizes medios by removing upstream code the agentic workload doesn't need. Tracked as STRIP-NNN issues.

## What is intentionally NOT in this file

- Per-session task state — transient.
- Deployment configuration, credentials, machine IDs, secrets.
