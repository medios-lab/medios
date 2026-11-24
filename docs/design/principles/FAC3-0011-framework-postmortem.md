```
CATALOG   FAC3-0011
TITLE     Mesos App Framework Development Guide — Gaps to Exploit
EDITION   001
STATUS    Draft
```

## What the guide optimizes for (as-written)

Scheduler + executor model with default executor and task groups (since 1.2), nested containers under an executor. Good isolation model; heavy cognitive load.

v1 HTTP Scheduler API is the recommended interface (stable from Mesos 1.0, introduced 0.24). Split history (driver APIs vs v1) increases decision surface for new frameworks.

Roles, quota, reservations documented, but quota semantics focus on minimum guarantees; practical guidance on fair-share + quotas interplay is scattered.

HA & reconciliation are documented at API level (operation feedback, UPDATE_OPERATION_STATUS), but no minimal patterns for small frameworks.

## Lost opportunities (technical)

**No canonical "single-file" reference framework.**
The guide points to examples and RENDLER, but lacks a one-page v1 HTTP scheduler that demonstrates SUBSCRIBE → OFFER → ACCEPT/LAUNCH → UPDATE → ACK with comments. This raised the perceived bar to "use Marathon/Aurora."

**Late, split abstraction for schedulers.**
The coexistence of language drivers and v1 HTTP for too long created ambiguity about "the right" API; many teams avoided writing frameworks at all. A "v1-only" starter path + deprecation ladder was missing.

**Executor-first complexity.**
Default executor + task groups + nested containers are powerful but heavy for lightweight batch. A minimal "command-only" path (no custom executor) should have been front-and-center with perf/limits called out.

**Quotas/roles lacked operational recipes.**
Docs state behavior; they don't ship prescriptive templates (e.g., "two-tenant DRF with quota floors + preemption policy X"). This blocked practical multi-tenant adoption without a platform layer.

**No topology/placement guidance for accelerators.**
The framework guide abstracts resources well, but never evolves into concrete placement recipes (PCIe domains, NUMA hints, device traits). This left "GPU-aware" as a framework-specific dark art.

**Insufficient "hello cluster" dev loop.**
There's no sanctioned micro-cluster harness + fixture offers + deterministic simulator to test schedulers locally. Teams jumped straight to real clusters or heavyweight stacks.

**Operation status feedback underused.**
The v1 id-ed operations + UPDATE_OPERATION_STATUS stream are documented, but there's no minimal retry/backoff/reference state machine in the guide—leading to ad-hoc error handling.

## Actionable deltas for Medios (thin fork, strictly within original semantics)

**Ship the missing single:** a ~150-line v1 scheduler (external to tree) demonstrating the end-to-end lifecycle with comments and a replayable transcript. Uses command executor only.

**Declare v1-only path:** document one blessed interface (HTTP v1), with a short note acknowledging legacy drivers but not recommending them.

**Recipe pack:** 3 prescriptive mini-docs:
- Two-tenant DRF + quota floors (API calls + expected allocator behavior).
- Preemption-ready batch (priority, preemptible flag convention, update handling).
- Command-only executor patterns (limits, when to move to custom executor).

**Local harness:** a deterministic offer generator + log validator so a scheduler can run offline tests before touching a cluster.

## Non-goals (to stay authentic)

No new scheduler semantics, no new resource types, no API forks. Only documentation, harness, and examples that sit beside the original guide.