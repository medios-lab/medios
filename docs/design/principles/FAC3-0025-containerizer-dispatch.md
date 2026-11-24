# FAC3-0025 — Containerizer Dispatch: One Per Cluster, No Composition

**Status**: Accepted (§5.1 — lock in single-containerizer-per-cluster). Decision owner 2026-05-24.
**Filed**: 2026-05-24
**Investigation requested in**: #208 workstream 1.

> **Decision framing (owner, 2026-05-24)**: a medios cluster is a *trusted governance zone*. The containerizer is a property of the zone, not of the task. Heterogeneous runtimes belong to separate zones (separate clusters / separate node pools), not to a composing dispatcher inside one node. This matches the medios thesis of single-tenant or trusted-tenant deployments (see README "What is intentionally not here") and the FAC3-0014 pure-scheduler model: the scheduler decides which workload runs where; the cluster is the unit of runtime homogeneity.

---

## §1. Question

What does the `--containerizers` flag actually do today, and is composition supported?

Posed because PR #205 (closed not-accepted) introduced node-side logic that *assumed* a multi-containerizer dispatch model, and the project did not have a definitive answer to point at.

---

## §2. Findings (read from the code, not from memory)

### §2.1. Multi-containerizer is already forbidden

`src/node/containerizer/containerizer.cpp:281-285`:

```cpp
if (containerizerTypes.size() != 1) {
  return Error(
      "Multiple containerizers not supported in medios: '" +
      flags.containerizers + "'");
}
```

This gate landed in commit `b83ac518b1b6` (PR #150, DR-030 base, 2026-05-18). It is intentional.

Implication: `--containerizers=mesos,docker_shell` and any other comma-delimited combination is rejected at node startup. There is no `ComposingContainerizer`, no dispatch ordering, no fallback chain. Exactly one containerizer instance runs per node.

The flag's comma-list shape is upstream vestige; the values are constrained to a 1-element list.

### §2.2. Two valid values

`mesos` and `docker_shell`. Anything else returns:

```
Unsupported --containerizers='<value>'. Supported: 'mesos', 'docker_shell' (DR-030)
```

(`containerizer.cpp:295-298`.)

### §2.3. Default is `mesos`

`src/node/flags.cpp:`:
```
add(&Flags::containerizers, "containerizers", "...", "mesos");
```

The flag's help text is stale upstream prose:
- claims options are `mesos` and `docker` (the latter does not exist in this fork)
- claims "the order they are specified is the order they are tried" (multi-containerizer is forbidden, ordering does not apply)

### §2.4. Per-containerizer task-shape contract

**`MesosContainerizer::launch`** (`src/node/containerizer/mesos/containerizer.cpp:1069-1080`) **accepts**:
- TaskInfo with no `container` field (bare-shell case — falls through to the default command executor)
- TaskInfo with `container.type = MESOS`

…and **returns `NOT_SUPPORTED`** for:
- TaskInfo with `container.type = DOCKER` (or any other non-MESOS type)

**`DockerShellContainerizer::launch`** (`src/node/containerizer/docker_shell/docker_shell_containerizer.cpp:92-100`) **accepts**:
- TaskInfo with `container.type = DOCKER`

…and **returns `NOT_SUPPORTED`** for:
- TaskInfo with no `container` field
- TaskInfo with `container.type != DOCKER`

### §2.5. NOT_SUPPORTED surface

Until PR #209 (closing #200), NOT_SUPPORTED was routed to `TASK_FAILED` with null `message`/`reason` (silent failure). PR #209 fixes this to `TASK_ERROR` + `REASON_TASK_INVALID` + structured message. This means: any unsupported task shape now produces a diagnostic the framework can act on, regardless of which containerizer is configured.

### §2.6. No wire-level "mesos" in containerizer naming

The `mesos` literal appears in:
- `flags.containerizers` value (CLI surface)
- `MesosContainerizer` C++ class names
- `src/node/containerizer/mesos/` directory tree
- `CGROUP_SEPARATOR` (`src/node/containerizer/mesos/constants.hpp:26`, value `"mesos"`)
- `ContainerInfo.type = MESOS` (protobuf enum, in `include/mesos/mesos.proto`)

The protobuf enum `ContainerInfo.Type.MESOS` is wire-level — frameworks send this value. Renaming it is a wire break for any framework with hardcoded `MESOS` in its TaskInfo construction. The other sites are all internal.

### §2.7. No test coverage for multi-containerizer

`git grep` for `containerizers=mesos,` and `containerizers=docker_shell,` against `tests/` returns zero matches. This is consistent with §2.1 — the case is rejected at startup and is not a valid configuration to test.

---

## §3. Behavior matrix

| `--containerizers=` | TaskInfo `container` | Result |
|---|---|---|
| `mesos`        | absent          | Runs via default command executor |
| `mesos`        | `type=MESOS`    | Runs via in-tree containerizer |
| `mesos`        | `type=DOCKER`   | `TASK_ERROR` + `REASON_TASK_INVALID` (post-PR #209) |
| `docker_shell` | absent          | `TASK_ERROR` + `REASON_TASK_INVALID` (post-PR #209) |
| `docker_shell` | `type=DOCKER`   | Runs via `medios-docker-executor` |
| `docker_shell` | `type=MESOS`    | `TASK_ERROR` + `REASON_TASK_INVALID` (post-PR #209) |
| `mesos,docker_shell` (any combo) | (any) | Node fails to start: "Multiple containerizers not supported in medios" |

This is the complete dispatch surface today. Two valid container modes, plus the bare-shell case on the `mesos` path.

---

## §4. Design implications

### §4.1. The `--containerizers` flag is structurally an enum, not a list

The flag's tokenized-list shape is upstream vestige. Today it accepts exactly one of two values. A cleaner surface would be `--containerizer=medios` (singular, enum-shaped). Renaming is non-trivial because it touches the flag name itself, not just values, and any scheduler / operator script using the current name breaks.

### §4.2. Containerizer choice is per-node, not per-task

Schedulers do not select which containerizer runs their task. Submission shape determines viability (`TaskInfo.container` shape must match the node's configured containerizer, or `TASK_ERROR`). This puts the burden on the framework to know what node it's targeting — or to handle `TASK_ERROR` gracefully and retry against a different node.

### §4.3. Adding a third containerizer (e.g. firecracker, CRI-shim, k8s)

The current shape easily admits a third option. It does NOT admit composing them on one node — that's the §2.1 gate. The composition question would re-surface if a use case demanded mixed workloads on one node (e.g. Docker tasks AND VM tasks on the same machine).

---

## §5. Open question for the owner — RESOLVED 2026-05-24

**Resolved: §5.1 — Lock in single-containerizer-per-cluster.** Governance framing: the cluster is a trusted governance zone; the containerizer is a property of the zone, not of the task. Mixed-runtime needs are solved by running separate clusters / separate node pools, not by composing inside a node.

Original framing of the question and the alternatives evaluated:

**Is "one containerizer per node, no composition" the durable design, or a temporary simplification adopted at DR-030 to keep the surface small?**

Three positions considered:

### §5.1. **Position A — Lock in single-containerizer.** This is the design.

Pros:
- Smaller surface to reason about. The dispatch matrix in §3 is the whole truth.
- Operator config is simpler: pick one, run it.
- Code: removes the comma-list parsing, removes the `containerizerTypes.size() != 1` gate, replaces `--containerizers` with `--containerizer` (singular enum).
- STYLE.md §3.1 violations (re-deriving dispatch from flag strings, as in PR #205) become structurally impossible — there is no dispatch table to re-derive.

Cons:
- Cluster operators with heterogeneous workloads must run separate node pools per containerizer.
- If a future runtime (firecracker for VMs, CRI for k8s interop) is needed alongside Docker on the same node, this position must reverse — and the reversal is a flag-name break.

Locks in: a thesis-level statement that medios's resource model is "the node IS the container runtime"; mixing runtimes is the operator's problem to solve at the node-pool layer.

### §5.2. **Position B — Keep the door open.**

Don't actively lock in. Keep the comma-list shape; keep the gate as "currently rejects >1 but the shape can be relaxed without breaking the wire."

Pros:
- Cheap optionality.
- If the multi-runtime case becomes real, the surface is already shaped right.

Cons:
- Confusing today — the flag pretends to be a list but isn't. Operators read upstream Mesos docs and form wrong expectations.
- STYLE.md §3.1 violations remain structurally possible: anyone writing dispatch logic against `flags.containerizers` is implicitly assuming a composition model that doesn't exist.
- Tech debt: stale flag help text, vestigial parsing code.

This is the status quo today. It is the most expensive position to leave undisturbed.

### §5.3. **Position C — Implement composition properly.**

Reverse §2.1. Add a `ComposingContainerizer` (upstream Mesos has one — `src/slave/containerizer/composing.cpp` in the original tree) that dispatches per task based on container type, falling through `NOT_SUPPORTED` to the next containerizer in the list.

Pros:
- Operator gets a single node pool that handles any task shape.
- The flag's documented semantics match its parsing.

Cons:
- Significant code addition (upstream composing.cpp is ~600 lines).
- Adds a real dispatch decision the validation layer (now per-containerizer) must respect — STYLE.md §3.1 returns as a live concern.
- Goes against medios's "smaller than upstream" thesis (DR-019, FAC3-0022 dependency parsimony).
- The Position-A simplification is more defensible until a real workload demands otherwise.

This was upstream Mesos's choice. It is not currently a medios commitment.

---

## §6. Decision (was: Recommendation)

**§5.1 — Lock in single-containerizer-per-cluster.** Accepted 2026-05-24.

Implementation work (these become commits / PRs):

1. **Flag surface**: drop the `flags.containerizers` comma-list parsing; replace the flag with `--containerizer` (singular, enum-shaped: `medios` | `docker_shell`).
2. **Gate removal**: delete the `containerizerTypes.size() != 1` gate (no longer reachable once the flag is singular).
3. **Help text**: rewrite to name only the two enum values, drop the stale "order they are specified is order tried" upstream prose.
4. **Docs**: `docs/architecture.md` "Containerizers" section reframed: "Each medios cluster runs exactly one containerizer. Choice is a property of the cluster, not the task. Heterogeneous-runtime needs are solved by running multiple clusters."

Sequencing:

- This work happens *after* #208 workstream 2 (the `mesos` → `medios` rename). Doing both in one PR conflates a rename with a flag-shape change.
- Workstream 2 PR is therefore: `--containerizers=mesos` → `--containerizers=medios` (still comma-list-shaped, still parsed the same way, gate still in place).
- A later, separate PR collapses `--containerizers` → `--containerizer` and removes the gate.
- Workstream 3 (testing) follows the flag-shape change so tests use the final surface.

Implication for STYLE.md: once the singular-enum landing is done, STYLE.md §3.1 (don't re-derive dispatch from configuration) becomes structurally enforceable for this surface — there is no dispatch table to re-derive when the flag is a 2-value enum.

---

## §7. Effect on workstream 3 (testing)

The multi-containerizer matrix in the #208 issue body collapses. Only single-containerizer × task-shape combinations remain — essentially what `tests/e2e/test_docker_shell.cpp` already covers, plus the symmetric coverage for `--containerizer=medios` paths:

- `--containerizer=medios` + bare-shell task → `TASK_FINISHED` (default command executor accepts no-`container` shape).
- `--containerizer=medios` + `container.type=MESOS` task → `TASK_FINISHED` (in-tree containerizer).
- `--containerizer=medios` + `container.type=DOCKER` task → `TASK_ERROR` + `REASON_TASK_INVALID` (post-PR #209).
- `--containerizer=docker_shell` matrix — already covered.

---

## §8. What this ADR does not decide

- The `mesos` → `medios` rename mechanics (#208 workstream 2).
- The proto-level `ContainerInfo.Type.MESOS` enum value — that is a separate wire-compat conversation.
- Whether `docker_shell` itself is the right long-term name (vs. `docker_cli`, `docker_direct`, or just `docker`).
- Whether the bare-shell case under `--containerizer=medios` is in or out of contract long-term.
