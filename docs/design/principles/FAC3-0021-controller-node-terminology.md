```
CATALOG   FAC3-0021
TITLE     Controller / Node Terminology
EDITION   001
STATUS    Accepted
```

## Decision

Use **controller** for the cluster coordinator and **node** for the
machine-side daemon in Medios product vocabulary.

The intended component names are:

- `medios-controller`
- `medios-node`

Use `controller process` and `node process` or `node daemon` when prose
needs to distinguish a running process from a physical or virtual machine.

## Rationale

The controller tracks cluster state, receives framework calls, generates
resource offers, coordinates task launches through nodes, and reconciles
lifecycle state. `controller` describes that role without implying that the
component owns framework scheduling policy.

The node represents a machine-side process that advertises resources, launches
executors and tasks, reports status updates, and participates in recovery.
`node` is neutral infrastructure vocabulary and aligns with modern cluster
language.

This project should avoid `agent` for the machine-side process. In 2026, and
especially in a project named Medios, `agent` reads as an AI-capable actor.
Reserve it for future agentic software components if the product grows in that
direction.

This project should also avoid making `worker` the core product noun. `worker`
is widely used in distributed compute systems, but it is generic and can carry
labor-hierarchy overtones. It is acceptable as informal explanatory language
only when paired with `node`, for example "worker node" in comparative docs.

## Alternatives Considered

### Controller / Worker

Accurate for orchestration, familiar from Kubernetes and Ray-style cluster
language. Rejected as the primary product pair because `worker` is generic and
less neutral than `node`.

### Controller / Agent

Architecturally reasonable in some AI products, but rejected because `agent`
should remain available for future agentic runtime concepts.

### ResourceManager / NodeManager

Closest to Hadoop YARN and very explicit about cluster resource management.
Rejected for product vocabulary because it is verbose and `ResourceManager`
sounds narrower than the Mesos controller role.

### Server / Client

Used successfully by Nomad. Rejected because `client` conflicts with external
frameworks and API clients in Mesos.

### Leader / Follower

Useful only inside HA or consensus discussions. Rejected for component names
because Mesos nodes are not followers in a consensus group.

### Control Plane / Data Plane

Useful as architecture language. Rejected as component names because Medios
needs concrete binaries, docs, and code symbols.

## Consequences

- New user-facing docs should use `controller` and `node`.
- New binary, script, and config names should prefer `controller` and `node`.
- Do not add compatibility aliases for removed product vocabulary unless an
  external wire contract requires them.
- Code migration should be staged and reviewable. Public API and wire-format
  changes require explicit senior review before implementation.
- `scheduler` remains reserved for Mesos frameworks. Do not rename the
  controller to scheduler.
- `agent` remains reserved for future AI/agentic concepts.
