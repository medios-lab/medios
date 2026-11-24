```
CATALOG   FAC3-0012
TITLE     Framework Epistemology: Why No One Built Small Ones
EDITION   001
STATUS    Draft
```

## Core Paradox

Mesos promised "bring your own framework" but shipped examples that looked like platforms.
Result: teams assumed framework = heavy investment, chose Marathon/Aurora instead.
The guide taught completeness, not composition.

## Ontological Mismatch

Original Mesos ontology: Resources are fungible scalars (cpu/mem/disk).
AI training reality: Resources have topology, affinity, temporal patterns.
Gap: No trait-based placement primitives in base ontology.

## Epistemological Questions for Medios

### 1. Resource Truth
Do we treat GPU as scalar (original ontology) or admit device traits as first-class?
If traits, how minimal? (pcie_domain only? or mig_profile, interconnect, etc?)

### 2. Framework Scope
Is a framework a scheduler (pure planning) or scheduler+executor (planning+runtime)?
For AI: can we get away with command executor only, or do checkpoint/preempt require custom?

### 3. Time Semantics
Original: offers expire, tasks run to completion.
AI training: jobs have deadlines, preemption is routine.
Do we annotate (attributes) or extend (new primitives)?

### 4. Fairness Unit
DRF assumes resource bundles are comparable.
AI: GPU-hours matter more than CPU-hours.
Do we weight resources in DRF or keep uniform and handle in framework?

### 5. Evidence Surface
What's the minimal telemetry every framework must emit so a human can audit fairness?
Is this enforced by controller or convention?

## Critical Decision — resolved (FAC3-0028, 2026-09-16)

A. **Orthodox:** GPU is a scalar, all intelligence in framework, no new primitives.
B. **Pragmatic:** GPU traits as attributes, placement hints, but no API changes.
C. **Heretical:** Admit time and topology as first-class dimensions (breaks authenticity).

Taken: A, with the judgment supplied by the operator rather than encoded in medios. The operator declares each resource name, count and access mode; medios checks the declaration at start and enforces it at run time. Topology and device traits stay with the operating agent and the framework.
