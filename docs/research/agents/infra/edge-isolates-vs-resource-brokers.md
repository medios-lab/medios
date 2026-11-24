# Agent infrastructure substrates: edge isolates and resource brokers

*medios research track — 2026-06-10. Positioning note; companion to
[offers.md](../../offers.md), [reservation-mvp.md](../../reservation-mvp.md),
and [gang-placement-sim.md](../../gang-placement-sim.md).*

## The question

Every team building agent infrastructure ships the harness its substrate
makes easy. Cloudflare's agents stack is the clearest articulated example
of one substrate; medios is the clearest example we control of another.
This note maps the two against each other to answer: are these substrates
competing for the same workload, and if not, where is the seam?

## Substrate 1: the edge isolate

Cloudflare's agent primitive is the durable object: an addressable class
instance that is created once for a given identity, receives every future
request and connection for that identity, persists its state (embedded
SQLite, object storage for large files), hibernates when idle, and wakes on
demand. On top of it sit an agents SDK (scheduling, callables, resumable
streaming, multi-client sync) and dynamic workers: V8 isolates spun up from
a string of code --- including LLM-generated code --- that start with zero
capability (no network, no environment, no ambient APIs) and receive only
the capabilities explicitly injected from outside. Their "code mode"
direction follows from this: instead of exposing thousands of tool
definitions to a model, expose a small programmable API surface and let the
model write code against it, executed in the capability-scoped sandbox.
This is the code-as-action result (CodeAct [1]) applied at the
infrastructure layer.

The economics of the unit matter more than the features: an isolate costs
milliseconds to create and effectively nothing to hold. The substrate's
compute unit is not scarce. Stateful-serverless platforms (Cloudburst [2])
and the serverless research agenda (Berkeley view [3]) identified state and
communication as the missing primitives of FaaS; durable objects answer
that critique by making identity-plus-state the unit of deployment.

## Substrate 2: the resource broker

medios's primitive is the offer: a controller brokers scarce, rivalrous
hardware (GPUs, memory, whole nodes) to frameworks that own placement
policy (FAC3-0014). The unit of compute is expensive to hold, contended,
and physical. Everything hard in the medios research track exists because
of that property: reservations (a held claim must exclude other claimants
--- verified end-to-end in [reservation-mvp.md](../../reservation-mvp.md)),
gang placement (joint feasibility collapses multiplicatively with gang size
--- measured in [gang-placement-sim.md](../../gang-placement-sim.md)), and
the interval lift (committing future capacity against an uncertain future,
[offers.md](../../offers.md)). The GPU-datacenter scheduling literature
[4][5] is the academic home of these problems; gang scheduling itself
predates all of it [6].

## Primitive correspondences

The two substrates independently converged on similar shapes where their
problems overlap:

| Edge-isolate primitive | Resource-broker primitive | Shared idea |
|---|---|---|
| Durable object: identity persists until torn down | Timeless reservation: claim persists until UNRESERVE | Held-until-released is the base primitive for long-lived agents |
| Dynamic worker: zero capability, explicit grants | Isolators as guardrails, identity-assertion + pluggable auth | Build security up from nothing, not down from a perimeter |
| Code mode: small programmable surface, model writes code | Offer model: small brokered surface, framework computes policy | The platform exposes a small surface; the client writes the composition (code, or placement policy) |
| Hibernate / wake on demand | Suppress / revive offer flow | Idle participants must cost nothing and must not starve others |

The correspondence in the first row is exact in structure: a durable object
is a degenerate reservation over compute identity (no time dimension, held
until released), just as the medios timeless reservation is the degenerate
interval over capacity. Both stacks chose the degenerate case first; medios
has the additional obligation of the interval lift because its resource is
scarce enough to book.

## What each substrate gets to skip

The deepest difference is not a feature but an absence.

The edge-isolate substrate never answers "who gets the resource when two
agents want it." There is no allocator, no offer cycle, no preemption
story, no feasibility cliff --- isolates are abundant, so scheduling
reduces to cron plus hibernation. Every hard question in the medios track
is a question that substrate is allowed to skip.

The resource broker, symmetrically, never answers session questions:
resumable streams, multi-client sync, client reconnection, per-user
addressing. medios deliberately pushes those downstream (no web UI, no
operator dashboards, API-first); the edge substrate makes exactly those
properties free.

Neither absence is a defect. The edge substrate owns session state; the
broker owns capacity under contention. Neither owns the other's problem.

## The composed architecture

The two compose along an arrow the edge-substrate teams already draw
themselves: when an edge agent needs real compute, it delegates --- to a
container, a sandbox provider, a browser farm. That delegate slot is where
a resource broker belongs.

In medios terms: an edge-resident agent loop is the natural shape of a
medios *framework*. The agent loop owns policy --- what to run, when, with
what budget. The broker owns the capacity it dispatches onto: reserve the
GPUs (timeless today, interval later), place the gang (greedy, anchored on
the bottleneck member), run the tasks, settle the grant. The split is
FAC3-0014 drawn from the other end: their "nexus that connects to
everything" and our "pure scheduler, framework owns policy" describe the
same boundary from opposite sides.

This composition is also the argument for the medios research track's
ordering. The primitives an edge-resident framework would need from the
broker are exactly the ones under verification: a claim it can hold while
its work is bursty (#317, verified), gang placement that does not require a
global solver (#316, confirmed greedy-viable), and eventually windows it
can book ahead (the v2 interval lift). If agentic workloads continue moving
toward GPU-heavy gangs, the edge substrates need something shaped like
medios on the far end of their delegate arrow.

## Implications for medios

1. **The reservation primitive is the integration point.** In the
   composed architecture, an external agent loop holds a medios
   reservation while idle and releases it on demand. The #317 test
   verifies exactly that sequence: held across allocation cycles with no
   task, excluded from other roles, returned to the pool on UNRESERVE.
2. **Token-efficient surface beats wide surface.** The code-as-action
   result [1] --- 30% fewer steps than tool-call enumeration --- and its
   infrastructure form (code mode) support the SDK direction already
   chosen: a small hand-written Go SDK with worked examples over a broad
   generated API.
3. **Do not compete on session plumbing.** Sync, resumability, and client
   addressing come free on the edge substrate and are medios non-goals
   (no web UI, no dashboards, API-first). Keep the boundary as drawn.
4. **The scarcity questions are the work.** Allocation under contention,
   gang feasibility, interval admission, and settlement do not arise on
   substrates where compute is abundant. The next concrete pieces are
   already scoped: the v0.5 rack-by-type axis in the gang sim (separate
   anchor ordering from demand ordering) and the v2 interval lift
   (lease/expiry, future booking) on the verified reservation primitive.

## References

[1] X. Wang et al., "Executable Code Actions Elicit Better LLM Agents,"
arXiv:2402.01030, 2024. https://arxiv.org/abs/2402.01030

[2] V. Sreekanti et al., "Cloudburst: Stateful Functions-as-a-Service,"
arXiv:2001.04592, 2020. https://arxiv.org/abs/2001.04592

[3] E. Jonas et al., "Cloud Programming Simplified: A Berkeley View on
Serverless Computing," arXiv:1902.03383, 2019.
https://arxiv.org/abs/1902.03383

[4] W. Gao et al., "Deep Learning Workload Scheduling in GPU Datacenters:
Taxonomy, Challenges and Vision," arXiv:2205.11913, 2022.
https://arxiv.org/abs/2205.11913

[5] A. Qiao et al., "Pollux: Co-adaptive Cluster Scheduling for
Goodput-Optimized Deep Learning," arXiv:2008.12260, OSDI 2021.
https://arxiv.org/abs/2008.12260

[6] J. K. Ousterhout, "Scheduling Techniques for Concurrent Systems,"
ICDCS, 1982.

[7] L. Wang et al., "A Survey on Large Language Model based Autonomous
Agents," arXiv:2308.11432, 2023. https://arxiv.org/abs/2308.11432

[8] B. Hindman et al., "Mesos: A Platform for Fine-Grained Resource
Sharing in the Data Center," NSDI 2011.

[9] A. Ghodsi et al., "Dominant Resource Fairness: Fair Allocation of
Multiple Resource Types," NSDI 2011.

[10] Cloudflare, "Agents SDK" and "Code Mode" public documentation,
https://developers.cloudflare.com/agents/ and
https://blog.cloudflare.com/code-mode/.
