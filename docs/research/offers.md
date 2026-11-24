# Offers

Two layers. The first decides whether a thing is a resource at all. The second
decides the shape of the grant. Reservation and gang are shapes, not separate
categories.

## Layer 1 — is the noun offerable

Properties a thing must have for medios to broker it as a resource.

1. **Scarce.** Not everyone can consume unlimited amounts at once.
2. **Rivalrous.** If one task consumes it, another task has less access to it.
3. **Allocatable.** The system can grant some quantity of it to a framework/task.
4. **Measurable.** The system can observe usage well enough to account for it.
5. **Enforceable.** The system can stop, throttle, revoke, or penalize overuse.
6. **Valuable to plan around.** Knowing the grant changes what a framework can
   decide to do.
7. **Settled.** Afterward, the system can say whether the grant was used well,
   unused, overused, released, violated, or preempted.

This layer is where soft resources get stressed. Token / spend offers pass 1, 2,
3, 6, 7 but are weak on 4 (usage is reported late and lossy) and 5 (medios can
gate the next grant but can't revoke an in-flight API call). The weakness on 4
and 5 is exactly why such resources lean on 7 — see
[FAC3-0026](../design/principles/FAC3-0026-offered-accepted-consumed.md).

## Layer 2 — what shape the grant takes

The noun being boring (a GPU passes all 7 trivially) does not make the offer
easy. Gang and reservation are hard because of the relationship between grants,
not the resource. The offer is one object; shape is a set of axes on it:

- **instantaneous vs interval** — a point in time, or a window
- **scalar/vector vs atomic set** — one grant, or N that stand or fall together
- **independent vs topological** — a quantity, or a placement relationship
  (NVLink domain, rack; and, in the time domain, an inter-window relationship)
- **best-effort vs committed** — revocable on pressure, or promised
- **revocable vs non-revocable** — can the held grant be preempted
- **splittable vs all-or-nothing**

## The model: an offer is a resource vector over a time window

A scalar offer is a resource vector at `t_now`:

```
{ gpu: 8, mem: 256GB }
```

A reservation offer is the same vector lifted into the time domain:

```
{ gpu: 8, mem: 256GB } over [10:00, 14:00)
```

The extra dimension is not metadata. It changes the contract: the framework is
no longer accepting available capacity, it is committing to a durable claim over
an interval. So the progression is scalar → vector → interval-valued vector, and
gang composes on top:

```
gang offer:             atomic set of resource vectors
reservation offer:      resource vector over a time window
gang reservation offer: atomic set of resource vectors over a shared/related
                        time window
```

```
Offer =
  resource identity
  resource vector
  shape
  time window          (always present; see below)
  commitment / settlement semantics
```

**The window is always present, scalar is the degenerate case.** A normal Mesos
offer already has a time window — it is valid until rescinded or `offer_timeout`.
That window is just short and implicit. A scalar offer is an interval offer whose
window is `[t_now, t_now + offer_timeout]`. Reservations do not add time; they
make a dimension that was always there explicit, long, and committed.
`offer_timeout` is the degenerate reservation medios already ships.

**The invariant survives the lift.** At any instant `t`:

```
sum(active_reservations_at_t) <= cluster_capacity_at_t
```

So the allocator stays a bin-packer. It can plan, compact, fragment, preempt,
and settle — the object being packed is now an interval-valued vector instead of
a point vector. medios does not need a new allocator paradigm; it lifts the
existing one into N+1 dimensions.

**Token offers in the time domain.** `$500 over [10:00, 14:00)` is a rate. A rate
is more enforceable than an instantaneous budget — you can throttle to hold the
line. The same lift that makes GPU reservations first-class makes spend more
measurable and enforceable, not less. The interval axis serves both ends.

> Reservations lift offers from points to intervals. Everything else is what
> shape you give the interval.

## Open questions the time dimension imports

- **Admission against an uncertain future.** Granting `[10:00, 14:00)` commits
  capacity that has not happened yet, against a `capacity_at_t` that is an
  estimate for future `t` (nodes fail). "Committed vs best-effort" is therefore
  not only a framework preference — it is the allocator promising against an
  estimate. Settlement needs a terminal state for *reserved-but-could-not-be-
  honored*.
- **Interval packing is harder than point packing.** Greedy point-in-time DRF is
  easy because it does not plan ahead. Compacting reservations across time is
  calendar defragmentation (multi-dimensional bin-packing with time — NP-hard).
  The likely medios-correct stance: offer interval-vectors and enforce the
  invariant, but stay greedy / first-fit and let frameworks negotiate windows,
  rather than putting an optimal temporal packer in the platform. That keeps the
  pure-scheduler split ([FAC3-0014](../design/principles/FAC3-0014-pure-scheduler.md)).
- **Gang reservation is an inter-window constraint, not one window.** "Shared"
  covers rendezvous (equal windows); pipelines want precedence (stage A `[10,12)`
  before stage B `[12,14)`). The atomic set carries a constraint over its
  windows, which is topology in time.

## Gang feasibility: the bottleneck member

A gang reservation is feasible only if all member reservations can be satisfied
jointly under their topology and timing constraints. Feasibility is a
conjunction over members, and the effective scheduling cost is dominated by the
hardest member — the one with the longest, scarcest, or least-movable window.

Consequences:

- **Conjunction makes large gangs fragile.** Infeasibility compounds
  multiplicatively. If each member is independently ~90% placeable under its
  constraints, a 64-member gang is `0.9^64` ≈ 0.1% feasible. The member *count*
  is itself a feasibility cliff, not just a cost. This is the formal reason gang
  scheduling deadlocks and starves.
- **The real metric is the size of the feasible set.** "Longest / scarcest /
  least-movable" are proxies for the member with the fewest viable
  `(placement × window)` options. Movability is slack — how many options remain.
  The dominant member is the one with the smallest feasible set.
- **Two cost types, not one.** *Scarce / immovable* drives **placement** cost
  (can it be satisfied at all, now). *Longest window* drives **calendar** cost
  (how much future it blocks and how badly it fragments compaction). A long hold
  on a common resource is a calendar problem, not a placement one.
- **Anchor on the bottleneck — this is why the NP-hard optimal pack is
  avoidable.** Schedule the smallest-feasible-set member first (most-constrained-
  first / fail-fast), then fit the rest around the anchor. Once the hard member
  is placed, the gang collapses toward near-independent placements. Concrete
  algorithm for the greedy stance above: rank members by feasible-set size, place
  ascending, backtrack only on the anchor.
- **The protocol pivots on the binding member.** medios surfaces the bottleneck
  member's feasible windows; the framework picks; medios fills the rest. The
  platform exposes the binding constraint, the framework decides — FAC3-0014
  intact.
- **The hardest member is also the preemption pivot.** Least-movable means most
  expensive to re-place, so evicting it kills the gang (it cannot reform). A gang
  reservation's revocability is the revocability of its hardest member;
  committed-vs-best-effort for the gang reduces to that one member.

The compression: **a gang reservation is its hardest member.** Feasibility, cost,
scheduling order, and revocability all collapse onto the member with the smallest
feasible set.

See also: [FAC3-0026 — offered, accepted, consumed](../design/principles/FAC3-0026-offered-accepted-consumed.md),
RFC-0001 — The Computer (#291, private history).
