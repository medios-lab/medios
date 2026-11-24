# Timeless dynamic reservation in medios: verification of hold-and-release semantics

*medios research track — 2026-06-09. Companion to [offers.md](offers.md);
experiment tracked as #317 (private history),
test landed in #319 (private history).*

## Abstract

medios inherits the full dynamic-reservation machinery from Mesos: RESERVE and
UNRESERVE offer operations, `Resource.ReservationInfo`, allocator-side
reservation tracking, and operator-API endpoints. Inherited code in a thin
fork is present but not necessarily functional; large parts of the upstream
surface are stubbed, and the reservation path had no evidence of ever being
exercised in medios. We test the claim from [offers.md](offers.md) that a
*timeless* reservation — a claim held from now until explicitly released — is
the degenerate case of the interval-offer model and should work end-to-end on
the machinery already in the tree, with no new product code. A two-stage
verification (run the four inherited reservation tests; add one test for the
four properties they leave uncovered) confirms the claim: resources reserved
for a role persist across allocation cycles with no task running, are never
offered to a framework in another role, are visible in the operator state
view, and return to the shared pool on release. The full suite passes 603/603.
The result validates "reservation as a claimed resource" at zero product-code
cost and unblocks the interval lift (lease, expiry, future booking) as a pure
design question.

## 1. Background and hypothesis

[offers.md](offers.md) models an offer as a resource vector over a time
window, with the ordinary scalar offer as the degenerate window
`[t_now, t_now + offer_timeout]`. Under that model a reservation does not add
a time dimension; it makes the window explicit, long, and committed. The
minimal reservation is *timeless*: the window `[now, until-UNRESERVE)`. At
that degenerate point the interval invariant

```
sum(active_reservations_at_t) <= capacity_at_t
```

collapses to the static `reserved + free = capacity`, which is exactly the
reserved-versus-unreserved bucketing the hierarchical allocator already
enforces (`RoleTree::trackReservations`, `reservationScalarQuantities_`,
`hierarchical.cpp:414-458`). If that reading is correct, medios already ships
the timeless reservation and no allocator work is needed to demonstrate it.

The risk is that the reading is correct about the design but wrong about the
tree. medios strips or stubs much of upstream Mesos (DR-007, DR-013), and a
code path can survive a fork as text without surviving as behavior. The same
session that scoped this experiment found inherited tests that had never run
under medios. Presence had to be separated from function.

**Hypothesis.** The dynamic-reservation path in the medios tree is functional
end-to-end. Specifically, with no task ever launched:

- **H1 (hold).** Resources RESERVEd for a role persist across allocation
  cycles as allocator state alone — they do not decay, leak back to the free
  pool, or require a running task to stay claimed.
- **H2 (exclusion).** A framework in a different role is never offered the
  reserved resources, even when the cluster is otherwise idle.
- **H3 (observability).** The held claim is visible to an operator through
  the controller state view.
- **H4 (release).** UNRESERVE returns the resources to the unreserved pool,
  after which a framework in another role receives them.

H1 and H4 together are the hold-and-release contract; H2 is what makes a
reservation a *claim* rather than a preference; H3 is what makes the claim
auditable.

## 2. Experiment

**Design principle.** Smallest probe first. Stage 0 costs one build and runs
tests that already exist; stage 1 adds a single test and no product code. A
failure at either stage names the broken component directly.

**Stage 0 — do the inherited tests pass?** The tree contains four reservation
tests (`ReserveCreateLaunchDestroyUnreserve` and
`ReserveAndUnreserveResourcesV1`, each parameterized pre/post
reservation-refinement). They were built and run on the Linux build host at
master `5b0d21dc3` by the build-dispatch agent.

**Stage 1 — close the coverage gap.** Reading the four tests shows what they
do not establish. The framework-path test exercises reserve and unreserve only
from the reserving framework's own point of view; the operator-API test
asserts only HTTP 202 on the calls and never checks that the reservation took
effect. None of H2 or H3 is covered, and H1 is covered only incidentally. We
added one test, `ControllerTest.ReservationCrossRoleHoldAndRelease`
(`tests/controller_tests.cpp`), against an in-process cluster: one controller,
one node (`cpus:8;mem:2048`), and two frameworks — A in role `alpha`, B in
role `beta`. The choreography:

1. A receives the full node and applies RESERVE `cpus:4` for `alpha`
   (dynamic reservation, principal matching A's).
2. A declines its offers for three consecutive allocation cycles, asserting
   each time that the re-offer still contains the reserved `cpus:4` (H1).
3. The controller `/state` endpoint is asserted to report
   `nodes[0].reserved_resources.alpha.cpus == 4` (H3).
4. A suppresses; B starts and receives offers. B's offer is asserted to
   contain only the unreserved remainder (`cpus == 4`) and no resource
   reserved to `alpha` (H2).
5. B suppresses; A revives, receives the still-held reservation, applies
   UNRESERVE, and suppresses again.
6. B revives and is asserted to receive the full node, all unreserved
   (`cpus:8;mem:2048` allocated to `beta`); `/state` is asserted to show the
   `alpha` reservation gone and `unreserved_resources.cpus == 8` (H4).

Only one framework is eligible for offers at each step
(suppress/revive choreography with queue-drain barriers between phases), so
every assertion is against a deterministic allocation, not a race. The probe
uses `cpus` rather than the issue's `gpu:4`: GPU resources are gated behind a
separate framework capability (`GPU_RESOURCES`, `hierarchical.cpp:3113`) that
is orthogonal to reservation semantics, and any scalar exercises the same
reserve/track/offer/release path.

## 3. Results

All stages pass on the Linux build host (full `ctest` suite, build-dispatch
agent):

| Stage | Test | Result |
|---|---|---|
| 0 | `ReserveCreateLaunchDestroyUnreserve` (pre & post refinement) | pass, 0.19s each |
| 0 | `ReserveAndUnreserveResourcesV1` (pre & post refinement) | pass, 0.15s each |
| 1 | `ReservationCrossRoleHoldAndRelease` (H1–H4) | pass, 0.30s |
| — | full suite at `ac3ccc7ac` | 603/603 |

Secondary findings from the verification:

- **Observability is per-node only.** `/state` and `GET_AGENTS` expose
  reservations on each node, which satisfies H3. The `/roles` endpoint does
  not report reserved-but-unallocated quantities per role, and the
  allocator's aggregate (`reservationScalarQuantities_`) is not surfaced over
  HTTP. An operator can see *where* resources are held but has no single
  cluster-wide reserved-versus-free figure per role.
- **The inherited operator-API test is status-code-only.** It would pass even
  if RESERVE were a silent no-op. Stage 1 closes that hole through the
  framework path; the operator path's effect is still asserted only
  indirectly.

**Threats to validity.** One node, one resource shape, scalar resources only.
The test does not exercise node restart (reservation checkpoint recovery),
controller failover, reservation refinement under hierarchical roles,
principal/ACL enforcement matrices, or concurrent reservation conflicts. It
verifies the contract, not its hardening.

## 4. Conclusion

The hypothesis holds on all four properties. The timeless dynamic reservation
— claim held until released, no time dimension — works end-to-end on
machinery already in the medios tree, at zero product-code cost. This
validates the offers.md framing empirically: the reserved/unreserved
bucketing is the degenerate interval invariant, and medios ships the
degenerate reservation today. The fork's "present but possibly stubbed" risk
was real in general but did not materialize on this path.

## 5. Next steps

1. **Interval lift (v2).** Lease/expiry (auto-UNRESERVE at `t1`) and future
   booking (reserve from future `t0`) are now a design exercise on a verified
   base. Admission against an uncertain future and the
   reserved-but-could-not-be-honored settlement state (offers.md open
   questions) are the substantive design work.
2. **Gang feasibility (#316, private history).**
   The placement simulation is independent of this result but shares the
   research goal: keep the allocator greedy. Its v2 stage composes with
   reservations verified here.
3. **Per-role reserved aggregate.** Small observability lift: surface
   reserved-versus-free per role (the allocator already tracks it). Worth
   doing when an operator-facing consumer exists; not before.
4. **Operator-path effect assertion.** Extend the V1 operator-API test to
   assert the reservation's effect, not just the 202.
5. **SDK surface.** Reservation wire types and a worked example in the Go SDK
   stay out of scope until the protocol shape (including v2) is settled.
