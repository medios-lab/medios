# Multi-framework contention under three subscriber shapes: a simulation

Pilot of a pre-registered design-of-experiments format. Hypotheses were registered in issue #363 (2026-07-07, before any run); numeric thresholds were fixed in the test source prior to first execution and unchanged since.

## Abstract

Independent frameworks can share one controller's offer stream; no test had ever run two concurrently. We drive the real hierarchical DRF allocator with scripted behavior models of common subscriber kinds — a services framework (long-running tasks, suppress-when-satisfied), a batch framework (bursty accept-and-finish), a kernel-per-task framework (small one-cycle stream), and a gang requester — under a paused clock, one pump per allocation cycle. All five pre-registered hypotheses were confirmed: no starvation at 60–70% service occupancy, suppression delivers zero offers, revive re-places within five cycles, decline filters expire rather than starve, and a gang REQUEST grant arrives as one atomic callback that overrides the requester's own decline filters. No product anomalies were observed in these five modeled scenarios. Two harness-bookkeeping defects found during bring-up were caught by the allocator's own invariants — evidence for simulating over real product code rather than mocks.

## 1. Motivation and prior work

Offer sharing across frameworks is designed behavior (DRF; Orthodoxy A). A cluster commonly runs a services framework, a batch framework and a kernel-per-task framework as independent scheduler subscribers, but the allocator suite exercises multi-framework DRF only with idealized mocks. #363 (reframed 2026-07-07) called for simulator-first validation of the sharing dynamics, in the idiom that greenlit gang admission ([gang-placement-sim.md](gang-placement-sim.md) → #323 → #354).

## 2. Hypotheses

Registered qualitatively in #363 before implementation; statistics and thresholds fixed in `tests/sim_framework_contention.cpp` before first execution.

- **H1 (starvation).** H0: a services framework holding ~60% of the cluster starves a bursty batch framework. Statistic: batch placements over 100 cycles and worst queue wait. Reject H0 if placements > 100 and max wait ≤ 10 cycles.
- **H2 (suppression).** H0: a suppressed framework still receives offers. Statistic: offer callbacks to the suppressed framework over 20 cycles. Reject H0 if zero.
- **H3 (revive latency).** H0: after a task death and revive, re-placement is unbounded. Statistic: cycles from kill to full re-placement. Reject H0 if ≤ 5.
- **H4 (filter expiry).** H0: refuse filters persist past their duration (a declining framework is permanently starved). Statistic: placement cycle of a framework declining with 5s filters until cycle 20. Reject H0 if placed by cycle 30.
- **H5 (gang atomicity under contention).** H0: a gang REQUEST grant is split across callbacks, or blocked by the requester's decline filters, under concurrent batch traffic. Statistic: post-request callbacks to the gang framework and member count. Reject H0 if exactly one callback carrying all three members.

## 3. Design of experiments

| Factor | Levels exercised |
|---|---|
| Cluster size | 2, 4, 5, 6, 10 nodes (8 cpus, 16384 mem each) |
| Service occupancy | 0%, ~50%, ~60%, ~70% |
| Framework mix | services+batch, services+kernels, picky+batch, batch+gang |
| Task lifetime | 1, 2, 3 cycles, and unbounded (services) |
| Refuse filter | none, 5s, 1000s |

One scenario per hypothesis; cells chosen to put each hypothesis under its adversarial condition rather than a full factorial. Held constant: single role per framework, no quota or weights, homogeneous nodes, allocation interval 1s, single trajectory per scenario (see §7).

## 4. Apparatus

`ContentionSim` (`tests/sim_framework_contention.cpp`, binary `sim_framework_contention`) drives the real `HierarchicalDRFAllocator` — not a mock — under `Clock::pause()`, one `pump()` per allocation cycle (advance + settle + drain). The harness mirrors the controller's bookkeeping: held shapes transition offered→allocated at launch; finishes recover allocated resources; remainders are declined with optional filters. Offer-callback granularity is recorded at the `offerCallback` boundary itself — the same boundary `Controller::offer` converts into a single OFFERS event — making it the correct atomicity observable for H5.

Model/world boundary: framework behavior is modeled (scripted policies); allocator behavior is real. The wire layer (HTTP scheduler API, SDK) is outside the apparatus entirely.

## 5. Results

All hypotheses' H0 rejected; the designed behavior held.

| H | Verdict | Evidence |
|---|---|---|
| H1 | **Confirmed** (H0 rejected) | `ServicesDoNotStarveBatch`: >100 placements, max wait ≤ 10 (assertions at sim_framework_contention.cpp:502,506) |
| H2 | **Confirmed** | `SuppressReviveInterplay`: zero callbacks to the suppressed framework across 20 cycles (:540) |
| H3 | **Confirmed** | re-placement within 5 cycles of kill (:559-560) |
| H4 | **Confirmed** | `DeclineFiltersExpireNotStarve`: placed by cycle 30 (:587,592); kernels variant `KernelChurnUnderServiceLoad`: >40 placements, wait ≤ 5 under 70% load (:620,622) |
| H5 | **Confirmed** | `GangGrantAtomicUnderContention`: exactly one post-request callback, three members (:696-697), with the requester's 1000s decline filters active on every node — locking the #354 semantic that an explicit REQUEST overrides the requester's own declines |

Verdicts are reported against thresholds; observed values (e.g. the actual max wait) are not extractable from the current harness, which exposes assertion booleans only — the gap #365 closes.

## 6. Deviations and negative results

- **H5's scenario was redesigned after a failed premise.** As first written, the test assumed the gang framework's only callback would be the grant; in fact any active framework receives cycle offers (15 callbacks observed). The replacement silences cycle traffic via the framework's own decline filters and proves that premise before relying on it (5 asserted-quiet cycles). The redesign strengthened the verifier: it added the filters-override semantic to what H5 locks. Thresholds were not changed.
- **Two harness defects during bring-up** (not product): (1) finished tasks were recovered as allocated without the offered→allocated transition the controller performs at launch — the allocator's CHECK at hierarchical.cpp:1914 rejected it; (2) `launch()` validated shapes against the original offer rather than the remaining balance, permitting over-commit — rejected at hierarchical.cpp:1948. Both fixed in the harness; both classes remain guarded by the allocator's own invariants.

## 7. Threats to validity

- **Single trajectory.** The allocator shuffles node order (`randomEngine`); each scenario ran one seed. Claims are existence proofs on one trajectory, not distributions. Replication via seed sweeps is #365.
- **Scenario-bounded.** No quota, weights, reservations, heterogeneous nodes, or role hierarchies; single-digit framework counts. "No product anomalies" means: none observed in these five modeled scenarios.
- **Model fidelity.** Framework policies are stylized (e.g. batch resubmits instantly). Wire-level behavior is invisible to this apparatus by construction.

## 8. Reproducibility

At medios `8043d4edf` (the #364 squash):

```
cmake -B build -G Ninja -DMEDIOS_BUILD_TESTS=ON
ninja -C build -j 1 sim_framework_contention
./build/tests/sim_framework_contention
```

Output: `[  PASSED  ] 5 tests.` in ~30ms total (macOS arm64; deterministic under paused clock, single built-in seed). Full command/output evidence: the evidence comment on #364.

## References

#363 (pre-registration), #364 (implementation + evidence), #354/#323 (gang admission), [gang-placement-sim.md](gang-placement-sim.md), [reservation-mvp.md](reservation-mvp.md), #365 (results convention).
