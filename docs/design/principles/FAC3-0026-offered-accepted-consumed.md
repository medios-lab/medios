# FAC3-0026 — Medios Manages Offered, Accepted, Consumed; Not Actuals

**Status**: Draft (filed 2026-05-28). Pending review.

> **One-line statement.** Medios is in the *offered → accepted → consumed* business. It is not in the *actuals* business. The instant a framework accepts an offer carrying a resource, that resource is "consumed" from the scheduler's books. What actually happened downstream — under-utilization, API spend drift, a crashed executor, a refunded API call — is observability and devops, not scheduler input.

---

## §1. Statement

A resource has three states in the medios scheduler:

1. **Offered.** The controller has advertised availability of some quantity of the resource to a framework in an offer.
2. **Accepted.** The framework has accepted the offer. The scheduler decrements its pool by the offered amount.
3. **Consumed.** Permanent record. The decrement is final. The scheduler does not reconcile this number against any external truth.

There is no fourth state called *actual*. There is no `UPDATE_USAGE` call that closes a loop between what was offered and what was used. There is no refund. There is no overrun. The scheduler keeps one set of books, and those books reflect what was *offered and accepted*, not what reality observed elsewhere.

This applies uniformly to every resource kind medios brokers:

- **Reusable resources** (CPU, memory, disk, ports, GPU). When a task ends, the offered quantity returns to the pool. The scheduler does not check whether the task actually used 2 CPUs or 1.5 — that is between the kernel, the cgroup isolator, and the operator's metrics pipeline.
- **Consumable resources** (tokens, API spend, request quota). When a task is launched, the offered quantity is decremented from the pool and does not return. The scheduler does not check whether the API call actually cost $0.18 instead of the $0.25 declared — that is between the worker, the provider's billing system, and the operator's spend dashboard.

The asymmetry between reusable and consumable resources is in their *return-to-pool* lifecycle, not in their offered/accepted/consumed lifecycle. Both are brokered identically by the offer loop.

---

## §2. Why

### §2.1. One set of books

A scheduler that maintains two views of resource state — "what I offered" and "what was actually used" — is a scheduler that has to reconcile them. Reconciliation is open-ended work: when does a discrepancy matter, when is it noise, who fixes it, what is the controller authoritative about when they diverge. Each answer creates a configuration surface, a policy hook, a debugging burden.

A scheduler that maintains one view — "what I offered and what was accepted" — is bounded. The math is closed. Discrepancies between books and reality are someone else's problem; the controller never sees them.

This is what classical Mesos already does for CPU. A task asks for 2 CPUs, the scheduler decrements 2 from the pool, the task may use 0.3 or burst to 4 (modulo isolation policy) — Mesos does not care. The kernel and the isolator care. The operator's metrics pipeline cares. The scheduler does not.

FAC3-0026 generalizes the same posture so it covers tokens, API spend, and any other resource a Resource Provider may advertise.

### §2.2. Closes the door on a class of features

The principle explicitly forecloses:

- Usage-based billing inside the scheduler.
- Automatic refunds of declared-but-unused budget.
- Drift alerting between declared and actual usage.
- Cost-per-task accounting on the controller.
- Anything that requires the controller to learn what "really" happened.

All of these are legitimate concerns. None of them are scheduler concerns. They live in observability tools (Prometheus, provider billing dashboards, cost-allocation pipelines) that read from logs, traces, and provider APIs — not from the controller's books.

Refusing these features in the controller is what keeps the controller small, fast, and operable. The same posture that motivated the strip manifest (DR-005 through DR-022, FAC3-0022 dependency parsimony) applies here: every concern not stripped from the controller becomes a maintenance liability.

### §2.3. NOP — not our problem

The internal vocabulary is *NOP — not our problem*. When a feature proposal arrives that requires the controller to know something about what happened downstream of a task launch, the first response is "NOP." The proposer's options are: (a) build the feature outside the controller against the data sources that already exist, or (b) make a positive case that the feature genuinely cannot work outside the controller. Most proposals dissolve at (a).

---

## §3. Mechanism for consumables

This principle would be empty if there were no way to model consumable resources in the offered/accepted/consumed loop. There is: the **Resource Provider API**, defined in `include/mesos/resource_provider/resource_provider.proto`, preserved in medios through the strip (STRIP-007, STRIP-008, STRIP-014) interface-only.

A Resource Provider is an external process that subscribes to the controller and sends `Call::UPDATE_STATE` carrying the resources it provides, point-in-time. The controller offers those resources to frameworks via the standard offer cycle, allocates them via DRF, decrements on accept. The provider's internal logic — refill, billing top-up, calendar reset, manual operator adjustment — is invisible to the controller. The provider sends the next `UPDATE_STATE` when its state changes; the controller updates its view; the loop continues.

This is the *"new machine joining the cluster"* analogy stated precisely: a Resource Provider advertises what exists right now, the controller takes the advertisement at face value, no questions asked about how that state came to be.

Wire-level evidence:

- `ResourceProviderInfo` (`mesos.proto:1117`): identifies a provider.
- `Resource.resource_provider_id` (`mesos.proto:1242`, semantics at `mesos.proto:2629-2638`): a resource without a `node_id` but with a `resource_provider_id` is a provider-scoped resource — cluster-global by construction, not tied to any node.
- `Call::UPDATE_STATE` (`resource_provider.proto:142-163`): provider-to-controller state advertisement, with `resource_version_uuid` for staleness detection.
- `Event::APPLY_OPERATION` (`resource_provider.proto:49-69`): controller-to-provider notification that a framework's accept references this provider's resources.

The proto already supports the principle. The medios task is to land an implementation that respects it.

### §3.1. Worked example — CPU (existing model)

```
Node N1 advertises: cpu=4, mem=8GB                  (existing per-node mechanism)
Controller offers: {framework=F, resources=[cpu=2, mem=4GB]}
Framework accepts.
Controller decrements: N1 now has cpu=2, mem=4GB available.
Task runs. Task uses 0.3 cpu sustained, 1.2 cpu peak (irrelevant to controller).
Task ends. Controller increments back: N1 has cpu=4, mem=8GB available.
```

Reconciliation between declared (2) and actual (0.3-1.2) is the isolator's view, not the scheduler's.

### §3.2. Worked example — Gemini tokens (new model, same loop)

```
Provider P1 advertises: gemini_usd=25.00            (new per-provider mechanism)
Controller offers: {framework=F, resources=[cpu=1, mem=512MB, gemini_usd=0.25]}
Framework accepts.
Controller decrements: P1 now has gemini_usd=24.75 available.
Task runs. Task calls Gemini, actual spend is $0.18 (irrelevant to controller).
Task ends. Controller does NOT increment back. gemini_usd stays at 24.75.
Later: P1 sends UPDATE_STATE with gemini_usd=29.75 (refilled by 5.00 per its
       own internal policy — controller doesn't know why).
Controller updates its view: P1 has gemini_usd=29.75 available.
```

The 0.07 difference between $0.25 declared and $0.18 actual is the operator's view via provider billing telemetry. The controller never learns it. The 5.00 increment from refill is the provider's internal concern; the controller sees only the resulting balance.

---

## §4. What this principle does NOT do

- It does not preclude observability tools from computing declared-vs-actual drift. They can, against billing exports + task launch records.
- It does not preclude rate-limiting tasks based on consumption (a framework can choose to delay task launches if it observes high overrun). That is a framework concern, not a scheduler concern.
- It does not preclude a Resource Provider from internally implementing arbitrary refill policy, calendar windows, multi-tenant sub-pools, or anything else. Provider internals are out of scope for medios.
- It does not preclude the scheduler from offering a cluster-wide budget across multiple frameworks via DRF. That falls out of the existing allocator without any new code — DRF treats provider-scoped resources identically to node-scoped resources.

---

## §5. Lineage

- **FAC3-0017** (Offer Loop): defines the offered/accepted/consumed primitive for node resources. FAC3-0026 generalizes the same primitive to provider-scoped resources.
- **DR-021** (#42): kept the OCI image execution path through the strip; explicit precedent for keeping abstractions whose impls are stubbed.
- **STRIP-007** (#28, closed): stripped CSI + Storage Resource Provider impls. Kept the Resource Provider API.
- **STRIP-008** (#29, closed): kept the Resource Provider framework as an abstract surface even though no Local Resource Providers ship in medios.
- **STRIP-014** (#62, closed): originally proposed to stub concrete Resource Provider impls. Closed 2026-05-24 as **deferred, not executed** — the project direction after STRIP-008 and the alpha.zero test-port work (#111) was to keep the Resource Provider framework as a real platform seam, not only headers. ~2.8K lines of RP framework code remain in tree (`src/resource_provider/`). The `/api/v1/resource_provider` HTTP endpoint is live on the node (`src/node/node.cpp:801`). This is the lineage-preservation move that makes FAC3-0026 implementable today, with no medios code changes required.
- **DR-007** (lineage preservation): when stripping, keep interfaces and call sites; stub the impls; don't excise concepts. This document is what FAC3-0026 cashes in.

---

## §6. Acceptance for the principle itself

This FAC3 is *accepted* when:

- A feature design for consumable resources is in tree and consistent with this principle.
- The medios audit of STRIP-014's remaining stub bodies has happened (filed as a separate issue) and the result is reflected in the feature doc.
- At least one Resource Provider implementation (the demo `gemini-tokens-provider`) advertises a consumable resource and the controller offers + decrements it correctly under the offered/accepted/consumed loop.
- No code in the controller, allocator, or scheduler API references the concept of "actual usage" or attempts reconciliation between declared and actual quantities.

Last item is the load-bearing one. The principle holds when the code refuses to learn what really happened.
