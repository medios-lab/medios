# BDD-PRs: behavioral contracts as the review interface

Status: **pilot proposal** — documentation and presence gate first, executable acceptance harness next.

## Purpose

Medios intentionally separates substrate behavior from framework policy. We use the same idea to separate *what a PR must cause* from *how an agent implements it*.

The human reviews a compact set of observable properties in Gherkin. An independent harness runs a Medios controller and node and produces evidence from events and externally observable side effects. Coding agents can write and revise implementation code, but must not quietly move the goalposts by weakening approved scenarios.

This extends the existing CTest/GoogleTest suite rather than replacing it. Keep CI-only dependencies out of the default C++ build (FAC3-0024).

## Contract for one PR

| Artifact | Owner | Meaning |
|---|---|---|
| `bdd/features/.../*.feature` | PR author, then human-approved | Expected behavior and failure modes |
| Step bindings + stimuli | Harness maintainer | Exercise real Medios APIs; may be shared |
| Oracle + predicates | Independent observer | Decide results from Medios events and external side effects |
| Trace (JSONL) | CI / harness | Time-ordered evidence for each scenario |
| Scenario results (JSON) | CI / harness | PASS, ASSERTION_FAILURE, INFRA_ERROR, NOT_RUN |
| Code changes | Human or agent implementer | One implementation satisfying the contract |

The validator must distinguish **unbound or unrun scenarios** from passing tests. Results should include the feature/scenario id, feature SHA, tested base/head commit, Medios build SHA, harness SHA, observable trace location, verdict, and environment.

### Bug-fix evidence

Run the same frozen contract against:
- **base:** reproduce failure (`ASSERTION_FAILURE`), not merely a compiler or infrastructure error;
- **head:** pass (`PASS`).

For newly introduced features, distinguish unsupported behavior on base from a reproduced defect. A base RED is not always meaningful. CI should also run the existing regression corpus on head.

An agent that edits both implementation and oracle in the same PR must flag that fact explicitly. Do not promote such a run to independent evidence until the oracle delta is separately reviewed.

## Harness architecture

1. **Fixture:** launch a fresh controller + node and an independently controlled fake framework/resource provider as needed. Bind loopback and assign isolated ports/work dirs.
2. **Stimulus:** submit tasks, cancel, restart, inject provider failures or delays; steps may vary by experiment.
3. **Observer:** record controller/node events and test-owned side effects, with stable task/job IDs. Do not use the implementation's own status response as the only source of truth.
4. **Matcher:** apply predicates to normalized traces. Check absence over a bounded observation interval, not at an arbitrary instant. Distinguish lack of an event from observation failure.
5. **Report:** write machine-readable outcome and a concise PR summary. Preserve raw logs for diagnosis.

Prefer using existing Medios-built binaries and the established CTest build rather than compiling the C++ tree twice. Avoid a separate 60–90 minute build per BDD job. A Python CI-only runner is acceptable if pinned; it must not become a mandatory CMake dependency.

### Minimal normalized trace event

```json
{"ts":"2026-11-24T00:00:00Z","scenario":"provider-denied","source":"medios-node","event":"task_state","task_id":"bdd-task-1","state":"TASK_FAILED"}
```

Store richer details separately; trace ingestion failures become `INFRA_ERROR`. Use a test-owned marker/nonce to prove the task actually ran when a scenario asserts execution.

## Agent workflow

1. Read `CONTRIBUTING.md`, relevant decision records, and this document before writing code.
2. Propose Gherkin scenarios before implementation. Ask for human decision only on genuinely ambiguous semantics (e.g. retry safety, timeout policy).
3. Freeze the approved scenario text/SHA for base/head experiments.
4. Implement the minimal change and run existing unit tests and the independent acceptance runner.
5. Generate synthetic fault variants, including stalls, truncated responses, duplicate callbacks, reconnects, and timeouts when applicable.
6. Promote each newly confirmed uncovered defect into an enduring regression scenario.
7. Report evidence and untested risks. Never equate scenario pass with proof of system-wide correctness.

For agent-authored PRs, the reviewer may reject the PR if the author has not described the behavioral contract. Human code inspection is optional, not forbidden; substantive architecture/security anomalies still escalate.

## Rollout plan

**Stage 0 (this proposal):** publish contribution policy, PR template, and a cheap PR presence/format check. This is *not* executable BDD.

**Stage 1:** implement a tiny Medios-backed runner for a resource-provider admission scenario with synthetic allow/deny/timeout responses, independent event observation, and JSONL traces. Integrate with existing C++ build artifacts.

**Stage 2:** require real acceptance results and base/head red-green evidence for `bdd-pilot` bug-fix PRs. Pilot adversarial agent review of scenario completeness.

**Stage 3:** require BDD-PR coverage for all changes to observable behavior, with maintainer-controlled exemptions. Make the executable `bdd-acceptance` GitHub status check required before merge.

## Rules for automated policy enforcement

The fast check may require a changed `.feature` file or explicit `BDD-EXEMPT` rationale for a PR changing runtime code. It **cannot** certify scenario execution. Semantic exemptions need human review.

Use a read-only `pull_request` workflow and minimal token permissions. Do not use `pull_request_target` to execute untrusted PR scripts. If rulesets/branch protection are enabled, make the lightweight check mandatory only after confirming the check runs on ordinary PRs; make `bdd-acceptance` mandatory only once it actually executes and reports all scenarios.

## Research measurements

Record time spent in human behavioral review, agent review cost, number of originally omitted scenarios, distinct reproduced defects, regressions caught, false positives, and accepted contract changes. Compare against traditional review without presuming the outcome.
