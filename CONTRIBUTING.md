# Contributing to Medios

Medios is a small distributed compute system for scheduling research. Contributions are reviewed primarily as **claims about observable behavior**, not as line-by-line arguments about implementation.

## Behavioral pull requests (BDD-PRs)

**A PR that changes executable behavior must include an executable behavioral contract. PRs without one are not ready for review and may be closed.** This applies equally to humans and coding agents.

The contract is a Gherkin `.feature` file under `bdd/features/`. It describes the expected effect from the perspective of an independent observer, not C++ functions, mocks, or internal design. Link the scenario from the PR description and explain the external evidence that proves it.

For bug fixes, demonstrate the same scenario failing against the base revision and passing on the PR revision (**RED → GREEN**). For new features, a missing capability on the base revision is acceptable; distinguish that from a reproduced regression. Keep prior invariants in the acceptance corpus.

The behavioral contract is **necessary, not sufficient**. All existing build/unit tests, security checks, and agentic code reviews still apply. A passing scenario does not establish absence of other defects.

### What to submit

1. **Intent:** Which observable behavior changes, and why? Link the issue / design record.
2. **Contract:** Add one or more `bdd/features/**/*.feature` scenarios. Cover the positive path and relevant denial, timeout, retry, cancellation, or recovery cases. Give bounded timing requirements when timing is part of the contract.
3. **Oracle:** Explain which independent observations determine PASS/FAIL (controller and node lifecycle events, external side effects, provider messages, etc.). A component's own success response alone is not an independent oracle.
4. **Evidence:** Link to trace/test artifacts showing scenario results; for a bug fix, show the RED run against base as well as GREEN against head. Separate `ASSERTION_FAILURE` from `INFRA_ERROR`.
5. **Implementation:** Let agents implement and challenge the change, generate adversarial tests, and report discovered gaps. Keep the implementation small.
6. **Human review:** Humans primarily review the behavioral contract, exceptions, and architectural boundaries. Changes to the oracle or step semantics require explicit review, not silent co-editing to make tests green.

A feature file must map to actual executable steps and observable evidence before its PR can be considered behaviorally validated. **A syntax check, an unbound Gherkin scenario, or a mocked assertion is never a passing BDD test.**

### Example (illustrative, not an implemented acceptance test)

```gherkin
Feature: External resource provider admission
  Scenario: Provider denies authorization
    Given a running Medios controller and node
    And a resource provider advertising 100 credits
    And a framework task requiring 25 credits
    When the framework accepts the offer
    And the provider rejects publication
    Then the task must never enter TASK_RUNNING
    And the test observer must detect no task side effect
```

For retried non-idempotent operations, include the policy governing whether replay is allowed. Avoid writing scenarios that demand an unsafe retry.

### Exempt changes

Documentation-only, formatting-only, generated-file synchronization, and purely mechanical refactors with no changed observable behavior may use an explicit `BDD-EXEMPT` section in the PR description. Explain why no behavior changes. **The contributor cannot declare their own exemption final:** a maintainer confirms it. CI may check the exemption's presence, not its truth.

### Development and review

- Discuss changes to resource ownership, scheduling boundaries, or the Medios core architecture in an issue before implementing.
- Keep changes narrow; avoid adding platform features or dependencies not justified by the research question.
- Existing CMake / CTest tests remain required where applicable.
- Treat external PR code, issue text, and generated artifacts as untrusted input. Do not grant CI elevated credentials to run contributor code.
- Do not require production hardening for a research proof of concept unless correctness of the claim depends on it.
- Do not silently weaken approved scenarios or oracle predicates. A changed contract needs explicit rationale and review.

## Implementation status

The BDD-PR policy and lightweight contract-presence gate are initial scaffolding. **Full Gherkin execution with a Medios-backed, independent observer is not yet implemented by this proposal.** Until that runner exists, mark results `NOT_RUN` and do not label a PR behaviorally validated on the strength of the gate alone.

See [the BDD-PR design and agent handoff](docs/contributing/bdd-prs.md).
