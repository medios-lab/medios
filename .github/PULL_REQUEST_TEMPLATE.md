## Behavioral claim
What externally observable behavior does this PR change or preserve? Link the issue/design record.

## Gherkin contract
Links to changed `bdd/features/**/*.feature` scenarios and which invariants they establish.

## Oracle and evidence
What independent observations establish the result? Link traces and results. Use PASS / ASSERTION_FAILURE / INFRA_ERROR / NOT_RUN explicitly.

**Base (for bug fix):** RED / NOT_RUN — evidence:
**Head:** GREEN / NOT_RUN — evidence:
**Existing regression corpus:** PASS / NOT_RUN — evidence:

## Agentic review
What adversarial failure cases were explored? Which remain untested? Were step bindings or oracle predicates changed?

## Exemption request (only if no observable behavior changes)
Remove this section unless requesting an exemption. If no scenario is included, add a separate line starting exactly `BDD-EXEMPT: ` followed by a concrete reason. Maintainer confirmation is required.
