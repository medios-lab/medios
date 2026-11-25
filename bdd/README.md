# Medios BDD acceptance corpus

Behavioral PRs introduce Gherkin contracts under `bdd/features/<area>/*.feature`.

**This directory is intentionally not populated with pretend-passing scenarios.** The first executable feature will land with its step bindings, live Medios fixture, independent observer, and trace matcher. Until then, the `bdd-contract` CI job checks only that behavioral PRs submit a contract (or an explicit exemption) and that feature files have a minimal structure. It does not run Gherkin, certify oracle independence, or claim behavioral correctness.

See [BDD-PR design](../docs/contributing/bdd-prs.md) and [contribution policy](../CONTRIBUTING.md).
