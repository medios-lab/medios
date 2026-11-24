```
CATALOG   FAC3-0023
TITLE     Automation and Oversight Autonomy as Core Thesis (No Heroics)
EDITION   001
STATUS    Accepted
```

## Decision

Medios's development methodology must embody the same node-autonomy thesis as the platform itself. Routine work is automated; oversight is distributed across nodes and records, not concentrated in any single human. The project does not depend on heroics, marathon sessions, or one specialist's flow state to make progress.

This is not a soft aspiration. It is a hard requirement: every recurring task that today requires a specific human must have a path to becoming node-driven, with the human role moving toward exception handling and architectural calls only.

## Why this is a project thesis, not a workflow preference

The strip plan was designed Friday 2026-05-08 and operationally complete Saturday 2026-05-09. Eleven STRIP issues resolved (9 stripped, 2 reframed), four PRs reviewed and merged, one CMake-multi-platform-workflow narrowed, one fresh README, four FAC3 principles documented (FAC3-0021 through FAC3-0023). Roughly 36 hours wall-clock.

That pace was real and it produced durable artifacts. **It also required:**

- A human with 20+ years of software engineering management experience
- That human in active flow state for most of the window
- A tendency toward hyperfocus on the part of that human
- A real and known burnout risk that compounds the bus-factor risk

The first three are non-replicable. The fourth is unsustainable. **A node-autonomy platform built only by methods that depend on heroics is not actually evidence that node autonomy works.** It is evidence that one specific human can drive a multi-node fleet for short bursts.

For Medios to credibly demonstrate its own thesis, the dev process must look like the product: nodes do routine work, oversight is distributed, humans are the architects and exception handlers, not the routers and reviewers of every cycle.

## What "automation and oversight autonomy" means in practice

Automation: routine work executes without human in the loop.

- Code review: the strip-cr skill workflow runs as CI, posting review comments automatically; humans see only blockers and architectural questions. Today the user invokes strip-cr by naming a PR; tomorrow GitHub Actions invokes it on PR open.
- Strip / refactor execution: Codex picks up issues from a `ready-for-codex` label without per-issue human kickoff. Today the user pings each issue; tomorrow the node polls.
- Dependency ordering: the system detects strip-NNN dependencies from issue bodies and applies the `blocked` label automatically when prerequisites are open. Today the user manually labeled STRIP-012; tomorrow a GitHub Action does.
- Verification: post-PR-merge grep checks (per FAC3-0022 hygiene) run automatically and post results. Today reviewers verify by hand; tomorrow CI verifies.
- Reflection: end-of-session reflections generate from telemetry without the user asking. Today the user requests them; tomorrow the system produces them.

Oversight autonomy: multiple nodes check each other; the human is one reviewer among several, not the only one.

- Claude reviews Codex's work (already happening). Codex could review Claude's work (not yet happening). Other nodes could review both (open question — which nodes, what specialization).
- DR/FAC3 records replace "ask the architect" with "read the records." Anyone (node or human) reads the corpus and proceeds with confidence.
- NODES.md sets L3 boundaries so a sub-node knows what to escalate; reduces the "every decision is the user's" pattern.
- Memory layer (`~/.claude/projects/.../memory/`) preserves context across sessions so a fresh Claude instance can pick up without the user re-explaining.

## Anti-patterns we are explicitly building against

The user has seen these fail in 20 years of engineering management. They are explicit non-goals:

1. **Single-point-of-failure architect**: "Only [name] understands the build." Decisions blocked on one human's calendar. Bus factor of 1. Medios's DR + FAC3 corpus is the explicit defense.

2. **Synchronous review bottleneck**: Every PR waits for the same reviewer. The reviewer becomes the rate limit. Burnout follows. strip-cr-as-CI is the explicit defense.

3. **Heroic onboarding**: New contributors (human or node) require multi-week ramp-up reading source code. NODES.md + the FAC3/DR corpus is the explicit defense.

4. **"Just one more session"**: Marathon-driven progress that depletes the contributor. The framing of phase-based work (PHASE-A, B, C) with explicit time budgets is the explicit defense.

5. **Tribal knowledge**: Decisions made in chat that don't survive the session. The discipline of posting decisions to GitHub issues with the `decision record` label is the explicit defense.

6. **Implicit dependencies**: "Everyone knows X needs Y first." The `blocked` label + explicit dependency callouts are the explicit defense.

## How this principle interacts with the platform thesis

Medios is a thin resource broker for agentic compute. The platform's value proposition: an LLM agent can declare resource needs and the broker handles scheduling without human-in-the-loop provisioning.

If the project that builds Medios requires human-in-the-loop for every cycle, the product's claim is undermined. The dev process is part of the demo. If we cannot run the project this way, why should anyone trust the platform to enable agentic compute autonomy?

This is not metaphor. It is the same engineering problem at two scopes.

## Concrete near-term commitments

Tier 1, already in place:
- strip-cr skill (PR review workflow, codified)
- NODES.md (L3 boundaries for sub-nodes)
- FAC3 + DR corpus (architectural decisions as searchable records)
- Memory layer (cross-session context preservation)
- `decision record`, `strip`, `phase`, `blocked` labels (taxonomic clarity)
- Per-issue clarification comments (issue-as-contract for Codex)

Tier 2, scheduled for PHASE-B or earlier:
- strip-cr workflow as a GitHub Action invoked on PR open
- Auto-application of `blocked` label based on issue-body dependency declarations
- Post-merge verification grep checks as CI step (FAC3-0022 hygiene automation)
- Co-Authored-By trailer enforcement via commit-msg hook (recurring nit across PRs #34, #45, #49)
- PR template (`.github/PULL_REQUEST_TEMPLATE.md`) matching the strip-cr definition-of-done shape

Tier 3, post-PHASE-C aspiration:
- Medios itself as the dev infrastructure: contributing nodes run on Medios
- Multi-node handoff protocol formalized (`docs/multi-node-handoff.md` or similar)
- Telemetry-driven reflection generation
- Burnout-pattern recognition in session telemetry; system suggests pause before it becomes a problem

## How to apply

When making any decision about how the project operates, ask:

1. Does this require a specific human?
2. Is the human role architecture/exceptions, or is it routing/review?
3. If a human leaves the project for a week, does work pause?
4. Could a fresh Claude/Codex/Cursor instance pick this up from the artifacts alone?

If the answer to any of those points to human-as-bottleneck, that is a debt item. Track it. Pay it down by the next phase.

This principle does not eliminate human judgment. It moves human judgment to where it adds the most value: architecture, novel decisions, taste calls, exception handling. Routine review, mechanical refactors, dependency ordering, verification — those become node and tooling work.

## References

- NODES.md — L3 boundaries for sub-nodes on Medios work
- `.claude/skills/strip-cr/SKILL.md` — codified PR review workflow
- DR-019 (#20) — modern stdlib port; an example of explicit phase-based work with time budgets
- DR-022 / FAC3-0022 (#44) — dependency parsimony; same energy applied to deps
- PHASE-A (#39), PHASE-B (#40), PHASE-C (#41) — explicit phase scoping with time-budget targets

---

Adopted: 2026-05-09 after the 36-hour strip-plan marathon made the cost of the current pace observable.
