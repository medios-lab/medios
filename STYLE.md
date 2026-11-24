# Medios Style Contract

Invariants for code in this repository. Each rule is a hard contract for new code unless the PR body cites the section number and an explicit exemption rationale.

This file is a contract, not a tutorial. Rules are checked in PR review. Violations are grounds to close a PR as `not accepted`.

When delegating to a non-interactive coding agent (e.g. `claude -p`, codex), the hand-off spec MUST require this file to be applied. Agents do not detect project-vocabulary or layering violations on their own.

---

## §1. Vocabulary

### §1.1. No upstream-Mesos vocabulary in new code

The string literals `"mesos"`, `"master"`, `"slave"`, and the noun `agent` (in the upstream "node" sense) MUST NOT appear in code added or modified by a PR.

Existing occurrences are tech debt tracked separately. A PR MUST NOT propagate them into new sites.

Wrong:
```cpp
if (strings::contains(flags.containerizers, "mesos")) { ... }
```

Right:
```cpp
if (containerizerSet.count(kContainerizerMesos) > 0) { ... }
```

Acceptable: editing an existing occurrence in-place, when the diff already touches that line for unrelated reasons. Forbidden: copying the literal into a new line.

### §1.2. Medios vocabulary is `controller` and `node`

Per FAC3-0021. `agent` is reserved for AI/agentic concepts only.

### §1.3. No named former-employer references in any artifact

Commits, PR bodies, issue bodies, code comments, doc prose, and chat-channel PR comments MUST NOT name former employers. GitHub edit history preserves the original; the fix is delete + repost, not edit.

---

## §2. Configuration & flags

### §2.1. Tokenized flag values MUST be parsed, not substring-matched

Flag values containing delimiters (`--containerizers=a,b`, `--isolation=cgroups,gpu`) are tokenized lists. Membership tests MUST use split + set membership.

Wrong:
```cpp
if (strings::contains(flags.containerizers, "docker_shell")) { ... }
```

Right:
```cpp
const vector<string> types = strings::split(flags.containerizers, ",");
const hashset<string> set(types.begin(), types.end());
if (set.count("docker_shell") > 0) { ... }
```

Reference site: `src/node/containerizer/containerizer.cpp:226`.

### §2.2. Named string values MUST be `constexpr` constants

String literals naming a contract (containerizer names, scheme names, role names, status reason names) MUST be `constexpr` constants declared in a header colocated with the thing they name. Call sites MUST reference the constant.

A literal `"docker_shell"` appearing in a `.cpp` file outside the containerizer that owns the name is a §2.2 violation.

---

## §3. Layering

### §3.1. Do not re-derive dispatcher decisions from configuration

If module X dispatches to one of several handlers based on configuration, code outside X MUST NOT re-implement that decision by inspecting the same configuration. It MUST ask X.

Wrong:
```cpp
// Re-deriving the containerizer dispatch table inside Node::__run:
if (strings::contains(flags.containerizers, "docker_shell") &&
    !strings::contains(flags.containerizers, "mesos")) { ... }
```

Right: ask the containerizer via its interface (`Containerizer::supports`, `LaunchResult::NOT_SUPPORTED`, or equivalent), or move the check into the containerizer itself.

### §3.2. Validation lives at the boundary it validates

A containerizer-specific contract (e.g. `docker_shell` requires `ContainerInfo` of type `DOCKER`) MUST be validated either inside that containerizer's `launch` path or behind a per-containerizer validation method exposed on the abstract `Containerizer` interface. Validation MUST NOT live in the upstream caller.

### §3.3. Use existing first-class concepts before adding parallel ones

Before adding a new validation/dispatch path, check whether an existing enum case, interface method, or status code already names the concept (e.g. `Containerizer::LaunchResult::NOT_SUPPORTED`). If one exists, use it; if it is broken, fix it in place rather than routing around it.

---

## §4. Comments

### §4.1. Comments name WHY, not WHAT

A comment MUST explain non-obvious motivation: a hidden constraint, an invariant, a workaround for a specific bug. A comment MUST NOT restate what well-named code already says.

Wrong:
```cpp
// Controller expects a new executor to be launched for this task(s).
if (launchExecutor.isSome() && launchExecutor.get()) { ... }
```

The variable name says it.

### §4.2. Decision-record-shaped comments belong in `docs/design/principles/`

A multi-paragraph code comment explaining an architectural rationale is a misplaced ADR. Move the prose to `docs/design/principles/FAC3-NNNN-*.md`; replace the code comment with a single line: `// See FAC3-NNNN.`

### §4.3. No references to the current task, fix, or commit

`// Added for #200`, `// Fixes the PR-X bug`, `// Used by Y` MUST NOT appear in code. That metadata belongs in commit messages and PR bodies. Code is read long after the issue is closed.

### §4.4. Default to no comment

If removing the comment would not confuse a future reader who knows C++ and this codebase, the comment MUST be removed.

---

## §5. Status updates

### §5.1. Pre-launch rejection uses `TASK_ERROR`, not `TASK_FAILED`

A task that never reached the executor MUST surface as `TASK_ERROR` (permanent framework error) or `TASK_DROPPED` (transient). `TASK_FAILED` is reserved for tasks that ran and failed.

### §5.2. Rejection status updates MUST have non-empty `message` and specific `reason`

Null `message` + null `reason` is a silent failure even when the state code is semantically correct. The `message` MUST name the validator that fired and what was expected. The `reason` MUST be the most specific applicable `TaskStatus::Reason` value.

### §5.3. Status `message` text MUST be machine-greppable

Frameworks parse `message` substrings. The text MUST include stable identifiers (validator name, expected shape) that a framework can grep on. Free prose that paraphrases the constraint differently each call site is forbidden.

---

## §6. Tests

### §6.1. Empty `SetUp() override {}` is forbidden

If a test does not need the parent fixture's setup, the test class MUST inherit from a fixture that does not perform that setup. An empty override implies the override is meaningful when it is not, and silently drops any future setup the parent adds.

### §6.2. Test fixture inheritance MUST be additive, not subtractive

A subclass MUST NOT exist to remove behavior from its parent. If a parent fixture's setup is unwanted in some cases, refactor so the unwanted behavior lives in a subclass, and inherit from the base.

### §6.3. E2E tests MAY substring-match on output, but MUST also assert the negative

A passing assertion on the presence of `TASK_ERROR` MUST be paired with an assertion on the absence of `TASK_FAILED` (and other states the test does not expect). Silent state drift then surfaces as a test failure, not a passing test.

---

## §7. Delegation to non-interactive agents

### §7.1. Hand-off specs MUST cite this file

A spec given to `claude -p`, codex, or a similar non-interactive subprocess MUST require this file to be applied and MUST cite the sections most likely to be violated for that task.

### §7.2. Violations are grounds for `not accepted`

A PR from a delegated run that violates this file MUST be closed as `not accepted`. The PR is retained as a `what-bad-looks-like` reference; the work is redone.

The closing comment MUST name the violated section numbers and quote the offending lines. This is the audit trail.

### §7.3. Style violations are not negotiable in a delegated PR

A delegated PR is not the place to discover or relax a rule. If a rule is ambiguous, fix the rule in this file first, then redo the delegated work. Loosening a rule under pressure from an in-flight PR is forbidden.

---

## §8. This file

### §8.1. Changes to this file land via PR with explicit approval

Changes MUST cite the case that motivated the new or modified rule. Drive-by rule additions are forbidden.

### §8.2. Rules MUST be invariants

A rule MUST be expressible as `MUST` / `MUST NOT` / `MAY`. Rules using `should`, `consider`, `try to`, or `prefer` are forbidden — they read as suggestions and do not anchor PR review.

---

## §9. Upstream-first design

Medios is a fork of Apache Mesos. The proto surfaces, enums, fields, RPC methods, and behavioral semantics under `include/mesos/`, `include/mesos/v1/`, and the libmesos source were inherited from upstream. Most surface-level problems we hit are already solved there.

### §9.1. Check upstream before designing any wire-surface change

Before proposing a new protobuf field, enum value, RPC method, or behavioral semantic on an inherited surface, the author MUST:

1. Grep our own proto for related fields (e.g. `git grep -n pull_image include/mesos/`).
2. If our proto is silent, check upstream at `https://raw.githubusercontent.com/apache/mesos/master/<path>` for the same surface.
3. Cite the result in the PR body, even when upstream has nothing.

If upstream already has the field/enum/method with the intent we need, the fix is to honor it per upstream semantics — NOT to design a new one.

Wrong: proposing a new `image_source` enum to control `docker pull` vs `docker run` of a local image, when `ContainerInfo.DockerInfo.force_pull_image` (bool, default false: use local if present; true: always pull) is already in our proto inherited from upstream.

Right: honor `force_pull_image` in the executor per the comment in the proto. Cite `apache/mesos:include/mesos/mesos.proto:3687` in the PR.

### §9.2. Reinvented inherited surface is grounds for not-accepted

A PR introducing a new wire-level field, enum value, or method when upstream already has one with the same intent MUST be closed not-accepted and redone using the existing surface.

### §9.3. Intentional divergence MUST be documented

If we diverge from upstream's design (smaller surface per FAC3-0022, different semantics for a stated reason), the PR body MUST cite the upstream source and explain the divergence. Silent divergence becomes invisible later and produces compounding confusion.

### §9.4. Hand-off specs (per §7) MUST require the §9.1 check

Specs for delegated runs MUST explicitly require an upstream-Mesos audit before any wire-surface design proposal. This rule was added 2026-05-25 after several cycles were burned designing an enum that already existed in our forked proto. The grep that would have caught it takes ten seconds.
