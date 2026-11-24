```
CATALOG   FAC3-0024
TITLE     OpenAPI Validation Runs in CI, Not the Default C++ Build
EDITION   001
STATUS    Accepted
```

## Decision

OpenAPI validation for Medios is a CI concern, not part of the default local
CMake build.

The specs under `api/openapi/` must be validated by a narrowly scoped GitHub
Actions workflow that runs on OpenAPI changes. The workflow should use a pinned
CI-only OpenAPI tool dependency, preferably Redocly CLI because it supports
OpenAPI 3.2 and already validates the hand-authored specs.

The default local build remains focused on compiling and testing Medios:

```sh
cmake --build build
ctest --test-dir build --output-on-failure
```

That build should not require Docker, Node.js, Python, npm packages, OpenAPI
linters, or any docs tooling just because the repository contains API
descriptions.

Developers changing `api/openapi/**` should be able to run the same validation
explicitly, using the same pinned container image and flags as CI. That command
belongs in the OpenAPI docs and CI workflow, not in the default CMake path.

## Motivation

OpenAPI support is part of Medios's product surface. Agents and humans need a
machine-readable description of the controller and node HTTP APIs, and broken
specs should not merge unnoticed.

At the same time, OpenAPI validation is not a runtime dependency and not a C++
build dependency. Pulling an extra scripting language, package manager, or docs
toolchain into the normal local build would contradict FAC3-0022's dependency
parsimony rule. It would also add friction to the common case: an engineer or
agent building C++ daemons and running C++ tests.

The right split is:

- CI protects the repository from invalid OpenAPI documents.
- Local C++ builds stay fast, predictable, and focused.
- OpenAPI authors have an explicit local validation command when they need it.

## Application

The follow-up implementation is tracked in #202.

The workflow should:

- Run on pull requests touching `api/openapi/**`.
- Run on pushes to `master` touching `api/openapi/**`.
- Use minimal permissions, normally `contents: read`.
- Lint both daemon specs:
  - `api/openapi/controller.openapi.json`
  - `api/openapi/node.openapi.json`
- Pin third-party actions to immutable commits where practical.
- Pin the OpenAPI tool by version or image digest; do not use `latest`.
- Treat OpenAPI syntax and structural spec errors as failures.
- Allow known skeleton-spec warnings initially, then tighten rules as schemas
  become more complete.

The workflow should not:

- Add Python, Node package metadata, or another repository-level language
  dependency just for this check.
- Add an OpenAPI target to the default CMake build.
- Require all local C++ builds to have Docker or OpenAPI tooling available.
- Implement full ProtoJSON schema generation.
- Add live controller/node contract tests.

## Local validation

Local validation is explicit. The command should mirror CI rather than invent a
separate path. Once #202 lands, the authoritative local command is the
containerized lint command shown by that workflow and the OpenAPI README.

This preserves a simple rule:

- If you are changing C++ code, run the normal build and tests.
- If you are changing OpenAPI files, also run the OpenAPI lint command.

## Consequences

The default local build will not catch OpenAPI breakage. That is intentional.
The enforcement point is CI, with an explicit local escape hatch for authors who
are working on the specs.

This keeps OpenAPI validation mandatory for review without making every Medios
build carry OpenAPI tooling. It also leaves room for deeper validation later:
generated ProtoJSON schemas, route drift checks, and live API contract tests can
be added as separate decisions once their dependency and runtime costs are
known.

## References

- #198 — initial OpenAPI support for all Medios endpoints
- #202 — add CI validation for OpenAPI specs
- FAC3-0022 — dependency parsimony
- FAC3-0023 — automation and oversight autonomy
- `api/openapi/README.md` — OpenAPI spec layout and local validation notes

---

Adopted: 2026-05-24
