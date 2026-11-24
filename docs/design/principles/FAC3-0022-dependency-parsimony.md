```
CATALOG   FAC3-0022
TITLE     Dependency Parsimony — consider the long-term carrying cost of low-level deps
EDITION   001
STATUS    Accepted
```

## Decision

Every new low-level dependency in Medios is evaluated against its **multi-year carrying cost**, not just its immediate utility. Default toward stdlib primitives, simple protocols, and replaceable alternatives. Heavy RPC frameworks, complex serialization libraries, and deep-transitive build-time tooling require explicit justification, not just usefulness.

## Motivation

Real-world experience from years of operating large monorepos: certain low-level dependencies become **enterprise risk components** — they impose disproportionate cost across the entire engineering organization, hold back product timelines, and force coordinated upgrades that block unrelated work.

The canonical example: **gRPC**. Apache Ray, among many other projects, has had product progress held back by gRPC upgrade cycles. The cost is structural, not incidental:

- gRPC pulls in **protobuf-grpc, abseil, c-ares, re2, zlib, OpenSSL** transitively (~30+MB compiled)
- ABI breaks across major versions
- Vendoring vs system-package tensions
- Cross-platform variance multiplies pain
- Major version upgrades require coordination across every dependent product
- Security CVE response time is measured in weeks, not days

Medios is committed to staying light enough that an LLM can hold the relevant code and dep graph in a single context window. Heavy-transitive deps directly violate that thesis.

## The principle

When considering a new dependency, evaluate against six questions:

1. **Transitive depth** — how many other things does it pull in? gRPC's transitive cost is roughly 5x its direct payload.
2. **ABI stability** — does it break across versions? Does it ship a stable C ABI, or only C++ (where ABI is fragile)?
3. **Build complexity** — native compile path? cross-compile pain? vendor-vs-system tensions? Does it require a code generator (protoc, capnp, flatc)?
4. **Maintenance velocity** — who upgrades us when CVEs hit? Is the upstream healthy? Is there a single maintainer or a real org?
5. **Replaceable** — can we use stdlib, a simpler alternative, or a hand-rolled minimal implementation?
6. **Carrier in our domain** — do other agentic platforms depend on it for compatibility? Or is it net-zero for ecosystem alignment?

If a dependency fails on (1)+(2)+(3) but passes on (5), default to NOT taking the dep. Write the smaller alternative.

## Application: Medios's dep ledger

After STRIP-007 (#28) lands, Medios's heavy-transitive deps are:

| Dep | Status | Risk class |
|---|---|---|
| **gRPC** | Dropped (STRIP-007) | ❌ enterprise-risk; was used only by CSI |
| **protobuf** | Kept | ⚠️ next-biggest enterprise-risk; necessary for v1 HTTP API wire format. Manage carefully; pin major version; don't take protobuf-grpc as a transitive dep. |
| **boost** | Kept (selective headers) | ⚠️ heavy if taken whole; we use selective subset (filesystem, system, etc. — much of what we used now in std::filesystem per DR-019). Revisit if a bounded stdlib replacement is reviewed. |
| **glog / gflags** | Kept | ⚠️ Google-style logging; replaceable with simpler alternatives (spdlog, std::print). Revisit post-port. |
| **libev / libevent** | Tied to libprocess | ⚠️ legacy event-loop libs; they stay while libprocess stays |
| **stout / libprocess** | Kept in-tree (DR-019 not taken, 2026-09-15) | ⚠️ in-tree legacy; interfaces typed in them are stable surfaces. A stdlib replacement is a bounded, separately reviewed change. |
| **OTel SDK** | Pending (DR-016) | ✅ modern, governance under CNCF; replaces Mesos's bespoke metrics |
| **OpenSSL** | Inherited via HTTP/TLS path | ⚠️ unavoidable for TLS; manage version pin |

**Net position**: Medios's heavy-transitive deps are protobuf, OpenSSL, selective boost, the in-tree `stout` and `libprocess` with their event-loop libs, and OTel if DR-016 lands. The carrying-cost principle stands; it does not mandate a port.

## Anti-patterns to avoid

- "We'll need this someday" — speculative dependency adds. Don't take a dep until a concrete code change requires it.
- "It's the industry standard" — gRPC was the industry standard for plugin protocols. CSI used it. K8s uses it. Medios still doesn't need it. Standards are heuristics, not requirements.
- "It's only N MB" — transitive depth matters more than direct payload. Always check `ldd` / dependency graph.
- "We can always remove it later" — once a dep is in the build, removing it usually requires re-architecting consumer code. The cost compounds.
- "It has a great API" — API ergonomics is one input among many. Carrying cost dominates over years.

## Concrete rules

1. **Use the project's types.** `stout` and `libprocess` stay in-tree (DR-019 not taken, 2026-09-15); use them where the surrounding code does. A stdlib replacement is a bounded, reviewed change, never a wholesale port.
2. **Default to HTTP/JSON for new RPC surfaces.** The v1 scheduler API is the existence proof: ~80 lines of Python is a working client. Avoid gRPC, Cap'n Proto, Thrift unless there's a compelling reason that survives the six-question test.
3. **Pin protobuf major version.** Don't auto-upgrade through major boundaries; treat each major as its own dep risk.
4. **No protobuf-grpc.** Even though we use protobuf, we don't use the gRPC binding. They're separable.
5. **No new deps without DR.** Adding a new external dependency is a project-level decision; open a DR explaining the six-question evaluation.
6. **Vendor only when forced.** Prefer system packages or `find_package`; vendor only when the system version is too old to use.

## Consequences

- The CMake build (DR-005) ships with a **minimal find_package list**. Each entry is justified.
- New PRs that introduce a `find_package(...)` call are flagged for DR review.
- Future cleanup target: replace boost-selective-headers with stdlib (PHASE-C era).
- Future cleanup target: replace glog/gflags with stdlib + spdlog or similar.
- After PHASE-C, the dep ledger is small enough that an LLM can hold the entire build graph in context — directly serving the agentic-platform thesis.

## References

- DR-019 (#20) — drop libprocess + stout, port to modern stdlib. Not taken (2026-09-15); both stay in-tree.
- STRIP-007 (#28) — drops CSI and therefore gRPC; canonical instance
- DR-005 (#6) — modern CMake build; deps are explicit and minimal
- DR-021 (#42) — strip Docker daemon, keep OCI image execution; same energy (don't take heavy daemon coupling for a feature alternative exists)

---

Adopted: 2026-05-09
