# Third-Party Dependency Lockfile

Authoritative record of vendored third-party artifacts used by the Medios build.
Pin disposition is enforced in `cmake/Dependencies.cmake`. This file is the human
and agent reference for what those pins resolve to and why.

## Pinning policy

- Git dependencies: pinned to 40-character commit SHA (always dereferencing
  annotated tags via `refs/tags/<tag>^{}` to their underlying commit SHA, #430).
  Original tag preserved as inline comment for human readability.
- URL tarball dependencies: pinned to upstream URL plus `URL_HASH SHA256=...`.
- No floating refs (`master`, `main`, `trunk`, `HEAD`, release branches).
- No silent version bumps. Any change to a pin is a reviewed PR.
- No system-package fallback at PHASE-A per FAC3-0023 (#50). Future opt-in
  override hooks (`-DMEDIOS_USE_SYSTEM_<DEP>=ON`) require their own DR.
- System-package shadowing is structurally prevented for the very-high-risk deps.
  `boost` and `protobuf` carry `OVERRIDE_FIND_PACKAGE` (BUILD-010 / #76), so any
  `find_package(<dep>)` call — direct or transitive — resolves to the pinned
  vendored copy. A contributor with system Boost or system protobuf installed
  cannot accidentally link against the wrong version. Requires CMake 3.24+.
  Other deps (gflags, glog, zlib, googletest, re2, leveldb, curl) are NOT
  overridden — they consume each other transitively in ways that conflict with
  glog/curl's install/export configuration. They're either uncommonly
  system-installed at risky versions or shadowing them is lower-impact than the
  install breakage. Reassess if a real-world shadowing escape is observed.

## Inventory

| Name | Version | Source | Pin | Role | Risk |
|---|---|---|---|---|---|
| boost | 1.83.0 | [tarball](https://github.com/boostorg/boost/releases/download/boost-1.83.0/boost-1.83.0.tar.xz) | `SHA256:c5a0688e1f0c05f354bbd0b32244d36085d9ffc9f932e8a18983a9908096f614` | runtime | very-high |
| zlib | v1.4 | [git](https://github.com/madler/zlib) | `99623ef7097352da7993ba614161fd6a311e046e` | runtime | high |
| protobuf | v25.3 | [git](https://github.com/protocolbuffers/protobuf) | `4a2aef570deb2bfb8927426558701e8bfc26f2a4` | runtime | very-high |
| gflags | v2.3.1 | [git](https://github.com/gflags/gflags) | `8f30ebfb2b206a8bd3a97ffe3b59009a952343e6` | runtime | medium |
| glog | v0.7.0 | [git](https://github.com/google/glog) | `34b8da6496aec6a98277808701cfa834fae9801f` | runtime | medium |
| GoogleTest | v1.15.2 | [git](https://github.com/google/googletest) | `b514bdc898e2951020cbdca1304b75f5950d1f59` | test | lower |
| elfio | Release_3.9 | [git](https://github.com/serge1/ELFIO) | `06097d962cbbbc1b2e6ea8aab25aac7f6108df26` | runtime (header-only) | lower |
| picojson | v1.3.0+PR135 | [git](https://github.com/kazuho/picojson) | `111c9be5188f7350c2eac9ddaedd8cca3d7bf394` | header-only | lower |
| RE2 | 2023-03-01 | [git](https://github.com/google/re2) | `3a8436ac436124a57a4e22d5c8713a2d42b381d7` | runtime | high |
| cURL | 8.23.0 | [tarball](https://curl.se/download/curl-8.23.0.tar.xz) | `SHA256:8dadd7d2536a314f982ad05a651b713f426bf7d02cc2c76cffb9796f2218e6c5` | runtime | very-high |
| LevelDB | 1.23 | [git](https://github.com/google/leveldb) | `99b3c03b3284f5886f9ef9a4ef703d57373e61be` | runtime | high |
| llhttp | release/v9.4.3 | [git](https://github.com/nodejs/llhttp) | `0e815792b167a9bd8ace259b95b7da953776c288` | runtime | medium |
| libev | 4.33 | [tarball](http://dist.schmorp.de/libev/libev-4.33.tar.gz) | `SHA256:507eb7b8d1015fbec5b935f34ebed15bf346bed04a11ab82b8eee848c4205aea` | runtime | medium |
| libseccomp | 2.4.4 | [tarball](https://github.com/seccomp/libseccomp/releases/download/v2.4.4/libseccomp-2.4.4.tar.gz) | `SHA256:4e79738d1ef3c9b7ca9769f1f8b8d84fc17143c2c1c432e53b9c64787e0ff3eb` | runtime (Linux, opt-in via `MEDIOS_ENABLE_SECCOMP_ISOLATOR`) | high |

## Role definitions

- **runtime**: linked into `medios-controller`, `medios-node`, `medios-execute`, or `libmedios`. Affects runtime behavior and must compile cleanly under the project's warning policy.
- **test**: linked only when `MEDIOS_BUILD_TESTS=ON`. Must not appear in install or export targets for runtime binaries.
- **header-only**: include-only; no compiled artifact contributed to runtime binaries.

## Risk definitions

Risk reflects the impact of an unexpected version drift or a tampered artifact, not the probability of either:

- **very-high**: serialization, network/TLS, large API surface — silent breakage is likely and security impact is realistic.
- **high**: persistent storage, compression, regex behavior, syscall filtering — observable correctness or security impact.
- **medium**: process configuration, event loop, HTTP parsing — observable correctness impact, lower security impact.
- **lower**: header-only or test-only — bounded blast radius.

## Dependency Audit Notes

- **boost 1.83.0 (#496, #675):** Only `circular_buffer`, `filesystem`, `icl`, `lexical_cast`, `range`, `system`, `uuid`, and `variant` are built (`BOOST_INCLUDE_LIBRARIES`). `Boost.Regex` is excluded per `FAC3-0022` because all regular expression matching in Medios uses `RE2` or `std::regex`. `CVE-2026-11460` (`Boost.Serialization`, unfixed upstream through 1.92.0) is unreachable as built because `BOOST_INCLUDE_LIBRARIES` in `cmake/Dependencies.cmake` excludes `serialization` (and none of the built Boost libraries depend on `Boost::serialization`) and no Medios target includes `boost/serialization` or `boost/archive`.
- **zlib v1.4 (#674):** The two unfixed upstream defects in `v1.4` (`contrib/iostream2` unbounded read and `inflate()` unsigned wraparound in the gzip `EXTRA` header copy via `inflateCopy()`/`inflateGetHeader()`, `madler/zlib#1419`) are unreachable as built because all `ZLIB_BUILD_*` contrib targets default to `OFF` and Medios (`stout/include/stout/gzip.hpp`) never calls `inflateGetHeader()` or `inflateCopy()`.
- **protobuf v25.3 (#430, #561):** Pinned to commit `4a2aef570deb2bfb8927426558701e8bfc26f2a4` (dereferenced from annotated tag `2fc6cf3fb16b431874a8428b66efe0fda8f26e01`). Patched via `cmake/patches/abseil-arm64-msse41-fix.cmake` (#173).
- **gflags v2.3.1 (#429, #430):** Bumped from `v2.2.2` to `v2.3.1` (`8f30ebfb2b206a8bd3a97ffe3b59009a952343e6`), which includes the `v2.3.0` fix for null-pointer-offset undefined behavior in `ProcessOptionsFromStringLocked`.
- **glog v0.7.0 (#498):** Latest `0.7.x` release (`34b8da6496aec6a98277808701cfa834fae9801f`); `0.7.1` adds no security fixes affecting Medios's usage.
- **picojson v1.3.0+PR135 (#419):** Pinned to upstream `master` head `111c9be5188f7350c2eac9ddaedd8cca3d7bf394`, which lands PR #135 (`DEFAULT_MAX_DEPTHS = 100`) mitigating `CVE-2018-11793`. In addition, `stout/include/stout/json.hpp` enforces `STOUT_JSON_MAX_DEPTH = 200` inside `DepthLimitedContext` prior to constructing values.
- **RE2 2023-03-01 (#422, #430):** Bumped from `2022-06-01` to `2023-03-01` (commit `3a8436ac436124a57a4e22d5c8713a2d42b381d7`, dereferenced from annotated tag `b025c6a3ae0ece9fe5fbfa2bb0eae4fe2fa4f412`), the final standalone Abseil-free RE2 release (avoiding Abseil `20240116+` header mismatches against `protobuf v25.3`'s vendored Abseil `20230802.1`), and patched via `cmake/patches/re2-simplify-empty-width.cmake` to backport upstream commit `e66463312e1d30d427bbde6c40e7fd627dcfb82e` collapsing counted repetitions of empty-width assertions in `re2/simplify.cc`.
- **cURL 8.23.0 (#386, #596, #681):** Configured with `CURL_USE_LIBSSH2=OFF`, `CURL_USE_LIBSSH=OFF`, `CURL_ENABLE_NTLM=OFF`, and per-protocol `CURL_DISABLE_*=ON` switches for every non-HTTP/HTTPS protocol (`curl 8.23.0` removed the legacy `HTTP_ONLY` CMake shortcut), limiting `libcurl` to `http` and `https`.
- **LevelDB 1.23 (#423, #676):** Patched via `cmake/patches/leveldb-cxx20-memory-order.cmake` and `cmake/patches/leveldb-footer-decode-bounds.cmake` to backport upstream commit `068d5ee1a3ac40dabd00d211d5013af44be55bea` (`Footer::DecodeFrom()` bounds check `input->size() < kEncodedLength` on truncated `.ldb` files), upstream commit `578eeb702ec056be97ef9f2ac2fa92efbb00d394` (`util/hash.cc` pointer subtraction check `while (limit - data >= 4)`), and upstream commit `29465588d3dfdc80fba5e198037582b8fdb5dd60` (`table/block.cc` `DecodeEntry()` subtractive overflow guard when validating `*non_shared` and `*value_length`).
- **llhttp release/v9.4.3 (#428):** Verified current latest upstream release (`0e815792b167a9bd8ace259b95b7da953776c288`), replacing archived `http-parser` (#412).
- **libev 4.33 (#497):** Served from `http://dist.schmorp.de/libev/libev-4.33.tar.gz` and patched via `cmake/patches/libev-nosigmask-polarity.cmake` to backport upstream CVS `ev.c` rev 1.539 (`if (!(origflags & EVFLAG_NOSIGMASK))` polarity fix in `ev_signal_start`).
- **elfio Release_3.9 (#420):** Only exercised on trusted host NVML/CUDA shared libraries during node GPU isolator setup (`src/modules/gpu/volume.cpp`); hardened via `cmake/patches/elfio-section-bounds.cmake` and section bounds/null checks in `stout/include/stout/elf.hpp`.
- **libseccomp 2.4.4 (#630):** Opt-in via `-DMEDIOS_ENABLE_SECCOMP_ISOLATOR=ON` on Linux. Pinned to `2.4.4` because it includes `clone3` (required by glibc 2.34+) while shipping pre-generated syscall tables so `gperf` is not required on build hosts.

## Verification

Confirm every dep is pinned with one of: 40-char SHA, or `URL_HASH SHA256=...`.

```
grep -cE 'GIT_TAG [0-9a-f]{40}|URL_HASH SHA256=' cmake/Dependencies.cmake
```

Expected count: **14** (10 git SHAs + 4 URL hashes).

To regenerate this lockfile after a pin change, resolve the new ref (always dereferencing annotated git tags to their underlying commit SHA):

```
git ls-remote <repo-url> 'refs/tags/<tag>^{}'     # annotated git tags (fall back to refs/tags/<tag> for lightweight tags)
shasum -a 256 <downloaded-tarball>                # tarball deps
```

Then update both `cmake/Dependencies.cmake` and this file in the same PR.

## References

- [FAC3-0022](docs/design/principles/FAC3-0022-dependency-parsimony.md) — dependency parsimony
- [FAC3-0023](docs/design/principles/FAC3-0023-automation-oversight-autonomy.md) — vendor-everything policy
- BUILD-008 #72 (private history) — this work
- BUILD-009 #73 (private history) — binary cache (next)
- #71 (private history) — build engineering parent issue
