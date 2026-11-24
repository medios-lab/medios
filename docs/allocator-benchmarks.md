# Allocator scale benchmarks (Sim A)

In-memory scale driver for the real `HierarchicalDRFAllocator`. Stands up
thousands of synthetic nodes and frameworks, runs timed allocation cycles, and
prints elapsed time per phase. No GPUs, no controller, no node processes, no
HTTP — the allocator is driven directly through its `addNode` / `addFramework` /
`updateQuota` / offer-callback surface. This is the tier that scales to the
node/framework counts where starvation, livelock, and DRF-fairness effects
under quota only become visible.

Source: `tests/hierarchical_allocator_benchmarks.cpp`.
Target: `test_hierarchical_allocator_benchmarks`.

## What it measures

| Fixture | Scale | What it times |
|---|---|---|
| `BENCHMARK_HierarchicalAllocations.MultiFrameworkAllocations` | many frameworks across roles | wall time to make all allocations, then a second cycle that should make none |
| `BENCHMARK_HierarchicalAllocator_WithQuotaParam.LargeAndSmallQuota` | 30 / 300 / 3000 nodes × {`drf`, `random`} sorter | one allocation cycle that satisfies every role's quota, mixing small (< 1 node) and large (> 1 node) quotas |
| `BENCHMARK_HierarchicalAllocator_WithNonQuotaVsQuotaParam.NonQuotaVsQuota` | 20 / 200 / 2000 nodes and frameworks × {`drf`, `random`} | same allocations with and without quota, for a like-for-like quota-overhead comparison |

Node and framework counts are fixed in the `INSTANTIATE_TEST_CASE_P` blocks at
the bottom of the source file; change them there to push scale up or down. The
`drf` / `random` sorter axis is the determinism knob — a given sorter + param
tuple reproduces the same cluster and the same allocation sequence every run.

## Build

The benchmark target builds with the rest of the test tree but is deliberately
left out of `gtest_discover_tests`, so it never runs during a normal
`ctest` pass. The shared test `main()` dlopens the test module libraries at
startup; those are wired as build dependencies of `medios-test-helpers-impl`
(which every test links), so a single-target build pulls them in automatically:

```bash
cmake -B build -G Ninja -DMEDIOS_BUILD_TESTS=ON
cmake --build build --target test_hierarchical_allocator_benchmarks
```

## Run

Two flags are required:

- `--benchmark` — the test environment's `BenchmarkFilter` skips every
  `BENCHMARK_`-prefixed test unless this is set.
- `LIBPROCESS_IP=127.0.0.1` — libprocess needs a bindable address.

The fixtures are name-prefixed `BENCHMARK_`, so a gtest filter selects them:

```bash
cd build
LIBPROCESS_IP=127.0.0.1 ./tests/test_hierarchical_allocator_benchmarks \
  --benchmark --gtest_filter='BENCHMARK_*'
```

Run a single scale point by narrowing the filter, e.g. only the 3000-node quota
case under the DRF sorter:

```bash
LIBPROCESS_IP=127.0.0.1 ./tests/test_hierarchical_allocator_benchmarks \
  --benchmark --gtest_filter='*WithQuotaParam*drf*'
```

## Reading the output

Each case prints its setup and timing to stdout, e.g.:

```
Added 3000 nodes in 412ms
Added 600 frameworks in 38ms
Made 3600 allocations in 1.7secs
Made 0 allocation in 21ms
```

- **Added N nodes / frameworks** — cluster construction time; not the metric
  under test, but a sanity check that setup cost isn't dominating.
- **Made K allocations in T** — the number under test: time for the allocator
  to offer all resources in one batch cycle.
- **Made 0 allocation in T** — the steady-state re-check. A second cycle should
  produce no new offers once everything is allocated; a non-zero count here is a
  correctness signal, not a timing one.

## Scope

This harness times and exercises the allocator at scale. It does not assert
gang-assembly outcomes (gangs assembled / timed out / partial launches) — those
hooks attach once gang assembly exists in the allocator. The deterministic
generator and timed-cycle loop are the substrate that work is designed against.

## Sorter benchmarks

The allocator benchmark above times end-to-end allocation. When a number there
looks slow, the sorter benchmark isolates the sort itself — the data structure
the allocator's fairness is built on.

Source: `tests/sorter_tests.cpp`. Target: `test_sorter`.

| Fixture | What it times |
|---|---|
| `CommonSorterTest.BENCHMARK_FullSort` | a full sort over a flat set of clients, across `DRFSorter` and `RandomSorter` |
| `CommonSorterTest.BENCHMARK_HierarchyFullSort` | a full sort over a hierarchy of roles/clients |

`test_sorter` is a normal discovered test target: its unit cases run on every
`ctest` pass. The two `BENCHMARK_` cases are skipped by the `BenchmarkFilter`
unless `--benchmark` is passed, so they only run on demand:

```bash
cmake --build build --target test_sorter
LIBPROCESS_IP=127.0.0.1 ./build/tests/test_sorter \
  --benchmark --gtest_filter='*BENCHMARK*'
```

(The same test-module prerequisite as above applies — the shared test `main()`
dlopen's them at startup.)
