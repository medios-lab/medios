# Simulation

Medios carries two simulators for testing the scheduler and allocator without
real hardware. They sit at different layers and answer different questions. Both
run GPU-free and container-free on a laptop.

A node can advertise resources it does not physically have. Both simulators rely
on this: synthetic resource strings stand in for real CPUs, memory, disk, and
GPUs, so scheduling logic can be exercised at sizes and hardware profiles the
test machine does not own.

## Sim A — direct allocator

Drives the real `HierarchicalDRFAllocator` in-memory through its public surface
(`addNode`, `addFramework`, `updateQuota`, and the offer callback). No
controller, no node, no HTTP, no containerizer, no networking. Synthetic
`Resources` strings are passed straight to `addNode`.

- **Layer:** allocator only.
- **Cost:** microseconds per allocation; thousands of synthetic nodes in-memory.
- **Answers:** allocation *policy* — DRF fairness, role weights, quota
  satisfaction, starvation, and behavior under load.

Two parts in tree:

| Part | File | Role |
|---|---|---|
| Unit harness | `tests/hierarchical_allocator_tests.cpp` | per-behavior correctness (`TEST_F` / `TEST_P`) |
| Scale driver | `tests/hierarchical_allocator_benchmarks.cpp` | timed allocation cycles across thousands of synthetic nodes |

The scale driver builds as `test_hierarchical_allocator_benchmarks` and is kept
off the default `ctest` run. See [allocator-benchmarks.md](allocator-benchmarks.md)
for how to build, run, and read it.

## Sim B — in-process cluster

Runs the real controller, real allocator, real node(s), and the real scheduler
HTTP API together in one process. Only execution is faked: `TestContainerizer`
accepts a task launch and runs nothing. GPUs and other hardware are advertised
through the node's `--resources` flag.

- **Layer:** the full controller → allocator → scheduler-API path, over the real
  wire.
- **Cost:** one process, faked execution; tens of fake nodes.
- **Answers:** *protocol* correctness — offer handling, Accept/Decline
  transactions, task lifecycle events, and what the real controller does with a
  given scheduler call.

In tree:

| Component | File | Notes |
|---|---|---|
| In-process launch | `src/local/local.cpp` | `local::launch()` (line 156) stands up controller + allocator + node in one process |
| Cluster harness | `tests/cluster.cpp`, `tests/cluster.hpp` | `Controller::start` / `Node::create` for test fixtures |
| Fake execution | `tests/containerizer.cpp`, `tests/containerizer.hpp` | `TestContainerizer` — real harness code, accepts launches, runs nothing |

`local::launch()` is compiled into `libmedios` and is reached from the scheduler
and node libraries; the cluster harness and `TestContainerizer` are used
directly by C++ test fixtures.

### What is not wired today

`src/local/main.cpp` exists as a standalone entry point, but no `add_executable`
declares it and no `medios-local` binary is built. Sim B is therefore available
as a **C++ harness** (from test fixtures, via `cluster.cpp` and
`local::launch`), not as a runnable `medios-local` command line. Standing up the
in-process cluster from the shell requires wiring that executable target.

## Choosing between them

| | Sim A | Sim B |
|---|---|---|
| Exercises | allocation policy | scheduler/controller protocol |
| Layer | allocator, in-memory | full controller → allocator → scheduler API |
| Scale | thousands of nodes, microseconds | tens of fake nodes, faked execution |
| Faked | everything except the allocator | execution only (`TestContainerizer`) |
| Entry point | `test_hierarchical_allocator_benchmarks` (built) | C++ test fixtures (`medios-local` CLI not built) |

Sim A when the question is *who gets what, and is that fair and fast at scale*.
Sim B when the question is *does the real wire behave correctly for a given
sequence of scheduler calls*.

## What neither does

- **No real execution.** Nothing launches a real process or container. Sim A has
  no execution layer at all; Sim B fakes it.
- **No real hardware.** GPUs and other resources are advertised, not present.
  Neither simulator verifies that a real device is bound or isolated.
- **No network fault model.** Sim B runs in one process; it does not model a
  real multi-host network, partitions, or latency.
