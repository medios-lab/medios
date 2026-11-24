# Medios

**A small distributed compute system for scheduling research.**

Medios is a resource broker derived from Apache Mesos. A controller allocates the CPUs, memory and GPUs of a set of machines to external schedulers, launches the tasks those schedulers choose, and reports each task's lifecycle back to them. Every scheduling decision belongs to the scheduler, so a researcher can test a placement or admission policy on real machines by writing a scheduler of their own, without building a distributed execution system for each experiment.

Medios keeps the parts of Mesos that run and isolate work: the controller, the node agent, the resource allocator, the containerizers and the Linux isolators. It drops the surrounding platform, including ZooKeeper and the replicated log, CSI storage and persistent volumes, the Java and Python bindings, the built-in authenticators and the web UI. What remains is small enough to read and modify, and it runs real workloads across multiple machines.

## Design

Medios follows the resource-offer model introduced by Apache Mesos. Nodes advertise their resources, the controller offers those resources to frameworks, and each framework decides which work to run, where to place it and how to respond to failure.

A cluster is one controller and one or more nodes. Frameworks talk to the controller through the scheduler HTTP API (`/api/v1/scheduler`, JSON or protobuf), which any HTTP client can use, or through the C++ `MediosSchedulerDriver`, which wraps the same protocol. The Go and Python SDKs (`medios-sdk-go`, `medios-sdk-python`) wrap it for frameworks written in those languages.

Medios supports:

- **Resource offers:** CPU, memory, disk, ports and GPU resources are offered to frameworks, which control task placement and scheduling.
- **Task execution:** command tasks and OCI or Docker container tasks, reported through the `TASK_STARTING`, `TASK_RUNNING` and `TASK_FINISHED` (or `TASK_FAILED`, `TASK_KILLED`) states. Isolation uses cgroups v1 or v2, PID and IPC namespaces, Linux capabilities and seccomp, POSIX rlimits, and an NVIDIA GPU isolator that loads as a module.
- **Gang admission:** a framework sends a `REQUEST` call listing one resource shape per node. The allocator grants all of the shapes on distinct nodes in a single offers event, or grants none of them.
- **Heterogeneous resources:** a resource can carry opaque key/value metadata, such as `class=h100` on a GPU resource. The allocator matches metadata by equality and does not interpret it, so hardware types are described without putting hardware-specific policy in the controller.
- **Independent frameworks:** several schedulers can run against one cluster, each with its own policy and workload semantics.

## Research

Medios is intended for experiments in distributed resource management and scheduling, such as gang scheduling, resource fragmentation, scheduling for heterogeneous hardware, utilization under competing workloads, and the interaction between scheduling policy and execution behavior. Gang admission and typed GPU resources exist because AI training, inference and agent workloads are where those questions are sharpest today.

The repository also carries two simulators that drive the same allocator with synthetic nodes, without containers or GPUs ([docs/Simulation.md](docs/Simulation.md)). A policy can be tried in simulation at sizes no lab owns and then run unchanged against real machines and workloads.

Medios does not prescribe a scheduling policy or a workload model. Researchers can write frameworks, change the allocator, and compare alternatives on a common execution substrate.

## Getting started

[docs/DEVELOPMENT.md](docs/DEVELOPMENT.md) covers prerequisites, the CMake build, running a one-controller, one-node cluster, and the test suite. [docs/scheduler-http-api.md](docs/scheduler-http-api.md) describes the protocol a framework speaks.

## Scope

Medios does not try to reproduce Kubernetes or the full capabilities of Apache Mesos. New functionality should serve one of two purposes:

1. Improve the ability to conduct scheduling experiments.
2. Make the existing system easier to understand, operate or modify.

Feature completeness and broad workload compatibility are not goals.

## Origins

Medios began as a fork of Apache Mesos in May 2026. It preserves the resource-offer architecture and much of the original execution code, simplifies the system around them, and adds support for current compute workloads such as gang admission and typed GPU resources.

Medios is released under the Apache License 2.0.

## Status

Medios is experimental software for research, not a production infrastructure service. It runs command and container tasks across distributed nodes. A GoogleTest suite in `tests/` covers resource allocation (including gang admission), task execution and containerization, and controller restart behavior.
