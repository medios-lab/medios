# Node resources inside a container

A container shares the host's kernel, so `sysconf(_SC_NPROCESSORS_ONLN)` and `sysinfo()` report the whole host even when the container may use two CPUs and 4 GB. When `--resources` leaves out `cpus` or `mem`, `medios-node` therefore sizes that resource from the limits placed on its own process, not from the host totals. This page describes those limits, where each kind of host keeps them, and what the node offers as a result.

## How the node sizes itself

For each of `cpus` and `mem` that `--resources` doesn't give, the node takes the lowest of the following:

| Resource | Host figure | Bounds applied |
|---|---|---|
| `cpus` | `sysconf(_SC_NPROCESSORS_ONLN)` | CPUs in the process's affinity mask (`sched_getaffinity`); the tightest CPU quota over the process's cgroup and its ancestors, which may be fractional |
| `mem` | `sysinfo()` total | the tightest memory limit over the process's cgroup and its ancestors |

The node then applies its usual memory reserve to the bounded figure: it keeps 1 GB back when the figure is 2 GB or more, and half otherwise. A 4 GiB limit gives 3072 MB, and a 3 GiB limit gives 2048 MB.

A bound only lowers a figure; a limit above the host total is ignored. A value given in `--resources` is used as written, and when both `cpus` and `mem` are given the node reads no limits at all. When a bound lowers a figure, the node logs one line at startup:

```
Auto-detected 192 cpus on the host; offering 2, the bound set by this process's CPU affinity mask and cgroup CPU quota
Auto-detected 791806576KB of memory on the host; sizing from 3906248KB, the cgroup memory limit on this process
```

A limit the node can't read is logged as a warning and treated as absent.

### cgroup v2

The node reads its own cgroup from the `0::` line of `/proc/self/cgroup`, then walks from that cgroup up through each parent to the root of its view of `/sys/fs/cgroup`. At each level:

- `cpu.max` holding `<quota> <period>` bounds the CPUs at `quota / period`; `max <period>` means no quota.
- `memory.max` holding a byte count bounds memory; `max` means no limit.

A control file is missing on the true root cgroup, and also wherever the parent hasn't enabled that controller in `cgroup.subtree_control`; a missing file means no limit at that level. The walk matters because a limit usually sits above the process. A systemd scope or a parent cgroup carries `cpu.max`, while the process runs in a child.

### cgroup v1

Each v1 controller is a separate hierarchy, mounted under `/sys/fs/cgroup/<controller>` and listed in `/proc/mounts` with type `cgroup`. For the `cpu` and `memory` hierarchies, the node reads its cgroup from the matching line of `/proc/self/cgroup` and walks up from there:

- `cpu.cfs_quota_us` over `cpu.cfs_period_us` bounds the CPUs; a quota of `-1` means no quota.
- `memory.limit_in_bytes` bounds memory. An unlimited v1 cgroup reports a value close to 2^63, which is above any host total and so has no effect.

Inside a Docker container on a v1 host, `/proc/self/cgroup` usually names the host path, such as `/docker/<id>`. Each hierarchy, however, is mounted with the container's own cgroup as its root, so that path doesn't exist under the mount. The walk skips the missing levels and reads the limit from the mount root.

A hybrid host mounts both versions. The node reads both, and the lowest bound from either one applies.

## What each kind of host exposes

The rows below were measured by starting `medios-node` without `--resources` under each setup and reading its registered resources.

| Host | cgroup layout seen by the node | Where the limits are | What the node bounds |
|---|---|---|---|
| RunPod CPU pod | Either v2 (`/sys/fs/cgroup` is `cgroup2fs`) or v1 (`/sys/fs/cgroup` is a `tmpfs` of read-only v1 hierarchies), depending on the host the pod lands on | v2: `memory.max` at the root of the view. v1: `memory/memory.limit_in_bytes` at the mount root; `cpu.cfs_quota_us` is `-1`. The CPU count comes from the affinity mask (`cpuset.cpus` such as `8,104`), not a quota. | CPUs by the affinity mask; memory by the pod's memory limit |
| Lambda or GCP VM, process in a systemd scope | v2 with systemd; a login shell sits in `/user.slice/.../session-N.scope` | `cpu.max` and `memory.max` on the scope or slice when set through `systemd-run -p CPUQuota= -p MemoryMax=` | Both, through the walk to the parent |
| Docker on a v2 host, default cgroup namespace | The container's cgroup is the root of the view (`0::/`) | `cpu.max` and `memory.max` on that root | `--cpus`, `--cpuset-cpus` and `--memory` |
| Docker on a v2 host with `--cgroupns=host` | `/proc/self/cgroup` names the container's cgroup by its host path | The same files under that path | The same as the default namespace |
| A medios task on a v2 node | The task runs in `<root>/<container id>/leaf` | The cgroups isolator enables the controllers and sets the task's limits on `<root>/<container id>`, the parent of `leaf` | Both, through the walk to the parent |
| Bare metal or a VM without limits | v2, or no cgroup limit on the node's process | None | The host totals stand, unless an affinity mask is set (for example with `taskset`) |

Measured offers, as cpus / mem in MB, for a 14-CPU, 55 GB v2 host and a 192-CPU v1 pod:

| Setup | Offered |
|---|---|
| v2 host, no limit | 14 / 55214 |
| v2 host, `taskset` to one CPU | 1 / 55214 |
| v2 host, parent cgroup `cpu.max 150000 100000` and `memory.max` 3 GiB | 1.5 / 2048 |
| v2 host, `systemd-run --scope -p CPUQuota=150% -p MemoryMax=3G` | 1.5 / 2048 |
| `docker run --cpuset-cpus=<one CPU> --memory=4g` | 1 / 3072 |
| `docker run --cpus=1.5 --memory=3g` | 1.5 / 2048 |
| `docker run --cgroupns=host --cpus=1 --memory=4g` | 1 / 3072 |
| v1 pod with a 2-CPU affinity mask and `memory.limit_in_bytes` 3999997952 | 2 / 2790 |

## Checking a host

These commands show what the node will see. Run them as the user and in the cgroup the node will run in:

```sh
stat -fc %T /sys/fs/cgroup          # cgroup2fs: v2; tmpfs: v1 or hybrid
cat /proc/self/cgroup               # this process's cgroup in each hierarchy
grep Cpus_allowed_list /proc/self/status
nproc                               # affinity mask count
getconf _NPROCESSORS_ONLN           # host CPU count
# v2, at the cgroup and each parent:
cat /sys/fs/cgroup/cpu.max /sys/fs/cgroup/memory.max
# v1:
cat /sys/fs/cgroup/cpu/cpu.cfs_quota_us /sys/fs/cgroup/cpu/cpu.cfs_period_us
cat /sys/fs/cgroup/memory/memory.limit_in_bytes
```

The node's startup log shows the result, both in the `Auto-detected` lines above and in the `Node resources:` line.

## Limits of this detection

- `cpu_set_t` holds 1024 CPUs. On a larger host `sched_getaffinity` fails, the failure is logged, and the host CPU count stands.
- Only the node's offered `cpus` and `mem` are bounded. A task that asks for an infinite CPU or memory limit still gets the host totals as its hard limit, and the OOM score adjustment divides by the host's memory (#8).
- A `medios-node` started inside another node's task reads that task's `MEDIOS_*` environment as flags, including the parent node's `--stores` (#7).
