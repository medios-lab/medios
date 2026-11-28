# Devices cgroup inside a RunPod pod, 2026-11-27

Pod `on4fw8wagefagx`: Secure Cloud, EU-SE-1, 1× NVIDIA A40, image
`runpod/pytorch:1.0.2-cu1281-torch280-ubuntu2404`, kernel 6.8.0-138-generic.
Commands ran as container root through the RunPod `exec-pod` API. The pod was
deleted after the check.

| Check | Result |
| --- | --- |
| cgroup version | v1; `/sys/fs/cgroup` is a tmpfs with one cgroup mount per controller |
| `devices` hierarchy | Mounted `rw` at `/sys/fs/cgroup/devices`, rooted at the pod's own `/docker/<id>` cgroup |
| Ownership of `devices.*` files | uid/gid 65534 (unmapped), because container root is in a user namespace: `uid_map` is `0 165536 65536` |
| `mkdir /sys/fs/cgroup/devices/probe45` | `Permission denied` |
| Capabilities | `CapEff 00000000a80425fb`, Docker's default set; no `CAP_SYS_ADMIN` |
| Device allow list (`devices.list`) | Includes `c 195:0`, `195:254`, `195:255`, `505:0`, `505:1` (nvidia0, nvidia-modeset, nvidiactl, nvidia-uvm, nvidia-uvm-tools) |

The mount option says `rw`, but a process in the pod cannot create a child
devices cgroup. A medios node running inside this pod therefore cannot use
`cgroups/devices`, and the containerizer refuses the GPU module's device
declarations (`containerizer.cpp:282-299`).

Scope: one Secure Cloud pod in one data center. The pod from the 2026-11-26
collision ran on Secure Cloud in another data center and was not inspected.
