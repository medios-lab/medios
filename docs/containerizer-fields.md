# Which task fields each containerizer honors

A node runs every task through one containerizer, chosen by its
`--containerizers` flag. The two containerizers read different parts of a `TaskInfo`, so the
same task can behave differently depending on where it lands. This page lists,
for each field a framework is likely to set, what each containerizer does with
it.

`medios` is the in-tree containerizer. What it honors depends on the
isolators the node enables with `--isolation`, so its column names the
isolator a field needs. `docker_shell` runs each task with `docker run`
through `medios-docker-executor`; it has no isolators, and its column
describes the `docker run` arguments it builds.

"Ignored" means the task runs as if the field were absent, with no status
or log line saying so. "Refused" means the task ends in `TASK_ERROR` with
reason `REASON_TASK_INVALID` and a message naming the field.

## Command and environment

| Field | `medios` | `docker_shell` |
|---|---|---|
| `command.value`, `shell`, `arguments` | Honored | Honored; the command runs inside the image |
| `command.environment` `VALUE` variables | Honored | Honored, as `--env` |
| `command.environment` `SECRET` variables | Honored | Ignored |
| `command.uris` | Fetched into the sandbox ([task-uris.md](task-uris.md)) | Fetched into the sandbox, which is bind-mounted into the container at the same path |
| `command.user` | Honored when the node allows switching user | Ignored; the container runs as its image's user |
| `kill_policy.grace_period` | Honored | Honored, as `docker stop --time`, rounded up to whole seconds; 10 s when unset |
| `max_completion_time` | Honored | Ignored |

## Resources

| Field | `medios` | `docker_shell` |
|---|---|---|
| `cpus`, `mem` | Enforced with `cgroups/cpu` and `cgroups/mem` (or `posix/*`); accounted only otherwise | Accounted by the controller; not passed to Docker, so the container is not limited |
| `disk` | Enforced with `disk/du`; accounted only otherwise | Accounted only |
| `ports` | Accounted; the task runs in the node's network namespace and binds the ports there | Accounted. On the default bridge, each port is published to the same host port (`--publish p:p`). With `docker.network: HOST`, nothing is published and the task binds the ports in the node's network namespace, as under `medios`; nothing stops it binding ports it was not granted |
| `gpus` | Assigned, not enforced, with `com_medioslab_medios_gpu_assign`. It picks the devices and sets `CUDA_VISIBLE_DEVICES` to their UUIDs, which only tells CUDA which devices to use. A container without its own rootfs sees the host's `/dev` and can open any GPU's device file, or set `CUDA_VISIBLE_DEVICES` itself in its command. A container with its own rootfs gets a private `/dev` with no GPU device files. Enforced by also listing `com_medioslab_medios_gpu_devices` (requires `cgroups/devices`): the container can open only the assigned GPUs' device files, which are bound into its rootfs if it has one. With neither isolator, accounted only, and CUDA uses device 0 unless the task sets `CUDA_VISIBLE_DEVICES` | Whole GPUs only. The node treats its declared `gpus` as Docker device indices `0..N-1` and passes the granted ones as `--gpus` and `NVIDIA_VISIBLE_DEVICES`; a container granted none gets `NVIDIA_VISIBLE_DEVICES=void` and sees no GPU. A fractional declaration or grant is refused. Assignments are not recovered after a node restart (#149) |

## ContainerInfo

| Field | `medios` | `docker_shell` |
|---|---|---|
| `type` | `MEDIOS` or `DOCKER` | `DOCKER` only; anything else is not launched on this node |
| `docker.image` | Pulled and run as the container's root filesystem with `filesystem/linux` and `docker/runtime` | Honored |
| Image config `User` | Ignored, with a warning in the node log. The task runs as `command.user` or the framework's `user` when the node switches user, and as the node's own user otherwise | Honored; the container runs as the image's user |
| `docker.force_pull_image` | Honored | Honored |
| `docker.network` | Ignored | Unset or `BRIDGE`: Docker's default bridge. `HOST`: `--network=host`, the node's network namespace. `NONE` and `USER`: refused |
| `docker.port_mappings` | Ignored | Ignored |
| `docker.privileged` | Ignored | Ignored |
| `docker.parameters` | Ignored | Honored for allowlisted task-level `docker run` flags (`entrypoint`, `user`, `workdir`, `hostname`, `stop-signal`, `ulimit`, `init`, `read-only`, `tmpfs`, `add-host`, `dns`, `dns-search`, `dns-option`, `label`, `log-opt`), passed as `--<key>=<value>` before the image (`#574`, e.g. `key: "entrypoint", value: ""`); any other key is refused |
| `volumes` (`STORE` source) | Honored with `filesystem/linux`; refused when the node cannot deliver stores | Honored, as `--volume` |
| `volumes` (any other source) | Not covered by this page | Refused |
| `network_infos` | Ignored; there is no network isolator | Ignored |
| `hostname` | Ignored | Ignored |
| `rlimit_info` | Honored with `posix/rlimits` | Ignored |
| `tty_info` | Honored | Ignored |

## LinuxInfo

| Field | `medios` | `docker_shell` |
|---|---|---|
| `ipc_mode: PRIVATE` | Own IPC namespace, with `namespaces/ipc` | `--ipc=private` |
| `ipc_mode: SHARE_PARENT` | For a top-level task, the node's IPC namespace and `/dev/shm`, with `namespaces/ipc` | Refused |
| `shm_size` (MB) | With `namespaces/ipc`: the size of the container's `/dev/shm`, and refused unless `ipc_mode` is `PRIVATE`. Without it: ignored; an image container gets a tmpfs `/dev/shm` with the kernel's default size, half of the node's RAM | `--shm-size`; refused unless `ipc_mode` is `PRIVATE`, and refused when 0. When unset, Docker's default of 64 MB |
| `capability_info`, `bounding_capabilities` | Honored with `linux/capabilities` | Ignored |
| `seccomp` | Honored with `linux/seccomp` | Ignored |
| `share_pid_namespace` | Honored with `namespaces/pid` | Ignored |

## Image provisioning and `--executor_registration_timeout` (#484)

For the `medios` containerizer, both OCI image provisioning (`Provisioner::provision`)
and `CommandInfo.uris` artifact fetching run before the executor registers with
`medios-node` and therefore count against `--executor_registration_timeout`
(default `1mins`). For the `docker_shell` containerizer, `CommandInfo.uris`
fetching likewise runs before `medios-docker-executor` registers (counting
against `--executor_registration_timeout`), whereas `docker pull` runs inside
`medios-docker-executor` after registration during `TASK_STARTING`. On nodes
that pull multi-gigabyte OCI images or artifacts for the first time (for example
CUDA or ML runtime images), operators should increase
`--executor_registration_timeout` (e.g. `--executor_registration_timeout=15mins`
or higher, sized for image/artifact size and network bandwidth) so first-time
provisioning is not terminated before the executor can register.

## Node restart and recovery (`docker_shell`, #149)

The `docker_shell` containerizer is intentionally stateless across `medios-node`
restarts: it does not checkpoint container state or re-attach to running Docker
containers on recovery. Tasks interrupted by a `medios-node` restart transition
to terminal/unreachable (`TASK_LOST` / `TASK_FAILED`), and frameworks are
expected to resubmit interrupted work from their own durable state.

## Finding out what a node runs

A node does not advertise its containerizer in offers by itself. Its
`--containerizers` and `--isolation` values are in the `flags` object of the
node's `/state` endpoint, and an operator can also declare the containerizer
as a node attribute with `--attributes`, which does appear in offers. A
framework that depends on a field in the "Ignored" cells above can constrain
placement on such an attribute.
