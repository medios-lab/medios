# Agents

This is a guide for LLM/AI agents that want to write schedulers (frameworks) against medios.

If you are an AI coding assistant working **on** the medios codebase rather than as a consumer, see `CLAUDE.md` and `docs/DEVELOPMENT.md` instead.

## What medios is, in one paragraph

Medios is a resource broker for compute. A controller runs on one machine; one or more nodes run on machines with resources (cpus, mem, disk, gpu). The controller asks nodes "what have you got?", combines the answers into resource offers, and hands those offers to your scheduler. Your scheduler decides which tasks to launch on which offers, sends back accept/decline decisions, and gets status updates as tasks run and finish. The platform does not have built-in policy — your scheduler is the policy.

## Where you sit in the picture

```
+-------------+        HTTP /api/v1/scheduler       +-------------+
| your        | <----------------------------------> |             |
| framework   |   subscribe / offers / accept /      | medios-     |
| (scheduler) |   decline / launch / update / ack    | controller  |
+-------------+                                      +-------------+
                                                            |
                                                            | internal
                                                            v
                                                     +-------------+
                                                     | medios-node |
                                                     |  (one per   |
                                                     |   machine)  |
                                                     +-------------+
```

Your scheduler is HTTP client to `/api/v1/scheduler` on the controller. You never talk to nodes directly. You never run a daemon. Your scheduler can be a Python script, a Go binary, an LLM call wrapped in HTTP, a notebook — anything that can speak HTTP/1.1 with `Content-Type: application/json` and process a long-lived response stream.

## The loop, in order

1. **SUBSCRIBE.** Open `POST /api/v1/scheduler` with `{"type":"SUBSCRIBE","subscribe":{"framework_info":{...}}}`. The response is a long-lived stream of newline-delimited JSON events (RecordIO format — see [docs/recordio.md](docs/recordio.md)).
2. **Receive `SUBSCRIBED`.** The controller assigns your framework an ID. Save it; you'll need it for every subsequent call.
3. **Receive `OFFERS`.** The controller streams offers as nodes become available. Each offer is "node X has 4 CPUs, 8 GB mem, ..., for these N seconds." You don't have to take it.
4. **Decide.** Decline what you don't want; accept what you do. Acceptance carries one or more `TaskInfo` records — "run this command/container with these resources."
5. **Receive `UPDATE` events.** As your task runs, the node sends status updates: `TASK_STARTING`, `TASK_RUNNING`, `TASK_FINISHED`, `TASK_FAILED`, `TASK_KILLED`. **You must acknowledge updates** by sending `{"type":"ACKNOWLEDGE","acknowledge":{...}}` back, or the node retransmits. The driver in [FAC3-0014](docs/design/principles/FAC3-0014-pure-scheduler.md) shows exactly how minimal this is.
6. **TEARDOWN.** When you're done, send `{"type":"TEARDOWN","framework_id":...}` to clean up.

That's the whole API surface for a basic scheduler. Five message types in (`SUBSCRIBE`, `ACCEPT`, `DECLINE`, `ACKNOWLEDGE`, `TEARDOWN`), three out (`SUBSCRIBED`, `OFFERS`, `UPDATE`).

## Minimum viable scheduler

A scheduler that submits one task, watches it complete, and exits:

```python
import json
import os
import pwd
import requests

CONTROLLER = "http://127.0.0.1:5050"

# 1. Subscribe.
# IMPORTANT: framework_info.user must be a real OS user that exists on every
# node this framework's tasks will run on. Empty string => silent TASK_LOST
# at launch time on default --switch_user=true nodes. See "Common gotchas"
# below.
subscribe = {
    "type": "SUBSCRIBE",
    "subscribe": {
        "framework_info": {
            "user": pwd.getpwuid(os.getuid()).pw_name,
            "name": "hello-agent",
        }
    }
}
r = requests.post(
    f"{CONTROLLER}/api/v1/scheduler",
    json=subscribe,
    stream=True,
    headers={"Accept": "application/json"},
)

framework_id = None
medios_stream_id = r.headers["Medios-Stream-Id"]
task_submitted = False

# 2. Read the RecordIO event stream.
buf = b""
for chunk in r.iter_content(chunk_size=4096):
    buf += chunk
    # RecordIO: "<size>\n<size-bytes-of-json>"
    while b"\n" in buf:
        size_str, _, rest = buf.partition(b"\n")
        size = int(size_str)
        if len(rest) < size:
            buf = size_str + b"\n" + rest
            break
        event = json.loads(rest[:size])
        buf = rest[size:]

        if event["type"] == "SUBSCRIBED":
            framework_id = event["subscribed"]["framework_id"]["value"]
            print(f"subscribed as {framework_id}")

        elif event["type"] == "OFFERS" and not task_submitted:
            offer = event["offers"]["offers"][0]
            task = {
                "name": "hello",
                "task_id": {"value": "hello-001"},
                "node_id": offer["node_id"],  # see note below
                "resources": [
                    {"name": "cpus", "type": "SCALAR", "scalar": {"value": 0.5}},
                    {"name": "mem",  "type": "SCALAR", "scalar": {"value": 128}},
                ],
                "command": {"shell": True, "value": "echo hello-from-agent"},
            }
            requests.post(
                f"{CONTROLLER}/api/v1/scheduler",
                json={
                    "type": "ACCEPT",
                    "framework_id": {"value": framework_id},
                    "accept": {
                        "offer_ids": [offer["id"]],
                        "operations": [{
                            "type": "LAUNCH",
                            "launch": {"task_infos": [task]},
                        }],
                    },
                },
                headers={"Medios-Stream-Id": medios_stream_id},
            )
            task_submitted = True

        elif event["type"] == "UPDATE":
            status = event["update"]["status"]
            print(f"task {status['task_id']['value']} -> {status['state']}")
            # Acknowledge the update.
            requests.post(
                f"{CONTROLLER}/api/v1/scheduler",
                json={
                    "type": "ACKNOWLEDGE",
                    "framework_id": {"value": framework_id},
                    "acknowledge": {
                        "node_id": status["node_id"],
                        "task_id": status["task_id"],
                        "uuid": status["uuid"],
                    },
                },
                headers={"Medios-Stream-Id": medios_stream_id},
            )
            if status["state"] in ("TASK_FINISHED", "TASK_FAILED", "TASK_KILLED"):
                requests.post(
                    f"{CONTROLLER}/api/v1/scheduler",
                    json={
                        "type": "TEARDOWN",
                        "framework_id": {"value": framework_id},
                    },
                    headers={"Medios-Stream-Id": medios_stream_id},
                )
                exit(0)
```

This is real working code against the current branch. ~80 lines, no SDK, no driver, no framework boilerplate.

## Wire-format note: `node_id` vs `agent_id`

medios renamed v0 protos to use `node_*` vocabulary per FAC3-0021. DR-031 (#159) renamed the HTTP v1 JSON wire as well. Current controllers emit `node_id`.

The examples here use `node_id`, which is what the v1 protos require on `Offer`, `TaskInfo` and `ACKNOWLEDGE`. A scheduler that must also talk to a pre-DR-031 controller reads whichever field the `OFFERS` event carries and uses the same name in `ACCEPT` and `ACKNOWLEDGE`.

## Long-running schedulers

The example above exits after one task. A real scheduler keeps the subscription stream open for the lifetime of the framework. Pattern:

- Loop reading events from the stream forever
- Accept/decline offers as they arrive
- Acknowledge updates promptly (node retransmits unacked updates)
- On stream disconnect, re-subscribe with the same `framework_id` (set `framework_info.id` on the SUBSCRIBE call). Then send `RECONCILE`. The controller does not replay events from the disconnected period: unacknowledged status updates come back because the node retransmits them, and everything else sent while you were away is lost ([framework-networking.md](docs/framework-networking.md#events-across-a-lost-subscription)).

The Medios-Stream-Id header is mandatory on every non-SUBSCRIBE call. The controller uses it to correlate calls with the subscribe stream.

## What you can submit

A `TaskInfo` carries one of:

- **A shell command**: `{"shell": true, "value": "echo foo"}`. The node runs it via the command executor.
- **An OCI container**: the image format is portable, the task envelope is not. A node running `--containerizers=docker_shell` accepts `{"container": {"type": "DOCKER", "docker": {"image": "alpine:latest"}}}`. A node running `--containerizers=medios` accepts `{"container": {"type": "MEDIOS", "medios": {"image": {"type": "DOCKER", "docker": {"name": "alpine:latest"}}}}}` and refuses the other shape with `TASK_ERROR` / `REASON_TASK_INVALID`. A node runs one containerizer (FAC3-0025); the framework has to know which, from deployment configuration or a node attribute the operator sets. An image container under `medios` gets the image's `ENV` and no `HOME`.
- **A custom executor**: for long-lived task groups, attach an `executor_info` and the node manages the executor lifetime. Most schedulers don't need this.

A command or container task can also have the node fetch artifacts into its sandbox first — a binary, an archive, a config — by listing them in `command.uris` (over `http(s)://` or `file://`). This is how you deliver a job binary without pre-staging or SSH. See [task-uris.md](docs/task-uris.md).

The minimum required fields on a `TaskInfo` are `name`, `task_id`, `node_id` (from the offer), `resources`, and one of `command` / `container` / `executor`.

## What resources look like

An offer's `resources` is a list of typed scalar/range/set values:

```json
[
  {"name": "cpus", "type": "SCALAR", "scalar": {"value": 4}},
  {"name": "mem",  "type": "SCALAR", "scalar": {"value": 4096}},
  {"name": "disk", "type": "SCALAR", "scalar": {"value": 16384}},
  {"name": "ports", "type": "RANGES", "ranges": {"range": [{"begin": 31000, "end": 32000}]}},
  {"name": "gpus", "type": "SCALAR", "scalar": {"value": 1}}
]
```

To launch a task you carve out a subset and put it in the `TaskInfo.resources` field. The node enforces what you took.

GPU offers come from the Nvidia GPU isolator per DR-027. Tasks request `gpus` as a scalar resource just like `cpus`.

## Idempotency, retries, and failures

- **Task IDs are caller-assigned.** Use UUIDs or content-addressable IDs. Two identical tasks need different IDs.
- **Offers can be withdrawn.** The controller sends `RESCIND` when an offer is no longer valid: the node was lost, or the controller runs with `--offer_timeout` and the offer sat unaccepted past it. Without that flag offers do not time out. `unavailability` on an offer is maintenance metadata, not a deadline. Handle `RESCIND`; accept fast or decline explicitly.
- **Updates are at-least-once.** Acknowledge with the matching `uuid` from the update; the node stops retransmitting once it sees the ack.
- **Reconciliation.** On reconnect, send `{"type":"RECONCILE","reconcile":{"tasks":[...]}}` to ask the controller for the current state of tasks you remember. It replies with `UPDATE` events for each.

## What medios will NOT do for you

- Schedule. You decide which tasks go on which offers. The platform has no built-in algorithm.
- Authenticate. Default is `identity_assertion` — your framework asserts its principal and the controller takes your word for it (per DR-029, #120). For real auth, plug a module in via `--authenticators`.
- Persist your state. Your scheduler is responsible for its own checkpointing if it needs to survive a restart. The controller does not remember your framework's intent — it only knows which tasks are currently running.
- Multi-tenant fairness. No DRF weighting, no quotas, no preemption between frameworks. The deployment is single-tenant or trusted-tenant by design.
- Serve a UI. There's no built-in dashboard. Use `/api/v1/operator` to query state from your own tooling.

## Useful references

- [docs/scheduler-http-api.md](docs/scheduler-http-api.md) — full API reference (verbose, but authoritative)
- [docs/recordio.md](docs/recordio.md) — the wire framing for the subscribe event stream
- [docs/design/principles/FAC3-0014-pure-scheduler.md](docs/design/principles/FAC3-0014-pure-scheduler.md) — the philosophical motivation + a Python reference scheduler in <100 lines
- [docs/design/principles/FAC3-0017-offer-model.md](docs/design/principles/FAC3-0017-offer-model.md) — the resource-offer model, what it's good for, what it isn't
- [docs/operator-http-api.md](docs/operator-http-api.md) — `/api/v1/operator` for querying cluster state
- [docs/architecture.md](docs/architecture.md) — the controller/node split, isolators, containerizers

## Common gotchas

These are the things that produce non-obvious failures for first-time framework authors. None of them are visible in the wire-format docs; all of them cost real time.

**`framework_info.user` must be a real OS user.** Empty string, missing field, or a username that doesn't exist on the node all produce silent `TASK_LOST` with no executor logs to inspect. The node tries to `setuid` to the requested user before forking the executor; when the lookup fails the executor never starts. For local dev, use whatever `whoami` returns. For multi-node deploys, use a username that exists on every node. Tracked as #187.

**Default command executor needs `--launcher_dir` on the node.** If you submit a `TaskInfo` with a `command` field but no explicit `executor`, the node forks the default command executor binary (`medios-executor`) and locates it via `--launcher_dir`. Without that flag set, the executor binary isn't found and the task hits `TASK_LOST` at fork time. Operators running medios for HTTP frameworks should set `--launcher_dir=<install-prefix>/bin` (or `<build-dir>/src` for dev builds). Tracked as #188.

**`Medios-Stream-Id` is required on every non-SUBSCRIBE call.** Read it from the SUBSCRIBE response headers and pass it back on every subsequent `ACCEPT` / `DECLINE` / `ACKNOWLEDGE` / `TEARDOWN` / `KILL`. Missing the header gets you a 400 with no useful body. Frameworks that batch operations across reconnects often regress on this.

**Acknowledge `UPDATE` events promptly.** Updates are at-least-once. If you don't reply with `ACKNOWLEDGE` carrying the matching `uuid`, the node retransmits the same update. For schedulers that act on every update (e.g., advancing internal state machines), missing acks produce duplicate work and inflated event streams.

**Offers can be withdrawn.** The controller rescinds an offer when the node is lost, or when it runs with `--offer_timeout` and the offer sat unaccepted past it; `unavailability` is maintenance metadata, not a deadline. Accept fast or decline explicitly — sitting on an offer leaks resources from other frameworks' view for no benefit.

**Task IDs are caller-assigned and must be unique within the framework.** UUIDs are safe. Reusing a `task_id` for a launched-then-finished task is allowed but confusing — most schedulers don't bother.

**On macOS, `medios-execute` needs `LIBPROCESS_IP=127.0.0.1`** if your hostname isn't in DNS or `/etc/hosts` (default for most laptops). libprocess resolves the host hostname at startup to determine its own IP; failing that resolution kills `medios-execute` with a `Failed to obtain the IP address` error before it can submit anything. Pure-HTTP framework clients (which don't use libprocess) are unaffected. Tracked as #189.

## Recovering task output: the sandbox layout

The node writes each task's `stdout` and `stderr` (plus any files the task itself creates) into a sandbox directory under the node's `--work_dir`. Path layout for tasks launched via the default command executor:

```
<node_work_dir>/nodes/<node_id>/frameworks/<framework_id>/executors/<executor_id>/runs/<container_id>/
  stdout
  stderr
  <anything the task wrote to cwd>
```

Field source table:

| segment | comes from |
|---|---|
| `<node_work_dir>` | the `--work_dir` flag on `medios-node` (e.g. `/tmp/medios-node`) |
| `<node_id>` | `Offer.node_id.value` on the offer you accepted |
| `<framework_id>` | the `SUBSCRIBED` event's `framework_id.value` |
| `<executor_id>` | for default-command-executor tasks, defaults to the task's `name` field (not `task_id`) |
| `<container_id>` | a UUID the node assigns at launch time; available on the `TaskStatus.container_status.container_id.value` field in the first `TASK_RUNNING` update |

So to recover a finished task's stdout from a known offer:

```python
sandbox = (
    f"{NODE_WORK_DIR}/nodes/{node_id}/frameworks/{framework_id}"
    f"/executors/{executor_id}/runs/{container_id}/stdout"
)
with open(sandbox) as f:
    output = f.read()
```

The framework cannot precompute `<container_id>` — it has to come from a status update. Capture it during the first `TASK_RUNNING` event and save it alongside the task's metadata if you need to read sandbox files later.

A useful alternative: have the task itself write to a known path that the framework controls, e.g. `--command='echo result > /shared/path/result.txt'`. Bypasses the sandbox-layout dance entirely. Works best when framework + node share a filesystem.

Tracked as #190.

## Quick troubleshooting

**`medios-execute` hangs at "Subscribed with framework ID"**: no offers received. Check the node started and registered (look at its logs). Check the `--resources` flag advertises more than your task needs.

**Tasks accepted but never reach `TASK_RUNNING`**: the node received the task and is trying to launch it. Check the node's log for the task's executor sandbox path, then look at `stdout` / `stderr` in that directory.

**Every task immediately hits `TASK_LOST`, no executor logs**: usually `framework_info.user` is empty or doesn't exist on the node (see Common gotchas, #187), or the node was started without `--launcher_dir` so it can't find `medios-executor` (#188). The node log will show either `Creating sandbox … for user ''` (user issue) or a "could not find executor" line (launcher_dir issue).

**Framework disconnects mid-stream**: the controller closes the SUBSCRIBE stream if it doesn't see calls correlated to your `Medios-Stream-Id` for too long. Re-subscribe with `framework_info.id` set to your previous framework_id.

**Offers don't match what `--resources` says**: the offer carves out resources the controller currently considers available. Other frameworks may have outstanding offers on the same node. Use `GET_NODES` on the operator API to see the unallocated total.

## Reporting bugs / asking questions

Issues at https://github.com/medios-lab/medios/issues. Label the issue `agent-framework` if it's about the consumer-facing API surface this guide covers.
