# Medios Python SDK: the calls a first framework needs

Package `medios_sdk`, available through `bin/medios-python`.

```python
from medios_sdk import medios                 # wire types and constants
from medios_sdk.scheduler import Client, SchedulerError
from medios_sdk.sandbox import Resolver       # reads task output files
```

## Connect

```python
client = Client(controller_url, "euler-sums")   # runs tasks as the current OS user
for ev in client.subscribe():                   # blocks; one event at a time
    ...
```

`ev.type` is one of `medios.EVENT_SUBSCRIBED`, `EVENT_OFFERS`, `EVENT_UPDATE`,
`EVENT_HEARTBEAT`, `EVENT_RESCIND`, `EVENT_ERROR`. After SUBSCRIBED,
`client.framework_id` holds the id the controller assigned.

## Offers

`ev.offers.offers` is a list of `medios.Offer`. Each has `id`, `node_id` and
`resources`, a list of `medios.Resource` with `name` (`"cpus"`, `"mem"`,
`"disk"`, `"gpus"`, `"ports"`) and, for scalars, `scalar.value`.

Answer every offer, either by launching tasks on it or by declining it.
An offer you ignore stays reserved for you and no other framework gets it.

```python
client.accept([offer.id], [task, task, ...])    # several tasks may share one offer
client.decline([offer.id], 120.0)               # don't re-offer it to me for 120 s
```

The tasks in one accept must fit inside that offer's resources together.

## A task

```python
task = medios.TaskInfo(
    name="zeta-2",
    task_id=medios.ID(value="zeta-2"),          # unique within the framework
    node_id=offer.node_id,
    principal="operator:preview",               # required: who the task runs for, kind:name
    resources=[
        medios.Resource(name="cpus", type="SCALAR", scalar=medios.Scalar(value=0.5)),
        medios.Resource(name="mem",  type="SCALAR", scalar=medios.Scalar(value=64)),
    ],
    command=medios.CommandInfo(shell=True, value="echo hello"),
)
```

`principal` must match `(operator|team|run):[A-Za-z0-9._-]+`; a missing one
fails when you construct the `TaskInfo`.

## Status updates

`ev.update.status` is a `medios.TaskStatus` with `task_id`, `node_id`,
`state`, `message`, `uuid` and `container_status`.

```python
st = ev.update.status
if st.uuid:
    client.acknowledge(st.task_id.value, st.node_id.value, st.uuid)  # or the node resends it
if st.container_status:
    container_id = st.container_status.container_id.value   # keep it: needed to read output
if st.state.is_terminal():
    ok = st.state == medios.TASK_FINISHED   # st.state.is_failure() for the failed states
```

The container id arrives on an early update (TASK_STARTING or TASK_RUNNING),
not on the terminal one, so store it per task id.

## Reading a task's stdout

```python
resolver = Resolver(work_dir=run["work_dir"] + "/node", node_http_port=run["node_port"])
path = resolver.path_for(st.node_id.value, client.framework_id,
                         task_id, container_id, "stdout")
text = resolver.read("127.0.0.1", path).data
```

`work_dir` is the node's work directory, which `bin/medios-try up` puts at
`<work_dir from run.json>/node`.

## Finishing

Call `client.teardown()` while you are still inside the `for ev in
client.subscribe()` loop, then `break`. Leaving the loop first closes the event
stream, the controller drops the framework, and `teardown()` then fails with
`Framework cannot be found`.

Errors from any call raise `SchedulerError` with the HTTP status and the
controller's message.
