# Run your first observed task

In about five minutes you submit a task, watch an observer report on it, and read the result off the controller.

## What you need

- A controller at `<controller>:5050` with one node registered to it that runs the `medios` containerizer, which launches a bare command task.
- `medios-execute` from the release's `bin/`.

## 1. Write the task

The task prints one progress line and sleeps. The observer prints two lines six seconds apart, then waits to be ended with the task. Save it as `/tmp/first.json`.

```json
{"name": "first-observed", "task_id": {"value": "first-observed-1"}, "node_id": {"value": ""},
 "principal": "<principal>",
 "resources": [{"name": "cpus", "type": "SCALAR", "scalar": {"value": 0.2}},
               {"name": "mem", "type": "SCALAR", "scalar": {"value": 64}}],
 "command": {"shell": true, "value": "echo 'progress: 50, half'; sleep 14"},
 "observers": [{"name": "obs", "command": {"shell": true,
   "value": "echo OK first look; sleep 6; echo ISSUE late look; sleep 60"}}]}
```

## 2. Submit it

```sh
medios-execute --controller=<controller>:5050 --task=file:///tmp/first.json
```

```text
Received status update TASK_STARTING for task 'first-observed-1'
Received status update TASK_RUNNING for task 'first-observed-1'
Received status update TASK_RUNNING for task 'first-observed-1'
Received status update TASK_RUNNING for task 'first-observed-1'
Received status update TASK_FINISHED for task 'first-observed-1'
```

`medios-execute` prints one line per status update; a `TASK_RUNNING` arrives each time the observer's line changes.

## 3. Read the statuses

```sh
curl -s http://<controller>:5050/tasks | python3 -c 'import json,sys
for t in json.load(sys.stdin)["tasks"]:
    if t["id"] == "first-observed-1":
        for s in t["statuses"]: print(s["state"], s.get("progress", {}).get("message"))'
```

```text
TASK_STARTING None
TASK_RUNNING obs: ISSUE late look
TASK_FINISHED obs: ISSUE late look
TASK_KILLED obs: ISSUE late look
```

The record keeps the newest status of each state. The final `TASK_KILLED` is the kill `medios-execute` sends as it exits, after the task has already finished.

## What you built

The task reported its own progress, and the observer put a judgment about it on the same record, where the later line replaced the earlier one. The observer's standard error is in the task's sandbox at `observers/obs.stderr`.

## What's next

- [Task observers](task-observers.md)
- [Watch a task's output for NaN](watch-task-output-for-nan.md)
