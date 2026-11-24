# Watch a task's output for NaN

You end with a task whose progress message reports the first NaN in its output, within a second of the task printing it.

## Before you begin

- A controller you can reach: `curl -s http://<controller>:5050/state` returns JSON, with a node that runs the `medios` containerizer, which launches a bare command task.
- `medios-execute` from the release's `bin/` is on your PATH.
- A training program that prints its loss, here `train.py`.

## 1. Write the task with an observer

The observer reads the task's `stdout` file and prints one line when the loss is NaN. Save it as `/tmp/loss-watch.json`.

```json
{"name": "loss-watch", "task_id": {"value": "loss-watch-1"}, "node_id": {"value": ""},
 "principal": "<principal>",
 "resources": [{"name": "cpus", "type": "SCALAR", "scalar": {"value": 1}},
               {"name": "mem", "type": "SCALAR", "scalar": {"value": 512}}],
 "command": {"shell": true, "value": "python3 -u train.py"},
 "observers": [{"name": "nan", "command": {"shell": true,
   "value": "tail -F stdout | grep -m1 -i nan | sed 's/^/ISSUE /'"}}]}
```

## 2. Submit the task

```sh
medios-execute --controller=<controller>:5050 --task=file:///tmp/loss-watch.json
```

```text
Received status update TASK_STARTING for task 'loss-watch-1'
Received status update TASK_RUNNING for task 'loss-watch-1'
```

## 3. Read the progress message

```sh
curl -s http://<controller>:5050/tasks | python3 -c 'import json,sys
for t in json.load(sys.stdin)["tasks"]:
    if t["id"] == "loss-watch-1": print(t["statuses"][-1]["progress"]["message"])'
```

For a task that printed `step 4120 loss nan`:

```text
nan: ISSUE step 4120 loss nan
```

## Clean up

The task ends when `train.py` does, and the executor ends the observer with it. Remove the task file:

```sh
rm /tmp/loss-watch.json
```

## What's next

- [Task observers](task-observers.md)
- [Task observer fields](task-observer-fields.md)
