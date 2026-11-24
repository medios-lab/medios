# Task observers

An observer is a command the executor runs beside a task, in the task's sandbox, for as long as the task runs. Each line the observer prints becomes the task's progress message, prefixed with the observer's name, so the task's record can carry a judgment the task never makes about itself. A task carries up to eight observers, and the controller records them with the task.

## An observer runs in the sandbox with the executor's environment

The executor starts each observer after the task's own process exists, with the sandbox as the working directory. The task's standard output is the sandbox file `stdout`, so an observer that watches the task reads `./stdout`; one that watches something else reads that instead. The observer receives the executor's environment rather than the task's, which is where a node names anything an observer needs from the machine, such as a credential file.

## An observer's output is the task's progress message

The executor reads the observer's standard output a line at a time, once a second, and the newest complete line becomes the task's progress message as `<name>: <line>`, cut at 4096 bytes on a character boundary. The line stays as the message until the task prints a newer progress line of its own, so on a task that reports progress often an observation is visible for a few seconds, and on a quiet task it stays until the next observation. The observer's standard error goes to the sandbox file `observers/<name>.stderr`.

## An observer has no authority over the task

Nothing an observer prints changes the task's progress value, resets its stall clock, or ends the task. The task's progress policy owns the value and the clock; an observer only ever sets the message.

## An observer lives and dies with the task

The executor signals observers when it signals the task, SIGTERM and then SIGKILL on the task's own grace timer, across each observer's whole process group. An observer that exits on its own is not restarted; its exit appears once as `<name>: exited <status>`. An observer that fails to start appears once as `<name>: did not start: <reason>`, and the task runs regardless. Observers run inside the task's container or cgroup and count against the task's allocation.

## Example

```json
"observers": [{"name": "nan", "command": {"shell": true,
  "value": "tail -F stdout | grep -m1 -i nan | sed 's/^/ISSUE /'"}}]
```

For a task that prints `step 4120 loss nan`, the task's progress message reads `nan: ISSUE step 4120 loss nan` within a second.

## Ways to use an observer

- A sampler: `nvidia-smi --query-gpu=utilization.gpu,memory.used --format=csv,noheader` every sixty seconds puts the device's state beside the task's progress.
- A checkpoint age: `stat -c %Y checkpoints/latest` against the clock, printed every few minutes, shows a run that stopped saving.
- An output-silence timer: the age of the `stdout` file's last write, printed every thirty seconds, catches a task whose progress value still moves while its logs have stopped.
- A log forwarder: `tail -F stdout | grep -E '^(epoch|eval)'` puts the lines a person reads onto the record and leaves the rest in the sandbox.
- A model's judgment: a program that sends the last few hundred lines of `stdout` to a language model every few minutes and prints `OK <reason>` or `ISSUE <reason>`, with its credential read from a file the executor environment names.
- Two observers with different questions, told apart by name, so their disagreement is on the record.

## What's next

- [Watch a task's output for NaN](watch-task-output-for-nan.md)
- [Run your first observed task](run-your-first-observed-task.md)
- [Task observer fields](task-observer-fields.md)
