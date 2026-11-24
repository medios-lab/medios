# Task finish hooks

A finish hook is a second command the executor runs in the task's sandbox after the work command exits, whatever ended it: a clean exit, a failure, a signal, a stall kill, or a kill from the framework. A task carries one hook and a cap on how long the hook may run, and the controller records both with the task. The task's state and reason stay the work command's; the hook reports its own outcome as a typed field on the terminal status, so a run that was killed still gets to write its last word without that changing what happened to the work.

## A task declares one hook and the cap it runs under

`TaskInfo.finish` is a `TaskFinish`, which carries two fields.

`command` (`CommandInfo`, required)
: The command the executor runs after the work command exits. `value` is required on it, because the launcher requires `value` on both its shell and its argv path; `arguments` add to `value` and never stand in for it. `environment` on it holds the framework's variables for the hook.

`cap` (`DurationInfo`, required)
: How long the hook may run. It must be positive, and no larger than the task's `max_completion_time` when the task sets a positive one. There is no default: an upload and a checkpoint flush differ by orders of magnitude, so a cap medios picked for you would either cut the hook off or hold the node's resources while the hook sits idle.

The controller copies the field onto the task record as `Task.finish`, where `/state` shows it beside the task's statuses, so you can read an outcome against the command and the cap that produced it.

## The hook starts when the work command is reaped

The executor builds the work command's terminal status first, from the work's wait status, and starts the hook only after that status is settled. A stall kill still reports `TASK_FAILED` with reason `REASON_PROGRESS_STALLED`, and a framework kill still reports `TASK_KILLED`, each with the hook's outcome beside it. The framework sees `TASK_RUNNING` until the hook ends, because the terminal status is the message that carries the outcome.

Observers are terminated with the work command, as they are on a task with no hook; they observe the work and not the hook. Nothing reads progress while the hook runs and the stall clock is stopped, because progress and health checks belong to the work command. A framework that relaunches the work submits a new task, with its own sandbox, and that task's hook runs there.

## The hook runs inside the task's deadline

With no `max_completion_time` on the task, the hook's limit is the cap. With a positive one, the limit is the smaller of the cap and what the ceiling has left when the work command is reaped, less the task's kill-policy grace period, which is 3 seconds when the task sets no kill policy. The grace comes out of the limit because the executor sends SIGTERM at the limit and SIGKILL one grace period later, so the last signal the hook can receive lands no later than the deadline the task was launched with. When the ceiling has nothing positive left after the grace is taken out, the hook does not start.

The limit that applied is reported as `FinishStatus.limit`, and how long the hook ran as `FinishStatus.duration`.

## The hook gets the executor's environment plus four variables it cannot override

The hook's environment is built from three sources in order, each overriding the names the one before it set: the executor's own process environment, which is what the node gave the executor through `--executor_environment_variables`; then `TaskFinish.command.environment`; then these four, which nothing a framework sets can displace.

`MEDIOS_WORK_EXIT_STATUS`
: The work command's exit code when it exited, and 128 plus the signal number when a signal ended it, which is the shell's `$?` convention.

`MEDIOS_TASK_ID`
: The value of the task's `TaskID`.

`MEDIOS_FRAMEWORK_ID`
: The value of the framework's `FrameworkID`.

`MEDIOS_NODE_ID`
: The node the task ran on. The node sets this for every executor it starts, and the hook does not start when it is missing, because identity is never defaulted.

The hook runs through the same launcher the work command does, as the task's uid, with the sandbox as its working directory, in the task's rootfs when the task has one, and inside the task's container and cgroup, so its processor time and memory count against the task's allocation. With a rootfs, the hook gets its own mount namespace and the task's view is rebuilt in it from the task's mounts, clone namespaces and symlink operations. An isolator's pre-exec commands prepare the work command and are not run again for the hook.

## The hook writes to its own sandbox files

Standard output goes to the sandbox file `finish.stdout` and standard error to `finish.stderr`, beside the work command's `stdout` and `stderr`. The executor creates both before the hook starts. Medios reads neither; what the hook writes, and where it goes from the sandbox, belongs to the framework.

## The outcome is a typed field with five states

`TaskStatus.finish` is a `FinishStatus` on the terminal status of a task that carried a hook. Its `state` is one of five, and no state is zero, so a status whose `finish` has no state is never read as a hook that ran.

`RAN`
: The hook exited on its own. `exit_code` carries the exit code, or `signal` the signal number when a signal ended it.

`TIMED_OUT`
: The hook was still running at `limit`, so the executor killed it with SIGTERM and then SIGKILL after the grace period.

`KILLED`
: A kill or an executor shutdown arrived while the hook was running and was applied to the hook by the same escalation. The work's state is whatever the work command earned, so a kill during the hook of a work command that succeeded reports `TASK_FINISHED` with `finish.state` of `KILLED`.

`DID_NOT_START`
: The hook never started, and `reason` says why: the work command was killed at the task's `max_completion_time`, so nothing of the ceiling is left; the ceiling had nothing left for the hook once the kill grace period was taken out of it; the executor is shutting down and the node is about to destroy the container; the work command's exit status is unknown; `MEDIOS_NODE_ID` is not set in the executor's environment; or the command could not be forked, with the launcher's error.

`LOST`
: The hook ran and its exit status could not be read. `reason` carries the read error, and `limit` and `duration` are known.

A `finish` that arrives with no `state`, a `RAN` with neither `exit_code` nor `signal`, or a `DID_NOT_START` or `LOST` with no `reason`, is dropped from the status at the node and at the controller with a warning, counted on `/metrics` as `node/invalid_task_finish_outcomes` and `controller/invalid_task_finish_outcomes`, and the rest of the status is delivered.

## The hook has no authority over the work command

Nothing the hook does changes the task's state, its reason, or its message, all of which are settled before the hook starts. The hook cannot extend the task's `max_completion_time` or any other deadline, and it cannot make the task's end later than the deadline it was launched with.

## The controller refuses a hook the command executor would not run

The built-in command executor runs the hook. A task that would run under a different executor is refused at submit as `TASK_ERROR` with reason `REASON_TASK_INVALID` and the failing check in `message`, before anything is launched.

- The task names its own executor: `Invalid finish: the task names executor '<executor-id>', which does not run a finish hook; the built-in command executor runs it`
- The task is a docker task, with `ContainerInfo.type` of `DOCKER`: `Invalid finish: the task is a docker task, and the docker executor does not run a finish hook; the built-in command executor runs it`
- The task is in a task group, which runs under the default executor: `Invalid finish: a task in a task group runs under the default executor, which does not run a finish hook`
- `Invalid finish: 'command' is required`
- `Invalid finish: 'command.value' is required`
- `Invalid finish: 'cap' is required; there is no default, as for 'stall_window'`
- `Invalid finish: 'cap' must be positive`
- `Invalid finish: 'cap' <cap> is larger than the task's 'max_completion_time' <ceiling>; the cap is counted inside the task's ceiling and the executor never extends a deadline`

## Example

The task writes checkpoints as it trains and may be killed before it finishes; the hook packs whatever is on disk and records what ended the work.

```json
{"name": "train", "task_id": {"value": "train-1"},
 "resources": [{"name": "cpus", "type": "SCALAR", "scalar": {"value": 2}},
               {"name": "mem", "type": "SCALAR", "scalar": {"value": 4096}}],
 "command": {"shell": true, "value": "python3 -u train.py"},
 "max_completion_time": {"nanoseconds": 3600000000000},
 "finish": {"cap": {"nanoseconds": 120000000000},
            "command": {"shell": true, "value":
              "tar czf final.tgz checkpoints; echo work=$MEDIOS_WORK_EXIT_STATUS"}}}
```

A kill partway through reports the work as killed and the hook as having packed the checkpoints, on one status:

```text
{"state": "TASK_KILLED",
 "finish": {"state": "RAN", "exit_code": 0,
            "limit": {"nanoseconds": 120000000000},
            "duration": {"nanoseconds": 11400000000}}}
```

## What's next

- [Task observers](task-observers.md)
- [Task state reasons](task-state-reasons.md)
- [Scheduler HTTP API](scheduler-http-api.md)
