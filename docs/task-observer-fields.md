# Task observer fields

Fields of `TaskObserver`, which appears as `TaskInfo.observers`, and what the controller and the executor make of them. The controller validates every entry at submit and refuses the task as `TASK_ERROR` with reason `REASON_TASK_INVALID` and the failing check in `message`.

`observers` (repeated `TaskObserver`, optional)
: Commands the executor runs beside the task for as long as it runs. At most eight per task; a ninth is refused with `Invalid observers: the task carries 9 observers; the limit is 8`. With the field absent the task runs with no observers. The controller copies the list onto the task record, where `/tasks` and `/state` show it.

`name` (string, required)
: One to thirty-two characters of `a-z`, `0-9` and `-`, unique among the observers on the task. It prefixes every progress message the observer produces. Refusals: `Invalid observers: 'name' is required on each observer`; `Invalid observers: 'name' '<name>' is <n> characters; the limit is 32`; `Invalid observers: 'name' '<name>' is outside [a-z0-9-]`; `Invalid observers: 'name' '<name>' is used by more than one observer on this task, so their lines could not be told apart`.

`command` (`CommandInfo`, required)
: The command the executor runs beside the task, in the task's sandbox, with the executor's environment. Shell form is accepted. Refusal: `Invalid observers: 'command' is required on observer '<name>'`.

## What the executor writes

`<name>: <line>`
: The observer's newest complete line, as the task's progress message, cut at 4096 bytes on a character boundary.

`<name>: exited <status>`
: The observer's exit status, once, after its last line has been sent. The executor does not restart it.

`<name>: did not start: <reason>`
: The observer could not be started. The task runs.

`observers/<name>.stderr`
: The sandbox file holding the observer's standard error.

## What's next

- [Task observers](task-observers.md)
