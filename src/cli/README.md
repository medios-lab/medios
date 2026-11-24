# src/cli

Command-line entry points for medios.

## `execute.cpp` → `medios-execute`

A minimal scheduler that launches a single task (or task group) against a
running controller and reports its terminal status. It is the smallest complete
framework in the tree: it subscribes over the `/api/v1/scheduler` HTTP API,
accepts the first offer that fits, launches the task, prints status updates, and
exits when the task reaches a terminal state (`TASK_FINISHED`, `TASK_FAILED`,
`TASK_KILLED`, ...).

Two roles:

- **Hello-world / smoke driver.** The quickstart in the top-level README uses it
  to prove an end-to-end task launch:
  `medios-execute --controller=127.0.0.1:5050 --name=hello --command='echo hello'`.
- **End-to-end test harness.** The `test_hello_world` and `test_docker_shell`
  suites spawn it as a real process against a real controller + node
  (`tests/e2e/cluster_harness.hpp`), so it doubles as the executable contract for
  "can medios run a task."

### Task specification

Either describe the task inline or pass a full `TaskInfo` as JSON:

- Inline: `--name`, `--command` (with `--shell` for `/bin/sh -c` vs. exec),
  `--environment`, `--resources` (e.g. `cpus:1;mem:128`), `--containerizer`.
- Explicit: `--task=<json>` (a single `TaskInfo`) or `--task_group=<json>`.

Other flags cover the surfaces a real framework needs to exercise: `--role`,
`--volumes`, `--networks`, `--rlimits`, `--effective_capabilities` /
`--bounding_capabilities`, `--kill_after`, `--checkpoint`, `--principal` /
`--secret`, `--content_type` (`application/json` or `application/x-protobuf`),
and `--tty`. Run `medios-execute --help` for the full list.

Built at `src/CMakeLists.txt` (`add_executable(medios-execute cli/execute.cpp)`).
