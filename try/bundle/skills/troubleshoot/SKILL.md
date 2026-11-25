---
name: medios-troubleshoot
description: Known Medios preview failures with their cause and fix, keyed by the error text. Use whenever a bin/medios-try command or the first framework fails.
---

# Troubleshooting

Find the row whose symptom matches the `error` field or the traceback, apply
the fix, and rerun the step that failed. If nothing matches, don't improvise a
fix to Medios itself: run `bin/medios-try diagnose` and follow
`skills/file-issue/SKILL.md`.

The Seen column says whether the failure has been reproduced in testing (yes)
or follows from the code path without having been reproduced (no).

| Symptom | Cause | Fix | Seen |
| --- | --- | --- | --- |
| `ValidationError` ... `principal` Field required | The `TaskInfo` has no `principal` | Add `principal="operator:preview"` (see `skills/first-framework/sdk-reference.md`) | yes |
| `TEARDOWN got HTTP 400: Framework cannot be found` | `teardown()` ran after leaving the `subscribe()` loop, which had already closed the stream | Call `client.teardown()` inside the loop, then `break` | yes |
| A task ends `TASK_ERROR` right after launch | Two tasks with the same task id, often from accepting a second offer for work already launched | Launch each task id once; decline offers once nothing is pending | yes |
| `"step": "platform"`, unsupported platform | The preview ships for macOS on Apple silicon and Linux on x86_64 or arm64 only | Tell the user; there is no build for this machine | no |
| `"step": "uv-download"` or `"step": "download"` | No network, a proxy, or GitHub unreachable | Check `curl -I https://github.com`; rerun once the network works | no |
| `"step": "verify"` or `"uv-verify"`, sha256 does not match | Corrupt or intercepted download | Rerun the step once. A second mismatch is a security problem: stop and file an issue | no |
| `"step": "install"`, Medios is not installed | `up` ran before `install` | Run `bin/medios-try install` | no |
| `"step": "up"`, `controller exited` or `node exited` | The process failed at start; the reason is in `log_tail`. `up` has already stopped the other process | Look for a port or permission error in `log_tail`. Otherwise run `diagnose` and file an issue | yes |
| `"step": "up"`, node did not register within 60s | The node started but never reached the controller; `up` has already stopped both processes | Rerun `bin/medios-try up` once; if it repeats, run `diagnose` and file an issue | no |
| `ModuleNotFoundError: No module named 'medios_sdk'` | The framework ran with plain `python` | Run it with `bin/medios-python euler.py` | no |
| A task ends `TASK_FAILED` | The task command failed | Read the task's `stderr` the same way as `stdout` (file name `"stderr"`) | no |
| The framework waits with no OFFERS event | The node's free resources are smaller than what the framework asks for, or another framework holds them | `bin/medios-try status`; compare `resources` with your tasks' cpus and mem | no |
