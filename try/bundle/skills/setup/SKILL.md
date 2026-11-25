---
name: medios-setup
description: Install the pinned Medios release in user space, start a controller and one node on 127.0.0.1, and confirm the node registered. Use for step 1 of the Medios preview.
---

# Set up the preview

Run each command from the bundle directory and read its JSON before the next.

1. `bin/medios-try install`

   Downloads the Medios release for this platform from the URL in
   `release.json`, checks its sha256 against the pinned value and unpacks it
   under `state/medios/`. The first run also downloads uv and a Python 3.12
   into `state/`; on the machine this bundle was tested on, the whole step
   took 44 s.

   Success: `"ok": true` and a `controller_version` line.

2. `bin/medios-try up`

   Starts `medios-controller` and `medios-node` bound to 127.0.0.1, on the
   first free ports from 5050 upward, and waits up to 60 s for the node to
   register. The node offers what it detects on this machine. To offer less,
   pass `--resources 'cpus:2;mem:1024;disk:1024'` (memory and disk in MB).

   Success: `"ok": true` with a `node_id`, `node_resources` and
   `controller_url`. The same values are saved in `state/run.json`, which your
   framework reads in the next step.

3. `bin/medios-try status`

   Success: `"ok": true`, both processes alive, one node with
   `"active": true`.

`GET <controller_url>/state` returns the full cluster state as JSON.

## When it fails

Every failure JSON names a `step` and an `error`, and `up` failures include the
last 30 lines of the failing process's log. Look the error up in
`skills/troubleshoot/SKILL.md`. Logs are in `state/logs/controller.log` and
`state/logs/node.log`.
