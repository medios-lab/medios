---
title: Medios documentation style guide
---

# Medios documentation style guide

Medios documentation tells a reader how to run work on a medios cluster and what
medios does with it. The reader writes a framework, submits tasks, or operates a
controller and nodes; they know distributed systems and they are reading because
they want one specific answer. A page gives that answer, in the reader's words,
with a command they can run.

Every page under `docs/` is one of four kinds. Pick the kind from the question
the reader arrives with, follow its template, and write the example first: the
example is the part the reader uses.

## Concept: what is this and how does it behave

A concept page defines one thing and describes how it behaves. It carries no
procedure.

Template:

1. Title: the thing's name.
2. One sentence that defines it, then two or three on how it behaves.
3. One section per property, each heading stating the property.
4. One small example that makes a property concrete.
5. `## What's next`, linking to the how-to and reference pages for the thing.

Example:

````markdown
# Offers

An offer is the controller's statement that a set of resources on one node is
available to one framework right now. The controller sends offers to a
subscribed framework on its event stream, and the framework replies by
accepting an offer with tasks or declining it. Resources in an offer belong to
that framework until it replies or the offer is rescinded.

## An offer names one node and one role

Each offer carries a `node_id`, the `framework_id` it was sent to, one role,
and a list of `Resource` entries. Resources from two nodes never arrive in one
offer, so a task that needs more than one node needs more than one offer.

## An offer is held until the framework replies

The controller rescinds an offer when the node is lost, when the framework
unsubscribes, or when `--offer_timeout` is set and that duration passes. With
no `--offer_timeout`, an offer is held until the framework replies.

```json
{"type": "OFFERS", "offers": {"offers": [{
  "id": {"value": "<offer-id>"},
  "node_id": {"value": "<node-id>"},
  "resources": [{"name": "cpus", "type": "SCALAR", "scalar": {"value": 2}}]}]}}
```

## What's next

- [Accept an offer and launch a task](accept-an-offer.md)
- [Scheduler HTTP API](scheduler-http-api.md)
````

## How-to: how do I do X

A how-to page gets one task done for a reader who already knows what they want.

Template:

1. Title: `<Verb> <object>`, naming the task exactly.
2. One sentence on what the reader ends up with.
3. `## Before you begin`, a list of conditions, each one checkable by a command.
4. Numbered steps, one action each, with the command and the output it prints.
5. `## Clean up`, the commands that undo the task.
6. `## What's next`.

Example:

````markdown
# Watch a task's loss for NaN

You end with a task whose progress message reports the first NaN in its output,
within a second of the task printing it.

## Before you begin

- A controller you can reach: `curl -s http://<controller>:5050/state`
  returns JSON.
- `medios-execute` from the release's `bin/` is on your PATH.

## 1. Add the observer to the task

An observer is a command the executor runs beside the task in its sandbox. This
one reads the task's `stdout` file and prints one line when it matches.

```json
{"name": "loss-watch", "task_id": {"value": "loss-watch-1"},
 "resources": [{"name": "cpus", "type": "SCALAR", "scalar": {"value": 0.2}},
               {"name": "mem", "type": "SCALAR", "scalar": {"value": 64}}],
 "command": {"shell": true, "value": "python3 -u train.py"},
 "observers": [{"name": "nan", "command": {"shell": true,
   "value": "tail -F stdout | grep -m1 -i nan | sed 's/^/ISSUE /'"}}]}
```

## 2. Submit the task

```sh
medios-execute --controller=<controller>:5050 --task=file:///tmp/loss-watch.json
```

## 3. Read the progress message

```sh
curl -s http://<controller>:5050/tasks
```

The task's newest status carries the observer's line, prefixed with its name.
For a task that printed `step 4120 loss nan`:

```text
"progress": {"message": "nan: ISSUE step 4120 loss nan", "sequence": 41}
```

## Clean up

The task ends when `train.py` does, and the executor ends the observer with it.
Remove the task file:

```sh
rm /tmp/loss-watch.json
```

## What's next

- [Task observers](task-observers.md)
````

## Reference: what are the exact values

A reference page lists every value of one kind, in a fixed order, with no
instruction and no argument. It opens with a sentence saying what the entries
have in common, then gives one entry per value.

Example:

````markdown
# Task observer fields

Fields of `TaskObserver`, which appears on `TaskInfo.observers`. The controller
validates every entry at submit.

`name` (string, required)
: One to thirty-two characters of `a-z`, `0-9` and `-`, unique among the
  observers on the task. It prefixes every progress message the observer
  produces. A name outside that set makes the controller refuse the task.

`command` (`CommandInfo`, required)
: The command the executor runs beside the task, in the task's sandbox, with
  the executor's environment. Shell form is accepted.
````

## Tutorial: walk me through this from nothing

A tutorial takes one reader through one scenario, start to finish, in order,
with every command and its output. It offers no alternatives and lists no
options; it links to the reference for those.

Template:

1. Title: `<Verb> your first <thing>`.
2. What the reader will have at the end, and how long it takes.
3. `## What you need`, two or three items.
4. Numbered steps in one unbroken sequence, each printing something visible.
5. `## What you built`, two sentences naming what each piece did.
6. `## What's next`.

Example:

````markdown
# Run your first observed task

In about five minutes you submit a task, watch an observer report on it, and
read the result off the controller.

## What you need

- A controller at `<controller>:5050` and one node registered with it.
- `medios-execute` from the release's `bin/`.

## 1. Write the task

The task prints a progress line and sleeps. The observer prints two lines, six
seconds apart.

```json
{"name": "first-observed", "task_id": {"value": "first-observed-1"},
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

## 3. Read the statuses

```sh
curl -s http://<controller>:5050/tasks
```

```text
TASK_RUNNING   obs: ISSUE late look
TASK_FINISHED  obs: ISSUE late look
```

## What you built

The task reported its own progress, and the observer reported a judgment about
it on the same record. The observer's standard error is in the sandbox at
`observers/obs.stderr`.

## What's next

- [Task observers](task-observers.md)
````

## Reference entry shapes

Each shape below is one filled-in entry. Copy the shape and keep its order.

A field:

```text
`observers` (repeated `TaskObserver`, optional)
: Commands the executor runs beside the task for as long as it runs. At most
  eight per task. With the field absent the task runs with no observers.
```

A flag:

```text
`--offer_timeout=<duration>` (controller, duration, no default)
: How long the controller holds an offer before rescinding it, for example
  `30secs`. With the flag unset, offers are held until the framework replies.
  The controller reads it at startup.
```

An endpoint:

```text
### POST /api/v1/scheduler

Accepts one scheduler `Call`. A `SUBSCRIBE` call returns `200 OK` with a
RecordIO event stream and a `Medios-Stream-Id` header; every later call carries
that header on a separate connection and returns `202 Accepted` once the
controller has taken the call for processing. `400 Bad Request` means the call
failed validation; `403 Forbidden` means the principal may not make it.

Request:

{"type": "ACCEPT", "framework_id": {"value": "<framework-id>"},
 "accept": {"offer_ids": [{"value": "<offer-id>"}], "operations": []}}

Response: 202 Accepted, empty body.
```

A status reason:

```text
`REASON_TASK_INVALID`
: Set by the controller on `TASK_ERROR` when a submitted task fails validation,
  with the failing check in `message`. The task never reached a node, so the
  framework resubmits a corrected task rather than retrying this one.
```

State the unit in the name or the type, every time: `--offer_timeout` takes a
duration, `mem` is in MiB.

## Voice and wording

Write to the reader as "you", in the present tense, in the active voice: "you
post the request to `/api/v1/scheduler`", "the controller sends an offer". Put
one idea in a sentence and keep sentences short. Use the plain word: use, start,
read, send.

Headings are sentence case and state what the section says. "The controller
rescinds an offer it has already sent" is a heading.

Name a number with its unit, and name every host, path, identifier and
credential with a placeholder in angle brackets: `<controller>`, `<node-id>`,
`<framework-id>`, `<credential-file>`. Keep a placeholder's spelling the same
across a page.

Use `controller` for the cluster coordinator and `node` for the machine-side
daemon. `agent` is an AI or agentic concept. Write API objects in their protobuf
spelling, in backticks: `TaskInfo`, `FrameworkID`, `Resource`. Backticks also
carry commands, flags, paths, field names and status values.

Examples run as written: complete fields, no ellipsis, under about twenty lines,
one idea each, command in one block and its output in the next, with the output
block marked `text`. Write the command without a shell prompt.

The banned-word list at
`~/.claude/skills/anti-ai-slop-writing/references/banned-words.md` applies to
every page. Check a page against it before you open the pull request.

## What a page covers

A page describes medios as it is today, and states behavior as plain fact: "the
controller rejects a task with an unknown role."

A page names medios surfaces: the controller, nodes, frameworks, offers, tasks,
executors, status reasons and the HTTP APIs. Links go to other pages under
`docs/` by relative path. When a page cannot stand without the SDK, it gives a
bare link to the SDK's documentation; the SDK's own docs describe the SDK.

Other material has its own home. Release notes carry what changed and when.
Decision records carry why a choice was made. Another product's documentation
describes how that product uses medios. Deployment state, hostnames and
credentials live with each deployment's operators, and a page uses placeholders in
their place.

## Before you open the pull request

Answer yes to each of these for every page you changed.

- Is the page one kind, following that kind's template?
- Does the title say what the reader gets?
- Does every example run as written and show its output?
- Is every number given with its unit, and every host, path and credential a
  placeholder in angle brackets?
- Is it second person, present tense, active voice?
- Does each heading state what its section says?
- Does every reference entry carry its type, whether it is required, its
  default, and what happens when it is absent?
- Do `controller` and `node` name the daemons, and `agent` only an agentic
  concept?
- Do the relative links under `docs/` resolve?
- Is the page clean against the banned-word list?

## Sources

- The four kinds, and the question each answers: https://diataxis.fr,
  https://diataxis.fr/tutorials/, https://diataxis.fr/how-to-guides/,
  https://diataxis.fr/reference/
- Page templates, `Before you start`, step-then-output, and the reference
  overview sentence: https://github.com/thegooddocsproject/templates
- Second person, present tense, active voice, sentence-case headings,
  placeholder and code formatting: https://developers.google.com/style,
  https://developers.google.com/style/code-samples
- Short sentences, plain words, front-loaded headings:
  https://learn.microsoft.com/en-us/style-guide/top-10-tips-style-voice
- Examples in every page, consistency, and docs stored beside the code:
  https://www.writethedocs.org/guide/writing/docs-principles/
- Field entry shape (name, type, required, default, behavior when absent) with
  one full request and response beside it:
  https://docs.stripe.com/api/charges/object
- Concept-then-procedure page shape and placeholder values in commands:
  https://tailscale.com/kb/1019/subnets
