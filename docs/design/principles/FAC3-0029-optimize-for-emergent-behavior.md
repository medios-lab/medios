```
CATALOG   FAC3-0029
TITLE     Optimize for emergent behavior — a primitive earns its place by what nobody designed on it
EDITION   001
STATUS    Proposed
```

## Decision

Medios is designed for what it does not know about. A primitive is added as the smallest thing that unblocks a shown need, it carries exactly one invariant that only medios can enforce, and it stays too small to encode any one use. Composition lives in the task's own surfaces (its sandbox, its files, its standard output, its environment, its process lifetime) and in the frameworks above medios, never in a field on the wire.

## Motivation

The observer is the worked example. The first draft was a `HealthAgent` with a model, a prompt, a cadence, a credential directory and an observation path: five fields, each a guess about one use, each bringing its own corner cases. The shipped primitive is a `TaskObserver` with a name and a command. Everything cut became something a task author composes in the sandbox instead. Within a day of the roll, one task ran with a poet asking a model for a haiku about its log, a critic reading the poet's file and grading it, a tripwire that printed once on the first NaN and exited, and a clock counting the poems, all on the task's record, with medios knowing nothing of poems, critics or models.

The same day produced the counterexample. An observer's line is visible for seconds on a task that prints its own progress often, and the first proposal was a retained per-observer field on the status, carried forward by the controller and mirrored by frameworks, so that a dashboard could show every observer's last line. It was a shape for an imagined use. A reporter observer that reads the others' files and prints the one line the record should carry does the same job as shell on the task, and the clock observer had already done it. The field was declined.

Mesos gave a task a sandbox and an executor and let nothing else into the room. Medios lets anything into the room and promises only that the room's invariants hold.

## The principle

Three rules, applied to every proposed addition to the wire or the executors:

1. **Medios holds the invariant and nothing else.** A primitive comes with one statement of what medios enforces about it, and that statement is the whole of what medios promises. For observers: an observer's output can never move the task's work value or its stall clock. Not what an observer is, not how often it runs, not what it reads.

2. **The sandbox is the composition surface.** A task already has files, standard streams, an environment and a process lifetime; a new primitive composes with a task, and with other instances of itself, through those and nothing new. If composing two of them needs a field, the primitive is wrong.

3. **A need is shown before it is shaped.** A framework must outgrow a pattern it can already express before medios grows a field for it, and what it then gets is the smallest primitive that unblocks it, not the feature it described. A dashboard wanting five lines side by side is a framework's need and a framework's field.

The test for a proposed field: if a task author could get the behavior with a command and a file, the field is a convenience, and conveniences live in frameworks. If they could not, because the behavior needs something only the executor or the controller can see or enforce, it is a primitive, and it arrives with its invariant.

## Application

- `TaskObserver {name, command}`: two fields, one invariant (no authority over value or clock), composition through `stdout`, the sandbox and the executor environment. The reporter, take-first and rotating patterns are task-level shell.
- `ProgressPolicy`: source, pattern, cadence, window and action are the executor's reading of the task's own output; medios does not interpret a reported value (FAC3-0026 and DR-038 territory), only records it.
- Declined under this principle: a retained per-observer field on `TaskStatus`; a per-kind field for an observer's model, prompt or budget; a credential field on the task.
- The question to ask of the next proposal: what did somebody build on it that we did not design? If the answer is nothing yet, it is not a primitive, it is a feature, and it waits.

## Related

FAC3-0022 (dependency parsimony: the same argument about carrying cost, applied to the wire instead of the build), FAC3-0026 (offered, accepted, consumed: medios records, it does not interpret), DR-041 (task observers).
