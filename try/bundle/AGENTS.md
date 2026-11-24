# Medios research preview

You are setting up a one-machine Medios preview for the user. Medios is a
resource manager derived from Apache Mesos: a controller offers a node's
resources to frameworks, and a framework decides which tasks to launch on them.

Work only from the files in this directory. Each step below names the skill
file that tells you how to do it; read that file when you reach the step, not
before.

## Rules

- User space only. Never run `sudo`, never change system settings, never
  install packages system-wide.
- Everything lives in this directory. State, logs, the Medios install, uv and
  Python go under `state/`. Removing the preview is `bin/medios-try down`
  followed by deleting this directory.
- Every `bin/medios-try` command prints one JSON object and exits non-zero on
  failure. Read the JSON instead of guessing from logs.
- When a step fails, read `skills/troubleshoot/SKILL.md` before trying a fix of
  your own. If no entry matches, stop and offer to help the user file an issue
  (`skills/file-issue/SKILL.md`).

## Steps

1. Install Medios and start a controller and node:
   `skills/setup/SKILL.md`.
2. Write and run your own framework that computes three Euler sums and checks
   them: `skills/first-framework/SKILL.md`.
3. Report to the user what started, what ran and what was verified, with the
   numbers from step 2.
4. Ask the user **"What do you want to run next?"** and show the menu in
   `skills/workloads/SKILL.md`. The user can also describe a workload of their
   own.
5. When the user is done, run `bin/medios-try down`.

If you can start subagents, you can hand step 1 or step 2 to one; give it this
file and the skill file for that step.
