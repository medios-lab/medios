# Contributing to Medios

A pull request to Medios states the behavior it changes as Gherkin scenarios,
and review starts from those scenarios. A reviewer reads them to learn what the
change is meant to do, then checks the code and the tests against them. People
and coding agents contribute under the same rules, and an agent may write the
scenarios for its own change.

[docs/DEVELOPMENT.md](docs/DEVELOPMENT.md) covers the build and the test suite.

## Scenarios state what a change does

A pull request that changes what an outside observer can see adds or edits
scenarios under `bdd/features/`, in the same pull request as the code. An
outside observer is a framework, an operator, or a task: anything that meets
Medios through its APIs, flags, logs, files and the tasks it runs.

A scenario is written in that observer's terms. It names controllers, nodes,
frameworks, offers, tasks, flags and HTTP responses, and it leaves out function
names, classes and other internals, so that a reader who has never opened the
source can say whether the behavior is the one they want.

Cover the path that works and the ways it fails: a refusal, a timeout, a
restart, a lost connection. When timing is part of the behavior, give the
bound. Keep the scenarios few enough to read in a minute or two; a one-line
fix usually needs one.

## The scenario tree is the scope of Medios

`bdd/features/` holds one directory per area of behavior, and together the
feature files describe everything Medios promises. A pull request that adds a
scenario to an existing feature refines a promise Medios already makes. A pull
request that adds a feature file makes a new one, so it links an issue in which
a maintainer agreed to the feature before the code was written.

## Each scenario names the test that proves it

A scenario is prose for a reviewer, and its proof is a test. Each scenario
carries a `@test:` tag with the name of that test as `ctest` reports it; the
test is ordinary C++ under `tests/` at whatever level fits the change, and no
tool runs the Gherkin itself.

```gherkin
Feature: A controller restart leaves running tasks alone

  @test:ControllerRestartTest.TaskSurvivesControllerRestart
  Scenario: The controller restarts while a task is running
    Given a controller, one node, and a framework with a task in TASK_RUNNING
    When the controller process is stopped and started again
    Then the node reregisters with the new controller
    And the framework is registered a second time
    And the controller's /state endpoint reports the task as TASK_RUNNING
```

One scenario can name several tests, with one tag for each. A scenario with no
test that proves it is a request for a behavior, and belongs in an issue.

## What the pull request description carries

1. The behavioral claim, in a sentence or two, with a link to the issue.
2. The scenarios added, and separately the scenarios changed or removed.
3. The evidence: the commit that was built, the host it was built on, and the
   test counts. For a bug fix, also state that the named test fails on the
   base commit and passes on the head commit, and how it failed.
4. What wasn't tested.

A pull request that changes nothing an outside observer can see, such as a
refactor, a documentation change or a build change, carries no scenarios and
says why in one sentence.

## What review checks

In this order:

1. The code changes no observable behavior that the scenarios leave out.
   Behavior beyond the scenarios comes out of the pull request and goes into
   one of its own; adding a scenario for it afterwards is the exception, and
   the description says why.
2. Every scenario is implemented: its named test exists, ran, and exercises
   what the scenario describes.
3. Each scenario states behavior an outside observer can see and leaves the
   code out.
4. An existing scenario that was weakened or removed is called out in the
   description, with the reason.
5. A pull request with no scenarios changes nothing an outside observer can
   see.

A pull request that edits a test together with the code that test covers says
so in the description.

## Other rules

- Raise a change to resource ownership, scheduling boundaries or a public
  header under `include/mesos/` in an issue before writing it.
- Add no runtime dependency without a maintainer's agreement.
- Commit titles take the form `[scope] Short imperative`, for example
  `[node] Read cpu.max from the root cgroup`.
- Report a security problem as [SECURITY.md](SECURITY.md) describes.
