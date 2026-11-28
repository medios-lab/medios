# Behavior scenarios

`bdd/features/` describes what Medios does, as Gherkin scenarios written from
the point of view of a framework, an operator or a task. Each directory under
it is one area of behavior, named for what Medios promises there, and holds
one `.feature` file per feature, for example:

```
bdd/features/
  controller-restart/
    running-tasks.feature
  node-resources/
    container-limits.feature
```

Every scenario carries one or more `@test:` tags, each naming a test as `ctest`
reports it. That test is the proof of the scenario; no tool runs the `.feature`
files themselves.

```gherkin
@test:ControllerRestartTest.TaskSurvivesControllerRestart
Scenario: The controller restarts while a task is running
```

To see the proof of a scenario, run its test:

```
ctest --test-dir build -R ControllerRestartTest.TaskSurvivesControllerRestart --output-on-failure
```

[CONTRIBUTING.md](../CONTRIBUTING.md) says when a pull request adds or edits a
scenario and what a reviewer checks.
