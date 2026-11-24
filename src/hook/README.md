# src/hook

The hook subsystem: operator-supplied extension points that let a loadable
module observe and rewrite a few pieces of task/node state as they pass through
the controller and node.

## What ships

The interface (`include/medios/hook.hpp`), the loader/dispatcher
(`manager.{hpp,cpp}`), and the call sites in the controller and node. **No hook
modules ship with medios.** Until an operator loads one, every hook point is a
no-op — `HookManager::hooksAvailable()` is `false` and the daemon skips the
dispatch entirely.

## How a hook is loaded

Two flags, on either `medios-controller` or `medios-node`:

- `--modules` registers shared-library modules and the names they expose (the
  standard module-manager mechanism).
- `--hooks=<name>[,<name>...]` selects which of those modules — by name — to
  activate as hooks.

At startup `HookManager::initialize(flags.hooks)` (controller `main.cpp`, node
`main.cpp`) instantiates each named module via the `ModuleManager` and stores it.
Unknown name or duplicate → startup fails with a specific error.

## The ten hook points

Controller:

| Hook | Called when | Effect |
|------|-------------|--------|
| `controllerLaunchTaskLabelDecorator` | launching a task | overwrites the task's `Labels` |
| `controllerLaunchTaskResourceDecorator` | launching a task | overwrites the task's `Resources` (e.g. supply a default for a resource a framework doesn't set yet) |
| `controllerNodeLostHook` | a node is declared lost, after frameworks are notified | side effect only |

Node:

| Hook | Called when | Effect |
|------|-------------|--------|
| `nodeResourcesDecorator` | node init, before advertising | overwrites advertised `Resources` |
| `nodeAttributesDecorator` | node init, before advertising | overwrites advertised `Attributes` |
| `nodeRunTaskLabelDecorator` | a run-task request arrives | overwrites the task's `Labels` |
| `nodeExecutorEnvironmentDecorator` | launching an executor | overwrites the executor command `Environment` |
| `nodePostFetchHook` | after container image provisioning | side effect in the sandbox before the command starts |
| `nodeTaskStatusDecorator` | a status update arrives from the executor | overwrites `labels` and `container_status` on the `TaskStatus` |
| `nodeRemoveExecutorHook` | an executor is being removed | side effect (cleanup) |

## Dispatch contract

These rules hold for every hook point and matter when writing or reasoning about
a hook module:

- **All loaded hooks run, in `--hooks` order.** `availableHooks` is a
  `LinkedHashMap`, so insertion order is preserved.
- **Decorators chain.** Each hook is handed the result of the previous one, on a
  single mutable copy. The last hook to return a value for a given field wins for
  that field; earlier hooks' changes to other fields survive.
- **`None()` means "no change."** A hook that returns `None` leaves the value as
  it stands.
- **Errors are non-fatal.** A hook returning an error is logged at `WARNING` and
  skipped; the chain continues. A hook never fails a task launch or a node
  startup.
- **Decoration is overwrite, not merge.** A returning hook replaces the whole
  field (labels, resources, environment, ...). A well-behaved module reads the
  incoming value and extends it rather than discarding it.

## Example: stamp an environment variable on every executor

Say every executor on a node should run with `CLUSTER=prod` in its environment —
a cluster-wide default no framework should have to set. That is one hook point
(`nodeExecutorEnvironmentDecorator`) and a few lines.

### 1. Implement the hook

```cpp
// example_hook.cpp
#include <medios/hook.hpp>
#include <medios/medios.hpp>
#include <medios/module.hpp>
#include <medios/module/hook.hpp>

using namespace medios;

class ExampleHook : public Hook
{
public:
  Result<Environment> nodeExecutorEnvironmentDecorator(
      const ExecutorInfo& executorInfo) override
  {
    // Start from the executor's existing environment and extend it, rather
    // than replacing it — the decorator overwrites the whole field, so a hook
    // that ignored the incoming value would erase everything already set.
    Environment environment = executorInfo.command().environment();

    Environment::Variable* variable = environment.add_variables();
    variable->set_name("CLUSTER");
    variable->set_value("prod");

    return environment;
  }
};


static Hook* createHook(const Parameters&)
{
  return new ExampleHook();
}


// The module name ('com_example_ExampleHook') is what --hooks references.
medios::modules::Module<Hook> com_example_ExampleHook(
    MEDIOS_MODULE_API_VERSION,
    MEDIOS_VERSION,
    "Example Author",
    "author@example.com",
    "Stamps CLUSTER=prod onto every executor.",
    nullptr,          // no compatibility check
    createHook);
```

### 2. Build it as a shared library

Compile against the medios headers into a `.so` (a `libexamplehook.so`). The
build flags match how the in-tree test modules are built; see
`tests/CMakeLists.txt` (`_TEST_MODULES`) for a working target.

### 3. Point `--modules` at it

```json
{
  "libraries": [
    {
      "file": "/opt/medios/libexamplehook.so",
      "modules": [ { "name": "com_example_ExampleHook" } ]
    }
  ]
}
```

### 4. Load it on the node

```
medios-node \
  ... \
  --modules=file:///opt/medios/modules.json \
  --hooks=com_example_ExampleHook
```

Now every executor the node launches gets `CLUSTER=prod`. The same shape applies
to the other nine points: derive from `Hook`, override the one method you care
about, register the module, load it with `--modules` + `--hooks`. Put a hook on
the controller (label/resource decorators, node-lost) by passing the same flags
to `medios-controller`.

## Why it exists here

Hooks are the seam for cluster-local policy that doesn't belong in a framework
(which is per-application) or in the platform core (which stays minimal): inject
a standing label on every task, default a resource the cluster accounts for,
stamp executor environment, run sandbox setup after provisioning. The interface
and dispatch are built and invoked; the modules are operator-supplied.
