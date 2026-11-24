# Networking contract for frameworks

This page states what Medios provides when a framework runs a service whose
tasks listen on ports and connect to each other, such as a Ray cluster with
one head and many workers. Medios has no network isolator, no per-task IP
address and no service names. A task is reached at its node's address on a
port the framework reserved for it.

## Addressing

A task's address is the node's hostname from the offer, together with ports
the framework reserved from that offer's `ports` resource. This holds on both
containerizers.

Each node advertises a `ports` resource of type `RANGES`; the node's
`--resources` flag sets the range, and a node that does not declare one
advertises `31000-32000`. A framework reserves ports the way it reserves any
resource: it puts a sub-range of the offered `ports` into the task's
`resources`. The controller checks that the sub-range is inside the offer and
accounts it, so no other task on that node is given those ports until the
task ends. Medios does not tell the process which ports it got; the
framework passes them in, through the command line or environment
variables.

Every port a task listens on must come from its reservation. Medios does not
stop a process from binding an unreserved port, but a port it did not reserve
can be given to another task on the same node, and then one of the two
fails to bind.

## Port binding by containerizer

Under the `medios` containerizer a task runs in the node's network
namespace. A process that binds a reserved port is reachable at
`(node hostname, port)` with no translation.

Under `docker_shell` the container is on Docker's default bridge network, and
the executor publishes each reserved port to the same port number on the
host (`--publish p:p`). Inside the container the process must:

- bind the reserved port number on all interfaces (`0.0.0.0`), not on the
  container's bridge address alone and not on `127.0.0.1`;
- advertise the node's hostname, not its own bridge address, to anything that
  will connect back. The bridge address is not reachable from other nodes.

A host-network mode for `docker_shell` is not provided, and
`docker.network` is ignored ([containerizer-fields.md](containerizer-fields.md)).

## Stable addresses across task replacement

Medios has no name that follows a task when it is replaced on another node.
Keeping one is the framework's job. The framework knows where it launched
each task, because it chose the offer; when it replaces a task, it gives the
new address to whatever needs it. For a head-and-workers service that means
the framework passes the head's current `host:port` to each worker it
launches, and launches replacement workers with the new address after the
head moves.

## Two tasks of one framework on one node

Distinct `ports` reservations keep two tasks from colliding on ports. Other
shared state on the node depends on the containerizer:

- Under `medios`, a task that does not run its own image shares the node's
  filesystem outside its sandbox, so paths such as `/tmp` are common to every
  such task on the node. Point scratch and socket directories at the sandbox
  (`MEDIOS_SANDBOX`). Without the `namespaces/ipc` isolator, such a task also
  shares the node's `/dev/shm`.
- Under `docker_shell`, each task has its own container filesystem, IPC
  namespace and `/dev/shm`; only reserved ports reach the host.

## Events across a lost subscription

The controller keeps no queue of events for a disconnected scheduler. An
event produced while the subscription is down is lost, with one exception:
a task status update the scheduler has not acknowledged stays with the node,
which sends it again until it is acknowledged, starting after 10 seconds and
doubling up to 10 minutes. After resubscribing with its `framework_id`, a
scheduler learns the current state of its tasks by sending `RECONCILE`
([scheduler-http-api.md](scheduler-http-api.md)); offers it held before the
disconnect are withdrawn and new ones arrive.
