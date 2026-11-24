# FAC3-0028 — The Operator Declares Hardware; Medios Checks and Enforces, It Does Not Discover or Decide

**Status**: Accepted (operator, 2026-09-16; merged in #466). DR-034 (#467) and DR-035 (#470) apply it.
**Owner framing (2026-09-16)**: Mesos tried to be both operator and allocator. Choosing how many GPUs each of a thousand nodes should advertise was overhead a person could not carry, so the platform grew heuristics to do it. Today the operator of a node is an agent. It can run nvidia-smi, read the topology, and reason about the hardware more deeply than any rule medios could encode. Medios's rule is to do as little as is necessary, and the GPU code is the clearest case of where it did too much.

---

## §1. Statement

What a node offers is a declaration by its operator. Medios does not discover hardware to decide what to offer, and it does not encode judgments about what a device is, how it may be divided, or who may be offered it.

Medios does two things with a declaration. At start it checks the declaration against the host, and refuses to start when the host cannot honour it. At run time it enforces the declaration: it accounts for the declared units, grants them to containers, and denies what was not granted.

The general form: a declaration of X onto a host that cannot honour X does not start. Medios never adjusts a declaration to fit the host, because an adjusted declaration is a fallback, and a fallback turns a loud misconfiguration into a quiet wrong allocation.

## §2. What this removes from medios

- Probing the driver at node start to compute a resource count. The count is declared.
- Choosing which physical device a task receives by any policy beyond the ledger's own accounting of declared units.
- Knowing what kind of device a resource name means. Whole GPU, unified-memory GPU and MIG slice are declarations of a name, a count and an access mode; medios enforces the ledger it was given.
- Deciding which frameworks may see a resource. A framework that does not ask for a resource does not receive it. Gating offers by framework capability is a cluster policy the operator turns on, not a default medios applies.
- Deciding whether a unit is divisible. Integer-only GPU counts were a judgment about hardware that a MIG slice or a shared device already contradicts.

## §3. What this keeps in medios

- The check at start. Where medios can cheaply verify a declaration against the host, it does, and it names the failure: the declared device that does not exist, the declared count the host cannot back, the declared path that is not there. This is the only place medios reasons about hardware, and it reasons only to refuse.
- Enforcement. The device-declaration seam on the containerizer, the cgroup device controller, the driver-volume injection and the unit ledger are how a declaration becomes a container's reality. They act on the declaration and on nothing else.
- Discovery as a checking tool, not an authority. The module may still ask the driver what exists, in order to map a declared unit to a device path and to confirm the declaration. Its answer never becomes the offer.

## §4. Why the operator, and why now

The operator can look. The judgment medios used to encode, "this host has N GPUs and each is one indivisible unit", is one an agent operating the node makes better and keeps current as hardware changes. Encoded in medios, the same judgment goes stale the day a GB10 shares its memory with the CPU or a card is split seven ways, and the code that held it becomes the thing every new device fights.

This is the same boundary FAC3-0018 draws between platform and framework, applied on the node side. FAC3-0018 keeps scheduling policy out of medios because the framework knows the jobs. This principle keeps hardware policy out of medios because the operator knows the host.

## §5. How it applies to milestone 3 (#450)

- The node's GPU resources come from `--resources`, declared by the operator. Core's startup probe of the driver is deleted.
- The GPU module checks each declared unit against discovery at start and refuses to start the node on a mismatch, naming the unit.
- The three flavours are declared resource names with a count and an access mode; the unit ledger enforces them. The unified-memory budget is the operator's statement about the host, recorded in the declaration.
- The integer-count rule is deleted. The framework capability gate becomes a controller policy flag, off by default.
- Acceptance: a grep of `src/` for GPU-specific code outside `src/modules/gpu` returns nothing, and a declaration the host cannot honour fails the node's start with the unit named.

## §6. Relation to earlier principles and records

- FAC3-0013 §1 ("Mesos doesn't know what a GPU is; it's just another scalar with a name") is the origin of this principle. This record adds who decides the scalar's value and what medios does with it.
- FAC3-0012's open "Critical Decision" (orthodox scalar / pragmatic attributes / heretical first-class topology) is resolved by this record as the orthodox stance with the operator supplying the judgment. It should be marked resolved.
- FAC3-0001 §4 lists the resource ontology as cpu, mem, disk, net. Under this principle the ontology is whatever the operator declares; the list is an example, not a bound.
- FAC3-0011's "no topology/placement guidance for accelerators" was read as a gap to fill with recipes. Under this principle the recipes belong to the operating agent and to frameworks, not to medios.
- FAC3-0026 (offered, accepted, consumed; not actuals) is the same stance on the scheduler side: the books are the declaration, not a measurement.
- DR-027 (#116) kept the in-tree NVIDIA isolator. Milestone 3 retires it; the module replaces it, and this principle is why the replacement does less.
