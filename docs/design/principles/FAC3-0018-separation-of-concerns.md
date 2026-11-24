```
CATALOG   FAC3-0018
TITLE     What Mesos Is and Isn't
EDITION   001
STATUS    Draft
```

## Mesos Concerns (Only)
- Resource accounting (what's free/used per node)
- Offer generation (who gets offered what)
- Task launching (tell executor to run)
- Task lifecycle (RUNNING/FINISHED/FAILED/KILLED)
- Resource reclamation (task done → resources free)

## Framework Concerns (Everything Else)
- Queue management (memory/DB/Redis/whatever)
- Scheduling policy (FIFO/priority/deadline/random)
- Failure handling (retry/abandon/checkpoint)
- State management (stateless/stateful/external)
- Job semantics (what a "job" even means)
- User interface (CLI/API/GUI/nothing)

## The Beauty

Mesos: "node7 has {cpu:16, mem:64000, gpu:1}"
Framework: "cool, I'll run job #451 there"
Mesos: "ok, running"
[later]
Mesos: "task done, node7 has {cpu:16, mem:64000, gpu:1} again"

Framework can be:
- 50 lines of Python with jobs=[]
- Distributed system with Cassandra backend
- Stateless lambda reading from Kafka
- Excel spreadsheet webhook

Mesos doesn't care.

## Anti-Patterns (Marathon/Aurora mistakes)
- Putting job queues in Mesos
- Making Mesos understand job dependencies
- Adding application semantics to offers
- Forcing frameworks into a mold

## Medios Purity

Ship 3 example frameworks:
1. pure.py: 50 lines, queue in memory
2. persistent.py: 150 lines, queue in SQLite
3. distributed.py: 200 lines, queue in Redis

All three are valid. User picks based on their needs.