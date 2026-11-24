```
CATALOG   FAC3-0013
TITLE     Pure Mesos: Correcting the Drift
EDITION   001
STATUS    Draft
```

## 1. Resources are Resources

CPU, mem, disk, GPU - all just resources in offers. No special cases.
Mesos doesn't know what a GPU is; it's just another scalar with a name.
Attributes (if any) are opaque strings the framework interprets.

## 2. Framework = Scheduler Only

Framework receives offers, makes decisions, sends LaunchTasks.
Checkpointing is the task's problem (write to disk before SIGTERM).
Executor runs what framework asked; framework doesn't touch execution.

## 3. Preemption = Offer Mechanics

Offers expire naturally (timeout) or by revocation (RESCIND).
Framework handles TASK_LOST/TASK_KILLED and decides: retry or not.
Platform provides kill; framework decides what to do about it.

## 4. Fairness = Not Mesos' Business

DRF exists as ONE allocator implementation.
Frameworks can ignore DRF completely; just take offers greedily.
Research belongs in custom allocators or framework-level decisions.

## 5. Telemetry = Not Mesos' Business

Mesos emits offer/launch/update events. That's it.
Frameworks log what they want.
Observability is external (Prometheus scraping endpoints, etc).

## Implications for Medios

### Checkpoint strategy:
- Framework launches with: "python train.py --checkpoint-dir=/tmp/ckpt"
- Task handles SIGTERM by writing checkpoint and exiting
- Framework sees TASK_KILLED, relaunches with same checkpoint-dir
- Zero custom executor code

### GPU scheduling:
- Node advertises: resources { name: "gpu" type: SCALAR value: 2.0 }
- Framework: if (offer.gpu >= job.gpu_need) { launch }
- That's it. No MIG awareness in Mesos.

### Fairness:
- Ship a greedy scheduler first (take any fitting offer)
- Research fairness in framework if you care
- Don't pollute Mesos core

### Hello World (corrected):
```python
# 50 lines, not 150
def resourcesMatch(offer, job):
    return offer['cpu'] >= job.cpu and offer['mem'] >= job.mem

def on_offers(offers):
    for offer in offers:
        if pending_jobs and resourcesMatch(offer, pending_jobs[0]):
            launch(offer, pending_jobs.pop(0))
        else:
            decline(offer)
```

This is the purity. Marathon/Aurora were the drift.