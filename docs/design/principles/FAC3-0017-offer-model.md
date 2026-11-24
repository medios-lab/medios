```
CATALOG   FAC3-0017
TITLE     What Mesos Actually Does (Offer Loop)
EDITION   001
STATUS    Draft
```

## Mesos Core Loop

1. Nodes report available resources to the controller
2. Controller runs allocator (DRF) to decide which framework gets offers
3. Controller sends offers to framework (resources available NOW)
4. Framework accepts/declines offers
5. If accepted: node runs executor with task
6. When task finishes: resources return to pool
7. Repeat

## Scale Clarity

"1000 jobs" = 1000 tasks queued in framework memory
Running concurrently = limited by actual cluster resources

Example (2 nodes, 4 CPUs each):
- Framework has 1000 jobs queued
- Mesos offers: Node1 {cpu:4, mem:8192}, Node2 {cpu:4, mem:8192}  
- Framework launches 8 jobs (using all CPUs)
- 992 jobs wait in framework queue
- As tasks finish, resources free up, new offers arrive
- Framework launches next batch

## What Mesos Doesn't Do
- Queue jobs (framework's problem)
- Retry failed tasks (framework's problem)  
- Know about job dependencies (framework's problem)
- Manage job state (framework's problem)

## Resource Truth (your H100 cluster)
- 10 nodes × 1 H100 each = max 10 GPU jobs running
- Framework can queue 10,000 GPU jobs
- Mesos just offers "node3 has gpu:1 available"
- Framework decides which queued job to run

## Tier-2 Scheduler Responsibility
- Maintain job queue
- Decide job → offer matching
- Handle task failures (retry or not)
- Track job history/state

## Mesos Limits (Real)
- Nodes: ~thousands per cluster
- Concurrent tasks: ~50K with tuned controller
- Offers/sec: ~hundreds
- Framework state: framework's RAM problem
