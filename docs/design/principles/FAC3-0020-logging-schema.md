```
CATALOG   FAC3-0020
TITLE     Minimal Logging Schema for Learning
EDITION   001
STATUS    Draft
```

## Decision Log (per scheduling decision)
```json
{
  "timestamp": 1234567890.123,
  "framework_id": "pure-001",
  "decision_id": "uuid-here",
  "offer": {
    "node_id": "node-7",
    "resources": {"cpu": 16.0, "mem": 64000, "gpu": 1.0},
    "attributes": {}
  },
  "action": "LAUNCH",
  "job": {
    "id": "job-451", 
    "cmd": "python train.py",
    "requested": {"cpu": 8.0, "mem": 32000, "gpu": 1.0},
    "queue_time_ms": 1523
  },
  "cluster_state": {
    "queue_depth": 47,
    "running_tasks": 12,
    "cluster_util": {"cpu": 0.72, "mem": 0.45, "gpu": 0.90}
  }
}
```

## Task Lifecycle Log
```json
{
  "timestamp": 1234567890.456,
  "task_id": "task-uuid",
  "job_id": "job-451",
  "event": "TASK_FINISHED",
  "runtime_ms": 34567,
  "exit_code": 0,
  "resources_used": {"cpu_seconds": 275.3, "mem_mb_seconds": 1100000, "gpu_seconds": 34.5}
}
```

## Cluster Snapshot (every N seconds)
```json
{
  "timestamp": 1234567900,
  "total_resources": {"cpu": 640, "mem": 2048000, "gpu": 10},
  "used_resources": {"cpu": 461, "mem": 921600, "gpu": 9},
  "frameworks": [
    {"id": "pure-001", "queued": 47, "running": 9}
  ],
  "offer_latency_p50_ms": 23,
  "offer_latency_p95_ms": 145
}
```

## What to Extract for Learning

**Features:**
- Queue depth at decision time
- Resource utilization at decision time  
- Job resource ratio (gpu/cpu, mem/cpu)
- Time of day/week patterns
- Job history (success rate by type)

**Outcomes:**
- Task completion time
- Task success/failure
- Resource efficiency (used/requested)
- Queue wait reduction
- Cluster utilization improvement

**Derived Metrics:**
- Makespan (time to clear queue)
- Fairness (variance in wait times)
- Throughput (jobs/hour)
- Efficiency (GPU-hours utilized / GPU-hours available)

## Storage

Start with append-only JSONL files:
```bash
logs/
  decisions.jsonl      # one line per decision
  lifecycle.jsonl      # one line per task event
  snapshots.jsonl      # one line per cluster snapshot
```

Graduate to time-series DB if volume demands.