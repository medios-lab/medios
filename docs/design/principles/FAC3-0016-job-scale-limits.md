```
CATALOG   FAC3-0016
TITLE     What is a Job (and Why 1000 Break)
EDITION   001
STATUS    Draft
```

## Job Definition (Medios)

Job = { cmd, cpu, mem, [gpu] }
- cmd: arbitrary shell command
- resources: what it needs to run
- No other semantics (duration, retries, dependencies)

## Scale Points (single H100 box)

### 1. Echo jobs (cmd: "echo $i", cpu: 0.001, mem: 1)
Limit: Linux PID max (~32K) or cgroup controller overhead
Actual: ~1000 concurrent before context switching kills perf

### 2. CPU-bound jobs (cmd: "python compute.py", cpu: 1.0, mem: 128)
Limit: Physical cores (e.g., 64 cores = 64 concurrent)
Actual: 64-128 with oversubscription

### 3. GPU training jobs (cmd: "python train.py", cpu: 8, mem: 64000, gpu: 1)
Limit: Physical GPUs (1 H100 = 1 job without MIG)
With MIG 7-way: 7 concurrent max
Actual: 1-7 depending on VRAM needs

### 4. Memory-bound jobs (cmd: "python cache_load.py", cpu: 0.1, mem: 32000)
Limit: Physical RAM / job mem (e.g., 512GB / 32GB = 16)
Actual: 10-16 with OS overhead

## Breaking Points

At ~100 concurrent tasks:
- Mesos controller → node heartbeat latency increases
- Offer cycle time degrades (batching helps)

At ~1000 concurrent tasks:
- Linux scheduler overhead dominates
- cgroup creation/teardown becomes bottleneck
- Log aggregation breaks unless async

At ~10000 tasks (attempted):
- OOM killer activates
- File descriptor limits hit
- Network stack (localhost) saturates

## Real Scale Limits (single-box)
- Echo/trivial: 500-1000 concurrent
- CPU-bound: # cores × 2
- Memory-bound: RAM / job_mem  
- GPU-bound: # GPUs (or MIG instances)

## Multi-node Changes
- Network becomes limit at ~10K tasks/cluster
- Scheduler state size matters (in-memory)
- Mesos controller becomes bottleneck ~50K tasks without tuning