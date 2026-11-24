```
CATALOG   FAC3-0019
TITLE     Scheduling as Online Learning Problem
EDITION   001
STATUS    Draft
```

## Setup
- Action space: which job to assign to which offer
- State: queue state, cluster utilization, time
- Reward: define later based on what you observe matters
- No predetermined policy

## Phase 1: Random + Logging
```python
def on_offers(offers):
    for offer in offers:
        if jobs:
            job = random.choice(jobs)  # random policy
            if fits(offer, job):
                launch(offer, job)
                log_decision(timestamp, offer, job, cluster_state)
```

## Phase 2: Observe Patterns
- GPU utilization over time
- Queue depths by job type
- Completion rates vs resource allocation
- Failure patterns

## Phase 3: Extract Reward Signal
Maybe you discover:
- Reward = GPU utilization × job completion rate
- Or: Reward = -max(queue_wait_time)
- Or: Reward = throughput / power consumption
- Let the data tell you

## Phase 4: Online Learning
- Start with epsilon-greedy
- Try contextual bandits
- Graduate to simple RL if needed
- Keep random baseline for comparison

## What This Avoids
- Premature optimization
- Academic fairness debates  
- Complex policies that don't match reality
- Lock-in to wrong objective

## Mesos Enables This
- Offers are your action opportunities
- Framework owns the policy completely
- Can A/B test different schedulers live
- No platform opinions to fight

## Bootstrap Plan
Week 1: Random scheduler, log everything
Week 2: Analyze logs, find natural reward
Week 3: Simple greedy improvement
Week 4: Compare to random baseline
Then: RL if the problem deserves it