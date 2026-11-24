---
name: medios-first-framework
description: Write a one-off Medios framework in Python that runs three Euler-sum tasks on the preview node, reads each task's output and checks it against the known value. Use for step 2 of the Medios preview, after setup succeeded.
---

# Your first framework

You write the framework yourself; it is the user's starting point for their own
work, so keep it short and readable. Everything you need from the SDK is in
`sdk-reference.md` next to this file. Don't read the SDK source.

## The job

Launch three tasks, one per sum, each computing the partial sum to N = 1,000,000:

| Task id | Sum | Expected value |
| --- | --- | --- |
| `zeta-2` | Σ 1/n² | π²/6 |
| `zeta-4` | Σ 1/n⁴ | π⁴/90 |
| `zeta-6` | Σ 1/n⁶ | π⁶/945 |

Each task runs one shell command and prints its result to stdout. This one
needs nothing beyond `awk`, which every Mac and Linux machine has; replace `S`
with 2, 4 or 6:

```sh
awk 'BEGIN{t=0; for(n=1000000;n>=1;n--) t+=1/(n^S); printf "%.15f\n", t}'
```

Summing from the smallest term up keeps the rounding error small. The partial
sum falls short of the full series by less than 1/N, so a result passes when it
is within 2/N = 2e-6 of the expected value. Give each task `cpus: 0.5` and
`mem: 64` (MB).

## Steps

1. Write `euler.py` in the bundle directory. Read `state/run.json` for
   `controller_url`, `work_dir` and `node_port`.
2. Run it: `bin/medios-python euler.py`. That command makes the SDK importable;
   plain `python` won't find it.
3. It should print one line per sum with the task state, the value read back,
   the expected value and the difference, and exit 0 only if all three pass.
4. Check that `GET <controller_url>/state` lists no active frameworks
   afterwards; a framework left behind means teardown didn't run.

Report the three lines to the user as the verified result.
