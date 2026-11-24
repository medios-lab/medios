```
CATALOG   FAC3-0015
TITLE     Pure Loop Validation
EDITION   001
STATUS    Draft
```

```bash
# Terminal 1: controller
# Terminal 2: node registered against the controller
# Terminal 3: minimal HTTP v1 scheduler that submits two command-executor tasks

# Expected output (Terminal 3):
Task TASK_RUNNING
Task TASK_RUNNING
Task TASK_FINISHED
Task TASK_FINISHED

# Node sandbox shows:
stdout: "Medios hello"
stdout: [training output]

# Metrics to capture:
SUBSCRIBE → first OFFERS:     < 500ms
OFFER → ACCEPT/LAUNCH:         < 50ms
LAUNCH → TASK_RUNNING:         < 1s
Total runtime:                 < 10s

# What this proves:
1. Framework is scheduler only (no executor code)
2. Resources are just numbers matched to needs
3. Command executor handles everything
4. No platforms, no YAML, no abstractions
```