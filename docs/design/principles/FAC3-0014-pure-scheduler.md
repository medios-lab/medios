```
CATALOG   FAC3-0014
TITLE     Minimal HTTP v1 Scheduler
EDITION   001
STATUS    Draft
```

```python
#!/usr/bin/env python3
# tools/pure/scheduler.py

import json
import requests
import sys
import uuid

CONTROLLER = sys.argv[1] if len(sys.argv) > 1 else "http://127.0.0.1:5050"
FRAMEWORK_ID = None
STREAM_ID = None

jobs = [
    {"cmd": "echo 'Medios hello' && sleep 5", "cpu": 0.1, "mem": 32},
    {"cmd": "python train.py --epochs=1", "cpu": 1.0, "mem": 1024}
]

def call(type, **kwargs):
    global STREAM_ID
    body = json.dumps({"type": type, **kwargs})
    headers = {"Content-Type": "application/json"}
    if STREAM_ID:
        headers["Medios-Stream-Id"] = STREAM_ID
    r = requests.post(
        f"{CONTROLLER}/api/v1/scheduler",
        data=body,
        headers=headers,
        stream=True)
    STREAM_ID = r.headers.get("Medios-Stream-Id", STREAM_ID)
    return r

def handle_offers(offers):
    for offer in offers:
        if jobs:
            job = jobs[0]
            cpus = next((r["scalar"]["value"] for r in offer["resources"] if r["name"] == "cpus"), 0)
            mem = next((r["scalar"]["value"] for r in offer["resources"] if r["name"] == "mem"), 0)
            if cpus >= job["cpu"] and mem >= job["mem"]:
                jobs.pop(0)
                task_id = str(uuid.uuid4())
                call("ACCEPT", 
                     offer_ids=[{"value": offer["id"]["value"]}],
                     operations=[{
                         "type": "LAUNCH",
                         "launch": {
                             "task_infos": [{
                                 "name": "task",
                                 "task_id": {"value": task_id},
                                 "node_id": offer["node_id"],
                                 "resources": [
                                     {"name": "cpus", "type": "SCALAR", "scalar": {"value": job["cpu"]}},
                                     {"name": "mem", "type": "SCALAR", "scalar": {"value": job["mem"]}}
                                 ],
                                 "command": {"value": job["cmd"]}
                             }]
                         }
                     }])
                continue
        call("DECLINE", offer_ids=[{"value": offer["id"]["value"]}])

# Subscribe
r = call("SUBSCRIBE", 
         framework_info={"user": "root", "name": "pure", "role": "*"})

for line in r.iter_lines():
    if not line: continue
    event = json.loads(line)
    if event["type"] == "SUBSCRIBED":
        FRAMEWORK_ID = event["subscribed"]["framework_id"]["value"]
    elif event["type"] == "OFFERS":
        handle_offers(event["offers"]["offers"])
    elif event["type"] == "UPDATE":
        call("ACKNOWLEDGE",
             node_id=event["update"]["status"]["node_id"],
             task_id=event["update"]["status"]["task_id"],
             uuid=event["update"]["status"]["uuid"])
        print(f"Task {event['update']['status']['state']}")
        if event["update"]["status"]["state"] == "TASK_FINISHED":
            if not jobs:
                sys.exit(0)

# Run: python3 tools/pure/scheduler.py http://127.0.0.1:5050
```
