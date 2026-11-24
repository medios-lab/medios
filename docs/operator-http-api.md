---
title: Apache Mesos - Operator HTTP API
layout: documentation
---

# Operator HTTP API

## Overview

Both controllers and nodes provide the `/api/v1` endpoint as the base URL for performing operator-related operations.

Similar to the [Scheduler](scheduler-http-api.md) and [Executor](executor-http-api.md) HTTP APIs, the operator endpoints only accept HTTP POST requests. The request body should be encoded in JSON (**Content-Type: application/json**) or Protobuf (**Content-Type: application/x-protobuf**).

For requests that Medios can answer synchronously and immediately, an HTTP response will be sent with status **200 OK**, possibly including a response body encoded in JSON or Protobuf. The encoding depends on the **Accept** header present in the request (the default encoding is JSON). Responses will be gzip compressed if the **Accept-Encoding** header is set to "gzip".

For requests that require asynchronous processing (e.g., `RESERVE_RESOURCES`), an HTTP response will be sent with status **202 Accepted**. For requests that result in a stream of events (`SUBSCRIBE`), a streaming HTTP response with [RecordIO](recordio.md) encoding is sent. Currently, gzip compression is not supported for streaming responses.

## Controller API

This API contains all the calls accepted by the controller. The canonical source of this information is [controller.proto](../include/medios/v1/controller/controller.proto) (NOTE: The protobuf definitions are subject to change before the beta API is finalized). These calls are typically made by human operators, tooling or services (e.g., Medios WebUI). While schedulers can make these calls as well, schedulers are expected to use the [Scheduler HTTP API](scheduler-http-api.md).

### Calls To Controller And Responses

Below are the example calls to the controller that result in synchronous responses from the API.

### GET_HEALTH

This call retrieves the health status of controller.

```
GET_HEALTH HTTP Request (JSON):

POST /api/v1  HTTP/1.1

Host: controllerhost:5050
Content-Type: application/json
Accept: application/json

{
  "type": "GET_HEALTH"
}


GET_HEALTH HTTP Response (JSON):

HTTP/1.1 200 OK

Content-Type: application/json

{
  "type": "GET_HEALTH",
  "get_health": {
    "healthy": true
  }
}

```

### GET_FLAGS

This call retrieves the controller's overall flag configuration.

```
GET_FLAGS HTTP Request (JSON):

POST /api/v1  HTTP/1.1

Host: controllerhost:5050
Content-Type: application/json
Accept: application/json

{
  "type": "GET_FLAGS"
}


GET_FLAGS HTTP Response (JSON):

HTTP/1.1 200 OK

Content-Type: application/json

{
  "type": "GET_FLAGS",
  "get_flags": {
    "flags": [
      {
        "name": "acls",
        "value": ""
      },
      {
        "name": "node_ping_timeout",
        "value": "15secs"
      },
      {
        "name": "node_reregister_timeout",
        "value": "10mins"
      },
      {
        "name": "allocation_interval",
        "value": "1secs"
      },
      {
        "name": "allocator",
        "value": "HierarchicalDRF"
      },
      {
        "name": "authenticate_nodes",
        "value": "true"
      },
      {
        "name": "authenticate_frameworks",
        "value": "true"
      },
      {
        "name": "authenticate_http_frameworks",
        "value": "true"
      },
      {
        "name": "authenticate_http_readonly",
        "value": "true"
      },
      {
        "name": "authenticate_http_readwrite",
        "value": "true"
      },
      {
        "name": "authenticators",
        "value": "identity_assertion"
      },
      {
        "name": "authorizers",
        "value": "local"
      },
      {
        "name": "credentials",
        "value": "/tmp/directory/credentials"
      },
      {
        "name": "framework_sorter",
        "value": "drf"
      },
      {
        "name": "help",
        "value": "false"
      },
      {
        "name": "hostname_lookup",
        "value": "true"
      },
      {
        "name": "http_authenticators",
        "value": "basic"
      },
      {
        "name": "http_framework_authenticators",
        "value": "basic"
      },
      {
        "name": "initialize_driver_logging",
        "value": "true"
      },
      {
        "name": "log_auto_initialize",
        "value": "true"
      },
      {
        "name": "logbufsecs",
        "value": "0"
      },
      {
        "name": "logging_level",
        "value": "INFO"
      },
      {
        "name": "max_node_ping_timeouts",
        "value": "5"
      },
      {
        "name": "max_completed_frameworks",
        "value": "50"
      },
      {
        "name": "max_completed_tasks_per_framework",
        "value": "1000"
      },
      {
        "name": "quiet",
        "value": "false"
      },
      {
        "name": "recovery_node_removal_limit",
        "value": "100%"
      },
      {
        "name": "registry",
        "value": "replicated_log"
      },
      {
        "name": "registry_fetch_timeout",
        "value": "1mins"
      },
      {
        "name": "registry_store_timeout",
        "value": "100secs"
      },
      {
        "name": "registry_strict",
        "value": "true"
      },
      {
        "name": "root_submissions",
        "value": "true"
      },
      {
        "name": "user_sorter",
        "value": "drf"
      },
      {
        "name": "version",
        "value": "false"
      },
      {
        "name": "work_dir",
        "value": "/tmp/directory/controller"
      },
      {
        "name": "zk_session_timeout",
        "value": "10secs"
      }
    ]
  }
}

```

### GET_VERSION

This call retrieves the controller's version information.

```
GET_VERSION HTTP Request (JSON):

POST /api/v1  HTTP/1.1

Host: controllerhost:5050
Content-Type: application/json
Accept: application/json

{
  "type": "GET_VERSION"
}


GET_VERSION HTTP Response (JSON):

HTTP/1.1 200 OK

Content-Type: application/json

{
  "type": "GET_VERSION",
  "get_version": {
    "version_info": {
      "version": "1.0.0",
      "build_date": "2016-06-24 23:18:37",
      "build_time": 1466810317,
      "build_user": "root"
    }
  }
}

```

### GET_METRICS

This call gives the snapshot of current metrics to the end user. If `timeout` is
set in the call, it will be used to determine the maximum amount of time the
API will take to respond. If the timeout is exceeded, some metrics may not be
included in the response.

```
GET_METRICS HTTP Request (JSON):

POST /api/v1  HTTP/1.1

Host: controllerhost:5050
Content-Type: application/json
Accept: application/json

{
  "type": "GET_METRICS",
  "get_metrics": {
    "timeout": {
      "nanoseconds": 5000000000
    }
  }
}


GET_METRICS HTTP Response (JSON):

HTTP/1.1 200 OK

Content-Type: application/json

{
  "type": "GET_METRICS",
  "get_metrics": {
    "metrics": [
      {
        "name": "allocator/event_queue_dispatches",
        "value": 1.0
      },
      {
        "name": "controller/nodes_active",
        "value": 0.0
      },
      {
        "name": "allocator/medios/resources/cpus/total",
        "value": 0.0
      },
      {
        "name": "controller/messages_revive_offers",
        "value": 0.0
      },
      {
        "name": "allocator/medios/allocation_runs",
        "value": 0.0
      },
      {
        "name": "controller/mem_used",
        "value": 0.0
      },
      {
        "name": "controller/valid_executor_to_framework_messages",
        "value": 0.0
      },
      {
        "name": "allocator/medios/resources/mem/total",
        "value": 0.0
      },
      {
        "name": "log/recovered",
        "value": 1.0
      },
      {
        "name": "registrar/registry_size_bytes",
        "value": 123.0
      },
      {
        "name": "controller/nodes_inactive",
        "value": 0.0
      },
      {
        "name": "controller/messages_unregister_node",
        "value": 0.0
      },
      {
        "name": "controller/gpus_total",
        "value": 0.0
      },
      {
        "name": "controller/disk_revocable_total",
        "value": 0.0
      },
      {
        "name": "controller/gpus_percent",
        "value": 0.0
      },
      {
        "name": "controller/mem_revocable_used",
        "value": 0.0
      },
      {
        "name": "controller/node_shutdowns_completed",
        "value": 0.0
      },
      {
        "name": "controller/invalid_status_updates",
        "value": 0.0
      },
      {
        "name": "controller/node_removals",
        "value": 0.0
      },
      {
        "name": "controller/messages_status_update",
        "value": 0.0
      },
      {
        "name": "controller/messages_framework_to_executor",
        "value": 0.0
      },
      {
        "name": "controller/cpus_revocable_percent",
        "value": 0.0
      },
      {
        "name": "controller/recovery_node_removals",
        "value": 0.0
      },
      {
        "name": "controller/event_queue_dispatches",
        "value": 0.0
      },
      {
        "name": "controller/messages_update_node",
        "value": 0.0
      },
      {
        "name": "allocator/medios/resources/mem/offered_or_allocated",
        "value": 0.0
      },
      {
        "name": "controller/messages_register_framework",
        "value": 0.0
      },
      {
        "name": "controller/cpus_percent",
        "value": 0.0
      },
      {
        "name": "controller/node_reregistrations",
        "value": 0.0
      },
      {
        "name": "controller/cpus_revocable_total",
        "value": 0.0
      },
      {
        "name": "controller/gpus_revocable_total",
        "value": 0.0
      },
      {
        "name": "controller/valid_status_updates",
        "value": 0.0
      },
      {
        "name": "system/load_15min",
        "value": 1.25
      },
      {
        "name": "controller/event_queue_http_requests",
        "value": 0.0
      },
      {
        "name": "controller/messages_decline_offers",
        "value": 0.0
      },
      {
        "name": "controller/tasks_staging",
        "value": 0.0
      },
      {
        "name": "controller/messages_register_node",
        "value": 0.0
      },
      {
        "name": "allocator/medios/resources/disk/offered_or_allocated",
        "value": 0.0
      },
      {
        "name": "system/mem_free_bytes",
        "value": 2320146432.0
      },
      {
        "name": "system/cpus_total",
        "value": 4.0
      },
      {
        "name": "controller/mem_percent",
        "value": 0.0
      },
      {
        "name": "controller/event_queue_messages",
        "value": 0.0
      },
      {
        "name": "controller/messages_reregister_node",
        "value": 0.0
      },
      {
        "name": "controller/gpus_used",
        "value": 0.0
      },
      {
        "name": "registrar/state_fetch_ms",
        "value": 16.787968
      },
      {
        "name": "controller/messages_launch_tasks",
        "value": 0.0
      },
      {
        "name": "controller/gpus_revocable_percent",
        "value": 0.0
      },
      {
        "name": "controller/disk_percent",
        "value": 0.0
      },
      {
        "name": "system/load_1min",
        "value": 1.74
      },
      {
        "name": "registrar/queued_operations",
        "value": 0.0
      },
      {
        "name": "controller/nodes_disconnected",
        "value": 0.0
      },
      {
        "name": "controller/invalid_status_update_acknowledgements",
        "value": 0.0
      },
      {
        "name": "system/load_5min",
        "value": 1.65
      },
      {
        "name": "controller/tasks_failed",
        "value": 0.0
      },
      {
        "name": "controller/node_registrations",
        "value": 0.0
      },
      {
        "name": "controller/frameworks_connected",
        "value": 0.0
      },
      {
        "name": "allocator/medios/event_queue_dispatches",
        "value": 0.0
      },
      {
        "name": "controller/messages_executor_to_framework",
        "value": 0.0
      },
      {
        "name": "system/mem_total_bytes",
        "value": 8057147392.0
      },
      {
        "name": "controller/cpus_revocable_used",
        "value": 0.0
      },
      {
        "name": "controller/tasks_killing",
        "value": 0.0
      },
      {
        "name": "allocator/medios/resources/cpus/offered_or_allocated",
        "value": 0.0
      },
      {
        "name": "controller/messages_exited_executor",
        "value": 0.0
      },
      {
        "name": "controller/valid_status_update_acknowledgements",
        "value": 0.0
      },
      {
        "name": "controller/disk_used",
        "value": 0.0
      },
      {
        "name": "controller/gpus_revocable_used",
        "value": 0.0
      },
      {
        "name": "controller/disk_revocable_percent",
        "value": 0.0
      },
      {
        "name": "controller/mem_revocable_percent",
        "value": 0.0
      },
      {
        "name": "controller/invalid_executor_to_framework_messages",
        "value": 0.0
      },
      {
        "name": "controller/node_shutdowns_scheduled",
        "value": 0.0
      },
      {
        "name": "controller/node_removals/reason_registered",
        "value": 0.0
      },
      {
        "name": "controller/messages_suppress_offers",
        "value": 0.0
      },
      {
        "name": "controller/uptime_secs",
        "value": 0.038900992
      },
      {
        "name": "allocator/medios/resources/disk/total",
        "value": 0.0
      },
      {
        "name": "controller/node_removals/reason_unregistered",
        "value": 0.0
      },
      {
        "name": "controller/disk_total",
        "value": 0.0
      },
      {
        "name": "controller/messages_resource_request",
        "value": 0.0
      },
      {
        "name": "controller/cpus_total",
        "value": 0.0
      },
      {
        "name": "controller/valid_framework_to_executor_messages",
        "value": 0.0
      },
      {
        "name": "controller/cpus_used",
        "value": 0.0
      },
      {
        "name": "controller/node_removals/reason_unhealthy",
        "value": 0.0
      },
      {
        "name": "controller/messages_kill_task",
        "value": 0.0
      },
      {
        "name": "controller/node_shutdowns_canceled",
        "value": 0.0
      },
      {
        "name": "controller/messages_deactivate_framework",
        "value": 0.0
      },
      {
        "name": "controller/messages_unregister_framework",
        "value": 0.0
      },
      {
        "name": "controller/mem_revocable_total",
        "value": 0.0
      },
      {
        "name": "controller/messages_reregister_framework",
        "value": 0.0
      },
      {
        "name": "controller/dropped_messages",
        "value": 0.0
      },
      {
        "name": "controller/invalid_framework_to_executor_messages",
        "value": 0.0
      },
      {
        "name": "controller/tasks_error",
        "value": 0.0
      },
      {
        "name": "controller/tasks_lost",
        "value": 0.0
      },
      {
        "name": "controller/messages_reconcile_operations",
        "value": 0.0
      },
      {
        "name": "controller/messages_reconcile_tasks",
        "value": 0.0
      },
      {
        "name": "controller/tasks_killed",
        "value": 0.0
      },
      {
        "name": "controller/tasks_finished",
        "value": 0.0
      },
      {
        "name": "controller/frameworks_inactive",
        "value": 0.0
      },
      {
        "name": "controller/tasks_running",
        "value": 0.0
      },
      {
        "name": "controller/tasks_starting",
        "value": 0.0
      },
      {
        "name": "registrar/state_store_ms",
        "value": 5.55392
      },
      {
        "name": "controller/mem_total",
        "value": 0.0
      },
      {
        "name": "controller/outstanding_offers",
        "value": 0.0
      },
      {
        "name": "controller/operator_event_stream_subscribers",
        "value": 0.0
      },
      {
        "name": "controller/frameworks_active",
        "value": 0.0
      },
      {
        "name": "controller/messages_authenticate",
        "value": 0.0
      },
      {
        "name": "controller/disk_revocable_used",
        "value": 0.0
      },
      {
        "name": "controller/frameworks_disconnected",
        "value": 0.0
      },
      {
        "name": "controller/nodes_connected",
        "value": 0.0
      },
      {
        "name": "controller/messages_status_update_acknowledgement",
        "value": 0.0
      },
      {
        "name": "controller/elected",
        "value": 1.0
      }
    ]
  }
}

```

### GET_LOGGING_LEVEL

This call retrieves the controller's logging level.

```
GET_LOGGING_LEVEL HTTP Request (JSON):

POST /api/v1  HTTP/1.1

Host: controllerhost:5050
Content-Type: application/json
Accept: application/json

{
  "type": "GET_LOGGING_LEVEL"
}


GET_LOGGING_LEVEL HTTP Response (JSON):

HTTP/1.1 200 OK

Content-Type: application/json

{
  "type": "GET_LOGGING_LEVEL",
  "get_logging_level": {
    "level": 0
  }
}

```

### SET_LOGGING_LEVEL

Sets the logging verbosity level for a specified duration for controller. Medios uses
[glog](https://github.com/google/glog) for logging. The library only uses
verbose logging which means nothing will be output unless the verbosity
level is set (by default it's 0, libprocess uses levels 1, 2, and 3).

```
SET_LOGGING_LEVEL HTTP Request (JSON):

POST /api/v1  HTTP/1.1

Host: controllerhost:5050
Content-Type: application/json
Accept: application/json

{
  "type": "SET_LOGGING_LEVEL",
  "set_logging_level": {
    "duration": {
      "nanoseconds": 60000000000
    },
    "level": 1
  }
}


SET_LOGGING_LEVEL HTTP Response:

HTTP/1.1 202 Accepted

```

### LIST_FILES

This call retrieves the file listing for a directory in controller.

```
LIST_FILES HTTP Request (JSON):

POST /api/v1  HTTP/1.1

Host: controllerhost:5050
Content-Type: application/json
Accept: application/json

{
  "type": "LIST_FILES",
  "list_files": {
    "path": "one/"
  }
}


LIST_FILES HTTP Response (JSON):

HTTP/1.1 200 OK

Content-Type: application/json

{
  "type": "LIST_FILES",
  "list_files": {
    "file_infos": [
      {
        "gid": "root",
        "mode": 16877,
        "mtime": {
          "nanoseconds": 1470820172000000000
        },
        "nlink": 2,
        "path": "one/2",
        "size": 4096,
        "uid": "root"
      },
      {
        "gid": "root",
        "mode": 16877,
        "mtime": {
          "nanoseconds": 1470820172000000000
        },
        "nlink": 2,
        "path": "one/3",
        "size": 4096,
        "uid": "root"
      },
      {
        "gid": "root",
        "mode": 33188,
        "mtime": {
          "nanoseconds": 1470820172000000000
        },
        "nlink": 1,
        "path": "one/two",
        "size": 3,
        "uid": "root"
      }
    ]
  }
}

```

### READ_FILE

Reads data from a file on the controller. This call takes the path of the
file to be read, the offset to start reading, and the maximum number of
bytes to read.

```
READ_FILE HTTP Request (JSON):

POST /api/v1  HTTP/1.1

Host: controllerhost:5050
Content-Type: application/json
Accept: application/json

{
  "type": "READ_FILE",
  "read_file": {
    "length": 6,
    "offset": 1,
    "path": "myname"
  }
}


READ_FILE HTTP Response (JSON):

HTTP/1.1 200 OK

Content-Type: application/json

{
  "type": "READ_FILE",
  "read_file": {
    "data": "b2R5",
    "size": 4
  }
}

```

### GET_STATE

This call retrieves the overall cluster state.

```
GET_STATE HTTP Request (JSON):

POST /api/v1  HTTP/1.1

Host: controllerhost:5050
Content-Type: application/json
Accept: application/json

{
  "type": "GET_STATE"
}


GET_STATE HTTP Response (JSON):

HTTP/1.1 200 OK

Content-Type: application/json

{
  "type": "GET_STATE",
  "get_state": {
    "get_nodes": {
      "nodes": [
        {
          "active": true,
          "node_info": {
            "hostname": "myhost",
            "id": {
              "value": "628984d0-4213-4140-bcb0-99d7ef46b1df-S0"
            },
            "port": 34626,
            "resources": [
              {
                "name": "cpus",
                "role": "*",
                "scalar": {
                  "value": 2.0
                },
                "type": "SCALAR"
              },
              {
                "name": "mem",
                "role": "*",
                "scalar": {
                  "value": 1024.0
                },
                "type": "SCALAR"
              },
              {
                "name": "disk",
                "role": "*",
                "scalar": {
                  "value": 1024.0
                },
                "type": "SCALAR"
              },
              {
                "name": "ports",
                "ranges": {
                  "range": [
                    {
                      "begin": 31000,
                      "end": 32000
                    }
                  ]
                },
                "role": "*",
                "type": "RANGES"
              }
            ]
          },
          "pid": "node(3)@127.0.1.1:34626",
          "registered_time": {
            "nanoseconds": 1470820172046531840
          },
          "total_resources": [
            {
              "name": "cpus",
              "role": "*",
              "scalar": {
                "value": 2.0
              },
              "type": "SCALAR"
            },
            {
              "name": "mem",
              "role": "*",
              "scalar": {
                "value": 1024.0
              },
              "type": "SCALAR"
            },
            {
              "name": "disk",
              "role": "*",
              "scalar": {
                "value": 1024.0
              },
              "type": "SCALAR"
            },
            {
              "name": "ports",
              "ranges": {
                "range": [
                  {
                    "begin": 31000,
                    "end": 32000
                  }
                ]
              },
              "role": "*",
              "type": "RANGES"
            }
          ],
          "version": "1.1.0"
        }
      ]
    },
    "get_executors": {
      "executors": [
        {
          "node_id": {
            "value": "628984d0-4213-4140-bcb0-99d7ef46b1df-S0"
          },
          "executor_info": {
            "command": {
              "shell": true,
              "value": ""
            },
            "executor_id": {
              "value": "default"
            },
            "framework_id": {
              "value": "628984d0-4213-4140-bcb0-99d7ef46b1df-0000"
            }
          }
        }
      ]
    },
    "get_frameworks": {
      "frameworks": [
        {
          "active": true,
          "connected": true,
          "framework_info": {
            "checkpoint": false,
            "failover_timeout": 0.0,
            "hostname": "abcdev",
            "id": {
              "value": "628984d0-4213-4140-bcb0-99d7ef46b1df-0000"
            },
            "name": "default",
            "principal": "my-principal",
            "role": "*",
            "user": "root"
          },
          "registered_time": {
            "nanoseconds": 1470820172039300864
          },
          "reregistered_time": {
            "nanoseconds": 1470820172039300864
          }
        }
      ]
    },
    "get_tasks": {
      "completed_tasks": [
        {
          "node_id": {
            "value": "628984d0-4213-4140-bcb0-99d7ef46b1df-S0"
          },
          "executor_id": {
            "value": "default"
          },
          "framework_id": {
            "value": "628984d0-4213-4140-bcb0-99d7ef46b1df-0000"
          },
          "name": "test-task",
          "resources": [
            {
              "name": "cpus",
              "role": "*",
              "scalar": {
                "value": 2.0
              },
              "type": "SCALAR"
            },
            {
              "name": "mem",
              "role": "*",
              "scalar": {
                "value": 1024.0
              },
              "type": "SCALAR"
            },
            {
              "name": "disk",
              "role": "*",
              "scalar": {
                "value": 1024.0
              },
              "type": "SCALAR"
            },
            {
              "name": "ports",
              "ranges": {
                "range": [
                  {
                    "begin": 31000,
                    "end": 32000
                  }
                ]
              },
              "role": "*",
              "type": "RANGES"
            }
          ],
          "state": "TASK_FINISHED",
          "status_update_state": "TASK_FINISHED",
          "status_update_uuid": "IWjmPnfgQCWxGVlNNwctcg==",
          "statuses": [
            {
              "node_id": {
                "value": "628984d0-4213-4140-bcb0-99d7ef46b1df-S0"
              },
              "container_status": {
                "network_infos": [
                  {
                    "ip_addresses": [
                      {
                        "ip_address": "127.0.1.1"
                      }
                    ]
                  }
                ]
              },
              "executor_id": {
                "value": "default"
              },
              "source": "SOURCE_EXECUTOR",
              "state": "TASK_RUNNING",
              "task_id": {
                "value": "eb5cb680-a998-4605-8811-e79db8734c02"
              },
              "timestamp": 1470820172.07315,
              "uuid": "hTaLQ0b5Q1OZuab7QclTKQ=="
            },
            {
              "node_id": {
                "value": "628984d0-4213-4140-bcb0-99d7ef46b1df-S0"
              },
              "container_status": {
                "network_infos": [
                  {
                    "ip_addresses": [
                      {
                        "ip_address": "127.0.1.1"
                      }
                    ]
                  }
                ]
              },
              "executor_id": {
                "value": "default"
              },
              "source": "SOURCE_EXECUTOR",
              "state": "TASK_FINISHED",
              "task_id": {
                "value": "eb5cb680-a998-4605-8811-e79db8734c02"
              },
              "timestamp": 1470820172.09382,
              "uuid": "IWjmPnfgQCWxGVlNNwctcg=="
            }
          ],
          "task_id": {
            "value": "eb5cb680-a998-4605-8811-e79db8734c02"
          }
        }
      ]
    }
  }
}

```

### GET_NODES

This call retrieves information about nodes tracked by the controller across three lifecycle lists (`#541`):
- `nodes`: currently registered nodes (full `NodeInfo`, resources, capabilities, and connection/drain state).
- `recovered_nodes`: nodes recovered from the registry after controller failover that have not yet re-registered (`NodeInfo` with `VIEW_ROLE`-filtered resources).
- `unreachable_nodes`: nodes marked unreachable in the registry (`id` and `unreachable_time` only). Once a node is marked unreachable (or until it re-registers or is garbage-collected / retired via `MARK_NODE_GONE`), the registry retains only its `NodeID` and the transition timestamp (`unreachable_time`); historical `NodeInfo` fields (hostname, port, resources) are not stored in the unreachable registry list and must be captured before retiring a node or correlated from prior `GET_NODES` / `NODE_REMOVED` observations.

```
GET_NODES HTTP Request (JSON):

POST /api/v1  HTTP/1.1

Host: controllerhost:5050
Content-Type: application/json
Accept: application/json

{
  "type": "GET_NODES"
}


GET_NODES HTTP Response (JSON):

HTTP/1.1 200 OK

Content-Type: application/json

{{
  "type": "GET_NODES",
  "get_nodes": {
    "nodes": [
      {
        "active": true,
        "node_info": {
          "hostname": "host",
          "id": {
            "value": "3669ea49-c3c4-4b13-adee-05b8f9cb2562-S0"
          },
          "port": 34626,
          "resources": [
            {
              "name": "cpus",
              "role": "*",
              "scalar": {
                "value": 2.0
              },
              "type": "SCALAR"
            },
            {
              "name": "mem",
              "role": "*",
              "scalar": {
                "value": 1024.0
              },
              "type": "SCALAR"
            },
            {
              "name": "disk",
              "role": "*",
              "scalar": {
                "value": 1024.0
              },
              "type": "SCALAR"
            },
            {
              "name": "ports",
              "ranges": {
                "range": [
                  {
                    "begin": 31000,
                    "end": 32000
                  }
                ]
              },
              "role": "*",
              "type": "RANGES"
            }
          ]
        },
        "pid": "node(1)@127.0.1.1:34626",
        "registered_time": {
          "nanoseconds": 1470820171393027072
        },
        "total_resources": [
          {
            "name": "cpus",
            "role": "*",
            "scalar": {
              "value": 2.0
            },
            "type": "SCALAR"
          },
          {
            "name": "mem",
            "role": "*",
            "scalar": {
              "value": 1024.0
            },
            "type": "SCALAR"
          },
          {
            "name": "disk",
            "role": "*",
            "scalar": {
              "value": 1024.0
            },
            "type": "SCALAR"
          },
          {
            "name": "ports",
            "ranges": {
              "range": [
                {
                  "begin": 31000,
                  "end": 32000
                }
              ]
            },
            "role": "*",
            "type": "RANGES"
          }
        ],
        "version": "1.1.0"
      }
    ]
  }
}

```

### GET_FRAMEWORKS

This call retrieves information about all the frameworks known to the controller.

```
GET_FRAMEWORKS HTTP Request (JSON):

POST /api/v1  HTTP/1.1

Host: controllerhost:5050
Content-Type: application/json
Accept: application/json

{
  "type": "GET_FRAMEWORKS"
}


GET_FRAMEWORKS HTTP Response (JSON):

HTTP/1.1 200 OK

Content-Type: application/json

{
  "type": "GET_FRAMEWORKS",
  "get_frameworks": {
    "frameworks": [
      {
        "active": true,
        "connected": true,
        "framework_info": {
          "checkpoint": false,
          "failover_timeout": 0.0,
          "hostname": "myhost",
          "id": {
            "value": "361be53a-4d1b-42c1-bec3-e3979eff90bd-0000"
          },
          "name": "default",
          "principal": "my-principal",
          "role": "*",
          "user": "root"
        },
        "registered_time": {
          "nanoseconds": 1470820171578306816
        },
        "reregistered_time": {
          "nanoseconds": 1470820171578306816
        }
      }
    ]
  }
}

```

### GET_EXECUTORS

Queries about all the executors known to the controller.

```
GET_EXECUTORS HTTP Request (JSON):

POST /api/v1  HTTP/1.1

Host: controllerhost:5050
Content-Type: application/json
Accept: application/json

{
  "type": "GET_EXECUTORS"
}


GET_EXECUTORS HTTP Response (JSON):

HTTP/1.1 200 OK

Content-Type: application/json

{
  "type": "GET_EXECUTORS",
  "get_executors": {
    "executors": [
      {
        "node_id": {
          "value": "f2ddc41d-6284-405e-8642-34953093140f-S0"
        },
        "executor_info": {
          "command": {
            "shell": true,
            "value": "exit 1"
          },
          "executor_id": {
            "value": "default"
          },
          "framework_id": {
            "value": "f2ddc41d-6284-405e-8642-34953093140f-0000"
          }
        }
      }
    ]
  }
}

```

### GET_TASKS

Query about all the tasks known to the controller.

```
GET_TASKS HTTP Request (JSON):

POST /api/v1  HTTP/1.1

Host: controllerhost:5050
Content-Type: application/json
Accept: application/json

{
  "type": "GET_TASKS"
}


GET_TASKS HTTP Response (JSON):

HTTP/1.1 200 OK

Content-Type: application/json

{
  "type": "GET_TASKS",
  "get_tasks": {
    "tasks": [
      {
        "node_id": {
          "value": "d4bd102f-e25f-46dc-bb5d-8b10bca133d8-S0"
        },
        "executor_id": {
          "value": "default"
        },
        "framework_id": {
          "value": "d4bd102f-e25f-46dc-bb5d-8b10bca133d8-0000"
        },
        "name": "test",
        "resources": [
          {
            "name": "cpus",
            "role": "*",
            "scalar": {
              "value": 2.0
            },
            "type": "SCALAR"
          },
          {
            "name": "mem",
            "role": "*",
            "scalar": {
              "value": 1024.0
            },
            "type": "SCALAR"
          },
          {
            "name": "disk",
            "role": "*",
            "scalar": {
              "value": 1024.0
            },
            "type": "SCALAR"
          },
          {
            "name": "ports",
            "ranges": {
              "range": [
                {
                  "begin": 31000,
                  "end": 32000
                }
              ]
            },
            "role": "*",
            "type": "RANGES"
          }
        ],
        "state": "TASK_RUNNING",
        "status_update_state": "TASK_RUNNING",
        "status_update_uuid": "ycLTRBo8TjKFTrh4vsBERg==",
        "statuses": [
          {
            "node_id": {
              "value": "d4bd102f-e25f-46dc-bb5d-8b10bca133d8-S0"
            },
            "container_status": {
              "network_infos": [
                {
                  "ip_addresses": [
                    {
                      "ip_address": "127.0.1.1"
                    }
                  ]
                }
              ]
            },
            "executor_id": {
              "value": "default"
            },
            "source": "SOURCE_EXECUTOR",
            "state": "TASK_RUNNING",
            "task_id": {
              "value": "1"
            },
            "timestamp": 1470820172.32565,
            "uuid": "ycLTRBo8TjKFTrh4vsBERg=="
          }
        ],
        "task_id": {
          "value": "1"
        }
      }
    ]
  }
}

```

### GET_ROLES

Query the information about roles.

```
GET_ROLES HTTP Request (JSON):

POST /api/v1  HTTP/1.1

Host: controllerhost:5050
Content-Type: application/json
Accept: application/json

{
  "type": "GET_ROLES"
}


GET_ROLES HTTP Response (JSON):

HTTP/1.1 200 OK

Content-Type: application/json

{
  "type": "GET_ROLES",
  "get_roles": {
    "roles": [
      {
        "name": "*",
        "weight": 1.0
      },
      {
        "frameworks": [
          {
            "value": "74bddcbc-4a02-4d64-b291-aed52032055f-0000"
          }
        ],
        "name": "role1",
        "resources": [
          {
            "name": "cpus",
            "role": "role1",
            "scalar": {
              "value": 0.5
            },
            "type": "SCALAR"
          },
          {
            "name": "mem",
            "role": "role1",
            "scalar": {
              "value": 512.0
            },
            "type": "SCALAR"
          },
          {
            "name": "ports",
            "ranges": {
              "range": [
                {
                  "begin": 31000,
                  "end": 31001
                }
              ]
            },
            "role": "role1",
            "type": "RANGES"
          },
          {
            "name": "disk",
            "role": "role1",
            "scalar": {
              "value": 1024.0
            },
            "type": "SCALAR"
          }
        ],
        "weight": 2.5
      }
    ]
  }
}

```

### GET_OPERATIONS

Returns a list of all offer operations throughout the cluster, not including
`LAUNCH` or `LAUNCH_GROUP` operations which can be retrieved with `GET_TASKS`.

```
GET_OPERATIONS HTTP Request (JSON):

POST /api/v1  HTTP/1.1

Host: controllerhost:5050
Content-Type: application/json
Accept: application/json

{
  "type": "GET_OPERATIONS"
}


GET_OPERATIONS HTTP Response (JSON):

HTTP/1.1 200 OK

Content-Type: application/json

{
  "type": "GET_OPERATIONS",
  "get_operations": {
    "operations": [
      {
        "framework_id": {"value": "74bddcbc-4a02-4d64-b291-aed52032055f-0000"},
        "node_id": {"value": "18083noa-j287-dan4-9qx6-l02b84nksb7z-0021"},
        "info": {
          "type": "CREATE_DISK",
          "id": {"value": "n2j8nksj-9827-82bk-nd7u-83hbchu7whdk-9978"},
          "create_disk": {
            "source": {
              "provider_id": {"value": "837hfmi2-u2u7-19pp-1884-812i8f02828j-0030"},
              "name": "disk",
              "type": "SCALAR",
              "scalar": {"value": 1024.0},
              "role": "storage-role-1",
              "allocation_info": {"role": "storage-role-1"},
              "reservation": {
                "type": "DYNAMIC",
                "role": "storage-role-1",
                "principal": "storage-service"
              },
              "reservations": [{
                "type": "DYNAMIC",
                "role": "storage-role-1",
                "principal": "storage-service"
              }],
              "disk": {
                "source": {
                  "type": "RAW",
                  "vendor": "nas-service",
                  "id": "vol-19827509",
                  "profile": "fast-volume"
                }
              }
            },
            "target_type": "MOUNT"
          }
        },
        "latest_status": {
          "operation_id": {"value": "n2j8nksj-9827-82bk-nd7u-83hbchu7whdk-9978"},
          "state": "OPERATION_PENDING",
          "uuid": {"value": "28987843-j288-1k0s-l29n-837ybzmo18tj-nv73"},
          "node_id": {"value": "18083noa-j287-dan4-9qx6-l02b84nksb7z-0021"},
          "resource_provider_id": {"value": "837hfmi2-u2u7-19pp-1884-812i8f02828j-0030"}
        },
        "statuses": [{
          "operation_id": {"value": "n2j8nksj-9827-82bk-nd7u-83hbchu7whdk-9978"},
          "state": "OPERATION_PENDING",
          "uuid": {"value": "28987843-j288-1k0s-l29n-837ybzmo18tj-nv73"},
          "node_id": {"value": "18083noa-j287-dan4-9qx6-l02b84nksb7z-0021"},
          "resource_provider_id": {"value": "837hfmi2-u2u7-19pp-1884-812i8f02828j-0030"}
        }],
        "uuid": {"value": "nsj27802-jd82-jd19-jd38-837jdfnoqfij-u284"}
      }
    ]
  }
}

```

### GET_WEIGHTS

This call retrieves the information about role weights.

```
GET_WEIGHTS HTTP Request (JSON):

POST /api/v1  HTTP/1.1

Host: controllerhost:5050
Content-Type: application/json
Accept: application/json

{
  "type": "GET_WEIGHTS"
}


GET_WEIGHTS HTTP Response (JSON):

HTTP/1.1 200 OK

Content-Type: application/json

{
  "type": "GET_WEIGHTS",
  "get_weights": {
    "weight_infos": [
      {
        "role": "role",
        "weight": 2.0
      }
    ]
  }
}

```

### UPDATE_WEIGHTS

This call updates weights for specific role. This call takes `weight_infos`
which needs `role` value and `weight` value.

```
UPDATE_WEIGHTS HTTP Request (JSON):

POST /api/v1  HTTP/1.1

Host: controllerhost:5050
Content-Type: application/json
Accept: application/json

{
  "type": "UPDATE_WEIGHTS",
  "update_weights": {
    "weight_infos": [
      {
        "role": "role",
        "weight": 4.0
      }
    ]
  }
}


UPDATE_WEIGHTS HTTP Response:

HTTP/1.1 202 Accepted

```

### GET_CONTROLLER

This call retrieves information about the controller.

```
GET_CONTROLLER HTTP Request (JSON):

POST /api/v1  HTTP/1.1

Host: controllerhost:5050
Content-Type: application/json
Accept: application/json

{
  "type": "GET_CONTROLLER"
}


GET_CONTROLLER HTTP Response (JSON):

HTTP/1.1 200 OK

Content-Type: application/json

{
  "type": "GET_CONTROLLER",
  "get_master": {
    "controller_info": {
      "address": {
        "hostname": "myhost",
        "ip": "127.0.1.1",
        "port": 34626
      },
      "hostname": "myhost",
      "id": "310ffdac-0b73-408d-acf0-2adcd21cb4b7",
      "ip": 16842879,
      "pid": "controller@127.0.1.1:34626",
      "port": 34626,
      "version": "1.1.0"
    }
  }
}

```

### RESERVE_RESOURCES

This call is used to update resource reservations.

This call reserves resources dynamically on a specific node. This call takes
`node_id`, the `source` resources of the operation and the target
`resources`, and updates the reservations of `source` to be equal to
the reservations of `target`.

For backwards compatibility we accept calls without the `sources` field present,
but only allow adding a single reservation refinement in that case;
otherwise we expect that `sources` can be transformed into `resources` by
repeatedly removing or adding identical reservations from all given
resources. This means that `source` and `resources` must be identical
in all values apart from reservation.

If authorization is enabled we expect the caller to be authorized to perform any
`UNRESERVE` or `RESERVE` operations needed to transform `sources` to
`resources`.

Example of a call creating reserved resources for role `role`:

```
RESERVE_RESOURCES HTTP Request (JSON):

POST /api/v1  HTTP/1.1

Host: controllerhost:5050
Content-Type: application/json
Accept: application/json

{
  "type": "RESERVE_RESOURCES",
  "reserve_resources": {
    "node_id": {
      "value": "1557de7d-547c-48db-b5d3-6bef9c9640ef-S0"
    },
    "source": [
      {
        "type": "SCALAR",
        "name": "cpus",
        "scalar": {
          "value": 1.0
        }
      },
      {
        "type": "SCALAR",
        "name": "mem",
        "scalar": {
          "value": 512.0
        }
      }
    ],
    "resources": [
      {
        "type": "SCALAR",
        "name": "cpus",
        "scalar": {
          "value": 1.0
        },
        "reservations": [
          {
            "type": "DYNAMIC",
            "role": "role",
            "principal": "my-principal"
          }
        ]
      },
      {
        "type": "SCALAR",
        "name": "mem",
        "scalar": {
          "value": 512.0
        },
        "reservations": [
          {
            "type": "DYNAMIC",
            "role": "role",
            "principal": "my-principal"
          }
        ]
      }
    ]
  }
}


RESERVE_RESOURCES HTTP Response:

HTTP/1.1 202 Accepted

```

Example of a call changing the reservation of resources from `role_a` to
`role_b`.

```
RESERVE_RESOURCES HTTP Request (JSON):

POST /api/v1  HTTP/1.1

Host: controllerhost:5050
Content-Type: application/json
Accept: application/json

{
  "type": "RESERVE_RESOURCES",
  "reserve_resources": {
    "node_id": {
      "value": "1557de7d-547c-48db-b5d3-6bef9c9640ef-S0"
    },
    "source": [
      {
        "type": "SCALAR",
        "name": "cpus",
        "scalar": {
          "value": 1.0
        },
        "reservations": [
          {
            "type": "DYNAMIC",
            "role": "role_a",
            "principal": "my-principal"
          }
        ]
      },
      {
        "type": "SCALAR",
        "name": "mem",
        "scalar": {
          "value": 512.0
        },
        "reservations": [
          {
            "type": "DYNAMIC",
            "role": "role_a",
            "principal": "my-principal"
          }
        ]
      }
    ],
    "resources": [
      {
        "type": "SCALAR",
        "name": "cpus",
        "scalar": {
          "value": 1.0
        },
        "reservations": [
          {
            "type": "DYNAMIC",
            "role": "role_b",
            "principal": "my-principal"
          }
        ]
      },
      {
        "type": "SCALAR",
        "name": "mem",
        "scalar": {
          "value": 512.0
        },
        "reservations": [
          {
            "type": "DYNAMIC",
            "role": "role_b",
            "principal": "my-principal"
          }
        ]
      }
    ]
  }
}


RESERVE_RESOURCES HTTP Response:

HTTP/1.1 202 Accepted

```

### UNRESERVE_RESOURCES

This call unreserve resources dynamically on a specific node. This call takes
`node_id` and `resources` details like the following.

```
UNRESERVE_RESOURCES HTTP Request (JSON):

POST /api/v1  HTTP/1.1

Host: controllerhost:5050
Content-Type: application/json
Accept: application/json

{
  "type": "UNRESERVE_RESOURCES",
  "unreserve_resources": {
    "node_id": {
      "value": "1557de7d-547c-48db-b5d3-6bef9c9640ef-S0"
    },
    "resources": [
      {
        "type": "SCALAR",
        "name": "cpus",
        "scalar": {
          "value": 1.0
        },
        "reservations": [
          {
            "type": "DYNAMIC",
            "role": "role",
            "principal": "my-principal"
          }
        ]
      },
      {
        "type": "SCALAR",
        "name": "mem",
        "scalar": {
          "value": 512.0
        },
        "reservations": [
          {
            "type": "DYNAMIC",
            "role": "role",
            "principal": "my-principal"
          }
        ]
      }
    ]
  }
}


UNRESERVE_RESOURCES HTTP Response:

HTTP/1.1 202 Accepted

```

### CREATE_VOLUMES

This call create [persistent volumes](persistent-volume.md) on reserved
resources. The request is forwarded asynchronously to the Medios node where the
reserved resources are located. That asynchronous message may not be delivered
or creating the volumes at the node might fail. This call takes `node_id`
and `volumes` details like the following.

```
CREATE_VOLUMES HTTP Request (JSON):

POST /api/v1  HTTP/1.1

Host: controllerhost:5050
Content-Type: application/json
Accept: application/json

{
  "type": "CREATE_VOLUMES",
  "create_volumes": {
    "node_id": {
      "value": "919141a8-b434-4946-86b9-e1b65c8171f6-S0"
    },
    "volumes": [
      {
        "type": "SCALAR",
        "disk": {
          "persistence": {
            "id": "id1",
            "principal": "my-principal"
          },
          "volume": {
            "container_path": "path1",
            "mode": "RW"
          }
        },
        "name": "disk",
        "role": "role1",
        "scalar": {
          "value": 64.0
        }
      }
    ]
  }
}


CREATE_VOLUMES HTTP Response:

HTTP/1.1 202 Accepted

```

### DESTROY_VOLUMES

This call destroys [persistent volumes](persistent-volume.md). The request is
forwarded asynchronously to the Medios node where the volumes are located.

```
DESTROY_VOLUMES HTTP Request (JSON):

POST /api/v1  HTTP/1.1

Host: controllerhost:5050
Content-Type: application/json
Accept: application/json

{
  "type": "DESTROY_VOLUMES",
  "destroy_volumes": {
    "node_id": {
      "value": "919141a8-b434-4946-86b9-e1b65c8171f6-S0"
    },
    "volumes": [
      {
        "disk": {
          "persistence": {
            "id": "id1",
            "principal": "my-principal"
          },
          "volume": {
            "container_path": "path1",
            "mode": "RW"
          }
        },
        "name": "disk",
        "role": "role1",
        "scalar": {
          "value": 64.0
        },
        "type": "SCALAR"
      }
    ]
  }
}


DESTROY_VOLUMES HTTP Response:

HTTP/1.1 202 Accepted

```

### GROW_VOLUME

This call grows the size of a [persistent volume](persistent-volume.md). The
request is forwarded asynchronously to the Medios node where the volume is
located.

```
GROW_VOLUME HTTP Request (JSON):

POST /api/v1  HTTP/1.1

Host: controllerhost:5050
Content-Type: application/json
Accept: application/json

{
  "type": "GROW_VOLUME",
  "grow_volume": {
    "node_id": {
      "value": "919141a8-b434-4946-86b9-e1b65c8171f6-S0"
    },
    "volume": {
      "disk": {
        "persistence": {
          "id": "id1",
          "principal": "my-principal"
        },
        "volume": {
          "container_path": "path1",
          "mode": "RW"
        }
      },
      "name": "disk",
      "role": "role1",
      "scalar": {
        "value": 64.0
      },
      "type": "SCALAR"
    },
    "addition": {
      "name": "disk",
      "role": "role1",
      "scalar": {
        "value": 64.0
      },
      "type": "SCALAR"
    }
  }
}


GROW_VOLUME HTTP Response:

HTTP/1.1 202 Accepted

```

### SHRINK_VOLUME

This call shrinks the size of a [persistent volume](persistent-volume.md).
The request is forwarded asynchronously to the Medios node where the volume
is located.

```
SHRINK_VOLUME HTTP Request (JSON):

POST /api/v1  HTTP/1.1

Host: controllerhost:5050
Content-Type: application/json
Accept: application/json

{
  "type": "SHRINK_VOLUME",
  "shrink_volume": {
    "node_id": {
      "value": "919141a8-b434-4946-86b9-e1b65c8171f6-S0"
    },
    "volume": {
      "disk": {
        "persistence": {
          "id": "id1",
          "principal": "my-principal"
        },
        "volume": {
          "container_path": "path1",
          "mode": "RW"
        }
      },
      "name": "disk",
      "role": "role1",
      "scalar": {
        "value": 128.0
      },
      "type": "SCALAR"
    },
    "subtract": {
      "value": 64.0
    }
  }
}


SHRINK_VOLUME HTTP Response:

HTTP/1.1 202 Accepted

```


### GET_MAINTENANCE_STATUS

This call retrieves the cluster's maintenance status.

```
GET_MAINTENANCE_STATUS HTTP Request (JSON):

POST /api/v1  HTTP/1.1

Host: controllerhost:5050
Content-Type: application/json
Accept: application/json

{
  "type": "GET_MAINTENANCE_STATUS"
}


GET_MAINTENANCE_STATUS HTTP Response (JSON):

HTTP/1.1 200 OK

Content-Type: application/json

{
  "type": "GET_MAINTENANCE_STATUS",
  "get_maintenance_status": {
    "status": {
      "draining_machines": [
        {
          "id": {
            "ip": "0.0.0.2"
          }
        },
        {
          "id": {
            "hostname": "myhost"
          }
        }
      ]
    }
  }
}

```

### GET_MAINTENANCE_SCHEDULE

This call retrieves the cluster's maintenance schedule.

```
GET_MAINTENANCE_SCHEDULE HTTP Request (JSON):

POST /api/v1  HTTP/1.1

Host: controllerhost:5050
Content-Type: application/json
Accept: application/json

{
  "type": "GET_MAINTENANCE_SCHEDULE"
}


GET_MAINTENANCE_SCHEDULE HTTP Response (JSON):

HTTP/1.1 200 OK

Content-Type: application/json

{
  "type": "GET_MAINTENANCE_SCHEDULE",
  "get_maintenance_schedule": {
    "schedule": {
      "windows": [
        {
          "machine_ids": [
            {
              "hostname": "myhost"
            },
            {
              "ip": "0.0.0.2"
            }
          ],
          "unavailability": {
            "start": {
              "nanoseconds": 1470849373150643200
            }
          }
        }
      ]
    }
  }
}

```

### UPDATE_MAINTENANCE_SCHEDULE

This call updates the cluster's maintenance schedule.

```
UPDATE_MAINTENANCE_SCHEDULE HTTP Request (JSON):

POST /api/v1  HTTP/1.1

Host: controllerhost:5050
Content-Type: application/json
Accept: application/json

{
  "type": "UPDATE_MAINTENANCE_SCHEDULE",
  "update_maintenance_schedule": {
    "schedule": {
      "windows": [
        {
          "machine_ids": [
            {
              "hostname": "myhost"
            },
            {
              "ip": "0.0.0.2"
            }
          ],
          "unavailability": {
            "start": {
              "nanoseconds": 1470820233192017920
            }
          }
        }
      ]
    }
  }
}


UPDATE_MAINTENANCE_SCHEDULE HTTP Response:

HTTP/1.1 202 Accepted

```

### START_MAINTENANCE

This call starts the maintenance of the cluster, this would bring a set of machines
down.

```
START_MAINTENANCE HTTP Request (JSON):

POST /api/v1  HTTP/1.1

Host: controllerhost:5050
Content-Type: application/json
Accept: application/json

{
  "type": "START_MAINTENANCE",
  "start_maintenance": {
    "machines": [
      {
        "hostname": "myhost",
        "ip": "0.0.0.3"
      }
    ]
  }
}


START_MAINTENANCE HTTP Response:

HTTP/1.1 202 Accepted

```

### STOP_MAINTENANCE

Stops the maintenance of the cluster, this would bring a set of machines
back up.

```
STOP_MAINTENANCE HTTP Request (JSON):

POST /api/v1  HTTP/1.1

Host: controllerhost:5050
Content-Type: application/json
Accept: application/json

{
  "type": "STOP_MAINTENANCE",
  "stop_maintenance": {
    "machines": [
      {
        "hostname": "myhost",
        "ip": "0.0.0.3"
      }
    ]
  }
}


STOP_MAINTENANCE HTTP Response:

HTTP/1.1 202 Accepted

```

### GET_QUOTA

This call retrieves the cluster's configured quotas.

```
GET_QUOTA HTTP Request (JSON):

POST /api/v1  HTTP/1.1

Host: controllerhost:5050
Content-Type: application/json
Accept: application/json

{
  "type": "GET_QUOTA"
}


GET_QUOTA HTTP Response (JSON):

HTTP/1.1 200 OK

Content-Type: application/json

{
  "type": "GET_QUOTA",
  "get_quota": {
    "status": {
      "infos": [
        {
          "configs" : [
            {
              "role": "dev",
              "limits": {
                "cpus": { "value": 2.0 },
                "mem":  { "value": 2048.0 },
                "disk": { "value": 4096.0 }
              }
            }
          ]
        }
      ]
    }
  }
}

```

### UPDATE_QUOTA

This call updates the quota for the specified role(s).
These configurations are applied in an all-or-nothing manner.
To reset a role's quota back to the default (no guarantees and no limits),
simply update its quota with empty guarantees and limits fields.

```
UPDATE_QUOTA HTTP Request (JSON):

POST /api/v1  HTTP/1.1

Host: controllerhost:5050
Content-Type: application/json
Accept: application/json

{
  "type": "UPDATE_QUOTA",
  "update_quota": {
    "force": false,
    "quota_configs": [
      {
        "role": "dev",
        "limits": {
          "cpus": { "value": 10 },
          "mem":  { "value": 2048 },
          "disk": { "value": 4096 }
        }
      },
      {
        "role": "test",
        "limits": {
          "cpus": { "value": 1 },
          "mem":  { "value": 256 },
          "disk": { "value": 512 }
        }
      }
    ]
  }
}

UPDATE_QUOTA HTTP Response:

HTTP/1.1 200 OK

```

### MARK_NODE_GONE

This call can be used by operators to assert that a node instance has
failed and is never coming back (e.g., ephemeral instance from cloud provider).
The controller would shutdown the node and send `TASK_GONE_BY_OPERATOR` updates
for all the running tasks. This signal can be used by stateful frameworks to
re-schedule their workloads (volumes, reservations etc.) to other node
instances. It is possible that the tasks might still be running if the
operator's assertion was wrong and the node was partitioned away from
the controller. The node would be shutdown when it tries to reregister with the
controller when the partition heals. This call is idempotent.

```
MARK_NODE_GONE HTTP Request (JSON):

POST /api/v1  HTTP/1.1

Host: controllerhost:5050
Content-Type: application/json
Accept: application/json

{
  "type": "MARK_NODE_GONE",
  "mark_node_gone": {
    "node_id": {
      "value": "3192b9d1-db71-4699-ae25-e28dfbf42de1"
    }
  }
}

MARK_NODE_GONE HTTP Response (JSON):

HTTP/1.1 200 OK
```

### DRAIN_NODE

Initiates [draining](maintenance.md) on the specified node.

```
DRAIN_NODE HTTP Request (JSON):

POST /api/v1  HTTP/1.1

Host: controllerhost:5050
Content-Type: application/json
Accept: application/json

{
  "type": "DRAIN_NODE",
  "drain_node": {
    "node_id": {
      "value": "3192b9d1-db71-4699-ae25-e28dfbf42de1"
    },
    "max_grace_period": {"seconds": 600},
    "mark_gone": false
  }
}

DRAIN_NODE HTTP Response (JSON):

HTTP/1.1 200 OK
```

### DEACTIVATE_NODE

Deactivates the specified node, preventing offers for that node's resources
from being sent to schedulers until the node is reactivated.

```
DEACTIVATE_NODE HTTP Request (JSON):

POST /api/v1  HTTP/1.1

Host: controllerhost:5050
Content-Type: application/json
Accept: application/json

{
  "type": "DEACTIVATE_NODE",
  "deactivate_node": {
    "node_id": {
      "value": "3192b9d1-db71-4699-ae25-e28dfbf42de1"
    }
  }
}

DEACTIVATE_NODE HTTP Response (JSON):

HTTP/1.1 200 OK
```

### REACTIVATE_NODE

Reactivates the specified node, resuming offers for that node's resources if
the node was previously deactivated.

```
REACTIVATE_NODE HTTP Request (JSON):

POST /api/v1  HTTP/1.1

Host: controllerhost:5050
Content-Type: application/json
Accept: application/json

{
  "type": "REACTIVATE_NODE",
  "reactivate_node": {
    "node_id": {
      "value": "3192b9d1-db71-4699-ae25-e28dfbf42de1"
    }
  }
}

REACTIVATE_NODE HTTP Response (JSON):

HTTP/1.1 200 OK
```

## Events

Currently, the only call that results in a streaming response is the `SUBSCRIBE` call sent to the controller API.

```
SUBSCRIBE Request (JSON):

POST /api/v1  HTTP/1.1

Host: controllerhost:5050
Content-Type: application/json
Accept: application/json

{
  "type": "SUBSCRIBE"
}

SUBSCRIBE Response Event (JSON):
HTTP/1.1 200 OK

Content-Type: application/json
Transfer-Encoding: chunked

<event-length>
{
  "type": "SUBSCRIBED",

  "subscribed" : {
    "get_state" : {...}
  }
}
<more events>
```

The client is expected to keep a **persistent** connection open to the endpoint even after getting a `SUBSCRIBED` HTTP Response event. This is indicated by "Connection: keep-alive" and "Transfer-Encoding: chunked" headers with *no* "Content-Length" header set. All subsequent events generated by Medios are streamed on this connection. The controller encodes each Event in [RecordIO](recordio.md) format, i.e., string representation of length of the event in bytes followed by JSON or binary Protobuf encoded event.

The following events are currently sent by the controller. The canonical source of this information is at [controller.proto](../include/medios/v1/controller/controller.proto). Note that when sending JSON encoded events, controller encodes raw bytes in Base64 and strings in UTF-8.

### SUBSCRIBED

The first event sent by the controller when a client sends a `SUBSCRIBE` request on the persistent connection. This includes a snapshot of the cluster state. See `SUBSCRIBE` above for details. Subsequent changes to the cluster state can result in more events (currently only `TASK_ADDED` and `TASK_UPDATED` are supported).

### HEARTBEAT

Periodically sent by the controller to the subscriber according to 'Subscribed.heartbeat_interval_seconds'. If the subscriber does not receive any events (including heartbeats) for an extended period of time (e.g., 5 x heartbeat_interval_seconds), it is likely that the connection is lost or there is a network partition. In that case, the subscriber should close the existing subscription connection and resubscribe using a backoff strategy.

```
HEARTBEAT Event (JSON)

<event-length>
{
  "type": "HEARTBEAT",
}
```

### TASK_ADDED

Sent whenever a task has been added to the controller. This can happen either when a new task launch is processed by the controller or when a node reregisters with a failed over controller.

```
TASK_ADDED Event (JSON)

<event-length>
{
  "type": "TASK_ADDED",

  "task_added": {
    "task": {
      "name": "dummy-task",
      "task_id": {
        "value": "d40f3f3e-bbe3-44af-a230-4cb1eae72f67"
      },
      "node_id": {
        "value": "f1c9cdc5-195e-41a7-a0d7-adaa9af07f81"
      },
      "command": {
        "value": "sleep",
        "arguments": [
          "100"
        ]
      }
    }
  }
}
```

### TASK_UPDATED

Sent whenever the state of the task changes in the controller. This can happen when a status update is received or generated by the controller. Since status updates are retried by the node, not all status updates received by the controller result in the event being sent.

```
TASK_UPDATED Event (JSON)

<event-length>
{
  "type": "TASK_UPDATED",

  "task_updated": {
    "task_id": {
        "value": "42154f1b-adcd-4421-bf13-8bd11adfafaf"
    },

    "framework_id": {
        "value": "49154f1b-8cf6-4421-bf13-8bd11dccd1f1"
    },

    "node_id": {
        "value": "2915adf-8aff-4421-bf13-afdafaf1f1"
    },

    "executor_id": {
        "value": "adfaf-adff-2421-bf13-adf23tafa21"
    },

    "state" : "TASK_RUNNING"
  }
}
```

### FRAMEWORK_ADDED

Sent whenever a framework becomes known to the controller. This can happen when a new framework registers with the controller.

```
FRAMEWORK_ADDED Event (JSON)

<event-length>
{
  "type": "FRAMEWORK_ADDED",

  "framework_added": {
    "framework": {
      "active": true,
      "allocated_resources": [],
      "connected": true,
      "framework_info": {
        "capabilities": [
          {
            "type": "RESERVATION_REFINEMENT"
          }
        ],
        "checkpoint": true,
        "failover_timeout": 0,
        "id": {
          "value": "a9ba2984-99c4-4183-8cd1-f7313426e21c-0147"
        },
        "name": "inverse-offer-example-framework",
        "role": "*",
        "user": "root"
      },
      "inverse_offers": [],
      "recovered": false,
      "registered_time": {
        "nanoseconds": 1501191957829317120
      },
      "reregistered_time": {
        "nanoseconds": 1501191957829317120
      }
    }
  }
}
```

### FRAMEWORK_UPDATED

Sent whenever a framework reregisters with the controller upon a disconnection (network error) or upon a controller failover.

```
FRAMEWORK_UPDATED Event (JSON)

<event-length>
{
  "type": "FRAMEWORK_UPDATED",

  "framework_updated": {
    "framework": {
      "active": true,
      "allocated_resources": [],
      "connected": true,
      "framework_info": {
        "capabilities": [
          {
            "type": "RESERVATION_REFINEMENT"
          }
        ],
        "checkpoint": true,
        "failover_timeout": 0,
        "id": {
          "value": "a9ba2984-99c4-4183-8cd1-f7313426e21c-0147"
        },
        "name": "inverse-offer-example-framework",
        "role": "*",
        "user": "root"
      },
      "inverse_offers": [],
      "recovered": false,
      "registered_time": {
        "nanoseconds": 1501191957829317120
      },
      "reregistered_time": {
        "nanoseconds": 1501191957829317120
      }
    }
  }
}
```

### FRAMEWORK_REMOVED

Sent whenever a framework is removed. This can happen when a framework is explicitly teardown by the operator or if it fails to reregister with the controller within the failover timeout.

```
FRAMEWORK_REMOVED Event (JSON)

<event-length>
{
  "type": "FRAMEWORK_REMOVED",

  "framework_removed": {
    "framework_info": {
      "capabilities": [
        {
          "type": "RESERVATION_REFINEMENT"
        }
      ],
      "checkpoint": true,
      "failover_timeout": 0,
      "id": {
        "value": "a9ba2984-99c4-4183-8cd1-f7313426e21c-0147"
      },
      "name": "inverse-offer-example-framework",
      "role": "*",
      "user": "root"
    }
  }
}
```

### NODE_ADDED

Sent whenever a node becomes known to it. This can happen when a node registered for the first time, or reregistered after a controller failover.

```
NODE_ADDED Event (JSON)

<event-length>
{
  "type": "NODE_ADDED",

  "node_added": {
    "node": {
      "active": true,
      "node_info": {
        "hostname": "172.31.2.24",
        "id": {
          "value": "c3946a13-75b4-4d3c-9d0e-fc10038dca85-S3"
        },
        "port": 5051,
        "resources": [],
      },
      "allocated_resources": [],
      "capabilities": [
        {
          "type": "MULTI_ROLE"
        },
        {
          "type": "HIERARCHICAL_ROLE"
        },
        {
          "type": "RESERVATION_REFINEMENT"
        }
      ],
      "offered_resources": [],
      "pid": "node(1)@172.31.2.24:5051",
      "registered_time": {
        "nanoseconds": 1500993262264135000
      },
      "reregistered_time": {
        "nanoseconds": 1500993263019321000
      },
      "total_resources": [],
      "version": "1.4.0"
    }
  }
}
```

### NODE_REMOVED

Sent whenever a node is removed. This can happen when the node is scheduled for maintenance. (NOTE: It's possible that a node might become active once it has been removed, i.e. if the controller has gc'ed its list of known "dead" nodes. See MESOS-5965 for context).

```
NODE_REMOVED Event (JSON)

<event-length>
{
  "type": "NODE_REMOVED",

  "node_removed": {
    "node_id": {
      "value": "c3946a13-75b4-4d3c-9d0e-fc10038dca85-S3"
    }
  }
}
```

## Node API

This API contains all the calls accepted by the node. The canonical source of this information is [node.proto](../include/medios/v1/node/node.proto) (NOTE: The protobuf definitions are subject to change before the beta API is finalized). These calls are typically made by human operators, tooling or services (e.g., Medios WebUI). While executors can make these calls as well, it is expected for those to use the [Executor HTTP API](executor-http-api.md).

### Calls To Node And Responses

Below are the example calls to node that result in synchronous responses from the API.

### GET_HEALTH

Request and Response are similar to GET_HEALTH call to the controller.

### GET_FLAGS

This call retrieves the node's flag configuration.

```
GET_FLAGS HTTP Request (JSON):

POST /api/v1  HTTP/1.1

Host: nodehost:5051
Content-Type: application/json
Accept: application/json

{
  "type": "GET_FLAGS"
}


GET_FLAGS HTTP Response (JSON):

HTTP/1.1 200 OK

Content-Type: application/json

{
  "type": "GET_FLAGS",
  "get_flags": {
    "flags": [
      {
        "name": "acls",
        "value": ""
      },
      {
        "name": "appc_simple_discovery_uri_prefix",
        "value": "http://"
      },
      {
        "name": "appc_store_dir",
        "value": "/tmp/medios/store/appc"
      },
      {
        "name": "authenticate_http_readonly",
        "value": "true"
      },
      {
        "name": "authenticate_http_readwrite",
        "value": "true"
      },
      {
        "name": "authenticatee",
        "value": "identity_assertion"
      },
      {
        "name": "authentication_backoff_factor",
        "value": "1secs"
      },
      {
        "name": "authorizer",
        "value": "local"
      },
      {
        "name": "cgroups_cpu_enable_pids_and_tids_count",
        "value": "false"
      },
      {
        "name": "cgroups_enable_cfs",
        "value": "false"
      },
      {
        "name": "cgroups_hierarchy",
        "value": "/sys/fs/cgroup"
      },
      {
        "name": "cgroups_limit_swap",
        "value": "false"
      },
      {
        "name": "cgroups_root",
        "value": "medios"
      },
      {
        "name": "container_disk_watch_interval",
        "value": "15secs"
      },
      {
        "name": "containerizers",
        "value": "medios"
      },
      {
        "name": "credential",
        "value": "/tmp/directory/credential"
      },
      {
        "name": "default_role",
        "value": "*"
      },
      {
        "name": "disk_watch_interval",
        "value": "1mins"
      },
      {
        "name": "docker",
        "value": "docker"
      },
      {
        "name": "docker_kill_orphans",
        "value": "true"
      },
      {
        "name": "docker_registry",
        "value": "https://registry-1.docker.io"
      },
      {
        "name": "docker_remove_delay",
        "value": "6hrs"
      },
      {
        "name": "docker_socket",
        "value": "/var/run/docker.sock"
      },
      {
        "name": "docker_stop_timeout",
        "value": "0ns"
      },
      {
        "name": "docker_store_dir",
        "value": "/tmp/medios/store/docker"
      },
      {
        "name": "docker_volume_checkpoint_dir",
        "value": "/var/run/medios/isolators/docker/volume"
      },
      {
        "name": "enforce_container_disk_quota",
        "value": "false"
      },
      {
        "name": "executor_registration_timeout",
        "value": "1mins"
      },
      {
        "name": "executor_shutdown_grace_period",
        "value": "5secs"
      },
      {
        "name": "gc_delay",
        "value": "1weeks"
      },
      {
        "name": "gc_disk_headroom",
        "value": "0.1"
      },
      {
        "name": "gc_non_executor_container_sandboxes",
        "value": "false"
      },
      {
        "name": "help",
        "value": "false"
      },
      {
        "name": "hostname_lookup",
        "value": "true"
      },
      {
        "name": "http_authenticators",
        "value": "basic"
      },
      {
        "name": "http_command_executor",
        "value": "false"
      },
      {
        "name": "http_credentials",
        "value": "/tmp/directory/http_credentials"
      },
      {
        "name": "image_provisioner_backend",
        "value": "copy"
      },
      {
        "name": "initialize_driver_logging",
        "value": "true"
      },
      {
        "name": "isolation",
        "value": "posix/cpu,posix/mem"
      },
      {
        "name": "launcher_dir",
        "value": "/my-directory"
      },
      {
        "name": "logbufsecs",
        "value": "0"
      },
      {
        "name": "logging_level",
        "value": "INFO"
      },
      {
        "name": "oversubscribed_resources_interval",
        "value": "15secs"
      },
      {
        "name": "perf_duration",
        "value": "10secs"
      },
      {
        "name": "perf_interval",
        "value": "1mins"
      },
      {
        "name": "qos_correction_interval_min",
        "value": "0ns"
      },
      {
        "name": "quiet",
        "value": "false"
      },
      {
        "name": "recover",
        "value": "reconnect"
      },
      {
        "name": "recovery_timeout",
        "value": "15mins"
      },
      {
        "name": "registration_backoff_factor",
        "value": "10ms"
      },
      {
        "name": "resources",
        "value": "cpus:2;gpus:0;mem:1024;disk:1024;ports:[31000-32000]"
      },
      {
        "name": "revocable_cpu_low_priority",
        "value": "true"
      },
      {
        "name": "sandbox_directory",
        "value": "/mnt/medios/sandbox"
      },
      {
        "name": "strict",
        "value": "true"
      },
      {
        "name": "switch_user",
        "value": "true"
      },
      {
        "name": "systemd_enable_support",
        "value": "true"
      },
      {
        "name": "systemd_runtime_directory",
        "value": "/run/systemd/system"
      },
      {
        "name": "version",
        "value": "false"
      },
      {
        "name": "work_dir",
        "value": "/tmp/directory"
      }
    ]
  }
}

```

### GET_VERSION

Request and Response are similar to GET_VERSION call to the controller.

### GET_METRICS

Request and Response are similar to GET_METRICS call to the controller.

### GET_LOGGING_LEVEL

Request and Response are similar to GET_LOGGING_LEVEL call to the controller.

### SET_LOGGING_LEVEL

Request and Response are similar to SET_LOGGING_LEVEL call to the controller.

### LIST_FILES

Request and Response are similar to LIST_FILES call to the controller.

### READ_FILE

Request and Response are similar to READ_FILE call to the controller.

### GET_STATE

This call retrieves full state of the node i.e. information about the tasks,
frameworks and executors running in the cluster.

```
GET_STATE HTTP Request (JSON):

POST /api/v1  HTTP/1.1

Host: nodehost:5051
Content-Type: application/json
Accept: application/json

{
  "type": "GET_STATE"
}


GET_STATE HTTP Response (JSON):

HTTP/1.1 200 OK

Content-Type: application/json

{
  "type": "GET_STATE",
  "get_state": {
    "get_executors": {
      "executors": [
        {
          "executor_info": {
            "command": {
              "arguments": [
                "medios-executor",
                "--launcher_dir=/my-directory"
              ],
              "shell": false,
              "value": "my-directory"
            },
            "executor_id": {
              "value": "1"
            },
            "framework_id": {
              "value": "8903b84e-112f-4b5f-aad3-7366f6ae7ecc-0000"
            },
            "name": "Command Executor (Task: 1) (Command: sh -c 'sleep 1000')",
            "resources": [
              {
                "name": "cpus",
                "role": "*",
                "scalar": {
                  "value": 0.1
                },
                "type": "SCALAR"
              },
              {
                "name": "mem",
                "role": "*",
                "scalar": {
                  "value": 32.0
                },
                "type": "SCALAR"
              }
            ],
            "source": "1"
          }
        }
      ]
    },
    "get_frameworks": {
      "frameworks": [
        {
          "framework_info": {
            "checkpoint": false,
            "failover_timeout": 0.0,
            "hostname": "myhost",
            "id": {
              "value": "8903b84e-112f-4b5f-aad3-7366f6ae7ecc-0000"
            },
            "name": "default",
            "principal": "my-principal",
            "role": "*",
            "user": "root"
          }
        }
      ]
    },
    "get_tasks": {
      "launched_tasks": [
        {
          "node_id": {
            "value": "8903b84e-112f-4b5f-aad3-7366f6ae7ecc-S0"
          },
          "framework_id": {
            "value": "8903b84e-112f-4b5f-aad3-7366f6ae7ecc-0000"
          },
          "name": "",
          "resources": [
            {
              "name": "cpus",
              "role": "*",
              "scalar": {
                "value": 2.0
              },
              "type": "SCALAR"
            },
            {
              "name": "mem",
              "role": "*",
              "scalar": {
                "value": 1024.0
              },
              "type": "SCALAR"
            },
            {
              "name": "disk",
              "role": "*",
              "scalar": {
                "value": 1024.0
              },
              "type": "SCALAR"
            },
            {
              "name": "ports",
              "ranges": {
                "range": [
                  {
                    "begin": 31000,
                    "end": 32000
                  }
                ]
              },
              "role": "*",
              "type": "RANGES"
            }
          ],
          "state": "TASK_RUNNING",
          "status_update_state": "TASK_RUNNING",
          "status_update_uuid": "2qlPayEJRJGPeaWlahI+WA==",
          "statuses": [
            {
              "node_id": {
                "value": "8903b84e-112f-4b5f-aad3-7366f6ae7ecc-S0"
              },
              "container_status": {
                "executor_pid": 19846,
                "network_infos": [
                  {
                    "ip_addresses": [
                      {
                        "ip_address": "127.0.1.1"
                      }
                    ]
                  }
                ]
              },
              "executor_id": {
                "value": "1"
              },
              "source": "SOURCE_EXECUTOR",
              "state": "TASK_RUNNING",
              "task_id": {
                "value": "1"
              },
              "timestamp": 1470898839.48066,
              "uuid": "2qlPayEJRJGPeaWlahI+WA=="
            }
          ],
          "task_id": {
            "value": "1"
          }
        }
      ]
    }
  }
}

```

### GET_CONTAINERS

This call retrieves information about containers running on this node. It contains
ContainerStatus and ResourceStatistics along with some metadata of the containers.

There are two knobs in the request to control the types of the containers this
API will return:
* `show_nested`: Whether to show nested containers [default: false].
* `show_standalone`: Whether to show standalone containers [default: false].

```
GET_CONTAINERS HTTP Request (JSON):

POST /api/v1  HTTP/1.1

Host: nodehost:5051
Content-Type: application/json
Accept: application/json

{
  "type": "GET_CONTAINERS",
  "get_containers": {
    "show_nested": true,
    "show_standalone": false
  }
}


GET_CONTAINERS HTTP Response (JSON):

HTTP/1.1 200 OK

Content-Type: application/json

{
  "type": "GET_CONTAINERS",
  "get_containers": {
    "containers": [
      {
        "container_id": {
          "value": "f0f97041-1860-4b4a-b279-91fec4e0ebd8"
        },
        "container_status": {
          "network_infos": [
            {
              "ip_addresses": [
                {
                  "ip_address": "192.168.1.20"
                }
              ]
            }
          ]
        },
        "executor_id": {
          "value": "default"
        },
        "executor_name": "",
        "framework_id": {
          "value": "cbe3c0f1-5655-4110-bc01-ae658a9dbab9-0000"
        },
        "resource_statistics": {
          "mem_limit_bytes": 2048,
          "timestamp": 0.0
        }
      }
    ]
  }
}

```

### GET_FRAMEWORKS

This call retrieves information about all the frameworks known to the node.

```
GET_FRAMEWORKS HTTP Request (JSON):

POST /api/v1  HTTP/1.1

Host: nodehost:5051
Content-Type: application/json
Accept: application/json

{
  "type": "GET_FRAMEWORKS"
}


GET_FRAMEWORKS HTTP Response (JSON):

HTTP/1.1 200 OK

Content-Type: application/json

{
  "type": "GET_FRAMEWORKS",
  "get_frameworks": {
    "frameworks": [
      {
        "framework_info": {
          "checkpoint": false,
          "failover_timeout": 0.0,
          "hostname": "myhost",
          "id": {
            "value": "17e8c0d4-5ee2-4937-bc1c-06c39eddb004-0000"
          },
          "name": "default",
          "principal": "my-principal",
          "role": "*",
          "user": "root"
        }
      }
    ]
  }
}

```

### GET_EXECUTORS

This call retrieves information about all the executors known to the node.

```
GET_EXECUTORS HTTP Request (JSON):

POST /api/v1  HTTP/1.1

Host: nodehost:5051
Content-Type: application/json
Accept: application/json

{
  "type": "GET_EXECUTORS"
}


GET_EXECUTORS HTTP Response (JSON):

HTTP/1.1 200 OK

Content-Type: application/json

{
  "type": "GET_EXECUTORS",
  "get_executors": {
    "executors": [
      {
        "executor_info": {
          "command": {
            "arguments": [
              "medios-executor",
              "--launcher_dir=/my-directory"
            ],
            "shell": false,
            "value": "/my-directory"
          },
          "executor_id": {
            "value": "1"
          },
          "framework_id": {
            "value": "5ffcfa79-00c4-4d93-94a3-2f3844126fd9-0000"
          },
          "name": "Command Executor (Task: 1) (Command: sh -c 'sleep 1000')",
          "resources": [
            {
              "name": "cpus",
              "role": "*",
              "scalar": {
                "value": 0.1
              },
              "type": "SCALAR"
            },
            {
              "name": "mem",
              "role": "*",
              "scalar": {
                "value": 32.0
              },
              "type": "SCALAR"
            }
          ],
          "source": "1"
        }
      }
    ]
  }
}

```

### GET_TASKS

This call retrieves information about all the tasks known to the node.

```
GET_TASKS HTTP Request (JSON):

POST /api/v1  HTTP/1.1

Host: nodehost:5051
Content-Type: application/json
Accept: application/json

{
  "type": "GET_TASKS"
}


GET_TASKS HTTP Response (JSON):

HTTP/1.1 200 OK

Content-Type: application/json

{
  "type": "GET_TASKS",
  "get_tasks": {
    "launched_tasks": [
      {
        "node_id": {
          "value": "70770d61-d666-4547-a808-787f63b00cf2-S0"
        },
        "framework_id": {
          "value": "70770d61-d666-4547-a808-787f63b00cf2-0000"
        },
        "name": "",
        "resources": [
          {
            "name": "cpus",
            "role": "*",
            "scalar": {
              "value": 2.0
            },
            "type": "SCALAR"
          },
          {
            "name": "mem",
            "role": "*",
            "scalar": {
              "value": 1024.0
            },
            "type": "SCALAR"
          },
          {
            "name": "disk",
            "role": "*",
            "scalar": {
              "value": 1024.0
            },
            "type": "SCALAR"
          },
          {
            "name": "ports",
            "ranges": {
              "range": [
                {
                  "begin": 31000,
                  "end": 32000
                }
              ]
            },
            "role": "*",
            "type": "RANGES"
          }
        ],
        "state": "TASK_RUNNING",
        "status_update_state": "TASK_RUNNING",
        "status_update_uuid": "0RC72iyRTQefoUL0ClcL0g==",
        "statuses": [
          {
            "node_id": {
              "value": "70770d61-d666-4547-a808-787f63b00cf2-S0"
            },
            "container_status": {
              "executor_pid": 27140,
              "network_infos": [
                {
                  "ip_addresses": [
                    {
                      "ip_address": "127.0.1.1"
                    }
                  ]
                }
              ]
            },
            "executor_id": {
              "value": "1"
            },
            "source": "SOURCE_EXECUTOR",
            "state": "TASK_RUNNING",
            "task_id": {
              "value": "1"
            },
            "timestamp": 1470900791.21577,
            "uuid": "0RC72iyRTQefoUL0ClcL0g=="
          }
        ],
        "task_id": {
          "value": "1"
        }
      }
    ]
  }
}

```

### GET_OPERATIONS

Returns a list of all offer operations known to the node, not including
`LAUNCH` or `LAUNCH_GROUP` operations which can be retrieved with `GET_TASKS`.

```
GET_OPERATIONS HTTP Request (JSON):

POST /api/v1  HTTP/1.1

Host: nodehost:5051
Content-Type: application/json
Accept: application/json

{
  "type": "GET_OPERATIONS"
}


GET_OPERATIONS HTTP Response (JSON):

HTTP/1.1 200 OK

Content-Type: application/json

{
  "type": "GET_OPERATIONS",
  "get_operations": {
    "operations": [
      {
        "framework_id": {"value": "74bddcbc-4a02-4d64-b291-aed52032055f-0000"},
        "node_id": {"value": "18083noa-j287-dan4-9qx6-l02b84nksb7z-0021"},
        "info": {
          "type": "CREATE_DISK",
          "id": {"value": "n2j8nksj-9827-82bk-nd7u-83hbchu7whdk-9978"},
          "create_disk": {
            "source": {
              "provider_id": {"value": "837hfmi2-u2u7-19pp-1884-812i8f02828j-0030"},
              "name": "disk",
              "type": "SCALAR",
              "scalar": {"value": 1024.0},
              "role": "storage-role-1",
              "allocation_info": {"role": "storage-role-1"},
              "reservation": {
                "type": "DYNAMIC",
                "role": "storage-role-1",
                "principal": "storage-service"
              },
              "reservations": [{
                "type": "DYNAMIC",
                "role": "storage-role-1",
                "principal": "storage-service"
              }],
              "disk": {
                "source": {
                  "type": "RAW",
                  "vendor": "nas-service",
                  "id": "vol-19827509",
                  "profile": "fast-volume"
                }
              }
            },
            "target_type": "MOUNT"
          }
        },
        "latest_status": {
          "operation_id": {"value": "n2j8nksj-9827-82bk-nd7u-83hbchu7whdk-9978"},
          "state": "OPERATION_PENDING",
          "uuid": {"value": "28987843-j288-1k0s-l29n-837ybzmo18tj-nv73"},
          "node_id": {"value": "18083noa-j287-dan4-9qx6-l02b84nksb7z-0021"},
          "resource_provider_id": {"value": "837hfmi2-u2u7-19pp-1884-812i8f02828j-0030"}
        },
        "statuses": [{
          "operation_id": {"value": "n2j8nksj-9827-82bk-nd7u-83hbchu7whdk-9978"},
          "state": "OPERATION_PENDING",
          "uuid": {"value": "28987843-j288-1k0s-l29n-837ybzmo18tj-nv73"},
          "node_id": {"value": "18083noa-j287-dan4-9qx6-l02b84nksb7z-0021"},
          "resource_provider_id": {"value": "837hfmi2-u2u7-19pp-1884-812i8f02828j-0030"}
        }],
        "uuid": {"value": "nsj27802-jd82-jd19-jd38-837jdfnoqfij-u284"}
      }
    ]
  }
}

```

### LAUNCH_NESTED_CONTAINER

This call launches a nested container. Any authorized entity,
including the executor itself, its tasks, or the operator can use this
API to launch a nested container.

**NOTE**: Successful invocation of this API will result in some metadata
tracked by the node and the creation of a container sandbox under the
parent container. The REMOVE_NESTED_CONTAINER should be used to remove
the metadata and sandbox. If the `--gc_non_executor_container_sandboxes`
node flag is enabled, the node will garbage collect the sandboxes of
any containers launched via this API.

```
LAUNCH_NESTED_CONTAINER HTTP Request (JSON):

POST /api/v1  HTTP/1.1

Host: nodehost:5051
Content-Type: application/json
Accept: application/json

{
  "type": "LAUNCH_NESTED_CONTAINER",
  "launch_nested_container": {
    "container_id": {
      "parent": {
        "parent": {
          "value": "27d44d12-ce9e-455f-9282-f580d8b56cad"
        },
        "value": "f5015d94-8093-477d-9551-9452acfad495"
      },
      "value": "3192b9d1-db71-4699-ae25-e28dfbf42de1"
    },
    "command": {
      "environment": {
        "variables": [
          {
            "name": "ENV_VAR_KEY",
            "type": "VALUE",
            "value": "env_var_value"
          }
        ]
      },
      "shell": true,
      "value": "exit 0"
    }
  }
}

LAUNCH_NESTED_CONTAINER HTTP Response (JSON):

HTTP/1.1 200 OK

```

### WAIT_NESTED_CONTAINER

This call waits for a nested container to terminate or exit. Any
authorized entity, including the executor itself, its tasks, or the
operator can use this API to wait on a nested container.

```
WAIT_NESTED_CONTAINER HTTP Request (JSON):

POST /api/v1  HTTP/1.1

Host: nodehost:5051
Content-Type: application/json
Accept: application/json

{
  "type": "WAIT_NESTED_CONTAINER",
  "wait_nested_container": {
    "container_id": {
      "parent": {
        "value": "6643b4be-583a-4dc3-bf23-a1ffb26dd452"
      },
      "value": "3192b9d1-db71-4699-ae25-e28dfbf42de1"
    }
  }
}

WAIT_NESTED_CONTAINER HTTP Response (JSON):

HTTP/1.1 200 OK

Content-Type: application/json

{
  "type": "WAIT_NESTED_CONTAINER",
  "wait_nested_container": {
    "exit_status": 0
  }
}

```

### KILL_NESTED_CONTAINER

This call initiates the destruction of a nested container. Any
authorized entity, including the executor itself, its tasks, or the
operator can use this API to kill a nested container.

```
KILL_NESTED_CONTAINER HTTP Request (JSON):

POST /api/v1  HTTP/1.1

Host: nodehost:5051
Content-Type: application/json
Accept: application/json

{
  "type": "KILL_NESTED_CONTAINER",
  "kill_nested_container": {
    "container_id": {
      "parent": {
        "value": "62d15977-acd4-4167-ae08-2e3738dc3ad6"
      },
      "value": "3192b9d1-db71-4699-ae25-e28dfbf42de1"
    }
  }
}

KILL_NESTED_CONTAINER HTTP Response (JSON):

HTTP/1.1 200 OK

```

### LAUNCH_NESTED_CONTAINER_SESSION

This call launches a nested container whose lifetime is tied to the
lifetime of the HTTP call establishing this connection. The STDOUT and
STDERR of the nested container is streamed back to the client so long
as the connection is active.

```
LAUNCH_NESTED_CONTAINER_SESSION HTTP Request (JSON):

POST /api/v1  HTTP/1.1

Host: nodehost:5051
Content-Type: application/json
Accept: application/recordio
Message-Accept: application/json

{
  "type": "LAUNCH_NESTED_CONTAINER_SESSION",
  "launch_nested_container_session": {
    "container_id": {
      "parent": {
        "parent": {
          "value": "bde04877-cb26-4277-976e-3ecf0c02e76b"
        },
        "value": "134bae93-cf5c-4938-87bf-f779bfcd0092"
      },
      "value": "e193a755-8528-4673-a05b-2cc2a01a8b94"
    },
    "command": {
      "environment": {
        "variables": [
          {
            "name": "ENV_VAR_KEY",
            "type": "VALUE",
            "value": "env_var_value"
          }
        ]
      },
      "shell": true,
      "value": "while [ true ]; do echo $(date); sleep 1; done"
    }
  }
}

LAUNCH_NESTED_CONTAINER_SESSION HTTP Response (JSON):

HTTP/1.1 200 OK

Content-Type: application/recordio
Message-Content-Type: application/json

90
{
  "type":"DATA",
  "data": {
    "type":"STDOUT",
    "data": "TW9uIEZlYiAyNyAwNzozOTozOCBVVEMgMjAxNwo="
  }
}90
{
  "type":"DATA",
  "data": {
    "type":"STDOUT",
    "data": "TW9uIEZlYiAyNyAwNzozOTozOSBVVEMgMjAxNwo="
  }
}90
{
  "type":"DATA",
  "data": {
    "type":"STDERR",
    "data": "TW9uIEZlYiAyNyAwNzozOTo0MCBVVEMgMjAxNwo="
  }
}
...

```

### ATTACH_CONTAINER_INPUT

This call attaches to the STDIN of the primary process of a container
and streams input to it. This call can only be made against containers
that have been launched with an associated IOSwitchboard (i.e. nested
containers launched via a LAUNCH_NESTED_CONTAINER_SESSION call or
normal containers launched with a TTYInfo in their ContainerInfo).
Only one ATTACH_CONTAINER_INPUT call can be active for a given
container at a time. Subsequent attempts to attach will fail.

The first message sent over an ATTACH_CONTAINER_INPUT stream must be
of type CONTAINER_ID and contain the ContainerID of the container
being attached to. Subsequent messages must be of type PROCESS_IO, but
they may contain subtypes of either DATA or CONTROL. DATA messages
must be of type STDIN and contain the actual data to stream to the
STDIN of the container being attached to. Currently, the only valid
CONTROL message sends a heartbeat to keep the connection alive. We may
add more CONTROL messages in the future. An empty DATA message of type
STDIN indicates EOF. If the container was launched with TTYInfo in their
ContainerInfo, an EOT DATA message is expected instead.

```
ATTACH_CONTAINER_INPUT HTTP Request (JSON):

POST /api/v1  HTTP/1.1

Host: nodehost:5051
Content-Type: application/recordio
Message-Content-Type: application/json
Accept: application/json

214
{
  "type": "ATTACH_CONTAINER_INPUT",
  "attach_container_input": {
    "type": "CONTAINER_ID",
    "container_id": {
      "value": "da737efb-a9d4-4622-84ef-f55eb07b861a"
    }
  }
}163
{
  "type": "ATTACH_CONTAINER_INPUT",
  "attach_container_input": {
    "type": "PROCESS_IO",
    "process_io": {
      "type": "DATA",
      "data": {
        "type": "STDIN",
        "data": "dGVzdAo="
      }
    }
  }
}210
{
  "type":
  "ATTACH_CONTAINER_INPUT",
  "attach_container_input": {
    "type": "PROCESS_IO",
    "process_io": {
      "type": "CONTROL",
      "control": {
        "type": "HEARTBEAT",
        "heartbeat": {
          "interval": {
            "nanoseconds": 30000000000
          }
        }
      }
    }
  }
}215
{
  "type": "ATTACH_CONTAINER_INPUT",
  "attach_container_input": {
    "type": "PROCESS_IO",
    "process_io": {
      "type": "DATA",
      "data": {
        "type": "STDIN",
        "data": ""
      }
    }
  }
}
...

ATTACH_CONTAINER_INPUT HTTP Response (JSON):

HTTP/1.1 200 OK

```

### ATTACH_CONTAINER_OUTPUT

This call attaches to the STDOUT and STDERR of the primary process of
a container and streams its output back to the client. This call can
only be made against containers that have been launched with an
associated IOSwitchboard (i.e. nested containers launched via a
LAUNCH_NESTED_CONTAINER_SESSION call or normal containers launched
with a TTYInfo in their ContainerInfo field).  Multiple
ATTACH_CONTAINER_OUTPUT calls can be active for a given container at
once.

```
ATTACH_CONTAINER_OUTPUT HTTP Request (JSON):

POST /api/v1  HTTP/1.1

Host: nodehost:5051
Content-Type: application/json
Accept: application/recordio
Message-Accept: application/json

{
  "type": "ATTACH_CONTAINER_OUTPUT",
  "attach_container_output": {
    "container_id": {
      "value": "e193a755-8528-4673-a05b-2cc2a01a8b94"
    }
  }
}

ATTACH_CONTAINER_OUTPUT HTTP Response (JSON):

HTTP/1.1 200 OK

Content-Type: application/recordio
Message-Content-Type: application/json

90
{
  "type":"DATA",
  "data": {
    "type":"STDOUT",
    "data": "TW9uIEZlYiAyNyAwNzozOTozOCBVVEMgMjAxNwo="
  }
}90
{
  "type":"DATA",
  "data": {
    "type":"STDOUT",
    "data": "TW9uIEZlYiAyNyAwNzozOTozOSBVVEMgMjAxNwo="
  }
}90
{
  "type":"DATA",
  "data": {
    "type":"STDERR",
    "data": "TW9uIEZlYiAyNyAwNzozOTo0MCBVVEMgMjAxNwo="
  }
}
...

```

### REMOVE_NESTED_CONTAINER

This call triggers the removal of a nested container and its artifacts
(e.g., the sandbox and runtime directories). This call can only be made
against containers that have already terminated, and whose parent
container has not been destroyed. Any authorized entity, including the
executor itself, its tasks, or the operator can use this API call.

```
REMOVE_NESTED_CONTAINER HTTP Request (JSON):

POST /api/v1  HTTP/1.1

Host: nodehost:5051
Content-Type: application/json
Accept: application/json

{
  "type": "REMOVE_NESTED_CONTAINER",
  "remove_nested_container": {
    "container_id": {
      "parent": {
        "value": "6643b4be-583a-4dc3-bf23-a1ffb26dd452"
      },
      "value": "3192b9d1-db71-4699-ae25-e28dfbf42de1"
    }
  }
}

REMOVE_NESTED_CONTAINER HTTP Response (JSON):

HTTP/1.1 200 OK
```

### GET_RESOURCE_PROVIDERS

This call retrieves information about all the resource providers known
to the node.

```
GET_RESOURCE_PROVIDERS HTTP Request (JSON):

POST /api/v1  HTTP/1.1

Host: nodehost:5051
Content-Type: application/json
Accept: application/json

{
  "type": "GET_RESOURCE_PROVIDERS"
}


GET_RESOURCE_PROVIDERS HTTP Response (JSON):

HTTP/1.1 200 OK

Content-Type: application/json

{
  "type": "GET_RESOURCE_PROVIDERS",
  "get_resource_providers": {
    "resource_providers": [
      {
        "resource_provider_info": {
          "id": {
            "value": "22d25a71-9134-4fe6-88f4-146c31ceaf4a"
          },
          "type": "org.apache.medios.rp.local.storage",
          "name": "test",
          "default_reservations": [
            {
              "type": "DYNAMIC",
              "role": "storage"
            }
          ],
          "storage": {
            "plugin": {
              "type": "org.apache.medios.csi.test",
              "name": "test_csi_plugin",
              "containers": [
                {
                  "services": [
                    "CONTROLLER_SERVICE",
                    "NODE_SERVICE"
                  ],
                  "command": {
                    "shell": false,
                    "value": "/opt/medios/bin/test-csi-plugin",
                    "arguments": [
                      "/opt/medios/bin/test-csi-plugin",
                      "--available_capacity=4GB",
                      "--volumes=",
                      "--work_dir=/var/run/medios/test_csi_plugin"
                    ]
                  }
                }
              ]
            }
          }
        }
      }
    ]
  }
}

```


### ADD_RESOURCE_PROVIDER_CONFIG

This call launches a Local Resource Provider on the node with the specified
`ResourceProviderInfo`.

```
ADD_RESOURCE_PROVIDER_CONFIG HTTP Request (JSON):

POST /api/v1  HTTP/1.1

Host: nodehost:5051
Content-Type: application/json
Accept: application/json

{
  "type": "ADD_RESOURCE_PROVIDER_CONFIG",
  "add_resource_provider_config": {
    "info": {
      "type": "org.apache.medios.rp.local.storage",
      "name": "test_slrp",
      "default_reservations": [
        {
          "type": "DYNAMIC",
          "role": "test-role"
        }
      ],
      "storage": {
        "plugin": {
          "type": "org.apache.medios.csi.test",
          "name": "test_plugin",
          "containers": [
            {
              "services": [
                "CONTROLLER_SERVICE",
                "NODE_SERVICE"
              ],
              "command": {
                "shell": true,
                "value": "/PATH/TO/test-csi-plugin --available_capacity=2GB --work_dir=workdir"
              },
              "resources": [
                { "name": "cpus", "type": "SCALAR", "scalar": { "value": 0.1 } },
                { "name": "mem", "type": "SCALAR", "scalar": { "value": 200.0 } }
              ]
            }
          ]
        }
      }
    }
  }
}

ADD_RESOURCE_PROVIDER_CONFIG HTTP Response (JSON):

HTTP/1.1 200 OK
```

Possible responses:

* `200 OK`: If a new config file is created.
* `400 Bad Request`: If the request is not well-formed.
* `401 Unauthorized`: If HTTP authentication fails.
* `403 Forbidden`: If the call is not authorized.
* `409 Conflict`: If another config file that describes a resource provider of the same type and name exists.
* `500 Internal Server Error`: If an unexpected error occurs.


### UPDATE_RESOURCE_PROVIDER_CONFIG

This call updates a Local Resource Provider on the node with the specified
`ResourceProviderInfo`.

```
UPDATE_RESOURCE_PROVIDER_CONFIG HTTP Request (JSON):

POST /api/v1  HTTP/1.1

Host: nodehost:5051
Content-Type: application/json
Accept: application/json

{
  "type": "UPDATE_RESOURCE_PROVIDER_CONFIG",
  "update_resource_provider_config": {
    "info": {
      "type": "org.apache.medios.rp.local.storage",
      "name": "test_slrp",
      "default_reservations": [
        {
          "type": "DYNAMIC",
          "role": "test-role"
        }
      ],
      "storage": {
        "plugin": {
          "type": "org.apache.medios.csi.test",
          "name": "test_plugin",
          "containers": [
            {
              "services": [
                "CONTROLLER_SERVICE",
                "NODE_SERVICE"
              ],
              "command": {
                "shell": true,
                "value": "/PATH/TO/test-csi-plugin --available_capacity=2GB --work_dir=workdir"
              },
              "resources": [
                { "name": "cpus", "type": "SCALAR", "scalar": { "value": 0.1 } },
                { "name": "mem", "type": "SCALAR", "scalar": { "value": 200.0 } }
              ]
            }
          ]
        }
      }
    }
  }
}

UPDATE_RESOURCE_PROVIDER_CONFIG HTTP Response (JSON):

HTTP/1.1 200 OK
```

Possible responses:

* `200 OK`: If an existing config file is updated.
* `400 Bad Request`: If the request is not well-formed.
* `401 Unauthorized`: If HTTP authentication fails.
* `403 Forbidden`: If the call is not authorized.
* `404 Not Found`: If no config file describes a resource provider of the same type and name exists.
* `500 Internal Server Error`: If an unexpected error occurs.


### REMOVE_RESOURCE_PROVIDER_CONFIG

This call terminates a given Local Resource Provider on the node and prevents
it from being launched again until the config is added back. The controller and the
node will think the resource provider has disconnected, similar to node
disconnection.

If there exists a task that is using the resources provided by the resource
provider, its execution will not be affected. However, offer operations for the
local resource provider will not be successful. In fact, if a local resource
provider is disconnected, the controller will rescind the offers related to that
local resource provider, effectively disallowing frameworks to perform
operations on the disconnected local resource provider.

The local resource provider can be re-added after its removal using
[`ADD_RESOURCE_PROVIDER_CONFIG`](#add_resource_provider_config). Note that
removing a local resource provider is different than marking a local resource
provider as gone, in which case the local resource provider will not be allowed
to be re-added.  Marking a local resource provider as gone is not yet supported.

```
REMOVE_RESOURCE_PROVIDER_CONFIG HTTP Request (JSON):

POST /api/v1  HTTP/1.1

Host: nodehost:5051
Content-Type: application/json
Accept: application/json

{
  "type": "REMOVE_RESOURCE_PROVIDER_CONFIG",
  "remove_resource_provider_config": {
    "type": "org.apache.medios.rp.local.storage",
    "name": "test_slrp"
  }
}

REMOVE_RESOURCE_PROVIDER_CONFIG HTTP Response (JSON):

HTTP/1.1 200 OK
```

Possible responses:

* `200 OK`: If the config file is removed.
* `400 Bad Request`: If the request is not well-formed.
* `401 Unauthorized`: If HTTP authentication fails.
* `403 Forbidden`: If the call is not authorized.
* `404 Not Found`: If the config file does not exist.
* `500 Internal Server Error`: If an unexpected error occurs.


### PRUNE_IMAGES

This call triggers garbage collection for container images. This call can
only be made when all running containers are launched with Medios version 1.5
or newer. An optional list of excluded images from GC can be speficied via
`prune_images.excluded_images` field.

```
PRUNE_IMAGES HTTP Request (JSON):

POST /api/v1  HTTP/1.1

Host: nodehost:5051
Content-Type: application/json
Accept: application/json

{
  "type": "PRUNE_IMAGES",
  "prune_images": {
    "excluded_images": [
      {"type":"DOCKER","docker":{"name":"mysql:latest"}}
    ]
  }
}

PRUNE_IMAGES HTTP Response (JSON):

HTTP/1.1 200 OK
```
