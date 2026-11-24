# Delivering task artifacts with `command.uris`

A task often needs files on the node before it runs — an executable, a config, an archive. Rather than pre-staging them (scp, a baked image), a framework lists them in `CommandInfo.uris` and the node fetches each into the task sandbox before the command starts. The command then references them by basename, because the sandbox is the command's working directory.

This is the standard delivery path. It needs no SSH access to nodes and no custom executor.

## The contract

`CommandInfo.uris` is a repeated `URI` message (canonical definition in [`medios.proto`](../include/medios/medios.proto), and on the wire via the [OpenAPI specs](../api/openapi/)):

| Field | Type | Default | Meaning |
|---|---|---|---|
| `value` | string (required) | — | The URI to fetch. |
| `executable` | bool | false | `chmod +x` the fetched file. |
| `extract` | bool | true | If the file is a recognized archive, extract it into the sandbox. |
| `output_file` | string | URI basename | Local name for the copy. A relative path stores it in a sandbox subdirectory. |
| `cache` | bool | false | Accepted for wire compatibility but **ignored** — see Caching below. |

## Supported schemes

- `http://` and `https://` — fetched with the node's HTTP client. This is the production path: point at an artifact server.
- `file://` and bare local paths — copied from the node's filesystem. Useful for local development.

## Behaviour

The node runs `medios-fetcher` (which must be present in the node's `--launcher_dir`) once per task, before the command. Each URI is downloaded directly into the sandbox. Then:

1. If `extract` is true and the file is a known archive (`.tar`, `.tar.gz`/`.tgz`, `.tar.bz2`, `.zip`, `.gz`), it is unpacked into the sandbox with `tar`/`unzip`/`gunzip`. Otherwise the file is left as-is.
2. If `executable` is true, the file is made executable.

If any URI fails to fetch, or `medios-fetcher` is missing from `launcher_dir`, the container fails at the `FETCHING` stage and the task gets `TASK_FAILED` — there is no silent fallback to running the command without its artifacts.

## Caching

Medios does not implement the fetcher cache. Every fetch downloads fresh into the sandbox (`BYPASS_CACHE`). The `cache` field is accepted so existing wire payloads validate, but setting it has no effect. If you need caching, cache at the artifact server (HTTP caching headers) or in your framework.

## Examples

Deliver an executable over HTTP and run it — the common case:

```json
{
  "name": "search-shard-7",
  "task_id": {"value": "search-shard-7"},
  "command": {
    "value": "./cpu_search_z --shard 7",
    "uris": [
      {"value": "http://artifacts.internal/cpu_search_z", "executable": true, "extract": false}
    ]
  }
}
```

Deliver and unpack a release archive:

```json
{
  "command": {
    "value": "./bin/run.sh",
    "uris": [
      {"value": "https://artifacts.internal/brawny-1.4.0.tar.gz", "extract": true}
    ]
  }
}
```

Store a config under a sandbox subdirectory without extracting:

```json
{
  "uris": [
    {"value": "http://artifacts.internal/job.cfg", "output_file": "conf/job.cfg", "extract": false}
  ]
}
```

## Operational requirement

For URI delivery to work, the node's `--launcher_dir` must contain `medios-fetcher` alongside the other helper binaries (`medios-executor`, `medios-containerizer`). A node deployed without it fails any task that lists `uris`. The `medios doctor` preflight checks for this.
