# Medios OpenAPI

This directory contains OpenAPI descriptions for the public Medios HTTP
surfaces.

The specs are split by daemon instead of collapsed into one path map because
the controller and node intentionally expose overlapping paths such as
`/api/v1`, `/state`, `/flags`, and `/health`. A single OpenAPI document cannot
define two different operations for the same method and path without lying
about the runtime API. Tooling that wants a multi-document Swagger UI can list
both specs.

- `controller.openapi.json` describes controller routes, including the
  scheduler endpoint hosted by the controller, controller internals such as
  the allocator and registrar debug routes, and shared process utility routes.
- `node.openapi.json` describes node routes, including executor and resource
  provider endpoints hosted by the node, and shared process utility routes.

Both specs include common libprocess/Medios routes exposed by the daemons:
`/files/*`, `/help`, `/logging/toggle`, `/memory-profiler/*`,
`/metrics/snapshot`, `/profiler/*`, `/system/stats.json`, `/version`, and
`/__processes__`.

`swagger-config.json` lists both specs for Swagger UI deployments that support
multiple OpenAPI documents.

The v1 APIs are typed RPC-style POST endpoints. For example, controller
operator calls use `POST /api/v1`, and the body `type` field selects the
logical call such as `GET_HEALTH` or `RESERVE_RESOURCES`. The OpenAPI specs
therefore model the real path once and attach the allowed call types as
metadata. Later schema-generation work should replace the placeholder payload
schemas with generated ProtoJSON schemas built from the v1 protobuf
descriptors.

OpenAPI validation is intentionally not part of the default local CMake build;
see `docs/design/principles/FAC3-0024-openapi-validation-ci.md`.

CI validation is tracked in #202. When editing these specs, run the same pinned
containerized linter command used by that workflow. The command should lint both
daemon specs and use a pinned Redocly CLI image or digest, not `latest`.

This first slice keeps the descriptions hand-authored. Route/protobuf drift
checks and generated ProtoJSON schemas should be added with the eventual
OpenAPI generation toolchain rather than introducing a separate repository
scripting language.
