# src/resource_provider

Compatibility stub for the Mesos Resource Provider subsystem.

## Status in Medios

- **CSI and Storage Local Resource Providers (`SLRP`) were stripped in `STRIP-009` (PR #46).** `LocalResourceProvider::create` and `LocalResourceProvider::validate` (`local.cpp`) reject all provider types with `"Unknown local resource provider type"`, and `LocalResourceProviderDaemon` (`daemon.{hpp,cpp}`) is retained solely as a no-op compatibility seam for `src/node/node.cpp` and the `/api/v1/resource_provider` wire surface.
- **Shared datasets and node-local storage use `store/<name>` (`DR-036`).** Instead of CSI or Local Resource Providers, Medios models node-resident shared datasets as operator-declared `store/<name>` shared resources (`--stores`, `src/node/stores.{hpp,cpp}`, `src/controller/stores.{hpp,cpp}`), mounted (RO, RW, or RW exclusive) into tasks via `Volume.Source.Type: STORE` (Linux bind-mounts in `filesystem/linux`, macOS symlinks in `filesystem/posix`) and exposed read-only over the node files API at `/stores/<name>`.
