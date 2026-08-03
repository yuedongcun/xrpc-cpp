# Vendored Dependencies

The source snapshots below are committed directly to this repository. The
normal xrpc source build does not download them or discover replacements from
the host system.

| Dependency | Version | Commit | Upstream | License |
| --- | --- | --- | --- | --- |
| Protocol Buffers | v21.12 | `f0dc78d7e6e331b8c6bb2d5283e06aa26883ca7c` | <https://github.com/protocolbuffers/protobuf> | BSD-3-Clause |
| nlohmann/json | v3.11.3 | `9cca280a4d0ccf0c08f47a99aa71d1b0e52f8d03` | <https://github.com/nlohmann/json> | MIT |
| GoogleTest | v1.14.0 | `f8d7d77c06936315286eb55f8de22cd23c188571` | <https://github.com/google/googletest> | BSD-3-Clause |
| liburing | liburing-2.5 | `f4e42a515cd78c8c9cac2be14222834be5f8df2b` | <https://github.com/axboe/liburing> | MIT or LGPL-2.1-only |

When updating a dependency, replace its complete source directory, update the
version and full commit hash above, review its license, then run the complete
local CI gate from a clean build directory.
