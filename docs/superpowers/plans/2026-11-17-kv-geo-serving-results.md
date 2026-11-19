# Axum KV Geo local backend proof

`scripts/run_geo_kv_integration.mjs` exercises publication, verification and managed Axum serving. See the [guide](../../guide/native-geo.md#reproduce-the-local-backend-proof) for the full command. The local run used Rust 1.98.1 debug binaries, official celld 0.6.3 and DynamoDB Local 3.3.1. The listener fixture was built at `1b7eeb89` with `geo-kv,celld-kv,aws-dynamodb,test-utils`; the later compiled-out lint change did not change the running fixture path. The operator tool was built from `7c0cd31e`. Both sample sizes were explicit command inputs, independent of the tool's default. Executables were copied to stable paths and their SHA-256 digests were checked again at the end of the run.

| Executable       | SHA-256                                                            |
| ---------------- | ------------------------------------------------------------------ |
| Stock celld      | `eb0a8158421b69d68dd5d28f2298a7d411dde498295833deeb1cc5ce53b7eb75` |
| Publisher        | `f9eee2fe8ab97ae560eac6334f7ce9891dd0f52d7d4463895e11741f8aaedd3d` |
| Listener fixture | `fdd5252dacba39bee572920a2973138d77cf2f990016eb33589277a84ea04275` |

## Correctness and lifecycle

Both backends served the same synthetic artifacts. The runner checks interrupted writes, identical replay, immutable conflicts, read-only serving, verified IPv4 and IPv6 results, startup rejection, request errors and deadlines, simultaneous pins, restart, rollback and retirement. It verifies celld denying a write under a read-only token. DynamoDB Local confirms the serving code issues no writes, but does not enforce hosted IAM.

Both backends then published matching 16 KiB and 64 KiB artifacts built once per size from the `GeoLite2-City` CSV release dated 2026-11-03. The proof checked every object through the serving binding and found no mismatches in 4,096 IPv4/IPv6 oracle comparisons per backend and size. The CSV and derived objects are not checked in.

| Object budget | Objects | Encoded bytes | Inventory SHA-256                                                  |
| ------------- | ------: | ------------: | ------------------------------------------------------------------ |
| 16 KiB        |   2,595 |    42,386,355 | `f840b300927cc6f2075d5f8a20710de355059fee5d363d5cbc6cf6e053939c24` |
| 64 KiB        |     649 |    42,259,651 | `7ec62707c1ba18204fe054222751f6d5330dfdd3428c6849b72a2c0660ca57c0` |

## Measurements and default

The HTTP workload contains 128 addresses sampled from the source blocks and sends four requests at a time. The first pass starts with an empty object cache. The second pass repeats those addresses in the same order. RSS is the managed host's process high-water mark. Response bytes include the backend's serialization.

| Backend        | Budget | Startup | Peak host RSS | First/repeat p95 | First/repeat KV reads per request | First/repeat response bytes per request |
| -------------- | -----: | ------: | ------------: | ---------------: | --------------------------------: | --------------------------------------: |
| celld          | 16 KiB |  106 ms |      29.9 MiB |    48.0 / 1.1 ms |                       1.25 / 0.00 |                              19,151 / 0 |
| celld          | 64 KiB |  106 ms |      31.7 MiB |   53.4 / 54.5 ms |                       1.06 / 1.06 |                         65,769 / 65,769 |
| DynamoDB Local | 16 KiB |  105 ms |      34.2 MiB |    29.4 / 1.9 ms |                       1.27 / 0.00 |                              26,130 / 0 |
| DynamoDB Local | 64 KiB |  106 ms |      37.6 MiB |   26.0 / 25.6 ms |                       1.06 / 1.06 |                         87,905 / 87,905 |

Default to 16 KiB. It transfers less data and cached the entire repeated working set in this run. The 64 KiB build costs fewer writes to publish and needs at most two object reads, versus three for 16 KiB. An operator can choose it explicitly. These local results do not establish production latency, traffic capacity, hosted IAM or fleet durability.
