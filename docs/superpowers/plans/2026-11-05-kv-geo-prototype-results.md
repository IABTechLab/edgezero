# KV Geo prototype results

- Date: 2026-11-05.
- Status: format/lookup experiment completed; production integration and remote KV acceptance remain open.
- Spec: [portable KV Geo](../specs/2026-11-05-kv-geo-design.md).
- Code and reproduction commands: [prototype README](../../../examples/geo-kv-prototype/README.md).
- Base revision: `72ceed7e2a802cfa536851dd20c95ca55ebea4fc`, plus the standalone prototype identified by the source fingerprint below.
- Environment: Linux x86_64, Rust 1.95.0, optimized Cargo release build, local file-backed artifact KV.
- Tested source fingerprint: `70227e734eee4709c7072a45a50dcc00ea15bd1c947c82596025a309e9c2ddbf`. Compute with `sha256sum examples/geo-kv-prototype/src/*.rs examples/geo-kv-prototype/Cargo.toml examples/geo-kv-prototype/Cargo.lock | sha256sum`.
- Tested release binary SHA-256: `dd9af4b519c1213f2094b6805a41307fc637137eded40751491384dbfb5e6878`.

## Dataset

Used the user's `GeoLite2-City-CSV_20261103.zip`, kept outside Git. Archive SHA-256 is `94745697bcd25751e38c9386260eeb29527f851ec9e3732cfe58aa62a07b9ab8`. ZIP CRC checks passed; a publisher-provided checksum was not available for comparison.

| Input | Count |
| --- | --- |
| English location rows | 75,449 |
| IPv4 source networks | 3,632,756 |
| IPv6 source networks | 2,091,942 |
| Merged IPv4 geographic intervals | 1,371,640 |
| Merged IPv6 geographic intervals | 605,625 |
| IPv4 networks without country | 768 |
| IPv6 networks without country | 724 |

The import joins geolocation IDs only. It does not substitute registered-country IDs or include names, coordinates or postal codes in the index. Source networks without country produce gaps, not guessed locations.

## Index shape

All objects were written and read back through the actual EdgeZero `KvHandle`. The transport was the prototype's local artifact-file `KvStore`, not a cloud service.

| Object budget | Encoded total | Objects, including manifest | Uncached path objects, IPv4/IPv6 | Import, build and read-back |
| --- | --- | --- | --- | --- |
| 16 KiB | 40.423 MiB | 2595 | 3/3 | 1.482 s |
| 64 KiB | 40.302 MiB | 649 | 2/2 | 1.487 s |
| 256 KiB | 40.272 MiB | 165 | 2/2 | 1.490 s |
| 384 KiB | 40.269 MiB | 111 | 2/2 | 1.460 s |

A resolver acquires the manifest with one additional read when opened. The path counts above exclude it and are maxima; root/directory gaps can return without a leaf read.

The 64 KiB build contained 42,259,649 encoded bytes and 649 objects. Its largest object was 65,532 bytes. Both address families fit a root directory followed by a leaf. Increasing shards beyond 64 KiB reduced key count but did not remove another read from this dataset's path.

Importer whole-process peak RSS was about 231.4 MiB. That process retains raw CIDRs for verification and builds merged ranges, so this is not serving memory.

## Lookup checks and local timings

For each size, checked 2,048 points per family in three passes: no object cache, an initially empty 4 MiB object cache, and a repeat using that same cache. All 49,152 comparisons across four sizes and three modes matched the independent source-CIDR oracle. These are repeated comparisons over the same 4,096-point workload, not 49,152 distinct random addresses or exhaustive dataset coverage.

The sample mix includes family endpoints, stratified source-network endpoints and neighbors, and deterministic uniform random addresses. Each pass found 2,917 geographic locations and 1,179 no-matches. It is a correctness workload with correlated neighbors and many uncovered addresses, not a user-traffic distribution.

| Object budget | Cache-disabled p95, microseconds | Repeated-cache p95, microseconds | Repeated-cache reads/query | Repeated-cache bytes/query |
| --- | --- | --- | --- | --- |
| 16 KiB | 63.5 | 46.5 | 0.240 | 3.83 KiB |
| 64 KiB | 158.5 | 142.9 | 0.228 | 14.51 KiB |
| 256 KiB | 524.9 | 514.6 | 0.184 | 46.51 KiB |
| 384 KiB | 1266.1 | 749.0 | 0.184 | 69.96 KiB |

Timings include local file reads, hashing, validation and lookup. OS page cache was uncontrolled. They do not measure remote latency, cold storage, concurrency, provider billing or cache behavior across short-lived isolates. The mean read/byte rates are specific to this sample ordering and cache budget.

A fresh 64 KiB `lookup` process resolved four sample inputs without loading source CSV or an inventory. It read one manifest, one directory and one leaf, retained 77,304 cache payload bytes, and peaked at 3,006,464 bytes of whole-process RSS. This proves a small lookup workload avoids the importer's full-data memory footprint. It does not establish maximum serving RSS under load. Cache limits count retained encoded payload, with allocator metadata and validation scratch memory in addition.

## Country-code policy mismatch

The real release contains `XK`, while EdgeZero's existing `CountryCode` rejects it. There are 393 IPv4 and 68 IPv6 source networks with that code.

The initial strict import failed on this mismatch. To finish the format experiment without changing production country policy, the standalone prototype preserves syntactically valid source country codes in a separate DTO and reports whether core accepts them. A fresh lookup over an actual `XK` interval returned the preserved code with `core_country_supported: false`. No shared allowlist or application outcome changed.

This is a decision needed before production implementation. Rejecting a complete release because it contains one unsupported code prevents using this dataset as supplied. Alternatives must explicitly define unsupported-code behavior rather than silently losing those intervals or calling them `NoMatch`.

## Verification record

| Check | Result |
| --- | --- |
| `cargo test --offline --locked --manifest-path examples/geo-kv-prototype/Cargo.toml` | 10 prototype tests passed |
| `cargo clippy --offline --locked --manifest-path examples/geo-kv-prototype/Cargo.toml --all-targets -- -D warnings` | Passed |
| Prototype and workspace `cargo fmt -- --check` commands | Passed |
| `cargo test --offline --locked -p edgezero-core` | 753 unit tests and enabled doctests passed; 13 existing doctests ignored |
| `cargo build --offline --locked --release --manifest-path examples/geo-kv-prototype/Cargo.toml` | Passed |
| Real-data build at 16/64/256/384 KiB | Every exported object read back and digest-checked |
| Repeat real-data builds | Same receipts and immutable object bytes accepted for every size |
| CSV oracle with cache off/on/repeated | 49,152 comparisons, zero mismatches |
| Fresh-process lookup, including real `XK` interval | No CSV/inventory preload; unsupported-domain flag preserved |
| Pre-PR `cargo test --offline --locked --workspace --all-targets` | 2,115 passed, six existing tests ignored |
| Pre-PR `cargo clippy --offline --locked --workspace --all-targets --all-features -- -D warnings` | Passed |
| Pre-PR `cargo check --offline --locked --workspace --all-targets --features 'fastly cloudflare spin'` | Passed |
| Pre-PR `cargo check --offline --locked -p edgezero-adapter-spin --target wasm32-wasip2 --features spin` | Passed |

Synthetic tests cover multilevel traversal, deterministic output, cache eviction, IPv4/IPv6 zero and maximum addresses, empty families, gaps, country-only results, absent geolocation without registered-country fallback, quoted CSV fields, bad references, duplicate identities, overlaps, bad family/CIDR input, missing objects, wrong digests and malformed records with matching digests. Missing expected data is an error, not a no-match.

Logs from this session are under `/tmp/geo-kv-prototype-*.log`. Dataset, artifacts and durable machine-readable measurements are under `~/.cache/edgezero/geo-kv-prototype/`; each `artifacts-<size>k/` directory contains `receipt.json`, `inventory.json`, `build-report.json` and `verification-report.json`. These are local artifacts, not committed production data.

## Next decisions

64 KiB is a reasonable next remote-KV experiment: it preserved two-read lookup paths while transferring and validating less data than 256/384 KiB shards. The locally fastest 16 KiB objects needed a third uncached read, which may lose once network latency is included. No serving default or wire format is frozen by these results.

Next work should decide country-code policy, exercise actual provider upload/read paths, and measure representative traffic and remote KV costs before changing request APIs or dispatch. City import and synthetic Country-shaped CSV were exercised; a full real Country release was not.

Not verified: production async provider composition, request cancellation/deadlines, reader-IP admission on edge adapters, WASM runtime behavior, live KV latency/capacity, globally visible publication, crash-safe uploads, licensing review or deletion across deployed caches. No live credentials or cloud resources were used. The pre-PR workspace gates above passed without changing the prototype's measured source fingerprint; logs are under `/tmp/geo-kv-pr-*.log`. No new Fastly/Cloudflare WASM runtime execution or live-service acceptance was performed.

The existing GitHub workflow base-branch filters do not include `feature/pluggable-geo-providers`, and root workspace checks do not include this standalone crate. The explicit prototype commands in this record are its local verification, not a claim of automated GitHub coverage.
