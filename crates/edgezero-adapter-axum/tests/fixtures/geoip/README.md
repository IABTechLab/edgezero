# Synthetic GeoIP fixtures

These files are original test data for EdgeZero, licensed under this repository's Apache-2.0 license. They contain no MaxMind, GeoLite, DB-IP or other production dataset content. Metadata names describe the schema under test and do not claim publisher authenticity. Addresses and locations are fabricated. Loopback US/CA and US/NY records exist solely for local hosting tests.

Rebuild from the repository root with Node.js:

```sh
node scripts/generate_geoip_fixtures.mjs
node scripts/generate_geoip_fixtures.mjs --check
```

The dependency-free generator writes deterministic MMDB version 2 files and `SHA256SUMS`. The fixed build epochs are synthetic. The default 24-bit tree, 28/32-bit variants, IPv4 and IPv6 records exercise reader format support, not data accuracy. No production download, account, credentials or network access is required.

`city.mmdb` includes 127/8 as US/CA and a matrix at 192.0.2.1 through 192.0.2.20 covering case normalization, country-only records, numeric subdivision `01`, unrecognized `ZZ`, missing first code with a later code, empty arrays, empty records, unsupported/empty/malformed codes, subdivision without country, registered/represented-country non-substitution, duplicate-key first-value behavior and unrelated later-entry types. Other addresses have no record. `city-replacement.mmdb` changes loopback and 192.0.2.1 to US/NY and changes the epoch. `country.mmdb` deliberately gives subdivisions an incompatible geographic shape to prove Country ignores them. `ipv6.mmdb` contains 2001:db8::/32 as GB/ENG and ::c000:201/128 for IPv4 lookup 192.0.2.1 as US/CA.

The `bad-*` files deliberately contain consumed type errors, off-path invalid pointers/UTF-8, nested non-string map keys, prohibited pointer chains, a tree cycle, a corrupt separator, or an unknown metadata pointer outside its section. `truncated.mmdb` is incomplete. `unsupported.mmdb` has an unsupported dataset type. `expansion-limit.mmdb` uses repeated shared arrays that pass memoized verification but exceed normal decoder expansion limits. `verification-limit.mmdb` and `metadata-verification-limit.mmdb` use overlapping valid string targets to exhaust the respective verifier work budgets. They are intentionally not usable databases. `all-values.mmdb` covers all scalar types and valid single pointers without imposing unrelated geographic schemas.

## Validation boundary

Preparation uses maxminddb 0.32.0 without mmap or unsafe string decoding. Its verifier checks metadata and reachable tree/data structure. An additional non-materializing Serde traversal follows all reachable data values through the normal decoder, requires string map keys and catches prohibited pointer chains even in unrelated fields. It uses the pinned reader's `deserialize_identifier` entry point recursively because that entry point checks pointer targets before falling back to normal value decoding. Direct `deserialize_any` would still accept pointer chains. This behavior is covered by regressions that first prove upstream verification accepts each malformed data fixture. Geographic paths are borrowed strings and are identical at startup and lookup. Geographic code semantics are intentionally checked only on lookup.

Residual metadata limitation: `bad-metadata-key.mmdb` reproduces a known startup-acceptance failure, retained in the explicitly ignored `geo_maxmind_rejects_unused_metadata_nonstring_keys` regression. Running that test with `--ignored` currently fails. This blocks #438 acceptance. The pinned public API exposes typed metadata and `verify()`, but no raw metadata decode operation. Consequently unknown metadata fields retain the upstream verifier's acceptance of non-string nested map keys and pointer-to-pointer chains. Known metadata fields receive upstream typed decoding; malformed unknown metadata pointers and UTF-8 still fail verification. No claim of stricter full metadata conformance is made. Unreachable data bytes are outside the reachable-record contract.

Normal decoding uses upstream finite value, payload and depth budgets per unique root. Shared graphs are expanded, not silently skipped. Network visits include no-data leaves and aliases before record-offset deduplication. A precheck reserves upstream's `3 * node_count + 1` tree allowance before verification. Bounds do not guarantee startup duration or a fixed RSS. No production-size GeoLite performance or memory measurement is established by these tiny fixtures.

File acquisition is supported only on Unix, with read-only `O_NONBLOCK` open, descriptor regular-file validation and bounded actual bytes read. Other platforms reject selected MaxMind as an unavailable implementation before opening a source because equivalent special-file safety has not been proven. Nonblocking open does not impose a deadline on regular-file filesystem I/O. Snapshots retain owned bytes, not a source handle or mmap, and remain valid after source replacement/deletion.
