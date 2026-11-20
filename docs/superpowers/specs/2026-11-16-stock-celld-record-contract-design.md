# Record contract on the stock celld store service

Implementation extension to the [stock-service design](2026-11-16-stock-celld-store-service-design.md) for the existing [portable record contract](2026-11-13-kv-record-contract-design.md). It belongs to the combined #472/#473 PR, not the ordinary-service foundation in #422.

The user approved the detailed implementation plan and PR ownership map on 2026-11-16. Cross-repository execution is recorded in `Projects/Trusted Server/plans/Retarget the Record Contract to the Stock Celld Store Service.md` in Obsidian. Approval permits local implementation and forward merges, not public changes or deployment.

## Unchanged foundation

Use official celld v0.6.3 with the EdgeZero-owned `services/celld-store` Worker and one SQLite Durable Object per namespace. Keep the `rows-v1` schema, script/class/object identities, namespace allocator, ordinary KV endpoints, authorization grants, output gate, alarms and automated service deployment. Do not introduce a fork, another row dataset, client-side CAS or a second service.

The service's durable namespace allocator is sufficient for opaque u64 generation non-reuse. DynamoDB's separate distribution requirement does not require a per-key celld allocator redesign.

## Wire contract

All responses carry `x-edgezero-store-version: 1`; the record-capable service also carries `x-edgezero-record-version: 1`. Object responses carry `x-edgezero-store-instance`. Record requests require record version 1 and `x-edgezero-expected-instance` captured by preparation. The capability request needs no expected instance and never increments the allocator or changes user rows.

| Operation                | Request                                                           | Result                                                                             |
| ------------------------ | ----------------------------------------------------------------- | ---------------------------------------------------------------------------------- |
| Capabilities             | Existing GET `/v1/stores/{alias}/capabilities`                    | Existing fields, `records: true`, `record_protocol: 1`, actual limits and instance |
| Lookup                   | GET `/v1/stores/{alias}/record?key=`                              | JSON `body`, nullable `metadata`, decimal-string `generation`; logical miss 404    |
| Physical exact existence | HEAD on the record URL                                            | 200 if the exact row is physically present, otherwise 404; no body                 |
| Write                    | POST `/v1/stores/{alias}/record-put?key=`                         | 204 written; 412 `precondition_failed` only for Add/CAS                            |
| Delete                   | POST `/v1/stores/{alias}/record-delete?key=`                      | 204, including absent row; preserve generation history                             |
| Listing                  | GET `/v1/stores/{alias}/record-keys?prefix=&limit=&cursor=&mode=` | JSON keys and nullable cursor; mode `partition` or `ordinary`                      |

Record-write JSON contains base64 `body`, optional nullable base64 UTF-8 `metadata`, optional nullable integer `ttl_ms`, `mode` and a decimal-string `generation` only for `if-generation-match`. Modes are `add`, `overwrite` and `if-generation-match`. Empty and absent metadata normalize to no metadata.

Keep 512-byte keys, service values up to 1 MiB and metadata up to 1024 UTF-8 bytes. Encoded request/response bounds include base64 expansion and framing; do not reduce decoded capacity to fit the ordinary raw-body bound. Records accept positive TTL rounded upward to integer milliseconds, through the existing maximum lifetime. Ordinary KV keeps its existing minute-based minimum and second-based wire behavior.

## Conditions, generations and uncertainty

Condition evaluation, generation allocation, row replacement/deletion and insertion of the mutation-ledger outcome happen in one synchronous transaction. Every replacement allocates a fresh, non-wrapping generation from durable metadata. Ordinary replacements use the same allocator, clear metadata and remove old expiry on no-TTL writes.

The expected instance is checked against durable metadata inside the mutation transaction before any effect. Checking only the response identity could detect a reset after a stale token already modified a replacement database. Read/list operations validate it too. Same-instance snapshot rollback remains an explicitly unsupported recovery case.

Every mutation uses a fresh client-generated ID. The fingerprint includes operation, key, mode, expected generation, metadata, TTL, expected instance and body. Existing ordinary fingerprints remain byte-compatible across the additive service update.

Read the ledger before evaluating current state. Store successful writes/deletes and rejected preconditions. A duplicate must return the first result even after intervening deletion or recreation. A different request under the same ID is invalid. Ledger insertion for rejection is a durable mutation and must schedule its cleanup too.

Keep the existing apply-before window and ledger retention. A late buffered call whose original ledger entry was swept must still fail its old deadline rather than apply again. The client never retries automatically. Dropping a response after commit is uncertain completion, not contention. These are bounded duplicate-delivery guarantees with the existing clock/retention assumptions, not unlimited external replay protection.

## TTL and existence

Lookup and list retain the service's native expiry filtering. Strong existence checks the physical row, including an expired-but-unswept value. Add/CAS operate on physical presence/generation until the native sweep deletes that row. This allows lookup miss and authoritative held state to coexist without falsely declaring absence.

Delete and sweep never reset the allocator. Sweep rechecks expiry before deleting; no-TTL replacement stores NULL expiry. TS retains responsibility for consent, tombstone contents and marker validity. No portable exact-expiry deadline is introduced.

## Pagination and preparation

Keep SQLite keyset pagination. Record cursor version 2 binds instance, alias, mode, prefix and position; ordinary service cursors remain version 1. The Rust outer cursor independently binds endpoint, alias, format, layout ID/version, instance-derived scope and operation. Neither cursor is interchangeable with the fork's protocol or another binding.

Only absent continuation ends traversal. Pages are authoritative for the provider-visible values at that page's read, not a multi-page snapshot. Ordinary enumeration through a record handle may use arbitrary/empty prefixes. Complete-partition validation stays in the application-declared Rust layout, not TS-specific service rules.

The default-off provider `records` feature shares the ordinary transport and dependencies. Preparation validates the service identity, both protocols, format, keyset support, no-expiry, ledger support, metadata limit and response/body instance agreement before publishing a handle. An unsupported or unavailable selected binding fails startup; there is no downgrade. Ordinary KV preparation remains probe-free.

## Evidence boundaries

Node SQLite fixtures prove transaction and codec behavior, not stock runtime durability. Wire fixtures prove the production Rust transport and error classification, not backend CAS. Real stock-service acceptance executes the public shared contract, independent Add/CAS, Unicode pagination and dropped committed replies. The existing two-node fleet proof must separately be extended to conditional outcomes and owner takeover.

All receipts identify source, official runtime digest and service content. Earlier fork-based proofs remain historical. No production capacity, bucket, recovery, deployment or rolling-upgrade qualification is implied.
