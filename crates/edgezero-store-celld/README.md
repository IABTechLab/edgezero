# Native scoped celld stores

All runtime features are default-off. Settings remain available without native transport dependencies.

- `kv` prepares the existing raw `/v1/.../value` binding without a remote probe.
- `records` retains the legacy, separate positive-TTL record client and binary metadata API.
- `records-v2` enables the portable core record contract over a fresh v2 dataset. It implies `kv` for shared transport, not automatic conversion of raw bindings.

Prepare an explicit record binding with:

```rust,ignore
preparation
    .prepare_record_store(&settings, &layout, token.as_ref())
    .await?;
```

The method takes `&mut CelldPreparation`, `&CelldKvSettings`, `&RecordLayout`, and `Option<&CelldAccessToken>`, returning `Result<KvHandle, PreparationError>`. Application layouts must be installed before provider preparation. The initializer can acquire `KvHandle::require_records()` without changing routing or probing with a mutation.

Preparation performs a read-only `/v2/stores/{alias}/record-capabilities` handshake and checks the exact format, keyset pagination, no-expiry support, metadata limit, and response version. An old or incompatible service fails preparation. Capability does not establish permanent availability or write authorization. There is no downgrade, data migration or raw-data fallback.

Ordinary and record operations on this binding use the same `/v2/.../record` rows. Ordinary replacements clear metadata, allocate a fresh generation and apply the supplied expiry. Ordinary no-TTL writes send `ttl_ms: null`, which stores actual non-expiry. Raw and v1 record datasets remain separate and are not automatically visible in v2.

Bodies remain arbitrary bytes. Portable metadata is optional UTF-8, with absent and empty normalized to None. Nonempty text is preserved exactly, invalid UTF-8 responses fail, and the actual celld limit is 1024 metadata bytes. The body cap is the configured native cap, at most 1 MiB. Existing core TTL bounds apply to the portable interface; relative TTL rounds upward to whole milliseconds. Expiry visibility and physical cleanup follow celld's native behavior. Exact HEAD existence may include expired rows pending sweep.

Both ordinary arbitrary-prefix/global listing and record partition listing use native keyset continuation. The latter validates complete application partitions. Cursors preserve the actual service token and bind provider scope, format/layout, query mode and prefix while retaining the last observed key for progress validation. Core also binds its public record cursor. Pages are live, not a cross-page snapshot. Stop only when continuation is absent; empty or short pages may continue.

Mutations are never automatically replayed. A dispatched v2 mutation with a lost, timed-out or untrustworthy response yields payload-free `KvError::MutationUncertain`. A definitive Add/CAS 412 is `PreconditionFailed`; an unconditional rejection is an error. Applications own reconciliation.

The matching companion service source is the isolated local celld `feat/scoped-records-v2` worktree, based on `9350a043860c4f41dccb692ec9de98ebd9dac3ae`. Its fresh `__kv_records_v2` format retains transactional namespace-local generation allocation across delete, expiry and restart. A cached older binary does not prove compatibility. Record the rebuilt artifact/source identity before acceptance.

## Focused proof

Colocated `records_v2` wire tests cover protocol preparation, ordinary/record routes, no-expiry encoding, metadata validation, cursor identity/progress and lost-reply classification without retries. Run legacy/raw neighboring tests separately as well.

`tests/records_v2_interop.rs` is an explicit ignored real-service test. Supply `CELLD_RECORDS_V2_ENDPOINT=http://127.0.0.1:<port>` for a freshly rebuilt disposable scoped listener with an empty writable alias `identity`, then run the test with `--ignored`. It executes the common public-handle contract, independent Add/CAS races, multi-group global listing, and an acknowledgement-dropping proxy that confirms actual server commits and no mutation replay. After a lost DELETE the proxy recreates the value before closing the connection, so an unsafe replay would erase observable data.

No hosted service, fleet isolation, publication, deployment or migration claim follows from local proof.
