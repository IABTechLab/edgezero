# Native celld KV and records

This optional provider talks to the EdgeZero-owned store service on official,
unmodified celld. The service source is `services/celld-store/` in the matching
EdgeZero revision. It is not the historical fork's scoped listener.

## Features

- No feature: settings only, without the native HTTP client/runtime graph.
- `kv`: ordinary byte KV through the service.
- `records`: the same transport plus the portable prepared record capability.
- Axum forwards `celld-kv` and `celld-records`, both default-off.

There is no `celld-config` provider or legacy fork record API on this line.

## Preparation and behavior

Applications declare record layouts before provider preparation. Record setup
performs a capability GET, checks the store/record versions, format, pagination,
no-expiry, ledger support, limits and instance identity, then publishes a handle.
Ordinary KV setup remains probe-free.

The record API supports Add, Overwrite, generation-match writes, text metadata,
authoritative physical existence and complete scoped pagination. Ordinary
operations on a record handle address the same rows and invalidate generations
on replacement. The durable namespace allocator is not deleted or expired with
values.

The client sends one POST per mutation with a fresh ID. The service records
successful and rejected outcomes transactionally, so internal celld duplicate
delivery returns the original outcome without another effect. A lost
acknowledgement is uncertain completion, not a conditional rejection.

Record requests carry the prepared instance and the service checks it before
effects. Cursors bind instance, endpoint, alias, layout, operation and prefix.
A replacement database invalidates old handles; restoring an older snapshot of
the same instance remains outside this protection.

Lookup/list use native expiry filtering. Physical existence and Add/CAS use the
physical value until sweep. Metadata is UTF-8, empty and absent are equivalent,
and the service supports 1024 metadata bytes plus values up to 1 MiB. Transport
bounds include base64 expansion.

## Checks

```sh
cargo test -p edgezero-store-celld --features records --lib
cargo clippy -p edgezero-store-celld --all-targets --all-features -- -D warnings
node --test services/celld-store/test/*.test.mjs
```

The ignored `stock_records_interop` target requires an explicit official
`CELLD_BIN` and `EDGEZERO_CELLD_STORE_SERVICE`, or an explicitly selected
disposable loopback service. It executes the shared public contract, independent
Add/CAS, global listing, maximum escaped keys and dropped committed replies.
Missing prerequisites fail rather than skip.

Node SQLite tests and scripted wire tests are not stock-runtime or fleet proof.
Production capacity, bucket qualification, same-instance restore and live
deployment remain separate work. See
[the application guide](../../docs/guide/native-celld-stores.md).
