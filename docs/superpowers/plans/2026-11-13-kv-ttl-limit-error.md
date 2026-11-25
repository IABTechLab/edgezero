# KV TTL limit error implementation plan

**Goal:** Make rejection above the portable KV TTL cap distinguishable without
changing supported retention windows.

**Architecture:** Keep validation in `KvHandle`; add a structured `KvError`
variant and preserve the HTTP 400 conversion. Registry and adapter code remain
unchanged.

**Tech stack:** Rust 2024, existing asynchronous KV tests, Markdown guide.

## Task 1: Establish regression coverage

- [x] Run baseline `cargo +1.98.1 test --locked -p edgezero-core`.
- [x] In `crates/edgezero-core/src/key_value_store.rs`, exercise
      `KvRegistry::single_id(...).named(...)` for typed and raw TTL writes.
- [x] Assert inclusive minimum/maximum, below-minimum validation, exact
      over-cap error fields, and no backend call for rejected writes.
- [x] Demonstrate failure against the existing generic over-cap rejection.

## Task 2: Implement the error contract

- [x] Add `TtlLimitExceeded { requested: Duration, maximum: Duration }` to
      `KvError` and return it only from the existing upper-bound guard.
- [x] Preserve HTTP 400 in `From<KvError> for EdgeError` and test conversion.
- [x] Update TTL method and constant documentation; run core tests.

## Task 3: Explain migration and verify

- [x] Update `docs/guide/kv.md` with the portable policy and explicit downstream
      handling, distinguishing cap rejection from unsupported backend TTLs.
- [x] Run Rust formatting, workspace tests, all-feature clippy, workspace
      feature compilation and the Spin WASM check required by `CLAUDE.md`.
- [x] Check affected adapter WASM builds and documentation lint/format.
- [x] Independently review the final change against issue #323, preserve any
      blocked check details, and prepare an accurate PR title and description.
