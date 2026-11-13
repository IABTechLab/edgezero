# Standard JA4 request interface — implementation plan

**Spec:** `docs/superpowers/specs/2026-11-12-ja4-request-interface-design.md`
**Issue:** #283
**Branch:** `spec/283-ja4-request-interface`

**Goal:** expose one provider-neutral, adapter-populated JA4 value
(`ctx.ja4()`) backed by `RequestIngress`, populated by Fastly and Cloudflare
from their runtime TLS APIs and reported as `NotExposed` by Spin and Axum.

**Architecture:** a new `edgezero_core::ja4` module holds the validated
fingerprint type and capture enums. `RequestIngress` gains an optional
`CapturedJa4` (absence = adapter made no claim). Each adapter appends one
`.with_ja4(...)` to the ingress it already builds. No adapter reads a header.

**Tech:** Rust 2024, no new dependencies, workspace `restriction` clippy lints.

## Tasks

### Task 1 — Core `ja4` module (spec §5.1–5.3, §5.6)

Files: create `crates/edgezero-core/src/ja4.rs`; modify `crates/edgezero-core/src/lib.rs`.

- [ ] Write colocated tests first: FoxIO example + `q`/`d`/`i`/zero sections/uppercase ALPN accepted; section accessors; rejection list (empty, 35/37 bytes, whitespace, uppercase hex, separators, unknown protocol/SNI, non-digit counts, non-ASCII, JA3 hash, `ja4_r`); `from_runtime_tls` for `None`/valid/invalid; redacted `Debug`; error text never contains input.
- [ ] Implement `Ja4Fingerprint` (`parse`, `as_str`, `summary`, `cipher_hash`, `extension_hash`, redacted `Debug`), `CapturedJa4` (+ `from_runtime_tls`), `Ja4Source`, `Ja4Unavailable`.
- [ ] `pub mod ja4;` in `lib.rs`.
- [ ] `cargo test -p edgezero-core ja4`.

### Task 2 — `RequestIngress` and `RequestContext` (spec §5.4–5.5)

Files: `crates/edgezero-core/src/ingress.rs`, `crates/edgezero-core/src/context.rs`.

- [ ] Add `ja4: Option<CapturedJa4>` field, `with_ja4`, `ja4()`; `new` sets `None`; `Debug` prints `ja4` (redacted through `Ja4Fingerprint`).
- [ ] Add `RequestContext::ja4()`.
- [ ] Tests: default `None`, round-trip, `Debug` redaction; context `None` without ingress and stored value with it.
- [ ] `cargo test -p edgezero-core`.

### Task 3 — Adapters (spec §6)

- [ ] Fastly `capture_request_ingress`: `.with_ja4(CapturedJa4::from_runtime_tls(req.get_tls_ja4()))`. Test (Viceroy): non-client request → `Unavailable(Missing)` even with a `ja4` header.
- [ ] Cloudflare `into_core_request`: read `req.cf()?.bot_management()?.ja4()` before body read. Browser contract test: no `cf` → `Unavailable(Missing)` with a `ja4` header present.
- [ ] Spin `capture_request_ingress`: `.with_ja4(Unavailable(NotExposed))`. Native colocated test.
- [ ] Axum `capture_request_ingress`: `.with_ja4(Unavailable(NotExposed))`. Test with `ja4` and `cloudfront-viewer-ja4-fingerprint` headers.

### Task 4 — Documentation (spec §11)

- [ ] `docs/guide/adapters/overview.md`: "Client TLS fingerprint (JA4)" section with example, capability matrix, trust model, FoxIO attribution.
- [ ] `fastly.md`: point raw-request section at `ctx.ja4()`.
- [ ] `cloudflare.md`: Enterprise Bot Management note.
- [ ] `spin.md`, `axum.md`: one-line `NotExposed` note.

### Task 5 — Verification

- [ ] `cargo fmt --all -- --check`
- [ ] `cargo clippy --workspace --all-targets --all-features -- -D warnings`
- [ ] `cargo test --workspace --all-targets`
- [ ] `cargo check --workspace --all-targets --features "fastly cloudflare spin"`
- [ ] `cargo check -p edgezero-adapter-spin --target wasm32-wasip2 --features spin`
- [ ] Docs Prettier.
- [ ] Manual (record in PR): Fastly HTTPS → `Captured`, HTTP → `Missing`; Cloudflare with/without Bot Management.
