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

- [x] Write colocated tests first: FoxIO example + `q`/`d`/`i`/zero sections/uppercase ALPN accepted; section accessors; rejection list (empty, 35/37 bytes, whitespace, uppercase hex, separators, unknown protocol/SNI, non-digit counts, non-ASCII, JA3 hash, `ja4_r`); `from_runtime_tls` for `None`/valid/invalid; redacted `Debug`; error text never contains input.
- [x] Implement `Ja4Fingerprint` (`parse`, `as_str`, `summary`, `cipher_hash`, `extension_hash`, redacted `Debug`), `CapturedJa4` (+ `from_runtime_tls`), `Ja4Source`, `Ja4Unavailable`.
- [x] `pub mod ja4;` in `lib.rs`.
- [x] `cargo test -p edgezero-core ja4`.

### Task 2 — `RequestIngress` and `RequestContext` (spec §5.4–5.5)

Files: `crates/edgezero-core/src/ingress.rs`, `crates/edgezero-core/src/context.rs`.

- [x] Add `ja4: Option<CapturedJa4>` field, `with_ja4`, `ja4()`; `new` sets `None`; `Debug` prints `ja4` (redacted through `Ja4Fingerprint`).
- [x] Add `RequestContext::ja4()`.
- [x] Tests: default `None`, round-trip, `Debug` redaction; context `None` without ingress and stored value with it.
- [x] `cargo test -p edgezero-core`.

### Task 3 — Adapters (spec §6)

- [x] Fastly `capture_request_ingress`: `.with_ja4(CapturedJa4::from_runtime_tls(req.get_tls_ja4()))`. Test (Viceroy): non-client request → `Unavailable(Missing)` even with a `ja4` header.
- [x] Cloudflare `into_core_request`: read `req.cf()?.bot_management()?.ja4()` before body read. Browser contract test: no `cf` → `Unavailable(Missing)` with a `ja4` header present.
- [x] Spin `capture_request_ingress`: `.with_ja4(Unavailable(NotExposed))`. Native colocated test.
- [x] Axum `capture_request_ingress`: `.with_ja4(Unavailable(NotExposed))`. Test with `ja4` and `cloudfront-viewer-ja4-fingerprint` headers.

### Task 4 — Documentation (spec §11)

- [x] `docs/guide/adapters/overview.md`: "Client TLS fingerprint (JA4)" section with example, capability matrix, trust model, FoxIO attribution.
- [x] `fastly.md`: point raw-request section at `ctx.ja4()`.
- [x] `cloudflare.md`: Enterprise Bot Management note.
- [x] `spin.md`, `axum.md`: one-line `NotExposed` note.

### Task 5 — Verification

- [x] `cargo fmt --all -- --check`
- [x] `cargo clippy --workspace --all-targets --all-features -- -D warnings`
- [x] `cargo test --workspace --all-targets`
- [x] `cargo check --workspace --all-targets --features "fastly cloudflare spin"`
- [x] `cargo check -p edgezero-adapter-spin --target wasm32-wasip2 --features spin`
- [x] Docs Prettier.
- [ ] Manual (record in PR): Fastly HTTPS → `Captured`, HTTP → `Missing`; Cloudflare with/without Bot Management.

## Verification log (2026-11-13)

| Check                                                                   | Result               |
| ----------------------------------------------------------------------- | -------------------- |
| `cargo fmt --all -- --check`                                            | pass                 |
| `cargo clippy --workspace --all-targets --all-features -- -D warnings`  | pass (rust 1.98.1)   |
| Cloudflare / Fastly clippy on their wasm targets                        | pass                 |
| `cargo test --workspace --all-targets`                                  | 1559 passed          |
| `cargo check --workspace --all-targets --features "fastly cloudflare spin"` | pass             |
| `cargo check -p edgezero-adapter-spin --target wasm32-wasip2 --features spin` | pass           |
| Fastly lib tests, Viceroy (`wasm32-wasip1`)                             | 98 passed            |
| Cloudflare contract tests, `wasm-bindgen-test-runner` + Chromium        | 14 passed            |
| Spin contract tests, wasmtime 48.0.1                                    | 12 passed            |
| Docs Prettier                                                           | pass                 |

**Deviation from spec §10 (Spin).** The spec asks for a wasmtime contract test
through `into_core_request` with a `ja4` header. `spin_sdk::http::Request`
wraps a WASI-HTTP incoming-request resource that only a Spin host can create,
so plain wasmtime cannot construct one. `capture_request_ingress` takes only
the URI, so headers cannot influence the JA4 result; the native colocated test
(`ingress_reports_ja4_not_exposed`) covers the behaviour.
