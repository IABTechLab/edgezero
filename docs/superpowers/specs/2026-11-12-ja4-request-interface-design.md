# Standard JA4 request interface

**Status:** Draft for review. No implementation has started.
**Date:** 2026-11-12.
**Issue:** [#283 Expose JA4 through a standard request interface](https://github.com/IABTechLab/edgezero/issues/283).
**Baseline:** EdgeZero `origin/main` `e11ea308`, which includes the request ingress metadata from [#403](https://github.com/IABTechLab/edgezero/pull/403).
**Related:** [#306](https://github.com/IABTechLab/edgezero/pull/306) (Fastly raw-request extension hook), [#409](https://github.com/IABTechLab/edgezero/pull/409) (Axum trusted proxy metadata, open), [#439](https://github.com/IABTechLab/edgezero/pull/439) (pluggable Geo providers and protected CloudFront ingress, open).

## 1. Problem and outcome

The only way for an EdgeZero application to read the client's JA4 TLS fingerprint today is the Fastly-only raw-request hook, `run_app_with_request_extensions`. The documented example (`docs/guide/adapters/fastly.md`, "Capturing raw-request signals") has each application define its own `Ja4(String)` type and call `fastly::Request::get_tls_ja4()` directly. Cloudflare, Spin and Axum have no equivalent. An application also cannot tell "this platform never supplies JA4" apart from "this request has no JA4".

**Outcome.** Handlers, middleware and pre-dispatch hooks read one adapter-populated value:

```rust
use edgezero_core::ja4::{CapturedJa4, Ja4Unavailable};

match ctx.ja4() {
    Some(CapturedJa4::Captured { fingerprint, source }) => {
        // fingerprint.as_str(), .summary(), .cipher_hash(), .extension_hash()
    }
    Some(CapturedJa4::Unavailable(Ja4Unavailable::NotExposed)) => { /* platform can't supply JA4 */ }
    Some(CapturedJa4::Unavailable(_)) => { /* none for this request, or the provider value was rejected */ }
    None => { /* the adapter made no claim (custom adapter, hand-built request) */ }
}
```

The value is either a validated fingerprint with its provenance, or an explicit reason it is unavailable. It is never derived from request headers.

## 2. Requirements

From the issue's "Done when":

| ID  | Requirement                                                                                  |
| --- | -------------------------------------------------------------------------------------------- |
| R1  | Add and document one standard, provider-neutral JA4 interface.                               |
| R2  | Populate it on adapters whose trusted provider APIs expose JA4.                              |
| R3  | Return explicit absence or unsupported behaviour on other adapters.                          |
| R4  | Cover present, absent and unsupported paths with core and adapter tests.                     |
| R5  | Preserve provenance and absence semantics; never infer a fingerprint from client-supplied headers. |

Additional constraints from `CLAUDE.md` and the workspace configuration:

- **WASM-compatible core.** No new dependencies and no Tokio in core or adapter crates.
- **Strict lints.** The workspace denies the whole clippy `restriction` group. That rules out `string_slice`, `indexing_slicing`, `unwrap_used`, `expect_used` and `arithmetic_side_effects`, and requires `#[inline]` on public items.
- **Small changes.** Each change touches as little existing code as possible.

### Non-goals

- Other JA4+ methods (JA4S, JA4H, JA4L, JA4X, JA4SSH, JA4T, JA4D, …). See §3.2.
- JA3, and Fastly's proprietary HTTP/2 and original-header fingerprints. These remain reachable through the existing Fastly hook.
- Lookups against FoxIO's JA4DB or any other fingerprint database.
- The unhashed and original-order variants (`ja4_r`, `ja4_o`, `ja4_ro`). No provider exposes them.
- Computing JA4 inside EdgeZero, and accepting JA4 from a trusted proxy header. Both are follow-ups (§13).
- A dedicated `FromRequest` extractor. `RequestContext::ja4()` is enough, and an extractor can be added later without breaking anything.

## 3. Background

### 3.1 JA4 format

JA4 is defined by FoxIO in [JA4 technical details](https://github.com/FoxIO-LLC/ja4/blob/main/technical_details/JA4.md). It is a 36-character string in three `_`-separated sections, for example `t13d1516h2_8daaf6152771_e5627efa2ab1`:

| Section | FoxIO name | Content                                                                                                                                                                         |
| ------- | ---------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `a`     | `JA4_a`    | 10 readable characters: protocol (`t` TLS, `q` QUIC, `d` DTLS), TLS version (`13`, `12`, …, `s3`, `d1`, `00`), SNI (`d` domain, `i` none), cipher count, extension count, ALPN first and last character |
| `b`     | `JA4_b`    | first 12 lowercase hex characters of the SHA-256 of the sorted cipher list, or `000000000000` if empty                                                                          |
| `c`     | `JA4_c`    | first 12 lowercase hex characters of the SHA-256 of the sorted extensions (excluding SNI and ALPN) and signature algorithms, or `000000000000` if empty                         |

FoxIO recommends matching on individual sections, for example `a` + `c`, which stays stable when a client only reorders or changes ciphers. The API therefore exposes each section.

### 3.2 FoxIO licensing and project status

| Topic           | Finding                                                                                                                                                                                                                                       | Consequence                                                                                                                       |
| --------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------- |
| Licence         | JA4 (TLS client) is BSD-3-Clause. All other JA4+ methods are under the FoxIO License 1.1, which forbids monetization without an OEM licence ([License FAQ](https://github.com/FoxIO-LLC/ja4/blob/main/License%20FAQ.md)).                     | EdgeZero ships JA4 only. This change passes provider strings through and vendors no FoxIO code, so it needs only attribution.     |
| Trademark       | "JA4+" is a FoxIO trademark.                                                                                                                                                                                                                  | Docs credit FoxIO and describe compatibility only.                                                                                |
| Releases        | [v1.0.0](https://github.com/FoxIO-LLC/ja4/releases/tag/v1.0.0) (2026-08-14) replaced the Zeek scripts with a C++ plugin. [v1.0.1](https://github.com/FoxIO-LLC/ja4/releases/tag/v1.0.1) (2026-08-31) fixed JA4/JA4H edge cases, including ignoring GREASE values in signature algorithms. No change to the JA4 format was announced. | Providers compute JA4 with their own implementations. The API records the source and does not claim that two providers produce identical values for the same client. |
| Rust library    | FoxIO's Rust code is a tshark-based command-line tool, not a library crate. The third-party `ja4` crate (0.3.0, published 2026-11-06) lists no repository.                                                                                    | No dependency is needed or added.                                                                                                 |
| JA4DB           | FoxIO runs a hosted fingerprint database. No published licence for bulk data was found.                                                                                                                                                      | Out of scope; applications may integrate it themselves.                                                                           |

### 3.3 Platform capabilities

Verified against the SDK versions pinned in `Cargo.lock` on the baseline.

| Platform               | JA4 source                                                                  | Availability                                                                                                                    |
| ---------------------- | --------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------- |
| Fastly (`fastly` 0.13.1) | `fastly::Request::get_tls_ja4() -> Option<&str>`                          | Client requests over TLS. `None` for plain HTTP (including Viceroy's local server) and non-client requests.                                               |
| Cloudflare (`worker` 0.8.5) | `Request::cf()?.bot_management()?.ja4() -> Option<String>`             | Only on zones with Enterprise Bot Management. `None` otherwise, and in local development and tests, which have no `cf` object. |
| Spin (`spin-sdk` 7.0.0) | none                                                                       | WASI-HTTP gives the component method, URI, headers and body only. The runtime's `spin-*` headers carry no TLS data.            |
| Axum (native)          | none                                                                        | The adapter listens on plain TCP and never sees a TLS handshake. #409's design explicitly excludes an incoming TLS implementation. |

## 4. Approaches considered

| Approach                                                                         | Assessment                                                                                                                                                                                                                                                                       | Decision        |
| -------------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | --------------- |
| **A. Field on `RequestIngress`, types in a new `edgezero_core::ja4` module**     | `RequestIngress` is already adapter-owned metadata whose absence means "capability unknown". Every adapter already builds it from the native request, and the Fastly adapter inserts it after merging the application's hook values, so application hooks cannot replace it. | **Chosen**      |
| B. Field on each adapter's context (`FastlyRequestContext`, …)                  | Not provider-neutral. `FastlyRequestContext` is inserted inside `convert_request`, *before* the hook values are merged, so a hook could silently replace it.                                                                                                                     | Rejected        |
| C. Separate core request extension                                               | Workable, but each adapter would need to repeat the insert-after-the-hook ordering that `RequestIngress` already guarantees.                                                                                                                                                     | Rejected        |
| D. Compute JA4 inside EdgeZero from raw ClientHello bytes                        | More capable (adds the unhashed variants), but a larger change that needs FoxIO conformance vectors. Only Fastly exposes the raw bytes.                                                                                                                                        | Follow-up (§13) |

The types live in their own module rather than in `ingress.rs`, because fingerprint parsing is a self-contained unit with its own tests, and `ingress.rs` stays focused on request-fidelity facts.

## 5. Design

### 5.1 Module `edgezero_core::ja4`

New file `crates/edgezero-core/src/ja4.rs`, declared as `pub mod ja4;` in `crates/edgezero-core/src/lib.rs`. All public items carry `#[inline]` and `#[must_use]` where applicable, as the workspace lints require.

```rust
//! Client JA4 TLS fingerprints captured by adapters.
//!
//! JA4 is a TLS client fingerprinting method by FoxIO, licensed under
//! BSD-3-Clause. JA4+ is a trademark of FoxIO.

/// A JA4 TLS client fingerprint, validated as `a_b_c` (§5.2).
#[derive(Clone, PartialEq, Eq)]
pub struct Ja4Fingerprint(Box<str>);

impl Ja4Fingerprint {
    /// Validates `value` against §5.2 without normalising it.
    ///
    /// # Errors
    /// Returns `EdgeError::bad_request("invalid JA4 fingerprint")` for any
    /// value that does not match. The error never contains the input.
    pub fn parse(value: &str) -> Result<Self, EdgeError>;

    /// The full 36-character fingerprint.
    pub fn as_str(&self) -> &str;

    /// FoxIO `JA4_a`: protocol, TLS version, SNI, cipher and extension counts, ALPN.
    pub fn summary(&self) -> &str;

    /// FoxIO `JA4_b`: truncated SHA-256 of the sorted cipher suites.
    pub fn cipher_hash(&self) -> &str;

    /// FoxIO `JA4_c`: truncated SHA-256 of the sorted extensions and signature algorithms.
    pub fn extension_hash(&self) -> &str;
}

/// What an adapter captured for this request.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CapturedJa4 {
    Captured {
        fingerprint: Ja4Fingerprint,
        source: Ja4Source,
    },
    Unavailable(Ja4Unavailable),
}

impl CapturedJa4 {
    /// Maps a runtime TLS accessor's result (§5.3).
    pub fn from_runtime_tls(value: Option<&str>) -> Self;
}

/// Where a captured fingerprint came from.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum Ja4Source {
    /// Computed by the runtime that terminated the client's TLS connection.
    RuntimeTls,
}

/// Why no fingerprint was captured.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum Ja4Unavailable {
    /// The runtime offers no JA4 API.
    NotExposed,
    /// The runtime exposes JA4 but returned none for this request.
    Missing,
    /// The runtime returned a value that failed validation; it was discarded.
    Malformed,
}
```

`Ja4Source` and `Ja4Unavailable` are `#[non_exhaustive]` because the follow-ups in §13 add variants (`TrustedProxyHeader`, `Computed`). `CapturedJa4` is exhaustive: "captured or unavailable" is the whole space.

### 5.2 Validation grammar

`Ja4Fingerprint::parse` accepts a value only if all of the following hold. Any non-ASCII byte fails one of the per-position checks, so no separate UTF-8 check is needed.

| Byte positions | Rule                                       | Meaning                          |
| -------------- | ------------------------------------------ | -------------------------------- |
| whole value    | exactly 36 bytes                           | checked first; bounds the work   |
| `0`            | `t`, `q` or `d`                            | protocol                         |
| `1..3`         | ASCII lowercase letter or digit            | TLS version code                 |
| `3`            | `d` or `i`                                 | SNI present or absent            |
| `4..8`         | ASCII digits                               | cipher and extension counts      |
| `8..10`        | ASCII letter (either case) or digit        | ALPN first and last character    |
| `10`, `23`     | `_`                                        | section separators               |
| `11..23`       | lowercase hex (`0-9`, `a-f`)               | `JA4_b`                          |
| `24..36`       | lowercase hex (`0-9`, `a-f`)               | `JA4_c`                          |

The rules are strict about shape but tolerant of the values inside section `a`. A future TLS version code is still accepted, while anything that is not a JA4 is rejected, including JA3 hashes, `ja4_r` strings and surrounding whitespace. Uppercase hex in `b` or `c` is rejected, not lowercased, because FoxIO defines those sections as lowercase.

**Implementation note.** The workspace lints forbid string slicing and indexing. Validation iterates over `value.bytes().enumerate()`, and the section accessors use `str::get` (for example `self.0.get(0..10).unwrap_or_default()`). After successful validation, `get` always returns `Some`.

### 5.3 Runtime mapping

`CapturedJa4::from_runtime_tls` is the single mapping used by every runtime-backed adapter:

| Runtime result                         | `CapturedJa4`                                                   |
| -------------------------------------- | --------------------------------------------------------------- |
| `None`                                 | `Unavailable(Missing)`                                          |
| `Some(v)` and `Ja4Fingerprint::parse(v)` succeeds | `Captured { fingerprint, source: Ja4Source::RuntimeTls }` |
| `Some(v)` and parsing fails            | `Unavailable(Malformed)`; `v` is dropped and never logged       |

`Malformed` is a separate variant so that a change in a provider's output format shows up as a distinct, observable state instead of reading as "no JA4".

### 5.4 `RequestIngress` integration

In `crates/edgezero-core/src/ingress.rs`:

```rust
pub struct RequestIngress {
    // existing fields unchanged
    ja4: Option<CapturedJa4>,
}

impl RequestIngress {
    // `new` is unchanged and sets `ja4: None`.

    /// Records the adapter's JA4 capture.
    pub fn with_ja4(self, ja4: CapturedJa4) -> Self;

    /// The adapter's JA4 capture. `None` means the adapter made no claim.
    pub const fn ja4(&self) -> Option<&CapturedJa4>;
}
```

- **`None` means unknown.** There is deliberately no `Unknown` variant. An adapter that never calls `with_ja4`, such as a custom adapter or a hand-built test request, leaves the field unset. This matches the module's existing rule that missing metadata means unknown capability.
- **`Debug`.** `RequestIngress`'s `Debug` gains a `ja4` field. It shows the variant and source but never the fingerprint (§5.6).

### 5.5 Access from handlers, middleware and hooks

In `crates/edgezero-core/src/context.rs`:

```rust
impl RequestContext {
    /// The adapter's JA4 capture for this request, read from `RequestIngress`.
    /// `None` if the request has no ingress metadata or the adapter made no claim.
    pub fn ja4(&self) -> Option<&CapturedJa4> {
        self.request()
            .extensions()
            .get::<RequestIngress>()
            .and_then(RequestIngress::ja4)
    }
}
```

- **Handlers and middleware** use `ctx.ja4()`.
- **Pre-dispatch hooks** (`PreDispatchHook::handle(&self, request: &mut Request)`, from #403) receive the `Request` rather than a `RequestContext`. They read the same value with `request.extensions().get::<RequestIngress>().and_then(RequestIngress::ja4)`.

### 5.6 Privacy

A JA4 fingerprint helps identify a client, and the `ingress` module already keeps identifying data out of `Debug` output.

- **`Ja4Fingerprint`** implements `Debug` as `Ja4Fingerprint([redacted])` and does not implement `Display` or `serde::Serialize`. Callers read the value explicitly with `as_str()` or a section accessor.
- **`CapturedJa4`** derives `Debug`, so it prints `Captured { fingerprint: Ja4Fingerprint([redacted]), source: RuntimeTls }`.
- **`parse` errors** never include the input.

### 5.7 Trust model

- **Provider values only.** A `Captured` value with source `RuntimeTls` comes only from the provider SDK accessor, never from a request header. R5 is met by construction: no adapter reads a header for JA4.
- **Ordering in the Fastly adapter.** The ingress snapshot, and therefore JA4, is inserted after the application's raw-request hook values are merged (`dispatch_with_handles` and `into_core_request_with_registries_and_ingress` in `crates/edgezero-adapter-fastly/src/request.rs`). A hook that inserts its own `RequestIngress` is overwritten.
- **Out of scope for the guarantee.** It does not protect against the application's own code. A caller of `dispatch_with_registries_and_ingress` supplies its own snapshot, and middleware or a pre-dispatch hook can mutate request extensions. This matches the existing `RequestIngress` contract: "Sources and preservation guarantees are caller assertions."

## 6. Adapter requirements

| Adapter    | Location on baseline                                                                         | Change                                                                                                                                                                                     |
| ---------- | -------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| Fastly     | `capture_request_ingress`, `crates/edgezero-adapter-fastly/src/request.rs:601`               | Append `.with_ja4(CapturedJa4::from_runtime_tls(req.get_tls_ja4()))` to the returned `RequestIngress`. Every Fastly entry point builds ingress through this function or receives a caller snapshot. |
| Cloudflare | `into_core_request`, `crates/edgezero-adapter-cloudflare/src/request.rs:279` (before `req.bytes()` at `:289`) | `let ja4 = req.cf().and_then(\|cf\| cf.bot_management()).and_then(\|bot\| bot.ja4());` then `RequestIngress::new(…).with_ja4(CapturedJa4::from_runtime_tls(ja4.as_deref()))`.            |
| Spin       | `capture_request_ingress`, `crates/edgezero-adapter-spin/src/ingress.rs:7`                   | `.with_ja4(CapturedJa4::Unavailable(Ja4Unavailable::NotExposed))`.                                                                                                                         |
| Axum       | `capture_request_ingress`, `crates/edgezero-adapter-axum/src/request.rs:75`                  | `.with_ja4(CapturedJa4::Unavailable(Ja4Unavailable::NotExposed))`.                                                                                                                         |

- `run_app_with_request_extensions` and `FastlyRequestContext` are unchanged. The hook remains the way to reach other Fastly-only signals such as `get_client_h2_fingerprint()`.
- No adapter reads a request header for JA4.

## 7. Capability matrix

| Adapter    | HTTPS client request                                 | Plain HTTP, local development, tests | Provider value fails validation |
| ---------- | ---------------------------------------------------- | ------------------------------------ | ------------------------------- |
| Fastly     | `Captured { RuntimeTls }`                            | `Unavailable(Missing)`               | `Unavailable(Malformed)`        |
| Cloudflare | `Captured { RuntimeTls }` with Enterprise Bot Management; otherwise `Unavailable(Missing)` | `Unavailable(Missing)` | `Unavailable(Malformed)`        |
| Spin       | `Unavailable(NotExposed)`                            | `Unavailable(NotExposed)`            | n/a                             |
| Axum       | `Unavailable(NotExposed)`                            | `Unavailable(NotExposed)`            | n/a                             |
| Custom adapter or hand-built request | `None`                     | `None`                               | n/a                             |

Fastly's `q` (QUIC/HTTP/3) fingerprints are accepted as-is. Whether Fastly reports JA4 for HTTP/3 connections is unverified (§14).

## 8. Why Spin and Axum report `NotExposed`

- **Spin.** The component receives requests through WASI-HTTP: method, URI, headers and body. The runtime adds `spin-*` headers (`spin-client-addr`, `spin-full-url`, …; see `crates/edgezero-adapter-spin/src/request.rs:58`), none of which carry TLS data. On hosted Spin and SpinKube, TLS ends at the platform ingress. Even when `spin up` terminates TLS itself, the ClientHello is never passed to the component.
- **Axum.** The adapter serves plain HTTP behind a TLS-terminating proxy, which is the deployment model #409 is built on. It never sees a ClientHello.
- **Why not read a header.** A header such as `cloudfront-viewer-ja4-fingerprint` reaches these adapters, but without a check on which hop set it, any client could forge it. That breaks R5. The trusted-proxy path in §13.1 makes such a header usable once #409 or #439 provides that check.

## 9. Edge cases

| Case                                                              | Behaviour                                                                 |
| ----------------------------------------------------------------- | ------------------------------------------------------------------------- |
| Fastly non-client request (e.g. built with `FastlyRequest::new`)   | SDK returns `None` → `Unavailable(Missing)`.                              |
| Cloudflare request without `cf` (wrangler dev, workerd, browser tests) | `cf()` is `None` → `Unavailable(Missing)`.                            |
| Cloudflare zone without Bot Management                            | `bot_management()` is `None` → `Unavailable(Missing)`.                   |
| Provider value with uppercase hex, whitespace or the wrong length | `Unavailable(Malformed)`; the value is dropped.                           |
| Client sends a `ja4` or `cloudfront-viewer-ja4-fingerprint` header | Ignored by every adapter; the header stays in the `HeaderMap` as an ordinary header. |
| Fastly raw-request hook inserts its own `RequestIngress`          | Overwritten by the adapter's snapshot.                                    |
| App code mutates extensions in middleware or a pre-dispatch hook  | Not prevented (§5.7).                                                     |

## 10. Testing and acceptance

| Layer                   | Runner                                   | Cases                                                                                                                                                                                                                                                                                                                                                         |
| ----------------------- | ---------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Core `ja4.rs`           | native `cargo test -p edgezero-core`     | • Accepts FoxIO's example `t13d1516h2_8daaf6152771_e5627efa2ab1`, a `q` value, a `d` value, `i` SNI, `000000000000` sections and uppercase ALPN characters.<br>• Section accessors return `t13d1516h2`, `8daaf6152771` and `e5627efa2ab1`.<br>• Rejects: empty, 35 and 37 bytes, leading or trailing whitespace, uppercase hex in `b` or `c`, wrong or missing separators, an unknown protocol or SNI letter, non-digit counts, a non-ASCII value of the right byte length, a 32-character JA3 hash and a `ja4_r` string.<br>• `from_runtime_tls` for `None`, valid and invalid input.<br>• `Debug` output of `Ja4Fingerprint`, `CapturedJa4` and `RequestIngress` never contains the fingerprint, and `parse` errors never contain the input. |
| Core `ingress.rs`, `context.rs` | native                           | • `RequestIngress::new(…).ja4()` is `None`; `with_ja4` round-trips.<br>• `RequestContext::ja4()` is `None` without `RequestIngress` and returns the stored value when present.                                                                                                                                                                                |
| Fastly                  | Viceroy (`wasm32-wasip1`)                | • `capture_request_ingress(&FastlyRequest::new(…))` gives `Some(Unavailable(Missing))`.<br>• Through `dispatch_with_registries`, a raw-request hook that inserts a `RequestIngress` with a fake captured JA4 does not change what the handler sees (modelled on the existing hook-override test at `crates/edgezero-adapter-fastly/src/request.rs:1153`).<br>• A client `ja4` header does not produce `Captured`.                                                 |
| Cloudflare              | `wasm-bindgen-test` in the browser       | `into_core_request` on a `web_sys::Request` (no `cf`) gives `Some(Unavailable(Missing))`, including when the request carries a `ja4` header.                                                                                                                                                                                                                    |
| Spin                    | native colocated test in `ingress.rs`, plus wasmtime contract test | `capture_request_ingress` gives `Some(Unavailable(NotExposed))`. It takes only the URI, so headers cannot influence it; the wasmtime contract test confirms the same through `into_core_request` with a `ja4` header present.                                                                 |
| Axum                    | native colocated test in `request.rs`    | `into_core_request` gives `Some(Unavailable(NotExposed))`, including when the request carries `ja4` and `cloudfront-viewer-ja4-fingerprint` headers.                                                                                                                                                                                                         |

**Manual verification.** This can't run in CI because it needs live provider accounts. Record the results in the pull request.

- A Fastly service over HTTPS returns `Captured`, and over HTTP returns `Missing`. This also confirms Fastly emits lowercase hex.
- A Cloudflare zone with Bot Management returns `Captured`; one without returns `Missing`.

**Acceptance against requirements:**

| Requirement | Met by                                                                 |
| ----------- | ---------------------------------------------------------------------- |
| R1          | §5 API and §11 documentation                                           |
| R2          | Fastly and Cloudflare (§6)                                             |
| R3          | Spin and Axum `NotExposed`; `Missing` and `Malformed` on Fastly and Cloudflare |
| R4          | §10 test matrix                                                        |
| R5          | §5.7 and the header-ignored tests in §10                               |

All five CI gates in `CLAUDE.md` must pass, plus the docs Prettier and ESLint checks.

## 11. Documentation

| File                                | Change                                                                                                                                                                                                                          |
| ----------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `docs/guide/adapters/overview.md`   | New section "Client TLS fingerprint (JA4)" after "Inbound request fidelity": the `ctx.ja4()` example, the capability matrix from §7, the trust model from §5.7 and the FoxIO attribution ("JA4 is a TLS client fingerprinting method by FoxIO, licensed under BSD-3-Clause. JA4+ is a trademark of FoxIO. Other JA4+ methods are not provided."). |
| `docs/guide/adapters/fastly.md`     | In "Capturing raw-request signals (JA4, H2 fingerprint)", replace the `Ja4(String)` example with a pointer to `ctx.ja4()`, and rename the section to cover the HTTP/2 fingerprint and other raw-request signals.              |
| `docs/guide/adapters/cloudflare.md` | Note that JA4 needs Enterprise Bot Management and is `Missing` otherwise.                                                                                                                                                      |
| `docs/guide/adapters/spin.md`, `axum.md` | One line: JA4 is `NotExposed`, with a link to the overview section.                                                                                                                                                       |

## 12. Compatibility

- **Additive only.** Existing `RequestIngress::new` callers compile unchanged.
- **Existing apps keep working.** Apps that insert their own `Ja4(String)` through the Fastly hook are unaffected and can migrate when convenient.
- **No dependencies or feature flags.** The workspace version is `0.1.0`, and adding variants later to the `#[non_exhaustive]` enums is non-breaking.
- **Merge order with #439.** #439 also edits `ingress.rs`, `context.rs` and adapter `request.rs` files. Whichever merges second rebases; the changes here are small and additive.

## 13. Follow-ups

Each needs its own spec or plan, and none blocks this change.

### 13.1 Axum: JA4 from a trusted proxy

- **Prerequisite.** #409's `TrustedProxyPolicy` / `CheckedProxyMetadata`, or #439's protected-CloudFront check, is on `main`.
- **Contract.**
  - When the direct peer passes that check, read one operator-configured header containing exactly one field. Example sources: CloudFront `CloudFront-Viewer-JA4-Fingerprint`, FoxIO's nginx module, the Apache Traffic Server JA4 plugin and HAProxy's ja4 module.
  - Validate the value with `Ja4Fingerprint::parse` and record it as a new `Ja4Source::TrustedProxyHeader`.
  - In every other case, keep `NotExposed`. A header from an unchecked peer is never captured.
- **Open question.** Should "header present but peer not trusted" get its own `Ja4Unavailable` variant?

### 13.2 Spin: trusted proxy or upstream runtime support

- **Option 1.** Apply the same trusted-proxy contract as §13.1. This first needs a Spin trust configuration, which does not exist today.
- **Option 2.** Propose to the Spin project that the runtime add a JA4 header when it terminates TLS, the same way it adds `spin-client-addr`.

### 13.3 EdgeZero-computed JA4

- **What.** A small in-tree ClientHello parser that uses the workspace `sha2` dependency, fed by Fastly's `get_tls_client_hello()`. It would add `Ja4Source::Computed` and possibly the unhashed variants.
- **Testing.** Against FoxIO's published sample captures, cross-checked against `get_tls_ja4()`.
- **Prerequisite.** Confirm whether the SDK returns the full TLS record or only the handshake message.

## 14. Risks and open questions

| Item                                                                 | Mitigation                                                                                                   |
| -------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------ |
| A provider emits a format the strict grammar rejects (for example uppercase hex) | Surfaces as `Malformed` instead of silent loss; manual verification (§10) checks real output before release. |
| Providers' JA4 implementations differ in edge cases (FoxIO v1.0.1 fixed several) | `Ja4Source` records provenance; docs say values are provider-computed and may differ across providers.       |
| Whether Fastly reports JA4 for HTTP/3 (QUIC) connections            | Unverified. Either result is handled; confirm during manual verification and document it.                   |
| Cloudflare `cf` access throwing instead of returning `None`         | `worker` exposes both accessors as `Option`. The browser test covers the missing-`cf` path.                 |

## 15. Decision log

| Decision                                         | Reason                                                                                                       |
| ------------------------------------------------ | ------------------------------------------------------------------------------------------------------------ |
| Store on `RequestIngress`                        | Existing adapter-owned provenance record; already inserted after hook values are merged (§4).               |
| Separate `ja4` module                            | Fingerprint parsing is a self-contained, independently testable unit.                                       |
| `Option<&CapturedJa4>` instead of an `Unknown` variant | Matches "absence means unknown capability".                                                            |
| Separate `Malformed` variant                     | Makes provider format changes observable; follows #439's `GeoStatus::Error(InvalidAssertion)` precedent.    |
| Strict shape, tolerant section `a`               | Rejects non-JA4 input without rejecting future TLS version codes.                                           |
| Redacted `Debug`, no `Display` or `Serialize`    | A fingerprint is a quasi-identifier; follows the `ingress` module's existing redaction.                     |
| Names follow `ingress.rs`                        | `CapturedTarget` → `CapturedJa4`; `TargetUnavailable::NotExposed` → `Ja4Unavailable::NotExposed`; `OriginSource::RuntimeUri` → `Ja4Source::RuntimeTls`. |
| JA4 only; no JA4+                                | FoxIO License 1.1 restricts commercial use of the other methods.                                            |
| No extractor                                     | YAGNI; `ctx.ja4()` meets R1, and an extractor can be added later.                                            |

## 16. References

- FoxIO: [JA4 repository](https://github.com/FoxIO-LLC/ja4), [JA4 technical details](https://github.com/FoxIO-LLC/ja4/blob/main/technical_details/JA4.md), [License FAQ](https://github.com/FoxIO-LLC/ja4/blob/main/License%20FAQ.md), [releases](https://github.com/FoxIO-LLC/ja4/releases), [ja4-nginx-module](https://github.com/FoxIO-LLC/ja4-nginx-module).
- Fastly: [`tls.client.ja4` VCL variable](https://www.fastly.com/documentation/reference/vcl/variables/client-connection/tls-client-ja4/); Rust SDK `fastly::Request::get_tls_ja4`.
- Cloudflare: [JA4 signals](https://blog.cloudflare.com/ja4-signals/); `worker::Cf::bot_management`.
- AWS: [CloudFront JA4 fingerprinting](https://aws.amazon.com/about-aws/whats-new/2024/10/amazon-cloudfront-ja4-fingerprinting), [CloudFront request headers](https://docs.aws.amazon.com/AmazonCloudFront/latest/DeveloperGuide/adding-cloudfront-headers.html).
- Apache Traffic Server: [JA4 Fingerprint Plugin](https://docs.trafficserver.apache.org/en/latest/admin-guide/plugins/ja4_fingerprint.en.html).
