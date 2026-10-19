# Pre-dispatch interception and inbound request fidelity implementation plan

**Goal:** Method-independent interception before router dispatch, with bounded,
confidential metadata that accurately describes runtime-visible request facts.

**Status:** Core feature implemented. This document supersedes earlier execution
notes, signatures, case counts and runtime flags; it is a maintenance and
verification checklist, not a chronological session log.

**Contract:** [Design](../specs/2026-10-05-pre-dispatch-request-fidelity-design.md).
**Branch:** `spec/pre-dispatch-request-fidelity`.

## Implemented surfaces

- `crates/edgezero-core/src/router.rs`: optional `PreDispatchHook` before lookup,
  state, introspection, context and middleware. Register a value with
  `pre_dispatch_hook` or an Arc with `pre_dispatch_hook_arc`; duplicate
  registration panics. Futures may be non-Send; hook values are Send + Sync.
- `crates/edgezero-core/src/request.rs`: bounded target/origin metadata,
  infallible `RequestIngress::new`, fallible `with_header_overrides` with an
  internal error for duplicate names, redacted Debug, public authority/target
  limits and shared host/effective-port comparison. Authorities are requested
  hosts, not operator approval; port spelling is retained.
- Fastly `src/request.rs`: capture before native mutation, explicit snapshot
  conversion and `dispatch_with_registries_and_ingress(app, req, stores,
  ingress, extend)`. Capture reports target NotExposed before reading the SDK
  URL for origin. Ordinary runners use the same registry dispatcher.
- Cloudflare `src/request.rs`: original runtime method token and one runtime URL
  read; parse the captured copy with `worker::Url`; preserve runtime UTF-8
  header bytes. Bounded conversion errors become Worker errors through standard
  runners and the fetch wrapper returns 500.
- Axum `src/request.rs`: preserve incoming URI/headers, trusted plain-HTTP
  transport binding and one consistent Host. Existing JSON buffering and
  non-JSON streaming remain in place.
- Spin `src/ingress.rs`: pure metadata helper shared by WASM conversion and host
  tests, separate from the WASM-only `src/request.rs`.
- `tests/fixtures/request-fidelity`: store-free runtime application and fixed
  boolean/count observations. `request-fidelity-runner` is an isolated native
  Rust supervisor; its Tokio dependency does not enter core or WASM adapters.

## Fidelity decisions

Axum, Cloudflare and Spin use `HeaderFidelity::RUNTIME_HEADER_MAP`: octets,
multiplicity and same-name order Unknown, global order Unavailable. Fastly uses
all Unknown. Its original-header names/count APIs provide partial ordering and
aggregate count but do not associate original fields with converted values.
Viceroy can lowercase/deduplicate names; production behavior needs verification.
No per-header preservation is inferred from aggregate count equality.

The hook cannot recover erased wire data, intercept an earlier native shortcut,
remove buffering or bypass outer response finalizers. Metadata capture remains
eager; changing it to lazy/opt-in requires separate lifecycle/performance work.
Generated `app!` routers cannot install a hook; use manual `Hooks::routes()`.

## Dependency refresh

The isolated fixture pins Fastly 0.13.1, Worker 0.8.5, wasm-bindgen 0.2.128 and
Spin SDK 7.0.0. Its lockfile must be regenerated with these pins together.
CI and the runner require worker-build 0.8.5 and Spin CLI 4.1.0. Rust, Node,
Viceroy and Wasmtime versions come from `.tool-versions`. The shared Spin pin
check includes this fixture. CI resolves fixture wasm-bindgen from its own lock,
and worker-build uses the npm-integrity-locked esbuild executable.

## Verification checklist

- [x] Run core, affected adapter and native runner tests after code changes.
- [x] Run workspace fmt, strict all-feature clippy and full workspace tests.
- [x] Check workspace `fastly cloudflare spin` features and Spin wasm32-wasip2.
- [x] Format/lint both isolated fixture workspaces; lint WASM fixture on each of
      wasm32-wasip1/Fastly, wasm32-unknown-unknown/Cloudflare and
      wasm32-wasip2/Spin with `--locked`.
- [x] Execute browser Cloudflare, Viceroy Fastly and Wasmtime Spin contracts.
      Spin uses plain `wasmtime run`; full Spin lib-test artifacts additionally
      need Spin-specific host interfaces and are not plain-Wasmtime contracts.
- [x] Run `./scripts/test-request-fidelity.sh all --record-observations` into a
      temporary file, inspect per-case differences, then replace the baseline.
      Run the ordinary comparison afterward. See the fixture README commands.
- [x] Run docs lint, format and build; review the final diff independently.

## Evidence and baseline provenance

The baseline contains 56 cases: Fastly 20, Cloudflare 19 and Spin 17, including
two direct-workerd no-hook routing regressions. Every case was freshly built and
recorded with the refreshed pins; the resulting JSON exactly matches the
checked-in baseline. The ordinary locked-build comparison also passes.
Earlier 47/54-case checkpoints and explicit Wasmtime flags are superseded by
the current registry and commands.

Spin 4.1.0 consumes chunked framing before component invocation: `chunked` and
`te-cl` now expose no Transfer-Encoding (count 0, chunked false), unlike Spin 4.0.
Fresh probes independently reproduce the [review's report](https://github.com/IABTechLab/edgezero/pull/403#pullrequestreview-5544176343).
Fastly's global-order boolean changes with the metadata correction, without
asserting stronger wire preservation. Other recorded outcomes remain unchanged.

## Current review validation

Rust 1.98.1 workspace tests pass 1,546 tests with zero failures and one ignored.
All five required workspace gates pass, including strict all-feature Clippy,
combined adapter feature compilation and Spin wasm32-wasip2 compilation.
Both isolated workspaces pass formatting and strict lint; the WASM fixture is
linted on all three target/feature combinations with its regenerated lock.
The native runner passes ten tests; its ignored direct-workerd routing test is
covered by the full runtime probe run.

Fastly/Viceroy passes seven WASM contracts and 97 runtime unit tests. Spin passes
12 contracts under plain Wasmtime 48.0.1. The complete 56-case runtime fixture
matrix passes with Viceroy 0.21.0, Spin 4.1.0, Worker/worker-build 0.8.5,
wasm-bindgen 0.2.128 and Node 24.20.0. Public matrices identify these tested
versions. Docs lint/format/build, dependency-pin checks, actionlint and whitespace
checks pass. All 12 Cloudflare browser contracts pass under the CI-pinned
Chrome/ChromeDriver 154.0.8037.92 with wasm-bindgen 0.2.128.

## Decision log

- Worker runtime UTF-8 strings must be copied as UTF-8; browser ByteString
  construction is not evidence of Worker wire bytes. Exact C3 A9/C4 80 assertions
  prevent the earlier encoding defect.
- Origin availability and hook dispatch prerequisites must pass before the
  UTF-8 regression can prove anything; assert the specific encoding error and
  include a passing valid verdict.
- Baseline failures print changed/added/removed cases. Child stdin is null, and
  direct child termination backs up external process-group signals before wait.
- The consumer owns visible-path classification, authentication, cookie policy,
  native shortcut ordering and terminal finalization. Its integration requires
  an immutable reviewed toolkit revision and tests on its own dependency graph.
- Prebuilt installers, cache redesign and opt-in capture remain separate work.
  No production capability or performance claim follows from local fixtures.
