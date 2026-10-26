# Pre-dispatch interception and inbound request fidelity implementation plan

**Goal:** Method-independent interception before router dispatch, with bounded,
confidential metadata that accurately describes runtime-visible request facts.

**Status:** Core feature implemented. This document records the implemented
surfaces, maintenance requirements and verification procedure.

**Contract:** [Design](../specs/2026-10-05-pre-dispatch-request-fidelity-design.md).

## Implemented surfaces

- `crates/edgezero-core/src/router.rs`: optional `PreDispatchHook` before lookup,
  state, introspection, context and middleware. Register a value with
  `pre_dispatch_hook` or an Arc with `pre_dispatch_hook_arc`; duplicate
  registration panics. Continued method/path changes return an internal error;
  header, extension, body and query changes remain allowed. Futures may be
  non-Send; hook values are Send + Sync.
- `crates/edgezero-core/src/ingress.rs`: bounded target/origin metadata,
  infallible `RequestIngress::new`, fallible `with_header_overrides` with an
  internal error for duplicate names, redacted Debug, public authority/target
  limits and shared host/effective-port comparison. The four metadata enums are
  non-exhaustive so acquisition sources and guarantees can evolve. Authorities are requested
  hosts, not operator approval; port spelling is retained.
- Fastly `src/request.rs`: capture before native mutation, explicit snapshot
  conversion and `dispatch_with_registries_and_ingress(app, req, stores,
  ingress, extend)`. Capture reports target NotExposed before reading the SDK
  URL for origin. Ordinary runners use the same registry dispatcher.
- Cloudflare `src/request.rs`: original runtime method token and one runtime URL
  read; parse the captured copy with `worker::Url`; preserve runtime UTF-8
  header bytes. Bounded conversion errors become Worker errors through standard
  runners and the fetch wrapper returns 500.
- Axum `src/request.rs`: preserve incoming URI/headers, listener-established plain-HTTP
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
Generated `app!` routers cannot install a hook. A manual `Hooks` wrapper must
delegate manifest metadata and explicitly preserve routes, middleware, state and
manifest introspection; see the routing guide.

## Dependency refresh

The isolated fixture pins Fastly 0.13.1, Worker 0.8.5, wasm-bindgen 0.2.128 and
Spin SDK 7.0.0. Its lockfile must be regenerated with these pins together.
CI and the runner require worker-build 0.8.5 and Spin CLI 4.1.0. Rust, Node,
Viceroy and Wasmtime versions come from `.tool-versions`. The shared Spin pin
check includes this fixture. CI resolves fixture wasm-bindgen from its own lock,
and worker-build uses the npm-integrity-locked esbuild executable. The
worker-build version is declared once beside Spin CLI in
`[package.metadata.request-fidelity]`. SDK drift checks compare exact Fastly,
Worker, wasm-bindgen and Spin SDK pins against both lockfiles.
CI caches the actual native-runner and WASM-fixture target directories with
versioned cache keys; debug lint and release probe builds keep their profiles.

## Verification checklist

- Run core, affected adapter and native runner tests after code changes.
- Run workspace fmt, strict all-feature clippy and full workspace tests.
- Check workspace `fastly cloudflare spin` features and Spin wasm32-wasip2.
- Format/lint both isolated fixture workspaces; lint WASM fixture on each of
  wasm32-wasip1/Fastly, wasm32-unknown-unknown/Cloudflare and
  wasm32-wasip2/Spin with `--locked`.
- Execute browser Cloudflare, Viceroy Fastly and Wasmtime Spin contracts.
  Spin uses plain `wasmtime run`; full Spin lib-test artifacts additionally
  need Spin-specific host interfaces and are not plain-Wasmtime contracts.
- Run `./scripts/test-request-fidelity.sh all --record-observations` into a
  temporary file, inspect per-case differences, then replace the baseline.
  Run the ordinary comparison afterward. See the fixture README commands.
- Run docs lint, format and build; review the final diff independently.

## Baseline maintenance

The checked-in `tests/fixtures/request-fidelity/observations.json` records local
runtime observations. `./scripts/test-request-fidelity.sh all` compares them
exactly. Inspect differences when updating SDK or runtime pins; baseline results
are not production preservation guarantees. Record run-specific validation in
the PR description.

## Decision log

- Worker runtime UTF-8 strings must be copied as UTF-8; browser ByteString
  construction is not evidence of Worker wire bytes. Exact C3 A9/C4 80 assertions
  guard against corruption from a ByteString/Latin-1 conversion.
- Origin availability and hook dispatch prerequisites must pass before the
  UTF-8 regression can prove anything; assert the specific encoding error and
  include a passing valid verdict.
- Baseline failures print changed/added/removed cases. Child stdin is null, and
  direct child termination backs up external process-group signals before wait.
- The consumer owns visible-path classification, authentication, cookie policy,
  native shortcut ordering and terminal finalization. Its integration requires
  an immutable reviewed toolkit revision and tests on its own dependency graph.
- The worker-build binary uses a version-specific CI cache; cold installs build
  from source while the pinned release has no Ubuntu prebuilt.
- Opt-in capture remains separate work.
- No production capability or performance claim follows from local fixtures.
