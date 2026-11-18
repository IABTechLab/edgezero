# Cloudflare and Spin geo implementation plan

**Goal:** Satisfy #480 through existing Cloudflare/Axum integration and a configured authenticated ingress source on Spin.

**Architecture:** Reuse #502 platform geo and Axum binding. Add one non-secret Spin App configuration, private credential capture/removal, deadline-bounded secret lookup after admission, and the existing immutable Geo outcome handoff. Use fixed geographic/token headers and a Spin variable reference.

**Technology:** Pinned Rust 1.98.1, Node 24.20.0, worker/worker-build 0.8.5, wasm-bindgen 0.2.128, Spin SDK8 and runtime4.1.0, existing subtle comparison and bounded secret lookup.

## 1. Scope, review and isolation

- [x] Independently review the combined spec and this plan; resolve blocking and optional actionable findings. Obtain approval for the revised Spin design before production edits.
- [x] Preserve the owned `/private/tmp/edgezero-480` worktree on `feature/480-cloudflare-spin-geo`, based on #502 `8cd7af96`. Recheck parent head before publication and reassess relevant changes.
- [x] Replace the provisional Cloudflare-only docs with these approved documents under `docs/superpowers/specs/2026-11-18-cloudflare-spin-geo-design.md` and `docs/superpowers/plans/2026-11-18-cloudflare-spin-geo.md`. Keep private evidence outside the repository.

## 2. Cloudflare contracts and guides

**Files:** `crates/edgezero-adapter-cloudflare/tests/contract.rs`, `docs/guide/adapters/cloudflare.md`, `docs/guide/native-geo.md`.

- [x] Retain/finish the already implemented managed matrix: service/run_app/dispatch_app, independent provider/source/trust/status assertions, US/CA vs DE/BE forged headers, region display name, lowercase/leading-zero subdivision, country-only, invalid region, XX/T1, no country and no metadata, sequential isolation.
- [x] Run scoped cargo test after every code edit. Execute WASM lib/contracts with `--features "cloudflare test-utils"` using the lock-matched runner and pinned Node.
- [x] Correct stale NoSelection claims. Explain country/subdivision outcomes, platform-only source, header rejection and local-versus-hosted limits.

## 3. Spin geo selection and bounded assertion logic

**Create:** `crates/edgezero-adapter-spin/src/geo.rs`.
**Modify:** `crates/edgezero-adapter-spin/src/lib.rs`, `crates/edgezero-core/src/geo.rs`.

- [x] Write failing host tests for validated variable-name configuration; exact fixed-header capture; absent/duplicate/oversized/non-ASCII credentials; credential removal; normalized original limits; constant-time match/mismatch; no-selection/untrusted/error/country-only/Located outcomes; duplicate/invalid geographic assertions.
- [x] Export `geo` configuration on native as well as WASM, with runtime-only secret resolution gated appropriately. Constructor accepts a valid Spin variable name only, retaining no credential bytes. No new runtime dependencies.
- [x] Add truthful non-exhaustive provenance variants for authenticated Spin ingress. Resolve provider label through existing validated ID APIs. Keep `GeoOutcome::platform`'s existing signature; for unsupported `SpinIngress`, return selected `spin-ingress` with `ProviderFailure`, document it and test it cannot yield Located/PlatformMetadata. The authenticated ingress resolver constructs outcomes directly. Ensure unavailable/error results retain the selected identity.
- [x] Implement minimal pure capture/validation/resolution helpers and independently typed outcome tests. Use GeoAssertion/CountryCode/SubdivisionCode, do not clone all request headers for the source input. Secret comparison uses existing subtle dependency.
- [x] Run `cargo +1.98.1 test --locked -p edgezero-core -p edgezero-adapter-spin` after each logical code edit. Confirm source-established failing cases then passing behavior before integrating dispatch.

## 4. Spin production dispatch and preservation tests

**Modify:** `crates/edgezero-adapter-spin/src/request.rs`; extend colocated WASM tests and/or `tests/contract.rs` using existing test-support machinery.

- [x] At the shared managed ingress function, obtain a clone of non-secret selection from App state; capture/strip every fixed token header privately. Validate original normalized head before removal can reduce budgets, drop original cloned head, and construct sanitized head for admission.
- [x] Ensure source construction, variable fetch and provider work happen only after Admitted. Preserve existing refused/aborted, body-laziness and response-egress paths. No credentials enter callbacks, App state or public extensions.
- [x] Resolve configured variable using existing `secret_store::bounded_secret_read` or bounded SecretStore consumer under `PreparedIngress::read_deadline`, with max expected credential128 bytes and validation min32 ASCII bytes. Missing/error/invalid config becomes a category-only Geo provider error; no raw SDK error propagation/logging.
- [x] Produce NoSelection when no configuration; UntrustedIngress for invalid/missing/mismatching token; authenticated assertions resolved once; pass geo to dispatch_admitted_with_geo with existing framework-error detail.
- [x] Regression tests cover no secret fetch on rejected/aborted admission or absent/structurally invalid/duplicate tokens; well-formed mismatches perform one bounded lookup. Also cover original header budget failure, sanitized admission views, private immutability, unchanged body handling and sequence isolation. Use test-only injection seam only for secret backend failures/deadlines; normal Spin runtime must also exercise the actual variable provider.
- [x] Run scoped cargo tests after each edit, then native strict Clippy and WASM compilation to catch cfg-only errors.

## 5. Normal runtime qualification

- [x] Retain executed Cloudflare runtime evidence if production inputs remain identical; otherwise rerun affected cases. Actual built fetch -> run_app -> action, Miniflare host-controlled cf (explicit empty base cf to avoid fallback), typed assertions and source/artifact hashes. Runtime-only dependencies stay task-owned.
- [x] Build an isolated Spin SDK8 fixture with real HTTP handler -> run_app::<Hooks>; configure SpinGeoIngress from Hooks, actual declared Spin secret variable, ordinary/body/geo routes and admission/middleware token-absence observations. Use installed verified Spin4.1.0 binary, not default4.0.
- [x] Start owned loopback Spin and fronting proxy processes. Proxy must remove all viewer assertions/token, inject metadata from a controlled host fixture and its credential, and forward to Spin. Do not expose a header switch that enables authentication inside the Rust app.
- [x] Prove US/CA, country-only, missing/malformed/duplicate assertions, direct/wrong/missing/duplicate credentials, viewer spoof replacement, repeated requests, off-mode and body/routes, credential removal at callback boundaries and original header-size enforcement. Correlate typed source/trust/provider output and artifact identity. Stop owned resources after preserving sanitized evidence.
- [x] Run existing Axum production-hosting geo manifest/settings acceptance test; establish country/region without TS profile. Do not claim deployed CloudFront/ALB configuration.
- [x] Report independent runtime verdicts for CF, Spin and Axum, separating real SDK/runtime execution with fixtures from hosted metadata accuracy and transport qualification. Failed or blocked required scope cannot be relabeled VERIFIED.

## 6. Documentation, full gates and fresh review

**Modify:** `docs/guide/adapters/spin.md`, current native/Cloudflare guides and approved spec/plan.

- [x] Document exact Hooks configuration, fixed headers, `[variables]`/component variable binding, credential entropy/size, missing/failure semantics, proxy replacement/HTTPS/isolation requirements, rotation limitation and default-off behavior. Explain that stock Spin does not perform geolocation.
- [x] Execute `cargo +1.98.1 fmt --all -- --check`; `cargo +1.98.1 clippy --workspace --all-targets --all-features -- -D warnings`; `cargo +1.98.1 test --workspace --all-targets`; `cargo +1.98.1 check --workspace --all-targets --features "fastly cloudflare spin"`; `cargo +1.98.1 check -p edgezero-adapter-spin --target wasm32-wasip2 --features spin`.
- [x] Execute affected Cloudflare WASM suites, Spin WASM contracts/runtime tests, docs lint/format/build and git diff --check; run `cargo +1.98.1 test --offline --locked --manifest-path crates/edgezero-test-harness/Cargo.toml` plus its acceptance manifest/coverage checks. Inspect `scripts/run_tests.sh` and execute its applicable equivalent checks (dependency/forwarding checks, native feature matrices, adapter contracts, app-demo workspace and WASM compilation, generated-consumer and documentation contracts), recording any missing tools/existing failures separately. Pin versions, revision/tree and exact outcomes.
- [x] Fresh independent implementation review against full requirements and affected unchanged consumers. Resolve all blocking and optional actionable findings and rerun affected checks.

## 7. Draft PR and fresh local self-review

- [ ] Before committing explain implemented behavior, tradeoffs and verification limits. Explicitly sign commits, verify locally, push new branch normally, and confirm GitHub Verified per new SHA.
- [ ] Create draft PR assigned to current user using repository template/pr-creator instructions, base #502. Explain stacking and combined acceptance accurately; use Closes #480 only if all required scoped acceptance is demonstrated. Follow authorized project-board handling while preserving advanced statuses.
- [ ] Perform fresh independent local self-review of full PR against merge base and relevant unchanged paths; recheck live head/base and required gates. Resolve findings under user's implementation authorization, repeat affected verification and update draft.
- [ ] Final report: understandable behavior/corrections, exact revision, runtime/control results and limits, local checks vs hosted CI, signatures/publication URL, findings state and owned-resource cleanup. Preserve implementation worktree/unpublished work; remove only clean disposable review resources.

## Local qualification checkpoint

The implementation reuses #502's Cloudflare/Fastly metadata and Axum manifest binding.
The additional Spin source is enabled explicitly by `SpinGeoIngress` configuration.

| Boundary                  | Executed evidence                                                                                                | Scope                                                                                                                                                      |
| ------------------------- | ---------------------------------------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Cloudflare platform input | 141 WASM library tests, 31 managed contracts, 15 built Worker runtime cases                                      | Local workerd with host-controlled metadata; normal fetch-to-run_app path                                                                                  |
| Spin ingress source       | Five pure helper tests, 37 WASM contracts, 25 SDK resource contracts, 47 built component runtime cases           | Actual Spin variable provider in the normal run_app runtime fixture; contracts inject an in-memory secret store through the same managed dispatch pipeline |
| Regression discrimination | Managed contract fails when token stripping is removed or the private outcome is dropped; restored source passes | Owned disposable source copy; production worktree unmodified by controls                                                                                   |
| Axum manifest selection   | Existing production-hosting manifest/settings test returns US/CA without a Trusted Server boot profile           | Local real listener; external CloudFront/ALB configuration not qualified                                                                                   |

The generic Wasmtime contract runner cannot execute the entire Spin library test binary:
that binary imports Spin-specific KV host interfaces. The maintained CI path executes
`contract` and `sdk_resources`; those suites pass. Changed pure helper tests also execute
natively, and the real Spin fixture covers the actual secret backend. No generic-runner
result is represented as live Spin service evidence.

Runtime checks do not establish hosted IP geolocation accuracy, deployed proxy header
replacement, transport isolation, or Trusted Server consent behavior. Those remain deployment
qualification obligations rather than changes to application policy.

The required root gates, 32 native feature combinations, generated-project checks, app-demo tests and WASM builds, 178 tooling tests, and dependency checks passed. The isolated harness passed all 672 cases with four existing ignored cases under serial scheduling and unchanged deadlines. Its initial concurrent run encountered an uncached fixture dependency and process observation timeouts; locked fixture fetch plus the serial run resolved local qualification without source or timeout changes.
