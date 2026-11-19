# Spin KV geography implementation plan

> Approved implementation plan. Integrate incrementally, with independent review and build/runtime verification.

**Goal:** Complete 480/486 with a deployable Spin KV resolver reusing PR454 and existing Cloudflare coverage.
**Architecture:** Authenticated single visitor-IP admission, declared Spin KV binding, request-owned pinned resolver, private GeoOutcome. Separate operator publication component reuses existing immutable publication logic.
**Tech stack:** Rust, portable core geo-kv, Spin SDK8/Spin4.1, existing EnvConfig and manifest, WASI p2; no new Tokio dependencies in portable code.

## Task 1: Integrate the existing foundation

- [ ] Refresh PR454/502/514 head/base and preserve clean implementation worktree; stop if unreviewed upstream changes alter assumptions.
- [ ] Normally merge exact reviewed 454 into feature/480-cloudflare-spin-geo; resolve crates/edgezero-core/src/geo.rs union and preserve intended private-outcome behavior. Do not alter parent branches or rewrite history. Run cargo test --workspace --all-targets after conflict resolution.
- [ ] Update existing docs/superpowers/specs/2026-11-18-cloudflare-spin-geo-design.md and corresponding plan to approved final design; retain accurate inherited scope.

## Task 2: Static deployment configuration

Files: crates/edgezero-core/src/manifest.rs; crates/edgezero-core/src/env_config.rs (narrow prefix-key enumeration and tests); crates/edgezero-adapter-spin/src/{geo.rs,lib.rs,request.rs}; affected Axum geo configuration validation/tests.

- [ ] First write failing manifest/config tests for Spin kv/off, env precedence, unknown GEO override names, invalid pins/store ids/variable names/reader mode/timeouts, provider-field mixing and Axum/Cloudflare/Fastly rejection of inappropriate settings. Confirm failures by scoped cargo test.
- [ ] Reuse ManifestAdapterGeo KV fields from454; add credential_variable and adapter-specific validation. Implement Spin immutable selection resolution through existing manifest and EnvConfig accessors on both public dispatch paths. No Rust-only activation requirement.
- [ ] Remove superseded SpinGeoIngress country/region API and activation tests; preserve useful token-admission implementation/tests. No draft compatibility shim unless a real consumer is discovered.
- [ ] Run scoped cargo test after each code change; build both native and spin-WASM feature surfaces.

## Task 3: Authenticated IP and request-owned resolver

Files: crates/edgezero-adapter-spin/src/{geo.rs,request.rs}; core geo provenance tests only as needed; crates/edgezero-adapter-spin/tests/{contract.rs,sdk_resources.rs} and existing test-support.

- [ ] Write failing tests proving a valid token plus IPv4/IPv6 reaches actual KV lookup, source/provenance and private context; cover country-only/no-match/unsupported location and mapped IPv6.
- [ ] Reuse credential capture/stripping/admission before callbacks; capture the bounded single x-edgezero-geo-ip privately and strip both IP and token headers before admission snapshots. Preserve request budget accounting before credential removal. Keep secrets/reader IP absent from logs and diagnostics.
- [ ] After admission/authentication resolve declared registry KvHandle, open pinned Resolver, verify roots, and lookup under a single absolute KV deadline computed once after authentication as min(admitted deadline, now + configured KV timeout), never reset per stage. Secret work remains covered by the admitted deadline. Use 384 KiB max object, path3, request-local 1 MiB/16-entry cache. Never retain SDK handles across calls or add background work.
- [ ] Map missing/corrupt data and backend failures to existing typed outcome; no policy/status or fallback changes. Exercise deterministic timeout/drop and secret failures; assert zero KV reads for rejected inputs and zero writes always.
- [ ] Add input-reach tests for duplicate token/IP, wrong token, spoof country/forwarding/platform headers, missing binding, wrong pin, corruption, cancellation, retained dispatch_app and repeated requests. Run cargo test for each changed crate and maintained WASM contracts after each code batch.

## Task 4: A supported binary dataset loading path

Files: examples/geo-kv-prototype/{Cargo.toml,src/lib.rs,src/main.rs,src/publication.rs,README.md}; new excluded examples/geo-spin-loader/{Cargo.toml,Cargo.lock,src/lib.rs,spin.toml,README.md}; operator runner under that example.

- [ ] Verify WASI compilation of the proposed small library before depending on it; keep importer/native backend dependencies isolated if required, without moving publication into production core.
- [ ] Expose existing index/store/publication library modules; add publisher feature to enable existing publish/report without native backends. Keep native tool commands behavior and tests unchanged.
- [ ] Add separate loader component and runner: read-only artifact mount; loopback-only listener; ephemeral credential supplied via Spin variable; authenticate exact single POST before work; validate artifact before target store preparation; publish then full verify; payload-free failure output; stop process using a trap on all exits including timeout and interruption. Enforce finite readiness/publication windows (30 seconds/10 minutes by default, configurable), with a cleanup-on-timeout test. Use environment-backed Spin variable injection and an in-memory POST so credentials never appear in process arguments or temporary files. Stage and remove an owned artifact copy inside the temporary application directory required by Spin. Never add this endpoint to serving manifests.
- [ ] Validate explicit backend configuration and exact store label. For local file-backed KV require absolute path, no implicit loader .spin database; document external backends and exclusive publisher ownership. Credentials must not appear in process arguments/logs.
- [ ] Red/green tests for tampering, wrong credentials, immutable replay/conflict, manifest-last/readback failure, remote full-tree failure and process cleanup. Run locked standalone tool tests and WASI component build after changes.

## Task 5: Real application verification and preservation

- [ ] Use pinned Rust1.98.1, SDK8 and actual Spin4.1 binary. Build synthetic artifact with core builder via offline tool; no licensed fixtures. Load through real loader into a declared local store and stop loader.
- [ ] Start separately built serving app using exactly that named store/absolute backend config, through normal manifest/env activation. Test US/CA and IPv6, country-only, missing coverage/unsupported code, wrong pin/corrupt or missing objects, permissions, auth/IP rejection, timeout, privacy, normal routes/bodies and repeated dispatch. Capture source inventory, artifact hashes and exact revision.
- [ ] Show meaningful deliberately broken lookup and authentication controls fail expected assertions, then restore. Verify loader port/process cleanup and that serving issues no writes.
- [ ] Re-run real Cloudflare metadata/forged-header proof and Axum manifest listener proof when relevant inputs change; run Fastly contracts after foundation merge. Separate local runtime evidence from hosted qualification.
- [ ] Run cargo fmt --all -- --check; cargo clippy --workspace --all-targets --all-features -- -D warnings; cargo test --workspace --all-targets; cargo check --workspace --all-targets --features "fastly cloudflare spin"; cargo check -p edgezero-adapter-spin --target wasm32-wasip2 --features spin. Complete repository run_tests equivalents, isolated consumer harness, affected standalone/example tests and docs lint/format/build.

## Task 6: Review and draft publication

- [ ] Obtain fresh independent full-delta code/architecture review and build validation; resolve all blocking and optional actionable findings, with tests after changes. Repeat affected runtime proof for final source.
- [ ] Update public technical docs for proxy IP contract, default off/configuration, pinned data publication/rollout, cold-request costs, capacity and host-allocation/cancellation limits; do not claim standalone direct-peer trust or production-load certification.
- [ ] Sign and locally verify new commits, normally push, verify GitHub signatures. Retarget existing draft514 to feature/kv-geoip and rewrite body around final delta, still linked to480/486. No force push, review requests, comments, merge or deployment.
- [ ] Fresh independent self-review after publication; inspect current-head CI without attributing older results. Fix findings under existing authorization and repeat required checks. Keep draft if any required evidence remains pending; report exact outstanding state.
