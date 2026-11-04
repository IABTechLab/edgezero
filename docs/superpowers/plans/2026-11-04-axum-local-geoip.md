# Axum local GeoIP implementation plan

- Status: draft implementation locally verified. Full #438 acceptance is blocked by unused-metadata validation and the remaining proof/scope gaps below.
- Date: 2026-11-04
- Issue: [#438](https://github.com/IABTechLab/edgezero/issues/438).
- Contract: [approved local GeoIP spec](../specs/2026-11-04-axum-local-geoip-design.md). User-approved requirements remain authoritative; unresolved gaps below are not approved scope changes.
- Base: `72ceed7e`, `feature/pluggable-geo-providers`, already published as draft [PR #439](https://github.com/IABTechLab/edgezero/pull/439).
- Delivery branch: `feature/axum-local-geoip`, published as draft [PR #453](https://github.com/IABTechLab/edgezero/pull/453) against `feature/pluggable-geo-providers`. Local GeoIP remains separate from #439.

## 1. Decisions and implementation gates

### Reader, limits and stop ownership

Pin `maxminddb = "=0.32.0"` with default features disabled. Enable neither mmap nor unsafe string decoding. Add it as a workspace dependency and optional native-only Axum dependency, selected by default-off `geo-maxmind`. SHA-256 uses the existing workspace `sha2` dependency behind that feature.

Use upstream `Reader::verify()` for metadata, search-tree and reachable value-graph verification, followed by network iteration, a non-materializing structural Serde pass and typed geographic projection decoding. Research executed fixtures proving upstream verification accepts non-string map keys and pointer-to-pointer values. The extra pass uses the same bounded decoder with strict string keys and identifier-based recursive decoding to reject these in reachable data records. The library exposes no raw metadata decoder; corresponding unused metadata-field cases remain an explicit validation gap, not a completed all-corruption guarantee. This is not a second MMDB parser. Verify consumed field shapes at startup, while code membership/syntax validation remains request-time behavior. A valid string `XK` loads successfully and fails only when resolved.

Initial configurable defaults are 128 MiB of actual input bytes and 32 million validation network visits. MaxMind's published May-August 2026 ranges show City MMDB files at 43.9-66.9 MB and about 4.8-5.9 million combined IPv4/IPv6 networks, with Country smaller. These are observed sizes, not vendor guarantees. The byte default gives roughly twice the published upper City size; the visit budget allows tree-verification overhead and growth. An operator can deliberately raise finite limits for a newer release. Source: [database sizes](https://dev.maxmind.com/geoip/docs/databases/city-and-country/).

Before verifier entry, check that metadata node count times its upstream three-visits allowance fits the selected visit budget. Enforce the same budget on projection iteration. The verifier bounds value/string work to eight times section bytes and memoizes shared targets. Its allocation is proportional to distinct referenced values. The decoder has finite container/payload/depth bounds; projection borrowing avoids copying large strings or constructing full city records. Document actual guarantees and test exhaustion rather than claiming a wall-clock or fixed-RSS limit.

Preparation stays synchronous, before native runtime/signals/stop polling, as explicitly accepted in the revised spec and the user's approval. A file-byte or work limit cannot interrupt blocked filesystem I/O. Reject nonregular sources without waiting for their contents. Preserve the existing cooperative stop contract after preparation. No detached worker or cosmetic timeout is introduced.

A representative production GeoLite snapshot requires an operator's free account and licensed download. Do not obtain credentials or distribute production data for tests. If no permitted representative artifact is available, measure synthetic fixtures and explicitly report production-size time/memory measurement as unverified, not passed.

### Public data shapes and compatibility

Use a portable `GeoInput` enum:

- `Assertions { country, subdivision }`, preserving bounded slot semantics.
- `ReaderIp { address: IpAddr, source: GeoIpSource }`.

`GeoIpSource` identifies direct peer, trusted Forwarded and trusted X-Forwarded input. Custom Debug redacts reader addresses. Add `NoMatch` resolution and `MissingReaderIp`/`NoMatch` unavailable reasons. Add local-database and supplied-reader-IP source kinds and a checked-reader trust basis carrying the source. Reserve `maxmind` with `cloudfront`.

Native capability becomes a configuration description with provider identity and typed input contract, including `PeerAllowed` or `ForwardedRequired` for reader input. It does not pretend to know eventual request provenance. Separate pure validation/capability from preparation. Keep selector/configuration types available in feature-disabled builds.

The workspace is unpublished (`publish = false`, version 0.1.0). This is a coordinated pre-release public API migration, not a compatibility shim. Migrate repository consumers in the same change and document breaking changes in the changelog/guide and PR. Before any release, external consumers must update input construction and exhaustive enum matches. Do not bump unrelated crate versions in this work.

## 2. Implementation checklist

### Stage A: portable inputs and existing provider migration

- [x] Add typed IP input/provenance, safe Debug, no-match/missing-input outcomes and reserved MaxMind identity in `crates/edgezero-core/src/geo.rs`.
- [x] Migrate core tests and built-in CloudFront parser/tests to `Assertions`.
- [x] Reject an unsupported provider input case as `ProviderFailure`, with no input probing/fallback.
- [x] Update native counted-provider assertions, test fixture outcome classifiers and exhaustive guide/rustdoc examples.

Proof: `cargo test --locked -p edgezero-core --lib geo` and focused CloudFront parser tests. Existing country, immutable outcome and legacy-forgery regressions remain passing.

### Stage B: bounded local database and fixtures

- [x] Add `geo-maxmind` feature, pinned native dependencies and workspace/consumer lock updates.
- [x] Add feature-independent settings/limits and source-free error categories, with path-redacted Debug.
- [x] Implement read-only nonblocking-open handling for nonregular Unix sources, bounded regular-file read, immutable reader, metadata allowlist, verifier and projection validation.
- [x] Record loaded type/build timestamp/SHA-256, and map City/Country records using only `country.iso_code` and first subdivision code.
- [x] Add small reproducible synthetic MMDB fixtures and provenance, with no production download dependency.
- [x] Test IPv4/IPv6 and mapped input, IPv6 against IPv4-only data, wrong input, City/Country, code errors, no-match, first-entry rules and unknown subdivisions.
- [ ] Test metadata/field-type/corruption failures, off-path corruption, byte/visit limits, missing/unreadable/nonregular files and safe errors.

Proof: focused MaxMind library suite with `--no-default-features --features geo-maxmind`. Source removal/replacement and snapshot identity pass. The unchecked corruption/permission item includes a confirmed malformed-unused-metadata failure, retained as an ignored acceptance regression, and untested unreadable-file denial. Production-size time/RSS is unverified. Consumer lockfiles required no changes and pass locked checks.

### Stage C: native source composition and pure capability

- [x] Add `MaxMind` and supplied-reader-IP selection, reader expectation and typed native capability.
- [x] Preserve existing supplied-assertion/CloudFront protected-source admission and credential stripping.
- [x] Split pure capability validation from runner-owned preparation in `geo.rs` and `run_options.rs`.
- [x] Capture reader input from private checked facts in `service.rs` before moving metadata into public extensions; invoke only after admission.
- [x] Distinguish actual direct peer from explicitly resolved forwarded reader using `client_source()`. Honor both modes, including partial forwarding success and unresolved chains.
- [x] Emit safe loaded-snapshot diagnostics after existing logging initialization in `dev_server.rs`.
- [x] Test repeat capability calls on missing paths without I/O, feature-disabled selection and supplied reader input without either built-in.

Proof: source-admission unit matrix for both forwarding families, both reader expectations, missing/untrusted/forged peers, and counted managed dispatch. Run disabled, CloudFront-only, MaxMind-only and combined feature suites.

### Stage D: real startup and replacement contracts

- [x] Extend the existing no-store production fixture with local/supplied-IP modes, explicit proxy trust and database settings. Fixture environment inputs remain test-only.
- [x] Add real-listener tests for country/state, shared immutable outcome, private checked-input ownership, no-match and country-only behavior.
- [x] Add startup failures proving no initializer, bind or ready transition for unavailable implementation, invalid database and exhausted budgets.
- [x] Prove startup/read access on a read-only file without sidecar writes, stable active results after replacement/removal, restart loading the new snapshot, and invalid replacement rejection.
- [x] Retain counted-provider zero-call paths on refused/aborted/draining/off/inadmissible requests and no finalization lookup.
- [x] Prove an already-requested embedding stop is honored after preparation without initializer/bind/readiness.

Proof: nonzero sentinels through `scripts/run_test_nonzero.sh`, `production_hosting` integration suite, affected native lifecycle tests. These are native local runtime proofs, not downstream consent-policy acceptance.

### Stage E: documentation and dependency/CI coverage

- [x] Extend `docs/guide/native-geo.md` with direct/proxied free GeoLite examples, supported schemas, licensing/attribution/update duties, mount/replacement instructions, limits, pre-runtime stop exception and migration guidance.
- [x] Keep guide examples in compiled module rustdoc. Handle all unavailable/error cases without granting permission.
- [x] Add changelog entry for breaking public API additions and migration.
- [x] Extend `scripts/check_geo_dependency_contract.sh` to prove MaxMind/sha2 native opt-in isolation and absence of mmap/unsafe decoder features, preserving CloudFront dependency equality.
- [x] Extend `.github/workflows/test.yml` with disabled, each-built-in and combined-feature MaxMind/supplied-IP/runtime sentinels.

Proof: public doc tests, dependency script, shell checks and docs lint/format/build. Public schema examples and internal relative links must match the implemented API.

## 3. Stable integration checkpoint

Serialize Cargo commands using this checkout's target directory. Reuse focused results for unchanged inputs. Record exact feature/revision or relevant fingerprints, outcome, elapsed time and evidence path. Run:

```sh
cargo fmt --all -- --check
cargo test --locked --workspace --all-targets < /dev/null
cargo test --locked -p edgezero-adapter-axum --all-targets --all-features
cargo test --locked -p edgezero-adapter-axum --no-default-features --features axum --lib
cargo test --locked -p edgezero-adapter-axum --no-default-features --features geo-cloudfront --lib
cargo test --locked -p edgezero-adapter-axum --no-default-features --features geo-maxmind --lib
cargo test --locked -p edgezero-adapter-axum --features geo-cloudfront,geo-maxmind --doc
cargo clippy --locked --workspace --all-targets --all-features -- -D warnings
cargo check --locked --workspace --all-targets --features "fastly cloudflare spin"
cargo check --locked -p edgezero-adapter-axum --no-default-features
cargo check --locked -p edgezero-adapter-axum --no-default-features --features native-bindings
cargo check --locked -p edgezero-adapter-fastly --no-default-features --features fastly --target wasm32-wasip1
cargo check --locked -p edgezero-adapter-cloudflare --no-default-features --features cloudflare --target wasm32-unknown-unknown
cargo check --locked -p edgezero-adapter-spin --no-default-features --features spin --target wasm32-wasip2
bash scripts/check_geo_dependency_contract.sh
bash scripts/check_native_store_dependency_contract.sh
RUSTDOCFLAGS='-D warnings' cargo doc --locked --workspace --all-features --no-deps
cargo test --locked --manifest-path examples/app-demo/Cargo.toml --workspace --all-targets
cargo check --locked --manifest-path tests/fixtures/reusable-app/Cargo.toml -p fixture-axum
```

Run explicit feature-disabled and feature-enabled hosting sentinels as well. Where installed, run existing WASM adapter dispatch contract runners, not just cross-target compilation. Missing runtimes or representative data remain named coverage gaps.

## 4. Review and draft PR

- [x] Inspect the combined diff against the approved spec and obtain independent correctness/architecture review. The capability issue is fixed; metadata rejection remains an explicit blocker in section 5.
- [x] Validate no unrelated work or credentials enter commits. Include the reviewed spec and this implementation/evidence record.
- [x] Commit after relevant tests, push `feature/axum-local-geoip`, and create draft #453 against `feature/pluggable-geo-providers` using the repository PR template.
- [x] Describe #439 stacking, #438 scope, public migration, exact check outcomes, pre-runtime preparation exception and unverified production-size/downstream behavior. The PR explicitly retains draft status and makes no consent or production-acceptance claim.

## 5. Evidence and decisions during implementation

Initial source inspection confirms `maxminddb` 0.32.0 is ISC-licensed and default-feature-free. Independent proposals favored either replacing provenance wholesale or extending the existing enums. The chosen path extends them to minimize the migration beyond the required typed input change. Source inspection also found stacked-base workflow filters were missing; both workflow filters now include the parent Geo branch. Source research and independent contract investigation use configured `openai-codex/gpt-6.1-sol` agents. Zellij pane creation failed without creating panes; research continued through background noninteractive agents. The coordinator owns implementation and shared Cargo commands.

### Independent review and blockers

The independent correctness/architecture review confirmed private checked-IP ownership, no fallback, read-only snapshots and side-effect-free capability. It found two concrete issues:

1. **Open acceptance blocker:** upstream 0.32.0 accepts non-string keys and pointer chains in unused metadata fields. Our structural pass closes the data-record cases, but the library exposes no raw metadata decoder. Full malformed-metadata startup rejection therefore remains unmet. A strict metadata verifier/raw-decode dependency fix or an explicitly approved requirement change is needed. Do not vendor a fork, add a second MMDB parser or weaken the approved spec silently. The PR must remain draft and must not claim #438 acceptance until resolved.
   Reproduced by executing `geo_maxmind_rejects_unused_metadata_nonstring_keys -- --ignored`: one expected failing test, `/tmp/geoip-known-metadata-blocker.log`. A real fixture process with `bad-metadata-key.mmdb` also reached readiness, `/tmp/geoip-known-metadata-hosting.log`. The default suite explicitly ignores this one regression; it is not a passing corruption check.
2. **Fixed:** non-Unix builds advertised MaxMind capability although acquisition is unavailable. Pure validation now includes target support and returns `UnavailableMaxMind` before preparation. Non-Unix support itself remains unimplemented and requires scope acceptance or an acquisition implementation.

Other unverified acceptance work includes representative production GeoLite preparation time/RSS, real read-only mount execution, unreadable-file permission denial, and preparer-specific mutation coverage. No managed Axum preparer hook is added; the existing portable immutable-outcome regressions and new native middleware/header/extension regressions pass. Existing permission-mode, FIFO/device, middleware/header/extension, startup-stop and real listener fixtures prove narrower claims only.

One MaxMind-only hosting run failed when a child could not bind its ephemeral reserved address. The unchanged existing harness releases its reservation before child bind. The identical full suite passed on rerun; no production retry or arbitrary port policy was added. Retain this transient test-harness limitation in the PR evidence.

Check | Tested state | Result | Elapsed | Evidence
--- | --- | --- | --- | ---
Baseline core | `72ceed7e`, before source edits | 8 passed | 17.64 s compile | `/tmp/geoip-baseline-core.log`
Portable core | Typed input migration | 9 passed | 6.54 s compile | `/tmp/geoip-core-contract.log`
Reader admission/managed dispatch | Axum only, no built-ins | 5 passed | 6.10 s compile | `/tmp/geoip-reader-managed.log`
Combined native Geo | CloudFront + MaxMind, including ready-stop and corruption | 26 passed | 17.37 s compile, 0.09 s tests | `/tmp/geoip-combined-geo.log`
Native all features | Before test-only lint cleanup | Passed, including 40 real-hosting tests | 54.204 s | `/tmp/geoip-native-all-features.log`
MaxMind-only real hosting | After test-only lint cleanup | 12 passed on rerun after one bind race | 0.35 s tests | `/tmp/geoip-maxmind-hosting-rerun.log`
Workspace Clippy | All targets/all features, after test-only lint cleanup | Passed | 1.242 s final run | `/tmp/geoip-workspace-clippy.log`
Dependency contracts | Isolated normal graphs | Geo and native-store scripts passed | 1.532 / 3.112 s | `/tmp/geoip-dependency-contracts.log`
Final workspace tests | Final Rust/test/build inputs | 2,121 passed, 6 existing ignored | 9.409 s | `/tmp/geoip-workspace-tests-final.log`
Final native all features | All targets, final inputs | 447 passed, 1 known metadata regression ignored | 13.980 s | `/tmp/geoip-native-all-features-final.log`
Final workspace Clippy | All targets/all features | Passed | 3.993 s | `/tmp/geoip-workspace-clippy-final.log`
Isolated native matrix | `axum`, CloudFront, MaxMind, both | Units: 328 / 327 / 340 / 339 passed; reader lanes each ignore the known regression. Hosting: 9 / 8 / 12 / 11 passed. Seven doc tests and all-target Clippy pass in every lane | Per-command timings in logs | `/tmp/geoip-matrix-*-{units,hosting,docs,clippy}.log`
Workspace platform features | `fastly cloudflare spin`, all targets | Passed | 16.667 s | `/tmp/geoip-workspace-feature-check.log`
Minimal native / bindings | No defaults, then bindings-only | Both passed | 5.628 / 2.646 s | `/tmp/geoip-minimal-checks.log`
WASM target compilation | Fastly wasip1 / Cloudflare unknown-unknown / Spin wasip2 | All passed, compile-only | 18.211 / 24.332 / 12.851 s | `/tmp/geoip-*-wasm-check.log`
WASM runtime dispatch contracts | Viceroy 0.21.0 / wasm-bindgen runner 0.2.128 / Wasmtime 48.0.2 | Fastly 24 / Cloudflare 29 / Spin 35 passed | 14.531 / 24.740 / 10.418 s | `/tmp/geoip-{fastly,cloudflare,spin}-runtime.log`
Workspace rustdoc | All features, no deps, warnings denied | Passed | 24.362 s | `/tmp/geoip-rustdoc.log`
App-demo tests | Unchanged excluded-workspace lock, shared target directory | 54 passed | 50.013 s | `/tmp/geoip-demo-tests.log`
Reusable native consumer | Unchanged excluded-workspace lock | Passed | 6.062 s | `/tmp/geoip-reusable-check.log`
Consumer locked checks | Both excluded workspaces, offline/locked, shared target | Both passed; no lockfile updates needed | 26.357 / 0.163 s | `/tmp/geoip-consumer-lock-checks.log`
Published docs | Final guide, lint / format / build | All passed | 1.950 / 1.577 / 3.730 s | `/tmp/geoip-docs-final.log`
Synthetic fixtures | Original Apache-2.0 generator and 26 fixtures | Syntax/reproduction/checksums passed | Not timed | `node scripts/generate_geoip_fixtures.mjs --check`
Formatting/static checks | Final source, workflows and internal documents | Rustfmt, whitespace, internal links/fences and actionlint passed; actionlint ShellCheck disabled | Not timed | Coordinator checks
Known metadata defect | Explicit ignored regression and real process | Reproduced as expected failure; malformed metadata reaches readiness | Not timed | `/tmp/geoip-known-metadata-{blocker,hosting}.log`

The Rust/runtime implementation is committed as `9dd7b83b`. Publication follow-ups update this plan and fix CI cache IDs only; Rust/runtime inputs are unchanged. Final Rust, crate-test, script, workflow and guide input aggregate SHA-256 is `c586481c33c350e05476a670a126d97d0e1dbe6e416b7d4783e4b3f2df2e9c75`, computed with `git ls-files -cm --others --exclude-standard -z -- crates Cargo.toml Cargo.lock scripts .github docs/guide | sort -zu | xargs -0 sha256sum | sha256sum`. Plan/spec evidence edits and the changelog are outside this aggregate. Toolchain: Rust/Cargo 1.95.0, Linux native; Node 24.12.0. Logs are local temporary evidence, not durable CI artifacts. The official Viceroy 0.21.0 archive SHA-256 is `52ed3c3ed59d926c81015d637509942c3195443c02c2eebbded9125484336a9e`; the installed 0.17.0 binary was not used for runtime proof.

The coordinator inspected returned changes and ran repository-native integration. Bounded implementation workers owned the reader/fixtures, hosting fixtures/tests, and guide/CI respectively; no files overlapped. Their scratch/static results were not substituted for native tests. The independent review's known metadata blocker was reproduced both as an intentionally failing ignored regression and through a real runner. Its non-Unix capability finding was fixed; non-Unix runtime support remains unavailable.

### First GitHub CI feedback

The combined-feature job in run `41914870792` failed before compilation because Actions cache keys cannot contain commas. The matrix previously interpolated `geo-cloudfront,geo-maxmind` directly. Fixed the source by giving every lane a comma-free `cache_key` while keeping the real feature list unchanged. A focused old-versus-new matrix regression check, actionlint and scoped Prettier pass; evidence is `/tmp/geoip-ci-cache-{failure,regression}.log` and `/tmp/geoip-ci-static-final.log`. No Rust/runtime test inputs changed, so local suite results above remain applicable. GitHub checks for the follow-up commit remain pending, not claimed passing.

### Work needed before acceptance

- [ ] Obtain a strict metadata-verification/raw-decode fix in the reader, pin it, and prove the saved metadata fixture fails before initializer/bind/readiness. Remove the ignored regression only after it passes.
- [ ] Resolve non-Unix support versus an explicitly accepted Unix-only delivery scope.
- [ ] Measure a permitted representative City/Country artifact for compatibility, preparation time and peak memory under the documented limits.
- [ ] Complete mount-level and permission-denied proof where required, retaining the distinction from the passing file-mode and source-independence tests.

The draft is an implementation checkpoint, not completed #438 acceptance, consent compliance or production qualification.
