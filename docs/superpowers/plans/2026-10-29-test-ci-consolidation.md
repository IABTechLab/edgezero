# Test and CI Consolidation Implementation Plan

> **For agentic workers:** Use superpowers:subagent-driven-development or superpowers:executing-plans. Keep the checklist current; do not replace runtime evidence with compilation.

**Goal:** Consolidate test ownership and CI execution without weakening PR #275's outbound requirements, and remove measured setup/build duplication.

**Architecture:** Unit and SDK contracts remain in their owning crates. Runnable guest fixtures belong to the existing isolated fixture workspace. The existing native Rust harness owns execution, process cleanup and persisted evidence validation. Native tests, synchronous Wasmtime tests, asynchronous CLI components and actual Spin HTTP remain distinct phases, not interchangeable proofs.

**Tech Stack:** Existing Rust 1.98.1, Node 24.20.0, Cargo workspaces, pinned Spin/Wasmtime/Viceroy runtimes and GitHub Actions. No new transport or executor, Python, compatibility aliases, provider deployment or runtime promotion.

## Authority and Baseline

The requester approved this direction on 2026-10-29. Starting source: `a9051e897c658f76bc40a78fba63d90177b2e030`.

Measured successful test workflow: `41007960919`. Spin runtime 104 seconds; CLI 302 seconds; Fastly 625 seconds, including 297 seconds installing stock Viceroy and 100 seconds building/testing the isolated readiness repair. These are warm observations, not cold-build measurements. Six minutes is a provisional warm critical-path target, not a guaranteed result.

Preserve the following ledger throughout migration:

| Boundary                            | Existing Proof                                                                                                                                   |
| ----------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------ |
| Spin adapter integration            | All 35 WASM contract tests and native preflight cases                                                                                            |
| Pinned SDK and absent async context | All 21 SDK-resource tests; keep missing-task tests outside an async export                                                                       |
| Async scheduling and ownership      | Eager CLI component, callback and response ownership, cutoff and terminal release                                                                |
| Real Spin transport                 | 68 redirect exchanges / 84 correlated receipts, framing and scope/handoff controls                                                               |
| Independent response caps           | 26 policy exchanges / 26 correlated receipts, typed errors, stages, native source polls and release                                              |
| Ingress fidelity                    | All existing fixed raw-wire cases and unchanged observation baseline                                                                             |
| Feature coverage                    | All eight native/WASM configurations; remove a duplicate check only when identical target/features are actually covered                          |
| Evidence                            | Discovery, nonzero execution, named markers, failed/malformed/missing/duplicate artifact rejection, source/build identity and fresh launch nonce |

Required check names remain unchanged. Original deadlines, timing tolerances, source/trailer completion, cold/first-request process freshness, decoder pins, Image Optimizer behavior and qualification limitations remain unchanged. The requirements document, reviewed leg identities and approval records are not rewritten by this cleanup.

## Task 1: Remove Repeated CI Work

**Files:** `.github/workflows/test.yml`, `.github/workflows/format.yml`, focused CI regression tests under `scripts/`.

- [x] Add tests for one authoritative Cargo cache, one common-audit execution, pinned runtime validation and readiness execution on cache hits; demonstrate failure against the old workflow.
- [x] Disable overlapping automatic Cargo restores where an explicit cache already owns those paths.
- [x] Run identical rustdoc, legacy/API and dependency audits once, while keeping the distinct host suites and check contexts.
- [x] Reuse verified pinned stock Viceroy when its version/identity matches instead of unconditional force installation. Keep stock and patched runtime identities separate.
- [x] Cache compatible isolated-repair build inputs/artifacts using compiler, platform, upstream revision, lock, patch and build-profile inputs. Always execute its readiness regressions; do not cache a passing result as evidence.
- [x] Validate workflow syntax, shell behavior and negative cache/identity cases. Report local proof separately from hosted timing improvement.

Local result: all 26 CI-tooling regression cases pass, including damaged executable/version/
identity, missing compiler/platform facts, failed commands, zero tests and cache-hit readiness
execution. Actionlint passes. Hosted timing remains unmeasured for these changes.

## Task 2: Give Spin Fixtures a Clear Home

**Files:** `crates/edgezero-adapter-spin/Cargo.toml`, its `src/{outbound,request,response}.rs` test support, `tests/{contract,sdk_resources}.rs`, existing `tests/fixtures/reusable-app/` workspace and new `fixture-spin-async` / `fixture-spin-wire` packages.

- [x] Record the existing case names and assertion coverage before moving code.
- [x] Define only the private-boundary bridge required to exercise the actual conversion/scheduler; do not recreate transport or scheduling in fixtures.
- [x] Move async/wire component entrypoints, manifests and full scenario bodies out of adapter examples/production implementation files into the existing fixture workspace.
- [x] Preserve separate CLI and HTTP exports and explicit package selection; the fixture workspace defaults only to `fixture-core`.
- [x] Group crate integration tests by responsibility without adding one test executable per module.
- [x] Delete obsolete example targets/files and scenario forwarding exports after migrating every caller. Retain narrowly gated test observation support only.
- [x] Run native contracts, target-specific strict Clippy, actual async component and real Spin wire tests. Keep the coverage ledger unchanged.

## Task 3: Consolidate Ingress Process Ownership

**Files:** `crates/edgezero-test-harness/src/main.rs`, a focused `fidelity` module, harness manifest/lock, `scripts/test-request-fidelity.sh`, old `tests/fixtures/request-fidelity-runner/` package.

- [x] Port the old runner's regression tests before deleting it, including cancellation, descendant cleanup, read deadlines, tools/pins and observation comparison.
- [x] Reuse the existing harness's process-group ownership and bounded teardown for builds and runtimes. Distinguish an expected command exit from a prematurely exited server explicitly.
- [x] Move raw-wire cases, build preparation and comparison into the existing harness; do not keep a second process supervisor or async signal executor.
- [x] Expose ingress fidelity as an explicit suite in the same CLI, with the same optional observation recording and finite readiness behavior.
- [x] Migrate all callers directly and delete the old package, lockfile, bootstrap target and obsolete commands. Keep the actual guest fixture and baseline unchanged.
- [x] Run all ported unit tests and actual local Spin ingress probes; retain Cloudflare/Fastly setup requirements and their raw-fidelity limitations.

All 21 old runner tests and the 56-case inventory are retained. A new three-level
cancellation regression covers a blocked child harness and a TERM-ignoring runtime
descendant. Normal and recording Spin runs match all 17 unchanged observations.
The PID-file race found during full verification now uses the spawn-time PID and
passes 12 consecutive focused runs. The moved runner also passes all 20 Fastly and
19 Cloudflare observations against the unchanged baseline; listeners close after each run.

## Task 4: One Spin Runtime Suite

**Files:** focused CI-suite module in `crates/edgezero-test-harness/src/`, `src/runners/wire.rs`, workflow and acceptance binding tests.

- [x] Add `--adapter spin --suite ci` to the existing harness. It selects the explicit build/feature, synchronous SDK, async CLI, HTTP wire and ingress phases.
- [x] Use the same owned-process implementation, with per-phase logs, elapsed times and original exit statuses. No implicit runtime downloads.
- [x] Reopen and validate persisted evidence after flush. Require both exact wire inventories, every declared marker and successful cleanup, not just producer counters or success strings.
- [x] Add negative tests for truncated/malformed/duplicate/wrong-case artifacts, missing markers, command failure and teardown failure; prove they fail before considering the suite successful.
- [x] Replace Spin's inline YAML Cargo/grep/jq case logic with setup, one suite invocation and artifact upload. Keep strict feature Clippy in its existing named check.
- [x] Update executable inventory commands/artifacts and binding tests without changing expected acceptance text, leg identities, applicability or qualification.

The 42 evidence-tool tests pass; the manifest remains 255 criteria / 511 legs, with
449 planned and qualification false. Redirect readback now replays ordered records
through the same matrix assertions, retaining legitimate repeated cases and rejecting
count-preserving substitutions, foreign reports/receipts and incomplete consumption. Command
diagnostics now preserve the original exit failure even with invalid UTF-8 (red/green
regression), and the ingress phase forwards its configured readiness timeout.

## Task 5: Build Reuse and Documentation

**Files:** harness build preparation, `startup.rs` where necessary, fixture/harness READMEs, CI/workflow tests, `scripts/run_tests.sh`, `CHANGELOG.md` and the existing adoption plan dashboard.

- [x] Reuse only identical target/profile/features/lock/compiler/embedded-input artifact sets. Preserve distinct debug/release and host/guest artifacts when their inputs differ.
- [x] Validate the embedded build identity independently of a fresh per-launch nonce. Do not use an unchecked skip-build option for acceptance credit.
- [x] Centralize local and CI invocation. Remove superseded commands and orphaned build roots rather than add aliases.
- [x] Update documentation and existing acceptance tracking together. Do not claim hosted qualification or exact-candidate freeze.

## Task 6: Verify and Measure

- [x] Run all five required root Rust gates, isolated harness/fixture formatting and strict Clippy, all moved tests and real Spin runtime phases.
- [x] Run guard/evidence/CI regression tests and docs lint/format/build. Audit for old targets, paths and duplicate supervisors.
- [x] Perform independent specification and quality reviews and fix findings.
- [x] Commit and push the verified consolidation to PR #275.
- [ ] Collect a successful hosted run and compare job critical path, summed worker time, cache transfer and build/install time with the baseline. Mark cold timing unmeasured until an explicit cache-miss run exists.
- [x] Mark completion only for implemented and verified tasks; retain any unclosed evidence or timing work explicitly.

Local verification (2026-10-30): all five root gates; both Spin WASM Clippy configurations;
all three Spin fixture packages' target Clippy; 352 harness tests (two intentionally ignored);
42 evidence, 97 execution/legacy guard and 25 CI-tooling tests; docs format/lint/build.
The full suite passed at `.runs/18e33c1d25021528-554d-0`: 35 synchronous contracts,
21 SDK tests, async ownership controls, 68 redirects / 84 receipts, 26 cap checks /
26 receipts and unchanged ingress observations. Its warm local phase total is about
50 seconds, not a hosted-CI timing result. A preceding run failed on the local macOS
LLVM lookup; supplying the installed toolchain library path resolved linking without
repository or runtime-pin changes. Required named checks and hosted qualification
limitations remain unchanged. Independent final review and hosted measurement remain open.

Final review's four P2 cleanup items are implemented: ingress-target caching in the named
Clippy jobs; content-bound build identities with fresh launch nonces; shared ownership for
`checked()` commands; and no blocking log forwarding while cancellation/reaping is pending.
The fingerprint includes source mutations, additions/deletions, locks/manifests, toolchain
and build configuration, and rejects incomplete inputs. Cargo still builds every requested
target/configuration. Explicit identity inputs keep synthetic process fixtures independent
of compiler probes and source scans. Final native verification passes 366 tests with two
intentionally ignored; all nine fingerprint cases and new cancellation/blocked-output cases
pass. Strict harness Clippy passes. The final full Spin CI suite passes at
`.runs/18e33e88c9d1af10-97c8-0`, including both persisted wire inventories and unchanged
ingress readback. A second wire run (`.runs/18e33ea39c4884b8-ab76-0`) passes all 94
exchanges with the same content identity and component fingerprint. Fresh launch
nonces remain independently enforced; generated output does not invalidate the build.

Review responsibilities: Pasteur reviewed CI ownership/caching and implemented the
content identity; Arendt moved Spin scenarios and independently found command/output
cancellation gaps; Boole migrated ingress ownership and verified all three local
providers; Pauli reviewed persisted evidence and reused the existing matrix for ordered
replay. The selected changes retain distinct execution boundaries and one shared
process owner rather than combining unlike proofs or introducing another executor.

## Hosted Checkpoint

Implementation commit `0a04ddb083147a1161eb46610bdd9644b3c92cbe` is pushed to PR #275.
Both [test workflow 41022052304](https://github.com/IABTechLab/edgezero/actions/runs/41022052304)
and [format/Clippy workflow 41022052131](https://github.com/IABTechLab/edgezero/actions/runs/41022052131)
pass. All required check names are retained. This is local-runtime/CI evidence, not hosted
provider qualification.

The first run populates new runtime/fixture cache keys; it is **not a comparable warm run**:

| Test Job             | Recorded Baseline (Seconds) | First Consolidated Run (Seconds) |
| -------------------- | --------------------------: | -------------------------------: |
| Axum                 |                         109 |                              313 |
| Fastly               |                         625 |                              990 |
| Workspace            |                         284 |                              650 |
| Cloudflare           |                         227 |                              245 |
| CLI                  |                         302 |                              458 |
| Spin                 |                         104 |                              260 |
| Demo                 |                         224 |                              170 |
| Test aggregate       |                           3 |                                4 |
| Sum of job durations |                        1878 |                             3090 |

Fastly's first stock install takes 288 seconds and its uncached isolated repair takes
378 seconds (baseline 297/100). These observations do not establish a speedup. The remaining
measurement checkbox requires a comparable warm run, cache-transfer data and a recorded
interpretation; the provisional six-minute target is not claimed achieved. Live follow-up
results belong in [PR #275](https://github.com/IABTechLab/edgezero/pull/275) without changing
the outbound acceptance inventory or qualification status.

## Verification Commands

Use the repository toolchain and locked/offline builds after fetching the changed test-only graph:

```sh
cargo fmt --all -- --check
cargo clippy --offline --locked --workspace --all-targets --all-features -- -D warnings
cargo test --offline --locked --workspace --all-targets
cargo check --offline --locked --workspace --all-targets --features "fastly cloudflare spin"
cargo check --offline --locked -p edgezero-adapter-spin --target wasm32-wasip2 --features spin
cargo test --offline --locked --manifest-path crates/edgezero-test-harness/Cargo.toml
cargo fmt --manifest-path tests/fixtures/reusable-app/Cargo.toml --all -- --check
cargo fmt --manifest-path crates/edgezero-test-harness/Cargo.toml --all -- --check
node --test scripts/run_test_nonzero.test.mjs scripts/check_outbound_cutover_evidence.test.mjs
node scripts/check_outbound_cutover_evidence.mjs --manifest
actionlint .github/workflows/test.yml .github/workflows/format.yml
```

Final local runtime entrypoint after migration:

```sh
cargo run --offline --locked --manifest-path crates/edgezero-test-harness/Cargo.toml -- --adapter spin --suite ci
```
