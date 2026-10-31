# Integration Test Consolidation Implementation Plan

> **For agentic workers:** Use `superpowers:executing-plans` to implement the approved consolidation task by task.

**Goal:** Give cross-runtime integration testing one discoverable home and one per-adapter execution interface without changing outbound behavior or acceptance requirements.

**Architecture:** Adapter-owned contracts and SDK tests stay beside their adapters. The isolated native `edgezero-test-harness` owns runtime fixtures, acceptance assets, orchestration and the integration guide. Native and guest dependency graphs, SDK entrypoints, runtime revisions and qualification boundaries remain separate.

**Tech Stack:** Rust, Cargo, Node.js test guards, existing provider runtimes and GitHub Actions.

## Approval And Constraints

The requester approved this layout and implementation in the conversation on 2026-10-30. No compatibility paths, alternate transports, new async executors, runtime upgrades, deployment or hosted qualification are authorized. Preserve every acceptance identity, source digest, expected assertion and planned/ready status. Update only execution paths and commands when relocation requires it.

## Tasks

### 1. Bind The Layout And Execution Contract

- [x] Add failing layout checks for fixtures and acceptance assets under `crates/edgezero-test-harness` and absence of the superseded root fixture paths.
- [x] Add CI regression checks requiring the same per-adapter Rust suite entrypoint locally and in CI, with the existing required job names.
- [x] Retain guards against missing tests, skipped phases, failed cleanup and invalid evidence.

### 2. Relocate Integration Assets

- [x] Move the two guest workspaces to `crates/edgezero-test-harness/fixtures/{reusable-app,request-fidelity}` without changing external dependency versions or workspace boundaries.
- [x] Move the acceptance inventory and runbook to `crates/edgezero-test-harness/acceptance`.
- [x] Update live Cargo paths, build identities, commands, guards, caches and documentation. Preserve historical measurements and approval records.
- [x] Replace relative repository-depth assumptions with explicit repository/fixture roots.
- [x] Verify metadata and compare every acceptance row with the original inventory.

### 3. Give Shared Spin Scenarios One Owner

- [x] Move only scenarios used across adapter contracts and executable components into explicit, isolated guest test support.
- [x] Replace cross-tree source includes with Cargo dependencies; retain private-boundary tests in the adapter.
- [x] Preserve SDK CLI versus HTTP component exports, resource acknowledgement, source release and the existing policy/wire inventories.
- [x] Compile and run scoped native and WASM tests after the migration.

### 4. Unify The Execution Interface

- [x] Extend the existing `--adapter NAME --suite ci` Rust entrypoint to the other adapters, with explicit native, SDK, feature-matrix and runtime phases.
- [x] Replace workflow-owned test sequencing with those same suite invocations, preserving runtime installation, required check names, assertion counts and artifact uploads.
- [x] Keep focused suite selection for iteration; do not combine all runtimes into one job.
- [x] Update the local test entrypoint to use the same per-adapter suites.

### 5. Verify And Review

- [x] Run harness/layout/tooling regression tests and strict Clippy for the isolated harness and guest support.
- [x] Run the five root CI gates, fixture checks, affected provider runtime suites and documentation checks.
- [x] Review for stale live paths, duplicate execution, lost cases, dependency drift and broadened capability claims.
- [x] Update this checklist, the main integration guide and CHANGELOG with actual results and any limitations.

## Verification Results (2026-10-30)

- All five root gates passed: 2,621 tests across 32 binaries, zero failures and one existing ignored generated-workspace test.
- Controller tests: 370 passed, zero failures and two existing ignored cases. Tooling/evidence/legacy regressions: 171 passed.
- The exact `--adapter NAME --suite ci` entrypoint completed with exit zero for Axum, Fastly, Cloudflare and Spin. Fastly retained the explicitly unqualified client-progress exit inside its validated phase. Spin separately executed all eight migrated shared wrappers.
- Strict Clippy passed for the controller, all three WASM adapter targets, the extracted Spin support/CLI/HTTP guests and the Spin progress fixture. Documentation lint, formatting and site build, dependency audits, actionlint, shellcheck and legacy guards passed.
- Structured comparison retained all 511 acceptance identities and every non-execution field. Only 214 commands and four artifact paths changed. Production, controller and ingress Cargo locks and the npm lock are unchanged. The lifecycle lock adds only the local support package and three fixture dependency edges; external dependency records are unchanged.
- Independent adapter/support and fixture/path reviews found no remaining consolidation blocker. Explicit mutation coverage now includes the acceptance inventory and shared-support source/manifest.

### Execution Boundaries

Runtime results are local regression evidence, not exact-candidate or hosted qualification. The earlier strict lifetime/timing failures and remaining acceptance work are unchanged. Local Cloudflare browser tests used installed Chrome 156.0.8078.74 and driver 156.0.8078.73; CI retains its existing Chrome 154.0.8037.92 setup.

Initial attempts retained their failures for a globally mismatched Wasmtime, an old wasm-bindgen runner and macOS LLVM loader resolution. Successful runs selected the existing pinned Wasmtime 48.0.2/bindgen 0.2.128 binaries and supplied the existing LLVM library through ignored build-directory links, without changing installed toolchains or source pins.

An experimental full Spin library execution could not link provider KV imports under plain Wasmtime. That command was not part of the previous runnable baseline and is not counted as a pass. The original SDK contract/resource targets remain executable; six private library clock tests retain compile/strict-Clippy coverage. All eight relocated shared wrappers now execute in the fixture support package. This limitation is also explicit in the integration guide.

### CodeQL Follow-Up

The first GitHub scan flagged exponential backtracking in the new test-only argument-array inspection regex. Replace it with a linear split and JSON parsing of the supported string-array subset; unsupported Rust expressions fail the binding check. Two additional regressions cover exact values/escaping/trailing commas and a 100,000-repetition malformed input. All 171 tooling tests and the Cargo evidence-assembly regression pass. No scanner suppression, production change or acceptance-policy change is introduced.

## Verification Commands

Run from the repository root using `.tool-versions` pins. Cargo operations are locked and offline after dependencies are available.

```sh
cargo test --offline --locked --manifest-path crates/edgezero-test-harness/Cargo.toml
node --test scripts/ci_tooling.test.mjs scripts/run_test_nonzero.test.mjs scripts/check_outbound_cutover_evidence.test.mjs
cargo fmt --all -- --check
cargo clippy --offline --locked --workspace --all-targets --all-features -- -D warnings
cargo test --offline --locked --workspace --all-targets
cargo check --offline --locked --workspace --all-targets --features "fastly cloudflare spin"
cargo check --offline --locked -p edgezero-adapter-spin --target wasm32-wasip2 --features spin
```

Runtime execution remains local/CI evidence, not hosted qualification. Existing strict timing failures remain reported at their original thresholds.
