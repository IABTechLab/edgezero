# Test Tooling Consolidation Implementation Plan

> **For agentic workers:** Use the executing-plans workflow and colocated regression tests. User approved the inline design on 2026-10-31.

**Goal:** Give the existing native Rust test harness ownership of repository checks and evidence validation, deleting superseded JavaScript implementations without weakening coverage.

**Architecture:** Add static `check` and evidence commands to `edgezero-test-harness`, before runtime argument parsing or fixture initialization. Keep independent validator modules and ordinary colocated unit tests. Runtime supervision, production HTTP behavior, acceptance policy and qualification decisions do not change.

**Tech Stack:** Existing Rust harness, `serde_json`, `toml`, `sha2`, Cargo/rustdoc subprocesses; JavaScript only where the provider runtime or documentation application requires it.

## Invariants

- No compatibility wrappers, second validator, new production dependency or provider deployment.
- Preserve dependency versions/checksums and normal exact pins; keep the separate no-CLI feature graph check.
- Preserve actual forwarding-example compilation, compiler-negative controls, exactly-one doctest and non-execution of handler code.
- Preserve all evidence source and leg hashes, safe integer restrictions, command/sentinel identity, artifact containment and hashing, approval attribution, failed/pending observations, no-overwrite behavior and qualification gates.
- Static checks must not create fixture runs, initialize runtime tools or start listeners.
- Build the harness once per CI job and share metadata per workspace; no nested harness Cargo invocation from a test guard.
- Node remains for the Cloudflare Miniflare launcher and documentation frontend. Hosted HTTPS driver replacement is separate because client interruption behavior is evidence-sensitive.

## Task 1: Dependency And Forwarding Checks

**Files:** `crates/edgezero-test-harness/src/{main.rs,checks.rs,checks/dependencies.rs,checks/forwarding.rs}`, existing dependency contract JSON, `.github/workflows/test.yml`, `scripts/{run_tests.sh,ci_tooling.test.mjs}`, harness README/SUITES and active runbook. Static CLI controls reuse the existing owned process-test helper in `main.rs`; do not add a separate unbounded integration-test subprocess runner.

- [x] Add a failing CLI regression: `check` must bypass runtime initialization and reject unsupported selectors.
- [x] Port all forwarding fence/artifact/listing/pass-summary controls into colocated Rust tests, including real compiler failures and fence-injection controls.
- [x] Port dependency auditing into one module: parse lockfiles as TOML, cache one all-feature metadata result per root/demo/reusable workspace, audit both codec contracts, separately inspect the no-CLI feature graph.
- [x] Add negative dependency fixtures for missing/duplicate packages, version/source/checksum drift, missing/weak/dev-only normal pins, missing graph edges and forbidden JSON map features.
- [x] Run failing tests before implementation, then implement the shared `check` command and make them pass.
- [x] Replace five JavaScript files and all active callers atomically; no old-command aliases. Preserve fetch-before-offline-audit and exactly-one-workspace-owner CI assertions.
- [x] Run real checks and harness tests/Clippy; verify active docs and workflow tooling.

Task 1 verification: 427 harness tests pass with two existing ignored; strict harness Clippy
and the combined `scripts/test_integration.sh check` pass. The five root gates pass with
2,636 workspace tests across 33 binaries and one existing ignored. CI-tooling tests pass 34;
unchanged evidence-tooling tests pass 58 and inventory remains 135 ready/376 planned,
unqualified. Audited JSON data moved byte-for-byte into `acceptance/dependencies/`.
Two independent reviewers found a final-stderr-cap race and unbounded CLI test subprocesses;
the deterministic late-stderr regression failed before repair, both streams are now rechecked,
and CLI controls reuse the existing supervised process helper with a 30-second deadline.
No production behavior, pin, capability, acceptance identity or qualification status changed.
Tasks 2 and 3 remain pending; this checkpoint does not claim complete script consolidation.

## Task 2: Evidence Validation

**Files:** existing `scripts/check_outbound_cutover_evidence{,.test}.mjs`, `crates/edgezero-test-harness/src/evidence.rs`, new focused harness evidence modules, existing shell test guard and tests, CI and active runbooks.

- [ ] Characterize current checker outputs and all mutation/negative controls before deletion; explicitly test JavaScript-compatible safe integers and canonical serialization used in hashes.
- [ ] Port manifest/sentinel, assembly, report and release validation into one Rust owner. Keep runtime observations separate from independent release validation.
- [ ] Preserve the existing case IDs, frozen source bytes/digests and leg-set policy; migrate command bindings only where invocation changes require it.
- [ ] Replace the Rust-to-Node assembly test with direct Rust validation and end-to-end binary tests.
- [ ] Migrate the shell guard to the already-built harness binary, avoiding recursive Cargo or a second sentinel implementation.
- [ ] Delete the checker and JS tests with the caller cutover. Keep pre-freeze drafts unqualified and open gates blocked.

## Task 3: Remaining Guards

- [ ] Move pure response-egress evidence validation, keeping the existing hosted HTTPS client until its interruption semantics are ported and verified.
- [ ] Migrate documentation/legacy/CI guards in focused checkpoints, preserving scanner failures, duplicate-YAML-key rejection, subprocess behavior and negative controls rather than replacing them with source-presence checks.
- [ ] Remove obsolete tooling package/dependencies only once no remaining caller needs them.

## Verification And Review

- [ ] Each checkpoint: scoped Rust tests, strict harness Clippy, Node tests for remaining callers, active docs/legacy checks, inventory validation and `git diff --check`.
- [ ] Before push: all five root gates from `CLAUDE.md`, plus harness tests/Clippy and actual migrated check commands.
- [ ] Independent code/evidence review; fix findings before committing. Update CHANGELOG, SUITES and the active implementation tracker with completed scope and explicit remaining work.
- [ ] Keep PR #275 draft; do not claim hosted qualification, timing repair or a frozen release candidate from tooling consolidation.
