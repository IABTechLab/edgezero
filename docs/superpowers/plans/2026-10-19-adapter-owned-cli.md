# Adapter-owned CLI implementation plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans
> to implement this plan centrally, task by task. Investigation/review agents
> remain read-only; do not edit overlapping files in parallel.

**Goal:** Repair PR #287's confirmed correctness defects while making provider
behavior entirely adapter-owned.

**Architecture:** Extend the neutral registry with adapter-owned local layout
metadata; share local orchestration and lossless env records. Replace nested
lock borrowing with ordinary write transactions and adapter-owned vendor
writeback isolation. No production CLI dispatch compares provider names.

**Tech Stack:** Rust 1.98.1, existing Cargo workspaces, registry traits,
toml_edit, fs4/std filesystem locks, existing adapter fake-provider fixtures.

**Spec:** `docs/superpowers/specs/2026-10-19-adapter-owned-cli-design.md`

## Task 1: Registry layout and adapter ownership

Files: `crates/edgezero-adapter/src/registry.rs`, each adapter's `src/cli/mod.rs`,
`crates/edgezero-cli/src/{provision,lib,generator}.rs`, shared scaffold metadata.

- [x] Add a failing fifth-adapter regression exercising a custom manifest,
      env sidecar, dry-run redaction, serve preload, generated tool pins/install
      notes, and templated logging defaults.
- [x] Add neutral local context/layout/artifact types and the pure adapter hook.
- [x] Implement each built-in adapter's own layout.
- [x] Replace CLI artifact/default-name/serve-env/provider-message switches.
- [x] Move tool pins/hints and templated logging into adapter blueprints.
- [x] Check unsupported-read messages use the adapter's reason.
- [x] Run `cargo test -p edgezero-cli -p edgezero-adapter` and adapter CLI tests;
      confirm fifth-adapter behavior and no provider-name dispatch remains.
- [x] Commit the verified ownership boundary.

## Task 2: Shared lossless env records

Files: `crates/edgezero-adapter/src/env_file.rs`,
`crates/edgezero-cli/src/{env_file,provision}.rs`.

- [x] Add failing corruption/leak/dedup/header regressions for single, double,
      and backtick quotes, CRLF, absent final newline, and literal shell text.
- [x] Implement borrowed record/assignment spans and explicit value decoding.
- [x] Move append/upsert key/header detection to logical records; preserve
      untouched bytes and reject unterminated active values before writes.
- [x] Use the same records for overlay reading and conservative redaction.
- [x] Run `cargo test -p edgezero-adapter -p edgezero-cli` and local provision
      tests for every adapter; verify the actual dry-run sentinels disappear.
- [x] Commit the verified env handling correction.

## Task 3: Selected validation and one local pipeline

Files: `crates/edgezero-cli/src/{config,provision,path_safety}.rs`,
`crates/edgezero-core/src/manifest.rs`.

- [x] Add failing mixed-adapter selection and derived-symlink regressions.
- [x] Keep core manifest validation structural; apply deployed-field ownership
      through registered adapters with selected/all scope.
- [x] Scope targeted path/typed checks to the selected adapter.
- [x] Validate adapter-declared derived paths in the original filesystem.
- [x] Consolidate real/staged and typed/untyped local execution into one body,
      retaining baseline ordering, partial outcomes, and cloud separation.
- [x] Run `cargo test -p edgezero-cli -p edgezero-core`, then adapter local
      provisioning tests; prove equivalent real/staged rejection behavior.
- [x] Commit the verified orchestration changes.

## Task 4: Portable lock lifetime and native writeback

Files: `crates/edgezero-cli/src/{provision_lock,lib,adapter}.rs`,
`crates/edgezero-adapter-fastly/src/cli/{run,mod}.rs`.

- [x] Add subprocess regressions for composed commands, owner cancellation,
      independent competing writers, and source vendor writeback isolation.
- [x] Simplify project locks to ordinary exclusive guards for provision/config
      writes; remove token/depth advertisement and deploy-wide borrowing.
- [x] Implement Fastly-owned private native manifest invocation and service-id
      merge through existing manifest lock/atomic replacement helpers.
- [x] Snapshot and merge under separate short locks. Re-read the source and
      change only service_id; refuse if its current value differs from both
      snapshot and validated vendor result. Preserve unrelated concurrent edits.
- [x] Cover native arg ownership, relative inputs, errors, cleanup, concurrent
      unrelated manifest edits, and vendor survival after parent termination.
- [x] Model vendor writes before argument processing and after parent death;
      assert both selected source and sibling fastly.toml remain intact. Cover
      detached/inline directory and environment flags and exclusive temp-file
      creation. Use explicit ready/release barriers rather than timed sleeps.
- [x] Preserve immutable-release integrity and exercise all fake deployment
      production/staging/recovery/store-free cases.
- [x] Run scoped CLI/Fastly tests and commit the verified lifetime correction.

## Task 5: Provider execution and persistence boundaries

Files: Fastly `src/cli/{mod,staging}.rs`, Cloudflare
`src/cli/push_cloud.rs`, their colocated tests.

- [x] Add failing managed-child-environment and local-read symlink regressions.
- [x] Thread resolved execution environment through Fastly managed native/API
      paths and lifecycle preflight/finalization hooks, preserving
      adapter-selected working directories and precedence. Test credentials
      supplied only through explicit execution context and matching selectors.
- [x] Guard Cloudflare local persistence before both reads and writes.
- [x] Run Fastly and Cloudflare CLI tests and the original local probes.
- [x] Commit the verified provider-owned fixes.

## Task 6: Documentation and final verification

Files: `CHANGELOG.md`, affected CLI/adapter guides, generated scaffold docs,
and spec amendments necessary to reflect selected scope and lock ownership.

- [x] Document registry/context/blueprint API migration and shell override
      transaction limits; remove contradictory provider assumptions in CLI help.
- [x] Run `cargo test --locked --workspace --all-targets`.
- [x] Run `cargo fmt --all -- --check`.
- [x] Run `cargo clippy --workspace --all-targets --all-features -- -D warnings`.
- [x] Run `cargo check --workspace --all-targets --features "fastly cloudflare spin"`.
- [x] Run `cargo check -p edgezero-adapter-spin --target wasm32-wasip2 --features spin`.
- [x] Run adapter WASM Clippy, demo/fixture tests, generated-project ignored
      compilation, docs lint/format/build, and deployment-action contracts.
- [x] Obtain independent read-only review of the resulting diff and resolve
      confirmed findings before pushing to the existing PR.
- [x] Push without force; inspect the new GitHub checks and report remaining
      reviewer sign-off. Preserve unrelated local edits and do not merge.

## Verification record

All 18 local gates passed after independent verification, including full
workspace tests, strict all-feature Clippy, adapter WASM combinations, demo and
lifecycle fixtures, and explicit generated-workspace compilation. Docs lint,
format, and build passed. Deployment action contracts reported 639 passes,
zero failures, and five existing Linux-only skips on macOS.

Four native fake-provider replays passed: production, staging, ambiguous
activation/recovery rollback, and store-free deployment. Every replay preserved
the exact immutable release tree. The production replay also modeled a native
startup upgrade with a source manifest missing `manifest_version`.

The implementation was centralized. Read-only investigations/proposals came
from the adapter-boundary, lock-lifetime, and env-record agents; plan review was
independent, and the build-audit agent verified all 18 gates. Confirmed review
findings were resolved before publication. Live GitHub check and reviewer
status remain authoritative for merge readiness.
