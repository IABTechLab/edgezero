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

- [ ] Add a failing fifth-adapter regression exercising a custom manifest,
      env sidecar, dry-run redaction, serve preload, generated tool pins/install
      notes, and templated logging defaults.
- [ ] Add neutral local context/layout/artifact types and the pure adapter hook.
- [ ] Implement each built-in adapter's own layout.
- [ ] Replace CLI artifact/default-name/serve-env/provider-message switches.
- [ ] Move tool pins/hints and templated logging into adapter blueprints.
- [ ] Check unsupported-read messages use the adapter's reason.
- [ ] Run `cargo test -p edgezero-cli -p edgezero-adapter` and adapter CLI tests;
      confirm fifth-adapter behavior and no provider-name dispatch remains.
- [ ] Commit the verified ownership boundary.

## Task 2: Shared lossless env records

Files: `crates/edgezero-adapter/src/env_file.rs`,
`crates/edgezero-cli/src/{env_file,provision}.rs`.

- [ ] Add failing corruption/leak/dedup/header regressions for single, double,
      and backtick quotes, CRLF, absent final newline, and literal shell text.
- [ ] Implement borrowed record/assignment spans and explicit value decoding.
- [ ] Move append/upsert key/header detection to logical records; preserve
      untouched bytes and reject unterminated active values before writes.
- [ ] Use the same records for overlay reading and conservative redaction.
- [ ] Run `cargo test -p edgezero-adapter -p edgezero-cli` and local provision
      tests for every adapter; verify the actual dry-run sentinels disappear.
- [ ] Commit the verified env handling correction.

## Task 3: Selected validation and one local pipeline

Files: `crates/edgezero-cli/src/{config,provision,path_safety}.rs`,
`crates/edgezero-core/src/manifest.rs`.

- [ ] Add failing mixed-adapter selection and derived-symlink regressions.
- [ ] Keep core manifest validation structural; apply deployed-field ownership
      through registered adapters with selected/all scope.
- [ ] Scope targeted path/typed checks to the selected adapter.
- [ ] Validate adapter-declared derived paths in the original filesystem.
- [ ] Consolidate real/staged and typed/untyped local execution into one body,
      retaining baseline ordering, partial outcomes, and cloud separation.
- [ ] Run `cargo test -p edgezero-cli -p edgezero-core`, then adapter local
      provisioning tests; prove equivalent real/staged rejection behavior.
- [ ] Commit the verified orchestration changes.

## Task 4: Portable lock lifetime and native writeback

Files: `crates/edgezero-cli/src/{provision_lock,lib,adapter}.rs`,
`crates/edgezero-adapter-fastly/src/cli/{run,mod}.rs`.

- [ ] Add subprocess regressions for composed commands, owner cancellation,
      independent competing writers, and source vendor writeback isolation.
- [ ] Simplify project locks to ordinary exclusive guards for provision/config
      writes; remove token/depth advertisement and deploy-wide borrowing.
- [ ] Implement Fastly-owned private native manifest invocation and service-id
      merge through existing manifest lock/atomic replacement helpers.
- [ ] Snapshot and merge under separate short locks. Re-read the source and
      change only service_id; refuse if its current value differs from both
      snapshot and validated vendor result. Preserve unrelated concurrent edits.
- [ ] Cover native arg ownership, relative inputs, errors, cleanup, concurrent
      unrelated manifest edits, and vendor survival after parent termination.
- [ ] Model vendor writes before argument processing and after parent death;
      assert both selected source and sibling fastly.toml remain intact. Cover
      detached/inline directory and environment flags and exclusive temp-file
      creation. Use explicit ready/release barriers rather than timed sleeps.
- [ ] Preserve immutable-release integrity and exercise all fake deployment
      production/staging/recovery/store-free cases.
- [ ] Run scoped CLI/Fastly tests and commit the verified lifetime correction.

## Task 5: Provider execution and persistence boundaries

Files: Fastly `src/cli/{mod,staging}.rs`, Cloudflare
`src/cli/push_cloud.rs`, their colocated tests.

- [ ] Add failing managed-child-environment and local-read symlink regressions.
- [ ] Thread resolved execution environment through Fastly managed native/API
      paths and lifecycle preflight/finalization hooks, preserving
      adapter-selected working directories and precedence. Test credentials
      supplied only through explicit execution context and matching selectors.
- [ ] Guard Cloudflare local persistence before both reads and writes.
- [ ] Run Fastly and Cloudflare CLI tests and the original local probes.
- [ ] Commit the verified provider-owned fixes.

## Task 6: Documentation and final verification

Files: `CHANGELOG.md`, affected CLI/adapter guides, generated scaffold docs,
and spec amendments necessary to reflect selected scope and lock ownership.

- [ ] Document registry/context/blueprint API migration and shell override
      transaction limits; remove contradictory provider assumptions in CLI help.
- [ ] Run `cargo test --locked --workspace --all-targets`.
- [ ] Run `cargo fmt --all -- --check`.
- [ ] Run `cargo clippy --workspace --all-targets --all-features -- -D warnings`.
- [ ] Run `cargo check --workspace --all-targets --features "fastly cloudflare spin"`.
- [ ] Run `cargo check -p edgezero-adapter-spin --target wasm32-wasip2 --features spin`.
- [ ] Run adapter WASM Clippy, demo/fixture tests, generated-project ignored
      compilation, docs lint/format/build, and deployment-action contracts.
- [ ] Obtain independent read-only review of the resulting diff and resolve
      confirmed findings before pushing to the existing PR.
- [ ] Push without force; inspect the new GitHub checks and report remaining
      reviewer sign-off. Preserve unrelated local edits and do not merge.
