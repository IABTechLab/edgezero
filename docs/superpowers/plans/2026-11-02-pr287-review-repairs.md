# PR 287 merge and review repairs

> **For agentic workers:** Use `superpowers:executing-plans`. Implementation is
> centralized; investigation, proposals, and verification agents stay read-only.

**Goal:** Merge current `main` into PR 287 and address the October 30 review.

**Architecture:** Preserve adapter-owned CLI policy. Shared code handles Cargo
resolution, action prerequisites, and fallible output without provider switches.
Reuse existing filesystem guards and smoke backup machinery.

**Tech stack:** Rust 1.98.1, Bash 3.2-compatible shell, existing Cargo workspaces,
fake vendor shims, and synthetic temporary fixtures.

**Design:** The existing adapter-owned CLI design remains authoritative. Prefer
small corrections to the current contracts over a new provider enum, deployment
framework, process-group launcher, or broad refactoring. The user authorized
merge conflict and review repairs; no merge to main or review dismissal is part
of this work.

## Merge

- [x] Merge `origin/main` into the latest PR head without rebasing or force push.
      Preserve the workflow's smoke checks and new ingress-fidelity checks.
      Inspect automatic source/doc merges and test the combined tree.

## Review repairs

For each behavioral fix, first add and run a reproducing regression, then make
the smallest implementation change and run affected crate tests.

- [x] **1 — Spin source containment.** In adapter `cli/run.rs`, perform copying
      on the same normalized path checked for containment and symlinks. Test
      `escaped/../victim.wasm` with an external symlink and preserve external
      bytes.
- [x] **2 — Fastly startup privacy.** In `cli/local_vendor.rs`, create the startup
      temporary directory owner-only before copying any manifest bytes. Test a
      `0600` source in a subprocess with umask `022`; assert directory privacy,
      selected-file restrictions, and preserved content.
- [x] **3 — Cargo target resolution.** In `edgezero-adapter/src/cli_support.rs`,
      use `--target-dir`, `CARGO_TARGET_DIR`, `--config`,
      `CARGO_BUILD_TARGET_DIR`, then file configuration. Select extensionless
      `config` over its sibling before inspecting target-dir. Test local and
      global fallback plus both environment precedence cases against Cargo.
- [x] **4 — Minimal feature builds.** In CLI `lib.rs`, expose stream helpers for
      either CLI or audit features, gate CLI-only imports and lint expectations,
      and add standalone audit/minimal-feature checks to CI.
- [x] **5 — Authentication prerequisites.** In CLI `adapter.rs`, make runtime
      secret/path prerequisites action-aware. Authentication retains child env
      precedence and cwd without requiring application secrets or complete
      runtime paths. Test registry and shell auth plus unchanged runtime gates.
- [x] **6 — Cloudflare namespace selection.** In adapter `cli/push_cloud.rs`,
      select preview for local operations when configured and primary otherwise;
      explicitly select primary for remote reads. Keep remote writes addressed
      by primary ID. Cover provision-to-push/read with both namespace IDs and
      primary-only configuration using a fake Wrangler.
- [x] **7 — Spin component grammar.** Derive a default component ID independently
      of the Cargo artifact name. Normalize underscores/case for inference and
      validate Spin's word grammar at adapter synthesis/validation boundaries.
      Reject incompatible inference or explicit selectors clearly; preserve the
      Cargo-derived WASM basename. Verify with installed Spin and regression
      tests.
- [x] **8 — Output errors.** In CLI `stream.rs`, return `io::Result` for payload
      writes. Propagate through structured/JSON renderers, diff dispatch, and
      command results, including unified previews before a push. Keep logger output
      explicitly best-effort. Test actual redirected output; audit output errors
      must reach its nonzero entrypoint result.
- [x] **9 — Axum smoke secret.** Export the runtime's prefixed secret variable
      in `scripts/smoke_test_config.sh`; retain the typed endpoint assertion.
- [x] **10 — Smoke descendant ownership.** In `scripts/lib/smoke_backup.sh`,
      capture the full owned process tree before teardown, terminate deepest
      descendants first, wait, and kill remaining captured processes. Missing or
      zombie processes count as stopped. Return failure if ownership capture or
      teardown fails; gate every caller's restoration on successful shutdown.
      Test non-listening grandchildren without lsof and failed shutdown without
      restoring files. Never kill unrelated port users.
- [x] **11 — Explicit Spin cloud consumers.** Correct the walkthrough and run
      the key-override cloud smoke against an isolated manifest with explicit
      `cloud = true`. Require the intended unsupported error rather than any
      nonzero status, and keep local operator manifests untouched.
- [x] **12 — Axum secret derivation.** Remove raw reserved-prefix rejection in
      adapter `cli/mod.rs`, retaining character and derived-name collision gates.
      Replace its rejection test with provision/env-store round-trip coverage
      proving the adapter host variable is untouched.
- [x] **13 — Spin variable docs.** Replace the application-secret `--env`
      alternative with `--variable`; preserve guest environment flags used for
      runtime store selectors.
- [x] **14 — Inline fixture fidelity.** Correct the two deployed-writeback
      fixtures in CLI `provision.rs`. Assert intended inline tables and old
      values exist before merging, then verify updates and preserved siblings.

## Verification and publication

- [x] Run workspace tests, format, strict all-feature Clippy, combined provider
      compilation, Spin WASIp2, adapter WASM Clippy, standalone/minimal CLI
      feature checks, shell tests/ShellCheck, docs checks, demo and reusable-app
      fixtures, ingress-fidelity harness, and generated-project compilation.
- [x] Obtain independent read-only reviews of the combined changes. Reproduce
      and fix confirmed issues, record decisions, and review the final diff.
- [ ] Commit and push normally to the existing PR branch; inspect all checks
      and final CodeQL on the exact pushed head. Report remaining human review.

## Decision record

Baseline workspace/all-target tests pass at `dac48385`. The live base advanced to
`87456c70`; an in-memory merge identifies only `.github/workflows/test.yml` as a
text conflict. Preserve both independent sets of checks.

`nov02_adapter_review` confirmed adapter findings 1, 2, 6, 7, and 12 and probed
Spin's actual identifier parser. `nov02_cli_review` confirmed findings 3, 4, 5,
8, and 14, including Cargo metadata probes for precedence and config selection.
`nov02_smoke_review` confirmed findings 9, 10, 11, and 13 and reproduced a
non-listening grandchild overwriting restored state. Read-only proposals chose
creation-time privacy, exact guarded destinations, action-aware prerequisites,
and a captured process tree instead of broader replacements.

The merge is recorded as `3912b215`. Central regression tests first reproduced
findings 1–8, 10, 12, and the misleading inline fixtures. The fixed suites pass;
the Axum HTTP smoke passed five checks with the corrected secret export. A
full typed-CLI/fake-Spin cloud replay now invokes the cloud writer and leaves
operator files unchanged. Fifty shell assertions pass, including failed-stop
restoration gates extracted from all four actual cleanup functions.

Independent adapter, CLI, and smoke reviewers approved the resulting behavior.
The CLI reviewer identified the audit helper's non-entrypoint exit lint; it now
returns a result to the existing entrypoint instead. The command builder accepts
already-resolved child environments, avoiding another argument-count suppression.

Additional validation found that `toml::Value::from_str` parses values rather
than full documents with the current TOML library. The preview test's structural
assertion was therefore skipped. Correcting its document parser first reproduced
the lost-structure behavior; the production preview now parses full documents
before redaction. Scalars/comments remain redacted and malformed input stays
fail-closed.

Final independent verification passed all 36 local gates in 332 seconds.
Generated projects compiled on the host and all three WASM targets. The smoke
helper suite passed 50 assertions; deployment contracts passed 643 assertions
with five existing non-Linux skips. Docs lint, format, and build passed. The
pinned-workerd Cloudflare ingress runtime probe remains intentionally ignored
by the native ingress matrix; this repair does not claim that optional probe
was executed locally. GitHub checks on the published head remain authoritative.

A post-push CI check exposed a merge integration gap outside the Rust matrix:
main's three hand-written request-fidelity manifests matched the PR's global
tracked-generated-state prohibition. The CLI and build reviewers independently
recommended an anchored exception for exactly those three files. Renaming would
unnecessarily change runtime discovery. The corrected actual workflow block
passes 16 real/synthetic Git-index policy cases: application manifests, secret
env files, and adjacent/nested fixture paths remain forbidden. The separate
ignored-file probes are unchanged. Actionlint validates the workflow; GitHub
checks are rerun on the follow-up head.
