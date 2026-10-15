# Rust Cache Review Remediation Implementation Plan

> **For agentic workers:** Use superpowers:executing-plans for inline test-first
> execution, with independent document and code review.

**Goal:** Address PR #417's verified review findings without weakening revision isolation.

**Architecture:** Add opt-in post-job target writes and restore-only defaults.
Bind repeated setup to source identity and skip duplicate restores. Prove first-build
compiler hits across jobs and retries; align deployer PR #36 with the new immutable pin.

**Tech Stack:** Bash, composite GitHub Actions, actions/cache v5, sccache v0.16.0,
yq v4, Cargo, existing deployer policy checks.

## Task 1: Cache action behavior and regressions

Files: `.github/actions/setup-rust-build-cache/action.yml`, `scripts/prepare.sh`,
`tests/run.sh`, `tests/revision.sh`, new `tests/review.sh`, and
`.github/actions/deploy-core/tests/run.sh`.

- [ ] Add failing tests for default `save-target: false`, restore-only metadata,
      opt-in writer metadata, invalid save modes, repeat skip flags, inferred local
      Git identity, mixed revision/settings rejection, and newline paths.
- [ ] Run `bash .github/actions/setup-rust-build-cache/tests/review.sh`; verify red.
- [ ] Implement `save-target` validation and two mutually exclusive target steps:
      restore-only when false; combined cache with post-job save when true. Gate all
      remote setup steps on `already-initialized != 'true'`; keep no restore prefixes.
- [ ] Infer local ownership SHA with `git -C "$project_real" rev-parse --verify HEAD`
      only when no revision was supplied. Compare workspace/SHA/lock/cache settings.
      Keep first non-Git setup allowed and reject unidentified repeats.
- [ ] Add `append_output`, remove unused internal outputs, reject path control
      characters, normalize cache-action identity in output contracts while retaining
      the separate pin gate, and fix cleanup of permission-restricted test fixtures.
- [ ] Run both old and new cache suites, full deployment contracts and ShellCheck.

## Task 2: Cross-job and retry-safe probes

Files: `.github/workflows/deploy-action.yml`, cache `tests/make-fixture.sh`,
`tests/assert-compiler-reuse.sh`, new `tests/fixture.sh`,
`.github/actions/deploy-fastly/tests/make-smoke-fixture.sh`.

- [ ] Add failing retry fixture tests: seed attempt 1 and consumer attempt 2 share
      an explicitly passed fixture ID and SHA; different run IDs change library bytes;
      next revision preserves compiler inputs. Add structural first-build assertions.
- [ ] Export seed fixture ID, use it in both consumers, and embed it in lib.rs.
      Seed builds native and wasm32-wasip1; all three jobs resolve, install and select
      the identical Rust toolchain from .tool-versions using one shared helper.
- [ ] Remove build/clean/rebuild from the reuse probe: reset stats, build once,
      assert Rust hits and zero cache read/write errors for each target.
- [ ] Set fixed fixture Git dates and record upstream revision in generated app
      source identity. Gate full fixture target writes on main, seed writes explicitly,
      and leave consumers restore-only. Extend existing workflow structural contracts.
- [ ] Run fixture regressions and a local two-revision, two-sccache-server probe
      with fresh target directories; require native/WASM first-build hits.

## Task 3: Documentation and aligned deployer

Files: `docs/guide/deploy-action-adoption.md`, CLI builder header comments,
deployer `.github/workflows/prepare-release.yml`, policy tests, README, CLAUDE,
and active cache specs/plans.

- [ ] Add complete checkout, validated SHA output, cache inputs, and diagnostic
      output example. Add migration checklist, explicit limitations and branch scope.
- [ ] Update deployer policy tests first to require five exact cache inputs and
      `save-target: ${{ github.ref == 'refs/heads/main' }}`; watch red before workflow edit.
- [ ] Preserve source selection, defaults, cache-target=true, protected deployment,
      CLI packaging, config-only, recovery, artifact retention, and no-Python policy.
- [ ] After upstream commit verification, advance all action/contract/checker
      pins together and validate 49 consumed input pairs against that exact checkout.

## Task 4: Verification and delivery

- [ ] Run full EdgeZero Cargo test/fmt/clippy/features/Spin gates, Bash contracts,
      actionlint, ShellCheck, pin gate, offline security scan and docs lint/build.
- [ ] Run all six deployer suites, YAML parsing, actionlint, full ShellCheck,
      live upstream contract checker and git diff --check in both worktrees.
- [ ] Obtain independent code review; fix substantive issues and reverify.
- [ ] Commit and push the existing PR #417 and #36 branches. Update PR descriptions
      with breaking build/cache migration and actual verification evidence; no merge,
      deployment, release or cache cleanup. Observe PR CI for the final heads.
