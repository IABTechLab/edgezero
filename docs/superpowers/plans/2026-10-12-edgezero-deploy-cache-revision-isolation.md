# Revision-isolated Rust Build Cache Implementation Plan

> **For agentic workers:** Use superpowers:executing-plans to implement this plan
> inline, with independent document and code reviews. Steps use checkboxes.

**Goal:** Share Rust dependency/compiler caches across commits while isolating
writable target artifacts by application source revision.

**Architecture:** Keep source keys stable and introduce a revision-specific v2
target key. Always publish an app-specific job-local target directory. Bind its
local reuse to a validated selection marker and preserve build/deploy boundaries.

**Tech Stack:** Bash, composite GitHub Actions, actions/cache v5, sccache v0.16.0,
mikefarah yq v4, and existing Cargo workspace verification.

## Task 1: Contract tests (red)

**File:** `.github/actions/setup-rust-build-cache/tests/run.sh`

- [ ] Add a reusable prepare invocation helper and isolated runner fixtures.
- [ ] Test required full lowercase revision with target caching enabled; invalid
  inputs leave GITHUB_OUTPUT/GITHUB_ENV empty.
- [ ] Test stable target paths in both modes, same-revision repeat setup,
  mixed-revision/workspace rejection, and unknown existing target contents.
- [ ] Test root/app/target/marker symlinks and confinement before directory writes.
- [ ] Parse action YAML with yq: source key contains no revision; target key has
  a version and full revision, with no restore-keys; expose hit/key/path outputs.
- [ ] Run `.github/actions/setup-rust-build-cache/tests/run.sh`; confirm failures
  are the missing revision contract and stable-directory behavior.

## Task 2: Minimal cache implementation (green)

**Files:** `.github/actions/setup-rust-build-cache/action.yml`,
`.github/actions/setup-rust-build-cache/scripts/prepare.sh`,
`.github/actions/build-app-cli/action.yml`

- [ ] Add optional metadata input `target-cache-revision`, require it in Bash
  only for cache-target=true, and validate with `^[0-9a-f]{40}$|^[0-9a-f]{64}$`.
- [ ] Validate all paths and selection state before emitting outputs/environment.
- [ ] Create confined nonsymlink cache/app/target directories and write an
  outside-target marker binding workspace and revision; reject unsafe reuse.
- [ ] Always emit CARGO_TARGET_DIR and EDGEZERO__RUST_CACHE__TARGET_DIR.
- [ ] Emit source/target key prefixes; target ends with the full source revision
  and `-exact`, preventing 40/64-character SHA prefix collisions. Test that
  collision explicitly. Use the new v2 target key and omit target restore-keys.
- [ ] Expose exact-hit outputs, cache keys, and directory. Keep sccache pins.
- [ ] Correct build-app-cli's cache-directory comment; do not change its build
  invocation, credential scrubbing, or packaging implementation.
- [ ] Run cache contracts and scoped Cargo tests; require zero failures.

## Task 3: Consumers, CI smoke tests, and documentation

**Files:** `.github/workflows/deploy-action.yml`,
`.github/actions/deploy-core/tests/run.sh`,
`docs/guide/deploy-action-adoption.md`

- [ ] Pass source SHA to every in-repository cache-target=true caller. Fixture
  release jobs use their own fixture revision, not github.sha.
- [ ] Update the existing cross-job target restore probe to the v2 contract and
  add a new-revision probe: no marker/target hit, source cache remains reusable.
- [ ] Use a tiny native/WASM library fixture to demonstrate compiler-cache reuse;
  log sccache statistics and fail if unchanged-library hits are absent.
- [ ] Extend structural workflow contracts for all revision inputs and probes.
- [ ] Document the new input, stable directory, cache layers, exact-hit semantics,
  migration, first-run cold cache, and honest performance limitations.
- [ ] Run all Bash action contracts and actionlint/ShellCheck/pin/security checks.

## Task 4: Verification and delivery

- [ ] Run `cargo test --workspace --all-targets`.
- [ ] Run `cargo fmt --all -- --check`.
- [ ] Run `cargo clippy --workspace --all-targets --all-features -- -D warnings`.
- [ ] Run `cargo check --workspace --all-targets --features 'fastly cloudflare spin'`.
- [ ] Run `cargo check -p edgezero-adapter-spin --target wasm32-wasip2 --features spin`.
- [ ] Run docs format/lint/build and `git diff --check`.
- [ ] Obtain independent code review and fix substantive findings.
- [ ] Create one linked Task issue with acceptance criteria, commit and push the
  branch, and open a PR against main using the repository template.
- [ ] Report actual verification results and leave the worktree available for
  follow-up; do not merge or release.
