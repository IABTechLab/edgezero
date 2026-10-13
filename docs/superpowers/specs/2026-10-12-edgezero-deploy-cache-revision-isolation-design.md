# Revision-isolated Rust build caches

## Goal

Reuse dependency sources and valid compiler results across application commits,
without restoring writable Cargo target artifacts from another source revision.
This implements the cache requirements approved in the conversation. Deployment
behavior, credential isolation, and release verification are unchanged.

## Action contract

`setup-rust-build-cache` keeps a stable `app-name` and adds
`target-cache-revision`. With `cache-target: true`, the revision is required and
must be a full lowercase Git SHA (40 or 64 hexadecimal characters). Callers must
resolve it from the application source, not the deployment repository. An
explicit revision is validated even with target caching disabled. Missing or
invalid required revisions fail before publishing outputs or changing runner
environment files. There is no legacy target-key fallback.

The action always publishes a stable app-specific `CARGO_TARGET_DIR` under
`RUNNER_TEMP`, including when `cache-target: false`. The latter controls remote
target restore/save only; source caching and sccache remain enabled. The SHA is
in the cache key, never the directory path. Native CLI and subsequent WASM
builds share this directory. Existing CLI native-target selection and Cargo
invocation remain mandatory.

## Cache layers

- Dependency source keys contain app, OS, architecture, and Cargo.lock digest.
  Their restore prefix omits the lock digest and source revision.
- A versioned target key contains app, OS, architecture, lock digest, and source
  revision followed by `-exact`. It has no broad or legacy restore prefix. The
  terminator prevents a 40-character SHA from prefix-matching a 64-character SHA.
- sccache manages compiler/input/environment-dependent results. Keeping build
  paths stable removes one avoidable cache-key change; not all crate types or
  compilations are cacheable and no particular speedup is promised.

Expose source and target `cache-hit` outputs (exact-match booleans), their keys,
and the target directory. Distinguish an exact source hit from prefix reuse in
documentation. Compiler statistics remain visible through sccache's job logs.

## Local isolation

Before creating any cache directory, reject symlinked cache-root, app, target,
or selection-marker components. Check canonical confinement below RUNNER_TEMP.
Reject pre-existing nonempty target directories without a selection marker.
Keep the selection marker outside the cached target directory. It binds the
directory to the canonical application workspace and selected source revision.
Reject subsequent setup for a different workspace/revision. Without an explicit
revision, setup is allowed only once for that app; callers needing another
setup must supply the revision. No cleanup of unrelated or unowned files occurs.

## Scope and migration

Update all in-repository action callers and the adoption guide. New releases
require external consumers to use a stable app-name and explicitly pass their
resolved source SHA when enabling target caching. Existing SHA-suffixed app names
must be replaced by the stable app name. First use of the new target namespace
is cold. Do not update trusted-server-deployer or publish a release in this PR.

## Verification

Local shell contracts cover valid/invalid revisions, outputs on failure, stable
paths with caching enabled/disabled, mixed-revision rejection, unknown target
contents, symlinks, traversal, and structured cache-key isolation. Existing CLI
contracts prove Cargo builds the host target instead of packaging a stale cached
root-level executable. GitHub smoke jobs restore an exact-revision marker and
prove another revision cannot restore it while dependency sources can be reused.
Log compiler cache statistics and require repeatable unchanged-library compiler
hits using a credential-free native/WASM fixture. Measure real-world CLI/WASM
timings after adoption separately; local tests cannot exercise GitHub's cache
service.
