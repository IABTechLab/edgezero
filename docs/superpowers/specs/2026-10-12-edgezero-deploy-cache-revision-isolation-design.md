# Revision-isolated Rust build caches

## Goal

Reuse dependency sources and valid compiler results across application commits,
without restoring writable Cargo target artifacts from another source revision.
This implements [issue #416](https://github.com/stackpop/edgezero/issues/416). Deployment
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
Reject subsequent setup for a different workspace, revision, lockfile or cache
settings. Identical repeats skip remote setup. Without an explicit revision,
local ownership can use Git HEAD; non-Git workspaces require an explicit revision
for repeat setup. No cleanup of unrelated or unowned files occurs.

## Scope and migration

Update all in-repository action callers and the adoption guide. New releases
require external consumers to use a stable app-name and explicitly pass their
resolved source SHA when enabling target caching. Existing SHA-suffixed app names
must be replaced by the stable app name. First use of the new target namespace
is cold. Deployer adoption is delivered separately in its own PR. Do not publish
a release in this PR.

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

## Review remediation (2026-10-13)

Target caching deliberately replaces #381's lockfile-only target policy: source
downloads and compiler outputs can cross revisions, but writable target archives
cannot. This costs a cold target on each new source SHA; it is not a promise of a
fully warm application build. Do not introduce a lockfile-only target fallback.

Add `save-target`, default false, requiring `cache-target: true` when enabled.
Read-only consumers use `actions/cache/restore`; opted-in writers use the combined
cache action, whose post-job hook saves only after successful builds. Both use
the same exact revision key and neither has target restore prefixes. CI fixtures
save full targets only from main; the small cross-job seed explicitly saves.
The deployer adopts the new contract and permits target writes only when its
workflow ref is main, independently of the selected application source branch.

Bind local ownership to source identity, workspace, lock digest, and cache
settings. When remote targets are disabled and no revision is supplied, use the
workspace's Git HEAD for local ownership if available; a non-Git workspace still
requires an explicit revision for repeat setup. An identical repeat publishes
the same path and keys but skips remote restores and sccache initialization.
Reject different selections, symlinks, unowned contents, and control characters
in paths before writing file-command channels. Keep the marker outside targets.

The cross-job seed compiles a run-specific library for native and WASM targets.
It exports its fixture identity so retrying only downstream jobs reconstructs
the same seed source, even when GITHUB_RUN_ATTEMPT changes. The next revision
changes only README and requires compiler hits on its first native/WASM builds,
using a fresh target and the same .tool-versions toolchain as the seed. All three
jobs install and select that shared toolchain. Existing
application fixtures use fixed commit dates and record the upstream EdgeZero
revision so unchanged inputs have stable SHAs without hiding dependency changes.

This is a breaking build-path/cache contract for callers that assumed an
application-local target directory or implicit target saves. Provide a complete
adoption example, migration checklist, safe repeat behavior, main-cache scope,
and explicit sccache limitations (bin/dylib/cdylib/proc-macro, build-script
execution, and linking). Deployment semantics and credential boundaries stay
unchanged. Do not clean caches, deploy, merge, or release during remediation.
