# Viceroy 0.21.0 Body Readiness Repair

This patch is part of PR #275. It repairs the local runtime, not the Fastly SDK or hosted
service. The repository's SDK/runtime version pins are unchanged. Do not replace an installed
Viceroy silently or describe a patched executable as unmodified 0.21.0 evidence.

Base: official Fastly Viceroy commit `a69d982d1d95e13c0fc9f92683b9118639bb4b04`, tag `v0.21.0`.
The upstream Cargo.lock SHA-256 is
`d642d177081f42aec75d25c0d1124c013d3abcb081886251420a90e9c53c1a2a`.
Upstream source is licensed under Apache-2.0 WITH LLVM-exception; its license files remain
authoritative. The patch changes only `src/body.rs` and includes co-located regression tests.

## Repair

Unmodified `Body::await_ready` consumes errors and reports empty frames as ready even though
`Body::read` skips empty frames and may then wait. The repair queues the original owned error,
skips empty frames cooperatively, and yields when gzip produces no output. Queuing rather than
adding an out-of-band error field preserves ownership through append/iteration. Size hints
remain unknown while an error is queued. No error is stringified, replaced with EOF or cloned.

Tests assert the first read poll is ready without another source poll after readiness. They
cover one-byte/16 KiB buffers and retained remainder, fresh readiness after exhaustion, empty
frames before data/pending/EOF, repeated error observation with original error identity,
append, cancellation, ready siblings, decoder errors/no-output and between-byte timeout.
The bounded-poll assertion rejects an infinite empty compressed source before its timer test
can run; a broken implementation cannot hang that regression indefinitely.

## Apply and Build

Run from the EdgeZero checkout with Rust 1.98.1. Choose a new temporary source directory;
the commands never modify the installed executable or the EdgeZero dependency graph. Fetching
the official source/dependencies needs network access; builds/tests afterward are offline/locked.

```sh
set -eu
EDGEZERO_ROOT=$(git rev-parse --show-toplevel)
: "${VICEROY_SOURCE:?choose a new absolute temporary directory}"
test ! -e "$VICEROY_SOURCE"
git clone --depth 1 --branch v0.21.0 https://github.com/fastly/Viceroy.git "$VICEROY_SOURCE"
base=$(git -C "$VICEROY_SOURCE" rev-parse --verify HEAD)
test "$base" = a69d982d1d95e13c0fc9f92683b9118639bb4b04
patch="$EDGEZERO_ROOT/patches/viceroy/0.21.0-body-readiness.patch"
git -C "$VICEROY_SOURCE" apply --check "$patch"
git -C "$VICEROY_SOURCE" apply "$patch"
export RUSTUP_TOOLCHAIN=1.98.1
export ASDF_RUST_VERSION=1.98.1
export CARGO_TARGET_DIR="$VICEROY_SOURCE/target"
export CARGO_PROFILE_DEV_DEBUG=0
export CARGO_INCREMENTAL=0
cargo fetch --locked --manifest-path "$VICEROY_SOURCE/Cargo.toml"
cargo test --offline --locked --manifest-path "$VICEROY_SOURCE/Cargo.toml" -p viceroy-lib --lib body::tests::readiness_
cargo test --offline --locked --manifest-path "$VICEROY_SOURCE/Cargo.toml" -p viceroy-lib --lib body::tests::
cargo build --offline --locked --manifest-path "$VICEROY_SOURCE/Cargo.toml" -p viceroy
```

Require ten passing new readiness tests and 24 passing body/collector tests, with zero failures.
The second filter also matches `collecting_body::tests`; these counts are not hosted cases.
The development profile has no debug symbols/incremental state; record actual compiler,
profile, source, lockfile, patch and binary hashes rather than inferring identity from version text.

## Guest-Level Control

Run the opt-in full SDK readiness/timing diagnostic with an explicit runtime override and a new
empty evidence directory:

```sh
unset CARGO_TARGET_DIR
VICEROY_BIN="$VICEROY_SOURCE/target/debug/viceroy" scripts/smoke_test_reusable_app.sh --adapter fastly --suite outbound-readiness --output NEW_EVIDENCE_DIRECTORY
```

The fixture must still use its own target directory, as the existing launcher enforces. The
expanded suite requires 135 probes and 147 origin receipts, including three direct/selected
error comparisons. Unmodified 0.21.0 remains a failed control; the repaired executable must
preserve `UnexpectedEof` in both modes. Any HTTP-readiness primitive or exchange-timing failure
exits 1. Timer-only controls are separate observations: lateness remains recorded, not waived.
Even a passing
matrix exits 2: production-path integration, exact-version review, backend policy and hosted
evidence are still required. Do not turn either disposition into an overall gate pass.

The current-operation multiplexer checkpoint extends this same suite with eighteen raw mixed/
full-buffer probes and twelve actual Send/Batch client probes. The 135-probe baseline stays
separately validated. Actual-client probes require exact payloads, typed source errors and
request-correlated origin receipts; preflight failures do not count as exercised receive paths.
CI selects the separate `outbound-client-progress` suite and requires exactly exit 2, exactly one
passing functional inventory with twelve probes, twelve expected probes and eighteen origin
requests, `qualification: false`, an explicit unverified/unqualified disposition and no failure
records. This functional gate does not run the full SDK timing matrix. The opt-in
`outbound-readiness` diagnostic above retains that complete matrix, its strict timing validators
and exit-1 failures unchanged; timing misses are not waived or relabeled as passes.

CI keeps its stock Viceroy installation, clones the exact upstream revision, checks lockfile
and patch hashes, and builds/tests the repair in a separate cached target directory. Fastly
runtime tests and the functional CI suite explicitly select that absolute executable through
`CARGO_TARGET_WASM32_WASIP1_RUNNER` and `VICEROY_BIN`. Neither PATH nor the installed runtime pin
is replaced. Callback-wide scheduling, hosted runtime promotion and resource qualification remain
open; these local checks cannot establish hosted interruption or cleanup bounds.

Keep the historical failed report and raw evidence unchanged. A separate repair record must
include exact upstream/patch/host-binary/guest-binary identities, commands, counts and outcomes.
Neither unit tests nor this local control approve backend bound B, optimizer horizon H(K),
production dispatch changes, capability promotion, candidate freeze or hosted qualification.

The [local repair record](../../docs/audits/evidence/2026-10-23-viceroy-readiness-repair.json)
retains both stock and repaired probe events with distinct host executable hashes. All 135
Viceroy library tests pass. Strict upstream clippy under Rust 1.98.1 reports the same diagnostics
on the untouched base and repaired source (7 library / 25 test errors); this patch does not
suppress or refactor those unrelated upstream warnings. EdgeZero's own strict clippy is separate.
