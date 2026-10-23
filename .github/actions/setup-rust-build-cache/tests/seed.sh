#!/usr/bin/env bash
set -euo pipefail
ACTION_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)
WORK_DIR=$(mktemp -d "${TMPDIR:-/tmp}/edgezero-cache-seed-test.XXXXXX")
trap 'rm -rf -- "$WORK_DIR"' EXIT
# shellcheck source=common.sh
source "$ACTION_DIR/tests/common.sh"
mkdir -p "$WORK_DIR/bin" "$WORK_DIR/workspace/cache-restore-fixture" "$WORK_DIR/runner"
export GITHUB_WORKSPACE="$WORK_DIR/workspace" RUNNER_TEMP="$WORK_DIR/runner"
export CARGO_TARGET_DIR="$RUNNER_TEMP/edgezero-rust-cache/cache-restore-smoke-v2/target"
export PROBE_LOG="$WORK_DIR/calls" PROBE_COUNT="$WORK_DIR/count"
cat >"$WORK_DIR/bin/rustc" <<'MOCK'
#!/usr/bin/env bash
printf 'host: %s\n' "${PROBE_HOST:-test-host}"
MOCK
cat >"$WORK_DIR/bin/sleep" <<'MOCK'
#!/usr/bin/env bash
printf 'sleep %s\n' "$*" >>"$PROBE_LOG"
MOCK
cat >"$WORK_DIR/bin/cargo" <<'MOCK'
#!/usr/bin/env bash
printf 'build %s\n' "$*" >>"$PROBE_LOG"
[[ "${PROBE_MODE:-}" != compile-failure ]] || exit 42
[[ "$PROBE_MODE" != compile-wasm-failure || "${*: -1}" != wasm32-wasip1 ]] || exit 42
[[ ! -d "$CARGO_TARGET_DIR/${*: -1}/debug" ]] || exit 43
mkdir -p "$CARGO_TARGET_DIR/${*: -1}/debug"
MOCK
cat >"$WORK_DIR/bin/sccache" <<'MOCK'
#!/usr/bin/env bash
if [[ "$1" == --zero-stats ]]; then
  printf 'zero\n' >>"$PROBE_LOG"
  exit 0
fi
[[ "$1" == --show-stats ]] || exit 2
count=$(cat "$PROBE_COUNT")
printf '%s\n' "$((count + 1))" >"$PROBE_COUNT"
if [[ "$PROBE_MODE" == permanent ||
  ( "$PROBE_MODE" == transient && "$count" == 0 ) ||
  ( "$PROBE_MODE" == wasm-transient && "$count" == 1 ) ||
  ( "$PROBE_MODE" == both-transient && ( "$count" == 0 || "$count" == 2 ) ) ||
  ( "$PROBE_MODE" == wasm-permanent && "$count" -ge 1 ) ]]; then
  printf '{"stats":{"cache_write_errors":1,"cache_writes":0}}\n'
elif [[ "$PROBE_MODE" == read-error ]]; then
  printf '{"stats":{"cache_writes":1,"cache_read_errors":1}}\n'
elif [[ "$PROBE_MODE" == compile-cache-error ]]; then
  printf '{"stats":{"cache_hits":{"counts":{"Rust":1}},"cache_errors":{"counts":{"Rust":1}}}}\n'
elif [[ "$PROBE_MODE" == malformed ]]; then
  printf 'not-json\n'
elif [[ "$PROBE_MODE" == empty ]]; then
  printf '{"stats":{}}\n'
elif [[ "$PROBE_MODE" == hit ]]; then
  printf '{"stats":{"cache_hits":{"counts":{"Rust":1}}}}\n'
else
  printf '{"stats":{"cache_writes":1}}\n'
fi
MOCK
chmod +x "$WORK_DIR/bin/"*
export PATH="$WORK_DIR/bin:$PATH"
run_seed() {
  rm -rf "$CARGO_TARGET_DIR"
  mkdir -p "$CARGO_TARGET_DIR"
  printf '0\n' >"$PROBE_COUNT"
  : >"$PROBE_LOG"
  PROBE_MODE="$1" bash "$ACTION_DIR/tests/seed-compiler-cache.sh" >"$WORK_DIR/log" 2>&1
}
run_seed transient || fail 'compiler seed does not recover from a transient write failure'
[[ "$(grep -c '^build ' "$PROBE_LOG")" == 3 ]] || fail 'retry does not rebuild fresh outputs'
[[ "$(grep -c '^sleep ' "$PROBE_LOG")" == 1 ]] || fail 'transient error does not back off'
run_seed hit || fail 'existing compiler hit is not accepted as published'
[[ "$(grep -c '^build ' "$PROBE_LOG")" == 2 ]] || fail 'successful seed does not build both targets'
grep -q -- '--target test-host$' "$PROBE_LOG" || fail 'successful seed omits native target'
grep -q -- '--target wasm32-wasip1$' "$PROBE_LOG" || fail 'successful seed omits WASM target'
[[ "$(grep -c '^zero$' "$PROBE_LOG")" == 2 ]] || fail 'successful seed does not reset stats for each target'
for mode in wasm-transient both-transient; do
  run_seed "$mode" || fail "$mode does not recover"
  expected=3
  if [[ "$mode" == both-transient ]]; then expected=4; fi
  [[ "$(grep -c '^build ' "$PROBE_LOG")" == "$expected" ]] || fail "$mode does not rebuild every attempt"
  [[ "$(grep -c '^zero$' "$PROBE_LOG")" == "$expected" ]] || fail "$mode does not reset every attempt"
done
for mode in permanent empty compile-failure compile-wasm-failure wasm-permanent read-error compile-cache-error malformed; do
  if run_seed "$mode"; then fail "accepted unsuccessful compiler seeding: $mode"; fi
  case "$mode" in
    compile-failure) expected=1 ;;
    compile-wasm-failure) expected=2 ;;
    wasm-permanent) expected=4 ;;
    *) expected=3 ;;
  esac
  [[ "$(grep -c '^build ' "$PROBE_LOG")" == "$expected" ]] || fail "$mode violates per-target retry limits"
  [[ "$(grep -c '^zero$' "$PROBE_LOG")" == "$expected" ]] || fail "$mode uses stale stats"
done
# Cleanup must never follow restored symlinks outside the fixture root.
for target in test-host wasm32-wasip1; do
  for component in namespace debug; do
    rm -rf -- "$CARGO_TARGET_DIR" "$WORK_DIR/outside"
    mkdir -p "$CARGO_TARGET_DIR" "$WORK_DIR/outside/debug"
    printf keep >"$WORK_DIR/outside/debug/marker"
    if [[ "$component" == namespace ]]; then
      ln -s "$WORK_DIR/outside" "$CARGO_TARGET_DIR/$target"
    else
      mkdir -p "$CARGO_TARGET_DIR/$target"
      ln -s "$WORK_DIR/outside/debug" "$CARGO_TARGET_DIR/$target/debug"
    fi
    status=0
    PROBE_MODE=hit bash "$ACTION_DIR/tests/seed-compiler-cache.sh" >"$WORK_DIR/log" 2>&1 || status=$?
    [[ -f "$WORK_DIR/outside/debug/marker" ]] || fail "cleanup escaped through $target $component symlink"
    [[ "$status" != 0 ]] || fail "accepted $target $component symlink"
    grep -Fq 'compiler seed cleanup must not follow target symlinks' "$WORK_DIR/log" || fail 'symlink case failed for an unrelated reason'
  done
done
rm -rf -- "$CARGO_TARGET_DIR"
mkdir -p "$CARGO_TARGET_DIR" "$WORK_DIR/runner/edgezero-rust-cache/debug"
printf keep >"$WORK_DIR/runner/edgezero-rust-cache/debug/marker"
if PROBE_HOST=../.. PROBE_MODE=hit bash "$ACTION_DIR/tests/seed-compiler-cache.sh" >"$WORK_DIR/log" 2>&1; then
  fail 'accepted traversal in compiler host'
fi
[[ -f "$WORK_DIR/runner/edgezero-rust-cache/debug/marker" ]] || fail 'removed output through host traversal'
grep -Fq 'rustc must report a safe host target component' "$WORK_DIR/log" || fail 'traversal case failed for an unrelated reason'
# A mistaken directory must never be removed by retry cleanup.
export CARGO_TARGET_DIR="$WORK_DIR/outside"
mkdir -p "$CARGO_TARGET_DIR"
printf keep >"$CARGO_TARGET_DIR/marker"
if PROBE_MODE=hit bash "$ACTION_DIR/tests/seed-compiler-cache.sh" >"$WORK_DIR/log" 2>&1; then
  fail 'accepted unconfined compiler seed directory'
fi
[[ -f "$CARGO_TARGET_DIR/marker" ]] || fail 'removed unconfined output'
printf 'compiler seed backoff contracts passed\n'
