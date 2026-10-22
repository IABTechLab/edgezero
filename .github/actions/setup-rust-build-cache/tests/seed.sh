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
printf 'host: test-host\n'
MOCK
cat >"$WORK_DIR/bin/sleep" <<'MOCK'
#!/usr/bin/env bash
printf 'sleep %s\n' "$*" >>"$PROBE_LOG"
MOCK
cat >"$WORK_DIR/bin/cargo" <<'MOCK'
#!/usr/bin/env bash
printf 'build %s\n' "$*" >>"$PROBE_LOG"
[[ "${PROBE_MODE:-}" != compile-failure ]] || exit 42
[[ ! -d "$CARGO_TARGET_DIR/${*: -1}/debug" ]] || exit 43
mkdir -p "$CARGO_TARGET_DIR/${*: -1}/debug"
MOCK
cat >"$WORK_DIR/bin/sccache" <<'MOCK'
#!/usr/bin/env bash
[[ "$1" == --show-stats ]] || exit 0
count=$(cat "$PROBE_COUNT")
printf '%s\n' "$((count + 1))" >"$PROBE_COUNT"
if [[ "$PROBE_MODE" == permanent || ( "$PROBE_MODE" == transient && "$count" == 0 ) ]]; then
  printf '{"stats":{"cache_write_errors":1,"cache_writes":0}}\n'
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
for mode in permanent empty compile-failure; do
  if run_seed "$mode"; then fail "accepted unsuccessful compiler seeding: $mode"; fi
  if [[ "$mode" == compile-failure ]]; then
    [[ "$(grep -c '^build ' "$PROBE_LOG")" == 1 ]] || fail 'compiler errors are retried'
  else
    [[ "$(grep -c '^build ' "$PROBE_LOG")" == 3 ]] || fail 'cache retries are not bounded to three attempts'
  fi
done
# A mistaken directory must never be removed by retry cleanup.
export CARGO_TARGET_DIR="$WORK_DIR/outside"
mkdir -p "$CARGO_TARGET_DIR"
printf keep >"$CARGO_TARGET_DIR/marker"
if PROBE_MODE=hit bash "$ACTION_DIR/tests/seed-compiler-cache.sh" >"$WORK_DIR/log" 2>&1; then
  fail 'accepted unconfined compiler seed directory'
fi
[[ -f "$CARGO_TARGET_DIR/marker" ]] || fail 'removed unconfined output'
printf 'compiler seed backoff contracts passed\n'
