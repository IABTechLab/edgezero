#!/usr/bin/env bash
set -euo pipefail

# Force real native/WASM recompilation at a stable path, rather than accepting
# Cargo freshness as compiler-cache evidence. Clean only this fixture package.
fixture="${GITHUB_WORKSPACE:?}/cache-restore-fixture"
target_dir="${CARGO_TARGET_DIR:?}"
expected="$(realpath "${RUNNER_TEMP:?}")/edgezero-rust-cache/cache-restore-smoke-v2/target"
[[ "$(realpath "$target_dir")" == "$expected" ]] || {
  printf '::error::compiler probe must use its confined fixture target directory\n' >&2
  exit 1
}
cd "$fixture"
host=$(rustc -vV | sed -n 's/^host: //p')
[[ -n "$host" ]] || exit 1
for target in "$host" wasm32-wasip1; do
  cargo build --locked --lib --target "$target"
  cargo clean --package edgezero-cache-probe --target "$target"
  sccache --zero-stats
  cargo build --locked --lib --target "$target"
  stats=$(sccache --show-stats --stats-format json)
  printf 'Compiler reuse for %s: %s\n' "$target" "$stats"
  jq -e '(.stats.cache_hits.counts.Rust // 0) > 0' <<<"$stats" >/dev/null || {
    printf '::error::no Rust compiler-cache hit after forced recompilation for %s\n' "$target" >&2
    exit 1
  }
done
