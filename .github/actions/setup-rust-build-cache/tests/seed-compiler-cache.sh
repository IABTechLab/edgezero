#!/usr/bin/env bash
set -euo pipefail

# Publish each target before downstream jobs test first-build compiler reuse.
fixture="${GITHUB_WORKSPACE:?}/cache-restore-fixture"
target_dir="${CARGO_TARGET_DIR:?}"
expected="$(realpath "${RUNNER_TEMP:?}")/edgezero-rust-cache/cache-restore-smoke-v2/target"
[[ "$(realpath "$target_dir")" == "$expected" ]] || {
  printf '::error::compiler seed must use its confined fixture target directory\n' >&2
  exit 1
}
cd "$fixture"
host=$(rustc -vV | sed -n 's/^host: //p')
[[ -n "$host" ]] || exit 1
for target in "$host" wasm32-wasip1; do
  for attempt in 1 2 3; do
    # Cargo must call sccache again after a failed publication, rather than
    # reporting locally fresh outputs as a successful cache seed.
    rm -rf -- "$target_dir/$target/debug"
    sccache --zero-stats
    cargo build --locked --lib --target "$target"
    stats=$(sccache --show-stats --stats-format json)
    printf 'Compiler seed for %s (attempt %s): %s\n' "$target" "$attempt" "$stats"
    if jq -e '((.stats.cache_hits.counts.Rust // 0) + (.stats.cache_writes // 0)) > 0 and (.stats.cache_errors.counts.Rust // 0) == 0 and (.stats.cache_read_errors // 0) == 0 and (.stats.cache_write_errors // 0) == 0' <<<"$stats" >/dev/null; then
      break
    fi
    if [[ "$attempt" == 3 ]]; then
      printf '::error::failed to publish compiler cache for %s after three attempts\n' "$target" >&2
      exit 1
    fi
    printf '::warning::compiler cache seed failed; retrying after backoff\n'
    sleep "$((attempt * 60))"
  done
done
