#!/usr/bin/env bash
set -euo pipefail

# Publish each target before downstream jobs test first-build compiler reuse.
fixture="${GITHUB_WORKSPACE:?}/cache-restore-fixture"
target_dir=$(realpath "${CARGO_TARGET_DIR:?}")
expected="$(realpath "${RUNNER_TEMP:?}")/edgezero-rust-cache/cache-restore-smoke-v2/target"
[[ "$target_dir" == "$expected" ]] || {
  printf '::error::compiler seed must use its confined fixture target directory\n' >&2
  exit 1
}
cd "$fixture"
host=$(rustc -vV | sed -n 's/^host: //p')
[[ "$host" =~ ^[A-Za-z0-9_][A-Za-z0-9_.-]*$ ]] || {
  printf '::error::rustc must report a safe host target component\n' >&2
  exit 1
}
for target in "$host" wasm32-wasip1; do
  for attempt in 1 2 3; do
    # Cargo must call sccache again after a failed publication, rather than
    # reporting locally fresh outputs as a successful cache seed.
    debug_dir="$target_dir/$target/debug"
    [[ ! -L "$target_dir/$target" && ! -L "$debug_dir" ]] || {
      printf '::error::compiler seed cleanup must not follow target symlinks\n' >&2
      exit 1
    }
    rm -rf -- "$debug_dir"
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
