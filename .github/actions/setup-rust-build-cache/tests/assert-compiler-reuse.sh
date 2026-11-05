#!/usr/bin/env bash
set -euo pipefail

# Require native/WASM hits on the first builds of a fresh revision-specific
# target directory. Only a previous job may have populated compiler outputs.
fixture="${GITHUB_WORKSPACE:?}/cache-restore-fixture"
target_dir="${CARGO_TARGET_DIR:?}"
expected="$(realpath "${RUNNER_TEMP:?}")/edgezero-rust-cache/cache-restore-smoke-v2/target"
[[ "$(realpath "$target_dir")" == "$expected" ]] || {
  printf '::error::compiler probe must use its confined fixture target directory\n' >&2
  exit 1
}
script_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=compiler-stats.sh
source "$script_dir/compiler-stats.sh"
cd "$fixture"
host=$(rustc -vV | sed -n 's/^host: //p')
[[ -n "$host" ]] || exit 1
for target in "$host" wasm32-wasip1; do
  [[ ! -e "$target_dir/$target/debug" ]] || {
    printf '::error::compiler probe target must be fresh before its first build\n' >&2
    exit 1
  }
  sccache --zero-stats
  cargo build --locked --lib --target "$target"
  stats=$(sccache --show-stats --stats-format json)
  printf 'Compiler reuse for %s: %s\n' "$target" "$stats"
  compiler_hit "$stats" || {
    printf '::error::expected error-free Rust compiler-cache reuse on the first build for %s\n' "$target" >&2
    exit 1
  }
done
