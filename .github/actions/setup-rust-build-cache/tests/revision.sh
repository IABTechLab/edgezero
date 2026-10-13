#!/usr/bin/env bash
set -euo pipefail

# Local behavioral and structural contracts; no network or provider credentials.
ACTION_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)
WORK_DIR=$(mktemp -d "${TMPDIR:-/tmp}/edgezero-cache-revision-test.XXXXXX")
trap 'rm -rf -- "$WORK_DIR"' EXIT
revision_a=1111111111111111111111111111111111111111
revision_b=2222222222222222222222222222222222222222
mkdir -p "$WORK_DIR/workspace/app" "$WORK_DIR/workspace/other"
printf 'version = 3\n' >"$WORK_DIR/workspace/app/Cargo.lock"
cp "$WORK_DIR/workspace/app/Cargo.lock" "$WORK_DIR/workspace/other/Cargo.lock"

fail() { printf 'cache revision test failed: %s\n' "$*" >&2; exit 1; }
assert_line() { grep -Fqx -- "$2" "$1" || fail "expected '$2' in $1"; }
value() { sed -n "s/^$1=//p" "$WORK_DIR/output"; }
prepare() {
  local runner="$1" mode="$2" revision="$3" project="${4:-app}"
  mkdir -p "$runner"
  : >"$WORK_DIR/output"
  : >"$WORK_DIR/env"
  env GITHUB_WORKSPACE="$WORK_DIR/workspace" RUNNER_TEMP="$runner" \
    RUNNER_OS=Linux RUNNER_ARCH=X64 GITHUB_OUTPUT="$WORK_DIR/output" \
    GITHUB_ENV="$WORK_DIR/env" EDGEZERO__RUST_CACHE__APP_NAME=example-app \
    EDGEZERO__RUST_CACHE__CACHE_TARGET="$mode" \
    EDGEZERO__RUST_CACHE__TARGET_CACHE_REVISION="$revision" \
    EDGEZERO__RUST_CACHE__WORKING_DIRECTORY="$project" \
    "$ACTION_DIR/scripts/prepare.sh" >"$WORK_DIR/log" 2>&1
}
reject() {
  local message="$1"
  shift
  if prepare "$@"; then fail "accepted $message"; fi
  [[ ! -s "$WORK_DIR/output" && ! -s "$WORK_DIR/env" ]] ||
    fail "published outputs/environment for $message"
}

for invalid in '' main abc 111111111111111111111111111111111111111 \
  11111111111111111111111111111111111111111A '../outside' $'abc\nCARGO_TARGET_DIR=/tmp'; do
  reject "invalid revision '$invalid'" "$WORK_DIR/invalid" true "$invalid"
  grep -Fq "target-cache-revision" "$WORK_DIR/log" || fail "missing revision diagnostic"
done
reject 'invalid optional revision' "$WORK_DIR/invalid" false main

prepare "$WORK_DIR/first" true "$revision_a"
target_a=$(value cargo-target-dir)
sources_a=$(value source-cache-key)
target_key_a=$(value target-cache-key)
[[ -n "$sources_a" && -n "$target_key_a" ]] || fail 'missing cache key outputs'
[[ "$target_key_a" == *"-rust-target-v2-"*"-$revision_a-exact" ]] || fail 'target key is not revision-isolated v2'
[[ "$sources_a" != *"$revision_a"* ]] || fail 'source key contains source revision'
assert_line "$WORK_DIR/env" "CARGO_TARGET_DIR=$target_a"
assert_line "$WORK_DIR/env" "EDGEZERO__RUST_CACHE__TARGET_DIR=$target_a"
[[ "$target_a" == "$(realpath "$WORK_DIR/first")/edgezero-rust-cache/example-app/target" ]] ||
  fail 'target path is not stable and app-specific'
printf 'retained\n' >"$target_a/marker"
prepare "$WORK_DIR/first" true "$revision_a"
assert_line "$target_a/marker" retained
reject 'mixed revision in one target directory' "$WORK_DIR/first" true "$revision_b"
reject 'mixed application workspace' "$WORK_DIR/first" true "$revision_a" other

prepare "$WORK_DIR/second" true "$revision_b"
[[ "$(value source-cache-key)" == "$sources_a" ]] || fail 'new SHA changed source cache key'
[[ "$(value target-cache-key)" != "$target_key_a" ]] || fail 'new SHA reused target cache key'
[[ ! -e "$(value cargo-target-dir)/marker" ]] || fail 'new runner inherited target contents'
prepare "$WORK_DIR/sha256" true 3333333333333333333333333333333333333333333333333333333333333333
prepare "$WORK_DIR/prefix-collision" true 1111111111111111111111111111111111111111333333333333333333333333
[[ "$(value target-cache-key)" != "$target_key_a"* ]] || fail '40-character revision can prefix-match a 64-character revision'

prepare "$WORK_DIR/uncached" false ''
uncached_target=$(value cargo-target-dir)
assert_line "$WORK_DIR/env" "CARGO_TARGET_DIR=$uncached_target"
[[ "$uncached_target" == "$(realpath "$WORK_DIR/uncached")/edgezero-rust-cache/example-app/target" ]] ||
  fail 'disabled target cache uses a random directory'
reject 'unidentified repeated target reuse' "$WORK_DIR/uncached" false ''
prepare "$WORK_DIR/uncached-revision" false "$revision_a"
reject 'mixed revision with target caching disabled' "$WORK_DIR/uncached-revision" false "$revision_b"

# Changing dependencies can reuse sources via the stable prefix, never targets.
prepare "$WORK_DIR/lock-before" true "$revision_a"
prefix_a=$(value source-cache-restore-prefix)
printf '# another dependency\n' >>"$WORK_DIR/workspace/app/Cargo.lock"
prepare "$WORK_DIR/lock-after" true "$revision_a"
[[ "$(value source-cache-key)" != "$sources_a" ]] || fail 'lockfile change did not change source key'
[[ "$(value source-cache-restore-prefix)" == "$prefix_a" ]] || fail 'lockfile change changed source restore prefix'
[[ "$(value target-cache-key)" != "$target_key_a" ]] || fail 'lockfile change did not change target key'

for component in root app target selection; do
  runner="$WORK_DIR/symlink-$component"
  outside="$WORK_DIR/outside-$component"
  mkdir -p "$runner" "$outside"
  case "$component" in
    root) ln -s "$outside" "$runner/edgezero-rust-cache" ;;
    app)
      mkdir -p "$runner/edgezero-rust-cache"
      ln -s "$outside" "$runner/edgezero-rust-cache/example-app" ;;
    target | selection)
      mkdir -p "$runner/edgezero-rust-cache/example-app"
      ln -s "$outside" "$runner/edgezero-rust-cache/example-app/$component" ;;
  esac
  reject "symlinked $component" "$runner" true "$revision_a"
  [[ -z "$(ls -A "$outside")" ]] || fail "wrote outside target through $component symlink"
done
mkdir -p "$WORK_DIR/unowned/edgezero-rust-cache/example-app/target"
printf 'unowned\n' >"$WORK_DIR/unowned/edgezero-rust-cache/example-app/target/sentinel"
reject 'unowned target contents' "$WORK_DIR/unowned" true "$revision_a"
assert_line "$WORK_DIR/unowned/edgezero-rust-cache/example-app/target/sentinel" unowned

newline_target="$WORK_DIR/newline/edgezero-rust-cache/example-app/target"
mkdir -p "$newline_target"
printf 'unowned\n' >"$newline_target/"$'\n'
reject 'newline-only filename in unowned target' "$WORK_DIR/newline" true "$revision_a"
assert_line "$newline_target/"$'\n' unowned

unreadable_target="$WORK_DIR/unreadable/edgezero-rust-cache/example-app/target"
mkdir -p "$unreadable_target"
printf 'unowned\n' >"$unreadable_target/sentinel"
chmod 300 "$unreadable_target"
# Root and some non-POSIX filesystems bypass directory read permissions.
if ls -A "$unreadable_target" >/dev/null 2>&1; then
  chmod 700 "$unreadable_target"
  printf 'unreadable directory regression skipped: permissions are not enforced\n'
else
  if prepare "$WORK_DIR/unreadable" true "$revision_a"; then
    chmod 700 "$unreadable_target"
    fail 'accepted unreadable target contents'
  fi
  chmod 700 "$unreadable_target"
  [[ ! -s "$WORK_DIR/output" && ! -s "$WORK_DIR/env" ]] || fail 'published outputs for unreadable target'
  assert_line "$unreadable_target/sentinel" unowned
fi

command -v yq >/dev/null || fail 'mikefarah yq v4 is required'
action="$ACTION_DIR/action.yml"
[[ "$(yq -r '.inputs."target-cache-revision".description' "$action")" != null ]] || fail 'missing revision input'
assert_metadata() {
  local query="$1" expected="$2"
  [[ "$(yq -r "$query" "$action")" == "$expected" ]] || fail "incorrect metadata at $query"
}
# shellcheck disable=SC2016 # Literal GitHub expressions are the tested contract.
assert_metadata '.runs.steps[] | select(.id == "cargo-sources") | .with.key' '${{ steps.prepare.outputs['\''source-cache-key'\''] }}'
# shellcheck disable=SC2016
assert_metadata '.runs.steps[] | select(.id == "cargo-target") | .with.key' '${{ steps.prepare.outputs['\''target-cache-key'\''] }}'
assert_metadata '.runs.steps[] | select(.id == "cargo-target") | .with."restore-keys"' null
for output in source-cache-hit target-cache-hit source-cache-key target-cache-key cargo-target-dir; do
  [[ "$(yq -r ".outputs.\"$output\".value" "$action")" != null ]] || fail "missing $output output"
done
printf 'cache revision contracts passed\n'
