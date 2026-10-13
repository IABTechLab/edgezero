#!/usr/bin/env bash
set -euo pipefail

# Review regressions: real local ownership behavior plus structural action gates.
ACTION_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)
WORK_DIR=$(mktemp -d "${TMPDIR:-/tmp}/edgezero-cache-review-test.XXXXXX")
trap 'rm -rf -- "$WORK_DIR"' EXIT
# shellcheck source=common.sh
source "$ACTION_DIR/tests/common.sh"
workspace="$WORK_DIR/workspace"
mkdir -p "$workspace/app" "$WORK_DIR/runner"
printf 'version = 3\n' >"$workspace/app/Cargo.lock"
git -C "$workspace/app" init -q
git -C "$workspace/app" add Cargo.lock
git -C "$workspace/app" -c core.hooksPath=/dev/null -c user.name=Test \
  -c user.email=test@example.com -c commit.gpgsign=false commit -qm seed
revision=$(git -C "$workspace/app" rev-parse HEAD)
prepare() {
  mkdir -p "${4:-$WORK_DIR/runner}"
  : >"$WORK_DIR/output"
  : >"$WORK_DIR/env"
  env GITHUB_WORKSPACE="$workspace" RUNNER_TEMP="${4:-$WORK_DIR/runner}" \
    RUNNER_OS=Linux RUNNER_ARCH=X64 GITHUB_OUTPUT="$WORK_DIR/output" GITHUB_ENV="$WORK_DIR/env" \
    EDGEZERO__RUST_CACHE__APP_NAME=review-app EDGEZERO__RUST_CACHE__WORKING_DIRECTORY=app \
    EDGEZERO__RUST_CACHE__CACHE_TARGET="$1" EDGEZERO__RUST_CACHE__SAVE_TARGET="$2" \
    EDGEZERO__RUST_CACHE__TARGET_CACHE_REVISION="$3" \
    "$ACTION_DIR/scripts/prepare.sh" >"$WORK_DIR/log" 2>&1
}
reject() {
  if prepare "$@"; then fail "accepted invalid setup: $*"; fi
  [[ ! -s "$WORK_DIR/output" && ! -s "$WORK_DIR/env" ]] || fail 'invalid setup published file commands'
}
prepare false false ''
assert_line "$WORK_DIR/output" 'already-initialized=false'
target=$(sed -n 's/^cargo-target-dir=//p' "$WORK_DIR/output")
printf 'newer local output\n' >"$target/marker"
prepare false false ''
assert_line "$WORK_DIR/output" 'already-initialized=true'
assert_line "$target/marker" 'newer local output'
printf 'next\n' >"$workspace/app/README"
git -C "$workspace/app" add README
git -C "$workspace/app" -c core.hooksPath=/dev/null -c user.name=Test \
  -c user.email=test@example.com -c commit.gpgsign=false commit -qm next
reject false false ''
reject false true "$revision" "$WORK_DIR/invalid-save"
reject true yes "$revision" "$WORK_DIR/invalid-save"
prepare true true "$revision" "$WORK_DIR/writer"
assert_line "$WORK_DIR/output" 'already-initialized=false'
prepare true true "$revision" "$WORK_DIR/writer"
assert_line "$WORK_DIR/output" 'already-initialized=true'
reject true false "$revision" "$WORK_DIR/writer"
newline_runner="$WORK_DIR/"$'runner\nCARGO_TARGET_DIR=/tmp'
mkdir -p "$newline_runner"
reject false false '' "$newline_runner"

command -v yq >/dev/null || fail 'mikefarah yq v4 is required for metadata contracts'
action="$ACTION_DIR/action.yml"
[[ "$(yq -r '.inputs."save-target".default' "$action")" == false ]] || fail 'target saves are not opt-in'
[[ "$(yq -r '.runs.steps[] | select(.id == "cargo-target") | .uses' "$action")" == actions/cache/restore@v5 ]] || fail 'default target step is not restore-only'
[[ "$(yq -r '.runs.steps[] | select(.id == "cargo-target-save") | .uses' "$action")" == actions/cache@v5 ]] || fail 'writer does not use the post-job save hook'
for step in cargo-target cargo-target-save; do
  STEP_ID="$step" yq -e '.runs.steps[] | select(.id == strenv(STEP_ID)) | .with."restore-keys" == null' "$action" >/dev/null || fail 'target fallback weakens revision isolation'
done
for step in cargo-sources cargo-target cargo-target-save sccache; do
  condition=$(STEP_ID="$step" yq -r '.runs.steps[] | select(.id == strenv(STEP_ID)) | .if' "$action")
  [[ "$condition" == *"already-initialized"* ]] || fail "repeat setup reruns $step"
done
printf 'cache review regressions passed\n'
