#!/usr/bin/env bash
set -euo pipefail

ACTION_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)
WORK_DIR=$(mktemp -d "${TMPDIR:-/tmp}/edgezero-cache-fixture-test.XXXXXX")
trap 'rm -rf -- "$WORK_DIR"' EXIT
# shellcheck source=common.sh
source "$ACTION_DIR/tests/common.sh"
fixture() {
  local directory="$1" attempt="$2" mode="$3" identity="$4"
  mkdir -p "$directory"
  GITHUB_WORKSPACE="$directory" GITHUB_OUTPUT="$directory/output" \
    GITHUB_RUN_ID=417 GITHUB_RUN_ATTEMPT="$attempt" CACHE_FIXTURE_ID="$identity" \
    bash "$ACTION_DIR/tests/make-fixture.sh" "$mode"
}
fixture "$WORK_DIR/seed" 1 seed 417-1
fixture "$WORK_DIR/retry" 2 seed 417-1
seed_sha=$(git -C "$WORK_DIR/seed/cache-restore-fixture" rev-parse HEAD)
retry_sha=$(git -C "$WORK_DIR/retry/cache-restore-fixture" rev-parse HEAD)
[[ "$seed_sha" == "$retry_sha" ]] || fail 'failed-jobs-only retry changes the seed revision'
fixture "$WORK_DIR/next" 2 next 417-1
[[ "$seed_sha" != "$(git -C "$WORK_DIR/next/cache-restore-fixture" rev-parse HEAD)" ]] || fail 'next revision is unchanged'
cmp "$WORK_DIR/seed/cache-restore-fixture/src/lib.rs" "$WORK_DIR/next/cache-restore-fixture/src/lib.rs" || fail 'next revision changes compiler inputs'
fixture "$WORK_DIR/other" 1 seed 418-1
if cmp -s "$WORK_DIR/seed/cache-restore-fixture/src/lib.rs" "$WORK_DIR/other/cache-restore-fixture/src/lib.rs"; then
  fail 'library is not unique to its seed run'
fi
grep -Fxq 'fixture-id=417-1' "$WORK_DIR/seed/output" || fail 'seed does not export fixture identity'
[[ "$(grep -c 'cargo build' "$ACTION_DIR/tests/assert-compiler-reuse.sh")" == 1 ]] || fail 'reuse probe primes its own cache'
if grep -q 'cargo clean' "$ACTION_DIR/tests/assert-compiler-reuse.sh"; then fail 'reuse probe accepts same-job rebuilds'; fi
workflow="$ACTION_DIR/../../workflows/deploy-action.yml"
seed=$(yq -r '.jobs."cache-restore-seed".steps[] | select(.name == "Seed native and WASM compiler outputs") | .run' "$workflow")
# shellcheck disable=SC2016 # Assert the literal shell command in workflow metadata.
[[ "$seed" == *'cargo build --locked --lib --target "$host"'* && "$seed" == *'cargo build --locked --lib --target wasm32-wasip1'* ]] || fail 'seed job does not compile both targets'
for job in cache-restore-seed cache-restore-smoke cache-new-revision-smoke; do
  JOB_ID="$job" yq -e '.jobs[strenv(JOB_ID)].steps[] | select(.name == "Install native and WASM compiler targets") | .run == "bash .github/actions/setup-rust-build-cache/tests/setup-toolchain.sh"' "$workflow" >/dev/null || fail "$job does not share its Rust toolchain"
done
# shellcheck disable=SC2016 # GitHub expressions are literal metadata contracts.
expected_identity='${{ needs.cache-restore-seed.outputs.fixture-id }}'
for job in cache-restore-smoke cache-new-revision-smoke; do
  actual=$(JOB_ID="$job" yq -r '.jobs[strenv(JOB_ID)].steps[] | select(.id == "fixture") | .env.CACHE_FIXTURE_ID' "$workflow")
  [[ "$actual" == "$expected_identity" ]] || fail "$job does not reuse the seed fixture identity"
done
[[ "$(yq -r '.jobs."cache-restore-seed".steps[] | select(.id == "cache") | .with."save-target"' "$workflow")" == true ]] || fail 'seed does not opt into saving'
for job in fixture-release store-free-release; do
  actual=$(JOB_ID="$job" yq -r '.jobs[strenv(JOB_ID)].steps[] | select(.uses == "./.github/actions/setup-rust-build-cache") | .with."cache-target"' "$workflow")
  [[ "$actual" == false ]] || fail "$job enables full target caching for a disposable fixture"
  actual=$(JOB_ID="$job" yq -r '.jobs[strenv(JOB_ID)].steps[] | select(.uses == "./.github/actions/setup-rust-build-cache") | (.with."save-target" // false)' "$workflow")
  [[ "$actual" == false ]] || fail "$job opts into saving full target caches"
done
fixture_script="$ACTION_DIR/../deploy-fastly/tests/make-smoke-fixture.sh"
grep -Fq 'GIT_AUTHOR_DATE=2000-01-01T00:00:00Z GIT_COMMITTER_DATE=2000-01-01T00:00:00Z' "$fixture_script" || fail 'application fixture dates are not deterministic'
grep -Fq '.edgezero-source-revision' "$fixture_script" || fail 'application fixture omits its path dependency revision'
printf 'cache fixture retry and first-build contracts passed\n'
