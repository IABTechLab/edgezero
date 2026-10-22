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
workflow="${CACHE_WORKFLOW_FILE:-$ACTION_DIR/../../workflows/deploy-action.yml}"
seed=$(yq -r '.jobs."cache-restore-seed".steps[] | select(.name == "Seed native and WASM compiler outputs") | .run' "$workflow")
[[ "$seed" == 'bash .github/actions/setup-rust-build-cache/tests/seed-compiler-cache.sh' ]] || fail 'seed does not verify compiler publication'
for job in cache-restore-seed cache-restore-smoke cache-new-revision-smoke; do
  JOB_ID="$job" yq -e '.jobs[strenv(JOB_ID)].steps[] | select(.name == "Install native and WASM compiler targets") | .run == "bash .github/actions/setup-rust-build-cache/tests/setup-toolchain.sh"' "$workflow" >/dev/null || fail "$job does not share its Rust toolchain"
done
# shellcheck disable=SC2016 # GitHub expressions are literal metadata contracts.
expected_identity='${{ needs.cache-restore-seed.outputs.fixture-id }}'
for job in cache-restore-smoke cache-new-revision-smoke; do
  actual=$(JOB_ID="$job" yq -r '.jobs[strenv(JOB_ID)].steps[] | select(.id == "fixture") | .env.CACHE_FIXTURE_ID' "$workflow")
  [[ "$actual" == "$expected_identity" ]] || fail "$job does not reuse the seed fixture identity"
done
[[ "$(yq -r '.jobs."cache-restore-seed".steps[] | select(.id == "cache") | .with."save-target"' "$workflow")" == false ]] || fail 'seed relies on unverified post-job saving'
# shellcheck disable=SC2016 # Validate the actual inputs to both action attempts.
expected_path='${{ steps.cache.outputs.cargo-target-dir }}'
# shellcheck disable=SC2016
expected_key='${{ steps.cache.outputs.target-cache-key }}'
retry_condition="steps.target-verify-1.outputs.cache-hit != 'true'"
for step in target-save-1 target-verify-1 target-save-2 target-verify-2; do
  actual=$(STEP_ID="$step" yq -r '.jobs."cache-restore-seed".steps[] | select(.id == strenv(STEP_ID)) | .with.path' "$workflow")
  [[ "$actual" == "$expected_path" ]] || fail "$step uses a different cache path"
  actual=$(STEP_ID="$step" yq -r '.jobs."cache-restore-seed".steps[] | select(.id == strenv(STEP_ID)) | .with.key' "$workflow")
  [[ "$actual" == "$expected_key" ]] || fail "$step uses a different cache key"
  actual=$(STEP_ID="$step" yq -r '.jobs."cache-restore-seed".steps[] | select(.id == strenv(STEP_ID)) | .if // ""' "$workflow")
  if [[ "$step" == *-2 ]]; then
    [[ "$actual" == "$retry_condition" ]] || fail "$step retries without a first-attempt miss"
  else
    [[ -z "$actual" ]] || fail "$step skips the initial publication attempt"
  fi
done
for job in fixture-release store-free-release; do
  actual=$(JOB_ID="$job" yq -r '.jobs[strenv(JOB_ID)].needs // [] | length' "$workflow")
  [[ "$actual" == 0 ]] || fail "$job depends on cache probe availability"
done
for attempt in 1 2; do
  STEP_ID="target-save-$attempt" yq -e '.jobs."cache-restore-seed".steps[] | select(.id == strenv(STEP_ID)) | .uses == "actions/cache/save@v5"' "$workflow" >/dev/null || fail 'missing explicit target save'
  STEP_ID="target-verify-$attempt" yq -e '.jobs."cache-restore-seed".steps[] | select(.id == strenv(STEP_ID)) | .with."lookup-only" == true' "$workflow" >/dev/null || fail 'target publication is not verified'
done
yq -e '.jobs."cache-restore-seed".steps[] | select(.id == "target-verify-2") | .with."fail-on-cache-miss" == true' "$workflow" >/dev/null || fail 'exhausted target publication retries stay green'
# The action can return successfully before honoring fail-on-cache-miss when
# the cache feature is unavailable. Require actual exact-hit output as well.
publication_step='.jobs."cache-restore-seed".steps[] | select(.name == "Require exact target seed publication")'
publication_assertion=$(yq -r "$publication_step | .run" "$workflow")
[[ -n "$publication_assertion" && "$publication_assertion" != null ]] || fail 'missing exact publication assertion'
# shellcheck disable=SC2016 # GitHub outputs must feed the assertion literally.
first_hit='${{ steps.target-verify-1.outputs.cache-hit }}'
# shellcheck disable=SC2016
second_hit='${{ steps.target-verify-2.outputs.cache-hit }}'
[[ "$(yq -r "$publication_step | .env.TARGET_SEED_FIRST_HIT" "$workflow")" == "$first_hit" ]] || fail 'publication assertion ignores first lookup output'
[[ "$(yq -r "$publication_step | .env.TARGET_SEED_SECOND_HIT" "$workflow")" == "$second_hit" ]] || fail 'publication assertion ignores second lookup output'
[[ "$(yq -r "$publication_step | .if // \"\"" "$workflow")" == '' ]] || fail 'publication assertion can be skipped after successful lookup steps'
[[ "$(yq -r "$publication_step | .\"continue-on-error\" // false" "$workflow")" == false ]] || fail 'publication assertion failure is ignored'
for first in '' false true partial; do
  for second in '' false true partial; do
    status=0
    TARGET_SEED_FIRST_HIT="$first" TARGET_SEED_SECOND_HIT="$second" \
      bash -e -o pipefail -c "$publication_assertion" >"$WORK_DIR/publication.log" 2>&1 || status=$?
    if [[ "$first" == true || "$second" == true ]]; then
      [[ "$status" == 0 ]] || fail 'publication assertion rejects a verified exact hit'
    else
      [[ "$status" != 0 ]] || fail "publication assertion accepts unverified outputs '$first'/'$second'"
    fi
  done
done
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
