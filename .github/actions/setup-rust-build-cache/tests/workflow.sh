#!/usr/bin/env bash
set -euo pipefail

# Contracts for the cache probe jobs in deploy-action.yml. publication.sh
# passes mutated copies of the workflow as the first argument.
ACTION_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)
WORK_DIR=$(mktemp -d "${TMPDIR:-/tmp}/edgezero-cache-workflow-test.XXXXXX")
trap 'rm -rf -- "$WORK_DIR"' EXIT
# shellcheck source=common.sh
source "$ACTION_DIR/tests/common.sh"
workflow="${1:-$ACTION_DIR/../../workflows/deploy-action.yml}"
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
[[ "$(yq -r '.jobs."cache-restore-seed".steps[] | select(.id == "cache") | .with."save-target"' "$workflow")" == true ]] || fail 'seed no longer exercises the action save path'
for job in fixture-release store-free-release; do
  actual=$(JOB_ID="$job" yq -r '.jobs[strenv(JOB_ID)].needs // [] | length' "$workflow")
  [[ "$actual" == 0 ]] || fail "$job depends on cache probe availability"
done
# Exercise both publication surfaces against their actual workflow wiring.
for kind in source target; do
  if [[ "$kind" == source ]]; then
    expected_path=$(yq -r '.runs.steps[] | select(.id == "cargo-sources") | .with.path' "$ACTION_DIR/action.yml")
  else
    # shellcheck disable=SC2016 # Literal GitHub output reference.
    expected_path='${{ steps.cache.outputs.cargo-target-dir }}'
  fi
  expected_key="\${{ steps.cache.outputs.$kind-cache-key }}"
  retry_condition="steps.$kind-verify-1.outputs.cache-hit != 'true'"
  for step in "$kind-save-1" "$kind-verify-1" "$kind-save-2" "$kind-verify-2"; do
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
  for attempt in 1 2; do
    STEP_ID="$kind-save-$attempt" yq -e '.jobs."cache-restore-seed".steps[] | select(.id == strenv(STEP_ID)) | .uses == "actions/cache/save@v5"' "$workflow" >/dev/null || fail 'missing explicit cache save'
    STEP_ID="$kind-verify-$attempt" yq -e '.jobs."cache-restore-seed".steps[] | select(.id == strenv(STEP_ID)) | .with."lookup-only" == true' "$workflow" >/dev/null || fail 'cache publication is not verified'
  done
  STEP_ID="$kind-verify-2" yq -e '.jobs."cache-restore-seed".steps[] | select(.id == strenv(STEP_ID)) | .with."fail-on-cache-miss" == true' "$workflow" >/dev/null || fail 'exhausted publication retries stay green'
  # The action can return successfully before honoring fail-on-cache-miss.
  # Require actual exact-hit output, including when availability checks fail.
  publication_step=".jobs.\"cache-restore-seed\".steps[] | select(.name == \"Require exact $kind seed publication\")"
  publication_assertion=$(yq -r "$publication_step | .run" "$workflow")
  [[ -n "$publication_assertion" && "$publication_assertion" != null ]] || fail 'missing exact publication assertion'
  first_hit="\${{ steps.$kind-verify-1.outputs.cache-hit }}"
  second_hit="\${{ steps.$kind-verify-2.outputs.cache-hit }}"
  [[ "$(yq -r "$publication_step | .env.CACHE_SEED_FIRST_HIT" "$workflow")" == "$first_hit" ]] || fail 'publication assertion ignores first lookup output'
  [[ "$(yq -r "$publication_step | .env.CACHE_SEED_SECOND_HIT" "$workflow")" == "$second_hit" ]] || fail 'publication assertion ignores second lookup output'
  [[ "$(yq -r "$publication_step | .if // \"\"" "$workflow")" == '' ]] || fail 'publication assertion can be skipped after successful lookup steps'
  [[ "$(yq -r "$publication_step | .\"continue-on-error\" // false" "$workflow")" == false ]] || fail 'publication assertion failure is ignored'
  for first in '' false true partial; do
    for second in '' false true partial; do
      status=0
      CACHE_SEED_FIRST_HIT="$first" CACHE_SEED_SECOND_HIT="$second" \
        bash -e -o pipefail -c "$publication_assertion" >"$WORK_DIR/publication.log" 2>&1 || status=$?
      if [[ "$first" == true || "$second" == true ]]; then
        [[ "$status" == 0 ]] || fail 'publication assertion rejects a verified exact hit'
      else
        [[ "$status" != 0 ]] || fail "publication assertion accepts unverified outputs '$first'/'$second'"
      fi
    done
  done
done
for job in fixture-release store-free-release; do
  actual=$(JOB_ID="$job" yq -r '.jobs[strenv(JOB_ID)].steps[] | select(.uses == "./.github/actions/setup-rust-build-cache") | .with."cache-target"' "$workflow")
  [[ "$actual" == false ]] || fail "$job enables full target caching for a disposable fixture"
  actual=$(JOB_ID="$job" yq -r '.jobs[strenv(JOB_ID)].steps[] | select(.uses == "./.github/actions/setup-rust-build-cache") | (.with."save-target" // false)' "$workflow")
  [[ "$actual" == false ]] || fail "$job opts into saving full target caches"
done
printf 'cache probe workflow contracts passed\n'
