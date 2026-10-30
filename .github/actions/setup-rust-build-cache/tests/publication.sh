#!/usr/bin/env bash
# Mutate the workflow itself so contract checks cannot merely test a detached if.
set -euo pipefail
ACTION_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)
WORK_DIR=$(mktemp -d "${TMPDIR:-/tmp}/edgezero-cache-publication-test.XXXXXX")
trap 'rm -rf -- "$WORK_DIR"' EXIT
# shellcheck source=common.sh
source "$ACTION_DIR/tests/common.sh"
workflow="$ACTION_DIR/../../workflows/deploy-action.yml"

reject_mutation() {
  local name="$1" expression="$2" diagnostic="$3" status=0
  yq "$expression" "$workflow" >"$WORK_DIR/workflow.yml"
  CACHE_WORKFLOW_FILE="$WORK_DIR/workflow.yml" bash "$ACTION_DIR/tests/fixture.sh" >"$WORK_DIR/log" 2>&1 || status=$?
  [[ "$status" == 1 ]] || fail "workflow guard accepted mutation: $name (exit $status)"
  grep -Fq "$diagnostic" "$WORK_DIR/log" || {
    cat "$WORK_DIR/log" >&2
    fail "mutation failed for an unrelated reason: $name"
  }
}

for kind in source target; do
  publication=".jobs.\"cache-restore-seed\".steps[] | select(.name == \"Require exact $kind seed publication\")"
  reject_mutation 'missing final gate' \
    ".jobs.\"cache-restore-seed\".steps |= map(select(.name != \"Require exact $kind seed publication\"))" \
    'missing exact publication assertion'
  reject_mutation 'hardcoded first hit' \
    "($publication | .env.CACHE_SEED_FIRST_HIT) = \"true\"" \
    'publication assertion ignores first lookup output'
  reject_mutation 'hardcoded second hit' \
    "($publication | .env.CACHE_SEED_SECOND_HIT) = \"true\"" \
    'publication assertion ignores second lookup output'
  reject_mutation 'skipped final gate' \
    "($publication | .if) = \"false\"" \
    'publication assertion can be skipped'
  reject_mutation 'ignored final failure' \
    "($publication | .\"continue-on-error\") = true" \
    'publication assertion failure is ignored'
  reject_mutation 'gate always succeeds' \
    "($publication | .run) = \"true\"" \
    'publication assertion accepts unverified outputs'
  reject_mutation 'gate always fails' \
    "($publication | .run) = \"false\"" \
    'publication assertion rejects a verified exact hit'
  for step in "$kind-save-1" "$kind-verify-1" "$kind-save-2" "$kind-verify-2"; do
    for input in path key; do
      reject_mutation "$step wrong $input" \
        "(.jobs.\"cache-restore-seed\".steps[] | select(.id == \"$step\") | .with.$input) = \"wrong\"" \
        "$step uses a different cache $input"
    done
    if [[ "$step" == *-2 ]]; then
      reject_mutation "$step wrong retry condition" \
        "(.jobs.\"cache-restore-seed\".steps[] | select(.id == \"$step\") | .if) = \"false\"" \
        "$step retries without a first-attempt miss"
    else
      reject_mutation "$step skipped initial attempt" \
        "(.jobs.\"cache-restore-seed\".steps[] | select(.id == \"$step\") | .if) = \"false\"" \
        "$step skips the initial publication attempt"
    fi
  done
done
printf 'source and target publication workflow mutation contracts passed\n'
