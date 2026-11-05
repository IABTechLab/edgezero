#!/usr/bin/env bash
set -euo pipefail

# Runs the fixture release's CLI with `deploy --format json` against the
# stateful fake Fastly provider, and asserts that a deploy failing after it
# cloned version 42 still reports that version and its state.
#
# Args: the expected `version_state` (`draft` or `unknown`).
# Reads (env):
#   GITHUB_WORKSPACE, RUNNER_TEMP, FAKE_CALL_LOG (from make-fake-fastly-env.sh)
# Writes: none

SCRIPT_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../../fastly-common/scripts/common.sh
source "$SCRIPT_DIR/../../fastly-common/scripts/common.sh"

main() {
  local expected_state="${1:?expected version_state is required}"
  local workspace="${GITHUB_WORKSPACE:?GITHUB_WORKSPACE is required}"
  local runner_temp="${RUNNER_TEMP:?RUNNER_TEMP is required}"
  local root bin cli fastly_archive envelope rc=0
  root=$(mktemp -d "$runner_temp/json-deploy-release.XXXXXX")
  bin=$(mktemp -d "$runner_temp/json-deploy-bin.XXXXXX")
  envelope="$bin/envelope.json"

  tar -xzf "$workspace/fixture-release/app-release.tar.gz" -C "$root"
  tar -xf "$root/$(jq -er '.app_cli.path' "$root/release.json")" -C "$bin"
  cli="$bin/$(jq -er '."app-cli-bin"' "$bin/app-cli-meta.json")"
  # make-fake-fastly-env.sh stages the fake `fastly` for install-fastly.sh;
  # this script runs no action, so it puts it on PATH itself.
  fastly_archive=$(find "$runner_temp/edgezero-action-tools/downloads" -name 'fastly-*-linux-amd64.tar.gz' | head -n 1)
  [[ -n "$fastly_archive" ]] || fail "the fake Fastly CLI archive is missing"
  tar -xzf "$fastly_archive" -C "$bin" fastly

  PATH="$bin:$PATH" FASTLY_API_TOKEN=dummy-token \
    "$cli" deploy --adapter fastly --format json --service-id dummyservice \
    --application-release "$root" >"$envelope" || rc=$?

  [[ "$rc" -ne 0 ]] || fail "the injected provider failure did not fail the deploy"
  grep -q '^PUT .*/service/dummyservice/version/40/clone$' "$FAKE_CALL_LOG" || fail "failure occurred before the clone"
  jq -e --arg state "$expected_state" \
    '.ok == false and .result.service_id == "dummyservice" and .result.version == 42 and .result.version_state == $state' \
    "$envelope" >/dev/null || fail "unexpected envelope: $(cat "$envelope")"
  notice "a failed JSON deploy reported version 42 as $expected_state"
}
main "$@"
