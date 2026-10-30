#!/usr/bin/env bash
set -euo pipefail

# Opt-in no-store Compose packaging check; not readiness/storage/drain proof.
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
export AXUM_IMAGE="${1:?pass a local loaded image digest}"
NAME="${2:-container-probe}"
[[ $# -le 2 && "$AXUM_IMAGE" == *@sha256:* && "$NAME" =~ ^[a-zA-Z0-9_-]+$ ]] \
  || { echo 'Expected image digest and generated app name' >&2; exit 2; }
export AXUM_PORT=0
PROJECT="edgezero-compose-$$-$RANDOM"
WORK="$(mktemp -d -t edgezero-compose.XXXXXX)"
compose() { docker compose -p "$PROJECT" -f "$ROOT/examples/axum-deployment/compose.yaml" "$@"; }
wait_for_http() {
  for _ in $(seq 1 30); do
    curl --fail --silent --max-time 2 "http://127.0.0.1:$PORT/" > "$WORK/body" && break
    sleep 1
  done
  curl --fail --silent --max-time 2 "http://127.0.0.1:$PORT/" > "$WORK/body"
  [[ "$(< "$WORK/body")" == "$NAME app" ]]
}
cleanup() {
  status=$?
  if [[ "$status" -eq 0 ]]; then
    rm -rf "$WORK"
  else
    compose logs > "$WORK/cleanup-compose.log" 2>&1 || true
    compose down > "$WORK/cleanup.log" 2>&1 || true
    echo "Diagnostics: $WORK" >&2
  fi
  exit "$status"
}
trap cleanup EXIT
compose config > "$WORK/config.yaml"
compose up -d --no-build --pull never
CONTAINER="$(compose ps -q app)"
PORT="$(compose port app 8787 | awk -F: '{print $NF}')"
wait_for_http
[[ "$(docker inspect "$CONTAINER" --format '{{.Config.User}} {{.HostConfig.ReadonlyRootfs}} {{.HostConfig.CapDrop}} {{.HostConfig.SecurityOpt}}')" \
  == '10001:10001 true [ALL] [no-new-privileges:true]' ]]
compose stop
[[ "$(docker inspect "$CONTAINER" --format '{{.State.ExitCode}}')" == 0 ]] \
  || { echo 'Container did not stop cleanly' >&2; exit 1; }
compose start
PORT="$(compose port app 8787 | awk -F: '{print $NF}')"
wait_for_http
compose logs > "$WORK/compose.log" 2>&1 || true
compose down > "$WORK/teardown.log" 2>&1
echo 'PASS: no-store Compose config, restricted HTTP startup, stop/start and teardown.'
echo 'No production readiness, durable state or bounded SIGTERM acceptance is implied.'
