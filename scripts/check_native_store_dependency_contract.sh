#!/usr/bin/env bash
# Independent normal dependency graphs; workspace --all-features is not isolation proof.
set -euo pipefail

assert_no_sdk() {
  local graph
  graph="$(cargo tree --offline --locked --edges normal --prefix none "$@")"
  if grep -Eq '^aws-(config|sdk-[^ ]+) v' <<< "$graph"; then
    printf 'AWS SDK leaked into isolated graph: %s\n' "$*" >&2
    exit 1
  fi
}

assert_no_sdk -p edgezero-cli --no-default-features --features cli,edgezero-adapter-axum
assert_no_sdk -p edgezero-store-aws --no-default-features
assert_no_sdk -p edgezero-store-aws --no-default-features --features appconfig-agent
assert_no_sdk -p edgezero-adapter-axum --no-default-features --features axum
assert_no_sdk -p edgezero-adapter-cloudflare --target wasm32-unknown-unknown --features cloudflare
assert_no_sdk -p edgezero-adapter-fastly --target wasm32-wasip1 --features fastly
assert_no_sdk -p edgezero-adapter-spin --target wasm32-wasip2 --features spin
dynamodb_graph="$(cargo tree --offline --locked --edges normal --prefix none -p edgezero-store-aws --no-default-features --features dynamodb-kv)"
secrets_graph="$(cargo tree --offline --locked --edges normal --prefix none -p edgezero-store-aws --no-default-features --features secrets-manager)"
stores_graph="$(cargo tree --offline --locked --edges normal --prefix none -p edgezero-adapter-axum --no-default-features --features aws-stores)"
if ! grep -Eq '^aws-sdk-dynamodb v' <<< "$dynamodb_graph" || ! grep -Eq '^aws-sdk-secretsmanager v' <<< "$secrets_graph"; then
  printf 'Selected AWS service SDK is missing.\n' >&2
  exit 1
fi
if grep -Eq '^aws-sdk-secretsmanager v' <<< "$dynamodb_graph" || grep -Eq '^aws-sdk-dynamodb v' <<< "$secrets_graph" || grep -Eq '^aws-sdk-dynamodb v' <<< "$stores_graph"; then
  printf 'AWS service SDK leaked into an independently selected provider.\n' >&2
  exit 1
fi
# Celld settings are present in bindings-only builds, but live transport is not.
celld_features() {
  local graph
  graph="$(cargo tree --offline --locked --edges normal --prefix none --format '{p}|{f}' "$@")"
  awk -F'|' '/^edgezero-store-celld v/ { sub(/ [(][*][)]$/, "", $2); print $2; exit }' <<< "$graph"
}

assert_celld_off() {
  local selected
  selected="$(celld_features "$@")"
  if [[ -n "$selected" ]]; then
    printf 'Live celld features leaked into isolated graph: %s: %s\n' "$*" "$selected" >&2
    exit 1
  fi
}
assert_celld_off -p edgezero-cli --no-default-features --features cli,edgezero-adapter-axum
assert_celld_off -p edgezero-adapter-axum --no-default-features --features axum
assert_celld_off -p edgezero-adapter-axum --no-default-features --features aws-stores,aws-dynamodb

celld_settings="$(cargo tree --offline --locked --edges normal --prefix none -p edgezero-store-celld --no-default-features)"
if grep -Eq '^(reqwest|tokio|edgezero-core|aws-config|aws-sdk-[^ ]+) v' <<< "$celld_settings"; then
  printf 'Runtime/client dependency leaked into settings-only celld.\n' >&2
  exit 1
fi
for feature in config kv; do
  assert_no_sdk -p edgezero-store-celld --no-default-features --features "$feature"
  graph="$(cargo tree --offline --locked --edges normal --prefix none -p edgezero-store-celld --no-default-features --features "$feature")"
  if grep -Eq '^(celld(-[^ ]+)?|v8|worker|worker-build|edgezero-adapter-[^ ]+) v' <<< "$graph"; then
    printf 'Server/Worker/adapter dependency leaked into celld provider: %s\n' "$feature" >&2
    exit 1
  fi
  selected="$(celld_features -p edgezero-store-celld --no-default-features --features "$feature")"
  other=config
  if [[ "$feature" == config ]]; then other=kv; fi
  if ! grep -Eq "(^|,)$feature(,|$)" <<< "$selected" || grep -Eq "(^|,)$other(,|$)" <<< "$selected"; then
    printf 'Celld service feature isolation failed: %s: %s\n' "$feature" "$selected" >&2
    exit 1
  fi
done
printf 'Native store SDK and celld feature isolation graphs passed.\n'
