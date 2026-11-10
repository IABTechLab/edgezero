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
printf 'Native store SDK isolation graphs passed.\n'
