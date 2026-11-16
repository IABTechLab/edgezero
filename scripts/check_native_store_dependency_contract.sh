#!/usr/bin/env bash
# Independent normal dependency graphs; workspace --all-features is not isolation proof.
set -euo pipefail
# shellcheck source=scripts/portable_dependency_checks.sh
source "$(dirname "${BASH_SOURCE[0]}")/portable_dependency_checks.sh"

assert_no_sdk() {
  local graph
  graph="$(cargo tree --offline --locked --edges normal --prefix none "$@")"
  if grep -Eq '^aws-(config|sdk-[^ ]+) v' <<< "$graph"; then
    printf 'AWS SDK leaked into isolated graph: %s\n' "$*" >&2
    exit 1
  fi
}

assert_no_native_runtime() {
  local scope="$1" graph forbidden
  shift
  graph="$(cargo tree --offline --locked --edges normal --prefix none --format '{p}|{f}' "$@")"
  forbidden='aws-(config|sdk-[^ ]+)|reqwest|celld(-[^ ]+)?|v8'
  if [[ "$scope" == core ]]; then
    forbidden+='|tokio(-[^ ]+)?'
  fi
  if grep -Eq "^($forbidden) v" <<< "$graph"; then
    printf 'Native runtime/client dependency leaked into core or WASM: %s\n' "$*" >&2
    exit 1
  fi
  if ! assert_wasm_tokio_features <<< "$graph"; then
    printf 'Tokio runtime/native-I/O features leaked into WASM: %s\n' "$*" >&2
    exit 1
  fi
}

assert_no_native_runtime core -p edgezero-core --all-features
assert_no_native_runtime wasm -p edgezero-adapter-cloudflare --target wasm32-unknown-unknown --features cloudflare
assert_no_native_runtime wasm -p edgezero-adapter-fastly --target wasm32-wasip1 --features fastly
assert_no_native_runtime wasm -p edgezero-adapter-spin --target wasm32-wasip2 --features spin
assert_no_native_runtime wasm -p edgezero-store-aws --target wasm32-wasip1 --no-default-features --features dynamodb-records
assert_no_native_runtime wasm -p edgezero-store-celld --target wasm32-wasip1 --no-default-features --features records-v2

assert_no_sdk -p edgezero-cli --no-default-features --features cli,edgezero-adapter-axum
assert_no_sdk -p edgezero-store-aws --no-default-features
assert_no_sdk -p edgezero-store-aws --no-default-features --features appconfig-agent
assert_no_sdk -p edgezero-adapter-axum --no-default-features --features axum
assert_no_sdk -p edgezero-adapter-cloudflare --target wasm32-unknown-unknown --features cloudflare
assert_no_sdk -p edgezero-adapter-fastly --target wasm32-wasip1 --features fastly
assert_no_sdk -p edgezero-adapter-spin --target wasm32-wasip2 --features spin
dynamodb_graph="$(cargo tree --offline --locked --edges normal --prefix none -p edgezero-store-aws --no-default-features --features dynamodb-kv)"
dynamodb_records_graph="$(cargo tree --offline --locked --edges normal --prefix none -p edgezero-store-aws --no-default-features --features dynamodb-records)"
secrets_graph="$(cargo tree --offline --locked --edges normal --prefix none -p edgezero-store-aws --no-default-features --features secrets-manager)"
stores_graph="$(cargo tree --offline --locked --edges normal --prefix none -p edgezero-adapter-axum --no-default-features --features aws-stores)"
if ! grep -Eq '^aws-sdk-dynamodb v' <<< "$dynamodb_graph" || ! grep -Eq '^aws-sdk-dynamodb v' <<< "$dynamodb_records_graph" || ! grep -Eq '^aws-sdk-secretsmanager v' <<< "$secrets_graph"; then
  printf 'Selected AWS service SDK is missing.\n' >&2
  exit 1
fi
if grep -Eq '^aws-sdk-secretsmanager v' <<< "$dynamodb_graph" || grep -Eq '^aws-sdk-secretsmanager v' <<< "$dynamodb_records_graph" || grep -Eq '^aws-sdk-dynamodb v' <<< "$secrets_graph" || grep -Eq '^aws-sdk-dynamodb v' <<< "$stores_graph"; then
  printf 'AWS service SDK leaked into an independently selected provider.\n' >&2
  exit 1
fi
# Celld settings are present in bindings-only builds, but live transport is not.
provider_features() {
  local package="$1" graph
  shift
  graph="$(cargo tree --offline --locked --edges normal --prefix none --format '{p}|{f}' "$@")"
  awk -F'|' -v package="$package" '$1 ~ ("^" package " v") { sub(/ [(][*][)]$/, "", $2); print $2; exit }' <<< "$graph"
}

assert_celld_off() {
  local selected
  selected="$(provider_features edgezero-store-celld "$@")"
  if [[ -n "$selected" ]]; then
    printf 'Live celld features leaked into isolated graph: %s: %s\n' "$*" "$selected" >&2
    exit 1
  fi
}
assert_celld_off -p edgezero-cli --no-default-features --features cli,edgezero-adapter-axum
assert_celld_off -p edgezero-adapter-axum --no-default-features --features axum
assert_celld_off -p edgezero-adapter-axum --no-default-features --features aws-stores,aws-dynamodb
assert_celld_off -p edgezero-adapter-axum --no-default-features --features aws-dynamodb-records
assert_no_sdk -p edgezero-adapter-axum --no-default-features --features celld-records

aws_raw_features="$(provider_features edgezero-store-aws -p edgezero-adapter-axum --no-default-features --features aws-dynamodb)"
aws_record_features="$(provider_features edgezero-store-aws -p edgezero-adapter-axum --no-default-features --features aws-dynamodb-records)"
celld_raw_features="$(provider_features edgezero-store-celld -p edgezero-adapter-axum --no-default-features --features celld-kv)"
celld_record_features="$(provider_features edgezero-store-celld -p edgezero-adapter-axum --no-default-features --features celld-records)"
if grep -Eq '(^|,)dynamodb-records(,|$)' <<< "$aws_raw_features" || ! grep -Eq '(^|,)dynamodb-records(,|$)' <<< "$aws_record_features"; then
  printf 'Explicit DynamoDB record feature forwarding failed.\n' >&2
  exit 1
fi
if grep -Eq '(^|,)records-v2(,|$)' <<< "$celld_raw_features" || ! grep -Eq '(^|,)records-v2(,|$)' <<< "$celld_record_features"; then
  printf 'Explicit celld record feature forwarding failed.\n' >&2
  exit 1
fi

celld_settings="$(cargo tree --offline --locked --edges normal --prefix none -p edgezero-store-celld --no-default-features)"
if grep -Eq '^(reqwest|tokio|edgezero-core|aws-config|aws-sdk-[^ ]+) v' <<< "$celld_settings"; then
  printf 'Runtime/client dependency leaked into settings-only celld.\n' >&2
  exit 1
fi
for feature in config kv records records-v2; do
  assert_no_sdk -p edgezero-store-celld --no-default-features --features "$feature"
  graph="$(cargo tree --offline --locked --edges normal --prefix none -p edgezero-store-celld --no-default-features --features "$feature")"
  if grep -Eq '^(celld(-[^ ]+)?|v8|worker|worker-build|edgezero-adapter-[^ ]+) v' <<< "$graph"; then
    printf 'Server/Worker/adapter dependency leaked into celld provider: %s\n' "$feature" >&2
    exit 1
  fi
  selected="$(provider_features edgezero-store-celld -p edgezero-store-celld --no-default-features --features "$feature")"
  forbidden=config
  case "$feature" in
    config) forbidden='kv|records|records-v2' ;;
    kv) forbidden='config|records|records-v2' ;;
    records) forbidden='config|records-v2' ;;
    records-v2) forbidden='config|records' ;;
  esac
  if ! grep -Eq "(^|,)$feature(,|$)" <<< "$selected" || grep -Eq "(^|,)($forbidden)(,|$)" <<< "$selected"; then
    printf 'Celld service feature isolation failed: %s: %s\n' "$feature" "$selected" >&2
    exit 1
  fi
done
printf 'Native store SDK, WASM runtime features and celld feature isolation graphs passed.\n'
