#!/usr/bin/env bash
# Check isolated normal graphs, not workspace-unified or test dependencies.
set -euo pipefail
export LC_ALL=C
# shellcheck source=scripts/portable_dependency_checks.sh
source "$(dirname "${BASH_SOURCE[0]}")/portable_dependency_checks.sh"

scratch="$(mktemp -d)"
trap 'rm -rf "$scratch"' EXIT

fail() {
  printf 'Geo dependency contract: %s\n' "$*" >&2
  exit 1
}

graph() {
  local name="$1" root="$2"
  shift 2
  cargo tree --offline --locked --edges normal --prefix none --format '{p}|{f}' \
    -p "$root" "$@" > "$scratch/$name"
  # Empty output, a wrong package, or a changed output format is not proof.
  grep -Eq "^${root} v[^|]+[|]" "$scratch/$name" \
    || fail "missing expected root package in $name"
  grep -Eq '^edgezero-core v[^|]+[|]' "$scratch/$name" \
    || fail "missing portable core in $name"
}

features() {
  local name="$1" root="$2"
  awk -F '|' -v root="$root" '$1 ~ "^" root " v" { print $2; exit }' "$scratch/$name"
}

assert_feature() {
  local name="$1" feature="$2" expected="$3" selected
  selected="$(features "$name" edgezero-adapter-axum)"
  if grep -Eq "(^|,)${feature}(,|$)" <<< "$selected"; then
    [[ "$expected" == on ]] || fail "$feature unexpectedly selected in $name"
  else
    [[ "$expected" == off ]] || fail "$feature missing in $name"
  fi
}

# The implementation must be default-off and add no normal runtime dependency.
graph native-default edgezero-adapter-axum
graph native-off edgezero-adapter-axum --no-default-features --features axum
graph native-on edgezero-adapter-axum --no-default-features --features geo-cloudfront
for name in native-default native-off native-on; do
  assert_feature "$name" axum on
  if grep -Eq '^aws-(config|sdk-[^ ]+) v' "$scratch/$name"; then
    fail "AWS SDK leaked into $name"
  fi
done
assert_feature native-default geo-cloudfront off
assert_feature native-off geo-cloudfront off
assert_feature native-on geo-cloudfront on
for name in native-off native-on; do
  # Ignore feature annotations and duplicate-tree markers, retaining exact versions.
  cut -d '|' -f 1 "$scratch/$name" | sort -u > "$scratch/$name.packages"
done
if ! cmp -s "$scratch/native-off.packages" "$scratch/native-on.packages"; then
  diff -u "$scratch/native-off.packages" "$scratch/native-on.packages" >&2 || true
  fail 'geo-cloudfront changed the isolated normal dependency set'
fi

# Portable core and real platform graphs must not acquire native hosting or runtimes.
assert_portable() {
  local name="$1" root="$2"
  shift 2
  graph "$name" "$root" "$@"
  if grep -Eq '^(edgezero-adapter-axum|axum(-[^ ]+)?|reqwest|hyper(-[^ ]+)?|mio|socket2|ipnet|aws-config|aws-sdk-[^ ]+) v' "$scratch/$name"; then
    fail "native runtime/SDK leaked into $name"
  fi
  # Platform adapters read the shared store bindings through the store crates' default-off
  # builds; check_native_store_dependency_contract.sh proves those carry no native runtime.
  if [[ "$name" == core ]] && grep -Eq '^edgezero-store-(aws|celld) v' "$scratch/$name"; then
    fail 'store crate leaked into portable core'
  fi
  if [[ "$name" == core ]] && grep -Eq '^tokio(-[^ ]+)? v' "$scratch/$name"; then
    fail 'Tokio dependency leaked into portable core'
  fi
  if ! assert_wasm_tokio_features < "$scratch/$name"; then
    fail "Tokio runtime/native-I/O features leaked into $name"
  fi
}
assert_portable core edgezero-core --no-default-features
for entry in 'cloudflare wasm32-unknown-unknown worker' 'fastly wasm32-wasip1 fastly' 'spin wasm32-wasip2 spin-sdk'; do
  read -r adapter target sdk <<< "$entry"
  assert_portable "$adapter" "edgezero-adapter-$adapter" \
    --no-default-features --features "$adapter" --target "$target"
  grep -Eq "^${sdk} v[^|]+[|]" "$scratch/$adapter" \
    || fail "missing selected platform SDK in $adapter"
done
printf 'Geo default-off, native dependency equality, and portable isolation graphs passed.\n'
