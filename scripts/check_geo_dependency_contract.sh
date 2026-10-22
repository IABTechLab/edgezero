#!/usr/bin/env bash
# Check isolated normal graphs, not workspace-unified or test dependencies.
set -euo pipefail
export LC_ALL=C

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

# Portable core and real platform graphs must not acquire native hosting/stores.
assert_portable() {
  local name="$1" root="$2"
  shift 2
  graph "$name" "$root" "$@"
  if grep -Eq '^(edgezero-adapter-axum|edgezero-store-(aws|celld)|axum(-[^ ]+)?|tokio-[^ ]+|reqwest|hyper(-[^ ]+)?|mio|socket2|ipnet|aws-config|aws-sdk-[^ ]+) v' "$scratch/$name"; then
    fail "native runtime/store/SDK leaked into $name"
  fi
  # Pinned worker 0.8.5 already uses tokio 1.52.3 with default-features=false
  # and no selected features on wasm32-unknown-unknown. That empty-feature
  # dependency is not the native executor. Keep the exception exact so a worker
  # upgrade or enabling runtime/net features requires a fresh isolation review.
  if [[ "$name" == cloudflare ]]; then
    grep -Eq '^worker v0[.]8[.]5[|]' "$scratch/$name" \
      || fail 'Cloudflare worker pin changed; review its empty-feature tokio exception'
    grep -Eq '^tokio v1[.]52[.]3[|]$' "$scratch/$name" \
      || fail 'missing expected Cloudflare empty-feature tokio dependency'
  fi
  if ! awk -F '|' -v name="$name" '
    /^tokio v/ {
      sub(/ [(][*][)]$/, "", $2)
      if (name != "cloudflare" || $1 != "tokio v1.52.3" || $2 != "") exit 1
    }
  ' "$scratch/$name"; then
    fail "unexpected tokio version or native runtime features in $name"
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
