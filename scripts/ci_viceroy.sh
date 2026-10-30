#!/usr/bin/env bash
set -euo pipefail

cd "${GITHUB_WORKSPACE:?}"
version="$(awk '$1 == "viceroy" {print $2}' .tool-versions)"
test "$version" = 0.21.0
RUSTUP_TOOLCHAIN="$(awk '$1 == "rust" {print $2}' .tool-versions)"
export RUSTUP_TOOLCHAIN
export ASDF_RUST_VERSION="$RUSTUP_TOOLCHAIN"
test -n "$RUSTUP_TOOLCHAIN"
patch="$GITHUB_WORKSPACE/patches/viceroy/$version-body-readiness.patch"
stock_dir="${RUNNER_TEMP:?}/viceroy-stock"
stock="$stock_dir/bin/viceroy"
repair="$RUNNER_TEMP/viceroy-readiness"

sha256() {
  sha256sum "$1" | awk '{print $1}'
}

test "$(sha256 "$patch")" = "${VICEROY_PATCH_SHA256:?}"
compiler="$(rustc --version --verbose)"
cargo_version="$(cargo --version)"
platform="$(uname -sm)"
profiles="$(env | LC_ALL=C sort | sed -n '/^CARGO_PROFILE_/p; /^CARGO_INCREMENTAL=/p')"
helper_sha="$(sha256 "$0")"
inputs="$(
  printf 'version=%s\n%s\n%s\n%s\nRUSTFLAGS=%s\nCARGO_ENCODED_RUSTFLAGS=%s\n%s\nhelper=%s\n' \
    "$version" "$compiler" "$cargo_version" "$platform" "${RUSTFLAGS:-}" "${CARGO_ENCODED_RUSTFLAGS:-}" "$profiles" "$helper_sha"
)"
stock_key="$(printf '%s\nprofile=release\n' "$inputs" | sha256sum | awk '{print $1}')"
repair_key="$(
  printf '%s\nupstream=%s\nlock=%s\npatch=%s\nprofile=dev debug=0 incremental=0\n' \
    "$inputs" "${VICEROY_REVISION:?}" "${VICEROY_LOCK_SHA256:?}" "$VICEROY_PATCH_SHA256" |
    sha256sum | awk '{print $1}'
)"

stock_valid() {
  [[ -x "$stock" && -f "$stock_dir/inputs.sha256" && -f "$stock_dir/executable.sha256" ]] &&
    [[ "$(cat "$stock_dir/inputs.sha256")" = "$stock_key" ]] &&
    [[ "$(sha256 "$stock")" = "$(cat "$stock_dir/executable.sha256")" ]] &&
    [[ "$("$stock" --version)" = "viceroy $version" ]]
}

case "${1:-}" in
  keys)
    printf 'version=%s\nstock-key=%s\nrepair-key=%s\n' "$version" "$stock_key" "$repair_key" >> "${GITHUB_OUTPUT:?}"
    ;;
  stock)
    if ! stock_valid; then
      mkdir -p "$stock_dir"
      rm -f "$stock_dir/inputs.sha256" "$stock_dir/executable.sha256"
      cargo install viceroy --version "=$version" --locked --root "$stock_dir" --force
      test "$("$stock" --version)" = "viceroy $version"
      sha256 "$stock" > "$stock_dir/executable.sha256"
      printf '%s\n' "$stock_key" > "$stock_dir/inputs.sha256"
    fi
    stock_valid
    printf '%s\n' "$stock_dir/bin" >> "${GITHUB_PATH:?}"
    ;;
  repair)
    stock_valid
    stock_identity="$(sha256 "$stock")"
    source="$RUNNER_TEMP/viceroy-readiness-source"
    evidence="$GITHUB_WORKSPACE/tests/fixtures/reusable-app/.runs/viceroy-repair"
    # Only Cargo inputs/artifacts are cached. Revalidate a fresh checkout and patch.
    test ! -e "$source" && test ! -L "$source"
    trap 'rm -rf "$source"' EXIT
    git clone --depth 1 --branch "v$version" https://github.com/fastly/Viceroy.git "$source"
    test "$(git -C "$source" rev-parse --verify HEAD)" = "$VICEROY_REVISION"
    test "$(sha256 "$source/Cargo.lock")" = "$VICEROY_LOCK_SHA256"
    git -C "$source" apply --check "$patch"
    git -C "$source" apply "$patch"
    export CARGO_HOME="$repair/cargo-home"
    export CARGO_TARGET_DIR="$repair/target"
    export CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0
    cargo fetch --locked --manifest-path "$source/Cargo.toml"
    # Never cache a passing result: discovery and execution run even on exact hits.
    bash scripts/run_test_nonzero.sh readiness_preserves_bytes_without_polling_the_source_again cargo test --offline --locked --manifest-path "$source/Cargo.toml" -p viceroy-lib --lib body::tests::readiness_
    bash scripts/run_test_nonzero.sh readiness_preserves_between_bytes_timeout_without_repolling_source cargo test --offline --locked --manifest-path "$source/Cargo.toml" -p viceroy-lib --lib body::tests::
    cargo build --offline --locked --manifest-path "$source/Cargo.toml" -p viceroy
    executable="$CARGO_TARGET_DIR/debug/viceroy"
    test -x "$executable"
    test "$(sha256 "$stock")" = "$stock_identity"
    mkdir -p "$evidence"
    {
      printf 'upstream=%s\nprofile=dev debug=0 incremental=0\ncache-inputs=%s\n' "$VICEROY_REVISION" "$repair_key"
      rustc --version
      sha256sum "$source/Cargo.lock" "$patch" "$source/src/body.rs" "$executable"
    } | tee "$evidence/build-identity.txt"
    printf 'executable=%s\n' "$executable" >> "${GITHUB_OUTPUT:?}"
    ;;
  *)
    echo "usage: $0 keys|stock|repair" >&2
    exit 2
    ;;
esac
