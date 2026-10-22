#!/usr/bin/env bash
set -euo pipefail

# Test-only source staging. Scaffolding and production dependency policy are unchanged.
# Usage: prepare_generated_axum_container.sh /empty/output/parent
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PARENT="${1:?pass a new disposable output directory}"
[[ $# -eq 1 ]] || { echo 'Expected one output directory' >&2; exit 2; }
[[ ! -e "$PARENT" ]] || { echo 'Output directory must not exist' >&2; exit 2; }
mkdir -p "$PARENT"
PARENT="$(cd "$PARENT" && pwd)"
[[ "$PARENT/" != "$ROOT/"* ]] || { echo 'Fixture must be outside the source checkout' >&2; exit 2; }
RUSTUP_TOOLCHAIN="$(awk '$1 == "rust" { print $2 }' "$ROOT/.tool-versions")"
export RUSTUP_TOOLCHAIN
[[ "$RUSTUP_TOOLCHAIN" =~ ^[0-9]+\.[0-9]+\.[0-9]+$ ]] || { echo 'Expected declared Rust pin' >&2; exit 2; }
cargo run --locked --manifest-path "$ROOT/Cargo.toml" -p edgezero-cli --bin edgezero -- \
  new container-probe --dir "$PARENT"
APP="$PARENT/container-probe"
node "$ROOT/scripts/axum_container_helpers.mjs" stage-sources "$ROOT" "$APP"
(
  cd "$APP"
  cargo generate-lockfile
  cargo metadata --locked --format-version 1 > "$PARENT/metadata.json"
)
node "$ROOT/scripts/axum_container_helpers.mjs" record-inputs "$APP" "$PARENT/metadata.json"
printf 'Prepared disposable fixture: %s\n' "$APP"
