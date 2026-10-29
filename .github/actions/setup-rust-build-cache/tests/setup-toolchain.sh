#!/usr/bin/env bash
set -euo pipefail

# All compiler smoke jobs must use identical rustc binaries and target libraries.
version=$(awk '$1 == "rust" { print $2 }' "${GITHUB_WORKSPACE:?}/.tool-versions")
[[ "$version" =~ ^[0-9]+\.[0-9]+\.[0-9]+$ ]] || { printf 'invalid Rust toolchain version\n' >&2; exit 1; }
rustup toolchain install "$version" --profile minimal --target wasm32-wasip1
printf 'RUSTUP_TOOLCHAIN=%s\n' "$version" >>"${GITHUB_ENV:?}"
