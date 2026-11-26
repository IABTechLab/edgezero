#!/usr/bin/env bash
set -euo pipefail
REPO_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_DIR"
exec "${CARGO:-cargo}" run --quiet --locked --manifest-path "$REPO_DIR/crates/edgezero-test-harness/Cargo.toml" -- "$@"
