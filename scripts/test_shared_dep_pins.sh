#!/usr/bin/env bash
# Exercise the real guard in isolated repositories, including malformed pins.
set -euo pipefail
ROOT=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)
WORK_DIR=$(mktemp -d "${TMPDIR:-/tmp}/edgezero-pin-guard.XXXXXX")
trap 'rm -rf -- "$WORK_DIR"' EXIT
FILES=(
  scripts/check_shared_dep_pins.sh Cargo.toml Cargo.lock
  examples/app-demo/Cargo.toml
  .github/actions/deploy-fastly/tests/make-smoke-fixture.sh
  crates/edgezero-cli/src/generator.rs
  tests/fixtures/reusable-app/crates/fixture-spin/Cargo.toml
  tests/fixtures/request-fidelity/Cargo.toml
  tests/fixtures/request-fidelity/Cargo.lock
)
MANIFEST=tests/fixtures/request-fidelity/Cargo.toml
LOCK=tests/fixtures/request-fidelity/Cargo.lock

reset_fixture() {
  local file
  rm -rf -- "$WORK_DIR/repo"
  mkdir -p "$WORK_DIR/repo"
  for file in "${FILES[@]}"; do
    mkdir -p "$WORK_DIR/repo/$(dirname -- "$file")"
    cp "$ROOT/$file" "$WORK_DIR/repo/$file"
  done
}

check_case() {
  local name="$1" expected="$2" status=0
  bash "$WORK_DIR/repo/scripts/check_shared_dep_pins.sh" >"$WORK_DIR/output" 2>&1 || status=$?
  if [[ "$status" != "$expected" ]]; then
    printf 'FAIL %s: expected exit %s, got %s\n' "$name" "$expected" "$status" >&2
    cat "$WORK_DIR/output" >&2
    exit 1
  fi
  printf 'PASS %s\n' "$name"
}

mutate_manifest() {
  local package="$1" mode="$2"
  awk -v package="$package" -v mode="$mode" '
    $1 == package && /version[[:space:]]*=/ {
      if (mode == "missing") next
      if (mode == "duplicate") print
      if (mode == "drift") sub(/version = "=[^"]+"/, "version = \"=99.0.0\"")
      if (mode == "caret") sub(/version = "=/, "version = \"^")
      if (mode == "misleading-key") {
        match($0, /version = "=[^"]+"/)
        exact=substr($0, RSTART, RLENGTH)
        sub(/version = "=/, "version = \"^")
        sub(/ }/, ", test_" exact " }")
      }
      if (mode == "comment") {
        match($0, /version = "=[^"]+"/)
        exact=substr($0, RSTART, RLENGTH)
        sub(/version = "=/, "version = \"^")
        $0=$0 " # " exact
      }
      if (mode == "duplicate-caret") {
        print
        sub(/version = "=/, "version = \"^")
      }
      if (mode == "space") sub(/version = "=/, "version = \"= ")
      if (mode == "reordered") {
        match($0, /version = "=[^"]+", /)
        version=substr($0, RSTART, RLENGTH)
        sub(/version = "=[^"]+", /, "")
        sub(/ }/, ", " version "}")
        $0=$0 " # trailing comment"
      }
    }
    { print }
  ' "$WORK_DIR/repo/$MANIFEST" >"$WORK_DIR/changed"
  mv "$WORK_DIR/changed" "$WORK_DIR/repo/$MANIFEST"
}

mutate_lock() {
  local file="$1" package="$2" mode="$3"
  awk -v package="$package" -v mode="$mode" '
    $0 == "[[package]]" { name="" }
    $1 == "name" { name=$3; gsub(/"/, "", name) }
    name == package && $1 == "version" {
      if (mode == "missing") next
      if (mode == "drift") $0="version = \"99.0.0\""
      if (mode == "duplicate") print "version = \"99.0.0\"\n\n[[package]]\nname = \"" package "\""
    }
    { print }
  ' "$WORK_DIR/repo/$file" >"$WORK_DIR/changed"
  mv "$WORK_DIR/changed" "$WORK_DIR/repo/$file"
}

reset_fixture
check_case unchanged 0
for package in fastly worker wasm-bindgen spin-sdk; do
  for mode in drift caret missing duplicate reordered space comment duplicate-caret misleading-key; do
    reset_fixture
    mutate_manifest "$package" "$mode"
    expected=1
    if [[ "$mode" == reordered || "$mode" == space ]]; then expected=0; fi
    check_case "$package $mode" "$expected"
  done
  for mode in drift duplicate missing; do
    reset_fixture
    mutate_lock "$LOCK" "$package" "$mode"
    check_case "$package fixture lock $mode" 1
  done
  reset_fixture
  mutate_lock Cargo.lock "$package" duplicate
  check_case "$package duplicate workspace lock" 1
done
for mode in missing wrong duplicate misplaced; do
  reset_fixture
  awk -v mode="$mode" '
    $0 == "[package.metadata.request-fidelity]" && mode == "misplaced" {
      print "[package.metadata.unrelated]"; next
    }
    $1 == "worker-build" {
      if (mode == "missing") next
      if (mode == "wrong") $0="worker-build = \"0.8.4\""
      if (mode == "duplicate") print
    }
    { print }
  ' "$WORK_DIR/repo/$MANIFEST" >"$WORK_DIR/changed"
  mv "$WORK_DIR/changed" "$WORK_DIR/repo/$MANIFEST"
  check_case "worker-build $mode" 1
done
