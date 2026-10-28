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
  local name="$1" expected="$2" diagnostic="${3:-check_shared_dep_pins: validator pins agree}" status=0
  bash "$WORK_DIR/repo/scripts/check_shared_dep_pins.sh" >"$WORK_DIR/output" 2>&1 || status=$?
  if [[ "$status" != "$expected" ]] || ! grep -Fq -- "$diagnostic" "$WORK_DIR/output"; then
    printf 'FAIL %s: expected exit %s and diagnostic %s, got exit %s\n' "$name" "$expected" "$diagnostic" "$status" >&2
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

mutate_shared_pin() {
  local file="$1" package="$2" mode="$3"
  awk -v package="$package" -v mode="$mode" '
    $0 ~ "^[[:space:]]*\"?" package "[[:space:]]*=" {
      if (mode == "missing") next
      if (mode == "duplicate") print
      if (mode == "numeric-feature" || mode == "valid-numeric-feature") {
        match($0, /"[~^=]*[0-9]+(\.[0-9]+)*"/)
        version=substr($0, RSTART, RLENGTH); gsub(/"/, "", version)
        if (mode == "numeric-feature") version="99.0.0"
        $0=package " = { features = [\"wasi7\"], version = \"" version "\" }"
        print
        next
      }
      sub(/[0-9]+(\.[0-9]+)*/, "99.0.0")
    }
    { print }
  ' "$WORK_DIR/repo/$file" >"$WORK_DIR/changed"
  mv "$WORK_DIR/changed" "$WORK_DIR/repo/$file"
}

reset_fixture
check_case unchanged 0
for file in examples/app-demo/Cargo.toml .github/actions/deploy-fastly/tests/make-smoke-fixture.sh crates/edgezero-cli/src/generator.rs; do
  reset_fixture
  rm "$WORK_DIR/repo/$file"
  diagnostic="$file: violation: no validator pin found"
  if [[ "$file" == crates/edgezero-cli/src/generator.rs ]]; then
    diagnostic="$file: violation: no validator seed found"
  fi
  check_case "missing pinned file $file" 1 "$diagnostic"
  if ! grep -Fq 'pin(s) out of sync' "$WORK_DIR/output"; then
    echo "FAIL missing pinned file $file: remaining checks did not finish" >&2
    exit 1
  fi
done
for package in validator spin-sdk; do
  reset_fixture
  mutate_shared_pin Cargo.toml "$package" missing
  check_case "missing root $package" 1 "check_shared_dep_pins: no $package pin in root Cargo.toml"
done
for file in examples/app-demo/Cargo.toml .github/actions/deploy-fastly/tests/make-smoke-fixture.sh crates/edgezero-cli/src/generator.rs; do
  for mode in missing drift; do
    reset_fixture
    mutate_shared_pin "$file" validator "$mode"
    diagnostic="$file: violation: no validator pin found"
    if [[ "$mode" == drift ]]; then diagnostic="$file: violation: validator 99.0.0"; fi
    if [[ "$file" == crates/edgezero-cli/src/generator.rs ]]; then
      diagnostic="$file: violation: no validator seed found"
      if [[ "$mode" == drift ]]; then diagnostic="$file: violation: seeds validator 99.0.0"; fi
    fi
    check_case "validator $file $mode" 1 "$diagnostic"
  done
done
for file in examples/app-demo/Cargo.toml tests/fixtures/reusable-app/crates/fixture-spin/Cargo.toml "$MANIFEST" crates/edgezero-cli/src/generator.rs; do
  for mode in missing drift; do
    reset_fixture
    mutate_shared_pin "$file" spin-sdk "$mode"
    diagnostic="$file: violation: no spin-sdk pin found"
    if [[ "$mode" == drift ]]; then diagnostic="$file: violation: spin-sdk major 99"; fi
    check_case "spin-sdk $file $mode" 1 "$diagnostic"
  done
done
for package in validator spin-sdk; do
  reset_fixture
  mutate_shared_pin examples/app-demo/Cargo.toml "$package" duplicate
  check_case "duplicate shared $package" 1 "violation: no $package pin found"
  reset_fixture
  mutate_shared_pin examples/app-demo/Cargo.toml "$package" numeric-feature
  diagnostic="violation: validator 99.0.0"
  if [[ "$package" == spin-sdk ]]; then diagnostic="violation: spin-sdk major 99"; fi
  check_case "numeric feature cannot hide $package drift" 1 "$diagnostic"
  reset_fixture
  mutate_shared_pin examples/app-demo/Cargo.toml "$package" valid-numeric-feature
  check_case "numeric feature before valid $package version" 0
done
for package in fastly worker wasm-bindgen spin-sdk; do
  version=$(awk -v package="$package" '
    $0 == "[[package]]" { name="" }
    $1 == "name" { name=$3; gsub(/"/, "", name) }
    name == package && $1 == "version" { gsub(/"/, "", $3); print $3 }
  ' "$ROOT/Cargo.lock")
  for mode in drift caret missing duplicate reordered space comment duplicate-caret misleading-key; do
    reset_fixture
    mutate_manifest "$package" "$mode"
    expected=1
    diagnostic="violation: $package requires one exact pin"
    if [[ "$mode" == drift ]]; then diagnostic="violation: $package pin=99.0.0"; fi
    if [[ "$mode" == reordered || "$mode" == space ]]; then expected=0; fi
    if [[ "$expected" == 0 ]]; then diagnostic="check_shared_dep_pins: validator pins agree"; fi
    check_case "$package $mode" "$expected" "$diagnostic"
  done
  for mode in drift duplicate missing; do
    reset_fixture
    mutate_lock "$LOCK" "$package" "$mode"
    diagnostic="violation: $package requires one exact pin"
    if [[ "$mode" == drift ]]; then diagnostic="violation: $package pin=$version lock=99.0.0"; fi
    check_case "$package fixture lock $mode" 1 "$diagnostic"
  done
  for mode in drift duplicate missing; do
    reset_fixture
    mutate_lock Cargo.lock "$package" "$mode"
    diagnostic="violation: $package requires one exact pin"
    if [[ "$mode" == drift ]]; then diagnostic="violation: $package pin=$version lock=$version, workspace lock has 99.0.0"; fi
    check_case "$package workspace lock $mode" 1 "$diagnostic"
  done
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
  check_case "worker-build $mode" 1 "violation: worker-build"
done

for mode in section-comment attached-comment spaced-comment unrelated-section; do
  reset_fixture
  awk -v mode="$mode" '
    $0 == "[package.metadata.request-fidelity]" {
      if (mode == "section-comment") $0=$0 " # tools"
      if (mode == "unrelated-section") print "[package.metadata.unrelated]\nworker-build = \"99.0.0\"\n"
    }
    $1 == "worker-build" {
      if (mode == "attached-comment") $0=$0 "# tools"
      if (mode == "spaced-comment") $0=$0 " # tools"
    }
    { print }
  ' "$WORK_DIR/repo/$MANIFEST" >"$WORK_DIR/changed"
  mv "$WORK_DIR/changed" "$WORK_DIR/repo/$MANIFEST"
  check_case "worker-build $mode" 0
done
