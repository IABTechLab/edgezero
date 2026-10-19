#!/usr/bin/env bash
# check_shared_dep_pins.sh — keep shared manifest and fixture SDK pins in sync.
#
# `edgezero-core` re-exports the `Validate` derive, so any crate that
# derives `AppConfig` links against the exact `validator` the core crate
# resolved. A crate that pins a different major gets a second, unrelated
# `Validate` trait and fails with:
#
#   error[E0277]: the trait bound `X: validator::traits::Validate` is not satisfied
#
# The pin lives in four places that cargo cannot reconcile for us: this
# workspace, the excluded `examples/app-demo` workspace, the scaffold
# generator's seed list, and the deploy smoke-test fixture. Only the
# first two share a lockfile, so the others drift silently until a
# deploy job fails.
#
# Exit 0 — every pin agrees.
# Exit 1 — prints each disagreeing file and its version.

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"

# Extract validator's version from a file. Accepts the table form with
# `version` in any key position (`validator = { features = [..], version
# = "X" }`), the bare form (`validator = "X"`), and the escaped spelling
# the scaffold seeds it as. Anchored to an assignment at line start so a
# commented-out or prose mention cannot shadow the real pin, and kept
# deliberately in step with `extract_version` in
# crates/edgezero-cli/src/generator.rs -- the two parsers describe the
# same grammar, and a difference between them shows up as this gate
# reporting a missing pin for one that is merely reformatted.
# Prints nothing when there is no match; callers treat empty as missing.
pin_in() {
  grep -hoE '^[[:space:]]*(\\?")?validator[[:space:]]*=[^#]*' "$1" 2>/dev/null |
    head -1 |
    grep -oE '[0-9]+(\.[0-9]+)*' |
    head -1 || true
}

WANT="$(pin_in Cargo.toml)"
if [[ -z "$WANT" ]]; then
  echo "check_shared_dep_pins: no validator pin in root Cargo.toml" >&2
  exit 1
fi

VIOLATIONS=0
for file in \
  examples/app-demo/Cargo.toml \
  .github/actions/deploy-fastly/tests/make-smoke-fixture.sh; do
  got="$(pin_in "$file")"
  if [[ -z "$got" ]]; then
    echo "$file: violation: no validator pin found (expected $WANT)"
    VIOLATIONS=$((VIOLATIONS + 1))
  elif [[ "$got" != "$WANT" ]]; then
    echo "$file: violation: validator $got, root Cargo.toml has $WANT"
    VIOLATIONS=$((VIOLATIONS + 1))
  fi
done

# The scaffold generator seeds the pin as a Rust string literal, and its
# own unit test compares it against the root manifest; check it here too
# so a mismatch is caught by the same gate as the rest. `pin_in` already
# understands the quoted-literal spelling, so it is reused rather than
# re-implemented -- a second parser here drifted from this one once.
gen="crates/edgezero-cli/src/generator.rs"
got="$(pin_in "$gen")"
if [[ -z "$got" ]]; then
  echo "$gen: violation: no validator seed found (expected $WANT)"
  VIOLATIONS=$((VIOLATIONS + 1))
elif [[ "$got" != "$WANT" ]]; then
  echo "$gen: violation: seeds validator $got, root Cargo.toml has $WANT"
  VIOLATIONS=$((VIOLATIONS + 1))
fi

# `spin-sdk` pins span isolated workspaces and the scaffold; they do not
# share one lockfile.
# A mismatch does not fail at the pin: the fixture resolves a second copy
# of the SDK and the build dies on `expected IncomingBody<Request>, found
# a different IncomingBody<Request>`, naming neither crate nor version.
# Only the major is comparable -- the root spells it `~7.0`, the seeds `7`.
spin_major_in() {
  grep -hoE '^[[:space:]]*(\\?")?spin-sdk[[:space:]]*=[^#]*' "$1" 2>/dev/null |
    head -1 |
    grep -oE '[0-9]+' |
    head -1 || true
}

SPIN_WANT="$(spin_major_in Cargo.toml)"
if [[ -z "$SPIN_WANT" ]]; then
  echo "check_shared_dep_pins: no spin-sdk pin in root Cargo.toml" >&2
  exit 1
fi

for file in \
  examples/app-demo/Cargo.toml \
  tests/fixtures/reusable-app/crates/fixture-spin/Cargo.toml \
  tests/fixtures/request-fidelity/Cargo.toml \
  crates/edgezero-cli/src/generator.rs; do
  got="$(spin_major_in "$file")"
  if [[ -z "$got" ]]; then
    echo "$file: violation: no spin-sdk pin found (expected major $SPIN_WANT)"
    VIOLATIONS=$((VIOLATIONS + 1))
  elif [[ "$got" != "$SPIN_WANT" ]]; then
    echo "$file: violation: spin-sdk major $got, root Cargo.toml has $SPIN_WANT"
    VIOLATIONS=$((VIOLATIONS + 1))
  fi
done

# Fixture SDKs must use the same resolved packages as the workspace. Check
# both the exact manifest pins and the isolated lockfile, not just SDK majors.
locked_version_in() {
  awk -v package="$2" '
    $0 == "[[package]]" { name=""; next }
    $1 == "name" { name=$3; gsub(/"/, "", name) }
    name == package && $1 == "version" {
      version=$3; gsub(/"/, "", version); count++; found=version
    }
    END { if (count == 1) print found }
  ' "$1"
}

fixture_manifest="tests/fixtures/request-fidelity/Cargo.toml"
fixture_lock="tests/fixtures/request-fidelity/Cargo.lock"
for package in fastly worker wasm-bindgen spin-sdk; do
  want="$(locked_version_in Cargo.lock "$package")"
  pin="$(awk -v package="$package" '
    $1 == package && $2 == "=" {
      if (match($0, /version[[:space:]]*=[[:space:]]*"=[^"]+"/)) {
        version=substr($0, RSTART, RLENGTH)
        sub(/^[^"]*"=/, "", version); sub(/"$/, "", version)
        count++; found=version
      }
    }
    END { if (count == 1) print found }
  ' "$fixture_manifest")"
  locked="$(locked_version_in "$fixture_lock" "$package")"
  if [[ -z "$want" || -z "$pin" || -z "$locked" ]]; then
    echo "$fixture_manifest / $fixture_lock: violation: $package requires one exact pin and one resolved version in each lock"
    VIOLATIONS=$((VIOLATIONS + 1))
  elif [[ "$pin" != "$want" || "$locked" != "$want" ]]; then
    echo "$fixture_manifest / $fixture_lock: violation: $package pin=$pin lock=$locked, workspace lock has $want"
    VIOLATIONS=$((VIOLATIONS + 1))
  fi
done

# worker-build's release must match the fixture's Worker SDK, but its pin is
# declared once in metadata and consumed by CI and the native runner.
worker_build="$(awk '$1 == "worker-build" { gsub(/"/, "", $3); print $3; exit }' "$fixture_manifest")"
worker_version="$(locked_version_in Cargo.lock worker)"
if [[ -z "$worker_build" || "$worker_build" != "$worker_version" ]]; then
  echo "$fixture_manifest: violation: worker-build $worker_build, workspace worker is $worker_version"
  VIOLATIONS=$((VIOLATIONS + 1))
fi

if ((VIOLATIONS > 0)); then
  echo "check_shared_dep_pins: $VIOLATIONS pin(s) out of sync" >&2
  exit 1
fi
echo "check_shared_dep_pins: validator pins agree ($WANT); spin-sdk majors agree ($SPIN_WANT); fixture SDK locks and worker-build agree"
