#!/usr/bin/env bash
set -euo pipefail

# A dependency-free library; only README changes between source revisions.
mode="${1:?fixture mode is required}"
[[ "$mode" == seed || "$mode" == next ]] || exit 2
fixture="${GITHUB_WORKSPACE:?}/cache-restore-fixture"
fixture_id="${CACHE_FIXTURE_ID:-${GITHUB_RUN_ID:-0}-${GITHUB_RUN_ATTEMPT:-1}}"
[[ "$fixture_id" =~ ^[0-9]+-[0-9]+$ ]] || { printf 'invalid cache fixture ID\n' >&2; exit 2; }
mkdir -p "$fixture/src"
printf '[package]\nname = "edgezero-cache-probe"\nversion = "0.1.0"\nedition = "2021"\n[workspace]\n' >"$fixture/Cargo.toml"
printf 'version = 3\n[[package]]\nname = "edgezero-cache-probe"\nversion = "0.1.0"\n' >"$fixture/Cargo.lock"
printf 'pub fn answer() -> u32 { 42 }\n' >"$fixture/src/lib.rs"
printf 'pub const CACHE_FIXTURE_ID: &str = "%s";\n' "$fixture_id" >>"$fixture/src/lib.rs"
printf 'cache fixture %s\n' "$fixture_id" >"$fixture/README.md"
git -C "$fixture" init -q
git -C "$fixture" add Cargo.toml Cargo.lock src/lib.rs README.md
GIT_AUTHOR_DATE=2000-01-01T00:00:00Z GIT_COMMITTER_DATE=2000-01-01T00:00:00Z \
  git -C "$fixture" -c core.hooksPath=/dev/null -c user.name=Test -c user.email=test@example.com \
  -c commit.gpgsign=false commit -qm seed
if [[ "$mode" == next ]]; then
  printf 'new revision, identical compiler inputs\n' >>"$fixture/README.md"
  git -C "$fixture" add README.md
  GIT_AUTHOR_DATE=2000-01-01T00:00:01Z GIT_COMMITTER_DATE=2000-01-01T00:00:01Z \
    git -C "$fixture" -c core.hooksPath=/dev/null -c user.name=Test -c user.email=test@example.com \
    -c commit.gpgsign=false commit -qm next
fi
printf 'revision=%s\n' "$(git -C "$fixture" rev-parse HEAD)" >>"${GITHUB_OUTPUT:?}"
printf 'fixture-id=%s\n' "$fixture_id" >>"$GITHUB_OUTPUT"
