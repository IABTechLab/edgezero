#!/usr/bin/env bash
set -euo pipefail

# Validates the application and Rust workspace selected by
# setup-rust-build-cache, binds its cache keys to the application and Cargo.lock
# bytes, and enables sccache for every later Rust build in the current GitHub
# Actions job. Always publishes a stable, confined, application-scoped
# CARGO_TARGET_DIR. Remote target artifacts are isolated by source revision.
#
# This script reads no application manifest and runs no application code. The
# action therefore remains independent of the package, adapter, and deployment
# that consume the compiled output.

fail() {
  printf '::error::%s\n' "$*" >&2
  exit 1
}

canonical_path() {
  local path="$1"
  [[ ! "$path" =~ [[:cntrl:]] ]] || fail 'Rust cache paths must not contain control characters'
  local resolved
  # Preserve trailing newlines until validation, including in symlink targets.
  resolved=$(realpath "$path" 2>/dev/null && printf '.') || fail "could not resolve path '$path'"
  resolved=${resolved%.}
  resolved=${resolved%$'\n'}
  [[ ! "$resolved" =~ [[:cntrl:]] ]] || fail 'Resolved Rust cache paths must not contain control characters'
  printf '%s' "$resolved"
}

append_output() {
  printf '%s=%s\n' "$1" "$2" >>"${GITHUB_OUTPUT:?}"
}

is_under() {
  local root="${1%/}"
  local path="${2%/}"
  [[ "$path" == "$root" || "$path" == "$root"/* ]]
}

sha256_file() {
  local file="$1"
  if command -v sha256sum >/dev/null 2>&1; then
    sha256sum "$file" | awk '{ print $1 }'
  elif command -v shasum >/dev/null 2>&1; then
    shasum -a 256 "$file" | awk '{ print $1 }'
  else
    fail "required command 'sha256sum' or 'shasum' was not found"
  fi
}

main() {
  local workspace="${GITHUB_WORKSPACE:?GITHUB_WORKSPACE is required}"
  : "${GITHUB_OUTPUT:?GITHUB_OUTPUT is required}"
  local env_file="${GITHUB_ENV:?GITHUB_ENV is required}"
  local app_name="${EDGEZERO__RUST_CACHE__APP_NAME-}"
  local cache_target="${EDGEZERO__RUST_CACHE__CACHE_TARGET-false}"
  local save_target="${EDGEZERO__RUST_CACHE__SAVE_TARGET-false}"
  local revision="${EDGEZERO__RUST_CACHE__TARGET_CACHE_REVISION-}"
  local working_directory="${EDGEZERO__RUST_CACHE__WORKING_DIRECTORY-}"

  [[ "$app_name" =~ ^[a-z0-9][a-z0-9._-]{0,63}$ ]] ||
    fail "input 'app-name' must be a lowercase app identifier using only letters, digits, '.', '_', or '-'"
  case "$cache_target" in
    true | false) ;;
    *) fail "input 'cache-target' must be 'true' or 'false'" ;;
  esac
  case "$save_target" in
    true | false) ;;
    *) fail "input 'save-target' must be 'true' or 'false'" ;;
  esac
  [[ "$save_target" != true || "$cache_target" == true ]] ||
    fail "input 'save-target' requires cache-target true"
  if [[ "$cache_target" == true || -n "$revision" ]]; then
    [[ "$revision" =~ ^[0-9a-f]{40}$ || "$revision" =~ ^[0-9a-f]{64}$ ]] ||
      fail "input 'target-cache-revision' must be a full lowercase Git SHA (required when cache-target is true)"
  fi
  [[ -n "$working_directory" ]] ||
    fail "input 'working-directory' cannot be empty"
  [[ "$working_directory" != /* ]] ||
    fail "input 'working-directory' must be relative to github.workspace"
  command -v realpath >/dev/null 2>&1 ||
    fail "required command 'realpath' was not found"

  local workspace_real project_real lockfile
  workspace_real=$(canonical_path "$workspace")
  [[ -d "$workspace/$working_directory" ]] ||
    fail "working-directory '$working_directory' does not exist or is not a directory"
  project_real=$(canonical_path "$workspace/$working_directory")
  is_under "$workspace_real" "$project_real" ||
    fail "input 'working-directory' must resolve inside github.workspace"

  lockfile="$project_real/Cargo.lock"
  [[ -f "$lockfile" && ! -L "$lockfile" ]] ||
    fail "working-directory '$working_directory' must contain a regular Cargo.lock file"

  local lock_sha
  lock_sha=$(sha256_file "$lockfile")
  [[ "$lock_sha" =~ ^[0-9a-f]{64}$ ]] ||
    fail "could not compute a lowercase SHA-256 for Cargo.lock"

  local runner_temp="${RUNNER_TEMP:?RUNNER_TEMP is required}"
  [[ -d "$runner_temp" ]] || fail "RUNNER_TEMP does not exist or is not a directory"
  local runner_temp_real cache_root app_dir target_dir selection_file path
  runner_temp_real=$(canonical_path "$runner_temp")
  cache_root="$runner_temp_real/edgezero-rust-cache"
  app_dir="$cache_root/$app_name"
  target_dir="$app_dir/target"
  selection_file="$app_dir/selection"
  for path in "$cache_root" "$app_dir" "$target_dir" "$selection_file"; do
    [[ ! -L "$path" ]] || fail "Rust cache path must not be a symlink: '$path'"
  done
  for path in "$cache_root" "$app_dir" "$target_dir"; do
    [[ ! -e "$path" || -d "$path" ]] || fail "Rust cache path is not a directory: '$path'"
  done
  [[ ! -e "$selection_file" || -f "$selection_file" ]] || fail "Rust cache selection is not a regular file"

  local ownership_revision="$revision" selection already_initialized=false
  if [[ -z "$ownership_revision" ]]; then
    ownership_revision=$(git -C "$project_real" rev-parse --verify HEAD 2>/dev/null || true)
    [[ "$ownership_revision" =~ ^[0-9a-f]{40}$ || "$ownership_revision" =~ ^[0-9a-f]{64}$ ]] || ownership_revision=''
  fi
  selection=$(printf '%s\n' "$project_real" "${ownership_revision:-uncached}" "$lock_sha" "$cache_target" "$save_target")
  if [[ -e "$selection_file" ]]; then
    [[ -n "$ownership_revision" ]] || fail "repeated non-Git cache setup requires target-cache-revision"
    [[ "$(<"$selection_file")" == "$selection" ]] ||
      fail "Rust target directory already belongs to a different workspace, revision, lockfile or cache settings"
    already_initialized=true
  elif [[ -d "$target_dir" ]]; then
    local target_contents
    # Quote control characters so newline-only names survive command substitution.
    target_contents=$(ls -Aq -- "$target_dir") || fail "could not inspect Rust target directory"
    [[ -z "$target_contents" ]] ||
      fail "Rust target directory contains unowned artifacts without a selection marker"
  fi

  local runner_os="${RUNNER_OS:?RUNNER_OS is required}" runner_arch="${RUNNER_ARCH:?RUNNER_ARCH is required}"
  [[ "$runner_os" =~ ^[a-zA-Z0-9_-]+$ && "$runner_arch" =~ ^[a-zA-Z0-9_-]+$ ]] ||
    fail "runner OS and architecture must be safe cache-key components"
  local source_prefix source_key target_key=''
  source_prefix="edgezero-app-$app_name-rust-sources-$runner_os-$runner_arch-"
  source_key="$source_prefix$lock_sha"
  [[ -z "$revision" ]] || target_key="edgezero-app-$app_name-rust-target-v2-$runner_os-$runner_arch-$lock_sha-$revision-exact"

  mkdir -p "$target_dir"
  target_dir=$(canonical_path "$target_dir")
  is_under "$runner_temp_real" "$target_dir" ||
    fail "Cargo target cache directory must resolve inside RUNNER_TEMP"
  printf '%s\n' "$selection" >"$selection_file"

  append_output already-initialized "$already_initialized"
  append_output source-cache-key "$source_key"
  append_output source-cache-restore-prefix "$source_prefix"
  append_output target-cache-key "$target_key"
  append_output cargo-target-dir "$target_dir"
  {
    printf 'RUSTC_WRAPPER=sccache\n'
    printf 'SCCACHE_GHA_ENABLED=true\n'
    printf 'CARGO_INCREMENTAL=0\n'
    printf 'CARGO_TARGET_DIR=%s\n' "$target_dir"
    printf 'EDGEZERO__RUST_CACHE__TARGET_DIR=%s\n' "$target_dir"
  } >>"$env_file"
}

main "$@"
