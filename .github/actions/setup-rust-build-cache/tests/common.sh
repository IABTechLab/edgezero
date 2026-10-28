#!/usr/bin/env bash
# Shared assertions for local cache contracts; callers own fixtures and cleanup.
fail() { printf 'Rust cache test failed: %s\n' "$*" >&2; exit 1; }
assert_line() { grep -Fqx -- "$2" "$1" || fail "expected '$2' in $1"; }
