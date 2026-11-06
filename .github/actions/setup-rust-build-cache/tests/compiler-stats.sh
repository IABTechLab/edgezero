#!/usr/bin/env bash
# Shared sccache stats predicates for the compiler seed and reuse probe.
compiler_error_free='(.stats.cache_errors.counts.Rust // 0) == 0 and (.stats.cache_read_errors // 0) == 0 and (.stats.cache_write_errors // 0) == 0'
# Any Rust miss means some unit, possibly the one just written, was not read back.
compiler_hit() {
  jq -e "(.stats.cache_hits.counts.Rust // 0) > 0 and (.stats.cache_misses.counts.Rust // 0) == 0 and $compiler_error_free" <<<"$1" >/dev/null
}
compiler_wrote() {
  jq -e "(.stats.cache_writes // 0) > 0 and $compiler_error_free" <<<"$1" >/dev/null
}
