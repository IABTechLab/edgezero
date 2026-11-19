#!/usr/bin/env bash
# Input: isolated normal dependency graph from cargo tree --format '{p}|{f}'.
# Tokio's async-I/O traits need no executor; reject runtime and native-I/O features.
assert_wasm_tokio_features() {
  awk -F '|' '
    /^tokio v/ {
      if (NF != 2) {
        print "missing or malformed Tokio feature data: " $0
        bad = 1
        next
      }
      sub(/ \(\*\)$/, "", $2)
      count = split($2, features, ",")
      for (i = 1; i <= count; i++) {
        if (features[i] ~ /^(fs|full|io-std|io-uring|net|process|rt|rt-multi-thread|signal|time)$/) {
          print "Tokio runtime/native-I/O feature in " $1 ": " features[i]
          bad = 1
        }
      }
    }
    END { exit bad }
  ' >&2
}
