#!/usr/bin/env bash
# Parse dependency requirements with the generator's TOML-based contract tests.
# Compare complete requirements, including audited exact pins, across the root,
# demo, reusable and request-fidelity fixtures, scaffold seeds, and deployment smoke fixture.
set -euo pipefail
REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"
for sentinel in seeded_dependencies_track_the_edgezero_workspace independent_dependencies_track_the_edgezero_workspace request_fidelity_dependencies_track_the_edgezero_workspace; do
  sh scripts/run_test_nonzero.sh "$sentinel" \
    cargo test --offline --locked -p edgezero-cli --features cli --lib "$sentinel"
done
