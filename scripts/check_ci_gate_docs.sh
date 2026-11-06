#!/usr/bin/env bash
# Keep the local check command aligned with the documented CI gates.
set -euo pipefail
ROOT=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)
cd "$ROOT"

fail() {
  printf 'check_ci_gate_docs: %s\n' "$1" >&2
  exit 1
}

for file in CLAUDE.md .claude/commands/check-ci.md; do
  [[ -f "$file" ]] || fail "missing $file"
done

gate_commands() {
  awk -v section="$2" '
    /^## CI Gates$/ { in_gates = 1; next }
    in_gates && /^## / { exit }
    (section == "all" || in_gates) && /^[0-9]+\. `.*`$/ {
      sub(/^[0-9]+\. `/, "")
      sub(/`$/, "")
      print
    }
  ' "$1"
}

expected=$(gate_commands CLAUDE.md gates)
actual=$(gate_commands .claude/commands/check-ci.md all)
[[ -n "$expected" ]] || fail 'no commands found in CLAUDE.md CI Gates section'
[[ "$actual" == "$expected" ]] || fail 'ordered gate commands differ between CLAUDE.md and .claude/commands/check-ci.md'

count=$(printf '%s\n' "$expected" | awk 'END { print NR }')
command_row=$(awk '/^\| `\/check-ci`/ { print }' CLAUDE.md)
[[ "$command_row" == *"Run all $count CI gate checks locally"* ]] || fail \
  "CLAUDE.md /check-ci row must say: Run all $count CI gate checks locally"

printf 'check_ci_gate_docs: %s gate commands agree\n' "$count"
