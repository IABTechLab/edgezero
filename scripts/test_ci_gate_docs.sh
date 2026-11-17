#!/usr/bin/env bash
# Exercise the documentation guard in isolated fixtures.
# Markdown backticks below are literal fixture data, not command substitutions.
# shellcheck disable=SC2016
set -euo pipefail
ROOT=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)
WORK_DIR=$(mktemp -d "${TMPDIR:-/tmp}/edgezero-gate-docs.XXXXXX")
trap 'rm -rf -- "$WORK_DIR"' EXIT

reset_fixture() {
  rm -rf -- "$WORK_DIR/repo"
  mkdir -p "$WORK_DIR/repo/scripts" "$WORK_DIR/repo/.claude/commands"
  cp "$ROOT/scripts/check_ci_gate_docs.sh" "$WORK_DIR/repo/scripts/"
  printf '%s\n' \
    '# Conventions' '1. `outside-before`' '## CI Gates' \
    '1. `cargo fmt --all -- --check`' '2. `cargo test --workspace --all-targets`' \
    '## Commands' '1. `outside-after`' \
    '| `/check-ci` | Run all 2 CI gate checks locally |' > "$WORK_DIR/repo/CLAUDE.md"
  printf '%s\n' \
    'Run the commands below sequentially.' \
    '1. `cargo fmt --all -- --check`' '2. `cargo test --workspace --all-targets`' \
    > "$WORK_DIR/repo/.claude/commands/check-ci.md"
}

replace() {
  local file="$1" old="$2" new="$3"
  awk -v old="$old" -v new="$new" '
    { position = index($0, old) }
    position { $0 = substr($0, 1, position - 1) new substr($0, position + length(old)) }
    { print }
  ' "$WORK_DIR/repo/$file" > "$WORK_DIR/changed"
  mv "$WORK_DIR/changed" "$WORK_DIR/repo/$file"
}

check_case() {
  local name="$1" expected="$2" diagnostic="$3" status=0
  # Deliberately invoke from outside the fixture repository.
  (cd "$WORK_DIR" && bash repo/scripts/check_ci_gate_docs.sh) > "$WORK_DIR/output" 2>&1 || status=$?
  if [[ "$status" != "$expected" ]] || ! grep -Fq -- "$diagnostic" "$WORK_DIR/output"; then
    printf 'FAIL %s: expected exit %s and diagnostic %s, got exit %s\n' "$name" "$expected" "$diagnostic" "$status" >&2
    head -40 "$WORK_DIR/output" >&2
    exit 1
  fi
  printf 'PASS %s\n' "$name"
}

reset_fixture
check_case 'matching commands, independent cwd, unrelated numbered examples' 0 '2 gate commands agree'

for file in CLAUDE.md .claude/commands/check-ci.md; do
  reset_fixture
  replace "$file" 'cargo fmt --all -- --check' 'cargo fmt --all'
  check_case "same-count drift in $file" 1 'ordered gate commands differ'

  reset_fixture
  rm "$WORK_DIR/repo/$file"
  check_case "missing $file" 1 "missing $file"
done

reset_fixture
replace .claude/commands/check-ci.md 'cargo fmt --all -- --check' 'temporary'
replace .claude/commands/check-ci.md 'cargo test --workspace --all-targets' 'cargo fmt --all -- --check'
replace .claude/commands/check-ci.md 'temporary' 'cargo test --workspace --all-targets'
check_case 'reordered commands' 1 'ordered gate commands differ'

reset_fixture
replace CLAUDE.md '## CI Gates' '## Other checks'
check_case 'missing CI Gates section' 1 'no commands found'

reset_fixture
replace CLAUDE.md '1. `cargo fmt --all -- --check`' ''
replace CLAUDE.md '2. `cargo test --workspace --all-targets`' ''
check_case 'empty CI Gates section' 1 'no commands found'

reset_fixture
printf 'Run checks.\n' > "$WORK_DIR/repo/.claude/commands/check-ci.md"
check_case 'empty local command list' 1 'ordered gate commands differ'

reset_fixture
printf '3. `cargo check --workspace`\n' >> "$WORK_DIR/repo/.claude/commands/check-ci.md"
check_case 'extra local gate' 1 'ordered gate commands differ'
replace CLAUDE.md '## Commands' $'3. `cargo check --workspace`\n## Commands'
check_case 'new matching gate with stale displayed count' 1 'Run all 3 CI gate checks locally'
replace CLAUDE.md 'Run all 2 CI gate checks locally' 'Run all 3 CI gate checks locally'
check_case 'new matching gate with derived count' 0 '3 gate commands agree'

reset_fixture
replace CLAUDE.md '| `/check-ci` |' '| `/other` |'
check_case 'missing command table row' 1 'CLAUDE.md /check-ci row must say'

reset_fixture
replace CLAUDE.md '## Commands' '## CI Gates follow-up'
check_case 'section stops at next level-two heading' 0 '2 gate commands agree'
