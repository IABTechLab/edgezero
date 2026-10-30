#!/usr/bin/env bash

set -euo pipefail

selection=
case "${1:-}" in
    --ignored|--include-ignored)
        selection=$1
        shift
        ;;
esac

if [ "$#" -lt 2 ]; then
    echo "usage: $0 [--ignored|--include-ignored] <sentinel>|--inventory <environment[,environment]> <command...>" >&2
    exit 2
fi

exact=0
if [ "$1" = --inventory ]; then
    if [ "$#" -lt 3 ]; then
        echo "inventory mode needs an environment and command" >&2
        exit 2
    fi
    sentinels=$(node "$(dirname "$0")/check_outbound_cutover_evidence.mjs" --sentinels "$2")
    exact=1
    shift 2
else
    sentinels=$1
    shift
fi
if ! REQUIRED_TEST_SENTINELS="$sentinels" awk '
    BEGIN {
        count = split(ENVIRON["REQUIRED_TEST_SENTINELS"], names, "\n")
        if (count == 0) exit 1
        for (i = 1; i <= count; i++) {
            if (names[i] !~ /[^[:space:]]/) exit 1
        }
        exit 0
    }
'; then
    echo "required sentinel names must be nonempty" >&2
    exit 2
fi
output=$(mktemp "${TMPDIR:-/tmp}/edgezero-tests.XXXXXX")
trap 'rm -f "$output"' EXIT HUP INT TERM

if [ -n "$selection" ]; then
    "$@" -- "$selection" --list >"$output"
else
    "$@" -- --list >"$output"
fi

REQUIRED_TEST_SENTINELS="$sentinels" awk -v exact="$exact" '
    BEGIN { required = split(ENVIRON["REQUIRED_TEST_SENTINELS"], names, "\n") }
    /: test$/ {
        count += 1
        name = $0
        sub(/: test$/, "", name)
        for (i = 1; i <= required; i++) {
            if (name == names[i] || (!exact && length(name) > length(names[i]) + 2 &&
                substr(name, length(name) - length(names[i]) - 1) == ("::" names[i]))) {
                found[i] = 1
            }
        }
    }
    END {
        if (count == 0) {
            print "selected test suite contains zero tests" > "/dev/stderr"
            exit 1
        }
        for (i = 1; i <= required; i++) {
            if (!found[i]) {
                print "required sentinel test not listed: " names[i] > "/dev/stderr"
                missing = 1
            }
        }
        if (missing) exit 1
    }
' "$output"

if [ -n "$selection" ]; then
    set -- "$@" -- "$selection"
fi

set +e
"$@" 2>&1 | tee "$output"
statuses=("${PIPESTATUS[@]}")
set -e
if [ "${statuses[0]}" -ne 0 ]; then
    exit "${statuses[0]}"
fi
if [ "${statuses[1]}" -ne 0 ]; then
    exit "${statuses[1]}"
fi

REQUIRED_TEST_SENTINELS="$sentinels" awk -v exact="$exact" '
    BEGIN { required = split(ENVIRON["REQUIRED_TEST_SENTINELS"], names, "\n") }
    { sub(/\r$/, "") }
    /^test result:/ {
        if ($0 !~ /^test result: ok\. [0-9]+ passed; 0 failed;/) {
            print "selected test suite reported an unsuccessful summary" > "/dev/stderr"
            invalid = 1
        } else if ($4 + 0 > 0) {
            passed = 1
        }
    }
    /^test [^[:space:]]+( - should panic)? \.\.\. ok$/ {
        name = $2
        for (i = 1; i <= required; i++) {
            if (name == names[i] || (!exact && length(name) > length(names[i]) + 2 &&
                substr(name, length(name) - length(names[i]) - 1) == ("::" names[i]))) {
                found[i] = 1
            }
        }
    }
    END {
        if (!passed) {
            print "selected test suite has no positive passed-test summary" > "/dev/stderr"
            invalid = 1
        }
        for (i = 1; i <= required; i++) {
            if (!found[i]) {
                print "required sentinel test not passed: " names[i] > "/dev/stderr"
                invalid = 1
            }
        }
        if (invalid) exit 1
    }
' "$output"
