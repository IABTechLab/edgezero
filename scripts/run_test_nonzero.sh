#!/bin/sh

set -eu

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
listing=$(mktemp "${TMPDIR:-/tmp}/edgezero-tests.XXXXXX")
trap 'rm -f "$listing"' EXIT HUP INT TERM

if [ -n "$selection" ]; then
    "$@" -- "$selection" --list >"$listing"
else
    "$@" -- --list >"$listing"
fi

REQUIRED_TEST_SENTINELS="$sentinels" awk -v exact="$exact" '
    BEGIN { required = split(ENVIRON["REQUIRED_TEST_SENTINELS"], names, "\n") }
    /: test$/ {
        count += 1
        name = $0
        sub(/: test$/, "", name)
        for (i = 1; i <= required; i++) {
            if (name == names[i] || (!exact && name ~ ("::" names[i] "$"))) {
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
' "$listing"

if [ -n "$selection" ]; then
    "$@" -- "$selection"
else
    "$@"
fi
