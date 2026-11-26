#!/bin/sh
# Test-support features (testing, sim, fuzzing) never reach a product build:
# the normal (non-dev) dependency graph of each product binary names none.
set -eu
cd "$(dirname "$0")/.."
fail=0
for p in vykar-cli vykar-gui vykar-server; do
    hits=$(cargo tree -p "$p" -e features,no-dev --target all --prefix none \
        | grep -E 'feature "(testing|sim|fuzzing)"' | sort -u || true)
    if [ -n "$hits" ]; then
        printf '%s enables test-support features in a product build:\n%s\n' "$p" "$hits" >&2
        fail=1
    fi
done
exit $fail
