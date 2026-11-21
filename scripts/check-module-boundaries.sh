#!/bin/sh
# Module boundaries inside vykar-core. Fails when a lower layer names a higher one.
#   engine modules (repo, index, compress, chunker) must not use commands or app;
#   commands must not use app;
#   index may use repo only through repo::format, which itself uses neither.
# A text match, not a compiler check: it sees `crate::`/`super::` paths and
# `crate::{..}` groups, not `super::{..}` groups, paths broken across lines or macros.
set -eu
root=${1:-$(cd "$(dirname "$0")/.." && pwd)}
cd "$root"
src=crates/vykar-core/src
fail=0

# Print every line of the given files with each `use` statement joined onto one
# line, prefixed by file:line, so grouped imports spanning lines are seen whole.
flatten() {
    find "$@" -name '*.rs' -type f | sort | while read -r f; do
        awk -v f="$f" '
            inuse { stmt = stmt " " $0; if ($0 ~ /;/) { print f ":" start ": " stmt; inuse = 0 }; next }
            /^[[:space:]]*(pub(\([a-z]+\))?[[:space:]]+)?use[[:space:]]/ && $0 !~ /;/ { inuse = 1; start = FNR; stmt = $0; next }
            { print f ":" FNR ": " $0 }
        ' "$f"
    done
}

# forbid DESCRIPTION ERE PATH...
forbid() {
    desc=$1; re=$2; shift 2
    hits=$(flatten "$@" | grep -E "$re" || true)
    if [ -n "$hits" ]; then
        printf 'module boundary violated: %s\n%s\n' "$desc" "$hits" >&2
        fail=1
    fi
}

w='([^A-Za-z0-9_]|$)'
up_ca="(crate|super(::super)*)::(commands|app)$w|crate::\{[^;]*(commands|app)$w"
forbid "repo/index/compress/chunker must not use commands or app" "$up_ca" \
    "$src/repo" "$src/index" "$src/compress" "$src/chunker"
forbid "commands must not use app" "(crate|super(::super)*)::app$w|crate::\{[^;]*app$w" "$src/commands"
hits=$(flatten "$src/index" | sed 's/crate::repo::format::/ALLOWED::/g' | grep -E "(crate|super(::super)*)::repo$w|crate::\{[^;]*repo$w" || true)
if [ -n "$hits" ]; then
    printf 'module boundary violated: index may use repo only through repo::format\n%s\n' "$hits" >&2
    fail=1
fi
forbid "repo/format.rs must stay a leaf (no crate::repo, crate::index, super::)" \
    "crate::(repo|index)$w|super::" "$src/repo/format.rs"
exit $fail
