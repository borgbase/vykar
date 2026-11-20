#!/usr/bin/env bash
#
# Generate the frozen segmented-index (format v4/v5) fixtures.
#
#   scripts/gen-segmented-fixtures.sh [--new-scenario NAME] <commit-ish>
#
# DO NOT RUN THIS TO "REFRESH" THE COMMITTED FIXTURES.
#
# The fixtures prove that today's binary can still read and extend segmented
# repositories written by an older one. Regenerating them with a newer binary
# hides exactly the regressions they exist to catch: one shared by the writer
# and the reader. `tests/segmented_fixture_compat.rs` pins each archive's
# BLAKE3 digest, so a regenerated archive fails that suite.
#
# The committed archives were generated from commit
# d0a2c1b56399b74f2c57d53e657a250e004ba3f4
# ("test(index): storage-fault tests for the v5 commit and upgrade").
# Each archive contains:
#
#   repo/       the repository. Its index head names a base, exactly two
#               segments over it, and a non-empty `retired` set (the chain
#               the last fold replaced)
#   source/     the exact tree the last snapshot contains
#   cache/      local state the generating binary wrote (pin, file cache,
#               full index cache and derived caches)
#   meta.json   encryption mode, passphrase, snapshot names, index objects,
#               format version and source commit
#
# v5 starts from `vykar init`. v4 starts from the frozen v2 fixture of the same
# mode, upgraded in place with `vykar upgrade`: that is the only way a v4
# repository comes to exist. Both then run 15 backups and one snapshot delete
# (16 segments), `compact --threshold 0` (the soft limit of 16 segments folds
# the chain into a base and keeps the previous objects as retired), and two
# more backups (two segments).
#
# With --new-scenario NAME the archives are written as v{4,5}-NAME-<mode>.tar.gz;
# the script never overwrites an existing archive.
#
# Scratch space (worktree, build, staging) is created under $TMPDIR (default
# /tmp). Set CARGO_TARGET_DIR to reuse a build directory across runs.

set -euo pipefail

SCENARIO=""
if [[ "${1:-}" == "--new-scenario" ]]; then
    SCENARIO="${2:?--new-scenario needs a name}"
    shift 2
fi
COMMIT="${1:?usage: $0 [--new-scenario NAME] <commit-ish>}"

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
FIXTURE_DIR="$REPO_ROOT/crates/vykar-core/tests/fixtures"
PASSPHRASE="fixture-passphrase"
MODES=(none aes256gcm chacha20poly1305)
MAX_ARCHIVE_BYTES=$((1024 * 1024)) # prek: check-added-large-files --maxkb=1024

archive_name() { # <version> <mode>
    if [[ -n "$SCENARIO" ]]; then
        echo "v$1-$SCENARIO-$2.tar.gz"
    else
        echo "v$1-$2.tar.gz"
    fi
}

for v in 4 5; do
    for mode in "${MODES[@]}"; do
        if [[ -e "$FIXTURE_DIR/$(archive_name "$v" "$mode")" ]]; then
            echo "refusing to overwrite $FIXTURE_DIR/$(archive_name "$v" "$mode"): fixtures are frozen" >&2
            exit 1
        fi
    done
done
for mode in "${MODES[@]}"; do
    [[ -f "$FIXTURE_DIR/v2-$mode.tar.gz" ]] || { echo "missing v2-$mode.tar.gz" >&2; exit 1; }
done

# An explicit template: macOS mktemp ignores TMPDIR without one.
WORK="$(mktemp -d "${TMPDIR:-/tmp}/vykar-fixtures.XXXXXX")"
OUT="$WORK/out" # archives move into FIXTURE_DIR only once all six are built
mkdir -p "$OUT"
cleanup() {
    if [[ -d "$WORK/src" ]]; then
        git -C "$REPO_ROOT" worktree remove --force "$WORK/src" || true
    fi
    rm -rf "$WORK"
}
trap cleanup EXIT

echo "==> building vykar from $COMMIT"
git -C "$REPO_ROOT" worktree add --detach "$WORK/src" "$COMMIT" >/dev/null
SHA="$(git -C "$WORK/src" rev-parse HEAD)"
TARGET="${CARGO_TARGET_DIR:-$WORK/target}"
(cd "$WORK/src" && CARGO_TARGET_DIR="$TARGET" cargo build --release -p vykar-cli >/dev/null)
VYKAR="$TARGET/release/vykar"
"$VYKAR" --version

# The same content as gen-v2-fixtures.sh's first snapshot.
make_source_v1() {
    local dir="$1"
    mkdir -p "$dir/nested"
    python3 - "$dir" <<'PY'
import sys, pathlib
d = pathlib.Path(sys.argv[1])
block = bytes((i * 37 + 11) % 256 for i in range(4096))
(d / "data.bin").write_bytes(block * 512)          # 2 MiB
(d / "nested" / "notes.txt").write_text("fixture line\n" * 4096)
(d / "empty.txt").write_bytes(b"")
PY
}

# One small new file per backup, so each backup's segment differs.
add_extra() { # <source> <i>
    mkdir -p "$1/extra"
    python3 - "$1/extra/f$2.txt" "$2" <<'PY'
import sys
open(sys.argv[1], "w").write(f"backup {sys.argv[2]}\n" * 256)
PY
}

# Prints "<base bytes> <segment count>" from `vykar info`.
index_shape() {
    "${run[@]}" info </dev/null | python3 -c '
import re, sys
m = re.search(r"^\s*Index\s+(\S+ \S+) base, (\d+) segments? ", sys.stdin.read(), re.M)
if not m:
    sys.exit("no Index row in `vykar info` output")
print(m.group(1).replace(" ", ""), m.group(2))
'
}

index_objects() { # <repo>
    ls "$1/index.d" | sort
}

die() {
    echo "error: $*" >&2
    exit 1
}

gen_one() { # <version> <mode>
    local version="$1" mode="$2"
    local stage="$WORK/stage-v$version-$mode"
    local repo="$stage/repo" src="$stage/source" cache="$stage/cache"
    local archive
    archive="$OUT/$(archive_name "$version" "$mode")"
    echo "==> generating v$version fixture: $mode"
    rm -rf "$stage"
    mkdir -p "$stage" "$cache"

    # Keep every per-user location off the operator's real directories.
    local home="$WORK/home-v$version-$mode"
    mkdir -p "$home"
    export HOME="$home" XDG_CACHE_HOME="$home/.cache" XDG_CONFIG_HOME="$home/.config" \
        XDG_DATA_HOME="$home/.local/share" USER=fixture LOGNAME=fixture NO_COLOR=1
    unset VYKAR_CONFIG
    export VYKAR_PASSPHRASE="$PASSPHRASE"

    cat >"$stage/vykar.yaml" <<YAML
sources:
  - path: $src
    label: fixture-src
repositories:
  - label: main
    url: $repo
encryption:
  mode: $mode
compression:
  algorithm: zstd
  zstd_level: 3
chunker:
  min_size: 8192
  avg_size: 16384
  max_size: 65536
cache_dir: $cache
hostname: fixture-host
YAML
    run=("$VYKAR" --config "$stage/vykar.yaml")

    local derived_from=""
    if [[ "$version" == 5 ]]; then
        mkdir -p "$repo" "$src"
        "${run[@]}" init </dev/null
        make_source_v1 "$src"
    else
        derived_from="v2-$mode.tar.gz"
        tar -xzf "$FIXTURE_DIR/$derived_from" -C "$stage" repo source
        "${run[@]}" upgrade --yes </dev/null
        read -r base segs <<<"$(index_shape)"
        [[ "$base" != "0B" && "$segs" == 0 ]] || die "after upgrade: base $base, $segs segments"
    fi
    local fmt
    fmt="$("${run[@]}" info </dev/null | awk '$1 == "Format" {print $2}')"
    [[ "$fmt" == "v$version" ]] || die "repository is at format $fmt, expected v$version"

    for i in $(seq 1 15); do
        if ((i >= 2)); then add_extra "$src" "$i"; fi
        "${run[@]}" backup </dev/null
    done
    local first
    first="$("${run[@]}" list </dev/null | awk 'NR>1 && NF && !f {print $1; f=1}')"
    "${run[@]}" snapshot delete "$first" </dev/null
    "${run[@]}" compact --threshold 0 </dev/null

    # The fold leaves the new base and the previous live set (retired).
    read -r base segs <<<"$(index_shape)"
    [[ "$base" != "0B" && "$segs" == 0 ]] || die "after compact: base $base, $segs segments (no fold)"
    local folded
    folded="$(index_objects "$repo")"
    (($(wc -l <<<"$folded") >= 2)) || die "after compact: no retired objects in index.d/"

    for i in 16 17; do
        add_extra "$src" "$i"
        "${run[@]}" backup </dev/null
    done
    "${run[@]}" check --verify-data </dev/null

    # Two segments over the same base, the retired set still on disk.
    read -r base segs <<<"$(index_shape)"
    [[ "$base" != "0B" && "$segs" == 2 ]] || die "final: base $base, $segs segments"
    local final
    final="$(index_objects "$repo")"
    [[ -z "$(comm -23 <(echo "$folded") <(echo "$final"))" ]] || die "objects vanished from index.d/"
    (($(wc -l <<<"$final") == $(wc -l <<<"$folded") + 2)) || die "expected exactly 2 new index objects"
    echo "    head: base, 2 segments, $(($(wc -l <<<"$folded") - 1)) retired"

    if [[ -n "$(find "$home" -type f)" ]]; then
        echo "warning: files written outside cache_dir:" >&2
        find "$home" -type f >&2
    fi

    local snapshots
    snapshots="$("${run[@]}" list </dev/null | awk 'NR>1 && NF {print $1}' | tr '\n' ' ')"

    python3 - "$stage" "$repo" "$mode" "$PASSPHRASE" "$SHA" "$snapshots" "$version" "$derived_from" <<'PY'
import json, os, pathlib, sys
stage, repo, mode, passphrase, sha, snaps, version, derived = sys.argv[1:9]
meta = {
    "encryption": mode,
    "passphrase": passphrase if mode != "none" else None,
    "repo_url_at_generation": repo,
    "generated_from_commit": sha,
    "format_version": int(version),
    "snapshots": snaps.split(),
    "expected_segments": 2,
    "index_objects": sorted(os.listdir(pathlib.Path(repo) / "index.d")),
    "derived_from": derived or None,
}
(pathlib.Path(stage) / "meta.json").write_text(json.dumps(meta, indent=2) + "\n")
PY

    # Python's tarfile, not tar(1): no AppleDouble members, and the operator's
    # user and group stay out of the headers.
    python3 - "$stage" "$archive" <<'PY'
import sys, tarfile
stage, archive = sys.argv[1:3]
def neutral(info):
    info.uid = info.gid = 0
    info.uname = info.gname = ""
    return info
with tarfile.open(archive, "w:gz", format=tarfile.GNU_FORMAT) as tar:
    for name in ("repo", "source", "cache", "meta.json"):
        tar.add(f"{stage}/{name}", arcname=name, filter=neutral)
PY
    python3 - "$archive" <<'PY'
import sys, tarfile
bad = [n for n in tarfile.open(sys.argv[1]).getnames()
       if n.split("/")[-1].startswith("._")]
if bad:
    sys.exit(f"AppleDouble members leaked into the archive: {bad[:5]}")
PY
    local size
    size="$(wc -c <"$archive" | tr -d ' ')"
    if ((size > MAX_ARCHIVE_BYTES)); then
        rm -f "$archive"
        die "$archive is $size bytes, over the 1024 KB limit"
    fi
    echo "    built $(basename "$archive") ($size bytes)"
}

for version in 5 4; do
    for mode in "${MODES[@]}"; do
        gen_one "$version" "$mode"
    done
done

mv "$OUT"/*.tar.gz "$FIXTURE_DIR/"
ls -l "$FIXTURE_DIR"/v[45]-*.tar.gz
echo "==> done"
