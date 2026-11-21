#!/usr/bin/env bash
#
# Generate the frozen repository-format-v3 fixtures.
#
#   scripts/gen-v3-fixtures.sh <tag>
#
# DO NOT RUN THIS TO "REFRESH" THE COMMITTED FIXTURES.
#
# Format v3 is what vykar 0.20 to 0.24 write: BLAKE3 chunk and pack IDs with a
# monolithic `index` and `index.gen`. The fixtures prove that today's binary
# reads and upgrades repositories those releases left behind. Regenerating
# them with a newer binary hides exactly the regressions they exist to catch.
# `tests/v3_fixture_compat.rs` pins each archive's BLAKE3 digest, so a
# regenerated archive fails that suite.
#
# The committed archives were generated with `scripts/gen-v3-fixtures.sh
# v0.24.0`, commit cd6782db29bff6638581758af72efcfda1267985. The script
# refuses a commit that is not a release tag. Scenario, per encryption mode:
#
#   init; backup A
#   add transient/only-in-b.txt and kept/from-b.txt; backup B
#   remove transient/, apply the second source change; backup C
#       (C references kept/from-b.txt's chunks, which sit in B's pack)
#   snapshot delete B; compact --threshold 0
#       (B's pack is partly live, so it is repacked: at least one repack is
#       required)
#   add nested/after-repack.txt; backup D (an index write after the repack)
#   check --verify-data
#
# Each archive contains:
#
#   repo/       the repository (format v3, monolithic index and index.gen)
#   source/     the exact tree snapshot D contains
#   cache/      local state the generating binary wrote
#   meta.json   encryption mode, passphrase, snapshot names in creation order
#               (A, C, D), the deleted snapshot (B), format version, source
#               tag and commit
#
# Scratch space (worktree, build, staging) is created under $TMPDIR (default
# /tmp). Set CARGO_TARGET_DIR to reuse a build directory across runs.

set -euo pipefail

TAG="${1:?usage: $0 <tag>}"

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
FIXTURE_DIR="$REPO_ROOT/crates/vykar-core/tests/fixtures"
PASSPHRASE="fixture-passphrase"
MODES=(none aes256gcm chacha20poly1305)
MAX_ARCHIVE_BYTES=$((1024 * 1024)) # prek: check-added-large-files --maxkb=1024

die() {
    echo "error: $*" >&2
    exit 1
}

for mode in "${MODES[@]}"; do
    if [[ -e "$FIXTURE_DIR/v3-$mode.tar.gz" ]]; then
        die "refusing to overwrite $FIXTURE_DIR/v3-$mode.tar.gz: fixtures are frozen"
    fi
done

# An explicit template: macOS mktemp ignores TMPDIR without one.
WORK="$(mktemp -d "${TMPDIR:-/tmp}/vykar-v3-fixtures.XXXXXX")"
OUT="$WORK/out" # archives move into FIXTURE_DIR only once all three are built
mkdir -p "$OUT"
cleanup() {
    if [[ -d "$WORK/src" ]]; then
        git -C "$REPO_ROOT" worktree remove --force "$WORK/src" || true
    fi
    rm -rf "$WORK"
}
trap cleanup EXIT

echo "==> building vykar from $TAG"
git -C "$REPO_ROOT" worktree add --detach "$WORK/src" "$TAG" >/dev/null
SHA="$(git -C "$WORK/src" rev-parse HEAD)"
RELEASE_TAG="$(git -C "$REPO_ROOT" describe --exact-match --tags "$SHA" 2>/dev/null)" ||
    die "$TAG ($SHA) is not a release tag"
TARGET="${CARGO_TARGET_DIR:-$WORK/target}"
(cd "$WORK/src" && CARGO_TARGET_DIR="$TARGET" cargo build --release --locked -p vykar-cli >/dev/null)
VYKAR="$TARGET/release/vykar"
VERSION="$("$VYKAR" --version)"
echo "$VERSION"
[[ "$VERSION" == "vykar ${RELEASE_TAG#v}" ]] || die "built '$VERSION', expected ${RELEASE_TAG#v}"

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

# The same change as gen-v2-fixtures.sh's second snapshot.
make_source_v2() {
    local dir="$1"
    python3 - "$dir" <<'PY'
import sys, pathlib
d = pathlib.Path(sys.argv[1])
# Keep the head of data.bin byte-identical so its chunks dedup, then append.
existing = (d / "data.bin").read_bytes()
tail = bytes((i * 91 + 7) % 256 for i in range(4096)) * 64
(d / "data.bin").write_bytes(existing + tail)
(d / "nested" / "added.txt").write_text("second snapshot only\n" * 2048)
PY
}

# B's two new files share one pack: the transient one dies with B, the kept
# one stays live through C, so compact must repack.
add_b_files() {
    local dir="$1"
    mkdir -p "$dir/transient" "$dir/kept"
    python3 - "$dir" <<'PY'
import sys, pathlib
d = pathlib.Path(sys.argv[1])
(d / "transient" / "only-in-b.txt").write_text(
    "".join(f"snapshot b only {i}\n" for i in range(8192)))
(d / "kept" / "from-b.txt").write_text(
    "".join(f"written in b, kept by c and d {i:06d}\n" for i in range(8192)))
PY
}

add_d_file() {
    python3 - "$1/nested/after-repack.txt" <<'PY'
import sys
open(sys.argv[1], "w").write("".join(f"after the repack {i}\n" for i in range(2048)))
PY
}

snapshot_names() {
    "${run[@]}" list </dev/null | awk 'NR>1 && NF {print $1}' | sort
}

# Runs one backup and prints the name of the one snapshot it created.
backup_new() {
    local before after new
    before="$(snapshot_names)"
    "${run[@]}" backup </dev/null >&2
    after="$(snapshot_names)"
    new="$(comm -13 <(echo "$before") <(echo "$after") | sed '/^$/d')"
    [[ -n "$new" && "$(wc -l <<<"$new")" -eq 1 ]] || die "backup created '$new', expected one snapshot"
    echo "$new"
}

gen_one() { # <mode>
    local mode="$1"
    local stage="$WORK/stage-$mode"
    local repo="$stage/repo" src="$stage/source" cache="$stage/cache"
    local archive="$OUT/v3-$mode.tar.gz"
    echo "==> generating v3 fixture: $mode"
    rm -rf "$stage"
    mkdir -p "$repo" "$src" "$cache"

    # Keep every per-user location off the operator's real directories.
    local home="$WORK/home-$mode"
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

    "${run[@]}" init </dev/null
    make_source_v1 "$src"
    local snap_a snap_b snap_c snap_d
    snap_a="$(backup_new)"

    add_b_files "$src"
    snap_b="$(backup_new)"

    rm -rf "$src/transient"
    make_source_v2 "$src"
    snap_c="$(backup_new)"

    "${run[@]}" snapshot delete "$snap_b" </dev/null
    local compact repacked
    compact="$("${run[@]}" compact --threshold 0 </dev/null 2>&1)"
    echo "$compact"
    repacked="$(sed -nE 's/.*Compaction complete: ([0-9]+) packs repacked,.*/\1/p' <<<"$compact")"
    [[ -n "$repacked" ]] || die "no 'Compaction complete' line in compact output"
    ((repacked >= 1)) || die "compact repacked $repacked packs, expected at least 1"

    add_d_file "$src"
    snap_d="$(backup_new)"

    "${run[@]}" check --verify-data </dev/null

    local fmt
    fmt="$("${run[@]}" info </dev/null | awk '$1 == "Format" {print $2}')"
    [[ "$fmt" == "v3" ]] || die "repository is at format '$fmt', expected v3"
    [[ -f "$repo/index" ]] || die "repo/index missing"
    [[ -f "$repo/index.gen" ]] || die "repo/index.gen missing"
    [[ ! -e "$repo/index.d" ]] || die "repo/index.d exists in a v3 repository"
    local listed expected
    listed="$(snapshot_names)"
    expected="$(printf '%s\n' "$snap_a" "$snap_c" "$snap_d" | sort)"
    [[ "$listed" == "$expected" ]] || die "list shows '$listed', expected A, C, D ('$expected')"
    if grep -qxF "$snap_b" <<<"$listed"; then
        die "deleted snapshot $snap_b is still listed"
    fi

    if [[ -n "$(find "$home" -type f)" ]]; then
        echo "warning: files written outside cache_dir:" >&2
        find "$home" -type f >&2
    fi

    python3 - "$stage" "$repo" "$mode" "$PASSPHRASE" "$SHA" "$RELEASE_TAG" \
        "$snap_a $snap_c $snap_d" "$snap_b" <<'PY'
import json, pathlib, sys
stage, repo, mode, passphrase, sha, tag, snaps, deleted = sys.argv[1:9]
meta = {
    "encryption": mode,
    "passphrase": passphrase if mode != "none" else None,
    "repo_url_at_generation": repo,
    "generated_from_commit": sha,
    "generated_from_tag": tag,
    "format_version": 3,
    "snapshots": snaps.split(),
    "deleted_snapshot": deleted,
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

for mode in "${MODES[@]}"; do
    gen_one "$mode"
done

mv "$OUT"/*.tar.gz "$FIXTURE_DIR/"
ls -l "$FIXTURE_DIR"/v3-*.tar.gz
echo "==> done"
