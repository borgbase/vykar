#!/usr/bin/env bash
#
# Regenerate the frozen repository-format-v2 fixtures.
#
#   scripts/gen-v2-fixtures.sh <pre-change-commit-ish>
#
# DO NOT RUN THIS TO "REFRESH" THE COMMITTED FIXTURES.
#
# The fixtures exist to prove that today's binary can still read repositories
# written by a binary that predates BLAKE3. Regenerating them with a newer
# binary destroys exactly the property they test — a regression shared by the
# writer and the reader becomes invisible. They are checked in as tar archives
# and are meant to stay byte-frozen for the life of format v2.
#
# This script is here to document how they were produced, and to produce a
# fixture for a *new* scenario (e.g. a fourth encryption mode) using a binary
# built from the same pre-change commit.
#
# The archives were generated from commit bfd0a85 ("docs: update benchmark"),
# the last commit before BLAKE3 landed. Each archive contains:
#
#   repo/       the repository itself (format v2)
#   source/     the exact tree that was backed up, for byte-identical restore
#   cache/      local state written by the pre-change binary: the TOFU pin file
#               and the per-repo file cache
#   meta.json   encryption mode, passphrase, snapshot names, the repo URL and
#               identity fingerprint at generation time, and the source commit
#
# `packs/` holds 256 shard directories, most of them empty, which git cannot
# store — hence tar archives extracted into a tempdir rather than loose files.
# They are gzipped to stay under the 1 MB per-file limit the pre-commit hooks
# enforce (~31 KB each compressed, ~3 MB raw).

set -euo pipefail

BASE_COMMIT="${1:-bfd0a85}"
REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
FIXTURE_DIR="$REPO_ROOT/crates/vykar-core/tests/fixtures"
PASSPHRASE="fixture-passphrase"

WORK="$(mktemp -d)"
trap 'rm -rf "$WORK"' EXIT

echo "==> building pre-change binary from $BASE_COMMIT"
git -C "$REPO_ROOT" worktree add --detach "$WORK/src" "$BASE_COMMIT" >/dev/null
BASE_SHA="$(git -C "$WORK/src" rev-parse HEAD)"
(cd "$WORK/src" && CARGO_TARGET_DIR="$WORK/target" cargo build --release -p vykar-cli >/dev/null)
VYKAR="$WORK/target/release/vykar"
"$VYKAR" --version

mkdir -p "$FIXTURE_DIR"

# Deterministic, self-similar content: the second snapshot repeats most of the
# first file's blocks so dedup has something to hit, and adds one new file.
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

gen_one() {
    local mode="$1"
    local stage="$WORK/stage-$mode"
    local repo="$stage/repo" src="$stage/source" cache="$stage/cache"
    echo "==> generating fixture: $mode"
    rm -rf "$stage"
    mkdir -p "$repo" "$src" "$cache"

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
YAML

    export VYKAR_PASSPHRASE="$PASSPHRASE"
    local run=("$VYKAR" --config "$stage/vykar.yaml")

    make_source_v1 "$src"
    "${run[@]}" init
    "${run[@]}" backup
    make_source_v2 "$src"
    "${run[@]}" backup
    "${run[@]}" check --verify-data

    # Snapshot IDs are server-assigned; record whatever the two backups produced.
    local snapshots
    snapshots="$("${run[@]}" list | awk 'NR>1 && NF {print $1}' | tr '\n' ' ')"

    python3 - "$stage" "$repo" "$mode" "$PASSPHRASE" "$BASE_SHA" "$snapshots" <<'PY'
import json, pathlib, sys
stage, repo, mode, passphrase, sha, snaps = sys.argv[1:7]
cache = pathlib.Path(stage) / "cache"
pin = next((p for p in cache.glob("pin.*")), None)
meta = {
    "encryption": mode,
    "passphrase": passphrase if mode != "none" else None,
    "repo_url_at_generation": repo,
    "snapshots": snaps.split(),
    "generated_from_commit": sha,
    "format_version": 2,
    "pin_fingerprint": json.loads(pin.read_text())["fingerprint"] if pin else None,
    "repo_id": json.loads(pin.read_text())["repo_id"] if pin else None,
}
(pathlib.Path(stage) / "meta.json").write_text(json.dumps(meta, indent=2) + "\n")
PY

    # COPYFILE_DISABLE: macOS bsdtar otherwise writes an AppleDouble `._x`
    # member beside every entry. bsdtar hides them again on extraction, but a
    # portable reader (the `tar` crate, GNU tar on CI) unpacks them as ordinary
    # files — which would put junk like `packs/ab/._<hex>` inside the fixture
    # repository and break `check`.
    COPYFILE_DISABLE=1 tar -C "$stage" -czf "$FIXTURE_DIR/v2-$mode.tar.gz" \
        repo source cache meta.json
    # Verified with python's tarfile, not `tar -t`: bsdtar hides AppleDouble
    # members on read too, so it cannot detect its own.
    python3 - "$FIXTURE_DIR/v2-$mode.tar.gz" <<'PY'
import sys, tarfile
bad = [n for n in tarfile.open(sys.argv[1]).getnames()
       if n.split("/")[-1].startswith("._")]
if bad:
    sys.exit(f"AppleDouble members leaked into the archive: {bad[:5]}")
PY
    echo "    wrote $FIXTURE_DIR/v2-$mode.tar.gz ($(du -h "$FIXTURE_DIR/v2-$mode.tar.gz" | cut -f1))"
}

for mode in none aes256gcm chacha20poly1305; do
    gen_one "$mode"
done

git -C "$REPO_ROOT" worktree remove --force "$WORK/src"
echo "==> done"
