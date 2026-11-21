#!/usr/bin/env bash
#
# Run the old-release `config` refusal check against the real release binaries.
#
#   scripts/check-old-release-refusal.sh [tag...]   (default: v0.19.1 v0.24.0)
#
# The test consumes exactly v0.19.1 and v0.24.0; other tags are built but the
# test then fails for the missing ones.
#
# Builds each tag from source with its own Cargo.lock (--locked) in a detached
# worktree, then runs `old_release_binaries_refuse_v6_map_by_version` in
# crates/vykar-core/tests/legacy_config_refusal.rs with those binaries: each
# must refuse the v6 probe map with "unsupported repository version: 6" and the
# positional control with "deserialization error". The test fails if a binary
# is missing (VYKAR_REQUIRE_OLD_BINARIES=1).
#
# Scratch space (worktrees, one target dir per tag) is created under $TMPDIR
# (default /tmp) and removed on exit.

set -euo pipefail

if (($# == 0)); then
    set -- v0.19.1 v0.24.0
fi

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
# An explicit template: macOS mktemp ignores TMPDIR without one.
WORK="$(mktemp -d "${TMPDIR:-/tmp}/vykar-old-release.XXXXXX")"
cleanup() {
    for src in "$WORK"/src-*; do
        if [[ -d "$src" ]]; then
            git -C "$REPO_ROOT" worktree remove --force "$src" || true
        fi
    done
    rm -rf "$WORK"
}
trap cleanup EXIT

export VYKAR_REQUIRE_OLD_BINARIES=1
for tag in "$@"; do
    echo "==> building vykar $tag"
    src="$WORK/src-$tag"
    target="$WORK/target-$tag"
    git -C "$REPO_ROOT" worktree add --detach "$src" "$tag" >/dev/null
    (cd "$src" && CARGO_TARGET_DIR="$target" cargo build --release --locked -p vykar-cli >/dev/null)
    "$target/release/vykar" --version
    # v0.24.0 -> VYKAR_BIN_V0_24_0
    var="VYKAR_BIN_$(tr '[:lower:].' '[:upper:]_' <<<"$tag")"
    export "$var=$target/release/vykar"
done

echo "==> running the refusal check"
cd "$REPO_ROOT"
cargo test -p vykar-core --test legacy_config_refusal \
    old_release_binaries_refuse_v6_map_by_version -- --exact --nocapture
