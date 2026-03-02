#!/usr/bin/env bash
# scripts/lib/common.sh — shared shell utilities for vykar scripts
#
# Source this file, don't execute it:
#   SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
#   source "$SCRIPT_DIR/lib/common.sh"

[[ -n "${_VYKAR_COMMON_LOADED:-}" ]] && return 0
_VYKAR_COMMON_LOADED=1

log() {
  printf '[%s] %s\n' "$(date '+%Y-%m-%d %H:%M:%S')" "$*"
}

die() {
  printf 'ERROR: %s\n' "$*" >&2
  exit 1
}

need() {
  command -v "$1" >/dev/null 2>&1 || die "missing required command: $1"
}

abs_path() {
  local p="$1"
  if [[ "$p" = /* ]]; then
    printf '%s\n' "$p"
  else
    printf '%s/%s\n' "$PWD" "$p"
  fi
}

yaml_escape() {
  printf '%s' "$1" | sed 's/\\/\\\\/g; s/"/\\"/g'
}

# Drop OS page/dentry/inode caches. Requires Linux + passwordless sudo.
# Silently skips on macOS or if sudo is unavailable.
drop_caches() {
  sync || true
  if [[ ! -f /proc/sys/vm/drop_caches ]]; then
    return 0
  fi
  if sudo -n true 2>/dev/null; then
    echo 3 | sudo -n tee /proc/sys/vm/drop_caches >/dev/null 2>&1 || \
      log "drop_caches: write failed"
  else
    log "drop_caches: passwordless sudo unavailable; skipping"
  fi
}

# Create a timestamped output directory: <base>/<YYYYMMDDTHHMMSSZ>/
# Prints the created path to stdout.
make_stamp_dir() {
  local base="$1"
  local stamp
  stamp="$(date -u +%Y%m%dT%H%M%SZ)"
  local dir="$base/$stamp"
  mkdir -p "$dir"
  printf '%s\n' "$dir"
}
