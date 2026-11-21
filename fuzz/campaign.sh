#!/usr/bin/env bash
# Long unattended fuzzing: every target in turn, growing a work corpus
# outside the repository so the committed seeds stay generator-only.
#
#   fuzz/campaign.sh [BASE_SECONDS] [TARGET...]
#
# Each target runs for a multiple of BASE_SECONDS (default 900), see
# budget_for. FUZZ_FLAT=1 gives every target exactly BASE_SECONDS.
#
# Environment: FUZZ_WORK (default ~/fuzz-work), FUZZ_JOBS (default 6),
# FUZZ_RSS_MB (per worker, default 4096).
set -u

# Tiers from the 2026-11 campaign (900s each), by feature growth between
# minute 5 and 15. Re-derive them from the logs when targets change.
budget_for() {
  if [ "${FUZZ_FLAT:-0}" = 1 ]; then
    echo "$per"
    return
  fi
  case "$1" in
    # Still growing (+9% features or more).
    fuzz_plaintext_read | fuzz_restore_paths | fuzz_file_cache_decode | \
      fuzz_item_stream | fuzz_msgpack_snapshot_meta | fuzz_msgpack_index_blob | \
      fuzz_metadata_msgpack | fuzz_full_index_cache)
      echo $((per * 4)) ;;
    # Coverage flat from minute 1.
    fuzz_pack_scan | fuzz_unpack_object | fuzz_protocol_keys | fuzz_index_base)
      echo $((per / 3)) ;;
    *) echo "$per" ;;
  esac
}

cd "$(dirname "$0")/.." || exit 1

per=${1:-900}
shift $(($# > 0 ? 1 : 0))
work=${FUZZ_WORK:-$HOME/fuzz-work}
jobs=${FUZZ_JOBS:-6}
rss=${FUZZ_RSS_MB:-4096}

if [ $# -gt 0 ]; then
  targets=("$@")
else
  targets=()
  for f in fuzz/fuzz_targets/fuzz_*.rs; do
    targets+=("$(basename "$f" .rs)")
  done
fi

total=0
for t in "${targets[@]}"; do
  total=$((total + $(budget_for "$t")))
done
echo "$(date -Is) ${#targets[@]} targets, $((total / 60)) min in total"

cargo +nightly fuzz build || exit 1

for t in "${targets[@]}"; do
  secs=$(budget_for "$t")
  mkdir -p "$work/corpus/$t" "$work/artifacts/$t" "$work/logs"
  echo "$(date -Is) start $t (${secs}s)"
  # The first corpus dir receives new inputs. -ignore_* keep a fork run
  # going after a finding, so every crash is collected.
  cargo +nightly fuzz run "$t" "$work/corpus/$t" "fuzz/corpus/$t" -- \
    -artifact_prefix="$work/artifacts/$t/" -fork="$jobs" -max_total_time="$secs" \
    -rss_limit_mb="$rss" -max_len=1048576 -timeout=10 \
    -ignore_crashes=1 -ignore_timeouts=1 -ignore_ooms=1 \
    >"$work/logs/$t.log" 2>&1
  found=$(find "$work/artifacts/$t" -type f | wc -l)
  echo "$(date -Is) done $t, artifacts: $found"
done
