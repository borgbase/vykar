#!/usr/bin/env bash
# Long unattended fuzzing: every target in turn, growing a work corpus
# outside the repository so the committed seeds stay generator-only.
#
#   fuzz/campaign.sh [SECONDS_PER_TARGET] [TARGET...]
#
# Environment: FUZZ_WORK (default ~/fuzz-work), FUZZ_JOBS (default 6),
# FUZZ_RSS_MB (per worker, default 4096).
set -u

cd "$(dirname "$0")/.." || exit 1

per=${1:-600}
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

cargo +nightly fuzz build || exit 1

for t in "${targets[@]}"; do
  mkdir -p "$work/corpus/$t" "$work/artifacts/$t" "$work/logs"
  echo "$(date -Is) start $t (${per}s)"
  # The first corpus dir receives new inputs. -ignore_* keep a fork run
  # going after a finding, so every crash is collected.
  cargo +nightly fuzz run "$t" "$work/corpus/$t" "fuzz/corpus/$t" -- \
    -artifact_prefix="$work/artifacts/$t/" -fork="$jobs" -max_total_time="$per" \
    -rss_limit_mb="$rss" -max_len=1048576 -timeout=10 \
    -ignore_crashes=1 -ignore_timeouts=1 -ignore_ooms=1 \
    >"$work/logs/$t.log" 2>&1
  found=$(find "$work/artifacts/$t" -type f | wc -l)
  echo "$(date -Is) done $t, artifacts: $found"
done
