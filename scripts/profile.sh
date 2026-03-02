#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
source "$SCRIPT_DIR/lib/common.sh"
source "$SCRIPT_DIR/lib/defaults.sh"
source "$SCRIPT_DIR/lib/vykar-repo.sh"

usage() {
  cat <<USAGE
Usage: $(basename "$0") [options]

Build vykar with the profiling profile, prepare repo state, and run
heaptrack and/or perf profiling on the selected command.

Options:
  --mode MODE              backup|restore|compact|prune|check (required)
  --backend NAME           Storage backend: local|rest|s3 (default: local)
  --source PATH            Source path to back up (default: \$CORPUS_LOCAL)
  --profiler NAME          heaptrack|perf|both (default: heaptrack)
  --skip-build             Skip cargo build, use existing target/profiling/vykar
  --no-drop-caches         Do not drop caches before profiling run
  --dry-run                Enable dry-run for compact/prune
  --verify-data            Include --verify-data for check
  --compact-threshold N    Compact threshold percentage (default: 10)
  --help                   Show help

Environment overrides: CORPUS_LOCAL, RUNTIME_ROOT, VYKAR_CONFIG, REST_URL,
REST_TOKEN, VYKAR_REST_TOKEN, VYKAR_TOKEN, PERF_EVENTS
If VYKAR_CONFIG is set, it overrides --backend and uses the given config as-is.
USAGE
}

# --- Parse args ---

MODE=""
BACKEND="local"
SOURCE_PATH="$CORPUS_LOCAL"
PROFILER="heaptrack"
SKIP_BUILD=0
NO_DROP_CACHES=0
DRY_RUN=0
VERIFY_DATA=0
COMPACT_THRESHOLD="10"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --mode)              MODE="${2:-}"; shift 2 ;;
    --backend)           BACKEND="${2:-}"; shift 2 ;;
    --source)            SOURCE_PATH="${2:-}"; shift 2 ;;
    --profiler)          PROFILER="${2:-}"; shift 2 ;;
    --skip-build)        SKIP_BUILD=1; shift ;;
    --no-drop-caches)    NO_DROP_CACHES=1; shift ;;
    --dry-run)           DRY_RUN=1; shift ;;
    --verify-data)       VERIFY_DATA=1; shift ;;
    --compact-threshold) COMPACT_THRESHOLD="${2:-}"; shift 2 ;;
    --help|-h)           usage; exit 0 ;;
    *)                   die "unknown option: $1" ;;
  esac
done

case "$MODE" in
  backup|restore|compact|prune|check) ;;
  *) die "invalid or missing --mode: $MODE (expected: backup|restore|compact|prune|check)" ;;
esac

case "$PROFILER" in
  heaptrack|perf|both) ;;
  *) die "invalid --profiler: $PROFILER (expected: heaptrack|perf|both)" ;;
esac

[[ "$BACKEND" =~ ^(local|rest|s3)$ ]] || die "invalid --backend: $BACKEND (expected: local|rest|s3)"

[[ -d "$SOURCE_PATH" ]] || die "source not found: $SOURCE_PATH"

SEED_SOURCE_PATH="$SOURCE_PATH/snapshot-1"
if [[ "$MODE" == "backup" || "$MODE" == "compact" ]]; then
  [[ -d "$SEED_SOURCE_PATH" ]] || die "seed source not found: $SEED_SOURCE_PATH"
fi

# --- Config: explicit override or auto-generated from --backend ---

CONFIG_GENERATED=0
if [[ -n "${VYKAR_CONFIG:-}" ]]; then
  CONFIG_PATH="$VYKAR_CONFIG"
  [[ -f "$CONFIG_PATH" ]] || die "config not found: $CONFIG_PATH"
fi

# --- Tool checks ---

need cargo
need perl
need git
if [[ "$PROFILER" == "heaptrack" || "$PROFILER" == "both" ]]; then
  need heaptrack
  need heaptrack_print
fi
if [[ "$PROFILER" == "perf" || "$PROFILER" == "both" ]]; then
  need perf
fi

# --- Constants ---

REPO_LABEL="profile-$BACKEND"
SNAPSHOT_LABEL="corpus-profile"
PERF_EVENTS="${PERF_EVENTS:-}"
PERF_RECORD_FREQ=99
COST_TYPE="peak"

# --- Output layout ---

OUT_ROOT="$RUNTIME_ROOT/heaptrack/reports"
STAMP="$(date -u +%Y%m%dT%H%M%SZ)"
RUN_ROOT="$OUT_ROOT/$STAMP"
RUN_DIR="$RUN_ROOT/$MODE"
mkdir -p "$RUN_DIR"

HEAPTRACK_DATA="$RUN_DIR/heaptrack.vykar.$STAMP.%p.gz"
ANALYSIS_TXT="$RUN_DIR/heaptrack.analysis.txt"
STACKS_TXT="$RUN_DIR/heaptrack.stacks.txt"
FLAMEGRAPH_SVG="$RUN_DIR/heaptrack.flamegraph.svg"
PERF_STAT_TXT="$RUN_DIR/perf.stat.txt"
PERF_STDOUT_TXT="$RUN_DIR/perf.stdout.txt"
PERF_LOG="$RUN_DIR/perf.log"
PERF_DATA="$RUN_DIR/perf.data"
PERF_RECORD_TXT="$RUN_DIR/perf.record.txt"
PERF_REPORT_TXT="$RUN_DIR/perf.report.txt"
PROFILE_LOG="$RUN_DIR/profile.log"
SETUP_LOG="$RUN_DIR/setup.log"
DROP_CACHES_LOG="$RUN_DIR/drop-caches.log"
META_TXT="$RUN_DIR/meta.txt"
HEAPTRACK_FILE=""

VYKAR_BIN="$REPO_ROOT/target/profiling/vykar"
PROFILE_CMD=()
SETUP_STEPS=()
CLEANUP_DIRS=()

add_setup_step() { SETUP_STEPS+=("$1"); }
register_cleanup_dir() { [[ -n "$1" ]] && CLEANUP_DIRS+=("$1"); }

cleanup_restored_data() {
  for d in "${CLEANUP_DIRS[@]}"; do
    [[ -n "$d" ]] && rm -rf "$d" || true
  done
}
trap cleanup_restored_data EXIT

# --- Generate config if not explicitly provided ---

if [[ -z "${VYKAR_CONFIG:-}" ]]; then
  CONFIG_GENERATED=1
  CONFIG_PATH="$RUN_DIR/vykar.profile.yaml"

  WORK_DIR="$RUN_DIR/work"
  mkdir -p "$WORK_DIR"

  case "$BACKEND" in
    local)
      PROFILE_REPO_DIR="$WORK_DIR/repository"
      mkdir -p "$PROFILE_REPO_DIR"
      REPO_URL="$PROFILE_REPO_DIR"
      register_cleanup_dir "$PROFILE_REPO_DIR"
      ;;
    rest)
      REPO_URL="$REST_URL"
      ;;
    s3)
      REPO_URL="s3://127.0.0.1:9000/vykar-profile/$REPO_LABEL"
      reset_minio
      ensure_s3_bucket "$REPO_URL"
      ;;
  esac

  write_vykar_config "$CONFIG_PATH" "$REPO_LABEL" "$REPO_URL" "$BACKEND" "$SOURCE_PATH"
  export VYKAR_PASSPHRASE="$PASSPHRASE"
fi

maybe_drop_caches() {
  if (( NO_DROP_CACHES == 1 )); then
    echo "skipped: drop_caches (--no-drop-caches)" >"$DROP_CACHES_LOG"
    return 0
  fi
  drop_caches 2>&1 | tee "$DROP_CACHES_LOG"
}

# --- Build ---

if (( SKIP_BUILD == 0 )); then
  echo "[1/6] Building vykar-cli with profiling profile..."
  (cd "$REPO_ROOT" && cargo build -p vykar-cli --profile profiling)
else
  echo "[1/6] Skipping build (--skip-build)"
fi
[[ -x "$VYKAR_BIN" ]] || die "built binary not found: $VYKAR_BIN"

# --- Setup ---

echo "[2/6] Running setup for mode: $MODE"
: >"$SETUP_LOG"

run_setup_reset_and_init() {
  add_setup_step "delete+init repo ($REPO_LABEL, backend=$BACKEND)"
  {
    vykar_repo_delete "$VYKAR_BIN" "$CONFIG_PATH" "$REPO_LABEL" "${REPO_URL:-}"
    vykar_repo_init "$VYKAR_BIN" "$CONFIG_PATH" "$REPO_LABEL"
  } 2>&1 | tee -a "$SETUP_LOG"
}

run_setup_backup() {
  local label="$1" src="$2"
  add_setup_step "backup ($src, label=$label)"
  echo "[setup] Backup: $src (label: $label)" | tee -a "$SETUP_LOG"
  "$VYKAR_BIN" --config "$CONFIG_PATH" backup -R "$REPO_LABEL" -l "$label" "$src" 2>&1 | tee -a "$SETUP_LOG"
}

case "$MODE" in
  backup)
    run_setup_reset_and_init
    run_setup_backup "$SNAPSHOT_LABEL" "$SEED_SOURCE_PATH"
    PROFILE_CMD=( "$VYKAR_BIN" --config "$CONFIG_PATH" backup -R "$REPO_LABEL" -l "$SNAPSHOT_LABEL" "$SOURCE_PATH" )
    ;;
  restore)
    run_setup_reset_and_init
    run_setup_backup "$SNAPSHOT_LABEL" "$SOURCE_PATH"
    RESTORE_DEST="$RUN_DIR/restore-target"
    rm -rf "$RESTORE_DEST"
    mkdir -p "$RESTORE_DEST"
    register_cleanup_dir "$RESTORE_DEST"
    add_setup_step "prepare restore destination ($RESTORE_DEST)"
    PROFILE_CMD=( "$VYKAR_BIN" --config "$CONFIG_PATH" restore -R "$REPO_LABEL" latest "$RESTORE_DEST" )
    ;;
  compact)
    SEED_LABEL="${SNAPSHOT_LABEL}-seed"
    run_setup_reset_and_init
    run_setup_backup "$SEED_LABEL" "$SEED_SOURCE_PATH"
    run_setup_backup "$SNAPSHOT_LABEL" "$SOURCE_PATH"
    # Delete seed snapshot to create reclaimable data
    SEED_SNAPSHOT="$("$VYKAR_BIN" --config "$CONFIG_PATH" list -R "$REPO_LABEL" -S "$SEED_LABEL" --last 1 | awk 'NR==2{print $1}')"
    [[ -n "$SEED_SNAPSHOT" ]] || die "could not resolve seed snapshot for compact setup"
    echo "[setup] Deleting seed snapshot: $SEED_SNAPSHOT" | tee -a "$SETUP_LOG"
    add_setup_step "delete seed snapshot ($SEED_SNAPSHOT)"
    "$VYKAR_BIN" --config "$CONFIG_PATH" snapshot delete "$SEED_SNAPSHOT" -R "$REPO_LABEL" 2>&1 | tee -a "$SETUP_LOG"
    PROFILE_CMD=( "$VYKAR_BIN" --config "$CONFIG_PATH" compact -R "$REPO_LABEL" --threshold "$COMPACT_THRESHOLD" )
    (( DRY_RUN == 1 )) && PROFILE_CMD+=( -n )
    ;;
  prune)
    run_setup_reset_and_init
    run_setup_backup "$SNAPSHOT_LABEL" "$SOURCE_PATH"
    PROFILE_CMD=( "$VYKAR_BIN" --config "$CONFIG_PATH" prune -R "$REPO_LABEL" )
    (( DRY_RUN == 1 )) && PROFILE_CMD+=( -n )
    ;;
  check)
    run_setup_reset_and_init
    run_setup_backup "$SNAPSHOT_LABEL" "$SOURCE_PATH"
    PROFILE_CMD=( "$VYKAR_BIN" --config "$CONFIG_PATH" check -R "$REPO_LABEL" )
    (( VERIFY_DATA == 1 )) && PROFILE_CMD+=( --verify-data )
    ;;
esac

# --- Drop caches ---

echo "[3/6] Dropping caches before measured command..."
add_setup_step "drop_caches before profile"
maybe_drop_caches

# --- Profiling ---

resolve_heaptrack_data_file() {
  HEAPTRACK_FILE="$(ls -1t "$RUN_DIR"/heaptrack.vykar."$STAMP".*.gz.zst "$RUN_DIR"/heaptrack.vykar."$STAMP".*.gz 2>/dev/null | head -n 1 || true)"
  [[ -n "$HEAPTRACK_FILE" ]] || die "could not find heaptrack output in: $RUN_DIR"
}

run_heaptrack_profile() {
  echo "[4/6] Running heaptrack for mode: $MODE"
  if [[ "$MODE" == "restore" && -n "${RESTORE_DEST:-}" ]]; then
    rm -rf "$RESTORE_DEST"
    mkdir -p "$RESTORE_DEST"
  fi
  set -o pipefail
  heaptrack -o "$HEAPTRACK_DATA" "${PROFILE_CMD[@]}" 2>&1 | tee "$PROFILE_LOG"
  set +o pipefail
}

run_perf_profile() {
  echo "[4/6] Running perf stat for mode: $MODE"
  if [[ "$MODE" == "restore" && -n "${RESTORE_DEST:-}" ]]; then
    rm -rf "$RESTORE_DEST"
    mkdir -p "$RESTORE_DEST"
  fi
  : >"$PERF_LOG"
  if [[ -n "$PERF_EVENTS" ]]; then
    perf stat -d -r 1 -e "$PERF_EVENTS" -- "${PROFILE_CMD[@]}" >"$PERF_STDOUT_TXT" 2>"$PERF_STAT_TXT"
  else
    perf stat -d -r 1 -- "${PROFILE_CMD[@]}" >"$PERF_STDOUT_TXT" 2>"$PERF_STAT_TXT"
  fi
  echo "[4/6] perf stat complete -> $PERF_STAT_TXT" | tee -a "$PERF_LOG"

  # Restore runs twice in perf mode (stat + record). Ensure a clean target for the second run.
  if [[ "$MODE" == "restore" && -n "${RESTORE_DEST:-}" ]]; then
    rm -rf "$RESTORE_DEST"
    mkdir -p "$RESTORE_DEST"
  fi

  echo "[4/6] Running perf record for mode: $MODE" | tee -a "$PERF_LOG"
  perf record -F "$PERF_RECORD_FREQ" -g --output "$PERF_DATA" -- "${PROFILE_CMD[@]}" >"$PERF_RECORD_TXT" 2>&1
  echo "[4/6] perf record complete -> $PERF_DATA" | tee -a "$PERF_LOG"

  echo "[5/6] Generating perf report..." | tee -a "$PERF_LOG"
  perf report --stdio --input "$PERF_DATA" >"$PERF_REPORT_TXT" 2>&1
}

render_heaptrack_reports() {
  echo "[5/6] Generating text analysis and stacks..."
  heaptrack_print -f "$HEAPTRACK_FILE" >"$ANALYSIS_TXT"
  heaptrack_print -f "$HEAPTRACK_FILE" --flamegraph-cost-type "$COST_TYPE" -F "$STACKS_TXT" >/dev/null

  FLAMEGRAPH_PL="$(command -v flamegraph.pl || true)"
  if [[ -z "$FLAMEGRAPH_PL" ]]; then
    FG_DIR="/tmp/FlameGraph"
    if [[ ! -x "$FG_DIR/flamegraph.pl" ]]; then
      rm -rf "$FG_DIR"
      git clone --depth 1 https://github.com/brendangregg/FlameGraph.git "$FG_DIR" >/dev/null 2>&1
    fi
    FLAMEGRAPH_PL="$FG_DIR/flamegraph.pl"
  fi

  echo "[6/6] Rendering flamegraph SVG..."
  perl "$FLAMEGRAPH_PL" \
    --title "heaptrack: $COST_TYPE (vykar-cli profiling, mode=$MODE)" \
    --colors mem \
    --countname "$COST_TYPE" \
    <"$STACKS_TXT" >"$FLAMEGRAPH_SVG"
}

if [[ "$PROFILER" == "heaptrack" ]]; then
  run_heaptrack_profile
  resolve_heaptrack_data_file
  render_heaptrack_reports
elif [[ "$PROFILER" == "perf" ]]; then
  run_perf_profile
else
  run_heaptrack_profile
  resolve_heaptrack_data_file
  render_heaptrack_reports
  if (( NO_DROP_CACHES == 0 )); then
    echo "[7/7] Dropping caches before perf run..."
    add_setup_step "drop_caches before perf profile"
    maybe_drop_caches
  fi
  run_perf_profile
fi

# --- Meta ---

{
  echo "mode=$MODE"
  echo "backend=$BACKEND"
  echo "timestamp_utc=$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  echo "vykar_bin=$VYKAR_BIN"
  echo "config=$CONFIG_PATH"
  echo "config_generated=$CONFIG_GENERATED"
  echo "repo=$REPO_LABEL"
  echo "source=$SOURCE_PATH"
  echo "profiler=$PROFILER"
  echo "skip_build=$SKIP_BUILD"
  echo "drop_caches=$((1 - NO_DROP_CACHES))"
  printf "profile_cmd="
  printf "%q " "${PROFILE_CMD[@]}"
  echo
  echo "setup_steps<<EOF"
  [[ "${#SETUP_STEPS[@]}" -gt 0 ]] && printf '%s\n' "${SETUP_STEPS[@]}"
  echo "EOF"
  [[ -n "$HEAPTRACK_FILE" ]] && echo "heaptrack_data=$HEAPTRACK_FILE"
  if [[ "$PROFILER" == "perf" || "$PROFILER" == "both" ]]; then
    echo "perf_stat_txt=$PERF_STAT_TXT"
    echo "perf_data=$PERF_DATA"
  fi
} >"$META_TXT"

# --- Summary ---

echo
echo "Run complete. Outputs:"
echo "  mode:      $MODE"
echo "  backend:   $BACKEND"
echo "  profiler:  $PROFILER"
echo "  run_dir:   $RUN_DIR"
[[ -n "$HEAPTRACK_FILE" ]] && echo "  flamegraph: $FLAMEGRAPH_SVG"
if [[ "$PROFILER" == "perf" || "$PROFILER" == "both" ]]; then
  echo "  perf_stat: $PERF_STAT_TXT"
  echo "  perf_data: $PERF_DATA"
fi
echo "  meta:      $META_TXT"
