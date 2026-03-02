#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/lib/common.sh"
source "$SCRIPT_DIR/lib/defaults.sh"
source "$SCRIPT_DIR/lib/vykar-repo.sh"

usage() {
  cat <<USAGE
Usage: $(basename "$0") [options]

Run an autonomous vykar stress test against a corpus dataset.
Defaults target the standard vykar benchmark server.

Options:
  --iterations N         Loop count (default: 1000)
  --check-every N        Run 'check' every N iterations; 0 disables (default: 50)
  --verify-data-every N  Run 'check --verify-data' every N iters; 0 disables (default: 0)
  --backend NAME         Storage backend: local|rest|s3 (default: local)
  --drop-caches          Drop OS file caches before backup and restore
  --time-v               Capture /usr/bin/time -v per vykar step into logs/*.timev
  --help                 Show help

Environment overrides (via env vars or scripts/lib/defaults.sh):
  CORPUS_LOCAL, REPO_URL, REST_URL, REST_TOKEN, VYKAR_REST_TOKEN, VYKAR_TOKEN,
  REST_DATA_DIR, ALLOW_INSECURE_HTTP,
  S3_REGION, S3_ACCESS_KEY, S3_SECRET_KEY,
  MINIO_SERVICE, MINIO_DATA_DIR, MINIO_HEALTH_URL, STRESS_ROOT
USAGE
}

# --- Parse args ---

ITERATIONS=1000
CHECK_EVERY=50
VERIFY_DATA_EVERY=0
BACKEND="local"
DROP_CACHES=0
TIME_V=0

while [[ $# -gt 0 ]]; do
  case "$1" in
    --iterations)      ITERATIONS="${2:-}"; shift 2 ;;
    --check-every)     CHECK_EVERY="${2:-}"; shift 2 ;;
    --verify-data-every) VERIFY_DATA_EVERY="${2:-}"; shift 2 ;;
    --backend)         BACKEND="${2:-}"; shift 2 ;;
    --drop-caches)     DROP_CACHES=1; shift ;;
    --time-v)          TIME_V=1; shift ;;
    --help|-h)         usage; exit 0 ;;
    *)                 die "unknown option: $1" ;;
  esac
done

[[ "$ITERATIONS" =~ ^[0-9]+$ ]] || die "--iterations must be a non-negative integer"
[[ "$CHECK_EVERY" =~ ^[0-9]+$ ]] || die "--check-every must be a non-negative integer"
[[ "$VERIFY_DATA_EVERY" =~ ^[0-9]+$ ]] || die "--verify-data-every must be a non-negative integer"
[[ "$BACKEND" =~ ^(local|rest|s3)$ ]] || die "--backend must be one of: local, rest, s3"

VYKAR_BIN="$(command -v vykar || true)"
[[ -n "$VYKAR_BIN" ]] || die "vykar binary not found on PATH"
VYKAR_BIN="$(abs_path "$VYKAR_BIN")"

CORPUS_DIR="$(abs_path "$CORPUS_LOCAL")"
[[ -d "$CORPUS_DIR" ]] || die "corpus directory not found: $CORPUS_DIR"
find "$CORPUS_DIR" -mindepth 1 -print -quit | grep -q . || die "corpus is empty: $CORPUS_DIR"

REPO_LABEL="stress-$BACKEND"

TIME_CMD="/usr/bin/time"
if [[ "$TIME_V" == "1" ]]; then
  [[ -x "$TIME_CMD" ]] || die "/usr/bin/time is required when --time-v is enabled"
fi

# Explicit HTTP opt-in for local testing (required by newer clients).
# Set ALLOW_INSECURE_HTTP=0 to strip this field from generated configs.
ALLOW_INSECURE_HTTP="${ALLOW_INSECURE_HTTP:-1}"
[[ "$ALLOW_INSECURE_HTTP" =~ ^(0|1)$ ]] || die "ALLOW_INSECURE_HTTP must be 0 or 1"

# --- Resolve repo URL ---

resolve_repo_url() {
  if [[ -n "${REPO_URL:-}" ]]; then
    REPO_URL_RESOLVED="$REPO_URL"
    return
  fi
  case "$BACKEND" in
    local) REPO_URL_RESOLVED="$REPO_DIR" ;;
    rest)  REPO_URL_RESOLVED="$REST_URL" ;;
    s3)    REPO_URL_RESOLVED="s3+http://127.0.0.1:9000/vykar-stress/$REPO_LABEL" ;;
  esac
}

# --- Work directories ---

STRESS_ROOT="${STRESS_ROOT:-$RUNTIME_ROOT/stress/$BACKEND}"
WORK_DIR="$STRESS_ROOT/work"
REPO_DIR="$WORK_DIR/repository"
RESTORE_DIR="$WORK_DIR/restore"
RUNTIME_DIR="$WORK_DIR/runtime"
CONFIG_PATH="$WORK_DIR/vykar.stress.yaml"
LOG_DIR="$WORK_DIR/logs"
HOME_DIR="$RUNTIME_DIR/home"
XDG_CACHE_DIR="$RUNTIME_DIR/xdg-cache"

# --- State tracking ---

RUN_OK=0
CURRENT_ITER=0
CURRENT_STEP="startup"
CURRENT_SNAPSHOT=""
REPO_URL_RESOLVED=""

# Log file paths for failure context
declare -A LAST_LOGS=()

# --- Helpers ---

run_vykar() {
  local iter="$1" name="$2"
  shift 2

  local log_file="$LOG_DIR/iter-$(printf '%06d' "$iter")-$name.log"
  local time_file="$LOG_DIR/iter-$(printf '%06d' "$iter")-$name.timev"
  local rc=0

  if [[ "$TIME_V" == "1" ]]; then
    HOME="$HOME_DIR" XDG_CACHE_HOME="$XDG_CACHE_DIR" \
      "$TIME_CMD" -v -o "$time_file" "$VYKAR_BIN" --config "$CONFIG_PATH" "$@" >"$log_file" 2>&1 || rc=$?
  else
    HOME="$HOME_DIR" XDG_CACHE_HOME="$XDG_CACHE_DIR" \
      "$VYKAR_BIN" --config "$CONFIG_PATH" "$@" >"$log_file" 2>&1 || rc=$?
  fi

  if [[ "$rc" -ne 0 ]]; then
    printf 'FAILED iteration=%s step=%s snapshot=%s command=%s log=%s\n' \
      "$iter" "$name" "${CURRENT_SNAPSHOT:-<none>}" "$*" "$log_file" >&2
    tail -n 120 "$log_file" >&2 || true
    return 1
  fi
  printf '%s\n' "$log_file"
}

extract_snapshot_id() {
  awk '/^Snapshot created: / { id = $3 } END { if (id != "") print id; else exit 1 }' "$1"
}

check_locks_clear() {
  local locks_dir="$REPO_DIR/locks"
  [[ -d "$locks_dir" ]] || return 0
  local count
  count="$(find "$locks_dir" -type f -name '*.json' | wc -l | tr -d ' ')"
  [[ "$count" == "0" ]] || die "stale lock file(s) detected in $locks_dir"
}

verify_restore_matches() {
  local iter="$1" restored="$2"
  local diff_file="$LOG_DIR/iter-$(printf '%06d' "$iter")-verify.diff"
  LAST_LOGS[verify_diff]="$diff_file"

  if diff -qr "$CORPUS_DIR" "$restored" >"$diff_file"; then
    return 0
  fi

  printf 'VERIFY MISMATCH iteration=%s snapshot=%s\n' "$iter" "${CURRENT_SNAPSHOT:-<none>}" >&2
  printf '  source_dir:   %s\n' "$CORPUS_DIR" >&2
  printf '  restored_dir: %s\n' "$restored" >&2
  sed -n '1,80p' "$diff_file" >&2 || true
  return 1
}

maybe_drop_caches() {
  [[ "$DROP_CACHES" == "1" ]] || return 0
  drop_caches
}

ensure_insecure_http_opt_in() {
  local config_path="$1"
  local repo_url="$2"

  [[ "$repo_url" == http://* || "$repo_url" == s3+http://* ]] || return 0

  if [[ "$ALLOW_INSECURE_HTTP" == "0" ]]; then
    local tmp_remove
    tmp_remove="$(mktemp)"
    awk '
      $0 ~ /^[[:space:]]*allow_insecure_http:[[:space:]]*true([[:space:]]*)$/ { next }
      { print }
    ' "$config_path" >"$tmp_remove"
    mv "$tmp_remove" "$config_path"
    return 0
  fi

  local tmp
  tmp="$(mktemp)"
  awk '
    $0 ~ /^[[:space:]]*allow_insecure_http:[[:space:]]*true([[:space:]]*)$/ { next }
    !done && $0 ~ /^[[:space:]]+url:[[:space:]]/ {
      print
      print "    allow_insecure_http: true"
      done = 1
      next
    }
    { print }
  ' "$config_path" >"$tmp"
  mv "$tmp" "$config_path"
  log "Enabled repository.allow_insecure_http=true for $repo_url"
}

# --- Cleanup / exit handler ---

cleanup() {
  if [[ "$BACKEND" == "local" ]]; then
    rm -rf "$REPO_DIR"
  fi
  rm -rf "$RESTORE_DIR" "$RUNTIME_DIR" "$CONFIG_PATH"
  if [[ "$TIME_V" == "1" && -d "$LOG_DIR" ]]; then
    log "Preserving logs (--time-v enabled): $LOG_DIR"
  else
    rm -rf "$LOG_DIR"
  fi
}

print_failure_context() {
  local exit_code="$1"
  printf 'Failure context:\n' >&2
  printf '  exit_code:      %s\n' "$exit_code" >&2
  printf '  iteration:      %s\n' "$CURRENT_ITER" >&2
  printf '  step:           %s\n' "$CURRENT_STEP" >&2
  printf '  snapshot:       %s\n' "${CURRENT_SNAPSHOT:-<none>}" >&2
  printf '  repository_url: %s\n' "$REPO_URL_RESOLVED" >&2
  [[ "$BACKEND" == "local" ]] && printf '  repository_dir: %s\n' "$REPO_DIR" >&2
  for key in "${!LAST_LOGS[@]}"; do
    [[ -n "${LAST_LOGS[$key]}" ]] && printf '  %-16s %s\n' "$key:" "${LAST_LOGS[$key]}" >&2
  done
}

on_exit() {
  local exit_code="$?"
  if [[ "$RUN_OK" == "1" ]]; then
    cleanup
    return
  fi
  print_failure_context "$exit_code"
  log "Run failed; preserving artifacts for debugging"
}

trap on_exit EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

# --- Main ---

main() {
  # Start from clean ephemeral state
  cleanup
  mkdir -p "$RESTORE_DIR" "$LOG_DIR" "$HOME_DIR" "$XDG_CACHE_DIR"
  [[ "$BACKEND" == "local" ]] && mkdir -p "$REPO_DIR"

  resolve_repo_url

  # Backend-specific setup
  [[ "$BACKEND" == "s3" ]] && reset_minio
  [[ "$BACKEND" == "s3" ]] && ensure_s3_bucket "$REPO_URL_RESOLVED"

  write_vykar_config "$CONFIG_PATH" "$REPO_LABEL" "$REPO_URL_RESOLVED" "$BACKEND" "$CORPUS_DIR"
  ensure_insecure_http_opt_in "$CONFIG_PATH" "$REPO_URL_RESOLVED"

  log "Stress backend=$BACKEND repo_url=$REPO_URL_RESOLVED"

  # Delete + init repo
  log "Deleting repository before init"
  CURRENT_STEP="delete-repo"
  LAST_LOGS[reset]="$LOG_DIR/iter-000000-delete-repo.log"
  vykar_repo_delete "$VYKAR_BIN" "$CONFIG_PATH" "$REPO_LABEL" "$REPO_URL_RESOLVED" \
    >"${LAST_LOGS[reset]}" 2>&1 || true

  log "Initializing repository"
  CURRENT_STEP="init"
  LAST_LOGS[init]="$(run_vykar 0 init init -R "$REPO_LABEL")"

  log "Starting stress run iterations=$ITERATIONS"

  local snapshot="" backup_log=""
  local backups=0 lists=0 restores=0 deletes=0 compacts=0 prunes=0 break_locks=0 checks=0 verify_checks=0
  local start_ts
  start_ts="$(date +%s)"

  for (( i=1; i<=ITERATIONS; i++ )); do
    CURRENT_ITER="$i"
    CURRENT_SNAPSHOT=""

    if [[ "$DROP_CACHES" == "1" ]]; then
      log "[$i/$ITERATIONS] drop caches (pre-backup)"
      CURRENT_STEP="drop-caches-pre-backup"
      maybe_drop_caches
    fi

    if [[ "$BACKEND" == "rest" || "$BACKEND" == "s3" ]]; then
      log "[$i/$ITERATIONS] break-lock"
      CURRENT_STEP="break-lock"
      LAST_LOGS[break_lock]="$(run_vykar "$i" break-lock break-lock -R "$REPO_LABEL")"
      break_locks=$((break_locks + 1))
    fi

    log "[$i/$ITERATIONS] backup"
    CURRENT_STEP="backup"
    if [[ "$BACKEND" == "rest" ]]; then
      backup_log="$(run_vykar "$i" backup backup -R "$REPO_LABEL" --upload-concurrency 6)"
    else
      backup_log="$(run_vykar "$i" backup backup -R "$REPO_LABEL")"
    fi
    LAST_LOGS[backup]="$backup_log"
    snapshot="$(extract_snapshot_id "$backup_log")" || die "failed to parse snapshot ID"
    CURRENT_SNAPSHOT="$snapshot"
    backups=$((backups + 1))
    check_locks_clear

    log "[$i/$ITERATIONS] list (snapshot $snapshot)"
    CURRENT_STEP="list"
    LAST_LOGS[list]="$(run_vykar "$i" list list -R "$REPO_LABEL" --last 20)"
    grep -q "$snapshot" "${LAST_LOGS[list]}" || die "snapshot '$snapshot' missing from list output"
    lists=$((lists + 1))

    local restore_target="$RESTORE_DIR/current"
    rm -rf "$restore_target"
    mkdir -p "$restore_target"

    if [[ "$DROP_CACHES" == "1" ]]; then
      log "[$i/$ITERATIONS] drop caches (pre-restore)"
      CURRENT_STEP="drop-caches-pre-restore"
      maybe_drop_caches
    fi

    log "[$i/$ITERATIONS] restore"
    CURRENT_STEP="restore"
    LAST_LOGS[restore]="$(run_vykar "$i" restore restore -R "$REPO_LABEL" "$snapshot" "$restore_target")"

    log "[$i/$ITERATIONS] verify"
    CURRENT_STEP="verify"
    verify_restore_matches "$i" "$restore_target" || die "restore verification failed (iteration $i)"
    restores=$((restores + 1))

    log "[$i/$ITERATIONS] delete"
    CURRENT_STEP="delete"
    LAST_LOGS[delete]="$(run_vykar "$i" delete snapshot delete "$snapshot" -R "$REPO_LABEL")"
    deletes=$((deletes + 1))
    check_locks_clear

    log "[$i/$ITERATIONS] compact"
    CURRENT_STEP="compact"
    LAST_LOGS[compact]="$(run_vykar "$i" compact compact -R "$REPO_LABEL" --threshold 0)"
    compacts=$((compacts + 1))
    check_locks_clear

    log "[$i/$ITERATIONS] prune"
    CURRENT_STEP="prune"
    LAST_LOGS[prune]="$(run_vykar "$i" prune prune -R "$REPO_LABEL")"
    prunes=$((prunes + 1))
    check_locks_clear

    if (( CHECK_EVERY > 0 && i % CHECK_EVERY == 0 )); then
      log "[$i/$ITERATIONS] check"
      CURRENT_STEP="check"
      LAST_LOGS[check]="$(run_vykar "$i" check check -R "$REPO_LABEL")"
      checks=$((checks + 1))
    fi

    if (( VERIFY_DATA_EVERY > 0 && i % VERIFY_DATA_EVERY == 0 )); then
      log "[$i/$ITERATIONS] check --verify-data"
      CURRENT_STEP="check-verify-data"
      LAST_LOGS[check_verify]="$(run_vykar "$i" check-data check -R "$REPO_LABEL" --verify-data)"
      verify_checks=$((verify_checks + 1))
    fi

    log "[$i/$ITERATIONS] done ($(( $(date +%s) - start_ts ))s elapsed)"
  done

  local elapsed=$(( $(date +%s) - start_ts ))

  log "Stress run complete"
  printf 'Summary:\n'
  printf '  iterations:          %s\n' "$ITERATIONS"
  printf '  backups:             %s\n' "$backups"
  printf '  lists:               %s\n' "$lists"
  printf '  restores:            %s\n' "$restores"
  printf '  deletes:             %s\n' "$deletes"
  printf '  compacts:            %s\n' "$compacts"
  printf '  prunes:              %s\n' "$prunes"
  printf '  break-lock:          %s\n' "$break_locks"
  printf '  check:               %s\n' "$checks"
  printf '  check --verify-data: %s\n' "$verify_checks"
  printf '  elapsed_sec:         %s\n' "$elapsed"

  CURRENT_STEP="complete"
  RUN_OK=1
}

main
