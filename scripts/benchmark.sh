#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/lib/common.sh"
source "$SCRIPT_DIR/lib/defaults.sh"

usage() {
  cat <<USAGE
Usage: $(basename "$0") [options]

Reproducible benchmark harness for vykar vs restic vs rustic vs borg vs kopia.

Per measured run: reset repo, init, untimed seed backup of snapshot-1, storage settle
(sync + fstrim + nvme flush + cooldown), drop caches, then timed benchmark.

Options:
  --dataset PATH   Dataset directory (default: \$CORPUS_REMOTE)
  --tool NAMES     Comma-separated tools: vykar,restic,rustic,borg,kopia (default: all)
  --runs N         Timed runs per operation (default: 3)
  --warmups N      Warmup runs per operation (default: 0)
  --perf           Run perf stat summary after timed runs
  --strace         Run strace -c summary after timed runs
  --help           Show help

Environment overrides: CORPUS_REMOTE, REPO_ROOT, PASSPHRASE
USAGE
}

# --- Parse args ---

DATASET_DIR="$CORPUS_REMOTE"
TOOL=""
SELECTED_TOOL_LABEL="all"
RUNS=3
WARMUP_RUNS=0
PROFILE_PERF=0
PROFILE_STRACE=0

while [[ $# -gt 0 ]]; do
  case "$1" in
    --dataset)  DATASET_DIR="${2:-}"; shift 2 ;;
    --tool)     TOOL="${2:-}"; shift 2 ;;
    --runs)     RUNS="${2:-}"; shift 2 ;;
    --warmups)  WARMUP_RUNS="${2:-}"; shift 2 ;;
    --perf)     PROFILE_PERF=1; shift ;;
    --strace)   PROFILE_STRACE=1; shift ;;
    --help|-h)  usage; exit 0 ;;
    *)          die "unknown option: $1" ;;
  esac
done

DATASET_SNAPSHOT1="$DATASET_DIR/snapshot-1"
DATASET_SNAPSHOT2="$DATASET_DIR/snapshot-2"
DATASET_BENCHMARK="$DATASET_DIR"

[[ -d "$DATASET_DIR" ]] || die "dataset dir not found: $DATASET_DIR"
[[ -d "$DATASET_SNAPSHOT1" ]] || die "missing required seed folder: $DATASET_SNAPSHOT1"
[[ -d "$DATASET_SNAPSHOT2" ]] || die "missing required benchmark folder: $DATASET_SNAPSHOT2"
[[ "$RUNS" =~ ^[1-9][0-9]*$ ]] || die "--runs must be a positive integer"
[[ "$WARMUP_RUNS" =~ ^[0-9]+$ ]] || die "--warmups must be a non-negative integer"

need /usr/bin/time
command -v perf >/dev/null 2>&1 && HAVE_PERF=1 || HAVE_PERF=0
command -v strace >/dev/null 2>&1 && HAVE_STRACE=1 || HAVE_STRACE=0

ALL_TOOLS=(vykar restic rustic borg kopia)
SELECTED_TOOLS=()

if [[ -z "$TOOL" ]]; then
  SELECTED_TOOLS=("${ALL_TOOLS[@]}")
else
  declare -A SEEN_TOOLS=()
  IFS=',' read -r -a tool_items <<<"$TOOL"
  for raw_tool in "${tool_items[@]}"; do
    tool_item="${raw_tool//[[:space:]]/}"
    [[ -n "$tool_item" ]] || die "--tool contains an empty item: '$TOOL'"
    case "$tool_item" in
      vykar|restic|rustic|borg|kopia) ;;
      *) die "--tool item must be one of: vykar, restic, rustic, borg, kopia; got: $tool_item" ;;
    esac
    if [[ -z "${SEEN_TOOLS[$tool_item]:-}" ]]; then
      SELECTED_TOOLS+=("$tool_item")
      SEEN_TOOLS["$tool_item"]=1
    fi
  done
fi

[[ "${#SELECTED_TOOLS[@]}" -gt 0 ]] || die "no tools selected"
for t in "${SELECTED_TOOLS[@]}"; do
  need "$t"
done

if [[ -n "$TOOL" ]]; then
  SELECTED_TOOL_LABEL="$(IFS=,; echo "${SELECTED_TOOLS[*]}")"
fi

# --- Output layout ---

STAMP=$(date -u +%Y%m%dT%H%M%SZ)
OUT_ROOT="$RUNTIME_ROOT/benchmarks/$STAMP"
LOGS="$OUT_ROOT/logs"
mkdir -p "$LOGS"

# --- Repos ---

VYKAR_REPO="$REPO_ROOT/bench-vykar"
RESTIC_REPO="$REPO_ROOT/bench-restic"
RUSTIC_REPO="$REPO_ROOT/bench-rustic"
BORG_REPO="$REPO_ROOT/bench-borg"
KOPIA_REPO="$REPO_ROOT/bench-kopia"
KOPIA_CONFIG="$OUT_ROOT/kopia.repository.config"
KOPIA_CACHE="$OUT_ROOT/kopia-cache"

sudo -n mkdir -p "$VYKAR_REPO" "$RESTIC_REPO" "$RUSTIC_REPO" "$BORG_REPO" "$KOPIA_REPO"
sudo -n chown -R "$USER:$USER" "$VYKAR_REPO" "$RESTIC_REPO" "$RUSTIC_REPO" "$BORG_REPO" "$KOPIA_REPO"

# --- Tool config ---

VYKAR_CFG="$OUT_ROOT/vykar.bench.yaml"
cat >"$VYKAR_CFG" <<YAML
repositories:
  - url: "$VYKAR_REPO"
    label: bench
compression:
  algorithm: zstd
YAML
chmod 600 "$VYKAR_CFG"

export VYKAR_CONFIG="$VYKAR_CFG"
export VYKAR_PASSPHRASE="$PASSPHRASE"
export RESTIC_REPOSITORY="$RESTIC_REPO"
export RESTIC_PASSWORD="$PASSPHRASE"
export RUSTIC_REPOSITORY="$RUSTIC_REPO"
export RUSTIC_PASSWORD="$PASSPHRASE"
export BORG_REPO="$BORG_REPO"
export BORG_PASSPHRASE="$PASSPHRASE"
export KOPIA_PASSWORD="$PASSPHRASE"

# --- Restore dirs ---

RESTORE_VYKAR="$OUT_ROOT/restore-vykar"
RESTORE_RESTIC="$OUT_ROOT/restore-restic"
RESTORE_RUSTIC="$OUT_ROOT/restore-rustic"
RESTORE_BORG="$OUT_ROOT/restore-borg"
RESTORE_KOPIA="$OUT_ROOT/restore-kopia"
mkdir -p "$RESTORE_VYKAR" "$RESTORE_RESTIC" "$RESTORE_RUSTIC" "$RESTORE_BORG" "$RESTORE_KOPIA"

cleanup_restore_dirs() {
  rm -rf "$RESTORE_VYKAR" "$RESTORE_RESTIC" "$RESTORE_RUSTIC" "$RESTORE_BORG" "$RESTORE_KOPIA"
  [[ -n "${KOPIA_CACHE:-}" ]] && rm -rf "$KOPIA_CACHE"
}
trap cleanup_restore_dirs EXIT

# --- Operation list ---

OPS=()
for selected_tool in "${SELECTED_TOOLS[@]}"; do
  OPS+=("${selected_tool}_backup" "${selected_tool}_restore")
done

# --- Tool helpers ---

tool_from_op()  { echo "${1%%_*}"; }
phase_from_op() { echo "${1##*_}"; }

# Per-run resolved values used by measured commands.
RUN_VYKAR_RESTORE_SNAPSHOT=""
RUN_BORG_BACKUP_ARCHIVE=""
RUN_BORG_RESTORE_ARCHIVE=""
declare -a STORAGE_TRIM_MOUNTS=()
declare -a STORAGE_NVME_DEVICES=()

run_with_op_env() {
  local op="$1"
  shift
  case "$op" in
    vykar_restore) VYKAR_RESTORE_SNAPSHOT="$RUN_VYKAR_RESTORE_SNAPSHOT" "$@" ;;
    borg_backup)  BORG_BACKUP_ARCHIVE="$RUN_BORG_BACKUP_ARCHIVE" "$@" ;;
    borg_restore) BORG_RESTORE_ARCHIVE="$RUN_BORG_RESTORE_ARCHIVE" "$@" ;;
    *)            "$@" ;;
  esac
}

append_unique_item() {
  local item="$1"
  shift
  local -n arr_ref="$1"
  local existing
  for existing in "${arr_ref[@]}"; do
    [[ "$existing" == "$item" ]] && return 0
  done
  arr_ref+=("$item")
}

mount_target_for_path() {
  local path="$1"
  if command -v findmnt >/dev/null 2>&1; then
    findmnt -n -o TARGET --target "$path" 2>/dev/null | head -n1
  else
    df -P "$path" 2>/dev/null | awk 'NR==2 { print $6 }'
  fi
}

mount_source_for_path() {
  local path="$1"
  if command -v findmnt >/dev/null 2>&1; then
    findmnt -n -o SOURCE --target "$path" 2>/dev/null | head -n1
  else
    df -P "$path" 2>/dev/null | awk 'NR==2 { print $1 }'
  fi
}

nvme_namespace_for_source() {
  local source="$1"
  case "$source" in
    /dev/nvme*n*p[0-9]*)
      printf '%s\n' "${source%p[0-9]*}"
      ;;
    /dev/nvme*n[0-9]*)
      printf '%s\n' "$source"
      ;;
    *)
      return 1
      ;;
  esac
}

build_storage_settle_targets() {
  local path mount source nvme_dev
  local -a paths=("$REPO_ROOT" "$OUT_ROOT")

  for path in "${paths[@]}"; do
    [[ -n "$path" ]] || continue
    mount="$(mount_target_for_path "$path")"
    [[ -n "$mount" ]] && append_unique_item "$mount" STORAGE_TRIM_MOUNTS
    source="$(mount_source_for_path "$path")"
    if nvme_dev="$(nvme_namespace_for_source "$source" 2>/dev/null)"; then
      append_unique_item "$nvme_dev" STORAGE_NVME_DEVICES
    fi
  done
}

run_storage_settle() {
  local prep_log="$1"
  local mount dev

  echo "[measure-prepare] sync storage" >>"$prep_log"
  sync >>"$prep_log" 2>&1 || true

  if [[ "${#STORAGE_TRIM_MOUNTS[@]}" -gt 0 ]]; then
    for mount in "${STORAGE_TRIM_MOUNTS[@]}"; do
      if ! sudo -n fstrim -v "$mount" >>"$prep_log" 2>&1; then
        echo "[measure-prepare] fstrim skipped/failed mount=$mount" >>"$prep_log"
      fi
    done
  else
    echo "[measure-prepare] fstrim skipped (no mount targets resolved)" >>"$prep_log"
  fi

  if command -v nvme >/dev/null 2>&1; then
    if [[ "${#STORAGE_NVME_DEVICES[@]}" -gt 0 ]]; then
      for dev in "${STORAGE_NVME_DEVICES[@]}"; do
        if ! sudo -n nvme flush "$dev" >>"$prep_log" 2>&1; then
          echo "[measure-prepare] nvme flush skipped/failed device=$dev" >>"$prep_log"
        fi
      done
    else
      echo "[measure-prepare] nvme flush skipped (no nvme devices resolved)" >>"$prep_log"
    fi
  else
    echo "[measure-prepare] nvme cli missing; skipping nvme flush" >>"$prep_log"
  fi

  echo "[measure-prepare] cooldown sleep 20s" >>"$prep_log"
  sleep 20
}

build_storage_settle_targets

repo_dir_for_tool() {
  case "$1" in
    vykar)   echo "$VYKAR_REPO" ;;
    restic) echo "$RESTIC_REPO" ;;
    rustic) echo "$RUSTIC_REPO" ;;
    borg)   echo "$BORG_REPO" ;;
    kopia)  echo "$KOPIA_REPO" ;;
    *) die "unknown tool: $1" ;;
  esac
}

restore_dir_for_tool() {
  case "$1" in
    vykar)   echo "$RESTORE_VYKAR" ;;
    restic) echo "$RESTORE_RESTIC" ;;
    rustic) echo "$RESTORE_RUSTIC" ;;
    borg)   echo "$RESTORE_BORG" ;;
    kopia)  echo "$RESTORE_KOPIA" ;;
    *) die "unknown tool: $1" ;;
  esac
}

clear_dir_contents() {
  local dir="$1" log_file="${2:-/dev/null}"
  mkdir -p "$dir"
  find "$dir" -mindepth 1 -maxdepth 1 -exec rm -rf -- {} + >>"$log_file" 2>&1
}

write_repo_size_bytes_for_tool() {
  local tool="$1" out_file="$2"
  local repo bytes
  repo="$(repo_dir_for_tool "$tool")"
  bytes="NA"
  if [[ -d "$repo" ]]; then
    bytes="$(du -sb "$repo" 2>/dev/null | awk 'NR==1 { print $1 }')"
    [[ "$bytes" =~ ^[0-9]+$ ]] || bytes="NA"
  fi
  printf '%s\n' "$bytes" >"$out_file"
}

reset_repo_for_tool() {
  local tool="$1" repo=""
  repo="$(repo_dir_for_tool "$tool")"
  sudo -n rm -rf "$repo"
  sudo -n mkdir -p "$repo"
  sudo -n chown -R "$USER:$USER" "$repo"
  if [[ "$tool" == "kopia" ]]; then
    rm -f "$KOPIA_CONFIG"
    rm -rf "$KOPIA_CACHE"
    mkdir -p "$KOPIA_CACHE"
  fi
}

cleanup_repo_for_tool() {
  local tool="$1" repo=""
  repo="$(repo_dir_for_tool "$tool")"
  # Remove repository contents to free disk between tools, then recreate empty dir for final stats.
  sudo -n rm -rf "$repo"
  sudo -n mkdir -p "$repo"
  sudo -n chown -R "$USER:$USER" "$repo"
}

cleanup_restore_for_tool() {
  local tool="$1" restore_dir=""
  restore_dir="$(restore_dir_for_tool "$tool")"
  rm -rf "$restore_dir"
  mkdir -p "$restore_dir"
}

init_repo_for_tool() {
  case "$1" in
    vykar)   vykar init -R bench ;;
    restic) restic init ;;
    rustic) rustic init ;;
    borg)   borg init --encryption=repokey-blake2 ;;
    kopia)
      kopia --config-file "$KOPIA_CONFIG" repository create filesystem --path="$KOPIA_REPO" --cache-directory="$KOPIA_CACHE"
      kopia --config-file "$KOPIA_CONFIG" policy set --global --compression=zstd
      ;;
    *) die "unknown tool: $1" ;;
  esac
}

backup_adhoc_for_tool() {
  local tool="$1" src="$2"
  case "$tool" in
    vykar)   vykar backup -R bench -l bench "$src" ;;
    restic) restic backup "$src" ;;
    rustic) rustic backup "$src" ;;
    borg)
      local arch="bench-$(date -u +%Y%m%dT%H%M%S)-$RANDOM"
      borg create --compression zstd,3 --stats "::$arch" "$src"
      ;;
    kopia) kopia --config-file "$KOPIA_CONFIG" snapshot create "$src" ;;
    *) die "unknown tool: $tool" ;;
  esac
}

measured_cmd_for_op() {
  case "$1" in
    vykar_backup)    echo "vykar backup -R bench -l bench '$DATASET_BENCHMARK'" ;;
    vykar_restore)   echo "vykar restore -R bench \"\$VYKAR_RESTORE_SNAPSHOT\" '$RESTORE_VYKAR'" ;;
    restic_backup)  echo "restic backup '$DATASET_BENCHMARK'" ;;
    restic_restore) echo "restic restore latest --target '$RESTORE_RESTIC'" ;;
    rustic_backup)  echo "rustic backup '$DATASET_BENCHMARK'" ;;
    rustic_restore) echo "rustic restore latest '$RESTORE_RUSTIC'" ;;
    borg_backup)    echo "borg create --compression zstd,3 --stats \"::\$BORG_BACKUP_ARCHIVE\" '$DATASET_BENCHMARK'" ;;
    borg_restore)   echo "(cd '$RESTORE_BORG' && borg extract \"::\$BORG_RESTORE_ARCHIVE\")" ;;
    kopia_backup)   echo "kopia --config-file '$KOPIA_CONFIG' snapshot create '$DATASET_BENCHMARK'" ;;
    kopia_restore)  echo "kopia --config-file '$KOPIA_CONFIG' snapshot restore '$DATASET_BENCHMARK' '$RESTORE_KOPIA' --snapshot-time latest" ;;
    *) die "unknown operation: $1" ;;
  esac
}

should_prepare_op_run() {
  [[ "$(phase_from_op "$1")" == "backup" ]]
}

prepare_op_run() {
  local op="$1" prep_log="$2"
  local tool
  tool=$(tool_from_op "$op")

  echo "[prepare] op=$op" >>"$prep_log"
  echo "[prepare] reset repo" >>"$prep_log"
  reset_repo_for_tool "$tool" >>"$prep_log" 2>&1
  echo "[prepare] init repo" >>"$prep_log"
  init_repo_for_tool "$tool" >>"$prep_log" 2>&1
  echo "[prepare] seed backup snapshot-1 (untimed)" >>"$prep_log"
  backup_adhoc_for_tool "$tool" "$DATASET_SNAPSHOT1" >>"$prep_log" 2>&1
}

prepare_measurement_for_op() {
  local op="$1" prep_log="$2"

  RUN_VYKAR_RESTORE_SNAPSHOT=""
  RUN_BORG_BACKUP_ARCHIVE=""
  RUN_BORG_RESTORE_ARCHIVE=""

  echo "[measure-prepare] op=$op" >>"$prep_log"

  case "$op" in
    vykar_restore)
      echo "[measure-prepare] clean restore dir: $RESTORE_VYKAR" >>"$prep_log"
      if ! clear_dir_contents "$RESTORE_VYKAR" "$prep_log"; then
        echo "[measure-prepare] failed to clean restore dir: $RESTORE_VYKAR" >>"$prep_log"
        return 1
      fi
      if ! RUN_VYKAR_RESTORE_SNAPSHOT="$(
        vykar list -R bench --last 1 | awk 'NR==2{print $1}'
      )"; then
        echo "[measure-prepare] failed to resolve latest vykar snapshot" >>"$prep_log"
        return 1
      fi
      [[ -n "$RUN_VYKAR_RESTORE_SNAPSHOT" ]] || {
        echo "[measure-prepare] empty latest vykar snapshot id" >>"$prep_log"
        return 1
      }
      echo "[measure-prepare] vykar snapshot=$RUN_VYKAR_RESTORE_SNAPSHOT" >>"$prep_log"
      ;;
    restic_restore)
      echo "[measure-prepare] clean restore dir: $RESTORE_RESTIC" >>"$prep_log"
      if ! clear_dir_contents "$RESTORE_RESTIC" "$prep_log"; then
        echo "[measure-prepare] failed to clean restore dir: $RESTORE_RESTIC" >>"$prep_log"
        return 1
      fi
      ;;
    rustic_restore)
      echo "[measure-prepare] clean restore dir: $RESTORE_RUSTIC" >>"$prep_log"
      if ! clear_dir_contents "$RESTORE_RUSTIC" "$prep_log"; then
        echo "[measure-prepare] failed to clean restore dir: $RESTORE_RUSTIC" >>"$prep_log"
        return 1
      fi
      ;;
    borg_backup)
      RUN_BORG_BACKUP_ARCHIVE="bench-$(date -u +%Y%m%dT%H%M%S)-$RANDOM"
      echo "[measure-prepare] borg archive=$RUN_BORG_BACKUP_ARCHIVE" >>"$prep_log"
      ;;
    borg_restore)
      echo "[measure-prepare] clean restore dir: $RESTORE_BORG" >>"$prep_log"
      if ! clear_dir_contents "$RESTORE_BORG" "$prep_log"; then
        echo "[measure-prepare] failed to clean restore dir: $RESTORE_BORG" >>"$prep_log"
        return 1
      fi
      if ! RUN_BORG_RESTORE_ARCHIVE="$(borg list --short | tail -n1)"; then
        echo "[measure-prepare] failed to resolve latest borg archive" >>"$prep_log"
        return 1
      fi
      [[ -n "$RUN_BORG_RESTORE_ARCHIVE" ]] || {
        echo "[measure-prepare] empty latest borg archive" >>"$prep_log"
        return 1
      }
      echo "[measure-prepare] borg archive=$RUN_BORG_RESTORE_ARCHIVE" >>"$prep_log"
      ;;
    kopia_restore)
      echo "[measure-prepare] clean restore dir: $RESTORE_KOPIA" >>"$prep_log"
      if ! clear_dir_contents "$RESTORE_KOPIA" "$prep_log"; then
        echo "[measure-prepare] failed to clean restore dir: $RESTORE_KOPIA" >>"$prep_log"
        return 1
      fi
      ;;
  esac

  run_storage_settle "$prep_log"

  echo "[measure-prepare] drop caches" >>"$prep_log"
  drop_caches >>"$prep_log" 2>&1
}

# --- Backfill helper ---

list_previous_run_roots() {
  local base="$RUNTIME_ROOT/benchmarks"
  [[ -d "$base" ]] || return 0
  local stamps=()
  for d in "$base"/*; do
    [[ -d "$d" ]] || continue
    local bn
    bn=$(basename "$d")
    [[ "$bn" =~ ^[0-9]{8}T[0-9]{6}Z$ ]] || continue
    [[ "$bn" < "$STAMP" ]] || continue
    stamps+=("$bn")
  done
  if [[ "${#stamps[@]}" -gt 0 ]]; then
    printf "%s\n" "${stamps[@]}" | sort -r | while IFS= read -r bn; do
      echo "$base/$bn"
    done
  fi
}

# --- Run one operation ---

run_one() {
  local op="$1"
  local cmd phase tool
  cmd=$(measured_cmd_for_op "$op")
  phase=$(phase_from_op "$op")
  tool=$(tool_from_op "$op")

  local out="$OUT_ROOT/profile.$op"
  local runs_dir="$out/runs"
  mkdir -p "$runs_dir"

  # Meta
  {
    echo "name=$op"
    echo "dataset=$DATASET_DIR"
    echo "dataset_snapshot_1=$DATASET_SNAPSHOT1"
    echo "dataset_snapshot_2=$DATASET_SNAPSHOT2"
    echo "dataset_benchmark=$DATASET_BENCHMARK"
    echo "timed_cmd=$cmd"
    echo "runs=$RUNS"
    echo "warmup_runs=$WARMUP_RUNS"
    echo "timestamp_utc=$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  } >"$out/meta.txt"

  local i rc=0 failed_runs=0 failed_warmups=0

  # Warmup
  for ((i = 1; i <= WARMUP_RUNS; i++)); do
    local run_label warmup_measure_prep_log
    run_label=$(printf "%03d" "$i")
    warmup_measure_prep_log="$out/warmup-$run_label.measure-prep.log"
    echo "[warmup] $op $i/$WARMUP_RUNS"
    if should_prepare_op_run "$op"; then
      if ! prepare_op_run "$op" "$out/warmup-$run_label.prep.log"; then
        failed_warmups=$((failed_warmups + 1))
        continue
      fi
    fi
    if ! prepare_measurement_for_op "$op" "$warmup_measure_prep_log"; then
      failed_warmups=$((failed_warmups + 1))
      continue
    fi
    if ! run_with_op_env "$op" bash -lc "$cmd" >"$out/warmup-$run_label.stdout.txt" 2>"$out/warmup-$run_label.stderr.txt"; then
      failed_warmups=$((failed_warmups + 1))
    fi
  done

  # Timed runs
  for ((i = 1; i <= RUNS; i++)); do
    local run_label run_stdout run_timev run_rc_file run_repo_size_file run_measure_prep_log
    run_label=$(printf "%03d" "$i")
    run_stdout="$runs_dir/run-$run_label.stdout.txt"
    run_timev="$runs_dir/run-$run_label.timev.txt"
    run_rc_file="$runs_dir/run-$run_label.rc"
    run_repo_size_file="$runs_dir/run-$run_label.repo-size-bytes.txt"
    run_measure_prep_log="$runs_dir/run-$run_label.measure-prep.log"

    if [[ "$phase" == "restore" ]]; then
      echo "[run] $op $i/$RUNS (no prep; uses preceding timed backup state)"
    else
      echo "[run] $op $i/$RUNS"
    fi

    if should_prepare_op_run "$op"; then
      if ! prepare_op_run "$op" "$runs_dir/run-$run_label.prep.log"; then
        rc=1; failed_runs=$((failed_runs + 1))
        : >"$run_timev"
        echo "$rc" >"$run_rc_file"
        write_repo_size_bytes_for_tool "$tool" "$run_repo_size_file"
        continue
      fi
    fi

    if ! prepare_measurement_for_op "$op" "$run_measure_prep_log"; then
      rc=1; failed_runs=$((failed_runs + 1))
      : >"$run_timev"
      echo "$rc" >"$run_rc_file"
      write_repo_size_bytes_for_tool "$tool" "$run_repo_size_file"
      continue
    fi

    if run_with_op_env "$op" /usr/bin/time -v bash -lc "$cmd" >"$run_stdout" 2>"$run_timev"; then
      rc=0
    else
      rc=$?; failed_runs=$((failed_runs + 1))
    fi
    echo "$rc" >"$run_rc_file"
    write_repo_size_bytes_for_tool "$tool" "$run_repo_size_file"
  done

  if [[ "$failed_runs" -eq 0 ]]; then
    echo "OK (time -v runs=$RUNS warmups=$WARMUP_RUNS failed_warmups=$failed_warmups)" >"$out/status.txt"
  else
    echo "FAILED (time -v failed_runs=$failed_runs/$RUNS failed_warmups=$failed_warmups/$WARMUP_RUNS)" >"$out/status.txt"
  fi

  # Optional perf stat
  if [[ "$PROFILE_PERF" == "1" && "$HAVE_PERF" == "1" ]]; then
    local perf_ready=1
    if should_prepare_op_run "$op"; then
      if ! prepare_op_run "$op" "$out/perf.prep.log"; then
        perf_ready=0
      fi
    fi
    if [[ "$perf_ready" == "1" ]] && prepare_measurement_for_op "$op" "$out/perf.measure-prep.log"; then
      run_with_op_env "$op" perf stat -d -r 1 -- bash -lc "$cmd" >"$out/perf.stdout.txt" 2>"$out/perf.stat.txt" || true
    fi
  fi

  # Optional strace
  if [[ "$PROFILE_STRACE" == "1" && "$HAVE_STRACE" == "1" ]]; then
    local strace_ready=1
    if should_prepare_op_run "$op"; then
      if ! prepare_op_run "$op" "$out/strace.prep.log"; then
        strace_ready=0
      fi
    fi
    if [[ "$strace_ready" == "1" ]] && prepare_measurement_for_op "$op" "$out/strace.measure-prep.log"; then
      run_with_op_env "$op" strace -c -f -qq -o "$out/strace.summary.txt" bash -lc "$cmd" >/dev/null 2>&1 || true
    fi
  fi
}

# --- Main ---

cd "$OUT_ROOT"

# Write commands manifest
{
  echo "workflow: per warmup/run => reset repo + init + untimed backup snapshot-1 + storage settle(sync/fstrim/nvme flush + 20s) + drop caches"
  echo "restore workflow: no repo prep; restore uses state from preceding timed <tool>_backup op"
  for op in "${OPS[@]}"; do
    echo "$op: $(measured_cmd_for_op "$op")"
  done
} >"$OUT_ROOT/commands.txt"

echo "[config] dataset=$DATASET_DIR runs=$RUNS tool=$SELECTED_TOOL_LABEL"
echo "[dataset] seed=$DATASET_SNAPSHOT1"
echo "[dataset] benchmark=$DATASET_BENCHMARK"

for op in "${OPS[@]}"; do
  run_one "$op"
  if [[ "$(phase_from_op "$op")" == "restore" ]]; then
    cleanup_repo_for_tool "$(tool_from_op "$op")"
    cleanup_restore_for_tool "$(tool_from_op "$op")"
  fi
done

# Repo size stats
du -sh "$VYKAR_REPO" "$RESTIC_REPO" "$RUSTIC_REPO" "$BORG_REPO" "$KOPIA_REPO" >"$OUT_ROOT/repo-sizes.txt"

# Tool-specific repo stats
{ echo "== vykar info =="; vykar info -R bench || true; } >"$OUT_ROOT/vykar.info.txt" 2>&1
{ echo "== restic snapshots =="; restic snapshots || true; echo; echo "== restic stats (raw-data) =="; restic stats --mode raw-data || true; } >"$OUT_ROOT/restic.stats.txt" 2>&1
{ echo "== rustic snapshots =="; rustic snapshots || true; echo; echo "== rustic stats =="; rustic stats || true; } >"$OUT_ROOT/rustic.stats.txt" 2>&1
{ echo "== borg info =="; borg info || true; echo; echo "== borg list =="; borg list || true; } >"$OUT_ROOT/borg.stats.txt" 2>&1
{ echo "== kopia repository status =="; kopia --config-file "$KOPIA_CONFIG" repository status || true; echo; echo "== kopia snapshots =="; kopia --config-file "$KOPIA_CONFIG" snapshot list || true; echo; echo "== kopia content stats =="; kopia --config-file "$KOPIA_CONFIG" content stats || true; } >"$OUT_ROOT/kopia.stats.txt" 2>&1

cat >"$OUT_ROOT/README.txt" <<EOF
Benchmark run: $STAMP
Dataset root: $DATASET_DIR
Seed snapshot (untimed): $DATASET_SNAPSHOT1
Benchmark dataset (timed): $DATASET_BENCHMARK
Runs per benchmark: $RUNS
Selected tool: $SELECTED_TOOL_LABEL

Workflow per run:
1) reset/init tool repo
2) untimed backup of snapshot-1
3) storage settle (sync + fstrim + nvme flush + cooldown) + drop caches
4) timed benchmark step:
   - backup ops: backup top-level dataset (snapshot-1 + snapshot-2)
   - restore ops: timed restore of latest from preceding timed backup op state

Outputs:
- commands.txt / repo-sizes.txt
- profile.<op>/runs/run-*.timev.txt
- profile.<op>/runs/run-*.repo-size-bytes.txt
- reports/summary.{tsv,md,json}
- reports/benchmark.summary.png
EOF

# Summary + chart report
REPORT_ARGS=()
if [[ -n "$TOOL" ]]; then
  mapfile -t PREV_RUN_ROOTS < <(list_previous_run_roots || true)
  if [[ "${#PREV_RUN_ROOTS[@]}" -gt 0 ]]; then
    echo "[report] backfill roots (${#PREV_RUN_ROOTS[@]}):"
    for prev in "${PREV_RUN_ROOTS[@]}"; do
      echo "  - $prev"
      REPORT_ARGS+=(--backfill-root "$prev")
    done
    if [[ "${#SELECTED_TOOLS[@]}" -eq 1 ]]; then
      REPORT_ARGS+=(--backfill-mode nonselected --selected-tool "${SELECTED_TOOLS[0]}")
    else
      REPORT_ARGS+=(--backfill-mode missing)
    fi
  else
    echo "[report] no previous run found for backfill"
  fi
fi

python3 "$SCRIPT_DIR/benchmark_report.py" all "$OUT_ROOT" --out-dir "$OUT_ROOT/reports" "${REPORT_ARGS[@]}"

echo "OK: results in $OUT_ROOT"
