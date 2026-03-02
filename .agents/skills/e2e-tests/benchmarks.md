---
name: benchmarks
description: "Compare vykar performance against restic, rustic, borg, and kopia on local backend"
---

# Performance Benchmarks

## Goal

Compare vykar against established backup tools — restic, rustic, borg, and kopia — on a local backend. Measure wall-clock time, peak memory usage, and CPU usage across common backup operations.

## Scope

- **Backend**: local only (eliminates network variability)
- **Source dataset**: default `~/corpus-remote` (smaller, faster iteration). Optionally `~/corpus-local` for stress.
- **Tools under test**: `vykar`, `restic`, `rustic`, `borg`, `kopia`

Dataset layout expected by the harness:
- `<dataset>/snapshot-1` (untimed seed snapshot)
- `<dataset>/snapshot-2` (second-state content included in benchmark dataset)

## Prerequisites

1. Install required tools if not present:
   ```bash
   sudo apt-get update -y
   sudo apt-get install -y restic time strace linux-perf
   # rustic is typically preinstalled in this sandbox; otherwise install it.
   ```
2. Verify all tools are available:
   ```bash
   vykar --version
   restic version
   rustic --version
   borg --version
   kopia --help
   ```
3. Ensure `/usr/bin/time` exists (not the shell builtin `time`).

## Quick Start (Scripted)

Use the bundled harness (preferred; produces comparable outputs every run).

Scripts live under the repository at `scripts/`. Defaults come from `scripts/lib/defaults.sh`.

```bash
REPO_ROOT="$(git rev-parse --show-toplevel)"

# Basic: repeated /usr/bin/time -v runs + averaged report metrics
bash "$REPO_ROOT/scripts/benchmark.sh" --runs 5

# Single tool + strace/perf on top of default profiling
bash "$REPO_ROOT/scripts/benchmark.sh" --tool vykar --runs 3 --strace --perf

# Custom dataset path
bash "$REPO_ROOT/scripts/benchmark.sh" --dataset ~/corpus-local --runs 3
```

The harness writes to `~/runtime/benchmarks/<UTC_STAMP>/`.

Post-processing (summary table + chart) runs automatically. Can also be invoked manually:
```bash
python3 "$REPO_ROOT/scripts/benchmark_report.py" all ~/runtime/benchmarks/<UTC_STAMP>
```

Each run writes:
- `~/runtime/benchmarks/<UTC_STAMP>/reports/summary.tsv`
- `~/runtime/benchmarks/<UTC_STAMP>/reports/summary.md`
- `~/runtime/benchmarks/<UTC_STAMP>/reports/summary.json`
- `~/runtime/benchmarks/<UTC_STAMP>/reports/benchmark.summary.png`

## What To Look At (Actionable Signals)

From `reports/summary.tsv`:
- `maxrss_kb`: memory spikes (often restore path)
- `*_std` columns: run-to-run variability per metric
- `user` vs `sys`: CPU vs kernel/IO bound work
- `fs_out`: write amplification during restore
- `throughput_mib_s`: based on top-level dataset size (`snapshot-1 + snapshot-2`)

From `strace.summary.txt` (when `--strace` is enabled):
- Heavy `futex` can indicate contention/over-threading.
- Heavy `statx/newfstatat/llistxattr/getdents64` indicates metadata-walk cost (tree scan).

From tool stats:
- vykar: `vykar.info.txt`
- restic: `restic.stats.txt` (includes `restic stats --mode raw-data`)
- rustic: `rustic.stats.txt`
- borg: `borg.stats.txt`

## Full vs Incremental

Decide what you are measuring:
- **Incremental**: run backups repeatedly against an existing repo (measures "unchanged tree" behavior).
- **Full**: wipe/re-init the repo before each backup run (measures ingest/pack performance).

The harness defaults to an "incremental-like" loop once initialized, but you can rerun it multiple times and compare across stamps. For full-ingest benchmarks, patch the harness to re-init repos per run or wrap each command with repo wipe + init.

Run controls:
- `--runs N` (default `3`): number of measured runs per operation.
- 1 unmeasured warmup run is always performed per operation.

Per warmup/measured run workflow:
1. Drop cache and reset/init the tool repo.
2. Create an untimed backup of `snapshot-1`.
3. Timed benchmark step:
   - backup ops: backup the top-level dataset (contains both snapshots)
   - restore ops: create untimed backup of top-level dataset, then timed restore

## Benchmark Phases

Test each tool through four phases:

### Phase 1: Repository Init
```bash
/usr/bin/time -v <tool> init ...
```

### Phase 2: First Backup (cold — no dedup)
```bash
/usr/bin/time -v <tool> backup ~/corpus-local ...
```

### Phase 3: Second Backup (warm — full dedup, no changes)
```bash
/usr/bin/time -v <tool> backup ~/corpus-local ...
```

### Phase 4: Full Restore
```bash
/usr/bin/time -v <tool> restore ... <restore_dir>
```

## Tool Setup

### vykar
```bash
export VYKAR_PASSPHRASE=123
vykar --config <config> init -R local
vykar --config <config> backup -R local -l bench ~/corpus-local
vykar --config <config> restore -R local <snapshot_id> <restore_dir>
```

### restic
```bash
export RESTIC_PASSWORD=123
restic init --repo ~/runtime/repos/restic-bench
restic backup --repo ~/runtime/repos/restic-bench ~/corpus-local
restic restore latest --repo ~/runtime/repos/restic-bench --target <restore_dir>
```

### rustic
```bash
export RUSTIC_PASSWORD=123
rustic init --repo ~/runtime/repos/rustic-bench
rustic backup --repo ~/runtime/repos/rustic-bench ~/corpus-local
rustic restore latest --repo ~/runtime/repos/rustic-bench <restore_dir>
```

### borg
```bash
export BORG_PASSPHRASE=123
export BORG_REPO=~/runtime/repos/borg-bench
borg init --encryption=repokey-blake2
borg create --compression zstd,3 ::bench-1 ~/corpus-local
mkdir -p <restore_dir>
(cd <restore_dir> && borg extract ::bench-1)
```

### kopia
```bash
export KOPIA_PASSWORD=123
kopia repository create filesystem --path ~/runtime/repos/kopia-bench
kopia snapshot create ~/corpus-local
kopia snapshot restore latest --target <restore_dir>
```

Adjust command syntax as needed — consult each tool's `--help` for exact flags.

## Metrics to Capture

From `/usr/bin/time -v` output:
- **Wall clock time** (`Elapsed (wall clock) time`)
- **Peak RSS** (`Maximum resident set size`)
- **CPU usage** (`Percent of CPU this job got`)
- **User time** (`User time`)
- **System time** (`System time`)
- **Context switches** (`Voluntary/Involuntary context switches`)
- **Page faults** (`Minor/Major page faults`)
- **FS IO** (`File system inputs/outputs`)

From `perf stat` (optional):
- Often blocked on locked-down hosts (`perf_event_paranoid=3`). Treat as best-effort.

## Output Format

Produce a comparison table:

| Phase | Metric | vykar | restic | rustic | borg | kopia |
|-------|--------|-------|--------|--------|------|-------|
| Init | Wall time | ... | ... | ... | ... | ... |
| First backup | Wall time | ... | ... | ... | ... | ... |
| First backup | Peak RSS | ... | ... | ... | ... | ... |
| Second backup | Wall time | ... | ... | ... | ... | ... |
| Restore | Wall time | ... | ... | ... | ... | ... |
| Restore | Peak RSS | ... | ... | ... | ... | ... |

Also note:
- Repository size on disk after first backup
- Repository size after second backup (dedup efficiency)
- Compression ratio if measurable

## Cleanup

1. Remove all benchmark repositories:
   ```bash
   rm -rf ~/runtime/repos/restic-bench ~/runtime/repos/rustic-bench ~/runtime/repos/borg-bench ~/runtime/repos/kopia-bench
   ```
2. Remove restore directories
3. Keep timing logs under `~/runtime/logs/`
