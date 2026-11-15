# Scripts

This directory contains the `testbench` Python project — a unified suite for testing, benchmarking, profiling, and stress-testing `vykar`. Run with `uv` from the repository root.

## Entry points

### `scenario` — YAML-driven end-to-end scenario runner

```bash
uv run --project scripts/testbench scenario scripts/scenarios/simple-backup.yaml
uv run --project scripts/testbench scenario scripts/scenarios/5xchurn.yaml --backend local --runs 3
```

### `benchmark` — comparative benchmark harness

Compares `vykar` with `restic`, `rustic`, `borg`, and `kopia`.
Local test/benchmark repositories are always created under `/mnt/repos` so the source corpus and repository stay on separate SSDs.

```bash
uv run --project scripts/testbench benchmark --runs 3
uv run --project scripts/testbench benchmark --runs 5 --tool vykar
uv run --project scripts/testbench benchmark --runs 3 --dataset ~/corpus-remote
```

### `bench-report` — benchmark chart generation

```bash
uv run --project scripts/testbench bench-report all ~/runtime/benchmarks/<STAMP>
uv run --project scripts/testbench bench-report chart ~/runtime/benchmarks/<STAMP>
```

### `stress` — autonomous stress tester

Runs repeated backup/restore/verify cycles.

```bash
uv run --project scripts/testbench stress --iterations 100 --backend local
uv run --project scripts/testbench stress --iterations 50 --check-every 5 --verify-data-every 10
```

### `profile` — heaptrack/perf profiler wrapper

```bash
uv run --project scripts/testbench profile --mode backup --backend local
uv run --project scripts/testbench profile --mode backup --profiler perf --skip-build
uv run --project scripts/testbench profile --mode backup-incremental --skip-build
```

`backup` profiles a backup of the whole corpus into a repo seeded with `snapshot-1`; `backup-incremental` re-runs a backup of an unchanged corpus (repo open, cache-hit walk, commit).

### `mem-ab` — interleaved A/B peak-RSS comparison

Peak RSS drifts between sweeps and varies run to run, so binaries are only comparable within one sweep. `mem-ab` runs every variant once per round, rotates the order, restores the same repository and cache snapshot before each run, and reports medians plus round-paired deltas against the first variant. Run it with two copies of the same binary first to see the noise floor.

```bash
uv run --project scripts/testbench mem-ab --variant base=/path/to/vykar-base --variant cand=target/profiling/vykar
uv run --project scripts/testbench mem-ab --variant base=a --variant cand=b --workload incremental --rounds 8
```

Sweeps take a host-wide lock (`/tmp/vykar-mem-ab.lock`), so concurrent invocations queue instead of perturbing each other.

`--workload both` (the default) means the two backup workloads. `--workload restore` backs up the whole `--source` once and measures `vykar restore -R mem-ab latest <dest>`. The repository is not copied per run (restore does not change it); only the small cache directory is reset, and `<dest>` (`--restore-dest`, default `$RUNTIME_ROOT/mem-ab-restore-dest`, so not on `/mnt/repos`) is recreated empty before and deleted after every run. `--reuse-work-dir` (implies `--keep`) reuses a work dir seeded by an earlier restore sweep, or seeds it if absent:

```bash
uv run --project scripts/testbench mem-ab --workload restore --work-dir /mnt/repos/mem-ab-restore --reuse-work-dir \
  --variant base=~/runtime/restore-mem/bin/vykar-base --variant cand=target/profiling/vykar --rounds 5
```

Every run also polls `/proc/<pid>/status` every 50 ms: `results.tsv` gains peak VmRSS/RssAnon/RssFile/RssShmem/Threads columns, the summary shows median peak RssAnon and RssFile+RssShmem, and `<workload>.r<N>.<variant>.timeline.tsv` holds the samples. Polled peaks can miss spikes shorter than the interval; `maxrss_kb` (from `wait4`) stays the exact total.

## Tests

```bash
uv run --project scripts/testbench --with pytest pytest scripts/testbench/tests/
```
