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

### `net-ab` — interleaved A/B over a shaped network link

Runs `vykar backup` against a network backend with simulated WAN latency on loopback and compares binaries the same way `mem-ab` does (each variant once per round, order rotated, medians plus round-paired deltas against the first variant). `--backend` is `s3` (MinIO), `rest` (vykar-server), `sftp` (sshd) or `local` (unshaped reference). Every run gets a fresh repository (`init` + timed `backup`, plus a timed no-change backup with `--incremental`), which is deleted afterwards.

```bash
uv run --project scripts/testbench net-ab --backend s3 --variant base=/path/to/vykar-base --variant cand=target/release/vykar
uv run --project scripts/testbench net-ab --backend sftp --variant base=a --variant cand=b --rounds 5 --delay-ms 20 --rate 1gbit
uv run --project scripts/testbench net-ab --backend rest --variant base=a --variant cand=a --variant-conn cand=8 --incremental
```

- **Shaping**: only the backend's TCP port (taken from the repository URL: MinIO 9000, `REST_URL` 8585, `SFTP_PORT` 22 by default) is routed through netem on `lo` (prio qdisc with 4 bands, u32 sport/dport filters into band 4). `--delay-ms` is per direction, so RTT = 2x (default 20 → 40 ms RTT); `--rate` adds a netem rate limit. `--delay-ms 0` adds no delay but still routes the port through a count-only `pfifo` so bandwidth is measured. The `local` backend never touches `tc`. Uses passwordless `sudo /sbin/tc`; IPv4 loopback endpoints only.
- **netem state** is read first and put back on exit, also on error, Ctrl-C and SIGTERM (`--keep-netem` leaves the shaping in place). An unshaped `lo` gets its qdisc removed; an existing setup in the same layout is rebuilt with its port, delay and rate. Any other qdisc on `lo` makes `net-ab` refuse to run.
- **Locking**: takes `/tmp/vykar-s3bench.lock` and then `/tmp/vykar-mem-ab.lock`, so it queues behind (and blocks) `mem-ab` and other benchmarks that take either lock.
- **Repositories**: `s3` uses `s3+http://<endpoint>/<--s3-bucket>/net-ab/<run-id>/<run>` (endpoint as in `config.resolve_repo_url`); the bucket (default `vykar-net-ab`) is created if missing and removed at the end if `net-ab` created it and it is empty; cleanup deletes the objects under the run's prefix. `rest`: vykar-server serves one repository, so the server must be empty — `net-ab` refuses otherwise, or with `--rest-stash-existing` downloads a small pack-less repository into the out dir, and re-creates it (layout + objects) at the end; cleanup is `vykar delete` plus deleting whatever the run left. `sftp` uses `$SFTP_BASE_DIR/net-ab-sftp/<run-id>/<run>`; on loopback the run directory is removed locally (a `vykar delete` over a 40 ms SFTP link takes minutes), otherwise with `vykar delete`. `local` repos and every run's cache live in `--work-dir` (default `/mnt/repos/net-ab/<run-id>`), removed at the end.
- **Measurements** (per run, in `--out`, default `$RUNTIME_ROOT/net-ab/<stamp>`): wall, user/sys CPU and `ru_maxrss` from `wait4`; peak RssAnon and threads from a 100 ms `/proc` poller; the shaped band's byte counter (both directions, that port only) sampled every 100 ms into `r<N>.<variant>.timeline.tsv` and ~250 ms MiB/s buckets in `.bw.tsv`, summarised as mean / p10 / p50 / p90 / max / cv / idle fraction over the active span, plus `avg_mibs` = bytes / wall. `results.json` holds all runs, `summary.txt` the table. Binaries that honour `$VYKAR_HTTP_TRACE` (a measurement-only patch, not in the tree) get a per-request summary in `results.json`; an empty or missing trace (always for SFTP) is ignored.

**Socket buffer limits.** At 40 ms RTT a TCP connection can move at most window / RTT. MinIO pins its buffers with `SO_RCVBUF`, which the kernel caps at `net.core.rmem_max`; with the Debian default (212992) every S3 measurement is capped at ~21 MiB/s until `net.core.rmem_max` (and `wmem_max`) are raised to 4194304. `net-ab` warns, without changing anything, when `net.core.rmem_max`, `net.core.wmem_max` or the `tcp_rmem`/`tcp_wmem` maxima are below 4 MiB while latency is on. To raise them for a session: `sudo sysctl -w net.core.rmem_max=4194304 net.core.wmem_max=4194304`. SFTP is additionally bounded by the SSH channel window the server advertises (OpenSSH: 2 MiB) per connection.

## Tests

```bash
uv run --project scripts/testbench --with pytest pytest scripts/testbench/tests/
```
