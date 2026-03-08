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
```

## Tests

```bash
uv run --project scripts/testbench --with pytest pytest scripts/testbench/tests/
```
