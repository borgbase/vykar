---
name: stress
description: "Run long-loop local backend stress testing with backup/restore/verify/delete lifecycle"
---

# Stress Testing (Local Corpus)

## Goal

Continuously exercise the local backend lifecycle to catch correctness, lock handling, and repository maintenance regressions under repeated operations.

## Scope

- **Backend**: local
- **Source dataset**: `~/corpus-local` by default
- **Lifecycle per iteration**: `backup -> list -> restore -> verify -> snapshot delete -> compact -> prune`
- **Optional periodic checks**: `check` and `check --verify-data`

## Script Location

Use the bundled harness:

```bash
REPO_ROOT="$(git rev-parse --show-toplevel)"
bash "$REPO_ROOT/scripts/stress.sh" --help
```

## Defaults

All defaults live in `scripts/lib/defaults.sh` and can be overridden via env vars.

- `vykar` binary: discovered on `PATH`
- Corpus: `~/corpus-local` (override with `CORPUS_LOCAL` env var)
- Backend: `local` (override with `--backend local|rest|s3`)
- Runtime root: `~/runtime/stress/<backend>` (override with `STRESS_ROOT` env var)

## Quick Runs

Smoke test:

```bash
REPO_ROOT="$(git rev-parse --show-toplevel)"
bash "$REPO_ROOT/scripts/stress.sh" \
  --iterations 1 \
  --check-every 1 \
  --verify-data-every 0
```

Longer stress pass:

```bash
REPO_ROOT="$(git rev-parse --show-toplevel)"
bash "$REPO_ROOT/scripts/stress.sh" \
  --iterations 1000 \
  --check-every 50 \
  --verify-data-every 0
```

Server run with cache dropping and timing:

```bash
bash "$REPO_ROOT/scripts/stress.sh" \
  --iterations 100 \
  --backend rest \
  --drop-caches \
  --time-v
```

## Outputs and Artifacts

- Working artifacts: `~/runtime/stress/<backend>/work/`
- Per-command logs: `~/runtime/stress/<backend>/work/logs/`

On success, transient work artifacts are removed automatically. On failure, artifacts are preserved and failure context is printed with pointers to relevant logs.

## Validation Expectations

1. `backup` succeeds every iteration.
2. Snapshot appears in `list` output.
3. `restore` completes for the newly created snapshot.
4. `diff -qr --no-dereference <source> <restore_dir>` reports no differences.
5. `snapshot delete`, `compact`, and `prune` complete without stale lock files.
6. Optional `check` operations pass when enabled.

## Failure Triage

When the harness fails, capture:

1. Failure context printed to stderr (iteration, step, snapshot, log paths).
2. Relevant logs under `~/runtime/stress/<backend>/work/logs/`.
3. Diff output path when restore verification fails.
4. Any stale lock files under `~/runtime/stress/<backend>/work/repository/locks/`.
