---
name: e2e-tests
description: "End-to-end validation suite for vykar backup tool on a Linux sandbox server"
---

# Vykar E2E Test Suite

You are a Linux sysadmin testing vykar on a dedicated sandbox server. Your goal is to validate backup and restore correctness across all supported backends, database integrations, container workflows, filesystem snapshot patterns, and performance benchmarks.

**Work autonomously.** This is a disposable sandbox — do not ask for permission or confirmation before running commands, installing packages, creating/deleting files, or making destructive changes. If something fails, diagnose and fix it yourself. Only stop to ask the user if you are completely stuck with no viable path forward.

## Sandbox Environment

The test server provides:

| Resource | Path | Purpose |
|----------|------|---------|
| Large corpus | `~/corpus-local` | Test data for local backend (large) |
| Small corpus | `~/corpus-remote` | Test data for S3/SFTP/REST backends (bandwidth-aware) |
| Base config | `~/vykar.sample.yaml` | Repo definitions, credentials, and connection details |
| Vykar docs | https://vykar.borgbase.com/ | Recipe reference for hooks, command_dumps, etc. |

**Installed tools**: `vykar`, `vykar-server`, `rclone`, `docker`, `podman`, database clients (pg, mariadb, mongo).
Install missing packages with `sudo apt-get install ...`.

## Sub-skills

Run each sub-skill to execute a specific test area. Results go to `~/runtime/`.

### Backends — Corpus backup + restore validation
- **`e2e-tests:backends:local`** — Full backup/restore with large corpus on local backend
- **`e2e-tests:backends:rest`** — Backup/restore against local `vykar-server` REST backend
- **`e2e-tests:backends:s3`** — Backup/restore with small corpus on S3 backend
- **`e2e-tests:backends:sftp`** — Backup/restore with small corpus on SFTP backend (timeout-bounded)

### Databases — Hooks and command_dumps patterns (large realistic data)
- **`e2e-tests:databases:postgres`** — PostgreSQL with hooks dump and command_dumps variants on ~10 GiB randomized schema
- **`e2e-tests:databases:mariadb`** — MariaDB with hooks dump and command_dumps variants on ~10 GiB randomized schema
- **`e2e-tests:databases:mongodb`** — MongoDB with command_dumps (mongodump --archive) on ~2.5 GiB randomized collections

### Containers — Volume backups and container integration
- **`e2e-tests:containers:docker`** — Static volumes, downtime hooks, DB exec dumps via Docker
- **`e2e-tests:containers:podman`** — Same scenarios using Podman commands

### Filesystems — Snapshot hooks patterns
- **`e2e-tests:filesystems:btrfs`** — Btrfs read-only subvolume snapshot hooks
- **`e2e-tests:filesystems:zfs`** — ZFS dataset snapshot hooks via .zfs/snapshot path

### Benchmarks
- **`e2e-tests:benchmarks`** — Compare vykar performance against restic and rustic (use `benchmarks.md` + bundled scripts under `scripts/`)
- **`e2e-tests:stress`** — Run long-loop backup/restore/delete stress validation against local corpus (use `stress.md` + `scripts/stress.sh`)

## Recommended Execution Order

1. **Backends** first (establishes corpus validation baseline)
2. **Databases** (large-data, container-based tests)
3. **Containers** (reuses DB patterns with volume workflows)
4. **Filesystems** (requires disk/partition setup)
5. **Stress** next (long-loop correctness/locking pressure on local backend)
6. **Benchmarks** last (long-running, independent)

## Shared Conventions

### Environment Setup
```bash
export VYKAR_PASSPHRASE=123    # non-interactive passphrase
```
- Use `sudo` for package installs and root-owned paths
- Working directory for all test artifacts: `~/runtime/`

### Database Data Volume Baseline
- Database scenarios are **not** small-smoke tests by default.
- Seed randomized, high-entropy data before backup using these defaults:
  - PostgreSQL: **~10 GiB**
  - MariaDB: **~10 GiB**
  - MongoDB: **~2.5 GiB** (faster validation target)
- Prefer these helper scripts from repo root:
  - `scripts/postgres-generate-random-data.sh --container <name> --target-gib 10`
  - `scripts/mariadb-generate-random-data.sh --container <name> --target-gib 10`
  - `scripts/mongodb-generate-random-data.sh --container <name> --target-gib 2.5`
- Record resulting size and table/collection counts in scenario logs/reports.

### Config Strategy
1. Copy `~/vykar.sample.yaml` to a scenario-specific config (e.g., `config.postgres.yaml`)
2. Add test-specific `sources` blocks per scenario; keep repo definitions from sample
3. Keep each scenario in a separate config file to avoid source overlap
4. Reference repos by label: `-R local`, `-R rest`, `-R s3`, `-R sftp`
5. For local REST server mode, use a single repository URL root (e.g. `http://127.0.0.1:8585`) with:
   - `access_token: "<token>"`
   - `allow_insecure_http: true`
   - do not append `/<repo-name>` in single-repo mode

### Validation Standard
Every test must verify:
1. `vykar backup` exits 0
2. `vykar list` shows new snapshot for expected source label
3. `vykar --config <config> snapshot list -R <repo> <snapshot_id>` confirms expected files or artifacts
4. Restore into temp directory and verify:
   - Corpus tests: `diff -qr --no-dereference <source> <restore_dir>` reports no differences
   - Database tests: restore dump, verify row/document counts and sampled content match seeded large dataset
5. Optional: SHA256 manifest comparison for stronger content verification

### Cleanup Standard
1. **Reset repo before reruns**: run `vykar --config <config> delete -R <repo> --yes-delete-this-repo` before `init`
   - Treat `not found`/missing repo as non-fatal
   - REST single-repo servers may reject `delete` (for example `400/404`); if so, continue with `init`/`backup` and record it
2. **Local**: remove temporary directories (dumps, restores, configs)
3. **Local REST server data**: if using single-repo mode, wipe server data dir between reruns (for this sandbox: `/mnt/repos/bench-vykar/vykar-server-data/*`)
4. **Remote storage**: `rclone delete --rmdirs <remote:path>` between runs
   - Do NOT use `rclone purge` (may fail with 403 on restricted buckets)
   - Treat `directory not found` from rclone as non-fatal
5. **Containers**: stop and remove after each scenario
6. **Filesystems**: unmount/destroy test pools after runs

### Run Matrix
For tests that span multiple backends, run in this order:
1. **local** first (fast feedback loop)
2. **rest** second (local server path, still exercises HTTP backend)
3. **s3** third
4. **sftp** last (known instability, use timeouts)

### SFTP Guardrails
SFTP can be intermittent even when rclone works fine against the same server:
- Wrap all vykar commands with `timeout`: `timeout 120s vykar init ...`, `timeout 3600s vykar backup ...`
- On timeout (exit 124), mark test as **BLOCKED**, kill stuck process, continue cleanup
- Do NOT rerun the entire test suite if only SFTP failed — isolate SFTP results
- Ensure no stuck `vykar` process remains after aborted SFTP steps

### Deliverables
Each sub-skill should produce:
1. Scenario-specific config file saved under `~/runtime/`
2. Log file under `~/runtime/logs/`
3. Pass/fail summary report under `~/runtime/reports/`

## Common Gotchas

- Mixing `sudo vykar` and regular `vykar` creates root-owned repo files — use `sudo rm -rf` for cleanup
- Command dump artifacts appear under `.vykar-dumps/` in snapshot listings
- Prefer `vykar --config <config> ...` in automation; keep `--config` explicit in all commands
- `vykar snapshot` CLI forms:
  - `vykar --config <config> snapshot list -R <repo> <snapshot_id>`
  - `vykar --config <config> snapshot delete -R <repo> <snapshot_id>`
- REST local server may run in single-repo mode (`http://127.0.0.1:8585` root URL) and reject path-style repos
- Use `diff -qr --no-dereference` to avoid false negatives on broken symlinks in corpora
- MariaDB modern images use `mariadb`, `mariadb-dump`, `mariadb-admin` (not `mysql*` names)
- For MariaDB `docker exec` dumps, prefer socket protocol with retries; in-container TCP to `127.0.0.1` can be intermittently unreliable on this sandbox
- For high-entropy PostgreSQL seed data, use `scripts/postgres-generate-random-data.sh --container <name> --target-gib <N>`
- For high-entropy MariaDB seed data, use `scripts/mariadb-generate-random-data.sh --container <name> --target-gib <N>`
- For high-entropy MongoDB seed data, use `scripts/mongodb-generate-random-data.sh --container <name> --target-gib <N>`
- Btrfs hook snapshots require backing up a real Btrfs subvolume (not a plain directory)
- ZFS restore diffs should ignore the virtual `.zfs` directory
- MongoDB host tools may be missing — use `docker exec` or `podman exec` as fallback
- Pre-pull container images before timed runs to avoid skewing measurements
- Sample config repo paths may need adjustment for the sandbox — verify and update before first run
