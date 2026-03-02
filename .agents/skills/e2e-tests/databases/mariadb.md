---
name: mariadb
description: "Test MariaDB backups with hooks and command_dumps patterns"
---

# MariaDB Integration

## Goal

Test MariaDB backups using both recipe patterns:
1. **Hooks** that write SQL dump files to disk
2. **command_dumps** that stream `mariadb-dump` stdout

## Test Data Setup

1. Pre-pull image: `sudo docker pull mariadb:11`
2. Start a MariaDB container:
   ```bash
   sudo docker run -d --name vykar-maria -e MARIADB_ROOT_PASSWORD=testpass -p 3306:3306 mariadb:11
   ```
3. Create test database `vykar_maria_test`
4. Create dedicated dump user (recommended for stable auth inside container):
   - user: `vykar`
   - host: `localhost`
   - password: `vykarpass`
   - grants: full privileges on `vykar_maria_test.*`
5. Generate realistic large data (default baseline: ~10 GiB):
   ```bash
   REPO_ROOT="$(git rev-parse --show-toplevel)"
   bash "$REPO_ROOT/scripts/mariadb-generate-random-data.sh" \
     --container vykar-maria \
     --target-gib 10 \
     --db vykar_maria_test
   ```
6. Verify generator output includes:
   - `final_bytes` around 10 GiB
   - Populated tables (`customers`, `products`, `orders`, `order_events`)
7. Save generator output to scenario log (required)

## Variant A: Hooks Dump to Temporary Directory

Configure source in vykar config:
- `label: maria-hooks`
- `path: <temp_dump_dir>`
- `hooks.before`: create dir + `mariadb-dump --protocol=socket --socket=/run/mysqld/mysqld.sock -u vykar -pvykarpass vykar_maria_test > <temp_dump_dir>/vykar_maria_test.sql`
- `hooks.after`: remove temp dir

Run backup and validate snapshot contains SQL dump file.

## Variant B: command_dumps

Configure source in vykar config:
- `label: maria-cmd`
- `command_dumps`:
  - `name: vykar_maria_test.sql`
  - `command: sh -lc 'for i in 1 2 3 4 5; do mariadb-dump --protocol=socket --socket=/run/mysqld/mysqld.sock -u vykar -pvykarpass vykar_maria_test && exit 0; sleep 1; done; exit 1'`

Run backup and validate artifact exists and is non-empty.

## Run Matrix

1. Initialize local repo, run both variants + restore check
2. Run REST backend variants second (local `vykar-server`)
3. Clean S3 with `rclone delete --rmdirs`, run both variants
4. Run SFTP variants last with timeouts — mark BLOCKED on timeout
5. Do NOT rerun full plan for SFTP-only failures

## Integrity Check

1. Restore SQL artifact from snapshot
2. Import into fresh database `vykar_maria_restore_test`
3. Verify restored counts match seeded source counts for:
   - `customers`
   - `products`
   - `orders`
   - `order_events`
4. Verify sampled restored row content is randomized (not fixed or all-zero payloads)

## Higher-Volume Variant (optional, >10 GiB)

Use this when stress-testing `command_dumps` at larger scales:

1. Re-run generator with a higher target:
   - `scripts/mariadb-generate-random-data.sh --container <maria_container> --target-gib 30`
2. Keep the dump command socket-based with retries:
   - `mariadb-dump --quick --ssl=0 --protocol=socket --socket=/run/mysqld/mysqld.sock -u vykar -pvykarpass vykar_maria_test`
3. Enforce generous backup/restore timeouts (`timeout 10800`)
4. Validate restored SQL artifact size (expect many GiB)

## Common Issues

- Modern MariaDB images use `mariadb`, `mariadb-dump`, `mariadb-admin` — not the legacy `mysql*` names
- `mariadb:11` images may not include a `mysql` binary at all; use `mariadb` client commands for probes/imports
- Root authentication mode can vary across images; a dedicated dump user is more reliable than root for `command_dumps`
- Some runs intermittently fail with socket/auth errors (`2002`/`1045`); add bounded retry around dump/backup when validating stability
- In this sandbox, host TCP to a mapped port can work while in-container `--protocol=tcp -h 127.0.0.1` fails intermittently; prefer in-container socket dumps for `docker exec` workflows
- Large `command_dumps` can drive high `vykar` RSS during capture; if memory pressure appears, prefer hook-based dump-to-file workflows for realistic large-data tests
- Avoid low-entropy fillers (`REPEAT('x', ...)`) for baseline tests; high-entropy random data is required
- Command dump artifacts appear under `.vykar-dumps/` in snapshot listings
- Use `sudo docker` if user lacks Docker socket access

## Cleanup

1. Drop test databases
2. Stop and remove MariaDB container: `sudo docker rm -f vykar-maria`
3. Clean remote storage paths with `rclone` between runs
4. Ensure no stuck `vykar` process remains after aborted SFTP steps
