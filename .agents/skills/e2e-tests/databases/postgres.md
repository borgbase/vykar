---
name: postgres
description: "Test PostgreSQL backups using hooks and command_dumps patterns"
---

# PostgreSQL Integration

## Goal

Test PostgreSQL backups using both recipe patterns from [vykar docs](https://vykar.borgbase.com/recipes#databases):
1. **Hooks** that write dump files to disk
2. **command_dumps** that stream stdout directly

## Test Data Setup

1. Start a Postgres container:
   ```bash
   sudo docker run -d --name vykar-pg -e POSTGRES_PASSWORD=testpass -p 5432:5432 postgres:16
   ```
2. Generate realistic large data (default baseline: ~10 GiB):
   ```bash
   REPO_ROOT="$(git rev-parse --show-toplevel)"
   bash "$REPO_ROOT/scripts/postgres-generate-random-data.sh" \
     --container vykar-pg \
     --target-gib 10
   ```
3. Verify generator output includes:
   - `final_bytes` around 10 GiB
   - Multiple populated tables (`customers`, `products`, `orders`, `order_events`)
4. Save generator output to scenario log (required)

## Variant A: Hooks Dump to Temporary Directory

Configure source in vykar config:
- `label: pg-hooks`
- `path: <temp_dump_dir>`
- `hooks.before`: create dir + `pg_dump -U postgres -Fc vykar_pg_test > <temp_dump_dir>/vykar_pg_test.dump`
- `hooks.after`: remove temp dir

Run backup and validate snapshot contains `vykar_pg_test.dump`.

## Variant B: command_dumps

Configure source in vykar config:
- `label: pg-cmd`
- `command_dumps`:
  - `name: vykar_pg_test.dump`
  - `command: pg_dump -U postgres -h 127.0.0.1 -Fc vykar_pg_test`

Run backup and validate artifact exists under `.vykar-dumps/` in snapshot listing.

## Run Matrix

Run both variants against each backend:
1. `local` first
2. `rest` second
3. `s3` third
4. `sftp` last (with timeouts, mark BLOCKED on failure)

Clean remote storage with `rclone delete --rmdirs` between backend runs.

## Integrity Check

1. Restore dump artifact from snapshot into temp directory
2. Create fresh database `vykar_pg_restore_test`
3. `pg_restore` the dump into the fresh database
4. Verify restored counts match seeded source counts for:
   - `customers`
   - `products`
   - `orders`
   - `order_events`
5. Verify at least one sampled restored row contains non-trivial randomized content
6. Ensure Postgres client/server major versions are compatible

## Common Issues

- `vykar snapshot` usage: `-R <repo>` belongs to `snapshot`, not `list` subcommand
- Command dump artifacts appear under `.vykar-dumps/` in snapshot listings
- Client/server version mismatches can cause `pg_restore` config parameter errors
- Use `sudo docker` if user lacks Docker socket access
- Large dumps can run for a long time; use generous command timeouts for remote backends

## Cleanup

1. Drop test databases
2. Stop and remove Postgres container: `sudo docker rm -f vykar-pg`
3. Clean remote storage paths with `rclone` before next scenario
