---
name: mongodb
description: "Test MongoDB backups using command_dumps with mongodump --archive"
---

# MongoDB Integration

## Goal

Test MongoDB backup workflow using `command_dumps` with `mongodump --archive --gzip`.

## Test Data Setup

1. Pre-pull image: `sudo docker pull mongo:7`
2. Start a MongoDB container:
   ```bash
   sudo docker run -d --name vykar-mongo -p 27017:27017 mongo:7
   ```
3. Generate realistic large data (default baseline: ~2.5 GiB):
   ```bash
   REPO_ROOT="$(git rev-parse --show-toplevel)"
   bash "$REPO_ROOT/scripts/mongodb-generate-random-data.sh" \
     --container vykar-mongo \
     --target-gib 2.5 \
     --db vykar_mongo_test
   ```
4. Verify generator output includes:
   - `final_db_bytes` around 2.5 GiB
   - Populated collections (`customers`, `products`, `orders`, `order_events`)
5. Save generator output to scenario log (required)

## Backup Variant: command_dumps

Configure source in vykar config:
- `label: mongodb`
- `command_dumps`:
  - `name: vykar_mongo_test.archive.gz`
  - `command: mongodump --archive --gzip --db vykar_mongo_test`

If host lacks `mongodump`, use the Docker exec fallback:
```yaml
command: sudo docker exec vykar-mongo sh -lc 'mongodump --archive --gzip --db vykar_mongo_test'
```

## Optional: Full-Instance Variant

Additional run capturing all databases:
- `name: all.archive.gz`
- `command: mongodump --archive --gzip` (no `--db` flag)

## Run Matrix

1. `local` first with full restore validation
2. `rest` second (local `vykar-server`)
3. Clean S3 with `rclone delete --rmdirs`, run `s3` third
4. `sftp` last (or skip when investigating unrelated scenarios)

## Integrity Check

1. Restore archive artifact from snapshot into temp directory
2. Restore into fresh database `vykar_mongo_restore_test`:
   ```bash
   mongorestore --archive=<artifact> --gzip --nsFrom='vykar_mongo_test.*' --nsTo='vykar_mongo_restore_test.*'
   ```
3. Verify restored document counts match seeded source counts for:
   - `customers`
   - `products`
   - `orders`
   - `order_events`
4. Verify sampled restored document content is randomized/high-entropy

If host lacks `mongorestore`, use `docker exec` approach.

## Common Issues

- Host may not have MongoDB client tools — `docker exec` is a reliable fallback
- Command dump artifacts appear under `.vykar-dumps/` in snapshot listings
- Large archives can take substantial time; use generous timeouts for slower backends

## Cleanup

1. Drop test databases
2. Stop and remove MongoDB container: `sudo docker rm -f vykar-mongo`
3. Clean remote storage paths with `rclone` between runs
