---
name: docker
description: "Validate vykar workflows with Docker: static volumes, downtime hooks, DB dumps"
---

# Docker Integration

## Goal

Validate vykar workflows for Docker-backed sources:
1. Static data volume backup
2. Brief downtime volume backup via hooks
3. Database container command dumps

## Setup

1. Ensure Docker daemon is running
2. Use `sudo docker ...` if user lacks Docker socket access
3. Use `sudo vykar ...` when backing up paths under `/var/lib/docker/volumes/...`
4. Prepare test containers and volumes:
   - A static volume seeded with deterministic files
   - A writable app volume for stop/start hook testing
   - Database containers (Postgres, MariaDB, MongoDB) for `docker exec` dumps

## Scenario A: Static Volume Path Backup

1. Create a Docker volume and seed it with test files:
   ```bash
   sudo docker volume create vykar-static-vol
   ```
2. Resolve host volume path:
   ```bash
   sudo docker volume inspect vykar-static-vol --format '{{ .Mountpoint }}'
   ```
3. Configure source with resolved path and label `docker-static`
4. Run backup and verify files appear in snapshot

## Scenario B: Volume Backup with Downtime Hooks

1. Run a container writing to a volume
2. Configure source with:
   - `hooks.before: sudo docker stop <container>`
   - `hooks.after: sudo docker start <container>`
3. Run backup while app writes load before/after stop window
4. Validate snapshot consistency and successful container restart

## Scenario C: Database Containers with command_dumps

Create per-container source entries using `docker exec`:
```yaml
command_dumps:
  - name: pg.dump
    command: sudo docker exec <pg_container> pg_dump -U postgres -Fc <db>
  - name: maria.sql
    command: sudo docker exec <maria_container> mariadb-dump --protocol=socket --socket=/run/mysqld/mysqld.sock -u <maria_user> -p"<maria_pass>" <db>
  - name: mongo.archive.gz
    command: sudo docker exec <mongo_container> sh -lc 'mongodump --archive --gzip --db <db>'
```

Run backup and verify each artifact is present and labeled correctly.

## Run Matrix

Run at least one Docker scenario per backend:
1. **local** — full matrix (all three scenarios)
2. **rest** — at least one scenario against local `vykar-server`
3. **s3** — at least one scenario
4. **sftp** — bounded probe with timeouts (mark BLOCKED on failure)

## Common Issues

- Mixing `sudo vykar` and regular `vykar` creates root-owned repo files — reset with `sudo rm -rf` before reruns
- `rclone purge` may return `directory not found` — treat as non-fatal
- Keep SFTP failures isolated — do NOT rerun local/rest/s3 if only SFTP probe failed
- MariaDB images use `mariadb-dump` (not `mysqldump`)
- For MariaDB dumps, prefer a dedicated DB user over root to avoid auth-plugin drift across image versions

## Cleanup

1. Remove test containers and volumes:
   ```bash
   sudo docker rm -f <containers>
   sudo docker volume rm <volumes>
   ```
2. Clean remote storage with `rclone delete --rmdirs` before next scenario
