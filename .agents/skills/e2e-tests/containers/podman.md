---
name: podman
description: "Repeat Docker scenarios using Podman commands"
---

# Podman Integration

## Goal

Repeat Docker-style container workflow tests using Podman command equivalents:
1. Static data volume backup
2. Brief downtime volume backup via hooks
3. Database container command dumps

## Setup

1. Ensure Podman is installed and functional
2. If rootless mode fails (e.g., missing `newuidmap`), use `sudo podman ...`
3. Use `sudo vykar ...` when backing up paths under `/var/lib/containers/storage/volumes/...`
4. Create equivalent test containers/volumes as used in Docker tests

## Command Translation

| Docker | Podman |
|--------|--------|
| `docker run` | `podman run` |
| `docker exec` | `podman exec` |
| `docker stop` | `podman stop` |
| `docker start` | `podman start` |
| `docker volume inspect` | `podman volume inspect` |
| `docker rm` | `podman rm` |
| `docker volume rm` | `podman volume rm` |

## Scenario A: Static Volume Backup

1. Create a Podman volume and seed with test files
2. Resolve host path via `podman volume inspect`
3. Configure source with resolved path and label `podman-static`
4. Run backup and verify snapshot file set

## Scenario B: Brief Downtime Hooks

1. Configure hooks:
   - `hooks.before: sudo podman stop <container>`
   - `hooks.after: sudo podman start <container>`
2. Validate container comes back and snapshot is consistent

## Scenario C: Database Containers

Run Postgres/MariaDB/MongoDB dumps through `podman exec` command_dumps:
```yaml
command_dumps:
  - name: pg.dump
    command: sudo podman exec <pg_container> pg_dump -U postgres -Fc <db>
  - name: maria.sql
    command: sudo podman exec <maria_container> mariadb-dump --protocol=socket --socket=/run/mysqld/mysqld.sock -u <maria_user> -p"<maria_pass>" <db>
  - name: mongo.archive.gz
    command: sudo podman exec <mongo_container> sh -lc 'mongodump --archive --gzip --db <db>'
```

Validate artifact naming and size.

## Run Matrix

Run at least one Podman scenario per backend:
1. **local** — full matrix
2. **rest** — at least one scenario against local `vykar-server`
3. **s3** — at least one scenario
4. **sftp** — bounded probe with timeouts

## Common Issues

- Same permission gotchas as Docker: mixing root/non-root creates ownership issues
- Rootless Podman may not work without `newuidmap` — fallback to `sudo podman`
- `rclone purge` may return `directory not found` — treat as non-fatal
- Keep SFTP failures isolated from local/rest/s3 results
- For MariaDB dumps, prefer a dedicated DB user over root to avoid auth-plugin drift across image versions

## Cleanup

1. Remove Podman containers and volumes:
   ```bash
   sudo podman rm -f <containers>
   sudo podman volume rm <volumes>
   ```
2. Clean remote storage with `rclone delete --rmdirs` after each major run
