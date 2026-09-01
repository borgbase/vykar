# Restore a Backup

## Locate snapshots

```bash
# List all snapshots
vykar list

# List the 5 most recent snapshots
vykar list --last 5

# List snapshots for a specific source
vykar list --source docs
```

## Inspect snapshot contents

Snapshot-oriented commands take an exact snapshot ID, or `latest`.

```bash
# List files inside a snapshot
vykar snapshot list a1b2c3d4

# List with details (type, permissions, size, mtime)
vykar snapshot list a1b2c3d4 --long

# Limit listing to a subtree
vykar snapshot list a1b2c3d4 --path src

# Sort listing by size (name, size, mtime)
vykar snapshot list a1b2c3d4 --sort size
```

## Inspect snapshot metadata

```bash
vykar snapshot info a1b2c3d4
```

## Find files across snapshots

Use `snapshot find` to locate files before choosing which snapshot to restore from.

```bash
# Find PDFs modified in the last 14 days
vykar snapshot find --name '*.pdf' --since 14d

# Limit search to one source and recent snapshots
vykar snapshot find --source docs --last 10 --name '*.docx'

# Search under a subtree with case-insensitive name matching
vykar snapshot find sub --iname 'report*' --since 7d

# Combine type and size filters
vykar snapshot find --type f --larger 1M --smaller 20M --since 30d
```

- `--last` must be `>= 1`.
- `--since` accepts positive spans with suffix `h`, `d`, `w`, `m` (months), or `y` (for example: `24h`, `7d`, `2w`, `6m`, `1y`).
- `--larger` means at least this size, and `--smaller` means at most this size.

## Restore to a directory

```bash
# Restore all files from a snapshot
vykar restore a1b2c3d4 /tmp/restored

# Restore the most recent snapshot
vykar restore latest /tmp/restored
```

Restore applies extended attributes (`xattrs`) by default. Control this with the top-level `xattrs.enabled` config setting.

### Restore part of a snapshot

`--pattern` takes a single glob, matched against paths **as they are stored in the snapshot** — exactly what `vykar snapshot list` prints. Check that form before writing a pattern.

```bash
# See the stored path form first
vykar snapshot list a1b2c3d4 | head

# Restore one subtree (the directory and everything under it)
vykar restore a1b2c3d4 /tmp/restored --pattern 'reports*'

# Multi-source snapshot: paths carry the source prefix
vykar restore a1b2c3d4 /tmp/restored --pattern 'home/user/documents/reports*'

# Every PDF, at any depth
vykar restore a1b2c3d4 /tmp/restored --pattern '*.pdf'
```

- Stored paths never begin with `/`. Backing up the single source `/home/user/documents` stores `reports/q1.pdf`; with several sources each path is prefixed with the source's absolute path minus the leading `/`, giving `home/user/documents/reports/q1.pdf`.
- `*` spans `/`, so `reports*` already selects the whole subtree — no `**` needed, and `*.pdf` matches at any depth. This differs from `snapshot find --name`, where `*` stops at a path separator.
- The glob must match the whole path. `--pattern 'reports'` restores only the directory itself, empty; use `reports*` to include its contents.
- Quote the pattern, or your shell expands `*` against the current directory before vykar sees it.
- Files land under the destination at their full stored path: restoring `home/user/documents/reports*` into `/tmp/restored` gives `/tmp/restored/home/user/documents/reports/`.
- A pattern that matches nothing is not an error — restore reports `Restored: 0 files, 0 dirs, 0 symlinks`. If you see that, re-check the paths with `vykar snapshot list`.

> **Important:** patterns are matched against snapshot-relative paths, not absolute filesystem paths. `--pattern '/home/user/documents/reports/*'` silently matches nothing. Drop the leading `/` and use the form `vykar snapshot list` shows.

## Browse via WebDAV and browser UI (mount)

Browse snapshot contents via a local read-only WebDAV server. The same endpoint also serves a built-in HTML browser UI.

```bash
# Serve all snapshots (default: http://127.0.0.1:8080)
vykar mount

# Serve a single snapshot
vykar mount --snapshot a1b2c3d4

# Only snapshots from a specific source
vykar mount --source docs

# Custom listen address
vykar mount --address 127.0.0.1:9090
```

## Related pages

- [Quick Start](quickstart.md)
- [Make a Backup](backup.md)
