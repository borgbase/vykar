# Make a Backup

## Run a backup

Back up all configured sources to all configured repositories:

```bash
vykar backup
```

By default, Vykar preserves filesystem extended attributes (`xattrs`). Configure this globally with `xattrs.enabled`, and override per source in rich `sources` entries.

If some files are unreadable or disappear during the run (for example, permission denied or a file vanishes), Vykar skips those files, still creates the snapshot from everything else, and returns exit code `3` to indicate partial success.

## Cloud-only content on macOS (iCloud Drive, Dropbox, OneDrive)

macOS FileProvider services keep evicted ("dataless") content in the cloud, and reading it would trigger a download. Vykar never forces that download, so two kinds of content can be missing from a snapshot:

- **Cloud-only files.** Their content is carried forward from the parent snapshot when size, mtime and inode all match. When there is no prior backup to carry forward from, the file is omitted.
- **Cloud-only directories.** In some contexts — notably a backup run from a background `launchd` job — listing such a directory fails outright. Vykar skips it instead of aborting the run, which means **the entire subtree below it is absent from the snapshot**.

Both are counted and reported at the end of the run, for example:

```text
  Cloud-only files skipped (no prior backup): 128
  Cloud-only directories that could not be listed: 2 (their contents are absent from this snapshot)
```

Neither counter affects the exit code: on a machine with iCloud Drive enabled they would otherwise mark every run as partial, permanently.

To capture this content today, materialize it locally before the backup — in Finder, right-click → "Download Now", or from the shell:

```bash
brctl download ~/Documents/some/path
```

Interactive runs also list cloud-only directories fine, so a foreground `vykar backup` may capture what a background `launchd` job cannot. Explicit control is planned as the per-source `dataless: skip|hydrate|hydrate-evict` setting — see the [roadmap](roadmap.md).

## Sources and labels

In its simplest form, sources are just a list of paths:

```yaml
sources:
  - /home/user/documents
  - /home/user/photos
```

When you use multiple simple string entries, vykar groups them into one source and creates one snapshot for that grouped source. If you want separate snapshots per path, use rich entries with explicit labels.

For more complex situations you can add overrides to source groups. Each "rich" source in your config produces its own snapshot. When you use the rich source form, the `label` field gives each source a short name you can reference from the CLI:

```yaml
sources:
  - label: "photos"
    path: "/home/user/photos"
  - label: "docs"
    paths:
      - "/home/user/documents"
      - "/home/user/notes"
    exclude: ["*.tmp"]
    hooks:
      before: "echo starting docs backup"
```

Back up only a specific source by label:

```bash
vykar backup --source docs
```

When targeting a specific repository, use `--repo`:

```bash
vykar backup --repo local --source docs
```

## Ad-hoc backups

You can still do ad-hoc backups of arbitrary folders and annotate them with a label, for example before a system change:

```bash
vykar backup --label before-upgrade /var/www
```

`--label` is only valid for ad-hoc backups with explicit path arguments. For example, this is rejected:

```bash
vykar backup --label before-upgrade
```

So you can identify it later in `vykar list` output.

## List and verify snapshots

```bash
# List all snapshots
vykar list

# List the 5 most recent snapshots
vykar list --last 5

# List snapshots for a specific source
vykar list --source docs

# List files inside a snapshot by ID
vykar snapshot list a1b2c3d4

# Find recent SQL dumps across recent snapshots
vykar snapshot find --last 5 --name '*.sql'

# Find logs from one source changed in the last week
vykar snapshot find --source myapp --since 7d --iname '*.log'
```

## Command dumps

You can capture the stdout of shell commands directly into your backup using `command_dumps`. This is useful for database dumps, API exports, or any generated data that doesn't live as a regular file on disk:

```yaml
sources:
  - label: databases
    command_dumps:
      - name: postgres.sql
        command: pg_dump -U myuser mydb
      - name: redis.rdb
        command: redis-cli --rdb -
```

Each source with `command_dumps` produces its own snapshot. An explicit `label` is required.

Each command runs via `sh -c` and the captured output is stored as a virtual file under `vykar-dumps/` in the snapshot. On restore, these appear as regular files:

```text
vykar-dumps/postgres.sql
vykar-dumps/redis.rdb
```

If any command exits with a non-zero status, the backup is aborted.

## Related pages

- [Quick Start](quickstart.md)
- [Configuration](configuration.md)
- [Restore a Backup](restore.md)
