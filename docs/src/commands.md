# Command Reference

All available commands, grouped by when you use them. Each command and subcommand provides its own `--help` output for command-specific options, and `vykar --help` shows global options.

## Setup

See [Configure and Initialize](init-setup.md).

| Command | Description |
|---------|-------------|
| `vykar config` | Generate a starter configuration file |
| `vykar init` | Initialize a new backup repository |
| `vykar info` | Show repository statistics (snapshot counts and size totals) |
| `vykar key export` | Print the passphrase-protected repository key as an armored text block for safekeeping |
| `vykar key import` | Restore the repository key from an export — works on a repository that can no longer be opened |

## Backup

See [Back Up](backup.md) and [Schedule Backups](daemon.md).

| Command | Description |
|---------|-------------|
| `vykar` | Run full backup process: `backup`, `prune`, `compact`, `check`. This is useful for automation. |
| `vykar backup` | Back up files to a new snapshot |
| `vykar daemon` | Run scheduled backup cycles in the foreground |

## Browse and restore

See [Restore](restore.md).

| Command | Description |
|---------|-------------|
| `vykar list` | List snapshots |
| `vykar snapshot list` | Show files and directories inside a snapshot |
| `vykar snapshot info` | Show metadata for a snapshot |
| `vykar snapshot find` | Find matching files across snapshots and show change timeline (`added`, `modified`, `unchanged`) |
| `vykar snapshot diff` | Compare changed regular files between two snapshots |
| `vykar restore` | Restore files from a snapshot |
| `vykar mount` | Browse snapshots via a local read-only WebDAV server and built-in browser UI |

## Maintenance

See [Maintain the Repository](maintenance.md).

| Command | Description |
|---------|-------------|
| `vykar prune` | Prune snapshots according to retention policy |
| `vykar compact` | Free space by repacking pack files after delete/prune |
| `vykar check` | Verify repository integrity (`--verify-data` for full content verification, `--repair` to fix problems) |
| `vykar snapshot delete` | Delete one or more snapshots |
| `vykar delete` | Delete an entire repository permanently |
| `vykar break-lock` | Remove stale repository locks left by interrupted processes when lock conflicts block operations |

## Common flags

| Flag | Meaning |
|------|---------|
| `-c`, `--config <path>` | Config file to use, overriding `VYKAR_CONFIG` and the [default search](configuration.md#config-file-locations) |
| `-R`, `--repo <label or path>` | Select one repository. See below for what happens without it |
| `-S`, `--source <label>` | Filter by source label, on the commands that work per source (`backup`, `list`, `prune`, `mount`, `snapshot find`) |
| `-v`, `-vv`, `-vvv` | Increase verbosity |

With several repositories configured and no `-R`:

- Commands that do not name a snapshot, such as `backup`, `list`, `prune`, `compact`, `check` and the bare `vykar` cycle, run against every repository in turn.
- Commands that name one snapshot (`restore`, `snapshot list`, `snapshot info`, `snapshot diff`, `snapshot delete`) probe the repositories and run in the one that holds it. If several hold a snapshot of that name, the command stops and asks for `-R`.
- `latest` is ambiguous across repositories and requires `-R`, as does deleting several snapshots in one command.
- `key export` and `key import` require `-R`: a key belongs to exactly one repository.

## Exit codes

- `0`: Success
- `1`: Error (command failed)
- `3`: Partial success (backup or restore completed, but one or more files were skipped)

`vykar backup` and the default `vykar` workflow can return `3` when a backup succeeds with skipped unreadable/missing files. `vykar restore --continue-on-error` returns `3` when it left out damaged files (see [Restore from a damaged repository](restore.md#restore-from-a-damaged-repository)).

A `before` hook that exits `75` [skips the run](configuration.md#skipping-a-run), and that exits `0`. So `vykar backup && ping` reports success with no new snapshot; for monitoring, ping from an `after_backup` hook instead.
