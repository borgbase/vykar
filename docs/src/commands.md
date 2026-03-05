# Command Reference

Below is a list of all available commands. Each command and subcommand provides its own `--help` output for command-specific options, and `vykar --help` shows global options.

| Command | Description |
|---------|-------------|
| `vykar` | Run full backup process: `backup`, `prune`, `compact`, `check`. This is useful for automation. |
| `vykar config` | Generate a starter configuration file |
| `vykar init` | Initialize a new backup repository |
| `vykar backup` | Back up files to a new snapshot |
| `vykar restore` | Restore files from a snapshot |
| `vykar list` | List snapshots |
| `vykar snapshot list` | Show files and directories inside a snapshot |
| `vykar snapshot info` | Show metadata for a snapshot |
| `vykar snapshot find` | Find matching files across snapshots and show change timeline (`added`, `modified`, `unchanged`) |
| `vykar snapshot delete` | Delete a specific snapshot |
| `vykar delete` | Delete an entire repository permanently |
| `vykar prune` | Prune snapshots according to retention policy |
| `vykar break-lock` | Remove stale repository locks left by interrupted processes when lock conflicts block operations |
| `vykar check` | Verify repository integrity (`--verify-data` for full content verification) |
| `vykar info` | Show repository statistics (snapshot counts and size totals) |
| `vykar compact` | Free space by repacking pack files after delete/prune |
| `vykar mount` | Browse snapshots via a local WebDAV server |

## Exit codes

- `0`: Success
- `1`: Error (command failed)
- `3`: Partial success (backup completed, but one or more files were skipped)

`vykar backup` and the default `vykar` workflow can return `3` when a backup succeeds with skipped unreadable/missing files.
