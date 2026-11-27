---
title: "Command Reference"
---

All available commands, grouped by when you use them. Each command and subcommand provides its own `--help` output for command-specific options, and `vykar --help` shows global options.

## Setup

See [Configure and Initialize](/init-setup).

| Command | Description |
|---------|-------------|
| `vykar config` | Generate a starter configuration file |
| `vykar init` | Initialize a new backup repository |
| `vykar info` | Show repository statistics (snapshot counts and size totals) |
| `vykar key export` | Print the passphrase-protected repository key as an armored text block for safekeeping |
| `vykar key import` | Restore the repository key from an export — works on a repository that can no longer be opened |

## Backup

See [Back Up](/backup) and [Schedule Backups](/daemon).

| Command | Description |
|---------|-------------|
| `vykar` | Run full backup process: `backup`, `prune`, `compact`, `check`. This is useful for automation. |
| `vykar backup` | Back up files to a new snapshot |
| `vykar daemon` | Run scheduled backup cycles in the foreground |

## Browse and restore

See [Restore](/restore).

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

See [Maintain the Repository](/maintenance).

| Command | Description |
|---------|-------------|
| `vykar prune` | Prune snapshots according to retention policy (the space is freed by the next `compact`) |
| `vykar compact` | Free the space delete and prune leave: mark the live snapshots, sweep unreferenced index entries, repack and delete packs |
| `vykar check` | Verify repository integrity (`--verify-data` for full content verification, `--repair` to fix problems) |
| `vykar snapshot delete` | Delete one or more snapshots (the space is freed by the next `compact`) |
| `vykar delete` | Delete an entire repository permanently |
| `vykar break-lock` | Remove any lock files left by vykar 0.24 or an interrupted upgrade (without asking), then the session markers left by killed processes (lists them and asks first) |
| `vykar upgrade` | Upgrade a v2/v3 repository to format v6 in place, writing a pack map for every existing pack (one-way; see [Upgrade the repository format](/maintenance#upgrade-the-repository-format)); `--commit-mode single|multi` switches a v6 repository's commit mode (see [Commit mode](/maintenance#commit-mode)) |

## Common flags

| Flag | Meaning |
|------|---------|
| `-c`, `--config <path>` | Config file to use, overriding `VYKAR_CONFIG` and the [default search](/configuration#config-file-locations) |
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

`vykar backup` and the default `vykar` workflow can return `3` when a backup succeeds with skipped unreadable/missing files. `vykar restore --continue-on-error` returns `3` when it left out damaged files (see [Restore from a damaged repository](/restore#restore-from-a-damaged-repository)).

A `before` hook that exits `75` [skips the run](/configuration#skipping-a-run), and that exits `0`. So `vykar backup && ping` reports success with no new snapshot; for monitoring, ping from an `after_backup` hook instead. The default `vykar` cycle also exits `0` when it skips an [unavailable local repository](/configuration#unavailable-local-repositories).
