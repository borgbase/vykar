---
title: "Command Reference"
---

All available commands, grouped by when you use them. Each command and subcommand provides its own `--help` output for command-specific options, and `vykar --help` shows global options.

## Setup

See [Configure and Initialize](/init-setup).

| Command | Description |
|---------|-------------|
| `vykar config` | Generate a starter configuration file |
| `vykar init` | Initialize a new backup repository (records its [commit mode](/maintenance#commit-mode)) |
| `vykar info` | Show repository statistics (snapshot counts and size totals) and the commit mode |
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
| `vykar clear-sessions` | Remove leftover lock files under `locks/` (vykar 0.24, an interrupted upgrade) without asking, then list every session marker and remove them all after confirmation; without a terminal it fails when any marker exists. `--stale` removes only stale markers (45 minutes old on the storage's clock, or of a process gone on this machine), without asking and also without a terminal; it removes the lock files only on a v6 repository and leaves them on a v2/v3 one, where they can be live (see [Remove stale session markers](/maintenance#remove-stale-session-markers)) |
| `vykar upgrade` | Upgrade a v2/v3 repository to format v6 in place, writing a pack map for every existing pack (one-way; see [Upgrade the repository format](/maintenance#upgrade-the-repository-format)); `--commit-mode single|multi` switches a v6 repository's commit mode (see [Commit mode](/maintenance#commit-mode)) |

## Common flags

| Flag | Meaning |
|------|---------|
| `-c`, `--config <path>` | Config file to use, overriding `VYKAR_CONFIG` and the [default search](/configuration#config-file-locations) |
| `-R`, `--repo <label or path>` | Select one repository. See below for what happens without it |
| `-S`, `--source <label>` | Filter by source label, on the commands that work per source (`backup`, `list`, `prune`, `mount`, `snapshot find`) |
| `-v`, `-vv`, `-vvv` | Increase verbosity |
| `--json` | Print the result as one JSON document on stdout (`list`, `info`, `backup`, `prune`, `compact`, `check`). See [JSON output](#json-output) |

With several repositories configured and no `-R`:

- Commands that do not name a snapshot, such as `backup`, `list`, `prune`, `compact`, `check` and the bare `vykar` cycle, run against every repository in turn.
- Commands that name one snapshot (`restore`, `snapshot list`, `snapshot info`, `snapshot diff`, `snapshot delete`) probe the repositories and run in the one that holds it. If several hold a snapshot of that name, the command stops and asks for `-R`.
- `latest` is ambiguous across repositories and requires `-R`, as does deleting several snapshots in one command.
- `key export` and `key import` require `-R`: a key belongs to exactly one repository.

## JSON output

`list`, `info`, `backup`, `prune` (also with `--compact`), `compact` and `check` take `--json`. The command then prints exactly one JSON document on stdout, pretty-printed, after every repository has run. Progress, warnings, prompts, the `--- Repository: … ---` lines and errors stay on stderr, and the output of hooks goes to stderr too, so `vykar backup --json | jq` works with hooks that print. Exit codes are the same as without `--json`.

`--json` is refused, with exit code `1` and nothing on stdout, for every other command, for the bare `vykar` cycle, for `check --repair` and for `compact --report-unreferenced`. No document is printed either when the command fails before any repository runs (a configuration error, an unknown `-R`). An interrupt before the first repository starts is different: it prints a document with an empty `repos` array and exits `130`.

The document:

```json
{
  "schema": 1,
  "command": "list",
  "repos": [
    {
      "repository": { "name": "nas", "label": "nas", "url": "/mnt/nas/vykar" },
      "outcome": { "status": "ok" },
      "result": {
        "snapshots": [
          {
            "name": "a1b2c3d4",
            "time": "2026-11-27T10:00:00Z",
            "hostname": "laptop",
            "source_label": "docs",
            "source_paths": ["/home/user/docs"],
            "stats": { "nfiles": 120, "original_size": 52428800, "compressed_size": 31457280, "deduplicated_size": 1048576, "errors": 0 }
          }
        ],
        "hidden": []
      }
    }
  ]
}
```

- `repos` has one entry per repository the command ran in, in order, also when only one is configured or `-R` selects one. After an interrupt it holds the repositories reached so far.
- `repository` is `name` (the label when there is one, else the url), `label` (`null` without one) and `url`. `name` is the same value as `repos[].name` in the daemon's [`/api/status.json`](/daemon#read-only-status-page), and is what joins the two.
- `outcome` is `{"status": "ok"}`, `{"status": "partial"}` (a backup with unreadable files, exit code `3`), `{"status": "skipped", "kind": …, "message": …}` or `{"status": "failed", "kind": …, "message": …}`. `message` is the text vykar prints; `kind` is meant for scripts:
  - error kinds: `passphrase` (a wrong passphrase; once the repository has opened, the same error means a damaged object), `repo_not_found`, `interrupted`, `hook` (a `before` hook failed or could not run), `config`, `other` (also a backup with failed sources and a check that found errors);
  - skip kinds: `hook` (a `before` hook exited `75`, or every source of a backup was skipped by its hooks), `passphrase_prompt_canceled`, `repo_unavailable`, `append_only_server`, `no_retention_rules`, `backup_failed`, `backup_skipped`, `check_not_configured`, `check_not_due`.
- `result` is the command's result, or `null` when it produced none. It can be present with any `outcome.status`: a backup with failed sources still lists the snapshots it created, a check that found errors lists them, and a `prune --compact` whose compact was skipped holds the prune's result.
- `backup`: `created` (one entry per snapshot: `source_label`, `snapshot_name`, `source_paths`, `stats`), `skipped` (`source_label`, `reason`) and `failed` (`source_label`, `error`) sources, and `aborted` (`source_label`, `error`, `interrupted`, `not_attempted`) when a repository failure stopped the run after a snapshot had been created.
- `prune`: `dry_run`, `stats` (`kept`, `pruned`, `warnings`) and `entries` (every keep/prune decision, with or without `--list`). With `--compact`, `compact_outcome` is the outcome of the compact half (`null` when the prune did not succeed and compact did not run) and `compact_stats` its statistics (`null` unless the compact produced them); the entry's `outcome` is the outcome of the whole command. When the command is interrupted between the prune and the compact, `compact_outcome` is `null` although `--compact` was given.
- `compact`: `dry_run` and `stats`. `info` and `check` give their statistics directly.

Timestamps are RFC 3339 in UTC, sizes are in bytes, and absent values are `null`: every document of a schema version has the same keys, except in `outcome`, where `kind` and `message` are present only with status `skipped` or `failed`. Within a schema version fields are only added, never renamed or removed; a change that is not additive raises `schema`. Scripts should ignore fields and `kind` values they do not know.

A backup, as an example of a failed outcome with a result:

```json
{
  "schema": 1,
  "command": "backup",
  "repos": [
    {
      "repository": { "name": "/backups/repo", "label": null, "url": "/backups/repo" },
      "outcome": { "status": "failed", "kind": "other", "message": "1 of 2 source(s) failed" },
      "result": {
        "created": [ { "source_label": "docs", "snapshot_name": "a1b2c3d4", "source_paths": ["/home/user/docs"], "stats": { "nfiles": 120, "original_size": 52428800, "compressed_size": 31457280, "deduplicated_size": 1048576, "errors": 0 }, "dataless_files_skipped": 0, "dataless_dirs_skipped": 0 } ],
        "skipped": [],
        "failed": [ { "source_label": "photos", "error": { "kind": "config", "message": "…" } } ],
        "aborted": null
      }
    }
  ]
}
```

## Exit codes

- `0`: Success
- `1`: Error (command failed)
- `3`: Partial success (backup or restore completed, but one or more files were skipped)

`vykar backup` and the default `vykar` workflow can return `3` when a backup succeeds with skipped unreadable/missing files. `vykar restore --continue-on-error` returns `3` when it left out damaged files (see [Restore from a damaged repository](/restore#restore-from-a-damaged-repository)).

A `before` hook that exits `75` [skips the run](/configuration#skipping-a-run), and that exits `0`. So `vykar backup && ping` reports success with no new snapshot; for monitoring, ping from an `after_backup` hook instead. The default `vykar` cycle also exits `0` when it skips an [unavailable local repository](/configuration#unavailable-local-repositories).
