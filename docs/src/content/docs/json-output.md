---
title: "JSON Output"
---

Every command accepts `--json`, which replaces the human-readable output with one JSON document on stdout. Use it in scripts and when integrating vykar into other software.

```bash
vykar list --json | jq -r '.repositories[].result.list.snapshots[].name'
```

The flag is global, so `vykar --json list` and `vykar list --json` are the same.

## Guarantees

- stdout holds exactly one JSON document, also when the command fails. It is compact on a pipe and indented on a terminal.
- The [exit codes](/commands#exit-codes) are unchanged, and the document's `status` matches them.
- Sizes and counts are plain integers in bytes and items. Times are RFC 3339 in UTC (`2026-01-15T14:30:00.123456789Z`). Paths are arrays of strings. A missing value is `null`, never `"-"` or `""`.
- A command never prompts. Commands that ask for confirmation need their `--yes` flag (`--yes-delete-this-repo` for `delete`), and `config` needs `--dest`. Set `VYKAR_PASSPHRASE` or `passcommand` for encrypted repositories.
- stderr carries progress, warnings and error text as before. It is meant for people and logs, not for parsing.
- `version` changes only for a change that breaks existing consumers. New fields can appear at any time, so ignore fields you do not know.

`daemon` and `mount` run until stopped and reject `--json`. The daemon publishes its status as JSON at `/api/status.json` instead (see [Daemon](/daemon)).

## Document

```json
{
  "version": 1,
  "command": "list",
  "status": "ok",
  "error": null,
  "result": {},
  "repositories": [
    {
      "label": "main",
      "url": "/backup/repo",
      "status": "ok",
      "skip_reason": null,
      "error": null,
      "result": {
        "list": { "snapshots": [], "hidden": [] }
      }
    }
  ]
}
```

| Field | Meaning |
|-------|---------|
| `command` | The command that ran: `list`, `snapshot info`, `key export`, or `cycle` for the bare `vykar` |
| `status` | `ok`, `partial` (exit code 3), `error` or `interrupted` |
| `error` | An error that belongs to no single repository, such as an unknown `-R` or a missing config file |
| `result` | Results of `config`, the only command that runs without a repository |
| `repositories` | One entry per repository the command ran against, in config order |

Each repository entry has its own `status` (`ok`, `partial`, `skipped`, `error` or `interrupted`) and `error`. `skip_reason` is set when a hook [skipped the run](/configuration#skipping-a-run). A command can fail after it produced a result: `check` reports its findings in `result.check` and then sets `error`.

## Results

`result` is an object keyed by what ran. Most commands add one key; `check --repair` adds `check` and `repair`, `prune --compact` adds `prune` and `compact`, and `break-lock --sessions` adds `break_lock` and `sessions`.

| Command | Key | Contents |
|---------|-----|----------|
| `init` | `init` | `url`, `encryption`, `fingerprint` |
| `backup` | `backup` | `created` (`snapshot`, `source_label`, `source_paths`, `stats`), `skipped`, `failed`, `aborted` |
| `list` | `list` | `snapshots` (`name`, `id`, `time`, `hostname`, `label`, `source_paths`, `stats`), and `hidden` snapshots that could not be read |
| `info` | `info` | Repository format, encryption, index layout, snapshot count and sizes |
| `snapshot list` | `snapshot_list` | Array of items: `path`, `type` (`file`, `dir`, `symlink`), `mode`, `size`, `mtime`, `link_target` |
| `snapshot info` | `snapshot_info` | `name`, `hostname`, `username`, `time`, `time_end`, `duration_secs`, `label`, `source_paths`, `comment`, `stats` |
| `snapshot diff` | `snapshot_diff` | `base_snapshot`, `target_snapshot`, `entries` (`path`, `change`, `old_size`, `new_size`, `size_delta`) |
| `snapshot find` | `snapshot_find` | Array of `path` with `hits` (`snapshot`, `snapshot_time`, `type`, `size`, `mtime`, `status`) |
| `snapshot delete` | `snapshot_delete` | `dry_run`, `snapshots`, `warnings` |
| `restore` | `restore` | File, directory and link counts, `total_bytes`, `warnings`, `damaged` |
| `prune` | `prune` | `dry_run`, `kept`, `pruned`, `snapshots` (`name`, `action`, `reasons`), `warnings` |
| `compact` | `compact` | `dry_run` and the pack and chunk counts; `--report-unreferenced` reports under `compact_unreferenced` |
| `check` | `check` | Counts of what was checked and `errors` (`context`, `message`) |
| `check --repair` | `repair` | `has_data_loss`, `actions` (`action`, `description`, `data_loss`), `applied` once applied, `repair_errors` |
| `delete` | `delete` | `keys_deleted`, `unknown_entries`, `root_removed` |
| `break-lock` | `break_lock` | `locks_removed` |
| `upgrade` | `upgrade` | `outcome` (`already_current` or `upgraded`) and the upgrade's counts |
| `key export` | `key_export` | `key_copies`, `message`, and `armor` (or `path` with `-o`) |
| `key import` | `key_import` | `proof`, `copies` (`key`, `outcome`, `reason`), `restored`, `repinned` |
| `config` | `config` | `path` (top-level `result`) |
| `vykar` (bare) | `cycle` | `steps` (`step`, `status`, `message`), `duration_secs`, and `backup`, `prune`, `compact`, `check` as above |

`stats` is the same object everywhere: `nfiles`, `original_size`, `compressed_size`, `deduplicated_size`, `errors`.
