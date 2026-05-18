# Configuration

Vykar is driven by a YAML configuration file. Generate a starter config with:

```bash
vykar config
```

## Config file locations

Vykar automatically finds config files in this order:

1. `--config <path>` flag
2. `VYKAR_CONFIG` environment variable
3. `./vykar.yaml` (project)
4. User config dir + `vykar/config.yaml`:
   - Unix: `$XDG_CONFIG_HOME/vykar/config.yaml` or `~/.config/vykar/config.yaml`
   - Windows: `%APPDATA%\\vykar\\config.yaml`
5. System config:
   - Unix: `/etc/vykar/config.yaml`
   - Windows: `%PROGRAMDATA%\\vykar\\config.yaml`

You can also set `VYKAR_PASSPHRASE` to supply the passphrase non-interactively.

Override the local cache directory with `cache_dir` at the top level:

```yaml
cache_dir: "/tmp/vykar-cache"
```

Defaults to the platform cache directory when omitted.

## Minimal example

A complete but minimal working config. Encryption defaults to `auto` (init benchmarks AES-256-GCM vs ChaCha20-Poly1305 and pins the repo), so you only need repositories and sources:

```yaml
repositories:
  - url: "/backup/repo"

sources:
  - "/home/user/documents"
```

Windows:

```yaml
repositories:
  - url: 'D:\Backups\repo'

sources:
  - 'C:\Users\me\Documents'
```

> **Windows paths and YAML quoting:** In YAML, double-quoted strings interpret backslashes as escape sequences — `"C:\Users\..."` will fail because `\U` is parsed as a hex escape. Use **single quotes** or **no quotes** for Windows paths:
> ```yaml
> # These work:
> - 'C:\Users\me\Documents'
> - C:\Users\me\Documents
>
> # This does NOT work:
> - "C:\Users\me\Documents"
> ```

## Repositories

**Local:**

```yaml
repositories:
  - label: "local"
    url: "/backups/repo"
    # Windows: url: 'D:\Backups\repo'
```

**S3:**

```yaml
repositories:
  - label: "s3"
    url: "s3://s3.us-east-1.amazonaws.com/my-bucket/vykar"
    region: "us-east-1"
    access_key_id: "AKIA..."
    secret_access_key: "..."
```

Each entry in the `repositories` list accepts the following fields. `url` is the only required one.

**Common fields (all backends):**

| Field | Default | Values | Description |
|-------|---------|--------|-------------|
| `url` | *(required)* | string | Repository URL or local path |
| `label` | — | string | Human label for `--repo` targeting |
| `allow_insecure_http` | `false` | bool | Allow plaintext HTTP (required for `http://` and `s3+http://` URLs) |
| `min_pack_size` | 32 MiB (33554432) | integer (bytes) | Minimum pack file size |
| `max_pack_size` | 192 MiB (201326592) | integer (bytes) | Maximum pack file size (hard ceiling: 512 MiB) |

**S3 fields:**

| Field | Default | Values | Description |
|-------|---------|--------|-------------|
| `region` | — | string | S3 region (defaults to `us-east-1` at runtime) |
| `access_key_id` | — | string | S3 access key ID |
| `secret_access_key` | — | string | S3 secret access key |
| `s3_soft_delete` | `false` | bool | Use soft delete for S3 Object Lock compatibility |

**SFTP fields:**

| Field | Default | Values | Description |
|-------|---------|--------|-------------|
| `sftp_key` | — | string | Path to SSH private key. Auto-detects `~/.ssh/{id_ed25519, id_rsa, id_ecdsa}` when omitted |
| `sftp_known_hosts` | — | string | Path to known_hosts file. Defaults to `~/.ssh/known_hosts` at runtime |
| `sftp_timeout` | — | integer (seconds, 5–300) | Per-request timeout. Defaults to 30s; clamped to 5–300s range |

**REST server fields:**

| Field | Default | Values | Description |
|-------|---------|--------|-------------|
| `access_token` | — | string | Bearer token for REST server auth |

Per-repo override sections (optional, replace top-level when set): `encryption`, `compression`, `retention`, `limits`. Per-repo-only section: `retry`. Per-repo `hooks` are additive — both global and repo hooks are kept and executed in the order described in [Execution order](#execution-order).

See [Storage Backends](backends.md) for all backend-specific options.

For remote repositories, transport is HTTPS-first by default. To intentionally use plaintext HTTP (for local/dev setups), set:

```yaml
repositories:
  - url: "http://localhost:8484"
    allow_insecure_http: true
```

For S3-compatible HTTP endpoints, use `s3+http://...` URLs with `allow_insecure_http: true`.

### Multiple repositories

Add more entries to `repositories:` to back up to multiple destinations. Top-level settings serve as defaults; each entry can override `encryption`, `compression`, `retention`, and `limits`.

```yaml
repositories:
  - label: "local"
    url: "/backups/local"

  - label: "remote"
    url: "s3://s3.us-east-1.amazonaws.com/bucket/remote"
    region: "us-east-1"
    access_key_id: "AKIA..."
    secret_access_key: "..."
    encryption:
      passcommand: "pass show vykar-remote"
    compression:
      algorithm: "zstd"             # Better ratio for remote
    retention:
      keep_daily: 30                 # Keep more on remote
    limits:
      connections: 2
      upload_mib_per_sec: 25
```

When `limits` is set on a repository entry, it replaces top-level `limits` for that repository.

By default, commands operate on all repositories. Use `--repo` / `-R` to target a single one:

```bash
vykar list --repo local
vykar list -R /backups/local
```

### Retry

Retry settings for transient remote errors. Repo-level only — there is no top-level `retry` section. Uses exponential backoff with jitter.

```yaml
repositories:
  - url: "s3://..."
    retry:
      max_retries: 5
      retry_delay_ms: 2000
```

| Field | Default | Values | Description |
|-------|---------|--------|-------------|
| `max_retries` | `5` | integer | Maximum retry attempts |
| `retry_delay_ms` | `1500` | integer (ms) | Initial delay between retries |
| `retry_max_delay_ms` | `60000` | integer (ms) | Maximum delay between retries |

> **Note:** The default (5 retries, ~1 minute of cumulative backoff on average with jitter, up to ~90s worst case) is sized to absorb a brief network gap such as WiFi reconnecting after laptop sleep. Raise `max_retries` further if you run on a flaky link; set it to `0` to fail fast for CI or scripted runs.

### 3-2-1 backup strategy

> **Tip:** Configuring both a local and a remote repository gives you a [3-2-1 backup](https://en.wikipedia.org/wiki/Backup#3-2-1_rule) setup: three copies of your data (the original files, the local backup, and the remote backup), on two different media types, with one copy offsite. The example above already achieves this.

## Sources

Sources define what to back up — filesystem paths, command output, or both. Each source entry produces one snapshot per backup run.

**Simple form:**

```yaml
sources:
  - "/home/user/documents"
  - "/home/user/photos"
  # Windows:
  # - 'C:\Users\me\Documents'
  # - 'C:\Users\me\Photos'
```

Simple entries are grouped into one source. With one simple path, the source label is derived from the directory name. With multiple simple paths, the grouped source label becomes `default`. Use rich entries if you want separate source labels or one snapshot per path.

**Rich form (single path):**

```yaml
sources:
  - label: "docs"
    path: "/home/user/documents"
    exclude: ["*.tmp", ".cache/**"]
    # exclude_if_present: [".nobackup", "CACHEDIR.TAG"]
    # one_file_system: true
    # git_ignore: false
    repos: ["main"]                  # Only back up to this repo (default: all)
    retention:
      keep_daily: 7
    hooks:
      before: "echo starting docs backup"
```

Each `path:` entry produces its own snapshot. To group multiple directories into a single snapshot, use `paths:` (plural) instead — see below.

**Rich form (multiple paths):**

Use `paths` (plural) to group several directories into a single source. An explicit `label` is required:

```yaml
sources:
  - label: "writing"
    paths:
      - "/home/user/documents"
      - "/home/user/notes"
    exclude: ["*.tmp"]
```

These directories are backed up together as one snapshot. You cannot use both `path` and `paths` on the same entry.

Inside a multi-path source, each path's contents land in the snapshot under a prefix derived from its full absolute path: leading `/` stripped on Unix, drive-letter colon dropped and backslashes converted to forward slashes on Windows. For example, `/etc` lands at `etc/…`, `/var/lib/machines/base/etc` lands at `var/lib/machines/base/etc/…`, and `C:\Users\me\docs` lands at `C/Users/me/docs/…`. This lets paths with the same basename — `paths: ["/etc", "/var/lib/machines/base/etc"]` — coexist in one source without colliding.

| Field | Default | Values | Description |
|-------|---------|--------|-------------|
| `path` | — | string | Single directory to back up (mutually exclusive with `paths`) |
| `paths` | — | list of strings | Multiple directories as one snapshot (requires `label`) |
| `label` | derived | string | Source label. Auto-derived from dir name for single path; required for multi-path and dump-only |
| `exclude` | `[]` | list of strings | Per-source exclude patterns (merged with global `exclude_patterns`) |
| `exclude_if_present` | — | list of strings | Per-source marker files. Inherits global `exclude_if_present` when omitted; **replaces** global when set |
| `one_file_system` | inherited | bool | Override global `one_file_system` |
| `git_ignore` | inherited | bool | Override global `git_ignore` |
| `xattrs` | inherited | `{enabled: bool}` | Override global `xattrs` |
| `repos` | `[]` (all) | list of strings | Restrict to named repositories |
| `retention` | inherited | object | Per-source retention policy |
| `hooks` | `{}` | object | Source-level hooks (`before`/`after`/`failed`/`finally` only) |
| `command_dumps` | `[]` | list | Command dump entries |

### Per-source overrides

Each source entry in rich form can override global settings. This lets you tailor backup behavior per directory:

```yaml
sources:
  - label: "docs"
    path: "/home/user/documents"
    exclude: ["*.tmp"]
    xattrs:
      enabled: false                 # Override top-level xattrs setting for this source
    repos: ["local"]                 # Only back up to the "local" repo
    retention:
      keep_daily: 7
      keep_weekly: 4

  - label: "photos"
    path: "/home/user/photos"
    repos: ["local", "remote"]       # Back up to both repos
    retention:
      keep_daily: 30
      keep_monthly: 12
    hooks:
      after: "echo photos backed up"
```

Per-source fields that override globals: `exclude`, `exclude_if_present`, `one_file_system`, `git_ignore`, `repos`, `retention`, `hooks`, `command_dumps`.

### Command Dumps

Capture the stdout of shell commands directly into your backup. Useful for database dumps, API exports, or any generated data that doesn't live as a regular file on disk.

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

| Field | Default | Values | Description |
|-------|---------|--------|-------------|
| `name` | *(required)* | string | Virtual filename (no `/` or `\`, no duplicates within source) |
| `command` | *(required)* | string | Shell command whose stdout is captured (run via `sh -c`) |

Output is stored as virtual files under `vykar-dumps/` in the snapshot. On restore they appear as regular files (e.g. `vykar-dumps/postgres.sql`).

To include command dumps in the same snapshot as filesystem paths, add both to one source entry:

```yaml
sources:
  - label: server
    paths:
      - /etc
      - /var/www
    command_dumps:
      - name: postgres.sql
        command: pg_dump -U myuser mydb
```

If a dump command exits with non-zero status, the backup is aborted. Any chunks already uploaded to packs remain on disk but are not added to the index; they are reclaimed on the next `vykar compact` run.

See [Backup — Command dumps](backup.md#command-dumps) for more details and [Recipes](recipes.md) for PostgreSQL, MySQL, MongoDB, and Docker examples.

## Encryption

Encryption is enabled by default (`auto` mode with Argon2id key derivation). You only need an `encryption` section to supply a passcommand, force a specific algorithm, or disable encryption.

```yaml
encryption:
  mode: "chacha20poly1305"
  passphrase: "correct-horse-battery-staple"
```

| Field | Default | Values | Description |
|-------|---------|--------|-------------|
| `mode` | `"auto"` | `"auto"`, `"aes256gcm"`, `"chacha20poly1305"`, `"none"` | Encryption algorithm. `auto` benchmarks at init |
| `passphrase` | — | string (quoted) | Inline passphrase (not recommended for production) |
| `passcommand` | — | string (quoted) | Shell command that prints the passphrase |

`none` mode requires no passphrase and creates no key file. Data is still checksummed via keyed BLAKE2b-256 chunk IDs to detect storage corruption, but is not authenticated against tampering. See [Architecture — Plaintext Mode](architecture.md#plaintext-mode-none) for details.

`passcommand` runs through the platform shell:

- Unix: `sh -c`
- Windows: `powershell -NoProfile -NonInteractive -Command`

For `vykar daemon`, encrypted repositories must have a non-interactive passphrase source available (`passcommand`, `passphrase`, or `VYKAR_PASSPHRASE`).

## Compression

LZ4 (default) is optimised for speed — even on incompressible data the overhead is negligible, and reduced I/O usually more than compensates. ZSTD gives better compression ratios at the cost of more CPU; level 3 is a good starting point. `none` disables compression entirely.

```yaml
compression:
  algorithm: "zstd"
  zstd_level: 6
```

| Field | Default | Values | Description |
|-------|---------|--------|-------------|
| `algorithm` | `"lz4"` | `"lz4"`, `"zstd"`, `"none"` | Compression algorithm |
| `zstd_level` | `3` | integer, 1–22 | Zstd compression level (only used with `zstd`). 1–3 favours speed, 6–9 balances speed and ratio, 19–22 maximises ratio at significant CPU cost. Most users should stay in the 3–6 range |

Use `--compression` on the CLI to override the configured algorithm for a single backup run:

```bash
vykar backup --compression zstd
```

## Chunker

```yaml
chunker:
  min_size: 524288      # 512 KiB
  avg_size: 2097152     # 2 MiB
  max_size: 8388608     # 8 MiB
```

| Field | Default | Values | Description |
|-------|---------|--------|-------------|
| `min_size` | 512 KiB (524288) | integer (bytes) | Minimum chunk size. Must be ≤ `avg_size` |
| `avg_size` | 2 MiB (2097152) | integer (bytes) | Average chunk size |
| `max_size` | 8 MiB (8388608) | integer (bytes, hard cap: 16 MiB) | Maximum chunk size. Clamped to 16 MiB if set higher |

## Exclude Patterns

Vykar uses [gitignore-style](https://git-scm.com/docs/gitignore#_pattern_format) patterns for file exclusion. Patterns can be set globally (`exclude_patterns`) or per-source (`exclude`); both lists are merged at runtime.

### Basic patterns

Wildcards and exact names match at any depth within a source:

```yaml
# Global excludes — apply to every source directory
exclude_patterns:
  - "*.tmp"              # any .tmp file, at any depth
  - "*.log"              # any .log file, at any depth
  - ".cache/"            # any directory named .cache (trailing / = dirs only)
  - "__pycache__/"       # same — directories only
  - ".DS_Store"          # exact filename, any depth
  - "Thumbs.db"
```

Per-source excludes target specific paths within a single source:

```yaml
sources:
  - path: "/home/user/videos"
    exclude:
      - "/TV"                          # Excludes <source>/TV
  - path: "/home/user/photos"
    exclude:
      - "/thumbnails"                  # Excludes <source>/thumbnails
      - "/My Albums"                   # Spaces in paths work fine
```

Per-source `exclude` patterns are added after global `exclude_patterns`. Both lists use the same matching rules.

### Anchoring and depth

Where a pattern matches depends on whether it contains a `/`:

- **No slash** (e.g., `*.tmp`, `TV`): matches at any depth, as if prefixed with `**/`.
- **Contains a slash** (e.g., `logs/debug`, `/Downloads`): anchored to the source root. A leading `/` is optional — `logs/debug` and `/logs/debug` behave identically.
- **Trailing `/`** (e.g., `.cache/`): only matches directories.

> **Important:** Patterns are matched against paths **relative to each source directory**, not against absolute filesystem paths. An absolute path like `/home/user/videos/TV` will not work — use per-source `exclude` with relative paths instead:
>
> ```yaml
> # WRONG — silently excludes nothing
> exclude_patterns:
>   - "/home/user/videos/TV"
>
> # CORRECT — anchored to the source root
> sources:
>   - path: "/home/user/videos"
>     exclude:
>       - "/TV"
> ```

### Negation (re-including files)

The `!` prefix overrides an earlier exclude, re-including the matched file or directory:

```yaml
exclude_patterns:
  - "*.log"
  - "!important.log"       # keep important.log despite the *.log rule
```

**Limitation:** a negation cannot re-include a file if its parent directory was already excluded. The excluded directory is never traversed, so patterns for files inside it are never evaluated. To work around this, re-include each parent directory explicitly:

```yaml
exclude_patterns:
  - "log*"                 # excludes logfiles/, logs/, logfile.log, etc.
  - "!logfiles/"           # re-include the directory so it is traversed
  - "!logfiles/logs/"      # same for the nested directory
  - "!logfile.log"         # now this re-includes matching files inside
```

### Other exclusion methods

```yaml
exclude_if_present:                  # Skip dirs containing any marker file
  - ".nobackup"
  - "CACHEDIR.TAG"
one_file_system: false               # Do not cross filesystem/mount boundaries (default false)
git_ignore: false                    # Respect .gitignore files (default false)
xattrs:                              # Extended attribute handling
  enabled: true                      # Preserve xattrs on backup/restore (default true, Unix-only)
```

| Field | Default | Values | Description |
|-------|---------|--------|-------------|
| `exclude_if_present` | `[]` | list of strings | Marker filenames — directories containing any of these are skipped |
| `one_file_system` | `false` | bool | Don't cross filesystem/mount boundaries |
| `git_ignore` | `false` | bool | Respect `.gitignore` files in source dirs |
| `xattrs.enabled` | `true` | bool | Preserve extended file attributes on backup/restore (Unix only) |

## Hostname

By default, vykar records the short system hostname (everything before the first `.`) in each snapshot. On macOS, `gethostname()` returns a network-dependent FQDN (e.g. `MyMac.local` vs `MyMac.fritz.box` depending on VPN); truncating at the first dot keeps the hostname stable across network changes. On Linux and Windows, hostnames typically have no dots, so this is a no-op.

To override the hostname recorded in snapshots:

```yaml
hostname: MyMachine
```

| Field | Default | Values | Description |
|-------|---------|--------|-------------|
| `hostname` | — | string | Override hostname in snapshots. Defaults to system short hostname at runtime |

This only affects snapshot metadata — lock files and session markers always use the raw system hostname.

## Retention

All fields optional. At least one should be set for the policy to have effect.

```yaml
retention:
  keep_daily: 7
  keep_weekly: 4
  keep_monthly: 6
  keep_within: "2d"
```

| Field | Default | Values | Description |
|-------|---------|--------|-------------|
| `keep_last` | — | integer | Keep N most recent snapshots |
| `keep_hourly` | — | integer | Keep N hourly snapshots |
| `keep_daily` | — | integer | Keep N daily snapshots |
| `keep_weekly` | — | integer | Keep N weekly snapshots |
| `keep_monthly` | — | integer | Keep N monthly snapshots |
| `keep_yearly` | — | integer | Keep N yearly snapshots |
| `keep_within` | — | duration string (`h`/`d`/`w`/`m`/`y`) | Keep all snapshots within this period. Suffixes: `h` = hours, `d` = days, `w` = weeks, `m` = months (30d), `y` = years (365d) |

## Compact

```yaml
compact:
  threshold: 30
```

| Field | Default | Values | Description |
|-------|---------|--------|-------------|
| `threshold` | `20` | number, 0–100 | Minimum % unused space to trigger repack. Reset to default if out of range |

## Check

Control the integrity check step during scheduled/daemon backup cycles. Standalone `vykar check` always runs a full 100% check regardless of these settings.

```yaml
check:
  max_percent: 10
  full_every: "30d"
```

| Field | Default | Values | Description |
|-------|---------|--------|-------------|
| `max_percent` | `0` | integer, 0–100 | % of packs/snapshots to verify per scheduled cycle. `0` = skip partial checks |
| `full_every` | `"60d"` | duration string (`s`/`m`/`h`/`d`) or `null` | Full 100% check interval. Overrides `max_percent` when due. `null` disables periodic full checks |

**How it works:** On each daemon/GUI cycle, vykar checks a local timestamp file to determine whether a full check is due. If `full_every` is due (or the timestamp is missing/corrupt), a full 100% check runs and the timestamp is updated. Otherwise, if `max_percent > 0`, a random sample of that percentage of packs and snapshots is verified. If `max_percent` is 0 and `full_every` is not yet due, the check step is skipped entirely (no index loaded).

Standalone `vykar check` always runs at 100% and does not update the daemon's timer — manual checks don't reset the schedule.

## Limits

```yaml
limits:
  connections: 4
  upload_mib_per_sec: 50
```

| Field | Default | Values | Description |
|-------|---------|--------|-------------|
| `connections` | `2` | integer, 1–16 | Parallel backend operations; also controls upload/restore concurrency |
| `threads` | `0` | integer, 0–128 | CPU worker threads. `0` = auto: local repos use ceil(cores/2) clamped to [2, 4]; remote repos use min(cores, 12). `1` = mostly sequential. Also available as `--threads` on the `backup` subcommand |
| `nice` | `0` | integer, -20–19 | Unix process niceness. `0` = unchanged. Ignored on Windows |
| `upload_mib_per_sec` | `0` | integer (MiB/s) | Upload bandwidth cap. `0` = unlimited |
| `download_mib_per_sec` | `0` | integer (MiB/s) | Download bandwidth cap. `0` = unlimited |

`limits.connections` also controls SFTP connection pool size, backup in-flight uploads, and restore reader concurrency. Internal pipeline knobs are now derived automatically from `connections` and `threads`.

## Hooks

Shell commands that run at specific points in the vykar command lifecycle. Hooks can be defined at three levels: global (top-level `hooks:`), per-repository, and per-source.

**Global / per-repository hooks** support both bare prefixes and command-specific variants:

```yaml
hooks:                               # Global hooks: run for backup/prune/check/compact
  before: "echo starting"
  after: "echo done"
  # before_backup: "echo backup starting"  # Command-specific hooks
  # failed: "notify-send 'vykar failed'"
  # finally: "cleanup.sh"
```

**Per-source hooks** only support bare prefixes (`before`, `after`, `failed`, `finally`) — command-specific variants like `before_backup` are not valid at the source level. Source hooks always run for `backup` since that is the only command that processes sources.

```yaml
sources:
  - label: immich
    path: /raid1/immich/db-backups
    hooks:
      before: '/raid1/immich/backup_db.sh'  # Correct
      # before_backup: '...'               # NOT valid here — use 'before' instead
```

### Hook types

| Hook       | Command-specific (global/repo only) | Runs when                        | Failure behavior       |
|------------|--------------------------------------|----------------------------------|------------------------|
| `before`   | `before_<cmd>`                       | Before the command               | Aborts the command     |
| `after`    | `after_<cmd>`                        | After success only               | Logged, doesn't affect result |
| `failed`   | `failed_<cmd>`                       | After failure only               | Logged, doesn't affect result |
| `finally`  | `finally_<cmd>`                      | Always, regardless of outcome    | Logged, doesn't affect result |

Hooks only run for `backup`, `prune`, `check`, and `compact`. The bare form (`before`, `after`, etc.) fires for all four commands. The command-specific form (`before_backup`, `failed_prune`, etc.) fires only for that command and is only available at the global and per-repository levels — **not** in per-source hooks.

### Execution order

1. `before` hooks run: global bare → repo bare → global specific → repo specific
2. The vykar command runs (skipped if a `before` hook fails)
3. On success: `after` hooks run (repo specific → global specific → repo bare → global bare)
   On failure: `failed` hooks run (same order)
4. `finally` hooks always run last (same order)

If a `before` hook fails, the command is skipped and both `failed` and `finally` hooks still run.

Each hook key maps to a shell command (string) or list of commands.

### Variable substitution

Hook commands support `{variable}` placeholders that are replaced before execution. Values are automatically shell-escaped.

| Variable         | Description                                  |
|------------------|----------------------------------------------|
| `{command}`      | The vykar command name (e.g. `backup`, `prune`) |
| `{repository}`   | Repository URL                               |
| `{label}`        | Repository label (empty if unset)            |
| `{error}`        | Error message (empty if no error)            |
| `{source_label}` | Source label (empty if unset)                |
| `{source_path}`  | Source path list (Unix `:`, Windows `;`)     |

The same values are also exported as environment variables: `VYKAR_COMMAND`, `VYKAR_REPOSITORY`, `VYKAR_LABEL`, `VYKAR_ERROR`, `VYKAR_SOURCE_LABEL`, `VYKAR_SOURCE_PATH`.

`{source_path}` / `VYKAR_SOURCE_PATH` joins multiple paths with `:` on Unix and `;` on Windows.

```yaml
hooks:
  failed:
    - 'notify-send "vykar {command} failed: {error}"'
  after_backup:
    - 'echo "Backed up {source_label} to {repository}"'
```

See [Recipes](recipes.md) for practical hook examples: database dumps, filesystem snapshots, network-aware backups, and monitoring notifications.

## Schedule

Configure the built-in daemon scheduler for automatic periodic backups. Used with `vykar daemon`.

```yaml
schedule:
  enabled: true
  every: "6h"
  on_startup: true
```

| Field | Default | Values | Description |
|-------|---------|--------|-------------|
| `enabled` | `false` | bool | Enable scheduled backups |
| `every` | — | duration string (`s`/`m`/`h`/`d`) | Interval between runs. Falls back to `24h` when neither `every` nor `cron` is set. Mutually exclusive with `cron` |
| `cron` | — | 5-field cron expression | Cron schedule. Mutually exclusive with `every` |
| `on_startup` | `false` | bool | Run backup immediately when daemon starts |
| `jitter_seconds` | `0` | integer | Random delay 0–N seconds added to each run |
| `passphrase_prompt_timeout_seconds` | `300` | integer (seconds) | Timeout for interactive passphrase prompts |

### Interval mode

The `every` field accepts `m` (minutes), `h` (hours), or `d` (days) suffixes; a plain integer is treated as days. If neither `every` nor `cron` is set, the default interval is `24h`.

### Cron mode

The `cron` field accepts a standard 5-field cron expression (`minute hour dom month dow`). Six-field (with seconds) and seven-field expressions are rejected.

```yaml
schedule:
  enabled: true
  cron: "0 3 * * *"          # daily at 3:00 AM
  jitter_seconds: 60
```

Common cron examples:
- `"0 3 * * *"` — daily at 3:00 AM
- `"30 2 * * 1-5"` — weekdays at 2:30 AM
- `"0 */6 * * *"` — every 6 hours on the hour
- `"0 0 * * 0"` — weekly on Sunday at midnight

`every` and `cron` are **mutually exclusive** — setting both is a configuration error.

Jitter (`jitter_seconds`) applies in both modes. In cron mode, jitter is added after the computed cron tick. Keep jitter small relative to the cron cadence to avoid skipping slots.

When multiple repositories are configured, schedule values are merged: `enabled` and `on_startup` are OR'd across repos, `jitter_seconds` and `passphrase_prompt_timeout_seconds` take the maximum, and `every` uses the shortest interval.

## Environment Variable Expansion

Config files support environment variable placeholders in values:

```yaml
repositories:
  - url: "${VYKAR_REPO_URL:-/backup/repo}"
    # access_token: "${VYKAR_ACCESS_TOKEN}"
```

Supported syntax:

- `${VAR}`: requires `VAR` to be set (hard error if missing)
- `${VAR:-default}`: uses `default` when `VAR` is unset or empty

Notes:

- Expansion runs on raw config text before YAML parsing.
- Variable names must match `[A-Za-z_][A-Za-z0-9_]*`.
- Malformed placeholders fail config loading.
- No escape syntax is supported for literal `${...}`.
- `${VAR}` in YAML comments is also expanded (since expansion runs before YAML parsing).

### Loading `.env` files

Use `env_file` to load variables from one or more files before expansion. This is useful for Docker-style `.env` files that store credentials:

```yaml
env_file: .db.env
# or multiple files:
# env_file:
#   - .db.env
#   - .app.env

repositories:
  - url: /backup/repo

sources:
  - label: databases
    command_dumps:
      - name: db.sql
        command: "mysqldump -u '${DB_USER}' -p'${DB_PASSWORD}' '${DB_DATABASE}'"
```

Where `.db.env` contains:

```
DB_USER=myuser
DB_PASSWORD=s3cret
DB_DATABASE=myapp
```

Paths are resolved relative to the config file's directory. The supported `.env` format is:

- `KEY=VALUE` — plain assignment
- `export KEY=VALUE` — `export` prefix is stripped
- `KEY="VALUE"` or `KEY='VALUE'` — quotes are stripped
- Blank lines and lines starting with `#` are skipped

### Shell expansion in `command_dumps`

Commands in `command_dumps` and `hooks` run via `sh -c`, so the shell performs its own variable expansion. There are two ways to reference variables:

| Syntax | Expanded by | On missing var |
|--------|------------|----------------|
| `${VAR}` | vykar (at config load) | Hard error |
| `$VAR` | shell (at runtime) | Empty string (silent) |

When using `env_file`, prefer `${VAR}` — vykar loads the file first, then expands the placeholder, giving you an immediate error if the variable is missing.

If you cannot use `env_file`, you can source the `.env` file directly in the command:

```yaml
command_dumps:
  - name: db.sql
    command: ". /path/to/.db.env && mysqldump -u $DB_USER -p$DB_PASSWORD $DB_DATABASE"
```

This pattern is self-contained and works without any wrapper script, but missing variables will silently produce empty strings.
