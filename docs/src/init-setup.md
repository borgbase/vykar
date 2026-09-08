# Initialize and Set Up a Repository

## Generate a configuration file

Create a starter config

```bash
vykar config
```

Or write it to a specific path:

```bash
vykar config --dest ~/.config/vykar/config.yaml
```

## Repository format

`vykar init` always creates a **format v3** repository, whose chunk and pack IDs use BLAKE3. There is no flag or config key to choose otherwise. `vykar info` reports the format and hash of any repository.

Existing **format v2** repositories (created by vykar 0.19 and earlier, using BLAKE2b) stay v2 for life and remain fully readable and writable — nothing needs migrating. The hash is part of every chunk's dedup identity and every pack's storage key, so it cannot be changed in place.

Two things to know before creating a new repository:

- **Older vykar binaries cannot open it** (`unsupported repository version: 3`). If several machines share one repository, upgrade them together — or keep using the existing repository, which needs no coordination at all.
- **A self-hosted `vykar-server` must be upgraded first** (0.20 or later). `init` checks this up front and refuses with a clear message rather than creating a repository whose first backup would fail.

## Encryption

Encryption is enabled by default (`mode: "auto"`). During `init`, vykar benchmarks AES-256-GCM and ChaCha20-Poly1305, chooses one, and stores that concrete mode in the repository config. No config is needed unless you want to force a mode or disable encryption with `mode: "none"`.

The passphrase is requested interactively at init time. You can also supply it via:

- `VYKAR_PASSPHRASE` environment variable
- `passcommand` in the config (e.g. `passcommand: "pass show vykar"`)
- `passphrase` in the config

## Configure repositories and sources

Set the repository URL and the directories to back up:

```yaml
repositories:
  - label: "main"
    url: "/backup/repo"

sources:
  - "/home/user/documents"
  - "/home/user/photos"
```

See [Configuration](configuration.md) for all available options.

## Initialize the repository

```bash
vykar init
```

This creates the repository structure at the configured URL. For encrypted repositories, you will be prompted to enter a passphrase.

If your config has multiple repositories, use `--repo` / `-R` to initialize one entry at a time:

```bash
vykar init --repo main
```

## Validate

Confirm the repository was created:

```bash
vykar info
```

Run a first backup and check results:

```bash
vykar backup
vykar list
```

## Next steps

- [Make a Backup](backup.md)
- [Configuration](configuration.md)
