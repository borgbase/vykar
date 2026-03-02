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
