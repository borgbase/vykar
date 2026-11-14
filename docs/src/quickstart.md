# Quick Start

## Install

Run the install script:

```bash
curl -fsSL https://vykar.borgbase.com/install.sh | sh
```

Or download a pre-built binary from the [releases page](https://github.com/borgbase/vykar/releases). A [Docker image](install.md#docker) is also available. See [Installing](install.md) for more details.


## Create a config file

Generate a starter config file, then edit it to set your repository path and source directories:

```bash
vykar config
```

## Initialize and back up

Initialize the repository (prompts for passphrase if encrypted):

```bash
vykar init
```

Create a backup of all configured sources:

```bash
vykar backup
```

Or back up any folder directly:

```bash
vykar backup ~/Documents
```

## Inspect snapshots

List all snapshots:

```bash
vykar list
```

List files inside a snapshot (use the snapshot ID shown by `vykar list`, or `latest`):

```bash
vykar snapshot list a1b2c3d4
```

Search for a file across recent snapshots:

```bash
vykar snapshot find --name '*.txt' --since 7d
```

## Restore

Restore files from a snapshot to a directory:

```bash
vykar restore a1b2c3d4 /tmp/restored
```

## Next steps

- [Back up your repository key](init-setup.md#back-up-your-repository-key) -- the passphrase alone cannot open an encrypted repository
- [Schedule backups](daemon.md) -- run unattended with systemd, launchd, or Docker
- [Back Up](backup.md), [Restore](restore.md), and [Maintain the Repository](maintenance.md) cover each step in detail
