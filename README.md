# Vykar Backup

A fast, encrypted, deduplicated backup tool written in Rust centered around a friendly YAML config file.

Inspired by [BorgBackup](https://github.com/borgbackup/borg/), [Borgmatic](https://torsion.org/borgmatic/), [Restic](https://github.com/restic/restic), and [Rustic](https://github.com/rustic-rs/rustic). Vykar uses its own on-disk format and is not compatible with Borg or Restic repositories.

**⚠️ Don't use for production backups yet, but do test it along other backup tools.**

## Features

- **Storage backends** — local filesystem, S3 (any compatible provider), SFTP, dedicated REST server
- **Encryption** with AES-256-GCM or ChaCha20-Poly1305 (auto-selected) and Argon2id key derivation
- **YAML-based configuration** with multiple repositories, hooks, and command dumps for monitoring and database backups
- **Deduplication** via FastCDC content-defined chunking with a memory-optimized engine (tiered dedup index + mmap-backed pack assembly)
- **Compression** with LZ4 or Zstandard
- **Built-in WebDAV and desktop GUI** to browse and restore snapshots
- **REST server** with append-only enforcement, quotas, and server-side compaction
- **Concurrent multi-client backups** — multiple machines back up to the same repository simultaneously; only the brief commit phase is serialized
- **Built-in scheduling** via `vykar daemon` — runs backup cycles on a configurable interval (no cron needed), with an optional [read-only HTTP status page](docs/src/daemon.md#read-only-status-page)
- **Resource limits** for worker threads, backend connections, and upload/download bandwidth
- **Cross-platform** — Linux, macOS, and Windows


## Benchmarks

Vykar leads in both speed and CPU efficiency, while maintaining competitive memory usage.

![Backup Tool Benchmark](docs/src/images/benchmark.summary.png)

All benchmarks were run on the same idle Intel i7-6700 CPU @ 3.40GHz machine with 2x Samsung PM981 NVMe drives. Compression settings were chosen to keep resulting repository sizes comparable. The sample corpus is a mix of small and large files with varying compressibility. See our [benchmark script](https://github.com/borgbase/vykar/tree/main/scripts) for full details.


## Quick start

```bash
curl -fsSL https://vykar.borgbase.com/install.sh | sh
```

Or download the latest release for your platform from the [releases page](https://github.com/borgbase/vykar/releases). A [Docker image](https://vykar.borgbase.com/install#docker) is also available.

```bash
# Generate a starter config and edit it
vykar config

# Initialize the repository and run a backup
vykar init
vykar backup

# List snapshots
vykar list
```

See the [full documentation](https://vykar.borgbase.com) for storage backends, restore, maintenance, and more.


## Desktop UI

<p align="center">
  <img src="docs/src/images/gui-screenshot.png" alt="Vykar GUI" width="380">
</p>

`vykar-gui` is a Slint-based desktop app that uses `vykar-core` directly (it does not shell out to the CLI).

- Run backups on demand
- List snapshots and browse snapshot contents
- Extract snapshot contents
- Run in the system tray with periodic background backups
- Uses `vykar.yaml` as the source of truth and auto-reloads config changes

Periodic GUI scheduling is configured in `vykar.yaml` via:

```yaml
schedule:
  enabled: true
  every: "24h"
  on_startup: false
  jitter_seconds: 0
  passphrase_prompt_timeout_seconds: 300
```

## Security

To report a security vulnerability, please email [hello@borgbase.com](mailto:hello@borgbase.com).

## License

GNU General Public License v3.0
