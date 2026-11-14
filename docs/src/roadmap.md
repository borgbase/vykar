# Roadmap

## Planned

| Feature | Description | Priority |
|---------|-------------|----------|
| **GUI Config Editing** | Structured editing of the config in the GUI, currently only via YAML | High |
| **Linux GUI packaging** | Native `.deb`/`.rpm` packages and a repository for streamlined installation | High |
| **Windows GUI packaging** | MSI installer and/or winget package for first-class Windows support | High |
| **Snapshot filtering** | By host, tag, path, date ranges | Medium |
| **Async I/O** | Non-blocking storage operations | Medium |
| **JSON output mode** | Structured JSON output for all CLI commands to enable scripting and integration with monitoring tools | Medium |
| **Per-token permissions** | Expand permissions from full/append-only to also limit reading and maintenance | Medium |
| **Special file support** | Extend `ItemType` with `BlockDevice`, `CharDevice`, `Fifo`, `Socket`; `mknod` during restore. Hard links are already backed up and restored | Medium |
| **macOS dataless file modes** | Per-source `dataless: skip\|hydrate\|hydrate-evict` to control whether cloud-only files (iCloud Drive, Dropbox, OneDrive, etc.) are skipped, hydrated for backup, or hydrated then evicted via `NSFileProviderManager`. The *skip* side has landed — skip-with-parent-reuse is the hardcoded default, unlistable cloud-only directories are non-fatal, and both are tallied and reported at the end of a run. The configurable modes (and hydration itself) have not. | Medium |
| **Nominal snapshot timestamp** | Add optional `time_nominal` to `SnapshotMeta` for the data's real-world timestamp (e.g. ZFS snapshot time), distinct from backup start/end times | Low |
