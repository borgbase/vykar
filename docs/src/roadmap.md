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

## Implemented

| Feature | Description |
|---------|-------------|
| **Pack files** | Chunks grouped into ~32 MiB packs with dynamic sizing, separate data/tree packs |
| **Retention policies** | `keep_daily`, `keep_weekly`, `keep_monthly`, `keep_yearly`, `keep_last`, `keep_within` |
| **snapshot delete command** | Remove individual snapshots, decrement refcounts |
| **prune command** | Apply retention policies, remove expired snapshots |
| **check command** | Structural integrity + optional `--verify-data` for full content verification |
| **Type-safe PackId** | Newtype for pack file identifiers with `storage_key()` |
| **compact command** | Rewrite packs to reclaim space from orphaned blobs after delete/prune |
| **REST server** | axum-based backup server with auth, append-only, quotas, freshness tracking, lock TTL, server-side compaction |
| **REST backend** | `StorageBackend` over HTTP with range-read support |
| **Tiered dedup index** | Backup dedup via session map + xor filter + mmap dedup cache, with safe fallback to HashMap dedup mode |
| **Restore mmap cache** | Index-light restore planning via local restore cache; fallback to filtered full-index loading when needed |
| **Incremental index update** | `save_state()` fast path merges `IndexDelta` into local full-index cache and serializes index from cache |
| **Bounded parallel pipeline** | Byte-budgeted pipeline (`pipeline_buffer_mib`) with bounded worker/upload concurrency |
| **mmap-backed pack assembly** | Data-pack assembly uses mmap-backed temp files (with fallback chain) to reduce heap residency under memory pressure |
| **cache_dir override** | Configurable root for file cache, dedup/restore/full-index caches, and preferred mmap temp-file location |
| **Parallel transforms** | rayon-backed compression/encryption within the bounded pipeline |
| **break-lock command** | Forced stale-lock cleanup for backend/object lock recovery |
| **Compact pack health accounting** | Compact analysis reports/tracks corrupt and orphan packs in addition to reclaimable dead bytes |
| **File-level cache** | inode/mtime/ctime skip for unchanged files — avoids read, chunk, compress, encrypt. Keys are 16-byte BLAKE2b path hashes (with transparent legacy migration). Stored locally under the per-repo cache root (default platform cache dir + `vykar`, or `cache_dir` override). |
| **Daemon mode** | `vykar daemon` runs scheduled backup→prune→compact→check cycles with two-stage signal handling |
| **Server-side pack verification** | `vykar check` delegates pack integrity checks to vykar-server when available; `--distrust-server` opts out |
| **Upload integrity** | REST `PUT` includes `X-Content-BLAKE2b` header; server verifies during streaming write |
| **vykar-protocol crate** | Shared wire-format types and pack/protocol version constants between client and server |
| **Type-safe SnapshotId** | Newtype for snapshot identifiers with `storage_key()` (ManifestId dropped — manifest is a singleton) |
