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
| **REST server** | axum-based backup server with auth, append-only enforcement, quotas, freshness tracking, and server-side compaction |
| **REST backend** | `StorageBackend` over HTTP with range-read support |
| **Tiered dedup index** | Backup dedup via session map + xor filter + mmap dedup cache, with safe fallback to HashMap dedup mode |
| **Restore mmap cache** | Restore-cache-first item-stream lookup with safe fallback to the full index when cache entries are stale or incomplete |
| **Append-only repository layout v2** | Snapshot listing derived from immutable `snapshots/<id>` blobs; `index` stores authenticated generation and `index.gen` is an advisory cache hint |
| **Bounded parallel pipeline** | Byte-budgeted pipeline with bounded worker/upload concurrency derived from `limits.threads` and `limits.connections` |
| **Heap-backed pack assembly** | Pack writers use heap-backed buffers after the mmap path was removed for reliability on some systems |
| **cache_dir override** | Configurable root for file cache, dedup/restore/full-index caches, and preferred mmap temp-file location |
| **Parallel transforms** | rayon-backed compression/encryption within the bounded pipeline |
| **break-lock command** | Forced stale-lock cleanup for backend/object lock recovery |
| **Compact pack health accounting** | Compact analysis reports/tracks corrupt and orphan packs in addition to reclaimable dead bytes |
| **File-level cache** | inode/mtime/ctime skip for unchanged files — avoids read, chunk, compress, encrypt. Keys are 16-byte BLAKE2b path hashes (with transparent legacy migration). Stored locally under the per-repo cache root (default platform cache dir + `vykar`, or `cache_dir` override). |
| **Daemon mode** | `vykar daemon` runs scheduled backup→prune→compact→check cycles with two-stage signal handling |
| **Server-side pack verification** | `vykar check` delegates pack integrity checks to vykar-server when available; `--distrust-server` opts out |
| **Upload integrity** | REST `PUT` includes `X-Content-BLAKE2b` header; server verifies during streaming write |
| **vykar-protocol crate** | Shared wire-format types and pack/protocol version constants between client and server |
| **Type-safe SnapshotId** | Newtype for snapshot identifiers with `storage_key()` for `snapshots/<id>` objects |
