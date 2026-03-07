# CLAUDE.md — vykar

## What this project is

A fast, encrypted, deduplicated backup tool written in Rust. YAML config inspired by Borgmatic. Uses pluggable storage backends (local/S3/SFTP/REST) and its own on-disk format.

## Build & test

```bash
cargo build --release        # binary at target/release/vykar
cargo check                  # fast type-check
cargo test                   # run unit + integration tests
make fmt                     # apply rustfmt across workspace
make pre-commit              # local CI gate: fmt-check + clippy -D warnings + tests
```

Minimum Rust version: 1.88 (some deps require it). Tested on macOS (aarch64).

## Project structure

```
Cargo.toml                              # workspace root
crates/
  vykar-core/                            # library crate — all backup logic
    src/
      lib.rs                            # module re-exports
      error.rs                          # VykarError enum (thiserror)
      config.rs                         # YAML config structs (serde)
      storage/
        mod.rs                          # StorageBackend trait (get/put/delete/exists/list/get_range/create_dir)
        local_backend.rs                # Native std::fs local filesystem backend
        s3_backend.rs                   # rusty-s3 + ureq S3 backend
        runtime.rs                      # Shared tokio runtime for async-backed adapters (SFTP)
      crypto/
        mod.rs                          # CryptoEngine trait + PlaintextEngine
        aes_gcm.rs                      # AES-256-GCM implementation
        key.rs                          # MasterKey, EncryptedKey, Argon2id KDF
        chunk_id.rs                     # ChunkId — keyed BLAKE2b-256 MAC
        pack_id.rs                      # PackId — unkeyed BLAKE2b-256 of pack contents
      compress/mod.rs                   # LZ4 / ZSTD / None with 1-byte tag prefix
      chunker/mod.rs                    # FastCDC wrapper
      index/mod.rs                      # ChunkIndex — HashMap<ChunkId, ChunkIndexEntry>
      repo/
        mod.rs                          # Repository struct — init, open, store_chunk, read_chunk, save_state, commit_concurrent_session
        file_cache.rs                   # FileCache — inode/mtime skip for unchanged files
        format.rs                       # RepoObj envelope — pack_object / unpack_object
        pack.rs                         # PackWriter, PackType, pack read/write helpers
        manifest.rs                     # Manifest — runtime-only snapshot list (never serialized to storage)
        snapshot_cache.rs                # Local snapshot cache — avoids O(n) GETs on open
        lock.rs                         # Advisory locks, SessionEntry, session register/deregister/refresh, acquire_lock_with_retry
        write_session.rs                # WriteSessionState — pack writers, upload queue, IndexDelta, session journal
      snapshot/
        mod.rs                          # SnapshotMeta, SnapshotStats
        item.rs                         # Item, ItemType, ChunkRef
      commands/
        mod.rs
        init.rs                         # vykar init
        backup/                         # vykar backup (two-phase: upload + commit)
          mod.rs                        # run(), two-phase entry point, session lifecycle
          pipeline.rs                   # parallel streaming pipeline (worker threads + ByteBudget)
          sequential.rs                 # single-threaded/rayon fallback path
          walk.rs                       # filesystem walk, Item construction, soft error handling
          chunk_process.rs              # chunk preparation and worker classification
          commit.rs                     # chunk commitment to repo (shared by pipeline/sequential)
          command_dump.rs               # shell command execution and capture
          concurrency.rs                # ByteBudget, PendingFiles work queue
        list.rs                         # vykar list (snapshots or snapshot contents)
        restore.rs                      # vykar restore (restore files)
        delete.rs                       # vykar delete (remove snapshot, decrement refcounts)
        prune.rs                        # vykar prune (retention policy)
        check.rs                        # vykar check (integrity verification)
        compact.rs                      # vykar compact (repack packs to reclaim space)
        util.rs                         # open_repo variants, with_repo_lock, with_maintenance_lock
  vykar-cli/                             # binary crate — thin CLI
    src/main.rs                         # clap CLI, passphrase handling, dispatches to vykar-core commands
```

## Architecture overview

### Data flow (backup)

Backup runs in two phases so multiple clients can upload concurrently.

**Phase 1 — Upload (no exclusive lock):**

1. Register session marker at `sessions/<id>.json`, probe for active maintenance lock
2. Open repo, set per-session journal key (`sessions/<id>.index`)
3. Walk source dirs (walkdir) → apply exclude patterns (globset)
4. For each file: check file cache (device, inode, mtime, ctime, size) → on hit, reuse cached `ChunkRef`s
5. On cache miss: read → FastCDC chunk → for each chunk:
   - Compute `ChunkId` = keyed BLAKE2b-256(chunk_id_key, data)
   - Check `ChunkIndex` + pending pack writers — if exists, skip (dedup hit)
   - Compress (LZ4/ZSTD) → encrypt (AES-256-GCM) → buffer into `PackWriter`
   - When pack reaches target size → flush to `packs/<shard>/<pack_id>`
6. Serialize all `Item` structs → chunk the item stream → store item-stream chunks (tree packs)
7. Build `SnapshotMeta` with `item_ptrs` → encrypt → pack snapshot blob in memory (NOT written to storage yet)

**Phase 2 — Commit (exclusive lock, brief):**

8. Acquire advisory lock with retry (10 attempts, exponential backoff)
9. `commit_concurrent_session()`: download fresh remote index, reconcile `IndexDelta`, persist index, write `snapshots/<id>` (commit point), update local snapshot cache
10. Deregister session, release lock, delete `sessions/<id>.index`

### Repository on-disk layout

```
<repo>/
  config              # write-once, unencrypted msgpack: RepoConfig (version 2, chunker params, pack size limits)
  keys/repokey        # write-once, Argon2id-wrapped master key
  index               # mutable, encrypted: IndexBlob {generation, chunks} (ChunkIndex + generation counter)
  index.gen           # mutable, unencrypted, advisory: u64 generation hint for cache lookups
  snapshots/<id>      # write-once, encrypted: SnapshotMeta per snapshot — source of truth for snapshot listing
  sessions/<id>.json  # ephemeral: session presence markers (concurrent backups)
  sessions/<id>.index # ephemeral: per-session crash-recovery journals
  packs/<xx>/<id>     # write-once: pack files containing compressed+encrypted chunks (256 shard dirs)
  locks/*.json        # advisory locks
```

### RepoObj wire format

- Encrypted: `[1-byte type_tag][12-byte nonce][ciphertext + 16-byte GCM tag]`
- Plaintext: `[1-byte type_tag][plaintext]`

The type tag byte is used as AAD (authenticated additional data) in AES-GCM.

### Key types

- `StorageBackend` trait (storage/mod.rs) — get/put/delete/exists/list/get_range/create_dir
- `CryptoEngine` trait (crypto/mod.rs) — encrypt/decrypt/chunk_id_key
- `ChunkId` (crypto/chunk_id.rs) — 32-byte keyed BLAKE2b-MAC for content-addressed dedup
- `PackId` (crypto/pack_id.rs) — 32-byte unkeyed BLAKE2b-256, has `storage_key()` → `packs/<shard>/<hex>`, `from_hex()`, `from_storage_key()`
- `PackWriter` (repo/pack.rs) — buffers encrypted blobs and flushes them as pack files
- `PackType` (repo/pack.rs) — `Data` (file content) or `Tree` (item-stream metadata)
- `Repository` (repo/mod.rs) — central orchestrator, owns storage + crypto + manifest (runtime) + index + pack writers
- `WriteSessionState` (repo/write_session.rs) — transient backup-session state: pack writers, upload queue, `IndexDelta`, session journal
- `IndexDelta` (index/mod.rs) — accumulated index mutations during backup: `new_entries` + `refcount_bumps`; `reconcile()` merges against fresh index at commit
- `SessionEntry` (repo/lock.rs) — JSON marker at `sessions/<id>.json` for concurrent backup coordination
- `Item` (snapshot/item.rs) — single filesystem entry (file/dir/symlink)
- `Compression` enum (compress/mod.rs) — 1-byte tag prefix on compressed data

## Important conventions

- All serialization uses `rmp_serde` (msgpack). Structs serialize as positional arrays — do **not** use `#[serde(skip_serializing_if)]` on Item fields (breaks positional deserialization).
- `blake2::Blake2bMac<U32>` has ambiguous trait methods — use `Mac::update(&mut hasher, data)` and `<KeyedBlake2b256 as KeyInit>::new_from_slice()` if needed.
- The `PlaintextEngine` still needs a `chunk_id_key` for deterministic dedup. For unencrypted repos, it's derived as `BLAKE2b(repo_id)`.
- `store_chunk()` requires a `PackType` argument — use `PackType::Data` for file content and `PackType::Tree` for item-stream metadata.
- `save_state()` takes `&mut self` (not `&self`) because it flushes pending pack writes before persisting the index.
- **Two-phase backup**: Phase 1 (no lock, session marker) handles upload; Phase 2 (exclusive lock, brief) handles commit via `commit_concurrent_session()`. Multiple clients can upload concurrently. Snapshot blob is packed in Phase 1 but only written to storage in Phase 2 as the commit point.
- **Per-session crash-recovery journal** at `sessions/<id>.index`, co-located with the session marker at `sessions/<id>.json`.
- **Manifest is runtime-only**: The `Manifest` struct is an in-memory snapshot list derived from `snapshots/` blobs on open. It is never serialized to storage. A local AEAD-encrypted snapshot cache avoids O(n) GETs on every open.
- **IndexBlob**: The `index` file stores `IndexBlob { generation: u64, chunks: ChunkIndex }`. The `index.gen` sidecar is an unencrypted advisory cache hint — never trusted for writes.
- **Delete/prune ordering**: Delete `snapshots/<id>` first (must succeed, failure aborts), then decrement refcounts and persist the index. Crash between delete and index persist leaves inflated refcounts (safe).
- **Maintenance lock**: `with_maintenance_lock()` (compact/delete/prune) acquires the advisory lock, cleans stale sessions (72 h), then refuses to proceed if any active sessions remain (`VykarError::ActiveSessions`).

### Output conventions

- **stdout (`println!`)**: Command results, statistics, tables. Only in `vykar-cli`. Pipe-safe.
- **stderr (`eprintln!`)**: User-facing status, errors, warnings, prompts, progress. Only in `vykar-cli`.
- **tracing macros**: Internal diagnostics in library crates. No timestamps when stderr is a TTY; full timestamps otherwise.
- **Progress callbacks**: Core commands needing real-time status use `run_with_progress()` with `Option<&mut dyn FnMut(Event)>`.
- **No print statements in vykar-core**: Core communicates results via return values and status via callbacks.

## Dependencies (key ones)

| Purpose | Crate |
|---------|-------|
| Encryption | `aes-gcm` 0.10 |
| Chunk IDs / Pack IDs | `blake2` 0.10 (Blake2bMac, Blake2bVar) |
| KDF | `argon2` 0.5 |
| Compression | `lz4_flex` 0.11, `zstd` 0.13 |
| Chunking | `fastcdc` 3 |
| Storage | `rusty-s3` 0.8 + `ureq` 2 for S3/REST, `std::fs` for local |
| Serialization | `rmp-serde` 1, `serde_json` 1 |
| CLI | `clap` 4 (derive), `serde_yaml` 0.9 |
| Filesystem | `walkdir` 2, `globset` 0.4, `xattr` 1 |

## Release

```bash
gh workflow run release.yml                              # trigger release build
gh run watch                                             # wait for it to finish
gh run download --name linux-x86_64-unknown-linux-gnu    # download Linux binary
```

## What's not implemented yet

- `mount` command
- Async I/O
- SSH RPC protocol (use the built-in SFTP backend instead)
- Hardlinks, block/char devices, FIFOs
