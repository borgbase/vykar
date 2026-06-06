# AGENTS.md — vykar

Fast, encrypted, deduplicated backup tool in Rust. Two crates: `vykar-core` (library) and `vykar-cli` (thin CLI).

## Verification

`make pre-commit` runs `fmt-check` + `lint` (clippy with `-D warnings`) + `doc-check` + `test` (`cargo test --workspace`). It is slow (~2 minutes). Run it **once** at the end of a change and capture its output to a file so downstream checks can consult the logs instead of re-running:

```
make pre-commit 2>&1 | tee /tmp/vykar-pre-commit.log
```

Do not keep re-invoking `cargo check`, `cargo test`, `cargo clippy`, or `make pre-commit` to inspect partial progress — they duplicate the same work. When iterating on a single test, run that test only (e.g. `cargo test -p vykar-core --lib tests::delete::delete_phase3_refcount_failure_returns_warning_not_error`).

## Repository on-disk layout

```
<repo>/
  config              # write-once, unencrypted msgpack: version, chunker params, pack size limits
  keys/repokey        # write-once, Argon2id-wrapped master key
  index               # mutable, encrypted: IndexBlob {generation, chunks} — the chunk index
  index.gen           # mutable, unencrypted, advisory: u64 generation hint for local cache (never trusted for writes)
  snapshots/<id>      # write-once, encrypted: SnapshotMeta — source of truth for snapshot listing
  sessions/<id>.json  # ephemeral: session presence markers (concurrent backups)
  sessions/<id>.index # ephemeral: per-session crash-recovery journals
  packs/<xx>/<id>     # write-once: compressed+encrypted chunks (256 shard dirs)
  locks/*.json        # advisory locks
```

## Architecture notes

- **Two-phase backup** enables concurrent uploads. Phase 1 (no lock) registers a session and uploads packs. Phase 2 (brief exclusive lock) reconciles the index and writes the snapshot blob as the commit point. See `backup/mod.rs` for the implementation.
- **Manifest is runtime-only**: populated from `snapshots/` blobs on open, never serialized. A local AEAD-encrypted snapshot cache avoids O(n) GETs.
- **Delete/prune ordering**: delete `snapshots/<id>` first (must succeed, failure aborts), then decrement refcounts and persist index. Crash between the two leaves inflated refcounts (safe).
- **Maintenance lock** (`with_maintenance_lock()`): acquires advisory lock, cleans stale sessions (>45 min since last refresh), refuses if any active sessions remain (`VykarError::ActiveSessions` — its inner `ActiveSessionList` carries host/pid/age for each blocking session).

## Conventions & gotchas

- **Serialization**: all wire formats use `rmp_serde` (msgpack) with positional arrays. Do **not** use `#[serde(skip_serializing_if)]` on `Item` fields — breaks positional deserialization.
- **blake2 trait ambiguity**: `Blake2bMac<U32>` has ambiguous trait methods. Always use fully-qualified `Mac::update(&mut hasher, data)` and `<KeyedBlake2b256 as KeyInit>::new_from_slice()`.
- **PlaintextEngine**: still needs a `chunk_id_key` for deterministic dedup. For unencrypted repos it's derived as `BLAKE2b(repo_id)`.
- **Output split**: `vykar-core` never prints — it communicates via return values and `Option<&mut dyn FnMut(Event)>` progress callbacks. `vykar-cli` uses stdout for results/tables and stderr for status/errors.
