# AGENTS.md — vykar

Fast, encrypted, deduplicated backup tool in Rust. Nine workspace crates:
`vykar-types` (leaf: IDs, errors, hash primitives), `vykar-common`,
`vykar-crypto`, `vykar-protocol` (client<->server wire types),
`vykar-storage` (backends), `vykar-core` (library), `vykar-cli`,
`vykar-server`, `vykar-gui`. Dependency direction is
`vykar-core` -> `vykar-storage` -> `vykar-protocol` -> `vykar-types`;
`vykar-types` depends on nothing internal, so anything that must cross a layer
boundary (e.g. `HashAlgorithm`) lives there. `fuzz/` is **excluded** from the
workspace with its own lockfile — `make pre-commit` does not build it.

## Verification

`make pre-commit` runs `fmt-check` + `lint` (clippy with `-D warnings`) + `doc-check` + `test-all` (`cargo test --workspace -- --include-ignored`, matching CI). It does **not** cover `fuzz/`, which is excluded from the workspace — run `cargo check --bins` inside `fuzz/` after touching anything it uses. It is slow (~2 minutes). Run it **once** at the end of a change and capture its output to a file so downstream checks can consult the logs instead of re-running:

```
make pre-commit 2>&1 | tee /tmp/vykar-pre-commit.log
```

Do not keep re-invoking `cargo check`, `cargo test`, `cargo clippy`, or `make pre-commit` to inspect partial progress — they duplicate the same work. When iterating on a single test, run that test only (e.g. `cargo test -p vykar-core --lib tests::delete::delete_phase3_refcount_failure_returns_warning_not_error`).

## Repository on-disk layout

```
<repo>/
  config              # write-once, unencrypted msgpack: version, chunker params, pack size limits
                      # EXACTLY 7 positional fields — see "Repository format" below
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
- **PlaintextEngine**: still needs a chunk-ID key for deterministic dedup. For unencrypted repos it is derived from `repo_id` — `BLAKE2b-256(repo_id)` in format v2, `blake3::derive_key(PLAINTEXT_CHUNK_ID_KEY_CONTEXT, repo_id)` in v3. The key is not secret (`repo_id` is unencrypted in `config`); it exists so plaintext repos use the same keyed hashing path, giving corruption detection rather than tamper resistance.
- **Repository format**: `RepoConfig.version` implies the chunk/pack hash — v2 = BLAKE2b, v3 = BLAKE3 — and `RepoFormat::from_version` is the single gate. `init` always writes v3; v2 repos stay v2 for life. Do **not** append a field to `RepoConfig`: it is a positional msgpack array, and rmp-serde errors on an array *longer* than the struct, so an old binary would fail to decode before reaching the version gate. Bump the version instead. Frozen v2 fixtures live in `crates/vykar-core/tests/fixtures/` and must never be regenerated.
- **Only two hashes follow the format**: chunk IDs and pack IDs. The TOFU fingerprint, pin/check-state file names, `PathHash` (BLAKE2b-**128**), the index-cache checksum and the hard-link `chunks_fingerprint` are BLAKE2b for every version, by design — each site says so in a comment. The one exception is `derive_plaintext_chunk_id_key`, which *is* the chunk-ID key.
- **Output split**: `vykar-core` never prints — it communicates via return values and `Option<&mut dyn FnMut(Event)>` progress callbacks. `vykar-cli` uses stdout for results/tables and stderr for status/errors.
