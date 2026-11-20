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
  index               # mutable, encrypted. v4/v5: IndexHead (small, names the live base and segments)
                      #   v2/v3: IndexBlob {generation, chunks} — the whole chunk index
  index.d/<64 hex>    # v4/v5, write-once: index base or segment, named by BLAKE3 of the stored bytes
  index.gen           # v2/v3 only, mutable, unencrypted, advisory: u64 generation hint (never trusted for writes)
  snapshots/<id>      # write-once, encrypted: SnapshotMeta — source of truth for snapshot listing
  sessions/<id>.json  # ephemeral: session presence markers (concurrent backups)
  sessions/<id>.index # ephemeral: per-session crash-recovery journals
  packs/<xx>/<id>     # write-once: compressed+encrypted chunks (256 shard dirs)
  locks/*.json        # advisory locks
```

## Architecture notes

- **Two-phase backup** enables concurrent uploads. Phase 1 (no lock) registers a session and uploads packs, ending with every pack durable (`flush_packs()` before the lock). Phase 2 (brief exclusive lock, waited for up to `COMMIT_LOCK_MAX_WAIT` = 10 min, interruptible) reconciles the index and writes the snapshot blob as the commit point. `run_against_repo` in `backup/mod.rs` drives both; the phase bodies are in `backup/phases.rs`.
- **Segmented index (v4/v5)**: every index write, under the lock, GETs the head, brings the local full cache (`full_index_cache`, matched by its BLAKE3 state-hash trailer) to it, and PUTs one segment of positional changes, then the head. Every `index.d/` read checks name and size first. Readers take no lock; a 404 on a named object makes them re-read the head: if it still names the object the object is lost (`VykarError::IndexObjectMissing`), otherwise a fold moved the head and they retry on it. A fold (hard limit on every write, soft limit at the end of compact) writes a new base and keeps the previous live set as `retired` for one more generation. Open lists snapshots before the single head GET. A legacy blob at `index`, an unreadable head or a broken chain is refused by everything except `check --repair`. See `repo/index_store.rs`, `repo/index_write.rs`.
- **Manifest is runtime-only**: populated from `snapshots/` blobs on open, never serialized. A local AEAD-encrypted snapshot cache avoids O(n) GETs.
- **Delete/prune ordering**: delete `snapshots/<id>` first (must succeed, failure aborts), then decrement refcounts and persist index. Crash between the two leaves inflated refcounts (safe).
- **Maintenance lock** (`with_maintenance_lock()`): acquires advisory lock, cleans stale sessions (>45 min since last refresh), refuses if any active sessions remain (`VykarError::ActiveSessions` — its inner `ActiveSessionList` carries host/pid/age for each blocking session).

## Conventions & gotchas

- **Serialization**: all wire formats use `rmp_serde` (msgpack) with positional arrays. Do **not** use `#[serde(skip_serializing_if)]` on `Item` fields — breaks positional deserialization.
- **blake2 trait ambiguity**: `Blake2bMac<U32>` has ambiguous trait methods. Always use fully-qualified `Mac::update(&mut hasher, data)` and `<KeyedBlake2b256 as KeyInit>::new_from_slice()`.
- **PlaintextEngine**: still needs a chunk-ID key for deterministic dedup. For unencrypted repos it is derived from `repo_id` — `BLAKE2b-256(repo_id)` in formats v2/v4, `blake3::derive_key(PLAINTEXT_CHUNK_ID_KEY_CONTEXT, repo_id)` in v3/v5 (it follows `chunk_hash()`). The key is not secret (`repo_id` is unencrypted in `config`); it exists so plaintext repos use the same keyed hashing path, giving corruption detection rather than tamper resistance. With no AEAD tag, every chunk read path (`read_chunk_at`, `read_chunks_coalesced_into`, restore's `process_read_group`) verifies the chunk ID when `!is_encrypting()`, via `repo::format::verify_chunk_id`.
- **Repository format**: `RepoConfig.version` implies the chunk/pack hash and the index layout — v2 = BLAKE2b, v3 = BLAKE3 (monolithic index); v4 = BLAKE2b, v5 = BLAKE3 (segmented index) — and `RepoFormat::from_version` is the single gate (`chunk_hash()`, `index_layout()`). `init` always writes v5. The hash never changes for a repository; `vykar upgrade` (`repo/upgrade.rs`) moves v2 → v4 and v3 → v5 in place, one-way, manual only. Tests of monolithic behaviour (`index.gen`, conditional index GET, backup fast path) must create v3 explicitly: `init_at_format`, `commands::init::run_at_format`, `tests::helpers::init_repo_v3`, `testutil::test_repo_plaintext_v3`. Do **not** append a field to `RepoConfig`: it is a positional msgpack array, and rmp-serde errors on an array *longer* than the struct, so an old binary would fail to decode before reaching the version gate. Bump the version instead. Frozen v2 fixtures live in `crates/vykar-core/tests/fixtures/` and must never be regenerated.
- **Only two hashes follow the format**: chunk IDs and pack IDs (BLAKE2b in v2/v4, BLAKE3 in v3/v5). Every auxiliary digest — the TOFU fingerprint, pin/check-state file names (`pin3.`/`check3.`), `PathHash`, the index-cache checksum, the index blob validator, the hard-link `chunks_fingerprint`, the item-stream cut-point hash — is BLAKE3 for every version. vykar's remaining BLAKE2b is the v2 format path (`derive_plaintext_chunk_id_key`, `HashAlgorithm::Blake2b`) and the REST client's `X-Content-BLAKE2b` fallback for non-pack PUTs to pre-0.20 servers. `blake2` stays in the tree regardless (`argon2`, `ssh-key`).
- **Output split**: `vykar-core` never prints — it communicates via return values and `Option<&mut dyn FnMut(Event)>` progress callbacks. `vykar-cli` uses stdout for results/tables and stderr for status/errors.
