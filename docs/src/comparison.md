# Comparison

How Vykar compares with Borg, Restic, Rustic and Kopia. Other tools change, so check each project's documentation for current details.

## Workflow & UX

| Aspect | Borg | Restic | Rustic | Kopia | Vykar |
|--------|------|--------|--------|-------|-------|
| Configuration | CLI (YAML via Borgmatic) | CLI (YAML via ResticProfile) | TOML config file | JSON config + CLI policies | YAML config with env-var expansion |
| Scheduling | Via Borgmatic | Via ResticProfile | External (cron/systemd) | Built-in (interval, cron) | Built-in (`vykar daemon`) |
| Storage | borgstore + SSH RPC | Local, S3, SFTP, REST, rclone | Local, S3, SFTP, REST | Local, S3, Azure, GCS, B2, SFTP, WebDAV, Rclone | Local, S3, SFTP, REST + vykar-server |
| Automation | Via Borgmatic (hooks + DB dumps) | Via ResticProfile (hooks only) | Native hooks | Native (before/after actions) | Native hooks + generic command capture |
| Restore UX | FUSE mount + Vorta (third-party) | FUSE mount + Backrest (third-party) | FUSE mount | FUSE mount or WebDAV + built-in UI | Built-in WebDAV + desktop GUI |
| Compression | LZ4, Zstd, Zlib, LZMA, None | Zstd, None | Zstd, None | Gzip, Zstd, S2, LZ4, Deflate, Pgzip | LZ4, Zstd, None |

## Repository Operations & Recovery

| Aspect | Borg | Restic | Rustic | Kopia | Vykar |
|--------|------|--------|--------|-------|-------|
| Concurrent backups | v1: exclusive; v2: shared locks | Shared locks for backup | [Lock-free](https://rustic.cli.rs/docs/commands/backup.html) | [Concurrent multi-client](https://kopia.io/docs/advanced/architecture/) | Session-based (commit serialized) |
| Repository access | SSH, append-only | rest-server, append-only | Via rustic-server | Built-in server with ACLs | REST server, append-only, quotas |
| Crash recovery | Checkpoints, rollback | Atomic rename | Atomic rename ([caveats](https://github.com/rustic-rs/rustic/discussions/1537)) | Atomic blobs ([caveats](https://github.com/kopia/kopia/issues/4305)) | Journals + two-phase commit |
| Prune / GC safety | Exclusive lock | Exclusive lock | Two-phase delete (23h) | Time-based GC (24h min) | Session-aware lock |
| Data verification | `check --repair`, full verify | `check --read-data`, repair | Restic-compat check | Verify + optional ECC | `check --verify-data`, server offload |
| Unchanged-file reuse | Persistent local filecache (v1 repo-wide; v2 per-series) | Parent snapshot tree | Parent snapshot tree(s) | Previous snapshot manifests/dirs | Per-source local filecache with parent-snapshot fallback |

## Security Model

| Aspect | Borg | Restic | Rustic | Kopia | Vykar |
|--------|------|--------|--------|-------|-------|
| Crypto construction | [v1: AES-CTR + HMAC (E&M); v2: AEAD](https://borgbackup.readthedocs.io/en/stable/internals/security.html) | [AES-CTR + Poly1305 (E-t-M)](https://restic.readthedocs.io/en/stable/100_references.html#keys-encryption-and-mac) | AES-CTR + Poly1305 (Restic-compat) | [AES-GCM / ChaCha20 (AEAD)](https://kopia.io/docs/advanced/architecture/) | AES-GCM / ChaCha20 (AEAD, AAD) |
| Key derivation | v1: PBKDF2; v2: Argon2 | scrypt (fixed params) | scrypt (Restic-compat) | scrypt | Argon2id (fixed params) |
| Content addressing | Keyed HMAC-SHA-256 / BLAKE2b | [SHA-256](https://restic.readthedocs.io/en/stable/100_references.html#backups-and-deduplication) | SHA-256 (Restic-compat) | Keyed hash (BLAKE2B-256-128 default) | Keyed BLAKE3 (repo format v3; keyed BLAKE2b-256 in v2) |
| Key zeroization | Python GC (non-deterministic) | Go GC (non-deterministic) | Rust `zeroize` | Go GC (non-deterministic) | `ZeroizeOnDrop` on all key types |
| Implementation safety | Python + C extensions | Go (GC, bounds-checked) | Rust (minimal unsafe) | Go (GC, bounds-checked) | Rust (minimal unsafe) |

<small>

**Crypto construction**: AEAD (Authenticated Encryption with Associated Data) provides confidentiality and integrity in a single pass. Encrypt-and-MAC (E&M) and Encrypt-then-MAC (E-t-M) are older two-step constructions. Domain-separated AAD binds ciphertext to its intended object type and identity, preventing cross-object substitution.

**Content addressing**: In a confirmation-of-file attack, an adversary who knows a file's content computes its expected chunk ID to confirm the file exists in the repository. A keyed hash rules this out by construction: without the repository key the ID cannot be computed. Tools that use an unkeyed hash (Restic, Rustic) are not exposed by that choice alone — they keep the plaintext hashes inside encrypted indexes and pack headers, so the defence rests on those staying confidential rather than on the hash itself.

**Key zeroization**: `ZeroizeOnDrop` overwrites key material in memory immediately when it goes out of scope. Garbage-collected runtimes (Go, Python) may leave key bytes in memory until the GC reclaims the allocation.

</small>


## Inspired by

- [BorgBackup](https://github.com/borgbackup/borg/): architecture, chunking strategy, repository concept, and overall backup pipeline.
- [Borgmatic](https://torsion.org/borgmatic/): YAML configuration approach, pipe-based database dumps.
- [Rustic](https://github.com/rustic-rs/rustic): pack file design and architectural references from a mature Rust backup tool.
- **Name**: From Latin *vicarius* ("substitute, stand-in") — because a backup is literally a substitute for lost data.
