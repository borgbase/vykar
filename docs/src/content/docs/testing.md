---
title: "Testing"
---

A single bug in serialization, encryption, or refcount tracking can silently destroy data. vykar's testing strategy uses layered tiers so that each tier catches a different class of defect — from logic errors in individual functions through emergent failures in multi-step workflows across storage backends.

## Unit Tests

Unit tests in `vykar-core` cover each subsystem in isolation. Fast feedback (seconds), deterministic, no I/O side effects beyond tempdirs.

| Category | Focus |
|----------|-------|
| Format & serialization | RepoObj envelope round-trips, pack header parsing, item serde |
| Chunk index | Add/remove/refcount/generation, dedup-only and tiered modes |
| Locking | Advisory lock acquire/release, stale cleanup, fence detection |
| Prune & retention | Policy evaluation (keep-last, keep-daily/weekly/monthly/yearly) |
| Repair | Plan-only vs apply modes, post-repair cleanliness assertions |
| Compact | Pack analysis, repack candidate selection, crash-safety ordering |
| Snapshot lifecycle | Manifest operations, delete ordering, multi-source configs |

## Property Tests

7 `proptest` blocks. Most run 1,000 random cases; the IndexDelta state machine runs 500, and the backup-restore round-trip — which builds a real repository per case — runs 16. These catch edge cases that hand-written examples miss — off-by-one in chunk boundaries, subtle serde field-order regressions, or nonce/context-binding failures in AEAD. Regressions are reproducible via proptest's seed persistence.

| Property | Invariant verified |
|----------|--------------------|
| Encryption round-trip | `decrypt(encrypt(P, ctx), ctx) == P` for both AES-256-GCM and ChaCha20-Poly1305; wrong-context decryption fails |
| Item serde round-trip | Arbitrary files, directories, and symlinks survive msgpack positional encode/decode |
| ChunkIndex serde round-trip | Varying refcounts, pack offsets, and generation numbers survive encode/decode |
| Chunker completeness & determinism | No gaps or overlaps; same input always produces same boundaries; size bounds respected; stream and slice APIs agree |
| Backup-restore round-trip | Arbitrary nested file trees (empty, small, large files; nested directories) restore byte-identical |
| Compression round-trip | `decompress(compress(codec, data)) == data` for all codecs (None, LZ4, ZSTD); output within size bound |
| IndexDelta state-machine | Refcount conservation after apply and after reconcile-then-apply with concurrent overlaps |

## Matrix Tests

9 corruption types tested against detection, repair, and resilience paths using `test-case` parametrization. Each corruption is applied to a known-good repository, then `check`, `repair`, and follow-up `backup` are run with assertions on the outcome.

| Corruption | `check` detects | `repair` fixes | Backup succeeds after |
|------------|:-:|:-:|:-:|
| BitFlipInPack | yes (Ok + errors) | yes | yes |
| BitFlipInBlob | yes (Ok + errors) | yes | yes |
| TruncatePack | yes (Ok + errors) | yes | yes |
| ZeroFillRegion | yes (Ok + errors) | yes | yes |
| DeletePack | yes (Ok + errors) | yes | yes |
| CorruptSnapshot | yes (Ok + errors) | yes | yes |
| DeleteIndex | yes (Ok + errors) | — | — |
| TruncateIndex | yes (Err) | not possible (Err) | — |
| CorruptConfig | yes (Err) | not possible (Err) | — |

## Fuzz Tests

20 coverage-guided fuzz targets via `cargo-fuzz` (libFuzzer). Each target feeds adversarial byte sequences into a parser, deserializer, or decrypt path, mutating from committed corpus seeds toward crashes, hangs, and OOM. Complements proptest by running for hours/days and optimizing for code-path coverage rather than round-trip invariants.

| Target | Function under test | Risk surface |
|--------|-------------------|--------------|
| `fuzz_pack_scan` | `scan_pack_blobs_bytes` | Integer overflow in length fields, truncated frames |
| `fuzz_decompress` | `decompress` + `decompress_metadata`, streaming zstd metadata reader | Decompression bombs, corrupt LZ4/Zstd frames, streaming vs buffered disagreement |
| `fuzz_msgpack_snapshot_meta` | `from_slice::<SnapshotMeta>` | Large collection size declarations |
| `fuzz_msgpack_index_blob` | `from_slice::<IndexBlob>` | Massive chunk index allocation |
| `fuzz_item_stream` | `for_each_decoded_item` | Streaming framing via `Deserializer::position()`, EOF handling |
| `fuzz_file_cache_decode` | `FileCache::decode_from_plaintext` | Manual msgpack marker parsing, allocation cap, legacy fallback |
| `fuzz_unpack_object` | `unpack_object` + `unpack_object_expect_with_context` (+ in-place) | AEAD envelope parse, nonce extraction, context/AAD wiring for chunk, index, head, base and segment objects, tag authentication |
| `fuzz_index_segment` | `Segment::decode` | Position gaps, counts, re-encode identity |
| `fuzz_index_head` | `IndexHead::decode` | Head framing, round-trip |
| `fuzz_index_base` | `index::base::decode_base_to_temp` | Sort order, pack table, varints, re-encode identity |
| `fuzz_index_apply` | `apply_to_temp` over a decoded base and 1-3 segments | Streaming apply disagreeing with the reference model, wrong declared results |
| `fuzz_index_object` | Store-level segment and base decode of `index.d/` objects | Name check, zstd streaming, error classification as damage |
| `fuzz_full_index_cache` | `MmapFullIndexCache::open_verified`, `encode_base`, apply | Local cache framing, unsorted entries behind a valid trailer |
| `fuzz_repo_config` | `RepoConfig` decode, `RepoConfig::validate`, pack-size targets, backup's chunker gate and both chunkers | Unauthenticated `config`: unknown versions, pack-size limits, chunker parameters reaching FastCDC, resumable chunker disagreeing with fastcdc |
| `fuzz_key_blob` | `inspect_key_blob` | Unauthenticated KDF parameters out of bounds (the KDF never runs) |
| `fuzz_restore_paths` | `sanitize_item_into`, symlink target audit over an item stream | Path traversal on restore, raw (non-UTF-8) names, against an independent model |
| `fuzz_pending_journal` | `decode_pending_journal`, recovery seeding | `sessions/<id>.index` from other clients, versioned and legacy shapes |
| `fuzz_protocol_keys` | Storage-key validators, pack-key temp paths, `parse_byte_range`, `validate_pack_header` | Server keys resolving outside the repository root, `Range` header parsing |
| `fuzz_metadata_msgpack` | `decode_metadata_msgpack::<IndexBlob>` | Streaming msgpack decode disagreeing with the buffered path |
| `fuzz_plaintext_read` | Pack scan, unpack, decompress, chunk-ID check for a plaintext engine | Forged chunks in unencrypted repositories, size hints changing the decoded bytes |

Corpus seeds are committed and deterministic. CI runs each target for 300 seconds weekly on nightly (`make fuzz-check` replays the corpus without new fuzzing for fast regression checks).

## Integration Tests

End-to-end tests at two levels:

- **In-process** (`vykar-core/tests/`, ~2600 lines): init → backup → list → restore → delete → prune → compact → check cycles exercising the `commands` API directly. Covers encryption modes, multi-source configs, lifecycle transitions, concurrent session logic, and crash-recovery journal round-trips.
- **CLI-level** (`vykar-cli/tests/`, ~1300 lines): spawn the `vykar` binary and assert on exit codes, stdout/stderr, and restored file content. Covers config parsing, multi-repo selection, and end-to-end command syntax.
- **Memory regression**: backup and restore of a controlled corpus with RSS sampling; asserts peak RSS stays below fixed caps (512 MiB backup, 384 MiB restore) to catch memory regressions in the pipeline.

## Scenario & Stress Tests

YAML-driven scenario runner (`scripts/testbench`) executes multi-phase workflows against all four storage backends (local, REST, S3/MinIO, SFTP).

- **Scenarios**: configurable corpus (mixed file types, sizes up to 2 GB), phases including `init → backup → verify (restore + diff) → check → churn → prune → compact → cleanup`. Churn simulation applies configurable adds, deletes, and modifications with growth caps to test incremental backup and dedup correctness over time.
- **Stress mode**: up to 1000 iterations of `backup → list → restore → verify → delete → compact → prune` with periodic `check` and optional `check --verify-data`. Catches state-accumulation bugs (leaking refcounts, index bloat, stale cache entries) that only manifest after many cycles.
- **Multi-backend coverage**: ensures storage-abstraction bugs do not hide behind the local filesystem.
