---
title: "Testing"
---

A single bug in serialization, encryption, or reference tracking can silently destroy data. vykar's testing strategy uses layered tiers so that each tier catches a different class of defect — from logic errors in individual functions through emergent failures in multi-step workflows across storage backends.

## Unit Tests

Unit tests in `vykar-core` cover each subsystem in isolation. Fast feedback (seconds), deterministic, no I/O side effects beyond tempdirs.

| Category | Focus |
|----------|-------|
| Format & serialization | RepoObj envelope round-trips, pack header parsing, item serde |
| Chunk index | Add/remove, the mapped base table, applied segments and edits |
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
| Legacy index blob round-trip | Legacy refcounts, pack offsets, and generation numbers survive encode/decode (the v2/v3 wire format) |
| Chunker completeness & determinism | No gaps or overlaps; same input always produces same boundaries; size bounds respected; stream and slice APIs agree |
| Backup-restore round-trip | Arbitrary nested file trees (empty, small, large files; nested directories) restore byte-identical |
| Compression round-trip | `decompress(compress(codec, data)) == data` for all codecs (None, LZ4, ZSTD); output within size bound |
| IndexDelta state machine | Additions applied once; rollback restores the reused set and the additions exactly |

## Protocol Simulation

The protocol simulator (`vykar-core/src/sim`, compiled for tests and with the `sim` feature) runs many logical clients over one in-memory store, `SimStore`, which logs every operation and injects faults (fail, fail after applying, delay until released, duplicate, crash). Clients run the real commands on virtual time with their own host identities. A checker judges the store and the history against the protocol's invariants: committed snapshots restore, acknowledged commits stay, nothing disappears silently, the index holds together, only allowed errors, writes only under the lock.

- **Scripted scenarios** hold one client at a pause point (a gate on its n-th matching operation) while another runs, and reproduce each known hazard (`sim::hazards`, `sim::interleavings`); once a hazard is fixed its scenario becomes a regression.
- **The driver** (`sim::driver`) runs a *case*: hosts, a script of commands per client, a fault plan and a sequence of choices. The backup commit and the index write are steppable machines, so the driver interleaves the commits of several clients step by step with whole maintenance commands, releases of delayed writes, time jumps and crashes, one at a time, taking the next choice at every decision (0, the default, runs the first client's next step). Every case ends with the checker; what it found is classified by a damage signature (H1's two lock holders, H3's late head, ...), and damage of no known shape fails the test.
- **Exploration** (`tests::sim_explore`) generates random cases with proptest: a general family must show no damage of an unknown shape, and two narrow families must find H1 and H3 and shrink them to a case with few non-default choices. A failing case is written as a trace (`$VYKAR_SIM_TRACE_DIR`, default `<temp>/vykar-sim`) whose first line is the case; `VYKAR_SIM_REPLAY=<trace>` with `tests::sim_driver::replay_from_env` runs it again. `VYKAR_SIM_SEED` sets the seed and `VYKAR_SIM_CASES` the number of general cases; `VYKAR_SIM_MEASURE=1` with `tests::sim_explore::measure` prints the cost per case.
- **Bounded exhaustive search** (`sim::explore`, `tests::sim_exhaustive`) runs every schedule of a small scenario with at most K preemptions: a decision that switches away from a client that could continue, reorders a client's own reads, releases a delayed write, jumps time or crashes a client. A client that finds the lock taken, or ends a command, yields for free. Pre-commit searches two backups on distinct hosts (clean), the same two on one hostname (H1 and nothing else), and a backup against delete plus compact (clean), at K = 1; maintenance commands are one step each. A gate-stepped search covers two raw lock acquisitions operation by operation, at K = 3.
- **Regression seeds** (`crates/vykar-core/tests/sim-regressions/*.json`, `tests::sim_regressions`) are cases with the exact damage signatures they must show and the decisions their choices took when saved, so a seed whose schedule drifted fails as such. `VYKAR_SIM_PROMOTE=<trace>` with `tests::sim_regressions::promote_from_env` prints a seed for a trace.
- **Campaign**: `make sim-campaign` (`SIM_SECONDS`, default 3600; `SIM_TRACE_DIR`, default `target/sim-campaign`) runs wider searches (K = 2 and 3, time jumps, one fault on one operation, three clients) and then random cases with seeds from the clock, on one worker per core. It fails if any case shows damage of an unknown shape, after writing its trace; each worker's case in progress is in `worker-<n>.current`, and the first trace of every signature seen is kept as `example-<signature>.trace`. CI runs it weekly (`.github/workflows/sim.yml`) and uploads the traces when it fails.

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

25 coverage-guided fuzz targets via `cargo-fuzz` (libFuzzer). Each target feeds adversarial byte sequences into a parser, deserializer, or decrypt path, mutating from committed corpus seeds toward crashes, hangs, and OOM. Complements proptest by running for hours/days and optimizing for code-path coverage rather than round-trip invariants.

| Target | Function under test | Risk surface |
|--------|-------------------|--------------|
| `fuzz_pack_scan` | `scan_pack_blobs_bytes` | Integer overflow in length fields, truncated frames |
| `fuzz_pack_trailer` | `PackTrailer::decode_body`, `parse_footer`, `open_trailer_frame`, `read_trailer_from_pack_bytes` | Lying footer and frame lengths, entry counts, overlapping offsets; non-canonical bodies |
| `fuzz_pack_map` | `PackMap::decode_body` | Lying pack and entry counts, unsorted or overlapping entries, oversized maps; non-canonical bodies |
| `fuzz_decompress` | `decompress` + `decompress_metadata`, streaming zstd metadata reader | Decompression bombs, corrupt LZ4/Zstd frames, streaming vs buffered disagreement |
| `fuzz_msgpack_snapshot_meta` | `from_slice::<SnapshotMeta>` | Large collection size declarations |
| `fuzz_msgpack_index_blob` | `from_slice::<IndexBlob>` | Massive chunk index allocation |
| `fuzz_item_stream` | `for_each_decoded_item` | Streaming framing via `Deserializer::position()`, EOF handling |
| `fuzz_file_cache_decode` | `FileCache::decode_from_plaintext` | Manual msgpack marker parsing, allocation cap, legacy fallback |
| `fuzz_unpack_object` | `unpack_object` + `unpack_object_expect_with_context` (+ in-place) | AEAD envelope parse, nonce extraction, context/AAD wiring for chunk, index, head, base and segment objects, tag authentication |
| `fuzz_index_head` | `IndexHead::decode` | Head framing, round-trip |
| `fuzz_index_base_v2` | `validate_base_body`, `BaseWriter` | Fixed-width base body: counts against the length, sort order, pack slots, unreferenced packs; re-encode identity |
| `fuzz_index_segment_v2` | `SegmentBody::decode` | Keyed segment: header counts before allocation, sections, kinds, pack table; re-encode identity |
| `fuzz_index_object` | Store-level segment and base decode (body version 2) of `index.d/` objects | Name check, zstd streaming, decode bounds, error classification as damage |
| `fuzz_repo_config` | `RepoConfig::decode` (map and positional), `DecodedConfig::validate`, re-encode of a v6 map, pack-size targets, backup's chunker gate and both chunkers | Unauthenticated `config`: unknown versions, pack-size limits, chunker parameters reaching FastCDC, resumable chunker disagreeing with fastcdc |
| `fuzz_key_blob` | `inspect_key_blob` | Unauthenticated KDF parameters out of bounds (the KDF never runs) |
| `fuzz_restore_paths` | `sanitize_item_into`, symlink target audit over an item stream | Path traversal on restore, raw (non-UTF-8) names, against an independent model |
| `fuzz_pending_journal` | `decode_pending_journal`, recovery seeding | `sessions/<id>.index` from other clients, versioned and legacy shapes |
| `fuzz_protocol_keys` | Storage-key validators, pack-key temp paths, `parse_byte_range`, `validate_pack_header` | Server keys resolving outside the repository root, `Range` header parsing |
| `fuzz_metadata_msgpack` | `decode_metadata_msgpack::<IndexBlob>` | Streaming msgpack decode disagreeing with the buffered path |
| `fuzz_plaintext_read` | Pack scan, then `decode_chunk_into` against the unpack + decompress composition for a plaintext engine, with and without size hints | Forged chunks in unencrypted repositories, size hints changing the decoded bytes |

Corpus seeds are committed and deterministic. CI runs each target for 300 seconds weekly on nightly (`make fuzz-check` replays the corpus without new fuzzing for fast regression checks).

## Integration Tests

End-to-end tests at two levels:

- **In-process** (`vykar-core/tests/`, ~2600 lines): init → backup → list → restore → delete → prune → compact → check cycles exercising the `commands` API directly. Covers encryption modes, multi-source configs, lifecycle transitions, concurrent session logic, and crash-recovery journal round-trips.
- **CLI-level** (`vykar-cli/tests/`, ~1300 lines): spawn the `vykar` binary and assert on exit codes, stdout/stderr, and restored file content. Covers config parsing, multi-repo selection, and end-to-end command syntax.
- **Memory regression**: backup and restore of a controlled corpus with RSS sampling; asserts peak RSS stays below fixed caps (512 MiB backup, 384 MiB restore) to catch memory regressions in the pipeline.

## Scenario & Stress Tests

YAML-driven scenario runner (`scripts/testbench`) executes multi-phase workflows against all four storage backends (local, REST, S3/MinIO, SFTP).

- **Scenarios**: configurable corpus (mixed file types, sizes up to 2 GB), phases including `init → backup → verify (restore + diff) → check → churn → prune → compact → cleanup`. Churn simulation applies configurable adds, deletes, and modifications with growth caps to test incremental backup and dedup correctness over time.
- **Stress mode**: up to 1000 iterations of `backup → list → restore → verify → delete → compact → prune` with periodic `check` and optional `check --verify-data`. Catches state-accumulation bugs (leaking index entries, index bloat, stale cache entries) that only manifest after many cycles.
- **Multi-backend coverage**: ensures storage-abstraction bugs do not hide behind the local filesystem.
