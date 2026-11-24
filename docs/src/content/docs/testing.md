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

The protocol simulator (`vykar-core/src/sim`, compiled for tests and with the `sim` feature) runs many logical clients over one in-memory store, `SimStore`, which logs every operation and injects faults (fail, fail after applying, delay until released, duplicate, crash). Clients run the real commands on virtual time with their own host identities. A checker judges the store and the history against the protocol's invariants: committed snapshots restore, acknowledged commits stay, nothing disappears silently, the index holds together, only allowed errors, writes only under the lock, no pack is deleted younger than 12 h or while its writer's run is live with its marker, whether a head ever named it or not, repack sources (packs the deleting run read first) excepted (invariant 10), and no backup publishes a head after a marker reap or gap with packs past the commit rule's age limit (invariant 11). Each operation record carries the store's clock (`OpRecord.at`, not part of canonical traces) for the last two. On a store that honours put-if-absent the checker also judges invariant 12 (no head replaced by other bytes, and each retained head names every live object of its predecessor; heads written by `check --repair`, the upgrade or init are exempt from the second part) and invariant 4 for every retained head (latest − 16 and newer), not only the latest.

- **Scripted scenarios** hold one client at a pause point (a gate on its n-th matching operation) while another runs, and reproduce each known hazard (`sim::hazards`, `sim::interleavings`); once a hazard is fixed its scenario becomes a regression. `SimStore` keeps its own clock, which time advances move together with the clients' and which a scenario can also step alone (an NTP correction on the server); every protocol age is read on it, so client clock skew changes nothing, and H2, H3-compact and H4 reach their damage through a store clock step. Like vykar-server, it keeps the mtime and writer of an `index.d/` or `index.h/` object when the same bytes are PUT again. Its *kind* (`StoreKind`) says what it does with heads and put-if-absent (04 step 8): `Plain` offers no put-if-absent and lets a head PUT with other bytes replace the head (the fallback, which every seed recorded before 04 step 8 runs on); `Conditional` honours put-if-absent and refuses a head overwrite with 409, as vykar-server does; `ConditionalOverwrite` honours put-if-absent but lets a plain PUT replace any object, heads included, as local, OpenSSH SFTP and AWS S3 do (between them, the path most users are on); `Ignoring` overwrites on a create (Garage: the client's check fails and it falls back); `RacyRelease` passes the check but lets a delayed create land over what is there (IDrive e2: the known hazard H-racy). The client runs vykar-storage's own check (`Verifier`) once per run. A client can be made a *lock client* (`Sim::set_lock_client`, `SimStore::handle_without_conditional`): its handles offer no put-if-absent whatever the kind, so it runs the lock protocol beside clients that have it (two clients whose verdicts differ). Every regression scenario states the kinds it holds on, and every known hazard what it does on the conditional kind (H3, H3-compact and H4 are closed there: their conditional twins are the regressions H3-conditional, H3-compact-conditional and H4-conditional). Scenarios use a plaintext repository unless they build an encrypted one (`SimConfig::encrypted`: AES-256-GCM, passphrase `SIM_PASSPHRASE`, its key wrapped with a cheap Argon2id so a search stays fast); there an index object's or pack's name is fresh on every write.
- **The driver** (`sim::driver`) runs a *case* on a fork of a template whose control snapshot `c0` was backed up more than 12 h before every case starts (so its segment is foldable and its entries sweepable, as a real repository's older snapshots are; `Template::young` leaves it young): hosts, a script of commands per client, a fault plan and a sequence of choices. The backup commit and the index write are steppable machines, so the driver interleaves the commits of several clients step by step with maintenance commands, releases of delayed writes, time jumps and crashes, one at a time, taking the next choice at every decision (0, the default, runs the first client's next step). Compact runs whole or gate-stepped (`Maintenance::Stepped`): on its own thread, held before each protocol-key operation (`sessions/`, `index.h/`, `index.d/`, `snapshots/`, `packmaps/`, the lock fence, pack deletes and the first pack-shard listing; the lock acquisition and release run inside a decision, as a backup's lock attempt does), one decision per operation; seeds recorded before 04 step 7 run it whole. Searches and random cases step it with time jumps and crashes too, but no jump is offered while a stepped compact is in flight (it holds the maintenance lock; a jump inside it would make it an aged-lock victim, which no signature describes). Delete is one decision. A case's `storage` picks the store kind (default `Plain`); on `Conditional` and `ConditionalOverwrite` the checker adds invariant 12. Its `template` picks the plaintext template (default) or the encrypted one (`Template::encrypted`, where the lock-free commits of 04 step 9 exist; object names differ between runs there, so nothing the driver decides may depend on one, and the determinism tests run on both templates), and its `lock_clients` the clients that are lock clients. A backup whose head generation another writer created backs off (`backoff`, a step of its own) and repeats its commit from the snapshot refresh. Every case ends with the checker; what it found is classified by a damage signature (two lock holders, H3's late head, ...), and damage of no known shape fails the test.
- **Exploration** (`tests::sim_explore`) generates random cases with proptest: a general family must show no damage of an unknown shape, a narrow family must find H3 and shrink it to a case with few non-default choices, and the one-hostname family (two backups on cloned machines) must stay clean; general cases are drawn on both store kinds, and H3's family on the conditional kind must show no late head's damage at all. A failing case is written as a trace (`$VYKAR_SIM_TRACE_DIR`, default `<temp>/vykar-sim`) whose first line is the case; `VYKAR_SIM_REPLAY=<trace>` with `tests::sim_driver::replay_from_env` runs it again. `VYKAR_SIM_SEED` sets the seed and `VYKAR_SIM_CASES` the number of general cases; `VYKAR_SIM_MEASURE=1` with `tests::sim_explore::measure` prints the cost per case.
- **Bounded exhaustive search** (`sim::explore`, `tests::sim_exhaustive`) runs every schedule of a small scenario with at most K preemptions: a decision that switches away from a client that could continue, reorders a client's own reads, releases a delayed write, jumps time or crashes a client. A client that finds the lock taken, or ends a command, yields for free. Pre-commit searches two backups on distinct hosts (clean), the same two on one hostname (clean since the installation ID), a backup against delete plus a gate-stepped compact (clean), and the same pair, whole and stepped, over the young template (clean, and no backup ends `StaleChunksDuringCommit`: the sweep keeps c0's young entries), at K = 1, each also on the conditional store, and the pair on the encrypted template; delete is one step. A gate-stepped search covers two raw lock acquisitions operation by operation, at K = 3.
- **Regression seeds** (`crates/vykar-core/tests/sim-regressions/*.json`, `tests::sim_regressions`) are cases with the exact damage signatures they must show and the decisions their choices took when saved, so a seed whose schedule drifted fails as such. Every seed is also replayed with its clients' wall clocks skewed by hours (`tests::sim_regressions::regression_seeds_are_skew_invariant`) and must give the same decisions, signatures and run outcomes. `VYKAR_SIM_PROMOTE=<trace>` with `tests::sim_regressions::promote_from_env` prints a seed for a trace. A seed `<name>-conditional` is the case of seed `<name>` on the conditional store, with what that store leaves of the hazard (most of the late-head seeds show nothing there; the lock's own limits remain).
- **Campaign**: `make sim-campaign` (`SIM_SECONDS`, default 3600; `SIM_TRACE_DIR`, default `target/sim-campaign`) runs wider searches (K = 2 and 3, time jumps, one fault on one operation, three clients; every scenario on both store kinds) and then random cases with seeds from the clock, on one worker per core. It fails if any case shows damage of an unknown shape, after writing its trace; each worker's case in progress is in `worker-<n>.current`, and the first trace of every signature seen is kept as `example-<signature>.trace`. CI runs it weekly (`.github/workflows/sim.yml`) and uploads the traces when it fails.

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
| `fuzz_file_cache_decode` | `FileCache::decode_from_plaintext`, `FileCache::decode_from_plaintext_skipping` | Manual msgpack marker parsing, allocation cap, legacy fallback |
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
