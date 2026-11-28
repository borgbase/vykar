# Fuzzing

Coverage-guided fuzz tests for vykar's parser, deserializer, and decrypt paths. Uses `cargo-fuzz` (libFuzzer) to mutate inputs toward crashes, hangs, and OOM.

## Why a standalone crate

`cargo-fuzz` requires nightly and injects its own `main()` via `libfuzzer-sys`. The `fuzz/` crate is excluded from the workspace (`exclude = ["fuzz"]` in the root `Cargo.toml`) so the main build stays on stable.

## Targets

| Target | What it fuzzes |
|--------|---------------|
| `fuzz_pack_scan` | Pack binary parser (`scan_pack_blobs_bytes`) |
| `fuzz_pack_trailer` | Pack trailer parsers (`PackTrailer::decode_body`, which must re-encode identically, `parse_footer`, `open_trailer_frame`, `read_trailer_from_pack_bytes`; the last two under a deterministic AEAD engine, so raw input reaches their framing and length checks) |
| `fuzz_pack_map` | Pack map body decoder (`PackMap::decode_body`, which must re-encode identically) |
| `fuzz_decompress` | LZ4/Zstd decompression (`decompress`, `decompress_metadata`, and the streaming zstd metadata reader against it); chunk decoding with and without a size hint must agree |
| `fuzz_msgpack_snapshot_meta` | `SnapshotMeta` deserialization |
| `fuzz_listing_ext` | Snapshot key index (`SnapshotMeta.ext`): `decode_ext`, `check_ext` against a listing built from the input, re-encode, and the writer's own index decoding and checking clean |
| `fuzz_msgpack_index_blob` | `IndexBlob` deserialization (the v2/v3 index, read only by the upgrade) |
| `fuzz_item_stream` | Streaming item decode (`for_each_decoded_item`) |
| `fuzz_file_cache_decode` | `FileCache::decode_from_plaintext` (custom msgpack parser) |
| `fuzz_unpack_object` | AEAD envelope parse + decrypt (legacy and context-bound paths, chunk/index/head/base/segment types, in-place variant) |
| `fuzz_index_head` | Index head decode (`IndexHead::decode`), checked to round-trip |
| `fuzz_index_base_v2` | Index base body version 2 (`validate_base_body`), checked to re-encode identically through `BaseWriter` |
| `fuzz_index_segment_v2` | Keyed index segment body version 2 (`SegmentBody::decode`), checked to re-encode identically |
| `fuzz_index_object` | `index.d/` objects through the store-level segment and base decoders of body version 2 (name check, unpack, zstd, codec, table install), against a buffered reference; the input is the object's plaintext, sealed with a deterministic AEAD engine |
| `fuzz_repo_config` | Repository `config` decode (map and positional, `RepoConfig::decode`) and `DecodedConfig::validate`, re-encode of a v6 map, pack-size targets, then backup's chunker gate and both chunkers (checked to cut identically) |
| `fuzz_key_blob` | Key blob screening (`inspect_key_blob`), checked against the KDF bounds; never runs the KDF |
| `fuzz_restore_paths` | Restore path sanitizer and symlink audit over an item stream, against an independent model |
| `fuzz_pending_journal` | Session journal decode (`decode_pending_journal`), re-encode and recovery seeding |
| `fuzz_protocol_keys` | Server storage-key validators, pack-key temp paths, `Range` parsing (`parse_byte_range`), `validate_pack_header` |
| `fuzz_metadata_msgpack` | Streaming `decode_metadata_msgpack::<IndexBlob>` against the buffered decode |

## Corpus

Seed files in `corpus/` are committed and deterministic. Regenerate them with:

```
cargo run --manifest-path fuzz/Cargo.toml --bin generate_corpus
```

The generator uses fixed timestamps and fixed-nonce AES-256-GCM so reruns produce byte-identical output. Crash artifacts (`artifacts/`) are gitignored.

## Local usage

Requires `cargo-fuzz` and a nightly toolchain:

```
cargo install cargo-fuzz
rustup toolchain install nightly
```

Run all targets (60s each):

```
make fuzz
```

`make fuzz-list` prints the targets `make fuzz`, `make fuzz-check` and CI run; it is every `fuzz/fuzz_targets/fuzz_*.rs`.

Replay committed corpus only (no new fuzzing, fast regression check):

```
make fuzz-check
```

Run a single target:

```
cargo +nightly fuzz run fuzz_pack_scan -- -max_total_time=30 -rss_limit_mb=4096 -max_len=1048576
```

## Long runs

`fuzz/campaign.sh [BASE_SECONDS] [TARGET...]` runs every target (or the ones named) in turn with `-fork`, keeping going after findings. Targets whose coverage was still growing at the end of the last campaign get 4× `BASE_SECONDS` (default 900, so one hour), those flat from the first minute get a third, the rest `BASE_SECONDS`: about 10 hours for all targets. The tiers live in `budget_for` in the script; `FUZZ_FLAT=1` gives every target `BASE_SECONDS`. The grown corpus, crash artifacts and per-target logs go to `$FUZZ_WORK` (default `~/fuzz-work`), never into `fuzz/corpus/`. `FUZZ_JOBS` (default 6) and `FUZZ_RSS_MB` (per worker, default 4096) size it. Rerunning continues from the grown corpus.

On a shared Linux box, run it as a capped user unit (`loginctl enable-linger` keeps it alive after logout):

```
systemd-run --user --unit=vykar-fuzz -p CPUQuota=600% -p MemoryMax=32G -p Nice=10 \
  "$PWD/fuzz/campaign.sh"                # from the repository root
journalctl --user -u vykar-fuzz -f     # progress
systemctl --user stop vykar-fuzz       # pause, e.g. for benchmarks
```

Findings land in `$FUZZ_WORK/artifacts/<target>/`. Reproduce with `cargo +nightly fuzz run <target> <artifact>`, shrink with `cargo +nightly fuzz tmin <target> <artifact>`, then turn the input into a unit test in the affected crate.

## CI

A weekly GitHub Actions workflow (`.github/workflows/fuzz.yml`) runs each target for 300s on nightly. Crash artifacts are uploaded on failure. The workflow uses the committed corpus directly -- it does not regenerate seeds.
