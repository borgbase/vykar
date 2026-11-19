# Fuzzing

Coverage-guided fuzz tests for vykar's parser, deserializer, and decrypt paths. Uses `cargo-fuzz` (libFuzzer) to mutate inputs toward crashes, hangs, and OOM.

## Why a standalone crate

`cargo-fuzz` requires nightly and injects its own `main()` via `libfuzzer-sys`. The `fuzz/` crate is excluded from the workspace (`exclude = ["fuzz"]` in the root `Cargo.toml`) so the main build stays on stable.

## Targets

| Target | What it fuzzes |
|--------|---------------|
| `fuzz_pack_scan` | Pack binary parser (`scan_pack_blobs_bytes`) |
| `fuzz_decompress` | LZ4/Zstd decompression (`decompress` + `decompress_metadata`) |
| `fuzz_msgpack_snapshot_meta` | `SnapshotMeta` deserialization |
| `fuzz_msgpack_index_blob` | `IndexBlob` deserialization |
| `fuzz_item_stream` | Streaming item decode (`for_each_decoded_item`) |
| `fuzz_file_cache_decode` | `FileCache::decode_from_plaintext` (custom msgpack parser) |
| `fuzz_unpack_object` | AEAD envelope parse + decrypt (legacy and context-bound paths) |
| `fuzz_index_segment` | Index segment decode (`Segment::decode`), checked to re-encode identically |
| `fuzz_index_head` | Index head decode (`IndexHead::decode`), checked to round-trip |
| `fuzz_index_base` | Index base decode (`decode_base_to_temp`), checked to re-encode identically |

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

Replay committed corpus only (no new fuzzing, fast regression check):

```
make fuzz-check
```

Run a single target:

```
cargo +nightly fuzz run fuzz_pack_scan -- -max_total_time=30 -rss_limit_mb=4096 -max_len=1048576
```

## CI

A weekly GitHub Actions workflow (`.github/workflows/fuzz.yml`) runs each target for 300s on nightly. Crash artifacts are uploaded on failure. The workflow uses the committed corpus directly -- it does not regenerate seeds.
