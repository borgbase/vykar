use std::collections::HashMap;

use proptest::prelude::*;

use crate::index::{IndexBlob, LegacyChunkIndex, LegacyIndexEntry};
use crate::repo::format::{
    ObjectType, pack_object_streaming_with_context, unpack_object_expect_with_context_into,
};
use crate::snapshot::item::{ChunkRef, Item, ItemType};
use vykar_crypto::aes_gcm::Aes256GcmEngine;
use vykar_crypto::chacha20_poly1305::ChaCha20Poly1305Engine;
use vykar_types::chunk_id::ChunkId;
use vykar_types::pack_id::PackId;

// ---------------------------------------------------------------------------
// Step 3: Format-layer encryption round-trip
// ---------------------------------------------------------------------------

proptest! {
    #![proptest_config(ProptestConfig::with_cases(1000))]
    #[test]
    fn format_layer_roundtrip_aes(
        plaintext in prop::collection::vec(any::<u8>(), 0..65536),
        enc_key in prop::array::uniform32(any::<u8>()),
        cid_key in prop::array::uniform32(any::<u8>()),
        context in prop::collection::vec(any::<u8>(), 0..64),
    ) {
        let engine = Aes256GcmEngine::new(&enc_key, crate::testutil::chunk_hasher_for(cid_key));
        let packed = pack_object_streaming_with_context(
            ObjectType::ChunkData, &context, plaintext.len(), &engine,
            |buf| { buf.extend_from_slice(&plaintext); Ok(()) },
        ).unwrap();
        let mut output = Vec::new();
        unpack_object_expect_with_context_into(
            &packed, ObjectType::ChunkData, &context, &engine, &mut output,
        ).unwrap();
        prop_assert_eq!(&plaintext, &output);
    }

    #[test]
    fn format_layer_roundtrip_chacha(
        plaintext in prop::collection::vec(any::<u8>(), 0..65536),
        enc_key in prop::array::uniform32(any::<u8>()),
        cid_key in prop::array::uniform32(any::<u8>()),
        context in prop::collection::vec(any::<u8>(), 0..64),
    ) {
        let engine = ChaCha20Poly1305Engine::new(&enc_key, crate::testutil::chunk_hasher_for(cid_key));
        let packed = pack_object_streaming_with_context(
            ObjectType::ChunkData, &context, plaintext.len(), &engine,
            |buf| { buf.extend_from_slice(&plaintext); Ok(()) },
        ).unwrap();
        let mut output = Vec::new();
        unpack_object_expect_with_context_into(
            &packed, ObjectType::ChunkData, &context, &engine, &mut output,
        ).unwrap();
        prop_assert_eq!(&plaintext, &output);
    }

    #[test]
    fn format_layer_wrong_context_fails(
        plaintext in prop::collection::vec(any::<u8>(), 1..1024),
        enc_key in prop::array::uniform32(any::<u8>()),
        cid_key in prop::array::uniform32(any::<u8>()),
        ctx1 in prop::collection::vec(any::<u8>(), 1..32),
        ctx2 in prop::collection::vec(any::<u8>(), 1..32),
    ) {
        prop_assume!(ctx1 != ctx2);
        let engine = Aes256GcmEngine::new(&enc_key, crate::testutil::chunk_hasher_for(cid_key));
        let packed = pack_object_streaming_with_context(
            ObjectType::ChunkData, &ctx1, plaintext.len(), &engine,
            |buf| { buf.extend_from_slice(&plaintext); Ok(()) },
        ).unwrap();
        let mut output = Vec::new();
        let result = unpack_object_expect_with_context_into(
            &packed, ObjectType::ChunkData, &ctx2, &engine, &mut output,
        );
        prop_assert!(result.is_err());
    }
}

// ---------------------------------------------------------------------------
// Step 4a: Item serde round-trip — variant-specific strategies
// ---------------------------------------------------------------------------

fn arb_chunk_id() -> impl Strategy<Value = ChunkId> {
    prop::array::uniform32(any::<u8>()).prop_map(ChunkId::from_bytes)
}

fn arb_chunk_ref() -> impl Strategy<Value = ChunkRef> {
    (arb_chunk_id(), 1..16_777_216u32, 1..16_777_216u32).prop_map(|(id, size, csize)| ChunkRef {
        id,
        size,
        csize,
    })
}

fn arb_xattrs() -> impl Strategy<Value = Option<HashMap<String, Vec<u8>>>> {
    prop::option::of(prop::collection::hash_map(
        "[a-z.]{1,16}",
        prop::collection::vec(any::<u8>(), 0..64),
        0..8,
    ))
}

fn arb_file_item() -> impl Strategy<Value = Item> {
    (
        "[a-z/]{1,32}",
        any::<u32>(),
        any::<u32>(),
        any::<u32>(),
        prop::option::of("[a-z]{1,8}"),
        prop::option::of("[a-z]{1,8}"),
        any::<i64>(),
        prop::option::of(any::<i64>()),
        prop::option::of(any::<i64>()),
        (
            arb_xattrs(),
            any::<bool>(),
            prop::collection::vec(arb_chunk_ref(), 1..10),
        ),
    )
        .prop_map(
            |(path, mode, uid, gid, user, group, mtime, atime, ctime, (xattrs, empty, chunks))| {
                if empty {
                    Item {
                        path,
                        entry_type: ItemType::RegularFile,
                        mode,
                        uid,
                        gid,
                        user,
                        group,
                        mtime,
                        atime,
                        ctime,
                        size: 0,
                        chunks: vec![],
                        link_target: None,
                        xattrs,
                        raw_names: None,
                        hardlink: None,
                    }
                } else {
                    let size = chunks.iter().map(|c| c.size as u64).sum();
                    Item {
                        path,
                        entry_type: ItemType::RegularFile,
                        mode,
                        uid,
                        gid,
                        user,
                        group,
                        mtime,
                        atime,
                        ctime,
                        size,
                        chunks,
                        link_target: None,
                        xattrs,
                        raw_names: None,
                        hardlink: None,
                    }
                }
            },
        )
}

fn arb_dir_item() -> impl Strategy<Value = Item> {
    (
        "[a-z/]{1,32}",
        any::<u32>(),
        any::<u32>(),
        any::<u32>(),
        prop::option::of("[a-z]{1,8}"),
        prop::option::of("[a-z]{1,8}"),
        any::<i64>(),
        prop::option::of(any::<i64>()),
        prop::option::of(any::<i64>()),
        arb_xattrs(),
    )
        .prop_map(
            |(path, mode, uid, gid, user, group, mtime, atime, ctime, xattrs)| Item {
                path,
                entry_type: ItemType::Directory,
                mode,
                uid,
                gid,
                user,
                group,
                mtime,
                atime,
                ctime,
                size: 0,
                chunks: vec![],
                link_target: None,
                xattrs,
                raw_names: None,
                hardlink: None,
            },
        )
}

fn arb_symlink_item() -> impl Strategy<Value = Item> {
    (
        "[a-z/]{1,32}",
        any::<u32>(),
        any::<u32>(),
        any::<u32>(),
        prop::option::of("[a-z]{1,8}"),
        prop::option::of("[a-z]{1,8}"),
        any::<i64>(),
        prop::option::of(any::<i64>()),
        prop::option::of(any::<i64>()),
        arb_xattrs(),
        "[a-z/]{1,32}",
    )
        .prop_map(
            |(path, mode, uid, gid, user, group, mtime, atime, ctime, xattrs, target)| Item {
                path,
                entry_type: ItemType::Symlink,
                mode,
                uid,
                gid,
                user,
                group,
                mtime,
                atime,
                ctime,
                size: 0,
                chunks: vec![],
                link_target: Some(target),
                xattrs,
                raw_names: None,
                hardlink: None,
            },
        )
}

fn arb_item() -> impl Strategy<Value = Item> {
    prop_oneof![arb_file_item(), arb_dir_item(), arb_symlink_item()]
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(1000))]
    #[test]
    fn item_serde_roundtrip(item in arb_item()) {
        let bytes = rmp_serde::to_vec(&item).unwrap();
        let decoded: Item = rmp_serde::from_slice(&bytes).unwrap();
        prop_assert_eq!(item, decoded);
    }

    #[test]
    fn items_vec_serde_roundtrip(items in prop::collection::vec(arb_item(), 0..50)) {
        let bytes = rmp_serde::to_vec(&items).unwrap();
        let decoded: Vec<Item> = rmp_serde::from_slice(&bytes).unwrap();
        prop_assert_eq!(items, decoded);
    }
}

// ---------------------------------------------------------------------------
// Step 4b: legacy index blob round-trip (the v2/v3 wire format)
// ---------------------------------------------------------------------------

fn arb_pack_id() -> impl Strategy<Value = PackId> {
    prop::array::uniform32(any::<u8>()).prop_map(PackId::from_bytes)
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(1000))]
    #[test]
    fn legacy_index_blob_serde_roundtrip(
        entries in prop::collection::hash_map(
            prop::array::uniform32(any::<u8>()).prop_map(ChunkId::from_bytes),
            (0..100u32, 1..16_777_216u32, arb_pack_id(), 0..u32::MAX as u64)
                .prop_map(|(refcount, stored_size, pack_id, pack_offset)| LegacyIndexEntry {
                    refcount,
                    stored_size,
                    pack_id,
                    pack_offset,
                }),
            0..200,
        ),
        generation in any::<u64>(),
    ) {
        let mut index = LegacyChunkIndex::new();
        for (id, entry) in &entries {
            index.insert(*id, *entry);
        }

        let blob = IndexBlob { generation, chunks: index };
        let bytes = rmp_serde::to_vec(&blob).unwrap();
        let decoded: IndexBlob = rmp_serde::from_slice(&bytes).unwrap();

        prop_assert_eq!(decoded.generation, generation);
        prop_assert_eq!(decoded.chunks.len(), entries.len());
        for (id, entry) in &entries {
            prop_assert_eq!(decoded.chunks.get(id), Some(entry));
        }
    }
}

// ---------------------------------------------------------------------------
// Chunking property tests
// ---------------------------------------------------------------------------

mod chunker {
    use std::io::{Cursor, Read};

    use proptest::prelude::*;

    use crate::chunker::{ChunkReader, chunk_data, chunk_stream, chunk_stream_bounded};
    use crate::repo::ChunkerParams;

    /// Generate valid ChunkerParams values that respect fastcdc's hard bounds:
    /// MINIMUM_MIN=64, AVERAGE_MIN=256, MAXIMUM_MIN=1024.
    ///
    /// All three sizes are generated as halves and doubled, so they are always
    /// even. fastcdc 5 requires even size parameters and
    /// `ChunkerParams::validate_stored` enforces it, so an odd value is not a
    /// valid config to generate. Every bound below is itself even, so doubling
    /// a half stays inside the range.
    fn arb_chunker_config() -> impl Strategy<Value = ChunkerParams> {
        // min_size: even values 64–2048
        (32..=1024u32)
            .prop_flat_map(|half_min| {
                let min_size = half_min * 2; // 64–2048, always even
                let avg_lo = 256u32.max(min_size * 2);
                let avg_hi = min_size * 8;
                (Just(min_size), avg_lo / 2..=avg_hi / 2)
            })
            .prop_flat_map(|(min_size, half_avg)| {
                let avg_size = half_avg * 2;
                let max_lo = 1024u32.max(avg_size * 2);
                let max_hi = avg_size * 4;
                (Just(min_size), Just(avg_size), max_lo / 2..=max_hi / 2)
            })
            .prop_map(|(min_size, avg_size, half_max)| ChunkerParams {
                min_size,
                avg_size,
                max_size: half_max * 2,
            })
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(1000))]

        /// Chunks cover the input with no gaps or overlaps.
        #[test]
        fn chunker_completeness(
            data in prop::collection::vec(any::<u8>(), 0..65536),
            config in arb_chunker_config(),
        ) {
            let chunks = chunk_data(&data, &config);

            // Verify offset continuity
            let mut expected_offset = 0usize;
            for &(offset, length) in &chunks {
                prop_assert_eq!(offset, expected_offset, "gap or overlap at offset {}", offset);
                expected_offset = offset + length;
            }
            prop_assert_eq!(expected_offset, data.len(), "chunks don't cover entire input");

            // Reconstruct and compare
            let reconstructed: Vec<u8> = chunks.iter()
                .flat_map(|&(offset, length)| &data[offset..offset + length])
                .copied()
                .collect();
            prop_assert_eq!(&data, &reconstructed);
        }

        /// Same input and config always produce identical chunk boundaries.
        #[test]
        fn chunker_determinism(
            data in prop::collection::vec(any::<u8>(), 0..65536),
            config in arb_chunker_config(),
        ) {
            let run1 = chunk_data(&data, &config);
            let run2 = chunk_data(&data, &config);
            prop_assert_eq!(run1, run2);
        }

        /// Non-final chunks respect min_size <= length <= max_size.
        #[test]
        fn chunker_size_bounds(
            data in prop::collection::vec(any::<u8>(), 0..65536),
            config in arb_chunker_config(),
        ) {
            let chunks = chunk_data(&data, &config);
            let min = config.min_size as usize;
            let max = config.max_size as usize;

            for (i, &(_offset, length)) in chunks.iter().enumerate() {
                let is_last = i == chunks.len() - 1;
                if is_last {
                    prop_assert!(length <= max,
                        "last chunk {length} exceeds max_size {max}");
                } else {
                    prop_assert!(length >= min,
                        "chunk {i} length {length} < min_size {min}");
                    prop_assert!(length <= max,
                        "chunk {i} length {length} > max_size {max}");
                }
            }
        }

        /// Stream and slice APIs produce identical chunk boundaries.
        #[test]
        fn chunker_stream_matches_slice(
            data in prop::collection::vec(any::<u8>(), 0..65536),
            config in arb_chunker_config(),
            limit in 0..65536u64,
        ) {
            let slice_chunks = chunk_data(&data, &config);
            let stream_chunks: Vec<(usize, usize)> = chunk_stream(Cursor::new(&data), &config)
                .map(|result| {
                    let chunk = result.expect("stream chunking should succeed");
                    (chunk.offset as usize, chunk.length)
                })
                .collect();
            prop_assert_eq!(slice_chunks, stream_chunks);

            let bounded_chunks: Vec<_> =
                chunk_stream_bounded(Cursor::new(&data).take(limit), &config)
                    .map(|result| {
                        let chunk = result.expect("bounded stream chunking should succeed");
                        (chunk.offset as usize, chunk.length)
                    })
                    .collect();
            let expected = chunk_data(&data[..data.len().min(limit as usize)], &config);
            prop_assert_eq!(&expected, &bounded_chunks);

            let mut reader = ChunkReader::new(Cursor::new(&data).take(limit), &config);
            let mut borrowed_chunks = Vec::new();
            let mut offset = 0;
            while let Some(chunk) = reader.next_chunk().expect("bounded reader should succeed") {
                prop_assert_eq!(chunk, &data[offset..offset + chunk.len()]);
                borrowed_chunks.push((offset, chunk.len()));
                offset += chunk.len();
            }
            prop_assert_eq!(expected, borrowed_chunks);
        }

        /// Any config from `arb_chunker_config()` passes `validate_stored()`.
        #[test]
        fn valid_config_always_validates(config in arb_chunker_config()) {
            prop_assert!(
                config.validate_stored().is_ok(),
                "validate_stored() failed for {:?}",
                config
            );
        }
    }
}

// ---------------------------------------------------------------------------
// Step 5: Backup-restore round-trip — nested trees
// ---------------------------------------------------------------------------

#[cfg(test)]
mod backup_restore {
    use crate::repo::control::CancelToken;
    use proptest::prelude::*;

    use crate::commands;
    use crate::tests::helpers::{backup_single_source, init_repo, pass};

    fn arb_nested_file_tree() -> impl Strategy<Value = Vec<(String, Vec<u8>)>> {
        let path_strategy = prop_oneof![
            "[a-z]{1,6}\\.[a-z]{1,3}",
            "[a-z]{1,4}/[a-z]{1,6}\\.[a-z]{1,3}",
            "[a-z]{1,4}/[a-z]{1,4}/[a-z]{1,6}\\.[a-z]{1,3}",
        ]
        // Windows reserves these device names, even with an extension.
        .prop_filter("Windows reserved name", |p: &String| {
            p.split('/').all(|c| {
                let stem = c.split('.').next().unwrap_or(c);
                !matches!(stem, "con" | "prn" | "aux" | "nul")
            })
        });
        prop::collection::hash_map(
            path_strategy,
            prop_oneof![
                Just(vec![]),
                prop::collection::vec(any::<u8>(), 1..64),
                prop::collection::vec(any::<u8>(), 64..4096),
                prop::collection::vec(any::<u8>(), 4096..65536),
            ],
            1..20,
        )
        .prop_map(|m| m.into_iter().collect::<Vec<_>>())
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(16))]
        #[test]
        #[ignore] // run with: cargo test -- --ignored
        fn backup_restore_nested_roundtrip(files in arb_nested_file_tree()) {
            let tmp = tempfile::tempdir().unwrap();
            let repo_dir = tmp.path().join("repo");
            let source_dir = tmp.path().join("source");
            let restore_dir = tmp.path().join("restore");

            // Create nested directories and files
            for (rel_path, content) in &files {
                let full = source_dir.join(rel_path);
                if let Some(parent) = full.parent() {
                    std::fs::create_dir_all(parent).unwrap();
                }
                std::fs::write(&full, content).unwrap();
            }

            let config = init_repo(&repo_dir);
            backup_single_source(&config, &source_dir, "test", "snap-1");
            commands::restore::run(
                &config,
                pass(&config),
                "snap-1",
                restore_dir.to_str().unwrap(),
                None,
                config.xattrs.enabled,
                false,
                &CancelToken::new(),
            )
            .unwrap();

            for (rel_path, expected) in &files {
                let actual = std::fs::read(restore_dir.join(rel_path)).unwrap();
                prop_assert_eq!(expected, &actual, "mismatch: {}", rel_path);
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Compression property tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod compression {
    use proptest::prelude::*;

    use crate::compress::{Compression, compress, compressed_size_bound, decompress};

    fn arb_compression() -> impl Strategy<Value = Compression> {
        prop_oneof![
            Just(Compression::None),
            Just(Compression::Lz4),
            Just(Compression::Zstd { level: 3 }),
        ]
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(1000))]

        /// decompress(compress(codec, data)) == data for all codecs.
        #[test]
        fn roundtrip_all_codecs(
            codec in arb_compression(),
            data in prop::collection::vec(any::<u8>(), 0..65536),
        ) {
            let compressed = compress(codec, &data).unwrap();
            let decompressed = decompress(&compressed).unwrap();
            prop_assert_eq!(&data, &decompressed);
        }

        /// compress(codec, data).len() <= compressed_size_bound(codec, data.len()).
        #[test]
        fn compressed_size_bound_holds(
            codec in arb_compression(),
            data in prop::collection::vec(any::<u8>(), 0..65536),
        ) {
            let compressed = compress(codec, &data).unwrap();
            let bound = compressed_size_bound(codec, data.len());
            prop_assert!(
                compressed.len() <= bound,
                "compressed len {} exceeds bound {} for {:?} with input len {}",
                compressed.len(), bound, codec, data.len(),
            );
        }
    }
}

// ---------------------------------------------------------------------------
// IndexDelta state-machine property tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod index_delta {
    use std::collections::{HashMap, HashSet};

    use proptest::prelude::*;

    use crate::index::{IndexDelta, NewChunkEntry};
    use vykar_types::chunk_id::ChunkId;

    use super::{arb_chunk_id, arb_pack_id};

    fn arb_new_chunk_entry(chunk_id: ChunkId) -> impl Strategy<Value = NewChunkEntry> {
        (1..100u32, arb_pack_id(), 0..u32::MAX as u64).prop_map(
            move |(stored_size, pack_id, pack_offset)| NewChunkEntry {
                chunk_id,
                stored_size,
                pack_id,
                pack_offset,
            },
        )
    }

    /// An IndexDelta with new_entries that may contain duplicate chunk_ids,
    /// plus reuse of a subset of those IDs and of other chunks.
    fn arb_index_delta_standalone() -> impl Strategy<Value = (IndexDelta, Vec<NewChunkEntry>)> {
        prop::collection::vec(arb_chunk_id().prop_flat_map(arb_new_chunk_entry), 1..30)
            .prop_flat_map(|entries| {
                let ids: Vec<ChunkId> = entries.iter().map(|e| e.chunk_id).collect();
                let entries_clone = entries.clone();
                let reused = prop::collection::vec(
                    prop_oneof![
                        (0..ids.len()).prop_map(move |idx| ids[idx % ids.len()]),
                        arb_chunk_id(),
                    ],
                    0..10,
                );
                reused.prop_map(move |reused| {
                    let mut delta = IndexDelta::new();
                    delta.new_entries = entries_clone.clone();
                    for id in &reused {
                        delta.reused.insert_key(id);
                    }
                    (delta, entries_clone.clone())
                })
            })
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(500))]

        /// After applying a delta with possibly-duplicate chunk_ids and
        /// reuse to an empty index, every new chunk is present once, at its
        /// first entry's location, and nothing else is: reuse is not
        /// persisted.
        #[test]
        fn apply_to_adds_each_new_chunk_once(
            (delta, entries) in arb_index_delta_standalone(),
        ) {
            let mut index = crate::index::LiveIndex::empty();
            delta.apply_to(&mut index);

            let mut first: HashMap<ChunkId, &NewChunkEntry> = HashMap::new();
            for entry in &entries {
                first.entry(entry.chunk_id).or_insert(entry);
            }
            prop_assert_eq!(index.len(), first.len());
            for (id, entry) in &first {
                let actual = index.get(id).unwrap();
                prop_assert_eq!(actual.pack_id, entry.pack_id);
                prop_assert_eq!(actual.pack_offset, entry.pack_offset);
            }
        }

        /// Dropping a file's reuse stage is equivalent to snapshotting the
        /// reused set by clone: stage → arbitrary in-stage reuse → drop
        /// restores exactly the set captured when the stage opened. New
        /// entries are not undone.
        #[test]
        fn reuse_stage_equivalent_to_clone(
            (mut delta, _entries) in arb_index_delta_standalone(),
            in_scope_reuse in prop::collection::vec(arb_chunk_id(), 0..40),
            extra_entry_id in arb_chunk_id(),
        ) {
            let set = |d: &IndexDelta| -> HashSet<ChunkId> { d.reused.iter().collect() };
            // Model: what a clone-based checkpoint would restore.
            let expected_reused = set(&delta);
            let expected_new_len = delta.new_entries.len();

            delta.reused.checkpoint();

            // Arbitrary in-stage mutations: reuse (some overlapping
            // pre-stage keys, some brand new) plus a new entry.
            let pre: Vec<ChunkId> = expected_reused.iter().copied().collect();
            for id in in_scope_reuse.iter().chain(pre.iter()) {
                delta.reused.insert_key(id);
            }
            let pack_id = vykar_types::pack_id::PackId::from_bytes([0u8; 32]);
            delta.add_new_entry(extra_entry_id, 100, pack_id, 0);

            delta.reused.rollback();

            prop_assert_eq!(delta.new_entries.len(), expected_new_len + 1);
            prop_assert_eq!(set(&delta), expected_reused);
            prop_assert!(!delta.reused.is_armed());
        }
    }
}
