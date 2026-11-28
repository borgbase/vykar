//! One-shot binary that generates seed corpus files for all fuzz targets.
//!
//! Run: `cargo run --manifest-path fuzz/Cargo.toml --bin generate_corpus`
//!
//! IMPORTANT: This generator must be fully deterministic. All outputs must be
//! identical across runs so that re-running it doesn't produce noisy diffs in
//! the committed corpus files. Avoid `Utc::now()`, random nonces, or any other
//! source of non-determinism.

use std::collections::HashMap;
use std::fs;
use std::path::Path;

use aes_gcm::Aes256Gcm;
use aes_gcm::aead::{Aead, KeyInit};
use vykar_core::compress::{Compression, compress};
use vykar_core::fuzzing::{self, ExtVerdict, ListingOp};
use vykar_core::index::base_v2::BaseWriter;
use vykar_core::index::head::{
    BaseRef, IndexHead, ObjectName, ObjectRef, SegmentKind, SegmentRef, SweepState,
};
use vykar_core::index::segment_v2::{Location, SegmentBody};
use vykar_core::index::sweep_record::{SWEEP_RECORD_VERSION, SweepRecord};
use vykar_core::index::{
    ChunkIndex, IndexBlob, LegacyChunkIndex, LegacyIndexEntry, PendingChunkEntry, PendingPackEntry,
    encode_pending_journal,
};
use vykar_core::repo::file_cache::{CachedChunkRef, CachedChunks, FileCache};
use vykar_core::repo::format::{ObjectType, unpack_object_expect_with_context};
use vykar_core::repo::pack::{PackType, PackWriter};
use vykar_core::repo::pack_map::{PackMap, PackMapEntry};
use vykar_core::repo::pack_trailer::read_trailer_from_pack_bytes;
use vykar_core::repo::{ChunkerParams, EncryptionMode, LegacyRepoConfig, RepoConfig};
use vykar_core::snapshot::item::{ChunkRef, Item, ItemRawNames, ItemType};
use vykar_core::snapshot::listing::{NoIndex, ORDER_DFS_NAME, ORDER_UNKNOWN, encode_ext};
use vykar_core::snapshot::{SnapshotMeta, SnapshotStats};
use vykar_crypto::key::{EncryptedKey, KdfParams};
use vykar_crypto::testing::DeterministicEngine;
use vykar_types::chunk_id::{ChunkHasher, ChunkId};
use vykar_types::hash::HashAlgorithm;
use vykar_types::pack_id::PackId;

const CORPUS_DIR: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/corpus");

/// A tagged zstd frame: a compressed block of declared size 0, then a last
/// raw block holding the 6 trailing bytes. libzstd 1.5.7's streaming decoder
/// accepts it; the one-shot decoder rejects it.
const ZSTD_EMPTY_COMPRESSED_BLOCK: &[u8] = &[
    0x02, 0x28, 0xb5, 0x2f, 0xfd, 0x00, 0x34, 0x04, 0x00, 0x00, 0x31, 0x00, 0x00, 0x73, 0x48, 0x9c,
    0x6b, 0x93, 0x8f,
];

/// Fixed epoch for deterministic snapshot timestamps (2025-01-01T00:00:00Z).
fn fixed_time() -> chrono::DateTime<chrono::Utc> {
    chrono::DateTime::from_timestamp(1_735_689_600, 0).unwrap()
}

fn write_seed(target: &str, name: &str, data: &[u8]) {
    let dir = Path::new(CORPUS_DIR).join(target);
    fs::create_dir_all(&dir).unwrap();
    fs::write(dir.join(name), data).unwrap();
}

/// The engine every sealed seed pack's trailer is written with: the
/// deterministic AEAD engine, fresh per call, so the trailer (and the
/// corpus) stays deterministic.
fn seed_engine() -> DeterministicEngine {
    DeterministicEngine::new(ChunkHasher::new(HashAlgorithm::Blake3, [7; 32]))
}

fn seed_pack_scan() {
    // Empty valid pack (header only, no blobs)
    let mut header = Vec::new();
    header.extend_from_slice(b"VGERPACK");
    header.push(1); // version
    write_seed("fuzz_pack_scan", "empty_pack", &header);

    // Pack with two small blobs via PackWriter
    let mut pw = PackWriter::new(PackType::Data, usize::MAX, HashAlgorithm::Blake2b);
    pw.add_blob(ChunkId::from_bytes([1; 32]), vec![0xDE, 0xAD])
        .unwrap();
    pw.add_blob(ChunkId::from_bytes([2; 32]), vec![0xBE, 0xEF, 0x42])
        .unwrap();
    let sealed = pw.seal(&seed_engine()).unwrap();
    write_seed(
        "fuzz_pack_scan",
        "two_blobs",
        sealed.data.as_bytes().expect("heap-buffered pack"),
    );

    // Pack with a single larger blob
    let mut pw = PackWriter::new(PackType::Data, usize::MAX, HashAlgorithm::Blake2b);
    pw.add_blob(ChunkId::from_bytes([3; 32]), vec![0x42; 256])
        .unwrap();
    let sealed = pw.seal(&seed_engine()).unwrap();
    write_seed(
        "fuzz_pack_scan",
        "one_large_blob",
        sealed.data.as_bytes().expect("heap-buffered pack"),
    );
}

fn seed_pack_trailer() {
    let mut pw = PackWriter::new(PackType::Data, usize::MAX, HashAlgorithm::Blake2b);
    pw.add_blob(ChunkId::from_bytes([1; 32]), vec![0xDE, 0xAD])
        .unwrap();
    pw.add_blob(ChunkId::from_bytes([2; 32]), vec![0xBE, 0xEF, 0x42])
        .unwrap();
    let sealed = pw.seal(&seed_engine()).unwrap();
    let pack = sealed.data.as_bytes().expect("heap-buffered pack");
    let trailer = read_trailer_from_pack_bytes(pack, &seed_engine())
        .unwrap()
        .expect("sealed pack has a trailer");
    let body = trailer.encode_body();
    let blob = trailer.seal_blob(&seed_engine()).unwrap();
    let mut frame = u32::try_from(blob.len()).unwrap().to_le_bytes().to_vec();
    frame.extend_from_slice(&blob);

    write_seed("fuzz_pack_trailer", "body", &body);
    write_seed("fuzz_pack_trailer", "frame", &frame);
    write_seed("fuzz_pack_trailer", "pack", pack);
    let mut newer = body.clone();
    newer[0] = 2;
    write_seed("fuzz_pack_trailer", "body_version_2", &newer);
    let mut empty = body[..6].to_vec();
    empty[2..6].copy_from_slice(&0u32.to_le_bytes());
    write_seed("fuzz_pack_trailer", "body_count_0", &empty);
}

fn seed_decompress() {
    let payload = b"hello world, this is test data for fuzzing decompression";

    // TAG_NONE (0x00)
    let none = compress(Compression::None, payload).unwrap();
    write_seed("fuzz_decompress", "none", &none);

    // TAG_LZ4 (0x01)
    let lz4 = compress(Compression::Lz4, payload).unwrap();
    write_seed("fuzz_decompress", "lz4", &lz4);

    // TAG_ZSTD (0x02)
    let zstd = compress(Compression::Zstd { level: 3 }, payload).unwrap();
    write_seed("fuzz_decompress", "zstd", &zstd);

    // Empty payload compressed
    let lz4_empty = compress(Compression::Lz4, b"").unwrap();
    write_seed("fuzz_decompress", "lz4_empty", &lz4_empty);

    let zstd_empty = compress(Compression::Zstd { level: 3 }, b"").unwrap();
    write_seed("fuzz_decompress", "zstd_empty", &zstd_empty);

    write_seed("fuzz_decompress", "zstd_tag_only", &[0x02]);
    // An RLE block of ~873 KiB in a frame with a 1 KiB window: the one-shot
    // decoder accepts it, streaming rejects it.
    write_seed(
        "fuzz_decompress",
        "zstd_rle_block_over_window",
        &[
            0x02, 0x28, 0xb5, 0x2f, 0xfd, 0x00, 0x01, 0x32, 0x00, 0x6d, 0x00, 0x01, 0x00, 0x00,
        ],
    );
    write_seed(
        "fuzz_decompress",
        "zstd_empty_compressed_block",
        ZSTD_EMPTY_COMPRESSED_BLOCK,
    );
}

fn seed_snapshot_meta() {
    let t = fixed_time();
    let meta = SnapshotMeta {
        name: "test-snapshot".into(),
        hostname: "fuzz-host".into(),
        username: "fuzzer".into(),
        time: t,
        time_end: t,
        chunker_params: Default::default(),
        comment: "fuzz seed".into(),
        item_ptrs: vec![ChunkId::from_bytes([0xAA; 32])],
        stats: Default::default(),
        source_label: "test".into(),
        source_paths: vec!["/tmp/test".into()],
        label: String::new(),
        ext: None,
        format_version: vykar_core::snapshot::CURRENT_FORMAT_VERSION,
    };
    let encoded = rmp_serde::to_vec(&meta).unwrap();
    write_seed("fuzz_msgpack_snapshot_meta", "valid", &encoded);

    // Minimal snapshot
    let minimal = SnapshotMeta {
        name: String::new(),
        hostname: String::new(),
        username: String::new(),
        time: t,
        time_end: t,
        chunker_params: Default::default(),
        comment: String::new(),
        item_ptrs: vec![],
        stats: Default::default(),
        source_label: String::new(),
        source_paths: vec![],
        label: String::new(),
        ext: None,
        format_version: vykar_core::snapshot::CURRENT_FORMAT_VERSION,
    };
    let encoded = rmp_serde::to_vec(&minimal).unwrap();
    write_seed("fuzz_msgpack_snapshot_meta", "minimal", &encoded);

    // Snapshot format 1: `valid` with item_ptrs as ChunkId's derived int
    // arrays (positional, as SnapshotMeta encodes).
    type MetaV1 = (
        String,
        String,
        String,
        chrono::DateTime<chrono::Utc>,
        chrono::DateTime<chrono::Utc>,
        ChunkerParams,
        String,
        Vec<ChunkId>,
        SnapshotStats,
        String,
        Vec<String>,
        String,
        Option<vykar_core::fuzzing::ByteBuf>,
        u32,
    );
    let v1: MetaV1 = (
        meta.name,
        meta.hostname,
        meta.username,
        meta.time,
        meta.time_end,
        meta.chunker_params,
        meta.comment,
        meta.item_ptrs,
        meta.stats,
        meta.source_label,
        meta.source_paths,
        meta.label,
        None,
        1,
    );
    let encoded = rmp_serde::to_vec(&v1).unwrap();
    write_seed("fuzz_msgpack_snapshot_meta", "valid_v1", &encoded);
}

fn seed_index_blob() {
    let blob = IndexBlob {
        generation: 42,
        chunks: Default::default(),
    };
    let encoded = rmp_serde::to_vec(&blob).unwrap();
    write_seed("fuzz_msgpack_index_blob", "empty_index", &encoded);
}

fn seed_index_head() {
    write_seed(
        "fuzz_index_head",
        "empty",
        &IndexHead::initial().encode().unwrap(),
    );
    let head = IndexHead {
        layout: vykar_core::index::head::HEAD_LAYOUT_VERSION,
        generation: 7,
        features: Vec::new(),
        base: Some(BaseRef {
            name: ObjectName([1; 32]),
            size: 100,
            entries: 10,
        }),
        segments: vec![SegmentRef {
            name: ObjectName([2; 32]),
            size: 20,
            kind: SegmentKind::Backup,
            entries: 2,
        }],
        retired: vec![ObjectName([9; 32])],
        sweep: SweepState {
            generation: 6,
            record: Some(ObjectRef {
                name: ObjectName([8; 32]),
                size: 40,
            }),
        },
    };
    write_seed(
        "fuzz_index_head",
        "base_segment_retired",
        &head.encode().unwrap(),
    );
}

/// The chunks of `seed_chunk_index(n)` as a legacy index at refcount 1,
/// inserted in the same order (so the seed bytes, which follow the map's
/// iteration order, are the ones the old `ChunkIndex` encoded).
fn seed_legacy_index(n: u8) -> LegacyChunkIndex {
    let mut index = LegacyChunkIndex::new();
    for i in 0..n {
        let id = ChunkId::from_bytes([i.wrapping_mul(37); 32]);
        index.insert(
            id,
            LegacyIndexEntry {
                refcount: 1,
                stored_size: 100 + u32::from(i),
                pack_id: PackId::from_bytes([i % 3 + 1; 32]),
                pack_offset: u64::from(i) * 4096,
            },
        );
    }
    index
}

/// `n` chunks spread over three packs.
fn seed_chunk_index(n: u8) -> ChunkIndex {
    let mut index = ChunkIndex::new();
    for i in 0..n {
        let id = ChunkId::from_bytes([i.wrapping_mul(37); 32]);
        index.add(
            id,
            100 + u32::from(i),
            PackId::from_bytes([i % 3 + 1; 32]),
            u64::from(i) * 4096,
        );
    }
    index
}

/// A keyed segment (body version 2) with an entry in every section.
fn all_sections_segment_v2() -> SegmentBody {
    let loc = |p: u8, off: u64, size: u32| Location {
        pack_id: PackId::from_bytes([p; 32]),
        pack_offset: off,
        stored_size: size,
    };
    SegmentBody {
        kind: SegmentKind::Maintenance,
        additions: vec![(ChunkId::from_bytes([3; 32]), loc(9, 64, 777))],
        removals: vec![ChunkId::from_bytes([4; 32]), ChunkId::from_bytes([5; 32])],
        relocations: vec![(ChunkId::from_bytes([6; 32]), loc(8, 13, 4096))],
        pack_trailers: vec![
            (PackId::from_bytes([8; 32]), 0),
            (PackId::from_bytes([9; 32]), 119),
        ],
    }
}

/// The base v2 body of `seed_chunk_index(n)`, every pack at trailer 0.
fn base_v2_body(n: u8) -> Vec<u8> {
    let index = seed_chunk_index(n);
    let mut entries: Vec<_> = index.iter().map(|(id, e)| (*id, *e)).collect();
    entries.sort_by(|a, b| a.0.as_bytes().cmp(b.0.as_bytes()));
    let mut packs: Vec<PackId> = entries.iter().map(|(_, e)| e.pack_id).collect();
    packs.sort_by(|a, b| a.as_bytes().cmp(b.as_bytes()));
    packs.dedup();
    let table: Vec<(PackId, u32)> = packs.iter().map(|p| (*p, 77)).collect();
    let mut w = BaseWriter::new(Vec::new(), &table, entries.len() as u32).unwrap();
    for (id, e) in &entries {
        let slot = packs.iter().position(|p| *p == e.pack_id).unwrap() as u32;
        w.push(id, slot, e.pack_offset, e.stored_size).unwrap();
    }
    w.finish().unwrap()
}

/// Seeds of `fuzz_index_object`: the plaintext of a packed segment and of
/// packed bases (the target seals its input itself).
fn seed_index_object() {
    let engine = seed_engine();
    let (_, segment) =
        vykar_core::fuzzing::pack_segment_v2(&all_sections_segment_v2(), &engine).unwrap();
    let plain = unpack_object_expect_with_context(
        &segment,
        ObjectType::IndexSegment,
        vykar_core::fuzzing::SEGMENT_CONTEXT,
        &engine,
    )
    .unwrap();
    write_seed("fuzz_index_object", "segment", &plain);
    for (name, n) in [("base_empty", 0u8), ("base_three_packs", 9)] {
        let (_, base) = vykar_core::fuzzing::pack_base_body(&base_v2_body(n), &engine).unwrap();
        let plain = unpack_object_expect_with_context(
            &base,
            ObjectType::IndexBase,
            vykar_core::fuzzing::BASE_CONTEXT,
            &engine,
        )
        .unwrap();
        write_seed("fuzz_index_object", name, &plain);
    }
}

fn seed_index_base_v2() {
    write_seed("fuzz_index_base_v2", "empty", &base_v2_body(0));
    write_seed("fuzz_index_base_v2", "three_packs", &base_v2_body(9));
}

fn seed_index_segment_v2() {
    write_seed(
        "fuzz_index_segment_v2",
        "all_sections",
        &all_sections_segment_v2().encode().unwrap(),
    );
    let mut backup = all_sections_segment_v2();
    backup.kind = SegmentKind::Backup;
    backup.removals.clear();
    backup.relocations.clear();
    backup.pack_trailers = vec![(PackId::from_bytes([9; 32]), 119)];
    write_seed("fuzz_index_segment_v2", "backup", &backup.encode().unwrap());
}

fn seed_item_stream() {
    let items = vec![
        Item {
            path: "/tmp/file.txt".into(),
            entry_type: ItemType::RegularFile,
            mode: 0o644,
            uid: 1000,
            gid: 1000,
            user: Some("test".into()),
            group: Some("test".into()),
            mtime: 1234567890_000_000_000,
            atime: None,
            ctime: None,
            size: 1024,
            chunks: vec![ChunkRef {
                id: ChunkId::from_bytes([0xCC; 32]),
                size: 1024,
                csize: 512,
            }],
            link_target: None,
            xattrs: None,
            raw_names: None,
            hardlink: None,
        },
        Item {
            path: "/tmp/dir".into(),
            entry_type: ItemType::Directory,
            mode: 0o755,
            uid: 0,
            gid: 0,
            user: None,
            group: None,
            mtime: 1234567890_000_000_000,
            atime: Some(1234567890_000_000_000),
            ctime: Some(1234567890_000_000_000),
            size: 0,
            chunks: vec![],
            link_target: None,
            xattrs: Some(HashMap::from([("user.test".into(), b"value".to_vec())])),
            raw_names: None,
            hardlink: None,
        },
        Item {
            path: "/tmp/link".into(),
            entry_type: ItemType::Symlink,
            mode: 0o777,
            uid: 1000,
            gid: 1000,
            user: None,
            group: None,
            mtime: 1234567890_000_000_000,
            atime: None,
            ctime: None,
            size: 0,
            chunks: vec![],
            link_target: Some("/tmp/file.txt".into()),
            xattrs: None,
            raw_names: None,
            hardlink: None,
        },
    ];

    // Concatenated msgpack items (production format)
    let mut stream = Vec::new();
    for item in &items {
        rmp_serde::encode::write(&mut stream, item).unwrap();
    }
    write_seed("fuzz_item_stream", "three_items", &stream);

    // Single item
    let mut single = Vec::new();
    rmp_serde::encode::write(&mut single, &items[0]).unwrap();
    write_seed("fuzz_item_stream", "single_file", &single);

    // The same in snapshot format 1 (chunk IDs as int arrays). These two
    // seeds are the pre-format-2 `three_items` and `single_file` and must
    // stay byte-identical: a vykar-core test checks the format-1 encoder
    // against them.
    let mut stream = Vec::new();
    for item in &items {
        vykar_core::fuzzing::encode_item_v1(item, &mut stream).unwrap();
    }
    write_seed("fuzz_item_stream", "three_items_v1", &stream);
    let mut single = Vec::new();
    vykar_core::fuzzing::encode_item_v1(&items[0], &mut single).unwrap();
    write_seed("fuzz_item_stream", "single_file_v1", &single);
}

fn seed_file_cache_decode() {
    // Current format: struct-as-array with PathHash bin keys
    let mut cache = FileCache::new();
    cache.begin_sections(&["/tmp".into()], &[1]);
    cache.insert(
        "/tmp/cached_file.txt",
        1,
        1000,
        1234567890,
        1234567890,
        4096,
        CachedChunks::from_vec(vec![CachedChunkRef {
            id: ChunkId::from_bytes([0xDD; 32]),
            size: 4096,
        }]),
    );
    let encoded = rmp_serde::to_vec(&cache).unwrap();
    write_seed("fuzz_file_cache_decode", "current_format", &encoded);

    // Legacy format: struct-as-map with String keys
    #[derive(serde::Serialize)]
    struct LegacyEntry {
        device: u64,
        inode: u64,
        mtime_ns: i64,
        ctime_ns: i64,
        size: u64,
        chunk_refs: Vec<ChunkRef>,
    }
    #[derive(serde::Serialize)]
    struct LegacyCache {
        entries: HashMap<String, LegacyEntry>,
    }
    let legacy = LegacyCache {
        entries: HashMap::from([(
            "/tmp/legacy.txt".into(),
            LegacyEntry {
                device: 1,
                inode: 2000,
                mtime_ns: 1234567890,
                ctime_ns: 1234567890,
                size: 8192,
                chunk_refs: vec![],
            },
        )]),
    };
    let encoded = rmp_serde::to_vec(&legacy).unwrap();
    write_seed("fuzz_file_cache_decode", "legacy_format", &encoded);

    // Empty cache
    let empty = FileCache::new();
    let encoded = rmp_serde::to_vec(&empty).unwrap();
    write_seed("fuzz_file_cache_decode", "empty", &encoded);
}

/// Encryption key matching fuzz_unpack_object's ENC_KEY.
const ENC_KEY: [u8; 32] = [0xAA; 32];

/// Domain-separation prefix (must match format.rs OBJECT_CONTEXT_AAD_PREFIX).
const OBJECT_CONTEXT_AAD_PREFIX: &[u8] = b"vger:object-context:v1\0";

/// Build a valid repo object envelope using AES-256-GCM with a fixed nonce.
///
/// Wire format: `[type_tag][12-byte nonce][ciphertext + 16-byte GCM tag]`
///
/// Using a fixed nonce is fine for corpus seeds — the point is to give the
/// fuzzer a valid starting input it can mutate, not to be cryptographically
/// secure.
fn pack_object_deterministic(obj_type: ObjectType, plaintext: &[u8], aad: &[u8]) -> Vec<u8> {
    let cipher = Aes256Gcm::new_from_slice(&ENC_KEY).unwrap();
    let nonce_bytes = [0u8; 12]; // fixed nonce for determinism

    let payload = aes_gcm::aead::Payload {
        msg: plaintext,
        aad,
    };
    let ciphertext = cipher.encrypt((&nonce_bytes).into(), payload).unwrap();

    let tag = obj_type as u8;
    let mut out = Vec::with_capacity(1 + 12 + ciphertext.len());
    out.push(tag);
    out.extend_from_slice(&nonce_bytes);
    out.extend_from_slice(&ciphertext);
    out
}

/// AAD of a context-bound object: `[tag] + OBJECT_CONTEXT_AAD_PREFIX + context`.
fn contextual_aad(obj_type: ObjectType, context: &[u8]) -> Vec<u8> {
    let mut aad = vec![obj_type as u8];
    aad.extend_from_slice(OBJECT_CONTEXT_AAD_PREFIX);
    aad.extend_from_slice(context);
    aad
}

fn seed_pack_map() {
    let entry = |id: u8, offset: u32, stored_size: u32| PackMapEntry {
        chunk_id: ChunkId::from_bytes([id; 32]),
        offset,
        stored_size,
    };
    let map = PackMap {
        packs: vec![
            (
                PackId::from_bytes([1; 32]),
                vec![entry(1, 13, 2), entry(2, 19, 3)],
            ),
            (PackId::from_bytes([2; 32]), vec![entry(3, 13, 256)]),
        ],
    };
    let body = map.encode_body();
    write_seed("fuzz_pack_map", "two_packs", &body);
    let mut newer = body.clone();
    newer[0] = 2;
    write_seed("fuzz_pack_map", "body_version_2", &newer);
    let mut empty = body[..5].to_vec();
    empty[1..5].copy_from_slice(&0u32.to_le_bytes());
    write_seed("fuzz_pack_map", "pack_count_0", &empty);
}

fn seed_unpack_object() {
    let plaintext = b"fuzz seed plaintext data";

    // Legacy path: AAD = [tag]
    let tag = ObjectType::ChunkData as u8;
    let packed = pack_object_deterministic(ObjectType::ChunkData, plaintext, &[tag]);
    write_seed("fuzz_unpack_object", "legacy_chunk_data", &packed);

    // Context-bound path: AAD = [tag] + prefix + context (chunk_id)
    let chunk_id_context: &[u8; 32] = &[0xCC; 32];
    let mut aad = vec![tag];
    aad.extend_from_slice(OBJECT_CONTEXT_AAD_PREFIX);
    aad.extend_from_slice(chunk_id_context);
    let packed = pack_object_deterministic(ObjectType::ChunkData, plaintext, &aad);
    write_seed("fuzz_unpack_object", "context_chunk_data", &packed);

    // Context-bound: index context
    let tag = ObjectType::ChunkIndex as u8;
    let mut aad = vec![tag];
    aad.extend_from_slice(OBJECT_CONTEXT_AAD_PREFIX);
    aad.extend_from_slice(b"index");
    let packed = pack_object_deterministic(ObjectType::ChunkIndex, b"index payload", &aad);
    write_seed("fuzz_unpack_object", "context_index", &packed);

    // Minimal: empty plaintext, legacy AAD
    let tag = ObjectType::Config as u8;
    let packed = pack_object_deterministic(ObjectType::Config, b"", &[tag]);
    write_seed("fuzz_unpack_object", "empty_config", &packed);

    // Index objects, each with its production context.
    let seeds: [(&str, ObjectType, &[u8], Vec<u8>); 4] = [
        (
            "context_index_head",
            ObjectType::IndexHead,
            b"index.h",
            IndexHead::initial().encode().unwrap(),
        ),
        (
            "context_sweep_record",
            ObjectType::SweepRecord,
            b"index.d/sweep",
            sweep_record().encode().unwrap(),
        ),
        (
            "context_index_base",
            ObjectType::IndexBase,
            b"index.d/base",
            b"index base payload".to_vec(),
        ),
        (
            "context_index_segment",
            ObjectType::IndexSegment,
            b"index.d/segment",
            // Any payload will do; these are the bytes of the empty v1
            // segment this seed was first generated from (version 1, a zero
            // state hash, four zero counts), so the seed stays the same.
            [&[1u8][..], &[0u8; 36]].concat(),
        ),
    ];
    for (name, obj_type, context, plaintext) in seeds {
        let aad = contextual_aad(obj_type, context);
        let packed = pack_object_deterministic(obj_type, &plaintext, &aad);
        write_seed("fuzz_unpack_object", name, &packed);
    }
}

fn sweep_record() -> SweepRecord {
    SweepRecord {
        version: SWEEP_RECORD_VERSION,
        generation: 6,
        time: fixed_time(),
        snapshots: vec![
            vykar_core::fuzzing::ByteBuf::from(vec![1; 32]),
            vykar_core::fuzzing::ByteBuf::from(vec![2; 32]),
        ],
        tree_chunks: 3,
        entries_live: 10,
        bytes_live: 1000,
        entries_removed: 2,
        bytes_removed: 200,
        references: 12,
        referenced_bytes: 1500,
    }
}

fn seed_sweep_record() {
    write_seed(
        "fuzz_sweep_record",
        "two_snapshots",
        &sweep_record().encode().unwrap(),
    );
    let empty = SweepRecord {
        snapshots: Vec::new(),
        ..sweep_record()
    };
    write_seed("fuzz_sweep_record", "empty", &empty.encode().unwrap());
}

/// A fuzz_listing_ext input (layout: fuzz_targets/fuzz_listing_ext.rs).
fn listing_ext_input(ext: Option<&[u8]>, stride: u32, ops: &[ListingOp<'_>]) -> Vec<u8> {
    assert!((1..=32).contains(&stride));
    let ext_bytes = ext.unwrap_or_default();
    let mut out = vec![u8::from(ext.is_none()), u8::try_from(stride - 1).unwrap()];
    out.extend_from_slice(&u16::try_from(ext_bytes.len()).unwrap().to_le_bytes());
    out.extend_from_slice(ext_bytes);
    for (i, op) in ops.iter().enumerate() {
        if i > 0 {
            out.push(b'\n');
        }
        match *op {
            ListingOp::Cut => {}
            ListingOp::BeginSegment(root) => {
                assert!(!root.contains(&b'\n'));
                out.push(0x01);
                out.extend_from_slice(root);
            }
            ListingOp::Push(path) => {
                assert!(!path.contains(&b'\n'));
                out.push(0x02);
                out.extend_from_slice(path);
            }
        }
    }
    out
}

fn from_hex(s: &str) -> Vec<u8> {
    (0..s.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&s[i..i + 2], 16).unwrap())
        .collect()
}

/// Write a fuzz_listing_ext seed whose ext `check` accepts.
fn write_listing_seed(name: &str, ext: &[u8], stride: u32, ops: &[ListingOp<'_>]) {
    let listing = fuzzing::write_listing(ops, stride);
    assert_eq!(
        fuzzing::check_ext(Some(ext), &listing.chunks),
        ExtVerdict::Ok,
        "{name}"
    );
    write_seed(
        "fuzz_listing_ext",
        name,
        &listing_ext_input(Some(ext), stride, ops),
    );
}

fn seed_listing_ext() {
    // The TREE of listing.rs's builder tests, two entries per tree chunk.
    let tree: &[&[u8]] = &[
        b"a", b"a.b", b"b", b"a/x", b"a/y", b"a/y/1", b"a/y/2", b"b/c", b"b/d",
    ];
    let mut tree_ops = Vec::new();
    for (i, p) in tree.iter().enumerate() {
        tree_ops.push(ListingOp::Push(p));
        if i % 2 == 1 {
            tree_ops.push(ListingOp::Cut);
        }
    }
    let tree_listing = fuzzing::write_listing(&tree_ops, 2);
    assert_eq!(tree_listing.index.order, ORDER_DFS_NAME);
    assert_eq!(tree_listing.chunks.len(), 5);
    let keys: Vec<&[u8]> = tree_listing
        .index
        .keys
        .iter()
        .map(|k| k.as_slice())
        .collect();
    assert_eq!(keys, [&b""[..], b"a/y", b"b/d"]);
    let tree_ext = encode_ext(&tree_listing.index).unwrap();
    write_listing_seed("tree_stride_2", &tree_ext, 2, &tree_ops);

    // Explicit segments, one of them empty, two entries per tree chunk.
    let segments: &[(&[u8], &[&[u8]])] = &[
        (
            b"vykar-dumps",
            &[b"vykar-dumps", b"vykar-dumps/a", b"vykar-dumps/b"],
        ),
        (b"none", &[]),
        (b"home/u/b", &[b"home/u/b", b"home/u/b/x", b"home/u/b/y/z"]),
        (b"home/u/a", &[b"home/u/a", b"home/u/a/k"]),
    ];
    let mut seg_ops = Vec::new();
    for (root, paths) in segments {
        seg_ops.push(ListingOp::BeginSegment(root));
        for (i, p) in paths.iter().enumerate() {
            seg_ops.push(ListingOp::Push(p));
            if i % 2 == 1 {
                seg_ops.push(ListingOp::Cut);
            }
        }
    }
    let seg_index = fuzzing::write_listing(&seg_ops, 1).index;
    assert_eq!(seg_index.order, ORDER_DFS_NAME);
    let first_ptrs: Vec<u32> = seg_index.segments.iter().map(|s| s.first_ptr).collect();
    assert_eq!(first_ptrs, [0, 2, 4]);
    write_listing_seed(
        "segments_stride_1",
        &encode_ext(&seg_index).unwrap(),
        1,
        &seg_ops,
    );

    // The frozen bytes of listing.rs's ext_golden_bytes, derived by the
    // writer's builder from a 33-chunk listing.
    let golden_paths: Vec<Vec<u8>> = (0..31)
        .map(|i| format!("r/a/a{i:02}").into_bytes())
        .collect();
    let mut golden_ops = vec![
        ListingOp::BeginSegment(b"vykar-dumps"),
        ListingOp::Push(b"vykar-dumps"),
        ListingOp::BeginSegment(b"r"),
    ];
    for p in &golden_paths {
        golden_ops.push(ListingOp::Push(p));
        golden_ops.push(ListingOp::Cut);
    }
    golden_ops.push(ListingOp::Push(b"r/a/b"));
    let golden_listing = fuzzing::write_listing(&golden_ops, 32);
    assert_eq!(golden_listing.chunks.len(), 33);
    let golden = golden_listing.index;
    let golden_ext = encode_ext(&golden).unwrap();
    assert_eq!(
        golden_ext,
        from_hex(concat!(
            "92019401",
            "92",
            "92c40b76796b61722d64756d707300",
            "92c4017201",
            "20",
            "92c400c403612f62"
        ))
    );
    write_listing_seed("golden", &golden_ext, 32, &golden_ops);

    // A trailing element after the index is ignored.
    let trailing = rmp_serde::to_vec(&(1u32, &golden, "extra")).unwrap();
    write_listing_seed("golden_trailing", &trailing, 32, &golden_ops);

    // An unknown ext version: no index, not a mismatch.
    let unknown = rmp_serde::to_vec(&(2u32, &golden)).unwrap();
    assert_eq!(
        fuzzing::check_ext(Some(&unknown), &golden_listing.chunks),
        ExtVerdict::NoIndex(NoIndex::UnknownVersion(2))
    );
    write_seed(
        "fuzz_listing_ext",
        "unknown_version",
        &listing_ext_input(Some(&unknown), 32, &golden_ops),
    );

    // A listing out of name order: the writer's order-0 index.
    let order0_ops = [ListingOp::Push(b"b"), ListingOp::Push(b"a")];
    let order0 = fuzzing::write_listing(&order0_ops, 2);
    assert_eq!(order0.index.order, ORDER_UNKNOWN);
    let order0_ext = encode_ext(&order0.index).unwrap();
    assert_eq!(
        fuzzing::check_ext(Some(&order0_ext), &order0.chunks),
        ExtVerdict::NoIndex(NoIndex::Unordered)
    );
    write_seed(
        "fuzz_listing_ext",
        "order_0",
        &listing_ext_input(Some(&order0_ext), 2, &order0_ops),
    );

    // No tree chunks at all.
    let empty = fuzzing::write_listing(&[], 32).index;
    assert_eq!(empty.order, ORDER_DFS_NAME);
    write_listing_seed("empty_listing", &encode_ext(&empty).unwrap(), 32, &[]);

    // An absent ext.
    assert_eq!(
        fuzzing::check_ext(None, &tree_listing.chunks),
        ExtVerdict::NoIndex(NoIndex::Absent)
    );
    write_seed(
        "fuzz_listing_ext",
        "absent",
        &listing_ext_input(None, 2, &tree_ops),
    );
}

/// A positional (v2/v3, or a refused development version) config.
fn repo_config(
    version: u32,
    encryption: EncryptionMode,
    chunker: ChunkerParams,
    min_pack: u32,
    max_pack: u32,
) -> Vec<u8> {
    rmp_serde::to_vec(&LegacyRepoConfig {
        version,
        id: vec![0x42; 32],
        chunker_params: chunker,
        encryption,
        created: fixed_time(),
        min_pack_size: min_pack,
        max_pack_size: max_pack,
    })
    .unwrap()
}

/// A v6 (map) config.
fn repo_config_v6(
    hash: HashAlgorithm,
    encryption: EncryptionMode,
    chunker: ChunkerParams,
    min_pack: u32,
    max_pack: u32,
) -> Vec<u8> {
    RepoConfig::new_v6(
        vec![0x42; 32],
        chunker,
        encryption,
        fixed_time(),
        min_pack,
        max_pack,
        hash,
    )
    .encode()
    .unwrap()
}

fn seed_repo_config() {
    const MIB: u32 = 1024 * 1024;
    let small = ChunkerParams {
        min_size: 1024,
        avg_size: 4096,
        max_size: 16384,
    };
    let aes = || EncryptionMode::Aes256Gcm;
    let seeds = [
        (
            "v6_default",
            repo_config_v6(
                HashAlgorithm::Blake3,
                aes(),
                ChunkerParams::default(),
                32 * MIB,
                192 * MIB,
            ),
        ),
        (
            "v6_blake2b_small",
            repo_config_v6(HashAlgorithm::Blake2b, aes(), small.clone(), MIB, 4 * MIB),
        ),
        // Unencrypted: refused by `validate`.
        (
            "v6_none",
            repo_config_v6(
                HashAlgorithm::Blake3,
                EncryptionMode::None,
                ChunkerParams::default(),
                32 * MIB,
                192 * MIB,
            ),
        ),
        (
            "v2_none",
            repo_config(2, EncryptionMode::None, small.clone(), MIB, 4 * MIB),
        ),
        (
            "v5_default",
            repo_config(5, aes(), ChunkerParams::default(), 32 * MIB, 192 * MIB),
        ),
        (
            "v2_small",
            repo_config(2, aes(), small.clone(), MIB, 4 * MIB),
        ),
        (
            "v4_min_above_max",
            repo_config(4, aes(), small, 64 * MIB, 32 * MIB),
        ),
        (
            "v3_odd_chunker",
            repo_config(
                3,
                aes(),
                ChunkerParams {
                    min_size: 257,
                    avg_size: 1024,
                    max_size: 4096,
                },
                MIB,
                4 * MIB,
            ),
        ),
    ];
    for (name, data) in seeds {
        write_seed("fuzz_repo_config", name, &data);
    }
}

fn seed_key_blob() {
    // Well-formed but never derived from: the target only screens it.
    let key = EncryptedKey {
        kdf: KdfParams {
            algorithm: "argon2id".into(),
            time_cost: 3,
            memory_cost: 64 * 1024,
            parallelism: 4,
            salt: vec![0x11; 32],
        },
        nonce: vec![0x22; 12],
        encrypted_payload: vec![0x33; 120],
    };
    write_seed("fuzz_key_blob", "valid", &rmp_serde::to_vec(&key).unwrap());
    let mut bad = key;
    bad.kdf.memory_cost = 8 * 1024 * 1024;
    write_seed(
        "fuzz_key_blob",
        "memory_too_large",
        &rmp_serde::to_vec(&bad).unwrap(),
    );
}

fn path_item(path: &str, entry_type: ItemType, link_target: Option<&str>) -> Item {
    Item {
        path: path.into(),
        entry_type,
        mode: 0o644,
        uid: 1000,
        gid: 1000,
        user: None,
        group: None,
        mtime: 1_234_567_890_000_000_000,
        atime: None,
        ctime: None,
        size: 0,
        chunks: vec![],
        link_target: link_target.map(Into::into),
        xattrs: None,
        raw_names: None,
        hardlink: None,
    }
}

fn seed_restore_paths() {
    let mut raw_file = path_item("", ItemType::RegularFile, None);
    let raw_path = b"dir/caf\xe9".to_vec();
    raw_file.path = String::from_utf8_lossy(&raw_path).into_owned();
    raw_file.raw_names = Some(ItemRawNames {
        path: Some(raw_path),
        link_target: None,
    });
    let mut raw_link = path_item("dir/raw-link", ItemType::Symlink, None);
    let raw_target = b"../\xfftarget".to_vec();
    raw_link.link_target = Some(String::from_utf8_lossy(&raw_target).into_owned());
    raw_link.raw_names = Some(ItemRawNames {
        path: None,
        link_target: Some(raw_target),
    });

    let items = [
        path_item("dir", ItemType::Directory, None),
        path_item("./dir//file.txt", ItemType::RegularFile, None),
        path_item("dir/../../escape", ItemType::RegularFile, None),
        path_item("/etc/passwd", ItemType::RegularFile, None),
        path_item("dir/abs-link", ItemType::Symlink, Some("/etc/shadow")),
        path_item("dir/up-link", ItemType::Symlink, Some("../../x")),
        path_item("dir/safe-link", ItemType::Symlink, Some("file.txt")),
        raw_file,
        raw_link,
    ];
    let mut stream = Vec::new();
    for item in &items {
        rmp_serde::encode::write(&mut stream, item).unwrap();
    }
    write_seed("fuzz_restore_paths", "mixed", &stream);
}

fn seed_pending_journal() {
    let entries = vec![
        PendingPackEntry {
            pack_id: PackId::from_bytes([1; 32]),
            chunks: vec![
                PendingChunkEntry {
                    chunk_id: ChunkId::from_bytes([2; 32]),
                    stored_size: 100,
                    pack_offset: 9,
                },
                PendingChunkEntry {
                    chunk_id: ChunkId::from_bytes([3; 32]),
                    stored_size: 200,
                    pack_offset: 113,
                },
            ],
        },
        PendingPackEntry {
            pack_id: PackId::from_bytes([1; 32]),
            chunks: vec![],
        },
    ];
    write_seed(
        "fuzz_pending_journal",
        "v2",
        &encode_pending_journal(&entries).unwrap(),
    );
    write_seed(
        "fuzz_pending_journal",
        "legacy",
        &rmp_serde::to_vec(&entries).unwrap(),
    );
    write_seed(
        "fuzz_pending_journal",
        "unknown_version",
        &[0x92, 0x03, 0x90],
    );
}

fn seed_protocol_keys() {
    let hex64 = "0123456789abcdef".repeat(4);
    let keys = [
        ("key_config", "config".to_string()),
        ("key_pack", format!("packs/01/{hex64}")),
        ("key_index_object", format!("index.d/{hex64}")),
        ("key_snapshot", "snapshots/abc".to_string()),
        ("key_session", "sessions/abc.index".to_string()),
        ("key_traversal", "snapshots/../config".to_string()),
        ("key_temp", "snapshots/.tmp.abc.1".to_string()),
    ];
    for (name, key) in keys {
        let mut data = vec![0u8];
        data.extend_from_slice(key.as_bytes());
        write_seed("fuzz_protocol_keys", name, &data);
    }
    let mut range = vec![1u8];
    range.extend_from_slice(b"bytes=200-299");
    write_seed("fuzz_protocol_keys", "range", &range);
    let mut header = vec![2u8];
    header.extend_from_slice(b"VGERPACK\x01");
    write_seed("fuzz_protocol_keys", "pack_header", &header);
}

fn seed_metadata_msgpack() {
    let blob = IndexBlob {
        generation: 7,
        chunks: seed_legacy_index(9),
    };
    let raw = rmp_serde::to_vec(&blob).unwrap();
    for (name, compression) in [
        ("zstd", Compression::Zstd { level: 3 }),
        ("lz4", Compression::Lz4),
        ("none", Compression::None),
    ] {
        write_seed(
            "fuzz_metadata_msgpack",
            name,
            &compress(compression, &raw).unwrap(),
        );
    }
}

fn main() {
    println!("Generating fuzz corpus seeds...");

    seed_pack_scan();
    println!("  fuzz_pack_scan: done");

    seed_pack_trailer();
    println!("  fuzz_pack_trailer: done");

    seed_pack_map();
    println!("  fuzz_pack_map: done");

    seed_decompress();
    println!("  fuzz_decompress: done");

    seed_snapshot_meta();
    println!("  fuzz_msgpack_snapshot_meta: done");

    seed_index_blob();
    println!("  fuzz_msgpack_index_blob: done");

    seed_item_stream();
    println!("  fuzz_item_stream: done");

    seed_file_cache_decode();
    println!("  fuzz_file_cache_decode: done");

    seed_unpack_object();
    println!("  fuzz_unpack_object: done");

    seed_index_head();
    println!("  fuzz_index_head: done");

    seed_index_base_v2();
    println!("  fuzz_index_base_v2: done");

    seed_index_segment_v2();
    println!("  fuzz_index_segment_v2: done");

    seed_sweep_record();
    println!("  fuzz_sweep_record: done");

    seed_listing_ext();
    println!("  fuzz_listing_ext: done");

    seed_index_object();
    println!("  fuzz_index_object: done");

    seed_repo_config();
    println!("  fuzz_repo_config: done");

    seed_key_blob();
    println!("  fuzz_key_blob: done");

    seed_restore_paths();
    println!("  fuzz_restore_paths: done");

    seed_pending_journal();
    println!("  fuzz_pending_journal: done");

    seed_protocol_keys();
    println!("  fuzz_protocol_keys: done");

    seed_metadata_msgpack();
    println!("  fuzz_metadata_msgpack: done");

    println!("All corpus seeds written to {CORPUS_DIR}");
}
