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

use aes_gcm::aead::{Aead, KeyInit};
use aes_gcm::Aes256Gcm;
use vykar_core::compress::{compress, Compression};
use vykar_core::config::ChunkerConfig;
use vykar_core::index::base::encode_base;
use vykar_core::index::dedup_cache::{
    build_full_index_cache_to_path, FullCacheEntry, MmapFullIndexCache,
};
use vykar_core::index::head::{BaseRef, IndexHead, ObjectName, SegmentRef};
use vykar_core::index::segment::{diff_to_temp, RefcountDelta, Relocation, Segment};
use vykar_core::index::{
    encode_pending_journal, ChunkIndex, IndexBlob, PendingChunkEntry, PendingPackEntry,
};
use vykar_core::repo::file_cache::{CachedChunkRef, CachedChunks, FileCache};
use vykar_core::repo::format::{pack_object_with_context, ObjectType};
use vykar_core::repo::pack::{PackType, PackWriter};
use vykar_core::repo::{EncryptionMode, RepoConfig};
use vykar_core::snapshot::item::{ChunkRef, Item, ItemRawNames, ItemType};
use vykar_core::snapshot::SnapshotMeta;
use vykar_crypto::key::{EncryptedKey, KdfParams};
use vykar_crypto::{CryptoEngine, PlaintextEngine};
use vykar_types::chunk_id::{ChunkHasher, ChunkId};
use vykar_types::hash::HashAlgorithm;
use vykar_types::pack_id::PackId;

const CORPUS_DIR: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/corpus");

/// Fixed epoch for deterministic snapshot timestamps (2025-01-01T00:00:00Z).
fn fixed_time() -> chrono::DateTime<chrono::Utc> {
    chrono::DateTime::from_timestamp(1_735_689_600, 0).unwrap()
}

fn write_seed(target: &str, name: &str, data: &[u8]) {
    let dir = Path::new(CORPUS_DIR).join(target);
    fs::create_dir_all(&dir).unwrap();
    fs::write(dir.join(name), data).unwrap();
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
    let sealed = pw.seal().unwrap();
    write_seed(
        "fuzz_pack_scan",
        "two_blobs",
        sealed.data.as_bytes().expect("heap-buffered pack"),
    );

    // Pack with a single larger blob
    let mut pw = PackWriter::new(PackType::Data, usize::MAX, HashAlgorithm::Blake2b);
    pw.add_blob(ChunkId::from_bytes([3; 32]), vec![0x42; 256])
        .unwrap();
    let sealed = pw.seal().unwrap();
    write_seed(
        "fuzz_pack_scan",
        "one_large_blob",
        sealed.data.as_bytes().expect("heap-buffered pack"),
    );
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
}

fn seed_index_blob() {
    let blob = IndexBlob {
        generation: 42,
        chunks: Default::default(),
    };
    let encoded = rmp_serde::to_vec(&blob).unwrap();
    write_seed("fuzz_msgpack_index_blob", "empty_index", &encoded);
}

fn seed_index_segment() {
    write_seed(
        "fuzz_index_segment",
        "empty",
        &Segment::empty([0; 32], 0).encode(),
    );
    write_seed(
        "fuzz_index_segment",
        "all_sections",
        &all_sections_segment().encode(),
    );
}

/// A segment with an entry in every section.
fn all_sections_segment() -> Segment {
    Segment {
        prev_state_hash: [7; 32],
        prev_entries: 1000,
        deltas: vec![
            RefcountDelta {
                position: 0,
                delta: 1,
            },
            RefcountDelta {
                position: 1,
                delta: 1,
            },
            RefcountDelta {
                position: 500,
                delta: -3,
            },
        ],
        relocations: vec![Relocation {
            position: 42,
            pack_id: PackId::from_bytes([9; 32]),
            pack_offset: 123_456,
            stored_size: 4096,
        }],
        new_entries: vec![FullCacheEntry {
            chunk_id: ChunkId::from_bytes([3; 32]),
            refcount: 2,
            stored_size: 777,
            pack_id: PackId::from_bytes([9; 32]),
            pack_offset: 64,
        }],
    }
}

fn seed_index_head() {
    write_seed(
        "fuzz_index_head",
        "empty",
        &IndexHead::empty().encode().unwrap(),
    );
    let head = IndexHead {
        layout: 1,
        state_hash: [3; 32],
        entries: 12,
        base: Some(BaseRef {
            name: ObjectName([1; 32]),
            size: 100,
            state_hash: [1; 32],
            entries: 10,
        }),
        segments: vec![SegmentRef {
            name: ObjectName([2; 32]),
            size: 20,
            state_hash_after: [3; 32],
            entries_after: 12,
        }],
        retired: vec![ObjectName([9; 32])],
    };
    write_seed(
        "fuzz_index_head",
        "base_segment_retired",
        &head.encode().unwrap(),
    );
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

fn seed_index_base() {
    let path = std::env::temp_dir().join("vykar-fuzz-corpus-full-cache");
    for (name, n) in [("empty", 0u8), ("three_packs", 9)] {
        let index = seed_chunk_index(n);
        build_full_index_cache_to_path(&index, 1, &path).unwrap();
        let cache = MmapFullIndexCache::open_verified(&path).unwrap();
        let mut data = vec![n];
        encode_base(&cache, &mut data).unwrap();
        write_seed("fuzz_index_base", name, &data);
    }
    let _ = fs::remove_file(&path);
}

/// `fuzz_index_apply` input: control byte, framed base and segments, the
/// last segment unframed.
fn frame(control: u8, base: &[u8], segments: &[Vec<u8>]) -> Vec<u8> {
    let mut out = vec![control];
    let (last, framed) = segments.split_last().unwrap();
    for body in std::iter::once(base).chain(framed.iter().map(Vec::as_slice)) {
        out.extend_from_slice(&u16::try_from(body.len()).unwrap().to_le_bytes());
        out.extend_from_slice(body);
    }
    out.extend_from_slice(last);
    out
}

/// One round of changes: two refcount bumps, a removal, a relocation and
/// two new chunks (`round` keeps the new IDs distinct).
fn change_index(index: &mut ChunkIndex, round: u8) {
    let mut ids: Vec<ChunkId> = index.iter().map(|(id, _)| *id).collect();
    ids.sort_unstable_by(|a, b| a.as_bytes().cmp(b.as_bytes()));
    index.increment_refcount_by(&ids[0], 2);
    index.increment_refcount_by(&ids[1], 1);
    index.remove(&ids[2]);
    index.update_location(&ids[3], PackId::from_bytes([0xE0 + round; 32]), 8192, 333);
    for k in 0..2 {
        index.add(
            ChunkId::from_bytes([0xF0 + 2 * round + k; 32]),
            200 + u32::from(k),
            PackId::from_bytes([0xE0 + round; 32]),
            u64::from(k) * 512,
        );
    }
}

/// The base body of `index` and the bodies of the segments that take it
/// through `rounds` (each `change` applied in turn).
fn base_and_segments(
    mut index: ChunkIndex,
    rounds: usize,
    change: impl Fn(&mut ChunkIndex, u8),
) -> (Vec<u8>, Vec<Vec<u8>>) {
    let path = std::env::temp_dir().join("vykar-fuzz-corpus-index-apply");
    build_full_index_cache_to_path(&index, 1, &path).unwrap();
    let mut cache = MmapFullIndexCache::open_verified(&path).unwrap();
    let mut base = Vec::new();
    encode_base(&cache, &mut base).unwrap();
    let mut segments = Vec::new();
    // Each state's file outlives its mapping.
    let mut files = Vec::new();
    for round in 0..rounds {
        change(&mut index, u8::try_from(round).unwrap());
        let (diff, tmp) = diff_to_temp(&cache, &index, &path).unwrap();
        segments.push(diff.segment.encode());
        cache = MmapFullIndexCache::open_verified(&tmp).unwrap();
        files.push(tmp);
    }
    drop(cache);
    drop(files);
    let _ = fs::remove_file(&path);
    (base, segments)
}

fn seed_index_apply() {
    let (base, one) = base_and_segments(seed_chunk_index(9), 1, change_index);
    write_seed("fuzz_index_apply", "one_segment", &frame(0, &base, &one));

    let (base, three) = base_and_segments(seed_chunk_index(9), 3, change_index);
    write_seed(
        "fuzz_index_apply",
        "three_segments",
        &frame(2, &base, &three),
    );
    // Layer 1 declares a wrong hash, caught when it finishes.
    write_seed(
        "fuzz_index_apply",
        "three_segments_lie",
        &frame(2 | 1 << 3, &base, &three),
    );
    // Layer 1 declares one entry too few, caught by layer 2's input.
    write_seed(
        "fuzz_index_apply",
        "three_segments_lie_fewer",
        &frame(2 | 1 << 3 | 2 << 5, &base, &three),
    );

    let (base, add) = base_and_segments(ChunkIndex::new(), 1, |index, _| {
        for k in 0..3u8 {
            index.add(
                ChunkId::from_bytes([0x10 + k; 32]),
                64,
                PackId::from_bytes([1; 32]),
                u64::from(k) * 64,
            );
        }
    });
    write_seed("fuzz_index_apply", "empty_base", &frame(0, &base, &add));
}

fn seed_index_object() {
    let engine = PlaintextEngine::new(ChunkHasher::new(HashAlgorithm::Blake3, [7; 32]));
    let (_, segment) = vykar_core::fuzzing::pack_segment(&all_sections_segment(), &engine).unwrap();
    write_seed("fuzz_index_object", "segment", &segment);

    let path = std::env::temp_dir().join("vykar-fuzz-corpus-index-object");
    for (name, n) in [("base_empty", 0u8), ("base_three_packs", 9)] {
        build_full_index_cache_to_path(&seed_chunk_index(n), 1, &path).unwrap();
        let cache = MmapFullIndexCache::open_verified(&path).unwrap();
        let (_, base) = vykar_core::fuzzing::pack_base(&cache, &engine).unwrap();
        write_seed("fuzz_index_object", name, &base);
    }
    let _ = fs::remove_file(&path);
}

fn seed_full_index_cache() {
    let path = std::env::temp_dir().join("vykar-fuzz-corpus-full-index-cache");
    build_full_index_cache_to_path(&seed_chunk_index(9), 1, &path).unwrap();
    let file = fs::read(&path).unwrap();
    let _ = fs::remove_file(&path);
    let without_trailer = &file[..file.len() - 32];
    let region = &without_trailer[28..];

    let mut sorted = vec![0u8];
    sorted.extend_from_slice(region);
    write_seed("fuzz_full_index_cache", "sorted", &sorted);

    let mut unsorted = vec![0u8];
    unsorted.extend_from_slice(&region[80..160]);
    unsorted.extend_from_slice(&region[..80]);
    unsorted.extend_from_slice(&region[160..]);
    write_seed("fuzz_full_index_cache", "unsorted", &unsorted);

    let mut raw_header = vec![1u8];
    raw_header.extend_from_slice(without_trailer);
    write_seed("fuzz_full_index_cache", "raw_header", &raw_header);
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
    let seeds: [(&str, ObjectType, &[u8], Vec<u8>); 3] = [
        (
            "context_index_head",
            ObjectType::IndexHead,
            b"index",
            IndexHead::empty().encode().unwrap(),
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
            Segment::empty([0; 32], 0).encode(),
        ),
    ];
    for (name, obj_type, context, plaintext) in seeds {
        let aad = contextual_aad(obj_type, context);
        let packed = pack_object_deterministic(obj_type, &plaintext, &aad);
        write_seed("fuzz_unpack_object", name, &packed);
    }
}

fn repo_config(version: u32, chunker: ChunkerConfig, min_pack: u32, max_pack: u32) -> Vec<u8> {
    rmp_serde::to_vec(&RepoConfig {
        version,
        id: vec![0x42; 32],
        chunker_params: chunker,
        encryption: EncryptionMode::None,
        created: fixed_time(),
        min_pack_size: min_pack,
        max_pack_size: max_pack,
    })
    .unwrap()
}

fn seed_repo_config() {
    const MIB: u32 = 1024 * 1024;
    let small = ChunkerConfig {
        min_size: 1024,
        avg_size: 4096,
        max_size: 16384,
    };
    let seeds = [
        (
            "v5_default",
            repo_config(5, ChunkerConfig::default(), 32 * MIB, 192 * MIB),
        ),
        ("v2_small", repo_config(2, small.clone(), MIB, 4 * MIB)),
        (
            "v4_min_above_max",
            repo_config(4, small, 64 * MIB, 32 * MIB),
        ),
        (
            "v3_odd_chunker",
            repo_config(
                3,
                ChunkerConfig {
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
        chunks: seed_chunk_index(9),
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

/// Chunk-ID key of `fuzz_plaintext_read`.
const PLAINTEXT_CHUNK_ID_KEY: [u8; 32] = [0x5A; 32];

fn seed_plaintext_read() {
    let plaintext = b"plaintext chunk read path, plaintext chunk read path";
    for (name, algorithm, compression) in [
        ("blake2b_lz4", HashAlgorithm::Blake2b, Compression::Lz4),
        (
            "blake3_zstd",
            HashAlgorithm::Blake3,
            Compression::Zstd { level: 3 },
        ),
    ] {
        let engine = PlaintextEngine::new(ChunkHasher::new(algorithm, PLAINTEXT_CHUNK_ID_KEY));
        let id = ChunkId::compute(engine.chunk_hasher(), plaintext);
        let blob = pack_object_with_context(
            ObjectType::ChunkData,
            id.as_bytes(),
            &compress(compression, plaintext).unwrap(),
            &engine,
        )
        .unwrap();
        let mut pw = PackWriter::new(PackType::Data, usize::MAX, algorithm);
        pw.add_blob(id, blob).unwrap();
        let sealed = pw.seal().unwrap();

        let mut data = vec![u8::from(algorithm == HashAlgorithm::Blake3)];
        data.extend_from_slice(id.as_bytes());
        data.extend_from_slice(&u32::try_from(plaintext.len()).unwrap().to_le_bytes());
        data.extend_from_slice(sealed.data.as_bytes().expect("heap-buffered pack"));
        write_seed("fuzz_plaintext_read", name, &data);
    }
}

fn main() {
    println!("Generating fuzz corpus seeds...");

    seed_pack_scan();
    println!("  fuzz_pack_scan: done");

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

    seed_index_segment();
    println!("  fuzz_index_segment: done");

    seed_index_head();
    println!("  fuzz_index_head: done");

    seed_index_base();
    println!("  fuzz_index_base: done");

    seed_index_apply();
    println!("  fuzz_index_apply: done");

    seed_index_object();
    println!("  fuzz_index_object: done");

    seed_full_index_cache();
    println!("  fuzz_full_index_cache: done");

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

    seed_plaintext_read();
    println!("  fuzz_plaintext_read: done");

    println!("All corpus seeds written to {CORPUS_DIR}");
}
