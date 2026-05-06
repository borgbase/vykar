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
use aes_gcm::{Aes256Gcm, Nonce};
use vykar_core::compress::{compress, Compression};
use vykar_core::index::IndexBlob;
use vykar_core::repo::file_cache::FileCache;
use vykar_core::repo::format::ObjectType;
use vykar_core::repo::pack::{PackType, PackWriter};
use vykar_core::snapshot::item::{ChunkRef, Item, ItemType};
use vykar_core::snapshot::SnapshotMeta;
use vykar_types::chunk_id::ChunkId;

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
    let mut pw = PackWriter::new(PackType::Data, usize::MAX);
    pw.add_blob(ChunkId::from_bytes([1; 32]), vec![0xDE, 0xAD])
        .unwrap();
    pw.add_blob(ChunkId::from_bytes([2; 32]), vec![0xBE, 0xEF, 0x42])
        .unwrap();
    let sealed = pw.seal().unwrap();
    write_seed("fuzz_pack_scan", "two_blobs", sealed.data.as_slice());

    // Pack with a single larger blob
    let mut pw = PackWriter::new(PackType::Data, usize::MAX);
    pw.add_blob(ChunkId::from_bytes([3; 32]), vec![0x42; 256])
        .unwrap();
    let sealed = pw.seal().unwrap();
    write_seed("fuzz_pack_scan", "one_large_blob", sealed.data.as_slice());
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
            xattrs: Some(HashMap::from([(
                "user.test".into(),
                b"value".to_vec(),
            )])),
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
    cache.insert(
        "/tmp/cached_file.txt",
        1,
        1000,
        1234567890,
        1234567890,
        4096,
        vec![ChunkRef {
            id: ChunkId::from_bytes([0xDD; 32]),
            size: 4096,
            csize: 2048,
        }],
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
    let nonce = Nonce::from_slice(&nonce_bytes);

    let payload = aes_gcm::aead::Payload {
        msg: plaintext,
        aad,
    };
    let ciphertext = cipher.encrypt(nonce, payload).unwrap();

    let tag = obj_type as u8;
    let mut out = Vec::with_capacity(1 + 12 + ciphertext.len());
    out.push(tag);
    out.extend_from_slice(&nonce_bytes);
    out.extend_from_slice(&ciphertext);
    out
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

    println!("All corpus seeds written to {CORPUS_DIR}");
}
