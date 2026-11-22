#![no_main]
use libfuzzer_sys::fuzz_target;
use vykar_crypto::aes_gcm::Aes256GcmEngine;
use vykar_types::chunk_id::ChunkHasher;
use vykar_types::hash::HashAlgorithm;

/// Fixed key pair for deterministic fuzzing.
const ENC_KEY: [u8; 32] = [0xAA; 32];
const CID_KEY: [u8; 32] = [0xBB; 32];

/// Representative contexts used by production code.
const CONTEXT_CHUNK_ID: &[u8; 32] = &[0xCC; 32];
/// `repo/mod.rs` `INDEX_OBJECT_CONTEXT`; also the index head's context.
const CONTEXT_INDEX: &[u8] = b"index";
/// `repo/index_store.rs` `BASE_CONTEXT`.
const CONTEXT_INDEX_BASE: &[u8] = b"index.d/base";
/// `repo/index_store.rs` `SEGMENT_CONTEXT`.
const CONTEXT_INDEX_SEGMENT: &[u8] = b"index.d/segment";

fuzz_target!(|data: &[u8]| {
    let engine = Aes256GcmEngine::new(&ENC_KEY, ChunkHasher::new(HashAlgorithm::Blake2b, CID_KEY));

    // Legacy path (AAD = [tag])
    let _ = vykar_core::repo::format::unpack_object(data, &engine);

    // Context-bound path (AAD = [tag] + prefix + context) — the production path
    let _ = vykar_core::repo::format::unpack_object_expect_with_context(
        data,
        vykar_core::repo::format::ObjectType::ChunkData,
        CONTEXT_CHUNK_ID,
        &engine,
    );
    let _ = vykar_core::repo::format::unpack_object_expect_with_context(
        data,
        vykar_core::repo::format::ObjectType::ChunkIndex,
        CONTEXT_INDEX,
        &engine,
    );
    let _ = vykar_core::repo::format::unpack_object_expect_with_context(
        data,
        vykar_core::repo::format::ObjectType::IndexHead,
        CONTEXT_INDEX,
        &engine,
    );
    let _ = vykar_core::repo::format::unpack_object_expect_with_context(
        data,
        vykar_core::repo::format::ObjectType::IndexSegment,
        CONTEXT_INDEX_SEGMENT,
        &engine,
    );
    let _ = vykar_core::repo::format::unpack_object_expect_with_context(
        data,
        vykar_core::repo::format::ObjectType::PackTrailer,
        vykar_core::repo::pack_trailer::TRAILER_CONTEXT,
        &engine,
    );
    let _ = vykar_core::repo::format::unpack_object_expect_with_context(
        data,
        vykar_core::repo::format::ObjectType::PackMap,
        vykar_core::repo::pack_map::PACK_MAP_CONTEXT,
        &engine,
    );
    let mut buf = data.to_vec();
    let _ = vykar_core::repo::format::unpack_object_expect_with_context_in_place(
        &mut buf,
        vykar_core::repo::format::ObjectType::IndexBase,
        CONTEXT_INDEX_BASE,
        &engine,
    );
});
