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
const CONTEXT_INDEX: &[u8] = b"index";

fuzz_target!(|data: &[u8]| {
    let engine = Aes256GcmEngine::new(
        &ENC_KEY,
        ChunkHasher::new(HashAlgorithm::Blake2b, CID_KEY),
    );

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
});
