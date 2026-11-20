#![no_main]
//! The read path of an unencrypted repository, composed as `read_chunk_at`
//! composes it: pack scan, envelope, decompression, then the chunk-ID check
//! that is the only integrity check without an AEAD tag.
//!
//! Input: a control byte (bit 0: BLAKE3 instead of BLAKE2b), the 32-byte
//! chunk ID the index claims for every blob, a little-endian `u32` size
//! hint as a snapshot item would carry it, then the pack.

use libfuzzer_sys::fuzz_target;
use vykar_core::compress::{decompress, decompress_with_hint};
use vykar_core::repo::format::{ObjectType, unpack_object_expect_with_context};
use vykar_core::repo::pack::scan_pack_blobs_bytes;
use vykar_crypto::{CryptoEngine, PlaintextEngine};
use vykar_types::chunk_id::{ChunkHasher, ChunkId};
use vykar_types::hash::HashAlgorithm;

/// Chunk-ID key of the generator's seeds.
const CHUNK_ID_KEY: [u8; 32] = [0x5A; 32];
const HEADER_LEN: usize = 1 + 32 + 4;

fuzz_target!(|data: &[u8]| {
    let Some((header, pack)) = data.split_at_checked(HEADER_LEN) else {
        return;
    };
    let algorithm = if header[0] & 1 == 1 {
        HashAlgorithm::Blake3
    } else {
        HashAlgorithm::Blake2b
    };
    let chunk_id = ChunkId::from_bytes(header[1..33].try_into().unwrap());
    // Capped: the bulk decoder zero-fills a buffer of the hint (at most
    // 32 MiB), which would dominate every run.
    let hint = (u32::from_le_bytes(header[33..37].try_into().unwrap()) as usize).min(1 << 20);
    let engine = PlaintextEngine::new(ChunkHasher::new(algorithm, CHUNK_ID_KEY));

    let Ok(blobs) = scan_pack_blobs_bytes(pack) else {
        return;
    };
    for (offset, len) in blobs {
        let blob = &pack[offset as usize..offset as usize + len as usize];
        let Ok(compressed) = unpack_object_expect_with_context(
            blob,
            ObjectType::ChunkData,
            chunk_id.as_bytes(),
            &engine,
        ) else {
            continue;
        };
        let Ok(plaintext) = decompress(&compressed) else {
            continue;
        };
        let verified =
            vykar_core::fuzzing::verify_chunk_id(engine.chunk_hasher(), &chunk_id, &plaintext);
        assert_eq!(
            verified.is_ok(),
            ChunkId::compute(engine.chunk_hasher(), &plaintext) == chunk_id
        );
        if verified.is_err() {
            continue;
        }
        // Restore decompresses with the size the item claims. A correct
        // size gives the same bytes; a wrong one may fail, never differ.
        assert_eq!(
            decompress_with_hint(&compressed, Some(plaintext.len())).unwrap(),
            plaintext
        );
        if let Ok(hinted) = decompress_with_hint(&compressed, Some(hint)) {
            assert_eq!(hinted, plaintext);
        }
    }
});
