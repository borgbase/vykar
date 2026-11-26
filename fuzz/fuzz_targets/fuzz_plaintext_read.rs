#![no_main]
// Plaintext-only: 10-single-protocol step 3 removes this.
//! The read path of an unencrypted repository through `decode_chunk_into`,
//! checked against the unpack + decompress composition: pack scan, envelope,
//! decompression, then the chunk-ID check that is the only integrity check
//! without an AEAD tag.
//!
//! Input: a control byte (bit 0: BLAKE3 instead of BLAKE2b), the 32-byte
//! chunk ID the index claims for every blob, a little-endian `u32` size
//! hint as a snapshot item would carry it, then the pack.

use libfuzzer_sys::fuzz_target;
use vykar_core::compress::decompress;
use vykar_core::repo::format::{
    BlobLocation, ChunkDecodeStage, ObjectType, decode_chunk_into,
    unpack_object_expect_with_context,
};
use vykar_core::repo::pack::scan_pack_blobs_bytes;
use vykar_crypto::{CryptoEngine, PlaintextEngine};
use vykar_types::chunk_id::{ChunkHasher, ChunkId};
use vykar_types::hash::HashAlgorithm;
use vykar_types::pack_id::PackId;

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
    // Capped: the bulk decoder reserves a buffer of the hint (at most
    // 32 MiB), and allocating that would dominate every run.
    let hint = (u32::from_le_bytes(header[33..37].try_into().unwrap()) as usize).min(1 << 20);
    let engine = PlaintextEngine::new(ChunkHasher::new(algorithm, CHUNK_ID_KEY));

    let Ok(blobs) = scan_pack_blobs_bytes(pack) else {
        return;
    };
    for (offset, len) in blobs {
        let blob = &pack[offset as usize..offset as usize + len as usize];
        let at = BlobLocation {
            chunk_id,
            pack_id: PackId::from_bytes([0; 32]),
            pack_offset: offset,
            stored_size: len,
        };
        let reference = unpack_object_expect_with_context(
            blob,
            ObjectType::ChunkData,
            chunk_id.as_bytes(),
            &engine,
        )
        .and_then(|c| decompress(&c));
        let mut plaintext = Vec::new();
        let decoded =
            decode_chunk_into(&mut blob.to_vec(), at, &engine, None, false, &mut plaintext);
        match (&reference, &decoded) {
            (Err(_), Err(e)) => assert!(
                matches!(
                    e.stage,
                    ChunkDecodeStage::Decrypt(_) | ChunkDecodeStage::Decompress(_)
                ),
                "{e:?}"
            ),
            (Err(e), Ok(())) => panic!("decoder accepted a blob the reference rejects: {e}"),
            (Ok(p), Err(e)) => match e.stage {
                ChunkDecodeStage::IdMismatch { actual } => {
                    assert_eq!(actual, ChunkId::compute(engine.chunk_hasher(), p));
                    assert_ne!(actual, chunk_id);
                }
                _ => panic!("decoder rejected a blob the reference decodes: {e:?}"),
            },
            (Ok(p), Ok(())) => {
                assert_eq!(&plaintext, p);
                assert_eq!(ChunkId::compute(engine.chunk_hasher(), p), chunk_id);
            }
        }
        if reference.is_err() || decoded.is_err() {
            continue;
        }
        // Restore decodes with the size the item claims. A correct size
        // gives the same bytes; a wrong one may fail, never differ.
        let mut hinted = Vec::new();
        decode_chunk_into(
            &mut blob.to_vec(),
            at,
            &engine,
            Some(plaintext.len()),
            false,
            &mut hinted,
        )
        .unwrap();
        assert_eq!(hinted, plaintext);
        match decode_chunk_into(
            &mut blob.to_vec(),
            at,
            &engine,
            Some(hint),
            false,
            &mut hinted,
        ) {
            Ok(()) => assert_eq!(hinted, plaintext),
            Err(e) => match e.stage {
                ChunkDecodeStage::SizeMismatch { expected, actual } => {
                    assert_eq!(expected, hint);
                    assert_eq!(actual, plaintext.len());
                }
                ChunkDecodeStage::Decompress(_) => {}
                _ => panic!("a wrong size hint failed at the wrong stage: {e:?}"),
            },
        }
    }
});
