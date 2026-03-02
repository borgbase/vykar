use crate::compress::{compress_append, compressed_size_bound, Compression};
use crate::repo::format::{pack_object_streaming_with_context, ObjectType};
use vykar_crypto::CryptoEngine;
use vykar_types::chunk_id::ChunkId;
use vykar_types::error::Result;

pub(crate) struct PreparedChunk {
    pub(crate) chunk_id: ChunkId,
    pub(crate) uncompressed_size: u32,
    pub(crate) packed: Vec<u8>,
}

/// A chunk that was only hashed (xor filter said "probably exists").
pub(crate) struct HashedChunk {
    pub(crate) chunk_id: ChunkId,
    pub(crate) data: Vec<u8>,
}

/// Result of worker-side classify: either fully transformed or hash-only.
pub(crate) enum WorkerChunk {
    /// Filter miss or no filter: already compressed+encrypted.
    Prepared(PreparedChunk),
    /// Filter hit: only hashed, raw data retained for false-positive fallback.
    Hashed(HashedChunk),
}

/// Classify a single chunk: hash → xor filter check → transform or hash-only.
pub(super) fn classify_chunk(
    chunk_id: ChunkId,
    data: Vec<u8>,
    dedup_filter: Option<&xorf::Xor8>,
    compression: Compression,
    crypto: &dyn CryptoEngine,
) -> Result<WorkerChunk> {
    if let Some(filter) = dedup_filter {
        use xorf::Filter;
        let key = crate::index::dedup_cache::chunk_id_to_u64(&chunk_id);
        if filter.contains(&key) {
            return Ok(WorkerChunk::Hashed(HashedChunk { chunk_id, data }));
        }
    }
    let packed = pack_chunk_data(&chunk_id, &data, compression, crypto)?;
    Ok(WorkerChunk::Prepared(PreparedChunk {
        chunk_id,
        uncompressed_size: data.len() as u32,
        packed,
    }))
}

/// Compress and encrypt a chunk into a single allocation.
///
/// Uses `pack_object_streaming_with_context` so the compressed data is written
/// directly into the output buffer and then encrypted in-place, avoiding the
/// intermediate Vec allocations of the separate compress → pack_object chain.
fn pack_chunk_data(
    chunk_id: &ChunkId,
    data: &[u8],
    compression: Compression,
    crypto: &dyn CryptoEngine,
) -> Result<Vec<u8>> {
    let estimate = compressed_size_bound(compression, data.len());
    pack_object_streaming_with_context(
        ObjectType::ChunkData,
        &chunk_id.0,
        estimate,
        crypto,
        |buf| compress_append(compression, data, buf),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::compress::decompress;
    use crate::repo::format::unpack_object_expect_with_context;
    use vykar_crypto::PlaintextEngine;

    /// Verify pack_chunk_data output can be unpacked and decompressed to the
    /// original data — i.e., it's wire-compatible with the old
    /// compress() → pack_object_with_context() chain.
    #[test]
    fn pack_chunk_data_roundtrip() {
        let key = [0xAA; 32];
        let engine = PlaintextEngine::new(&key);
        let payload = b"the quick brown fox jumps over the lazy dog, repeatedly!";
        let chunk_id = ChunkId::compute(engine.chunk_id_key(), payload);

        let codecs = [
            Compression::None,
            Compression::Lz4,
            Compression::Zstd { level: 3 },
        ];

        for codec in codecs {
            let packed = pack_chunk_data(&chunk_id, payload, codec, &engine).unwrap();
            let compressed = unpack_object_expect_with_context(
                &packed,
                ObjectType::ChunkData,
                &chunk_id.0,
                &engine,
            )
            .unwrap();
            let recovered = decompress(&compressed).unwrap();
            assert_eq!(recovered, payload, "{codec:?}: roundtrip mismatch");
        }
    }

    /// Verify pack_chunk_data produces output identical to the old two-step
    /// compress() → pack_object_with_context() chain.
    #[test]
    fn pack_chunk_data_matches_two_step() {
        use crate::compress::compress;
        use crate::repo::format::pack_object_with_context;

        let key = [0xBB; 32];
        let engine = PlaintextEngine::new(&key);
        let payload = vec![0x42; 4096];
        let chunk_id = ChunkId::compute(engine.chunk_id_key(), &payload);

        let codecs = [
            Compression::None,
            Compression::Lz4,
            Compression::Zstd { level: 3 },
        ];

        for codec in codecs {
            let streaming = pack_chunk_data(&chunk_id, &payload, codec, &engine).unwrap();
            let compressed = compress(codec, &payload).unwrap();
            let two_step =
                pack_object_with_context(ObjectType::ChunkData, &chunk_id.0, &compressed, &engine)
                    .unwrap();
            // With PlaintextEngine, output must be byte-identical
            assert_eq!(
                streaming, two_step,
                "{codec:?}: streaming vs two-step mismatch"
            );
        }
    }

    /// Verify the AES-256-GCM encrypted path: correct context decrypts,
    /// wrong context is rejected.
    #[test]
    fn pack_chunk_data_aes_gcm_roundtrip_and_context_binding() {
        use vykar_crypto::aes_gcm::Aes256GcmEngine;

        let enc_key = [0x11; 32];
        let cid_key = [0x22; 32];
        let engine = Aes256GcmEngine::new(&enc_key, &cid_key);
        let payload = b"encrypted chunk data for context-binding test";
        let chunk_id = ChunkId::compute(engine.chunk_id_key(), payload);

        for codec in [
            Compression::None,
            Compression::Lz4,
            Compression::Zstd { level: 3 },
        ] {
            let packed = pack_chunk_data(&chunk_id, payload, codec, &engine).unwrap();

            // Correct context succeeds
            let compressed = unpack_object_expect_with_context(
                &packed,
                ObjectType::ChunkData,
                &chunk_id.0,
                &engine,
            )
            .unwrap();
            let recovered = decompress(&compressed).unwrap();
            assert_eq!(
                recovered, payload,
                "{codec:?}: encrypted roundtrip mismatch"
            );

            // Wrong context must fail (AAD mismatch → GCM tag verification failure)
            let wrong_context = [0xFF; 32];
            let result = unpack_object_expect_with_context(
                &packed,
                ObjectType::ChunkData,
                &wrong_context,
                &engine,
            );
            assert!(
                result.is_err(),
                "{codec:?}: decryption with wrong context should fail"
            );
        }
    }

    /// Verify compressed_size_bound is tight enough that the streaming buffer
    /// in pack_chunk_data never needs to reallocate.
    #[test]
    fn pack_chunk_data_no_realloc() {
        let key = [0xCC; 32];
        let engine = PlaintextEngine::new(&key);
        let payload = vec![0x42; 8192];
        let chunk_id = ChunkId::compute(engine.chunk_id_key(), &payload);

        for codec in [
            Compression::None,
            Compression::Lz4,
            Compression::Zstd { level: 3 },
        ] {
            // PlaintextEngine layout: [tag 1][plaintext]
            // The streaming function allocates Vec::with_capacity(1 + estimate).
            // Allocators may round up, so probe for the actual initial capacity.
            let estimate = compressed_size_bound(codec, payload.len());
            let requested = 1 + estimate;
            let initial_capacity = Vec::<u8>::with_capacity(requested).capacity();

            let packed = pack_chunk_data(&chunk_id, &payload, codec, &engine).unwrap();
            assert!(
                packed.capacity() <= initial_capacity,
                "{codec:?}: buffer grew beyond initial allocation \
                 (capacity {} > initial {initial_capacity}, requested {requested})",
                packed.capacity(),
            );
        }
    }
}
