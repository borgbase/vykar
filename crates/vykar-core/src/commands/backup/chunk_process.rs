use std::borrow::Cow;

use crate::compress::{Compression, compress_append, compressed_size_bound};
use crate::index::table::{DedupPrefilter, PrefilterVerdict};
use crate::repo::format::{ObjectType, pack_object_streaming_with_context};
use vykar_crypto::CryptoEngine;
use vykar_types::chunk_id::ChunkId;
use vykar_types::error::Result;

pub(crate) struct PreparedChunk {
    pub(crate) chunk_id: ChunkId,
    pub(crate) uncompressed_size: u32,
    pub(crate) packed: Vec<u8>,
}

/// Where the worker found an existing chunk in the session's view.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ExistingAt {
    /// At this position of the view's table, so the consumer can record the
    /// refcount bump without a second lookup.
    Table(u32),
    /// Among the additions of the view's segments: bumped by key.
    Added,
}

/// A chunk the worker confirmed in the session's view.
pub(crate) struct ExistingChunk {
    pub(crate) chunk_id: ChunkId,
    pub(crate) uncompressed_size: u32,
    pub(crate) stored_size: u32,
    pub(crate) at: ExistingAt,
}

/// Result of worker-side classify.
pub(crate) enum WorkerChunk {
    /// Not in the session's table, or no filter: already
    /// compressed+encrypted. The consumer still dedups it against chunks
    /// added earlier in this session.
    Prepared(PreparedChunk),
    /// Confirmed dedup hit in the session's table: no data retained.
    Existing(ExistingChunk),
}

/// Classify a single chunk: hash → prefilter → transform, or data-less hit.
///
/// `data` is usually a slice lent by the chunker's read buffer. Filter hits
/// are confirmed against the table right here, so a worker holding a whole
/// already-stored file keeps a few bytes per chunk instead of a raw copy, and
/// a false positive is packed like any other new chunk. The transform path
/// reads `data` in place.
pub(super) fn classify_chunk(
    chunk_id: ChunkId,
    data: Cow<'_, [u8]>,
    prefilter: Option<&DedupPrefilter>,
    compression: Compression,
    crypto: &dyn CryptoEngine,
) -> Result<WorkerChunk> {
    match prefilter.map(|p| p.lookup(&chunk_id)) {
        None | Some(PrefilterVerdict::Absent) => {}
        Some(PrefilterVerdict::Present {
            stored_size,
            position,
        }) => {
            return Ok(WorkerChunk::Existing(ExistingChunk {
                chunk_id,
                uncompressed_size: data.len() as u32,
                stored_size,
                at: ExistingAt::Table(position),
            }));
        }
        Some(PrefilterVerdict::PresentAdded { stored_size }) => {
            return Ok(WorkerChunk::Existing(ExistingChunk {
                chunk_id,
                uncompressed_size: data.len() as u32,
                stored_size,
                at: ExistingAt::Added,
            }));
        }
    }
    let mut packed = pack_chunk_data(&chunk_id, &data, compression, crypto)?;
    // Compression reserves for the worst case. These buffers can wait for
    // an entire segment in the pipeline, so retaining input-sized capacity
    // for compressible chunks wastes substantial memory. Only shrink when
    // the saving is large enough to justify a possible realloc/copy.
    if packed.capacity() / 2 > packed.len() && packed.capacity() - packed.len() >= 64 * 1024 {
        packed.shrink_to_fit();
    }
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
        chunk_id.as_bytes(),
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

    use crate::index::ChunkIndex;
    use crate::index::dedup_cache::chunk_id_to_u64;
    use vykar_types::pack_id::PackId;

    /// Prefilter over a table holding `stored` chunks.
    fn table_prefilter(stored: &[(ChunkId, u32)]) -> ((), DedupPrefilter) {
        let mut index = ChunkIndex::new();
        for &(id, stored_size) in stored {
            index.add(id, stored_size, PackId::from_bytes([0x01; 32]), 0);
        }
        ((), crate::testutil::prefilter_of(&index))
    }

    #[test]
    fn confirmed_presession_hit_retains_no_data() {
        let engine = PlaintextEngine::new(crate::testutil::chunk_hasher_for([0xAA; 32]));
        let payload = vec![0x42; 1024];
        let chunk_id = ChunkId::compute(engine.chunk_hasher(), &payload);
        let (_dir, prefilter) = table_prefilter(&[(chunk_id, 77)]);
        let result = classify_chunk(
            chunk_id,
            Cow::Borrowed(&payload),
            Some(&prefilter),
            Compression::Lz4,
            &engine,
        )
        .unwrap();
        let WorkerChunk::Existing(chunk) = result else {
            panic!("chunk in the table must be a data-less hit");
        };
        assert_eq!(chunk.chunk_id, chunk_id);
        assert_eq!(chunk.uncompressed_size, 1024);
        assert_eq!(chunk.stored_size, 77);
    }

    /// A filter false positive (same 64-bit filter key, different chunk) is
    /// ruled out by the table and packed in the worker.
    #[test]
    fn filter_false_positive_is_packed_in_worker() {
        let engine = PlaintextEngine::new(crate::testutil::chunk_hasher_for([0xAA; 32]));
        let payload = vec![0x42; 1024];
        let chunk_id = ChunkId::compute(engine.chunk_hasher(), &payload);
        let mut colliding = *chunk_id.as_bytes();
        colliding[31] ^= 0xFF;
        let colliding = ChunkId::from_bytes(colliding);
        assert_eq!(chunk_id_to_u64(&colliding), chunk_id_to_u64(&chunk_id));
        let (_dir, prefilter) = table_prefilter(&[(colliding, 77)]);
        let result = classify_chunk(
            chunk_id,
            Cow::Borrowed(&payload),
            Some(&prefilter),
            Compression::Lz4,
            &engine,
        )
        .unwrap();
        let WorkerChunk::Prepared(chunk) = result else {
            panic!("false positive must be packed in the worker");
        };
        assert_eq!(chunk.chunk_id, chunk_id);
        assert_eq!(chunk.uncompressed_size, 1024);
    }

    #[test]
    fn prepared_compressible_chunks_release_excess_capacity() {
        let engine = PlaintextEngine::new(crate::testutil::chunk_hasher_for([0xAA; 32]));
        let payload = vec![0x42; 1024 * 1024];
        let chunk_id = ChunkId::compute(engine.chunk_hasher(), &payload);
        for compression in [Compression::Lz4, Compression::Zstd { level: 3 }] {
            let chunk = classify_chunk(
                chunk_id,
                Cow::Borrowed(&payload),
                None,
                compression,
                &engine,
            )
            .unwrap();
            let WorkerChunk::Prepared(prepared) = chunk else {
                panic!("no filter must prepare the chunk");
            };
            assert!(prepared.packed.capacity() < payload.len() / 2);
            assert_eq!(prepared.chunk_id, chunk_id);
            assert_eq!(prepared.uncompressed_size as usize, payload.len());
            let compressed = unpack_object_expect_with_context(
                &prepared.packed,
                ObjectType::ChunkData,
                chunk_id.as_bytes(),
                &engine,
            )
            .unwrap();
            assert_eq!(decompress(&compressed).unwrap(), payload);
        }
    }

    /// Verify pack_chunk_data output can be unpacked and decompressed to the
    /// original data — i.e., it's wire-compatible with the old
    /// compress() → pack_object_with_context() chain.
    #[test]
    fn pack_chunk_data_roundtrip() {
        let key = [0xAA; 32];
        let engine = PlaintextEngine::new(crate::testutil::chunk_hasher_for(key));
        let payload = b"the quick brown fox jumps over the lazy dog, repeatedly!";
        let chunk_id = ChunkId::compute(engine.chunk_hasher(), payload);

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
                chunk_id.as_bytes(),
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
        let engine = PlaintextEngine::new(crate::testutil::chunk_hasher_for(key));
        let payload = vec![0x42; 4096];
        let chunk_id = ChunkId::compute(engine.chunk_hasher(), &payload);

        let codecs = [
            Compression::None,
            Compression::Lz4,
            Compression::Zstd { level: 3 },
        ];

        for codec in codecs {
            let streaming = pack_chunk_data(&chunk_id, &payload, codec, &engine).unwrap();
            let compressed = compress(codec, &payload).unwrap();
            let two_step = pack_object_with_context(
                ObjectType::ChunkData,
                chunk_id.as_bytes(),
                &compressed,
                &engine,
            )
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
        let engine = Aes256GcmEngine::new(&enc_key, crate::testutil::chunk_hasher_for(cid_key));
        let payload = b"encrypted chunk data for context-binding test";
        let chunk_id = ChunkId::compute(engine.chunk_hasher(), payload);

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
                chunk_id.as_bytes(),
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
        let engine = PlaintextEngine::new(crate::testutil::chunk_hasher_for(key));
        let payload = vec![0x42; 8192];
        let chunk_id = ChunkId::compute(engine.chunk_hasher(), &payload);

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
