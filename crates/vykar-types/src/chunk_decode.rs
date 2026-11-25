//! A stored chunk blob that failed to decode, with where it sits. The decode
//! itself (`repo::format::decode_chunk_into`) lives in vykar-core; the types
//! are here so that `VykarError::ChunkDecode` can carry them.

use crate::chunk_id::ChunkId;
use crate::error::VykarError;
use crate::pack_id::PackId;

/// Where a stored chunk blob sits, and the ID it is stored under.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BlobLocation {
    pub chunk_id: ChunkId,
    pub pack_id: PackId,
    pub pack_offset: u64,
    pub stored_size: u32,
}

/// The decode step that failed, with its cause.
#[derive(Debug)]
pub enum ChunkDecodeStage {
    /// Envelope, AEAD or object-type check. Boxed to keep the error under
    /// `clippy::result_large_err`.
    Decrypt(Box<VykarError>),
    Decompress(Box<VykarError>),
    /// Only with a size hint: the decoded length differs from it.
    SizeMismatch {
        expected: usize,
        actual: usize,
    },
    /// The recomputed chunk ID differs from `BlobLocation::chunk_id`.
    IdMismatch {
        actual: ChunkId,
    },
}

#[derive(Debug)]
pub struct ChunkDecodeError {
    pub stage: ChunkDecodeStage,
    pub at: BlobLocation,
}

impl ChunkDecodeError {
    /// The located message, without any variant prefix.
    fn located_message(&self) -> String {
        let at = self.at;
        match &self.stage {
            ChunkDecodeStage::Decrypt(e) | ChunkDecodeStage::Decompress(e) => format!(
                "chunk {} in pack {} (offset {}, size {}): {e}",
                at.chunk_id, at.pack_id, at.pack_offset, at.stored_size
            ),
            ChunkDecodeStage::SizeMismatch { expected, actual } => format!(
                "chunk {} in pack {} (offset {}, size {}) size mismatch after restore decode: \
                 expected {} bytes, got {} bytes",
                at.chunk_id, at.pack_id, at.pack_offset, at.stored_size, expected, actual
            ),
            ChunkDecodeStage::IdMismatch { actual } => {
                let expected = at.chunk_id;
                format!(
                    "chunk {expected} in pack {} (offset {}, size {}) \
                     hash mismatch after decode: expected {expected}, got {actual}",
                    at.pack_id, at.pack_offset, at.stored_size
                )
            }
        }
    }

    /// The error with the blob's location in every message (restore's form).
    pub fn into_located(self) -> VykarError {
        VykarError::ChunkDecode(Box::new(self))
    }
}

/// Decrypt and decompress failures print the located message; size and ID
/// mismatches add the `InvalidFormat` prefix, as they always have.
impl std::fmt::Display for ChunkDecodeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.stage {
            ChunkDecodeStage::Decrypt(_) | ChunkDecodeStage::Decompress(_) => {
                f.write_str(&self.located_message())
            }
            ChunkDecodeStage::SizeMismatch { .. } | ChunkDecodeStage::IdMismatch { .. } => {
                write!(f, "invalid repository format: {}", self.located_message())
            }
        }
    }
}

impl std::error::Error for ChunkDecodeError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match &self.stage {
            ChunkDecodeStage::Decrypt(e) | ChunkDecodeStage::Decompress(e) => Some(e.as_ref()),
            ChunkDecodeStage::SizeMismatch { .. } | ChunkDecodeStage::IdMismatch { .. } => None,
        }
    }
}

/// Decrypt and decompress failures keep their original error, as the read
/// paths have always returned them; size and ID mismatches carry the location.
impl From<ChunkDecodeError> for VykarError {
    fn from(e: ChunkDecodeError) -> Self {
        match e.stage {
            ChunkDecodeStage::Decrypt(inner) | ChunkDecodeStage::Decompress(inner) => *inner,
            stage => {
                let e = ChunkDecodeError { stage, at: e.at };
                VykarError::InvalidFormat(e.located_message())
            }
        }
    }
}
