//! The content-digest algorithm a repository uses for chunk IDs and pack IDs.
//!
//! This is a leaf-crate type on purpose: it crosses every layer boundary in the
//! workspace (`vykar-core` → `vykar-storage` → `vykar-protocol`), so the client,
//! the storage backends and the REST wire format can all name the same
//! algorithm without any of them depending on the others.
//!
//! Which algorithm a repository uses is implied by its format version and
//! resolved once at open time — see `vykar_core::repo::RepoFormat`. Nothing
//! here decides; it only carries the decision.

use serde::{Deserialize, Serialize};

/// A 256-bit content-digest algorithm.
///
/// Only chunk IDs and pack IDs are selectable. Every other digest in the
/// workspace (TOFU fingerprint, cache file names, `PathHash`, the index
/// checksum) is unconditionally BLAKE2b for all repository formats.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum HashAlgorithm {
    /// Keyed/unkeyed BLAKE2b-256. Repository format v2.
    ///
    /// The default so that a server deserializing a request from a
    /// pre-BLAKE3 client reads the algorithm those clients actually used.
    #[default]
    Blake2b,
    /// Keyed/unkeyed BLAKE3. Repository format v3.
    Blake3,
}

impl HashAlgorithm {
    /// Lowercase wire/display name. Matches the serde representation.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Blake2b => "blake2b",
            Self::Blake3 => "blake3",
        }
    }
}

impl std::fmt::Display for HashAlgorithm {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// A streaming, unkeyed 256-bit hasher over either algorithm.
///
/// Used where pack bytes are hashed as they stream past rather than buffered:
/// the client's `PackId::compute` has the whole pack in memory and does not
/// need this, but the server's repack and verify-packs handlers do, and both
/// sides must agree on the digest by construction.
// Both variants boxed: `blake3::Hasher` is ~1.9 KiB and `blake2b_simd::State`
// ~224 bytes, so leaving either inline trips `clippy::large_enum_variant`
// (commit-blocking under CI's `-D warnings`). One allocation per pack is noise.
pub enum Hasher256 {
    Blake2b(Box<blake2b_simd::State>),
    Blake3(Box<blake3::Hasher>),
}

impl Hasher256 {
    pub fn new(algo: HashAlgorithm) -> Self {
        match algo {
            HashAlgorithm::Blake2b => Self::Blake2b(Box::new(
                blake2b_simd::Params::new().hash_length(32).to_state(),
            )),
            HashAlgorithm::Blake3 => Self::Blake3(Box::new(blake3::Hasher::new())),
        }
    }

    /// The algorithm this hasher was constructed with.
    pub fn algorithm(&self) -> HashAlgorithm {
        match self {
            Self::Blake2b(_) => HashAlgorithm::Blake2b,
            Self::Blake3(_) => HashAlgorithm::Blake3,
        }
    }

    pub fn update(&mut self, data: &[u8]) {
        match self {
            Self::Blake2b(state) => {
                state.update(data);
            }
            Self::Blake3(hasher) => {
                hasher.update(data);
            }
        }
    }

    pub fn finalize(mut self) -> [u8; 32] {
        match &mut self {
            Self::Blake2b(state) => {
                let mut out = [0u8; 32];
                out.copy_from_slice(state.finalize().as_bytes());
                out
            }
            Self::Blake3(hasher) => *hasher.finalize().as_bytes(),
        }
    }

    /// Finish and render as a 64-character lowercase hex string.
    pub fn finalize_hex(self) -> String {
        hex::encode(self.finalize())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn as_str_matches_serde_representation() {
        for algo in [HashAlgorithm::Blake2b, HashAlgorithm::Blake3] {
            let json = serde_json::to_string(&algo).expect("serializes");
            assert_eq!(json, format!("\"{}\"", algo.as_str()));
        }
    }

    #[test]
    fn default_is_blake2b() {
        // Load-bearing: `#[serde(default)]` on the REST DTOs relies on an
        // absent `hash` field meaning "pre-BLAKE3 client".
        assert_eq!(HashAlgorithm::default(), HashAlgorithm::Blake2b);
    }

    #[test]
    fn unknown_algorithm_name_is_rejected() {
        // The server returns 400 for an unknown algorithm purely because
        // deserialization fails; nothing else validates the string.
        assert!(serde_json::from_str::<HashAlgorithm>("\"blake9\"").is_err());
    }

    /// Deterministic filler so the vectors below stay readable.
    fn pattern(len: usize) -> Vec<u8> {
        (0..len).map(|i| (i % 251) as u8).collect()
    }

    #[test]
    fn streaming_matches_one_shot() {
        for algo in [HashAlgorithm::Blake2b, HashAlgorithm::Blake3] {
            for len in [0usize, 1, 1023, 1024, 1025, 4096, 8192] {
                let data = pattern(len);
                let mut one_shot = Hasher256::new(algo);
                one_shot.update(&data);

                let mut chunked = Hasher256::new(algo);
                for piece in data.chunks(7).filter(|c| !c.is_empty()) {
                    chunked.update(piece);
                }
                assert_eq!(
                    one_shot.finalize(),
                    chunked.finalize(),
                    "{algo} diverged at {len} bytes when fed in pieces"
                );
            }
        }
    }

    #[test]
    fn algorithm_round_trips_through_hasher() {
        for algo in [HashAlgorithm::Blake2b, HashAlgorithm::Blake3] {
            assert_eq!(Hasher256::new(algo).algorithm(), algo);
        }
    }

    /// `Hasher256` is the server's half of the pack digest; the client's half
    /// is `PackId::compute`. They must agree, so both are pinned to the same
    /// published vectors for the empty input.
    #[test]
    fn empty_input_known_answers() {
        assert_eq!(
            Hasher256::new(HashAlgorithm::Blake2b).finalize_hex(),
            "0e5751c026e543b2e8ab2eb06099daa1d1e5df47778f7787faab45cdf12fe3a8",
        );
        assert_eq!(
            Hasher256::new(HashAlgorithm::Blake3).finalize_hex(),
            "af1349b9f5f9a1a6a0404dea36dcc9499bcb25c9adc112b7cc9a93cae41f3262",
        );
    }
}
