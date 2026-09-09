use crate::hash::HashAlgorithm;

hash_id! {
    /// A 32-byte pack file identifier computed as an unkeyed 256-bit hash.
    PackId
}

impl PackId {
    /// Compute a pack ID as an unkeyed hash of the entire pack contents.
    ///
    /// Unkeyed in both algorithms, matching the original BLAKE2b semantics: a
    /// pack ID is a storage key that the server must be able to recompute from
    /// the bytes alone, without any repository key.
    ///
    /// Pack IDs are storage keys for already-written objects, so both digests
    /// are pinned by known-answer tests — see `compute_known_answers` below.
    pub fn compute(data: &[u8], algo: HashAlgorithm) -> Self {
        match algo {
            HashAlgorithm::Blake2b => {
                let hash = blake2b_simd::Params::new().hash_length(32).hash(data);
                let mut out = [0u8; 32];
                out.copy_from_slice(hash.as_bytes());
                PackId(out)
            }
            HashAlgorithm::Blake3 => PackId(*blake3::hash(data).as_bytes()),
        }
    }

    /// First byte as a two-char hex string, used for shard directory.
    pub fn shard_prefix(&self) -> String {
        hex::encode(&self.0[..1])
    }

    /// Storage key path: `packs/<shard>/<full_hex>`.
    pub fn storage_key(&self) -> String {
        format!("packs/{}/{}", self.shard_prefix(), self.to_hex())
    }

    /// Parse a `PackId` from a 64-character hex string.
    ///
    /// # Errors
    ///
    /// Returns an error if `hex_str` is not valid hex or does not decode to
    /// exactly 32 bytes.
    pub fn from_hex(hex_str: &str) -> std::result::Result<Self, String> {
        let bytes = hex::decode(hex_str).map_err(|e| format!("invalid hex: {e}"))?;
        if bytes.len() != 32 {
            return Err(format!("expected 32 bytes, got {}", bytes.len()));
        }
        let mut arr = [0u8; 32];
        arr.copy_from_slice(&bytes);
        Ok(PackId(arr))
    }

    /// Parse a `PackId` from a storage key path like `packs/ab/<hex>`.
    ///
    /// # Errors
    ///
    /// Returns an error if the final path component is not a valid full pack
    /// ID hex string.
    pub fn from_storage_key(key: &str) -> std::result::Result<Self, String> {
        let hex_str = key
            .rsplit('/')
            .next()
            .ok_or_else(|| "empty storage key".to_string())?;
        Self::from_hex(hex_str)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Deterministic filler so the vectors below stay readable.
    ///
    /// Also exactly the filler the official BLAKE3 test vectors use, which is
    /// what lets `blake3_matches_official_test_vectors` compare directly.
    fn pattern(len: usize) -> Vec<u8> {
        (0..len).map(|i| (i % 251) as u8).collect()
    }

    /// Known-answer vectors for unkeyed BLAKE2b-256 (repository format v2).
    ///
    /// Pack IDs are storage keys for already-written pack files, so a digest
    /// change orphans every pack in every existing repository. These pin the
    /// algorithm across hash-implementation swaps; a failure means the digest
    /// changed, not that the expectation is stale. The empty-input vector is
    /// the published BLAKE2b-256 test value.
    #[test]
    fn compute_known_answers() {
        let cases: &[(usize, &str)] = &[
            (
                0,
                "0e5751c026e543b2e8ab2eb06099daa1d1e5df47778f7787faab45cdf12fe3a8",
            ),
            (
                1,
                "03170a2e7597b7b7e3d84c05391d139a62b157e78786d8c082f29dcf4c111314",
            ),
            (
                127,
                "f2fe67ff342e21b8f45e8f2e0bcd1d9243245d50ee6c78042e9c491388791c72",
            ),
            (
                128,
                "c3582f71ebb2be66fa5dd750f80baae97554f3b015663c8be377cfcb2488c1d1",
            ),
            (
                129,
                "f7f3c46ba2564ff4c4c162da1f5b605f9f1c4aa6a20652a9f9a337c1a2f5b9c9",
            ),
            (
                1000,
                "b372d0608f720c8c3dd41e9c8eecb10143b41abe520b616607e754bf79c08331",
            ),
        ];
        for (len, expected) in cases {
            assert_eq!(
                PackId::compute(&pattern(*len), HashAlgorithm::Blake2b).to_hex(),
                *expected,
                "unkeyed BLAKE2b-256 changed for {len}-byte input"
            );
        }
    }

    /// Known-answer vectors for unkeyed BLAKE3 (repository format v3), pinning
    /// `PackId::compute` itself rather than the `blake3` crate.
    ///
    /// These are simultaneously the project's official *unkeyed* vectors — the
    /// published `hash` column for the same input filler — so they also pin the
    /// tree mode across the 1024-byte chunk boundary and its subtree joins.
    ///
    /// <https://github.com/BLAKE3-team/BLAKE3/blob/master/test_vectors/test_vectors.json>
    #[test]
    fn compute_known_answers_blake3() {
        let cases: &[(usize, &str)] = &[
            (
                0,
                "af1349b9f5f9a1a6a0404dea36dcc9499bcb25c9adc112b7cc9a93cae41f3262",
            ),
            (
                1,
                "2d3adedff11b61f14c886e35afa036736dcd87a74d27b5c1510225d0f592e213",
            ),
            (
                1023,
                "10108970eeda3eb932baac1428c7a2163b0e924c9a9e25b35bba72b28f70bd11",
            ),
            (
                1024,
                "42214739f095a406f3fc83deb889744ac00df831c10daa55189b5d121c855af7",
            ),
            (
                1025,
                "d00278ae47eb27b34faecf67b4fe263f82d5412916c1ffd97c8cb7fb814b8444",
            ),
            (
                2048,
                "e776b6028c7cd22a4d0ba182a8bf62205d2ef576467e838ed6f2529b85fba24a",
            ),
            (
                2049,
                "5f4d72f40d7a5f82b15ca2b2e44b1de3c2ef86c426c95c1af0b6879522563030",
            ),
            (
                4096,
                "015094013f57a5277b59d8475c0501042c0b642e531b0a1c8f58d2163229e969",
            ),
            (
                8192,
                "aae792484c8efe4f19e2ca7d371d8c467ffb10748d8a5a1ae579948f718a2a63",
            ),
        ];
        for (len, expected) in cases {
            assert_eq!(
                PackId::compute(&pattern(*len), HashAlgorithm::Blake3).to_hex(),
                *expected,
                "unkeyed BLAKE3 changed for {len}-byte input"
            );
        }
    }

    #[test]
    fn algorithms_disagree_on_the_same_input() {
        let data = pattern(4096);
        assert_ne!(
            PackId::compute(&data, HashAlgorithm::Blake2b),
            PackId::compute(&data, HashAlgorithm::Blake3)
        );
    }

    /// The BLAKE2b path must agree with the RustCrypto `blake2` crate, which is
    /// kept as a dev-dependency purely as an independent reference.
    #[test]
    fn compute_matches_rustcrypto_blake2() {
        use blake2::{Blake2b256, Digest};

        for len in [0usize, 1, 63, 64, 127, 128, 129, 255, 1000, 4096] {
            let data = pattern(len);
            let mut reference = Blake2b256::new();
            reference.update(&data);
            let expected = reference.finalize();
            assert_eq!(
                PackId::compute(&data, HashAlgorithm::Blake2b).to_hex(),
                hex::encode(expected),
                "divergence from RustCrypto blake2 at {len} bytes"
            );
        }
    }

    /// `Hasher256` is what the server uses to recompute a pack ID from bytes it
    /// streams past. It must agree with the client's `PackId::compute` in both
    /// algorithms, or a server-side repack would name its output pack wrongly.
    #[test]
    fn matches_streaming_hasher256() {
        for algo in [HashAlgorithm::Blake2b, HashAlgorithm::Blake3] {
            for len in [0usize, 1, 1024, 4096, 8192] {
                let data = pattern(len);
                let mut streaming = crate::hash::Hasher256::new(algo);
                for piece in data.chunks(1000).filter(|c| !c.is_empty()) {
                    streaming.update(piece);
                }
                assert_eq!(
                    streaming.finalize_hex(),
                    PackId::compute(&data, algo).to_hex(),
                    "{algo} streaming digest disagrees with PackId::compute at {len} bytes"
                );
            }
        }
    }

    #[test]
    fn storage_key_roundtrip() {
        for algo in [HashAlgorithm::Blake2b, HashAlgorithm::Blake3] {
            let id = PackId::compute(b"pack contents", algo);
            let key = id.storage_key();
            assert!(key.starts_with(&format!("packs/{}/", id.shard_prefix())));
            assert_eq!(PackId::from_storage_key(&key), Ok(id));
        }
    }
}
