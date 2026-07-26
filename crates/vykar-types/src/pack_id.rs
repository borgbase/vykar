hash_id! {
    /// A 32-byte pack file identifier computed as unkeyed BLAKE2b-256.
    PackId
}

impl PackId {
    /// Compute a pack ID as unkeyed BLAKE2b-256 of the entire pack contents.
    ///
    /// Pack IDs are storage keys for already-written objects, so the digest is
    /// pinned by known-answer tests — see `compute_known_answers` below.
    pub fn compute(data: &[u8]) -> Self {
        let hash = blake2b_simd::Params::new().hash_length(32).hash(data);
        let mut out = [0u8; 32];
        out.copy_from_slice(hash.as_bytes());
        PackId(out)
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
    fn pattern(len: usize) -> Vec<u8> {
        (0..len).map(|i| (i % 251) as u8).collect()
    }

    /// Known-answer vectors for unkeyed BLAKE2b-256.
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
                PackId::compute(&pattern(*len)).to_hex(),
                *expected,
                "unkeyed BLAKE2b-256 changed for {len}-byte input"
            );
        }
    }

    /// The active implementation must agree with the RustCrypto `blake2` crate,
    /// which is kept as a dev-dependency purely as an independent reference.
    #[test]
    fn compute_matches_rustcrypto_blake2() {
        use blake2::digest::{Update, VariableOutput};
        use blake2::Blake2bVar;

        for len in [0usize, 1, 63, 64, 127, 128, 129, 255, 1000, 4096] {
            let data = pattern(len);
            let mut reference = Blake2bVar::new(32).expect("blake2 accepts 32-byte output");
            reference.update(&data);
            let mut expected = [0u8; 32];
            reference
                .finalize_variable(&mut expected)
                .expect("finalize_variable writes 32 bytes");
            assert_eq!(
                PackId::compute(&data).to_hex(),
                hex::encode(expected),
                "divergence from RustCrypto blake2 at {len} bytes"
            );
        }
    }

    #[test]
    fn storage_key_roundtrip() {
        let id = PackId::compute(b"pack contents");
        let key = id.storage_key();
        assert!(key.starts_with(&format!("packs/{}/", id.shard_prefix())));
        assert_eq!(PackId::from_storage_key(&key), Ok(id));
    }
}
