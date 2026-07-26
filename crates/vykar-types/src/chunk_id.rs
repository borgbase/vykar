hash_id! {
    /// A 32-byte chunk identifier computed as keyed BLAKE2b-256.
    ChunkId
}

impl ChunkId {
    /// Compute a chunk ID using keyed BLAKE2b-256 (BLAKE2b-MAC with 32-byte output).
    ///
    /// This is the dedup identity for the whole repository, so the digest is
    /// pinned by known-answer tests — see `compute_known_answers` below.
    pub fn compute(key: &[u8; 32], data: &[u8]) -> Self {
        let hash = blake2b_simd::Params::new()
            .hash_length(32)
            .key(key)
            .hash(data);
        let mut out = [0u8; 32];
        out.copy_from_slice(hash.as_bytes());
        ChunkId(out)
    }

    /// First byte as a two-char hex string, used for shard directory.
    pub fn shard_prefix(&self) -> String {
        hex::encode(&self.0[..1])
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_chunk_id_key() -> [u8; 32] {
        [0xAA; 32]
    }

    #[test]
    fn compute_deterministic() {
        let key = test_chunk_id_key();
        let data = b"hello world";
        let id1 = ChunkId::compute(&key, data);
        let id2 = ChunkId::compute(&key, data);
        assert_eq!(id1, id2);
    }

    #[test]
    fn compute_different_data_different_id() {
        let key = test_chunk_id_key();
        let id1 = ChunkId::compute(&key, b"hello");
        let id2 = ChunkId::compute(&key, b"world");
        assert_ne!(id1, id2);
    }

    #[test]
    fn compute_different_key_different_id() {
        let key1 = [0xAA; 32];
        let key2 = [0xBB; 32];
        let data = b"same data";
        let id1 = ChunkId::compute(&key1, data);
        let id2 = ChunkId::compute(&key2, data);
        assert_ne!(id1, id2);
    }

    #[test]
    fn to_hex_length() {
        let key = test_chunk_id_key();
        let id = ChunkId::compute(&key, b"test");
        assert_eq!(id.to_hex().len(), 64);
    }

    #[test]
    fn shard_prefix_is_first_byte() {
        let id = ChunkId::from_bytes([0xAB; 32]);
        assert_eq!(id.shard_prefix(), "ab");
    }

    #[test]
    fn empty_data_produces_valid_id() {
        let key = test_chunk_id_key();
        let id = ChunkId::compute(&key, b"");
        assert_eq!(id.to_hex().len(), 64);
        assert_ne!(*id.as_bytes(), [0u8; 32]);
    }

    /// Deterministic filler so the vectors below stay readable.
    fn pattern(len: usize) -> Vec<u8> {
        (0..len).map(|i| (i % 251) as u8).collect()
    }

    fn counting_key() -> [u8; 32] {
        let mut key = [0u8; 32];
        for (i, b) in key.iter_mut().enumerate() {
            *b = i as u8;
        }
        key
    }

    /// Known-answer vectors for keyed BLAKE2b-256.
    ///
    /// `ChunkId::compute` is the dedup identity: any change to its output
    /// invalidates every chunk index in every existing repository. These
    /// digests pin the algorithm so a hash-implementation swap cannot silently
    /// change it. Do not regenerate them — a failure here means the digest
    /// changed, which is a compatibility break, not a stale expectation.
    #[test]
    fn compute_known_answers() {
        // (key, input length, expected hex digest)
        let cases: &[([u8; 32], usize, &str)] = &[
            (
                [0xAA; 32],
                0,
                "fd948c147bdb3bca460cae5763cddec887aff968a06545ae5b4eaa3f0054c645",
            ),
            (
                [0xAA; 32],
                1,
                "fb405f3e390ed16ca46f7db3fa701797ebb39388597756d428481b386510a49b",
            ),
            (
                [0xAA; 32],
                127,
                "a4941d8a52451b50a7472f293547fd3dacd3dff56f68a4e91b57c11c6b0cc0c1",
            ),
            (
                [0xAA; 32],
                128,
                "4a6299d6f27117cb073e5073f0e6af5c8e7be8369869686b24f482500442ecec",
            ),
            (
                [0xAA; 32],
                129,
                "ed11cdb98211d926c035f74df6cf2921f2bc1d94e23b8aad3e23402a00bf488e",
            ),
            (
                [0xAA; 32],
                1000,
                "e8fbb60049110a3e0cfd0771336d57b5b73445691c8472e54830aa3496db5753",
            ),
            (
                counting_key(),
                0,
                "4e51e7a913fc80137da52880fecca175bf81e117d5c68126dc2774033517ea0d",
            ),
            (
                counting_key(),
                1,
                "41ff93a4eaeebd3b78a93438a6f62a92ab5959c859e682b72c7def406197ca4d",
            ),
            (
                counting_key(),
                127,
                "12879e69734944f25f9ed95cbfdbb3659a307e485ed8ec118ce7c60f107357f1",
            ),
            (
                counting_key(),
                128,
                "138893f1631ef3165629515d6ed800da3771b7926dced294205c7507351deebc",
            ),
            (
                counting_key(),
                129,
                "ca60f75cbb714330c046d8f28b4ed351a3ee81776bb02a96abb646fe573e3d5c",
            ),
            (
                counting_key(),
                1000,
                "bf9c0d3a2590251349ad634ad07f03958d0be63d5f9533daf62752de734b2c76",
            ),
        ];
        for (key, len, expected) in cases {
            assert_eq!(
                ChunkId::compute(key, &pattern(*len)).to_hex(),
                *expected,
                "keyed BLAKE2b-256 changed for {len}-byte input"
            );
        }
    }

    /// The active implementation must agree with the RustCrypto `blake2` crate,
    /// which is kept as a dev-dependency purely as an independent reference.
    #[test]
    fn compute_matches_rustcrypto_blake2() {
        use blake2::digest::consts::U32;
        use blake2::digest::Mac;
        use blake2::Blake2bMac;

        for key in [[0xAA; 32], counting_key(), [0u8; 32]] {
            for len in [0usize, 1, 63, 64, 127, 128, 129, 255, 1000, 4096] {
                let data = pattern(len);
                let mut reference =
                    Blake2bMac::<U32>::new_from_slice(&key).expect("blake2 accepts 32-byte keys");
                Mac::update(&mut reference, &data);
                let expected = hex::encode(reference.finalize().into_bytes());
                assert_eq!(
                    ChunkId::compute(&key, &data).to_hex(),
                    expected,
                    "divergence from RustCrypto blake2 at {len} bytes"
                );
            }
        }
    }

    #[test]
    fn serde_roundtrip() {
        let key = test_chunk_id_key();
        let id = ChunkId::compute(&key, b"roundtrip test");
        let serialized = rmp_serde::to_vec(&id).unwrap();
        let deserialized: ChunkId = rmp_serde::from_slice(&serialized).unwrap();
        assert_eq!(id, deserialized);
    }
}
