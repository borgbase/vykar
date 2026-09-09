use crate::hash::HashAlgorithm;
use zeroize::{Zeroize, ZeroizeOnDrop};

hash_id! {
    /// A 32-byte chunk identifier computed as a keyed 256-bit hash.
    ChunkId
}

/// The chunk-ID key bundled with the algorithm that consumes it.
///
/// The key alone was threaded through the whole chunking path; widening it to
/// carry the algorithm makes it structurally impossible to compute a chunk ID
/// without having chosen one, and rules out pairing a v2 repository's key with
/// v3 hashing.
///
/// For an encrypted repository the key is secret material from
/// `keys/repokey`, so the type is not `Copy` and zeroizes the key on drop.
/// Engines own it and hand out references; worker threads that need an owned
/// value clone it explicitly.
#[derive(Clone, Zeroize, ZeroizeOnDrop)]
pub struct ChunkHasher {
    #[zeroize(skip)]
    algo: HashAlgorithm,
    key: [u8; 32],
}

impl ChunkHasher {
    pub const fn new(algo: HashAlgorithm, key: [u8; 32]) -> Self {
        Self { algo, key }
    }

    pub const fn algorithm(&self) -> HashAlgorithm {
        self.algo
    }

    /// The raw 32-byte key.
    ///
    /// For the callers that hash the key itself rather than hashing *with*
    /// it: the TOFU identity fingerprint and the `check` runner's fingerprint
    /// comparison.
    pub const fn key(&self) -> &[u8; 32] {
        &self.key
    }
}

/// Redacting: for an encrypted repository the chunk-ID key is secret key
/// material from `keys/repokey`, and this type is reachable from structs that
/// derive `Debug`.
impl std::fmt::Debug for ChunkHasher {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ChunkHasher")
            .field("algo", &self.algo)
            .field("key", &"<redacted>")
            .finish()
    }
}

impl ChunkId {
    /// Compute a chunk ID with `hasher`'s algorithm and key.
    ///
    /// **Always keyed, in both algorithms.** `index/hasher.rs` takes the first
    /// eight bytes of this digest as a `HashMap` bucket key, which is only safe
    /// because an attacker who controls chunk content cannot predict the
    /// digest. An unkeyed `blake3::hash` here would be a HashDoS hole.
    ///
    /// This is the dedup identity for the whole repository, so both digests are
    /// pinned by known-answer tests — see `compute_known_answers` below.
    pub fn compute(hasher: &ChunkHasher, data: &[u8]) -> Self {
        match hasher.algorithm() {
            HashAlgorithm::Blake2b => {
                let hash = blake2b_simd::Params::new()
                    .hash_length(32)
                    .key(hasher.key())
                    .hash(data);
                let mut out = [0u8; 32];
                out.copy_from_slice(hash.as_bytes());
                ChunkId(out)
            }
            HashAlgorithm::Blake3 => ChunkId(*blake3::keyed_hash(hasher.key(), data).as_bytes()),
        }
    }

    /// First byte as a two-char hex string, used for shard directory.
    pub fn shard_prefix(&self) -> String {
        hex::encode(&self.0[..1])
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_key() -> [u8; 32] {
        [0xAA; 32]
    }

    /// BLAKE2b hasher over `key` — the format-v2 configuration.
    fn b2(key: [u8; 32]) -> ChunkHasher {
        ChunkHasher::new(HashAlgorithm::Blake2b, key)
    }

    /// BLAKE3 hasher over `key` — the format-v3 configuration.
    fn b3(key: [u8; 32]) -> ChunkHasher {
        ChunkHasher::new(HashAlgorithm::Blake3, key)
    }

    #[test]
    fn compute_deterministic() {
        let data = b"hello world";
        for hasher in [b2(test_key()), b3(test_key())] {
            assert_eq!(
                ChunkId::compute(&hasher, data),
                ChunkId::compute(&hasher, data)
            );
        }
    }

    #[test]
    fn compute_different_data_different_id() {
        for hasher in [b2(test_key()), b3(test_key())] {
            assert_ne!(
                ChunkId::compute(&hasher, b"hello"),
                ChunkId::compute(&hasher, b"world")
            );
        }
    }

    #[test]
    fn compute_different_key_different_id() {
        let data = b"same data";
        for (a, b) in [
            (b2([0xAA; 32]), b2([0xBB; 32])),
            (b3([0xAA; 32]), b3([0xBB; 32])),
        ] {
            assert_ne!(ChunkId::compute(&a, data), ChunkId::compute(&b, data));
        }
    }

    /// The same key under the two algorithms must never collide — that is what
    /// makes a repository's format version part of its dedup identity.
    #[test]
    fn algorithms_disagree_on_the_same_input() {
        let data = b"format v2 vs v3";
        assert_ne!(
            ChunkId::compute(&b2(test_key()), data),
            ChunkId::compute(&b3(test_key()), data)
        );
    }

    #[test]
    fn to_hex_length() {
        for hasher in [b2(test_key()), b3(test_key())] {
            assert_eq!(ChunkId::compute(&hasher, b"test").to_hex().len(), 64);
        }
    }

    #[test]
    fn shard_prefix_is_first_byte() {
        let id = ChunkId::from_bytes([0xAB; 32]);
        assert_eq!(id.shard_prefix(), "ab");
    }

    #[test]
    fn empty_data_produces_valid_id() {
        for hasher in [b2(test_key()), b3(test_key())] {
            let id = ChunkId::compute(&hasher, b"");
            assert_eq!(id.to_hex().len(), 64);
            assert_ne!(*id.as_bytes(), [0u8; 32]);
        }
    }

    #[test]
    fn debug_does_not_leak_the_key() {
        let rendered = format!("{:?}", b3(test_key()));
        assert!(rendered.contains("Blake3"), "got: {rendered}");
        assert!(
            !rendered.contains("170") && !rendered.contains("aa"),
            "chunk-ID key leaked into Debug output: {rendered}"
        );
    }

    /// Deterministic filler so the vectors below stay readable.
    ///
    /// Also exactly the filler the official BLAKE3 test vectors use, which is
    /// what lets `blake3_matches_official_test_vectors` compare directly.
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

    /// Known-answer vectors for keyed BLAKE2b-256 (repository format v2).
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
                ChunkId::compute(&b2(*key), &pattern(*len)).to_hex(),
                *expected,
                "keyed BLAKE2b-256 changed for {len}-byte input"
            );
        }
    }

    /// Known-answer vectors for keyed BLAKE3 (repository format v3).
    ///
    /// These pin `ChunkId::compute` itself, not the `blake3` crate. An
    /// assertion like `keyed_hash(k, d) != hash(d)` would only test the
    /// dependency and would still pass if `compute` were changed to unkeyed
    /// hashing; fixed digests catch that.
    ///
    /// The lengths straddle BLAKE3's 1024-byte chunk boundary and its subtree
    /// joins (0, 1, 1023, 1024, 1025, 2048, 2049, 4096, 8192), which is where a
    /// wrong tree-mode implementation diverges.
    #[test]
    fn compute_known_answers_blake3() {
        let aa: &[(usize, &str)] = &[
            (
                0,
                "f83ab8799bfb5e29abcd7319730deaf5577cbc8a9dd91f73d0dd05330b4fd676",
            ),
            (
                1,
                "f9066d4da85b43209d410533df1f16c672d340007649abc10607ef65ced79ab9",
            ),
            (
                1023,
                "74a0a6f8f82d88f74800eafa3985b876d5cd35e3b0f4634906aebf6d9457aa4d",
            ),
            (
                1024,
                "84bf6e0ad0c1582dde74e0ad6377d9f2ec51e66e3f39e49321533f08547b9bf0",
            ),
            (
                1025,
                "2a4fb3011c64515107ff5579bcc8f367cb076921593a8536f3230fbeea660275",
            ),
            (
                2048,
                "9f582b820ebf01d51726d7529805faf02dc5a8a0cec4c1b00cbfb97edd3ceca9",
            ),
            (
                2049,
                "5d9d5f0088be8cffba15e86d5491194cab2dedcbbcc6d9d82493a4f1b69723db",
            ),
            (
                4096,
                "e02aaff1cea7ddf1a5f23038005e9313c9cf9ba209b921c7d95aebe186a0c954",
            ),
            (
                8192,
                "6a47a62a97fa1d57fe4dcf47adcdddaa5625d745678240bb817607b46bd092f0",
            ),
        ];
        let counting: &[(usize, &str)] = &[
            (
                0,
                "73492b19995d71cdb1e9d74decc09809eb732f1b00bc95c27cb15f9dd4d6478f",
            ),
            (
                1,
                "d08b45c6b127ee94f3f8527a0b82a5f80be1695a0eaec6022e772c0eb95a7e8b",
            ),
            (
                1023,
                "da1f18069871512af22af9f13dc005800dfd52c55f42753b5ae718086fe2ee44",
            ),
            (
                1024,
                "f45a9249a627fdf1fcf13c0e6376f6a9a9b2056d6e1b5693a4b119a3453665f9",
            ),
            (
                1025,
                "82223147a9b804a0c3f9a921b8d8aee250d1a51bb76be72152e6d5e8f27349b3",
            ),
            (
                2048,
                "636bfa717d4f9fc3e59da9b2e5cce6a2b78eb70469c0fce49da38b5419892423",
            ),
            (
                2049,
                "5442eec85e3fd173dcff07c39cd8cff9689f17224471e655618ed728cf03b056",
            ),
            (
                4096,
                "e8c6e859e0480c4b062457defd04d2f4303b6cc280a0fe080ec5c4346a171937",
            ),
            (
                8192,
                "c659141d9d7e6efafd2f274d4307b9ab3369f058c6d03cd5ba17d4518d77bd49",
            ),
        ];
        for (key, cases) in [([0xAA; 32], aa), (counting_key(), counting)] {
            for (len, expected) in cases {
                assert_eq!(
                    ChunkId::compute(&b3(key), &pattern(*len)).to_hex(),
                    *expected,
                    "keyed BLAKE3 changed for {len}-byte input"
                );
            }
        }
    }

    /// Cross-check the BLAKE3 tree mode against the project's published test
    /// vectors, using their key and their input filler.
    ///
    /// <https://github.com/BLAKE3-team/BLAKE3/blob/master/test_vectors/test_vectors.json>
    #[test]
    fn blake3_matches_official_test_vectors() {
        let key: [u8; 32] = *b"whats the Elvish word for friend";
        let cases: &[(usize, &str)] = &[
            (
                0,
                "92b2b75604ed3c761f9d6f62392c8a9227ad0ea3f09573e783f1498a4ed60d26",
            ),
            (
                1,
                "6d7878dfff2f485635d39013278ae14f1454b8c0a3a2d34bc1ab38228a80c95b",
            ),
            (
                1023,
                "c951ecdf03288d0fcc96ee3413563d8a6d3589547f2c2fb36d9786470f1b9d6e",
            ),
            (
                1024,
                "75c46f6f3d9eb4f55ecaaee480db732e6c2105546f1e675003687c31719c7ba4",
            ),
            (
                1025,
                "357dc55de0c7e382c900fd6e320acc04146be01db6a8ce7210b7189bd664ea69",
            ),
            (
                2048,
                "879cf1fa2ea0e79126cb1063617a05b6ad9d0b696d0d757cf053439f60a99dd1",
            ),
            (
                2049,
                "9f29700902f7c86e514ddc4df1e3049f258b2472b6dd5267f61bf13983b78dd5",
            ),
            (
                4096,
                "befc660aea2f1718884cd8deb9902811d332f4fc4a38cf7c7300d597a081bfc0",
            ),
            (
                8192,
                "dc9637c8845a770b4cbf76b8daec0eebf7dc2eac11498517f08d44c8fc00d58a",
            ),
        ];
        for (len, expected) in cases {
            assert_eq!(
                ChunkId::compute(&b3(key), &pattern(*len)).to_hex(),
                *expected,
                "ChunkId::compute diverges from the official BLAKE3 keyed_hash \
                 vector at {len} bytes"
            );
        }
    }

    /// The BLAKE2b path must agree with the RustCrypto `blake2` crate, which is
    /// kept as a dev-dependency purely as an independent reference.
    #[test]
    fn compute_matches_rustcrypto_blake2() {
        use blake2::digest::consts::U32;
        use blake2::digest::{KeyInit, Mac};
        use blake2::Blake2bMac;

        for key in [[0xAA; 32], counting_key(), [0u8; 32]] {
            for len in [0usize, 1, 63, 64, 127, 128, 129, 255, 1000, 4096] {
                let data = pattern(len);
                let mut reference = <Blake2bMac<U32> as KeyInit>::new_from_slice(&key)
                    .expect("blake2 accepts 32-byte keys");
                Mac::update(&mut reference, &data);
                let expected = hex::encode(reference.finalize().into_bytes());
                assert_eq!(
                    ChunkId::compute(&b2(key), &data).to_hex(),
                    expected,
                    "divergence from RustCrypto blake2 at {len} bytes"
                );
            }
        }
    }

    #[test]
    fn serde_roundtrip() {
        let id = ChunkId::compute(&b2(test_key()), b"roundtrip test");
        let serialized = rmp_serde::to_vec(&id).unwrap();
        let deserialized: ChunkId = rmp_serde::from_slice(&serialized).unwrap();
        assert_eq!(id, deserialized);
    }
}
