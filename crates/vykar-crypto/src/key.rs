use std::fmt;

use aes_gcm::aead::{Aead, KeyInit, Payload};
use aes_gcm::{Aes256Gcm, Nonce};
use argon2::Argon2;
use rand::TryRng;
use serde::{Deserialize, Serialize};
use subtle::ConstantTimeEq;
use zeroize::{Zeroize, ZeroizeOnDrop, Zeroizing};

use vykar_types::error::{Result, VykarError};

/// The master key material — never stored in plaintext on disk.
/// Automatically zeroized on drop to prevent key material from lingering in memory.
#[derive(Zeroize, ZeroizeOnDrop)]
pub struct MasterKey {
    pub encryption_key: [u8; 32],
    pub chunk_id_key: [u8; 32],
}

/// Serialized payload inside the encrypted key blob.
/// Zeroized on drop to prevent key material from lingering in memory.
#[derive(Serialize, Deserialize, Zeroize, ZeroizeOnDrop)]
struct MasterKeyPayload {
    encryption_key: Vec<u8>,
    chunk_id_key: Vec<u8>,
}

/// KDF parameters stored alongside the encrypted key.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KdfParams {
    pub algorithm: String,
    pub time_cost: u32,
    pub memory_cost: u32,
    pub parallelism: u32,
    pub salt: Vec<u8>,
}

/// On-disk format stored at `keys/repokey`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EncryptedKey {
    pub kdf: KdfParams,
    pub nonce: Vec<u8>,
    pub encrypted_payload: Vec<u8>,
}

// KDF parameter bounds to reject maliciously crafted key blobs.
const MAX_TIME_COST: u32 = 10;
const MAX_PARALLELISM: u32 = 16;
const MAX_MEMORY_KIB: u32 = 524_288; // 512 MiB
const MIN_SALT_LEN: usize = 16;
const MAX_SALT_LEN: usize = 64;

/// Plausible length bounds for the wrapped-payload ciphertext.
///
/// `MasterKeyPayload` deliberately carries no `serde_bytes`, so its two
/// 32-byte `Vec<u8>` fields serialize as msgpack *integer arrays*: one byte
/// per value below 0x80, two bytes otherwise. The plaintext is a 1-byte array
/// header plus two 3-byte array headers plus 64..=128 encoded bytes, i.e.
/// 71..=135; GCM adds a 16-byte tag. An exact length check is therefore
/// impossible — this loose range is the tightest screen available.
const MIN_WRAPPED_PAYLOAD_LEN: usize = 71 + 16;
const MAX_WRAPPED_PAYLOAD_LEN: usize = 135 + 16;

/// An unambiguous defect in a stored key blob.
///
/// Every variant is detectable *without* the passphrase, which is the point:
/// a blob that trips one of these is corrupt, and saying so beats blaming the
/// operator's typing. A blob that passes all of them and still fails to
/// unwrap is the genuinely ambiguous case — wrong passphrase, or damage inside
/// the authenticated payload, which AEAD cannot tell apart.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum KeyBlobDefect {
    /// msgpack framing damaged — the bytes are not an `EncryptedKey` at all.
    Framing(String),
    /// KDF parameters outside the accepted bounds.
    KdfParams,
    /// Nonce is not the 12 bytes AES-256-GCM requires.
    NonceLength(usize),
    /// Wrapped payload cannot hold a 64-byte master key at any encoding length.
    PayloadLength(usize),
}

impl fmt::Display for KeyBlobDefect {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Framing(detail) => write!(f, "damaged msgpack framing ({detail})"),
            Self::KdfParams => write!(f, "key-derivation parameters out of range"),
            Self::NonceLength(len) => write!(f, "nonce is {len} bytes, expected 12"),
            Self::PayloadLength(len) => write!(
                f,
                "wrapped key is {len} bytes, expected {MIN_WRAPPED_PAYLOAD_LEN}-{MAX_WRAPPED_PAYLOAD_LEN}"
            ),
        }
    }
}

/// Decode a stored key blob and screen it for unambiguous corruption.
///
/// This runs no key derivation and needs no passphrase, so it is cheap enough
/// to apply to every copy on every open, and it works on legacy single-copy
/// repositories where there is nothing to corroborate against.
///
/// # Errors
///
/// Returns the specific [`KeyBlobDefect`] found. A successful return means
/// only that the blob is *well-formed* — it says nothing about whether it
/// unwraps.
pub fn inspect_key_blob(bytes: &[u8]) -> std::result::Result<EncryptedKey, KeyBlobDefect> {
    let key: EncryptedKey =
        rmp_serde::from_slice(bytes).map_err(|e| KeyBlobDefect::Framing(e.to_string()))?;
    validate(&key)?;
    Ok(key)
}

/// Every structural check a key blob must pass before key derivation is
/// attempted. Shared by [`inspect_key_blob`] and [`MasterKey::from_encrypted`]
/// so the two can never disagree about what counts as well-formed.
fn validate(key: &EncryptedKey) -> std::result::Result<(), KeyBlobDefect> {
    let kdf = &key.kdf;
    // `Params::new` never sees the algorithm string, so it is checked here.
    if kdf.algorithm != "argon2id"
        || kdf.time_cost == 0
        || kdf.time_cost > MAX_TIME_COST
        || kdf.parallelism == 0
        || kdf.parallelism > MAX_PARALLELISM
        || kdf.memory_cost == 0
        || kdf.memory_cost > MAX_MEMORY_KIB
        || kdf.salt.len() < MIN_SALT_LEN
        || kdf.salt.len() > MAX_SALT_LEN
    {
        return Err(KeyBlobDefect::KdfParams);
    }
    // Argon2's own invariants (notably `memory_cost >= 8 * parallelism`), so
    // a blob that cannot be derived from is reported as corrupt here rather
    // than surfacing as a derivation failure later.
    if argon2::Params::new(kdf.memory_cost, kdf.time_cost, kdf.parallelism, Some(32)).is_err() {
        return Err(KeyBlobDefect::KdfParams);
    }
    if key.nonce.len() != 12 {
        return Err(KeyBlobDefect::NonceLength(key.nonce.len()));
    }
    let payload_len = key.encrypted_payload.len();
    if !(MIN_WRAPPED_PAYLOAD_LEN..=MAX_WRAPPED_PAYLOAD_LEN).contains(&payload_len) {
        return Err(KeyBlobDefect::PayloadLength(payload_len));
    }
    Ok(())
}

impl MasterKey {
    /// Compare two master keys in constant time over
    /// `encryption_key || chunk_id_key`.
    ///
    /// This is secret material, so no early-exit `memcmp`: the caller uses it
    /// to decide whether two divergent key copies wrap the *same* key, and a
    /// timing side channel there would leak the key a byte at a time.
    pub fn ct_eq(&self, other: &Self) -> bool {
        let a = self.encryption_key.ct_eq(&other.encryption_key);
        let b = self.chunk_id_key.ct_eq(&other.chunk_id_key);
        (a & b).into()
    }

    /// Generate a new random master key using OS entropy.
    ///
    /// # Errors
    ///
    /// Returns an error if the operating system entropy source is unavailable.
    pub fn generate() -> Result<Self> {
        let mut encryption_key = [0u8; 32];
        let mut chunk_id_key = [0u8; 32];
        rand::rngs::SysRng
            .try_fill_bytes(&mut encryption_key)
            .map_err(|e| VykarError::KeyDerivation(format!("OS entropy unavailable: {e}")))?;
        rand::rngs::SysRng
            .try_fill_bytes(&mut chunk_id_key)
            .map_err(|e| VykarError::KeyDerivation(format!("OS entropy unavailable: {e}")))?;
        Ok(Self {
            encryption_key,
            chunk_id_key,
        })
    }

    /// Encrypt the master key with a passphrase using Argon2id + AES-256-GCM.
    ///
    /// # Errors
    ///
    /// Returns an error when key derivation, serialization, or encryption
    /// fails, or when the operating system entropy source is unavailable.
    pub fn to_encrypted<P: AsRef<[u8]>>(&self, passphrase: P) -> Result<EncryptedKey> {
        let passphrase = passphrase.as_ref();

        // Generate salt using OS entropy
        let mut salt = vec![0u8; 32];
        rand::rngs::SysRng
            .try_fill_bytes(&mut salt)
            .map_err(|e| VykarError::KeyDerivation(format!("OS entropy unavailable: {e}")))?;

        // Derive a wrapping key from the passphrase
        let kdf = KdfParams {
            algorithm: "argon2id".to_string(),
            time_cost: 3,
            memory_cost: 65536, // 64 MiB
            parallelism: 4,
            salt: salt.clone(),
        };
        let wrapping_key = derive_key_from_passphrase(passphrase, &kdf)?;

        // Serialize the master key payload
        let payload = MasterKeyPayload {
            encryption_key: self.encryption_key.to_vec(),
            chunk_id_key: self.chunk_id_key.to_vec(),
        };
        let plaintext = Zeroizing::new(rmp_serde::to_vec(&payload)?);

        // Encrypt with AES-256-GCM, binding KDF params as AAD to prevent
        // parameter substitution attacks on the key blob.
        let kdf_aad = kdf_params_aad_v1(&kdf);
        let cipher = Aes256Gcm::new_from_slice(wrapping_key.as_ref())
            .map_err(|e| VykarError::KeyDerivation(format!("cipher init: {e}")))?;
        let mut nonce_bytes = [0u8; 12];
        rand::rngs::SysRng
            .try_fill_bytes(&mut nonce_bytes)
            .map_err(|e| VykarError::KeyDerivation(format!("OS entropy unavailable: {e}")))?;
        let ciphertext = cipher
            .encrypt(
                (&nonce_bytes).into(),
                Payload {
                    msg: plaintext.as_ref(),
                    aad: &kdf_aad,
                },
            )
            .map_err(|e| VykarError::KeyDerivation(format!("encrypt: {e}")))?;

        Ok(EncryptedKey {
            kdf,
            nonce: nonce_bytes.to_vec(),
            encrypted_payload: ciphertext,
        })
    }

    /// Decrypt the master key from its on-disk format.
    ///
    /// Tries decryption in order:
    /// 1. v1 AAD (stable manual encoding)
    /// 2. Legacy msgpack AAD (pre-v1 repos)
    /// 3. No AAD (pre-AAD repos)
    ///
    /// # Errors
    ///
    /// Returns an error when KDF parameters are invalid, authentication fails,
    /// or the decrypted key payload is malformed.
    pub fn from_encrypted<P: AsRef<[u8]>>(encrypted: &EncryptedKey, passphrase: P) -> Result<Self> {
        let passphrase = passphrase.as_ref();

        validate(encrypted).map_err(|_| VykarError::DecryptionFailed)?;

        let wrapping_key = derive_key_from_passphrase(passphrase, &encrypted.kdf)?;

        let cipher = Aes256Gcm::new_from_slice(wrapping_key.as_ref())
            .map_err(|_| VykarError::DecryptionFailed)?;
        // Length validated above; &[u8; 12] -> &Nonce via infallible conversion.
        let nonce_bytes: &[u8; 12] = encrypted
            .nonce
            .as_slice()
            .try_into()
            .map_err(|_| VykarError::DecryptionFailed)?;
        let nonce: &Nonce<aes_gcm::aead::consts::U12> = nonce_bytes.into();

        // Try v1 AAD first, then legacy msgpack AAD, then no AAD
        let plaintext = try_decrypt_with_v1_aad(&cipher, nonce, encrypted)
            .or_else(|| try_decrypt_with_legacy_aad(&cipher, nonce, encrypted))
            .or_else(|| try_decrypt_no_aad(&cipher, nonce, encrypted))
            .ok_or(VykarError::DecryptionFailed)?;
        let plaintext = Zeroizing::new(plaintext);

        let payload: MasterKeyPayload =
            rmp_serde::from_slice(&plaintext).map_err(|_| VykarError::DecryptionFailed)?;

        let mut encryption_key = [0u8; 32];
        let mut chunk_id_key = [0u8; 32];
        if payload.encryption_key.len() != 32 || payload.chunk_id_key.len() != 32 {
            return Err(VykarError::DecryptionFailed);
        }
        encryption_key.copy_from_slice(&payload.encryption_key);
        chunk_id_key.copy_from_slice(&payload.chunk_id_key);

        Ok(Self {
            encryption_key,
            chunk_id_key,
        })
    }
}

/// Try decryption with v1 AAD (stable manual encoding).
fn try_decrypt_with_v1_aad(
    cipher: &Aes256Gcm,
    nonce: &Nonce<aes_gcm::aead::consts::U12>,
    encrypted: &EncryptedKey,
) -> Option<Vec<u8>> {
    let aad = kdf_params_aad_v1(&encrypted.kdf);
    cipher
        .decrypt(
            nonce,
            Payload {
                msg: encrypted.encrypted_payload.as_ref(),
                aad: &aad,
            },
        )
        .ok()
}

/// Try decryption with legacy msgpack AAD.
fn try_decrypt_with_legacy_aad(
    cipher: &Aes256Gcm,
    nonce: &Nonce<aes_gcm::aead::consts::U12>,
    encrypted: &EncryptedKey,
) -> Option<Vec<u8>> {
    let aad = kdf_params_aad_legacy(&encrypted.kdf).ok()?;
    cipher
        .decrypt(
            nonce,
            Payload {
                msg: encrypted.encrypted_payload.as_ref(),
                aad: &aad,
            },
        )
        .ok()
}

/// Try decryption with no AAD (pre-AAD repos).
fn try_decrypt_no_aad(
    cipher: &Aes256Gcm,
    nonce: &Nonce<aes_gcm::aead::consts::U12>,
    encrypted: &EncryptedKey,
) -> Option<Vec<u8>> {
    cipher
        .decrypt(nonce, encrypted.encrypted_payload.as_ref())
        .ok()
}

/// Compute stable v1 AAD bytes from KDF parameters.
///
/// Format: `b"vger:kdf-aad:v1\0"` || `algorithm_len` (u32 LE) ||
/// `algorithm_bytes` || `time_cost` (u32 LE) || `memory_cost` (u32 LE) ||
/// `parallelism` (u32 LE) || `salt_len` (u32 LE) || `salt_bytes`
///
/// This uses manual byte encoding with no serde dependency, ensuring
/// stability across `rmp_serde` versions.
fn kdf_params_aad_v1(kdf: &KdfParams) -> Vec<u8> {
    // Wire-format constant — DO NOT rename (backward compatibility)
    let prefix = b"vger:kdf-aad:v1\0";
    let algo_bytes = kdf.algorithm.as_bytes();
    let capacity = prefix.len() + 4 + algo_bytes.len() + 4 + 4 + 4 + 4 + kdf.salt.len();
    let mut buf = Vec::with_capacity(capacity);
    buf.extend_from_slice(prefix);
    buf.extend_from_slice(
        &u32::try_from(algo_bytes.len())
            .expect("KDF algorithm name length fits u32")
            .to_le_bytes(),
    );
    buf.extend_from_slice(algo_bytes);
    buf.extend_from_slice(&kdf.time_cost.to_le_bytes());
    buf.extend_from_slice(&kdf.memory_cost.to_le_bytes());
    buf.extend_from_slice(&kdf.parallelism.to_le_bytes());
    buf.extend_from_slice(
        &u32::try_from(kdf.salt.len())
            .expect("KDF salt length fits u32")
            .to_le_bytes(),
    );
    buf.extend_from_slice(&kdf.salt);
    buf
}

/// Legacy msgpack-based AAD for backwards compatibility.
fn kdf_params_aad_legacy(kdf: &KdfParams) -> Result<Vec<u8>> {
    rmp_serde::to_vec(kdf).map_err(|e| VykarError::KeyDerivation(format!("serialize kdf aad: {e}")))
}

/// Derive a 32-byte key from a passphrase using Argon2id.
fn derive_key_from_passphrase(
    passphrase: impl AsRef<[u8]>,
    kdf: &KdfParams,
) -> Result<Zeroizing<[u8; 32]>> {
    let params = argon2::Params::new(kdf.memory_cost, kdf.time_cost, kdf.parallelism, Some(32))
        .map_err(|e| VykarError::KeyDerivation(format!("argon2 params: {e}")))?;
    let argon2 = Argon2::new(argon2::Algorithm::Argon2id, argon2::Version::V0x13, params);

    let mut output = Zeroizing::new([0u8; 32]);
    argon2
        .hash_password_into(passphrase.as_ref(), &kdf.salt, output.as_mut())
        .map_err(|e| VykarError::KeyDerivation(format!("argon2 hash: {e}")))?;
    Ok(output)
}

#[cfg(test)]
mod tests {
    use super::*;
    use rand::Rng;

    const TEST_PASSPHRASE: &str = "test-passphrase-123";

    /// Argon2id key derivation is repository format: `keys/repokey` blobs are
    /// wrapped with this key, so a change makes every existing repository
    /// unopenable. The round-trip tests cannot catch that — they re-derive
    /// with the same code on both sides — so pin a known answer at the
    /// parameters `EncryptedKey::wrap` actually writes (t=3, m=64 MiB, p=4).
    /// Verified byte-identical across argon2 0.5 and 0.6.
    #[test]
    fn derive_key_from_passphrase_known_answer() {
        let kdf = KdfParams {
            algorithm: "argon2id".to_string(),
            time_cost: 3,
            memory_cost: 65536,
            parallelism: 4,
            salt: (0u8..32).collect(),
        };
        let key = derive_key_from_passphrase("test-passphrase-123", &kdf).unwrap();
        assert_eq!(
            hex::encode(key.as_ref()),
            "5f35887cb2c78be1ac24320a8c74bf8823c2a6958b4a3d6df0a61dd4a5fa4b61",
            "Argon2id derivation changed; existing keys/repokey blobs would not unwrap"
        );
    }

    fn make_test_kdf() -> KdfParams {
        let mut salt = vec![0u8; 32];
        rand::rngs::SysRng
            .try_fill_bytes(&mut salt)
            .expect("OS entropy source unavailable");
        KdfParams {
            algorithm: "argon2id".to_string(),
            time_cost: 1,
            memory_cost: 8192,
            parallelism: 1,
            salt,
        }
    }

    #[test]
    fn test_kdf_memory_limit_boundary() {
        let mut key = EncryptedKey {
            kdf: make_test_kdf(),
            nonce: vec![0u8; 12],
            encrypted_payload: vec![0u8; MIN_WRAPPED_PAYLOAD_LEN],
        };
        key.kdf.memory_cost = MAX_MEMORY_KIB;
        assert!(validate(&key).is_ok());

        key.kdf.memory_cost = MAX_MEMORY_KIB + 1;
        assert_eq!(validate(&key), Err(KeyBlobDefect::KdfParams));
    }

    #[test]
    fn test_nonce_wrong_length() {
        let key = MasterKey::generate().unwrap();
        let mut encrypted = key.to_encrypted(TEST_PASSPHRASE).unwrap();
        // Replace nonce with wrong length
        encrypted.nonce = vec![0u8; 8];
        let result = MasterKey::from_encrypted(&encrypted, TEST_PASSPHRASE);
        assert!(
            matches!(result, Err(VykarError::DecryptionFailed)),
            "8-byte nonce should be rejected"
        );
    }

    #[test]
    fn test_kdf_excessive_memory() {
        let encrypted = EncryptedKey {
            kdf: KdfParams {
                algorithm: "argon2id".to_string(),
                time_cost: 3,
                memory_cost: u32::MAX,
                parallelism: 4,
                salt: vec![0u8; 32],
            },
            nonce: vec![0u8; 12],
            encrypted_payload: vec![0u8; 64],
        };
        let result = MasterKey::from_encrypted(&encrypted, TEST_PASSPHRASE);
        assert!(
            matches!(result, Err(VykarError::DecryptionFailed)),
            "excessive memory_cost should be rejected"
        );
    }

    #[test]
    fn test_kdf_bad_algorithm() {
        let encrypted = EncryptedKey {
            kdf: KdfParams {
                algorithm: "scrypt".to_string(),
                time_cost: 3,
                memory_cost: 65536,
                parallelism: 4,
                salt: vec![0u8; 32],
            },
            nonce: vec![0u8; 12],
            encrypted_payload: vec![0u8; 64],
        };
        let result = MasterKey::from_encrypted(&encrypted, TEST_PASSPHRASE);
        assert!(
            matches!(result, Err(VykarError::DecryptionFailed)),
            "non-argon2id algorithm should be rejected"
        );
    }

    #[test]
    fn test_kdf_salt_too_short() {
        let encrypted = EncryptedKey {
            kdf: KdfParams {
                algorithm: "argon2id".to_string(),
                time_cost: 3,
                memory_cost: 65536,
                parallelism: 4,
                salt: vec![0u8; 8], // too short
            },
            nonce: vec![0u8; 12],
            encrypted_payload: vec![0u8; 64],
        };
        let result = MasterKey::from_encrypted(&encrypted, TEST_PASSPHRASE);
        assert!(
            matches!(result, Err(VykarError::DecryptionFailed)),
            "short salt should be rejected"
        );
    }

    #[test]
    fn test_aad_v1_roundtrip() {
        let key = MasterKey::generate().unwrap();
        let encrypted = key.to_encrypted(TEST_PASSPHRASE).unwrap();
        let decrypted = MasterKey::from_encrypted(&encrypted, TEST_PASSPHRASE).unwrap();
        assert_eq!(key.encryption_key, decrypted.encryption_key);
        assert_eq!(key.chunk_id_key, decrypted.chunk_id_key);
    }

    #[test]
    fn test_aad_legacy_compat() {
        // Simulate a key encrypted with the old msgpack AAD
        let key = MasterKey::generate().unwrap();
        let kdf = make_test_kdf();
        let wrapping_key = derive_key_from_passphrase(TEST_PASSPHRASE, &kdf).unwrap();

        let payload = MasterKeyPayload {
            encryption_key: key.encryption_key.to_vec(),
            chunk_id_key: key.chunk_id_key.to_vec(),
        };
        let plaintext = rmp_serde::to_vec(&payload).unwrap();

        let legacy_aad = kdf_params_aad_legacy(&kdf).unwrap();
        let cipher = Aes256Gcm::new_from_slice(wrapping_key.as_ref()).unwrap();
        let mut nonce_bytes = [0u8; 12];
        rand::rng().fill_bytes(&mut nonce_bytes);
        let ciphertext = cipher
            .encrypt(
                (&nonce_bytes).into(),
                Payload {
                    msg: plaintext.as_ref(),
                    aad: &legacy_aad,
                },
            )
            .unwrap();

        let encrypted = EncryptedKey {
            kdf,
            nonce: nonce_bytes.to_vec(),
            encrypted_payload: ciphertext,
        };

        let decrypted = MasterKey::from_encrypted(&encrypted, TEST_PASSPHRASE).unwrap();
        assert_eq!(key.encryption_key, decrypted.encryption_key);
        assert_eq!(key.chunk_id_key, decrypted.chunk_id_key);
    }

    #[test]
    fn generate_produces_nonzero_keys() {
        let key = MasterKey::generate().unwrap();
        assert_ne!(key.encryption_key, [0u8; 32]);
        assert_ne!(key.chunk_id_key, [0u8; 32]);
    }

    #[test]
    fn generate_produces_different_keys_each_time() {
        let k1 = MasterKey::generate().unwrap();
        let k2 = MasterKey::generate().unwrap();
        assert_ne!(k1.encryption_key, k2.encryption_key);
        assert_ne!(k1.chunk_id_key, k2.chunk_id_key);
    }

    #[test]
    fn encryption_key_and_chunk_id_key_are_different() {
        let key = MasterKey::generate().unwrap();
        assert_ne!(key.encryption_key, key.chunk_id_key);
    }

    #[test]
    fn wrong_passphrase_fails_decrypt() {
        let key = MasterKey::generate().unwrap();
        let encrypted = key.to_encrypted("correct").unwrap();
        let result = MasterKey::from_encrypted(&encrypted, "wrong");
        assert!(result.is_err());
    }

    #[test]
    fn encrypted_key_serde_roundtrip() {
        let key = MasterKey::generate().unwrap();
        let encrypted = key.to_encrypted("pass").unwrap();
        let serialized = rmp_serde::to_vec(&encrypted).unwrap();
        let deserialized: EncryptedKey = rmp_serde::from_slice(&serialized).unwrap();
        let recovered = MasterKey::from_encrypted(&deserialized, "pass").unwrap();
        assert_eq!(key.encryption_key, recovered.encryption_key);
        assert_eq!(key.chunk_id_key, recovered.chunk_id_key);
    }

    #[test]
    fn byte_buffer_passphrase_roundtrip() {
        let key = MasterKey::generate().unwrap();
        let passphrase = Zeroizing::new(TEST_PASSPHRASE.as_bytes().to_vec());

        let encrypted = key.to_encrypted(passphrase.clone()).unwrap();
        let decrypted = MasterKey::from_encrypted(&encrypted, passphrase).unwrap();

        assert_eq!(key.encryption_key, decrypted.encryption_key);
        assert_eq!(key.chunk_id_key, decrypted.chunk_id_key);
    }

    fn wrapped(passphrase: &str) -> Vec<u8> {
        let key = MasterKey::generate().unwrap();
        rmp_serde::to_vec(&key.to_encrypted(passphrase).unwrap()).unwrap()
    }

    #[test]
    fn inspect_accepts_a_freshly_wrapped_blob() {
        let bytes = wrapped(TEST_PASSPHRASE);
        assert!(inspect_key_blob(&bytes).is_ok());
    }

    /// The payload bounds are reasoned from the msgpack encoding rather than
    /// measured, so pin them against the payload the writer actually
    /// serializes. Exercised over the serialized plaintext plus the GCM tag
    /// so the sweep costs no Argon2id derivations.
    #[test]
    fn wrapped_payload_length_stays_within_the_screened_range() {
        const GCM_TAG_LEN: usize = 16;
        // The extremes: every byte below 0x80 (1 byte each) and every byte at
        // or above it (2 bytes each), plus a random draw in between.
        let mut extremes = vec![
            MasterKey {
                encryption_key: [0x00; 32],
                chunk_id_key: [0x7f; 32],
            },
            MasterKey {
                encryption_key: [0x80; 32],
                chunk_id_key: [0xff; 32],
            },
        ];
        for _ in 0..32 {
            extremes.push(MasterKey::generate().unwrap());
        }

        for key in &extremes {
            let payload = MasterKeyPayload {
                encryption_key: key.encryption_key.to_vec(),
                chunk_id_key: key.chunk_id_key.to_vec(),
            };
            let len = rmp_serde::to_vec(&payload).unwrap().len() + GCM_TAG_LEN;
            assert!(
                (MIN_WRAPPED_PAYLOAD_LEN..=MAX_WRAPPED_PAYLOAD_LEN).contains(&len),
                "wrapped payload of {len} bytes falls outside the screened range"
            );
        }
    }

    #[test]
    fn inspect_reports_damaged_framing() {
        let defect = inspect_key_blob(b"not msgpack at all").unwrap_err();
        assert!(matches!(defect, KeyBlobDefect::Framing(_)), "{defect:?}");
    }

    #[test]
    fn inspect_reports_bad_kdf_params() {
        let key = MasterKey::generate().unwrap();
        let encrypted = key.to_encrypted(TEST_PASSPHRASE).unwrap();

        let mut over_ceiling = encrypted.clone();
        over_ceiling.kdf.memory_cost = u32::MAX;

        // Inside every ceiling, but below Argon2's own `m >= 8p` floor. Only
        // `Params::new` catches this; the bounds checks alone let it through.
        let mut too_little_memory = encrypted.clone();
        too_little_memory.kdf.memory_cost = 1;
        too_little_memory.kdf.parallelism = 16;

        let mut wrong_algorithm = encrypted;
        wrong_algorithm.kdf.algorithm = "scrypt".into();

        for bad in [over_ceiling, too_little_memory, wrong_algorithm] {
            let bytes = rmp_serde::to_vec(&bad).unwrap();
            assert_eq!(
                inspect_key_blob(&bytes).unwrap_err(),
                KeyBlobDefect::KdfParams
            );
            // And the unwrap path agrees rather than reaching key derivation.
            assert!(matches!(
                MasterKey::from_encrypted(&bad, TEST_PASSPHRASE),
                Err(VykarError::DecryptionFailed)
            ));
        }
    }

    #[test]
    fn inspect_reports_short_nonce() {
        let key = MasterKey::generate().unwrap();
        let mut encrypted = key.to_encrypted(TEST_PASSPHRASE).unwrap();
        encrypted.nonce = vec![0u8; 8];
        let bytes = rmp_serde::to_vec(&encrypted).unwrap();
        assert_eq!(
            inspect_key_blob(&bytes).unwrap_err(),
            KeyBlobDefect::NonceLength(8)
        );
    }

    #[test]
    fn inspect_reports_out_of_range_payload() {
        let key = MasterKey::generate().unwrap();
        let mut encrypted = key.to_encrypted(TEST_PASSPHRASE).unwrap();
        encrypted.encrypted_payload.truncate(32);
        let bytes = rmp_serde::to_vec(&encrypted).unwrap();
        assert_eq!(
            inspect_key_blob(&bytes).unwrap_err(),
            KeyBlobDefect::PayloadLength(32)
        );
    }

    /// A truncated payload that survives the length screen is exactly the
    /// ambiguous case: `from_encrypted` must still fail closed.
    #[test]
    fn well_formed_but_tampered_payload_still_fails_to_unwrap() {
        let key = MasterKey::generate().unwrap();
        let mut encrypted = key.to_encrypted(TEST_PASSPHRASE).unwrap();
        encrypted.encrypted_payload[0] ^= 0xff;
        let bytes = rmp_serde::to_vec(&encrypted).unwrap();
        assert!(inspect_key_blob(&bytes).is_ok(), "defect must be ambiguous");
        assert!(matches!(
            MasterKey::from_encrypted(&encrypted, TEST_PASSPHRASE),
            Err(VykarError::DecryptionFailed)
        ));
    }

    #[test]
    fn ct_eq_matches_structural_equality() {
        let a = MasterKey::generate().unwrap();
        let b = MasterKey::generate().unwrap();
        let a_again = MasterKey {
            encryption_key: a.encryption_key,
            chunk_id_key: a.chunk_id_key,
        };
        assert!(a.ct_eq(&a_again));
        assert!(!a.ct_eq(&b));

        // Differing in only one half must not compare equal.
        let half = MasterKey {
            encryption_key: a.encryption_key,
            chunk_id_key: b.chunk_id_key,
        };
        assert!(!a.ct_eq(&half));
    }

    #[test]
    fn test_aad_none_compat() {
        // Simulate a key encrypted with no AAD (pre-AAD repos)
        let key = MasterKey::generate().unwrap();
        let kdf = make_test_kdf();
        let wrapping_key = derive_key_from_passphrase(TEST_PASSPHRASE, &kdf).unwrap();

        let payload = MasterKeyPayload {
            encryption_key: key.encryption_key.to_vec(),
            chunk_id_key: key.chunk_id_key.to_vec(),
        };
        let plaintext = rmp_serde::to_vec(&payload).unwrap();

        let cipher = Aes256Gcm::new_from_slice(wrapping_key.as_ref()).unwrap();
        let mut nonce_bytes = [0u8; 12];
        rand::rng().fill_bytes(&mut nonce_bytes);
        // Encrypt with no AAD
        let ciphertext = cipher
            .encrypt((&nonce_bytes).into(), plaintext.as_ref())
            .unwrap();

        let encrypted = EncryptedKey {
            kdf,
            nonce: nonce_bytes.to_vec(),
            encrypted_payload: ciphertext,
        };

        let decrypted = MasterKey::from_encrypted(&encrypted, TEST_PASSPHRASE).unwrap();
        assert_eq!(key.encryption_key, decrypted.encryption_key);
        assert_eq!(key.chunk_id_key, decrypted.chunk_id_key);
    }
}
