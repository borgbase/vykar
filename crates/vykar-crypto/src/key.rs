use aes_gcm::aead::{Aead, KeyInit, Payload};
use aes_gcm::{Aes256Gcm, Nonce};
use argon2::Argon2;
use rand::TryRngCore;
use serde::{Deserialize, Serialize};
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

/// Validate KDF parameters are within safe bounds.
fn validate_kdf_params(kdf: &KdfParams) -> Result<()> {
    if kdf.algorithm != "argon2id" {
        return Err(VykarError::DecryptionFailed);
    }
    if kdf.time_cost == 0 || kdf.time_cost > MAX_TIME_COST {
        return Err(VykarError::DecryptionFailed);
    }
    if kdf.parallelism == 0 || kdf.parallelism > MAX_PARALLELISM {
        return Err(VykarError::DecryptionFailed);
    }
    if kdf.memory_cost == 0 || kdf.memory_cost > MAX_MEMORY_KIB {
        return Err(VykarError::DecryptionFailed);
    }
    if kdf.salt.len() < MIN_SALT_LEN || kdf.salt.len() > MAX_SALT_LEN {
        return Err(VykarError::DecryptionFailed);
    }
    Ok(())
}

impl MasterKey {
    /// Generate a new random master key using OS entropy.
    pub fn generate() -> Self {
        let mut encryption_key = [0u8; 32];
        let mut chunk_id_key = [0u8; 32];
        rand::rngs::OsRng
            .try_fill_bytes(&mut encryption_key)
            .expect("OS entropy source unavailable");
        rand::rngs::OsRng
            .try_fill_bytes(&mut chunk_id_key)
            .expect("OS entropy source unavailable");
        Self {
            encryption_key,
            chunk_id_key,
        }
    }

    /// Encrypt the master key with a passphrase using Argon2id + AES-256-GCM.
    pub fn to_encrypted<P: AsRef<[u8]>>(&self, passphrase: P) -> Result<EncryptedKey> {
        let passphrase = passphrase.as_ref();

        // Generate salt using OS entropy
        let mut salt = vec![0u8; 32];
        rand::rngs::OsRng
            .try_fill_bytes(&mut salt)
            .expect("OS entropy source unavailable");

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
        rand::rngs::OsRng
            .try_fill_bytes(&mut nonce_bytes)
            .expect("OS entropy source unavailable");
        let nonce = Nonce::from_slice(&nonce_bytes);
        let ciphertext = cipher
            .encrypt(
                nonce,
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
    pub fn from_encrypted<P: AsRef<[u8]>>(encrypted: &EncryptedKey, passphrase: P) -> Result<Self> {
        let passphrase = passphrase.as_ref();

        // Validate nonce length to avoid panic in Nonce::from_slice
        if encrypted.nonce.len() != 12 {
            return Err(VykarError::DecryptionFailed);
        }

        // Validate KDF parameters are within safe bounds
        validate_kdf_params(&encrypted.kdf)?;

        let wrapping_key = derive_key_from_passphrase(passphrase, &encrypted.kdf)?;

        let cipher = Aes256Gcm::new_from_slice(wrapping_key.as_ref())
            .map_err(|_| VykarError::DecryptionFailed)?;
        let nonce = Nonce::from_slice(&encrypted.nonce);

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
/// Format: `b"vger:kdf-aad:v1\0"` || algorithm_len (u32 LE) || algorithm_bytes
/// || time_cost (u32 LE) || memory_cost (u32 LE) || parallelism (u32 LE)
/// || salt_len (u32 LE) || salt_bytes
///
/// This uses manual byte encoding with no serde dependency, ensuring
/// stability across rmp_serde versions.
fn kdf_params_aad_v1(kdf: &KdfParams) -> Vec<u8> {
    // Wire-format constant — DO NOT rename (backward compatibility)
    let prefix = b"vger:kdf-aad:v1\0";
    let algo_bytes = kdf.algorithm.as_bytes();
    let capacity = prefix.len() + 4 + algo_bytes.len() + 4 + 4 + 4 + 4 + kdf.salt.len();
    let mut buf = Vec::with_capacity(capacity);
    buf.extend_from_slice(prefix);
    buf.extend_from_slice(&(algo_bytes.len() as u32).to_le_bytes());
    buf.extend_from_slice(algo_bytes);
    buf.extend_from_slice(&kdf.time_cost.to_le_bytes());
    buf.extend_from_slice(&kdf.memory_cost.to_le_bytes());
    buf.extend_from_slice(&kdf.parallelism.to_le_bytes());
    buf.extend_from_slice(&(kdf.salt.len() as u32).to_le_bytes());
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
    use rand::RngCore;

    const TEST_PASSPHRASE: &str = "test-passphrase-123";

    fn make_test_kdf() -> KdfParams {
        let mut salt = vec![0u8; 32];
        rand::rngs::OsRng
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
        let mut kdf = make_test_kdf();
        kdf.memory_cost = MAX_MEMORY_KIB;
        assert!(validate_kdf_params(&kdf).is_ok());

        kdf.memory_cost = MAX_MEMORY_KIB + 1;
        assert!(matches!(
            validate_kdf_params(&kdf),
            Err(VykarError::DecryptionFailed)
        ));
    }

    #[test]
    fn test_nonce_wrong_length() {
        let key = MasterKey::generate();
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
        let key = MasterKey::generate();
        let encrypted = key.to_encrypted(TEST_PASSPHRASE).unwrap();
        let decrypted = MasterKey::from_encrypted(&encrypted, TEST_PASSPHRASE).unwrap();
        assert_eq!(key.encryption_key, decrypted.encryption_key);
        assert_eq!(key.chunk_id_key, decrypted.chunk_id_key);
    }

    #[test]
    fn test_aad_legacy_compat() {
        // Simulate a key encrypted with the old msgpack AAD
        let key = MasterKey::generate();
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
        let nonce = Nonce::from_slice(&nonce_bytes);
        let ciphertext = cipher
            .encrypt(
                nonce,
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
        let key = MasterKey::generate();
        assert_ne!(key.encryption_key, [0u8; 32]);
        assert_ne!(key.chunk_id_key, [0u8; 32]);
    }

    #[test]
    fn generate_produces_different_keys_each_time() {
        let k1 = MasterKey::generate();
        let k2 = MasterKey::generate();
        assert_ne!(k1.encryption_key, k2.encryption_key);
        assert_ne!(k1.chunk_id_key, k2.chunk_id_key);
    }

    #[test]
    fn encryption_key_and_chunk_id_key_are_different() {
        let key = MasterKey::generate();
        assert_ne!(key.encryption_key, key.chunk_id_key);
    }

    #[test]
    fn wrong_passphrase_fails_decrypt() {
        let key = MasterKey::generate();
        let encrypted = key.to_encrypted("correct").unwrap();
        let result = MasterKey::from_encrypted(&encrypted, "wrong");
        assert!(result.is_err());
    }

    #[test]
    fn encrypted_key_serde_roundtrip() {
        let key = MasterKey::generate();
        let encrypted = key.to_encrypted("pass").unwrap();
        let serialized = rmp_serde::to_vec(&encrypted).unwrap();
        let deserialized: EncryptedKey = rmp_serde::from_slice(&serialized).unwrap();
        let recovered = MasterKey::from_encrypted(&deserialized, "pass").unwrap();
        assert_eq!(key.encryption_key, recovered.encryption_key);
        assert_eq!(key.chunk_id_key, recovered.chunk_id_key);
    }

    #[test]
    fn byte_buffer_passphrase_roundtrip() {
        let key = MasterKey::generate();
        let passphrase = Zeroizing::new(TEST_PASSPHRASE.as_bytes().to_vec());

        let encrypted = key.to_encrypted(passphrase.clone()).unwrap();
        let decrypted = MasterKey::from_encrypted(&encrypted, passphrase).unwrap();

        assert_eq!(key.encryption_key, decrypted.encryption_key);
        assert_eq!(key.chunk_id_key, decrypted.chunk_id_key);
    }

    #[test]
    fn test_aad_none_compat() {
        // Simulate a key encrypted with no AAD (pre-AAD repos)
        let key = MasterKey::generate();
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
        let nonce = Nonce::from_slice(&nonce_bytes);
        // Encrypt with no AAD
        let ciphertext = cipher.encrypt(nonce, plaintext.as_ref()).unwrap();

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
