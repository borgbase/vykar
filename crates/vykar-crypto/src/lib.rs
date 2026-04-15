pub mod key;
pub mod select;

use vykar_types::error::Result;

/// Trait for encrypting and decrypting repository objects.
pub trait CryptoEngine: Send + Sync {
    /// Encrypt plaintext. Returns `[nonce][ciphertext+tag]`.
    /// `aad` is authenticated but not encrypted (e.g., the type tag byte).
    fn encrypt(&self, plaintext: &[u8], aad: &[u8]) -> Result<Vec<u8>>;

    /// Decrypt data produced by `encrypt`.
    /// `aad` must match what was passed during encryption.
    fn decrypt(&self, data: &[u8], aad: &[u8]) -> Result<Vec<u8>>;

    /// Encrypt `buffer` in-place and return `(nonce, tag)`.
    /// Avoids allocating a separate ciphertext buffer.
    fn encrypt_in_place_detached(
        &self,
        buffer: &mut [u8],
        aad: &[u8],
    ) -> Result<([u8; 12], [u8; 16])>;

    /// Decrypt data produced by `encrypt` into a caller-provided buffer.
    /// Reuses existing capacity in `output` to reduce allocation churn.
    fn decrypt_into(&self, data: &[u8], aad: &[u8], output: &mut Vec<u8>) -> Result<()> {
        *output = self.decrypt(data, aad)?;
        Ok(())
    }

    /// Whether this engine actually encrypts data.
    /// `PlaintextEngine` returns false; real ciphers return true.
    fn is_encrypting(&self) -> bool;

    /// The key used for computing chunk IDs (keyed BLAKE2b-256).
    fn chunk_id_key(&self) -> &[u8; 32];
}

/// Generate a `CryptoEngine` implementation for an AEAD cipher.
///
/// All AEAD engines share the same nonce-generation, wire-format, and
/// error-handling logic — only the underlying cipher type differs.
macro_rules! impl_aead_engine {
    ($engine:ident, $cipher:ident, $crate_path:ident, $label:literal) => {
        #[doc = concat!($label, " authenticated encryption engine.")]
        pub struct $engine {
            cipher: $crate_path::$cipher,
            chunk_id_key: [u8; 32],
        }

        impl $engine {
            pub fn new(encryption_key: &[u8; 32], chunk_id_key: &[u8; 32]) -> Self {
                use $crate_path::aead::KeyInit;
                let cipher = <$crate_path::$cipher>::new_from_slice(encryption_key)
                    .expect(concat!("valid 32-byte key for ", $label));
                Self {
                    cipher,
                    chunk_id_key: *chunk_id_key,
                }
            }
        }

        impl $crate::CryptoEngine for $engine {
            fn encrypt(&self, plaintext: &[u8], aad: &[u8]) -> vykar_types::error::Result<Vec<u8>> {
                use rand::RngCore;
                use $crate_path::aead::Aead;
                let mut rng = rand::rng();
                let mut nonce_bytes = [0u8; 12];
                rng.fill_bytes(&mut nonce_bytes);
                let nonce = $crate_path::Nonce::from_slice(&nonce_bytes);

                let payload = $crate_path::aead::Payload {
                    msg: plaintext,
                    aad,
                };
                let ciphertext = self.cipher.encrypt(nonce, payload).map_err(|e| {
                    vykar_types::error::VykarError::Other(format!(
                        concat!($label, " encrypt: {}"),
                        e
                    ))
                })?;

                // Wire format: [12-byte nonce][ciphertext with appended 16-byte tag]
                let mut out = Vec::with_capacity(12 + ciphertext.len());
                out.extend_from_slice(&nonce_bytes);
                out.extend_from_slice(&ciphertext);
                Ok(out)
            }

            fn decrypt(&self, data: &[u8], aad: &[u8]) -> vykar_types::error::Result<Vec<u8>> {
                use $crate_path::aead::Aead;
                if data.len() < 12 + 16 {
                    return Err(vykar_types::error::VykarError::DecryptionFailed);
                }
                let (nonce_bytes, ciphertext) = data.split_at(12);
                let nonce = $crate_path::Nonce::from_slice(nonce_bytes);

                let payload = $crate_path::aead::Payload {
                    msg: ciphertext,
                    aad,
                };
                self.cipher
                    .decrypt(nonce, payload)
                    .map_err(|_| vykar_types::error::VykarError::DecryptionFailed)
            }

            fn decrypt_into(
                &self,
                data: &[u8],
                aad: &[u8],
                output: &mut Vec<u8>,
            ) -> vykar_types::error::Result<()> {
                use $crate_path::aead::AeadInPlace;
                if data.len() < 12 + 16 {
                    return Err(vykar_types::error::VykarError::DecryptionFailed);
                }
                let (nonce_bytes, ct_and_tag) = data.split_at(12);
                let nonce = $crate_path::Nonce::from_slice(nonce_bytes);
                let (ciphertext, tag_bytes) = ct_and_tag.split_at(ct_and_tag.len() - 16);
                let tag = $crate_path::Tag::from_slice(tag_bytes);
                output.clear();
                output.extend_from_slice(ciphertext); // reuses existing capacity
                self.cipher
                    .decrypt_in_place_detached(nonce, aad, output, tag)
                    .map_err(|_| vykar_types::error::VykarError::DecryptionFailed)?;
                Ok(())
            }

            fn encrypt_in_place_detached(
                &self,
                buffer: &mut [u8],
                aad: &[u8],
            ) -> vykar_types::error::Result<([u8; 12], [u8; 16])> {
                use rand::RngCore;
                use $crate_path::aead::AeadInPlace;
                let mut rng = rand::rng();
                let mut nonce_bytes = [0u8; 12];
                rng.fill_bytes(&mut nonce_bytes);
                let nonce = $crate_path::Nonce::from_slice(&nonce_bytes);

                let tag = self
                    .cipher
                    .encrypt_in_place_detached(nonce, aad, buffer)
                    .map_err(|e| {
                        vykar_types::error::VykarError::Other(format!(
                            concat!($label, " encrypt_in_place: {}"),
                            e
                        ))
                    })?;

                let mut tag_bytes = [0u8; 16];
                tag_bytes.copy_from_slice(&tag);
                Ok((nonce_bytes, tag_bytes))
            }

            fn is_encrypting(&self) -> bool {
                true
            }

            fn chunk_id_key(&self) -> &[u8; 32] {
                &self.chunk_id_key
            }
        }
    };
}

pub mod aes_gcm;
pub mod chacha20_poly1305;

/// No-encryption engine. Still computes deterministic chunk IDs.
pub struct PlaintextEngine {
    chunk_id_key: [u8; 32],
}

impl PlaintextEngine {
    pub fn new(chunk_id_key: &[u8; 32]) -> Self {
        Self {
            chunk_id_key: *chunk_id_key,
        }
    }
}

impl CryptoEngine for PlaintextEngine {
    fn encrypt(&self, plaintext: &[u8], _aad: &[u8]) -> Result<Vec<u8>> {
        Ok(plaintext.to_vec())
    }

    fn decrypt(&self, data: &[u8], _aad: &[u8]) -> Result<Vec<u8>> {
        Ok(data.to_vec())
    }

    fn decrypt_into(&self, data: &[u8], _aad: &[u8], output: &mut Vec<u8>) -> Result<()> {
        output.clear();
        output.extend_from_slice(data);
        Ok(())
    }

    fn encrypt_in_place_detached(
        &self,
        _buffer: &mut [u8],
        _aad: &[u8],
    ) -> Result<([u8; 12], [u8; 16])> {
        Ok(([0u8; 12], [0u8; 16]))
    }

    fn is_encrypting(&self) -> bool {
        false
    }

    fn chunk_id_key(&self) -> &[u8; 32] {
        &self.chunk_id_key
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn plaintext_encrypt_is_identity() {
        let key = [0xAA; 32];
        let engine = PlaintextEngine::new(&key);
        let data = b"hello plaintext";
        let encrypted = engine.encrypt(data, b"aad").unwrap();
        assert_eq!(encrypted, data);
    }

    #[test]
    fn plaintext_decrypt_is_identity() {
        let key = [0xAA; 32];
        let engine = PlaintextEngine::new(&key);
        let data = b"hello plaintext";
        let decrypted = engine.decrypt(data, b"aad").unwrap();
        assert_eq!(decrypted, data);
    }

    #[test]
    fn plaintext_chunk_id_key() {
        let key = [0xBB; 32];
        let engine = PlaintextEngine::new(&key);
        assert_eq!(engine.chunk_id_key(), &key);
    }

    #[test]
    fn plaintext_roundtrip_ignores_aad() {
        let key = [0xCC; 32];
        let engine = PlaintextEngine::new(&key);
        let data = b"test data";
        let encrypted = engine.encrypt(data, b"aad1").unwrap();
        let decrypted = engine.decrypt(&encrypted, b"different_aad").unwrap();
        assert_eq!(decrypted, data);
    }
}
