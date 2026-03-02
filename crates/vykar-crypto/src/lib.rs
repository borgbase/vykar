pub mod aes_gcm;
pub mod chacha20_poly1305;
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
