use aes_gcm::aead::{Aead, AeadInPlace, KeyInit};
use aes_gcm::{Aes256Gcm, Nonce};
use rand::RngCore;

use super::CryptoEngine;
use vykar_types::error::{Result, VykarError};

/// AES-256-GCM authenticated encryption engine.
pub struct Aes256GcmEngine {
    cipher: Aes256Gcm,
    chunk_id_key: [u8; 32],
}

impl Aes256GcmEngine {
    pub fn new(encryption_key: &[u8; 32], chunk_id_key: &[u8; 32]) -> Self {
        let cipher =
            Aes256Gcm::new_from_slice(encryption_key).expect("valid 32-byte key for AES-256-GCM");
        Self {
            cipher,
            chunk_id_key: *chunk_id_key,
        }
    }
}

impl CryptoEngine for Aes256GcmEngine {
    fn encrypt(&self, plaintext: &[u8], aad: &[u8]) -> Result<Vec<u8>> {
        let mut rng = rand::thread_rng();
        let mut nonce_bytes = [0u8; 12];
        rng.fill_bytes(&mut nonce_bytes);
        let nonce = Nonce::from_slice(&nonce_bytes);

        let payload = aes_gcm::aead::Payload {
            msg: plaintext,
            aad,
        };
        let ciphertext = self
            .cipher
            .encrypt(nonce, payload)
            .map_err(|e| VykarError::Other(format!("AES-GCM encrypt: {e}")))?;

        // Wire format: [12-byte nonce][ciphertext with appended 16-byte tag]
        let mut out = Vec::with_capacity(12 + ciphertext.len());
        out.extend_from_slice(&nonce_bytes);
        out.extend_from_slice(&ciphertext);
        Ok(out)
    }

    fn decrypt(&self, data: &[u8], aad: &[u8]) -> Result<Vec<u8>> {
        if data.len() < 12 + 16 {
            return Err(VykarError::DecryptionFailed);
        }
        let (nonce_bytes, ciphertext) = data.split_at(12);
        let nonce = Nonce::from_slice(nonce_bytes);

        let payload = aes_gcm::aead::Payload {
            msg: ciphertext,
            aad,
        };
        self.cipher
            .decrypt(nonce, payload)
            .map_err(|_| VykarError::DecryptionFailed)
    }

    fn decrypt_into(&self, data: &[u8], aad: &[u8], output: &mut Vec<u8>) -> Result<()> {
        if data.len() < 12 + 16 {
            return Err(VykarError::DecryptionFailed);
        }
        let (nonce_bytes, ct_and_tag) = data.split_at(12);
        let nonce = Nonce::from_slice(nonce_bytes);
        let (ciphertext, tag_bytes) = ct_and_tag.split_at(ct_and_tag.len() - 16);
        let tag = aes_gcm::Tag::from_slice(tag_bytes);
        output.clear();
        output.extend_from_slice(ciphertext); // reuses existing capacity
        self.cipher
            .decrypt_in_place_detached(nonce, aad, output, tag)
            .map_err(|_| VykarError::DecryptionFailed)?;
        Ok(())
    }

    fn encrypt_in_place_detached(
        &self,
        buffer: &mut [u8],
        aad: &[u8],
    ) -> Result<([u8; 12], [u8; 16])> {
        let mut rng = rand::thread_rng();
        let mut nonce_bytes = [0u8; 12];
        rng.fill_bytes(&mut nonce_bytes);
        let nonce = Nonce::from_slice(&nonce_bytes);

        let tag = self
            .cipher
            .encrypt_in_place_detached(nonce, aad, buffer)
            .map_err(|e| VykarError::Other(format!("AES-GCM encrypt_in_place: {e}")))?;

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

#[cfg(test)]
mod tests {
    use super::*;
    use vykar_types::error::VykarError;

    fn test_aes_engine() -> Aes256GcmEngine {
        let enc_key = [0x11; 32];
        let cid_key = [0x22; 32];
        Aes256GcmEngine::new(&enc_key, &cid_key)
    }

    #[test]
    fn encrypt_decrypt_roundtrip() {
        let engine = test_aes_engine();
        let data = b"secret message";
        let aad = b"type_tag";
        let encrypted = engine.encrypt(data, aad).unwrap();
        let decrypted = engine.decrypt(&encrypted, aad).unwrap();
        assert_eq!(decrypted, data);
    }

    #[test]
    fn ciphertext_different_from_plaintext() {
        let engine = test_aes_engine();
        let data = b"secret message";
        let encrypted = engine.encrypt(data, b"aad").unwrap();
        // Encrypted output includes 12-byte nonce + ciphertext + 16-byte tag
        assert_ne!(&encrypted[12..], data.as_slice());
    }

    #[test]
    fn wrong_aad_fails_decrypt() {
        let engine = test_aes_engine();
        let data = b"secret message";
        let encrypted = engine.encrypt(data, b"correct_aad").unwrap();
        let result = engine.decrypt(&encrypted, b"wrong_aad");
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), VykarError::DecryptionFailed));
    }

    #[test]
    fn corrupted_ciphertext_fails_decrypt() {
        let engine = test_aes_engine();
        let data = b"secret message";
        let mut encrypted = engine.encrypt(data, b"aad").unwrap();
        // Corrupt a byte in the ciphertext portion (after the 12-byte nonce)
        if encrypted.len() > 14 {
            encrypted[14] ^= 0xFF;
        }
        let result = engine.decrypt(&encrypted, b"aad");
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), VykarError::DecryptionFailed));
    }

    #[test]
    fn truncated_ciphertext_fails_decrypt() {
        let engine = test_aes_engine();
        // Less than 12 (nonce) + 16 (tag) = 28 bytes
        let short_data = vec![0u8; 20];
        let result = engine.decrypt(&short_data, b"aad");
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), VykarError::DecryptionFailed));
    }

    #[test]
    fn encrypt_produces_different_ciphertext_each_time() {
        let engine = test_aes_engine();
        let data = b"same plaintext";
        let aad = b"aad";
        let enc1 = engine.encrypt(data, aad).unwrap();
        let enc2 = engine.encrypt(data, aad).unwrap();
        // Different nonces → different ciphertext
        assert_ne!(enc1, enc2);
        // But both decrypt to the same plaintext
        assert_eq!(engine.decrypt(&enc1, aad).unwrap(), data);
        assert_eq!(engine.decrypt(&enc2, aad).unwrap(), data);
    }

    #[test]
    fn chunk_id_key_returns_correct_value() {
        let enc_key = [0x11; 32];
        let cid_key = [0x22; 32];
        let engine = Aes256GcmEngine::new(&enc_key, &cid_key);
        assert_eq!(engine.chunk_id_key(), &cid_key);
    }

    #[test]
    fn empty_plaintext_roundtrip() {
        let engine = test_aes_engine();
        let aad = b"tag";
        let encrypted = engine.encrypt(b"", aad).unwrap();
        let decrypted = engine.decrypt(&encrypted, aad).unwrap();
        assert_eq!(decrypted, b"");
    }
}
