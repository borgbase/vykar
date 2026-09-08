impl_aead_engine!(
    ChaCha20Poly1305Engine,
    ChaCha20Poly1305,
    chacha20poly1305,
    "ChaCha20-Poly1305"
);

#[cfg(test)]
mod tests {
    use super::*;
    use crate::CryptoEngine;
    use vykar_types::chunk_id::ChunkHasher;
    use vykar_types::error::VykarError;
    use vykar_types::hash::HashAlgorithm;

    fn test_chacha_engine() -> ChaCha20Poly1305Engine {
        let enc_key = [0x33; 32];
        ChaCha20Poly1305Engine::new(
            &enc_key,
            ChunkHasher::new(HashAlgorithm::Blake2b, [0x44; 32]),
        )
    }

    #[test]
    fn encrypt_decrypt_roundtrip() {
        let engine = test_chacha_engine();
        let data = b"secret message";
        let aad = b"type_tag";
        let encrypted = engine.encrypt(data, aad).unwrap();
        let decrypted = engine.decrypt(&encrypted, aad).unwrap();
        assert_eq!(decrypted, data);
    }

    #[test]
    fn ciphertext_different_from_plaintext() {
        let engine = test_chacha_engine();
        let data = b"secret message";
        let encrypted = engine.encrypt(data, b"aad").unwrap();
        // Encrypted output includes 12-byte nonce + ciphertext + 16-byte tag
        assert_ne!(&encrypted[12..], data.as_slice());
    }

    #[test]
    fn wrong_aad_fails_decrypt() {
        let engine = test_chacha_engine();
        let data = b"secret message";
        let encrypted = engine.encrypt(data, b"correct_aad").unwrap();
        let result = engine.decrypt(&encrypted, b"wrong_aad");
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), VykarError::DecryptionFailed));
    }

    #[test]
    fn corrupted_ciphertext_fails_decrypt() {
        let engine = test_chacha_engine();
        let data = b"secret message";
        let mut encrypted = engine.encrypt(data, b"aad").unwrap();
        if encrypted.len() > 14 {
            encrypted[14] ^= 0xFF;
        }
        let result = engine.decrypt(&encrypted, b"aad");
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), VykarError::DecryptionFailed));
    }

    #[test]
    fn truncated_ciphertext_fails_decrypt() {
        let engine = test_chacha_engine();
        let short_data = vec![0u8; 20];
        let result = engine.decrypt(&short_data, b"aad");
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), VykarError::DecryptionFailed));
    }

    #[test]
    fn encrypt_produces_different_ciphertext_each_time() {
        let engine = test_chacha_engine();
        let data = b"same plaintext";
        let aad = b"aad";
        let enc1 = engine.encrypt(data, aad).unwrap();
        let enc2 = engine.encrypt(data, aad).unwrap();
        assert_ne!(enc1, enc2);
        assert_eq!(engine.decrypt(&enc1, aad).unwrap(), data);
        assert_eq!(engine.decrypt(&enc2, aad).unwrap(), data);
    }

    #[test]
    fn chunk_hasher_returns_correct_value() {
        let enc_key = [0x33; 32];
        let cid_key = [0x44; 32];
        let engine = ChaCha20Poly1305Engine::new(
            &enc_key,
            ChunkHasher::new(HashAlgorithm::Blake2b, cid_key),
        );
        assert_eq!(engine.chunk_hasher().key(), &cid_key);
        assert_eq!(engine.chunk_hasher().algorithm(), HashAlgorithm::Blake2b);
    }
}
