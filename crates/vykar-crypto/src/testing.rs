//! A deterministic AEAD engine for tests whose expected bytes are fixed
//! (byte-golden pack, trailer and index tests) and for fuzz targets.
//! AES-256-GCM under a fixed key with a counter nonce: the same calls in the
//! same order give the same bytes. Nonces repeat across instances, so it
//! never stands in for a product engine in a repository the simulator or
//! a protocol test runs on. Test support, not API.

use std::sync::atomic::{AtomicU64, Ordering};

use ::aes_gcm::aead::{Aead, AeadInOut, KeyInit, Payload};
use vykar_types::chunk_id::ChunkHasher;
use vykar_types::error::{Result, VykarError};

use crate::CryptoEngine;

/// The key every [`DeterministicEngine`] encrypts with.
pub const DETERMINISTIC_KEY: [u8; 32] = [0x7e; 32];

/// AES-256-GCM under [`DETERMINISTIC_KEY`] with nonces 0, 1, 2, ... per
/// instance. Decryption is the product [`crate::aes_gcm::Aes256GcmEngine`].
pub struct DeterministicEngine {
    /// Encrypts, with the counter nonce.
    cipher: ::aes_gcm::Aes256Gcm,
    /// Decrypts: the product code path.
    open: crate::aes_gcm::Aes256GcmEngine,
    next: AtomicU64,
}

impl DeterministicEngine {
    /// Nonces 0, 1, 2, ... from this instance's first encryption on.
    pub fn new(chunk_hasher: ChunkHasher) -> Self {
        Self {
            cipher: ::aes_gcm::Aes256Gcm::new((&DETERMINISTIC_KEY).into()),
            open: crate::aes_gcm::Aes256GcmEngine::new(&DETERMINISTIC_KEY, chunk_hasher),
            next: AtomicU64::new(0),
        }
    }

    /// The nonce of this instance's n-th encryption (0-based): n as
    /// little-endian in bytes 0..8, zero in 8..12.
    pub fn nonce(n: u64) -> [u8; 12] {
        let mut nonce = [0u8; 12];
        for (dst, src) in nonce.iter_mut().zip(n.to_le_bytes()) {
            *dst = src;
        }
        nonce
    }

    fn next_nonce(&self) -> [u8; 12] {
        Self::nonce(self.next.fetch_add(1, Ordering::Relaxed))
    }
}

impl CryptoEngine for DeterministicEngine {
    fn encrypt(&self, plaintext: &[u8], aad: &[u8]) -> Result<Vec<u8>> {
        let nonce = self.next_nonce();
        let ciphertext = self
            .cipher
            .encrypt(
                (&nonce).into(),
                Payload {
                    msg: plaintext,
                    aad,
                },
            )
            .map_err(|e| VykarError::Other(format!("deterministic AES-GCM encrypt: {e}")))?;
        // Wire format of the AEAD engines: [12-byte nonce][ciphertext + 16-byte tag].
        let mut out = Vec::with_capacity(12 + ciphertext.len());
        out.extend_from_slice(&nonce);
        out.extend_from_slice(&ciphertext);
        Ok(out)
    }

    fn decrypt(&self, data: &[u8], aad: &[u8]) -> Result<Vec<u8>> {
        self.open.decrypt(data, aad)
    }

    fn decrypt_into(&self, data: &[u8], aad: &[u8], output: &mut Vec<u8>) -> Result<()> {
        self.open.decrypt_into(data, aad, output)
    }

    fn decrypt_in_place<'a>(&self, data: &'a mut [u8], aad: &[u8]) -> Result<&'a mut [u8]> {
        self.open.decrypt_in_place(data, aad)
    }

    fn encrypt_in_place_detached(
        &self,
        buffer: &mut [u8],
        aad: &[u8],
    ) -> Result<([u8; 12], [u8; 16])> {
        let nonce = self.next_nonce();
        let tag = self
            .cipher
            .encrypt_inout_detached((&nonce).into(), aad, buffer.into())
            .map_err(|e| {
                VykarError::Other(format!("deterministic AES-GCM encrypt_in_place: {e}"))
            })?;
        let mut tag_bytes = [0u8; 16];
        tag_bytes.copy_from_slice(&tag);
        Ok((nonce, tag_bytes))
    }

    fn is_encrypting(&self) -> bool {
        true
    }

    fn chunk_hasher(&self) -> &ChunkHasher {
        self.open.chunk_hasher()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::aes_gcm::Aes256GcmEngine;
    use vykar_types::hash::HashAlgorithm;

    fn hasher() -> ChunkHasher {
        ChunkHasher::new(HashAlgorithm::Blake3, [7; 32])
    }

    #[test]
    fn deterministic_engine_repeats_its_bytes() {
        let run = || {
            let engine = DeterministicEngine::new(hasher());
            let a = engine.encrypt(b"first", b"aad").unwrap();
            let b = engine.encrypt(b"first", b"aad").unwrap();
            (a, b)
        };
        let (a1, b1) = run();
        let (a2, b2) = run();
        assert_eq!(a1, a2);
        assert_eq!(b1, b2);
        // Two calls on one instance use different nonces.
        assert_ne!(a1[..12], b1[..12]);
        assert_ne!(a1, b1);
    }

    #[test]
    fn deterministic_engine_is_aes_gcm_on_the_wire() {
        let engine = DeterministicEngine::new(hasher());
        let sealed = engine.encrypt(b"payload", b"aad").unwrap();
        assert_eq!(sealed[..12], DeterministicEngine::nonce(0));
        assert_eq!(sealed.len(), 12 + b"payload".len() + 16);
        let product = Aes256GcmEngine::new(&DETERMINISTIC_KEY, hasher());
        assert_eq!(product.decrypt(&sealed, b"aad").unwrap(), b"payload");
        assert_eq!(engine.decrypt(&sealed, b"aad").unwrap(), b"payload");
        let second = engine.encrypt(b"payload", b"aad").unwrap();
        assert_eq!(second[..12], DeterministicEngine::nonce(1));
    }

    #[test]
    fn deterministic_engine_rejects_a_flipped_byte() {
        let engine = DeterministicEngine::new(hasher());
        let sealed = engine.encrypt(b"some payload bytes", b"aad").unwrap();
        for i in 0..sealed.len() {
            let mut flipped = sealed.clone();
            flipped[i] ^= 0x01;
            assert!(
                matches!(
                    engine.decrypt(&flipped, b"aad"),
                    Err(VykarError::DecryptionFailed)
                ),
                "flipped byte {i} must fail authentication"
            );
        }
        assert!(matches!(
            engine.decrypt(&sealed, b"other aad"),
            Err(VykarError::DecryptionFailed)
        ));
    }

    #[test]
    fn deterministic_in_place_agrees_with_encrypt() {
        let a = DeterministicEngine::new(hasher());
        let b = DeterministicEngine::new(hasher());
        let sealed = a.encrypt(b"in place", b"aad").unwrap();
        let mut buffer = b"in place".to_vec();
        let (nonce, tag) = b.encrypt_in_place_detached(&mut buffer, b"aad").unwrap();
        assert_eq!(nonce, DeterministicEngine::nonce(0));
        assert_eq!(sealed[..12], nonce);
        assert_eq!(sealed[12..12 + buffer.len()], buffer[..]);
        assert_eq!(sealed[12 + buffer.len()..], tag);
    }
}
