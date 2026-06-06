use blake2::digest::{Update, VariableOutput};
use blake2::Blake2bVar;
use serde::{Deserialize, Serialize};
use std::fmt;

/// A 32-byte pack file identifier computed as unkeyed BLAKE2b-256.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct PackId([u8; 32]);

impl PackId {
    /// Wrap a 32-byte array as a `PackId`. Any 32-byte value is a valid ID.
    pub const fn from_bytes(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }

    /// Borrow the raw 32 bytes.
    pub const fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }

    /// Compute a pack ID as unkeyed BLAKE2b-256 of the entire pack contents.
    ///
    /// # Panics
    ///
    /// Panics only if the `BLAKE2b` implementation rejects the fixed 32-byte
    /// output length used by `PackId`.
    pub fn compute(data: &[u8]) -> Self {
        let mut hasher = Blake2bVar::new(32).expect("BLAKE2b accepts 32-byte output per spec");
        hasher.update(data);
        let mut out = [0u8; 32];
        hasher
            .finalize_variable(&mut out)
            .expect("finalize_variable writes the requested 32 bytes");
        PackId(out)
    }

    /// Hex-encode the full pack ID.
    pub fn to_hex(&self) -> String {
        hex::encode(self.0)
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

impl fmt::Debug for PackId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "PackId({})", &self.to_hex()[..16])
    }
}

impl fmt::Display for PackId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", &self.to_hex()[..16])
    }
}
