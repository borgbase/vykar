use rand::RngCore;
use serde::{Deserialize, Serialize};
use std::fmt;

/// A 32-byte snapshot identifier (random).
#[derive(Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct SnapshotId(pub [u8; 32]);

impl SnapshotId {
    /// Generate a random snapshot ID.
    pub fn generate() -> Self {
        let mut buf = [0u8; 32];
        rand::thread_rng().fill_bytes(&mut buf);
        SnapshotId(buf)
    }

    /// Hex-encode the full snapshot ID.
    pub fn to_hex(&self) -> String {
        hex::encode(self.0)
    }

    /// Storage key path: `snapshots/<hex>`.
    pub fn storage_key(&self) -> String {
        format!("snapshots/{}", self.to_hex())
    }

    /// Parse a SnapshotId from a 64-character hex string.
    pub fn from_hex(hex_str: &str) -> std::result::Result<Self, String> {
        let bytes = hex::decode(hex_str).map_err(|e| format!("invalid hex: {e}"))?;
        if bytes.len() != 32 {
            return Err(format!("expected 32 bytes, got {}", bytes.len()));
        }
        let mut arr = [0u8; 32];
        arr.copy_from_slice(&bytes);
        Ok(SnapshotId(arr))
    }

    /// Raw bytes for use as AAD context.
    pub fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }
}

impl fmt::Debug for SnapshotId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "SnapshotId({})", &self.to_hex()[..16])
    }
}

impl fmt::Display for SnapshotId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", &self.to_hex()[..16])
    }
}
