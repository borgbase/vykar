use rand::Rng;

hash_id! {
    /// A 32-byte snapshot identifier (random).
    SnapshotId
}

impl SnapshotId {
    /// Generate a random snapshot ID.
    pub fn generate() -> Self {
        let mut buf = [0u8; 32];
        rand::rng().fill_bytes(&mut buf);
        SnapshotId(buf)
    }

    /// Storage key path: `snapshots/<hex>`.
    pub fn storage_key(&self) -> String {
        format!("snapshots/{}", self.to_hex())
    }

    /// Parse a `SnapshotId` from a 64-character hex string.
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
        Ok(SnapshotId(arr))
    }
}
