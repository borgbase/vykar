//! The sweep record: what one mark-and-sweep found. Stored under
//! `index.d/<64 hex>` (tag `SweepRecord`, context `b"index.d/sweep"`, a
//! zstd-compressed msgpack map), named by the head's `sweep.record`.
//! Informational: nothing that decides liveness reads it.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use vykar_types::error::{Result, VykarError};

/// The only sweep record version this binary reads and writes.
pub const SWEEP_RECORD_VERSION: u32 = 1;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SweepRecord {
    pub version: u32,
    /// Generation of the head that names it (wire key `gen`).
    #[serde(rename = "gen")]
    pub generation: u64,
    /// The client's clock; informational.
    pub time: DateTime<Utc>,
    /// Snapshot IDs marked, sorted by bytes; each a 32-byte bin.
    pub snapshots: Vec<serde_bytes::ByteBuf>,
    /// Tree chunks the mark walked.
    pub tree_chunks: u64,
    /// Entries kept (marked) and their stored bytes.
    pub entries_live: u64,
    pub bytes_live: u64,
    /// Entries this sweep removed and their stored bytes.
    pub entries_removed: u64,
    pub bytes_removed: u64,
    /// Chunk references decoded.
    pub references: u64,
    /// Sum of stored size times references, counted with snapshot
    /// multiplicity (what `info` shows as "Referenced").
    pub referenced_bytes: u64,
}

impl SweepRecord {
    /// Serialize as a msgpack map with the field names as keys.
    pub fn encode(&self) -> Result<Vec<u8>> {
        Ok(rmp_serde::to_vec_named(self)?)
    }

    /// Deserialize. A record of another version is
    /// `UnsupportedVersion(version)`; other failures are decode errors.
    pub fn decode(bytes: &[u8]) -> Result<Self> {
        #[derive(Deserialize)]
        struct VersionOnly {
            version: u32,
        }
        if let Ok(v) = rmp_serde::from_slice::<VersionOnly>(bytes)
            && v.version != SWEEP_RECORD_VERSION
        {
            return Err(VykarError::UnsupportedVersion(v.version));
        }
        Ok(rmp_serde::from_slice(bytes)?)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample() -> SweepRecord {
        SweepRecord {
            version: SWEEP_RECORD_VERSION,
            generation: 4,
            time: DateTime::from_timestamp(1_700_000_000, 0).unwrap(),
            snapshots: vec![
                serde_bytes::ByteBuf::from(vec![1; 32]),
                serde_bytes::ByteBuf::from(vec![2; 32]),
            ],
            tree_chunks: 3,
            entries_live: 10,
            bytes_live: 1000,
            entries_removed: 2,
            bytes_removed: 200,
            references: 12,
            referenced_bytes: 1500,
        }
    }

    /// REC-1.
    #[test]
    fn round_trips_and_refuses_other_versions() {
        let rec = sample();
        let bytes = rec.encode().unwrap();
        assert!((0x80..=0x8f).contains(&bytes[0]) || bytes[0] == 0xde);
        assert_eq!(SweepRecord::decode(&bytes).unwrap(), rec);

        let mut newer = sample();
        newer.version = 2;
        assert!(matches!(
            SweepRecord::decode(&newer.encode().unwrap()),
            Err(VykarError::UnsupportedVersion(2))
        ));
        assert!(SweepRecord::decode(&[0xc0]).is_err());
    }
}
