pub mod item;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::config::ChunkerConfig;
use vykar_types::chunk_id::ChunkId;
use vykar_types::error::{Result, VykarError};

/// Snapshot format version stamped by the current writer. Absent / `0` means a
/// legacy snapshot written before the discriminator existed. Readers refuse a
/// version greater than this (see [`SnapshotMeta::supported`]). See the Format
/// Evolution section of `architecture.md`.
pub const CURRENT_FORMAT_VERSION: u32 = 1;

/// Metadata for a single snapshot, stored at `snapshots/<id>`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SnapshotMeta {
    pub name: String,
    pub hostname: String,
    pub username: String,
    pub time: DateTime<Utc>,
    pub time_end: DateTime<Utc>,
    pub chunker_params: ChunkerConfig,
    #[serde(default)]
    pub comment: String,
    /// Chunk IDs that contain the serialized item stream.
    pub item_ptrs: Vec<ChunkId>,
    #[serde(default)]
    pub stats: SnapshotStats,
    /// Label of the source that produced this snapshot.
    #[serde(default)]
    pub source_label: String,
    /// Source directories that were backed up.
    #[serde(default)]
    pub source_paths: Vec<String>,
    /// Legacy field kept for backward compatibility with existing snapshots.
    /// New snapshots always write `""`.
    #[serde(default)]
    pub label: String,
    /// Reserved opaque extension blob — the escape hatch for future
    /// snapshot-level metadata inside a frozen-field-count envelope. Always
    /// written `None` in this release; never read or validated here. See the
    /// Format Evolution section of `architecture.md`.
    #[serde(default, with = "serde_bytes")]
    pub ext: Option<Vec<u8>>,
    /// Snapshot format version. **Must remain the last field** — the envelope
    /// field count is frozen so this discriminator is always decodable and a
    /// future snapshot is recognized (and refused) rather than mistaken for
    /// corruption. Absent / `0` = legacy.
    #[serde(default)]
    pub format_version: u32,
}

impl SnapshotMeta {
    /// Refuse snapshots written by a newer format than this binary supports.
    /// Returns [`VykarError::UnsupportedSnapshotVersion`] when `format_version`
    /// exceeds [`CURRENT_FORMAT_VERSION`]; the envelope still decodes, so this
    /// is a clean refusal, never a corruption misclassification.
    pub fn supported(&self) -> Result<()> {
        if self.format_version > CURRENT_FORMAT_VERSION {
            return Err(VykarError::UnsupportedSnapshotVersion {
                version: self.format_version,
            });
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct SnapshotStats {
    pub nfiles: u64,
    pub original_size: u64,
    pub compressed_size: u64,
    pub deduplicated_size: u64,
    /// Number of files that could not be read (soft errors skipped).
    #[serde(default)]
    pub errors: u64,
}

/// Snapshot of byte-counter fields on `SnapshotStats`, used to roll back
/// partial commits when a file drifts mid-read.
///
/// Only covers the three size counters — `nfiles` is incremented exactly
/// once on successful commit and `errors` is bumped by the skip path, so
/// neither needs rollback support.
#[derive(Clone, Copy)]
pub struct ByteCounterSnapshot {
    original_size: u64,
    compressed_size: u64,
    deduplicated_size: u64,
}

impl SnapshotStats {
    pub fn snapshot_byte_counters(&self) -> ByteCounterSnapshot {
        ByteCounterSnapshot {
            original_size: self.original_size,
            compressed_size: self.compressed_size,
            deduplicated_size: self.deduplicated_size,
        }
    }

    pub fn restore_byte_counters(&mut self, s: ByteCounterSnapshot) {
        self.original_size = s.original_size;
        self.compressed_size = s.compressed_size;
        self.deduplicated_size = s.deduplicated_size;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_meta() -> SnapshotMeta {
        SnapshotMeta {
            name: "snap".into(),
            hostname: "host".into(),
            username: "user".into(),
            time: Utc::now(),
            time_end: Utc::now(),
            chunker_params: ChunkerConfig::default(),
            comment: String::new(),
            item_ptrs: vec![],
            stats: SnapshotStats::default(),
            source_label: String::new(),
            source_paths: vec![],
            label: String::new(),
            ext: None,
            format_version: CURRENT_FORMAT_VERSION,
        }
    }

    #[test]
    fn supported_accepts_current_and_legacy() {
        let mut m = sample_meta();
        m.supported().unwrap();
        m.format_version = 0; // legacy
        m.supported().unwrap();
    }

    #[test]
    fn supported_rejects_newer() {
        let mut m = sample_meta();
        m.format_version = CURRENT_FORMAT_VERSION + 1;
        let err = m.supported().unwrap_err();
        assert!(
            matches!(err, VykarError::UnsupportedSnapshotVersion { version } if version == CURRENT_FORMAT_VERSION + 1)
        );
    }

    /// Pin the on-wire layout exactly: `SnapshotMeta` serializes as a positional
    /// msgpack array whose **field count is frozen** at 14 (a `fixarray`, header
    /// byte `0x90 | 14 == 0x9E`) and whose **every field position and type** is
    /// fixed, with `ext` (serde_bytes) second-to-last and `format_version` (u32)
    /// last. The test decodes the bytes through an *independent* positional tuple
    /// that mirrors each field's exact type, then re-serializes it and asserts
    /// byte-for-byte equality with the original — so any reorder, retype, add, or
    /// remove (which would break the frozen `format_version` discriminator)
    /// fails here, not just a field-count change.
    #[test]
    fn snapshot_meta_layout_is_pinned() {
        use serde_bytes::ByteBuf;

        const SNAPSHOT_META_FIELD_COUNT: u8 = 14;

        // Distinctive values for every field so a swap of two same-typed fields
        // would still change the byte stream / tuple values.
        let t0 = DateTime::<Utc>::from_timestamp(1_700_000_000, 0).unwrap();
        let t1 = DateTime::<Utc>::from_timestamp(1_700_000_999, 0).unwrap();
        let meta = SnapshotMeta {
            name: "the-name".into(),
            hostname: "the-host".into(),
            username: "the-user".into(),
            time: t0,
            time_end: t1,
            chunker_params: ChunkerConfig {
                min_size: 11,
                avg_size: 22,
                max_size: 33,
            },
            comment: "the-comment".into(),
            item_ptrs: vec![ChunkId::from_bytes([7u8; 32])],
            stats: SnapshotStats {
                nfiles: 5,
                original_size: 6,
                compressed_size: 7,
                deduplicated_size: 8,
                errors: 9,
            },
            source_label: "the-source-label".into(),
            source_paths: vec!["the-source-path".into()],
            label: "the-label".into(),
            ext: Some(vec![0xDE, 0xAD, 0xBE, 0xEF]),
            format_version: 3,
        };
        let bytes = rmp_serde::to_vec(&meta).unwrap();

        // Field-count canary (cheap, precise message).
        assert_eq!(
            bytes.first().copied(),
            Some(0x90 | SNAPSHOT_META_FIELD_COUNT),
            "SnapshotMeta field count changed — the frozen envelope must stay \
             {SNAPSHOT_META_FIELD_COUNT} fields with format_version last and ext before it"
        );

        // Independent positional layout mirroring the exact field order + types.
        // `ByteBuf` pins `ext` as a msgpack `bin` (serde_bytes), not an array;
        // `u32` pins `format_version` in the final position.
        type Layout = (
            String,          // name
            String,          // hostname
            String,          // username
            DateTime<Utc>,   // time
            DateTime<Utc>,   // time_end
            ChunkerConfig,   // chunker_params
            String,          // comment
            Vec<ChunkId>,    // item_ptrs
            SnapshotStats,   // stats
            String,          // source_label
            Vec<String>,     // source_paths
            String,          // label
            Option<ByteBuf>, // ext
            u32,             // format_version
        );
        let decoded: Layout = rmp_serde::from_slice(&bytes)
            .expect("SnapshotMeta layout must match the pinned positional tuple");

        // Golden round-trip: the independent tuple must re-serialize to the exact
        // same bytes — pins order + type of every field including nested structs.
        let reserialized = rmp_serde::to_vec(&decoded).unwrap();
        assert_eq!(
            reserialized, bytes,
            "SnapshotMeta wire layout drifted from the pinned positional tuple"
        );

        // Spot-check the boundary fields by value/position.
        assert_eq!(decoded.0, "the-name", "position 0 must be `name`");
        assert_eq!(decoded.11, "the-label", "position 11 must be `label`");
        assert_eq!(
            decoded.12.as_deref().map(|b| b.to_vec()),
            Some(vec![0xDE, 0xAD, 0xBE, 0xEF]),
            "position 12 must be the serde_bytes `ext` blob"
        );
        assert_eq!(
            decoded.13, 3,
            "position 13 must be the `format_version` u32"
        );
    }

    /// An old-format array (without `ext`/`format_version`) still decodes, with
    /// `ext == None` and `format_version == 0`.
    #[test]
    fn old_array_decodes_with_defaults() {
        #[derive(Serialize)]
        struct OldMeta {
            name: String,
            hostname: String,
            username: String,
            time: DateTime<Utc>,
            time_end: DateTime<Utc>,
            chunker_params: ChunkerConfig,
            comment: String,
            item_ptrs: Vec<ChunkId>,
            stats: SnapshotStats,
            source_label: String,
            source_paths: Vec<String>,
            label: String,
        }
        let now = Utc::now();
        let old = OldMeta {
            name: "snap".into(),
            hostname: "h".into(),
            username: "u".into(),
            time: now,
            time_end: now,
            chunker_params: ChunkerConfig::default(),
            comment: String::new(),
            item_ptrs: vec![],
            stats: SnapshotStats::default(),
            source_label: String::new(),
            source_paths: vec![],
            label: String::new(),
        };
        let bytes = rmp_serde::to_vec(&old).unwrap();
        let decoded: SnapshotMeta = rmp_serde::from_slice(&bytes).unwrap();
        assert_eq!(decoded.ext, None);
        assert_eq!(decoded.format_version, 0);
        decoded.supported().unwrap();
    }

    /// A future snapshot — populated `ext` and a higher `format_version` — still
    /// decodes the (frozen) envelope and is refused, never treated as corrupt.
    #[test]
    fn future_snapshot_decodes_envelope_and_is_refused() {
        let mut m = sample_meta();
        m.ext = Some(vec![1, 2, 3, 4]);
        m.format_version = CURRENT_FORMAT_VERSION + 5;
        let bytes = rmp_serde::to_vec(&m).unwrap();
        let decoded: SnapshotMeta = rmp_serde::from_slice(&bytes).unwrap();
        assert_eq!(decoded.ext.as_deref(), Some(&[1u8, 2, 3, 4][..]));
        assert_eq!(decoded.format_version, CURRENT_FORMAT_VERSION + 5);
        assert!(decoded.supported().is_err());
    }
}
