pub(crate) mod bin_id;
pub mod item;
pub mod listing;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::chunker::ChunkerParams;
use vykar_types::chunk_id::ChunkId;
use vykar_types::error::{Result, VykarError};

/// Snapshot format version stamped by the current writer. Absent / `0` means a
/// legacy snapshot written before the discriminator existed. Readers refuse a
/// version greater than this (see [`SnapshotMeta::supported`]). See the Format
/// Evolution section of `architecture.md`.
///
/// 2: chunk IDs in `Item.chunks` and `item_ptrs` are 32-byte msgpack bin
/// (06 step 6a). 1 (0.17 to this change) and 0 decode with the same reader.
pub const CURRENT_FORMAT_VERSION: u32 = 2;

/// Number of fields in the frozen [`SnapshotMeta`] envelope, pinned by
/// `snapshot_meta_layout_is_pinned`.
///
/// Read paths use this to explain a positional-array length mismatch in terms
/// the user can act on instead of surfacing a raw serde error. A *shorter*
/// array still decodes (every field from `comment` on carries
/// `#[serde(default)]`), so a length mismatch always means a *longer* array —
/// i.e. a snapshot written by a newer vykar.
pub const SNAPSHOT_META_FIELD_COUNT: u32 = 14;

/// Metadata for a single snapshot, stored at `snapshots/<id>`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SnapshotMeta {
    pub name: String,
    pub hostname: String,
    pub username: String,
    pub time: DateTime<Utc>,
    pub time_end: DateTime<Utc>,
    pub chunker_params: ChunkerParams,
    #[serde(default)]
    pub comment: String,
    /// Chunk IDs that contain the serialized item stream. One bin of 32
    /// bytes per pointer from format 2; format 1's int arrays decode too.
    #[serde(with = "bin_id::seq")]
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
    /// Snapshot extension blob, versioned on its own
    /// ([`listing::EXT_VERSION`]): the listing key index of 06 step 4.
    /// Advisory: a reader that ignores it, or cannot decode it, scans the
    /// listing. Older binaries never read it. See the Format Evolution
    /// section of `architecture.md`.
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

/// Per-snapshot counters, persisted at envelope position 8 of [`SnapshotMeta`].
///
/// **Frozen at 5 positional msgpack elements.** This type is nested *before*
/// the `format_version` discriminator, so growing it shifts every later field:
/// an older vykar would then read a new snapshot as a decode failure —
/// corruption — instead of a clean newer-version refusal. Run-local counters
/// (e.g. the macOS dataless file/directory tallies) therefore live on
/// `BackupOutcome`, not here. Anything that genuinely must be persisted goes in
/// `SnapshotMeta::ext`.
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

/// Snapshot of byte-counter fields on `SnapshotStats`, used by `FileTxn` to
/// drop a skipped segmented file's share of the counters.
///
/// Only covers the three size counters — `nfiles` is incremented exactly
/// once on successful commit and `errors` is bumped by the skip path, so
/// neither needs restoring.
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
            chunker_params: ChunkerParams::default(),
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
    /// fails here, not just a field-count change. The element type of
    /// `item_ptrs` is pinned as a 32-byte bin (snapshot format 2).
    #[test]
    fn snapshot_meta_layout_is_pinned() {
        use serde_bytes::ByteBuf;

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
            chunker_params: ChunkerParams {
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
            Some(0x90 | u8::try_from(SNAPSHOT_META_FIELD_COUNT).unwrap()),
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
            (u32, u32, u32), // chunker_params
            String,          // comment
            Vec<ByteBuf>,    // item_ptrs: one bin per pointer (format 2)
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
        assert_eq!(
            decoded.7.len(),
            1,
            "position 7 must be `item_ptrs` with one pointer"
        );
        assert_eq!(
            decoded.7[0].as_slice(),
            [7u8; 32],
            "item_ptrs elements must be 32-byte bins"
        );
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
            chunker_params: ChunkerParams,
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
            chunker_params: ChunkerParams::default(),
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

    /// The envelope before 06 step 6a, all derived: `item_ptrs` as plain
    /// `Vec<ChunkId>` (int arrays).
    #[derive(Serialize, Deserialize)]
    struct PreV2Meta {
        name: String,
        hostname: String,
        username: String,
        time: DateTime<Utc>,
        time_end: DateTime<Utc>,
        chunker_params: ChunkerParams,
        comment: String,
        item_ptrs: Vec<ChunkId>,
        stats: SnapshotStats,
        source_label: String,
        source_paths: Vec<String>,
        label: String,
        #[serde(with = "serde_bytes")]
        ext: Option<Vec<u8>>,
        format_version: u32,
    }

    fn two_ptrs() -> Vec<ChunkId> {
        vec![
            ChunkId::from_bytes([1; 32]),
            ChunkId::from_bytes([0xfe; 32]),
        ]
    }

    /// A binary that knows only format 1 decodes a format-2 envelope (rmp-serde
    /// reads a 32-byte bin into a derived `[u8; 32]`) and so refuses it by
    /// version, not with a parse error that could read as corruption.
    #[test]
    fn format_1_reader_refuses_format_2_by_version() {
        let mut m = sample_meta();
        m.item_ptrs = two_ptrs();
        assert_eq!(m.format_version, 2);
        let bytes = rmp_serde::to_vec(&m).unwrap();
        let old: PreV2Meta = rmp_serde::from_slice(&bytes)
            .expect("a format-1 reader must decode the format-2 envelope");
        assert_eq!(old.item_ptrs, two_ptrs());
        assert_eq!(old.format_version, 2);
        // What a format-1 binary's supported() compares against.
        assert!(old.format_version > 1);
    }

    /// A format-1 envelope (int-array `item_ptrs`) decodes; re-encoding
    /// writes bins and keeps the 14-field envelope.
    #[test]
    fn format_1_envelope_decodes() {
        let now = Utc::now();
        let old = PreV2Meta {
            name: "snap".into(),
            hostname: "h".into(),
            username: "u".into(),
            time: now,
            time_end: now,
            chunker_params: ChunkerParams::default(),
            comment: String::new(),
            item_ptrs: two_ptrs(),
            stats: SnapshotStats::default(),
            source_label: String::new(),
            source_paths: vec![],
            label: String::new(),
            ext: None,
            format_version: 1,
        };
        let bytes = rmp_serde::to_vec(&old).unwrap();
        let decoded: SnapshotMeta = rmp_serde::from_slice(&bytes).unwrap();
        assert_eq!(decoded.item_ptrs, two_ptrs());
        assert_eq!(decoded.format_version, 1);
        decoded.supported().unwrap();

        let again = rmp_serde::to_vec(&decoded).unwrap();
        assert_eq!(
            again.first().copied(),
            Some(0x90 | u8::try_from(SNAPSHOT_META_FIELD_COUNT).unwrap())
        );
        let mut ptrs = vec![0x92, 0xc4, 0x20];
        ptrs.extend_from_slice(&[1; 32]);
        ptrs.extend_from_slice(&[0xc4, 0x20]);
        ptrs.extend_from_slice(&[0xfe; 32]);
        assert!(
            again.windows(ptrs.len()).any(|w| w == ptrs.as_slice()),
            "re-encoded item_ptrs must be bins"
        );
    }
}
