pub mod item;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::config::ChunkerConfig;
use vykar_types::chunk_id::ChunkId;

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
