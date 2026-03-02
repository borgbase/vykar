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
