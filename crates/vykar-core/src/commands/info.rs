use chrono::{DateTime, Utc};

use crate::config::VykarConfig;
use crate::repo::EncryptionMode;
use vykar_types::error::Result;

use super::list::load_snapshot_meta;
use super::util::open_repo;
use crate::repo::OpenOptions;

/// Repository statistics for the `info` command.
#[derive(Debug, Clone)]
pub struct InfoStats {
    pub snapshot_count: usize,
    pub last_snapshot_time: Option<DateTime<Utc>>,
    pub raw_size: u64,
    pub compressed_size: u64,
    pub deduplicated_size: u64,
    pub unique_stored_size: u64,
    pub referenced_stored_size: u64,
    pub unique_chunks: usize,
    pub repo_created: DateTime<Utc>,
    pub encryption: EncryptionMode,
}

/// Run `vykar info`.
pub fn run(config: &VykarConfig, passphrase: Option<&str>) -> Result<InfoStats> {
    let repo = open_repo(config, passphrase, OpenOptions::new().with_index())?;

    let mut raw_size = 0u64;
    let mut compressed_size = 0u64;
    let mut deduplicated_size = 0u64;
    let mut last_snapshot_time = None;

    for entry in &repo.manifest().snapshots {
        if last_snapshot_time.is_none_or(|current| entry.time > current) {
            last_snapshot_time = Some(entry.time);
        }

        let meta = load_snapshot_meta(&repo, &entry.name)?;
        raw_size = raw_size.saturating_add(meta.stats.original_size);
        compressed_size = compressed_size.saturating_add(meta.stats.compressed_size);
        deduplicated_size = deduplicated_size.saturating_add(meta.stats.deduplicated_size);
    }

    let mut unique_stored_size = 0u64;
    let mut referenced_stored_size = 0u64;
    for (_chunk_id, chunk) in repo.chunk_index().iter() {
        unique_stored_size = unique_stored_size.saturating_add(chunk.stored_size as u64);
        referenced_stored_size =
            referenced_stored_size.saturating_add(chunk.stored_size as u64 * chunk.refcount as u64);
    }

    Ok(InfoStats {
        snapshot_count: repo.manifest().snapshots.len(),
        last_snapshot_time,
        raw_size,
        compressed_size,
        deduplicated_size,
        unique_stored_size,
        referenced_stored_size,
        unique_chunks: repo.chunk_index().len(),
        repo_created: repo.config.created,
        encryption: repo.config.encryption.clone(),
    })
}
