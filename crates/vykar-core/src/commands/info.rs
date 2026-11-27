use chrono::{DateTime, Utc};

use crate::config::VykarConfig;
use crate::repo::{EncryptionMode, RepoFormat, WriterMode};
use vykar_types::error::Result;

use super::list::load_snapshot_meta;
use super::util::open_repo;
use crate::repo::OpenOptions;

/// Repository statistics for the `info` command. Serialized in `vykar info
/// --json` (schema 1): the field names are JSON names, so only add fields.
#[derive(Debug, Clone, serde::Serialize)]
pub struct InfoStats {
    pub snapshot_count: usize,
    pub last_snapshot_time: Option<DateTime<Utc>>,
    pub original_size: u64,
    pub compressed_size: u64,
    pub deduplicated_size: u64,
    /// Stored bytes of every index entry, including entries no snapshot
    /// references any more until the next compact sweeps them.
    pub unique_stored_size: u64,
    /// Stored bytes times references, counted with snapshot multiplicity,
    /// as the last sweep found them. `None` before the first compact.
    pub referenced_stored_size: Option<u64>,
    /// When the last sweep ran (the client clock of that compact).
    pub last_sweep_time: Option<DateTime<Utc>>,
    pub unique_chunks: usize,
    pub repo_created: DateTime<Utc>,
    /// JSON: `EncryptionMode::as_str`, not the repository config's encoding.
    #[serde(serialize_with = "ser_encryption")]
    pub encryption: EncryptionMode,
    /// Layout version. Worth showing because users will hold a mix of v2 and
    /// v3 repositories, and the format determines which binaries can read one.
    /// JSON: `{"version": 6, "chunk_hash": "blake3"}`.
    #[serde(serialize_with = "ser_format")]
    pub format: RepoFormat,
    /// Shape of the index (absent on a handle without a head).
    pub index: Option<IndexLayoutStats>,
    /// The last compact on this machine refused its mark because the index
    /// lacks a chunk a live snapshot needs, and no compact has marked since
    /// (nor a repair applied): [`crate::commands::compact_refusal`].
    pub compact_refused: Option<crate::commands::compact_refusal::CompactRefusal>,
    /// As `config` records it. JSON: "single" or "multi" (the
    /// `--commit-mode` values).
    #[serde(serialize_with = "ser_writer_mode")]
    pub writer_mode: WriterMode,
    /// The repository is single-writer and this handle's storage offers
    /// put-if-absent (as far as it knows without a request): `vykar upgrade
    /// --commit-mode multi` could switch it.
    pub put_if_absent_offered: bool,
}

fn ser_encryption<S: serde::Serializer>(
    mode: &EncryptionMode,
    s: S,
) -> std::result::Result<S::Ok, S::Error> {
    s.serialize_str(mode.as_str())
}

// `serialize_with` passes a reference.
#[allow(clippy::trivially_copy_pass_by_ref)]
fn ser_format<S: serde::Serializer>(
    format: &RepoFormat,
    s: S,
) -> std::result::Result<S::Ok, S::Error> {
    use serde::ser::SerializeStruct;
    let mut st = s.serialize_struct("RepoFormat", 2)?;
    st.serialize_field("version", &format.version())?;
    st.serialize_field("chunk_hash", format.chunk_hash().as_str())?;
    st.end()
}

#[allow(clippy::trivially_copy_pass_by_ref)]
fn ser_writer_mode<S: serde::Serializer>(
    mode: &WriterMode,
    s: S,
) -> std::result::Result<S::Ok, S::Error> {
    s.serialize_str(match mode {
        WriterMode::Single => "single",
        WriterMode::Multi => "multi",
    })
}

/// Shape of a segmented index: its base and the segments written since.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize)]
pub struct IndexLayoutStats {
    pub base_bytes: u64,
    pub segments: usize,
    pub segment_bytes: u64,
}

/// Run `vykar info`.
pub fn run(config: &VykarConfig, passphrase: Option<&str>) -> Result<InfoStats> {
    let repo = open_repo(
        config,
        passphrase,
        OpenOptions::new().with_index(),
        &crate::repo::control::CancelToken::new(),
    )?;

    let mut original_size = 0u64;
    let mut compressed_size = 0u64;
    let mut deduplicated_size = 0u64;
    let mut last_snapshot_time = None;

    for entry in &repo.manifest().snapshots {
        if last_snapshot_time.is_none_or(|current| entry.time > current) {
            last_snapshot_time = Some(entry.time);
        }

        let meta = load_snapshot_meta(&repo, &entry.name)?;
        original_size = original_size.saturating_add(meta.stats.original_size);
        compressed_size = compressed_size.saturating_add(meta.stats.compressed_size);
        deduplicated_size = deduplicated_size.saturating_add(meta.stats.deduplicated_size);
    }

    let mut unique_stored_size = 0u64;
    for (_chunk_id, chunk) in repo.chunk_index().iter() {
        unique_stored_size = unique_stored_size.saturating_add(chunk.stored_size as u64);
    }
    let record = match repo.index_head() {
        Some(head) => crate::repo::index_store::fetch_sweep_record(
            repo.storage().as_ref(),
            repo.crypto().as_ref(),
            head,
        )
        .unwrap_or_else(|e| {
            tracing::warn!("the last sweep record cannot be read: {e}");
            None
        }),
        None => None,
    };

    Ok(InfoStats {
        snapshot_count: repo.manifest().snapshots.len(),
        last_snapshot_time,
        original_size,
        compressed_size,
        deduplicated_size,
        unique_stored_size,
        referenced_stored_size: record.as_ref().map(|r| r.referenced_bytes),
        last_sweep_time: record.as_ref().map(|r| r.time),
        unique_chunks: repo.chunk_index().len(),
        repo_created: repo.config().created,
        encryption: repo.config().encryption.clone(),
        format: repo.format(),
        index: repo.index_head().map(|head| IndexLayoutStats {
            base_bytes: head.base.as_ref().map_or(0, |b| b.size),
            segments: head.segments.len(),
            segment_bytes: head.segments.iter().map(|s| s.size).sum(),
        }),
        compact_refused: crate::commands::compact_refusal::read(repo.cache_dir().as_deref()),
        writer_mode: repo.writer_mode(),
        put_if_absent_offered: repo.writer_mode() == WriterMode::Single
            && repo.storage_offers_put_if_absent(),
    })
}
