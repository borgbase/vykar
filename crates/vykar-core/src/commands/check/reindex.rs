//! `check --repair` re-indexes the chunks live snapshots reference that the
//! index lost, from the packs the index names none of (a lost index update:
//! the loser's packs are on storage, no head names them).

use std::collections::HashSet;

use super::scan::{ScanResult, is_transient_io};
use super::types::{IntegrityIssue, RepairIndexLoad};
use crate::commands::list::for_each_decoded_item;
use crate::commands::mark::{
    ChunkOutcome, OnChunkError, TreeChunkSet, default_decode_threads, walk_tree_chunks,
};
use crate::repo::control::CancelToken;
use crate::repo::index_write::reindex_refusal;
use crate::repo::{IndexRecovery, Repo, TreeChunkSource};
use crate::snapshot::item::ItemType;
use vykar_types::chunk_id::ChunkId;
use vykar_types::error::{Result, VykarError};
use vykar_types::pack_id::PackId;

/// What the repair re-indexed before the scan its plan is built from.
pub(super) struct Reindexed {
    pub(super) chunk_count: usize,
    pub(super) pack_ids: Vec<PackId>,
}

/// When `scan` found live references the index lacks (`DanglingItemPtr`,
/// `DanglingFileChunk`) and the index was read (`IndexRecovery::Complete`),
/// add those that packs the index names none of describe, then the data
/// chunks of the trees of every snapshot with a missing tree pointer that
/// are still missing (read from storage, now that the trees are indexed).
/// `None` when nothing was added. Read-only on storage; edits the in-memory
/// index only.
///
/// Complete: every tree pointer the index lacks is a `DanglingItemPtr`,
/// whether or not its snapshot was walked; a snapshot with one was not
/// walked (`UnreadableSnapshot`), and the second round walks exactly those
/// snapshots' trees. Tree chunks do not nest, so no further round is needed.
pub(super) fn reindex_lost_entries(
    repo: &mut Repo,
    scan: &ScanResult,
    load: &RepairIndexLoad,
    cancel: &CancelToken,
) -> Result<Option<Reindexed>> {
    if load.recovery != IndexRecovery::Complete {
        return Ok(None);
    }
    let mut want: Vec<ChunkId> = Vec::new();
    let mut unwalked: HashSet<&str> = HashSet::new();
    for issue in &scan.issues {
        match issue {
            IntegrityIssue::DanglingItemPtr {
                snapshot_name,
                chunk_id,
            } => {
                want.push(*chunk_id);
                unwalked.insert(snapshot_name);
            }
            IntegrityIssue::DanglingFileChunk { chunk_id, .. } => want.push(*chunk_id),
            _ => {}
        }
    }
    if want.is_empty() {
        return Ok(None);
    }
    let recovered = repo.unindexed_pack_entries(cancel)?;
    for pack in &recovered.uncovered {
        // Only logged: `RepairPlan::uncovered_packs` is a rebuild's.
        tracing::warn!(
            "pack {} is named by no index entry and describes none of its chunks ({})",
            pack.pack_id,
            pack.reason
        );
    }
    if recovered.entries.is_empty() {
        return Ok(None);
    }
    let (n1, mut packs) = repo.add_recovered_entries(&recovered, want);

    // The data chunks under the trees just indexed.
    let ptr_lists: Vec<Vec<ChunkId>> = repo
        .manifest()
        .snapshots
        .iter()
        .filter(|e| unwalked.contains(e.name.as_str()))
        .filter_map(|e| scan.snapshot_item_ptrs.get(&e.id))
        .map(|ptrs| ptrs.iter().copied().collect())
        .collect();
    let mut data: Vec<ChunkId> = Vec::new();
    if !ptr_lists.is_empty() {
        let lists: Vec<&[ChunkId]> = ptr_lists.iter().map(Vec::as_slice).collect();
        let set = TreeChunkSet::build(&lists);
        let index = repo.chunk_index();
        walk_tree_chunks(
            repo,
            &set,
            TreeChunkSource::Storage,
            OnChunkError::Record,
            default_decode_threads(),
            cancel,
            |_, plain| {
                // A decode error keeps what was read; the second scan
                // reports it.
                let mut ids = Vec::new();
                let _ = for_each_decoded_item(plain, |item| {
                    cancel.check()?;
                    if item.entry_type == ItemType::RegularFile {
                        ids.extend(
                            item.chunks
                                .iter()
                                .map(|r| r.id)
                                .filter(|id| !index.contains(id)),
                        );
                    }
                    Ok(())
                });
                ids
            },
            |_, outcome| {
                match outcome {
                    ChunkOutcome::Decoded(ids) => data.extend(ids),
                    // A storage error: the data chunks under this tree
                    // would be reported missing although the packs may
                    // hold them, so the repair fails instead.
                    ChunkOutcome::ReadFailed(e) if is_transient_io(&e) => {
                        return Err(reindex_refusal(VykarError::Other(e.to_string())));
                    }
                    // A tree still missing or unreadable: the second scan
                    // reports it.
                    ChunkOutcome::NotInIndex | ChunkOutcome::ReadFailed(_) => {}
                }
                Ok(())
            },
        )?;
        cancel.check()?;
    }
    let (n2, packs2) = repo.add_recovered_entries(&recovered, data);
    packs.extend(packs2);
    packs.sort_unstable_by_key(|p| *p.as_bytes());
    packs.dedup();
    let chunk_count = n1 + n2;
    if chunk_count == 0 {
        return Ok(None);
    }
    tracing::info!(
        chunks = chunk_count,
        packs = packs.len(),
        unindexed_packs = recovered.counts.packs,
        "re-indexed chunks the index lost"
    );
    Ok(Some(Reindexed {
        chunk_count,
        pack_ids: packs,
    }))
}
