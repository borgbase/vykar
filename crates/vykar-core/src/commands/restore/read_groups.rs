//! Phase 3: coalesce per-chunk targets into pack-aligned `ReadGroup`s. Each
//! read group maps to a single storage range GET.

use std::collections::HashMap;

use vykar_types::chunk_id::ChunkId;
use vykar_types::error::{Result, VykarError};
use vykar_types::pack_id::PackId;

use smallvec::SmallVec;

use super::plan::{ChunkTargets, WriteTarget};
use super::{MAX_COALESCE_GAP, MAX_READ_SIZE};

/// A chunk within a coalesced read group.
pub(super) struct PlannedBlob {
    pub(super) chunk_id: ChunkId,
    pub(super) pack_offset: u64,
    pub(super) stored_size: u32,
    pub(super) expected_size: u32,
    /// Most chunks are referenced by exactly one file, so SmallVec stores
    /// the single target inline without a heap allocation.
    pub(super) targets: SmallVec<[WriteTarget; 1]>,
}

/// A coalesced read — maps to a single storage range GET.
pub(super) struct ReadGroup {
    pub(super) pack_id: PackId,
    pub(super) read_start: u64,
    pub(super) read_end: u64, // exclusive
    pub(super) blobs: Vec<PlannedBlob>,
}

/// Look up each unique chunk's pack location and coalesce into ReadGroups.
/// Consumes `chunk_targets` by value.
pub(super) fn build_read_groups<L>(
    chunk_targets: HashMap<ChunkId, ChunkTargets>,
    lookup: L,
) -> Result<Vec<ReadGroup>>
where
    L: Fn(&ChunkId) -> Option<(PackId, u64, u32)>,
{
    let mut pack_blobs: HashMap<PackId, Vec<PlannedBlob>> = HashMap::new();

    for (chunk_id, ct) in chunk_targets {
        let (pack_id, pack_offset, stored_size) =
            lookup(&chunk_id).ok_or(VykarError::ChunkNotInIndex(chunk_id))?;
        pack_blobs.entry(pack_id).or_default().push(PlannedBlob {
            chunk_id,
            pack_offset,
            stored_size,
            expected_size: ct.expected_size,
            targets: ct.targets,
        });
    }

    // For each pack: sort blobs by offset, then coalesce into ReadGroups.
    let mut groups: Vec<ReadGroup> = Vec::new();

    for (pack_id, mut blobs) in pack_blobs {
        blobs.sort_by_key(|b| b.pack_offset);

        let mut iter = blobs.into_iter();
        let first = iter
            .next()
            .expect("invariant: pack_blobs entries are non-empty by construction above");

        let first_end = first
            .pack_offset
            .checked_add(first.stored_size as u64)
            .ok_or_else(|| {
                VykarError::InvalidFormat(format!(
                    "pack offset overflow in pack {pack_id}: offset {} + size {}",
                    first.pack_offset, first.stored_size,
                ))
            })?;
        let mut cur = ReadGroup {
            pack_id,
            read_start: first.pack_offset,
            read_end: first_end,
            blobs: vec![first],
        };

        for blob in iter {
            let blob_end = blob
                .pack_offset
                .checked_add(blob.stored_size as u64)
                .ok_or_else(|| {
                    VykarError::InvalidFormat(format!(
                        "pack offset overflow in pack {pack_id}: offset {} + size {}",
                        blob.pack_offset, blob.stored_size,
                    ))
                })?;
            let gap = blob.pack_offset.saturating_sub(cur.read_end);
            // sort-by-offset gives blob_end >= cur.read_start in practice, but
            // checked_sub keeps a corrupted index from wrapping silently.
            let merged_size = blob_end.checked_sub(cur.read_start).ok_or_else(|| {
                VykarError::InvalidFormat(format!(
                    "blob ordering violation in pack {pack_id}: blob_end {blob_end} < read_start {}",
                    cur.read_start,
                ))
            })?;

            if gap <= MAX_COALESCE_GAP && merged_size <= MAX_READ_SIZE {
                // Coalesce into the current group.
                cur.read_end = blob_end;
                cur.blobs.push(blob);
            } else {
                // Start a new group.
                groups.push(cur);
                cur = ReadGroup {
                    pack_id,
                    read_start: blob.pack_offset,
                    read_end: blob_end,
                    blobs: vec![blob],
                };
            }
        }
        groups.push(cur);
    }

    Ok(groups)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::commands::restore::plan::PlannedFile;
    use crate::commands::restore::test_support::{
        dummy_chunk_id, dummy_pack_id, index_lookup, make_file_item, make_file_item_with_size,
    };
    use crate::index::ChunkIndex;
    use crate::snapshot::item::Item;
    use std::path::PathBuf;
    use std::sync::atomic::AtomicBool;

    /// Plan reads using a lookup closure that returns `(pack_id, pack_offset, stored_size)`.
    /// The closure abstracts over ChunkIndex vs MmapRestoreCache.
    fn plan_reads<L>(
        file_items: &[(&Item, PathBuf)],
        lookup: L,
    ) -> Result<(Vec<PlannedFile>, Vec<ReadGroup>)>
    where
        L: Fn(&ChunkId) -> Option<(PackId, u64, u32)>,
    {
        let mut files: Vec<PlannedFile> = Vec::with_capacity(file_items.len());

        // Collect all (ChunkId → ChunkTargets) across all files.
        let mut chunk_targets: HashMap<ChunkId, ChunkTargets> = HashMap::new();

        for (file_idx, (item, target_path)) in file_items.iter().enumerate() {
            let mut file_offset: u64 = 0;
            for chunk_ref in &item.chunks {
                let entry = chunk_targets
                    .entry(chunk_ref.id)
                    .or_insert_with(|| ChunkTargets {
                        expected_size: chunk_ref.size,
                        targets: SmallVec::new(),
                    });
                if entry.expected_size != chunk_ref.size {
                    return Err(VykarError::InvalidFormat(format!(
                        "chunk {} has inconsistent logical sizes in snapshot metadata: {} vs {}",
                        chunk_ref.id, entry.expected_size, chunk_ref.size
                    )));
                }
                entry.targets.push(WriteTarget {
                    file_idx,
                    file_offset,
                });
                file_offset = file_offset
                    .checked_add(chunk_ref.size as u64)
                    .ok_or_else(|| {
                        VykarError::InvalidFormat(format!(
                            "file offset overflow building plan for {:?}",
                            item.path
                        ))
                    })?;
            }
            if file_offset != item.size {
                return Err(VykarError::InvalidFormat(format!(
                    "regular file {:?} has size {} but chunk sizes sum to {}",
                    item.path, item.size, file_offset
                )));
            }
            files.push(PlannedFile {
                rel_path: target_path.clone(),
                total_size: file_offset,
                mode: item.mode,
                mtime: item.mtime,
                xattrs: item.xattrs.clone(),
                created: AtomicBool::new(false),
            });
        }

        let groups = build_read_groups(chunk_targets, lookup)?;
        Ok((files, groups))
    }

    #[test]
    fn plan_reads_single_blob_per_pack() {
        let pack = dummy_pack_id(1);
        let mut index = ChunkIndex::new();
        index.add(dummy_chunk_id(0xAA), 100, pack, 1000);

        let item = make_file_item("a.txt", vec![(0xAA, 200)]);
        let file_items = vec![(&item, PathBuf::from("/tmp/out/a.txt"))];

        let (files, groups) = plan_reads(&file_items, index_lookup(&index)).unwrap();
        assert_eq!(files.len(), 1);
        assert_eq!(files[0].total_size, 200);
        assert_eq!(groups.len(), 1);
        assert_eq!(groups[0].blobs.len(), 1);
        assert_eq!(groups[0].read_start, 1000);
        assert_eq!(groups[0].read_end, 1100);
    }

    #[test]
    fn plan_reads_coalesces_adjacent_blobs() {
        let pack = dummy_pack_id(1);
        let mut index = ChunkIndex::new();
        // Two blobs close together in the same pack (gap = 4 bytes for length prefix)
        index.add(dummy_chunk_id(0xAA), 100, pack, 1000);
        index.add(dummy_chunk_id(0xBB), 100, pack, 1104); // 1000 + 100 + 4 = 1104

        let item = make_file_item("a.txt", vec![(0xAA, 200), (0xBB, 300)]);
        let file_items = vec![(&item, PathBuf::from("/tmp/out/a.txt"))];

        let (files, groups) = plan_reads(&file_items, index_lookup(&index)).unwrap();
        assert_eq!(files.len(), 1);
        assert_eq!(files[0].total_size, 500);
        // Both blobs should be coalesced into one ReadGroup
        assert_eq!(groups.len(), 1);
        assert_eq!(groups[0].blobs.len(), 2);
        assert_eq!(groups[0].read_start, 1000);
        assert_eq!(groups[0].read_end, 1204);
    }

    #[test]
    fn plan_reads_splits_on_large_gap() {
        let pack = dummy_pack_id(1);
        let mut index = ChunkIndex::new();
        // Two blobs far apart (gap > MAX_COALESCE_GAP)
        index.add(dummy_chunk_id(0xAA), 100, pack, 1000);
        index.add(
            dummy_chunk_id(0xBB),
            100,
            pack,
            1000 + 100 + MAX_COALESCE_GAP + 1,
        );

        let item = make_file_item("a.txt", vec![(0xAA, 200), (0xBB, 300)]);
        let file_items = vec![(&item, PathBuf::from("/tmp/out/a.txt"))];

        let (_files, groups) = plan_reads(&file_items, index_lookup(&index)).unwrap();
        // Should be split into two ReadGroups
        assert_eq!(groups.len(), 2);
        assert_eq!(groups[0].blobs.len(), 1);
        assert_eq!(groups[1].blobs.len(), 1);
    }

    #[test]
    fn plan_reads_splits_on_max_read_size() {
        let pack = dummy_pack_id(1);
        let mut index = ChunkIndex::new();
        // First blob takes up almost MAX_READ_SIZE, second would push it over
        let big_size = MAX_READ_SIZE as u32 - 100;
        index.add(dummy_chunk_id(0xAA), big_size, pack, 1000);
        index.add(dummy_chunk_id(0xBB), 200, pack, 1000 + big_size as u64 + 4);

        let item = make_file_item("a.txt", vec![(0xAA, 5000), (0xBB, 300)]);
        let file_items = vec![(&item, PathBuf::from("/tmp/out/a.txt"))];

        let (_files, groups) = plan_reads(&file_items, index_lookup(&index)).unwrap();
        // Should be split because merged_size > MAX_READ_SIZE
        assert_eq!(groups.len(), 2);
    }

    #[test]
    fn plan_reads_dedup_across_files() {
        let pack = dummy_pack_id(1);
        let mut index = ChunkIndex::new();
        index.add(dummy_chunk_id(0xAA), 100, pack, 1000);

        // Two files sharing the same chunk
        let item_a = make_file_item("a.txt", vec![(0xAA, 200)]);
        let item_b = make_file_item("b.txt", vec![(0xAA, 200)]);
        let file_items = vec![
            (&item_a, PathBuf::from("/tmp/out/a.txt")),
            (&item_b, PathBuf::from("/tmp/out/b.txt")),
        ];

        let (files, groups) = plan_reads(&file_items, index_lookup(&index)).unwrap();
        assert_eq!(files.len(), 2);
        // Only one ReadGroup since it's the same chunk
        assert_eq!(groups.len(), 1);
        assert_eq!(groups[0].blobs.len(), 1);
        // The blob should have two write targets
        assert_eq!(groups[0].blobs[0].targets.len(), 2);
    }

    #[test]
    fn plan_reads_rejects_inconsistent_logical_chunk_sizes() {
        let pack = dummy_pack_id(1);
        let mut index = ChunkIndex::new();
        index.add(dummy_chunk_id(0xAA), 100, pack, 1000);

        let item_a = make_file_item("a.txt", vec![(0xAA, 200)]);
        let item_b = make_file_item("b.txt", vec![(0xAA, 300)]);
        let file_items = vec![
            (&item_a, PathBuf::from("/tmp/out/a.txt")),
            (&item_b, PathBuf::from("/tmp/out/b.txt")),
        ];

        let err = match plan_reads(&file_items, index_lookup(&index)) {
            Ok(_) => panic!("expected inconsistent logical size error"),
            Err(e) => e.to_string(),
        };
        assert!(err.contains("inconsistent logical sizes"));
    }

    #[test]
    fn plan_reads_empty_file_no_groups() {
        let index = ChunkIndex::new();
        let item = make_file_item("empty.txt", vec![]);
        let file_items = vec![(&item, PathBuf::from("/tmp/out/empty.txt"))];

        let (files, groups) = plan_reads(&file_items, index_lookup(&index)).unwrap();
        assert_eq!(files.len(), 1);
        assert_eq!(files[0].total_size, 0);
        assert_eq!(groups.len(), 0);
    }

    #[test]
    fn plan_reads_multiple_packs() {
        let pack_a = dummy_pack_id(1);
        let pack_b = dummy_pack_id(2);
        let mut index = ChunkIndex::new();
        index.add(dummy_chunk_id(0xAA), 100, pack_a, 1000);
        index.add(dummy_chunk_id(0xBB), 100, pack_b, 2000);

        let item = make_file_item("a.txt", vec![(0xAA, 200), (0xBB, 300)]);
        let file_items = vec![(&item, PathBuf::from("/tmp/out/a.txt"))];

        let (_files, groups) = plan_reads(&file_items, index_lookup(&index)).unwrap();
        // Separate packs → separate ReadGroups
        assert_eq!(groups.len(), 2);
    }

    #[test]
    fn build_read_groups_rejects_offset_overflow() {
        // A corrupted index returns pack_offset=u64::MAX with stored_size=16.
        // build_read_groups must surface InvalidFormat instead of wrapping
        // silently and producing a nonsensical read range.
        let cid = dummy_chunk_id(0xAA);
        let pack_id = dummy_pack_id(1);
        let mut chunk_targets: HashMap<ChunkId, ChunkTargets> = HashMap::new();
        chunk_targets.insert(
            cid,
            ChunkTargets {
                expected_size: 16,
                targets: SmallVec::new(),
            },
        );

        let lookup = |_id: &ChunkId| Some((pack_id, u64::MAX, 16u32));
        let err = match build_read_groups(chunk_targets, lookup) {
            Ok(_) => panic!("expected overflow error"),
            Err(e) => e.to_string(),
        };
        assert!(
            err.contains("pack offset overflow"),
            "expected overflow error, got: {err}"
        );
    }

    #[test]
    fn plan_reads_rejects_size_mismatch_with_chunks() {
        let index = ChunkIndex::new();
        let item = make_file_item_with_size("a.txt", 100, vec![(0xAA, 50)]);
        let file_items = vec![(&item, PathBuf::from("/tmp/out/a.txt"))];

        let err = match plan_reads(&file_items, index_lookup(&index)) {
            Ok(_) => panic!("expected size-vs-chunks mismatch error"),
            Err(e) => e.to_string(),
        };
        assert!(
            err.contains("chunk sizes sum to"),
            "expected size-vs-chunks mismatch error, got: {err}"
        );
    }
}
