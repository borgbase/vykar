//! Pointer analysis of snapshot listings: where new tree chunks sit in
//! `item_ptrs`, and what a content-defined pointer-leaf cut would upload.

use std::collections::{HashMap, HashSet};
use std::time::Instant;

use serde::Deserialize;

use super::shape::{DirShape, EntryKind, Percentiles, ShapeReport, percentiles};
use crate::commands::list::load_snapshot_meta;
use crate::repo::Repository;
use crate::repo::manifest::SnapshotEntry;
use crate::snapshot::SnapshotMeta;
use crate::snapshot::item::{Item, ItemType};
use vykar_types::chunk_id::ChunkId;
use vykar_types::error::{Result, VykarError};

/// Length of the separator key for a chunk whose first key is `first`,
/// following a chunk whose last key is `prev_last`: `lcp + 1`, capped at
/// `first.len()`. The first chunk (`prev_last = None`) has an empty key.
pub fn separator_len(prev_last: Option<&[u8]>, first: &[u8]) -> usize {
    let Some(prev) = prev_last else {
        return 0;
    };
    let lcp = prev.iter().zip(first).take_while(|(a, b)| a == b).count();
    (lcp + 1).min(first.len())
}

/// Encoded size of one simulated leaf record `[bin32 chunk_id, bin key]` in msgpack.
pub fn leaf_record_len(key_len: usize) -> usize {
    let bin_header = if key_len < 256 {
        2
    } else if key_len < 65536 {
        3
    } else {
        5
    };
    1 + 2 + 32 + bin_header + key_len
}

/// msgpack array header for `n` elements.
fn array_header_len(n: usize) -> usize {
    if n < 16 {
        1
    } else if n < 65536 {
        3
    } else {
        5
    }
}

#[derive(Debug, Clone, Copy)]
pub struct LeafCut {
    pub min: usize,
    pub rate: u64,
    pub max: usize,
}

impl LeafCut {
    /// `ItemStreamCutter`'s shape scaled to `target`: min = T/4, rate = 3T/4,
    /// max = 4T, which gives a mean leaf of about T.
    pub fn for_target(target: usize) -> Self {
        Self {
            min: target / 4,
            rate: (target as u64) * 3 / 4,
            max: target * 4,
        }
    }

    /// Boundary after a record whose chunk ID is `id`. Chunk IDs are keyed
    /// hashes already, so no extra key is needed.
    pub fn should_cut(&self, id: &ChunkId, record_len: usize, leaf_len: usize) -> bool {
        if leaf_len >= self.max {
            return true;
        }
        if leaf_len < self.min {
            return false;
        }
        let mut first = [0u8; 8];
        first.copy_from_slice(id.as_bytes().get(..8).unwrap_or(&[0; 8]));
        let h = u64::from_le_bytes(first);
        (h as u128) * (self.rate as u128) < (record_len as u128) << 64
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Leaf {
    pub digest: [u8; 32],
    pub bytes: u64,
    pub records: u32,
    /// Key length of the leaf's first record (its separator in the root).
    pub first_key_len: u32,
}

/// Cut the pointer list into leaves. `records[i] = (item_ptrs[i], key_i)`.
/// The leaf digest is BLAKE3 over each record's `id || u32-le key_len || key`.
pub fn cut_leaves(records: &[(ChunkId, &[u8])], cut: &LeafCut) -> Vec<Leaf> {
    let mut leaves = Vec::new();
    let mut hasher = blake3::Hasher::new();
    let mut bytes = 0usize;
    let mut count = 0u32;
    let mut first_key_len = 0u32;
    for (id, key) in records {
        if count == 0 {
            first_key_len = key.len() as u32;
        }
        let record_len = leaf_record_len(key.len());
        hasher.update(id.as_bytes());
        hasher.update(&(key.len() as u32).to_le_bytes());
        hasher.update(key);
        bytes += record_len;
        count += 1;
        if cut.should_cut(id, record_len, bytes) {
            leaves.push(Leaf {
                digest: *hasher.finalize().as_bytes(),
                bytes: bytes as u64,
                records: count,
                first_key_len,
            });
            hasher = blake3::Hasher::new();
            bytes = 0;
            count = 0;
        }
    }
    if count > 0 {
        leaves.push(Leaf {
            digest: *hasher.finalize().as_bytes(),
            bytes: bytes as u64,
            records: count,
            first_key_len,
        });
    }
    leaves
}

/// Indices in `cur` of chunk IDs absent from `prev`.
pub fn new_positions(prev: &[ChunkId], cur: &[ChunkId]) -> Vec<u32> {
    let prev: HashSet<&ChunkId> = prev.iter().collect();
    cur.iter()
        .enumerate()
        .filter(|(_, id)| !prev.contains(id))
        .map(|(i, _)| i as u32)
        .collect()
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub struct Clustering {
    pub runs: u64,
    pub run_len: Percentiles,
    /// Unchanged pointers between consecutive runs.
    pub gap: Percentiles,
}

/// Runs of consecutive positions, and gaps (in pointers) between consecutive
/// runs. `positions` must be ascending.
pub fn clustering(positions: &[u32]) -> Clustering {
    let mut run_lens = Vec::new();
    let mut gaps = Vec::new();
    let mut iter = positions.iter().copied();
    if let Some(first) = iter.next() {
        let (mut start, mut end) = (first, first);
        for p in iter {
            if p == end + 1 {
                end = p;
            } else {
                run_lens.push(u64::from(end - start + 1));
                gaps.push(u64::from(p - end - 1));
                start = p;
                end = p;
            }
        }
        run_lens.push(u64::from(end - start + 1));
    }
    Clustering {
        runs: run_lens.len() as u64,
        run_len: percentiles(&mut run_lens),
        gap: percentiles(&mut gaps),
    }
}

#[derive(Debug, Clone)]
pub struct ListingOptions {
    pub label: Option<String>,
    pub after: Option<chrono::DateTime<chrono::Utc>>,
    pub before: Option<chrono::DateTime<chrono::Utc>>,
    pub leaf_targets: Vec<usize>,
    /// Decode the latest selected listing into a [`ShapeReport`].
    pub shape: bool,
    /// `Some(n)`: read no tree chunks; every separator key is `n` bytes.
    pub assume_key_len: Option<usize>,
}

impl Default for ListingOptions {
    fn default() -> Self {
        Self {
            label: None,
            after: None,
            before: None,
            leaf_targets: vec![4096, 16384, 32768],
            shape: true,
            assume_key_len: None,
        }
    }
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct SnapshotSummary {
    pub name: String,
    pub id: String,
    pub time: String,
    pub source_label: String,
    pub source_paths: Vec<String>,
    pub pointers: u64,
    /// `rmp_serde` size of today's `item_ptrs`.
    pub ptrs_bytes_today: u64,
    /// Step 6a estimate: `item_ptrs` as one bin32 per pointer.
    pub ptrs_bytes_bin: u64,
    pub ext_present: bool,
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct LeafSim {
    pub target: u64,
    pub leaves: u64,
    pub leaf_bytes: u64,
    pub new_leaves: u64,
    pub new_leaf_bytes: u64,
    /// `leaves * 35` plus each leaf's first-record key length.
    pub root_bytes: u64,
    pub upload_bytes: u64,
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct PairReport {
    pub prev: String,
    pub cur: String,
    pub source_label: String,
    pub hours_apart: u64,
    pub new_chunks: u64,
    /// Stored sizes of the new chunks, from the index.
    pub new_stored_bytes: u64,
    pub positions: Vec<u32>,
    pub clustering: Clustering,
    pub leaves: Vec<LeafSim>,
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct ListingReport {
    pub snapshots: Vec<SnapshotSummary>,
    pub shape_snapshot: Option<String>,
    pub shape: Option<ShapeReport>,
    pub distinct_tree_chunks: u64,
    pub decoded_chunks: u64,
    /// Separator key lengths over every pointer of every selected snapshot;
    /// `None` with `assume_key_len`.
    pub key_lengths: Option<Percentiles>,
    pub pairs: Vec<PairReport>,
    pub elapsed_ms: u64,
}

/// First and last path of a tree chunk.
type ChunkKeys = (Vec<u8>, Vec<u8>);

fn select(entries: &[SnapshotEntry], opts: &ListingOptions) -> Vec<SnapshotEntry> {
    let mut v: Vec<SnapshotEntry> = entries
        .iter()
        .filter(|s| opts.label.as_ref().is_none_or(|l| &s.source_label == l))
        .filter(|s| opts.after.is_none_or(|t| s.time >= t))
        .filter(|s| opts.before.is_none_or(|t| s.time < t))
        .cloned()
        .collect();
    v.sort_by_key(|s| s.time);
    v
}

/// Selected snapshots in time order, as `--list` shows them.
pub fn select_snapshots(repo: &Repository, opts: &ListingOptions) -> Vec<SnapshotEntry> {
    select(&repo.manifest().snapshots, opts)
}

fn decode_chunk(
    repo: &mut Repository,
    id: &ChunkId,
    shape: Option<&mut DirShape>,
) -> Result<ChunkKeys> {
    let entry = *repo
        .chunk_index()
        .get(id)
        .ok_or_else(|| VykarError::Other(format!("tree chunk {id} not in the index")))?;
    let data = repo.read_chunk_at(id, &entry.pack_id, entry.pack_offset, entry.stored_size)?;
    let mut shape = shape;
    let mut de = rmp_serde::Deserializer::new(std::io::Cursor::new(data.as_slice()));
    let mut first: Option<Vec<u8>> = None;
    let mut last: Vec<u8> = Vec::new();
    let mut pos = 0u64;
    while (pos as usize) < data.len() {
        let item = Item::deserialize(&mut de).map_err(|e| {
            VykarError::InvalidFormat(format!("tree chunk {id}: entry at byte {pos}: {e}"))
        })?;
        let next = de.position();
        if let Some(s) = shape.as_deref_mut() {
            let kind = match item.entry_type {
                ItemType::Directory => EntryKind::Dir,
                ItemType::Symlink => EntryKind::Symlink,
                ItemType::RegularFile => EntryKind::File,
            };
            s.push(item.path_bytes(), kind, next - pos);
        }
        if first.is_none() {
            first = Some(item.path_bytes().to_vec());
        }
        last.clear();
        last.extend_from_slice(item.path_bytes());
        pos = next;
    }
    if pos as usize != data.len() {
        return Err(VykarError::InvalidFormat(format!(
            "tree chunk {id}: consumed {pos} of {} bytes",
            data.len()
        )));
    }
    let first = first
        .ok_or_else(|| VykarError::InvalidFormat(format!("tree chunk {id} holds no entries")))?;
    Ok((first, last))
}

/// Separator key of every pointer of one snapshot.
fn separator_keys<'a>(
    ptrs: &[ChunkId],
    memo: &'a HashMap<ChunkId, ChunkKeys>,
) -> Result<Vec<&'a [u8]>> {
    let mut prev_last: Option<&[u8]> = None;
    let mut keys = Vec::with_capacity(ptrs.len());
    for id in ptrs {
        let (first, last) = memo
            .get(id)
            .ok_or_else(|| VykarError::Other(format!("tree chunk {id} was not decoded")))?;
        let len = separator_len(prev_last, first);
        keys.push(first.get(..len).unwrap_or(first));
        prev_last = Some(last);
    }
    Ok(keys)
}

fn summarize(entry: &SnapshotEntry, meta: &SnapshotMeta) -> Result<SnapshotSummary> {
    let n = meta.item_ptrs.len();
    Ok(SnapshotSummary {
        name: entry.name.clone(),
        id: entry.id.to_hex(),
        time: entry.time.to_rfc3339(),
        source_label: entry.source_label.clone(),
        source_paths: meta.source_paths.clone(),
        pointers: n as u64,
        ptrs_bytes_today: rmp_serde::to_vec(&meta.item_ptrs)?.len() as u64,
        ptrs_bytes_bin: (n * 34 + array_header_len(n)) as u64,
        ext_present: meta.ext.is_some(),
    })
}

/// Analyze the selected snapshots: shape of the latest listing, positions of
/// new tree chunks between consecutive snapshots of the same source, and a
/// simulated pointer-leaf cut per target.
pub fn analyze(repo: &mut Repository, opts: &ListingOptions) -> Result<ListingReport> {
    let start = Instant::now();
    let selected = select_snapshots(repo, opts);
    let mut metas = Vec::with_capacity(selected.len());
    for entry in &selected {
        metas.push(load_snapshot_meta(repo, &entry.name)?);
    }
    let mut snapshots = Vec::with_capacity(selected.len());
    for (entry, meta) in selected.iter().zip(&metas) {
        snapshots.push(summarize(entry, meta)?);
    }
    let distinct: HashSet<ChunkId> = metas
        .iter()
        .flat_map(|m| m.item_ptrs.iter().copied())
        .collect();

    let mut memo: HashMap<ChunkId, ChunkKeys> = HashMap::new();
    let mut shape_report = None;
    let mut shape_snapshot = None;
    if opts.assume_key_len.is_none() {
        // Stream order of the latest listing first, then the rest by pack location.
        if let (Some(entry), Some(meta)) = (selected.last(), metas.last()) {
            let mut shape = opts.shape.then(|| DirShape::new(true));
            for id in &meta.item_ptrs {
                if !memo.contains_key(id) {
                    let keys = decode_chunk(repo, id, shape.as_mut())?;
                    memo.insert(*id, keys);
                }
            }
            if let Some(s) = shape {
                shape_report = Some(s.finish());
                shape_snapshot = Some(entry.name.clone());
            }
        }
        let mut rest: Vec<(ChunkId, [u8; 32], u64)> = Vec::new();
        for id in &distinct {
            if memo.contains_key(id) {
                continue;
            }
            let e = repo
                .chunk_index()
                .get(id)
                .ok_or_else(|| VykarError::Other(format!("tree chunk {id} not in the index")))?;
            rest.push((*id, *e.pack_id.as_bytes(), e.pack_offset));
        }
        rest.sort_unstable_by_key(|a| (a.1, a.2));
        for (id, _, _) in rest {
            let keys = decode_chunk(repo, &id, None)?;
            memo.insert(id, keys);
        }
    }

    // Separator keys and leaves per snapshot.
    let assumed: Vec<u8> = vec![0; opts.assume_key_len.unwrap_or(0)];
    let mut all_key_lens = Vec::new();
    let mut leaves_per_snap: Vec<Vec<Vec<Leaf>>> = Vec::with_capacity(metas.len());
    for meta in &metas {
        let keys: Vec<&[u8]> = if opts.assume_key_len.is_some() {
            meta.item_ptrs
                .iter()
                .enumerate()
                .map(|(i, _)| if i == 0 { &[][..] } else { assumed.as_slice() })
                .collect()
        } else {
            separator_keys(&meta.item_ptrs, &memo)?
        };
        if opts.assume_key_len.is_none() {
            all_key_lens.extend(keys.iter().map(|k| k.len() as u64));
        }
        let records: Vec<(ChunkId, &[u8])> = meta
            .item_ptrs
            .iter()
            .copied()
            .zip(keys.iter().copied())
            .collect();
        leaves_per_snap.push(
            opts.leaf_targets
                .iter()
                .map(|t| cut_leaves(&records, &LeafCut::for_target(*t)))
                .collect(),
        );
    }

    // Consecutive pairs within each source label.
    let mut pairs = Vec::new();
    let mut last_of_label: HashMap<&str, usize> = HashMap::new();
    for (cur_idx, entry) in selected.iter().enumerate() {
        let Some(prev_idx) = last_of_label.insert(entry.source_label.as_str(), cur_idx) else {
            continue;
        };
        let (Some(prev_entry), Some(prev_meta), Some(cur_meta)) = (
            selected.get(prev_idx),
            metas.get(prev_idx),
            metas.get(cur_idx),
        ) else {
            continue;
        };
        let positions = new_positions(&prev_meta.item_ptrs, &cur_meta.item_ptrs);
        let mut new_stored_bytes = 0u64;
        for p in &positions {
            let id = cur_meta
                .item_ptrs
                .get(*p as usize)
                .ok_or_else(|| VykarError::Other("position out of range".into()))?;
            let e = repo
                .chunk_index()
                .get(id)
                .ok_or_else(|| VykarError::Other(format!("tree chunk {id} not in the index")))?;
            new_stored_bytes += u64::from(e.stored_size);
        }
        let mut leaves = Vec::new();
        if let (Some(prev_leaves), Some(cur_leaves)) =
            (leaves_per_snap.get(prev_idx), leaves_per_snap.get(cur_idx))
        {
            for ((target, pl), cl) in opts.leaf_targets.iter().zip(prev_leaves).zip(cur_leaves) {
                let prev_digests: HashSet<&[u8; 32]> = pl.iter().map(|l| &l.digest).collect();
                let new: Vec<&Leaf> = cl
                    .iter()
                    .filter(|l| !prev_digests.contains(&l.digest))
                    .collect();
                let new_leaf_bytes: u64 = new.iter().map(|l| l.bytes).sum();
                let root_bytes = cl.len() as u64 * 35
                    + cl.iter().map(|l| u64::from(l.first_key_len)).sum::<u64>();
                leaves.push(LeafSim {
                    target: *target as u64,
                    leaves: cl.len() as u64,
                    leaf_bytes: cl.iter().map(|l| l.bytes).sum(),
                    new_leaves: new.len() as u64,
                    new_leaf_bytes,
                    root_bytes,
                    upload_bytes: root_bytes + new_leaf_bytes,
                });
            }
        }
        let hours_apart = (entry.time - prev_entry.time).num_hours().max(0) as u64;
        pairs.push(PairReport {
            prev: prev_entry.name.clone(),
            cur: entry.name.clone(),
            source_label: entry.source_label.clone(),
            hours_apart,
            new_chunks: positions.len() as u64,
            new_stored_bytes,
            clustering: clustering(&positions),
            positions,
            leaves,
        });
    }

    Ok(ListingReport {
        snapshots,
        shape_snapshot,
        shape: shape_report,
        distinct_tree_chunks: distinct.len() as u64,
        decoded_chunks: memo.len() as u64,
        key_lengths: opts
            .assume_key_len
            .is_none()
            .then(|| percentiles(&mut all_key_lens)),
        pairs,
        elapsed_ms: start.elapsed().as_millis() as u64,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ids(n: usize, seed: u64) -> Vec<ChunkId> {
        let mut x = seed;
        (0..n)
            .map(|_| {
                let mut b = [0u8; 32];
                for chunk in b.chunks_mut(8) {
                    x ^= x << 13;
                    x ^= x >> 7;
                    x ^= x << 17;
                    chunk.copy_from_slice(&x.to_le_bytes());
                }
                ChunkId::from_bytes(b)
            })
            .collect()
    }

    #[test]
    fn separator_len_cases() {
        assert_eq!(separator_len(None, b"abc"), 0);
        assert_eq!(separator_len(Some(b"abc"), b"abd"), 3);
        assert_eq!(separator_len(Some(b"abc"), b"b"), 1);
        assert_eq!(separator_len(Some(b"abc"), b"abc"), 3);
        assert_eq!(separator_len(Some(b"ab"), b"abc"), 3);
    }

    #[test]
    fn leaf_record_len_headers() {
        assert_eq!(leaf_record_len(0), 37);
        assert_eq!(leaf_record_len(255), 292);
        assert_eq!(leaf_record_len(256), 294);
    }

    fn new_leaf_count(prev: &[Leaf], cur: &[Leaf]) -> usize {
        let prev: HashSet<&[u8; 32]> = prev.iter().map(|l| &l.digest).collect();
        cur.iter().filter(|l| !prev.contains(&l.digest)).count()
    }

    #[test]
    fn leaf_cut_resyncs_after_insert() {
        let key = [7u8; 40];
        let base = ids(50_000, 0x9e37_79b9_7f4a_7c15);
        let records = |v: &[ChunkId]| -> Vec<(ChunkId, &[u8])> {
            v.iter().map(|id| (*id, &key[..])).collect()
        };
        let cut = LeafCut::for_target(16384);
        let before = cut_leaves(&records(&base), &cut);

        let mut inserted = base.clone();
        inserted.insert(25_000, ids(1, 42)[0]);
        let after = cut_leaves(&records(&inserted), &cut);
        assert!(new_leaf_count(&before, &after) <= 3);

        let mut removed = base.clone();
        removed.remove(25_000);
        let after = cut_leaves(&records(&removed), &cut);
        assert!(new_leaf_count(&before, &after) <= 3);

        for t in [4096u64, 16384, 32768] {
            let leaves = cut_leaves(&records(&base), &LeafCut::for_target(t as usize));
            let total: u64 = leaves.iter().map(|l| l.bytes).sum();
            let mean = total / leaves.len() as u64;
            assert!(
                (t / 2..=t * 2).contains(&mean),
                "mean leaf {mean} B for target {t}"
            );
        }
    }

    #[test]
    fn new_positions_and_clustering() {
        let all = ids(13, 1);
        let prev: Vec<ChunkId> = all[..10].to_vec();
        let mut cur = prev.clone();
        cur[2] = all[10];
        cur[3] = all[11];
        cur[7] = all[12];
        let pos = new_positions(&prev, &cur);
        assert_eq!(pos, vec![2, 3, 7]);

        let runs = clustering(&pos);
        assert_eq!(runs.runs, 2);
        assert_eq!(
            (runs.run_len.sum, runs.run_len.max, runs.run_len.count),
            (3, 2, 2)
        );
        assert_eq!((runs.gap.count, runs.gap.max), (1, 3));

        assert_eq!(clustering(&[]).runs, 0);
    }
}
