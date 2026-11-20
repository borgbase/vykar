//! Reference model of `segment::apply_to_temp`: an entry list and a BTreeMap,
//! no streaming, no positional cursors. Agrees with `apply_to_temp` on
//! success or failure for any chain; error messages are not compared.
//!
//! Uses only `pub` items, so it can be reused outside this module.

use std::collections::{BTreeMap, BTreeSet};

use crate::index::dedup_cache::{encode_full_entry, FullCacheEntry, StateHash};
use crate::index::segment::{ChainStep, Segment};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Reject {
    /// A segment's `prev_state_hash`/`prev_entries` differ from the state before it.
    ChainMismatch { step: usize },
    /// The layer's input has a different length from the segment's `prev_entries`.
    CountMismatch { step: usize },
    /// A new entry's chunk is in the layer's input (removed by this layer or not).
    NewEntryInPredecessor { step: usize },
    /// A refcount delta leaves 0..=u32::MAX.
    RefcountOutOfRange { step: usize },
    /// A relocation at a position this layer's delta removes.
    RelocationOfRemoved { step: usize },
    /// The layer's output differs from the step's `state_hash_after`/`entries_after`.
    ResultMismatch { step: usize },
}

/// BLAKE3 over the canonical records, as the full cache trailer.
pub(crate) fn state_hash(entries: &[FullCacheEntry]) -> StateHash {
    let mut hasher = blake3::Hasher::new();
    for e in entries {
        hasher.update(&encode_full_entry(e));
    }
    hasher.finalize().into()
}

/// One layer over `input` (sorted, unique). Checks only the layer's own
/// ops, not the chain or the result; `step` is for the reject reason.
pub(crate) fn apply_layer(
    input: &[FullCacheEntry],
    segment: &Segment,
    step: usize,
) -> Result<Vec<FullCacheEntry>, Reject> {
    let deltas: BTreeMap<u64, i64> = segment
        .deltas
        .iter()
        .map(|d| (u64::from(d.position), d.delta))
        .collect();
    let relocations: BTreeMap<u64, _> = segment
        .relocations
        .iter()
        .map(|r| (u64::from(r.position), r))
        .collect();

    let mut out: BTreeMap<[u8; 32], FullCacheEntry> = BTreeMap::new();
    for (i, entry) in input.iter().enumerate() {
        let pos = i as u64;
        let mut e = *entry;
        let mut removed = false;
        if let Some(&delta) = deltas.get(&pos) {
            let r = i128::from(e.refcount) + i128::from(delta);
            if !(0..=i128::from(u32::MAX)).contains(&r) {
                return Err(Reject::RefcountOutOfRange { step });
            }
            e.refcount = u32::try_from(r).expect("checked range");
            removed = r == 0;
        }
        if let Some(r) = relocations.get(&pos) {
            if removed {
                return Err(Reject::RelocationOfRemoved { step });
            }
            e.pack_id = r.pack_id;
            e.pack_offset = r.pack_offset;
            e.stored_size = r.stored_size;
        }
        if !removed {
            out.insert(*e.chunk_id.as_bytes(), e);
        }
    }

    let input_ids: BTreeSet<[u8; 32]> = input.iter().map(|e| *e.chunk_id.as_bytes()).collect();
    for n in &segment.new_entries {
        if input_ids.contains(n.chunk_id.as_bytes()) {
            return Err(Reject::NewEntryInPredecessor { step });
        }
        out.insert(*n.chunk_id.as_bytes(), *n);
    }
    Ok(out.into_values().collect())
}

/// `apply_to_temp(prev, chain)`: `Ok(entries)` iff it succeeds, with these entries.
pub(crate) fn apply_chain(
    prev: &[FullCacheEntry],
    chain: &[ChainStep<'_>],
) -> Result<Vec<FullCacheEntry>, Reject> {
    let mut expected = (state_hash(prev), prev.len() as u64);
    for (k, step) in chain.iter().enumerate() {
        if (step.segment.prev_state_hash, step.segment.prev_entries) != expected {
            return Err(Reject::ChainMismatch { step: k });
        }
        expected = (step.state_hash_after, step.entries_after);
    }

    let mut cur = prev.to_vec();
    for (k, step) in chain.iter().enumerate() {
        if cur.len() as u64 != step.segment.prev_entries {
            return Err(Reject::CountMismatch { step: k });
        }
        let out = apply_layer(&cur, step.segment, k)?;
        if (state_hash(&out), out.len() as u64) != (step.state_hash_after, step.entries_after) {
            return Err(Reject::ResultMismatch { step: k });
        }
        cur = out;
    }
    Ok(cur)
}
