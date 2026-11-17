//! Grouping of blobs in one pack into range reads, shared by
//! `Repository::read_chunks_coalesced_into` and restore's read planner.

use vykar_types::error::{Result, VykarError};
use vykar_types::pack_id::PackId;

/// Largest gap (bytes) between two blobs of one pack that are still
/// fetched in a single range read.
pub(crate) const MAX_COALESCE_GAP: u64 = 256 * 1024;

/// Largest single coalesced range read (bytes).
pub(crate) const MAX_READ_SIZE: u64 = 16 * 1024 * 1024;

/// Sort `blobs` (all in `pack_id`) by offset and split them into range
/// reads: a blob joins the current read while the gap before it is at
/// most `MAX_COALESCE_GAP` and the read stays within `MAX_READ_SIZE`.
/// `extent` gives a blob's `(pack_offset, stored_size)`. Calls
/// `emit(read_start, read_end, blobs)` once per read, in offset order,
/// with exactly sized vectors; `read_end` is exclusive.
///
/// An offset that overflows `u64` (corrupt index) is `InvalidFormat`.
pub(crate) fn coalesce_pack_reads<T>(
    pack_id: &PackId,
    mut blobs: Vec<T>,
    extent: impl Fn(&T) -> (u64, u32),
    mut emit: impl FnMut(u64, u64, Vec<T>),
) -> Result<()> {
    blobs.sort_by_key(|b| extent(b).0);

    // Pass 1: find the read boundaries as (blob count, read_start, read_end).
    let mut spans: Vec<(usize, u64, u64)> = Vec::new();
    for blob in &blobs {
        let (pack_offset, stored_size) = extent(blob);
        let blob_end = pack_offset.checked_add(stored_size as u64).ok_or_else(|| {
            VykarError::InvalidFormat(format!(
                "pack offset overflow in pack {pack_id}: offset {pack_offset} + size {stored_size}",
            ))
        })?;
        if let Some((len, read_start, read_end)) = spans.last_mut() {
            let gap = pack_offset.saturating_sub(*read_end);
            // sort-by-offset gives blob_end >= read_start in practice, but
            // checked_sub keeps a corrupted index from wrapping silently.
            let merged_size = blob_end.checked_sub(*read_start).ok_or_else(|| {
                VykarError::InvalidFormat(format!(
                    "blob ordering violation in pack {pack_id}: blob_end {blob_end} < read_start {read_start}",
                ))
            })?;
            if gap <= MAX_COALESCE_GAP && merged_size <= MAX_READ_SIZE {
                *len += 1;
                *read_end = blob_end;
                continue;
            }
        }
        spans.push((1, pack_offset, blob_end));
    }

    // Pass 2: move the blobs into exactly sized per-read vectors. Callers
    // hold the reads until they are processed, so push-growth slack here
    // would be held for most of a batch.
    let mut iter = blobs.into_iter();
    for (len, read_start, read_end) in spans {
        let mut read_blobs = Vec::with_capacity(len);
        read_blobs.extend(iter.by_ref().take(len));
        emit(read_start, read_end, read_blobs);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    type Blob = (u64, u32);
    type Read = (u64, u64, Vec<Blob>);

    fn reads(blobs: Vec<Blob>) -> Result<Vec<Read>> {
        let mut out = Vec::new();
        coalesce_pack_reads(
            &PackId::from_bytes([7; 32]),
            blobs,
            |b| *b,
            |start, end, blobs| out.push((start, end, blobs)),
        )?;
        Ok(out)
    }

    #[test]
    fn merges_up_to_gap_and_size_bounds_inclusive() {
        let r = reads(vec![(0, 100), (100 + MAX_COALESCE_GAP, 100)]).unwrap();
        assert_eq!(r.len(), 1);
        assert_eq!((r[0].0, r[0].1), (0, 200 + MAX_COALESCE_GAP));

        let r = reads(vec![(0, 100), (101 + MAX_COALESCE_GAP, 100)]).unwrap();
        assert_eq!(r.len(), 2);

        let half = (MAX_READ_SIZE / 2) as u32;
        let r = reads(vec![(0, half), (half as u64, half)]).unwrap();
        assert_eq!(r.len(), 1);
        assert_eq!(r[0].1, MAX_READ_SIZE);

        let r = reads(vec![(0, half), (half as u64, half + 1)]).unwrap();
        assert_eq!(r.len(), 2);
    }

    #[test]
    fn sorts_and_sizes_exactly() {
        let r = reads(vec![(300, 10), (0, 10), (5_000_000, 10), (100, 10)]).unwrap();
        assert_eq!(r.len(), 2);
        assert_eq!(r[0].2, vec![(0, 10), (100, 10), (300, 10)]);
        assert_eq!(r[1].2, vec![(5_000_000, 10)]);
        assert!(r[0].0 < r[1].0);
        for (_, _, blobs) in &r {
            assert_eq!(blobs.capacity(), blobs.len());
        }
    }

    #[test]
    fn duplicate_offsets_share_a_read() {
        let r = reads(vec![(64, 32), (64, 32)]).unwrap();
        assert_eq!(r.len(), 1);
        assert_eq!((r[0].0, r[0].1), (64, 96));
        assert_eq!(r[0].2.len(), 2);
    }

    #[test]
    fn offset_overflow_is_invalid_format() {
        let err = reads(vec![(u64::MAX, 16)]).unwrap_err();
        assert!(matches!(err, VykarError::InvalidFormat(_)), "{err:?}");
        assert!(err.to_string().contains("pack offset overflow"), "{err}");
    }
}
