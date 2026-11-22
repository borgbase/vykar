//! Index base, body version 2: the full sorted records of one index state,
//! fixed-width, with pack IDs moved into a table.
//!
//! Layout (before compression; all integers little-endian):
//!
//! ```text
//! version      u8 = 2
//! reserved     [u8; 3] = 0
//! pack_count   u32
//! entry_count  u32
//! packs        pack_count  × { pack_id [32], trailer_len u32 }                     36 B
//! records      entry_count × { chunk_id [32], pack_slot u32, pack_offset u32,
//!                              stored_size u32 }                                 44 B
//! ```
//!
//! Fixed width, so the decompressed body *is* the local table file
//! (memory-mapped, binary-searched) and its length follows from the counts.
//! `trailer_len` is the length of the pack's trailer frame (the 4-byte length
//! prefix plus the trailer blob), 0 for a pack without one.
//!
//! Canonical: packs strictly increasing by ID, every pack referenced by a
//! record, records strictly increasing by chunk ID, slots in range, reserved
//! bytes zero, the length exact, at most one pack per record and at least
//! one pack when there are records. Every violation is `InvalidFormat`; a
//! version other than 2 is `UnsupportedVersion`, so a v1 base (version byte
//! 1) is refused by version, never misparsed.

// Every slice below is taken at an offset computed from counts that were
// checked against the body's length first.
#![allow(clippy::indexing_slicing)]

use std::io::Write;

use vykar_types::chunk_id::ChunkId;
use vykar_types::error::{Result, VykarError};
use vykar_types::pack_id::PackId;

pub const BASE_V2_VERSION: u8 = 2;
pub const BASE_V2_HEADER_LEN: usize = 12;
pub const BASE_V2_PACK_LEN: usize = 36;
pub const BASE_V2_RECORD_LEN: usize = 44;

fn invalid(msg: impl Into<String>) -> VykarError {
    VykarError::InvalidFormat(format!("index base: {}", msg.into()))
}

fn u32_at(b: &[u8], at: usize) -> u32 {
    u32::from_le_bytes(b[at..at + 4].try_into().expect("4 bytes"))
}

/// The exact body length for these counts.
pub(crate) fn base_body_len(pack_count: u32, entry_count: u32) -> u64 {
    BASE_V2_HEADER_LEN as u64
        + BASE_V2_PACK_LEN as u64 * u64::from(pack_count)
        + BASE_V2_RECORD_LEN as u64 * u64::from(entry_count)
}

/// A record's offset in a pack as stored: `InvalidFormat` beyond u32.
pub(crate) fn offset_u32(chunk_id: &ChunkId, pack_offset: u64) -> Result<u32> {
    u32::try_from(pack_offset).map_err(|_| {
        invalid(format!(
            "pack offset {pack_offset} of chunk {chunk_id} does not fit a v2 index record"
        ))
    })
}

/// Streams a canonical body: the pack table up front, then the records in
/// chunk-ID order. Checks order and slot bounds as it goes; [`Self::finish`]
/// checks the record count and that every pack was referenced.
pub struct BaseWriter<W: Write> {
    w: W,
    pack_count: u32,
    referenced: Vec<bool>,
    unreferenced: usize,
    entry_count: u32,
    pushed: u32,
    last: Option<ChunkId>,
}

impl<W: Write> BaseWriter<W> {
    /// Write the header and `packs` (pack ID, trailer length), which must be
    /// strictly increasing by ID.
    pub fn new(mut w: W, packs: &[(PackId, u32)], entry_count: u32) -> Result<Self> {
        let pack_count = u32::try_from(packs.len()).map_err(|_| invalid("too many packs"))?;
        if pack_count > entry_count || (entry_count > 0 && pack_count == 0) {
            return Err(invalid(format!(
                "{pack_count} packs for {entry_count} records"
            )));
        }
        if packs
            .windows(2)
            .any(|p| p[0].0.as_bytes() >= p[1].0.as_bytes())
        {
            return Err(invalid("pack table not strictly increasing"));
        }
        let mut header = [0u8; BASE_V2_HEADER_LEN];
        header[0] = BASE_V2_VERSION;
        header[4..8].copy_from_slice(&pack_count.to_le_bytes());
        header[8..12].copy_from_slice(&entry_count.to_le_bytes());
        w.write_all(&header)?;
        for (pack, trailer_len) in packs {
            w.write_all(pack.as_bytes())?;
            w.write_all(&trailer_len.to_le_bytes())?;
        }
        Ok(Self {
            w,
            pack_count,
            referenced: vec![false; packs.len()],
            unreferenced: packs.len(),
            entry_count,
            pushed: 0,
            last: None,
        })
    }

    /// Append one record; `pack_slot` indexes the pack table.
    pub fn push(
        &mut self,
        chunk_id: &ChunkId,
        pack_slot: u32,
        pack_offset: u64,
        stored_size: u32,
    ) -> Result<()> {
        if self.pushed == self.entry_count {
            return Err(invalid("more records than announced"));
        }
        if self
            .last
            .is_some_and(|l| l.as_bytes() >= chunk_id.as_bytes())
        {
            return Err(invalid("records not strictly increasing"));
        }
        if pack_slot >= self.pack_count {
            return Err(invalid(format!("pack slot {pack_slot} out of range")));
        }
        let offset = offset_u32(chunk_id, pack_offset)?;
        let mut rec = [0u8; BASE_V2_RECORD_LEN];
        rec[..32].copy_from_slice(chunk_id.as_bytes());
        rec[32..36].copy_from_slice(&pack_slot.to_le_bytes());
        rec[36..40].copy_from_slice(&offset.to_le_bytes());
        rec[40..44].copy_from_slice(&stored_size.to_le_bytes());
        self.w.write_all(&rec)?;
        let seen = &mut self.referenced[pack_slot as usize];
        if !*seen {
            *seen = true;
            self.unreferenced -= 1;
        }
        self.pushed += 1;
        self.last = Some(*chunk_id);
        Ok(())
    }

    /// Check the counts and hand back the writer.
    pub fn finish(self) -> Result<W> {
        if self.pushed != self.entry_count {
            return Err(invalid(format!(
                "{} of {} records written",
                self.pushed, self.entry_count
            )));
        }
        if self.unreferenced > 0 {
            return Err(invalid("a pack of the table has no record"));
        }
        Ok(self.w)
    }
}

/// A validated body, read in place.
#[derive(Clone, Copy)]
pub(crate) struct BaseView<'a> {
    body: &'a [u8],
    pack_count: u32,
    entry_count: u32,
}

impl<'a> BaseView<'a> {
    /// [`validate_base_body`], then a view over `body`.
    #[cfg(any(test, feature = "fuzzing"))]
    pub(crate) fn new(body: &'a [u8]) -> Result<Self> {
        let (pack_count, entry_count) = validate_base_body(body)?;
        Ok(Self::trusted(body, pack_count, entry_count))
    }

    /// A view over a body validated before (a mapped table file).
    pub(crate) fn trusted(body: &'a [u8], pack_count: u32, entry_count: u32) -> Self {
        Self {
            body,
            pack_count,
            entry_count,
        }
    }

    #[cfg(any(test, feature = "fuzzing"))]
    pub(crate) fn pack_count(&self) -> u32 {
        self.pack_count
    }

    #[cfg(any(test, feature = "fuzzing"))]
    pub(crate) fn entry_count(&self) -> u32 {
        self.entry_count
    }

    fn pack_base(&self, slot: u32) -> usize {
        BASE_V2_HEADER_LEN + BASE_V2_PACK_LEN * slot as usize
    }

    fn record_base(&self, position: u32) -> usize {
        BASE_V2_HEADER_LEN
            + BASE_V2_PACK_LEN * self.pack_count as usize
            + BASE_V2_RECORD_LEN * position as usize
    }

    /// Pack ID and trailer length of a slot. Panics out of range.
    pub(crate) fn pack_at(&self, slot: u32) -> (PackId, u32) {
        assert!(slot < self.pack_count, "pack slot out of range");
        let at = self.pack_base(slot);
        let id = PackId::from_bytes(self.body[at..at + 32].try_into().expect("32 bytes"));
        (id, u32_at(self.body, at + 32))
    }

    /// The slot of `pack`, by binary search.
    pub(crate) fn find_pack(&self, pack: &PackId) -> Option<u32> {
        let (mut lo, mut hi) = (0u32, self.pack_count);
        while lo < hi {
            let mid = lo + (hi - lo) / 2;
            let at = self.pack_base(mid);
            match self.body[at..at + 32].cmp(pack.as_bytes().as_slice()) {
                std::cmp::Ordering::Less => lo = mid + 1,
                std::cmp::Ordering::Greater => hi = mid,
                std::cmp::Ordering::Equal => return Some(mid),
            }
        }
        None
    }

    /// The 32 chunk ID bytes of a record. Panics out of range.
    pub(crate) fn chunk_id_bytes_at(&self, position: u32) -> &'a [u8] {
        assert!(position < self.entry_count, "record position out of range");
        let at = self.record_base(position);
        &self.body[at..at + 32]
    }

    /// Chunk ID, pack slot, offset and stored size of a record.
    pub(crate) fn record_at(&self, position: u32) -> (ChunkId, u32, u32, u32) {
        let id_bytes = self.chunk_id_bytes_at(position);
        let at = self.record_base(position);
        (
            ChunkId::from_bytes(id_bytes.try_into().expect("32 bytes")),
            u32_at(self.body, at + 32),
            u32_at(self.body, at + 36),
            u32_at(self.body, at + 40),
        )
    }

    /// The position of `id`, by binary search.
    pub(crate) fn find(&self, id: &ChunkId) -> Option<u32> {
        let (mut lo, mut hi) = (0u32, self.entry_count);
        while lo < hi {
            let mid = lo + (hi - lo) / 2;
            match self.chunk_id_bytes_at(mid).cmp(id.as_bytes().as_slice()) {
                std::cmp::Ordering::Less => lo = mid + 1,
                std::cmp::Ordering::Greater => hi = mid,
                std::cmp::Ordering::Equal => return Some(mid),
            }
        }
        None
    }
}

/// Validate a whole body (local-file open, tests, fuzz); returns
/// `(pack_count, entry_count)`.
pub fn validate_base_body(body: &[u8]) -> Result<(u32, u32)> {
    if body.len() < BASE_V2_HEADER_LEN {
        if let Some(&v) = body.first()
            && v != BASE_V2_VERSION
        {
            return Err(VykarError::UnsupportedVersion(u32::from(v)));
        }
        return Err(invalid("truncated header"));
    }
    if body[0] != BASE_V2_VERSION {
        return Err(VykarError::UnsupportedVersion(u32::from(body[0])));
    }
    if body[1..4] != [0, 0, 0] {
        return Err(invalid("reserved bytes are not zero"));
    }
    let pack_count = u32_at(body, 4);
    let entry_count = u32_at(body, 8);
    if base_body_len(pack_count, entry_count) != body.len() as u64 {
        return Err(invalid(format!(
            "length {} does not match {pack_count} packs and {entry_count} records",
            body.len()
        )));
    }
    if pack_count > entry_count || (entry_count > 0 && pack_count == 0) {
        return Err(invalid(format!(
            "{pack_count} packs for {entry_count} records"
        )));
    }
    let view = BaseView::trusted(body, pack_count, entry_count);
    for slot in 1..pack_count {
        let a = view.pack_base(slot - 1);
        let b = view.pack_base(slot);
        if body[a..a + 32] >= body[b..b + 32] {
            return Err(invalid("pack table not strictly increasing"));
        }
    }
    let mut referenced = vec![false; pack_count as usize];
    let mut prev: Option<&[u8]> = None;
    for position in 0..entry_count {
        let id = view.chunk_id_bytes_at(position);
        if prev.is_some_and(|p| p >= id) {
            return Err(invalid("records not strictly increasing"));
        }
        prev = Some(id);
        let slot = u32_at(body, view.record_base(position) + 32);
        let Some(seen) = referenced.get_mut(slot as usize) else {
            return Err(invalid(format!("pack slot {slot} out of range")));
        };
        *seen = true;
    }
    if referenced.iter().any(|r| !r) {
        return Err(invalid("a pack of the table has no record"));
    }
    Ok((pack_count, entry_count))
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    fn cid(b: u8) -> ChunkId {
        ChunkId::from_bytes([b; 32])
    }

    fn pid(b: u8) -> PackId {
        PackId::from_bytes([b; 32])
    }

    /// (chunk, slot, offset, size) records over packs.
    fn build(packs: &[(PackId, u32)], records: &[(ChunkId, u32, u64, u32)]) -> Result<Vec<u8>> {
        let mut w = BaseWriter::new(Vec::new(), packs, records.len() as u32)?;
        for (id, slot, off, size) in records {
            w.push(id, *slot, *off, *size)?;
        }
        w.finish()
    }

    fn good() -> Vec<u8> {
        build(
            &[(pid(1), 0), (pid(2), 77)],
            &[
                (cid(1), 1, 10, 100),
                (cid(2), 0, 20, 200),
                (cid(3), 1, 30, 300),
            ],
        )
        .unwrap()
    }

    /// BASE-1.
    #[test]
    fn writer_output_validates_and_reads_back() {
        let body = good();
        assert_eq!(body.len() as u64, base_body_len(2, 3));
        let view = BaseView::new(&body).unwrap();
        assert_eq!((view.pack_count(), view.entry_count()), (2, 3));
        assert_eq!(view.pack_at(0), (pid(1), 0));
        assert_eq!(view.pack_at(1), (pid(2), 77));
        assert_eq!(view.find_pack(&pid(2)), Some(1));
        assert_eq!(view.find_pack(&pid(3)), None);
        assert_eq!(view.record_at(2), (cid(3), 1, 30, 300));
        assert_eq!(view.find(&cid(2)), Some(1));
        assert_eq!(view.find(&cid(9)), None);
        let empty = build(&[], &[]).unwrap();
        assert_eq!(validate_base_body(&empty).unwrap(), (0, 0));
    }

    /// BASE-2.
    #[test]
    fn canonical_violations_are_refused() {
        let invalid = |r: Result<Vec<u8>>| matches!(r, Err(VykarError::InvalidFormat(_)));
        let checked = |body: Vec<u8>| validate_base_body(&body).map(|_| body);
        // The writer refuses.
        assert!(invalid(build(
            &[(pid(2), 0), (pid(1), 0)],
            &[(cid(1), 0, 0, 1), (cid(2), 1, 0, 1)]
        )));
        assert!(invalid(build(
            &[(pid(1), 0)],
            &[(cid(2), 0, 0, 1), (cid(1), 0, 0, 1)]
        )));
        assert!(invalid(build(
            &[(pid(1), 0)],
            &[(cid(1), 0, 0, 1), (cid(1), 0, 0, 1)]
        )));
        assert!(invalid(build(&[(pid(1), 0)], &[(cid(1), 1, 0, 1)])));
        assert!(invalid(build(
            &[(pid(1), 0), (pid(2), 0)],
            &[(cid(1), 0, 0, 1), (cid(2), 0, 0, 1)]
        )));
        assert!(invalid(build(
            &[(pid(1), 0), (pid(2), 0)],
            &[(cid(1), 0, 0, 1)]
        )));
        assert!(invalid(build(&[], &[(cid(1), 0, 0, 1)])));
        // The validator refuses the same bodies built by hand.
        let body = good();
        let header = BASE_V2_HEADER_LEN;
        let rec0 = header + 2 * BASE_V2_PACK_LEN;
        let mut b = body.clone();
        b[header..header + 32].copy_from_slice(&[3; 32]);
        assert!(invalid(checked(b)), "unsorted packs");
        let mut b = body.clone();
        b[rec0..rec0 + 32].copy_from_slice(&[2; 32]);
        assert!(invalid(checked(b)), "duplicate chunk");
        let mut b = body.clone();
        b[rec0..rec0 + 32].copy_from_slice(&[4; 32]);
        assert!(invalid(checked(b)), "unsorted records");
        let mut b = body.clone();
        b[rec0 + 32] = 2;
        assert!(invalid(checked(b)), "slot out of range");
        let mut b = body.clone();
        b[rec0 + 32 + BASE_V2_RECORD_LEN] = 1;
        assert!(invalid(checked(b)), "pack 0 unreferenced");
        let mut b = body.clone();
        b[2] = 1;
        assert!(invalid(checked(b)), "reserved byte");
        let mut b = body.clone();
        b.pop();
        assert!(invalid(checked(b)), "short");
        let mut b = body.clone();
        b.push(0);
        assert!(invalid(checked(b)), "long");
        let mut b = body.clone();
        b[4] = 4;
        b.extend_from_slice(&[0; 2 * BASE_V2_PACK_LEN]);
        assert!(invalid(checked(b)), "pack_count > entry_count");
        for v in [1u8, 3] {
            let mut b = body.clone();
            b[0] = v;
            assert!(matches!(
                validate_base_body(&b),
                Err(VykarError::UnsupportedVersion(x)) if x == u32::from(v)
            ));
        }
        assert!(matches!(
            validate_base_body(&[1]),
            Err(VykarError::UnsupportedVersion(1))
        ));
    }

    /// BASE-3.
    #[test]
    fn an_offset_beyond_u32_is_refused() {
        let mut w = BaseWriter::new(Vec::new(), &[(pid(1), 0)], 1).unwrap();
        let err = w
            .push(&cid(1), 0, u64::from(u32::MAX) + 1, 1)
            .unwrap_err()
            .to_string();
        assert!(err.contains("does not fit"), "{err}");
        let mut w = BaseWriter::new(Vec::new(), &[(pid(1), 0)], 1).unwrap();
        w.push(&cid(1), 0, u64::from(u32::MAX), 1).unwrap();
        w.finish().unwrap();
    }

    proptest! {
        /// BASE-4.
        #[test]
        fn random_entry_sets_round_trip(
            entries in proptest::collection::btree_map(
                any::<[u8; 32]>(),
                (0u8..4, any::<u32>(), any::<u32>()),
                0..64,
            ),
            trailers in any::<[u32; 4]>(),
        ) {
            let mut packs: Vec<(PackId, u32)> = entries
                .values()
                .map(|(p, _, _)| (pid(*p), trailers[*p as usize]))
                .collect();
            packs.sort_by(|a, b| a.0.as_bytes().cmp(b.0.as_bytes()));
            packs.dedup_by(|a, b| a.0 == b.0);
            let mut w = BaseWriter::new(Vec::new(), &packs, entries.len() as u32).unwrap();
            for (id, (p, off, size)) in &entries {
                let slot = packs.iter().position(|(q, _)| *q == pid(*p)).unwrap() as u32;
                w.push(&ChunkId::from_bytes(*id), slot, u64::from(*off), *size).unwrap();
            }
            let body = w.finish().unwrap();
            let view = BaseView::new(&body).unwrap();
            prop_assert_eq!(view.entry_count() as usize, entries.len());
            for (id, (p, off, size)) in &entries {
                let id = ChunkId::from_bytes(*id);
                let pos = view.find(&id).unwrap();
                let (rid, slot, roff, rsize) = view.record_at(pos);
                prop_assert_eq!(rid, id);
                prop_assert_eq!(view.pack_at(slot), (pid(*p), trailers[*p as usize]));
                prop_assert_eq!((roff, rsize), (*off, *size));
            }
        }
    }
}
