//! Keyed index segment, body version 2: what one index write changed, by
//! chunk ID. Segments name no predecessor; a head applies them over its base
//! in its order.
//!
//! Layout (before compression; all integers little-endian):
//!
//! ```text
//! version       u8 = 2
//! kind          u8 (0 backup, 1 maintenance)
//! reserved      [u8; 2] = 0
//! pack_count    u32
//! additions     u32
//! removals      u32
//! relocations   u32
//! packs         pack_count  × { pack_id [32], trailer_len u32 }
//! additions     additions   × { chunk_id [32], pack_slot u32, pack_offset u32, stored_size u32 }
//! removals      removals    × { chunk_id [32] }
//! relocations   relocations × { chunk_id [32], pack_slot u32, pack_offset u32, stored_size u32 }
//! ```
//!
//! Canonical: each section strictly increasing by chunk ID, a chunk ID in
//! at most one section, packs strictly increasing and each referenced by an
//! addition or a relocation, the length exact, and not empty (no writer
//! writes an empty segment). A backup segment only adds. Violations are
//! `InvalidFormat`; a version other than 2 is `UnsupportedVersion`.
//!
//! No section is reserved for later use: a new section (a two-phase
//! sweep's condemned entries, a list of reused chunks) is a new body
//! version, which this binary refuses by version, and is named by a head
//! feature so that older binaries refuse the index before they read or
//! write it (architecture.md, Segmented Index: the sweep is single-phase).

// Every slice is taken at an offset computed from counts checked against the
// body's length first.
#![allow(clippy::indexing_slicing)]

use std::collections::HashSet;

use vykar_types::chunk_id::ChunkId;
use vykar_types::error::{Result, VykarError};
use vykar_types::pack_id::PackId;

use super::base_v2::offset_u32;
use super::head::SegmentKind;

pub const SEGMENT_V2_VERSION: u8 = 2;
const HEADER_LEN: usize = 20;
const PACK_LEN: usize = 36;
const LOCATED_LEN: usize = 44;
const REMOVAL_LEN: usize = 32;

fn invalid(msg: impl Into<String>) -> VykarError {
    VykarError::InvalidFormat(format!("index segment: {}", msg.into()))
}

fn u32_at(b: &[u8], at: usize) -> u32 {
    u32::from_le_bytes(b[at..at + 4].try_into().expect("4 bytes"))
}

fn id_at(b: &[u8], at: usize) -> [u8; 32] {
    b[at..at + 32].try_into().expect("32 bytes")
}

/// Where a chunk is stored.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Location {
    pub pack_id: PackId,
    pub pack_offset: u64,
    pub stored_size: u32,
}

/// A decoded keyed segment, pack slots resolved.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SegmentBody {
    pub kind: SegmentKind,
    /// Sorted by chunk ID.
    pub additions: Vec<(ChunkId, Location)>,
    /// Sorted.
    pub removals: Vec<ChunkId>,
    /// Sorted by chunk ID.
    pub relocations: Vec<(ChunkId, Location)>,
    /// The trailer length of every pack an addition or relocation names,
    /// sorted by pack ID.
    pub pack_trailers: Vec<(PackId, u32)>,
}

fn strictly_increasing<'a>(mut ids: impl Iterator<Item = &'a ChunkId>) -> bool {
    let Some(mut prev) = ids.next() else {
        return true;
    };
    for id in ids {
        if prev.as_bytes() >= id.as_bytes() {
            return false;
        }
        prev = id;
    }
    true
}

impl SegmentBody {
    /// A segment of `kind` with no changes.
    pub fn empty(kind: SegmentKind) -> Self {
        Self {
            kind,
            additions: Vec::new(),
            removals: Vec::new(),
            relocations: Vec::new(),
            pack_trailers: Vec::new(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.additions.is_empty() && self.removals.is_empty() && self.relocations.is_empty()
    }

    /// Additions + removals + relocations (what `SegmentRef::entries` says).
    pub fn entries(&self) -> u64 {
        (self.additions.len() + self.removals.len() + self.relocations.len()) as u64
    }

    /// The canonical-form rules of the module docs.
    pub fn validate(&self) -> Result<()> {
        if self.is_empty() {
            return Err(invalid("no changes"));
        }
        if self.kind == SegmentKind::Backup
            && (!self.removals.is_empty() || !self.relocations.is_empty())
        {
            return Err(invalid("a backup segment changes existing entries"));
        }
        if !strictly_increasing(self.additions.iter().map(|(id, _)| id))
            || !strictly_increasing(self.removals.iter())
            || !strictly_increasing(self.relocations.iter().map(|(id, _)| id))
        {
            return Err(invalid("a section is not strictly increasing"));
        }
        let mut seen = HashSet::with_capacity(self.entries() as usize);
        for id in self
            .additions
            .iter()
            .map(|(id, _)| id)
            .chain(self.removals.iter())
            .chain(self.relocations.iter().map(|(id, _)| id))
        {
            if !seen.insert(*id) {
                return Err(invalid(format!("chunk {id} in more than one section")));
            }
        }
        if self
            .pack_trailers
            .windows(2)
            .any(|p| p[0].0.as_bytes() >= p[1].0.as_bytes())
        {
            return Err(invalid("pack table not strictly increasing"));
        }
        let mut referenced = vec![false; self.pack_trailers.len()];
        for (id, loc) in self.additions.iter().chain(self.relocations.iter()) {
            offset_u32(id, loc.pack_offset)?;
            let slot = self
                .slot_of(&loc.pack_id)
                .ok_or_else(|| invalid(format!("pack {} not in the pack table", loc.pack_id)))?;
            referenced[slot] = true;
        }
        if referenced.iter().any(|r| !r) {
            return Err(invalid("a pack of the table is not referenced"));
        }
        Ok(())
    }

    fn slot_of(&self, pack: &PackId) -> Option<usize> {
        self.pack_trailers
            .binary_search_by(|(p, _)| p.as_bytes().cmp(pack.as_bytes()))
            .ok()
    }

    /// The canonical body; [`Self::validate`] first.
    pub fn encode(&self) -> Result<Vec<u8>> {
        self.validate()?;
        let count = |n: usize| u32::try_from(n).map_err(|_| invalid("section too large"));
        let mut out = Vec::with_capacity(
            HEADER_LEN
                + PACK_LEN * self.pack_trailers.len()
                + LOCATED_LEN * (self.additions.len() + self.relocations.len())
                + REMOVAL_LEN * self.removals.len(),
        );
        out.push(SEGMENT_V2_VERSION);
        out.push(u8::from(self.kind));
        out.extend_from_slice(&[0, 0]);
        for n in [
            self.pack_trailers.len(),
            self.additions.len(),
            self.removals.len(),
            self.relocations.len(),
        ] {
            out.extend_from_slice(&count(n)?.to_le_bytes());
        }
        for (pack, trailer_len) in &self.pack_trailers {
            out.extend_from_slice(pack.as_bytes());
            out.extend_from_slice(&trailer_len.to_le_bytes());
        }
        let located = |out: &mut Vec<u8>, id: &ChunkId, loc: &Location| -> Result<()> {
            let slot = self.slot_of(&loc.pack_id).expect("validated") as u32;
            out.extend_from_slice(id.as_bytes());
            out.extend_from_slice(&slot.to_le_bytes());
            out.extend_from_slice(&offset_u32(id, loc.pack_offset)?.to_le_bytes());
            out.extend_from_slice(&loc.stored_size.to_le_bytes());
            Ok(())
        };
        for (id, loc) in &self.additions {
            located(&mut out, id, loc)?;
        }
        for id in &self.removals {
            out.extend_from_slice(id.as_bytes());
        }
        for (id, loc) in &self.relocations {
            located(&mut out, id, loc)?;
        }
        Ok(out)
    }

    /// Decode and validate a body. `max_entries` (the head's
    /// `SegmentRef::entries`) bounds the header counts before anything is
    /// allocated.
    pub fn decode(body: &[u8], max_entries: u64) -> Result<Self> {
        if let Some(&v) = body.first()
            && v != SEGMENT_V2_VERSION
        {
            return Err(VykarError::UnsupportedVersion(u32::from(v)));
        }
        if body.len() < HEADER_LEN {
            return Err(invalid("truncated header"));
        }
        let kind = SegmentKind::try_from(body[1]).map_err(invalid)?;
        if body[2..4] != [0, 0] {
            return Err(invalid("reserved bytes are not zero"));
        }
        let packs = u64::from(u32_at(body, 4));
        let adds = u64::from(u32_at(body, 8));
        let rems = u64::from(u32_at(body, 12));
        let rels = u64::from(u32_at(body, 16));
        if adds + rems + rels > max_entries {
            return Err(invalid(format!(
                "{} entries, more than the head's {max_entries}",
                adds + rems + rels
            )));
        }
        if packs > adds + rels {
            return Err(invalid("more packs than located entries"));
        }
        let expected = HEADER_LEN as u64
            + PACK_LEN as u64 * packs
            + LOCATED_LEN as u64 * (adds + rels)
            + REMOVAL_LEN as u64 * rems;
        if expected != body.len() as u64 {
            return Err(invalid(format!(
                "length {} does not match its counts",
                body.len()
            )));
        }
        let mut at = HEADER_LEN;
        let mut pack_trailers = Vec::with_capacity(packs as usize);
        for _ in 0..packs {
            pack_trailers.push((PackId::from_bytes(id_at(body, at)), u32_at(body, at + 32)));
            at += PACK_LEN;
        }
        let located = |at: &mut usize| -> Result<(ChunkId, Location)> {
            let id = ChunkId::from_bytes(id_at(body, *at));
            let slot = u32_at(body, *at + 32) as usize;
            let (pack_id, _) = *pack_trailers
                .get(slot)
                .ok_or_else(|| invalid(format!("pack slot {slot} out of range")))?;
            let loc = Location {
                pack_id,
                pack_offset: u64::from(u32_at(body, *at + 36)),
                stored_size: u32_at(body, *at + 40),
            };
            *at += LOCATED_LEN;
            Ok((id, loc))
        };
        let mut additions = Vec::with_capacity(adds as usize);
        for _ in 0..adds {
            additions.push(located(&mut at)?);
        }
        let mut removals = Vec::with_capacity(rems as usize);
        for _ in 0..rems {
            removals.push(ChunkId::from_bytes(id_at(body, at)));
            at += REMOVAL_LEN;
        }
        let mut relocations = Vec::with_capacity(rels as usize);
        for _ in 0..rels {
            relocations.push(located(&mut at)?);
        }
        let seg = Self {
            kind,
            additions,
            removals,
            relocations,
            pack_trailers,
        };
        seg.validate()?;
        Ok(seg)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    fn cid(b: u8) -> ChunkId {
        ChunkId::from_bytes([b; 32])
    }

    fn loc(p: u8, off: u64, size: u32) -> Location {
        Location {
            pack_id: PackId::from_bytes([p; 32]),
            pack_offset: off,
            stored_size: size,
        }
    }

    fn backup() -> SegmentBody {
        SegmentBody {
            kind: SegmentKind::Backup,
            additions: vec![(cid(1), loc(9, 10, 100)), (cid(2), loc(9, 20, 200))],
            removals: Vec::new(),
            relocations: Vec::new(),
            pack_trailers: vec![(PackId::from_bytes([9; 32]), 123)],
        }
    }

    fn maintenance() -> SegmentBody {
        SegmentBody {
            kind: SegmentKind::Maintenance,
            additions: vec![(cid(1), loc(8, 10, 100))],
            removals: vec![cid(2), cid(3)],
            relocations: vec![(cid(4), loc(9, 0, 7))],
            pack_trailers: vec![
                (PackId::from_bytes([8; 32]), 0),
                (PackId::from_bytes([9; 32]), 55),
            ],
        }
    }

    /// SEG-1.
    #[test]
    fn round_trips() {
        for seg in [backup(), maintenance()] {
            let body = seg.encode().unwrap();
            assert_eq!(body[0], SEGMENT_V2_VERSION);
            let decoded = SegmentBody::decode(&body, seg.entries()).unwrap();
            assert_eq!(decoded, seg);
            assert_eq!(decoded.encode().unwrap(), body);
        }
        assert_eq!(maintenance().entries(), 4);
    }

    /// SEG-2.
    #[test]
    fn invalid_segments_are_refused() {
        let invalid = |s: SegmentBody| matches!(s.encode(), Err(VykarError::InvalidFormat(_)));
        let mut s = backup();
        s.removals.push(cid(5));
        assert!(invalid(s), "backup with a removal");
        let mut s = backup();
        s.relocations.push((cid(5), loc(9, 0, 1)));
        assert!(invalid(s), "backup with a relocation");
        let mut s = maintenance();
        s.removals = vec![cid(1)];
        assert!(invalid(s), "chunk in two sections");
        assert!(invalid(SegmentBody::empty(SegmentKind::Maintenance)));
        let mut s = backup();
        s.additions.reverse();
        assert!(invalid(s), "unsorted");
        let mut s = backup();
        s.pack_trailers.push((PackId::from_bytes([10; 32]), 0));
        assert!(invalid(s), "unreferenced pack");
        let mut s = backup();
        s.pack_trailers.clear();
        assert!(invalid(s), "pack missing from the table");

        let mut body = backup().encode().unwrap();
        body[1] = 2;
        assert!(matches!(
            SegmentBody::decode(&body, 10),
            Err(VykarError::InvalidFormat(_))
        ));
        let mut body = backup().encode().unwrap();
        body[0] = 1;
        assert!(matches!(
            SegmentBody::decode(&body, 10),
            Err(VykarError::UnsupportedVersion(1))
        ));
        let mut body = backup().encode().unwrap();
        body.push(0);
        assert!(SegmentBody::decode(&body, 10).is_err());
        let mut body = backup().encode().unwrap();
        body[3] = 1;
        assert!(SegmentBody::decode(&body, 10).is_err());
    }

    /// SEG-3: the head's entry count bounds the header before any allocation.
    #[test]
    fn header_counts_are_bounded_before_allocation() {
        let mut body = vec![SEGMENT_V2_VERSION, 0, 0, 0];
        body.extend_from_slice(&1u32.to_le_bytes());
        body.extend_from_slice(&(1u32 << 31).to_le_bytes());
        body.extend_from_slice(&0u32.to_le_bytes());
        body.extend_from_slice(&0u32.to_le_bytes());
        body.extend_from_slice(&[0; 20]);
        let err = SegmentBody::decode(&body, 1000).unwrap_err().to_string();
        assert!(err.contains("more than the head"), "{err}");
        // Even with an unbounded limit the length check refuses it.
        assert!(SegmentBody::decode(&body, u64::MAX).is_err());
        let ok = backup().encode().unwrap();
        assert!(SegmentBody::decode(&ok, 1).is_err());
    }

    fn arb_segment() -> impl Strategy<Value = SegmentBody> {
        (
            any::<bool>(),
            proptest::collection::btree_map(
                any::<[u8; 32]>(),
                (0u8..3, 0u8..4, any::<u32>(), any::<u32>()),
                1..40,
            ),
            any::<[u32; 4]>(),
        )
            .prop_map(|(backup, entries, trailers)| {
                let kind = if backup {
                    SegmentKind::Backup
                } else {
                    SegmentKind::Maintenance
                };
                let mut seg = SegmentBody::empty(kind);
                let mut packs = std::collections::BTreeMap::new();
                for (id, (section, p, off, size)) in entries {
                    let id = ChunkId::from_bytes(id);
                    let section = if backup { 0 } else { section };
                    let l = loc(p, u64::from(off), size);
                    match section {
                        0 => seg.additions.push((id, l)),
                        1 => seg.removals.push(id),
                        _ => seg.relocations.push((id, l)),
                    }
                    if section != 1 {
                        packs.insert([p; 32], trailers[p as usize]);
                    }
                }
                seg.pack_trailers = packs
                    .into_iter()
                    .map(|(p, t)| (PackId::from_bytes(p), t))
                    .collect();
                seg
            })
    }

    proptest! {
        /// SEG-4.
        #[test]
        fn decode_of_encode_is_identity(seg in arb_segment()) {
            let body = seg.encode().unwrap();
            let decoded = SegmentBody::decode(&body, seg.entries()).unwrap();
            prop_assert_eq!(&decoded, &seg);
            prop_assert_eq!(decoded.encode().unwrap(), body);
        }
    }
}
