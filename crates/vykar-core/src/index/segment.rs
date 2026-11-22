//! Index segments: the change between two index states, by position.
//!
//! A segment records what turns one index state (the *predecessor*, a sorted
//! run of canonical records) into the next:
//!
//! - refcount deltas at positions of the predecessor's sorted order; a
//!   result of 0 removes the entry,
//! - relocations (new pack, offset, stored size) at such positions,
//! - new entries, as full records in chunk-ID order.
//!
//! Positions instead of chunk IDs are what keep a segment small: nearly every
//! backup bumps the refcount of most chunks, and a bumped position costs a
//! fraction of a byte after compression where a chunk ID costs 32.
//!
//! An index state is identified by its `state_hash`, BLAKE3 over its
//! canonical records — the full index cache's checksum trailer. [`apply`]
//! streams a chain of segments over a predecessor cache in one pass and
//! checks each layer's resulting state against the expected one; [`diff_to_temp`]
//! produces the segment between a cache and an in-memory [`ChunkIndex`].
//!
//! The encoding here is the segment *body*; compression, encryption and
//! naming belong to the storage layer.

// Column slices are indexed only below lengths checked against the decoded
// counts; segment positions are bounded by `prev_entries` before use.
#![allow(clippy::indexing_slicing)]

use std::path::Path;

use tempfile::TempPath;
use vykar_types::chunk_id::ChunkId;
use vykar_types::error::{Result, VykarError};
use vykar_types::pack_id::PackId;

use super::ChunkIndex;
use super::dedup_cache::{FullCacheEntry, FullCacheWriter, MmapFullIndexCache, StateHash};
pub use super::table::diff_live_to_temp;

/// Segment body layout version.
const SEGMENT_VERSION: u8 = 1;

/// Minimum encoded bytes of one element of each section, used to bound the
/// header counts against the input before allocating.
const MIN_DELTA_BYTES: usize = 2; // position gap + value, one varint each
const MIN_RELOCATION_BYTES: usize = 1 + 32 + 1 + 1;
const MIN_NEW_ENTRY_BYTES: usize = 32 + 1 + 1 + 32 + 1;

/// A refcount change at a predecessor position. Never 0.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RefcountDelta {
    pub position: u32,
    pub delta: i64,
}

/// A new location for the entry at a predecessor position.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Relocation {
    pub position: u32,
    pub pack_id: PackId,
    pub pack_offset: u64,
    pub stored_size: u32,
}

/// The change from one index state to the next. See the module docs.
///
/// Invariants (enforced by [`Segment::decode`] and [`Segment::validate`]):
/// delta and relocation positions strictly increasing and below
/// `prev_entries`; deltas non-zero; new entries strictly increasing by chunk
/// ID with refcount ≥ 1.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Segment {
    pub prev_state_hash: StateHash,
    pub prev_entries: u64,
    pub deltas: Vec<RefcountDelta>,
    pub relocations: Vec<Relocation>,
    pub new_entries: Vec<FullCacheEntry>,
}

/// Encoded size of each section, for measurement.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct SegmentSizes {
    pub header: usize,
    pub delta_positions: usize,
    pub delta_values: usize,
    pub relocations: usize,
    pub new_entries: usize,
}

impl SegmentSizes {
    pub fn total(&self) -> usize {
        self.header + self.delta_positions + self.delta_values + self.relocations + self.new_entries
    }
}

fn invalid(msg: impl Into<String>) -> VykarError {
    VykarError::InvalidFormat(format!("index segment: {}", msg.into()))
}

// ---------------------------------------------------------------------------
// Varints
// ---------------------------------------------------------------------------

fn put_uvarint(out: &mut Vec<u8>, mut v: u64) {
    while v >= 0x80 {
        out.push((v as u8) | 0x80);
        v >>= 7;
    }
    out.push(v as u8);
}

/// LEB128, rejecting overlong encodings and values above `u64::MAX`, so that
/// every value has exactly one encoding.
fn get_uvarint(input: &mut &[u8]) -> Result<u64> {
    let mut value: u64 = 0;
    for i in 0..10 {
        let (&byte, rest) = input
            .split_first()
            .ok_or_else(|| invalid("truncated varint"))?;
        *input = rest;
        let bits = u64::from(byte & 0x7f);
        if i == 9 && bits > 1 {
            return Err(invalid("varint overflows u64"));
        }
        value |= bits << (7 * i);
        if byte & 0x80 == 0 {
            if i > 0 && byte == 0 {
                return Err(invalid("overlong varint"));
            }
            return Ok(value);
        }
    }
    Err(invalid("varint too long"))
}

fn zigzag(v: i64) -> u64 {
    ((v << 1) ^ (v >> 63)) as u64
}

fn unzigzag(u: u64) -> i64 {
    ((u >> 1) as i64) ^ -((u & 1) as i64)
}

fn take<'a>(input: &mut &'a [u8], n: usize) -> Result<&'a [u8]> {
    if input.len() < n {
        return Err(invalid("truncated"));
    }
    let (head, rest) = input.split_at(n);
    *input = rest;
    Ok(head)
}

fn get_u32(input: &mut &[u8], what: &str) -> Result<u32> {
    u32::try_from(get_uvarint(input)?).map_err(|_| invalid(format!("{what} exceeds u32")))
}

fn get_count(input: &mut &[u8]) -> Result<usize> {
    usize::try_from(get_uvarint(input)?).map_err(|_| invalid("count exceeds usize"))
}

/// Positions as gaps: `pos_i - pos_{i-1} - 1`, with `pos_{-1} = -1`.
fn put_positions(out: &mut Vec<u8>, positions: impl Iterator<Item = u32>) {
    let mut next: u64 = 0;
    for p in positions {
        put_uvarint(out, u64::from(p) - next);
        next = u64::from(p) + 1;
    }
}

fn get_positions(input: &mut &[u8], n: usize, prev_entries: u64) -> Result<Vec<u32>> {
    let mut positions = Vec::with_capacity(n);
    let mut next: u64 = 0;
    for _ in 0..n {
        let p = next
            .checked_add(get_uvarint(input)?)
            .filter(|&p| p < prev_entries)
            .ok_or_else(|| invalid("position out of range"))?;
        positions.push(u32::try_from(p).map_err(|_| invalid("position exceeds u32"))?);
        next = p + 1;
    }
    Ok(positions)
}

// ---------------------------------------------------------------------------
// Encode / decode
// ---------------------------------------------------------------------------

impl Segment {
    /// A segment that changes nothing.
    pub fn empty(prev_state_hash: StateHash, prev_entries: u64) -> Self {
        Self {
            prev_state_hash,
            prev_entries,
            deltas: Vec::new(),
            relocations: Vec::new(),
            new_entries: Vec::new(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.deltas.is_empty() && self.relocations.is_empty() && self.new_entries.is_empty()
    }

    /// Check the structural invariants (see the type docs).
    pub fn validate(&self) -> Result<()> {
        let increasing = |a: u32, b: u32| a < b;
        if !self
            .deltas
            .windows(2)
            .all(|w| increasing(w[0].position, w[1].position))
            || !self
                .relocations
                .windows(2)
                .all(|w| increasing(w[0].position, w[1].position))
        {
            return Err(invalid("positions not strictly increasing"));
        }
        let in_range = |p: u32| u64::from(p) < self.prev_entries;
        if !self.deltas.iter().all(|d| in_range(d.position))
            || !self.relocations.iter().all(|r| in_range(r.position))
        {
            return Err(invalid("position out of range"));
        }
        if self.deltas.iter().any(|d| d.delta == 0) {
            return Err(invalid("zero refcount delta"));
        }
        if !self
            .new_entries
            .windows(2)
            .all(|w| w[0].chunk_id.as_bytes() < w[1].chunk_id.as_bytes())
        {
            return Err(invalid("new entries not strictly increasing"));
        }
        if self.new_entries.iter().any(|e| e.refcount == 0) {
            return Err(invalid("new entry with refcount 0"));
        }
        Ok(())
    }

    pub fn encode(&self) -> Vec<u8> {
        self.encode_with_sizes().0
    }

    /// Encode, also reporting the size of each section.
    ///
    /// Layout: version(1) · prev_state_hash(32) · varints prev_entries,
    /// n_deltas, n_relocations, n_new · delta position gaps · zigzag delta
    /// values · relocation position gaps · relocation pack IDs · relocation
    /// offsets · relocation sizes · new chunk IDs · new refcounts · new sizes
    /// · new pack IDs · new offsets. One column per field keeps similar bytes
    /// together for the compressor.
    pub fn encode_with_sizes(&self) -> (Vec<u8>, SegmentSizes) {
        debug_assert!(self.validate().is_ok(), "encoding an invalid segment");
        self.encode_raw()
    }

    /// Positions must be strictly increasing; nothing else is checked.
    fn encode_raw(&self) -> (Vec<u8>, SegmentSizes) {
        let mut out = Vec::with_capacity(
            64 + self.deltas.len() * 3 + self.relocations.len() * 48 + self.new_entries.len() * 80,
        );
        let mut sizes = SegmentSizes::default();
        let mut mark = 0usize;
        let mut section = |out: &Vec<u8>, slot: &mut usize| {
            *slot = out.len() - mark;
            mark = out.len();
        };

        out.push(SEGMENT_VERSION);
        out.extend_from_slice(&self.prev_state_hash);
        put_uvarint(&mut out, self.prev_entries);
        put_uvarint(&mut out, self.deltas.len() as u64);
        put_uvarint(&mut out, self.relocations.len() as u64);
        put_uvarint(&mut out, self.new_entries.len() as u64);
        section(&out, &mut sizes.header);

        put_positions(&mut out, self.deltas.iter().map(|d| d.position));
        section(&out, &mut sizes.delta_positions);
        for d in &self.deltas {
            put_uvarint(&mut out, zigzag(d.delta));
        }
        section(&out, &mut sizes.delta_values);

        put_positions(&mut out, self.relocations.iter().map(|r| r.position));
        for r in &self.relocations {
            out.extend_from_slice(r.pack_id.as_bytes());
        }
        for r in &self.relocations {
            put_uvarint(&mut out, r.pack_offset);
        }
        for r in &self.relocations {
            put_uvarint(&mut out, u64::from(r.stored_size));
        }
        section(&out, &mut sizes.relocations);

        for e in &self.new_entries {
            out.extend_from_slice(e.chunk_id.as_bytes());
        }
        for e in &self.new_entries {
            put_uvarint(&mut out, u64::from(e.refcount));
        }
        for e in &self.new_entries {
            put_uvarint(&mut out, u64::from(e.stored_size));
        }
        for e in &self.new_entries {
            out.extend_from_slice(e.pack_id.as_bytes());
        }
        for e in &self.new_entries {
            put_uvarint(&mut out, e.pack_offset);
        }
        section(&out, &mut sizes.new_entries);

        (out, sizes)
    }

    /// Decode and validate. Every count is bounded against the remaining
    /// input before anything is allocated for it.
    pub fn decode(bytes: &[u8]) -> Result<Self> {
        let mut input = bytes;
        let version = take(&mut input, 1)?[0];
        if version != SEGMENT_VERSION {
            return Err(VykarError::UnsupportedVersion(u32::from(version)));
        }
        let prev_state_hash: StateHash = take(&mut input, 32)?.try_into().expect("32-byte slice");
        let prev_entries = get_uvarint(&mut input)?;
        let n_deltas = get_count(&mut input)?;
        let n_relocs = get_count(&mut input)?;
        let n_new = get_count(&mut input)?;

        let min_bytes = n_deltas
            .checked_mul(MIN_DELTA_BYTES)
            .and_then(|a| Some((a, n_relocs.checked_mul(MIN_RELOCATION_BYTES)?)))
            .and_then(|(a, b)| a.checked_add(b))
            .and_then(|a| a.checked_add(n_new.checked_mul(MIN_NEW_ENTRY_BYTES)?));
        if min_bytes.is_none_or(|m| m > input.len()) {
            return Err(invalid("counts exceed input length"));
        }

        let delta_positions = get_positions(&mut input, n_deltas, prev_entries)?;
        let mut deltas = Vec::with_capacity(n_deltas);
        for position in delta_positions {
            let delta = unzigzag(get_uvarint(&mut input)?);
            if delta == 0 {
                return Err(invalid("zero refcount delta"));
            }
            deltas.push(RefcountDelta { position, delta });
        }

        let reloc_positions = get_positions(&mut input, n_relocs, prev_entries)?;
        let mut relocations: Vec<Relocation> = reloc_positions
            .into_iter()
            .map(|position| Relocation {
                position,
                pack_id: PackId::from_bytes([0; 32]),
                pack_offset: 0,
                stored_size: 0,
            })
            .collect();
        for r in &mut relocations {
            r.pack_id = PackId::from_bytes(take(&mut input, 32)?.try_into().expect("32 bytes"));
        }
        for r in &mut relocations {
            r.pack_offset = get_uvarint(&mut input)?;
        }
        for r in &mut relocations {
            r.stored_size = get_u32(&mut input, "stored size")?;
        }

        let ids = take(
            &mut input,
            n_new.checked_mul(32).ok_or_else(|| invalid("count"))?,
        )?;
        let mut new_entries: Vec<FullCacheEntry> = ids
            .as_chunks::<32>()
            .0
            .iter()
            .map(|id| FullCacheEntry {
                chunk_id: ChunkId::from_bytes(*id),
                refcount: 0,
                stored_size: 0,
                pack_id: PackId::from_bytes([0; 32]),
                pack_offset: 0,
            })
            .collect();
        for e in &mut new_entries {
            e.refcount = get_u32(&mut input, "refcount")?;
        }
        for e in &mut new_entries {
            e.stored_size = get_u32(&mut input, "stored size")?;
        }
        for e in &mut new_entries {
            e.pack_id = PackId::from_bytes(take(&mut input, 32)?.try_into().expect("32 bytes"));
        }
        for e in &mut new_entries {
            e.pack_offset = get_uvarint(&mut input)?;
        }

        if !input.is_empty() {
            return Err(invalid("trailing bytes"));
        }
        let segment = Self {
            prev_state_hash,
            prev_entries,
            deltas,
            relocations,
            new_entries,
        };
        segment.validate()?;
        Ok(segment)
    }
}

// ---------------------------------------------------------------------------
// Apply: a streaming pipeline of segment layers
// ---------------------------------------------------------------------------

/// One step of a chain: a segment and the state it must produce.
#[derive(Debug, Clone, Copy)]
pub struct ChainStep<'a> {
    pub segment: &'a Segment,
    pub state_hash_after: StateHash,
    pub entries_after: u64,
}

/// Per-layer cursor over its segment, plus the hash of what it emitted.
struct Layer<'a> {
    segment: &'a Segment,
    /// Predecessor position of the next input entry.
    in_pos: u64,
    last_in: Option<ChunkId>,
    next_delta: usize,
    next_reloc: usize,
    /// Next new entry to emit.
    next_new: usize,
    /// Cursor for the "new entry duplicates a predecessor entry" check.
    dup_check: usize,
    /// Next surviving input entry, already transformed.
    pending: Option<FullCacheEntry>,
    input_done: bool,
    hasher: blake3::Hasher,
    emitted: u64,
}

impl<'a> Layer<'a> {
    fn new(segment: &'a Segment) -> Self {
        Self {
            segment,
            in_pos: 0,
            last_in: None,
            next_delta: 0,
            next_reloc: 0,
            next_new: 0,
            dup_check: 0,
            pending: None,
            input_done: false,
            hasher: blake3::Hasher::new(),
            emitted: 0,
        }
    }

    /// Apply this layer's ops to the input entry at `self.in_pos`. Returns
    /// `None` when the entry is removed.
    fn transform(&mut self, mut entry: FullCacheEntry) -> Result<Option<FullCacheEntry>> {
        if self.in_pos >= self.segment.prev_entries {
            return Err(invalid("predecessor has more entries than recorded"));
        }
        if self
            .last_in
            .is_some_and(|last| last.as_bytes() >= entry.chunk_id.as_bytes())
        {
            return Err(invalid("predecessor not strictly sorted"));
        }
        self.last_in = Some(entry.chunk_id);

        let new = &self.segment.new_entries;
        while self.dup_check < new.len()
            && new[self.dup_check].chunk_id.as_bytes() < entry.chunk_id.as_bytes()
        {
            self.dup_check += 1;
        }
        if new
            .get(self.dup_check)
            .is_some_and(|n| n.chunk_id == entry.chunk_id)
        {
            return Err(invalid("new entry already in predecessor"));
        }

        let pos = self.in_pos;
        self.in_pos += 1;
        let removed = match self.segment.deltas.get(self.next_delta) {
            Some(d) if u64::from(d.position) == pos => {
                self.next_delta += 1;
                entry.refcount = i64::from(entry.refcount)
                    .checked_add(d.delta)
                    .and_then(|rc| u32::try_from(rc).ok())
                    .ok_or_else(|| invalid("refcount out of range"))?;
                entry.refcount == 0
            }
            // An entry no delta touches is kept as is, even at refcount 0.
            _ => false,
        };
        match self.segment.relocations.get(self.next_reloc) {
            Some(r) if u64::from(r.position) == pos => {
                if removed {
                    return Err(invalid("relocation of a removed entry"));
                }
                self.next_reloc += 1;
                entry.pack_id = r.pack_id;
                entry.pack_offset = r.pack_offset;
                entry.stored_size = r.stored_size;
            }
            _ => {}
        }
        Ok((!removed).then_some(entry))
    }

    /// Check that the layer consumed exactly its predecessor.
    fn check_consumed(&self) -> Result<()> {
        if self.in_pos != self.segment.prev_entries {
            return Err(invalid("predecessor has fewer entries than recorded"));
        }
        debug_assert_eq!(self.next_delta, self.segment.deltas.len());
        debug_assert_eq!(self.next_reloc, self.segment.relocations.len());
        Ok(())
    }

    /// Check that the layer consumed exactly its predecessor and produced
    /// the expected state.
    fn finish(self, expected: &ChainStep<'_>) -> Result<()> {
        self.check_consumed()?;
        if self.emitted != expected.entries_after {
            return Err(invalid(format!(
                "produced {} entries, expected {}",
                self.emitted, expected.entries_after
            )));
        }
        let hash: StateHash = self.hasher.finalize().into();
        if hash != expected.state_hash_after {
            return Err(invalid("resulting state hash mismatch"));
        }
        Ok(())
    }
}

/// Source of the bottom layer's input: the predecessor cache.
struct BaseSource<'a> {
    cache: &'a MmapFullIndexCache,
    next: usize,
}

/// Next output entry of the top layer of `layers`, pulling input from the
/// layer below (or `base`). Recursion depth is the chain length.
fn pull(layers: &mut [Layer<'_>], base: &mut BaseSource<'_>) -> Result<Option<FullCacheEntry>> {
    let Some((top, below)) = layers.split_last_mut() else {
        if base.next >= base.cache.entry_count() as usize {
            return Ok(None);
        }
        base.next += 1;
        return Ok(Some(base.cache.entry_at(base.next - 1)));
    };

    while top.pending.is_none() && !top.input_done {
        match pull(below, base)? {
            Some(entry) => top.pending = top.transform(entry)?,
            None => top.input_done = true,
        }
    }

    let new = top.segment.new_entries.get(top.next_new);
    let out = match (top.pending, new) {
        (None, None) => return Ok(None),
        (Some(p), None) => {
            top.pending = None;
            p
        }
        (None, Some(n)) => {
            top.next_new += 1;
            *n
        }
        (Some(p), Some(n)) => match p.chunk_id.as_bytes().cmp(n.chunk_id.as_bytes()) {
            std::cmp::Ordering::Less => {
                top.pending = None;
                p
            }
            std::cmp::Ordering::Greater => {
                top.next_new += 1;
                *n
            }
            std::cmp::Ordering::Equal => return Err(invalid("new entry already in predecessor")),
        },
    };
    top.hasher
        .update(&super::dedup_cache::encode_full_entry(&out));
    top.emitted += 1;
    Ok(Some(out))
}

/// Apply `chain` to `prev` in one streaming pass, writing the result as a
/// full index cache into a temp file beside `target` (generation derived
/// from the final state hash).
///
/// Each step's segment must name the state before it (`prev`'s, then the
/// previous step's `state_hash_after`), and each layer's output is checked
/// against its step's `state_hash_after` and `entries_after`. One read of
/// `prev` and one write of the result however long the chain is; memory is
/// the segments themselves.
pub fn apply_to_temp(
    prev: &MmapFullIndexCache,
    chain: &[ChainStep<'_>],
    target: &Path,
) -> Result<(TempPath, StateHash, u64)> {
    let mut expected_hash = prev.state_hash();
    let mut expected_entries = u64::from(prev.entry_count());
    for step in chain {
        if step.segment.prev_state_hash != expected_hash
            || step.segment.prev_entries != expected_entries
        {
            return Err(invalid("segment does not follow the preceding state"));
        }
        expected_hash = step.state_hash_after;
        expected_entries = step.entries_after;
    }

    let mut layers: Vec<Layer<'_>> = chain.iter().map(|s| Layer::new(s.segment)).collect();
    let mut base = BaseSource {
        cache: prev,
        next: 0,
    };
    let mut w = FullCacheWriter::new_beside(target)?;
    while let Some(entry) = pull(&mut layers, &mut base)? {
        w.push(&entry)?;
    }
    if layers.is_empty() {
        // Nothing to check per layer; the copy must still be exact.
        let (tmp, hash, count) = w.finish(None)?;
        if hash != prev.state_hash() {
            return Err(invalid("copy of predecessor changed its state"));
        }
        return Ok((tmp.into_temp_path(), hash, count));
    }
    for (layer, step) in layers.into_iter().zip(chain) {
        layer.finish(step)?;
    }
    let (tmp, hash, count) = w.finish(None)?;
    debug_assert_eq!(hash, expected_hash);
    debug_assert_eq!(count, expected_entries);
    Ok((tmp.into_temp_path(), hash, count))
}

/// [`apply_to_temp`], persisted over `target`. Returns the new state hash.
pub fn apply(
    prev: &MmapFullIndexCache,
    chain: &[ChainStep<'_>],
    target: &Path,
) -> Result<StateHash> {
    let (tmp, hash, _) = apply_to_temp(prev, chain, target)?;
    tmp.persist(target).map_err(|e| e.error)?;
    Ok(hash)
}

// ---------------------------------------------------------------------------
// Diff: sorted predecessor cache vs in-memory ChunkIndex
// ---------------------------------------------------------------------------

/// A segment together with the state it produces.
#[derive(Debug, Clone)]
pub struct Diff {
    pub segment: Segment,
    pub state_hash_after: StateHash,
    pub entries_after: u64,
}

impl Diff {
    pub fn step(&self) -> ChainStep<'_> {
        ChainStep {
            segment: &self.segment,
            state_hash_after: self.state_hash_after,
            entries_after: self.entries_after,
        }
    }
}

/// The segment that turns `prev` into `next`, and `next` written as a full
/// index cache into a temp file beside `target`.
///
/// Sorts `next` by chunk ID (a vector of 24-byte references, ~180 MB at 7.4M
/// entries), then merge-walks it against `prev` in one pass, writing the
/// output in the same pass. New entries come out of the walk in order. On
/// a 7.4M-entry synthetic index this took 2–3.5 s, against 3.4 s for a
/// per-entry hash lookup without new entries and 9.4 s with them (which
/// then needed a binary search per map entry and a second pass).
///
/// Removal is a delta to 0, so refcount 0 is not a state a segment can
/// produce: entries of `next` at refcount 0 are treated as absent, unless
/// `prev` holds them at 0 too (then they are kept). A `prev` entry at
/// refcount 0 that `next` lacks cannot be removed and is an error.
pub fn diff_to_temp(
    prev: &MmapFullIndexCache,
    next: &ChunkIndex,
    target: &Path,
) -> Result<(Diff, TempPath)> {
    // Big-endian prefix: integer order equals the byte order of the first 8
    // bytes, so the sort compares u64s and touches the map only on ties.
    let mut sorted: Vec<(u64, &ChunkId, &super::ChunkIndexEntry)> = next
        .iter()
        .map(|(id, e)| {
            let prefix = u64::from_be_bytes(id.as_bytes()[..8].try_into().expect("8 bytes"));
            (prefix, id, e)
        })
        .collect();
    sorted.sort_unstable_by(|a, b| {
        a.0.cmp(&b.0)
            .then_with(|| a.1.as_bytes().cmp(b.1.as_bytes()))
    });

    let mut deltas = Vec::new();
    let mut relocations = Vec::new();
    let mut new_entries = Vec::new();
    let mut w = FullCacheWriter::new_beside(target)?;
    let mut last: Option<ChunkId> = None;
    let mut j = 0usize;

    let record = |id: &ChunkId, e: &super::ChunkIndexEntry| FullCacheEntry {
        chunk_id: *id,
        refcount: e.refcount,
        stored_size: e.stored_size,
        pack_id: e.pack_id,
        pack_offset: e.pack_offset,
    };

    for (i, old) in prev.iter().enumerate() {
        if last.is_some_and(|l| l.as_bytes() >= old.chunk_id.as_bytes()) {
            return Err(invalid("predecessor not strictly sorted"));
        }
        last = Some(old.chunk_id);
        while let Some(&(_, id, e)) = sorted.get(j) {
            if id.as_bytes() >= old.chunk_id.as_bytes() {
                break;
            }
            j += 1;
            if e.refcount > 0 {
                let entry = record(id, e);
                w.push(&entry)?;
                new_entries.push(entry);
            }
        }
        let position = i as u32;
        let cur = match sorted.get(j) {
            Some(&(_, id, e)) if *id == old.chunk_id => {
                j += 1;
                Some(e)
            }
            _ => None,
        };
        match cur.filter(|e| e.refcount > 0 || old.refcount == 0) {
            None => {
                if old.refcount == 0 {
                    return Err(invalid(
                        "predecessor entry with refcount 0 cannot be removed",
                    ));
                }
                deltas.push(RefcountDelta {
                    position,
                    delta: -i64::from(old.refcount),
                });
            }
            Some(e) => {
                if e.refcount != old.refcount {
                    deltas.push(RefcountDelta {
                        position,
                        delta: i64::from(e.refcount) - i64::from(old.refcount),
                    });
                }
                if e.pack_id != old.pack_id
                    || e.pack_offset != old.pack_offset
                    || e.stored_size != old.stored_size
                {
                    relocations.push(Relocation {
                        position,
                        pack_id: e.pack_id,
                        pack_offset: e.pack_offset,
                        stored_size: e.stored_size,
                    });
                }
                w.push(&record(&old.chunk_id, e))?;
            }
        }
    }
    for &(_, id, e) in &sorted[j..] {
        if e.refcount > 0 {
            let entry = record(id, e);
            w.push(&entry)?;
            new_entries.push(entry);
        }
    }

    let (tmp, state_hash_after, entries_after) = w.finish(None)?;
    let diff = Diff {
        segment: Segment {
            prev_state_hash: prev.state_hash(),
            prev_entries: u64::from(prev.entry_count()),
            deltas,
            relocations,
            new_entries,
        },
        state_hash_after,
        entries_after,
    };
    Ok((diff, tmp.into_temp_path()))
}

// ---------------------------------------------------------------------------
// Merge: sorted predecessor cache + a backup session's IndexDelta
// ---------------------------------------------------------------------------

/// Sum-merge of two bump streams, each in chunk-ID order.
struct MergedBumps<A: Iterator<Item = (ChunkId, u64)>, B: Iterator<Item = (ChunkId, u64)>> {
    a: std::iter::Peekable<A>,
    b: std::iter::Peekable<B>,
}

impl<A, B> Iterator for MergedBumps<A, B>
where
    A: Iterator<Item = (ChunkId, u64)>,
    B: Iterator<Item = (ChunkId, u64)>,
{
    type Item = (ChunkId, u64);

    fn next(&mut self) -> Option<Self::Item> {
        use std::cmp::Ordering;
        let order = match (self.a.peek(), self.b.peek()) {
            (None, None) => return None,
            (Some(_), None) => Ordering::Less,
            (None, Some(_)) => Ordering::Greater,
            (Some(x), Some(y)) => x.0.as_bytes().cmp(y.0.as_bytes()),
        };
        match order {
            Ordering::Less => self.a.next(),
            Ordering::Greater => self.b.next(),
            Ordering::Equal => {
                let (id, x) = self.a.next()?;
                let (_, y) = self.b.next()?;
                Some((id, x + y))
            }
        }
    }
}

/// A session's new entries in chunk-ID order, one per chunk at the first
/// one's location, each with refcount 1: counts are not maintained (05 step
/// 8), and v1 requires at least 1.
fn sorted_new_entries(entries: &[super::NewChunkEntry]) -> Vec<FullCacheEntry> {
    let mut v: Vec<FullCacheEntry> = entries
        .iter()
        .map(|e| FullCacheEntry {
            chunk_id: e.chunk_id,
            refcount: 1,
            stored_size: e.stored_size,
            pack_id: e.pack_id,
            pack_offset: e.pack_offset,
        })
        .collect();
    // Stable: the first location of a chunk stays first.
    v.sort_by(|a, b| a.chunk_id.as_bytes().cmp(b.chunk_id.as_bytes()));
    v.dedup_by(|later, kept| later.chunk_id == kept.chunk_id);
    v
}

/// The segment that applies a backup session's `delta` to `prev`, and the
/// resulting state written as a full index cache into a temp file beside
/// `target`.
///
/// One merge-walk over four sorted inputs: `prev`, the positional bumps (in
/// the session's table order, which is chunk-ID order), the keyed bumps, and
/// the new entries. No map of bumps is built, so `prev` may be a later
/// state than the one the session started from. Refcounts are not
/// persisted (05 step 8): the bumps are the session's reused set, and they
/// are only checked. The segment holds new entries only:
///
/// - a new entry already in `prev` (another client committed the chunk
///   first) writes nothing: the existing entry stays, and this session's
///   copy of the bytes is dead,
/// - every other new entry is written with refcount 1,
/// - a bump whose chunk is in neither `prev` nor the new entries is
///   [`VykarError::StaleChunksDuringCommit`].
///
/// Should the positional bumps turn out not to be in order (a damaged local
/// table), all bumps are collected and sorted instead.
pub fn merge_delta_to_temp(
    prev: &MmapFullIndexCache,
    delta: &super::IndexDelta,
    target: &Path,
) -> Result<(Diff, TempPath)> {
    let news = sorted_new_entries(&delta.new_entries);
    let mut keyed: Vec<(ChunkId, u64)> = delta
        .refcount_bumps
        .iter()
        .filter(|&(_, &count)| count > 0)
        .map(|(id, &count)| (*id, u64::from(count)))
        .collect();
    keyed.sort_unstable_by(|a, b| a.0.as_bytes().cmp(b.0.as_bytes()));
    let cached = || {
        delta
            .cached_bumps
            .iter()
            .flat_map(|c| c.iter_nonzero())
            .map(|(id, count)| (id, u64::from(count)))
    };

    let merged = MergedBumps {
        a: cached().peekable(),
        b: keyed.iter().copied().peekable(),
    };
    if let Some(result) = merge_walk(prev, merged, &news, target)? {
        return Ok(result);
    }

    tracing::debug!("positional bumps out of chunk-ID order, sorting all bumps");
    let mut all: Vec<(ChunkId, u64)> = cached().chain(keyed).collect();
    all.sort_unstable_by(|a, b| a.0.as_bytes().cmp(b.0.as_bytes()));
    all.dedup_by(|later, kept| {
        let same = later.0 == kept.0;
        if same {
            kept.1 += later.1;
        }
        same
    });
    merge_walk(prev, all.into_iter(), &news, target)?
        .ok_or_else(|| invalid("bumps out of order after sorting"))
}

/// The next record of [`merge_walk`]: a predecessor entry (with the new
/// entry for the same chunk, if any), or a new entry alone.
enum MergeStep<'a> {
    Old(FullCacheEntry, Option<&'a FullCacheEntry>),
    New(&'a FullCacheEntry),
}

/// One pass of [`merge_delta_to_temp`]. `Ok(None)` (temp file deleted) when
/// `bumps` is not strictly increasing.
fn merge_walk(
    prev: &MmapFullIndexCache,
    bumps: impl Iterator<Item = (ChunkId, u64)>,
    news: &[FullCacheEntry],
    target: &Path,
) -> Result<Option<(Diff, TempPath)>> {
    use std::cmp::Ordering;

    let mut bumps = bumps.peekable();
    let mut last_bump: Option<ChunkId> = None;
    let mut last_old: Option<ChunkId> = None;
    let mut new_entries = Vec::new();
    let mut writer = FullCacheWriter::new_beside(target)?;
    let prev_count = prev.entry_count() as usize;
    let mut pos = 0usize;
    let mut next_new = 0usize;

    loop {
        let old = (pos < prev_count).then(|| prev.entry_at(pos));
        let step = match (old, news.get(next_new)) {
            (None, None) => break,
            (Some(o), None) => MergeStep::Old(o, None),
            (None, Some(n)) => MergeStep::New(n),
            (Some(o), Some(n)) => match o.chunk_id.as_bytes().cmp(n.chunk_id.as_bytes()) {
                Ordering::Less => MergeStep::Old(o, None),
                Ordering::Greater => MergeStep::New(n),
                Ordering::Equal => MergeStep::Old(o, Some(n)),
            },
        };
        let key = match &step {
            MergeStep::Old(o, _) => o.chunk_id,
            MergeStep::New(n) => n.chunk_id,
        };

        // Bumps arrive in key order, so one below `key` targets a chunk in
        // neither input. A matching bump is only consumed: reuse is checked,
        // not counted.
        match bumps.peek() {
            Some(&(id, _)) if last_bump.is_some_and(|l| l.as_bytes() >= id.as_bytes()) => {
                return Ok(None);
            }
            Some(&(id, _)) => match id.as_bytes().cmp(key.as_bytes()) {
                Ordering::Less => return Err(VykarError::StaleChunksDuringCommit),
                Ordering::Equal => {
                    last_bump = Some(id);
                    bumps.next();
                }
                Ordering::Greater => {}
            },
            None => {}
        }

        match step {
            MergeStep::Old(entry, same_new) => {
                if last_old.is_some_and(|l| l.as_bytes() >= entry.chunk_id.as_bytes()) {
                    return Err(invalid("predecessor not strictly sorted"));
                }
                last_old = Some(entry.chunk_id);
                // A new entry another client committed first: the existing
                // entry stays as it is.
                writer.push(&entry)?;
                pos += 1;
                if same_new.is_some() {
                    next_new += 1;
                }
            }
            MergeStep::New(new) => {
                writer.push(new)?;
                new_entries.push(*new);
                next_new += 1;
            }
        }
    }
    if let Some(&(id, _)) = bumps.peek() {
        if last_bump.is_some_and(|l| l.as_bytes() >= id.as_bytes()) {
            return Ok(None);
        }
        return Err(VykarError::StaleChunksDuringCommit);
    }

    let (tmp, state_hash_after, entries_after) = writer.finish(None)?;
    let diff = Diff {
        segment: Segment {
            prev_state_hash: prev.state_hash(),
            prev_entries: u64::from(prev.entry_count()),
            deltas: Vec::new(),
            relocations: Vec::new(),
            new_entries,
        },
        state_hash_after,
        entries_after,
    };
    Ok(Some((diff, tmp.into_temp_path())))
}

#[cfg(any(test, feature = "fuzzing"))]
#[doc(hidden)]
pub mod model;

#[cfg(test)]
mod tests;
