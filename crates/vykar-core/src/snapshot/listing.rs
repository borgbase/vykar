//! Key index of a snapshot listing (06 step 4), stored in
//! [`SnapshotMeta::ext`](super::SnapshotMeta::ext).
//!
//! The listing (the item stream) is written in **order 1** ("dfs-name"): the
//! entries of each listing segment sort by [`dfs_name_cmp`] of their
//! segment-relative paths. A segment is one resolved source root, or the
//! command dumps; each begins at a tree-chunk boundary. The key index
//! records the segments and one separator key per `stride`-th tree chunk,
//! so a reader can find the tree chunks that hold one directory without
//! decoding the whole listing (`commands::listing`).
//!
//! # Format
//!
//! msgpack, positional, every byte string a msgpack bin:
//!
//! ```text
//! SnapshotExt  = [ext_version: u32 = 1, listing: ListingIndex, ...ignored trailing elements]
//! ListingIndex = [order: u8, segments: [Segment], stride: u32, keys: [bin]]
//! Segment      = [root: bin, first_ptr: u32]
//! ```
//!
//! The frame is frozen: `ext` is a msgpack array whose first element is a
//! u32 version. A reader ignores a version it does not know only when that
//! frame holds; anything else does not decode (and `check` warns).
//! Trailing elements after `listing` are ignored, so whatever goes there
//! must be advisory too. `ListingIndex` and `Segment` are strict.
//!
//! - **order 1** promises: within each segment every entry `p`
//!   (`Item::path_bytes()`) is the root itself or under `root + "/"` (an
//!   empty root contains everything), has no empty component, and the
//!   entries' segment-relative paths are non-decreasing under
//!   [`dfs_name_cmp`]. Equal consecutive paths are allowed. **order 0**:
//!   unknown, no segments and no keys; readers scan.
//! - Segment `s` covers the tree chunks `[first_ptr_s, first_ptr_{s+1})`,
//!   the last one up to `item_ptrs.len()`. `segments[0].first_ptr == 0`,
//!   `first_ptr` strictly increasing, segments in emission order. Roots
//!   without entries are not recorded.
//! - `keys[j]` belongs to tree chunk `c = j * stride`: empty when `c` is
//!   the first chunk of its segment, else the shortest byte prefix of the
//!   relative path of `c`'s first entry that does not sort before the
//!   relative path of the last entry of chunk `c - 1`. So
//!   `last(c - 1) <= keys[j] <= first(c)`.
//!
//! Absent, undecodable, of an unknown version or order, or failing the
//! structural checks of [`decode_ext`]: no index, and readers scan.
//!
//! # Trust
//!
//! The writer derives the index from the paths it writes and writes order 0
//! on the first entry that breaks the promise; nobody declares it. A reader
//! verifies every chunk it decodes (order, containment, the key and the
//! predecessor chunk) and scans on any failure. It trusts, without reading
//! them, the order of the chunks it skips and the segment table (roots and
//! `first_ptr`, which decide which segments are read at all). A full
//! `check` verifies both for every chunk (`check_ext`).
//!
//! The index describes one exact `item_ptrs`. Code that rewrites a
//! snapshot's `item_ptrs` must replace `ext` or set it to `None`: an index
//! copied onto different tree chunks can pass the structural checks, and a
//! wrong segment table can make a read miss entries.

use std::cmp::Ordering;

use serde::de::{self, IgnoredAny, SeqAccess, Visitor};
use serde::{Deserialize, Deserializer, Serialize};
use serde_bytes::ByteBuf;
use vykar_types::error::Result;

/// Version of the `SnapshotExt` blob (independent of
/// `SnapshotMeta.format_version`).
pub const EXT_VERSION: u32 = 1;
/// The listing's order is not known: readers scan.
pub const ORDER_UNKNOWN: u8 = 0;
/// Order 1, "dfs-name" (see the module doc).
pub const ORDER_DFS_NAME: u8 = 1;
/// One key per this many tree chunks.
pub const STRIDE: u32 = 32;
/// Largest stride a reader accepts.
const MAX_STRIDE: u32 = 65_536;

/// One listing segment: a source root, or the command dumps.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Segment {
    /// Snapshot path every entry of the segment equals or starts with + "/";
    /// empty for a root whose entries are not prefixed.
    pub root: ByteBuf,
    /// Index into `item_ptrs` of the segment's first tree chunk.
    pub first_ptr: u32,
}

/// The key index of one listing.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ListingIndex {
    /// [`ORDER_UNKNOWN`] or [`ORDER_DFS_NAME`].
    pub order: u8,
    pub segments: Vec<Segment>,
    pub stride: u32,
    /// One per `stride`-th tree chunk (see the module doc).
    pub keys: Vec<ByteBuf>,
}

/// Why a snapshot has no usable index. [`NoIndex::warns`] is true for the
/// kinds `check` reports (`Undecodable`, `Invalid`); the others are
/// legitimate.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NoIndex {
    /// `ext` is absent (snapshots written before the index existed).
    Absent,
    /// An `ext` version this binary does not know.
    UnknownVersion(u32),
    /// `ext` does not decode.
    Undecodable(String),
    /// Order 0: the writer saw the listing break order 1.
    Unordered,
    /// An order this binary does not know.
    UnknownOrder(u8),
    /// The index decodes but its structure is invalid.
    Invalid(&'static str),
}

impl NoIndex {
    /// Whether `check` reports this kind as a mismatch.
    pub fn warns(&self) -> bool {
        matches!(self, NoIndex::Undecodable(_) | NoIndex::Invalid(_))
    }
}

impl std::fmt::Display for NoIndex {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            NoIndex::Absent => write!(f, "the snapshot has no key index"),
            NoIndex::UnknownVersion(v) => write!(f, "unknown key index version {v}"),
            NoIndex::Undecodable(e) => write!(f, "the key index does not decode: {e}"),
            NoIndex::Unordered => write!(f, "the listing is not in name order"),
            NoIndex::UnknownOrder(o) => write!(f, "unknown listing order {o}"),
            NoIndex::Invalid(why) => write!(f, "invalid key index: {why}"),
        }
    }
}

/// Encode `index` as `SnapshotExt` version 1.
pub fn encode_ext(index: &ListingIndex) -> Result<Vec<u8>> {
    Ok(rmp_serde::to_vec(&(EXT_VERSION, index))?)
}

/// A decoded `SnapshotExt` frame.
enum Decoded {
    V1(ListingIndex),
    Other(u32),
}

impl<'de> Deserialize<'de> for Decoded {
    fn deserialize<D: Deserializer<'de>>(d: D) -> std::result::Result<Self, D::Error> {
        struct V;
        impl<'de> Visitor<'de> for V {
            type Value = Decoded;
            fn expecting(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                f.write_str("a snapshot ext array [version, ...]")
            }
            fn visit_seq<A: SeqAccess<'de>>(
                self,
                mut seq: A,
            ) -> std::result::Result<Decoded, A::Error> {
                let version: u32 = seq
                    .next_element()?
                    .ok_or_else(|| de::Error::invalid_length(0, &self))?;
                let out = if version == EXT_VERSION {
                    let index: ListingIndex = seq
                        .next_element()?
                        .ok_or_else(|| de::Error::invalid_length(1, &self))?;
                    Decoded::V1(index)
                } else {
                    Decoded::Other(version)
                };
                // rmp-serde rejects a visitor that leaves elements unread.
                while seq.next_element::<IgnoredAny>()?.is_some() {}
                Ok(out)
            }
        }
        d.deserialize_seq(V)
    }
}

/// Decode and validate `ext` for a listing of `n_ptrs` tree chunks. `Ok`
/// only for order 1 with a valid structure. Bounded: work is linear in the
/// length of `ext`, and nothing is allocated from a length the input does
/// not back.
pub fn decode_ext(ext: Option<&[u8]>, n_ptrs: usize) -> std::result::Result<ListingIndex, NoIndex> {
    let bytes = ext.ok_or(NoIndex::Absent)?;
    let index = match rmp_serde::from_slice::<Decoded>(bytes) {
        Ok(Decoded::V1(index)) => index,
        Ok(Decoded::Other(v)) => return Err(NoIndex::UnknownVersion(v)),
        Err(e) => return Err(NoIndex::Undecodable(e.to_string())),
    };
    match index.order {
        ORDER_DFS_NAME => {}
        ORDER_UNKNOWN => return Err(NoIndex::Unordered),
        other => return Err(NoIndex::UnknownOrder(other)),
    }
    validate(&index, n_ptrs).map_err(NoIndex::Invalid)?;
    Ok(index)
}

/// The structural rules of an order-1 index over `n_ptrs` tree chunks.
fn validate(index: &ListingIndex, n_ptrs: usize) -> std::result::Result<(), &'static str> {
    if index.stride == 0 || index.stride > MAX_STRIDE {
        return Err("stride out of range");
    }
    if index.keys.len() != n_ptrs.div_ceil(index.stride as usize) {
        return Err("wrong number of keys");
    }
    if index.segments.is_empty() != (n_ptrs == 0) {
        return Err("segments do not match the listing");
    }
    if index.segments.first().is_some_and(|s| s.first_ptr != 0) {
        return Err("the first segment does not start at chunk 0");
    }
    let mut prev: Option<u32> = None;
    for s in &index.segments {
        if prev.is_some_and(|p| s.first_ptr <= p) {
            return Err("segment starts not increasing");
        }
        if s.first_ptr as usize >= n_ptrs {
            return Err("segment starts past the listing");
        }
        if !s.root.is_empty() && !is_clean(&s.root) {
            return Err("segment root has an empty component");
        }
        prev = Some(s.first_ptr);
    }
    Ok(())
}

/// `p` split at its last "/": (parent, name). No "/": no parent.
fn split_parent(p: &[u8]) -> (Option<&[u8]>, &[u8]) {
    match p.iter().rposition(|b| *b == b'/') {
        Some(i) => (p.get(..i), p.get(i + 1..).unwrap_or_default()),
        None => (None, p),
    }
}

/// Order 1 over path bytes: by the components of the parent (compared
/// element by element as bytes, a list before its extensions), then by the
/// name as bytes. Defined on any byte string. Does not allocate.
pub fn dfs_name_cmp(a: &[u8], b: &[u8]) -> Ordering {
    let (pa, na) = split_parent(a);
    let (pb, nb) = split_parent(b);
    if pa == pb {
        return na.cmp(nb);
    }
    let ca = pa.into_iter().flat_map(|x| x.split(|c| *c == b'/'));
    let cb = pb.into_iter().flat_map(|x| x.split(|c| *c == b'/'));
    ca.cmp(cb).then_with(|| na.cmp(nb))
}

/// Non-empty, without an empty component (no leading or trailing "/", no
/// "//").
pub(crate) fn is_clean(path: &[u8]) -> bool {
    !path.is_empty() && path.split(|b| *b == b'/').all(|c| !c.is_empty())
}

/// `path` relative to segment root `root`: `path` for an empty root, ""
/// for the root itself, the rest after `root + "/"`; `None` outside it.
pub(crate) fn rel_in<'p>(root: &[u8], path: &'p [u8]) -> Option<&'p [u8]> {
    if root.is_empty() {
        return Some(path);
    }
    let rest = path.strip_prefix(root)?;
    if rest.is_empty() {
        return Some(rest);
    }
    rest.strip_prefix(b"/")
}

/// The parent of `path`: everything before its last "/", "" without one.
pub(crate) fn parent_of(path: &[u8]) -> &[u8] {
    split_parent(path).0.unwrap_or_default()
}

/// The shortest byte prefix of `first` that does not sort before
/// `prev_last`; `first` itself when even it does (an out-of-order pair,
/// which the builder never asks for).
pub(crate) fn separator(prev_last: &[u8], first: &[u8]) -> Vec<u8> {
    // Monotone in n: a byte prefix never sorts after the string it is a
    // prefix of, so the search is valid.
    let less =
        |n: usize| dfs_name_cmp(first.get(..n).unwrap_or(first), prev_last) == Ordering::Less;
    if less(first.len()) {
        return first.to_vec();
    }
    let (mut lo, mut hi) = (0usize, first.len());
    while lo < hi {
        let mid = lo + (hi - lo) / 2;
        if less(mid) {
            lo = mid + 1;
        } else {
            hi = mid;
        }
    }
    first.get(..lo).unwrap_or(first).to_vec()
}

/// Builds the key index from the entries as the writer appends them, and
/// checks order 1 on the way: the first violation makes the result order 0.
#[derive(Debug)]
pub(crate) struct ListingIndexBuilder {
    ok: bool,
    stride: u32,
    /// Root of a segment announced but not yet started.
    pending: Option<Vec<u8>>,
    segments: Vec<Segment>,
    /// Root of the current segment.
    root: Vec<u8>,
    /// Relative path of the previous entry of the current segment.
    prev: Vec<u8>,
    has_prev: bool,
    keys: Vec<ByteBuf>,
}

impl ListingIndexBuilder {
    pub(crate) fn new() -> Self {
        Self::with_stride(STRIDE)
    }

    pub(crate) fn with_stride(stride: u32) -> Self {
        Self {
            ok: true,
            stride: stride.max(1),
            pending: None,
            segments: Vec::new(),
            root: Vec::new(),
            prev: Vec::new(),
            has_prev: false,
            keys: Vec::new(),
        }
    }

    /// The next observed entry starts a segment rooted at `root`. The caller
    /// has closed the open chunk (`ListingWriter::begin_segment` does). A
    /// pending root is replaced: an empty segment is never recorded.
    pub(crate) fn begin_segment(&mut self, root: Vec<u8>) {
        self.pending = Some(root);
    }

    /// `path`: `Item::path_bytes()` of the entry about to be appended to
    /// tree chunk number `chunk` (`item_ptrs.len()` at that time);
    /// `first_in_chunk`: the open chunk was empty before this entry.
    pub(crate) fn observe(&mut self, path: &[u8], chunk: usize, first_in_chunk: bool) {
        if !self.ok {
            return;
        }
        if let Some(root) = self.pending.take() {
            let Ok(first_ptr) = u32::try_from(chunk) else {
                self.ok = false;
                return;
            };
            if !first_in_chunk || !(root.is_empty() || is_clean(&root)) {
                self.ok = false;
                return;
            }
            self.segments.push(Segment {
                root: ByteBuf::from(root.clone()),
                first_ptr,
            });
            self.root = root;
            self.has_prev = false;
        } else if self.segments.is_empty() {
            // No begin_segment: one implicit segment, rooted at "".
            if chunk != 0 || !first_in_chunk {
                self.ok = false;
                return;
            }
            self.segments.push(Segment {
                root: ByteBuf::new(),
                first_ptr: 0,
            });
            self.root.clear();
            self.has_prev = false;
        }
        if !is_clean(path) {
            self.ok = false;
            return;
        }
        let Some(rel) = rel_in(&self.root, path) else {
            self.ok = false;
            return;
        };
        if self.has_prev && dfs_name_cmp(rel, &self.prev) == Ordering::Less {
            self.ok = false;
            return;
        }
        let stride = self.stride as usize;
        if first_in_chunk && chunk.is_multiple_of(stride) {
            if self.keys.len() != chunk / stride {
                self.ok = false;
                return;
            }
            let key = if self.has_prev {
                separator(&self.prev, rel)
            } else {
                Vec::new()
            };
            self.keys.push(ByteBuf::from(key));
        }
        self.prev.clear();
        self.prev.extend_from_slice(rel);
        self.has_prev = true;
    }

    /// The index of a listing of `n_ptrs` tree chunks: order 1 when every
    /// entry kept the promise, order 0 otherwise.
    pub(crate) fn finish(self, n_ptrs: usize) -> ListingIndex {
        let stride = self.stride;
        if self.ok
            && self.keys.len() == n_ptrs.div_ceil(stride as usize)
            && (n_ptrs == 0) == self.segments.is_empty()
        {
            ListingIndex {
                order: ORDER_DFS_NAME,
                segments: self.segments,
                stride,
                keys: self.keys,
            }
        } else {
            ListingIndex {
                order: ORDER_UNKNOWN,
                segments: Vec::new(),
                stride,
                keys: Vec::new(),
            }
        }
    }
}

/// What `check` keeps of one tree chunk's paths to validate a key index.
#[derive(Debug, Clone, Default)]
pub(crate) struct ChunkPaths {
    first: Vec<u8>,
    last: Vec<u8>,
    /// Longest common prefix of every path, ending at a component boundary.
    lcp: Vec<u8>,
    items: usize,
    /// Non-decreasing under [`dfs_name_cmp`].
    ordered: bool,
    /// Every path [`is_clean`].
    clean: bool,
}

impl ChunkPaths {
    /// Add the next entry of the chunk.
    pub(crate) fn observe(&mut self, path: &[u8]) {
        if self.items == 0 {
            self.first = path.to_vec();
            self.last = path.to_vec();
            self.lcp = path.to_vec();
            self.ordered = true;
            self.clean = is_clean(path);
        } else {
            self.ordered &= dfs_name_cmp(path, &self.last) != Ordering::Less;
            self.clean &= is_clean(path);
            let n = component_lcp(&self.lcp, path);
            self.lcp.truncate(n);
            self.last.clear();
            self.last.extend_from_slice(path);
        }
        self.items += 1;
    }

    /// Every entry equals `root` or lies under it.
    fn inside(&self, root: &[u8]) -> bool {
        rel_in(root, &self.lcp).is_some()
    }
}

/// Length of the longest common prefix of `a` and `b` that ends at a
/// component boundary in both.
fn component_lcp(a: &[u8], b: &[u8]) -> usize {
    let mut n = a.iter().zip(b).take_while(|(x, y)| x == y).count();
    let boundary = |s: &[u8], n: usize| n == s.len() || s.get(n) == Some(&b'/');
    while n > 0 && !(boundary(a, n) && boundary(b, n)) {
        n -= 1;
    }
    n
}

/// Check a validated order-1 `index` against the summaries of every tree
/// chunk of the listing, `chunks[i]` for `item_ptrs[i]`. The first failure,
/// naming the chunk and the rule.
pub(crate) fn check_against(
    index: &ListingIndex,
    chunks: &[&ChunkPaths],
) -> std::result::Result<(), String> {
    let n = chunks.len();
    let stride = index.stride.max(1) as usize;
    for (s, seg) in index.segments.iter().enumerate() {
        let a = seg.first_ptr as usize;
        let e = index
            .segments
            .get(s + 1)
            .map_or(n, |next| next.first_ptr as usize)
            .min(n);
        let root: &[u8] = &seg.root;
        let rel = |p: &'_ [u8], c: usize| -> std::result::Result<Vec<u8>, String> {
            rel_in(root, p)
                .map(<[u8]>::to_vec)
                .ok_or_else(|| format!("tree chunk {c}: entry outside its segment root"))
        };
        let mut prev_last: Option<Vec<u8>> = None;
        for c in a..e {
            let Some(chunk) = chunks.get(c) else {
                return Err(format!("tree chunk {c}: missing"));
            };
            if chunk.items == 0 {
                return Err(format!("tree chunk {c}: empty"));
            }
            if !chunk.clean {
                return Err(format!("tree chunk {c}: a path with an empty component"));
            }
            if !chunk.ordered {
                return Err(format!("tree chunk {c}: entries out of order"));
            }
            if !chunk.inside(root) {
                return Err(format!("tree chunk {c}: entry outside its segment root"));
            }
            let first = rel(&chunk.first, c)?;
            if let Some(pl) = &prev_last
                && dfs_name_cmp(pl, &first) == Ordering::Greater
            {
                return Err(format!(
                    "tree chunk {c}: first entry sorts before the previous chunk's last"
                ));
            }
            if c.is_multiple_of(stride) {
                let Some(key) = index.keys.get(c / stride) else {
                    return Err(format!("tree chunk {c}: no key"));
                };
                match &prev_last {
                    None if !key.is_empty() => {
                        return Err(format!("tree chunk {c}: non-empty key at a segment start"));
                    }
                    None => {}
                    Some(pl) => {
                        if dfs_name_cmp(key, pl) == Ordering::Less {
                            return Err(format!(
                                "tree chunk {c}: key sorts before the previous chunk's last entry"
                            ));
                        }
                        if dfs_name_cmp(key, &first) == Ordering::Greater {
                            return Err(format!("tree chunk {c}: key sorts after its first entry"));
                        }
                    }
                }
            }
            prev_last = Some(rel(&chunk.last, c)?);
        }
    }
    Ok(())
}

/// What `check` makes of a snapshot's `ext`.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum ExtCheck {
    Ok,
    /// No index, for a legitimate reason (see [`NoIndex::warns`]).
    NoIndex(NoIndex),
    /// The index does not decode, is invalid, or contradicts the listing.
    Mismatch(String),
}

/// Decode `ext` and check it against the summaries of the listing's tree
/// chunks (see [`check_against`]).
pub(crate) fn check_ext(ext: Option<&[u8]>, chunks: &[&ChunkPaths]) -> ExtCheck {
    match decode_ext(ext, chunks.len()) {
        Err(no) if no.warns() => ExtCheck::Mismatch(no.to_string()),
        Err(no) => ExtCheck::NoIndex(no),
        Ok(index) => match check_against(&index, chunks) {
            Ok(()) => ExtCheck::Ok,
            Err(reason) => ExtCheck::Mismatch(reason),
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    fn lt(a: &[u8], b: &[u8]) -> bool {
        dfs_name_cmp(a, b) == Ordering::Less
    }

    #[test]
    fn dfs_name_cmp_table() {
        assert!(lt(b"a.b", b"a/x"));
        assert!(lt(b"a/x", b"a.b/y"));
        assert!(lt(b"b", b"a/x"));
        assert!(lt(b"a/b", b"a/b/c"));
        assert!(lt(b"", b"a"));
        assert!(lt(b"", b"a/b"));
        assert_eq!(dfs_name_cmp(b"a/b", b"a/b"), Ordering::Equal);
        assert!(lt(b"n\x80", "né".as_bytes()));
        assert!(lt("né".as_bytes(), b"n\xEF\xBF\xBD"));
        // A backslash is a name byte, not a separator.
        assert!(lt(b"a\\b", b"a/a"));
        assert!(lt(b"a/z", b"a\\b/a"));
    }

    /// The tuple oracle of tests/name_order.rs.
    fn oracle_key(p: &[u8]) -> (Vec<&[u8]>, &[u8]) {
        let mut c: Vec<&[u8]> = p.split(|b| *b == b'/').collect();
        let name = c.pop().unwrap();
        (c, name)
    }

    fn clean_path() -> impl Strategy<Value = Vec<u8>> {
        let comp = prop::collection::vec(
            prop::sample::select(vec![b'a', b'b', b'.', b'-', b' ', b'\\', 0x80, 0xff]),
            1..4,
        );
        prop::collection::vec(comp, 1..4).prop_map(|cs| cs.join(&b'/'))
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(256))]

        #[test]
        fn dfs_name_cmp_agrees_with_the_oracle(mut paths in prop::collection::vec(clean_path(), 0..40)) {
            let mut by_oracle = paths.clone();
            by_oracle.sort_by(|a, b| oracle_key(a).cmp(&oracle_key(b)));
            paths.sort_by(|a, b| dfs_name_cmp(a, b));
            prop_assert_eq!(paths, by_oracle);
        }

        #[test]
        fn separator_is_the_shortest_prefix_between(a in clean_path(), b in clean_path()) {
            let (last, first) = if dfs_name_cmp(&a, &b) == Ordering::Greater { (b, a) } else { (a, b) };
            let sep = separator(&last, &first);
            prop_assert!(first.starts_with(&sep));
            prop_assert!(dfs_name_cmp(&last, &sep) != Ordering::Greater);
            prop_assert!(dfs_name_cmp(&sep, &first) != Ordering::Greater);
            if let Some(n) = sep.len().checked_sub(1) {
                prop_assert!(lt(&first[..n], &last));
            }
            prop_assert_eq!(separator(&first, &first), first.clone());
        }
    }

    /// Feed `segments` (root, paths) to a builder, one chunk per `per_chunk`
    /// entries and a cut at every segment start; returns the index and the
    /// chunk count.
    fn build(
        segments: &[(&[u8], &[&[u8]])],
        per_chunk: usize,
        stride: u32,
        explicit: bool,
    ) -> (ListingIndex, Vec<Vec<Vec<u8>>>) {
        let mut b = ListingIndexBuilder::with_stride(stride);
        let mut chunks: Vec<Vec<Vec<u8>>> = Vec::new();
        let mut open: Vec<Vec<u8>> = Vec::new();
        for (root, paths) in segments {
            if !open.is_empty() {
                chunks.push(std::mem::take(&mut open));
            }
            if explicit {
                b.begin_segment(root.to_vec());
            }
            for p in *paths {
                b.observe(p, chunks.len(), open.is_empty());
                open.push(p.to_vec());
                if open.len() == per_chunk {
                    chunks.push(std::mem::take(&mut open));
                }
            }
        }
        if !open.is_empty() {
            chunks.push(open);
        }
        (b.finish(chunks.len()), chunks)
    }

    fn summaries(chunks: &[Vec<Vec<u8>>]) -> Vec<ChunkPaths> {
        chunks
            .iter()
            .map(|c| {
                let mut s = ChunkPaths::default();
                for p in c {
                    s.observe(p);
                }
                s
            })
            .collect()
    }

    fn check(index: &ListingIndex, sums: &[ChunkPaths]) -> std::result::Result<(), String> {
        let refs: Vec<&ChunkPaths> = sums.iter().collect();
        check_against(index, &refs)
    }

    const TREE: &[&[u8]] = &[
        b"a", b"a.b", b"b", b"a/x", b"a/y", b"a/y/1", b"a/y/2", b"b/c", b"b/d",
    ];

    #[test]
    fn builder_implicit_segment_is_order_1() {
        let (ix, chunks) = build(&[(b"", TREE)], 2, 2, false);
        assert_eq!(ix.order, ORDER_DFS_NAME);
        assert_eq!(
            ix.segments,
            vec![Segment {
                root: ByteBuf::new(),
                first_ptr: 0
            }]
        );
        assert_eq!(chunks.len(), 5);
        assert_eq!(ix.keys.len(), 3);
        assert!(ix.keys[0].is_empty());
        // chunk 2 starts with "a/y" after "a/x": the separator "a/y".
        assert_eq!(ix.keys[1].as_slice(), b"a/y");
        // chunk 4 starts with "b/d" after "b/c".
        assert_eq!(ix.keys[2].as_slice(), b"b/d");
        check(&ix, &summaries(&chunks)).unwrap();
    }

    #[test]
    fn builder_segments() {
        let dumps: &[&[u8]] = &[b"vykar-dumps", b"vykar-dumps/a", b"vykar-dumps/b"];
        let one: &[&[u8]] = &[b"home/u/b", b"home/u/b/x", b"home/u/b/y/z"];
        let two: &[&[u8]] = &[b"home/u/a", b"home/u/a/k"];
        let (ix, chunks) = build(
            &[
                (b"vykar-dumps", dumps),
                (b"none", &[]),
                (b"home/u/b", one),
                (b"home/u/a", two),
            ],
            2,
            1,
            true,
        );
        assert_eq!(ix.order, ORDER_DFS_NAME);
        let segs: Vec<(&[u8], u32)> = ix
            .segments
            .iter()
            .map(|s| (s.root.as_slice(), s.first_ptr))
            .collect();
        // The empty segment is not recorded.
        assert_eq!(
            segs,
            vec![
                (&b"vykar-dumps"[..], 0),
                (&b"home/u/b"[..], 2),
                (&b"home/u/a"[..], 4)
            ]
        );
        assert_eq!(ix.keys.len(), chunks.len());
        for s in &ix.segments {
            assert!(ix.keys[s.first_ptr as usize].is_empty());
        }
        // Keys are relative to the segment root.
        assert_eq!(ix.keys[3].as_slice(), b"y");
        check(&ix, &summaries(&chunks)).unwrap();
    }

    #[test]
    fn builder_order_violations_give_order_0() {
        let order0 = |segments: &[(&[u8], &[&[u8]])]| build(segments, 2, 1, true).0;
        // Outside the root.
        assert_eq!(order0(&[(b"r", &[b"r", b"s"])]).order, ORDER_UNKNOWN);
        // Decreasing.
        assert_eq!(order0(&[(b"", &[b"b", b"a"])]).order, ORDER_UNKNOWN);
        // Unclean paths.
        for bad in [&b"a//b"[..], b"/a", b"a/", b""] {
            let ix = order0(&[(b"", &[b"0", bad])]);
            assert_eq!(ix.order, ORDER_UNKNOWN, "{bad:?}");
            assert!(ix.segments.is_empty() && ix.keys.is_empty());
        }
        // Equal consecutive paths are fine, also across a chunk boundary.
        let (ix, chunks) = build(&[(b"", &[b"a", b"b", b"b", b"c"])], 2, 1, true);
        assert_eq!(ix.order, ORDER_DFS_NAME);
        assert_eq!(ix.keys[1].as_slice(), b"b");
        check(&ix, &summaries(&chunks)).unwrap();
        // An entry pushed mid-chunk at a segment start (no forced cut).
        let mut b = ListingIndexBuilder::with_stride(1);
        b.observe(b"a", 0, true);
        b.begin_segment(b"r".to_vec());
        b.observe(b"r", 0, false);
        assert_eq!(b.finish(1).order, ORDER_UNKNOWN);
    }

    #[test]
    fn builder_keys_count() {
        for n in 0..10usize {
            let paths: Vec<Vec<u8>> = (0..n).map(|i| format!("f{i}").into_bytes()).collect();
            let refs: Vec<&[u8]> = paths.iter().map(Vec::as_slice).collect();
            let (ix, chunks) = build(&[(b"", &refs)], 1, 3, false);
            assert_eq!(ix.order, ORDER_DFS_NAME);
            assert_eq!(ix.keys.len(), chunks.len().div_ceil(3));
            assert_eq!(ix.segments.is_empty(), n == 0);
            decode_ext(Some(&encode_ext(&ix).unwrap()), chunks.len()).unwrap();
        }
    }

    fn golden() -> ListingIndex {
        ListingIndex {
            order: ORDER_DFS_NAME,
            segments: vec![
                Segment {
                    root: ByteBuf::from(b"vykar-dumps".to_vec()),
                    first_ptr: 0,
                },
                Segment {
                    root: ByteBuf::from(b"r".to_vec()),
                    first_ptr: 1,
                },
            ],
            stride: 32,
            keys: vec![ByteBuf::new(), ByteBuf::from(b"a/b".to_vec())],
        }
    }

    #[test]
    fn ext_golden_bytes() {
        let bytes = encode_ext(&golden()).unwrap();
        assert_eq!(
            hex::encode(&bytes),
            "92019401\
             92\
             92c40b76796b61722d64756d707300\
             92c4017201\
             20\
             92c400c403612f62"
        );
        assert_eq!(decode_ext(Some(&bytes), 33).unwrap(), golden());
    }

    fn enc<T: Serialize>(v: &T) -> Vec<u8> {
        rmp_serde::to_vec(v).unwrap()
    }

    #[test]
    fn ext_decode_rules() {
        let g = golden();
        assert_eq!(decode_ext(None, 33), Err(NoIndex::Absent));
        assert_eq!(
            decode_ext(Some(&enc(&(2u32, &g))), 33),
            Err(NoIndex::UnknownVersion(2))
        );
        assert_eq!(
            decode_ext(Some(&enc(&(7u32,))), 33),
            Err(NoIndex::UnknownVersion(7))
        );
        // A trailing element is ignored.
        assert_eq!(
            decode_ext(Some(&enc(&(1u32, &g, "extra"))), 33),
            Ok(g.clone())
        );
        // ListingIndex is strict.
        let five = (g.order, &g.segments, g.stride, &g.keys, 0u8);
        assert!(matches!(
            decode_ext(Some(&enc(&(1u32, five))), 33),
            Err(NoIndex::Undecodable(_))
        ));
        assert!(matches!(
            decode_ext(Some(&enc(&(1u32,))), 33),
            Err(NoIndex::Undecodable(_))
        ));
        assert!(matches!(
            decode_ext(Some(b"garbage"), 33),
            Err(NoIndex::Undecodable(_))
        ));
        assert!(matches!(
            decode_ext(Some(&[]), 33),
            Err(NoIndex::Undecodable(_))
        ));
        let with = |f: &dyn Fn(&mut ListingIndex)| {
            let mut ix = g.clone();
            f(&mut ix);
            decode_ext(Some(&encode_ext(&ix).unwrap()), 33)
        };
        assert_eq!(with(&|ix| ix.order = 0), Err(NoIndex::Unordered));
        assert_eq!(with(&|ix| ix.order = 7), Err(NoIndex::UnknownOrder(7)));
        for (f, what) in [
            (
                &(|ix: &mut ListingIndex| ix.stride = 0) as &dyn Fn(&mut ListingIndex),
                "stride 0",
            ),
            (
                &|ix: &mut ListingIndex| ix.stride = 65_537,
                "stride too large",
            ),
            (
                &|ix: &mut ListingIndex| ix.keys.push(ByteBuf::new()),
                "keys count",
            ),
            (
                &|ix: &mut ListingIndex| ix.segments[1].first_ptr = 0,
                "not increasing",
            ),
            (
                &|ix: &mut ListingIndex| ix.segments[1].first_ptr = 33,
                "past the end",
            ),
            (
                &|ix: &mut ListingIndex| ix.segments[0].first_ptr = 1,
                "first not 0",
            ),
            (
                &|ix: &mut ListingIndex| ix.segments[1].root = ByteBuf::from(b"a//b".to_vec()),
                "unclean root",
            ),
            (&|ix: &mut ListingIndex| ix.segments.clear(), "no segments"),
        ] {
            assert!(matches!(with(f), Err(NoIndex::Invalid(_))), "{what}");
        }
        // Wrong pointer count for the keys.
        assert!(matches!(
            decode_ext(Some(&encode_ext(&g).unwrap()), 70),
            Err(NoIndex::Invalid(_))
        ));
        // An empty listing: no segments, no keys.
        let empty = ListingIndex {
            order: 1,
            segments: vec![],
            stride: 32,
            keys: vec![],
        };
        assert_eq!(decode_ext(Some(&encode_ext(&empty).unwrap()), 0), Ok(empty));
    }

    #[test]
    fn ext_decode_is_bounded_on_hostile_bytes() {
        // [1, [1, [], 32, array32 of 0xFFFFFFFF keys...]] and nothing after.
        let mut huge_keys = vec![
            0x92, 0x01, 0x94, 0x01, 0x90, 0x20, 0xdd, 0xff, 0xff, 0xff, 0xff,
        ];
        assert!(matches!(
            decode_ext(Some(&huge_keys), 1),
            Err(NoIndex::Undecodable(_))
        ));
        // One key claiming a 4 GiB bin.
        huge_keys.truncate(6);
        huge_keys.extend_from_slice(&[0x91, 0xc6, 0xff, 0xff, 0xff, 0xff, 0x00]);
        assert!(matches!(
            decode_ext(Some(&huge_keys), 1),
            Err(NoIndex::Undecodable(_))
        ));
        // An unknown version followed by an array32 header claiming 4G elements.
        let unknown = [0xdd, 0xff, 0xff, 0xff, 0xff, 0x02];
        assert!(matches!(
            decode_ext(Some(&unknown), 1),
            Err(NoIndex::Undecodable(_))
        ));
    }

    #[test]
    fn check_against_rejects_each_rule() {
        let paths: Vec<Vec<u8>> = (0..12).map(|i| format!("d/f{i:02}").into_bytes()).collect();
        let mut refs: Vec<&[u8]> = vec![b"d"];
        refs.extend(paths.iter().map(Vec::as_slice));
        let (ix, chunks) = build(&[(b"", &refs)], 2, 2, true);
        let sums = summaries(&chunks);
        check(&ix, &sums).unwrap();
        let bad_key = |key: &[u8], j: usize| {
            let mut ix = ix.clone();
            ix.keys[j] = ByteBuf::from(key.to_vec());
            check(&ix, &sums)
        };
        // Chunk 2 holds d/f03, d/f04; chunk 1 ends with d/f02.
        assert!(bad_key(b"d/f05", 1).is_err(), "key above its chunk's first");
        assert!(
            bad_key(b"d/f01", 1).is_err(),
            "key below the previous chunk's last"
        );
        assert!(
            bad_key(b"d", 0).is_err(),
            "non-empty key at a segment start"
        );
        // Wrong root.
        let mut wrong_root = ix.clone();
        wrong_root.segments[0].root = ByteBuf::from(b"e".to_vec());
        assert!(check(&wrong_root, &sums).is_err());
        // A chunk out of order inside.
        let mut unordered = chunks.clone();
        unordered[3].swap(0, 1);
        assert!(check(&ix, &summaries(&unordered)).is_err());
        // Order broken across chunks.
        let mut across = chunks.clone();
        across.swap(2, 3);
        assert!(check(&ix, &summaries(&across)).is_err());
        // An entry outside the root in the middle of a chunk.
        let (ix2, chunks2) = build(&[(b"r", &[b"r", b"r/a", b"r/b"])], 3, 1, true);
        check(&ix2, &summaries(&chunks2)).unwrap();
        let mut outside = chunks2.clone();
        outside[0][1] = b"q/a".to_vec();
        assert!(check(&ix2, &summaries(&outside)).is_err());
        // check_ext maps the kinds.
        let refs: Vec<&ChunkPaths> = sums.iter().collect();
        assert_eq!(
            check_ext(Some(&encode_ext(&ix).unwrap()), &refs),
            ExtCheck::Ok
        );
        assert_eq!(check_ext(None, &refs), ExtCheck::NoIndex(NoIndex::Absent));
        assert!(matches!(
            check_ext(Some(b"x"), &refs),
            ExtCheck::Mismatch(_)
        ));
        let mut wrong = ix.clone();
        wrong.keys[1] = ByteBuf::from(b"d/f05".to_vec());
        assert!(matches!(
            check_ext(Some(&encode_ext(&wrong).unwrap()), &refs),
            ExtCheck::Mismatch(_)
        ));
    }

    #[test]
    fn component_lcp_cases() {
        assert_eq!(component_lcp(b"ab/c", b"ab/d"), 2);
        assert_eq!(component_lcp(b"abc", b"abd"), 0);
        assert_eq!(component_lcp(b"r", b"r/x"), 1);
        assert_eq!(component_lcp(b"r/x", b"r"), 1);
        assert_eq!(component_lcp(b"r/ab", b"r/a"), 1);
    }
}
