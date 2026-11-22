//! Path-scoped reads of a snapshot listing (06 step 4): the entries of one
//! directory ([`ListingView::dir_entries`]) or of one subtree
//! ([`ListingView::subtree`]), read through the listing's key index (see
//! [`crate::snapshot::listing`]) instead of the whole listing, on a
//! repository whose chunk index is loaded.
//!
//! The index path starts at the grid chunk of the last key below the
//! queried range; for a directory's children and descendants the search
//! takes the last key not greater than `dir/`. It reads tree chunks through
//! `Repository::read_tree_chunks_split`, the local tree-chunk cache first,
//! and verifies every chunk it decodes: each entry is clean, inside its
//! segment root and not before the previous one; the first chunk it starts
//! at is not before its key; the chunk before that ends before the queried
//! range. Any failure (and an absent, unknown or invalid index) falls back
//! to a full lazy scan of the listing.
//!
//! Results are identical to the scan provided the chunks the index path
//! skips obey the order flag and the segment table, which the writer
//! derives itself and `check` verifies. Results are delivered as they are
//! decoded; when a fallback happens after some were delivered, the scan
//! skips as many matches and checks (by count and a hash of their paths)
//! that they are the ones delivered. A contradiction is an error
//! (`VykarError::InvalidFormat`), never a silently different sequence.

use std::cmp::Ordering;

use serde::Deserialize;
use serde_bytes::ByteBuf;
use tracing::debug;

use crate::commands::list::{LazyItemStream, resolve_from_index};
use crate::repo::control::CancelToken;
use crate::repo::{Repository, TreeChunkSource};
use crate::snapshot::SnapshotMeta;
use crate::snapshot::item::Item;
use crate::snapshot::listing::{
    ListingIndex, NoIndex, decode_ext, dfs_name_cmp, is_clean, parent_of, rel_in,
};
use vykar_types::chunk_id::ChunkId;
use vykar_types::error::{Result, VykarError};

/// Largest number of tree chunks one read of the index path requests,
/// whatever the index's stride: bounds the reader's memory.
const MAX_WINDOW: usize = 64;

/// A snapshot's listing opened for path-scoped reads.
pub struct ListingView<'m> {
    meta: &'m SnapshotMeta,
    index: std::result::Result<ListingIndex, NoIndex>,
    /// Never use the index (always scan).
    ignore: bool,
}

/// What one read cost.
#[derive(Debug, Clone, Default)]
pub struct ListingRead {
    /// The index answered the query without a fallback.
    pub via_index: bool,
    /// Why a scan was used (the reason there is no usable index, or the
    /// check that failed).
    pub fallback: Option<String>,
    /// Tree chunks requested (index path plus scan), from cache or storage.
    pub chunks_read: usize,
    /// Stored (compressed, encrypted) bytes of those chunks.
    pub stored_bytes: u64,
    /// Plaintext bytes decoded on the index path.
    pub plain_bytes: u64,
    /// Tree chunks that held at least one result (index path).
    pub match_chunks: usize,
    /// Entries passed to the visitor.
    pub matches: usize,
    /// Queries these counts cover (1 for one read; see [`Self::absorb`]).
    pub queries: usize,
}

impl ListingRead {
    /// Add `other`'s counts. `via_index` stays true only while every
    /// absorbed query was answered by the index; `fallback` keeps the
    /// first reason.
    pub fn absorb(&mut self, other: ListingRead) {
        self.via_index = (self.queries == 0 || self.via_index) && other.via_index;
        if self.fallback.is_none() {
            self.fallback = other.fallback;
        }
        self.chunks_read += other.chunks_read;
        self.stored_bytes += other.stored_bytes;
        self.plain_bytes += other.plain_bytes;
        self.match_chunks += other.match_chunks;
        self.matches += other.matches;
        self.queries += other.queries;
    }
}

impl<'m> ListingView<'m> {
    /// Decode and validate `meta.ext` once (bounded; never fails).
    pub fn new(meta: &'m SnapshotMeta) -> Self {
        Self {
            meta,
            index: decode_ext(meta.ext.as_deref(), meta.item_ptrs.len()),
            ignore: false,
        }
    }

    /// A view that never uses the index (always scans). For tests and
    /// measurement.
    pub fn ignore_index(meta: &'m SnapshotMeta) -> Self {
        Self {
            ignore: true,
            ..Self::new(meta)
        }
    }

    /// The decoded index, or why there is none.
    pub fn index(&self) -> std::result::Result<&ListingIndex, &NoIndex> {
        self.index.as_ref()
    }

    /// Entries whose parent is `dir` (snapshot path bytes as
    /// `Item::path_bytes()` stores them; leading and trailing "/" are
    /// ignored; empty = top level), in stream order.
    ///
    /// The repository's chunk index must be loaded; a pointer it lacks
    /// fails the read, as every listing reader does.
    pub fn dir_entries(
        &self,
        repo: &Repository,
        dir: &[u8],
        shutdown: &CancelToken,
        mut visit: impl FnMut(Item) -> Result<()>,
    ) -> Result<ListingRead> {
        self.read(repo, Query::DirEntries(trim_dir(dir)), shutdown, &mut visit)
    }

    /// `dir` itself (every entry whose path equals it) and every entry
    /// under `dir + "/"`, in stream order. An empty `dir` is the whole
    /// listing (a scan).
    ///
    /// The repository's chunk index must be loaded; a pointer it lacks
    /// fails the read, as every listing reader does.
    pub fn subtree(
        &self,
        repo: &Repository,
        dir: &[u8],
        shutdown: &CancelToken,
        mut visit: impl FnMut(Item) -> Result<()>,
    ) -> Result<ListingRead> {
        self.read(repo, Query::Subtree(trim_dir(dir)), shutdown, &mut visit)
    }

    fn read(
        &self,
        repo: &Repository,
        q: Query<'_>,
        shutdown: &CancelToken,
        visit: &mut dyn FnMut(Item) -> Result<()>,
    ) -> Result<ListingRead> {
        let ptrs = &self.meta.item_ptrs;
        let mut stats = ListingRead {
            queries: 1,
            ..ListingRead::default()
        };
        let mut delivered = Delivered::new();
        if !q.is_whole_listing() {
            match (self.ignore, &self.index) {
                (true, _) => stats.fallback = Some("key index ignored".into()),
                (false, Err(no)) => stats.fallback = Some(no.to_string()),
                (false, Ok(index)) => {
                    let mut spans = RepoSpans {
                        repo,
                        ptrs,
                        shutdown,
                    };
                    let outcome = query_index(
                        index,
                        ptrs.len(),
                        q,
                        &mut spans,
                        shutdown,
                        &mut |item| {
                            delivered.note(item.path_bytes());
                            visit(item)
                        },
                        &mut stats,
                    )?;
                    match outcome {
                        Outcome::Done => {
                            stats.via_index = true;
                            return Ok(stats);
                        }
                        Outcome::Fallback(reason) => {
                            debug!(
                                snapshot = %self.meta.name,
                                %reason,
                                "listing read falls back to a scan"
                            );
                            stats.fallback = Some(reason);
                        }
                    }
                }
            }
        }
        let mut stream = LazyItemStream::open(repo, ptrs, shutdown)?;
        stats.chunks_read += ptrs.len();
        stats.stored_bytes += ptrs
            .iter()
            .filter_map(|id| repo.chunk_index().get(id))
            .map(|e| u64::from(e.stored_size))
            .sum::<u64>();
        scan_after(
            q,
            &delivered,
            &self.meta.name,
            &mut |f| stream.for_each_item(f),
            visit,
            &mut stats,
        )?;
        Ok(stats)
    }
}

/// `dir` without leading and trailing "/".
fn trim_dir(dir: &[u8]) -> &[u8] {
    let start = dir.iter().position(|b| *b != b'/').unwrap_or(dir.len());
    let end = dir
        .iter()
        .rposition(|b| *b != b'/')
        .map_or(start, |i| i + 1);
    dir.get(start..end).unwrap_or_default()
}

/// Reads a contiguous span of a listing's tree chunks.
pub(crate) trait SpanReader {
    /// Plaintexts of tree chunks `first..end` concatenated into `out`
    /// (cleared first); chunk `first + i` ends at `ends[i]`. Returns the
    /// stored bytes read.
    fn read_span(
        &mut self,
        first: usize,
        end: usize,
        out: &mut Vec<u8>,
        ends: &mut Vec<usize>,
    ) -> Result<u64>;
}

/// Production reader: resolved through the chunk index, read through the
/// tree-chunk cache first.
struct RepoSpans<'a> {
    repo: &'a Repository,
    ptrs: &'a [ChunkId],
    shutdown: &'a CancelToken,
}

impl SpanReader for RepoSpans<'_> {
    fn read_span(
        &mut self,
        first: usize,
        end: usize,
        out: &mut Vec<u8>,
        ends: &mut Vec<usize>,
    ) -> Result<u64> {
        let ids = self
            .ptrs
            .get(first..end)
            .ok_or_else(|| VykarError::Other("tree chunk span out of range".into()))?;
        let locs = resolve_from_index(self.repo.chunk_index(), ids)?;
        out.clear();
        self.repo.read_tree_chunks_split(
            &locs,
            out,
            ends,
            self.shutdown,
            TreeChunkSource::CacheFirst,
        )?;
        Ok(locs.iter().map(|l| u64::from(l.3)).sum())
    }
}

/// A path-scoped query; the directory is already trimmed.
#[derive(Debug, Clone, Copy)]
pub(crate) enum Query<'q> {
    DirEntries(&'q [u8]),
    Subtree(&'q [u8]),
}

impl<'q> Query<'q> {
    fn dir(self) -> &'q [u8] {
        match self {
            Query::DirEntries(d) | Query::Subtree(d) => d,
        }
    }

    fn is_whole_listing(self) -> bool {
        matches!(self, Query::Subtree(d) if d.is_empty())
    }
}

/// Full-path predicate of a query; the index path and the scan both filter
/// with it.
pub(crate) fn matches(q: Query<'_>, p: &[u8]) -> bool {
    match q {
        Query::DirEntries(d) => parent_of(p) == d,
        Query::Subtree(d) => {
            d.is_empty() || p == d || (p.starts_with(d) && p.get(d.len()) == Some(&b'/'))
        }
    }
}

/// How the index path ended.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum Outcome {
    Done,
    Fallback(String),
}

/// The part of one segment a query needs, in segment-relative paths.
#[derive(Debug, Clone, Copy)]
enum Range<'d> {
    /// Every entry of the segment.
    Whole,
    /// Entries equal to the path.
    Exact(&'d [u8]),
    /// Entries whose parent is the path.
    Children(&'d [u8]),
    /// Entries under the path.
    Descendants(&'d [u8]),
}

impl Range<'_> {
    /// The smallest relative path the range can hold.
    fn lower(self) -> Vec<u8> {
        match self {
            Range::Whole => Vec::new(),
            Range::Exact(dr) => dr.to_vec(),
            Range::Children(dr) | Range::Descendants(dr) => {
                if dr.is_empty() {
                    Vec::new()
                } else {
                    let mut lo = dr.to_vec();
                    lo.push(b'/');
                    lo
                }
            }
        }
    }

    /// `rel` (not below the lower bound) is past the range. Exact for clean
    /// paths.
    fn past(self, rel: &[u8]) -> bool {
        match self {
            Range::Whole => false,
            Range::Exact(dr) => rel != dr,
            Range::Children(dr) => parent_of(rel) != dr,
            Range::Descendants(dr) => {
                if dr.is_empty() {
                    return false;
                }
                let p = parent_of(rel);
                !(p == dr || (p.starts_with(dr) && p.get(dr.len()) == Some(&b'/')))
            }
        }
    }
}

/// The ranges of the segment rooted at `root` that query `q` needs, in
/// stream order.
fn segment_ranges<'d>(q: Query<'d>, root: &[u8]) -> [Option<Range<'d>>; 2] {
    let d = q.dir();
    let inside: Option<&'d [u8]> = if root.is_empty() {
        Some(d)
    } else if d == root {
        Some(&[])
    } else {
        d.strip_prefix(root).and_then(|r| r.strip_prefix(b"/"))
    };
    if let Some(dr) = inside {
        return match q {
            Query::DirEntries(_) => [Some(Range::Children(dr)), None],
            Query::Subtree(_) if !root.is_empty() && d == root => [Some(Range::Whole), None],
            Query::Subtree(_) => [Some(Range::Exact(dr)), Some(Range::Descendants(dr))],
        };
    }
    let above = !root.is_empty()
        && (d.is_empty()
            || root
                .strip_prefix(d)
                .is_some_and(|r| r.first() == Some(&b'/')));
    if !above {
        return [None, None];
    }
    match q {
        Query::DirEntries(_) if parent_of(root) == d => [Some(Range::Exact(&[])), None],
        Query::DirEntries(_) => [None, None],
        Query::Subtree(_) => [Some(Range::Whole), None],
    }
}

/// The index path's reading state.
struct RangeReader<'x> {
    keys: &'x [ByteBuf],
    stride: usize,
    q: Query<'x>,
    spans: &'x mut dyn SpanReader,
    shutdown: &'x CancelToken,
    emit: &'x mut dyn FnMut(Item) -> Result<()>,
    stats: &'x mut ListingRead,
    buf: Vec<u8>,
    ends: Vec<usize>,
    /// Relative path of the previous decoded entry of the range.
    prev: Vec<u8>,
}

impl RangeReader<'_> {
    /// Read range `r` of the segment rooted at `root` covering chunks
    /// `[a, e)`. `Ok(Some(reason))` asks for the fallback; `Err` is a
    /// visitor error or a cancellation.
    fn read_range(
        &mut self,
        root: &[u8],
        seg_start: usize,
        seg_end: usize,
        range: Range<'_>,
    ) -> Result<Option<String>> {
        if seg_start >= seg_end {
            return Ok(None);
        }
        let stride = self.stride;
        let lo = range.lower();
        let (start, key_j) = if lo.is_empty() {
            (seg_start, None)
        } else {
            // The last key below the lower bound among the keys of chunks
            // after the segment's first. A Children/Descendants bound ends
            // in "/" and no clean entry equals it, so a key equal to it
            // already separates the range from every earlier chunk; Exact
            // keeps the strict search, because duplicates of its bound may
            // straddle a chunk boundary.
            let inclusive = matches!(range, Range::Children(_) | Range::Descendants(_));
            let j_lo = seg_start / stride + 1;
            let j_hi = (seg_end - 1) / stride;
            let candidates = if j_lo <= j_hi {
                self.keys.get(j_lo..=j_hi)
            } else {
                None
            };
            let found = candidates
                .map(|ks| {
                    ks.partition_point(|k| match dfs_name_cmp(k, &lo) {
                        Ordering::Less => true,
                        Ordering::Equal => inclusive,
                        Ordering::Greater => false,
                    })
                })
                .filter(|p| *p > 0)
                .map(|p| j_lo + p - 1);
            match found {
                Some(j) => (j * stride, Some(j)),
                None => (seg_start, None),
            }
        };
        let first = if !lo.is_empty() && start > seg_start {
            start - 1
        } else {
            start
        };
        let mut planned_end = if lo.is_empty() {
            seg_start + 1
        } else {
            (start / stride + 1) * stride
        }
        .min(seg_end);
        let mut grow = 1usize;
        let mut has_prev = false;
        let mut cur = first;
        while cur < seg_end {
            let end = planned_end.min(cur + MAX_WINDOW).min(seg_end);
            self.shutdown.check()?;
            let stored = match self
                .spans
                .read_span(cur, end, &mut self.buf, &mut self.ends)
            {
                Ok(bytes) => bytes,
                Err(VykarError::Interrupted) => return Err(VykarError::Interrupted),
                Err(err) => return Ok(Some(format!("tree chunk read failed: {err}"))),
            };
            if self.ends.len() != end - cur {
                return Ok(Some("tree chunk read returned a wrong chunk count".into()));
            }
            self.stats.chunks_read += end - cur;
            self.stats.stored_bytes += stored;
            self.stats.plain_bytes += self.buf.len() as u64;
            let mut chunk_start = 0usize;
            for i in 0..self.ends.len() {
                let chunk = cur + i;
                let chunk_end = self.ends.get(i).copied().unwrap_or_default();
                let Some(bytes) = self.buf.get(chunk_start..chunk_end) else {
                    return Ok(Some(format!("tree chunk {chunk} out of bounds")));
                };
                chunk_start = chunk_end;
                if bytes.is_empty() {
                    return Ok(Some(format!("tree chunk {chunk} is empty")));
                }
                let mut de = rmp_serde::Deserializer::new(std::io::Cursor::new(bytes));
                let mut first_entry = true;
                let mut matched_here = false;
                let mut done = false;
                while (de.position() as usize) < bytes.len() {
                    let Ok(item) = Item::deserialize(&mut de) else {
                        return Ok(Some(format!("tree chunk {chunk} does not decode")));
                    };
                    let path = item.path_bytes();
                    if !is_clean(path) {
                        return Ok(Some("entry with an empty path component".into()));
                    }
                    let Some(rel) = rel_in(root, path) else {
                        return Ok(Some("entry outside its segment root".into()));
                    };
                    if has_prev && dfs_name_cmp(rel, &self.prev) == Ordering::Less {
                        return Ok(Some("listing out of order".into()));
                    }
                    if first_entry
                        && chunk == start
                        && let Some(key) = key_j.and_then(|j| self.keys.get(j))
                        && dfs_name_cmp(rel, key) == Ordering::Less
                    {
                        return Ok(Some("first entry sorts before its key".into()));
                    }
                    first_entry = false;
                    self.prev.clear();
                    self.prev.extend_from_slice(rel);
                    has_prev = true;
                    if chunk < start || dfs_name_cmp(rel, &lo) == Ordering::Less {
                        continue;
                    }
                    if range.past(rel) {
                        done = true;
                        break;
                    }
                    if matches(self.q, path) {
                        matched_here = true;
                        self.stats.matches += 1;
                        (self.emit)(item)?;
                    }
                }
                if matched_here {
                    self.stats.match_chunks += 1;
                }
                if done {
                    return Ok(None);
                }
                if chunk < start && dfs_name_cmp(&self.prev, &lo) != Ordering::Less {
                    return Ok(Some("predecessor chunk reaches the queried range".into()));
                }
            }
            cur = end;
            if cur >= planned_end {
                planned_end = cur + grow.min(stride).min(MAX_WINDOW);
                grow = grow.saturating_mul(2);
            }
        }
        Ok(None)
    }
}

/// The index path. A visitor error is returned as `Err`; every problem with
/// the index or a chunk (a read error other than `Interrupted`, a decode
/// error, an order, containment, key or predecessor check) is
/// `Ok(Outcome::Fallback(reason))`.
pub(crate) fn query_index(
    index: &ListingIndex,
    n_ptrs: usize,
    q: Query<'_>,
    spans: &mut dyn SpanReader,
    shutdown: &CancelToken,
    emit: &mut dyn FnMut(Item) -> Result<()>,
    stats: &mut ListingRead,
) -> Result<Outcome> {
    let mut reader = RangeReader {
        keys: &index.keys,
        stride: (index.stride as usize).max(1),
        q,
        spans,
        shutdown,
        emit,
        stats,
        buf: Vec::new(),
        ends: Vec::new(),
        prev: Vec::new(),
    };
    for (s, seg) in index.segments.iter().enumerate() {
        let a = seg.first_ptr as usize;
        let e = index
            .segments
            .get(s + 1)
            .map_or(n_ptrs, |next| next.first_ptr as usize)
            .min(n_ptrs);
        for r in segment_ranges(q, &seg.root).into_iter().flatten() {
            if let Some(reason) = reader.read_range(&seg.root, a, e, r)? {
                return Ok(Outcome::Fallback(reason));
            }
        }
    }
    Ok(Outcome::Done)
}

/// Visits every entry of a listing in stream order with the visitor it is
/// given (the scan).
pub(crate) type ForEachItem<'f> = dyn FnMut(&mut dyn FnMut(Item) -> Result<()>) -> Result<()> + 'f;

/// Count and hash of the paths delivered so far.
struct Delivered {
    count: usize,
    hash: blake3::Hasher,
}

impl Delivered {
    fn new() -> Self {
        Self {
            count: 0,
            hash: blake3::Hasher::new(),
        }
    }

    fn note(&mut self, path: &[u8]) {
        self.count += 1;
        self.hash.update(&(path.len() as u64).to_le_bytes());
        self.hash.update(path);
    }
}

/// The scan after the index path: `for_each` visits every entry of the
/// listing in stream order; the matches past the `delivered` ones go to
/// `visit`. The skipped matches must be the delivered ones.
fn scan_after(
    q: Query<'_>,
    delivered: &Delivered,
    snapshot: &str,
    for_each: &mut ForEachItem<'_>,
    visit: &mut dyn FnMut(Item) -> Result<()>,
    stats: &mut ListingRead,
) -> Result<()> {
    let contradiction = || {
        VykarError::InvalidFormat(format!(
            "snapshot '{snapshot}': its listing contradicts its key index after results \
             were delivered; run check"
        ))
    };
    let k = delivered.count;
    let mut seen = Delivered::new();
    for_each(&mut |item| {
        let p = item.path_bytes();
        if !matches(q, p) {
            return Ok(());
        }
        if seen.count < k {
            seen.note(p);
            if seen.count == k && seen.hash.finalize() != delivered.hash.finalize() {
                return Err(contradiction());
            }
            return Ok(());
        }
        stats.matches += 1;
        visit(item)
    })?;
    if seen.count < k {
        return Err(contradiction());
    }
    Ok(())
}

/// The index path, then the scan for what it could not answer, over any
/// span reader and scan: what [`ListingView`] does, for the unit tests.
#[cfg(test)]
pub(crate) fn run_query(
    index: std::result::Result<&ListingIndex, &NoIndex>,
    n_ptrs: usize,
    q: Query<'_>,
    spans: &mut dyn SpanReader,
    shutdown: &CancelToken,
    for_each: &mut ForEachItem<'_>,
    visit: &mut dyn FnMut(Item) -> Result<()>,
) -> Result<ListingRead> {
    let mut stats = ListingRead {
        queries: 1,
        ..ListingRead::default()
    };
    let mut delivered = Delivered::new();
    match index {
        Err(no) => stats.fallback = Some(no.to_string()),
        Ok(index) => {
            let outcome = query_index(
                index,
                n_ptrs,
                q,
                spans,
                shutdown,
                &mut |item| {
                    delivered.note(item.path_bytes());
                    visit(item)
                },
                &mut stats,
            )?;
            match outcome {
                Outcome::Done => {
                    stats.via_index = true;
                    return Ok(stats);
                }
                Outcome::Fallback(reason) => stats.fallback = Some(reason),
            }
        }
    }
    scan_after(q, &delivered, "test", for_each, visit, &mut stats)?;
    Ok(stats)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::commands::list::for_each_decoded_item;
    use crate::snapshot::item::{ItemRawNames, ItemType};
    use crate::snapshot::listing::{ListingIndexBuilder, NoIndex};
    use proptest::prelude::*;

    fn item(path: &[u8]) -> Item {
        Item {
            path: String::from_utf8_lossy(path).into_owned(),
            entry_type: ItemType::RegularFile,
            mode: 0o644,
            uid: 0,
            gid: 0,
            user: None,
            group: None,
            mtime: 0,
            atime: None,
            ctime: None,
            size: 0,
            chunks: Vec::new(),
            link_target: None,
            xattrs: None,
            raw_names: std::str::from_utf8(path).is_err().then(|| ItemRawNames {
                path: Some(path.to_vec()),
                link_target: None,
            }),
            hardlink: None,
        }
    }

    fn encode(path: &[u8]) -> Vec<u8> {
        rmp_serde::to_vec(&item(path)).unwrap()
    }

    /// Chunk plaintexts in memory; stored bytes = plaintext length.
    struct MemSpans<'a> {
        chunks: &'a [Vec<u8>],
        max_width: usize,
    }

    impl SpanReader for MemSpans<'_> {
        fn read_span(
            &mut self,
            first: usize,
            end: usize,
            out: &mut Vec<u8>,
            ends: &mut Vec<usize>,
        ) -> Result<u64> {
            out.clear();
            ends.clear();
            for chunk in &self.chunks[first..end] {
                out.extend_from_slice(chunk);
                ends.push(out.len());
            }
            self.max_width = self.max_width.max(end - first);
            Ok(out.len() as u64)
        }
    }

    struct Listing {
        chunks: Vec<Vec<u8>>,
        index: ListingIndex,
        /// Every entry of `chunks`, decoded chunk by chunk.
        all: Vec<Vec<u8>>,
    }

    impl Listing {
        /// Replace chunk `c` by the entry `path` (the index stays).
        fn replace_chunk(&mut self, c: usize, path: &[u8]) {
            self.chunks[c] = encode(path);
            self.all = decode_all(&self.chunks);
        }

        fn oracle(&self, q: Query<'_>) -> Vec<Vec<u8>> {
            self.all.iter().filter(|p| matches(q, p)).cloned().collect()
        }

        /// The index path alone.
        fn index_path(&self, q: Query<'_>) -> (Outcome, Vec<Vec<u8>>, ListingRead, usize) {
            let mut spans = MemSpans {
                chunks: &self.chunks,
                max_width: 0,
            };
            let mut stats = ListingRead::default();
            let mut got = Vec::new();
            let outcome = query_index(
                &self.index,
                self.chunks.len(),
                q,
                &mut spans,
                &CancelToken::new(),
                &mut |i| {
                    got.push(i.path_bytes().to_vec());
                    Ok(())
                },
                &mut stats,
            )
            .unwrap();
            (outcome, got, stats, spans.max_width)
        }

        /// The index path and the fallback scan.
        fn run(
            &self,
            q: Query<'_>,
            visit: &mut dyn FnMut(Item) -> Result<()>,
        ) -> Result<ListingRead> {
            let mut spans = MemSpans {
                chunks: &self.chunks,
                max_width: 0,
            };
            let chunks = &self.chunks;
            run_query(
                Ok(&self.index),
                chunks.len(),
                q,
                &mut spans,
                &CancelToken::new(),
                &mut |f| {
                    for c in chunks {
                        for_each_decoded_item(c, &mut *f)?;
                    }
                    Ok(())
                },
                visit,
            )
        }

        fn run_paths(&self, q: Query<'_>) -> (Vec<Vec<u8>>, ListingRead) {
            let mut got = Vec::new();
            let stats = self
                .run(q, &mut |i| {
                    got.push(i.path_bytes().to_vec());
                    Ok(())
                })
                .unwrap();
            (got, stats)
        }
    }

    fn decode_all(chunks: &[Vec<u8>]) -> Vec<Vec<u8>> {
        let mut out = Vec::new();
        for c in chunks {
            for_each_decoded_item(c, |i| {
                out.push(i.path_bytes().to_vec());
                Ok(())
            })
            .unwrap();
        }
        out
    }

    /// Lay out `segments` as the writer does: a forced cut at every segment
    /// start, then a cut after each entry where `cut` says so.
    fn lay_out(
        segments: &[(Vec<u8>, Vec<Vec<u8>>)],
        stride: u32,
        cut: &mut dyn FnMut() -> bool,
    ) -> Listing {
        let mut b = ListingIndexBuilder::with_stride(stride);
        let mut chunks: Vec<Vec<u8>> = Vec::new();
        let mut open = Vec::new();
        for (root, paths) in segments {
            if !open.is_empty() {
                chunks.push(std::mem::take(&mut open));
            }
            b.begin_segment(root.clone());
            for p in paths {
                b.observe(p, chunks.len(), open.is_empty());
                open.extend_from_slice(&encode(p));
                if cut() {
                    chunks.push(std::mem::take(&mut open));
                }
            }
        }
        if !open.is_empty() {
            chunks.push(open);
        }
        let index = b.finish(chunks.len());
        let all = decode_all(&chunks);
        Listing { chunks, index, all }
    }

    struct Rng(u64);

    impl Rng {
        fn next(&mut self) -> u64 {
            self.0 ^= self.0 << 13;
            self.0 ^= self.0 >> 7;
            self.0 ^= self.0 << 17;
            self.0
        }

        fn below(&mut self, n: u64) -> u64 {
            self.next() % n
        }
    }

    const ALPHABET: &[u8] = b"ab.- \\\x80\xff";

    /// A random tree in walker order: a directory's entries in path-byte
    /// order, then each subdirectory the same way. Paths relative to
    /// `prefix`; directories also go to `dirs`.
    fn gen_tree(
        rng: &mut Rng,
        prefix: &[u8],
        depth: usize,
        out: &mut Vec<Vec<u8>>,
        dirs: &mut Vec<Vec<u8>>,
    ) {
        let n = if rng.below(8) == 0 {
            20 + rng.below(40)
        } else {
            rng.below(6)
        };
        let mut names = std::collections::BTreeSet::new();
        for _ in 0..n {
            let len = 1 + rng.below(3) as usize;
            let name: Vec<u8> = (0..len)
                .map(|_| ALPHABET[rng.below(ALPHABET.len() as u64) as usize])
                .collect();
            names.insert(name);
        }
        let mut subdirs = Vec::new();
        for name in names {
            let mut p = prefix.to_vec();
            if !p.is_empty() {
                p.push(b'/');
            }
            p.extend_from_slice(&name);
            out.push(p.clone());
            if depth < 4 && rng.below(3) == 0 {
                dirs.push(p.clone());
                subdirs.push(p);
            }
        }
        for d in subdirs {
            gen_tree(rng, &d, depth + 1, out, dirs);
        }
    }

    const PREFIXES: &[&[u8]] = &[b"home/u/b", b"home/u/a", b"srv", b"home/v", b"a.b/c"];

    /// A random listing (optional dumps, then one SkipRoot root or 1-3
    /// EmitRoot roots in non-sorted order) and the queries to try on it.
    fn random_listing(seed: u64) -> (Listing, Vec<Vec<u8>>) {
        let mut rng = Rng(seed | 1);
        let mut segments = Vec::new();
        let mut queries: Vec<Vec<u8>> =
            vec![Vec::new(), b"zz/none".to_vec(), b"vykar-dumps".to_vec()];
        if rng.below(2) == 0 {
            let mut d = vec![b"vykar-dumps".to_vec()];
            for name in ["alpha", "zeta", "m"]
                .iter()
                .take(1 + rng.below(3) as usize)
            {
                d.push(format!("vykar-dumps/{name}").into_bytes());
            }
            d[1..].sort();
            segments.push((b"vykar-dumps".to_vec(), d));
        }
        if rng.below(2) == 0 {
            let (mut paths, mut dirs) = (Vec::new(), Vec::new());
            gen_tree(&mut rng, b"", 0, &mut paths, &mut dirs);
            queries.extend(paths.iter().take(3).cloned());
            queries.extend(dirs);
            segments.push((Vec::new(), paths));
        } else {
            let mut prefixes: Vec<&[u8]> = PREFIXES.to_vec();
            for i in (1..prefixes.len()).rev() {
                prefixes.swap(i, rng.below(i as u64 + 1) as usize);
            }
            for prefix in prefixes.iter().take(1 + rng.below(3) as usize) {
                let (mut paths, mut dirs) = (vec![prefix.to_vec()], vec![prefix.to_vec()]);
                gen_tree(&mut rng, prefix, 0, &mut paths, &mut dirs);
                // Every ancestor of the prefix.
                for (i, b) in prefix.iter().enumerate() {
                    if *b == b'/' {
                        queries.push(prefix[..i].to_vec());
                    }
                }
                queries.extend(paths.iter().skip(1).take(2).cloned());
                queries.extend(dirs);
                segments.push((prefix.to_vec(), paths));
            }
        }
        let stride = 1 + rng.below(4) as u32;
        let cut_rate = 1 + rng.below(6);
        let mut cut = || rng.next().is_multiple_of(cut_rate);
        let listing = lay_out(&segments, stride, &mut cut);
        (listing, queries)
    }

    fn queries(dirs: &[Vec<u8>]) -> impl Iterator<Item = Query<'_>> {
        dirs.iter()
            .take(80)
            .flat_map(|d| [Query::DirEntries(d), Query::Subtree(d)])
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(64))]

        #[test]
        fn index_path_equals_scan(seed in any::<u64>()) {
            let (listing, dirs) = random_listing(seed);
            prop_assert_eq!(listing.index.order, crate::snapshot::listing::ORDER_DFS_NAME);
            for q in queries(&dirs) {
                let (outcome, got, stats, _) = listing.index_path(q);
                prop_assert_eq!(&outcome, &Outcome::Done, "{:?}", q);
                prop_assert_eq!(&got, &listing.oracle(q), "{:?}", q);
                prop_assert_eq!(stats.matches, got.len());
            }
        }
    }

    #[test]
    fn corrupted_keys_still_equal_the_scan() {
        let mut fallbacks = 0usize;
        for seed in 0..48u64 {
            let (mut listing, dirs) = random_listing(seed.wrapping_mul(0x9E37_79B9_7F4A_7C15));
            let n = listing.index.keys.len();
            if n < 2 {
                continue;
            }
            let mut rng = Rng(seed + 7);
            let j = 1 + rng.below(n as u64 - 1) as usize;
            let other = rng.below(n as u64) as usize;
            listing.index.keys[j] = match seed % 4 {
                0 => ByteBuf::from(
                    (0..rng.below(6))
                        .map(|_| rng.next() as u8)
                        .collect::<Vec<u8>>(),
                ),
                1 => ByteBuf::new(),
                2 => listing.index.keys[other.max(j)].clone(),
                _ => listing.index.keys[other.min(j)].clone(),
            };
            for q in queries(&dirs) {
                let (got, stats) = listing.run_paths(q);
                assert_eq!(got, listing.oracle(q), "seed {seed} {q:?}");
                fallbacks += usize::from(stats.fallback.is_some());
            }
        }
        assert!(fallbacks > 0, "no corrupted key was ever noticed");
    }

    fn one_per_chunk(paths: &[&[u8]], stride: u32) -> Listing {
        lay_out(
            &[(Vec::new(), paths.iter().map(|p| p.to_vec()).collect())],
            stride,
            &mut || true,
        )
    }

    #[test]
    fn duplicates_straddling_a_grid_boundary() {
        let listing = one_per_chunk(&[b"a", b"b", b"b", b"c", b"b/x"], 1);
        assert_eq!(
            listing.index.order,
            crate::snapshot::listing::ORDER_DFS_NAME
        );
        for (q, want) in [
            (Query::DirEntries(b""), vec![&b"a"[..], b"b", b"b", b"c"]),
            (Query::Subtree(b"b"), vec![&b"b"[..], b"b", b"b/x"]),
            (Query::DirEntries(b"b"), vec![&b"b/x"[..]]),
        ] {
            let (outcome, got, _, _) = listing.index_path(q);
            assert_eq!(outcome, Outcome::Done);
            assert_eq!(got, want, "{q:?}");
        }
    }

    #[test]
    fn segments_sharing_a_root_come_out_in_stream_order() {
        let segments = vec![
            (
                b"vykar-dumps".to_vec(),
                vec![b"vykar-dumps".to_vec(), b"vykar-dumps/a".to_vec()],
            ),
            (
                Vec::new(),
                vec![
                    b"b".to_vec(),
                    b"vykar-dumps".to_vec(),
                    b"vykar-dumps/x".to_vec(),
                ],
            ),
        ];
        let listing = lay_out(&segments, 1, &mut || true);
        assert_eq!(
            listing.index.order,
            crate::snapshot::listing::ORDER_DFS_NAME
        );
        for (q, want) in [
            (
                Query::DirEntries(b"vykar-dumps"),
                vec![&b"vykar-dumps/a"[..], b"vykar-dumps/x"],
            ),
            (
                Query::Subtree(b"vykar-dumps"),
                vec![
                    &b"vykar-dumps"[..],
                    b"vykar-dumps/a",
                    b"vykar-dumps",
                    b"vykar-dumps/x",
                ],
            ),
            (
                Query::DirEntries(b""),
                vec![&b"vykar-dumps"[..], b"b", b"vykar-dumps"],
            ),
        ] {
            let (outcome, got, _, _) = listing.index_path(q);
            assert_eq!(outcome, Outcome::Done);
            assert_eq!(got, want, "{q:?}");
            assert_eq!(got, listing.oracle(q));
        }
    }

    /// "a0".."a9", "d", "d/f00".."d/f19", one entry per chunk, stride 4.
    fn d_listing() -> Listing {
        let mut paths: Vec<Vec<u8>> = (0..10).map(|i| format!("a{i}").into_bytes()).collect();
        paths.push(b"d".to_vec());
        paths.extend((0..20).map(|i| format!("d/f{i:02}").into_bytes()));
        lay_out(&[(Vec::new(), paths)], 4, &mut || true)
    }

    #[test]
    fn fallback_after_partial_delivery_delivers_each_entry_once() {
        let mut listing = d_listing();
        let q = Query::DirEntries(b"d");
        // Chunk 16 held d/f05: put d/f00 there, out of order in a later window.
        listing.replace_chunk(16, b"d/f00");
        let (outcome, got, _, _) = listing.index_path(q);
        assert!(matches!(outcome, Outcome::Fallback(_)), "{outcome:?}");
        assert!(!got.is_empty(), "nothing was delivered before the fallback");
        let (got, stats) = listing.run_paths(q);
        assert!(stats.fallback.is_some());
        assert!(!stats.via_index);
        assert_eq!(got, listing.oracle(q));
        assert_eq!(stats.matches, got.len());
    }

    #[test]
    fn contradiction_after_delivery_is_an_error() {
        let mut listing = d_listing();
        let q = Query::DirEntries(b"d");
        // A match in a chunk the index path skips, out of order ...
        listing.replace_chunk(2, b"d/f03");
        // ... and a violation it notices after delivering some results.
        listing.replace_chunk(16, b"d/f00");
        let (outcome, got, _, _) = listing.index_path(q);
        assert!(matches!(outcome, Outcome::Fallback(_)));
        assert!(!got.is_empty());
        let err = listing.run(q, &mut |_| Ok(())).unwrap_err();
        assert!(matches!(err, VykarError::InvalidFormat(_)), "{err:?}");
    }

    #[test]
    fn visitor_error_is_returned_not_a_fallback() {
        let listing = d_listing();
        let mut n = 0;
        let err = listing
            .run(Query::DirEntries(b"d"), &mut |_| {
                n += 1;
                if n == 2 {
                    Err(VykarError::Other("visitor stops".into()))
                } else {
                    Ok(())
                }
            })
            .unwrap_err();
        assert!(
            matches!(&err, VykarError::Other(m) if m == "visitor stops"),
            "{err:?}"
        );
        assert_eq!(n, 2);
    }

    #[test]
    fn windows_are_bounded_whatever_the_stride() {
        let paths: Vec<Vec<u8>> = (0..3000).map(|i| format!("f{i:04}").into_bytes()).collect();
        let listing = lay_out(&[(Vec::new(), paths)], 1024, &mut || true);
        for q in [
            Query::Subtree(b"f1500"),
            Query::DirEntries(b""),
            Query::Subtree(b"f2999"),
        ] {
            let (outcome, got, _, width) = listing.index_path(q);
            assert_eq!(outcome, Outcome::Done);
            assert_eq!(got, listing.oracle(q));
            assert!(width <= MAX_WINDOW, "{q:?}: read {width} chunks at once");
        }
    }

    #[test]
    fn no_index_scans() {
        let listing = d_listing();
        let mut spans = MemSpans {
            chunks: &listing.chunks,
            max_width: 0,
        };
        let chunks = &listing.chunks;
        let mut got = Vec::new();
        let stats = run_query(
            Err(&NoIndex::Unordered),
            chunks.len(),
            Query::DirEntries(b"d"),
            &mut spans,
            &CancelToken::new(),
            &mut |f| {
                for c in chunks {
                    for_each_decoded_item(c, &mut *f)?;
                }
                Ok(())
            },
            &mut |i| {
                got.push(i.path_bytes().to_vec());
                Ok(())
            },
        )
        .unwrap();
        assert_eq!(spans.max_width, 0);
        assert!(stats.fallback.is_some());
        assert_eq!(got, listing.oracle(Query::DirEntries(b"d")));
    }

    /// "a", "b", "a/x0".."a/x9", "b/y0".."b/y2", "c/z", one entry per
    /// chunk, stride 4: "b/y0" is chunk 12, whose key is "b/".
    fn b_listing() -> Listing {
        let mut paths: Vec<Vec<u8>> = vec![b"a".to_vec(), b"b".to_vec()];
        paths.extend((0..10).map(|i| format!("a/x{i}").into_bytes()));
        paths.extend((0..3).map(|i| format!("b/y{i}").into_bytes()));
        paths.push(b"c/z".to_vec());
        lay_out(&[(Vec::new(), paths)], 4, &mut || true)
    }

    #[test]
    fn children_starting_a_grid_chunk_read_one_span() {
        let listing = b_listing();
        assert_eq!(listing.index.keys[3].as_slice(), b"b/");
        for q in [Query::DirEntries(b"b"), Query::Subtree(b"b")] {
            let (outcome, got, stats, _) = listing.index_path(q);
            assert_eq!(outcome, Outcome::Done, "{q:?}");
            assert_eq!(got, listing.oracle(q), "{q:?}");
            if let Query::DirEntries(_) = q {
                // The predecessor (chunk 11) and the span from chunk 12 to
                // the listing's end. The strict search started at chunk 8
                // and read 9 chunks.
                assert!(stats.chunks_read <= 4 + 1, "{} chunks", stats.chunks_read);
            }
        }
    }

    #[test]
    fn a_key_equal_to_the_bound_is_still_checked() {
        let q = Query::DirEntries(b"b");
        // A key equal to the bound one grid span too early: chunk 8's first
        // entry ("a/x6") sorts before it.
        let mut listing = b_listing();
        listing.index.keys[2] = ByteBuf::from(b"b/".to_vec());
        listing.index.keys[3] = ByteBuf::from(b"c/".to_vec());
        let (got, stats) = listing.run_paths(q);
        assert_eq!(got, listing.oracle(q));
        assert!(stats.fallback.is_some() && !stats.via_index);
        // The right key, but the predecessor chunk holds an entry of the
        // range.
        let mut listing = b_listing();
        listing.replace_chunk(11, b"b/y0");
        let (outcome, _, _, _) = listing.index_path(q);
        assert_eq!(
            outcome,
            Outcome::Fallback("predecessor chunk reaches the queried range".into())
        );
        let (got, stats) = listing.run_paths(q);
        assert_eq!(got, listing.oracle(q));
        assert_eq!(got.iter().filter(|p| p.as_slice() == b"b/y0").count(), 2);
        assert!(stats.fallback.is_some() && !stats.via_index);
    }

    #[test]
    fn absorb_sums_and_keeps_the_first_fallback() {
        let mut total = ListingRead::default();
        let one = |via_index: bool, fallback: Option<&str>| ListingRead {
            via_index,
            fallback: fallback.map(String::from),
            chunks_read: 2,
            stored_bytes: 10,
            plain_bytes: 20,
            match_chunks: 1,
            matches: 3,
            queries: 1,
        };
        total.absorb(one(true, None));
        assert!(total.via_index);
        total.absorb(one(false, Some("first")));
        total.absorb(one(false, Some("second")));
        total.absorb(one(true, None));
        assert!(!total.via_index);
        assert_eq!(total.fallback.as_deref(), Some("first"));
        assert_eq!(
            (
                total.chunks_read,
                total.stored_bytes,
                total.queries,
                total.matches
            ),
            (8, 40, 4, 12)
        );
    }

    #[test]
    fn trim_dir_strips_slashes() {
        assert_eq!(trim_dir(b"/a/b/"), b"a/b");
        assert_eq!(trim_dir(b"///"), b"");
        assert_eq!(trim_dir(b""), b"");
        assert_eq!(trim_dir(b"a"), b"a");
    }
}
