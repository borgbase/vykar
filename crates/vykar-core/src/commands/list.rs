use std::io::{BufRead, Read};
use std::sync::Arc;

use serde::Deserialize;
use tracing::info;

use crate::config::VykarConfig;
use crate::repo::control::CancelToken;
use crate::repo::format::{
    BlobLocation, ObjectType, decode_chunk_into, unpack_object_expect_with_context,
};
use crate::repo::manifest::SnapshotEntry;
use crate::repo::relocate::{self, MAX_RELOCATE_ROUNDS, RelocationSource, pack_not_found};
use crate::repo::tree_cache::TreeChunkCache;
use crate::repo::{Repo, TreeChunkSource};
use crate::snapshot::SnapshotMeta;
use crate::snapshot::item::Item;
use vykar_crypto::CryptoEngine;
use vykar_storage::StorageBackend;
use vykar_types::chunk_id::ChunkId;
use vykar_types::error::{Result, VykarError};
use vykar_types::pack_id::PackId;
use vykar_types::snapshot_id::SnapshotId;

use super::listing::{ListingRead, ListingView};
use super::mark::{Located, cut_batches_with, push_spans};
use super::util::open_repo;
use crate::repo::OpenOptions;

/// List all snapshots in the repository.
pub fn list_snapshots(
    config: &VykarConfig,
    passphrase: Option<&str>,
) -> Result<Vec<SnapshotEntry>> {
    let repo = open_repo(config, passphrase, OpenOptions::new(), &CancelToken::new())?;
    Ok(repo.manifest().snapshots.clone())
}

/// List all items in a specific snapshot, reading the listing a batch of
/// tree chunks at a time and decoding each tree chunk on its own.
/// Resolves the item stream through the local chunk table where there is
/// one, so no index download is needed.
pub fn list_snapshot_items(
    config: &VykarConfig,
    passphrase: Option<&str>,
    snapshot_name: &str,
) -> Result<Vec<Item>> {
    list_snapshot_items_inner(config, passphrase, snapshot_name).map(|(items, _)| items)
}

/// Like [`list_snapshot_items`], but also returns the `source_paths` from
/// the snapshot metadata so the caller can reconstruct absolute paths.
pub fn list_snapshot_items_with_source_paths(
    config: &VykarConfig,
    passphrase: Option<&str>,
    snapshot_name: &str,
) -> Result<(Vec<Item>, Vec<String>)> {
    list_snapshot_items_inner(config, passphrase, snapshot_name)
}

fn list_snapshot_items_inner(
    config: &VykarConfig,
    passphrase: Option<&str>,
    snapshot_name: &str,
) -> Result<(Vec<Item>, Vec<String>)> {
    let mut items = Vec::new();
    let source_paths = for_each_listed_item_inner(config, passphrase, snapshot_name, |item| {
        items.push(item);
        Ok(())
    })?;
    Ok((items, source_paths))
}

/// Call `visit` for each item of snapshot `snapshot_name`, in stream order,
/// without holding the listing in memory: tree chunks are read in bounded
/// batches (the local tree-chunk cache first) and each is decoded on its own.
/// Resolves the item stream through the local chunk table where there is one.
pub fn for_each_listed_item(
    config: &VykarConfig,
    passphrase: Option<&str>,
    snapshot_name: &str,
    visit: impl FnMut(Item) -> Result<()>,
) -> Result<()> {
    for_each_listed_item_inner(config, passphrase, snapshot_name, visit).map(|_| ())
}

/// [`for_each_listed_item`], also returning the snapshot's `source_paths`.
fn for_each_listed_item_inner(
    config: &VykarConfig,
    passphrase: Option<&str>,
    snapshot_name: &str,
    visit: impl FnMut(Item) -> Result<()>,
) -> Result<Vec<String>> {
    let (repo, meta, origin) = open_listing(config, passphrase, snapshot_name)?;
    for_each_item_by_chunk(
        &repo,
        &meta.item_ptrs,
        TreeChunkSource::CacheFirst,
        LISTING_BATCH_CHUNKS,
        LISTING_BATCH_STORED_BYTES,
        &CancelToken::new(),
        visit,
    )
    .map_err(|e| origin.explain(&repo, e))?;
    Ok(meta.source_paths)
}

/// Open the repository, resolve `snapshot_name`, load its meta and the
/// chunk index. Also returns where the read started, for
/// [`ReadOrigin::explain`].
fn open_listing(
    config: &VykarConfig,
    passphrase: Option<&str>,
    snapshot_name: &str,
) -> Result<(Repo, SnapshotMeta, ReadOrigin)> {
    let mut repo = open_repo(config, passphrase, OpenOptions::new(), &CancelToken::new())?;

    let resolved_name = repo
        .manifest()
        .resolve_snapshot(snapshot_name)?
        .name
        .clone();

    let origin = ReadOrigin::of(&repo, &resolved_name)?;
    let meta = load_snapshot_meta(&repo, &resolved_name)?;
    ItemStreamReader::new(&repo).ensure_index(&mut repo)?;
    Ok((repo, meta, origin))
}

/// Call `visit` for every item whose display path (`Item::path`) equals
/// `dir` or starts with `dir + "/"`, in stream order: the filter of
/// `vykar snapshot list --path`. An empty `dir` is every item
/// ([`for_each_listed_item`]). Reads only the tree chunks of that subtree
/// when the snapshot's key index is usable and `dir` is a clean path
/// without U+FFFD; otherwise the whole listing, a batch of tree chunks at
/// a time.
pub fn for_each_listed_item_under(
    config: &VykarConfig,
    passphrase: Option<&str>,
    snapshot_name: &str,
    dir: &str,
    visit: impl FnMut(Item) -> Result<()>,
) -> Result<()> {
    if dir.is_empty() {
        return for_each_listed_item(config, passphrase, snapshot_name, visit);
    }
    let (repo, meta, origin) = open_listing(config, passphrase, snapshot_name)?;
    list_under_on_repo(&repo, &meta, dir, &CancelToken::new(), visit)
        .map(|_| ())
        .map_err(|e| origin.explain(&repo, e))
}

/// Where a read of one snapshot started: the generation of the head it
/// was opened at and the snapshot.
///
/// A snapshot deleted and compacted between the open and the index load
/// finds its chunks gone from the index the load installed (it re-read a
/// newer head because a fold removed an object of the open one), and fails
/// with [`VykarError::ChunkNotInIndex`]. [`Self::explain`] tells that
/// apart from damage.
#[derive(Clone, Copy, Debug)]
pub(crate) struct ReadOrigin {
    opened_at: u64,
    snapshot: SnapshotId,
}

impl ReadOrigin {
    /// Before the index load: the handle's head generation and the
    /// snapshot `snapshot_name` names.
    pub(crate) fn of(repo: &Repo, snapshot_name: &str) -> Result<Self> {
        let entry = repo
            .manifest()
            .find_snapshot(snapshot_name)
            .ok_or_else(|| VykarError::SnapshotNotFound(snapshot_name.into()))?;
        Ok(Self {
            opened_at: repo.index_generation(),
            snapshot: entry.id,
        })
    }

    /// `e`, or [`VykarError::ChunkRemovedDuringRead`] in its place (wrapped
    /// as `e` was) when `e` is a chunk missing from the index, the index
    /// was loaded at a head newer than the open one, and the snapshot is no
    /// longer stored (one existence probe). Otherwise it is damage and `e`
    /// stays.
    pub(crate) fn explain(&self, repo: &Repo, e: VykarError) -> VykarError {
        let chunk_id = match &e {
            VykarError::ChunkNotInIndex(id) => *id,
            VykarError::RestoreSourceUnreadable(inner) => match **inner {
                VykarError::ChunkNotInIndex(id) => id,
                _ => return e,
            },
            _ => return e,
        };
        if repo.index_generation() <= self.opened_at
            || !matches!(
                repo.storage().exists(&self.snapshot.storage_key()),
                Ok(false)
            )
        {
            return e;
        }
        let removed = VykarError::ChunkRemovedDuringRead { chunk_id };
        match e {
            VykarError::RestoreSourceUnreadable(_) => {
                VykarError::RestoreSourceUnreadable(Box::new(removed))
            }
            _ => removed,
        }
    }
}

/// How [`list_under_on_repo`] read, for the debug line and tests.
#[derive(Debug)]
#[cfg_attr(not(test), allow(dead_code))] // the counts are read by tests
pub(crate) enum ListUnderRead {
    Index(ListingRead),
    Whole(ChunkedListingStats),
}

/// Whether a display path can be narrowed to the raw-byte subtree of the
/// same bytes: it is clean (no empty component, no leading or trailing
/// "/") and holds no U+FFFD, so every display path equal to it or under it
/// has a raw path equal to it or under it.
pub(crate) fn narrowable_display_path(path: &str) -> bool {
    !path.contains('\u{FFFD}') && crate::snapshot::listing::is_clean(path.as_bytes())
}

/// Whether display path `path` is `dir` or under `dir + "/"`.
pub(crate) fn display_path_under(path: &str, dir: &str) -> bool {
    path.strip_prefix(dir)
        .is_some_and(|rest| rest.is_empty() || rest.starts_with('/'))
}

/// [`for_each_listed_item_under`] on an open repository whose chunk index
/// is loaded; `dir` non-empty.
///
/// Every pointer of the listing is resolved first, so a pointer the chunk
/// index lacks fails with [`VykarError::ChunkNotInIndex`] even where the
/// subtree read would not reach it.
pub(crate) fn list_under_on_repo(
    repo: &Repo,
    meta: &SnapshotMeta,
    dir: &str,
    shutdown: &CancelToken,
    mut visit: impl FnMut(Item) -> Result<()>,
) -> Result<ListUnderRead> {
    resolve_from_index(repo.chunk_index(), &meta.item_ptrs)?;
    let mut keep = |item: Item| {
        if display_path_under(&item.path, dir) {
            visit(item)
        } else {
            Ok(())
        }
    };
    let view = ListingView::new(meta);
    let read = if narrowable_display_path(dir) && view.index().is_ok() {
        let stats = view.subtree(repo, dir.as_bytes(), shutdown, &mut keep)?;
        tracing::debug!(
            "list under: index (via_index {}, fallback {:?}, {} tree chunks, {} B stored, {} matches)",
            stats.via_index,
            stats.fallback,
            stats.chunks_read,
            stats.stored_bytes,
            stats.matches
        );
        ListUnderRead::Index(stats)
    } else {
        let stats = for_each_item_by_chunk(
            repo,
            &meta.item_ptrs,
            TreeChunkSource::CacheFirst,
            LISTING_BATCH_CHUNKS,
            LISTING_BATCH_STORED_BYTES,
            shutdown,
            keep,
        )?;
        tracing::debug!("list under: whole listing ({} tree chunks)", stats.chunks);
        ListUnderRead::Whole(stats)
    };
    Ok(read)
}

/// Upper bound on tree chunks per read batch of [`for_each_item_by_chunk`].
pub(crate) const LISTING_BATCH_CHUNKS: usize = 2_000;
/// Upper bound on stored bytes per read batch of [`for_each_item_by_chunk`]:
/// the batch's stored bytes and their plaintext are what is resident besides
/// what the visitor keeps (about 2.5x this in total).
pub(crate) const LISTING_BATCH_STORED_BYTES: u64 = 16 << 20;

/// What [`for_each_item_by_chunk`] read, for debug lines and tests.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ChunkedListingStats {
    pub(crate) chunks: usize,
    pub(crate) batches: usize,
    pub(crate) stored_bytes: u64,
    pub(crate) plaintext_bytes: u64,
    pub(crate) items: u64,
}

/// Call `visit` for every item of the listing `item_ptrs`, in stream
/// order, reading it a batch of tree chunks at a time and decoding each
/// tree chunk on its own.
///
/// Every pointer is resolved through `repo.chunk_index()` before any read,
/// so a pointer the index lacks fails with [`VykarError::ChunkNotInIndex`]
/// even when its chunk is cached (as [`load_item_stream_from_ptrs`]).
/// A batch is at most `max_chunks` chunks and `max_stored` stored bytes
/// ([`LISTING_BATCH_CHUNKS`], [`LISTING_BATCH_STORED_BYTES`] outside
/// tests), read through [`Repo::read_tree_chunks_split`] from `source`.
/// Read errors are returned as the read returned them. A tree chunk that
/// does not decode on its own (one that ends inside an entry included) is
/// an error naming the chunk; errors from `visit` are returned unchanged.
pub(crate) fn for_each_item_by_chunk(
    repo: &Repo,
    item_ptrs: &[ChunkId],
    source: TreeChunkSource,
    max_chunks: usize,
    max_stored: u64,
    shutdown: &CancelToken,
    mut visit: impl FnMut(Item) -> Result<()>,
) -> Result<ChunkedListingStats> {
    shutdown.check()?;
    let located: Vec<Located> = resolve_from_index(repo.chunk_index(), item_ptrs)?
        .into_iter()
        .enumerate()
        .collect();

    let mut stats = ChunkedListingStats::default();
    let mut buf: Vec<u8> = Vec::new();
    let mut ends: Vec<usize> = Vec::new();
    let mut spans: Vec<(usize, std::ops::Range<usize>)> = Vec::new();
    let mut items: Vec<Item> = Vec::new();
    for batch in cut_batches_with(&located, max_chunks, max_stored) {
        shutdown.check()?;
        buf.clear();
        spans.clear();
        let locs: Vec<ChunkLoc> = batch.iter().map(|(_, loc)| *loc).collect();
        repo.read_tree_chunks_split(&locs, &mut buf, &mut ends, shutdown, source)?;
        push_spans(&mut spans, 0, 0, &ends, batch.len(), buf.len())?;
        stats.batches += 1;
        stats.chunks += batch.len();
        stats.stored_bytes += locs.iter().map(|loc| u64::from(loc.3)).sum::<u64>();
        stats.plaintext_bytes += buf.len() as u64;

        for (pos, range) in &spans {
            let id = locs.get(*pos).map(|loc| loc.0).ok_or_else(|| {
                VykarError::Other("internal error: tree chunk span outside its batch".into())
            })?;
            let plain = buf.get(range.clone()).ok_or_else(|| {
                VykarError::Other(format!(
                    "internal error: tree chunk {id} outside the read buffer"
                ))
            })?;
            // Decode the whole chunk before visiting, so a decode error and
            // an error of `visit` stay apart.
            items.clear();
            for_each_decoded_item(plain, |item| {
                items.push(item);
                Ok(())
            })
            .map_err(|e| {
                VykarError::Other(format!("tree chunk {id} does not decode on its own: {e}"))
            })?;
            for item in items.drain(..) {
                stats.items += 1;
                visit(item)?;
            }
        }
    }
    tracing::debug!(
        "listing by chunk: {} tree chunks in {} batches, {} B stored, {} B plaintext, {} items",
        stats.chunks,
        stats.batches,
        stats.stored_bytes,
        stats.plaintext_bytes,
        stats.items
    );
    Ok(stats)
}

/// A snapshot listing, plus any snapshot this binary could not read.
///
/// `hidden` is the honesty flag: when it is non-empty, `snapshots` is an
/// incomplete view of the repository and the caller must say so instead of
/// presenting a silently truncated list.
#[derive(Debug, Default)]
pub struct SnapshotListing {
    pub snapshots: Vec<(SnapshotEntry, Option<crate::snapshot::SnapshotStats>)>,
    pub hidden: Vec<crate::repo::snapshot_cache::SkippedSnapshot>,
}

/// List all snapshots with their stats (loaded from snapshot metadata).
/// Returns `None` for stats when the snapshot blob cannot be read or decrypted.
pub fn list_snapshots_with_stats(
    config: &VykarConfig,
    passphrase: Option<&str>,
) -> Result<SnapshotListing> {
    let repo = open_repo(config, passphrase, OpenOptions::new(), &CancelToken::new())?;
    let entries = repo.manifest().snapshots.clone();
    let mut snapshots = Vec::with_capacity(entries.len());
    for entry in entries {
        let stats = load_snapshot_meta(&repo, &entry.name)
            .ok()
            .map(|meta| meta.stats);
        snapshots.push((entry, stats));
    }
    Ok(SnapshotListing {
        snapshots,
        hidden: repo.skipped_snapshots().to_vec(),
    })
}

/// Get metadata for a specific snapshot.
pub fn get_snapshot_meta(
    config: &VykarConfig,
    passphrase: Option<&str>,
    snapshot_name: &str,
) -> Result<SnapshotMeta> {
    let repo = open_repo(config, passphrase, OpenOptions::new(), &CancelToken::new())?;
    let resolved_name = repo
        .manifest()
        .resolve_snapshot(snapshot_name)?
        .name
        .clone();
    load_snapshot_meta(&repo, &resolved_name)
}

/// Load the SnapshotMeta for a snapshot by name.
pub fn load_snapshot_meta(repo: &Repo, snapshot_name: &str) -> Result<SnapshotMeta> {
    let entry = repo
        .manifest()
        .find_snapshot(snapshot_name)
        .ok_or_else(|| VykarError::SnapshotNotFound(snapshot_name.into()))?;

    let meta_data = repo
        .storage()
        .get(&entry.id.storage_key())?
        .ok_or_else(|| VykarError::SnapshotNotFound(snapshot_name.into()))?;
    decode_snapshot_meta(&entry.id, &meta_data, repo.crypto().as_ref())
}

/// Authenticate and decode the stored object of snapshot `id`, refusing a
/// foreign envelope ([`VykarError::IncompatibleSnapshotEnvelope`]) and a
/// format newer than this build ([`VykarError::UnsupportedSnapshotVersion`]).
pub(crate) fn decode_snapshot_meta(
    id: &SnapshotId,
    blob: &[u8],
    crypto: &dyn CryptoEngine,
) -> Result<SnapshotMeta> {
    let meta_bytes =
        unpack_object_expect_with_context(blob, ObjectType::SnapshotMeta, id.as_bytes(), crypto)?;
    // A length mismatch here is a foreign envelope, not corruption: decryption
    // already authenticated the blob. Report it as such so the user gets the
    // remedy instead of a raw serde error.
    let meta: SnapshotMeta = rmp_serde::from_slice(&meta_bytes).map_err(|e| {
        match crate::repo::snapshot_cache::classify_decode_error(&e, &meta_bytes) {
            reason @ crate::repo::snapshot_cache::SkipReason::IncompatibleEnvelope { .. } => {
                VykarError::IncompatibleSnapshotEnvelope {
                    detail: reason.to_string(),
                }
            }
            _ => VykarError::Deserialization(e),
        }
    })?;
    // Fail closed on snapshots written by a newer format than we support so
    // every item-reading op (restore, ls, diff, find, mount) refuses rather
    // than misinterpreting a future layout. Snapshot *listing* reads the
    // manifest, not this function, so it is unaffected.
    meta.supported()?;
    Ok(meta)
}

/// Load and deserialize all items from a snapshot.
pub fn load_snapshot_items(
    repo: &mut Repo,
    snapshot_name: &str,
    shutdown: &CancelToken,
) -> Result<Vec<Item>> {
    let items_stream = load_snapshot_item_stream(repo, snapshot_name, shutdown)?;
    decode_items_stream(&items_stream)
}

/// Load the raw concatenated item stream bytes for a snapshot, reading tree
/// chunks from the local tree-chunk cache first.
pub fn load_snapshot_item_stream(
    repo: &mut Repo,
    snapshot_name: &str,
    shutdown: &CancelToken,
) -> Result<Vec<u8>> {
    let snapshot_meta = load_snapshot_meta(repo, snapshot_name)?;
    load_item_stream_from_ptrs(repo, &snapshot_meta.item_ptrs, shutdown)
}

/// [`load_snapshot_item_stream`], reading every tree chunk from storage (never
/// the local tree-chunk cache, which is not filled either). For commands that
/// verify the repository.
pub fn load_snapshot_item_stream_from_storage(
    repo: &mut Repo,
    snapshot_name: &str,
    shutdown: &CancelToken,
) -> Result<Vec<u8>> {
    let snapshot_meta = load_snapshot_meta(repo, snapshot_name)?;
    resolve_and_read(
        repo,
        &snapshot_meta.item_ptrs,
        resolve_through_index,
        shutdown,
        TreeChunkSource::Storage,
    )
}

/// Item-stream reader for one read-only operation: resolves item-stream
/// chunks through the chunk index of the snapshot view, loaded at most once
/// (from the local chunk table when it is at the open head, so without an
/// index download).
pub struct ItemStreamReader {
    index_loaded: bool,
}

impl ItemStreamReader {
    pub fn new(_repo: &Repo) -> Self {
        Self {
            index_loaded: false,
        }
    }

    /// Load the raw item stream bytes for `snapshot_name`.
    pub fn load(
        &mut self,
        repo: &mut Repo,
        snapshot_name: &str,
        shutdown: &CancelToken,
    ) -> Result<Vec<u8>> {
        shutdown.check()?;
        self.ensure_index(repo)?;
        load_snapshot_item_stream(repo, snapshot_name, shutdown)
    }

    /// Open the item stream of `snapshot_name` for lazy, chunk-at-a-time
    /// decoding (see [`LazyItemStream`]) instead of materializing it.
    ///
    /// Chunk locations are resolved up front through the chunk index (loaded
    /// at most once). The first range read happens here, so a cancellation or
    /// an unreadable stream head fails before the caller mutates anything.
    /// (A restore of selected subtrees reads through
    /// [`crate::commands::listing::ListingView`] instead: it resolves every
    /// pointer up front, but its first listing read happens after the
    /// destination is prepared.)
    pub fn open_lazy<'a>(
        &mut self,
        repo: &mut Repo,
        snapshot_name: &str,
        shutdown: &'a CancelToken,
    ) -> Result<LazyItemStream<'a>> {
        shutdown.check()?;
        let meta = load_snapshot_meta(repo, snapshot_name)?;
        self.open_lazy_ptrs(repo, &meta.item_ptrs, shutdown)
    }

    /// [`Self::open_lazy`] over explicit item pointers (a meta the caller
    /// already holds).
    pub fn open_lazy_ptrs<'a>(
        &mut self,
        repo: &mut Repo,
        item_ptrs: &[ChunkId],
        shutdown: &'a CancelToken,
    ) -> Result<LazyItemStream<'a>> {
        shutdown.check()?;
        self.ensure_index(repo)?;
        LazyItemStream::open(repo, item_ptrs, shutdown)
    }

    /// Load the full chunk index unless this reader already did.
    pub fn ensure_index(&mut self, repo: &mut Repo) -> Result<()> {
        if !self.index_loaded {
            repo.load_chunk_index_for_snapshot_view()?;
            self.index_loaded = true;
        }
        Ok(())
    }

    /// Resolve a chunk to `(pack_id, pack_offset, stored_size)` through the
    /// chunk index, which must be loaded ([`Self::ensure_index`]).
    pub fn lookup(&self, repo: &Repo, id: &ChunkId) -> Option<(PackId, u64, u32)> {
        repo.chunk_index()
            .get(id)
            .map(|e| (e.pack_id, e.pack_offset, e.stored_size))
    }

    /// Whether this reader loaded the full chunk index.
    pub fn index_loaded(&self) -> bool {
        self.index_loaded
    }
}

/// `(chunk_id, pack_id, pack_offset, stored_size)` of one item-stream chunk.
pub(crate) type ChunkLoc = (ChunkId, PackId, u64, u32);

/// Upper bound on one range read of [`LazyItemStream`]. Smaller than the
/// eager reader's 16 MiB coalescing cap because the raw window stays resident
/// while its chunks are decoded; item-stream chunks average about 32 KiB
/// plaintext, so one window still spans dozens of them.
const LAZY_WINDOW_MAX: u64 = 4 * 1024 * 1024;

/// Largest gap between two stream-consecutive chunks of one pack that is
/// still read through rather than split into two range reads.
const LAZY_WINDOW_GAP: u64 = 256 * 1024;

pub(crate) fn resolve_from_index(
    index: &crate::index::LiveIndex,
    ids: &[ChunkId],
) -> Result<Vec<ChunkLoc>> {
    ids.iter()
        .map(|id| {
            let e = index.get(id).ok_or(VykarError::ChunkNotInIndex(*id))?;
            Ok((*id, e.pack_id, e.pack_offset, e.stored_size))
        })
        .collect()
}

/// A snapshot's item stream read on demand: only the current range read
/// (at most `LAZY_WINDOW_MAX`, 4 MiB) and the plaintext of the chunk being decoded
/// are resident, instead of the whole decompressed stream. Every tree chunk
/// holds whole entries (see `ListingWriter`); this reader does not rely on it
/// and decodes across chunk boundaries through its [`BufRead`] impl.
///
/// A chunk held by the local tree-chunk cache is decoded from there on its
/// own, and windows stop before such a chunk, so it is not downloaded;
/// downloaded chunks are written to the cache.
///
/// Holds its own storage and crypto handles so the caller can keep using the
/// repository mutably while the stream is being consumed. Every chunk is
/// authenticated (AEAD) or, for unencrypted repositories, checked against its
/// chunk ID, exactly like [`Repo::read_chunks_coalesced_into`].
///
/// A pack that answers 404 (a concurrent compact repacked or removed it
/// after the index was loaded) is re-resolved against the latest head
/// (`repo::relocate`), on every such 404: the remaining chunks of that pack
/// are looked up again and the read retried where the latest head stores
/// them; a chunk the latest head no longer has fails the stream with
/// [`VykarError::ChunkRemovedDuringRead`]. Any other failure to read or
/// decode a chunk at its location re-resolves the remaining locations once
/// through the index at a freshly read head, downloaded and dropped right
/// after, and the read is retried.
pub struct LazyItemStream<'a> {
    storage: Arc<dyn StorageBackend>,
    crypto: Arc<dyn CryptoEngine>,
    shutdown: &'a CancelToken,
    chunks: Vec<ChunkLoc>,
    /// Re-resolves the chunks of a pack that answered 404.
    relocation: RelocationSource,
    /// The last error came from the 404 path, which re-resolved already.
    pack_missing: bool,
    /// The local tree-chunk cache: read before a window is planned, filled
    /// from every window. `None` reads storage only.
    cache: Option<TreeChunkCache>,
    /// Chunks decoded from the cache, for the end-of-stream debug line.
    cache_hits: usize,
    /// Whether the locations were already re-resolved once.
    retried: bool,
    /// Range-read cap; [`LAZY_WINDOW_MAX`] outside tests.
    window_max: u64,
    /// Index into `chunks` of the next chunk to decode.
    next: usize,
    /// Raw bytes of the current range read, starting at pack offset
    /// `window_start` and covering `chunks[next..window_end]`.
    window: Vec<u8>,
    window_start: u64,
    window_end: usize,
    /// Plaintext of the chunk being decoded and the read position in it.
    cur: Vec<u8>,
    pos: usize,
    /// The repository error behind the last failed read; `io::Error` cannot
    /// carry it through the msgpack decoder, so it is recovered from here.
    error: Option<VykarError>,
    /// Range reads issued, for the end-of-stream debug line.
    reads: usize,
    done: bool,
}

impl<'a> LazyItemStream<'a> {
    /// Open the stream of `item_ptrs` on a repository whose chunk index is
    /// already loaded ([`ItemStreamReader::ensure_index`], or a repository
    /// opened with `OpenOptions::with_index`). Resolves every pointer, then
    /// performs the first read, as [`ItemStreamReader::open_lazy_ptrs`].
    pub(crate) fn open(
        repo: &Repo,
        item_ptrs: &[ChunkId],
        shutdown: &'a CancelToken,
    ) -> Result<LazyItemStream<'a>> {
        shutdown.check()?;
        let chunks = resolve_from_index(repo.chunk_index(), item_ptrs)?;

        let mut stream = LazyItemStream {
            storage: Arc::clone(repo.storage()),
            crypto: Arc::clone(repo.crypto()),
            shutdown,
            chunks,
            relocation: repo.relocation_source(),
            pack_missing: false,
            cache: repo.tree_cache().cloned(),
            cache_hits: 0,
            retried: false,
            window_max: LAZY_WINDOW_MAX,
            next: 0,
            window: Vec::new(),
            window_start: 0,
            window_end: 0,
            cur: Vec::new(),
            pos: 0,
            error: None,
            reads: 0,
            done: false,
        };
        if let Err(e) = stream.fill_buf() {
            return Err(stream.error.take().unwrap_or(VykarError::Io(e)));
        }
        Ok(stream)
    }

    /// A stream over explicit chunk locations with a custom range-read cap,
    /// for tests that exercise window and chunk boundaries.
    #[cfg(test)]
    /// `retry: false` disables the re-resolve on a failed read.
    pub(crate) fn for_test(
        repo: &Repo,
        chunks: Vec<ChunkLoc>,
        retry: bool,
        window_max: u64,
        shutdown: &'a CancelToken,
    ) -> Self {
        Self {
            storage: Arc::clone(repo.storage()),
            crypto: Arc::clone(repo.crypto()),
            shutdown,
            chunks,
            relocation: repo.relocation_source(),
            pack_missing: false,
            cache: None,
            cache_hits: 0,
            retried: !retry,
            window_max,
            next: 0,
            window: Vec::new(),
            window_start: 0,
            window_end: 0,
            cur: Vec::new(),
            pos: 0,
            error: None,
            reads: 0,
            done: false,
        }
    }

    /// [`Self::for_test`] reading through the repository's tree-chunk cache.
    #[cfg(test)]
    pub(crate) fn for_test_with_cache(
        repo: &Repo,
        chunks: Vec<ChunkLoc>,
        retry: bool,
        window_max: u64,
        shutdown: &'a CancelToken,
    ) -> Self {
        Self {
            cache: repo.tree_cache().cloned(),
            ..Self::for_test(repo, chunks, retry, window_max, shutdown)
        }
    }

    /// Whether a location failed and the stream re-resolved the rest.
    #[cfg(test)]
    pub(crate) fn retried(&self) -> bool {
        self.retried
    }

    /// Call `visit` for each item in stream order.
    pub fn for_each_item(&mut self, visit: impl FnMut(Item) -> Result<()>) -> Result<()> {
        let result = for_each_item_in(&mut *self, visit);
        result.map_err(|e| self.error.take().unwrap_or(e))
    }

    /// Pick the range read that starts at chunk `first`: it extends over the
    /// following stream chunks while they sit forward in the same pack and
    /// are not in the tree-chunk cache, with gaps of at most
    /// [`LAZY_WINDOW_GAP`], up to [`LAZY_WINDOW_MAX`] bytes.
    /// Returns `(start, end, end_chunk)`.
    fn plan_window(&self, first: usize) -> Result<(PackId, u64, u64, usize)> {
        let overflow = |pack: &PackId| {
            VykarError::InvalidFormat(format!("pack offset overflow in item stream (pack {pack})"))
        };
        let &(_, pack, start, size) = self
            .chunks
            .get(first)
            .ok_or_else(|| VykarError::Other("item stream window out of range".into()))?;
        let mut end = start
            .checked_add(u64::from(size))
            .ok_or_else(|| overflow(&pack))?;
        let mut next = first + 1;
        while let Some(&(id, p, offset, size)) = self.chunks.get(next) {
            if p != pack || offset < end {
                break;
            }
            // A cached chunk is decoded from the cache, not downloaded.
            if self.cache.as_ref().is_some_and(|c| c.contains(&id)) {
                break;
            }
            let blob_end = offset
                .checked_add(u64::from(size))
                .ok_or_else(|| overflow(&pack))?;
            if offset - end > LAZY_WINDOW_GAP || blob_end - start > self.window_max {
                break;
            }
            end = blob_end;
            next += 1;
        }
        Ok((pack, start, end, next))
    }

    /// Decode `chunks[next]` into `cur`, fetching a new window first if the
    /// chunk is not in the current one.
    fn try_load_next(&mut self) -> Result<()> {
        let idx = self.next;
        if idx >= self.window_end {
            // Free the consumed window before fetching the next one.
            self.window = Vec::new();
            self.shutdown.check()?;
            if self.load_cached(idx) {
                return Ok(());
            }
            let mut rounds = 0;
            let (window, start, end_chunk) = loop {
                let (pack, start, end, end_chunk) = self.plan_window(idx)?;
                if let Some(window) =
                    self.storage
                        .get_range(&pack.storage_key(), start, end - start)?
                {
                    break (window, start, end_chunk);
                }
                let relocated = if rounds == MAX_RELOCATE_ROUNDS {
                    Err(pack_not_found(&pack))
                } else {
                    self.relocate_rest(&pack)
                };
                if let Err(e) = relocated {
                    self.pack_missing = true;
                    return Err(e);
                }
                rounds += 1;
            };
            self.window = window;
            self.window_start = start;
            self.window_end = end_chunk;
            self.reads += 1;
        }
        let &(chunk_id, pack_id, offset, stored_size) = self
            .chunks
            .get(idx)
            .ok_or_else(|| VykarError::Other("item stream chunk out of range".into()))?;
        let blob = usize::try_from(offset - self.window_start)
            .ok()
            .and_then(|local| {
                self.window
                    .get(local..local.checked_add(stored_size as usize)?)
            })
            .ok_or_else(|| {
                VykarError::Other(format!(
                    "blob extends beyond downloaded range in pack {pack_id}"
                ))
            })?;
        // Decode a copy: after a failed decode the window is kept and the
        // same bytes are decoded again on retry.
        let mut raw = blob.to_vec();
        let mut plaintext = Vec::new();
        decode_chunk_into(
            &mut raw,
            BlobLocation {
                chunk_id,
                pack_id,
                pack_offset: offset,
                stored_size,
            },
            self.crypto.as_ref(),
            None,
            false,
            &mut plaintext,
        )?;
        if let Some(cache) = &self.cache {
            cache.insert(&chunk_id, blob);
        }
        self.cur = plaintext;
        self.pos = 0;
        self.next += 1;
        Ok(())
    }

    /// `pack` answered 404: look the stream's remaining chunks in it up at
    /// the latest head and move them there. A chunk the latest head no
    /// longer has is [`VykarError::ChunkRemovedDuringRead`].
    fn relocate_rest(&mut self, pack: &PackId) -> Result<()> {
        let at: Vec<usize> = (self.next..self.chunks.len())
            .filter(|&i| self.chunks.get(i).is_some_and(|c| c.1 == *pack))
            .collect();
        let ids: Vec<ChunkId> = at
            .iter()
            .filter_map(|&i| self.chunks.get(i).map(|c| c.0))
            .collect();
        let located = self.relocation.relocator().relocate(pack, &ids)?;
        for (i, loc) in at.into_iter().zip(located) {
            let Some(chunk) = self.chunks.get_mut(i) else {
                continue;
            };
            match loc {
                relocate::Located::Gone => {
                    return Err(VykarError::ChunkRemovedDuringRead { chunk_id: chunk.0 });
                }
                relocate::Located::At {
                    pack_id,
                    pack_offset,
                    stored_size,
                } => *chunk = (chunk.0, pack_id, pack_offset, stored_size),
            }
        }
        Ok(())
    }

    /// Decode `chunks[idx]` from the tree-chunk cache into `cur`. False when
    /// there is no cache or no entry, or the entry does not decode (it is
    /// deleted then); the caller downloads the chunk.
    fn load_cached(&mut self, idx: usize) -> bool {
        let Some(cache) = &self.cache else {
            return false;
        };
        let Some(&(chunk_id, pack_id, pack_offset, stored_size)) = self.chunks.get(idx) else {
            return false;
        };
        let Some(mut stored) = cache.get(&chunk_id) else {
            return false;
        };
        let mut plaintext = Vec::new();
        let at = BlobLocation {
            chunk_id,
            pack_id,
            pack_offset,
            stored_size,
        };
        if let Err(e) = decode_chunk_into(
            &mut stored,
            at,
            self.crypto.as_ref(),
            None,
            false,
            &mut plaintext,
        ) {
            tracing::debug!("tree-chunk cache: entry {chunk_id} does not decode: {e:?}");
            cache.remove(&chunk_id);
            return false;
        }
        self.cur = plaintext;
        self.pos = 0;
        self.next += 1;
        // The next chunk plans a window afresh.
        self.window_end = self.next;
        self.cache_hits += 1;
        true
    }

    fn load_next(&mut self) -> Result<()> {
        match self.try_load_next() {
            Ok(()) => Ok(()),
            // A cancellation is not a stale location.
            Err(e @ VykarError::Interrupted) => Err(e),
            // A 404 was re-resolved against the latest head already.
            Err(e) if self.pack_missing => {
                self.pack_missing = false;
                Err(e)
            }
            Err(e) if !self.retried => {
                info!("item-stream chunk unreadable, re-resolving against the latest index: {e}");
                self.retried = true;
                // When the retry cannot be set up, the read error is the one
                // to report, not the retry's.
                let rest = self.chunks.get(self.next..).unwrap_or_default();
                let ids: Vec<ChunkId> = rest.iter().map(|c| c.0).collect();
                let resolved = Repo::fetch_full_index(self.storage.as_ref(), self.crypto.as_ref())
                    .and_then(|index| resolve_from_index(&index, &ids));
                let resolved = match resolved {
                    Ok(resolved) => resolved,
                    Err(retry @ VykarError::Interrupted) => return Err(retry),
                    Err(retry) => {
                        info!("cannot re-resolve the item stream: {retry}");
                        return Err(e);
                    }
                };
                self.chunks.truncate(self.next);
                self.chunks.extend(resolved);
                self.window = Vec::new();
                self.window_end = self.next;
                self.try_load_next()
            }
            Err(e) => Err(e),
        }
    }
}

impl BufRead for LazyItemStream<'_> {
    fn fill_buf(&mut self) -> std::io::Result<&[u8]> {
        while self.pos >= self.cur.len() {
            if self.next >= self.chunks.len() {
                self.cur = Vec::new();
                self.window = Vec::new();
                self.pos = 0;
                if !self.done {
                    self.done = true;
                    tracing::debug!(
                        "item stream: {} chunks, {} from cache, in {} range reads",
                        self.chunks.len(),
                        self.cache_hits,
                        self.reads
                    );
                }
                break;
            }
            if let Err(e) = self.load_next() {
                let msg = e.to_string();
                self.error = Some(e);
                // Not `ErrorKind::Interrupted`: rmp-serde retries those.
                return Err(std::io::Error::other(msg));
            }
        }
        Ok(self.cur.get(self.pos..).unwrap_or_default())
    }

    fn consume(&mut self, amt: usize) {
        self.pos = self.pos.saturating_add(amt).min(self.cur.len());
    }
}

impl Read for LazyItemStream<'_> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let avail = self.fill_buf()?;
        let n = avail.len().min(buf.len());
        if let (Some(dst), Some(src)) = (buf.get_mut(..n), avail.get(..n)) {
            dst.copy_from_slice(src);
        }
        self.consume(n);
        Ok(n)
    }
}

/// Decode concatenated msgpack items from `reader` until it is exhausted.
fn for_each_item_in<R: BufRead>(
    reader: R,
    mut visit: impl FnMut(Item) -> Result<()>,
) -> Result<()> {
    let mut de = rmp_serde::Deserializer::new(reader);
    // Between items the decoder holds no peeked marker, so an empty buffer
    // here is a clean end of stream.
    while !de.get_mut().fill_buf()?.is_empty() {
        let item = Item::deserialize(&mut de)?;
        visit(item)?;
    }
    Ok(())
}

/// Anything restore can stream items from: a materialized item stream or a
/// [`LazyItemStream`].
pub trait ItemSource {
    fn for_each_item(self, visit: impl FnMut(Item) -> Result<()>) -> Result<()>;
}

impl<T: AsRef<[u8]> + ?Sized> ItemSource for &T {
    fn for_each_item(self, visit: impl FnMut(Item) -> Result<()>) -> Result<()> {
        for_each_decoded_item(self.as_ref(), visit)
    }
}

impl ItemSource for &mut LazyItemStream<'_> {
    fn for_each_item(self, visit: impl FnMut(Item) -> Result<()>) -> Result<()> {
        LazyItemStream::for_each_item(self, visit)
    }
}

/// Load item stream using a lookup closure instead of the chunk index.
/// Returns ChunkNotInIndex if any tree-pack chunk is missing from the lookup.
pub fn load_snapshot_item_stream_via_lookup<L>(
    repo: &mut Repo,
    snapshot_name: &str,
    lookup: L,
    shutdown: &CancelToken,
) -> Result<Vec<u8>>
where
    L: Fn(&ChunkId) -> Option<(PackId, u64, u32)>,
{
    let snapshot_meta = load_snapshot_meta(repo, snapshot_name)?;
    resolve_and_read(
        repo,
        &snapshot_meta.item_ptrs,
        |chunk_id, _repo| lookup(chunk_id).ok_or(VykarError::ChunkNotInIndex(*chunk_id)),
        shutdown,
        TreeChunkSource::CacheFirst,
    )
}

/// Load the raw concatenated item stream bytes from chunk pointers using the chunk index,
/// reading tree chunks from the local tree-chunk cache first.
/// Returns ChunkNotInIndex if a chunk is missing from the chunk index, cached or not.
pub fn load_item_stream_from_ptrs(
    repo: &mut Repo,
    item_ptrs: &[ChunkId],
    shutdown: &CancelToken,
) -> Result<Vec<u8>> {
    resolve_and_read(
        repo,
        item_ptrs,
        resolve_through_index,
        shutdown,
        TreeChunkSource::CacheFirst,
    )
}

/// The location of `chunk_id` in the repository's chunk index.
fn resolve_through_index(chunk_id: &ChunkId, repo: &Repo) -> Result<(PackId, u64, u32)> {
    let entry = repo
        .chunk_index()
        .get(chunk_id)
        .ok_or(VykarError::ChunkNotInIndex(*chunk_id))?;
    Ok((entry.pack_id, entry.pack_offset, entry.stored_size))
}

/// Resolve every chunk location first, then read them via coalesced range
/// reads from `source`. Resolution comes before any read, so a pointer the
/// index lacks fails even when its chunk is cached.
fn resolve_and_read<R>(
    repo: &mut Repo,
    item_ptrs: &[ChunkId],
    resolve: R,
    shutdown: &CancelToken,
    source: TreeChunkSource,
) -> Result<Vec<u8>>
where
    R: Fn(&ChunkId, &Repo) -> Result<(PackId, u64, u32)>,
{
    shutdown.check()?;
    let chunks: Vec<(ChunkId, PackId, u64, u32)> = item_ptrs
        .iter()
        .map(|chunk_id| {
            let (pack_id, pack_offset, stored_size) = resolve(chunk_id, repo)?;
            Ok((*chunk_id, pack_id, pack_offset, stored_size))
        })
        .collect::<Result<_>>()?;

    let mut items_stream = Vec::new();
    repo.read_tree_chunks_into(&chunks, &mut items_stream, shutdown, source)?;
    Ok(items_stream)
}

/// Load and deserialize all items using a lookup closure.
pub fn load_snapshot_items_via_lookup<L>(
    repo: &mut Repo,
    snapshot_name: &str,
    lookup: L,
    shutdown: &CancelToken,
) -> Result<Vec<Item>>
where
    L: Fn(&ChunkId) -> Option<(PackId, u64, u32)>,
{
    let items_stream = load_snapshot_item_stream_via_lookup(repo, snapshot_name, lookup, shutdown)?;
    decode_items_stream(&items_stream)
}

/// Decode item stream bytes and call `visit` for each item in stream order.
///
/// Applied to one tree chunk, an error includes a chunk that ends inside an
/// entry; the mark and `check` decode each tree chunk on its own this way.
pub fn for_each_decoded_item(
    items_stream: &[u8],
    mut visit: impl FnMut(Item) -> Result<()>,
) -> Result<()> {
    if items_stream.is_empty() {
        return Ok(());
    }

    // Items are encoded as concatenated MsgPack Item objects.
    let mut de = rmp_serde::Deserializer::new(std::io::Cursor::new(items_stream));
    while (de.position() as usize) < items_stream.len() {
        let item = Item::deserialize(&mut de)?;
        visit(item)?;
    }
    Ok(())
}

/// Stream items from a snapshot without materializing `Vec<Item>`.
pub fn for_each_snapshot_item(
    repo: &mut Repo,
    snapshot_name: &str,
    shutdown: &CancelToken,
    visit: impl FnMut(Item) -> Result<()>,
) -> Result<()> {
    let items_stream = load_snapshot_item_stream(repo, snapshot_name, shutdown)?;
    for_each_decoded_item(&items_stream, visit)
}

fn decode_items_stream(items_stream: &[u8]) -> Result<Vec<Item>> {
    let mut items = Vec::new();
    for_each_decoded_item(items_stream, |item| {
        items.push(item);
        Ok(())
    })?;
    Ok(items)
}

#[cfg(test)]
mod tests {
    use super::decode_items_stream;
    use crate::snapshot::item::Item;

    #[test]
    fn decode_streamed_item_sequence() {
        let item = Item {
            path: "b.txt".into(),
            entry_type: crate::snapshot::item::ItemType::RegularFile,
            mode: 0o644,
            uid: 1000,
            gid: 1000,
            user: None,
            group: None,
            mtime: 0,
            atime: None,
            ctime: None,
            size: 0,
            chunks: Vec::new(),
            link_target: None,
            xattrs: None,
            raw_names: None,
            hardlink: None,
        };

        let mut bytes = Vec::new();
        bytes.extend_from_slice(&rmp_serde::to_vec(&item).unwrap());
        bytes.extend_from_slice(&rmp_serde::to_vec(&item).unwrap());

        let decoded = decode_items_stream(&bytes).unwrap();
        assert_eq!(decoded.len(), 2);
        assert_eq!(decoded[0].path, "b.txt");
        assert_eq!(decoded[1].path, "b.txt");
    }
}
