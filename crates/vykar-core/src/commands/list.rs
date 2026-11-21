use std::io::{BufRead, Read};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use serde::Deserialize;
use tracing::info;

use crate::config::VykarConfig;
use crate::index::dedup_cache::MmapRestoreCache;
use crate::repo::Repository;
use crate::repo::format::{
    BlobLocation, ObjectType, decode_chunk_into, unpack_object_expect_with_context,
};
use crate::repo::manifest::SnapshotEntry;
use crate::snapshot::SnapshotMeta;
use crate::snapshot::item::Item;
use vykar_crypto::CryptoEngine;
use vykar_storage::StorageBackend;
use vykar_types::chunk_id::ChunkId;
use vykar_types::error::{Result, VykarError};
use vykar_types::pack_id::PackId;

use super::util::{check_interrupted, open_repo};
use crate::repo::OpenOptions;

/// List all snapshots in the repository.
pub fn list_snapshots(
    config: &VykarConfig,
    passphrase: Option<&str>,
) -> Result<Vec<SnapshotEntry>> {
    let repo = open_repo(config, passphrase, OpenOptions::new())?;
    Ok(repo.manifest().snapshots.clone())
}

/// List all items in a specific snapshot.
/// Tries the local restore cache first to avoid downloading the full index.
/// Falls back to the full index (with blob cache) on cache miss.
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
    let (mut repo, _session_guard) =
        super::util::open_repo_with_read_session(config, passphrase, OpenOptions::new())?;

    let resolved_name = repo
        .manifest()
        .resolve_snapshot(snapshot_name)?
        .name
        .clone();

    let source_paths = load_snapshot_meta(&repo, &resolved_name)?.source_paths;
    let stream = ItemStreamReader::new(&repo).load(&mut repo, &resolved_name, None)?;
    let items = decode_items_stream(&stream)?;
    Ok((items, source_paths))
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
    let repo = open_repo(config, passphrase, OpenOptions::new())?;
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
    let repo = open_repo(config, passphrase, OpenOptions::new())?;
    let resolved_name = repo
        .manifest()
        .resolve_snapshot(snapshot_name)?
        .name
        .clone();
    load_snapshot_meta(&repo, &resolved_name)
}

/// Load the SnapshotMeta for a snapshot by name.
pub fn load_snapshot_meta(repo: &Repository, snapshot_name: &str) -> Result<SnapshotMeta> {
    let entry = repo
        .manifest()
        .find_snapshot(snapshot_name)
        .ok_or_else(|| VykarError::SnapshotNotFound(snapshot_name.into()))?;

    let meta_data = repo
        .storage
        .get(&entry.id.storage_key())?
        .ok_or_else(|| VykarError::SnapshotNotFound(snapshot_name.into()))?;

    let meta_bytes = unpack_object_expect_with_context(
        &meta_data,
        ObjectType::SnapshotMeta,
        entry.id.as_bytes(),
        repo.crypto.as_ref(),
    )?;
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
    repo: &mut Repository,
    snapshot_name: &str,
    shutdown: Option<&AtomicBool>,
) -> Result<Vec<Item>> {
    let items_stream = load_snapshot_item_stream(repo, snapshot_name, shutdown)?;
    decode_items_stream(&items_stream)
}

/// Load the raw concatenated item stream bytes for a snapshot.
pub fn load_snapshot_item_stream(
    repo: &mut Repository,
    snapshot_name: &str,
    shutdown: Option<&AtomicBool>,
) -> Result<Vec<u8>> {
    let snapshot_meta = load_snapshot_meta(repo, snapshot_name)?;
    load_item_stream_from_ptrs(repo, &snapshot_meta.item_ptrs, shutdown)
}

/// Item-stream reader for one read-only operation: tries the local restore
/// cache first to avoid downloading the chunk index, and falls back to the
/// full index (loaded at most once) on any cache miss or read/decrypt error.
pub struct ItemStreamReader {
    restore_cache: Option<MmapRestoreCache>,
    index_loaded: bool,
    /// Raised by a [`LazyItemStream`] of this reader when a cached location
    /// turned out to be unreadable mid-stream: the cache must not serve any
    /// further lookup.
    cache_distrusted: Arc<AtomicBool>,
}

impl ItemStreamReader {
    pub fn new(repo: &Repository) -> Self {
        Self {
            restore_cache: repo.open_restore_cache(),
            index_loaded: false,
            cache_distrusted: Arc::new(AtomicBool::new(false)),
        }
    }

    /// Load the raw item stream bytes for `snapshot_name`.
    pub fn load(
        &mut self,
        repo: &mut Repository,
        snapshot_name: &str,
        shutdown: Option<&AtomicBool>,
    ) -> Result<Vec<u8>> {
        if let Some(cache) = &self.restore_cache {
            match load_snapshot_item_stream_via_lookup(
                repo,
                snapshot_name,
                |chunk_id| cache.lookup(chunk_id),
                shutdown,
            ) {
                Ok(stream) => return Ok(stream),
                // A cancellation is not a stale cache — re-raise it instead of
                // falling through, which would load the full index and retry.
                Err(e @ VykarError::Interrupted) => return Err(e),
                Err(e) => {
                    // The cache is generation-pinned, so a miss means it is
                    // incomplete or corrupt; drop it once the index is needed.
                    info!("restore cache incomplete or stale, falling back to full index: {e}");
                    self.restore_cache = None;
                }
            }
        }
        self.ensure_index(repo)?;
        load_snapshot_item_stream(repo, snapshot_name, shutdown)
    }

    /// Open the item stream of `snapshot_name` for lazy, chunk-at-a-time
    /// decoding (see [`LazyItemStream`]) instead of materializing it.
    ///
    /// Chunk locations are resolved up front, through the restore cache when
    /// it holds every item-stream chunk, else through the full index (loaded
    /// at most once). The first range read happens here, so a cancellation or
    /// an unreadable stream head fails before the caller mutates anything.
    pub fn open_lazy<'a>(
        &mut self,
        repo: &mut Repository,
        snapshot_name: &str,
        shutdown: Option<&'a AtomicBool>,
    ) -> Result<LazyItemStream<'a>> {
        check_interrupted(shutdown)?;
        let meta = load_snapshot_meta(repo, snapshot_name)?;
        let mut chunks = None;
        if let Some(cache) = &self.restore_cache {
            let resolved: std::result::Result<Vec<ChunkLoc>, ChunkId> = meta
                .item_ptrs
                .iter()
                .map(|id| {
                    cache
                        .lookup(id)
                        .map(|(pack_id, offset, size)| (*id, pack_id, offset, size))
                        .ok_or(*id)
                })
                .collect();
            match resolved {
                Ok(c) => chunks = Some(c),
                Err(missing) => {
                    info!(
                        "restore cache lacks item-stream chunk {missing}, falling back to full index"
                    );
                    self.restore_cache = None;
                }
            }
        }
        let from_cache = chunks.is_some();
        let chunks = match chunks {
            Some(c) => c,
            None => {
                self.ensure_index(repo)?;
                resolve_from_index(repo.chunk_index(), &meta.item_ptrs)?
            }
        };

        let mut stream = LazyItemStream {
            storage: Arc::clone(&repo.storage),
            crypto: Arc::clone(&repo.crypto),
            format: repo.format(),
            shutdown,
            chunks,
            from_cache,
            cache_distrusted: Arc::clone(&self.cache_distrusted),
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
        if from_cache && !stream.from_cache {
            // The stream head was unreadable at the cached coordinates: stop
            // trusting the cache for the caller's own lookups too.
            self.restore_cache = None;
        }
        Ok(stream)
    }

    /// Load the full chunk index unless this reader already did.
    pub fn ensure_index(&mut self, repo: &mut Repository) -> Result<()> {
        if !self.index_loaded {
            repo.load_chunk_index_for_snapshot_view()?;
            self.index_loaded = true;
        }
        Ok(())
    }

    /// Prepare [`Self::lookup`] for `ids`: keep resolving through the restore
    /// cache when it holds every one of them, otherwise drop the cache and
    /// load the full index (once; later calls keep using the index).
    ///
    /// The cache is generation-pinned, so a miss means it is incomplete or
    /// corrupt — not that the chunk is gone. Falling back *before* a caller
    /// declares a chunk missing keeps an incomplete cache from marking files
    /// damaged.
    ///
    /// This only checks presence. The pin is the advisory `index.gen`, so a
    /// cache can hold every chunk and still be stale (locations of packs a
    /// compaction has since rewritten); a caller that fails to read a chunk
    /// at a cached location must call [`Self::fall_back_to_index`] and
    /// resolve it again before treating the chunk as unreadable.
    pub fn ensure_resolvable<'a>(
        &mut self,
        repo: &mut Repository,
        ids: impl IntoIterator<Item = &'a ChunkId>,
    ) -> Result<()> {
        if self.cache_distrusted.load(Ordering::Relaxed) && self.restore_cache.is_some() {
            info!("restore cache had an unreadable item-stream location, using the full index");
            self.restore_cache = None;
        }
        if let Some(cache) = &self.restore_cache {
            match ids.into_iter().find(|id| cache.lookup(id).is_none()) {
                None => return Ok(()),
                Some(missing) => {
                    info!("restore cache lacks chunk {missing}, falling back to full index");
                    self.restore_cache = None;
                }
            }
        }
        self.ensure_index(repo)
    }

    /// Stop using the restore cache and load the full index, which serves
    /// every later [`Self::lookup`].
    pub fn fall_back_to_index(&mut self, repo: &mut Repository) -> Result<()> {
        self.restore_cache = None;
        self.ensure_index(repo)
    }

    /// Resolve a chunk to `(pack_id, pack_offset, stored_size)` through the
    /// restore cache, or through the full index once the cache was dropped.
    /// Call [`Self::ensure_resolvable`] first.
    pub fn lookup(&self, repo: &Repository, id: &ChunkId) -> Option<(PackId, u64, u32)> {
        match &self.restore_cache {
            Some(cache) => cache.lookup(id),
            None => repo
                .chunk_index()
                .get(id)
                .map(|e| (e.pack_id, e.pack_offset, e.stored_size)),
        }
    }

    /// Whether lookups are still served by the restore cache.
    pub fn uses_restore_cache(&self) -> bool {
        self.restore_cache.is_some()
    }

    /// Whether this reader loaded the full chunk index.
    pub fn index_loaded(&self) -> bool {
        self.index_loaded
    }
}

/// `(chunk_id, pack_id, pack_offset, stored_size)` of one item-stream chunk.
type ChunkLoc = (ChunkId, PackId, u64, u32);

/// Upper bound on one range read of [`LazyItemStream`]. Smaller than the
/// eager reader's 16 MiB coalescing cap because the raw window stays resident
/// while its chunks are decoded; item-stream chunks average about 32 KiB
/// plaintext, so one window still spans dozens of them.
const LAZY_WINDOW_MAX: u64 = 4 * 1024 * 1024;

/// Largest gap between two stream-consecutive chunks of one pack that is
/// still read through rather than split into two range reads.
const LAZY_WINDOW_GAP: u64 = 256 * 1024;

fn resolve_from_index(index: &crate::index::ChunkIndex, ids: &[ChunkId]) -> Result<Vec<ChunkLoc>> {
    ids.iter()
        .map(|id| {
            let e = index
                .get(id)
                .ok_or_else(|| VykarError::Other(format!("chunk not found: {id}")))?;
            Ok((*id, e.pack_id, e.pack_offset, e.stored_size))
        })
        .collect()
}

/// A snapshot's item stream read on demand: only the current range read
/// (at most `LAZY_WINDOW_MAX`, 4 MiB) and the plaintext of the chunk being decoded
/// are resident, instead of the whole decompressed stream. Items may straddle
/// chunk boundaries; the [`BufRead`] impl hides them.
///
/// Holds its own storage and crypto handles so the caller can keep using the
/// repository mutably while the stream is being consumed. Every chunk is
/// authenticated (AEAD) or, for unencrypted repositories, checked against its
/// chunk ID, exactly like [`Repository::read_chunks_coalesced_into`].
///
/// When the locations came from the restore cache and a chunk cannot be read
/// or decoded at them, the remaining locations are re-resolved once through a
/// freshly downloaded full index (dropped right after) and the read retried.
pub struct LazyItemStream<'a> {
    storage: Arc<dyn StorageBackend>,
    crypto: Arc<dyn CryptoEngine>,
    format: crate::repo::RepoFormat,
    shutdown: Option<&'a AtomicBool>,
    chunks: Vec<ChunkLoc>,
    from_cache: bool,
    /// Shared with the [`ItemStreamReader`]; raised on a cache fallback.
    cache_distrusted: Arc<AtomicBool>,
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
    /// A stream over explicit chunk locations with a custom range-read cap,
    /// for tests that exercise window and chunk boundaries.
    #[cfg(test)]
    pub(crate) fn for_test(
        repo: &Repository,
        chunks: Vec<ChunkLoc>,
        from_cache: bool,
        window_max: u64,
        shutdown: Option<&'a AtomicBool>,
    ) -> Self {
        Self {
            storage: Arc::clone(&repo.storage),
            crypto: Arc::clone(&repo.crypto),
            format: repo.format(),
            shutdown,
            chunks,
            from_cache,
            cache_distrusted: Arc::new(AtomicBool::new(false)),
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

    /// Whether a cached location failed and the stream fell back to the index.
    #[cfg(test)]
    pub(crate) fn cache_distrusted(&self) -> bool {
        self.cache_distrusted.load(Ordering::Relaxed)
    }

    /// Call `visit` for each item in stream order.
    pub fn for_each_item(&mut self, visit: impl FnMut(Item) -> Result<()>) -> Result<()> {
        let result = for_each_item_in(&mut *self, visit);
        result.map_err(|e| self.error.take().unwrap_or(e))
    }

    /// Pick the range read that starts at chunk `first`: it extends over the
    /// following stream chunks while they sit forward in the same pack, with
    /// gaps of at most [`LAZY_WINDOW_GAP`], up to [`LAZY_WINDOW_MAX`] bytes.
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
        while let Some(&(_, p, offset, size)) = self.chunks.get(next) {
            if p != pack || offset < end {
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
            check_interrupted(self.shutdown)?;
            let (pack, start, end, end_chunk) = self.plan_window(idx)?;
            self.window = self
                .storage
                .get_range(&pack.storage_key(), start, end - start)?
                .ok_or_else(|| VykarError::Other(format!("pack not found: {pack}")))?;
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
        self.cur = plaintext;
        self.pos = 0;
        self.next += 1;
        Ok(())
    }

    fn load_next(&mut self) -> Result<()> {
        match self.try_load_next() {
            Ok(()) => Ok(()),
            // A cancellation is not a stale cache.
            Err(e @ VykarError::Interrupted) => Err(e),
            Err(e) if self.from_cache => {
                info!("restore cache location unreadable, falling back to full index: {e}");
                self.from_cache = false;
                self.cache_distrusted.store(true, Ordering::Relaxed);
                let (_, index) = Repository::fetch_full_index(
                    self.storage.as_ref(),
                    self.crypto.as_ref(),
                    self.format,
                )?;
                let rest = self.chunks.get(self.next..).unwrap_or_default();
                let ids: Vec<ChunkId> = rest.iter().map(|c| c.0).collect();
                let resolved = resolve_from_index(&index, &ids)?;
                drop(index);
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
                        "item stream: {} chunks in {} range reads",
                        self.chunks.len(),
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
    repo: &mut Repository,
    snapshot_name: &str,
    lookup: L,
    shutdown: Option<&AtomicBool>,
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
    )
}

/// Load the raw concatenated item stream bytes from chunk pointers using the chunk index.
pub fn load_item_stream_from_ptrs(
    repo: &mut Repository,
    item_ptrs: &[ChunkId],
    shutdown: Option<&AtomicBool>,
) -> Result<Vec<u8>> {
    resolve_and_read(
        repo,
        item_ptrs,
        |chunk_id, repo| {
            let entry = *repo
                .chunk_index()
                .get(chunk_id)
                .ok_or_else(|| VykarError::Other(format!("chunk not found: {chunk_id}")))?;
            Ok((entry.pack_id, entry.pack_offset, entry.stored_size))
        },
        shutdown,
    )
}

/// Resolve chunk locations and read them via coalesced range reads.
fn resolve_and_read<R>(
    repo: &mut Repository,
    item_ptrs: &[ChunkId],
    resolve: R,
    shutdown: Option<&AtomicBool>,
) -> Result<Vec<u8>>
where
    R: Fn(&ChunkId, &Repository) -> Result<(PackId, u64, u32)>,
{
    check_interrupted(shutdown)?;
    let chunks: Vec<(ChunkId, PackId, u64, u32)> = item_ptrs
        .iter()
        .map(|chunk_id| {
            let (pack_id, pack_offset, stored_size) = resolve(chunk_id, repo)?;
            Ok((*chunk_id, pack_id, pack_offset, stored_size))
        })
        .collect::<Result<_>>()?;

    let mut items_stream = Vec::new();
    repo.read_chunks_coalesced_into(&chunks, &mut items_stream, shutdown)?;
    Ok(items_stream)
}

/// Load and deserialize all items using a lookup closure.
pub fn load_snapshot_items_via_lookup<L>(
    repo: &mut Repository,
    snapshot_name: &str,
    lookup: L,
    shutdown: Option<&AtomicBool>,
) -> Result<Vec<Item>>
where
    L: Fn(&ChunkId) -> Option<(PackId, u64, u32)>,
{
    let items_stream = load_snapshot_item_stream_via_lookup(repo, snapshot_name, lookup, shutdown)?;
    decode_items_stream(&items_stream)
}

/// Decode item stream bytes and call `visit` for each item in stream order.
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
    repo: &mut Repository,
    snapshot_name: &str,
    shutdown: Option<&AtomicBool>,
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
