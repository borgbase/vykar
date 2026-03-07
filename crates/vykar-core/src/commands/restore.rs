use std::collections::{HashMap, HashSet};
#[cfg(not(unix))]
use std::io::{Seek, Write as IoWrite};
#[cfg(unix)]
use std::os::unix::fs::FileExt;
use std::path::{Component, Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;

use smallvec::SmallVec;
use tracing::{debug, info};

use crate::compress;
use crate::config::VykarConfig;
use crate::platform::fs;
use crate::repo::format::{unpack_object_expect_with_context_into, ObjectType};
#[cfg(test)]
use crate::snapshot::item::Item;
use crate::snapshot::item::ItemType;
use vykar_crypto::CryptoEngine;
use vykar_storage::StorageBackend;
use vykar_types::chunk_id::ChunkId;
use vykar_types::error::{Result, VykarError};
use vykar_types::pack_id::PackId;

use crate::repo::Repository;

// ---------------------------------------------------------------------------
// Constants for coalesced parallel restore
// ---------------------------------------------------------------------------

/// Maximum gap (in bytes) between blobs in the same pack that will be coalesced
/// into a single range read rather than issuing separate requests.
const MAX_COALESCE_GAP: u64 = 256 * 1024; // 256 KiB

/// Maximum size of a single coalesced range read.
const MAX_READ_SIZE: u64 = 16 * 1024 * 1024; // 16 MiB

// MAX_READER_THREADS removed — uses config.limits.restore_concurrency() instead.

/// Maximum number of simultaneously open output files per restore worker.
/// Caps fd usage while still avoiding per-chunk open/close churn.
const MAX_OPEN_FILES_PER_GROUP: usize = 16;

/// Maximum size (in bytes) of the write accumulator before flushing.
/// Batching consecutive same-file writes into a single `pwrite64` reduces
/// syscall count and inode rwsem contention.
const MAX_WRITE_BATCH: usize = 1024 * 1024; // 1 MiB

// ---------------------------------------------------------------------------
// Data structures for the coalesced parallel restore
// ---------------------------------------------------------------------------

/// Where to write a chunk's decompressed data.
struct WriteTarget {
    file_idx: usize,
    file_offset: u64,
}

/// A chunk within a coalesced read group.
struct PlannedBlob {
    chunk_id: ChunkId,
    pack_offset: u64,
    stored_size: u32,
    expected_size: u32,
    /// Most chunks are referenced by exactly one file, so SmallVec stores
    /// the single target inline without a heap allocation.
    targets: SmallVec<[WriteTarget; 1]>,
}

/// A coalesced read — maps to a single storage range GET.
struct ReadGroup {
    pack_id: PackId,
    read_start: u64,
    read_end: u64, // exclusive
    blobs: Vec<PlannedBlob>,
}

/// Output file metadata for post-restore attribute application.
struct PlannedFile {
    rel_path: PathBuf,
    total_size: u64,
    mode: u32,
    mtime: i64,
    xattrs: Option<HashMap<String, Vec<u8>>>,
    /// CAS flag: the first worker to open this file calls `set_len`.
    /// Prevents repeated `ftruncate` syscalls when multiple workers open
    /// the same large file across different read groups.
    created: AtomicBool,
}

// ---------------------------------------------------------------------------
// Public entry points
// ---------------------------------------------------------------------------

/// Run `vykar restore`.
pub fn run(
    config: &VykarConfig,
    passphrase: Option<&str>,
    snapshot_name: &str,
    dest: &str,
    pattern: Option<&str>,
    xattrs_enabled: bool,
) -> Result<RestoreStats> {
    let filter = pattern
        .map(|p| {
            globset::GlobBuilder::new(p)
                .literal_separator(false)
                .build()
                .map(|g| g.compile_matcher())
        })
        .transpose()
        .map_err(|e| VykarError::Config(format!("invalid pattern: {e}")))?;

    restore_with_filter(
        config,
        passphrase,
        snapshot_name,
        dest,
        xattrs_enabled,
        move |path| {
            filter
                .as_ref()
                .map(|matcher| matcher.is_match(path))
                .unwrap_or(true)
        },
    )
}

/// Run `vykar restore` for a selected set of paths.
///
/// An item is included if its path exactly matches an entry in `selected_paths`,
/// or if any prefix of its path matches (i.e. a parent directory was selected).
pub fn run_selected(
    config: &VykarConfig,
    passphrase: Option<&str>,
    snapshot_name: &str,
    dest: &str,
    selected_paths: &HashSet<String>,
    xattrs_enabled: bool,
) -> Result<RestoreStats> {
    restore_with_filter(
        config,
        passphrase,
        snapshot_name,
        dest,
        xattrs_enabled,
        |path| path_matches_selection(path, selected_paths),
    )
}

// ---------------------------------------------------------------------------
// Core restore logic — phased approach
// ---------------------------------------------------------------------------

fn restore_with_filter<F>(
    config: &VykarConfig,
    passphrase: Option<&str>,
    snapshot_name: &str,
    dest: &str,
    xattrs_enabled: bool,
    mut include_path: F,
) -> Result<RestoreStats>
where
    F: FnMut(&str) -> bool,
{
    let (mut repo, _session_guard) =
        super::util::open_repo_with_read_session(config, passphrase, true, true)?;
    // Shrink blob cache for restore — the parallel pipeline reads pack data
    // directly via storage.get_range(), so the cache only serves the small
    // item-stream tree-pack chunks. 2 MiB is plenty.
    repo.set_blob_cache_max_bytes(2 * 1024 * 1024);
    let xattrs_enabled = if xattrs_enabled && !fs::xattrs_supported() {
        tracing::warn!(
            "xattrs requested but not supported on this platform; continuing without xattrs"
        );
        false
    } else {
        xattrs_enabled
    };

    // Try to open the mmap restore cache before loading the index.
    let restore_cache = repo.open_restore_cache();

    // Resolve "latest" or exact snapshot name
    let resolved_name = repo
        .manifest()
        .resolve_snapshot(snapshot_name)?
        .name
        .clone();
    if resolved_name != snapshot_name {
        info!("Resolved '{}' to snapshot {}", snapshot_name, resolved_name);
    }

    // Load raw item stream bytes (not decoded Items).  When the restore cache
    // is available, read tree-pack chunks via the cache to avoid loading the
    // full chunk index.
    let items_stream = if let Some(ref cache) = restore_cache {
        match super::list::load_snapshot_item_stream_via_lookup(&mut repo, &resolved_name, |id| {
            cache.lookup(id)
        }) {
            Ok(stream) => stream,
            Err(_) => {
                info!("restore cache incomplete or stale, falling back to full index");
                repo.load_chunk_index()?;
                super::list::load_snapshot_item_stream(&mut repo, &resolved_name)?
            }
        }
    } else {
        repo.load_chunk_index()?;
        super::list::load_snapshot_item_stream(&mut repo, &resolved_name)?
    };

    let dest_root = validate_and_prepare_dest(dest)?;

    // Create a hidden temp directory inside dest so all writes happen there.
    // On success we rename top-level entries into dest; on failure we
    // remove_dir_all the temp root so no partial files are left at dest.
    let temp_dir_name = format!(
        ".vykar-restore-{:016x}",
        rand::Rng::gen::<u64>(&mut rand::thread_rng())
    );
    let temp_root = dest_root.join(&temp_dir_name);
    std::fs::create_dir_all(&temp_root)?;

    let cleanup = |e: VykarError| -> VykarError {
        let _ = std::fs::remove_dir_all(&temp_root);
        e
    };

    // Stream items: create dirs/symlinks immediately, build file plan + chunk targets.
    let (mut planned_files, chunk_targets, mut stats, verified_dirs) =
        stream_and_plan(&items_stream, &temp_root, &mut include_path, xattrs_enabled)
            .map_err(&cleanup)?;
    drop(items_stream); // free raw bytes before read group building
    planned_files.shrink_to_fit(); // reclaim amortized-doubling slack (~2x → 1x)

    if !planned_files.is_empty() {
        // Build read groups from chunk targets — always via full index.
        let mut groups = if !chunk_targets.is_empty() {
            build_read_groups_via_index(&mut repo, chunk_targets).map_err(&cleanup)?
        } else {
            Vec::new()
        };

        // Free chunk index memory — all pack locations are now in PlannedBlob structs.
        // After this point repo is only used for .storage and .crypto.
        repo.clear_chunk_index();

        // Sort groups by pack ID (shard-aligned) then offset for sequential I/O.
        groups.sort_by(|a, b| {
            a.pack_id
                .0
                .cmp(&b.pack_id.0)
                .then(a.read_start.cmp(&b.read_start))
        });

        debug!(
            "planned {} coalesced read groups for {} files",
            groups.len(),
            planned_files.len(),
        );

        // Phase 3: Ensure parent directories exist + create empty files.
        // Non-empty files are created on first write in Phase 4 (avoids
        // the double-open: create + set_len here, then reopen for writing).
        // Safety: parents verified during directory creation are trusted —
        // this is a single-process operation so no concurrent destination
        // tampering can occur. Unverified parents still get the full
        // canonicalize check.
        let phase3_result: Result<()> = (|| {
            for pf in &planned_files {
                let full_path = temp_root.join(&pf.rel_path);
                if full_path
                    .parent()
                    .is_none_or(|p| !verified_dirs.contains(p))
                {
                    ensure_parent_exists_within_root(&full_path, &temp_root)?;
                }
                if pf.total_size == 0 {
                    std::fs::File::create(&full_path)?;
                }
            }
            Ok(())
        })();
        phase3_result.map_err(&cleanup)?;

        // Phase 4: Parallel restore — download ranges, decrypt, decompress, write.
        let bytes_written = execute_parallel_restore(
            &planned_files,
            groups,
            &repo.storage,
            repo.crypto.as_ref(),
            &temp_root,
            config.limits.restore_concurrency(),
        )
        .map_err(&cleanup)?;

        // Phase 5a: Move all top-level entries from temp root to dest root.
        let move_result: Result<()> = (|| {
            for entry in std::fs::read_dir(&temp_root)? {
                let entry = entry?;
                let final_path = dest_root.join(entry.file_name());
                std::fs::rename(entry.path(), &final_path)?;
            }
            std::fs::remove_dir(&temp_root)?; // now empty
            Ok(())
        })();
        move_result.map_err(&cleanup)?;

        // Phase 5b: Apply file metadata (mode, mtime, xattrs).
        // Use fd-based fchmod/futimens when possible to avoid redundant path
        // lookups (replaces 2 path syscalls per file with 1 open).  Falls back
        // to path-based calls on open failure (e.g. mode 0o000 or 0o200).
        for pf in &planned_files {
            let target_path = dest_root.join(&pf.rel_path);
            // xattrs remain path-based (no fd-based xattr API in std).
            if xattrs_enabled {
                apply_item_xattrs(&target_path, pf.xattrs.as_ref());
            }
            let (mtime_secs, mtime_nanos) = split_unix_nanos(pf.mtime);
            // fd-based fchmod/futimens are Unix-only; on other platforms
            // fall through to the path-based calls to avoid silent no-ops.
            #[cfg(unix)]
            {
                if let Ok(file) = std::fs::File::open(&target_path) {
                    let _ = fs::apply_mode_fd(&file, pf.mode);
                    let _ = fs::set_file_mtime_fd(&file, mtime_secs, mtime_nanos);
                } else {
                    let _ = fs::apply_mode(&target_path, pf.mode);
                    let _ = fs::set_file_mtime(&target_path, mtime_secs, mtime_nanos);
                }
            }
            #[cfg(not(unix))]
            {
                let _ = fs::apply_mode(&target_path, pf.mode);
                let _ = fs::set_file_mtime(&target_path, mtime_secs, mtime_nanos);
            }
        }

        stats.files = planned_files.len() as u64;
        stats.total_bytes = bytes_written;
    } else {
        // No files to restore — just move dirs/symlinks from temp root to dest.
        let move_result: Result<()> = (|| {
            for entry in std::fs::read_dir(&temp_root)? {
                let entry = entry?;
                let final_path = dest_root.join(entry.file_name());
                std::fs::rename(entry.path(), &final_path)?;
            }
            std::fs::remove_dir(&temp_root)?;
            Ok(())
        })();
        move_result.map_err(&cleanup)?;
    }

    info!(
        "Restored {} files, {} dirs, {} symlinks ({} bytes)",
        stats.files, stats.dirs, stats.symlinks, stats.total_bytes
    );

    Ok(stats)
}

// ---------------------------------------------------------------------------
// Streaming item processing — single-pass over raw msgpack bytes
// ---------------------------------------------------------------------------

/// Stream items from raw bytes: create dirs/symlinks immediately, build file
/// plan + chunk targets.  All item types are handled in a single pass over the
/// msgpack stream, avoiding redundant deserialization.
///
/// Directories in the snapshot stream typically precede their children (natural
/// `walkdir` order), so `verified_dirs` is populated before files/symlinks that
/// need it.  When a file or symlink appears before its parent directory item,
/// `ensure_parent_exists_within_root` handles it (the later directory item's
/// `create_dir_all` is a no-op, but mode/xattrs are still applied).
///
/// Because directories are created during decoding, a malformed item stream
/// that fails to decode partway through may leave partial directories on disk.
/// This is acceptable — directory creation is idempotent and restore is not
/// transactional.
#[allow(clippy::type_complexity)]
fn stream_and_plan<F>(
    items_stream: &[u8],
    dest_root: &Path,
    include_path: &mut F,
    xattrs_enabled: bool,
) -> Result<(
    Vec<PlannedFile>,
    HashMap<ChunkId, ChunkTargets>,
    RestoreStats,
    HashSet<PathBuf>,
)>
where
    F: FnMut(&str) -> bool,
{
    let mut stats = RestoreStats::default();
    let mut verified_dirs: HashSet<PathBuf> = HashSet::new();
    verified_dirs.insert(dest_root.to_path_buf());
    let mut planned_files = Vec::new();
    let mut chunk_targets: HashMap<ChunkId, ChunkTargets> = HashMap::new();
    let mut rel_scratch = PathBuf::new();

    super::list::for_each_decoded_item(items_stream, |item| {
        if !include_path(&item.path) {
            return Ok(());
        }
        match item.entry_type {
            ItemType::Directory => {
                sanitize_item_path_into(&item.path, &mut rel_scratch)?;
                let target = dest_root.join(&rel_scratch);
                ensure_path_within_root(&target, dest_root)?;
                std::fs::create_dir_all(&target)?;
                ensure_path_within_root(&target, dest_root)?;
                let _ = fs::apply_mode(&target, item.mode);
                if xattrs_enabled {
                    apply_item_xattrs(&target, item.xattrs.as_ref());
                }
                verified_dirs.insert(target);
                stats.dirs += 1;
            }
            ItemType::Symlink => {
                if let Some(ref link_target) = item.link_target {
                    sanitize_item_path_into(&item.path, &mut rel_scratch)?;
                    let target = dest_root.join(&rel_scratch);
                    if target.parent().is_none_or(|p| !verified_dirs.contains(p)) {
                        ensure_parent_exists_within_root(&target, dest_root)?;
                    }
                    let _ = std::fs::remove_file(&target);
                    fs::create_symlink(Path::new(link_target), &target)?;
                    if xattrs_enabled {
                        apply_item_xattrs(&target, item.xattrs.as_ref());
                    }
                    stats.symlinks += 1;
                }
            }
            ItemType::RegularFile => {
                sanitize_item_path_into(&item.path, &mut rel_scratch)?;
                let file_idx = planned_files.len();
                let mut file_offset: u64 = 0;
                for chunk_ref in &item.chunks {
                    let entry = chunk_targets
                        .entry(chunk_ref.id)
                        .or_insert_with(|| ChunkTargets {
                            expected_size: chunk_ref.size,
                            targets: SmallVec::new(),
                        });
                    if entry.expected_size != chunk_ref.size {
                        return Err(VykarError::InvalidFormat(format!(
                            "chunk {} has inconsistent logical sizes in snapshot metadata: {} vs {}",
                            chunk_ref.id, entry.expected_size, chunk_ref.size
                        )));
                    }
                    entry.targets.push(WriteTarget {
                        file_idx,
                        file_offset,
                    });
                    file_offset += chunk_ref.size as u64;
                }
                planned_files.push(PlannedFile {
                    rel_path: rel_scratch.clone(),
                    total_size: file_offset,
                    mode: item.mode,
                    mtime: item.mtime,
                    xattrs: item.xattrs,
                    created: AtomicBool::new(false),
                });
            }
        }
        Ok(())
    })?;

    Ok((planned_files, chunk_targets, stats, verified_dirs))
}

// ---------------------------------------------------------------------------
// Read planning — group chunks by pack and coalesce adjacent ranges
// ---------------------------------------------------------------------------

/// Aggregated write targets and expected logical size for a single chunk.
struct ChunkTargets {
    expected_size: u32,
    targets: SmallVec<[WriteTarget; 1]>,
}

/// Plan reads using a lookup closure that returns `(pack_id, pack_offset, stored_size)`.
/// The closure abstracts over ChunkIndex vs MmapRestoreCache.
#[cfg(test)]
fn plan_reads<L>(
    file_items: &[(&Item, PathBuf)],
    lookup: L,
) -> Result<(Vec<PlannedFile>, Vec<ReadGroup>)>
where
    L: Fn(&ChunkId) -> Option<(PackId, u64, u32)>,
{
    let mut files: Vec<PlannedFile> = Vec::with_capacity(file_items.len());

    // Collect all (ChunkId → ChunkTargets) across all files.
    let mut chunk_targets: HashMap<ChunkId, ChunkTargets> = HashMap::new();

    for (file_idx, (item, target_path)) in file_items.iter().enumerate() {
        let mut file_offset: u64 = 0;
        for chunk_ref in &item.chunks {
            let entry = chunk_targets
                .entry(chunk_ref.id)
                .or_insert_with(|| ChunkTargets {
                    expected_size: chunk_ref.size,
                    targets: SmallVec::new(),
                });
            if entry.expected_size != chunk_ref.size {
                return Err(VykarError::InvalidFormat(format!(
                    "chunk {} has inconsistent logical sizes in snapshot metadata: {} vs {}",
                    chunk_ref.id, entry.expected_size, chunk_ref.size
                )));
            }
            entry.targets.push(WriteTarget {
                file_idx,
                file_offset,
            });
            file_offset += chunk_ref.size as u64;
        }
        files.push(PlannedFile {
            rel_path: target_path.clone(),
            total_size: file_offset,
            mode: item.mode,
            mtime: item.mtime,
            xattrs: item.xattrs.clone(),
            created: AtomicBool::new(false),
        });
    }

    let groups = build_read_groups(chunk_targets, lookup)?;
    Ok((files, groups))
}

/// Look up each unique chunk's pack location and coalesce into ReadGroups.
/// Consumes `chunk_targets` by value.
fn build_read_groups<L>(
    chunk_targets: HashMap<ChunkId, ChunkTargets>,
    lookup: L,
) -> Result<Vec<ReadGroup>>
where
    L: Fn(&ChunkId) -> Option<(PackId, u64, u32)>,
{
    let mut pack_blobs: HashMap<PackId, Vec<PlannedBlob>> = HashMap::new();

    for (chunk_id, ct) in chunk_targets {
        let (pack_id, pack_offset, stored_size) =
            lookup(&chunk_id).ok_or(VykarError::ChunkNotInIndex(chunk_id))?;
        pack_blobs.entry(pack_id).or_default().push(PlannedBlob {
            chunk_id,
            pack_offset,
            stored_size,
            expected_size: ct.expected_size,
            targets: ct.targets,
        });
    }

    // For each pack: sort blobs by offset, then coalesce into ReadGroups.
    let mut groups: Vec<ReadGroup> = Vec::new();

    for (pack_id, mut blobs) in pack_blobs {
        blobs.sort_by_key(|b| b.pack_offset);

        let mut iter = blobs.into_iter();
        let first = iter.next().unwrap(); // pack_blobs only has non-empty vecs

        let mut cur = ReadGroup {
            pack_id,
            read_start: first.pack_offset,
            read_end: first.pack_offset + first.stored_size as u64,
            blobs: vec![first],
        };

        for blob in iter {
            let blob_end = blob.pack_offset + blob.stored_size as u64;
            let gap = blob.pack_offset.saturating_sub(cur.read_end);
            let merged_size = blob_end - cur.read_start;

            if gap <= MAX_COALESCE_GAP && merged_size <= MAX_READ_SIZE {
                // Coalesce into the current group.
                cur.read_end = blob_end;
                cur.blobs.push(blob);
            } else {
                // Start a new group.
                groups.push(cur);
                cur = ReadGroup {
                    pack_id,
                    read_start: blob.pack_offset,
                    read_end: blob_end,
                    blobs: vec![blob],
                };
            }
        }
        groups.push(cur);
    }

    Ok(groups)
}

/// Load the chunk index (if not already loaded), filter it to only the needed
/// chunks, and build read groups.  Shared by the "cache incomplete" and
/// "no cache" code paths.
fn build_read_groups_via_index(
    repo: &mut Repository,
    chunk_targets: HashMap<ChunkId, ChunkTargets>,
) -> Result<Vec<ReadGroup>> {
    if repo.chunk_index().is_empty() {
        repo.load_chunk_index()?;
    }
    let needed: HashSet<ChunkId> = chunk_targets.keys().copied().collect();
    repo.retain_chunk_index(&needed);
    drop(needed);
    info!(
        "loaded filtered chunk index ({} entries)",
        repo.chunk_index().len()
    );
    let index = repo.chunk_index();
    build_read_groups(chunk_targets, |id| {
        index
            .get(id)
            .map(|e| (e.pack_id, e.pack_offset, e.stored_size))
    })
}

// ---------------------------------------------------------------------------
// Write accumulator
// ---------------------------------------------------------------------------

/// Groups the write accumulator state (buffer, target file, start offset)
/// into a single struct.  One instance is created per `process_read_group`
/// call; its allocation is reused across all sequences within that group.
struct PendingWrite {
    file_idx: usize, // usize::MAX = no active sequence
    start: u64,
    buf: Vec<u8>,
}

impl PendingWrite {
    fn new() -> Self {
        Self {
            file_idx: usize::MAX,
            start: 0,
            buf: Vec::with_capacity(MAX_WRITE_BATCH),
        }
    }

    fn is_active(&self) -> bool {
        self.file_idx != usize::MAX
    }

    fn reset(&mut self) {
        self.buf.clear();
        self.file_idx = usize::MAX;
    }

    /// Start a new accumulation sequence, reusing the existing allocation.
    fn rebind(&mut self, file_idx: usize, start: u64) {
        self.buf.clear();
        self.file_idx = file_idx;
        self.start = start;
    }
}

// ---------------------------------------------------------------------------
// LRU file handle cache
// ---------------------------------------------------------------------------

/// A small LRU cache for open file handles.  Capacity is capped at
/// `MAX_OPEN_FILES_PER_GROUP` (16).  Linear scan is negligible compared
/// to the syscall cost of opening/closing files, and the small size keeps
/// the bookkeeping trivial.
struct LruHandles {
    /// Entries ordered from least-recently-used (front) to most-recently-used (back).
    entries: Vec<(usize, std::fs::File)>,
}

impl LruHandles {
    fn new() -> Self {
        Self {
            entries: Vec::with_capacity(MAX_OPEN_FILES_PER_GROUP),
        }
    }

    /// Return a mutable reference to the file handle for `file_idx`,
    /// promoting it to most-recently-used.  Returns `None` if not cached.
    fn get(&mut self, file_idx: usize) -> Option<&mut std::fs::File> {
        if let Some(pos) = self.entries.iter().position(|(idx, _)| *idx == file_idx) {
            // Move to back (MRU position).
            let entry = self.entries.remove(pos);
            self.entries.push(entry);
            Some(&mut self.entries.last_mut().unwrap().1)
        } else {
            None
        }
    }

    /// Insert a handle for `file_idx`.  If the cache is full, evicts the
    /// least-recently-used entry (front of the vec).
    fn insert(&mut self, file_idx: usize, file: std::fs::File) {
        if self.entries.len() >= MAX_OPEN_FILES_PER_GROUP {
            self.entries.remove(0); // evict LRU
        }
        self.entries.push((file_idx, file));
    }
}

// ---------------------------------------------------------------------------
// Parallel restore execution
// ---------------------------------------------------------------------------

/// Partition `groups` (already sorted by pack_id, offset) into `num_threads`
/// buckets with pack-affinity: groups sharing a pack_id are assigned to the
/// same bucket when possible, falling back to the lightest bucket when the
/// affinity bucket would become a straggler.
fn partition_groups(groups: Vec<ReadGroup>, num_threads: usize) -> Vec<Vec<ReadGroup>> {
    let total_bytes: u64 = groups.iter().map(|g| g.read_end - g.read_start).sum();
    let cap = total_bytes / num_threads as u64 * 13 / 10; // 1.3x fair share

    let mut buckets: Vec<Vec<ReadGroup>> = (0..num_threads).map(|_| Vec::new()).collect();
    let mut bucket_bytes: Vec<u64> = vec![0; num_threads];

    // Track which bucket last saw each pack_id.
    let mut pack_affinity: HashMap<PackId, usize> = HashMap::new();

    for group in groups {
        let group_bytes = group.read_end - group.read_start;

        // Try affinity bucket first.
        let dest = if let Some(&aff) = pack_affinity.get(&group.pack_id) {
            if bucket_bytes[aff] + group_bytes <= cap {
                aff
            } else {
                // Affinity bucket would exceed cap — assign to lightest.
                lightest_bucket(&bucket_bytes)
            }
        } else {
            lightest_bucket(&bucket_bytes)
        };

        pack_affinity.insert(group.pack_id, dest);
        bucket_bytes[dest] += group_bytes;
        buckets[dest].push(group);
    }

    // Drop empty buckets.
    buckets.retain(|b| !b.is_empty());
    buckets
}

/// Return the index of the bucket with the fewest total bytes.
fn lightest_bucket(bucket_bytes: &[u64]) -> usize {
    bucket_bytes
        .iter()
        .enumerate()
        .min_by_key(|(_, &b)| b)
        .map(|(i, _)| i)
        .unwrap_or(0)
}

fn execute_parallel_restore(
    files: &[PlannedFile],
    groups: Vec<ReadGroup>,
    storage: &Arc<dyn StorageBackend>,
    crypto: &dyn CryptoEngine,
    root: &Path,
    restore_concurrency: usize,
) -> Result<u64> {
    if groups.is_empty() {
        return Ok(0);
    }

    let num_threads = restore_concurrency.min(groups.len());
    let buckets = partition_groups(groups, num_threads);

    let bytes_written = AtomicU64::new(0);
    let cancelled = AtomicBool::new(false);

    std::thread::scope(|s| {
        let mut handles = Vec::with_capacity(buckets.len());

        for bucket in &buckets {
            let bytes_written = &bytes_written;
            let cancelled = &cancelled;

            handles.push(s.spawn(move || -> Result<()> {
                let mut data_buf = Vec::new();
                let mut decrypt_buf = Vec::new();
                let mut decompress_buf = Vec::new();

                for group in bucket {
                    if cancelled.load(Ordering::Acquire) {
                        return Ok(());
                    }
                    if let Err(e) = process_read_group(
                        group,
                        files,
                        storage,
                        crypto,
                        bytes_written,
                        cancelled,
                        &mut data_buf,
                        &mut decrypt_buf,
                        &mut decompress_buf,
                        root,
                    ) {
                        cancelled.store(true, Ordering::Release);
                        return Err(e);
                    }

                    // Cap retained buffer capacity to avoid permanent high-water
                    // marks from outlier-large groups/blobs (~50 MiB RSS savings).
                    const BUF_KEEP_CAP: usize = 2 * 1024 * 1024;
                    for buf in [&mut data_buf, &mut decrypt_buf, &mut decompress_buf] {
                        buf.clear();
                        if buf.capacity() > BUF_KEEP_CAP {
                            buf.shrink_to(BUF_KEEP_CAP);
                        }
                    }
                }
                Ok(())
            }));
        }

        let mut first_error: Option<VykarError> = None;
        for handle in handles {
            match handle.join() {
                Ok(Ok(())) => {}
                Ok(Err(e)) => {
                    cancelled.store(true, Ordering::Release);
                    if first_error.is_none() {
                        first_error = Some(e);
                    }
                }
                Err(_panic) => {
                    cancelled.store(true, Ordering::Release);
                    if first_error.is_none() {
                        first_error = Some(VykarError::Other("restore worker panicked".into()));
                    }
                }
            }
        }

        match first_error {
            Some(e) => Err(e),
            None => Ok(bytes_written.load(Ordering::Relaxed)),
        }
    })
}

/// Process a single coalesced read group: download the range, decrypt and
/// decompress each blob, then write the plaintext data to target files.
///
/// `data_buf`, `decrypt_buf`, and `decompress_buf` are caller-owned scratch
/// buffers reused across groups within the same thread, reducing per-group
/// allocation churn. The caller caps retained capacity at 2 MiB between groups.
#[allow(clippy::too_many_arguments)]
fn process_read_group(
    group: &ReadGroup,
    files: &[PlannedFile],
    storage: &Arc<dyn StorageBackend>,
    crypto: &dyn CryptoEngine,
    bytes_written: &AtomicU64,
    cancelled: &AtomicBool,
    data_buf: &mut Vec<u8>,
    decrypt_buf: &mut Vec<u8>,
    decompress_buf: &mut Vec<u8>,
    root: &Path,
) -> Result<()> {
    if cancelled.load(Ordering::Acquire) {
        return Ok(());
    }

    let pack_key = group.pack_id.storage_key();
    let read_len = group.read_end - group.read_start;

    if !storage.get_range_into(&pack_key, group.read_start, read_len, data_buf)? {
        return Err(VykarError::Other(format!(
            "pack not found: {}",
            group.pack_id
        )));
    }
    let data = &data_buf[..];

    let mut file_handles = LruHandles::new();

    // Write accumulator — batches consecutive same-file writes into one syscall.
    // Pre-allocated to MAX_WRITE_BATCH; reused across sequences within this group.
    let mut pw = PendingWrite::new();

    for blob in &group.blobs {
        if cancelled.load(Ordering::Acquire) {
            // Dropping `pw` loses buffered data, but the caller will
            // clean up the temp restore root on error/cancellation so
            // no partial files reach the final destination.
            return Ok(());
        }

        let local_offset = (blob.pack_offset - group.read_start) as usize;
        let local_end = local_offset + blob.stored_size as usize;

        if local_end > data.len() {
            return Err(VykarError::Other(format!(
                "blob extends beyond downloaded range in pack {}",
                group.pack_id
            )));
        }

        let raw = &data[local_offset..local_end];
        unpack_object_expect_with_context_into(
            raw,
            ObjectType::ChunkData,
            &blob.chunk_id.0,
            crypto,
            decrypt_buf,
        )
        .map_err(|e| {
            VykarError::Other(format!(
                "chunk {} in pack {} (offset {}, size {}): {e}",
                blob.chunk_id, group.pack_id, blob.pack_offset, blob.stored_size
            ))
        })?;
        compress::decompress_into_with_hint(
            decrypt_buf,
            Some(blob.expected_size as usize),
            decompress_buf,
        )
        .map_err(|e| {
            VykarError::Other(format!(
                "chunk {} in pack {} (offset {}, size {}): {e}",
                blob.chunk_id, group.pack_id, blob.pack_offset, blob.stored_size
            ))
        })?;

        if decompress_buf.len() != blob.expected_size as usize {
            return Err(VykarError::InvalidFormat(format!(
                "chunk {} in pack {} (offset {}, size {}) size mismatch after restore decode: \
                 expected {} bytes, got {} bytes",
                blob.chunk_id,
                group.pack_id,
                blob.pack_offset,
                blob.stored_size,
                blob.expected_size,
                decompress_buf.len()
            )));
        }

        for target in &blob.targets {
            let contiguous = pw.is_active()
                && pw.file_idx == target.file_idx
                && target.file_offset == pw.start + pw.buf.len() as u64;
            let can_append = contiguous && pw.buf.len() + decompress_buf.len() <= MAX_WRITE_BATCH;

            if can_append {
                pw.buf.extend_from_slice(decompress_buf);
                // Eager flush when batch is full.
                if pw.buf.len() >= MAX_WRITE_BATCH {
                    write_buf(
                        &pw.buf,
                        pw.file_idx,
                        pw.start,
                        &mut file_handles,
                        files,
                        bytes_written,
                        root,
                    )?;
                    pw.reset();
                }
            } else {
                // Flush any active pending write first.
                flush_pending(&mut pw, &mut file_handles, files, bytes_written, root)?;

                if contiguous || decompress_buf.len() >= MAX_WRITE_BATCH {
                    // Contiguous overflow or standalone large chunk — write
                    // directly from decompress_buf so no data lingers in the
                    // accumulator (same cancellation safety as the old code).
                    write_buf(
                        decompress_buf,
                        target.file_idx,
                        target.file_offset,
                        &mut file_handles,
                        files,
                        bytes_written,
                        root,
                    )?;
                } else {
                    // Start new accumulation sequence (reuses existing allocation).
                    pw.rebind(target.file_idx, target.file_offset);
                    pw.buf.extend_from_slice(decompress_buf);
                }
            }
        }
    }

    // Flush any remaining pending write.
    flush_pending(&mut pw, &mut file_handles, files, bytes_written, root)?;

    Ok(())
}

/// Write `buf` to the target file at `offset`.  Opens/creates the file handle
/// on first access and calls CAS `set_len`.  Increments `bytes_written`.
fn write_buf(
    buf: &[u8],
    file_idx: usize,
    offset: u64,
    file_handles: &mut LruHandles,
    files: &[PlannedFile],
    bytes_written: &AtomicU64,
    root: &Path,
) -> Result<()> {
    if file_handles.get(file_idx).is_none() {
        let pf = &files[file_idx];
        let full_path = root.join(&pf.rel_path);
        // Create-on-first-write: truncate(false) prevents one worker from
        // destroying another worker's writes to the same file.
        let handle = std::fs::OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(false)
            .open(&full_path)
            .map_err(|e| {
                VykarError::Other(format!(
                    "failed to open {} for writing: {e}",
                    full_path.display()
                ))
            })?;
        // CAS: only the first opener calls set_len (ftruncate).
        if !pf.created.swap(true, Ordering::AcqRel) {
            handle.set_len(pf.total_size)?;
        }
        file_handles.insert(file_idx, handle);
    }
    let fh = file_handles
        .get(file_idx)
        .ok_or_else(|| VykarError::Other("missing file handle in restore worker".into()))?;
    #[cfg(unix)]
    fh.write_all_at(buf, offset)?;
    #[cfg(not(unix))]
    {
        fh.seek(std::io::SeekFrom::Start(offset))?;
        fh.write_all(buf)?;
    }
    bytes_written.fetch_add(buf.len() as u64, Ordering::Relaxed);
    Ok(())
}

/// Flush the write accumulator to disk if it contains data, then reset it.
fn flush_pending(
    pw: &mut PendingWrite,
    file_handles: &mut LruHandles,
    files: &[PlannedFile],
    bytes_written: &AtomicU64,
    root: &Path,
) -> Result<()> {
    if pw.is_active() && !pw.buf.is_empty() {
        write_buf(
            &pw.buf,
            pw.file_idx,
            pw.start,
            file_handles,
            files,
            bytes_written,
            root,
        )?;
    }
    pw.reset();
    Ok(())
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Check if a path matches the selection set.
/// A path matches if it's exactly in the set, or any prefix (ancestor) is in the set.
fn path_matches_selection(path: &str, selected: &HashSet<String>) -> bool {
    if selected.contains(path) {
        return true;
    }
    // Check if any ancestor is selected
    let mut current = path;
    while let Some(slash_idx) = current.rfind('/') {
        current = &current[..slash_idx];
        if selected.contains(current) {
            return true;
        }
    }
    false
}

fn apply_item_xattrs(target: &Path, xattrs: Option<&HashMap<String, Vec<u8>>>) {
    let Some(xattrs) = xattrs else {
        return;
    };

    let mut names: Vec<&str> = xattrs.keys().map(String::as_str).collect();
    names.sort_unstable();

    for name in names {
        let Some(value) = xattrs.get(name) else {
            continue;
        };

        #[cfg(unix)]
        if let Err(e) = xattr::set(target, name, value) {
            tracing::warn!(
                path = %target.display(),
                attr = %name,
                error = %e,
                "failed to restore extended attribute"
            );
        }
        #[cfg(not(unix))]
        {
            let _ = target;
            let _ = name;
            let _ = value;
        }
    }
}

#[derive(Debug, Default)]
pub struct RestoreStats {
    pub files: u64,
    pub dirs: u64,
    pub symlinks: u64,
    pub total_bytes: u64,
}

/// Sanitize and write a snapshot item path into a caller-provided scratch buffer.
/// Reuses the PathBuf allocation across calls (~387K items), avoiding per-item
/// intermediate PathBuf allocations.
fn sanitize_item_path_into(raw: &str, out: &mut PathBuf) -> Result<()> {
    let path = Path::new(raw);
    if path.is_absolute() {
        return Err(VykarError::InvalidFormat(format!(
            "refusing to restore absolute path: {raw}"
        )));
    }
    out.clear();
    for component in path.components() {
        match component {
            Component::Normal(part) => out.push(part),
            Component::CurDir => {}
            Component::ParentDir | Component::RootDir | Component::Prefix(_) => {
                return Err(VykarError::InvalidFormat(format!(
                    "refusing to restore unsafe path: {raw}"
                )));
            }
        }
    }
    if out.as_os_str().is_empty() {
        return Err(VykarError::InvalidFormat(format!(
            "refusing to restore empty path: {raw}"
        )));
    }
    Ok(())
}

#[cfg(test)]
fn sanitize_item_path(raw: &str) -> Result<PathBuf> {
    let path = Path::new(raw);
    if path.is_absolute() {
        return Err(VykarError::InvalidFormat(format!(
            "refusing to restore absolute path: {raw}"
        )));
    }
    let mut out = PathBuf::new();
    for component in path.components() {
        match component {
            Component::Normal(part) => out.push(part),
            Component::CurDir => {}
            Component::ParentDir | Component::RootDir | Component::Prefix(_) => {
                return Err(VykarError::InvalidFormat(format!(
                    "refusing to restore unsafe path: {raw}"
                )));
            }
        }
    }
    if out.as_os_str().is_empty() {
        return Err(VykarError::InvalidFormat(format!(
            "refusing to restore empty path: {raw}"
        )));
    }
    Ok(out)
}

/// Validate the restore destination: must be non-existing or empty.
/// Creates the directory if it doesn't exist. Returns the canonicalized path.
fn validate_and_prepare_dest(dest: &str) -> Result<PathBuf> {
    let dest_path = Path::new(dest);
    if dest_path.exists() {
        let is_empty = dest_path
            .read_dir()
            .map_err(|e| VykarError::Other(format!("cannot read destination '{}': {e}", dest)))?
            .next()
            .is_none();
        if !is_empty {
            return Err(VykarError::Config(format!(
                "restore destination '{}' is not empty; use an empty or non-existing directory",
                dest
            )));
        }
    } else {
        std::fs::create_dir_all(dest_path)?;
    }
    dest_path
        .canonicalize()
        .map_err(|e| VykarError::Other(format!("invalid destination '{}': {e}", dest)))
}

fn ensure_parent_exists_within_root(target: &Path, root: &Path) -> Result<()> {
    if let Some(parent) = target.parent() {
        ensure_path_within_root(parent, root)?;
        std::fs::create_dir_all(parent)?;
        ensure_path_within_root(parent, root)?;
    }
    Ok(())
}

fn ensure_path_within_root(path: &Path, root: &Path) -> Result<()> {
    let mut cursor = Some(path);
    while let Some(candidate) = cursor {
        if candidate.exists() {
            let canonical = candidate
                .canonicalize()
                .map_err(|e| VykarError::Other(format!("path check failed: {e}")))?;
            if !canonical.starts_with(root) {
                return Err(VykarError::InvalidFormat(format!(
                    "refusing to restore outside destination: {}",
                    path.display()
                )));
            }
            return Ok(());
        }
        cursor = candidate.parent();
    }
    Err(VykarError::InvalidFormat(format!(
        "invalid restore target path: {}",
        path.display()
    )))
}

fn split_unix_nanos(total_nanos: i64) -> (i64, u32) {
    let secs = total_nanos.div_euclid(1_000_000_000);
    let nanos = total_nanos.rem_euclid(1_000_000_000) as u32;
    (secs, nanos)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::AtomicBool;
    use std::sync::Arc;

    use tempfile::tempdir;

    use crate::compress::Compression;
    use crate::index::ChunkIndex;
    use crate::repo::format::pack_object_with_context;
    use crate::snapshot::item::{ChunkRef, Item, ItemType};
    use crate::testutil::{test_chunk_id_key, MemoryBackend};
    use vykar_crypto::PlaintextEngine;
    use vykar_storage::StorageBackend;
    use vykar_types::chunk_id::ChunkId;
    use vykar_types::pack_id::PackId;

    #[test]
    fn split_unix_nanos_handles_negative_values() {
        let (secs, nanos) = split_unix_nanos(-1);
        assert_eq!(secs, -1);
        assert_eq!(nanos, 999_999_999);
    }

    #[test]
    fn split_unix_nanos_handles_positive_values() {
        let (secs, nanos) = split_unix_nanos(1_500_000_000);
        assert_eq!(secs, 1);
        assert_eq!(nanos, 500_000_000);
    }

    #[test]
    fn path_matches_selection_ancestors() {
        let mut selected = HashSet::new();
        selected.insert("docs/notes".to_string());
        assert!(path_matches_selection("docs/notes/todo.txt", &selected));
        assert!(path_matches_selection("docs/notes", &selected));
        assert!(!path_matches_selection("docs/other", &selected));
        assert!(!path_matches_selection("documents", &selected));
    }

    #[test]
    fn sanitize_rejects_parent_dir_traversal() {
        let err = sanitize_item_path("../etc/passwd").unwrap_err().to_string();
        assert!(err.contains("unsafe path"));
    }

    // -----------------------------------------------------------------------
    // plan_reads tests
    // -----------------------------------------------------------------------

    fn dummy_chunk_id(byte: u8) -> ChunkId {
        ChunkId([byte; 32])
    }

    fn dummy_pack_id(byte: u8) -> PackId {
        PackId([byte; 32])
    }

    /// Helper: create a lookup closure from a ChunkIndex.
    fn index_lookup(index: &ChunkIndex) -> impl Fn(&ChunkId) -> Option<(PackId, u64, u32)> + '_ {
        move |id| {
            index
                .get(id)
                .map(|e| (e.pack_id, e.pack_offset, e.stored_size))
        }
    }

    fn make_file_item(path: &str, chunks: Vec<(u8, u32)>) -> Item {
        Item {
            path: path.to_string(),
            entry_type: ItemType::RegularFile,
            mode: 0o644,
            uid: 1000,
            gid: 1000,
            user: None,
            group: None,
            mtime: 0,
            atime: None,
            ctime: None,
            size: chunks.iter().map(|(_, s)| *s as u64).sum(),
            chunks: chunks
                .into_iter()
                .map(|(id_byte, size)| ChunkRef {
                    id: dummy_chunk_id(id_byte),
                    size,
                    csize: size, // not used by plan_reads
                })
                .collect(),
            link_target: None,
            xattrs: None,
        }
    }

    #[test]
    fn plan_reads_single_blob_per_pack() {
        let pack = dummy_pack_id(1);
        let mut index = ChunkIndex::new();
        index.add(dummy_chunk_id(0xAA), 100, pack, 1000);

        let item = make_file_item("a.txt", vec![(0xAA, 200)]);
        let file_items = vec![(&item, PathBuf::from("/tmp/out/a.txt"))];

        let (files, groups) = plan_reads(&file_items, index_lookup(&index)).unwrap();
        assert_eq!(files.len(), 1);
        assert_eq!(files[0].total_size, 200);
        assert_eq!(groups.len(), 1);
        assert_eq!(groups[0].blobs.len(), 1);
        assert_eq!(groups[0].read_start, 1000);
        assert_eq!(groups[0].read_end, 1100);
    }

    #[test]
    fn plan_reads_coalesces_adjacent_blobs() {
        let pack = dummy_pack_id(1);
        let mut index = ChunkIndex::new();
        // Two blobs close together in the same pack (gap = 4 bytes for length prefix)
        index.add(dummy_chunk_id(0xAA), 100, pack, 1000);
        index.add(dummy_chunk_id(0xBB), 100, pack, 1104); // 1000 + 100 + 4 = 1104

        let item = make_file_item("a.txt", vec![(0xAA, 200), (0xBB, 300)]);
        let file_items = vec![(&item, PathBuf::from("/tmp/out/a.txt"))];

        let (files, groups) = plan_reads(&file_items, index_lookup(&index)).unwrap();
        assert_eq!(files.len(), 1);
        assert_eq!(files[0].total_size, 500);
        // Both blobs should be coalesced into one ReadGroup
        assert_eq!(groups.len(), 1);
        assert_eq!(groups[0].blobs.len(), 2);
        assert_eq!(groups[0].read_start, 1000);
        assert_eq!(groups[0].read_end, 1204);
    }

    #[test]
    fn plan_reads_splits_on_large_gap() {
        let pack = dummy_pack_id(1);
        let mut index = ChunkIndex::new();
        // Two blobs far apart (gap > MAX_COALESCE_GAP)
        index.add(dummy_chunk_id(0xAA), 100, pack, 1000);
        index.add(
            dummy_chunk_id(0xBB),
            100,
            pack,
            1000 + 100 + MAX_COALESCE_GAP + 1,
        );

        let item = make_file_item("a.txt", vec![(0xAA, 200), (0xBB, 300)]);
        let file_items = vec![(&item, PathBuf::from("/tmp/out/a.txt"))];

        let (_files, groups) = plan_reads(&file_items, index_lookup(&index)).unwrap();
        // Should be split into two ReadGroups
        assert_eq!(groups.len(), 2);
        assert_eq!(groups[0].blobs.len(), 1);
        assert_eq!(groups[1].blobs.len(), 1);
    }

    #[test]
    fn plan_reads_splits_on_max_read_size() {
        let pack = dummy_pack_id(1);
        let mut index = ChunkIndex::new();
        // First blob takes up almost MAX_READ_SIZE, second would push it over
        let big_size = MAX_READ_SIZE as u32 - 100;
        index.add(dummy_chunk_id(0xAA), big_size, pack, 1000);
        index.add(dummy_chunk_id(0xBB), 200, pack, 1000 + big_size as u64 + 4);

        let item = make_file_item("a.txt", vec![(0xAA, 5000), (0xBB, 300)]);
        let file_items = vec![(&item, PathBuf::from("/tmp/out/a.txt"))];

        let (_files, groups) = plan_reads(&file_items, index_lookup(&index)).unwrap();
        // Should be split because merged_size > MAX_READ_SIZE
        assert_eq!(groups.len(), 2);
    }

    #[test]
    fn plan_reads_dedup_across_files() {
        let pack = dummy_pack_id(1);
        let mut index = ChunkIndex::new();
        index.add(dummy_chunk_id(0xAA), 100, pack, 1000);

        // Two files sharing the same chunk
        let item_a = make_file_item("a.txt", vec![(0xAA, 200)]);
        let item_b = make_file_item("b.txt", vec![(0xAA, 200)]);
        let file_items = vec![
            (&item_a, PathBuf::from("/tmp/out/a.txt")),
            (&item_b, PathBuf::from("/tmp/out/b.txt")),
        ];

        let (files, groups) = plan_reads(&file_items, index_lookup(&index)).unwrap();
        assert_eq!(files.len(), 2);
        // Only one ReadGroup since it's the same chunk
        assert_eq!(groups.len(), 1);
        assert_eq!(groups[0].blobs.len(), 1);
        // The blob should have two write targets
        assert_eq!(groups[0].blobs[0].targets.len(), 2);
    }

    #[test]
    fn plan_reads_rejects_inconsistent_logical_chunk_sizes() {
        let pack = dummy_pack_id(1);
        let mut index = ChunkIndex::new();
        index.add(dummy_chunk_id(0xAA), 100, pack, 1000);

        let item_a = make_file_item("a.txt", vec![(0xAA, 200)]);
        let item_b = make_file_item("b.txt", vec![(0xAA, 300)]);
        let file_items = vec![
            (&item_a, PathBuf::from("/tmp/out/a.txt")),
            (&item_b, PathBuf::from("/tmp/out/b.txt")),
        ];

        let err = match plan_reads(&file_items, index_lookup(&index)) {
            Ok(_) => panic!("expected inconsistent logical size error"),
            Err(e) => e.to_string(),
        };
        assert!(err.contains("inconsistent logical sizes"));
    }

    #[test]
    fn plan_reads_empty_file_no_groups() {
        let index = ChunkIndex::new();
        let item = make_file_item("empty.txt", vec![]);
        let file_items = vec![(&item, PathBuf::from("/tmp/out/empty.txt"))];

        let (files, groups) = plan_reads(&file_items, index_lookup(&index)).unwrap();
        assert_eq!(files.len(), 1);
        assert_eq!(files[0].total_size, 0);
        assert_eq!(groups.len(), 0);
    }

    #[test]
    fn plan_reads_multiple_packs() {
        let pack_a = dummy_pack_id(1);
        let pack_b = dummy_pack_id(2);
        let mut index = ChunkIndex::new();
        index.add(dummy_chunk_id(0xAA), 100, pack_a, 1000);
        index.add(dummy_chunk_id(0xBB), 100, pack_b, 2000);

        let item = make_file_item("a.txt", vec![(0xAA, 200), (0xBB, 300)]);
        let file_items = vec![(&item, PathBuf::from("/tmp/out/a.txt"))];

        let (_files, groups) = plan_reads(&file_items, index_lookup(&index)).unwrap();
        // Separate packs → separate ReadGroups
        assert_eq!(groups.len(), 2);
    }

    #[test]
    fn process_read_group_rejects_decode_size_mismatch() {
        let temp = tempdir().unwrap();
        let out = temp.path().join("out.bin");
        let file = std::fs::File::create(&out).unwrap();
        file.set_len(5).unwrap();

        let files = vec![PlannedFile {
            rel_path: PathBuf::from("out.bin"),
            total_size: 5,
            mode: 0o644,
            mtime: 0,
            xattrs: None,
            created: AtomicBool::new(true), // pre-created in test
        }];

        let payload = b"abc";
        let compressed = crate::compress::compress(Compression::None, payload).unwrap();
        let crypto = PlaintextEngine::new(&test_chunk_id_key());
        let packed = pack_object_with_context(
            ObjectType::ChunkData,
            &dummy_chunk_id(0xAA).0,
            &compressed,
            &crypto,
        )
        .unwrap();

        let pack_id = dummy_pack_id(9);
        let backend = Arc::new(MemoryBackend::new());
        backend.put(&pack_id.storage_key(), &packed).unwrap();
        let storage: Arc<dyn StorageBackend> = backend;

        let group = ReadGroup {
            pack_id,
            read_start: 0,
            read_end: packed.len() as u64,
            blobs: vec![PlannedBlob {
                chunk_id: dummy_chunk_id(0xAA),
                pack_offset: 0,
                stored_size: packed.len() as u32,
                expected_size: 5,
                targets: smallvec::smallvec![WriteTarget {
                    file_idx: 0,
                    file_offset: 0,
                }],
            }],
        };

        let bytes_written = AtomicU64::new(0);
        let cancelled = AtomicBool::new(false);
        let mut data_buf = Vec::new();
        let mut decrypt_buf = Vec::new();
        let mut decompress_buf = Vec::new();
        let err = process_read_group(
            &group,
            &files,
            &storage,
            &crypto,
            &bytes_written,
            &cancelled,
            &mut data_buf,
            &mut decrypt_buf,
            &mut decompress_buf,
            temp.path(),
        )
        .unwrap_err()
        .to_string();
        assert!(
            err.contains("size mismatch after restore decode"),
            "expected size mismatch error, got: {err}"
        );
    }

    // -----------------------------------------------------------------------
    // process_read_group write-loop tests
    // -----------------------------------------------------------------------

    /// Helper: compress + encrypt + pack a raw payload into a RepoObj blob.
    fn pack_blob(chunk_id: ChunkId, data: &[u8], crypto: &dyn CryptoEngine) -> Vec<u8> {
        let compressed = crate::compress::compress(Compression::None, data).unwrap();
        pack_object_with_context(ObjectType::ChunkData, &chunk_id.0, &compressed, crypto).unwrap()
    }

    /// Build a single-blob ReadGroup from the given packed bytes.
    fn single_blob_group(
        pack_id: PackId,
        chunk_id: ChunkId,
        packed: &[u8],
        expected_size: u32,
        targets: SmallVec<[WriteTarget; 1]>,
    ) -> ReadGroup {
        ReadGroup {
            pack_id,
            read_start: 0,
            read_end: packed.len() as u64,
            blobs: vec![PlannedBlob {
                chunk_id,
                pack_offset: 0,
                stored_size: packed.len() as u32,
                expected_size,
                targets,
            }],
        }
    }

    /// Concatenate multiple packed blobs into one pack buffer and build a
    /// ReadGroup with one PlannedBlob per entry.
    #[allow(clippy::type_complexity)]
    fn multi_blob_group(
        pack_id: PackId,
        entries: Vec<(ChunkId, Vec<u8>, u32, SmallVec<[WriteTarget; 1]>)>,
    ) -> (Vec<u8>, ReadGroup) {
        let mut pack_data = Vec::new();
        let mut blobs = Vec::new();
        for (chunk_id, packed, expected_size, targets) in entries {
            let offset = pack_data.len() as u64;
            let stored_size = packed.len() as u32;
            pack_data.extend_from_slice(&packed);
            blobs.push(PlannedBlob {
                chunk_id,
                pack_offset: offset,
                stored_size,
                expected_size,
                targets,
            });
        }
        let group = ReadGroup {
            pack_id,
            read_start: 0,
            read_end: pack_data.len() as u64,
            blobs,
        };
        (pack_data, group)
    }

    #[test]
    fn process_read_group_large_chunk_direct_write() {
        // A single blob with expected_size >= MAX_WRITE_BATCH should be
        // written directly from decompress_buf, bypassing the accumulator.
        let temp = tempdir().unwrap();
        let out = temp.path().join("big.bin");

        let big_data = vec![0xABu8; MAX_WRITE_BATCH + 1024];
        let crypto = PlaintextEngine::new(&test_chunk_id_key());
        let cid = dummy_chunk_id(0x01);
        let packed = pack_blob(cid, &big_data, &crypto);

        let files = vec![PlannedFile {
            rel_path: PathBuf::from("big.bin"),
            total_size: big_data.len() as u64,
            mode: 0o644,
            mtime: 0,
            xattrs: None,
            created: AtomicBool::new(false),
        }];

        let pack_id = dummy_pack_id(1);
        let backend = Arc::new(MemoryBackend::new());
        backend.put(&pack_id.storage_key(), &packed).unwrap();
        let storage: Arc<dyn StorageBackend> = backend;

        let group = single_blob_group(
            pack_id,
            cid,
            &packed,
            big_data.len() as u32,
            smallvec::smallvec![WriteTarget {
                file_idx: 0,
                file_offset: 0,
            }],
        );

        let bytes_written = AtomicU64::new(0);
        let cancelled = AtomicBool::new(false);
        let mut data_buf = Vec::new();
        let mut decrypt_buf = Vec::new();
        let mut decompress_buf = Vec::new();
        process_read_group(
            &group,
            &files,
            &storage,
            &crypto,
            &bytes_written,
            &cancelled,
            &mut data_buf,
            &mut decrypt_buf,
            &mut decompress_buf,
            temp.path(),
        )
        .unwrap();

        assert_eq!(bytes_written.load(Ordering::Relaxed), big_data.len() as u64);
        let contents = std::fs::read(&out).unwrap();
        assert_eq!(contents, big_data);
    }

    #[test]
    fn process_read_group_exact_batch_boundary() {
        // Two consecutive contiguous blobs whose cumulative size hits exactly
        // MAX_WRITE_BATCH.  Verifies the eager flush fires at the boundary.
        let temp = tempdir().unwrap();
        let out = temp.path().join("exact.bin");

        let half = MAX_WRITE_BATCH / 2;
        let chunk_a = vec![0xAAu8; half];
        let chunk_b = vec![0xBBu8; MAX_WRITE_BATCH - half];

        let crypto = PlaintextEngine::new(&test_chunk_id_key());
        let cid_a = dummy_chunk_id(0x0A);
        let cid_b = dummy_chunk_id(0x0B);
        let packed_a = pack_blob(cid_a, &chunk_a, &crypto);
        let packed_b = pack_blob(cid_b, &chunk_b, &crypto);

        let total_size = (chunk_a.len() + chunk_b.len()) as u64;
        let files = vec![PlannedFile {
            rel_path: PathBuf::from("exact.bin"),
            total_size,
            mode: 0o644,
            mtime: 0,
            xattrs: None,
            created: AtomicBool::new(false),
        }];

        let pack_id = dummy_pack_id(2);
        let (pack_data, group) = multi_blob_group(
            pack_id,
            vec![
                (
                    cid_a,
                    packed_a,
                    chunk_a.len() as u32,
                    smallvec::smallvec![WriteTarget {
                        file_idx: 0,
                        file_offset: 0,
                    }],
                ),
                (
                    cid_b,
                    packed_b,
                    chunk_b.len() as u32,
                    smallvec::smallvec![WriteTarget {
                        file_idx: 0,
                        file_offset: chunk_a.len() as u64,
                    }],
                ),
            ],
        );

        let backend = Arc::new(MemoryBackend::new());
        backend.put(&pack_id.storage_key(), &pack_data).unwrap();
        let storage: Arc<dyn StorageBackend> = backend;

        let bytes_written = AtomicU64::new(0);
        let cancelled = AtomicBool::new(false);
        let mut data_buf = Vec::new();
        let mut decrypt_buf = Vec::new();
        let mut decompress_buf = Vec::new();
        process_read_group(
            &group,
            &files,
            &storage,
            &crypto,
            &bytes_written,
            &cancelled,
            &mut data_buf,
            &mut decrypt_buf,
            &mut decompress_buf,
            temp.path(),
        )
        .unwrap();

        assert_eq!(bytes_written.load(Ordering::Relaxed), total_size);
        let contents = std::fs::read(&out).unwrap();
        let mut expected = chunk_a;
        expected.extend_from_slice(&chunk_b);
        assert_eq!(contents, expected);
    }

    #[test]
    fn process_read_group_cross_file_switch() {
        // Two blobs targeting different files in the same read group.
        // Verifies flush occurs on file switch and both files get correct content.
        let temp = tempdir().unwrap();
        let out_a = temp.path().join("a.bin");
        let out_b = temp.path().join("b.bin");

        let data_a = vec![0xAAu8; 4096];
        let data_b = vec![0xBBu8; 8192];

        let crypto = PlaintextEngine::new(&test_chunk_id_key());
        let cid_a = dummy_chunk_id(0x0A);
        let cid_b = dummy_chunk_id(0x0B);
        let packed_a = pack_blob(cid_a, &data_a, &crypto);
        let packed_b = pack_blob(cid_b, &data_b, &crypto);

        let files = vec![
            PlannedFile {
                rel_path: PathBuf::from("a.bin"),
                total_size: data_a.len() as u64,
                mode: 0o644,
                mtime: 0,
                xattrs: None,
                created: AtomicBool::new(false),
            },
            PlannedFile {
                rel_path: PathBuf::from("b.bin"),
                total_size: data_b.len() as u64,
                mode: 0o644,
                mtime: 0,
                xattrs: None,
                created: AtomicBool::new(false),
            },
        ];

        let pack_id = dummy_pack_id(3);
        let (pack_data, group) = multi_blob_group(
            pack_id,
            vec![
                (
                    cid_a,
                    packed_a,
                    data_a.len() as u32,
                    smallvec::smallvec![WriteTarget {
                        file_idx: 0,
                        file_offset: 0,
                    }],
                ),
                (
                    cid_b,
                    packed_b,
                    data_b.len() as u32,
                    smallvec::smallvec![WriteTarget {
                        file_idx: 1,
                        file_offset: 0,
                    }],
                ),
            ],
        );

        let backend = Arc::new(MemoryBackend::new());
        backend.put(&pack_id.storage_key(), &pack_data).unwrap();
        let storage: Arc<dyn StorageBackend> = backend;

        let bytes_written = AtomicU64::new(0);
        let cancelled = AtomicBool::new(false);
        let mut data_buf = Vec::new();
        let mut decrypt_buf = Vec::new();
        let mut decompress_buf = Vec::new();
        process_read_group(
            &group,
            &files,
            &storage,
            &crypto,
            &bytes_written,
            &cancelled,
            &mut data_buf,
            &mut decrypt_buf,
            &mut decompress_buf,
            temp.path(),
        )
        .unwrap();

        let total = (data_a.len() + data_b.len()) as u64;
        assert_eq!(bytes_written.load(Ordering::Relaxed), total);
        assert_eq!(std::fs::read(&out_a).unwrap(), data_a);
        assert_eq!(std::fs::read(&out_b).unwrap(), data_b);
    }

    #[test]
    fn process_read_group_contiguous_batching() {
        // Multiple small contiguous blobs to the same file should be batched
        // into fewer writes.  We verify correct content at the end.
        let temp = tempdir().unwrap();
        let out = temp.path().join("batched.bin");

        let num_chunks = 8u8;
        let chunk_size = 1024usize; // well under MAX_WRITE_BATCH
        let crypto = PlaintextEngine::new(&test_chunk_id_key());

        let mut entries = Vec::new();
        let mut expected_data = Vec::new();
        for i in 0u8..num_chunks {
            let data = vec![0x10 + i; chunk_size];
            expected_data.extend_from_slice(&data);
            let cid = dummy_chunk_id(0x10 + i);
            let packed = pack_blob(cid, &data, &crypto);
            entries.push((
                cid,
                packed,
                chunk_size as u32,
                smallvec::smallvec![WriteTarget {
                    file_idx: 0,
                    file_offset: (i as u64) * (chunk_size as u64),
                }],
            ));
        }

        let files = vec![PlannedFile {
            rel_path: PathBuf::from("batched.bin"),
            total_size: expected_data.len() as u64,
            mode: 0o644,
            mtime: 0,
            xattrs: None,
            created: AtomicBool::new(false),
        }];

        let pack_id = dummy_pack_id(4);
        let (pack_data, group) = multi_blob_group(pack_id, entries);

        let backend = Arc::new(MemoryBackend::new());
        backend.put(&pack_id.storage_key(), &pack_data).unwrap();
        let storage: Arc<dyn StorageBackend> = backend;

        let bytes_written = AtomicU64::new(0);
        let cancelled = AtomicBool::new(false);
        let mut data_buf = Vec::new();
        let mut decrypt_buf = Vec::new();
        let mut decompress_buf = Vec::new();
        process_read_group(
            &group,
            &files,
            &storage,
            &crypto,
            &bytes_written,
            &cancelled,
            &mut data_buf,
            &mut decrypt_buf,
            &mut decompress_buf,
            temp.path(),
        )
        .unwrap();

        assert_eq!(
            bytes_written.load(Ordering::Relaxed),
            expected_data.len() as u64
        );
        assert_eq!(std::fs::read(&out).unwrap(), expected_data);
    }

    // -----------------------------------------------------------------------
    // stream_and_plan tests
    // -----------------------------------------------------------------------

    fn make_dir_item(path: &str, mode: u32) -> Item {
        Item {
            path: path.to_string(),
            entry_type: ItemType::Directory,
            mode,
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
        }
    }

    fn make_symlink_item(path: &str, target: &str) -> Item {
        Item {
            path: path.to_string(),
            entry_type: ItemType::Symlink,
            mode: 0o777,
            uid: 1000,
            gid: 1000,
            user: None,
            group: None,
            mtime: 0,
            atime: None,
            ctime: None,
            size: 0,
            chunks: Vec::new(),
            link_target: Some(target.to_string()),
            xattrs: None,
        }
    }

    fn serialize_items(items: &[Item]) -> Vec<u8> {
        let mut bytes = Vec::new();
        for item in items {
            bytes.extend_from_slice(&rmp_serde::to_vec(item).unwrap());
        }
        bytes
    }

    #[test]
    fn stream_and_plan_dirs_before_files() {
        let temp = tempdir().unwrap();
        let dest = &temp.path().canonicalize().unwrap();

        // Serialize in reverse order: file before its parent dir.
        let items = vec![
            make_file_item("mydir/a.txt", vec![(0xAA, 100)]),
            make_dir_item("mydir", 0o755),
        ];
        let stream = serialize_items(&items);

        let (planned_files, chunk_targets, stats, _verified_dirs) =
            stream_and_plan(&stream, dest, &mut |_| true, false).unwrap();

        // Directory was created (pass 1 runs before pass 2).
        assert!(dest.join("mydir").is_dir());
        assert_eq!(stats.dirs, 1);

        // File is in planned_files.
        assert_eq!(planned_files.len(), 1);
        assert_eq!(planned_files[0].rel_path, Path::new("mydir/a.txt"));
        assert_eq!(chunk_targets.len(), 1);
    }

    #[test]
    fn stream_and_plan_respects_filter() {
        let temp = tempdir().unwrap();
        let dest = &temp.path().canonicalize().unwrap();

        let items = vec![
            make_dir_item("included", 0o755),
            make_dir_item("excluded", 0o755),
            make_file_item("included/a.txt", vec![(0xAA, 100)]),
            make_file_item("excluded/b.txt", vec![(0xBB, 200)]),
        ];
        let stream = serialize_items(&items);

        let (planned_files, _chunk_targets, stats, _verified_dirs) = stream_and_plan(
            &stream,
            dest,
            &mut |p: &str| p.starts_with("included"),
            false,
        )
        .unwrap();

        // Only the included directory was created.
        assert!(dest.join("included").is_dir());
        assert!(!dest.join("excluded").exists());
        assert_eq!(stats.dirs, 1);

        // Only the included file is planned.
        assert_eq!(planned_files.len(), 1);
        assert_eq!(planned_files[0].rel_path, Path::new("included/a.txt"));
    }

    #[test]
    fn stream_and_plan_only_retains_files() {
        let temp = tempdir().unwrap();
        let dest = &temp.path().canonicalize().unwrap();

        let n_dirs = 5;
        let m_files = 3;
        let mut items: Vec<Item> = Vec::new();
        for i in 0..n_dirs {
            items.push(make_dir_item(&format!("dir{i}"), 0o755));
        }
        for i in 0..m_files {
            items.push(make_file_item(
                &format!("dir0/file{i}.txt"),
                vec![((0xA0 + i) as u8, 100)],
            ));
        }
        // Add a symlink too — should not be in planned_files.
        items.push(make_symlink_item("dir0/link", "file0.txt"));
        let stream = serialize_items(&items);

        let (planned_files, _chunk_targets, stats, _verified_dirs) =
            stream_and_plan(&stream, dest, &mut |_| true, false).unwrap();

        assert_eq!(planned_files.len(), m_files as usize);
        assert_eq!(stats.dirs, n_dirs);
        assert_eq!(stats.symlinks, 1);
    }

    #[test]
    fn stream_and_plan_decode_failure_leaves_partial_dirs() {
        // Extraction is not transactional: a decode error partway through the
        // stream may leave already-created directories on disk.  This test
        // documents that behavior as intentional.
        let temp = tempdir().unwrap();
        let dest = &temp.path().canonicalize().unwrap();

        let mut stream = serialize_items(&[make_dir_item("aaa", 0o755)]);
        // Append garbage bytes to trigger a decode error after the first item.
        stream.extend_from_slice(&[0xFF, 0xFF, 0xFF]);

        let result = stream_and_plan(&stream, dest, &mut |_| true, false);
        assert!(result.is_err());
        // The directory from before the corrupt bytes was still created.
        assert!(dest.join("aaa").is_dir());
    }

    #[test]
    fn stream_and_plan_symlink_before_parent_dir() {
        // Symlink appears before its parent directory in the stream.
        // Single-pass should handle this via ensure_parent_exists_within_root
        // and then apply the correct mode when the directory item arrives.
        let temp = tempdir().unwrap();
        let dest = &temp.path().canonicalize().unwrap();

        let items = vec![
            make_symlink_item("mydir/link", "target"),
            make_dir_item("mydir", 0o750),
        ];
        let stream = serialize_items(&items);

        let (_planned_files, _chunk_targets, stats, verified_dirs) =
            stream_and_plan(&stream, dest, &mut |_| true, false).unwrap();

        // Directory exists and is in verified_dirs.
        assert!(dest.join("mydir").is_dir());
        assert!(verified_dirs.contains(&dest.join("mydir")));

        // Symlink was created and points to the right target.
        let link_path = dest.join("mydir/link");
        assert!(link_path
            .symlink_metadata()
            .unwrap()
            .file_type()
            .is_symlink());
        assert_eq!(
            std::fs::read_link(&link_path).unwrap().to_str().unwrap(),
            "target"
        );

        // Directory has the correct mode from the directory item (not default).
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mode = std::fs::metadata(dest.join("mydir"))
                .unwrap()
                .permissions()
                .mode()
                & 0o777;
            assert_eq!(mode, 0o750);
        }

        assert_eq!(stats.dirs, 1);
        assert_eq!(stats.symlinks, 1);
    }

    #[test]
    fn stream_and_plan_file_before_parent_dir() {
        // File item appears before its parent directory in the stream.
        // The directory should still get its mode applied when the dir item
        // is processed later.
        let temp = tempdir().unwrap();
        let dest = &temp.path().canonicalize().unwrap();

        let items = vec![
            make_file_item("mydir/a.txt", vec![(0xAA, 100)]),
            make_dir_item("mydir", 0o750),
        ];
        let stream = serialize_items(&items);

        let (planned_files, chunk_targets, stats, verified_dirs) =
            stream_and_plan(&stream, dest, &mut |_| true, false).unwrap();

        // Directory exists with correct mode.
        assert!(dest.join("mydir").is_dir());
        assert!(verified_dirs.contains(&dest.join("mydir")));
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mode = std::fs::metadata(dest.join("mydir"))
                .unwrap()
                .permissions()
                .mode()
                & 0o777;
            assert_eq!(mode, 0o750);
        }

        // File is in planned_files.
        assert_eq!(planned_files.len(), 1);
        assert_eq!(planned_files[0].rel_path, Path::new("mydir/a.txt"));
        assert_eq!(chunk_targets.len(), 1);
        assert_eq!(stats.dirs, 1);
    }

    // -----------------------------------------------------------------------
    // validate_and_prepare_dest tests
    // -----------------------------------------------------------------------

    #[test]
    fn validate_dest_rejects_non_empty_directory() {
        let temp = tempdir().unwrap();
        // Create a file inside so it's non-empty.
        std::fs::write(temp.path().join("existing.txt"), b"data").unwrap();
        let err = validate_and_prepare_dest(temp.path().to_str().unwrap())
            .unwrap_err()
            .to_string();
        assert!(
            err.contains("not empty"),
            "expected 'not empty' error, got: {err}"
        );
    }

    #[test]
    fn validate_dest_accepts_empty_directory() {
        let temp = tempdir().unwrap();
        let dest = validate_and_prepare_dest(temp.path().to_str().unwrap()).unwrap();
        assert!(dest.is_dir());
    }

    #[test]
    fn validate_dest_creates_non_existing_directory() {
        let temp = tempdir().unwrap();
        let new_dir = temp.path().join("brand-new");
        assert!(!new_dir.exists());
        let dest = validate_and_prepare_dest(new_dir.to_str().unwrap()).unwrap();
        assert!(dest.is_dir());
    }

    #[test]
    fn validate_dest_creates_nested_non_existing_directory() {
        let temp = tempdir().unwrap();
        let new_dir = temp.path().join("a/b/c");
        assert!(!new_dir.exists());
        let dest = validate_and_prepare_dest(new_dir.to_str().unwrap()).unwrap();
        assert!(dest.is_dir());
    }
}
