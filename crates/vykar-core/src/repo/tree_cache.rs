//! Local, content-addressed cache of tree (item-stream) chunks.
//!
//! Each entry is one file, `<repo cache dir>/tree_chunks/<2 hex>/<64 hex chunk id>`,
//! holding the chunk's stored blob bytes exactly as they sit in a pack: the
//! `ChunkData` envelope, compressed and, for an encrypted repository,
//! AEAD-sealed under the repository key with the chunk ID as AAD. Nothing is
//! re-encrypted and there is no new format: a reader decodes an entry with
//! `decode_chunk_into`, the same trust as a pack read, and treats a failure
//! as a miss (deleting the entry). So a stale, torn or foreign entry costs a
//! download, never a wrong result.
//!
//! The cache only replaces the download. Item pointers are still resolved
//! through the chunk index first, so a chunk missing from the index fails
//! even when it is cached, and the cache is never a source of locations.
//!
//! Every operation is best-effort and never fails its caller: a read error is
//! a miss, the first write error disables writes for the handle (one warn),
//! and `retain` counts and skips what it cannot delete. Writes are not
//! fsynced. Two processes on one host need no coordination for correctness:
//! entries are
//! written to unique `.tc-*` temp files and renamed into place, only
//! `retain` and `remove` delete, and a reader that finds an entry gone
//! downloads the chunk. They can cost each other downloads: `retain` has no
//! grace period, so a drop in one process deletes the entries a backup still
//! running in another has written (its chunks are in no persisted index
//! yet), and a handle whose index is older than the published one drops
//! entries committed since.

use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, SystemTime};

use tracing::{debug, warn};
use vykar_types::chunk_id::ChunkId;

/// Subdirectory of the repo cache dir.
pub(crate) const TREE_CACHE_DIR: &str = "tree_chunks";
/// Prefix of temp files inside a shard dir while an entry is written.
const TEMP_PREFIX: &str = ".tc-";
/// A temp file older than this is a crashed writer's leftover; `retain` deletes it.
pub(crate) const TEMP_MAX_AGE: Duration = Duration::from_secs(60 * 60);

/// Cheap to clone; shared by a repository handle and the streams it opens.
#[derive(Clone)]
pub(crate) struct TreeChunkCache {
    inner: Arc<Inner>,
}

struct Inner {
    root: PathBuf,
    /// Set after the first failed write: this handle stops writing (one warn).
    writes_disabled: AtomicBool,
}

/// What one [`TreeChunkCache::retain`] pass did.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub(crate) struct RetainStats {
    pub kept: u64,
    pub removed: u64,
    pub removed_bytes: u64,
    pub temps_removed: u64,
    /// Entries or dirs that could not be listed or deleted (skipped).
    pub errors: u64,
}

/// Whether `name` is `len` lowercase hex characters.
fn is_lower_hex(name: &str, len: usize) -> bool {
    name.len() == len && name.bytes().all(|b| matches!(b, b'0'..=b'9' | b'a'..=b'f'))
}

/// The chunk ID an entry file name stands for.
fn parse_entry_name(name: &str) -> Option<ChunkId> {
    if !is_lower_hex(name, 64) {
        return None;
    }
    let bytes: [u8; 32] = hex::decode(name).ok()?.try_into().ok()?;
    Some(ChunkId::from_bytes(bytes))
}

impl TreeChunkCache {
    /// `<repo_cache_dir>/tree_chunks`; `None` when there is no cache dir.
    /// Creates nothing on disk.
    pub(crate) fn for_repo(repo_id: &[u8], cache_dir_override: Option<&Path>) -> Option<Self> {
        crate::cache_paths::repo_cache_dir(repo_id, cache_dir_override)
            .map(|dir| Self::at(dir.join(TREE_CACHE_DIR)))
    }

    /// A cache rooted at `root`.
    pub(crate) fn at(root: PathBuf) -> Self {
        Self {
            inner: Arc::new(Inner {
                root,
                writes_disabled: AtomicBool::new(false),
            }),
        }
    }

    pub(crate) fn root(&self) -> &Path {
        &self.inner.root
    }

    fn shard_dir(&self, id: &ChunkId) -> PathBuf {
        self.inner.root.join(id.shard_prefix())
    }

    /// `root/<shard>/<64 hex>`.
    pub(crate) fn entry_path(&self, id: &ChunkId) -> PathBuf {
        self.shard_dir(id).join(id.to_hex())
    }

    /// Whether an entry file exists (one `symlink_metadata` call; regular file only).
    pub(crate) fn contains(&self, id: &ChunkId) -> bool {
        std::fs::symlink_metadata(self.entry_path(id)).is_ok_and(|m| m.file_type().is_file())
    }

    /// The stored bytes of `id`, or `None` (absent or unreadable; errors
    /// other than NotFound are logged at debug). Not verified here: the
    /// caller decodes them with `decode_chunk_into` and calls
    /// [`remove`](Self::remove) on failure.
    pub(crate) fn get(&self, id: &ChunkId) -> Option<Vec<u8>> {
        match std::fs::read(self.entry_path(id)) {
            Ok(bytes) => Some(bytes),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => None,
            Err(e) => {
                debug!("tree-chunk cache: cannot read entry {id}: {e}");
                None
            }
        }
    }

    /// Best-effort write of `stored` as the entry for `id`: skipped when the
    /// entry exists or writes are disabled; otherwise written to a `.tc-*`
    /// temp file in the shard dir (created on demand) and renamed over the
    /// entry name. Never fails the caller.
    pub(crate) fn insert(&self, id: &ChunkId, stored: &[u8]) {
        if self.inner.writes_disabled.load(Ordering::Relaxed) || self.contains(id) {
            return;
        }
        let Err(e) = self.try_insert(id, stored) else {
            return;
        };
        // Another writer renamed the same entry into place first (a rename
        // over an open file fails on Windows): that writer won.
        if self.contains(id) {
            debug!("tree-chunk cache: entry {id} written concurrently: {e}");
            return;
        }
        if self.inner.writes_disabled.swap(true, Ordering::Relaxed) {
            debug!("tree-chunk cache: write of {id} failed: {e}");
        } else {
            warn!(
                "cannot write the local tree-chunk cache in {}: {e}; \
                 this run continues without caching tree chunks",
                self.root().display()
            );
        }
    }

    fn try_insert(&self, id: &ChunkId, stored: &[u8]) -> std::io::Result<()> {
        let shard = self.shard_dir(id);
        let builder = {
            let mut b = tempfile::Builder::new();
            b.prefix(TEMP_PREFIX);
            b
        };
        let mut tmp = match builder.tempfile_in(&shard) {
            Ok(tmp) => tmp,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                std::fs::create_dir_all(&shard)?;
                builder.tempfile_in(&shard)?
            }
            Err(e) => return Err(e),
        };
        tmp.write_all(stored)?;
        tmp.persist(self.entry_path(id)).map_err(|e| e.error)?;
        Ok(())
    }

    /// Best-effort delete of one entry (a corrupt one).
    pub(crate) fn remove(&self, id: &ChunkId) {
        match std::fs::remove_file(self.entry_path(id)) {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => debug!("tree-chunk cache: cannot delete entry {id}: {e}"),
        }
    }

    /// Delete every entry whose chunk ID `keep` rejects, and every `.tc-*`
    /// temp file older than [`TEMP_MAX_AGE`]. An entry in a shard dir that
    /// does not match its name is deleted too (no lookup reaches it). Names
    /// that are neither entries nor temps are left alone. Errors are counted,
    /// logged at debug, and skipped (a Windows sharing violation is retried at
    /// the next call). Logs one debug line with the stats.
    ///
    /// Called today after a prune or delete has persisted its refcount
    /// decrements, and after a backup commit, with "is in the persisted
    /// index" as `keep`. The sweep replaces that call with the mark's
    /// referenced set once prune stops decrementing; a size cap would be a
    /// second predicate here.
    pub(crate) fn retain(&self, keep: impl Fn(&ChunkId) -> bool) -> RetainStats {
        let mut stats = RetainStats::default();
        let root = &self.inner.root;
        let shards = match std::fs::read_dir(root) {
            Ok(shards) => shards,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return stats,
            Err(e) => {
                debug!("tree-chunk cache: cannot list {}: {e}", root.display());
                stats.errors += 1;
                return stats;
            }
        };
        let now = SystemTime::now();
        for shard in shards {
            let Ok(shard) = shard else {
                stats.errors += 1;
                continue;
            };
            let shard_name = shard.file_name();
            let Some(shard_name) = shard_name.to_str().filter(|n| is_lower_hex(n, 2)) else {
                continue;
            };
            if !shard.file_type().is_ok_and(|t| t.is_dir()) {
                continue;
            }
            let entries = match std::fs::read_dir(shard.path()) {
                Ok(entries) => entries,
                Err(e) => {
                    debug!("tree-chunk cache: cannot list shard {shard_name}: {e}");
                    stats.errors += 1;
                    continue;
                }
            };
            for entry in entries {
                let Ok(entry) = entry else {
                    stats.errors += 1;
                    continue;
                };
                let name = entry.file_name();
                let Some(name) = name.to_str() else {
                    continue;
                };
                let Ok(meta) = entry.metadata() else {
                    stats.errors += 1;
                    continue;
                };
                if !meta.is_file() {
                    continue;
                }
                let is_temp = if name.starts_with(TEMP_PREFIX) {
                    let old = meta
                        .modified()
                        .ok()
                        .and_then(|m| now.duration_since(m).ok())
                        .is_some_and(|age| age > TEMP_MAX_AGE);
                    if !old {
                        continue;
                    }
                    true
                } else if let Some(id) = parse_entry_name(name) {
                    let reachable = id.shard_prefix() == shard_name;
                    if reachable && keep(&id) {
                        stats.kept += 1;
                        continue;
                    }
                    false
                } else {
                    continue;
                };
                match std::fs::remove_file(entry.path()) {
                    Ok(()) if is_temp => stats.temps_removed += 1,
                    Ok(()) => {
                        stats.removed += 1;
                        stats.removed_bytes += meta.len();
                    }
                    Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                    Err(e) => {
                        debug!("tree-chunk cache: cannot delete {shard_name}/{name}: {e}");
                        stats.errors += 1;
                    }
                }
            }
        }
        debug!(
            "tree-chunk cache retain: kept {}, removed {} ({} bytes), temps {}, errors {}",
            stats.kept, stats.removed, stats.removed_bytes, stats.temps_removed, stats.errors
        );
        stats
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn id(n: u8) -> ChunkId {
        ChunkId::from_bytes([n; 32])
    }

    fn cache() -> (tempfile::TempDir, TreeChunkCache) {
        let dir = tempfile::tempdir().unwrap();
        let cache = TreeChunkCache::at(dir.path().join(TREE_CACHE_DIR));
        (dir, cache)
    }

    #[test]
    fn insert_then_get_roundtrips() {
        let (_dir, cache) = cache();
        cache.insert(&id(1), b"stored bytes");
        assert!(cache.contains(&id(1)));
        assert_eq!(cache.get(&id(1)).as_deref(), Some(&b"stored bytes"[..]));
        assert_eq!(
            cache.entry_path(&id(1)),
            cache.root().join("01").join(id(1).to_hex())
        );
    }

    #[test]
    fn get_missing_is_none() {
        let (_dir, cache) = cache();
        assert!(cache.get(&id(2)).is_none());
        assert!(!cache.contains(&id(2)));
    }

    #[test]
    fn insert_existing_is_a_no_op() {
        let (_dir, cache) = cache();
        cache.insert(&id(3), b"first");
        let path = cache.entry_path(&id(3));
        let old = SystemTime::now() - Duration::from_secs(600);
        std::fs::File::options()
            .write(true)
            .open(&path)
            .unwrap()
            .set_modified(old)
            .unwrap();
        cache.insert(&id(3), b"second");
        assert_eq!(cache.get(&id(3)).as_deref(), Some(&b"first"[..]));
        assert_eq!(std::fs::metadata(&path).unwrap().modified().unwrap(), old);
    }

    #[test]
    fn remove_deletes_one_entry() {
        let (_dir, cache) = cache();
        cache.insert(&id(4), b"x");
        cache.insert(&id(5), b"y");
        cache.remove(&id(4));
        cache.remove(&id(6));
        assert!(!cache.contains(&id(4)));
        assert!(cache.contains(&id(5)));
    }

    #[test]
    fn retain_removes_rejected_entries_and_keeps_the_rest() {
        let (_dir, cache) = cache();
        for n in 1..=5 {
            cache.insert(&id(n), &vec![n; usize::from(n)]);
        }
        let stats = cache.retain(|c| *c == id(2) || *c == id(4));
        assert_eq!(
            stats,
            RetainStats {
                kept: 2,
                removed: 3,
                removed_bytes: 1 + 3 + 5,
                temps_removed: 0,
                errors: 0,
            }
        );
        for n in 1..=5 {
            assert_eq!(cache.contains(&id(n)), n == 2 || n == 4, "entry {n}");
        }
    }

    #[test]
    fn retain_removes_entries_in_the_wrong_shard() {
        let (_dir, cache) = cache();
        cache.insert(&id(7), b"x");
        let wrong = cache.root().join("08").join(id(7).to_hex());
        std::fs::create_dir_all(wrong.parent().unwrap()).unwrap();
        std::fs::copy(cache.entry_path(&id(7)), &wrong).unwrap();
        let stats = cache.retain(|_| true);
        assert_eq!((stats.kept, stats.removed), (1, 1));
        assert!(!wrong.exists());
        assert!(cache.contains(&id(7)));
    }

    #[test]
    fn retain_removes_old_temps_only() {
        let (_dir, cache) = cache();
        let shard = cache.root().join("aa");
        std::fs::create_dir_all(&shard).unwrap();
        let old = shard.join(".tc-old");
        let fresh = shard.join(".tc-fresh");
        std::fs::write(&old, b"o").unwrap();
        std::fs::write(&fresh, b"f").unwrap();
        std::fs::File::options()
            .write(true)
            .open(&old)
            .unwrap()
            .set_modified(SystemTime::now() - Duration::from_secs(2 * 60 * 60))
            .unwrap();
        let stats = cache.retain(|_| false);
        assert_eq!(stats.temps_removed, 1);
        assert_eq!(stats.removed, 0);
        assert!(!old.exists());
        assert!(fresh.exists());
    }

    #[test]
    fn retain_leaves_foreign_names() {
        let (_dir, cache) = cache();
        let shard = cache.root().join("ab");
        std::fs::create_dir_all(&shard).unwrap();
        let names = ["README".to_string(), "ab".repeat(31) + "a", "AB".repeat(32)];
        for name in &names {
            std::fs::write(shard.join(name), b"keep me").unwrap();
        }
        std::fs::write(cache.root().join("README"), b"keep me").unwrap();
        let stats = cache.retain(|_| false);
        assert_eq!(stats, RetainStats::default());
        for name in &names {
            assert!(shard.join(name).exists(), "{name}");
        }
        assert!(cache.root().join("README").exists());
    }

    #[test]
    fn retain_on_missing_root_is_empty() {
        let (_dir, cache) = cache();
        assert_eq!(cache.retain(|_| false), RetainStats::default());
        assert!(!cache.root().exists());
    }

    #[cfg(unix)]
    #[test]
    fn insert_failure_disables_writes() {
        use std::os::unix::fs::PermissionsExt;
        let (_dir, cache) = cache();
        let locked = cache.root().join(id(1).shard_prefix());
        std::fs::create_dir_all(&locked).unwrap();
        std::fs::set_permissions(&locked, std::fs::Permissions::from_mode(0o500)).unwrap();
        // Root ignores the mode: nothing to test then.
        if std::fs::write(locked.join("probe"), b"").is_ok() {
            return;
        }
        cache.insert(&id(1), b"x");
        assert!(!cache.contains(&id(1)));
        cache.insert(&id(2), b"y");
        assert!(!cache.contains(&id(2)));
        assert!(!cache.root().join(id(2).shard_prefix()).exists());
        std::fs::set_permissions(&locked, std::fs::Permissions::from_mode(0o700)).unwrap();
    }
}
