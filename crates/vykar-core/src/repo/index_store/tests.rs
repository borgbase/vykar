use std::sync::{Arc, Mutex};

use super::*;
use crate::index::dedup_cache::{build_full_index_cache_to_path, chunk_index_from_full_cache};
use crate::index::head::empty_state_hash;
use crate::index::segment::diff_to_temp;
use crate::index::{ChunkIndex, ChunkIndexEntry};
use crate::repo::{EncryptionMode, RepoFormat};
use crate::tests::helpers::FailGet;
use crate::testutil::MemoryBackend;
use vykar_crypto::PlaintextEngine;
use vykar_crypto::key::MasterKey;
use vykar_storage::{InnerBackend, delegate_storage_backend};
use vykar_types::chunk_id::{ChunkHasher, ChunkId};
use vykar_types::hash::HashAlgorithm;
use vykar_types::pack_id::PackId;

/// Logs every `get` key in call order.
struct GetLog {
    inner: Arc<dyn StorageBackend>,
    gets: Mutex<Vec<String>>,
}

impl InnerBackend for GetLog {
    fn inner_backend(&self) -> &dyn StorageBackend {
        &*self.inner
    }
}

delegate_storage_backend! {
    for GetLog;
    except [get, get_if_changed];

    fn get(&self, key: &str) -> Result<Option<Vec<u8>>> {
        self.gets.lock().unwrap().push(key.to_string());
        self.inner.get(key)
    }

    fn get_if_changed(&self, key: &str, _known: &[u8; 32]) -> Result<vykar_storage::ConditionalGet> {
        vykar_storage::get_unconditionally(self, key)
    }
}

impl GetLog {
    fn new(inner: Arc<dyn StorageBackend>) -> Self {
        Self {
            inner,
            gets: Mutex::new(Vec::new()),
        }
    }

    fn take(&self) -> Vec<String> {
        std::mem::take(&mut *self.gets.lock().unwrap())
    }
}

fn plaintext() -> Arc<dyn CryptoEngine> {
    Arc::new(PlaintextEngine::new(ChunkHasher::new(
        HashAlgorithm::Blake3,
        [7; 32],
    )))
}

fn encrypted() -> Arc<dyn CryptoEngine> {
    let key = MasterKey::generate().unwrap();
    crate::repo::key::build_engine(&EncryptionMode::Aes256Gcm, RepoFormat::V5, &key).unwrap()
}

fn id(n: u32) -> ChunkId {
    let mut b = [0u8; 32];
    b[..4].copy_from_slice(&n.wrapping_mul(2_654_435_761).to_be_bytes());
    ChunkId::from_bytes(b)
}

pub(crate) fn index_with(range: std::ops::Range<u32>, extra_refs: u32) -> ChunkIndex {
    let mut index = ChunkIndex::new();
    for n in range {
        index.add(
            id(n),
            50 + n,
            PackId::from_bytes([(n % 5) as u8 + 1; 32]),
            u64::from(n),
        );
        if extra_refs > 0 {
            index.increment_refcount_by(&id(n), extra_refs);
        }
    }
    index
}

pub(crate) fn entries(index: &ChunkIndex) -> Vec<(ChunkId, ChunkIndexEntry)> {
    let mut v: Vec<_> = index.iter().map(|(k, e)| (*k, *e)).collect();
    v.sort_by(|a, b| a.0.as_bytes().cmp(b.0.as_bytes()));
    v
}

/// A minimal writer for the segmented layout, the way the write path will
/// use it: segments by diffing against its own full cache, and folds that
/// delete the previous fold's retired objects before writing the new base.
pub(crate) struct Writer {
    storage: Arc<dyn StorageBackend>,
    crypto: Arc<dyn CryptoEngine>,
    dir: tempfile::TempDir,
    pub(crate) head: IndexHead,
}

impl Writer {
    pub(crate) fn new(storage: Arc<dyn StorageBackend>, crypto: Arc<dyn CryptoEngine>) -> Self {
        let dir = tempfile::tempdir().unwrap();
        build_full_index_cache_to_path(&ChunkIndex::new(), 1, &dir.path().join("cache")).unwrap();
        let w = Self {
            storage,
            crypto,
            dir,
            head: IndexHead::empty(),
        };
        w.put_head();
        w
    }

    fn cache_path(&self) -> PathBuf {
        self.dir.path().join("cache")
    }

    fn put_head(&self) {
        self.storage
            .put(
                "index",
                &pack_head(&self.head, self.crypto.as_ref()).unwrap(),
            )
            .unwrap();
    }

    /// Write `next` as a segment and publish the head.
    pub(crate) fn commit(&mut self, next: &ChunkIndex) {
        let prev = MmapFullIndexCache::open_verified(&self.cache_path()).unwrap();
        let (diff, tmp) = diff_to_temp(&prev, next, &self.cache_path()).unwrap();
        drop(prev);
        tmp.persist(self.cache_path()).unwrap();
        let obj = pack_segment(&diff.segment, self.crypto.as_ref()).unwrap();
        self.storage.put(&obj.key(), &obj.bytes).unwrap();
        self.head.segments.push(SegmentRef {
            name: obj.name,
            size: obj.size(),
            state_hash_after: diff.state_hash_after,
            entries_after: diff.entries_after,
        });
        self.head.state_hash = diff.state_hash_after;
        self.head.entries = diff.entries_after;
        self.put_head();
    }

    /// Fold the current state into a new base.
    pub(crate) fn fold(&mut self) {
        for name in self.head.retired.drain(..) {
            self.storage.delete(&object_key(&name)).unwrap();
        }
        let cache = MmapFullIndexCache::open_verified(&self.cache_path()).unwrap();
        let obj = pack_base(&cache, self.crypto.as_ref()).unwrap();
        self.storage.put(&obj.key(), &obj.bytes).unwrap();
        self.head.retired = self.head.live_objects().collect();
        self.head.base = Some(BaseRef {
            name: obj.name,
            size: obj.size(),
            state_hash: cache.state_hash(),
            entries: u64::from(cache.entry_count()),
        });
        self.head.segments.clear();
        self.put_head();
    }

    /// Fold into a base of `next`, a state other than the current one, as a
    /// write at the hard limit does.
    pub(crate) fn fold_to(&mut self, next: &ChunkIndex) {
        for name in self.head.retired.drain(..) {
            self.storage.delete(&object_key(&name)).unwrap();
        }
        build_full_index_cache_to_path(next, 1, &self.cache_path()).unwrap();
        let cache = MmapFullIndexCache::open_verified(&self.cache_path()).unwrap();
        let obj = pack_base(&cache, self.crypto.as_ref()).unwrap();
        self.storage.put(&obj.key(), &obj.bytes).unwrap();
        self.head.retired = self.head.live_objects().collect();
        self.head.base = Some(BaseRef {
            name: obj.name,
            size: obj.size(),
            state_hash: cache.state_hash(),
            entries: u64::from(cache.entry_count()),
        });
        self.head.segments.clear();
        self.head.state_hash = cache.state_hash();
        self.head.entries = u64::from(cache.entry_count());
        self.put_head();
    }

    /// PUT the segment from the current state to `next` and leave the head
    /// and the cache alone: the debris of a failed head PUT.
    pub(crate) fn put_unreferenced_segment(&self, next: &ChunkIndex) -> ObjectName {
        let prev = MmapFullIndexCache::open_verified(&self.cache_path()).unwrap();
        let (diff, tmp) = diff_to_temp(&prev, next, &self.cache_path()).unwrap();
        drop(tmp);
        let obj = pack_segment(&diff.segment, self.crypto.as_ref()).unwrap();
        self.storage.put(&obj.key(), &obj.bytes).unwrap();
        obj.name
    }

    /// PUT a base of `index` that no head names.
    pub(crate) fn put_unreferenced_base(&self, index: &ChunkIndex) -> ObjectName {
        let path = self.dir.path().join("unreferenced");
        build_full_index_cache_to_path(index, 1, &path).unwrap();
        let cache = MmapFullIndexCache::open_verified(&path).unwrap();
        let obj = pack_base(&cache, self.crypto.as_ref()).unwrap();
        self.storage.put(&obj.key(), &obj.bytes).unwrap();
        obj.name
    }
}

fn sync(
    storage: &dyn StorageBackend,
    crypto: &dyn CryptoEngine,
    head: IndexHead,
    path: Option<&Path>,
    scratch: Option<TempPath>,
    use_local: bool,
) -> Result<SyncedIndex> {
    sync_full_cache(
        storage,
        crypto,
        head,
        CacheLocation {
            path,
            scratch,
            use_local,
        },
        2,
    )
}

fn index_d_gets(gets: &[String]) -> usize {
    gets.iter()
        .filter(|k| k.starts_with(INDEX_OBJECT_DIR))
        .count()
}

#[test]
fn head_round_trips_through_storage_encoding() {
    for crypto in [plaintext(), encrypted()] {
        let head = IndexHead::empty();
        let packed = pack_head(&head, crypto.as_ref()).unwrap();
        assert_eq!(packed[0], ObjectType::IndexHead as u8);
        assert_eq!(decode_head(&packed, crypto.as_ref()).unwrap(), head);
    }
}

#[test]
fn legacy_index_blob_is_named_in_the_error() {
    let crypto = plaintext();
    let legacy = pack_object_with_context(
        ObjectType::ChunkIndex,
        INDEX_OBJECT_CONTEXT,
        b"whatever",
        crypto.as_ref(),
    )
    .unwrap();
    let err = decode_head(&legacy, crypto.as_ref())
        .unwrap_err()
        .to_string();
    assert!(err.contains("monolithic index"), "{err}");
}

/// Cold download, then catch-up from a local cache at an earlier state,
/// then nothing to do.
#[test]
fn sync_downloads_catches_up_and_reuses() {
    for crypto in [plaintext(), encrypted()] {
        let mem: Arc<dyn StorageBackend> = Arc::new(MemoryBackend::new());
        let log = GetLog::new(Arc::clone(&mem));
        let mut w = Writer::new(Arc::clone(&mem), Arc::clone(&crypto));
        let s1 = index_with(0..300, 0);
        let s2 = index_with(100..400, 1);
        let s3 = index_with(150..420, 2);
        w.commit(&s1);
        w.fold();
        w.commit(&s2);

        let cache_dir = tempfile::tempdir().unwrap();
        let path = cache_dir.path().join("sub").join("full_index_cache");

        let synced = sync(
            &log,
            crypto.as_ref(),
            w.head.clone(),
            Some(&path),
            None,
            true,
        )
        .unwrap();
        assert_eq!(synced.source, SyncSource::Downloaded { segments: 1 });
        assert!(synced.scratch.is_none(), "persisted at the cache path");
        assert_eq!(
            entries(&chunk_index_from_full_cache(&synced.cache)),
            entries(&s2)
        );
        assert_eq!(index_d_gets(&log.take()), 2, "base and one segment");
        drop(synced);

        w.commit(&s3);
        let synced = sync(
            &log,
            crypto.as_ref(),
            w.head.clone(),
            Some(&path),
            None,
            true,
        )
        .unwrap();
        assert_eq!(synced.source, SyncSource::CaughtUp { segments: 1 });
        assert_eq!(
            entries(&chunk_index_from_full_cache(&synced.cache)),
            entries(&s3)
        );
        assert_eq!(index_d_gets(&log.take()), 1, "only the new segment");
        drop(synced);

        let synced = sync(
            &log,
            crypto.as_ref(),
            w.head.clone(),
            Some(&path),
            None,
            true,
        )
        .unwrap();
        assert_eq!(synced.source, SyncSource::UpToDate);
        assert!(log.take().is_empty());
    }
}

/// A local cache at the base state applies every segment in one pass.
#[test]
fn sync_from_base_state_applies_the_whole_chain() {
    let crypto = plaintext();
    let mem: Arc<dyn StorageBackend> = Arc::new(MemoryBackend::new());
    let mut w = Writer::new(Arc::clone(&mem), Arc::clone(&crypto));
    w.commit(&index_with(0..50, 0));
    w.fold();
    let cache_dir = tempfile::tempdir().unwrap();
    let path = cache_dir.path().join("full_index_cache");
    std::fs::copy(w.cache_path(), &path).unwrap();
    let last = index_with(20..90, 3);
    w.commit(&index_with(10..60, 1));
    w.commit(&index_with(15..80, 2));
    w.commit(&last);

    let synced = sync(
        mem.as_ref(),
        crypto.as_ref(),
        w.head.clone(),
        Some(&path),
        None,
        true,
    )
    .unwrap();
    assert_eq!(synced.source, SyncSource::CaughtUp { segments: 3 });
    assert_eq!(
        entries(&chunk_index_from_full_cache(&synced.cache)),
        entries(&last)
    );
}

/// A local cache at a state outside the chain is not used.
#[test]
fn sync_ignores_an_unrelated_local_cache() {
    let crypto = plaintext();
    let mem: Arc<dyn StorageBackend> = Arc::new(MemoryBackend::new());
    let mut w = Writer::new(Arc::clone(&mem), Arc::clone(&crypto));
    let s1 = index_with(0..30, 0);
    w.commit(&s1);
    let cache_dir = tempfile::tempdir().unwrap();
    let path = cache_dir.path().join("full_index_cache");
    build_full_index_cache_to_path(&index_with(500..510, 0), 9, &path).unwrap();

    let synced = sync(
        mem.as_ref(),
        crypto.as_ref(),
        w.head.clone(),
        Some(&path),
        None,
        true,
    )
    .unwrap();
    assert_eq!(synced.source, SyncSource::Downloaded { segments: 1 });
    assert_eq!(
        entries(&chunk_index_from_full_cache(&synced.cache)),
        entries(&s1)
    );
}

/// An empty base (`base: None`) with no segments is the empty index.
#[test]
fn sync_of_the_empty_head() {
    let crypto = plaintext();
    let mem: Arc<dyn StorageBackend> = Arc::new(MemoryBackend::new());
    let synced = sync(
        mem.as_ref(),
        crypto.as_ref(),
        IndexHead::empty(),
        None,
        None,
        true,
    )
    .unwrap();
    assert_eq!(synced.cache.entry_count(), 0);
    assert_eq!(synced.cache.state_hash(), empty_state_hash());
    assert!(synced.scratch.is_some(), "no cache path: a temp file");
}

/// Without a cache path the result is a temp file, reused by the next sync
/// of the same run.
#[test]
fn sync_without_cache_dir_keeps_a_scratch_file() {
    let crypto = plaintext();
    let mem: Arc<dyn StorageBackend> = Arc::new(MemoryBackend::new());
    let log = GetLog::new(Arc::clone(&mem));
    let mut w = Writer::new(Arc::clone(&mem), Arc::clone(&crypto));
    w.commit(&index_with(0..40, 0));
    let synced = sync(&log, crypto.as_ref(), w.head.clone(), None, None, true).unwrap();
    let scratch = synced.scratch.expect("temp cache");
    assert!(scratch.exists());
    log.take();

    let s2 = index_with(5..45, 1);
    w.commit(&s2);
    let synced = sync(
        &log,
        crypto.as_ref(),
        w.head.clone(),
        None,
        Some(scratch),
        true,
    )
    .unwrap();
    assert_eq!(synced.source, SyncSource::CaughtUp { segments: 1 });
    assert_eq!(
        entries(&chunk_index_from_full_cache(&synced.cache)),
        entries(&s2)
    );
    assert_eq!(index_d_gets(&log.take()), 1);
}

/// `use_local: false` downloads everything and leaves the local cache alone.
#[test]
fn sync_without_local_caches_verifies_the_whole_chain() {
    let crypto = plaintext();
    let mem: Arc<dyn StorageBackend> = Arc::new(MemoryBackend::new());
    let log = GetLog::new(Arc::clone(&mem));
    let mut w = Writer::new(Arc::clone(&mem), Arc::clone(&crypto));
    w.commit(&index_with(0..40, 0));
    w.fold();
    w.commit(&index_with(0..60, 0));
    let path = w.cache_path();
    let before = std::fs::read(&path).unwrap();

    let synced = sync(
        &log,
        crypto.as_ref(),
        w.head.clone(),
        Some(&path),
        None,
        false,
    )
    .unwrap();
    assert_eq!(synced.source, SyncSource::Downloaded { segments: 1 });
    assert!(synced.scratch.is_some());
    assert_eq!(index_d_gets(&log.take()), 2);
    assert_eq!(std::fs::read(&path).unwrap(), before);
}

/// A modified object, or a valid object under another object's name, fails
/// the name check before decryption.
#[test]
fn tampered_or_swapped_objects_fail_the_name_check() {
    for crypto in [plaintext(), encrypted()] {
        let mem: Arc<dyn StorageBackend> = Arc::new(MemoryBackend::new());
        let mut w = Writer::new(Arc::clone(&mem), Arc::clone(&crypto));
        w.commit(&index_with(0..40, 0));
        w.commit(&index_with(0..50, 1));
        let [a, b] = [&w.head.segments[0], &w.head.segments[1]].map(|s| object_key(&s.name));

        let original = mem.get(&b).unwrap().unwrap();
        let mut flipped = original.clone();
        *flipped.last_mut().unwrap() ^= 1;
        mem.put(&b, &flipped).unwrap();
        let err = sync(
            mem.as_ref(),
            crypto.as_ref(),
            w.head.clone(),
            None,
            None,
            true,
        )
        .err()
        .expect("tampered segment")
        .to_string();
        assert!(err.contains("does not match its name"), "{err}");

        mem.put(&b, &mem.get(&a).unwrap().unwrap()).unwrap();
        let err = sync(
            mem.as_ref(),
            crypto.as_ref(),
            w.head.clone(),
            None,
            None,
            true,
        )
        .err()
        .expect("swapped segment")
        .to_string();
        assert!(err.contains("does not match its name"), "{err}");
    }
}

/// A head whose recorded state disagrees with what its objects produce is
/// rejected even when every object is intact.
#[test]
fn wrong_recorded_state_is_rejected() {
    let crypto = plaintext();
    let mem: Arc<dyn StorageBackend> = Arc::new(MemoryBackend::new());
    let mut w = Writer::new(Arc::clone(&mem), Arc::clone(&crypto));
    w.commit(&index_with(0..40, 0));
    w.fold();
    let mut head = w.head.clone();
    head.base.as_mut().unwrap().state_hash = [9; 32];
    head.state_hash = [9; 32];
    assert!(sync(mem.as_ref(), crypto.as_ref(), head, None, None, true).is_err());
}

/// A reader holding a pre-fold head completes after one fold (its objects
/// are retired, not deleted). Once the next fold deleted them it gets a 404
/// and recovers by re-reading the head.
#[test]
fn reader_on_a_retired_head_survives_one_fold_and_recovers_after_two() {
    let crypto = plaintext();
    let mem: Arc<dyn StorageBackend> = Arc::new(MemoryBackend::new());
    let log = GetLog::new(Arc::clone(&mem));
    let mut w = Writer::new(Arc::clone(&mem), Arc::clone(&crypto));
    w.commit(&index_with(0..40, 0));
    w.fold();
    w.commit(&index_with(0..50, 0));
    let old_head = w.head.clone();

    w.commit(&index_with(0..60, 1));
    w.fold();
    let synced = sync(&log, crypto.as_ref(), old_head.clone(), None, None, true).unwrap();
    assert_eq!(synced.head, old_head, "finished on the retired head");
    assert!(!log.take().contains(&"index".to_string()));

    let latest = index_with(0..70, 2);
    w.commit(&latest);
    w.fold();
    assert!(
        mem.get(&object_key(&old_head.segments[0].name))
            .unwrap()
            .is_none()
    );
    let old_base = object_key(&old_head.base.as_ref().unwrap().name);
    let synced = sync(&log, crypto.as_ref(), old_head, None, None, true).unwrap();
    assert_eq!(synced.head, w.head, "re-read the head");
    assert_eq!(
        entries(&chunk_index_from_full_cache(&synced.cache)),
        entries(&latest)
    );
    let gets = log.take();
    assert_eq!(gets.iter().filter(|k| *k == "index").count(), 1);
    assert!(
        !gets.contains(&old_base),
        "the missing segment is found before any base download: {gets:?}"
    );
}

/// A base and one segment on top of it.
fn base_and_segment() -> (Arc<dyn StorageBackend>, Arc<dyn CryptoEngine>, Writer) {
    let crypto = plaintext();
    let mem: Arc<dyn StorageBackend> = Arc::new(MemoryBackend::new());
    let mut w = Writer::new(Arc::clone(&mem), Arc::clone(&crypto));
    w.commit(&index_with(0..40, 0));
    w.fold();
    w.commit(&index_with(0..50, 1));
    (mem, crypto, w)
}

fn head_gets(gets: &[String]) -> usize {
    gets.iter().filter(|k| *k == "index").count()
}

/// A segment the unchanged head still names is lost: one head re-read, no
/// retries, and no base download (segments are fetched first).
#[test]
fn missing_segment_under_an_unchanged_head_fails_at_once() {
    let (mem, crypto, w) = base_and_segment();
    let log = GetLog::new(Arc::clone(&mem));
    let seg = object_key(&w.head.segments[0].name);
    mem.delete(&seg).unwrap();
    let err = sync(&log, crypto.as_ref(), w.head.clone(), None, None, true)
        .err()
        .expect("lost segment");
    assert!(
        matches!(&err, VykarError::IndexObjectMissing { kind: "segment", key } if *key == seg),
        "{err:?}"
    );
    let msg = err.to_string();
    assert!(msg.contains("check --repair"), "{msg}");
    assert!(!msg.contains("retry later"), "{msg}");
    let gets = log.take();
    assert_eq!(head_gets(&gets), 1, "{gets:?}");
    assert!(
        !gets.contains(&object_key(&w.head.base.as_ref().unwrap().name)),
        "{gets:?}"
    );
    assert_eq!(index_d_gets(&gets), 1, "{gets:?}");
}

#[test]
fn missing_base_under_an_unchanged_head_fails_at_once() {
    let (mem, crypto, w) = base_and_segment();
    let log = GetLog::new(Arc::clone(&mem));
    let base = object_key(&w.head.base.as_ref().unwrap().name);
    mem.delete(&base).unwrap();
    let err = sync(&log, crypto.as_ref(), w.head.clone(), None, None, true)
        .err()
        .expect("lost base");
    assert!(
        matches!(&err, VykarError::IndexObjectMissing { kind: "base", key } if *key == base),
        "{err:?}"
    );
    let gets = log.take();
    assert_eq!(head_gets(&gets), 1, "{gets:?}");
    assert_eq!(
        index_d_gets(&gets),
        2,
        "the segment, then the base: {gets:?}"
    );
}

/// Another client appended to the chain after the loss: the head changed,
/// but it still names the missing segment, so it is lost all the same.
#[test]
fn missing_object_still_named_by_a_moved_head_fails_at_once() {
    let crypto = plaintext();
    let mem: Arc<dyn StorageBackend> = Arc::new(MemoryBackend::new());
    let log = GetLog::new(Arc::clone(&mem));
    let mut w = Writer::new(Arc::clone(&mem), Arc::clone(&crypto));
    w.commit(&index_with(0..40, 0));
    w.commit(&index_with(0..50, 1));
    let old = w.head.clone();
    let seg = object_key(&old.segments[0].name);
    mem.delete(&seg).unwrap();
    w.commit(&index_with(0..60, 2));
    assert_ne!(w.head, old);

    let err = sync(&log, crypto.as_ref(), old, None, None, true)
        .err()
        .expect("lost segment");
    assert!(
        matches!(&err, VykarError::IndexObjectMissing { kind: "segment", key } if *key == seg),
        "{err:?}"
    );
    assert_eq!(head_gets(&log.take()), 1);
}

/// An object the re-read head only retires may be deleted by a fold in
/// progress, so the reader retries on that head instead of reporting loss.
#[test]
fn missing_retired_object_is_retried_not_reported_lost() {
    let crypto = plaintext();
    let mem: Arc<dyn StorageBackend> = Arc::new(MemoryBackend::new());
    let log = GetLog::new(Arc::clone(&mem));
    let mut w = Writer::new(Arc::clone(&mem), Arc::clone(&crypto));
    w.commit(&index_with(0..40, 0));
    w.commit(&index_with(0..50, 1));
    let old = w.head.clone();
    w.fold();
    assert!(w.head.retired.contains(&old.segments[0].name));
    mem.delete(&object_key(&old.segments[0].name)).unwrap();

    let synced = sync(&log, crypto.as_ref(), old, None, None, true).unwrap();
    assert_eq!(synced.head, w.head);
    assert_eq!(head_gets(&log.take()), 1);
}

/// Serves a queue of heads at `index`, one per GET, and has no `index.d/`
/// objects at all: every re-read head moved and none names what is missing.
struct MovingHead {
    inner: Arc<dyn StorageBackend>,
    heads: Mutex<std::collections::VecDeque<Vec<u8>>>,
    gets: Mutex<Vec<String>>,
}

impl InnerBackend for MovingHead {
    fn inner_backend(&self) -> &dyn StorageBackend {
        &*self.inner
    }
}

delegate_storage_backend! {
    for MovingHead;
    except [get, get_if_changed];

    fn get(&self, key: &str) -> Result<Option<Vec<u8>>> {
        self.gets.lock().unwrap().push(key.to_string());
        if key == "index" {
            return Ok(self.heads.lock().unwrap().pop_front());
        }
        if key.starts_with(INDEX_OBJECT_DIR) {
            return Ok(None);
        }
        self.inner.get(key)
    }

    fn get_if_changed(&self, key: &str, _known: &[u8; 32]) -> Result<vykar_storage::ConditionalGet> {
        vykar_storage::get_unconditionally(self, key)
    }
}

/// The error of a reader whose objects vanish on every attempt while the
/// head keeps moving, and the GETs it made.
fn moving_head_error() -> (VykarError, Vec<String>) {
    let crypto = plaintext();
    let mem: Arc<dyn StorageBackend> = Arc::new(MemoryBackend::new());
    let mut w = Writer::new(Arc::clone(&mem), Arc::clone(&crypto));
    let mut heads = Vec::new();
    for i in 0..5u32 {
        w.commit(&index_with(0..(40 + 10 * i), i));
        w.fold();
        heads.push(w.head.clone());
    }
    let storage = MovingHead {
        inner: Arc::clone(&mem),
        heads: Mutex::new(
            heads[1..]
                .iter()
                .map(|h| pack_head(h, crypto.as_ref()).unwrap())
                .collect(),
        ),
        gets: Mutex::new(Vec::new()),
    };
    let err = sync(
        &storage,
        crypto.as_ref(),
        heads[0].clone(),
        None,
        None,
        true,
    )
    .err()
    .expect("gives up");
    let gets = storage.gets.into_inner().unwrap();
    (err, gets)
}

#[test]
fn vanishing_objects_under_a_moving_head_give_up_with_retry_later() {
    let (err, gets) = moving_head_error();
    assert!(matches!(err, VykarError::IndexHeadUnstable), "{err:?}");
    let msg = err.to_string();
    assert!(
        msg.contains("retry later") && !msg.contains("check --repair"),
        "{msg}"
    );
    assert_eq!(head_gets(&gets), MAX_HEAD_RETRIES + 1, "{gets:?}");
    assert_eq!(
        index_d_gets(&gets),
        MAX_HEAD_RETRIES + 1,
        "one base 404 per attempt: {gets:?}"
    );
}

#[test]
fn fetch_all_preserves_order_and_reports_absence() {
    let mem = MemoryBackend::new();
    let keys: Vec<String> = (0..9).map(|i| format!("k{i}")).collect();
    for (i, k) in keys.iter().enumerate() {
        mem.put(k, &[i as u8]).unwrap();
    }
    for width in [1, 3, 16] {
        assert_eq!(
            fetch_all(&mem, &keys, width).unwrap(),
            Fetched::All((0..9u8).map(|i| vec![i]).collect())
        );
    }
    mem.delete("k4").unwrap();
    for width in [1, 3] {
        assert_eq!(fetch_all(&mem, &keys, width).unwrap(), Fetched::Absent(4));
    }
    mem.delete("k2").unwrap();
    for width in [1, 3, 16] {
        assert_eq!(
            fetch_all(&mem, &keys, width).unwrap(),
            Fetched::Absent(2),
            "the first absent key wins"
        );
    }
    assert_eq!(fetch_all(&mem, &[], 4).unwrap(), Fetched::All(Vec::new()));
}

/// A base at `0..40`, then segments to `0..50` and `0..60` (extra refs).
fn salvage_fixture() -> (Arc<dyn StorageBackend>, Arc<dyn CryptoEngine>, Writer) {
    let crypto = plaintext();
    let mem: Arc<dyn StorageBackend> = Arc::new(MemoryBackend::new());
    let mut w = Writer::new(Arc::clone(&mem), Arc::clone(&crypto));
    w.commit(&index_with(0..40, 0));
    w.fold();
    w.commit(&index_with(0..50, 0));
    w.commit(&index_with(0..60, 1));
    (mem, crypto, w)
}

/// `base_read`, `applied` and the entries of a salvaged state.
type SalvagedState = (bool, usize, Vec<(ChunkId, ChunkIndexEntry)>);

/// `salvage_chain`'s flags and the entries of the state it read.
fn salvage(
    storage: &dyn StorageBackend,
    crypto: &dyn CryptoEngine,
    head: &IndexHead,
) -> Result<SalvagedState> {
    let dir = tempfile::tempdir().unwrap();
    let s = salvage_chain(storage, crypto, head, &dir.path().join("x"))?;
    let cache = MmapFullIndexCache::open_verified(&s.cache).unwrap();
    Ok((
        s.base_read,
        s.applied,
        entries(&chunk_index_from_full_cache(&cache)),
    ))
}

#[test]
fn salvage_reads_an_intact_chain() {
    let (mem, crypto, w) = salvage_fixture();
    let (base_read, applied, got) = salvage(mem.as_ref(), crypto.as_ref(), &w.head).unwrap();
    assert!(base_read);
    assert_eq!(applied, 2);
    assert_eq!(got, entries(&index_with(0..60, 1)));
}

#[test]
fn salvage_stops_at_a_missing_or_damaged_object() {
    let (mem, crypto, w) = salvage_fixture();
    mem.delete(&object_key(&w.head.segments[0].name)).unwrap();
    let (base_read, applied, got) = salvage(mem.as_ref(), crypto.as_ref(), &w.head).unwrap();
    assert!(base_read);
    assert_eq!(applied, 0);
    assert_eq!(got, entries(&index_with(0..40, 0)));

    let (mem, crypto, w) = salvage_fixture();
    let key = object_key(&w.head.segments[1].name);
    let mut bytes = mem.get(&key).unwrap().unwrap();
    *bytes.last_mut().unwrap() ^= 1;
    mem.put(&key, &bytes).unwrap();
    let (base_read, applied, got) = salvage(mem.as_ref(), crypto.as_ref(), &w.head).unwrap();
    assert!(base_read);
    assert_eq!(applied, 1);
    assert_eq!(got, entries(&index_with(0..50, 0)));

    let (mem, crypto, w) = salvage_fixture();
    mem.delete(&object_key(&w.head.base.as_ref().unwrap().name))
        .unwrap();
    let (base_read, applied, got) = salvage(mem.as_ref(), crypto.as_ref(), &w.head).unwrap();
    assert!(!base_read);
    assert_eq!(applied, 0);
    assert!(got.is_empty());
}

#[test]
fn salvage_propagates_storage_errors() {
    let (mem, crypto, w) = salvage_fixture();
    let base = object_key(&w.head.base.as_ref().unwrap().name);
    let seg = object_key(&w.head.segments[1].name);
    for key in [base, seg] {
        let failing = key.clone();
        let storage = FailGet::new(Arc::clone(&mem), move |k| k == failing, usize::MAX);
        let err = salvage(&storage, crypto.as_ref(), &w.head)
            .expect_err("storage error")
            .to_string();
        assert!(err.contains("injected") && err.contains(&key), "{err}");
    }
}

#[test]
fn salvage_refuses_a_segment_from_a_newer_vykar() {
    let crypto = plaintext();
    let mem: Arc<dyn StorageBackend> = Arc::new(MemoryBackend::new());
    let mut w = Writer::new(Arc::clone(&mem), Arc::clone(&crypto));
    w.commit(&index_with(0..40, 0));
    let seg_ref = &w.head.segments[0];
    let bytes = mem.get(&object_key(&seg_ref.name)).unwrap().unwrap();
    let mut body = decode_segment(seg_ref, &bytes, crypto.as_ref())
        .unwrap()
        .encode();
    body[0] = 99;
    let packed = pack_object_streaming_with_context(
        ObjectType::IndexSegment,
        SEGMENT_CONTEXT,
        body.len() + 64,
        crypto.as_ref(),
        |buf| {
            compress::compress_stream_zstd(buf, 3, |w| {
                std::io::Write::write_all(w, &body)?;
                Ok(())
            })
        },
    )
    .unwrap();
    let obj = PackedObject::new(packed);
    mem.put(&obj.key(), &obj.bytes).unwrap();
    let mut head = w.head.clone();
    head.segments[0].name = obj.name;
    head.segments[0].size = obj.size();
    let dir = tempfile::tempdir().unwrap();
    let r = salvage_chain(mem.as_ref(), crypto.as_ref(), &head, &dir.path().join("x"));
    assert!(
        matches!(r, Err(VykarError::UnsupportedVersion(99))),
        "{:?}",
        r.err()
    );
}

#[test]
fn object_damage_classification() {
    for e in [
        VykarError::InvalidFormat("x".into()),
        VykarError::DecryptionFailed,
        VykarError::Decompression("x".into()),
        VykarError::UnknownObjectType(9),
        VykarError::UnknownCompressionTag(9),
        VykarError::IndexObjectMissing {
            kind: "segment",
            key: "index.d/x".into(),
        },
    ] {
        assert!(is_object_damage(&e), "{e:?}");
    }
    for e in [
        moving_head_error().0,
        VykarError::IndexHeadUnstable,
        VykarError::Other("x".into()),
        VykarError::Io(std::io::Error::other("x")),
        VykarError::UnsupportedVersion(9),
        VykarError::Interrupted,
    ] {
        assert!(!is_object_damage(&e), "{e:?}");
    }
}

/// Records `delete` and `batch_delete_keys` calls. A batch deletes every
/// key, or with `fail` only the first one and then errors (a partial batch).
struct BatchDeletes {
    inner: Arc<dyn StorageBackend>,
    fail: bool,
    batches: Mutex<Vec<(Vec<String>, vykar_storage::BatchDeleteScope)>>,
    deletes: Mutex<Vec<String>>,
}

impl BatchDeletes {
    fn new(inner: Arc<dyn StorageBackend>, fail: bool) -> Self {
        Self {
            inner,
            fail,
            batches: Mutex::new(Vec::new()),
            deletes: Mutex::new(Vec::new()),
        }
    }
}

impl InnerBackend for BatchDeletes {
    fn inner_backend(&self) -> &dyn StorageBackend {
        &*self.inner
    }
}

delegate_storage_backend! {
    for BatchDeletes;
    except [delete, batch_delete_keys];

    fn delete(&self, key: &str) -> Result<()> {
        self.deletes.lock().unwrap().push(key.to_string());
        self.inner.delete(key)
    }

    fn batch_delete_keys(&self, keys: &[String], scope: vykar_storage::BatchDeleteScope) -> Result<()> {
        self.batches.lock().unwrap().push((keys.to_vec(), scope));
        if self.fail {
            if let Some(first) = keys.first() {
                self.inner.delete(first)?;
            }
            return Err(VykarError::Other("HTTP 408".into()));
        }
        for key in keys {
            self.inner.delete(key)?;
        }
        Ok(())
    }
}

fn gc_name(b: u8) -> ObjectName {
    ObjectName([b; 32])
}

const FOREIGN_KEY: &str = "index.d/README";

/// A memory backend holding the objects `names` and a foreign file.
fn gc_storage(names: &[u8]) -> Arc<dyn StorageBackend> {
    let mem: Arc<dyn StorageBackend> = Arc::new(MemoryBackend::new());
    for &b in names {
        mem.put(&object_key(&gc_name(b)), b"x").unwrap();
    }
    mem.put(FOREIGN_KEY, b"x").unwrap();
    mem
}

/// Live: object 1 (the base); retired: object 2.
fn gc_head() -> IndexHead {
    IndexHead {
        layout: crate::index::head::HEAD_LAYOUT_VERSION,
        state_hash: [1; 32],
        entries: 1,
        base: Some(BaseRef {
            name: gc_name(1),
            size: 1,
            state_hash: [1; 32],
            entries: 1,
        }),
        segments: vec![],
        retired: vec![gc_name(2)],
    }
}

fn keys_of(names: &[u8]) -> Vec<String> {
    let mut keys: Vec<String> = names.iter().map(|&b| object_key(&gc_name(b))).collect();
    keys.sort();
    keys
}

fn remaining(storage: &dyn StorageBackend) -> Vec<String> {
    let mut keys = storage.list(INDEX_OBJECT_DIR).unwrap();
    keys.sort();
    keys
}

fn remaining_after_gc() -> Vec<String> {
    let mut keys = keys_of(&[1]);
    keys.push(FOREIGN_KEY.to_string());
    keys.sort();
    keys
}

#[test]
fn collect_garbage_deletes_in_one_batch() {
    for (drop_retired, garbage) in [(true, &[2u8, 3, 4][..]), (false, &[3, 4][..])] {
        let storage = BatchDeletes::new(gc_storage(&[1, 2, 3, 4]), false);
        let deleted = collect_garbage(&storage, &gc_head(), drop_retired).unwrap();
        assert_eq!(deleted, garbage.len());
        let batches = storage.batches.lock().unwrap().clone();
        assert_eq!(batches.len(), 1, "{drop_retired}");
        let (mut keys, scope) = batches.into_iter().next().unwrap();
        keys.sort();
        assert_eq!(keys, keys_of(garbage));
        assert_eq!(scope, vykar_storage::BatchDeleteScope::KeysOnly);
        assert!(storage.deletes.lock().unwrap().is_empty());
        if drop_retired {
            assert_eq!(remaining(&storage), remaining_after_gc());
        }
    }
}

#[test]
fn collect_garbage_falls_back_when_the_batch_fails() {
    let storage = BatchDeletes::new(gc_storage(&[1, 2, 3, 4]), true);
    assert_eq!(collect_garbage(&storage, &gc_head(), true).unwrap(), 3);
    assert_eq!(storage.batches.lock().unwrap().len(), 1);
    let mut deletes = storage.deletes.lock().unwrap().clone();
    deletes.sort();
    assert_eq!(deletes, keys_of(&[2, 3, 4]));
    assert_eq!(remaining(&storage), remaining_after_gc());
}

#[test]
fn collect_garbage_without_batch_support() {
    let storage = gc_storage(&[1, 2, 3, 4]);
    assert_eq!(collect_garbage(&*storage, &gc_head(), true).unwrap(), 3);
    assert_eq!(remaining(&*storage), remaining_after_gc());
}

/// Fails `delete` of one key; no batch support.
struct FailOneDelete {
    inner: Arc<dyn StorageBackend>,
    key: String,
}

impl InnerBackend for FailOneDelete {
    fn inner_backend(&self) -> &dyn StorageBackend {
        &*self.inner
    }
}

delegate_storage_backend! {
    for FailOneDelete;
    except [delete];

    fn delete(&self, key: &str) -> Result<()> {
        if key == self.key {
            return Err(VykarError::Other(format!("injected delete failure: {key}")));
        }
        self.inner.delete(key)
    }
}

/// A failed single delete is logged, not counted, and does not stop the
/// others.
#[test]
fn collect_garbage_skips_a_failed_delete() {
    let failing = object_key(&gc_name(3));
    let storage = FailOneDelete {
        inner: gc_storage(&[1, 2, 3, 4]),
        key: failing.clone(),
    };
    assert_eq!(collect_garbage(&storage, &gc_head(), true).unwrap(), 2);
    let mut expected = remaining_after_gc();
    expected.push(failing);
    expected.sort();
    assert_eq!(remaining(&storage), expected);
}

#[test]
fn collect_garbage_with_nothing_to_delete_sends_no_request() {
    let storage = BatchDeletes::new(gc_storage(&[1, 2]), false);
    assert_eq!(collect_garbage(&storage, &gc_head(), false).unwrap(), 0);
    assert!(storage.batches.lock().unwrap().is_empty());
    assert!(storage.deletes.lock().unwrap().is_empty());
}

/// A recorded head state that the objects do not produce is rejected before
/// anything is installed: the cache path keeps what it held. Covers both the
/// segment branch and the base-only branch of the final state check.
#[test]
fn wrong_head_state_leaves_the_cache_path_alone() {
    let crypto = plaintext();
    let mem: Arc<dyn StorageBackend> = Arc::new(MemoryBackend::new());
    let mut w = Writer::new(Arc::clone(&mem), Arc::clone(&crypto));
    w.commit(&index_with(0..40, 0));
    w.fold();
    let base_only = w.head.clone();
    w.commit(&index_with(0..50, 1));
    let with_segment = w.head.clone();

    for mut head in [base_only, with_segment] {
        head.state_hash = [9; 32];
        let cache_dir = tempfile::tempdir().unwrap();
        let path = cache_dir.path().join("full_index_cache");
        let sync_err = || {
            sync(
                mem.as_ref(),
                crypto.as_ref(),
                head.clone(),
                Some(&path),
                None,
                true,
            )
            .err()
            .expect("wrong head state")
            .to_string()
        };

        let err = sync_err();
        assert!(err.contains("differs from the head"), "{err}");
        assert!(!path.exists(), "nothing installed at the path");

        build_full_index_cache_to_path(&index_with(500..510, 0), 9, &path).unwrap();
        let before = std::fs::read(&path).unwrap();
        let err = sync_err();
        assert!(err.contains("differs from the head"), "{err}");
        assert_eq!(std::fs::read(&path).unwrap(), before, "path unchanged");
    }
}

/// Smoke test for concurrent installs over one path: two readers on
/// different heads share one cache path, each renaming its result over the
/// other's, and both succeed with their own head's state. Not a regression
/// guard: the interleaving is not deterministic, so it would only
/// occasionally catch a sync that re-opens the path after its rename.
#[test]
fn concurrent_syncs_over_one_cache_path_succeed() {
    let crypto = plaintext();
    let mem: Arc<dyn StorageBackend> = Arc::new(MemoryBackend::new());
    let mut w = Writer::new(Arc::clone(&mem), Arc::clone(&crypto));
    let s1 = index_with(0..300, 0);
    let s2 = index_with(100..400, 1);
    w.commit(&s1);
    let h1 = w.head.clone();
    w.commit(&s2);
    let h2 = w.head.clone();
    let cache_dir = tempfile::tempdir().unwrap();
    let path = cache_dir.path().join("full_index_cache");

    std::thread::scope(|scope| {
        for (head, want) in [(&h1, &s1), (&h2, &s2)] {
            let (mem, crypto, path) = (&mem, &crypto, &path);
            scope.spawn(move || {
                for _ in 0..25 {
                    let synced = sync(
                        mem.as_ref(),
                        crypto.as_ref(),
                        head.clone(),
                        Some(path),
                        None,
                        true,
                    )
                    .unwrap();
                    assert_eq!(synced.head, *head);
                    assert_eq!(
                        entries(&chunk_index_from_full_cache(&synced.cache)),
                        entries(want)
                    );
                }
            });
        }
    });
}

/// A downloaded base with no segments to apply is the result as mapped, and
/// is renamed to the cache path while mapped.
#[test]
fn sync_downloads_a_base_without_segments_and_persists_it() {
    let crypto = plaintext();
    let mem: Arc<dyn StorageBackend> = Arc::new(MemoryBackend::new());
    let mut w = Writer::new(Arc::clone(&mem), Arc::clone(&crypto));
    let s1 = index_with(0..120, 0);
    w.commit(&s1);
    w.fold();
    let cache_dir = tempfile::tempdir().unwrap();
    let path = cache_dir.path().join("full_index_cache");

    let synced = sync(
        mem.as_ref(),
        crypto.as_ref(),
        w.head.clone(),
        Some(&path),
        None,
        true,
    )
    .unwrap();
    assert_eq!(synced.source, SyncSource::Downloaded { segments: 0 });
    assert!(synced.scratch.is_none(), "persisted at the cache path");
    let at_path = MmapFullIndexCache::open_verified(&path).expect("cache at the path");
    assert_eq!(
        (at_path.state_hash(), u64::from(at_path.entry_count())),
        (w.head.state_hash, w.head.entries)
    );
    assert_eq!(
        entries(&chunk_index_from_full_cache(&synced.cache)),
        entries(&s1)
    );
}

/// A reader on an older head persists its state over a newer cache (there
/// is no comparison, by design); the next sync to the newer head catches up
/// from it with the missing segment only.
#[test]
fn a_late_reader_leaves_an_older_state_that_the_next_sync_catches_up() {
    let crypto = plaintext();
    let mem: Arc<dyn StorageBackend> = Arc::new(MemoryBackend::new());
    let log = GetLog::new(Arc::clone(&mem));
    let mut w = Writer::new(Arc::clone(&mem), Arc::clone(&crypto));
    w.commit(&index_with(0..60, 0));
    let h1 = w.head.clone();
    let s2 = index_with(10..80, 1);
    w.commit(&s2);
    let h2 = w.head.clone();
    let cache_dir = tempfile::tempdir().unwrap();
    let path = cache_dir.path().join("full_index_cache");

    let state_at_path = || {
        let c = MmapFullIndexCache::open_verified(&path).unwrap();
        c.state_hash()
    };
    let read = |head: &IndexHead| {
        sync(
            mem.as_ref(),
            crypto.as_ref(),
            head.clone(),
            Some(&path),
            None,
            true,
        )
        .unwrap()
    };
    drop(read(&h2));
    assert_eq!(state_at_path(), h2.state_hash);
    drop(read(&h1));
    assert_eq!(
        state_at_path(),
        h1.state_hash,
        "the older state replaced it"
    );

    let synced = sync(&log, crypto.as_ref(), h2, Some(&path), None, true).unwrap();
    assert_eq!(synced.source, SyncSource::CaughtUp { segments: 1 });
    assert_eq!(index_d_gets(&log.take()), 1);
    assert_eq!(
        entries(&chunk_index_from_full_cache(&synced.cache)),
        entries(&s2)
    );
}

/// The writer's catch-up applies segments to a local cache in the chain and
/// otherwise gives up without touching storage; a lost segment is an error.
#[test]
fn catch_up_never_downloads_a_base() {
    let crypto = plaintext();
    let mem: Arc<dyn StorageBackend> = Arc::new(MemoryBackend::new());
    let log = GetLog::new(Arc::clone(&mem));
    let mut w = Writer::new(Arc::clone(&mem), Arc::clone(&crypto));
    w.commit(&index_with(0..60, 0));
    let h1 = w.head.clone();
    w.commit(&index_with(10..80, 1));
    let h2 = w.head.clone();
    let cache_dir = tempfile::tempdir().unwrap();
    let path = cache_dir.path().join("full_index_cache");

    let catch_up = |head: &IndexHead| {
        let mut location = CacheLocation {
            path: Some(&path),
            scratch: None,
            use_local: true,
        };
        let r = catch_up_local_cache(&log, crypto.as_ref(), head, &mut location, 2);
        assert!(location.scratch.is_none());
        r
    };
    let at_path = || {
        let c = MmapFullIndexCache::open_verified(&path).unwrap();
        (c.state_hash(), u64::from(c.entry_count()))
    };
    let read = |head: &IndexHead| {
        sync(
            mem.as_ref(),
            crypto.as_ref(),
            head.clone(),
            Some(&path),
            None,
            true,
        )
        .unwrap()
    };

    // (a) No local cache.
    assert!(catch_up(&h2).unwrap().is_none());
    assert_eq!(index_d_gets(&log.take()), 0);

    // (b) A cache outside the chain.
    build_full_index_cache_to_path(&index_with(500..510, 0), 9, &path).unwrap();
    assert!(catch_up(&h2).unwrap().is_none());
    assert_eq!(index_d_gets(&log.take()), 0);

    // (c) A cache at an earlier state of the chain.
    drop(read(&h1));
    assert_eq!(at_path(), (h1.state_hash, h1.entries));
    let (cache, source) = catch_up(&h2).unwrap().expect("caught up");
    assert_eq!(source, SyncSource::CaughtUp { segments: 1 });
    assert_eq!(
        (cache.state_hash(), u64::from(cache.entry_count())),
        (h2.state_hash, h2.entries)
    );
    drop(cache);
    assert_eq!(index_d_gets(&log.take()), 1);
    assert_eq!(at_path(), (h2.state_hash, h2.entries));

    // (d) A cache at the head.
    let (_, source) = catch_up(&h2).unwrap().expect("up to date");
    assert_eq!(source, SyncSource::UpToDate);
    assert_eq!(index_d_gets(&log.take()), 0);

    // (e) A cache at an earlier state, and the segment it needs is lost.
    drop(read(&h1));
    let lost = h2.segments.last().unwrap().name;
    mem.delete(&object_key(&lost)).unwrap();
    match catch_up(&h2) {
        Err(VykarError::IndexObjectMissing { kind, key }) => {
            assert_eq!(kind, "segment");
            assert_eq!(key, object_key(&lost));
        }
        Err(e) => panic!("unexpected error: {e}"),
        Ok(r) => panic!("expected IndexObjectMissing, got {:?}", r.map(|(_, s)| s)),
    }
}

#[test]
fn classify_head_tells_damage_from_legacy_and_newer_heads() {
    for crypto in [plaintext(), encrypted()] {
        let c = crypto.as_ref();
        let packed = pack_head(&IndexHead::empty(), c).unwrap();
        assert!(
            matches!(classify_head(&packed, c), StoredHead::Head(h) if h == IndexHead::empty())
        );

        let legacy =
            pack_object_with_context(ObjectType::ChunkIndex, INDEX_OBJECT_CONTEXT, b"x", c)
                .unwrap();
        assert!(matches!(classify_head(&legacy, c), StoredHead::Legacy));

        let mut flipped = packed.clone();
        *flipped.last_mut().unwrap() ^= 1;
        let nil = pack_object_with_context(ObjectType::IndexHead, INDEX_OBJECT_CONTEXT, b"\xc0", c)
            .unwrap();
        let half = packed[..packed.len() / 2].to_vec();
        for damaged in [vec![0u8; 10], flipped, nil, half] {
            assert!(
                matches!(classify_head(&damaged, c), StoredHead::Unreadable(_)),
                "{:?}",
                classify_head(&damaged, c)
            );
        }

        let layout2 = pack_head(
            &IndexHead {
                layout: 2,
                ..IndexHead::empty()
            },
            c,
        )
        .unwrap();
        assert!(matches!(
            classify_head(&layout2, c),
            StoredHead::UnknownLayout(2)
        ));
        let longer = |layout: u8| {
            let mut body = IndexHead::empty().encode().unwrap();
            body[0] = 0x97;
            body[1] = layout;
            body.push(0xc0);
            pack_object_with_context(ObjectType::IndexHead, INDEX_OBJECT_CONTEXT, &body, c).unwrap()
        };
        assert!(matches!(
            classify_head(&longer(2), c),
            StoredHead::UnknownLayout(2)
        ));
        assert!(matches!(
            classify_head(&longer(1), c),
            StoredHead::Unreadable(_)
        ));

        let unknown = [200u8, 1, 2, 3];
        assert!(matches!(
            classify_head(&unknown, c),
            StoredHead::UnknownType(200)
        ));

        let err = decode_head(&longer(2), c).unwrap_err().to_string();
        assert!(err.contains("unsupported index head layout 2"), "{err}");
        assert!(matches!(
            decode_head(&unknown, c),
            Err(VykarError::UnknownObjectType(200))
        ));
    }
}

/// [`rebuild_without_head`] with temp files in `dir`.
fn rebuild_in(
    dir: &Path,
    storage: &dyn StorageBackend,
    crypto: &dyn CryptoEngine,
) -> Result<HeadlessOutcome> {
    let names = list_objects(storage)?;
    rebuild_without_head(storage, crypto, names, &dir.join("x"))
}

fn rebuild(storage: &dyn StorageBackend, crypto: &dyn CryptoEngine) -> Result<HeadlessOutcome> {
    let dir = tempfile::tempdir().unwrap();
    rebuild_in(dir.path(), storage, crypto)
}

/// The entries of the rebuilt index, and the counts.
fn rebuilt(
    storage: &dyn StorageBackend,
    crypto: &dyn CryptoEngine,
) -> (Vec<(ChunkId, ChunkIndexEntry)>, Headless) {
    let dir = tempfile::tempdir().unwrap();
    match rebuild_in(dir.path(), storage, crypto).unwrap() {
        HeadlessOutcome::Rebuilt(r) => {
            let cache = MmapFullIndexCache::open_verified(&r.cache).unwrap();
            let got = entries(&chunk_index_from_full_cache(&cache));
            drop(cache);
            (got, r)
        }
        HeadlessOutcome::Refused(msg) => panic!("refused: {msg}"),
    }
}

fn refused(storage: &dyn StorageBackend, crypto: &dyn CryptoEngine) -> String {
    match rebuild(storage, crypto).unwrap() {
        HeadlessOutcome::Refused(msg) => msg,
        HeadlessOutcome::Rebuilt(r) => panic!(
            "rebuilt: bases {}, applied {}, unplaced {}, damaged {}",
            r.bases, r.applied, r.unplaced, r.damaged
        ),
    }
}

fn counts(r: &Headless) -> (usize, usize, usize, usize) {
    (r.bases, r.applied, r.unplaced, r.damaged)
}

fn writer(crypto: &Arc<dyn CryptoEngine>) -> (Arc<dyn StorageBackend>, Writer) {
    let mem: Arc<dyn StorageBackend> = Arc::new(MemoryBackend::new());
    let w = Writer::new(Arc::clone(&mem), Arc::clone(crypto));
    (mem, w)
}

#[test]
fn headless_rebuild_follows_the_chain_from_the_empty_state() {
    for crypto in [plaintext(), encrypted()] {
        let (mem, mut w) = writer(&crypto);
        w.commit(&index_with(0..40, 0));
        let s2 = index_with(10..60, 1);
        w.commit(&s2);
        let (got, r) = rebuilt(mem.as_ref(), crypto.as_ref());
        assert_eq!(got, entries(&s2));
        assert_eq!(counts(&r), (0, 2, 0, 0));
    }
}

#[test]
fn headless_rebuild_across_a_same_state_fold() {
    for crypto in [plaintext(), encrypted()] {
        let (mem, mut w) = writer(&crypto);
        w.commit(&index_with(0..40, 0));
        w.commit(&index_with(10..60, 1));
        w.fold();
        let s3 = index_with(10..70, 2);
        w.commit(&s3);
        let (got, r) = rebuilt(mem.as_ref(), crypto.as_ref());
        assert_eq!(got, entries(&s3));
        // One apply from the base, two from the empty state (the second
        // joining at the base's state).
        assert_eq!(counts(&r), (1, 3, 0, 0));

        w.fold();
        let s4 = index_with(20..70, 0);
        w.commit(&s4);
        let (got, r) = rebuilt(mem.as_ref(), crypto.as_ref());
        assert_eq!(got, entries(&s4));
        assert_eq!(r.bases, 2);
        assert_eq!(r.unplaced, 0);
    }
}

/// After a fold that changes the state, nothing links the retired chain to
/// the new base: two end states.
#[test]
fn headless_rebuild_refuses_two_end_states_after_a_state_changing_fold() {
    for crypto in [plaintext(), encrypted()] {
        let (mem, mut w) = writer(&crypto);
        w.commit(&index_with(0..40, 0));
        w.commit(&index_with(10..60, 1));
        w.fold_to(&index_with(10..60, 0));
        let s4 = index_with(10..80, 0);
        w.commit(&s4);
        let live_base = w.head.base.as_ref().unwrap().name;
        let msg = refused(mem.as_ref(), crypto.as_ref());
        assert!(msg.contains("2 different index states"), "{msg}");
        assert!(msg.contains("the empty index"), "{msg}");
        assert!(msg.contains(&live_base.to_hex()), "{msg}");

        for name in &w.head.retired {
            mem.delete(&object_key(name)).unwrap();
        }
        let (got, r) = rebuilt(mem.as_ref(), crypto.as_ref());
        assert_eq!(got, entries(&s4));
        assert_eq!((r.bases, r.applied), (1, 1));
    }
}

#[test]
fn headless_rebuild_falls_back_to_the_retired_generation() {
    for crypto in [plaintext(), encrypted()] {
        for later_commit in [false, true] {
            let (mem, mut w) = writer(&crypto);
            w.commit(&index_with(0..40, 0));
            let s2 = index_with(10..60, 1);
            w.commit(&s2);
            w.fold_to(&index_with(10..60, 0));
            if later_commit {
                w.commit(&index_with(10..80, 0));
            }
            let base = w.head.base.as_ref().unwrap().name;
            mem.delete(&object_key(&base)).unwrap();
            let (got, r) = rebuilt(mem.as_ref(), crypto.as_ref());
            assert_eq!(got, entries(&s2));
            assert_eq!(r.damaged, 0);
            assert_eq!(r.unplaced, usize::from(later_commit));
        }
    }
}

/// Accepted: when a fold returns to a state inside the retired chain, the
/// walk follows the retired segment past it. Nothing on storage tells this
/// from compact's fold at the head's state. Usually harmless: the extra
/// entries were committed once and no snapshot references them now, so the
/// repair's refcount rebuild drops them; any removal it implies is in the
/// plan.
#[test]
fn headless_rebuild_runs_past_a_fold_back_to_an_earlier_state() {
    for crypto in [plaintext(), encrypted()] {
        let (mem, mut w) = writer(&crypto);
        let s1 = index_with(0..40, 0);
        w.commit(&s1);
        let s2 = index_with(10..60, 1);
        w.commit(&s2);
        w.fold_to(&s1);
        let (got, r) = rebuilt(mem.as_ref(), crypto.as_ref());
        assert_eq!(got, entries(&s2));
        assert_eq!(counts(&r), (1, 2, 0, 0));
    }
}

/// A segment left by a failed head PUT that follows the latest state is
/// applied: nothing tells it from a committed one.
#[test]
fn headless_rebuild_applies_tail_debris() {
    for crypto in [plaintext(), encrypted()] {
        let (mem, mut w) = writer(&crypto);
        w.commit(&index_with(0..40, 0));
        w.commit(&index_with(10..60, 1));
        let x = index_with(10..65, 1);
        w.put_unreferenced_segment(&x);
        let (got, r) = rebuilt(mem.as_ref(), crypto.as_ref());
        assert_eq!(got, entries(&x));
        assert_eq!(counts(&r), (0, 3, 0, 0));
    }
}

#[test]
fn headless_rebuild_refuses_a_fork() {
    for crypto in [plaintext(), encrypted()] {
        let (mem, mut w) = writer(&crypto);
        w.commit(&index_with(0..40, 0));
        let debris = w.put_unreferenced_segment(&index_with(0..45, 0));
        w.commit(&index_with(10..60, 1));
        let committed = w.head.segments[1].name;
        let msg = refused(mem.as_ref(), crypto.as_ref());
        assert!(msg.contains("2 segments follow the same state"), "{msg}");
        assert!(msg.contains(&debris.to_hex()), "{msg}");
        assert!(msg.contains(&committed.to_hex()), "{msg}");
        assert!(!msg.contains("remove the objects"), "{msg}");
    }
}

#[test]
fn headless_rebuild_refuses_a_cycle() {
    let crypto = plaintext();
    let (mem, mut w) = writer(&crypto);
    let s1 = index_with(0..40, 0);
    w.commit(&s1);
    w.commit(&index_with(10..60, 1));
    w.commit(&s1);
    let msg = refused(mem.as_ref(), crypto.as_ref());
    assert!(msg.contains("return to an earlier state"), "{msg}");
    assert!(!msg.contains("remove the objects"), "{msg}");
}

#[test]
fn headless_rebuild_skips_damaged_objects() {
    for crypto in [plaintext(), encrypted()] {
        let (mem, mut w) = writer(&crypto);
        w.commit(&index_with(0..40, 0));
        let s2 = index_with(10..60, 1);
        w.commit(&s2);
        mem.put(&format!("{INDEX_OBJECT_DIR}{}", "ab".repeat(32)), b"x")
            .unwrap();
        let garbage = pack_object_with_context(
            ObjectType::IndexSegment,
            SEGMENT_CONTEXT,
            // Uncompressed (tag 0), segment version 1, then garbage.
            b"\x00\x01garbage",
            crypto.as_ref(),
        )
        .unwrap();
        mem.put(&object_key(&ObjectName::of(&garbage)), &garbage)
            .unwrap();
        mem.put(&format!("{INDEX_OBJECT_DIR}foo"), b"x").unwrap();
        let (got, r) = rebuilt(mem.as_ref(), crypto.as_ref());
        assert_eq!(got, entries(&s2));
        assert_eq!(counts(&r), (0, 2, 0, 2));
    }
}

#[test]
fn headless_rebuild_refuses_objects_of_other_types() {
    for crypto in [plaintext(), encrypted()] {
        let blob = pack_object_with_context(
            ObjectType::ChunkIndex,
            INDEX_OBJECT_CONTEXT,
            b"whatever",
            crypto.as_ref(),
        )
        .unwrap();
        for (bytes, tag) in [(blob, 4u8), (vec![200u8, 7, 7], 200)] {
            let (mem, mut w) = writer(&crypto);
            w.commit(&index_with(0..40, 0));
            let name = ObjectName::of(&bytes);
            mem.put(&object_key(&name), &bytes).unwrap();
            let msg = refused(mem.as_ref(), crypto.as_ref());
            assert!(
                msg.contains("neither an index base nor an index segment"),
                "{msg}"
            );
            assert!(msg.contains(&format!("type tag {tag}")), "{msg}");
            assert!(msg.contains(&name.to_hex()), "{msg}");
        }
    }
}

#[test]
fn headless_rebuild_refuses_objects_from_a_newer_vykar() {
    let crypto = plaintext();
    let pack_raw = |ty: ObjectType, context: &[u8], body: &[u8]| {
        pack_object_streaming_with_context(ty, context, body.len() + 64, crypto.as_ref(), |buf| {
            compress::compress_stream_zstd(buf, 3, |w| {
                std::io::Write::write_all(w, body)?;
                Ok(())
            })
        })
        .unwrap()
    };

    let (mem, mut w) = writer(&crypto);
    w.commit(&index_with(0..40, 0));
    let seg_ref = w.head.segments[0].clone();
    let bytes = mem.get(&object_key(&seg_ref.name)).unwrap().unwrap();
    let mut body = decode_segment(&seg_ref, &bytes, crypto.as_ref())
        .unwrap()
        .encode();
    body[0] = 99;
    let packed = pack_raw(ObjectType::IndexSegment, SEGMENT_CONTEXT, &body);
    mem.put(&object_key(&ObjectName::of(&packed)), &packed)
        .unwrap();
    assert!(matches!(
        rebuild(mem.as_ref(), crypto.as_ref()),
        Err(VykarError::UnsupportedVersion(99))
    ));

    let (mem, _w) = writer(&crypto);
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("cache");
    build_full_index_cache_to_path(&index_with(0..40, 0), 1, &path).unwrap();
    let cache = MmapFullIndexCache::open_verified(&path).unwrap();
    let mut body = Vec::new();
    base::encode_base(&cache, &mut body).unwrap();
    body[0] = 2;
    let packed = pack_raw(ObjectType::IndexBase, BASE_CONTEXT, &body);
    mem.put(&object_key(&ObjectName::of(&packed)), &packed)
        .unwrap();
    assert!(matches!(
        rebuild(mem.as_ref(), crypto.as_ref()),
        Err(VykarError::UnsupportedVersion(2))
    ));
}

/// An unknown compression tag behind a matching name is a newer writer, not
/// damage.
#[test]
fn headless_rebuild_refuses_an_unknown_compression() {
    for crypto in [plaintext(), encrypted()] {
        let (mem, mut w) = writer(&crypto);
        w.commit(&index_with(0..40, 0));
        let seg_ref = w.head.segments[0].clone();
        let bytes = mem.get(&object_key(&seg_ref.name)).unwrap().unwrap();
        let body = decode_segment(&seg_ref, &bytes, crypto.as_ref())
            .unwrap()
            .encode();
        let packed = pack_object_streaming_with_context(
            ObjectType::IndexSegment,
            SEGMENT_CONTEXT,
            body.len() + 1,
            crypto.as_ref(),
            |buf| {
                buf.push(0xEE);
                buf.extend_from_slice(&body);
                Ok(())
            },
        )
        .unwrap();
        let name = ObjectName::of(&packed);
        mem.put(&object_key(&name), &packed).unwrap();
        let msg = refused(mem.as_ref(), crypto.as_ref());
        assert!(msg.contains("written by a newer vykar"), "{msg}");
        assert!(msg.contains("tag 238"), "{msg}");
        assert!(msg.contains(&name.to_hex()), "{msg}");
    }
}

/// [`headless_rebuild_refuses_an_unknown_compression`] for a base.
#[test]
fn headless_rebuild_refuses_a_base_with_an_unknown_compression() {
    let dir = tempfile::tempdir().unwrap();
    let cache = base_cache(dir.path());
    let mut body = Vec::new();
    base::encode_base(&cache, &mut body).unwrap();
    let mut payload = vec![0xEE];
    payload.extend_from_slice(&body);
    for crypto in [plaintext(), encrypted()] {
        let (mem, mut w) = writer(&crypto);
        w.commit(&index_with(0..40, 0));
        let (base_ref, bytes) = base_object(crypto.as_ref(), &payload, &cache);
        mem.put(&object_key(&base_ref.name), &bytes).unwrap();
        let msg = refused(mem.as_ref(), crypto.as_ref());
        assert!(msg.contains("written by a newer vykar"), "{msg}");
        assert!(msg.contains("tag 238"), "{msg}");
        assert!(msg.contains(&base_ref.name.to_hex()), "{msg}");
    }
}

/// The full cache of `index_with(0..40, 0)`, in `dir`.
fn base_cache(dir: &Path) -> MmapFullIndexCache {
    let path = dir.join("cache");
    build_full_index_cache_to_path(&index_with(0..40, 0), 1, &path).unwrap();
    MmapFullIndexCache::open_verified(&path).unwrap()
}

/// The zstd payload (codec tag included) of `cache`'s base.
fn zstd_base_payload(cache: &MmapFullIndexCache) -> Vec<u8> {
    let mut payload = Vec::new();
    compress::compress_stream_zstd(&mut payload, 3, |w| base::encode_base(cache, w)).unwrap();
    payload
}

/// A named base object whose payload is `payload` (codec tag included), and its ref.
fn base_object(
    crypto: &dyn CryptoEngine,
    payload: &[u8],
    cache: &MmapFullIndexCache,
) -> (BaseRef, Vec<u8>) {
    let bytes =
        pack_object_with_context(ObjectType::IndexBase, BASE_CONTEXT, payload, crypto).unwrap();
    let base_ref = BaseRef {
        name: ObjectName::of(&bytes),
        size: bytes.len() as u64,
        state_hash: cache.state_hash(),
        entries: u64::from(cache.entry_count()),
    };
    (base_ref, bytes)
}

/// Decode `payload` as a stored base in an empty dir; the error and whether
/// the dir is still empty afterwards.
fn decode_base_payload(
    crypto: &dyn CryptoEngine,
    payload: &[u8],
    cache: &MmapFullIndexCache,
) -> (Result<TempPath>, bool) {
    let dir = tempfile::tempdir().unwrap();
    let (base_ref, bytes) = base_object(crypto, payload, cache);
    let out = decode_base_to_temp(&base_ref, bytes, crypto, &dir.path().join("anchor"));
    let empty = std::fs::read_dir(dir.path()).unwrap().next().is_none();
    (out, empty)
}

#[test]
fn base_with_a_truncated_zstd_frame_is_object_damage() {
    let cache_dir = tempfile::tempdir().unwrap();
    let cache = base_cache(cache_dir.path());
    let payload = zstd_base_payload(&cache);
    for crypto in [plaintext(), encrypted()] {
        let (ok, _) = decode_base_payload(crypto.as_ref(), &payload, &cache);
        ok.unwrap();
        for cut in [payload.len() / 2, payload.len() - 1] {
            let (out, empty) = decode_base_payload(crypto.as_ref(), &payload[..cut], &cache);
            let e = out.unwrap_err();
            assert!(is_object_damage(&e), "cut at {cut}: {e:?}");
            assert!(empty, "cut at {cut}: a temp file was left behind");
        }
    }
}

#[test]
fn base_with_bytes_after_its_zstd_frame_is_object_damage() {
    let cache_dir = tempfile::tempdir().unwrap();
    let cache = base_cache(cache_dir.path());
    let mut payload = zstd_base_payload(&cache);
    payload.push(0x00);
    for crypto in [plaintext(), encrypted()] {
        let (out, empty) = decode_base_payload(crypto.as_ref(), &payload, &cache);
        let e = out.unwrap_err();
        assert!(is_object_damage(&e), "{e:?}");
        assert!(empty, "a temp file was left behind");
    }
}

/// A listed object that is gone at GET time is skipped as damage.
#[test]
fn headless_rebuild_skips_a_listed_object_that_is_gone() {
    let crypto = plaintext();
    let (mem, mut w) = writer(&crypto);
    w.commit(&index_with(0..40, 0));
    let s2 = index_with(10..60, 1);
    w.commit(&s2);
    let mut names = list_objects(mem.as_ref()).unwrap();
    names.push(ObjectName([0xab; 32]));
    let dir = tempfile::tempdir().unwrap();
    let r = match rebuild_without_head(mem.as_ref(), crypto.as_ref(), names, &dir.path().join("x"))
        .unwrap()
    {
        HeadlessOutcome::Rebuilt(r) => r,
        HeadlessOutcome::Refused(msg) => panic!("refused: {msg}"),
    };
    let cache = MmapFullIndexCache::open_verified(&r.cache).unwrap();
    assert_eq!(entries(&chunk_index_from_full_cache(&cache)), entries(&s2));
    assert_eq!(counts(&r), (0, 2, 0, 1));
}

/// A segment that follows a reached state but does not apply to it ends the
/// walk there; it is damaged, not unplaced.
#[test]
fn headless_rebuild_stops_at_a_segment_that_does_not_apply() {
    for crypto in [plaintext(), encrypted()] {
        let (mem, mut w) = writer(&crypto);
        let s1 = index_with(0..40, 0);
        w.commit(&s1);
        let seg_ref = w.head.segments[0].clone();
        let bytes = mem.get(&object_key(&seg_ref.name)).unwrap().unwrap();
        let first = decode_segment(&seg_ref, &bytes, crypto.as_ref()).unwrap();
        // Adds an entry the state already holds.
        let bad = Segment {
            prev_state_hash: w.head.state_hash,
            prev_entries: w.head.entries,
            deltas: Vec::new(),
            relocations: Vec::new(),
            new_entries: first.new_entries[..1].to_vec(),
        };
        let obj = pack_segment(&bad, crypto.as_ref()).unwrap();
        mem.put(&obj.key(), &obj.bytes).unwrap();
        let (got, r) = rebuilt(mem.as_ref(), crypto.as_ref());
        assert_eq!(got, entries(&s1));
        assert_eq!(counts(&r), (0, 1, 0, 1));
    }
}

#[test]
fn headless_rebuild_propagates_storage_errors() {
    let crypto = plaintext();
    let (mem, mut w) = writer(&crypto);
    w.commit(&index_with(0..40, 0));
    w.commit(&index_with(10..60, 1));
    let key = object_key(&w.head.segments[1].name);
    let failing = key.clone();
    let storage = FailGet::new(Arc::clone(&mem), move |k| k == failing, usize::MAX);
    let err = match rebuild(&storage, crypto.as_ref()) {
        Err(e) => e.to_string(),
        Ok(_) => panic!("expected a storage error"),
    };
    assert!(err.contains("injected") && err.contains(&key), "{err}");
}

#[test]
fn headless_rebuild_of_nothing_is_the_empty_index() {
    let crypto = plaintext();
    let mem = MemoryBackend::new();
    let (got, r) = rebuilt(&mem, crypto.as_ref());
    assert!(got.is_empty());
    assert_eq!(counts(&r), (0, 0, 0, 0));

    for crypto in [plaintext(), encrypted()] {
        let (mem, mut w) = writer(&crypto);
        w.commit(&index_with(0..40, 0));
        w.fold_to(&index_with(0..50, 0));
        w.commit(&index_with(0..60, 0));
        let base = w.head.base.as_ref().unwrap().name;
        mem.delete(&object_key(&base)).unwrap();
        for name in &w.head.retired {
            mem.delete(&object_key(name)).unwrap();
        }
        let (got, r) = rebuilt(mem.as_ref(), crypto.as_ref());
        assert!(got.is_empty());
        assert_eq!(counts(&r), (0, 0, 1, 0));
    }
}

/// A base left by a failed headless repair write: refused when its state is
/// new, joined when it is the chain's.
#[test]
fn headless_rebuild_with_a_debris_base() {
    for crypto in [plaintext(), encrypted()] {
        let (mem, mut w) = writer(&crypto);
        w.commit(&index_with(0..40, 0));
        let s2 = index_with(10..60, 1);
        w.commit(&s2);
        let debris = w.put_unreferenced_base(&index_with(10..60, 0));
        let msg = refused(mem.as_ref(), crypto.as_ref());
        assert!(msg.contains("2 different index states"), "{msg}");
        assert!(
            msg.contains(&format!(
                "from base {}: 0 segment(s) applied",
                debris.to_hex()
            )),
            "{msg}"
        );
        assert!(
            msg.contains("remove the objects that do not belong"),
            "{msg}"
        );

        mem.delete(&object_key(&debris)).unwrap();
        w.put_unreferenced_base(&s2);
        let (got, r) = rebuilt(mem.as_ref(), crypto.as_ref());
        assert_eq!(got, entries(&s2));
        assert_eq!(r.bases, 1);
    }
}
