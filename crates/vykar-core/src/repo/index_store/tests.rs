use std::sync::{Arc, Mutex};

use super::*;
use crate::index::dedup_cache::{build_full_index_cache_to_path, chunk_index_from_full_cache};
use crate::index::head::empty_state_hash;
use crate::index::segment::diff_to_temp;
use crate::index::{ChunkIndex, ChunkIndexEntry};
use crate::repo::{EncryptionMode, RepoFormat};
use crate::testutil::MemoryBackend;
use vykar_crypto::key::MasterKey;
use vykar_crypto::PlaintextEngine;
use vykar_storage::{delegate_storage_backend, InnerBackend};
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
    assert!(mem
        .get(&object_key(&old_head.segments[0].name))
        .unwrap()
        .is_none());
    let synced = sync(&log, crypto.as_ref(), old_head, None, None, true).unwrap();
    assert_eq!(synced.head, w.head, "re-read the head");
    assert_eq!(
        entries(&chunk_index_from_full_cache(&synced.cache)),
        entries(&latest)
    );
    assert_eq!(log.take().iter().filter(|k| *k == "index").count(), 1);
}

/// When objects keep vanishing the reader gives up after bounded retries.
#[test]
fn vanishing_objects_give_up_after_bounded_retries() {
    let crypto = plaintext();
    let mem: Arc<dyn StorageBackend> = Arc::new(MemoryBackend::new());
    let log = GetLog::new(Arc::clone(&mem));
    let mut w = Writer::new(Arc::clone(&mem), Arc::clone(&crypto));
    w.commit(&index_with(0..40, 0));
    mem.delete(&object_key(&w.head.segments[0].name)).unwrap();
    let err = sync(&log, crypto.as_ref(), w.head.clone(), None, None, true)
        .err()
        .expect("gives up");
    assert!(err.to_string().contains("vanishing"), "{err}");
    let heads = log.take().iter().filter(|k| *k == "index").count();
    assert_eq!(heads, MAX_HEAD_RETRIES);
}

#[test]
fn fetch_all_preserves_order_and_reports_absence() {
    let mem = MemoryBackend::new();
    let keys: Vec<String> = (0..9).map(|i| format!("k{i}")).collect();
    for (i, k) in keys.iter().enumerate() {
        mem.put(k, &[i as u8]).unwrap();
    }
    for width in [1, 3, 16] {
        let got = fetch_all(&mem, &keys, width).unwrap().unwrap();
        assert_eq!(got, (0..9u8).map(|i| vec![i]).collect::<Vec<_>>());
    }
    mem.delete("k4").unwrap();
    for width in [1, 3] {
        assert!(fetch_all(&mem, &keys, width).unwrap().is_none());
    }
    assert_eq!(fetch_all(&mem, &[], 4).unwrap(), Some(Vec::new()));
}
