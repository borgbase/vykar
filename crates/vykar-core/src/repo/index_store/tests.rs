use std::sync::{Arc, Mutex};

use super::*;
use crate::index::dedup_cache::build_full_index_cache_to_path;
use crate::index::head::{HEAD_LAYOUT_VERSION, SegmentKind, SweepState, empty_state_hash};
use crate::index::segment::diff_to_temp;
use crate::index::table::{self, ChunkTable};
use crate::index::{ChunkIndex, ChunkIndexEntry};
use crate::repo::{EncryptionMode, RepoFormat};
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
    except [get];

    fn get(&self, key: &str) -> Result<Option<Vec<u8>>> {
        self.gets.lock().unwrap().push(key.to_string());
        self.inner.get(key)
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
    crate::repo::key::build_engine(&EncryptionMode::Aes256Gcm, RepoFormat::V6, &key).unwrap()
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

pub(crate) fn entries(index: &impl crate::testutil::EntryIter) -> Vec<(ChunkId, ChunkIndexEntry)> {
    index.entry_vec()
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
            head: IndexHead::initial(),
        };
        w.put_head();
        w
    }

    fn cache_path(&self) -> PathBuf {
        self.dir.path().join("cache")
    }

    fn put_head(&self) {
        put_head(self.storage.as_ref(), &self.head, self.crypto.as_ref()).unwrap();
    }

    /// Write `next` as a segment and publish the head.
    pub(crate) fn commit(&mut self, next: &ChunkIndex) {
        let prev = MmapFullIndexCache::open_verified(&self.cache_path()).unwrap();
        let (diff, tmp) = diff_to_temp(&prev, next, &self.cache_path()).unwrap();
        drop(prev);
        tmp.persist(self.cache_path()).unwrap();
        let obj = pack_segment(&diff.segment, self.crypto.as_ref()).unwrap();
        self.storage.put(&obj.key(), &obj.bytes).unwrap();
        self.head = self.head.successor();
        self.head.segments.push(SegmentRef {
            name: obj.name,
            size: obj.size(),
            kind: SegmentKind::Maintenance,
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
        self.head = self.head.successor();
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
    dir: Option<&Path>,
    live: Option<Arc<ChunkTable>>,
    use_local: bool,
) -> Result<SyncedIndex> {
    sync_full_cache(
        storage,
        crypto,
        head,
        CacheLocation {
            dir,
            live,
            use_local,
        },
        2,
    )
}

/// The full cache file `src` copied into `dir` as the table of its state.
fn place_table(dir: &Path, src: &Path) -> PathBuf {
    let state = MmapFullIndexCache::open_verified(src).unwrap().state_hash();
    let path = table::table_path(dir, &state);
    std::fs::copy(src, &path).unwrap();
    path
}

/// `index` written into `dir` as the table of its state.
fn write_table(dir: &Path, index: &ChunkIndex) -> PathBuf {
    let src = dir.join("source");
    build_full_index_cache_to_path(index, 1, &src).unwrap();
    let path = place_table(dir, &src);
    std::fs::remove_file(src).unwrap();
    path
}

/// The states of the table files in `dir`, sorted.
fn table_states(dir: &Path) -> Vec<StateHash> {
    let mut v: Vec<StateHash> = table::list_tables(dir)
        .into_iter()
        .map(|(s, _)| s)
        .collect();
    v.sort_unstable();
    v
}

fn index_d_gets(gets: &[String]) -> usize {
    gets.iter()
        .filter(|k| k.starts_with(INDEX_OBJECT_DIR))
        .count()
}

#[test]
fn head_round_trips_through_storage_encoding() {
    for crypto in [plaintext(), encrypted()] {
        let head = IndexHead::initial();
        let packed = pack_head(&head, crypto.as_ref()).unwrap();
        assert_eq!(packed[0], ObjectType::IndexHead as u8);
        let decoded = classify_head(1, &packed, crypto.as_ref()).into_head();
        assert_eq!(decoded.unwrap(), head);
    }
}

/// STORE-1: the highest generation wins; keys that are not canonical
/// generations are ignored.
#[test]
fn fetch_head_takes_the_highest_generation() {
    let crypto = plaintext();
    let mem: Arc<dyn StorageBackend> = Arc::new(MemoryBackend::new());
    let mut head = IndexHead::initial();
    for _ in 0..3 {
        put_head(mem.as_ref(), &head, crypto.as_ref()).unwrap();
        head = head.successor();
    }
    mem.put("index.h/01", b"foreign").unwrap();
    mem.put("index.h/x", b"foreign").unwrap();
    mem.put("index.h/99", b"").unwrap();
    mem.delete("index.h/99").unwrap();
    assert_eq!(list_head_generations(mem.as_ref()).unwrap(), vec![1, 2, 3]);
    assert_eq!(
        fetch_head(mem.as_ref(), crypto.as_ref())
            .unwrap()
            .generation,
        3
    );
}

/// STORE-2: no head; a head under the wrong generation's key.
#[test]
fn missing_head_and_misplaced_head() {
    let crypto = plaintext();
    let mem: Arc<dyn StorageBackend> = Arc::new(MemoryBackend::new());
    let err = fetch_head(mem.as_ref(), crypto.as_ref())
        .unwrap_err()
        .to_string();
    assert!(err.contains("index head missing"), "{err}");
    assert!(fetch_latest_head_bytes(mem.as_ref()).unwrap().is_none());

    let mut head = IndexHead::initial();
    head.generation = 4;
    mem.put(&head_key(5), &pack_head(&head, crypto.as_ref()).unwrap())
        .unwrap();
    let err = fetch_head(mem.as_ref(), crypto.as_ref())
        .unwrap_err()
        .to_string();
    assert!(err.contains("index.h/5 names generation 4"), "{err}");
    let data = mem.get(&head_key(5)).unwrap().unwrap();
    assert!(matches!(
        classify_head(5, &data, crypto.as_ref()),
        StoredHead::Unreadable(_)
    ));
}

/// Deletes the latest head on its first `n` GETs (another writer's GC
/// between the LIST and the GET).
struct VanishingHead {
    inner: Arc<dyn StorageBackend>,
    n: Mutex<usize>,
    /// The head stays and answers 404 every time (true), or each vanished
    /// head is deleted for good and the next one is found (false).
    persistent: bool,
}

impl InnerBackend for VanishingHead {
    fn inner_backend(&self) -> &dyn StorageBackend {
        &*self.inner
    }
}

delegate_storage_backend! {
    for VanishingHead;
    except [get];

    fn get(&self, key: &str) -> Result<Option<Vec<u8>>> {
        let mut n = self.n.lock().unwrap();
        if key.starts_with(HEAD_PREFIX) && *n > 0 {
            *n -= 1;
            if !self.persistent {
                self.inner.delete(key)?;
            }
            return Ok(None);
        }
        self.inner.get(key)
    }
}

/// STORE-3: a 404 after the LIST re-lists; a persistent 404 gives up.
#[test]
fn a_head_vanishing_after_the_listing_is_retried() {
    let crypto = plaintext();
    let mem: Arc<dyn StorageBackend> = Arc::new(MemoryBackend::new());
    let mut head = IndexHead::initial();
    for _ in 0..3 {
        put_head(mem.as_ref(), &head, crypto.as_ref()).unwrap();
        head = head.successor();
    }
    let vanishing = VanishingHead {
        inner: Arc::clone(&mem),
        n: Mutex::new(1),
        persistent: false,
    };
    assert_eq!(
        fetch_head(&vanishing, crypto.as_ref()).unwrap().generation,
        2
    );

    let stuck = VanishingHead {
        inner: Arc::clone(&mem),
        n: Mutex::new(usize::MAX),
        persistent: true,
    };
    assert!(matches!(
        fetch_head(&stuck, crypto.as_ref()),
        Err(VykarError::IndexHeadUnstable)
    ));
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
        let path = cache_dir.path().join("sub");

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
        assert_eq!(
            synced.table.file_path(),
            Some(table::table_path(&path, &w.head.state_hash).as_path()),
            "published under its state"
        );
        assert_eq!(entries(&*synced.table), entries(&s2));
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
        assert_eq!(entries(&*synced.table), entries(&s3));
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
    let path = cache_dir.path().to_path_buf();
    place_table(&path, &w.cache_path());
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
    assert_eq!(entries(&*synced.table), entries(&last));
}

/// Tables at states outside the chain are not used, nor is a file whose
/// name is a state of the chain but whose content is another state.
#[test]
fn local_start_ignores_tables_outside_the_chain() {
    let crypto = plaintext();
    let mem: Arc<dyn StorageBackend> = Arc::new(MemoryBackend::new());
    let mut w = Writer::new(Arc::clone(&mem), Arc::clone(&crypto));
    let base_state = w.head.state_hash;
    let s1 = index_with(0..30, 0);
    w.commit(&s1);
    let cache_dir = tempfile::tempdir().unwrap();
    let path = cache_dir.path().to_path_buf();
    let unrelated = write_table(&path, &index_with(500..510, 0));
    // The chain's base state (the empty index) named over other content.
    std::fs::copy(&unrelated, table::table_path(&path, &base_state)).unwrap();

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
    assert_eq!(entries(&*synced.table), entries(&s1));
}

/// An empty base (`base: None`) with no segments is the empty index.
#[test]
fn sync_of_the_empty_head() {
    let crypto = plaintext();
    let mem: Arc<dyn StorageBackend> = Arc::new(MemoryBackend::new());
    let synced = sync(
        mem.as_ref(),
        crypto.as_ref(),
        IndexHead::initial(),
        None,
        None,
        true,
    )
    .unwrap();
    assert_eq!(synced.table.len(), 0);
    assert_eq!(synced.table.state().0, empty_state_hash());
    assert!(
        synced.table.file_path().is_none(),
        "no cache dir: a temp file"
    );
}

/// Without a cache dir the result is a temp file, reused by the next sync
/// of the same run as the live table.
#[test]
fn sync_without_cache_dir_keeps_a_scratch_file() {
    let crypto = plaintext();
    let mem: Arc<dyn StorageBackend> = Arc::new(MemoryBackend::new());
    let log = GetLog::new(Arc::clone(&mem));
    let mut w = Writer::new(Arc::clone(&mem), Arc::clone(&crypto));
    w.commit(&index_with(0..40, 0));
    let synced = sync(&log, crypto.as_ref(), w.head.clone(), None, None, true).unwrap();
    assert!(synced.table.file_path().is_none(), "temp table");
    let live = synced.table;
    log.take();

    let s2 = index_with(5..45, 1);
    w.commit(&s2);
    let synced = sync(
        &log,
        crypto.as_ref(),
        w.head.clone(),
        None,
        Some(live),
        true,
    )
    .unwrap();
    assert_eq!(synced.source, SyncSource::CaughtUp { segments: 1 });
    assert_eq!(entries(&*synced.table), entries(&s2));
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
    let cache_dir = tempfile::tempdir().unwrap();
    let path = cache_dir.path().to_path_buf();
    let local = place_table(&path, &w.cache_path());
    let before = std::fs::read(&local).unwrap();
    let live = Arc::new(ChunkTable::open_file(&local).unwrap());

    let synced = sync(
        &log,
        crypto.as_ref(),
        w.head.clone(),
        Some(&path),
        Some(live),
        false,
    )
    .unwrap();
    assert_eq!(synced.source, SyncSource::Downloaded { segments: 1 });
    assert!(synced.table.file_path().is_none());
    assert_eq!(index_d_gets(&log.take()), 2);
    assert_eq!(std::fs::read(&local).unwrap(), before);
    assert_eq!(table_states(&path), vec![w.head.state_hash]);
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
    assert_eq!(head_gets(&log.take()), 0);

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
    assert_eq!(entries(&*synced.table), entries(&latest));
    let gets = log.take();
    assert_eq!(head_gets(&gets), 1);
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
    gets.iter().filter(|k| k.starts_with(HEAD_PREFIX)).count()
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

/// Serves a queue of heads under `index.h/`, one per LIST and GET, and has
/// no `index.d/` objects at all: every re-read head moved and none names
/// what is missing.
struct MovingHead {
    inner: Arc<dyn StorageBackend>,
    heads: Mutex<std::collections::VecDeque<(u64, Vec<u8>)>>,
    gets: Mutex<Vec<String>>,
}

impl InnerBackend for MovingHead {
    fn inner_backend(&self) -> &dyn StorageBackend {
        &*self.inner
    }
}

delegate_storage_backend! {
    for MovingHead;
    except [get, list, list_sized, list_meta, server_ops];

    fn get(&self, key: &str) -> Result<Option<Vec<u8>>> {
        self.gets.lock().unwrap().push(key.to_string());
        if key.starts_with(HEAD_PREFIX) {
            return Ok(self.heads.lock().unwrap().pop_front().map(|(_, b)| b));
        }
        if key.starts_with(INDEX_OBJECT_DIR) {
            return Ok(None);
        }
        self.inner.get(key)
    }

    fn list(&self, prefix: &str) -> Result<Vec<String>> {
        if prefix == HEAD_PREFIX {
            return Ok(self
                .heads
                .lock()
                .unwrap()
                .front()
                .map(|(g, _)| head_key(*g))
                .into_iter()
                .collect());
        }
        self.inner.list(prefix)
    }

    fn list_sized(&self, prefix: &str) -> Result<Vec<vykar_storage::ListedObject>> {
        self.inner.list_sized(prefix)
    }

    fn list_meta(&self, prefix: &str) -> Result<Vec<vykar_storage::ListedObject>> {
        self.inner.list_meta(prefix)
    }

    fn server_ops(&self) -> Option<&dyn vykar_storage::ServerOps> {
        None
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
                .map(|h| (h.generation, pack_head(h, crypto.as_ref()).unwrap()))
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

/// Records `delete` and batch-delete calls. A batch deletes every
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
    except [delete, batch_delete];

    fn delete(&self, key: &str) -> Result<()> {
        self.deletes.lock().unwrap().push(key.to_string());
        self.inner.delete(key)
    }

    fn batch_delete(&self) -> Option<&dyn vykar_storage::BatchDelete> {
        Some(self)
    }
}

impl vykar_storage::BatchDelete for BatchDeletes {
    fn delete_keys(&self, keys: &[String], scope: vykar_storage::BatchDeleteScope) -> Result<()> {
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

/// A memory backend whose `list_meta` reports mtimes: `now` for every key
/// unless `age` set another; and that refuses deletes of `refused` keys as
/// an append-only server does.
struct Aged {
    inner: Arc<dyn StorageBackend>,
    now: std::time::SystemTime,
    mtimes: Mutex<std::collections::HashMap<String, std::time::SystemTime>>,
    refused: Vec<String>,
}

impl Aged {
    fn new() -> Self {
        Self {
            inner: Arc::new(MemoryBackend::new()),
            now: std::time::SystemTime::UNIX_EPOCH + std::time::Duration::from_secs(1 << 30),
            mtimes: Mutex::new(Default::default()),
            refused: Vec::new(),
        }
    }

    /// Make `key` two hours old.
    fn age(&self, key: &str) {
        self.mtimes.lock().unwrap().insert(
            key.to_string(),
            self.now - std::time::Duration::from_secs(7200),
        );
    }
}

impl InnerBackend for Aged {
    fn inner_backend(&self) -> &dyn StorageBackend {
        &*self.inner
    }
}

delegate_storage_backend! {
    for Aged;
    except [delete, list_meta, batch_delete];

    fn delete(&self, key: &str) -> Result<()> {
        if self.refused.iter().any(|k| k == key) {
            return Err(VykarError::Other(format!("403 append-only: {key}")));
        }
        self.inner.delete(key)
    }

    fn list_meta(&self, prefix: &str) -> Result<Vec<vykar_storage::ListedObject>> {
        let mtimes = self.mtimes.lock().unwrap();
        Ok(self
            .inner
            .list_sized(prefix)?
            .into_iter()
            .map(|mut o| {
                o.mtime = Some(mtimes.get(&o.key).copied().unwrap_or(self.now));
                o
            })
            .collect())
    }

    fn batch_delete(&self) -> Option<&dyn vykar_storage::BatchDelete> {
        None
    }
}

/// The head of generation `generation`, naming the base object
/// `gc_name(generation)`.
fn gc_head(generation: u64) -> IndexHead {
    let b = generation as u8;
    IndexHead {
        layout: HEAD_LAYOUT_VERSION,
        generation,
        features: Vec::new(),
        state_hash: [b; 32],
        entries: 1,
        base: Some(BaseRef {
            name: gc_name(b),
            size: 1,
            state_hash: [b; 32],
            entries: 1,
        }),
        segments: vec![],
        retired: vec![],
        sweep: SweepState::default(),
    }
}

/// Heads 1..=20, each naming its own object; objects 200 (unnamed, old) and
/// 201 (unnamed, young) and a foreign file. Everything is old except head
/// 20, head 2 and object 201.
fn gc_fixture(storage: &Aged) -> IndexHead {
    let crypto = plaintext();
    for g in 1..=20u64 {
        let head = gc_head(g);
        put_head(storage, &head, crypto.as_ref()).unwrap();
        storage.put(&object_key(&gc_name(g as u8)), b"x").unwrap();
        if g != 20 && g != 2 {
            storage.age(&head_key(g));
        }
        storage.age(&object_key(&gc_name(g as u8)));
    }
    for b in [200u8, 201] {
        storage.put(&object_key(&gc_name(b)), b"x").unwrap();
    }
    storage.age(&object_key(&gc_name(200)));
    storage.put(FOREIGN_KEY, b"x").unwrap();
    storage.age(FOREIGN_KEY);
    gc_head(20)
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

/// What the fixture keeps: the objects of heads 2 and 4..=20, object 201
/// and the foreign file.
fn remaining_after_gc() -> Vec<String> {
    let mut names: Vec<u8> = (4..=20).collect();
    names.extend([2, 201]);
    let mut keys = keys_of(&names);
    keys.push(FOREIGN_KEY.to_string());
    keys.sort();
    keys
}

/// The object garbage of the fixture: heads 1 and 3's objects and 200.
fn gc_garbage() -> Vec<String> {
    keys_of(&[1, 3, 200])
}

const GRACE: std::time::Duration = INDEX_GC_GRACE;

/// STORE-4: heads below latest − 16 that are old go, with the objects no
/// kept head names that are old.
#[test]
fn collect_index_garbage_keeps_recent_young_and_named() {
    let storage = Aged::new();
    let latest = gc_fixture(&storage);
    let stats = collect_index_garbage(&storage, plaintext().as_ref(), &latest, GRACE).unwrap();
    assert_eq!(stats.skipped, None);
    assert_eq!(stats.heads_deleted, 2);
    assert_eq!(stats.objects_deleted, 3);
    let mut want_heads: Vec<u64> = (4..=20).collect();
    want_heads.insert(0, 2);
    assert_eq!(list_head_generations(&storage).unwrap(), want_heads);
    assert_eq!(remaining(&storage), remaining_after_gc());

    // A second run finds nothing more.
    let again = collect_index_garbage(&storage, plaintext().as_ref(), &latest, GRACE).unwrap();
    assert_eq!((again.heads_deleted, again.objects_deleted), (0, 0));
}

/// The objects a kept head names as retired or as its sweep record stay.
#[test]
fn collect_index_garbage_keeps_retired_objects_and_records() {
    let storage = Aged::new();
    let crypto = plaintext();
    let mut latest = gc_fixture(&storage);
    latest.retired = vec![gc_name(200)];
    put_head(&storage, &latest, crypto.as_ref()).unwrap();
    let mut kept = gc_head(19);
    kept.sweep = SweepState {
        generation: 19,
        record: Some(crate::index::head::ObjectRef {
            name: gc_name(1),
            size: 1,
        }),
    };
    put_head(&storage, &kept, crypto.as_ref()).unwrap();
    storage.age(&head_key(19));
    let stats = collect_index_garbage(&storage, crypto.as_ref(), &latest, GRACE).unwrap();
    assert_eq!(stats.objects_deleted, 1, "only object 3");
    assert!(storage.exists(&object_key(&gc_name(200))).unwrap());
    assert!(storage.exists(&object_key(&gc_name(1))).unwrap());
    assert!(!storage.exists(&object_key(&gc_name(3))).unwrap());
}

/// STORE-5: without mtimes nothing is deleted.
#[test]
fn collect_index_garbage_without_mtimes_deletes_nothing() {
    let mem = MemoryBackend::new();
    let crypto = plaintext();
    for g in 1..=20u64 {
        put_head(&mem, &gc_head(g), crypto.as_ref()).unwrap();
        mem.put(&object_key(&gc_name(g as u8)), b"x").unwrap();
    }
    let stats = collect_index_garbage(&mem, crypto.as_ref(), &gc_head(20), GRACE).unwrap();
    assert_eq!(stats.skipped, Some("no mtimes"));
    assert_eq!((stats.heads_deleted, stats.objects_deleted), (0, 0));
    assert_eq!(list_head_generations(&mem).unwrap().len(), 20);
}

/// STORE-6: a kept head that does not decode stops the object part; the
/// heads part has run.
#[test]
fn an_unreadable_retained_head_keeps_every_object() {
    let storage = Aged::new();
    let latest = gc_fixture(&storage);
    storage.put(&head_key(5), b"garbage").unwrap();
    storage.age(&head_key(5));
    let stats = collect_index_garbage(&storage, plaintext().as_ref(), &latest, GRACE).unwrap();
    assert_eq!(stats.skipped, Some("unreadable retained head"));
    assert_eq!(stats.heads_deleted, 2);
    assert_eq!(stats.objects_deleted, 0);
    assert!(storage.exists(&object_key(&gc_name(200))).unwrap());
}

/// STORE-7: a refused head delete is logged; its objects still go.
#[test]
fn a_refused_head_delete_does_not_stop_the_run() {
    let mut storage = Aged::new();
    storage.refused = vec![head_key(1)];
    let latest = gc_fixture(&storage);
    let stats = collect_index_garbage(&storage, plaintext().as_ref(), &latest, GRACE).unwrap();
    assert_eq!(stats.heads_deleted, 1);
    assert!(storage.exists(&head_key(1)).unwrap());
    assert_eq!(stats.objects_deleted, 3);
    assert_eq!(remaining(&storage), remaining_after_gc());
}

#[test]
fn collect_index_garbage_deletes_objects_in_one_batch() {
    let aged = Arc::new(Aged::new());
    let latest = gc_fixture(&aged);
    let storage = BatchDeletes::new(aged, false);
    let stats = collect_index_garbage(&storage, plaintext().as_ref(), &latest, GRACE).unwrap();
    assert_eq!(stats.objects_deleted, 3);
    let batches = storage.batches.lock().unwrap().clone();
    assert_eq!(batches.len(), 1);
    let (mut keys, scope) = batches.into_iter().next().unwrap();
    keys.sort();
    assert_eq!(keys, gc_garbage());
    assert_eq!(scope, vykar_storage::BatchDeleteScope::KeysOnly);
    let deletes = storage.deletes.lock().unwrap().clone();
    assert!(
        deletes.iter().all(|k| k.starts_with(HEAD_PREFIX)),
        "{deletes:?}"
    );
    assert_eq!(remaining(&storage), remaining_after_gc());
}

#[test]
fn collect_index_garbage_falls_back_when_the_batch_fails() {
    let aged = Arc::new(Aged::new());
    let latest = gc_fixture(&aged);
    let storage = BatchDeletes::new(aged, true);
    let stats = collect_index_garbage(&storage, plaintext().as_ref(), &latest, GRACE).unwrap();
    assert_eq!(storage.batches.lock().unwrap().len(), 1);
    // The partial batch deleted the first key; its single delete is a no-op.
    assert_eq!(stats.objects_deleted, 3);
    let mut deletes: Vec<String> = storage
        .deletes
        .lock()
        .unwrap()
        .iter()
        .filter(|k| k.starts_with(INDEX_OBJECT_DIR))
        .cloned()
        .collect();
    deletes.sort();
    assert_eq!(deletes, gc_garbage());
    assert_eq!(remaining(&storage), remaining_after_gc());
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
    except [delete, batch_delete];

    fn delete(&self, key: &str) -> Result<()> {
        if key == self.key {
            return Err(VykarError::Other(format!("injected delete failure: {key}")));
        }
        self.inner.delete(key)
    }

    fn batch_delete(&self) -> Option<&dyn vykar_storage::BatchDelete> {
        None
    }
}

/// A failed single delete is logged, not counted, and does not stop the
/// others.
#[test]
fn collect_index_garbage_skips_a_failed_delete() {
    let aged = Arc::new(Aged::new());
    let latest = gc_fixture(&aged);
    let failing = object_key(&gc_name(3));
    let storage = FailOneDelete {
        inner: aged,
        key: failing.clone(),
    };
    let stats = collect_index_garbage(&storage, plaintext().as_ref(), &latest, GRACE).unwrap();
    assert_eq!(stats.objects_deleted, 2);
    let mut expected = remaining_after_gc();
    expected.push(failing);
    expected.sort();
    assert_eq!(remaining(&storage), expected);
}

#[test]
fn collect_index_garbage_with_nothing_to_delete_sends_no_request() {
    let aged = Arc::new(Aged::new());
    let crypto = plaintext();
    let head = gc_head(1);
    put_head(aged.as_ref(), &head, crypto.as_ref()).unwrap();
    aged.put(&object_key(&gc_name(1)), b"x").unwrap();
    aged.age(&object_key(&gc_name(1)));
    let storage = BatchDeletes::new(aged, false);
    let stats = collect_index_garbage(&storage, crypto.as_ref(), &head, GRACE).unwrap();
    assert_eq!((stats.heads_deleted, stats.objects_deleted), (0, 0));
    assert!(storage.batches.lock().unwrap().is_empty());
    assert!(storage.deletes.lock().unwrap().is_empty());
}

/// STORE-8: a backup segment that changes existing entries is refused; the
/// same segment labelled maintenance decodes.
#[test]
fn a_backup_segment_with_deltas_is_refused() {
    let crypto = plaintext();
    let seg = Segment {
        prev_state_hash: [1; 32],
        prev_entries: 3,
        deltas: vec![crate::index::segment::RefcountDelta {
            position: 0,
            delta: -1,
        }],
        relocations: Vec::new(),
        new_entries: Vec::new(),
    };
    let obj = pack_segment(&seg, crypto.as_ref()).unwrap();
    let seg_ref = |kind| SegmentRef {
        name: obj.name,
        size: obj.size(),
        kind,
        state_hash_after: [2; 32],
        entries_after: 3,
    };
    let err = decode_segment(&seg_ref(SegmentKind::Backup), &obj.bytes, crypto.as_ref())
        .unwrap_err()
        .to_string();
    assert!(
        err.contains("a backup segment changes existing entries"),
        "{err}"
    );
    assert_eq!(
        decode_segment(
            &seg_ref(SegmentKind::Maintenance),
            &obj.bytes,
            crypto.as_ref()
        )
        .unwrap(),
        seg
    );
}

/// Sweep records round-trip through their storage encoding, checked
/// against the head's reference.
#[test]
fn sweep_record_round_trips_through_storage() {
    for crypto in [plaintext(), encrypted()] {
        let rec = SweepRecord {
            version: crate::index::sweep_record::SWEEP_RECORD_VERSION,
            generation: 3,
            time: chrono::DateTime::from_timestamp(1_700_000_000, 0).unwrap(),
            snapshots: vec![serde_bytes::ByteBuf::from(vec![1; 32])],
            tree_chunks: 1,
            entries_live: 2,
            bytes_live: 3,
            entries_removed: 4,
            bytes_removed: 5,
            references: 6,
            referenced_bytes: 7,
        };
        let obj = pack_sweep_record(&rec, crypto.as_ref()).unwrap();
        let r = ObjectRef {
            name: obj.name,
            size: obj.size(),
        };
        assert_eq!(
            decode_sweep_record(&r, &obj.bytes, crypto.as_ref()).unwrap(),
            rec
        );
        let mut wrong = r;
        wrong.size += 1;
        assert!(decode_sweep_record(&wrong, &obj.bytes, crypto.as_ref()).is_err());
    }
}

/// A recorded head state that the objects do not produce is rejected before
/// anything is installed: the cache dir keeps what it held. Covers both the
/// segment branch and the base-only branch of the final state check.
#[test]
fn wrong_head_state_leaves_the_cache_dir_alone() {
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
        let path = cache_dir.path().to_path_buf();
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
        assert!(table_states(&path).is_empty(), "nothing installed");

        let other = write_table(&path, &index_with(500..510, 0));
        let before = std::fs::read(&other).unwrap();
        let err = sync_err();
        assert!(err.contains("differs from the head"), "{err}");
        assert_eq!(std::fs::read(&other).unwrap(), before, "table unchanged");
        assert_eq!(table_states(&path).len(), 1);
    }
}

/// Concurrent syncs into one cache dir: two readers on different heads and
/// two on the same head. Every sync succeeds with its own head's state, and
/// each state is published once (one file, never replaced while mapped).
#[test]
fn concurrent_syncs_over_one_cache_dir_succeed() {
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
    let path = cache_dir.path().to_path_buf();

    std::thread::scope(|scope| {
        for (head, want) in [(&h1, &s1), (&h2, &s2), (&h2, &s2)] {
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
                    assert_eq!(entries(&*synced.table), entries(want));
                }
            });
        }
    });
    let mut want = vec![h1.state_hash, h2.state_hash];
    want.sort_unstable();
    assert_eq!(table_states(&path), want);
}

/// A downloaded base with no segments to apply is the result as mapped, and
/// is published in the cache dir while mapped.
#[test]
fn sync_downloads_a_base_without_segments_and_persists_it() {
    let crypto = plaintext();
    let mem: Arc<dyn StorageBackend> = Arc::new(MemoryBackend::new());
    let mut w = Writer::new(Arc::clone(&mem), Arc::clone(&crypto));
    let s1 = index_with(0..120, 0);
    w.commit(&s1);
    w.fold();
    let cache_dir = tempfile::tempdir().unwrap();
    let path = cache_dir.path().to_path_buf();

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
    let file = table::table_path(&path, &w.head.state_hash);
    assert_eq!(synced.table.file_path(), Some(file.as_path()));
    let at_path = MmapFullIndexCache::open_verified(&file).expect("table in the cache dir");
    assert_eq!(
        (at_path.state_hash(), u64::from(at_path.entry_count())),
        (w.head.state_hash, w.head.entries)
    );
    assert_eq!(entries(&*synced.table), entries(&s1));
}

/// A reader on an older head publishes its state beside the newer one
/// instead of over it; a later sync to the newer head finds that table and
/// downloads nothing.
#[test]
fn a_late_reader_does_not_displace_the_newer_table() {
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
    let path = cache_dir.path().to_path_buf();

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
    drop(read(&h1));
    let mut want = vec![h1.state_hash, h2.state_hash];
    want.sort_unstable();
    assert_eq!(table_states(&path), want, "both states coexist");

    let synced = sync(&log, crypto.as_ref(), h2, Some(&path), None, true).unwrap();
    assert_eq!(synced.source, SyncSource::UpToDate);
    assert_eq!(index_d_gets(&log.take()), 0);
    assert_eq!(entries(&*synced.table), entries(&s2));
}

/// Garbage under the head's table name is replaced by the synced table, and
/// the next sync finds it (R1: a damaged table under a state name heals).
#[test]
fn a_damaged_table_under_the_state_name_heals() {
    let crypto = plaintext();
    let mem: Arc<dyn StorageBackend> = Arc::new(MemoryBackend::new());
    let log = GetLog::new(Arc::clone(&mem));
    let mut w = Writer::new(Arc::clone(&mem), Arc::clone(&crypto));
    let s1 = index_with(0..60, 0);
    w.commit(&s1);
    let cache_dir = tempfile::tempdir().unwrap();
    let path = cache_dir.path().to_path_buf();
    let file = table::table_path(&path, &w.head.state_hash);
    std::fs::write(&file, b"not a table").unwrap();

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
    assert_eq!(synced.table.file_path(), Some(file.as_path()));
    drop(synced);
    assert!(
        ChunkTable::open_file(&file).is_some(),
        "the file is valid now"
    );
    log.take();

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
    assert_eq!(index_d_gets(&log.take()), 0);
    assert_eq!(entries(&*synced.table), entries(&s1));
}

#[test]
fn classify_head_tells_damage_from_newer_heads() {
    for crypto in [plaintext(), encrypted()] {
        let c = crypto.as_ref();
        let initial = IndexHead::initial();
        let packed = pack_head(&initial, c).unwrap();
        assert!(matches!(classify_head(1, &packed, c), StoredHead::Head(h) if h == initial));

        let mut flipped = packed.clone();
        *flipped.last_mut().unwrap() ^= 1;
        let nil =
            pack_object_with_context(ObjectType::IndexHead, HEAD_CONTEXT, b"\xc0", c).unwrap();
        let half = packed[..packed.len() / 2].to_vec();
        let mut damaged_cases = vec![vec![0u8; 10], flipped, nil, half];
        // A head under v4/v5's context does not authenticate under
        // index.h's (a plaintext repository authenticates nothing).
        if c.is_encrypting() {
            damaged_cases.push(
                pack_object_with_context(
                    ObjectType::IndexHead,
                    b"index",
                    &initial.encode().unwrap(),
                    c,
                )
                .unwrap(),
            );
        }
        for damaged in damaged_cases {
            assert!(
                matches!(classify_head(1, &damaged, c), StoredHead::Unreadable(_)),
                "{:?}",
                classify_head(1, &damaged, c)
            );
        }

        let layout3 = pack_head(
            &IndexHead {
                layout: 3,
                ..IndexHead::initial()
            },
            c,
        )
        .unwrap();
        assert!(matches!(
            classify_head(1, &layout3, c),
            StoredHead::UnknownLayout(3)
        ));
        // A positional (layout 1) body sealed under the new context.
        let positional =
            rmp_serde::to_vec(&(1u32, serde_bytes::Bytes::new(&[0; 32]), 0u64)).unwrap();
        let positional =
            pack_object_with_context(ObjectType::IndexHead, HEAD_CONTEXT, &positional, c).unwrap();
        assert!(matches!(
            classify_head(1, &positional, c),
            StoredHead::UnknownLayout(1)
        ));

        // HEAD-4.
        let featured = pack_head(
            &IndexHead {
                features: vec!["x".into()],
                ..IndexHead::initial()
            },
            c,
        )
        .unwrap();
        match classify_head(1, &featured, c) {
            StoredHead::UnknownFeatures(names) => assert_eq!(names, vec!["x".to_string()]),
            other => panic!("expected UnknownFeatures, got {other:?}"),
        }
        let err = classify_head(1, &featured, c)
            .into_head()
            .unwrap_err()
            .to_string();
        assert!(err.contains("newer vykar"), "{err}");

        let unknown = [200u8, 1, 2, 3];
        assert!(matches!(
            classify_head(1, &unknown, c),
            StoredHead::UnknownType(200)
        ));

        let err = classify_head(1, &layout3, c)
            .into_head()
            .unwrap_err()
            .to_string();
        assert!(err.contains("unsupported index head layout 3"), "{err}");
        assert!(matches!(
            classify_head(1, &unknown, c).into_head(),
            Err(VykarError::UnknownObjectType(200))
        ));
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
