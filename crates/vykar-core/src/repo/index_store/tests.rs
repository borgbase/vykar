use std::sync::{Arc, Mutex};

use super::*;
use crate::index::dedup_cache::build_full_index_cache_to_path;
use crate::index::head::empty_state_hash;
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
        IndexHead::empty(),
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
    assert_eq!(entries(&*synced.table), entries(&latest));
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
    except [get];

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
