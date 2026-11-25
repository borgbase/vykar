//! The local tree-chunk cache: backup and readers fill it, item-stream
//! readers read it first, check never touches it, and prune, delete and
//! backup drop what the index no longer holds.

use std::collections::HashSet;
use std::io::Read;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use vykar_storage::local_backend::LocalBackend;
use vykar_storage::{InnerBackend, StorageBackend, delegate_storage_backend};
use vykar_types::chunk_id::ChunkId;
use vykar_types::error::{Result, VykarError};
use vykar_types::pack_id::PackId;

use crate::commands;
use crate::commands::list::LazyItemStream;
use crate::commands::restore::RestoreOptions;
use crate::compress::Compression;
use crate::config::{EncryptionModeConfig, RetentionConfig, VykarConfig};
use crate::repo::control::CancelToken;
use crate::repo::pack::PackType;
use crate::repo::tree_cache::TreeChunkCache;
use crate::repo::{
    ChunkerParams, EncryptionMode, OpenOptions, Repo, TreeChunkSource, WriteSessionOptions,
};
use crate::testutil::{PutLog, RecordingBackend, init_test_environment};

use super::helpers::{backup_single_source_with, make_test_config, source_entry};

const PASS: &str = "tree-cache-pass";

type Loc = (ChunkId, PackId, u64, u32);

fn modes() -> [EncryptionMode; 2] {
    [EncryptionMode::None, EncryptionMode::Aes256Gcm]
}

/// The chunk IDs of the entries under `root`.
fn cached_ids(root: &Path) -> HashSet<ChunkId> {
    let mut ids = HashSet::new();
    let Ok(shards) = std::fs::read_dir(root) else {
        return ids;
    };
    for shard in shards {
        for entry in std::fs::read_dir(shard.unwrap().path()).unwrap() {
            let name = entry.unwrap().file_name().into_string().unwrap();
            if let Ok(bytes) = hex::decode(&name)
                && let Ok(bytes) = <[u8; 32]>::try_from(bytes)
            {
                ids.insert(ChunkId::from_bytes(bytes));
            }
        }
    }
    ids
}

fn cache_of(repo: &Repo) -> &TreeChunkCache {
    repo.tree_cache()
        .expect("test repositories have a cache dir")
}

// ---------------------------------------------------------------------------
// Reads over explicit chunk lists (in-memory repository)
// ---------------------------------------------------------------------------

fn memory_repo(mode: EncryptionMode) -> (Repo, crate::repo::WriteSession, PutLog) {
    init_test_environment();
    let (backend, log) = RecordingBackend::new();
    let pass = match mode {
        EncryptionMode::None => None,
        _ => Some(PASS),
    };
    let repo = Repo::init(
        Box::new(backend),
        mode,
        ChunkerParams::default(),
        pass,
        None,
        None,
    )
    .expect("init");
    let ws = repo.begin_write_session(&WriteSessionOptions::default());
    (repo, ws, log)
}

/// Store `n` distinct tree chunks (one pack) and return their concatenation
/// and their locations, in store order.
fn store_tree_chunks(
    repo: &mut Repo,
    ws: &mut crate::repo::WriteSession,
    n: usize,
) -> (Vec<u8>, Vec<Loc>) {
    let mut stream = Vec::new();
    let mut ids = Vec::new();
    for i in 0..n {
        let data = format!("tree-cache-chunk-{i:04}-{}", "y".repeat(i * 13)).into_bytes();
        ids.push(
            ws.store_chunk(repo, &data, Compression::None, PackType::Tree)
                .unwrap()
                .0,
        );
        stream.extend_from_slice(&data);
    }
    crate::testutil::save_and_resume(repo, ws);
    let locs = ids
        .iter()
        .map(|id| {
            let e = repo.chunk_index().get(id).unwrap();
            (*id, e.pack_id, e.pack_offset, e.stored_size)
        })
        .collect();
    (stream, locs)
}

fn read(repo: &Repo, locs: &[Loc], source: TreeChunkSource) -> Vec<u8> {
    let mut out = Vec::new();
    repo.read_tree_chunks_into(locs, &mut out, &CancelToken::new(), source)
        .unwrap();
    out
}

/// The stored bytes of `loc` in its pack (one range read).
fn pack_bytes(repo: &Repo, loc: &Loc) -> Vec<u8> {
    repo.storage()
        .get_range(&loc.1.storage_key(), loc.2, u64::from(loc.3))
        .unwrap()
        .unwrap()
}

fn clear(cache: &TreeChunkCache) {
    std::fs::remove_dir_all(cache.root()).unwrap();
}

#[test]
fn store_caches_new_tree_chunks_as_stored_bytes_only() {
    for mode in modes() {
        let (mut repo, mut ws, _log) = memory_repo(mode.clone());
        let (_, locs) = store_tree_chunks(&mut repo, &mut ws, 3);
        let (data_id, _, _) = ws
            .store_chunk(&repo, b"a data chunk", Compression::None, PackType::Data)
            .unwrap();
        crate::testutil::save_and_resume(&mut repo, &mut ws);
        let cache = cache_of(&repo);
        for loc in &locs {
            assert_eq!(
                cache.get(&loc.0).unwrap(),
                pack_bytes(&repo, loc),
                "{mode:?}: entry differs from the pack"
            );
        }
        assert!(!cache.contains(&data_id), "{mode:?}: data chunk cached");
    }
}

#[test]
fn cached_item_stream_reads_no_tree_pack() {
    for mode in modes() {
        let (mut repo, mut ws, log) = memory_repo(mode.clone());
        let (stream, locs) = store_tree_chunks(&mut repo, &mut ws, 5);
        log.reset_get_range_count();
        assert_eq!(read(&repo, &locs, TreeChunkSource::CacheFirst), stream);
        assert_eq!(log.get_range_count(), 0, "{mode:?}");
        assert_eq!(read(&repo, &locs, TreeChunkSource::Storage), stream);
        assert!(log.get_range_count() > 0, "{mode:?}");
    }
}

#[test]
fn a_storage_read_neither_reads_nor_fills_the_cache() {
    let (mut repo, mut ws, log) = memory_repo(EncryptionMode::Aes256Gcm);
    let (stream, locs) = store_tree_chunks(&mut repo, &mut ws, 4);
    clear(cache_of(&repo));
    log.reset_get_range_count();
    assert_eq!(read(&repo, &locs, TreeChunkSource::Storage), stream);
    assert_eq!(log.get_range_count(), 1);
    assert!(cached_ids(cache_of(&repo).root()).is_empty());
}

#[test]
fn a_miss_downloads_and_fills() {
    for mode in modes() {
        let (mut repo, mut ws, log) = memory_repo(mode.clone());
        let (stream, locs) = store_tree_chunks(&mut repo, &mut ws, 5);
        clear(cache_of(&repo));
        log.reset_get_range_count();
        assert_eq!(read(&repo, &locs, TreeChunkSource::CacheFirst), stream);
        assert!(log.get_range_count() > 0, "{mode:?}");
        for loc in &locs {
            assert!(cache_of(&repo).contains(&loc.0), "{mode:?}: not filled");
        }
        log.reset_get_range_count();
        assert_eq!(read(&repo, &locs, TreeChunkSource::CacheFirst), stream);
        assert_eq!(log.get_range_count(), 0, "{mode:?}");
    }
}

#[test]
fn a_corrupt_entry_is_replaced_from_storage() {
    for mode in modes() {
        let (mut repo, mut ws, log) = memory_repo(mode.clone());
        let (stream, locs) = store_tree_chunks(&mut repo, &mut ws, 5);
        let path = cache_of(&repo).entry_path(&locs[2].0);
        let mut bytes = std::fs::read(&path).unwrap();
        *bytes.last_mut().unwrap() ^= 0x01;
        std::fs::write(&path, bytes).unwrap();

        log.reset_get_range_count();
        assert_eq!(read(&repo, &locs, TreeChunkSource::CacheFirst), stream);
        assert_eq!(
            log.get_range_count(),
            1,
            "{mode:?}: one read for the one miss"
        );
        let stored = pack_bytes(&repo, &locs[2]);
        assert_eq!(cache_of(&repo).get(&locs[2].0).unwrap(), stored, "{mode:?}");

        // An empty file is what a write without fsync leaves after power loss.
        std::fs::write(&path, b"").unwrap();
        log.reset_get_range_count();
        assert_eq!(read(&repo, &locs, TreeChunkSource::CacheFirst), stream);
        assert_eq!(log.get_range_count(), 1, "{mode:?}: empty entry is a miss");
        assert_eq!(cache_of(&repo).get(&locs[2].0).unwrap(), stored, "{mode:?}");
    }
}

#[test]
fn an_entry_under_the_wrong_name_is_rejected() {
    for mode in modes() {
        let (mut repo, mut ws, log) = memory_repo(mode.clone());
        let (stream, locs) = store_tree_chunks(&mut repo, &mut ws, 3);
        let cache = cache_of(&repo);
        std::fs::copy(cache.entry_path(&locs[0].0), cache.entry_path(&locs[1].0)).unwrap();

        log.reset_get_range_count();
        assert_eq!(read(&repo, &locs, TreeChunkSource::CacheFirst), stream);
        assert_eq!(log.get_range_count(), 1, "{mode:?}");
    }
}

#[test]
fn a_pointer_missing_from_the_index_fails_even_when_cached() {
    let (mut repo, mut ws, _log) = memory_repo(EncryptionMode::None);
    let (_, locs) = store_tree_chunks(&mut repo, &mut ws, 3);
    let ids: Vec<ChunkId> = locs.iter().map(|l| l.0).collect();
    repo.unchecked_writer().chunk_index_mut().remove(&ids[1]);
    assert!(cache_of(&repo).contains(&ids[1]));
    let err = commands::list::load_item_stream_from_ptrs(&mut repo, &ids, &CancelToken::new())
        .unwrap_err();
    assert!(
        matches!(err, VykarError::ChunkNotInIndex(id) if id == ids[1]),
        "got {err:?}"
    );
}

#[test]
fn read_tree_chunks_split_bounds_match_per_chunk_reads() {
    for mode in modes() {
        let (mut repo, mut ws, _log) = memory_repo(mode.clone());
        // 20 tree chunks in 3 packs.
        let mut locs: Vec<Loc> = Vec::new();
        for pack in 0..3 {
            for i in 0..7 {
                if locs.len() == 20 {
                    break;
                }
                let data = format!("split-{pack}-{i:02}-{}", "q".repeat(i * 31)).into_bytes();
                let id = ws
                    .store_chunk(&repo, &data, Compression::None, PackType::Tree)
                    .unwrap()
                    .0;
                locs.push((id, PackId::from_bytes([0; 32]), 0, 0));
            }
            crate::testutil::save_and_resume(&mut repo, &mut ws);
        }
        for loc in &mut locs {
            let e = repo.chunk_index().get(&loc.0).unwrap();
            *loc = (loc.0, e.pack_id, e.pack_offset, e.stored_size);
        }
        let packs: HashSet<PackId> = locs.iter().map(|l| l.1).collect();
        assert_eq!(packs.len(), 3, "{mode:?}");
        // Interleave the packs and repeat one chunk.
        let mut order: Vec<Loc> = (0..7)
            .flat_map(|i| [i, 7 + i, 14 + i])
            .filter_map(|i| locs.get(i).copied())
            .collect();
        order.insert(5, locs[3]);
        let expected: Vec<Vec<u8>> = order
            .iter()
            .map(|l| repo.read_chunk_at(&l.0, &l.1, l.2, l.3).unwrap())
            .collect();

        let cache = cache_of(&repo);
        for (i, loc) in locs.iter().enumerate() {
            if i % 2 == 0 {
                cache.remove(&loc.0);
            }
        }
        for source in [TreeChunkSource::CacheFirst, TreeChunkSource::Storage] {
            let mut out = b"prefix".to_vec();
            let mut ends = vec![99];
            repo.read_tree_chunks_split(&order, &mut out, &mut ends, &CancelToken::new(), source)
                .unwrap();
            assert_eq!(ends.len(), order.len(), "{mode:?} {source:?}");
            let mut start = b"prefix".len();
            for (i, &end) in ends.iter().enumerate() {
                assert_eq!(
                    out[start..end],
                    expected[i][..],
                    "{mode:?} {source:?} chunk {i}"
                );
                start = end;
            }
            assert_eq!(start, out.len());
        }
    }
}

// ---------------------------------------------------------------------------
// Lazy stream (restore)
// ---------------------------------------------------------------------------

fn read_lazy(repo: &Repo, locs: &[Loc], log: &PutLog) -> (Vec<u8>, u64) {
    let shutdown = CancelToken::new();
    let mut stream =
        LazyItemStream::for_test_with_cache(repo, locs.to_vec(), false, 1 << 20, &shutdown);
    log.reset_get_range_count();
    let mut out = Vec::new();
    stream.read_to_end(&mut out).unwrap();
    (out, log.get_range_count())
}

#[test]
fn lazy_stream_reads_cached_chunks_without_range_reads() {
    for mode in modes() {
        let (mut repo, mut ws, log) = memory_repo(mode.clone());
        let (stream, locs) = store_tree_chunks(&mut repo, &mut ws, 6);
        let (out, reads) = read_lazy(&repo, &locs, &log);
        assert_eq!(out, stream, "{mode:?}");
        assert_eq!(reads, 0, "{mode:?}");
    }
}

#[test]
fn lazy_stream_mixes_cached_and_uncached_chunks() {
    for mode in modes() {
        let (mut repo, mut ws, log) = memory_repo(mode.clone());
        let (stream, locs) = store_tree_chunks(&mut repo, &mut ws, 11);
        let cache = cache_of(&repo);
        // Uncached: 0, 3, 6, 9 and the run 10..; chunks are in one pack in
        // stream order, so each maximal uncached run is one range read.
        let uncached: Vec<usize> = (0..locs.len()).filter(|i| i % 3 == 0 || *i == 10).collect();
        for &i in &uncached {
            cache.remove(&locs[i].0);
        }
        let runs = uncached
            .iter()
            .enumerate()
            .filter(|&(n, &i)| n == 0 || uncached[n - 1] + 1 != i)
            .count();
        assert_eq!(runs, 4);
        let (out, reads) = read_lazy(&repo, &locs, &log);
        assert_eq!(out, stream, "{mode:?}");
        assert_eq!(reads, runs as u64, "{mode:?}");
    }
}

#[test]
fn lazy_stream_fills_the_cache_on_miss() {
    let (mut repo, mut ws, log) = memory_repo(EncryptionMode::Aes256Gcm);
    let (stream, locs) = store_tree_chunks(&mut repo, &mut ws, 6);
    clear(cache_of(&repo));
    let (out, reads) = read_lazy(&repo, &locs, &log);
    assert_eq!(out, stream);
    assert_eq!(reads, 1);
    let expected: HashSet<ChunkId> = locs.iter().map(|l| l.0).collect();
    assert_eq!(cached_ids(cache_of(&repo).root()), expected);
    let (out, reads) = read_lazy(&repo, &locs, &log);
    assert_eq!(out, stream);
    assert_eq!(reads, 0);
}

// ---------------------------------------------------------------------------
// Commands on a local repository
// ---------------------------------------------------------------------------

/// The keys of every range read a handle issued.
type Reads = Arc<Mutex<Vec<String>>>;

/// `LocalBackend` that records the key of every range read.
struct CountingBackend {
    inner: LocalBackend,
    reads: Reads,
}

impl InnerBackend for CountingBackend {
    fn inner_backend(&self) -> &dyn StorageBackend {
        &self.inner
    }
}

delegate_storage_backend! {
    for CountingBackend;
    except [get_range, get_range_into, open_range];

    fn get_range(&self, key: &str, offset: u64, length: u64) -> Result<Option<Vec<u8>>> {
        self.reads.lock().unwrap().push(key.to_string());
        self.inner.get_range(key, offset, length)
    }

    fn get_range_into(&self, key: &str, offset: u64, length: u64, buf: &mut Vec<u8>) -> Result<bool> {
        self.reads.lock().unwrap().push(key.to_string());
        self.inner.get_range_into(key, offset, length, buf)
    }

    fn open_range<'a>(
        &self,
        key: &str,
        offset: u64,
        length: u64,
        buf: &'a mut Vec<u8>,
    ) -> Result<Option<Box<dyn vykar_storage::RangeReader + 'a>>> {
        vykar_storage::buffered_range(self, key, offset, length, buf)
    }
}

/// One host backing up one source into a local repository.
struct Host {
    tmp: tempfile::TempDir,
    repo_dir: PathBuf,
    source_dir: PathBuf,
    config: VykarConfig,
    pass: Option<&'static str>,
}

impl Host {
    fn new(encrypted: bool) -> Self {
        let tmp = tempfile::tempdir().unwrap();
        let repo_dir = tmp.path().join("repo");
        let source_dir = tmp.path().join("source");
        std::fs::create_dir_all(&repo_dir).unwrap();
        std::fs::create_dir_all(&source_dir).unwrap();
        let mut config = make_test_config(&repo_dir);
        if encrypted {
            config.encryption.mode = EncryptionModeConfig::Aes256Gcm;
        }
        let pass = encrypted.then_some(PASS);
        commands::init::run(&config, pass).unwrap();
        config.retention = RetentionConfig {
            keep_last: Some(2),
            ..RetentionConfig::default()
        };
        Self {
            tmp,
            repo_dir,
            source_dir,
            config,
            pass,
        }
    }

    /// Add a file named after the snapshot, so every listing differs, and
    /// back up.
    fn backup(&self, name: &str) {
        std::fs::write(self.source_dir.join(format!("{name}.txt")), name).unwrap();
        backup_single_source_with(&self.config, self.pass, &self.source_dir, "src", name);
        std::thread::sleep(Duration::from_millis(2));
    }

    /// A handle that records range reads, on this host's cache dir (or
    /// `cache_dir`).
    fn open_with_cache(&self, cache_dir: Option<PathBuf>) -> (Repo, Reads) {
        let reads = Reads::default();
        let backend = CountingBackend {
            inner: LocalBackend::new(self.repo_dir.to_str().unwrap()).unwrap(),
            reads: Arc::clone(&reads),
        };
        let repo = Repo::open(
            Box::new(backend),
            self.pass,
            cache_dir,
            OpenOptions::new().with_index(),
        )
        .unwrap();
        (repo, reads)
    }

    fn open(&self) -> (Repo, Reads) {
        self.open_with_cache(None)
    }

    fn item_ptrs(&self, name: &str) -> Vec<ChunkId> {
        let (repo, _) = self.open();
        commands::list::load_snapshot_meta(&repo, name)
            .unwrap()
            .item_ptrs
    }

    fn cache_root(&self) -> PathBuf {
        cache_of(&self.open().0).root().to_path_buf()
    }

    fn cached(&self) -> HashSet<ChunkId> {
        cached_ids(&self.cache_root())
    }

    fn sources(&self) -> Vec<crate::config::SourceEntry> {
        vec![source_entry(&self.source_dir, "src")]
    }
}

/// The storage keys of the packs that hold `ids`, by the index of `repo`.
fn pack_keys(repo: &Repo, ids: &[ChunkId]) -> HashSet<String> {
    ids.iter()
        .map(|id| repo.chunk_index().get(id).unwrap().pack_id.storage_key())
        .collect()
}

/// Range reads against `keys`.
fn reads_of(reads: &Reads, keys: &HashSet<String>) -> usize {
    reads
        .lock()
        .unwrap()
        .iter()
        .filter(|k| keys.contains(*k))
        .count()
}

/// Range reads against any pack.
fn pack_reads(reads: &Reads) -> usize {
    reads
        .lock()
        .unwrap()
        .iter()
        .filter(|k| k.starts_with("packs/"))
        .count()
}

fn set<'a>(lists: impl IntoIterator<Item = &'a Vec<ChunkId>>) -> HashSet<ChunkId> {
    lists.into_iter().flatten().copied().collect()
}

#[test]
fn backup_writes_new_tree_chunks_to_the_cache() {
    for encrypted in [false, true] {
        let host = Host::new(encrypted);
        host.backup("s1");
        let ptrs = host.item_ptrs("s1");
        assert_eq!(host.cached(), set([&ptrs]), "encrypted={encrypted}");
        let (repo, _) = host.open();
        for id in &ptrs {
            let e = repo.chunk_index().get(id).unwrap();
            let pack = std::fs::read(host.repo_dir.join(e.pack_id.storage_key())).unwrap();
            let stored = &pack[e.pack_offset as usize..][..e.stored_size as usize];
            assert_eq!(cache_of(&repo).get(id).unwrap(), stored);
        }
    }
}

#[test]
fn another_handle_reads_what_backup_cached() {
    let host = Host::new(true);
    host.backup("s1");
    let (mut repo, reads) = host.open();
    let ptrs = commands::list::load_snapshot_meta(&repo, "s1")
        .unwrap()
        .item_ptrs;
    reads.lock().unwrap().clear();
    let items = commands::list::load_snapshot_items(&mut repo, "s1", &CancelToken::new()).unwrap();
    assert!(items.iter().any(|i| i.path.ends_with("s1.txt")));
    assert_eq!(reads_of(&reads, &pack_keys(&repo, &ptrs)), 0);
}

#[test]
fn check_reads_tree_chunks_from_storage() {
    let host = Host::new(false);
    host.backup("s1");
    // Corrupt the stored listing; the pack stays.
    let (repo, _) = host.open();
    let id = host.item_ptrs("s1")[0];
    let e = repo.chunk_index().get(&id).unwrap();
    drop(repo);
    let path = host.repo_dir.join(e.pack_id.storage_key());
    let mut pack = std::fs::read(&path).unwrap();
    pack[e.pack_offset as usize + e.stored_size as usize / 2] ^= 0x01;
    std::fs::write(&path, pack).unwrap();

    let result = commands::check::run(&host.config, None, false, false).unwrap();
    let messages: Vec<String> = result.errors.iter().map(|e| e.message.clone()).collect();
    assert!(
        messages.iter().any(|m| m.contains("load item stream")),
        "check must read the corrupt listing from storage: {messages:?}"
    );
    // Readers that do not verify the repository use the good cached copy.
    let items = commands::list::list_snapshot_items(&host.config, None, "s1").unwrap();
    assert!(items.iter().any(|i| i.path.ends_with("s1.txt")));

    // Check does not fill the cache either.
    std::fs::remove_dir_all(host.cache_root()).unwrap();
    commands::check::run(&host.config, None, false, false).unwrap();
    assert!(host.cached().is_empty());
}

fn compact(config: &crate::config::VykarConfig, pass: Option<&str>) {
    commands::compact::run(config, pass, 10.0, None, false, &CancelToken::new()).unwrap();
}

/// CP-5: prune keeps the cache (it reads nothing); the compact after it
/// drops the tree chunks only the pruned snapshot used, reading the live
/// listings from the cache.
#[test]
fn compact_after_prune_drops_the_pruned_snapshots_unique_tree_chunks() {
    for encrypted in [false, true] {
        let host = Host::new(encrypted);
        for name in ["s1", "s2", "s3"] {
            host.backup(name);
        }
        let [p1, p2, p3] = ["s1", "s2", "s3"].map(|n| host.item_ptrs(n));
        let live = set([&p2, &p3]);
        let unique: Vec<ChunkId> = p1.iter().filter(|id| !live.contains(id)).copied().collect();
        assert!(!unique.is_empty(), "s1 must have tree chunks of its own");

        let before = host.cached();
        let (mut repo, reads) = host.open();
        reads.lock().unwrap().clear();
        let (stats, _) = commands::prune::run_on_repo(
            &mut repo,
            &host.config,
            false,
            false,
            &host.sources(),
            &[],
            &CancelToken::new(),
        )
        .unwrap();
        assert_eq!(stats.pruned, 1, "encrypted={encrypted}");
        assert!(stats.warnings.is_empty(), "{:?}", stats.warnings);
        assert_eq!(pack_reads(&reads), 0, "encrypted={encrypted}");
        drop(repo);
        assert_eq!(host.cached(), before, "prune keeps the cache");

        compact(&host.config, host.pass);
        let cached = host.cached();
        assert!(
            unique.iter().all(|id| !cached.contains(id)),
            "encrypted={encrypted}"
        );
        assert!(live.is_subset(&cached), "encrypted={encrypted}");
    }
}

#[test]
fn prune_with_nothing_to_prune_keeps_the_cache() {
    let host = Host::new(false);
    host.backup("s1");
    host.backup("s2");
    // An entry the index does not hold: only a drop would remove it.
    let (repo, _) = host.open();
    let foreign = ChunkId::from_bytes([0x5C; 32]);
    cache_of(&repo).insert(&foreign, b"not a chunk of this repository");
    drop(repo);
    let before = host.cached();

    let (stats, _) = commands::prune::run(
        &host.config,
        None,
        false,
        false,
        &host.sources(),
        &[],
        &CancelToken::new(),
    )
    .unwrap();
    assert_eq!(stats.pruned, 0);
    assert_eq!(host.cached(), before);
    assert!(before.contains(&foreign));
}

#[test]
fn delete_keeps_the_cache_and_compact_drops_unreferenced_tree_chunks() {
    let host = Host::new(false);
    host.backup("s1");
    host.backup("s2");
    let (p1, p2) = (host.item_ptrs("s1"), host.item_ptrs("s2"));
    let before = host.cached();

    let (mut repo, reads) = host.open();
    reads.lock().unwrap().clear();
    let result =
        commands::delete::run_on_repo(&mut repo, &["s1"], false, &CancelToken::new()).unwrap();
    assert!(result.warnings.is_empty(), "{:?}", result.warnings);
    assert_eq!(pack_reads(&reads), 0, "delete reads no pack");
    drop(repo);
    assert_eq!(host.cached(), before, "delete keeps the cache");

    compact(&host.config, host.pass);
    let cached = host.cached();
    let live = set([&p2]);
    assert!(
        p1.iter()
            .filter(|id| !live.contains(id))
            .all(|id| !cached.contains(id))
    );
    assert!(live.is_subset(&cached));
}

#[test]
fn delete_dry_run_keeps_the_cache() {
    let host = Host::new(false);
    host.backup("s1");
    host.backup("s2");
    let before = host.cached();
    let (mut repo, reads) = host.open();
    reads.lock().unwrap().clear();
    let result =
        commands::delete::run_on_repo(&mut repo, &["s1"], true, &CancelToken::new()).unwrap();
    assert_eq!(result.stats.len(), 1);
    assert_eq!(
        pack_reads(&reads),
        0,
        "the dry run's listing came from the cache"
    );
    drop(repo);
    assert_eq!(host.cached(), before);
}

/// A backup drops nothing from the cache, even after another host pruned
/// and compacted; this host's next compact does.
#[test]
fn only_compact_drops_chunks_another_host_pruned() {
    let host = Host::new(false);
    host.backup("s1");
    host.backup("s2");
    let p1 = host.item_ptrs("s1");

    // Another host (its own cache dir) prunes s1.
    let other_cache = host.tmp.path().join("other-cache");
    let (mut other, _) = host.open_with_cache(Some(other_cache));
    let mut keep_one = host.config.clone();
    keep_one.retention.keep_last = Some(1);
    let (stats, _) = commands::prune::run_on_repo(
        &mut other,
        &keep_one,
        false,
        false,
        &host.sources(),
        &[],
        &CancelToken::new(),
    )
    .unwrap();
    assert_eq!(stats.pruned, 1);
    drop(other);
    let mut other_config = host.config.clone();
    other_config.cache_dir = Some(
        host.tmp
            .path()
            .join("other-cache")
            .to_string_lossy()
            .into_owned(),
    );
    compact(&other_config, host.pass);
    assert!(p1.iter().all(|id| host.cached().contains(id)));

    host.backup("s3");
    let live = set([&host.item_ptrs("s2"), &host.item_ptrs("s3")]);
    let unique: Vec<&ChunkId> = p1.iter().filter(|id| !live.contains(id)).collect();
    assert!(!unique.is_empty());
    assert!(
        unique.iter().all(|id| host.cached().contains(id)),
        "a backup drops nothing"
    );

    compact(&host.config, host.pass);
    let cached = host.cached();
    assert!(unique.iter().all(|id| !cached.contains(id)));
    assert!(live.is_subset(&cached));
}

#[test]
fn restore_reads_the_listing_from_the_cache() {
    let host = Host::new(true);
    host.backup("s1");
    let ptrs = host.item_ptrs("s1");
    let opts = || RestoreOptions {
        xattrs_enabled: false,
        verify_chunks: false,
        continue_on_error: false,
    };
    let restore = |dest: &str| {
        let (mut repo, reads) = host.open();
        let tree_packs = pack_keys(&repo, &ptrs);
        reads.lock().unwrap().clear();
        let dest = host.tmp.path().join(dest);
        commands::restore::run_on_repo(
            &mut repo,
            "s1",
            dest.to_str().unwrap(),
            None,
            opts(),
            &CancelToken::new(),
        )
        .unwrap();
        assert!(dest.join("s1.txt").is_file());
        reads_of(&reads, &tree_packs)
    };

    assert_eq!(restore("warm"), 0);
    // Cold: the listing is downloaded and cached again.
    std::fs::remove_dir_all(host.cache_root()).unwrap();
    assert!(restore("cold") > 0);
    assert_eq!(host.cached(), set([&ptrs]));
}
