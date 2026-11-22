//! The mark engine against an independent sequential oracle: the report of
//! `compact --report-unreferenced`, check's shared tree-chunk traversal and
//! the repair refcount rebuild.

use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use vykar_storage::local_backend::LocalBackend;
use vykar_storage::{InnerBackend, StorageBackend, delegate_storage_backend};
use vykar_types::chunk_id::ChunkId;
use vykar_types::error::{Result, VykarError};

use crate::commands;
use crate::commands::check::{IntegrityIssue, RepairMode};
use crate::commands::compact::{UnreferencedReport, report_unreferenced_on_repo};
use crate::commands::list::{
    for_each_decoded_item, load_snapshot_item_stream_from_storage, load_snapshot_meta,
};
use crate::commands::mark::{
    MarkOptions, MissingRefs, TreeChunkSet, load_snapshots_strict, mark_live,
};
use crate::compress::Compression;
use crate::config::{EncryptionModeConfig, RetentionConfig, VykarConfig};
use crate::repo::control::CancelToken;
use crate::repo::format::{ObjectType, pack_object_with_context};
use crate::repo::pack::PackType;
use crate::repo::{OpenOptions, Repository, TreeChunkSource, WriteSessionOptions};
use crate::snapshot::item::{Item, ItemType};

use super::helpers::{backup_single_source_with, make_test_config, source_entry};

const PASS: &str = "mark-pass";

// ---------------------------------------------------------------------------
// Fixture
// ---------------------------------------------------------------------------

/// Range reads issued through a [`Probe`], by key and length.
type Reads = Arc<Mutex<Vec<(String, u64)>>>;

/// Which range reads a [`Probe`] fails with an I/O error: those of `key`
/// (only those covering byte `at` of it, when set), the first `times`
/// times (`usize::MAX`: always).
struct Fail {
    key: String,
    at: Option<u64>,
    times: Mutex<usize>,
}

/// `LocalBackend` that records every range read and can fail some.
struct Probe {
    inner: LocalBackend,
    reads: Reads,
    fail: Option<Fail>,
}

impl Probe {
    fn range(&self, key: &str, offset: u64, length: u64) -> Result<()> {
        self.reads.lock().unwrap().push((key.to_string(), length));
        if let Some(fail) = &self.fail
            && fail.key == key
            && fail
                .at
                .is_none_or(|at| offset <= at && at < offset + length)
        {
            let mut times = fail.times.lock().unwrap();
            if *times > 0 {
                if *times != usize::MAX {
                    *times -= 1;
                }
                return Err(VykarError::Io(std::io::Error::other(format!(
                    "injected range failure: {key}"
                ))));
            }
        }
        Ok(())
    }
}

impl InnerBackend for Probe {
    fn inner_backend(&self) -> &dyn StorageBackend {
        &self.inner
    }
}

delegate_storage_backend! {
    for Probe;
    except [get_range, get_range_into, open_range];

    fn get_range(&self, key: &str, offset: u64, length: u64) -> Result<Option<Vec<u8>>> {
        self.range(key, offset, length)?;
        self.inner.get_range(key, offset, length)
    }

    fn get_range_into(&self, key: &str, offset: u64, length: u64, buf: &mut Vec<u8>) -> Result<bool> {
        self.range(key, offset, length)?;
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

/// A local repository and its source directories.
struct Fx {
    tmp: tempfile::TempDir,
    repo_dir: PathBuf,
    config: VykarConfig,
    pass: Option<&'static str>,
}

impl Fx {
    fn new(encrypted: bool) -> Self {
        let tmp = tempfile::tempdir().unwrap();
        let repo_dir = tmp.path().join("repo");
        std::fs::create_dir_all(&repo_dir).unwrap();
        let mut config = make_test_config(&repo_dir);
        config.xattrs.enabled = false;
        if encrypted {
            config.encryption.mode = EncryptionModeConfig::Aes256Gcm;
        }
        let pass = encrypted.then_some(PASS);
        commands::init::run(&config, pass).unwrap();
        Self {
            tmp,
            repo_dir,
            config,
            pass,
        }
    }

    /// A source directory under the fixture, created if needed.
    fn src(&self, name: &str) -> PathBuf {
        let dir = self.tmp.path().join(name);
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    fn backup(&self, src: &Path, name: &str) {
        backup_single_source_with(&self.config, self.pass, src, "src", name);
        // Distinct snapshot times.
        std::thread::sleep(Duration::from_millis(5));
    }

    fn open_probe(&self, fail: Option<(String, usize)>) -> (Repository, Reads) {
        self.open_probe_with(fail.map(|(key, times)| Fail {
            key,
            at: None,
            times: Mutex::new(times),
        }))
    }

    fn open_probe_with(&self, fail: Option<Fail>) -> (Repository, Reads) {
        let reads = Reads::default();
        let backend = Probe {
            inner: LocalBackend::new(self.repo_dir.to_str().unwrap()).unwrap(),
            reads: Arc::clone(&reads),
            fail,
        };
        let repo = Repository::open(
            Box::new(backend),
            self.pass,
            None,
            OpenOptions::new().with_index(),
        )
        .unwrap();
        (repo, reads)
    }

    fn open(&self) -> Repository {
        self.open_probe(None).0
    }

    fn item_ptrs(&self, name: &str) -> Vec<ChunkId> {
        load_snapshot_meta(&self.open(), name).unwrap().item_ptrs
    }

    fn report(&self) -> Result<UnreferencedReport> {
        commands::compact::report_unreferenced(&self.config, self.pass, &CancelToken::new())
    }

    /// Delete snapshot `name`'s object behind vykar's back.
    fn leak_snapshot(&self, name: &str) {
        let repo = self.open();
        let id = repo.manifest().find_snapshot(name).unwrap().id;
        repo.storage.delete(&id.storage_key()).unwrap();
    }

    fn cache_root(&self) -> PathBuf {
        self.open()
            .tree_cache()
            .expect("test repositories have a cache dir")
            .root()
            .to_path_buf()
    }

    fn cached(&self) -> HashSet<ChunkId> {
        let mut ids = HashSet::new();
        let Ok(shards) = std::fs::read_dir(self.cache_root()) else {
            return ids;
        };
        for shard in shards {
            let Ok(entries) = std::fs::read_dir(shard.unwrap().path()) else {
                continue;
            };
            for entry in entries {
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

    /// The index head and the names under `index.d/`: what a report must
    /// not change.
    fn state(&self) -> (Vec<u8>, Vec<String>) {
        let head = std::fs::read(self.repo_dir.join("index")).unwrap();
        let mut objects = Vec::new();
        collect_files(&self.repo_dir.join("index.d"), &mut objects);
        objects.sort();
        (head, objects)
    }

    /// Edit the index under the maintenance lock and persist it.
    fn edit_index(&self, f: impl FnOnce(&mut Repository)) {
        let mut repo = self.open();
        commands::util::with_maintenance_lock(&mut repo, |repo| {
            repo.load_chunk_index_under_lock()?;
            f(repo);
            repo.save_state()
        })
        .unwrap();
    }
}

fn collect_files(dir: &Path, out: &mut Vec<String>) {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return;
    };
    for entry in entries {
        let path = entry.unwrap().path();
        if path.is_dir() {
            collect_files(&path, out);
        } else {
            out.push(path.to_string_lossy().into_owned());
        }
    }
}

fn write_files(dir: &Path, prefix: &str, n: usize) {
    for i in 0..n {
        std::fs::write(
            dir.join(format!("{prefix}-{i:03}.txt")),
            format!("{prefix} file {i} {}", "z".repeat(i % 7)),
        )
        .unwrap();
    }
}

/// `dirs` × 600 small files with unique content: several tree chunks
/// (each dir is roughly two).
fn write_tree(dir: &Path, tag: &str, dirs: usize) {
    for d in 0..dirs {
        let sub = dir.join(format!("{tag}-{d:02}"));
        std::fs::create_dir_all(&sub).unwrap();
        for f in 0..600 {
            std::fs::write(sub.join(format!("f-{f:04}.txt")), format!("{tag}/{d}/{f}")).unwrap();
        }
    }
}

/// The independent oracle: today's sequential refcount rebuild, per
/// snapshot of the manifest, over the snapshot's whole item stream.
fn sequential_refcounts(repo: &mut Repository) -> HashMap<ChunkId, u32> {
    let mut counts: HashMap<ChunkId, u32> = HashMap::new();
    let names: Vec<String> = repo
        .manifest()
        .snapshots
        .iter()
        .map(|e| e.name.clone())
        .collect();
    for name in &names {
        let meta = load_snapshot_meta(repo, name).unwrap();
        for id in &meta.item_ptrs {
            if repo.chunk_index().contains(id) {
                *counts.entry(*id).or_insert(0) += 1;
            }
        }
        let stream =
            load_snapshot_item_stream_from_storage(repo, name, &CancelToken::new()).unwrap();
        for_each_decoded_item(&stream, |item| {
            if item.entry_type == ItemType::RegularFile {
                for r in &item.chunks {
                    if repo.chunk_index().contains(&r.id) {
                        *counts.entry(r.id).or_insert(0) += 1;
                    }
                }
            }
            Ok(())
        })
        .unwrap();
    }
    counts
}

/// Per index entry: (marked, counted references), by `mark_live` over the
/// strict listing.
fn mark_counts(
    repo: &Repository,
    source: TreeChunkSource,
    decode_threads: usize,
) -> HashMap<ChunkId, (bool, u32)> {
    let shutdown = CancelToken::new();
    let snaps = load_snapshots_strict(repo, &shutdown).unwrap();
    let names: Vec<String> = snaps.iter().map(|s| s.meta.name.clone()).collect();
    let ptrs: Vec<&[ChunkId]> = snaps.iter().map(|s| s.meta.item_ptrs.as_slice()).collect();
    let set = TreeChunkSet::build(&ptrs);
    let positions = repo.chunk_index().positions();
    let mark = mark_live(
        repo,
        &set,
        &names,
        &positions,
        source,
        MarkOptions {
            count: true,
            missing_refs: MissingRefs::Abort,
            decode_threads,
        },
        &shutdown,
    )
    .unwrap();
    assert_eq!(mark.walk.chunks, set.len());
    let mut out = HashMap::new();
    positions.for_each_live(|p, id, _| {
        out.insert(id, (mark.is_marked(p), mark.count(p).unwrap()));
    });
    out
}

/// Four backups of two sources sharing files, one `delete` (which
/// decrements), then the object of `s2` deleted behind vykar's back: a
/// leak. Returns the fixture and `s2`'s item pointers.
fn leaky(encrypted: bool) -> (Fx, Vec<ChunkId>) {
    let fx = Fx::new(encrypted);
    let (a, b) = (fx.src("a"), fx.src("b"));
    write_files(&a, "a", 40);
    write_files(&b, "b", 40);
    std::fs::write(a.join("shared.bin"), vec![7u8; 300_000]).unwrap();
    std::fs::write(b.join("shared.bin"), vec![7u8; 300_000]).unwrap();
    fx.backup(&a, "s1");
    fx.backup(&b, "s2");
    write_files(&a, "a2", 10);
    std::fs::write(a.join("a-000.txt"), "changed").unwrap();
    fx.backup(&a, "s3");
    write_files(&b, "b2", 10);
    fx.backup(&b, "s4");
    commands::delete::run(&fx.config, fx.pass, &["s1"], false, &CancelToken::new()).unwrap();
    let leaked = fx.item_ptrs("s2");
    fx.leak_snapshot("s2");
    (fx, leaked)
}

// ---------------------------------------------------------------------------
// Tree-chunk set
// ---------------------------------------------------------------------------

#[test]
fn tree_chunk_set_counts_occurrences_and_first_snapshot() {
    let id = |n: u8| ChunkId::from_bytes([n; 32]);
    let s0 = vec![id(1), id(2), id(1)];
    let s1 = vec![id(2), id(3)];
    let s2: Vec<ChunkId> = Vec::new();
    let s3 = vec![id(3), id(3), id(4)];
    let set = TreeChunkSet::build(&[&s0, &s1, &s2, &s3]);
    assert_eq!(set.len(), 4);
    assert!(!set.is_empty());
    let expect = [(1, 2, 0), (2, 2, 0), (3, 3, 1), (4, 1, 3)];
    for (n, occurrences, first) in expect {
        let k = set.slot(&id(n)).unwrap();
        assert_eq!(set.id(k), &id(n));
        assert_eq!(set.occurrences(k), occurrences, "chunk {n}");
        assert_eq!(set.first_snapshot(k), first, "chunk {n}");
        assert!(set.contains(&id(n)));
    }
    assert!(!set.contains(&id(9)));
    assert!(TreeChunkSet::build(&[]).is_empty());
}

// ---------------------------------------------------------------------------
// The report against the sequential oracle
// ---------------------------------------------------------------------------

#[test]
fn report_matches_a_sequential_rebuild() {
    for encrypted in [false, true] {
        let (fx, _) = leaky(encrypted);
        let mut repo = fx.open();
        let oracle = sequential_refcounts(&mut repo);
        let entries: Vec<_> = repo.chunk_index().iter().collect();

        let report = fx.report().unwrap();
        let mut zero: Vec<ChunkId> = entries
            .iter()
            .filter(|(id, _)| !oracle.contains_key(id))
            .map(|(id, _)| *id)
            .collect();
        zero.sort_unstable_by(|a, b| a.as_bytes().cmp(b.as_bytes()));
        assert!(
            !zero.is_empty(),
            "encrypted={encrypted}: the leak left entries"
        );
        assert_eq!(report.unreferenced, zero, "encrypted={encrypted}");
        assert_eq!(report.entries, entries.len());
        assert_eq!(report.referenced + report.unreferenced.len(), entries.len());
        let zero_bytes: u64 = entries
            .iter()
            .filter(|(id, _)| !oracle.contains_key(id))
            .map(|(_, e)| u64::from(e.stored_size))
            .sum();
        assert_eq!(report.unreferenced_bytes, zero_bytes);
        let mismatches = entries
            .iter()
            .filter(|(id, e)| oracle.get(id).copied().unwrap_or(0) != e.refcount)
            .count();
        assert!(mismatches > 0, "the leak inflated refcounts");
        assert_eq!(
            report.refcount_mismatches, mismatches,
            "encrypted={encrypted}"
        );
        assert_eq!(report.unreferenced_with_refcount, zero.len());
        assert_eq!(report.referenced_with_zero_refcount, 0);
        assert_eq!(report.snapshots, 2);
        assert!(report.packs_with_unreferenced > 0);

        for threads in [1, 4] {
            let counts = mark_counts(&repo, TreeChunkSource::Storage, threads);
            assert_eq!(counts.len(), entries.len());
            for (id, _) in &entries {
                let (marked, count) = counts[id];
                let expected = oracle.get(id).copied().unwrap_or(0);
                assert_eq!(count, expected, "encrypted={encrypted}: count of {id}");
                assert_eq!(marked, expected > 0, "encrypted={encrypted}: mark of {id}");
            }
        }
    }
}

#[test]
fn report_on_a_healthy_repository_is_clean() {
    for encrypted in [false, true] {
        let fx = Fx::new(encrypted);
        let src = fx.src("src");
        write_files(&src, "f", 30);
        for (n, name) in ["s1", "s2", "s3", "s4"].iter().enumerate() {
            write_files(&src, &format!("g{n}"), 5);
            fx.backup(&src, name);
        }
        let mut config = fx.config.clone();
        config.retention = RetentionConfig {
            keep_last: Some(2),
            ..RetentionConfig::default()
        };
        let (stats, _) = commands::prune::run(
            &config,
            fx.pass,
            false,
            false,
            &[source_entry(&src, "src")],
            &[],
            &CancelToken::new(),
        )
        .unwrap();
        assert_eq!(stats.pruned, 2);

        let report = fx.report().unwrap();
        assert_eq!(report.snapshots, 2);
        assert!(report.unreferenced.is_empty(), "{:?}", report.unreferenced);
        assert_eq!(report.refcount_mismatches, 0, "encrypted={encrypted}");
        assert_eq!(report.referenced, report.entries);
        assert_eq!(report.packs_with_unreferenced, 0);
        assert!(report.items > 0 && report.references > 0 && report.tree_chunks > 0);
    }
}

#[test]
fn report_with_no_snapshots_lists_every_entry() {
    let fx = Fx::new(false);
    let src = fx.src("src");
    write_files(&src, "f", 10);
    fx.backup(&src, "s1");
    fx.leak_snapshot("s1");
    let report = fx.report().unwrap();
    assert_eq!(report.snapshots, 0);
    assert!(report.entries > 0);
    assert_eq!(report.unreferenced.len(), report.entries);
    assert_eq!(
        report.packs_only_unreferenced,
        report.packs_with_unreferenced
    );
}

/// D7: the chunk references of a non-regular item are marked but not
/// counted, as the stored refcounts do.
#[test]
fn references_of_non_regular_items_are_marked_not_counted() {
    let fx = Fx::new(false);
    let src = fx.src("src");
    write_files(&src, "f", 10);
    fx.backup(&src, "s1");
    let mut moved: Option<ChunkId> = None;
    let mut kept: Option<ChunkId> = None;
    replace_tree_chunk(&fx, "s1", 0, |plain| {
        let mut items = decode_items(&plain);
        let mut files = items
            .iter_mut()
            .filter(|i| i.entry_type == ItemType::RegularFile && i.chunks.len() == 1);
        let a = files.next().unwrap();
        a.entry_type = ItemType::Symlink;
        moved = Some(a.chunks[0].id);
        kept = Some(files.next().unwrap().chunks[0].id);
        encode_items(&items)
    });
    let (moved, kept) = (moved.unwrap(), kept.unwrap());
    let repo = fx.open();
    for threads in [1, 4] {
        let counts = mark_counts(&repo, TreeChunkSource::Storage, threads);
        assert_eq!(counts[&moved], (true, 0), "threads={threads}");
        assert_eq!(counts[&kept], (true, 1), "threads={threads}");
    }
    drop(repo);
    let report = fx.report().unwrap();
    assert!(!report.unreferenced.contains(&moved));
}

// ---------------------------------------------------------------------------
// Reads
// ---------------------------------------------------------------------------

/// Bytes read from the packs that hold `ids`.
fn bytes_read_from(reads: &Reads, repo: &Repository, ids: &[ChunkId]) -> (usize, u64) {
    let keys: HashSet<String> = ids
        .iter()
        .map(|id| repo.chunk_index().get(id).unwrap().pack_id.storage_key())
        .collect();
    let reads = reads.lock().unwrap();
    let hits: Vec<_> = reads.iter().filter(|(k, _)| keys.contains(k)).collect();
    (hits.len(), hits.iter().map(|(_, n)| n).sum())
}

#[test]
fn mark_decodes_each_distinct_tree_chunk_once() {
    let fx = Fx::new(false);
    let src = fx.src("src");
    write_tree(&src, "t", 6);
    for name in ["s1", "s2", "s3"] {
        fx.backup(&src, name);
    }
    let p1 = fx.item_ptrs("s1");
    assert!(p1.len() >= 4, "only {} tree chunks", p1.len());
    assert_eq!(fx.item_ptrs("s2"), p1);
    assert_eq!(fx.item_ptrs("s3"), p1);
    let distinct: HashSet<ChunkId> = p1.iter().copied().collect();

    // One snapshot's listing, read from storage.
    let (mut repo, reads) = fx.open_probe(None);
    repo.set_tree_cache(None);
    load_snapshot_item_stream_from_storage(&mut repo, "s1", &CancelToken::new()).unwrap();
    let one_copy = bytes_read_from(&reads, &repo, &p1);
    assert!(one_copy.1 > 0);

    reads.lock().unwrap().clear();
    let snaps = load_snapshots_strict(&repo, &CancelToken::new()).unwrap();
    let names: Vec<String> = snaps.iter().map(|s| s.meta.name.clone()).collect();
    let ptrs: Vec<&[ChunkId]> = snaps.iter().map(|s| s.meta.item_ptrs.as_slice()).collect();
    let set = TreeChunkSet::build(&ptrs);
    assert_eq!(set.len(), distinct.len());
    let positions = repo.chunk_index().positions();
    let mark = mark_live(
        &repo,
        &set,
        &names,
        &positions,
        TreeChunkSource::CacheFirst,
        MarkOptions {
            count: false,
            missing_refs: MissingRefs::Abort,
            decode_threads: 4,
        },
        &CancelToken::new(),
    )
    .unwrap();
    assert_eq!(mark.walk.chunks, distinct.len());
    assert_eq!(mark.walk.batches, 1);
    assert_eq!(bytes_read_from(&reads, &repo, &p1).1, one_copy.1);
    assert_eq!(mark.count(0), None, "no counts without count: true");
}

#[test]
fn report_reads_tree_chunks_from_the_cache() {
    let fx = Fx::new(true);
    let src = fx.src("src");
    write_tree(&src, "t", 2);
    fx.backup(&src, "s1");
    write_files(&src, "more", 5);
    fx.backup(&src, "s2");
    let ptrs: Vec<ChunkId> = [fx.item_ptrs("s1"), fx.item_ptrs("s2")].concat();

    let (mut repo, reads) = fx.open_probe(None);
    reads.lock().unwrap().clear();
    let report = report_unreferenced_on_repo(&mut repo, &CancelToken::new()).unwrap();
    assert!(report.tree_chunks > 0);
    assert_eq!(bytes_read_from(&reads, &repo, &ptrs).0, 0);
}

#[test]
fn report_drops_cached_tree_chunks_no_snapshot_uses() {
    let (fx, leaked) = leaky(false);
    let live: HashSet<ChunkId> = [fx.item_ptrs("s3"), fx.item_ptrs("s4")]
        .concat()
        .into_iter()
        .collect();
    let unique: Vec<ChunkId> = leaked
        .iter()
        .filter(|id| !live.contains(id))
        .copied()
        .collect();
    assert!(!unique.is_empty());
    assert!(unique.iter().all(|id| fx.cached().contains(id)));

    fx.report().unwrap();
    let cached = fx.cached();
    assert!(unique.iter().all(|id| !cached.contains(id)));
    assert!(live.is_subset(&cached));
}

// ---------------------------------------------------------------------------
// Fail closed
// ---------------------------------------------------------------------------

/// Damage `fx`, run the report and return its error. A refused report
/// changes nothing on storage and drops nothing from the tree-chunk cache
/// (its reads may fill it).
fn refused(fx: &Fx, damage: impl FnOnce(&Fx)) -> VykarError {
    damage(fx);
    let before = fx.state();
    let cached = fx.cached();
    let err = fx.report().unwrap_err();
    assert_eq!(
        fx.state(),
        before,
        "a refused report changed something: {err}"
    );
    assert!(
        cached.is_subset(&fx.cached()),
        "a refused report dropped cached tree chunks: {err}"
    );
    err
}

fn two_snapshots(encrypted: bool) -> Fx {
    let fx = Fx::new(encrypted);
    let src = fx.src("src");
    write_files(&src, "f", 20);
    fx.backup(&src, "s1");
    write_files(&src, "g", 5);
    fx.backup(&src, "s2");
    fx
}

fn assert_framed(err: &VykarError, needle: &str) {
    let msg = err.to_string();
    assert!(
        msg.starts_with("cannot report unreferenced chunks, nothing was changed:")
            && msg.contains(needle),
        "expected a framed error naming {needle:?}, got: {msg}"
    );
}

#[test]
fn report_refuses_a_garbage_snapshot_object() {
    for encrypted in [false, true] {
        let fx = two_snapshots(encrypted);
        let key = format!("snapshots/{}", "ab".repeat(32));
        let err = refused(&fx, |fx| {
            std::fs::write(fx.repo_dir.join(&key), rand::random::<[u8; 32]>()).unwrap();
        });
        assert_framed(&err, &format!("snapshot object {key}"));
    }
}

#[test]
fn report_refuses_a_non_id_key_under_snapshots() {
    let fx = two_snapshots(false);
    let err = refused(&fx, |fx| {
        std::fs::write(fx.repo_dir.join("snapshots/not-an-id"), b"x").unwrap();
    });
    assert_framed(
        &err,
        "unexpected object snapshots/not-an-id under snapshots/",
    );
}

#[test]
fn report_refuses_a_newer_snapshot_format() {
    let fx = two_snapshots(true);
    let err = refused(&fx, |fx| {
        let repo = fx.open();
        let entry = repo.manifest().find_snapshot("s2").unwrap().clone();
        let mut meta = load_snapshot_meta(&repo, "s2").unwrap();
        meta.format_version += 1;
        let packed = pack_object_with_context(
            ObjectType::SnapshotMeta,
            entry.id.as_bytes(),
            &rmp_serde::to_vec(&meta).unwrap(),
            repo.crypto.as_ref(),
        )
        .unwrap();
        repo.storage.put(&entry.id.storage_key(), &packed).unwrap();
    });
    assert!(
        matches!(err, VykarError::UnsupportedSnapshotVersion { .. }),
        "got {err:?}"
    );
}

/// A `RegularFile` chunk reference of snapshot `name`.
fn a_file_chunk(fx: &Fx, name: &str) -> ChunkId {
    let mut repo = fx.open();
    commands::list::load_snapshot_items(&mut repo, name, &CancelToken::new())
        .unwrap()
        .iter()
        .find(|i| i.entry_type == ItemType::RegularFile && !i.chunks.is_empty())
        .unwrap()
        .chunks[0]
        .id
}

#[test]
fn report_refuses_a_data_reference_missing_from_the_index() {
    for encrypted in [false, true] {
        let fx = two_snapshots(encrypted);
        let id = a_file_chunk(&fx, "s2");
        let err = refused(&fx, |fx| {
            fx.edit_index(|repo| {
                repo.chunk_index_mut().remove(&id).unwrap();
            });
        });
        assert_framed(
            &err,
            &format!("references chunk {id}, which is not in the index"),
        );
    }
}

#[test]
fn report_refuses_a_tree_chunk_missing_from_the_index() {
    let fx = two_snapshots(false);
    let id = fx.item_ptrs("s2")[0];
    let err = refused(&fx, |fx| {
        fx.edit_index(|repo| {
            repo.chunk_index_mut().remove(&id).unwrap();
        });
        assert!(fx.cached().contains(&id), "the chunk stays cached");
    });
    assert_framed(
        &err,
        &format!("names tree chunk {id}, which is not in the index"),
    );
}

#[test]
fn report_refuses_an_unreadable_tree_chunk() {
    let fx = two_snapshots(false);
    let err = refused(&fx, |fx| {
        let repo = fx.open();
        let id = fx.item_ptrs("s2")[0];
        let pack = repo.chunk_index().get(&id).unwrap().pack_id;
        std::fs::remove_file(fx.repo_dir.join(pack.storage_key())).unwrap();
        std::fs::remove_dir_all(fx.cache_root()).unwrap();
    });
    assert_framed(&err, "reading tree chunks:");
}

#[test]
fn report_is_cancellable() {
    let fx = two_snapshots(false);
    let mut repo = fx.open();
    let token = CancelToken::new();
    token.cancel();
    let err = report_unreferenced_on_repo(&mut repo, &token).unwrap_err();
    assert!(matches!(err, VykarError::Interrupted), "got {err:?}");
}

#[test]
fn report_refuses_while_a_session_is_active() {
    let fx = two_snapshots(false);
    let sessions = fx.repo_dir.join("sessions");
    std::fs::create_dir_all(&sessions).unwrap();
    let now = chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Secs, true);
    std::fs::write(
        sessions.join("deadbeef-1234-5678-9abc-def012345678.json"),
        format!(
            r#"{{"hostname":"other-host","pid":99999,"registered_at":"{now}","last_refresh":"{now}"}}"#
        ),
    )
    .unwrap();
    let err = fx.report().unwrap_err();
    assert!(matches!(err, VykarError::ActiveSessions(_)), "got {err:?}");
}

// ---------------------------------------------------------------------------
// check on the traversal
// ---------------------------------------------------------------------------

/// Re-seal snapshot `name`'s object with its `item_ptrs` edited by `f`.
fn edit_item_ptrs(repo: &Repository, name: &str, f: impl FnOnce(&mut Vec<ChunkId>)) {
    let entry = repo.manifest().find_snapshot(name).unwrap().clone();
    let mut meta = load_snapshot_meta(repo, name).unwrap();
    f(&mut meta.item_ptrs);
    let packed = pack_object_with_context(
        ObjectType::SnapshotMeta,
        entry.id.as_bytes(),
        &rmp_serde::to_vec(&meta).unwrap(),
        repo.crypto.as_ref(),
    )
    .unwrap();
    repo.storage.put(&entry.id.storage_key(), &packed).unwrap();
}

/// Snapshot `name` with its tree chunk `k` replaced by `edit` of its
/// plaintext: the result is stored as a new tree chunk and the snapshot
/// object re-sealed with the new pointer, which is returned.
fn replace_tree_chunk(
    fx: &Fx,
    name: &str,
    k: usize,
    edit: impl FnOnce(Vec<u8>) -> Vec<u8>,
) -> ChunkId {
    let mut repo = fx.open();
    let old = load_snapshot_meta(&repo, name).unwrap().item_ptrs[k];
    let chunk = edit(repo.read_chunk(&old).unwrap());
    repo.begin_write_session(&WriteSessionOptions::default())
        .unwrap();
    let (id, _, _) = repo
        .store_chunk(&chunk, Compression::None, PackType::Tree)
        .unwrap();
    repo.save_state().unwrap();
    edit_item_ptrs(&repo, name, |ptrs| ptrs[k] = id);
    id
}

fn decode_items(plain: &[u8]) -> Vec<Item> {
    let mut items = Vec::new();
    for_each_decoded_item(plain, |item| {
        items.push(item);
        Ok(())
    })
    .unwrap();
    items
}

fn encode_items(items: &[Item]) -> Vec<u8> {
    let mut chunk = Vec::new();
    for item in items {
        chunk.extend_from_slice(&rmp_serde::to_vec(item).unwrap());
    }
    chunk
}

/// Snapshot `name` with one item of its tree chunk `k` made invalid
/// (its size disagrees with its chunks). Returns the item's path.
fn invalidate_item(fx: &Fx, name: &str, k: usize) -> String {
    let mut path = String::new();
    replace_tree_chunk(fx, name, k, |plain| {
        let mut items = decode_items(&plain);
        let item = items
            .iter_mut()
            .find(|i| i.entry_type == ItemType::RegularFile && !i.chunks.is_empty())
            .unwrap();
        item.size += 1;
        path = item.path.clone();
        encode_items(&items)
    });
    path
}

fn sequential_check(repo: &mut Repository) -> (Vec<String>, usize, Vec<Vec<HashSet<ChunkId>>>) {
    let mut issues = Vec::new();
    let mut items_total = 0;
    let mut per_item_all = Vec::new();
    let entries = repo.manifest().snapshots.clone();
    for entry in &entries {
        let meta = load_snapshot_meta(repo, &entry.name).unwrap();
        for id in &meta.item_ptrs {
            if !repo.chunk_index().contains(id) {
                issues.push(format!(
                    "{:?}",
                    IntegrityIssue::DanglingItemPtr {
                        snapshot_name: entry.name.clone(),
                        chunk_id: *id,
                    }
                ));
            }
        }
        let stream =
            load_snapshot_item_stream_from_storage(repo, &entry.name, &CancelToken::new()).unwrap();
        let mut idx = 0;
        let mut per_item = Vec::new();
        for_each_decoded_item(&stream, |item| {
            if let Err(e) = item.validate() {
                issues.push(format!(
                    "{:?}",
                    IntegrityIssue::InvalidItem {
                        snapshot_id: entry.id,
                        snapshot_name: Some(entry.name.clone()),
                        item_index: idx,
                        item_path: item.path.clone(),
                        reason: e.to_string(),
                    }
                ));
            }
            let mut set = HashSet::new();
            if item.entry_type == ItemType::RegularFile {
                for r in &item.chunks {
                    set.insert(r.id);
                    if !repo.chunk_index().contains(&r.id) {
                        issues.push(format!(
                            "{:?}",
                            IntegrityIssue::DanglingFileChunk {
                                snapshot_name: entry.name.clone(),
                                item_index: idx,
                                path: item.path.clone(),
                                chunk_id: r.id,
                            }
                        ));
                    }
                }
            }
            per_item.push(set);
            idx += 1;
            Ok(())
        })
        .unwrap();
        items_total += idx;
        per_item_all.push(per_item);
    }
    (issues, items_total, per_item_all)
}

#[test]
fn check_reports_unchanged_issues() {
    for encrypted in [false, true] {
        let fx = Fx::new(encrypted);
        let (x, y) = (fx.src("x"), fx.src("y"));
        write_tree(&x, "x", 3);
        write_tree(&y, "y", 2);
        fx.backup(&x, "A");
        write_files(&x.join("x-01"), "new", 30);
        fx.backup(&x, "B");
        fx.backup(&y, "C");
        assert!(fx.item_ptrs("B").len() >= 3, "B needs several tree chunks");
        let path = invalidate_item(&fx, "B", 1);
        // A file both A and B hold.
        let mut repo = fx.open();
        let items_a =
            commands::list::load_snapshot_items(&mut repo, "A", &CancelToken::new()).unwrap();
        let shared = items_a
            .iter()
            .rev()
            .find(|i| i.entry_type == ItemType::RegularFile && !i.chunks.is_empty())
            .unwrap()
            .chunks[0]
            .id;
        drop(repo);
        fx.edit_index(|repo| {
            repo.chunk_index_mut().remove(&shared).unwrap();
        });

        let mut repo = fx.open();
        let (expected, items_total, per_item) = sequential_check(&mut repo);
        let entries = repo.manifest().snapshots.clone();
        let item_ptrs: HashMap<String, Vec<ChunkId>> = entries
            .iter()
            .map(|e| {
                let ptrs = load_snapshot_meta(&repo, &e.name).unwrap().item_ptrs;
                (e.name.clone(), ptrs)
            })
            .collect();
        let first_chunk_items = {
            let plain = repo.read_chunk(&item_ptrs["B"][0]).unwrap();
            let mut n = 0;
            for_each_decoded_item(&plain, |_| {
                n += 1;
                Ok(())
            })
            .unwrap();
            n
        };
        assert!(
            expected
                .iter()
                .any(|i| i.contains("InvalidItem") && i.contains(&path)),
            "{expected:?}"
        );
        assert_eq!(
            expected
                .iter()
                .filter(|i| i.contains("DanglingFileChunk"))
                .count(),
            2
        );

        for collect in [false, true] {
            let scan = commands::check::scan_for_test(&mut repo, &fx.config, collect).unwrap();
            let got: Vec<String> = scan.issues.iter().map(|i| format!("{i:?}")).collect();
            assert_eq!(got, expected, "encrypted={encrypted} collect={collect}");
            assert_eq!(scan.snapshots_checked, 3);
            assert_eq!(scan.items_checked, items_total);
            for (entry, sets) in entries.iter().zip(&per_item) {
                assert_eq!(scan.snapshot_item_counts[&entry.id], sets.len());
                if collect {
                    assert_eq!(&scan.snapshot_per_item_chunks[&entry.id], sets);
                    let files: HashSet<ChunkId> = sets.iter().flatten().copied().collect();
                    let ptrs: HashSet<ChunkId> = item_ptrs[&entry.name].iter().copied().collect();
                    let all: HashSet<ChunkId> = files.union(&ptrs).copied().collect();
                    assert_eq!(scan.snapshot_chunk_refs[&entry.name], all);
                } else {
                    assert!(scan.snapshot_per_item_chunks.is_empty());
                }
            }
        }
        let invalid_index = commands::check::scan_for_test(&mut repo, &fx.config, false)
            .unwrap()
            .issues
            .iter()
            .find_map(|i| match i {
                IntegrityIssue::InvalidItem { item_index, .. } => Some(*item_index),
                _ => None,
            })
            .unwrap();
        assert!(
            invalid_index >= first_chunk_items,
            "item_index {invalid_index} is the item's position in the snapshot"
        );
    }
}

/// Flip a byte in the middle of the stored blob of `id`; the pack stays.
fn flip_blob(fx: &Fx, id: &ChunkId) {
    let repo = fx.open();
    let e = repo.chunk_index().get(id).unwrap();
    let path = fx.repo_dir.join(e.pack_id.storage_key());
    let mut pack = std::fs::read(&path).unwrap();
    pack[e.pack_offset as usize + e.stored_size as usize / 2] ^= 0x01;
    std::fs::write(&path, pack).unwrap();
}

/// A and B back up the same unchanged source (same tree chunks), C another.
fn shared_trees(encrypted: bool) -> Fx {
    let fx = Fx::new(encrypted);
    let (x, y) = (fx.src("x"), fx.src("y"));
    write_tree(&x, "x", 3);
    write_tree(&y, "y", 1);
    fx.backup(&x, "A");
    fx.backup(&x, "B");
    fx.backup(&y, "C");
    assert_eq!(fx.item_ptrs("A"), fx.item_ptrs("B"));
    fx
}

#[test]
fn check_attributes_a_bad_tree_chunk_to_every_snapshot_that_uses_it() {
    for encrypted in [false, true] {
        let fx = shared_trees(encrypted);
        let c: HashSet<ChunkId> = fx.item_ptrs("C").into_iter().collect();
        let ptrs = fx.item_ptrs("A");
        assert!(ptrs.len() <= crate::commands::mark::MARK_BATCH_CHUNKS);
        let bad = *ptrs.iter().find(|id| !c.contains(id)).unwrap();
        flip_blob(&fx, &bad);

        let mut repo = fx.open();
        let scan = commands::check::scan_for_test(&mut repo, &fx.config, false).unwrap();
        let names: Vec<&str> = scan
            .issues
            .iter()
            .map(|i| match i {
                IntegrityIssue::UnreadableSnapshot {
                    snapshot_name,
                    detail,
                } => {
                    assert!(detail.starts_with("load item stream: "), "{detail}");
                    snapshot_name.as_str()
                }
                other => panic!("unexpected issue {other:?}"),
            })
            .collect();
        assert_eq!(names, ["A", "B"], "encrypted={encrypted}");
        assert_eq!(scan.snapshots_checked, 1);

        let result = commands::check::run(&fx.config, fx.pass, false, false).unwrap();
        assert_eq!(result.errors.len(), 2, "{:?}", result.errors);
        assert_eq!(result.snapshots_checked, 1);
    }
}

/// The pack holding A's tree chunks, which B shares and C does not use.
fn shared_tree_pack(fx: &Fx) -> String {
    let repo = fx.open();
    let pack = |id: &ChunkId| repo.chunk_index().get(id).unwrap().pack_id.storage_key();
    let a: HashSet<String> = fx.item_ptrs("A").iter().map(pack).collect();
    let c: HashSet<String> = fx.item_ptrs("C").iter().map(pack).collect();
    assert_eq!(a.len(), 1, "A's tree chunks are in one pack");
    let key = a.into_iter().next().unwrap();
    assert!(!c.contains(&key));
    key
}

/// Range reads of `key` recorded in `reads`.
fn reads_of(reads: &Reads, key: &str) -> usize {
    reads
        .lock()
        .unwrap()
        .iter()
        .filter(|(k, _)| k == key)
        .count()
}

/// The snapshot names of `issues`, each of which must be `issue` with a
/// detail starting with `prefix`.
fn stream_issue_names<'a>(
    issues: &'a [IntegrityIssue],
    read_failed: bool,
    prefix: &str,
) -> Vec<&'a str> {
    issues
        .iter()
        .map(|i| match (i, read_failed) {
            (
                IntegrityIssue::SnapshotReadFailed {
                    snapshot_name,
                    detail,
                },
                true,
            )
            | (
                IntegrityIssue::UnreadableSnapshot {
                    snapshot_name,
                    detail,
                },
                false,
            ) => {
                assert!(detail.starts_with(prefix), "{detail}");
                snapshot_name.as_str()
            }
            (other, _) => panic!("unexpected issue {other:?}"),
        })
        .collect()
}

#[test]
fn check_attributes_a_read_failure_only_when_it_recurs() {
    let fx = shared_trees(false);
    let key = shared_tree_pack(&fx);
    let chunks = fx.item_ptrs("A").len();
    assert!(chunks >= 4);

    // Persistent: attributed, with bounded reads.
    let (mut repo, reads) = fx.open_probe(Some((key.clone(), usize::MAX)));
    let scan = commands::check::scan_for_test(&mut repo, &fx.config, false).unwrap();
    for i in &scan.issues {
        if let IntegrityIssue::SnapshotReadFailed { detail, .. } = i {
            assert!(detail.contains("injected range failure"), "{detail}");
        }
    }
    let names = stream_issue_names(&scan.issues, true, "load item stream: ");
    assert_eq!(names, ["A", "B"]);
    assert_eq!(scan.snapshots_checked, 1);
    // The batch, the pack, and the first chunk of the pack.
    let tries = reads_of(&reads, &key);
    assert_eq!(
        tries, 3,
        "{tries} reads of the failing pack ({chunks} chunks)"
    );
    drop(repo);

    // Transient: nothing to report.
    let (mut repo, _) = fx.open_probe(Some((key, 1)));
    let scan = commands::check::scan_for_test(&mut repo, &fx.config, false).unwrap();
    assert!(scan.issues.is_empty(), "{:?}", scan.issues);
    assert_eq!(scan.snapshots_checked, 3);
}

#[test]
fn check_spreads_only_an_io_failure_to_the_rest_of_its_pack() {
    // A, B and D back up the same source; then B keeps only the pack's
    // first tree chunk and D only its last.
    let fx = Fx::new(false);
    let x = fx.src("x");
    write_tree(&x, "x", 3);
    for name in ["A", "B", "D"] {
        fx.backup(&x, name);
    }
    let repo = fx.open();
    let mut ordered: Vec<(ChunkId, u64, u32)> = fx
        .item_ptrs("A")
        .iter()
        .map(|id| {
            let e = repo.chunk_index().get(id).unwrap();
            (*id, e.pack_offset, e.stored_size)
        })
        .collect();
    let packs: HashSet<String> = fx
        .item_ptrs("A")
        .iter()
        .map(|id| repo.chunk_index().get(id).unwrap().pack_id.storage_key())
        .collect();
    assert_eq!(packs.len(), 1, "A's tree chunks are in one pack");
    let key = packs.into_iter().next().unwrap();
    ordered.sort_unstable_by_key(|(_, offset, _)| *offset);
    let n = ordered.len();
    assert!(n >= 4, "only {n} tree chunks");
    let (first, target, last) = (ordered[0], ordered[1], ordered[n - 1]);
    edit_item_ptrs(&repo, "B", |ptrs| *ptrs = vec![first.0]);
    edit_item_ptrs(&repo, "D", |ptrs| *ptrs = vec![last.0]);
    drop(repo);

    // An I/O failure on the pack's second chunk: the first chunk is read,
    // the second fails, and its error goes to the rest of the pack unread.
    let (mut repo, reads) = fx.open_probe_with(Some(Fail {
        key: key.clone(),
        at: Some(target.1 + u64::from(target.2) / 2),
        times: Mutex::new(usize::MAX),
    }));
    let scan = commands::check::scan_for_test(&mut repo, &fx.config, false).unwrap();
    let names = stream_issue_names(&scan.issues, true, "load item stream: ");
    assert_eq!(names, ["A", "D"]);
    assert_eq!(scan.snapshots_checked, 1);
    // The batch, the pack, the first chunk and the second.
    assert_eq!(reads_of(&reads, &key), 4);
    drop(repo);

    // Any other failure stays with its chunk: every chunk of the pack is
    // re-read on its own, and only A, which uses the bad one, fails.
    flip_blob(&fx, &target.0);
    let (mut repo, reads) = fx.open_probe(None);
    let scan = commands::check::scan_for_test(&mut repo, &fx.config, false).unwrap();
    let names = stream_issue_names(&scan.issues, false, "load item stream: ");
    assert_eq!(names, ["A"]);
    assert_eq!(scan.snapshots_checked, 2);
    assert_eq!(reads_of(&reads, &key), 2 + n);
}

#[test]
fn a_tree_chunk_that_does_not_decode_alone() {
    // A's tree chunk 1 loses its last byte; chunks 0 and 2 each hold an
    // invalid item.
    let fx = Fx::new(false);
    let (x, y) = (fx.src("x"), fx.src("y"));
    write_tree(&x, "x", 3);
    write_tree(&y, "y", 1);
    fx.backup(&x, "A");
    fx.backup(&y, "C");
    assert!(fx.item_ptrs("A").len() >= 3, "A needs three tree chunks");
    let path0 = invalidate_item(&fx, "A", 0);
    let path2 = invalidate_item(&fx, "A", 2);
    let cut = replace_tree_chunk(&fx, "A", 1, |mut plain| {
        plain.pop();
        plain
    });

    // The items decoded before the cut, and those of the other chunks.
    let mut repo = fx.open();
    let count = |plain: &[u8]| {
        let mut n = 0usize;
        let r = for_each_decoded_item(plain, |_| {
            n += 1;
            Ok(())
        });
        (n, r.is_ok())
    };
    let ptrs_a = fx.item_ptrs("A");
    let (items0, ok0) = count(&repo.read_chunk(&ptrs_a[0]).unwrap());
    let (items1, ok1) = count(&repo.read_chunk(&cut).unwrap());
    assert!(ok0 && !ok1 && items1 > 0);
    let items_c: usize = fx
        .item_ptrs("C")
        .iter()
        .map(|id| count(&repo.read_chunk(id).unwrap()).0)
        .sum();
    let id_c = repo.manifest().find_snapshot("C").unwrap().id;
    let id_a = repo.manifest().find_snapshot("A").unwrap().id;

    for collect in [false, true] {
        let scan = commands::check::scan_for_test(&mut repo, &fx.config, collect).unwrap();
        // The decode error first, then the items before it; chunk 2 is
        // not walked.
        match scan.issues.as_slice() {
            [
                IntegrityIssue::UnreadableSnapshot {
                    snapshot_name,
                    detail,
                },
                IntegrityIssue::InvalidItem {
                    snapshot_name: Some(invalid_in),
                    item_path,
                    item_index,
                    ..
                },
            ] => {
                assert_eq!(snapshot_name, "A");
                assert!(detail.starts_with("decode items: "), "{detail}");
                assert_eq!(invalid_in, "A");
                assert_eq!(item_path, &path0);
                assert!(*item_index < items0);
            }
            other => panic!("collect={collect}: unexpected issues {other:?}"),
        }
        assert!(!scan.issues.iter().any(
            |i| matches!(i, IntegrityIssue::InvalidItem { item_path, .. } if *item_path == path2)
        ));
        assert_eq!(scan.snapshots_checked, 2);
        assert_eq!(scan.items_checked, items0 + items1 + items_c);
        assert!(!scan.snapshot_item_counts.contains_key(&id_a));
        assert_eq!(scan.snapshot_item_counts[&id_c], items_c);
        assert!(!scan.snapshot_per_item_chunks.contains_key(&id_a));
        assert_eq!(
            scan.snapshot_per_item_chunks.get(&id_c).map(Vec::len),
            collect.then_some(items_c)
        );
    }
    drop(repo);

    let err = refused(&fx, |_| {});
    assert_framed(&err, &format!("tree chunk {cut} of snapshot"));
    assert_framed(&err, "does not decode");
}

// ---------------------------------------------------------------------------
// Repair on the traversal
// ---------------------------------------------------------------------------

#[test]
fn repair_rebuild_matches_a_sequential_rebuild() {
    for encrypted in [false, true] {
        let (fx, _) = leaky(encrypted);
        let oracle = sequential_refcounts(&mut fx.open());
        let result = commands::check::run_with_repair(
            &fx.config,
            fx.pass,
            false,
            RepairMode::Apply,
            None,
            &CancelToken::new(),
        )
        .unwrap();
        assert!(
            result.repair_errors.is_empty(),
            "{:?}",
            result.repair_errors
        );

        let repo = fx.open();
        let after: HashMap<ChunkId, u32> = repo
            .chunk_index()
            .iter()
            .map(|(id, e)| (id, e.refcount))
            .collect();
        assert_eq!(after, oracle, "encrypted={encrypted}");
        let report = fx.report().unwrap();
        assert!(report.unreferenced.is_empty());
        assert_eq!(report.refcount_mismatches, 0);
    }
}
