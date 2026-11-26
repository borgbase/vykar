//! The backup's diagnostic counters (`BackupOutcome::instrumentation`)
//! against what reached the repository.

use std::fs;
use std::path::Path;
use std::time::Duration;

use super::helpers::pass;
use crate::commands::backup::BackupOutcome;
use crate::repo::memory_budget::MemorySnapshot;
use crate::tests::helpers::{make_test_config, try_backup_single_source};
use vykar_storage::StorageBackend;
use vykar_types::error::Result;

/// Six 8 KiB files of pseudo-random content (no chunk repeats within or
/// across files), all above the test chunker's `min_size`, so none takes the
/// whole-file small read.
fn write_distinct_files(src: &Path) {
    for i in 0..6u64 {
        let mut x = 0x9e37_79b9_7f4a_7c15 ^ (i + 1);
        let data: Vec<u8> = (0..8 * 1024)
            .map(|_| {
                x ^= x << 13;
                x ^= x >> 7;
                x ^= x << 17;
                x.to_le_bytes()[0]
            })
            .collect();
        fs::write(src.join(format!("f{i}.bin")), data).unwrap();
    }
}

fn backup_with_threads(dir: &Path, threads: usize, small_files: bool) -> BackupOutcome {
    let repo_dir = dir.join("repo");
    let src = dir.join("src");
    fs::create_dir_all(&src).unwrap();
    let mut config = make_test_config(&repo_dir);
    config.limits.threads = threads;
    config.chunker.min_size = 1024;
    config.chunker.avg_size = 2048;
    config.chunker.max_size = 4096;
    crate::commands::init::run(&config, pass(&config)).unwrap();

    write_distinct_files(&src);
    if small_files {
        for i in 0..3u8 {
            fs::write(src.join(format!("s{i}.txt")), vec![i; 100]).unwrap();
        }
    }
    try_backup_single_source(&config, &src, "test", "snap").unwrap()
}

/// (count, total bytes) of the pack objects under `<repo>/packs/<shard>/`.
fn packs_on_disk(repo_dir: &Path) -> (u64, u64) {
    let mut count = 0;
    let mut bytes = 0;
    for shard in fs::read_dir(repo_dir.join("packs")).unwrap() {
        let shard = shard.unwrap();
        if !shard.file_type().unwrap().is_dir() {
            continue;
        }
        for pack in fs::read_dir(shard.path()).unwrap() {
            let pack = pack.unwrap();
            count += 1;
            bytes += pack.metadata().unwrap().len();
        }
    }
    (count, bytes)
}

#[test]
fn pack_counters_match_storage() {
    for threads in [1, 2] {
        let dir = tempfile::tempdir().unwrap();
        let outcome = backup_with_threads(dir.path(), threads, true);
        let m = &outcome.instrumentation;
        let w = &m.write;

        let (count, bytes) = packs_on_disk(&dir.path().join("repo"));
        assert_eq!(w.data_packs + w.tree_packs, count, "threads {threads}");
        assert_eq!(
            w.data_pack_bytes + w.tree_pack_bytes,
            bytes,
            "threads {threads}"
        );
        assert_eq!(m.threads, threads);
        assert!(m.pipeline.is_some());

        let p = m.pipeline.unwrap();
        assert_eq!(p.workers, threads);
        assert!(p.consumer_recv_wait <= p.wall);
        assert!(p.consumer_busy <= p.wall);
        assert!(p.consumer_cap_wait <= w.upload_cap_wait);
        assert!(p.worker_busy > Duration::ZERO);
        assert!(p.worker_transform <= p.worker_busy);
    }
}

/// Issue 1 of plan 02: at threads = 1 the old sequential path force-sealed a
/// pack per new file above `min_size`. The pipeline seals the same packs at
/// every thread count.
#[test]
fn pack_layout_does_not_depend_on_threads() {
    let mut tree_packs = Vec::new();
    for threads in [1, 2, 4] {
        let dir = tempfile::tempdir().unwrap();
        let w = backup_with_threads(dir.path(), threads, false)
            .instrumentation
            .write;
        assert_eq!(w.data_packs, 1, "threads {threads}");
        tree_packs.push(w.tree_packs);
    }
    assert!(
        tree_packs.windows(2).all(|w| w[0] == w[1]),
        "tree packs differ across thread counts: {tree_packs:?}"
    );
}

/// The budget's accounting identities after a backup: everything released,
/// the pipeline within its cap, the total within the ceiling.
fn assert_settled(m: &MemorySnapshot, what: &str) {
    assert_eq!(m.pipeline, 0, "{what}");
    assert_eq!(m.open_packs, 0, "{what}");
    assert_eq!(m.sealed_packs, 0, "{what}");
    assert!(m.pipeline_cap > 0, "{what}");
    assert!(m.peak_pipeline > 0, "{what}");
    assert!(m.peak_pipeline <= m.pipeline_cap, "{what}: {m:?}");
    assert!(m.peak_total <= m.ceiling, "{what}: {m:?}");
}

#[test]
fn memory_budget_settles_after_backup() {
    for threads in [1, 2] {
        let dir = tempfile::tempdir().unwrap();
        let m = backup_with_threads(dir.path(), threads, true)
            .instrumentation
            .memory;
        assert_settled(&m, &format!("threads {threads}"));
        // Local: tree packs are on the heap.
        assert!(m.peak_open_packs > 0, "threads {threads}");
    }
}

/// A remote-looking backend (not local, not staged, one upload slot) whose
/// pack uploads are slow, so closed packs queue behind the slot.
struct SlowRemote {
    inner: Box<dyn StorageBackend>,
}

impl vykar_storage::InnerBackend for SlowRemote {
    fn inner_backend(&self) -> &dyn StorageBackend {
        &*self.inner
    }
}

vykar_storage::delegate_storage_backend! {
    for SlowRemote;
    except [put_pack, begin_staged_put, conditional, profile];

    // Test wrapper: keeps every write on `put`.
    fn conditional(&self) -> Option<&dyn vykar_storage::ConditionalWrite> {
        None
    }

    fn begin_staged_put(
        &self,
        _prefix: &str,
    ) -> Result<Option<Box<dyn vykar_storage::StagedPut>>> {
        Ok(None)
    }

    fn profile(&self) -> vykar_storage::BackendProfile {
        vykar_storage::BackendProfile {
            local: false,
            staged_put: false,
            transfer_width: 1,
            ..self.inner.profile()
        }
    }

    fn put_pack(&self, key: &str, data: vykar_storage::PackData) -> Result<()> {
        std::thread::sleep(Duration::from_millis(20));
        self.inner.put_pack(key, data)
    }
}

/// The real pipeline on the remote policy at threads = 1, with packs of
/// 1 KiB behind one slow upload slot: the backup completes, closed packs
/// are charged to the sealed account past `W × T` (where lending starts),
/// and the budget settles. One closed pack already exceeds `T`, so this
/// shows the remote path is wired to the budget, not that packs queued;
/// lending against the walk is covered by the budget's own tests.
#[test]
fn remote_budget_lends_sealed_packs_and_settles() {
    let dir = tempfile::tempdir().unwrap();
    let repo_dir = dir.path().join("repo");
    let src = dir.path().join("src");
    fs::create_dir_all(&src).unwrap();
    let mut config = make_test_config(&repo_dir);
    config.limits.threads = 1;
    config.chunker.min_size = 1024;
    config.chunker.avg_size = 2048;
    config.chunker.max_size = 4096;
    config.repository.min_pack_size = 1024;
    config.repository.max_pack_size = 1024;
    crate::commands::init::run(&config, pass(&config)).unwrap();
    // The wrapper below offers no put-if-absent: lock mode.
    crate::tests::helpers::make_single_writer(&repo_dir);
    write_distinct_files(&src);

    let source_paths = vec![src.to_string_lossy().to_string()];
    let wrap = |inner: Box<dyn StorageBackend>| -> Box<dyn StorageBackend> {
        Box::new(SlowRemote { inner })
    };
    let outcome = crate::commands::backup::run_impl(
        &config,
        crate::commands::backup::BackupRequest {
            snapshot_name: "snap",
            passphrase: Some(crate::testing::TEST_PASSPHRASE),
            source_paths: &source_paths,
            source_label: "test",
            exclude_patterns: &[],
            exclude_if_present: &[],
            one_file_system: true,
            git_ignore: false,
            xattrs_enabled: false,
            compression: crate::compress::Compression::None,
            command_dumps: &[],
            verbose: false,
        },
        None,
        &crate::repo::control::CancelToken::default(),
        Some(&wrap),
        crate::repo::HostEnv::system(),
    )
    .unwrap();

    let m = outcome.instrumentation.memory;
    assert_settled(&m, "remote");
    assert_eq!(m.pack_target, 1024);
    assert!(
        outcome.instrumentation.write.data_packs > 3,
        "{:?}",
        outcome.instrumentation.write
    );
    // W = 1: sealed bytes beyond one target lend pipeline capacity.
    assert!(m.peak_sealed_packs > m.pack_target, "{m:?}");
}
