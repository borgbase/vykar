//! Readers against storage that refuses every write (04 step 7's done
//! criterion "restore succeeds against storage that refuses every write",
//! extended): readers register no session marker and take no lock, so a
//! restore, an item listing, find, diff and check open and run with
//! read-only storage access and attempt no write at all.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use vykar_storage::local_backend::LocalBackend;
use vykar_storage::{InnerBackend, StorageBackend, delegate_storage_backend};
use vykar_types::error::{Result, VykarError};

use super::mark::Fx;
use crate::commands;
use crate::commands::find::FindFilter;
use crate::commands::list::{ItemStreamReader, LISTING_BATCH_CHUNKS, LISTING_BATCH_STORED_BYTES};
use crate::commands::restore::RestoreOptions;
use crate::repo::control::CancelToken;
use crate::repo::{OpenOptions, Repo, TreeChunkSource};

/// Refuses (and counts) every write: PUTs of any kind, staged writes,
/// deletes, directory creation and the temp-file sweep.
struct RefuseWrites {
    inner: LocalBackend,
    writes: Arc<AtomicUsize>,
}

impl RefuseWrites {
    fn refuse(&self, what: &str, key: &str) -> VykarError {
        self.writes.fetch_add(1, Ordering::SeqCst);
        VykarError::Io(std::io::Error::new(
            std::io::ErrorKind::PermissionDenied,
            format!("read-only storage: {what} {key}"),
        ))
    }
}

impl InnerBackend for RefuseWrites {
    fn inner_backend(&self) -> &dyn StorageBackend {
        &self.inner
    }
}

delegate_storage_backend! {
    for RefuseWrites;
    except [
        put, delete, create_dir, put_owned, put_pack, begin_staged_put,
        remove_stale_temp_files, conditional, batch_delete, profile
    ];

    fn conditional(&self) -> Option<&dyn vykar_storage::ConditionalWrite> {
        None
    }

    fn batch_delete(&self) -> Option<&dyn vykar_storage::BatchDelete> {
        None
    }

    fn profile(&self) -> vykar_storage::BackendProfile {
        vykar_storage::BackendProfile {
            staged_put: false,
            ..self.inner.profile()
        }
    }

    fn put(&self, key: &str, _data: &[u8]) -> Result<()> {
        Err(self.refuse("put", key))
    }

    fn put_owned(&self, key: &str, _data: Vec<u8>) -> Result<()> {
        Err(self.refuse("put", key))
    }

    fn put_pack(&self, key: &str, _data: vykar_storage::PackData) -> Result<()> {
        Err(self.refuse("put", key))
    }

    fn begin_staged_put(&self, prefix: &str) -> Result<Option<Box<dyn vykar_storage::StagedPut>>> {
        Err(self.refuse("staged put", prefix))
    }

    fn delete(&self, key: &str) -> Result<()> {
        Err(self.refuse("delete", key))
    }

    fn create_dir(&self, key: &str) -> Result<()> {
        Err(self.refuse("mkdir", key))
    }

    fn remove_stale_temp_files(&self, _older_than: std::time::Duration) -> Result<usize> {
        Err(self.refuse("sweep", "temp files"))
    }
}

/// An encrypted repository (both key copies present, so the open has
/// nothing to backfill) with `s1` and `s2`.
fn fixture() -> Fx {
    let fx = Fx::new();
    let one = fx.src("one");
    std::fs::write(one.join("a.txt"), b"first file").unwrap();
    std::fs::create_dir_all(one.join("sub")).unwrap();
    std::fs::write(one.join("sub/b.txt"), b"second file").unwrap();
    fx.backup(&one, "s1");
    std::fs::write(one.join("c.txt"), b"third file").unwrap();
    fx.backup(&one, "s2");
    fx
}

fn open_read_only(fx: &Fx, writes: &Arc<AtomicUsize>) -> Repo {
    let backend = RefuseWrites {
        inner: LocalBackend::new(fx.repo_dir.to_str().unwrap()).unwrap(),
        writes: Arc::clone(writes),
    };
    commands::util::open_repo_on(
        Box::new(backend),
        &commands::util::open_request(&fx.config),
        fx.pass,
        OpenOptions::new(),
    )
    .unwrap()
}

#[test]
fn readers_work_against_storage_that_refuses_writes() {
    let fx = fixture();
    let writes = Arc::new(AtomicUsize::new(0));

    // Restore.
    let mut repo = open_read_only(&fx, &writes);
    let dest = fx.src("out");
    let stats = commands::restore::run_on_repo(
        &mut repo,
        "s2",
        dest.to_str().unwrap(),
        None,
        RestoreOptions {
            xattrs_enabled: false,
            verify_chunks: true,
            continue_on_error: false,
        },
        &CancelToken::new(),
    )
    .unwrap();
    assert_eq!(stats.files, 3);
    for (rel, data) in [
        ("a.txt", &b"first file"[..]),
        ("sub/b.txt", b"second file"),
        ("c.txt", b"third file"),
    ] {
        assert_eq!(std::fs::read(dest.join(rel)).unwrap(), data, "{rel}");
    }

    // Item listing.
    let mut repo = open_read_only(&fx, &writes);
    let meta = commands::list::load_snapshot_meta(&repo, "s1").unwrap();
    ItemStreamReader::new(&repo)
        .ensure_index(&mut repo)
        .unwrap();
    let mut paths = Vec::new();
    commands::list::for_each_item_by_chunk(
        &repo,
        &meta.item_ptrs,
        TreeChunkSource::CacheFirst,
        LISTING_BATCH_CHUNKS,
        LISTING_BATCH_STORED_BYTES,
        &CancelToken::new(),
        |item| {
            paths.push(item.path);
            Ok(())
        },
    )
    .unwrap();
    assert!(paths.iter().any(|p| p.ends_with("sub/b.txt")), "{paths:?}");

    // Find (its `_on_repo` form: `find::run` takes a config).
    let mut repo = open_read_only(&fx, &writes);
    let entries = repo.manifest().snapshots.clone();
    let filter = FindFilter::build(None, Some("c.txt"), None, None, None, None, None).unwrap();
    let (timelines, _) =
        commands::find::find_on_repo(&mut repo, &entries, &filter, &CancelToken::new()).unwrap();
    assert_eq!(timelines.len(), 1);

    // Diff (likewise).
    let mut repo = open_read_only(&fx, &writes);
    let base = repo.manifest().find_snapshot("s1").unwrap().clone();
    let target = repo.manifest().find_snapshot("s2").unwrap().clone();
    let (diff, _) =
        commands::diff::diff_on_repo(&mut repo, &base, &target, &CancelToken::new()).unwrap();
    assert_eq!(diff.len(), 1, "c.txt added");

    // Check.
    let mut repo = open_read_only(&fx, &writes);
    let result = commands::check::run_on_repo(
        &mut repo,
        &fx.config,
        &commands::check::CheckRequest {
            distrust_server: true,
            ..Default::default()
        },
        None,
        &CancelToken::new(),
    )
    .unwrap();
    assert!(result.errors.is_empty(), "{:?}", result.errors);

    assert_eq!(
        writes.load(Ordering::SeqCst),
        0,
        "a reader attempted a write"
    );
}
