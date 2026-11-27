//! Opening and reading repositories in the segmented index formats (v6).

use std::sync::{Arc, Mutex};

use crate::repo::ChunkerParams;
use crate::repo::format::ObjectType;
use crate::repo::identity::{self, KeyProof};
use crate::repo::index_store::INDEX_OBJECT_DIR;
use crate::repo::index_store::tests::{Writer, entries, index_with};
use crate::repo::{EncryptionMode, OpenOptions, Repo, RepoFormat};
use crate::testutil::MemoryBackend;
use vykar_crypto::key::{EncryptedKey, MasterKey};
use vykar_storage::{InnerBackend, StorageBackend, delegate_storage_backend};
use vykar_types::error::Result;

/// Logs `get` and `list` calls in order.
struct OpLog {
    inner: Arc<dyn StorageBackend>,
    ops: Arc<Mutex<Vec<String>>>,
}

impl InnerBackend for OpLog {
    fn inner_backend(&self) -> &dyn StorageBackend {
        &*self.inner
    }
}

delegate_storage_backend! {
    for OpLog;
    except [get, list, list_sized, list_meta, server_ops];

    fn list_meta(
        &self,
        prefix: &str,
    ) -> vykar_types::error::Result<Vec<vykar_storage::ListedObject>> {
        self.list_sized(prefix)
    }

    fn get(&self, key: &str) -> Result<Option<Vec<u8>>> {
        self.ops.lock().unwrap().push(format!("get {key}"));
        self.inner.get(key)
    }

    fn list(&self, prefix: &str) -> Result<Vec<String>> {
        self.ops.lock().unwrap().push(format!("list {prefix}"));
        self.inner.list(prefix)
    }

    fn list_sized(&self, prefix: &str) -> Result<Vec<vykar_storage::ListedObject>> {
        self.ops.lock().unwrap().push(format!("list {prefix}"));
        self.inner.list_sized(prefix)
    }

    // No server behind the inner backend: listings stay on `list_sized`.
    fn server_ops(&self) -> Option<&dyn vykar_storage::ServerOps> {
        None
    }
}

struct Env {
    inner: Arc<dyn StorageBackend>,
    cache: tempfile::TempDir,
    repo: Repo,
}

fn init(format: RepoFormat) -> Env {
    crate::testutil::init_test_environment();
    let inner: Arc<dyn StorageBackend> = Arc::new(MemoryBackend::new());
    let cache = tempfile::tempdir().unwrap();
    let repo = Repo::init_at_format(
        Box::new(Arc::clone(&inner)),
        EncryptionMode::Aes256Gcm,
        ChunkerParams::default(),
        Some("pw"),
        None,
        Some(cache.path().to_path_buf()),
        format,
    )
    .unwrap();
    Env { inner, cache, repo }
}

impl Env {
    fn open_logged(&self, opts: OpenOptions) -> (Repo, Vec<String>) {
        let ops = Arc::new(Mutex::new(Vec::new()));
        let storage = Box::new(OpLog {
            inner: Arc::clone(&self.inner),
            ops: Arc::clone(&ops),
        });
        let repo = Repo::open(
            storage,
            Some("pw"),
            Some(self.cache.path().to_path_buf()),
            opts,
        )
        .unwrap();
        let ops = ops.lock().unwrap().clone();
        (repo, ops)
    }

    fn writer(&self) -> Writer {
        Writer::new(Arc::clone(&self.inner), Arc::clone(self.repo.crypto()))
    }
}

fn index_d_gets(ops: &[String]) -> usize {
    ops.iter()
        .filter(|o| o.starts_with(&format!("get {INDEX_OBJECT_DIR}")))
        .count()
}

#[test]
fn init_writes_an_empty_head_and_no_sidecar() {
    let env = init(RepoFormat::CURRENT);
    let index = env.inner.get("index.h/1").unwrap().unwrap();
    assert_eq!(index[0], ObjectType::IndexHead as u8);
    assert!(env.inner.get("index").unwrap().is_none());
    assert!(env.inner.get("index.gen").unwrap().is_none());
    assert_eq!(env.repo.index_generation(), 1);

    let (repo, _) = env.open_logged(OpenOptions::new().with_index());
    assert_eq!(repo.format(), RepoFormat::CURRENT);
    assert!(repo.chunk_index().is_empty());
    assert_eq!(repo.index_generation(), env.repo.index_generation());
}

/// The head is listed and read exactly once, after the snapshot listing,
/// with or without the index; `index` and `index.gen` are never read.
#[test]
fn open_lists_snapshots_before_the_single_head_get() {
    let env = init(RepoFormat::CURRENT);
    let mut w = env.writer();
    w.commit(&index_with(0..20));
    for opts in [OpenOptions::new(), OpenOptions::new().with_index()] {
        let (repo, ops) = env.open_logged(opts);
        let pos = |op: &str| {
            ops.iter()
                .position(|o| o == op)
                .unwrap_or_else(|| panic!("{op}: {ops:?}"))
        };
        let head_get = format!("get index.h/{}", w.head.generation);
        assert!(pos("list snapshots/") < pos("list index.h/"), "{ops:?}");
        assert!(pos("list index.h/") < pos(&head_get), "{ops:?}");
        assert_eq!(
            ops.iter().filter(|o| o.starts_with("get index.h/")).count(),
            1,
            "{ops:?}"
        );
        assert!(
            !ops.iter().any(|o| o == "get index.gen" || o == "get index"),
            "{ops:?}"
        );
        assert_eq!(repo.index_generation(), w.head.generation);
    }
}

#[test]
fn snapshot_reader_loads_the_head_captured_at_open() {
    let env = init(RepoFormat::CURRENT);
    let mut writer = env.writer();
    let opened_index = index_with(0..20);
    writer.commit(&opened_index);
    let (mut repo, _) = env.open_logged(OpenOptions::new());
    writer.commit(&index_with(0..30));

    let ops = Arc::new(Mutex::new(Vec::new()));
    repo.set_storage_for_test(Arc::new(OpLog {
        inner: Arc::clone(&env.inner),
        ops: Arc::clone(&ops),
    }));
    crate::commands::list::ItemStreamReader::new(&repo)
        .ensure_index(&mut repo)
        .unwrap();

    assert_eq!(entries(repo.chunk_index()), entries(&opened_index));
    let ops = ops.lock().unwrap();
    assert!(!ops.iter().any(|op| op == "get index"), "{ops:?}");
    assert_eq!(index_d_gets(&ops), 1);
}

/// An open with the index builds it from base and segments, then reuses
/// and catches up the local full cache on later opens.
#[test]
fn open_with_index_loads_and_catches_up_the_local_cache() {
    for format in [
        RepoFormat::V6(vykar_types::hash::HashAlgorithm::Blake2b),
        RepoFormat::CURRENT,
    ] {
        let env = init(format);
        let mut w = env.writer();
        w.commit(&index_with(0..100));
        w.fold();
        let s2 = index_with(50..150);
        w.commit(&s2);

        let (repo, ops) = env.open_logged(OpenOptions::new().with_index());
        assert_eq!(entries(repo.chunk_index()), entries(&s2));
        assert_eq!(index_d_gets(&ops), 2);
        let dir =
            crate::cache_paths::repo_cache_dir(&repo.config().id, Some(env.cache.path())).unwrap();
        let path = crate::index::table::base_table_path(&dir, &w.head.base.as_ref().unwrap().name);
        assert!(
            path.exists(),
            "the downloaded base is kept for the next open"
        );

        let (repo, ops) = env.open_logged(OpenOptions::new().with_index());
        assert_eq!(entries(repo.chunk_index()), entries(&s2));
        assert_eq!(index_d_gets(&ops), 0);

        let s3 = index_with(60..170);
        w.commit(&s3);
        let (mut repo, ops) = env.open_logged(OpenOptions::new().with_index());
        assert_eq!(entries(repo.chunk_index()), entries(&s3));
        assert_eq!(index_d_gets(&ops), 1, "only the new segment");

        // Lazy load and the maintenance load see later heads too.
        let s4 = index_with(70..180);
        w.commit(&s4);
        repo.load_chunk_index().unwrap();
        assert_eq!(entries(repo.chunk_index()), entries(&s4));
        let s5 = index_with(80..190);
        w.commit(&s5);
        repo.load_latest_chunk_index().unwrap();
        assert_eq!(entries(repo.chunk_index()), entries(&s5));
        assert_eq!(repo.index_generation(), w.head.generation);
    }
}

/// `check`'s load fetches every object of the chain and leaves the local
/// cache alone; the static fetch used by readers that cannot borrow the
/// repository builds the same index.
#[test]
fn uncached_and_static_loads_fetch_the_whole_chain() {
    let env = init(RepoFormat::CURRENT);
    let mut w = env.writer();
    w.commit(&index_with(0..30));
    w.fold();
    let s2 = index_with(10..40);
    w.commit(&s2);
    let (_, _) = env.open_logged(OpenOptions::new().with_index());

    let ops = Arc::new(Mutex::new(Vec::new()));
    let logged = OpLog {
        inner: Arc::clone(&env.inner),
        ops: Arc::clone(&ops),
    };
    let index = Repo::fetch_full_index(&logged, env.repo.crypto().as_ref()).unwrap();
    assert_eq!(entries(&index), entries(&s2));
    assert_eq!(index_d_gets(&ops.lock().unwrap()), 2);

    let (mut repo, _) = env.open_logged(OpenOptions::new());
    let ops = Arc::new(Mutex::new(Vec::new()));
    repo.set_storage_for_test(Arc::new(OpLog {
        inner: Arc::clone(&env.inner),
        ops: Arc::clone(&ops),
    }));
    repo.load_chunk_index_uncached().unwrap();
    assert_eq!(entries(repo.chunk_index()), entries(&s2));
    assert_eq!(index_d_gets(&ops.lock().unwrap()), 2);
}

/// A v6 repository never reads `index`: an object there (debris of an
/// upgrade, or anything else) changes nothing.
#[test]
fn an_object_at_index_is_ignored() {
    let env = init(RepoFormat::CURRENT);
    let legacy = crate::tests::helpers::legacy_index_blob(
        &crate::index::LegacyChunkIndex::new(),
        7,
        env.repo.crypto().as_ref(),
    );
    env.inner.put("index", &legacy).unwrap();
    let (repo, ops) = env.open_logged(OpenOptions::new().with_index());
    assert_eq!(repo.index_head(), env.repo.index_head());
    assert!(!ops.iter().any(|o| o == "get index"), "{ops:?}");
}

/// The key proof accepts the latest head under `index.h/` as
/// repository-authored data: a v6 repository with no snapshots and no pin
/// still proves its key (R2).
#[test]
fn prove_key_accepts_an_index_head() {
    let env = init(RepoFormat::CURRENT);
    let blob = env.inner.get(crate::repo::KEY_PRIMARY).unwrap().unwrap();
    let enc: EncryptedKey = rmp_serde::from_slice(&blob).unwrap();
    let key = MasterKey::from_encrypted(&enc, "pw").unwrap();
    let proof = identity::prove_key(
        env.inner.as_ref(),
        env.repo.config(),
        RepoFormat::CURRENT,
        None,
        None,
        &key,
    )
    .unwrap();
    assert_eq!(proof, KeyProof::AuthenticatedData);

    let other = MasterKey::generate().unwrap();
    let proof = identity::prove_key(
        env.inner.as_ref(),
        env.repo.config(),
        RepoFormat::CURRENT,
        None,
        None,
        &other,
    )
    .unwrap();
    assert_eq!(proof, KeyProof::Unproven);

    // Encrypted repositories read the segmented index as well.
    let mut w = env.writer();
    let s1 = index_with(0..25);
    w.commit(&s1);
    w.fold();
    let (repo, _) = env.open_logged(OpenOptions::new().with_index());
    assert_eq!(entries(repo.chunk_index()), entries(&s1));
}

/// Writes on an encrypted repository: a session's chunks land in a segment
/// (or a base), and other clients read them back.
#[test]
fn encrypted_writes_round_trip() {
    use crate::compress::Compression;
    use crate::repo::WriteSessionOptions;
    use crate::repo::pack::PackType;

    for format in [
        RepoFormat::V6(vykar_types::hash::HashAlgorithm::Blake2b),
        RepoFormat::CURRENT,
    ] {
        let mut env = init(format);
        for round in 0..3u8 {
            let mut ws = env
                .repo
                .begin_write_session(&WriteSessionOptions::default());
            for i in 0..5u8 {
                ws.store_chunk(
                    &env.repo,
                    &[round, i, 7, 7, 7],
                    Compression::None,
                    PackType::Data,
                )
                .unwrap();
            }
            env.repo.unchecked_writer().save_session(ws).unwrap();
        }
        let head = env.repo.index_head().unwrap().clone();
        assert_eq!(env.repo.chunk_index().len(), 15);
        assert_eq!(head.segments.iter().map(|s| s.entries).sum::<u64>(), 15);
        let (repo, _) = env.open_logged(OpenOptions::new().with_index());
        assert_eq!(repo.index_head(), Some(&head));
        assert_eq!(entries(repo.chunk_index()), entries(env.repo.chunk_index()));

        let cold = tempfile::tempdir().unwrap();
        let repo = Repo::open(
            Box::new(Arc::clone(&env.inner)),
            Some("pw"),
            Some(cold.path().to_path_buf()),
            OpenOptions::new().with_index(),
        )
        .unwrap();
        assert_eq!(entries(repo.chunk_index()), entries(env.repo.chunk_index()));
    }
}
