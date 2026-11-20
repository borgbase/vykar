//! Opening and reading repositories in the segmented index formats (v4/v5).

use std::sync::{Arc, Mutex};

use crate::config::ChunkerConfig;
use crate::index::dedup_cache::{full_index_cache_path, generation_for_state};
use crate::index::head::empty_state_hash;
use crate::repo::format::ObjectType;
use crate::repo::identity::{self, KeyProof};
use crate::repo::index_store::tests::{entries, index_with, Writer};
use crate::repo::index_store::INDEX_OBJECT_DIR;
use crate::repo::{
    derive_plaintext_chunk_id_key, EncryptionMode, OpenOptions, RepoFormat, Repository,
};
use crate::testutil::MemoryBackend;
use vykar_crypto::key::{EncryptedKey, MasterKey};
use vykar_storage::{delegate_storage_backend, InnerBackend, StorageBackend};
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
    except [get, get_if_changed, list, list_sized, list_tree_sized];

    fn get(&self, key: &str) -> Result<Option<Vec<u8>>> {
        self.ops.lock().unwrap().push(format!("get {key}"));
        self.inner.get(key)
    }

    fn get_if_changed(&self, key: &str, _known: &[u8; 32]) -> Result<vykar_storage::ConditionalGet> {
        vykar_storage::get_unconditionally(self, key)
    }

    fn list(&self, prefix: &str) -> Result<Vec<String>> {
        self.ops.lock().unwrap().push(format!("list {prefix}"));
        self.inner.list(prefix)
    }

    fn list_sized(&self, prefix: &str) -> Result<Vec<vykar_storage::ListedObject>> {
        self.ops.lock().unwrap().push(format!("list {prefix}"));
        self.inner.list_sized(prefix)
    }

    fn list_tree_sized(&self, prefix: &str) -> Result<Vec<vykar_storage::ListedObject>> {
        self.ops.lock().unwrap().push(format!("list {prefix}"));
        self.inner.list_tree_sized(prefix)
    }
}

struct Env {
    inner: Arc<dyn StorageBackend>,
    cache: tempfile::TempDir,
    repo: Repository,
}

fn init(format: RepoFormat, encryption: EncryptionMode) -> Env {
    crate::testutil::init_test_environment();
    let inner: Arc<dyn StorageBackend> = Arc::new(MemoryBackend::new());
    let cache = tempfile::tempdir().unwrap();
    let pass = (encryption != EncryptionMode::None).then_some("pw");
    let repo = Repository::init_at_format(
        Box::new(Arc::clone(&inner)),
        encryption,
        ChunkerConfig::default(),
        pass,
        None,
        Some(cache.path().to_path_buf()),
        format,
    )
    .unwrap();
    Env { inner, cache, repo }
}

impl Env {
    fn open_logged(&self, opts: OpenOptions) -> (Repository, Vec<String>) {
        let ops = Arc::new(Mutex::new(Vec::new()));
        let storage = Box::new(OpLog {
            inner: Arc::clone(&self.inner),
            ops: Arc::clone(&ops),
        });
        let pass = self.repo.crypto.is_encrypting().then_some("pw");
        let repo =
            Repository::open(storage, pass, Some(self.cache.path().to_path_buf()), opts).unwrap();
        let ops = ops.lock().unwrap().clone();
        (repo, ops)
    }

    fn writer(&self) -> Writer {
        Writer::new(Arc::clone(&self.inner), Arc::clone(&self.repo.crypto))
    }
}

fn index_d_gets(ops: &[String]) -> usize {
    ops.iter()
        .filter(|o| o.starts_with(&format!("get {INDEX_OBJECT_DIR}")))
        .count()
}

#[test]
fn init_writes_an_empty_head_and_no_sidecar() {
    let env = init(RepoFormat::V5, EncryptionMode::None);
    let index = env.inner.get("index").unwrap().unwrap();
    assert_eq!(index[0], ObjectType::IndexHead as u8);
    assert!(env.inner.get("index.gen").unwrap().is_none());
    assert_eq!(
        env.repo.index_generation(),
        generation_for_state(&empty_state_hash())
    );

    let (repo, _) = env.open_logged(OpenOptions::new().with_index());
    assert_eq!(repo.format(), RepoFormat::V5);
    assert!(repo.chunk_index().is_empty());
    assert_eq!(repo.index_generation(), env.repo.index_generation());
}

/// The head is read exactly once, after the snapshot listing, with or
/// without the index; `index.gen` is never read.
#[test]
fn open_lists_snapshots_before_the_single_head_get() {
    let env = init(RepoFormat::V5, EncryptionMode::None);
    let mut w = env.writer();
    w.commit(&index_with(0..20, 0));
    for opts in [OpenOptions::new(), OpenOptions::new().with_index()] {
        let (repo, ops) = env.open_logged(opts);
        let pos = |op: &str| {
            ops.iter()
                .position(|o| o == op)
                .unwrap_or_else(|| panic!("{op}: {ops:?}"))
        };
        assert!(pos("list snapshots/") < pos("get index"), "{ops:?}");
        assert_eq!(
            ops.iter().filter(|o| *o == "get index").count(),
            1,
            "{ops:?}"
        );
        assert!(!ops.iter().any(|o| o == "get index.gen"), "{ops:?}");
        assert_eq!(
            repo.index_generation(),
            generation_for_state(&w.head.state_hash)
        );
    }
}

#[test]
fn snapshot_reader_loads_the_head_captured_at_open() {
    let env = init(RepoFormat::V5, EncryptionMode::None);
    let mut writer = env.writer();
    let opened_index = index_with(0..20, 0);
    writer.commit(&opened_index);
    let (mut repo, _) = env.open_logged(OpenOptions::new());
    writer.commit(&index_with(0..30, 0));

    let ops = Arc::new(Mutex::new(Vec::new()));
    repo.storage = Arc::new(OpLog {
        inner: Arc::clone(&env.inner),
        ops: Arc::clone(&ops),
    });
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
    for format in [RepoFormat::V4, RepoFormat::V5] {
        let env = init(format, EncryptionMode::None);
        let mut w = env.writer();
        w.commit(&index_with(0..100, 0));
        w.fold();
        let s2 = index_with(50..150, 1);
        w.commit(&s2);

        let (repo, ops) = env.open_logged(OpenOptions::new().with_index());
        assert_eq!(entries(repo.chunk_index()), entries(&s2));
        assert_eq!(index_d_gets(&ops), 2);
        let path = full_index_cache_path(&repo.config.id, Some(env.cache.path())).unwrap();
        assert!(path.exists(), "the synced cache is kept for the next open");

        let (repo, ops) = env.open_logged(OpenOptions::new().with_index());
        assert_eq!(entries(repo.chunk_index()), entries(&s2));
        assert_eq!(index_d_gets(&ops), 0);

        let s3 = index_with(60..170, 2);
        w.commit(&s3);
        let (mut repo, ops) = env.open_logged(OpenOptions::new().with_index());
        assert_eq!(entries(repo.chunk_index()), entries(&s3));
        assert_eq!(index_d_gets(&ops), 1, "only the new segment");

        // Lazy load and the maintenance load see later heads too.
        let s4 = index_with(70..180, 3);
        w.commit(&s4);
        repo.load_chunk_index().unwrap();
        assert_eq!(entries(repo.chunk_index()), entries(&s4));
        let s5 = index_with(80..190, 4);
        w.commit(&s5);
        repo.load_chunk_index_under_lock().unwrap();
        assert_eq!(entries(repo.chunk_index()), entries(&s5));
        assert_eq!(
            repo.index_generation(),
            generation_for_state(&w.head.state_hash)
        );
    }
}

/// `check`'s load fetches every object of the chain and leaves the local
/// cache alone; the static fetch used by readers that cannot borrow the
/// repository builds the same index.
#[test]
fn uncached_and_static_loads_fetch_the_whole_chain() {
    let env = init(RepoFormat::V5, EncryptionMode::None);
    let mut w = env.writer();
    w.commit(&index_with(0..30, 0));
    w.fold();
    let s2 = index_with(10..40, 1);
    w.commit(&s2);
    let (_, _) = env.open_logged(OpenOptions::new().with_index());

    let ops = Arc::new(Mutex::new(Vec::new()));
    let logged = OpLog {
        inner: Arc::clone(&env.inner),
        ops: Arc::clone(&ops),
    };
    let (_, index) =
        Repository::fetch_full_index(&logged, env.repo.crypto.as_ref(), RepoFormat::V5).unwrap();
    assert_eq!(entries(&index), entries(&s2));
    assert_eq!(index_d_gets(&ops.lock().unwrap()), 2);

    let (mut repo, _) = env.open_logged(OpenOptions::new());
    let ops = Arc::new(Mutex::new(Vec::new()));
    repo.storage = Arc::new(OpLog {
        inner: Arc::clone(&env.inner),
        ops: Arc::clone(&ops),
    });
    repo.load_chunk_index_uncached().unwrap();
    assert_eq!(entries(repo.chunk_index()), entries(&s2));
    assert_eq!(index_d_gets(&ops.lock().unwrap()), 2);
}

/// A monolithic blob at `index` of a segmented repository (an old binary
/// overwrote the head) is named, not misread.
#[test]
fn legacy_blob_at_index_is_refused() {
    let env = init(RepoFormat::V5, EncryptionMode::None);
    let v3 = init(RepoFormat::V3, EncryptionMode::None);
    let legacy = v3.inner.get("index").unwrap().unwrap();
    env.inner.put("index", &legacy).unwrap();
    let err = Repository::open(
        Box::new(Arc::clone(&env.inner)),
        None,
        Some(env.cache.path().to_path_buf()),
        OpenOptions::new(),
    )
    .err()
    .expect("refused")
    .to_string();
    assert!(err.contains("monolithic index"), "{err}");
    assert!(err.contains("check --repair"), "{err}");
}

/// The key proof accepts a head at `index` as repository-authored data.
#[test]
fn prove_key_accepts_an_index_head() {
    let env = init(RepoFormat::V5, EncryptionMode::Aes256Gcm);
    let blob = env.inner.get(crate::repo::KEY_PRIMARY).unwrap().unwrap();
    let enc: EncryptedKey = rmp_serde::from_slice(&blob).unwrap();
    let key = MasterKey::from_encrypted(&enc, "pw").unwrap();
    let proof = identity::prove_key(
        env.inner.as_ref(),
        &env.repo.config,
        RepoFormat::V5,
        None,
        None,
        &key,
    )
    .unwrap();
    assert_eq!(proof, KeyProof::AuthenticatedData);

    let other = MasterKey::generate().unwrap();
    let proof = identity::prove_key(
        env.inner.as_ref(),
        &env.repo.config,
        RepoFormat::V5,
        None,
        None,
        &other,
    )
    .unwrap();
    assert_eq!(proof, KeyProof::Unproven);

    // Encrypted repositories read the segmented index as well.
    let mut w = env.writer();
    let s1 = index_with(0..25, 0);
    w.commit(&s1);
    w.fold();
    let (repo, _) = env.open_logged(OpenOptions::new().with_index());
    assert_eq!(entries(repo.chunk_index()), entries(&s1));
}

/// The plaintext chunk-ID key follows the chunk hash: a v2 repository keeps
/// its key as v4, a v3 one as v5.
#[test]
fn plaintext_key_follows_the_chunk_hash() {
    let id = [0x42u8; 32];
    assert_eq!(
        derive_plaintext_chunk_id_key(&id, RepoFormat::V4),
        derive_plaintext_chunk_id_key(&id, RepoFormat::V2)
    );
    assert_eq!(
        derive_plaintext_chunk_id_key(&id, RepoFormat::V5),
        derive_plaintext_chunk_id_key(&id, RepoFormat::V3)
    );
}

/// Writes on an encrypted repository: a session's chunks land in a segment
/// (or a base), and other clients read them back.
#[test]
fn encrypted_writes_round_trip() {
    use crate::compress::Compression;
    use crate::repo::pack::PackType;
    use crate::repo::WriteSessionOptions;

    for format in [RepoFormat::V4, RepoFormat::V5] {
        let mut env = init(format, EncryptionMode::Aes256Gcm);
        for round in 0..3u8 {
            env.repo
                .begin_write_session(&WriteSessionOptions::default())
                .unwrap();
            for i in 0..5u8 {
                env.repo
                    .store_chunk(&[round, i, 7, 7, 7], Compression::None, PackType::Data)
                    .unwrap();
            }
            env.repo.save_state().unwrap();
        }
        let head = env.repo.index_head().unwrap().clone();
        assert_eq!(head.entries, 15);
        let (repo, _) = env.open_logged(OpenOptions::new().with_index());
        assert_eq!(repo.index_head(), Some(&head));
        assert_eq!(entries(repo.chunk_index()), entries(env.repo.chunk_index()));

        let cold = tempfile::tempdir().unwrap();
        let repo = Repository::open(
            Box::new(Arc::clone(&env.inner)),
            Some("pw"),
            Some(cold.path().to_path_buf()),
            OpenOptions::new().with_index(),
        )
        .unwrap();
        assert_eq!(entries(repo.chunk_index()), entries(env.repo.chunk_index()));
    }
}
