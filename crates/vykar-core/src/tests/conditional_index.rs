//! Conditional GET of `index` against the local index blob cache: a storage
//! that answers "unchanged" lets open and commit use the local copy, and every
//! other answer leaves today's behaviour in place.

use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use vykar_storage::{delegate_storage_backend, ConditionalGet, InnerBackend, StorageBackend};
use vykar_types::chunk_id::ChunkId;
use vykar_types::error::Result;

use crate::commands;
use crate::commands::backup::{self, BackupProgressEvent};
use crate::config::VykarConfig;
use crate::index::dedup_cache;
use crate::index::ChunkIndexEntry;
use crate::repo::{OpenOptions, Repository};

const FAST: &str = "index unchanged, fast path";
const SLOW: &str = "decode index";

/// How a conditional `index` read is answered.
#[derive(Clone)]
enum Answer {
    /// Like vykar-server: 304 when the stored bytes hash to the validator.
    Honest,
    /// 304 regardless of what is stored.
    Lie,
    /// 200 with these bytes regardless of what is stored.
    Replay(Arc<Vec<u8>>),
}

#[derive(Clone)]
struct Etag {
    full_index_gets: Arc<AtomicUsize>,
    not_modified: Arc<AtomicUsize>,
    /// Conditional reads numbered from 0 (open, then commit) answer
    /// `Honest` before this one and `late` from it on.
    switch_at: usize,
    late: Answer,
}

impl Etag {
    fn honest() -> Self {
        Self::switching_at(usize::MAX, Answer::Honest)
    }

    fn switching_at(switch_at: usize, late: Answer) -> Self {
        Self {
            full_index_gets: Arc::default(),
            not_modified: Arc::default(),
            switch_at,
            late,
        }
    }

    fn counts(&self) -> (usize, usize) {
        (
            self.full_index_gets.load(Ordering::SeqCst),
            self.not_modified.load(Ordering::SeqCst),
        )
    }

    fn wrap(&self, inner: Box<dyn StorageBackend>) -> Box<dyn StorageBackend> {
        Box::new(EtagServer {
            inner,
            etag: self.clone(),
            cond_calls: AtomicUsize::new(0),
        })
    }
}

/// Emulates a vykar-server's conditional GET of `index` over any backend.
struct EtagServer {
    inner: Box<dyn StorageBackend>,
    etag: Etag,
    cond_calls: AtomicUsize,
}

impl EtagServer {
    fn fetched(&self, key: &str, data: Option<Vec<u8>>) -> ConditionalGet {
        if key == "index" {
            self.etag.full_index_gets.fetch_add(1, Ordering::SeqCst);
        }
        ConditionalGet::Fetched(data)
    }

    fn not_modified(&self) -> ConditionalGet {
        self.etag.not_modified.fetch_add(1, Ordering::SeqCst);
        ConditionalGet::Unchanged
    }
}

impl InnerBackend for EtagServer {
    fn inner_backend(&self) -> &dyn StorageBackend {
        &*self.inner
    }
}

delegate_storage_backend! {
    for EtagServer;
    except [get, get_if_changed];

    fn get(&self, key: &str) -> Result<Option<Vec<u8>>> {
        if key == "index" {
            self.etag.full_index_gets.fetch_add(1, Ordering::SeqCst);
        }
        self.inner.get(key)
    }

    fn get_if_changed(&self, key: &str, known_blake3: &[u8; 32]) -> Result<ConditionalGet> {
        let call = self.cond_calls.fetch_add(1, Ordering::SeqCst);
        let answer = if call >= self.etag.switch_at {
            self.etag.late.clone()
        } else {
            Answer::Honest
        };
        match answer {
            Answer::Lie => Ok(self.not_modified()),
            Answer::Replay(bytes) => Ok(self.fetched(key, Some(bytes.to_vec()))),
            Answer::Honest => {
                let data = self.inner.get(key)?;
                match &data {
                    Some(bytes) if blake3::hash(bytes).as_bytes() == known_blake3 => {
                        Ok(self.not_modified())
                    }
                    _ => Ok(self.fetched(key, data)),
                }
            }
        }
    }
}

struct Fixture {
    dir: tempfile::TempDir,
    config: VykarConfig,
    repo_dir: PathBuf,
    cache_dir: PathBuf,
    src: PathBuf,
}

impl Fixture {
    /// A repository with snapshot `s1`, its caches in a private cache dir.
    fn new() -> Self {
        let dir = tempfile::tempdir().unwrap();
        let repo_dir = dir.path().join("repo");
        let cache_dir = dir.path().join("cache");
        let src = dir.path().join("src");
        fs::create_dir_all(&src).unwrap();
        fs::write(src.join("a.bin"), vec![0x11u8; 64 * 1024]).unwrap();
        let mut config = crate::tests::helpers::init_repo(&repo_dir);
        config.cache_dir = Some(cache_dir.to_string_lossy().into_owned());
        let fx = Self {
            dir,
            config,
            repo_dir,
            cache_dir,
            src,
        };
        fx.backup("s1", None);
        fx
    }

    /// Back up `src` under `name`, returning the commit stages.
    fn backup(&self, name: &str, etag: Option<&Etag>) -> Vec<&'static str> {
        let source_paths = vec![self.src.to_string_lossy().to_string()];
        let mut stages = Vec::new();
        let mut on_event = |e: BackupProgressEvent| {
            if let BackupProgressEvent::CommitStage { stage } = e {
                stages.push(stage);
            }
        };
        let wrap = |inner: Box<dyn StorageBackend>| match etag {
            Some(etag) => etag.wrap(inner),
            None => inner,
        };
        backup::run_impl(
            &self.config,
            backup::BackupRequest {
                snapshot_name: name,
                passphrase: None,
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
            Some(&mut on_event),
            None,
            Some(&wrap),
        )
        .unwrap();
        stages
    }

    fn local(&self) -> Box<dyn StorageBackend> {
        Box::new(
            vykar_storage::local_backend::LocalBackend::new(self.repo_dir.to_str().unwrap())
                .unwrap(),
        )
    }

    fn open(&self, storage: Box<dyn StorageBackend>, cache_dir: &Path) -> Repository {
        Repository::open(
            storage,
            None,
            Some(cache_dir.to_path_buf()),
            OpenOptions::new().with_index(),
        )
        .unwrap()
    }

    /// Open through `etag` with this client's cache dir.
    fn open_via(&self, etag: &Etag) -> Repository {
        self.open(etag.wrap(self.local()), &self.cache_dir)
    }

    /// Open over plain local storage, with a throwaway cache dir.
    fn open_fresh(&self) -> Repository {
        let cache = tempfile::tempdir_in(self.dir.path()).unwrap();
        self.open(self.local(), cache.path())
    }

    fn repo_id(&self) -> Vec<u8> {
        self.open_fresh().config.id.clone()
    }

    fn blob_cache_path(&self) -> PathBuf {
        dedup_cache::index_blob_cache_path(&self.repo_id(), Some(&self.cache_dir)).unwrap()
    }

    /// Another client (its own cache dir) rewrites `index` at a new
    /// generation, bumping one refcount when `bump` is set.
    fn rewrite_index_elsewhere(&self, bump: bool) -> Repository {
        let cache = self.dir.path().join("other-cache");
        let mut repo = self.open(self.local(), &cache);
        let first = *repo.chunk_index().iter().next().unwrap().0;
        let index = repo.chunk_index_mut();
        if bump {
            index.increment_refcount(&first);
        }
        repo.save_state().unwrap();
        repo
    }

    /// Copy repository and cache dir, for a scenario that must start from
    /// the same state as another.
    fn fork(&self) -> Self {
        let dir = tempfile::tempdir().unwrap();
        let repo_dir = dir.path().join("repo");
        let cache_dir = dir.path().join("cache");
        copy_dir_all(&self.repo_dir, &repo_dir);
        copy_dir_all(&self.cache_dir, &cache_dir);
        let mut config = self.config.clone();
        config.repository.url = repo_dir.to_string_lossy().into_owned();
        config.cache_dir = Some(cache_dir.to_string_lossy().into_owned());
        Self {
            dir,
            config,
            repo_dir,
            cache_dir,
            src: self.src.clone(),
        }
    }

    fn assert_check_clean(&self) {
        let result = commands::check::run(&self.config, None, true, false).unwrap();
        assert!(
            result.errors.is_empty(),
            "check errors: {:?}",
            result.errors
        );
    }
}

fn copy_dir_all(from: &Path, to: &Path) {
    fs::create_dir_all(to).unwrap();
    for entry in fs::read_dir(from).unwrap() {
        let entry = entry.unwrap();
        let target = to.join(entry.file_name());
        if entry.file_type().unwrap().is_dir() {
            copy_dir_all(&entry.path(), &target);
        } else {
            fs::copy(entry.path(), &target).unwrap();
        }
    }
}

fn entries(repo: &Repository) -> HashMap<ChunkId, ChunkIndexEntry> {
    repo.chunk_index().iter().map(|(id, e)| (*id, *e)).collect()
}

#[test]
fn second_backup_uses_304_at_open_and_commit() {
    let fx = Fixture::new();
    fs::write(fx.src.join("b.bin"), vec![0x22u8; 64 * 1024]).unwrap();

    let etag = Etag::honest();
    let stages = fx.backup("s2", Some(&etag));

    // No unconditional `index` read is left on this path: the key-proof read
    // (`identity::prove_key`) runs only for diverging key copies, and the
    // reload fallbacks only when a cache is unusable.
    assert_eq!(etag.counts(), (0, 2), "(full GETs, 304s)");
    assert!(stages.contains(&FAST), "expected fast path: {stages:?}");
    fx.assert_check_clean();
}

#[test]
fn open_after_304_matches_remote_index() {
    let fx = Fixture::new();
    let etag = Etag::honest();
    let via_etag = fx.open_via(&etag);
    let plain = fx.open_fresh();

    assert_eq!(etag.counts(), (0, 1));
    assert_eq!(via_etag.index_generation(), plain.index_generation());
    assert_eq!(entries(&via_etag), entries(&plain));
}

#[test]
fn corrupt_cached_payload_falls_back_at_open_but_not_at_commit() {
    let fx = Fixture::new();
    let path = fx.blob_cache_path();
    let mut cached = fs::read(&path).unwrap();
    *cached.last_mut().unwrap() ^= 0x01;
    fs::write(&path, &cached).unwrap();
    fs::write(fx.src.join("b.bin"), vec![0x22u8; 64 * 1024]).unwrap();

    let etag = Etag::honest();
    let stages = fx.backup("s2", Some(&etag));

    // The open verifies the payload and downloads instead; the commit only
    // needs the header digest and the full index cache (D18).
    assert_eq!(etag.counts(), (1, 2), "(full GETs, 304s)");
    assert!(stages.contains(&FAST), "expected fast path: {stages:?}");
    fx.assert_check_clean();
}

/// A 304 for a cache at another generation than the session's: the verified
/// local bytes go through the existing comparison, which fails, so the slow
/// path runs on them.
#[test]
fn not_modified_at_other_generation_takes_the_slow_path() {
    let fx = Fixture::new();
    let cached = dedup_cache::CachedIndexBlob::open(&fx.repo_id(), Some(&fx.cache_dir)).unwrap();
    let generation = cached.generation();
    let payload = cached.read_verified().unwrap();
    // Same payload (so the same digest), another generation in the header.
    dedup_cache::write_index_blob_cache(
        &payload,
        generation + 1,
        &fx.repo_id(),
        Some(&fx.cache_dir),
    )
    .unwrap();
    fs::write(fx.src.join("b.bin"), vec![0x22u8; 64 * 1024]).unwrap();

    let etag = Etag::honest();
    let stages = fx.backup("s2", Some(&etag));

    assert_eq!(etag.counts(), (0, 2), "(full GETs, 304s)");
    assert!(stages.contains(&SLOW), "expected slow path: {stages:?}");
    assert!(!stages.contains(&FAST), "unexpected fast path: {stages:?}");
    fx.assert_check_clean();
}

/// A 304 at the session's generation without a full index cache decodes the
/// verified local bytes; nothing is downloaded.
#[test]
fn not_modified_without_full_cache_decodes_the_local_copy() {
    let fx = Fixture::new();
    let full = dedup_cache::full_index_cache_path(&fx.repo_id(), Some(&fx.cache_dir)).unwrap();
    fs::remove_file(&full).unwrap();
    fs::write(fx.src.join("b.bin"), vec![0x22u8; 64 * 1024]).unwrap();

    let etag = Etag::honest();
    let stages = fx.backup("s2", Some(&etag));

    assert_eq!(etag.counts(), (0, 2), "(full GETs, 304s)");
    assert!(stages.contains(&SLOW), "expected slow path: {stages:?}");
    fx.assert_check_clean();
}

#[test]
fn index_written_elsewhere_is_downloaded() {
    let fx = Fixture::new();
    let other = fx.rewrite_index_elsewhere(false);

    let etag = Etag::honest();
    let repo = fx.open_via(&etag);

    assert_eq!(etag.counts(), (1, 0));
    assert_eq!(repo.index_generation(), other.index_generation());
}

/// Characterises the trust argument: a 304 can only make the client use the
/// index it last wrote itself, never a fabricated one.
#[test]
fn lying_304_only_replays_the_clients_own_index() {
    let fx = Fixture::new();
    let own = fx.open_fresh();
    let other = fx.rewrite_index_elsewhere(true);
    assert_ne!(entries(&own), entries(&other));

    let etag = Etag::switching_at(0, Answer::Lie);
    let repo = fx.open_via(&etag);

    assert_eq!(etag.counts(), (0, 1));
    assert_eq!(repo.index_generation(), own.index_generation());
    assert_eq!(entries(&repo), entries(&own));
}

/// A lying 304 at commit, after another client rewrote `index`, must end
/// exactly where a server replaying this client's last index as a 200 would:
/// both drop the other writer's change, and neither does anything more. Run
/// with the lie from the open on (fast path) and at the commit only (slow
/// path on the replayed bytes).
#[test]
fn lying_304_at_commit_equals_a_replayed_200() {
    let fx = Fixture::new();
    let own_bytes = Arc::new(fs::read(fx.repo_dir.join("index")).unwrap());
    let own = entries(&fx.open_fresh());
    let other = fx.rewrite_index_elsewhere(true);
    let bumped = entries(&other)
        .into_iter()
        .find(|(id, e)| own.get(id).is_some_and(|o| o.refcount != e.refcount))
        .map(|(id, _)| id)
        .unwrap();
    fs::write(fx.src.join("b.bin"), vec![0x22u8; 64 * 1024]).unwrap();

    // Honest server: the other writer's bump survives the commit.
    let honest = fx.fork();
    let honest_stages = honest.backup("s2", Some(&Etag::honest()));
    assert!(honest_stages.contains(&SLOW), "{honest_stages:?}");
    let after_honest = entries(&honest.open_fresh());

    for (switch_at, expected_stage) in [(0, FAST), (1, SLOW)] {
        let lie = fx.fork();
        let lie_etag = Etag::switching_at(switch_at, Answer::Lie);
        let lie_stages = lie.backup("s2", Some(&lie_etag));

        let replay = fx.fork();
        let replay_etag = Etag::switching_at(switch_at, Answer::Replay(Arc::clone(&own_bytes)));
        let replay_stages = replay.backup("s2", Some(&replay_etag));

        assert!(
            lie_stages.contains(&expected_stage),
            "switch at {switch_at}: {lie_stages:?}"
        );
        assert_eq!(lie_stages, replay_stages, "switch at {switch_at}");
        // The honest open (switch at 1) downloads the other writer's index.
        assert_eq!(lie_etag.counts(), (switch_at, 2 - switch_at));
        assert_eq!(replay_etag.counts(), (2, 0), "switch at {switch_at}");

        let after_lie = entries(&lie.open_fresh());
        let after_replay = entries(&replay.open_fresh());
        assert_eq!(after_lie, after_replay, "switch at {switch_at}");
        // Both lose exactly the other writer's bump, nothing else.
        assert_eq!(
            after_lie[&bumped].refcount + 1,
            after_honest[&bumped].refcount,
            "switch at {switch_at}"
        );
        assert_eq!(after_lie.len(), after_honest.len(), "switch at {switch_at}");
    }
}

#[test]
fn no_blob_cache_sends_plain_get() {
    let fx = Fixture::new();
    fs::remove_file(fx.blob_cache_path()).unwrap();

    let etag = Etag::honest();
    fx.open_via(&etag);

    assert_eq!(etag.counts(), (1, 0));
}
