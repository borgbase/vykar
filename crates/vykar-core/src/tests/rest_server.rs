//! The segmented index (v5) through `RestBackend` against an in-process
//! vykar-server, in normal and append-only mode. Opening a v5 repository at
//! all is the positive path of the `segmented_index` capability check.

use std::path::{Path, PathBuf};

use super::helpers::{backup_single_source, make_test_config};
use crate::commands;
use crate::config::VykarConfig;
use crate::index::ChunkIndex;
use crate::index::head::IndexHead;
use crate::repo::control::CancelToken;
use crate::repo::index_store::INDEX_OBJECT_DIR;
use crate::repo::index_write::FoldPolicy;
use crate::repo::{OpenOptions, RepoFormat, Repository};
use vykar_server::config::ServerSection;

const TOKEN: &str = "rest-server-test";

/// vykar-server on 127.0.0.1, serving `data` as a single repository.
/// Dropping it drops the runtime, which stops the server.
struct Server {
    // Declared first so the server stops before `data` is removed.
    _rt: tokio::runtime::Runtime,
    url: String,
    data: tempfile::TempDir,
}

impl Server {
    fn start(append_only: bool) -> Self {
        let data = tempfile::tempdir().unwrap();
        let state = vykar_server::state::AppState::new(
            ServerSection {
                data_dir: data.path().to_string_lossy().into_owned(),
                token: TOKEN.into(),
                append_only,
                ..Default::default()
            },
            Some(1 << 40),
        );
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .unwrap();
        let listener = rt
            .block_on(tokio::net::TcpListener::bind("127.0.0.1:0"))
            .unwrap();
        let url = format!("http://127.0.0.1:{}", listener.local_addr().unwrap().port());
        rt.spawn(async move {
            let _ = axum::serve(listener, vykar_server::handlers::router(state)).await;
        });
        Self { _rt: rt, url, data }
    }

    /// Files under `index.d/` in the server's data directory.
    fn index_objects(&self) -> usize {
        std::fs::read_dir(self.data.path().join(INDEX_OBJECT_DIR))
            .unwrap()
            .filter(|e| e.as_ref().unwrap().file_type().unwrap().is_file())
            .count()
    }
}

/// A client config for the server's repository, with its own cache dir.
fn client(server: &Server, tmp: &Path, cache: &str) -> VykarConfig {
    let mut config = make_test_config(tmp);
    config.repository.url.clone_from(&server.url);
    config.repository.access_token = Some(TOKEN.into());
    config.repository.allow_insecure_http = true;
    config.cache_dir = Some(tmp.join(cache).to_string_lossy().into_owned());
    config
}

fn open(config: &VykarConfig) -> Repository {
    commands::util::open_repo(config, None, OpenOptions::new()).unwrap()
}

fn head(config: &VykarConfig) -> IndexHead {
    open(config).index_head().cloned().unwrap()
}

fn sorted_entries(index: &ChunkIndex) -> Vec<String> {
    let mut v: Vec<String> = index
        .iter()
        .map(|(id, e)| format!("{id:?} {e:?}"))
        .collect();
    v.sort();
    v
}

fn entries(config: &VykarConfig) -> Vec<String> {
    let repo = commands::util::open_repo(config, None, OpenOptions::new().with_index()).unwrap();
    sorted_entries(repo.chunk_index())
}

fn check_clean(config: &VykarConfig) {
    for distrust_server in [false, true] {
        let result = commands::check::run(config, None, true, distrust_server).unwrap();
        assert!(result.errors.is_empty(), "{:?}", result.errors);
    }
}

struct Client {
    tmp: tempfile::TempDir,
    source: PathBuf,
    config: VykarConfig,
}

impl Client {
    fn init(server: &Server) -> Self {
        let tmp = tempfile::tempdir().unwrap();
        let source = tmp.path().join("src");
        std::fs::create_dir_all(&source).unwrap();
        let config = client(server, tmp.path(), "cache");
        let repo = commands::init::run(&config, None).unwrap();
        assert_eq!(repo.format(), RepoFormat::V5);
        Self {
            tmp,
            source,
            config,
        }
    }

    fn write(&self, name: &str, content: &str) {
        std::fs::write(self.source.join(name), content).unwrap();
    }

    fn backup(&self, name: &str) {
        backup_single_source(&self.config, &self.source, "src", name);
    }

    /// The index as a client with no local cache builds it.
    fn cold_entries(&self, server: &Server, cache: &str) -> Vec<String> {
        entries(&client(server, self.tmp.path(), cache))
    }
}

#[test]
fn v5_backup_fold_and_compact_through_the_server() {
    let server = Server::start(false);
    let c = Client::init(&server);
    for (name, file, content) in [
        ("s1", "a", "alpha"),
        ("s2", "b", "bravo"),
        ("s3", "c", "charlie"),
    ] {
        c.write(file, content);
        c.backup(name);
    }
    assert_eq!(head(&c.config).segments.len(), 3);
    assert_eq!(server.index_objects(), 3);

    commands::delete::run(&c.config, None, &["s1"], false, &CancelToken::new()).unwrap();
    let after_delete = head(&c.config);
    assert_eq!(after_delete.segments.len(), 4);

    let compact = || {
        let mut repo = open(&c.config);
        let mut policy = FoldPolicy::default();
        policy.soft.segments = 2;
        repo.set_fold_policy(policy);
        commands::compact::run_on_repo(&mut repo, 0.0, None, false, &CancelToken::new()).unwrap();
    };
    compact();
    let folded = head(&c.config);
    assert!(folded.segments.is_empty(), "past the soft limit");
    assert!(
        after_delete
            .live_objects()
            .all(|n| folded.retired.contains(&n)),
        "{folded:?} vs {after_delete:?}"
    );
    assert_eq!(server.index_objects(), 1 + folded.retired.len());

    let debris = server
        .data
        .path()
        .join(INDEX_OBJECT_DIR)
        .join("ab".repeat(32));
    std::fs::write(&debris, b"debris").unwrap();
    compact();
    assert!(!debris.exists(), "debris is collected");
    assert_eq!(head(&c.config), folded);
    assert_eq!(server.index_objects(), 1 + folded.retired.len());

    check_clean(&c.config);
    assert_eq!(c.cold_entries(&server, "cold"), entries(&c.config));

    let dest = c.tmp.path().join("restore");
    commands::restore::run(
        &c.config,
        None,
        "s3",
        dest.to_str().unwrap(),
        None,
        false,
        true,
        &CancelToken::new(),
    )
    .unwrap();
    let restored = find(&dest, "c");
    assert_eq!(std::fs::read_to_string(restored).unwrap(), "charlie");
}

/// The fold's `index.d/` deletes are accepted in append-only mode.
#[test]
fn v5_folds_at_the_hard_limit_on_an_append_only_server() {
    let server = Server::start(true);
    let c = Client::init(&server);
    c.write("a", "alpha");
    c.write("b", "bravo");
    c.backup("s1");

    let mut repo = open(&c.config);
    let mut policy = FoldPolicy::default();
    policy.hard.segments = 3;
    policy.hard.percent = 100_000;
    repo.set_fold_policy(policy);

    let mut counts = Vec::new();
    let mut last = None;
    for round in 0..5u32 {
        repo.load_chunk_index_under_lock().unwrap();
        let id = *repo.chunk_index().iter().next().unwrap().0;
        repo.chunk_index_mut().increment_refcount_by(&id, round + 1);
        let live: Vec<_> = repo.index_head().unwrap().live_objects().collect();
        repo.save_state().unwrap();
        let head = repo.index_head().unwrap().clone();
        assert_eq!(head, self::head(&c.config));
        if head.segments.is_empty() {
            assert_eq!(head.retired, live, "a fold retires the previous live set");
        }
        counts.push(head.segments.len());
        last = Some(head);
    }
    assert_eq!(counts, vec![2, 0, 1, 2, 0]);
    assert_eq!(server.index_objects(), 1 + last.unwrap().retired.len());
    assert_eq!(
        c.cold_entries(&server, "cold"),
        sorted_entries(repo.chunk_index())
    );
    drop(repo);
    check_clean(&c.config);
}

/// The file called `name` somewhere below `dir`.
fn find(dir: &Path, name: &str) -> PathBuf {
    fn walk(dir: &Path, name: &str) -> Option<PathBuf> {
        std::fs::read_dir(dir).ok()?.find_map(|entry| {
            let path = entry.ok()?.path();
            if path.is_dir() {
                walk(&path, name)
            } else {
                path.file_name().is_some_and(|n| n == name).then_some(path)
            }
        })
    }
    walk(dir, name).unwrap_or_else(|| panic!("{name} not found below {}", dir.display()))
}
