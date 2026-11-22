//! The segmented index (v6) through `RestBackend` against an in-process
//! vykar-server, in normal and append-only mode. Opening a v6 repository at
//! all is the positive path of the `segmented_index` capability check.

use std::path::{Path, PathBuf};

use super::helpers::{backup_single_source, make_test_config};
use crate::commands;
use crate::config::VykarConfig;
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
    commands::util::open_repo(config, None, OpenOptions::new(), &CancelToken::new()).unwrap()
}

fn head(config: &VykarConfig) -> IndexHead {
    open(config).index_head().cloned().unwrap()
}

/// Entries without their refcounts: a repair writes multiplicity counts,
/// every other writer 1 (05 step 8), and nothing decides on them.
fn sorted_entries(index: &impl crate::testutil::EntryIter) -> Vec<String> {
    let mut v: Vec<String> = index
        .entry_vec()
        .iter()
        .map(|(id, e)| format!("{id:?} {} {} {}", e.pack_id, e.pack_offset, e.stored_size))
        .collect();
    v.sort();
    v
}

fn entries(config: &VykarConfig) -> Vec<String> {
    let repo = commands::util::open_repo(
        config,
        None,
        OpenOptions::new().with_index(),
        &CancelToken::new(),
    )
    .unwrap();
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
        assert_eq!(repo.format(), RepoFormat::CURRENT);
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
fn v6_backup_sweep_fold_and_compact_through_the_server() {
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
    assert_eq!(after_delete.segments.len(), 3, "delete writes no index");

    let compact = || {
        let mut repo = open(&c.config);
        let mut policy = FoldPolicy::default();
        policy.soft.segments = 2;
        repo.set_fold_policy(policy);
        repo.set_index_gc_grace(std::time::Duration::ZERO);
        commands::compact::run_on_repo(&mut repo, 0.0, None, false, &CancelToken::new()).unwrap();
    };
    let named_on_server = |h: &crate::index::head::IndexHead| {
        h.named_objects().all(|n| {
            server
                .data
                .path()
                .join(INDEX_OBJECT_DIR)
                .join(n.to_hex())
                .exists()
        })
    };
    compact();
    let folded = head(&c.config);
    assert!(folded.segments.is_empty(), "past the soft limit");
    assert!(folded.sweep.record.is_some());
    assert!(
        after_delete
            .live_objects()
            .all(|n| folded.retired.contains(&n)),
        "{folded:?} vs {after_delete:?}"
    );
    assert!(named_on_server(&folded));

    let debris = server
        .data
        .path()
        .join(INDEX_OBJECT_DIR)
        .join("ab".repeat(32));
    std::fs::write(&debris, b"debris").unwrap();
    compact();
    assert!(!debris.exists(), "debris is collected");
    let after = head(&c.config);
    assert_eq!(after.base, folded.base);
    assert!(named_on_server(&after) && named_on_server(&folded));

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

/// Folds at the hard limit on an append-only server: every head is a new
/// key, so nothing is overwritten.
#[test]
fn v6_folds_at_the_hard_limit_on_an_append_only_server() {
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
    let mut toggled = None;
    for _ in 0..5u32 {
        repo.load_chunk_index_under_lock().unwrap();
        crate::tests::segmented_writes::toggle_an_entry(&mut repo, &mut toggled);
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
    // Round 2 repeats round 0's removal after the fold retired it: on a
    // plaintext repository the segment's name is already named (retired),
    // so that write folds too.
    assert_eq!(counts, vec![2, 0, 0, 1, 2]);
    let last = last.unwrap();
    assert!(last.named_objects().all(|n| {
        server
            .data
            .path()
            .join(INDEX_OBJECT_DIR)
            .join(n.to_hex())
            .exists()
    }));
    assert_eq!(
        c.cold_entries(&server, "cold"),
        sorted_entries(repo.chunk_index())
    );
    // Put the removed entry back before the check.
    repo.load_chunk_index_under_lock().unwrap();
    crate::tests::segmented_writes::toggle_an_entry(&mut repo, &mut toggled);
    repo.save_state().unwrap();
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

/// The put-if-absent and `list_meta` conformance suite through `RestBackend`
/// against the server, in both modes. `locks/` is ephemeral, so the suite's
/// deletes are allowed in append-only mode too.
#[test]
fn conditional_conformance_suite_passes() {
    use vykar_storage::StorageBackend;
    for append_only in [false, true] {
        let server = Server::start(append_only);
        let backend = vykar_storage::rest_backend::RestBackend::new(
            &server.url,
            Some(TOKEN),
            vykar_storage::RetryConfig {
                max_retries: 0,
                ..Default::default()
            },
            None,
        )
        .unwrap();
        assert!(backend.conditional().is_some(), "the server advertises it");
        vykar_storage::conformance::run(
            &backend,
            &vykar_storage::conformance::ConformanceOptions::default(),
        )
        .unwrap_or_else(|e| panic!("append_only={append_only}: {e}"));
    }
}

/// This server advertises `repack` and `repack_trailer`.
#[test]
fn server_offers_repack_and_the_repack_trailer() {
    use vykar_storage::{ServerOp, StorageBackend};
    let server = Server::start(false);
    let backend = vykar_storage::rest_backend::RestBackend::new(
        &server.url,
        Some(TOKEN),
        vykar_storage::RetryConfig {
            max_retries: 0,
            ..Default::default()
        },
        None,
    )
    .unwrap();
    let ops = backend.server_ops().expect("a server is behind REST");
    assert!(ops.offers(ServerOp::Repack));
    assert!(ops.offers(ServerOp::RepackTrailer));
    assert!(ops.offers(ServerOp::VerifyPacks));
    assert!(ops.offers(ServerOp::ListTree));
    assert!(ops.offers(ServerOp::ObjectSizes));
}

/// A repack plan with a trailer through `RestBackend` against the server: the
/// trailer is the new pack's last frame, the pack ID covers it, and
/// verify-packs accepts the pack.
#[test]
fn server_repack_appends_a_trailer_end_to_end() {
    use vykar_protocol::{
        RepackBlobRef, RepackOperationRequest, RepackPlanRequest, VerifyBlobRef, VerifyPackRequest,
        VerifyPacksPlanRequest,
    };
    use vykar_storage::{ServerOp, StorageBackend};
    use vykar_types::pack_id::PackId;

    let server = Server::start(false);
    let c = Client::init(&server);
    c.write("a", "alpha");
    c.backup("s1");
    let hash = open(&c.config).content_hash();

    let backend = vykar_storage::rest_backend::RestBackend::new(
        &server.url,
        Some(TOKEN),
        vykar_storage::RetryConfig {
            max_retries: 0,
            ..Default::default()
        },
        None,
    )
    .unwrap();
    let source = backend
        .list("packs/")
        .unwrap()
        .into_iter()
        .find(|k| k.starts_with("packs/"))
        .expect("the backup wrote a pack");
    let bytes = backend.get(&source).unwrap().unwrap();

    // Every frame of the source pack, `[u32 LE len][blob]` from the header on.
    let mut keep = Vec::new();
    let mut pos = vykar_protocol::PACK_HEADER_SIZE;
    while pos < bytes.len() {
        let len = u32::from_le_bytes(bytes[pos..pos + 4].try_into().unwrap());
        keep.push(RepackBlobRef {
            offset: (pos + 4) as u64,
            length: u64::from(len),
        });
        pos += 4 + len as usize;
    }
    assert_eq!(pos, bytes.len(), "the source pack is whole frames");

    let trailer = b"e2e-trailer";
    let plan = RepackPlanRequest::new(
        vec![RepackOperationRequest {
            source_pack: source.clone(),
            keep_blobs: keep.clone(),
            delete_after: false,
            trailer: Some(trailer.to_vec()),
        }],
        hash,
    );
    let ops = backend.server_ops().expect("a server is behind REST");
    assert!(ops.offers(ServerOp::RepackTrailer));
    let result = ops.repack(&plan).unwrap();
    let op = result.completed.first().unwrap();
    let new_key = op.new_pack.clone().unwrap();
    assert_ne!(new_key, source);
    let expected_offsets: Vec<u64> = keep.iter().map(|b| b.offset).collect();
    assert_eq!(op.new_offsets, expected_offsets);

    let new_bytes = backend.get(&new_key).unwrap().unwrap();
    let mut tail = 11u32.to_le_bytes().to_vec();
    tail.extend_from_slice(trailer);
    assert!(new_bytes.ends_with(&tail));
    assert_eq!(new_bytes.len(), bytes.len() + tail.len());
    assert_eq!(new_key, PackId::compute(&new_bytes, hash).storage_key());

    let verify = ops
        .verify_packs(&VerifyPacksPlanRequest::new(
            vec![VerifyPackRequest {
                pack_key: new_key,
                expected_size: new_bytes.len() as u64,
                expected_blobs: keep
                    .iter()
                    .map(|b| VerifyBlobRef {
                        offset: b.offset,
                        length: b.length,
                    })
                    .collect(),
            }],
            hash,
        ))
        .unwrap();
    let r = verify.results.first().unwrap();
    assert!(r.hash_valid && r.header_valid && r.blobs_valid, "{r:?}");
}

/// Every pack on the server, with its length: `(key, len)`.
fn server_packs(server: &Server) -> Vec<(String, u64)> {
    let mut packs = Vec::new();
    for shard in std::fs::read_dir(server.data.path().join("packs")).unwrap() {
        let shard = shard.unwrap();
        if !shard.file_type().unwrap().is_dir() {
            continue;
        }
        for pack in std::fs::read_dir(shard.path()).unwrap() {
            let pack = pack.unwrap();
            let name = pack.file_name().to_string_lossy().into_owned();
            if name.len() != 64 {
                continue;
            }
            let key = format!("packs/{}/{name}", shard.file_name().to_string_lossy());
            packs.push((key, pack.metadata().unwrap().len()));
        }
    }
    packs.sort();
    packs
}

/// Each pack's trailer, read through the REST backend with the repository's
/// key, lists exactly the index entries naming that pack. Returns the pack
/// types found, by key.
fn assert_trailers_match_index(
    server: &Server,
    config: &VykarConfig,
) -> std::collections::HashMap<String, crate::repo::pack_trailer::TrailerPackType> {
    use crate::repo::pack_trailer::{TrailerRead, read_pack_trailer};
    let repo = commands::util::open_repo(
        config,
        None,
        OpenOptions::new().with_index(),
        &CancelToken::new(),
    )
    .unwrap();
    let mut types = std::collections::HashMap::new();
    for (key, len) in server_packs(server) {
        let trailer = match read_pack_trailer(
            repo.storage.as_ref(),
            &key,
            len,
            repo.crypto.as_ref(),
        )
        .unwrap()
        {
            TrailerRead::Found(t) => t,
            other => panic!("{key}: {other:?}"),
        };
        let mut indexed: Vec<(String, u32, u64)> = repo
            .chunk_index()
            .iter()
            .filter(|(_, e)| e.pack_id.storage_key() == key)
            .map(|(id, e)| (format!("{id:?}"), e.stored_size, e.pack_offset))
            .collect();
        indexed.sort_by_key(|e| e.2);
        let listed: Vec<(String, u32, u64)> = trailer
            .entries
            .iter()
            .map(|e| {
                (
                    format!("{:?}", e.chunk_id),
                    e.stored_size,
                    u64::from(e.offset),
                )
            })
            .collect();
        assert_eq!(listed, indexed, "{key}");
        types.insert(key, trailer.pack_type);
    }
    types
}

/// Backups and server-side compact write packs whose trailers match the
/// index; check and restore read them.
#[test]
fn trailer_packs_through_the_server() {
    use crate::repo::pack_trailer::TrailerPackType;
    let server = Server::start(false);
    let c = Client::init(&server);
    // s1's data pack holds a and b; only b outlives s1, so deleting s1
    // leaves that pack partly dead.
    c.write("a", "alpha");
    c.write("b", "bravo");
    c.backup("s1");
    std::fs::remove_file(c.source.join("a")).unwrap();
    c.write("c", "charlie");
    c.backup("s2");
    c.write("d", "delta");
    c.backup("s3");
    let before = assert_trailers_match_index(&server, &c.config);
    assert!(!before.is_empty());
    assert!(
        before
            .values()
            .all(|t| matches!(t, TrailerPackType::Data | TrailerPackType::Tree)),
        "{before:?}"
    );

    commands::delete::run(&c.config, None, &["s1"], false, &CancelToken::new()).unwrap();
    let mut repo = open(&c.config);
    let stats =
        commands::compact::run_on_repo(&mut repo, 1.0, None, false, &CancelToken::new()).unwrap();
    drop(repo);
    assert!(stats.packs_repacked > 0, "{stats:?}");

    let after = assert_trailers_match_index(&server, &c.config);
    let new: Vec<_> = after
        .iter()
        .filter(|(key, _)| !before.contains_key(*key))
        .collect();
    assert!(!new.is_empty(), "{after:?}");
    assert!(
        new.iter().all(|(_, t)| **t == TrailerPackType::Repacked),
        "{new:?}"
    );

    check_clean(&c.config);
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
    assert_eq!(
        std::fs::read_to_string(find(&dest, "c")).unwrap(),
        "charlie"
    );
    assert_eq!(std::fs::read_to_string(find(&dest, "b")).unwrap(), "bravo");
}

/// The rebuild of the index from the packs through the server: with
/// `index` and `index.d/` gone, a cold client's repair takes every pack from
/// its trailer and loses nothing; pack maps written through the server then
/// describe every pack, and a second rebuild reads only them.
#[test]
fn repair_rebuilds_a_deleted_index_from_the_packs_through_the_server() {
    let server = Server::start(false);
    let c = Client::init(&server);
    c.write("a", &"alpha".repeat(10_000));
    c.backup("s1");
    c.write("b", &"bravo".repeat(10_000));
    c.backup("s2");
    c.write("c", "charlie");
    c.backup("s3");
    commands::delete::run(&c.config, None, &["s1"], false, &CancelToken::new()).unwrap();
    commands::compact::run(&c.config, None, 0.0, None, false, &CancelToken::new()).unwrap();
    let before = entries(&c.config);
    let packs = server_packs(&server).len();

    let delete_index = || {
        super::helpers::remove_index_heads(server.data.path());
        for e in std::fs::read_dir(server.data.path().join(INDEX_OBJECT_DIR)).unwrap() {
            let path = e.unwrap().path();
            if path.is_file() {
                std::fs::remove_file(path).unwrap();
            }
        }
    };
    let rebuild = |cache: &str| {
        let cold = client(&server, c.tmp.path(), cache);
        let result = commands::check::run_with_repair(
            &cold,
            None,
            false,
            commands::check::RepairMode::Apply,
            None,
            &CancelToken::new(),
        )
        .unwrap();
        assert!(!result.plan.has_data_loss, "{:?}", result.plan.actions);
        assert!(
            result.repair_errors.is_empty(),
            "{:?}",
            result.repair_errors
        );
        let commands::check::IndexRecovery::RebuiltFromPacks { counts, .. } =
            result.plan.index_recovery
        else {
            panic!("{:?}", result.plan.index_recovery);
        };
        assert_eq!((counts.packs, counts.uncovered), (packs, 0), "{counts:?}");
        (cold, counts)
    };

    delete_index();
    let (cold, counts) = rebuild("cold-1");
    assert_eq!(counts.from_trailers, packs);
    assert_eq!(entries(&cold), before);
    check_clean(&cold);
    let dest = c.tmp.path().join("restore-s2");
    commands::restore::run(
        &cold,
        None,
        "s2",
        dest.to_str().unwrap(),
        None,
        false,
        true,
        &CancelToken::new(),
    )
    .unwrap();
    assert_eq!(
        std::fs::read_to_string(find(&dest, "b")).unwrap(),
        "bravo".repeat(10_000)
    );

    let stats = commands::pack_maps::run(&cold, None, &CancelToken::new()).unwrap();
    assert_eq!(stats.packs_mapped, packs);
    delete_index();
    let (cold, counts) = rebuild("cold-2");
    assert_eq!(counts.from_maps, packs);
    assert_eq!(entries(&cold), before);
    c.write("d", "delta");
    backup_single_source(&cold, &c.source, "src", "s4");
}
