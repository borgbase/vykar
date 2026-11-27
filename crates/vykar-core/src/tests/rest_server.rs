//! The segmented index (v6) through `RestBackend` against an in-process
//! vykar-server, in normal and append-only mode. Opening a v6 repository at
//! all is the positive path of the `segmented_index` capability check.

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use super::helpers::{
    PAST_T_PACK, age_repo_objects, backup_single_source, make_test_config, pass, set_store_now,
};
use crate::commands;
use crate::commands::restore::{RestoreOptions, RestoreRequest, RestoreSelection};
use crate::config::VykarConfig;
use crate::index::head::IndexHead;
use crate::repo::control::CancelToken;
use crate::repo::index_store::INDEX_OBJECT_DIR;
use crate::repo::index_write::FoldPolicy;
use crate::repo::{OpenOptions, Repo, RepoFormat};
use vykar_server::config::ServerSection;

const TOKEN: &str = "rest-server-test";

/// A request the server answered: method, path and query, status.
type Request = (axum::http::Method, String, u16);

/// vykar-server on 127.0.0.1, serving `data` as a single repository, with a
/// log of the requests it answered. Dropping it drops the runtime, which
/// stops the server.
struct Server {
    // Declared first so the server stops before `data` is removed.
    _rt: tokio::runtime::Runtime,
    url: String,
    data: tempfile::TempDir,
    requests: Arc<Mutex<Vec<Request>>>,
    /// While set, the next `PUT packs/..` is stored and then answered 503,
    /// as a proxy does when the server's reply is lost (the flag clears).
    lose_next_pack_reply: Arc<AtomicBool>,
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
        let requests: Arc<Mutex<Vec<Request>>> = Arc::default();
        let log = requests.clone();
        let lose_next_pack_reply: Arc<AtomicBool> = Arc::default();
        let lose = lose_next_pack_reply.clone();
        let app = vykar_server::handlers::router(state).layer(axum::middleware::from_fn(
            move |req: axum::extract::Request, next: axum::middleware::Next| {
                let (log, lose) = (log.clone(), lose.clone());
                async move {
                    let (method, uri) = (req.method().clone(), req.uri().to_string());
                    let lost = method == axum::http::Method::PUT
                        && request_key(&uri).starts_with("packs/")
                        && lose.swap(false, Ordering::SeqCst);
                    let mut resp = next.run(req).await;
                    if lost {
                        use axum::response::IntoResponse;
                        resp = axum::http::StatusCode::SERVICE_UNAVAILABLE.into_response();
                    }
                    log.lock()
                        .unwrap()
                        .push((method, uri, resp.status().as_u16()));
                    resp
                }
            },
        ));
        rt.spawn(async move {
            let _ = axum::serve(listener, app).await;
        });
        Self {
            _rt: rt,
            url,
            data,
            requests,
            lose_next_pack_reply,
        }
    }

    /// The requests answered since the last call.
    fn take_requests(&self) -> Vec<Request> {
        std::mem::take(&mut *self.requests.lock().unwrap())
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

fn open(config: &VykarConfig) -> Repo {
    commands::util::open_repo(
        config,
        pass(config),
        OpenOptions::new(),
        &CancelToken::new(),
    )
    .unwrap()
}

fn head(config: &VykarConfig) -> IndexHead {
    open(config).index_head().cloned().unwrap()
}

/// The entries, sorted, as strings.
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
        pass(config),
        OpenOptions::new().with_index(),
        &CancelToken::new(),
    )
    .unwrap();
    sorted_entries(repo.chunk_index())
}

fn check_clean(config: &VykarConfig) {
    for distrust_server in [false, true] {
        let result = commands::check::run(
            config,
            pass(config),
            &commands::check::CheckRequest {
                verify_data: true,
                distrust_server,
                ..Default::default()
            },
        )
        .unwrap();
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
        let repo = commands::init::run(&config, pass(&config)).unwrap();
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

    commands::delete::run(
        &c.config,
        pass(&c.config),
        &["s1"],
        false,
        &CancelToken::new(),
    )
    .unwrap();
    let after_delete = head(&c.config);
    assert_eq!(after_delete.segments.len(), 3, "delete writes no index");

    let compact = || {
        let mut repo = open(&c.config);
        let mut policy = FoldPolicy::default();
        policy.soft.segments = 2;
        repo.set_fold_policy(policy);
        repo.set_index_gc_grace(std::time::Duration::ZERO);
        commands::compact::run_on_repo(
            &mut repo,
            &commands::compact::CompactRequest {
                threshold: 0.0,
                max_repack_size: None,
                dry_run: false,
            },
            &CancelToken::new(),
        )
        .unwrap();
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
    // The backups' segments and packs are old: s1's entries are swept and
    // every segment is foldable (04 step 7).
    age_repo_objects(server.data.path(), &["index.d", "packs"], PAST_T_PACK);
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
        pass(&c.config),
        &RestoreRequest {
            snapshot: "s3",
            dest: dest.to_str().unwrap(),
            selection: RestoreSelection::All,
            options: RestoreOptions {
                verify_chunks: true,
                ..Default::default()
            },
        },
        None,
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
    // s1's segment is old, so the chain is foldable (04 step 7).
    age_repo_objects(server.data.path(), &["index.d"], PAST_T_PACK);

    let mut repo = open(&c.config);
    set_store_now(&mut repo);
    let mut policy = FoldPolicy::default();
    policy.hard.segments = 3;
    policy.hard.percent = 100_000;
    repo.set_fold_policy(policy);

    let mut counts = Vec::new();
    let mut last = None;
    let mut toggled = None;
    for _ in 0..5u32 {
        repo.load_latest_chunk_index().unwrap();
        crate::tests::segmented_writes::toggle_an_entry(&mut repo, &mut toggled);
        let live: Vec<_> = repo.index_head().unwrap().live_objects().collect();
        repo.unchecked_writer().save_state().unwrap();
        let head = repo.index_head().unwrap().clone();
        assert_eq!(head, self::head(&c.config));
        if head.segments.is_empty() {
            assert_eq!(head.retired, live, "a fold retires the previous live set");
        }
        counts.push(head.segments.len());
        last = Some(head);
    }
    // Every segment of an encrypted repository has a fresh name, so round 2
    // appends after the fold, and round 4 reaches the hard limit of three
    // and folds again.
    assert_eq!(counts, vec![2, 0, 1, 2, 0]);
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
    repo.load_latest_chunk_index().unwrap();
    crate::tests::segmented_writes::toggle_an_entry(&mut repo, &mut toggled);
    repo.unchecked_writer().save_state().unwrap();
    drop(repo);
    check_clean(&c.config);
}

/// A backup whose commit folds the index, as the simulator's driver runs
/// one: the fold's garbage collection runs inside the backup.
fn backup_with_fold(c: &Client, name: &str) {
    fn no_events(_: crate::repo::control::CommitEvent) {}
    let source_paths = vec![c.source.to_string_lossy().into_owned()];
    let req = commands::backup::BackupRequest {
        snapshot_name: name,
        passphrase: Some(crate::testing::TEST_PASSPHRASE),
        source_paths: &source_paths,
        source_label: "src",
        exclude_patterns: &[],
        exclude_if_present: &[],
        one_file_system: true,
        git_ignore: false,
        xattrs_enabled: false,
        compression: crate::compress::Compression::None,
        command_dumps: &[],
        verbose: false,
    };
    let mut pending = commands::backup::upload(
        &c.config,
        &req,
        None,
        &CancelToken::new(),
        None,
        crate::repo::HostEnv::system(),
    )
    .map_err(|(_, e)| e)
    .unwrap();
    pending.repo_mut().request_index_fold();
    pending.protect_commit(None, &CancelToken::new()).unwrap();
    let mut commit = pending.begin_commit();
    let result = commit.drive(&no_events);
    commit
        .finish(result, None, &crate::repo::control::CancelToken::new())
        .unwrap();
}

/// An unnamed `index.d/` object thirteen hours old: older than the
/// objects' GC grace (12 h) relative to anything just written, and named by
/// no head.
fn plant_old_debris(server: &Server) -> PathBuf {
    let debris = server
        .data
        .path()
        .join(INDEX_OBJECT_DIR)
        .join("ab".repeat(32));
    std::fs::write(&debris, b"debris").unwrap();
    std::fs::File::options()
        .write(true)
        .open(&debris)
        .unwrap()
        .set_modified(std::time::SystemTime::now() - PAST_T_PACK)
        .unwrap();
    debris
}

/// The object key a logged request names: its path without the leading
/// `/` and the query.
fn request_key(uri: &str) -> &str {
    let path = uri.split('?').next().unwrap_or(uri);
    path.trim_start_matches('/')
}

/// The deletes in `reqs` an append-only server refuses: every DELETE of a
/// key the key-class table forbids deleting, and every batch delete.
fn refused_deletes(reqs: &[Request]) -> Vec<String> {
    reqs.iter()
        .filter(|(method, uri, _)| {
            (*method == axum::http::Method::DELETE
                && !vykar_protocol::append_only_allows(
                    request_key(uri),
                    vykar_protocol::Verb::Delete,
                ))
                || (*method == axum::http::Method::POST && uri.contains("batch-delete"))
        })
        .map(|(method, uri, _)| format!("{method} {uri}"))
        .collect()
}

/// The 03 step 7 done criterion: a backup whose commit folds, against an
/// append-only server, sends no delete the server refuses and gets no 403;
/// its garbage collection is skipped. The normal-mode run shows the same
/// backup does collect the debris there.
#[test]
fn a_backup_against_an_append_only_server_issues_no_refused_delete() {
    for append_only in [false, true] {
        let server = Server::start(append_only);
        let c = Client::init(&server);
        c.write("a", "alpha");
        c.backup("s1");
        // s1's segment is old, so s2's commit folds it (04 step 7).
        age_repo_objects(server.data.path(), &["index.d"], PAST_T_PACK);
        let debris = plant_old_debris(&server);
        c.write("b", "bravo");
        server.take_requests();
        backup_with_fold(&c, "s2");
        let reqs = server.take_requests();
        let folded = head(&c.config);
        assert!(
            folded.base.is_some() && folded.segments.len() == 1,
            "append_only={append_only}: the commit folds s1's segment and keeps its own: \
             {folded:?}"
        );
        if append_only {
            assert!(debris.exists(), "the GC is skipped");
            assert_eq!(refused_deletes(&reqs), Vec::<String>::new());
            let forbidden: Vec<_> = reqs.iter().filter(|r| r.2 == 403).collect();
            assert!(forbidden.is_empty(), "{forbidden:?}");
            assert!(
                reqs.iter()
                    .any(|(m, uri, _)| *m == axum::http::Method::DELETE
                        && (request_key(uri).starts_with("sessions/")
                            || request_key(uri).starts_with("locks/"))),
                "the log records the backup's ephemeral deletes: {reqs:?}"
            );
        } else {
            assert!(!debris.exists(), "the GC collects the debris");
            assert!(
                reqs.iter()
                    .any(|(m, uri, _)| (*m == axum::http::Method::POST
                        && uri.contains("batch-delete"))
                        || (*m == axum::http::Method::DELETE
                            && request_key(uri).starts_with(INDEX_OBJECT_DIR))),
                "{reqs:?}"
            );
        }
        check_clean(&c.config);
    }
}

/// 04 step 8: a backup through vykar-server creates its head with
/// `If-None-Match: *`, in normal and in append-only mode (`sessions/*` is
/// ephemeral there: the probe may be created and deleted). The in-process
/// harness logs no request headers; what shows the conditional path is the
/// check's second create of its probe answered 412, its DELETE answered
/// with success, the head's create answered 201, and `sessions/` holding no
/// probe afterwards.
#[test]
fn backup_creates_heads_with_if_none_match() {
    for append_only in [false, true] {
        let server = Server::start(append_only);
        let c = Client::init(&server);
        c.write("a", "alpha");
        server.take_requests();
        c.backup("s1");
        let reqs = server.take_requests();
        let answers = |method: axum::http::Method, prefix: &str| -> Vec<u16> {
            reqs.iter()
                .filter(|(m, uri, _)| *m == method && request_key(uri).starts_with(prefix))
                .map(|r| r.2)
                .collect()
        };
        let ctx = format!("append_only={append_only}: {reqs:?}");
        assert_eq!(
            answers(axum::http::Method::PUT, "sessions/vykar-probe-"),
            vec![201, 412],
            "{ctx}"
        );
        let deletes = answers(axum::http::Method::DELETE, "sessions/vykar-probe-");
        assert_eq!(deletes.len(), 1, "{ctx}");
        assert!((200..300).contains(&deletes[0]), "{ctx}");
        assert_eq!(
            answers(axum::http::Method::PUT, "index.h/"),
            vec![201],
            "{ctx}"
        );
        let probes: Vec<String> = std::fs::read_dir(server.data.path().join("sessions"))
            .unwrap()
            .map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
            .filter(|n| n.starts_with("vykar-probe-"))
            .collect();
        assert!(probes.is_empty(), "append_only={append_only}: {probes:?}");
        check_clean(&c.config);
    }
}

/// The snapshot objects on the server, sorted.
fn server_snapshots(server: &Server) -> Vec<String> {
    let mut names: Vec<String> = std::fs::read_dir(server.data.path().join("snapshots"))
        .unwrap()
        .map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
        .collect();
    names.sort();
    names
}

/// A refused command sent no delete outside `locks/` and `sessions/`, got
/// no 403, and left the index head and the snapshots as they were.
fn assert_refused_cleanly(
    server: &Server,
    config: &VykarConfig,
    before: &(IndexHead, Vec<String>),
    what: &str,
) -> Vec<Request> {
    let reqs = server.take_requests();
    let deletes: Vec<_> = reqs
        .iter()
        .filter(|(m, uri, _)| {
            let key = request_key(uri);
            (*m == axum::http::Method::DELETE
                && !key.starts_with("locks/")
                && !key.starts_with("sessions/"))
                || (*m == axum::http::Method::POST && uri.contains("batch-delete"))
        })
        .collect();
    assert!(deletes.is_empty(), "{what}: {deletes:?}");
    let forbidden: Vec<_> = reqs.iter().filter(|r| r.2 == 403).collect();
    assert!(forbidden.is_empty(), "{what}: {forbidden:?}");
    assert_eq!(head(config), before.0, "{what}");
    assert_eq!(server_snapshots(server), before.1, "{what}");
    reqs
}

fn is_append_only_refusal(e: &vykar_types::error::VykarError, command: &str) -> bool {
    matches!(e, vykar_types::error::VykarError::AppendOnlyServer { command: c } if *c == command)
}

/// compact, prune, snapshot delete and `check --repair` stop before their
/// first change on an advertising server; dry runs and runs that remove
/// nothing still succeed.
#[test]
fn deleting_commands_refuse_early_on_an_append_only_server() {
    let server = Server::start(true);
    let c = Client::init(&server);
    c.write("a", "alpha");
    c.backup("s1");
    c.write("b", "bravo");
    c.backup("s2");
    let before = (head(&c.config), server_snapshots(&server));
    assert_eq!(before.1.len(), 2);
    let cancel = CancelToken::new();

    // compact: refused before its session, so nothing is written at all.
    server.take_requests();
    let err = commands::compact::run(
        &c.config,
        pass(&c.config),
        &commands::compact::CompactRequest {
            threshold: 0.0,
            max_repack_size: None,
            dry_run: false,
        },
        &cancel,
    )
    .unwrap_err();
    assert!(is_append_only_refusal(&err, "vykar compact"), "{err:?}");
    let reqs = assert_refused_cleanly(&server, &c.config, &before, "compact");
    let puts: Vec<_> = reqs
        .iter()
        .filter(|r| r.0 == axum::http::Method::PUT)
        .collect();
    assert!(puts.is_empty(), "{puts:?}");
    commands::compact::run(
        &c.config,
        pass(&c.config),
        &commands::compact::CompactRequest {
            threshold: 0.0,
            max_repack_size: None,
            dry_run: true,
        },
        &cancel,
    )
    .unwrap();
    server.take_requests();

    // prune: refused only when it would remove a snapshot.
    let mut pruning = c.config.clone();
    pruning.retention.keep_last = Some(1);
    let err = commands::prune::run(
        &pruning,
        pass(&pruning),
        &commands::prune::PruneRequest::default(),
        &cancel,
    )
    .err()
    .unwrap();
    assert!(is_append_only_refusal(&err, "vykar prune"), "{err:?}");
    assert_refused_cleanly(&server, &c.config, &before, "prune");
    let (stats, _) = commands::prune::run(
        &pruning,
        pass(&pruning),
        &commands::prune::PruneRequest {
            dry_run: true,
            ..Default::default()
        },
        &cancel,
    )
    .unwrap();
    assert_eq!(stats.pruned, 1);
    pruning.retention.keep_last = Some(5);
    let (stats, _) = commands::prune::run(
        &pruning,
        pass(&pruning),
        &commands::prune::PruneRequest::default(),
        &cancel,
    )
    .unwrap();
    assert_eq!(stats.pruned, 0);
    server.take_requests();

    // snapshot delete: unknown names still fail first.
    let err = commands::delete::run(&c.config, pass(&c.config), &["s1"], false, &cancel)
        .err()
        .unwrap();
    assert!(
        is_append_only_refusal(&err, "vykar snapshot delete"),
        "{err:?}"
    );
    assert_refused_cleanly(&server, &c.config, &before, "delete");
    let err = commands::delete::run(&c.config, pass(&c.config), &["nope"], false, &cancel)
        .err()
        .unwrap();
    assert!(
        matches!(err, vykar_types::error::VykarError::SnapshotNotFound(_)),
        "{err:?}"
    );
    commands::delete::run(&c.config, pass(&c.config), &["s1"], true, &cancel).unwrap();
    server.take_requests();

    // check --repair with data loss: refused by capability, no delete probe.
    let (pack, _) = server_packs(&server).into_iter().next().unwrap();
    std::fs::remove_file(server.data.path().join(&pack)).unwrap();
    let plan = commands::check::run_with_repair(
        &c.config,
        pass(&c.config),
        false,
        commands::check::RepairMode::PlanOnly,
        None,
        &cancel,
    )
    .unwrap()
    .plan;
    assert!(plan.has_data_loss, "{:?}", plan.actions);
    server.take_requests();
    let err = commands::check::run_with_repair(
        &c.config,
        pass(&c.config),
        false,
        commands::check::RepairMode::Apply,
        None,
        &cancel,
    )
    .err()
    .unwrap();
    assert!(
        is_append_only_refusal(&err, "vykar check --repair"),
        "{err:?}"
    );
    let reqs = assert_refused_cleanly(&server, &c.config, &before, "check --repair");
    assert!(
        !reqs.iter().any(|(_, uri, _)| uri.contains(".repair-probe")),
        "{reqs:?}"
    );
}

/// The full cycle skips compact on an append-only server (it belongs to the
/// maintenance window) and runs it on a normal one.
#[test]
fn the_cycle_skips_compact_on_an_append_only_server() {
    use crate::app::operations::{CycleStep, StepOutcome, run_full_cycle_for_repo};
    for append_only in [false, true] {
        let server = Server::start(append_only);
        let c = Client::init(&server);
        c.write("a", "alpha");
        let mut repo = super::helpers::resolved_repo(
            c.config.clone(),
            vec![super::helpers::source_entry(&c.source, "src")],
        );
        // A check that opens, so the open count shows whether it reopened.
        repo.config.check.max_percent = 100;
        let (result, opens) = crate::repo::count_repo_opens(|| {
            run_full_cycle_for_repo(
                &repo,
                Some(crate::testing::TEST_PASSPHRASE),
                &CancelToken::new(),
                false,
                &[],
                crate::app::preflight::UnavailableRepo::Skip,
                &|_| {},
            )
        });
        let outcome = |step: CycleStep| {
            result
                .steps
                .iter()
                .find(|(s, _)| *s == step)
                .map(|(_, o)| o.clone())
                .unwrap_or_else(|| panic!("no outcome for {}", step.command_name()))
        };
        assert!(
            matches!(outcome(CycleStep::Backup), StepOutcome::Ok),
            "{:?}",
            result.steps
        );
        assert!(
            matches!(outcome(CycleStep::Prune), StepOutcome::Skipped(r)
                if r.kind == crate::app::SkipKind::NoRetentionRules && r.message.contains("no retention")),
            "{:?}",
            result.steps
        );
        match outcome(CycleStep::Compact) {
            StepOutcome::Skipped(r) if append_only => {
                assert_eq!(r.kind, crate::app::SkipKind::AppendOnlyServer);
                assert!(r.message.contains("append-only"), "{r}");
            }
            StepOutcome::Ok if !append_only => {}
            other => panic!("append_only={append_only}: {other:?}"),
        }
        assert!(
            !matches!(outcome(CycleStep::Check), StepOutcome::Failed(_)),
            "{:?}",
            result.steps
        );
        // The backup's open and the session compact opened. A compact that
        // fails (the append-only skip is an `Err`) drops the session, so
        // check opens again.
        let check_reopens = usize::from(append_only);
        assert_eq!(opens, 2 + check_reopens, "append_only={append_only}");
    }
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
/// against the server, in both modes, with a second client for the listing
/// case. `sessions/` is ephemeral, so the suite's deletes are allowed in
/// append-only mode too.
#[test]
fn conditional_conformance_suite_passes() {
    use vykar_storage::StorageBackend;
    for append_only in [false, true] {
        let server = Server::start(append_only);
        let client = || {
            vykar_storage::rest_backend::RestBackend::new(
                &server.url,
                Some(TOKEN),
                vykar_storage::RetryConfig {
                    max_retries: 0,
                    ..Default::default()
                },
                None,
            )
            .unwrap()
        };
        let backend = client();
        let second = client();
        assert!(backend.conditional().is_some(), "the server advertises it");
        vykar_storage::conformance::run(
            &backend,
            &second,
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
        pass(config),
        OpenOptions::new().with_index(),
        &CancelToken::new(),
    )
    .unwrap();
    let mut types = std::collections::HashMap::new();
    for (key, len) in server_packs(server) {
        let trailer =
            match read_pack_trailer(repo.storage().as_ref(), &key, len, repo.crypto().as_ref())
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

    commands::delete::run(
        &c.config,
        pass(&c.config),
        &["s1"],
        false,
        &CancelToken::new(),
    )
    .unwrap();
    age_repo_objects(server.data.path(), &["index.d", "packs"], PAST_T_PACK);
    let mut repo = open(&c.config);
    let stats = commands::compact::run_on_repo(
        &mut repo,
        &commands::compact::CompactRequest {
            threshold: 1.0,
            max_repack_size: None,
            dry_run: false,
        },
        &CancelToken::new(),
    )
    .unwrap();
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
        pass(&c.config),
        &RestoreRequest {
            snapshot: "s3",
            dest: dest.to_str().unwrap(),
            selection: RestoreSelection::All,
            options: RestoreOptions {
                verify_chunks: true,
                ..Default::default()
            },
        },
        None,
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
    commands::delete::run(
        &c.config,
        pass(&c.config),
        &["s1"],
        false,
        &CancelToken::new(),
    )
    .unwrap();
    // s1's segment is old: the compact sweeps its entries, as the repair
    // below does (it keeps none for young segments).
    age_repo_objects(server.data.path(), &["index.d", "packs"], PAST_T_PACK);
    commands::compact::run(
        &c.config,
        pass(&c.config),
        &commands::compact::CompactRequest {
            threshold: 0.0,
            max_repack_size: None,
            dry_run: false,
        },
        &CancelToken::new(),
    )
    .unwrap();
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
            pass(&cold),
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
        pass(&cold),
        &RestoreRequest {
            snapshot: "s2",
            dest: dest.to_str().unwrap(),
            selection: RestoreSelection::All,
            options: RestoreOptions {
                verify_chunks: true,
                ..Default::default()
            },
        },
        None,
        &CancelToken::new(),
    )
    .unwrap();
    assert_eq!(
        std::fs::read_to_string(find(&dest, "b")).unwrap(),
        "bravo".repeat(10_000)
    );

    let stats = commands::pack_maps::run(&cold, pass(&cold), &CancelToken::new()).unwrap();
    assert_eq!(stats.packs_mapped, packs);
    delete_index();
    let (cold, counts) = rebuild("cold-2");
    assert_eq!(counts.from_maps, packs);
    assert_eq!(entries(&cold), before);
    c.write("d", "delta");
    backup_single_source(&cold, &c.source, "src", "s4");
}

/// A pack PUT whose reply is lost on an append-only server (04
/// "Crash-recovery journals"): the server stored the pack, the backend
/// retries the PUT, and the server refuses the retry (403, the key exists)
/// before reading its body. The stored pack has the sealed size, so the
/// backup uses it and commits, and the snapshot restores.
#[test]
fn a_lost_pack_reply_on_an_append_only_server_is_recognised() {
    let server = Server::start(true);
    let mut c = Client::init(&server);
    c.config.repository.retry.retry_delay_ms = 10;
    c.config.repository.retry.retry_max_delay_ms = 50;
    c.write("a", &"alpha ".repeat(4096));
    server.take_requests();
    server.lose_next_pack_reply.store(true, Ordering::SeqCst);

    backup_single_source(&c.config, &c.source, "src", "s1");
    assert!(!server.lose_next_pack_reply.load(Ordering::SeqCst));
    let pack_puts: Vec<(String, u16)> = server
        .take_requests()
        .into_iter()
        .filter(|(m, uri, _)| {
            *m == axum::http::Method::PUT && request_key(uri).starts_with("packs/")
        })
        .map(|(_, uri, status)| (request_key(&uri).to_string(), status))
        .collect();
    let (lost, _) = pack_puts
        .iter()
        .find(|(_, status)| *status == 503)
        .expect("a pack PUT answered 503");
    let statuses: Vec<u16> = pack_puts
        .iter()
        .filter(|(key, _)| key == lost)
        .map(|(_, status)| *status)
        .collect();
    assert_eq!(statuses, vec![503, 403], "{pack_puts:?}");

    check_clean(&c.config);
    let dest = c.tmp.path().join("restore");
    commands::restore::run(
        &c.config,
        pass(&c.config),
        &RestoreRequest {
            snapshot: "s1",
            dest: dest.to_str().unwrap(),
            selection: RestoreSelection::All,
            options: RestoreOptions {
                verify_chunks: true,
                ..Default::default()
            },
        },
        None,
        &CancelToken::new(),
    )
    .unwrap();
    assert_eq!(
        std::fs::read_to_string(find(&dest, "a")).unwrap(),
        "alpha ".repeat(4096)
    );
}

/// The commit-mode switch rewrites `config`, which an append-only server
/// refuses: both directions are refused before anything is written (no
/// put-if-absent probe under `sessions/`), with `config` unchanged. On a
/// server without append-only the switch succeeds.
#[test]
fn commit_mode_switch_is_refused_on_an_append_only_server() {
    use crate::repo::{DecodedConfig, RepoConfig, WriterMode};
    let probe_writes = |reqs: &[Request]| -> Vec<String> {
        reqs.iter()
            .filter(|(method, uri, _)| {
                *method != axum::http::Method::GET
                    && *method != axum::http::Method::HEAD
                    && request_key(uri).starts_with(vykar_storage::PUT_IF_ABSENT_PROBE_KEY_PREFIX)
            })
            .map(|(_, uri, _)| uri.clone())
            .collect()
    };
    let server = Server::start(true);
    let c = Client::init(&server);
    c.write("a", "alpha");
    c.backup("s1");
    let config_path = server.data.path().join("config");
    let mode = || match RepoConfig::decode(&std::fs::read(&config_path).unwrap()).unwrap() {
        DecodedConfig::Map(c) => c.writer_mode(),
        DecodedConfig::Positional(_) => panic!("positional"),
    };
    assert_eq!(mode(), WriterMode::Multi, "init passed the check");
    let switch = |want| commands::upgrade::run_with_mode(&c.config, pass(&c.config), Some(want));

    let before = std::fs::read(&config_path).unwrap();
    server.take_requests();
    let err = switch(WriterMode::Single).unwrap_err().to_string();
    assert!(
        err.contains("cannot be rewritten") && err.contains("append-only"),
        "{err}"
    );
    assert_eq!(std::fs::read(&config_path).unwrap(), before);

    // A single-writer repository (as one made before the feature existed)
    // switched to multi-writer: refused before the check writes a probe.
    let DecodedConfig::Map(mut single) = RepoConfig::decode(&before).unwrap() else {
        unreachable!()
    };
    single.set_writer_mode(WriterMode::Single);
    std::fs::write(&config_path, single.encode().unwrap()).unwrap();
    let before = std::fs::read(&config_path).unwrap();
    server.take_requests();
    let err = switch(WriterMode::Multi).unwrap_err().to_string();
    assert!(
        err.contains("cannot be rewritten") && err.contains("append-only"),
        "{err}"
    );
    assert_eq!(std::fs::read(&config_path).unwrap(), before);
    let probes = probe_writes(&server.take_requests());
    assert!(probes.is_empty(), "{probes:?}");
    c.backup("s2");

    let server = Server::start(false);
    let c = Client::init(&server);
    c.write("a", "alpha");
    c.backup("s1");
    let config_path = server.data.path().join("config");
    commands::upgrade::run_with_mode(&c.config, pass(&c.config), Some(WriterMode::Single)).unwrap();
    let DecodedConfig::Map(after) =
        RepoConfig::decode(&std::fs::read(&config_path).unwrap()).unwrap()
    else {
        unreachable!()
    };
    assert_eq!(after.writer_mode(), WriterMode::Single);
    c.backup("s2");
    check_clean(&c.config);
}
