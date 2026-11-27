//! The storage operations of the backup commit and the index write, in
//! order, pinned before they were split into steppable machines (08 step 4).
//! Each scenario projects one run's op log to (op, key class, outcome);
//! between two mutations, reads form a multiset (sorted), since some of them
//! run concurrently. `VYKAR_PRINT_OP_ORDER=1` prints the projections.
//! Since 10-single-protocol step 2 the scenarios run on an encrypted
//! repository on the fallback store; since step 7 that is the single-writer
//! protocol, no lock: the backup's single-writer listings of `sessions/`
//! around its commit mark and its pre-head listing, compact in a session
//! with its single-writer check, heads as EXISTS, PUT and read-back. The
//! `multi_writer_*` scenarios pin the conditional store's multi-writer
//! protocol, fixed through step 7 (invariant I6) but for the one `config`
//! GET a maintenance session makes after its registration. Step 8 breaks I6
//! by design: every registration lists `sessions/` instead of `locks/` (a
//! compact's extra `ListMeta sessions/`), a backup's commit lists
//! `sessions/` only before its head (no `ListMeta locks/`), and the
//! `Sim::custom` scenarios run as exclusive sessions (their marker PUT, one
//! EXISTS per reuse interval, the re-read of `config` and the marker's
//! delete instead of the lock's keys).

// Printing is the point of the env-driven helpers here (projections,
// replays, the measurement); every other path asserts.
#![allow(clippy::print_stderr)]

use std::time::Duration;

use crate::compress::Compression;
use crate::repo::ages::T_PACK;
use crate::repo::pack::PackType;
use crate::repo::{OpenOptions, Repo, RepoWriter, WriteSession, WriteSessionOptions};
use crate::sim::log::key_class;
use crate::sim::{OpKind, OpRecord, RunRecord, Sim, SimConfig, SourceTree, StoreKind};

/// `records` as "op class outcome" lines; the reads between two mutations
/// sorted, a run of equal lines as one with its count ("x3"). Reads of
/// `packs/` (not a protocol key) between two mutations are one line,
/// "reads packs/": whether a presence check SIZEs each pack or LISTs their
/// shards depends on how the random pack IDs fall into shards.
pub(super) fn project(records: &[OpRecord]) -> Vec<String> {
    let mut lines = Vec::new();
    let mut reads: Vec<String> = Vec::new();
    for r in records {
        let line = format!("{:?} {} {}", r.op, key_class(&r.key), r.outcome.name());
        if r.op.is_mutation() {
            flush_reads(&mut reads, &mut lines);
            lines.push(line);
        } else if key_class(&r.key) == "packs/" {
            if !reads.iter().any(|l| l == PACK_READS) {
                reads.push(PACK_READS.to_string());
            }
        } else {
            reads.push(line);
        }
    }
    flush_reads(&mut reads, &mut lines);
    let mut out: Vec<String> = Vec::new();
    let mut i = 0;
    while i < lines.len() {
        let n = lines[i..].iter().take_while(|l| **l == lines[i]).count();
        out.push(if n == 1 {
            lines[i].clone()
        } else {
            format!("{} x{n}", lines[i])
        });
        i += n;
    }
    out
}

const PACK_READS: &str = "reads packs/";

fn flush_reads(reads: &mut Vec<String>, lines: &mut Vec<String>) {
    reads.sort();
    lines.append(reads);
}

/// The records of `run`. A backup's from the start of its commit
/// protection on (phase 1 uploads from worker threads), without `sessions/*.index` writes (the final
/// journal PUT runs in the background until the commit's flush joins it).
pub(crate) fn run_records(sim: &Sim, run: &RunRecord, backup: bool) -> Vec<OpRecord> {
    let mine: Vec<OpRecord> = sim
        .store()
        .log()
        .into_iter()
        .filter(|r| r.client == run.client && r.run == Some(run.index))
        .collect();
    if !backup {
        return mine;
    }
    // Dropped before the start is looked for: a background journal PUT is
    // no commit op.
    let mine: Vec<OpRecord> = mine
        .into_iter()
        .filter(|r| {
            !(r.op.is_write() && r.key.starts_with("sessions/") && r.key.ends_with(".index"))
        })
        .collect();
    // Phase 1 ends with the last pack upload; a backup that uploads no
    // pack is past its open once the open's listing of `sessions/` ran
    // (its second: the registration lists first, 10-single-protocol step
    // 8). The commit starts at the first session-marker op (not `.index`)
    // after both: the commit mark or the single-writer listing, whichever
    // the protocol runs first.
    let last_pack = mine.iter().rposition(|r| r.op == OpKind::PutPack);
    let open_listing = mine
        .iter()
        .enumerate()
        .filter(|(_, r)| !r.op.is_write() && r.key == "sessions/")
        .nth(1)
        .map(|(i, _)| i)
        .expect("a backup lists sessions/ at its registration and its open");
    let after = last_pack.map_or(open_listing, |p| p.max(open_listing));
    let start = (after + 1..mine.len())
        .find(|&i| {
            let key = &mine[i].key;
            key.starts_with("sessions/") && !key.ends_with(".index")
        })
        .expect("a backup's commit touches its session marker");
    let out: Vec<OpRecord> = mine.into_iter().skip(start).collect();
    assert!(
        !out.is_empty(),
        "a backup's commit records no storage operation"
    );
    out
}

fn check(name: &str, got: Vec<String>, want: &[&str]) {
    if std::env::var_os("VYKAR_PRINT_OP_ORDER").is_some() {
        eprintln!("--- {name}");
        for l in &got {
            eprintln!("        \"{l}\",");
        }
    }
    assert_eq!(got, want, "{name}: storage operation order changed");
}

fn tree(seed: &str) -> SourceTree {
    SourceTree::seeded(seed, 2, 16 * 1024)
}

#[test]
fn first_backup_writes_a_segment() {
    let mut sim = Sim::new();
    let c = sim.client("c");
    let r = sim.backup(c, "t0", &tree("t0"));
    r.expect_ok();
    check(
        "first backup",
        project(&run_records(&sim, &r, true)),
        &[
            // The single-writer listing before the commit mark (encrypted repository).
            "ListMeta sessions/ ok",
            "Put sessions/ ok",
            "Exists sessions/ ok x2",
            "Get index.h/ ok",
            "List index.h/ ok",
            "List snapshots/ ok",
            "ListMeta sessions/ ok x2",
            "reads packs/",
            "Put index.d/ ok",
            "Exists index.h/ not-found",
            "Put index.h/ ok",
            "Exists sessions/ ok",
            "Get index.h/ ok",
            "Put snapshots/ ok",
            "Delete sessions/ ok x3",
        ],
    );
}

#[test]
fn second_backup_of_the_same_tree_writes_a_segment() {
    let mut sim = Sim::new();
    let (c, d) = (sim.client("c"), sim.client("d"));
    sim.backup(c, "t0", &tree("t0")).expect_ok();
    let r = sim.backup(d, "t1", &tree("t0"));
    r.expect_ok();
    check(
        "second backup, same tree",
        project(&run_records(&sim, &r, true)),
        &[
            // The single-writer listing before the commit mark (encrypted repository).
            "ListMeta sessions/ ok",
            "Put sessions/ ok",
            "Exists sessions/ ok x2",
            "Get index.h/ ok",
            "List index.h/ ok",
            "List snapshots/ ok",
            "ListMeta sessions/ ok x2",
            "reads packs/",
            "Put index.d/ ok",
            "Exists index.h/ not-found",
            "Put index.h/ ok",
            "Exists sessions/ ok",
            "Get index.h/ ok",
            "Put snapshots/ ok",
            "Delete sessions/ ok x3",
        ],
    );
}

/// A8: a commit whose delta's additions are all in the index already
/// (another session committed the same chunk first) writes no index
/// object, only a head (Q2, on a single-writer repository too since
/// 10-single-protocol step 7, R2): it goes from the pack check to the head
/// write, the fence and the snapshot PUT. At repository level, since two
/// `Sim` backups never share a tree chunk (each backs up its own source
/// directory).
#[test]
fn commit_with_no_new_additions_writes_a_head_only() {
    let mut sim = Sim::new();
    let (c, d) = (sim.client("c"), sim.client("d"));
    sim.backup(c, "t0", &tree("t0")).expect_ok();
    let store = sim.store().clone();
    let other = store.handle(d);
    let cache = tempfile::tempdir().unwrap();
    let mut commit_from = 0;
    let r = sim.custom(c, "no additions", |repo: &mut RepoWriter<'_>| {
        let data = b"one chunk both sessions store";
        let mut ws = repo.begin_write_session(&WriteSessionOptions::default());
        ws.store_chunk(repo, data, Compression::None, PackType::Data)?;
        let mut first = Repo::open(
            Box::new(other),
            store.passphrase(),
            Some(cache.path().to_path_buf()),
            OpenOptions::new().with_index(),
        )?;
        let mut first_ws = first.begin_write_session(&WriteSessionOptions::default());
        first_ws.store_chunk(&first, data, Compression::None, PackType::Data)?;
        commit_as(&mut first.unchecked_writer(), first_ws, "first")?;
        commit_from = store.log().last().map_or(0, |r| r.step);
        commit_as(repo, ws, "second")
    });
    r.expect_ok();
    let recs: Vec<OpRecord> = run_records(&sim, &r, false)
        .into_iter()
        .filter(|r| r.step > commit_from)
        .collect();
    assert!(
        !recs
            .iter()
            .any(|r| r.op.is_write() && r.key.starts_with("index.d/")),
        "the commit wrote an index object"
    );
    assert_eq!(
        recs.iter()
            .filter(|r| r.op.is_write() && r.key.starts_with("index.h/"))
            .count(),
        1,
        "the commit wrote no head"
    );
    check(
        "commit, no new additions",
        project(&recs),
        &[
            "Exists sessions/ ok",
            "PutPack packs/ ok",
            "Put sessions/ ok",
            "Exists index.h/ not-found",
            "Get index.d/ ok",
            "Get index.h/ ok",
            "Get snapshots/ ok",
            "List index.h/ ok",
            "List snapshots/ ok",
            "Put index.h/ ok",
            "Get index.h/ ok",
            "ListMeta index.h/ ok",
            "Put snapshots/ ok",
            "Delete sessions/ ok",
        ],
    );
}

fn snapshot_entry(name: &str) -> crate::repo::manifest::SnapshotEntry {
    crate::repo::manifest::SnapshotEntry {
        name: name.into(),
        id: vykar_types::snapshot_id::SnapshotId::generate(),
        time: chrono::Utc::now(),
        source_label: String::new(),
        label: String::new(),
        source_paths: Vec::new(),
        hostname: String::new(),
    }
}

/// Commit the session `ws` of `repo` as snapshot `name`.
fn commit_as(
    repo: &mut RepoWriter<'_>,
    ws: WriteSession,
    name: &str,
) -> vykar_types::error::Result<()> {
    repo.commit_session_blocking(
        ws,
        snapshot_entry(name),
        b"snapshot".to_vec(),
        &mut crate::repo::file_cache::FileCache::new(),
        &|_: crate::repo::control::CommitEvent| {},
    )
}

#[test]
fn compact_after_delete_writes_a_record_and_a_segment() {
    let mut sim = Sim::new();
    let c = sim.client("c");
    sim.backup(c, "t0", &tree("t0")).expect_ok();
    sim.backup(c, "t1", &tree("t1")).expect_ok();
    sim.delete(c, &["t0"]).expect_ok();
    // t0's segment and packs are old: its entries are swept, its packs go
    // (04 step 7).
    sim.advance(T_PACK + Duration::from_secs(60));
    let r = sim.compact(c);
    r.expect_ok();
    check(
        "compact after delete",
        project(&run_records(&sim, &r, false)),
        &[
            "Get config ok",
            "Get index.h/ ok",
            // The key copies, read at open (encrypted since 10-single-protocol step 2).
            "Get keys/ ok x2",
            "List index.h/ ok",
            "List snapshots/ ok",
            "Put sessions/ ok",
            "Get config ok",
            "Get index.h/ ok x3",
            "Get snapshots/ ok",
            "List index.h/ ok x3",
            "List sessions/ ok",
            "List snapshots/ ok",
            "ListMeta index.d/ ok",
            "ListMeta sessions/ ok x3",
            "reads packs/",
            "Put index.d/ ok x2",
            "Exists index.h/ not-found",
            "Exists sessions/ ok",
            "Put index.h/ ok",
            "Exists sessions/ ok x2",
            "Get index.h/ ok",
            "Delete packs/ ok x2",
            "Exists sessions/ ok x2",
            "Get index.h/ ok x4",
            "List index.h/ ok",
            "List packmaps/ ok",
            "ListMeta index.d/ ok",
            "ListMeta index.h/ ok",
            "Delete sessions/ ok",
        ],
    );
}

#[test]
fn compact_with_nothing_to_remove_writes_a_head_only() {
    let mut sim = Sim::new();
    let c = sim.client("c");
    sim.backup(c, "t0", &tree("t0")).expect_ok();
    let r = sim.compact(c);
    r.expect_ok();
    let recs = run_records(&sim, &r, false);
    assert!(
        recs.iter()
            .any(|r| r.op.is_write() && r.key.starts_with("index.h/")),
        "compact wrote no head"
    );
    check(
        "compact, nothing to remove",
        project(&recs),
        &[
            "Get config ok",
            "Get index.h/ ok",
            // The key copies, read at open (encrypted since 10-single-protocol step 2).
            "Get keys/ ok x2",
            "List index.h/ ok",
            "List snapshots/ ok",
            "Put sessions/ ok",
            "Get config ok",
            "Get index.h/ ok x3",
            "Get snapshots/ ok",
            "List index.h/ ok x3",
            "List sessions/ ok",
            "List snapshots/ ok",
            "ListMeta index.d/ ok",
            "ListMeta sessions/ ok x3",
            "reads packs/",
            "Put index.d/ ok",
            "Exists index.h/ not-found",
            "Exists sessions/ ok",
            "Put index.h/ ok",
            "Exists sessions/ ok x2",
            "Get index.h/ ok x4",
            "List index.h/ ok",
            "List packmaps/ ok",
            "ListMeta index.d/ ok",
            "ListMeta index.h/ ok",
            "Delete sessions/ ok",
        ],
    );
}

#[test]
fn requested_fold_writes_a_base_and_collects_garbage() {
    let mut sim = Sim::new();
    let c = sim.client("c");
    sim.backup(c, "t0", &tree("t0")).expect_ok();
    // t0's segment is old, so the fold folds it (04 step 7); the
    // fold-prefix step lists index.d/ for its age.
    sim.advance(T_PACK + Duration::from_secs(60));
    let r = sim.custom(c, "fold", |repo: &mut RepoWriter<'_>| {
        repo.load_latest_chunk_index()?;
        repo.request_index_fold();
        repo.mark_index_dirty();
        repo.save_state()
    });
    r.expect_ok();
    check(
        "fold",
        project(&run_records(&sim, &r, false)),
        &[
            "Get config ok",
            "Get index.h/ ok",
            // The key copies, read at open (encrypted since 10-single-protocol step 2).
            "Get keys/ ok x2",
            "List index.h/ ok",
            "List snapshots/ ok",
            "Put sessions/ ok",
            "Exists sessions/ ok",
            "Get config ok",
            "Get index.h/ ok x2",
            "List index.h/ ok x2",
            "ListMeta index.d/ ok",
            "ListMeta sessions/ ok",
            "Put index.d/ ok",
            "Exists index.h/ not-found",
            "Put index.h/ ok",
            "Get index.h/ ok x3",
            "ListMeta index.d/ ok",
            "ListMeta index.h/ ok",
            "Delete sessions/ ok",
        ],
    );
}
// ── Multi-writer (10-single-protocol step 7, invariant I6) ──
//
// Pinned on the conditional store before step 7 and kept fixed through it:
// a multi-writer run whose marker record is continuous makes the same
// requests in the same order.

/// A multi-writer repository: the conditional store passes the
/// put-if-absent check at init.
fn conditional() -> Sim {
    Sim::with_config(SimConfig {
        kind: StoreKind::Conditional,
        ..SimConfig::default()
    })
}

#[test]
fn multi_writer_first_backup_writes_a_segment() {
    let mut sim = conditional();
    let c = sim.client("c");
    let r = sim.backup(c, "t0", &tree("t0"));
    r.expect_ok();
    check(
        "multi-writer first backup",
        project(&run_records(&sim, &r, true)),
        &[
            "Put sessions/ ok",
            "Exists sessions/ ok x2",
            "Get index.h/ ok",
            "List index.h/ ok",
            "List snapshots/ ok",
            "ListMeta sessions/ ok",
            "reads packs/",
            "Put index.d/ ok",
            "PutIfAbsent index.h/ ok",
            "Exists sessions/ ok",
            "Put snapshots/ ok",
            "Delete sessions/ ok x3",
        ],
    );
}

#[test]
fn multi_writer_second_backup_of_the_same_tree_writes_a_segment() {
    let mut sim = conditional();
    let (c, d) = (sim.client("c"), sim.client("d"));
    sim.backup(c, "t0", &tree("t0")).expect_ok();
    let r = sim.backup(d, "t1", &tree("t0"));
    r.expect_ok();
    check(
        "multi-writer second backup, same tree",
        project(&run_records(&sim, &r, true)),
        &[
            "Put sessions/ ok",
            "Exists sessions/ ok x2",
            "Get index.h/ ok",
            "List index.h/ ok",
            "List snapshots/ ok",
            "ListMeta sessions/ ok",
            "reads packs/",
            "Put index.d/ ok",
            "PutIfAbsent index.h/ ok",
            "Exists sessions/ ok",
            "Put snapshots/ ok",
            "Delete sessions/ ok x3",
        ],
    );
}

/// A reuse-only commit (a delta whose additions another session committed
/// first) on a multi-writer repository: the commit machine forces
/// a head. Outside any run (no lock, no marker), at repository level as
/// `commit_with_no_new_additions_writes_no_index`.
#[test]
fn multi_writer_commit_with_no_new_additions_writes_a_head() {
    use crate::repo::machine::{StepCx, drive_blocking};
    let mut sim = conditional();
    let (c, d) = (sim.client("c"), sim.client("d"));
    sim.backup(c, "t0", &tree("t0")).expect_ok();
    let open = |client| {
        Repo::open(
            Box::new(sim.store().handle(client)),
            sim.store().passphrase(),
            Some(sim.client_dir(client).join("repo-cache")),
            OpenOptions::new()
                .with_index()
                .host_env(sim.host_env(client)),
        )
        .unwrap()
    };
    let (mut rc, mut rd) = (open(c), open(d));
    let data = b"one chunk both sessions store";
    let mut ws = rc.begin_write_session(&WriteSessionOptions::default());
    ws.store_chunk(&rc, data, Compression::None, PackType::Data)
        .unwrap();
    let mut first_ws = rd.begin_write_session(&WriteSessionOptions::default());
    first_ws
        .store_chunk(&rd, data, Compression::None, PackType::Data)
        .unwrap();
    commit_as(&mut rd.unchecked_writer(), first_ws, "first").unwrap();
    // The put-if-absent check runs before the commit, not in it.
    assert_eq!(rc.commit_mode().unwrap(), crate::repo::WriterMode::Multi);
    let from = sim.store().step();
    let mut m =
        crate::repo::BackupCommit::new(ws, snapshot_entry("second"), b"snapshot".to_vec(), None);
    let events = |_: crate::repo::control::CommitEvent| {};
    drive_blocking(
        &mut m,
        &mut StepCx {
            repo: &mut rc,
            fence: &crate::repo::Fence::unchecked(),
            events: &events,
        },
    )
    .unwrap();
    let recs: Vec<OpRecord> = sim
        .store()
        .log()
        .into_iter()
        .filter(|r| r.client == c && r.step > from)
        .collect();
    check(
        "multi-writer commit, no new additions",
        project(&recs),
        &[
            "PutPack packs/ ok",
            "Put sessions/ ok",
            "Get index.d/ ok",
            "Get index.h/ ok",
            "Get snapshots/ ok",
            "List index.h/ ok",
            "List snapshots/ ok",
            "PutIfAbsent index.h/ ok",
            "ListMeta index.h/ ok",
            "Put snapshots/ ok",
        ],
    );
}

#[test]
fn multi_writer_compact_after_delete_writes_a_record_and_a_segment() {
    let mut sim = conditional();
    let c = sim.client("c");
    sim.backup(c, "t0", &tree("t0")).expect_ok();
    sim.backup(c, "t1", &tree("t1")).expect_ok();
    sim.delete(c, &["t0"]).expect_ok();
    sim.advance(T_PACK + Duration::from_secs(60));
    let r = sim.compact(c);
    r.expect_ok();
    check(
        "multi-writer compact after delete",
        project(&run_records(&sim, &r, false)),
        &[
            "Get config ok",
            "Get index.h/ ok",
            "Get keys/ ok x2",
            "List index.h/ ok",
            "List snapshots/ ok",
            "PutIfAbsent sessions/ ok",
            "PutIfAbsent sessions/ exists",
            "Get sessions/ ok",
            "Delete sessions/ ok",
            "Put sessions/ ok",
            "Get config ok",
            "Get index.h/ ok x3",
            "Get snapshots/ ok",
            "List index.h/ ok x3",
            "List sessions/ ok",
            "List snapshots/ ok",
            "ListMeta index.d/ ok",
            "ListMeta sessions/ ok x2",
            "reads packs/",
            "Put index.d/ ok x2",
            "Exists sessions/ ok",
            "PutIfAbsent index.h/ ok",
            "Exists sessions/ ok x2",
            "Delete packs/ ok x2",
            "Exists sessions/ ok x2",
            "Get index.h/ ok x4",
            "List index.h/ ok",
            "List packmaps/ ok",
            "ListMeta index.d/ ok",
            "ListMeta index.h/ ok",
            "Delete sessions/ ok",
        ],
    );
}

#[test]
fn multi_writer_delete_removes_a_snapshot() {
    let mut sim = conditional();
    let c = sim.client("c");
    sim.backup(c, "t0", &tree("t0")).expect_ok();
    sim.backup(c, "t1", &tree("t1")).expect_ok();
    let r = sim.delete(c, &["t0"]);
    r.expect_ok();
    check(
        "multi-writer delete",
        project(&run_records(&sim, &r, false)),
        &[
            "Get config ok",
            "Get index.h/ ok",
            "Get keys/ ok x2",
            "List index.h/ ok",
            "List snapshots/ ok",
            "PutIfAbsent sessions/ ok",
            "PutIfAbsent sessions/ exists",
            "Get sessions/ ok",
            "Delete sessions/ ok",
            "Put sessions/ ok",
            "Exists sessions/ ok",
            "Get config ok",
            "ListMeta sessions/ ok",
            "Delete snapshots/ ok",
            "Delete sessions/ ok",
        ],
    );
}
