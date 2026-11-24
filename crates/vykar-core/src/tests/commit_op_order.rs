//! The storage operations of the backup commit and the index write, in
//! order, pinned before they were split into steppable machines (08 step 4).
//! Each scenario projects one run's op log to (op, key class, outcome);
//! between two mutations, reads form a multiset (sorted), since some of them
//! run concurrently. `VYKAR_PRINT_OP_ORDER=1` prints the projections.

// Printing is the point of the env-driven helpers here (projections,
// replays, the measurement); every other path asserts.
#![allow(clippy::print_stderr)]

use std::time::Duration;

use crate::compress::Compression;
use crate::repo::ages::T_PACK;
use crate::repo::pack::PackType;
use crate::repo::{OpenOptions, Repository, WriteSessionOptions};
use crate::sim::log::key_class;
use crate::sim::{OpRecord, RunRecord, Sim, SourceTree};

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

/// The records of `run`. A backup's from its commit lock on (the
/// `locks/` reads right before its first `locks/` write; phase 1 uploads
/// from worker threads), without `sessions/*.index` writes (the final
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
    // Dropped before the start is looked for: a background journal PUT
    // logged between the lock's LIST and its PUT would end the walk back.
    let mine: Vec<OpRecord> = mine
        .into_iter()
        .filter(|r| {
            !(r.op.is_write() && r.key.starts_with("sessions/") && r.key.ends_with(".index"))
        })
        .collect();
    let mut start = mine
        .iter()
        .position(|r| r.op.is_write() && r.key.starts_with("locks/"))
        .unwrap_or(mine.len());
    while start > 0
        && !mine[start - 1].op.is_mutation()
        && mine[start - 1].key.starts_with("locks/")
    {
        start -= 1;
    }
    mine.into_iter().skip(start).collect()
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
            "ListMeta locks/ ok",
            "Put locks/ ok",
            "Exists sessions/ ok",
            "Get index.h/ ok",
            "List index.h/ ok",
            "List snapshots/ ok",
            "ListMeta locks/ ok",
            "reads packs/",
            "Put index.d/ ok",
            "Exists locks/ ok",
            "Put index.h/ ok",
            "Exists locks/ ok",
            "Put snapshots/ ok",
            "Delete sessions/ ok x2",
            "Delete locks/ ok",
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
            "ListMeta locks/ ok",
            "Put locks/ ok",
            "Exists sessions/ ok",
            "Get index.h/ ok",
            "List index.h/ ok",
            "List snapshots/ ok",
            "ListMeta locks/ ok",
            "reads packs/",
            "Put index.d/ ok",
            "Exists locks/ ok",
            "Put index.h/ ok",
            "Exists locks/ ok",
            "Put snapshots/ ok",
            "Delete sessions/ ok x2",
            "Delete locks/ ok",
        ],
    );
}

/// A8: a commit whose delta's additions are all in the index already
/// (another session committed the same chunk first) writes no index
/// object and no head: it goes from the pack check to the fence and the
/// snapshot PUT. At repository level, since two `Sim` backups never share
/// a tree chunk (each backs up its own source directory).
#[test]
fn commit_with_no_new_additions_writes_no_index() {
    let mut sim = Sim::new();
    let (c, d) = (sim.client("c"), sim.client("d"));
    sim.backup(c, "t0", &tree("t0")).expect_ok();
    let store = sim.store().clone();
    let other = store.handle(d);
    let cache = tempfile::tempdir().unwrap();
    let mut commit_from = 0;
    let r = sim.custom(c, "no additions", |repo: &mut Repository| {
        let data = b"one chunk both sessions store";
        repo.begin_write_session(&WriteSessionOptions::default())?;
        repo.store_chunk(data, Compression::None, PackType::Data)?;
        let mut first = Repository::open(
            Box::new(other),
            None,
            Some(cache.path().to_path_buf()),
            OpenOptions::new().with_index(),
        )?;
        first.begin_write_session(&WriteSessionOptions::default())?;
        first.store_chunk(data, Compression::None, PackType::Data)?;
        commit_as(&mut first, "first")?;
        commit_from = store.log().last().map_or(0, |r| r.step);
        commit_as(repo, "second")
    });
    r.expect_ok();
    let recs: Vec<OpRecord> = run_records(&sim, &r, false)
        .into_iter()
        .filter(|r| r.step > commit_from)
        .collect();
    assert!(
        !recs
            .iter()
            .any(|r| r.op.is_write()
                && (r.key.starts_with("index.d/") || r.key.starts_with("index.h/"))),
        "the commit wrote an index object"
    );
    check(
        "commit, no new additions",
        project(&recs),
        &[
            "PutPack packs/ ok",
            "Put sessions/ ok",
            "Exists locks/ ok",
            "Get index.d/ ok",
            "Get index.h/ ok",
            "Get snapshots/ ok",
            "List index.h/ ok",
            "List snapshots/ ok",
            "Put snapshots/ ok",
            "Delete locks/ ok",
        ],
    );
}

/// Commit the active session of `repo` as snapshot `name`.
fn commit_as(repo: &mut Repository, name: &str) -> vykar_types::error::Result<()> {
    let entry = crate::repo::manifest::SnapshotEntry {
        name: name.into(),
        id: vykar_types::snapshot_id::SnapshotId::generate(),
        time: chrono::Utc::now(),
        source_label: String::new(),
        label: String::new(),
        source_paths: Vec::new(),
        hostname: String::new(),
    };
    repo.commit_session_blocking(
        entry,
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
            "List index.h/ ok",
            "List snapshots/ ok",
            "ListMeta locks/ ok",
            "Put locks/ ok",
            "Get index.h/ ok x2",
            "Get snapshots/ ok",
            "List index.h/ ok x2",
            "List snapshots/ ok",
            "ListMeta index.d/ ok",
            "ListMeta locks/ ok",
            "ListMeta sessions/ ok",
            "reads packs/",
            "Put index.d/ ok x2",
            "Exists locks/ ok",
            "Put index.h/ ok",
            "Exists locks/ ok",
            "Delete packs/ ok x2",
            "Get index.h/ ok x4",
            "List index.h/ ok",
            "List packmaps/ ok",
            "ListMeta index.d/ ok",
            "ListMeta index.h/ ok",
            "Delete locks/ ok",
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
            "List index.h/ ok",
            "List snapshots/ ok",
            "ListMeta locks/ ok",
            "Put locks/ ok",
            "Get index.h/ ok x2",
            "Get snapshots/ ok",
            "List index.h/ ok x2",
            "List snapshots/ ok",
            "ListMeta index.d/ ok",
            "ListMeta locks/ ok",
            "ListMeta sessions/ ok",
            "reads packs/",
            "Put index.d/ ok",
            "Exists locks/ ok",
            "Put index.h/ ok",
            "Get index.h/ ok x3",
            "List index.h/ ok",
            "List packmaps/ ok",
            "ListMeta index.d/ ok",
            "ListMeta index.h/ ok",
            "Delete locks/ ok",
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
    let r = sim.custom(c, "fold", |repo: &mut Repository| {
        repo.load_chunk_index_under_lock()?;
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
            "List index.h/ ok",
            "List snapshots/ ok",
            "ListMeta locks/ ok",
            "Put locks/ ok",
            "Get index.h/ ok x2",
            "List index.h/ ok x2",
            "ListMeta index.d/ ok",
            "ListMeta locks/ ok",
            "Put index.d/ ok",
            "Exists locks/ ok",
            "Put index.h/ ok",
            "Get index.h/ ok x2",
            "ListMeta index.d/ ok",
            "ListMeta index.h/ ok",
            "Delete locks/ ok",
        ],
    );
}
