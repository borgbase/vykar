//! Invariant checker over the store's current visible state and the history.
//!
//! The checker reads through production code (`Repository::open`, the index
//! loader, snapshot and chunk reads, restore) on the read-only observer
//! handle, but takes object presence and sizes from the store map and
//! compares restore output with the harness's own record of each source.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::fmt;

use vykar_types::chunk_id::ChunkId;

use super::fault::ClientId;
use super::history::{ClientOp, ErrorClass, History, ObservationCause, RunOutcome};
use super::runner::SourceTree;
use super::store::SimStore;
use crate::commands::list;
use crate::commands::restore::{self, RestoreOptions};
use crate::repo::control::CancelToken;
use crate::repo::{OpenOptions, Repository, index_store};
use crate::snapshot::item::ItemType;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum Invariant {
    CommittedSnapshotsRestore = 1,
    AcknowledgedCommitsDurable = 2,
    NoSilentRemoval = 3,
    IndexIntegrity = 4,
    // 5 (no refcount deflation) is retired: stored refcounts mean nothing
    // since 05 step 8. The numbers of the others stay.
    /// Every protected write lands while the store holds exactly one lock
    /// key, written by the same run.
    LockExclusion = 7,
    AllowedErrors = 8,
    /// Once every lock and marker has aged out, a new client can back up
    /// and compact. Needs [`Sim::check_invariants`](super::runner::Sim::check_invariants).
    NoPermanentWedge = 9,
}

impl Invariant {
    /// Invariants of today's protocol that step 1 implements.
    pub const STEP1: &'static [Invariant] = &[
        Invariant::CommittedSnapshotsRestore,
        Invariant::AcknowledgedCommitsDurable,
        Invariant::NoSilentRemoval,
        Invariant::IndexIntegrity,
        Invariant::AllowedErrors,
    ];

    /// STEP1 plus invariants 7 and 9 (08 step 3).
    pub const STEP3: &'static [Invariant] = &[
        Invariant::CommittedSnapshotsRestore,
        Invariant::AcknowledgedCommitsDurable,
        Invariant::NoSilentRemoval,
        Invariant::IndexIntegrity,
        Invariant::AllowedErrors,
        Invariant::LockExclusion,
        Invariant::NoPermanentWedge,
    ];
}

/// What exactly was wrong; each kind belongs to one invariant.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum ViolationKind {
    // 4
    HeadUndecodable,
    HeadObjectAbsent,
    HeadObjectSizeMismatch,
    ChainUnloadable,
    IndexNamesAbsentPack,
    // 1
    MetaUnreadable,
    /// A tree (item-stream) chunk of this snapshot is not in the index.
    TreeChunkNotInIndex,
    /// A file chunk of this snapshot is not in the index.
    FileChunkNotInIndex,
    PackAbsent,
    ChunkUnreadable,
    ItemStreamUnreadable,
    RestoreFailed,
    RestoreDiffers,
    // 2
    AcknowledgedCommitMissing,
    // 3
    SilentRemoval,
    // 8
    DisallowedError,
    // 7
    /// A protected write while the store held no lock key.
    WriteWithoutLock,
    /// A protected write while the one lock key was another run's.
    WriteUnderOtherLock,
    /// A protected write while the store held more than one lock key.
    WriteUnderSeveralLocks,
    // 9
    WedgedBackup,
    WedgedCompact,
}

impl ViolationKind {
    pub fn invariant(self) -> Invariant {
        type K = ViolationKind;
        match self {
            K::HeadUndecodable
            | K::HeadObjectAbsent
            | K::HeadObjectSizeMismatch
            | K::ChainUnloadable
            | K::IndexNamesAbsentPack => Invariant::IndexIntegrity,
            K::MetaUnreadable
            | K::TreeChunkNotInIndex
            | K::FileChunkNotInIndex
            | K::PackAbsent
            | K::ChunkUnreadable
            | K::ItemStreamUnreadable
            | K::RestoreFailed
            | K::RestoreDiffers => Invariant::CommittedSnapshotsRestore,
            K::AcknowledgedCommitMissing => Invariant::AcknowledgedCommitsDurable,
            K::SilentRemoval => Invariant::NoSilentRemoval,
            K::DisallowedError => Invariant::AllowedErrors,
            K::WriteWithoutLock | K::WriteUnderOtherLock | K::WriteUnderSeveralLocks => {
                Invariant::LockExclusion
            }
            K::WedgedBackup | K::WedgedCompact => Invariant::NoPermanentWedge,
        }
    }
}

#[derive(Clone, Debug)]
pub struct Violation {
    pub invariant: Invariant,
    pub kind: ViolationKind,
    pub subject: String,
    pub detail: String,
}

/// An invariant that could not be evaluated for `subject`.
#[derive(Clone, Debug)]
pub struct NotEvaluated {
    pub invariant: Invariant,
    pub subject: String,
    pub reason: String,
}

#[derive(Clone, Debug, Default)]
pub struct Report {
    pub violations: Vec<Violation>,
    /// E.g. the index unloadable for 1.
    pub not_evaluated: Vec<NotEvaluated>,
}

impl Report {
    /// No violations and nothing left unevaluated.
    pub fn is_clean(&self) -> bool {
        self.violations.is_empty() && self.not_evaluated.is_empty()
    }

    pub fn violated(&self, inv: Invariant) -> bool {
        self.violations.iter().any(|v| v.invariant == inv)
    }

    pub fn violated_for(&self, inv: Invariant, subject: &str) -> bool {
        self.violations
            .iter()
            .any(|v| v.invariant == inv && v.subject == subject)
    }

    pub fn has_kind(&self, kind: ViolationKind, subject: &str) -> bool {
        self.violations
            .iter()
            .any(|v| v.kind == kind && v.subject == subject)
    }

    pub fn violated_set(&self) -> BTreeSet<Invariant> {
        self.violations.iter().map(|v| v.invariant).collect()
    }

    pub fn not_evaluated_for(&self, inv: Invariant, subject: &str) -> bool {
        self.not_evaluated
            .iter()
            .any(|n| n.invariant == inv && n.subject == subject)
    }

    /// Panics listing everything.
    pub fn assert_clean(&self) {
        assert!(self.is_clean(), "invariant report not clean:\n{self}");
    }

    /// Append `other`'s violations and skips.
    pub fn extend(&mut self, other: Report) {
        self.violations.extend(other.violations);
        self.not_evaluated.extend(other.not_evaluated);
    }

    pub(super) fn add(
        &mut self,
        kind: ViolationKind,
        subject: impl Into<String>,
        detail: impl Into<String>,
    ) {
        self.violations.push(Violation {
            invariant: kind.invariant(),
            kind,
            subject: subject.into(),
            detail: detail.into(),
        });
    }

    pub(super) fn skip(
        &mut self,
        invariant: Invariant,
        subject: impl Into<String>,
        reason: impl Into<String>,
    ) {
        self.not_evaluated.push(NotEvaluated {
            invariant,
            subject: subject.into(),
            reason: reason.into(),
        });
    }
}

impl fmt::Display for Report {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.is_clean() {
            return writeln!(f, "clean");
        }
        for v in &self.violations {
            writeln!(
                f,
                "violated {:?} ({:?}) for {}: {}",
                v.invariant, v.kind, v.subject, v.detail
            )?;
        }
        for n in &self.not_evaluated {
            writeln!(
                f,
                "not evaluated {:?} for {}: {}",
                n.invariant, n.subject, n.reason
            )?;
        }
        Ok(())
    }
}

/// Open the store read-only through the observer, with a fresh cache.
pub(crate) fn open_observer(
    store: &SimStore,
    cache: &std::path::Path,
) -> vykar_types::error::Result<Repository> {
    Repository::open(
        Box::new(store.observer()),
        None,
        Some(cache.to_path_buf()),
        OpenOptions::new()
            .tolerate_broken_index()
            .skip_key_backfill(),
    )
}

/// Storage keys of the base, segments and sweep record the current head
/// names.
pub fn head_object_keys(store: &SimStore) -> std::result::Result<BTreeSet<String>, String> {
    let tmp = tempfile::tempdir().map_err(|e| e.to_string())?;
    let repo = open_observer(store, tmp.path()).map_err(|e| e.to_string())?;
    let head = index_store::fetch_head(repo.storage.as_ref(), repo.crypto.as_ref())
        .map_err(|e| e.to_string())?;
    Ok(head
        .live_objects()
        .chain(head.sweep.record.iter().map(|r| r.name))
        .map(|n| index_store::object_key(&n))
        .collect())
}

#[derive(Clone)]
enum ChunkFault {
    NotInIndex,
    PackAbsent(String),
    Unreadable(String),
}

/// Present in the index, pack present, and readable (`read_chunk` verifies
/// the plaintext chunk ID on an unencrypted repository).
fn chunk_fault(
    repo: &mut Repository,
    store: &SimStore,
    memo: &mut HashMap<ChunkId, Option<ChunkFault>>,
    id: &ChunkId,
) -> Option<ChunkFault> {
    if let Some(known) = memo.get(id) {
        return known.clone();
    }
    let fault = match repo.chunk_index().get(id).map(|e| e.pack_id) {
        None => Some(ChunkFault::NotInIndex),
        Some(pack) if store.object(&pack.storage_key()).is_none() => {
            Some(ChunkFault::PackAbsent(pack.storage_key()))
        }
        Some(_) => repo
            .read_chunk(id)
            .err()
            .map(|e| ChunkFault::Unreadable(e.to_string())),
    };
    memo.insert(*id, fault.clone());
    fault
}

fn report_chunk(
    report: &mut Report,
    name: &str,
    id: &ChunkId,
    fault: ChunkFault,
    tree: bool,
    path: &str,
) {
    let at = if tree {
        "tree chunk".to_string()
    } else {
        format!("chunk of {path}")
    };
    match fault {
        ChunkFault::NotInIndex => report.add(
            if tree {
                ViolationKind::TreeChunkNotInIndex
            } else {
                ViolationKind::FileChunkNotInIndex
            },
            name,
            format!("{at} {id} not in index"),
        ),
        ChunkFault::PackAbsent(p) => report.add(
            ViolationKind::PackAbsent,
            name,
            format!("{at} {id}: pack {p} absent"),
        ),
        ChunkFault::Unreadable(e) => report.add(
            ViolationKind::ChunkUnreadable,
            name,
            format!("{at} {id} unreadable: {e}"),
        ),
    }
}

/// Invariant 4. Returns whether the index chain loaded.
fn check_index(store: &SimStore, repo: &mut Repository, report: &mut Report) -> bool {
    match index_store::fetch_head(repo.storage.as_ref(), repo.crypto.as_ref()) {
        Err(e) => report.add(
            ViolationKind::HeadUndecodable,
            "index",
            format!("head does not decode: {e}"),
        ),
        Ok(head) => {
            let named = head
                .base
                .iter()
                .map(|b| (b.name, b.size))
                .chain(head.segments.iter().map(|s| (s.name, s.size)))
                .chain(head.sweep.record.iter().map(|r| (r.name, r.size)));
            for (name, size) in named {
                let key = index_store::object_key(&name);
                match store.object(&key) {
                    None => report.add(
                        ViolationKind::HeadObjectAbsent,
                        &key,
                        "named by head, absent",
                    ),
                    Some(o) if o.data.len() as u64 != size => report.add(
                        ViolationKind::HeadObjectSizeMismatch,
                        &key,
                        format!("size mismatch: head {size}, stored {}", o.data.len()),
                    ),
                    Some(_) => {}
                }
            }
        }
    }
    if let Err(e) = repo.load_chunk_index_uncached() {
        report.add(
            ViolationKind::ChainUnloadable,
            "index",
            format!("chain does not load: {e}"),
        );
        return false;
    }
    let packs: BTreeSet<String> = repo
        .chunk_index()
        .iter()
        .map(|(_, e)| e.pack_id.storage_key())
        .collect();
    for key in packs {
        if store.object(&key).is_none() {
            report.add(
                ViolationKind::IndexNamesAbsentPack,
                &key,
                "index names an absent pack",
            );
        }
    }
    true
}

/// Invariant 1 over every manifest snapshot.
fn check_snapshots(
    store: &SimStore,
    history: &History,
    repo: &mut Repository,
    scratch: &std::path::Path,
    want_restore: bool,
    report: &mut Report,
) {
    let mut memo: HashMap<ChunkId, Option<ChunkFault>> = HashMap::new();
    // Snapshots without an index-level violation, restored at the end.
    let mut restorable: Vec<String> = Vec::new();
    let entries: Vec<String> = repo
        .manifest()
        .snapshots
        .iter()
        .map(|e| e.name.clone())
        .collect();
    for name in &entries {
        let before = report.violations.len();
        let meta = match list::load_snapshot_meta(repo, name) {
            Ok(m) => m,
            Err(e) => {
                report.add(
                    ViolationKind::MetaUnreadable,
                    name,
                    format!("meta unreadable: {e}"),
                );
                continue;
            }
        };
        for id in &meta.item_ptrs {
            if let Some(f) = chunk_fault(repo, store, &mut memo, id) {
                report_chunk(report, name, id, f, true, "");
            }
        }
        if report.violations.len() > before {
            continue;
        }
        let stream =
            match list::load_snapshot_item_stream_from_storage(repo, name, &CancelToken::new()) {
                Ok(s) => s,
                Err(e) => {
                    report.add(
                        ViolationKind::ItemStreamUnreadable,
                        name,
                        format!("item stream unreadable: {e}"),
                    );
                    continue;
                }
            };
        let mut file_chunks: Vec<(String, ChunkId)> = Vec::new();
        let decoded = list::for_each_decoded_item(&stream, |item| {
            if item.entry_type == ItemType::RegularFile {
                for c in &item.chunks {
                    file_chunks.push((item.path.clone(), c.id));
                }
            }
            Ok(())
        });
        if let Err(e) = decoded {
            report.add(
                ViolationKind::ItemStreamUnreadable,
                name,
                format!("items do not decode: {e}"),
            );
            continue;
        }
        for (path, id) in &file_chunks {
            if let Some(f) = chunk_fault(repo, store, &mut memo, id) {
                report_chunk(report, name, id, f, false, path);
            }
        }
        if report.violations.len() == before && want_restore {
            restorable.push(name.clone());
        }
    }
    // Last: restore clears the repository's chunk index when it finishes.
    for (i, name) in restorable.iter().enumerate() {
        let Some(expected) = history.trees.get(name) else {
            continue;
        };
        let dest = scratch.join(format!("restore-{i}"));
        let restored = restore::run_on_repo(
            repo,
            name,
            &dest.to_string_lossy(),
            None,
            RestoreOptions {
                xattrs_enabled: false,
                verify_chunks: true,
                continue_on_error: false,
            },
            &CancelToken::new(),
        );
        if let Err(e) = restored {
            report.add(
                ViolationKind::RestoreFailed,
                name,
                format!("restore failed: {e}"),
            );
            continue;
        }
        match SourceTree::read_from(&dest) {
            Err(e) => report.add(
                ViolationKind::RestoreFailed,
                name,
                format!("restored tree unreadable: {e}"),
            ),
            Ok(got) if &got != expected => report.add(
                ViolationKind::RestoreDiffers,
                name,
                format!("restore differs: {}", first_difference(expected, &got)),
            ),
            Ok(_) => {}
        }
    }
}

fn first_difference(expected: &SourceTree, got: &SourceTree) -> String {
    let e = expected.files();
    let g = got.files();
    for (path, data) in e {
        match g.get(path) {
            None => return format!("{path} missing"),
            Some(d) if d != data => return format!("{path} content differs"),
            Some(_) => {}
        }
    }
    g.keys()
        .find(|p| !e.contains_key(*p))
        .map(|p| format!("{p} unexpected"))
        .unwrap_or_else(|| "no difference".into())
}

fn check_durable(history: &History, report: &mut Report) {
    let Some(last) = history.observations.last() else {
        return;
    };
    let present: BTreeSet<&str> = last.names().collect();
    for (i, run) in history.runs.iter().enumerate() {
        let ClientOp::Backup { snapshot } = &run.op else {
            continue;
        };
        if !run.is_ok() {
            continue;
        }
        let removed_later = history.runs[i + 1..].iter().any(|r| {
            matches!(r.op, ClientOp::Delete { .. } | ClientOp::Prune)
                && r.removed().contains(snapshot)
        });
        if !removed_later && !present.contains(snapshot.as_str()) {
            report.add(
                ViolationKind::AcknowledgedCommitMissing,
                snapshot,
                format!("acknowledged commit (run {i}) missing"),
            );
        }
    }
}

fn check_no_silent_removal(history: &History, report: &mut Report) {
    for pair in history.observations.windows(2) {
        let (prev, next) = (&pair[0], &pair[1]);
        let after: BTreeSet<&str> = next.names().collect();
        let gone: BTreeSet<&str> = prev.names().filter(|n| !after.contains(n)).collect();
        if gone.is_empty() {
            continue;
        }
        let run = match &next.cause {
            ObservationCause::Run(i)
            | ObservationCause::Release {
                issuing_run: Some(i),
                ..
            } => history.runs.get(*i),
            _ => None,
        };
        for name in gone {
            let explained = run.is_some_and(|r| {
                matches!(r.op, ClientOp::Delete { .. } | ClientOp::Prune)
                    && r.removed().iter().any(|n| n == name)
            });
            if !explained {
                let by = match run {
                    Some(r) => format!("run {} {:?}", r.index, r.op),
                    None => format!("{:?}", next.cause),
                };
                report.add(
                    ViolationKind::SilentRemoval,
                    name,
                    format!("removed by {by}"),
                );
            }
        }
    }
}

fn check_errors(history: &History, report: &mut Report) {
    for run in &history.runs {
        if let RunOutcome::Err(e) = &run.outcome
            && e.class == ErrorClass::Disallowed
        {
            report.add(
                ViolationKind::DisallowedError,
                format!("run {} {:?}", run.index, run.op),
                &e.display,
            );
        }
    }
}

/// Prefixes of keys whose deletion only a lock holder may do.
const PROTECTED_DELETE_PREFIXES: &[&str] =
    &["snapshots/", "packs/", "index.d/", "index.h/", "packmaps/"];

/// A write only a lock holder may do: a head, or an index object.
fn protected_write(key: &str) -> bool {
    key.starts_with("index.h/") || key.starts_with("index.d/")
}

fn is_lock_key(key: &str) -> bool {
    key.starts_with("locks/") && key.ends_with(".json")
}

/// Invariant 7, as the store sees it: replay the op log, tracking which
/// lock keys exist and which (client, run) last wrote each, and judge every
/// applied protected write against the keys present just before it. Late
/// effects count at the step they land. The tamper client and the Init run
/// (`Repository::init` writes `index.h/1` with no lock) are exempt.
///
/// Limit: a key counts as held from its PUT. `acquire_lock` publishes a
/// tentative key that can coexist with the holder's until the loser deletes
/// it, so a protected write by the rightful holder in that window reads as
/// `WriteUnderSeveralLocks`. No scripted scenario holds a contender there;
/// an ordering search (08 step 4) must count a key as held only once its
/// writer's confirming LIST has returned.
fn check_lock_exclusion(store: &SimStore, history: &History, report: &mut Report) {
    let init_runs: BTreeSet<usize> = history
        .runs
        .iter()
        .filter(|r| matches!(r.op, ClientOp::Init))
        .map(|r| r.index)
        .collect();
    let mut held: BTreeMap<String, (ClientId, Option<usize>)> = BTreeMap::new();
    for rec in store.log() {
        if !rec.applied() {
            continue;
        }
        let protected = if rec.op.is_write() {
            protected_write(&rec.key)
        } else if rec.op.is_delete() {
            rec.all_keys()
                .iter()
                .any(|k| PROTECTED_DELETE_PREFIXES.iter().any(|p| k.starts_with(p)))
        } else {
            false
        };
        let exempt =
            rec.client == store.tamper_id() || rec.run.is_some_and(|r| init_runs.contains(&r));
        if protected && !exempt {
            let mine = (rec.client, rec.run);
            let kind = match held.len() {
                0 => Some(ViolationKind::WriteWithoutLock),
                1 if held.values().next() != Some(&mine) => {
                    Some(ViolationKind::WriteUnderOtherLock)
                }
                1 => None,
                _ => Some(ViolationKind::WriteUnderSeveralLocks),
            };
            if let Some(kind) = kind {
                let subject = rec
                    .run
                    .map_or_else(|| "no run".to_string(), |r| format!("run {r}"));
                let holders: Vec<String> = held
                    .iter()
                    .map(|(k, (c, r))| format!("{k} by {} run {r:?}", store.client_name(*c)))
                    .collect();
                report.add(
                    kind,
                    subject,
                    format!(
                        "step {} {:?} {} by {}; locks held: {holders:?}",
                        rec.step,
                        rec.op,
                        rec.key,
                        store.client_name(rec.client)
                    ),
                );
            }
        }
        if rec.op.is_write() && is_lock_key(&rec.key) {
            held.insert(rec.key.clone(), (rec.client, rec.run));
        }
        for k in rec.deleted_keys() {
            held.remove(k);
        }
    }
}

pub fn check_invariants(store: &SimStore, history: &History, set: &[Invariant]) -> Report {
    let mut report = Report::default();
    let tmp = tempfile::tempdir().expect("checker scratch");
    let cache = tmp.path().join("cache");
    let mut repo = match open_observer(store, &cache) {
        Ok(r) => r,
        Err(e) => {
            for inv in set {
                report.skip(*inv, "repository", format!("open failed: {e}"));
            }
            return report;
        }
    };
    let want = |inv| set.contains(&inv);

    let mut index_report = Report::default();
    let index_ok = check_index(store, &mut repo, &mut index_report);
    if want(Invariant::IndexIntegrity) {
        report.violations.extend(index_report.violations);
    }

    if want(Invariant::CommittedSnapshotsRestore) {
        if index_ok {
            let mut snap_report = Report::default();
            check_snapshots(
                store,
                history,
                &mut repo,
                tmp.path(),
                want(Invariant::CommittedSnapshotsRestore),
                &mut snap_report,
            );
            report.violations.extend(
                snap_report
                    .violations
                    .into_iter()
                    .filter(|v| want(v.invariant)),
            );
            report.not_evaluated.extend(
                snap_report
                    .not_evaluated
                    .into_iter()
                    .filter(|n| want(n.invariant)),
            );
        } else {
            report.skip(
                Invariant::CommittedSnapshotsRestore,
                "index",
                "chain does not load",
            );
        }
    }
    if want(Invariant::AcknowledgedCommitsDurable) {
        check_durable(history, &mut report);
    }
    if want(Invariant::NoSilentRemoval) {
        check_no_silent_removal(history, &mut report);
    }
    if want(Invariant::AllowedErrors) {
        check_errors(history, &mut report);
    }
    if want(Invariant::LockExclusion) {
        check_lock_exclusion(store, history, &mut report);
    }
    if want(Invariant::NoPermanentWedge) {
        report.skip(
            Invariant::NoPermanentWedge,
            "repository",
            "needs Sim::check_invariants",
        );
    }
    report
}
