//! Invariant checker over the store's current visible state and the history.
//!
//! The checker reads through production code (`Repository::open`, the index
//! loader, snapshot and chunk reads, restore) on the read-only observer
//! handle, but takes object presence and sizes from the store map and
//! compares restore output with the harness's own record of each source.

use std::collections::{BTreeSet, HashMap};
use std::fmt;

use vykar_types::chunk_id::ChunkId;

use super::history::{ClientOp, ErrorClass, History, ObservationCause, RunOutcome};
use super::runner::SourceTree;
use super::store::SimStore;
use crate::commands::list;
use crate::commands::restore::{self, RestoreOptions};
use crate::repo::{OpenOptions, Repository, index_store};
use crate::snapshot::item::ItemType;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum Invariant {
    CommittedSnapshotsRestore = 1,
    AcknowledgedCommitsDurable = 2,
    NoSilentRemoval = 3,
    IndexIntegrity = 4,
    NoRefcountDeflation = 5,
    AllowedErrors = 8,
}

impl Invariant {
    /// Invariants of today's protocol that step 1 implements.
    pub const STEP1: &'static [Invariant] = &[
        Invariant::CommittedSnapshotsRestore,
        Invariant::AcknowledgedCommitsDurable,
        Invariant::NoSilentRemoval,
        Invariant::IndexIntegrity,
        Invariant::NoRefcountDeflation,
        Invariant::AllowedErrors,
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
    // 5
    RefcountDeflated,
    // 2
    AcknowledgedCommitMissing,
    // 3
    SilentRemoval,
    // 8
    DisallowedError,
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
            K::RefcountDeflated => Invariant::NoRefcountDeflation,
            K::AcknowledgedCommitMissing => Invariant::AcknowledgedCommitsDurable,
            K::SilentRemoval => Invariant::NoSilentRemoval,
            K::DisallowedError => Invariant::AllowedErrors,
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
    /// E.g. the index unloadable for 1 and 5, or a snapshot whose item
    /// stream does not load for 5.
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

    fn add(&mut self, kind: ViolationKind, subject: impl Into<String>, detail: impl Into<String>) {
        self.violations.push(Violation {
            invariant: kind.invariant(),
            kind,
            subject: subject.into(),
            detail: detail.into(),
        });
    }

    fn skip(
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

/// Storage keys of the base and segments the current head names.
pub fn head_object_keys(store: &SimStore) -> std::result::Result<BTreeSet<String>, String> {
    let tmp = tempfile::tempdir().map_err(|e| e.to_string())?;
    let repo = open_observer(store, tmp.path()).map_err(|e| e.to_string())?;
    let head = index_store::fetch_head(repo.storage.as_ref(), repo.crypto.as_ref())
        .map_err(|e| e.to_string())?;
    Ok(head
        .base
        .iter()
        .map(|b| b.name)
        .chain(head.segments.iter().map(|s| s.name))
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
                .chain(head.segments.iter().map(|s| (s.name, s.size)));
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

/// Invariants 1 and 5 over every manifest snapshot.
fn check_snapshots(
    store: &SimStore,
    history: &History,
    repo: &mut Repository,
    scratch: &std::path::Path,
    want_restore: bool,
    report: &mut Report,
) {
    let mut memo: HashMap<ChunkId, Option<ChunkFault>> = HashMap::new();
    let mut refs: HashMap<ChunkId, u32> = HashMap::new();
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
                report.skip(Invariant::NoRefcountDeflation, name, "meta unreadable");
                continue;
            }
        };
        for id in &meta.item_ptrs {
            *refs.entry(*id).or_insert(0) += 1;
            if let Some(f) = chunk_fault(repo, store, &mut memo, id) {
                report_chunk(report, name, id, f, true, "");
            }
        }
        if report.violations.len() > before {
            report.skip(
                Invariant::NoRefcountDeflation,
                name,
                "item stream not loadable: tree chunks faulty",
            );
            continue;
        }
        let stream = match list::load_snapshot_item_stream(repo, name, None) {
            Ok(s) => s,
            Err(e) => {
                report.add(
                    ViolationKind::ItemStreamUnreadable,
                    name,
                    format!("item stream unreadable: {e}"),
                );
                report.skip(
                    Invariant::NoRefcountDeflation,
                    name,
                    "item stream unreadable",
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
            report.skip(Invariant::NoRefcountDeflation, name, "items do not decode");
            continue;
        }
        for (path, id) in &file_chunks {
            *refs.entry(*id).or_insert(0) += 1;
            if let Some(f) = chunk_fault(repo, store, &mut memo, id) {
                report_chunk(report, name, id, f, false, path);
            }
        }
        if report.violations.len() == before && want_restore {
            restorable.push(name.clone());
        }
    }
    // Chunks absent from the index are invariant 1's; a skipped snapshot
    // only lowers the counts, so a deflation seen here is real.
    let index = repo.chunk_index();
    let mut deflated: Vec<(ChunkId, u32, u32)> = refs
        .iter()
        .filter_map(|(id, n)| {
            let e = index.get(id)?;
            (e.refcount < *n).then_some((*id, e.refcount, *n))
        })
        .collect();
    deflated.sort_by_key(|(id, _, _)| *id.as_bytes());
    for (id, r, n) in deflated {
        report.add(
            ViolationKind::RefcountDeflated,
            id.to_string(),
            format!("refcount {r} < references {n}"),
        );
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
            None,
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

    if want(Invariant::CommittedSnapshotsRestore) || want(Invariant::NoRefcountDeflation) {
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
            for inv in [
                Invariant::CommittedSnapshotsRestore,
                Invariant::NoRefcountDeflation,
            ] {
                if want(inv) {
                    report.skip(inv, "index", "chain does not load");
                }
            }
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
    report
}
