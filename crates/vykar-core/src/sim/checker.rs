//! Invariant checker over the store's current visible state and the history.
//!
//! The checker reads through production code (`Repo::open`, the index
//! loader, snapshot and chunk reads, restore) on the read-only observer
//! handle, but takes object presence and sizes from the store map and
//! compares restore output with the harness's own record of each source.
//!
//! What it covers of the commit protocol's invariants (04 "Invariants")
//! today, by this checker's numbers; later parts extend it:
//!
//! | 04 | Here |
//! |---|---|
//! | I1 (no pack deleted while referenced, young or in a marker's horizon) | 1 (a committed snapshot's packs exist), plus 10: no pack deleted younger than T_pack or while its own session's marker was live, repack sources excepted |
//! | I2 (every retained head names only objects that exist) | 4, for every retained head (latest − 16 and newer) on stores that honour put-if-absent; the latest head elsewhere |
//! | I3 (heads immutable, one per generation) | 12 on stores that honour put-if-absent; 7 (lock exclusion) stands in on the fallback |
//! | I4 (every listed snapshot covered by some retained head) | 1, against the latest head |
//! | I5 (no removal without a completed mark) | not yet (the H5 regression is scripted) |
//! | I6 (no snapshot references a chunk missing from its commit head) | 1 approximates it against the latest head |
//! | I7 (a reader returns correct data or a typed error) | 1 (restore against the source), 8 |
//! | I8 (a marker gap with an old pack does not commit) | 11 |

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::fmt;

use vykar_types::chunk_id::ChunkId;

use super::fault::{ClientId, OpKind};
use super::history::{ClientOp, ErrorClass, History, ObservationCause, RunOutcome, RunRecord};
use super::log::{OpOutcome, OpRecord};
use super::runner::SourceTree;
use super::store::SimStore;
use crate::commands::list;
use crate::commands::restore::{self, RestoreOptions};
use crate::repo::control::CancelToken;
use crate::repo::{OpenOptions, Repo, index_store};
use crate::snapshot::item::ItemType;
use chrono::{DateTime, Utc};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum Invariant {
    CommittedSnapshotsRestore = 1,
    AcknowledgedCommitsDurable = 2,
    NoSilentRemoval = 3,
    IndexIntegrity = 4,
    // 5 (no reference-count deflation) is retired: no counts are stored
    // since 05 step 8. The numbers of the others stay.
    /// Every protected write lands while the store holds exactly one lock
    /// key, written by the same run.
    LockExclusion = 7,
    AllowedErrors = 8,
    /// Once every lock and marker has aged out, a new client can back up
    /// and compact. Needs [`Sim::check_invariants`](super::runner::Sim::check_invariants).
    NoPermanentWedge = 9,
    /// I1's age and horizon parts, from the op log: a pack (never
    /// committed, or emptied by a sweep; repack sources excepted) is deleted
    /// only once T_PACK old, and not while its writer's run is in flight
    /// with a marker key present.
    OrphanGrace = 10,
    /// I8: a backup whose marker key was deleted by another client, or not
    /// written for more than MARKER_GAP_LIMIT, before its head PUT, and
    /// whose oldest own pack was older than PACK_AGE_LIMIT −
    /// HEAD_LANDING_ALLOWANCE at the last op it issued before that PUT,
    /// publishes no head.
    MarkerGapCommit = 11,
    /// I3 (04 step 8): no stored head is replaced by other bytes, and each
    /// retained head names every live object of its predecessor (it was
    /// written over it). Asked for (`STEP8`) on stores that honour
    /// put-if-absent; with it, invariant 4 judges every retained head.
    HeadsImmutable = 12,
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

    /// STEP3 plus invariants 10 and 11 (04 step 7).
    pub const STEP7: &'static [Invariant] = &[
        Invariant::CommittedSnapshotsRestore,
        Invariant::AcknowledgedCommitsDurable,
        Invariant::NoSilentRemoval,
        Invariant::IndexIntegrity,
        Invariant::AllowedErrors,
        Invariant::LockExclusion,
        Invariant::NoPermanentWedge,
        Invariant::OrphanGrace,
        Invariant::MarkerGapCommit,
    ];

    /// STEP7 plus invariant 12 (04 step 8), for stores that honour
    /// put-if-absent; there invariant 4 also judges every retained head.
    pub const STEP8: &'static [Invariant] = &[
        Invariant::CommittedSnapshotsRestore,
        Invariant::AcknowledgedCommitsDurable,
        Invariant::NoSilentRemoval,
        Invariant::IndexIntegrity,
        Invariant::AllowedErrors,
        Invariant::LockExclusion,
        Invariant::NoPermanentWedge,
        Invariant::OrphanGrace,
        Invariant::MarkerGapCommit,
        Invariant::HeadsImmutable,
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
    /// A retained head other than the latest names an absent object (on
    /// stores that honour put-if-absent).
    RetainedHeadObjectAbsent,
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
    // 10
    /// A pack no head named was deleted younger than T_PACK.
    YoungOrphanDeleted,
    /// A pack no head named was deleted while its writer's run was in
    /// flight with a marker key present.
    LiveSessionPackDeleted,
    // 11
    /// A backup published a head after a marker reap or gap with packs
    /// past the commit rule's age limit.
    OldPackCommittedAfterMarkerGap,
    // 12
    /// A stored head was replaced by other bytes.
    HeadOverwritten,
    /// A retained head does not name a live object of its predecessor.
    HeadSkipsPredecessor,
}

impl ViolationKind {
    pub fn invariant(self) -> Invariant {
        type K = ViolationKind;
        match self {
            K::HeadUndecodable
            | K::HeadObjectAbsent
            | K::HeadObjectSizeMismatch
            | K::ChainUnloadable
            | K::IndexNamesAbsentPack
            | K::RetainedHeadObjectAbsent => Invariant::IndexIntegrity,
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
            K::YoungOrphanDeleted | K::LiveSessionPackDeleted => Invariant::OrphanGrace,
            K::OldPackCommittedAfterMarkerGap => Invariant::MarkerGapCommit,
            K::HeadOverwritten | K::HeadSkipsPredecessor => Invariant::HeadsImmutable,
        }
    }
}

#[derive(Clone, Debug)]
pub struct Violation {
    pub invariant: Invariant,
    pub kind: ViolationKind,
    pub subject: String,
    pub detail: String,
    /// The store step of the operation it was found at, when there is one.
    pub step: Option<u64>,
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
        self.add_at(kind, subject, None, detail);
    }

    /// [`Self::add`] for a violation found at store step `step`.
    pub(super) fn add_at(
        &mut self,
        kind: ViolationKind,
        subject: impl Into<String>,
        step: Option<u64>,
        detail: impl Into<String>,
    ) {
        self.violations.push(Violation {
            invariant: kind.invariant(),
            kind,
            subject: subject.into(),
            detail: detail.into(),
            step,
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
) -> vykar_types::error::Result<Repo> {
    Repo::open(
        Box::new(store.observer()),
        store.passphrase(),
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
    let head = index_store::fetch_head(repo.storage().as_ref(), repo.crypto().as_ref())
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
    repo: &mut Repo,
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
fn check_index(store: &SimStore, repo: &mut Repo, report: &mut Report) -> bool {
    match index_store::fetch_head(repo.storage().as_ref(), repo.crypto().as_ref()) {
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

/// The retained heads (latest − `HEADS_KEPT` and newer) that decode, by
/// generation, read from the store map.
fn retained_heads(store: &SimStore, repo: &Repo) -> BTreeMap<u64, crate::index::head::IndexHead> {
    let gens: Vec<u64> = store
        .keys(index_store::HEAD_PREFIX)
        .iter()
        .filter_map(|k| vykar_protocol::head_generation(k))
        .collect();
    let Some(&latest) = gens.iter().max() else {
        return BTreeMap::new();
    };
    let floor = latest.saturating_sub(index_store::HEADS_KEPT);
    gens.into_iter()
        .filter(|g| *g >= floor)
        .filter_map(|g| {
            let data = store.object(&index_store::head_key(g))?.data;
            index_store::classify_head(g, &data, repo.crypto().as_ref())
                .into_head()
                .ok()
                .map(|h| (g, h))
        })
        .collect()
}

/// Invariant 4 for the retained heads other than the latest (04 I2): each
/// names only objects that exist.
fn check_retained_heads(store: &SimStore, repo: &Repo, report: &mut Report) {
    let heads = retained_heads(store, repo);
    let latest = heads.keys().max().copied();
    for (g, head) in &heads {
        if Some(*g) == latest {
            continue;
        }
        for name in head.named_objects() {
            let key = index_store::object_key(&name);
            if store.object(&key).is_none() {
                report.add(
                    ViolationKind::RetainedHeadObjectAbsent,
                    &key,
                    format!("named by retained head {g}, absent"),
                );
            }
        }
    }
}

/// Invariant 12 (04 I3): no head replaced by other bytes, and every
/// retained head names (live or retired) each live object of the retained
/// head one generation below it. A head written by `check --repair`, the
/// upgrade or init (a write without a previous head) is exempt from the
/// second part.
fn check_heads_immutable(store: &SimStore, history: &History, repo: &Repo, report: &mut Report) {
    for (key, step, writer) in store.head_overwrites() {
        report.add_at(
            ViolationKind::HeadOverwritten,
            &key,
            Some(step),
            format!(
                "step {step}: {key} replaced by {} with other bytes",
                store.client_name(writer)
            ),
        );
    }
    let log = store.log();
    let headless_writer = |key: &str| {
        log.iter()
            .rev()
            .find(|r| r.op.is_write() && r.applied() && r.key == key)
            .and_then(|r| r.run)
            .and_then(|i| history.runs.iter().find(|x| x.index == i))
            .is_some_and(|run| match &run.op {
                ClientOp::Init | ClientOp::CheckRepair => true,
                ClientOp::Custom { label } => label.contains("upgrade"),
                _ => false,
            })
    };
    let heads = retained_heads(store, repo);
    for (g, head) in &heads {
        let Some(prev) = g.checked_sub(1).and_then(|p| heads.get(&p)) else {
            continue;
        };
        let key = index_store::head_key(*g);
        if headless_writer(&key) {
            continue;
        }
        let named: std::collections::HashSet<_> = head.named_objects().collect();
        let skipped: Vec<String> = prev
            .live_objects()
            .filter(|n| !named.contains(n))
            .map(|n| index_store::object_key(&n))
            .collect();
        if !skipped.is_empty() {
            report.add(
                ViolationKind::HeadSkipsPredecessor,
                &key,
                format!("does not name {skipped:?} of head {}", g - 1),
            );
        }
    }
}

/// Invariant 1 over every manifest snapshot.
fn check_snapshots(
    store: &SimStore,
    history: &History,
    repo: &mut Repo,
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
            report.add(ViolationKind::DisallowedError, run_subject(run), &e.display);
        }
    }
}

/// The subject of a run's `DisallowedError`.
pub(crate) fn run_subject(run: &RunRecord) -> String {
    format!("run {} {:?}", run.index, run.op)
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
/// effects count at the step they land. The tamper client, the Init run
/// (`Repo::init` writes `index.h/1` with no lock) and lock-free runs
/// (`RunRecord::lock_free`, 04 step 9: they write heads with put-if-absent
/// and no lock) are exempt; every lock-mode run is judged, beside lock-free
/// ones too.
///
/// Limit: a key counts as held from its PUT. `acquire_lock` publishes a
/// tentative key that can coexist with the holder's until the loser deletes
/// it, so a protected write by the rightful holder in that window reads as
/// `WriteUnderSeveralLocks`. No scripted scenario holds a contender there,
/// and in the driver (`sim::driver`) `acquire_lock` is one step, so a
/// contender's tentative key never coexists with the holder's across a
/// decision. A delayed or duplicated lock PUT that lands later is a key
/// with no holder; the driver names that shape (`OrphanLockKey`).
fn check_lock_exclusion(store: &SimStore, history: &History, report: &mut Report) {
    let exempt_runs: BTreeSet<usize> = history
        .runs
        .iter()
        .filter(|r| matches!(r.op, ClientOp::Init) || r.lock_free)
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
            rec.client == store.tamper_id() || rec.run.is_some_and(|r| exempt_runs.contains(&r));
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
                report.add_at(
                    kind,
                    subject,
                    Some(rec.step),
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

fn is_marker_key(key: &str) -> bool {
    key.starts_with("sessions/") && key.ends_with(".json")
}

fn is_head_key(key: &str) -> bool {
    key.starts_with("index.h/")
}

fn applied_write(r: &OpRecord) -> bool {
    r.applied() && r.op.is_write()
}

/// A record of an operation issued, not of a parked effect landing.
fn issued(r: &OpRecord) -> bool {
    !matches!(
        r.outcome,
        OpOutcome::Released(_) | OpOutcome::ReleasedRefused(_)
    )
}

fn violation(kind: ViolationKind, subject: String, step: u64, detail: String) -> Violation {
    Violation {
        invariant: kind.invariant(),
        kind,
        subject,
        detail,
        step: Some(step),
    }
}

/// `b − a` as a std duration, zero when negative.
fn between(a: DateTime<Utc>, b: DateTime<Utc>) -> std::time::Duration {
    (b - a).to_std().unwrap_or_default()
}

/// Invariant 10 over the op log. Every applied delete of a `packs/` key by
/// a run (tamper deletes are outside runs) is judged against the latest
/// applied write of the key before it: the pack must have been at least
/// `T_PACK` old, and its writer's run must not have been in flight with
/// one of its marker keys present. Whether a head ever named the pack does
/// not matter (04 I1 in full since phase F part 8c: a pack a sweep empties
/// obeys the same rule as one never committed).
///
/// A repack source is not judged: the deleting run read the pack (`Get`
/// or `GetRange` with outcome `Ok`) before the delete, as a client-side
/// repack does with its sources and an orphan delete never does (the
/// simulator's store offers no server-side repack). Its live entries were
/// relocated by chunk ID before the delete (04 step 7 D9). Only the
/// writer's own session is checked for the horizon: the driver never
/// refreshes markers, so no other session's horizon reaches further back
/// than its own registration.
pub(crate) fn orphan_grace(log: &[OpRecord], runs: &[RunRecord]) -> Vec<Violation> {
    let t_pack = crate::repo::ages::T_PACK;
    let mut out = Vec::new();
    // Marker keys present, replayed from applied writes and deletes.
    let mut present: BTreeSet<&str> = BTreeSet::new();
    for (pos, del) in log.iter().enumerate() {
        if del.run.is_some() {
            for key in del.deleted_keys() {
                if !key.starts_with("packs/") {
                    continue;
                }
                let Some(put_pos) = log[..pos]
                    .iter()
                    .rposition(|r| applied_write(r) && r.key == key)
                else {
                    continue;
                };
                let put = &log[put_pos];
                let Some(writer) = put.run else {
                    continue;
                };
                let repack_source = log[put_pos + 1..pos].iter().any(|r| {
                    r.run == del.run
                        && matches!(r.op, OpKind::Get | OpKind::GetRange)
                        && r.outcome == OpOutcome::Ok
                        && r.key == key
                });
                if repack_source {
                    continue;
                }
                let age = between(put.at, del.at);
                let subject = format!("run {writer} {key}");
                if age < t_pack {
                    out.push(violation(
                        ViolationKind::YoungOrphanDeleted,
                        subject,
                        del.step,
                        format!(
                            "step {} deleted {key} {} s after its PUT (step {}), not a repack source",
                            del.step,
                            age.as_secs(),
                            put.step
                        ),
                    ));
                    continue;
                }
                let in_flight = runs
                    .iter()
                    .find(|r| r.index == writer)
                    .is_none_or(|r| r.first_step <= del.step && del.step <= r.last_step);
                let marker_present = log[..pos]
                    .iter()
                    .filter(|r| r.run == Some(writer) && applied_write(r) && is_marker_key(&r.key))
                    .any(|r| present.contains(r.key.as_str()));
                if in_flight && marker_present {
                    out.push(violation(
                        ViolationKind::LiveSessionPackDeleted,
                        subject,
                        del.step,
                        format!(
                            "step {} deleted {key} while its writer's run was in flight with a marker key present",
                            del.step
                        ),
                    ));
                }
            }
        }
        if del.applied() {
            if del.op.is_write() && is_marker_key(&del.key) {
                present.insert(del.key.as_str());
            }
            for k in del.deleted_keys() {
                present.remove(k);
            }
        }
    }
    out
}

/// Invariant 11 over the op log and the backup runs: per applied
/// `index.h/` write `h` of a backup run, the marker was bad (an applied
/// delete of one of its marker keys by another client before `h`, or `h`
/// more than `MARKER_GAP_LIMIT` after its latest applied marker write)
/// and its packs were too old (the `at` of the last op it issued before
/// `h` was issued, minus the earliest of its applied pack PUTs, plus
/// `HEAD_LANDING_ALLOWANCE`, exceeds `PACK_AGE_LIMIT`).
pub(crate) fn marker_gap_commit(log: &[OpRecord], runs: &[RunRecord]) -> Vec<Violation> {
    use crate::repo::ages::{HEAD_LANDING_ALLOWANCE, PACK_AGE_LIMIT};
    use crate::repo::lock::MARKER_GAP_LIMIT;
    let mut out = Vec::new();
    for run in runs {
        if !matches!(run.op, ClientOp::Backup { .. }) {
            continue;
        }
        let mine = |r: &&OpRecord| r.run == Some(run.index);
        for (h_pos, h) in log.iter().enumerate() {
            if !(mine(&h) && applied_write(h) && is_head_key(&h.key)) {
                continue;
            }
            let before = &log[..h_pos];
            let markers: BTreeSet<&str> = before
                .iter()
                .filter(mine)
                .filter(|r| applied_write(r) && is_marker_key(&r.key))
                .map(|r| r.key.as_str())
                .collect();
            let reaped = before.iter().any(|r| {
                r.client != run.client && r.deleted_keys().iter().any(|k| markers.contains(k))
            });
            let last_marker = before
                .iter()
                .filter(mine)
                .filter(|r| applied_write(r) && is_marker_key(&r.key))
                .map(|r| r.at)
                .max();
            let gap = last_marker.is_some_and(|m| between(m, h.at) > MARKER_GAP_LIMIT);
            if !(reaped || gap) {
                continue;
            }
            let issue_pos = match h.outcome {
                OpOutcome::Released(id) => log
                    .iter()
                    .position(|r| matches!(r.outcome, OpOutcome::Parked(p, _) if p == id))
                    .unwrap_or(h_pos),
                _ => h_pos,
            };
            let Some(t_last) = log[..issue_pos]
                .iter()
                .rev()
                .find(|r| mine(r) && issued(r))
                .map(|r| r.at)
            else {
                continue;
            };
            let Some(oldest) = before
                .iter()
                .filter(mine)
                .filter(|r| applied_write(r) && r.key.starts_with("packs/"))
                .map(|r| r.at)
                .min()
            else {
                continue;
            };
            let age = between(oldest, t_last);
            if age + HEAD_LANDING_ALLOWANCE > PACK_AGE_LIMIT {
                out.push(violation(
                    ViolationKind::OldPackCommittedAfterMarkerGap,
                    run_subject(run),
                    h.step,
                    format!(
                        "step {} wrote {} with a {} marker; its oldest pack was {} s old at its last op",
                        h.step,
                        h.key,
                        if reaped { "reaped" } else { "gapped" },
                        age.as_secs()
                    ),
                ));
            }
        }
    }
    out
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
    // Invariant 12 and the retained heads' part of 4 are asked for
    // together (`STEP8`): on the fallback (`StoreKind::Plain`) a lost lock
    // or a late head replaces heads by design (07 stands in for I3 there).
    if want(Invariant::IndexIntegrity) {
        report.violations.extend(index_report.violations);
        if want(Invariant::HeadsImmutable) {
            check_retained_heads(store, &repo, &mut report);
        }
    }
    if want(Invariant::HeadsImmutable) {
        check_heads_immutable(store, history, &repo, &mut report);
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
    if want(Invariant::OrphanGrace) || want(Invariant::MarkerGapCommit) {
        let log = store.log();
        if want(Invariant::OrphanGrace) {
            report.violations.extend(orphan_grace(&log, &history.runs));
        }
        if want(Invariant::MarkerGapCommit) {
            report
                .violations
                .extend(marker_gap_commit(&log, &history.runs));
        }
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sim::history::{ClientOp, RunOutcome, RunRecord};

    const BACKUP: ClientId = ClientId(2);
    const OTHER: ClientId = ClientId(3);

    fn t0() -> DateTime<Utc> {
        DateTime::parse_from_rfc3339("2026-01-01T00:00:00Z")
            .unwrap()
            .with_timezone(&Utc)
    }

    fn mins(m: i64) -> DateTime<Utc> {
        t0() + chrono::Duration::minutes(m)
    }

    /// A hand-built log: records in step order.
    #[derive(Default)]
    struct Log(Vec<OpRecord>);

    impl Log {
        fn op(&mut self, client: ClientId, run: usize, op: OpKind, key: &str, at: DateTime<Utc>) {
            let step = self.0.len() as u64 + 1;
            self.0.push(OpRecord {
                step,
                client,
                run: Some(run),
                thread: "t".into(),
                op,
                key: key.into(),
                keys: Vec::new(),
                absent: Vec::new(),
                len: None,
                outcome: OpOutcome::Ok,
                label: None,
                at,
            });
        }
    }

    fn run(index: usize, client: ClientId, op: ClientOp, last_step: u64) -> RunRecord {
        RunRecord {
            index,
            client,
            op,
            first_step: 0,
            last_step,
            outcome: RunOutcome::Ok {
                removed: Vec::new(),
                detail: String::new(),
            },
            lock_free: false,
        }
    }

    fn backup_run(last_step: u64) -> RunRecord {
        run(
            0,
            BACKUP,
            ClientOp::Backup {
                snapshot: "a1".into(),
            },
            last_step,
        )
    }

    fn kinds(v: &[Violation]) -> Vec<ViolationKind> {
        v.iter().map(|v| v.kind).collect()
    }

    const PACK: &str = "packs/aa/aa00";

    /// Invariant 7 judges a lock-mode run's head write without a lock and
    /// exempts the same write by a lock-free run (phase F part 10b).
    #[test]
    fn lock_exclusion_exempts_lock_free_runs() {
        use crate::sim::store::{SimConfig, SimStore};
        use vykar_storage::StorageBackend;
        for lock_free in [false, true] {
            let store = SimStore::new(SimConfig::default());
            let a = store.register_client("a");
            store.set_current_run(a, Some(0));
            store.handle(a).put("index.h/2", b"head").unwrap();
            let history = History {
                runs: vec![RunRecord {
                    lock_free,
                    ..run(0, a, ClientOp::Compact, store.step())
                }],
                ..History::default()
            };
            let mut report = Report::default();
            check_lock_exclusion(&store, &history, &mut report);
            let want = if lock_free {
                vec![]
            } else {
                vec![ViolationKind::WriteWithoutLock]
            };
            assert_eq!(kinds(&report.violations), want, "lock_free {lock_free}");
        }
    }

    #[test]
    fn checker_flags_a_young_orphan_delete() {
        let mut log = Log::default();
        log.op(BACKUP, 0, OpKind::PutPack, PACK, mins(0));
        log.op(OTHER, 1, OpKind::Delete, PACK, mins(60));
        let runs = [backup_run(1), run(1, OTHER, ClientOp::Compact, 2)];
        assert_eq!(
            kinds(&orphan_grace(&log.0, &runs)),
            [ViolationKind::YoungOrphanDeleted]
        );
    }

    #[test]
    fn checker_flags_a_live_sessions_pack_delete() {
        let mut log = Log::default();
        log.op(BACKUP, 0, OpKind::Put, "sessions/s.json", mins(0));
        log.op(BACKUP, 0, OpKind::PutPack, PACK, mins(0));
        log.op(OTHER, 1, OpKind::Delete, PACK, mins(13 * 60));
        // The backup's run is still in flight at the delete.
        let runs = [backup_run(100), run(1, OTHER, ClientOp::Compact, 3)];
        assert_eq!(
            kinds(&orphan_grace(&log.0, &runs)),
            [ViolationKind::LiveSessionPackDeleted]
        );
        // Not once its marker is gone.
        let mut log = Log::default();
        log.op(BACKUP, 0, OpKind::Put, "sessions/s.json", mins(0));
        log.op(BACKUP, 0, OpKind::PutPack, PACK, mins(0));
        log.op(OTHER, 1, OpKind::Delete, "sessions/s.json", mins(13 * 60));
        log.op(OTHER, 1, OpKind::Delete, PACK, mins(13 * 60));
        assert!(orphan_grace(&log.0, &runs).is_empty());
    }

    #[test]
    fn checker_flags_a_young_swept_pack() {
        let mut log = Log::default();
        log.op(BACKUP, 0, OpKind::PutPack, PACK, mins(0));
        log.op(BACKUP, 0, OpKind::Put, "index.h/2", mins(1));
        log.op(OTHER, 1, OpKind::Delete, PACK, mins(60));
        let runs = [backup_run(2), run(1, OTHER, ClientOp::Compact, 3)];
        assert_eq!(
            kinds(&orphan_grace(&log.0, &runs)),
            [ViolationKind::YoungOrphanDeleted]
        );
    }

    #[test]
    fn checker_accepts_an_old_swept_pack() {
        let mut log = Log::default();
        log.op(BACKUP, 0, OpKind::PutPack, PACK, mins(0));
        log.op(BACKUP, 0, OpKind::Put, "index.h/2", mins(1));
        log.op(OTHER, 1, OpKind::Delete, PACK, mins(13 * 60));
        let runs = [backup_run(2), run(1, OTHER, ClientOp::Compact, 3)];
        assert!(orphan_grace(&log.0, &runs).is_empty());
    }

    #[test]
    fn checker_excuses_a_repack_source() {
        let mut log = Log::default();
        log.op(BACKUP, 0, OpKind::PutPack, PACK, mins(0));
        log.op(BACKUP, 0, OpKind::Put, "index.h/2", mins(1));
        log.op(OTHER, 1, OpKind::GetRange, PACK, mins(60));
        log.op(OTHER, 1, OpKind::Delete, PACK, mins(60));
        let runs = [backup_run(2), run(1, OTHER, ClientOp::Compact, 4)];
        assert!(orphan_grace(&log.0, &runs).is_empty());
        // A read by another run does not make it one.
        let mut log = Log::default();
        log.op(BACKUP, 0, OpKind::PutPack, PACK, mins(0));
        log.op(BACKUP, 0, OpKind::Get, PACK, mins(1));
        log.op(OTHER, 1, OpKind::Delete, PACK, mins(60));
        assert_eq!(
            kinds(&orphan_grace(&log.0, &runs)),
            [ViolationKind::YoungOrphanDeleted]
        );
    }

    #[test]
    fn checker_flags_a_young_recovered_then_swept_pack() {
        let mut log = Log::default();
        log.op(BACKUP, 0, OpKind::PutPack, PACK, mins(0));
        log.op(BACKUP, 0, OpKind::Put, "sessions/x.index", mins(0));
        // Run 2 recovers the crashed run's journal and commits.
        log.op(OTHER, 2, OpKind::Get, "sessions/x.index", mins(5));
        log.op(OTHER, 2, OpKind::Put, "index.h/2", mins(6));
        log.op(OTHER, 3, OpKind::Delete, PACK, mins(60));
        let runs = [
            backup_run(2),
            run(
                2,
                OTHER,
                ClientOp::Backup {
                    snapshot: "b1".into(),
                },
                4,
            ),
            run(3, OTHER, ClientOp::Compact, 5),
        ];
        assert_eq!(
            kinds(&orphan_grace(&log.0, &runs)),
            [ViolationKind::YoungOrphanDeleted]
        );
    }

    #[test]
    fn checker_flags_an_old_pack_commit_after_a_marker_gap() {
        let mut log = Log::default();
        log.op(BACKUP, 0, OpKind::Put, "sessions/s.json", mins(0));
        log.op(BACKUP, 0, OpKind::PutPack, PACK, mins(0));
        log.op(
            BACKUP,
            0,
            OpKind::Exists,
            "locks/l.json",
            mins(11 * 60 + 30),
        );
        log.op(BACKUP, 0, OpKind::Put, "index.h/2", mins(11 * 60 + 30));
        let runs = [backup_run(4)];
        assert_eq!(
            kinds(&marker_gap_commit(&log.0, &runs)),
            [ViolationKind::OldPackCommittedAfterMarkerGap]
        );
        // Young packs: the same gap commits.
        let mut log = Log::default();
        log.op(BACKUP, 0, OpKind::Put, "sessions/s.json", mins(0));
        log.op(BACKUP, 0, OpKind::PutPack, PACK, mins(0));
        log.op(BACKUP, 0, OpKind::Exists, "locks/l.json", mins(60));
        log.op(BACKUP, 0, OpKind::Put, "index.h/2", mins(60));
        assert!(marker_gap_commit(&log.0, &runs).is_empty());
        // A reap by another client counts like a gap.
        let mut log = Log::default();
        log.op(BACKUP, 0, OpKind::Put, "sessions/s.json", mins(0));
        log.op(BACKUP, 0, OpKind::PutPack, PACK, mins(0));
        log.op(
            BACKUP,
            0,
            OpKind::Put,
            "sessions/s.1.json",
            mins(11 * 60 + 25),
        );
        log.op(
            OTHER,
            1,
            OpKind::Delete,
            "sessions/s.json",
            mins(11 * 60 + 26),
        );
        log.op(BACKUP, 0, OpKind::Put, "index.h/2", mins(11 * 60 + 30));
        assert_eq!(
            kinds(&marker_gap_commit(&log.0, &runs)),
            [ViolationKind::OldPackCommittedAfterMarkerGap]
        );
    }

    #[test]
    fn checker_accepts_an_old_pack_commit_with_fresh_marker_writes() {
        let mut log = Log::default();
        log.op(BACKUP, 0, OpKind::Put, "sessions/s.json", mins(0));
        log.op(BACKUP, 0, OpKind::PutPack, PACK, mins(0));
        log.op(
            BACKUP,
            0,
            OpKind::Put,
            "sessions/s.1.json",
            mins(11 * 60 + 25),
        );
        log.op(
            BACKUP,
            0,
            OpKind::Exists,
            "locks/l.json",
            mins(11 * 60 + 30),
        );
        log.op(BACKUP, 0, OpKind::Put, "index.h/2", mins(11 * 60 + 30));
        assert!(marker_gap_commit(&log.0, &[backup_run(5)]).is_empty());
    }
}
