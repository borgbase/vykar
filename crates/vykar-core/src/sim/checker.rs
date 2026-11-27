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
//! | I3 (heads immutable, one per generation) | 12 on a multi-writer repository; a single-writer one replaces heads by design (10-single-protocol: I3 is multi-writer only) |
//! | I4 (every listed snapshot covered by some retained head) | 1, against the latest head |
//! | I5 (no removal without a completed mark) | not yet (the H5 regression is scripted) |
//! | I6 (no snapshot references a chunk missing from its commit head) | 1 approximates it against the latest head |
//! | I7 (a reader returns correct data or a typed error) | 1 (restore against the source), 8 |
//! | I8 (a marker gap with an old pack does not commit) | 11 |
//! | exclusive sessions keep every other writer out (10-single-protocol step 8) | 13 |

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::fmt;

use vykar_types::chunk_id::ChunkId;

use super::fault::{ClientId, OpKind};
use super::history::{
    AllowedError, ClientOp, ErrorClass, History, ObservationCause, RunOutcome, RunRecord,
};
use super::log::{OpOutcome, OpRecord};
use super::runner::SourceTree;
use super::store::SimStore;
use crate::commands::list;
use crate::commands::restore::{self, RestoreOptions, RestoreRequest, RestoreSelection};
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
    // since 05 step 8; 7 (lock exclusion) is retired with the lock,
    // 10-single-protocol step 10. The numbers of the others stay.
    AllowedErrors = 8,
    /// Once every marker has aged out, a new client can back up
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
    /// HEAD_LANDING_ALLOWANCE when it issued that PUT, publishes no head.
    MarkerGapCommit = 11,
    /// I3 (04 step 8): no stored head is replaced by other bytes, and each
    /// retained head names every live object of its predecessor (it was
    /// written over it). Asked for (`STEP8`) on stores that honour
    /// put-if-absent; with it, invariant 4 judges every retained head.
    HeadsImmutable = 12,
    /// Exclusive sessions keep every other writer out (10-single-protocol
    /// steps 8 and 10; in place of invariant 7). See `exclusive_sessions`.
    ExclusiveSessions = 13,
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

    /// STEP1 plus invariants 13 and 9.
    pub const STEP3: &'static [Invariant] = &[
        Invariant::CommittedSnapshotsRestore,
        Invariant::AcknowledgedCommitsDurable,
        Invariant::NoSilentRemoval,
        Invariant::IndexIntegrity,
        Invariant::AllowedErrors,
        Invariant::ExclusiveSessions,
        Invariant::NoPermanentWedge,
    ];

    /// STEP3 plus invariants 10 and 11 (04 step 7).
    pub const STEP7: &'static [Invariant] = &[
        Invariant::CommittedSnapshotsRestore,
        Invariant::AcknowledgedCommitsDurable,
        Invariant::NoSilentRemoval,
        Invariant::IndexIntegrity,
        Invariant::AllowedErrors,
        Invariant::ExclusiveSessions,
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
        Invariant::ExclusiveSessions,
        Invariant::NoPermanentWedge,
        Invariant::OrphanGrace,
        Invariant::MarkerGapCommit,
        Invariant::HeadsImmutable,
    ];

    /// What the driver judges a case by, and what a known hazard's end state
    /// is classified with: STEP7 without 9 (it needs `Sim::check_invariants`
    /// and ages a fork), plus 12 on a multi-writer repository.
    pub fn judged(multi_writer: bool) -> Vec<Invariant> {
        let mut out = vec![
            Invariant::CommittedSnapshotsRestore,
            Invariant::AcknowledgedCommitsDurable,
            Invariant::NoSilentRemoval,
            Invariant::IndexIntegrity,
            Invariant::AllowedErrors,
            Invariant::ExclusiveSessions,
            Invariant::OrphanGrace,
            Invariant::MarkerGapCommit,
        ];
        if multi_writer {
            out.push(Invariant::HeadsImmutable);
        }
        out
    }
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
    // 13
    /// A protected op of an exclusive run while no entered exclusive hold was
    /// present (its own marker reaped or gone).
    ExclusiveWriteWithoutHold,
    /// A protected op of an exclusive run while the one entered hold present
    /// was another run's.
    ExclusiveWriteUnderOtherHold,
    /// A protected op of an exclusive run while entered holds of two or more
    /// runs were present.
    ExclusiveWriteUnderSeveralHolds,
    /// A protected op of a non-exclusive run while another run's entered
    /// exclusive hold was present.
    SharedWriteUnderExclusive,
    /// A protected op of a run that is neither exclusive nor shared
    /// (`RunRecord::shared` false), or of no run, with no hold present.
    WriteWithoutSession,
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
            K::WedgedBackup | K::WedgedCompact => Invariant::NoPermanentWedge,
            K::YoungOrphanDeleted | K::LiveSessionPackDeleted => Invariant::OrphanGrace,
            K::OldPackCommittedAfterMarkerGap => Invariant::MarkerGapCommit,
            K::HeadOverwritten | K::HeadSkipsPredecessor => Invariant::HeadsImmutable,
            K::ExclusiveWriteWithoutHold
            | K::ExclusiveWriteUnderOtherHold
            | K::ExclusiveWriteUnderSeveralHolds
            | K::SharedWriteUnderExclusive
            | K::WriteWithoutSession => Invariant::ExclusiveSessions,
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

/// Present in the index, pack present, and readable (`read_chunk`
/// authenticates the blob).
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
            &RestoreRequest {
                snapshot: name,
                dest: &dest.to_string_lossy(),
                selection: RestoreSelection::All,
                options: RestoreOptions {
                    xattrs_enabled: false,
                    verify_chunks: true,
                    continue_on_error: false,
                },
            },
            None,
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

/// Prefixes of keys whose deletion only a session holder may do.
const PROTECTED_DELETE_PREFIXES: &[&str] =
    &["snapshots/", "packs/", "index.d/", "index.h/", "packmaps/"];

/// An exclusive session's marker key (`sessions/x-*.json`, 10-single-protocol
/// step 8): a hold for invariant 13.
fn is_exclusive_marker_key(key: &str) -> bool {
    crate::repo::sessions::parse_session_marker_key(key)
        .is_some_and(|(id, _)| crate::repo::sessions::is_exclusive_session(id))
}

/// A write or delete no run may make under another run's exclusive session:
/// an applied write of a head, an index object or a snapshot, or an applied
/// delete naming a key under one of `PROTECTED_DELETE_PREFIXES`.
fn protected_shared(rec: &OpRecord) -> bool {
    if rec.op.is_write() {
        ["index.h/", "index.d/", "snapshots/"]
            .iter()
            .any(|p| rec.key.starts_with(p))
    } else if rec.op.is_delete() {
        rec.all_keys()
            .iter()
            .any(|k| PROTECTED_DELETE_PREFIXES.iter().any(|p| k.starts_with(p)))
    } else {
        false
    }
}

/// Invariant 13 over the applied op log: exclusive sessions keep every other
/// writer out.
///
/// An exclusive marker key is `sessions/x-*.json`. Run R is exclusive when
/// the log has an applied write of an exclusive marker key by R;
/// `first_put(R)` is that write's step and `listed(R)` the step of R's first
/// `ListMeta` of exactly `sessions/` with outcome `Ok` after it (its
/// registration listing). R entered unless its `RunRecord` ends
/// `Allowed(ActiveSessions)` (its registration refused); a run not in `runs`
/// (in flight) and a key written outside any run (tamper) count as entered.
/// The holds present before op p are the distinct (client, run) owners of the
/// exclusive marker keys present just before p (written and not deleted
/// since, batch deletes included), keeping only entered owners and, for an
/// owner with a run, only once `listed(owner)` is before p.
///
/// Then for every applied op p whose client is not the tamper client and
/// whose run is not Init:
/// - If p's run R is exclusive, p is protected when [`protected_shared`], or
///   when p is a mutation landing after `first_put(R)` with a key outside
///   `sessions/` and the legacy lock directory. A protected p is judged
///   against the holds: none, `ExclusiveWriteWithoutHold`; exactly one that
///   is not R's, `ExclusiveWriteUnderOtherHold`; exactly R's own, nothing;
///   two or more, `ExclusiveWriteUnderSeveralHolds`.
/// - Otherwise (a shared run, a run without a session, an op outside any run)
///   only [`protected_shared`] ops are judged: any hold present,
///   `SharedWriteUnderExclusive`; none, and R's `RunRecord` not `shared`
///   (or p has no run), `WriteWithoutSession`; else nothing.
///
/// A late effect is judged at the step it lands.
///
/// What it exempts: the tamper client's ops; Init; holds whose keys were
/// deleted (reaped or deregistered), judged by presence; tentative holds (a
/// refused registration's key, and any exclusive key before its owner's
/// registration listing); `sessions/` and the legacy lock directory for
/// exclusive runs; pack uploads and pack-map writes of shared runs.
///
/// What it does not see: shared pack, pack-map and journal writes under an
/// exclusive hold. They are legitimate where a backup misjudged no live
/// exclusive session and stops at its pre-head listing
/// (`tests::sim_exclusion` case (d)), and forbidden by the exclusive search's
/// hand rule (`tests::sim_exhaustive::exclusive_against_backup_search`),
/// which stays the judge of those.
pub(crate) fn exclusive_sessions(
    log: &[OpRecord],
    runs: &[RunRecord],
    tamper: ClientId,
) -> Vec<Violation> {
    use crate::repo::sessions::LEGACY_LOCKS_PREFIX;
    let record = |i: usize| runs.iter().find(|r| r.index == i);
    let mut first_put: BTreeMap<usize, u64> = BTreeMap::new();
    for r in log {
        if applied_write(r)
            && is_exclusive_marker_key(&r.key)
            && let Some(run) = r.run
        {
            first_put.entry(run).or_insert(r.step);
        }
    }
    let mut listed: BTreeMap<usize, u64> = BTreeMap::new();
    for r in log {
        if r.op == OpKind::ListMeta
            && r.key == "sessions/"
            && r.outcome == OpOutcome::Ok
            && let Some(run) = r.run
            && first_put.get(&run).is_some_and(|&fp| fp < r.step)
        {
            listed.entry(run).or_insert(r.step);
        }
    }
    let entered = |run: Option<usize>| {
        run.and_then(record).is_none_or(|r| {
            r.error_class() != Some(&ErrorClass::Allowed(AllowedError::ActiveSessions))
        })
    };
    let init: BTreeSet<usize> = runs
        .iter()
        .filter(|r| matches!(r.op, ClientOp::Init))
        .map(|r| r.index)
        .collect();
    let mut present: BTreeMap<String, (ClientId, Option<usize>)> = BTreeMap::new();
    let mut out = Vec::new();
    for rec in log {
        if !rec.applied() {
            continue;
        }
        if rec.client != tamper && !rec.run.is_some_and(|r| init.contains(&r)) {
            let holds: BTreeSet<(ClientId, Option<usize>)> = present
                .values()
                .copied()
                .filter(|&(_, run)| {
                    entered(run)
                        && run.is_none_or(|i| listed.get(&i).is_some_and(|&l| l < rec.step))
                })
                .collect();
            let exclusive = rec.run.and_then(|r| first_put.get(&r).map(|&fp| (r, fp)));
            let kind = if let Some((r, fp)) = exclusive {
                let own_data = rec.op.is_mutation()
                    && rec.step > fp
                    && rec.all_keys().iter().any(|k| {
                        !k.starts_with("sessions/") && !k.starts_with(LEGACY_LOCKS_PREFIX)
                    });
                if !(protected_shared(rec) || own_data) {
                    None
                } else {
                    match holds.len() {
                        0 => Some(ViolationKind::ExclusiveWriteWithoutHold),
                        1 if holds.contains(&(rec.client, Some(r))) => None,
                        1 => Some(ViolationKind::ExclusiveWriteUnderOtherHold),
                        _ => Some(ViolationKind::ExclusiveWriteUnderSeveralHolds),
                    }
                }
            } else if !protected_shared(rec) {
                None
            } else if !holds.is_empty() {
                Some(ViolationKind::SharedWriteUnderExclusive)
            } else if rec.run.and_then(record).is_none_or(|r| !r.shared) {
                Some(ViolationKind::WriteWithoutSession)
            } else {
                None
            };
            if let Some(kind) = kind {
                let subject = rec
                    .run
                    .map_or_else(|| "no run".to_string(), |r| format!("run {r}"));
                let held: Vec<String> = present
                    .iter()
                    .filter(|(_, owner)| holds.contains(owner))
                    .map(|(k, (c, r))| format!("{k} by client {} run {r:?}", c.0))
                    .collect();
                out.push(violation(
                    kind,
                    subject,
                    rec.step,
                    format!(
                        "step {} {:?} {} by client {}; exclusive holds: {held:?}",
                        rec.step, rec.op, rec.key, rec.client.0
                    ),
                ));
            }
        }
        if rec.op.is_write() && is_exclusive_marker_key(&rec.key) {
            present.insert(rec.key.clone(), (rec.client, rec.run));
        }
        for k in rec.deleted_keys() {
            present.remove(k);
        }
    }
    out
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
/// and its packs were too old (the clock at `h`'s own issue — its
/// record's `issued`, which is before `at` when a gate held the request;
/// the parked record's for a released write — minus the earliest of its applied pack
/// PUTs, plus `HEAD_LANDING_ALLOWANCE`, exceeds `PACK_AGE_LIMIT`). The
/// product runs the pack-age term at its head step, just before this
/// request; a clock read any earlier (the last op the run issued before
/// `h`) misses a suspend between its fence and its head write when nothing
/// is issued between them, as on a multi-writer head write.
pub(crate) fn marker_gap_commit(log: &[OpRecord], runs: &[RunRecord]) -> Vec<Violation> {
    use crate::repo::ages::{HEAD_LANDING_ALLOWANCE, PACK_AGE_LIMIT};
    use crate::repo::sessions::MARKER_GAP_LIMIT;
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
            let t_issue = log[issue_pos].issued;
            let Some(oldest) = before
                .iter()
                .filter(mine)
                .filter(|r| applied_write(r) && r.key.starts_with("packs/"))
                .map(|r| r.at)
                .min()
            else {
                continue;
            };
            let age = between(oldest, t_issue);
            if age + HEAD_LANDING_ALLOWANCE > PACK_AGE_LIMIT {
                out.push(violation(
                    ViolationKind::OldPackCommittedAfterMarkerGap,
                    run_subject(run),
                    h.step,
                    format!(
                        "step {} wrote {} with a {} marker; its oldest pack was {} s old at its issue",
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
    // together (`STEP8`): a single-writer repository replaces heads by
    // design (a late or stalled head; I3 is multi-writer only).
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
    if want(Invariant::ExclusiveSessions) {
        report.violations.extend(exclusive_sessions(
            &store.log(),
            &history.runs,
            store.tamper_id(),
        ));
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
                issued: at,
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
            shared: false,
            exclusive: false,
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

    const TAMPER: ClientId = ClientId(1);
    const XA: &str = "sessions/x-a.json";
    const XB: &str = "sessions/x-b.json";

    impl Log {
        fn tamper(&mut self, op: OpKind, key: &str) {
            self.op(TAMPER, 0, op, key, mins(0));
            self.0.last_mut().unwrap().run = None;
        }

        /// An exclusive run's registration: its marker PUT and its listing.
        fn register(&mut self, client: ClientId, run: usize, key: &str) {
            self.op(client, run, OpKind::Put, key, mins(0));
            self.op(client, run, OpKind::ListMeta, "sessions/", mins(0));
        }
    }

    fn exclusive_kinds(log: &Log, runs: &[RunRecord]) -> Vec<ViolationKind> {
        kinds(&exclusive_sessions(&log.0, runs, TAMPER))
    }

    fn refused(index: usize, client: ClientId) -> RunRecord {
        RunRecord {
            outcome: RunOutcome::Err(crate::sim::history::ErrorRecord {
                display: "active sessions".into(),
                class: ErrorClass::Allowed(AllowedError::ActiveSessions),
            }),
            ..run(index, client, ClientOp::CheckRepair, 100)
        }
    }

    fn shared(index: usize, client: ClientId, op: ClientOp) -> RunRecord {
        RunRecord {
            shared: true,
            ..run(index, client, op, 100)
        }
    }

    #[test]
    fn exclusive_write_after_its_marker_is_reaped() {
        let mut log = Log::default();
        log.register(BACKUP, 0, XA);
        log.op(OTHER, 1, OpKind::Delete, XA, mins(50));
        log.op(BACKUP, 0, OpKind::Put, "index.h/2", mins(50));
        let runs = [
            run(0, BACKUP, ClientOp::CheckRepair, 100),
            shared(1, OTHER, ClientOp::Compact),
        ];
        assert_eq!(
            exclusive_kinds(&log, &runs),
            [ViolationKind::ExclusiveWriteWithoutHold]
        );
    }

    #[test]
    fn two_entered_exclusive_holds() {
        let runs = [
            run(0, BACKUP, ClientOp::CheckRepair, 100),
            run(1, OTHER, ClientOp::CheckRepair, 100),
        ];
        let mut log = Log::default();
        log.register(BACKUP, 0, XA);
        log.register(OTHER, 1, XB);
        log.op(OTHER, 1, OpKind::Put, "index.h/2", mins(1));
        assert_eq!(
            exclusive_kinds(&log, &runs),
            [ViolationKind::ExclusiveWriteUnderSeveralHolds]
        );
        let mut log = Log::default();
        log.register(BACKUP, 0, XA);
        log.register(OTHER, 1, XB);
        log.op(BACKUP, 0, OpKind::Delete, XB, mins(1));
        log.op(OTHER, 1, OpKind::Put, "index.h/2", mins(1));
        assert_eq!(
            exclusive_kinds(&log, &runs),
            [ViolationKind::ExclusiveWriteUnderOtherHold]
        );
    }

    /// Invariant 7 read this as `WriteUnderSeveralLocks` (its documented
    /// limit: a key counted as held from its PUT).
    #[test]
    fn a_refused_registration_is_no_hold() {
        let mut log = Log::default();
        log.register(BACKUP, 0, XA);
        log.register(OTHER, 1, XB);
        log.op(BACKUP, 0, OpKind::Put, "index.h/2", mins(1));
        let runs = [
            run(0, BACKUP, ClientOp::CheckRepair, 100),
            refused(1, OTHER),
        ];
        assert_eq!(exclusive_kinds(&log, &runs), []);
    }

    /// A run whose registration was refused holds nothing, so a write it
    /// made anyway is judged without a hold.
    #[test]
    fn a_refused_run_that_wrote_is_judged() {
        let mut log = Log::default();
        log.register(OTHER, 1, XB);
        log.op(OTHER, 1, OpKind::Put, "snapshots/s", mins(1));
        assert_eq!(
            exclusive_kinds(&log, &[refused(1, OTHER)]),
            [ViolationKind::ExclusiveWriteWithoutHold]
        );
    }

    #[test]
    fn a_hold_starts_at_the_registration_listing() {
        let runs = [
            run(0, BACKUP, ClientOp::CheckRepair, 100),
            shared(
                1,
                OTHER,
                ClientOp::Backup {
                    snapshot: "b".into(),
                },
            ),
        ];
        let mut log = Log::default();
        log.op(BACKUP, 0, OpKind::Put, XA, mins(0));
        log.op(OTHER, 1, OpKind::Put, "snapshots/s", mins(0));
        log.op(BACKUP, 0, OpKind::ListMeta, "sessions/", mins(0));
        assert_eq!(exclusive_kinds(&log, &runs), []);
        let mut log = Log::default();
        log.register(BACKUP, 0, XA);
        log.op(OTHER, 1, OpKind::Put, "snapshots/s", mins(0));
        assert_eq!(
            exclusive_kinds(&log, &runs),
            [ViolationKind::SharedWriteUnderExclusive]
        );
        let mut log = Log::default();
        log.register(BACKUP, 0, XA);
        log.op(BACKUP, 0, OpKind::Delete, XA, mins(0));
        log.op(OTHER, 1, OpKind::Put, "snapshots/s", mins(0));
        assert_eq!(exclusive_kinds(&log, &runs), []);
    }

    /// An exclusive run's own protected write between its marker PUT and its
    /// registration listing is not yet under its hold.
    #[test]
    fn an_exclusive_write_before_its_listing_has_no_hold() {
        let mut log = Log::default();
        log.op(BACKUP, 0, OpKind::Put, XA, mins(0));
        log.op(BACKUP, 0, OpKind::Put, "index.d/x", mins(0));
        log.op(BACKUP, 0, OpKind::ListMeta, "sessions/", mins(0));
        assert_eq!(
            exclusive_kinds(&log, &[run(0, BACKUP, ClientOp::CheckRepair, 100)]),
            [ViolationKind::ExclusiveWriteWithoutHold]
        );
    }

    #[test]
    fn an_exclusive_runs_data_write_is_protected() {
        let runs = [run(0, BACKUP, ClientOp::CheckRepair, 100)];
        let mut log = Log::default();
        log.register(BACKUP, 0, XA);
        log.op(BACKUP, 0, OpKind::Delete, XA, mins(0));
        log.op(BACKUP, 0, OpKind::PutPack, PACK, mins(0));
        assert_eq!(
            exclusive_kinds(&log, &runs),
            [ViolationKind::ExclusiveWriteWithoutHold]
        );
        // The open's key backfill, before its first exclusive marker PUT.
        let mut log = Log::default();
        log.op(BACKUP, 0, OpKind::Put, "keys/repokey", mins(0));
        log.register(BACKUP, 0, XA);
        assert_eq!(exclusive_kinds(&log, &runs), []);
    }

    #[test]
    fn a_sessionless_write() {
        let mut log = Log::default();
        log.op(BACKUP, 0, OpKind::Put, "index.d/x", mins(0));
        assert_eq!(
            exclusive_kinds(&log, &[run(0, BACKUP, ClientOp::Compact, 100)]),
            [ViolationKind::WriteWithoutSession]
        );
        assert_eq!(
            exclusive_kinds(&log, &[shared(0, BACKUP, ClientOp::Compact)]),
            []
        );
    }

    #[test]
    fn a_legacy_lock_key_is_no_hold() {
        let legacy = format!(
            "{}{:020}-{:032x}.json",
            crate::repo::sessions::LEGACY_LOCKS_PREFIX,
            0,
            1u128
        );
        let mut log = Log::default();
        log.tamper(OpKind::Put, &legacy);
        log.register(BACKUP, 0, XA);
        log.op(BACKUP, 0, OpKind::Put, "index.h/2", mins(1));
        assert_eq!(
            exclusive_kinds(&log, &[run(0, BACKUP, ClientOp::CheckRepair, 100)]),
            []
        );
    }

    /// An exclusive run's own writes under `sessions/` and the legacy lock
    /// directory are not judged, hold or none: its marker key is gone here,
    /// yet its journal PUT, its delete of another session's marker and its
    /// delete of a legacy lock key pass; a data write after them does not.
    #[test]
    fn an_exclusive_runs_session_and_legacy_writes_are_exempt() {
        let legacy = format!(
            "{}{:020}-{:032x}.json",
            crate::repo::sessions::LEGACY_LOCKS_PREFIX,
            0,
            1u128
        );
        let mut log = Log::default();
        log.register(BACKUP, 0, XA);
        log.op(OTHER, 1, OpKind::Delete, XA, mins(50));
        log.op(BACKUP, 0, OpKind::Put, "sessions/x-a.index", mins(50));
        log.op(BACKUP, 0, OpKind::Delete, "sessions/s.json", mins(50));
        log.op(BACKUP, 0, OpKind::Delete, &legacy, mins(50));
        let runs = [
            run(0, BACKUP, ClientOp::CheckRepair, 100),
            shared(1, OTHER, ClientOp::Compact),
        ];
        assert_eq!(exclusive_kinds(&log, &runs), []);
        log.op(BACKUP, 0, OpKind::Put, "index.d/x", mins(50));
        assert_eq!(
            exclusive_kinds(&log, &runs),
            [ViolationKind::ExclusiveWriteWithoutHold]
        );
    }

    /// A batch delete that names an exclusive marker key removes its hold,
    /// as a single delete does; one naming only other keys does not.
    #[test]
    fn a_batch_delete_removes_a_hold() {
        let runs = [
            run(0, BACKUP, ClientOp::CheckRepair, 100),
            shared(1, OTHER, ClientOp::Compact),
        ];
        let batch = |keys: &[&str]| {
            let mut log = Log::default();
            log.register(BACKUP, 0, XA);
            log.op(OTHER, 1, OpKind::BatchDelete, keys[0], mins(50));
            log.0.last_mut().unwrap().keys = keys.iter().map(|k| k.to_string()).collect();
            log.op(OTHER, 1, OpKind::Put, "snapshots/s", mins(50));
            log
        };
        assert_eq!(exclusive_kinds(&batch(&["sessions/s.json", XA]), &runs), []);
        assert_eq!(
            exclusive_kinds(&batch(&["sessions/s.json", "sessions/t.json"]), &runs),
            [ViolationKind::SharedWriteUnderExclusive]
        );
    }

    #[test]
    fn init_and_tamper_are_exempt() {
        let mut log = Log::default();
        log.op(BACKUP, 0, OpKind::Put, "index.h/1", mins(0));
        log.tamper(OpKind::Put, "index.h/2");
        log.tamper(OpKind::Delete, "snapshots/s");
        assert_eq!(
            exclusive_kinds(&log, &[run(0, BACKUP, ClientOp::Init, 1)]),
            []
        );
        // A tamper-planted exclusive key is a hold from its PUT.
        let mut log = Log::default();
        log.tamper(OpKind::Put, XB);
        log.op(BACKUP, 0, OpKind::Put, "index.h/2", mins(0));
        assert_eq!(
            exclusive_kinds(&log, &[shared(0, BACKUP, ClientOp::Compact)]),
            [ViolationKind::SharedWriteUnderExclusive]
        );
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
            "sessions/s.json",
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
        log.op(BACKUP, 0, OpKind::Exists, "sessions/s.json", mins(60));
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
            "sessions/s.json",
            mins(11 * 60 + 30),
        );
        log.op(BACKUP, 0, OpKind::Put, "index.h/2", mins(11 * 60 + 30));
        assert!(marker_gap_commit(&log.0, &[backup_run(5)]).is_empty());
    }

    /// Invariant 11 reads the clock at the head write's own issue: a
    /// multi-writer head write issues nothing between its segment PUT and
    /// its put-if-absent, so a suspend there is seen only at the create
    /// itself (step 10 F1). The last op before it would be the segment PUT,
    /// 6 h 1 min in, and the commit would pass.
    #[test]
    fn checker_flags_an_old_pack_commit_suspended_before_its_head_create() {
        let suspended = |head_at| {
            let mut log = Log::default();
            log.op(BACKUP, 0, OpKind::Put, "sessions/s.json", mins(0));
            log.op(BACKUP, 0, OpKind::PutPack, PACK, mins(0));
            log.op(BACKUP, 0, OpKind::Put, "index.d/aa", mins(6 * 60 + 1));
            log.op(BACKUP, 0, OpKind::PutIfAbsent, "index.h/2", head_at);
            log
        };
        let runs = [backup_run(4)];
        assert_eq!(
            kinds(&marker_gap_commit(&suspended(mins(12 * 60 + 2)).0, &runs)),
            [ViolationKind::OldPackCommittedAfterMarkerGap]
        );
        // No suspend before the create: the packs are young enough.
        assert!(marker_gap_commit(&suspended(mins(6 * 60 + 1)).0, &runs).is_empty());
    }

    /// A released head write is judged at its parked record's issue, not
    /// at its release: issued past the limit it is flagged; issued in time
    /// and released late it is not (a late head is invariant 11's only
    /// through its issue).
    #[test]
    fn checker_judges_a_released_head_write_at_its_issue() {
        use crate::sim::fault::{FaultAction, ParkedId};
        let released = |issued_at, released_at| {
            let mut log = Log::default();
            log.op(BACKUP, 0, OpKind::Put, "sessions/s.json", mins(0));
            log.op(BACKUP, 0, OpKind::PutPack, PACK, mins(0));
            log.op(BACKUP, 0, OpKind::Put, "index.d/aa", mins(60));
            log.op(BACKUP, 0, OpKind::PutIfAbsent, "index.h/2", issued_at);
            log.0.last_mut().unwrap().outcome = OpOutcome::Parked(ParkedId(7), FaultAction::Delay);
            log.op(BACKUP, 0, OpKind::PutIfAbsent, "index.h/2", released_at);
            log.0.last_mut().unwrap().outcome = OpOutcome::Released(ParkedId(7));
            log
        };
        let runs = [backup_run(5)];
        assert_eq!(
            kinds(&marker_gap_commit(
                &released(mins(11 * 60 + 30), mins(11 * 60 + 31)).0,
                &runs
            )),
            [ViolationKind::OldPackCommittedAfterMarkerGap]
        );
        assert!(marker_gap_commit(&released(mins(6 * 60), mins(12 * 60)).0, &runs).is_empty());
    }

    /// A head write a gate held is judged at the clock when the gate
    /// caught it (`issued`), not when it was logged (`at`): a request in
    /// flight for 45 minutes was issued in time, and the check after the
    /// head returned is what answers its late landing. Issued that late, it
    /// is flagged.
    #[test]
    fn checker_judges_a_gated_head_write_at_its_issue() {
        let gated = |issued_at| {
            let mut log = Log::default();
            log.op(BACKUP, 0, OpKind::Put, "sessions/s.json", mins(0));
            log.op(BACKUP, 0, OpKind::PutPack, PACK, mins(0));
            log.op(BACKUP, 0, OpKind::Put, "index.h/2", mins(11 * 60 + 55));
            log.0.last_mut().unwrap().issued = issued_at;
            log
        };
        let runs = [backup_run(3)];
        assert!(marker_gap_commit(&gated(mins(11 * 60 + 10)).0, &runs).is_empty());
        assert_eq!(
            kinds(&marker_gap_commit(&gated(mins(11 * 60 + 55)).0, &runs)),
            [ViolationKind::OldPackCommittedAfterMarkerGap]
        );
    }
}
