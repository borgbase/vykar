use std::collections::HashSet;

use vykar_types::chunk_id::ChunkId;
use vykar_types::pack_id::PackId;
use vykar_types::snapshot_id::SnapshotId;

/// A single integrity issue found during check.
#[derive(Debug)]
pub struct CheckError {
    pub context: String,
    pub message: String,
}

/// Summary of a check run.
#[derive(Debug)]
pub struct CheckResult {
    pub snapshots_checked: usize,
    pub items_checked: usize,
    pub chunks_existence_checked: usize,
    pub packs_existence_checked: usize,
    pub chunks_data_verified: usize,
    pub errors: Vec<CheckError>,
    /// Per-item impact records for snapshot items whose chunks live in a pack
    /// the existence check confirmed missing. Empty on healthy repos.
    pub item_impacts: Vec<ItemImpact>,
    /// True when the check was skipped entirely (e.g. max_percent=0 and full_every not due).
    pub skipped: bool,
}

// ---------------------------------------------------------------------------
// Structured integrity issues (for repair)
// ---------------------------------------------------------------------------

/// Structured integrity issue detected during check.
#[derive(Debug, Clone)]
pub enum IntegrityIssue {
    /// Snapshot blob fails to decrypt or deserialize.
    CorruptSnapshot {
        snapshot_id: SnapshotId,
        snapshot_name: Option<String>,
    },
    /// Raw `snapshots/<id>` with unparseable ID (never enters manifest).
    InvalidSnapshotKey { storage_key: String },
    /// Snapshot item_ptrs reference chunk not in index.
    DanglingItemPtr {
        snapshot_name: String,
        chunk_id: ChunkId,
    },
    /// File in snapshot references chunk not in index.
    DanglingFileChunk {
        snapshot_name: String,
        /// 0-based ordinal of this item within the decoded items_stream. Used
        /// by item-granular repair to locate the exact item to drop without
        /// relying on `path`, which may be duplicated across items.
        item_index: usize,
        path: String,
        chunk_id: ChunkId,
    },
    /// Pack referenced by index does not exist in storage.
    MissingPack { pack_id: PackId },
    /// Pack exists but fails header/hash/blob verification (--verify-data).
    CorruptPackContent { pack_id: PackId, detail: String },
    /// Individual chunk fails decrypt/decompress/ID check (--verify-data).
    CorruptChunk {
        chunk_id: ChunkId,
        pack_id: PackId,
        detail: String,
    },
    /// Pack existence check returned an I/O error (not confirmed missing).
    PackExistenceCheckFailed { pack_id: PackId, detail: String },
    /// Snapshot items could not be loaded or decoded (proven corruption).
    UnreadableSnapshot {
        snapshot_name: String,
        detail: String,
    },
    /// Snapshot meta or items failed to load due to I/O (not proven corrupt).
    SnapshotReadFailed {
        snapshot_name: String,
        detail: String,
    },
    /// Snapshot item failed per-item invariant validation.
    InvalidItem {
        snapshot_id: SnapshotId,
        snapshot_name: Option<String>,
        /// 0-based ordinal of this item within the decoded items_stream. Used
        /// by item-granular repair to locate the exact item to drop without
        /// relying on `item_path`, which may be duplicated across items.
        item_index: usize,
        item_path: String,
        reason: String,
    },
}

impl IntegrityIssue {
    /// Convert to a display-oriented CheckError.
    pub fn to_check_error(&self) -> CheckError {
        match self {
            IntegrityIssue::CorruptSnapshot {
                snapshot_name,
                snapshot_id,
            } => {
                let ctx = match snapshot_name {
                    Some(name) => format!("snapshot '{name}'"),
                    None => format!("snapshot {snapshot_id}"),
                };
                CheckError {
                    context: ctx,
                    message: "failed to load metadata: corrupt or undecryptable".into(),
                }
            }
            IntegrityIssue::InvalidSnapshotKey { storage_key } => CheckError {
                context: "snapshots".into(),
                message: format!("invalid snapshot key: {storage_key}"),
            },
            IntegrityIssue::DanglingItemPtr {
                snapshot_name,
                chunk_id,
            } => CheckError {
                context: format!("snapshot '{snapshot_name}' item_ptrs"),
                message: format!("chunk {chunk_id} not in index"),
            },
            IntegrityIssue::DanglingFileChunk {
                snapshot_name,
                path,
                chunk_id,
                ..
            } => CheckError {
                context: format!("snapshot '{snapshot_name}' file '{path}'"),
                message: format!("chunk {chunk_id} not in index"),
            },
            IntegrityIssue::MissingPack { pack_id } => CheckError {
                context: "chunk index".into(),
                message: format!("pack {pack_id} missing from storage"),
            },
            IntegrityIssue::CorruptPackContent { pack_id, detail } => CheckError {
                context: "verify-data".into(),
                message: format!("pack {pack_id}: {detail}"),
            },
            IntegrityIssue::CorruptChunk {
                chunk_id, detail, ..
            } => CheckError {
                context: "verify-data".into(),
                message: format!("chunk {chunk_id}: {detail}"),
            },
            IntegrityIssue::PackExistenceCheckFailed { pack_id, detail } => CheckError {
                context: "chunk index".into(),
                message: format!("pack {pack_id} existence check failed: {detail}"),
            },
            IntegrityIssue::UnreadableSnapshot {
                snapshot_name,
                detail,
            } => CheckError {
                context: format!("snapshot '{snapshot_name}'"),
                message: format!("failed to load items: {detail}"),
            },
            IntegrityIssue::SnapshotReadFailed {
                snapshot_name,
                detail,
            } => CheckError {
                context: format!("snapshot '{snapshot_name}'"),
                message: format!("I/O error: {detail}"),
            },
            IntegrityIssue::InvalidItem {
                snapshot_id,
                snapshot_name,
                item_path,
                reason,
                ..
            } => {
                let ctx = match snapshot_name {
                    Some(name) => format!("snapshot '{name}' item '{item_path}'"),
                    None => format!("snapshot {snapshot_id} item '{item_path}'"),
                };
                CheckError {
                    context: ctx,
                    message: reason.clone(),
                }
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Per-item impact records (drives item-level repair in #123)
// ---------------------------------------------------------------------------

/// A snapshot item whose chunks live in a pack that is missing from storage.
///
/// Carries enough identity (`snapshot_id` + `item_index`) for #123's surgical
/// repair to locate the exact decoded record when rewriting a snapshot's
/// `items_stream`. `item_path` alone is not unique — duplicate paths can occur.
#[derive(Debug, Clone)]
pub struct ItemImpact {
    pub snapshot_id: SnapshotId,
    pub snapshot_name: String,
    /// 0-based ordinal of this item within the decoded items_stream. Stable
    /// across re-walks of the same stream.
    pub item_index: usize,
    pub item_path: String,
    /// `(chunk_id, pack_id)` pairs for this item's chunks that live in a pack
    /// the existence check confirmed missing. Always non-empty.
    pub affected_chunks: Vec<(ChunkId, PackId)>,
}

impl ItemImpact {
    /// Render this impact as a user-facing CheckError.
    pub fn to_check_error(&self) -> CheckError {
        let mut packs: Vec<PackId> = self.affected_chunks.iter().map(|(_, p)| *p).collect();
        packs.sort_by_key(|a| *a.as_bytes());
        packs.dedup();

        let message = if let [only] = packs.as_slice() {
            format!("references missing pack {only}")
        } else {
            let list = packs
                .iter()
                .map(|p| p.to_string())
                .collect::<Vec<_>>()
                .join(", ");
            format!("references missing packs {list}")
        };

        CheckError {
            context: format!(
                "snapshot '{}' item '{}'",
                self.snapshot_name, self.item_path
            ),
            message,
        }
    }
}

// ---------------------------------------------------------------------------
// Repair types
// ---------------------------------------------------------------------------

/// An action the repair engine will execute.
#[derive(Debug, Clone)]
pub enum RepairAction {
    RemoveCorruptSnapshot {
        snapshot_id: SnapshotId,
        name: Option<String>,
    },
    RemoveInvalidSnapshotKey {
        storage_key: String,
    },
    RemoveDanglingIndexEntries {
        pack_id: PackId,
        chunk_count: usize,
    },
    /// Pack header invalid — remove ALL index entries for this pack.
    RemoveCorruptPack {
        pack_id: PackId,
        chunk_count: usize,
    },
    /// Individual chunks failed client-side verify — remove only these entries.
    RemoveCorruptChunks {
        pack_id: PackId,
        chunk_ids: Vec<ChunkId>,
    },
    RemoveDanglingSnapshot {
        snapshot_name: String,
        missing_chunks: usize,
    },
    /// Rewrite a snapshot under a new SnapshotId with the listed item ordinals
    /// dropped. Existing storage blob for `snapshot_id` is removed; the
    /// rewritten blob is committed at the new id, the manifest entry's name is
    /// preserved. `dropped_paths` and `reasons` are parallel to `item_indices`
    /// and exist solely to power user-facing dry-run output.
    DropItemsFromSnapshot {
        snapshot_id: SnapshotId,
        snapshot_name: String,
        item_indices: Vec<usize>,
        dropped_paths: Vec<String>,
        reasons: Vec<String>,
    },
    RebuildRefcounts,
}

/// The computed plan for a repair operation.
#[derive(Debug)]
pub struct RepairPlan {
    pub actions: Vec<RepairAction>,
    pub has_data_loss: bool,
}

/// Whether to just show the plan or actually apply it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RepairMode {
    PlanOnly,
    Apply,
}

/// Result of a repair operation.
#[derive(Debug)]
pub struct RepairResult {
    pub check_result: CheckResult,
    pub plan: RepairPlan,
    pub applied: Vec<RepairAction>,
    pub repair_errors: Vec<String>,
}

#[derive(Debug, Clone)]
pub enum CheckProgressEvent {
    SnapshotStarted {
        current: usize,
        total: usize,
        name: String,
    },
    PacksExistencePhaseStarted {
        total_packs: usize,
    },
    PacksExistenceProgress {
        checked: usize,
        total_packs: usize,
        missing: usize,
    },
    ChunksDataPhaseStarted {
        total_chunks: usize,
    },
    ChunksDataProgress {
        verified: usize,
        total_chunks: usize,
    },
    ServerVerifyPhaseStarted {
        total_packs: usize,
    },
    ServerVerifyProgress {
        verified: usize,
        total_packs: usize,
    },
}

pub(super) fn emit_progress(
    progress: &mut Option<&mut dyn FnMut(CheckProgressEvent)>,
    event: CheckProgressEvent,
) {
    if let Some(callback) = progress.as_deref_mut() {
        callback(event);
    }
}

/// Outcome of attempting server-side pack verification.
pub(crate) enum ServerVerifyOutcome {
    /// Server handled some or all packs. `verified_packs` is the set that was
    /// actually checked; any packs not in this set still need client-side work.
    Ok {
        errors: Vec<CheckError>,
        packs_responded: usize,
        #[allow(dead_code)] // used in tests only
        packs_passed: usize,
        chunks_verified: usize,
        verified_packs: HashSet<PackId>,
    },
    /// Server doesn't support verify-packs at all — fall back entirely.
    Fallback,
}

/// Result of processing a single batch of server verify responses.
pub(crate) struct ProcessedVerifyResult {
    pub packs_responded: usize,
    pub packs_passed: usize,
    pub chunks_verified: usize,
}
