use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use super::types::{CheckProgressEvent, IntegrityIssue, ItemImpact, emit_progress};
use crate::commands::list::{
    for_each_decoded_item, load_item_stream_of_meta, load_snapshot_meta_of,
};
use crate::commands::mark::{
    ChunkOutcome, OnChunkError, TreeChunkSet, default_decode_threads, walk_tree_chunks,
};
use crate::config::VykarConfig;
use crate::index::{ChunkIndexEntry, LiveIndex};
use crate::repo::control::{CancelToken, EventSink};
use crate::repo::format::{
    BlobLocation, ChunkDecodeStage, ObjectType, decode_chunk_into,
    unpack_object_expect_with_context,
};
use crate::repo::manifest::SnapshotEntry;
use crate::repo::pack::read_blob_from_pack;
use crate::repo::pack_trailer::{
    PackTrailer, TrailerRead, read_pack_trailer, read_trailer_from_pack_bytes,
};
use crate::repo::{Repo, TreeChunkSource};
use crate::snapshot::item::ItemType;
use crate::snapshot::listing::{ChunkPaths, ExtCheck, check_ext};
use vykar_crypto::CryptoEngine;
use vykar_protocol::validate_pack_header;
use vykar_storage::StorageBackend;
use vykar_types::chunk_id::ChunkId;
use vykar_types::error::{Result, VykarError};
use vykar_types::pack_id::PackId;
use vykar_types::snapshot_id::SnapshotId;

/// Number of chunks in a pack before we download the full pack instead of
/// issuing individual range reads.
const BATCH_THRESHOLD: usize = 3;

/// Returns `true` if the error is a transient I/O failure (not proven
/// corruption). Crypto, deserialization, format, and decompression errors are
/// considered evidence of corruption.
pub(super) fn is_transient_io(err: &VykarError) -> bool {
    err.io_error().is_some()
}

// ---------------------------------------------------------------------------
// Integrity scan (shared by read-only check and repair)
// ---------------------------------------------------------------------------

/// Options controlling the integrity scan phases.
pub(super) struct ScanOptions<'a> {
    /// Collect per-snapshot chunk refs (needed for repair plan).
    pub(super) collect_chunk_refs: bool,
    /// Detect orphan snapshot blobs on storage not in the manifest.
    pub(super) detect_orphans: bool,
    /// Run client-side crypto verification of chunk data.
    pub(super) verify_data: bool,
    /// Packs already verified server-side — skip existence and data checks for these.
    pub(super) skip_packs: Option<&'a HashSet<PackId>>,
    /// If set, Phase 1 checks only the listed snapshots with these IDs
    /// (a sampled check's snapshot sample).
    pub(super) snapshot_sample: Option<&'a HashSet<SnapshotId>>,
}

/// Counters collected during an integrity scan.
#[derive(Debug, Default)]
pub(super) struct ScanCounters {
    pub(super) snapshots_checked: usize,
    pub(super) key_files_checked: usize,
    pub(super) items_checked: usize,
    pub(super) chunks_existence_checked: usize,
    pub(super) packs_existence_checked: usize,
    pub(super) chunks_data_verified: usize,
}

/// Output of [`repair_scan`]: counters, issues, and per-snapshot chunk refs.
pub(super) struct ScanResult {
    pub(super) counters: ScanCounters,
    pub(super) issues: Vec<IntegrityIssue>,
    /// Maps each snapshot name to the set of chunk IDs it references
    /// (item_ptrs and file chunks combined; used by the chunks-to-remove
    /// dangling-snapshot path).
    pub(super) snapshot_chunk_refs: HashMap<String, HashSet<ChunkId>>,
    /// Per-snapshot `item_ptrs` chunks, partitioned out from
    /// `snapshot_chunk_refs`. Populated whenever the scan reads a snapshot's
    /// metadata, even if the items_stream subsequently fails to load — so the
    /// item-ptrs coverage gate has data to fail-closed against.
    pub(super) snapshot_item_ptrs: HashMap<SnapshotId, HashSet<ChunkId>>,
    /// Per-snapshot, per-item file chunk sets. Indexed by 0-based item ordinal
    /// within the decoded items_stream. Empty inner sets for non-`RegularFile`
    /// items. Populated only when `collect_chunk_refs == true` AND the
    /// items_stream walk completes successfully — snapshots whose walk fails
    /// or skips will be absent from this map (the planner reads that absence
    /// as "refuse item-level repair").
    pub(super) snapshot_per_item_chunks: HashMap<SnapshotId, Vec<HashSet<ChunkId>>>,
    /// Per-snapshot total item count. Populated in lockstep with
    /// `snapshot_per_item_chunks`: present iff the items_stream walk completed.
    pub(super) snapshot_item_counts: HashMap<SnapshotId, usize>,
    /// Items whose chunks reference a pack confirmed missing in Phase 2.
    /// Empty when no missing packs were detected.
    pub(super) item_impacts: Vec<ItemImpact>,
    /// The key copy that still holds the bytes which unwrapped at open, and
    /// the copies that need rewriting from it. Both empty on a healthy
    /// repository, and `key_source` is `None` when no stored copy matches —
    /// in which case nothing is repaired, because there is no copy on storage
    /// to repair *from*.
    pub(super) key_source: Option<String>,
    pub(super) key_copies_to_restore: Vec<String>,
    /// `(snapshot name, reason)` of every snapshot whose listing key index
    /// does not match its listing (06 step 4). A warning, not an issue:
    /// readers then scan, and nothing is repaired. Each is logged where it
    /// is found; only the unit tests read this list.
    #[cfg_attr(not(test), allow(dead_code))]
    pub(super) listing_index_warnings: Vec<(String, String)>,
}

/// Outcome of the repository-key phase.
#[derive(Default)]
struct KeyScan {
    checked: usize,
    issues: Vec<IntegrityIssue>,
    /// The stored copy still holding the bytes that unwrapped at open.
    source: Option<String>,
    /// Copies that should be rewritten from `source`. Always empty when
    /// `source` is `None` — there would be nothing on storage to copy from.
    to_restore: Vec<String>,
}

/// What one key copy looks like on this pass.
enum KeyCopyFinding {
    /// Byte-identical to the copy that unwrapped at open.
    Intact,
    Absent,
    /// I/O failure — not proven corruption, so never queued for a rewrite.
    Unreadable(String),
    /// Present but undecodable.
    Corrupt(String),
    /// Present and well-formed, but not the bytes this repository opened with.
    Foreign,
}

/// Phase K: re-read both repository key copies and compare them against each
/// other and against the bytes that unwrapped at open.
///
/// **Byte comparison rather than a second unwrap, deliberately.** Unwrapping
/// again costs another Argon2id derivation (t=3, m=64 MiB, p=4) on every
/// `check`, and would need the passphrase threaded all the way into the scan.
/// The blob already unwrapped at open, so comparing bytes proves the stored
/// objects are still intact at neither cost.
fn scan_key_copies(repo: &Repo) -> KeyScan {
    let winner = repo.key_blob();

    let mut checked = 0usize;
    let mut findings: Vec<(String, KeyCopyFinding)> = Vec::new();

    for storage_key in [crate::repo::KEY_PRIMARY, crate::repo::KEY_SECONDARY] {
        let finding = match repo.storage().get(storage_key) {
            Ok(None) => KeyCopyFinding::Absent,
            Err(e) => KeyCopyFinding::Unreadable(e.to_string()),
            Ok(Some(bytes)) => {
                checked += 1;
                // Screen every fresh read for unambiguous damage — including
                // the copy that matches, so a defect is never inferred from
                // the byte comparison alone.
                match vykar_crypto::key::inspect_key_blob(&bytes) {
                    Err(defect) => KeyCopyFinding::Corrupt(defect.to_string()),
                    Ok(_) if bytes == winner => KeyCopyFinding::Intact,
                    Ok(_) => KeyCopyFinding::Foreign,
                }
            }
        };
        findings.push((storage_key.to_string(), finding));
    }

    let source = findings
        .iter()
        .find(|(_, f)| matches!(f, KeyCopyFinding::Intact))
        .map(|(key, _)| key.clone());

    let mut scan = KeyScan {
        checked,
        source: source.clone(),
        ..KeyScan::default()
    };

    // A copy is queued for rewriting only while some stored copy still holds
    // the key: with `source == None` there is nothing on storage to repair
    // *from*, so damage is reported and both copies are left alone.
    let queue = |scan: &mut KeyScan, storage_key: String| {
        if source.is_some() {
            scan.to_restore.push(storage_key);
        }
    };

    for (storage_key, finding) in findings {
        match finding {
            KeyCopyFinding::Intact => {}
            KeyCopyFinding::Absent => {
                scan.issues.push(IntegrityIssue::MissingKeyCopy {
                    storage_key: storage_key.clone(),
                });
                queue(&mut scan, storage_key);
            }
            // Not proven corruption, so never rewritten — the object may be
            // intact and merely unreachable.
            KeyCopyFinding::Unreadable(detail) => {
                scan.issues.push(IntegrityIssue::UnreadableKeyCopy {
                    storage_key,
                    detail,
                });
            }
            KeyCopyFinding::Corrupt(detail) => {
                scan.issues.push(IntegrityIssue::CorruptKeyCopy {
                    storage_key: storage_key.clone(),
                    detail,
                });
                queue(&mut scan, storage_key);
            }
            KeyCopyFinding::Foreign => match source {
                Some(ref good) => {
                    scan.issues.push(IntegrityIssue::DivergentKeyCopies {
                        good_key: good.clone(),
                        bad_key: storage_key.clone(),
                    });
                    queue(&mut scan, storage_key);
                }
                None => scan.issues.push(IntegrityIssue::CorruptKeyCopy {
                    storage_key,
                    detail: "changed since the repository was opened".into(),
                }),
            },
        }
    }
    scan
}

/// Map a `SnapshotMeta` decode failure to an integrity issue.
///
/// Only call this once the AEAD decrypt has succeeded. Authenticated
/// decryption proves the bytes are intact and the key is right, so a
/// positional-array length mismatch can only mean the writer used a different
/// envelope — a newer vykar. Classifying that as
/// [`IntegrityIssue::IncompatibleSnapshotEnvelope`] rather than
/// `CorruptSnapshot` is what keeps `check --repair` from deleting another
/// host's snapshot.
fn classify_meta_decode_issue(
    err: &rmp_serde::decode::Error,
    meta_bytes: &[u8],
    snapshot_id: SnapshotId,
    snapshot_name: Option<String>,
) -> IntegrityIssue {
    use crate::repo::snapshot_cache::{SkipReason, classify_decode_error};
    match classify_decode_error(err, meta_bytes) {
        reason @ SkipReason::IncompatibleEnvelope { .. } => {
            IntegrityIssue::IncompatibleSnapshotEnvelope {
                snapshot_id,
                snapshot_name,
                detail: reason.to_string(),
            }
        }
        _ => IntegrityIssue::CorruptSnapshot {
            snapshot_id,
            snapshot_name,
        },
    }
}

/// One snapshot of Phase 1 between its metadata load and the attribution
/// of the tree-chunk results.
struct SnapshotPass {
    entry: SnapshotEntry,
    /// Metadata and `DanglingItemPtr` issues, in today's order.
    issues: Vec<IntegrityIssue>,
    /// `None` when the metadata did not load.
    item_ptrs: Option<Vec<ChunkId>>,
    /// `SnapshotMeta.ext`, checked against the listing in Phase 1c.
    ext: Option<Vec<u8>>,
}

/// An item-level finding in one tree chunk, by the item's ordinal in the
/// chunk; kept in item order, as the issues are reported.
enum ItemFinding {
    Invalid {
        index: usize,
        path: String,
        reason: String,
    },
    Dangling {
        index: usize,
        path: String,
        chunk_id: ChunkId,
    },
}

/// What decoding one tree chunk on its own found.
struct CheckedChunk {
    items: usize,
    issues: Vec<ItemFinding>,
    /// Per item, its file chunk set (empty for non-regular items); collect
    /// mode only.
    per_item: Vec<HashSet<ChunkId>>,
    /// Decoding stopped here, after `items` items.
    error: Option<VykarError>,
    /// What the key-index check needs of the chunk's paths.
    paths: ChunkPaths,
}

/// Count down one use of slot `k`; true on the last.
fn release_use(uses_left: &mut [u32], k: usize) -> bool {
    match uses_left.get_mut(k) {
        Some(n) => {
            *n = n.saturating_sub(1);
            *n == 0
        }
        None => true,
    }
}

/// Decode one tree chunk and check its items: `validate()` and every
/// `RegularFile` chunk reference against the index.
fn check_tree_chunk(
    plain: &[u8],
    index: &LiveIndex,
    collect_refs: bool,
    cancel: &CancelToken,
) -> CheckedChunk {
    let mut chunk = CheckedChunk {
        items: 0,
        issues: Vec::new(),
        per_item: Vec::new(),
        error: None,
        paths: ChunkPaths::default(),
    };
    let result = for_each_decoded_item(plain, |item| {
        // Per-item poll: a single multi-million-item snapshot must stay
        // responsive, not just the per-snapshot boundary.
        cancel.check()?;
        let idx = chunk.items;
        chunk.items += 1;
        chunk.paths.observe(item.path_bytes());
        if let Err(e) = item.validate() {
            chunk.issues.push(ItemFinding::Invalid {
                index: idx,
                path: item.path.clone(),
                reason: e.to_string(),
            });
        }
        let mut this_item: HashSet<ChunkId> = HashSet::new();
        if item.entry_type == ItemType::RegularFile {
            for chunk_ref in &item.chunks {
                if collect_refs {
                    this_item.insert(chunk_ref.id);
                }
                if !index.contains(&chunk_ref.id) {
                    chunk.issues.push(ItemFinding::Dangling {
                        index: idx,
                        path: item.path.clone(),
                        chunk_id: chunk_ref.id,
                    });
                }
            }
        }
        if collect_refs {
            chunk.per_item.push(this_item);
        }
        Ok(())
    });
    chunk.error = result.err();
    chunk
}

/// Check snapshot `name`'s key index (`ext`) against the path summaries of
/// its decoded tree chunks. A mismatch is logged and recorded in
/// `warnings`; it is not an integrity issue (readers scan instead).
fn check_listing_index(
    name: &str,
    ext: Option<&[u8]>,
    ptrs: &[ChunkId],
    set: &TreeChunkSet,
    outcomes: &[Option<ChunkOutcome<CheckedChunk>>],
    warnings: &mut Vec<(String, String)>,
) {
    let mut sums: Vec<&ChunkPaths> = Vec::with_capacity(ptrs.len());
    for id in ptrs {
        match set.slot(id).and_then(|k| outcomes.get(k)) {
            Some(Some(ChunkOutcome::Decoded(c))) => sums.push(&c.paths),
            _ => return,
        }
    }
    match check_ext(ext, &sums) {
        ExtCheck::Ok => {}
        ExtCheck::NoIndex(no) => {
            tracing::debug!(snapshot = %name, reason = %no, "no listing key index");
        }
        ExtCheck::Mismatch(reason) => {
            tracing::warn!(
                "snapshot '{name}': its listing index does not match the listing ({reason}); \
                 reads of this snapshot scan the whole listing. Nothing is damaged."
            );
            warnings.push((name.to_string(), reason));
        }
    }
}

/// What [`integrity_scan`] found, for the crate's unit tests.
#[cfg(test)]
pub(crate) struct ScanForTest {
    pub(crate) issues: Vec<IntegrityIssue>,
    pub(crate) snapshots_checked: usize,
    pub(crate) items_checked: usize,
    pub(crate) snapshot_per_item_chunks: HashMap<SnapshotId, Vec<HashSet<ChunkId>>>,
    pub(crate) snapshot_item_counts: HashMap<SnapshotId, usize>,
    pub(crate) snapshot_chunk_refs: HashMap<String, HashSet<ChunkId>>,
    pub(crate) listing_index_warnings: Vec<(String, String)>,
}

/// Run [`integrity_scan`] without orphan detection or data verification.
#[cfg(test)]
pub(crate) fn scan_for_test(
    repo: &mut Repo,
    config: &VykarConfig,
    collect_chunk_refs: bool,
) -> Result<ScanForTest> {
    let opts = ScanOptions {
        collect_chunk_refs,
        detect_orphans: false,
        verify_data: false,
        skip_packs: None,
        snapshot_sample: None,
    };
    let scan = integrity_scan(repo, config, &opts, None, &CancelToken::new())?;
    Ok(ScanForTest {
        issues: scan.issues,
        snapshots_checked: scan.counters.snapshots_checked,
        items_checked: scan.counters.items_checked,
        snapshot_per_item_chunks: scan.snapshot_per_item_chunks,
        snapshot_item_counts: scan.snapshot_item_counts,
        snapshot_chunk_refs: scan.snapshot_chunk_refs,
        listing_index_warnings: scan.listing_index_warnings,
    })
}

/// An [`IntegrityIssue::DuplicateSnapshotName`] for each name more than one
/// of `entries` carries, by name; the IDs in the order of `entries`.
pub(super) fn duplicate_names(entries: &[SnapshotEntry]) -> Vec<IntegrityIssue> {
    let mut by_name: std::collections::BTreeMap<&str, Vec<SnapshotId>> =
        std::collections::BTreeMap::new();
    for entry in entries {
        by_name.entry(&entry.name).or_default().push(entry.id);
    }
    by_name
        .into_iter()
        .filter(|(_, ids)| ids.len() > 1)
        .map(|(name, ids)| IntegrityIssue::DuplicateSnapshotName {
            snapshot_name: name.to_string(),
            snapshot_ids: ids,
        })
        .collect()
}

/// Run the integrity scan, producing structured issues.
///
/// `ScanOptions` controls which phases run and which packs are skipped.
/// The caller is responsible for calling `repo.refresh_snapshot_list()` before
/// this function when repair-level freshness is needed.
pub(super) fn integrity_scan(
    repo: &mut Repo,
    config: &VykarConfig,
    opts: &ScanOptions,
    events: Option<&dyn EventSink<CheckProgressEvent>>,
    cancel: &CancelToken,
) -> Result<ScanResult> {
    let mut counters = ScanCounters::default();
    let mut issues: Vec<IntegrityIssue> = Vec::new();
    let mut snapshot_chunk_refs: HashMap<String, HashSet<ChunkId>> = HashMap::new();
    let mut snapshot_item_ptrs: HashMap<SnapshotId, HashSet<ChunkId>> = HashMap::new();
    let mut snapshot_per_item_chunks: HashMap<SnapshotId, Vec<HashSet<ChunkId>>> = HashMap::new();
    let mut snapshot_item_counts: HashMap<SnapshotId, usize> = HashMap::new();
    let mut listing_index_warnings: Vec<(String, String)> = Vec::new();

    let concurrency = repo.storage().profile().request_width;

    // Phase K: repository key copies. Runs before the snapshot passes so a
    // damaged key file is the first thing reported, and so a healthy run says
    // out loud that the key was verified.
    let key_scan = scan_key_copies(repo);
    counters.key_files_checked = key_scan.checked;
    issues.extend(key_scan.issues);

    // Phase 0: Raw storage scan for corrupted/invalid snapshots not in manifest.
    if opts.detect_orphans {
        let manifest_ids: HashSet<String> = repo
            .manifest()
            .snapshots
            .iter()
            .map(|e| e.id.to_hex())
            .collect();
        let remote_keys = repo.storage().list("snapshots/")?;
        for key in &remote_keys {
            cancel.check()?;
            let Some(id_hex) = key.strip_prefix("snapshots/") else {
                continue;
            };
            if id_hex.is_empty() || manifest_ids.contains(id_hex) {
                continue;
            }
            match SnapshotId::from_hex(id_hex) {
                Err(_) => {
                    issues.push(IntegrityIssue::InvalidSnapshotKey {
                        storage_key: key.clone(),
                    });
                }
                Ok(snapshot_id) => match repo.storage().get(key) {
                    Ok(Some(blob)) => {
                        // Keep decrypt and decode failures separate: a decode
                        // failure *after* a successful decrypt is a version
                        // skew, not corruption, and repair must not delete it.
                        match unpack_object_expect_with_context(
                            &blob,
                            ObjectType::SnapshotMeta,
                            snapshot_id.as_bytes(),
                            repo.crypto().as_ref(),
                        ) {
                            Err(_) => {
                                issues.push(IntegrityIssue::CorruptSnapshot {
                                    snapshot_id,
                                    snapshot_name: None,
                                });
                            }
                            Ok(meta_bytes) => {
                                match rmp_serde::from_slice::<crate::snapshot::SnapshotMeta>(
                                    &meta_bytes,
                                ) {
                                    // A too-new blob whose envelope still
                                    // matches deserializes fine: classify
                                    // unsupported, never corrupt.
                                    Ok(meta)
                                        if meta.format_version
                                            > crate::snapshot::CURRENT_FORMAT_VERSION =>
                                    {
                                        issues.push(IntegrityIssue::UnsupportedSnapshotVersion {
                                            snapshot_id,
                                            snapshot_name: None,
                                            version: meta.format_version,
                                        });
                                    }
                                    Ok(_) => {}
                                    Err(e) => issues.push(classify_meta_decode_issue(
                                        &e,
                                        &meta_bytes,
                                        snapshot_id,
                                        None,
                                    )),
                                }
                            }
                        }
                    }
                    Ok(None) => {
                        tracing::warn!("snapshot {snapshot_id} listed but not found, skipping");
                    }
                    Err(e) => {
                        issues.push(IntegrityIssue::SnapshotReadFailed {
                            snapshot_name: format!("(orphan {snapshot_id})"),
                            detail: format!("GET failed: {e}"),
                        });
                    }
                },
            }
        }
    }

    // A name more than one object carries: a lookup by name finds only the
    // first. Every object is checked below, by its own ID.
    issues.extend(duplicate_names(&repo.manifest().snapshots));

    // Phase 1: Check each snapshot in manifest
    let all_snapshot_entries = repo.manifest().snapshots.clone();
    let snapshot_entries: Vec<_> = match opts.snapshot_sample {
        Some(sample) => all_snapshot_entries
            .into_iter()
            .filter(|e| sample.contains(&e.id))
            .collect(),
        None => all_snapshot_entries,
    };
    let snapshot_count = snapshot_entries.len();

    // Phase 1a: snapshot metadata, in sample order. Issues are kept per
    // snapshot so the final order stays per snapshot.
    let mut passes: Vec<SnapshotPass> = Vec::with_capacity(snapshot_count);
    for (i, entry) in snapshot_entries.into_iter().enumerate() {
        cancel.check()?;
        emit_progress(
            events,
            CheckProgressEvent::SnapshotStarted {
                current: i + 1,
                total: snapshot_count,
                name: entry.name.clone(),
            },
        );
        let mut pass = SnapshotPass {
            entry,
            issues: Vec::new(),
            item_ptrs: None,
            ext: None,
        };
        let entry = &pass.entry;

        // By the entry's own object: several objects can carry one name.
        let meta = match load_snapshot_meta_of(repo, entry) {
            Ok(m) => m,
            Err(VykarError::UnsupportedSnapshotVersion { version }) => {
                // The envelope decoded — the blob is intact, just too new.
                // Classify as unsupported (never corrupt) and skip walking
                // item_ptrs, since we can't trust a future item layout.
                pass.issues
                    .push(IntegrityIssue::UnsupportedSnapshotVersion {
                        snapshot_id: entry.id,
                        snapshot_name: Some(entry.name.clone()),
                        version,
                    });
                passes.push(pass);
                continue;
            }
            Err(VykarError::IncompatibleSnapshotEnvelope { detail }) => {
                // Decrypt succeeded and only the positional layout differs, so
                // the blob is intact and was written by a newer vykar. Same
                // treatment as a too-new format version: report, never repair.
                pass.issues
                    .push(IntegrityIssue::IncompatibleSnapshotEnvelope {
                        snapshot_id: entry.id,
                        snapshot_name: Some(entry.name.clone()),
                        detail,
                    });
                passes.push(pass);
                continue;
            }
            // A cancelled storage read is not corruption (see below).
            Err(e @ VykarError::Interrupted) => return Err(e),
            // Listed when the repository was opened, its object gone now:
            // a prune or delete removed it during the check (readers
            // register no marker since 04 step 7). Skipped, as the orphan
            // pass skips one; an object that exists and does not decode
            // stays corrupt.
            Err(VykarError::SnapshotNotFound(_)) => {
                tracing::warn!(
                    "snapshot {} was deleted during the check, skipping",
                    entry.name
                );
                continue;
            }
            Err(e) => {
                if is_transient_io(&e) {
                    pass.issues.push(IntegrityIssue::SnapshotReadFailed {
                        snapshot_name: entry.name.clone(),
                        detail: format!("load metadata: {e}"),
                    });
                } else {
                    pass.issues.push(IntegrityIssue::CorruptSnapshot {
                        snapshot_id: entry.id,
                        snapshot_name: Some(entry.name.clone()),
                    });
                }
                passes.push(pass);
                continue;
            }
        };

        // Verify item_ptrs exist in chunk index; optionally collect chunk refs.
        // `snapshot_item_ptrs` is populated unconditionally (fail-closed input
        // for the item-ptrs coverage gate), even when `collect_chunk_refs` is
        // disabled or the items_stream walk later fails.
        for chunk_id in &meta.item_ptrs {
            snapshot_item_ptrs
                .entry(entry.id)
                .or_default()
                .insert(*chunk_id);
            if opts.collect_chunk_refs {
                snapshot_chunk_refs
                    .entry(entry.name.clone())
                    .or_default()
                    .insert(*chunk_id);
            }
            if !repo.chunk_index().contains(chunk_id) {
                pass.issues.push(IntegrityIssue::DanglingItemPtr {
                    snapshot_name: entry.name.clone(),
                    chunk_id: *chunk_id,
                });
            }
        }
        pass.item_ptrs = Some(meta.item_ptrs);
        pass.ext = meta.ext;
        passes.push(pass);
    }

    // Phase 1b: read and decode every distinct tree chunk once, from storage.
    let ptr_lists: Vec<&[ChunkId]> = passes
        .iter()
        .filter_map(|p| p.item_ptrs.as_deref())
        .collect();
    let set = TreeChunkSet::build(&ptr_lists);
    drop(ptr_lists);
    let collect_refs = opts.collect_chunk_refs;
    let mut outcomes: Vec<Option<ChunkOutcome<CheckedChunk>>> =
        std::iter::repeat_with(|| None).take(set.len()).collect();
    {
        let repo: &Repo = repo;
        let index = repo.chunk_index();
        walk_tree_chunks(
            repo,
            &set,
            TreeChunkSource::Storage,
            OnChunkError::Record,
            default_decode_threads(),
            cancel,
            |_, plain| check_tree_chunk(plain, index, collect_refs, cancel),
            |k, outcome| {
                if let Some(slot) = outcomes.get_mut(k) {
                    *slot = Some(outcome);
                }
                Ok(())
            },
        )?;
    }

    // Phase 1c: attribute the results to the snapshots, in sample order.
    // A snapshot's stream fails to load when one of its pointers is not in
    // the index (resolution comes before any read) or its read failed.
    let stream_failure = |ptrs: &[ChunkId]| -> Result<Option<IntegrityIssue>> {
        let outcome = |id: &ChunkId| set.slot(id).and_then(|k| outcomes.get(k)?.as_ref());
        for id in ptrs {
            match outcome(id) {
                Some(ChunkOutcome::NotInIndex) => {
                    return Ok(Some(IntegrityIssue::UnreadableSnapshot {
                        snapshot_name: String::new(),
                        detail: format!("load item stream: {}", VykarError::ChunkNotInIndex(*id)),
                    }));
                }
                Some(_) => {}
                None => {
                    return Err(VykarError::Other(format!(
                        "internal error: tree chunk {id} was not walked"
                    )));
                }
            }
        }
        for id in ptrs {
            if let Some(ChunkOutcome::ReadFailed(e)) = outcome(id) {
                let detail = format!("load item stream: {e}");
                return Ok(Some(if is_transient_io(e) {
                    IntegrityIssue::SnapshotReadFailed {
                        snapshot_name: String::new(),
                        detail,
                    }
                } else {
                    IntegrityIssue::UnreadableSnapshot {
                        snapshot_name: String::new(),
                        detail,
                    }
                }));
            }
        }
        Ok(None)
    };
    let mut stream_issues: Vec<Option<IntegrityIssue>> = Vec::with_capacity(passes.len());
    // Per slot, how many pointer occurrences of walked snapshots remain; a
    // chunk's per-item sets are moved out on the last one.
    let mut uses_left: Vec<u32> = vec![0; set.len()];
    for pass in &passes {
        let issue = match &pass.item_ptrs {
            Some(ptrs) => stream_failure(ptrs)?,
            None => None,
        };
        if issue.is_none()
            && let Some(ptrs) = &pass.item_ptrs
        {
            for id in ptrs {
                if let Some(n) = set.slot(id).and_then(|k| uses_left.get_mut(k)) {
                    *n += 1;
                }
            }
        }
        stream_issues.push(issue);
    }

    for (pass, stream_issue) in passes.into_iter().zip(stream_issues) {
        issues.extend(pass.issues);
        let entry = pass.entry;
        let Some(ptrs) = pass.item_ptrs else {
            continue;
        };
        if let Some(mut issue) = stream_issue {
            match &mut issue {
                IntegrityIssue::UnreadableSnapshot { snapshot_name, .. }
                | IntegrityIssue::SnapshotReadFailed { snapshot_name, .. } => {
                    *snapshot_name = entry.name.clone();
                }
                _ => {}
            }
            issues.push(issue);
            continue;
        }

        let mut offset = 0usize;
        let mut item_issues: Vec<IntegrityIssue> = Vec::new();
        let mut file_chunk_ids: HashSet<ChunkId> = HashSet::new();
        let mut per_item_chunks: Vec<HashSet<ChunkId>> = Vec::new();
        let mut walk_error: Option<String> = None;
        let mut done = 0usize;
        for id in &ptrs {
            done += 1;
            let Some(k) = set.slot(id) else {
                continue;
            };
            let last_use = release_use(&mut uses_left, k);
            let Some(Some(ChunkOutcome::Decoded(chunk))) = outcomes.get_mut(k) else {
                continue;
            };
            for issue in &chunk.issues {
                item_issues.push(match issue {
                    ItemFinding::Invalid {
                        index,
                        path,
                        reason,
                    } => IntegrityIssue::InvalidItem {
                        snapshot_id: entry.id,
                        snapshot_name: Some(entry.name.clone()),
                        item_index: offset + index,
                        item_path: path.clone(),
                        reason: reason.clone(),
                    },
                    ItemFinding::Dangling {
                        index,
                        path,
                        chunk_id,
                    } => IntegrityIssue::DanglingFileChunk {
                        snapshot_name: entry.name.clone(),
                        item_index: offset + index,
                        path: path.clone(),
                        chunk_id: *chunk_id,
                    },
                });
            }
            if collect_refs {
                let sets = if last_use {
                    std::mem::take(&mut chunk.per_item)
                } else {
                    chunk.per_item.clone()
                };
                for s in &sets {
                    file_chunk_ids.extend(s.iter().copied());
                }
                per_item_chunks.extend(sets);
            }
            offset += chunk.items;
            if let Some(e) = &chunk.error {
                // A cancelled decode is not an unreadable snapshot.
                if matches!(e, VykarError::Interrupted) {
                    return Err(VykarError::Interrupted);
                }
                walk_error = Some(e.to_string());
                break;
            }
        }
        // Pointers after a decode error are not walked: release them.
        for id in ptrs.get(done..).unwrap_or_default() {
            if let Some(k) = set.slot(id)
                && release_use(&mut uses_left, k)
                && let Some(Some(ChunkOutcome::Decoded(chunk))) = outcomes.get_mut(k)
            {
                chunk.per_item = Vec::new();
            }
        }

        let walk_ok = walk_error.is_none();
        if walk_ok {
            check_listing_index(
                &entry.name,
                pass.ext.as_deref(),
                &ptrs,
                &set,
                &outcomes,
                &mut listing_index_warnings,
            );
        }
        if let Some(e) = walk_error {
            issues.push(IntegrityIssue::UnreadableSnapshot {
                snapshot_name: entry.name.clone(),
                detail: format!("decode items: {e}"),
            });
        }
        issues.extend(item_issues);
        if collect_refs {
            snapshot_chunk_refs
                .entry(entry.name.clone())
                .or_default()
                .extend(file_chunk_ids);
            // Only publish per-item chunks and item count when the walk
            // completed: a partial walk would yield a vec shorter than the
            // true item count, and the planner's data-presence gate must be
            // able to reject such snapshots from item-level repair.
            if walk_ok {
                debug_assert_eq!(per_item_chunks.len(), offset);
                snapshot_per_item_chunks.insert(entry.id, per_item_chunks);
                snapshot_item_counts.insert(entry.id, offset);
            }
        } else if walk_ok {
            // collect_chunk_refs disabled (read-only check): we don't need the
            // per-item chunk sets, but record the count so a future repair
            // pass can see this snapshot was successfully walked.
            snapshot_item_counts.insert(entry.id, offset);
        }

        counters.items_checked += offset;
        counters.snapshots_checked += 1;
    }
    drop(outcomes);

    // Phase 2: Pack existence check
    let mut pack_chunks: HashMap<PackId, Vec<(ChunkId, ChunkIndexEntry)>> = HashMap::new();
    for (chunk_id, entry) in repo.chunk_index().iter() {
        if let Some(skip) = opts.skip_packs
            && skip.contains(&entry.pack_id)
        {
            continue;
        }
        pack_chunks
            .entry(entry.pack_id)
            .or_default()
            .push((chunk_id, entry));
    }
    let packs_for_existence: Vec<(PackId, usize)> = pack_chunks
        .iter()
        .map(|(pack_id, chunks)| (*pack_id, chunks.len()))
        .collect();

    let mut missing_packs: HashSet<PackId> = HashSet::new();
    if !packs_for_existence.is_empty() {
        emit_progress(
            events,
            CheckProgressEvent::PacksExistencePhaseStarted {
                total_packs: packs_for_existence.len(),
            },
        );

        let (existence_checked, missing_count, pack_issues) =
            parallel_pack_existence(repo.storage(), &packs_for_existence, concurrency, cancel);
        // Workers `break` out of the work queue on cancellation, so a short
        // count must never be folded into the result as coverage.
        cancel.check()?;
        counters.packs_existence_checked = existence_checked;

        // Count chunks only in packs whose existence was definitively resolved
        // (Ok(true) or Ok(false)). Packs with I/O errors are not counted.
        let io_failed_packs: HashSet<PackId> = pack_issues
            .iter()
            .filter_map(|issue| match issue {
                IntegrityIssue::PackExistenceCheckFailed { pack_id, .. } => Some(*pack_id),
                _ => None,
            })
            .collect();
        counters.chunks_existence_checked = pack_chunks
            .iter()
            .filter(|(pid, _)| !io_failed_packs.contains(pid))
            .map(|(_, chunks)| chunks.len())
            .sum();

        for issue in &pack_issues {
            if let IntegrityIssue::MissingPack { pack_id } = issue {
                missing_packs.insert(*pack_id);
            }
        }

        issues.extend(pack_issues);

        emit_progress(
            events,
            CheckProgressEvent::PacksExistenceProgress {
                checked: existence_checked,
                total_packs: packs_for_existence.len(),
                missing: missing_count,
            },
        );
    }

    // Phase 2b: Locate snapshot items affected by missing packs (issue #122).
    // Cheap on healthy repos — short-circuits when no packs are missing.
    let item_impacts = locate_items_in_missing_packs(repo, &missing_packs, cancel)?;

    // Phase 3: Verify data (client-side crypto verification)
    if opts.verify_data {
        let remaining_chunks: usize = pack_chunks.values().map(|chunks| chunks.len()).sum();

        emit_progress(
            events,
            CheckProgressEvent::ChunksDataPhaseStarted {
                total_chunks: remaining_chunks,
            },
        );

        let packs_vec: Vec<(PackId, Vec<(ChunkId, ChunkIndexEntry)>)> =
            pack_chunks.into_iter().collect();

        let (data_count, data_issues) = parallel_verify_data(
            repo.storage(),
            repo.crypto(),
            &packs_vec,
            config.limits.verify_data_concurrency(),
            BATCH_THRESHOLD,
            cancel,
        );
        cancel.check()?;
        counters.chunks_data_verified = data_count;
        let data_issues = drop_trailer_issues_covered_by_maps(
            repo,
            &packs_vec,
            data_issues,
            config.limits.verify_data_concurrency(),
            cancel,
        )?;
        issues.extend(data_issues);

        emit_progress(
            events,
            CheckProgressEvent::ChunksDataProgress {
                verified: data_count,
                total_chunks: remaining_chunks,
            },
        );
    }

    // Final guard: closes the window where the flag is raised after the last
    // loop-level check — e.g. a callback setting it on `PacksExistenceProgress`
    // with `verify_data = false`, where nothing downstream polls again.
    cancel.check()?;

    Ok(ScanResult {
        counters,
        issues,
        snapshot_chunk_refs,
        snapshot_item_ptrs,
        snapshot_per_item_chunks,
        snapshot_item_counts,
        item_impacts,
        key_source: key_scan.source,
        key_copies_to_restore: key_scan.to_restore,
        listing_index_warnings,
    })
}

/// Walk every snapshot's item stream and emit one [`ItemImpact`] per
/// regular-file item that references a chunk in `missing_packs`.
///
/// Snapshots whose item stream fails to load — or whose decode aborts mid-stream
/// — are silently skipped. Such failures are already surfaced by the main scan
/// as `UnreadableSnapshot` / `SnapshotReadFailed` issues. A cancellation is the
/// one failure that is *not* skipped: it is re-raised, so a truncated impact
/// list can never be mistaken for a complete one.
fn locate_items_in_missing_packs(
    repo: &mut Repo,
    missing_packs: &HashSet<PackId>,
    cancel: &CancelToken,
) -> Result<Vec<ItemImpact>> {
    if missing_packs.is_empty() {
        return Ok(Vec::new());
    }

    // Restrict the chunk → pack lookup to chunks in missing packs.
    let chunk_to_missing_pack: HashMap<ChunkId, PackId> = repo
        .chunk_index()
        .iter()
        .filter(|(_, entry)| missing_packs.contains(&entry.pack_id))
        .map(|(chunk_id, entry)| (chunk_id, entry.pack_id))
        .collect();

    if chunk_to_missing_pack.is_empty() {
        return Ok(Vec::new());
    }

    let entries = repo.manifest().snapshots.clone();
    let mut impacts: Vec<ItemImpact> = Vec::new();

    for entry in &entries {
        cancel.check()?;
        // By the entry's own object: several objects can carry one name.
        let items_stream = match load_snapshot_meta_of(repo, entry)
            .and_then(|meta| load_item_stream_of_meta(repo, &meta, cancel))
        {
            Ok(s) => s,
            Err(e @ VykarError::Interrupted) => return Err(e),
            Err(_) => continue,
        };

        let mut item_index: usize = 0;
        let mut local: Vec<ItemImpact> = Vec::new();
        let walk = for_each_decoded_item(&items_stream, |item| {
            cancel.check()?;
            let idx = item_index;
            item_index += 1;
            if item.entry_type == ItemType::RegularFile {
                let mut affected: Vec<(ChunkId, PackId)> = Vec::new();
                for chunk_ref in &item.chunks {
                    if let Some(pack_id) = chunk_to_missing_pack.get(&chunk_ref.id) {
                        affected.push((chunk_ref.id, *pack_id));
                    }
                }
                if !affected.is_empty() {
                    local.push(ItemImpact {
                        snapshot_id: entry.id,
                        snapshot_name: entry.name.clone(),
                        item_index: idx,
                        item_path: item.path.clone(),
                        affected_chunks: affected,
                    });
                }
            }
            Ok(())
        });
        // Drop partial impacts if decode aborted mid-stream — the snapshot is
        // already reported as UnreadableSnapshot by the main scan, and #123
        // will treat it as whole-snapshot doomed. A cancelled walk is the
        // exception: silently dropping it and moving to the next snapshot
        // would hand back a truncated list as if it were complete.
        match walk {
            Ok(()) => impacts.extend(local),
            Err(e @ VykarError::Interrupted) => return Err(e),
            Err(_) => {}
        }
    }

    Ok(impacts)
}

/// Parallel pack existence check producing IntegrityIssue variants.
/// Returns `(packs_actually_checked, missing_count, issues)` — `packs_actually_checked`
/// counts packs whose existence was definitively resolved (Ok(true) or Ok(false));
/// packs with I/O errors are NOT counted so the summary does not claim complete coverage.
/// `missing_count` is the subset of those resolved as Ok(false).
fn parallel_pack_existence(
    storage: &Arc<dyn StorageBackend>,
    packs: &[(PackId, usize)],
    concurrency: usize,
    cancel: &CancelToken,
) -> (usize, usize, Vec<IntegrityIssue>) {
    if packs.is_empty() {
        return (0, 0, Vec::new());
    }

    let present_ok = AtomicUsize::new(0);
    let missing_count = AtomicUsize::new(0);
    let issues = Mutex::new(Vec::new());

    crate::repo::bounded::for_each_bounded(
        concurrency,
        packs,
        cancel,
        |(pack_id, _chunk_count)| {
            let pack_key = pack_id.storage_key();
            match storage.exists(&pack_key) {
                Ok(true) => {
                    present_ok.fetch_add(1, Ordering::Relaxed);
                }
                Ok(false) => {
                    missing_count.fetch_add(1, Ordering::Relaxed);
                    issues
                        .lock()
                        .expect("scan present issues lock not poisoned")
                        .push(IntegrityIssue::MissingPack { pack_id: *pack_id });
                }
                Err(e) => {
                    issues
                        .lock()
                        .expect("scan present issues lock not poisoned")
                        .push(IntegrityIssue::PackExistenceCheckFailed {
                            pack_id: *pack_id,
                            detail: e.to_string(),
                        });
                }
            }
        },
    );

    let present = present_ok.load(Ordering::Relaxed);
    let missing = missing_count.load(Ordering::Relaxed);
    (
        present + missing,
        missing,
        issues
            .into_inner()
            .expect("scan present issues lock not poisoned"),
    )
}

/// Parallel verify-data producing IntegrityIssue variants.
fn parallel_verify_data(
    storage: &Arc<dyn StorageBackend>,
    crypto: &Arc<dyn CryptoEngine>,
    packs: &[(PackId, Vec<(ChunkId, ChunkIndexEntry)>)],
    concurrency: usize,
    batch_threshold: usize,
    cancel: &CancelToken,
) -> (usize, Vec<IntegrityIssue>) {
    if packs.is_empty() {
        return (0, Vec::new());
    }

    let verified = AtomicUsize::new(0);
    let issues = Mutex::new(Vec::new());

    crate::repo::bounded::for_each_bounded(concurrency, packs, cancel, |(pack_id, chunks)| {
        let mut local_issues = Vec::new();
        let count = if chunks.len() >= batch_threshold {
            verify_pack_full(
                storage.as_ref(),
                crypto.as_ref(),
                pack_id,
                chunks,
                &mut local_issues,
            )
        } else {
            verify_pack_individual(
                storage.as_ref(),
                crypto.as_ref(),
                pack_id,
                chunks,
                &mut local_issues,
                cancel,
            )
        };

        verified.fetch_add(count, Ordering::Relaxed);
        if !local_issues.is_empty() {
            issues
                .lock()
                .expect("scan verify issues lock not poisoned")
                .extend(local_issues);
        }
    });

    (
        verified.load(Ordering::Relaxed),
        issues
            .into_inner()
            .expect("scan verify issues lock not poisoned"),
    )
}

/// Download the full pack and verify each chunk locally.
pub(crate) fn verify_pack_full(
    storage: &dyn StorageBackend,
    crypto: &dyn CryptoEngine,
    pack_id: &PackId,
    chunks: &[(ChunkId, ChunkIndexEntry)],
    issues: &mut Vec<IntegrityIssue>,
) -> usize {
    let pack_key = pack_id.storage_key();
    let pack_data = match storage.get(&pack_key) {
        Ok(Some(data)) => data,
        Ok(None) => {
            issues.push(IntegrityIssue::CorruptPackContent {
                pack_id: *pack_id,
                detail: "pack not found (full GET)".into(),
            });
            return 0;
        }
        Err(e) => {
            issues.push(IntegrityIssue::PackExistenceCheckFailed {
                pack_id: *pack_id,
                detail: format!("full GET failed: {e}"),
            });
            return 0;
        }
    };

    if validate_pack_header(&pack_data).is_err() {
        issues.push(IntegrityIssue::CorruptPackContent {
            pack_id: *pack_id,
            detail: "invalid pack header".into(),
        });
        return 0;
    }

    match read_trailer_from_pack_bytes(&pack_data, crypto) {
        Ok(None) => {}
        Ok(Some(trailer)) => check_trailer(pack_id, &trailer, chunks, issues),
        Err(e) => trailer_damage(pack_id, e, issues),
    }

    let mut count = 0;
    let mut raw = Vec::new();
    for (chunk_id, entry) in chunks {
        let start = match usize::try_from(entry.pack_offset) {
            Ok(s) => s,
            Err(_) => {
                issues.push(IntegrityIssue::CorruptChunk {
                    chunk_id: *chunk_id,
                    pack_id: *pack_id,
                    detail: format!(
                        "pack_offset {} exceeds addressable range",
                        entry.pack_offset
                    ),
                });
                continue;
            }
        };
        let size = match usize::try_from(entry.stored_size) {
            Ok(s) => s,
            Err(_) => {
                issues.push(IntegrityIssue::CorruptChunk {
                    chunk_id: *chunk_id,
                    pack_id: *pack_id,
                    detail: format!(
                        "stored_size {} exceeds addressable range",
                        entry.stored_size
                    ),
                });
                continue;
            }
        };
        let end = match start.checked_add(size) {
            Some(e) => e,
            None => {
                issues.push(IntegrityIssue::CorruptChunk {
                    chunk_id: *chunk_id,
                    pack_id: *pack_id,
                    detail: format!(
                        "blob range overflows (offset={}, size={})",
                        entry.pack_offset, entry.stored_size
                    ),
                });
                continue;
            }
        };
        if end > pack_data.len() {
            issues.push(IntegrityIssue::CorruptChunk {
                chunk_id: *chunk_id,
                pack_id: *pack_id,
                detail: format!(
                    "blob range [{start}..{end}) exceeds pack size {}",
                    pack_data.len()
                ),
            });
            continue;
        }

        let blob = pack_data
            .get(start..end)
            .expect("end <= pack_data.len() (checked above)");
        // Entries of a corrupt index can overlap; decoding in place would let
        // one failed decrypt damage a valid neighbour, so decode a copy.
        raw.clear();
        raw.extend_from_slice(blob);
        let at = BlobLocation {
            chunk_id: *chunk_id,
            pack_id: *pack_id,
            pack_offset: entry.pack_offset,
            stored_size: entry.stored_size,
        };
        count += verify_single_chunk(crypto, at, &mut raw, issues);
    }
    count
}

/// Verify each chunk individually via range reads.
fn verify_pack_individual(
    storage: &dyn StorageBackend,
    crypto: &dyn CryptoEngine,
    pack_id: &PackId,
    chunks: &[(ChunkId, ChunkIndexEntry)],
    issues: &mut Vec<IntegrityIssue>,
    cancel: &CancelToken,
) -> usize {
    if !cancel.is_cancelled() {
        verify_trailer_by_range(storage, crypto, pack_id, chunks, issues);
    }
    let mut count = 0;
    // One range read per chunk: without this poll a cancelled verify keeps
    // issuing storage requests for the rest of the pack. `break` rather than
    // `Err` — the caller's post-scope guard raises the error.
    for (chunk_id, entry) in chunks {
        if cancel.is_cancelled() {
            break;
        }
        let mut raw =
            match read_blob_from_pack(storage, pack_id, entry.pack_offset, entry.stored_size) {
                Ok(data) => data,
                // A cancelled read is not damage; the caller's guard raises it.
                Err(VykarError::Interrupted) => break,
                Err(e) => {
                    issues.push(IntegrityIssue::CorruptChunk {
                        chunk_id: *chunk_id,
                        pack_id: *pack_id,
                        detail: format!("read failed: {e}"),
                    });
                    continue;
                }
            };
        let at = BlobLocation {
            chunk_id: *chunk_id,
            pack_id: *pack_id,
            pack_offset: entry.pack_offset,
            stored_size: entry.stored_size,
        };
        count += verify_single_chunk(crypto, at, &mut raw, issues);
    }
    count
}

/// Read a pack's trailer with range reads and check it against the pack's
/// index entries. A pack that is gone or a failed request pushes nothing:
/// the chunk reads that follow report it.
fn verify_trailer_by_range(
    storage: &dyn StorageBackend,
    crypto: &dyn CryptoEngine,
    pack_id: &PackId,
    chunks: &[(ChunkId, ChunkIndexEntry)],
    issues: &mut Vec<IntegrityIssue>,
) {
    let key = pack_id.storage_key();
    let Ok(Some(len)) = storage.size(&key) else {
        return;
    };
    match read_pack_trailer(storage, &key, len, crypto) {
        Ok(TrailerRead::Found(trailer)) => check_trailer(pack_id, &trailer, chunks, issues),
        Ok(TrailerRead::Damaged(e)) => trailer_damage(pack_id, e, issues),
        Ok(TrailerRead::Absent | TrailerRead::PackMissing) | Err(_) => {}
    }
}

/// Push a `CorruptPackTrailer` when `trailer` does not list every indexed
/// chunk of the pack at its indexed location.
fn check_trailer(
    pack_id: &PackId,
    trailer: &PackTrailer,
    chunks: &[(ChunkId, ChunkIndexEntry)],
    issues: &mut Vec<IntegrityIssue>,
) {
    if let Some(detail) = trailer_mismatch(trailer, chunks) {
        issues.push(IntegrityIssue::CorruptPackTrailer {
            pack_id: *pack_id,
            detail,
        });
    }
}

/// Push a `CorruptPackTrailer` for a trailer that cannot be read. A trailer
/// of a newer body version or codec is not damage: it is logged only.
fn trailer_damage(pack_id: &PackId, e: VykarError, issues: &mut Vec<IntegrityIssue>) {
    if let VykarError::UnsupportedVersion(_) = e {
        tracing::warn!("the trailer of pack {pack_id} was written by a newer vykar ({e})");
        return;
    }
    issues.push(IntegrityIssue::CorruptPackTrailer {
        pack_id: *pack_id,
        detail: e.to_string(),
    });
}

/// The first way `trailer` disagrees with the pack's index entries: an
/// indexed (chunk ID, offset, stored size) the trailer does not list. Extra
/// trailer entries are fine (chunks the index dropped).
fn trailer_mismatch(
    trailer: &PackTrailer,
    chunks: &[(ChunkId, ChunkIndexEntry)],
) -> Option<String> {
    let listed: HashSet<(ChunkId, u64, u32)> = trailer
        .entries
        .iter()
        .map(|e| (e.chunk_id, u64::from(e.offset), e.stored_size))
        .collect();
    let mut missing = chunks
        .iter()
        .filter(|(id, e)| !listed.contains(&(*id, e.pack_offset, e.stored_size)));
    let first = missing.next()?.0;
    let n = 1 + missing.count();
    Some(format!(
        "lists {n} of {} indexed chunk(s) at other locations or not at all (first: {first})",
        chunks.len()
    ))
}

/// Drop the `CorruptPackTrailer` issues of packs a pack map describes in
/// full (every indexed entry of the pack listed): such a pack does not
/// depend on its trailer any more. Maps are read only when there is such an
/// issue; a failure to read them keeps every issue.
fn drop_trailer_issues_covered_by_maps(
    repo: &Repo,
    packs: &[(PackId, Vec<(ChunkId, ChunkIndexEntry)>)],
    issues: Vec<IntegrityIssue>,
    width: usize,
    cancel: &CancelToken,
) -> Result<Vec<IntegrityIssue>> {
    if !issues
        .iter()
        .any(|i| matches!(i, IntegrityIssue::CorruptPackTrailer { .. }))
    {
        return Ok(issues);
    }
    let maps = match crate::repo::pack_map::read_pack_maps(
        repo.storage().as_ref(),
        repo.crypto().as_ref(),
        width,
        true,
        cancel,
    ) {
        Ok(maps) => maps,
        Err(VykarError::Interrupted) => return Err(VykarError::Interrupted),
        Err(e) => {
            tracing::warn!("cannot read the pack maps ({e}); trailer issues are kept");
            return Ok(issues);
        }
    };
    let chunks_of: HashMap<&PackId, &Vec<(ChunkId, ChunkIndexEntry)>> =
        packs.iter().map(|(id, chunks)| (id, chunks)).collect();
    let covered = |pack_id: &PackId| {
        let (Some(entries), Some(chunks)) = (maps.by_pack.get(pack_id), chunks_of.get(pack_id))
        else {
            return false;
        };
        let listed: HashSet<(ChunkId, u64, u32)> = entries
            .iter()
            .map(|e| (e.chunk_id, u64::from(e.offset), e.stored_size))
            .collect();
        chunks
            .iter()
            .all(|(id, e)| listed.contains(&(*id, e.pack_offset, e.stored_size)))
    };
    Ok(issues
        .into_iter()
        .filter(|i| match i {
            IntegrityIssue::CorruptPackTrailer { pack_id, .. } => !covered(pack_id),
            _ => true,
        })
        .collect())
}

/// Decrypt, decompress, and recompute ChunkId for one blob, decoding in place
/// in `raw`. Returns 1 on success, 0 on error.
fn verify_single_chunk(
    crypto: &dyn CryptoEngine,
    at: BlobLocation,
    raw: &mut [u8],
    issues: &mut Vec<IntegrityIssue>,
) -> usize {
    // A fresh buffer per chunk: a reused one changes how much a frame
    // without a recorded size may decode to before it is rejected.
    let mut out = Vec::new();
    let Err(e) = decode_chunk_into(raw, at, crypto, None, true, &mut out) else {
        return 1;
    };
    let detail = match e.stage {
        ChunkDecodeStage::Decrypt(inner) => format!("decrypt failed: {inner}"),
        ChunkDecodeStage::Decompress(inner) => format!("decompress failed: {inner}"),
        ChunkDecodeStage::IdMismatch { actual } => format!("ID mismatch (recomputed {actual})"),
        // Check passes no size hint, so this does not occur.
        ChunkDecodeStage::SizeMismatch { expected, actual } => {
            format!("size mismatch: expected {expected} bytes, got {actual} bytes")
        }
    };
    issues.push(IntegrityIssue::CorruptChunk {
        chunk_id: at.chunk_id,
        pack_id: at.pack_id,
        detail,
    });
    0
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Per-pack scripted responses for `exists()`, keyed by storage key.
    struct ScriptedExistsBackend {
        responses: std::sync::Mutex<HashMap<String, std::result::Result<bool, String>>>,
    }

    impl StorageBackend for ScriptedExistsBackend {
        fn get(&self, _key: &str) -> vykar_types::error::Result<Option<Vec<u8>>> {
            Ok(None)
        }
        fn put(&self, _key: &str, _data: &[u8]) -> vykar_types::error::Result<()> {
            Ok(())
        }
        fn delete(&self, _key: &str) -> vykar_types::error::Result<()> {
            Ok(())
        }
        fn exists(&self, key: &str) -> vykar_types::error::Result<bool> {
            match self.responses.lock().unwrap().get(key) {
                Some(Ok(b)) => Ok(*b),
                Some(Err(msg)) => Err(VykarError::Other(msg.clone())),
                None => Ok(true),
            }
        }
        fn list(&self, _prefix: &str) -> vykar_types::error::Result<Vec<String>> {
            Ok(Vec::new())
        }
        fn get_range(
            &self,
            _key: &str,
            _offset: u64,
            _length: u64,
        ) -> vykar_types::error::Result<Option<Vec<u8>>> {
            Ok(None)
        }
        fn create_dir(&self, _key: &str) -> vykar_types::error::Result<()> {
            Ok(())
        }

        fn profile(&self) -> vykar_storage::BackendProfile {
            vykar_storage::BackendProfile {
                local: false,
                staged_put: false,
                request_width: 1,
                transfer_width: 1,
            }
        }

        vykar_storage::unsupported_server_ops!();
    }

    #[test]
    fn parallel_pack_existence_excludes_io_failures_from_checked_count() {
        let present = PackId::from_bytes([0x01u8; 32]);
        let missing = PackId::from_bytes([0x02u8; 32]);
        let errored = PackId::from_bytes([0x03u8; 32]);

        let mut responses: HashMap<String, std::result::Result<bool, String>> = HashMap::new();
        responses.insert(present.storage_key(), Ok(true));
        responses.insert(missing.storage_key(), Ok(false));
        responses.insert(errored.storage_key(), Err("simulated I/O error".into()));

        let backend: Arc<dyn StorageBackend> = Arc::new(ScriptedExistsBackend {
            responses: std::sync::Mutex::new(responses),
        });

        let packs = vec![(present, 1), (missing, 1), (errored, 1)];
        let (checked, missing_count, issues) =
            parallel_pack_existence(&backend, &packs, 2, &CancelToken::new());

        // Definitively-resolved packs only: present + missing. The I/O-errored pack
        // must NOT be counted, otherwise progress overstates coverage.
        assert_eq!(checked, 2, "checked should exclude I/O-errored packs");
        assert_eq!(missing_count, 1, "exactly one Ok(false) pack");
        assert_eq!(
            issues.len(),
            2,
            "expected MissingPack + PackExistenceCheckFailed"
        );
        assert!(issues.iter().any(|i| matches!(
            i,
            IntegrityIssue::MissingPack { pack_id } if *pack_id == missing
        )));
        assert!(issues.iter().any(|i| matches!(
            i,
            IntegrityIssue::PackExistenceCheckFailed { pack_id, .. } if *pack_id == errored
        )));
    }

    /// A zstd frame restore rejects must also fail `check --verify-data`,
    /// even where libzstd's streaming decoder would accept it.
    #[test]
    fn verify_single_chunk_rejects_frame_restore_rejects() {
        use crate::repo::format::pack_object_with_context;
        use vykar_crypto::testing::DeterministicEngine;
        use vykar_types::chunk_id::ChunkHasher;
        use vykar_types::hash::HashAlgorithm;

        let frame = crate::compress::tests::ZSTD_EMPTY_COMPRESSED_BLOCK;
        let hasher = ChunkHasher::new(HashAlgorithm::Blake3, [0x5A; 32]);
        let engine = DeterministicEngine::new(hasher.clone());
        let chunk_id = ChunkId::compute(&hasher, &frame[frame.len() - 6..]);
        let pack_id = PackId::from_bytes([0x01; 32]);
        let mut raw =
            pack_object_with_context(ObjectType::ChunkData, chunk_id.as_bytes(), frame, &engine)
                .unwrap();
        let at = BlobLocation {
            chunk_id,
            pack_id,
            pack_offset: 0,
            stored_size: raw.len() as u32,
        };

        let mut issues = Vec::new();
        let verified = verify_single_chunk(&engine, at, &mut raw, &mut issues);
        assert_eq!(verified, 0);
        assert!(
            matches!(
                issues.as_slice(),
                [IntegrityIssue::CorruptChunk { detail, .. }] if detail.starts_with("decompress failed")
            ),
            "{issues:?}"
        );
    }
}
