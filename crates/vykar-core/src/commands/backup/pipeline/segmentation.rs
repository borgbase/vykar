//! Large-file segmentation state machine.
//!
//! A file larger than `segment_size` is split by the walker into
//! `FileSegment` work entries that travel the pipeline as ordered messages.
//! This module tracks the in-flight accumulator across segments, validates
//! ordering / file-identity / segment-count invariants, and on a skip drops
//! the file's staged reuse and stats (its chunks stay, unreferenced).

use std::sync::Arc;

use crate::platform::fs;
use crate::repo::WriteSession;
use crate::repo::control::EventSink;
use crate::snapshot::SnapshotStats;
use crate::snapshot::item::Item;
use vykar_types::error::{Result, VykarError};

use super::super::BackupProgressEvent;
use super::super::commit::FileTxn;

/// Tracks in-progress accumulation of a segmented large file.
pub(super) struct LargeFileAccum {
    pub(super) item: Item,
    pub(super) abs_path: Arc<str>,
    /// Pre-read fstat captured by segment 0's worker. Each segment's worker
    /// already validates its own `pre_meta` against the walker's metadata,
    /// so by transitivity all segments agree — we use this value at
    /// finalization to populate the item fields.
    pub(super) metadata: fs::MetadataSummary,
    pub(super) next_expected_index: usize,
    pub(super) num_segments: usize,
    /// Open from segment 0 until the file is finalized or skipped.
    pub(super) txn: FileTxn,
}

/// Take the current segmented accumulator, drop the file's staged reuse,
/// restore stats, and emit a skip warning. Returns the number of remaining
/// segments the consumer loop should drain
/// (`num_segments - current_segment_index - 1`).
///
/// Nothing is removed from the index delta: the chunks earlier segments
/// stored stay as entries no snapshot references, which a compact sweeps.
///
/// Called by:
/// - the main loop when the worker reports `SegmentSkipped` at
///   `segment_index > 0` with an active accumulator,
/// - the main loop when a `WorkerErr` arrives soft during accumulation.
///
/// The file's transaction is open for the entire lifetime of the
/// accumulator, so aborting it here is unconditional.
pub(super) fn skip_large_file(
    ws: &mut WriteSession,
    stats: &mut SnapshotStats,
    large_file_accum: &mut Option<LargeFileAccum>,
    progress: Option<&dyn EventSink<BackupProgressEvent>>,
    current_segment_index: usize,
    reason: &str,
) -> usize {
    let Some(accum) = large_file_accum.take() else {
        return 0;
    };
    accum.txn.abort(ws, stats);
    stats.errors += 1;
    super::super::emit_post_commit_warning(
        progress,
        format!("skipping file '{}': {reason}", accum.abs_path),
    );
    accum.num_segments.saturating_sub(current_segment_index + 1)
}

/// Validate and update the segment accumulator state machine.
///
/// For segment 0: opens the file's [`FileTxn`] (staging its reuse and
/// snapshotting the stats byte counters) and installs the accumulator
/// (errors if one already exists). Ordering matters: the overlap and
/// presence checks run first, then the transaction is opened, then the
/// accumulator is installed. That way a nested-segmentation error cannot
/// leave a stray stage, and accumulator-present always implies
/// stage-open.
///
/// For continuations: validates ordering, file identity, and segment count.
/// Cross-segment metadata drift is not checked here — each segment's
/// worker already verifies `pre_meta` against the walker's metadata, which
/// is identical across segments.
#[allow(clippy::too_many_arguments)]
pub(super) fn validate_segment_accum(
    large_file_accum: &mut Option<LargeFileAccum>,
    item: Option<Item>,
    abs_path: Arc<str>,
    pre_meta: Option<fs::MetadataSummary>,
    segment_index: usize,
    num_segments: usize,
    dedup_before: u64,
    ws: &mut WriteSession,
    stats: &SnapshotStats,
) -> Result<()> {
    if segment_index == 0 {
        if large_file_accum.is_some() {
            return Err(VykarError::Other("nested large file segmentation".into()));
        }
        let item =
            item.ok_or_else(|| VykarError::Other("BUG: segment 0 must carry item".into()))?;
        let pre_meta = pre_meta
            .ok_or_else(|| VykarError::Other("BUG: segment 0 must carry pre_meta".into()))?;
        // Open the transaction BEFORE installing the accumulator so the
        // invariant `accum.is_some() ⇒ stage open` holds unconditionally.
        let txn = FileTxn::begin(ws, stats, dedup_before);
        *large_file_accum = Some(LargeFileAccum {
            item,
            abs_path,
            metadata: pre_meta,
            next_expected_index: 1,
            num_segments,
            txn,
        });
    } else {
        let accum = large_file_accum
            .as_mut()
            .ok_or_else(|| VykarError::Other("FileSegment without preceding segment 0".into()))?;
        if segment_index != accum.next_expected_index {
            return Err(VykarError::Other(format!(
                "segment index mismatch: expected {}, got {segment_index}",
                accum.next_expected_index
            )));
        }
        if abs_path != accum.abs_path {
            return Err(VykarError::Other("segment file identity mismatch".into()));
        }
        if num_segments != accum.num_segments {
            return Err(VykarError::Other(format!(
                "segment count mismatch: expected {}, got {num_segments}",
                accum.num_segments
            )));
        }
        accum.next_expected_index += 1;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::super::test_support::{test_item, test_metadata, test_stats};
    use super::*;

    #[test]
    fn segment_out_of_order() {
        let (_repo, mut ws) = crate::testutil::test_repo_with_session();
        let stats = test_stats();
        let mut accum: Option<LargeFileAccum> = None;
        let meta = test_metadata();

        // Feed segment 0.
        validate_segment_accum(
            &mut accum,
            Some(test_item("file_a")),
            "/tmp/file_a".into(),
            Some(meta),
            0,
            3,
            0,
            &mut ws,
            &stats,
        )
        .unwrap();

        // Skip segment 1, feed segment 2 → error.
        let err = validate_segment_accum(
            &mut accum,
            None,
            "/tmp/file_a".into(),
            None,
            2,
            3,
            0,
            &mut ws,
            &stats,
        )
        .unwrap_err();
        assert!(
            err.to_string().contains("segment index mismatch"),
            "expected 'segment index mismatch', got: {err}"
        );
        ws.drop_file_reuse();
    }

    #[test]
    fn segment_file_identity_mismatch() {
        let (_repo, mut ws) = crate::testutil::test_repo_with_session();
        let stats = test_stats();
        let mut accum: Option<LargeFileAccum> = None;
        let meta = test_metadata();

        // Feed segment 0 for file A.
        validate_segment_accum(
            &mut accum,
            Some(test_item("file_a")),
            "/tmp/file_a".into(),
            Some(meta),
            0,
            3,
            0,
            &mut ws,
            &stats,
        )
        .unwrap();

        // Feed segment 1 with different abs_path → error.
        let err = validate_segment_accum(
            &mut accum,
            None,
            "/tmp/file_b".into(),
            None,
            1,
            3,
            0,
            &mut ws,
            &stats,
        )
        .unwrap_err();
        assert!(
            err.to_string().contains("segment file identity mismatch"),
            "expected 'segment file identity mismatch', got: {err}"
        );
        ws.drop_file_reuse();
    }

    #[test]
    fn segment_nested_start() {
        let (_repo, mut ws) = crate::testutil::test_repo_with_session();
        let stats = test_stats();
        let mut accum: Option<LargeFileAccum> = None;
        let meta = test_metadata();

        // Feed segment 0 for file A (3 segments).
        validate_segment_accum(
            &mut accum,
            Some(test_item("file_a")),
            "/tmp/file_a".into(),
            Some(meta),
            0,
            3,
            0,
            &mut ws,
            &stats,
        )
        .unwrap();

        // Feed segment 0 for file B before file A completes → error.
        // The overlap check must fire BEFORE FileTxn::begin, so the stage
        // stays open from file A only (and the second begin does not panic).
        let err = validate_segment_accum(
            &mut accum,
            Some(test_item("file_b")),
            "/tmp/file_b".into(),
            Some(meta),
            0,
            2,
            0,
            &mut ws,
            &stats,
        )
        .unwrap_err();
        assert!(
            err.to_string().contains("nested large file segmentation"),
            "expected 'nested large file segmentation', got: {err}"
        );
        assert!(ws.file_reuse_staged(), "file A's stage is still open");
        ws.drop_file_reuse();
    }

    #[test]
    fn segment_without_start() {
        let (_repo, mut ws) = crate::testutil::test_repo_with_session();
        let stats = test_stats();
        let mut accum: Option<LargeFileAccum> = None;

        // Feed segment 1 with no prior segment 0 → error.
        let err = validate_segment_accum(
            &mut accum,
            None,
            "/tmp/file_a".into(),
            None,
            1,
            3,
            0,
            &mut ws,
            &stats,
        )
        .unwrap_err();
        assert!(
            err.to_string()
                .contains("FileSegment without preceding segment 0"),
            "expected 'FileSegment without preceding segment 0', got: {err}"
        );
    }

    #[test]
    fn incomplete_accumulator_check() {
        let (_repo, mut ws) = crate::testutil::test_repo_with_session();
        let stats = test_stats();
        let mut accum: Option<LargeFileAccum> = None;
        let meta = test_metadata();

        // Feed segment 0 and 1 of a 3-segment file, but not segment 2.
        validate_segment_accum(
            &mut accum,
            Some(test_item("file_a")),
            "/tmp/file_a".into(),
            Some(meta),
            0,
            3,
            0,
            &mut ws,
            &stats,
        )
        .unwrap();
        validate_segment_accum(
            &mut accum,
            None,
            "/tmp/file_a".into(),
            None,
            1,
            3,
            0,
            &mut ws,
            &stats,
        )
        .unwrap();

        // Simulate the post-loop check from run_parallel_pipeline.
        assert!(accum.is_some(), "accum should still be active");
        let a = accum.as_ref().unwrap();
        assert_eq!(a.next_expected_index, 2);
        assert_eq!(a.num_segments, 3);
        // The real pipeline generates this error:
        let err_msg = format!(
            "incomplete segmented file '{}': received {}/{} segments",
            a.abs_path, a.next_expected_index, a.num_segments,
        );
        assert!(
            err_msg.contains("incomplete segmented file"),
            "expected incomplete message, got: {err_msg}"
        );
        ws.drop_file_reuse();
    }

    #[test]
    fn segment_count_mismatch() {
        let (_repo, mut ws) = crate::testutil::test_repo_with_session();
        let stats = test_stats();
        let mut accum: Option<LargeFileAccum> = None;
        let meta = test_metadata();

        // Feed segment 0 with num_segments=3.
        validate_segment_accum(
            &mut accum,
            Some(test_item("file_a")),
            "/tmp/file_a".into(),
            Some(meta),
            0,
            3,
            0,
            &mut ws,
            &stats,
        )
        .unwrap();

        // Feed segment 1 with different num_segments → error.
        let err = validate_segment_accum(
            &mut accum,
            None,
            "/tmp/file_a".into(),
            None,
            1,
            5,
            0,
            &mut ws,
            &stats,
        )
        .unwrap_err();
        assert!(
            err.to_string().contains("segment count mismatch"),
            "expected 'segment count mismatch', got: {err}"
        );
        ws.drop_file_reuse();
    }

    #[test]
    fn segment_happy_path() {
        let (_repo, mut ws) = crate::testutil::test_repo_with_session();
        let stats = test_stats();
        let mut accum: Option<LargeFileAccum> = None;
        let meta = test_metadata();

        // Feed all 3 segments in order.
        for i in 0..3 {
            let item = if i == 0 {
                Some(test_item("file_a"))
            } else {
                None
            };
            let pre_meta = (i == 0).then_some(meta);
            validate_segment_accum(
                &mut accum,
                item,
                "/tmp/file_a".into(),
                pre_meta,
                i,
                3,
                0,
                &mut ws,
                &stats,
            )
            .unwrap();
        }

        // Accumulator should be present with next_expected_index == 3.
        let a = accum.as_ref().unwrap();
        assert_eq!(a.next_expected_index, 3);
        assert_eq!(a.num_segments, 3);
        assert_eq!(a.item.path, "file_a");
        assert_eq!(&*a.abs_path, "/tmp/file_a");
        assert!(
            ws.file_reuse_staged(),
            "the stage is open until the file ends"
        );
        ws.drop_file_reuse();
    }

    /// Drives `skip_large_file` directly (Mechanism B): an accumulator
    /// midway through a 4-segment file is dropped, byte counters are
    /// restored, and the drain count for the remaining segments is reported
    /// correctly.
    #[test]
    fn skip_large_file_drains_and_restores_stats() {
        let (_repo, mut ws) = crate::testutil::test_repo_with_session();

        // Open the transaction just like validate_segment_accum would for
        // segment 0, over byte counters of all zeros; then dirty them so we
        // can verify they get restored.
        let txn = FileTxn::begin(&mut ws, &SnapshotStats::default(), 0);
        assert!(ws.file_reuse_staged());
        let mut stats = SnapshotStats {
            deduplicated_size: 12345,
            original_size: 99999,
            compressed_size: 4242,
            errors: 0,
            ..Default::default()
        };

        let accum = LargeFileAccum {
            item: test_item("big"),
            abs_path: Arc::from("/tmp/big"),
            metadata: test_metadata(),
            next_expected_index: 2,
            num_segments: 4,
            txn,
        };
        let mut accum = Some(accum);
        let drain = skip_large_file(&mut ws, &mut stats, &mut accum, None, 2, "drift");

        // 4 segments, skipped at index 2 → segments 3 remain to drain.
        assert_eq!(drain, 1, "4 segs, skipped at idx 2 → 1 remaining");
        assert!(accum.is_none(), "accumulator cleared");
        assert!(!ws.file_reuse_staged(), "the stage must be dropped");
        assert_eq!(stats.errors, 1);
        assert_eq!(
            stats.deduplicated_size, 0,
            "deduplicated_size restored from snapshot"
        );
        assert_eq!(
            stats.original_size, 0,
            "original_size restored from snapshot"
        );
        assert_eq!(
            stats.compressed_size, 0,
            "compressed_size restored from snapshot"
        );
    }

    /// Idempotency: calling `skip_large_file` with no accumulator is a
    /// no-op — drain count of zero, stats untouched.
    #[test]
    fn skip_large_file_no_accum_is_noop() {
        let (_repo, mut ws) = crate::testutil::test_repo_with_session();
        let mut stats = SnapshotStats {
            deduplicated_size: 7,
            ..Default::default()
        };
        let mut accum: Option<LargeFileAccum> = None;
        let drain = skip_large_file(&mut ws, &mut stats, &mut accum, None, 0, "ignored");

        assert_eq!(drain, 0);
        assert_eq!(stats.errors, 0, "no error counted when accum was empty");
        assert_eq!(
            stats.deduplicated_size, 7,
            "stats untouched when accum was empty"
        );
    }
}
