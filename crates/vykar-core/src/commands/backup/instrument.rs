//! Diagnostic timings of one backup run, logged at debug on target `vykar::stats`.

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use crate::repo::write_session::{WriteMetricsSnapshot, duration_nanos};
use crate::snapshot::SnapshotStats;

/// Tracing target of the summary events (`VYKAR_LOG=warn,vykar::stats=debug`).
pub(crate) const STATS_TARGET: &str = "vykar::stats";

/// A duration summed by several threads (Relaxed; read after they are joined).
#[derive(Debug, Default)]
pub(super) struct StageClock(AtomicU64);

impl StageClock {
    pub(super) fn add(&self, d: Duration) {
        self.0.fetch_add(duration_nanos(d), Ordering::Relaxed);
    }
    pub(super) fn total(&self) -> Duration {
        Duration::from_nanos(self.0.load(Ordering::Relaxed))
    }
}

/// Shared by the walk and worker threads of one pipeline run.
#[derive(Debug, Default)]
pub(super) struct PipelineClocks {
    pub(super) worker_busy: StageClock,
    /// Part of `worker_busy` spent hashing, compressing and encrypting chunks;
    /// the rest is read and cut.
    pub(super) worker_transform: StageClock,
    pub(super) worker_recv_wait: StageClock,
    pub(super) worker_send_wait: StageClock,
    pub(super) walk_budget_wait: StageClock,
    pub(super) walk_send_wait: StageClock,
}

#[derive(Debug, Clone, Copy, Default)]
pub(crate) struct PipelineTimings {
    pub(crate) workers: usize,
    /// Wall time of the consumer loop.
    pub(crate) wall: Duration,
    /// `wall - consumer_recv_wait`.
    pub(crate) consumer_busy: Duration,
    pub(crate) consumer_recv_wait: Duration,
    /// Upload-cap wait inside the consumer loop (part of `consumer_busy`);
    /// the session-wide total also covers command dumps and the final flush.
    pub(crate) consumer_cap_wait: Duration,
    /// Sums over all workers.
    pub(crate) worker_busy: Duration,
    pub(crate) worker_transform: Duration,
    pub(crate) worker_recv_wait: Duration,
    pub(crate) worker_send_wait: Duration,
    pub(crate) walk_budget_wait: Duration,
    pub(crate) walk_send_wait: Duration,
}

#[derive(Debug, Clone, Copy, Default)]
pub(crate) struct BackupInstrumentation {
    pub(crate) threads: usize,
    pub(crate) phase1: Duration,
    /// Command dumps plus the walk.
    pub(crate) walk: Duration,
    /// `flush_packs_journal_in_background` at the end of phase 1.
    pub(crate) final_flush: Duration,
    pub(crate) pipeline: Option<PipelineTimings>,
    pub(crate) write: WriteMetricsSnapshot,
}

pub(super) fn ms(d: Duration) -> u64 {
    u64::try_from(d.as_millis()).unwrap_or(u64::MAX)
}

impl PipelineTimings {
    pub(super) fn emit(&self) {
        tracing::debug!(
            target: STATS_TARGET,
            workers = self.workers,
            pipeline_wall_ms = ms(self.wall),
            consumer_busy_ms = ms(self.consumer_busy),
            consumer_recv_wait_ms = ms(self.consumer_recv_wait),
            consumer_cap_wait_ms = ms(self.consumer_cap_wait),
            worker_busy_ms = ms(self.worker_busy),
            worker_transform_ms = ms(self.worker_transform),
            worker_recv_wait_ms = ms(self.worker_recv_wait),
            worker_send_wait_ms = ms(self.worker_send_wait),
            walk_budget_wait_ms = ms(self.walk_budget_wait),
            walk_send_wait_ms = ms(self.walk_send_wait),
            "backup pipeline stats"
        );
    }
}

impl BackupInstrumentation {
    pub(super) fn emit(&self, stats: &SnapshotStats) {
        let w = &self.write;
        tracing::debug!(
            target: STATS_TARGET,
            threads = self.threads,
            pipeline = self.pipeline.is_some(),
            phase1_ms = ms(self.phase1),
            walk_ms = ms(self.walk),
            final_flush_ms = ms(self.final_flush),
            nfiles = stats.nfiles,
            original_size = stats.original_size,
            compressed_size = stats.compressed_size,
            deduplicated_size = stats.deduplicated_size,
            data_packs = w.data_packs,
            tree_packs = w.tree_packs,
            data_pack_bytes = w.data_pack_bytes,
            tree_pack_bytes = w.tree_pack_bytes,
            seal_ms = ms(w.seal),
            upload_cap_wait_ms = ms(w.upload_cap_wait),
            final_upload_wait_ms = ms(w.final_upload_wait),
            upload_put_ms = ms(w.upload_put),
            upload_seal_ms = ms(w.upload_seal),
            "backup write stats"
        );
    }
}
