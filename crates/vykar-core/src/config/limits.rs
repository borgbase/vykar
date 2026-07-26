use serde::{Deserialize, Serialize};

use vykar_types::error::{Result, VykarError};

/// Default number of parallel backend connections.
const DEFAULT_CONNECTIONS: usize = 2;
/// Auto mode cap for backup worker threads (`threads: 0`).
const AUTO_THREADS_MAX: usize = 12;

/// Minimum value for `limits.threads` (0 = auto).
pub const THREADS_MIN: usize = 0;
/// Maximum value for `limits.threads`.
pub const THREADS_MAX: usize = 128;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResourceLimitsConfig {
    /// Parallel backend operations (SFTP pool, upload concurrency, restore threads).
    /// Range: 1–16. Default: 2.
    #[serde(default = "default_connections")]
    pub connections: usize,
    /// CPU worker threads for backup transforms.
    /// 0 = auto: local repos use ceil(cores/2) clamped to [2, 4]; remote repos use min(cores, 12). 1 = sequential.
    #[serde(default)]
    pub threads: usize,
    /// Unix process niceness target (-20..19). 0 = unchanged.
    #[serde(default)]
    pub nice: i32,
    /// Upload bandwidth cap in MiB/s. 0 = unlimited.
    #[serde(default)]
    pub upload_mib_per_sec: u64,
    /// Download bandwidth cap in MiB/s. 0 = unlimited.
    #[serde(default)]
    pub download_mib_per_sec: u64,
}

fn default_connections() -> usize {
    DEFAULT_CONNECTIONS
}

impl Default for ResourceLimitsConfig {
    fn default() -> Self {
        Self {
            connections: DEFAULT_CONNECTIONS,
            threads: 0,
            nice: 0,
            upload_mib_per_sec: 0,
            download_mib_per_sec: 0,
        }
    }
}

impl ResourceLimitsConfig {
    pub fn validate(&self) -> Result<()> {
        if !(1..=16).contains(&self.connections) {
            return Err(VykarError::Config(format!(
                "limits.connections must be in [1, 16], got {}",
                self.connections
            )));
        }
        if self.threads > THREADS_MAX {
            return Err(VykarError::Config(format!(
                "limits.threads must be in [{THREADS_MIN}, {THREADS_MAX}], got {}",
                self.threads
            )));
        }
        if !(-20..=19).contains(&self.nice) {
            return Err(VykarError::Config(format!(
                "limits.nice must be in [-20, 19], got {}",
                self.nice
            )));
        }
        Ok(())
    }

    /// Auto-resolved backup worker count, accounting for backend locality.
    /// Local repos are I/O-bound and benefit from fewer workers to reduce
    /// channel contention. Remote repos keep higher parallelism for
    /// overlapping compress/encrypt with network I/O.
    pub fn effective_backup_threads(&self, is_local: bool) -> usize {
        if self.threads > 0 {
            return self.threads;
        }
        auto_backup_threads(
            std::thread::available_parallelism().map_or(2, |n| n.get()),
            is_local,
        )
    }

    /// Pipeline depth derived from connections.
    pub fn effective_pipeline_depth(&self) -> usize {
        self.connections.max(2)
    }

    /// Pipeline buffer size in bytes, derived from worker count.
    pub fn pipeline_buffer_for_workers(&self, workers: usize) -> usize {
        workers
            .saturating_mul(64 * 1024 * 1024)
            .clamp(64 * 1024 * 1024, 1024 * 1024 * 1024)
    }

    /// Segment size in bytes for large-file pipeline splitting (fixed 64 MiB).
    pub fn segment_size_bytes(&self) -> usize {
        64 * 1024 * 1024
    }

    /// Transform batch size in bytes (fixed 32 MiB).
    pub fn transform_batch_bytes(&self) -> usize {
        32 * 1024 * 1024
    }

    /// Max pending chunk actions (fixed 8192).
    pub fn max_pending_actions(&self) -> usize {
        8192
    }

    /// Upload concurrency = connections.
    pub fn upload_concurrency(&self) -> usize {
        self.connections
    }

    /// Concurrency for listing/existence checks.
    pub fn listing_concurrency(&self, is_remote: bool) -> usize {
        if is_remote {
            (self.connections * 3).min(24)
        } else {
            self.connections.min(8)
        }
    }

    /// Restore reader thread count = connections.
    pub fn restore_concurrency(&self) -> usize {
        self.connections
    }

    /// Verify-data concurrency: capped at 4 (each thread holds ~128 MiB).
    pub fn verify_data_concurrency(&self) -> usize {
        self.connections.min(4)
    }
}

/// Pure helper for testability (no `available_parallelism()` call).
fn auto_backup_threads(cores: usize, is_local: bool) -> usize {
    if is_local {
        if cores == 1 {
            1
        } else {
            cores.div_ceil(2).clamp(2, 4)
        }
    } else {
        cores.min(AUTO_THREADS_MAX)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn auto_backup_threads_local() {
        assert_eq!(auto_backup_threads(1, true), 1);
        assert_eq!(auto_backup_threads(2, true), 2);
        assert_eq!(auto_backup_threads(4, true), 2);
        assert_eq!(auto_backup_threads(8, true), 4);
        assert_eq!(auto_backup_threads(16, true), 4);
    }

    #[test]
    fn auto_backup_threads_remote() {
        assert_eq!(auto_backup_threads(1, false), 1);
        assert_eq!(auto_backup_threads(2, false), 2);
        assert_eq!(auto_backup_threads(8, false), 8);
        assert_eq!(auto_backup_threads(24, false), 12);
    }

    #[test]
    fn explicit_threads_bypass_auto() {
        let cfg = ResourceLimitsConfig {
            threads: 6,
            ..Default::default()
        };
        assert_eq!(cfg.effective_backup_threads(true), 6);
        assert_eq!(cfg.effective_backup_threads(false), 6);
    }

    #[test]
    fn validate_threads_accepts_bounds() {
        let auto = ResourceLimitsConfig {
            threads: 0,
            ..Default::default()
        };
        assert!(auto.validate().is_ok());

        let max = ResourceLimitsConfig {
            threads: THREADS_MAX,
            ..Default::default()
        };
        assert!(max.validate().is_ok());
    }

    #[test]
    fn validate_threads_rejects_over_max() {
        let cfg = ResourceLimitsConfig {
            threads: THREADS_MAX + 1,
            ..Default::default()
        };
        let err = cfg.validate().unwrap_err().to_string();
        assert!(
            err.contains("limits.threads"),
            "error should mention threads: {err}"
        );
    }

    #[test]
    fn pipeline_buffer_sizing() {
        let cfg = ResourceLimitsConfig::default();
        assert_eq!(cfg.pipeline_buffer_for_workers(4), 256 * 1024 * 1024);
        assert_eq!(cfg.pipeline_buffer_for_workers(8), 512 * 1024 * 1024);
        assert_eq!(cfg.pipeline_buffer_for_workers(1), 64 * 1024 * 1024);
        assert_eq!(cfg.pipeline_buffer_for_workers(20), 1024 * 1024 * 1024);
    }
}
