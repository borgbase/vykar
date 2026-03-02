use serde::{Deserialize, Serialize};

use vykar_types::error::{Result, VykarError};

/// Default maximum number of in-flight background pack uploads.
pub const DEFAULT_UPLOAD_CONCURRENCY: usize = 2;

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ResourceLimitsConfig {
    #[serde(default)]
    pub cpu: CpuLimitsConfig,
    #[serde(default)]
    pub io: IoLimitsConfig,
    #[serde(default)]
    pub network: NetworkLimitsConfig,
}

impl ResourceLimitsConfig {
    pub fn validate(&self) -> Result<()> {
        self.cpu.validate()
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct CpuLimitsConfig {
    /// Max CPU worker threads for backup transforms (0 = use all cores, 1 = sequential).
    #[serde(default)]
    pub max_threads: usize,
    /// Unix process niceness target (-20..19). 0 = unchanged.
    #[serde(default)]
    pub nice: i32,
    /// Max in-flight background pack uploads (default: 2, range: 1-16).
    #[serde(default)]
    pub max_upload_concurrency: Option<usize>,
    /// Batch size in MiB for transform flushes (default: 32, range: 4-256).
    #[serde(default)]
    pub transform_batch_mib: Option<usize>,
    /// Max pending chunk actions before flush (default: 8192, range: 64-65536).
    #[serde(default)]
    pub transform_batch_chunks: Option<usize>,
    /// Depth of the pipeline channel buffer (default: 4, 0 = disable pipeline).
    #[serde(default)]
    pub pipeline_depth: Option<usize>,
    /// Max in-flight chunk bytes in the pipeline channel (default: 256 MiB, range: 32-1024).
    #[serde(default)]
    pub pipeline_buffer_mib: Option<usize>,
    /// Segment size in MiB for large-file pipeline splitting (default: 64, range: 16-256).
    /// Files larger than this are split into segments for parallel worker processing.
    /// Changing this value may reduce dedup effectiveness against prior snapshots.
    #[serde(default)]
    pub segment_size_mib: Option<usize>,
}

/// Validate that an `Option<T>` field, if present, falls within an inclusive range.
macro_rules! validate_range {
    ($field:expr, $name:expr, $min:expr, $max:expr) => {
        if let Some(n) = $field {
            if !($min..=$max).contains(&n) {
                return Err(VykarError::Config(format!(
                    "{} must be in [{}, {}], got {n}",
                    $name, $min, $max
                )));
            }
        }
    };
}

impl CpuLimitsConfig {
    fn validate(&self) -> Result<()> {
        if !(-20..=19).contains(&self.nice) {
            return Err(VykarError::Config(format!(
                "limits.cpu.nice must be in [-20, 19], got {}",
                self.nice
            )));
        }
        validate_range!(
            self.max_upload_concurrency,
            "limits.cpu.max_upload_concurrency",
            1,
            16
        );
        validate_range!(
            self.transform_batch_mib,
            "limits.cpu.transform_batch_mib",
            4,
            256
        );
        validate_range!(
            self.transform_batch_chunks,
            "limits.cpu.transform_batch_chunks",
            64,
            65536
        );
        validate_range!(self.pipeline_depth, "limits.cpu.pipeline_depth", 0, 64);
        validate_range!(
            self.pipeline_buffer_mib,
            "limits.cpu.pipeline_buffer_mib",
            32,
            1024
        );
        validate_range!(
            self.segment_size_mib,
            "limits.cpu.segment_size_mib",
            16,
            256
        );
        Ok(())
    }

    /// Effective transform batch size in bytes.
    pub fn transform_batch_bytes(&self) -> usize {
        self.transform_batch_mib.unwrap_or(32) * 1024 * 1024
    }

    /// Effective max pending chunk actions.
    pub fn max_pending_actions(&self) -> usize {
        self.transform_batch_chunks.unwrap_or(8192)
    }

    /// Effective upload concurrency limit.
    pub fn upload_concurrency(&self) -> usize {
        self.max_upload_concurrency
            .unwrap_or(DEFAULT_UPLOAD_CONCURRENCY)
    }

    /// Effective pipeline depth (0 = disabled).
    pub fn effective_pipeline_depth(&self) -> usize {
        self.pipeline_depth.unwrap_or(4)
    }

    /// Effective pipeline buffer size in bytes.
    pub fn pipeline_buffer_bytes(&self) -> usize {
        self.pipeline_buffer_mib.unwrap_or(256) * 1024 * 1024
    }

    /// Effective segment size in bytes for large-file pipeline splitting.
    pub fn segment_size_bytes(&self) -> usize {
        self.segment_size_mib.unwrap_or(64) * 1024 * 1024
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct IoLimitsConfig {
    /// Source-file read limit in MiB/s (0 = unlimited).
    #[serde(default)]
    pub read_mib_per_sec: u64,
    /// Local repository write limit in MiB/s (0 = unlimited).
    #[serde(default)]
    pub write_mib_per_sec: u64,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct NetworkLimitsConfig {
    /// Remote backend read limit in MiB/s (0 = unlimited).
    #[serde(default)]
    pub read_mib_per_sec: u64,
    /// Remote backend write limit in MiB/s (0 = unlimited).
    #[serde(default)]
    pub write_mib_per_sec: u64,
}
