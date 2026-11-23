mod repair_apply;
mod repair_plan;
mod runner;
mod scan;
mod server_verify;
pub(crate) mod state;
mod types;

#[cfg(test)]
pub(crate) use runner::{PINNED_SAMPLE, SampleOf};
#[cfg(any(test, feature = "sim"))]
pub(crate) use runner::{repair_open_options, run_on_repo, run_with_repair_on_repo};
pub use runner::{run, run_with_progress, run_with_repair};
pub use types::{
    CheckError, CheckProgressEvent, CheckResult, IndexLoss, IndexRecovery, IntegrityIssue,
    ItemImpact, RebuildCounts, RepairAction, RepairIndexLoad, RepairMode, RepairPlan, RepairResult,
    UncoveredPack,
};

// Re-exported for the crate-level unit tests in `tests::check_command`, which
// reach these internals through `commands::check`.
#[cfg(test)]
pub(crate) use scan::{scan_for_test, verify_pack_full};
#[cfg(test)]
pub(crate) use server_verify::{process_verify_response, try_server_verify};
#[cfg(test)]
pub(crate) use types::ServerVerifyOutcome;
