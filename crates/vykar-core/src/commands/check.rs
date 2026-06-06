mod repair_apply;
mod repair_plan;
mod runner;
mod scan;
mod server_verify;
mod types;

pub use runner::{run, run_with_progress, run_with_repair};
pub use types::{
    CheckError, CheckProgressEvent, CheckResult, IntegrityIssue, ItemImpact, RepairAction,
    RepairMode, RepairPlan, RepairResult,
};

#[allow(unused_imports)]
pub(crate) use scan::verify_pack_full;
#[allow(unused_imports)]
pub(crate) use server_verify::{process_verify_response, try_server_verify};
#[allow(unused_imports)]
pub(crate) use types::ProcessedVerifyResult;
#[allow(unused_imports)]
pub(crate) use types::ServerVerifyOutcome;
