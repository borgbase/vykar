//! Protocol simulation harness: many logical clients over one in-memory store.
//!
//! - `store`: `SimStore`, the shared deterministic backend with an op log and
//!   faults (`Fail`, `FailApplied`, `Delay`, `Duplicate`, `Crash`).
//! - `gate`: pause points, which hold one client operation before it takes
//!   effect until the test resumes it.
//! - `runner`: `Sim`, which runs real client commands, on the calling thread
//!   or on their own thread, each client with its own cache directory, and
//!   records a `History`.
//! - `env`: virtual time (`SimTime`, `SimClock`) and per-client host identity
//!   (`SimHosts`), from which each client gets its own `HostEnv`.
//! - `checker`: `check_invariants`, which judges the store and the history
//!   against the protocol invariants.
//! - `hazards`: scenarios of known protocol hazards and their regressions;
//!   `interleavings` holds the ones that need two clients in flight at once.
//! - `driver`: the deterministic driver over the steppable commit machines
//!   (08 step 4): cases, choice sequences, traces and damage signatures.
//! - `explore`: bounded exhaustive search over the driver's decisions, with
//!   at most K preemptions (08 step 5).
//!
//! Test support, not API: a panic is a test failure. Compiled for this
//! crate's tests and with the `sim` feature; never enable it in a product
//! build.

// `--all-features` lints this as library code; it is test support.
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::panic_in_result_fn,
    clippy::indexing_slicing
)]

pub mod checker;
pub mod driver;
pub mod env;
pub mod explore;
pub mod fault;
pub mod gate;
pub mod hazards;
pub mod history;
pub mod interleavings;
pub mod log;
pub mod runner;
pub mod store;

pub use checker::{Invariant, Report, Violation, ViolationKind, check_invariants};
pub use driver::{
    Action, Case, Command, FaultKind, FaultSpec, HostPlan, KeyClass, LABEL_LOCKED, Outcome, Policy,
    Signature, Template, Trace, TraceEntry, case_from_trace, free_actions, run_case,
};
pub use env::{INIT_PID_NS, SimClock, SimHosts, SimMachine, SimTime, TimeMode, Waiter};
pub use explore::{Bound, Control, Exploration, Point, Search, dfs, explore, explore_with};
pub use fault::{
    ClientId, FaultAction, FaultRule, INJECTED_MARKER, KeyMatch, OpKind, OpMatch, ParkedEffect,
    ParkedId, RuleId,
};
pub use gate::{GateId, GateRule, Paused};
pub use history::{AllowedError, ClientOp, ErrorClass, History, RunOutcome, RunRecord};
pub use log::{OpOutcome, OpRecord};
pub use runner::{Call, CustomFn, Sim, SourceTree, Spawned};
pub use store::{SimClient, SimConfig, SimStore, StoredObject};
