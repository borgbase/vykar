//! Protocol simulation harness: many logical clients over one in-memory store.
//!
//! - `store`: `SimStore`, the shared deterministic backend with an op log and
//!   faults (`Fail`, `FailApplied`, `Delay`, `Duplicate`, `Crash`).
//! - `runner`: `Sim`, which runs real client commands one after another, each
//!   client with its own cache directory, and records a `History`.
//! - `env`: virtual time (`SimTime`, `SimClock`) and per-client host identity
//!   (`SimHosts`), from which each client gets its own `HostEnv`.
//! - `checker`: `check_invariants`, which judges the store and the history
//!   against the protocol invariants.
//! - `hazards`: scenarios of known protocol hazards and their regressions.
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
pub mod env;
pub mod fault;
pub mod hazards;
pub mod history;
pub mod log;
pub mod runner;
pub mod store;

pub use checker::{Invariant, Report, Violation, ViolationKind, check_invariants};
pub use env::{SimClock, SimHosts, SimTime, TimeMode, Waiter};
pub use fault::{
    ClientId, FaultAction, FaultRule, INJECTED_MARKER, KeyMatch, OpKind, OpMatch, ParkedEffect,
    ParkedId, RuleId,
};
pub use history::{AllowedError, ClientOp, ErrorClass, History, RunOutcome, RunRecord};
pub use log::{OpOutcome, OpRecord};
pub use runner::{Sim, SourceTree};
pub use store::{SimClient, SimConfig, SimStore, StoredObject};
