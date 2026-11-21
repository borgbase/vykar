//! The store's op log: one record per logged operation or release.

use super::fault::{ClientId, FaultAction, OpKind, ParkedId};

#[derive(Clone, Debug)]
pub struct OpRecord {
    pub step: u64,
    pub client: ClientId,
    pub run: Option<usize>,
    /// Thread name, else the `ThreadId` debug form.
    pub thread: String,
    pub op: OpKind,
    /// `BatchDelete`: the first key, or "" when empty.
    pub key: String,
    /// `BatchDelete` only.
    pub keys: Vec<String>,
    /// Keys a delete named that were already absent when it applied.
    pub absent: Vec<String>,
    /// Bytes written, returned, or requested (range).
    pub len: Option<u64>,
    pub outcome: OpOutcome,
}

impl OpRecord {
    /// The op changed the store as issued (directly, or by a later release).
    pub fn applied(&self) -> bool {
        matches!(
            self.outcome,
            OpOutcome::Ok
                | OpOutcome::Injected(FaultAction::FailApplied)
                | OpOutcome::Parked(_, FaultAction::Duplicate)
                | OpOutcome::Released(_)
        )
    }

    /// Keys this record's delete removed: none unless it is an applied
    /// delete, never one that was already absent, and each key once.
    pub fn deleted_keys(&self) -> Vec<&str> {
        if !self.op.is_delete() || !self.applied() {
            return Vec::new();
        }
        let mut out: Vec<&str> = Vec::new();
        for k in self.all_keys() {
            if !self.absent.iter().any(|a| a == k) && !out.contains(&k) {
                out.push(k);
            }
        }
        out
    }

    /// Every key the record names.
    pub fn all_keys(&self) -> Vec<&str> {
        if self.op == OpKind::BatchDelete {
            self.keys.iter().map(String::as_str).collect()
        } else {
            vec![self.key.as_str()]
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum OpOutcome {
    Ok,
    NotFound,
    /// Append-only overwrite, or a write on the observer handle.
    Refused,
    /// Returned the backend's own error (e.g. short range read).
    Error,
    /// Fail / FailApplied / Crash fired on this op.
    Injected(FaultAction),
    /// Delay or Duplicate fired; effect parked under this id.
    Parked(ParkedId, FaultAction),
    /// Op of a crashed client: not applied, `Err`.
    Crashed,
    /// A parked effect applied by `release` (logged under its original client).
    Released(ParkedId),
    /// A released effect the store refused (append-only overwrite).
    ReleasedRefused(ParkedId),
}

pub(crate) fn thread_label() -> String {
    let t = std::thread::current();
    match t.name() {
        Some(n) => n.to_string(),
        None => format!("{:?}", t.id()),
    }
}
