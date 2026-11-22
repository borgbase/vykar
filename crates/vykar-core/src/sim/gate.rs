//! Pause points: a gate stops one client operation before it takes effect,
//! until the test resumes it (08 step 3, "scripted pause points").

use super::fault::{ClientId, KeyMatch, OpKind, OpMatch};

/// Stop `client`'s `nth` (1-based) operation matching `op` and `key`,
/// counted from when the gate is added. Fires once.
///
/// A held operation has taken no step, is not logged and has no effect
/// until it is resumed; crash state and fault rules are judged when it
/// proceeds.
#[derive(Clone, Debug)]
pub struct GateRule {
    pub client: ClientId,
    pub op: OpMatch,
    /// Matches if any of the operation's keys matches (every key of a
    /// `BatchDelete`).
    pub key: KeyMatch,
    pub nth: u32,
}

impl GateRule {
    /// The first matching operation.
    pub fn new(client: ClientId, op: OpMatch, key: KeyMatch) -> Self {
        Self {
            client,
            op,
            key,
            nth: 1,
        }
    }

    pub fn nth(mut self, n: u32) -> Self {
        self.nth = n;
        self
    }
}

/// A gate of a [`SimStore`](super::store::SimStore).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct GateId(pub(crate) u32);

/// The operation a gate holds.
#[derive(Clone, Debug)]
pub struct Paused {
    pub gate: GateId,
    pub client: ClientId,
    /// The client's run when the operation was issued.
    pub run: Option<usize>,
    /// `thread_label()` of the blocked thread.
    pub thread: String,
    pub op: OpKind,
    /// `BatchDelete`: the first key.
    pub key: String,
    /// The store step when it paused; the op itself takes a later step.
    pub at_step: u64,
}

#[derive(Clone, Debug)]
pub(crate) enum GatePhase {
    /// Counting matching operations.
    Armed { count: u32 },
    /// Holding an operation.
    Paused(Paused),
    /// Resumed, or disarmed before it fired.
    Open,
    /// Reached `nth` on an operation another gate already held.
    Missed,
}

pub(crate) struct GateState {
    pub(crate) id: GateId,
    pub(crate) rule: GateRule,
    pub(crate) phase: GatePhase,
}
