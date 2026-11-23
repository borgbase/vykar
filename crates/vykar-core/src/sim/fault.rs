//! Fault vocabulary of [`SimStore`](super::store::SimStore): which operations
//! a rule matches and what happens to the matched one.

/// A logical client of the shared store. Ids 0 and 1 are reserved for the
/// observer and tamper handles.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct ClientId(pub(crate) u16);

/// One storage operation as the store sees it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum OpKind {
    Get,
    GetRange,
    Exists,
    Size,
    List,
    ListSized,
    ListMeta,
    Put,
    PutOwned,
    PutPack,
    StagedCommit,
    Delete,
    BatchDelete,
    CreateDir,
}

impl OpKind {
    pub fn is_write(self) -> bool {
        matches!(
            self,
            OpKind::Put | OpKind::PutOwned | OpKind::PutPack | OpKind::StagedCommit
        )
    }

    pub fn is_delete(self) -> bool {
        matches!(self, OpKind::Delete | OpKind::BatchDelete)
    }

    pub fn is_mutation(self) -> bool {
        self.is_write() || self.is_delete()
    }
}

#[derive(Clone, Copy, Debug)]
pub enum OpMatch {
    Kind(OpKind),
    AnyWrite,
    AnyDelete,
    AnyMutation,
    /// Every operation that is not a mutation (`CreateDir` included).
    AnyRead,
    Any,
}

impl OpMatch {
    pub fn matches(self, op: OpKind) -> bool {
        match self {
            OpMatch::Kind(k) => k == op,
            OpMatch::AnyWrite => op.is_write(),
            OpMatch::AnyDelete => op.is_delete(),
            OpMatch::AnyMutation => op.is_mutation(),
            OpMatch::AnyRead => !op.is_mutation(),
            OpMatch::Any => true,
        }
    }

    /// Whether this can match an operation without an effect to apply.
    pub(crate) fn can_match_non_mutation(self) -> bool {
        match self {
            OpMatch::Kind(k) => !k.is_mutation(),
            OpMatch::AnyWrite | OpMatch::AnyDelete | OpMatch::AnyMutation => false,
            OpMatch::AnyRead | OpMatch::Any => true,
        }
    }
}

#[derive(Clone, Debug)]
pub enum KeyMatch {
    Any,
    Exact(String),
    Prefix(String),
}

impl KeyMatch {
    pub fn matches(&self, key: &str) -> bool {
        match self {
            KeyMatch::Any => true,
            KeyMatch::Exact(k) => k == key,
            KeyMatch::Prefix(p) => key.starts_with(p.as_str()),
        }
    }
}

/// What a fired rule does to its operation.
///
/// - `Fail`: not applied, `Err`.
/// - `FailApplied`: applied, `Err` (the "outcome unknown" case of `put`).
/// - `Delay`: not applied now, `Err`; the effect is parked until released.
/// - `Duplicate`: applied, `Ok`; a copy is parked and lands again on release.
/// - `Crash`: not applied, `Err`; every later op of the client fails too.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FaultAction {
    Fail,
    FailApplied,
    Delay,
    Duplicate,
    Crash,
}

impl FaultAction {
    /// The action needs an effect to apply or park.
    pub(crate) fn needs_effect(self) -> bool {
        matches!(
            self,
            FaultAction::FailApplied | FaultAction::Delay | FaultAction::Duplicate
        )
    }
}

#[derive(Clone, Debug)]
pub struct FaultRule {
    pub client: Option<ClientId>,
    pub op: OpMatch,
    pub key: KeyMatch,
    /// Fires on the nth matching operation (1-based), once.
    pub nth: u32,
    pub action: FaultAction,
}

impl FaultRule {
    pub fn new(op: OpMatch, key: KeyMatch, action: FaultAction) -> Self {
        Self {
            client: None,
            op,
            key,
            nth: 1,
            action,
        }
    }

    /// Any write of exactly `key`.
    pub fn put(key: &str, action: FaultAction) -> Self {
        Self::new(OpMatch::AnyWrite, KeyMatch::Exact(key.to_string()), action)
    }

    pub fn client(mut self, client: ClientId) -> Self {
        self.client = Some(client);
        self
    }

    pub fn nth(mut self, n: u32) -> Self {
        self.nth = n;
        self
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct RuleId(pub(crate) u32);

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct ParkedId(pub(crate) u64);

/// An effect withheld by `Delay` or `Duplicate`, waiting for a release.
#[derive(Clone, Debug)]
pub struct ParkedEffect {
    pub id: ParkedId,
    pub client: ClientId,
    pub run: Option<usize>,
    pub op: OpKind,
    pub key: String,
    /// `BatchDelete` only.
    pub keys: Vec<String>,
    pub issued_at_step: u64,
    /// The issuing client's label at the issue.
    pub label: Option<&'static str>,
    /// `Delay` or `Duplicate`.
    pub action: FaultAction,
    pub(crate) effect: Effect,
}

#[derive(Clone, Debug)]
pub(crate) enum Effect {
    Put(Vec<u8>),
    Delete(Vec<String>),
}

/// Every injected error's message starts with this.
pub const INJECTED_MARKER: &str = "vykar-sim injected fault";
