//! `SimStore`: one in-memory object store shared by many logical clients,
//! each through its own faultable, logged handle.
//!
//! Every operation runs under one lock, so it is atomic and takes exactly one
//! step of the store's op counter. Effects withheld by `Delay` or `Duplicate`
//! stay parked until the test releases them, which is how a request that
//! times out at the client and lands later on the server is modelled.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Condvar, Mutex, MutexGuard};

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use vykar_storage::{
    BackendProfile, BatchDelete, BatchDeleteScope, ConditionalSupport, ConditionalWrite, Created,
    ListedObject, PackData, StagedPut, Verifier,
};
use vykar_types::error::{Result, VykarError};

use super::fault::{
    ClientId, Effect, FaultAction, FaultRule, INJECTED_MARKER, OpKind, ParkedEffect, ParkedId,
    RuleId,
};
use super::gate::{GateId, GatePhase, GateRule, GateState, Paused};
use super::log::{OpOutcome, OpRecord, thread_label};

const OBSERVER: ClientId = ClientId(0);
const TAMPER: ClientId = ClientId(1);

/// What the store's backend does with heads and conditional writes (04 step
/// 8). Every kind but `ConditionalOverwrite` keeps an identical re-PUT of
/// an `index.d/` or `index.h/` key as a no-op with its first mtime
/// (vykar-server's rule).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub enum StoreKind {
    /// No put-if-absent (`conditional()` is None): a backend that does not
    /// offer it (S3 off AWS with `s3_conditional_writes: auto`, SFTP without
    /// `hardlink@openssh.com`) or whose check failed. Heads are plain PUTs
    /// under the lock, and one with other bytes replaces the head. The
    /// fallback; every seed recorded before 04 step 8 runs on it.
    #[default]
    Plain,
    /// vykar-server: honest put-if-absent; a plain PUT of an existing head
    /// with other bytes is refused (409, as vykar-server answers).
    Conditional,
    /// Garage 2.5.0: put-if-absent overwrites and answers Yes; the client's
    /// check concludes Unsupported and it falls back to plain PUTs, which
    /// overwrite.
    Ignoring,
    /// IDrive e2: passes the sequential check; a delayed or duplicated
    /// create that lands later overwrites whatever is there (the
    /// concurrent-create failure the check cannot see).
    RacyRelease,
    /// Local, OpenSSH SFTP, AWS S3: put-if-absent honoured; a plain PUT
    /// replaces any object, heads included (phase F part 10b: what a
    /// lock-mode client's plain head PUT does beside a lock-free one), and
    /// an identical re-PUT of an `index.d/` or `index.h/` key renews its
    /// mtime (and writer), as these backends do: a duplicated plain head
    /// PUT of a lock client makes that head younger, which keeps it from
    /// the index GC longer. With random nonces no client re-creates an
    /// identical object name.
    ConditionalOverwrite,
}

impl StoreKind {
    // serde's `skip_serializing_if` passes a reference.
    #[allow(clippy::trivially_copy_pass_by_ref)]
    pub fn is_plain(&self) -> bool {
        *self == StoreKind::Plain
    }

    /// Whether the store honours put-if-absent, so heads are immutable
    /// (04 I3) and the checker judges it.
    pub fn honours_creates(self) -> bool {
        matches!(
            self,
            StoreKind::Conditional | StoreKind::ConditionalOverwrite
        )
    }
}

#[derive(Clone, Debug)]
pub struct SimConfig {
    pub profile: BackendProfile,
    pub append_only: bool,
    /// `batch_delete()` offered (REST/S3-like) instead of None.
    pub batch_delete: bool,
    pub kind: StoreKind,
}

/// The passphrase of every encrypted simulator repository.
pub const SIM_PASSPHRASE: &str = "sim";

impl Default for SimConfig {
    fn default() -> Self {
        Self {
            profile: BackendProfile {
                local: false,
                staged_put: false,
                request_width: 1,
                transfer_width: 1,
            },
            append_only: false,
            batch_delete: false,
            kind: StoreKind::Plain,
        }
    }
}

#[derive(Clone, Debug)]
pub struct StoredObject {
    pub data: Vec<u8>,
    pub mtime: DateTime<Utc>,
    pub writer: ClientId,
}

struct RuleState {
    id: RuleId,
    rule: FaultRule,
    count: u32,
    spent: bool,
}

struct State {
    objects: BTreeMap<String, StoredObject>,
    step: u64,
    clock: DateTime<Utc>,
    log: Vec<OpRecord>,
    rules: Vec<RuleState>,
    next_rule: u32,
    parked: BTreeMap<ParkedId, ParkedEffect>,
    next_parked: u64,
    crashed: BTreeSet<ClientId>,
    names: Vec<String>,
    /// The run each client is in, if any.
    runs: BTreeMap<ClientId, usize>,
    /// The label of each client's operations, if any (`set_label`).
    labels: BTreeMap<ClientId, &'static str>,
    gates: Vec<GateState>,
    next_gate: u32,
    /// Every replacement of a stored head by other bytes: (key, step, writer).
    head_overwrites: Vec<(String, u64, ClientId)>,
    /// Clients whose handles offer no put-if-absent ([`SimStore::set_lock_client`]).
    lock_clients: BTreeSet<ClientId>,
    /// Runs that wrote (or attempted) a session marker key flagged
    /// `committing` ([`SimStore::committed_lock_free_in_run`]).
    committing_runs: BTreeSet<usize>,
    /// Runs that wrote (or attempted) any session marker key
    /// ([`SimStore::registered_in_run`]).
    registered_runs: BTreeSet<usize>,
}

/// How [`State::apply`] treats an effect.
#[derive(Clone, Copy)]
struct Rules {
    append_only: bool,
    kind: StoreKind,
    /// A parked effect landing late.
    release: bool,
    /// Tampering: no refusal (it models damage, not a client).
    tamper: bool,
}

struct Shared {
    config: SimConfig,
    state: Mutex<State>,
    /// Paired with `state`: signalled when a gate pauses or opens.
    gate_cv: Condvar,
}

/// What the rule pass decided for one operation.
enum Verdict {
    Observer,
    Crashed,
    Fault(FaultAction),
    Clean,
}

#[derive(Clone)]
pub struct SimStore {
    shared: Arc<Shared>,
}

fn injected(action: FaultAction, op: OpKind, key: &str) -> VykarError {
    VykarError::Other(format!("{INJECTED_MARKER}: {action:?} on {op:?} {key}"))
}

fn crashed_err(op: OpKind, key: &str) -> VykarError {
    VykarError::Other(format!("{INJECTED_MARKER}: client crashed ({op:?} {key})"))
}

fn append_only_overwrite_refusal(key: &str) -> VykarError {
    // MemoryBackend's text.
    VykarError::Other(format!(
        "403 Forbidden: repository is append-only, {key} already exists"
    ))
}

fn append_only_delete_refusal(key: &str) -> VykarError {
    // MemoryBackend's text.
    VykarError::Other(format!(
        "403 Forbidden: repository is append-only, delete of {key} refused"
    ))
}

/// An append-only server allows overwriting `key`. Refused for keys outside
/// the schema.
fn append_only_overwrite_ok(key: &str) -> bool {
    vykar_protocol::append_only_allows(key, vykar_protocol::Verb::Overwrite)
}

/// An append-only server allows deleting `key`. Allowed for keys outside the
/// schema, which only synthetic test keys are.
fn append_only_delete_ok(key: &str) -> bool {
    vykar_protocol::key_class(key)
        .is_none_or(|c| c.append_only_allows(vykar_protocol::Verb::Delete))
}

fn is_head_key(key: &str) -> bool {
    key.starts_with("index.h/")
}

fn observer_refusal() -> VykarError {
    VykarError::Other("vykar-sim: observer handle is read-only".into())
}

impl State {
    fn record(
        &mut self,
        client: ClientId,
        op: OpKind,
        key: &str,
        keys: &[String],
        len: Option<u64>,
        outcome: OpOutcome,
    ) {
        let run = self.runs.get(&client).copied();
        let label = self.labels.get(&client).copied();
        self.record_for_run(client, run, label, op, key, keys, len, outcome);
    }

    #[allow(clippy::too_many_arguments)]
    fn record_for_run(
        &mut self,
        client: ClientId,
        run: Option<usize>,
        label: Option<&'static str>,
        op: OpKind,
        key: &str,
        keys: &[String],
        len: Option<u64>,
        outcome: OpOutcome,
    ) {
        self.log.push(OpRecord {
            step: self.step,
            client,
            run,
            thread: thread_label(),
            op,
            key: key.to_string(),
            keys: keys.to_vec(),
            absent: Vec::new(),
            len,
            outcome,
            label,
            at: self.clock,
        });
    }

    /// Note on the record just logged which keys its delete found absent.
    fn note_absent(&mut self, absent: Vec<String>) {
        if let Some(rec) = self.log.last_mut() {
            rec.absent = absent;
        }
    }

    /// Steps 1-4 of every op: count the step, then decide by observer,
    /// crash state and fault rules, in that order.
    fn verdict(&mut self, client: ClientId, observer: bool, op: OpKind, keys: &[&str]) -> Verdict {
        self.step += 1;
        if observer {
            return Verdict::Observer;
        }
        if self.crashed.contains(&client) {
            return Verdict::Crashed;
        }
        let mut fired = None;
        for r in self.rules.iter_mut().filter(|r| !r.spent) {
            if r.rule.client.is_some_and(|c| c != client)
                || !r.rule.op.matches(op)
                || !keys.iter().any(|k| r.rule.key.matches(k))
            {
                continue;
            }
            r.count += 1;
            if r.count >= r.rule.nth {
                // A later rule reaching `nth` on the same op is spent unfired,
                // so it cannot fire late on an op it was not written for.
                r.spent = true;
                if fired.is_none() {
                    fired = Some(r.rule.action);
                }
            }
        }
        match fired {
            Some(FaultAction::Crash) => {
                self.crashed.insert(client);
                Verdict::Fault(FaultAction::Crash)
            }
            Some(a) => Verdict::Fault(a),
            None => Verdict::Clean,
        }
    }

    /// Apply an effect at the current clock. Refuses what an append-only
    /// server refuses (key-class table); a refused batch deletes nothing.
    /// `Ok(None)`: a create found its key and applied nothing.
    fn apply(
        &mut self,
        rules: Rules,
        writer: ClientId,
        key: &str,
        effect: &Effect,
    ) -> std::result::Result<Option<Vec<String>>, VykarError> {
        let mut absent = Vec::new();
        match effect {
            Effect::Put(data) => {
                // vykar-server's rule for content-addressed `index.d/` and
                // write-once `index.h/` keys: a PUT of the stored bytes stores
                // nothing and renews neither mtime nor writer (answered 204,
                // in append-only mode too); `ConditionalOverwrite` (local,
                // SFTP, S3) rewrites the object, renewing both. A head PUT
                // with other bytes is refused 409 on the conditional kind
                // (vykar-server) and replaces the head on the others.
                if let Some(existing) = self.objects.get(key) {
                    let write_once = matches!(
                        vykar_protocol::put_rule(key),
                        vykar_protocol::PutRule::NamedByBlake3 | vykar_protocol::PutRule::WriteOnce
                    );
                    if existing.data == *data
                        && write_once
                        && rules.kind != StoreKind::ConditionalOverwrite
                    {
                        return Ok(Some(absent));
                    }
                    if !rules.tamper
                        && rules.kind == StoreKind::Conditional
                        && is_head_key(key)
                        && existing.data != *data
                    {
                        return Err(VykarError::Other(format!(
                            "409 Conflict: index head {key} exists with other bytes; heads are \
                             write-once"
                        )));
                    }
                }
                if !rules.tamper
                    && rules.append_only
                    && self.objects.contains_key(key)
                    && !append_only_overwrite_ok(key)
                {
                    return Err(append_only_overwrite_refusal(key));
                }
                self.store_object(writer, key, data);
            }
            Effect::PutIfAbsent(data) => {
                let overwrite = match rules.kind {
                    StoreKind::Ignoring => true,
                    StoreKind::RacyRelease => rules.release,
                    StoreKind::Plain | StoreKind::Conditional | StoreKind::ConditionalOverwrite => {
                        false
                    }
                };
                if self.objects.contains_key(key) && !overwrite {
                    return Ok(None);
                }
                self.store_object(writer, key, data);
            }
            Effect::Delete(keys) => {
                if !rules.tamper
                    && rules.append_only
                    && let Some(refused) = keys.iter().find(|k| !append_only_delete_ok(k))
                {
                    return Err(append_only_delete_refusal(refused));
                }
                let mut removed = BTreeSet::new();
                for k in keys {
                    if self.objects.remove(k).is_some() {
                        removed.insert(k.as_str());
                    } else if !removed.contains(k.as_str()) {
                        absent.push(k.clone());
                    }
                }
            }
        }
        Ok(Some(absent))
    }

    /// Store `data` at `key` at the current clock, noting a head replaced
    /// by other bytes.
    fn store_object(&mut self, writer: ClientId, key: &str, data: &[u8]) {
        if is_head_key(key)
            && self
                .objects
                .get(key)
                .is_some_and(|existing| existing.data != data)
        {
            self.head_overwrites
                .push((key.to_string(), self.step, writer));
        }
        self.objects.insert(
            key.to_string(),
            StoredObject {
                data: data.to_vec(),
                mtime: self.clock,
                writer,
            },
        );
    }

    #[allow(clippy::too_many_arguments)]
    fn park(
        &mut self,
        client: ClientId,
        op: OpKind,
        key: &str,
        keys: &[String],
        action: FaultAction,
        effect: Effect,
    ) -> ParkedId {
        let id = ParkedId(self.next_parked);
        self.next_parked += 1;
        self.parked.insert(
            id,
            ParkedEffect {
                id,
                client,
                run: self.runs.get(&client).copied(),
                op,
                key: key.to_string(),
                keys: keys.to_vec(),
                issued_at_step: self.step,
                label: self.labels.get(&client).copied(),
                action,
                effect,
            },
        );
        id
    }
}

impl SimStore {
    pub fn new(config: SimConfig) -> Self {
        let clock = DateTime::from_timestamp(1_767_225_600, 0).expect("valid epoch"); // 2026-01-01
        Self::from_parts(
            config,
            BTreeMap::new(),
            0,
            clock,
            vec!["observer".into(), "tamper".into()],
        )
    }

    fn from_parts(
        config: SimConfig,
        objects: BTreeMap<String, StoredObject>,
        step: u64,
        clock: DateTime<Utc>,
        names: Vec<String>,
    ) -> Self {
        Self {
            shared: Arc::new(Shared {
                config,
                state: Mutex::new(State {
                    objects,
                    step,
                    clock,
                    log: Vec::new(),
                    rules: Vec::new(),
                    next_rule: 0,
                    parked: BTreeMap::new(),
                    next_parked: 0,
                    crashed: BTreeSet::new(),
                    names,
                    runs: BTreeMap::new(),
                    labels: BTreeMap::new(),
                    gates: Vec::new(),
                    next_gate: 0,
                    head_overwrites: Vec::new(),
                    lock_clients: BTreeSet::new(),
                    committing_runs: BTreeSet::new(),
                    registered_runs: BTreeSet::new(),
                }),
                gate_cv: Condvar::new(),
            }),
        }
    }

    /// A deep copy for invariant 9: same objects, clock, step counter,
    /// client names and config; empty log; no rules, parked effects,
    /// crashes, runs, gates or lock clients (faults have stopped).
    pub fn fork(&self) -> SimStore {
        self.fork_as(self.shared.config.kind)
    }

    /// [`Self::fork`] with another [`StoreKind`] (a driver case's
    /// `storage` on the template's store).
    pub fn fork_as(&self, kind: StoreKind) -> SimStore {
        let st = self.state();
        Self::from_parts(
            SimConfig {
                kind,
                ..self.shared.config.clone()
            },
            st.objects.clone(),
            st.step,
            st.clock,
            st.names.clone(),
        )
    }

    /// Every replacement of a stored head by other bytes since the store
    /// (or fork) was made: (key, step, writer).
    pub fn head_overwrites(&self) -> Vec<(String, u64, ClientId)> {
        self.state().head_overwrites.clone()
    }

    fn rules(&self, release: bool) -> Rules {
        Rules {
            append_only: self.shared.config.append_only,
            kind: self.shared.config.kind,
            release,
            tamper: false,
        }
    }

    fn state(&self) -> MutexGuard<'_, State> {
        self.shared.state.lock().unwrap()
    }

    /// The client that `tamper_put` and `tamper_delete` log under.
    pub fn tamper_id(&self) -> ClientId {
        TAMPER
    }

    pub fn config(&self) -> &SimConfig {
        &self.shared.config
    }

    /// The passphrase of the repository the harness initialised here.
    pub fn passphrase(&self) -> Option<&'static str> {
        Some(SIM_PASSPHRASE)
    }

    pub fn register_client(&self, name: &str) -> ClientId {
        let mut st = self.state();
        let id = ClientId(u16::try_from(st.names.len()).expect("too many clients"));
        st.names.push(name.to_string());
        id
    }

    pub fn client_name(&self, client: ClientId) -> String {
        self.state()
            .names
            .get(usize::from(client.0))
            .cloned()
            .unwrap_or_else(|| format!("client#{}", client.0))
    }

    /// Storage handle for `client`: logged, faultable. A lock client's
    /// ([`Self::set_lock_client`]) offers no put-if-absent, as
    /// [`Self::handle_without_conditional`]: every handle the harness builds
    /// comes from here, so the client falls back in every run.
    pub fn handle(&self, client: ClientId) -> SimClient {
        let lock_client = self.state().lock_clients.contains(&client);
        self.new_handle(client, lock_client)
    }

    /// A handle for `client` whose `conditional()` is None on every store
    /// kind: a client whose backend does not offer put-if-absent, or whose
    /// check concluded Unsupported, beside clients that have it (the
    /// mixed-verdict case of phase F part 10).
    pub fn handle_without_conditional(&self, client: ClientId) -> SimClient {
        self.new_handle(client, true)
    }

    /// Every later [`Self::handle`] of `client` offers no put-if-absent.
    pub fn set_lock_client(&self, client: ClientId) {
        self.state().lock_clients.insert(client);
    }

    /// The clients [`set_lock_client`](Self::set_lock_client) named.
    pub fn lock_clients(&self) -> BTreeSet<ClientId> {
        self.state().lock_clients.clone()
    }

    fn new_handle(&self, client: ClientId, no_conditional: bool) -> SimClient {
        assert!(
            client != OBSERVER && client != TAMPER,
            "reserved client id {client:?}"
        );
        SimClient {
            store: self.clone(),
            client,
            observer: false,
            no_conditional,
            cond: Arc::new(Verifier::new()),
        }
    }

    /// Unlogged, unfaulted, read-only handle for the checker.
    pub fn observer(&self) -> SimClient {
        SimClient {
            store: self.clone(),
            client: OBSERVER,
            observer: true,
            no_conditional: false,
            cond: Arc::new(Verifier::new()),
        }
    }

    /// Rejects `nth == 0`, and FailApplied/Delay/Duplicate on an `OpMatch`
    /// that can match an operation without an effect.
    pub fn add_fault(&self, rule: FaultRule) -> Result<RuleId> {
        if rule.nth == 0 {
            return Err(VykarError::Other(
                "vykar-sim: fault rule nth is 1-based".into(),
            ));
        }
        if rule.action.needs_effect() && rule.op.can_match_non_mutation() {
            return Err(VykarError::Other(format!(
                "vykar-sim: {:?} needs an effect, but {:?} can match a read",
                rule.action, rule.op
            )));
        }
        let mut st = self.state();
        let id = RuleId(st.next_rule);
        st.next_rule += 1;
        st.rules.push(RuleState {
            id,
            rule,
            count: 0,
            spent: false,
        });
        Ok(id)
    }

    /// Whether the rule fired or was spent.
    pub fn rule_spent(&self, id: RuleId) -> bool {
        self.state().rules.iter().any(|r| r.id == id && r.spent)
    }

    pub fn crash(&self, client: ClientId) {
        self.state().crashed.insert(client);
    }

    pub fn is_crashed(&self, client: ClientId) -> bool {
        self.state().crashed.contains(&client)
    }

    pub fn parked(&self) -> Vec<ParkedEffect> {
        self.state().parked.values().cloned().collect()
    }

    /// Apply one parked effect now. Err if unknown or already released.
    /// A crashed issuer does not block it: a late landing outlives its client.
    pub fn release(&self, id: ParkedId) -> Result<()> {
        let mut st = self.state();
        let parked = st
            .parked
            .remove(&id)
            .ok_or_else(|| VykarError::Other(format!("vykar-sim: no parked effect {id:?}")))?;
        st.step += 1;
        let (outcome, absent) =
            match st.apply(self.rules(true), parked.client, &parked.key, &parked.effect) {
                Ok(Some(absent)) => (OpOutcome::Released(id), absent),
                // A create that found its key, or a refusal.
                Ok(None) | Err(_) => (OpOutcome::ReleasedRefused(id), Vec::new()),
            };
        let len = parked.effect.len();
        st.record_for_run(
            parked.client,
            parked.run,
            parked.label,
            parked.op,
            &parked.key,
            &parked.keys,
            len,
            outcome,
        );
        st.note_absent(absent);
        Ok(())
    }

    /// Drop one parked effect without applying it: the late request never
    /// lands. Unlogged; its record stays `Parked`. Err if unknown.
    pub fn discard(&self, id: ParkedId) -> Result<()> {
        self.state()
            .parked
            .remove(&id)
            .map(|_| ())
            .ok_or_else(|| VykarError::Other(format!("vykar-sim: no parked effect {id:?}")))
    }

    /// Release all parked effects in `ParkedId` order; returns them.
    pub fn release_all(&self) -> Vec<ParkedId> {
        let ids: Vec<ParkedId> = self.state().parked.keys().copied().collect();
        for id in &ids {
            self.release(*id).expect("parked effect vanished");
        }
        ids
    }

    pub fn log(&self) -> Vec<OpRecord> {
        self.state().log.clone()
    }

    pub fn step(&self) -> u64 {
        self.state().step
    }

    pub fn now(&self) -> DateTime<Utc> {
        self.state().clock
    }

    pub fn advance_clock(&self, by: std::time::Duration) {
        let by = chrono::Duration::from_std(by).expect("clock advance out of range");
        let mut st = self.state();
        st.clock += by;
    }

    /// Step the store's clock alone, in either direction: an NTP correction
    /// on the server. Client clocks do not move; later writes get mtimes on
    /// the stepped clock.
    pub fn step_clock(&self, by: chrono::Duration) {
        let mut st = self.state();
        st.clock += by;
    }

    /// Admin read (unlogged).
    pub fn object(&self, key: &str) -> Option<StoredObject> {
        self.state().objects.get(key).cloned()
    }

    /// Admin listing (unlogged), sorted.
    pub fn keys(&self, prefix: &str) -> Vec<String> {
        self.state()
            .objects
            .keys()
            .filter(|k| k.starts_with(prefix))
            .cloned()
            .collect()
    }

    /// Keys `client` wrote successfully under `prefix` (Ok, FailApplied,
    /// Duplicate or released), from the log, in first-write order.
    pub fn keys_written_by(&self, client: ClientId, prefix: &str) -> Vec<String> {
        let st = self.state();
        let mut seen = BTreeSet::new();
        st.log
            .iter()
            .filter(|r| r.client == client && r.op.is_write() && r.applied())
            .filter(|r| r.key.starts_with(prefix))
            .filter(|r| seen.insert(r.key.clone()))
            .map(|r| r.key.clone())
            .collect()
    }

    /// Damage for checker sensitivity tests; logged under the reserved
    /// "tamper" client, never faulted.
    pub fn tamper_put(&self, key: &str, data: &[u8]) {
        let mut st = self.state();
        st.step += 1;
        let effect = Effect::Put(data.to_vec());
        // Tampering bypasses append-only and write-once heads: it models
        // damage, not a client.
        let rules = Rules {
            tamper: true,
            ..self.rules(false)
        };
        st.apply(rules, TAMPER, key, &effect)
            .expect("unrestricted put");
        st.record(
            TAMPER,
            OpKind::Put,
            key,
            &[],
            Some(data.len() as u64),
            OpOutcome::Ok,
        );
    }

    pub fn tamper_delete(&self, key: &str) -> bool {
        let mut st = self.state();
        st.step += 1;
        let existed = st.objects.remove(key).is_some();
        let outcome = if existed {
            OpOutcome::Ok
        } else {
            OpOutcome::NotFound
        };
        st.record(TAMPER, OpKind::Delete, key, &[], None, outcome);
        existed
    }

    /// Whether run `run` attempted a session marker PUT flagged
    /// `committing`: the lock-free writer's commit mark, which a backup
    /// writes only once it decided to commit lock-free
    /// (`PendingCommit::commit_mode`: an encrypted repository, a passed
    /// put-if-absent check and a continuous marker record) and a lock-mode
    /// commit never writes. The simulator's record of a run's mode,
    /// `RunRecord::lock_free`.
    pub fn committed_lock_free_in_run(&self, run: usize) -> bool {
        self.state().committing_runs.contains(&run)
    }

    /// Whether run `run` wrote (or attempted) a session marker key: a
    /// maintenance run that did ran lock-free (04 step 9: under the lock
    /// maintenance registers no session); `RunRecord::lock_free`.
    pub fn registered_in_run(&self, run: usize) -> bool {
        self.state().registered_runs.contains(&run)
    }

    /// Tag `client`'s operations with `run` until set to None.
    pub(crate) fn set_current_run(&self, client: ClientId, run: Option<usize>) {
        let mut st = self.state();
        match run {
            Some(r) => st.runs.insert(client, r),
            None => st.runs.remove(&client),
        };
    }

    /// Label `client`'s operations with `label` until set to None (the
    /// driver's step names). `fork` drops labels.
    pub(crate) fn set_label(&self, client: ClientId, label: Option<&'static str>) {
        let mut st = self.state();
        match label {
            Some(l) => st.labels.insert(client, l),
            None => st.labels.remove(&client),
        };
    }

    /// Rejects `nth == 0` and the reserved observer/tamper ids.
    pub fn add_gate(&self, rule: GateRule) -> Result<GateId> {
        if rule.nth == 0 {
            return Err(VykarError::Other("vykar-sim: gate nth is 1-based".into()));
        }
        if rule.client == OBSERVER || rule.client == TAMPER {
            return Err(VykarError::Other(format!(
                "vykar-sim: gate on reserved client id {:?}",
                rule.client
            )));
        }
        let mut st = self.state();
        let id = GateId(st.next_gate);
        st.next_gate += 1;
        st.gates.push(GateState {
            id,
            rule,
            phase: GatePhase::Armed { count: 0 },
        });
        Ok(id)
    }

    /// The client a gate names, if the gate exists.
    pub fn gate_client(&self, id: GateId) -> Option<ClientId> {
        self.state()
            .gates
            .iter()
            .find(|g| g.id == id)
            .map(|g| g.rule.client)
    }

    /// Whether the gate is still counting operations.
    pub fn gate_armed(&self, id: GateId) -> bool {
        self.state()
            .gates
            .iter()
            .any(|g| g.id == id && matches!(g.phase, GatePhase::Armed { .. }))
    }

    /// The operation the gate holds now, if any.
    pub fn gate_paused(&self, id: GateId) -> Option<Paused> {
        gate_paused_in(&self.state(), id)
    }

    /// Block (real time, at most `timeout`) until the gate holds an operation.
    /// None on timeout, or if the gate is Open or Missed.
    pub fn wait_gate(&self, id: GateId, timeout: std::time::Duration) -> Option<Paused> {
        self.wait_gate_or(id, timeout, &|| false)
    }

    /// [`Self::wait_gate`], also ending early once `stop()` holds. `stop`
    /// is judged under the state lock, so a thread that makes it true and
    /// then calls [`Self::notify_gates`] is never missed.
    pub(crate) fn wait_gate_or(
        &self,
        id: GateId,
        timeout: std::time::Duration,
        stop: &dyn Fn() -> bool,
    ) -> Option<Paused> {
        let st = self.state();
        let (st, _) = self
            .shared
            .gate_cv
            .wait_timeout_while(st, timeout, |s| {
                !stop()
                    && s.gates
                        .iter()
                        .any(|g| g.id == id && matches!(g.phase, GatePhase::Armed { .. }))
            })
            .unwrap();
        gate_paused_in(&st, id)
    }

    /// Wake every gate waiter, so it judges its `stop` again.
    /// Poison-tolerant: it runs in a drop guard, also on unwind.
    pub(crate) fn notify_gates(&self) {
        let _st = self
            .shared
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        self.shared.gate_cv.notify_all();
    }

    /// Paused: let the operation proceed. Armed: disarm, so it never fires.
    /// Err if unknown, Open or Missed.
    pub fn resume_gate(&self, id: GateId) -> Result<()> {
        let mut st = self.state();
        let gate = st
            .gates
            .iter_mut()
            .find(|g| g.id == id)
            .ok_or_else(|| VykarError::Other(format!("vykar-sim: no gate {id:?}")))?;
        match gate.phase {
            GatePhase::Armed { .. } | GatePhase::Paused(_) => gate.phase = GatePhase::Open,
            GatePhase::Open | GatePhase::Missed => {
                return Err(VykarError::Other(format!(
                    "vykar-sim: gate {id:?} is not armed or paused"
                )));
            }
        }
        self.shared.gate_cv.notify_all();
        Ok(())
    }

    /// Every operation held by a gate now.
    pub fn paused(&self) -> Vec<Paused> {
        self.state()
            .gates
            .iter()
            .filter_map(|g| match &g.phase {
                GatePhase::Paused(p) => Some(p.clone()),
                _ => None,
            })
            .collect()
    }

    /// Whether any of `client`'s threads is held by a gate.
    pub fn client_paused(&self, client: ClientId) -> bool {
        self.state()
            .gates
            .iter()
            .any(|g| matches!(&g.phase, GatePhase::Paused(p) if p.client == client))
    }

    /// Whether `client` has a gate still Armed.
    pub fn has_armed_gate(&self, client: ClientId) -> bool {
        self.state()
            .gates
            .iter()
            .any(|g| g.rule.client == client && matches!(g.phase, GatePhase::Armed { .. }))
    }

    /// Open every gate (Sim's Drop), so blocked threads can finish.
    pub fn open_all_gates(&self) {
        let mut st = self.state();
        for g in &mut st.gates {
            g.phase = GatePhase::Open;
        }
        self.shared.gate_cv.notify_all();
    }

    /// Count `op` against `client`'s armed gates; if one reaches its `nth`,
    /// hold the calling thread (state lock released) until it is opened.
    fn pass_gates<'a>(
        &'a self,
        mut st: MutexGuard<'a, State>,
        client: ClientId,
        observer: bool,
        op: OpKind,
        keys: &[&str],
    ) -> MutexGuard<'a, State> {
        if observer || st.gates.is_empty() {
            return st;
        }
        let run = st.runs.get(&client).copied();
        let at_step = st.step;
        let mut held = None;
        for g in &mut st.gates {
            let GatePhase::Armed { count } = &mut g.phase else {
                continue;
            };
            if g.rule.client != client || !g.rule.matches(op, keys) {
                continue;
            }
            *count += 1;
            if *count < g.rule.nth {
                continue;
            }
            if held.is_some() {
                g.phase = GatePhase::Missed;
                continue;
            }
            held = Some(g.id);
            g.phase = GatePhase::Paused(Paused {
                gate: g.id,
                client,
                run,
                thread: thread_label(),
                op,
                key: keys.first().map(|k| k.to_string()).unwrap_or_default(),
                at_step,
            });
        }
        let Some(id) = held else {
            return st;
        };
        self.shared.gate_cv.notify_all();
        self.shared
            .gate_cv
            .wait_while(st, |s| gate_paused_in(s, id).is_some())
            .unwrap()
    }

    /// One read-like op (no effect): `f` computes the result, its outcome
    /// and its length from the object map.
    fn read_op<T>(
        &self,
        client: ClientId,
        observer: bool,
        op: OpKind,
        key: &str,
        f: impl FnOnce(&BTreeMap<String, StoredObject>) -> (Result<T>, OpOutcome, Option<u64>),
    ) -> Result<T> {
        let st = self.state();
        let mut st = self.pass_gates(st, client, observer, op, &[key]);
        match st.verdict(client, observer, op, &[key]) {
            Verdict::Observer => f(&st.objects).0,
            Verdict::Crashed => {
                st.record(client, op, key, &[], None, OpOutcome::Crashed);
                Err(crashed_err(op, key))
            }
            Verdict::Fault(action) => {
                st.record(client, op, key, &[], None, OpOutcome::Injected(action));
                Err(injected(action, op, key))
            }
            Verdict::Clean => {
                let (result, outcome, len) = f(&st.objects);
                st.record(client, op, key, &[], len, outcome);
                result
            }
        }
    }

    /// One mutation. `keys` is non-empty only for a batch delete.
    /// `Ok(false)`: a create found its key and applied nothing.
    fn mutate_op(
        &self,
        client: ClientId,
        observer: bool,
        op: OpKind,
        key: &str,
        keys: &[String],
        effect: Effect,
    ) -> Result<bool> {
        let rules = self.rules(false);
        let len = effect.len();
        let match_keys: Vec<&str> = if op == OpKind::BatchDelete {
            keys.iter().map(String::as_str).collect()
        } else {
            vec![key]
        };
        let st = self.state();
        let mut st = self.pass_gates(st, client, observer, op, &match_keys);
        match st.verdict(client, observer, op, &match_keys) {
            Verdict::Observer => Err(observer_refusal()),
            Verdict::Crashed => {
                st.record(client, op, key, keys, len, OpOutcome::Crashed);
                Err(crashed_err(op, key))
            }
            Verdict::Fault(action @ (FaultAction::Fail | FaultAction::Crash)) => {
                st.record(client, op, key, keys, len, OpOutcome::Injected(action));
                Err(injected(action, op, key))
            }
            Verdict::Fault(FaultAction::FailApplied) => match st.apply(rules, client, key, &effect)
            {
                Ok(Some(absent)) => {
                    let outcome = OpOutcome::Injected(FaultAction::FailApplied);
                    st.record(client, op, key, keys, len, outcome);
                    st.note_absent(absent);
                    Err(injected(FaultAction::FailApplied, op, key))
                }
                Ok(None) => {
                    // The create found its key; the reply is lost all the same.
                    st.record(client, op, key, keys, len, OpOutcome::Exists);
                    Err(injected(FaultAction::FailApplied, op, key))
                }
                Err(e) => {
                    st.record(client, op, key, keys, len, OpOutcome::Refused);
                    Err(e)
                }
            },
            Verdict::Fault(FaultAction::Delay) => {
                let id = st.park(client, op, key, keys, FaultAction::Delay, effect);
                let outcome = OpOutcome::Parked(id, FaultAction::Delay);
                st.record(client, op, key, keys, len, outcome);
                Err(injected(FaultAction::Delay, op, key))
            }
            Verdict::Fault(FaultAction::Duplicate) => match st.apply(rules, client, key, &effect) {
                Ok(Some(absent)) => {
                    let id = st.park(client, op, key, keys, FaultAction::Duplicate, effect);
                    let outcome = OpOutcome::Parked(id, FaultAction::Duplicate);
                    st.record(client, op, key, keys, len, outcome);
                    st.note_absent(absent);
                    Ok(true)
                }
                Ok(None) => {
                    st.record(client, op, key, keys, len, OpOutcome::Exists);
                    Ok(false)
                }
                Err(e) => {
                    st.record(client, op, key, keys, len, OpOutcome::Refused);
                    Err(e)
                }
            },
            Verdict::Clean => match st.apply(rules, client, key, &effect) {
                Ok(Some(absent)) => {
                    st.record(client, op, key, keys, len, OpOutcome::Ok);
                    st.note_absent(absent);
                    Ok(true)
                }
                Ok(None) => {
                    st.record(client, op, key, keys, len, OpOutcome::Exists);
                    Ok(false)
                }
                Err(e) => {
                    st.record(client, op, key, keys, len, OpOutcome::Refused);
                    Err(e)
                }
            },
        }
    }
}

fn gate_paused_in(st: &State, id: GateId) -> Option<Paused> {
    st.gates
        .iter()
        .find(|g| g.id == id)
        .and_then(|g| match &g.phase {
            GatePhase::Paused(p) => Some(p.clone()),
            _ => None,
        })
}

/// One client's view of the store.
#[derive(Clone)]
pub struct SimClient {
    store: SimStore,
    client: ClientId,
    observer: bool,
    /// `conditional()` is None whatever the store's kind
    /// ([`SimStore::handle_without_conditional`]).
    no_conditional: bool,
    /// The put-if-absent check's cache, as a production backend instance
    /// has one: shared by this handle's clones (one per run).
    cond: Arc<Verifier>,
}

impl SimClient {
    pub fn client(&self) -> ClientId {
        self.client
    }

    fn read<T>(
        &self,
        op: OpKind,
        key: &str,
        f: impl FnOnce(&BTreeMap<String, StoredObject>) -> (Result<T>, OpOutcome, Option<u64>),
    ) -> Result<T> {
        self.store.read_op(self.client, self.observer, op, key, f)
    }

    fn write(&self, op: OpKind, key: &str, data: Vec<u8>) -> Result<()> {
        self.store
            .mutate_op(self.client, self.observer, op, key, &[], Effect::Put(data))
            .map(|_| ())
    }

    /// The store's raw create, unverified (what the check runs).
    fn raw_create(&self, key: &str, data: &[u8]) -> Result<Created> {
        let created = self.store.mutate_op(
            self.client,
            self.observer,
            OpKind::PutIfAbsent,
            key,
            &[],
            Effect::PutIfAbsent(data.to_vec()),
        )?;
        Ok(if created {
            Created::Yes
        } else {
            Created::AlreadyExists
        })
    }
}

impl ConditionalWrite for SimClient {
    fn put_if_absent(&self, key: &str, data: &[u8]) -> Result<Created> {
        self.cond
            .put_if_absent("sim", self, &|k, d| self.raw_create(k, d), key, data)
    }

    fn verify(&self) -> Result<ConditionalSupport> {
        self.cond.verify(self, &|k, d| self.raw_create(k, d))
    }
}

/// Whether a session marker body carries `"committing": true`.
fn is_committing_marker(data: &[u8]) -> bool {
    serde_json::from_slice::<serde_json::Value>(data)
        .ok()
        .and_then(|v| v.get("committing").and_then(serde_json::Value::as_bool))
        .unwrap_or(false)
}

fn found_or_not<T>(v: Option<T>, len: Option<u64>) -> (Result<Option<T>>, OpOutcome, Option<u64>) {
    match v {
        Some(v) => (Ok(Some(v)), OpOutcome::Ok, len),
        None => (Ok(None), OpOutcome::NotFound, None),
    }
}

impl vykar_storage::StorageBackend for SimClient {
    fn get(&self, key: &str) -> Result<Option<Vec<u8>>> {
        self.read(OpKind::Get, key, |m| {
            let v = m.get(key).map(|o| o.data.clone());
            let len = v.as_ref().map(|d| d.len() as u64);
            found_or_not(v, len)
        })
    }

    fn put(&self, key: &str, data: &[u8]) -> Result<()> {
        if key.starts_with("sessions/") && key.ends_with(".json") {
            let committing = is_committing_marker(data);
            let mut st = self.store.state();
            if let Some(run) = st.runs.get(&self.client).copied() {
                st.registered_runs.insert(run);
                if committing {
                    st.committing_runs.insert(run);
                }
            }
        }
        self.write(OpKind::Put, key, data.to_vec())
    }

    fn delete(&self, key: &str) -> Result<()> {
        self.store
            .mutate_op(
                self.client,
                self.observer,
                OpKind::Delete,
                key,
                &[],
                Effect::Delete(vec![key.to_string()]),
            )
            .map(|_| ())
    }

    fn exists(&self, key: &str) -> Result<bool> {
        self.read(OpKind::Exists, key, |m| {
            if m.contains_key(key) {
                (Ok(true), OpOutcome::Ok, None)
            } else {
                (Ok(false), OpOutcome::NotFound, None)
            }
        })
    }

    fn list(&self, prefix: &str) -> Result<Vec<String>> {
        self.read(OpKind::List, prefix, |m| {
            let keys: Vec<String> = m
                .keys()
                .filter(|k| k.starts_with(prefix))
                .cloned()
                .collect();
            let n = keys.len() as u64;
            (Ok(keys), OpOutcome::Ok, Some(n))
        })
    }

    fn list_sized(&self, prefix: &str) -> Result<Vec<ListedObject>> {
        self.read(OpKind::ListSized, prefix, |m| {
            let listed: Vec<ListedObject> = m
                .iter()
                .filter(|(k, _)| k.starts_with(prefix))
                .map(|(k, o)| ListedObject {
                    key: k.clone(),
                    size: Some(o.data.len() as u64),
                    mtime: None,
                })
                .collect();
            let n = listed.len() as u64;
            (Ok(listed), OpOutcome::Ok, Some(n))
        })
    }

    fn list_meta(&self, prefix: &str) -> Result<Vec<ListedObject>> {
        self.read(OpKind::ListMeta, prefix, |m| {
            let listed: Vec<ListedObject> = m
                .iter()
                .filter(|(k, _)| k.starts_with(prefix))
                .map(|(k, o)| ListedObject {
                    key: k.clone(),
                    size: Some(o.data.len() as u64),
                    mtime: Some(o.mtime.into()),
                })
                .collect();
            let n = listed.len() as u64;
            (Ok(listed), OpOutcome::Ok, Some(n))
        })
    }

    fn size(&self, key: &str) -> Result<Option<u64>> {
        self.read(OpKind::Size, key, |m| {
            let v = m.get(key).map(|o| o.data.len() as u64);
            found_or_not(v, v)
        })
    }

    fn get_range(&self, key: &str, offset: u64, length: u64) -> Result<Option<Vec<u8>>> {
        self.read(OpKind::GetRange, key, |m| {
            let Some(obj) = m.get(key) else {
                return (Ok(None), OpOutcome::NotFound, Some(length));
            };
            let data = &obj.data;
            // Short reads fail exactly as MemoryBackend's do.
            let start = offset as usize;
            let Some(end) = start.checked_add(length as usize) else {
                let e = VykarError::Other(format!(
                    "short read on {key} at offset {offset}: offset + length overflows usize"
                ));
                return (Err(e), OpOutcome::Error, Some(length));
            };
            if start >= data.len() || end > data.len() {
                let e = VykarError::Other(format!(
                    "short read on {key} at offset {offset}: expected {length} bytes, got {}",
                    data.len().saturating_sub(start)
                ));
                return (Err(e), OpOutcome::Error, Some(length));
            }
            (
                Ok(Some(data[start..end].to_vec())),
                OpOutcome::Ok,
                Some(length),
            )
        })
    }

    fn create_dir(&self, key: &str) -> Result<()> {
        self.read(OpKind::CreateDir, key, |_| (Ok(()), OpOutcome::Ok, None))
    }

    fn put_owned(&self, key: &str, data: Vec<u8>) -> Result<()> {
        self.write(OpKind::PutOwned, key, data)
    }

    fn put_pack(&self, key: &str, data: PackData) -> Result<()> {
        // The release signal fires when `_release` drops, after the op.
        let (bytes, _release) = data.into_parts();
        self.write(OpKind::PutPack, key, bytes)
    }

    fn begin_staged_put(&self, prefix: &str) -> Result<Option<Box<dyn StagedPut>>> {
        if !self.store.shared.config.profile.staged_put {
            return Ok(None);
        }
        Ok(Some(Box::new(SimStagedPut {
            client: self.clone(),
            prefix: prefix.trim_end_matches('/').to_string(),
            buf: Vec::new(),
        })))
    }

    fn batch_delete(&self) -> Option<&dyn BatchDelete> {
        self.store
            .shared
            .config
            .batch_delete
            .then_some(self as &dyn BatchDelete)
    }

    fn profile(&self) -> BackendProfile {
        self.store.shared.config.profile
    }

    fn conditional(&self) -> Option<&dyn ConditionalWrite> {
        if self.no_conditional {
            return None;
        }
        match self.store.shared.config.kind {
            StoreKind::Plain => None,
            _ => (!self.cond.refused()).then_some(self as &dyn ConditionalWrite),
        }
    }

    vykar_storage::unsupported_server_ops!(except[conditional, batch_delete]);
}

impl BatchDelete for SimClient {
    fn delete_keys(&self, keys: &[String], _scope: BatchDeleteScope) -> Result<()> {
        // The scope only matters for directories and temp files, and the
        // store has neither.
        let first = keys.first().map(String::as_str).unwrap_or("");
        self.store
            .mutate_op(
                self.client,
                self.observer,
                OpKind::BatchDelete,
                first,
                keys,
                Effect::Delete(keys.to_vec()),
            )
            .map(|_| ())
    }
}

/// A staged upload: bytes stay in the buffer, invisible, until `commit`.
struct SimStagedPut {
    client: SimClient,
    prefix: String,
    buf: Vec<u8>,
}

impl StagedPut for SimStagedPut {
    fn append(&mut self, data: &[u8]) -> Result<()> {
        self.buf.extend_from_slice(data);
        Ok(())
    }

    fn commit(self: Box<Self>, key: &str) -> Result<()> {
        if !key.starts_with(&format!("{}/", self.prefix)) {
            return Err(VykarError::Other(format!(
                "vykar-sim: staged commit of {key} outside {}/",
                self.prefix
            )));
        }
        self.client.write(OpKind::StagedCommit, key, self.buf)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sim::fault::{KeyMatch, OpMatch};
    use crate::sim::gate::GateRule;
    use vykar_storage::StorageBackend;

    fn two_clients(config: SimConfig) -> (SimStore, SimClient, SimClient) {
        let store = SimStore::new(config);
        let a = store.register_client("a");
        let b = store.register_client("b");
        let (ha, hb) = (store.handle(a), store.handle(b));
        (store, ha, hb)
    }

    fn last(store: &SimStore) -> OpRecord {
        store.log().last().cloned().expect("log empty")
    }

    #[test]
    fn basic_ops_roundtrip() {
        let (store, a, _) = two_clients(SimConfig::default());
        a.put("k/2", b"two").unwrap();
        a.put("k/1", b"one").unwrap();
        a.put("other", b"x").unwrap();
        assert_eq!(a.get("k/1").unwrap().as_deref(), Some(&b"one"[..]));
        assert!(a.exists("k/2").unwrap());
        assert_eq!(a.size("k/2").unwrap(), Some(3));
        assert_eq!(a.list("k/").unwrap(), vec!["k/1", "k/2"]);
        let sized = a.list_sized("k/").unwrap();
        assert_eq!(sized.len(), 2);
        assert_eq!(sized[0].size, Some(3));
        let meta = a.list_meta("k/").unwrap();
        assert_eq!(meta[0].mtime, Some(store.now().into()));
        assert_eq!(last(&store).op, OpKind::ListMeta);
        assert_eq!(
            a.get_range("k/1", 1, 2).unwrap().as_deref(),
            Some(&b"ne"[..])
        );
        let short = a.get_range("k/1", 2, 5).unwrap_err().to_string();
        assert!(short.contains("short read"), "{short}");
        assert_eq!(last(&store).outcome, OpOutcome::Error);
        assert!(a.get("absent").unwrap().is_none());
        assert_eq!(last(&store).outcome, OpOutcome::NotFound);
        assert!(a.get_range("absent", 0, 1).unwrap().is_none());
        a.delete("k/1").unwrap();
        a.delete("k/1").unwrap();
        assert!(!a.exists("k/1").unwrap());
        assert_eq!(store.keys(""), vec!["k/2", "other"]);
    }

    #[test]
    fn fail_not_applied() {
        let (store, a, _) = two_clients(SimConfig::default());
        store
            .add_fault(FaultRule::put("k", FaultAction::Fail))
            .unwrap();
        let err = a.put("k", b"v").unwrap_err().to_string();
        assert!(err.contains(INJECTED_MARKER), "{err}");
        assert!(store.object("k").is_none());
        assert_eq!(last(&store).outcome, OpOutcome::Injected(FaultAction::Fail));
    }

    #[test]
    fn fail_applied_lands() {
        let (store, a, _) = two_clients(SimConfig::default());
        store
            .add_fault(FaultRule::put("k", FaultAction::FailApplied))
            .unwrap();
        assert!(a.put("k", b"v").is_err());
        assert_eq!(store.object("k").unwrap().data, b"v");
        assert_eq!(store.keys_written_by(a.client(), ""), vec!["k"]);
    }

    #[test]
    fn delay_parks_and_release_lands() {
        let (store, a, _) = two_clients(SimConfig::default());
        store
            .add_fault(FaultRule::put("k", FaultAction::Delay))
            .unwrap();
        assert!(a.put("k", b"v").is_err());
        assert!(store.object("k").is_none());
        let parked = store.parked();
        assert_eq!(parked.len(), 1);
        assert_eq!(parked[0].key, "k");
        assert_eq!(parked[0].client, a.client());
        store.advance_clock(std::time::Duration::from_secs(60));
        store.release(parked[0].id).unwrap();
        let obj = store.object("k").unwrap();
        assert_eq!(obj.data, b"v");
        assert_eq!(obj.mtime, store.now());
        assert_eq!(last(&store).outcome, OpOutcome::Released(parked[0].id));
        assert_eq!(last(&store).client, a.client());
        assert!(store.release(parked[0].id).is_err());
    }

    #[test]
    fn identical_index_re_put_keeps_mtime_and_writer() {
        let (store, a, b) = two_clients(SimConfig::default());
        let segment = format!("index.d/{}", "ab".repeat(32));
        a.put(&segment, b"seg").unwrap();
        let first = store.object(&segment).unwrap();
        store.advance_clock(std::time::Duration::from_secs(60));
        b.put(&segment, b"seg").unwrap();
        let again = store.object(&segment).unwrap();
        assert_eq!((again.mtime, again.writer), (first.mtime, a.client()));
        assert_eq!(last(&store).outcome, OpOutcome::Ok);

        // Other bytes renew (the doubles do not refuse).
        b.put(&segment, b"other").unwrap();
        let other = store.object(&segment).unwrap();
        assert_eq!((other.mtime, other.writer), (store.now(), b.client()));

        // The same for a head.
        a.put("index.h/3", b"head").unwrap();
        let head = store.object("index.h/3").unwrap();
        store.advance_clock(std::time::Duration::from_secs(60));
        b.put("index.h/3", b"head").unwrap();
        assert_eq!(store.object("index.h/3").unwrap().mtime, head.mtime);
    }

    #[test]
    fn lock_re_put_renews_mtime() {
        let (store, a, _) = two_clients(SimConfig::default());
        a.put("locks/x.json", b"l").unwrap();
        store.advance_clock(std::time::Duration::from_secs(60));
        a.put("locks/x.json", b"l").unwrap();
        assert_eq!(store.object("locks/x.json").unwrap().mtime, store.now());
    }

    #[test]
    fn step_clock_moves_later_mtimes_both_ways() {
        let (store, a, _) = two_clients(SimConfig::default());
        let start = store.now();
        store.step_clock(chrono::Duration::hours(3));
        a.put("locks/x.json", b"l").unwrap();
        assert_eq!(
            store.object("locks/x.json").unwrap().mtime,
            start + chrono::Duration::hours(3)
        );
        store.step_clock(chrono::Duration::hours(-5));
        a.put("locks/y.json", b"l").unwrap();
        assert_eq!(
            store.object("locks/y.json").unwrap().mtime,
            start - chrono::Duration::hours(2)
        );
    }

    #[test]
    fn delayed_put_overwrites_newer_value() {
        let (store, a, b) = two_clients(SimConfig::default());
        store
            .add_fault(FaultRule::put("k", FaultAction::Delay).client(a.client()))
            .unwrap();
        assert!(a.put("k", b"v1").is_err());
        b.put("k", b"v2").unwrap();
        assert_eq!(store.release_all().len(), 1);
        let obj = store.object("k").unwrap();
        assert_eq!(obj.data, b"v1");
        assert_eq!(obj.writer, a.client());
    }

    #[test]
    fn duplicate_reapplies_old_bytes() {
        let (store, a, _) = two_clients(SimConfig::default());
        store
            .add_fault(FaultRule::put("k", FaultAction::Duplicate))
            .unwrap();
        a.put("k", b"v1").unwrap();
        assert_eq!(store.object("k").unwrap().data, b"v1");
        a.put("k", b"v2").unwrap();
        store.release_all();
        assert_eq!(store.object("k").unwrap().data, b"v1");
    }

    #[test]
    fn delay_on_delete() {
        let (store, a, _) = two_clients(SimConfig::default());
        a.put("k", b"v").unwrap();
        store
            .add_fault(FaultRule::new(
                OpMatch::AnyDelete,
                KeyMatch::Exact("k".into()),
                FaultAction::Delay,
            ))
            .unwrap();
        assert!(a.delete("k").is_err());
        assert!(store.object("k").is_some());
        store.release_all();
        assert!(store.object("k").is_none());
    }

    #[test]
    fn nth_counts_only_matching_ops() {
        let (store, a, b) = two_clients(SimConfig::default());
        let rule = store
            .add_fault(
                FaultRule::new(
                    OpMatch::Kind(OpKind::Put),
                    KeyMatch::Prefix("p/".into()),
                    FaultAction::Fail,
                )
                .client(a.client())
                .nth(2),
            )
            .unwrap();
        a.put("q/1", b"x").unwrap(); // key does not match
        b.put("p/1", b"x").unwrap(); // client does not match
        b.put("p/2", b"x").unwrap();
        a.put("p/3", b"x").unwrap(); // 1st match
        assert!(!store.rule_spent(rule));
        assert!(a.put("p/4", b"x").is_err()); // 2nd match: fires
        assert!(store.rule_spent(rule));
        a.put("p/5", b"x").unwrap(); // fires once
        assert!(store.object("p/4").is_none());
        assert!(store.object("p/5").is_some());
    }

    #[test]
    fn invalid_rules_rejected() {
        let store = SimStore::new(SimConfig::default());
        for action in [
            FaultAction::Delay,
            FaultAction::FailApplied,
            FaultAction::Duplicate,
        ] {
            assert!(
                store
                    .add_fault(FaultRule::new(OpMatch::AnyRead, KeyMatch::Any, action))
                    .is_err()
            );
            assert!(
                store
                    .add_fault(FaultRule::new(OpMatch::Any, KeyMatch::Any, action))
                    .is_err()
            );
        }
        assert!(
            store
                .add_fault(FaultRule::put("k", FaultAction::Fail).nth(0))
                .is_err()
        );
        assert!(
            store
                .add_fault(FaultRule::new(
                    OpMatch::AnyRead,
                    KeyMatch::Any,
                    FaultAction::Fail
                ))
                .is_ok()
        );
    }

    #[test]
    fn crash_blocks_later_ops() {
        let (store, a, b) = two_clients(SimConfig::default());
        store
            .add_fault(FaultRule::put("early", FaultAction::Delay))
            .unwrap();
        assert!(a.put("early", b"v").is_err());
        store.crash(a.client());
        assert!(store.is_crashed(a.client()));
        let err = a.put("k", b"v").unwrap_err().to_string();
        assert!(err.contains(INJECTED_MARKER), "{err}");
        assert!(store.object("k").is_none());
        assert_eq!(last(&store).outcome, OpOutcome::Crashed);
        assert!(a.get("k").is_err());
        b.put("k", b"b").unwrap();
        assert_eq!(store.release_all().len(), 1);
        assert!(store.object("early").is_some());
    }

    #[test]
    fn crash_rule() {
        let (store, a, _) = two_clients(SimConfig::default());
        store
            .add_fault(FaultRule::new(OpMatch::AnyWrite, KeyMatch::Any, FaultAction::Crash).nth(2))
            .unwrap();
        a.put("1", b"x").unwrap();
        assert!(a.put("2", b"x").is_err());
        assert!(store.is_crashed(a.client()));
        assert!(store.object("2").is_none());
        assert!(a.put("3", b"x").is_err());
    }

    #[test]
    fn append_only_refuses_overwrite() {
        let config = SimConfig {
            append_only: true,
            ..SimConfig::default()
        };
        let (store, a, b) = two_clients(config);
        a.put("k", b"v1").unwrap();
        let err = b.put("k", b"v2").unwrap_err().to_string();
        assert!(err.contains("append-only"), "{err}");
        assert_eq!(last(&store).outcome, OpOutcome::Refused);
        assert_eq!(store.object("k").unwrap().data, b"v1");

        store
            .add_fault(FaultRule::put("j", FaultAction::Delay))
            .unwrap();
        assert!(a.put("j", b"late").is_err());
        b.put("j", b"first").unwrap();
        let ids = store.release_all();
        assert_eq!(last(&store).outcome, OpOutcome::ReleasedRefused(ids[0]));
        assert_eq!(store.object("j").unwrap().data, b"first");

        a.delete("k").unwrap();
        assert!(store.object("k").is_none());
    }

    #[test]
    fn append_only_follows_the_key_class_table() {
        let config = SimConfig {
            append_only: true,
            batch_delete: true,
            ..SimConfig::default()
        };
        let (store, a, _) = two_clients(config);
        for key in [
            "index",
            "locks/a",
            "config",
            "index.h/1",
            "snapshots/s",
            "sessions/s",
        ] {
            a.put(key, b"one").unwrap();
        }
        a.put("index", b"two").unwrap();
        a.put("locks/a", b"two").unwrap();
        for key in ["config", "index.h/1"] {
            let err = a.put(key, b"two").unwrap_err().to_string();
            assert!(err.contains("append-only"), "{key}: {err}");
            assert_eq!(last(&store).outcome, OpOutcome::Refused);
            assert_eq!(store.object(key).unwrap().data, b"one");
        }
        let err = a.delete("snapshots/s").unwrap_err().to_string();
        assert!(err.contains("append-only"), "{err}");
        assert_eq!(last(&store).outcome, OpOutcome::Refused);
        assert!(store.object("snapshots/s").is_some());
        a.delete("sessions/s").unwrap();
        assert!(store.object("sessions/s").is_none());

        // A batch fails as a whole on its first refused key.
        let keys = ["locks/a".to_string(), "snapshots/s".to_string()];
        let err = a
            .batch_delete()
            .expect("batch delete offered")
            .delete_keys(&keys, BatchDeleteScope::KeysOnly)
            .unwrap_err()
            .to_string();
        assert!(err.contains("append-only"), "{err}");
        assert_eq!(last(&store).outcome, OpOutcome::Refused);
        assert!(store.object("locks/a").is_some());
    }

    #[test]
    fn staged_put() {
        let config = SimConfig {
            profile: BackendProfile {
                staged_put: true,
                ..SimConfig::default().profile
            },
            ..SimConfig::default()
        };
        let (store, a, _) = two_clients(config);
        let mut staged = a.begin_staged_put("packs").unwrap().unwrap();
        staged.append(b"ab").unwrap();
        staged.append(b"cd").unwrap();
        assert!(store.keys("").is_empty());
        staged.commit("packs/00/p1").unwrap();
        assert_eq!(store.object("packs/00/p1").unwrap().data, b"abcd");
        assert_eq!(last(&store).op, OpKind::StagedCommit);

        store
            .add_fault(FaultRule::new(
                OpMatch::Kind(OpKind::StagedCommit),
                KeyMatch::Any,
                FaultAction::Fail,
            ))
            .unwrap();
        let mut staged = a.begin_staged_put("packs").unwrap().unwrap();
        staged.append(b"x").unwrap();
        assert!(staged.commit("packs/00/p2").is_err());
        assert!(store.object("packs/00/p2").is_none());

        let (_, plain, _) = two_clients(SimConfig::default());
        assert!(plain.begin_staged_put("packs").unwrap().is_none());
    }

    #[test]
    fn batch_delete() {
        let (store, a, _) = two_clients(SimConfig::default());
        let keys = vec!["a".to_string(), "b".to_string()];
        assert!(a.batch_delete().is_none());

        let config = SimConfig {
            batch_delete: true,
            ..SimConfig::default()
        };
        let (store2, a2, _) = two_clients(config);
        a2.put("a", b"1").unwrap();
        a2.put("c", b"3").unwrap();
        let before = store2.log().len();
        a2.batch_delete()
            .expect("batch delete offered")
            .delete_keys(&keys, BatchDeleteScope::KeysOnly)
            .unwrap();
        assert_eq!(store2.keys(""), vec!["c"]);
        let log = store2.log();
        assert_eq!(log.len(), before + 1);
        assert_eq!(log[before].op, OpKind::BatchDelete);
        assert_eq!(log[before].keys, keys);
        assert_eq!(log[before].absent, vec!["b".to_string()]);
        assert_eq!(log[before].deleted_keys(), vec!["a"]);
        drop(store);
    }

    #[test]
    fn batch_delete_naming_a_key_twice_deletes_it_once() {
        let config = SimConfig {
            batch_delete: true,
            ..SimConfig::default()
        };
        let (store, a, _) = two_clients(config);
        a.put("a", b"1").unwrap();
        let keys = vec!["a".to_string(), "a".to_string()];
        a.batch_delete()
            .expect("batch delete offered")
            .delete_keys(&keys, BatchDeleteScope::KeysOnly)
            .unwrap();
        assert!(store.object("a").is_none());
        let rec = last(&store);
        assert!(rec.absent.is_empty());
        assert_eq!(rec.deleted_keys(), vec!["a"]);
    }

    #[test]
    fn delete_of_absent_key_deletes_nothing() {
        let (store, a, _) = two_clients(SimConfig::default());
        a.put("k", b"v").unwrap();
        a.delete("k").unwrap();
        assert_eq!(last(&store).deleted_keys(), vec!["k"]);
        a.delete("k").unwrap();
        let rec = last(&store);
        assert!(rec.applied());
        assert_eq!(rec.absent, vec!["k".to_string()]);
        assert!(rec.deleted_keys().is_empty());

        // A late duplicate of a delete finds the key already gone.
        a.put("d", b"v").unwrap();
        store
            .add_fault(
                FaultRule::new(
                    OpMatch::Kind(OpKind::Delete),
                    KeyMatch::Exact("d".into()),
                    FaultAction::Duplicate,
                )
                .client(a.client()),
            )
            .unwrap();
        a.delete("d").unwrap();
        assert_eq!(last(&store).deleted_keys(), vec!["d"]);
        store.release_all();
        assert!(last(&store).deleted_keys().is_empty());
    }

    #[test]
    fn discard_drops_parked_effect() {
        let (store, a, _) = two_clients(SimConfig::default());
        store
            .add_fault(FaultRule::put("k", FaultAction::Delay).client(a.client()))
            .unwrap();
        assert!(a.put("k", b"v").is_err());
        let id = store.parked()[0].id;
        store.discard(id).unwrap();
        assert!(store.parked().is_empty());
        assert!(store.object("k").is_none());
        assert!(store.discard(id).is_err());
        assert!(store.release(id).is_err());
    }

    #[test]
    fn observer_unlogged_and_read_only() {
        let (store, a, _) = two_clients(SimConfig::default());
        a.put("k", b"v").unwrap();
        let before = store.log().len();
        let obs = store.observer();
        assert_eq!(obs.get("k").unwrap().as_deref(), Some(&b"v"[..]));
        assert!(obs.put("k", b"w").is_err());
        assert!(obs.delete("k").is_err());
        assert_eq!(store.log().len(), before);
        assert_eq!(store.object("k").unwrap().data, b"v");
    }

    #[test]
    fn op_log_fields() {
        let (store, a, b) = two_clients(SimConfig::default());
        store.set_current_run(a.client(), Some(7));
        a.put("k", b"abc").unwrap();
        store.set_current_run(a.client(), None);
        b.get("k").unwrap();
        b.get_range("k", 0, 2).unwrap();
        let log = store.log();
        assert!(log.windows(2).all(|w| w[0].step < w[1].step));
        assert_eq!(log[0].client, a.client());
        assert_eq!(log[0].run, Some(7));
        assert_eq!(log[0].op, OpKind::Put);
        assert_eq!(log[0].key, "k");
        assert_eq!(log[0].len, Some(3));
        assert_eq!(log[0].outcome, OpOutcome::Ok);
        assert_eq!(log[1].client, b.client());
        assert_eq!(log[1].run, None);
        assert_eq!(log[1].op, OpKind::Get);
        assert_eq!(log[1].len, Some(3));
        assert_eq!(log[2].op, OpKind::GetRange);
        assert_eq!(log[2].len, Some(2));
        assert!(!log[0].thread.is_empty());
    }

    #[test]
    fn server_ops_unsupported() {
        let (_, a, _) = two_clients(SimConfig::default());
        assert!(a.server_ops().is_none());
        assert!(a.batch_delete().is_none());
        assert_eq!(
            a.remove_stale_temp_files(std::time::Duration::ZERO)
                .unwrap(),
            0
        );
    }

    const WAIT: std::time::Duration = std::time::Duration::from_secs(10);

    fn put_gate(store: &SimStore, client: ClientId, key: KeyMatch) -> GateId {
        store
            .add_gate(GateRule::new(client, OpMatch::AnyWrite, key))
            .unwrap()
    }

    #[test]
    fn gate_holds_op_before_it_applies() {
        let (store, a, b) = two_clients(SimConfig::default());
        let gate = put_gate(&store, a.client(), KeyMatch::Exact("k".into()));
        let handle = std::thread::spawn({
            let a = a.clone();
            move || a.put("k", b"v")
        });
        let held = store.wait_gate(gate, WAIT).expect("gate reached");
        assert_eq!(
            (held.op, held.key.as_str(), held.client),
            (OpKind::Put, "k", a.client())
        );
        let step = store.step();
        assert!(store.object("k").is_none());
        assert!(store.log().iter().all(|r| r.key != "k"));
        assert_eq!(step, held.at_step);
        b.put("j", b"w").unwrap();
        assert_eq!(store.step(), step + 1);
        store.resume_gate(gate).unwrap();
        handle.join().unwrap().unwrap();
        assert!(store.object("k").is_some());
        assert_eq!(last(&store).key, "k");
        assert!(store.gate_paused(gate).is_none());
    }

    #[test]
    fn gate_counts_from_creation_and_fires_once() {
        let (store, a, _) = two_clients(SimConfig::default());
        a.put("x", b"0").unwrap();
        let gate = store
            .add_gate(
                GateRule::new(a.client(), OpMatch::AnyWrite, KeyMatch::Prefix("x".into())).nth(2),
            )
            .unwrap();
        a.put("x1", b"1").unwrap();
        let handle = std::thread::spawn({
            let a = a.clone();
            move || a.put("x2", b"2")
        });
        assert_eq!(store.wait_gate(gate, WAIT).expect("held").key, "x2");
        store.resume_gate(gate).unwrap();
        handle.join().unwrap().unwrap();
        a.put("x3", b"3").unwrap();
        assert!(store.paused().is_empty());
    }

    #[test]
    fn resume_before_reached_disarms() {
        let (store, a, _) = two_clients(SimConfig::default());
        let gate = put_gate(&store, a.client(), KeyMatch::Any);
        assert!(store.has_armed_gate(a.client()));
        store.resume_gate(gate).unwrap();
        assert!(!store.has_armed_gate(a.client()));
        a.put("k", b"v").unwrap();
        assert!(store.resume_gate(gate).is_err());
        assert!(store.wait_gate(gate, WAIT).is_none());
    }

    #[test]
    fn crash_while_held_fails_on_resume() {
        let (store, a, _) = two_clients(SimConfig::default());
        let gate = put_gate(&store, a.client(), KeyMatch::Exact("k".into()));
        let handle = std::thread::spawn({
            let a = a.clone();
            move || a.put("k", b"v")
        });
        store.wait_gate(gate, WAIT).expect("held");
        assert!(store.client_paused(a.client()));
        store.crash(a.client());
        store.resume_gate(gate).unwrap();
        let err = handle.join().unwrap().unwrap_err().to_string();
        assert!(err.contains("crashed"), "{err}");
        assert_eq!(last(&store).outcome, OpOutcome::Crashed);
        assert!(store.object("k").is_none());
    }

    #[test]
    fn gates_ignore_observer_and_other_clients() {
        let (store, a, b) = two_clients(SimConfig::default());
        b.put("k", b"v").unwrap();
        let gate = store
            .add_gate(GateRule::new(a.client(), OpMatch::Any, KeyMatch::Any))
            .unwrap();
        b.put("k", b"w").unwrap();
        b.get("k").unwrap();
        store.observer().get("k").unwrap();
        assert!(store.gate_armed(gate));
        assert!(store.paused().is_empty());
        assert!(
            store
                .add_gate(GateRule::new(OBSERVER, OpMatch::Any, KeyMatch::Any))
                .is_err()
        );
        assert!(
            store
                .add_gate(GateRule::new(a.client(), OpMatch::Any, KeyMatch::Any).nth(0))
                .is_err()
        );
    }

    fn of_kind(kind: StoreKind) -> (SimStore, SimClient, SimClient) {
        two_clients(SimConfig {
            kind,
            ..SimConfig::default()
        })
    }

    #[test]
    fn put_if_absent_creates_once() {
        let (store, a, b) = of_kind(StoreKind::Conditional);
        let cond = a.conditional().expect("offered");
        assert_eq!(cond.put_if_absent("index.h/2", b"a").unwrap(), Created::Yes);
        assert_eq!(last(&store).op, OpKind::PutIfAbsent);
        assert_eq!(last(&store).outcome, OpOutcome::Ok);
        let cond_b = b.conditional().expect("offered");
        assert_eq!(
            cond_b.put_if_absent("index.h/2", b"b").unwrap(),
            Created::AlreadyExists
        );
        assert_eq!(last(&store).outcome, OpOutcome::Exists);
        assert!(!last(&store).applied());
        assert_eq!(store.object("index.h/2").unwrap().data, b"a");
        // Each handle ran the check once: its probe is gone.
        assert!(store.keys("locks/").is_empty());
        let probes = store
            .log()
            .iter()
            .filter(|r| r.key.starts_with("locks/vykar-probe-") && r.op == OpKind::PutIfAbsent)
            .count();
        assert_eq!(probes, 4, "two creates per handle's check");
        assert!(store.head_overwrites().is_empty());
    }

    #[test]
    fn released_create_finds_the_key() {
        let (store, a, b) = of_kind(StoreKind::Conditional);
        a.conditional().unwrap().verify().unwrap();
        store
            .add_fault(
                FaultRule::new(
                    OpMatch::Kind(OpKind::PutIfAbsent),
                    KeyMatch::Exact("index.h/2".into()),
                    FaultAction::Delay,
                )
                .client(a.client()),
            )
            .unwrap();
        assert!(
            a.conditional()
                .unwrap()
                .put_if_absent("index.h/2", b"a")
                .is_err()
        );
        b.conditional()
            .unwrap()
            .put_if_absent("index.h/2", b"b")
            .unwrap();
        store.release_all();
        assert!(matches!(
            last(&store).outcome,
            OpOutcome::ReleasedRefused(_)
        ));
        assert_eq!(store.object("index.h/2").unwrap().data, b"b");
    }

    #[test]
    fn conditional_kind_refuses_a_head_overwrite() {
        let (store, a, _) = of_kind(StoreKind::Conditional);
        a.put("index.h/2", b"one").unwrap();
        a.put("index.h/2", b"one").unwrap();
        let err = a.put("index.h/2", b"two").unwrap_err().to_string();
        assert!(err.contains("409"), "{err}");
        assert_eq!(last(&store).outcome, OpOutcome::Refused);
        assert_eq!(store.object("index.h/2").unwrap().data, b"one");
        assert!(store.head_overwrites().is_empty());
        // Tampering bypasses it, and is recorded.
        store.tamper_put("index.h/2", b"three");
        assert_eq!(store.head_overwrites().len(), 1);
    }

    #[test]
    fn plain_kind_records_a_head_overwrite() {
        let (store, a, _) = of_kind(StoreKind::Plain);
        assert!(a.conditional().is_none());
        a.put("index.h/2", b"one").unwrap();
        a.put("index.h/2", b"two").unwrap();
        assert_eq!(store.object("index.h/2").unwrap().data, b"two");
        let overwrites = store.head_overwrites();
        assert_eq!(overwrites.len(), 1);
        assert_eq!(overwrites[0].0, "index.h/2");
        assert_eq!(overwrites[0].2, a.client());
    }

    #[test]
    fn ignoring_kind_fails_the_check() {
        let (store, a, _) = of_kind(StoreKind::Ignoring);
        let cond = a.conditional().expect("offered before the check");
        assert!(matches!(
            cond.verify().unwrap(),
            ConditionalSupport::Unsupported { .. }
        ));
        assert!(a.conditional().is_none());
        assert!(store.keys("locks/").is_empty());
    }

    #[test]
    fn racy_release_overwrites() {
        let (store, a, b) = of_kind(StoreKind::RacyRelease);
        assert_eq!(
            a.conditional().unwrap().verify().unwrap(),
            ConditionalSupport::Verified
        );
        store
            .add_fault(
                FaultRule::new(
                    OpMatch::Kind(OpKind::PutIfAbsent),
                    KeyMatch::Exact("index.h/2".into()),
                    FaultAction::Delay,
                )
                .client(a.client()),
            )
            .unwrap();
        assert!(
            a.conditional()
                .unwrap()
                .put_if_absent("index.h/2", b"a")
                .is_err()
        );
        assert_eq!(
            b.conditional()
                .unwrap()
                .put_if_absent("index.h/2", b"b")
                .unwrap(),
            Created::Yes
        );
        store.release_all();
        assert!(matches!(last(&store).outcome, OpOutcome::Released(_)));
        assert_eq!(store.object("index.h/2").unwrap().data, b"a");
        assert_eq!(store.head_overwrites().len(), 1);
    }

    /// Local, SFTP, S3: a create of an existing key is refused, a plain PUT
    /// of a head replaces it (recorded).
    #[test]
    fn conditional_overwrite_replaces_a_head_on_a_plain_put() {
        let (store, a, b) = of_kind(StoreKind::ConditionalOverwrite);
        assert!(StoreKind::ConditionalOverwrite.honours_creates());
        let cond = a.conditional().expect("offered");
        assert_eq!(cond.verify().unwrap(), ConditionalSupport::Verified);
        assert_eq!(cond.put_if_absent("index.h/2", b"a").unwrap(), Created::Yes);
        assert_eq!(
            b.conditional()
                .unwrap()
                .put_if_absent("index.h/2", b"b")
                .unwrap(),
            Created::AlreadyExists
        );
        assert_eq!(store.object("index.h/2").unwrap().data, b"a");
        assert!(store.head_overwrites().is_empty());
        b.put("index.h/2", b"c").unwrap();
        assert_eq!(store.object("index.h/2").unwrap().data, b"c");
        let overwrites = store.head_overwrites();
        assert_eq!(overwrites.len(), 1);
        assert_eq!(overwrites[0].2, b.client());
    }

    /// Local, SFTP, S3 rewrite an object PUT again with the same bytes, so its
    /// mtime and writer are renewed (vykar-server and every other kind keep
    /// them); the replacement is no head overwrite. A marker PUT flagged
    /// `committing` is recorded for the run of its handle's client.
    #[test]
    fn conditional_overwrite_renews_an_identical_re_put() {
        for (kind, renews) in [
            (StoreKind::ConditionalOverwrite, true),
            (StoreKind::Conditional, false),
            (StoreKind::Plain, false),
        ] {
            let (store, a, b) = of_kind(kind);
            let object = format!("index.d/{}", "ab".repeat(32));
            for key in ["index.h/2", object.as_str()] {
                a.put(key, b"same").unwrap();
                let first = store.object(key).unwrap().mtime;
                store.advance_clock(std::time::Duration::from_secs(60));
                b.put(key, b"same").unwrap();
                let obj = store.object(key).unwrap();
                assert_eq!(obj.mtime != first, renews, "{kind:?} {key}");
                assert_eq!(obj.writer == b.client(), renews, "{kind:?} {key}");
            }
            assert!(store.head_overwrites().is_empty(), "{kind:?}");
        }
        let (store, a, _) = of_kind(StoreKind::ConditionalOverwrite);
        store.set_current_run(a.client(), Some(3));
        a.put("sessions/s.1.json", br#"{"committing":false}"#)
            .unwrap();
        assert!(!store.committed_lock_free_in_run(3));
        a.put("sessions/s.2.json", br#"{"committing":true}"#)
            .unwrap();
        assert!(store.committed_lock_free_in_run(3));
        assert!(!store.committed_lock_free_in_run(4));
    }

    /// A handle without put-if-absent beside one with it, on one store;
    /// a lock client's every handle has none.
    #[test]
    fn handle_without_conditional_offers_no_put_if_absent() {
        for kind in [
            StoreKind::Conditional,
            StoreKind::ConditionalOverwrite,
            StoreKind::RacyRelease,
        ] {
            let (store, a, b) = of_kind(kind);
            assert!(
                store
                    .handle_without_conditional(a.client())
                    .conditional()
                    .is_none()
            );
            assert!(a.conditional().is_some(), "{kind:?}");
            store.set_lock_client(b.client());
            assert!(store.handle(b.client()).conditional().is_none(), "{kind:?}");
            assert!(store.handle(a.client()).conditional().is_some(), "{kind:?}");
            // A fork forgets its lock clients, as it forgets runs.
            assert!(store.fork().handle(b.client()).conditional().is_some());
        }
    }

    #[test]
    fn fork_as_changes_the_kind() {
        let (store, a, _) = of_kind(StoreKind::Plain);
        a.put("k", b"v").unwrap();
        let fork = store.fork_as(StoreKind::Conditional);
        assert_eq!(fork.config().kind, StoreKind::Conditional);
        assert_eq!(fork.keys(""), vec!["k"]);
        assert_eq!(store.fork().config().kind, StoreKind::Plain);
    }

    #[test]
    fn fork_is_independent() {
        let (store, a, _) = two_clients(SimConfig::default());
        a.put("k", b"v").unwrap();
        store.advance_clock(std::time::Duration::from_secs(5));
        let fork = store.fork();
        assert!(fork.log().is_empty());
        assert_eq!(fork.now(), store.now());
        assert_eq!(fork.step(), store.step());
        assert_eq!(fork.client_name(a.client()), "a");
        let fa = fork.handle(a.client());
        fa.put("only-fork", b"x").unwrap();
        fa.delete("k").unwrap();
        assert!(store.object("only-fork").is_none());
        assert!(store.object("k").is_some());
        assert_eq!(fork.keys(""), vec!["only-fork"]);
    }
}
