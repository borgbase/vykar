//! The deterministic driver over the steppable commit machines (08 step 4):
//! its storage operations stay inside the machine steps they declare, its
//! default schedule is production's, it reproduces the scripted hazard H3,
//! H1's schedules stay clean since 04 step 1, a late-booted clone still
//! shows `TwoLockHolders` (H1-late-clone) and reaps a live marker, whose
//! victim fails typed since 04 step 2, and its traces replay.

// Printing is the point of the env-driven helpers here (projections,
// replays, the measurement); every other path asserts.
#![allow(clippy::print_stderr)]

use std::collections::{BTreeMap, BTreeSet};

use super::commit_op_order::{project, run_records};
use crate::repo::machine::{Access, StepKind};
use crate::sim::checker::{Invariant, NotEvaluated, Report, Violation, ViolationKind, run_subject};
use crate::sim::driver::*;
use crate::sim::history::{AllowedError, ErrorClass};
use crate::sim::{ClientOp, OpKind, OpOutcome, OpRecord, RunRecord};

fn backup() -> Command {
    Command::Backup {
        shared: true,
        fold: false,
    }
}

fn case(hosts: HostPlan, clients: Vec<Vec<Command>>, faults: Vec<FaultSpec>) -> Case {
    Case {
        hosts,
        clients,
        faults,
        crash: false,
        advances: true,
        choices: vec![],
        policy: Policy::First,
        skews: vec![],
    }
}

fn two_backups() -> Case {
    case(
        HostPlan::Distinct,
        vec![vec![backup()], vec![backup()]],
        vec![],
    )
}

fn access(op: OpKind) -> Access {
    match op {
        OpKind::Get | OpKind::GetRange | OpKind::Exists | OpKind::Size => Access::Read,
        OpKind::List | OpKind::ListSized | OpKind::ListMeta => Access::List,
        OpKind::Delete | OpKind::BatchDelete => Access::Delete,
        OpKind::Put
        | OpKind::PutOwned
        | OpKind::PutPack
        | OpKind::StagedCommit
        | OpKind::CreateDir => Access::Write,
    }
}

/// The first decision at which client `client`'s step is labelled
/// `label`, on the default schedule of `c`.
fn decision_of(t: &Template, c: &Case, client: u8, label: &str) -> usize {
    let o = run_case(t, c);
    o.trace
        .entries
        .iter()
        .position(|e| e.action == (Action::Step { client, pick: 0 }) && e.label.starts_with(label))
        .unwrap_or_else(|| panic!("no {label} step of client {client}:\n{}", o.trace.render()))
}

/// `c` with `choice` at decision `n`, the default before it.
fn with_choice(c: &Case, n: usize, choice: u16) -> Case {
    let mut c = c.clone();
    c.choices.resize(n, 0);
    c.choices.push(choice);
    c
}

/// The backup runs of the driver's clients.
fn driven_backups(o: &Outcome) -> Vec<RunRecord> {
    o.sim
        .history()
        .runs
        .iter()
        .filter(|r| matches!(r.op, ClientOp::Backup { .. }))
        .filter(|r| o.sim.store().client_name(r.client) != "ctl")
        .cloned()
        .collect()
}

/// Every op of a driven backup's commit window (after its lock step,
/// through its snapshot PUT) is issued inside a machine step and is one
/// that step declares; `Local` steps issue none; each head and snapshot
/// PUT step issues exactly one write. Lock steps touch only `locks/`,
/// finish steps only `sessions/` and `locks/`.
fn check_guard(name: &str, o: &Outcome) {
    let log = o.sim.store().log();
    let entry_of = |rec: &OpRecord| {
        o.trace
            .entries
            .iter()
            .position(|e| rec.step > e.steps.0 && rec.step <= e.steps.1)
    };
    let mut checked = 0;
    for run in driven_backups(o) {
        let recs: Vec<&OpRecord> = log
            .iter()
            .filter(|r| r.client == run.client && r.run == Some(run.index))
            .filter(|r| !matches!(r.outcome, OpOutcome::Released(_)))
            .collect();
        let context = || {
            format!(
                "{name}, run {} ({:?}):\n{}",
                run.index,
                run.op,
                o.trace.render()
            )
        };
        // (entry, label) -> ops of that step.
        let mut steps: BTreeMap<(usize, &str), Vec<&OpRecord>> = BTreeMap::new();
        for r in &recs {
            let label = r
                .label
                .unwrap_or_else(|| panic!("unlabelled op {r:?}: {}", context()));
            match label {
                "upload" | "crash" => {}
                "lock" => assert!(
                    r.key.starts_with("locks/"),
                    "lock step touched {}: {}",
                    r.key,
                    context()
                ),
                "finish" => assert!(
                    r.key.starts_with("sessions/") || r.key.starts_with("locks/"),
                    "finish touched {}: {}",
                    r.key,
                    context()
                ),
                step => {
                    let kind = StepKind::from_name(step).unwrap_or_else(|| {
                        panic!("label {step} is no machine step: {}", context())
                    });
                    let a = access(r.op);
                    assert!(
                        kind.may_touch()
                            .iter()
                            .any(|(acc, prefix)| *acc == a && r.key.starts_with(prefix)),
                        "{step} made {:?} {} ({a:?}), not among {:?}: {}",
                        r.op,
                        r.key,
                        kind.may_touch(),
                        context()
                    );
                    let entry = entry_of(r)
                        .unwrap_or_else(|| panic!("op outside every decision: {}", context()));
                    steps.entry((entry, step)).or_default().push(r);
                    checked += 1;
                }
            }
        }
        for ((_, step), ops) in &steps {
            match *step {
                "local" => panic!("a local step issued {} ops: {}", ops.len(), context()),
                "put-head" | "put-snapshot" => assert!(
                    ops.len() == 1 && ops[0].op.is_write(),
                    "{step} issued {:?}: {}",
                    ops.iter().map(|r| (r.op, &r.key)).collect::<Vec<_>>(),
                    context()
                ),
                _ => {}
            }
        }
    }
    assert!(checked > 0, "{name}: no commit-window op checked");
}

#[test]
fn commit_window_ops_stay_in_machine_steps() {
    let t = Template::new();
    let default = two_backups();
    let reads = decision_of(&t, &default, 0, "refresh-snapshots");
    let head_first = with_choice(&default, reads, 1);
    let probe_last = with_choice(&with_choice(&default, reads, 0), reads + 1, 1);
    let fold = case(
        HostPlan::Distinct,
        vec![vec![Command::Backup {
            shared: true,
            fold: true,
        }]],
        vec![],
    );
    let after_compact = case(
        HostPlan::Distinct,
        vec![
            vec![Command::DeleteControl, Command::Compact],
            vec![backup()],
        ],
        vec![],
    );
    let head_fails = case(
        HostPlan::Distinct,
        vec![vec![backup()]],
        vec![FaultSpec {
            client: 0,
            class: KeyClass::IndexHead,
            nth: 1,
            kind: FaultKind::Fail,
        }],
    );
    for (name, c) in [
        ("default", &default),
        ("head first", &head_first),
        ("probe last", &probe_last),
        ("fold", &fold),
        ("after compact", &after_compact),
        ("head fails", &head_fails),
    ] {
        let o = run_case(&t, c);
        check_guard(name, &o);
        if name == "fold" {
            assert!(
                o.trace
                    .entries
                    .iter()
                    .any(|e| e.label.starts_with("put-base")),
                "the fold case took no fold:\n{}",
                o.trace.render()
            );
        }
        if name != "head fails" {
            assert!(o.report.is_clean(), "{name}:\n{}", o.trace.render());
        }
    }
    let orders: Vec<Vec<String>> = [&default, &head_first, &probe_last]
        .iter()
        .map(|c| {
            run_case(&t, c)
                .trace
                .entries
                .iter()
                .map(|e| e.label.clone())
                .collect()
        })
        .collect();
    assert_ne!(orders[0], orders[1], "the head-first order was not taken");
    assert_ne!(orders[0], orders[2], "the probe-last order was not taken");
}

#[test]
fn driver_default_schedule_matches_production() {
    let t = Template::new();
    let mut prod = t.fork();
    let a = prod.client("a");
    let r = prod.backup(a, "a1", &backup_tree("a1", true));
    r.expect_ok();
    let want = project(&run_records(&prod, &r, true));

    let o = run_case(&t, &case(HostPlan::Distinct, vec![vec![backup()]], vec![]));
    let runs = driven_backups(&o);
    assert_eq!(runs.len(), 1);
    assert!(runs[0].is_ok(), "{}", o.trace.render());
    let got = project(&run_records(&o.sim, &runs[0], true));
    assert_eq!(
        got, want,
        "the driver's default schedule differs from production"
    );
}

/// H3 through the driver: A's head PUT is delayed and lands after B
/// committed on top of the old head (`hazards::h3_with(AfterB)`); dropped,
/// it does no harm (`Never`).
#[test]
fn driver_reproduces_h3_scripted() {
    let t = Template::new();
    let c = case(
        HostPlan::Distinct,
        vec![vec![backup()], vec![backup()]],
        vec![FaultSpec {
            client: 0,
            class: KeyClass::IndexHead,
            nth: 1,
            kind: FaultKind::Delay,
        }],
    );
    let stop = run_case(&t, &c);
    assert!(stop.report.is_clean(), "{}", stop.trace.render());
    assert!(stop.signatures.is_empty());
    let last = stop.trace.entries.len() - 1;
    assert_eq!(stop.trace.entries[last].action, Action::Stop);
    assert_eq!(
        stop.trace.entries[last].options,
        2,
        "{}",
        stop.trace.render()
    );
    let release = run_case(&t, &with_choice(&c, last, 1));
    assert!(
        release.signatures.contains(&Signature::LateHeadWrite),
        "{}",
        release.trace.render()
    );
}

/// The b of `o`'s driven backups.
fn backup_of_b(o: &Outcome) -> RunRecord {
    driven_backups(o)
        .into_iter()
        .find(|r| o.sim.store().client_name(r.client) == "b")
        .expect("b ran")
}

fn assert_locked(o: &Outcome, r: &RunRecord) {
    assert_eq!(
        r.error_class(),
        Some(&crate::sim::ErrorClass::Allowed(
            crate::sim::AllowedError::Locked
        )),
        "{}",
        o.trace.render()
    );
}

/// H1's schedule through the driver: B uploads while A is at its head PUT.
/// Before 04 step 1 B's registration on the same hostname (another boot ID)
/// reaped A's live lock and A wrote its head with no lock
/// (`interleavings::h1_with`). Now, on cloned machines as on distinct
/// hosts, the same choices leave B `Locked` and the case clean.
#[test]
fn driver_h1_schedule_is_clean() {
    let t = Template::new();
    let mut c = case(
        HostPlan::SameHostname,
        vec![vec![backup()], vec![backup()]],
        vec![],
    );
    c.advances = false;
    let at = decision_of(&t, &c, 0, "put-head");
    let c = with_choice(&c, at, 1);
    for hosts in [HostPlan::SameHostname, HostPlan::Distinct] {
        let mut c = c.clone();
        c.hosts = hosts;
        let o = run_case(&t, &c);
        assert!(o.signatures.is_empty(), "{}", o.trace.render());
        assert!(o.report.is_clean(), "{}", o.trace.render());
        assert_locked(&o, &backup_of_b(&o));
    }
}

/// H1-session's schedule through the driver: compact runs at A's
/// "fetch-head". Before 04 step 1 compact on the same hostname reaped A's
/// live marker and lock there. Now compact finds A's commit lock held and
/// ends `Locked` (it takes the lock before it looks at sessions), and A
/// commits.
#[test]
fn driver_session_reap_schedule_is_clean() {
    let t = Template::new();
    let mut c = case(
        HostPlan::SameHostname,
        vec![
            vec![Command::Backup {
                shared: false,
                fold: false,
            }],
            vec![Command::Compact],
        ],
        vec![],
    );
    let at = decision_of(&t, &c, 0, "fetch-head");
    c = with_choice(&c, at, 1);
    let o = run_case(&t, &c);
    assert!(o.signatures.is_empty(), "{}", o.trace.render());
    assert!(o.report.is_clean(), "{}", o.trace.render());
    let ra = driven_backups(&o)
        .into_iter()
        .find(|r| o.sim.store().client_name(r.client) == "a")
        .expect("a ran");
    assert!(ra.is_ok(), "{}", o.trace.render());
    let rb = o
        .sim
        .history()
        .runs
        .iter()
        .find(|r| o.sim.store().client_name(r.client) == "b")
        .expect("b ran")
        .clone();
    assert_locked(&o, &rb);
}

/// The victim of a marker reap whose packs compact deleted fails typed:
/// a's run is `Allowed(StaleChunksDuringCommit)` (04 step 2: the pack check
/// fails, the re-probe finds the marker gone), the report is clean and no
/// signature is left. On that report, a `DisallowedError` of a's run is
/// claimed by exactly `sig`.
fn assert_victim_fails_typed(o: &Outcome, sig: Signature) -> Report {
    let render = || o.trace.render();
    let ra = driven_backups(o)
        .into_iter()
        .find(|r| o.sim.store().client_name(r.client) == "a")
        .expect("a ran");
    assert_eq!(
        ra.error_class(),
        Some(&ErrorClass::Allowed(AllowedError::StaleChunksDuringCommit)),
        "{}",
        render()
    );
    assert!(o.signatures.is_empty(), "{}", render());
    assert!(o.report.is_clean(), "{}", render());
    let mut claimed = o.report.clone();
    claimed.violations.push(Violation {
        invariant: Invariant::AllowedErrors,
        kind: ViolationKind::DisallowedError,
        subject: run_subject(&ra),
        detail: "injected by the test".into(),
        step: None,
    });
    assert_eq!(
        classify(&o.sim, &claimed, &o.trace.entries),
        BTreeSet::from([sig]),
        "{}",
        render()
    );
    claimed
}

/// `c` with `jumps` consecutive `Advance { secs: 21660 }` at client a's
/// "fetch-head", then client b's step (the compact).
fn jumps_then_compact(t: &Template, c: &Case, jumps: usize) -> Case {
    let at = decision_of(t, c, 0, "fetch-head");
    let jump = Action::Advance { secs: 21660 };
    let mut c = c.clone();
    c.choices.truncate(at);
    for n in 0..jumps {
        let probe = run_case(t, &c);
        while c.choices.len() < at + n {
            c.choices.push(0);
        }
        let advance = probe.trace.entries[at + n]
            .actions
            .iter()
            .position(|a| *a == jump)
            .unwrap_or_else(|| {
                panic!(
                    "{jump:?} not offered at fetch-head (jump {n}):\n{}",
                    probe.trace.render()
                )
            });
        c.choices.push(u16::try_from(advance).unwrap());
    }
    let after = run_case(t, &c);
    let b_step = after.trace.entries[at + jumps]
        .actions
        .iter()
        .position(|a| matches!(a, Action::Step { client: 1, .. }))
        .unwrap_or_else(|| panic!("no step of b after the jumps:\n{}", after.trace.render()));
    c.choices.push(u16::try_from(b_step).unwrap());
    c
}

/// A marker reaped by age (`AgedSessionReaped`): compact runs after two
/// jumps past both stale ages at A's "fetch-head" (12 h 2 min, so A's
/// packs are older than T_pack: 04 step 5 keeps younger orphans), reaps
/// A's aged lock and marker and deletes its packs, and A's commit fails
/// typed (D9 of 04 step 2). The signature claims an injected disallowed
/// error of A's run and nothing else: an unrelated violation, or a skipped
/// evaluation, added to the report is left to `Unknown`.
#[test]
fn aged_reap_fails_typed_and_claims_only_its_victim() {
    let t = Template::new();
    let mut c = case(
        HostPlan::Distinct,
        vec![
            vec![Command::Backup {
                shared: false,
                fold: false,
            }],
            vec![Command::Compact],
        ],
        vec![],
    );
    c = jumps_then_compact(&t, &c, 2);
    let o = run_case(&t, &c);
    let claimed = assert_victim_fails_typed(&o, Signature::AgedSessionReaped);
    assert!(!Signature::AgedSessionReaped.cascades());

    let mut unrelated = claimed;
    unrelated.violations.push(Violation {
        invariant: Invariant::CommittedSnapshotsRestore,
        kind: ViolationKind::RestoreDiffers,
        subject: "c0".into(),
        detail: "injected by the test".into(),
        step: None,
    });
    assert_eq!(
        classify(&o.sim, &unrelated, &o.trace.entries),
        BTreeSet::from([Signature::AgedSessionReaped, Signature::Unknown])
    );

    let mut skipped = o.report.clone();
    skipped.not_evaluated.push(NotEvaluated {
        invariant: Invariant::IndexIntegrity,
        subject: "index".into(),
        reason: "injected by the test".into(),
    });
    assert_eq!(
        classify(&o.sim, &skipped, &o.trace.entries),
        BTreeSet::from([Signature::Unknown])
    );
}

/// The schedule above with one jump: A's packs are 6 h old at the compact,
/// orphans younger than T_pack, so compact keeps them (04 step 5). A's
/// lock is reaped by age, A writes its segment and then fails at its
/// fence: an allowed error, `AgedLockReaped` for that write, nothing else.
#[test]
fn one_jump_keeps_the_victims_packs() {
    let t = Template::new();
    let c = case(
        HostPlan::Distinct,
        vec![
            vec![Command::Backup {
                shared: false,
                fold: false,
            }],
            vec![Command::Compact],
        ],
        vec![],
    );
    let c = jumps_then_compact(&t, &c, 1);
    let o = run_case(&t, &c);
    let render = || o.trace.render();
    let a = o
        .sim
        .history()
        .runs
        .iter()
        .find(|r| o.sim.store().client_name(r.client) == "a")
        .expect("a ran")
        .client;
    let packs = o.sim.store().keys_written_by(a, "packs/");
    assert!(!packs.is_empty(), "{}", render());
    for k in &packs {
        assert!(
            o.sim.store().object(k).is_some(),
            "{k} deleted\n{}",
            render()
        );
    }
    let ra = driven_backups(&o)
        .into_iter()
        .find(|r| r.client == a)
        .expect("a's backup");
    assert!(
        matches!(ra.error_class(), Some(ErrorClass::Allowed(_))),
        "{}",
        render()
    );
    assert!(
        !o.signatures.is_empty()
            && o.signatures
                .iter()
                .all(|s| matches!(s, Signature::AgedLockReaped | Signature::AgedSessionReaped)),
        "{}",
        render()
    );
}

/// `c` with a jump of 61 s at client a's first step labelled `label`,
/// then client b's steps until b's command ends; past that the default.
/// The jump puts b of [`HostPlan::LateClone`] past its boot, at an uptime
/// shorter than the age of a's entries.
fn jump_then_b(t: &Template, c: &Case, label: &str) -> Case {
    let at = decision_of(t, c, 0, label);
    let probe = run_case(t, c);
    let jump = Action::Advance { secs: 61 };
    let advance = probe.trace.entries[at]
        .actions
        .iter()
        .position(|a| *a == jump)
        .unwrap_or_else(|| panic!("{jump:?} not offered at {label}:\n{}", probe.trace.render()));
    let mut c = with_choice(c, at, u16::try_from(advance).unwrap());
    loop {
        let out = run_case(t, &c);
        let Some(entry) = out.trace.entries.get(c.choices.len()) else {
            panic!(
                "the case ended before b's command did:\n{}",
                out.trace.render()
            );
        };
        let step = entry
            .actions
            .iter()
            .position(|a| *a == (Action::Step { client: 1, pick: 0 }))
            .unwrap_or_else(|| panic!("b has no step:\n{}", out.trace.render()));
        c.choices.push(u16::try_from(step).unwrap());
        // The decision just added ran b's step; its entry says if it ended.
        let out = run_case(t, &c);
        if out.trace.entries[c.choices.len() - 1].ended {
            return c;
        }
    }
}

/// H1-late-clone through the driver: a clone of a's installation that
/// boots after a took its commit lock, then registers a minute later,
/// reaps a's young lock as one of its own earlier boot, and a writes on:
/// exactly `TwoLockHolders`. The same choices on clones booted long before
/// are clean.
#[test]
fn late_clone_reaps_live_lock() {
    let t = Template::new();
    let own = Command::Backup {
        shared: false,
        fold: false,
    };
    let c = case(
        HostPlan::LateClone,
        vec![vec![own.clone()], vec![own]],
        vec![],
    );
    let c = jump_then_b(&t, &c, "fetch-head");
    let o = run_case(&t, &c);
    assert_eq!(
        o.signatures,
        BTreeSet::from([Signature::TwoLockHolders]),
        "{}",
        o.trace.render()
    );
    let mut early = c.clone();
    early.hosts = HostPlan::SameHostname;
    let o = run_case(&t, &early);
    assert!(o.signatures.is_empty(), "{}", o.trace.render());
}

/// H1-late-clone for markers: a late-booted clone compacts at a's
/// "fetch-head" a minute after its boot and reaps a's young marker and lock
/// as its own earlier boot's. Until 04 step 5 it also deleted a's packs
/// and a's commit failed typed (04 step 2). Since then a's packs, a minute
/// old, are orphans younger than T_pack and stay: a writes its segment
/// under the reaped lock and fails at its fence (`LockExpired`), so what
/// is left is the lock half of H1-late-clone, exactly `TwoLockHolders`.
#[test]
fn late_clone_reaps_live_marker() {
    let t = Template::new();
    let c = case(
        HostPlan::LateClone,
        vec![
            vec![Command::Backup {
                shared: false,
                fold: false,
            }],
            vec![Command::Compact],
        ],
        vec![],
    );
    let c = jump_then_b(&t, &c, "fetch-head");
    let o = run_case(&t, &c);
    let render = || o.trace.render();
    let ra = driven_backups(&o)
        .into_iter()
        .find(|r| o.sim.store().client_name(r.client) == "a")
        .expect("a ran");
    assert_eq!(
        ra.error_class(),
        Some(&ErrorClass::Allowed(AllowedError::LockExpired)),
        "{}",
        render()
    );
    for k in o.sim.store().keys_written_by(ra.client, "packs/") {
        assert!(
            o.sim.store().object(&k).is_some(),
            "{k} deleted\n{}",
            render()
        );
    }
    assert_eq!(
        o.signatures,
        BTreeSet::from([Signature::TwoLockHolders]),
        "{}",
        render()
    );
}

#[test]
fn trace_round_trips() {
    let t = Template::new();
    let mut c = two_backups();
    c.faults.push(FaultSpec {
        client: 1,
        class: KeyClass::Snapshot,
        nth: 1,
        kind: FaultKind::Duplicate,
    });
    c.choices = vec![0, 1, 0, 2, 1, 0, 0, 3];
    let o = run_case(&t, &c);
    let text = o.trace.render();
    let parsed = case_from_trace(&text).unwrap();
    assert_eq!(parsed, c);
    let again = run_case(&t, &parsed);
    assert_eq!(again.trace.canonical(), o.trace.canonical());
    assert_eq!(again.signatures, o.signatures);
    let dir = tempfile::tempdir().unwrap();
    let path = o.trace.write_in(dir.path(), "trace-round-trip").unwrap();
    let read = std::fs::read_to_string(path).unwrap();
    assert_eq!(case_from_trace(&read).unwrap(), c);
}

/// `VYKAR_SIM_REPLAY=<trace file>`: run that trace's case and print its
/// trace. A no-op otherwise.
#[test]
fn replay_from_env() {
    let Some(path) = std::env::var_os("VYKAR_SIM_REPLAY") else {
        return;
    };
    let text = std::fs::read_to_string(&path).unwrap();
    let c = case_from_trace(&text).unwrap();
    let o = run_case(&Template::new(), &c);
    eprintln!("{}", o.trace.render());
}
