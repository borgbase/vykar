//! The deterministic driver over the steppable commit machines (08 step 4):
//! its storage operations stay inside the machine steps they declare, its
//! default schedule is production's, it reproduces the scripted hazards H1
//! and H3, and its traces replay.

// Printing is the point of the env-driven helpers here (projections,
// replays, the measurement); every other path asserts.
#![allow(clippy::print_stderr)]

use std::collections::{BTreeMap, BTreeSet};

use super::commit_op_order::{project, run_records};
use crate::repo::machine::{Access, StepKind};
use crate::sim::checker::{Invariant, NotEvaluated, Violation, ViolationKind};
use crate::sim::driver::*;
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

/// H1 through the driver: B (same hostname, other boot ID) uploads while A
/// is at its head PUT; B's registration reaps A's live lock and A writes
/// its head with no lock (`interleavings::h1_with`). On distinct hosts the
/// same choices leave B `Locked`.
#[test]
fn driver_reproduces_h1_scripted() {
    let t = Template::new();
    let mut c = case(
        HostPlan::SameHostname,
        vec![vec![backup()], vec![backup()]],
        vec![],
    );
    c.advances = false;
    let at = decision_of(&t, &c, 0, "put-head");
    let c = with_choice(&c, at, 1);
    let o = run_case(&t, &c);
    assert!(
        o.signatures.contains(&Signature::TwoLockHolders),
        "{}",
        o.trace.render()
    );

    let mut distinct = c.clone();
    distinct.hosts = HostPlan::Distinct;
    let o = run_case(&t, &distinct);
    assert!(o.report.is_clean(), "{}", o.trace.render());
    let runs = driven_backups(&o);
    let b = runs
        .iter()
        .find(|r| o.sim.store().client_name(r.client) == "b")
        .expect("b ran");
    assert_eq!(
        b.error_class(),
        Some(&crate::sim::ErrorClass::Allowed(
            crate::sim::AllowedError::Locked
        )),
        "{}",
        o.trace.render()
    );
}

/// H1-session through the driver: compact on the same hostname reaps A's
/// live marker (and lock) after A's marker probe; A's commit then fails
/// with the untyped "pack … missing" error. `SessionReaped` claims that
/// error and nothing else: an unrelated violation, or a skipped
/// evaluation, added to the report is left to `Unknown`.
#[test]
fn session_reap_claims_only_its_victim() {
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
    let render = || o.trace.render();
    assert_eq!(
        o.signatures,
        BTreeSet::from([Signature::SessionReaped]),
        "{}",
        render()
    );
    assert!(
        matches!(
            o.report.violations.as_slice(),
            [v] if v.kind == ViolationKind::DisallowedError && v.subject.contains("\"a1\"")
        ),
        "{}",
        render()
    );

    let mut unrelated = o.report.clone();
    unrelated.violations.push(Violation {
        invariant: Invariant::CommittedSnapshotsRestore,
        kind: ViolationKind::RestoreDiffers,
        subject: "c0".into(),
        detail: "injected by the test".into(),
        step: None,
    });
    assert_eq!(
        classify(&o.sim, &unrelated, &o.trace.entries),
        BTreeSet::from([Signature::SessionReaped, Signature::Unknown])
    );

    let mut skipped = o.report.clone();
    skipped.not_evaluated.push(NotEvaluated {
        invariant: Invariant::IndexIntegrity,
        subject: "index".into(),
        reason: "injected by the test".into(),
    });
    assert_eq!(
        classify(&o.sim, &skipped, &o.trace.entries),
        BTreeSet::from([Signature::SessionReaped, Signature::Unknown])
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
