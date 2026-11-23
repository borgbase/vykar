//! Bounded exhaustive search over the driver (08 step 5): every schedule of
//! a small scenario with at most K preemptions, and a gate-stepped search
//! over two raw lock acquisitions. The expectations are exact: a clean
//! scenario must stay clean, and so must the one-hostname pair since 04
//! step 1 (installation ID) removed H1. The scenario builders and the
//! campaign's list are shared with `tests::sim_campaign`.

// The searches print their counts for the commit message and for anyone
// re-measuring the bound; every other path asserts.
#![allow(clippy::print_stderr)]

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use crate::sim::driver::*;
use crate::sim::explore::{Bound, Control, Exploration, Point, Search, dfs, explore};
use crate::sim::history::{AllowedError, ErrorClass};
use crate::sim::{Call, ClientId, GateId, GateRule, KeyMatch, OpKind, OpMatch, RunRecord, Sim};

/// Preemption bound of the pre-commit driver searches. K = 2 (08's
/// default) takes about 8 s for the three searches here, against 1.1 s at
/// K = 1 and a budget of 3 s; the campaign runs K = 2 and K = 3.
pub(super) const PRECOMMIT_K: u32 = 1;

/// Every pre-commit search stays under this many runs; the tests assert the
/// search completed, so a bound that silently stopped exploring fails.
pub(super) const PRECOMMIT_MAX_RUNS: usize = 5_000;

/// Preemption bound of the pre-commit gate-stepped lock search: K = 3, the
/// least that reaches both clients backing off, costs well under a second.
const GATE_K: u32 = 3;

pub(super) fn backup() -> Command {
    Command::Backup {
        shared: true,
        fold: false,
    }
}

fn base(hosts: HostPlan, clients: Vec<Vec<Command>>) -> Case {
    Case {
        hosts,
        clients,
        faults: vec![],
        crash: false,
        advances: false,
        choices: vec![],
        policy: Policy::Continue,
        skews: vec![],
        maintenance: Maintenance::Whole,
    }
}

/// `c` with its compact gate-stepped (one decision per protocol-key
/// operation).
pub(super) fn stepped(c: Case) -> Case {
    Case {
        maintenance: Maintenance::Stepped,
        ..c
    }
}

/// Two backups.
pub(super) fn lock_pair(hosts: HostPlan) -> Case {
    base(hosts, vec![vec![backup()], vec![backup()]])
}

/// a: one backup; b: delete c0, then compact.
pub(super) fn commit_vs_compact(hosts: HostPlan) -> Case {
    base(
        hosts,
        vec![
            vec![backup()],
            vec![Command::DeleteControl, Command::Compact],
        ],
    )
}

/// a and b: one backup each; c: delete c0, then compact. Distinct hosts.
pub(super) fn three_clients() -> Case {
    base(
        HostPlan::Distinct,
        vec![
            vec![backup()],
            vec![backup()],
            vec![Command::DeleteControl, Command::Compact],
        ],
    )
}

/// One fault on one operation (08's "fault placement"): client a or b, any
/// protocol key class, any kind, on its first write of that class.
pub(super) fn fault_plans() -> Vec<FaultSpec> {
    let mut out = Vec::new();
    for client in 0..2u8 {
        for class in [
            KeyClass::IndexHead,
            KeyClass::IndexObject,
            KeyClass::Snapshot,
            KeyClass::Lock,
            KeyClass::Session,
        ] {
            for kind in [
                FaultKind::Fail,
                FaultKind::FailApplied,
                FaultKind::Delay,
                FaultKind::Duplicate,
            ] {
                out.push(FaultSpec {
                    client,
                    class,
                    nth: 1,
                    kind,
                });
            }
        }
    }
    out
}

/// The campaign's searches, cheapest first: (name, base, K).
pub(super) fn campaign_scenarios() -> Vec<(String, Case, u32)> {
    let bases = [
        ("pair-distinct", lock_pair(HostPlan::Distinct)),
        ("pair-hostname", lock_pair(HostPlan::SameHostname)),
        ("compact-distinct", commit_vs_compact(HostPlan::Distinct)),
        (
            "compact-hostname",
            commit_vs_compact(HostPlan::SameHostname),
        ),
        ("three-clients", three_clients()),
    ];
    // Every base at K = 2 (08's default, wider than pre-commit's
    // PRECOMMIT_K), and the distinct pair, the cheapest, at K = 3.
    let mut out: Vec<(String, Case, u32)> = bases
        .iter()
        .map(|(name, c)| (format!("{name}-k2"), c.clone(), 2))
        .collect();
    out.push(("pair-distinct-k3".into(), bases[0].1.clone(), 3));
    // The compact bases with compact gate-stepped, at K = 2, and at K = 1
    // with time jumps (none inside the stepped compact, see `Maintenance`)
    // and with crashes: the cases that exercise compact beside live
    // backups, I8 and the journal rule (04 step 7).
    for (name, c) in bases.iter().filter(|(n, _)| !n.starts_with("pair")) {
        out.push((format!("{name}-stepped-k2"), stepped(c.clone()), 2));
        let mut jumps = stepped(c.clone());
        jumps.advances = true;
        out.push((format!("{name}-stepped-advances-k1"), jumps, 1));
        let mut crash = stepped(c.clone());
        crash.crash = true;
        out.push((format!("{name}-stepped-crash-k1"), crash, 1));
    }
    for (name, c) in &bases {
        let mut c = c.clone();
        c.advances = true;
        out.push((format!("{name}-advances-k1"), c, 1));
    }
    for (name, c) in &bases {
        for f in fault_plans() {
            let mut c = c.clone();
            let label = format!(
                "{name}-fault-{}-{:?}-{:?}-k1",
                ["a", "b"][usize::from(f.client)],
                f.class,
                f.kind
            );
            c.faults = vec![f];
            out.push((label, c, 1));
        }
    }
    for f in fault_plans() {
        let label = format!(
            "compact-distinct-stepped-fault-{}-{:?}-{:?}-k1",
            ["a", "b"][usize::from(f.client)],
            f.class,
            f.kind
        );
        let mut c = stepped(commit_vs_compact(HostPlan::Distinct));
        c.faults = vec![f];
        out.push((label, c, 1));
    }
    out
}

fn print_search(name: &str, e: &Exploration) {
    let sigs: BTreeMap<Signature, usize> =
        e.signatures.iter().map(|(s, (n, _))| (*s, *n)).collect();
    eprintln!(
        "{name}: {} runs (complete {}, max depth {}), {} clean, signatures {sigs:?}, {:?}",
        e.search.runs, e.search.complete, e.search.max_depth, e.clean, e.elapsed
    );
}

/// Write the trace of `case` where it outlives the test, and return the
/// path and the rendered trace.
fn kept_trace(t: &Template, name: &str, case: &Case) -> String {
    let o = run_case(t, case);
    let dir = std::env::temp_dir().join("vykar-sim-exhaustive");
    let path = o.trace.write_in(&dir, name).map_or_else(
        |e| format!("(not written: {e})"),
        |p| p.display().to_string(),
    );
    format!("trace: {path}\n{}", o.trace.render())
}

fn search(t: &Template, c: &Case, stop_on_damage: bool) -> Exploration {
    explore(
        t,
        c,
        Bound {
            preemptions: PRECOMMIT_K,
            max_runs: PRECOMMIT_MAX_RUNS,
        },
        |o| {
            if stop_on_damage && !o.signatures.is_empty() {
                Control::Stop
            } else {
                Control::Continue
            }
        },
    )
}

/// Every schedule of `c` within the bound is clean.
fn assert_clean(name: &str, c: &Case) {
    let t = Template::new();
    let e = search(&t, c, true);
    print_search(name, &e);
    if let Some((sig, (_, case))) = e.signatures.iter().next() {
        panic!(
            "{name}: a schedule shows {sig:?}:\n{}",
            kept_trace(&t, name, case)
        );
    }
    assert!(e.search.complete, "{name}: the search did not complete");
    assert_eq!(e.clean, e.search.runs);
}

#[test]
fn lock_pair_on_distinct_hosts_is_clean() {
    assert_clean("pair-distinct", &lock_pair(HostPlan::Distinct));
}

/// On one hostname (cloned machines: one installation, other boot IDs) the
/// second backup's registration no longer reaps the first one's live lock
/// (H1 before 04 step 1): every schedule is clean.
#[test]
fn lock_pair_on_one_hostname_is_clean() {
    assert_clean("pair-hostname", &lock_pair(HostPlan::SameHostname));
}

/// A backup against delete + compact, compact one protocol-key operation at
/// a time (`Maintenance::Stepped`) against every boundary of the backup:
/// before its registration (compact removes c0's chunks, the backup
/// uploads the shared file anew); between upload and lock compact
/// proceeds (04 step 7): it keeps the backup's uploads (young, and inside
/// its horizon) and, when the delete of c0 came first, sweeps c0's
/// chunks, so the backup fails its reuse re-check as
/// `StaleChunksDuringCommit` (allowed), or commits when nothing it reused
/// was swept; a backup whose registration probe meets compact's lock fails
/// `Locked`; inside the commit window compact fails `Locked`; after the
/// finish. The driver never refreshes markers, so a search exercises the
/// age guard and I8, not the horizon
/// (`tests::sim::a_refreshed_marker_keeps_uploads_older_than_t_pack` does).
/// The same search with time jumps (none inside the stepped compact) is
/// campaign-only (`compact-distinct-stepped-advances-k1`): 297 runs, all
/// clean, in about 3.7 s, over the pre-commit budget.
#[test]
fn backup_against_compact_is_clean() {
    assert_clean(
        "compact-distinct-stepped",
        &stepped(commit_vs_compact(HostPlan::Distinct)),
    );
}

/// Most `locks/` ops one client makes in one raw acquisition, release
/// included (about five are expected); one more is a harness bug.
const MAX_LOCK_OPS: usize = 12;

/// Real time to wait for a client's next `locks/` op or its end.
const GATE_WAIT: Duration = Duration::from_secs(60);

#[derive(Default, Debug)]
struct LockPairTally {
    schedules: usize,
    /// Both clients inside the lock at once.
    double_holds: usize,
    /// Both runs ended Locked.
    both_backed_off: usize,
    /// Exactly one run ended Ok.
    one_held: usize,
    /// Both clients wrote a lock key and one backed off after its own.
    contended_puts: usize,
    first_double: Option<Vec<usize>>,
}

/// A client of the gate search: its gates, the next one, and whether it is
/// held at it or done.
struct Contender {
    id: ClientId,
    gates: Vec<GateId>,
    next: usize,
    held: bool,
}

impl Contender {
    /// Wait for the client's next `locks/` op (held) or its end.
    fn wait(&mut self, sim: &Sim) {
        assert!(
            self.next < MAX_LOCK_OPS,
            "more than {MAX_LOCK_OPS} locks/ ops by {}",
            sim.store().client_name(self.id)
        );
        self.held = sim
            .poll_paused(self.gates[self.next], GATE_WAIT)
            .unwrap_or_else(|e| panic!("harness: {e}"))
            .is_some();
    }
}

/// Two clients each run a no-op under the repo lock. Every op of both on a
/// lock key or a listing of `locks/` is held at a gate (a contended
/// pre-check's clock probe PUT and DELETE are not: no lock reader lists a
/// probe, so their order does not matter; its listing, by which the
/// pre-check judges, is gated); the search picks which held op proceeds next.
/// Continuing the client resumed last is free while it is held; when it is
/// done (or at the start) every held client is free; anything else is a
/// preemption.
fn lock_acquisitions(hosts: HostPlan, bound: Bound) -> (Search, LockPairTally) {
    let template = Template::new();
    let mut tally = LockPairTally::default();
    let search = dfs(bound, |prefix| {
        let mut sim = template.fork();
        let ids = ["a", "b"].map(|name| match hosts {
            HostPlan::Distinct => sim.client(name),
            HostPlan::SameHostname => sim.client_on(name, "ubuntu", Some(&format!("boot-{name}"))),
            HostPlan::LateClone => unreachable!("the lock-pair search places no late clone"),
        });
        let inside: Arc<Mutex<Vec<(ClientId, u64)>>> = Arc::default();
        let mut cs: Vec<Contender> = ids
            .iter()
            .map(|&id| Contender {
                id,
                gates: (1..=MAX_LOCK_OPS as u32)
                    .map(|n| sim.gate(GateRule::new(id, OpMatch::Any, KeyMatch::LockKey).nth(n)))
                    .collect(),
                next: 0,
                held: false,
            })
            .collect();
        let mut spawned = Vec::new();
        for c in &mut cs {
            let (store, inside, id) = (sim.store().clone(), Arc::clone(&inside), c.id);
            let f: crate::sim::CustomFn = Box::new(move |_| {
                inside.lock().unwrap().push((id, store.step()));
                Ok(())
            });
            spawned.push(sim.spawn(
                c.id,
                Call::Custom {
                    label: "hold".into(),
                    f,
                },
            ));
            // Spawning asserts every other run is held or done.
            c.wait(&sim);
        }

        let mut points = Vec::new();
        let mut last: Option<usize> = None;
        loop {
            let held: Vec<usize> = (0..cs.len()).filter(|&i| cs[i].held).collect();
            if held.is_empty() {
                break;
            }
            let continuing = last.filter(|&i| cs[i].held);
            let costs: Vec<u32> = held
                .iter()
                .map(|&i| u32::from(continuing.is_some_and(|c| c != i)))
                .collect();
            let taken = prefix
                .get(points.len())
                .copied()
                .unwrap_or_else(|| costs.iter().position(|c| *c == 0).expect("a free option"));
            let i = held[taken];
            points.push(Point { costs, taken });
            sim.resume(cs[i].gates[cs[i].next]);
            cs[i].next += 1;
            cs[i].wait(&sim);
            last = Some(i);
        }

        let runs: Vec<RunRecord> = spawned.into_iter().map(|s| sim.join(s)).collect();
        let locked =
            |r: &RunRecord| r.error_class() == Some(&ErrorClass::Allowed(AllowedError::Locked));
        for r in &runs {
            assert!(
                r.is_ok() || locked(r),
                "a raw acquisition ended neither Ok nor Locked: {} / {}",
                runs[0].detail(),
                runs[1].detail()
            );
        }
        let log = sim.store().log();
        let inside = inside.lock().unwrap().clone();
        let interval = |c: ClientId| {
            let (_, s) = *inside.iter().find(|(id, _)| *id == c)?;
            let end = log
                .iter()
                .find(|r| {
                    r.client == c
                        && r.step > s
                        && matches!(r.op, OpKind::Delete | OpKind::BatchDelete)
                        && KeyMatch::LockKey.matches(&r.key)
                })
                .map_or_else(
                    || {
                        runs.iter()
                            .find(|r| r.client == c)
                            .map_or(s, |r| r.last_step)
                    },
                    |r| r.step,
                );
            Some((s, end))
        };
        tally.schedules += 1;
        if let (Some((sa, ra)), Some((sb, rb))) = (interval(ids[0]), interval(ids[1]))
            && sa <= rb
            && sb <= ra
        {
            tally.double_holds += 1;
            if tally.first_double.is_none() {
                tally.first_double = Some(points.iter().map(|p| p.taken).collect());
            }
        }
        let ok = runs.iter().filter(|r| r.is_ok()).count();
        if ok == 0 {
            tally.both_backed_off += 1;
        }
        if ok == 1 {
            tally.one_held += 1;
        }
        let wrote = |c: ClientId| {
            log.iter()
                .any(|r| r.client == c && r.op.is_write() && KeyMatch::LockKey.matches(&r.key))
        };
        if ids.iter().all(|&c| wrote(c)) && runs.iter().any(locked) {
            tally.contended_puts += 1;
        }
        Some(points)
    });
    (search, tally)
}

/// 08's "lock acquire vs lock acquire", operation by operation. On
/// distinct hosts and, since 04 step 1 (installation ID), on one hostname
/// (cloned machines) no schedule puts both inside the lock, and every run
/// ends Ok or Locked: the pre-check of one no longer reaps the other's live
/// key, before or after its confirming LIST (H1 at acquisition level).
/// Both clients backing off (`sim_exclusion::simultaneous_acquires_both_back_off`)
/// needs three preemptions; at K = 2 the check that the search reaches
/// contention is a lock PUT by each client with one backing off.
#[test]
fn lock_acquisitions_race() {
    let bound = Bound {
        preemptions: GATE_K,
        max_runs: PRECOMMIT_MAX_RUNS,
    };
    for hosts in [HostPlan::Distinct, HostPlan::SameHostname] {
        let start = Instant::now();
        let (s, t) = lock_acquisitions(hosts, bound);
        eprintln!(
            "gate search, {hosts:?}, K = {GATE_K}: {s:?} {t:?} in {:?}",
            start.elapsed()
        );
        assert!(s.complete);
        assert_eq!(t.double_holds, 0, "two holders on {hosts:?}: {t:?}");
        assert!(t.one_held > 0, "{t:?}");
        assert!(
            t.contended_puts > 0,
            "the search never reached contention: {t:?}"
        );
        if GATE_K >= 3 {
            assert!(t.both_backed_off > 0, "{t:?}");
        }
    }
}

/// `VYKAR_SIM_MEASURE=1`: every pre-commit driver search at K = 1 and
/// K = 2, and the gate search at K = 2 and 3, with runs and wall time.
#[test]
fn measure_bounds() {
    if std::env::var("VYKAR_SIM_MEASURE").ok().as_deref() != Some("1") {
        return;
    }
    let t = Template::new();
    for k in [1, 2] {
        let mut total = Duration::ZERO;
        for (name, c) in [
            ("pair-distinct", lock_pair(HostPlan::Distinct)),
            ("pair-hostname", lock_pair(HostPlan::SameHostname)),
            ("compact-distinct", commit_vs_compact(HostPlan::Distinct)),
            (
                "compact-distinct-stepped",
                stepped(commit_vs_compact(HostPlan::Distinct)),
            ),
        ] {
            let e = explore(
                &t,
                &c,
                Bound {
                    preemptions: k,
                    max_runs: 100_000,
                },
                |_| Control::Continue,
            );
            print_search(&format!("{name} K = {k}"), &e);
            total += e.elapsed;
        }
        eprintln!("driver searches at K = {k}: {total:?}");
    }
    for k in [2, 3] {
        for hosts in [HostPlan::Distinct, HostPlan::SameHostname] {
            let start = Instant::now();
            let (s, tally) = lock_acquisitions(
                hosts,
                Bound {
                    preemptions: k,
                    max_runs: 100_000,
                },
            );
            eprintln!(
                "gate {hosts:?} K = {k}: {s:?} {tally:?} in {:?}",
                start.elapsed()
            );
        }
    }
}
