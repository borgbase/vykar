//! Bounded exhaustive search over the driver (08 step 5): every schedule of
//! a small scenario with at most K preemptions, a gate-stepped search over
//! two raw lock acquisitions, and gate-stepped searches of the
//! single-writer check (10-single-protocol step 7). The expectations are
//! exact: a clean scenario must stay clean, and so must the one-hostname
//! pair since 04 step 1 (installation ID) removed H1. The scenario builders
//! and the campaign's list are shared with `tests::sim_campaign`.
//!
//! Retired by 10-single-protocol step 7 (lock clients' lock-mode commits
//! exist in no binary): `lock_free_backup_against_a_locked_compact_is_clean`
//! (its "locked compact" was a lock client's), `mixed_lock_and_lock_free_backups_are_clean`
//! and `the_marker_handshake_keeps_heads_immutable` with its
//! `handshake_search` (a lock-mode backup beside a lock-free one; the
//! handshake itself still guards the exclusive commands, searched by
//! `tests::lock_free` and `tests::sim_exclusion`).

// The searches print their counts for the commit message and for anyone
// re-measuring the bound; every other path asserts.
#![allow(clippy::print_stderr)]

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use crate::sim::driver::*;
use crate::sim::explore::{Bound, Control, Exploration, Point, Search, dfs, explore};
use crate::sim::history::{AllowedError, ErrorClass};
use crate::sim::{
    Call, ClientId, GateId, GateRule, Invariant, KeyMatch, OpKind, OpMatch, RunRecord, Sim,
};

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
        storage: StoreKind::Plain,
        template: TemplateKind::Encrypted,
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

/// `c` on the conditional store (04 step 8).
pub(super) fn conditional(c: Case) -> Case {
    Case {
        storage: StoreKind::Conditional,
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

/// a: a backup, then delete c0; b: compact. Distinct hosts (phase F part
/// 10d: a delete that lands during a compact's mark).
pub(super) fn delete_during_compact() -> Case {
    base(
        HostPlan::Distinct,
        vec![
            vec![backup(), Command::DeleteControl],
            vec![Command::Compact],
        ],
    )
}

/// a: a backup sharing c0's file, then a compact; b: a compact. Distinct
/// hosts (phase F part 10d: two compacts at once).
pub(super) fn two_compacts() -> Case {
    base(
        HostPlan::Distinct,
        vec![vec![backup(), Command::Compact], vec![Command::Compact]],
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
/// protocol key class but the lock (since 10-single-protocol step 7 no
/// backup or maintenance run of the driver writes a lock key, so a lock
/// fault repeats its no-fault twin), any kind, on its first write of that
/// class.
pub(super) fn fault_plans() -> Vec<FaultSpec> {
    let mut out = Vec::new();
    for client in 0..2u8 {
        for class in [
            KeyClass::IndexHead,
            KeyClass::IndexObject,
            KeyClass::Snapshot,
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

/// The campaign's searches, in the order the campaign hands them out:
/// (name, base, K). The K = 1 scenarios (the faults, time jumps and
/// crashes) take turns by store family (plain, lock-free, then plain
/// again), and the K >= 2 searches are placed at a fixed
/// stride through that list, the first at its head, K = 2 before K = 3 and
/// taking turns by family too. Within one family and K the cheapest
/// variants come first ([`scenario_cost`]). A budget cut short so covers
/// every family and some wider searches, instead of one family's K = 1
/// scenarios alone.
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
    // Phase F part 10d's maintenance bases, on the multi family only (in
    // the single family they add nothing the compact bases above do not): a
    // delete during a stepped compact, and two compacts, each at K = 1 with
    // time jumps (inside the stepped compact too) and at K = 2.
    let mut multi_only: Vec<(String, Case, u32)> = Vec::new();
    for (name, c) in [
        ("delete-during-compact", delete_during_compact()),
        ("two-compacts", two_compacts()),
    ] {
        let c = conditional(stepped(c));
        let mut jumps = c.clone();
        jumps.advances = true;
        multi_only.push((format!("{name}-stepped-advances-k1-multi"), jumps, 1));
        multi_only.push((format!("{name}-stepped-k2-multi"), c, 2));
    }
    // Every scenario on a multi-writer repository too (the conditional
    // store; local, OpenSSH SFTP, AWS S3 and vykar-server all offer
    // put-if-absent, 04 step 8, N6). Families: 0 single (the fallback store:
    // a single-writer repository, 10-single-protocol step 7), 1 multi.
    let mut all: Vec<(u32, usize, (String, Case, u32))> = Vec::new();
    for (name, c, k) in out {
        all.push((k, 1, (format!("{name}-multi"), conditional(c.clone()), k)));
        all.push((k, 0, (name, c, k)));
    }
    for (name, c, k) in multi_only {
        all.push((k, 1, (name, c, k)));
    }
    // Within one (K, family) the cheapest first; stable, so the build order
    // stays among equals.
    all.sort_by_key(|(k, family, (name, _, _))| (*k, *family, scenario_cost(name)));
    let mut narrow: Vec<Vec<(String, Case, u32)>> = vec![Vec::new(); 2];
    let mut wide: Vec<Vec<Vec<(String, Case, u32)>>> = Vec::new();
    for (k, family, s) in all {
        if k == 1 {
            narrow[family].push(s);
        } else {
            let at = usize::try_from(k - 2).expect("small K");
            if wide.len() <= at {
                wide.resize_with(at + 1, || vec![Vec::new(); 2]);
            }
            wide[at][family].push(s);
        }
    }
    let narrow = take_turns(narrow);
    let wide: Vec<_> = wide.into_iter().flat_map(take_turns).collect();
    let stride = ((narrow.len() + wide.len()) / wide.len().max(1)).max(1);
    let (mut narrow, mut wide) = (narrow.into_iter(), wide.into_iter());
    let mut out = Vec::new();
    loop {
        let next = if out.len() % stride == 0 {
            wide.next().or_else(|| narrow.next())
        } else {
            narrow.next().or_else(|| wide.next())
        };
        match next {
            Some(s) => out.push(s),
            None => return out,
        }
    }
}

/// Interleave `families`: the first of each in turn, then the second of
/// each, and so on, skipping a family once it is used up.
fn take_turns<T>(families: Vec<Vec<T>>) -> Vec<T> {
    let mut iters: Vec<_> = families.into_iter().map(Vec::into_iter).collect();
    let mut out = Vec::new();
    loop {
        let before = out.len();
        out.extend(iters.iter_mut().filter_map(Iterator::next));
        if out.len() == before {
            return out;
        }
    }
}

/// A scenario's rough cost from its name, for the campaign's order within
/// one family and K: unstepped before gate-stepped (a stepped compact has
/// far more decision points), then by base: the two pairs, the two
/// backup-against-compact bases, three clients.
fn scenario_cost(name: &str) -> (bool, usize) {
    let base = [
        "pair-distinct",
        "pair-hostname",
        "compact-distinct",
        "compact-hostname",
        "delete-during-compact",
        "two-compacts",
        "three-clients",
    ]
    .iter()
    .position(|b| name.starts_with(b))
    .unwrap_or(usize::MAX);
    (name.contains("-stepped"), base)
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

/// [`assert_clean`], and some schedule has a run that committed in a
/// shared session (`RunRecord::shared`: the case reaches the path it is
/// about).
fn assert_clean_lock_free(name: &str, c: &Case) {
    assert_clean_lock_free_on(&Template::new(), name, c);
}

/// [`assert_clean_lock_free`] on template `t`.
fn assert_clean_lock_free_on(t: &Template, name: &str, c: &Case) {
    let mut lock_free = 0usize;
    let e = explore(
        t,
        c,
        Bound {
            preemptions: PRECOMMIT_K,
            max_runs: PRECOMMIT_MAX_RUNS,
        },
        |o| {
            if o.sim.history().runs.iter().any(|r| r.shared) {
                lock_free += 1;
            }
            if o.signatures.is_empty() {
                Control::Continue
            } else {
                Control::Stop
            }
        },
    );
    print_search(name, &e);
    if let Some((sig, (_, case))) = e.signatures.iter().next() {
        panic!(
            "{name}: a schedule shows {sig:?}:\n{}",
            kept_trace(t, name, case)
        );
    }
    assert!(e.search.complete, "{name}: the search did not complete");
    assert_eq!(e.clean, e.search.runs);
    assert!(lock_free > 0, "{name}: no schedule committed in a session");
}

/// Two lock-free backups (phase F part 10c: the encrypted template on the
/// conditional store), on distinct hosts and on one hostname: every
/// schedule is clean (their head creates meet as part 9's do; no lock is
/// taken), and the search completes in the pre-commit bound although
/// object names are random per run.
#[test]
fn lock_free_pair_is_clean() {
    assert_clean_lock_free(
        "lock-free-pair-distinct",
        &conditional(lock_pair(HostPlan::Distinct)),
    );
    assert_clean_lock_free(
        "lock-free-pair-hostname",
        &conditional(lock_pair(HostPlan::SameHostname)),
    );
}

/// A lock-free backup against a lock-free delete + compact (phase F part
/// 10d: both clients' handles pass the put-if-absent check), whole and
/// gate-stepped, on the aged and the young template: every schedule is
/// clean. The compact registers a `writes_heads` marker instead of the
/// lock; its second session listing skips the sweep beside a committing
/// backup; a backup that commits during its mark moves the head, and the
/// compact marks again or its sweep head's create is taken; the backup's
/// reuse re-check fails it `StaleChunksDuringCommit` when it read the sweep.
#[test]
fn lock_free_backup_against_lock_free_compact_is_clean() {
    let cvc = || conditional(commit_vs_compact(HostPlan::Distinct));
    assert_clean_lock_free("lock-free-compact-distinct", &cvc());
    assert_clean_lock_free("lock-free-compact-distinct-stepped", &stepped(cvc()));
}

/// [`lock_free_backup_against_lock_free_compact_is_clean`] on the young
/// template (c0's segment younger than T_pack: its chunks stay unswept).
#[test]
fn lock_free_backup_against_lock_free_compact_young_is_clean() {
    let cvc = || conditional(commit_vs_compact(HostPlan::Distinct));
    assert_clean_lock_free_on(&Template::young(), "lock-free-compact-young", &cvc());
    assert_clean_lock_free_on(
        &Template::young(),
        "lock-free-compact-young-stepped",
        &stepped(cvc()),
    );
}

/// The stepped lock-free compact with time jumps offered inside it (phase
/// F part 10d: it holds no lock, so a jump cannot age one), on the aged
/// template: every schedule is clean (a jump past a marker's staleness
/// inside the compact reaps nothing live: the driver's backups refresh).
#[test]
fn lock_free_backup_against_a_stepped_lock_free_compact_with_jumps_is_clean() {
    let mut c = stepped(conditional(commit_vs_compact(HostPlan::Distinct)));
    c.advances = true;
    assert_clean_lock_free("lock-free-compact-stepped-advances", &c);
}

/// [`lock_free_backup_against_a_stepped_lock_free_compact_with_jumps_is_clean`]
/// on the young template.
#[test]
fn lock_free_backup_against_a_stepped_lock_free_compact_with_jumps_young_is_clean() {
    let mut c = stepped(conditional(commit_vs_compact(HostPlan::Distinct)));
    c.advances = true;
    assert_clean_lock_free_on(
        &Template::young(),
        "lock-free-compact-young-stepped-advances",
        &c,
    );
}

/// The stepped lock-free compact with one crash offered (either client, at
/// any decision), aged and young templates: every schedule is clean.
#[test]
fn lock_free_backup_against_a_stepped_lock_free_compact_with_a_crash_is_clean() {
    let mut c = stepped(conditional(commit_vs_compact(HostPlan::Distinct)));
    c.crash = true;
    assert_clean_lock_free("lock-free-compact-stepped-crash", &c);
    assert_clean_lock_free_on(
        &Template::young(),
        "lock-free-compact-young-stepped-crash",
        &c,
    );
}

/// a: a backup, then a lock-free delete of c0; b: a stepped lock-free
/// compact. The delete can land between the compact's snapshot listing and
/// its GET of c0 (the mark skips it once a second listing no longer shows
/// it) or anywhere else: every schedule is clean.
#[test]
fn lock_free_delete_during_compact_is_clean() {
    assert_clean_lock_free(
        "lock-free-delete-during-compact",
        &stepped(conditional(delete_during_compact())),
    );
}

/// a: a backup sharing c0's file, then a compact; b: a stepped compact;
/// both lock-free: their sweeps meet at a head generation (one marks
/// again), their repacks rebase over each other's heads, and an orphan or
/// source one deleted is gone for the other. Every schedule is clean.
#[test]
fn two_lock_free_compacts_are_clean() {
    assert_clean_lock_free(
        "two-lock-free-compacts",
        &stepped(conditional(two_compacts())),
    );
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

/// The same pair over the young template (c0's segment younger than
/// T_pack, 04 step 7), whole and stepped: compact's sweep keeps c0's
/// entries, so the backup's reuse of the shared file holds and it never
/// ends `StaleChunksDuringCommit`; every schedule is clean and the search
/// completes.
#[test]
fn backup_against_compact_over_a_young_control_never_goes_stale() {
    let t = Template::young();
    for (name, c) in [
        (
            "compact-young-distinct",
            commit_vs_compact(HostPlan::Distinct),
        ),
        (
            "compact-young-distinct-stepped",
            stepped(commit_vs_compact(HostPlan::Distinct)),
        ),
        // The same search as lock-free-compact-young-stepped, kept here for
        // the stale count this test asserts on the lock-free route too.
        (
            "compact-young-distinct-stepped-lock-free",
            conditional(stepped(commit_vs_compact(HostPlan::Distinct))),
        ),
    ] {
        let mut stale = 0usize;
        let e = explore(
            &t,
            &c,
            Bound {
                preemptions: PRECOMMIT_K,
                max_runs: PRECOMMIT_MAX_RUNS,
            },
            |o| {
                let went_stale = o.sim.history().runs.iter().any(|r| {
                    r.error_class()
                        == Some(&ErrorClass::Allowed(AllowedError::StaleChunksDuringCommit))
                });
                if went_stale {
                    stale += 1;
                }
                Control::Continue
            },
        );
        print_search(name, &e);
        if let Some((sig, (_, case))) = e.signatures.iter().next() {
            panic!(
                "{name}: a schedule shows {sig:?}:\n{}",
                kept_trace(&t, name, case)
            );
        }
        assert!(e.search.complete, "{name}: the search did not complete");
        assert_eq!(e.clean, e.search.runs, "{name}");
        assert_eq!(stale, 0, "{name}: a backup went stale over a young c0");
    }
}

/// Most `sessions/` ops one client's main thread makes in one exclusive
/// registration, deregistration included; one more is a harness bug.
const MAX_LOCK_OPS: usize = 12;

/// Real time to wait for a client's next gated op or its end.
const GATE_WAIT: Duration = Duration::from_secs(60);

#[derive(Default, Debug)]
struct LockPairTally {
    schedules: usize,
    /// Both clients inside their sessions at once.
    double_holds: usize,
    /// Both runs refused.
    both_backed_off: usize,
    /// Exactly one run ended Ok.
    one_held: usize,
    /// Both clients wrote an exclusive marker and one refused after its own.
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
    /// Most gated ops of the client in one schedule.
    max: usize,
}

impl Contender {
    /// Wait for the client's next gated op (held) or its end.
    fn wait(&mut self, sim: &Sim) {
        assert!(
            self.next < self.max,
            "more than {} gated ops by {}",
            self.max,
            sim.store().client_name(self.id)
        );
        self.held = sim
            .poll_paused(self.gates[self.next], GATE_WAIT)
            .unwrap_or_else(|e| panic!("harness: {e}"))
            .is_some();
    }
}

/// Two clients register exclusive sessions (`Call::ExclusiveCustom`,
/// 10-single-protocol step 8) and each runs a no-op in it, on `template`.
/// Every op of both on `sessions/` (the main threads only: their PUT,
/// LIST, GETs, EXISTS and DELETE) is held at a gate; the search picks which
/// held op proceeds next. Continuing the client resumed last is free while
/// it is held; when it is done (or at the start) every held client is
/// free; anything else is a preemption. A refusal is `ActiveSessions`, and
/// a hold lasts from the body's entry to the delete of the session's marker.
fn exclusive_registrations(
    template: &Template,
    hosts: HostPlan,
    bound: Bound,
) -> (Search, LockPairTally) {
    let gated = || KeyMatch::Prefix("sessions/".into());
    let holds = |key: &str| key.starts_with("sessions/x-") && key.ends_with(".json");
    let refusal = AllowedError::ActiveSessions;
    let mut tally = LockPairTally::default();
    let search = dfs(bound, |prefix| {
        let mut sim = template.fork();
        let ids = ["a", "b"].map(|name| match hosts {
            HostPlan::Distinct => sim.client(name),
            HostPlan::SameHostname => sim.client_on(name, "ubuntu", Some(&format!("boot-{name}"))),
            HostPlan::LateClone => unreachable!("the exclusive-pair search places no late clone"),
        });
        let inside: Arc<Mutex<Vec<(ClientId, u64)>>> = Arc::default();
        let mut cs: Vec<Contender> = ids
            .iter()
            .map(|&id| Contender {
                id,
                gates: (1..=MAX_LOCK_OPS as u32)
                    .map(|n| {
                        sim.gate(
                            GateRule::new(id, OpMatch::Any, gated())
                                .not_on_thread(crate::repo::sessions::HEARTBEAT_THREAD_NAME)
                                .nth(n),
                        )
                    })
                    .collect(),
                next: 0,
                held: false,
                max: MAX_LOCK_OPS,
            })
            .collect();
        let mut spawned = Vec::new();
        for c in &mut cs {
            let (store, inside, id) = (sim.store().clone(), Arc::clone(&inside), c.id);
            let f: crate::sim::CustomFn = Box::new(move |_| {
                inside.lock().unwrap().push((id, store.step()));
                Ok(())
            });
            let call = Call::ExclusiveCustom {
                label: "hold".into(),
                f,
            };
            spawned.push(sim.spawn(c.id, call));
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
        let locked = |r: &RunRecord| r.error_class() == Some(&ErrorClass::Allowed(refusal.clone()));
        for r in &runs {
            assert!(
                r.is_ok() || locked(r),
                "a raw acquisition ended neither Ok nor refused ({refusal:?}): {} / {}",
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
                        && holds(&r.key)
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
                .any(|r| r.client == c && r.op.is_write() && holds(&r.key))
        };
        if ids.iter().all(|&c| wrote(c)) && runs.iter().any(locked) {
            tally.contended_puts += 1;
        }
        Some(points)
    });
    (search, tally)
}

/// 10-single-protocol step 8's successor of 08's "lock acquire vs lock
/// acquire" (the lock's own race went with the lock in step 9): two
/// exclusive registrations, every op of both on `sessions/` held at a gate.
/// No schedule puts both inside their sessions at once (X1); every run
/// ends Ok or `ActiveSessions`; at K ≥ 3 both refusing is reached.
#[test]
fn exclusive_registrations_race() {
    let bound = Bound {
        preemptions: GATE_K,
        max_runs: PRECOMMIT_MAX_RUNS,
    };
    for hosts in [HostPlan::Distinct, HostPlan::SameHostname] {
        let start = Instant::now();
        let (s, t) = exclusive_registrations(&Template::new(), hosts, bound);
        eprintln!(
            "exclusive registrations, {hosts:?}, K = {GATE_K}: {s:?} {t:?} in {:?}",
            start.elapsed()
        );
        assert!(s.complete);
        assert_eq!(
            t.double_holds, 0,
            "two exclusive holders on {hosts:?}: {t:?}"
        );
        assert!(t.one_held > 0, "{t:?}");
        if GATE_K >= 3 {
            assert!(t.both_backed_off > 0, "{t:?}");
        }
    }
}

/// Most gated ops one client makes in one schedule of
/// [`exclusive_against_backup_search`] (a dozen or two are expected); one
/// more is a harness bug.
const MAX_EXCLUSIVE_BACKUP_OPS: usize = 64;

#[derive(Default, Debug)]
struct ExclusiveBackupTally {
    schedules: usize,
    /// The exclusive run refused `ActiveSessions`.
    exclusive_refused: usize,
    /// The backup ended `Locked`.
    backup_locked: usize,
    /// Both ran (one after the other).
    both_ok: usize,
    /// Schedules in which the backup's `sessions/` keys were written while
    /// the exclusive run held the repository (the residual: its marker's
    /// registration PUT and the DELETE of its refusal).
    residual_marker_writes: usize,
    first_violation: Option<(Vec<usize>, String)>,
}

/// The ops [`exclusive_against_backup_search`] holds: every op on
/// `sessions/` but on journals (PUT, LIST, GET, EXISTS, DELETE:
/// registrations, the exclusive run's write check, marks, listings,
/// deregistrations; a backup's journal is written from its upload threads,
/// whose order is not the search's) and the backup's head and snapshot
/// writes.
fn exclusive_backup_gated(op: OpKind, key: &str) -> bool {
    (key.starts_with("sessions/") && !key.ends_with(".index"))
        || (op.is_write() && (key.starts_with("index.h/") || key.starts_with("snapshots/")))
}

/// An exclusive session (client a, `Call::ExclusiveCustom` recording its
/// entry) against a backup (client b) on a `kind` store, every op of both
/// in [`exclusive_backup_gated`] (the main threads only) held at a gate,
/// the search picking which proceeds next (as [`single_writer_search`]:
/// a client held again at a listing of `sessions/` right after one, or
/// after a read of a marker, backed off, and continuing it costs a
/// preemption, so a wait loop is bounded by K). Each schedule: every run
/// ends Ok, `Locked` or `ActiveSessions`; when the exclusive run entered,
/// no write of the backup lands between its registration listing and the
/// DELETE of its marker, except the backup's own marker keys
/// (`sessions/<id>[.<n>].json`: a registration PUT and its refusal's
/// DELETE, the residual of two registrations racing); `Invariant::STEP1`
/// and invariant 13 hold. The hand rule is the judge of the hold: invariant
/// 13 sees the backup's head, index and snapshot writes and protected
/// deletes under it, not its pack, pack-map or journal writes.
fn exclusive_against_backup_search(
    template: &Template,
    kind: StoreKind,
    bound: Bound,
) -> (Search, ExclusiveBackupTally) {
    let mut tally = ExclusiveBackupTally::default();
    let search = dfs(bound, |prefix| {
        let mut sim = template.fork_as(kind);
        let (a, b) = (sim.client("a"), sim.client("b"));
        let mut cs: Vec<Contender> = [a, b]
            .into_iter()
            .map(|id| Contender {
                id,
                gates: (1..=MAX_EXCLUSIVE_BACKUP_OPS as u32)
                    .map(|n| {
                        sim.gate(
                            GateRule::new(id, OpMatch::Any, KeyMatch::Any)
                                .filter(exclusive_backup_gated)
                                .not_on_thread(crate::repo::sessions::HEARTBEAT_THREAD_NAME)
                                .nth(n),
                        )
                    })
                    .collect(),
                next: 0,
                held: false,
                max: MAX_EXCLUSIVE_BACKUP_OPS,
            })
            .collect();
        let entered: Arc<Mutex<Option<u64>>> = Arc::default();
        let mut spawned = Vec::new();
        for (i, c) in cs.iter_mut().enumerate() {
            let call = if i == 0 {
                let (store, entered) = (sim.store().clone(), Arc::clone(&entered));
                Call::ExclusiveCustom {
                    label: "hold".into(),
                    f: Box::new(move |_| {
                        *entered.lock().unwrap() = Some(store.step());
                        Ok(())
                    }),
                }
            } else {
                Call::Backup {
                    snapshot: "b1".into(),
                    tree: crate::sim::SourceTree::seeded("b1", 2, 16 * 1024),
                }
            };
            spawned.push(sim.spawn(c.id, call));
            c.wait(&sim);
        }

        let held_at = |c: &Contender| {
            sim.store()
                .gate_paused(c.gates[c.next])
                .map(|p| (p.op, p.key))
        };
        let is_sessions_listing =
            |(op, key): &(OpKind, String)| OpMatch::AnyList.matches(*op) && key == "sessions/";
        let mut points = Vec::new();
        let mut last: Option<(usize, bool)> = None;
        loop {
            let held: Vec<usize> = (0..cs.len()).filter(|&i| cs[i].held).collect();
            if held.is_empty() {
                break;
            }
            let costs: Vec<u32> = held
                .iter()
                .map(|&i| match last {
                    Some((l, false)) if cs[l].held => u32::from(i != l),
                    Some((l, true)) if cs[l].held => u32::from(i == l && held.len() > 1),
                    _ => 0,
                })
                .collect();
            let taken = prefix
                .get(points.len())
                .copied()
                .unwrap_or_else(|| costs.iter().position(|c| *c == 0).expect("a free option"));
            let i = held[taken];
            points.push(Point { costs, taken });
            let resumed = held_at(&cs[i]).expect("held");
            sim.resume(cs[i].gates[cs[i].next]);
            cs[i].next += 1;
            cs[i].wait(&sim);
            // A wait (the backup's pre-head listing `Locked`) lists
            // `sessions/` and reads the holder's marker, then lists again.
            let backed_off = cs[i].held
                && held_at(&cs[i]).is_some_and(|at| {
                    is_sessions_listing(&at)
                        && (is_sessions_listing(&resumed)
                            || (resumed.0 == OpKind::Get && resumed.1.starts_with("sessions/")))
                });
            last = Some((i, backed_off));
        }

        let runs: Vec<RunRecord> = spawned.into_iter().map(|s| sim.join(s)).collect();
        tally.schedules += 1;
        let ended =
            |r: &RunRecord, e: AllowedError| r.error_class() == Some(&ErrorClass::Allowed(e));
        let mut violation = None;
        for r in &runs {
            if !(r.is_ok()
                || ended(r, AllowedError::Locked)
                || ended(r, AllowedError::ActiveSessions))
            {
                violation = Some(format!("a run ended otherwise: {}", r.detail()));
            }
        }
        if ended(&runs[0], AllowedError::ActiveSessions) {
            tally.exclusive_refused += 1;
        }
        if ended(&runs[1], AllowedError::Locked) {
            tally.backup_locked += 1;
        }
        if runs.iter().all(RunRecord::is_ok) {
            tally.both_ok += 1;
        }
        let log = sim.store().log();
        let entry = *entered.lock().unwrap();
        if let Some(entry) = entry {
            let a_run = runs[0].index;
            let listed = log
                .iter()
                .filter(|r| {
                    r.client == a
                        && r.run == Some(a_run)
                        && r.step < entry
                        && OpMatch::AnyList.matches(r.op)
                        && r.key == "sessions/"
                })
                .map(|r| r.step)
                .max()
                .expect("the exclusive run listed sessions/ before it entered");
            let released = log
                .iter()
                .find(|r| {
                    r.client == a
                        && r.step > entry
                        && r.op.is_delete()
                        && r.key.starts_with("sessions/x-")
                        && r.key.ends_with(".json")
                })
                .map_or(runs[0].last_step, |r| r.step);
            let own_marker = |key: &str| {
                crate::repo::sessions::parse_session_marker_key(key)
                    .is_some_and(|(id, _)| !crate::repo::sessions::is_exclusive_session(id))
            };
            let inside: Vec<String> = log
                .iter()
                .filter(|r| {
                    r.client == b
                        && r.applied()
                        && r.op.is_mutation()
                        && r.step > listed
                        && r.step < released
                })
                .map(|r| format!("{} {:?} {}", r.step, r.op, r.key))
                .collect();
            let (marker, other): (Vec<&String>, Vec<&String>) = inside
                .iter()
                .partition(|w| w.rsplit(' ').next().is_some_and(own_marker));
            if !marker.is_empty() {
                tally.residual_marker_writes += 1;
            }
            if !other.is_empty() && violation.is_none() {
                violation = Some(format!(
                    "the backup wrote under the exclusive hold (steps {listed}..{released}): \
                     {other:?}"
                ));
            }
        }
        let mut judged = Invariant::STEP1.to_vec();
        judged.push(Invariant::ExclusiveSessions);
        let report = sim.check_invariants(&judged);
        if !report.violations.is_empty() && violation.is_none() {
            violation = Some(format!("{report}"));
        }
        if let Some(v) = violation
            && tally.first_violation.is_none()
        {
            tally.first_violation = Some((points.iter().map(|p| p.taken).collect(), v));
        }
        Some(points)
    });
    (search, tally)
}

/// 10-single-protocol step 8: an exclusive session against a backup, every
/// `sessions/` op of both and the backup's head and snapshot writes one at
/// a time, at [`GATE_K`], on both store kinds (exclusive sessions exist in
/// both modes). In every schedule the exclusive run refuses
/// `ActiveSessions`, or the backup refuses `Locked` (or ran after it):
/// no shared write lands under a live exclusive hold but the backup's own
/// marker keys (the residual of racing registrations, see
/// [`exclusive_against_backup_search`]); invariant 13 holds as well. One
/// test per kind, so the two run in parallel (about 20 s and 60 s, debug build).
fn assert_exclusive_against_backup(kind: StoreKind) {
    let bound = Bound {
        preemptions: GATE_K,
        max_runs: PRECOMMIT_MAX_RUNS,
    };
    let start = Instant::now();
    let (s, t) = exclusive_against_backup_search(&Template::new(), kind, bound);
    eprintln!(
        "exclusive against backup, {kind:?}, K = {GATE_K}: {s:?} {t:?} in {:?}",
        start.elapsed()
    );
    assert!(s.complete, "{kind:?}: {s:?}");
    assert!(
        t.first_violation.is_none(),
        "{kind:?}: {:?}",
        t.first_violation
    );
    assert!(t.exclusive_refused > 0, "{kind:?}: {t:?}");
    assert!(t.backup_locked > 0, "{kind:?}: {t:?}");
    assert!(t.both_ok > 0, "{kind:?}: {t:?}");
}

/// [`assert_exclusive_against_backup`] on a single-writer (plain) store.
#[test]
fn exclusive_against_backup_race() {
    assert_exclusive_against_backup(StoreKind::Plain);
}

/// [`assert_exclusive_against_backup`] on a store with put-if-absent.
#[test]
fn exclusive_against_backup_race_conditional() {
    assert_exclusive_against_backup(StoreKind::Conditional);
}

/// Most gated ops one client makes in one schedule of the single-writer
/// search (about a dozen are expected); one more is a harness bug.
const MAX_SINGLE_WRITER_OPS: usize = 64;

/// What [`single_writer_search`] runs against a backup.
#[derive(Clone, Copy, Debug)]
pub(super) enum SingleWriterRival {
    /// A second backup.
    Backup,
    /// A compact (registered `writes_heads`; with nothing to remove it
    /// still writes a head).
    Compact,
}

#[derive(Default, Debug)]
pub(super) struct SingleWriterTally {
    schedules: usize,
    /// Schedules whose end state violates `Invariant::STEP1`.
    pub(super) damaged: usize,
    /// Schedules in which both clients PUT one `index.h/` key.
    pub(super) double_generation: usize,
    /// Runs that ended `ConcurrentHeadWrite`.
    pub(super) concurrent_head_writes: usize,
    /// Runs that ended neither Ok nor `Locked`.
    pub(super) other_endings: usize,
    /// Schedules in which a client backed off: a backup unmarked after its
    /// mark (a third marker key) or ended `Locked`, or the compact ended
    /// `Locked`.
    backoffs: usize,
    both_ok: usize,
    pub(super) first_damage: Option<(Vec<usize>, String)>,
}

/// The single-writer check's ops (10-single-protocol step 7): every
/// listing of `sessions/`, every marker PUT, and the head write's EXISTS
/// and PUT.
fn single_writer_gated(op: OpKind, key: &str) -> bool {
    (OpMatch::AnyList.matches(op) && key == "sessions/")
        || (op == OpKind::Put && key.starts_with("sessions/") && key.ends_with(".json"))
        || (matches!(op, OpKind::Exists | OpKind::Put) && key.starts_with("index.h/"))
}

/// The single-writer check (10-single-protocol step 7, invariant I1),
/// operation by operation: on a single-writer repository (the fallback
/// store) client a backs up and client b backs up or compacts. Every op of
/// both in [`single_writer_gated`] (the main threads only) is held at a
/// gate; the search picks which held op proceeds next. A client held again
/// at a listing of `sessions/` right after one (it backed off) gives way:
/// continuing it costs a preemption, so a wait loop is bounded by K.
pub(super) fn single_writer_search(
    template: &Template,
    rival: SingleWriterRival,
    bound: Bound,
) -> (Search, SingleWriterTally) {
    let mut tally = SingleWriterTally::default();
    let search = dfs(bound, |prefix| {
        let mut sim = template.fork();
        let (a, b) = (sim.client("a"), sim.client("b"));
        let mut cs: Vec<Contender> = [a, b]
            .into_iter()
            .map(|id| Contender {
                id,
                gates: (1..=MAX_SINGLE_WRITER_OPS as u32)
                    .map(|n| {
                        sim.gate(
                            GateRule::new(id, OpMatch::Any, KeyMatch::Any)
                                .filter(single_writer_gated)
                                .not_on_thread(crate::repo::sessions::HEARTBEAT_THREAD_NAME)
                                .nth(n),
                        )
                    })
                    .collect(),
                next: 0,
                held: false,
                max: MAX_SINGLE_WRITER_OPS,
            })
            .collect();
        let backup_of = |name: &str| Call::Backup {
            snapshot: name.into(),
            tree: crate::sim::SourceTree::seeded(name, 2, 16 * 1024),
        };
        let mut spawned = Vec::new();
        for (i, c) in cs.iter_mut().enumerate() {
            let call = match (i, rival) {
                (1, SingleWriterRival::Compact) => Call::Compact,
                (1, SingleWriterRival::Backup) => backup_of("b1"),
                _ => backup_of("a1"),
            };
            spawned.push(sim.spawn(c.id, call));
            c.wait(&sim);
        }

        let held_at = |c: &Contender| {
            sim.store()
                .gate_paused(c.gates[c.next])
                .map(|p| (p.op, p.key))
        };
        let is_sessions_listing =
            |(op, key): &(OpKind, String)| OpMatch::AnyList.matches(*op) && key == "sessions/";
        let mut points = Vec::new();
        let mut last: Option<(usize, bool)> = None;
        loop {
            let held: Vec<usize> = (0..cs.len()).filter(|&i| cs[i].held).collect();
            if held.is_empty() {
                break;
            }
            let costs: Vec<u32> = held
                .iter()
                .map(|&i| match last {
                    Some((l, false)) if cs[l].held => u32::from(i != l),
                    Some((l, true)) if cs[l].held => u32::from(i == l && held.len() > 1),
                    _ => 0,
                })
                .collect();
            let taken = prefix
                .get(points.len())
                .copied()
                .unwrap_or_else(|| costs.iter().position(|c| *c == 0).expect("a free option"));
            let i = held[taken];
            points.push(Point { costs, taken });
            let resumed = held_at(&cs[i]).expect("held");
            sim.resume(cs[i].gates[cs[i].next]);
            cs[i].next += 1;
            cs[i].wait(&sim);
            // Held again at a listing of `sessions/` right after one, or
            // after the unmark that follows one: it backed off. Not after
            // the first two gated ops, the registration's PUT and its
            // listing of `sessions/` (10-single-protocol step 8), which the
            // open's listing (a backup) or the single-writer check (a
            // compact) follows at once.
            let backed_off = cs[i].held
                && cs[i].next > 2
                && held_at(&cs[i]).is_some_and(|at| {
                    is_sessions_listing(&at)
                        && (is_sessions_listing(&resumed) || resumed.0 == OpKind::Put)
                });
            last = Some((i, backed_off));
        }

        let runs: Vec<RunRecord> = spawned.into_iter().map(|s| sim.join(s)).collect();
        tally.schedules += 1;
        let locked =
            |r: &RunRecord| r.error_class() == Some(&ErrorClass::Allowed(AllowedError::Locked));
        for r in &runs {
            if r.error_class() == Some(&ErrorClass::Allowed(AllowedError::ConcurrentHeadWrite)) {
                tally.concurrent_head_writes += 1;
            } else if !r.is_ok() && !locked(r) {
                tally.other_endings += 1;
            }
        }
        if runs.iter().all(RunRecord::is_ok) {
            tally.both_ok += 1;
        }
        let log = sim.store().log();
        let marker_puts = |c: ClientId| {
            log.iter()
                .filter(|r| {
                    r.client == c
                        && r.op == OpKind::Put
                        && r.key.starts_with("sessions/")
                        && r.key.ends_with(".json")
                })
                .count()
        };
        if runs.iter().any(locked)
            || marker_puts(a) > 2
            || matches!(rival, SingleWriterRival::Backup) && marker_puts(b) > 2
        {
            tally.backoffs += 1;
        }
        let heads_of = |c: ClientId| -> std::collections::BTreeSet<String> {
            log.iter()
                .filter(|r| r.client == c && r.op == OpKind::Put && r.key.starts_with("index.h/"))
                .map(|r| r.key.clone())
                .collect()
        };
        if heads_of(a).intersection(&heads_of(b)).next().is_some() {
            tally.double_generation += 1;
        }
        let report = sim.check_invariants(Invariant::STEP1);
        if !report.violations.is_empty() {
            tally.damaged += 1;
            if tally.first_damage.is_none() {
                tally.first_damage = Some((
                    points.iter().map(|p| p.taken).collect(),
                    format!("{report}"),
                ));
            }
        }
        Some(points)
    });
    (search, tally)
}

/// Preemption bound of the campaign's single-writer searches
/// (`tests::sim_campaign::campaign` runs them).
pub(super) const SINGLE_WRITER_CAMPAIGN_K: u32 = 4;

/// Preemption bound of the pre-commit single-writer searches.
const SINGLE_WRITER_K: u32 = 2;

/// [`single_writer_search`] at [`SINGLE_WRITER_K`] with `rival`: every schedule is
/// clean under `Invariant::STEP1`, no head generation is PUT by both, no
/// run ends `ConcurrentHeadWrite` (the check kept the second writer out
/// before its head write) and every run ends Ok or `Locked`; the search
/// reaches a back-off and a schedule where both commit.
fn assert_single_writer_race(name: &str, rival: SingleWriterRival) {
    let start = Instant::now();
    let (s, t) = single_writer_search(
        &Template::new(),
        rival,
        Bound {
            preemptions: SINGLE_WRITER_K,
            max_runs: PRECOMMIT_MAX_RUNS,
        },
    );
    eprintln!(
        "{name}, K = {SINGLE_WRITER_K}: {s:?} {t:?} in {:?}",
        start.elapsed()
    );
    assert!(s.complete, "{name}: {s:?}");
    assert_eq!(t.damaged, 0, "{name}: {:?}", t.first_damage);
    assert_eq!(t.double_generation, 0, "{name}: {t:?}");
    assert_eq!(t.concurrent_head_writes, 0, "{name}: {t:?}");
    assert_eq!(t.other_endings, 0, "{name}: {t:?}");
    assert!(t.backoffs > 0, "{name}: no schedule backed off: {t:?}");
    assert!(t.both_ok > 0, "{name}: {t:?}");
}

/// Two single-writer backups (10-single-protocol step 7), their
/// `sessions/` listings, marker PUTs and head EXISTS and PUT one at a time
/// (replaces the lock acquisitions race's role for backups, whose commits
/// no longer take the lock): each flags its marker before the listing it
/// decides on, so no two pass it (I1).
#[test]
fn single_writer_checks_race() {
    assert_single_writer_race("single-writer backups", SingleWriterRival::Backup);
}

/// A single-writer backup against a compact, the same way: the compact
/// never waits and refuses beside a committing backup; the backup waits
/// while the compact is flagged.
#[test]
fn single_writer_compact_race() {
    assert_single_writer_race(
        "single-writer backup and compact",
        SingleWriterRival::Compact,
    );
}

/// What [`gapped_backup_search`] runs against a suspended backup
/// (10-single-protocol step 7, the suspend rule; review A5).
#[derive(Clone, Copy, Debug)]
pub(super) enum GappedRival {
    /// `vykar upgrade --commit-mode multi` of a single-writer repository
    /// whose storage passes the check.
    SwitchToMulti,
    /// `vykar upgrade --commit-mode single` of a multi-writer repository.
    SwitchToSingle,
    /// `check --repair` (under the maintenance lock).
    CheckRepair,
    /// A compact (a session) after the delete of c0, on a single-writer
    /// repository.
    Compact,
}

#[derive(Default, Debug)]
pub(super) struct GappedTally {
    schedules: usize,
    /// Schedules whose end state violates [`GAPPED_INVARIANTS`].
    pub(super) damaged: usize,
    /// Schedules in which the suspended backup committed.
    committed: usize,
    /// Schedules in which it re-read `config` after its mark.
    reread: usize,
    pub(super) first_damage: Option<(Vec<usize>, String)>,
}

/// What the gapped search judges: invariants 1 to 4, the allowed errors,
/// and invariant 11 (a commit whose record had a gap keeps the pack-age
/// term).
const GAPPED_INVARIANTS: &[Invariant] = &[
    Invariant::CommittedSnapshotsRestore,
    Invariant::AcknowledgedCommitsDurable,
    Invariant::NoSilentRemoval,
    Invariant::IndexIntegrity,
    Invariant::AllowedErrors,
    Invariant::MarkerGapCommit,
];

/// The ops [`gapped_backup_search`] holds: every listing of `sessions/`,
/// every marker PUT, the EXISTS and DELETE of an exclusive session's marker
/// (its write check and its deregistration, 10-single-protocol step 8),
/// every op on `config`, and the head writes (EXISTS, PUT, put-if-absent).
fn gapped_gated(op: OpKind, key: &str) -> bool {
    (OpMatch::AnyList.matches(op) && key == "sessions/")
        || (op == OpKind::Put && key.starts_with("sessions/") && key.ends_with(".json"))
        || (matches!(op, OpKind::Exists | OpKind::Delete)
            && key.starts_with("sessions/x-")
            && key.ends_with(".json"))
        || key == "config"
        || (matches!(op, OpKind::Exists | OpKind::Put | OpKind::PutIfAbsent)
            && key.starts_with("index.h/"))
}

/// The suspend rule against what can run while a backup is suspended
/// (review A5): client a's backup is held after its uploads, its machine
/// suspended 46 min (its marker stale, its record with a gap), then a and
/// `rival` (client b) race with every op of both in [`gapped_gated`] held
/// at a gate, the search picking which proceeds next (as
/// [`single_writer_search`]). Each end state is judged by
/// [`GAPPED_INVARIANTS`].
pub(super) fn gapped_backup_search(
    template: &Template,
    rival: GappedRival,
    bound: Bound,
) -> (Search, GappedTally) {
    let mut tally = GappedTally::default();
    let kind = match rival {
        GappedRival::SwitchToMulti | GappedRival::SwitchToSingle => StoreKind::Conditional,
        GappedRival::CheckRepair | GappedRival::Compact => StoreKind::Plain,
    };
    let search = dfs(bound, |prefix| {
        let mut sim = template.fork_as(kind);
        let (a, b) = (sim.client("a"), sim.client("b"));
        if matches!(rival, GappedRival::SwitchToMulti) {
            let r = sim.call(
                b,
                Call::SwitchMode {
                    mode: crate::repo::WriterMode::Single,
                },
            );
            assert!(r.is_ok(), "{}", r.detail());
        }
        if matches!(rival, GappedRival::Compact) {
            let r = sim.call(
                b,
                Call::Delete {
                    names: vec!["c0".into()],
                },
            );
            assert!(r.is_ok(), "{}", r.detail());
        }
        let upload = sim.gate(GateRule::new(
            a,
            OpMatch::AnyWrite,
            KeyMatch::Prefix("packs/".into()),
        ));
        let sa = sim.spawn(
            a,
            Call::Backup {
                snapshot: "a1".into(),
                tree: crate::sim::SourceTree::seeded("a1", 2, 16 * 1024),
            },
        );
        sim.expect_paused(upload, "a's first pack PUT");
        let wait = Duration::from_secs(10);
        assert!(sim.time().wait_for_waiters(1, wait));
        sim.advance(Duration::from_secs(46 * 60));
        assert!(sim.time().wait_for_waiters(1, wait));
        let mut cs: Vec<Contender> = [a, b]
            .into_iter()
            .map(|id| Contender {
                id,
                gates: (1..=MAX_SINGLE_WRITER_OPS as u32)
                    .map(|n| {
                        sim.gate(
                            GateRule::new(id, OpMatch::Any, KeyMatch::Any)
                                .filter(gapped_gated)
                                .not_on_thread(crate::repo::sessions::HEARTBEAT_THREAD_NAME)
                                .nth(n),
                        )
                    })
                    .collect(),
                next: 0,
                held: false,
                max: MAX_SINGLE_WRITER_OPS,
            })
            .collect();
        sim.resume(upload);
        cs[0].wait(&sim);
        let call = match rival {
            GappedRival::SwitchToMulti => Call::SwitchMode {
                mode: crate::repo::WriterMode::Multi,
            },
            GappedRival::SwitchToSingle => Call::SwitchMode {
                mode: crate::repo::WriterMode::Single,
            },
            GappedRival::CheckRepair => Call::CheckRepair,
            GappedRival::Compact => Call::Compact,
        };
        let sb = sim.spawn(b, call);
        cs[1].wait(&sim);
        // A client held again at a listing of `sessions/` right after one
        // (a wait: a backup's contended protection) gives way: continuing
        // it costs a preemption, so a wait loop is bounded by K.
        let held_at = |c: &Contender| {
            sim.store()
                .gate_paused(c.gates[c.next])
                .map(|p| (p.op, p.key))
        };
        let is_listing =
            |(op, key): &(OpKind, String)| OpMatch::AnyList.matches(*op) && key == "sessions/";
        let mut points = Vec::new();
        let mut last: Option<(usize, bool)> = None;
        loop {
            let held: Vec<usize> = (0..cs.len()).filter(|&i| cs[i].held).collect();
            if held.is_empty() {
                break;
            }
            let costs: Vec<u32> = held
                .iter()
                .map(|&i| match last {
                    Some((l, false)) if cs[l].held => u32::from(i != l),
                    Some((l, true)) if cs[l].held => u32::from(i == l && held.len() > 1),
                    _ => 0,
                })
                .collect();
            let taken = prefix
                .get(points.len())
                .copied()
                .unwrap_or_else(|| costs.iter().position(|c| *c == 0).expect("a free option"));
            let i = held[taken];
            points.push(Point { costs, taken });
            let resumed = held_at(&cs[i]).expect("held");
            sim.resume(cs[i].gates[cs[i].next]);
            cs[i].next += 1;
            cs[i].wait(&sim);
            let backed_off = cs[i].held
                && held_at(&cs[i]).is_some_and(|at| is_listing(&at) && is_listing(&resumed));
            last = Some((i, backed_off));
        }
        let ra = sim.join(sa);
        let _ = sim.join(sb);
        tally.schedules += 1;
        if ra.is_ok() {
            tally.committed += 1;
        }
        let log = sim.store().log();
        let mark = log.iter().position(|r| {
            r.client == a
                && r.op == OpKind::Put
                && r.key.starts_with("sessions/")
                && r.key.ends_with(".1.json")
        });
        if mark.is_some_and(|m| {
            log[m..]
                .iter()
                .any(|r| r.client == a && r.op == OpKind::Get && r.key == "config")
        }) {
            tally.reread += 1;
        }
        let report = sim.check_invariants(GAPPED_INVARIANTS);
        if !report.violations.is_empty() {
            tally.damaged += 1;
            if tally.first_damage.is_none() {
                tally.first_damage = Some((
                    points.iter().map(|p| p.taken).collect(),
                    format!("{report}"),
                ));
            }
        }
        Some(points)
    });
    (search, tally)
}

/// Preemption bound of the pre-commit gapped searches: K = 2 takes about
/// 4 s each (34 to 88 runs); K = 3 was run when they were added (see the
/// step's report) and again with exclusive rivals (10-single-protocol step
/// 8): switch to single 110 runs, `check --repair` 198, switch to multi
/// 198, compact 380, all clean, each committing and re-reading `config`
/// in every schedule (20 s to 85 s each, debug build).
const GAPPED_K: u32 = 2;

/// [`gapped_backup_search`] at [`GAPPED_K`] against `rival`: every
/// schedule is clean and the search completes; the suspended backup
/// re-reads `config` after its mark in some schedule (and commits in some,
/// except beside `check --repair`, which may refuse it nothing).
fn assert_gapped_clean(name: &str, rival: GappedRival) {
    let start = Instant::now();
    let (s, t) = gapped_backup_search(
        &Template::new(),
        rival,
        Bound {
            preemptions: GAPPED_K,
            max_runs: PRECOMMIT_MAX_RUNS,
        },
    );
    eprintln!(
        "{name}, K = {GAPPED_K}: {s:?} {t:?} in {:?}",
        start.elapsed()
    );
    assert!(s.complete, "{name}: {s:?}");
    assert_eq!(t.damaged, 0, "{name}: {:?}", t.first_damage);
    assert!(
        t.reread > 0,
        "{name}: the backup never re-read config: {t:?}"
    );
    assert!(t.committed > 0, "{name}: the backup never committed: {t:?}");
}

/// A5: a suspended backup against a switch to multi-writer.
#[test]
fn gapped_backup_against_a_switch_to_multi_writer_is_clean() {
    assert_gapped_clean("gapped vs switch to multi", GappedRival::SwitchToMulti);
}

/// A5: a suspended backup against a switch to single-writer.
#[test]
fn gapped_backup_against_a_switch_to_single_writer_is_clean() {
    assert_gapped_clean("gapped vs switch to single", GappedRival::SwitchToSingle);
}

/// A5: a suspended backup against `check --repair`.
#[test]
fn gapped_backup_against_check_repair_is_clean() {
    assert_gapped_clean("gapped vs check --repair", GappedRival::CheckRepair);
}

/// A5: a suspended backup against a compact (one op at a time, as every
/// gated op of both runs is a decision).
#[test]
fn gapped_backup_against_a_compact_is_clean() {
    assert_gapped_clean("gapped vs compact", GappedRival::Compact);
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
        let bases = [
            ("pair-distinct", lock_pair(HostPlan::Distinct)),
            ("pair-hostname", lock_pair(HostPlan::SameHostname)),
            ("compact-distinct", commit_vs_compact(HostPlan::Distinct)),
            (
                "compact-distinct-stepped",
                stepped(commit_vs_compact(HostPlan::Distinct)),
            ),
        ];
        let twins: Vec<(String, Case)> = bases
            .iter()
            .map(|(n, c)| (format!("{n}-conditional"), conditional(c.clone())))
            .collect();
        for (name, c) in bases
            .iter()
            .map(|(n, c)| (n.to_string(), c.clone()))
            .chain(twins)
        {
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
            let (s, tally) = exclusive_registrations(
                &Template::new(),
                hosts,
                Bound {
                    preemptions: k,
                    max_runs: 100_000,
                },
            );
            eprintln!(
                "exclusive gate {hosts:?} K = {k}: {s:?} {tally:?} in {:?}",
                start.elapsed()
            );
        }
    }
}

/// `VYKAR_SIM_MEASURE=1`: the searches phase F parts 10c and 10d name, on
/// the encrypted template (runs and wall time, against
/// `PRECOMMIT_MAX_RUNS`). Their mixed-mode searches (a lock client beside
/// lock-free ones) went with 10-single-protocol step 7: no binary is such a
/// client.
#[test]
fn measure_part_10_searches() {
    if std::env::var("VYKAR_SIM_MEASURE").ok().as_deref() != Some("1") {
        return;
    }
    let start = Instant::now();
    let enc = Template::new();
    eprintln!("encrypted template built in {:?}", start.elapsed());
    let start = Instant::now();
    let young = Template::young();
    eprintln!("encrypted young template built in {:?}", start.elapsed());
    let delete_during_compact = delete_during_compact();
    let two_compacts = two_compacts();
    let advances = |mut c: Case| {
        c.advances = true;
        c
    };
    let crash = |mut c: Case| {
        c.crash = true;
        c
    };
    let cvc = || commit_vs_compact(HostPlan::Distinct);
    fn k1<'t>(name: &str, t: &'t Template, c: Case) -> (String, &'t Template, Case, u32) {
        (name.to_string(), t, c, 1)
    }
    let searches: Vec<(String, &Template, Case, u32)> = vec![
        // 10c.
        k1(
            "lock_free_pair_is_clean: pair-distinct",
            &enc,
            conditional(lock_pair(HostPlan::Distinct)),
        ),
        k1(
            "lock_free_pair_is_clean: pair-hostname",
            &enc,
            conditional(lock_pair(HostPlan::SameHostname)),
        ),
        k1(
            "lock_free_backup_against_lock_free_compact: whole",
            &enc,
            conditional(cvc()),
        ),
        k1(
            "lock_free_backup_against_lock_free_compact: stepped",
            &enc,
            conditional(stepped(cvc())),
        ),
        // 10d.
        k1(
            "lock_free_backup_against_lock_free_compact: stepped advances",
            &enc,
            conditional(advances(stepped(cvc()))),
        ),
        k1(
            "lock_free_backup_against_lock_free_compact: stepped crash",
            &enc,
            conditional(crash(stepped(cvc()))),
        ),
        k1(
            "lock_free_backup_against_lock_free_compact: young whole",
            &young,
            conditional(cvc()),
        ),
        k1(
            "lock_free_backup_against_lock_free_compact: young stepped",
            &young,
            conditional(stepped(cvc())),
        ),
        k1(
            "lock_free_backup_against_lock_free_compact: young stepped advances",
            &young,
            conditional(advances(stepped(cvc()))),
        ),
        k1(
            "lock_free_backup_against_lock_free_compact: young stepped crash",
            &young,
            conditional(crash(stepped(cvc()))),
        ),
        k1(
            "lock_free_delete_during_compact: stepped",
            &enc,
            conditional(stepped(delete_during_compact)),
        ),
        k1(
            "two_lock_free_compacts: stepped",
            &enc,
            conditional(stepped(two_compacts)),
        ),
        (
            "three-clients-lockfree (campaign) K = 2".to_string(),
            &enc,
            conditional(three_clients()),
            2,
        ),
    ];
    for (name, t, c, k) in searches {
        let e = explore(
            t,
            &c,
            Bound {
                preemptions: k,
                max_runs: 100_000,
            },
            |_| Control::Continue,
        );
        print_search(&format!("{name} K = {k}"), &e);
    }
    for hosts in [HostPlan::Distinct, HostPlan::SameHostname] {
        let start = Instant::now();
        let (s, tally) = exclusive_registrations(
            &enc,
            hosts,
            Bound {
                preemptions: GATE_K,
                max_runs: 100_000,
            },
        );
        eprintln!(
            "exclusive gate {hosts:?} K = {GATE_K} (encrypted): {s:?} {tally:?} in {:?}",
            start.elapsed()
        );
    }
}
