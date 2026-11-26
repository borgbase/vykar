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
        lock_clients: vec![],
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

/// `c` on the encrypted template (phase F part 10b), where the lock-free
/// path of 04 step 9 exists.
pub(super) fn encrypted(c: Case) -> Case {
    Case {
        template: TemplateKind::Encrypted,
        ..c
    }
}

/// `c` on the store whose plain PUT overwrites heads (local, SFTP, S3),
/// with clients `lock_clients` falling back to the lock protocol.
pub(super) fn mixed(c: Case, lock_clients: Vec<u8>) -> Case {
    Case {
        storage: StoreKind::ConditionalOverwrite,
        lock_clients,
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

/// The campaign's searches, in the order the campaign hands them out:
/// (name, base, K). The K = 1 scenarios (the faults, time jumps and
/// crashes) take turns by store family (plain, conditional, lock-free,
/// mixed, then plain again), and the K >= 2 searches are placed at a fixed
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
    // Phase F part 10d's lock-free maintenance bases, on the lock-free
    // family only (every client lock-free; in the other families they add
    // nothing the compact bases above do not): a delete during a stepped
    // compact, and two compacts, each at K = 1 with time jumps (inside the
    // stepped compact too: it holds no lock) and at K = 2.
    let mut lock_free_only: Vec<(String, Case, u32)> = Vec::new();
    for (name, c) in [
        ("delete-during-compact", delete_during_compact()),
        ("two-compacts", two_compacts()),
    ] {
        let c = encrypted(conditional(stepped(c)));
        let mut jumps = c.clone();
        jumps.advances = true;
        lock_free_only.push((format!("{name}-stepped-advances-k1-lockfree"), jumps, 1));
        lock_free_only.push((format!("{name}-stepped-k2-lockfree"), c, 2));
    }
    // Every scenario lock-free too (phase F part 10c: the encrypted
    // template on the conditional store; local, OpenSSH SFTP, AWS S3 and
    // vykar-server all offer put-if-absent, 04 step 8, N6), and the pair and
    // three-client scenarios with client b in lock mode beside the others on
    // the store whose plain PUT replaces heads (the mixed verdict). Families:
    // 0 plain (the fallback store, lock mode with the marker handshake),
    // 1 lock-free, 2 mixed. The plaintext template's conditional family went
    // with 10-single-protocol step 2: encrypted, it is the lock-free one.
    let mut all: Vec<(u32, usize, (String, Case, u32))> = Vec::new();
    for (name, c, k) in out {
        // A fault on a lock key never fires in the lock-free family: since
        // phase F part 10d no client there takes the lock (backups and
        // maintenance alike), so such a scenario repeats its no-fault twin.
        let lock_fault = c.faults.iter().any(|f| f.class == KeyClass::Lock);
        if !lock_fault {
            all.push((
                k,
                1,
                (
                    format!("{name}-lockfree"),
                    encrypted(conditional(c.clone())),
                    k,
                ),
            ));
        }
        if name.starts_with("pair") || name.starts_with("three") {
            all.push((
                k,
                2,
                (
                    format!("{name}-mixed"),
                    encrypted(mixed(c.clone(), vec![1])),
                    k,
                ),
            ));
        }
        all.push((k, 0, (name, c, k)));
    }
    for (name, c, k) in lock_free_only {
        all.push((k, 1, (name, c, k)));
    }
    // Within one (K, family) the cheapest first; stable, so the build order
    // stays among equals.
    all.sort_by_key(|(k, family, (name, _, _))| (*k, *family, scenario_cost(name)));
    let mut narrow: Vec<Vec<(String, Case, u32)>> = vec![Vec::new(); 3];
    let mut wide: Vec<Vec<Vec<(String, Case, u32)>>> = Vec::new();
    for (k, family, s) in all {
        if k == 1 {
            narrow[family].push(s);
        } else {
            let at = usize::try_from(k - 2).expect("small K");
            if wide.len() <= at {
                wide.resize_with(at + 1, || vec![Vec::new(); 3]);
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
    let t = Template::of_kind(c.template);
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

/// [`assert_clean`], and some schedule has a run that committed lock-free
/// (the case reaches the path it is about).
fn assert_clean_lock_free(name: &str, c: &Case) {
    assert_clean_lock_free_on(&Template::of_kind(c.template), name, c);
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
            if o.sim.history().runs.iter().any(|r| r.lock_free) {
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
    assert!(lock_free > 0, "{name}: no schedule committed lock-free");
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
        &encrypted(conditional(lock_pair(HostPlan::Distinct))),
    );
    assert_clean_lock_free(
        "lock-free-pair-hostname",
        &encrypted(conditional(lock_pair(HostPlan::SameHostname))),
    );
}

/// A lock-free backup against a lock-mode delete + compact (client b's
/// handle offers no put-if-absent, so it takes the maintenance lock: phase
/// F part 10c's shape, now the mixed case), whole and stepped: every
/// schedule is clean. Compact yields (`Locked`) at its session listing to
/// a backup that has marked its commit; a commit that marks after compact
/// took the lock waits at its probe; one that commits before compact's
/// listing is swept from by the rules of 04 step 7, as under the lock.
#[test]
fn lock_free_backup_against_a_locked_compact_is_clean() {
    assert_clean_lock_free(
        "lock-free-backup-locked-compact-distinct",
        &encrypted(mixed(commit_vs_compact(HostPlan::Distinct), vec![1])),
    );
    assert_clean_lock_free(
        "lock-free-backup-locked-compact-distinct-stepped",
        &encrypted(mixed(
            stepped(commit_vs_compact(HostPlan::Distinct)),
            vec![1],
        )),
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
    let cvc = || encrypted(conditional(commit_vs_compact(HostPlan::Distinct)));
    assert_clean_lock_free("lock-free-compact-distinct", &cvc());
    assert_clean_lock_free("lock-free-compact-distinct-stepped", &stepped(cvc()));
}

/// [`lock_free_backup_against_lock_free_compact_is_clean`] on the young
/// template (c0's segment younger than T_pack: its chunks stay unswept).
#[test]
fn lock_free_backup_against_lock_free_compact_young_is_clean() {
    let cvc = || encrypted(conditional(commit_vs_compact(HostPlan::Distinct)));
    assert_clean_lock_free_on(
        &Template::encrypted_young(),
        "lock-free-compact-young",
        &cvc(),
    );
    assert_clean_lock_free_on(
        &Template::encrypted_young(),
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
    let mut c = stepped(encrypted(conditional(commit_vs_compact(
        HostPlan::Distinct,
    ))));
    c.advances = true;
    assert_clean_lock_free("lock-free-compact-stepped-advances", &c);
}

/// [`lock_free_backup_against_a_stepped_lock_free_compact_with_jumps_is_clean`]
/// on the young template.
#[test]
fn lock_free_backup_against_a_stepped_lock_free_compact_with_jumps_young_is_clean() {
    let mut c = stepped(encrypted(conditional(commit_vs_compact(
        HostPlan::Distinct,
    ))));
    c.advances = true;
    assert_clean_lock_free_on(
        &Template::encrypted_young(),
        "lock-free-compact-young-stepped-advances",
        &c,
    );
}

/// The stepped lock-free compact with one crash offered (either client, at
/// any decision), aged and young templates: every schedule is clean.
#[test]
fn lock_free_backup_against_a_stepped_lock_free_compact_with_a_crash_is_clean() {
    let mut c = stepped(encrypted(conditional(commit_vs_compact(
        HostPlan::Distinct,
    ))));
    c.crash = true;
    assert_clean_lock_free("lock-free-compact-stepped-crash", &c);
    assert_clean_lock_free_on(
        &Template::encrypted_young(),
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
        &stepped(encrypted(conditional(delete_during_compact()))),
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
        &stepped(encrypted(conditional(two_compacts()))),
    );
}

/// A lock-free backup and a lock-mode one (client b's handle offers no
/// put-if-absent) on a store whose plain PUT replaces heads, through the
/// driver: every schedule is clean, invariant 12 included (the marker
/// handshake; [`the_marker_handshake_keeps_heads_immutable`] searches it
/// operation by operation).
#[test]
fn mixed_lock_and_lock_free_backups_are_clean() {
    assert_clean_lock_free(
        "mixed-pair-distinct",
        &encrypted(mixed(lock_pair(HostPlan::Distinct), vec![1])),
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
    let t = Template::encrypted_young();
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
    /// Most gated ops of the client in one schedule.
    max: usize,
}

impl Contender {
    /// Wait for the client's next `locks/` op (held) or its end.
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

/// Two clients each run a no-op under the repo lock. Every op of both on a
/// lock key or a listing of `locks/` is held at a gate (a contended
/// pre-check's clock probe PUT and DELETE are not: no lock reader lists a
/// probe, so their order does not matter; its listing, by which the
/// pre-check judges, is gated); the search picks which held op proceeds next.
/// Continuing the client resumed last is free while it is held; when it is
/// done (or at the start) every held client is free; anything else is a
/// preemption.
fn lock_acquisitions(hosts: HostPlan, bound: Bound) -> (Search, LockPairTally) {
    lock_acquisitions_on(&Template::encrypted(), hosts, bound)
}

/// [`lock_acquisitions`] on `template`.
fn lock_acquisitions_on(
    template: &Template,
    hosts: HostPlan,
    bound: Bound,
) -> (Search, LockPairTally) {
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

/// Most gated ops one client makes in one schedule of the handshake search
/// (about a dozen are expected); one more is a harness bug.
const MAX_HANDSHAKE_OPS: usize = 64;

#[derive(Default, Debug)]
pub(super) struct HandshakeTally {
    schedules: usize,
    /// Schedules whose end state violates [`HANDSHAKE_INVARIANTS`].
    pub(super) damaged: usize,
    /// Schedules in which the lock holder released a lock it had confirmed
    /// with no head PUT in between: it yielded to the flagged marker.
    yields: usize,
    /// Schedules in which the lock-free backup listed `locks/` more than
    /// twice (registration, the probe after its mark): it met a live lock.
    waits: usize,
    both_ok: usize,
    pub(super) first_damage: Option<(Vec<usize>, String)>,
}

/// Which of a client's ops the handshake search holds at a gate.
type GateFilter = fn(OpKind, &str) -> bool;

/// The lock-free writer's half of the handshake: its marker PUTs (the
/// flagged one among them), its listings of `locks/`, its head creates.
fn lock_free_gated(op: OpKind, key: &str) -> bool {
    (op == OpKind::Put && key.starts_with("sessions/") && key.ends_with(".json"))
        || (OpMatch::AnyList.matches(op) && key == "locks/")
        || (op == OpKind::PutIfAbsent && key.starts_with("index.h/"))
}

/// The lock holder's half: every op on a lock key or `locks/`, its
/// listings of `sessions/` (the yield's among them), its plain head PUTs.
fn lock_mode_gated(op: OpKind, key: &str) -> bool {
    KeyMatch::LockKey.matches(key)
        || (OpMatch::AnyList.matches(op) && key == "sessions/")
        || (op == OpKind::Put && key.starts_with("index.h/"))
}

/// What the handshake search judges: invariant 12 and the snapshots.
const HANDSHAKE_INVARIANTS: &[Invariant] = &[
    Invariant::CommittedSnapshotsRestore,
    Invariant::AcknowledgedCommitsDurable,
    Invariant::IndexIntegrity,
    Invariant::AllowedErrors,
    Invariant::HeadsImmutable,
];

/// The marker handshake (04 step 9, phase F part 10c), operation by
/// operation: on an encrypted repository over a store whose plain PUT
/// replaces heads (local, SFTP, S3), client a backs up lock-free and client
/// b, whose handle offers no put-if-absent, backs up under the lock. Every
/// op of a's half ([`lock_free_gated`]) and of b's ([`lock_mode_gated`]) is
/// held at a gate, the head writes included (without them each side's
/// commit would run between two of the other's gated ops, and no order of
/// the handshake could show damage); the search picks which held op
/// proceeds next, as [`lock_acquisitions_on`] does. Each schedule's end
/// state is judged by [`HANDSHAKE_INVARIANTS`].
pub(super) fn handshake_search(template: &Template, bound: Bound) -> (Search, HandshakeTally) {
    let mut tally = HandshakeTally::default();
    let search = dfs(bound, |prefix| {
        let mut sim = template.fork_as(StoreKind::ConditionalOverwrite);
        let (a, b) = (sim.client("a"), sim.client("b"));
        sim.set_lock_client(b);
        let halves: [(ClientId, GateFilter); 2] = [(a, lock_free_gated), (b, lock_mode_gated)];
        let mut cs: Vec<Contender> = halves
            .into_iter()
            .map(|(id, f)| Contender {
                id,
                gates: (1..=MAX_HANDSHAKE_OPS as u32)
                    .map(|n| {
                        sim.gate(
                            GateRule::new(id, OpMatch::Any, KeyMatch::Any)
                                .filter(f)
                                .nth(n),
                        )
                    })
                    .collect(),
                next: 0,
                held: false,
                max: MAX_HANDSHAKE_OPS,
            })
            .collect();
        let mut spawned = Vec::new();
        for (c, name) in cs.iter_mut().zip(["a1", "b1"]) {
            spawned.push(sim.spawn(
                c.id,
                Call::Backup {
                    snapshot: name.into(),
                    tree: crate::sim::SourceTree::seeded(name, 2, 16 * 1024),
                },
            ));
            c.wait(&sim);
        }

        // A client that backed off (the holder released a lock it yielded,
        // the lock-free backup met a live lock) and is held at its next
        // attempt gives way: continuing it costs a preemption, so a retry
        // loop is bounded by K.
        let held_at = |c: &Contender| {
            sim.store()
                .gate_paused(c.gates[c.next])
                .map(|p| (p.op, p.key))
        };
        let is_listing = |(op, _): &(OpKind, String)| OpMatch::AnyList.matches(*op);
        let is_locks_listing = |at: &(OpKind, String)| is_listing(at) && at.1 == "locks/";
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
            // Held again at a listing of the directory it just listed, or
            // at a listing of `locks/` right after releasing a lock.
            let backed_off = cs[i].held
                && held_at(&cs[i]).is_some_and(|at| {
                    (is_listing(&at) && is_listing(&resumed) && at.1 == resumed.1)
                        || (is_locks_listing(&at)
                            && resumed.0.is_delete()
                            && KeyMatch::LockKey.matches(&resumed.1))
                });
            last = Some((i, backed_off));
        }

        let runs: Vec<RunRecord> = spawned.into_iter().map(|s| sim.join(s)).collect();
        assert!(
            !runs[0].is_ok() || runs[0].lock_free,
            "a commits lock-free: {}",
            runs[0].detail()
        );
        assert!(!runs[1].lock_free, "b commits under the lock");
        tally.schedules += 1;
        if runs.iter().all(RunRecord::is_ok) {
            tally.both_ok += 1;
        }
        let log = sim.store().log();
        let lock_free_listings = log
            .iter()
            .filter(|r| r.client == a && OpMatch::AnyList.matches(r.op) && r.key == "locks/")
            .count();
        if lock_free_listings > 2 {
            tally.waits += 1;
        }
        let yielded = log.iter().any(|put| {
            put.client == b
                && put.op == OpKind::Put
                && KeyMatch::LockKey.matches(&put.key)
                && put.key != "locks/"
                && log
                    .iter()
                    .find(|d| {
                        d.client == b && d.step > put.step && d.op.is_delete() && d.key == put.key
                    })
                    .is_some_and(|d| {
                        !log.iter().any(|w| {
                            w.client == b
                                && w.step > put.step
                                && w.step < d.step
                                && w.op.is_write()
                                && w.key.starts_with("index.h/")
                        })
                    })
                && runs[1].is_ok()
        });
        if yielded {
            tally.yields += 1;
        }
        let report = sim.check_invariants(HANDSHAKE_INVARIANTS);
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

/// Preemption bound of the campaign's handshake search: the least that
/// reaches a lock holder whose session listing came before its lock PUT
/// (`tests::sim_campaign::campaign` runs it; 921 schedules, about 70 s).
pub(super) const HANDSHAKE_CAMPAIGN_K: u32 = 4;

/// The marker handshake's search (04 step 9) at [`GATE_K`]: no schedule
/// replaces a head or leaves a committed snapshot unrestorable, and the
/// search reaches both halves of the handshake (the lock holder yielding to
/// the flagged marker, the lock-free backup waiting for a held lock). With
/// the lock-free side's order swapped (`locks/` probed before the mark) it
/// finds replaced heads at this bound (8 of 434 schedules when it was
/// tried); with the lock holder's swapped (`sessions/` listed before the
/// lock PUT) only at [`HANDSHAKE_CAMPAIGN_K`] (12 of 530), which the
/// campaign runs.
#[test]
fn the_marker_handshake_keeps_heads_immutable() {
    let start = Instant::now();
    let t = Template::encrypted();
    let (s, tally) = handshake_search(
        &t,
        Bound {
            preemptions: GATE_K,
            max_runs: PRECOMMIT_MAX_RUNS,
        },
    );
    eprintln!(
        "handshake search, K = {GATE_K}: {s:?} {tally:?} in {:?}",
        start.elapsed()
    );
    assert!(s.complete, "{s:?}");
    assert_eq!(tally.damaged, 0, "{:?}", tally.first_damage);
    assert!(tally.yields > 0, "the holder never yielded: {tally:?}");
    assert!(
        tally.waits > 0,
        "the lock-free backup never waited: {tally:?}"
    );
    assert!(tally.both_ok > 0, "{tally:?}");
}

/// `VYKAR_SIM_MEASURE=1`: every pre-commit driver search at K = 1 and
/// K = 2, and the gate search at K = 2 and 3, with runs and wall time.
#[test]
fn measure_bounds() {
    if std::env::var("VYKAR_SIM_MEASURE").ok().as_deref() != Some("1") {
        return;
    }
    let t = Template::encrypted();
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

/// `VYKAR_SIM_MEASURE=1`: the searches phase F parts 10c and 10d name, as
/// far as they exist before them, on the encrypted template (runs and wall
/// time, against `PRECOMMIT_MAX_RUNS`): every client still commits under
/// the lock here, so a lock-free search's size is estimated by its
/// lock-mode twin.
#[test]
fn measure_part_10_searches() {
    if std::env::var("VYKAR_SIM_MEASURE").ok().as_deref() != Some("1") {
        return;
    }
    let start = Instant::now();
    let enc = Template::encrypted();
    eprintln!("encrypted template built in {:?}", start.elapsed());
    let start = Instant::now();
    let young = Template::encrypted_young();
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
            encrypted(conditional(lock_pair(HostPlan::Distinct))),
        ),
        k1(
            "lock_free_pair_is_clean: pair-hostname",
            &enc,
            encrypted(conditional(lock_pair(HostPlan::SameHostname))),
        ),
        k1(
            "lock_free_backup_against_a_locked_compact: whole",
            &enc,
            encrypted(conditional(cvc())),
        ),
        k1(
            "lock_free_backup_against_a_locked_compact: stepped",
            &enc,
            encrypted(conditional(stepped(cvc()))),
        ),
        k1(
            "mixed_lock_and_lock_free_backups: pair-distinct",
            &enc,
            encrypted(mixed(lock_pair(HostPlan::Distinct), vec![1])),
        ),
        // 10d.
        k1(
            "lock_free_backup_against_lock_free_compact: stepped advances",
            &enc,
            encrypted(conditional(advances(stepped(cvc())))),
        ),
        k1(
            "lock_free_backup_against_lock_free_compact: stepped crash",
            &enc,
            encrypted(conditional(crash(stepped(cvc())))),
        ),
        k1(
            "lock_free_backup_against_lock_free_compact: young whole",
            &young,
            encrypted(conditional(cvc())),
        ),
        k1(
            "lock_free_backup_against_lock_free_compact: young stepped",
            &young,
            encrypted(conditional(stepped(cvc()))),
        ),
        k1(
            "lock_free_backup_against_lock_free_compact: young stepped advances",
            &young,
            encrypted(conditional(advances(stepped(cvc())))),
        ),
        k1(
            "lock_free_backup_against_lock_free_compact: young stepped crash",
            &young,
            encrypted(conditional(crash(stepped(cvc())))),
        ),
        k1(
            "lock_free_delete_during_compact: stepped",
            &enc,
            encrypted(conditional(stepped(delete_during_compact))),
        ),
        k1(
            "two_lock_free_compacts: stepped",
            &enc,
            encrypted(conditional(stepped(two_compacts))),
        ),
        (
            "three-clients-lockfree (campaign) K = 2".to_string(),
            &enc,
            encrypted(conditional(three_clients())),
            2,
        ),
        (
            "three-clients-mixed (campaign) K = 2".to_string(),
            &enc,
            encrypted(mixed(three_clients(), vec![2])),
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
        let (s, tally) = lock_acquisitions_on(
            &enc,
            hosts,
            Bound {
                preemptions: GATE_K,
                max_runs: 100_000,
            },
        );
        eprintln!(
            "gate {hosts:?} K = {GATE_K} (encrypted): {s:?} {tally:?} in {:?}",
            start.elapsed()
        );
    }
}
