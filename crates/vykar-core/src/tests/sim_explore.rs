//! Random exploration of the driver's cases (08 step 4). A general family
//! must show no damage of an unknown shape; a narrow family must find H3
//! under random schedules and shrink it to a short case, and the
//! one-hostname family (H1's before 04 step 1) must stay clean. The
//! seed is `VYKAR_SIM_SEED` (else fixed), the case count of the general
//! family `VYKAR_SIM_CASES`; `VYKAR_SIM_MEASURE=1` runs the measurement.

// Printing is the point of the env-driven helpers here (projections,
// replays, the measurement); every other path asserts.
#![allow(clippy::print_stderr)]

use std::time::{Duration, Instant};

use proptest::prelude::*;
use proptest::strategy::ValueTree;
use proptest::test_runner::{Config, RngSeed, TestCaseError, TestError, TestRunner};

use crate::sim::driver::*;

const DEFAULT_SEED: u64 = 0x5eed_0008_0004;

fn seed() -> u64 {
    std::env::var("VYKAR_SIM_SEED")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(DEFAULT_SEED)
}

fn general_cases(default: u32) -> u32 {
    std::env::var("VYKAR_SIM_CASES")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(default)
}

/// Shrinking is bounded by iterations only (no real-time bound, which would
/// make the shrunk case depend on the machine's load).
fn runner(cases: u32) -> TestRunner {
    runner_with(cases, seed())
}

/// [`runner`] with its own seed (the campaign's).
pub(super) fn runner_with(cases: u32, seed: u64) -> TestRunner {
    TestRunner::new(Config {
        cases,
        rng_seed: RngSeed::Fixed(seed),
        failure_persistence: None,
        max_shrink_iters: 200,
        ..Config::default()
    })
}

fn choices() -> impl Strategy<Value = Vec<u16>> {
    prop::collection::vec(prop_oneof![3 => Just(0u16), 1 => 1u16..8], 0..48)
}

fn backup_command() -> impl Strategy<Value = Command> {
    (any::<bool>(), prop::bool::weighted(0.2))
        .prop_map(|(shared, fold)| Command::Backup { shared, fold })
}

fn command() -> impl Strategy<Value = Command> {
    prop_oneof![
        4 => backup_command(),
        1 => Just(Command::Compact),
        1 => Just(Command::DeleteControl),
    ]
}

fn key_class() -> impl Strategy<Value = KeyClass> {
    prop_oneof![
        Just(KeyClass::IndexHead),
        Just(KeyClass::IndexObject),
        Just(KeyClass::Snapshot),
        Just(KeyClass::Lock),
        Just(KeyClass::Session),
    ]
}

fn fault_kind() -> impl Strategy<Value = FaultKind> {
    prop_oneof![
        Just(FaultKind::Fail),
        Just(FaultKind::FailApplied),
        Just(FaultKind::Delay),
        Just(FaultKind::Duplicate),
    ]
}

fn fault(
    clients: usize,
    class: impl Strategy<Value = KeyClass>,
) -> impl Strategy<Value = FaultSpec> {
    (0..clients as u8, class, 1u8..=2, fault_kind()).prop_map(|(client, class, nth, kind)| {
        FaultSpec {
            client,
            class,
            nth,
            kind,
        }
    })
}

fn hosts() -> impl Strategy<Value = HostPlan> {
    prop_oneof![Just(HostPlan::Distinct), Just(HostPlan::SameHostname)]
}

/// Keep the first `DeleteControl` of a case; later ones become `Compact`.
fn one_delete(mut clients: Vec<Vec<Command>>) -> Vec<Vec<Command>> {
    let mut seen = false;
    for c in clients.iter_mut().flatten() {
        if *c == Command::DeleteControl {
            if seen {
                *c = Command::Compact;
            }
            seen = true;
        }
    }
    clients
}

/// The store kinds a random case is drawn on: the fallback and the
/// conditional store (04 step 8); shrinking prefers the fallback.
fn storage() -> impl Strategy<Value = StoreKind> {
    prop_oneof![Just(StoreKind::Plain), Just(StoreKind::Conditional)]
}

/// 2-3 clients, 1-2 commands each, at most one `DeleteControl`, 0-2
/// faults of any class and kind, a crash or not, any hosts, time jumps;
/// compact whole or gate-stepped (no jump inside a stepped compact, see
/// `Maintenance`; shrinking prefers whole); the fallback or the
/// conditional store.
pub(super) fn general() -> impl Strategy<Value = Case> {
    (2usize..=3)
        .prop_flat_map(|n| {
            (
                prop::collection::vec(prop::collection::vec(command(), 1..=2), n),
                prop::collection::vec(fault(n, key_class()), 0..=2),
                any::<bool>(),
                hosts(),
                choices(),
                (prop::bool::weighted(0.25), storage()),
            )
        })
        .prop_map(
            |(clients, faults, crash, hosts, choices, (stepped, storage))| Case {
                hosts,
                clients: one_delete(clients),
                faults,
                crash,
                advances: true,
                choices,
                policy: Policy::First,
                skews: vec![],
                maintenance: if stepped {
                    Maintenance::Stepped
                } else {
                    Maintenance::Whole
                },
                storage,
            },
        )
}

fn two_backups() -> Vec<Vec<Command>> {
    let b = Command::Backup {
        shared: true,
        fold: false,
    };
    vec![vec![b.clone()], vec![b]]
}

/// The one-hostname family (H1's before 04 step 1): two backups on cloned
/// machines, no faults, no crash, no time jumps (an aged lock is reaped by
/// design).
fn one_hostname_family() -> impl Strategy<Value = Case> {
    choices().prop_map(|choices| Case {
        hosts: HostPlan::SameHostname,
        clients: two_backups(),
        faults: vec![],
        crash: false,
        advances: false,
        choices,
        policy: Policy::First,
        skews: vec![],
        maintenance: Maintenance::Whole,
        storage: StoreKind::Plain,
    })
}

/// H3's family: two backups on distinct hosts, no crash, and one delayed
/// head PUT of either client; the schedule, and with it when (and whether)
/// the delayed PUT lands, is random. Narrower than "any write fault on a
/// head, an index object or a snapshot": with that, about 1 case in 1000
/// shows H3 (only a delayed head of the client that commits first does), and
/// a search of a few hundred cases mostly misses it; with this, about 1 in
/// 80 does.
fn h3_family() -> impl Strategy<Value = Case> {
    (0u8..2, choices()).prop_map(|(client, choices)| Case {
        hosts: HostPlan::Distinct,
        clients: two_backups(),
        faults: vec![FaultSpec {
            client,
            class: KeyClass::IndexHead,
            nth: 1,
            kind: FaultKind::Delay,
        }],
        crash: false,
        advances: true,
        choices,
        policy: Policy::First,
        skews: vec![],
        maintenance: Maintenance::Whole,
        storage: StoreKind::Plain,
    })
}

/// Report a failing case: the seed, the case as JSON, and its trace file.
fn report_failure(test: &str, template: &Template, case: &Case) -> String {
    let o = run_case(template, case);
    let path = o.trace.write(test).map_or_else(
        |e| format!("(not written: {e})"),
        |p| p.display().to_string(),
    );
    format!(
        "seed {}; minimal case ({} non-default choices, {} faults):\n{}\ntrace: {path}\n{}",
        seed(),
        case.non_default_choices(),
        case.faults.len(),
        serde_json::to_string(case).expect("case serializes"),
        o.report
    )
}

#[test]
fn random_cases_have_no_unknown_violation() {
    let template = Template::new();
    let result = runner(general_cases(64)).run(&general(), |case| {
        let o = run_case(&template, &case);
        prop_assert!(
            !o.signatures.contains(&Signature::Unknown),
            "damage of an unknown shape"
        );
        Ok(())
    });
    if let Err(TestError::Fail(_, case)) = &result {
        panic!(
            "{}",
            report_failure("random-cases-unknown", &template, case)
        );
    }
    result.unwrap();
}

/// Search `family` (up to `cases` cases) for `sig`. The minimal case
/// must have at most 15 non-default choices and replay to the same canonical
/// trace three times.
fn finds(
    test: &str,
    family: impl Strategy<Value = Case>,
    cases: u32,
    sig: Signature,
    fixed_by: &str,
) {
    let template = Template::new();
    let result = runner(cases).run(&family, |case| {
        let o = run_case(&template, &case);
        if o.signatures.contains(&sig) {
            return Err(TestCaseError::fail(format!("{sig:?}")));
        }
        Ok(())
    });
    let case = match result {
        Err(TestError::Fail(_, case)) => case,
        Ok(()) => panic!(
            "{sig:?} no longer reproduces under random exploration: {fixed_by} fixed it? \
             move this to the regressions"
        ),
        Err(e) => panic!("{e}"),
    };
    let first = run_case(&template, &case);
    assert!(
        first.signatures.contains(&sig),
        "the minimal case does not show {sig:?}:\n{}",
        report_failure(test, &template, &case)
    );
    eprintln!(
        "{test}: found {sig:?}; minimal case has {} non-default choices of {} decisions",
        case.non_default_choices(),
        first.trace.entries.len()
    );
    assert!(
        case.non_default_choices() <= 15 && case.faults.len() <= 2,
        "the minimal case is not short:\n{}",
        report_failure(test, &template, &case)
    );
    for _ in 0..3 {
        let again = run_case(&template, &case);
        assert_eq!(
            again.trace.canonical(),
            first.trace.canonical(),
            "the minimal case does not replay:\n{}",
            report_failure(test, &template, &case)
        );
    }
}

/// Since 04 step 1 (installation ID) no schedule of the one-hostname
/// family shows any signature.
#[test]
fn one_hostname_exploration_is_clean() {
    let template = Template::new();
    let result = runner(256).run(&one_hostname_family(), |case| {
        let o = run_case(&template, &case);
        prop_assert!(o.signatures.is_empty(), "signatures {:?}", o.signatures);
        Ok(())
    });
    if let Err(TestError::Fail(_, case)) = &result {
        panic!("{}", report_failure("one-hostname-clean", &template, case));
    }
    result.unwrap();
}

#[test]
fn random_exploration_finds_h3() {
    finds(
        "finds-h3",
        h3_family(),
        512,
        Signature::LateHeadWrite,
        "04 step 8 (create-if-absent heads)",
    );
}

/// H3's family on a conditional store (04 step 8): a delayed head create
/// that lands later finds its generation taken or extends the head it was
/// built on, so no case shows a late head write's damage, or anything
/// unexplained.
#[test]
fn random_exploration_on_conditional_finds_no_late_head() {
    let template = Template::new();
    let family = h3_family().prop_map(|c| Case {
        storage: StoreKind::Conditional,
        ..c
    });
    let result = runner(256).run(&family, |case| {
        let o = run_case(&template, &case);
        for sig in [
            Signature::LateHeadWrite,
            Signature::LateHeadThenCompact,
            Signature::LateHeadSegmentCollected,
            Signature::Unknown,
        ] {
            prop_assert!(!o.signatures.contains(&sig), "{sig:?}");
        }
        Ok(())
    });
    if let Err(TestError::Fail(_, case)) = &result {
        panic!(
            "{}",
            report_failure("conditional-no-late-head", &template, case)
        );
    }
    result.unwrap();
}

#[test]
fn random_cases_replay_identically() {
    let template = Template::new();
    let result = runner(16).run(&general(), |case| {
        let a = run_case(&template, &case);
        let b = run_case(&template, &case);
        prop_assert_eq!(a.trace.canonical(), b.trace.canonical());
        prop_assert_eq!(a.signatures, b.signatures);
        Ok(())
    });
    if let Err(TestError::Fail(_, case)) = &result {
        panic!("{}", report_failure("replay", &template, case));
    }
    result.unwrap();
}

fn percentile(sorted: &[Duration], p: usize) -> Duration {
    sorted
        .get((sorted.len() * p / 100).min(sorted.len().saturating_sub(1)))
        .copied()
        .unwrap_or_default()
}

/// `VYKAR_SIM_MEASURE=1`: per-case cost and replay share of the general
/// family, and the wall time of the tests above.
#[test]
fn measure() {
    if std::env::var("VYKAR_SIM_MEASURE").ok().as_deref() != Some("1") {
        return;
    }
    let template = Template::new();
    let mut times = Vec::new();
    let mut decisions = Vec::new();
    let mut failing = 0usize;
    let mut replayed = 0usize;
    let mut signatures = std::collections::BTreeMap::new();
    let mut runner = runner(general_cases(64));
    for _ in 0..general_cases(64) {
        let case = general().new_tree(&mut runner).expect("case").current();
        let start = Instant::now();
        let o = run_case(&template, &case);
        times.push(start.elapsed());
        decisions.push(o.trace.entries.len());
        for s in &o.signatures {
            *signatures.entry(*s).or_insert(0usize) += 1;
        }
        if !o.signatures.is_empty() {
            failing += 1;
            let again = run_case(&template, &case);
            if again.trace.canonical() == o.trace.canonical() {
                replayed += 1;
            } else {
                let (a, b) = (o.trace.canonical(), again.trace.canonical());
                let at = a
                    .iter()
                    .zip(&b)
                    .position(|(x, y)| x != y)
                    .unwrap_or(a.len().min(b.len()));
                eprintln!(
                    "replay differs at entry {at}:\n  {:?}\n  {:?}\n{}",
                    a.get(at),
                    b.get(at),
                    serde_json::to_string(&case).expect("case serializes")
                );
            }
        }
    }
    times.sort();
    decisions.sort_unstable();
    eprintln!(
        "per case: median {:?}, p95 {:?}, max {:?}; decisions median {}, max {}",
        percentile(&times, 50),
        percentile(&times, 95),
        times.last().copied().unwrap_or_default(),
        decisions.get(decisions.len() / 2).copied().unwrap_or(0),
        decisions.last().copied().unwrap_or(0)
    );
    eprintln!(
        "signatures: {signatures:?}; failing cases {failing}, replayed identically {replayed}"
    );
    for (name, f) in [
        (
            "random_cases_have_no_unknown_violation",
            random_cases_have_no_unknown_violation as fn(),
        ),
        (
            "one_hostname_exploration_is_clean",
            one_hostname_exploration_is_clean,
        ),
        ("random_exploration_finds_h3", random_exploration_finds_h3),
        (
            "random_cases_replay_identically",
            random_cases_replay_identically,
        ),
    ] {
        let start = Instant::now();
        f();
        eprintln!("{name}: {:?}", start.elapsed());
    }
}
