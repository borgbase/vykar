//! The long simulation campaign (08 step 5): the wider bounded searches of
//! `sim_exhaustive::campaign_scenarios`, then random cases with seeds from
//! the clock, until a deadline. Workers take scenarios from one shared
//! counter, in the list's order, until [`SCENARIO_SHARE`] of the budget has
//! passed (a search still running then stops [`SEARCH_GRACE_SHARE`] later,
//! incomplete), and run random cases from then (or once the list is done)
//! until the deadline; the report says which scenarios completed, which
//! were cut short and which never started, per family. A campaign that ran
//! no random case fails; one of [`FULL_BUDGET`] or more also fails unless
//! every family completed a scenario and a K >= 2 search completed. A case with damage of an unknown shape is a
//! failure: its trace is written and the run fails at the end, after every
//! failure of the run was collected. Each worker's case in progress is on
//! disk, so a panic (or a CI timeout) leaves it replayable.
//!
//! `make sim-campaign` runs it: `VYKAR_SIM_CAMPAIGN_SECS` (required; unset,
//! the test is a no-op), `VYKAR_SIM_JOBS` (default: the available
//! parallelism) and `VYKAR_SIM_TRACE_DIR`.

// The campaign reports on stderr; every other path asserts.
#![allow(clippy::print_stderr)]

use std::cell::Cell;
use std::collections::BTreeMap;
use std::panic::AssertUnwindSafe;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Mutex, MutexGuard, PoisonError};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use proptest::test_runner::{TestCaseError, TestError};

use super::sim_exhaustive::{HANDSHAKE_CAMPAIGN_K, backup, campaign_scenarios, handshake_search};
use super::sim_explore::{campaign_cases, runner_with};
use crate::sim::driver::*;
use crate::sim::explore::{Bound, Control, explore_with};

/// Runs per search at most (a K = 3 search of three clients would
/// otherwise outlast any deadline check between searches).
const SEARCH_MAX_RUNS: usize = 200_000;

/// The share of the budget in which workers take new scenarios; the rest
/// is the random cases'.
const SCENARIO_SHARE: f64 = 0.6;

/// How much of the budget a search in progress at the scenario deadline
/// still gets before it stops, incomplete: the random cases keep their
/// share.
const SEARCH_GRACE_SHARE: f64 = 0.02;

/// From this budget on (the weekly job's), a campaign fails unless every
/// family completed at least one scenario and at least one K >= 2 search
/// completed; below it that only warns.
const FULL_BUDGET: Duration = Duration::from_secs(3600);

struct CampaignConfig {
    deadline: Instant,
    /// No new scenario is started after this.
    scenario_deadline: Instant,
    /// A search still running stops here, incomplete.
    search_deadline: Instant,
    jobs: usize,
    trace_dir: PathBuf,
    scenarios: Vec<(String, Case, u32)>,
    /// Random rounds per worker after the scenarios; None: until the
    /// deadline.
    random_rounds: Option<usize>,
    /// Cases per random round.
    cases_per_round: u32,
    /// Stop every worker after this many failures.
    max_failures: usize,
    /// Whether an outcome is a failure.
    failure: fn(&Outcome) -> bool,
}

#[derive(Debug, Default)]
struct CampaignReport {
    runs: usize,
    random_cases: usize,
    signatures: BTreeMap<Signature, usize>,
    failures: Vec<PathBuf>,
    /// The first trace of each signature seen, for regression seeds.
    examples: BTreeMap<Signature, PathBuf>,
    /// Per search started: its index in the scenario list, name, runs,
    /// complete, elapsed.
    searches: Vec<(usize, String, usize, bool, Duration)>,
}

/// A scenario's family, for the report: which store and template it runs
/// on (see `sim_exhaustive::campaign_scenarios`).
fn family(case: &Case) -> &'static str {
    match (case.template, case.storage) {
        (TemplateKind::Encrypted, StoreKind::ConditionalOverwrite) => "mixed",
        (TemplateKind::Encrypted, _) => "lockfree",
        (_, StoreKind::Conditional) => "conditional",
        _ => "plain",
    }
}

/// Per family: (completed, incomplete, not started).
type FamilyCounts = BTreeMap<&'static str, (usize, usize, usize)>;

fn family_counts(cfg: &CampaignConfig, r: &CampaignReport) -> FamilyCounts {
    let mut out = FamilyCounts::new();
    let mut started = vec![None; cfg.scenarios.len()];
    for (i, _, _, complete, _) in &r.searches {
        started[*i] = Some(*complete);
    }
    for ((_, case, _), s) in cfg.scenarios.iter().zip(started) {
        let e = out.entry(family(case)).or_default();
        match s {
            Some(true) => e.0 += 1,
            Some(false) => e.1 += 1,
            None => e.2 += 1,
        }
    }
    out
}

/// The signatures 04 step 8 closes on a store that honours put-if-absent.
const CLOSED_ON_CONDITIONAL: [Signature; 4] = [
    Signature::LateHeadWrite,
    Signature::LateHeadThenCompact,
    Signature::LateHeadSegmentCollected,
    Signature::AgedLockPacksCollected,
];

/// The signatures closed on every store. `AgedLockSegmentCollected` (phase
/// F part 10b2: the 12 h object grace and the slow-index-write refusal)
/// needs no exemption for a head PUT released late: the signature requires
/// a non-released head PUT, and a released-late one is
/// `LateHeadSegmentCollected`'s, which stays closed on the conditional
/// store only.
const CLOSED_EVERYWHERE: [Signature; 1] = [Signature::AgedLockSegmentCollected];

/// The campaign's rule: damage no signature explains, damage of a shape
/// closed on every store, or, where every client creates heads with
/// put-if-absent, damage of a shape 04 step 8 closed there. That is the
/// conditional store (vykar-server) and, without lock clients, the store
/// whose plain PUT overwrites (local, SFTP, S3: phase F part 10c decides it
/// as the conditional store, V1); with a lock client there its plain head
/// PUTs can replace heads again, so those shapes stay open.
/// `LateHeadCreateReplaced` (a lock client's plain PUT over a failed
/// lock-free run's late create) is open only there: without lock clients,
/// or on the conditional store, it is a failure.
fn unknown(o: &Outcome) -> bool {
    let case = &o.trace.case;
    o.signatures.contains(&Signature::Unknown)
        || CLOSED_EVERYWHERE.iter().any(|s| o.signatures.contains(s))
        || (o.signatures.contains(&Signature::LateHeadCreateReplaced)
            && (case.lock_clients.is_empty() || case.storage == StoreKind::Conditional))
        || (case.storage.honours_creates()
            && case.lock_clients.is_empty()
            && CLOSED_ON_CONDITIONAL
                .iter()
                .any(|s| o.signatures.contains(s)))
}

fn lock(r: &Mutex<CampaignReport>) -> MutexGuard<'_, CampaignReport> {
    r.lock().unwrap_or_else(PoisonError::into_inner)
}

fn current_path(dir: &Path, worker: usize) -> PathBuf {
    dir.join(format!("worker-{worker}.current"))
}

/// Write the case a worker is about to run, as a trace's first line.
fn write_current(dir: &Path, worker: usize, case: &Case) {
    let line = format!(
        "case: {}\n",
        serde_json::to_string(case).expect("case serializes")
    );
    std::fs::write(current_path(dir, worker), line).expect("write the current case");
}

/// Count `o` and keep the first trace of each of its signatures; if it is
/// a failure and `name` is given, write its trace and record it. Returns
/// whether it is a failure.
fn record(
    cfg: &CampaignConfig,
    report: &Mutex<CampaignReport>,
    o: &Outcome,
    name: Option<&str>,
) -> bool {
    let failed = (cfg.failure)(o);
    let mut r = lock(report);
    r.runs += 1;
    for s in &o.signatures {
        *r.signatures.entry(*s).or_insert(0) += 1;
        if !r.examples.contains_key(s)
            && let Ok(p) = o.trace.write_in(&cfg.trace_dir, &format!("example-{s:?}"))
        {
            r.examples.insert(*s, p);
        }
    }
    if failed && let Some(name) = name {
        let path = o
            .trace
            .write_in(&cfg.trace_dir, name)
            .expect("write a failing trace");
        r.failures.push(path);
    }
    failed
}

fn stop(cfg: &CampaignConfig, report: &Mutex<CampaignReport>) -> bool {
    Instant::now() >= cfg.deadline || lock(report).failures.len() >= cfg.max_failures
}

fn worker(cfg: &CampaignConfig, report: &Mutex<CampaignReport>, next: &AtomicUsize, j: usize) {
    let templates = Templates::new();
    loop {
        if stop(cfg, report) {
            return;
        }
        if Instant::now() >= cfg.scenario_deadline {
            break;
        }
        let pos = next.fetch_add(1, Ordering::Relaxed);
        let Some((name, base, k)) = cfg.scenarios.get(pos) else {
            break;
        };
        let mut n = 0usize;
        let e = explore_with(
            templates.get(base.template),
            base,
            Bound {
                preemptions: *k,
                max_runs: SEARCH_MAX_RUNS,
            },
            |c| write_current(&cfg.trace_dir, j, c),
            |o| {
                n += 1;
                record(cfg, report, o, Some(&format!("{name}-{n}")));
                if stop(cfg, report) || Instant::now() >= cfg.search_deadline {
                    Control::Stop
                } else {
                    Control::Continue
                }
            },
        );
        eprintln!(
            "campaign worker {j}: {name}: {} runs, complete {}, {} clean, {:?}",
            e.search.runs, e.search.complete, e.clean, e.elapsed
        );
        lock(report).searches.push((
            pos,
            name.clone(),
            e.search.runs,
            e.search.complete,
            e.elapsed,
        ));
    }
    let mut round = 0usize;
    while cfg.random_rounds.is_none_or(|n| round < n) && !stop(cfg, report) {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_or(0, |d| d.as_nanos() as u64);
        let seed = nanos ^ ((j as u64) << 56);
        eprintln!("campaign worker {j} round {round} seed {seed}");
        // After a round's first failure every run is a shrink rerun: it is
        // judged but not counted.
        let shrinking = Cell::new(false);
        let result = runner_with(cfg.cases_per_round, seed).run(&campaign_cases(), |case| {
            write_current(&cfg.trace_dir, j, &case);
            let o = templates.run(&case);
            let failed = if shrinking.get() {
                (cfg.failure)(&o)
            } else {
                lock(report).random_cases += 1;
                // A failure is written once, for the shrunk case, below.
                record(cfg, report, &o, None)
            };
            if failed {
                shrinking.set(true);
                return Err(TestCaseError::fail("campaign failure"));
            }
            Ok(())
        });
        match result {
            Ok(()) => {}
            Err(TestError::Fail(_, case)) => {
                let o = templates.run(&case);
                let path = o
                    .trace
                    .write_in(&cfg.trace_dir, &format!("random-{seed}"))
                    .expect("write a failing trace");
                lock(report).failures.push(path);
            }
            Err(e) => panic!("campaign worker {j}: {e}"),
        }
        round += 1;
    }
}

/// Run the campaign: `cfg.jobs` workers, each with its own template,
/// taking the next scenario from a shared counter until
/// `cfg.scenario_deadline` (or the list's end), then random rounds. A
/// worker that panics is a failure pointing at its `.current` case.
fn run_campaign(cfg: &CampaignConfig) -> CampaignReport {
    std::fs::create_dir_all(&cfg.trace_dir).expect("create the trace directory");
    let report = Mutex::new(CampaignReport::default());
    let next = AtomicUsize::new(0);
    std::thread::scope(|s| {
        for j in 0..cfg.jobs {
            let (report, next) = (&report, &next);
            s.spawn(move || {
                let r = std::panic::catch_unwind(AssertUnwindSafe(|| worker(cfg, report, next, j)));
                let current = current_path(&cfg.trace_dir, j);
                match r {
                    Ok(()) => {
                        let _ = std::fs::remove_file(&current);
                    }
                    Err(_) => {
                        eprintln!(
                            "campaign worker {j} panicked; its case is in {}",
                            current.display()
                        );
                        lock(report).failures.push(current);
                    }
                }
            });
        }
    });
    report.into_inner().unwrap_or_else(PoisonError::into_inner)
}

fn print_report(cfg: &CampaignConfig, r: &CampaignReport, elapsed: Duration) -> FamilyCounts {
    eprintln!(
        "campaign: {} runs ({} random cases) in {elapsed:?}",
        r.runs, r.random_cases
    );
    let mut searches: Vec<_> = r.searches.iter().collect();
    searches.sort_by_key(|s| s.0);
    for (_, name, runs, complete, took) in searches {
        eprintln!("  search {name}: {runs} runs, complete {complete}, {took:?}");
    }
    let counts = family_counts(cfg, r);
    for (family, (done, cut, unstarted)) in &counts {
        eprintln!(
            "  scenarios {family}: {done} completed, {cut} incomplete, {unstarted} not started"
        );
    }
    eprintln!("  signatures: {:?}", r.signatures);
    for (s, p) in &r.examples {
        eprintln!("  example {s:?}: {}", p.display());
    }
    for p in &r.failures {
        eprintln!("  failure: {}", p.display());
    }
    counts
}

#[test]
fn campaign() {
    let Some(secs) = std::env::var_os("VYKAR_SIM_CAMPAIGN_SECS") else {
        return;
    };
    let secs: u64 = secs
        .to_str()
        .and_then(|s| s.parse().ok())
        .expect("VYKAR_SIM_CAMPAIGN_SECS is a number of seconds");
    let jobs = std::env::var("VYKAR_SIM_JOBS")
        .ok()
        .map(|s| s.parse::<usize>().expect("VYKAR_SIM_JOBS is a number"))
        .unwrap_or_else(|| std::thread::available_parallelism().map_or(1, |n| n.get()))
        .max(1);
    let trace_dir = std::env::var_os("VYKAR_SIM_TRACE_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|| std::env::temp_dir().join("vykar-sim"));
    eprintln!(
        "campaign: {secs} s, {jobs} workers, traces in {}",
        trace_dir.display()
    );
    let start = Instant::now();
    let budget = Duration::from_secs(secs);
    let cfg = CampaignConfig {
        deadline: start + budget,
        scenario_deadline: start + budget.mul_f64(SCENARIO_SHARE),
        search_deadline: start + budget.mul_f64(SCENARIO_SHARE + SEARCH_GRACE_SHARE),
        jobs,
        trace_dir: trace_dir.clone(),
        scenarios: campaign_scenarios(),
        random_rounds: None,
        cases_per_round: 256,
        max_failures: 50,
        failure: unknown,
    };
    // Beside the workers: the marker handshake's gate search at the
    // campaign's bound (phase F part 10c).
    let (report, handshake) = std::thread::scope(|s| {
        let handshake = s.spawn(|| {
            let start = Instant::now();
            let (search, tally) = handshake_search(
                &Template::encrypted(),
                Bound {
                    preemptions: HANDSHAKE_CAMPAIGN_K,
                    max_runs: SEARCH_MAX_RUNS,
                },
            );
            eprintln!(
                "campaign: handshake search K = {HANDSHAKE_CAMPAIGN_K}: {search:?} {tally:?} in {:?}",
                start.elapsed()
            );
            (search, tally)
        });
        let report = run_campaign(&cfg);
        (
            report,
            handshake.join().expect("the handshake search panicked"),
        )
    });
    let counts = print_report(&cfg, &report, start.elapsed());
    let unstarted: usize = counts.values().map(|c| c.2).sum();
    if unstarted > 0 {
        eprintln!(
            "campaign: WARNING: {unstarted} of {} scenarios were not started in {secs} s",
            cfg.scenarios.len()
        );
    }
    let coverage = coverage_gaps(&cfg, &report, &counts);
    for gap in &coverage {
        eprintln!("campaign: WARNING: {gap}");
    }
    let (search, tally) = handshake;
    assert!(
        search.complete,
        "the handshake search did not complete: {search:?}"
    );
    assert_eq!(
        tally.damaged, 0,
        "the handshake search: {:?}",
        tally.first_damage
    );
    assert!(
        report.failures.is_empty(),
        "{} failing cases; traces in {}",
        report.failures.len(),
        trace_dir.display()
    );
    assert!(
        report.random_cases > 0,
        "the campaign ran no random case in {secs} s"
    );
    assert!(
        budget < FULL_BUDGET || coverage.is_empty(),
        "the campaign's coverage in {secs} s: {coverage:?}"
    );
}

/// The campaign's random cases narrowed to the shape of its stale-session
/// unknowns (phase F part 10d fix round 3): time jumps, a gate-stepped
/// compact (one command made `Compact` when the case has none), the
/// encrypted template on the conditional store, on the store whose plain PUT
/// overwrites, or on that store with client b in lock mode (as
/// [`campaign_cases`]: a handle without put-if-absent on vykar-server's
/// write-once heads is not a deployment), any hosts.
fn stale_session_cases() -> impl proptest::strategy::Strategy<Value = Case> {
    use proptest::prelude::*;
    (super::sim_explore::general(), 0u8..3).prop_map(|(mut c, kind)| {
        if !c.clients.iter().flatten().any(|x| *x == Command::Compact)
            && let Some(last) = c.clients.last_mut().and_then(|x| x.last_mut())
        {
            *last = Command::Compact;
        }
        Case {
            advances: true,
            maintenance: Maintenance::Stepped,
            template: TemplateKind::Encrypted,
            storage: if kind == 0 {
                StoreKind::Conditional
            } else {
                StoreKind::ConditionalOverwrite
            },
            lock_clients: if kind == 2 { vec![1] } else { vec![] },
            ..c
        }
    })
}

/// `VYKAR_SIM_FOCUS_CASES=<n>`: run about n cases of
/// [`stale_session_cases`] on all workers (`VYKAR_SIM_JOBS`), unshrunk,
/// and print the signatures seen; each case the campaign would fail is
/// written to `VYKAR_SIM_TRACE_DIR` and fails the run at the end. A no-op
/// otherwise.
#[test]
fn stale_session_search() {
    let Some(total) = std::env::var("VYKAR_SIM_FOCUS_CASES").ok().map(|s| {
        s.parse::<usize>()
            .expect("VYKAR_SIM_FOCUS_CASES is a number")
    }) else {
        return;
    };
    let jobs = std::env::var("VYKAR_SIM_JOBS")
        .ok()
        .map(|s| s.parse::<usize>().expect("VYKAR_SIM_JOBS is a number"))
        .unwrap_or_else(|| std::thread::available_parallelism().map_or(1, |n| n.get()))
        .max(1);
    let trace_dir = std::env::var_os("VYKAR_SIM_TRACE_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|| std::env::temp_dir().join("vykar-sim"));
    std::fs::create_dir_all(&trace_dir).expect("create the trace directory");
    let base = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.as_nanos() as u64);
    let per_round = 50usize;
    let rounds = total.div_ceil(per_round);
    let next = AtomicUsize::new(0);
    let report = Mutex::new(CampaignReport::default());
    let start = Instant::now();
    std::thread::scope(|s| {
        for j in 0..jobs {
            let (next, report, trace_dir) = (&next, &report, &trace_dir);
            s.spawn(move || {
                let templates = Templates::new();
                loop {
                    let round = next.fetch_add(1, Ordering::Relaxed);
                    if round >= rounds {
                        return;
                    }
                    let seed = base ^ (round as u64).rotate_left(40);
                    let n = Cell::new(0usize);
                    let result =
                        runner_with(per_round as u32, seed).run(&stale_session_cases(), |case| {
                            write_current(trace_dir, j, &case);
                            let o = templates.run(&case);
                            n.set(n.get() + 1);
                            let mut r = lock(report);
                            r.random_cases += 1;
                            for sig in &o.signatures {
                                *r.signatures.entry(*sig).or_insert(0) += 1;
                            }
                            if unknown(&o) {
                                let path = o
                                    .trace
                                    .write_in(trace_dir, &format!("focus-{seed}-{}", n.get()))
                                    .expect("write a failing trace");
                                eprintln!("stale_session_search: failure {}", path.display());
                                r.failures.push(path);
                            }
                            Ok(())
                        });
                    if let Err(e) = result {
                        panic!("stale_session_search worker {j}: {e}");
                    }
                    let _ = std::fs::remove_file(current_path(trace_dir, j));
                }
            });
        }
    });
    let r = lock(&report);
    eprintln!(
        "stale_session_search: {} cases on {jobs} workers in {:?}, base seed {base}",
        r.random_cases,
        start.elapsed()
    );
    eprintln!("stale_session_search: signatures {:?}", r.signatures);
    assert!(
        r.failures.is_empty(),
        "{} failing cases: {:?}",
        r.failures.len(),
        r.failures
    );
}

/// What the campaign failed to cover: a family with no completed scenario,
/// no completed K >= 2 search.
fn coverage_gaps(cfg: &CampaignConfig, r: &CampaignReport, counts: &FamilyCounts) -> Vec<String> {
    let mut gaps: Vec<String> = ["plain", "conditional", "lockfree", "mixed"]
        .into_iter()
        .filter(|f| counts.get(f).is_none_or(|c| c.0 == 0))
        .map(|f| format!("no {f} scenario completed"))
        .collect();
    let wide_done = r
        .searches
        .iter()
        .any(|(i, _, _, complete, _)| *complete && cfg.scenarios[*i].2 >= 2);
    if !wide_done {
        gaps.push("no K >= 2 search completed".into());
    }
    gaps
}

/// The campaign's order reaches every family and a wider search early: the
/// first twenty scenarios hold a K = 1 scenario of each family and a
/// K >= 2 search, and the first half of the list holds at least a third
/// of the K >= 2 searches.
#[test]
fn campaign_order_reaches_every_family_early() {
    let all = campaign_scenarios();
    let head = &all[..20];
    for f in ["plain", "conditional", "lockfree", "mixed"] {
        assert!(
            head.iter().any(|(_, c, k)| *k == 1 && family(c) == f),
            "no K = 1 {f} scenario among the first 20: {:?}",
            head.iter().map(|s| &s.0).collect::<Vec<_>>()
        );
    }
    assert!(head.iter().any(|(_, _, k)| *k >= 2));
    let wide = all.iter().filter(|(_, _, k)| *k >= 2).count();
    let early = all[..all.len() / 2]
        .iter()
        .filter(|(_, _, k)| *k >= 2)
        .count();
    assert!(
        early * 3 >= wide,
        "{early} of {wide} K >= 2 searches in the first half"
    );
}

/// What the weekly job uploads, checked locally: a campaign whose rule
/// fails on H3 writes the failing trace, and the trace replays to H3.
#[test]
fn campaign_writes_failing_traces() {
    let dir = tempfile::tempdir().unwrap();
    let h3 = Case {
        hosts: HostPlan::Distinct,
        clients: vec![vec![backup()], vec![backup()]],
        faults: vec![FaultSpec {
            client: 0,
            class: KeyClass::IndexHead,
            nth: 1,
            kind: FaultKind::Delay,
        }],
        crash: false,
        advances: false,
        choices: vec![],
        policy: Policy::Continue,
        skews: vec![],
        maintenance: Maintenance::Whole,
        storage: StoreKind::Plain,
        template: TemplateKind::Plain,
        lock_clients: vec![],
    };
    let deadline = Instant::now() + Duration::from_secs(600);
    let cfg = CampaignConfig {
        deadline,
        scenario_deadline: deadline,
        search_deadline: deadline,
        jobs: 1,
        trace_dir: dir.path().to_path_buf(),
        scenarios: vec![("h3".into(), h3, 1)],
        random_rounds: Some(0),
        cases_per_round: 1,
        max_failures: 1,
        failure: |o| o.signatures.contains(&Signature::LateHeadWrite),
    };
    let report = run_campaign(&cfg);
    assert_eq!(report.failures.len(), 1, "{report:?}");
    let path = &report.failures[0];
    let text = std::fs::read_to_string(path).unwrap();
    let case = case_from_trace(&text).unwrap();
    let o = run_case(&Template::new(), &case);
    assert!(
        o.signatures.contains(&Signature::LateHeadWrite),
        "{}",
        o.trace.render()
    );
    assert!(!current_path(dir.path(), 0).exists());
}

/// `LateHeadCreateReplaced` is open only with a lock client on the store
/// whose plain PUT replaces heads: the same outcome judged as a case
/// without lock clients, or on the conditional store, is a failure.
#[test]
fn late_head_create_replaced_is_closed_without_lock_clients() {
    let mut o = run_case(
        &Template::encrypted(),
        &super::sim_driver::late_create_replaced_case(),
    );
    assert_eq!(
        o.signatures,
        std::collections::BTreeSet::from([Signature::LateHeadCreateReplaced]),
        "{}",
        o.trace.render()
    );
    assert!(!unknown(&o));
    let case = o.trace.case.clone();
    o.trace.case.lock_clients.clear();
    assert!(unknown(&o), "without lock clients");
    o.trace.case = Case {
        storage: StoreKind::Conditional,
        ..case
    };
    assert!(unknown(&o), "on the conditional store");
}
