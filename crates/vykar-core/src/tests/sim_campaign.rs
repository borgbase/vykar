//! The long simulation campaign (08 step 5): the wider bounded searches of
//! `sim_exhaustive::campaign_scenarios`, then random cases with seeds from
//! the clock, until a deadline. A case with damage of an unknown shape is a
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
use std::sync::{Mutex, MutexGuard, PoisonError};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use proptest::test_runner::{TestCaseError, TestError};

use super::sim_exhaustive::{backup, campaign_scenarios};
use super::sim_explore::{general, runner_with};
use crate::sim::driver::*;
use crate::sim::explore::{Bound, Control, explore_with};

/// Runs per search at most (a K = 3 search of three clients would
/// otherwise outlast any deadline check between searches).
const SEARCH_MAX_RUNS: usize = 200_000;

struct CampaignConfig {
    deadline: Instant,
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
    /// Per search: name, runs, complete, elapsed.
    searches: Vec<(String, usize, bool, Duration)>,
}

/// The campaign's rule: damage no signature explains.
fn unknown(o: &Outcome) -> bool {
    o.signatures.contains(&Signature::Unknown)
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

fn worker(cfg: &CampaignConfig, report: &Mutex<CampaignReport>, j: usize) {
    let template = Template::new();
    for (name, base, k) in cfg.scenarios.iter().skip(j).step_by(cfg.jobs) {
        if stop(cfg, report) {
            return;
        }
        let mut n = 0usize;
        let e = explore_with(
            &template,
            base,
            Bound {
                preemptions: *k,
                max_runs: SEARCH_MAX_RUNS,
            },
            |c| write_current(&cfg.trace_dir, j, c),
            |o| {
                n += 1;
                record(cfg, report, o, Some(&format!("{name}-{n}")));
                if stop(cfg, report) {
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
        lock(report)
            .searches
            .push((name.clone(), e.search.runs, e.search.complete, e.elapsed));
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
        let result = runner_with(cfg.cases_per_round, seed).run(&general(), |case| {
            write_current(&cfg.trace_dir, j, &case);
            let o = run_case(&template, &case);
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
                let o = run_case(&template, &case);
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
/// worker j taking scenarios j, j + jobs, …, then random rounds. A worker
/// that panics is a failure pointing at its `.current` case.
fn run_campaign(cfg: &CampaignConfig) -> CampaignReport {
    std::fs::create_dir_all(&cfg.trace_dir).expect("create the trace directory");
    let report = Mutex::new(CampaignReport::default());
    std::thread::scope(|s| {
        for j in 0..cfg.jobs {
            let report = &report;
            s.spawn(move || {
                let r = std::panic::catch_unwind(AssertUnwindSafe(|| worker(cfg, report, j)));
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

fn print_report(r: &CampaignReport, elapsed: Duration) {
    eprintln!(
        "campaign: {} runs ({} random cases) in {elapsed:?}",
        r.runs, r.random_cases
    );
    for (name, runs, complete, took) in &r.searches {
        eprintln!("  search {name}: {runs} runs, complete {complete}, {took:?}");
    }
    eprintln!("  signatures: {:?}", r.signatures);
    for (s, p) in &r.examples {
        eprintln!("  example {s:?}: {}", p.display());
    }
    for p in &r.failures {
        eprintln!("  failure: {}", p.display());
    }
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
    let cfg = CampaignConfig {
        deadline: start + Duration::from_secs(secs),
        jobs,
        trace_dir: trace_dir.clone(),
        scenarios: campaign_scenarios(),
        random_rounds: None,
        cases_per_round: 256,
        max_failures: 50,
        failure: unknown,
    };
    let report = run_campaign(&cfg);
    print_report(&report, start.elapsed());
    assert!(
        report.failures.is_empty(),
        "{} failing cases; traces in {}",
        report.failures.len(),
        trace_dir.display()
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
    };
    let cfg = CampaignConfig {
        deadline: Instant::now() + Duration::from_secs(600),
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
