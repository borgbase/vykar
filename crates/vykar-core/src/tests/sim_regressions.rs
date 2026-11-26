//! Regression seeds of the simulation (08 step 5): driver cases under
//! `crates/vykar-core/tests/sim-regressions/*.json`, each with the exact
//! damage signatures it must show and the decisions its choices took when it
//! was saved. A protocol fix that removes a hazard makes its seeds fail
//! until their `expect` is updated; a machine that gained or lost a step
//! makes them fail as drifted. `VYKAR_SIM_PROMOTE=<trace file>` prints a
//! seed for a trace (name from `VYKAR_SIM_PROMOTE_NAME`, else the file
//! stem).
//!
//! Every seed runs on the encrypted template (since 10-single-protocol
//! step 2; the plaintext template went, and with it the `<name>-conditional`
//! seeds of phase F part 9, which were that template on the conditional
//! store). A seed without `storage` runs on the fallback store
//! (`StoreKind::Plain`, every seed recorded before 04 step 8), where its
//! clients take the lock with the marker handshake; a `<name>-lockfree` seed
//! is the same case on the conditional store, where its backups commit
//! lock-free (phase F part 10c: each lock decision becomes the commit mark
//! and the probe of `locks/`), and a `mixed-…` seed has a lock client
//! beside a lock-free one.

// Printing a seed is the point of the promotion helper; every other path
// asserts.
#![allow(clippy::print_stderr)]

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use crate::sim::driver::*;

#[derive(Debug, Serialize, Deserialize)]
struct Seed {
    /// The file stem.
    name: String,
    /// Where it came from, and which plan fixes the hazard it shows.
    note: String,
    /// The exact signature set the case must give. Never contains Unknown.
    expect: BTreeSet<Signature>,
    /// What the decisions 0..case.choices.len() did when the seed was saved:
    /// the action and the label of what ran. A seed whose schedule no longer
    /// means the same (a machine gained or lost a step) fails as drifted
    /// instead of silently testing something else.
    at: Vec<(Action, String)>,
    case: Case,
}

fn seed_dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/sim-regressions")
}

/// The decisions of `o` that `choices` covered.
fn taken(o: &Outcome, choices: usize) -> Vec<(Action, String)> {
    o.trace
        .entries
        .iter()
        .take(choices)
        .map(|e| (e.action.clone(), e.label.clone()))
        .collect()
}

/// The plan that removes a signature's hazard, as its doc comment names it.
fn fixed_by(s: Signature) -> &'static str {
    match s {
        Signature::TwoLockHolders | Signature::SessionReaped => "04 step 1",
        Signature::AgedLockReaped | Signature::OrphanLockKey => {
            "part 10 (04 step 9: the lock leaves the commit path)"
        }
        Signature::AgedSessionReaped => {
            "part 10 (04 step 9 and the reused section; narrowed by step 7: horizon, I8, \
             young segments)"
        }
        Signature::LateSnapshotWrite => {
            "part 10 (04 step 9 and the reused section; narrowed by step 7: needs a segment \
             older than T_pack)"
        }
        Signature::AgedLockSegmentCollected => {
            "04 step 8 on conditional backends (phase F part 9); on every store by the \
             slow-index-write refusal (phase F part 10b2)"
        }
        Signature::LateHeadWrite
        | Signature::LateHeadThenCompact
        | Signature::LateHeadSegmentCollected
        | Signature::AgedLockPacksCollected => {
            "04 step 8 on conditional backends (phase F part 9); the fallback keeps it"
        }
        Signature::LateIndexWrite => {
            "head writes: 04 step 8 on conditional backends; index.d/ writes: with \
             invariant 7, part 10"
        }
        Signature::LateHeadCreateReplaced => {
            "not planned: mixed modes' residual (a failed lock-free run's late create replaced \
             by a lock client's plain PUT; nothing committed is damaged)"
        }
        Signature::Unknown => "nothing: Unknown is never expected",
    }
}

#[test]
fn regression_seeds_hold() {
    let mut paths: Vec<PathBuf> = std::fs::read_dir(seed_dir())
        .expect("read the seed directory")
        .map(|e| e.expect("seed directory entry").path())
        .filter(|p| p.extension().is_some_and(|x| x == "json"))
        .collect();
    paths.sort();
    assert!(!paths.is_empty(), "no seeds in {}", seed_dir().display());
    let templates = Templates::new();
    let mut failures = Vec::new();
    for path in &paths {
        let stem = path.file_stem().unwrap().to_string_lossy().into_owned();
        let text = std::fs::read_to_string(path).unwrap();
        let seed: Seed = match serde_json::from_str(&text) {
            Ok(s) => s,
            Err(e) => {
                failures.push(format!("seed {stem}: does not parse: {e}"));
                continue;
            }
        };
        if seed.name != stem {
            failures.push(format!("seed {stem}: its name is {:?}", seed.name));
        }
        if seed.expect.contains(&Signature::Unknown) {
            failures.push(format!("seed {stem}: expects Unknown"));
            continue;
        }
        let o = templates.run(&seed.case);
        if seed.case.choices.len() > o.trace.entries.len() {
            failures.push(format!(
                "seed {stem}: {} choices but only {} decisions ran; trim the choices",
                seed.case.choices.len(),
                o.trace.entries.len()
            ));
            continue;
        }
        let at = taken(&o, seed.case.choices.len());
        if at != seed.at {
            failures.push(format!(
                "seed {stem} drifted: its choices now select {at:?} instead of {:?}; \
                 re-derive it from its note\n{}",
                seed.at,
                o.trace.render()
            ));
            continue;
        }
        if o.signatures != seed.expect {
            let gone: Vec<String> = seed
                .expect
                .difference(&o.signatures)
                .map(|s| format!("{s:?} (fixed by {})", fixed_by(*s)))
                .collect();
            failures.push(format!(
                "seed {stem}: signatures {:?}, expected {:?}; a protocol fix removed {}? \
                 update expect and the note\n{}",
                o.signatures,
                seed.expect,
                gone.join(", "),
                o.trace.render()
            ));
        }
    }
    assert!(
        failures.is_empty(),
        "{} of {} seeds fail:\n{}",
        failures.len(),
        paths.len(),
        failures.join("\n\n")
    );
}

/// The outcome class of each run: Ok, the allowed error, or disallowed.
fn run_classes(o: &Outcome) -> Vec<String> {
    o.sim
        .history()
        .runs
        .iter()
        .map(|r| {
            if r.is_ok() {
                "Ok".to_string()
            } else {
                format!("{:?}", r.error_class())
            }
        })
        .collect()
}

/// 04 step 5: no protocol decision reads a client's wall clock, so every
/// seed gives the same decisions, signatures and run outcomes with its
/// clients' clocks skewed by hours (cut to the seed's client count). A
/// difference is a finding, not a reason to relax this.
#[test]
fn regression_seeds_are_skew_invariant() {
    const SKEWS: [i64; 3] = [5 * 3600, -5 * 3600, 13 * 3600];
    let mut paths: Vec<PathBuf> = std::fs::read_dir(seed_dir())
        .expect("read the seed directory")
        .map(|e| e.expect("seed directory entry").path())
        .filter(|p| p.extension().is_some_and(|x| x == "json"))
        .collect();
    paths.sort();
    let templates = Templates::new();
    let mut failures = Vec::new();
    for path in &paths {
        let stem = path.file_stem().unwrap().to_string_lossy().into_owned();
        let seed: Seed = serde_json::from_str(&std::fs::read_to_string(path).unwrap())
            .unwrap_or_else(|e| panic!("seed {stem}: {e}"));
        let plain = templates.run(&seed.case);
        let mut skewed_case = seed.case.clone();
        skewed_case.skews = SKEWS
            .iter()
            .copied()
            .cycle()
            .take(seed.case.clients.len())
            .collect();
        let skewed = templates.run(&skewed_case);
        let n = seed.case.choices.len();
        if taken(&plain, n) != taken(&skewed, n)
            || plain.signatures != skewed.signatures
            || run_classes(&plain) != run_classes(&skewed)
        {
            failures.push(format!(
                "seed {stem} differs under skews {:?}:\nunskewed:\n{}\nskewed:\n{}",
                skewed_case.skews,
                plain.trace.render(),
                skewed.trace.render()
            ));
        }
    }
    assert!(failures.is_empty(), "{}", failures.join("\n\n"));
}

/// `VYKAR_SIM_PROMOTE=<trace file>`: run the trace's case and print it as a
/// seed (note "TODO", expect = the signatures found, choices that no
/// decision read trimmed). A no-op otherwise.
#[test]
fn promote_from_env() {
    let Some(path) = std::env::var_os("VYKAR_SIM_PROMOTE").map(PathBuf::from) else {
        return;
    };
    let text = std::fs::read_to_string(&path).unwrap();
    let mut case = case_from_trace(&text).unwrap();
    let o = Templates::new().run(&case);
    case.choices.truncate(o.trace.entries.len());
    assert!(
        !o.signatures.contains(&Signature::Unknown),
        "an unexplained case is classified or fixed first, never committed as a seed:\n{}",
        o.trace.render()
    );
    let name = std::env::var("VYKAR_SIM_PROMOTE_NAME").unwrap_or_else(|_| {
        path.file_stem()
            .map(|s| s.to_string_lossy().into_owned())
            .unwrap_or_default()
    });
    let seed = Seed {
        name,
        note: "TODO".into(),
        expect: o.signatures.clone(),
        at: taken(&o, case.choices.len()),
        case,
    };
    eprintln!(
        "{}",
        serde_json::to_string_pretty(&seed).expect("seed serializes")
    );
}
