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
//! (`StoreKind::Plain`, every seed recorded before 04 step 8), a
//! single-writer repository: until 10-single-protocol step 7 its clients
//! took the lock with the marker handshake, since then they run the
//! single-writer protocol (the lock decision became the single-writer
//! listings, the commit mark and the probe of the legacy lock directory; a stepped compact
//! registers and makes its single-writer check). A `<name>-lockfree` seed is
//! the same case on the conditional store, where its backups commit
//! lock-free (phase F part 10c: each lock decision becomes the commit mark
//! and the probe of the legacy lock directory). Step 7 re-derived every fallback-store seed
//! from its decisions ([`rederive`]; each note says what it shows since);
//! the `-lockfree` seeds replay unchanged.
//!
//! Format (10-single-protocol step 10): `Case` and `Seed` refuse unknown
//! fields, and `case.template` is required. A seed recorded before step 10
//! does not parse: without `template` ("missing field"), with "Plain", with
//! a `Lock` fault or with `lock_clients` (unknown variant or field). `runs`
//! is the class of each run (`Ok`, its allowed error, or disallowed) the
//! seed must end with. Step 10 re-recorded every seed: the labels
//! "probe-locks" and "lock (locked)" became [`LABEL_LIST_EXCLUSIVE`] and
//! [`LABEL_CONTENDED`] ("wait (locked)"), the 3 h 1 min jump went from the
//! driver's advances (a seed's jump of 3 h 1 min became 46 min), the Lock
//! faults that no client fired were dropped, and the choices were
//! re-derived from their decisions ([`rederive`]); each note says so.
//!
//! Closed by 10-single-protocol step 10 (F1: the pack-age term re-run at
//! the head step): `marker-gap-suspend-before-head` (single-writer, a
//! gapped backup's head with packs past the limit) and
//! `marker-gap-suspend-before-head-packs-swept-lockfree` (multi-writer: that
//! head naming two packs a compact had deleted). Both were `Unknown` before
//! the fix and now refuse the head with `StaleChunksDuringCommit`; the
//! second is judged by invariant 11 only since it reads the clock at the
//! head write's own issue.
//!
//! Retired by 10-single-protocol step 7: the seven `mixed-…` seeds
//! (`mixed-aged-lock-overwrites-lock-free-head`,
//! `mixed-aged-marker-head-overwritten`, `mixed-aged-marker-late-compact-head`,
//! `mixed-aged-session-reaped-before-listing`, `mixed-late-create-replaced`,
//! `mixed-late-lock-head-replaced`, `mixed-late-lock-head-replaces-compact-head`):
//! a lock client's lock-mode commit beside lock-free ones, which no binary
//! makes since then (and the stale-config route that produced one is closed
//! by the re-read of `config`); the single-writer late-head residual they
//! touched is H3's. `orphan-lock-key`: no backup writes a lock key.

// Printing a seed is the point of the promotion helper; every other path
// asserts.
#![allow(clippy::print_stderr)]

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use crate::sim::driver::*;

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Seed {
    /// The file stem.
    name: String,
    /// Where it came from, and what removes the hazard it shows or why it stays.
    note: String,
    /// The exact signature set the case must give. Never contains Unknown.
    expect: BTreeSet<Signature>,
    /// What the decisions 0..case.choices.len() did when the seed was saved:
    /// the action and the label of what ran. A seed whose schedule no longer
    /// means the same (a machine gained or lost a step) fails as drifted
    /// instead of silently testing something else.
    at: Vec<(Action, String)>,
    /// How each run ended ([`run_classes`]): which typed refusal, which
    /// commit.
    runs: Vec<String>,
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

/// What removes a signature's hazard, or why it stays.
fn fixed_by(s: Signature) -> &'static str {
    match s {
        Signature::SessionReaped => "04 step 1",
        Signature::AgedSessionReaped => {
            "not planned: a reap by age of a stalled run's marker is the marker's documented \
             limit (H-committing-reaped); the session horizon, I8 and young segments narrow it"
        }
        Signature::LateSnapshotWrite => {
            "not planned: a snapshot PUT released after its writer ended (R3); young segments \
             narrow it to a segment older than T_pack"
        }
        Signature::LateHeadWrite
        | Signature::LateHeadThenCompact
        | Signature::LateHeadSegmentCollected => {
            "04 step 8 on multi-writer repositories; single-writer ones keep it (H3)"
        }
        Signature::SingleWriterHeadRace => {
            "not planned: the single-writer check is best effort (H-late-head-single)"
        }
        Signature::ExclusiveSessionReaped => {
            "not planned (H1-late-clone; a stall past the write check)"
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
        let runs = run_classes(&o);
        if runs != seed.runs {
            failures.push(format!(
                "seed {stem}: runs ended {runs:?}, expected {:?}; update runs and the note \
                 only with the reason\n{}",
                seed.runs,
                o.trace.render()
            ));
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

/// A case or seed in the format before 10-single-protocol step 10 does not
/// parse, and the error names what is wrong: no `template`, the "Plain"
/// template, a `Lock` fault, `lock_clients`; a seed with a field it does
/// not know.
#[test]
fn an_old_format_case_or_seed_does_not_parse() {
    let case = serde_json::json!({
        "hosts": "Distinct",
        "clients": [[{"Backup": {"shared": true, "fold": false}}]],
        "faults": [],
        "crash": false,
        "choices": [],
        "template": "Encrypted",
    });
    let parses = |v: &serde_json::Value| serde_json::from_value::<Case>(v.clone());
    parses(&case).expect("today's format parses");
    let mut no_template = case.clone();
    no_template.as_object_mut().unwrap().remove("template");
    let mut plain = case.clone();
    plain["template"] = "Plain".into();
    let mut lock_fault = case.clone();
    lock_fault["faults"] =
        serde_json::json!([{"client": 0, "class": "Lock", "nth": 1, "kind": "Fail"}]);
    let mut lock_clients = case.clone();
    lock_clients["lock_clients"] = serde_json::json!([1]);
    for (v, named) in [
        (no_template, "missing field `template`"),
        (plain, "unknown variant `Plain`"),
        (lock_fault, "unknown variant `Lock`"),
        (lock_clients, "unknown field `lock_clients`"),
    ] {
        let text = v.to_string();
        let err = serde_json::from_str::<Case>(&text)
            .expect_err(named)
            .to_string();
        assert!(err.contains(named), "{named}: {err}");
        let err = case_from_trace(&format!("case: {text}\n")).expect_err(named);
        assert!(
            err.starts_with("bad case: ") && err.contains(named),
            "{named}: {err}"
        );
    }
    let seed = serde_json::json!({
        "name": "s",
        "note": "n",
        "expect": [],
        "at": [],
        "runs": [],
        "case": case,
    });
    serde_json::from_value::<Seed>(seed.clone()).expect("today's seed parses");
    let mut extra = seed;
    extra["lock_clients"] = serde_json::json!([]);
    let err = serde_json::from_value::<Seed>(extra)
        .expect_err("an unknown field")
        .to_string();
    assert!(err.contains("unknown field `lock_clients`"), "{err}");
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
        runs: run_classes(&o),
        case,
    };
    eprintln!(
        "{}",
        serde_json::to_string_pretty(&seed).expect("seed serializes")
    );
}

/// Labels of a backup's commit protection (10-single-protocol step 7).
fn is_protection(label: &str) -> bool {
    label.starts_with("wait")
        || label.starts_with(LABEL_LIST_WRITERS)
        || label.starts_with(LABEL_MARK_COMMITTING)
        || label.starts_with(LABEL_LIST_EXCLUSIVE)
}

/// Whether the new decision labelled `new` ends what the old decision
/// labelled `old` did, for a step of the same client: the same label; a
/// pre-head listing that started the commit machine (its local steps
/// following) and one that did; a contended attempt and a contended one; a
/// protection that gave up and one that did; a protection and the commit
/// step that follows one that ended otherwise.
fn ends_intent(old: &str, new: &str) -> bool {
    if old == new {
        return true;
    }
    let commit_started = |l: &str| l.starts_with(LABEL_LIST_EXCLUSIVE) && !l.contains('(');
    if commit_started(old) && commit_started(new) {
        return true;
    }
    if old == LABEL_CONTENDED && new == LABEL_CONTENDED {
        return true;
    }
    if old.ends_with("(gave up)") && new.ends_with("(gave up)") {
        return true;
    }
    // A protection that ended otherwise than it did: the commit machine's
    // first step.
    is_protection(old) && !is_protection(new)
}

/// Re-derive `seed`'s choices on today's protocol from what its decisions
/// did (10-single-protocol steps 7 and 10): each old decision's action is
/// taken again where the driver offers it, and a step of a client is
/// repeated (up to 12 inserted steps) until its label ends what the old
/// step did ([`ends_intent`]): a lock attempt became the single-writer
/// listings, the mark and the pre-head listing; a stepped compact's new
/// steps (its registration, its single-writer check, the head's EXISTS and
/// read-back) are taken on the way to the old one's next step. A decision
/// is mapped by its action, not its index, so a removed advance shifts no
/// other choice. `Err` names the first old decision
/// no new decision can stand for.
fn rederive(templates: &Templates, seed: &Seed) -> Result<Case, String> {
    let mut case = seed.case.clone();
    case.choices.clear();
    let mut o = templates.run(&case);
    for (i, (action, label)) in seed.at.iter().enumerate() {
        let mut inserted = 0;
        loop {
            let n = case.choices.len();
            let entry = o
                .trace
                .entries
                .get(n)
                .ok_or_else(|| format!("decision {i} ({action:?} {label}): the case ended"))?;
            let offered = entry.actions.iter().position(|a| a == action);
            let pick = match (offered, action) {
                (Some(p), _) => p,
                // The client's command ended earlier than it did (a commit
                // that refuses sooner): the old decision has no stand-in.
                (None, Action::Step { client, .. })
                    if !entry
                        .actions
                        .iter()
                        .any(|a| matches!(a, Action::Step { client: c, .. } if c == client)) =>
                {
                    eprintln!(
                        "REDERIVE {}: decision {i} ({action:?} {label}) skipped: its client ended",
                        seed.name
                    );
                    break;
                }
                // Stop once every client is done: the remaining clients'
                // steps first (a client that waits now where it did not).
                (None, Action::Stop) if inserted < 40 => {
                    inserted += 1;
                    entry
                        .actions
                        .iter()
                        .position(|a| matches!(a, Action::Step { pick: 0, .. }))
                        .ok_or_else(|| format!("decision {i}: neither Stop nor a step offered"))?
                }
                _ => {
                    return Err(format!(
                        "decision {i} ({action:?} {label}): not offered at {n} ({:?})",
                        entry.actions
                    ));
                }
            };
            if matches!(action, Action::Stop) && offered.is_none() {
                case.choices.push(u16::try_from(pick).expect("few actions"));
                o = templates.run(&case);
                continue;
            }
            case.choices.push(u16::try_from(pick).expect("few actions"));
            o = templates.run(&case);
            let new = &o.trace.entries[n].label;
            let is_step = matches!(action, Action::Step { .. });
            if !is_step || ends_intent(label, new) {
                break;
            }
            let compact = label.starts_with("compact") && new.starts_with("compact");
            if !(is_protection(new) || compact || inserted < 3) || inserted >= 12 {
                return Err(format!(
                    "decision {i} ({action:?} {label}): the step became {new:?}"
                ));
            }
            inserted += 1;
        }
    }
    Ok(case)
}

/// `VYKAR_SIM_REDERIVE=<dir>`: re-derive every seed ([`rederive`]) and
/// write each as `<dir>/<name>.json` (its note kept, its choices, `at`,
/// expect and runs new) with its trace, printing per seed the old and new
/// signatures and run outcomes, for review before a seed is replaced
/// (10-single-protocol steps 7 and 10 re-derived the corpus this way). A
/// seed that replays is written unchanged, with its trace, so the out
/// directory holds the whole corpus.
/// `VYKAR_SIM_REDERIVE_ONLY=<a,b>` names the seeds, and
/// `VYKAR_SIM_REDERIVE_FROM=<dir>` reads them from another directory (a
/// seed edited by hand first). A no-op otherwise.
#[test]
fn rederive_from_env() {
    let Some(out) = std::env::var_os("VYKAR_SIM_REDERIVE").map(PathBuf::from) else {
        return;
    };
    std::fs::create_dir_all(&out).unwrap();
    let only = std::env::var("VYKAR_SIM_REDERIVE_ONLY").ok();
    let dir = std::env::var_os("VYKAR_SIM_REDERIVE_FROM").map_or_else(seed_dir, PathBuf::from);
    let mut paths: Vec<PathBuf> = std::fs::read_dir(dir)
        .unwrap()
        .map(|e| e.unwrap().path())
        .filter(|p| p.extension().is_some_and(|x| x == "json"))
        .collect();
    paths.sort();
    let templates = Templates::new();
    for path in &paths {
        let stem = path.file_stem().unwrap().to_string_lossy().into_owned();
        if only
            .as_deref()
            .is_some_and(|o| !o.split(',').any(|x| x == stem))
        {
            continue;
        }
        let seed: Seed = serde_json::from_str(&std::fs::read_to_string(path).unwrap()).unwrap();
        let replay = templates.run(&seed.case);
        if taken(&replay, seed.case.choices.len()) == seed.at {
            eprintln!(
                "REDERIVE {stem}: replays; signatures {:?} (expect {:?}); runs {:?}",
                replay.signatures,
                seed.expect,
                run_classes(&replay)
            );
            std::fs::write(
                out.join(format!("{stem}.json")),
                serde_json::to_string_pretty(&seed).unwrap() + "\n",
            )
            .unwrap();
            std::fs::write(out.join(format!("{stem}.trace")), replay.trace.render()).unwrap();
            continue;
        }
        match rederive(&templates, &seed) {
            Ok(case) => {
                let o = templates.run(&case);
                eprintln!(
                    "REDERIVE {stem}: re-derived; signatures {:?} (expect {:?}); runs {:?}",
                    o.signatures,
                    seed.expect,
                    run_classes(&o)
                );
                let new = Seed {
                    name: seed.name.clone(),
                    note: seed.note.clone(),
                    expect: o.signatures.clone(),
                    at: taken(&o, case.choices.len()),
                    runs: run_classes(&o),
                    case,
                };
                std::fs::write(
                    out.join(format!("{stem}.json")),
                    serde_json::to_string_pretty(&new).unwrap() + "\n",
                )
                .unwrap();
                std::fs::write(out.join(format!("{stem}.trace")), o.trace.render()).unwrap();
            }
            Err(e) => eprintln!("REDERIVE {stem}: FAILED {e}"),
        }
    }
}
