//! Hazards that need two clients in flight at once, scripted with gates
//! (08 step 3).
//!
//! Each scenario spawns one client, holds it at a gate, runs or spawns the
//! other, and resumes the first. Only one client is runnable at a time, so
//! virtual time stays in Auto mode. Gates select an operation by (client,
//! nth matching operation), never by a key the run invents; the keys a
//! verdict needs (markers) are read back from the op log.
//!
//! H4-segment is a regression since 05 step 8 (index garbage is collected
//! by age): [`h4_segment_regression`]. H1 and H1-session are regressions
//! since 04 step 1 (installation ID): [`h1_regression`],
//! [`h1_session_regression`]. H2-marker is a regression since 04 step 2
//! (non-resurrecting marker refresh): [`h2_marker_regression`]. 04 step 5
//! (ages on the store's clock) moved H4's skew route to the regressions and
//! kept H4 on a store clock step ([`Hosts::StoreStepped`]).
//!
//! Retired by 10-single-protocol step 7: H4 and H4-skew. Their subject was
//! compact's own lock route after a lost lock (a compact under the
//! maintenance lock that lost it to an age reap and deleted on); since step
//! 7 compact runs in a session on every repository and no command reaches
//! that route (step 9 deleted it). The neutralised H4 and H4-segment
//! variants and the no-prerequisite check of H4 went with them. H1 and
//! H1-late-clone hold exclusive sessions ([`Call::ExclusiveDelete`]); H2 is
//! retired (10-single-protocol step 9).

use std::time::Duration;

use super::checker::{Invariant, Report, ViolationKind};
use super::env::{INIT_PID_NS, SimMachine};
use super::fault::{ClientId, KeyMatch, OpKind, OpMatch};
use super::gate::{GateId, GateRule, Paused};
use super::hazards::{
    HazardOutcome, base_controls, broken, deleted_in_run, different_damage, reproduced,
};
use super::history::{AllowedError, ErrorClass, RunRecord};
use super::log::OpRecord;
use super::runner::{Call, GATE_TIMEOUT, Sim, SourceTree, seeded_bytes};

/// Where the two clients of a scenario live.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Hosts {
    /// Cloned VMs: hostname "ubuntu" and one installation on both, boot IDs
    /// "boot-a" and "boot-b", both booted long before the scenario. H1's
    /// condition before 04 step 1.
    SameHostname,
    /// Two installations that share the hostname "ubuntu" (two machines
    /// left at a distribution's default name).
    Namesakes,
    /// Containers on one kernel sharing a hostname and a config volume:
    /// one installation, one boot ID, PID namespaces `INIT_PID_NS + 1` and
    /// `+ 2`.
    SharedUts,
    /// Hostnames "host-a" and "host-b".
    Distinct,
    /// Distinct hosts, b's wall clock 6 h 1 min ahead. Before 04 step 5 b
    /// reaped a's live lock by age (04 issue 1); since then it reaps nothing
    /// (ages are on the store's clock).
    Skewed,
    /// Distinct hosts; the scenario steps the store's clock forward by what
    /// it needs right before the reaper runs (an NTP correction on the
    /// server).
    StoreStepped,
    /// As `SameHostname`, but b's machine boots 30 s after the scenario's
    /// clients are placed, and a scenario that uses it lets a minute pass
    /// once a holds its lock: b's uptime is then shorter than the age of
    /// a's live entry, so b takes it for its own earlier boot (04 step 1's
    /// residual, H1-late-clone).
    LateClone,
    /// As `LateClone`, but the two machines are namesakes (installations
    /// "install-a" and "install-b"): only the installation comparison keeps
    /// b from taking a's live entry for its own earlier boot.
    LateNamesake,
}

/// The checked set for these scenarios: STEP1 and invariants 13, 10 and 11.
pub const CHECKED: &[Invariant] = &[
    Invariant::CommittedSnapshotsRestore,
    Invariant::AcknowledgedCommitsDurable,
    Invariant::NoSilentRemoval,
    Invariant::IndexIntegrity,
    Invariant::AllowedErrors,
    Invariant::ExclusiveSessions,
    Invariant::OrphanGrace,
    Invariant::MarkerGapCommit,
];

const FILE_SIZE: usize = 64 * 1024;

/// A scenario body: `Err` carries an early outcome (a broken precondition).
type Scenario = Result<HazardOutcome, HazardOutcome>;

fn settle(s: Scenario) -> HazardOutcome {
    s.unwrap_or_else(|o| o)
}

fn seeded(name: &str) -> SourceTree {
    SourceTree::seeded(name, 3, FILE_SIZE)
}

fn pair(sim: &mut Sim, hosts: Hosts) -> (ClientId, ClientId) {
    match hosts {
        Hosts::SameHostname => (
            sim.client_on("a", "ubuntu", Some("boot-a")),
            sim.client_on("b", "ubuntu", Some("boot-b")),
        ),
        Hosts::Namesakes => (
            sim.client_at(
                "a",
                SimMachine::new("ubuntu", Some("boot-a")).installation(Some("install-a")),
            ),
            sim.client_at(
                "b",
                SimMachine::new("ubuntu", Some("boot-b")).installation(Some("install-b")),
            ),
        ),
        Hosts::SharedUts => (
            sim.client_at(
                "a",
                SimMachine::new("ubuntu", Some("boot-1")).pid_ns(Some(INIT_PID_NS + 1)),
            ),
            sim.client_at(
                "b",
                SimMachine::new("ubuntu", Some("boot-1")).pid_ns(Some(INIT_PID_NS + 2)),
            ),
        ),
        Hosts::Distinct => (
            sim.client_on("a", "host-a", Some("boot-a")),
            sim.client_on("b", "host-b", Some("boot-b")),
        ),
        Hosts::LateClone | Hosts::LateNamesake => {
            let boot = chrono::Duration::from_std(sim.time().elapsed()).expect("sim time in range")
                + chrono::Duration::seconds(30);
            let (ia, ib) = if hosts == Hosts::LateNamesake {
                ("install-a", "install-b")
            } else {
                ("install-ubuntu", "install-ubuntu")
            };
            (
                sim.client_at(
                    "a",
                    SimMachine::new("ubuntu", Some("boot-a")).installation(Some(ia)),
                ),
                sim.client_at(
                    "b",
                    SimMachine::new("ubuntu", Some("boot-b"))
                        .installation(Some(ib))
                        .booted_at(boot),
                ),
            )
        }
        Hosts::Skewed => {
            let (a, b) = (sim.client("a"), sim.client("b"));
            sim.set_skew(b, chrono::Duration::seconds(6 * 3600 + 60));
            (a, b)
        }
        Hosts::StoreStepped => (sim.client("a"), sim.client("b")),
    }
}

/// Step the store's clock forward by `by` for [`Hosts::StoreStepped`]; a
/// no-op for the other placements.
fn step_before_reaper(sim: &mut Sim, hosts: Hosts, by: chrono::Duration) {
    if hosts == Hosts::StoreStepped {
        sim.step_store_clock(by);
    }
}

fn ok(run: &RunRecord, what: &str) -> Result<(), HazardOutcome> {
    if run.is_ok() {
        Ok(())
    } else {
        Err(broken(format!("{what} failed: {}", run.detail())))
    }
}

fn allowed(run: &RunRecord, e: AllowedError) -> bool {
    run.error_class() == Some(&ErrorClass::Allowed(e))
}

/// Wait for `gate`; a gate not reached is a broken scenario, named by
/// `what` so it can be retargeted.
fn reach(sim: &Sim, gate: GateId, what: &str) -> Result<Paused, HazardOutcome> {
    match sim.poll_paused(gate, GATE_TIMEOUT) {
        Ok(Some(p)) => Ok(p),
        Ok(None) => Err(broken(format!(
            "{what}: the run ended without reaching its gate; retarget the gate"
        ))),
        Err(e) => Err(broken(format!("{what}: {e}"))),
    }
}

/// Applied records of `run` (by `client`) matching `f`.
fn records_of(
    sim: &Sim,
    client: ClientId,
    run: usize,
    f: impl Fn(&OpRecord) -> bool,
) -> Vec<OpRecord> {
    sim.store()
        .log()
        .into_iter()
        .filter(|r| r.client == client && r.run == Some(run) && r.applied() && f(r))
        .collect()
}

/// The first key matching `f` that `run` wrote.
fn first_write_of(sim: &Sim, run: &RunRecord, f: impl Fn(&str) -> bool) -> Option<String> {
    records_of(sim, run.client, run.index, |r| r.op.is_write() && f(&r.key))
        .into_iter()
        .next()
        .map(|r| r.key)
}

/// The session marker `run` wrote.
fn marker_key_of(sim: &Sim, run: &RunRecord) -> Option<String> {
    first_write_of(sim, run, |k| {
        k.starts_with("sessions/") && k.ends_with(".json")
    })
}

/// Steps at which `client`'s run `run` deleted `key`.
fn steps_deleting(sim: &Sim, client: ClientId, run: usize, key: &str) -> Vec<u64> {
    records_of(sim, client, run, |r| r.deleted_keys().contains(&key))
        .iter()
        .map(|r| r.step)
        .collect()
}

/// `c`'s third listing of `sessions/` (the first is its registration's,
/// 10-single-protocol step 8, the second the open's), after every pack is
/// uploaded: on a single-writer repository the listing before its commit
/// mark (step 7), unflagged; on a multi-writer one the pre-head listing
/// after its mark.
fn before_commit(c: ClientId) -> GateRule {
    GateRule::new(
        c,
        OpMatch::Kind(OpKind::ListMeta),
        KeyMatch::Exact("sessions/".into()),
    )
    .nth(3)
}

/// The exclusive session's marker `run` wrote (`sessions/x-*.json`).
fn exclusive_key_of(sim: &Sim, run: &RunRecord) -> Option<String> {
    first_write_of(sim, run, |k| {
        k.starts_with("sessions/x-") && k.ends_with(".json")
    })
}

/// `c` wrote no exclusive session marker.
fn took_no_exclusive(sim: &Sim, c: ClientId) -> bool {
    !sim.store()
        .keys_written_by(c, "sessions/x-")
        .iter()
        .any(|k| k.ends_with(".json"))
}

/// The outcome of an exclusive scenario whose clients registered no
/// exclusive session.
fn no_exclusive_taken(evidence: String) -> HazardOutcome {
    HazardOutcome::PrerequisiteNotReproduced {
        prerequisite: "an exclusive session held by a",
        evidence: format!(
            "the clients registered no exclusive session
{evidence}"
        ),
    }
}

fn run_violation(report: &Report, run: usize, kind: ViolationKind) -> bool {
    report.has_kind(kind, &format!("run {run}"))
}

fn outcomes(ra: &RunRecord, rb: &RunRecord) -> String {
    format!("a: {:?}\nb: {:?}", ra.outcome, rb.outcome)
}

fn controls(report: &Report) -> Result<(), HazardOutcome> {
    base_controls(report).map_err(broken)
}

// ── H1 ──────────────────────────────────────────────────────────────────

/// The host placements that shared a hostname, and so met H1's rule
/// before 04 step 1. `LateNamesake` is the one that only the installation
/// comparison keeps clean.
const ONE_HOSTNAME: &[Hosts] = &[
    Hosts::SameHostname,
    Hosts::Namesakes,
    Hosts::SharedUts,
    Hosts::LateNamesake,
];

/// Placements whose b boots after a's write: the scenario lets a minute
/// pass once a holds its entry.
fn boots_late(hosts: Hosts) -> bool {
    matches!(hosts, Hosts::LateClone | Hosts::LateNamesake)
}

/// H1 as a regression: on cloned machines, namesakes (also one booted
/// after a's write) and containers that share a hostname, b keeps a's live
/// exclusive marker (ActiveSessions) and a finishes.
pub fn h1_regression() -> Report {
    not_reproduced_on("H1", ONE_HOSTNAME, h1_with)
}

/// H1-session as a regression: the same placements, compact refuses on
/// a's live marker (ActiveSessions) and a commits.
pub fn h1_session_regression() -> Report {
    not_reproduced_on("H1-session", ONE_HOSTNAME, h1_session_with)
}

/// H1-late-clone: a clone of a's installation that booted after a wrote
/// its exclusive marker takes the live session for one of its own earlier
/// boot and reaps it; a then writes under b's session. The residual of 04
/// step 1's uptime rule.
pub fn h1_late_clone() -> HazardOutcome {
    h1_with(Hosts::LateClone)
}

/// H1-late-clone's end state: the sim, a's and b's runs and the report
/// (`CHECKED`); `Err` with the outcome of a scenario that ended early.
#[cfg(test)]
pub(crate) fn h1_late_clone_runs() -> Result<H1End, HazardOutcome> {
    h1_runs(Hosts::LateClone)
}

/// The end state of an H1 scenario: the sim, a's run, b's run, the report.
pub(crate) type H1End = (Sim, RunRecord, RunRecord, Report);

/// A clean report if every placement gives `NotReproduced`; else one
/// `DisallowedError` per other outcome, subject `"<id> on <hosts>"`.
fn not_reproduced_on(id: &str, hosts: &[Hosts], run: fn(Hosts) -> HazardOutcome) -> Report {
    let mut out = Report::default();
    for &h in hosts {
        let outcome = run(h);
        if !matches!(outcome, HazardOutcome::NotReproduced { .. }) {
            out.add(
                ViolationKind::DisallowedError,
                format!("{id} on {h:?}"),
                format!("{outcome:?}"),
            );
        }
    }
    out
}

/// H1: a live exclusive session's marker reaped by another client that
/// shares the holder's hostname, whose holder then writes under the
/// reaper's session. The clients are placed by `hosts`; since 04 step 1 no
/// placement reproduces it.
///
/// A deletes t0 and is held at its snapshot delete, after its write check.
/// B deletes t1: its registration's reap removes A's marker when it shares
/// A's hostname and takes A's process for dead, and B is held at its own
/// first write check. Then A resumes. Both delete as exclusive sessions
/// ([`Call::ExclusiveDelete`], 10-single-protocol step 8), as `check
/// --repair`, upgrade and pack maps run; until step 8 both held the
/// maintenance lock and the reap was the stale-lock sweep's. A refused B
/// (`ActiveSessions`, A's marker kept) is the fix.
pub fn h1_with(hosts: Hosts) -> HazardOutcome {
    settle(h1_scenario(hosts))
}

fn h1_scenario(hosts: Hosts) -> Scenario {
    let (sim, ra, rb, report) = h1_runs(hosts)?;
    controls(&report)?;
    let key = exclusive_key_of(&sim, &ra);
    let reaped: Vec<u64> = key
        .as_deref()
        .map(|k| steps_deleting(&sim, rb.client, rb.index, k))
        .unwrap_or_default();
    let inside = reaped
        .iter()
        .any(|s| (ra.first_step..ra.last_step).contains(s));
    let evidence = format!(
        "{}\na's exclusive marker {key:?} deleted by b at {reaped:?}\n{report}",
        outcomes(&ra, &rb)
    );
    Ok(
        if inside
            && run_violation(
                &report,
                ra.index,
                ViolationKind::ExclusiveWriteUnderOtherHold,
            )
        {
            reproduced(&sim, evidence)
        } else if allowed(&rb, AllowedError::ActiveSessions)
            && ra.is_ok()
            && !report.violated(Invariant::ExclusiveSessions)
        {
            HazardOutcome::NotReproduced { evidence }
        } else {
            different_damage(evidence)
        },
    )
}

/// The runs of [`h1_scenario`] up to its report.
fn h1_runs(hosts: Hosts) -> Result<H1End, HazardOutcome> {
    let mut sim = Sim::new();
    let c = sim.client("c");
    ok(&sim.backup(c, "t0", &seeded("t0")), "control backup t0")?;
    ok(&sim.backup(c, "t1", &seeded("t1")), "control backup t1")?;
    let (a, b) = pair(&mut sim, hosts);
    let ga = sim.gate(GateRule::new(
        a,
        OpMatch::AnyDelete,
        KeyMatch::Prefix("snapshots/".into()),
    ));
    // b's write check (10-single-protocol step 8: the EXISTS of its
    // exclusive marker).
    let gb = sim.gate(GateRule::new(
        b,
        OpMatch::Kind(OpKind::Exists),
        KeyMatch::Prefix("sessions/x-".into()),
    ));
    let sa = sim.spawn(
        a,
        Call::ExclusiveDelete {
            names: vec!["t0".into()],
        },
    );
    reach(&sim, ga, "a's snapshot delete")?;
    if boots_late(hosts) {
        // b boots after a's marker write.
        sim.advance(Duration::from_secs(60));
    }
    let sb = sim.spawn(
        b,
        Call::ExclusiveDelete {
            names: vec!["t1".into()],
        },
    );
    let (ra, rb) = match sim.poll_paused(gb, GATE_TIMEOUT).map_err(broken)? {
        Some(_) => {
            sim.resume(ga);
            let ra = sim.join(sa);
            sim.resume(gb);
            (ra, sim.join(sb))
        }
        None => {
            let rb = sim.join(sb);
            if !allowed(&rb, AllowedError::ActiveSessions) {
                if took_no_exclusive(&sim, a) && took_no_exclusive(&sim, b) {
                    sim.resume(ga);
                    let ra = sim.join(sa);
                    return Err(no_exclusive_taken(outcomes(&ra, &rb)));
                }
                return Err(broken(format!(
                    "b ended before its write check, not ActiveSessions: {:?}",
                    rb.outcome
                )));
            }
            sim.resume(ga);
            (sim.join(sa), rb)
        }
    };
    let report = sim.check_invariants(CHECKED);
    Ok((sim, ra, rb, report))
}

/// H1-session: a live backup's session marker reaped by a compact that
/// shares its hostname; compact deletes the backup's uploaded packs, and
/// the backup fails. The clients are placed by `hosts`; since 04 step 1 no
/// placement reproduces it.
///
/// A backs up a1 and is held at its single-writer listing before its
/// commit mark, after phase 1 uploaded its packs. B compacts beside A's live
/// backup (since 04 step 7 it proceeds, keeping A's uploads). Then A
/// resumes.
pub fn h1_session_with(hosts: Hosts) -> HazardOutcome {
    settle(h1_session_scenario(hosts))
}

fn h1_session_scenario(hosts: Hosts) -> Scenario {
    let mut sim = Sim::new();
    let c = sim.client("c");
    ok(&sim.backup(c, "t0", &seeded("t0")), "control backup t0")?;
    let (a, b) = pair(&mut sim, hosts);
    // The 1st listing of sessions/ is the open's, the 2nd the single-writer
    // listing before the commit mark (on a multi-writer repository: the
    // pre-head listing after it), after every pack is uploaded.
    let ga = sim.gate(before_commit(a));
    let sa = sim.spawn(
        a,
        Call::Backup {
            snapshot: "a1".into(),
            tree: seeded("a1"),
        },
    );
    reach(&sim, ga, "a's first commit step")?;
    let a_packs = sim.store().keys_written_by(a, "packs/");
    if a_packs.is_empty() {
        return Err(broken("a paused before uploading; retarget the gate"));
    }
    if boots_late(hosts) {
        // b boots after a's marker write.
        sim.advance(Duration::from_secs(60));
    }
    let rb = sim.compact(b);
    sim.resume(ga);
    let ra = sim.join(sa);
    let report = sim.check_invariants(CHECKED);
    controls(&report)?;
    let b_deleted = deleted_in_run(&sim, b, rb.index);
    let marker = marker_key_of(&sim, &ra);
    let marker_reaped = marker.as_ref().is_some_and(|m| b_deleted.contains(m));
    let packs_deleted = a_packs.iter().all(|k| b_deleted.contains(k));
    let evidence = format!(
        "{}\na's marker {marker:?} deleted by b: {marker_reaped}\n\
         a's packs {a_packs:?}, all deleted by b: {packs_deleted}\n{report}",
        outcomes(&ra, &rb)
    );
    Ok(
        if marker_reaped && packs_deleted && allowed(&ra, AllowedError::StaleChunksDuringCommit) {
            reproduced(&sim, evidence)
        } else if !marker_reaped
            && rb.is_ok()
            && ra.is_ok()
            && !a_packs.iter().any(|k| b_deleted.contains(k))
            && !report.violated_for(Invariant::CommittedSnapshotsRestore, "a1")
        {
            HazardOutcome::NotReproduced { evidence }
        } else {
            different_damage(evidence)
        },
    )
}

/// When maintenance reaps a's registration marker in H2-marker.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MarkerReap {
    /// Compact reaps the 46-min-old marker while the heartbeat is held
    /// before its refresh PUT; compact runs (no live marker) and keeps a's
    /// uploads by age; the refresh then writes the next key and finds the
    /// old one gone. A reaped session marker is never written again; the
    /// backup commits while its uploads are young (I8) and its snapshot
    /// restores.
    BeforeRefresh,
    /// The refresh PUT of the next key has landed, its presence check of the
    /// old key has not: compact judges a's session by its newest key, which
    /// is fresh, reaps nothing and runs beside it (04 step 7); the check
    /// then finds the old key present and a commits.
    AfterPut,
}

/// H2-marker with the reap placed by `reap`. Before 04 step 2 a refresh
/// read the marker and wrote it back, so a reap between the two was undone
/// and the backup was never told.
///
/// A backs up a1 and is held at journal recovery's LIST of `sessions/`, its
/// first main-thread operation after the heartbeat started. The heartbeat
/// is stepped to its first refresh (30 polls of 30 s); 31 min later its
/// registration marker is 46 min old and B compacts.
pub fn h2_marker_with(reap: MarkerReap) -> HazardOutcome {
    settle(h2_marker_scenario(reap))
}

/// H2-marker as a regression: both placements `NotReproduced`; any other
/// outcome is a `DisallowedError` with subject `"H2-marker at <reap>"`.
pub fn h2_marker_regression() -> Report {
    let mut out = Report::default();
    for reap in [MarkerReap::BeforeRefresh, MarkerReap::AfterPut] {
        let outcome = h2_marker_with(reap);
        if !matches!(outcome, HazardOutcome::NotReproduced { .. }) {
            out.add(
                ViolationKind::DisallowedError,
                format!("H2-marker at {reap:?}"),
                format!("{outcome:?}"),
            );
        }
    }
    out
}

fn h2_marker_scenario(reap: MarkerReap) -> Scenario {
    const WAIT: Duration = Duration::from_secs(5);
    let mut sim = Sim::new();
    let c = sim.client("c");
    ok(&sim.backup(c, "t0", &seeded("t0")), "control backup t0")?;
    let (a, b) = (sim.client("a"), sim.client("b"));
    let sessions = || KeyMatch::Prefix("sessions/".into());
    // The second listing of `sessions/`: the first is the registration's
    // (10-single-protocol step 8), before the heartbeat runs.
    let g_main =
        sim.gate(GateRule::new(a, OpMatch::AnyList, KeyMatch::Exact("sessions/".into())).nth(2));
    let sa = sim.spawn(
        a,
        Call::Backup {
            snapshot: "a1".into(),
            tree: seeded("a1"),
        },
    );
    reach(&sim, g_main, "a's journal-recovery LIST")?;
    // A's main thread is held: only the heartbeat touches sessions/ now.
    let g_put = sim.gate(GateRule::new(a, OpMatch::AnyWrite, sessions()));
    let g_exists = sim.gate(GateRule::new(a, OpMatch::Kind(OpKind::Exists), sessions()));
    let heartbeat = || broken("heartbeat did not reach its refresh");
    if !sim.time().wait_for_waiters(1, WAIT) {
        return Err(heartbeat());
    }
    for i in 0..30 {
        sim.advance(Duration::from_secs(30));
        if i < 29 && !sim.time().wait_for_waiters(1, WAIT) {
            return Err(heartbeat());
        }
    }
    reach(&sim, g_put, "the heartbeat's refresh PUT")?;
    let stale = Duration::from_secs(31 * 60);
    let rb = match reap {
        MarkerReap::BeforeRefresh => {
            sim.advance(stale);
            let rb = sim.compact(b);
            sim.resume(g_put);
            // Released or disarmed: the check runs ungated.
            sim.resume(g_exists);
            rb
        }
        MarkerReap::AfterPut => {
            sim.resume(g_put);
            reach(&sim, g_exists, "the refresh's presence check")?;
            sim.advance(stale);
            let rb = sim.compact(b);
            sim.resume(g_exists);
            rb
        }
    };
    // The refresh must end before a's main thread goes on to commit: wait
    // until the heartbeat is back in its timed wait.
    if !sim.time().wait_for_waiters(1, WAIT) {
        return Err(heartbeat());
    }
    sim.resume(g_main);
    let ra = sim.join(sa);
    let report = sim.check_invariants(CHECKED);
    controls(&report)?;
    let Some(marker) = marker_key_of(&sim, &ra) else {
        return Err(broken(format!("a wrote no marker\n{}", outcomes(&ra, &rb))));
    };
    let reaped = steps_deleting(&sim, b, rb.index, &marker);
    let rewrites: Vec<(u64, String)> = reaped
        .first()
        .map(|s1| {
            records_of(&sim, a, ra.index, |r| {
                r.op.is_write() && r.key == marker && r.step > *s1
            })
            .into_iter()
            .map(|r| (r.step, r.thread))
            .collect()
        })
        .unwrap_or_default();
    let left: Vec<String> = sim
        .store()
        .keys_written_by(a, "sessions/")
        .into_iter()
        .filter(|k| k.ends_with(".json") && sim.store().object(k).is_some())
        .collect();
    let evidence = format!(
        "{}\na's marker {marker} deleted by b at {reaped:?}, written by a after: \
         {rewrites:?}; a's markers left: {left:?}\n{report}",
        outcomes(&ra, &rb)
    );
    let restores = !report.violated_for(Invariant::CommittedSnapshotsRestore, "a1");
    let held = match reap {
        MarkerReap::BeforeRefresh => !reaped.is_empty() && rb.is_ok() && ra.is_ok() && restores,
        MarkerReap::AfterPut => reaped.is_empty() && rb.is_ok() && ra.is_ok() && restores,
    };
    Ok(if !reaped.is_empty() && !rewrites.is_empty() {
        reproduced(&sim, evidence)
    } else if held && rewrites.is_empty() && left.is_empty() {
        HazardOutcome::NotReproduced { evidence }
    } else if reap == MarkerReap::BeforeRefresh && reaped.is_empty() {
        broken(format!(
            "B did not reap A's marker; check the advance against the staleness \
             constants\n{evidence}"
        ))
    } else {
        different_damage(evidence)
    })
}

// ── H4 ──────────────────────────────────────────────────────────────────

/// How compact A and backup B are ordered in H4.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Order {
    /// B runs while A is held at its head PUT, and is held at its own gate.
    Interleaved,
    /// A runs to the end before B starts.
    Sequential,
}

/// Where backup B is held in H4-segment.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum BackupGate {
    /// After its segment PUT and fence, before its head PUT (H4-segment).
    Head,
}

/// H4-segment as a regression: `HeadObjectAbsent` for B's segment if it is
/// absent, else a clean report. The H4 damage of the same run is not this
/// regression's subject.
pub fn h4_segment_regression() -> Report {
    let mut out = Report::default();
    let (sim, b) = match h4_run(Hosts::StoreStepped, Order::Interleaved, BackupGate::Head) {
        Ok(Ok(run)) => run,
        Ok(Err(outcome)) | Err(outcome) => {
            out.add(
                ViolationKind::DisallowedError,
                "scenario",
                format!("{outcome:?}"),
            );
            return out;
        }
    };
    let segments = sim.store().keys_written_by(b, "index.d/");
    if segments.is_empty() {
        out.add(ViolationKind::HeadObjectAbsent, "b", "b wrote no segment");
    }
    for k in segments {
        if sim.store().object(&k).is_none() {
            out.add(ViolationKind::HeadObjectAbsent, &k, "b's segment deleted");
        }
    }
    out
}

/// The runs of H4 and H4-segment: A and B in flight as `order` says.
/// `Ok(Err(o))`: an early outcome that is not a broken precondition.
#[allow(clippy::type_complexity)]
fn h4_runs(
    hosts: Hosts,
    order: Order,
    at: BackupGate,
) -> Result<Result<(Sim, ClientId, ClientId, RunRecord, RunRecord), HazardOutcome>, HazardOutcome> {
    let mut sim = Sim::new();
    let c = sim.client("c");
    let x = seeded("x");
    ok(&sim.backup(c, "t0", &x), "setup backup t0")?;
    // Same bytes as x's first file: its chunk dedups into t0's pack.
    let t1 = SourceTree::new().file("f000.bin", seeded_bytes("x/0", FILE_SIZE));
    ok(&sim.backup(c, "t1", &t1), "setup backup t1")?;
    ok(&sim.delete(c, &["t0"]), "setup delete t0")?;
    // t0's segment is old, so A's sweep takes t0's entries and its repack
    // has t0's pack to merge (04 step 7 keeps young segments' additions).
    sim.advance(crate::repo::ages::T_PACK + std::time::Duration::from_secs(60));
    let (a, b) = pair(&mut sim, hosts);
    let head = || KeyMatch::Prefix("index.h/".into());
    // A's first head is its sweep's; the second follows the repack batch.
    let ga = sim.gate(GateRule::new(a, OpMatch::AnyWrite, head()).nth(2));
    let gb = sim.gate(GateRule::new(
        b,
        OpMatch::AnyWrite,
        match at {
            BackupGate::Head => head(),
        },
    ));
    let sa = sim.spawn(a, Call::Compact);
    reach(&sim, ga, "compact a's second head PUT (nth 2)")?;
    step_before_reaper(&mut sim, hosts, chrono::Duration::seconds(6 * 3600 + 60));
    if sim.store().keys_written_by(a, "packs/").is_empty() {
        return Err(broken(
            "compact repacked nothing before its second head PUT (nth 2); adjust the \
             setup, the threshold or the gate's nth",
        ));
    }
    let backup_b1 = || Call::Backup {
        snapshot: "b1".into(),
        tree: seeded("b1"),
    };
    let (ra, rb) = match order {
        Order::Interleaved => {
            let sb = sim.spawn(b, backup_b1());
            if sim.poll_paused(gb, GATE_TIMEOUT).map_err(broken)?.is_none() {
                let rb = sim.join(sb);
                sim.resume(ga);
                let ra = sim.join(sa);
                if !allowed(&rb, AllowedError::Locked) {
                    return Err(broken(format!(
                        "b ended before its gate: {}",
                        outcomes(&ra, &rb)
                    )));
                }
                if !ra.is_ok() {
                    return Ok(Err(different_damage(format!(
                        "b was locked out but a failed\n{}",
                        outcomes(&ra, &rb)
                    ))));
                }
                return Ok(Err(HazardOutcome::PrerequisiteNotReproduced {
                    prerequisite: "a reaped live lock",
                    evidence: format!("b could not reap a's lock\n{}", outcomes(&ra, &rb)),
                }));
            }
            sim.resume(ga);
            let ra = sim.join(sa);
            sim.resume(gb);
            (ra, sim.join(sb))
        }
        Order::Sequential => {
            sim.resume(ga);
            let ra = sim.join(sa);
            sim.resume(gb);
            (ra, sim.call(b, backup_b1()))
        }
    };
    Ok(Ok((sim, a, b, ra, rb)))
}

/// [`h4_runs`], keeping only the store and B.
fn h4_run(
    hosts: Hosts,
    order: Order,
    at: BackupGate,
) -> Result<Result<(Sim, ClientId), HazardOutcome>, HazardOutcome> {
    Ok(h4_runs(hosts, order, at)?.map(|(sim, _, b, _, _)| (sim, b)))
}
