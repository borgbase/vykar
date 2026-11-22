//! Hazards that need two clients in flight at once, scripted with gates
//! (08 step 3).
//!
//! Each scenario spawns one client, holds it at a gate, runs or spawns the
//! other, and resumes the first. Only one client is runnable at a time, so
//! virtual time stays in Auto mode. Gates select an operation by (client,
//! nth matching operation), never by a key the run invents; the keys a
//! verdict needs (lock keys, markers) are read back from the op log.

use std::time::Duration;

use super::checker::{Invariant, Report, ViolationKind};
use super::fault::{ClientId, KeyMatch, OpKind, OpMatch};
use super::gate::{GateId, GateRule, Paused};
use super::hazards::{HazardOutcome, base_controls, broken, deleted_in_run, different_damage};
use super::history::{AllowedError, ErrorClass, RunRecord};
use super::log::{OpOutcome, OpRecord};
use super::runner::{Call, GATE_TIMEOUT, Sim, SourceTree, seeded_bytes};

/// Where the two clients of a scenario live.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Hosts {
    /// Hostname "ubuntu" on both, boot IDs "boot-a" and "boot-b": two
    /// machines with one name (cloned VMs). The H1 condition.
    SameHostname,
    /// Hostnames "host-a" and "host-b".
    Distinct,
}

/// The checked set for these scenarios: STEP1 and invariant 7.
pub const CHECKED: &[Invariant] = &[
    Invariant::CommittedSnapshotsRestore,
    Invariant::AcknowledgedCommitsDurable,
    Invariant::NoSilentRemoval,
    Invariant::IndexIntegrity,
    Invariant::NoRefcountDeflation,
    Invariant::AllowedErrors,
    Invariant::LockExclusion,
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
        Hosts::Distinct => (
            sim.client_on("a", "host-a", Some("boot-a")),
            sim.client_on("b", "host-b", Some("boot-b")),
        ),
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

/// The lock key `run` wrote, read back from the log.
fn lock_key_of(sim: &Sim, run: &RunRecord) -> Option<String> {
    first_write_of(sim, run, |k| {
        k.starts_with("locks/") && k.ends_with(".json")
    })
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

/// Steps at which `client`'s run `run` wrote `key`.
fn steps_writing(sim: &Sim, client: ClientId, run: usize, key: &str) -> Vec<u64> {
    records_of(sim, client, run, |r| r.op.is_write() && r.key == key)
        .iter()
        .map(|r| r.step)
        .collect()
}

fn lock_violation(report: &Report, run: usize, kind: ViolationKind) -> bool {
    report.has_kind(kind, &format!("run {run}"))
}

fn outcomes(ra: &RunRecord, rb: &RunRecord) -> String {
    format!("a: {:?}\nb: {:?}", ra.outcome, rb.outcome)
}

fn controls(report: &Report) -> Result<(), HazardOutcome> {
    base_controls(report).map_err(broken)
}

// ── H1 ──────────────────────────────────────────────────────────────────

/// H1: a live maintenance lock is reaped by a machine with the same
/// hostname and another boot ID, and its holder writes under the reaper's
/// lock.
pub fn h1_live_lock_reaped() -> HazardOutcome {
    h1_with(Hosts::SameHostname)
}

/// H1 with the clients placed by `hosts`; `Distinct` neutralises it.
///
/// A deletes t0 and is held at its snapshot delete, after its fence check.
/// B deletes t1: its stale-lock sweep reaps A's lock when it shares A's
/// hostname, and B is held at its own first fence check. Then A resumes.
pub fn h1_with(hosts: Hosts) -> HazardOutcome {
    settle(h1_scenario(hosts))
}

fn h1_scenario(hosts: Hosts) -> Scenario {
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
    let gb = sim.gate(GateRule::new(
        b,
        OpMatch::Kind(OpKind::Exists),
        KeyMatch::Prefix("locks/".into()),
    ));
    let sa = sim.spawn(
        a,
        Call::Delete {
            names: vec!["t0".into()],
        },
    );
    reach(&sim, ga, "a's snapshot delete")?;
    let sb = sim.spawn(
        b,
        Call::Delete {
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
            if !allowed(&rb, AllowedError::Locked) {
                return Err(broken(format!(
                    "b ended before its fence check, not Locked: {:?}",
                    rb.outcome
                )));
            }
            sim.resume(ga);
            (sim.join(sa), rb)
        }
    };
    let report = sim.check_invariants(CHECKED);
    controls(&report)?;
    let key = lock_key_of(&sim, &ra);
    let reaped: Vec<u64> = key
        .as_deref()
        .map(|k| steps_deleting(&sim, b, rb.index, k))
        .unwrap_or_default();
    let inside = reaped
        .iter()
        .any(|s| (ra.first_step..ra.last_step).contains(s));
    let evidence = format!(
        "{}\na's lock {key:?} deleted by b at {reaped:?}\n{report}",
        outcomes(&ra, &rb)
    );
    Ok(
        if inside && lock_violation(&report, ra.index, ViolationKind::WriteUnderOtherLock) {
            HazardOutcome::Reproduced { evidence }
        } else if allowed(&rb, AllowedError::Locked)
            && ra.is_ok()
            && !report.violated(Invariant::LockExclusion)
        {
            HazardOutcome::NotReproduced { evidence }
        } else {
            different_damage(evidence)
        },
    )
}

/// H1-session: the same-hostname rule reaps a live backup's session marker;
/// compact deletes the backup's uploaded packs, and the backup fails.
pub fn h1_session_reaped() -> HazardOutcome {
    h1_session_with(Hosts::SameHostname)
}

/// H1-session with the clients placed by `hosts`; `Distinct` neutralises
/// it.
///
/// A backs up a1 and is held at its commit lock's stale-lock sweep, after
/// phase 1 uploaded its packs. B compacts. Then A resumes.
pub fn h1_session_with(hosts: Hosts) -> HazardOutcome {
    settle(h1_session_scenario(hosts))
}

fn h1_session_scenario(hosts: Hosts) -> Scenario {
    let mut sim = Sim::new();
    let c = sim.client("c");
    ok(&sim.backup(c, "t0", &seeded("t0")), "control backup t0")?;
    let (a, b) = pair(&mut sim, hosts);
    // The 1st LIST of locks/ is the registration probe, the 2nd the commit
    // lock's sweep, after every pack is uploaded.
    let ga = sim.gate(
        GateRule::new(
            a,
            OpMatch::Kind(OpKind::List),
            KeyMatch::Exact("locks/".into()),
        )
        .nth(2),
    );
    let sa = sim.spawn(
        a,
        Call::Backup {
            snapshot: "a1".into(),
            tree: seeded("a1"),
        },
    );
    reach(&sim, ga, "a's commit-lock sweep")?;
    let a_packs = sim.store().keys_written_by(a, "packs/");
    if a_packs.is_empty() {
        return Err(broken("a paused before uploading; retarget the gate"));
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
            HazardOutcome::Reproduced { evidence }
        } else if allowed(&rb, AllowedError::ActiveSessions)
            && ra.is_ok()
            && !report.violated_for(Invariant::CommittedSnapshotsRestore, "a1")
        {
            HazardOutcome::NotReproduced { evidence }
        } else {
            different_damage(evidence)
        },
    )
}

// ── H2 ──────────────────────────────────────────────────────────────────

/// When B reaps A's lock in H2.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum H2Reap {
    /// B reaps between A's fence `exists` and its refresh PUT: the hazard.
    DuringRefresh,
    /// B reaps and finishes while A waits before its fence `exists`:
    /// the fence must then fail (what a fixed refresh looks like).
    BeforeFence,
}

/// H2: a lock refresh recreates a reaped key; both holders finish and both
/// fences pass.
pub fn h2_lock_refresh_resurrects() -> HazardOutcome {
    h2_with(H2Reap::DuringRefresh)
}

/// H2 with the reap placed by `reap`; `BeforeFence` neutralises it.
///
/// A deletes t0 and is held at its first fence check; time moves 3 h 1 min,
/// so A's lock is due for refresh. B, its clock 3 h ahead, sees A's lock as
/// older than 6 h and reaps it.
pub fn h2_with(reap: H2Reap) -> HazardOutcome {
    settle(h2_scenario(reap))
}

fn h2_scenario(reap: H2Reap) -> Scenario {
    let mut sim = Sim::new();
    let c = sim.client("c");
    ok(&sim.backup(c, "t0", &seeded("t0")), "control backup t0")?;
    ok(&sim.backup(c, "t1", &seeded("t1")), "control backup t1")?;
    let (a, b) = (sim.client("a"), sim.client("b"));
    sim.set_skew(b, chrono::Duration::hours(3));
    let locks = || KeyMatch::Prefix("locks/".into());
    let g_fence = sim.gate(GateRule::new(a, OpMatch::Kind(OpKind::Exists), locks()));
    // The 1st write under locks/ is the acquisition, the 2nd the refresh.
    let g_refresh = sim.gate(GateRule::new(a, OpMatch::AnyWrite, locks()).nth(2));
    let gb = sim.gate(GateRule::new(b, OpMatch::Kind(OpKind::Exists), locks()));
    let sa = sim.spawn(
        a,
        Call::Delete {
            names: vec!["t0".into()],
        },
    );
    reach(&sim, g_fence, "a's first fence check")?;
    sim.advance(Duration::from_secs(3 * 3600 + 60));
    let delete_t1 = || Call::Delete {
        names: vec!["t1".into()],
    };
    let (ra, rb) = match reap {
        H2Reap::DuringRefresh => {
            sim.resume(g_fence);
            reach(&sim, g_refresh, "a's lock refresh")?;
            let sb = sim.spawn(b, delete_t1());
            if sim.poll_paused(gb, GATE_TIMEOUT).map_err(broken)?.is_none() {
                let rb = sim.join(sb);
                return Err(broken(format!(
                    "B did not reap A's lock; check the skew and advance against \
                     the staleness constants: {:?}",
                    rb.outcome
                )));
            }
            sim.resume(g_refresh);
            let ra = sim.join(sa);
            sim.resume(gb);
            (ra, sim.join(sb))
        }
        H2Reap::BeforeFence => {
            sim.resume(gb);
            let sb = sim.spawn(b, delete_t1());
            let rb = sim.join(sb);
            sim.resume(g_fence);
            // Disarms it if the refresh is never reached.
            sim.resume(g_refresh);
            (sim.join(sa), rb)
        }
    };
    let report = sim.check_invariants(CHECKED);
    controls(&report)?;
    let Some(key) = lock_key_of(&sim, &ra) else {
        return Err(broken(format!(
            "a wrote no lock key\n{}",
            outcomes(&ra, &rb)
        )));
    };
    let reaped = steps_deleting(&sim, b, rb.index, &key);
    let rewritten: Vec<u64> = reaped
        .first()
        .map(|s1| {
            steps_writing(&sim, a, ra.index, &key)
                .into_iter()
                .filter(|s| s > s1)
                .collect()
        })
        .unwrap_or_default();
    let fence_passed_after = rewritten.first().is_some_and(|s2| {
        !records_of(&sim, a, ra.index, |r| {
            r.op == OpKind::Exists && r.key == key && r.outcome == OpOutcome::Ok && r.step > *s2
        })
        .is_empty()
    });
    let evidence = format!(
        "{}\na's lock {key} deleted by b at {reaped:?}, rewritten by a at {rewritten:?}, \
         a later fence passed: {fence_passed_after}\n{report}",
        outcomes(&ra, &rb)
    );
    Ok(
        if !reaped.is_empty()
            && fence_passed_after
            && ra.is_ok()
            && rb.is_ok()
            && lock_violation(&report, ra.index, ViolationKind::WriteUnderSeveralLocks)
        {
            HazardOutcome::Reproduced { evidence }
        } else if !reaped.is_empty()
            && rewritten.is_empty()
            && allowed(&ra, AllowedError::LockExpired)
        {
            HazardOutcome::NotReproduced { evidence }
        } else if reap == H2Reap::DuringRefresh && !rb.is_ok() {
            broken(format!(
                "B did not reap A's lock; check the skew and advance against the \
                 staleness constants\n{evidence}"
            ))
        } else {
            different_damage(evidence)
        },
    )
}

/// When maintenance reaps the marker in H2-marker.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MarkerReap {
    /// Maintenance reaps between the heartbeat's GET and PUT: the hazard.
    BetweenGetAndPut,
    /// Maintenance reaps before the heartbeat's GET, which then finds the
    /// marker missing and does not recreate it.
    BeforeGet,
}

/// H2-marker: a session-marker refresh recreates a reaped marker, and the
/// backup is never told.
pub fn h2_marker_refresh_resurrects() -> HazardOutcome {
    h2_marker_with(MarkerReap::BetweenGetAndPut)
}

/// H2-marker with the reap placed by `reap`; `BeforeGet` neutralises it.
///
/// A backs up a1 and is held at journal recovery's LIST of `sessions/`, its
/// first main-thread operation after the heartbeat started. The heartbeat
/// is stepped to its first refresh (30 polls of 30 s); 31 min later its
/// marker is 46 min old and B's compact reaps it.
pub fn h2_marker_with(reap: MarkerReap) -> HazardOutcome {
    settle(h2_marker_scenario(reap))
}

fn h2_marker_scenario(reap: MarkerReap) -> Scenario {
    const WAIT: Duration = Duration::from_secs(5);
    let mut sim = Sim::new();
    let c = sim.client("c");
    ok(&sim.backup(c, "t0", &seeded("t0")), "control backup t0")?;
    let (a, b) = (sim.client("a"), sim.client("b"));
    let sessions = || KeyMatch::Prefix("sessions/".into());
    let g_main = sim.gate(GateRule::new(
        a,
        OpMatch::Kind(OpKind::List),
        KeyMatch::Exact("sessions/".into()),
    ));
    let sa = sim.spawn(
        a,
        Call::Backup {
            snapshot: "a1".into(),
            tree: seeded("a1"),
        },
    );
    reach(&sim, g_main, "a's journal-recovery LIST")?;
    // A's main thread is held: only the heartbeat touches sessions/ now.
    let g_get = sim.gate(GateRule::new(a, OpMatch::Kind(OpKind::Get), sessions()));
    let g_put = sim.gate(GateRule::new(a, OpMatch::AnyWrite, sessions()));
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
    reach(&sim, g_get, "heartbeat did not reach its refresh")?;
    let stale = Duration::from_secs(31 * 60);
    let rb = match reap {
        MarkerReap::BetweenGetAndPut => {
            sim.resume(g_get);
            reach(&sim, g_put, "the heartbeat's marker PUT")?;
            sim.advance(stale);
            let rb = sim.compact(b);
            sim.resume(g_put);
            // The marker write must land before a's main thread goes on to
            // probe it: wait until the heartbeat is back in its timed wait.
            if !sim.time().wait_for_waiters(1, WAIT) {
                return Err(heartbeat());
            }
            rb
        }
        MarkerReap::BeforeGet => {
            sim.advance(stale);
            let rb = sim.compact(b);
            sim.resume(g_get);
            // Disarm, so the journal PUT under sessions/ is not caught.
            sim.resume(g_put);
            rb
        }
    };
    sim.resume(g_main);
    let ra = sim.join(sa);
    let report = sim.check_invariants(CHECKED);
    controls(&report)?;
    ok(&rb, "compact b")?;
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
    let evidence = format!(
        "{}\na's marker {marker} deleted by b at {reaped:?}, written by a after: \
         {rewrites:?}\n{report}",
        outcomes(&ra, &rb)
    );
    Ok(
        if !reaped.is_empty() && rewrites.iter().any(|(_, t)| t == "session-refresh") && ra.is_ok()
        {
            HazardOutcome::Reproduced { evidence }
        } else if !reaped.is_empty()
            && rewrites.is_empty()
            && allowed(&ra, AllowedError::StaleChunksDuringCommit)
        {
            HazardOutcome::NotReproduced { evidence }
        } else {
            different_damage(evidence)
        },
    )
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

/// Where backup B is held in H4.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum BackupGate {
    /// After its head GET, before its segment PUT (H4).
    Segment,
    /// After its segment PUT and fence, before its head PUT (H4-segment).
    Head,
}

/// H4: compact loses its lock (H1's rule) and deletes sources with no fence
/// after its head PUT; a backup that read the head before then commits a
/// head that names the deleted packs.
pub fn h4_compact_deletes_after_lost_lock() -> HazardOutcome {
    h4_with(Hosts::SameHostname, Order::Interleaved)
}

/// H4 with the clients placed by `hosts` and ordered by `order`.
/// `Sequential` neutralises it; `Distinct` removes its prerequisite (H1).
///
/// Compact A is held at its head PUT, after its segment PUT. Backup B, whose
/// registration probe reaps A's lock, is held after its head GET and before
/// its segment PUT. A resumes, deletes t0's old pack and ends `Ok`; then B
/// commits on the head it read.
pub fn h4_with(hosts: Hosts, order: Order) -> HazardOutcome {
    settle(h4_scenario(hosts, order, BackupGate::Segment))
}

/// H4-segment: a compact whose lock was reaped deletes a concurrent
/// writer's segment as unreferenced.
pub fn h4_segment_deleted() -> HazardOutcome {
    h4_segment_with(Hosts::SameHostname, Order::Interleaved)
}

/// H4-segment: as H4, but B is held at its head PUT, after its segment PUT.
/// A's end-of-run index upkeep (`collect_garbage`, unfenced) deletes B's
/// segment, and B's head then names it.
pub fn h4_segment_with(hosts: Hosts, order: Order) -> HazardOutcome {
    settle(h4_scenario(hosts, order, BackupGate::Head))
}

fn h4_scenario(hosts: Hosts, order: Order, at: BackupGate) -> Scenario {
    let mut sim = Sim::new();
    let c = sim.client("c");
    let x = seeded("x");
    ok(&sim.backup(c, "t0", &x), "setup backup t0")?;
    // Same bytes as x's first file: its chunk dedups into t0's pack.
    let t1 = SourceTree::new().file("f000.bin", seeded_bytes("x/0", FILE_SIZE));
    ok(&sim.backup(c, "t1", &t1), "setup backup t1")?;
    ok(&sim.delete(c, &["t0"]), "setup delete t0")?;
    let (a, b) = pair(&mut sim, hosts);
    let head = || KeyMatch::Exact("index".into());
    let ga = sim.gate(GateRule::new(a, OpMatch::AnyWrite, head()));
    let gb = sim.gate(GateRule::new(
        b,
        OpMatch::AnyWrite,
        match at {
            BackupGate::Segment => KeyMatch::Prefix("index.d/".into()),
            BackupGate::Head => head(),
        },
    ));
    let sa = sim.spawn(a, Call::Compact);
    reach(&sim, ga, "compact a's head PUT")?;
    if sim.store().keys_written_by(a, "packs/").is_empty() {
        return Err(broken(
            "compact repacked nothing; adjust the setup or the threshold",
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
                return Ok(HazardOutcome::PrerequisiteNotReproduced {
                    prerequisite: "H1",
                    evidence: format!("b could not reap a's lock\n{}", outcomes(&ra, &rb)),
                });
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
    let report = sim.check_invariants(CHECKED);
    controls(&report)?;
    let key = lock_key_of(&sim, &ra);
    let reap = key
        .as_deref()
        .map(|k| steps_deleting(&sim, b, rb.index, k))
        .unwrap_or_default()
        .into_iter()
        .find(|s| (ra.first_step..ra.last_step).contains(s));
    let a_deleted = deleted_in_run(&sim, a, ra.index);
    let evidence_head = format!(
        "{}\na's lock {key:?} deleted by b at {reap:?}",
        outcomes(&ra, &rb)
    );
    let healthy = rb.is_ok()
        && !report.violated(Invariant::IndexIntegrity)
        && !report.violated_for(Invariant::CommittedSnapshotsRestore, "t1");
    let reproduced = match at {
        BackupGate::Segment => {
            let after_reap = reap.is_some_and(|s1| {
                !records_of(&sim, a, ra.index, |r| {
                    r.step > s1 && r.deleted_keys().iter().any(|k| k.starts_with("packs/"))
                })
                .is_empty()
            });
            let named: Vec<&String> = a_deleted
                .iter()
                .filter(|k| {
                    k.starts_with("packs/")
                        && report.has_kind(ViolationKind::IndexNamesAbsentPack, k)
                })
                .collect();
            let evidence = format!(
                "{evidence_head}\na deleted packs after the reap: {after_reap}; \
                 the index names packs a deleted: {named:?}\n{report}"
            );
            let hit = reap.is_some()
                && after_reap
                && !named.is_empty()
                && report.violated_for(Invariant::CommittedSnapshotsRestore, "t1")
                && rb.is_ok();
            (hit, evidence)
        }
        BackupGate::Head => {
            let b_segments = sim.store().keys_written_by(b, "index.d/");
            let dangling: Vec<&String> = b_segments
                .iter()
                .filter(|k| {
                    a_deleted.contains(*k) && report.has_kind(ViolationKind::HeadObjectAbsent, k)
                })
                .collect();
            let evidence = format!(
                "{evidence_head}\nb's segments {b_segments:?}, deleted by a and named by \
                 the head: {dangling:?}\n{report}"
            );
            (
                reap.is_some() && !dangling.is_empty() && rb.is_ok(),
                evidence,
            )
        }
    };
    let (hit, evidence) = reproduced;
    Ok(if hit {
        HazardOutcome::Reproduced { evidence }
    } else if healthy {
        HazardOutcome::NotReproduced { evidence }
    } else {
        different_damage(evidence)
    })
}
