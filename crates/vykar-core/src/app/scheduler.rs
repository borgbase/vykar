use std::fs::File;
use std::time::{Duration, SystemTime};

use rand::RngExt;

use crate::app::service::RepoSelection;
use crate::config::ScheduleConfig;
use vykar_types::error::{Result, VykarError};

/// Process-wide scheduler lock: an OS file lock (`flock(2)` on Unix,
/// `LockFileEx` on Windows) on `<config dir>/vykar/scheduler.lock`.
///
/// The OS releases the lock automatically when the process exits (even on crash).
/// Only one scheduler (daemon or GUI) can hold this lock at a time.
pub struct SchedulerLock {
    _file: Option<File>, // None when lock dir was inaccessible (fail-open)
}

impl SchedulerLock {
    /// Try to acquire the process-wide scheduler lock.
    ///
    /// Returns `Some(lock)` if acquired (or if locking is unavailable — fail-open).
    /// Returns `None` if another scheduler already holds the lock.
    pub fn try_acquire() -> Option<Self> {
        Self::try_acquire_at(None)
    }

    /// Like [`SchedulerLock::try_acquire`] but allows overriding the lock file path (for tests).
    pub fn try_acquire_at(path_override: Option<&std::path::Path>) -> Option<Self> {
        let path = match path_override {
            Some(p) => p.to_path_buf(),
            None => {
                let Some(config) = vykar_common::paths::config_dir() else {
                    tracing::warn!("could not determine config dir; scheduler lock skipped");
                    return Some(Self { _file: None });
                };
                config.join("vykar").join("scheduler.lock")
            }
        };

        if let Some(parent) = path.parent()
            && let Err(e) = std::fs::create_dir_all(parent)
        {
            tracing::warn!(%e, "could not create scheduler lock dir; proceeding without lock");
            return Some(Self { _file: None });
        }

        let file = match File::options()
            .write(true)
            .create(true)
            .truncate(false)
            .open(&path)
        {
            Ok(f) => f,
            Err(e) => {
                tracing::warn!(%e, "could not open scheduler lock file; proceeding without lock");
                return Some(Self { _file: None });
            }
        };

        match file.try_lock() {
            Ok(()) => Some(Self { _file: Some(file) }),
            Err(std::fs::TryLockError::WouldBlock) => None,
            Err(std::fs::TryLockError::Error(e)) => {
                tracing::warn!(%e, "scheduler lock failed unexpectedly; proceeding without lock");
                Some(Self { _file: Some(file) })
            }
        }
    }
}

pub fn random_jitter(jitter_seconds: u64) -> Duration {
    if jitter_seconds == 0 {
        return Duration::ZERO;
    }
    let secs = rand::rng().random_range(0..=jitter_seconds);
    Duration::from_secs(secs)
}

/// Compute the delay from `now` until the next cron tick (local time), plus jitter.
fn next_cron_delay(schedule: &ScheduleConfig, now: SystemTime) -> Result<Duration> {
    let expr = schedule.cron.as_deref().unwrap_or("");
    let cron = crate::config::parse_cron(expr)
        .map_err(|e| VykarError::Config(format!("schedule.cron: invalid expression: {e}")))?;

    let delay = cron_delay_at(&cron, &chrono::DateTime::<chrono::Local>::from(now))?;
    Ok(delay + random_jitter(schedule.jitter_seconds))
}

/// Delay from `now` until the next cron tick strictly after it.
fn cron_delay_at<Tz: chrono::TimeZone>(
    cron: &croner::Cron,
    now: &chrono::DateTime<Tz>,
) -> Result<Duration> {
    let next = cron
        .find_next_occurrence(now, false)
        .map_err(|e| VykarError::Config(format!("schedule.cron: no next occurrence: {e}")))?;
    Ok((next - now.clone())
        .to_std()
        .unwrap_or(Duration::from_secs(60)))
}

/// Unified entry point: returns the delay until the next scheduled run.
/// Uses cron when `schedule.cron` is set, otherwise falls back to `every` interval.
pub fn next_run_delay(schedule: &ScheduleConfig) -> Result<Duration> {
    next_run_delay_at(schedule, SystemTime::now())
}

/// [`next_run_delay`] measured from `now` (cron is evaluated in local time).
pub fn next_run_delay_at(schedule: &ScheduleConfig, now: SystemTime) -> Result<Duration> {
    if schedule.is_cron() {
        next_cron_delay(schedule, now)
    } else {
        Ok(schedule.every_duration()? + random_jitter(schedule.jitter_seconds))
    }
}

/// Terse cadence summary for a metric card: the interval string, the cron
/// expression, or `"Off"` when the schedule is disabled or globally paused.
#[must_use]
pub fn schedule_brief(schedule: &ScheduleConfig, paused: bool) -> String {
    if !schedule.enabled || paused {
        return "Off".to_string();
    }
    if let Some(ref cron) = schedule.cron {
        return cron.clone();
    }
    schedule.every.clone().unwrap_or_else(|| "24h".to_string())
}

/// Identity used to decide whether all repos share one cadence. Compares the
/// parsed interval rather than the raw string, so `60m` and `1h` are the same
/// schedule rather than a spurious "per-repo".
fn schedule_key(schedule: &ScheduleConfig) -> (bool, Option<String>, Option<u64>) {
    (
        schedule.enabled,
        schedule.cron.clone(),
        schedule.every_duration().ok().map(|d| d.as_secs()),
    )
}

/// Top-level schedule summary: the shared cadence when every repo agrees,
/// `"per-repo"` when they differ, `"Off"` when paused or repo-less. The
/// per-repo column carries the detail in the mixed case.
#[must_use]
pub fn repos_schedule_brief(schedules: &[&ScheduleConfig], paused: bool) -> String {
    if paused {
        return "Off".to_string();
    }
    let mut iter = schedules.iter();
    let Some(first) = iter.next() else {
        return "Off".to_string();
    };
    let first_key = schedule_key(first);
    if iter.any(|s| schedule_key(s) != first_key) {
        return "per-repo".to_string();
    }
    schedule_brief(first, paused)
}

/// Per-repository next-run times, index-aligned with a slice of repositories.
///
/// An entry is `None` when the repo's schedule is disabled *or* unschedulable —
/// a broken cadence on one repo must not stop the others from running, so
/// construction degrades instead of failing.
#[derive(Debug, Clone, Default)]
struct SchedulePlan {
    next_runs: Vec<Option<SystemTime>>,
}

impl SchedulePlan {
    /// Build a plan from one schedule per repository, in repository order,
    /// with every slot counted from `now`.
    ///
    /// Returns the plan plus the per-index errors of any entry whose next run
    /// could not be computed. Those entries are `None` and never fire on the
    /// timer.
    ///
    /// `honor_on_startup` is false on the reload path, where a repo's
    /// `on_startup` must not re-trigger an immediate backup.
    fn new(
        schedules: &[ScheduleConfig],
        now: SystemTime,
        honor_on_startup: bool,
    ) -> (Self, Vec<(usize, VykarError)>) {
        let mut next_runs = Vec::with_capacity(schedules.len());
        let mut errors = Vec::new();

        for (idx, schedule) in schedules.iter().enumerate() {
            if !schedule.enabled {
                next_runs.push(None);
                continue;
            }
            if honor_on_startup && schedule.on_startup {
                next_runs.push(Some(now));
                continue;
            }
            match next_run_delay_at(schedule, now) {
                Ok(delay) => next_runs.push(Some(now + delay)),
                Err(e) => {
                    next_runs.push(None);
                    errors.push((idx, e));
                }
            }
        }

        (Self { next_runs }, errors)
    }

    /// The earliest upcoming run across all repositories, or `None` when every
    /// entry is disabled/unschedulable.
    fn next_wake(&self) -> Option<SystemTime> {
        self.next_runs.iter().flatten().min().copied()
    }

    /// Indices of the repositories whose next run has arrived.
    fn due(&self, now: SystemTime) -> Vec<usize> {
        self.next_runs
            .iter()
            .enumerate()
            .filter_map(|(idx, next)| match next {
                Some(t) if *t <= now => Some(idx),
                _ => None,
            })
            .collect()
    }

    /// The next run for one repository, if it has one.
    fn next_run(&self, idx: usize) -> Option<SystemTime> {
        self.next_runs.get(idx).copied().flatten()
    }

    fn len(&self) -> usize {
        self.next_runs.len()
    }

    /// Move one entry to `now + interval` (drift, not fixed slots). On error the
    /// entry becomes `None` and the error is returned.
    ///
    /// `idx` must index this plan; an out-of-range one is a caller bug. It
    /// trips a `debug_assert!` and is a no-op in release; the `None` return
    /// then reads as "no error", not as "not rescheduled".
    fn reschedule(
        &mut self,
        idx: usize,
        schedule: &ScheduleConfig,
        now: SystemTime,
    ) -> Option<VykarError> {
        debug_assert!(
            idx < self.next_runs.len(),
            "reschedule index {idx} out of range for {} entries",
            self.next_runs.len()
        );
        let slot = self.next_runs.get_mut(idx)?;
        if !schedule.enabled {
            *slot = None;
            return None;
        }
        match next_run_delay_at(schedule, now) {
            Ok(delay) => {
                *slot = Some(now + delay);
                None
            }
            Err(e) => {
                *slot = None;
                Some(e)
            }
        }
    }
}

/// Identifies one piece of work the scheduler knows is running.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct CycleId(u64);

/// Why the scheduler hands out a cycle.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CycleTrigger {
    /// One or more per-repo slots arrived.
    Scheduled,
    /// [`Scheduler::trigger`] (SIGUSR1): every repository.
    Adhoc,
}

/// A cycle the frontend must run now, then report with [`Scheduler::finished`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CycleOrder {
    pub id: CycleId,
    /// `Indices` (ascending) for a scheduled cycle, `All` for an ad-hoc one.
    pub selection: RepoSelection,
    pub trigger: CycleTrigger,
}

/// Work a frontend starts on its own (not handed out by [`Scheduler::poll`]).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Work {
    /// A full cycle over `selection` (GUI "Back up all"). When it ends, the
    /// overdue slots of the repos it covered are recomputed from that moment.
    Cycle(RepoSelection),
    /// Anything else that must hold scheduled cycles off (a single-repo or
    /// single-source backup). Reschedules nothing.
    Other,
}

/// What [`Scheduler::poll`] decided.
#[derive(Debug, Default, PartialEq, Eq)]
pub struct Poll {
    /// Run this now. The scheduler counts it as running until `finished(id)`.
    pub run: Option<CycleOrder>,
    /// When to poll again if nothing else happens. `None` means wait for an
    /// input (paused, running, or nothing scheduled).
    pub wake_at: Option<SystemTime>,
}

/// Result of [`Scheduler::finished`].
#[derive(Debug, Default)]
pub struct Rescheduled {
    /// Indices whose slot was recomputed (including those that failed to).
    pub indices: Vec<usize>,
    /// Indices whose cadence could not be computed; they lost their slot.
    pub errors: Vec<(usize, VykarError)>,
}

/// The scheduling state machine shared by the daemon and the GUI.
///
/// It is pure: it holds no clock, and every input that depends on time takes
/// `now`. The frontend sleeps or waits, delivers inputs, and runs what
/// [`Scheduler::poll`] hands out.
///
/// Rules:
///
/// - **One piece of work at a time.** While anything handed out by `poll` or
///   recorded with [`Scheduler::started`] runs, `poll` hands out nothing.
/// - **Ad-hoc first.** A pending [`Scheduler::trigger`] is handed out as one
///   cycle over every repository before any due slot, even when paused (pause
///   is about automatic runs, not explicit requests). Repeated triggers before
///   it runs coalesce.
/// - **Paused** hands out no scheduled cycle.
/// - **Scheduled cycles** cover the repositories whose slot has arrived, in
///   index order.
/// - **Rescheduling happens at the end** ([`Scheduler::finished`]), counted
///   from the end time. Every repository of a scheduled cycle is rescheduled,
///   whatever its outcome. An ad-hoc cycle or a [`Work::Cycle`] reschedules
///   only the repositories it covered whose slot passed while it ran, so a
///   future slot keeps its cadence. [`Work::Other`] reschedules nothing.
/// - **A broken cadence** costs only that repository its slot; the others keep
///   firing. A disabled repository is never due.
/// - **Reload** ([`Scheduler::load`] with `honor_on_startup = false`)
///   recomputes every slot from the reload time. Work that was running keeps
///   holding scheduled cycles off, but its end reschedules nothing, because
///   its indices belong to the previous configuration.
#[derive(Debug, Default)]
pub struct Scheduler {
    schedules: Vec<ScheduleConfig>,
    plan: SchedulePlan,
    paused: bool,
    adhoc_pending: bool,
    running: Vec<Running>,
    generation: u64,
    next_id: u64,
}

#[derive(Debug)]
struct Running {
    id: CycleId,
    generation: u64,
    on_finish: OnFinish,
}

#[derive(Debug)]
enum OnFinish {
    /// Scheduled order: reschedule each of these indices from the end time.
    All(Vec<usize>),
    /// Ad-hoc order or `Work::Cycle`: reschedule only those of the selection
    /// overdue at the end.
    Overdue(RepoSelection),
    /// `Work::Other`.
    Nothing,
}

impl Scheduler {
    /// Replace the schedules (index-aligned with the repositories) and compute
    /// every slot from `now`. Returns the per-index errors of the cadences that
    /// could not be computed; those repositories get no slot.
    ///
    /// `honor_on_startup` is true only on a process's first load; a reload
    /// must pass false. Running work stays running, but its end no longer
    /// reschedules anything. A pending trigger and the pause flag survive.
    pub fn load(
        &mut self,
        schedules: Vec<ScheduleConfig>,
        now: SystemTime,
        honor_on_startup: bool,
    ) -> Vec<(usize, VykarError)> {
        let (plan, errors) = SchedulePlan::new(&schedules, now, honor_on_startup);
        self.plan = plan;
        self.schedules = schedules;
        self.generation = self.generation.wrapping_add(1);
        errors
    }

    /// Stop (or resume) handing out scheduled cycles. Triggers still run.
    pub fn set_paused(&mut self, paused: bool) {
        self.paused = paused;
    }

    pub fn is_paused(&self) -> bool {
        self.paused
    }

    /// Ask for one ad-hoc cycle over every repository; repeated triggers
    /// before it runs coalesce.
    pub fn trigger(&mut self) {
        self.adhoc_pending = true;
    }

    /// Decide what to do at `now`. Hands out at most one order, and nothing
    /// while any work runs.
    pub fn poll(&mut self, now: SystemTime) -> Poll {
        if !self.running.is_empty() {
            return Poll::default();
        }
        if self.adhoc_pending {
            self.adhoc_pending = false;
            let id = self.push_running(OnFinish::Overdue(RepoSelection::All));
            return Poll {
                run: Some(CycleOrder {
                    id,
                    selection: RepoSelection::All,
                    trigger: CycleTrigger::Adhoc,
                }),
                wake_at: None,
            };
        }
        if self.paused {
            return Poll::default();
        }
        let due = self.plan.due(now);
        if !due.is_empty() {
            let id = self.push_running(OnFinish::All(due.clone()));
            return Poll {
                run: Some(CycleOrder {
                    id,
                    selection: RepoSelection::Indices(due),
                    trigger: CycleTrigger::Scheduled,
                }),
                wake_at: None,
            };
        }
        Poll {
            run: None,
            wake_at: self.plan.next_wake(),
        }
    }

    /// Record work the frontend started itself; pass the id to
    /// [`Scheduler::finished`] when it ends.
    pub fn started(&mut self, work: Work) -> CycleId {
        let on_finish = match work {
            Work::Cycle(selection) => OnFinish::Overdue(selection),
            Work::Other => OnFinish::Nothing,
        };
        self.push_running(on_finish)
    }

    /// End running work at `now` (the work's end). Unknown ids are a no-op.
    pub fn finished(&mut self, id: CycleId, now: SystemTime) -> Rescheduled {
        let mut done = Rescheduled::default();
        let Some(pos) = self.running.iter().position(|r| r.id == id) else {
            return done;
        };
        let running = self.running.swap_remove(pos);
        if running.generation != self.generation {
            return done;
        }
        let len = self.plan.len().min(self.schedules.len());
        let candidates: Vec<usize> = match running.on_finish {
            OnFinish::All(indices) => indices.into_iter().filter(|&i| i < len).collect(),
            OnFinish::Overdue(selection) => {
                let covered: Vec<usize> = match selection {
                    RepoSelection::All => (0..len).collect(),
                    RepoSelection::Indices(v) => v,
                    RepoSelection::Explicit(i) => vec![i],
                };
                covered
                    .into_iter()
                    .filter(|&i| i < len && self.plan.next_run(i).is_some_and(|t| t <= now))
                    .collect()
            }
            OnFinish::Nothing => Vec::new(),
        };
        for idx in candidates {
            let Some(schedule) = self.schedules.get(idx) else {
                continue;
            };
            if let Some(e) = self.plan.reschedule(idx, schedule, now) {
                done.errors.push((idx, e));
            }
            done.indices.push(idx);
        }
        done
    }

    /// Whether any work (handed out or started) is running.
    pub fn is_running(&self) -> bool {
        !self.running.is_empty()
    }

    /// Indices of the repositories whose slot has arrived at `now`.
    pub fn due(&self, now: SystemTime) -> Vec<usize> {
        self.plan.due(now)
    }

    /// The earliest slot across all repositories, or `None` when nothing is
    /// scheduled.
    pub fn next_wake(&self) -> Option<SystemTime> {
        self.plan.next_wake()
    }

    /// The slot of one repository, if it has one.
    pub fn next_run(&self, idx: usize) -> Option<SystemTime> {
        self.plan.next_run(idx)
    }

    /// Number of repositories loaded.
    pub fn len(&self) -> usize {
        self.plan.len()
    }

    pub fn is_empty(&self) -> bool {
        self.plan.len() == 0
    }

    fn push_running(&mut self, on_finish: OnFinish) -> CycleId {
        let id = CycleId(self.next_id);
        self.next_id = self.next_id.wrapping_add(1);
        self.running.push(Running {
            id,
            generation: self.generation,
            on_finish,
        });
        id
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sched(enabled: bool, every: Option<&str>, on_startup: bool) -> ScheduleConfig {
        ScheduleConfig {
            enabled,
            every: every.map(str::to_string),
            cron: None,
            on_startup,
            jitter_seconds: 0,
            passphrase_prompt_timeout_seconds: 300,
        }
    }

    #[test]
    fn schedule_key_compares_parsed_intervals() {
        // `60m` and `1h` are the same cadence, not a mixed config.
        assert_eq!(
            schedule_key(&sched(true, Some("60m"), false)),
            schedule_key(&sched(true, Some("1h"), false))
        );
        assert_ne!(
            schedule_key(&sched(true, Some("1h"), false)),
            schedule_key(&sched(true, Some("1d"), false))
        );
        assert_ne!(
            schedule_key(&sched(true, Some("1h"), false)),
            schedule_key(&sched(false, Some("1h"), false))
        );
    }

    #[test]
    fn brief_is_per_repo_when_cadences_differ() {
        let hourly = sched(true, Some("1h"), false);
        let sixty_min = sched(true, Some("60m"), false);
        let daily = sched(true, Some("1d"), false);

        assert_eq!(repos_schedule_brief(&[&hourly, &sixty_min], false), "1h");
        assert_eq!(repos_schedule_brief(&[&hourly, &daily], false), "per-repo");
        assert_eq!(repos_schedule_brief(&[&hourly], true), "Off");
        assert_eq!(repos_schedule_brief(&[], false), "Off");
    }

    #[test]
    fn brief_prefers_cron_then_every_then_the_default() {
        let mut cron = sched(true, Some("1h"), false);
        cron.cron = Some("0 3 * * *".into());
        assert_eq!(schedule_brief(&cron, false), "0 3 * * *");
        assert_eq!(
            schedule_brief(&sched(true, Some("30m"), false), false),
            "30m"
        );
        assert_eq!(schedule_brief(&sched(true, None, false), false), "24h");
        assert_eq!(
            schedule_brief(&sched(false, Some("30m"), false), false),
            "Off"
        );
    }

    /// A fixed start for the fake clock.
    fn t0() -> SystemTime {
        SystemTime::UNIX_EPOCH + Duration::from_secs(1_000_000_000)
    }

    fn mins(m: u64) -> Duration {
        Duration::from_secs(m * 60)
    }

    #[test]
    fn plan_due_set_and_earliest_wake() {
        let fast = sched(true, Some("1s"), false);
        let slow = sched(true, Some("1h"), false);
        let (plan, errors) = SchedulePlan::new(&[slow, fast], t0(), false);
        assert!(errors.is_empty());

        let fast_at = plan.next_run(1).unwrap();
        assert_eq!(fast_at, t0() + Duration::from_secs(1));
        assert_eq!(plan.next_wake(), Some(fast_at));

        // Before the fast slot nothing is due; after it only the fast repo is.
        assert!(plan.due(t0()).is_empty());
        assert_eq!(plan.due(fast_at), vec![1]);
    }

    #[test]
    fn plan_skips_disabled_entries() {
        let off = sched(false, Some("1h"), false);
        let on = sched(true, Some("1h"), false);
        let (plan, errors) = SchedulePlan::new(&[off, on], t0(), false);
        assert!(errors.is_empty());
        assert!(plan.next_run(0).is_none());
        assert!(plan.next_run(1).is_some());
        assert_eq!(plan.next_wake(), plan.next_run(1));
    }

    #[test]
    fn plan_honors_on_startup_only_when_asked() {
        let startup = sched(true, Some("1h"), true);
        let plain = sched(true, Some("1h"), false);

        let (honored, _) = SchedulePlan::new(&[startup.clone(), plain.clone()], t0(), true);
        assert_eq!(honored.due(t0()), vec![0]);

        let (ignored, _) = SchedulePlan::new(&[startup, plain], t0(), false);
        assert!(ignored.due(t0()).is_empty());
    }

    #[test]
    fn plan_reschedule_moves_only_that_entry() {
        let fast = sched(true, Some("1s"), false);
        let slow = sched(true, Some("1h"), false);
        let (mut plan, _) = SchedulePlan::new(&[fast.clone(), slow], t0(), false);

        let slow_before = plan.next_run(1).unwrap();
        let later = t0() + mins(10);
        assert!(plan.reschedule(0, &fast, later).is_none());

        assert_eq!(plan.next_run(1), Some(slow_before));
        assert_eq!(plan.next_run(0), Some(later + Duration::from_secs(1)));
    }

    #[test]
    fn plan_unschedulable_entry_degrades_to_none() {
        let (plan, errors) =
            SchedulePlan::new(&[broken(), sched(true, Some("1h"), false)], t0(), false);
        assert!(plan.next_run(0).is_none());
        assert!(plan.next_run(1).is_some());
        assert_eq!(errors.len(), 1);
        assert_eq!(errors[0].0, 0);
    }

    #[test]
    fn plan_all_disabled_has_no_wake() {
        let off = sched(false, None, false);
        let (plan, errors) = SchedulePlan::new(&[off.clone(), off], t0(), true);
        assert!(errors.is_empty());
        assert_eq!(plan.len(), 2);
        assert!(plan.next_wake().is_none());
        assert!(plan.due(t0()).is_empty());
    }

    fn broken() -> ScheduleConfig {
        ScheduleConfig {
            enabled: true,
            every: None,
            cron: Some("not a cron".into()),
            on_startup: false,
            jitter_seconds: 0,
            passphrase_prompt_timeout_seconds: 300,
        }
    }

    fn hourly() -> ScheduleConfig {
        sched(true, Some("1h"), false)
    }

    /// A scheduler loaded at `t0` with `schedules` (no `on_startup`).
    fn loaded(schedules: Vec<ScheduleConfig>) -> Scheduler {
        let mut s = Scheduler::default();
        assert!(s.load(schedules, t0(), false).is_empty());
        s
    }

    #[test]
    fn scheduled_cycle_reschedules_from_its_end() {
        let mut s = loaded(vec![hourly()]);
        assert_eq!(s.next_run(0), Some(t0() + mins(60)));

        let order = s.poll(t0() + mins(60)).run.unwrap();
        assert_eq!(order.selection, RepoSelection::Indices(vec![0]));
        assert_eq!(order.trigger, CycleTrigger::Scheduled);

        let done = s.finished(order.id, t0() + mins(80));
        assert_eq!(done.indices, vec![0]);
        assert!(done.errors.is_empty());
        assert_eq!(s.next_run(0), Some(t0() + mins(140)));
        assert!(!s.is_running());
    }

    #[test]
    fn nothing_is_handed_out_while_work_runs() {
        let mut s = loaded(vec![hourly()]);
        let order = s.poll(t0() + mins(60)).run.unwrap();
        assert!(s.is_running());
        assert_eq!(s.poll(t0() + mins(61)), Poll::default());
        assert_eq!(s.poll(t0() + mins(600)), Poll::default());
        s.finished(order.id, t0() + mins(70));
        assert!(s.poll(t0() + mins(71)).run.is_none());
    }

    #[test]
    fn only_due_repos_run_in_index_order() {
        let mut s = loaded(vec![
            sched(true, Some("30m"), false),
            hourly(),
            sched(true, Some("20m"), false),
        ]);
        let order = s.poll(t0() + mins(30)).run.unwrap();
        assert_eq!(order.selection, RepoSelection::Indices(vec![0, 2]));
        s.finished(order.id, t0() + mins(35));
        assert_eq!(s.next_run(1), Some(t0() + mins(60)));
    }

    #[test]
    fn every_repo_of_an_order_is_rescheduled_whatever_happened() {
        let mut s = loaded(vec![hourly(), hourly()]);
        let order = s.poll(t0() + mins(60)).run.unwrap();
        assert_eq!(order.selection, RepoSelection::Indices(vec![0, 1]));
        let done = s.finished(order.id, t0() + mins(65));
        assert_eq!(done.indices, vec![0, 1]);
        assert_eq!(s.next_run(0), Some(t0() + mins(125)));
        assert_eq!(s.next_run(1), Some(t0() + mins(125)));
    }

    #[test]
    fn on_startup_only_on_first_load() {
        let mut s = Scheduler::default();
        let schedules = vec![sched(true, Some("1h"), true), hourly()];
        assert!(s.load(schedules.clone(), t0(), true).is_empty());
        assert_eq!(s.due(t0()), vec![0]);

        let reload_at = t0() + mins(5);
        assert!(s.load(schedules, reload_at, false).is_empty());
        assert!(s.due(reload_at).is_empty());
        assert_eq!(s.next_run(0), Some(reload_at + mins(60)));
    }

    #[test]
    fn trigger_preserves_future_slots() {
        let mut s = loaded(vec![hourly()]);
        s.trigger();
        let order = s.poll(t0() + mins(10)).run.unwrap();
        assert_eq!(order.selection, RepoSelection::All);
        assert_eq!(order.trigger, CycleTrigger::Adhoc);
        let done = s.finished(order.id, t0() + mins(20));
        assert!(done.indices.is_empty());
        assert_eq!(s.next_run(0), Some(t0() + mins(60)));
    }

    #[test]
    fn trigger_overrunning_a_slot_moves_it_from_the_end() {
        let mut s = loaded(vec![hourly()]);
        s.trigger();
        let order = s.poll(t0() + mins(50)).run.unwrap();
        let done = s.finished(order.id, t0() + mins(70));
        assert_eq!(done.indices, vec![0]);
        assert_eq!(s.next_run(0), Some(t0() + mins(130)));
    }

    #[test]
    fn trigger_waits_for_running_work_and_coalesces() {
        let mut s = loaded(vec![hourly()]);
        let order = s.poll(t0() + mins(60)).run.unwrap();
        s.trigger();
        s.trigger();
        assert_eq!(s.poll(t0() + mins(61)), Poll::default());
        s.finished(order.id, t0() + mins(62));

        let adhoc = s.poll(t0() + mins(63)).run.unwrap();
        assert_eq!(adhoc.trigger, CycleTrigger::Adhoc);
        s.finished(adhoc.id, t0() + mins(64));
        assert!(s.poll(t0() + mins(65)).run.is_none());
    }

    #[test]
    fn trigger_beats_due_slots_and_absorbs_them() {
        let mut s = loaded(vec![hourly(), sched(true, Some("2h"), false)]);
        s.trigger();
        let at = t0() + mins(60);
        let order = s.poll(at).run.unwrap();
        assert_eq!(order.trigger, CycleTrigger::Adhoc);
        let done = s.finished(order.id, at + mins(5));
        assert_eq!(done.indices, vec![0]);

        let after = s.poll(at + mins(6));
        assert!(after.run.is_none());
        assert!(after.wake_at.unwrap() > at + mins(6));
    }

    #[test]
    fn paused_hands_out_nothing_but_triggers() {
        let mut s = loaded(vec![hourly()]);
        s.set_paused(true);
        assert!(s.is_paused());
        let at = t0() + mins(60);
        assert_eq!(s.poll(at), Poll::default());

        s.trigger();
        let adhoc = s.poll(t0() + mins(10)).run.unwrap();
        assert_eq!(adhoc.trigger, CycleTrigger::Adhoc);
        s.finished(adhoc.id, t0() + mins(20));
        assert_eq!(s.poll(at), Poll::default());

        s.set_paused(false);
        let order = s.poll(at).run.unwrap();
        assert_eq!(order.trigger, CycleTrigger::Scheduled);
    }

    #[test]
    fn started_other_blocks_and_reschedules_nothing() {
        let mut s = loaded(vec![hourly()]);
        let id = s.started(Work::Other);
        let at = t0() + mins(60);
        assert_eq!(s.poll(at), Poll::default());
        let done = s.finished(id, at + mins(10));
        assert!(done.indices.is_empty());
        assert_eq!(s.next_run(0), Some(at));
        assert!(s.poll(at + mins(10)).run.is_some());
    }

    #[test]
    fn started_cycle_reschedules_only_overdue_covered_repos() {
        let mut s = loaded(vec![hourly(), hourly()]);
        let id = s.started(Work::Cycle(RepoSelection::Indices(vec![0])));
        let end = t0() + mins(70);
        let done = s.finished(id, end);
        assert_eq!(done.indices, vec![0]);
        assert_eq!(s.next_run(0), Some(end + mins(60)));
        assert_eq!(s.next_run(1), Some(t0() + mins(60)));
    }

    #[test]
    fn reload_during_work_leaves_new_slots_alone() {
        let mut s = loaded(vec![hourly()]);
        let order = s.poll(t0() + mins(60)).run.unwrap();
        let reload_at = t0() + mins(65);
        assert!(
            s.load(vec![hourly(), hourly()], reload_at, false)
                .is_empty()
        );
        // The old work still holds scheduled cycles off.
        assert_eq!(s.poll(reload_at + mins(120)), Poll::default());
        let done = s.finished(order.id, t0() + mins(90));
        assert!(done.indices.is_empty());
        assert_eq!(s.next_run(0), Some(reload_at + mins(60)));
        assert_eq!(s.next_run(1), Some(reload_at + mins(60)));
    }

    #[test]
    fn finished_with_unknown_id_is_a_no_op() {
        let mut s = loaded(vec![hourly()]);
        let order = s.poll(t0() + mins(60)).run.unwrap();
        let done = s.finished(CycleId(order.id.0 + 100), t0() + mins(61));
        assert!(done.indices.is_empty());
        assert!(s.is_running());
        s.finished(order.id, t0() + mins(62));
        assert!(!s.is_running());
        // A second finish of the same id is unknown too.
        assert!(s.finished(order.id, t0() + mins(63)).indices.is_empty());
    }

    #[test]
    fn wake_at_is_the_earliest_slot_and_none_when_nothing_is_scheduled() {
        let mut s = loaded(vec![sched(true, Some("2h"), false), hourly()]);
        assert_eq!(s.poll(t0()).wake_at, Some(t0() + mins(60)));

        let mut off = loaded(vec![sched(false, None, false), sched(false, None, false)]);
        assert_eq!(off.len(), 2);
        assert_eq!(off.poll(t0()), Poll::default());
        assert!(Scheduler::default().is_empty());
    }

    #[test]
    fn broken_cadence_loses_only_its_slot() {
        let mut s = Scheduler::default();
        let errors = s.load(vec![broken(), hourly()], t0(), false);
        assert_eq!(errors.len(), 1);
        assert_eq!(errors[0].0, 0);
        let order = s.poll(t0() + mins(60)).run.unwrap();
        assert_eq!(order.selection, RepoSelection::Indices(vec![1]));
    }

    #[test]
    fn a_jittered_slot_is_bounded_from_the_end() {
        let mut jittered = hourly();
        jittered.jitter_seconds = 30;
        let mut s = loaded(vec![jittered]);
        let order = s.poll(t0() + mins(61)).run.unwrap();
        let end = t0() + mins(70);
        s.finished(order.id, end);
        let next = s.next_run(0).unwrap();
        assert!(next >= end + mins(60));
        assert!(next <= end + mins(60) + Duration::from_secs(30));
    }

    #[test]
    fn next_run_delay_at_uses_the_given_time_for_cron() {
        let schedule = ScheduleConfig {
            enabled: true,
            every: None,
            cron: Some("*/5 * * * *".into()),
            on_startup: false,
            jitter_seconds: 0,
            passphrase_prompt_timeout_seconds: 300,
        };
        let now = t0() + Duration::from_millis(12_345);
        let delay = next_run_delay_at(&schedule, now).unwrap();
        assert!(delay <= Duration::from_secs(300));

        let cron = crate::config::parse_cron("*/5 * * * *").unwrap();
        let expected = cron_delay_at(&cron, &chrono::DateTime::<chrono::Local>::from(now)).unwrap();
        assert_eq!(delay, expected);
    }

    #[test]
    fn interval_parses_valid_value() {
        let schedule = ScheduleConfig {
            enabled: true,
            every: Some("2h".into()),
            cron: None,
            on_startup: false,
            jitter_seconds: 0,
            passphrase_prompt_timeout_seconds: 300,
        };

        let delay = next_run_delay(&schedule).unwrap();
        assert_eq!(delay.as_secs(), 2 * 3600);
    }

    #[test]
    fn interval_defaults_to_24h_when_none() {
        let schedule = ScheduleConfig {
            enabled: true,
            every: None,
            cron: None,
            on_startup: false,
            jitter_seconds: 0,
            passphrase_prompt_timeout_seconds: 300,
        };

        let delay = next_run_delay(&schedule).unwrap();
        assert_eq!(delay.as_secs(), 24 * 3600);
    }

    #[test]
    fn jitter_bounds_are_respected() {
        for _ in 0..64 {
            let jitter = random_jitter(5).as_secs();
            assert!(jitter <= 5);
        }
    }

    #[test]
    fn cron_delay_at_handles_boundaries() {
        use chrono::{TimeZone, Utc};

        let cron = crate::config::parse_cron("*/5 * * * *").unwrap();
        let at = |h, m, s| Utc.with_ymd_and_hms(2026, 1, 15, h, m, s).unwrap();
        let ms = chrono::Duration::milliseconds;

        let mid = cron_delay_at(&cron, &at(8, 27, 30)).unwrap();
        assert_eq!(mid, Duration::from_secs(150));

        // Sub-second delays are valid; `as_secs()` would truncate this to 0.
        let just_before = cron_delay_at(&cron, &(at(8, 29, 59) + ms(400))).unwrap();
        assert_eq!(just_before, Duration::from_millis(600));

        let on_boundary = cron_delay_at(&cron, &at(8, 30, 0)).unwrap();
        assert_eq!(on_boundary, Duration::from_secs(300));

        let just_after = cron_delay_at(&cron, &(at(8, 30, 0) + ms(500))).unwrap();
        assert_eq!(just_after, Duration::from_millis(299_500));
    }

    #[test]
    fn cron_next_run_is_positive() {
        let schedule = ScheduleConfig {
            enabled: true,
            every: None,
            cron: Some("*/5 * * * *".into()),
            on_startup: false,
            jitter_seconds: 0,
            passphrase_prompt_timeout_seconds: 300,
        };

        let delay = next_run_delay(&schedule).unwrap();
        assert!(delay > Duration::ZERO);
        assert!(delay <= Duration::from_secs(5 * 60));
    }

    #[test]
    fn cron_with_jitter() {
        let schedule = ScheduleConfig {
            enabled: true,
            every: None,
            cron: Some("0 3 * * *".into()),
            on_startup: false,
            jitter_seconds: 60,
            passphrase_prompt_timeout_seconds: 300,
        };

        let delay = next_run_delay(&schedule).unwrap();
        assert!(delay > Duration::ZERO);
        assert!(delay <= Duration::from_secs(24 * 3600 + 60));
    }

    #[test]
    fn next_run_is_in_future() {
        let schedule = ScheduleConfig {
            enabled: true,
            every: Some("30m".into()),
            cron: None,
            on_startup: false,
            jitter_seconds: 0,
            passphrase_prompt_timeout_seconds: 300,
        };

        let delay = next_run_delay(&schedule).unwrap();
        assert!(delay.as_secs() > 0);
    }

    #[test]
    fn scheduler_lock_contention() {
        let dir = tempfile::tempdir().unwrap();
        let lock_path = dir.path().join("scheduler.lock");

        // First acquire succeeds
        let lock1 = SchedulerLock::try_acquire_at(Some(&lock_path));
        assert!(lock1.is_some(), "first lock acquisition should succeed");

        // Second acquire fails (same lock file, a second handle)
        let lock2 = SchedulerLock::try_acquire_at(Some(&lock_path));
        assert!(lock2.is_none(), "second lock acquisition should fail");

        // Drop the first lock — now a new acquire should succeed
        drop(lock1);
        let lock3 = SchedulerLock::try_acquire_at(Some(&lock_path));
        assert!(lock3.is_some(), "lock after release should succeed");
    }
}
