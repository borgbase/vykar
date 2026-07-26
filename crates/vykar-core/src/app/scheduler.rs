// libc::flock for the process-wide scheduler lock; SAFETY documented per block.
#![allow(unsafe_code)]

use std::fs::File;
use std::time::{Duration, SystemTime};

use rand::RngExt;

use crate::config::ScheduleConfig;
use vykar_types::error::{Result, VykarError};

/// Process-wide scheduler lock backed by `flock(2)`.
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

        if let Some(parent) = path.parent() {
            if let Err(e) = std::fs::create_dir_all(parent) {
                tracing::warn!(%e, "could not create scheduler lock dir; proceeding without lock");
                return Some(Self { _file: None });
            }
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

        Self::platform_flock(file)
    }

    #[cfg(unix)]
    fn platform_flock(file: File) -> Option<Self> {
        use std::os::unix::io::AsRawFd;
        let fd = file.as_raw_fd();
        // SAFETY: fd belongs to the `file` owned in this scope (still alive
        // through the call), and the flag set is a valid combination per the
        // flock(2) contract.
        let ret = unsafe { libc::flock(fd, libc::LOCK_EX | libc::LOCK_NB) };
        if ret == 0 {
            Some(Self { _file: Some(file) })
        } else {
            let errno = std::io::Error::last_os_error();
            match errno.raw_os_error() {
                // EWOULDBLOCK and EAGAIN alias on most platforms; check both for portability.
                Some(e) if e == libc::EWOULDBLOCK || e == libc::EAGAIN => None,
                _ => {
                    tracing::warn!(%errno, "flock failed unexpectedly; proceeding without lock");
                    Some(Self { _file: Some(file) })
                }
            }
        }
    }

    #[cfg(windows)]
    fn platform_flock(file: File) -> Option<Self> {
        // Windows locking not implemented yet — fail-open.
        Some(Self { _file: Some(file) })
    }
}

pub fn random_jitter(jitter_seconds: u64) -> Duration {
    if jitter_seconds == 0 {
        return Duration::ZERO;
    }
    let secs = rand::rng().random_range(0..=jitter_seconds);
    Duration::from_secs(secs)
}

/// Compute the delay until the next cron tick, plus jitter.
fn next_cron_delay(schedule: &ScheduleConfig) -> Result<Duration> {
    let expr = schedule.cron.as_deref().unwrap_or("");
    let cron: croner::Cron = expr
        .parse()
        .map_err(|e| VykarError::Config(format!("schedule.cron: invalid expression: {e}")))?;

    let now = chrono::Local::now();
    let next = cron
        .find_next_occurrence(&now, false)
        .map_err(|e| VykarError::Config(format!("schedule.cron: no next occurrence: {e}")))?;

    let delay = (next - now).to_std().unwrap_or(Duration::from_secs(60));

    Ok(delay + random_jitter(schedule.jitter_seconds))
}

/// Unified entry point: returns the delay until the next scheduled run.
/// Uses cron when `schedule.cron` is set, otherwise falls back to `every` interval.
pub fn next_run_delay(schedule: &ScheduleConfig) -> Result<Duration> {
    if schedule.is_cron() {
        next_cron_delay(schedule)
    } else {
        Ok(schedule.every_duration()? + random_jitter(schedule.jitter_seconds))
    }
}

/// Per-repository next-run times, index-aligned with a slice of repositories.
///
/// Both schedulers (daemon and GUI) share this so the due-set and next-wake
/// logic exist in one place. An entry is `None` when the repo's schedule is
/// disabled *or* unschedulable — a broken cadence on one repo must not stop the
/// others from running, so construction degrades instead of failing.
#[derive(Debug, Clone, Default)]
pub struct SchedulePlan {
    next_runs: Vec<Option<SystemTime>>,
}

impl SchedulePlan {
    /// Build a plan from one schedule per repository, in repository order.
    ///
    /// Returns the plan plus the per-index errors of any entry whose next run
    /// could not be computed; the caller logs them (core never prints). Those
    /// entries are `None` and simply never fire on the timer.
    ///
    /// `honor_on_startup` is false on the SIGHUP/reload path, where a repo's
    /// `on_startup` must not re-trigger an immediate backup.
    pub fn new(
        schedules: &[&ScheduleConfig],
        honor_on_startup: bool,
    ) -> (Self, Vec<(usize, VykarError)>) {
        let now = SystemTime::now();
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
            match next_run_delay(schedule) {
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
    /// entry is disabled/unschedulable (the timer then never fires; ad-hoc
    /// triggers and the status page still work).
    pub fn next_wake(&self) -> Option<SystemTime> {
        self.next_runs.iter().flatten().min().copied()
    }

    /// Indices of the repositories whose next run has arrived.
    pub fn due(&self, now: SystemTime) -> Vec<usize> {
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
    pub fn next_run(&self, idx: usize) -> Option<SystemTime> {
        self.next_runs.get(idx).copied().flatten()
    }

    pub fn len(&self) -> usize {
        self.next_runs.len()
    }

    pub fn is_empty(&self) -> bool {
        self.next_runs.is_empty()
    }

    /// Move one entry to `now + interval` (drift, not fixed slots). On error the
    /// entry becomes `None` and the error is returned for the caller to log.
    ///
    /// `idx` must index this plan — indices come from [`SchedulePlan::due`], so
    /// an out-of-range one is a caller bug. It trips a `debug_assert!` and is a
    /// no-op in release; note that the `None` return then reads as "no error",
    /// not as "not rescheduled".
    pub fn reschedule(&mut self, idx: usize, schedule: &ScheduleConfig) -> Option<VykarError> {
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
        match next_run_delay(schedule) {
            Ok(delay) => {
                *slot = Some(SystemTime::now() + delay);
                None
            }
            Err(e) => {
                *slot = None;
                Some(e)
            }
        }
    }

    /// Reschedule only the entries whose slot has already passed, leaving future
    /// slots untouched. Used after an ad-hoc (SIGUSR1) cycle so the configured
    /// cadence is preserved when the cycle finished before the next slot.
    pub fn reschedule_overdue(
        &mut self,
        schedules: &[&ScheduleConfig],
        now: SystemTime,
    ) -> Vec<(usize, VykarError)> {
        let mut errors = Vec::new();
        for idx in self.due(now) {
            let Some(schedule) = schedules.get(idx) else {
                continue;
            };
            if let Some(e) = self.reschedule(idx, schedule) {
                errors.push((idx, e));
            }
        }
        errors
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
    fn plan_due_set_and_earliest_wake() {
        let fast = sched(true, Some("1s"), false);
        let slow = sched(true, Some("1h"), false);
        let (plan, errors) = SchedulePlan::new(&[&slow, &fast], false);
        assert!(errors.is_empty());

        let fast_at = plan.next_run(1).unwrap();
        assert_eq!(plan.next_wake(), Some(fast_at));

        // Before the fast slot nothing is due; after it only the fast repo is.
        assert!(plan.due(SystemTime::now()).is_empty());
        assert_eq!(plan.due(fast_at), vec![1]);
    }

    #[test]
    fn plan_skips_disabled_entries() {
        let off = sched(false, Some("1h"), false);
        let on = sched(true, Some("1h"), false);
        let (plan, errors) = SchedulePlan::new(&[&off, &on], false);
        assert!(errors.is_empty());
        assert!(plan.next_run(0).is_none());
        assert!(plan.next_run(1).is_some());
        assert_eq!(plan.next_wake(), plan.next_run(1));
    }

    #[test]
    fn plan_honors_on_startup_only_when_asked() {
        let startup = sched(true, Some("1h"), true);
        let plain = sched(true, Some("1h"), false);

        let (honored, _) = SchedulePlan::new(&[&startup, &plain], true);
        assert_eq!(honored.due(SystemTime::now()), vec![0]);

        let (ignored, _) = SchedulePlan::new(&[&startup, &plain], false);
        assert!(ignored.due(SystemTime::now()).is_empty());
    }

    #[test]
    fn plan_reschedule_moves_only_that_entry() {
        let fast = sched(true, Some("1s"), false);
        let slow = sched(true, Some("1h"), false);
        let (mut plan, _) = SchedulePlan::new(&[&fast, &slow], false);

        let slow_before = plan.next_run(1).unwrap();
        let fast_before = plan.next_run(0).unwrap();
        assert!(plan.reschedule(0, &fast).is_none());

        assert_eq!(plan.next_run(1), Some(slow_before));
        assert!(plan.next_run(0).unwrap() >= fast_before);
    }

    #[test]
    fn plan_reschedule_overdue_preserves_future_slots() {
        let overdue = sched(true, Some("1h"), true); // fires immediately
        let future = sched(true, Some("1h"), false);
        let (mut plan, _) = SchedulePlan::new(&[&overdue, &future], true);

        let future_before = plan.next_run(1).unwrap();
        let errors = plan.reschedule_overdue(&[&overdue, &future], SystemTime::now());
        assert!(errors.is_empty());

        assert_eq!(plan.next_run(1), Some(future_before));
        assert!(plan.due(SystemTime::now()).is_empty());
    }

    #[test]
    fn plan_unschedulable_entry_degrades_to_none() {
        let bad = ScheduleConfig {
            enabled: true,
            every: None,
            cron: Some("not a cron".into()),
            on_startup: false,
            jitter_seconds: 0,
            passphrase_prompt_timeout_seconds: 300,
        };
        let good = sched(true, Some("1h"), false);

        let (plan, errors) = SchedulePlan::new(&[&bad, &good], false);
        assert!(plan.next_run(0).is_none());
        assert!(plan.next_run(1).is_some());
        assert_eq!(errors.len(), 1);
        assert_eq!(errors[0].0, 0);
    }

    #[test]
    fn plan_all_disabled_has_no_wake() {
        let off = sched(false, None, false);
        let (plan, errors) = SchedulePlan::new(&[&off, &off], true);
        assert!(errors.is_empty());
        assert_eq!(plan.len(), 2);
        assert!(plan.next_wake().is_none());
        assert!(plan.due(SystemTime::now()).is_empty());
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
        assert!(delay.as_secs() > 0);
        assert!(delay.as_secs() <= 5 * 60);
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
        // Should be positive (cron delay + up to 60s jitter)
        assert!(delay.as_secs() > 0);
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

    #[cfg(unix)]
    #[test]
    fn scheduler_lock_contention() {
        let dir = tempfile::tempdir().unwrap();
        let lock_path = dir.path().join("scheduler.lock");

        // First acquire succeeds
        let lock1 = SchedulerLock::try_acquire_at(Some(&lock_path));
        assert!(lock1.is_some(), "first lock acquisition should succeed");

        // Second acquire fails (same lock file, different open-file-description)
        let lock2 = SchedulerLock::try_acquire_at(Some(&lock_path));
        assert!(lock2.is_none(), "second lock acquisition should fail");

        // Drop the first lock — now a new acquire should succeed
        drop(lock1);
        let lock3 = SchedulerLock::try_acquire_at(Some(&lock_path));
        assert!(lock3.is_some(), "lock after release should succeed");
    }
}
