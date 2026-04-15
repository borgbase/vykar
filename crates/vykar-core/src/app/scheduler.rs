use std::fs::File;
use std::time::Duration;

use rand::Rng;

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
        // SAFETY: fd is valid and we pass well-defined flags.
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

#[cfg(test)]
mod tests {
    use super::*;

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
