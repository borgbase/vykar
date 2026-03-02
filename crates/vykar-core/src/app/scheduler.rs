use std::time::{Duration, Instant};

use rand::Rng;

use crate::config::ScheduleConfig;
use vykar_types::error::Result;

pub fn schedule_interval(schedule: &ScheduleConfig) -> Result<Duration> {
    schedule.every_duration()
}

pub fn random_jitter(jitter_seconds: u64) -> Duration {
    if jitter_seconds == 0 {
        return Duration::ZERO;
    }
    let secs = rand::thread_rng().gen_range(0..=jitter_seconds);
    Duration::from_secs(secs)
}

pub fn next_run_in(schedule: &ScheduleConfig, now: Instant) -> Result<Instant> {
    let interval = schedule_interval(schedule)?;
    Ok(now + interval + random_jitter(schedule.jitter_seconds))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn interval_parses_valid_value() {
        let schedule = ScheduleConfig {
            enabled: true,
            every: "2h".to_string(),
            on_startup: false,
            jitter_seconds: 0,
            passphrase_prompt_timeout_seconds: 300,
        };

        assert_eq!(schedule_interval(&schedule).unwrap().as_secs(), 2 * 3600);
    }

    #[test]
    fn jitter_bounds_are_respected() {
        for _ in 0..64 {
            let jitter = random_jitter(5).as_secs();
            assert!(jitter <= 5);
        }
    }

    #[test]
    fn next_run_is_in_future() {
        let schedule = ScheduleConfig {
            enabled: true,
            every: "30m".to_string(),
            on_startup: false,
            jitter_seconds: 0,
            passphrase_prompt_timeout_seconds: 300,
        };

        let now = Instant::now();
        let next = next_run_in(&schedule, now).unwrap();
        assert!(next > now);
    }
}
