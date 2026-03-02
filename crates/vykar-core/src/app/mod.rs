use std::path::Path;

use crate::config::{self, ConfigSource, ResolvedRepo, ScheduleConfig};
use vykar_types::error::{Result, VykarError};

pub mod operations;
pub mod passphrase;
pub mod scheduler;

#[derive(Debug, Clone)]
pub struct RuntimeConfig {
    pub source: ConfigSource,
    pub repos: Vec<ResolvedRepo>,
}

impl RuntimeConfig {
    pub fn schedule(&self) -> ScheduleConfig {
        let mut iter = self.repos.iter().map(|r| r.config.schedule.clone());
        let Some(mut merged) = iter.next() else {
            return ScheduleConfig::default();
        };

        // For multi-repo configs, pick the shortest interval and union the
        // trigger semantics so no repo's cadence is accidentally ignored.
        for schedule in iter {
            merged.enabled |= schedule.enabled;
            merged.on_startup |= schedule.on_startup;
            merged.jitter_seconds = merged.jitter_seconds.max(schedule.jitter_seconds);
            merged.passphrase_prompt_timeout_seconds = merged
                .passphrase_prompt_timeout_seconds
                .max(schedule.passphrase_prompt_timeout_seconds);

            let candidate_secs = schedule.every_duration().map(|d| d.as_secs()).ok();
            let merged_secs = merged.every_duration().map(|d| d.as_secs()).ok();
            match (merged_secs, candidate_secs) {
                (Some(current), Some(candidate)) if candidate < current => {
                    merged.every = schedule.every.clone();
                }
                _ => {}
            }
        }

        merged
    }
}

pub fn load_runtime_config(config_path: Option<&str>) -> Result<RuntimeConfig> {
    let source = config::resolve_config_path(config_path).ok_or_else(|| {
        VykarError::Config("no configuration file found in default search paths".into())
    })?;
    let repos = config::load_and_resolve(source.path())?;
    Ok(RuntimeConfig { source, repos })
}

pub fn load_runtime_config_from_path(path: &Path) -> Result<Vec<ResolvedRepo>> {
    config::load_and_resolve(path)
}
