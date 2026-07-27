use std::path::Path;

use crate::config::{self, ConfigSource, ResolvedRepo};
use vykar_types::error::{Result, VykarError};

pub(crate) mod check_state;
pub mod operations;
pub mod passphrase;
pub mod scheduler;
pub mod views;

#[derive(Debug, Clone)]
pub struct RuntimeConfig {
    pub source: ConfigSource,
    pub repos: Vec<ResolvedRepo>,
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
