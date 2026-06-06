use std::fmt;
use std::path::{Path, PathBuf};

use vykar_common::paths;

/// Tracks where the config file was found.
#[derive(Debug, Clone)]
pub enum ConfigSource {
    /// Explicitly passed via `--config`.
    CliArg(PathBuf),
    /// Set via the `VYKAR_CONFIG` env var.
    EnvVar(PathBuf),
    /// Found by searching standard locations.
    SearchOrder { path: PathBuf, level: &'static str },
}

impl ConfigSource {
    pub fn path(&self) -> &Path {
        match self {
            ConfigSource::CliArg(p) => p,
            ConfigSource::EnvVar(p) => p,
            ConfigSource::SearchOrder { path, .. } => path,
        }
    }
}

impl fmt::Display for ConfigSource {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ConfigSource::CliArg(p) => write!(f, "{} (--config)", p.display()),
            ConfigSource::EnvVar(p) => write!(f, "{} (VYKAR_CONFIG)", p.display()),
            ConfigSource::SearchOrder { path, level } => {
                write!(f, "{} ({})", path.display(), level)
            }
        }
    }
}

/// Returns search locations in priority order: project, user, system.
pub fn default_config_search_paths() -> Vec<(PathBuf, &'static str)> {
    let mut paths = vec![(PathBuf::from("vykar.yaml"), "project")];

    #[cfg(windows)]
    let user_config = paths::config_dir().map(|base| base.join("vykar").join("config.yaml"));

    #[cfg(not(windows))]
    let user_config = std::env::var_os("XDG_CONFIG_HOME")
        .map(PathBuf::from)
        .filter(|p| p.is_absolute())
        .or_else(|| paths::home_dir().map(|h| h.join(".config")))
        .map(|base| base.join("vykar").join("config.yaml"));

    if let Some(p) = user_config {
        paths.push((p, "user"));
    }

    #[cfg(windows)]
    {
        let program_data = std::env::var_os("PROGRAMDATA")
            .map(PathBuf::from)
            .unwrap_or_else(|| PathBuf::from(r"C:\ProgramData"));
        paths.push((program_data.join("vykar").join("config.yaml"), "system"));
    }

    #[cfg(not(windows))]
    {
        // System config
        paths.push((PathBuf::from("/etc/vykar/config.yaml"), "system"));
    }

    paths
}

/// Resolve which config file to use.
///
/// Priority: CLI arg > `VYKAR_CONFIG` env var > first existing file from search paths.
/// Returns `None` if nothing is found.
pub fn resolve_config_path(cli_config: Option<&str>) -> Option<ConfigSource> {
    // 1. Explicit --config
    if let Some(path) = cli_config {
        return Some(ConfigSource::CliArg(PathBuf::from(path)));
    }

    // 2. VYKAR_CONFIG env var
    if let Ok(val) = std::env::var("VYKAR_CONFIG") {
        if !val.is_empty() {
            return Some(ConfigSource::EnvVar(PathBuf::from(val)));
        }
    }

    // 3. Search standard locations
    for (path, level) in default_config_search_paths() {
        if path.exists() {
            return Some(ConfigSource::SearchOrder { path, level });
        }
    }

    None
}

#[cfg(test)]
mod tests {
    use super::super::test_support::EnvGuard;
    use super::*;
    use crate::testutil::CWD_LOCK;
    use std::fs;

    #[test]
    fn test_search_paths_order() {
        let paths = default_config_search_paths();
        assert!(paths.len() >= 2);
        assert_eq!(paths[0].1, "project");
        // Last entry should be system
        assert_eq!(paths.last().unwrap().1, "system");
        // If there are 3 entries, middle is user
        if paths.len() == 3 {
            assert_eq!(paths[1].1, "user");
        }
    }

    #[test]
    fn test_resolve_cli_arg_wins() {
        let result = resolve_config_path(Some("/tmp/override.yaml"));
        let source = result.unwrap();
        assert!(matches!(source, ConfigSource::CliArg(_)));
        assert_eq!(source.path(), Path::new("/tmp/override.yaml"));
    }

    #[test]
    fn test_resolve_env_var() {
        let _lock = CWD_LOCK.lock().unwrap();
        let _guard = EnvGuard::set("VYKAR_CONFIG", "/tmp/env-config.yaml");
        let result = resolve_config_path(None);
        let source = result.unwrap();
        assert!(matches!(source, ConfigSource::EnvVar(_)));
        assert_eq!(source.path(), Path::new("/tmp/env-config.yaml"));
    }

    #[test]
    fn test_resolve_search_finds_project() {
        let _lock = CWD_LOCK.lock().unwrap();
        let dir = tempfile::tempdir().unwrap();
        let config_path = dir.path().join("vykar.yaml");
        fs::write(&config_path, "repositories:\n  - url: /tmp/repo\n").unwrap();

        let original = std::env::current_dir().unwrap();
        std::env::set_current_dir(dir.path()).unwrap();
        let _env_guard = EnvGuard::set("VYKAR_CONFIG", "");

        let result = resolve_config_path(None);
        std::env::set_current_dir(original).unwrap();

        let source = result.unwrap();
        assert!(matches!(
            source,
            ConfigSource::SearchOrder {
                level: "project",
                ..
            }
        ));
    }

    #[test]
    fn test_resolve_nothing_found() {
        let _lock = CWD_LOCK.lock().unwrap();
        let dir = tempfile::tempdir().unwrap();
        let original = std::env::current_dir().unwrap();
        std::env::set_current_dir(dir.path()).unwrap();
        let _env_guard = EnvGuard::set("VYKAR_CONFIG", "");
        let _xdg_guard = EnvGuard::set("XDG_CONFIG_HOME", dir.path().to_str().unwrap());

        let result = resolve_config_path(None);
        std::env::set_current_dir(original).unwrap();

        assert!(result.is_none());
    }
}
