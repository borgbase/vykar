use std::io::{self, Write};
use std::path::{Path, PathBuf};

use vykar_core::app;
use vykar_core::config;

fn finish_secure_write(mut file: std::fs::File, contents: &[u8]) -> io::Result<()> {
    // fchmod before writing so contents never exist at a wider mode.
    // apply_mode_fd is a no-op on non-Unix.
    vykar_core::platform::fs::apply_mode_fd(&file, 0o600)?;
    file.write_all(contents)?;
    file.sync_all()
}

/// Create a new config file with owner-only permissions (0o600 on Unix).
/// Fails with AlreadyExists if `path` already exists.
pub(crate) fn create_new_config_secure(path: &Path, contents: &[u8]) -> io::Result<()> {
    let mut opts = std::fs::OpenOptions::new();
    opts.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        opts.mode(0o600);
    }
    let file = opts.open(path)?;
    finish_secure_write(file, contents)
}

/// Overwrite `path` with `contents`, applying owner-only permissions (0o600)
/// via fchmod on the open fd before writing. Tolerates a stale file already
/// present at `path` — intended for tmp files in an atomic-rename flow.
pub(crate) fn write_tmp_secure(path: &Path, contents: &[u8]) -> io::Result<()> {
    let mut opts = std::fs::OpenOptions::new();
    opts.write(true).create(true).truncate(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        opts.mode(0o600);
    }
    let file = opts.open(path)?;
    finish_secure_write(file, contents)
}

/// Load and fully validate a config file: parse YAML, check non-empty, validate schedule.
/// Returns the parsed repos or a human-readable error string.
pub(crate) fn validate_config(
    config_path: &std::path::Path,
) -> Result<Vec<config::ResolvedRepo>, String> {
    let repos = app::load_runtime_config_from_path(config_path).map_err(|e| format!("{e}"))?;
    let first = repos
        .first()
        .ok_or("Config is empty (no repositories defined).")?;
    // Validate schedule is usable (parses interval or cron)
    vykar_core::app::scheduler::next_run_delay(&first.config.schedule)
        .map_err(|e| format!("Invalid schedule: {e}"))?;
    Ok(repos)
}

pub(crate) fn resolve_or_create_config(
    saved_config_path: Option<&str>,
) -> Result<app::RuntimeConfig, Box<dyn std::error::Error>> {
    use vykar_core::config::ConfigSource;

    // 0. Try saved config path from GUI state (if file still exists)
    if let Some(saved) = saved_config_path {
        let path = PathBuf::from(saved);
        if path.is_file() {
            if let Ok(repos) = config::load_and_resolve(&path) {
                let source = ConfigSource::SearchOrder {
                    path,
                    level: "user",
                };
                return Ok(app::RuntimeConfig { source, repos });
            }
        }
    }

    // 1. Check standard search paths (env var, project, user, system)
    if let Some(source) = config::resolve_config_path(None) {
        let repos = config::load_and_resolve(source.path())?;
        return Ok(app::RuntimeConfig { source, repos });
    }

    // 2. No config found — ask the user what to do
    let user_config_path = config::default_config_search_paths()
        .into_iter()
        .find(|(_, level)| *level == "user")
        .map(|(p, _)| p);

    let message = match &user_config_path {
        Some(p) => format!(
            "No vykar configuration file was found.\n\n\
             Create a starter config at\n{}?\n\n\
             Select No to open an existing file instead.",
            p.display()
        ),
        None => "No vykar configuration file was found.\n\n\
                 Select Yes to pick an existing config file."
            .to_string(),
    };

    let choice = tinyfiledialogs::message_box_yes_no(
        "No configuration found",
        &message,
        tinyfiledialogs::MessageBoxIcon::Question,
        tinyfiledialogs::YesNo::Yes,
    );

    let config_path = if choice == tinyfiledialogs::YesNo::Yes {
        if let Some(path) = user_config_path {
            if let Some(parent) = path.parent() {
                std::fs::create_dir_all(parent)?;
            }
            create_new_config_secure(&path, config::minimal_config_template().as_bytes())?;
            path
        } else {
            // No user-level path available, fall through to file picker
            let picked = tinyfiledialogs::open_file_dialog(
                "Open vykar config",
                "",
                Some((&["*.yaml", "*.yml"], "YAML files")),
            );
            match picked {
                Some(p) => PathBuf::from(p),
                None => std::process::exit(0),
            }
        }
    } else {
        let picked = tinyfiledialogs::open_file_dialog(
            "Open vykar config",
            "",
            Some((&["*.yaml", "*.yml"], "YAML files")),
        );
        match picked {
            Some(p) => PathBuf::from(p),
            None => std::process::exit(0),
        }
    };

    let repos = config::load_and_resolve(&config_path)?;
    let source = ConfigSource::SearchOrder {
        path: config_path,
        level: "user",
    };
    Ok(app::RuntimeConfig { source, repos })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[cfg(unix)]
    fn mode_of(path: &Path) -> u32 {
        use std::os::unix::fs::PermissionsExt;
        std::fs::metadata(path).unwrap().permissions().mode()
    }

    #[test]
    fn write_tmp_secure_overrides_stale_wide_mode() {
        let dir = tempfile::tempdir().unwrap();
        let tmp_path = dir.path().join("config.yaml.tmp");

        std::fs::write(&tmp_path, b"leaked-old").unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(&tmp_path, std::fs::Permissions::from_mode(0o644)).unwrap();
        }

        write_tmp_secure(&tmp_path, b"fresh").unwrap();

        #[cfg(unix)]
        assert_eq!(mode_of(&tmp_path) & 0o077, 0);
        assert_eq!(std::fs::read(&tmp_path).unwrap(), b"fresh");
    }

    #[test]
    fn create_new_config_secure_fresh_mode() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("config.yaml");

        create_new_config_secure(&path, b"template").unwrap();

        #[cfg(unix)]
        assert_eq!(mode_of(&path) & 0o077, 0);
        assert_eq!(std::fs::read(&path).unwrap(), b"template");
    }

    #[test]
    fn create_new_config_secure_refuses_to_clobber() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("config.yaml");
        std::fs::write(&path, b"original").unwrap();

        let err = create_new_config_secure(&path, b"new").unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::AlreadyExists);
        assert_eq!(std::fs::read(&path).unwrap(), b"original");
    }
}
