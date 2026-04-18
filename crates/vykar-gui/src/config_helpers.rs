use std::path::PathBuf;

use vykar_core::app;
use vykar_core::config;

/// Load and fully validate a config file: parse YAML, check non-empty, validate schedule.
/// Returns the parsed repos or a human-readable error string.
pub(crate) fn validate_config(
    config_path: &std::path::Path,
) -> Result<Vec<config::ResolvedRepo>, String> {
    let repos = app::load_runtime_config_from_path(config_path).map_err(|e| format!("{e}"))?;
    if repos.is_empty() {
        return Err("Config is empty (no repositories defined).".into());
    }
    // Validate schedule is usable (parses interval or cron)
    vykar_core::app::scheduler::next_run_delay(&repos[0].config.schedule)
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
            std::fs::write(&path, config::minimal_config_template())?;
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
