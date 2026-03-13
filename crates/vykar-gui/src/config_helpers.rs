use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use crossbeam_channel::Sender;
use vykar_core::app;
use vykar_core::config;

use crate::messages::{AppCommand, UiEvent};
use crate::repo_helpers::send_log;
use crate::scheduler;
use crate::view_models::send_structured_data;

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

/// Apply a (possibly new) config file: load, validate, update runtime state, and notify the UI.
/// When `update_source` is true the runtime source path is switched to `config_path`.
/// Returns `true` on success, `false` on failure.
#[allow(clippy::too_many_arguments)]
pub(crate) fn apply_config(
    config_path: PathBuf,
    update_source: bool,
    runtime: &mut app::RuntimeConfig,
    config_display_path: &mut PathBuf,
    passphrases: &mut HashMap<String, zeroize::Zeroizing<String>>,
    sched: &Arc<Mutex<scheduler::SchedulerState>>,
    schedule_paused: bool,
    scheduler_lock_held: bool,
    ui_tx: &Sender<UiEvent>,
    app_tx: &Sender<AppCommand>,
) -> bool {
    let repos = match validate_config(&config_path) {
        Ok(v) => v,
        Err(msg) => {
            send_log(ui_tx, format!("{msg} Keeping previous config."));
            return false;
        }
    };
    let schedule = repos[0].config.schedule.clone();

    if update_source {
        use vykar_core::config::ConfigSource;
        runtime.source = ConfigSource::SearchOrder {
            path: config_path.clone(),
            level: "user",
        };
    }
    runtime.repos = repos;
    passphrases.clear();

    if let Ok(mut state) = sched.lock() {
        state.enabled = schedule.enabled && scheduler_lock_held;
        state.paused = schedule_paused || !scheduler_lock_held;
        state.every = schedule
            .every_duration()
            .unwrap_or(Duration::from_secs(24 * 60 * 60));
        state.cron = schedule.cron.clone();
        state.jitter_seconds = schedule.jitter_seconds;
        // Compute initial next_run via the scheduler delay (includes jitter)
        let delay = vykar_core::app::scheduler::next_run_delay(&schedule)
            .unwrap_or(Duration::from_secs(24 * 60 * 60));
        state.next_run = Some(Instant::now() + delay);
    }

    let canonical = dunce::canonicalize(&config_path).unwrap_or_else(|_| config_path.clone());
    *config_display_path = canonical.clone();

    let schedule_desc = if scheduler_lock_held {
        scheduler::schedule_description(&schedule, schedule_paused)
    } else {
        "disabled (external scheduler)".to_string()
    };
    let _ = ui_tx.send(UiEvent::ConfigInfo {
        path: canonical.display().to_string(),
        schedule: schedule_desc,
    });
    send_structured_data(ui_tx, &runtime.repos);
    let _ = app_tx.send(AppCommand::FetchAllRepoInfo);
    send_log(ui_tx, "Configuration reloaded.");

    // Send raw config text to populate the editor tab
    match std::fs::read_to_string(&canonical) {
        Ok(text) => {
            let _ = ui_tx.send(UiEvent::ConfigText(text));
        }
        Err(e) => {
            send_log(ui_tx, format!("Could not read config file for editor: {e}"));
        }
    }

    true
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
