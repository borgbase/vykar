//! Worker test harness shared by the handler tests.

use std::collections::HashMap;
use std::path::Path;

use vykar_core::config;

use crate::messages::{AppCommand, UiEvent};

use super::WorkerContext;

pub(super) struct Harness {
    pub(super) ctx: WorkerContext,
    pub(super) ui_rx: crossbeam_channel::Receiver<UiEvent>,
    pub(super) tmp: tempfile::TempDir,
    /// Commands the worker queued for itself. Held so sends keep succeeding.
    pub(super) app_rx: crossbeam_channel::Receiver<AppCommand>,
    // Held so sends on the context's channels keep succeeding.
    _sched_rx: crossbeam_channel::Receiver<()>,
}

/// A worker over the config `build_yaml` returns, written to
/// `<tmp>/config.yaml`. `build_yaml` gets the temp dir, so the config can point
/// the repo, sources and `cache_dir` inside it.
pub(super) fn harness(build_yaml: impl FnOnce(&Path) -> String) -> Harness {
    crate::init_test_environment();
    let tmp = tempfile::tempdir().unwrap();
    let path = tmp.path().join("config.yaml");
    std::fs::write(&path, build_yaml(tmp.path())).unwrap();

    let (ui_tx, ui_rx) = crossbeam_channel::unbounded();
    let (app_tx, app_rx) = crossbeam_channel::unbounded();
    let (sched_notify_tx, sched_rx) = crossbeam_channel::bounded(1);
    let ctx = WorkerContext {
        service: vykar_core::app::AppService::new(),
        repo_info: HashMap::new(),
        config_display_path: path.clone(),
        runtime: vykar_core::app::RuntimeConfig {
            repos: config::load_and_resolve(&path).unwrap(),
            source: config::ConfigSource::CliArg(path),
        },
        app_tx,
        ui_tx,
        sched_notify_tx,
        scheduler: Default::default(),
        operation_running: Default::default(),
        cancel_requested: Default::default(),
        scheduler_lock_held: true,
        schedule_paused: false,
        mount: None,
    };
    Harness {
        ctx,
        ui_rx,
        tmp,
        app_rx,
        _sched_rx: sched_rx,
    }
}

/// Log lines and red-status messages emitted so far.
pub(super) fn drain(rx: &crossbeam_channel::Receiver<UiEvent>) -> (Vec<String>, Vec<String>) {
    let (mut logs, mut errors) = (Vec::new(), Vec::new());
    for event in rx.try_iter() {
        match event {
            UiEvent::LogEntry { message, .. } => logs.push(message),
            UiEvent::ErrorStatus(message) => errors.push(message),
            _ => {}
        }
    }
    (logs, errors)
}

/// Hook command appending `line` (a literal word) to `path`, mirroring
/// `vykar_core::testutil::cmd_append`, which is not visible outside core.
pub(super) fn cmd_append(path: &Path, line: &str) -> String {
    #[cfg(windows)]
    {
        let escaped = path.display().to_string().replace('\'', "''");
        format!("Add-Content -Path '{escaped}' -Value {line}")
    }

    #[cfg(not(windows))]
    {
        format!("echo {line} >> '{}'", path.display())
    }
}
