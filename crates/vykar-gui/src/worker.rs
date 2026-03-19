use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use chrono::{DateTime, Local};
use crossbeam_channel::{Receiver, Sender};
use vykar_core::app::{self, operations};
use vykar_core::commands::find::{FileStatus, FindFilter, FindScope};
use vykar_core::commands::init;
use vykar_core::config;
use vykar_types::error::VykarError;

use crate::config_helpers;
use crate::messages::{AppCommand, FindResultRow, RepoInfoData, SnapshotRowData, UiEvent};
use crate::progress::{
    format_bytes, format_check_status, format_count, format_step_outcome, BackupStatusTracker,
};
use crate::repo_helpers::{
    find_repo_for_snapshot, format_repo_name, get_or_resolve_passphrase, log_backup_report,
    select_repos, send_log,
};
use crate::scheduler;
use crate::view_models::send_structured_data;
use crate::APP_TITLE;

fn finish_operation(
    backup_running: &AtomicBool,
    ui_tx: &Sender<UiEvent>,
    sched_notify_tx: &Sender<()>,
) {
    backup_running.store(false, Ordering::SeqCst);
    let _ = ui_tx.send(UiEvent::OperationFinished);
    let _ = ui_tx.send(UiEvent::Status("Idle".to_string()));
    let _ = sched_notify_tx.try_send(());
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn run_worker(
    app_tx: Sender<AppCommand>,
    cmd_rx: Receiver<AppCommand>,
    ui_tx: Sender<UiEvent>,
    scheduler: Arc<Mutex<scheduler::SchedulerState>>,
    backup_running: Arc<AtomicBool>,
    cancel_requested: Arc<AtomicBool>,
    mut runtime: app::RuntimeConfig,
    scheduler_lock_held: bool,
    sched_notify_tx: Sender<()>,
) {
    let mut passphrases: HashMap<String, zeroize::Zeroizing<String>> = HashMap::new();

    let mut config_display_path = dunce::canonicalize(runtime.source.path())
        .unwrap_or_else(|_| runtime.source.path().to_path_buf());

    let schedule = runtime.schedule();
    let schedule_paused = !scheduler_lock_held;
    let schedule_delay = vykar_core::app::scheduler::next_run_delay(&schedule)
        .unwrap_or_else(|_| Duration::from_secs(24 * 60 * 60));

    if let Ok(mut state) = scheduler.lock() {
        state.enabled = schedule.enabled && scheduler_lock_held;
        state.paused = !scheduler_lock_held;
        state.every = schedule
            .every_duration()
            .unwrap_or(Duration::from_secs(24 * 60 * 60));
        state.cron = schedule.cron.clone();
        state.jitter_seconds = schedule.jitter_seconds;
        state.next_run = Some(Instant::now() + schedule_delay);
    }
    let _ = sched_notify_tx.try_send(());

    let schedule_desc = if scheduler_lock_held {
        scheduler::schedule_description(&schedule, false)
    } else {
        "disabled (external scheduler)".to_string()
    };
    let _ = ui_tx.send(UiEvent::ConfigInfo {
        path: config_display_path.display().to_string(),
        schedule: schedule_desc,
    });

    send_structured_data(&ui_tx, &runtime.repos);

    // Populate the editor tab with the current config file contents
    if let Ok(text) = std::fs::read_to_string(&config_display_path) {
        let _ = ui_tx.send(UiEvent::ConfigText(text));
    }

    // Auto-fetch repo info at startup
    let _ = app_tx.send(AppCommand::FetchAllRepoInfo);

    if scheduler_lock_held && schedule.enabled && schedule.on_startup {
        send_log(
            &ui_tx,
            "Scheduled on-startup backup requested by configuration.",
        );
        let _ = app_tx.send(AppCommand::RunBackupAll { scheduled: true });
    }

    while let Ok(cmd) = cmd_rx.recv() {
        match cmd {
            AppCommand::RunBackupAll { scheduled } => {
                cancel_requested.store(false, Ordering::SeqCst);
                backup_running.store(true, Ordering::SeqCst);
                let _ = ui_tx.send(UiEvent::OperationStarted { cancellable: true });
                let _ = ui_tx.send(UiEvent::Status(if scheduled {
                    "Running scheduled backup cycle...".to_string()
                } else {
                    "Running backup cycle...".to_string()
                }));

                let mut any_snapshots_created = false;
                let total = runtime.repos.len();
                for (i, repo) in runtime.repos.iter().enumerate() {
                    if cancel_requested.load(Ordering::SeqCst) {
                        send_log(&ui_tx, "Backup cancelled by user.");
                        break;
                    }

                    let repo_name = format_repo_name(repo);
                    let _ = ui_tx.send(UiEvent::Status(format!(
                        "[{}] ({}/{total})...",
                        repo_name,
                        i + 1
                    )));

                    let passphrase = match get_or_resolve_passphrase(repo, &mut passphrases) {
                        Ok(pass) => pass,
                        Err(e) => {
                            send_log(
                                &ui_tx,
                                format!("[{repo_name}] failed to resolve passphrase: {e}"),
                            );
                            continue;
                        }
                    };

                    if repo.config.encryption.mode != vykar_core::config::EncryptionModeConfig::None
                        && passphrase.is_none()
                    {
                        send_log(
                            &ui_tx,
                            format!(
                                "[{repo_name}] passphrase prompt canceled; skipping this repository"
                            ),
                        );
                        continue;
                    }

                    let mut tracker = BackupStatusTracker::new(repo_name.clone());
                    let ui_tx_progress = ui_tx.clone();
                    let rn = repo_name.clone();
                    let result = operations::run_full_cycle_for_repo(
                        repo,
                        passphrase.as_deref().map(|s| s.as_str()),
                        Some(&cancel_requested),
                        &mut |event| match &event {
                            operations::CycleEvent::StepStarted(step) => {
                                let _ = ui_tx_progress.send(UiEvent::Status(format!(
                                    "[{rn}] {}...",
                                    step.command_name()
                                )));
                            }
                            operations::CycleEvent::Backup(evt) => {
                                if let Some(status) = tracker.format(evt) {
                                    let _ = ui_tx_progress.send(UiEvent::Status(status));
                                }
                            }
                            operations::CycleEvent::Check(evt) => {
                                let _ = ui_tx_progress
                                    .send(UiEvent::Status(format_check_status(&rn, evt)));
                            }
                            operations::CycleEvent::HookWarning { warning, .. } => {
                                send_log(
                                    &ui_tx_progress,
                                    format!("[{rn}] hook warning: {warning}"),
                                );
                            }
                            _ => {}
                        },
                    );

                    if let Some(ref report) = result.backup_report {
                        if !report.created.is_empty() {
                            any_snapshots_created = true;
                        }
                        log_backup_report(&ui_tx, &repo_name, report);
                    }
                    for (step, outcome) in &result.steps {
                        let msg = format_step_outcome(&repo_name, *step, outcome);
                        if !msg.is_empty() {
                            send_log(&ui_tx, msg);
                        }
                    }
                }

                if any_snapshots_created {
                    let _ = ui_tx.send(UiEvent::TriggerSnapshotRefresh);
                    let _ = app_tx.send(AppCommand::FetchAllRepoInfo);
                }

                finish_operation(&backup_running, &ui_tx, &sched_notify_tx);
            }
            AppCommand::RunBackupRepo { repo_name } => {
                let repo_name_sel = repo_name.trim().to_string();
                if repo_name_sel.is_empty() {
                    send_log(&ui_tx, "Select a repository first.");
                    continue;
                }

                let repo = match config::select_repo(&runtime.repos, &repo_name_sel) {
                    Some(r) => r,
                    None => {
                        send_log(&ui_tx, format!("No repository matching '{repo_name_sel}'."));
                        continue;
                    }
                };

                cancel_requested.store(false, Ordering::SeqCst);
                backup_running.store(true, Ordering::SeqCst);
                let _ = ui_tx.send(UiEvent::OperationStarted { cancellable: true });
                let rn = format_repo_name(repo);
                let _ = ui_tx.send(UiEvent::Status(format!("Running backup for [{rn}]...")));

                let passphrase = match get_or_resolve_passphrase(repo, &mut passphrases) {
                    Ok(p) => p,
                    Err(e) => {
                        send_log(&ui_tx, format!("[{rn}] passphrase error: {e}"));
                        finish_operation(&backup_running, &ui_tx, &sched_notify_tx);
                        continue;
                    }
                };

                if repo.config.encryption.mode != vykar_core::config::EncryptionModeConfig::None
                    && passphrase.is_none()
                {
                    send_log(
                        &ui_tx,
                        format!("[{rn}] passphrase prompt canceled; skipping."),
                    );
                    finish_operation(&backup_running, &ui_tx, &sched_notify_tx);
                    continue;
                }

                let mut tracker = BackupStatusTracker::new(rn.clone());
                let ui_tx_progress = ui_tx.clone();
                match operations::run_backup_selection(
                    repo,
                    &repo.sources,
                    passphrase.as_deref().map(|s| s.as_str()),
                    Some(&cancel_requested),
                    false,
                    Some(&mut |evt| match evt {
                        operations::BackupRunEvent::Backup(bpe) => {
                            if let Some(status) = tracker.format(&bpe) {
                                let _ = ui_tx_progress.send(UiEvent::Status(status));
                            }
                        }
                        operations::BackupRunEvent::HookWarning { warning, .. } => {
                            send_log(&ui_tx_progress, format!("[{rn}] hook warning: {warning}"));
                        }
                    }),
                ) {
                    Ok(report) => {
                        if !report.created.is_empty() {
                            let _ = app_tx.send(AppCommand::RefreshSnapshots {
                                repo_selector: rn.clone(),
                            });
                            let _ = app_tx.send(AppCommand::FetchAllRepoInfo);
                        }
                        log_backup_report(&ui_tx, &rn, &report);
                    }
                    Err(e) => send_log(&ui_tx, format!("[{rn}] backup failed: {e}")),
                }

                finish_operation(&backup_running, &ui_tx, &sched_notify_tx);
            }
            AppCommand::RunBackupSource { source_label } => {
                let source_label = source_label.trim().to_string();
                if source_label.is_empty() {
                    send_log(&ui_tx, "Select a source first.");
                    continue;
                }

                cancel_requested.store(false, Ordering::SeqCst);
                backup_running.store(true, Ordering::SeqCst);
                let _ = ui_tx.send(UiEvent::OperationStarted { cancellable: true });
                let _ = ui_tx.send(UiEvent::Status(format!(
                    "Running backup for source '{source_label}'..."
                )));

                let mut any_backed_up = false;
                let total = runtime.repos.len();
                for (i, repo) in runtime.repos.iter().enumerate() {
                    if cancel_requested.load(Ordering::SeqCst) {
                        send_log(&ui_tx, "Backup cancelled by user.");
                        break;
                    }

                    let matching_sources: Vec<config::SourceEntry> = repo
                        .sources
                        .iter()
                        .filter(|s| s.label == source_label)
                        .cloned()
                        .collect();

                    if matching_sources.is_empty() {
                        continue;
                    }

                    let repo_name = format_repo_name(repo);
                    let _ = ui_tx.send(UiEvent::Status(format!(
                        "Backing up [{}] ({}/{total})...",
                        repo_name,
                        i + 1
                    )));

                    let passphrase = match get_or_resolve_passphrase(repo, &mut passphrases) {
                        Ok(p) => p,
                        Err(e) => {
                            send_log(&ui_tx, format!("[{repo_name}] passphrase error: {e}"));
                            continue;
                        }
                    };

                    if repo.config.encryption.mode != vykar_core::config::EncryptionModeConfig::None
                        && passphrase.is_none()
                    {
                        send_log(
                            &ui_tx,
                            format!("[{repo_name}] passphrase prompt canceled; skipping."),
                        );
                        continue;
                    }

                    let mut tracker = BackupStatusTracker::new(repo_name.clone());
                    let ui_tx_progress = ui_tx.clone();
                    let rn_src = repo_name.clone();
                    match operations::run_backup_selection(
                        repo,
                        &matching_sources,
                        passphrase.as_deref().map(|s| s.as_str()),
                        Some(&cancel_requested),
                        false,
                        Some(&mut |evt| match evt {
                            operations::BackupRunEvent::Backup(bpe) => {
                                if let Some(status) = tracker.format(&bpe) {
                                    let _ = ui_tx_progress.send(UiEvent::Status(status));
                                }
                            }
                            operations::BackupRunEvent::HookWarning { warning, .. } => {
                                send_log(
                                    &ui_tx_progress,
                                    format!("[{rn_src}] hook warning: {warning}"),
                                );
                            }
                        }),
                    ) {
                        Ok(report) => {
                            if !report.created.is_empty() {
                                any_backed_up = true;
                            }
                            log_backup_report(&ui_tx, &repo_name, &report);
                        }
                        Err(e) => {
                            send_log(&ui_tx, format!("[{repo_name}] backup failed: {e}"));
                        }
                    }
                }

                if !any_backed_up {
                    send_log(
                        &ui_tx,
                        format!("No repositories found with source '{source_label}'."),
                    );
                } else {
                    let _ = ui_tx.send(UiEvent::TriggerSnapshotRefresh);
                    let _ = app_tx.send(AppCommand::FetchAllRepoInfo);
                }

                finish_operation(&backup_running, &ui_tx, &sched_notify_tx);
            }
            AppCommand::FetchAllRepoInfo => {
                let _ = ui_tx.send(UiEvent::Status("Fetching repository info...".to_string()));

                let mut items = Vec::new();
                let mut labels = Vec::new();

                let total = runtime.repos.len();
                for (i, repo) in runtime.repos.iter().enumerate() {
                    let repo_name = format_repo_name(repo);
                    let _ = ui_tx.send(UiEvent::Status(format!(
                        "Loading repo info: [{}] ({}/{total})...",
                        repo_name,
                        i + 1
                    )));
                    let url = repo.config.repository.url.clone();
                    let passphrase = match get_or_resolve_passphrase(repo, &mut passphrases) {
                        Ok(p) => p,
                        Err(e) => {
                            send_log(&ui_tx, format!("[{repo_name}] passphrase error: {e}"));
                            continue;
                        }
                    };

                    match vykar_core::commands::info::run(
                        &repo.config,
                        passphrase.as_deref().map(|s| s.as_str()),
                    ) {
                        Ok(stats) => {
                            let last_snapshot = stats
                                .last_snapshot_time
                                .map(|t| {
                                    let local: DateTime<Local> = t.with_timezone(&Local);
                                    local.format("%Y-%m-%d %H:%M:%S").to_string()
                                })
                                .unwrap_or_else(|| "N/A".to_string());

                            items.push(RepoInfoData {
                                name: repo_name.clone(),
                                url,
                                snapshots: stats.snapshot_count.to_string(),
                                last_snapshot,
                                size: format_bytes(stats.deduplicated_size),
                            });
                            labels.push(repo_name);
                        }
                        Err(e) => {
                            if matches!(e, VykarError::RepoNotFound(_)) {
                                let confirmed = tinyfiledialogs::message_box_yes_no(
                                    &format!("{APP_TITLE} — Repository Not Initialized"),
                                    &format!(
                                        "Repository {repo_name} at {url} is not initialized.\n\
                                         Would you like to initialize it now?",
                                    ),
                                    tinyfiledialogs::MessageBoxIcon::Question,
                                    tinyfiledialogs::YesNo::Yes,
                                );
                                if confirmed == tinyfiledialogs::YesNo::Yes {
                                    // Resolve passphrase for init following the canonical rule:
                                    // 1. encryption: none → None
                                    // 2. Configured source (passphrase field / passcommand)
                                    //    → reuse already-resolved value (no re-execution)
                                    // 3. Interactive GUI prompt with enter + confirm
                                    //
                                    // We only reuse the outer `passphrase` when it provably
                                    // came from a configured source. If it came from a single
                                    // interactive password_box (no confirmation), we must NOT
                                    // reuse it — init needs enter+confirm to avoid typos.
                                    // Note: VYKAR_PASSPHRASE env var is not checked here
                                    // because take_env_passphrase() removes it on first read,
                                    // making the probe unreliable in a GUI context.
                                    let has_configured_source =
                                        repo.config.encryption.passphrase.is_some()
                                            || repo.config.encryption.passcommand.is_some();
                                    let init_pass: Option<zeroize::Zeroizing<String>> =
                                        if repo.config.encryption.mode
                                            == vykar_core::config::EncryptionModeConfig::None
                                        {
                                            None
                                        } else if has_configured_source && passphrase.is_some() {
                                            passphrase.clone()
                                        } else {
                                            let title = format!(
                                                "{APP_TITLE} — New Passphrase ({repo_name})"
                                            );
                                            let p1 = tinyfiledialogs::password_box(
                                                &title,
                                                "Enter new passphrase:",
                                            );
                                            match p1.filter(|v| !v.is_empty()) {
                                                None => {
                                                    send_log(
                                                        &ui_tx,
                                                        format!(
                                                            "[{repo_name}] Init cancelled \
                                                             (no passphrase)."
                                                        ),
                                                    );
                                                    continue;
                                                }
                                                Some(p1_val) => {
                                                    let p2 = tinyfiledialogs::password_box(
                                                        &format!(
                                                            "{APP_TITLE} — Confirm Passphrase \
                                                             ({repo_name})"
                                                        ),
                                                        "Confirm passphrase:",
                                                    );
                                                    match p2 {
                                                        Some(ref p2_val) if p2_val == &p1_val => {
                                                            Some(zeroize::Zeroizing::new(p1_val))
                                                        }
                                                        _ => {
                                                            send_log(
                                                                &ui_tx,
                                                                format!(
                                                                    "[{repo_name}] Passphrases \
                                                                     do not match."
                                                                ),
                                                            );
                                                            continue;
                                                        }
                                                    }
                                                }
                                            }
                                        };

                                    let retry_pass = init_pass.clone();
                                    match init::run(
                                        &repo.config,
                                        init_pass.as_deref().map(|s| s.as_str()),
                                    ) {
                                        Ok(_) => {
                                            send_log(
                                                &ui_tx,
                                                format!("[{repo_name}] Repository initialized."),
                                            );
                                            if let Some(p) = init_pass {
                                                passphrases
                                                    .insert(repo.config.repository.url.clone(), p);
                                            }
                                        }
                                        Err(VykarError::RepoAlreadyExists(_)) => {
                                            send_log(
                                                &ui_tx,
                                                format!(
                                                    "[{repo_name}] Repository was initialized \
                                                     concurrently."
                                                ),
                                            );
                                        }
                                        Err(init_err) => {
                                            send_log(
                                                &ui_tx,
                                                format!("[{repo_name}] init failed: {init_err}"),
                                            );
                                            continue;
                                        }
                                    }

                                    // Retry info with the init passphrase to populate the repo card
                                    if let Ok(stats) = vykar_core::commands::info::run(
                                        &repo.config,
                                        retry_pass.as_deref().map(|s| s.as_str()),
                                    ) {
                                        let last_snapshot = stats
                                            .last_snapshot_time
                                            .map(|t| {
                                                let local: DateTime<Local> =
                                                    t.with_timezone(&Local);
                                                local.format("%Y-%m-%d %H:%M:%S").to_string()
                                            })
                                            .unwrap_or_else(|| "N/A".to_string());
                                        items.push(RepoInfoData {
                                            name: repo_name.clone(),
                                            url: url.clone(),
                                            snapshots: stats.snapshot_count.to_string(),
                                            last_snapshot,
                                            size: format_bytes(stats.deduplicated_size),
                                        });
                                        labels.push(repo_name);
                                    }
                                } else {
                                    send_log(
                                        &ui_tx,
                                        format!("[{repo_name}] Repository initialization skipped."),
                                    );
                                }
                            } else {
                                send_log(&ui_tx, format!("[{repo_name}] info failed: {e}"));
                            }
                        }
                    }
                }

                let _ = ui_tx.send(UiEvent::RepoModelData { items, labels });
                let _ = ui_tx.send(UiEvent::Status("Idle".to_string()));
            }
            AppCommand::RefreshSnapshots { repo_selector } => {
                let _ = ui_tx.send(UiEvent::Status("Loading snapshots...".to_string()));

                let repos_to_scan = match select_repos(&runtime.repos, &repo_selector) {
                    Ok(repos) => repos,
                    Err(e) => {
                        send_log(&ui_tx, format!("Failed to select repository: {e}"));
                        let _ = ui_tx.send(UiEvent::Status("Idle".to_string()));
                        continue;
                    }
                };

                let mut data = Vec::new();

                for repo in repos_to_scan {
                    let repo_name = format_repo_name(repo);
                    let passphrase = match get_or_resolve_passphrase(repo, &mut passphrases) {
                        Ok(pass) => pass,
                        Err(e) => {
                            send_log(&ui_tx, format!("[{repo_name}] passphrase error: {e}"));
                            continue;
                        }
                    };

                    match operations::list_snapshots_with_stats(
                        &repo.config,
                        passphrase.as_deref().map(|s| s.as_str()),
                    ) {
                        Ok(mut snapshots) => {
                            snapshots.sort_by_key(|(s, _)| s.time);
                            for (s, stats) in snapshots {
                                let ts: DateTime<Local> = s.time.with_timezone(&Local);
                                let sources = if s.source_paths.is_empty() {
                                    if s.source_label.is_empty() {
                                        "-".to_string()
                                    } else {
                                        s.source_label.clone()
                                    }
                                } else {
                                    s.source_paths.join("\n")
                                };
                                let label = if s.source_label.is_empty() {
                                    "-".to_string()
                                } else {
                                    s.source_label.clone()
                                };
                                let hostname = if s.hostname.is_empty() {
                                    "-".to_string()
                                } else {
                                    s.hostname.clone()
                                };
                                let (files, size, nfiles, size_bytes) = match stats {
                                    Some(st) => (
                                        format_count(st.nfiles),
                                        format_bytes(st.deduplicated_size),
                                        Some(st.nfiles),
                                        Some(st.deduplicated_size),
                                    ),
                                    None => ("-".to_string(), "-".to_string(), None, None),
                                };
                                data.push(SnapshotRowData {
                                    id: s.name.clone(),
                                    hostname,
                                    time_str: ts.format("%Y-%m-%d %H:%M:%S").to_string(),
                                    source: sources,
                                    label,
                                    files,
                                    size,
                                    nfiles,
                                    size_bytes,
                                    time_epoch: s.time.timestamp(),
                                    repo_name: repo_name.clone(),
                                });
                            }
                        }
                        Err(e) => {
                            send_log(
                                &ui_tx,
                                format!("[{repo_name}] snapshot listing failed: {e}"),
                            );
                        }
                    }
                }

                let _ = ui_tx.send(UiEvent::SnapshotTableData { data });
                let _ = ui_tx.send(UiEvent::Status("Idle".to_string()));
            }
            AppCommand::FetchSnapshotContents {
                repo_name,
                snapshot_name,
            } => {
                let _ = ui_tx.send(UiEvent::Status("Loading snapshot contents...".to_string()));

                match find_repo_for_snapshot(
                    &runtime.repos,
                    &repo_name,
                    &snapshot_name,
                    &mut passphrases,
                ) {
                    Ok((repo, passphrase)) => {
                        match operations::list_snapshot_items(
                            &repo.config,
                            passphrase.as_deref().map(|s| s.as_str()),
                            &snapshot_name,
                        ) {
                            Ok(items) => {
                                send_log(
                                    &ui_tx,
                                    format!(
                                        "Loaded {} item(s) from snapshot {} in [{}]",
                                        items.len(),
                                        snapshot_name,
                                        format_repo_name(repo)
                                    ),
                                );

                                let _ = ui_tx.send(UiEvent::SnapshotContentsData {
                                    repo_name: repo_name.clone(),
                                    snapshot_name: snapshot_name.clone(),
                                    items,
                                });
                            }
                            Err(e) => {
                                send_log(&ui_tx, format!("Failed to load snapshot items: {e}"));
                            }
                        }
                    }
                    Err(e) => {
                        send_log(&ui_tx, format!("Failed to resolve snapshot: {e}"));
                    }
                }

                let _ = ui_tx.send(UiEvent::Status("Idle".to_string()));
            }
            AppCommand::RestoreSelected {
                repo_name,
                snapshot,
                dest,
                paths,
            } => {
                cancel_requested.store(false, Ordering::SeqCst);
                let _ = ui_tx.send(UiEvent::OperationStarted { cancellable: false });
                let _ = ui_tx.send(UiEvent::Status("Restoring selected items...".to_string()));

                match find_repo_for_snapshot(
                    &runtime.repos,
                    &repo_name,
                    &snapshot,
                    &mut passphrases,
                ) {
                    Ok((repo, passphrase)) => {
                        let path_set: std::collections::HashSet<String> =
                            paths.into_iter().collect();
                        match operations::restore_selected(
                            &repo.config,
                            passphrase.as_deref().map(|s| s.as_str()),
                            &snapshot,
                            &dest,
                            &path_set,
                        ) {
                            Ok(stats) => {
                                send_log(
                                    &ui_tx,
                                    format!(
                                        "Restored selected items from {} -> {} (files={}, dirs={}, symlinks={}, bytes={})",
                                        snapshot,
                                        dest,
                                        stats.files,
                                        stats.dirs,
                                        stats.symlinks,
                                        format_bytes(stats.total_bytes),
                                    ),
                                );
                                let _ = ui_tx.send(UiEvent::RestoreFinished {
                                    success: true,
                                    message: format!(
                                        "files={}, dirs={}, symlinks={}, bytes={}",
                                        stats.files,
                                        stats.dirs,
                                        stats.symlinks,
                                        format_bytes(stats.total_bytes),
                                    ),
                                });
                            }
                            Err(e) => {
                                send_log(&ui_tx, format!("Restore failed: {e}"));
                                let _ = ui_tx.send(UiEvent::RestoreFinished {
                                    success: false,
                                    message: format!("{e}"),
                                });
                            }
                        }
                    }
                    Err(e) => {
                        send_log(&ui_tx, format!("Failed to resolve snapshot: {e}"));
                        let _ = ui_tx.send(UiEvent::RestoreFinished {
                            success: false,
                            message: format!("{e}"),
                        });
                    }
                }

                let _ = ui_tx.send(UiEvent::OperationFinished);
                let _ = ui_tx.send(UiEvent::Status("Idle".to_string()));
            }
            AppCommand::DeleteSnapshot {
                repo_name,
                snapshot_name,
            } => {
                // Confirm with user
                let confirmed = tinyfiledialogs::message_box_yes_no(
                    "Delete Snapshot",
                    &format!(
                        "Are you sure you want to delete snapshot {snapshot_name} from {repo_name}?"
                    ),
                    tinyfiledialogs::MessageBoxIcon::Question,
                    tinyfiledialogs::YesNo::No,
                );

                if confirmed == tinyfiledialogs::YesNo::No {
                    send_log(&ui_tx, "Snapshot deletion cancelled.");
                    continue;
                }

                cancel_requested.store(false, Ordering::SeqCst);
                let _ = ui_tx.send(UiEvent::OperationStarted { cancellable: false });
                let _ = ui_tx.send(UiEvent::Status("Deleting snapshot...".to_string()));

                let repo = match config::select_repo(&runtime.repos, &repo_name) {
                    Some(r) => r,
                    None => {
                        send_log(&ui_tx, format!("No repository matching '{repo_name}'."));
                        let _ = ui_tx.send(UiEvent::OperationFinished);
                        let _ = ui_tx.send(UiEvent::Status("Idle".to_string()));
                        continue;
                    }
                };

                let passphrase = match get_or_resolve_passphrase(repo, &mut passphrases) {
                    Ok(p) => p,
                    Err(e) => {
                        send_log(&ui_tx, format!("[{repo_name}] passphrase error: {e}"));
                        let _ = ui_tx.send(UiEvent::OperationFinished);
                        let _ = ui_tx.send(UiEvent::Status("Idle".to_string()));
                        continue;
                    }
                };

                match operations::delete_snapshot(
                    &repo.config,
                    passphrase.as_deref().map(|s| s.as_str()),
                    &snapshot_name,
                ) {
                    Ok(stats) => {
                        send_log(
                            &ui_tx,
                            format!(
                                "[{repo_name}] Deleted snapshot '{}': {} chunks freed, {} reclaimed",
                                stats.snapshot_name,
                                stats.chunks_deleted,
                                format_bytes(stats.space_freed),
                            ),
                        );
                        // Auto-refresh snapshots and repo details
                        let _ = app_tx.send(AppCommand::RefreshSnapshots {
                            repo_selector: repo_name,
                        });
                        let _ = app_tx.send(AppCommand::FetchAllRepoInfo);
                    }
                    Err(e) => {
                        send_log(&ui_tx, format!("[{repo_name}] delete failed: {e}"));
                    }
                }
                let _ = ui_tx.send(UiEvent::OperationFinished);
                let _ = ui_tx.send(UiEvent::Status("Idle".to_string()));
            }
            AppCommand::FindFiles {
                repo_name,
                name_pattern,
            } => {
                cancel_requested.store(false, Ordering::SeqCst);
                let _ = ui_tx.send(UiEvent::OperationStarted { cancellable: false });
                let _ = ui_tx.send(UiEvent::Status("Searching files...".to_string()));

                let repo = match config::select_repo(&runtime.repos, &repo_name) {
                    Some(r) => r,
                    None => {
                        send_log(&ui_tx, format!("No repository matching '{repo_name}'."));
                        let _ = ui_tx.send(UiEvent::OperationFinished);
                        let _ = ui_tx.send(UiEvent::Status("Idle".to_string()));
                        continue;
                    }
                };

                let passphrase = match get_or_resolve_passphrase(repo, &mut passphrases) {
                    Ok(p) => p,
                    Err(e) => {
                        send_log(&ui_tx, format!("[{repo_name}] passphrase error: {e}"));
                        let _ = ui_tx.send(UiEvent::OperationFinished);
                        let _ = ui_tx.send(UiEvent::Status("Idle".to_string()));
                        continue;
                    }
                };

                let filter = match FindFilter::build(
                    None,
                    Some(&name_pattern),
                    None,
                    None,
                    None,
                    None,
                    None,
                ) {
                    Ok(f) => f,
                    Err(e) => {
                        send_log(&ui_tx, format!("Invalid name pattern: {e}"));
                        let _ = ui_tx.send(UiEvent::OperationFinished);
                        let _ = ui_tx.send(UiEvent::Status("Idle".to_string()));
                        continue;
                    }
                };

                let scope = FindScope {
                    source_label: None,
                    last_n: None,
                };

                match vykar_core::commands::find::run(
                    &repo.config,
                    passphrase.as_deref().map(|s| s.as_str()),
                    &scope,
                    &filter,
                ) {
                    Ok(timelines) => {
                        let mut rows = Vec::new();
                        for tl in &timelines {
                            for ah in &tl.hits {
                                let ts: DateTime<Local> =
                                    ah.hit.snapshot_time.with_timezone(&Local);
                                rows.push(FindResultRow {
                                    path: tl.path.clone(),
                                    snapshot: ah.hit.snapshot_name.clone(),
                                    date: ts.format("%Y-%m-%d %H:%M:%S").to_string(),
                                    size: format_bytes(ah.hit.size),
                                    status: match ah.status {
                                        FileStatus::Added => "Added".to_string(),
                                        FileStatus::Modified => "Modified".to_string(),
                                        FileStatus::Unchanged => "Unchanged".to_string(),
                                    },
                                });
                            }
                        }
                        send_log(
                            &ui_tx,
                            format!(
                                "[{repo_name}] Find '{}': {} paths, {} total hits",
                                name_pattern,
                                timelines.len(),
                                rows.len(),
                            ),
                        );
                        let _ = ui_tx.send(UiEvent::FindResultsData { rows });
                    }
                    Err(e) => {
                        send_log(&ui_tx, format!("[{repo_name}] find failed: {e}"));
                    }
                }

                let _ = ui_tx.send(UiEvent::OperationFinished);
                let _ = ui_tx.send(UiEvent::Status("Idle".to_string()));
            }
            AppCommand::OpenConfigFile => {
                let path = runtime.source.path().display().to_string();
                send_log(&ui_tx, format!("Opening config file: {path}"));
                let _ = std::process::Command::new("open").arg(&path).spawn();
            }
            AppCommand::ReloadConfig => {
                let config_path = dunce::canonicalize(runtime.source.path())
                    .unwrap_or_else(|_| runtime.source.path().to_path_buf());
                config_helpers::apply_config(
                    config_path,
                    false,
                    &mut runtime,
                    &mut config_display_path,
                    &mut passphrases,
                    &scheduler,
                    schedule_paused,
                    scheduler_lock_held,
                    &ui_tx,
                    &app_tx,
                    &sched_notify_tx,
                );
            }
            AppCommand::SwitchConfig => {
                let picked = tinyfiledialogs::open_file_dialog(
                    "Open vykar config",
                    "",
                    Some((&["*.yaml", "*.yml"], "YAML files")),
                );
                if let Some(path_str) = picked {
                    config_helpers::apply_config(
                        PathBuf::from(path_str),
                        true,
                        &mut runtime,
                        &mut config_display_path,
                        &mut passphrases,
                        &scheduler,
                        schedule_paused,
                        scheduler_lock_held,
                        &ui_tx,
                        &app_tx,
                        &sched_notify_tx,
                    );
                }
            }
            AppCommand::SaveAndApplyConfig { yaml_text } => {
                let config_path = config_display_path.clone();
                let tmp_path = config_path.with_extension("yaml.tmp");
                if let Err(e) = std::fs::write(&tmp_path, &yaml_text) {
                    let _ = ui_tx.send(UiEvent::ConfigSaveError(format!("Write failed: {e}")));
                    continue;
                }

                if let Err(msg) = config_helpers::validate_config(&tmp_path) {
                    let _ = std::fs::remove_file(&tmp_path);
                    let _ = ui_tx.send(UiEvent::ConfigSaveError(msg));
                    continue;
                }

                if let Err(e) = std::fs::rename(&tmp_path, &config_path) {
                    let _ = std::fs::remove_file(&tmp_path);
                    let _ = ui_tx.send(UiEvent::ConfigSaveError(format!("Rename failed: {e}")));
                    continue;
                }

                // apply_config re-runs validate_config internally, which is
                // redundant but harmless — it keeps the function self-contained.
                if config_helpers::apply_config(
                    config_path,
                    false,
                    &mut runtime,
                    &mut config_display_path,
                    &mut passphrases,
                    &scheduler,
                    schedule_paused,
                    scheduler_lock_held,
                    &ui_tx,
                    &app_tx,
                    &sched_notify_tx,
                ) {
                    send_log(&ui_tx, "Configuration saved and applied.");
                } else {
                    let _ = ui_tx.send(UiEvent::ConfigSaveError(
                        "Config saved to disk but failed to apply. Check log for details.".into(),
                    ));
                }
            }
            AppCommand::ShowWindow => {
                let _ = ui_tx.send(UiEvent::ShowWindow);
            }
            AppCommand::Quit => {
                let _ = ui_tx.send(UiEvent::Quit);
                break;
            }
        }
    }
}
