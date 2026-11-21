use std::collections::HashMap;
use std::sync::atomic::Ordering;

use crossbeam_channel::Sender;
use vykar_core::app::operations;
use vykar_core::app::preflight::UnavailableRepo;
use vykar_core::commands::backup::BackupProgressEvent;
use vykar_core::config;
use vykar_types::error::VykarError;
use zeroize::Zeroizing;

use crate::messages::{AppCommand, UiEvent};
use crate::progress::{BackupStatusTracker, format_check_status, format_step_outcome};
use crate::repo_helpers::{
    InitTarget, PassphraseRun, classify_init_target, log_backup_report, send_log,
    with_passphrase_retry,
};

use super::WorkerContext;
use super::shared::{OpGuard, run_selection_with_progress};

pub(super) fn handle_backup_all(ctx: &mut WorkerContext, scheduled: bool) {
    run_backup_cycle(ctx, scheduled, None);
}

/// Scheduled cycle for the repositories whose per-repo slot came due.
pub(super) fn handle_backup_repos(ctx: &mut WorkerContext, repo_names: Vec<String>) {
    run_backup_cycle(ctx, true, Some(&repo_names));
}

/// Indices of the repositories a cycle should cover: all of them, or just the
/// ones named in `only` (per-repo schedules). Names are matched as produced by
/// `ResolvedRepo::label_or_url`. An empty result for a non-empty `only` means the config
/// changed underneath the scheduler.
fn select_repo_indices(repos: &[config::ResolvedRepo], only: Option<&[String]>) -> Vec<usize> {
    match only {
        Some(names) => repos
            .iter()
            .enumerate()
            .filter(|(_, r)| names.contains(&r.label_or_url().to_string()))
            .map(|(idx, _)| idx)
            .collect(),
        None => (0..repos.len()).collect(),
    }
}

/// Passphrase step of a cycle, run after the `before` hooks pass so a skip hook
/// sees neither a dialog nor a connection. Validates and caches the passphrase
/// (retrying a wrong dialog entry; a wrong value is never cached).
/// The cycle buries decryption failures inside `steps` as a string, so it cannot
/// drive `with_passphrase_retry` directly — the `info::run` probe does.
fn resolve_cycle_passphrase(
    repo: &config::ResolvedRepo,
    passphrases: &mut HashMap<String, Zeroizing<String>>,
    ui_tx: &Sender<UiEvent>,
) -> Result<Option<Zeroizing<String>>, VykarError> {
    let outcome = with_passphrase_retry(repo, passphrases, 3, |pass| {
        vykar_core::commands::info::run(&repo.config, pass)
    });
    match outcome {
        Ok(PassphraseRun::Ran(_)) => Ok(passphrases.get(&repo.config.repository.url).cloned()),
        Ok(PassphraseRun::Canceled) => {
            Err(VykarError::Skipped("passphrase prompt canceled".into()))
        }
        Err(VykarError::RepoNotFound(_)) => {
            let (log, reason) =
                if classify_init_target(&repo.config.repository.url) == InitTarget::Unavailable {
                    (
                        "repository path not available (drive not connected?)",
                        "repository not available",
                    )
                } else {
                    ("repository not initialized", "repository not initialized")
                };
            send_log(ui_tx, format!("[{}] {log}", repo.label_or_url()));
            Err(VykarError::Other(reason.to_string()))
        }
        Err(e) => Err(VykarError::Other(format!("failed to open repository: {e}"))),
    }
}

/// Run a full cycle over the configured repositories, optionally restricted to
/// `only` (per-repo schedules). Names are matched as produced by
/// `ResolvedRepo::label_or_url`.
fn run_backup_cycle(ctx: &mut WorkerContext, scheduled: bool, only: Option<&[String]>) {
    let status = if scheduled {
        "Running scheduled backup cycle..."
    } else {
        "Running backup cycle..."
    };
    let mut guard = OpGuard::backup(
        &ctx.ui_tx,
        &ctx.cancel_requested,
        &ctx.operation_running,
        &ctx.backup_running,
        &ctx.sched_notify_tx,
        status,
    );

    let selected = select_repo_indices(&ctx.runtime.repos, only);

    // A config reload between the scheduler firing and this handler running can
    // rename or remove a repo. Say so rather than silently doing nothing.
    if selected.is_empty() {
        if let Some(names) = only {
            send_log(
                &ctx.ui_tx,
                format!(
                    "Scheduled backup skipped: no repository matches {} (config changed?).",
                    names.join(", ")
                ),
            );
        }
        return;
    }

    let mut any_snapshots_created = false;
    // Per-repo failures, aggregated into a single `guard.fail` after the loop so
    // a partial failure shows one red status naming the failed repos (rather than
    // a per-iteration flash that a later success would visually overwrite).
    let mut failures: Vec<(String, String)> = Vec::new();
    let total = selected.len();
    for (i, &repo_idx) in selected.iter().enumerate() {
        if ctx.cancel_requested.load(Ordering::SeqCst) {
            send_log(&ctx.ui_tx, "Backup cancelled by user.");
            break;
        }

        let Some(repo) = ctx.runtime.repos.get(repo_idx) else {
            continue;
        };
        let repo_name = repo.label_or_url().to_string();
        let _ = ctx.ui_tx.send(UiEvent::Status(format!(
            "[{}] ({}/{total})...",
            repo_name,
            i + 1
        )));

        let mut tracker = BackupStatusTracker::new(repo_name.clone());
        let ui_tx_progress = ctx.ui_tx.clone();
        let rn = repo_name.clone();
        let passphrases = &mut ctx.passphrases;
        let ui_tx = &ctx.ui_tx;
        let result = operations::run_full_cycle_with_passphrase_source(
            repo,
            &mut || resolve_cycle_passphrase(repo, passphrases, ui_tx),
            Some(&ctx.cancel_requested),
            false,
            &[],
            UnavailableRepo::Skip,
            &mut |event| match &event {
                operations::CycleEvent::StepStarted(step) => {
                    let _ = ui_tx_progress.send(UiEvent::Status(format!(
                        "[{rn}] {}...",
                        step.command_name()
                    )));
                }
                operations::CycleEvent::Backup(BackupProgressEvent::Warning { message }) => {
                    send_log(&ui_tx_progress, format!("[{rn}] warning: {message}"));
                }
                operations::CycleEvent::Backup(evt) => {
                    if let Some(status) = tracker.format(evt) {
                        let _ = ui_tx_progress.send(UiEvent::Status(status));
                    }
                }
                operations::CycleEvent::Check(evt) => {
                    let _ = ui_tx_progress.send(UiEvent::Status(format_check_status(&rn, evt)));
                }
                operations::CycleEvent::HookWarning { warning, .. } => {
                    send_log(&ui_tx_progress, format!("[{rn}] hook warning: {warning}"));
                }
                operations::CycleEvent::StepWarning { step, message } => {
                    send_log(
                        &ui_tx_progress,
                        format!("[{rn}] {} warning: {message}", step.command_name()),
                    );
                }
                _ => {}
            },
        );

        if let Some(ref report) = result.backup_report {
            if !report.created.is_empty() {
                any_snapshots_created = true;
            }
            log_backup_report(&ctx.ui_tx, &repo_name, report);
        }
        for (step, outcome) in &result.steps {
            let msg = format_step_outcome(&repo_name, *step, outcome);
            if !msg.is_empty() {
                send_log(&ctx.ui_tx, msg);
            }
        }

        if result.has_failures() {
            let reason = result
                .steps
                .iter()
                .filter_map(|(step, o)| match o {
                    operations::StepOutcome::Failed(e) => {
                        Some(format!("{} failed: {e}", step.command_name()))
                    }
                    _ => None,
                })
                .collect::<Vec<_>>()
                .join(", ");
            failures.push((repo_name.clone(), reason));
        }
    }

    if !failures.is_empty() {
        let detail = failures
            .iter()
            .map(|(name, reason)| format!("[{name}] {reason}"))
            .collect::<Vec<_>>()
            .join("; ");
        guard.fail(format!(
            "Backup failed for {} of {total} repositories: {detail}",
            failures.len()
        ));
    }

    if any_snapshots_created {
        let _ = ctx.ui_tx.send(UiEvent::TriggerSnapshotRefresh);
        let _ = ctx.app_tx.send(AppCommand::FetchAllRepoInfo);
    }
}

pub(super) fn handle_backup_repo(ctx: &mut WorkerContext, repo_name: String) {
    let repo_name_sel = repo_name.trim().to_string();
    if repo_name_sel.is_empty() {
        send_log(&ctx.ui_tx, "Select a repository first.");
        return;
    }

    let repo = match config::select_repo(&ctx.runtime.repos, &repo_name_sel) {
        Some(r) => r,
        None => {
            send_log(
                &ctx.ui_tx,
                format!("No repository matching '{repo_name_sel}'."),
            );
            return;
        }
    };

    let rn = repo.label_or_url().to_string();
    let mut guard = OpGuard::backup(
        &ctx.ui_tx,
        &ctx.cancel_requested,
        &ctx.operation_running,
        &ctx.backup_running,
        &ctx.sched_notify_tx,
        format!("Running backup for [{rn}]..."),
    );

    let outcome = with_passphrase_retry(repo, &mut ctx.passphrases, 3, |pass| {
        run_selection_with_progress(&ctx.ui_tx, &ctx.cancel_requested, repo, &repo.sources, pass)
    });

    match outcome {
        Ok(PassphraseRun::Ran(report)) => {
            if !report.created.is_empty() {
                let _ = ctx.app_tx.send(AppCommand::RefreshSnapshots {
                    repo_selector: rn.clone(),
                });
                let _ = ctx.app_tx.send(AppCommand::FetchAllRepoInfo);
            }
            log_backup_report(&ctx.ui_tx, &rn, &report);
            if let Some(summary) = report.failure_summary() {
                guard.fail(format!("[{rn}] backup failed: {summary}"));
            }
        }
        Ok(PassphraseRun::Canceled) => {
            send_log(
                &ctx.ui_tx,
                format!("[{rn}] passphrase prompt canceled; skipping."),
            );
        }
        Err(VykarError::Skipped(reason)) => {
            send_log(&ctx.ui_tx, format!("[{rn}] backup skipped: {reason}"));
        }
        Err(e) => {
            guard.fail(format!("[{rn}] backup failed: {e}"));
        }
    }
}

pub(super) fn handle_backup_source(ctx: &mut WorkerContext, source_label: String) {
    let source_label = source_label.trim().to_string();
    if source_label.is_empty() {
        send_log(&ctx.ui_tx, "Select a source first.");
        return;
    }

    let mut guard = OpGuard::backup(
        &ctx.ui_tx,
        &ctx.cancel_requested,
        &ctx.operation_running,
        &ctx.backup_running,
        &ctx.sched_notify_tx,
        format!("Running backup for source '{source_label}'..."),
    );

    let mut any_backed_up = false;
    // Repos that actually carry the source (attempted), and per-repo failures
    // aggregated into a single `guard.fail` after the loop — mirrors
    // `handle_backup_all` so a partial failure shows one red status naming the
    // failed repos rather than a misleading all-red state.
    let mut attempted = 0usize;
    let mut failures: Vec<(String, String)> = Vec::new();
    let total = ctx.runtime.repos.len();
    for (i, repo) in ctx.runtime.repos.iter().enumerate() {
        if ctx.cancel_requested.load(Ordering::SeqCst) {
            send_log(&ctx.ui_tx, "Backup cancelled by user.");
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
        attempted += 1;

        let repo_name = repo.label_or_url().to_string();
        let _ = ctx.ui_tx.send(UiEvent::Status(format!(
            "Backing up [{}] ({}/{total})...",
            repo_name,
            i + 1
        )));

        let outcome = with_passphrase_retry(repo, &mut ctx.passphrases, 3, |pass| {
            run_selection_with_progress(
                &ctx.ui_tx,
                &ctx.cancel_requested,
                repo,
                &matching_sources,
                pass,
            )
        });

        match outcome {
            Ok(PassphraseRun::Ran(report)) => {
                if !report.created.is_empty() {
                    any_backed_up = true;
                }
                log_backup_report(&ctx.ui_tx, &repo_name, &report);
                if let Some(summary) = report.failure_summary() {
                    failures.push((repo_name.clone(), summary));
                }
            }
            Ok(PassphraseRun::Canceled) => {
                send_log(
                    &ctx.ui_tx,
                    format!("[{repo_name}] passphrase prompt canceled; skipping."),
                );
            }
            Err(VykarError::Skipped(reason)) => {
                send_log(
                    &ctx.ui_tx,
                    format!("[{repo_name}] backup skipped: {reason}"),
                );
            }
            Err(e) => {
                send_log(&ctx.ui_tx, format!("[{repo_name}] backup failed: {e}"));
                failures.push((repo_name.clone(), format!("{e}")));
            }
        }
    }

    if !failures.is_empty() {
        let detail = failures
            .iter()
            .map(|(name, reason)| format!("[{name}] {reason}"))
            .collect::<Vec<_>>()
            .join("; ");
        guard.fail(format!(
            "Backup failed for {} of {attempted} repositories with source '{source_label}': {detail}",
            failures.len()
        ));
    }

    if any_backed_up {
        let _ = ctx.ui_tx.send(UiEvent::TriggerSnapshotRefresh);
        let _ = ctx.app_tx.send(AppCommand::FetchAllRepoInfo);
    } else if attempted == 0 {
        send_log(
            &ctx.ui_tx,
            format!("No repositories found with source '{source_label}'."),
        );
    }
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use super::*;
    use crate::worker::test_support::{self, Harness, drain};

    fn two_repos() -> Vec<config::ResolvedRepo> {
        let yaml = "repositories:\n  \
                      - url: /backups/nas\n    label: nas\n  \
                      - url: /backups/remote\n\
                    encryption:\n  mode: none\nsources: []\n";
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("config.yaml");
        std::fs::write(&path, yaml).unwrap();
        config::load_and_resolve(&path).unwrap()
    }

    #[test]
    fn no_filter_selects_every_repo() {
        let repos = two_repos();
        assert_eq!(select_repo_indices(&repos, None), vec![0, 1]);
    }

    #[test]
    fn filter_selects_only_named_repos() {
        let repos = two_repos();
        // Labelled repos match on label, unlabelled ones on URL.
        assert_eq!(
            select_repo_indices(&repos, Some(&["nas".to_string()])),
            vec![0]
        );
        assert_eq!(
            select_repo_indices(&repos, Some(&["/backups/remote".to_string()])),
            vec![1]
        );
    }

    /// A worker whose only repo does not exist, so anything past the `before`
    /// hook would surface as an error (in a cycle, as an unavailable-repo skip).
    fn harness(before_backup: &str) -> Harness {
        test_support::harness(|tmp| {
            let repo = tmp.join("volume").join("repo");
            let source = tmp.join("source");
            std::fs::create_dir_all(&source).unwrap();
            format!(
                "repositories:\n  - url: '{}'\n    label: nas\n\
                 encryption:\n  mode: none\n\
                 cache_dir: '{}'\n\
                 hooks:\n  before_backup: {before_backup}\n\
                 sources:\n  - path: '{}'\n    label: src\n",
                repo.display(),
                tmp.join("cache").display(),
                source.display(),
            )
        })
    }

    type Handler = fn(&mut WorkerContext);

    const HANDLERS: [(&str, Handler); 3] = [
        ("cycle", |ctx| handle_backup_all(ctx, true)),
        ("repo", |ctx| handle_backup_repo(ctx, "nas".to_string())),
        ("source", |ctx| handle_backup_source(ctx, "src".to_string())),
    ];

    #[test]
    fn hook_skip_is_logged_and_not_a_failure() {
        for (name, handler) in HANDLERS {
            let mut h = harness("exit 75");
            handler(&mut h.ctx);
            let (logs, errors) = drain(&h.ui_rx);

            assert!(errors.is_empty(), "{name}: {errors:?}");
            assert!(
                logs.iter().any(|l| l.contains("requested skip")),
                "{name}: {logs:?}"
            );
            assert!(
                !logs.iter().any(|l| l.contains("No repositories found")),
                "{name}: {logs:?}"
            );
            assert!(
                !logs.iter().any(|l| l.contains("FAILED")),
                "{name}: {logs:?}"
            );
        }
    }

    /// Guards the test above: the same setup does go red without the skip code.
    #[test]
    fn other_hook_exit_codes_are_still_failures() {
        for (name, handler) in HANDLERS {
            let mut h = harness("exit 3");
            handler(&mut h.ctx);
            let (_, errors) = drain(&h.ui_rx);
            assert_eq!(errors.len(), 1, "{name}: {errors:?}");
        }
    }

    const FIXTURE_HOOKS: [&str; 4] = [
        "before_backup",
        "after_backup",
        "failed_backup",
        "finally_backup",
    ];

    /// YAML single-quoted scalar.
    fn quoted(value: &str) -> String {
        format!("'{}'", value.replace('\'', "''"))
    }

    /// The shared unavailable-repository fixture: `nas` at `<tmp>/volume/repo`
    /// (not created), with every backup hook appending its name to
    /// `<tmp>/hooks.log`. `before_backup` replaces that hook's command.
    fn unavailable_harness(before_backup: Option<&dyn Fn(&Path) -> String>) -> Harness {
        test_support::harness(|tmp| {
            let source = tmp.join("source");
            std::fs::create_dir_all(&source).unwrap();
            std::fs::write(source.join("a.txt"), b"data").unwrap();
            let log = tmp.join("hooks.log");
            let hooks = FIXTURE_HOOKS
                .iter()
                .map(|name| {
                    let cmd = match before_backup {
                        Some(build) if *name == "before_backup" => build(tmp),
                        _ => test_support::cmd_append(&log, name),
                    };
                    format!("  {name}: {}\n", quoted(&cmd))
                })
                .collect::<Vec<_>>()
                .concat();
            format!(
                "repositories:\n  - url: {}\n    label: nas\n\
                 encryption:\n  mode: none\n\
                 cache_dir: {}\n\
                 hooks:\n{hooks}\
                 sources:\n  - path: {}\n    label: src\n",
                quoted(&tmp.join("volume").join("repo").display().to_string()),
                quoted(&tmp.join("cache").display().to_string()),
                quoted(&source.display().to_string()),
            )
        })
    }

    fn hook_log(h: &Harness) -> Vec<String> {
        std::fs::read_to_string(h.tmp.path().join("hooks.log"))
            .unwrap_or_default()
            .lines()
            .map(|l| l.trim().to_string())
            .collect()
    }

    /// An unavailable repository in a cycle is skipped after its `before`
    /// hooks, without a red status.
    #[test]
    fn unavailable_repo_is_skipped_after_hooks() {
        let mut h = unavailable_harness(None);
        handle_backup_all(&mut h.ctx, true);
        let (logs, errors) = drain(&h.ui_rx);

        assert!(errors.is_empty(), "{errors:?}");
        assert_eq!(
            logs.iter()
                .filter(
                    |l| l.contains("[nas] backup: skipped (repository not found at")
                        && l.contains("vykar init")
                )
                .count(),
            1,
            "{logs:?}"
        );
        assert!(
            !logs
                .iter()
                .any(|l| l.contains("FAILED") || l.contains("drive not connected")),
            "{logs:?}"
        );
        assert_eq!(hook_log(&h), vec!["before_backup", "finally_backup"]);
    }

    /// "Back up" for one repository does not use the cycle and still fails.
    #[test]
    fn explicit_repo_backup_still_fails_when_unavailable() {
        let mut h = unavailable_harness(None);
        handle_backup_repo(&mut h.ctx, "nas".to_string());
        let (_, errors) = drain(&h.ui_rx);
        assert_eq!(errors.len(), 1, "{errors:?}");
    }

    #[cfg(unix)]
    #[test]
    fn before_hook_that_mounts_the_repository_runs_the_cycle() {
        let mount = |tmp: &Path| {
            format!(
                "mv '{}' '{}'",
                tmp.join("store").display(),
                tmp.join("volume").display()
            )
        };
        let mut h = unavailable_harness(Some(&mount));
        let volume = h.tmp.path().join("volume");
        std::fs::create_dir_all(volume.join("repo")).unwrap();
        vykar_core::commands::init::run(&h.ctx.runtime.repos.first().unwrap().config, None)
            .unwrap();
        std::fs::rename(&volume, h.tmp.path().join("store")).unwrap();

        handle_backup_all(&mut h.ctx, true);
        let (logs, errors) = drain(&h.ui_rx);

        assert!(errors.is_empty(), "{errors:?} {logs:?}");
        assert!(
            !logs
                .iter()
                .any(|l| l.contains("skipped (repository not found")),
            "{logs:?}"
        );
        assert!(
            hook_log(&h).iter().any(|l| l == "after_backup"),
            "{:?}",
            hook_log(&h)
        );
    }

    #[test]
    fn log_backup_report_reports_skipped_sources() {
        let (tx, rx) = crossbeam_channel::unbounded();
        let report = operations::BackupRunReport {
            created: Vec::new(),
            skipped: vec![operations::SkippedSource {
                label: "src".to_string(),
                reason: "not home".to_string(),
            }],
            ..Default::default()
        };

        log_backup_report(&tx, "nas", &report);

        let (logs, _) = drain(&rx);
        assert_eq!(logs, vec!["[nas] source src skipped: not home"]);
    }

    /// A config reload can rename or remove a repo between the scheduler firing
    /// and the worker handling the command; the cycle must then cover nothing
    /// (the caller logs it) rather than falling back to every repo.
    #[test]
    fn stale_name_selects_nothing() {
        let repos = two_repos();
        assert!(select_repo_indices(&repos, Some(&["gone".to_string()])).is_empty());
        assert!(select_repo_indices(&repos, Some(&[])).is_empty());
    }
}
