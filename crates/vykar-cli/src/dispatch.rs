use std::io::IsTerminal;
use std::sync::atomic::AtomicBool;

use crate::progress::BackupProgressRenderer;
use vykar_core::app::operations::{self, CycleEvent, CycleStep, FullCycleResult, StepOutcome};
use vykar_core::app::preflight::{self, Availability, UnavailableRepo};
use vykar_core::config::{EncryptionModeConfig, ResolvedRepo, VykarConfig};
use vykar_storage::{ParsedUrl, parse_repo_url};

use crate::cli::{Commands, KeyCommand};
use crate::cmd;
use crate::cmd::backup::BackupRunOpts;
use crate::cmd::check::{format_check_progress, print_check_summary};
use crate::error::{CliError, CliResult};
use crate::format::print_backup_stats;
use crate::passphrase::get_passphrase;
use vykar_common::display::format_duration_seconds;
use vykar_types::error::VykarError;

pub(crate) fn warn_if_untrusted_rest(config: &VykarConfig, label: Option<&str>) {
    let Ok(parsed) = parse_repo_url(&config.repository.url) else {
        return;
    };
    let ParsedUrl::Rest { url } = parsed else {
        return;
    };

    let repo_name = label.unwrap_or(&config.repository.url);
    if config.encryption.mode == EncryptionModeConfig::None {
        eprintln!(
            "Warning: repository '{repo_name}' uses REST with plaintext mode (encryption.mode=none)."
        );
    }
    if url.starts_with("http://") {
        eprintln!(
            "Warning: repository '{repo_name}' uses non-HTTPS REST URL '{url}'. Transport is not TLS-protected."
        );
    }
}

/// How a default-actions cycle ended, when no step failed.
pub(crate) struct CycleOutcome {
    /// Backup had soft errors but still succeeded.
    pub partial: bool,
    /// Why the backup step (and so the whole cycle) was skipped, if it was.
    pub skipped: Option<String>,
    /// The skip in `skipped` is because the local repository was missing.
    pub unavailable: bool,
}

pub(crate) fn run_default_actions(
    repo: &ResolvedRepo,
    shutdown: Option<&AtomicBool>,
    verbose: u8,
    source_filter: &[String],
    unavailable: UnavailableRepo,
) -> CliResult<CycleOutcome> {
    let start = std::time::Instant::now();
    let label = repo.label.as_deref();

    // A repository that is missing now can only appear through a `before` hook,
    // so its passphrase is fetched after the hooks, if it appears at all. Any
    // other repository gets its passphrase first, as before.
    let defer = unavailable == UnavailableRepo::Skip
        && matches!(preflight::availability(repo), Availability::LocalMissing(_));
    let mut early = if defer {
        None
    } else {
        get_passphrase(&repo.config, label)?
    };
    let mut resolve = || {
        if defer {
            get_passphrase(&repo.config, label).map_err(|e| match e {
                CliError::Vykar(e) => e,
                e => VykarError::Other(e.to_string()),
            })
        } else {
            Ok(early.take())
        }
    };

    let is_tty = std::io::stderr().is_terminal();
    let show_progress = is_tty || verbose > 0;
    let mut backup_renderer: Option<BackupProgressRenderer> = None;

    let result = operations::run_full_cycle_with_passphrase_source(
        repo,
        &mut resolve,
        shutdown,
        verbose >= 1,
        source_filter,
        unavailable,
        &mut |event| {
            match &event {
                CycleEvent::StepStarted(step) => {
                    eprintln!("==> Starting {}", step.command_name());
                    if matches!(step, CycleStep::Backup) && show_progress {
                        backup_renderer = Some(BackupProgressRenderer::new(verbose, is_tty));
                    }
                }
                CycleEvent::StepFinished(step, outcome) => {
                    if matches!(step, CycleStep::Backup) {
                        if let Some(ref mut r) = backup_renderer {
                            r.finish();
                        }
                        backup_renderer = None;
                    }
                    if let StepOutcome::Failed(e) = outcome {
                        eprintln!("Error: {e}");
                    }
                }
                CycleEvent::Backup(evt) => {
                    if let Some(ref mut r) = backup_renderer {
                        r.on_event(evt.clone());
                    }
                }
                CycleEvent::Check(evt) => {
                    format_check_progress(evt);
                }
                // HookWarning: tracing::warn! already fired inside log_hook_errors.
                // Events are for GUI consumers only.
                CycleEvent::HookWarning { .. } => {}
                CycleEvent::StepWarning { step, message } => {
                    // tracing::warn! already fired inside the step;
                    // duplicate to stderr so CLI users see the warning
                    // even without the tracing subscriber.
                    eprintln!("warning: [{}] {message}", step.command_name());
                }
            }
        },
    );

    // Ensure renderer is cleaned up if cycle ended abruptly (e.g. shutdown).
    if let Some(ref mut r) = backup_renderer {
        r.finish();
    }

    print_step_details(&result);
    print_summary(&result.steps, start);

    if result.has_failures() {
        Err(CliError::from("one or more steps failed"))
    } else {
        Ok(CycleOutcome {
            partial: result.had_partial(),
            skipped: result.backup_skipped().map(str::to_owned),
            unavailable: result.repo_unavailable,
        })
    }
}

/// Print the detailed per-step output (matching what individual CLI wrappers print).
fn print_step_details(result: &FullCycleResult) {
    // Backup summaries
    if let Some(ref report) = result.backup_report {
        for created in &report.created {
            let stats = &created.stats;
            let paths_display = created.source_paths.join(", ");
            println!("Snapshot created: {}", created.snapshot_name);
            println!(
                "  Source: {paths_display} (label: {})",
                created.source_label
            );
            if stats.errors > 0 {
                eprintln!(
                    "Warning: {} file(s) could not be read and were excluded from the snapshot",
                    stats.errors
                );
            }
            print_backup_stats(stats);
        }
        for skipped in &report.skipped {
            eprintln!("Skipped source '{}': {}", skipped.label, skipped.reason);
        }
        for failed in &report.failed {
            eprintln!("Failed source '{}': {}", failed.label, failed.error);
        }
        if let Some(aborted) = &report.aborted {
            crate::format::print_aborted_run(aborted);
        }
    }

    // Prune stats
    if let Some(ref stats) = result.prune_stats {
        cmd::prune::print_prune_summary(stats);
    }

    // Compact stats
    if let Some(ref stats) = result.compact_stats {
        cmd::compact::print_compact_summary(stats);
    }

    // Check results (skip summary if check was skipped — the step outcome table already shows it)
    if let Some(ref result) = result.check_result
        && !result.skipped
    {
        print_check_summary(result);
    }
}

/// Prints the summary table.
fn print_summary(steps: &[(CycleStep, StepOutcome)], start: std::time::Instant) {
    let elapsed = start.elapsed();

    eprintln!();
    eprintln!("=== Summary ===");
    for (step, result) in steps {
        let name = step.command_name();
        match result {
            StepOutcome::Ok => eprintln!("  {name:<12} ok"),
            StepOutcome::Partial => eprintln!("  {name:<12} ok (partial)"),
            StepOutcome::Failed(e) => {
                eprintln!("  {name:<12} FAILED: {e}");
            }
            StepOutcome::Skipped(reason) => eprintln!("  {name:<12} skipped ({reason})"),
        }
    }

    eprintln!(
        "  Duration:    {}",
        format_duration_seconds(elapsed.as_secs() as i64)
    );
}

/// Returns `Ok(had_partial)` — `true` if backup had soft errors but still succeeded.
pub(crate) fn dispatch_command(
    command: &Commands,
    repo: &ResolvedRepo,
    shutdown: Option<&AtomicBool>,
    verbose: u8,
) -> CliResult<bool> {
    let cfg = &repo.config;
    let label = repo.label.as_deref();
    let sources = &repo.sources;

    match command {
        Commands::Init { .. } => cmd::init::run_init(cfg, label).map(|()| false),
        Commands::Backup {
            label: user_label,
            compression,
            connections,
            threads,
            source,
            paths,
            ..
        } => cmd::backup::run_backup(
            repo,
            BackupRunOpts {
                user_label: user_label.clone(),
                compression_override: compression.clone(),
                connections: connections.map(usize::from),
                threads: threads.map(usize::from),
                paths: paths.clone(),
                source_filter: source,
                shutdown,
                verbose,
            },
        ),
        Commands::List { source, last, .. } => {
            cmd::list::run_list(cfg, label, source, *last).map(|()| false)
        }
        Commands::Snapshot { command, .. } => {
            cmd::snapshot::run_snapshot_command(command, cfg, label, shutdown).map(|()| false)
        }
        Commands::Restore {
            snapshot,
            dest,
            pattern,
            verify,
            continue_on_error,
            ..
        } => cmd::restore::run_restore(
            cfg,
            label,
            snapshot.clone(),
            dest.clone(),
            pattern.clone(),
            *verify,
            *continue_on_error,
            shutdown,
        ),
        Commands::Delete {
            yes_delete_this_repo,
            ..
        } => cmd::delete::run_delete_repo(cfg, label, *yes_delete_this_repo).map(|()| false),
        Commands::Prune {
            dry_run,
            list,
            source,
            ..
        } => cmd::prune::run_prune(cfg, label, *dry_run, *list, sources, source, shutdown)
            .map(|()| false),
        Commands::Check {
            verify_data,
            distrust_server,
            repair,
            dry_run,
            yes,
            ..
        } => cmd::check::run_check(
            cfg,
            label,
            *verify_data,
            *distrust_server,
            *repair,
            *dry_run,
            *yes,
            shutdown,
        )
        .map(|()| false),
        Commands::Info { .. } => cmd::info::run_info(cfg, label).map(|()| false),
        Commands::Key { command, .. } => match command {
            KeyCommand::Export { output, .. } => {
                cmd::key::run_key_export(cfg, label, output.as_deref()).map(|()| false)
            }
            KeyCommand::Import { file, force, .. } => {
                cmd::key::run_key_import(cfg, label, file, *force).map(|()| false)
            }
        },
        Commands::Mount {
            snapshot,
            source,
            address,
            cache_size,
            ..
        } => cmd::mount::run_mount(
            cfg,
            label,
            snapshot.clone(),
            address.clone(),
            *cache_size,
            source,
        )
        .map(|()| false),
        Commands::BreakLock { sessions, .. } => {
            cmd::break_lock::run_break_lock(cfg, label, *sessions).map(|()| false)
        }
        Commands::Compact {
            threshold,
            max_repack_size,
            dry_run,
            ..
        } => {
            let t = threshold.unwrap_or(cfg.compact.threshold);
            cmd::compact::run_compact(cfg, label, t, max_repack_size.clone(), *dry_run, shutdown)
                .map(|()| false)
        }
        Commands::Upgrade { yes, .. } => {
            cmd::upgrade::run_upgrade(cfg, label, *yes).map(|()| false)
        }
        Commands::Config { .. } => Err(CliError::from(
            "'config' command should be handled before config resolution",
        )),
        Commands::Daemon { .. } => Err(CliError::from(
            "'daemon' command should be handled before per-repo dispatch",
        )),
    }
}
