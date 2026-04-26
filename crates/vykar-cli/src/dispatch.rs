use std::io::IsTerminal;
use std::sync::atomic::AtomicBool;

use crate::progress::BackupProgressRenderer;
use vykar_core::app::operations::{self, CycleEvent, CycleStep, FullCycleResult, StepOutcome};
use vykar_core::config::{EncryptionModeConfig, ResolvedRepo, VykarConfig};
use vykar_storage::{parse_repo_url, ParsedUrl};

use crate::cli::Commands;
use crate::cmd;
use crate::cmd::check::{format_check_progress, print_check_summary};
use crate::format::{format_bytes, print_backup_stats};
use crate::passphrase::with_repo_passphrase;

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

/// Returns `Ok(had_partial)` — `true` if backup had soft errors but still succeeded.
pub(crate) fn run_default_actions(
    repo: &ResolvedRepo,
    shutdown: Option<&AtomicBool>,
    verbose: u8,
    source_filter: &[String],
) -> Result<bool, Box<dyn std::error::Error>> {
    let start = std::time::Instant::now();
    let label = repo.label.as_deref();

    let result = with_repo_passphrase(&repo.config, label, |passphrase| {
        let is_tty = std::io::stderr().is_terminal();
        let show_progress = is_tty || verbose > 0;
        let mut backup_renderer: Option<BackupProgressRenderer> = None;

        let cycle_result = operations::run_full_cycle_for_repo(
            repo,
            passphrase,
            shutdown,
            verbose >= 1,
            source_filter,
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
                        if matches!(outcome, StepOutcome::Failed(..)) {
                            if let StepOutcome::Failed(e) = outcome {
                                eprintln!("Error: {e}");
                            }
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

        Ok(cycle_result)
    })?;

    print_step_details(&result);
    print_summary(&result.steps, start)?;

    if result.has_failures() {
        Err("one or more steps failed".into())
    } else {
        Ok(result.had_partial())
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
    }

    // Prune stats
    if let Some(ref stats) = result.prune_stats {
        println!(
            "Pruned {} snapshots (kept {}), freed {} chunks ({})",
            stats.pruned,
            stats.kept,
            stats.chunks_deleted,
            format_bytes(stats.space_freed),
        );
    }

    // Compact stats
    if let Some(ref stats) = result.compact_stats {
        println!(
            "Compaction complete: {} packs repacked, {} empty packs deleted, {} freed",
            stats.packs_repacked,
            stats.packs_deleted_empty,
            format_bytes(stats.space_freed),
        );
        if stats.packs_corrupt > 0 {
            eprintln!(
                "  Warning: {} corrupt pack(s) found; run `vykar check --verify-data` for details",
                stats.packs_corrupt,
            );
        }
        if stats.packs_orphan > 0 {
            eprintln!(
                "  {} orphan pack(s) (present on disk but not in index)",
                stats.packs_orphan,
            );
        }
    }

    // Check results (skip summary if check was skipped — the step outcome table already shows it)
    if let Some(ref result) = result.check_result {
        if !result.skipped {
            print_check_summary(result);
        }
    }
}

/// Prints the summary table.
fn print_summary(
    steps: &[(CycleStep, StepOutcome)],
    start: std::time::Instant,
) -> Result<(), Box<dyn std::error::Error>> {
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

    let secs = elapsed.as_secs();
    let mins = secs / 60;
    let secs = secs % 60;
    if mins > 0 {
        eprintln!("  Duration:    {mins}m {secs:02}s");
    } else {
        eprintln!("  Duration:    {secs}s");
    }

    Ok(())
}

/// Returns `Ok(had_partial)` — `true` if backup had soft errors but still succeeded.
pub(crate) fn dispatch_command(
    command: &Commands,
    repo: &ResolvedRepo,
    shutdown: Option<&AtomicBool>,
    verbose: u8,
) -> Result<bool, Box<dyn std::error::Error>> {
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
            user_label.clone(),
            compression.clone(),
            connections.map(|v| v as usize),
            threads.map(|v| v as usize),
            paths.clone(),
            source,
            shutdown,
            verbose,
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
            ..
        } => cmd::restore::run_restore(
            cfg,
            label,
            snapshot.clone(),
            dest.clone(),
            pattern.clone(),
            *verify,
        )
        .map(|()| false),
        Commands::Delete {
            yes_delete_this_repo,
            ..
        } => cmd::delete::run_delete_repo(cfg, label, *yes_delete_this_repo).map(|()| false),
        Commands::Prune {
            dry_run,
            list,
            source,
            compact,
            ..
        } => cmd::prune::run_prune(
            cfg, label, *dry_run, *list, sources, source, *compact, shutdown,
        )
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
        )
        .map(|()| false),
        Commands::Info { .. } => cmd::info::run_info(cfg, label).map(|()| false),
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
        Commands::Config { .. } => {
            Err("'config' command should be handled before config resolution".into())
        }
        Commands::Daemon => {
            Err("'daemon' command should be handled before per-repo dispatch".into())
        }
    }
}

/// For local repos, check if the config sentinel file is reachable.
/// Returns `Some(path)` if local and config definitely missing, `None` otherwise.
pub(crate) fn local_repo_unavailable(repo: &ResolvedRepo) -> Option<String> {
    let parsed = parse_repo_url(&repo.config.repository.url).ok()?;
    if let ParsedUrl::Local { path } = parsed {
        let config_path = std::path::Path::new(&path).join("config");
        match config_path.try_exists() {
            Ok(false) => return Some(path), // definitely missing → skip
            Ok(true) => {}                  // present → proceed
            Err(_) => {}                    // can't tell → let it fail normally
        }
    }
    None
}
