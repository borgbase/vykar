use std::sync::atomic::{AtomicBool, Ordering};

use crate::hooks::{self, HookContext};
use vykar_core::config::{EncryptionModeConfig, HooksConfig, SourceEntry, VykarConfig};
use vykar_storage::{parse_repo_url, ParsedUrl};

use crate::cli::Commands;
use crate::cmd;

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

enum StepResult {
    Ok,
    Partial,
    Failed(String),
    Skipped(&'static str),
}

fn make_hook_ctx(command: &str, cfg: &VykarConfig, repo_label: &Option<String>) -> HookContext {
    HookContext {
        command: command.to_string(),
        repository: cfg.repository.url.clone(),
        label: repo_label.clone(),
        error: None,
        source_label: None,
        source_paths: None,
    }
}

/// Returns `Ok(had_partial)` — `true` if backup had soft errors but still succeeded.
#[allow(clippy::too_many_arguments)]
pub(crate) fn run_default_actions(
    cfg: &VykarConfig,
    label: Option<&str>,
    sources: &[SourceEntry],
    global_hooks: &HooksConfig,
    repo_hooks: &HooksConfig,
    repo_label: &Option<String>,
    shutdown: Option<&AtomicBool>,
    verbose: u8,
) -> Result<bool, Box<dyn std::error::Error>> {
    let start = std::time::Instant::now();
    let mut steps: Vec<(&str, StepResult)> = Vec::new();

    let shutting_down = |s: Option<&AtomicBool>| s.is_some_and(|f| f.load(Ordering::SeqCst));

    // 1. Backup
    eprintln!("==> Starting backup");
    let mut had_partial = false;
    let backup_ok = match hooks::run_with_hooks(
        global_hooks,
        repo_hooks,
        &mut make_hook_ctx("backup", cfg, repo_label),
        || {
            cmd::backup::run_backup(
                cfg,
                label,
                None,
                None,
                None,
                vec![],
                sources,
                &[],
                shutdown,
                verbose,
            )
        },
    ) {
        Ok(partial) => {
            if partial {
                had_partial = true;
                steps.push(("backup", StepResult::Partial));
            } else {
                steps.push(("backup", StepResult::Ok));
            }
            true
        }
        Err(e) => {
            eprintln!("Error: {e}");
            steps.push(("backup", StepResult::Failed(e.to_string())));
            false
        }
    };

    if shutting_down(shutdown) {
        return print_summary(&steps, start, had_partial);
    }

    // 2. Prune — skip if no retention rules configured
    let has_retention = cfg.retention.has_any_rule()
        || sources
            .iter()
            .any(|s| s.retention.as_ref().is_some_and(|r| r.has_any_rule()));

    if !has_retention {
        steps.push(("prune", StepResult::Skipped("no retention rules")));
    } else if !backup_ok {
        steps.push(("prune", StepResult::Skipped("backup failed")));
    } else {
        eprintln!("==> Starting prune");
        match hooks::run_with_hooks(
            global_hooks,
            repo_hooks,
            &mut make_hook_ctx("prune", cfg, repo_label),
            || cmd::prune::run_prune(cfg, label, false, false, sources, &[], false, shutdown),
        ) {
            Ok(()) => steps.push(("prune", StepResult::Ok)),
            Err(e) => {
                eprintln!("Error: {e}");
                steps.push(("prune", StepResult::Failed(e.to_string())));
            }
        }
    }

    if shutting_down(shutdown) {
        return print_summary(&steps, start, had_partial);
    }

    // 3. Compact
    if !backup_ok {
        steps.push(("compact", StepResult::Skipped("backup failed")));
    } else {
        eprintln!("==> Starting compact");
        match hooks::run_with_hooks(
            global_hooks,
            repo_hooks,
            &mut make_hook_ctx("compact", cfg, repo_label),
            || cmd::compact::run_compact(cfg, label, cfg.compact.threshold, None, false, shutdown),
        ) {
            Ok(()) => steps.push(("compact", StepResult::Ok)),
            Err(e) => {
                eprintln!("Error: {e}");
                steps.push(("compact", StepResult::Failed(e.to_string())));
            }
        }
    }

    if shutting_down(shutdown) {
        return print_summary(&steps, start, had_partial);
    }

    // 4. Check (metadata-only)
    eprintln!("==> Starting check");
    match hooks::run_with_hooks(
        global_hooks,
        repo_hooks,
        &mut make_hook_ctx("check", cfg, repo_label),
        || cmd::check::run_check(cfg, label, false, false),
    ) {
        Ok(()) => steps.push(("check", StepResult::Ok)),
        Err(e) => {
            eprintln!("Error: {e}");
            steps.push(("check", StepResult::Failed(e.to_string())));
        }
    }

    print_summary(&steps, start, had_partial)
}

/// Prints the summary and returns `Ok(had_partial)`.
fn print_summary(
    steps: &[(&str, StepResult)],
    start: std::time::Instant,
    had_partial: bool,
) -> Result<bool, Box<dyn std::error::Error>> {
    let elapsed = start.elapsed();
    let mut had_failure = false;

    eprintln!();
    eprintln!("=== Summary ===");
    for (name, result) in steps {
        match result {
            StepResult::Ok => eprintln!("  {name:<12} ok"),
            StepResult::Partial => eprintln!("  {name:<12} ok (partial)"),
            StepResult::Failed(e) => {
                had_failure = true;
                eprintln!("  {name:<12} FAILED: {e}");
            }
            StepResult::Skipped(reason) => eprintln!("  {name:<12} skipped ({reason})"),
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

    if had_failure {
        Err("one or more steps failed".into())
    } else {
        Ok(had_partial)
    }
}

/// Returns `Ok(had_partial)` — `true` if backup had soft errors but still succeeded.
pub(crate) fn dispatch_command(
    command: &Commands,
    cfg: &VykarConfig,
    label: Option<&str>,
    sources: &[SourceEntry],
    shutdown: Option<&AtomicBool>,
    verbose: u8,
) -> Result<bool, Box<dyn std::error::Error>> {
    match command {
        Commands::Init { .. } => cmd::init::run_init(cfg, label).map(|()| false),
        Commands::Backup {
            label: user_label,
            compression,
            connections,
            source,
            paths,
            ..
        } => cmd::backup::run_backup(
            cfg,
            label,
            user_label.clone(),
            compression.clone(),
            connections.map(|v| v as usize),
            paths.clone(),
            sources,
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
            ..
        } => cmd::restore::run_restore(cfg, label, snapshot.clone(), dest.clone(), pattern.clone())
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
            ..
        } => cmd::check::run_check(cfg, label, *verify_data, *distrust_server).map(|()| false),
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
