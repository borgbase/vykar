#![allow(clippy::print_stderr, clippy::print_stdout)]
#![cfg_attr(
    test,
    allow(
        clippy::expect_used,
        clippy::indexing_slicing,
        clippy::panic,
        clippy::unwrap_used
    )
)]

mod cli;
mod cmd;
mod config_gen;
mod dispatch;
mod error;
mod format;
mod passphrase;
mod progress;
mod prompt;
mod resolve;
pub(crate) mod signal;
mod table;

use std::io::IsTerminal;
use std::process::ExitCode;
use std::sync::atomic::Ordering;

use clap::Parser;

use vykar_core::app::operations;
use vykar_core::config::{self, ResolvedRepo};

use crate::cli::{Cli, Commands};
use crate::config_gen::run_config_generate;
use crate::dispatch::{
    dispatch_command, local_repo_unavailable, run_default_actions, warn_if_untrusted_rest,
};
use crate::error::{CliError, CliResult};
use crate::resolve::{
    classify_diff_target, classify_snapshot_target, repo_display_name, DiffDispatch,
    SnapshotDispatch,
};

/// Exit code: hard error (backup failed, config error, etc.).
pub(crate) const EXIT_ERROR: u8 = 1;
/// Exit code: partial success (backup completed but some files were skipped).
pub(crate) const EXIT_PARTIAL: u8 = 3;
/// Exit code: cooperative shutdown via signal (SIGINT/SIGTERM).
pub(crate) const EXIT_INTERRUPTED: u8 = 130;

pub fn run() -> ExitCode {
    run_cli(Cli::parse())
}

pub(crate) fn run_cli(cli: Cli) -> ExitCode {
    signal::install_signal_handlers();

    // Initialize logging — auto-upgrade to info for daemon
    let filter = match cli.verbose {
        0 if matches!(&cli.command, Some(Commands::Daemon { .. })) => "info",
        0 => "warn",
        1 => "info",
        2 => "debug",
        _ => "trace",
    };
    let is_tty = std::io::stderr().is_terminal();
    let builder = tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_target(false)
        .with_writer(progress::ProgressAwareStderr);
    if is_tty {
        builder.without_time().init();
    } else {
        builder.init();
    }

    // Reject top-level --repo/--source when a subcommand is present.
    // These flags belong on the subcommand itself (e.g. `vykar backup -R repo`).
    if cli.command.is_some() && (cli.repo.is_some() || !cli.source.is_empty()) {
        eprintln!(
            "Error: --repo/--source on the bare command cannot be combined with a subcommand.\n\
             Place -R/--repo and -S/--source after the subcommand instead."
        );
        return ExitCode::from(EXIT_ERROR);
    }

    // Handle `config` subcommand early — no config file needed
    if let Some(Commands::Config { dest }) = &cli.command {
        if let Err(e) = run_config_generate(dest.as_deref()) {
            eprintln!("Error: {e}");
            return ExitCode::from(EXIT_ERROR);
        }
        return ExitCode::SUCCESS;
    }

    // Resolve config file
    let source = match config::resolve_config_path(cli.config.as_deref()) {
        Some(s) => s,
        None => {
            eprintln!("Error: no configuration file found.");
            eprintln!("Searched:");
            for (path, level) in config::default_config_search_paths() {
                eprintln!("  {} ({})", path.display(), level);
            }
            eprintln!();
            eprintln!("Run `vykar config` to generate a starter config file.");
            return ExitCode::from(EXIT_ERROR);
        }
    };

    tracing::info!("Using config: {source}");

    // Handle `daemon` subcommand early — it owns its own config lifecycle
    // (loads, validates, and reloads config internally).
    if let Some(Commands::Daemon {
        http_listen,
        http_allow_public,
    }) = &cli.command
    {
        if cli.trust_repo {
            eprintln!("Error: --trust-repo cannot be used with the daemon command");
            return ExitCode::from(EXIT_ERROR);
        }
        if let Some(addr) = http_listen {
            if !addr.ip().is_loopback() && !*http_allow_public {
                eprintln!(
                    "Error: --http-listen {addr} binds to a non-loopback address; \
                     pass --http-allow-public (or set VYKAR_HTTP_ALLOW_PUBLIC=1) to confirm"
                );
                return ExitCode::from(EXIT_ERROR);
            }
        }
        if let Err(e) = cmd::daemon::run_daemon(source, *http_listen) {
            eprintln!("Error: {e}");
            return ExitCode::from(EXIT_ERROR);
        }
        return ExitCode::SUCCESS;
    }

    let mut all_repos = match config::load_and_resolve(source.path()) {
        Ok(r) => r,
        Err(e) => {
            eprintln!("Error: {e}");
            return ExitCode::from(EXIT_ERROR);
        }
    };

    if all_repos.is_empty() {
        eprintln!("Error: no repositories configured. Edit your config file and add a 'repositories' section.");
        return ExitCode::from(EXIT_ERROR);
    }

    // Effective --repo selector: subcommand flag takes precedence, then top-level.
    let repo_selector = cli
        .command
        .as_ref()
        .and_then(|cmd| cmd.repo())
        .or(cli.repo.as_deref());

    // --trust-repo validation: must target exactly one repo.
    // Rejected for multi-repo without -R (would silently re-pin unrelated
    // repos during probing/dispatch).
    if cli.trust_repo && repo_selector.is_none() && all_repos.len() > 1 {
        eprintln!(
            "Error: --trust-repo requires -R / --repo when multiple repositories are configured"
        );
        return ExitCode::from(EXIT_ERROR);
    }

    // Resolve --repo selector and set --trust-repo on the single targeted repo.
    if let Some(selector) = repo_selector {
        let found = all_repos
            .iter()
            .any(|r| r.label.as_deref() == Some(selector) || r.config.repository.url == selector);
        if !found {
            eprintln!("Error: no repository matching '{selector}'");
            eprintln!("Available repositories:");
            for r in &all_repos {
                let label = r.label.as_deref().unwrap_or("-");
                eprintln!("  {label:12} {}", r.config.repository.url);
            }
            return ExitCode::from(EXIT_ERROR);
        }
        if cli.trust_repo {
            for repo in &mut all_repos {
                if repo.label.as_deref() == Some(selector) || repo.config.repository.url == selector
                {
                    repo.config.trust_repo = true;
                }
            }
        }
    } else if cli.trust_repo {
        // Single-repo config (multi-repo already rejected above).
        all_repos
            .first_mut()
            .expect("single-repo config was validated")
            .config
            .trust_repo = true;
    }
    let repos: Vec<&ResolvedRepo> = if let Some(selector) = repo_selector {
        vec![config::select_repo(&all_repos, selector).expect("repo selector was validated")]
    } else {
        all_repos.iter().collect()
    };

    let multi = repos.len() > 1;

    // Bulk snapshot delete requires -R when multiple repos are configured,
    // since the smart single-snapshot probe cannot handle multiple names.
    if multi {
        if let Some(cli::Commands::Snapshot {
            command: cli::SnapshotCommand::Delete { snapshots, .. },
        }) = cli.command.as_ref()
        {
            if snapshots.len() > 1 {
                eprintln!(
                    "Error: deleting multiple snapshots requires -R / --repo \
                     when multiple repositories are configured"
                );
                return ExitCode::from(EXIT_ERROR);
            }
        }
    }

    // Smart snapshot diff dispatch in multi-repo configs: probe both snapshot
    // names and pick the unique containing repo, or error with a helpful
    // message. Single-repo configs fall through to the default path below.
    if multi {
        if let Some(cli::Commands::Snapshot {
            command:
                cli::SnapshotCommand::Diff {
                    snapshot_a,
                    snapshot_b,
                },
        }) = cli.command.as_ref()
        {
            for repo in &repos {
                warn_if_untrusted_rest(&repo.config, repo.label.as_deref());
            }
            match classify_diff_target(snapshot_a, snapshot_b, &repos) {
                DiffDispatch::LatestRequiresRepo => {
                    eprintln!(
                        "Error: 'latest' is ambiguous in snapshot diff when multiple repositories \
                         are configured; rename the snapshot or scope the config"
                    );
                    return ExitCode::from(EXIT_ERROR);
                }
                DiffDispatch::SnapshotNotFound { snapshot } => {
                    eprintln!(
                        "Error: snapshot '{snapshot}' not found in any configured repository"
                    );
                    return ExitCode::from(EXIT_ERROR);
                }
                DiffDispatch::DifferentRepos { a_repo, b_repo } => {
                    eprintln!(
                        "Error: snapshot diff requires both snapshots to live in the same \
                         repository: '{snapshot_a}' is in '{a_repo}', '{snapshot_b}' is in '{b_repo}'"
                    );
                    return ExitCode::from(EXIT_ERROR);
                }
                DiffDispatch::Ambiguous {
                    snapshot,
                    repos: rs,
                } => {
                    let names: Vec<&str> = rs
                        .iter()
                        .map(|i| {
                            repo_display_name(
                                repos
                                    .get(*i)
                                    .copied()
                                    .expect("dispatch repo index is valid"),
                            )
                        })
                        .collect();
                    eprintln!(
                        "Error: snapshot '{snapshot}' is present in multiple repositories: {}. \
                         Rename the snapshot or scope the config.",
                        names.join(", ")
                    );
                    return ExitCode::from(EXIT_ERROR);
                }
                DiffDispatch::ProbeError { errors } => {
                    eprintln!("Error: could not probe all repositories");
                    for (i, err) in &errors {
                        let repo = repos
                            .get(*i)
                            .copied()
                            .expect("dispatch repo index is valid");
                        eprintln!("  {}:  {err}", repo_display_name(repo));
                    }
                    return ExitCode::from(EXIT_ERROR);
                }
                DiffDispatch::Unique(idx) => {
                    let Some(repo) = repos.get(idx).copied() else {
                        eprintln!("Internal error: diff target repo index out of range");
                        return ExitCode::from(EXIT_ERROR);
                    };
                    let result = run_repo_command(&cli, repo);
                    if signal::SHUTDOWN.load(Ordering::SeqCst) {
                        eprintln!("Interrupted");
                        return ExitCode::from(EXIT_INTERRUPTED);
                    }
                    match result {
                        Ok(true) => return ExitCode::from(EXIT_PARTIAL),
                        Ok(false) => return ExitCode::SUCCESS,
                        Err(e) => {
                            eprintln!("Error: {e}");
                            return ExitCode::from(EXIT_ERROR);
                        }
                    }
                }
            }
        }
    }

    // Smart snapshot dispatch: when multiple repos are configured and the
    // command targets a specific snapshot, probe repos to find the one that
    // actually contains it, rather than running against all repos.
    if let (true, Some(snap)) = (multi, cli.command.as_ref().and_then(|c| c.snapshot_name())) {
        // Emit REST/plaintext warnings before probing backends
        for repo in &repos {
            warn_if_untrusted_rest(&repo.config, repo.label.as_deref());
        }

        match classify_snapshot_target(snap, &repos) {
            SnapshotDispatch::RequireRepo => {
                eprintln!(
                    "Error: 'latest' requires -R / --repo when multiple repositories are configured"
                );
                return ExitCode::from(EXIT_ERROR);
            }
            SnapshotDispatch::NotFound => {
                eprintln!("Error: snapshot '{snap}' not found in any configured repository");
                return ExitCode::from(EXIT_ERROR);
            }
            SnapshotDispatch::Unique(idx) => {
                // Single match — dispatch without banner
                let Some(repo) = repos.get(idx).copied() else {
                    eprintln!("Internal error: snapshot dispatch index out of range");
                    return ExitCode::from(EXIT_ERROR);
                };
                let result = run_repo_command(&cli, repo);
                if signal::SHUTDOWN.load(Ordering::SeqCst) {
                    eprintln!("Interrupted");
                    return ExitCode::from(EXIT_INTERRUPTED);
                }
                match result {
                    Ok(true) => return ExitCode::from(EXIT_PARTIAL),
                    Ok(false) => {}
                    Err(e) => {
                        eprintln!("Error: {e}");
                        return ExitCode::from(EXIT_ERROR);
                    }
                }
            }
            SnapshotDispatch::Ambiguous(indices) => {
                let names: Vec<&str> = indices
                    .iter()
                    .map(|i| {
                        repo_display_name(
                            repos
                                .get(*i)
                                .copied()
                                .expect("dispatch repo index is valid"),
                        )
                    })
                    .collect();
                eprintln!(
                    "Error: snapshot '{snap}' found in multiple repositories: {}. \
                     Use -R / --repo to select one.",
                    names.join(", ")
                );
                return ExitCode::from(EXIT_ERROR);
            }
            SnapshotDispatch::ProbeError { matches, errors } => {
                eprintln!("Error: could not probe all repositories");
                for (i, err) in &errors {
                    let repo = repos
                        .get(*i)
                        .copied()
                        .expect("dispatch repo index is valid");
                    eprintln!("  {}:  {err}", repo_display_name(repo));
                }
                for i in &matches {
                    let repo = repos
                        .get(*i)
                        .copied()
                        .expect("dispatch repo index is valid");
                    eprintln!("  {}:  found '{snap}'", repo_display_name(repo));
                }
                eprintln!("Use -R / --repo to target a specific repository.");
                return ExitCode::from(EXIT_ERROR);
            }
        }
        return ExitCode::SUCCESS;
    }

    // Default path: run against all selected repos
    let mut had_error = false;
    let mut had_partial = false;
    let repo_explicitly_selected = repo_selector.is_some();

    for repo in &repos {
        if signal::SHUTDOWN.load(Ordering::SeqCst) {
            break;
        }

        // Pre-flight: skip unavailable local repos in multi-repo bare
        // command (no subcommand) when no explicit --repo was given.
        // Subcommands like `init` must reach uninitialized repos normally.
        if cli.command.is_none() && multi && !repo_explicitly_selected {
            if let Some(path) = local_repo_unavailable(repo) {
                eprintln!(
                    "Warning: skipping '{}' — repository not found at '{path}'",
                    repo_display_name(repo),
                );
                continue;
            }
        }

        if multi {
            eprintln!("--- Repository: {} ---", repo_display_name(repo));
        }

        let result = run_repo_command(&cli, repo);
        if signal::SHUTDOWN.load(Ordering::SeqCst) {
            eprintln!("Interrupted");
            return ExitCode::from(EXIT_INTERRUPTED);
        }
        match result {
            Ok(partial) => {
                if partial {
                    had_partial = true;
                }
            }
            Err(e) => {
                eprintln!("Error: {e}");
                had_error = true;
                if multi {
                    continue;
                }
                return ExitCode::from(EXIT_ERROR);
            }
        }
    }

    if signal::SHUTDOWN.load(Ordering::SeqCst) {
        eprintln!("Interrupted");
        return ExitCode::from(EXIT_INTERRUPTED);
    }
    if had_error {
        return ExitCode::from(EXIT_ERROR);
    }
    if had_partial {
        return ExitCode::from(EXIT_PARTIAL);
    }
    ExitCode::SUCCESS
}

/// Execute the CLI command (or default actions) against one repo.
/// Returns `Ok(had_partial)` where `true` means backup had soft errors.
fn run_repo_command(cli: &Cli, repo: &ResolvedRepo) -> CliResult<bool> {
    warn_if_untrusted_rest(&repo.config, repo.label.as_deref());

    let shutdown = Some(&signal::SHUTDOWN as &std::sync::atomic::AtomicBool);
    match &cli.command {
        Some(cmd) => {
            if matches!(cmd, Commands::Backup { .. }) {
                // Backup: hooks handled by run_backup_selection in core
                dispatch_command(cmd, repo, shutdown, cli.verbose)
            } else {
                // Other commands: wrap with repo-level hooks via core. The closure
                // flattens the CLI's CliError into VykarError::Other to match the
                // library signature; display-only usage downstream makes the lost
                // downcast info immaterial.
                let run_action = || -> vykar_types::error::Result<bool> {
                    dispatch_command(cmd, repo, shutdown, cli.verbose)
                        .map_err(|e| vykar_types::error::VykarError::Other(e.to_string()))
                };
                operations::run_command_with_hooks(repo, cmd.name(), run_action)
                    .map_err(CliError::from)
            }
        }
        None => run_default_actions(repo, shutdown, cli.verbose, &cli.source),
    }
}
