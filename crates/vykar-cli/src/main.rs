mod cli;
mod cmd;
mod config_gen;
mod dispatch;
mod format;
mod passphrase;
mod progress;
mod prompt;
pub(crate) mod signal;
mod table;

use std::io::IsTerminal;
use std::sync::atomic::Ordering;

use clap::Parser;

use vykar_core::app::operations;
use vykar_core::config::{self, ResolvedRepo};

use crate::passphrase::with_repo_passphrase;

use cli::{Cli, Commands};
use config_gen::run_config_generate;
use dispatch::{dispatch_command, run_default_actions, warn_if_untrusted_rest};

/// Exit code: hard error (backup failed, config error, etc.).
const EXIT_ERROR: i32 = 1;
/// Exit code: partial success (backup completed but some files were skipped).
const EXIT_PARTIAL: i32 = 3;
/// Exit code: cooperative shutdown via signal (SIGINT/SIGTERM).
const EXIT_INTERRUPTED: i32 = 130;

fn main() {
    let cli = Cli::parse();
    signal::install_signal_handlers();

    // Initialize logging — auto-upgrade to info for daemon
    let filter = match cli.verbose {
        0 if matches!(&cli.command, Some(Commands::Daemon)) => "info",
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

    // Handle `config` subcommand early — no config file needed
    if let Some(Commands::Config { dest }) = &cli.command {
        if let Err(e) = run_config_generate(dest.as_deref()) {
            eprintln!("Error: {e}");
            std::process::exit(1);
        }
        return;
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
            std::process::exit(1);
        }
    };

    tracing::info!("Using config: {source}");

    // Handle `daemon` subcommand early — it owns its own config lifecycle
    // (loads, validates, and reloads config internally).
    if matches!(&cli.command, Some(Commands::Daemon)) {
        if cli.trust_repo {
            eprintln!("Error: --trust-repo cannot be used with the daemon command");
            std::process::exit(1);
        }
        if let Err(e) = cmd::daemon::run_daemon(source) {
            eprintln!("Error: {e}");
            std::process::exit(1);
        }
        return;
    }

    let mut all_repos = match config::load_and_resolve(source.path()) {
        Ok(r) => r,
        Err(e) => {
            eprintln!("Error: {e}");
            std::process::exit(1);
        }
    };

    if all_repos.is_empty() {
        eprintln!("Error: no repositories configured. Edit your config file and add a 'repositories' section.");
        std::process::exit(1);
    }

    // --trust-repo validation: must target exactly one repo.
    // Rejected for multi-repo without -R (would silently re-pin unrelated
    // repos during probing/dispatch).
    if cli.trust_repo {
        let repo_selector = cli.command.as_ref().and_then(|cmd| cmd.repo());
        if repo_selector.is_none() && all_repos.len() > 1 {
            eprintln!(
                "Error: --trust-repo requires -R / --repo when multiple repositories are configured"
            );
            std::process::exit(1);
        }
    }

    // Resolve --repo selector and set --trust-repo on the single targeted repo.
    let repo_selector = cli.command.as_ref().and_then(|cmd| cmd.repo());
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
            std::process::exit(1);
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
        all_repos[0].config.trust_repo = true;
    }
    let repos: Vec<&ResolvedRepo> = if let Some(selector) = repo_selector {
        vec![config::select_repo(&all_repos, selector).unwrap()]
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
                std::process::exit(1);
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
                std::process::exit(1);
            }
            SnapshotDispatch::NotFound => {
                eprintln!("Error: snapshot '{snap}' not found in any configured repository");
                std::process::exit(1);
            }
            SnapshotDispatch::Unique(idx) => {
                // Single match — dispatch without banner
                let result = run_repo_command(&cli, repos[idx]);
                if signal::SHUTDOWN.load(Ordering::SeqCst) {
                    eprintln!("Interrupted");
                    std::process::exit(EXIT_INTERRUPTED);
                }
                match result {
                    Ok(true) => std::process::exit(EXIT_PARTIAL),
                    Ok(false) => {}
                    Err(e) => {
                        eprintln!("Error: {e}");
                        std::process::exit(EXIT_ERROR);
                    }
                }
            }
            SnapshotDispatch::Ambiguous(indices) => {
                let names: Vec<&str> = indices
                    .iter()
                    .map(|i| repo_display_name(repos[*i]))
                    .collect();
                eprintln!(
                    "Error: snapshot '{snap}' found in multiple repositories: {}. \
                     Use -R / --repo to select one.",
                    names.join(", ")
                );
                std::process::exit(1);
            }
            SnapshotDispatch::ProbeError { matches, errors } => {
                eprintln!("Error: could not probe all repositories");
                for (i, err) in &errors {
                    eprintln!("  {}:  {err}", repo_display_name(repos[*i]));
                }
                for i in &matches {
                    eprintln!("  {}:  found '{snap}'", repo_display_name(repos[*i]));
                }
                eprintln!("Use -R / --repo to target a specific repository.");
                std::process::exit(1);
            }
        }
        return;
    }

    // Default path: run against all selected repos
    let mut had_error = false;
    let mut had_partial = false;

    for repo in &repos {
        if signal::SHUTDOWN.load(Ordering::SeqCst) {
            break;
        }
        if multi {
            eprintln!("--- Repository: {} ---", repo_display_name(repo));
        }

        let result = run_repo_command(&cli, repo);
        if signal::SHUTDOWN.load(Ordering::SeqCst) {
            eprintln!("Interrupted");
            std::process::exit(EXIT_INTERRUPTED);
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
                } else {
                    std::process::exit(EXIT_ERROR);
                }
            }
        }
    }

    if signal::SHUTDOWN.load(Ordering::SeqCst) {
        eprintln!("Interrupted");
        std::process::exit(EXIT_INTERRUPTED);
    }
    if had_error {
        std::process::exit(EXIT_ERROR);
    }
    if had_partial {
        std::process::exit(EXIT_PARTIAL);
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn repo_display_name(repo: &ResolvedRepo) -> &str {
    repo.label.as_deref().unwrap_or(&repo.config.repository.url)
}

/// Result of probing multiple repos for a snapshot name.
enum SnapshotDispatch {
    /// "latest" is ambiguous across repos — caller must specify --repo.
    RequireRepo,
    /// Snapshot not found in any repo (and all probes succeeded).
    NotFound,
    /// Exactly one repo contains the snapshot.
    Unique(usize),
    /// Multiple repos contain the snapshot.
    Ambiguous(Vec<usize>),
    /// At least one probe failed — we can't be sure of the result.
    ProbeError {
        matches: Vec<usize>,
        errors: Vec<(usize, String)>,
    },
}

/// Classify where a snapshot lives across multiple repos.
/// Pure decision logic — no I/O side effects beyond the lightweight probes.
fn classify_snapshot_target(snap: &str, repos: &[&ResolvedRepo]) -> SnapshotDispatch {
    if snap.eq_ignore_ascii_case("latest") {
        return SnapshotDispatch::RequireRepo;
    }

    let mut matches: Vec<usize> = Vec::new();
    let mut errors: Vec<(usize, String)> = Vec::new();

    for (i, repo) in repos.iter().enumerate() {
        match probe_snapshot(&repo.config, repo.label.as_deref(), snap) {
            Ok(true) => matches.push(i),
            Ok(false) => {}
            Err(e) => errors.push((i, e.to_string())),
        }
    }

    if !errors.is_empty() {
        return SnapshotDispatch::ProbeError { matches, errors };
    }

    match matches.len() {
        0 => SnapshotDispatch::NotFound,
        1 => SnapshotDispatch::Unique(matches[0]),
        _ => SnapshotDispatch::Ambiguous(matches),
    }
}

/// Probe whether a repo's manifest contains a snapshot (lightweight open).
fn probe_snapshot(
    config: &vykar_core::config::VykarConfig,
    label: Option<&str>,
    snapshot_name: &str,
) -> Result<bool, Box<dyn std::error::Error>> {
    with_repo_passphrase(config, label, |passphrase| {
        let repo = vykar_core::commands::util::open_repo(
            config,
            passphrase,
            vykar_core::OpenOptions::new(),
        )
        .map_err(|e| -> Box<dyn std::error::Error> { Box::new(e) })?;
        Ok(repo.manifest().find_snapshot(snapshot_name).is_some())
    })
}

/// Execute the CLI command (or default actions) against one repo.
/// Returns `Ok(had_partial)` where `true` means backup had soft errors.
fn run_repo_command(cli: &Cli, repo: &ResolvedRepo) -> Result<bool, Box<dyn std::error::Error>> {
    warn_if_untrusted_rest(&repo.config, repo.label.as_deref());

    let shutdown = Some(&signal::SHUTDOWN as &std::sync::atomic::AtomicBool);
    match &cli.command {
        Some(cmd) => {
            let run_action = || dispatch_command(cmd, repo, shutdown, cli.verbose);
            if matches!(cmd, Commands::Backup { .. }) {
                // Backup: hooks handled by run_backup_selection in core
                run_action()
            } else {
                // Other commands: wrap with repo-level hooks via core
                operations::run_command_with_hooks(repo, cmd.name(), run_action)
            }
        }
        None => run_default_actions(repo, shutdown, cli.verbose),
    }
}
