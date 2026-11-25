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
mod output;
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
use vykar_core::app::preflight::UnavailableRepo;
use vykar_core::config::{self, ResolvedRepo};

use crate::cli::{Cli, Commands};
use crate::config_gen::run_config_generate;
use crate::dispatch::{dispatch_command, run_default_actions, warn_if_untrusted_rest};
use crate::error::{CliError, CliResult};
use crate::output::{Output, Status};
use crate::resolve::{
    DiffDispatch, SnapshotDispatch, classify_diff_target, classify_snapshot_target, repo_name_at,
    repo_names_at,
};

/// Exit code: hard error (backup failed, config error, etc.).
pub(crate) const EXIT_ERROR: u8 = 1;
/// Exit code: partial success (backup completed but some files were skipped,
/// or `restore --continue-on-error` left out damaged files).
pub(crate) const EXIT_PARTIAL: u8 = 3;
/// Exit code: cooperative shutdown via signal (SIGINT/SIGTERM).
pub(crate) const EXIT_INTERRUPTED: u8 = 130;

pub fn run() -> ExitCode {
    match Cli::try_parse() {
        Ok(cli) => run_cli(cli),
        Err(e) => usage_error(&e),
    }
}

/// Report a command line clap rejected as clap does, plus with `--json` an
/// error document whose `command` is empty, since none could be parsed.
fn usage_error(e: &clap::Error) -> ExitCode {
    use clap::error::ErrorKind;

    let json = std::env::args_os().skip(1).any(|a| a == "--json");
    if !json || matches!(e.kind(), ErrorKind::DisplayHelp | ErrorKind::DisplayVersion) {
        e.exit();
    }
    // Ignored like any other stderr write: the document is what matters here.
    let _ = e.print();
    let mut out = Output::new(true, String::new());
    // `Display` drops clap's ANSI styling.
    let message = e.render().to_string();
    let message = message.trim_end();
    out.set_error(
        message
            .strip_prefix("error: ")
            .unwrap_or(message)
            .to_string(),
    );
    let code = u8::try_from(e.exit_code()).unwrap_or(EXIT_ERROR);
    out.finish(ExitCode::from(code))
}

pub(crate) fn run_cli(cli: Cli) -> ExitCode {
    signal::install_signal_handlers();
    init_logging(&cli);

    let command = cli
        .command
        .as_ref()
        .map_or_else(|| "cycle".to_string(), Commands::full_name);
    let mut out = Output::new(cli.json, command);
    passphrase::set_non_interactive(cli.json);
    let code = match out.divert_stdout() {
        Ok(()) => run_with_output(&cli, &mut out),
        Err(e) => out.fail(format!("could not redirect stdout for --json: {e}")),
    };
    out.finish(code)
}

fn run_with_output(cli: &Cli, out: &mut Output) -> ExitCode {
    let source = match resolve_config_source(cli, out) {
        Ok(s) => s,
        Err(code) => return code,
    };

    let all_repos = match load_repos(cli, &source, out) {
        Ok(r) => r,
        Err(code) => return code,
    };

    let repo_selector = effective_repo_selector(cli);
    let repos: Vec<&ResolvedRepo> = match repo_selector {
        Some(selector) => {
            vec![config::select_repo(&all_repos, selector).expect("repo selector was validated")]
        }
        None => all_repos.iter().collect(),
    };

    if let Some(code) = dispatch_targeted(cli, &repos, out) {
        return code;
    }

    run_all_repos(cli, &repos, out)
}

/// `EnvFilter` directives appended to the `-v`-derived log filter
/// (e.g. `vykar::stats=debug`).
const LOG_FILTER_ENV: &str = "VYKAR_LOG";

/// Tracing filter for this invocation: the `-v`-derived level, followed by
/// `VYKAR_LOG` when it is non-empty.
fn log_filter(verbose: u8, daemon: bool, env_extra: Option<String>) -> String {
    let base = match verbose {
        0 if daemon => "info",
        0 => "warn",
        1 => "info",
        2 => "debug",
        _ => "trace",
    };
    match env_extra.filter(|f| !f.trim().is_empty()) {
        Some(extra) => format!("{base},{extra}"),
        None => base.to_string(),
    }
}

/// Initialize tracing. The daemon runs at `info` by default; everything else
/// stays quiet until `-v` is passed. `VYKAR_LOG` adds directives.
fn init_logging(cli: &Cli) {
    // VYKAR_LOG is an internal measurement aid (e.g. backup pipeline stats
    // without `-v`'s per-file output), deliberately not documented. It comes
    // after the default, so it can add or override directives but a malformed
    // or bare value cannot drop the default level for other targets.
    let filter = log_filter(
        cli.verbose,
        matches!(&cli.command, Some(Commands::Daemon { .. })),
        std::env::var(LOG_FILTER_ENV).ok(),
    );
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
}

/// Effective `--repo` selector: the subcommand's flag wins over the top-level one.
fn effective_repo_selector(cli: &Cli) -> Option<&str> {
    cli.command
        .as_ref()
        .and_then(|cmd| cmd.repo())
        .or(cli.repo.as_deref())
}

/// Run the command against the one repository that holds its target.
fn run_targeted(cli: &Cli, repo: &ResolvedRepo, out: &mut Output) -> ExitCode {
    out.begin_repo(repo);
    let result = run_repo_command(cli, repo, out);
    let status = out.end_repo(&result);
    report_status(status, &result);
    status.exit_code()
}

/// Print the stderr line for a repository command that was interrupted or
/// failed.
fn report_status(status: Status, result: &CliResult<bool>) {
    match (status, result) {
        (Status::Interrupted, _) => eprintln!("Interrupted"),
        (Status::Error, Err(e)) => eprintln!("Error: {e}"),
        _ => {}
    }
}

/// Validate the flags that must be checked before any config is read, run the
/// commands that own their own config lifecycle, and locate the config file for
/// everything else. `Err(code)` means the process is finished.
fn resolve_config_source(cli: &Cli, out: &mut Output) -> Result<config::ConfigSource, ExitCode> {
    // Reject top-level --repo/--source when a subcommand is present.
    // These flags belong on the subcommand itself (e.g. `vykar backup -R repo`).
    if cli.command.is_some() && (cli.repo.is_some() || !cli.source.is_empty()) {
        return Err(out.fail(
            "--repo/--source on the bare command cannot be combined with a subcommand.\n\
             Place -R/--repo and -S/--source after the subcommand instead.",
        ));
    }

    // `daemon` and `mount` serve until stopped, so they have no result to
    // report; the daemon publishes its status as JSON over HTTP instead.
    if let Some(cmd @ (Commands::Daemon { .. } | Commands::Mount { .. })) = &cli.command
        && cli.json
    {
        return Err(out.fail(format!("--json is not supported by `{}`", cmd.name())));
    }

    // Handle `config` subcommand early — no config file needed
    if let Some(Commands::Config { dest }) = &cli.command {
        if let Err(e) = run_config_generate(dest.as_deref(), out) {
            return Err(out.fail(e));
        }
        return Err(ExitCode::SUCCESS);
    }

    // Resolve config file
    let Some(source) = config::resolve_config_path(cli.config.as_deref()) else {
        let mut message = String::from("no configuration file found.\nSearched:");
        for (path, level) in config::default_config_search_paths() {
            message.push_str(&format!("\n  {} ({})", path.display(), level));
        }
        message.push_str("\n\nRun `vykar config` to generate a starter config file.");
        return Err(out.fail(message));
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
            return Err(out.fail("--trust-repo cannot be used with the daemon command"));
        }
        if let Some(addr) = http_listen
            && !addr.ip().is_loopback()
            && !*http_allow_public
        {
            return Err(out.fail(format!(
                "--http-listen {addr} binds to a non-loopback address; \
                     pass --http-allow-public (or set VYKAR_HTTP_ALLOW_PUBLIC=1) to confirm"
            )));
        }
        if let Err(e) = cmd::daemon::run_daemon(source, *http_listen) {
            return Err(out.fail(e));
        }
        return Err(ExitCode::SUCCESS);
    }

    Ok(source)
}

/// Load the configured repositories and apply `--repo` / `--trust-repo`.
/// `Err(code)` means the flags were rejected or the config was unusable.
fn load_repos(
    cli: &Cli,
    source: &config::ConfigSource,
    out: &mut Output,
) -> Result<Vec<ResolvedRepo>, ExitCode> {
    let mut all_repos = match config::load_and_resolve(source.path()) {
        Ok(r) => r,
        Err(e) => return Err(out.fail(e)),
    };

    if all_repos.is_empty() {
        return Err(out.fail(
            "no repositories configured. Edit your config file and add a 'repositories' section.",
        ));
    }

    let repo_selector = effective_repo_selector(cli);

    // --trust-repo validation: must target exactly one repo.
    // Rejected for multi-repo without -R (would silently re-pin unrelated
    // repos during probing/dispatch).
    if cli.trust_repo && repo_selector.is_none() && all_repos.len() > 1 {
        return Err(
            out.fail("--trust-repo requires -R / --repo when multiple repositories are configured")
        );
    }

    // Resolve --repo selector and set --trust-repo on the single targeted repo.
    if let Some(selector) = repo_selector {
        let matches_selector = |r: &ResolvedRepo| {
            r.label.as_deref() == Some(selector) || r.config.repository.url == selector
        };
        if !all_repos.iter().any(matches_selector) {
            let mut message =
                format!("no repository matching '{selector}'\nAvailable repositories:");
            for r in &all_repos {
                let label = r.label.as_deref().unwrap_or("-");
                message.push_str(&format!("\n  {label:12} {}", r.config.repository.url));
            }
            return Err(out.fail(message));
        }
        if cli.trust_repo {
            for repo in all_repos.iter_mut().filter(|r| matches_selector(r)) {
                repo.config.trust_repo = true;
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

    Ok(all_repos)
}

/// Multi-repo dispatch for commands that name a specific snapshot: probe the
/// repositories, pick the one that actually holds it, and run there. Returns
/// `None` when the command should fall through to the run-everywhere path.
fn dispatch_targeted(cli: &Cli, repos: &[&ResolvedRepo], out: &mut Output) -> Option<ExitCode> {
    if repos.len() < 2 {
        return None;
    }

    // A key belongs to exactly one repository. `key export` across several
    // would concatenate armor blocks (to stdout) or overwrite one file (with
    // `-o`), and `key import` carries one repository's key. Refused here,
    // before any repository is opened, so nothing is touched.
    if let Some(cli::Commands::Key { command }) = cli.command.as_ref() {
        return Some(out.fail(format!(
            "`key {}` requires -R / --repo when multiple repositories are \
             configured — a key belongs to exactly one repository.",
            command.name()
        )));
    }

    // Bulk snapshot delete requires -R when multiple repos are configured,
    // since the smart single-snapshot probe cannot handle multiple names.
    if let Some(cli::Commands::Snapshot {
        command: cli::SnapshotCommand::Delete { snapshots, .. },
    }) = cli.command.as_ref()
        && snapshots.len() > 1
    {
        return Some(out.fail(
            "deleting multiple snapshots requires -R / --repo \
                 when multiple repositories are configured",
        ));
    }

    if let Some(cli::Commands::Snapshot {
        command:
            cli::SnapshotCommand::Diff {
                snapshot_a,
                snapshot_b,
            },
    }) = cli.command.as_ref()
    {
        return Some(dispatch_diff(cli, repos, snapshot_a, snapshot_b, out));
    }

    let snap = cli.command.as_ref().and_then(|c| c.snapshot_name())?;
    Some(dispatch_snapshot(cli, repos, snap, out))
}

/// Probe both snapshot names and run the diff in the single repository that
/// holds them both, or explain why that could not be determined.
fn dispatch_diff(
    cli: &Cli,
    repos: &[&ResolvedRepo],
    snapshot_a: &str,
    snapshot_b: &str,
    out: &mut Output,
) -> ExitCode {
    // Emit REST/plaintext warnings before probing backends.
    for repo in repos {
        warn_if_untrusted_rest(&repo.config, repo.label.as_deref());
    }
    match classify_diff_target(snapshot_a, snapshot_b, repos) {
        DiffDispatch::LatestRequiresRepo => out.fail(
            "\'latest\' is ambiguous in snapshot diff when multiple repositories \
                 are configured; rename the snapshot or scope the config",
        ),
        DiffDispatch::SnapshotNotFound { snapshot } => out.fail(format!(
            "snapshot \'{snapshot}\' not found in any configured repository"
        )),
        DiffDispatch::DifferentRepos { a_repo, b_repo } => out.fail(format!(
            "snapshot diff requires both snapshots to live in the same \
                 repository: \'{snapshot_a}\' is in \'{a_repo}\', \'{snapshot_b}\' is in \'{b_repo}\'"
        )),
        DiffDispatch::Ambiguous {
            snapshot,
            repos: rs,
        } => out.fail(format!(
            "snapshot \'{snapshot}\' is present in multiple repositories: {}. \
                 Rename the snapshot or scope the config.",
            repo_names_at(repos, &rs).join(", ")
        )),
        DiffDispatch::ProbeError { errors } => {
            let mut message = String::from("could not probe all repositories");
            for (i, err) in &errors {
                message.push_str(&format!("\n  {}:  {err}", repo_name_at(repos, *i)));
            }
            out.fail(message)
        }
        DiffDispatch::Unique(idx) => match repos.get(idx).copied() {
            Some(repo) => run_targeted(cli, repo, out),
            None => out.fail("internal error: diff target repo index out of range"),
        },
    }
}

/// Probe the repositories for `snap` and run the command in the one that holds
/// it, or explain why that could not be determined.
fn dispatch_snapshot(cli: &Cli, repos: &[&ResolvedRepo], snap: &str, out: &mut Output) -> ExitCode {
    // Emit REST/plaintext warnings before probing backends.
    for repo in repos {
        warn_if_untrusted_rest(&repo.config, repo.label.as_deref());
    }

    match classify_snapshot_target(snap, repos) {
        SnapshotDispatch::RequireRepo => {
            out.fail("\'latest\' requires -R / --repo when multiple repositories are configured")
        }
        SnapshotDispatch::NotFound => out.fail(format!(
            "snapshot \'{snap}\' not found in any configured repository"
        )),
        // Single match — dispatch without banner
        SnapshotDispatch::Unique(idx) => match repos.get(idx).copied() {
            Some(repo) => run_targeted(cli, repo, out),
            None => out.fail("internal error: snapshot dispatch index out of range"),
        },
        SnapshotDispatch::Ambiguous(indices) => out.fail(format!(
            "snapshot \'{snap}\' found in multiple repositories: {}. \
                 Use -R / --repo to select one.",
            repo_names_at(repos, &indices).join(", ")
        )),
        SnapshotDispatch::ProbeError { matches, errors } => {
            let mut message = String::from("could not probe all repositories");
            for (i, err) in &errors {
                message.push_str(&format!("\n  {}:  {err}", repo_name_at(repos, *i)));
            }
            for i in &matches {
                message.push_str(&format!(
                    "\n  {}:  found \'{snap}\'",
                    repo_name_at(repos, *i)
                ));
            }
            message.push_str("\nUse -R / --repo to target a specific repository.");
            out.fail(message)
        }
    }
}

/// Run the command against every selected repository, aggregating the outcome:
/// any hard error wins over a partial, which wins over success.
fn run_all_repos(cli: &Cli, repos: &[&ResolvedRepo], out: &mut Output) -> ExitCode {
    let multi = repos.len() > 1;
    let mut had_error = false;
    let mut had_partial = false;

    for repo in repos {
        if signal::SHUTDOWN.load(Ordering::SeqCst) {
            break;
        }

        if multi {
            eprintln!("--- Repository: {} ---", repo.label_or_url());
        }

        out.begin_repo(repo);
        let result = run_repo_command(cli, repo, out);
        let status = out.end_repo(&result);
        report_status(status, &result);
        match status {
            Status::Interrupted => return status.exit_code(),
            Status::Error if !multi => return status.exit_code(),
            Status::Error => had_error = true,
            Status::Partial => had_partial = true,
            Status::Ok | Status::Skipped => {}
        }
    }

    let status = if signal::SHUTDOWN.load(Ordering::SeqCst) {
        eprintln!("Interrupted");
        Status::Interrupted
    } else if had_error {
        Status::Error
    } else if had_partial {
        Status::Partial
    } else {
        Status::Ok
    };
    status.exit_code()
}

/// Run `action` inside the repo-level hooks for `command`. The CLI error is
/// flattened to `VykarError::Other` to fit the core signature; only its text
/// is used downstream, so the lost downcast info is immaterial.
fn run_hooked(
    repo: &ResolvedRepo,
    command: &str,
    action: impl FnOnce() -> CliResult<bool>,
) -> CliResult<bool> {
    operations::run_command_with_hooks(repo, command, || {
        action().map_err(|e| vykar_types::error::VykarError::Other(e.to_string()))
    })
    .map_err(CliError::from)
}

/// Execute the CLI command (or default actions) against one repo.
/// Returns `Ok(had_partial)` where `true` means backup had soft errors. A skip
/// requested by a hook, or a bare cycle skipped because its local repository is
/// unavailable, is reported here and counts as success.
fn run_repo_command(cli: &Cli, repo: &ResolvedRepo, out: &mut Output) -> CliResult<bool> {
    warn_if_untrusted_rest(&repo.config, repo.label.as_deref());

    let shutdown = signal::shutdown_token();
    let result = match &cli.command {
        Some(cmd) => {
            if matches!(cmd, Commands::Backup { .. }) {
                // Backup: hooks handled by run_backup_selection in core
                dispatch_command(cmd, repo, shutdown, cli.verbose, out)
            } else {
                let result = run_hooked(repo, cmd.name(), || {
                    dispatch_command(cmd, repo, shutdown, cli.verbose, out)
                });
                match (result, cmd) {
                    // Compact runs as its own hooked command once prune's lifecycle
                    // has ended, as in the default cycle. A failed or skipped prune
                    // does not compact, nor does one interrupted by a shutdown.
                    (
                        Ok(partial),
                        Commands::Prune {
                            compact: true,
                            dry_run,
                            ..
                        },
                    ) if !signal::SHUTDOWN.load(Ordering::SeqCst) => {
                        run_hooked(repo, "compact", || {
                            crate::cmd::compact::run_compact(
                                &repo.config,
                                repo.label.as_deref(),
                                repo.config.compact.threshold,
                                None,
                                *dry_run,
                                shutdown,
                                out,
                            )
                            .map(|()| partial)
                        })
                    }
                    (other, _) => other,
                }
            }
        }
        None => {
            // An explicitly selected repository runs and fails if it is missing.
            let unavailable = if effective_repo_selector(cli).is_some() {
                UnavailableRepo::Attempt
            } else {
                UnavailableRepo::Skip
            };
            run_default_actions(repo, shutdown, cli.verbose, &cli.source, unavailable, out).map(
                |o| {
                    if let Some(reason) = &o.skipped {
                        if o.unavailable {
                            eprintln!("Warning: skipping '{}' — {reason}", repo.label_or_url());
                        }
                        out.skip_repo(reason);
                    }
                    o.partial
                },
            )
        }
    };

    match result {
        Err(CliError::Vykar(vykar_types::error::VykarError::Skipped(reason))) => {
            eprintln!("Skipped: {reason}");
            out.skip_repo(&reason);
            Ok(false)
        }
        other => other,
    }
}

#[cfg(test)]
mod tests {
    use super::log_filter;

    #[test]
    fn log_filter_appends_non_empty_env() {
        assert_eq!(log_filter(0, false, None), "warn");
        assert_eq!(log_filter(0, true, None), "info");
        assert_eq!(log_filter(2, false, None), "debug");
        assert_eq!(log_filter(5, false, None), "trace");
        assert_eq!(log_filter(1, false, Some(String::new())), "info");
        assert_eq!(log_filter(1, false, Some("  ".into())), "info");
        assert_eq!(
            log_filter(0, false, Some("vykar::stats=debug".into())),
            "warn,vykar::stats=debug"
        );
        assert_eq!(log_filter(0, true, Some("verbose".into())), "info,verbose");
    }

    /// A bare word, a target-only directive or a malformed value must not
    /// silence warnings from other targets.
    #[test]
    fn log_filter_env_keeps_default_level() {
        use tracing::Level;
        use tracing_subscriber::EnvFilter;
        use tracing_subscriber::layer::SubscriberExt;

        for extra in ["verbose", "vykar::stats=debug", "foo=bar"] {
            let filter = EnvFilter::new(log_filter(0, false, Some(extra.into())));
            let subscriber = tracing_subscriber::registry().with(filter);
            tracing::subscriber::with_default(subscriber, || {
                assert!(
                    tracing::enabled!(target: "vykar_core::other", Level::WARN),
                    "{extra}"
                );
                assert!(
                    !tracing::enabled!(target: "vykar_core::other", Level::INFO),
                    "{extra}"
                );
            });
        }
    }
}
