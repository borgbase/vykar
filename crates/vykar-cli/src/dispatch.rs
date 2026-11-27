use vykar_core::app::AppService;
use vykar_core::app::passphrase::RepoPassphrase;
use vykar_core::config::{ResolvedRepo, VykarConfig};
use vykar_core::repo::control::CancelToken;
use vykar_storage::{ParsedUrl, parse_repo_url};

use crate::cli::{Commands, KeyCommand};
use crate::cmd;
use crate::cmd::backup::BackupRunOpts;
use crate::error::{CliError, CliResult};
use crate::json::Output;

pub(crate) fn warn_if_untrusted_rest(config: &VykarConfig, label: Option<&str>) {
    let Ok(parsed) = parse_repo_url(&config.repository.url) else {
        return;
    };
    let ParsedUrl::Rest { url } = parsed else {
        return;
    };

    let repo_name = label.unwrap_or(&config.repository.url);
    if url.starts_with("http://") {
        eprintln!(
            "Warning: repository '{repo_name}' uses non-HTTPS REST URL '{url}'. Transport is not TLS-protected."
        );
    }
}

/// Backup: hooks, passphrase and sources through the app service. Returns
/// `Ok(had_partial)` — `true` if backup had soft errors but still succeeded.
pub(crate) fn dispatch_backup(
    command: &Commands,
    repo: &ResolvedRepo,
    service: &mut AppService,
    cancel: &CancelToken,
    verbose: u8,
    out: Output<'_>,
) -> CliResult<bool> {
    let Commands::Backup {
        label: user_label,
        compression,
        connections,
        threads,
        source,
        paths,
        ..
    } = command
    else {
        return Err(CliError::from("dispatch_backup called for another command"));
    };
    cmd::backup::run_backup(
        service,
        repo,
        BackupRunOpts {
            user_label: user_label.clone(),
            compression_override: compression.clone(),
            connections: connections.map(usize::from),
            threads: threads.map(usize::from),
            paths: paths.clone(),
            source_filter: source,
            cancel,
            verbose,
        },
        out,
    )
}

/// Every other per-repository command. `passphrase` is lazy: commands that
/// need none (init, delete, clear-sessions) never resolve it. Returns
/// `Ok(had_partial)`. `out` reaches list, info, prune, compact and check;
/// the other commands never see `Output::Json` (`--json` refuses them).
pub(crate) fn dispatch_command(
    command: &Commands,
    repo: &ResolvedRepo,
    passphrase: &mut RepoPassphrase<'_>,
    cancel: &CancelToken,
    out: Output<'_>,
) -> CliResult<bool> {
    let cfg = &repo.config;
    let label = repo.label.as_deref();
    let sources = &repo.sources;

    match command {
        Commands::Init { .. } => cmd::init::run_init(cfg, label).map(|()| false),
        Commands::Backup { .. } => Err(CliError::from(
            "'backup' is dispatched through the app service",
        )),
        Commands::List { source, last, .. } => {
            cmd::list::run_list(source, *last, cancel, passphrase, out).map(|()| false)
        }
        Commands::Snapshot { command, .. } => {
            cmd::snapshot::run_snapshot_command(command, cancel, passphrase).map(|()| false)
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
            snapshot.clone(),
            dest.clone(),
            pattern.clone(),
            *verify,
            *continue_on_error,
            cancel,
            passphrase,
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
        } => cmd::prune::run_prune(
            cfg, *dry_run, *list, sources, source, cancel, passphrase, out,
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
            *verify_data,
            *distrust_server,
            *repair,
            *dry_run,
            *yes,
            cancel,
            passphrase,
            out,
        )
        .map(|()| false),
        Commands::Info { .. } => cmd::info::run_info(cfg, label, passphrase, out).map(|()| false),
        Commands::Key { command, .. } => match command {
            KeyCommand::Export { output, .. } => {
                cmd::key::run_key_export(cfg, output.as_deref(), passphrase).map(|()| false)
            }
            KeyCommand::Import { file, force, .. } => {
                cmd::key::run_key_import(cfg, file, *force, passphrase).map(|()| false)
            }
        },
        Commands::Mount {
            snapshot,
            source,
            address,
            cache_size,
            ..
        } => cmd::mount::run_mount(
            snapshot.as_deref(),
            address,
            *cache_size,
            source,
            cancel,
            passphrase,
        )
        .map(|()| false),
        Commands::ClearSessions { stale, .. } => {
            cmd::clear_sessions::run_clear_sessions(cfg, label, *stale).map(|()| false)
        }
        Commands::Compact {
            report_unreferenced: true,
            ..
        } => cmd::compact::run_report_unreferenced(cancel, passphrase).map(|()| false),
        Commands::Compact {
            threshold,
            max_repack_size,
            dry_run,
            ..
        } => {
            let t = threshold.unwrap_or(cfg.compact.threshold);
            cmd::compact::run_compact(
                cfg,
                t,
                max_repack_size.clone(),
                *dry_run,
                cancel,
                passphrase,
                out,
            )
            .map(|()| false)
        }
        Commands::Upgrade {
            yes, commit_mode, ..
        } => cmd::upgrade::run_upgrade(cfg, *yes, commit_mode.map(Into::into), passphrase)
            .map(|()| false),
        Commands::Config { .. } => Err(CliError::from(
            "'config' command should be handled before config resolution",
        )),
        Commands::Daemon { .. } => Err(CliError::from(
            "'daemon' command should be handled before per-repo dispatch",
        )),
    }
}
