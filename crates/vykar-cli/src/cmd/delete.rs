use std::io::IsTerminal;

use vykar_core::commands;
use vykar_core::config::VykarConfig;
use vykar_core::repo::OpenOptions;
use vykar_core::repo::control::CancelToken;

use crate::error::{CliError, CliResult};
use vykar_core::app::passphrase::RepoPassphrase;

pub(crate) fn run_delete(
    snapshot_names: &[String],
    dry_run: bool,
    cancel: &CancelToken,
    passphrase: &mut RepoPassphrase<'_>,
) -> CliResult<()> {
    let name_refs: Vec<&str> = snapshot_names.iter().map(|s| s.as_str()).collect();
    let mut repo = passphrase.open::<CliError>(OpenOptions::new(), cancel)?;
    let result = commands::delete::run_on_repo(&mut repo, &name_refs, dry_run, cancel)?;
    drop(repo);

    for stats in &result.stats {
        if dry_run {
            println!("Dry run: would delete snapshot '{}'", stats.snapshot_name);
        } else {
            println!("Deleted snapshot '{}'", stats.snapshot_name);
        }
    }
    if !dry_run && !result.stats.is_empty() {
        println!("Run `vykar compact` to reclaim their space.");
    }

    for w in &result.warnings {
        eprintln!("warning: {w}");
    }

    Ok(())
}

pub(crate) fn run_delete_repo(
    config: &VykarConfig,
    label: Option<&str>,
    yes_delete_this_repo: bool,
) -> CliResult<()> {
    // Verify the repo exists before prompting — use list + is_known_repo_key
    // so we detect partially-deleted repos (e.g. config gone but packs remain).
    let backend = vykar_core::storage::backend_from_config(
        &config
            .repository
            .storage_config(config.limits.connections)
            .with_cancel(crate::signal::shutdown_token().clone()),
    )?;
    let all_keys = backend.list("")?;
    let has_repo_keys = all_keys
        .iter()
        .any(|k| commands::delete_repo::is_known_repo_key(k));
    if !has_repo_keys {
        return Err(CliError::from(format!(
            "no repository found at '{}'",
            config.repository.url
        )));
    }
    drop(backend);

    if !yes_delete_this_repo {
        if !std::io::stdin().is_terminal() {
            return Err(CliError::from(
                "refusing to delete repository without confirmation in non-interactive mode; \
                 use --yes-delete-this-repo to skip the prompt",
            ));
        }

        let repo_name = label.unwrap_or(&config.repository.url);
        eprintln!(
            "WARNING: This will permanently delete the entire repository '{repo_name}' \
             and ALL its snapshots."
        );
        eprintln!();
        eprint!("Type 'delete' to confirm: ");
        std::io::Write::flush(&mut std::io::stderr())?;

        let mut input = String::new();
        std::io::stdin().read_line(&mut input)?;
        if input.trim() != "delete" {
            eprintln!("Aborted.");
            return Ok(());
        }
    }

    let stats = commands::delete_repo::run(config)?;

    let repo_name = label.unwrap_or(&config.repository.url);

    if stats.unknown_entries.is_empty() {
        println!(
            "Repository '{repo_name}' deleted ({} keys removed).",
            stats.keys_deleted
        );
    } else {
        println!(
            "Repository '{repo_name}' deleted ({} keys removed, {} unknown entries left).",
            stats.keys_deleted,
            stats.unknown_entries.len()
        );
        eprintln!();
        eprintln!(
            "Warning: {} unknown entries were not removed:",
            stats.unknown_entries.len()
        );
        for entry in &stats.unknown_entries {
            eprintln!("  {entry}");
        }
    }

    if stats.is_local && !stats.root_removed && stats.unknown_entries.is_empty() {
        eprintln!(
            "Note: repository directory '{}' could not be fully removed; \
             it may contain empty directories, other non-file entries, or temp-named \
             files ('.tmp.*') in directories vykar does not manage.",
            config.repository.url
        );
    }

    Ok(())
}
