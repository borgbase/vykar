use std::io::IsTerminal;
use std::sync::atomic::AtomicBool;

use vykar_core::commands;
use vykar_core::config::VykarConfig;

use crate::error::{CliError, CliResult};
use crate::format::format_bytes;
use crate::passphrase::with_repo_passphrase;

pub(crate) fn run_delete(
    config: &VykarConfig,
    label: Option<&str>,
    snapshot_names: &[String],
    dry_run: bool,
    shutdown: Option<&AtomicBool>,
) -> CliResult<()> {
    let name_refs: Vec<&str> = snapshot_names.iter().map(|s| s.as_str()).collect();
    let result = with_repo_passphrase(config, label, |passphrase| {
        Ok(commands::delete::run(
            config, passphrase, &name_refs, dry_run, shutdown,
        )?)
    })?;

    for stats in &result.stats {
        if dry_run {
            println!("Dry run: would delete snapshot '{}'", stats.snapshot_name);
            println!(
                "  Would free: {} chunks, {}",
                stats.chunks_deleted,
                format_bytes(stats.space_freed),
            );
        } else {
            println!("Deleted snapshot '{}'", stats.snapshot_name);
            println!(
                "  Freed: {} chunks, {}",
                stats.chunks_deleted,
                format_bytes(stats.space_freed),
            );
        }
    }

    if result.stats.len() > 1 {
        let total_chunks: u64 = result.stats.iter().map(|s| s.chunks_deleted).sum();
        let total_space: u64 = result.stats.iter().map(|s| s.space_freed).sum();
        if dry_run {
            println!(
                "Total: would free {} chunks, {}",
                total_chunks,
                format_bytes(total_space),
            );
        } else {
            println!(
                "Total: freed {} chunks, {}",
                total_chunks,
                format_bytes(total_space),
            );
        }
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
    let backend =
        vykar_core::storage::backend_from_config(&config.repository, config.limits.connections)?;
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
             it may contain empty directories or other non-file entries.",
            config.repository.url
        );
    }

    Ok(())
}
