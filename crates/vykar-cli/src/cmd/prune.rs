use vykar_core::commands;
use vykar_core::config::{SourceEntry, VykarConfig};
use vykar_core::repo::control::CancelToken;

use crate::error::CliResult;
use crate::format::format_bytes;
use crate::passphrase::with_repo_passphrase;

pub(crate) fn run_prune(
    config: &VykarConfig,
    label: Option<&str>,
    dry_run: bool,
    list: bool,
    sources: &[SourceEntry],
    source_filter: &[String],
    shutdown: &CancelToken,
) -> CliResult<()> {
    let (stats, list_entries) = with_repo_passphrase(config, label, |passphrase| {
        Ok(commands::prune::run(
            config,
            passphrase,
            dry_run,
            list,
            sources,
            source_filter,
            shutdown,
        )?)
    })?;

    if list || dry_run {
        for entry in &list_entries {
            if entry.reasons.is_empty() {
                println!("{:<6} {}", entry.action, entry.snapshot_name);
            } else {
                println!(
                    "{:<6} {}  [{}]",
                    entry.action,
                    entry.snapshot_name,
                    entry.reasons.join(", "),
                );
            }
        }
        println!();
    }

    if dry_run {
        println!(
            "Dry run: would keep {} and prune {} snapshots",
            stats.kept, stats.pruned,
        );
    } else {
        print_prune_summary(&stats);
    }

    for w in &stats.warnings {
        eprintln!("warning: {w}");
    }

    Ok(())
}

/// The one-line prune result, shared with the full-cycle summary in `dispatch`.
pub(crate) fn print_prune_summary(stats: &commands::prune::PruneStats) {
    println!(
        "Pruned {} snapshots (kept {}), freed {} chunks ({})",
        stats.pruned,
        stats.kept,
        stats.chunks_deleted,
        format_bytes(stats.space_freed),
    );
}
