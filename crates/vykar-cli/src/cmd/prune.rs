use vykar_core::commands;
use vykar_core::config::{SourceEntry, VykarConfig};
use vykar_core::repo::control::CancelToken;

use crate::error::CliResult;
use vykar_core::app::passphrase::RepoPassphrase;

pub(crate) fn run_prune(
    config: &VykarConfig,
    dry_run: bool,
    list: bool,
    sources: &[SourceEntry],
    source_filter: &[String],
    cancel: &CancelToken,
    passphrase: &mut RepoPassphrase<'_>,
) -> CliResult<()> {
    let (stats, list_entries) = passphrase.with(|passphrase| -> CliResult<_> {
        Ok(commands::prune::run(
            config,
            passphrase,
            dry_run,
            list,
            sources,
            source_filter,
            cancel,
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
        if stats.pruned > 0 {
            println!("Run `vykar compact` to reclaim their space.");
        }
    }

    for w in &stats.warnings {
        eprintln!("warning: {w}");
    }

    Ok(())
}

/// The one-line prune result, shared with the full-cycle summary in `dispatch`.
pub(crate) fn print_prune_summary(stats: &commands::prune::PruneStats) {
    println!("Pruned {} snapshots (kept {})", stats.pruned, stats.kept);
}
