use vykar_core::commands;
use vykar_core::commands::prune::PruneRequest;
use vykar_core::config::{SourceEntry, VykarConfig};
use vykar_core::repo::OpenOptions;
use vykar_core::repo::control::CancelToken;

use crate::error::{CliError, CliResult};
use crate::json::{JsonResult, Output, PruneJson};
use vykar_core::app::passphrase::RepoPassphrase;

#[allow(clippy::too_many_arguments)]
pub(crate) fn run_prune(
    config: &VykarConfig,
    dry_run: bool,
    list: bool,
    sources: &[SourceEntry],
    source_filter: &[String],
    cancel: &CancelToken,
    passphrase: &mut RepoPassphrase<'_>,
    mut out: Output<'_>,
) -> CliResult<()> {
    let mut repo = passphrase.open::<CliError>(OpenOptions::new(), cancel)?;
    let (stats, list_entries) = commands::prune::run_on_repo(
        &mut repo,
        config,
        &PruneRequest {
            dry_run,
            // The document always lists every decision.
            list: list || out.is_json(),
            sources,
            source_filter,
        },
        cancel,
    )?;
    // Closed before the summary, so no handle is open while `after` hooks run.
    drop(repo);

    if out.is_json() {
        for w in &stats.warnings {
            eprintln!("warning: {w}");
        }
        out.keep(JsonResult::Prune(PruneJson {
            dry_run,
            stats,
            entries: list_entries,
            compact_stats: None,
            compact_outcome: None,
        }));
        return Ok(());
    }

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
