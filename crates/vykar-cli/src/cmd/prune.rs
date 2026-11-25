use serde::Serialize;

use vykar_core::commands;
use vykar_core::commands::prune::{PruneListEntry, PruneStats};
use vykar_core::config::{SourceEntry, VykarConfig};
use vykar_core::repo::control::CancelToken;

use crate::error::CliResult;
use crate::output::{Output, Report};
use crate::passphrase::with_repo_passphrase;

#[allow(clippy::too_many_arguments)]
pub(crate) fn run_prune(
    config: &VykarConfig,
    label: Option<&str>,
    dry_run: bool,
    list: bool,
    sources: &[SourceEntry],
    source_filter: &[String],
    shutdown: &CancelToken,
    out: &mut Output,
) -> CliResult<()> {
    let (stats, list_entries) = with_repo_passphrase(config, label, |passphrase| {
        Ok(commands::prune::run(
            config,
            passphrase,
            dry_run,
            list || out.is_json(),
            sources,
            source_filter,
            shutdown,
        )?)
    })?;

    out.emit(&PruneReport {
        stats,
        list_entries,
        dry_run,
        list,
    })
}

struct PruneReport {
    stats: PruneStats,
    list_entries: Vec<PruneListEntry>,
    dry_run: bool,
    list: bool,
}

#[derive(Serialize)]
pub(crate) struct PruneJson<'a> {
    dry_run: bool,
    kept: usize,
    pruned: usize,
    /// Every snapshot with its decision; empty in the full-cycle summary.
    snapshots: Vec<PruneEntryJson<'a>>,
    warnings: &'a [String],
}

#[derive(Serialize)]
struct PruneEntryJson<'a> {
    name: &'a str,
    action: &'a str,
    reasons: &'a [String],
}

impl<'a> PruneJson<'a> {
    /// The counts alone, as the full cycle reports them.
    pub(crate) fn summary(stats: &'a PruneStats) -> Self {
        Self {
            dry_run: false,
            kept: stats.kept,
            pruned: stats.pruned,
            snapshots: Vec::new(),
            warnings: &stats.warnings,
        }
    }
}

impl Report for PruneReport {
    const KEY: &'static str = "prune";

    fn json(&self) -> impl Serialize {
        PruneJson {
            dry_run: self.dry_run,
            snapshots: self
                .list_entries
                .iter()
                .map(|entry| PruneEntryJson {
                    name: &entry.snapshot_name,
                    action: &entry.action,
                    reasons: &entry.reasons,
                })
                .collect(),
            ..PruneJson::summary(&self.stats)
        }
    }

    fn print_human(&self) {
        let (stats, dry_run) = (&self.stats, self.dry_run);
        if self.list || dry_run {
            for entry in &self.list_entries {
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
            print_prune_summary(stats);
            if stats.pruned > 0 {
                println!("Run `vykar compact` to reclaim their space.");
            }
        }

        for w in &stats.warnings {
            eprintln!("warning: {w}");
        }
    }
}

/// The one-line prune result, shared with the full-cycle summary in `dispatch`.
pub(crate) fn print_prune_summary(stats: &commands::prune::PruneStats) {
    println!("Pruned {} snapshots (kept {})", stats.pruned, stats.kept);
}
