use std::collections::HashMap;

use crate::config::{RetentionConfig, SourceEntry, VykarConfig};
use crate::prune::{PruneDecision, apply_policy_by_label};
use crate::repo::control::CancelToken;
use vykar_types::error::{Result, VykarError};

use super::snapshot_ops::{prepare_removal_targets, remove_snapshots};
use super::util::{open_repo, refuse_on_append_only_server, with_snapshot_removal_session};
use crate::repo::{OpenOptions, Repo};

/// Aggregate result of a prune operation.
///
/// Prune only deletes `snapshots/<id>` objects (the commit point of each
/// removal); it does not read or write the index. The space the pruned
/// snapshots alone used is reclaimed by the next `vykar compact`.
pub struct PruneStats {
    pub kept: usize,
    pub pruned: usize,
    pub warnings: Vec<String>,
}

/// Formatted list entry for --list output.
pub struct PruneListEntry {
    pub action: String,
    pub snapshot_name: String,
    pub reasons: Vec<String>,
}

pub fn run(
    config: &VykarConfig,
    passphrase: Option<&str>,
    dry_run: bool,
    list: bool,
    sources: &[SourceEntry],
    source_filter: &[String],
    cancel: &CancelToken,
) -> Result<(PruneStats, Vec<PruneListEntry>)> {
    // The backend observes `cancel` in its retry waits. The phases past
    // the commit point do not poll it, so after a cancel their requests get
    // one attempt each and no retries, and on SFTP a read in flight is
    // abandoned; they are crash-safe by design.
    let mut repo = open_repo(config, passphrase, OpenOptions::new(), cancel)?;
    run_on_repo(
        &mut repo,
        config,
        dry_run,
        list,
        sources,
        source_filter,
        cancel,
    )
}

/// [`run`] on an already opened repository, in a registered session
/// (`with_snapshot_removal_session`).
pub(crate) fn run_on_repo(
    repo: &mut Repo,
    config: &VykarConfig,
    dry_run: bool,
    list: bool,
    sources: &[SourceEntry],
    source_filter: &[String],
    cancel: &CancelToken,
) -> Result<(PruneStats, Vec<PruneListEntry>)> {
    with_snapshot_removal_session(repo, |repo| {
        let now = repo.host_env().now_utc();

        // When --source is given, restrict to matching snapshots only
        let target_snapshots = if source_filter.is_empty() {
            repo.manifest().snapshots.clone()
        } else {
            repo.manifest()
                .snapshots
                .iter()
                .filter(|e| source_filter.contains(&e.source_label))
                .cloned()
                .collect()
        };

        // Build per-source retention map
        let source_retentions: HashMap<String, RetentionConfig> = sources
            .iter()
            .filter_map(|s| s.retention.as_ref().map(|r| (s.label.clone(), r.clone())))
            .collect();

        // Always group by source_label — labels are intrinsic to each
        // snapshot (recorded at backup time), so retention must apply
        // per-label regardless of whether the local config has a
        // `sources:` block. (Issue #138.)
        if !config.retention.has_any_rule() && source_retentions.values().all(|r| !r.has_any_rule())
        {
            return Err(VykarError::Config(
                "no retention rules configured — set at least one keep_* option in the retention section or per-source".into(),
            ));
        }

        let decisions = apply_policy_by_label(
            &target_snapshots,
            &config.retention,
            &source_retentions,
            now,
        )?;

        // Build list output
        let mut list_entries = Vec::new();
        let mut kept = 0usize;
        let mut to_prune: Vec<String> = Vec::new();

        for entry in &decisions {
            match &entry.decision {
                PruneDecision::Keep { reasons } => {
                    kept += 1;
                    if list || dry_run {
                        list_entries.push(PruneListEntry {
                            action: "keep".into(),
                            snapshot_name: entry.snapshot_name.clone(),
                            reasons: reasons.clone(),
                        });
                    }
                }
                PruneDecision::Prune => {
                    to_prune.push(entry.snapshot_name.clone());
                    if list || dry_run {
                        list_entries.push(PruneListEntry {
                            action: "prune".into(),
                            snapshot_name: entry.snapshot_name.clone(),
                            reasons: Vec::new(),
                        });
                    }
                }
            }
        }

        if dry_run {
            return Ok((
                PruneStats {
                    kept,
                    pruned: to_prune.len(),
                    warnings: Vec::new(),
                },
                list_entries,
            ));
        }

        if !to_prune.is_empty() {
            refuse_on_append_only_server(repo.storage().as_ref(), "vykar prune")?;
        }

        // Process oldest first
        to_prune.reverse();

        let targets = prepare_removal_targets(repo, &to_prune)?;
        let removed = remove_snapshots(repo, targets, cancel)?;

        Ok((
            PruneStats {
                kept,
                pruned: removed.len(),
                warnings: Vec::new(),
            },
            list_entries,
        ))
    })
}
