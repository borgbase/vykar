use std::collections::HashSet;

use vykar_types::error::Result;

use super::snapshot_ops::{prepare_removal_targets, remove_snapshots};
use super::util::{open_repo, refuse_on_append_only_server, with_maintenance_lock};
use crate::repo::control::CancelToken;
use crate::repo::{OpenOptions, Repository};

pub struct DeleteStats {
    pub snapshot_name: String,
}

/// Aggregate result of a delete operation.
///
/// Delete only deletes `snapshots/<id>` objects (the commit point of each
/// removal); it does not read or write the index. The space the deleted
/// snapshots alone used is reclaimed by the next `vykar compact`.
pub struct DeleteResult {
    pub stats: Vec<DeleteStats>,
    pub warnings: Vec<String>,
}

pub fn run(
    config: &crate::config::VykarConfig,
    passphrase: Option<&str>,
    snapshot_names: &[&str],
    dry_run: bool,
    shutdown: &CancelToken,
) -> Result<DeleteResult> {
    // The backend observes `shutdown` in its retry waits. The phases past
    // the commit point do not poll it, so after a cancel their requests get
    // one attempt each and no retries, and on SFTP a read in flight is
    // abandoned; they are crash-safe by design.
    let mut repo = open_repo(config, passphrase, OpenOptions::new(), shutdown)?;
    run_on_repo(&mut repo, snapshot_names, dry_run, shutdown)
}

/// [`run`] on an already opened repository, under the maintenance lock. A
/// dry run only resolves the names.
pub(crate) fn run_on_repo(
    repo: &mut Repository,
    snapshot_names: &[&str],
    dry_run: bool,
    shutdown: &CancelToken,
) -> Result<DeleteResult> {
    // Phase 0: Deduplicate while preserving order.
    let mut seen = HashSet::new();
    let unique_names: Vec<&str> = snapshot_names
        .iter()
        .copied()
        .filter(|name| seen.insert(*name))
        .collect();

    with_maintenance_lock(repo, |repo| {
        let targets = prepare_removal_targets(repo, &unique_names)?;
        let names: Vec<String> = if dry_run {
            targets.into_iter().map(|t| t.snapshot_name).collect()
        } else {
            if !targets.is_empty() {
                refuse_on_append_only_server(repo.storage.as_ref(), "vykar snapshot delete")?;
            }
            remove_snapshots(repo, targets, shutdown)?
        };
        Ok(DeleteResult {
            stats: names
                .into_iter()
                .map(|snapshot_name| DeleteStats { snapshot_name })
                .collect(),
            warnings: Vec::new(),
        })
    })
}
