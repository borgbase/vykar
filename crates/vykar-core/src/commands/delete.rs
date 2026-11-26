use std::collections::HashSet;

use vykar_types::error::Result;

use super::snapshot_ops::{prepare_removal_targets, remove_snapshots};
use super::util::{open_repo, refuse_on_append_only_server, with_snapshot_removal_exclusion};
use crate::repo::control::CancelToken;
use crate::repo::{OpenOptions, Repo};

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
    cancel: &CancelToken,
) -> Result<DeleteResult> {
    // The backend observes `cancel` in its retry waits. The phases past
    // the commit point do not poll it, so after a cancel their requests get
    // one attempt each and no retries, and on SFTP a read in flight is
    // abandoned; they are crash-safe by design.
    let mut repo = open_repo(config, passphrase, OpenOptions::new(), cancel)?;
    run_on_repo(&mut repo, snapshot_names, dry_run, cancel)
}

/// [`run`] on an already opened repository, under the maintenance lock or,
/// lock-free, a registered session (`with_snapshot_removal_exclusion`). A
/// dry run only resolves the names.
pub(crate) fn run_on_repo(
    repo: &mut Repo,
    snapshot_names: &[&str],
    dry_run: bool,
    cancel: &CancelToken,
) -> Result<DeleteResult> {
    // Phase 0: Deduplicate while preserving order.
    let mut seen = HashSet::new();
    let unique_names: Vec<&str> = snapshot_names
        .iter()
        .copied()
        .filter(|name| seen.insert(*name))
        .collect();

    with_snapshot_removal_exclusion(repo, |repo| {
        let targets = prepare_removal_targets(repo, &unique_names)?;
        let names: Vec<String> = if dry_run {
            targets.into_iter().map(|t| t.snapshot_name).collect()
        } else {
            if !targets.is_empty() {
                refuse_on_append_only_server(repo.storage().as_ref(), "vykar snapshot delete")?;
            }
            remove_snapshots(repo, targets, cancel)?
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
