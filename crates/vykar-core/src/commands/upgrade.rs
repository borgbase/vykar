//! `vykar upgrade`: move a repository to the current format in place.

use super::util::with_open_repo_maintenance_lock;
use crate::config::VykarConfig;
use crate::repo::{OpenOptions, UpgradeOutcome};
use vykar_types::error::Result;

/// Upgrade the repository's index to the segmented layout (v2 → v4,
/// v3 → v5) under the maintenance lock, or complete an interrupted upgrade.
/// The repository is opened without its index; the upgrade loads it.
pub fn run(config: &VykarConfig, passphrase: Option<&str>) -> Result<UpgradeOutcome> {
    with_open_repo_maintenance_lock(config, passphrase, OpenOptions::new(), |repo| {
        repo.upgrade_index_layout()
    })
}
