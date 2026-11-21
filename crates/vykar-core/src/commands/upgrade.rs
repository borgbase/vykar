//! `vykar upgrade`: move a repository to the current format in place.

use super::util::with_open_repo_maintenance_lock;
use crate::config::VykarConfig;
use crate::repo::{OpenOptions, UpgradeOutcome};
use vykar_types::error::Result;

/// Upgrade the repository's index to the segmented layout (v2 → v4,
/// v3 → v5) under the maintenance lock, or complete an interrupted upgrade.
/// The only command that opens a v2/v3 repository; it opens it without its
/// index, and the upgrade reads the legacy index itself.
pub fn run(config: &VykarConfig, passphrase: Option<&str>) -> Result<UpgradeOutcome> {
    let opts = OpenOptions::new().legacy_format();
    with_open_repo_maintenance_lock(config, passphrase, opts, |repo| repo.upgrade_index_layout())
}
