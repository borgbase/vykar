//! Writing pack maps for the packs that carry no trailer.

use super::util::{open_repo, with_maintenance_lock};
use crate::config::VykarConfig;
use crate::repo::OpenOptions;
use crate::repo::control::CancelToken;
use vykar_types::error::Result;

pub use crate::repo::pack_map::PackMapStats;

/// Write pack maps for every pack the index names that no readable map
/// covers (packs written without a trailer, before format v6 support).
/// Takes the maintenance lock in both commit modes (04 step 9 leaves it
/// locked: a lock-free compact deletes maps none of whose packs it saw
/// alive, so maps are written only where the handshake keeps such a compact
/// out). No CLI command runs it yet; `vykar upgrade` will.
pub fn run(
    config: &VykarConfig,
    passphrase: Option<&str>,
    cancel: &CancelToken,
) -> Result<PackMapStats> {
    let mut repo = open_repo(config, passphrase, OpenOptions::new(), cancel)?;
    with_maintenance_lock(&mut repo, |repo| {
        repo.load_chunk_index_under_lock()?;
        repo.write_pack_maps(None, cancel)
    })
}
