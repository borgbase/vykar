//! Writing pack maps for the packs that carry no trailer.

use super::util::{open_repo, with_exclusive_session};
use crate::config::VykarConfig;
use crate::repo::OpenOptions;
use crate::repo::control::CancelToken;
use vykar_types::error::Result;

pub use crate::repo::pack_map::PackMapStats;

/// Write pack maps for every pack the index names that no readable map
/// covers (packs written without a trailer, before format v6 support).
/// Runs as an exclusive session in both commit modes (10-single-protocol
/// step 8), so it refuses beside any live session (a backup included) and
/// every other writer refuses beside it: a compact deletes maps none of
/// whose packs it saw alive, so maps are written only where no compact
/// runs. No CLI command runs it yet; `vykar upgrade` will.
pub fn run(
    config: &VykarConfig,
    passphrase: Option<&str>,
    cancel: &CancelToken,
) -> Result<PackMapStats> {
    let mut repo = open_repo(config, passphrase, OpenOptions::new(), cancel)?;
    with_exclusive_session(&mut repo, |repo| {
        repo.load_chunk_index_under_lock()?;
        repo.write_pack_maps(None, cancel)
    })
}
