//! Conditional fetch of the remote `index` against the local index blob cache.

use std::path::Path;

use tracing::debug;
use vykar_storage::{ConditionalGet, StorageBackend};
use vykar_types::error::Result;

use crate::index::dedup_cache::CachedIndexBlob;

/// The remote `index`, as [`fetch_index_object`] obtained it.
pub(crate) enum IndexObject {
    /// Downloaded from storage (`None` = absent).
    Remote(Option<Vec<u8>>),
    /// Storage reported the object byte-identical to this local cache file.
    Cached(CachedIndexBlob),
}

/// GET `index`, conditional on the local index blob cache when one exists.
/// Local cache problems are never errors; storage errors are returned as from
/// `get`.
///
/// The cache file stays open across the request (on a 200, the whole
/// download), so the digest sent and any bytes later used come from one file.
pub(crate) fn fetch_index_object(
    storage: &dyn StorageBackend,
    repo_id: &[u8],
    cache_dir: Option<&Path>,
) -> Result<IndexObject> {
    let Some(cached) = CachedIndexBlob::open(repo_id, cache_dir) else {
        return Ok(IndexObject::Remote(storage.get("index")?));
    };
    match storage.get_if_changed("index", cached.digest())? {
        ConditionalGet::Unchanged => {
            debug!(
                generation = cached.generation(),
                "index unchanged on storage, using the local copy"
            );
            Ok(IndexObject::Cached(cached))
        }
        ConditionalGet::Fetched(data) => Ok(IndexObject::Remote(data)),
    }
}

impl IndexObject {
    /// The object's bytes. A cached copy is read from the open cache file and
    /// verified against the digest storage matched. One that fails
    /// verification is replaced by an unconditional GET.
    pub(crate) fn into_bytes(self, storage: &dyn StorageBackend) -> Result<Option<Vec<u8>>> {
        match self {
            Self::Remote(data) => Ok(data),
            Self::Cached(cached) => match cached.read_verified() {
                Some(bytes) => Ok(Some(bytes)),
                None => {
                    debug!("local index copy failed verification, downloading index");
                    storage.get("index")
                }
            },
        }
    }
}
