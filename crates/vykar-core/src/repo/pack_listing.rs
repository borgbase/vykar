//! Listing the packs under `packs/`, with sizes and mtimes: one request on
//! a vykar-server that lists a whole tree, else one listing per shard
//! directory.

use std::sync::Mutex;

use tracing::warn;
use vykar_storage::{ServerOp, StorageBackend};
use vykar_types::error::{Result, VykarError};
use vykar_types::pack_id::PackId;

use super::control::CancelToken;

/// A pack found by [`list_packs`]; `size` and `mtime` are `None` when the
/// listing did not report them.
pub(crate) struct ListedPack {
    pub key: String,
    pub pack_id: PackId,
    pub size: Option<u64>,
    /// Last modification on the storage's clock.
    pub mtime: Option<std::time::SystemTime>,
}

/// Directory under which pack shards live.
pub(crate) const PACKS_PREFIX: &str = "packs/";

/// Every pack in the 256 shard directories, with its size and mtime where
/// the listing reports them. One request where the backend lists a whole tree at once
/// (vykar-server); otherwise, or if that request fails, one listing per shard.
pub(crate) fn list_packs(
    storage: &dyn StorageBackend,
    concurrency: usize,
) -> Result<Vec<ListedPack>> {
    let Some(server) = storage
        .server_ops()
        .filter(|s| s.offers(ServerOp::ListTree))
    else {
        return parallel_list_packs(storage, concurrency);
    };
    match server.list_tree_meta(PACKS_PREFIX) {
        Ok(objects) => {
            let mut packs = Vec::with_capacity(objects.len());
            push_listed(
                objects.into_iter().filter(|o| in_pack_shard(&o.key)),
                &mut packs,
            );
            Ok(packs)
        }
        // A cancelled listing is not a reason to list every shard instead.
        Err(e @ VykarError::Interrupted) => Err(e),
        Err(e) => {
            warn!("pack listing: one-request pack listing failed ({e}); listing per shard");
            parallel_list_packs(storage, concurrency)
        }
    }
}

/// Whether `key` lies below one of the shard directories `packs/00/` ..
/// `packs/ff/`, i.e. whether a per-shard listing would have returned it.
fn in_pack_shard(key: &str) -> bool {
    let Some(rest) = key.strip_prefix(PACKS_PREFIX) else {
        return false;
    };
    let mut bytes = rest.bytes();
    let hex = |b: Option<u8>| matches!(b, Some(b'0'..=b'9' | b'a'..=b'f'));
    hex(bytes.next()) && hex(bytes.next()) && bytes.next() == Some(b'/')
}

/// Turn listed objects into packs: directory markers are skipped, and
/// non-pack keys are logged and skipped.
fn push_listed(
    objects: impl IntoIterator<Item = vykar_storage::ListedObject>,
    out: &mut Vec<ListedPack>,
) {
    for vykar_storage::ListedObject { key, size, mtime } in objects {
        if key.ends_with('/') {
            continue;
        }
        match PackId::from_storage_key(&key) {
            Ok(pack_id) => out.push(ListedPack {
                key,
                pack_id,
                size,
                mtime,
            }),
            Err(e) => warn!("Skipping invalid pack key '{}': {}", key, e),
        }
    }
}

/// Discover all pack files across 256 shard directories, in parallel, with
/// their sizes and mtimes where the backend's listing reports them. The fallback of
/// [`list_packs`].
fn parallel_list_packs(
    storage: &dyn StorageBackend,
    concurrency: usize,
) -> Result<Vec<ListedPack>> {
    let results: Mutex<Vec<ListedPack>> = Mutex::new(Vec::new());
    let errors: Mutex<Vec<VykarError>> = Mutex::new(Vec::new());
    let shards: Vec<u16> = (0..256).collect();

    // Not cancellable: `cancel` is honoured by the analysis that follows.
    crate::repo::bounded::for_each_bounded(concurrency, &shards, &CancelToken::new(), |&shard| {
        let prefix = format!("packs/{:02x}/", shard);
        let objects = match storage.list_meta(&prefix) {
            Ok(k) => k,
            Err(e) => {
                errors
                    .lock()
                    .expect("pack listing errors lock not poisoned")
                    .push(e);
                return;
            }
        };

        let mut local = Vec::new();
        push_listed(objects, &mut local);
        if !local.is_empty() {
            results
                .lock()
                .expect("pack listing results lock not poisoned")
                .extend(local);
        }
    });

    let errs = errors
        .into_inner()
        .expect("pack listing errors lock not poisoned");
    if let Some(first) = errs.into_iter().next() {
        return Err(first);
    }

    Ok(results
        .into_inner()
        .expect("pack listing results lock not poisoned"))
}

#[cfg(test)]
mod tests {
    use super::in_pack_shard;

    #[test]
    fn in_pack_shard_matches_exactly_the_shard_directories() {
        let hex = "ab".repeat(32);
        for key in [
            "packs/00/x".to_string(),
            format!("packs/ff/{hex}"),
            "packs/ab/nested/x".to_string(),
        ] {
            assert!(in_pack_shard(&key), "{key}");
        }
        for key in [
            format!("packs/{hex}"),
            "packs/AB/x".to_string(),
            "packs/abc/x".to_string(),
            "packs/ab".to_string(),
            "packs/a/x".to_string(),
            "packsab/x".to_string(),
            "index".to_string(),
            "snapshots/ab/x".to_string(),
        ] {
            assert!(!in_pack_shard(&key), "{key}");
        }
    }
}
