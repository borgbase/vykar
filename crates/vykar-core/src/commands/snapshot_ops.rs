use crate::repo::Repository;
use crate::repo::control::CancelToken;
use vykar_types::error::{Result, VykarError};

/// A snapshot resolved for removal. Only constructed by
/// [`prepare_removal_targets`]; the storage key stays private so callers must
/// go through [`remove_snapshots`] to delete the blob.
pub struct RemovalTarget {
    pub snapshot_name: String,
    snapshot_key: String,
}

/// Resolve every name against the manifest. No storage access, no mutation:
/// an unknown name fails with `SnapshotNotFound` before anything is deleted.
pub fn prepare_removal_targets<S: AsRef<str>>(
    repo: &Repository,
    names: &[S],
) -> Result<Vec<RemovalTarget>> {
    names
        .iter()
        .map(|name| {
            let name = name.as_ref();
            repo.manifest()
                .find_snapshot(name)
                .map(|e| RemovalTarget {
                    snapshot_name: name.to_string(),
                    snapshot_key: e.id.storage_key(),
                })
                .ok_or_else(|| VykarError::SnapshotNotFound(name.into()))
        })
        .collect()
}

/// Delete the `snapshots/<id>` objects (each fenced by the maintenance lock),
/// remove them from the manifest and re-sync the local snapshot list cache.
/// Returns the removed names. The index is not read or written: the chunks
/// only these snapshots referenced stay until `vykar compact` sweeps them.
pub fn remove_snapshots(
    repo: &mut Repository,
    targets: Vec<RemovalTarget>,
    shutdown: &CancelToken,
) -> Result<Vec<String>> {
    let mut removed = Vec::with_capacity(targets.len());
    let result = (|| {
        for target in targets {
            shutdown.check()?;
            repo.check_lock_fence()?;
            repo.storage.delete(&target.snapshot_key).map_err(|e| {
                VykarError::Other(format!(
                    "failed to delete snapshot object {}: {e}",
                    target.snapshot_name
                ))
            })?;
            repo.manifest_mut().remove_snapshot(&target.snapshot_name);
            removed.push(target.snapshot_name);
        }
        Ok(())
    })();
    // The cache is best-effort; it follows the manifest whatever happened.
    repo.persist_snapshot_cache_from_manifest();
    result.map(|()| removed)
}
