use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};

use crate::error::ServerError;
use crate::state::AppState;

/// Maximum keys accepted in one batch-delete request. Clients chunk larger
/// deletes (see `delete_repo` / the `BatchDelete` impl in `rest_backend`).
const MAX_BATCH_DELETE_KEYS: usize = 200_000;

pub(super) async fn batch_delete(
    state: AppState,
    body: axum::body::Bytes,
    cleanup_dirs: bool,
) -> Result<Response, ServerError> {
    let keys: Vec<String> = serde_json::from_slice(&body)
        .map_err(|e| ServerError::BadRequest(format!("invalid JSON: {e}")))?;

    if keys.len() > MAX_BATCH_DELETE_KEYS {
        return Err(ServerError::BadRequest(format!(
            "too many keys: {} (max {MAX_BATCH_DELETE_KEYS})",
            keys.len()
        )));
    }

    // Append-only permits what single DELETE permits (the key-class table):
    // temp files, locks and sessions. Fail fast — name the
    // first refused key and delete nothing.
    if state.inner.config.append_only
        && let Some(key) = keys
            .iter()
            .find(|k| !vykar_protocol::append_only_allows(k, vykar_protocol::Verb::Delete))
    {
        return Err(ServerError::Forbidden(format!(
            "append-only: cannot batch-delete non-temp key: {key}"
        )));
    }

    let state_clone = state.clone();

    for key in &keys {
        // Use lenient path validation for cleanup - allows .tmp.* leftovers
        // from interrupted PUTs while still preventing path traversal.
        let Some(file_path) = state_clone.file_path_for_cleanup(key) else {
            tracing::warn!(key = %key, "batch-delete: skipping key with unsafe path");
            continue;
        };

        let old_size = match tokio::fs::metadata(&file_path).await {
            Ok(meta) => meta.len(),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => 0,
            Err(e) => return Err(ServerError::from(e)),
        };
        if let Err(e) = tokio::fs::remove_file(&file_path).await {
            if e.kind() != std::io::ErrorKind::NotFound {
                return Err(ServerError::from(e));
            }
        } else if !vykar_protocol::is_temp_file(key) {
            // Temp-file bytes were never committed to `quota_usage` (they live
            // in `quota_reserved` while the upload runs), so subtracting them
            // would drift usage low — same rationale as `delete_object`.
            state_clone.sub_quota_usage(old_size);
        }
    }

    if cleanup_dirs {
        let data_dir = &state_clone.inner.data_dir;
        // Listings hide temp files, so the client cannot name them; purge
        // them here so their directories can go too. Never in append-only
        // mode: an empty key list passes the guard above, and the purge would
        // destroy other clients' in-flight uploads. (Delete-repo cannot get
        // this far there anyway; it fails on its first committed key.)
        //
        // Otherwise the server cannot know whether a concurrent PUT is live
        // (delete-repo takes no lock). Unlinking its temp file only fails that
        // PUT (`persist` hits NotFound, the client retries), the same outcome
        // as the 24 h sweep catching a stalled upload; and the repository it
        // targets is being deleted anyway.
        //
        // Root files older versions left behind (`LEGACY_ROOT_FILES`) are
        // hidden from listings for the same reason and go with them. Unlike
        // temp files their bytes are in `quota_usage`.
        let purged = if state_clone.inner.config.append_only {
            Vec::new()
        } else {
            let purge_dir = data_dir.clone();
            let (purged, legacy_bytes) = tokio::task::spawn_blocking(move || {
                let purged = crate::cleanup::purge_temp_files(&purge_dir);
                (purged, remove_legacy_root_files(&purge_dir))
            })
            .await
            .map_err(|e| ServerError::Internal(format!("temp purge task failed: {e}")))?;
            state_clone.sub_quota_usage(legacy_bytes);
            purged
        };
        let mut dirs: std::collections::BTreeSet<std::path::PathBuf> =
            std::collections::BTreeSet::new();
        let deleted = keys
            .iter()
            .filter_map(|key| state_clone.file_path_for_cleanup(key));
        for p in deleted.chain(purged) {
            let mut cur = p.parent().map(std::path::Path::to_path_buf);
            while let Some(d) = cur {
                if d == *data_dir || !d.starts_with(data_dir) {
                    break;
                }
                dirs.insert(d.clone());
                cur = d.parent().map(std::path::Path::to_path_buf);
            }
        }
        // Sort deepest-first so children are removed before parents.
        let mut sorted: Vec<_> = dirs.into_iter().collect();
        sorted.sort_by_key(|b| std::cmp::Reverse(b.components().count()));
        for dir in sorted {
            match tokio::fs::remove_dir(&dir).await {
                Ok(()) => {}
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                Err(e) if dir_not_empty(&e) => {}
                Err(e) => {
                    tracing::warn!(dir = %dir.display(), error = %e, "cleanup-dirs: unexpected error");
                }
            }
        }
        // Also attempt to remove data_dir itself (parity with local delete_repo).
        match tokio::fs::remove_dir(data_dir).await {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) if dir_not_empty(&e) => {}
            Err(e) => {
                tracing::warn!(dir = %data_dir.display(), error = %e, "cleanup-dirs: unexpected error removing data_dir");
            }
        }
    }

    Ok(StatusCode::NO_CONTENT.into_response())
}

/// Remove the files in `data_dir` named in `LEGACY_ROOT_FILES` and return
/// the bytes they held. A symlink of such a name is removed too (the link,
/// not its target) and counts no bytes. A file that cannot be removed is
/// logged and kept.
fn remove_legacy_root_files(data_dir: &std::path::Path) -> u64 {
    let mut bytes = 0;
    for name in vykar_protocol::LEGACY_ROOT_FILES {
        let path = data_dir.join(name);
        let Ok(meta) = std::fs::symlink_metadata(&path) else {
            continue;
        };
        let link = meta.file_type().is_symlink();
        if !meta.is_file() && !link {
            continue;
        }
        match std::fs::remove_file(&path) {
            Ok(()) if link => {}
            Ok(()) => bytes += meta.len(),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => {
                tracing::warn!(path = %path.display(), error = %e, "cleanup-dirs: cannot remove legacy root file");
            }
        }
    }
    bytes
}

fn dir_not_empty(e: &std::io::Error) -> bool {
    #[cfg(unix)]
    {
        e.raw_os_error() == Some(libc::ENOTEMPTY) || e.raw_os_error() == Some(libc::EEXIST)
    }
    #[cfg(not(unix))]
    {
        let _ = e;
        false
    }
}

#[cfg(test)]
mod tests {
    use axum::http::StatusCode;

    use crate::handlers::test_helpers::*;

    #[tokio::test]
    async fn batch_delete_with_temp_files() {
        let (router, _state, tmp) = setup_app(0);

        // Create regular repo files.
        let resp = authed_put(router.clone(), "/config", b"cfg".to_vec()).await;
        assert_status(&resp, StatusCode::CREATED);
        let resp = authed_put(router.clone(), "/index.gen", b"gen".to_vec()).await;
        assert_status(&resp, StatusCode::CREATED);

        // Create temp files at root level (simulating interrupted PUTs).
        std::fs::write(tmp.path().join(".tmp.config.0"), b"partial").unwrap();
        std::fs::write(tmp.path().join(".tmp.index.gen.0"), b"partial").unwrap();

        // Create a temp file inside a pack shard directory.
        let shard_dir = tmp.path().join("packs").join("ab");
        std::fs::write(
            shard_dir
                .join(".tmp.deadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeef.0"),
            b"partial pack",
        )
        .unwrap();

        // batch-delete should succeed with all these keys (including temp files).
        let keys = serde_json::to_vec(&serde_json::json!([
            "config",
            "index.gen",
            ".tmp.config.0",
            ".tmp.index.gen.0",
            "packs/ab/.tmp.deadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeef.0"
        ]))
        .unwrap();
        let resp = authed_post(router.clone(), "/?batch-delete", keys).await;
        assert_status(&resp, StatusCode::NO_CONTENT);

        // Verify all files are gone.
        assert!(!tmp.path().join("config").exists());
        assert!(!tmp.path().join("index.gen").exists());
        assert!(!tmp.path().join(".tmp.config.0").exists());
        assert!(!tmp.path().join(".tmp.index.gen.0").exists());
    }

    #[tokio::test]
    async fn append_only_batch_delete_refuses_index_objects() {
        let (router, _state, tmp) = setup_app_append_only(0);
        let obj = format!("index.d/{}", "c3".repeat(32));
        std::fs::create_dir_all(tmp.path().join("index.d")).unwrap();
        std::fs::write(tmp.path().join(&obj), b"x").unwrap();
        std::fs::write(tmp.path().join("snapshots").join("abc"), b"s").unwrap();

        // Mixed with a committed snapshot: refused, nothing deleted.
        let keys = serde_json::to_vec(&serde_json::json!([obj, "snapshots/abc"])).unwrap();
        let resp = authed_post(router.clone(), "/?batch-delete", keys).await;
        assert_status(&resp, StatusCode::FORBIDDEN);
        assert!(tmp.path().join(&obj).exists());

        // Alone: refused as well (03 step 1a), the object survives.
        let keys = serde_json::to_vec(&serde_json::json!([obj])).unwrap();
        let resp = authed_post(router, "/?batch-delete", keys).await;
        assert_status(&resp, StatusCode::FORBIDDEN);
        assert!(tmp.path().join(&obj).exists());
        assert!(tmp.path().join("snapshots").join("abc").exists());
    }

    /// Locks and sessions are deletable in a batch as they are one by one.
    #[tokio::test]
    async fn append_only_batch_delete_allows_locks_and_sessions() {
        for query in ["/?batch-delete", "/?batch-delete&cleanup-dirs"] {
            let (router, _state, tmp) = setup_app_append_only(0);
            std::fs::create_dir_all(tmp.path().join("sessions")).unwrap();
            std::fs::write(tmp.path().join("locks/a.json"), b"l").unwrap();
            std::fs::write(tmp.path().join("sessions/s.json"), b"s").unwrap();

            let keys = serde_json::to_vec(&serde_json::json!(["locks/a.json", "sessions/s.json"]))
                .unwrap();
            let resp = authed_post(router.clone(), query, keys).await;
            assert_status(&resp, StatusCode::NO_CONTENT);
            assert!(!tmp.path().join("locks/a.json").exists(), "{query}");
            assert!(!tmp.path().join("sessions/s.json").exists(), "{query}");

            // A removed `locks/` directory is recreated by the next PUT.
            let resp = authed_put(router, "/locks/x", b"l".to_vec()).await;
            assert_status(&resp, StatusCode::CREATED);
        }
    }

    #[tokio::test]
    async fn append_only_batch_delete_all_temps_ok() {
        let (router, _state, tmp) = setup_app_append_only(0);
        std::fs::write(tmp.path().join(".tmp.config.0"), b"x").unwrap();
        std::fs::write(
            tmp.path().join("packs").join("ab").join(".tmp.deadbeef.0"),
            b"y",
        )
        .unwrap();

        let keys = serde_json::to_vec(&serde_json::json!([
            ".tmp.config.0",
            "packs/ab/.tmp.deadbeef.0"
        ]))
        .unwrap();
        let resp = authed_post(router, "/?batch-delete", keys).await;
        assert_status(&resp, StatusCode::NO_CONTENT);
        assert!(!tmp.path().join(".tmp.config.0").exists());
        assert!(
            !tmp.path()
                .join("packs")
                .join("ab")
                .join(".tmp.deadbeef.0")
                .exists()
        );
    }

    #[tokio::test]
    async fn append_only_batch_delete_mixed_forbidden_nothing_deleted() {
        let (router, _state, tmp) = setup_app_append_only(0);
        std::fs::write(tmp.path().join(".tmp.config.0"), b"x").unwrap();
        std::fs::write(tmp.path().join("config"), b"cfg").unwrap();

        let keys = serde_json::to_vec(&serde_json::json!([".tmp.config.0", "config"])).unwrap();
        let resp = authed_post(router, "/?batch-delete", keys).await;
        assert_status(&resp, StatusCode::FORBIDDEN);

        // Fail-fast: nothing deleted, not even the temp key that preceded it.
        assert!(
            tmp.path().join(".tmp.config.0").exists(),
            "temp must survive"
        );
        assert!(tmp.path().join("config").exists(), "committed must survive");
    }

    #[tokio::test]
    async fn batch_delete_cleanup_dirs_removes_empty_dirs() {
        let (router, _state, tmp) = setup_app(0);
        let data_dir = tmp.path();

        // Write files directly to disk (bypasses PUT checksum requirements).
        std::fs::write(data_dir.join("config"), b"cfg").unwrap();
        let pack_key = "packs/ab/".to_string() + &"ab".repeat(32);
        std::fs::write(data_dir.join(&pack_key), b"packdata").unwrap();
        std::fs::write(data_dir.join("snapshots/snap1"), b"snapdata").unwrap();

        // batch-delete with cleanup-dirs.
        let keys = serde_json::to_vec(&serde_json::json!(["config", pack_key, "snapshots/snap1"]))
            .unwrap();
        let resp = authed_post(router.clone(), "/?batch-delete&cleanup-dirs", keys).await;
        assert_status(&resp, StatusCode::NO_CONTENT);

        // Files should be gone.
        assert!(!data_dir.join("config").exists());
        assert!(!data_dir.join(&pack_key).exists());
        assert!(!data_dir.join("snapshots/snap1").exists());

        // The shard dir packs/ab should be removed (empty after file deletion).
        assert!(
            !data_dir.join("packs/ab").exists(),
            "packs/ab shard dir should be removed"
        );
        // packs/ still has other shard dirs (00-ff minus ab), so it stays.
        // snapshots/ had only snap1, so it should be removed.
        assert!(
            !data_dir.join("snapshots").exists(),
            "snapshots dir should be removed"
        );
    }

    #[tokio::test]
    async fn batch_delete_cleanup_dirs_purges_unlisted_temps() {
        let (router, _state, tmp) = setup_app(0);
        let data_dir = tmp.path();

        std::fs::write(data_dir.join("config"), b"cfg").unwrap();
        std::fs::write(data_dir.join(".tmp.config.0"), b"partial").unwrap();
        // packs/ab holds only a temp file; no committed key names it.
        std::fs::write(data_dir.join("packs/ab/.tmp.deadbeef.0"), b"partial").unwrap();
        // Leave packs/ab as the only shard so packs/ can go too.
        for entry in std::fs::read_dir(data_dir.join("packs")).unwrap() {
            let path = entry.unwrap().path();
            if path.file_name().unwrap() != "ab" {
                std::fs::remove_dir(path).unwrap();
            }
        }

        let keys = serde_json::to_vec(&serde_json::json!(["config"])).unwrap();
        let resp = authed_post(router.clone(), "/?batch-delete&cleanup-dirs", keys).await;
        assert_status(&resp, StatusCode::NO_CONTENT);

        assert!(!data_dir.join(".tmp.config.0").exists());
        assert!(!data_dir.join("packs/ab").exists());
        assert!(!data_dir.join("packs").exists());
    }

    /// Listings hide a `manifest` left by vykar 0.11 and earlier, so the
    /// client cannot name it; delete-repo's cleanup removes it, with its
    /// bytes, so the data directory can go.
    #[tokio::test]
    async fn batch_delete_cleanup_dirs_removes_legacy_root_files() {
        let tmp = tempfile::tempdir().unwrap();
        std::fs::write(tmp.path().join("config"), b"cfg").unwrap();
        std::fs::write(tmp.path().join("manifest"), vec![0u8; 1000]).unwrap();
        let (router, state, tmp) = setup_app_in(tmp, 0, false);
        // The startup scan counts the file's bytes.
        assert_eq!(state.quota_used(), 1003);
        // Drop the helper's skeleton so only the two files are left.
        for d in ["keys", "snapshots", "locks", "packs"] {
            std::fs::remove_dir_all(tmp.path().join(d)).unwrap();
        }
        let keys = serde_json::to_vec(&serde_json::json!(["config"])).unwrap();
        let resp = authed_post(router, "/?batch-delete&cleanup-dirs", keys).await;
        assert_status(&resp, StatusCode::NO_CONTENT);
        assert!(!tmp.path().join("manifest").exists());
        assert_eq!(state.quota_used(), 0);
        assert!(!tmp.path().exists(), "data dir should be removed");
    }

    /// A symlink named `manifest` goes too: the link, not what it names.
    #[cfg(unix)]
    #[tokio::test]
    async fn batch_delete_cleanup_dirs_removes_a_legacy_root_symlink() {
        let tmp = tempfile::tempdir().unwrap();
        let outside = tempfile::tempdir().unwrap();
        let target = outside.path().join("target");
        std::fs::write(&target, b"kept").unwrap();
        std::fs::write(tmp.path().join("config"), b"cfg").unwrap();
        std::os::unix::fs::symlink(&target, tmp.path().join("manifest")).unwrap();
        let (router, state, tmp) = setup_app_in(tmp, 0, false);
        assert_eq!(state.quota_used(), 3);
        for d in ["keys", "snapshots", "locks", "packs"] {
            std::fs::remove_dir_all(tmp.path().join(d)).unwrap();
        }
        let keys = serde_json::to_vec(&serde_json::json!(["config"])).unwrap();
        let resp = authed_post(router, "/?batch-delete&cleanup-dirs", keys).await;
        assert_status(&resp, StatusCode::NO_CONTENT);
        assert_eq!(std::fs::read(&target).unwrap(), b"kept");
        assert_eq!(state.quota_used(), 0);
        assert!(!tmp.path().exists(), "data dir should be removed");
    }

    #[tokio::test]
    async fn append_only_cleanup_dirs_keeps_legacy_root_files() {
        let tmp = tempfile::tempdir().unwrap();
        std::fs::write(tmp.path().join("manifest"), b"old").unwrap();
        let (router, _state, tmp) = setup_app_in(tmp, 0, true);
        let keys = serde_json::to_vec(&serde_json::json!([])).unwrap();
        let resp = authed_post(router, "/?batch-delete&cleanup-dirs", keys).await;
        assert_status(&resp, StatusCode::NO_CONTENT);
        assert_eq!(std::fs::read(tmp.path().join("manifest")).unwrap(), b"old");
    }

    #[tokio::test]
    async fn append_only_cleanup_dirs_does_not_purge_temps() {
        let (router, _state, tmp) = setup_app_append_only(0);
        let live = tmp.path().join("packs/ab/.tmp.inflight.0");
        std::fs::write(&live, b"uploading").unwrap();

        let keys = serde_json::to_vec(&serde_json::json!([])).unwrap();
        let resp = authed_post(router, "/?batch-delete&cleanup-dirs", keys).await;
        assert_status(&resp, StatusCode::NO_CONTENT);
        assert!(live.exists(), "another client's upload must survive");
    }

    #[tokio::test]
    async fn batch_delete_without_cleanup_dirs_preserves_dirs() {
        let (router, _state, tmp) = setup_app(0);
        let data_dir = tmp.path();

        // Write a pack file directly to disk.
        let pack_key = "packs/ab/".to_string() + &"ab".repeat(32);
        std::fs::write(data_dir.join(&pack_key), b"packdata").unwrap();

        // batch-delete WITHOUT cleanup-dirs flag.
        let keys = serde_json::to_vec(&serde_json::json!([pack_key])).unwrap();
        let resp = authed_post(router.clone(), "/?batch-delete", keys).await;
        assert_status(&resp, StatusCode::NO_CONTENT);

        // File should be gone.
        assert!(!data_dir.join(&pack_key).exists());

        // But directories should still exist.
        assert!(
            data_dir.join("packs/ab").exists(),
            "shard dir should remain without cleanup-dirs"
        );
        assert!(
            data_dir.join("packs").exists(),
            "packs dir should remain without cleanup-dirs"
        );
    }

    /// Without `cleanup-dirs` only the named keys go: another client's
    /// in-flight upload survives a compact's batch delete.
    #[tokio::test]
    async fn batch_delete_without_cleanup_dirs_keeps_unlisted_temps() {
        let (router, _state, tmp) = setup_app(0);
        let data_dir = tmp.path();
        let pack_key = "packs/ab/".to_string() + &"ab".repeat(32);
        let temp_key = "packs/ab/.tmp.inflight.0";
        std::fs::write(data_dir.join(&pack_key), b"packdata").unwrap();
        std::fs::write(data_dir.join(temp_key), b"partial").unwrap();

        let keys = serde_json::to_vec(&serde_json::json!([pack_key])).unwrap();
        let resp = authed_post(router, "/?batch-delete", keys).await;
        assert_status(&resp, StatusCode::NO_CONTENT);

        assert!(!data_dir.join(&pack_key).exists());
        assert!(
            data_dir.join(temp_key).exists(),
            "unlisted temp must survive"
        );
    }

    #[tokio::test]
    async fn batch_delete_cleanup_dirs_preserves_nonempty_dirs() {
        let (router, _state, tmp) = setup_app(0);
        let data_dir = tmp.path();

        // Write two pack files in the same shard directly to disk.
        let pack1_key = "packs/ab/".to_string() + &"ab".repeat(32);
        let pack2_key = "packs/ab/".to_string() + &"cd".repeat(32);
        std::fs::write(data_dir.join(&pack1_key), b"pack1").unwrap();
        std::fs::write(data_dir.join(&pack2_key), b"pack2").unwrap();

        // Delete only one with cleanup-dirs.
        let keys = serde_json::to_vec(&serde_json::json!([pack1_key])).unwrap();
        let resp = authed_post(router.clone(), "/?batch-delete&cleanup-dirs", keys).await;
        assert_status(&resp, StatusCode::NO_CONTENT);

        // Deleted file should be gone.
        assert!(!data_dir.join(&pack1_key).exists());
        // Remaining file should still exist.
        assert!(data_dir.join(&pack2_key).exists());
        // Dir should still exist because it's not empty.
        assert!(
            data_dir.join("packs/ab").exists(),
            "shard dir should remain when not empty"
        );
    }

    #[tokio::test]
    async fn batch_delete_cleanup_dirs_missing_keys_no_error() {
        let (router, _state, _tmp) = setup_app(0);

        // Delete nonexistent keys with cleanup-dirs.
        let keys = serde_json::to_vec(&serde_json::json!([
            "config",
            "index.gen",
            "snapshots/nope"
        ]))
        .unwrap();
        let resp = authed_post(router.clone(), "/?batch-delete&cleanup-dirs", keys).await;
        assert_status(&resp, StatusCode::NO_CONTENT);
    }

    #[tokio::test]
    async fn put_recreates_dirs_after_cleanup() {
        let (router, _state, tmp) = setup_app(0);
        let data_dir = tmp.path();

        // Write a pack file directly to disk, then delete with cleanup-dirs.
        let pack_key = "packs/ab/".to_string() + &"ab".repeat(32);
        std::fs::write(data_dir.join(&pack_key), b"packdata").unwrap();

        let keys = serde_json::to_vec(&serde_json::json!([pack_key])).unwrap();
        let resp = authed_post(router.clone(), "/?batch-delete&cleanup-dirs", keys).await;
        assert_status(&resp, StatusCode::NO_CONTENT);
        assert!(
            !data_dir.join("packs/ab").exists(),
            "shard dir should be removed"
        );

        // PUT a non-pack file that doesn't need checksum - e.g. a snapshot.
        let resp = authed_put(router.clone(), "/snapshots/newsnap", b"snapdata".to_vec()).await;
        assert_status(&resp, StatusCode::CREATED);
        assert!(
            data_dir.join("snapshots/newsnap").exists(),
            "new snapshot file should exist after PUT"
        );
    }
}
