use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};

use crate::error::ServerError;
use crate::state::AppState;

pub(super) async fn batch_delete(
    state: AppState,
    body: axum::body::Bytes,
    cleanup_dirs: bool,
) -> Result<Response, ServerError> {
    if state.inner.config.append_only {
        return Err(ServerError::Forbidden(
            "append-only: batch-delete not allowed".into(),
        ));
    }

    let keys: Vec<String> = serde_json::from_slice(&body)
        .map_err(|e| ServerError::BadRequest(format!("invalid JSON: {e}")))?;

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
        } else {
            state_clone.sub_quota_usage(old_size);
        }
    }

    if cleanup_dirs {
        let data_dir = &state_clone.inner.data_dir;
        let mut dirs: std::collections::BTreeSet<std::path::PathBuf> =
            std::collections::BTreeSet::new();
        for key in &keys {
            if let Some(p) = state_clone.file_path_for_cleanup(key) {
                let mut cur = p.parent().map(std::path::Path::to_path_buf);
                while let Some(d) = cur {
                    if d == *data_dir || !d.starts_with(data_dir) {
                        break;
                    }
                    dirs.insert(d.clone());
                    cur = d.parent().map(std::path::Path::to_path_buf);
                }
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
