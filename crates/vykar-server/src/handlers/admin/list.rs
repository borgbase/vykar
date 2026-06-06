use axum::response::{IntoResponse, Response};

use crate::error::ServerError;
use crate::state::AppState;

/// GET /?list - list all keys in the repository.
pub(super) async fn repo_list_all(state: AppState) -> Result<Response, ServerError> {
    let data_dir = state.inner.data_dir.clone();

    let keys = tokio::task::spawn_blocking(move || -> std::io::Result<Vec<String>> {
        let mut keys = Vec::new();
        list_all_recursive(&data_dir, &data_dir, &mut keys)?;
        keys.sort();
        Ok(keys)
    })
    .await
    .map_err(|e| ServerError::Internal(e.to_string()))?
    .map_err(ServerError::from)?;

    Ok(axum::Json(keys).into_response())
}

/// Recursively list all files under `dir`, producing keys relative to `root`.
fn list_all_recursive(
    dir: &std::path::Path,
    root: &std::path::Path,
    out: &mut Vec<String>,
) -> std::io::Result<()> {
    for entry in std::fs::read_dir(dir)? {
        let entry = entry?;
        let path = entry.path();
        if path.is_dir() {
            list_all_recursive(&path, root, out)?;
        } else if let Ok(rel) = path.strip_prefix(root) {
            // Use forward slashes for storage keys regardless of platform.
            let key: String = rel
                .iter()
                .map(|c| c.to_string_lossy())
                .collect::<Vec<_>>()
                .join("/");
            out.push(key);
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use axum::http::StatusCode;

    use crate::handlers::test_helpers::*;

    #[tokio::test]
    async fn list_all_keys_in_repo() {
        let (router, _state, _tmp) = setup_app(0);

        // PUT two known keys.
        let resp = authed_put(router.clone(), "/config", b"cfg".to_vec()).await;
        assert_status(&resp, StatusCode::CREATED);

        let resp = authed_put(router.clone(), "/index.gen", b"gen".to_vec()).await;
        assert_status(&resp, StatusCode::CREATED);

        // GET /?list should return both keys.
        let resp = authed_get(router.clone(), "/?list").await;
        assert_status(&resp, StatusCode::OK);

        let keys: Vec<String> =
            serde_json::from_slice(&body_bytes(resp).await).expect("parse JSON");
        assert!(
            keys.contains(&"config".to_string()),
            "expected 'config' in {keys:?}"
        );
        assert!(
            keys.contains(&"index.gen".to_string()),
            "expected 'index.gen' in {keys:?}"
        );
    }
}
