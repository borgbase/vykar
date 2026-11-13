use axum::response::Response;

use vykar_protocol::SizedKey;

use crate::error::ServerError;
use crate::handlers::objects::list_response;
use crate::state::AppState;

/// GET /?list - list all keys in the repository; `&sized` adds sizes.
pub(super) async fn repo_list_all(state: AppState, sized: bool) -> Result<Response, ServerError> {
    let data_dir = state.inner.data_dir.clone();

    let keys = tokio::task::spawn_blocking(move || -> std::io::Result<Vec<SizedKey>> {
        let mut keys = Vec::new();
        list_all_recursive(&data_dir, &data_dir, &mut keys)?;
        keys.sort_by(|a, b| a.key.cmp(&b.key));
        Ok(keys)
    })
    .await
    .map_err(|e| ServerError::Internal(e.to_string()))?
    .map_err(ServerError::from)?;

    Ok(list_response(keys, sized))
}

/// Recursively list all files under `dir`, producing keys relative to `root`.
fn list_all_recursive(
    dir: &std::path::Path,
    root: &std::path::Path,
    out: &mut Vec<SizedKey>,
) -> std::io::Result<()> {
    for entry in std::fs::read_dir(dir)? {
        let entry = entry?;
        let path = entry.path();
        // Follows symlinks, matching HEAD; an entry gone mid-walk is skipped.
        let Ok(meta) = std::fs::metadata(&path) else {
            continue;
        };
        if meta.is_dir() {
            list_all_recursive(&path, root, out)?;
        } else if let Ok(rel) = path.strip_prefix(root) {
            // Use forward slashes for storage keys regardless of platform.
            let key: String = rel
                .iter()
                .map(|c| c.to_string_lossy())
                .collect::<Vec<_>>()
                .join("/");
            if !vykar_protocol::is_temp_file(&key) {
                out.push(SizedKey {
                    key,
                    size: meta.len(),
                });
            }
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

    #[tokio::test]
    async fn list_sized_reports_sizes_and_hides_temps() {
        let (router, _state, tmp) = setup_app(0);
        std::fs::write(tmp.path().join("config"), b"cfg").unwrap();
        let pack = "packs/ab/".to_string() + &"ab".repeat(32);
        std::fs::write(tmp.path().join(&pack), vec![0u8; 1234]).unwrap();
        std::fs::write(tmp.path().join("packs/ab/.tmp.x.0"), b"partial").unwrap();

        let expected = vec![
            vykar_protocol::SizedKey {
                key: "config".into(),
                size: 3,
            },
            vykar_protocol::SizedKey {
                key: pack.clone(),
                size: 1234,
            },
        ];

        let resp = authed_get(router.clone(), "/?list&sized").await;
        assert_status(&resp, StatusCode::OK);
        let keys: Vec<vykar_protocol::SizedKey> =
            serde_json::from_slice(&body_bytes(resp).await).unwrap();
        assert_eq!(keys, expected);

        let resp = authed_get(router.clone(), "/packs?list&sized").await;
        assert_status(&resp, StatusCode::OK);
        let keys: Vec<vykar_protocol::SizedKey> =
            serde_json::from_slice(&body_bytes(resp).await).unwrap();
        assert_eq!(keys, expected[1..]);

        // Without `sized`, bare keys as before.
        let resp = authed_get(router, "/packs?list").await;
        let keys: Vec<String> = serde_json::from_slice(&body_bytes(resp).await).unwrap();
        assert_eq!(keys, vec![pack]);
    }
}
