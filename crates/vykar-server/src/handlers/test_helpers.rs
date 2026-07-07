use axum::body::Body;
use axum::http::{Request, StatusCode};
use axum::Router;
use tower::ServiceExt;

use crate::config::ServerSection;
use crate::quota::{QuotaSource, QuotaState};
use crate::state::AppState;

pub const TEST_TOKEN: &str = "test-token";

/// Create a wired-up router and AppState backed by a temp directory.
///
/// The repo directory structure is pre-created so `file_path()` resolves
/// (the `existing_ancestor_within` check needs the dirs to exist).
pub fn setup_app(quota: u64) -> (Router, AppState, tempfile::TempDir) {
    setup_app_with(quota, false)
}

/// Like [`setup_app`] but with append-only mode enabled.
pub fn setup_app_append_only(quota: u64) -> (Router, AppState, tempfile::TempDir) {
    setup_app_with(quota, true)
}

pub fn setup_app_with(quota: u64, append_only: bool) -> (Router, AppState, tempfile::TempDir) {
    let tmp = tempfile::tempdir().expect("create tempdir");
    let data_dir = tmp.path().to_path_buf();

    // Create repo structure directly in data_dir (single-repo mode)
    std::fs::create_dir_all(data_dir.join("keys")).unwrap();
    std::fs::create_dir_all(data_dir.join("snapshots")).unwrap();
    std::fs::create_dir_all(data_dir.join("locks")).unwrap();
    for i in 0..=255u8 {
        std::fs::create_dir_all(data_dir.join("packs").join(format!("{i:02x}"))).unwrap();
    }

    // Build a deterministic QuotaState — no auto-detection in tests.
    let (source, limit) = if quota > 0 {
        (QuotaSource::Explicit, quota)
    } else {
        (QuotaSource::Unlimited, 0)
    };
    let quota_state = QuotaState::new(source, limit, true, data_dir.clone());

    let config = ServerSection {
        data_dir: data_dir.to_string_lossy().into_owned(),
        token: TEST_TOKEN.to_string(),
        append_only,
        ..Default::default()
    };

    let state = AppState::new_with_quota(config, quota_state);
    let router = super::router(state.clone());
    (router, state, tmp)
}

/// Send an authenticated PUT request with the given body bytes.
pub async fn authed_put(router: Router, path: &str, body: Vec<u8>) -> axum::response::Response {
    let req = Request::builder()
        .method("PUT")
        .uri(path)
        .header("Authorization", format!("Bearer {TEST_TOKEN}"))
        .header("Content-Length", body.len().to_string())
        .body(Body::from(body))
        .unwrap();

    router.oneshot(req).await.unwrap()
}

/// Send an authenticated GET request.
pub async fn authed_get(router: Router, path: &str) -> axum::response::Response {
    let req = Request::builder()
        .method("GET")
        .uri(path)
        .header("Authorization", format!("Bearer {TEST_TOKEN}"))
        .body(Body::empty())
        .unwrap();

    router.oneshot(req).await.unwrap()
}

/// Send an authenticated DELETE request.
pub async fn authed_delete(router: Router, path: &str) -> axum::response::Response {
    let req = Request::builder()
        .method("DELETE")
        .uri(path)
        .header("Authorization", format!("Bearer {TEST_TOKEN}"))
        .body(Body::empty())
        .unwrap();

    router.oneshot(req).await.unwrap()
}

/// Send an authenticated POST request with the given body bytes.
pub async fn authed_post(router: Router, path: &str, body: Vec<u8>) -> axum::response::Response {
    let req = Request::builder()
        .method("POST")
        .uri(path)
        .header("Authorization", format!("Bearer {TEST_TOKEN}"))
        .header("Content-Type", "application/json")
        .body(Body::from(body))
        .unwrap();

    router.oneshot(req).await.unwrap()
}

/// Read full response body into `Vec<u8>`.
pub async fn body_bytes(response: axum::response::Response) -> Vec<u8> {
    axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("collect body")
        .to_vec()
}

/// Assert response has expected status.
pub fn assert_status(response: &axum::response::Response, expected: StatusCode) {
    assert_eq!(
        response.status(),
        expected,
        "expected {expected}, got {}",
        response.status()
    );
}

/// Recursively assert no server temp files remain under `dir`. Checks both the
/// current `.tmp.*` prefix and the legacy `.repack_tmp.*` prefix.
pub fn assert_no_temp_files(dir: &std::path::Path) {
    for path in walk_file_paths(dir) {
        let name = path.file_name().unwrap().to_string_lossy();
        assert!(
            !name.starts_with(".tmp.") && !name.starts_with(".repack_tmp."),
            "leftover temp file: {}",
            path.display()
        );
    }
}

/// Recursively collect every file path under `dir`.
pub fn walk_file_paths(dir: &std::path::Path) -> Vec<std::path::PathBuf> {
    let mut out = Vec::new();
    if let Ok(entries) = std::fs::read_dir(dir) {
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                out.extend(walk_file_paths(&path));
            } else {
                out.push(path);
            }
        }
    }
    out
}
