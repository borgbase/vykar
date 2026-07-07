use axum::extract::{Query, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};

use crate::error::ServerError;
use crate::state::AppState;

use super::batch_delete::batch_delete;
use super::init::repo_init;
use super::list::repo_list_all;
use super::repack::repack;
use super::stats::repo_stats;
use super::verify_packs::verify_packs;
use super::verify_structure::verify_structure;

/// Timeout for admin actions, applied per-action here rather than as a route
/// layer so that repack can be exempted. Repack deletes source packs as it
/// completes operations and its blocking task keeps running after a timeout,
/// so cutting the response mid-run would leave the client without the results
/// it needs to point its index at the new packs. Repack's work is bounded by
/// `MAX_REPACK_OUTPUT_BYTES` instead.
const ADMIN_ACTION_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(600);

/// Run an admin action under [`ADMIN_ACTION_TIMEOUT`], returning 408 on
/// expiry. Any `spawn_blocking` work the action started keeps running to
/// completion in the background (same semantics as a route-level timeout);
/// only read-only or idempotent actions are wrapped.
async fn with_admin_timeout(
    fut: impl std::future::Future<Output = Result<Response, ServerError>>,
) -> Result<Response, ServerError> {
    match tokio::time::timeout(ADMIN_ACTION_TIMEOUT, fut).await {
        Ok(result) => result,
        Err(_) => Ok(StatusCode::REQUEST_TIMEOUT.into_response()),
    }
}

#[derive(serde::Deserialize, Default)]
pub struct RepoQuery {
    pub stats: Option<String>,
    #[serde(rename = "verify-structure")]
    pub verify_structure: Option<String>,
    pub init: Option<String>,
    #[serde(rename = "batch-delete")]
    pub batch_delete: Option<String>,
    #[serde(rename = "cleanup-dirs")]
    pub cleanup_dirs: Option<String>,
    pub repack: Option<String>,
    #[serde(rename = "verify-packs")]
    pub verify_packs: Option<String>,
    pub list: Option<String>,
}

/// GET / - dispatches based on query parameter.
pub async fn repo_dispatch(
    State(state): State<AppState>,
    Query(query): Query<RepoQuery>,
) -> Result<Response, ServerError> {
    if query.stats.is_some() {
        return with_admin_timeout(repo_stats(state)).await;
    }
    if query.verify_structure.is_some() {
        return with_admin_timeout(verify_structure(state)).await;
    }
    if query.list.is_some() {
        return with_admin_timeout(repo_list_all(state)).await;
    }
    Err(ServerError::BadRequest(
        "missing query parameter (stats, verify-structure, list)".into(),
    ))
}

/// POST / - dispatches based on query parameter.
pub async fn repo_action_dispatch(
    State(state): State<AppState>,
    Query(query): Query<RepoQuery>,
    body: axum::body::Bytes,
) -> Result<Response, ServerError> {
    if query.init.is_some() {
        return with_admin_timeout(repo_init(state)).await;
    }
    if query.batch_delete.is_some() {
        let cleanup_dirs = query.cleanup_dirs.is_some();
        return with_admin_timeout(batch_delete(state, body, cleanup_dirs)).await;
    }
    if query.repack.is_some() {
        // Deliberately not wrapped in the timeout — see ADMIN_ACTION_TIMEOUT.
        return repack(state, body).await;
    }
    if query.verify_packs.is_some() {
        return with_admin_timeout(verify_packs(state, body)).await;
    }
    Err(ServerError::BadRequest(
        "missing query parameter (init, batch-delete, repack, verify-packs)".into(),
    ))
}

/// GET /health - unauthenticated health check.
pub async fn health() -> impl IntoResponse {
    axum::Json(serde_json::json!({
        "status": "ok",
        "version": env!("CARGO_PKG_VERSION"),
    }))
}
