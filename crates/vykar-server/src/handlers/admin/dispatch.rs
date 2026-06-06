use axum::extract::{Query, State};
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
        return repo_stats(state).await;
    }
    if query.verify_structure.is_some() {
        return verify_structure(state).await;
    }
    if query.list.is_some() {
        return repo_list_all(state).await;
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
        return repo_init(state).await;
    }
    if query.batch_delete.is_some() {
        let cleanup_dirs = query.cleanup_dirs.is_some();
        return batch_delete(state, body, cleanup_dirs).await;
    }
    if query.repack.is_some() {
        return repack(state, body).await;
    }
    if query.verify_packs.is_some() {
        return verify_packs(state, body).await;
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
