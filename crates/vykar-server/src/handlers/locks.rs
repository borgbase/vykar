use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};

use crate::error::ServerError;
use crate::state::{read_unpoisoned, write_unpoisoned, AppState, LockInfo};

#[derive(serde::Deserialize)]
pub struct LockRequest {
    pub hostname: String,
    #[serde(default)]
    pub pid: u64,
}

/// POST /locks/{id} — acquire a lock.
pub async fn acquire_lock(
    State(state): State<AppState>,
    Path(id): Path<String>,
    axum::Json(body): axum::Json<LockRequest>,
) -> Result<Response, ServerError> {
    let ttl = state.inner.config.lock_ttl_seconds;

    let mut locks = write_unpoisoned(&state.inner.locks, "locks");

    // Check if lock already exists and is not expired
    if let Some(existing) = locks.get(&id) {
        if !existing.is_expired() {
            return Err(ServerError::Conflict(format!(
                "lock '{id}' already held by {}",
                existing.hostname
            )));
        }
    }

    locks.insert(
        id.clone(),
        LockInfo {
            hostname: body.hostname,
            pid: body.pid,
            acquired_at: chrono::Utc::now(),
            ttl_seconds: ttl,
        },
    );

    Ok(StatusCode::CREATED.into_response())
}

/// DELETE /locks/{id} — release a lock.
pub async fn release_lock(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Result<Response, ServerError> {
    let mut locks = write_unpoisoned(&state.inner.locks, "locks");
    if locks.remove(&id).is_some() {
        return Ok(StatusCode::NO_CONTENT.into_response());
    }
    Err(ServerError::NotFound(format!("lock '{id}' not found")))
}

/// GET /locks — list active locks.
pub async fn list_locks(State(state): State<AppState>) -> Result<Response, ServerError> {
    let locks = read_unpoisoned(&state.inner.locks, "locks");

    let result: Vec<serde_json::Value> = locks
        .iter()
        .filter(|(_, info)| !info.is_expired())
        .map(|(id, info)| {
            serde_json::json!({
                "id": id,
                "hostname": info.hostname,
                "pid": info.pid,
                "acquired_at": info.acquired_at,
                "ttl_seconds": info.ttl_seconds,
            })
        })
        .collect();

    Ok(axum::Json(result).into_response())
}
