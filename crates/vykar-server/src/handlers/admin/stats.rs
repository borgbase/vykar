use axum::response::{IntoResponse, Response};

use crate::error::ServerError;
use crate::state::{read_unpoisoned, AppState};

pub(super) async fn repo_stats(state: AppState) -> Result<Response, ServerError> {
    let data_dir = state.inner.data_dir.clone();

    let (total_bytes, total_objects, total_packs) =
        tokio::task::spawn_blocking(move || count_repo_stats(&data_dir))
            .await
            .map_err(|e| ServerError::Internal(e.to_string()))?;

    // Refresh quota from filesystem before reporting.
    let qs = state.inner.quota_state.clone();
    let usage = state.quota_used();
    tokio::task::spawn_blocking(move || qs.refresh(usage))
        .await
        .map_err(|e| ServerError::Internal(e.to_string()))?;

    let last_backup = *read_unpoisoned(&state.inner.last_backup_at, "last_backup_at");

    let quota_bytes = state.quota_limit();
    let quota_used = state.quota_used();
    let quota_source = state.inner.quota_state.source();

    Ok(axum::Json(serde_json::json!({
        "total_bytes": total_bytes,
        "total_objects": total_objects,
        "total_packs": total_packs,
        "last_backup_at": last_backup,
        "quota_bytes": quota_bytes,
        "quota_used_bytes": quota_used,
        "quota_source": quota_source,
    }))
    .into_response())
}

fn walk(dir: &std::path::Path, bytes: &mut u64, objects: &mut u64) {
    if let Ok(entries) = std::fs::read_dir(dir) {
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                walk(&path, bytes, objects);
            } else if let Ok(meta) = path.metadata() {
                *bytes += meta.len();
                *objects += 1;
            }
        }
    }
}

fn count_packs(dir: &std::path::Path, count: &mut u64) {
    if let Ok(entries) = std::fs::read_dir(dir) {
        for entry in entries.flatten() {
            if entry.path().is_dir() {
                count_packs(&entry.path(), count);
            } else {
                *count += 1;
            }
        }
    }
}

fn count_repo_stats(repo_dir: &std::path::Path) -> (u64, u64, u64) {
    let mut total_bytes = 0u64;
    let mut total_objects = 0u64;
    let mut total_packs = 0u64;

    walk(repo_dir, &mut total_bytes, &mut total_objects);

    // Count packs specifically.
    let packs_dir = repo_dir.join("packs");
    if packs_dir.exists() {
        count_packs(&packs_dir, &mut total_packs);
    }

    (total_bytes, total_objects, total_packs)
}
