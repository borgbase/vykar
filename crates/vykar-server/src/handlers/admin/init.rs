use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};

use crate::error::ServerError;
use crate::state::AppState;

pub(super) async fn repo_init(state: AppState) -> Result<Response, ServerError> {
    let data_dir = state.inner.data_dir.clone();

    // Reject if data_dir contains unexpected entries.
    let bad = crate::state::unexpected_entries(&data_dir);
    if !bad.is_empty() {
        return Err(ServerError::Conflict(format!(
            "data directory contains unexpected entries: {}",
            bad.join(", ")
        )));
    }

    // Check if already initialized (config file exists).
    let config_path = data_dir.join("config");
    match tokio::fs::metadata(&config_path).await {
        Ok(_) => {
            return Err(ServerError::Conflict("repo already initialized".into()));
        }
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
        Err(e) => return Err(ServerError::from(e)),
    }

    let data_dir_clone = data_dir.clone();
    tokio::task::spawn_blocking(move || {
        // Create base dirs.
        std::fs::create_dir_all(&data_dir_clone)?;
        std::fs::create_dir_all(data_dir_clone.join("keys"))?;
        std::fs::create_dir_all(data_dir_clone.join("snapshots"))?;
        std::fs::create_dir_all(data_dir_clone.join("locks"))?;
        // Create 256 pack shard directories.
        for i in 0..=255u8 {
            std::fs::create_dir_all(data_dir_clone.join("packs").join(format!("{i:02x}")))?;
        }
        Ok::<_, std::io::Error>(())
    })
    .await
    .map_err(|e| ServerError::Internal(e.to_string()))?
    .map_err(ServerError::from)?;

    Ok(StatusCode::CREATED.into_response())
}
