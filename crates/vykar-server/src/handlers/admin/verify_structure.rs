use axum::response::{IntoResponse, Response};

use vykar_protocol::{PACK_MAGIC, PACK_VERSION_MAX, PACK_VERSION_MIN};

use crate::error::ServerError;
use crate::state::AppState;

pub(super) async fn verify_structure(state: AppState) -> Result<Response, ServerError> {
    let data_dir = state.inner.data_dir.clone();

    let result = tokio::task::spawn_blocking(move || check_structure(&data_dir))
        .await
        .map_err(|e| ServerError::Internal(e.to_string()))?;

    Ok(axum::Json(result).into_response())
}

fn check_structure(repo_dir: &std::path::Path) -> serde_json::Value {
    let mut errors: Vec<String> = Vec::new();
    let mut pack_count = 0u64;
    let mut total_size = 0u64;

    // Check required files.
    for required in &["config", "index", "keys/repokey"] {
        let path = repo_dir.join(required);
        if !path.exists() {
            errors.push(format!("missing required file: {required}"));
        }
    }

    // Check pack shard structure.
    let packs_dir = repo_dir.join("packs");
    if packs_dir.exists() {
        if let Ok(shards) = std::fs::read_dir(&packs_dir) {
            for shard_entry in shards.flatten() {
                let shard_name = shard_entry.file_name().to_string_lossy().to_string();

                // Verify shard is 2-char hex.
                if shard_name.len() != 2 || !shard_name.chars().all(|c| c.is_ascii_hexdigit()) {
                    errors.push(format!("invalid shard directory: packs/{shard_name}"));
                    continue;
                }

                if let Ok(packs) = std::fs::read_dir(shard_entry.path()) {
                    for pack_entry in packs.flatten() {
                        let pack_name = pack_entry.file_name().to_string_lossy().to_string();
                        pack_count += 1;

                        // Verify pack name is 64-char hex.
                        if pack_name.len() != 64
                            || !pack_name.chars().all(|c| c.is_ascii_hexdigit())
                        {
                            errors
                                .push(format!("invalid pack name: packs/{shard_name}/{pack_name}"));
                        }

                        let meta = pack_entry.metadata();
                        if let Ok(meta) = meta {
                            let size = meta.len();
                            total_size += size;

                            // Check minimum size: magic(8) + version(1) = 9.
                            if size < 9 {
                                errors.push(format!(
                                    "pack too small ({size} bytes): packs/{shard_name}/{pack_name}"
                                ));
                            } else {
                                // Check magic + version (read only 9 bytes).
                                match std::fs::File::open(pack_entry.path()).and_then(|mut f| {
                                    use std::io::Read;
                                    let mut hdr = [0u8; 9];
                                    f.read_exact(&mut hdr)?;
                                    Ok(hdr)
                                }) {
                                    Ok(hdr) => {
                                        if &hdr[..8] != PACK_MAGIC {
                                            errors.push(format!(
                                                "invalid pack magic: packs/{shard_name}/{pack_name}"
                                            ));
                                        } else if !(PACK_VERSION_MIN..=PACK_VERSION_MAX)
                                            .contains(&hdr[8])
                                        {
                                            errors.push(format!(
                                                "unsupported pack version {}: packs/{shard_name}/{pack_name}",
                                                hdr[8]
                                            ));
                                        }
                                    }
                                    Err(e) => {
                                        errors.push(format!(
                                            "read error for packs/{shard_name}/{pack_name}: {e}"
                                        ));
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    // Check for stale locks.
    let locks_dir = repo_dir.join("locks");
    let stale_locks = if locks_dir.exists() {
        std::fs::read_dir(&locks_dir).map_or(0, |entries| entries.flatten().count())
    } else {
        0
    };

    serde_json::json!({
        "ok": errors.is_empty(),
        "errors": errors,
        "pack_count": pack_count,
        "total_size": total_size,
        "stale_locks": stale_locks,
    })
}
