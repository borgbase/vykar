use axum::response::{IntoResponse, Response};

use vykar_protocol::{PACK_HEADER_SIZE, validate_pack_header};

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
    let mut temp_files = 0u64;

    // Check required files.
    for required in &["config", "keys/repokey"] {
        let path = repo_dir.join(required);
        if !path.exists() {
            errors.push(format!("missing required file: {required}"));
        }
    }
    // The index head: `index` (formats v2-v5) or a head under `index.h/`
    // (v6 and later, which have no `index`).
    if !repo_dir.join("index").exists() && !has_index_head(repo_dir) {
        errors.push("missing required file: index (or a head under index.h/)".into());
    }

    // Check pack shard structure.
    let packs_dir = repo_dir.join("packs");
    if packs_dir.exists()
        && let Ok(shards) = std::fs::read_dir(&packs_dir)
    {
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
                    // Upload/repack debris is not a pack: report it as a
                    // count, not as a malformed pack. The server sweeps it
                    // once it is older than `TEMP_DEBRIS_MAX_AGE`.
                    if vykar_protocol::is_temp_file(&pack_name) {
                        temp_files += 1;
                        continue;
                    }
                    pack_count += 1;

                    // Verify pack name is 64-char hex.
                    if pack_name.len() != 64 || !pack_name.chars().all(|c| c.is_ascii_hexdigit()) {
                        errors.push(format!("invalid pack name: packs/{shard_name}/{pack_name}"));
                    }

                    let meta = pack_entry.metadata();
                    if let Ok(meta) = meta {
                        let size = meta.len();
                        total_size += size;

                        if size < PACK_HEADER_SIZE as u64 {
                            errors.push(format!(
                                "pack too small ({size} bytes): packs/{shard_name}/{pack_name}"
                            ));
                        } else {
                            // Read the header only — magic + version byte.
                            match std::fs::File::open(pack_entry.path()).and_then(|mut f| {
                                use std::io::Read;
                                let mut hdr = [0u8; PACK_HEADER_SIZE];
                                f.read_exact(&mut hdr)?;
                                Ok(hdr)
                            }) {
                                Ok(hdr) => {
                                    if let Err(reason) = validate_pack_header(&hdr) {
                                        errors.push(format!(
                                            "{reason}: packs/{shard_name}/{pack_name}"
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
        "temp_files": temp_files,
    })
}

/// Whether `index.h/` holds at least one head key (`index.h/<gen>`).
fn has_index_head(repo_dir: &std::path::Path) -> bool {
    let Ok(entries) = std::fs::read_dir(repo_dir.join(vykar_protocol::HEAD_DIR)) else {
        return false;
    };
    entries.flatten().any(|e| {
        let name = e.file_name();
        let key = format!("{}/{}", vykar_protocol::HEAD_DIR, name.to_string_lossy());
        e.path().is_file() && vykar_protocol::head_generation(&key).is_some()
    })
}

#[cfg(test)]
mod tests {
    use axum::http::StatusCode;

    use super::super::test_support::{build_pack, write_pack};
    use crate::handlers::test_helpers::*;

    #[tokio::test]
    async fn temp_debris_is_counted_not_reported_as_error() {
        let (router, _state, tmp) = setup_app(0);
        std::fs::write(tmp.path().join("config"), b"cfg").unwrap();
        std::fs::write(tmp.path().join("index"), b"idx").unwrap();
        std::fs::write(tmp.path().join("keys/repokey"), b"key").unwrap();

        let (pack_bytes, _) = build_pack(&[b"hello"]);
        let key = write_pack(tmp.path(), &pack_bytes);
        let shard_dir = tmp.path().join(&key).parent().unwrap().to_path_buf();
        std::fs::write(shard_dir.join(".tmp.abcdef.0"), b"partial upload").unwrap();

        let resp = authed_get(router, "/?verify-structure").await;
        assert_status(&resp, StatusCode::OK);
        let json: serde_json::Value = serde_json::from_slice(&body_bytes(resp).await).unwrap();

        assert_eq!(json["ok"], true, "{json}");
        assert!(json["errors"].as_array().unwrap().is_empty(), "{json}");
        assert_eq!(json["pack_count"], 1);
        assert_eq!(json["total_size"], pack_bytes.len() as u64);
        assert_eq!(json["temp_files"], 1);
    }

    /// A v6 repository has no `index`: a head under `index.h/` satisfies
    /// the check, an empty `index.h/` does not.
    #[tokio::test]
    async fn a_head_under_index_h_replaces_index() {
        let (router, _state, tmp) = setup_app(0);
        std::fs::write(tmp.path().join("config"), b"cfg").unwrap();
        std::fs::write(tmp.path().join("keys/repokey"), b"key").unwrap();
        std::fs::create_dir_all(tmp.path().join("index.h")).unwrap();

        let resp = authed_get(router.clone(), "/?verify-structure").await;
        assert_status(&resp, StatusCode::OK);
        let json: serde_json::Value = serde_json::from_slice(&body_bytes(resp).await).unwrap();
        assert_eq!(json["ok"], false, "{json}");
        assert!(
            json["errors"][0].as_str().unwrap().contains("index.h/"),
            "{json}"
        );

        std::fs::write(tmp.path().join("index.h/x"), b"foreign").unwrap();
        std::fs::write(tmp.path().join("index.h/1"), b"head").unwrap();
        let resp = authed_get(router, "/?verify-structure").await;
        assert_status(&resp, StatusCode::OK);
        let json: serde_json::Value = serde_json::from_slice(&body_bytes(resp).await).unwrap();
        assert_eq!(json["ok"], true, "{json}");
    }
}
