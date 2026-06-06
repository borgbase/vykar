use std::sync::atomic::{AtomicU64, Ordering::Relaxed};

use axum::response::{IntoResponse, Response};
use blake2::digest::{Update, VariableOutput};
use blake2::Blake2bVar;

use vykar_protocol::{
    check_protocol_version, is_valid_pack_key, validate_blob_ref, RepackOperationResult,
    RepackPlanRequest, RepackResultResponse, PACK_HEADER_SIZE, PACK_MAGIC, PACK_VERSION_CURRENT,
};

use crate::error::ServerError;
use crate::state::AppState;

static REPACK_TEMP_COUNTER: AtomicU64 = AtomicU64::new(0);

const MAX_REPACK_OPS: usize = 10_000;
const MAX_KEEP_BLOBS_PER_OP: usize = 200_000;

pub(super) async fn repack(
    state: AppState,
    body: axum::body::Bytes,
) -> Result<Response, ServerError> {
    let plan: RepackPlanRequest = serde_json::from_slice(&body)
        .map_err(|e| ServerError::BadRequest(format!("invalid repack plan: {e}")))?;
    validate_repack_plan(&plan)?;

    if state.inner.config.append_only && plan.operations.iter().any(|op| op.delete_after) {
        return Err(ServerError::Forbidden(
            "append-only: repack with delete not allowed".into(),
        ));
    }

    let state_clone = state.clone();

    let results = tokio::task::spawn_blocking(move || execute_repack(&state_clone, &plan))
        .await
        .map_err(|e| ServerError::Internal(e.to_string()))?
        .map_err(ServerError::Internal)?;

    Ok(axum::Json(results).into_response())
}

#[allow(clippy::too_many_lines)]
fn execute_repack(
    state: &AppState,
    plan: &RepackPlanRequest,
) -> Result<RepackResultResponse, String> {
    use std::io::{BufWriter, Read, Seek, SeekFrom, Write};

    let mut completed = Vec::new();

    for op in &plan.operations {
        let source_path = state
            .file_path(&op.source_pack)
            .ok_or_else(|| "invalid pack path".to_string())?;

        if op.keep_blobs.is_empty() {
            // Just delete the pack.
            if op.delete_after {
                let old_size = source_path.metadata().map_or(0, |m| m.len());
                let _ = std::fs::remove_file(&source_path);
                state.sub_quota_usage(old_size);
            }
            completed.push(RepackOperationResult {
                source_pack: op.source_pack.clone(),
                new_pack: None,
                new_offsets: vec![],
                deleted: op.delete_after,
            });
            continue;
        }

        // Open source pack.
        let mut source = std::fs::File::open(&source_path)
            .map_err(|e| format!("open {}: {e}", op.source_pack))?;
        let source_len = source
            .metadata()
            .map_err(|e| format!("stat {}: {e}", op.source_pack))?
            .len();

        // Create temp file for streaming write.
        let temp_id = REPACK_TEMP_COUNTER.fetch_add(1, Relaxed);
        let temp_path = source_path.with_file_name(format!(".repack_tmp.{temp_id}"));

        // Write new pack to temp file. Collect write errors so we can
        // drop the file handle before cleanup (required on Windows).
        let temp_file =
            std::fs::File::create(&temp_path).map_err(|e| format!("create temp: {e}"))?;
        let mut writer = BufWriter::new(temp_file);
        let mut hasher = Blake2bVar::new(32).expect("valid output size");

        let mut pack_offset: u64 =
            u64::try_from(PACK_HEADER_SIZE).expect("pack header size fits u64");
        let mut new_offsets = Vec::with_capacity(op.keep_blobs.len());
        let mut scratch = Vec::new();

        let write_result: Result<(), String> = (|| {
            // Write pack header: magic + version.
            write_and_hash(&mut writer, &mut hasher, PACK_MAGIC)
                .map_err(|e| format!("write header: {e}"))?;
            write_and_hash(&mut writer, &mut hasher, &[PACK_VERSION_CURRENT])
                .map_err(|e| format!("write version: {e}"))?;

            for blob_ref in &op.keep_blobs {
                if blob_ref.length == 0 {
                    return Err("repack blob length must be > 0".to_string());
                }
                let end = blob_ref
                    .offset
                    .checked_add(blob_ref.length)
                    .ok_or_else(|| "repack blob range overflow".to_string())?;
                if end > source_len {
                    return Err(format!(
                        "repack blob range out of bounds: offset={} length={} file_size={source_len}",
                        blob_ref.offset, blob_ref.length
                    ));
                }

                // Cross-check the on-disk 4-byte length prefix against the
                // requested blob length. The prefix lives at offset - 4; if
                // it doesn't match, the client's index metadata is stale.
                let prefix_offset = blob_ref.offset.checked_sub(4).ok_or_else(|| {
                    format!(
                        "repack blob at offset {} has no room for length prefix",
                        blob_ref.offset
                    )
                })?;
                let mut prefix_buf = [0u8; 4];
                source
                    .seek(SeekFrom::Start(prefix_offset))
                    .map_err(|e| format!("seek prefix: {e}"))?;
                source
                    .read_exact(&mut prefix_buf)
                    .map_err(|e| format!("read prefix: {e}"))?;
                let on_disk_len = u32::from_le_bytes(prefix_buf);
                if u64::from(on_disk_len) != blob_ref.length {
                    return Err(format!(
                        "repack blob at offset {}: on-disk length prefix ({}) \
                         does not match requested length ({})",
                        blob_ref.offset, on_disk_len, blob_ref.length
                    ));
                }

                // Read blob from source into reusable scratch buffer
                // (cursor is already at blob_ref.offset after reading the prefix).
                let blob_len_usize = usize::try_from(blob_ref.length)
                    .map_err(|_| "repack blob length does not fit usize".to_string())?;
                scratch.resize(blob_len_usize, 0);
                source
                    .read_exact(&mut scratch)
                    .map_err(|e| format!("read: {e}"))?;

                // Write length prefix.
                let blob_len = u32::try_from(blob_ref.length)
                    .map_err(|_| "repack blob length exceeds pack format max".to_string())?;
                write_and_hash(&mut writer, &mut hasher, &blob_len.to_le_bytes())
                    .map_err(|e| format!("write len: {e}"))?;

                // Record offset past the length prefix.
                new_offsets.push(pack_offset + 4);

                // Write blob data.
                write_and_hash(&mut writer, &mut hasher, &scratch)
                    .map_err(|e| format!("write blob: {e}"))?;

                pack_offset += 4 + blob_ref.length;
            }

            writer.flush().map_err(|e| format!("flush: {e}"))?;
            Ok(())
        })();

        // Drop writer (closes file handle) before any cleanup or rename.
        drop(writer);

        if let Err(e) = write_result {
            let _ = std::fs::remove_file(&temp_path);
            return Err(e);
        }

        // Finalize hash -> pack ID.
        let pack_id_hex = finalize_blake2b_256_hex(hasher);
        let shard = &pack_id_hex[..2];
        let new_pack_key = format!("packs/{shard}/{pack_id_hex}");

        let new_pack_path = state.file_path(&new_pack_key).ok_or_else(|| {
            let _ = std::fs::remove_file(&temp_path);
            "invalid new pack path".to_string()
        })?;

        if let Some(parent) = new_pack_path.parent() {
            std::fs::create_dir_all(parent).map_err(|e| {
                let _ = std::fs::remove_file(&temp_path);
                format!("mkdir: {e}")
            })?;
        }

        std::fs::rename(&temp_path, &new_pack_path).map_err(|e| {
            let _ = std::fs::remove_file(&temp_path);
            format!("rename new pack: {e}")
        })?;

        // Update quota.
        state.add_quota_usage(pack_offset);

        // Delete source if requested.
        if op.delete_after {
            let old_size = source_path.metadata().map_or(0, |m| m.len());
            let _ = std::fs::remove_file(&source_path);
            state.sub_quota_usage(old_size);
        }

        completed.push(RepackOperationResult {
            source_pack: op.source_pack.clone(),
            new_pack: Some(new_pack_key),
            new_offsets,
            deleted: op.delete_after,
        });
    }

    Ok(RepackResultResponse { completed })
}

/// Write data to writer and feed to hasher in one step.
fn write_and_hash(
    writer: &mut impl std::io::Write,
    hasher: &mut Blake2bVar,
    data: &[u8],
) -> std::io::Result<()> {
    writer.write_all(data)?;
    hasher.update(data);
    Ok(())
}

fn finalize_blake2b_256_hex(hasher: Blake2bVar) -> String {
    let mut out = [0u8; 32];
    hasher
        .finalize_variable(&mut out)
        .expect("valid output buffer length");
    hex::encode(out)
}

fn validate_repack_plan(plan: &RepackPlanRequest) -> Result<(), ServerError> {
    check_protocol_version(plan.protocol_version).map_err(ServerError::BadRequest)?;
    if plan.operations.len() > MAX_REPACK_OPS {
        return Err(ServerError::BadRequest(format!(
            "too many repack operations: {} (max {MAX_REPACK_OPS})",
            plan.operations.len()
        )));
    }
    for (idx, op) in plan.operations.iter().enumerate() {
        if !is_valid_pack_key(&op.source_pack) {
            return Err(ServerError::BadRequest(format!(
                "invalid source_pack at operation {idx}: {}",
                op.source_pack
            )));
        }
        if op.keep_blobs.len() > MAX_KEEP_BLOBS_PER_OP {
            return Err(ServerError::BadRequest(format!(
                "too many keep_blobs at operation {idx}: {} (max {MAX_KEEP_BLOBS_PER_OP})",
                op.keep_blobs.len()
            )));
        }
        for (blob_idx, blob) in op.keep_blobs.iter().enumerate() {
            validate_blob_ref(
                blob.offset,
                blob.length,
                &format!("operation {idx} blob {blob_idx}"),
            )
            .map_err(ServerError::BadRequest)?;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use axum::http::StatusCode;

    use super::super::test_support::*;
    use crate::handlers::test_helpers::*;
    use vykar_protocol::PACK_MAGIC;

    #[tokio::test]
    async fn repack_single_blob() {
        let (router, _state, tmp) = setup_app(0);

        let blob = b"hello world repack";
        let (pack_bytes, refs) = build_pack(&[blob]);
        let source_key = write_pack(tmp.path(), &pack_bytes);

        let body = repack_body(&[repack_op(&source_key, &refs, false)]);
        let resp = authed_post(router.clone(), "/?repack", body).await;
        assert_status(&resp, StatusCode::OK);

        let result: serde_json::Value = serde_json::from_slice(&body_bytes(resp).await).unwrap();
        let completed = result["completed"].as_array().unwrap();
        assert_eq!(completed.len(), 1);

        let op = &completed[0];
        let new_pack_key = op["new_pack"].as_str().unwrap();
        assert!(new_pack_key.starts_with("packs/"));

        // Read the new pack file and verify magic + version.
        let new_pack_path = tmp.path().join(new_pack_key);
        let new_pack_data = std::fs::read(&new_pack_path).expect("read new pack");
        assert_eq!(&new_pack_data[..8], PACK_MAGIC);
        assert_eq!(new_pack_data[8], 0x01);

        // Verify the returned offset points to the correct blob data.
        let new_offset = op["new_offsets"][0].as_u64().unwrap();
        let blob_data = &new_pack_data[new_offset as usize..new_offset as usize + blob.len()];
        assert_eq!(blob_data, blob);
    }

    #[tokio::test]
    async fn repack_multiple_blobs_offsets() {
        let (router, _state, tmp) = setup_app(0);

        let blobs: Vec<&[u8]> = vec![b"first", b"second-blob", b"third!!"];
        let (pack_bytes, refs) = build_pack(&blobs);
        let source_key = write_pack(tmp.path(), &pack_bytes);

        let body = repack_body(&[repack_op(&source_key, &refs, false)]);
        let resp = authed_post(router, "/?repack", body).await;
        assert_status(&resp, StatusCode::OK);

        let result: serde_json::Value = serde_json::from_slice(&body_bytes(resp).await).unwrap();
        let op = &result["completed"][0];
        let new_pack_key = op["new_pack"].as_str().unwrap();
        let new_offsets: Vec<u64> = op["new_offsets"]
            .as_array()
            .unwrap()
            .iter()
            .map(|v| v.as_u64().unwrap())
            .collect();
        assert_eq!(new_offsets.len(), 3);

        let new_pack_path = tmp.path().join(new_pack_key);
        let new_pack_data = std::fs::read(&new_pack_path).expect("read new pack");

        // Each returned offset should read back the original blob.
        for (i, blob) in blobs.iter().enumerate() {
            let off = new_offsets[i] as usize;
            let actual = &new_pack_data[off..off + blob.len()];
            assert_eq!(actual, *blob, "blob {i} mismatch");
        }
    }

    #[tokio::test]
    async fn repack_hash_matches_content() {
        let (router, _state, tmp) = setup_app(0);

        let (pack_bytes, refs) = build_pack(&[b"hash-check-data"]);
        let source_key = write_pack(tmp.path(), &pack_bytes);

        let body = repack_body(&[repack_op(&source_key, &refs, false)]);
        let resp = authed_post(router, "/?repack", body).await;
        assert_status(&resp, StatusCode::OK);

        let result: serde_json::Value = serde_json::from_slice(&body_bytes(resp).await).unwrap();
        let new_pack_key = result["completed"][0]["new_pack"].as_str().unwrap();

        // The pack key is packs/<shard>/<hex>. The hex should be blake2b-256 of contents.
        let pack_hex = new_pack_key.split('/').next_back().unwrap();
        let new_pack_path = tmp.path().join(new_pack_key);
        let new_pack_data = std::fs::read(&new_pack_path).expect("read new pack");
        let actual_hash = blake2b_256_hex(&new_pack_data);
        assert_eq!(actual_hash, pack_hex);
    }

    #[tokio::test]
    async fn repack_delete_source() {
        let (router, state, tmp) = setup_app(0);

        let (pack_bytes, refs) = build_pack(&[b"delete-me"]);
        let source_key = write_pack(tmp.path(), &pack_bytes);
        let source_path = tmp.path().join(&source_key);
        assert!(source_path.exists());

        // Seed quota with the source pack size.
        state.add_quota_usage(pack_bytes.len() as u64);

        let body = repack_body(&[repack_op(&source_key, &refs, true)]);
        let resp = authed_post(router, "/?repack", body).await;
        assert_status(&resp, StatusCode::OK);

        let result: serde_json::Value = serde_json::from_slice(&body_bytes(resp).await).unwrap();
        let op = &result["completed"][0];
        assert!(op["deleted"].as_bool().unwrap());
        assert!(op["new_pack"].as_str().is_some());

        // Source should be gone.
        assert!(!source_path.exists(), "source pack not deleted");
    }

    #[tokio::test]
    async fn repack_empty_keeps_deletes_only() {
        let (router, state, tmp) = setup_app(0);

        let (pack_bytes, _refs) = build_pack(&[b"going-away"]);
        let source_key = write_pack(tmp.path(), &pack_bytes);
        let source_path = tmp.path().join(&source_key);

        state.add_quota_usage(pack_bytes.len() as u64);
        let used_before = state.quota_used();

        // Repack with empty keep_blobs + delete_after.
        let body = repack_body(&[repack_op(&source_key, &[], true)]);
        let resp = authed_post(router, "/?repack", body).await;
        assert_status(&resp, StatusCode::OK);

        let result: serde_json::Value = serde_json::from_slice(&body_bytes(resp).await).unwrap();
        let op = &result["completed"][0];
        assert!(op["new_pack"].is_null());
        assert!(op["deleted"].as_bool().unwrap());
        assert!(op["new_offsets"].as_array().unwrap().is_empty());

        assert!(!source_path.exists(), "source pack not deleted");
        assert!(
            state.quota_used() < used_before,
            "quota should have decreased"
        );
    }
}
