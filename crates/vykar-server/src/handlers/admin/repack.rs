use std::sync::atomic::{AtomicU64, Ordering::Relaxed};

use axum::response::{IntoResponse, Response};
use blake2::digest::{Update, VariableOutput};
use blake2::Blake2bVar;

use vykar_protocol::{
    check_protocol_version, is_valid_pack_key, repack_op_output_size, validate_blob_ref,
    RepackOperationResult, RepackPlanRequest, RepackResultResponse, PACK_HEADER_SIZE, PACK_MAGIC,
    PACK_VERSION_CURRENT,
};

use crate::error::ServerError;
use crate::state::{AppState, QuotaReservation};

static REPACK_TEMP_COUNTER: AtomicU64 = AtomicU64::new(0);

const MAX_REPACK_OPS: usize = 10_000;
const MAX_KEEP_BLOBS_PER_OP: usize = 200_000;
/// Maximum total output bytes a single repack plan may produce. Shared with the
/// client (which chunks larger plans) via `vykar_protocol`.
const MAX_REPACK_BYTES: u64 = vykar_protocol::MAX_REPACK_OUTPUT_BYTES;

pub(super) async fn repack(
    state: AppState,
    body: axum::body::Bytes,
) -> Result<Response, ServerError> {
    let plan: RepackPlanRequest = serde_json::from_slice(&body)
        .map_err(|e| ServerError::BadRequest(format!("invalid repack plan: {e}")))?;
    let total_output = validate_repack_plan(&plan)?;

    if state.inner.config.append_only && plan.operations.iter().any(|op| op.delete_after) {
        return Err(ServerError::Forbidden(
            "append-only: repack with delete not allowed".into(),
        ));
    }

    // Reserve the whole plan's output up-front. This is intentional even for
    // net-freeing plans: the new packs exist on disk before their sources are
    // deleted, so peak usage — not the final delta — is what the quota must
    // admit. `commit_partial` moves each op's bytes from reserved to committed
    // as it lands; any remainder (early error / panic) is released on Drop.
    let mut reservation = state
        .try_reserve_quota(total_output)
        .map_err(|(used, limit)| {
            ServerError::PayloadTooLarge(format!(
                "quota exceeded: used {used}, limit {limit}, repack output {total_output}"
            ))
        })?;

    let state_clone = state.clone();

    let results =
        tokio::task::spawn_blocking(move || execute_repack(&state_clone, &plan, &mut reservation))
            .await
            .map_err(|e| ServerError::Internal(e.to_string()))?
            .map_err(ServerError::Internal)?;

    Ok(axum::Json(results).into_response())
}

/// Execute a repack plan all-or-nothing with respect to destruction: phase A
/// writes and renames every new pack (rolling back renamed packs on any
/// error, so a failed plan deletes nothing), and only then does phase B
/// delete the source packs. Without this ordering, an op failing mid-plan
/// would return a bare 500 after earlier ops already deleted their sources,
/// leaving the client unable to point its index at the new packs.
///
/// Residual window: a crash (or lost success response) between phase B and
/// the client receiving the results still strands deleted sources; closing
/// that fully would need a client-driven delete protocol.
#[allow(clippy::too_many_lines)]
fn execute_repack(
    state: &AppState,
    plan: &RepackPlanRequest,
    reservation: &mut QuotaReservation,
) -> Result<RepackResultResponse, String> {
    use std::io::{BufWriter, Read, Seek, SeekFrom, Write};

    let mut completed = Vec::new();
    // New packs renamed into place so far: (path, committed bytes).
    let mut renamed: Vec<(std::path::PathBuf, u64)> = Vec::new();
    // Every path returned as some op's new pack (renamed or already present):
    // phase B must never delete these, they carry live data.
    let mut result_packs: std::collections::HashSet<std::path::PathBuf> =
        std::collections::HashSet::new();
    // Source packs to delete in phase B (includes delete-only ops).
    let mut pending_delete: Vec<std::path::PathBuf> = Vec::new();

    // Resolve every source path up front: fails before any I/O, and gives the
    // rollback below the complete set of paths it must never delete.
    let mut source_paths = Vec::with_capacity(plan.operations.len());
    for op in &plan.operations {
        source_paths.push(
            state
                .file_path(&op.source_pack)
                .ok_or_else(|| "invalid pack path".to_string())?,
        );
    }

    // Phase A: write, fsync, and rename every new pack. No source is touched.
    let phase_a: Result<(), String> = (|| {
        for (op, source_path) in plan.operations.iter().zip(&source_paths) {
            if op.keep_blobs.is_empty() {
                // Delete-only op: the deletion is the destructive action, so it
                // is deferred to phase B like every other source deletion.
                if op.delete_after {
                    pending_delete.push(source_path.clone());
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
            let mut source = std::fs::File::open(source_path)
                .map_err(|e| format!("open {}: {e}", op.source_pack))?;
            let source_len = source
                .metadata()
                .map_err(|e| format!("stat {}: {e}", op.source_pack))?
                .len();

            // Create temp file for streaming write.
            let temp_id = REPACK_TEMP_COUNTER.fetch_add(1, Relaxed);
            // Use the unified `.tmp.` prefix so `vykar_protocol::is_temp_file`
            // recognizes repack debris everywhere (cleanup, append-only delete).
            let temp_path = source_path.with_file_name(format!(".tmp.repack.{temp_id}"));

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

            if let Err(e) = write_result {
                drop(writer);
                let _ = std::fs::remove_file(&temp_path);
                return Err(e);
            }

            // Flush the BufWriter and fsync the temp file before rename so its
            // contents survive power loss. into_inner() closes the file handle
            // (required on Windows before rename); sync_data() persists the bytes.
            let temp_file = writer.into_inner().map_err(|e| {
                let _ = std::fs::remove_file(&temp_path);
                format!("flush temp: {e}")
            })?;
            temp_file.sync_data().map_err(|e| {
                let _ = std::fs::remove_file(&temp_path);
                format!("sync temp: {e}")
            })?;
            drop(temp_file);

            // Finalize hash -> pack ID.
            let pack_id_hex = finalize_blake2b_256_hex(hasher);
            let shard = &pack_id_hex[..2];
            let new_pack_key = format!("packs/{shard}/{pack_id_hex}");

            let new_pack_path = state.file_path(&new_pack_key).ok_or_else(|| {
                let _ = std::fs::remove_file(&temp_path);
                "invalid new pack path".to_string()
            })?;

            // Content-addressed identity: if the target already exists, its
            // bytes already are exactly this op's output — whether it is the
            // op's own source (keep-everything op) or a pre-existing pack
            // (e.g. from an earlier attempt whose response was lost). Keep
            // it, discard the temp, and commit no quota. It must never be
            // renamed over and tracked for rollback: a pre-existing pack may
            // already be referenced by the client index, so rollback removal
            // would destroy live data.
            if new_pack_path == *source_path || new_pack_path.exists() {
                let _ = std::fs::remove_file(&temp_path);
                let deletes_source = op.delete_after && new_pack_path != *source_path;
                if deletes_source {
                    pending_delete.push(source_path.clone());
                }
                result_packs.insert(new_pack_path);
                completed.push(RepackOperationResult {
                    source_pack: op.source_pack.clone(),
                    new_pack: Some(new_pack_key),
                    new_offsets,
                    deleted: deletes_source,
                });
                continue;
            }

            if let Some(parent) = new_pack_path.parent() {
                let parent_existed = parent.exists();
                std::fs::create_dir_all(parent).map_err(|e| {
                    let _ = std::fs::remove_file(&temp_path);
                    format!("mkdir: {e}")
                })?;
                // Fsync the newly-created ancestor chain up to data_dir (rare path).
                if !parent_existed {
                    let data_dir = state.inner.data_dir.as_path();
                    let mut cursor = Some(parent);
                    while let Some(dir) = cursor {
                        crate::state::fsync_dir(dir).map_err(|e| {
                            let _ = std::fs::remove_file(&temp_path);
                            format!("fsync dir: {e}")
                        })?;
                        if dir == data_dir {
                            break;
                        }
                        cursor = dir.parent();
                    }
                }
            }

            std::fs::rename(&temp_path, &new_pack_path).map_err(|e| {
                let _ = std::fs::remove_file(&temp_path);
                format!("rename new pack: {e}")
            })?;

            // Commit accounting immediately after the rename: the bytes are on
            // disk even if the fsync below fails, and undercounting is the unsafe
            // direction (rollback subtracts again for packs it removes).
            reservation.commit_partial(pack_offset);
            renamed.push((new_pack_path.clone(), pack_offset));
            result_packs.insert(new_pack_path.clone());

            // Fsync the parent so the rename survives power loss before we ack.
            if let Some(parent) = new_pack_path.parent() {
                crate::state::fsync_dir(parent).map_err(|e| format!("fsync new pack dir: {e}"))?;
            }

            if op.delete_after {
                pending_delete.push(source_path.clone());
            }

            completed.push(RepackOperationResult {
                source_pack: op.source_pack.clone(),
                new_pack: Some(new_pack_key),
                new_offsets,
                deleted: op.delete_after,
            });
        }
        Ok(())
    })();

    if let Err(e) = phase_a {
        // Roll back: a failed plan must destroy nothing. Every renamed path
        // was freshly created by this plan (existing targets take the
        // already-present branch above and are never renamed over), so
        // removing them cannot destroy referenced data. Quota is subtracted
        // only for packs actually removed.
        for (path, bytes) in &renamed {
            if std::fs::remove_file(path).is_ok() {
                state.sub_quota_usage(*bytes);
            }
        }
        return Err(e);
    }

    // Phase B: every new pack is durable; delete the sources. Errors are
    // ignored — an undeleted source is unreferenced garbage once the client
    // applies the results. Non-goal: fsync after removal — a resurrected
    // deleted source pack is likewise unreferenced garbage, not a correctness
    // issue.
    for source_path in pending_delete {
        // Never delete a path that is also some op's result pack
        // (byte-identical cross-op output) — it carries live data.
        if result_packs.contains(&source_path) {
            continue;
        }
        let old_size = source_path.metadata().map_or(0, |m| m.len());
        if std::fs::remove_file(&source_path).is_ok() {
            state.sub_quota_usage(old_size);
        }
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

/// Validate the plan and return its total output size in bytes.
///
/// A single validation-layer cap suffices (unlike verify-packs' two layers):
/// output size is exactly `PACK_HEADER_SIZE + Σ(4 + blob.length)` per op, and
/// blob lengths are cross-checked against the on-disk length prefixes during
/// execution, so a declared oversize plan is rejected before any file access.
fn validate_repack_plan(plan: &RepackPlanRequest) -> Result<u64, ServerError> {
    check_protocol_version(plan.protocol_version).map_err(ServerError::BadRequest)?;
    if plan.operations.len() > MAX_REPACK_OPS {
        return Err(ServerError::BadRequest(format!(
            "too many repack operations: {} (max {MAX_REPACK_OPS})",
            plan.operations.len()
        )));
    }
    let mut total_output: u64 = 0;
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
        total_output = total_output.saturating_add(repack_op_output_size(op));
    }
    if total_output > MAX_REPACK_BYTES {
        return Err(ServerError::BadRequest(format!(
            "repack output too large: {total_output} bytes (max {MAX_REPACK_BYTES})"
        )));
    }
    Ok(total_output)
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

        // Keep a strict subset so the new pack differs from the source
        // (a keep-everything op is the identity case tested separately).
        let (pack_bytes, refs) = build_pack(&[b"keep-me", b"delete-me"]);
        let source_key = write_pack(tmp.path(), &pack_bytes);
        let source_path = tmp.path().join(&source_key);
        assert!(source_path.exists());

        // Seed quota with the source pack size.
        state.add_quota_usage(pack_bytes.len() as u64);

        let body = repack_body(&[repack_op(&source_key, &refs[..1], true)]);
        let resp = authed_post(router, "/?repack", body).await;
        assert_status(&resp, StatusCode::OK);

        let result: serde_json::Value = serde_json::from_slice(&body_bytes(resp).await).unwrap();
        let op = &result["completed"][0];
        assert!(op["deleted"].as_bool().unwrap());
        let new_pack_key = op["new_pack"].as_str().unwrap();
        assert!(
            tmp.path().join(new_pack_key).exists(),
            "new pack must exist"
        );

        // Source should be gone.
        assert!(!source_path.exists(), "source pack not deleted");
    }

    #[tokio::test]
    async fn repack_keep_everything_is_identity() {
        // A keep-everything op produces byte-identical output, so the "new"
        // pack IS the source. The op must not destroy it (the old code renamed
        // onto the source and then deleted it as the "source"), and must
        // report deleted: false.
        let (router, state, tmp) = setup_app(0);
        let (pack_bytes, refs) = build_pack(&[b"identical-content"]);
        let source_key = write_pack(tmp.path(), &pack_bytes);
        let source_path = tmp.path().join(&source_key);
        state.add_quota_usage(pack_bytes.len() as u64);
        let quota_before = state.quota_used();

        let body = repack_body(&[repack_op(&source_key, &refs, true)]);
        let resp = authed_post(router, "/?repack", body).await;
        assert_status(&resp, StatusCode::OK);

        let result: serde_json::Value = serde_json::from_slice(&body_bytes(resp).await).unwrap();
        let op = &result["completed"][0];
        assert_eq!(op["new_pack"].as_str().unwrap(), source_key);
        assert!(!op["deleted"].as_bool().unwrap(), "nothing was deleted");
        assert!(source_path.exists(), "pack must survive identity repack");
        assert_eq!(state.quota_used(), quota_before, "no accounting change");
        assert_eq!(state.quota_reserved(), 0);
        assert_no_temp_files(tmp.path());
    }

    #[tokio::test]
    async fn repack_oversized_plan_rejected() {
        // Declared output over 2 GiB is rejected in validation, before any file
        // access — no need to write a 2 GiB pack.
        let (router, _state, _tmp) = setup_app(0);
        let source_key = format!("packs/ab/{}", "ab".repeat(32));
        // One blob of exactly 2 GiB pushes header + 4 + length over the cap.
        let body = repack_body(&[repack_op(
            &source_key,
            &[(13, 2 * 1024 * 1024 * 1024)],
            false,
        )]);
        let resp = authed_post(router, "/?repack", body).await;
        assert_status(&resp, StatusCode::BAD_REQUEST);
        let text = String::from_utf8(body_bytes(resp).await).unwrap();
        assert!(text.contains("output too large"), "got: {text}");
    }

    #[tokio::test]
    async fn repack_over_quota_rejected() {
        // Output size exceeds the quota: 413, source untouched, nothing committed.
        let (router, state, tmp) = setup_app(10);
        let (pack_bytes, refs) = build_pack(&[b"delete-me"]);
        let source_key = write_pack(tmp.path(), &pack_bytes);
        let source_path = tmp.path().join(&source_key);

        let body = repack_body(&[repack_op(&source_key, &refs, true)]);
        let resp = authed_post(router, "/?repack", body).await;
        assert_status(&resp, StatusCode::PAYLOAD_TOO_LARGE);

        assert!(source_path.exists(), "source untouched on quota rejection");
        assert_eq!(state.quota_reserved(), 0, "reservation released");
        assert_no_temp_files(tmp.path());
    }

    #[tokio::test]
    async fn repack_existing_new_pack_survives_rollback() {
        // The pack an op's output hashes to already exists (e.g. from an
        // earlier attempt whose response was lost) and may be referenced by
        // the client index. A later op's failure must roll back without
        // touching it: it was never renamed over, so it is not rollback's to
        // delete.
        let (router, state, tmp) = setup_app(0);
        let (pack_bytes, refs) = build_pack(&[b"pre-existing", b"dead-blob"]);
        let source_key = write_pack(tmp.path(), &pack_bytes);
        // Pre-create the exact pack op 1 would produce (header + kept blob).
        let (expected_output, _) = build_pack(&[b"pre-existing"]);
        let existing_key = write_pack(tmp.path(), &expected_output);
        state.add_quota_usage((pack_bytes.len() + expected_output.len()) as u64);
        let quota_before = state.quota_used();

        let missing_key = format!("packs/cd/{}", "cd".repeat(32));
        let body = repack_body(&[
            repack_op(&source_key, &refs[..1], true),
            repack_op(&missing_key, &[(13, 5)], true),
        ]);
        let resp = authed_post(router, "/?repack", body).await;
        assert_status(&resp, StatusCode::INTERNAL_SERVER_ERROR);

        assert!(
            tmp.path().join(&existing_key).exists(),
            "pre-existing pack must survive rollback"
        );
        assert!(
            tmp.path().join(&source_key).exists(),
            "source must survive rollback"
        );
        assert_eq!(state.quota_used(), quota_before, "no accounting change");
        assert_eq!(state.quota_reserved(), 0, "reservation released");
        assert_no_temp_files(tmp.path());
    }

    #[tokio::test]
    async fn repack_existing_new_pack_reused() {
        // Success path of the same case: the op's output already exists, so it
        // is returned as-is (no rename, no quota commit) and the source is
        // still deleted per delete_after.
        let (router, state, tmp) = setup_app(0);
        let (pack_bytes, refs) = build_pack(&[b"pre-existing", b"dead-blob"]);
        let source_key = write_pack(tmp.path(), &pack_bytes);
        let (expected_output, _) = build_pack(&[b"pre-existing"]);
        let existing_key = write_pack(tmp.path(), &expected_output);
        state.add_quota_usage((pack_bytes.len() + expected_output.len()) as u64);
        let quota_before = state.quota_used();

        let body = repack_body(&[repack_op(&source_key, &refs[..1], true)]);
        let resp = authed_post(router, "/?repack", body).await;
        assert_status(&resp, StatusCode::OK);

        let result: serde_json::Value = serde_json::from_slice(&body_bytes(resp).await).unwrap();
        let op = &result["completed"][0];
        assert_eq!(op["new_pack"].as_str().unwrap(), existing_key);
        assert!(op["deleted"].as_bool().unwrap());
        assert!(tmp.path().join(&existing_key).exists(), "new pack kept");
        assert!(!tmp.path().join(&source_key).exists(), "source deleted");
        // No commit for the already-present pack; only the source freed.
        assert_eq!(state.quota_used(), quota_before - pack_bytes.len() as u64);
        assert_eq!(state.quota_reserved(), 0);
        assert_no_temp_files(tmp.path());
    }

    #[tokio::test]
    async fn repack_failed_plan_deletes_nothing() {
        // All-or-nothing: op 1 is valid, op 2 references a missing source pack
        // and fails in phase A. The plan must destroy nothing: op 1's source
        // survives, its already-renamed new pack is rolled back, and quota
        // accounting returns to the starting state.
        let (router, state, tmp) = setup_app(0);
        // Keep a strict subset so op 1 really renames a new pack into place
        // (the rollback path under test).
        let (pack_bytes, refs) = build_pack(&[b"survives-a-failed-plan", b"dead-blob"]);
        let source_key = write_pack(tmp.path(), &pack_bytes);
        let source_path = tmp.path().join(&source_key);
        state.add_quota_usage(pack_bytes.len() as u64);
        let quota_before = state.quota_used();

        let missing_key = format!("packs/cd/{}", "cd".repeat(32));
        let body = repack_body(&[
            repack_op(&source_key, &refs[..1], true),
            repack_op(&missing_key, &[(13, 5)], true),
        ]);
        let resp = authed_post(router, "/?repack", body).await;
        assert_status(&resp, StatusCode::INTERNAL_SERVER_ERROR);

        assert!(source_path.exists(), "op 1 source must survive");
        let pack_files: Vec<_> = walk_file_paths(&tmp.path().join("packs"));
        assert_eq!(
            pack_files,
            vec![source_path],
            "rolled-back new pack must not remain"
        );
        assert_eq!(state.quota_used(), quota_before, "accounting rolled back");
        assert_eq!(state.quota_reserved(), 0, "reservation released");
        assert_no_temp_files(tmp.path());
    }

    #[tokio::test]
    async fn repack_quota_reflects_net_change() {
        let (router, state, tmp) = setup_app(0); // unlimited
                                                 // Keep a strict subset so the repack actually shrinks the pack.
        let (pack_bytes, refs) = build_pack(&[b"keep-this-blob", b"dead-blob"]);
        let source_key = write_pack(tmp.path(), &pack_bytes);
        state.add_quota_usage(pack_bytes.len() as u64);
        let before = state.quota_used();

        let body = repack_body(&[repack_op(&source_key, &refs[..1], true)]);
        let resp = authed_post(router, "/?repack", body).await;
        assert_status(&resp, StatusCode::OK);

        // Committed usage = old - deleted-source + new-pack output.
        let op_out = 9 + 4 + "keep-this-blob".len() as u64;
        assert_eq!(
            state.quota_used(),
            before - pack_bytes.len() as u64 + op_out
        );
        assert_eq!(state.quota_reserved(), 0, "reservation fully committed");
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
