use axum::response::{IntoResponse, Response};
use blake2::digest::{Update, VariableOutput};
use blake2::Blake2bVar;

use vykar_protocol::{
    check_protocol_version, is_valid_pack_key, validate_blob_ref, VerifyPackRequest,
    VerifyPackResult, VerifyPacksPlanRequest, VerifyPacksResponse, PACK_HEADER_SIZE, PACK_MAGIC,
    PACK_VERSION_MAX, PACK_VERSION_MIN,
};

use crate::error::ServerError;
use crate::state::AppState;

/// Must match or slightly exceed client-side `SERVER_VERIFY_BATCH_SIZE` (100).
const MAX_VERIFY_PACKS: usize = 100;
/// Maximum estimated bytes of pack I/O per verify request (matches client-side cap).
const MAX_VERIFY_BYTES: u64 = 2 * 1024 * 1024 * 1024; // 2 GiB

pub(super) async fn verify_packs(
    state: AppState,
    body: axum::body::Bytes,
) -> Result<Response, ServerError> {
    let plan: VerifyPacksPlanRequest = serde_json::from_slice(&body)
        .map_err(|e| ServerError::BadRequest(format!("invalid verify-packs plan: {e}")))?;
    validate_verify_packs_plan(&plan)?;

    let state_clone = state.clone();

    let results = tokio::task::spawn_blocking(move || execute_verify_packs(&state_clone, &plan))
        .await
        .map_err(|e| ServerError::Internal(e.to_string()))?;

    Ok(axum::Json(results).into_response())
}

fn validate_verify_packs_plan(plan: &VerifyPacksPlanRequest) -> Result<(), ServerError> {
    check_protocol_version(plan.protocol_version).map_err(ServerError::BadRequest)?;
    if plan.packs.len() > MAX_VERIFY_PACKS {
        return Err(ServerError::BadRequest(format!(
            "too many packs to verify: {} (max {MAX_VERIFY_PACKS})",
            plan.packs.len()
        )));
    }

    // Byte-volume gate: the server reads each full pack from disk, so bound
    // total I/O using the client-declared expected_size per pack.
    let mut total_bytes: u64 = 0;

    for (idx, entry) in plan.packs.iter().enumerate() {
        if !is_valid_pack_key(&entry.pack_key) {
            return Err(ServerError::BadRequest(format!(
                "invalid pack_key at pack {idx}: {}",
                entry.pack_key
            )));
        }
        if entry.expected_size == 0 {
            return Err(ServerError::BadRequest(format!(
                "expected_size must be > 0 at pack {idx}"
            )));
        }

        for (blob_idx, blob) in entry.expected_blobs.iter().enumerate() {
            validate_blob_ref(
                blob.offset,
                blob.length,
                &format!("pack {idx} blob {blob_idx}"),
            )
            .map_err(ServerError::BadRequest)?;
        }
        total_bytes = total_bytes.saturating_add(entry.expected_size);
    }

    if total_bytes > MAX_VERIFY_BYTES {
        return Err(ServerError::BadRequest(format!(
            "estimated verify I/O too large: {total_bytes} bytes (max {MAX_VERIFY_BYTES})"
        )));
    }

    Ok(())
}

fn execute_verify_packs(state: &AppState, plan: &VerifyPacksPlanRequest) -> VerifyPacksResponse {
    let mut results = Vec::with_capacity(plan.packs.len());
    let mut bytes_read: u64 = 0;
    let mut truncated = false;

    for entry in &plan.packs {
        // Stat before reading to enforce byte cap with actual file sizes.
        let Some(file_path) = state.file_path(&entry.pack_key) else {
            results.push(VerifyPackResult {
                pack_key: entry.pack_key.clone(),
                hash_valid: false,
                header_valid: false,
                blobs_valid: false,
                error: Some("invalid pack path".into()),
            });
            continue;
        };
        let file_size = match std::fs::metadata(&file_path) {
            Ok(m) => m.len(),
            Err(e) => {
                results.push(VerifyPackResult {
                    pack_key: entry.pack_key.clone(),
                    hash_valid: false,
                    header_valid: false,
                    blobs_valid: false,
                    error: Some(format!("stat failed: {e}")),
                });
                continue;
            }
        };
        if bytes_read.saturating_add(file_size) > MAX_VERIFY_BYTES {
            truncated = true;
            break;
        }
        bytes_read += file_size;
        results.push(verify_single_pack(&file_path, entry));
    }

    VerifyPacksResponse { results, truncated }
}

fn verify_single_pack(file_path: &std::path::Path, entry: &VerifyPackRequest) -> VerifyPackResult {
    let file = match std::fs::File::open(file_path) {
        Ok(f) => f,
        Err(e) => {
            return VerifyPackResult {
                pack_key: entry.pack_key.clone(),
                hash_valid: false,
                header_valid: false,
                blobs_valid: false,
                error: Some(format!("read failed: {e}")),
            };
        }
    };
    let file_len = match file.metadata() {
        Ok(m) => m.len(),
        Err(e) => {
            return VerifyPackResult {
                pack_key: entry.pack_key.clone(),
                hash_valid: false,
                header_valid: false,
                blobs_valid: false,
                error: Some(format!("stat failed: {e}")),
            };
        }
    };
    verify_pack_from_reader(file, file_len, entry, 256 * 1024)
}

/// Streaming pack verification: computes hash, validates header, and checks
/// blob boundaries in a single pass without loading the entire pack into memory.
///
/// Accepts a generic reader so tests can use `Cursor<Vec<u8>>` with small
/// buffer sizes to exercise boundary splits.
#[allow(clippy::too_many_lines)]
// Bounds for indexing here are enforced by `min(avail.len(), total - filled)`
// and `header_buf[..8]` length checks; clippy can't prove these.
#[allow(clippy::indexing_slicing)]
fn verify_pack_from_reader<R: std::io::Read>(
    reader: R,
    file_len: u64,
    entry: &VerifyPackRequest,
    buf_capacity: usize,
) -> VerifyPackResult {
    use std::collections::HashSet;
    use std::io::{BufRead, BufReader};

    let expected_hash = entry.pack_key.split('/').next_back().unwrap_or("");

    // Build set of expected blob (offset, length) pairs to check off during scan.
    let mut remaining: HashSet<(u64, u64)> = entry
        .expected_blobs
        .iter()
        .map(|b| (b.offset, b.length))
        .collect();

    let mut reader = BufReader::with_capacity(buf_capacity, reader);
    let mut hasher = Blake2bVar::new(32).expect("valid output size");

    // Helper: read_exact through BufReader, feeding every byte to the hasher.
    macro_rules! read_exact_hashed {
        ($reader:expr, $hasher:expr, $buf:expr) => {{
            let buf: &mut [u8] = $buf;
            let total = buf.len();
            let mut filled = 0;
            while filled < total {
                let avail = match $reader.fill_buf() {
                    Ok(b) if b.is_empty() => {
                        break; // EOF
                    }
                    Ok(b) => b,
                    Err(e) => {
                        return VerifyPackResult {
                            pack_key: entry.pack_key.clone(),
                            hash_valid: false,
                            header_valid: false,
                            blobs_valid: false,
                            error: Some(format!("read error: {e}")),
                        };
                    }
                };
                let n = std::cmp::min(avail.len(), total - filled);
                buf[filled..filled + n].copy_from_slice(&avail[..n]);
                $hasher.update(&avail[..n]);
                $reader.consume(n);
                filled += n;
            }
            filled
        }};
    }

    // 1. Read header (9 bytes: 8 magic + 1 version).
    let mut header_buf = [0u8; 9];
    let header_read = read_exact_hashed!(reader, hasher, &mut header_buf);

    let header_valid = header_read == 9
        && &header_buf[..8] == PACK_MAGIC
        && (PACK_VERSION_MIN..=PACK_VERSION_MAX).contains(&header_buf[8]);

    // 2. Forward-scan blob boundaries while hashing all remaining bytes.
    let mut blobs_valid = header_valid;
    if header_valid {
        let mut pos = u64::try_from(PACK_HEADER_SIZE).expect("pack header size fits u64");

        loop {
            // Need at least 4 bytes for a length prefix.
            if pos + 4 > file_len {
                if pos != file_len {
                    blobs_valid = false;
                }
                break;
            }

            // Read 4-byte LE length prefix.
            let mut len_buf = [0u8; 4];
            let n = read_exact_hashed!(reader, hasher, &mut len_buf);
            if n != 4 {
                blobs_valid = false;
                break;
            }
            let blob_len = u64::from(u32::from_le_bytes(len_buf));
            pos += 4;

            // Check blob fits within file.
            if pos + blob_len > file_len {
                blobs_valid = false;
                break;
            }

            // Check off this blob against expected set.
            let blob_offset = pos;
            remaining.remove(&(blob_offset, blob_len));

            // Read blob data, feeding to hasher (skip in terms of semantics).
            let mut blob_remaining = blob_len;
            let mut skip_buf = [0u8; 8192];
            while blob_remaining > 0 {
                let to_read = std::cmp::min(
                    usize::try_from(blob_remaining).unwrap_or(usize::MAX),
                    skip_buf.len(),
                );
                let n = read_exact_hashed!(reader, hasher, &mut skip_buf[..to_read]);
                if n == 0 {
                    blobs_valid = false;
                    break;
                }
                blob_remaining -= n as u64;
            }
            if blob_remaining > 0 {
                break; // short read, already set blobs_valid = false
            }

            pos += blob_len;
        }

        // All expected blobs must have been found.
        if !remaining.is_empty() {
            blobs_valid = false;
        }
    }

    // 3. Drain any remaining bytes into the hasher so the hash covers the
    //    full file. This handles: header-invalid (rest unhashed), structural
    //    error (remaining data after early break), and concurrent file growth
    //    (bytes appended after the initial metadata() call).
    let mut extra_bytes: u64 = 0;
    let drain_err: Option<String> = loop {
        let n = {
            let buf = match reader.fill_buf() {
                Ok([]) => break None,
                Ok(b) => b,
                Err(e) => break Some(format!("read error: {e}")),
            };
            hasher.update(buf);
            buf.len()
        };
        reader.consume(n);
        extra_bytes += n as u64;
    };
    // Extra bytes after what the structural scan consumed means the file had
    // unexpected trailing data (or grew concurrently).
    if header_valid && extra_bytes > 0 {
        blobs_valid = false;
    }

    // 4. Finalize hash.
    let actual_hash = finalize_blake2b_256_hex(hasher);
    let hash_valid = actual_hash == expected_hash;

    let error = if let Some(e) = drain_err {
        Some(e)
    } else if !hash_valid {
        Some(format!(
            "hash mismatch: expected {expected_hash}, got {actual_hash}"
        ))
    } else if !header_valid {
        Some("invalid pack header".into())
    } else if !blobs_valid {
        Some("blob boundary mismatch".into())
    } else {
        None
    };

    VerifyPackResult {
        pack_key: entry.pack_key.clone(),
        hash_valid,
        header_valid,
        blobs_valid,
        error,
    }
}

fn finalize_blake2b_256_hex(hasher: Blake2bVar) -> String {
    let mut out = [0u8; 32];
    hasher
        .finalize_variable(&mut out)
        .expect("valid output buffer length");
    hex::encode(out)
}

#[cfg(test)]
mod tests {
    use super::super::test_support::{blake2b_256_hex, build_pack};
    use super::{verify_pack_from_reader, VerifyPackRequest, VerifyPackResult};
    use vykar_protocol::{
        VerifyBlobRef as ProtoVerifyBlobRef, PACK_HEADER_SIZE, PACK_MAGIC, PACK_VERSION_CURRENT,
    };

    /// Build a VerifyPackRequest from pack bytes and optional expected blobs.
    fn verify_request(pack_bytes: &[u8], expected_blobs: Vec<(u64, u64)>) -> VerifyPackRequest {
        let hash = blake2b_256_hex(pack_bytes);
        let shard = &hash[..2];
        VerifyPackRequest {
            pack_key: format!("packs/{shard}/{hash}"),
            expected_size: pack_bytes.len() as u64,
            expected_blobs: expected_blobs
                .into_iter()
                .map(|(offset, length)| ProtoVerifyBlobRef { offset, length })
                .collect(),
        }
    }

    /// Run verify_pack_from_reader with a Cursor and a given buffer size.
    fn verify_via_cursor(
        pack_bytes: &[u8],
        entry: &VerifyPackRequest,
        buf_size: usize,
    ) -> VerifyPackResult {
        let cursor = std::io::Cursor::new(pack_bytes.to_vec());
        verify_pack_from_reader(cursor, pack_bytes.len() as u64, entry, buf_size)
    }

    #[test]
    fn verify_valid_pack() {
        let (pack, refs) = build_pack(&[b"hello", b"world"]);
        let req = verify_request(&pack, refs);
        let result = verify_via_cursor(&pack, &req, 256 * 1024);
        assert!(result.hash_valid, "hash should be valid");
        assert!(result.header_valid, "header should be valid");
        assert!(result.blobs_valid, "blobs should be valid");
        assert!(result.error.is_none(), "no error expected");
    }

    #[test]
    fn verify_corrupt_hash() {
        let (mut pack, refs) = build_pack(&[b"data"]);
        let req = verify_request(&pack, refs);
        // Corrupt a byte after the header.
        pack[PACK_HEADER_SIZE + 5] ^= 0xff;
        let result = verify_via_cursor(&pack, &req, 256 * 1024);
        assert!(
            !result.hash_valid,
            "hash should be invalid after corruption"
        );
        assert!(result.error.as_ref().unwrap().contains("hash mismatch"));
    }

    #[test]
    fn verify_bad_magic() {
        let (mut pack, refs) = build_pack(&[b"data"]);
        // Corrupt magic before computing request (so hash matches the corrupt data).
        pack[0] = b'X';
        let req = verify_request(&pack, refs);
        let result = verify_via_cursor(&pack, &req, 256 * 1024);
        assert!(result.hash_valid, "hash should match the corrupt data");
        assert!(!result.header_valid, "header should be invalid");
        assert!(
            !result.blobs_valid,
            "blobs should be invalid when header is bad"
        );
    }

    #[test]
    fn verify_bad_version() {
        let (mut pack, refs) = build_pack(&[b"data"]);
        // Set version to 0xFF (invalid).
        pack[8] = 0xFF;
        let req = verify_request(&pack, refs);
        let result = verify_via_cursor(&pack, &req, 256 * 1024);
        assert!(result.hash_valid);
        assert!(!result.header_valid, "bad version should invalidate header");
        assert!(!result.blobs_valid);
    }

    #[test]
    fn verify_blob_offset_mismatch() {
        let (pack, refs) = build_pack(&[b"hello", b"world"]);
        // Provide wrong offset for the second blob.
        let wrong_refs = vec![refs[0], (refs[1].0 + 1, refs[1].1)];
        let req = verify_request(&pack, wrong_refs);
        let result = verify_via_cursor(&pack, &req, 256 * 1024);
        assert!(result.hash_valid);
        assert!(result.header_valid);
        assert!(!result.blobs_valid, "wrong offset should fail blob check");
    }

    #[test]
    fn verify_blob_length_mismatch() {
        let (pack, refs) = build_pack(&[b"hello"]);
        let wrong_refs = vec![(refs[0].0, refs[0].1 + 1)];
        let req = verify_request(&pack, wrong_refs);
        let result = verify_via_cursor(&pack, &req, 256 * 1024);
        assert!(result.hash_valid);
        assert!(result.header_valid);
        assert!(!result.blobs_valid, "wrong length should fail blob check");
    }

    #[test]
    fn verify_trailing_bytes() {
        let (mut pack, _refs) = build_pack(&[b"data"]);
        // Append trailing garbage before computing hash.
        pack.extend_from_slice(b"garbage");
        let req = verify_request(&pack, vec![]);
        let result = verify_via_cursor(&pack, &req, 256 * 1024);
        assert!(result.hash_valid);
        assert!(result.header_valid);
        assert!(!result.blobs_valid, "trailing bytes should fail");
    }

    #[test]
    fn verify_empty_expected_blobs_valid_structure() {
        let (pack, _refs) = build_pack(&[b"a", b"b", b"c"]);
        let req = verify_request(&pack, vec![]);
        let result = verify_via_cursor(&pack, &req, 256 * 1024);
        assert!(result.hash_valid);
        assert!(result.header_valid);
        assert!(
            result.blobs_valid,
            "empty expected_blobs with valid structure should pass"
        );
        assert!(result.error.is_none());
    }

    #[test]
    fn verify_empty_expected_blobs_with_trailing_bytes() {
        let (mut pack, _refs) = build_pack(&[b"data"]);
        pack.push(0x42); // single trailing byte
        let req = verify_request(&pack, vec![]);
        let result = verify_via_cursor(&pack, &req, 256 * 1024);
        assert!(result.hash_valid);
        assert!(result.header_valid);
        assert!(
            !result.blobs_valid,
            "empty expected_blobs with trailing bytes should fail"
        );
    }

    #[test]
    fn verify_duplicate_expected_blobs() {
        let (pack, refs) = build_pack(&[b"data"]);
        // Duplicate the same blob ref twice.
        let dup_refs = vec![refs[0], refs[0]];
        let req = verify_request(&pack, dup_refs);
        let result = verify_via_cursor(&pack, &req, 256 * 1024);
        assert!(result.hash_valid);
        assert!(result.header_valid);
        assert!(
            result.blobs_valid,
            "duplicate expected blobs should still pass (subset check)"
        );
    }

    #[test]
    fn verify_small_buffer_forces_splits() {
        // Use buffer size of 13 bytes to force length prefixes to split across reads.
        let (pack, refs) = build_pack(&[b"aaaa", b"bbbbbbbb", b"cc"]);
        let req = verify_request(&pack, refs);
        let result = verify_via_cursor(&pack, &req, 13);
        assert!(result.hash_valid, "hash should be valid with small buffer");
        assert!(
            result.header_valid,
            "header should be valid with small buffer"
        );
        assert!(
            result.blobs_valid,
            "blobs should be valid with small buffer"
        );
        assert!(result.error.is_none());
    }

    #[test]
    fn verify_small_buffer_trailing_bytes() {
        let (mut pack, _refs) = build_pack(&[b"data"]);
        pack.extend_from_slice(b"XX");
        let req = verify_request(&pack, vec![]);
        let result = verify_via_cursor(&pack, &req, 7);
        assert!(result.hash_valid);
        assert!(result.header_valid);
        assert!(
            !result.blobs_valid,
            "trailing bytes detected with small buffer"
        );
    }

    #[test]
    fn verify_header_only_pack() {
        // Pack with just a header, no blobs.
        let mut pack = Vec::new();
        pack.extend_from_slice(PACK_MAGIC);
        pack.push(PACK_VERSION_CURRENT);
        let req = verify_request(&pack, vec![]);
        let result = verify_via_cursor(&pack, &req, 256 * 1024);
        assert!(result.hash_valid);
        assert!(result.header_valid);
        assert!(result.blobs_valid, "header-only pack should be valid");
    }

    #[test]
    fn verify_missing_file_via_single_pack() {
        let path = std::path::Path::new("/nonexistent/pack/file");
        let entry = VerifyPackRequest {
            pack_key: format!("packs/ab/{}", "cc".repeat(32)),
            expected_size: 100,
            expected_blobs: vec![],
        };
        let result = super::verify_single_pack(path, &entry);
        assert!(!result.hash_valid);
        assert!(!result.header_valid);
        assert!(!result.blobs_valid);
        assert!(result.error.is_some());
    }

    #[test]
    fn verify_unsorted_expected_blobs() {
        let (pack, refs) = build_pack(&[b"aaa", b"bbb", b"ccc"]);
        // Provide refs in reverse order.
        let reversed_refs = vec![refs[2], refs[0], refs[1]];
        let req = verify_request(&pack, reversed_refs);
        let result = verify_via_cursor(&pack, &req, 256 * 1024);
        assert!(result.hash_valid);
        assert!(result.header_valid);
        assert!(
            result.blobs_valid,
            "unsorted expected blobs should still pass"
        );
    }

    #[test]
    fn verify_single_byte_buffer() {
        // Extreme: buffer size of 1 byte.
        let (pack, refs) = build_pack(&[b"test"]);
        let req = verify_request(&pack, refs);
        let result = verify_via_cursor(&pack, &req, 1);
        assert!(result.hash_valid);
        assert!(result.header_valid);
        assert!(result.blobs_valid);
    }
}
