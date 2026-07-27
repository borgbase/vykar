//! TOCTOU-safe file reads shared by the sequential and pipeline backup paths.
//!
//! Every backup read of a regular file follows the same invariant:
//!
//! 1. open the file and `fstat` the **descriptor** (never the path again),
//! 2. compare that against the walk-time metadata — anything that changed
//!    between walk and open invalidates the plan built from it,
//! 3. read a bounded range, feeding the bytes downstream,
//! 4. `fstat` the descriptor again and require the metadata to be unchanged
//!    *and* the byte count to match what was planned.
//!
//! Both drift checks produce [`VykarError::FileChangedDuringRead`], which
//! `is_soft_file_error()` classifies as skippable: callers turn it into a
//! per-file warning instead of aborting the backup.

use std::io::Read;
use std::path::Path;

use crate::chunker;
use crate::config::ChunkerConfig;
use crate::limits::{self, ByteRateLimiter};
use crate::platform::fs;
use vykar_types::error::{Result, VykarError};

use super::read_source::BackupSource;

/// What to read from an already-opened, drift-checked source.
#[derive(Debug, Clone, Copy)]
pub(super) enum ReadPlan {
    /// The whole file as a single buffer — the sub-min-chunk fast path.
    /// Validates that exactly `pre_meta.size` bytes were read.
    Whole,
    /// The whole file, split by FastCDC. Validates that the chunks cover
    /// exactly `pre_meta.size` bytes.
    Chunked,
    /// The byte range `[offset, offset + len)`, split by FastCDC. Validates
    /// that the chunks cover exactly `len` bytes: the segment plan was
    /// derived from the walk-time size, so a short read means the file no
    /// longer matches that plan even if its metadata still does.
    Segment { offset: u64, len: u64 },
}

/// Open `path` and verify it has not drifted since the walk.
///
/// Returns the open source plus the pre-read `fstat`, which is the canonical
/// metadata for everything that follows: the committed `Item`, the file-cache
/// entry, and the post-read comparison.
///
/// Kept separate from [`read_range_drift_checked`] so callers can run it
/// *before* arming any rollback state — a failure here means nothing was
/// committed and nothing needs unwinding.
pub(super) fn open_checked(
    path: &Path,
    walk_meta: &fs::MetadataSummary,
) -> Result<(BackupSource, fs::MetadataSummary)> {
    let source = BackupSource::open(path).map_err(VykarError::Io)?;
    let pre_meta = fs::fstat_summary(source.file()).map_err(VykarError::Io)?;
    // Walk-to-open drift check — catches pre-open mutation and rename-atop
    // (device+inode differ).
    if !fs::metadata_matches(&pre_meta, walk_meta) {
        return Err(VykarError::FileChangedDuringRead {
            path: path.to_string_lossy().into_owned(),
            dataless: pre_meta.is_dataless,
        });
    }
    Ok((source, pre_meta))
}

/// Read `plan` from an already drift-checked `source`, handing the data to
/// `on_chunk`, then verify the file did not change during the read.
///
/// `path` is only used for error messages. `pre_meta` must be the `fstat`
/// returned by [`open_checked`].
///
/// Streaming plans call `on_chunk` as chunks are produced, so a drift
/// detected afterwards can leave partial work behind — those callers are
/// responsible for rolling it back (see `with_rollback_checkpoint`).
/// [`ReadPlan::Whole`] instead buffers and only hands the data over *after*
/// the post-read check passes, so a drift leaves the caller untouched.
pub(super) fn read_range_drift_checked(
    source: &mut BackupSource,
    path: &str,
    pre_meta: &fs::MetadataSummary,
    plan: ReadPlan,
    chunker_config: &ChunkerConfig,
    limiter: Option<&ByteRateLimiter>,
    mut on_chunk: impl FnMut(Vec<u8>) -> Result<()>,
) -> Result<()> {
    // For whole-file plans the read is hard-capped at `size + 1`: an
    // intra-read append then trips the exact-byte-count check below instead
    // of feeding unbounded data downstream. Segments read exactly `len` —
    // one extra byte would spill into the next segment's range.
    let (read_limit, expected_bytes) = match plan {
        ReadPlan::Whole | ReadPlan::Chunked => (pre_meta.size + 1, pre_meta.size),
        ReadPlan::Segment { offset, len } => {
            source.seek_from_start(offset).map_err(VykarError::Io)?;
            (len, len)
        }
    };

    let mut total_bytes: u64 = 0;
    // Held back until the post-read check passes (see the doc comment).
    let mut whole_file: Option<Vec<u8>> = None;

    match plan {
        ReadPlan::Whole => {
            // On 32-bit hosts a `u64 -> usize` cast would silently truncate a
            // multi-GiB file's pre-allocation; refuse upfront.
            let cap = usize::try_from(pre_meta.size).map_err(|_| {
                VykarError::Other(format!(
                    "file {path} too large for this platform: {} bytes",
                    pre_meta.size,
                ))
            })?;
            let mut data = Vec::with_capacity(cap);
            let reader = Read::take(&mut *source, read_limit);
            limits::LimitedReader::new(reader, limiter)
                .read_to_end(&mut data)
                .map_err(VykarError::Io)?;
            total_bytes = data.len() as u64;
            whole_file = Some(data);
        }
        ReadPlan::Chunked | ReadPlan::Segment { .. } => {
            let reader = Read::take(&mut *source, read_limit);
            for chunk_result in
                chunker::chunk_stream(limits::LimitedReader::new(reader, limiter), chunker_config)
            {
                let chunk = chunk_result.map_err(|e| match e {
                    fastcdc::v2020::Error::IoError(ioe) => VykarError::Io(ioe),
                    other => VykarError::Other(format!("chunking failed for {path}: {other}")),
                })?;
                total_bytes = total_bytes.saturating_add(chunk.data.len() as u64);
                on_chunk(chunk.data)?;
            }
        }
    }

    // Intra-read + short-read drift check.
    let post_meta = fs::fstat_summary(source.file()).map_err(VykarError::Io)?;
    if !fs::metadata_matches(pre_meta, &post_meta) || total_bytes != expected_bytes {
        return Err(VykarError::FileChangedDuringRead {
            path: path.to_string(),
            dataless: post_meta.is_dataless,
        });
    }

    if let Some(data) = whole_file {
        on_chunk(data)?;
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    /// Write `bytes` to a temp file and return it plus the `fstat` a walk
    /// would have produced for it.
    fn source_file(bytes: &[u8]) -> (tempfile::TempDir, std::path::PathBuf, fs::MetadataSummary) {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("data.bin");
        let mut f = std::fs::File::create(&path).unwrap();
        f.write_all(bytes).unwrap();
        f.sync_all().unwrap();
        let meta = fs::fstat_summary(&f).unwrap();
        (dir, path, meta)
    }

    fn read_segment(
        path: &std::path::Path,
        meta: &fs::MetadataSummary,
        offset: u64,
        len: u64,
    ) -> Result<Vec<u8>> {
        let (mut source, pre_meta) = open_checked(path, meta)?;
        let mut got = Vec::new();
        read_range_drift_checked(
            &mut source,
            &path.to_string_lossy(),
            &pre_meta,
            ReadPlan::Segment { offset, len },
            &ChunkerConfig::default(),
            None,
            |data| {
                got.extend_from_slice(&data);
                Ok(())
            },
        )?;
        Ok(got)
    }

    #[test]
    fn segment_read_covers_exactly_its_range() {
        let bytes: Vec<u8> = (0..200u16).map(|i| i as u8).collect();
        let (_dir, path, meta) = source_file(&bytes);

        let got = read_segment(&path, &meta, 50, 100).unwrap();
        assert_eq!(got, bytes[50..150]);
    }

    /// A segment that cannot deliver its full range is drift: the plan was
    /// derived from the walk-time size, so a short read means the file no
    /// longer matches it even when the metadata check still passes.
    #[test]
    fn segment_short_read_is_reported_as_drift() {
        let bytes = vec![0xABu8; 100];
        let (_dir, path, meta) = source_file(&bytes);

        // Ask for 100 bytes starting at 50 — only 50 are available.
        let err = read_segment(&path, &meta, 50, 100).unwrap_err();
        assert!(
            matches!(err, VykarError::FileChangedDuringRead { .. }),
            "expected a drift error, got: {err}"
        );
    }
}
