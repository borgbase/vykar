//! Pack uploads in parts over several connections (vykar-server `pack_parts`).
//!
//! One TCP stream over a long-RTT link is capped by its window, and a pack is
//! otherwise one PUT on one stream, so the stream count would equal the number
//! of packs in flight. Here a pack is sent as slices of the buffer already in
//! memory — no copy — from scoped worker threads, bounded across every pack
//! of the backend by a shared [`PartSlots`](crate::http_util::PartSlots)
//! budget, and finished with one completion request that the server answers
//! only once the pack is durable.
//! Wire format: `crates/vykar-server/src/handlers/parts.rs`.

use std::sync::atomic::Ordering;

use super::RestBackend;
use crate::retry::HttpRetryError;

/// Hidden benchmarking knobs, not a supported configuration surface.
pub(super) const PART_CONCURRENCY_ENV: &str = "VYKAR_REST_PART_CONCURRENCY";
const PART_SIZE_ENV: &str = "VYKAR_REST_PART_SIZE_MIB";

/// Target part size. Packs at or below it stay one whole-object PUT; larger
/// ones are cut into equal parts no larger than this.
pub(super) const DEFAULT_PART_SIZE: usize = 16 * 1024 * 1024;
const MIN_PART_SIZE: usize = 1024 * 1024;

pub(super) fn part_size() -> usize {
    crate::http_util::env_usize(PART_SIZE_ENV).map_or(DEFAULT_PART_SIZE, |mib| {
        mib.saturating_mul(1024 * 1024).max(MIN_PART_SIZE)
    })
}

/// Byte ranges of `len` split into the fewest equal parts of at most
/// `part_size` bytes (the last one may be shorter by the rounding).
pub(super) fn part_ranges(len: usize, part_size: usize) -> Vec<std::ops::Range<usize>> {
    let part_size = part_size.max(1);
    let count = len.div_ceil(part_size).max(1);
    let each = len.div_ceil(count);
    (0..count)
        .map(|i| (i * each).min(len)..((i + 1) * each).min(len))
        .filter(|r| !r.is_empty())
        .collect()
}

fn new_token() -> String {
    hex::encode(rand::random::<[u8; 16]>())
}

/// Why a part upload did not finish. `permanent` is a 4xx-class refusal that
/// a later pack would meet again (a proxy rejecting the query, a server that
/// lost the capability); transient failures leave the path enabled.
pub(super) struct PartsFailure {
    pub permanent: bool,
    pub message: String,
}

impl PartsFailure {
    fn from_http(what: &str, err: &HttpRetryError) -> Self {
        Self {
            permanent: matches!(err, HttpRetryError::Permanent(_)),
            message: format!("{what}: {err}"),
        }
    }
}

impl RestBackend {
    /// Upload pack `data` (stored at `key`, content digest `pack_hex`) in
    /// parts, then complete it. The caller falls back to a whole PUT on `Err`.
    pub(super) fn put_pack_in_parts(
        &self,
        key: &str,
        pack_hex: &str,
        digest_header: &str,
        data: &[u8],
    ) -> std::result::Result<(), PartsFailure> {
        let token = self.send_pack_parts(key, data)?;
        self.complete_pack_parts(key, pack_hex, digest_header, &token)
    }

    /// Send every part of pack `data`; the upload token on success.
    ///
    /// Workers claim part indices in ascending order; the first failure stops
    /// new parts from starting, and every worker is joined before this
    /// returns, so the server never sees a completion racing a part of the
    /// same upload.
    pub(super) fn send_pack_parts(
        &self,
        key: &str,
        data: &[u8],
    ) -> std::result::Result<String, PartsFailure> {
        let token = new_token();
        let ranges = part_ranges(data.len(), self.part_size);
        let base = self.url(key);
        let total = data.len();
        crate::http_util::run_parts(&self.part_slots, ranges.len(), |i| {
            let range = ranges.get(i).expect("index below ranges.len()");
            let chunk = data.get(range.clone()).expect("ranges lie within data");
            let url = format!("{base}?upload={token}&offset={}&total={total}", range.start);
            self.retry_call(
                &format!("PUT {key} part {}", i + 1),
                || self.authed(self.agent.put(&url)).send(chunk),
                |resp| {
                    let status = resp.status().as_u16();
                    crate::retry::classify_status(
                        status,
                        format!("REST PUT {key} part {}: HTTP {status}", i + 1),
                    )
                },
            )
            .map_err(|e| PartsFailure::from_http(&format!("part {}", i + 1), &e))
        })?;
        Ok(token)
    }

    /// Complete the part upload `token` of pack `key`. Needs no pack bytes,
    /// so `put_pack` releases its buffer before calling this.
    ///
    /// Retrying is safe: once committed, the server answers a repeated
    /// completion from the stored object's digest.
    pub(super) fn complete_pack_parts(
        &self,
        key: &str,
        pack_hex: &str,
        digest_header: &str,
        token: &str,
    ) -> std::result::Result<(), PartsFailure> {
        let url = format!("{}?complete&upload={token}", self.url(key));
        self.retry_call(
            &format!("COMPLETE {key}"),
            || {
                self.authed(self.agent.post(&url))
                    .header(digest_header, pack_hex)
                    .send(&[] as &[u8])
            },
            |resp| {
                let status = resp.status().as_u16();
                crate::retry::classify_status(status, format!("REST COMPLETE {key}: HTTP {status}"))
            },
        )
        .map_err(|e| PartsFailure::from_http("complete", &e))?;
        self.parts_proven.store(true, Ordering::Relaxed);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ranges_cover_the_buffer_in_equal_parts() {
        let mib = 1024 * 1024;
        for (len, size, parts) in [
            (40 * mib, 16 * mib, 3),
            (32 * mib, 16 * mib, 2),
            (32 * mib + 1, 16 * mib, 3),
            (5, 16 * mib, 1),
            (7, 2, 4),
        ] {
            let ranges = part_ranges(len, size);
            assert_eq!(ranges.len(), parts, "len {len} size {size}");
            assert_eq!(ranges.first().unwrap().start, 0);
            assert_eq!(ranges.last().unwrap().end, len);
            for pair in ranges.windows(2) {
                assert_eq!(pair[0].end, pair[1].start, "contiguous");
            }
            assert!(ranges.iter().all(|r| r.len() <= size));
            let max = ranges.iter().map(|r| r.len()).max().unwrap();
            let min = ranges.iter().map(|r| r.len()).min().unwrap();
            assert!(max - min <= parts, "near-equal parts: {ranges:?}");
        }
    }

    #[test]
    fn tokens_are_valid_and_distinct() {
        let a = new_token();
        assert!(vykar_protocol::is_valid_upload_token(&a), "{a}");
        assert_ne!(a, new_token());
    }
}
