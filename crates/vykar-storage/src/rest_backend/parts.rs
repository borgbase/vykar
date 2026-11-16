//! Pack uploads in parts over several connections (vykar-server `pack_parts`).
//!
//! One TCP stream over a long-RTT link is capped by its window, and a pack is
//! otherwise one PUT on one stream, so the stream count would equal the number
//! of packs in flight. Here a pack is sent as slices of the buffer already in
//! memory — no copy — from scoped worker threads, bounded across every pack
//! of the backend by a shared [`PartSlots`] budget, and finished with one
//! completion request that the server answers only once the pack is durable.
//! Wire format: `crates/vykar-server/src/handlers/parts.rs`.

use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Mutex;

use super::RestBackend;
use crate::http_util::PartSlots;
use crate::retry::HttpRetryError;

/// Hidden benchmarking knobs, not a supported configuration surface.
const PART_CONCURRENCY_ENV: &str = "VYKAR_REST_PART_CONCURRENCY";
const PART_SIZE_ENV: &str = "VYKAR_REST_PART_SIZE_MIB";
/// Upper bound on the backend-wide part concurrency (also caps the override).
const MAX_PART_CONCURRENCY: usize = 64;

/// Target part size. Packs at or below it stay one whole-object PUT; larger
/// ones are cut into equal parts no larger than this.
pub(super) const DEFAULT_PART_SIZE: usize = 16 * 1024 * 1024;
const MIN_PART_SIZE: usize = 1024 * 1024;

/// Backend-wide number of concurrent part PUTs: 4 per configured connection
/// (default 2), clamped to 4..=32, like the S3 multipart path.
pub(super) fn part_concurrency(max_connections: Option<usize>) -> usize {
    if let Some(n) = env_usize(PART_CONCURRENCY_ENV) {
        return n.clamp(1, MAX_PART_CONCURRENCY);
    }
    (max_connections.unwrap_or(2) * 4).clamp(4, 32)
}

pub(super) fn part_size() -> usize {
    env_usize(PART_SIZE_ENV).map_or(DEFAULT_PART_SIZE, |mib| {
        mib.saturating_mul(1024 * 1024).max(MIN_PART_SIZE)
    })
}

fn env_usize(name: &str) -> Option<usize> {
    std::env::var(name).ok()?.trim().parse().ok()
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
    /// parts, then complete it.
    ///
    /// Workers claim part indices in ascending order; the first failure stops
    /// new parts from starting, and every worker is joined before the
    /// completion is sent, so the server never sees a completion racing a part
    /// of the same upload. The caller falls back to a whole PUT on `Err`.
    pub(super) fn put_pack_in_parts(
        &self,
        key: &str,
        pack_hex: &str,
        digest_header: &str,
        data: &[u8],
    ) -> std::result::Result<(), PartsFailure> {
        let slots: &PartSlots = &self.part_slots;
        let token = new_token();
        let ranges = part_ranges(data.len(), self.part_size);
        let base = self.url(key);
        let total = data.len();

        let next = AtomicUsize::new(0);
        let failed = AtomicBool::new(false);
        let failure: Mutex<Option<(usize, PartsFailure)>> = Mutex::new(None);

        let worker = || loop {
            let i = next.fetch_add(1, Ordering::Relaxed);
            let Some(range) = ranges.get(i) else {
                return;
            };
            if failed.load(Ordering::Acquire) {
                return;
            }
            let _slot = slots.acquire();
            if failed.load(Ordering::Acquire) {
                return;
            }
            let chunk = data.get(range.clone()).expect("ranges lie within data");
            let url = format!("{base}?upload={token}&offset={}&total={total}", range.start);
            let result = self.retry_call(
                &format!("PUT {key} part {}", i + 1),
                || self.authed(self.agent.put(&url)).send(chunk),
                |resp| {
                    let status = resp.status().as_u16();
                    crate::retry::classify_status(
                        status,
                        format!("REST PUT {key} part {}: HTTP {status}", i + 1),
                    )
                },
            );
            if let Err(e) = result {
                failed.store(true, Ordering::Release);
                let mut first = failure.lock().unwrap_or_else(|e| e.into_inner());
                if first.as_ref().is_none_or(|(j, _)| i < *j) {
                    *first = Some((i, PartsFailure::from_http(&format!("part {}", i + 1), &e)));
                }
                return;
            }
        };

        let workers = slots.capacity().min(ranges.len());
        std::thread::scope(|s| {
            for _ in 1..workers {
                s.spawn(worker);
            }
            worker();
        });
        if let Some((_, f)) = failure.into_inner().unwrap_or_else(|e| e.into_inner()) {
            return Err(f);
        }

        // Retrying is safe: once committed, the server answers a repeated
        // completion from the stored object's digest.
        let url = format!("{base}?complete&upload={token}");
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
        .map_err(|e| PartsFailure::from_http("complete", &e))
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
    fn concurrency_follows_connections() {
        if std::env::var_os(PART_CONCURRENCY_ENV).is_some() {
            return;
        }
        assert_eq!(part_concurrency(None), 8);
        assert_eq!(part_concurrency(Some(1)), 4);
        assert_eq!(part_concurrency(Some(4)), 16);
        assert_eq!(part_concurrency(Some(64)), 32);
    }

    #[test]
    fn tokens_are_valid_and_distinct() {
        let a = new_token();
        assert!(vykar_protocol::is_valid_upload_token(&a), "{a}");
        assert_ne!(a, new_token());
    }
}
