use std::io::Read;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Condvar, Mutex};
use std::time::Duration;

use vykar_types::error::{Result, VykarError};

use crate::retry::HttpRetryError;

/// Build the `Range` header value for a `length`-byte read at `offset`.
///
/// `label` names the backend ("REST"/"S3") and only appears in the error.
///
/// # Errors
///
/// Returns an error when `offset + length` overflows `u64`.
pub fn range_header(label: &str, key: &str, offset: u64, length: u64) -> Result<String> {
    let end = offset
        .checked_add(length)
        .and_then(|n| n.checked_sub(1))
        .ok_or_else(|| {
            VykarError::Other(format!(
                "{label} GET_RANGE {key}: offset {offset} + length {length} overflows u64"
            ))
        })?;
    Ok(format!("bytes={offset}-{end}"))
}

/// Read the body of a range response, requiring exactly `length` bytes.
///
/// A short read is an error, not a silent truncation — it surfaces as
/// `BodyIo(UnexpectedEof)`, which the retry layer treats as transient, so a
/// connection dropped mid-body is retried rather than returning partial data.
///
/// # Errors
///
/// Returns `Permanent` when `length` exceeds the platform's `usize`, and
/// `BodyIo` on a read failure or a short body.
pub fn read_range_body(
    resp: &mut http::Response<ureq::Body>,
    label: &str,
    key: &str,
    offset: u64,
    length: u64,
) -> std::result::Result<Vec<u8>, HttpRetryError> {
    let cap = usize::try_from(length).map_err(|_| {
        HttpRetryError::Permanent(format!(
            "{label} GET_RANGE {key}: length {length} exceeds platform usize"
        ))
    })?;
    let mut buf = Vec::with_capacity(cap);
    resp.body_mut()
        .as_reader()
        .take(length)
        .read_to_end(&mut buf)
        .map_err(HttpRetryError::BodyIo)?;
    if buf.len() != cap {
        return Err(HttpRetryError::BodyIo(std::io::Error::new(
            std::io::ErrorKind::UnexpectedEof,
            format!(
                "short read on {key} at offset {offset}: expected {length} bytes, got {}",
                buf.len()
            ),
        )));
    }
    Ok(buf)
}

/// Extract and parse the `Content-Length` header from HTTP response headers.
pub fn extract_content_length(headers: &http::HeaderMap, context: &str) -> Result<u64> {
    let header = headers.get(http::header::CONTENT_LENGTH).ok_or_else(|| {
        VykarError::Other(format!("{context}: response missing Content-Length header"))
    })?;
    let val = header
        .to_str()
        .map_err(|_| VykarError::Other(format!("{context}: non-ASCII Content-Length header")))?;
    val.parse::<u64>()
        .map_err(|_| VykarError::Other(format!("{context}: invalid Content-Length header: {val}")))
}

/// Counting semaphore bounding the in-flight part uploads across every
/// multipart (S3) or part (REST) upload of one backend.
pub(crate) struct PartSlots {
    free: Mutex<usize>,
    cv: Condvar,
    capacity: usize,
}

impl PartSlots {
    pub(crate) fn new(capacity: usize) -> Self {
        let capacity = capacity.max(1);
        Self {
            free: Mutex::new(capacity),
            cv: Condvar::new(),
            capacity,
        }
    }

    pub(crate) fn capacity(&self) -> usize {
        self.capacity
    }

    pub(crate) fn acquire(&self) -> PartSlot<'_> {
        let mut free = self.free.lock().unwrap_or_else(|e| e.into_inner());
        while *free == 0 {
            free = self.cv.wait(free).unwrap_or_else(|e| e.into_inner());
        }
        *free -= 1;
        PartSlot(self)
    }
}

/// RAII permit from [`PartSlots::acquire`]; released on drop (including unwind).
pub(crate) struct PartSlot<'a>(&'a PartSlots);

impl Drop for PartSlot<'_> {
    fn drop(&mut self) {
        let mut free = self.0.free.lock().unwrap_or_else(|e| e.into_inner());
        *free += 1;
        drop(free);
        self.0.cv.notify_one();
    }
}

/// Run `send(i, prepare(i))` for every part index `0..count`, at most
/// `slots` sends at a time across the backend, on up to
/// `slots.capacity().min(count)` scoped threads (the caller is one of them).
///
/// Indices are claimed in ascending order. `prepare` runs after the claim and
/// before a slot is taken, so a slot (and its pooled connection) never idles
/// on local work such as hashing. The first failure stops new parts from
/// starting; parts already sending finish. Every worker is joined before this
/// returns, so the caller can complete or abort the upload knowing no part is
/// still in flight. Of several failures the lowest index wins, matching what
/// a sequential upload would have reported.
pub(crate) fn run_parts_prepared<P, E, Prep, Run>(
    slots: &PartSlots,
    count: usize,
    prepare: Prep,
    send: Run,
) -> std::result::Result<(), E>
where
    Prep: Fn(usize) -> P + Sync,
    Run: Fn(usize, P) -> std::result::Result<(), E> + Sync,
    E: Send,
{
    let next = AtomicUsize::new(0);
    let failed = AtomicBool::new(false);
    let failure: Mutex<Option<(usize, E)>> = Mutex::new(None);

    let worker = || loop {
        let i = next.fetch_add(1, Ordering::Relaxed);
        if i >= count {
            return;
        }
        let prepared = prepare(i);
        if failed.load(Ordering::Acquire) {
            return;
        }
        let _slot = slots.acquire();
        if failed.load(Ordering::Acquire) {
            return;
        }
        if let Err(e) = send(i, prepared) {
            failed.store(true, Ordering::Release);
            let mut first = failure.lock().unwrap_or_else(|e| e.into_inner());
            if first.as_ref().is_none_or(|(j, _)| i < *j) {
                *first = Some((i, e));
            }
            return;
        }
    };

    let workers = slots.capacity().min(count);
    std::thread::scope(|s| {
        for _ in 1..workers {
            s.spawn(worker);
        }
        worker();
    });
    match failure.into_inner().unwrap_or_else(|e| e.into_inner()) {
        Some((_, e)) => Err(e),
        None => Ok(()),
    }
}

/// [`run_parts_prepared`] with nothing to prepare outside the slot.
pub(crate) fn run_parts<E, Run>(
    slots: &PartSlots,
    count: usize,
    send: Run,
) -> std::result::Result<(), E>
where
    Run: Fn(usize) -> std::result::Result<(), E> + Sync,
    E: Send,
{
    run_parts_prepared(slots, count, |_| (), |i, ()| send(i))
}

/// Upper bound on a backend-wide part concurrency (also caps the env override).
const MAX_PART_CONCURRENCY: usize = 64;

/// A `usize` from environment variable `name`; `None` when unset or unparsable.
pub(crate) fn env_usize(name: &str) -> Option<usize> {
    std::env::var(name).ok()?.trim().parse().ok()
}

/// Backend-wide number of concurrent part PUTs for the S3 multipart and REST
/// part uploads: 4 per configured connection (default 2), clamped to 4..=32.
/// `env_name` is a hidden benchmarking override, clamped to 1..=64.
///
/// One stream at 40 ms RTT is capped near 100 MiB/s by its send window, and
/// every part pays ~1 RTT of fixed turnaround, so throughput scales with the
/// number of concurrent streams rather than with the number of packs in
/// flight. A writer runs up to [`crate::BackendProfile::transfer_width`] +
/// [`crate::UPLOAD_HEADROOM`] pack uploads at once; parts of each pack
/// upload in parallel, bounded across all packs by this many slots.
pub(crate) fn part_concurrency(env_name: &str, max_connections: Option<usize>) -> usize {
    if let Some(n) = env_usize(env_name) {
        return n.clamp(1, MAX_PART_CONCURRENCY);
    }
    (max_connections.unwrap_or(2) * 4).clamp(4, 32)
}

/// Phase timeouts for the REST and S3 agents; S3 uses `Default`. `Default`
/// uses the crate constants; tests shrink them to exercise stalled servers.
#[derive(Debug, Clone, Copy)]
pub(crate) struct HttpTimeouts {
    pub resolve: Duration,
    pub send_request: Duration,
    /// Response-header wait for data operations (agent default).
    pub recv_response: Duration,
    /// Response-header wait for admin actions that run server-side work.
    pub admin: Duration,
    /// Response-header wait for server-side repack.
    pub repack: Duration,
}

impl Default for HttpTimeouts {
    fn default() -> Self {
        Self {
            resolve: crate::HTTP_RESOLVE_TIMEOUT,
            send_request: crate::HTTP_SEND_REQUEST_TIMEOUT,
            recv_response: crate::HTTP_RECV_RESPONSE_TIMEOUT,
            admin: crate::HTTP_ADMIN_RECV_RESPONSE_TIMEOUT,
            repack: crate::HTTP_REPACK_RECV_RESPONSE_TIMEOUT,
        }
    }
}

/// The ureq agent shared by the REST and S3 backends: statuses are returned,
/// not raised; every phase has a finite timeout (connect 30 s, request/response
/// bodies 5 min each, the rest from `timeouts`); `pool` idle connections are
/// kept per host and in total for `HTTP_IDLE_AGE`; TLS verifies against the
/// OS trust store.
pub(crate) fn build_agent(timeouts: &HttpTimeouts, pool: usize) -> ureq::Agent {
    ureq::Agent::config_builder()
        .http_status_as_error(false)
        .timeout_resolve(Some(timeouts.resolve))
        .timeout_connect(Some(Duration::from_secs(30)))
        .timeout_send_request(Some(timeouts.send_request))
        .timeout_recv_response(Some(timeouts.recv_response))
        .timeout_send_body(Some(Duration::from_secs(5 * 60)))
        .timeout_recv_body(Some(Duration::from_secs(5 * 60)))
        .max_idle_connections_per_host(pool)
        .max_idle_connections(pool)
        .max_idle_age(crate::HTTP_IDLE_AGE)
        .tls_config(crate::http_tls_config())
        .build()
        .into()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn headers_with(name: &str, value: &str) -> http::HeaderMap {
        let mut map = http::HeaderMap::new();
        map.insert(
            http::header::HeaderName::from_bytes(name.as_bytes()).unwrap(),
            http::header::HeaderValue::from_str(value).unwrap(),
        );
        map
    }

    #[test]
    fn range_header_formats_inclusive_end() {
        assert_eq!(range_header("REST", "k", 0, 100).unwrap(), "bytes=0-99");
        assert_eq!(range_header("S3", "k", 10, 1).unwrap(), "bytes=10-10");
    }

    #[test]
    fn range_header_rejects_overflow() {
        let err = range_header("REST", "k", u64::MAX, 2)
            .unwrap_err()
            .to_string();
        assert!(err.contains("overflows u64"), "got: {err}");
    }

    #[test]
    fn valid_content_length() {
        let headers = headers_with("content-length", "42");
        assert_eq!(extract_content_length(&headers, "test").unwrap(), 42);
    }

    #[test]
    fn zero_content_length() {
        let headers = headers_with("content-length", "0");
        assert_eq!(extract_content_length(&headers, "test").unwrap(), 0);
    }

    #[test]
    fn missing_content_length() {
        let headers = http::HeaderMap::new();
        let err = extract_content_length(&headers, "test")
            .unwrap_err()
            .to_string();
        assert!(err.contains("missing Content-Length"), "got: {err}");
    }

    #[test]
    fn non_numeric_content_length() {
        let headers = headers_with("content-length", "garbage");
        let err = extract_content_length(&headers, "test")
            .unwrap_err()
            .to_string();
        assert!(err.contains("invalid Content-Length"), "got: {err}");
    }

    #[test]
    fn negative_content_length() {
        let headers = headers_with("content-length", "-1");
        let err = extract_content_length(&headers, "test")
            .unwrap_err()
            .to_string();
        assert!(err.contains("invalid Content-Length"), "got: {err}");
    }

    #[test]
    fn part_concurrency_follows_connections() {
        let env = "VYKAR_TEST_UNSET_PART_CONCURRENCY";
        assert_eq!(part_concurrency(env, None), 8);
        assert_eq!(part_concurrency(env, Some(1)), 4);
        assert_eq!(part_concurrency(env, Some(4)), 16);
        assert_eq!(part_concurrency(env, Some(64)), 32);
    }

    #[test]
    fn build_agent_config_matches_backends() {
        let agent = build_agent(&HttpTimeouts::default(), 7);
        let config = agent.config();
        let t = config.timeouts();
        assert_eq!(t.resolve, Some(crate::HTTP_RESOLVE_TIMEOUT));
        assert_eq!(t.connect, Some(Duration::from_secs(30)));
        assert_eq!(t.send_request, Some(crate::HTTP_SEND_REQUEST_TIMEOUT));
        assert_eq!(t.recv_response, Some(crate::HTTP_RECV_RESPONSE_TIMEOUT));
        assert_eq!(t.send_body, Some(Duration::from_secs(300)));
        assert_eq!(t.recv_body, Some(Duration::from_secs(300)));
        assert_eq!(config.max_idle_connections(), 7);
        assert_eq!(config.max_idle_connections_per_host(), 7);
        assert_eq!(config.max_idle_age(), crate::HTTP_IDLE_AGE);
        assert!(!config.http_status_as_error());
        assert!(matches!(
            config.tls_config().root_certs(),
            ureq::tls::RootCerts::PlatformVerifier
        ));
    }

    #[test]
    fn run_parts_runs_every_index_once() {
        let slots = PartSlots::new(4);
        let seen = Mutex::new(Vec::new());
        let result = run_parts(&slots, 37, |i| {
            seen.lock().unwrap().push(i);
            Ok::<_, ()>(())
        });
        assert_eq!(result, Ok(()));
        let mut seen = seen.into_inner().unwrap();
        seen.sort_unstable();
        assert_eq!(seen, (0..37).collect::<Vec<_>>());

        let slots = PartSlots::new(1);
        let result = run_parts(&slots, 0, |_| -> std::result::Result<(), ()> {
            panic!("no part to send")
        });
        assert_eq!(result, Ok(()));
    }

    #[test]
    fn run_parts_lowest_index_failure_wins() {
        let slots = PartSlots::new(8);
        let barrier = std::sync::Barrier::new(8);
        let result = run_parts(&slots, 8, |i| {
            barrier.wait();
            match i {
                5 => Err(5),
                2 => {
                    std::thread::sleep(Duration::from_millis(50));
                    Err(2)
                }
                _ => Ok(()),
            }
        });
        assert_eq!(result, Err(2));
    }

    #[test]
    fn run_parts_stops_claiming_after_failure() {
        let slots = PartSlots::new(1);
        let prepared = Mutex::new(Vec::new());
        let sent = Mutex::new(Vec::new());
        let result = run_parts_prepared(
            &slots,
            10,
            |i| prepared.lock().unwrap().push(i),
            |i, ()| {
                sent.lock().unwrap().push(i);
                if i == 3 {
                    Err(i)
                } else {
                    Ok(())
                }
            },
        );
        assert_eq!(result, Err(3));
        assert_eq!(prepared.into_inner().unwrap(), [0, 1, 2, 3]);
        assert_eq!(sent.into_inner().unwrap(), [0, 1, 2, 3]);
    }

    #[test]
    fn run_parts_prepare_runs_without_a_slot() {
        let slots = std::sync::Arc::new(PartSlots::new(1));
        let held = slots.acquire();
        let (tx, rx) = std::sync::mpsc::channel();
        let worker = {
            let slots = std::sync::Arc::clone(&slots);
            std::thread::spawn(move || {
                let tx = Mutex::new(tx);
                run_parts_prepared(
                    &slots,
                    1,
                    |i| tx.lock().unwrap().send(i).unwrap(),
                    |_, ()| Ok::<_, ()>(()),
                )
            })
        };
        let prepared = rx.recv_timeout(Duration::from_secs(5));
        drop(held);
        let joined = worker.join();
        assert_eq!(prepared, Ok(0));
        assert_eq!(joined.unwrap(), Ok(()));
    }

    #[test]
    fn run_parts_bounds_in_flight_sends() {
        let slots = PartSlots::new(3);
        let in_flight = AtomicUsize::new(0);
        let max = AtomicUsize::new(0);
        let result = run_parts(&slots, 30, |_| {
            let now = in_flight.fetch_add(1, Ordering::SeqCst) + 1;
            max.fetch_max(now, Ordering::SeqCst);
            std::thread::sleep(Duration::from_millis(2));
            in_flight.fetch_sub(1, Ordering::SeqCst);
            Ok::<_, ()>(())
        });
        assert_eq!(result, Ok(()));
        let max = max.load(Ordering::SeqCst);
        assert!((1..=3).contains(&max), "max in flight {max}");
    }
}
