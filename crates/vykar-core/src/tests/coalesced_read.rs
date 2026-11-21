use std::collections::HashSet;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use vykar_storage::{BackendProfile, InnerBackend, StorageBackend, delegate_storage_backend};
use vykar_types::chunk_id::ChunkId;
use vykar_types::error::{Result, VykarError};

use crate::compress::Compression;
use crate::repo::control::CancelToken;
use crate::repo::pack::PackType;
use crate::repo::{EncryptionMode, Repository, WriteSessionOptions};
use crate::testutil::{MemoryBackend, PutLog, RecordingBackend, init_test_environment};

fn repo_on_counting_backend() -> (Repository, PutLog) {
    init_test_environment();
    let (backend, log) = RecordingBackend::new();
    let mut repo = Repository::init(
        Box::new(backend),
        crate::repo::EncryptionMode::None,
        crate::repo::ChunkerParams::default(),
        None,
        None,
        None,
    )
    .expect("init");
    repo.begin_write_session(&WriteSessionOptions::default())
        .expect("begin write session");
    (repo, log)
}

/// Store N distinct chunks, flush packs, and return their IDs.
fn store_chunks(repo: &mut Repository, count: usize) -> Vec<vykar_types::chunk_id::ChunkId> {
    let mut ids = Vec::with_capacity(count);
    for i in 0..count {
        let data = format!("chunk-data-{i:04}-padding-to-make-it-unique");
        let (id, _, _) = repo
            .store_chunk(data.as_bytes(), Compression::None, PackType::Tree)
            .unwrap();
        ids.push(id);
    }
    repo.flush_packs().unwrap();
    ids
}

/// Resolve chunk IDs to (ChunkId, PackId, offset, stored_size) tuples via the index.
fn resolve_chunks(
    repo: &Repository,
    ids: &[vykar_types::chunk_id::ChunkId],
) -> Vec<(
    vykar_types::chunk_id::ChunkId,
    vykar_types::pack_id::PackId,
    u64,
    u32,
)> {
    ids.iter()
        .map(|id| {
            let entry = *repo.chunk_index().get(id).unwrap();
            (*id, entry.pack_id, entry.pack_offset, entry.stored_size)
        })
        .collect()
}

#[test]
fn adjacent_blobs_coalesce_into_single_read() {
    let (mut repo, log) = repo_on_counting_backend();
    let ids = store_chunks(&mut repo, 3);
    let chunks = resolve_chunks(&repo, &ids);

    // All chunks should be in the same pack (small data, one flush).
    assert_eq!(
        chunks
            .iter()
            .map(|c| c.1)
            .collect::<std::collections::HashSet<_>>()
            .len(),
        1,
        "expected all chunks in one pack"
    );

    log.reset_get_range_count();

    let mut out = Vec::new();
    repo.read_chunks_coalesced_into(&chunks, &mut out, &CancelToken::new())
        .unwrap();

    // Should have been a single coalesced get_range call.
    assert_eq!(log.get_range_count(), 1, "expected 1 coalesced get_range");

    // Verify output matches reading chunks individually.
    let mut expected = Vec::new();
    for id in &ids {
        expected.extend_from_slice(&repo.read_chunk(id).unwrap());
    }
    assert_eq!(out, expected);
}

/// The repository does not cache decoded chunks: reading chunks that were
/// just read goes to storage again.
#[test]
fn repeat_reads_go_to_storage() {
    let (mut repo, log) = repo_on_counting_backend();
    let ids = store_chunks(&mut repo, 3);
    let chunks = resolve_chunks(&repo, &ids);

    let mut expected = Vec::new();
    for id in &ids {
        expected.extend_from_slice(&repo.read_chunk(id).unwrap());
    }

    log.reset_get_range_count();
    let mut out = Vec::new();
    repo.read_chunks_coalesced_into(&chunks, &mut out, &CancelToken::new())
        .unwrap();

    assert_eq!(
        log.get_range_count(),
        1,
        "expected the one coalesced get_range"
    );
    assert_eq!(out, expected);
}

#[test]
fn cross_pack_grouping() {
    let (mut repo, log) = repo_on_counting_backend();

    // Store chunks in separate packs by flushing between each.
    let data_a = b"pack-a-chunk-data-padding-unique-aa";
    let data_b = b"pack-b-chunk-data-padding-unique-bb";

    let (id_a, _, _) = repo
        .store_chunk(data_a, Compression::None, PackType::Tree)
        .unwrap();
    repo.flush_packs().unwrap();

    let (id_b, _, _) = repo
        .store_chunk(data_b, Compression::None, PackType::Tree)
        .unwrap();
    repo.flush_packs().unwrap();

    let chunks = resolve_chunks(&repo, &[id_a, id_b]);

    // Verify they're in different packs.
    assert_ne!(
        chunks[0].1, chunks[1].1,
        "expected chunks in different packs"
    );

    log.reset_get_range_count();

    let mut out = Vec::new();
    repo.read_chunks_coalesced_into(&chunks, &mut out, &CancelToken::new())
        .unwrap();

    // One get_range per pack.
    assert_eq!(
        log.get_range_count(),
        2,
        "expected 2 get_range calls (one per pack)"
    );

    // Verify order: id_a data then id_b data.
    let expected_a = repo.read_chunk(&id_a).unwrap();
    let expected_b = repo.read_chunk(&id_b).unwrap();
    let mut expected = Vec::new();
    expected.extend_from_slice(&expected_a);
    expected.extend_from_slice(&expected_b);
    assert_eq!(out, expected);
}

#[test]
fn output_order_preserved_across_packs() {
    let (mut repo, _log) = repo_on_counting_backend();

    // Create chunks in two packs, but request them in interleaved order.
    let (id_a1, _, _) = repo
        .store_chunk(
            b"a1-data-unique-padding-xxxxx",
            Compression::None,
            PackType::Tree,
        )
        .unwrap();
    let (id_a2, _, _) = repo
        .store_chunk(
            b"a2-data-unique-padding-yyyyy",
            Compression::None,
            PackType::Tree,
        )
        .unwrap();
    repo.flush_packs().unwrap();

    let (id_b1, _, _) = repo
        .store_chunk(
            b"b1-data-unique-padding-zzzzz",
            Compression::None,
            PackType::Tree,
        )
        .unwrap();
    repo.flush_packs().unwrap();

    // Request order: a1, b1, a2 — interleaved across packs.
    let chunks = resolve_chunks(&repo, &[id_a1, id_b1, id_a2]);

    let mut out = Vec::new();
    repo.read_chunks_coalesced_into(&chunks, &mut out, &CancelToken::new())
        .unwrap();

    let mut expected = Vec::new();
    expected.extend_from_slice(&repo.read_chunk(&id_a1).unwrap());
    expected.extend_from_slice(&repo.read_chunk(&id_b1).unwrap());
    expected.extend_from_slice(&repo.read_chunk(&id_a2).unwrap());
    assert_eq!(
        out, expected,
        "output must follow input order regardless of pack grouping"
    );
}

#[test]
fn empty_input_is_noop() {
    let (repo, log) = repo_on_counting_backend();
    log.reset_get_range_count();

    let mut out = Vec::new();
    repo.read_chunks_coalesced_into(&[], &mut out, &CancelToken::new())
        .unwrap();

    assert!(out.is_empty());
    assert_eq!(log.get_range_count(), 0);
}

#[test]
fn load_item_stream_from_ptrs_reports_chunk_not_in_index() {
    let (mut repo, _log) = repo_on_counting_backend();
    let missing = ChunkId::from_bytes([0xEE; 32]);
    let err = crate::commands::list::load_item_stream_from_ptrs(
        &mut repo,
        &[missing],
        &CancelToken::new(),
    )
    .unwrap_err();
    assert!(
        matches!(err, VykarError::ChunkNotInIndex(id) if id == missing),
        "got: {err}"
    );
}

#[test]
fn large_gap_splits_into_separate_reads() {
    let (mut repo, log) = repo_on_counting_backend();

    // Store: small chunk A, large gap chunk (>256 KiB), small chunk B — all in one flush.
    let data_a = b"small-chunk-a-unique-data-xxxxx";
    let gap_data = vec![0x42u8; 300 * 1024]; // 300 KiB — exceeds MAX_COALESCE_GAP
    let data_b = b"small-chunk-b-unique-data-yyyyy";

    let (id_a, _, _) = repo
        .store_chunk(data_a, Compression::None, PackType::Tree)
        .unwrap();
    let (id_gap, _, _) = repo
        .store_chunk(&gap_data, Compression::None, PackType::Tree)
        .unwrap();
    let (id_b, _, _) = repo
        .store_chunk(data_b, Compression::None, PackType::Tree)
        .unwrap();
    repo.flush_packs().unwrap();

    // All three should be in the same pack.
    let all_chunks = resolve_chunks(&repo, &[id_a, id_gap, id_b]);
    assert_eq!(
        all_chunks
            .iter()
            .map(|c| c.1)
            .collect::<std::collections::HashSet<_>>()
            .len(),
        1,
        "expected all chunks in one pack"
    );

    // Request only A and B, skipping the gap chunk.
    // The gap between A's end and B's start is > 256 KiB (the gap chunk sits between them).
    let chunks = resolve_chunks(&repo, &[id_a, id_b]);
    let gap_between = chunks[1].2 - (chunks[0].2 + chunks[0].3 as u64);
    assert!(
        gap_between > 256 * 1024,
        "gap {gap_between} should exceed MAX_COALESCE_GAP (256 KiB) to force a split"
    );

    log.reset_get_range_count();

    let mut out = Vec::new();
    repo.read_chunks_coalesced_into(&chunks, &mut out, &CancelToken::new())
        .unwrap();

    // Gap exceeds threshold — should be 2 separate get_range calls.
    assert_eq!(
        log.get_range_count(),
        2,
        "expected 2 get_range calls due to gap split"
    );

    // Verify output correctness.
    let mut expected = Vec::new();
    expected.extend_from_slice(&repo.read_chunk(&id_a).unwrap());
    expected.extend_from_slice(&repo.read_chunk(&id_b).unwrap());
    assert_eq!(out, expected);
}

#[test]
fn duplicate_chunk_ids_coalesce_into_single_range_read() {
    let (mut repo, log) = repo_on_counting_backend();
    let ids = store_chunks(&mut repo, 2);
    let chunks = resolve_chunks(&repo, &ids);

    // Build input with duplicates: [chunk0, chunk1, chunk0, chunk1].
    let mut dup_chunks = chunks.clone();
    dup_chunks.extend_from_slice(&chunks);

    log.reset_get_range_count();

    let mut out = Vec::new();
    repo.read_chunks_coalesced_into(&dup_chunks, &mut out, &CancelToken::new())
        .unwrap();

    // Duplicates sit at the same pack offset and coalesce into one range read.
    assert_eq!(
        log.get_range_count(),
        1,
        "duplicates at same offset should coalesce into a single range read"
    );

    // Output should be chunk0, chunk1, chunk0, chunk1 in order.
    let c0 = repo.read_chunk(&ids[0]).unwrap();
    let c1 = repo.read_chunk(&ids[1]).unwrap();
    let mut expected = Vec::new();
    expected.extend_from_slice(&c0);
    expected.extend_from_slice(&c1);
    expected.extend_from_slice(&c0);
    expected.extend_from_slice(&c1);
    assert_eq!(out, expected);
}

// ---------------------------------------------------------------------------
// Cancellation
// ---------------------------------------------------------------------------

/// Backend that raises a shared cancellation flag from *inside* the first
/// `get_range`, and counts how many range reads were issued. Lets a test prove
/// the read loop polls between storage requests rather than only at entry.
struct CancelOnFirstReadBackend {
    inner: MemoryBackend,
    flag: CancelToken,
    reads: Arc<AtomicUsize>,
    width: usize,
}

impl InnerBackend for CancelOnFirstReadBackend {
    fn inner_backend(&self) -> &dyn StorageBackend {
        &self.inner
    }
}

delegate_storage_backend! {
    for CancelOnFirstReadBackend;
    except [get_range, profile];

    fn get_range(&self, key: &str, offset: u64, length: u64) -> Result<Option<Vec<u8>>> {
        self.reads.fetch_add(1, Ordering::Relaxed);
        self.flag.cancel();
        self.inner.get_range(key, offset, length)
    }

    fn profile(&self) -> BackendProfile {
        BackendProfile {
            request_width: self.width,
            ..self.inner.profile()
        }
    }
}

/// Build a repo on the cancelling backend with the given request width and
/// store one chunk per pack, so the coalescer produces one read group (and
/// therefore one `get_range`) per chunk.
fn repo_with_cancelling_backend(
    packs: usize,
    width: usize,
) -> (
    Repository,
    CancelToken,
    Arc<AtomicUsize>,
    Vec<vykar_types::chunk_id::ChunkId>,
) {
    init_test_environment();
    let flag = CancelToken::new();
    let reads = Arc::new(AtomicUsize::new(0));
    let backend = CancelOnFirstReadBackend {
        inner: MemoryBackend::new(),
        flag: flag.clone(),
        reads: Arc::clone(&reads),
        width,
    };
    let mut repo = Repository::init(
        Box::new(backend),
        crate::repo::EncryptionMode::None,
        crate::repo::ChunkerParams::default(),
        None,
        None,
        None,
    )
    .expect("init");
    repo.begin_write_session(&WriteSessionOptions::default())
        .expect("begin write session");

    let mut ids = Vec::with_capacity(packs);
    for i in 0..packs {
        let data = format!("cancel-chunk-{i:04}-padding-to-make-it-unique");
        let (id, _, _) = repo
            .store_chunk(data.as_bytes(), Compression::None, PackType::Tree)
            .unwrap();
        repo.flush_packs().unwrap();
        ids.push(id);
    }
    reads.store(0, Ordering::Relaxed);
    (repo, flag, reads, ids)
}

/// Cancelling during the *first* of several range reads must abort the read
/// with `Interrupted` after exactly that one request. The pair of assertions is
/// the point: `Err` alone would also hold for an entry-only check, and a read
/// count of one alone would also hold for a loop that never polls at all.
/// Width 1, so this covers the sequential path.
#[test]
fn coalesced_read_cancels_between_range_reads() {
    let (repo, flag, reads, ids) = repo_with_cancelling_backend(3, 1);
    let chunks = resolve_chunks(&repo, &ids);
    assert_eq!(
        chunks
            .iter()
            .map(|c| c.1)
            .collect::<std::collections::HashSet<_>>()
            .len(),
        3,
        "expected one pack (and so one read group) per chunk"
    );
    assert!(!flag.is_cancelled(), "flag must start clear");

    let mut out = Vec::new();
    let err = repo
        .read_chunks_coalesced_into(&chunks, &mut out, &flag)
        .unwrap_err();

    assert!(
        matches!(err, VykarError::Interrupted),
        "expected Interrupted, got: {err}"
    );
    assert_eq!(
        reads.load(Ordering::Relaxed),
        1,
        "the loop must poll between range reads, not run to completion"
    );
}

/// With several workers, each polls the flag before its next range read, so a
/// cancellation raised inside the first read stops the fan-out after at most
/// one read per worker.
#[test]
fn parallel_coalesced_read_cancels_within_one_read_per_worker() {
    let (repo, flag, reads, ids) = repo_with_cancelling_backend(12, 4);
    let chunks = resolve_chunks(&repo, &ids);

    let mut out = Vec::new();
    let err = repo
        .read_chunks_coalesced_into(&chunks, &mut out, &flag)
        .unwrap_err();

    assert!(
        matches!(err, VykarError::Interrupted),
        "expected Interrupted, got: {err}"
    );
    let reads = reads.load(Ordering::Relaxed);
    assert!((1..=4).contains(&reads), "range reads {reads}");
}

/// A flag that is already set issues no storage request at all. Kept separate
/// from the mid-loop test: a pre-set flag trips the first loop-head check
/// before any read, so on its own it cannot demonstrate mid-loop cancellation.
#[test]
fn pre_cancelled_coalesced_read_issues_no_request() {
    for width in [1, 4] {
        let (repo, flag, reads, ids) = repo_with_cancelling_backend(3, width);
        let chunks = resolve_chunks(&repo, &ids);
        flag.cancel();

        let mut out = Vec::new();
        let err = repo
            .read_chunks_coalesced_into(&chunks, &mut out, &flag)
            .unwrap_err();

        assert!(
            matches!(err, VykarError::Interrupted),
            "width {width}: expected Interrupted, got: {err}"
        );
        assert_eq!(
            reads.load(Ordering::Relaxed),
            0,
            "width {width}: expected no range reads"
        );
        assert!(out.is_empty(), "width {width}: nothing may be emitted");
    }
}

/// Flip one byte of `data` inside its stored blob in a plaintext repo. With no
/// compression and no AEAD the blob stays decodable at the same length, so only
/// the chunk ID check can notice.
fn flip_byte_in_stored_chunk(repo: &Repository, id: &vykar_types::chunk_id::ChunkId, data: &[u8]) {
    let entry = *repo.chunk_index().get(id).unwrap();
    let key = entry.pack_id.storage_key();
    let mut pack = repo.storage.get(&key).unwrap().unwrap();
    let start = entry.pack_offset as usize;
    let blob = &pack[start..start + entry.stored_size as usize];
    let pos = blob
        .windows(data.len())
        .position(|w| w == data)
        .expect("plaintext chunk is stored verbatim");
    pack[start + pos + data.len() / 2] ^= 0x01;
    repo.storage.put(&key, &pack).unwrap();
}

#[test]
fn plaintext_read_chunk_rejects_corrupt_blob() {
    let mut repo = crate::testutil::test_repo_plaintext();
    let data = b"plaintext-chunk-that-will-be-corrupted-on-disk";
    let (id, _, _) = repo
        .store_chunk(data, Compression::None, PackType::Tree)
        .unwrap();
    repo.flush_packs().unwrap();
    flip_byte_in_stored_chunk(&repo, &id, data);

    for attempt in 0..2 {
        let err = repo.read_chunk(&id).unwrap_err();
        assert!(
            matches!(&err, VykarError::InvalidFormat(m) if m.contains("hash mismatch")),
            "attempt {attempt}: expected hash mismatch, got: {err}"
        );
    }
}

#[test]
fn plaintext_coalesced_read_rejects_corrupt_blob() {
    let mut repo = crate::testutil::test_repo_plaintext();
    let ids = store_chunks(&mut repo, 3);
    let corrupt = "chunk-data-0001-padding-to-make-it-unique";
    flip_byte_in_stored_chunk(&repo, &ids[1], corrupt.as_bytes());

    let chunks = resolve_chunks(&repo, &ids);
    let mut out = Vec::new();
    let err = repo
        .read_chunks_coalesced_into(&chunks, &mut out, &CancelToken::new())
        .unwrap_err();
    assert!(
        matches!(&err, VykarError::InvalidFormat(m) if m.contains("hash mismatch")),
        "expected hash mismatch, got: {err}"
    );
}

// ---------------------------------------------------------------------------
// Parallel reads
// ---------------------------------------------------------------------------

/// Shared knobs and counters of a [`TunedBackend`].
#[derive(Default)]
struct Probe {
    width: AtomicUsize,
    reads: AtomicUsize,
    in_flight: AtomicUsize,
    max_in_flight: AtomicUsize,
    /// Keys whose range reads sleep `delay_ms` first.
    slow: Mutex<HashSet<String>>,
    /// Sleep `delay_ms` in every range read.
    slow_all: AtomicBool,
    delay_ms: AtomicU64,
}

impl Probe {
    fn reset_counters(&self) {
        self.reads.store(0, Ordering::SeqCst);
        self.max_in_flight.store(0, Ordering::SeqCst);
    }
}

/// MemoryBackend with a settable request width; counts range reads and the
/// most that were in flight at once, and can slow down chosen reads.
struct TunedBackend {
    inner: MemoryBackend,
    probe: Arc<Probe>,
}

impl InnerBackend for TunedBackend {
    fn inner_backend(&self) -> &dyn StorageBackend {
        &self.inner
    }
}

delegate_storage_backend! {
    for TunedBackend;
    except [get_range, profile];

    fn get_range(&self, key: &str, offset: u64, length: u64) -> Result<Option<Vec<u8>>> {
        let p = &self.probe;
        p.reads.fetch_add(1, Ordering::SeqCst);
        let now = p.in_flight.fetch_add(1, Ordering::SeqCst) + 1;
        p.max_in_flight.fetch_max(now, Ordering::SeqCst);
        if p.slow_all.load(Ordering::SeqCst) || p.slow.lock().unwrap().contains(key) {
            std::thread::sleep(Duration::from_millis(p.delay_ms.load(Ordering::SeqCst)));
        }
        let result = self.inner.get_range(key, offset, length);
        p.in_flight.fetch_sub(1, Ordering::SeqCst);
        result
    }

    fn profile(&self) -> BackendProfile {
        BackendProfile {
            request_width: self.probe.width.load(Ordering::SeqCst),
            ..self.inner.profile()
        }
    }
}

fn repo_on_tuned_backend(mode: EncryptionMode) -> (Repository, Arc<Probe>) {
    init_test_environment();
    let probe = Arc::new(Probe::default());
    probe.width.store(2, Ordering::SeqCst);
    let backend = TunedBackend {
        inner: MemoryBackend::new(),
        probe: Arc::clone(&probe),
    };
    let passphrase = match mode {
        EncryptionMode::None => None,
        _ => Some("test-pass"),
    };
    let mut repo = Repository::init(
        Box::new(backend),
        mode,
        crate::repo::ChunkerParams::default(),
        passphrase,
        None,
        None,
    )
    .expect("init");
    repo.begin_write_session(&WriteSessionOptions::default())
        .expect("begin write session");
    (repo, probe)
}

/// Store `pack_sizes[p]` chunks in pack `p` (one flush per pack). Returns each
/// chunk's id and plaintext, pack by pack.
fn store_packs(repo: &mut Repository, pack_sizes: &[usize]) -> Vec<Vec<(ChunkId, Vec<u8>)>> {
    let mut packs = Vec::with_capacity(pack_sizes.len());
    for (p, &n) in pack_sizes.iter().enumerate() {
        let mut chunks = Vec::with_capacity(n);
        for c in 0..n {
            // Varied lengths, so blobs differ in size as well as content.
            let len = 64 + (p * 7 + c * 13) * 97 % 4000;
            let data: Vec<u8> = format!("pack-{p:02}-chunk-{c:02}-")
                .into_bytes()
                .into_iter()
                .chain((0..len).map(|i| (i * 31 + p * 17 + c * 5) as u8))
                .collect();
            let (id, _, _) = repo
                .store_chunk(&data, Compression::None, PackType::Tree)
                .unwrap();
            chunks.push((id, data));
        }
        repo.flush_packs().unwrap();
        packs.push(chunks);
    }
    packs
}

fn pack_key(repo: &Repository, id: &ChunkId) -> String {
    repo.chunk_index().get(id).unwrap().pack_id.storage_key()
}

/// Later groups finishing first must not reorder the output.
#[test]
fn parallel_read_keeps_stream_order_when_later_groups_finish_first() {
    let (mut repo, probe) = repo_on_tuned_backend(EncryptionMode::None);
    let packs = store_packs(&mut repo, &[2; 8]);
    // Interleaved across packs: every pack's first chunk, then every second.
    let order: Vec<&(ChunkId, Vec<u8>)> = (0..2)
        .flat_map(|c| packs.iter().map(move |p| &p[c]))
        .collect();
    let ids: Vec<ChunkId> = order.iter().map(|(id, _)| *id).collect();
    let expected: Vec<u8> = order.iter().flat_map(|(_, d)| d.clone()).collect();

    probe.width.store(4, Ordering::SeqCst);
    probe.delay_ms.store(50, Ordering::SeqCst);
    probe.slow.lock().unwrap().insert(pack_key(&repo, &ids[0]));
    probe.reset_counters();

    let chunks = resolve_chunks(&repo, &ids);
    let mut out = Vec::new();
    repo.read_chunks_coalesced_into(&chunks, &mut out, &CancelToken::new())
        .unwrap();

    assert_eq!(out, expected);
    assert_eq!(probe.reads.load(Ordering::SeqCst), 8, "one read per pack");
}

#[test]
fn parallel_read_overlaps_range_reads_up_to_width() {
    let (mut repo, probe) = repo_on_tuned_backend(EncryptionMode::None);
    let packs = store_packs(&mut repo, &[1; 9]);
    let ids: Vec<ChunkId> = packs.iter().map(|p| p[0].0).collect();
    let expected: Vec<u8> = packs.iter().flat_map(|p| p[0].1.clone()).collect();
    let chunks = resolve_chunks(&repo, &ids);
    probe.delay_ms.store(20, Ordering::SeqCst);
    probe.slow_all.store(true, Ordering::SeqCst);

    for width in [3, 1] {
        probe.width.store(width, Ordering::SeqCst);
        probe.reset_counters();

        let mut out = Vec::new();
        repo.read_chunks_coalesced_into(&chunks, &mut out, &CancelToken::new())
            .unwrap();

        assert_eq!(out, expected, "width {width}");
        assert_eq!(probe.reads.load(Ordering::SeqCst), 9, "width {width}");
        let max = probe.max_in_flight.load(Ordering::SeqCst);
        if width == 1 {
            assert_eq!(max, 1, "sequential path must read one at a time");
        } else {
            assert!((2..=width).contains(&max), "max in flight {max}");
        }
    }
}

/// The error is the one of the earliest failing group, not the first to be
/// recorded: group 1's read is slowed so group 4's failure lands first.
#[test]
fn parallel_read_returns_the_earliest_failing_group() {
    let (mut repo, probe) = repo_on_tuned_backend(EncryptionMode::None);
    let packs = store_packs(&mut repo, &[1; 6]);
    let ids: Vec<ChunkId> = packs.iter().map(|p| p[0].0).collect();
    let chunks = resolve_chunks(&repo, &ids);
    let missing_pack = chunks[1].1;

    repo.storage.delete(&missing_pack.storage_key()).unwrap();
    flip_byte_in_stored_chunk(&repo, &ids[4], &packs[4][0].1);
    probe.delay_ms.store(50, Ordering::SeqCst);
    probe
        .slow
        .lock()
        .unwrap()
        .insert(missing_pack.storage_key());

    for width in [4, 1] {
        probe.width.store(width, Ordering::SeqCst);
        for attempt in 0..5 {
            let mut out = Vec::new();
            let err = repo
                .read_chunks_coalesced_into(&chunks, &mut out, &CancelToken::new())
                .unwrap_err();
            assert!(
                matches!(&err, VykarError::Other(m)
                    if m.contains("pack not found") && m.contains(&missing_pack.to_string())),
                "width {width}, attempt {attempt}: expected pack 1 not found, got: {err}"
            );
        }
    }
}

/// The same item stream at every width, with and without AEAD.
#[test]
fn item_stream_bytes_identical_across_widths() {
    for mode in [EncryptionMode::None, EncryptionMode::Aes256Gcm] {
        let (mut repo, probe) = repo_on_tuned_backend(mode.clone());
        let packs = store_packs(&mut repo, &[3, 1, 2, 3, 3]);
        let all: Vec<&(ChunkId, Vec<u8>)> = packs.iter().flatten().collect();
        // A fixed permutation that hops between packs, plus one repeat.
        let order: Vec<&(ChunkId, Vec<u8>)> = [7, 0, 11, 3, 5, 9, 1, 4, 10, 2, 8, 6, 0]
            .iter()
            .map(|&i| all[i])
            .collect();
        let ids: Vec<ChunkId> = order.iter().map(|(id, _)| *id).collect();
        let expected: Vec<u8> = order.iter().flat_map(|(_, d)| d.clone()).collect();

        for width in [1, 2, 4, 16] {
            probe.width.store(width, Ordering::SeqCst);
            let stream = crate::commands::list::load_item_stream_from_ptrs(
                &mut repo,
                &ids,
                &CancelToken::new(),
            )
            .unwrap();
            assert!(
                stream == expected,
                "{mode:?}, width {width}: stream differs"
            );
        }
    }
}

/// Duplicates in one group share bytes of the fetched range; an in-place AEAD
/// decrypt of the first must not destroy the second.
#[test]
fn duplicate_chunk_ids_decode_under_aead() {
    let (mut repo, probe) = repo_on_tuned_backend(EncryptionMode::Aes256Gcm);
    let ids = store_chunks(&mut repo, 2);
    let chunks = resolve_chunks(&repo, &ids);
    let mut dup_chunks = chunks.clone();
    dup_chunks.extend_from_slice(&chunks);

    let c0 = repo.read_chunk(&ids[0]).unwrap();
    let c1 = repo.read_chunk(&ids[1]).unwrap();
    let expected = [c0.as_slice(), &c1, &c0, &c1].concat();

    for width in [1, 4] {
        probe.width.store(width, Ordering::SeqCst);
        probe.reset_counters();
        let mut out = Vec::new();
        repo.read_chunks_coalesced_into(&dup_chunks, &mut out, &CancelToken::new())
            .unwrap();
        assert_eq!(probe.reads.load(Ordering::SeqCst), 1, "width {width}");
        assert_eq!(out, expected, "width {width}");
    }
}
