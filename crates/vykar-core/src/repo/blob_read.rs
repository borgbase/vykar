//! The one fetch/decode loop for coalesced pack reads: a range of one pack is
//! opened with one storage request, its blobs are read in offset order,
//! decoded in place with [`decode_chunk_into`], and each is handed to a
//! per-blob visitor. Which thread reads which group, and what happens to the
//! plaintext, belong to the caller.
//!
//! Users: restore's file-data reads and
//! `Repository::read_chunks_coalesced_into` (whole item-stream loads).

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};

use super::bounded::try_for_each_bounded;
use super::coalesce::coalesce_pack_reads;
use super::control::CancelToken;
use super::format::{BlobLocation, ChunkDecodeError, decode_chunk_into};
use vykar_crypto::CryptoEngine;
use vykar_storage::StorageBackend;
use vykar_types::chunk_id::ChunkId;
use vykar_types::error::{Result, VykarError};
use vykar_types::pack_id::PackId;

/// A planned blob: where it is stored and what it must decode to.
pub(crate) trait PlannedRead {
    fn chunk_id(&self) -> ChunkId;
    fn pack_offset(&self) -> u64;
    fn stored_size(&self) -> u32;
    /// Exact plaintext length, when the caller knows it (restore). Passed to
    /// [`decode_chunk_into`] as the size hint and checked against the result.
    fn size_hint(&self) -> Option<usize> {
        None
    }
}

/// One coalesced range read: blobs of `pack_id`, sorted by offset, all inside
/// `[read_start, read_end)`.
pub(crate) struct ReadGroup<B> {
    pub(crate) pack_id: PackId,
    pub(crate) read_start: u64,
    pub(crate) read_end: u64, // exclusive
    pub(crate) blobs: Vec<B>,
}

impl<B> ReadGroup<B> {
    /// Bytes covered by the range request.
    pub(crate) fn range_len(&self) -> u64 {
        self.read_end - self.read_start
    }
}

/// Coalesce each pack's blobs into range reads ([`coalesce_pack_reads`]) and
/// return every group, packs in `by_pack` iteration order. The caller sorts.
///
/// # Errors
///
/// An offset that overflows `u64` (corrupt index), as `InvalidFormat`.
pub(crate) fn coalesce_groups<B: PlannedRead>(
    by_pack: HashMap<PackId, Vec<B>>,
) -> Result<Vec<ReadGroup<B>>> {
    let mut groups = Vec::new();
    for (pack_id, blobs) in by_pack {
        coalesce_pack_reads(
            &pack_id,
            blobs,
            |b| (b.pack_offset(), b.stored_size()),
            |read_start, read_end, blobs| {
                groups.push(ReadGroup {
                    pack_id,
                    read_start,
                    read_end,
                    blobs,
                })
            },
        )?;
    }
    Ok(groups)
}

/// Per-thread scratch for [`BlobReader::read_range`]: `raw` holds the opened
/// range (or, for a streaming backend such as `LocalBackend`, the current
/// piece), `plain` the decoded blob.
///
/// Both are reused from blob to blob and group to group. The plaintext `Vec`
/// stays reused unless the visitor takes it (`std::mem::take`), which is what a
/// caller that keeps every plaintext does. `check --verify-data` does not read
/// through here and allocates a fresh buffer per chunk on purpose.
#[derive(Default)]
pub(crate) struct ReadBuffers {
    raw: Vec<u8>,
    plain: Vec<u8>,
}

impl ReadBuffers {
    /// Largest capacity [`trim`](Self::trim) keeps, so one outlier group does
    /// not pin its high-water mark for the rest of a pass.
    pub(crate) const KEEP_CAP: usize = 2 * 1024 * 1024;

    /// Clear both buffers and shrink each to at most [`KEEP_CAP`](Self::KEEP_CAP).
    pub(crate) fn trim(&mut self) {
        for buf in [&mut self.raw, &mut self.plain] {
            buf.clear();
            if buf.capacity() > Self::KEEP_CAP {
                buf.shrink_to(Self::KEEP_CAP);
            }
        }
    }
}

/// How a group read ended when the visitor did not fail.
#[derive(Debug)]
pub(crate) enum GroupEnd {
    /// Every blob was read and visited.
    Done,
    /// `stopping()` returned true, before the open or before a blob. Blobs
    /// before that point were visited.
    Stopped,
    /// `open_range` returned `None`: the pack does not exist. Nothing was
    /// visited. (The hook for re-resolving a blob whose pack moved.)
    PackNotFound,
    /// `open_range` failed. Nothing was visited.
    OpenFailed(VykarError),
    /// Reading blob `index` failed. Blobs before it were visited, none from
    /// it on.
    ReadFailed { index: usize, error: VykarError },
}

/// What the visitor of [`BlobReader::read_range`] receives for one blob: the
/// plaintext buffer, or why the blob did not decode.
pub(crate) type Decoded<'p> = std::result::Result<&'p mut Vec<u8>, ChunkDecodeError>;

/// Storage, crypto and the verify flag a read decodes with.
#[derive(Clone, Copy)]
pub(crate) struct BlobReader<'a> {
    storage: &'a dyn StorageBackend,
    crypto: &'a dyn CryptoEngine,
    verify: bool,
}

impl<'a> BlobReader<'a> {
    /// `verify` checks every chunk ID; without it only a non-encrypting engine
    /// does (see [`decode_chunk_into`]).
    pub(crate) fn new(
        storage: &'a dyn StorageBackend,
        crypto: &'a dyn CryptoEngine,
        verify: bool,
    ) -> Self {
        Self {
            storage,
            crypto,
            verify,
        }
    }

    /// Open `[offset, offset + length)` of `pack_id` with one
    /// `open_range` request into the raw buffer of `bufs`, then for each blob
    /// in order: poll `stopping`, read the blob's stored bytes, decode them in
    /// place into the plaintext buffer (size hint `blob.size_hint()`, this
    /// reader's verify flag), and call `visit` with the plaintext or the
    /// decode error.
    ///
    /// A decode error does not end the group. A visitor error ends it at once
    /// and is returned as `Err`. `blobs` must be sorted by offset without
    /// overlaps and lie inside the range (the storage `RangeReader`
    /// contract); a blob that breaks it ends the group as
    /// [`GroupEnd::ReadFailed`]. The visitor may `std::mem::take` the
    /// plaintext `Vec`.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn read_range<B: PlannedRead, E>(
        &self,
        pack_id: &PackId,
        offset: u64,
        length: u64,
        blobs: &[B],
        bufs: &mut ReadBuffers,
        stopping: impl Fn() -> bool,
        mut visit: impl FnMut(&B, Decoded<'_>) -> std::result::Result<(), E>,
    ) -> std::result::Result<GroupEnd, E> {
        if stopping() {
            return Ok(GroupEnd::Stopped);
        }
        let ReadBuffers { raw, plain } = bufs;
        let mut reader = match self
            .storage
            .open_range(&pack_id.storage_key(), offset, length, raw)
        {
            Ok(Some(r)) => r,
            Ok(None) => return Ok(GroupEnd::PackNotFound),
            Err(e) => return Ok(GroupEnd::OpenFailed(e)),
        };
        for (index, blob) in blobs.iter().enumerate() {
            if stopping() {
                return Ok(GroupEnd::Stopped);
            }
            let raw = match reader.read_at(blob.pack_offset(), blob.stored_size() as usize) {
                Ok(raw) => raw,
                Err(error) => return Ok(GroupEnd::ReadFailed { index, error }),
            };
            let at = BlobLocation {
                chunk_id: blob.chunk_id(),
                pack_id: *pack_id,
                pack_offset: blob.pack_offset(),
                stored_size: blob.stored_size(),
            };
            let decoded =
                decode_chunk_into(raw, at, self.crypto, blob.size_hint(), self.verify, plain)
                    .map(|()| &mut *plain);
            visit(blob, decoded)?;
        }
        Ok(GroupEnd::Done)
    }

    /// [`read_range`](Self::read_range) over the whole of `group`.
    pub(crate) fn read_group<B: PlannedRead, E>(
        &self,
        group: &ReadGroup<B>,
        bufs: &mut ReadBuffers,
        stopping: impl Fn() -> bool,
        visit: impl FnMut(&B, Decoded<'_>) -> std::result::Result<(), E>,
    ) -> std::result::Result<GroupEnd, E> {
        self.read_range(
            &group.pack_id,
            group.read_start,
            group.range_len(),
            &group.blobs,
            bufs,
            stopping,
            visit,
        )
    }
}

/// Run `fetch` over `groups` on up to `width` threads and pass each result
/// to `accept` on the calling thread, in completion order. Threads claim
/// groups in slice order and poll `shutdown` before each fetch. With
/// `width <= 1` or a single group everything runs on the calling thread, in
/// order, and the first error is returned at once.
///
/// Returns the error of the earliest group (in `groups` order) that failed.
/// Every group before a failed one has been claimed, so for any failure that
/// does not depend on timing this is the error the sequential loop returns.
/// After the first failure no further group is claimed, and results that
/// arrive later are dropped. Results are queued unbounded until `accept`
/// takes them.
pub(crate) fn fetch_groups_bounded<G: Sync, T: Send>(
    width: usize,
    groups: &[G],
    shutdown: &CancelToken,
    fetch: impl Fn(&G) -> Result<T> + Sync,
    mut accept: impl FnMut(T) -> Result<()>,
) -> Result<()> {
    let threads = width.min(groups.len());
    if threads <= 1 {
        for group in groups {
            shutdown.check()?;
            accept(fetch(group)?)?;
        }
        return Ok(());
    }

    let indexed: Vec<(usize, &G)> = groups.iter().enumerate().collect();
    let (tx, rx) = crossbeam_channel::unbounded::<(usize, Result<T>)>();
    // Raised when `accept` fails, so workers stop claiming groups. Not raised
    // for fetch errors: an earlier, already-claimed group must still report.
    let stop = AtomicBool::new(false);
    let mut first_err: Option<(usize, VykarError)> = None;
    std::thread::scope(|s| {
        let stop = &stop;
        let fetch = &fetch;
        s.spawn(move || {
            // `Err(())` only stops handing out groups; the error goes over `tx`.
            let _ = try_for_each_bounded(width, &indexed, |&(idx, group)| {
                if stop.load(Ordering::Relaxed) {
                    return Err(());
                }
                let result = shutdown.check().and_then(|()| fetch(group));
                let failed = result.is_err();
                // The receiver lives until this thread drops `tx`.
                let _ = tx.send((idx, result));
                if failed { Err(()) } else { Ok(()) }
            });
        });
        // Ends once the spawned thread returns and drops `tx`.
        for (idx, result) in rx {
            let result = match result {
                Ok(product) if first_err.is_none() => accept(product).inspect_err(|_| {
                    stop.store(true, Ordering::Relaxed);
                }),
                Ok(_) => Ok(()),
                Err(e) => Err(e),
            };
            if let Err(e) = result
                && first_err.as_ref().is_none_or(|(i, _)| idx < *i)
            {
                first_err = Some((idx, e));
            }
        }
    });
    first_err.map_or(Ok(()), |(_, e)| Err(e))
}

#[cfg(test)]
mod tests {
    use std::cell::Cell;
    use std::sync::atomic::AtomicUsize;
    use std::time::Duration;

    use super::*;
    use crate::compress::Compression;
    use crate::repo::coalesce::MAX_COALESCE_GAP;
    use crate::repo::format::{ChunkDecodeStage, ObjectType, pack_object_with_context};
    use crate::testutil::{MemoryBackend, RecordingBackend, test_chunk_hasher};
    use vykar_crypto::PlaintextEngine;
    use vykar_crypto::aes_gcm::Aes256GcmEngine;

    struct Blob {
        id: ChunkId,
        offset: u64,
        size: u32,
        hint: Option<usize>,
    }

    impl PlannedRead for Blob {
        fn chunk_id(&self) -> ChunkId {
            self.id
        }
        fn pack_offset(&self) -> u64 {
            self.offset
        }
        fn stored_size(&self) -> u32 {
            self.size
        }
        fn size_hint(&self) -> Option<usize> {
            self.hint
        }
    }

    fn id_of(data: &[u8]) -> ChunkId {
        ChunkId::compute(&test_chunk_hasher(), data)
    }

    fn seal(context: ChunkId, data: &[u8], crypto: &dyn CryptoEngine) -> Vec<u8> {
        let compressed = crate::compress::compress(Compression::None, data).unwrap();
        pack_object_with_context(
            ObjectType::ChunkData,
            context.as_bytes(),
            &compressed,
            crypto,
        )
        .unwrap()
    }

    fn payloads(n: usize) -> Vec<Vec<u8>> {
        (0..n)
            .map(|i| format!("blob-read-payload-{i:02}-{}", "x".repeat(i * 7)).into_bytes())
            .collect()
    }

    /// Pack `payloads` back to back, each sealed under its real chunk ID.
    fn pack(crypto: &dyn CryptoEngine, payloads: &[Vec<u8>]) -> (Vec<u8>, Vec<Blob>) {
        let mut data = Vec::new();
        let mut blobs = Vec::new();
        for p in payloads {
            let sealed = seal(id_of(p), p, crypto);
            blobs.push(Blob {
                id: id_of(p),
                offset: data.len() as u64,
                size: sealed.len() as u32,
                hint: None,
            });
            data.extend_from_slice(&sealed);
        }
        (data, blobs)
    }

    fn pack_id() -> PackId {
        PackId::from_bytes([0x5A; 32])
    }

    fn store(data: &[u8]) -> MemoryBackend {
        let backend = MemoryBackend::new();
        backend.put(&pack_id().storage_key(), data).unwrap();
        backend
    }

    /// `[first offset, last end)` of `blobs`.
    fn span(blobs: &[Blob]) -> (u64, u64) {
        let start = blobs.iter().map(|b| b.offset).min().unwrap();
        let end = blobs
            .iter()
            .map(|b| b.offset + u64::from(b.size))
            .max()
            .unwrap();
        (start, end - start)
    }

    type Visited = Vec<(ChunkId, std::result::Result<Vec<u8>, ChunkDecodeError>)>;

    /// Read `blobs` as one range and record every visit.
    fn read_all(
        storage: &dyn StorageBackend,
        crypto: &dyn CryptoEngine,
        verify: bool,
        blobs: &[Blob],
        range: (u64, u64),
    ) -> (GroupEnd, Visited) {
        let mut visited = Vec::new();
        let end = BlobReader::new(storage, crypto, verify)
            .read_range(
                &pack_id(),
                range.0,
                range.1,
                blobs,
                &mut ReadBuffers::default(),
                || false,
                |blob, decoded| {
                    visited.push((blob.id, decoded.cloned()));
                    Ok::<(), ()>(())
                },
            )
            .unwrap();
        (end, visited)
    }

    #[test]
    fn read_range_visits_blobs_in_order() {
        let crypto = PlaintextEngine::new(test_chunk_hasher());
        let payloads = payloads(3);
        let (data, blobs) = pack(&crypto, &payloads);
        let storage = store(&data);
        let (end, visited) = read_all(&storage, &crypto, false, &blobs, span(&blobs));
        assert!(matches!(end, GroupEnd::Done), "got {end:?}");
        assert_eq!(visited.len(), 3);
        for ((id, plain), payload) in visited.iter().zip(&payloads) {
            assert_eq!(*id, id_of(payload));
            assert_eq!(plain.as_ref().unwrap(), payload);
        }
    }

    #[test]
    fn missing_pack_is_pack_not_found() {
        let crypto = PlaintextEngine::new(test_chunk_hasher());
        let (_, blobs) = pack(&crypto, &payloads(2));
        let storage = MemoryBackend::new();
        let (end, visited) = read_all(&storage, &crypto, false, &blobs, span(&blobs));
        assert!(matches!(end, GroupEnd::PackNotFound), "got {end:?}");
        assert!(visited.is_empty());
    }

    #[test]
    fn open_failure_is_open_failed() {
        let crypto = PlaintextEngine::new(test_chunk_hasher());
        let (data, blobs) = pack(&crypto, &payloads(2));
        let storage = store(&data);
        let (start, len) = span(&blobs);
        let (end, visited) = read_all(&storage, &crypto, false, &blobs, (start, len + 10));
        assert!(
            matches!(&end, GroupEnd::OpenFailed(e) if e.to_string().contains("short read")),
            "got {end:?}"
        );
        assert!(visited.is_empty());
    }

    #[test]
    fn overlapping_blob_is_read_failed_at_its_index() {
        let crypto = PlaintextEngine::new(test_chunk_hasher());
        let (data, mut blobs) = pack(&crypto, &payloads(1));
        let storage = store(&data);
        let again = Blob { ..blobs[0] };
        blobs.push(again);
        let (end, visited) = read_all(&storage, &crypto, false, &blobs, span(&blobs));
        assert!(
            matches!(end, GroupEnd::ReadFailed { index: 1, .. }),
            "got {end:?}"
        );
        assert_eq!(visited.len(), 1);
        assert!(visited[0].1.is_ok());
    }

    #[test]
    fn decode_error_goes_to_visitor_and_group_continues() {
        let plaintext = PlaintextEngine::new(test_chunk_hasher());
        let aes = Aes256GcmEngine::new(&[0x11; 32], test_chunk_hasher());
        let engines: [&dyn CryptoEngine; 2] = [&plaintext, &aes];
        for crypto in engines {
            let payloads = payloads(3);
            let (mut data, blobs) = pack(crypto, &payloads);
            let mid = &blobs[1];
            let pos = if crypto.is_encrypting() {
                // The last byte of the AEAD tag.
                (mid.offset + u64::from(mid.size) - 1) as usize
            } else {
                let blob = &data[mid.offset as usize..][..mid.size as usize];
                let at = blob
                    .windows(payloads[1].len())
                    .position(|w| w == payloads[1].as_slice())
                    .expect("plaintext payload is stored verbatim");
                mid.offset as usize + at + 3
            };
            data[pos] ^= 0x01;
            let storage = store(&data);

            let (end, visited) = read_all(&storage, crypto, false, &blobs, span(&blobs));
            let enc = crypto.is_encrypting();
            assert!(matches!(end, GroupEnd::Done), "encrypting={enc}: {end:?}");
            assert_eq!(visited.len(), 3);
            assert_eq!(visited[0].1.as_ref().unwrap(), &payloads[0]);
            let stage = &visited[1].1.as_ref().unwrap_err().stage;
            if enc {
                assert!(matches!(stage, ChunkDecodeStage::Decrypt(_)), "{stage:?}");
            } else {
                assert!(
                    matches!(stage, ChunkDecodeStage::IdMismatch { .. }),
                    "{stage:?}"
                );
            }
            assert_eq!(visited[2].1.as_ref().unwrap(), &payloads[2]);
        }
    }

    #[test]
    fn size_hint_and_verify_are_passed() {
        let plaintext = PlaintextEngine::new(test_chunk_hasher());
        let payloads = payloads(1);
        let (data, mut blobs) = pack(&plaintext, &payloads);
        blobs[0].hint = Some(payloads[0].len() + 1);
        let storage = store(&data);
        let (_, visited) = read_all(&storage, &plaintext, false, &blobs, span(&blobs));
        assert!(matches!(
            visited[0].1.as_ref().unwrap_err().stage,
            ChunkDecodeStage::SizeMismatch { .. }
        ));

        // Sealed (and so authenticated) under an ID that is not the payload's.
        let aes = Aes256GcmEngine::new(&[0x11; 32], test_chunk_hasher());
        let wrong = ChunkId::from_bytes([0xEE; 32]);
        let sealed = seal(wrong, &payloads[0], &aes);
        let storage = store(&sealed);
        let blobs = [Blob {
            id: wrong,
            offset: 0,
            size: sealed.len() as u32,
            hint: None,
        }];
        let (_, visited) = read_all(&storage, &aes, true, &blobs, span(&blobs));
        assert!(matches!(
            visited[0].1.as_ref().unwrap_err().stage,
            ChunkDecodeStage::IdMismatch { .. }
        ));
        let (_, visited) = read_all(&storage, &aes, false, &blobs, span(&blobs));
        assert_eq!(visited[0].1.as_ref().unwrap(), &payloads[0]);
    }

    #[test]
    fn stop_before_open_issues_no_request() {
        let crypto = PlaintextEngine::new(test_chunk_hasher());
        let (data, blobs) = pack(&crypto, &payloads(2));
        let (storage, log) = RecordingBackend::new();
        storage.put(&pack_id().storage_key(), &data).unwrap();
        log.reset_get_range_count();
        let (start, len) = span(&blobs);
        let mut visits = 0;
        let end = BlobReader::new(&storage, &crypto, false)
            .read_range(
                &pack_id(),
                start,
                len,
                &blobs,
                &mut ReadBuffers::default(),
                || true,
                |_, _| {
                    visits += 1;
                    Ok::<(), ()>(())
                },
            )
            .unwrap();
        assert!(matches!(end, GroupEnd::Stopped), "got {end:?}");
        assert_eq!(visits, 0);
        assert_eq!(log.get_range_count(), 0);
    }

    #[test]
    fn stop_between_blobs() {
        let crypto = PlaintextEngine::new(test_chunk_hasher());
        let (data, blobs) = pack(&crypto, &payloads(3));
        let storage = store(&data);
        let (start, len) = span(&blobs);
        let stop = Cell::new(false);
        let mut visits = 0;
        let end = BlobReader::new(&storage, &crypto, false)
            .read_range(
                &pack_id(),
                start,
                len,
                &blobs,
                &mut ReadBuffers::default(),
                || stop.get(),
                |_, decoded| {
                    decoded.unwrap();
                    visits += 1;
                    stop.set(true);
                    Ok::<(), ()>(())
                },
            )
            .unwrap();
        assert!(matches!(end, GroupEnd::Stopped), "got {end:?}");
        assert_eq!(visits, 1);
    }

    #[test]
    fn visitor_error_short_circuits() {
        let crypto = PlaintextEngine::new(test_chunk_hasher());
        let (data, blobs) = pack(&crypto, &payloads(2));
        let storage = store(&data);
        let (start, len) = span(&blobs);
        let mut visits = 0;
        let result = BlobReader::new(&storage, &crypto, false).read_range(
            &pack_id(),
            start,
            len,
            &blobs,
            &mut ReadBuffers::default(),
            || false,
            |_, _| {
                visits += 1;
                Err("visitor failed")
            },
        );
        assert!(matches!(result, Err("visitor failed")));
        assert_eq!(visits, 1);
    }

    #[test]
    fn visitor_may_take_plaintext() {
        let crypto = PlaintextEngine::new(test_chunk_hasher());
        let payloads = payloads(3);
        let (data, blobs) = pack(&crypto, &payloads);
        let storage = store(&data);
        let (start, len) = span(&blobs);
        let mut taken = Vec::new();
        let mut bufs = ReadBuffers::default();
        let end = BlobReader::new(&storage, &crypto, false)
            .read_range(
                &pack_id(),
                start,
                len,
                &blobs,
                &mut bufs,
                || false,
                |_, decoded| {
                    taken.push(std::mem::take(decoded.unwrap()));
                    Ok::<(), ()>(())
                },
            )
            .unwrap();
        assert!(matches!(end, GroupEnd::Done), "got {end:?}");
        assert_eq!(taken, payloads);
        assert!(bufs.plain.is_empty());
    }

    #[test]
    fn trim_caps_capacity() {
        let mut bufs = ReadBuffers::default();
        bufs.raw.resize(ReadBuffers::KEEP_CAP + 4096, 1);
        bufs.plain.resize(ReadBuffers::KEEP_CAP * 2, 2);
        bufs.trim();
        for buf in [&bufs.raw, &bufs.plain] {
            assert!(buf.is_empty());
            assert!(buf.capacity() <= ReadBuffers::KEEP_CAP);
        }
    }

    #[test]
    fn coalesce_groups_matches_coalesce_pack_reads() {
        let blob = |byte: u8, offset: u64| Blob {
            id: ChunkId::from_bytes([byte; 32]),
            offset,
            size: 100,
            hint: None,
        };
        let far = 204 + MAX_COALESCE_GAP + 1;
        let pack_a = PackId::from_bytes([1; 32]);
        let pack_b = PackId::from_bytes([2; 32]);
        let mut by_pack = HashMap::new();
        by_pack.insert(pack_a, vec![blob(3, far), blob(2, 104), blob(1, 0)]);
        by_pack.insert(pack_b, vec![blob(4, 500)]);

        let mut groups = coalesce_groups(by_pack).unwrap();
        groups.sort_by_key(|g| (*g.pack_id.as_bytes(), g.read_start));
        let got: Vec<(PackId, u64, u64, Vec<u64>)> = groups
            .iter()
            .map(|g| {
                let offsets = g.blobs.iter().map(|b| b.offset).collect();
                (g.pack_id, g.read_start, g.read_end, offsets)
            })
            .collect();
        assert_eq!(
            got,
            vec![
                (pack_a, 0, 204, vec![0, 104]),
                (pack_a, far, far + 100, vec![far]),
                (pack_b, 500, 600, vec![500]),
            ]
        );
    }

    #[test]
    fn fetch_groups_bounded_reports_earliest_error() {
        let groups: Vec<usize> = (0..8).collect();
        for width in [4, 1] {
            let err = fetch_groups_bounded(
                width,
                &groups,
                &CancelToken::new(),
                |&g| {
                    if g == 2 {
                        std::thread::sleep(Duration::from_millis(50));
                    }
                    if g == 2 || g == 5 {
                        Err(VykarError::Other(format!("group {g} failed")))
                    } else {
                        Ok(g)
                    }
                },
                |_| Ok(()),
            )
            .unwrap_err();
            assert_eq!(err.to_string(), "group 2 failed", "width {width}");
        }
    }

    #[test]
    fn fetch_groups_bounded_accept_error_stops_claims() {
        let groups: Vec<usize> = (0..64).collect();
        for width in [2, 1] {
            let fetches = AtomicUsize::new(0);
            let err = fetch_groups_bounded(
                width,
                &groups,
                &CancelToken::new(),
                |&g| {
                    fetches.fetch_add(1, Ordering::SeqCst);
                    std::thread::sleep(Duration::from_millis(2));
                    Ok(g)
                },
                |_| Err(VykarError::Other("accept failed".into())),
            )
            .unwrap_err();
            assert_eq!(err.to_string(), "accept failed", "width {width}");
            let n = fetches.load(Ordering::SeqCst);
            if width == 1 {
                assert_eq!(n, 1);
            } else {
                assert!(n < groups.len(), "width {width}: {n} fetches");
            }
        }
    }
}
