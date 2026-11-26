use std::panic::AssertUnwindSafe;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use super::memory_budget::HeldBytes;
use super::pack::PackedChunkEntry;
use vykar_types::error::{Result, VykarError};
use vykar_types::pack_id::PackId;

/// Extra upload handles allowed beyond `max_in_flight_uploads` before
/// blocking; the storage crate's [`vykar_storage::UPLOAD_HEADROOM`].
pub(super) const UPLOAD_QUEUE_HEADROOM: usize = vykar_storage::UPLOAD_HEADROOM;

/// The error of an upload that panicked, or was dropped without running.
fn panicked_error() -> VykarError {
    VykarError::Other("pack upload thread panicked".into())
}

/// A confirmed upload: what the session journals and adds to its delta.
pub(crate) struct ConfirmedPack {
    pub(crate) pack_id: PackId,
    pub(crate) entries: Vec<PackedChunkEntry>,
    pub(crate) trailer_len: u32,
}

/// A queued or running pack upload, plus what to record once confirmed.
pub(crate) struct PendingUpload {
    /// Sent on the session's completion channel when the job finishes.
    seq: u64,
    /// The pack's entries. Its ID is known only once the upload thread
    /// has finished the pack, and arrives with the result.
    pub(crate) entries: Vec<PackedChunkEntry>,
    pub(crate) trailer_len: u32,
    /// Set, before `seq` goes out on the completion channel, when the job
    /// returned, panicked or was dropped unrun.
    finished: Arc<AtomicBool>,
    /// The job's outcome: the pack ID on success. Disconnected without a
    /// value when the job was dropped unrun.
    result: crossbeam_channel::Receiver<Result<PackId>>,
}

impl PendingUpload {
    /// Whether the upload's job has finished (its result is waiting).
    pub(crate) fn is_finished(&self) -> bool {
        self.finished.load(Ordering::Acquire)
    }
}

/// Announces a job's completion on drop, so it also happens when the upload
/// panics (dev/test builds unwind) or the job is dropped unrun: marks the
/// upload finished, then sends its `seq`.
struct UploadDone {
    tx: crossbeam_channel::Sender<u64>,
    seq: u64,
    finished: Arc<AtomicBool>,
}

impl Drop for UploadDone {
    fn drop(&mut self) {
        self.finished.store(true, Ordering::Release);
        // The receiver lives as long as the session; a closed channel only
        // means nobody is waiting any more.
        let _ = self.tx.send(self.seq);
    }
}

/// What an upload thread runs.
type UploadJob = Box<dyn FnOnce() + Send + 'static>;

/// Pack uploads waiting to be joined, run on a fixed pool of long-lived
/// threads. Joined in completion order, not in submission order. A
/// confirmed upload is handed to the caller (`on_confirmed`), which
/// journals it; a failed one never is. The first failure is remembered
/// (`failure`).
pub(crate) struct UploadQueue {
    pending: Vec<PendingUpload>,
    /// Upload jobs send their `seq` here as they finish, so a blocked
    /// caller wakes on whichever upload completes first.
    done_tx: crossbeam_channel::Sender<u64>,
    done_rx: crossbeam_channel::Receiver<u64>,
    next_seq: u64,
    /// Upload slots. `UPLOAD_QUEUE_HEADROOM` more closed packs may queue
    /// before `wait_for_slot` blocks, so up to this plus the headroom
    /// uploads run at once, one per upload thread. The source of the slot
    /// count the memory budget lends against.
    pub(crate) max_in_flight_uploads: usize,
    /// Jobs for the upload threads. Unbounded: the slot bound
    /// (`wait_for_slot`) bounds what is queued.
    jobs_tx: crossbeam_channel::Sender<UploadJob>,
    jobs_rx: crossbeam_channel::Receiver<UploadJob>,
    /// Long-lived upload threads, started on demand up to
    /// `max_in_flight_uploads + UPLOAD_QUEUE_HEADROOM`. Never joined:
    /// dropping the queue closes `jobs_tx`; each thread runs the jobs still
    /// queued, then exits (detached, as per-pack threads were).
    workers: Vec<std::thread::JoinHandle<()>>,
    /// The text of the first upload or submit failure. Once set, the
    /// session refuses to store or save (`WriteSession::refuse_if_failed`):
    /// the lost pack's chunks are still dedup hits. The queue itself keeps
    /// working, so `flush_on_abort` can still join and journal what landed.
    failure: Option<String>,
}

impl UploadQueue {
    /// Empty queue. Starts no thread.
    pub(crate) fn new(max_in_flight_uploads: usize) -> Self {
        let (done_tx, done_rx) = crossbeam_channel::unbounded();
        let (jobs_tx, jobs_rx) = crossbeam_channel::unbounded();
        Self {
            pending: Vec::new(),
            done_tx,
            done_rx,
            next_seq: 0,
            max_in_flight_uploads,
            jobs_tx,
            jobs_rx,
            workers: Vec::new(),
            failure: None,
        }
    }

    /// Remember `e` as the queue's failure (the first one is kept) and hand
    /// it back unchanged.
    pub(super) fn record_failure(&mut self, e: VykarError) -> VykarError {
        self.failure.get_or_insert_with(|| e.to_string());
        e
    }

    /// The first upload or submit failure, if any.
    pub(crate) fn failure(&self) -> Option<&str> {
        self.failure.as_deref()
    }

    /// Queue `upload` for an upload thread. `held` charges the pack's heap
    /// bytes to the memory budget until the upload drops it (it is handed
    /// over), or returns, panics or is dropped unrun; the upload returns the
    /// ID of the pack it stored. Starts one more upload
    /// thread while fewer than `max_in_flight_uploads + UPLOAD_QUEUE_HEADROOM`
    /// exist. Err only when an upload thread cannot be started.
    pub(crate) fn submit<F>(
        &mut self,
        entries: Vec<PackedChunkEntry>,
        trailer_len: u32,
        held: HeldBytes,
        upload: F,
    ) -> Result<()>
    where
        F: FnOnce(HeldBytes) -> Result<PackId> + Send + 'static,
    {
        // Read at every call: tests change `max_in_flight_uploads` after
        // `new`.
        let pool = self
            .max_in_flight_uploads
            .saturating_add(UPLOAD_QUEUE_HEADROOM);
        if self.workers.len() < pool {
            let rx = self.jobs_rx.clone();
            let spawned = std::thread::Builder::new()
                .name(format!("vykar-upload-{}", self.workers.len()))
                .spawn(move || {
                    for job in rx {
                        job();
                    }
                });
            let worker = spawned.map_err(|e| {
                self.record_failure(VykarError::Other(format!(
                    "starting a pack upload thread: {e}"
                )))
            })?;
            self.workers.push(worker);
        }

        let seq = self.next_seq;
        self.next_seq += 1;
        let finished = Arc::new(AtomicBool::new(false));
        let done = UploadDone {
            tx: self.done_tx.clone(),
            seq,
            finished: Arc::clone(&finished),
        };
        let (result_tx, result_rx) = crossbeam_channel::bounded(1);
        let job: UploadJob = Box::new(move || {
            // Sound: the upload owns what it touches, except atomics and the
            // storage and crypto `Arc`s, which a panic leaves consistent.
            let result = std::panic::catch_unwind(AssertUnwindSafe(move || upload(held)))
                .unwrap_or_else(|_| Err(panicked_error()));
            let _ = result_tx.send(result);
            // Marks the upload finished, then sends `seq`.
            drop(done);
        });
        if self.jobs_tx.send(job).is_err() {
            return Err(self.record_failure(VykarError::Other("pack upload queue closed".into())));
        }
        self.pending.push(PendingUpload {
            seq,
            entries,
            trailer_len,
            finished,
            result: result_rx,
        });
        Ok(())
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.pending.is_empty()
    }

    /// Whether any queued upload has finished (not yet joined).
    pub(crate) fn any_finished(&self) -> bool {
        self.pending.iter().any(PendingUpload::is_finished)
    }

    /// Remove every queued upload without joining it (abort path).
    pub(crate) fn take_all(&mut self) -> Vec<PendingUpload> {
        std::mem::take(&mut self.pending)
    }

    /// Join one upload, waiting for it: its pack to record if it succeeded,
    /// the upload's error otherwise.
    pub(crate) fn finish(upload: PendingUpload) -> Result<ConfirmedPack> {
        let PendingUpload {
            entries,
            trailer_len,
            result,
            ..
        } = upload;
        // Disconnected without a value: the job was dropped unrun.
        let pack_id = result.recv().map_err(|_| panicked_error())??;
        Ok(ConfirmedPack {
            pack_id,
            entries,
            trailer_len,
        })
    }

    /// Block until any queued upload finishes, then join it.
    /// `Ok(None)` when nothing is queued.
    fn finish_any(&mut self) -> Result<Option<ConfirmedPack>> {
        loop {
            if self.pending.is_empty() {
                return Ok(None);
            }
            if let Some(i) = self.pending.iter().position(PendingUpload::is_finished) {
                let upload = self.pending.swap_remove(i);
                return Self::finish(upload)
                    .map(Some)
                    .map_err(|e| self.record_failure(e));
            }
            // Every queued upload sends exactly once; messages from uploads
            // already joined elsewhere are skipped.
            let Ok(seq) = self.done_rx.recv() else {
                return Err(self
                    .record_failure(VykarError::Other("upload completion channel closed".into())));
            };
            if let Some(i) = self.pending.iter().position(|u| u.seq == seq) {
                let upload = self.pending.swap_remove(i);
                return Self::finish(upload)
                    .map(Some)
                    .map_err(|e| self.record_failure(e));
            }
        }
    }

    /// Join all finished uploads, propagating the first error. Each
    /// confirmed upload goes to `on_confirmed` before the next is joined.
    pub(crate) fn drain_finished(
        &mut self,
        mut on_confirmed: impl FnMut(ConfirmedPack),
    ) -> Result<()> {
        let mut i = 0;
        while let Some(slot) = self.pending.get(i) {
            if slot.is_finished() {
                let upload = self.pending.swap_remove(i);
                on_confirmed(Self::finish(upload).map_err(|e| self.record_failure(e))?);
            } else {
                i += 1;
            }
        }
        Ok(())
    }

    /// Join every queued upload and drop their stale completions; the first
    /// upload error, if any.
    pub(crate) fn join_all(&mut self, mut on_confirmed: impl FnMut(ConfirmedPack)) -> Result<()> {
        let mut first_err: Option<VykarError> = None;
        for upload in std::mem::take(&mut self.pending) {
            match Self::finish(upload) {
                Ok(confirmed) => on_confirmed(confirmed),
                Err(e) => {
                    first_err.get_or_insert(e);
                }
            }
        }
        // All joined. A job sends its `seq` just after its result, so a
        // late one is skipped by `finish_any` like any stale completion.
        while self.done_rx.try_recv().is_ok() {}
        match first_err {
            Some(e) => Err(self.record_failure(e)),
            None => Ok(()),
        }
    }

    /// Slot bound: with all slots plus headroom taken, wait for whichever
    /// upload finishes first. Bounds the queued jobs and the staged packs
    /// (one open temp file each), not memory. Returns whether it waited.
    pub(crate) fn wait_for_slot(
        &mut self,
        mut on_confirmed: impl FnMut(ConfirmedPack),
    ) -> Result<bool> {
        if self.pending.len()
            < self
                .max_in_flight_uploads
                .saturating_add(UPLOAD_QUEUE_HEADROOM)
        {
            return Ok(false);
        }
        // All slots + headroom taken: wait for the first one to finish.
        if let Some(confirmed) = self.finish_any()? {
            on_confirmed(confirmed);
        }
        Ok(true)
    }

    /// Join whichever queued upload finishes first. `Ok(false)` when none
    /// is queued (the caller's floor).
    pub(crate) fn join_one(&mut self, mut on_confirmed: impl FnMut(ConfirmedPack)) -> Result<bool> {
        match self.finish_any()? {
            Some(confirmed) => {
                on_confirmed(confirmed);
                Ok(true)
            }
            None => Ok(false),
        }
    }

    #[cfg(test)]
    pub(crate) fn len(&self) -> usize {
        self.pending.len()
    }

    #[cfg(test)]
    pub(crate) fn pending(&self) -> &[PendingUpload] {
        &self.pending
    }

    #[cfg(test)]
    pub(crate) fn worker_count(&self) -> usize {
        self.workers.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::repo::memory_budget::{MemoryBudget, PackPolicy};

    #[test]
    fn join_all_confirms_only_successes() {
        let a = PackId::from_bytes([1; 32]);
        let b = PackId::from_bytes([2; 32]);
        let mut queue = UploadQueue::new(2);
        queue
            .submit(Vec::new(), 0, HeldBytes::none(), move |_| Ok(a))
            .unwrap();
        queue
            .submit(Vec::new(), 0, HeldBytes::none(), move |_| Ok(b))
            .unwrap();
        queue
            .submit(Vec::new(), 0, HeldBytes::none(), |_| {
                Err(VykarError::Other("x".into()))
            })
            .unwrap();

        let mut confirmed = Vec::new();
        let result = queue.join_all(|p| confirmed.push(p.pack_id));
        assert!(result.is_err());
        assert_eq!(confirmed, vec![a, b]);
        assert!(queue.is_empty());
    }

    /// An upload that drops its charge stops counting at once, while its
    /// thread still runs and before it is joined.
    #[test]
    fn released_upload_stops_counting_before_its_thread_returns() {
        let budget = MemoryBudget::new(1, 100, 10);
        let mut queue = UploadQueue::new(1);
        let (open_a, gate_a) = std::sync::mpsc::channel::<()>();
        let (open_b, gate_b) = std::sync::mpsc::channel::<()>();
        let (go_tx, go) = std::sync::mpsc::channel::<()>();
        let (ack, acked) = std::sync::mpsc::channel::<()>();
        queue
            .submit(Vec::new(), 0, budget.hold_sealed(100), move |_held| {
                let _ = gate_a.recv();
                Ok(PackId::from_bytes([1; 32]))
            })
            .unwrap();
        queue
            .submit(Vec::new(), 0, budget.hold_sealed(100), move |held| {
                let _ = go.recv();
                drop(held);
                let _ = ack.send(());
                let _ = gate_b.recv();
                Ok(PackId::from_bytes([2; 32]))
            })
            .unwrap();

        // Two packs held, one slot of 100: (200 - 100) / 2.
        assert_eq!(budget.lent(), 50);
        go_tx.send(()).unwrap();
        acked.recv().unwrap();
        assert_eq!(budget.snapshot().sealed_packs, 100);
        assert_eq!(budget.lent(), 0);
        // Nothing joined yet, so the second upload is still second.
        assert!(!queue.pending()[1].is_finished());

        open_a.send(()).unwrap();
        open_b.send(()).unwrap();
        queue.join_all(|_| {}).unwrap();
        assert_eq!(budget.snapshot().sealed_packs, 0);
    }

    /// Released bytes no longer count against the local cap, although the
    /// upload is still queued.
    #[test]
    fn cap_ignores_released_bytes() {
        let budget = MemoryBudget::new(2, 100, 10);
        budget.configure(PackPolicy::CapAtTarget, true);
        let mut queue = UploadQueue::new(2);
        let (open, gate) = std::sync::mpsc::channel::<()>();
        let (ack, acked) = std::sync::mpsc::channel::<()>();
        queue
            .submit(Vec::new(), 0, budget.hold_sealed(100), move |held| {
                drop(held);
                let _ = ack.send(());
                let _ = gate.recv();
                Ok(PackId::from_bytes([1; 32]))
            })
            .unwrap();
        acked.recv().unwrap();
        assert!(budget.sealed_room(100));
        assert_eq!(queue.len(), 1);
        assert!(!queue.any_finished());

        open.send(()).unwrap();
        queue.join_all(|_| {}).unwrap();
    }

    /// A dropped-unrun or panicking job still releases its charge.
    #[test]
    fn failed_upload_releases_its_charge() {
        let budget = MemoryBudget::new(1, 100, 10);
        let mut queue = UploadQueue::new(1);
        queue
            .submit(
                Vec::new(),
                0,
                budget.hold_sealed(70),
                |_held| -> Result<PackId> { panic!("injected upload panic") },
            )
            .unwrap();
        assert!(queue.join_all(|_| {}).is_err());
        assert_eq!(budget.snapshot().sealed_packs, 0);
    }

    #[test]
    fn join_one_floor_and_first_finished() {
        let mut queue = UploadQueue::new(2);
        assert!(!queue.join_one(|_| panic!("nothing queued")).unwrap());
        queue
            .submit(Vec::new(), 0, HeldBytes::none(), |_| Ok(pack_id_of(3)))
            .unwrap();
        let mut confirmed = Vec::new();
        assert!(queue.join_one(|p| confirmed.push(p.pack_id)).unwrap());
        assert_eq!(confirmed, vec![pack_id_of(3)]);
        assert!(queue.is_empty());
    }

    fn pack_id_of(n: u32) -> PackId {
        let mut id = [0u8; 32];
        id[..4].copy_from_slice(&n.to_le_bytes());
        PackId::from_bytes(id)
    }

    /// A thousand uploads run on the same few threads: never more than the
    /// slots plus the headroom.
    #[test]
    fn pool_stays_flat_over_many_uploads() {
        let mut queue = UploadQueue::new(2);
        let pool = 2 + UPLOAD_QUEUE_HEADROOM;
        let mut confirmed = Vec::new();
        for n in 0..1_000u32 {
            queue.wait_for_slot(|p| confirmed.push(p.pack_id)).unwrap();
            queue
                .submit(Vec::new(), 0, HeldBytes::none(), move |_| Ok(pack_id_of(n)))
                .unwrap();
            assert!(queue.worker_count() <= pool);
        }
        assert_eq!(queue.worker_count(), pool);
        queue.join_all(|p| confirmed.push(p.pack_id)).unwrap();
        confirmed.sort_by_key(|p| *p.as_bytes());
        let mut expected: Vec<PackId> = (0..1_000).map(pack_id_of).collect();
        expected.sort_by_key(|p| *p.as_bytes());
        assert_eq!(confirmed, expected);
    }

    /// A panicking upload is an error for its pack only: the thread that ran
    /// it takes the next job.
    #[test]
    fn panicking_upload_is_an_error_and_its_thread_survives() {
        let mut queue = UploadQueue::new(1);
        let pool = 1 + UPLOAD_QUEUE_HEADROOM;
        for n in 0..pool as u32 {
            queue
                .submit(Vec::new(), 0, HeldBytes::none(), move |_| Ok(pack_id_of(n)))
                .unwrap();
        }
        queue.join_all(|_| {}).unwrap();
        assert_eq!(queue.worker_count(), pool);

        queue
            .submit(Vec::new(), 0, HeldBytes::none(), |_| -> Result<PackId> {
                panic!("injected upload panic")
            })
            .unwrap();
        let err = UploadQueue::finish(queue.take_all().remove(0))
            .err()
            .expect("a panicking upload fails");
        assert!(err.to_string().contains("panicked"), "{err}");

        queue
            .submit(Vec::new(), 0, HeldBytes::none(), |_| Ok(pack_id_of(7)))
            .unwrap();
        let mut confirmed = Vec::new();
        queue.join_all(|p| confirmed.push(p.pack_id)).unwrap();
        assert_eq!(confirmed, vec![pack_id_of(7)]);
        assert_eq!(queue.worker_count(), pool);
    }

    /// Dropping the queue does not cancel its uploads: running and still
    /// queued jobs all run.
    #[test]
    fn dropping_the_queue_still_runs_queued_uploads() {
        let mut queue = UploadQueue::new(1);
        let pool = 1 + UPLOAD_QUEUE_HEADROOM;
        let ran = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let mut gates = Vec::new();
        // One more than the pool: a direct submit bypasses the slot bound, so
        // the last job waits in the queue behind the running ones.
        for n in 0..=pool as u32 {
            let (open, gate) = std::sync::mpsc::channel::<()>();
            gates.push(open);
            let ran = Arc::clone(&ran);
            queue
                .submit(Vec::new(), 0, HeldBytes::none(), move |_| {
                    let _ = gate.recv();
                    ran.fetch_add(1, Ordering::SeqCst);
                    Ok(pack_id_of(n))
                })
                .unwrap();
        }
        assert_eq!(queue.worker_count(), pool);
        drop(queue);
        for open in gates {
            open.send(()).unwrap();
        }
        let start = std::time::Instant::now();
        while ran.load(Ordering::SeqCst) != pool + 1 {
            assert!(start.elapsed() < std::time::Duration::from_secs(10));
            std::thread::sleep(std::time::Duration::from_millis(1));
        }
    }

    /// The first upload error is remembered on every join path, and a later
    /// success does not clear it. The two submit error paths (an upload
    /// thread that cannot be started, a closed job channel) record through
    /// the same `record_failure`; neither can be forced here without a
    /// test hook.
    #[test]
    fn a_failed_upload_is_remembered() {
        fn fail(msg: &'static str) -> impl FnOnce(HeldBytes) -> Result<PackId> + Send + 'static {
            move |_: HeldBytes| Err(VykarError::Other(msg.into()))
        }

        let mut queue = UploadQueue::new(2);
        assert!(queue.failure().is_none());
        queue
            .submit(Vec::new(), 0, HeldBytes::none(), fail("first"))
            .unwrap();
        queue
            .submit(Vec::new(), 0, HeldBytes::none(), fail("second"))
            .unwrap();
        assert!(queue.join_all(|_| {}).is_err());
        assert!(queue.failure().unwrap().contains("first"));
        queue
            .submit(Vec::new(), 0, HeldBytes::none(), |_| Ok(pack_id_of(1)))
            .unwrap();
        queue.join_all(|_| {}).unwrap();
        assert!(queue.failure().unwrap().contains("first"));

        let mut queue = UploadQueue::new(2);
        queue
            .submit(Vec::new(), 0, HeldBytes::none(), fail("drained"))
            .unwrap();
        while !queue.any_finished() {
            std::thread::yield_now();
        }
        assert!(queue.drain_finished(|_| {}).is_err());
        assert!(queue.failure().unwrap().contains("drained"));

        let mut queue = UploadQueue::new(2);
        queue
            .submit(Vec::new(), 0, HeldBytes::none(), fail("joined"))
            .unwrap();
        assert!(queue.join_one(|_| {}).is_err());
        assert!(queue.failure().unwrap().contains("joined"));
    }
}
