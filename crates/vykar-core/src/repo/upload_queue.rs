use std::panic::AssertUnwindSafe;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

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

/// Handed to an upload: drop it (or call `release`) once the pack's heap
/// bytes are freed, so the queue stops counting them.
pub(crate) struct HeldBytes(Arc<AtomicBool>);

impl HeldBytes {
    pub(crate) fn release(self) {}
}

impl Drop for HeldBytes {
    fn drop(&mut self) {
        self.0.store(true, Ordering::Release);
    }
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
    /// Heap bytes held by the closed pack when queued (0 for a staged pack,
    /// whose bytes are already in a temp file); see `held_bytes` for what is
    /// still held.
    pub(crate) bytes: usize,
    /// Set once the upload has freed the heap bytes.
    released: Arc<AtomicBool>,
    /// Set, before `seq` goes out on the completion channel, when the job
    /// returned, panicked or was dropped unrun.
    finished: Arc<AtomicBool>,
    /// The job's outcome: the pack ID on success. Disconnected without a
    /// value when the job was dropped unrun.
    result: crossbeam_channel::Receiver<Result<PackId>>,
}

impl PendingUpload {
    /// Heap bytes this upload still holds: `bytes` until the upload releases
    /// them or its job finishes, then 0.
    pub(crate) fn held_bytes(&self) -> usize {
        if self.released.load(Ordering::Acquire) || self.is_finished() {
            0
        } else {
            self.bytes
        }
    }

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
/// journals it; a failed one never is.
pub(crate) struct UploadQueue {
    pending: Vec<PendingUpload>,
    /// Upload jobs send their `seq` here as they finish, so a blocked
    /// caller wakes on whichever upload completes first.
    done_tx: crossbeam_channel::Sender<u64>,
    done_rx: crossbeam_channel::Receiver<u64>,
    next_seq: u64,
    /// Upload slots. `UPLOAD_QUEUE_HEADROOM` more closed packs may queue
    /// before `wait_below_count_cap` blocks, so up to this plus the headroom
    /// uploads run at once, one per upload thread.
    pub(crate) max_in_flight_uploads: usize,
    /// Also bound closed-but-unwritten packs by bytes: the queued packs plus
    /// the one about to be closed may not exceed the current data-pack
    /// target, so at most one closed pack waits while the next one fills.
    /// Enabled for local repositories, where a write rarely overlaps
    /// usefully with the next seal; see `cap_pending_uploads`.
    pub(crate) cap_pending_upload_bytes: bool,
    /// Jobs for the upload threads. Unbounded: the count cap
    /// (`wait_below_count_cap`) bounds what is queued.
    jobs_tx: crossbeam_channel::Sender<UploadJob>,
    jobs_rx: crossbeam_channel::Receiver<UploadJob>,
    /// Long-lived upload threads, started on demand up to
    /// `max_in_flight_uploads + UPLOAD_QUEUE_HEADROOM`. Never joined:
    /// dropping the queue closes `jobs_tx`; each thread runs the jobs still
    /// queued, then exits (detached, as per-pack threads were).
    workers: Vec<std::thread::JoinHandle<()>>,
}

impl UploadQueue {
    /// Empty queue; the byte cap starts disabled. Starts no thread.
    pub(crate) fn new(max_in_flight_uploads: usize) -> Self {
        let (done_tx, done_rx) = crossbeam_channel::unbounded();
        let (jobs_tx, jobs_rx) = crossbeam_channel::unbounded();
        Self {
            pending: Vec::new(),
            done_tx,
            done_rx,
            next_seq: 0,
            max_in_flight_uploads,
            cap_pending_upload_bytes: false,
            jobs_tx,
            jobs_rx,
            workers: Vec::new(),
        }
    }

    /// Queue `upload` for an upload thread. `bytes` is the heap the upload
    /// holds until it drops the [`HeldBytes`] it is handed, or returns; the
    /// upload returns the ID of the pack it stored. Starts one more upload
    /// thread while fewer than `max_in_flight_uploads + UPLOAD_QUEUE_HEADROOM`
    /// exist. Err only when an upload thread cannot be started.
    pub(crate) fn submit<F>(
        &mut self,
        entries: Vec<PackedChunkEntry>,
        trailer_len: u32,
        bytes: usize,
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
            let worker = std::thread::Builder::new()
                .name(format!("vykar-upload-{}", self.workers.len()))
                .spawn(move || {
                    for job in rx {
                        job();
                    }
                })
                .map_err(|e| VykarError::Other(format!("starting a pack upload thread: {e}")))?;
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
        let released = Arc::new(AtomicBool::new(false));
        let held = HeldBytes(Arc::clone(&released));
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
        self.jobs_tx
            .send(job)
            .map_err(|_| VykarError::Other("pack upload queue closed".into()))?;
        self.pending.push(PendingUpload {
            seq,
            entries,
            trailer_len,
            bytes,
            released,
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
                return Self::finish(upload).map(Some);
            }
            // Every queued upload sends exactly once; messages from uploads
            // already joined elsewhere are skipped.
            let seq = self
                .done_rx
                .recv()
                .map_err(|_| VykarError::Other("upload completion channel closed".into()))?;
            if let Some(i) = self.pending.iter().position(|u| u.seq == seq) {
                let upload = self.pending.swap_remove(i);
                return Self::finish(upload).map(Some);
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
                on_confirmed(Self::finish(upload)?);
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
            Some(e) => Err(e),
            None => Ok(()),
        }
    }

    /// Count cap: with all slots plus headroom taken, wait for whichever
    /// upload finishes first. Returns whether it waited.
    pub(crate) fn wait_below_count_cap(
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
        // All slots + buffer full — wait for the first one to finish.
        if let Some(confirmed) = self.finish_any()? {
            on_confirmed(confirmed);
        }
        Ok(true)
    }

    /// Byte cap (no-op unless `cap_pending_upload_bytes`): wait until queued
    /// bytes plus `incoming` fit `cap` or the queue is empty. Returns whether
    /// it waited.
    pub(crate) fn wait_below_byte_cap(
        &mut self,
        incoming: usize,
        cap: usize,
        mut on_confirmed: impl FnMut(ConfirmedPack),
    ) -> Result<bool> {
        // Byte cap on sealed-but-unwritten packs. `incoming` is the buffer
        // about to be sealed. The count cap alone allows 4 sealed packs of
        // up to the 192 MiB target each; when the disk falls behind, they
        // pile up and peak RSS jumps by one pack per straggler. An empty
        // queue never blocks, so one upload is always in flight.
        if !self.cap_pending_upload_bytes {
            return Ok(false);
        }
        let mut blocked = false;
        while !self.pending.is_empty() {
            let pending: usize = self.pending.iter().map(PendingUpload::held_bytes).sum();
            if pending.saturating_add(incoming) <= cap {
                break;
            }
            // Wait for whichever queued upload finishes first.
            if let Some(confirmed) = self.finish_any()? {
                on_confirmed(confirmed);
            }
            blocked = true;
        }
        Ok(blocked)
    }

    /// Pipeline budget to lend to the upload queue: half the heap bytes still
    /// held by queued uploads (see `PendingUpload::held_bytes`: a pack whose
    /// upload released its buffer, or whose job finished, holds none),
    /// beyond what the upload slots themselves hold (`max_in_flight_uploads`
    /// packs of the current data-pack target). The backup pipeline subtracts
    /// this from its byte budget, so the slack queued behind a slow link is
    /// not held a second time as processed pipeline output.
    ///
    /// Half, not all: the pipeline's lead is what refills the queue when an
    /// upload returns. Lending the full slack measured -22% peak RSS but
    /// +11% wall on REST at 40 ms RTT; half kept wall within 1.5%.
    ///
    /// Always 0 when the byte cap already bounds the queue (local).
    pub(crate) fn slack_bytes(&self, pack_target: usize) -> usize {
        if self.cap_pending_upload_bytes {
            return 0;
        }
        let pending: usize = self.pending.iter().map(PendingUpload::held_bytes).sum();
        let in_slots = self.max_in_flight_uploads.saturating_mul(pack_target);
        pending.saturating_sub(in_slots) / 2
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

    #[test]
    fn join_all_confirms_only_successes() {
        let a = PackId::from_bytes([1; 32]);
        let b = PackId::from_bytes([2; 32]);
        let mut queue = UploadQueue::new(2);
        queue.submit(Vec::new(), 0, 0, move |_| Ok(a)).unwrap();
        queue.submit(Vec::new(), 0, 0, move |_| Ok(b)).unwrap();
        queue
            .submit(Vec::new(), 0, 0, |_| Err(VykarError::Other("x".into())))
            .unwrap();

        let mut confirmed = Vec::new();
        let result = queue.join_all(|p| confirmed.push(p.pack_id));
        assert!(result.is_err());
        assert_eq!(confirmed, vec![a, b]);
        assert!(queue.is_empty());
    }

    #[test]
    fn byte_cap_disabled_never_waits() {
        let mut queue = UploadQueue::new(2);
        assert!(!queue.cap_pending_upload_bytes);
        let (release, gate) = std::sync::mpsc::channel::<()>();
        queue
            .submit(Vec::new(), 0, 100, move |_held| {
                let _ = gate.recv();
                Ok(PackId::from_bytes([1; 32]))
            })
            .unwrap();

        let waited = queue
            .wait_below_byte_cap(100, 100, |_| panic!("nothing may be joined"))
            .unwrap();
        assert!(!waited);
        assert_eq!(queue.len(), 1);

        release.send(()).unwrap();
        queue.join_all(|_| {}).unwrap();
    }

    #[test]
    fn released_upload_stops_counting_before_its_thread_returns() {
        let mut queue = UploadQueue::new(1);
        let (open_a, gate_a) = std::sync::mpsc::channel::<()>();
        let (open_b, gate_b) = std::sync::mpsc::channel::<()>();
        let (go_tx, go) = std::sync::mpsc::channel::<()>();
        let (ack, acked) = std::sync::mpsc::channel::<()>();
        queue
            .submit(Vec::new(), 0, 100, move |_held| {
                let _ = gate_a.recv();
                Ok(PackId::from_bytes([1; 32]))
            })
            .unwrap();
        queue
            .submit(Vec::new(), 0, 100, move |held| {
                let _ = go.recv();
                drop(held);
                let _ = ack.send(());
                let _ = gate_b.recv();
                Ok(PackId::from_bytes([2; 32]))
            })
            .unwrap();

        // Two packs held, one slot of 100: (200 - 100) / 2.
        assert_eq!(queue.slack_bytes(100), 50);
        go_tx.send(()).unwrap();
        acked.recv().unwrap();
        assert_eq!(queue.slack_bytes(100), 0);
        // Nothing joined yet, so the second upload is still second.
        let upload_b = &queue.pending()[1];
        assert!(!upload_b.is_finished());
        assert_eq!(upload_b.held_bytes(), 0);

        open_a.send(()).unwrap();
        open_b.send(()).unwrap();
        queue.join_all(|_| {}).unwrap();
    }

    #[test]
    fn byte_cap_ignores_released_bytes() {
        let mut queue = UploadQueue::new(2);
        queue.cap_pending_upload_bytes = true;
        let (open, gate) = std::sync::mpsc::channel::<()>();
        let (ack, acked) = std::sync::mpsc::channel::<()>();
        queue
            .submit(Vec::new(), 0, 100, move |held| {
                drop(held);
                let _ = ack.send(());
                let _ = gate.recv();
                Ok(PackId::from_bytes([1; 32]))
            })
            .unwrap();
        acked.recv().unwrap();

        // If the cap still counted the released bytes it would block on the
        // upload; the watchdog then opens the gate and the asserts fail.
        let watchdog = open.clone();
        std::thread::spawn(move || {
            std::thread::sleep(std::time::Duration::from_secs(10));
            let _ = watchdog.send(());
        });
        let mut joined = 0;
        let waited = queue
            .wait_below_byte_cap(100, 100, |_| joined += 1)
            .unwrap();
        assert!(!waited);
        assert_eq!(joined, 0);
        assert_eq!(queue.len(), 1);

        open.send(()).unwrap();
        queue.join_all(|_| {}).unwrap();
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
            queue
                .wait_below_count_cap(|p| confirmed.push(p.pack_id))
                .unwrap();
            queue
                .submit(Vec::new(), 0, 0, move |_| Ok(pack_id_of(n)))
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
                .submit(Vec::new(), 0, 0, move |_| Ok(pack_id_of(n)))
                .unwrap();
        }
        queue.join_all(|_| {}).unwrap();
        assert_eq!(queue.worker_count(), pool);

        queue
            .submit(Vec::new(), 0, 0, |_| -> Result<PackId> {
                panic!("injected upload panic")
            })
            .unwrap();
        let err = UploadQueue::finish(queue.take_all().remove(0))
            .err()
            .expect("a panicking upload fails");
        assert!(err.to_string().contains("panicked"), "{err}");

        queue
            .submit(Vec::new(), 0, 0, |_| Ok(pack_id_of(7)))
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
        // One more than the pool: a direct submit bypasses the count cap, so
        // the last job waits in the queue behind the running ones.
        for n in 0..=pool as u32 {
            let (open, gate) = std::sync::mpsc::channel::<()>();
            gates.push(open);
            let ran = Arc::clone(&ran);
            queue
                .submit(Vec::new(), 0, 0, move |_| {
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
}
