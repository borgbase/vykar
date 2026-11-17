use std::thread::JoinHandle;

use crate::index::PendingChunkEntry;
use vykar_types::error::{Result, VykarError};
use vykar_types::pack_id::PackId;

/// Extra upload handles allowed beyond `max_in_flight_uploads` before
/// blocking; the storage crate's [`vykar_storage::UPLOAD_HEADROOM`].
pub(super) const UPLOAD_QUEUE_HEADROOM: usize = vykar_storage::UPLOAD_HEADROOM;

/// A background pack upload plus what to journal once it is confirmed.
pub(crate) struct PendingUpload {
    /// Sent on the session's completion channel when the thread finishes.
    seq: u64,
    pub(crate) pack_id: PackId,
    pub(crate) journal_chunks: Vec<PendingChunkEntry>,
    /// Cleared by rollback: the pack is still uploaded (an orphan for
    /// compact) but must not enter the journal.
    pub(crate) journal: bool,
    /// Heap bytes held by the sealed pack until the upload thread returns
    /// (0 for a staged pack, whose bytes are already in a temp file).
    pub(crate) bytes: usize,
    pub(crate) handle: JoinHandle<Result<()>>,
}

/// Announces an upload thread's completion on drop, so the send also happens
/// when the upload panics (dev/test builds unwind).
struct UploadDone {
    tx: crossbeam_channel::Sender<u64>,
    seq: u64,
}

impl Drop for UploadDone {
    fn drop(&mut self) {
        // The receiver lives as long as the session; a closed channel only
        // means nobody is waiting any more.
        let _ = self.tx.send(self.seq);
    }
}

/// Background pack uploads waiting to be joined, in no particular order.
/// A confirmed upload is handed to the caller (`on_confirmed`), which
/// journals it; a failed or rolled-back one never is.
pub(crate) struct UploadQueue {
    pending: Vec<PendingUpload>,
    /// Upload threads send their `seq` here as they finish, so a blocked
    /// caller wakes on whichever upload completes first.
    done_tx: crossbeam_channel::Sender<u64>,
    done_rx: crossbeam_channel::Receiver<u64>,
    next_seq: u64,
    /// Upload slots. `UPLOAD_QUEUE_HEADROOM` more sealed packs may queue
    /// before `wait_below_count_cap` blocks, so up to this plus the headroom
    /// uploads run at once.
    pub(crate) max_in_flight_uploads: usize,
    /// Also bound sealed-but-unwritten packs by bytes: the queued packs plus
    /// the one about to be sealed may not exceed the current data-pack
    /// target, so at most one sealed pack waits while the next one fills.
    /// Enabled for local repositories, where a write rarely overlaps
    /// usefully with the next seal; see `cap_pending_uploads`.
    pub(crate) cap_pending_upload_bytes: bool,
}

impl UploadQueue {
    /// Empty queue; the byte cap starts disabled.
    pub(crate) fn new(max_in_flight_uploads: usize) -> Self {
        let (done_tx, done_rx) = crossbeam_channel::unbounded();
        Self {
            pending: Vec::new(),
            done_tx,
            done_rx,
            next_seq: 0,
            max_in_flight_uploads,
            cap_pending_upload_bytes: false,
        }
    }

    /// Run `upload` on a background thread and queue it for joining.
    /// `bytes` is the heap the upload holds until it returns.
    pub(crate) fn spawn<F>(
        &mut self,
        pack_id: PackId,
        journal_chunks: Vec<PendingChunkEntry>,
        bytes: usize,
        upload: F,
    ) where
        F: FnOnce() -> Result<()> + Send + 'static,
    {
        let seq = self.next_seq;
        self.next_seq += 1;
        let done = UploadDone {
            tx: self.done_tx.clone(),
            seq,
        };
        let handle = std::thread::spawn(move || {
            let _done = done;
            upload()
        });
        self.pending.push(PendingUpload {
            seq,
            pack_id,
            journal_chunks,
            journal: true,
            bytes,
            handle,
        });
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.pending.is_empty()
    }

    /// Whether any queued upload thread has returned (not yet joined).
    pub(crate) fn any_finished(&self) -> bool {
        self.pending.iter().any(|u| u.handle.is_finished())
    }

    /// Remove every queued upload without joining it (abort path).
    pub(crate) fn take_all(&mut self) -> Vec<PendingUpload> {
        std::mem::take(&mut self.pending)
    }

    /// Keep uploads of rolled-back packs still in flight from being
    /// journaled when they complete.
    pub(crate) fn exclude_from_journal(&mut self, pack_ids: &[PackId]) {
        let rolled_back: std::collections::HashSet<_> = pack_ids.iter().collect();
        for upload in &mut self.pending {
            if rolled_back.contains(&upload.pack_id) {
                upload.journal = false;
            }
        }
    }

    /// Join one upload. `Some((pack_id, chunks))` if it succeeded and was not
    /// rolled back; `None` if rolled back; the upload's error otherwise.
    pub(crate) fn finish(
        upload: PendingUpload,
    ) -> Result<Option<(PackId, Vec<PendingChunkEntry>)>> {
        let PendingUpload {
            pack_id,
            journal_chunks,
            journal,
            handle,
            ..
        } = upload;
        handle
            .join()
            .map_err(|_| VykarError::Other("pack upload thread panicked".into()))??;
        Ok(journal.then_some((pack_id, journal_chunks)))
    }

    /// Block until any queued upload finishes, then join it.
    /// `Ok(None)` when nothing is queued.
    fn finish_any(&mut self) -> Result<Option<(PackId, Vec<PendingChunkEntry>)>> {
        loop {
            if self.pending.is_empty() {
                return Ok(None);
            }
            if let Some(i) = self.pending.iter().position(|u| u.handle.is_finished()) {
                let upload = self.pending.swap_remove(i);
                return Self::finish(upload);
            }
            // Every queued upload sends exactly once; messages from uploads
            // already joined elsewhere are skipped.
            let seq = self
                .done_rx
                .recv()
                .map_err(|_| VykarError::Other("upload completion channel closed".into()))?;
            if let Some(i) = self.pending.iter().position(|u| u.seq == seq) {
                let upload = self.pending.swap_remove(i);
                return Self::finish(upload);
            }
        }
    }

    /// Join all finished upload threads, propagating the first error. Each
    /// confirmed upload goes to `on_confirmed` before the next is joined.
    pub(crate) fn drain_finished(
        &mut self,
        mut on_confirmed: impl FnMut(PackId, Vec<PendingChunkEntry>),
    ) -> Result<()> {
        let mut i = 0;
        while let Some(slot) = self.pending.get(i) {
            if slot.handle.is_finished() {
                let upload = self.pending.swap_remove(i);
                if let Some((pack_id, chunks)) = Self::finish(upload)? {
                    on_confirmed(pack_id, chunks);
                }
            } else {
                i += 1;
            }
        }
        Ok(())
    }

    /// Join every queued upload and drop their stale completions; the first
    /// upload error, if any.
    pub(crate) fn join_all(
        &mut self,
        mut on_confirmed: impl FnMut(PackId, Vec<PendingChunkEntry>),
    ) -> Result<()> {
        let mut first_err: Option<VykarError> = None;
        for upload in std::mem::take(&mut self.pending) {
            match Self::finish(upload) {
                Ok(Some((pack_id, chunks))) => on_confirmed(pack_id, chunks),
                Ok(None) => {}
                Err(e) => {
                    first_err.get_or_insert(e);
                }
            }
        }
        // All joined, so all have sent: drop their stale completions.
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
        mut on_confirmed: impl FnMut(PackId, Vec<PendingChunkEntry>),
    ) -> Result<bool> {
        if self.pending.len()
            < self
                .max_in_flight_uploads
                .saturating_add(UPLOAD_QUEUE_HEADROOM)
        {
            return Ok(false);
        }
        // All slots + buffer full — wait for the first one to finish.
        if let Some((pack_id, chunks)) = self.finish_any()? {
            on_confirmed(pack_id, chunks);
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
        mut on_confirmed: impl FnMut(PackId, Vec<PendingChunkEntry>),
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
            let pending: usize = self.pending.iter().map(|u| u.bytes).sum();
            if pending.saturating_add(incoming) <= cap {
                break;
            }
            // Wait for whichever queued upload finishes first.
            if let Some((pack_id, chunks)) = self.finish_any()? {
                on_confirmed(pack_id, chunks);
            }
            blocked = true;
        }
        Ok(blocked)
    }

    /// Pipeline budget to lend to the upload queue: half the heap bytes of
    /// sealed packs whose upload has not returned yet, beyond what the
    /// upload slots themselves hold (`max_in_flight_uploads` packs of the
    /// current data-pack target). The backup pipeline subtracts this from
    /// its byte budget, so the slack queued behind a slow link is not held
    /// a second time as processed pipeline output.
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
        // A finished upload thread has already dropped its buffer.
        let pending: usize = self
            .pending
            .iter()
            .filter(|u| !u.handle.is_finished())
            .map(|u| u.bytes)
            .sum();
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
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn join_all_confirms_only_journaled_successes() {
        let a = PackId::from_bytes([1; 32]);
        let b = PackId::from_bytes([2; 32]);
        let c = PackId::from_bytes([3; 32]);
        let mut queue = UploadQueue::new(2);
        queue.spawn(a, Vec::new(), 0, || Ok(()));
        queue.spawn(b, Vec::new(), 0, || Ok(()));
        queue.exclude_from_journal(&[b]);
        queue.spawn(c, Vec::new(), 0, || Err(VykarError::Other("x".into())));

        let mut confirmed = Vec::new();
        let result = queue.join_all(|id, _| confirmed.push(id));
        assert!(result.is_err());
        assert_eq!(confirmed, vec![a]);
        assert!(queue.is_empty());
    }

    #[test]
    fn byte_cap_disabled_never_waits() {
        let mut queue = UploadQueue::new(2);
        assert!(!queue.cap_pending_upload_bytes);
        let (release, gate) = std::sync::mpsc::channel::<()>();
        queue.spawn(PackId::from_bytes([1; 32]), Vec::new(), 100, move || {
            let _ = gate.recv();
            Ok(())
        });

        let waited = queue
            .wait_below_byte_cap(100, 100, |_, _| panic!("nothing may be joined"))
            .unwrap();
        assert!(!waited);
        assert_eq!(queue.len(), 1);

        release.send(()).unwrap();
        queue.join_all(|_, _| {}).unwrap();
    }
}
