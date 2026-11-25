//! One budget for the backup's buffered bytes: what the pipeline holds
//! between the walk and the consumer, the packs being filled, and the
//! closed heap packs waiting for their upload.
//!
//! Three accounts with their own rules, one object, one ceiling and one
//! figure (the [`MemorySnapshot`]):
//!
//! - **pipeline**: raw bytes of files and segments read ahead of the
//!   consumer, acquired by the walk before the read and released by the
//!   consumer once their chunks are committed. Capped at `P`.
//! - **open packs**: heap bytes of the data and tree packs being filled.
//! - **sealed packs**: closed heap packs not yet uploaded, charged at
//!   submit by their final length and released when the upload frees the
//!   buffer ([`HeldBytes`]).
//!
//! How sealed packs and the pipeline interact is the [`PackPolicy`].

use std::mem;
use std::sync::{Arc, Condvar, Mutex};

use vykar_protocol::PACK_HEADER_SIZE;
use vykar_types::error::{Result, VykarError};

use super::pack::{MAX_BLOBS_PER_PACK, PackType};
use super::pack_trailer::trailer_frame_len_upper;
use super::upload_queue::UPLOAD_QUEUE_HEADROOM;

/// Who may borrow pipeline capacity against sealed packs.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PackPolicy {
    /// Remote: sealed heap packs beyond `slots × target` lend half their
    /// bytes out of the pipeline capacity. No byte cap on sealed packs; the
    /// upload slot bound alone limits them.
    ///
    /// Half, not all: the pipeline's lead is what refills the upload queue
    /// when an upload returns. Lending all of it measured -22% peak RSS but
    /// +11% wall on REST at 40 ms RTT; half kept wall within 1.5%.
    LendHalf,
    /// Local: no lending; sealed heap packs plus the one about to close may
    /// not exceed one data-pack target, so at most one closed pack waits
    /// while the next one fills. A write rarely overlaps usefully with the
    /// next seal there.
    CapAtTarget,
}

/// One figure for everything the budget governs.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct MemorySnapshot {
    pub(crate) pipeline: usize,
    pub(crate) open_packs: usize,
    pub(crate) sealed_packs: usize,
    /// `P`; 0 before the pipeline ran.
    pub(crate) pipeline_cap: usize,
    /// The current data-pack target `T`.
    pub(crate) pack_target: usize,
    /// [`MemoryBudget::ceiling`] at the time of the snapshot.
    pub(crate) ceiling: usize,
    pub(crate) peak_pipeline: usize,
    pub(crate) peak_open_packs: usize,
    pub(crate) peak_sealed_packs: usize,
    /// Peak of pipeline + open_packs + sealed_packs, sampled at every change.
    pub(crate) peak_total: usize,
}

/// See the module docs. Every method takes one leaf mutex: nothing else is
/// locked, no I/O, channel operation or callback runs while it is held.
pub(crate) struct MemoryBudget {
    state: Mutex<State>,
    /// Waited on only by [`Self::acquire_pipeline`]; notified by every
    /// decrease of `pipeline` or `sealed`, every change of the pack target,
    /// slot count or policy, and by poison.
    changed: Condvar,
}

struct State {
    pack_policy: PackPolicy,
    /// Upload slots `W`: a mirror of `UploadQueue::max_in_flight_uploads`,
    /// which is the source; the write session refreshes it at every submit.
    slots: usize,
    /// Whether data packs are filled on the heap (not staged); for the
    /// ceiling only.
    heap_data_packs: bool,
    /// `P`; 0 until `begin_pipeline`.
    pipeline_cap: usize,
    /// `B`, the largest packed chunk; for the ceiling only.
    max_blob: usize,
    /// `T`, the current data-pack target.
    pack_target: usize,
    /// `Tt`, the tree-pack target; for the ceiling only.
    tree_target: usize,
    pipeline: usize,
    open_data: usize,
    open_tree: usize,
    sealed: usize,
    poisoned: bool,
    peak_pipeline: usize,
    peak_open: usize,
    peak_sealed: usize,
    peak_total: usize,
}

impl State {
    /// Pipeline capacity lent to sealed packs.
    fn lent(&self) -> usize {
        match self.pack_policy {
            PackPolicy::LendHalf => {
                self.sealed
                    .saturating_sub(self.slots.saturating_mul(self.pack_target))
                    / 2
            }
            PackPolicy::CapAtTarget => 0,
        }
    }

    fn update_peaks(&mut self) {
        let open = self.open_data.saturating_add(self.open_tree);
        self.peak_pipeline = self.peak_pipeline.max(self.pipeline);
        self.peak_open = self.peak_open.max(open);
        self.peak_sealed = self.peak_sealed.max(self.sealed);
        let total = self
            .pipeline
            .saturating_add(open)
            .saturating_add(self.sealed);
        self.peak_total = self.peak_total.max(total);
    }

    /// Largest heap pack of `target`: header, the target, one blob of `B`
    /// with its length prefix past it, and the trailer frame.
    fn pack_bound(&self, target: usize) -> usize {
        PACK_HEADER_SIZE
            .saturating_add(target)
            .saturating_add(4)
            .saturating_add(self.max_blob)
            .saturating_add(trailer_frame_len_upper(MAX_BLOBS_PER_PACK))
    }

    /// Upper bound of pipeline + open + sealed under the policy, assuming
    /// no blob exceeds `B` (an item-stream chunk can, with a small chunker
    /// or a very large entry; the figure is reported, never enforced):
    ///
    /// - LendHalf: `P + (W + H) × data_pack + open_data + tree_pack`: the
    ///   slot bound admits `W + H` sealed packs of either type.
    /// - CapAtTarget: `P + max(T, largest sealed pack) + open_data +
    ///   tree_pack`: sealed packs plus the incoming one fit `T`, or one
    ///   pack of any size is alone.
    ///
    /// `open_data` is a data pack being filled on the heap (0 when data
    /// packs are staged). The floor of the pipeline admits one entry of up
    /// to `P` whatever is lent, so the pipeline term is `P` either way.
    fn ceiling(&self) -> usize {
        let data_pack = self.pack_bound(self.pack_target);
        let tree_pack = self.pack_bound(self.tree_target);
        let open_data = if self.heap_data_packs { data_pack } else { 0 };
        let sealed = match self.pack_policy {
            PackPolicy::LendHalf => self
                .slots
                .saturating_add(UPLOAD_QUEUE_HEADROOM)
                .saturating_mul(data_pack.max(tree_pack)),
            PackPolicy::CapAtTarget => {
                let largest = if self.heap_data_packs {
                    data_pack.max(tree_pack)
                } else {
                    tree_pack
                };
                self.pack_target.max(largest)
            }
        };
        self.pipeline_cap
            .saturating_add(sealed)
            .saturating_add(open_data)
            .saturating_add(tree_pack)
    }
}

impl MemoryBudget {
    /// A budget for a write session with `slots` upload slots and these
    /// pack targets. The policy starts as [`PackPolicy::LendHalf`], which is
    /// inert without a pipeline; `begin_write_session` sets the session's.
    pub(crate) fn new(slots: usize, pack_target: usize, tree_target: usize) -> Arc<Self> {
        Arc::new(Self {
            state: Mutex::new(State {
                pack_policy: PackPolicy::LendHalf,
                slots,
                heap_data_packs: true,
                pipeline_cap: 0,
                max_blob: 0,
                pack_target,
                tree_target,
                pipeline: 0,
                open_data: 0,
                open_tree: 0,
                sealed: 0,
                poisoned: false,
                peak_pipeline: 0,
                peak_open: 0,
                peak_sealed: 0,
                peak_total: 0,
            }),
            changed: Condvar::new(),
        })
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, State> {
        self.state.lock().expect("MemoryBudget mutex not poisoned")
    }

    /// Set the session's policy, and whether its data packs are filled on
    /// the heap (`!stream_data_packs`; the ceiling's open data pack).
    pub(crate) fn configure(&self, pack_policy: PackPolicy, heap_data_packs: bool) {
        let mut st = self.lock();
        st.pack_policy = pack_policy;
        st.heap_data_packs = heap_data_packs;
        self.changed.notify_all();
    }

    #[cfg(test)]
    pub(crate) fn pack_policy(&self) -> PackPolicy {
        self.lock().pack_policy
    }

    /// Refresh the upload slot count from its source
    /// (`UploadQueue::max_in_flight_uploads`).
    pub(crate) fn set_slots(&self, slots: usize) {
        let mut st = self.lock();
        if st.slots != slots {
            st.slots = slots;
            self.changed.notify_all();
        }
    }

    /// Set `P` and `B` for one pipeline run; clears the poison of an
    /// earlier run.
    pub(crate) fn begin_pipeline(&self, pipeline_cap: usize, max_blob: usize) {
        let mut st = self.lock();
        debug_assert_eq!(st.pipeline, 0, "pipeline bytes held at pipeline start");
        st.pipeline_cap = pipeline_cap;
        st.max_blob = max_blob;
        st.poisoned = false;
    }

    /// Walk thread: block until `n` pipeline bytes may be held, then hold
    /// them. Returns the granted bytes: `n` clamped to `P`, so an entry
    /// larger than the pipeline still proceeds (it holds all of it and runs
    /// alone). Granted iff nothing is held (the floor: lending throttles the
    /// walk, never stalls it) or `pipeline + n <= P - lent`. Err once
    /// poisoned.
    pub(crate) fn acquire_pipeline(&self, n: usize) -> Result<usize> {
        let mut st = self.lock();
        let n = n.min(st.pipeline_cap);
        loop {
            if st.poisoned {
                return Err(VykarError::Other("pipeline budget poisoned".into()));
            }
            let room = st.pipeline_cap.saturating_sub(st.lent());
            if st.pipeline == 0 || st.pipeline.saturating_add(n) <= room {
                st.pipeline += n;
                st.update_peaks();
                return Ok(n);
            }
            st = self
                .changed
                .wait(st)
                .expect("MemoryBudget condvar wait not poisoned");
        }
    }

    /// Return `n` pipeline bytes and wake the walk.
    pub(crate) fn release_pipeline(&self, n: usize) {
        let mut st = self.lock();
        debug_assert!(n <= st.pipeline, "released {n} > held {}", st.pipeline);
        st.pipeline = st.pipeline.saturating_sub(n);
        self.changed.notify_all();
    }

    /// Fail every current and future `acquire_pipeline`. The pack side
    /// never fails: an aborted backup still closes and uploads its packs.
    pub(crate) fn poison(&self) {
        let mut st = self.lock();
        st.poisoned = true;
        self.changed.notify_all();
    }

    /// Consumer: the heap bytes of the open pack of `pack_type`, after every
    /// append and at close (0).
    pub(crate) fn set_open(&self, pack_type: PackType, heap_bytes: usize) {
        let mut st = self.lock();
        match pack_type {
            PackType::Data => st.open_data = heap_bytes,
            PackType::Tree => st.open_tree = heap_bytes,
        }
        st.update_peaks();
    }

    /// Consumer: the data-pack target changed.
    pub(crate) fn set_pack_target(&self, target: usize) {
        let mut st = self.lock();
        st.pack_target = target;
        self.changed.notify_all();
    }

    /// Consumer, at submit: charge a closed heap pack of `bytes`. The
    /// returned guard releases it once, when dropped.
    pub(crate) fn hold_sealed(self: &Arc<Self>, bytes: usize) -> HeldBytes {
        if bytes == 0 {
            return HeldBytes::none();
        }
        let mut st = self.lock();
        st.sealed = st.sealed.saturating_add(bytes);
        st.update_peaks();
        drop(st);
        HeldBytes {
            budget: Some(Arc::clone(self)),
            bytes,
        }
    }

    fn release_sealed(&self, bytes: usize) {
        let mut st = self.lock();
        debug_assert!(
            bytes <= st.sealed,
            "released {bytes} > sealed {}",
            st.sealed
        );
        st.sealed = st.sealed.saturating_sub(bytes);
        self.changed.notify_all();
    }

    /// Whether a pack of `incoming` heap bytes may close now. CapAtTarget:
    /// sealed + incoming fit the data-pack target. LendHalf: always.
    pub(crate) fn sealed_room(&self, incoming: usize) -> bool {
        let st = self.lock();
        match st.pack_policy {
            PackPolicy::LendHalf => true,
            PackPolicy::CapAtTarget => st.sealed.saturating_add(incoming) <= st.pack_target,
        }
    }

    pub(crate) fn snapshot(&self) -> MemorySnapshot {
        let st = self.lock();
        MemorySnapshot {
            pipeline: st.pipeline,
            open_packs: st.open_data.saturating_add(st.open_tree),
            sealed_packs: st.sealed,
            pipeline_cap: st.pipeline_cap,
            pack_target: st.pack_target,
            ceiling: st.ceiling(),
            peak_pipeline: st.peak_pipeline,
            peak_open_packs: st.peak_open,
            peak_sealed_packs: st.peak_sealed,
            peak_total: st.peak_total,
        }
    }

    /// Upper bound of pipeline + open + sealed under the policy, with the
    /// current targets; see `State::ceiling`.
    #[cfg(test)]
    pub(crate) fn ceiling(&self) -> usize {
        self.lock().ceiling()
    }

    #[cfg(test)]
    pub(crate) fn is_poisoned(&self) -> bool {
        self.lock().poisoned
    }

    /// Pipeline capacity currently lent to sealed packs.
    #[cfg(test)]
    pub(crate) fn lent(&self) -> usize {
        self.lock().lent()
    }

    /// A budget with only a pipeline account of `cap` (pipeline tests).
    #[cfg(test)]
    pub(crate) fn for_pipeline(cap: usize) -> Arc<Self> {
        let budget = Self::new(1, 0, 0);
        budget.begin_pipeline(cap, 0);
        budget
    }
}

/// Heap bytes of one closed pack, released into the budget when dropped,
/// on whichever thread drops it (an upload thread, or a backend's).
pub(crate) struct HeldBytes {
    budget: Option<Arc<MemoryBudget>>,
    bytes: usize,
}

impl HeldBytes {
    /// Holds nothing (a staged pack, tests).
    pub(crate) fn none() -> Self {
        Self {
            budget: None,
            bytes: 0,
        }
    }

    /// Release the bytes now; the same as dropping the guard.
    pub(crate) fn release(self) {}
}

impl Drop for HeldBytes {
    fn drop(&mut self) {
        if let Some(budget) = self.budget.take() {
            budget.release_sealed(self.bytes);
        }
    }
}

/// Pipeline bytes held by one entry; released on drop unless defused
/// (worker failure safety).
///
/// [`Self::defuse`] transfers the bytes to the `ProcessedEntry`; the
/// consumer then releases them explicitly.
pub(crate) struct PipelineGuard<'a> {
    budget: &'a MemoryBudget,
    bytes: usize,
}

impl<'a> PipelineGuard<'a> {
    /// Acquire `n` bytes, returning a guard that releases them on drop
    /// unless defused.
    #[cfg(test)] // Production code uses from_pre_acquired.
    pub(crate) fn new(budget: &'a MemoryBudget, n: usize) -> Result<Self> {
        let bytes = budget.acquire_pipeline(n)?;
        Ok(Self { budget, bytes })
    }

    /// Wrap bytes the walk thread acquired before dispatch (no acquire), so
    /// they are released if the worker `?`-bails.
    pub(crate) fn from_pre_acquired(budget: &'a MemoryBudget, bytes: usize) -> Self {
        Self { budget, bytes }
    }

    /// Consume the guard without releasing the bytes; returns their count
    /// for `ProcessedEntry::ProcessedFile.acquired_bytes`.
    pub(crate) fn defuse(self) -> usize {
        let bytes = self.bytes;
        mem::forget(self);
        bytes
    }
}

impl Drop for PipelineGuard<'_> {
    fn drop(&mut self) {
        if self.bytes > 0 {
            self.budget.release_pipeline(self.bytes);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::time::Duration;

    /// Spawn `acquire_pipeline(n)` and give it time to block.
    fn blocked(b: &Arc<MemoryBudget>, n: usize) -> std::thread::JoinHandle<usize> {
        let b2 = Arc::clone(b);
        let h = std::thread::spawn(move || b2.acquire_pipeline(n).unwrap());
        std::thread::sleep(Duration::from_millis(50));
        h
    }

    #[test]
    fn acquire_release() {
        let budget = MemoryBudget::for_pipeline(1024);
        budget.acquire_pipeline(512).unwrap();
        budget.acquire_pipeline(512).unwrap();
        // Exhausted: release and re-acquire.
        budget.release_pipeline(1024);
        budget.acquire_pipeline(1024).unwrap();
        budget.release_pipeline(1024);
        assert_eq!(budget.snapshot().pipeline, 0);
    }

    #[test]
    fn blocks_and_unblocks() {
        let budget = MemoryBudget::for_pipeline(100);
        budget.acquire_pipeline(100).unwrap();

        let acquired = Arc::new(AtomicBool::new(false));
        let acquired2 = Arc::clone(&acquired);
        let budget2 = Arc::clone(&budget);
        let handle = std::thread::spawn(move || {
            budget2.acquire_pipeline(50).unwrap();
            acquired2.store(true, Ordering::SeqCst);
            budget2.release_pipeline(50);
        });

        std::thread::sleep(Duration::from_millis(50));
        assert!(!acquired.load(Ordering::SeqCst), "should be blocked");
        budget.release_pipeline(100);
        handle.join().unwrap();
        assert!(acquired.load(Ordering::SeqCst), "should have acquired");
    }

    #[test]
    fn oversized_request_clamps() {
        // A request above the capacity is clamped to it instead of waiting
        // forever.
        let budget = MemoryBudget::for_pipeline(64);
        assert_eq!(budget.acquire_pipeline(128).unwrap(), 64);
        budget.release_pipeline(64);
        assert_eq!(budget.snapshot().pipeline, 0);
    }

    #[test]
    fn poison_unblocks() {
        let budget = MemoryBudget::for_pipeline(100);
        budget.acquire_pipeline(100).unwrap();

        let budget2 = Arc::clone(&budget);
        let handle = std::thread::spawn(move || {
            assert!(budget2.acquire_pipeline(50).is_err(), "fails after poison");
        });
        std::thread::sleep(Duration::from_millis(50));
        budget.poison();
        handle.join().unwrap();

        let err = budget.acquire_pipeline(1).unwrap_err();
        assert_eq!(err.to_string(), "pipeline budget poisoned");
    }

    #[test]
    fn concurrent_stress() {
        let budget = MemoryBudget::for_pipeline(1000);
        let handles: Vec<_> = (0..8)
            .map(|_| {
                let b = Arc::clone(&budget);
                std::thread::spawn(move || {
                    for _ in 0..100 {
                        b.acquire_pipeline(100).unwrap();
                        std::thread::yield_now();
                        b.release_pipeline(100);
                    }
                })
            })
            .collect();
        for h in handles {
            h.join().unwrap();
        }
        assert_eq!(budget.snapshot().pipeline, 0);
        budget.acquire_pipeline(1000).unwrap();
        budget.release_pipeline(1000);
    }

    #[test]
    fn enforces_cap() {
        let cap = 500usize;
        let budget = MemoryBudget::for_pipeline(cap);
        let in_flight = Arc::new(AtomicUsize::new(0));
        let peak = Arc::new(AtomicUsize::new(0));
        let handles: Vec<_> = (0..8)
            .map(|_| {
                let b = Arc::clone(&budget);
                let inf = Arc::clone(&in_flight);
                let pk = Arc::clone(&peak);
                std::thread::spawn(move || {
                    for _ in 0..50 {
                        let chunk = 100;
                        b.acquire_pipeline(chunk).unwrap();
                        let current = inf.fetch_add(chunk, Ordering::SeqCst) + chunk;
                        pk.fetch_max(current, Ordering::Relaxed);
                        std::thread::yield_now();
                        inf.fetch_sub(chunk, Ordering::SeqCst);
                        b.release_pipeline(chunk);
                    }
                })
            })
            .collect();
        for h in handles {
            h.join().unwrap();
        }
        let peak = peak.load(Ordering::SeqCst);
        assert!(peak <= cap, "peak {peak} exceeded cap {cap}");
        assert!(budget.snapshot().peak_pipeline <= cap);
    }

    /// LendHalf, W = 1, T = 100, P = 100: sealed packs beyond the slot lend
    /// half their bytes; an upload dropping its charge wakes the walk with
    /// no consumer call; the floor admits once the pipeline is empty.
    #[test]
    fn lending_lowers_walk_share() {
        let budget = MemoryBudget::new(1, 100, 10);
        budget.begin_pipeline(100, 0);
        let pack_a = budget.hold_sealed(100);
        let pack_b = budget.hold_sealed(60);
        assert_eq!(budget.lent(), 30);
        // Nothing held: the floor grants even above P - lent.
        assert_eq!(budget.acquire_pipeline(50).unwrap(), 50);
        // 50 held, room 70: 30 more must wait.
        let waiter = blocked(&budget, 30);
        assert!(!waiter.is_finished(), "lending must hold the request back");
        // The upload frees 60: lent 0, the waiter wakes on its own.
        drop(pack_b);
        assert_eq!(budget.lent(), 0);
        assert_eq!(waiter.join().unwrap(), 30);

        // Lend more than P: 80 held, blocked until everything is released
        // (floor), not until the packs are uploaded.
        let pack_c = budget.hold_sealed(400);
        assert!(budget.lent() >= 100);
        let waiter = blocked(&budget, 20);
        assert!(!waiter.is_finished());
        budget.release_pipeline(30);
        std::thread::sleep(Duration::from_millis(20));
        assert!(!waiter.is_finished(), "still holding 50");
        budget.release_pipeline(50);
        assert_eq!(waiter.join().unwrap(), 20);
        budget.release_pipeline(20);
        drop((pack_a, pack_c));
        let snap = budget.snapshot();
        assert_eq!((snap.pipeline, snap.sealed_packs), (0, 0));
        assert_eq!(budget.acquire_pipeline(100).unwrap(), 100);
    }

    /// W = 2: two packs of T fill the slots (lent 0), four lend T, one
    /// released leaves T/2; CapAtTarget never lends.
    #[test]
    fn lending_counts_sealed_beyond_the_slots() {
        let t = 1000;
        let budget = MemoryBudget::new(2, t, 10);
        let p1 = budget.hold_sealed(t);
        let p2 = budget.hold_sealed(t);
        assert_eq!(budget.lent(), 0);
        let p3 = budget.hold_sealed(t);
        let p4 = budget.hold_sealed(t);
        assert_eq!(budget.lent(), t);
        drop(p1);
        assert_eq!(budget.lent(), t / 2);
        // The slot count follows its source.
        budget.set_slots(1);
        assert_eq!(budget.lent(), t);
        budget.configure(PackPolicy::CapAtTarget, true);
        assert_eq!(budget.lent(), 0);
        drop((p2, p3, p4));
        assert_eq!(budget.snapshot().sealed_packs, 0);
    }

    #[test]
    fn sealed_room_caps_at_target_only_under_cap_at_target() {
        let budget = MemoryBudget::new(2, 100, 10);
        let held = budget.hold_sealed(80);
        assert!(budget.sealed_room(1000), "LendHalf never caps");
        budget.configure(PackPolicy::CapAtTarget, false);
        assert!(budget.sealed_room(20));
        assert!(!budget.sealed_room(21));
        // A larger target makes room.
        budget.set_pack_target(200);
        assert!(budget.sealed_room(120));
        drop(held);
        assert!(budget.sealed_room(200));
    }

    #[test]
    fn ceiling_matches_formula() {
        const MIB: usize = 1024 * 1024;
        let (p, t, tt, b) = (256 * MIB, 32 * MIB, 4 * MIB, 8 * MIB + 64 * 1024);
        let pack = |target: usize| {
            PACK_HEADER_SIZE + target + 4 + b + trailer_frame_len_upper(MAX_BLOBS_PER_PACK)
        };
        let budget = MemoryBudget::new(2, t, tt);
        budget.begin_pipeline(p, b);
        let h = UPLOAD_QUEUE_HEADROOM;
        // Remote: W + H sealed packs plus the open data and tree packs.
        assert_eq!(budget.ceiling(), p + (2 + h + 1) * pack(t) + pack(tt));
        // The target only grows; the ceiling follows it.
        budget.set_pack_target(2 * t);
        assert_eq!(budget.ceiling(), p + (2 + h + 1) * pack(2 * t) + pack(tt));

        // Local, staged data packs: one oversize tree pack or T of sealed
        // packs, no open data pack.
        budget.set_pack_target(t);
        budget.configure(PackPolicy::CapAtTarget, false);
        assert_eq!(budget.ceiling(), p + t.max(pack(tt)) + pack(tt));
        // Local, heap data packs (a rate-limited repository): the open data
        // pack, and a sealed one of up to its bound.
        budget.configure(PackPolicy::CapAtTarget, true);
        assert_eq!(budget.ceiling(), p + pack(t) + pack(t) + pack(tt));
        assert_eq!(budget.snapshot().ceiling, budget.ceiling());
    }

    #[test]
    fn peaks_track_sum() {
        let budget = MemoryBudget::new(1, 100, 10);
        budget.begin_pipeline(100, 0);
        budget.acquire_pipeline(40).unwrap();
        budget.set_open(PackType::Data, 30);
        budget.set_open(PackType::Tree, 5);
        let held = budget.hold_sealed(20);
        budget.release_pipeline(40);
        budget.set_open(PackType::Data, 0);
        drop(held);
        let s = budget.snapshot();
        assert_eq!((s.pipeline, s.open_packs, s.sealed_packs), (0, 5, 0));
        assert_eq!(s.peak_pipeline, 40);
        assert_eq!(s.peak_open_packs, 35);
        assert_eq!(s.peak_sealed_packs, 20);
        assert_eq!(s.peak_total, 40 + 35 + 20);
        assert_eq!(s.pipeline_cap, 100);
        assert_eq!(s.pack_target, 100);
    }

    #[test]
    fn poison_spares_pack_side() {
        let budget = MemoryBudget::new(1, 100, 10);
        budget.begin_pipeline(100, 0);
        budget.configure(PackPolicy::CapAtTarget, true);
        budget.poison();
        budget.set_open(PackType::Data, 10);
        let held = budget.hold_sealed(50);
        assert!(budget.sealed_room(50));
        assert_eq!(budget.snapshot().sealed_packs, 50);
        drop(held);
        assert!(budget.acquire_pipeline(1).is_err());
    }

    #[test]
    fn begin_pipeline_clears_poison() {
        let budget = MemoryBudget::for_pipeline(100);
        budget.poison();
        assert!(budget.acquire_pipeline(1).is_err());
        budget.begin_pipeline(100, 0);
        assert_eq!(budget.acquire_pipeline(1).unwrap(), 1);
        budget.release_pipeline(1);
    }

    #[test]
    fn held_bytes_release_once() {
        let budget = MemoryBudget::new(1, 100, 10);
        let a = budget.hold_sealed(30);
        let b = budget.hold_sealed(20);
        a.release();
        assert_eq!(budget.snapshot().sealed_packs, 20);
        drop(b);
        assert_eq!(budget.snapshot().sealed_packs, 0);
        // A zero charge and `none` hold nothing.
        drop(budget.hold_sealed(0));
        HeldBytes::none().release();
        assert_eq!(budget.snapshot().sealed_packs, 0);
        assert_eq!(budget.snapshot().peak_sealed_packs, 50);
    }

    #[test]
    fn guard_releases_on_drop() {
        let budget = MemoryBudget::for_pipeline(100);
        {
            let _guard = PipelineGuard::new(&budget, 100).unwrap();
        }
        budget.acquire_pipeline(100).unwrap();
        budget.release_pipeline(100);
    }

    #[test]
    fn guard_defuse_transfers() {
        let budget = MemoryBudget::for_pipeline(100);
        let bytes = PipelineGuard::new(&budget, 80).unwrap().defuse();
        assert_eq!(bytes, 80);
        assert_eq!(budget.snapshot().pipeline, 80, "defused bytes stay held");
        budget.release_pipeline(80);
        assert_eq!(budget.snapshot().pipeline, 0);
    }

    #[test]
    fn guard_oversized_request_clamps_to_capacity() {
        let budget = MemoryBudget::for_pipeline(64);
        let bytes = PipelineGuard::new(&budget, 128).unwrap().defuse();
        assert_eq!(bytes, 64, "defused bytes match the grant");
        budget.release_pipeline(bytes);
        budget.acquire_pipeline(64).unwrap();
        budget.release_pipeline(64);
    }
}
