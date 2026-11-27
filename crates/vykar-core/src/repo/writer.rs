//! The exclusion a write runs under, as a value ([`Fence`]), and the one
//! handle that writes the repository's shared state ([`RepoWriter`]).

use std::collections::HashSet;
use std::ops::{Deref, DerefMut};
use std::sync::Arc;

use vykar_types::error::Result;
use vykar_types::pack_id::PackId;

use super::Repo;
use super::WriterMode;
use super::index_write::{MaintenanceWrite, OnMoved};
use super::upgrade::{UpgradeOutcome, WriterModeReport};
use super::write_session::WriteSession;
use crate::index::LiveIndex;

/// A fence check: `Ok` while the exclusion still holds.
pub(crate) type FenceFn = Arc<dyn Fn() -> Result<()> + Send + Sync>;

/// The check a write makes before it changes shared state: that the
/// exclusion it runs under still holds. Every persisting path is handed one;
/// there is no "no fence".
#[derive(Clone)]
pub(crate) struct Fence(FenceKind);

#[derive(Clone)]
enum FenceKind {
    /// The repository lock is still this client's (`lock::build_lock_fence`);
    /// no command takes the lock since 10-single-protocol step 8.
    Lock(FenceFn),
    /// A maintenance session or an exclusive session: its marker is present
    /// and its record continuous (`commands::util::maintenance_marker_fence`,
    /// `commands::util::exclusive_write_check`).
    Marker(FenceFn),
    /// A lock-free backup commit: no lock and no marker check here; the
    /// commit machine checks its marker itself (`HeadCheck` at the head
    /// fence, `check_marker_before_snapshot`). Passes.
    LockFreeBackup,
    /// Tests: what a handle without a fence did before fences were typed.
    /// Passes.
    #[cfg(test)]
    Unchecked,
}

impl Fence {
    /// The fence of a held repository lock.
    pub(crate) fn lock(check: FenceFn) -> Self {
        Self(FenceKind::Lock(check))
    }

    /// The fence of a lock-free maintenance run's session marker.
    pub(crate) fn marker(check: FenceFn) -> Self {
        Self(FenceKind::Marker(check))
    }

    /// The fence of a lock-free backup commit: passes (the commit machine
    /// checks its marker itself). Built only by `PendingCommit::begin_commit`.
    pub(crate) fn lock_free_backup() -> Self {
        Self(FenceKind::LockFreeBackup)
    }

    /// Tests: a fence that passes.
    #[cfg(test)]
    pub(crate) fn unchecked() -> Self {
        Self(FenceKind::Unchecked)
    }

    /// Whether this is a held lock's fence. A session run (maintenance
    /// session, backup) checks its fence before the index garbage
    /// collection; a lock holder does not.
    pub(crate) fn is_lock(&self) -> bool {
        matches!(self.0, FenceKind::Lock(_))
    }

    /// Tests: whether this is the lock-free backup's fence.
    #[cfg(test)]
    pub(crate) fn is_lock_free_backup(&self) -> bool {
        matches!(self.0, FenceKind::LockFreeBackup)
    }

    /// `Ok` for a lock-free backup (and the tests' unchecked fence);
    /// otherwise the lock or marker check.
    pub(crate) fn check(&self) -> Result<()> {
        match &self.0 {
            FenceKind::Lock(f) | FenceKind::Marker(f) => f(),
            FenceKind::LockFreeBackup => Ok(()),
            #[cfg(test)]
            FenceKind::Unchecked => Ok(()),
        }
    }
}

/// The one way to write the repository's shared state: a repository
/// borrowed together with the fence of the exclusion it runs under
/// (`commands::util::with_exclusive_session` and its siblings). Reads, and
/// the repository's own `&mut self` methods, go through `Deref` and
/// `DerefMut` to the repository; the persist and index-mutation methods
/// exist only here.
///
/// A method later added to `Repo` under a name this type already
/// has is shadowed when called through a writer.
///
/// The repository itself has no persist, index-mutation or chunk-store
/// method. Each `compile_fail` example below has a compiling control of the
/// same shape.
///
/// ```compile_fail,E0599
/// fn f(r: &mut vykar_core::repo::Repo) {
///     let _ = r.save_state();
/// }
/// ```
///
/// ```compile_fail,E0599
/// fn f(r: &mut vykar_core::repo::Repo) {
///     let _ = r.chunk_index_mut();
/// }
/// ```
///
/// ```
/// fn f(r: &mut vykar_core::repo::Repo) {
///     let _ = r.chunk_index();
/// }
/// ```
///
/// ```compile_fail,E0599
/// use vykar_core::compress::Compression;
/// use vykar_core::repo::pack::PackType;
/// fn f(r: &mut vykar_core::repo::Repo) {
///     let _ = r.store_chunk(b"x", Compression::None, PackType::Data);
/// }
/// ```
///
/// ```
/// use vykar_core::compress::Compression;
/// use vykar_core::repo::pack::PackType;
/// fn f(r: &mut vykar_core::repo::Repo) {
///     let _ = (b"x", Compression::None, PackType::Data, r.chunk_index());
/// }
/// ```
pub struct RepoWriter<'r> {
    repo: &'r mut Repo,
    fence: Fence,
}

impl Deref for RepoWriter<'_> {
    type Target = Repo;

    fn deref(&self) -> &Repo {
        self.repo
    }
}

impl DerefMut for RepoWriter<'_> {
    fn deref_mut(&mut self) -> &mut Repo {
        self.repo
    }
}

impl RepoWriter<'_> {
    /// Persist the dirty index (a maintenance segment over the loaded head)
    /// and the dirty file cache. Only writes components that are dirty.
    pub fn save_state(&mut self) -> Result<()> {
        // An index write leaves the repository's table at the new state.
        if self.repo.index_dirty {
            self.repo.persist_segmented_index(&self.fence)?;
        }
        self.repo.save_file_cache_if_dirty()
    }

    /// Flush `ws`, apply its delta to the index as loaded (the write that
    /// follows refuses a head that moved since), then [`Self::save_state`].
    /// The session is consumed either way: on error its packs are flushed
    /// and its journal written ([`WriteSession::flush_on_abort`]) before
    /// the error returns. The `pending_index` file itself is deleted later
    /// by `clear_pending_index()` from the backup command.
    ///
    /// Refuses, before anything is applied, when a chunk the session stored
    /// or reused has no index entry, or when an upload of the session
    /// failed. The backup commit does not come here: it takes its delta in
    /// the commit machine.
    pub fn save_session(&mut self, mut ws: WriteSession) -> Result<()> {
        let applied = ws.flush_and_take_saved_delta(self.repo).map(|delta| {
            if let Some(delta) = delta.filter(|d| !d.is_empty()) {
                delta.apply_to(&mut self.repo.chunk_index);
                self.repo.index_dirty = true;
            }
        });
        match applied.and_then(|()| self.save_state()) {
            Ok(()) => {
                ws.finished = true;
                Ok(())
            }
            Err(e) => {
                ws.flush_on_abort(self.repo);
                Err(e)
            }
        }
    }

    /// Mutable access to the chunk index. Marks it dirty.
    pub fn chunk_index_mut(&mut self) -> &mut LiveIndex {
        self.repo.index_dirty = true;
        &mut self.repo.chunk_index
    }

    /// Mark the chunk index as needing persistence on the next
    /// [`Self::save_state`].
    pub fn mark_index_dirty(&mut self) {
        self.repo.index_dirty = true;
    }

    /// Rewrite the index into the current layout and switch the repository
    /// to the current format (`Repo::upgrade_index_layout_under`).
    pub fn upgrade_index_layout(&mut self) -> Result<UpgradeOutcome> {
        self.upgrade_index_layout_with(None)
    }

    /// [`Self::upgrade_index_layout`] recording `requested` as the writer
    /// mode (`vykar upgrade --commit-mode`); on a v6 repository a `Some`
    /// switches the mode.
    pub fn upgrade_index_layout_with(
        &mut self,
        requested: Option<WriterMode>,
    ) -> Result<UpgradeOutcome> {
        self.repo.upgrade_index_layout_under(&self.fence, requested)
    }

    /// Switch a v6 repository's writer mode (`Repo::set_writer_mode_under`)
    /// under this writer's fence. The caller holds the maintenance lock and
    /// has refused live sessions: a session that read the old mode must not
    /// commit across the switch.
    pub fn set_writer_mode(&mut self, want: WriterMode) -> Result<WriterModeReport> {
        self.repo.set_writer_mode_under(want, &self.fence)
    }

    /// `Repo::persist_maintenance_under` with this writer's fence.
    pub(crate) fn persist_maintenance(&mut self) -> Result<MaintenanceWrite> {
        self.repo.persist_maintenance_under(&self.fence)
    }

    /// `Repo::maintain_segmented_index_under` with this writer's
    /// fence.
    pub(crate) fn maintain_segmented_index(&mut self, on_moved: OnMoved) -> Result<()> {
        if self.repo.index_dirty {
            self.save_state()?;
        }
        self.repo
            .maintain_segmented_index_under(on_moved, &self.fence)
    }

    /// `Repo::rebase_relocations_under` (it re-applies relocations
    /// to the index, so only a writer may).
    pub(crate) fn rebase_relocations(
        &mut self,
        sources: &HashSet<PackId>,
    ) -> Result<(usize, HashSet<PackId>)> {
        self.repo.rebase_relocations_under(sources)
    }

    /// Tests: `Repo::commit_session_blocking` under this writer's
    /// fence.
    #[cfg(test)]
    pub(crate) fn commit_session_blocking(
        &mut self,
        ws: WriteSession,
        entry: super::manifest::SnapshotEntry,
        packed: Vec<u8>,
        new_file_cache: &mut super::file_cache::FileCache,
        events: &dyn super::control::EventSink<super::control::CommitEvent>,
    ) -> Result<()> {
        self.repo.commit_session_blocking_under(
            &self.fence,
            ws,
            entry,
            packed,
            new_file_cache,
            events,
        )
    }

    /// The fence check before a fenced delete (compact, snapshot removal).
    pub(crate) fn check_fence(&self) -> Result<()> {
        self.fence.check()
    }

    /// Tests: run the rest of the scope under `fence`.
    #[cfg(test)]
    pub(crate) fn replace_fence_for_test(&mut self, fence: Fence) {
        self.fence = fence;
    }
}

impl Repo {
    /// A writer under `fence`. Only the exclusion scopes of
    /// `commands::util`, the backup commit and tests call this.
    pub(crate) fn writer(&mut self, fence: Fence) -> RepoWriter<'_> {
        RepoWriter { repo: self, fence }
    }

    /// Tests: a writer whose fence passes, for writes outside any exclusion.
    #[cfg(test)]
    pub(crate) fn unchecked_writer(&mut self) -> RepoWriter<'_> {
        self.writer(Fence::unchecked())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use vykar_types::error::VykarError;

    fn failing() -> FenceFn {
        Arc::new(|| Err(VykarError::LockExpired("lost".into())))
    }

    #[test]
    fn lock_and_marker_fences_return_their_check_error() {
        for fence in [Fence::lock(failing()), Fence::marker(failing())] {
            assert!(matches!(fence.check(), Err(VykarError::LockExpired(_))));
            assert!(!fence.is_lock_free_backup());
        }
        assert!(Fence::lock(Arc::new(|| Ok(()))).check().is_ok());
    }

    #[test]
    fn the_lock_free_backup_fence_passes() {
        let fence = Fence::lock_free_backup();
        assert!(fence.check().is_ok());
        assert!(fence.is_lock_free_backup());
    }
}
