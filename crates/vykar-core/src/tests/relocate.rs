//! Relocation on a pack 404 (04 step 7, "Reader"), against a real delete
//! and compact: a handle that planned its reads before another handle
//! deleted a snapshot and compacted reads the chunks where the latest head
//! stores them, or fails typed when they are gone; a pack lost under an
//! unmoved head keeps today's error. Also `check`'s passes under a head
//! that keeps moving.

use std::path::Path;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use vykar_storage::local_backend::LocalBackend;
use vykar_storage::{InnerBackend, StorageBackend, delegate_storage_backend};
use vykar_types::chunk_id::ChunkId;
use vykar_types::error::{Result, VykarError};
use vykar_types::pack_id::PackId;

use super::mark::Fx;
use crate::commands;
use crate::commands::restore::RestoreOptions;
use crate::repo::control::CancelToken;
use crate::repo::{OpenOptions, Repo};

/// A hook run once, before the first range read of a pack.
type Hook = Box<dyn FnOnce() + Send>;

/// Counts head LISTs and GETs; runs `hook` before the first pack range
/// read of the restore data path (`open_range`).
struct Counting {
    inner: LocalBackend,
    head_lists: Arc<AtomicUsize>,
    head_gets: Arc<AtomicUsize>,
    hook: Mutex<Option<Hook>>,
}

impl InnerBackend for Counting {
    fn inner_backend(&self) -> &dyn StorageBackend {
        &self.inner
    }
}

delegate_storage_backend! {
    for Counting;
    except [get, list, get_range_into, open_range, list_sized, list_meta, server_ops];

    fn server_ops(&self) -> Option<&dyn vykar_storage::ServerOps> {
        None
    }

    fn get(&self, key: &str) -> Result<Option<Vec<u8>>> {
        if key.starts_with("index.h/") {
            self.head_gets.fetch_add(1, Ordering::SeqCst);
        }
        self.inner.get(key)
    }

    fn list(&self, prefix: &str) -> Result<Vec<String>> {
        self.count_list(prefix);
        self.inner.list(prefix)
    }

    fn get_range_into(&self, key: &str, offset: u64, length: u64, buf: &mut Vec<u8>) -> Result<bool> {
        self.inner.get_range_into(key, offset, length, buf)
    }

    fn open_range<'a>(
        &self,
        key: &str,
        offset: u64,
        length: u64,
        buf: &'a mut Vec<u8>,
    ) -> Result<Option<Box<dyn vykar_storage::RangeReader + 'a>>> {
        if key.starts_with("packs/") {
            let hook = self.hook.lock().unwrap().take();
            if let Some(hook) = hook {
                hook();
            }
        }
        vykar_storage::buffered_range(self, key, offset, length, buf)
    }

    fn list_sized(&self, prefix: &str) -> Result<Vec<vykar_storage::ListedObject>> {
        self.count_list(prefix);
        self.inner.list_sized(prefix)
    }

    fn list_meta(&self, prefix: &str) -> Result<Vec<vykar_storage::ListedObject>> {
        self.count_list(prefix);
        self.inner.list_meta(prefix)
    }
}

impl Counting {
    fn count_list(&self, prefix: &str) {
        if prefix.starts_with("index.h/") {
            self.head_lists.fetch_add(1, Ordering::SeqCst);
        }
    }
}

/// `a.bin` and `b.bin`, 64 KiB each of distinct bytes.
fn file_bytes(name: &str) -> Vec<u8> {
    crate::sim::runner::seeded_bytes(name, 64 * 1024)
}

/// `s1` holds a.bin and b.bin, `s2` only a.bin: after `s1` is deleted,
/// a compact at threshold 0 repacks the pack holding a.bin (b.bin is dead)
/// and deletes it.
fn fixture() -> Fx {
    let fx = Fx::new();
    let ab = fx.src("ab");
    std::fs::write(ab.join("a.bin"), file_bytes("a")).unwrap();
    std::fs::write(ab.join("b.bin"), file_bytes("b")).unwrap();
    fx.backup(&ab, "s1");
    let a = fx.src("a");
    std::fs::write(a.join("a.bin"), file_bytes("a")).unwrap();
    fx.backup(&a, "s2");
    fx
}

/// A handle on `fx` with the index loaded, through a counting backend.
fn open_counting(fx: &Fx, hook: Option<Hook>) -> (Repo, Arc<AtomicUsize>, Arc<AtomicUsize>) {
    let (lists, gets) = (Arc::default(), Arc::default());
    let backend = Counting {
        inner: LocalBackend::new(fx.repo_dir.to_str().unwrap()).unwrap(),
        head_lists: Arc::clone(&lists),
        head_gets: Arc::clone(&gets),
        hook: Mutex::new(hook),
    };
    let repo = commands::util::open_repo_on(
        Box::new(backend),
        &commands::util::open_request(&fx.config),
        fx.pass,
        OpenOptions::new().with_index(),
    )
    .unwrap();
    (repo, lists, gets)
}

fn head_reads(lists: &AtomicUsize, gets: &AtomicUsize) -> (usize, usize) {
    (lists.load(Ordering::SeqCst), gets.load(Ordering::SeqCst))
}

/// The chunks of file `path` in snapshot `name`.
fn file_chunks(fx: &Fx, name: &str, path: &str) -> Vec<ChunkId> {
    let items = commands::list::list_snapshot_items(&fx.config, fx.pass, name).unwrap();
    let item = items
        .iter()
        .find(|i| i.path.ends_with(path))
        .unwrap_or_else(|| panic!("no {path} in {name}"));
    assert!(!item.chunks.is_empty());
    item.chunks.iter().map(|c| c.id).collect()
}

/// Their locations in `repo`'s index.
fn located(repo: &Repo, ids: &[ChunkId]) -> Vec<(ChunkId, PackId, u64, u32)> {
    ids.iter()
        .map(|id| {
            let e = repo.chunk_index().get(id).expect("indexed");
            (*id, e.pack_id, e.pack_offset, e.stored_size)
        })
        .collect()
}

/// Delete `name` and compact, the repository's objects aged past T_pack
/// first so the sweep takes the deleted snapshot's entries (04 step 7).
fn delete_and_compact(config: &crate::config::VykarConfig, pass: Option<&str>, name: &str) {
    commands::delete::run(config, pass, &[name], false, &CancelToken::new()).unwrap();
    super::helpers::age_repo_objects(
        Path::new(&config.repository.url),
        &["index.d", "packs"],
        super::helpers::PAST_T_PACK,
    );
    commands::compact::run(config, pass, 0.0, None, false, &CancelToken::new()).unwrap();
}

fn pack_exists(fx: &Fx, pack: &PackId) -> bool {
    fx.repo_dir.join(pack.storage_key()).exists()
}

#[test]
fn coalesced_read_follows_a_repack() {
    let fx = fixture();
    let a = file_chunks(&fx, "s2", "a.bin");
    let (h, lists, gets) = open_counting(&fx, None);
    let planned = located(&h, &a);
    delete_and_compact(&fx.config, fx.pass, "s1");
    assert!(
        planned.iter().any(|(_, p, _, _)| !pack_exists(&fx, p)),
        "the compact did not remove a.bin's pack"
    );

    let before = head_reads(&lists, &gets);
    let mut out = Vec::new();
    h.read_chunks_coalesced_into(&planned, &mut out, &CancelToken::new())
        .unwrap();
    assert_eq!(out, file_bytes("a"));
    let after = head_reads(&lists, &gets);
    assert_eq!((after.0 - before.0, after.1 - before.1), (1, 1));

    // The relocation's index is kept: no head read for the next 404.
    let mut again = Vec::new();
    for id in &a {
        again.extend(h.read_chunk(id).unwrap());
    }
    assert_eq!(again, file_bytes("a"));
    assert_eq!(head_reads(&lists, &gets), after);
}

#[test]
fn coalesced_read_of_a_swept_chunk_fails_typed() {
    let fx = fixture();
    let b = file_chunks(&fx, "s1", "b.bin");
    let (h, _, _) = open_counting(&fx, None);
    let planned = located(&h, &b);
    delete_and_compact(&fx.config, fx.pass, "s1");

    let err = h
        .read_chunks_coalesced_into(&planned, &mut Vec::new(), &CancelToken::new())
        .unwrap_err();
    assert!(
        matches!(err, VykarError::ChunkRemovedDuringRead { chunk_id } if chunk_id == b[0]),
        "{err:?}"
    );
    let err = h.read_chunk(&b[0]).unwrap_err();
    assert!(
        matches!(err, VykarError::ChunkRemovedDuringRead { chunk_id } if chunk_id == b[0]),
        "{err:?}"
    );
}

#[test]
fn a_lost_pack_under_an_unmoved_head_keeps_the_old_error() {
    let fx = fixture();
    let a = file_chunks(&fx, "s2", "a.bin");
    let (h, lists, gets) = open_counting(&fx, None);
    let planned = located(&h, &a);
    let pack = planned[0].1;
    std::fs::remove_file(fx.repo_dir.join(pack.storage_key())).unwrap();

    let before = head_reads(&lists, &gets);
    let err = h
        .read_chunks_coalesced_into(&planned, &mut Vec::new(), &CancelToken::new())
        .unwrap_err();
    assert_eq!(err.to_string(), format!("pack not found: {pack}"));
    let after = head_reads(&lists, &gets);
    assert_eq!((after.0 - before.0, after.1 - before.1), (1, 1));
}

/// Restore `name` on `repo` into a fresh directory under `fx`.
fn restore(
    fx: &Fx,
    repo: &mut Repo,
    name: &str,
    dest: &str,
    continue_on_error: bool,
) -> Result<(commands::restore::RestoreStats, std::path::PathBuf)> {
    let dest = fx.src(dest);
    commands::restore::run_on_repo(
        repo,
        name,
        dest.to_str().unwrap(),
        None,
        RestoreOptions {
            xattrs_enabled: false,
            verify_chunks: true,
            continue_on_error,
        },
        &CancelToken::new(),
    )
    .map(|stats| (stats, dest))
}

fn find_file(dir: &Path, name: &str) -> Option<std::path::PathBuf> {
    for entry in std::fs::read_dir(dir).ok()? {
        let path = entry.ok()?.path();
        if path.is_dir() {
            if let Some(found) = find_file(&path, name) {
                return Some(found);
            }
        } else if path.file_name().is_some_and(|n| n == name) {
            return Some(path);
        }
    }
    None
}

/// A restore planned from the head before a delete and compact (the
/// handle's index is the old one) reads a.bin from its new pack.
#[test]
fn restore_follows_a_repack() {
    for continue_on_error in [false, true] {
        let fx = fixture();
        let (mut h, _, _) = open_counting(&fx, None);
        delete_and_compact(&fx.config, fx.pass, "s1");
        let (stats, dest) = restore(&fx, &mut h, "s2", "out", continue_on_error).unwrap();
        assert_eq!(stats.damaged_total, 0);
        let a = find_file(&dest, "a.bin").expect("a.bin restored");
        assert_eq!(std::fs::read(a).unwrap(), file_bytes("a"));
    }
}

/// A restore of `s1` whose first data read runs "delete s1 + compact"
/// first: strict, it fails with `ChunkRemovedDuringRead` inside
/// `RestoreSourceUnreadable`; in continue mode b.bin is recorded as
/// damaged and a.bin (relocated) restored.
#[test]
fn restore_of_a_removed_snapshot_fails_typed() {
    for continue_on_error in [false, true] {
        let fx = fixture();
        let b = file_chunks(&fx, "s1", "b.bin");
        let (config, pass) = (fx.config.clone(), fx.pass);
        let hook: Hook = Box::new(move || delete_and_compact(&config, pass, "s1"));
        let (mut h, _, _) = open_counting(&fx, Some(hook));
        let result = restore(&fx, &mut h, "s1", "out", continue_on_error);
        if !continue_on_error {
            let err = result.unwrap_err();
            let VykarError::RestoreSourceUnreadable(inner) = &err else {
                panic!("{err:?}");
            };
            assert!(
                matches!(**inner, VykarError::ChunkRemovedDuringRead { chunk_id } if chunk_id == b[0]),
                "{err:?}"
            );
            assert!(err.is_removed_during_read());
            continue;
        }
        let (stats, dest) = result.unwrap();
        assert_eq!(stats.damaged_total, 1, "{:?}", stats.damaged);
        assert!(find_file(&dest, "b.bin").is_none());
        let a = find_file(&dest, "a.bin").expect("a.bin restored");
        assert_eq!(std::fs::read(a).unwrap(), file_bytes("a"));
    }
}

/// Answers `exists` of `pack` with "absent" on its first `missing` calls
/// (one per check pass), and runs `move_head` (what moves the head is the
/// caller's choice) before each of its first `moves` calls: a
/// concurrent compact (or backup) as a check sees it.
struct MissingWhileMoving {
    inner: LocalBackend,
    pack: String,
    missing: usize,
    moves: usize,
    calls: Arc<AtomicUsize>,
    move_head: Box<dyn Fn() + Send + Sync>,
}

impl InnerBackend for MissingWhileMoving {
    fn inner_backend(&self) -> &dyn StorageBackend {
        &self.inner
    }
}

delegate_storage_backend! {
    for MissingWhileMoving;
    except [exists];

    fn exists(&self, key: &str) -> Result<bool> {
        if key == self.pack {
            let n = self.calls.fetch_add(1, Ordering::SeqCst);
            if n < self.moves {
                (self.move_head)();
            }
            if n < self.missing {
                return Ok(false);
            }
        }
        self.inner.exists(key)
    }
}

/// Publish a new head (a fold) without changing the index's contents.
fn move_head(config: &crate::config::VykarConfig, pass: Option<&str>) {
    let mut repo =
        commands::util::open_repo(config, pass, OpenOptions::new(), &CancelToken::new()).unwrap();
    let before = repo.index_generation();
    commands::util::with_maintenance_lock(&mut repo, |repo| {
        repo.load_chunk_index_under_lock()?;
        repo.request_full_index_fold();
        repo.mark_index_dirty();
        repo.save_state()
    })
    .unwrap();
    assert!(repo.index_generation() > before, "the head did not move");
}

/// A check of `fx` at `percent` through a [`MissingWhileMoving`] on
/// `pack`; the result, how far the head the check loaded last moved from
/// the open one, and the `exists` calls of `pack` (one per pass that
/// checked it).
fn check_through(
    fx: &Fx,
    pack: PackId,
    missing: usize,
    moves: usize,
    move_head: Box<dyn Fn() + Send + Sync>,
    percent: u8,
) -> (crate::commands::check::CheckResult, usize, usize) {
    let calls = Arc::new(AtomicUsize::new(0));
    let backend = MissingWhileMoving {
        inner: LocalBackend::new(fx.repo_dir.to_str().unwrap()).unwrap(),
        pack: pack.storage_key(),
        missing,
        moves,
        calls: Arc::clone(&calls),
        move_head,
    };
    let mut repo = commands::util::open_repo_on(
        Box::new(backend),
        &commands::util::open_request(&fx.config),
        fx.pass,
        OpenOptions::new(),
    )
    .unwrap();
    let before = repo.index_generation();
    let result = commands::check::run_on_repo(
        &mut repo,
        &fx.config,
        false,
        true,
        None,
        percent,
        false,
        &CancelToken::new(),
    )
    .unwrap();
    let moved = (repo.index_generation() - before) as usize;
    (result, moved, calls.load(Ordering::SeqCst))
}

/// The pack of a.bin in `fx`.
fn pack_of_a(fx: &Fx) -> PackId {
    let a = file_chunks(fx, "s2", "a.bin");
    fx.open().chunk_index().get(&a[0]).unwrap().pack_id
}

/// Run a compact (threshold 0, nothing to repack): a new sweep generation,
/// so a check that loaded the head before sees entries that may be gone.
fn compact_moves_head(config: &crate::config::VykarConfig, pass: Option<&str>) {
    commands::compact::run(config, pass, 0.0, None, false, &CancelToken::new()).unwrap();
}

/// A check whose pack `a.bin` lives in looks missing, with a compact
/// moving the head, during its first `missing` passes; how far the head the
/// check loaded last, and the latest head, moved from the open one.
fn check_while_moving(missing: usize) -> (crate::commands::check::CheckResult, usize, usize) {
    let fx = fixture();
    let pack = pack_of_a(&fx);
    let (config, pass) = (fx.config.clone(), fx.pass);
    let mover = Box::new(move || compact_moves_head(&config, pass));
    let before = fx.open().index_generation();
    let (result, moved, _) = check_through(&fx, pack, missing, missing, mover, 100);
    let latest = fx.open().index_generation();
    (result, moved, (latest - before) as usize)
}

/// A3: findings while the head moves are checked again, pass after pass:
/// the third pass is clean, so the check is.
#[test]
fn check_passes_again_while_the_head_moves() {
    let (result, moved, latest) = check_while_moving(2);
    assert!(result.errors.is_empty(), "{:?}", result.errors);
    assert!(moved > 0);
    assert_eq!(
        moved, latest,
        "the last pass loaded the head the second compact wrote"
    );
}

/// A3: a head that moves during every pass ends the check at the bound,
/// with its findings and the reason in words.
#[test]
fn check_gives_up_after_three_passes_of_a_moving_head() {
    let (result, _, _) = check_while_moving(3);
    let last = result.errors.last().expect("findings");
    assert!(
        last.message
            .contains("index was rewritten during every pass"),
        "{:?}",
        result.errors
    );
    assert!(result.errors.len() >= 2, "{:?}", result.errors);
}

/// A7: a chunk missing from an index loaded at a head newer than the open
/// one, of a snapshot no longer stored, is `ChunkRemovedDuringRead`; while
/// the snapshot is stored, or under the open head, it stays damage.
#[test]
fn chunk_not_in_index_of_a_removed_snapshot_is_explained() {
    let fx = fixture();
    let b = file_chunks(&fx, "s1", "b.bin")[0];
    let (mut h, _, _) = open_counting(&fx, None);
    let gone = commands::list::ReadOrigin::of(&h, "s1").unwrap();
    let kept = commands::list::ReadOrigin::of(&h, "s2").unwrap();
    let missing = || VykarError::ChunkNotInIndex(b);
    assert!(matches!(
        gone.explain(&h, missing()),
        VykarError::ChunkNotInIndex(_)
    ));

    delete_and_compact(&fx.config, fx.pass, "s1");
    // Under the open head the error stays, whatever storage holds now.
    assert!(matches!(
        gone.explain(&h, missing()),
        VykarError::ChunkNotInIndex(_)
    ));
    h.load_chunk_index_uncached().unwrap();
    assert!(matches!(
        gone.explain(&h, missing()),
        VykarError::ChunkRemovedDuringRead { chunk_id } if chunk_id == b
    ));
    let wrapped = gone.explain(&h, VykarError::RestoreSourceUnreadable(Box::new(missing())));
    assert!(wrapped.is_removed_during_read(), "{wrapped:?}");
    assert!(matches!(
        kept.explain(&h, missing()),
        VykarError::ChunkNotInIndex(_)
    ));
}

/// F2: a head moved only by a backup (backup segments add, never remove)
/// does not make check run again: one pass, its findings as they are.
#[test]
fn check_runs_once_when_a_backup_moves_the_head() {
    let fx = fixture();
    let pack = pack_of_a(&fx);
    let src = fx.src("c");
    std::fs::write(src.join("c.bin"), file_bytes("c")).unwrap();
    let (config, pass) = (fx.config.clone(), fx.pass);
    let mover = Box::new(move || {
        super::helpers::backup_single_source_with(&config, pass, &src, "src", "s3");
    });
    let (result, _, calls) = check_through(&fx, pack, 1, 1, mover, 100);
    assert_eq!(calls, 1, "one pass checked the pack");
    assert!(!result.errors.is_empty(), "the finding is reported");
    assert!(
        result
            .errors
            .iter()
            .all(|e| !e.message.contains("index was rewritten during every pass")),
        "{:?}",
        result.errors
    );
    assert!(
        commands::list::list_snapshot_items(&fx.config, fx.pass, "s3").is_ok(),
        "the backup committed"
    );
}

/// F2: a head moved by a compact (a new sweep generation) makes check run
/// again; the second pass is clean.
#[test]
fn check_runs_again_when_a_compact_moves_the_head() {
    let fx = fixture();
    let pack = pack_of_a(&fx);
    let (config, pass) = (fx.config.clone(), fx.pass);
    let mover = Box::new(move || compact_moves_head(&config, pass));
    let (result, moved, calls) = check_through(&fx, pack, 1, 1, mover, 100);
    assert!(moved > 0, "the compact did not move the head");
    assert_eq!(calls, 2, "a second pass checked the pack");
    assert!(result.errors.is_empty(), "{:?}", result.errors);
}

/// F1: a sampled check draws its sample once. Real damage in a sampled-in
/// pack, the head moved during the first pass: the second pass checks the
/// same pack and the finding stays (a second draw is pinned to another
/// pack, so a check that drew again would come back clean).
#[test]
fn a_sampled_check_keeps_its_findings_when_it_runs_again() {
    use crate::commands::check::{PINNED_SAMPLE, SampleOf};

    let fx = fixture();
    let damaged = pack_of_a(&fx);
    let mut packs: Vec<PackId> = {
        let repo = fx.open();
        let mut seen = std::collections::HashSet::new();
        repo.chunk_index()
            .iter()
            .map(|(_, e)| e.pack_id)
            .filter(|p| seen.insert(*p))
            .collect()
    };
    packs.sort_by_cached_key(PackId::storage_key);
    assert!(packs.len() >= 2, "the fixture has one pack");
    let at = packs.iter().position(|p| *p == damaged).unwrap();
    std::fs::remove_file(fx.repo_dir.join(damaged.storage_key())).unwrap();

    let pack_draws = Arc::new(AtomicUsize::new(0));
    let draws = Arc::clone(&pack_draws);
    PINNED_SAMPLE.set(Some(Box::new(move |of, total, keep| match of {
        SampleOf::Packs => {
            assert_eq!(keep, 1);
            if draws.fetch_add(1, Ordering::SeqCst) == 0 {
                vec![at]
            } else {
                vec![(at + 1) % total]
            }
        }
        SampleOf::Snapshots => (0..keep).collect(),
    })));
    // The fixture's two backup segments fold into a base: a new base, so
    // the check runs again (a compact would refuse over the lost pack).
    let (config, pass) = (fx.config.clone(), fx.pass);
    let mover = Box::new(move || move_head(&config, pass));
    let (result, moved, calls) = check_through(&fx, damaged, 0, 1, mover, 1);
    PINNED_SAMPLE.set(None);

    assert_eq!(moved, 1, "the second pass loaded the moved head");
    assert_eq!(calls, 2, "both passes checked the damaged pack");
    assert_eq!(
        pack_draws.load(Ordering::SeqCst),
        1,
        "the sample was drawn once"
    );
    assert!(
        result
            .errors
            .iter()
            .any(|e| e.message.contains(&damaged.to_string())),
        "{:?}",
        result.errors
    );
}
