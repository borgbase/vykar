//! `with_exclusive_session`'s epilogue (10-single-protocol step 8; the
//! lock's epilogue before): the action's result survives a failed
//! deregistration, a write session the action began is flushed before the
//! session ends, the handle's storage and session state are restored, and a
//! refused registration runs nothing.

use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use crate::commands::util::{with_exclusive_session, with_exclusive_session_as};
use crate::compress::Compression;
use crate::repo::ChunkerParams;
use crate::repo::InitOptions;
use crate::repo::pack::PackType;
use crate::repo::sessions;
use crate::repo::{EncryptionMode, Fence, Repo, WriteSessionOptions};
use vykar_storage::StorageBackend;
use vykar_types::error::{Result, VykarError};

#[derive(Clone)]
struct MarkerBackend {
    state: Arc<MarkerBackendState>,
}

struct MarkerBackendState {
    data: Mutex<HashMap<String, Vec<u8>>>,
    deregister_calls: AtomicUsize,
    fail_deregister: bool,
    /// The next this many PUTs of a session journal fail.
    fail_journal_puts: AtomicUsize,
}

impl MarkerBackend {
    fn new(fail_deregister: bool) -> Self {
        Self {
            state: Arc::new(MarkerBackendState {
                data: Mutex::new(HashMap::new()),
                deregister_calls: AtomicUsize::new(0),
                fail_deregister,
                fail_journal_puts: AtomicUsize::new(0),
            }),
        }
    }

    fn deregister_calls(&self) -> usize {
        self.state.deregister_calls.load(Ordering::SeqCst)
    }
}

impl StorageBackend for MarkerBackend {
    fn get(&self, key: &str) -> Result<Option<Vec<u8>>> {
        let map = self.state.data.lock().unwrap();
        Ok(map.get(key).cloned())
    }

    fn put(&self, key: &str, data: &[u8]) -> Result<()> {
        if key.ends_with(".index")
            && self
                .state
                .fail_journal_puts
                .try_update(Ordering::SeqCst, Ordering::SeqCst, |n| n.checked_sub(1))
                .is_ok()
        {
            return Err(VykarError::Other("injected journal PUT failure".into()));
        }
        let mut map = self.state.data.lock().unwrap();
        map.insert(key.to_string(), data.to_vec());
        Ok(())
    }

    fn delete(&self, key: &str) -> Result<()> {
        // Track deregistration via deletes of exclusive session markers.
        if key.starts_with("sessions/x-") && key.ends_with(".json") {
            self.state.deregister_calls.fetch_add(1, Ordering::SeqCst);
            if self.state.fail_deregister {
                return Err(VykarError::Other("forced release failure".into()));
            }
        }
        let mut map = self.state.data.lock().unwrap();
        map.remove(key);
        Ok(())
    }

    fn exists(&self, key: &str) -> Result<bool> {
        let map = self.state.data.lock().unwrap();
        Ok(map.contains_key(key))
    }

    fn list(&self, prefix: &str) -> Result<Vec<String>> {
        let map = self.state.data.lock().unwrap();
        Ok(map
            .keys()
            .filter(|k| k.starts_with(prefix))
            .cloned()
            .collect())
    }

    fn get_range(&self, key: &str, offset: u64, length: u64) -> Result<Option<Vec<u8>>> {
        let map = self.state.data.lock().unwrap();
        let Some(data) = map.get(key) else {
            return Ok(None);
        };
        let start = offset as usize;
        let end = start.checked_add(length as usize).ok_or_else(|| {
            VykarError::Other(format!(
                "short read on {key} at offset {offset}: offset + length overflows usize"
            ))
        })?;
        if start >= data.len() || end > data.len() {
            return Err(VykarError::Other(format!(
                "short read on {key} at offset {offset}: expected {length} bytes, got {}",
                data.len().saturating_sub(start)
            )));
        }
        Ok(Some(data[start..end].to_vec()))
    }

    fn create_dir(&self, _key: &str) -> Result<()> {
        Ok(())
    }

    fn profile(&self) -> vykar_storage::BackendProfile {
        vykar_storage::BackendProfile {
            local: false,
            staged_put: false,
            request_width: 1,
            transfer_width: 1,
        }
    }

    vykar_storage::unsupported_server_ops!();
}

fn init_repo_with_backend(backend: MarkerBackend) -> Repo {
    Repo::init(
        Box::new(backend),
        EncryptionMode::Aes256Gcm,
        ChunkerParams::default(),
        Some(crate::testing::TEST_PASSPHRASE),
        None,
        None,
    )
    .unwrap()
}

fn small_pack_repo(backend: &MarkerBackend) -> Repo {
    let small_config = InitOptions {
        min_pack_size: 256,
        max_pack_size: 256,
        ..Default::default()
    };
    Repo::init(
        Box::new(backend.clone()),
        EncryptionMode::Aes256Gcm,
        ChunkerParams::default(),
        Some(crate::testing::TEST_PASSPHRASE),
        Some(&small_config),
        None,
    )
    .unwrap()
}

fn exclusive_markers(storage: &dyn StorageBackend) -> Vec<String> {
    storage
        .list("sessions/x-")
        .unwrap()
        .into_iter()
        .filter(|k| k.ends_with(".json"))
        .collect()
}

#[test]
fn the_action_error_is_kept_when_deregistration_also_fails() {
    let backend = MarkerBackend::new(true);
    let mut repo = init_repo_with_backend(backend.clone());

    let result: Result<()> =
        with_exclusive_session(&mut repo, |_repo| Err(VykarError::Other("boom".into())));
    assert!(matches!(result, Err(VykarError::Other(msg)) if msg == "boom"));
    // Three attempts (`deregister_keys`).
    assert_eq!(backend.deregister_calls(), 3);
}

#[test]
fn success_is_kept_when_deregistration_fails() {
    // Post-commit policy: if the action succeeded, a failed deregistration is
    // non-fatal (`tracing::warn!` only): the work is committed. The marker
    // left behind stops blocking 45 minutes after its last refresh, or
    // `vykar break-lock --sessions` removes it.
    let backend = MarkerBackend::new(true);
    let mut repo = init_repo_with_backend(backend.clone());

    let result: Result<()> = with_exclusive_session(&mut repo, |_repo| Ok(()));
    assert!(
        result.is_ok(),
        "a failed deregistration must not fail a successful action"
    );
    assert_eq!(backend.deregister_calls(), 3);
}

#[test]
fn the_action_flushes_its_session_before_deregistration() {
    crate::testutil::init_test_environment();

    let backend = MarkerBackend::new(false);
    let mut repo = small_pack_repo(&backend);

    let result: Result<()> = with_exclusive_session(&mut repo, |repo| {
        // Begin a write session, store enough to trigger a pack flush, then
        // fail: the session's owner flushes it on its error path.
        let mut ws = repo.begin_write_session(&WriteSessionOptions::default());
        ws.store_chunk(repo, &vec![0xABu8; 300], Compression::None, PackType::Data)?;
        ws.flush_on_abort(repo);
        Err(VykarError::Other("simulated failure".into()))
    });

    assert!(result.is_err());
    // flush_on_abort wrote pending_index before the session deregistered.
    assert!(
        repo.storage().exists("sessions/default.index").unwrap(),
        "sessions/default.index should exist after the error path"
    );
    assert_eq!(backend.deregister_calls(), 1, "the session deregistered");
    assert!(exclusive_markers(repo.storage().as_ref()).is_empty());
}

/// A failed `save_session` flushes its session before the error returns:
/// the journal PUT during the session's own flush fails, then the fence
/// refuses the index write, and only the error arm's rewrite of the journal
/// (`flush_on_abort`) leaves the journal behind, under the session ID the
/// write session was given (an exclusive session's for a repair,
/// `sessions/x-<hex>.index`).
#[test]
fn a_failed_save_session_writes_the_journal() {
    crate::testutil::init_test_environment();

    let backend = MarkerBackend::new(false);
    let mut repo = small_pack_repo(&backend);
    let id = format!("x-{:032x}", 7u128);

    let mut ws = repo.begin_write_session(&WriteSessionOptions {
        session_id: id.clone(),
        ..WriteSessionOptions::default()
    });
    ws.store_chunk(&repo, &[0xABu8; 300], Compression::None, PackType::Data)
        .unwrap();
    backend.state.fail_journal_puts.store(1, Ordering::SeqCst);
    let err = repo
        .writer(Fence::marker(Arc::new(|| {
            Err(VykarError::MarkerLostDuringCommit)
        })))
        .save_session(ws)
        .unwrap_err();

    assert!(matches!(err, VykarError::MarkerLostDuringCommit), "{err}");
    assert_eq!(
        backend.state.fail_journal_puts.load(Ordering::SeqCst),
        0,
        "the session's own flush tried the journal first"
    );
    assert!(
        repo.storage()
            .exists(&format!("sessions/{id}.index"))
            .unwrap(),
        "the failed save flushed its session under its ID"
    );
    assert!(!repo.storage().exists("sessions/default.index").unwrap());
}

/// After the call the handle's storage is the raw backend again and the
/// lock state is cleared, whatever the action returned; inside it the
/// storage is the write-checked wrapper and the session is `x-` + 32 hex.
#[test]
fn the_storage_and_session_state_are_restored() {
    let backend = MarkerBackend::new(false);
    let mut repo = init_repo_with_backend(backend);
    let raw = Arc::clone(repo.storage());
    for fail in [false, true] {
        let result: Result<()> = with_exclusive_session_as(&mut repo, None, |w, session| {
            assert!(!Arc::ptr_eq(w.storage(), &raw), "the storage is wrapped");
            let id = session.id();
            assert!(
                id.len() == 34
                    && id.starts_with("x-")
                    && id[2..].bytes().all(|b| b.is_ascii_hexdigit()),
                "{id}"
            );
            assert!(w.live_sessions().is_empty());
            if fail {
                Err(VykarError::Other("boom".into()))
            } else {
                Ok(())
            }
        });
        assert_eq!(result.is_err(), fail);
        assert!(Arc::ptr_eq(repo.storage(), &raw));
        assert!(repo.store_time().is_none());
        assert!(repo.live_sessions().is_empty());
        assert!(exclusive_markers(raw.as_ref()).is_empty());
    }
}

/// A registration that finds another live session refuses
/// `ActiveSessions`, deletes its own marker and runs no action.
#[test]
fn a_refused_registration_runs_nothing_and_leaves_no_marker() {
    let backend = MarkerBackend::new(false);
    let mut repo = init_repo_with_backend(backend);
    let env = repo.host_env().clone();
    let shared = sessions::register_session(repo.storage().as_ref(), "live-backup", &env).unwrap();
    let ran = AtomicUsize::new(0);
    let result: Result<()> = with_exclusive_session(&mut repo, |_| {
        ran.fetch_add(1, Ordering::SeqCst);
        Ok(())
    });
    assert!(
        matches!(result, Err(VykarError::ActiveSessions(_))),
        "{result:?}"
    );
    assert_eq!(ran.load(Ordering::SeqCst), 0);
    assert!(exclusive_markers(repo.storage().as_ref()).is_empty());
    assert!(repo.live_sessions().is_empty());
    sessions::deregister_session(repo.storage().as_ref(), shared.session_id(), &env);
}

/// The exclusive check guards the index write (`fence_blocks_persist_index`
/// of the lock's fence before 10-single-protocol step 8): with its marker
/// removed and no reuse of an earlier EXISTS, `save_state` fails
/// `MarkerLostDuringCommit` and no head is written.
#[test]
fn a_removed_marker_blocks_the_index_write() {
    let backend = MarkerBackend::new(false);
    let mut repo = init_repo_with_backend(backend);
    repo.set_exclusive_check_reuse(std::time::Duration::ZERO);
    let raw = Arc::clone(repo.storage());
    let heads_before = raw.list("index.h/").unwrap();
    let result: Result<()> = with_exclusive_session(&mut repo, |w| {
        for key in exclusive_markers(raw.as_ref()) {
            raw.delete(&key)?;
        }
        // Edit the index so save_state persists it.
        let pack = vykar_types::pack_id::PackId::from_bytes([8; 32]);
        w.chunk_index_mut().add(
            vykar_types::chunk_id::ChunkId::from_bytes([7; 32]),
            10,
            pack,
            0,
        );
        w.chunk_index_mut().set_pack_trailer(pack, 0);
        w.save_state()
    });
    assert!(
        matches!(result, Err(VykarError::MarkerLostDuringCommit)),
        "{result:?}"
    );
    assert_eq!(raw.list("index.h/").unwrap(), heads_before);
}
