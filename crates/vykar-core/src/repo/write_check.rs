//! A storage wrapper that runs a check before every write it forwards
//! (10-single-protocol step 6: the v2/v3 upgrade's legacy lock key,
//! `upgrade::LegacyUpgradeLock`; step 8 reuses it for exclusive sessions).
//!
//! Checked: `put`, `delete`, `create_dir`, `put_owned`, `put_pack`,
//! `remove_stale_temp_files`, a staged put's `commit` and
//! `batch_delete().delete_keys`. Not checked: reads, a staged put's
//! `append` (nothing is visible before `commit`), put-if-absent writes
//! through `conditional()` (in the upgrade only the init-time check's probes
//! under `sessions/`) and `server_ops()` (the upgrade uses only its reads).

use std::sync::Arc;

use vykar_storage::{
    BackendProfile, BatchDelete, BatchDeleteScope, ConditionalWrite, InnerBackend, PackData,
    StagedPut, StorageBackend, delegate_storage_backend,
};
use vykar_types::error::Result;

use super::writer::FenceFn;

/// `inner` with `check` run before every write; a failed check forwards
/// nothing and returns its error.
pub(crate) struct WriteChecked {
    inner: Arc<dyn StorageBackend>,
    check: FenceFn,
}

impl WriteChecked {
    pub(crate) fn new(inner: Arc<dyn StorageBackend>, check: FenceFn) -> Self {
        Self { inner, check }
    }
}

impl InnerBackend for WriteChecked {
    fn inner_backend(&self) -> &dyn StorageBackend {
        &*self.inner
    }
}

delegate_storage_backend! {
    for WriteChecked;
    except [put, delete, create_dir, put_owned, put_pack, begin_staged_put,
            remove_stale_temp_files, conditional, batch_delete, profile];

    fn put(&self, key: &str, data: &[u8]) -> Result<()> {
        (self.check)()?;
        self.inner.put(key, data)
    }

    fn delete(&self, key: &str) -> Result<()> {
        (self.check)()?;
        self.inner.delete(key)
    }

    fn create_dir(&self, key: &str) -> Result<()> {
        (self.check)()?;
        self.inner.create_dir(key)
    }

    fn put_owned(&self, key: &str, data: Vec<u8>) -> Result<()> {
        (self.check)()?;
        self.inner.put_owned(key, data)
    }

    // A failed check drops `data`, which fires its release.
    fn put_pack(&self, key: &str, data: PackData) -> Result<()> {
        (self.check)()?;
        self.inner.put_pack(key, data)
    }

    fn begin_staged_put(&self, prefix: &str) -> Result<Option<Box<dyn StagedPut>>> {
        Ok(self.inner.begin_staged_put(prefix)?.map(|inner| {
            Box::new(CheckedStaged {
                inner,
                check: Arc::clone(&self.check),
            }) as Box<dyn StagedPut>
        }))
    }

    fn remove_stale_temp_files(&self, older_than: std::time::Duration) -> Result<usize> {
        (self.check)()?;
        self.inner.remove_stale_temp_files(older_than)
    }

    // Forwarded unchecked: returning `None` would make the init-time check
    // record single-writer. TODO(10-single-protocol step 8): check these
    // too once exclusive sessions use this wrapper beside conditional heads.
    fn conditional(&self) -> Option<&dyn ConditionalWrite> {
        self.inner.conditional()
    }

    fn batch_delete(&self) -> Option<&dyn BatchDelete> {
        self.inner.batch_delete().map(|_| self as &dyn BatchDelete)
    }

    // Staged puts stay what the inner backend offers.
    fn profile(&self) -> BackendProfile {
        self.inner.profile()
    }
}

impl BatchDelete for WriteChecked {
    fn delete_keys(&self, keys: &[String], scope: BatchDeleteScope) -> Result<()> {
        (self.check)()?;
        match self.inner.batch_delete() {
            Some(batch) => batch.delete_keys(keys, scope),
            // The inner backend stopped offering its batch API.
            None => keys.iter().try_for_each(|k| self.inner.delete(k)),
        }
    }
}

/// A staged put whose `commit` (the write that publishes) runs the check.
struct CheckedStaged {
    inner: Box<dyn StagedPut>,
    check: FenceFn,
}

impl StagedPut for CheckedStaged {
    fn append(&mut self, data: &[u8]) -> Result<()> {
        self.inner.append(data)
    }

    fn commit(self: Box<Self>, key: &str) -> Result<()> {
        (self.check)()?;
        self.inner.commit(key)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

    use super::*;
    use crate::testutil::MemoryBackend;
    use vykar_types::error::VykarError;

    /// A memory backend that logs the calls reaching it and offers a
    /// staged put and a batch delete.
    struct Inner {
        mem: MemoryBackend,
        log: Arc<Mutex<Vec<String>>>,
    }

    impl Inner {
        fn note(&self, op: &str, key: &str) {
            self.log.lock().unwrap().push(format!("{op} {key}"));
        }
    }

    struct Staged {
        bytes: Vec<u8>,
        mem: Arc<Inner>,
    }

    impl StagedPut for Staged {
        fn append(&mut self, data: &[u8]) -> Result<()> {
            self.bytes.extend_from_slice(data);
            Ok(())
        }
        fn commit(self: Box<Self>, key: &str) -> Result<()> {
            self.mem.note("commit", key);
            self.mem.mem.put(key, &self.bytes)
        }
    }

    /// The test's handle to the inner backend (the staged put needs an Arc).
    struct Shared(Arc<Inner>);

    impl InnerBackend for Shared {
        fn inner_backend(&self) -> &dyn StorageBackend {
            &self.0.mem
        }
    }

    delegate_storage_backend! {
        for Shared;
        except [put, delete, exists, list, get_range, create_dir, put_owned, put_pack,
                begin_staged_put, size, list_sized, list_meta, remove_stale_temp_files,
                conditional, batch_delete, server_ops, profile];

        fn put(&self, key: &str, data: &[u8]) -> Result<()> {
            self.0.note("put", key);
            self.0.mem.put(key, data)
        }
        fn delete(&self, key: &str) -> Result<()> {
            self.0.note("delete", key);
            self.0.mem.delete(key)
        }
        fn exists(&self, key: &str) -> Result<bool> {
            self.0.note("exists", key);
            self.0.mem.exists(key)
        }
        fn list(&self, prefix: &str) -> Result<Vec<String>> {
            self.0.note("list", prefix);
            self.0.mem.list(prefix)
        }
        fn get_range(&self, key: &str, offset: u64, length: u64) -> Result<Option<Vec<u8>>> {
            self.0.note("get_range", key);
            self.0.mem.get_range(key, offset, length)
        }
        fn create_dir(&self, key: &str) -> Result<()> {
            self.0.note("create_dir", key);
            self.0.mem.create_dir(key)
        }
        fn put_owned(&self, key: &str, data: Vec<u8>) -> Result<()> {
            self.0.note("put_owned", key);
            self.0.mem.put(key, &data)
        }
        fn put_pack(&self, key: &str, data: PackData) -> Result<()> {
            self.0.note("put_pack", key);
            self.0.mem.put(key, data.as_slice())
        }
        fn begin_staged_put(&self, _prefix: &str) -> Result<Option<Box<dyn StagedPut>>> {
            Ok(Some(Box::new(Staged {
                bytes: Vec::new(),
                mem: Arc::clone(&self.0),
            })))
        }
        fn size(&self, key: &str) -> Result<Option<u64>> {
            self.0.note("size", key);
            self.0.mem.size(key)
        }
        fn list_sized(&self, prefix: &str) -> Result<Vec<vykar_storage::ListedObject>> {
            self.0.note("list_sized", prefix);
            self.0.mem.list_sized(prefix)
        }
        fn list_meta(&self, prefix: &str) -> Result<Vec<vykar_storage::ListedObject>> {
            self.0.note("list_meta", prefix);
            self.0.mem.list_meta(prefix)
        }
        fn remove_stale_temp_files(&self, older_than: std::time::Duration) -> Result<usize> {
            self.0.note("remove_stale_temp_files", "");
            self.0.mem.remove_stale_temp_files(older_than)
        }
        fn conditional(&self) -> Option<&dyn ConditionalWrite> {
            None
        }
        fn batch_delete(&self) -> Option<&dyn BatchDelete> {
            Some(self)
        }
        fn server_ops(&self) -> Option<&dyn vykar_storage::ServerOps> {
            None
        }
        fn profile(&self) -> BackendProfile {
            BackendProfile {
                staged_put: true,
                ..self.0.mem.profile()
            }
        }
    }

    impl BatchDelete for Shared {
        fn delete_keys(&self, keys: &[String], _scope: BatchDeleteScope) -> Result<()> {
            self.0.note("delete_keys", &keys.join(","));
            keys.iter().try_for_each(|k| self.0.mem.delete(k))
        }
    }

    struct Rig {
        checked: WriteChecked,
        checks: Arc<AtomicUsize>,
        fail: Arc<AtomicBool>,
        log: Arc<Mutex<Vec<String>>>,
    }

    impl Rig {
        fn new() -> Self {
            let log = Arc::new(Mutex::new(Vec::new()));
            let inner = Arc::new(Shared(Arc::new(Inner {
                mem: MemoryBackend::new(),
                log: Arc::clone(&log),
            })));
            let checks = Arc::new(AtomicUsize::new(0));
            let fail = Arc::new(AtomicBool::new(false));
            let check: FenceFn = {
                let (checks, fail, log) =
                    (Arc::clone(&checks), Arc::clone(&fail), Arc::clone(&log));
                Arc::new(move || {
                    checks.fetch_add(1, Ordering::SeqCst);
                    log.lock().unwrap().push("check".into());
                    if fail.load(Ordering::SeqCst) {
                        return Err(VykarError::LockExpired("injected".into()));
                    }
                    Ok(())
                })
            };
            Self {
                checked: WriteChecked::new(inner, check),
                checks,
                fail,
                log,
            }
        }

        fn take_log(&self) -> Vec<String> {
            std::mem::take(&mut *self.log.lock().unwrap())
        }
    }

    type Write = fn(&WriteChecked) -> Result<()>;

    /// Each write and the log line its forward leaves.
    fn writes() -> Vec<(&'static str, Write, &'static str)> {
        vec![
            ("put", |s| s.put("k", b"v"), "put k"),
            (
                "put_owned",
                |s| s.put_owned("k", b"v".to_vec()),
                "put_owned k",
            ),
            (
                "put_pack",
                |s| s.put_pack("k", PackData::new(b"v".to_vec())),
                "put_pack k",
            ),
            ("delete", |s| s.delete("k"), "delete k"),
            ("create_dir", |s| s.create_dir("d/"), "create_dir d/"),
            (
                "remove_stale_temp_files",
                |s| {
                    s.remove_stale_temp_files(std::time::Duration::ZERO)
                        .map(|_| ())
                },
                "remove_stale_temp_files ",
            ),
            (
                "commit",
                |s| {
                    let mut staged = s.begin_staged_put("k").unwrap().unwrap();
                    staged.append(b"v").unwrap();
                    staged.commit("k")
                },
                "commit k",
            ),
            (
                "delete_keys",
                |s| {
                    s.batch_delete()
                        .unwrap()
                        .delete_keys(&["k".to_string()], BatchDeleteScope::KeysOnly)
                },
                "delete_keys k",
            ),
        ]
    }

    #[test]
    fn every_write_is_checked_once_before_its_forward() {
        for (name, write, forwarded) in writes() {
            let rig = Rig::new();
            write(&rig.checked).unwrap();
            assert_eq!(rig.checks.load(Ordering::SeqCst), 1, "{name}");
            assert_eq!(
                rig.take_log(),
                vec!["check".to_string(), forwarded.to_string()],
                "{name}"
            );
        }
    }

    #[test]
    fn a_failed_check_forwards_nothing() {
        for (name, write, _) in writes() {
            let rig = Rig::new();
            rig.fail.store(true, Ordering::SeqCst);
            let err = write(&rig.checked).unwrap_err();
            assert!(matches!(err, VykarError::LockExpired(_)), "{name}: {err}");
            assert_eq!(rig.take_log(), vec!["check".to_string()], "{name}");
        }
    }

    #[test]
    fn a_refused_pack_is_still_released() {
        let rig = Rig::new();
        rig.fail.store(true, Ordering::SeqCst);
        let released = Arc::new(AtomicBool::new(false));
        let flag = Arc::clone(&released);
        let data =
            PackData::with_release(b"v".to_vec(), move || flag.store(true, Ordering::SeqCst));
        assert!(rig.checked.put_pack("k", data).is_err());
        assert!(released.load(Ordering::SeqCst));
    }

    #[test]
    fn reads_are_not_checked() {
        let rig = Rig::new();
        rig.checked.put("k", b"value").unwrap();
        rig.checks.store(0, Ordering::SeqCst);
        let s = &rig.checked;
        assert!(s.get("k").unwrap().is_some());
        s.list("").unwrap();
        s.list_sized("").unwrap();
        s.list_meta("").unwrap();
        assert!(s.exists("k").unwrap());
        assert_eq!(s.size("k").unwrap(), Some(5));
        s.get_range("k", 0, 2).unwrap();
        assert_eq!(rig.checks.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn conditional_and_profile_are_the_inner_backends() {
        let rig = Rig::new();
        assert!(rig.checked.conditional().is_none());
        assert!(rig.checked.profile().staged_put);
        // A backend with put-if-absent keeps offering it.
        let mem: Arc<dyn StorageBackend> = Arc::new(MemoryBackend::new());
        let wrapped = WriteChecked::new(Arc::clone(&mem), Arc::new(|| Ok(())));
        assert_eq!(wrapped.conditional().is_some(), mem.conditional().is_some());
        assert_eq!(wrapped.profile().staged_put, mem.profile().staged_put);
        assert_eq!(
            wrapped.batch_delete().is_some(),
            mem.batch_delete().is_some()
        );
    }
}
