//! A shared conformance suite for [`crate::ConditionalWrite`] and
//! [`StorageBackend::list_meta`], run by every backend's tests against a real
//! or in-process store, and opt-in against MinIO and AWS.
//!
//! Library code (tests in other crates call it), so it reports a failed case
//! as `Err` naming it instead of panicking.

use std::sync::Barrier;

use crate::{ConditionalSupport, Created, MAX_PUT_IF_ABSENT_BYTES, StorageBackend};

/// What [`run`] checks and where it writes.
pub struct ConformanceOptions {
    /// Directory every key goes under; default `locks/` (valid on
    /// vykar-server in every mode, deletable in append-only, ignored by lock
    /// readers).
    pub prefix: String,
    /// The backend keeps a deleted key as existing (S3 soft delete): skip the
    /// re-create-after-delete case.
    pub tombstones: bool,
    /// Concurrent creators per round (default 8).
    pub writers: usize,
    /// Race rounds (default 5).
    pub rounds: usize,
    /// `list_meta` must report an mtime (true for every real backend here).
    pub expect_mtime: bool,
}

impl Default for ConformanceOptions {
    fn default() -> Self {
        Self {
            prefix: "locks/".into(),
            tombstones: false,
            writers: 8,
            rounds: 5,
            expect_mtime: true,
        }
    }
}

type Outcome = std::result::Result<(), String>;

fn ensure(ok: bool, case: &str, what: impl FnOnce() -> String) -> Outcome {
    if ok {
        Ok(())
    } else {
        Err(format!("{case}: {}", what()))
    }
}

/// Run every case against `store`. Requires `store.conditional()` to be
/// `Some`. Writes only below a fresh `<prefix>vykar-probe-…` name per key and
/// deletes what it wrote.
///
/// # Errors
///
/// The first failed case, by name. A cleanup failure is reported only if no
/// case failed first.
pub fn run(store: &dyn StorageBackend, opts: &ConformanceOptions) -> Outcome {
    let mut written = Vec::new();
    let result = run_cases(store, opts, &mut written);
    let mut cleanup = Ok(());
    for key in &written {
        if let Err(e) = store.delete(key)
            && cleanup.is_ok()
        {
            cleanup = Err(format!("cleanup: delete {key}: {e}"));
        }
    }
    result.and(cleanup)
}

fn run_cases(
    store: &dyn StorageBackend,
    opts: &ConformanceOptions,
    written: &mut Vec<String>,
) -> Outcome {
    let base = format!("{}{}", opts.prefix, crate::probe::new_prefix());
    let cond = store
        .conditional()
        .ok_or_else(|| "conditional(): None, the suite needs Some".to_string())?;

    // 1. The sequential check.
    let support = cond.verify().map_err(|e| format!("verify: {e}"))?;
    ensure(support == ConditionalSupport::Verified, "verify", || {
        format!("{support:?}")
    })?;

    // 2. Create.
    let k = format!("{base}-create");
    let a = crate::probe::payload("A", &k, 256);
    let b = crate::probe::payload("B", &k, 256);
    written.push(k.clone());
    let got = cond
        .put_if_absent(&k, &a)
        .map_err(|e| format!("create: {e}"))?;
    ensure(got == Created::Yes, "create", || format!("{got:?}"))?;
    let stored = store.get(&k).map_err(|e| format!("create: get: {e}"))?;
    ensure(stored.as_deref() == Some(&a[..]), "create", || {
        "stored bytes differ".into()
    })?;

    // 3. A second create with other bytes is refused and changes nothing.
    let got = cond
        .put_if_absent(&k, &b)
        .map_err(|e| format!("repeat_other: {e}"))?;
    ensure(got == Created::AlreadyExists, "repeat_other", || {
        format!("{got:?}")
    })?;
    let stored = store
        .get(&k)
        .map_err(|e| format!("repeat_other: get: {e}"))?;
    ensure(stored.as_deref() == Some(&a[..]), "repeat_other", || {
        "the refused create replaced the object".into()
    })?;

    // 4. The same bytes again: still "exists".
    let got = cond
        .put_if_absent(&k, &a)
        .map_err(|e| format!("repeat_same: {e}"))?;
    ensure(got == Created::AlreadyExists, "repeat_same", || {
        format!("{got:?}")
    })?;

    // 5. Listings.
    let meta = store
        .list_meta(&opts.prefix)
        .map_err(|e| format!("list_meta: {e}"))?;
    let entry = meta
        .iter()
        .find(|o| o.key == k)
        .ok_or_else(|| format!("list_meta: {k} missing"))?;
    ensure(entry.size == Some(a.len() as u64), "list_meta", || {
        format!("size {:?}, want {}", entry.size, a.len())
    })?;
    ensure(
        !opts.expect_mtime || entry.mtime.is_some(),
        "list_meta",
        || "no mtime reported".into(),
    )?;
    let sized = store
        .list_sized(&opts.prefix)
        .map_err(|e| format!("list_sized: {e}"))?;
    let entry = sized
        .iter()
        .find(|o| o.key == k)
        .ok_or_else(|| format!("list_sized: {k} missing"))?;
    ensure(entry.mtime.is_none(), "list_sized", || {
        "list_sized reported an mtime".into()
    })?;

    // 6. Re-create after delete.
    if !opts.tombstones {
        store
            .delete(&k)
            .map_err(|e| format!("recreate: delete: {e}"))?;
        let got = cond
            .put_if_absent(&k, &b)
            .map_err(|e| format!("recreate: {e}"))?;
        ensure(got == Created::Yes, "recreate", || format!("{got:?}"))?;
        let stored = store.get(&k).map_err(|e| format!("recreate: get: {e}"))?;
        ensure(stored.as_deref() == Some(&b[..]), "recreate", || {
            "stored bytes differ".into()
        })?;
    }

    // 7. Concurrent creators: exactly one wins.
    for round in 0..opts.rounds {
        let key = format!("{base}-race{round}");
        written.push(key.clone());
        race_round(store, &key, opts.writers.max(2))?;
    }

    // 8. Over the cap: refused, nothing created.
    let key = format!("{base}-overcap");
    let big = vec![0u8; MAX_PUT_IF_ABSENT_BYTES + 1];
    ensure(cond.put_if_absent(&key, &big).is_err(), "overcap", || {
        "an over-cap body was accepted".into()
    })?;
    let exists = store
        .exists(&key)
        .map_err(|e| format!("overcap: exists: {e}"))?;
    ensure(!exists, "overcap", || {
        "an over-cap body created the key".into()
    })?;
    Ok(())
}

fn race_round(store: &dyn StorageBackend, key: &str, writers: usize) -> Outcome {
    let case = format!("race {key}");
    let cond = store
        .conditional()
        .ok_or_else(|| format!("{case}: conditional() became None"))?;
    let payloads: Vec<Vec<u8>> = (0..writers)
        .map(|i| crate::probe::payload(&format!("w{i}"), key, 256))
        .collect();
    let barrier = Barrier::new(writers);
    let results: Vec<_> = std::thread::scope(|s| {
        let handles: Vec<_> = payloads
            .iter()
            .map(|data| {
                let barrier = &barrier;
                s.spawn(move || {
                    barrier.wait();
                    cond.put_if_absent(key, data)
                })
            })
            .collect();
        handles
            .into_iter()
            .map(|h| {
                h.join().unwrap_or_else(|_| {
                    Err(vykar_types::error::VykarError::Other(
                        "writer thread panicked".into(),
                    ))
                })
            })
            .collect::<Vec<vykar_types::error::Result<Created>>>()
    });
    let mut winner = None;
    for (i, r) in results.iter().enumerate() {
        match r {
            Ok(Created::Yes) if winner.is_none() => winner = Some(i),
            Ok(Created::Yes) => return Err(format!("{case}: two creators won")),
            Ok(Created::AlreadyExists) => {}
            Err(e) => return Err(format!("{case}: writer {i}: {e}")),
        }
    }
    let winner = winner.ok_or_else(|| format!("{case}: no creator won"))?;
    let stored = store.get(key).map_err(|e| format!("{case}: get: {e}"))?;
    ensure(
        stored.as_deref() == payloads.get(winner).map(Vec::as_slice),
        &case,
        || "the stored bytes are not the winner's".into(),
    )
}
