//! A shared conformance suite for [`crate::ConditionalWrite`] (the
//! sequential check, the init-time race, creates and concurrent creators),
//! [`StorageBackend::list_meta`], and listings started after a write on
//! another backend instance, run by every backend's tests against a real or
//! in-process store, and opt-in against MinIO, AWS and a real SFTP server.
//!
//! Library code (tests in other crates call it), so it reports a failed case
//! as `Err` naming it instead of panicking.

use std::sync::Barrier;

use crate::{ConditionalSupport, Created, MAX_PUT_IF_ABSENT_BYTES, StorageBackend};

/// What [`run`] checks and where it writes.
pub struct ConformanceOptions {
    /// Directory every key goes under; default `sessions/` (valid on
    /// vykar-server in every mode, deletable in append-only, ignored by
    /// session-marker and journal readers).
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
    /// Rounds of the listing case, alternating which handle writes
    /// (default 10).
    pub listing_rounds: usize,
}

impl Default for ConformanceOptions {
    fn default() -> Self {
        Self {
            prefix: "sessions/".into(),
            tombstones: false,
            writers: 8,
            rounds: 5,
            expect_mtime: true,
            listing_rounds: 10,
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

/// Run every case against `store`; `second` is another backend instance on
/// the same storage (own connections, no shared state), for the listing
/// case: a key written through one handle must be listed by a LIST the
/// other starts afterwards. Requires `store.conditional()` to be `Some`.
/// Writes only below a fresh `<prefix>vykar-probe-…` name per key (the
/// init-time race below `sessions/vykar-probe-…`) and deletes what it wrote.
///
/// # Errors
///
/// The first failed case, by name. A cleanup failure is reported only if no
/// case failed first.
pub fn run(
    store: &dyn StorageBackend,
    second: &dyn StorageBackend,
    opts: &ConformanceOptions,
) -> Outcome {
    let mut written = Vec::new();
    let result = run_cases(store, second, opts, &mut written);
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
    second: &dyn StorageBackend,
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

    // 1b. The init-time check: the cached sequential answer plus the race.
    let support = cond
        .verify_at_init()
        .map_err(|e| format!("verify_at_init: {e}"))?;
    ensure(
        support == ConditionalSupport::Verified,
        "verify_at_init",
        || format!("{support:?}"),
    )?;

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

    // 5b. "Now" on the store's clock: the mtime of a key just written
    // measures the age of one written just before it.
    if opts.expect_mtime {
        let first = format!("{base}-a");
        let second = format!("{base}-b");
        written.push(first.clone());
        written.push(second.clone());
        store
            .put(&first, b"a")
            .map_err(|e| format!("store_now: put: {e}"))?;
        store
            .put(&second, b"b")
            .map_err(|e| format!("store_now: put: {e}"))?;
        let listing = store
            .list_meta(&opts.prefix)
            .map_err(|e| format!("store_now: list_meta: {e}"))?;
        let now = crate::StoreNow::of_written(&listing, &second)
            .ok_or_else(|| format!("store_now: {second} not listed with an mtime"))?;
        let mtime = listing
            .iter()
            .find(|o| o.key == first)
            .and_then(|o| o.mtime)
            .ok_or_else(|| format!("store_now: {first} not listed with an mtime"))?;
        let age = now.age_of(mtime);
        ensure(
            age < std::time::Duration::from_secs(60),
            "store_now",
            || format!("a key written just before is {age:?} old"),
        )?;
    }

    // 5c. A listing started after a write's reply shows the key, whichever
    // handle wrote it (session markers are plain PUTs, read through both
    // list calls).
    for round in 0..opts.listing_rounds.max(1) {
        let (writer, lister, which) = if round % 2 == 0 {
            (store, second, "second")
        } else {
            (second, store, "first")
        };
        let k = format!("{base}-list{round}");
        written.push(k.clone());
        writer
            .put(&k, b"l")
            .map_err(|e| format!("list_after_write: round {round}: put: {e}"))?;
        let missing = |call: &str| {
            format!("list_after_write: round {round}: {k} not in {call} through the {which} handle")
        };
        let listed = lister
            .list(&opts.prefix)
            .map_err(|e| format!("list_after_write: round {round}: list: {e}"))?;
        if !listed.contains(&k) {
            return Err(missing("list"));
        }
        let meta = lister
            .list_meta(&opts.prefix)
            .map_err(|e| format!("list_after_write: round {round}: list_meta: {e}"))?;
        if !meta.iter().any(|o| o.key == k) {
            return Err(missing("list_meta"));
        }
    }

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
