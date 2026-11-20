//! Bounded fan-out of independent tasks (typically storage requests) over
//! scoped threads. Up to `width` threads claim items in order from a shared
//! counter; with `width <= 1` or a single item the work runs inline on the
//! caller's thread, in order.

use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Mutex, PoisonError};

/// Run `task` on every item until one fails, on up to `width` scoped threads.
/// Stops handing out work after the first error and returns it (the first one
/// recorded, not the lowest index); `Ok` only if every task succeeded.
pub(crate) fn try_for_each_bounded<T, E, F>(
    width: usize,
    items: &[T],
    task: F,
) -> std::result::Result<(), E>
where
    T: Sync,
    E: Send,
    F: Fn(&T) -> std::result::Result<(), E> + Sync,
{
    let threads = width.min(items.len());
    if threads <= 1 {
        return items.iter().try_for_each(task);
    }
    let next = AtomicUsize::new(0);
    let failed = AtomicBool::new(false);
    let first_err: Mutex<Option<E>> = Mutex::new(None);
    std::thread::scope(|s| {
        for _ in 0..threads {
            s.spawn(|| {
                loop {
                    if failed.load(Ordering::Relaxed) {
                        break;
                    }
                    let Some(item) = items.get(next.fetch_add(1, Ordering::Relaxed)) else {
                        break;
                    };
                    if let Err(e) = task(item) {
                        failed.store(true, Ordering::Relaxed);
                        let mut slot = first_err.lock().unwrap_or_else(PoisonError::into_inner);
                        if slot.is_none() {
                            *slot = Some(e);
                        }
                        break;
                    }
                }
            });
        }
    });
    match first_err
        .into_inner()
        .unwrap_or_else(PoisonError::into_inner)
    {
        Some(e) => Err(e),
        None => Ok(()),
    }
}

/// Run `task` on every item, on up to `width` scoped threads, whatever the
/// tasks report — `task` records its own outcome. Stops claiming items once
/// `cancel` is raised (checked before each claim); items not reached are
/// skipped silently, so a caller that counts coverage must re-check `cancel`.
pub(crate) fn for_each_bounded<T, F>(
    width: usize,
    items: &[T],
    cancel: Option<&AtomicBool>,
    task: F,
) where
    T: Sync,
    F: Fn(&T) + Sync,
{
    let cancelled = || cancel.is_some_and(|f| f.load(Ordering::Relaxed));
    let threads = width.min(items.len());
    if threads <= 1 {
        for item in items {
            if cancelled() {
                break;
            }
            task(item);
        }
        return;
    }
    let next = AtomicUsize::new(0);
    std::thread::scope(|s| {
        for _ in 0..threads {
            s.spawn(|| {
                loop {
                    if cancelled() {
                        break;
                    }
                    let Some(item) = items.get(next.fetch_add(1, Ordering::Relaxed)) else {
                        break;
                    };
                    task(item);
                }
            });
        }
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    fn counters(n: usize) -> Vec<AtomicUsize> {
        (0..n).map(|_| AtomicUsize::new(0)).collect()
    }

    fn counts(calls: &[AtomicUsize]) -> Vec<usize> {
        calls.iter().map(|c| c.load(Ordering::SeqCst)).collect()
    }

    #[test]
    fn try_for_each_runs_every_item() {
        let items: Vec<usize> = (0..50).collect();
        for width in [0, 1, 4, 100] {
            let calls = counters(items.len());
            let result = try_for_each_bounded(width, &items, |&i| {
                calls[i].fetch_add(1, Ordering::SeqCst);
                Ok::<_, ()>(())
            });
            assert_eq!(result, Ok(()), "width {width}");
            assert!(counts(&calls).iter().all(|&c| c == 1), "width {width}");
        }
    }

    #[test]
    fn try_for_each_inline_stops_at_first_error_in_order() {
        let items: Vec<usize> = (0..10).collect();
        let calls = counters(items.len());
        let result = try_for_each_bounded(1, &items, |&i| {
            calls[i].fetch_add(1, Ordering::SeqCst);
            if i == 3 || i == 7 { Err(i) } else { Ok(()) }
        });
        assert_eq!(result, Err(3));
        assert_eq!(counts(&calls), [1, 1, 1, 1, 0, 0, 0, 0, 0, 0]);
    }

    #[test]
    fn try_for_each_parallel_stops_handing_out_work() {
        let items: Vec<usize> = (0..1000).collect();
        let calls = AtomicUsize::new(0);
        let result = try_for_each_bounded(4, &items, |&i| {
            calls.fetch_add(1, Ordering::SeqCst);
            Err(i)
        });
        assert!(result.is_err());
        let calls = calls.load(Ordering::SeqCst);
        assert!((1..=4).contains(&calls), "calls {calls}");
    }

    #[test]
    fn width_one_runs_inline_on_caller_thread() {
        let caller = std::thread::current().id();
        let items = [0, 1, 2];
        let threads = Mutex::new(Vec::new());
        for_each_bounded(1, &items, None, |_| {
            threads.lock().unwrap().push(std::thread::current().id());
        });
        let result = try_for_each_bounded(1, &items, |_| {
            threads.lock().unwrap().push(std::thread::current().id());
            Ok::<_, ()>(())
        });
        assert_eq!(result, Ok(()));
        let threads = threads.into_inner().unwrap();
        assert_eq!(threads.len(), 6);
        assert!(threads.iter().all(|&id| id == caller));
    }

    #[test]
    fn for_each_runs_every_item_despite_failures() {
        let items: Vec<usize> = (0..64).collect();
        for width in [0, 1, 3, 100] {
            let calls = counters(items.len());
            let errors = Mutex::new(Vec::new());
            for_each_bounded(width, &items, None, |&i| {
                calls[i].fetch_add(1, Ordering::SeqCst);
                if i % 2 == 0 {
                    errors.lock().unwrap().push(i);
                }
            });
            assert!(counts(&calls).iter().all(|&c| c == 1), "width {width}");
            assert_eq!(errors.into_inner().unwrap().len(), 32, "width {width}");
        }
    }

    #[test]
    fn for_each_honours_cancel() {
        let items: Vec<usize> = (0..1000).collect();

        for width in [0, 1, 4] {
            let cancel = AtomicBool::new(true);
            let calls = AtomicUsize::new(0);
            for_each_bounded(width, &items, Some(&cancel), |_| {
                calls.fetch_add(1, Ordering::SeqCst);
            });
            assert_eq!(calls.load(Ordering::SeqCst), 0, "width {width}");
        }

        let cancel = AtomicBool::new(false);
        let calls = counters(items.len());
        for_each_bounded(1, &items, Some(&cancel), |&i| {
            calls[i].fetch_add(1, Ordering::SeqCst);
            if i == 5 {
                cancel.store(true, Ordering::Relaxed);
            }
        });
        let counts = counts(&calls);
        assert!(counts[..=5].iter().all(|&c| c == 1));
        assert!(counts[6..].iter().all(|&c| c == 0));

        let cancel = AtomicBool::new(false);
        let calls = AtomicUsize::new(0);
        for_each_bounded(4, &items, Some(&cancel), |_| {
            calls.fetch_add(1, Ordering::SeqCst);
            cancel.store(true, Ordering::Relaxed);
        });
        let calls = calls.load(Ordering::SeqCst);
        assert!((1..=4).contains(&calls), "calls {calls}");
    }

    #[test]
    fn for_each_bounds_concurrency() {
        let items: Vec<usize> = (0..30).collect();
        let in_flight = AtomicUsize::new(0);
        let max = AtomicUsize::new(0);
        for_each_bounded(3, &items, None, |_| {
            let now = in_flight.fetch_add(1, Ordering::SeqCst) + 1;
            max.fetch_max(now, Ordering::SeqCst);
            std::thread::sleep(Duration::from_millis(2));
            in_flight.fetch_sub(1, Ordering::SeqCst);
        });
        let max = max.load(Ordering::SeqCst);
        assert!((1..=3).contains(&max), "max in flight {max}");
    }
}
