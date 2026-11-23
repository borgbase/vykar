//! Engine control types: cancellation and event delivery. Nothing here
//! depends on `commands` or `app`.

use std::cell::RefCell;

pub use vykar_types::cancel::{CancelToken, Cancelled};

/// Receives the events of one operation. Object safe; `emit` takes `&self`
/// so a sink can be shared. A sink that worker threads share is passed as
/// `&(dyn EventSink<E> + Sync)`.
pub trait EventSink<E> {
    fn emit(&self, event: E);
}

impl<E, F: Fn(E) + ?Sized> EventSink<E> for F {
    fn emit(&self, event: E) {
        self(event);
    }
}

/// Adapts a stateful `FnMut` callback for use on one thread.
pub struct FnMutSink<F>(RefCell<F>);

impl<F> FnMutSink<F> {
    pub fn new(f: F) -> Self {
        Self(RefCell::new(f))
    }
}

impl<E, F: FnMut(E)> EventSink<E> for FnMutSink<F> {
    fn emit(&self, event: E) {
        (self.0.borrow_mut())(event);
    }
}

/// Progress of a backup commit (`repo::commit_machine::BackupCommit` and
/// `Repository::finish_backup_commit`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CommitEvent {
    /// A commit stage starts.
    Stage { stage: &'static str },
    /// A non-fatal issue after the commit point; the snapshot is durable.
    /// `tracing::warn!` has already fired.
    Warning { message: String },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fn_mut_sink_forwards_in_order() {
        let mut seen = Vec::new();
        {
            let sink = FnMutSink::new(|e: u32| seen.push(e));
            let sink: &dyn EventSink<u32> = &sink;
            sink.emit(1);
            sink.emit(2);
        }
        assert_eq!(seen, vec![1, 2]);
    }

    #[test]
    fn plain_closure_is_a_sink() {
        let total = std::cell::Cell::new(0);
        let add = |e: u32| total.set(total.get() + e);
        let sink: &dyn EventSink<u32> = &add;
        sink.emit(3);
        sink.emit(4);
        assert_eq!(total.get(), 7);
    }
}
