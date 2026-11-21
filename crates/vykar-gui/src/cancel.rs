use std::sync::{Arc, Mutex, PoisonError};

use vykar_core::repo::control::CancelToken;

/// The cancel token of the worker's current operation. The UI cancels the
/// current token; each operation starts on a fresh one (`renew`).
#[derive(Clone, Default)]
pub(crate) struct CancelSlot(Arc<Mutex<CancelToken>>);

impl CancelSlot {
    /// Install and return a fresh token. Called once at operation start.
    pub(crate) fn renew(&self) -> CancelToken {
        let token = CancelToken::new();
        *self.0.lock().unwrap_or_else(PoisonError::into_inner) = token.clone();
        token
    }

    /// The current operation's token.
    pub(crate) fn current(&self) -> CancelToken {
        self.0
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }

    /// Cancel the current operation.
    pub(crate) fn cancel(&self) {
        self.current().cancel();
    }

    pub(crate) fn is_cancelled(&self) -> bool {
        self.current().is_cancelled()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn renew_installs_an_uncancelled_token() {
        let slot = CancelSlot::default();
        slot.cancel();
        assert!(slot.is_cancelled());
        let token = slot.renew();
        assert!(!token.is_cancelled());
        assert!(!slot.is_cancelled());
    }

    #[test]
    fn cancel_reaches_the_current_token_only() {
        let slot = CancelSlot::default();
        let first = slot.renew();
        slot.cancel();
        assert!(first.is_cancelled());
        let second = slot.renew();
        assert!(!second.is_cancelled());
        assert!(first.is_cancelled());
        assert!(!slot.is_cancelled());
    }
}
