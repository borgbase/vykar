use std::cell::RefCell;
use std::sync::mpsc;

use slint::{ComponentHandle, Weak};

use crate::{event_consumer, MainWindow};

thread_local! {
    static MAIN_WINDOW: RefCell<Option<Weak<MainWindow>>> = const { RefCell::new(None) };
    // Dropping the sender wakes the waiting worker with a `None` reply (cancel).
    static PENDING_REPLY: RefCell<Option<mpsc::SyncSender<Option<String>>>> =
        const { RefCell::new(None) };
}

fn finish(reply: Option<String>) {
    if let Some(tx) = PENDING_REPLY.with(|cell| cell.borrow_mut().take()) {
        let _ = tx.send(reply);
    }
}

/// Wire the prompt embedded in the main window. Call once, on the UI thread,
/// before the event loop starts.
pub(crate) fn install(ui: &MainWindow) {
    MAIN_WINDOW.with(|cell| *cell.borrow_mut() = Some(ui.as_weak()));
    ui.on_password_submitted(|value| finish(Some(value.to_string())));
    ui.on_password_cancelled(|| finish(None));
}

/// Close an open prompt and cancel it, so the worker isn't left waiting on a
/// prompt nobody can see. Does nothing if no prompt is open.
pub(crate) fn cancel_pending(ui: &MainWindow) {
    ui.invoke_close_password_prompt();
    finish(None);
}

/// Show a modal password prompt and block until the user responds.
///
/// Can be called from any thread (typically the worker thread).
/// Returns `Some(password)` on submit, `None` on cancel/close/error.
pub(crate) fn show_password_dialog(title: &str, message: &str) -> Option<String> {
    show_password_dialog_with_error(title, message, "")
}

/// Like [`show_password_dialog`] but renders `error` (when non-empty) as a red
/// line above the input — used when re-prompting after an incorrect passphrase.
pub(crate) fn show_password_dialog_with_error(
    title: &str,
    message: &str,
    error: &str,
) -> Option<String> {
    let (tx, rx) = mpsc::sync_channel(1);
    let title = title.to_string();
    let message = message.to_string();
    let error = error.to_string();

    if slint::invoke_from_event_loop(move || {
        let Some(ui) = MAIN_WINDOW.with(|cell| cell.borrow().as_ref().and_then(Weak::upgrade))
        else {
            return;
        };
        // Replacing a sender still pending here cancels that older prompt.
        PENDING_REPLY.with(|cell| *cell.borrow_mut() = Some(tx));
        ui.set_password_prompt_title(title.into());
        ui.set_password_prompt_message(message.into());
        ui.set_password_prompt_error(error.into());
        event_consumer::raise_main_window(&ui);
        ui.invoke_show_password_prompt();
    })
    .is_err()
    {
        return None;
    }

    rx.recv().ok().flatten()
}
