use std::cell::RefCell;
use std::sync::mpsc;

use slint::ComponentHandle;

use crate::PasswordDialog;

thread_local! {
    static DIALOG_HANDLE: RefCell<Option<PasswordDialog>> = const { RefCell::new(None) };
}

/// Drop the stored dialog handle, releasing the component and its LineEdit state.
///
/// Deferred to the event loop so the component is not dropped while one of its
/// own callbacks is still executing.
fn drop_handle() {
    let _ = slint::invoke_from_event_loop(|| {
        DIALOG_HANDLE.with(|cell| {
            cell.borrow_mut().take();
        });
    });
}

/// Show a modal password dialog and block until the user responds.
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
        let dialog = PasswordDialog::new().expect("password dialog component can be created");
        dialog.set_dialog_title(title.into());
        dialog.set_dialog_message(message.into());
        dialog.set_error_text(error.into());

        // Callbacks must capture the dialog weakly: a strong handle stored in a
        // closure owned by the component itself is a reference cycle that keeps
        // the native window alive forever (#155).
        let weak = dialog.as_weak();
        let tx_submit = tx.clone();
        dialog.on_submitted(move |value| {
            let _ = tx_submit.send(Some(value.to_string()));
            if let Some(d) = weak.upgrade() {
                let _ = d.hide();
            }
            drop_handle();
        });

        let weak = dialog.as_weak();
        let tx_cancel = tx.clone();
        dialog.on_cancelled(move || {
            let _ = tx_cancel.send(None);
            if let Some(d) = weak.upgrade() {
                let _ = d.hide();
            }
            drop_handle();
        });

        dialog.window().on_close_requested(move || {
            let _ = tx.try_send(None);
            drop_handle();
            slint::CloseRequestResponse::HideWindow
        });

        let _ = dialog.show();
        DIALOG_HANDLE.with(|cell| *cell.borrow_mut() = Some(dialog));
    })
    .is_err()
    {
        return None;
    }

    rx.recv().ok().flatten()
}
