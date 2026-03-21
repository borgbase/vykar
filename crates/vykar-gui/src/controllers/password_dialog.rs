use std::cell::RefCell;
use std::sync::mpsc;

use slint::ComponentHandle;

use crate::PasswordDialog;

thread_local! {
    static DIALOG_HANDLE: RefCell<Option<PasswordDialog>> = const { RefCell::new(None) };
}

/// Drop the stored dialog handle, releasing the component and its LineEdit state.
fn drop_handle() {
    DIALOG_HANDLE.with(|cell| {
        cell.borrow_mut().take();
    });
}

/// Show a modal password dialog and block until the user responds.
///
/// Can be called from any thread (typically the worker thread).
/// Returns `Some(password)` on submit, `None` on cancel/close/error.
pub(crate) fn show_password_dialog(title: &str, message: &str) -> Option<String> {
    let (tx, rx) = mpsc::sync_channel(1);
    let title = title.to_string();
    let message = message.to_string();

    if slint::invoke_from_event_loop(move || {
        let dialog = PasswordDialog::new().unwrap();
        dialog.set_dialog_title(title.into());
        dialog.set_dialog_message(message.into());

        let d = dialog.clone_strong();
        let tx_submit = tx.clone();
        dialog.on_submitted(move |value| {
            let _ = tx_submit.send(Some(value.to_string()));
            let _ = d.hide();
            drop_handle();
        });

        let d = dialog.clone_strong();
        let tx_cancel = tx.clone();
        dialog.on_cancelled(move || {
            let _ = tx_cancel.send(None);
            let _ = d.hide();
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
