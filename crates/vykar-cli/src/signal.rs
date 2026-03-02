use std::sync::atomic::{AtomicBool, Ordering};

/// Global shutdown flag. Set to `true` on first SIGINT/SIGTERM.
pub static SHUTDOWN: AtomicBool = AtomicBool::new(false);

/// Install signal handlers for cooperative shutdown.
///
/// First signal sets [`SHUTDOWN`] and restores the default handler so a
/// second signal terminates immediately.
pub fn install_signal_handlers() {
    #[cfg(unix)]
    {
        // Safety: signal handler only sets an atomic bool and restores default handler.
        unsafe {
            libc::signal(
                libc::SIGTERM,
                unix_signal_handler as *const () as libc::sighandler_t,
            );
            libc::signal(
                libc::SIGINT,
                unix_signal_handler as *const () as libc::sighandler_t,
            );
        }
    }

    #[cfg(windows)]
    {
        unsafe {
            windows_sys::Win32::System::Console::SetConsoleCtrlHandler(
                Some(windows_console_handler),
                1, // TRUE
            );
        }
    }
}

#[cfg(unix)]
extern "C" fn unix_signal_handler(sig: libc::c_int) {
    SHUTDOWN.store(true, Ordering::SeqCst);
    // Restore default handler so a second signal kills immediately
    unsafe {
        libc::signal(sig, libc::SIG_DFL);
    }
}

#[cfg(windows)]
unsafe extern "system" fn windows_console_handler(ctrl_type: u32) -> i32 {
    // CTRL_C_EVENT (0), CTRL_BREAK_EVENT (1), CTRL_CLOSE_EVENT (2)
    if ctrl_type <= 2 {
        SHUTDOWN.store(true, Ordering::SeqCst);
        // Unregister this handler so a second signal terminates immediately
        windows_sys::Win32::System::Console::SetConsoleCtrlHandler(
            Some(windows_console_handler),
            0, // FALSE = remove
        );
        return 1; // TRUE = handled this time
    }
    0 // FALSE = not handled
}
