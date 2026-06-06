// libc::signal / Win32 SetConsoleCtrlHandler for cooperative shutdown; SAFETY per block.
#![allow(unsafe_code)]

use std::sync::atomic::{AtomicBool, Ordering};

/// Global shutdown flag. Set to `true` on first SIGINT/SIGTERM.
pub static SHUTDOWN: AtomicBool = AtomicBool::new(false);

/// Global reload flag (Unix only). Set to `true` on SIGHUP.
/// The daemon checks this between backup cycles and re-reads config.
pub static RELOAD: AtomicBool = AtomicBool::new(false);

/// Global trigger flag (Unix only). Set to `true` on SIGUSR1.
/// The daemon checks this between backup cycles and runs an immediate backup.
pub static TRIGGER: AtomicBool = AtomicBool::new(false);

/// Install signal handlers for cooperative shutdown.
///
/// First signal sets [`SHUTDOWN`] and restores the default handler so a
/// second signal terminates immediately.
pub fn install_signal_handlers() {
    #[cfg(unix)]
    {
        // SAFETY: each handler we install only stores an atomic bool (and the
        // SIGINT/SIGTERM handler restores SIG_DFL via libc::signal). All
        // operations are async-signal-safe per POSIX; no heap allocation,
        // locks, or non-reentrant functions are reached.
        unsafe {
            libc::signal(
                libc::SIGTERM,
                unix_signal_handler as *const () as libc::sighandler_t,
            );
            libc::signal(
                libc::SIGINT,
                unix_signal_handler as *const () as libc::sighandler_t,
            );
            libc::signal(
                libc::SIGHUP,
                unix_reload_handler as *const () as libc::sighandler_t,
            );
            libc::signal(
                libc::SIGUSR1,
                unix_trigger_handler as *const () as libc::sighandler_t,
            );
        }
    }

    #[cfg(windows)]
    {
        // SAFETY: SetConsoleCtrlHandler accepts a static function pointer and
        // an integer flag; no aliasing or lifetime concerns.
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
    // SAFETY: libc::signal with SIG_DFL is async-signal-safe and only
    // mutates the per-process disposition for `sig`.
    unsafe {
        libc::signal(sig, libc::SIG_DFL);
    }
}

#[cfg(unix)]
extern "C" fn unix_reload_handler(_sig: libc::c_int) {
    RELOAD.store(true, Ordering::SeqCst);
    // Do NOT restore default handler — SIGHUP should be repeatable
}

#[cfg(unix)]
extern "C" fn unix_trigger_handler(_sig: libc::c_int) {
    TRIGGER.store(true, Ordering::SeqCst);
    // Do NOT restore default handler — SIGUSR1 should be repeatable
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
