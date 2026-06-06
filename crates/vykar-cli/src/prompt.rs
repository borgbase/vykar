// Termios / Win32 console mode for hidden passphrase entry; SAFETY documented per block.
#![allow(unsafe_code)]

use std::io::{self, BufRead, IsTerminal, Write};

pub(crate) fn prompt_hidden(prompt: &str) -> io::Result<String> {
    eprint!("{prompt}");
    io::stderr().flush()?;

    let mut line = String::new();
    let read_result = read_hidden_line(&mut line);
    eprintln!();
    read_result?;

    while line.ends_with('\n') || line.ends_with('\r') {
        line.pop();
    }
    Ok(line)
}

#[cfg(unix)]
fn read_hidden_line(buf: &mut String) -> io::Result<()> {
    use std::os::fd::AsRawFd;

    let stdin = io::stdin();
    if !stdin.is_terminal() {
        stdin.lock().read_line(buf)?;
        return Ok(());
    }

    let fd = stdin.as_raw_fd();
    // SAFETY: termios is a C POD; the zeroed value is overwritten in full by
    // tcgetattr below before being read.
    let mut original = unsafe { std::mem::zeroed::<libc::termios>() };

    // SAFETY: fd is a valid stdin file descriptor (stdin is borrowed for the
    // lifetime of this function), and `original` is exclusively borrowed.
    if unsafe { libc::tcgetattr(fd, &mut original) } != 0 {
        return Err(io::Error::last_os_error());
    }

    let mut no_echo = original;
    no_echo.c_lflag &= !libc::ECHO;

    // SAFETY: fd is valid and `no_echo` is a fully-initialized termios value
    // (a copy of the one populated by tcgetattr).
    if unsafe { libc::tcsetattr(fd, libc::TCSANOW, &no_echo) } != 0 {
        return Err(io::Error::last_os_error());
    }

    struct RestoreTermios {
        fd: i32,
        original: libc::termios,
    }

    impl Drop for RestoreTermios {
        fn drop(&mut self) {
            // SAFETY: `self.original` was produced by a successful tcgetattr
            // and `self.fd` was valid for the borrow that constructed this
            // guard; the guard is dropped before stdin's borrow ends.
            let _ = unsafe { libc::tcsetattr(self.fd, libc::TCSANOW, &self.original) };
        }
    }

    let _restore = RestoreTermios { fd, original };
    stdin.lock().read_line(buf)?;
    Ok(())
}

#[cfg(windows)]
fn read_hidden_line(buf: &mut String) -> io::Result<()> {
    use windows_sys::Win32::Foundation::{HANDLE, INVALID_HANDLE_VALUE};
    use windows_sys::Win32::System::Console::{
        GetConsoleMode, GetStdHandle, SetConsoleMode, ENABLE_ECHO_INPUT, STD_INPUT_HANDLE,
    };

    let stdin = io::stdin();
    if !stdin.is_terminal() {
        stdin.lock().read_line(buf)?;
        return Ok(());
    }

    // SAFETY: STD_INPUT_HANDLE is a constant and GetStdHandle has no
    // Rust-side preconditions; we validate the returned handle below.
    let handle = unsafe { GetStdHandle(STD_INPUT_HANDLE) };

    if handle.is_null() || handle == INVALID_HANDLE_VALUE {
        stdin.lock().read_line(buf)?;
        return Ok(());
    }

    let mut mode: u32 = 0;
    // SAFETY: `handle` was validated above; `mode` is exclusively borrowed
    // and properly aligned.
    if unsafe { GetConsoleMode(handle, &mut mode) } == 0 {
        stdin.lock().read_line(buf)?;
        return Ok(());
    }

    let new_mode = mode & !ENABLE_ECHO_INPUT;
    // SAFETY: `handle` was validated above; `new_mode` is a u32 derived from
    // the previous mode, so it is a valid mode value for the console.
    if unsafe { SetConsoleMode(handle, new_mode) } == 0 {
        return Err(io::Error::last_os_error());
    }

    struct RestoreConsoleMode {
        handle: HANDLE,
        mode: u32,
    }

    impl Drop for RestoreConsoleMode {
        fn drop(&mut self) {
            // SAFETY: both values came from a successful console-mode round
            // trip during construction of this guard.
            let _ = unsafe { SetConsoleMode(self.handle, self.mode) };
        }
    }

    let _restore = RestoreConsoleMode { handle, mode };
    stdin.lock().read_line(buf)?;
    Ok(())
}

#[cfg(not(any(unix, windows)))]
fn read_hidden_line(buf: &mut String) -> io::Result<()> {
    io::stdin().lock().read_line(buf)?;
    Ok(())
}
