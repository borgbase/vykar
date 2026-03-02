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
    let mut original = unsafe {
        // Safe because zeroed memory is immediately initialized by tcgetattr.
        std::mem::zeroed::<libc::termios>()
    };

    if unsafe {
        // Safe because fd is a valid stdin file descriptor and `original` is writable.
        libc::tcgetattr(fd, &mut original)
    } != 0
    {
        return Err(io::Error::last_os_error());
    }

    let mut no_echo = original;
    no_echo.c_lflag &= !libc::ECHO;

    if unsafe {
        // Safe because fd is valid and no_echo is a valid termios struct.
        libc::tcsetattr(fd, libc::TCSANOW, &no_echo)
    } != 0
    {
        return Err(io::Error::last_os_error());
    }

    struct RestoreTermios {
        fd: i32,
        original: libc::termios,
    }

    impl Drop for RestoreTermios {
        fn drop(&mut self) {
            let _ = unsafe {
                // Safe because values were obtained from a successful tcgetattr call.
                libc::tcsetattr(self.fd, libc::TCSANOW, &self.original)
            };
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

    let handle = unsafe {
        // Safe because STD_INPUT_HANDLE is a constant and the API has no Rust-side invariants.
        GetStdHandle(STD_INPUT_HANDLE)
    };

    if handle.is_null() || handle == INVALID_HANDLE_VALUE {
        stdin.lock().read_line(buf)?;
        return Ok(());
    }

    let mut mode: u32 = 0;
    if unsafe {
        // Safe because `handle` is a console handle and `mode` is writable.
        GetConsoleMode(handle, &mut mode)
    } == 0
    {
        stdin.lock().read_line(buf)?;
        return Ok(());
    }

    let new_mode = mode & !ENABLE_ECHO_INPUT;
    if unsafe {
        // Safe because `handle` and mode flags come from Win32 console APIs.
        SetConsoleMode(handle, new_mode)
    } == 0
    {
        return Err(io::Error::last_os_error());
    }

    struct RestoreConsoleMode {
        handle: HANDLE,
        mode: u32,
    }

    impl Drop for RestoreConsoleMode {
        fn drop(&mut self) {
            let _ = unsafe {
                // Safe because values were produced by successful console-mode calls.
                SetConsoleMode(self.handle, self.mode)
            };
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
