// libc/Win32 ioctl for terminal width detection; SAFETY documented per block.
#![allow(unsafe_code)]

use std::io::{self, IsTerminal, Stderr, Write};
use std::sync::atomic::{AtomicBool, Ordering::Relaxed};
use std::sync::{Mutex, MutexGuard};
use std::time::{Duration, Instant};

use tracing_subscriber::fmt::MakeWriter;
use vykar_core::commands;

use crate::format::format_bytes;

const PROGRESS_REDRAW_INTERVAL: Duration = Duration::from_millis(100);
const DEFAULT_PROGRESS_COLUMNS: usize = 120;

// ---------------------------------------------------------------------------
// Shared state between the progress renderer and the tracing writer
// ---------------------------------------------------------------------------

/// True while a backup progress line is being displayed on stderr.
static PROGRESS_ACTIVE: AtomicBool = AtomicBool::new(false);

/// Serializes all stderr writes between the progress renderer and tracing.
static STDERR_LOCK: Mutex<()> = Mutex::new(());

fn acquire_stderr_lock() -> MutexGuard<'static, ()> {
    STDERR_LOCK.lock().unwrap_or_else(|e| e.into_inner())
}

// ---------------------------------------------------------------------------
// Progress-aware tracing writer
// ---------------------------------------------------------------------------

/// A [`MakeWriter`] that clears the progress line before each tracing event,
/// preventing log messages from corrupting the `\r`-based progress display.
pub(crate) struct ProgressAwareStderr;

/// Holds the `STDERR_LOCK` guard for the entire lifetime of a single tracing
/// write, so the lock spans from the line-clear through the full log message.
pub(crate) struct ProgressWriter {
    _guard: MutexGuard<'static, ()>,
    inner: Stderr,
}

impl Write for ProgressWriter {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.inner.write(buf)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.inner.flush()
    }
}

impl<'a> MakeWriter<'a> for ProgressAwareStderr {
    type Writer = ProgressWriter;

    fn make_writer(&'a self) -> Self::Writer {
        let guard = acquire_stderr_lock();
        let mut stderr = io::stderr();

        if PROGRESS_ACTIVE.load(Relaxed) && stderr.is_terminal() {
            // Clear the current progress line so the log message starts clean.
            let _ = stderr.write_all(b"\r\x1b[2K");
        }

        ProgressWriter {
            _guard: guard,
            inner: stderr,
        }
    }
}

// ---------------------------------------------------------------------------
// Backup progress renderer
// ---------------------------------------------------------------------------

pub(crate) struct BackupProgressRenderer {
    current_file: Option<String>,
    nfiles: u64,
    original_size: u64,
    compressed_size: u64,
    deduplicated_size: u64,
    errors: u64,
    last_draw: Instant,
    last_line_len: usize,
    rendered_any: bool,
    verbose: u8,
    is_tty: bool,
}

impl BackupProgressRenderer {
    pub(crate) fn new(verbose: u8, is_tty: bool) -> Self {
        if is_tty {
            PROGRESS_ACTIVE.store(true, Relaxed);
        }
        Self {
            current_file: None,
            nfiles: 0,
            original_size: 0,
            compressed_size: 0,
            deduplicated_size: 0,
            errors: 0,
            last_draw: Instant::now(),
            last_line_len: 0,
            rendered_any: false,
            verbose,
            is_tty,
        }
    }

    pub(crate) fn on_event(&mut self, event: commands::backup::BackupProgressEvent) {
        match event {
            commands::backup::BackupProgressEvent::FileProcessed {
                path,
                status,
                added_bytes,
            } => {
                if self.verbose >= 1 {
                    if status == commands::backup::FileStatus::Unchanged && self.verbose < 2 {
                        return;
                    }
                    let status_str = match status {
                        commands::backup::FileStatus::New => "new      ",
                        commands::backup::FileStatus::Modified => "modified ",
                        commands::backup::FileStatus::Unchanged => "unchanged",
                    };
                    let size_suffix = if status == commands::backup::FileStatus::Unchanged {
                        String::new()
                    } else {
                        format!(" ({} added)", format_bytes(added_bytes))
                    };
                    let _guard = acquire_stderr_lock();
                    if self.is_tty {
                        eprint!("\r\x1b[2K");
                        self.last_line_len = 0;
                    }
                    eprintln!("{status_str} {path}{size_suffix}");
                }
                return;
            }
            commands::backup::BackupProgressEvent::FileStarted { path } => {
                if self.is_tty {
                    self.current_file = Some(path);
                }
            }
            commands::backup::BackupProgressEvent::StatsUpdated {
                nfiles,
                original_size,
                compressed_size,
                deduplicated_size,
                errors,
                current_file,
            } => {
                if self.is_tty {
                    self.nfiles = nfiles;
                    self.original_size = original_size;
                    self.compressed_size = compressed_size;
                    self.deduplicated_size = deduplicated_size;
                    self.errors = errors;
                    if let Some(path) = current_file {
                        self.current_file = Some(path);
                    }
                }
            }
            commands::backup::BackupProgressEvent::CommitStage { stage } => {
                if self.is_tty {
                    let _guard = acquire_stderr_lock();
                    eprint!("\r\x1b[2K");
                    self.last_line_len = 0;
                    eprint!("Committing: {stage}...");
                    let _ = io::stderr().flush();
                    self.rendered_any = true;
                }
                return;
            }
            commands::backup::BackupProgressEvent::Warning { message } => {
                // Defensive duplicate of tracing::warn! — guarantees the
                // warning reaches stderr even without the tracing subscriber.
                let _guard = acquire_stderr_lock();
                if self.is_tty {
                    eprint!("\r\x1b[2K");
                    self.last_line_len = 0;
                }
                eprintln!("warning: {message}");
                return;
            }
            commands::backup::BackupProgressEvent::SourceStarted { .. }
            | commands::backup::BackupProgressEvent::SourceFinished { .. } => return,
        }

        if self.is_tty {
            self.render(false);
        }
    }

    pub(crate) fn finish(&mut self) {
        if !self.is_tty || !self.rendered_any {
            PROGRESS_ACTIVE.store(false, Relaxed);
            return;
        }
        self.render(true);
        // Final newline under the lock so it doesn't race with tracing.
        {
            let _guard = acquire_stderr_lock();
            eprintln!();
        }
        PROGRESS_ACTIVE.store(false, Relaxed);
        self.rendered_any = false;
        self.last_line_len = 0;
    }

    fn render(&mut self, force: bool) {
        if !force && self.rendered_any && self.last_draw.elapsed() < PROGRESS_REDRAW_INTERVAL {
            return;
        }
        self.last_draw = Instant::now();

        let file = self.current_file.as_deref().unwrap_or("-");
        let errors_suffix = if self.errors > 0 {
            format!(", Errors: {}", self.errors)
        } else {
            String::new()
        };
        let prefix = format!(
            "Files: {}, Original: {}, Compressed: {}, Deduplicated: {}{errors_suffix}, Current: ",
            self.nfiles,
            format_bytes(self.original_size),
            format_bytes(self.compressed_size),
            format_bytes(self.deduplicated_size),
        );

        let columns = terminal_columns().saturating_sub(5);
        let available = columns.saturating_sub(str_display_width(&prefix));
        let current = truncate_middle(file, available);
        let line = format!("{prefix}{current}");
        let line_len = str_display_width(&line);
        let pad_len = self.last_line_len.saturating_sub(line_len);

        {
            let _guard = acquire_stderr_lock();
            eprint!("\r{line}{}", " ".repeat(pad_len));
            let _ = io::stderr().flush();
        }

        self.last_line_len = line_len;
        self.rendered_any = true;
    }
}

fn terminal_columns() -> usize {
    terminal_columns_os()
        .or_else(|| {
            std::env::var("COLUMNS")
                .ok()
                .and_then(|v| v.parse::<usize>().ok())
                .filter(|&v| v > 0)
        })
        .unwrap_or(DEFAULT_PROGRESS_COLUMNS)
}

/// Query the OS for the terminal width of stderr.
#[cfg(unix)]
fn terminal_columns_os() -> Option<usize> {
    use libc::{ioctl, winsize, STDERR_FILENO, TIOCGWINSZ};
    // SAFETY: `winsize` is a C POD; ioctl with TIOCGWINSZ writes the struct
    // through the `&mut ws` pointer, which is exclusive and properly aligned.
    unsafe {
        let mut ws: winsize = std::mem::zeroed();
        if ioctl(STDERR_FILENO, TIOCGWINSZ, &mut ws) == 0 && ws.ws_col > 0 {
            Some(ws.ws_col as usize)
        } else {
            None
        }
    }
}

#[cfg(windows)]
fn terminal_columns_os() -> Option<usize> {
    use windows_sys::Win32::System::Console::{
        GetConsoleScreenBufferInfo, GetStdHandle, CONSOLE_SCREEN_BUFFER_INFO, STD_ERROR_HANDLE,
    };
    // SAFETY: GetStdHandle is always sound; CONSOLE_SCREEN_BUFFER_INFO is a
    // C POD that the kernel populates through the exclusive `&mut info`
    // pointer.
    unsafe {
        let handle = GetStdHandle(STD_ERROR_HANDLE);
        let mut info: CONSOLE_SCREEN_BUFFER_INFO = std::mem::zeroed();
        if GetConsoleScreenBufferInfo(handle, &mut info) != 0 {
            let width = (info.srWindow.Right - info.srWindow.Left + 1) as usize;
            if width > 0 {
                Some(width)
            } else {
                None
            }
        } else {
            None
        }
    }
}

#[cfg(not(any(unix, windows)))]
fn terminal_columns_os() -> Option<usize> {
    None
}

use vykar_common::display::{str_display_width, truncate_middle};
