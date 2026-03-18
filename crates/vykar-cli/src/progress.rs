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

// ---------------------------------------------------------------------------
// Display-width helpers (East Asian Wide / Fullwidth characters)
// ---------------------------------------------------------------------------

/// Return the terminal display width of a single character.
/// CJK and fullwidth characters occupy 2 columns; everything else occupies 1.
fn char_display_width(c: char) -> usize {
    let cp = c as u32;
    if matches!(cp,
        0x1100..=0x115F   // Hangul Jamo initials
        | 0x2E80..=0x303E // CJK Radicals, Kangxi, Symbols & Punctuation
        | 0x3040..=0x33FF // Hiragana, Katakana, Bopomofo, CJK Compat
        | 0x3400..=0x4DBF // CJK Extension A
        | 0x4E00..=0x9FFF // CJK Unified Ideographs
        | 0xAC00..=0xD7AF // Hangul Syllables
        | 0xF900..=0xFAFF // CJK Compat Ideographs
        | 0xFE30..=0xFE6F // CJK Compat Forms
        | 0xFF01..=0xFF60 // Fullwidth Forms
        | 0xFFE0..=0xFFE6 // Fullwidth Signs
        | 0x20000..=0x3FFFF // CJK Extensions B–G
    ) {
        2
    } else {
        1
    }
}

/// Return the terminal display width of a string (sum of character widths).
fn str_display_width(s: &str) -> usize {
    s.chars().map(char_display_width).sum()
}

// ---------------------------------------------------------------------------
// Middle-truncation for file paths
// ---------------------------------------------------------------------------

/// Truncate a string to `max_cols` **display columns**, showing both the
/// beginning and end with `...` in the middle (e.g. `/very/l...file.txt`).
fn truncate_middle(input: &str, max_cols: usize) -> String {
    if max_cols == 0 {
        return String::new();
    }

    let input_width = str_display_width(input);
    if input_width <= max_cols {
        return input.to_string();
    }

    if max_cols <= 3 {
        return ".".repeat(max_cols);
    }

    let keep = max_cols - 3; // columns available for head + tail
    let head_budget = keep / 2;
    let tail_budget = keep - head_budget;

    // Build head: take chars until we'd exceed head_budget columns.
    let mut head_str = String::new();
    let mut head_used = 0;
    for c in input.chars() {
        let w = char_display_width(c);
        if head_used + w > head_budget {
            break;
        }
        head_str.push(c);
        head_used += w;
    }

    // Build tail: take chars from the end until we'd exceed tail_budget columns.
    let mut tail_chars: Vec<char> = Vec::new();
    let mut tail_used = 0;
    for c in input.chars().rev() {
        let w = char_display_width(c);
        if tail_used + w > tail_budget {
            break;
        }
        tail_chars.push(c);
        tail_used += w;
    }
    tail_chars.reverse();
    let tail_str: String = tail_chars.into_iter().collect();

    format!("{head_str}...{tail_str}")
}

#[cfg(test)]
mod tests {
    use super::{str_display_width, truncate_middle};

    #[test]
    fn truncate_middle_shows_head_and_tail() {
        let input = "/very/long/path/to/a/file.txt";
        let out = truncate_middle(input, 16);
        // keep = 13, head = 6, tail = 7
        assert_eq!(out, "/very/...ile.txt");
        assert_eq!(str_display_width(&out), 16);
    }

    #[test]
    fn truncate_middle_returns_original_when_short() {
        let input = "short.txt";
        assert_eq!(truncate_middle(input, 32), input);
    }

    #[test]
    fn truncate_middle_handles_tiny_widths() {
        assert_eq!(truncate_middle("abcdef", 0), "");
        assert_eq!(truncate_middle("abcdef", 1), ".");
        assert_eq!(truncate_middle("abcdef", 2), "..");
        assert_eq!(truncate_middle("abcdef", 3), "...");
    }

    #[test]
    fn truncate_middle_exact_fit() {
        let input = "exactly10!";
        assert_eq!(truncate_middle(input, 10), input);
    }

    #[test]
    fn truncate_middle_one_over() {
        // 11 chars, max 10 → keep=7, head=3, tail=4
        let input = "abcdefghijk";
        let out = truncate_middle(input, 10);
        assert_eq!(out, "abc...hijk");
        assert_eq!(str_display_width(&out), 10);
    }

    #[test]
    fn truncate_middle_unicode() {
        let input = "aaaa\u{00e9}\u{00e9}\u{00e9}\u{00e9}bbbb"; // 12 chars, all width 1
        let out = truncate_middle(input, 10);
        // keep=7, head=3, tail=4
        assert_eq!(out, "aaa...bbbb");
        assert_eq!(str_display_width(&out), 10);
    }

    #[test]
    fn truncate_middle_cjk() {
        // Each CJK char = 2 columns. "文件" = 4 cols, "/" = 1, "路径" = 4, "/" = 1,
        // "测试报告.pdf" = 8+4 = 12. Total display width = 22.
        let input = "文件/路径/测试报告.pdf";
        let out = truncate_middle(input, 16);
        // keep=13, head_budget=6, tail_budget=7
        // head: "文件/" = 2+2+1 = 5 cols (next char "路" would be 7 > 6) → "文件/"
        // tail from end: ".pdf" = 4, "告" = 2 → 6, "报" = 2 → 8 > 7 → tail = "告.pdf"
        assert_eq!(out, "文件/...告.pdf");
        assert!(str_display_width(&out) <= 16);
    }
}
