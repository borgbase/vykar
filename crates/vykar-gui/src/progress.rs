use std::time::Instant;

use vykar_core::app::operations::{CycleStep, StepOutcome};
use vykar_core::commands::backup::BackupProgressEvent;
use vykar_core::commands::check::CheckProgressEvent;

const THROTTLE_MS: u128 = 250;

pub fn format_bytes(bytes: u64) -> String {
    const KB: f64 = 1024.0;
    const MB: f64 = KB * 1024.0;
    const GB: f64 = MB * 1024.0;

    let b = bytes as f64;
    if b >= GB {
        format!("{:.2} GiB", b / GB)
    } else if b >= MB {
        format!("{:.2} MiB", b / MB)
    } else if b >= KB {
        format!("{:.2} KiB", b / KB)
    } else {
        format!("{bytes} B")
    }
}

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

fn str_display_width(s: &str) -> usize {
    s.chars().map(char_display_width).sum()
}

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

pub fn format_count(n: u64) -> String {
    let s = n.to_string();
    let mut result = String::with_capacity(s.len() + s.len() / 3);
    for (i, c) in s.chars().enumerate() {
        if i > 0 && (s.len() - i).is_multiple_of(3) {
            result.push(',');
        }
        result.push(c);
    }
    result
}

pub struct BackupStatusTracker {
    repo_name: String,
    last_update: Instant,
    current_file: Option<String>,
}

impl BackupStatusTracker {
    pub fn new(repo_name: String) -> Self {
        Self {
            repo_name,
            last_update: Instant::now() - std::time::Duration::from_millis(THROTTLE_MS as u64),
            current_file: None,
        }
    }

    /// Returns Some(status_string) if enough time has elapsed or the event is significant.
    /// Returns None if the update should be skipped (throttled).
    pub fn format(&mut self, event: &BackupProgressEvent) -> Option<String> {
        match event {
            BackupProgressEvent::SourceStarted { source_path } => {
                self.last_update = Instant::now();
                Some(format!(
                    "[{}] backing up {}...",
                    self.repo_name, source_path
                ))
            }
            BackupProgressEvent::FileStarted { path } => {
                self.current_file = Some(path.clone());
                None
            }
            BackupProgressEvent::StatsUpdated {
                nfiles,
                original_size,
                current_file,
                ..  // errors, compressed_size, deduplicated_size
            } => {
                if let Some(f) = current_file {
                    self.current_file = Some(f.clone());
                }
                let now = Instant::now();
                if now.duration_since(self.last_update).as_millis() < THROTTLE_MS {
                    return None;
                }
                self.last_update = now;
                let file_suffix = match &self.current_file {
                    Some(f) => format!(" - {}", truncate_middle(f, 60)),
                    None => String::new(),
                };
                Some(format!(
                    "[{}] {} files, {}{}",
                    self.repo_name,
                    format_count(*nfiles),
                    format_bytes(*original_size),
                    file_suffix,
                ))
            }
            BackupProgressEvent::CommitStage { stage } => {
                self.last_update = Instant::now();
                Some(format!("[{}] committing: {}...", self.repo_name, stage))
            }
            _ => None,
        }
    }
}

pub fn format_check_status(repo_name: &str, event: &CheckProgressEvent) -> String {
    match event {
        CheckProgressEvent::SnapshotStarted {
            current,
            total,
            name,
        } => format!("[{repo_name}] checking snapshot {current}/{total}: {name}..."),
        CheckProgressEvent::PacksExistencePhaseStarted { total_packs } => {
            format!("[{repo_name}] verifying packs (0/{total_packs})...")
        }
        CheckProgressEvent::PacksExistenceProgress {
            checked,
            total_packs,
        } => format!("[{repo_name}] verifying packs ({checked}/{total_packs})..."),
        CheckProgressEvent::ChunksDataPhaseStarted { total_chunks } => {
            format!("[{repo_name}] verifying data (0/{total_chunks})...")
        }
        CheckProgressEvent::ChunksDataProgress {
            verified,
            total_chunks,
        } => format!("[{repo_name}] verifying data ({verified}/{total_chunks})..."),
        CheckProgressEvent::ServerVerifyPhaseStarted { total_packs } => {
            format!("[{repo_name}] verifying server packs (0/{total_packs})...")
        }
        CheckProgressEvent::ServerVerifyProgress {
            verified,
            total_packs,
        } => format!("[{repo_name}] verifying server packs ({verified}/{total_packs})..."),
    }
}

/// Format a step outcome for the GUI log. Returns empty string for Ok steps
/// (backup reports are logged separately with more detail).
pub fn format_step_outcome(repo_name: &str, step: CycleStep, outcome: &StepOutcome) -> String {
    let name = step.command_name();
    match outcome {
        StepOutcome::Ok => {
            if matches!(step, CycleStep::Backup) {
                // Backup details are logged via log_backup_report
                String::new()
            } else {
                format!("[{repo_name}] {name}: ok")
            }
        }
        StepOutcome::Partial => format!("[{repo_name}] {name}: ok (partial — some files skipped)"),
        StepOutcome::Skipped(reason) => format!("[{repo_name}] {name}: skipped ({reason})"),
        StepOutcome::Failed(e) => format!("[{repo_name}] {name}: FAILED: {e}"),
    }
}

#[cfg(test)]
mod tests {
    use super::{str_display_width, truncate_middle};

    #[test]
    fn truncate_middle_shows_head_and_tail() {
        let input = "/very/long/path/to/a/file.txt";
        let out = truncate_middle(input, 16);
        assert_eq!(out, "/very/...ile.txt");
        assert_eq!(str_display_width(&out), 16);
    }

    #[test]
    fn truncate_middle_returns_original_when_short() {
        assert_eq!(truncate_middle("short.txt", 32), "short.txt");
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
        assert_eq!(truncate_middle("exactly10!", 10), "exactly10!");
    }

    #[test]
    fn truncate_middle_one_over() {
        let out = truncate_middle("abcdefghijk", 10);
        assert_eq!(out, "abc...hijk");
        assert_eq!(str_display_width(&out), 10);
    }

    #[test]
    fn truncate_middle_unicode() {
        let input = "aaaa\u{00e9}\u{00e9}\u{00e9}\u{00e9}bbbb"; // 12 chars, all width 1
        let out = truncate_middle(input, 10);
        assert_eq!(out, "aaa...bbbb");
        assert_eq!(str_display_width(&out), 10);
    }

    #[test]
    fn truncate_middle_cjk() {
        let input = "文件/路径/测试报告.pdf";
        let out = truncate_middle(input, 16);
        assert_eq!(out, "文件/...告.pdf");
        assert!(str_display_width(&out) <= 16);
    }

    #[test]
    fn truncate_middle_combining_diaeresis() {
        // This is the exact crash case: NFC-decomposed ö = o + \u{0308}
        let input = "Documents/Children Books/Das Lo\u{0308}schflugzeug Nummer 292/Das Lo\u{0308}schflugzeug Nummer 292-EN.ai";
        let out = truncate_middle(input, 60);
        assert!(str_display_width(&out) <= 60);
        assert!(out.contains("..."));
        // Must not panic — that's the main assertion
    }
}
