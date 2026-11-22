//! The lock, session and maintenance code reads time and host identity only
//! through `HostEnv`. A text scan: it misses aliases, and is a tripwire, not
//! a proof.

const FILES: &[(&str, &str)] = &[
    ("repo/lock.rs", include_str!("../repo/lock.rs")),
    (
        "repo/journal_recovery.rs",
        include_str!("../repo/journal_recovery.rs"),
    ),
    ("commands/util.rs", include_str!("../commands/util.rs")),
    (
        "commands/backup/phases.rs",
        include_str!("../commands/backup/phases.rs"),
    ),
    (
        "commands/backup/mod.rs",
        include_str!("../commands/backup/mod.rs"),
    ),
    ("commands/prune.rs", include_str!("../commands/prune.rs")),
];

const FORBIDDEN: &[&str] = &[
    "Utc::now",
    "SystemTime::now",
    "Instant::now",
    "thread::sleep",
    "platform::hostname",
    "platform::short_hostname",
    "platform::boot_id",
    "platform::is_pid_alive",
    "process::id",
];

/// The lines before the inline test module (`#[cfg(test)]` directly
/// followed by `mod tests`), with their 1-based numbers.
fn non_test_lines(src: &str) -> Vec<(usize, &str)> {
    let lines: Vec<&str> = src.lines().collect();
    let end = lines
        .windows(2)
        .position(|w| w[0].trim() == "#[cfg(test)]" && w[1].trim_start().starts_with("mod tests"))
        .unwrap_or(lines.len());
    lines[..end]
        .iter()
        .enumerate()
        .map(|(i, l)| (i + 1, *l))
        .collect()
}

#[test]
fn protocol_code_reads_time_and_identity_only_through_host_env() {
    let mut hits = Vec::new();
    for (name, src) in FILES {
        for (n, line) in non_test_lines(src) {
            if line.trim_start().starts_with("//") {
                continue;
            }
            if FORBIDDEN.iter().any(|f| line.contains(f)) {
                hits.push(format!("{name}:{n}: {}", line.trim()));
            }
        }
    }
    assert!(hits.is_empty(), "direct reads:\n{}", hits.join("\n"));
}

#[test]
fn scan_sees_host_env_reads() {
    // The scan would pass vacuously on empty input.
    assert!(include_str!("../repo/host_env.rs").contains("Instant::now"));
    for (name, src) in FILES {
        assert!(!non_test_lines(src).is_empty(), "{name} scanned empty");
    }
}
