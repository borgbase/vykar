//! The backup commit's and the index write's storage writes are issued by
//! their steppable machines (`repo/commit_machine.rs`,
//! `repo/publish_machine.rs`), not by `repo/commit.rs` or
//! `repo/index_write.rs` around them, so every write of the commit window
//! is a machine step the simulator's driver can schedule (08 step 4). A
//! text scan: a tripwire, not a proof.

const SCANNED: &[(&str, &str)] = &[
    ("repo/commit.rs", include_str!("../repo/commit.rs")),
    (
        "repo/index_write.rs",
        include_str!("../repo/index_write.rs"),
    ),
];

const MACHINES: &[(&str, &str)] = &[
    (
        "repo/commit_machine.rs",
        include_str!("../repo/commit_machine.rs"),
    ),
    (
        "repo/publish_machine.rs",
        include_str!("../repo/publish_machine.rs"),
    ),
];

const WRITES: &[&str] = &[
    ".put(",
    ".put_owned(",
    ".delete(",
    "put_head(",
    "delete_keys(",
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
fn commit_and_index_write_issue_no_writes_of_their_own() {
    let mut hits = Vec::new();
    for (name, src) in SCANNED {
        for (n, line) in non_test_lines(src) {
            if line.trim_start().starts_with("//") {
                continue;
            }
            if WRITES.iter().any(|w| line.contains(w)) {
                hits.push(format!("{name}:{n}: {}", line.trim()));
            }
        }
    }
    assert!(
        hits.is_empty(),
        "writes outside the machines:\n{}",
        hits.join("\n")
    );
}

#[test]
fn scan_sees_the_machines_writes() {
    // The scan would pass vacuously on input that never writes.
    for (name, src) in MACHINES {
        assert!(
            non_test_lines(src).iter().any(|(_, l)| l.contains(".put(")),
            "{name} has no .put("
        );
    }
    for (name, src) in SCANNED {
        assert!(!non_test_lines(src).is_empty(), "{name} scanned empty");
    }
}
