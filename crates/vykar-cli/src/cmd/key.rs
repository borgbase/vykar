//! `vykar key export` and `vykar key import`: the armor format, file I/O and
//! reporting. Whether a key may be exported or imported is decided in
//! `vykar_core::commands::key`.

use std::io::{IsTerminal, Read, Write};
use std::path::{Path, PathBuf};

use base64::Engine;
use vykar_core::commands::key::{self, CopyOutcome, ImportReport, KeyExport};
use vykar_core::config::VykarConfig;
use vykar_core::repo::identity::KeyProof;
use vykar_core::repo::{KeyCopyState, KEY_PRIMARY, KEY_SECONDARY};

use crate::error::{CliError, CliResult};
use crate::passphrase::with_repo_passphrase;

const ARMOR_BEGIN: &str = "-----BEGIN VYKAR REPOSITORY KEY-----";
const ARMOR_END: &str = "-----END VYKAR REPOSITORY KEY-----";
const ARMOR_HEADER: &str = "repository:";
const ARMOR_WRAP: usize = 64;

const B64: base64::engine::general_purpose::GeneralPurpose =
    base64::engine::general_purpose::STANDARD;

fn encode_armor(export: &KeyExport) -> String {
    let body = B64.encode(&export.blob);
    let mut out = String::with_capacity(body.len() + 160);
    out.push_str(ARMOR_BEGIN);
    out.push('\n');
    out.push_str(&format!(
        "{ARMOR_HEADER} {}\n",
        hex::encode(&export.repo_id)
    ));
    for line in body.as_bytes().chunks(ARMOR_WRAP) {
        out.push_str(&String::from_utf8_lossy(line));
        out.push('\n');
    }
    out.push_str(ARMOR_END);
    out.push('\n');
    out
}

/// Parse one armored block. Well-formedness of the blob itself is checked by
/// core when the key is used, not here.
pub(crate) fn parse_armor(text: &str) -> CliResult<KeyExport> {
    let mut repo_id: Option<Vec<u8>> = None;
    let mut body = String::new();
    let mut inside = false;
    let mut saw_end = false;

    for line in text.lines() {
        let line = line.trim();
        if line == ARMOR_BEGIN {
            inside = true;
            continue;
        }
        if line == ARMOR_END {
            saw_end = true;
            break;
        }
        if !inside || line.is_empty() {
            continue;
        }
        if let Some(value) = line.strip_prefix(ARMOR_HEADER) {
            repo_id = Some(
                hex::decode(value.trim())
                    .map_err(|e| CliError::from(format!("malformed repository header: {e}")))?,
            );
            continue;
        }
        body.push_str(line);
    }

    if !inside || !saw_end {
        return Err(CliError::from(
            "not a vykar key export: missing the BEGIN/END armor lines",
        ));
    }
    let blob = B64
        .decode(body.as_bytes())
        .map_err(|e| CliError::from(format!("malformed key export: {e}")))?;
    let Some(repo_id) = repo_id else {
        return Err(CliError::from(
            "not a vykar key export: missing the `repository:` header",
        ));
    };
    Ok(KeyExport { repo_id, blob })
}

pub(crate) fn run_key_export(
    config: &VykarConfig,
    label: Option<&str>,
    output: Option<&str>,
) -> CliResult<()> {
    let exported = with_repo_passphrase(config, label, |passphrase| {
        Ok(key::export(config, passphrase)?)
    })?;

    eprintln!("{}", describe_selection(exported.copies));

    let armor = encode_armor(&exported.export);
    match output {
        // The armor is the *result*, so it goes to stdout; status goes to
        // stderr, per the output-split convention.
        None => print!("{armor}"),
        Some(path) => {
            write_export_file(Path::new(path), &armor)?;
            eprintln!("Wrote repository key export to {path}");
        }
    }
    eprintln!(
        "Store this alongside the repository passphrase. Both are required, and neither \
         can be recovered from the other."
    );
    Ok(())
}

/// Say which copy the export came from, and flag anything the operator should
/// act on before trusting this repository's redundancy.
fn describe_selection(state: KeyCopyState) -> String {
    match state {
        KeyCopyState::Matched => format!(
            "Verified: {KEY_PRIMARY} and {KEY_SECONDARY} match and unwrap with this passphrase."
        ),
        KeyCopyState::OneMissing { missing } => format!(
            "Verified, but {missing} is missing or unreadable — this repository currently \
             holds one usable key copy. Run `vykar check` to see which, and \
             `vykar check --repair` to restore it if it is genuinely absent."
        ),
        KeyCopyState::Divergent { good, bad } => format!(
            "Verified {good}; exported from it. {bad} disagrees with it — run \
             `vykar check --repair` to rewrite {bad}."
        ),
        KeyCopyState::NotApplicable => "Verified.".to_string(),
    }
}

#[cfg(unix)]
fn write_export_file(path: &Path, armor: &str) -> CliResult<()> {
    use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};

    // `.mode()` only applies at *creation*, so exporting over an existing
    // world-readable file would silently leave it that way. Tighten the open
    // file descriptor before a single byte is written: the file is truncated
    // and still empty at this point, so nothing is ever readable at the old
    // permissions.
    let mut file = std::fs::OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .mode(0o600)
        .open(path)?;
    file.set_permissions(std::fs::Permissions::from_mode(0o600))?;
    file.write_all(armor.as_bytes())?;
    Ok(())
}

#[cfg(not(unix))]
fn write_export_file(path: &Path, armor: &str) -> CliResult<()> {
    std::fs::write(path, armor)?;
    Ok(())
}

pub(crate) fn run_key_import(
    config: &VykarConfig,
    label: Option<&str>,
    source: &str,
    force: bool,
) -> CliResult<()> {
    let export = parse_armor(&read_source(source)?)?;

    let report = with_repo_passphrase(config, label, |passphrase| {
        Ok(key::import(config, passphrase, &export, force)?)
    })?;

    if report.proof == KeyProof::Unproven {
        eprintln!(
            "Warning: importing an unverified key. Nothing here proves it belongs to this \
             repository; if it is the wrong key, the repository will stay permanently \
             unreadable."
        );
    }
    report_import(&report)?;
    if report.repinned {
        eprintln!(
            "Re-pinned the repository identity for '{}'.",
            config.repository.url
        );
    }
    Ok(())
}

/// Report per-copy outcomes. Exits non-zero only if *no* copy ended up
/// correct.
fn report_import(report: &ImportReport) -> CliResult<()> {
    for (storage_key, outcome) in &report.outcomes {
        match outcome {
            CopyOutcome::AlreadyCorrect => {
                println!("{storage_key}: already correct, left untouched");
            }
            CopyOutcome::Created => println!("{storage_key}: created"),
            CopyOutcome::Replaced => println!("{storage_key}: replaced"),
            CopyOutcome::Refused(reason) => println!("{storage_key}: not written — {reason}"),
        }
    }
    if !report.restored() {
        return Err(CliError::from(
            "no key copy could be written; the repository key was not restored",
        ));
    }
    if report.partial() {
        eprintln!(
            "Partial success: the repository key is in place, but not every copy could be \
             written."
        );
    }
    Ok(())
}

fn read_source(source: &str) -> CliResult<String> {
    if source == "-" {
        if std::io::stdin().is_terminal() {
            eprintln!("Reading key export from stdin; end with Ctrl-D.");
        }
        let mut buf = String::new();
        std::io::stdin().read_to_string(&mut buf)?;
        return Ok(buf);
    }
    let path = PathBuf::from(source);
    std::fs::read_to_string(&path)
        .map_err(|e| CliError::from(format!("could not read {}: {e}", path.display())))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_export() -> KeyExport {
        KeyExport {
            repo_id: vec![0xABu8; 32],
            blob: (0u8..=255).cycle().take(180).collect(),
        }
    }

    #[test]
    fn armor_round_trips() {
        let export = sample_export();
        let armor = encode_armor(&export);
        assert_eq!(parse_armor(&armor).unwrap(), export);
    }

    #[test]
    fn armor_wraps_the_body() {
        let armor = encode_armor(&sample_export());
        let body: Vec<&str> = armor
            .lines()
            .filter(|l| !l.starts_with("-----") && !l.starts_with(ARMOR_HEADER))
            .collect();
        assert!(body.len() > 1, "body should wrap onto several lines");
        for line in body {
            assert!(line.len() <= ARMOR_WRAP, "line too long: {line}");
        }
    }

    #[test]
    fn armor_survives_crlf_and_stray_blank_lines() {
        let armor = encode_armor(&sample_export());
        let mangled = armor
            .replace('\n', "\r\n")
            .replace(ARMOR_END, &format!("\r\n{ARMOR_END}"));
        assert!(parse_armor(&mangled).is_ok());
    }

    #[test]
    fn missing_armor_lines_are_rejected() {
        let blob = B64.encode(sample_export().blob);
        let err = parse_armor(&format!("repository: aabb\n{blob}\n")).unwrap_err();
        assert!(err.to_string().contains("armor"), "{err}");
    }

    #[test]
    fn missing_repository_header_is_rejected() {
        let blob = B64.encode(sample_export().blob);
        let text = format!("{ARMOR_BEGIN}\n{blob}\n{ARMOR_END}\n");
        let err = parse_armor(&text).unwrap_err();
        assert!(err.to_string().contains("repository:"), "{err}");
    }

    #[test]
    fn non_base64_body_is_rejected() {
        let text = format!("{ARMOR_BEGIN}\nrepository: aabb\n!!not base64!!\n{ARMOR_END}\n");
        let err = parse_armor(&text).unwrap_err();
        assert!(err.to_string().contains("malformed key export"), "{err}");
    }

    /// The header is a paste check, not authentication — it is plain editable
    /// text and core's `prove_key` is what actually authorizes an import.
    #[test]
    fn repo_id_header_is_carried_verbatim() {
        let export = KeyExport {
            repo_id: vec![0x01, 0x02, 0x03],
            blob: vec![0u8; 100],
        };
        let armor = encode_armor(&export);
        assert!(armor.contains("repository: 010203"));
        assert_eq!(parse_armor(&armor).unwrap().repo_id, vec![1, 2, 3]);
    }
}
