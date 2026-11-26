//! Local filesystem probe: `tempfile::NamedTempFile::persist_noclobber`.

use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::Barrier;

use tempfile::NamedTempFile;
use vykar_types::error::{Result, VykarError};

use super::{
    CREATE, CaseResult, Cleanup, LIST_AFTER_WRITE, Ledger, Outcome, ProbeOptions, REPEAT_OTHER,
    REPEAT_SAME, RaceResult, Report, UNSUPPORTED_DETAIL, is_created, is_exists, is_listed,
    listing_outcome, payload,
};

const BODY_LEN: usize = 1024;

/// Probe `persist_noclobber` under `root`, which must be an existing directory.
/// Errors only before anything is written (root missing, root holds `config` without
/// `allow_repo_root`, prefix directory already exists).
pub fn run_local(root: &Path, opts: &ProbeOptions) -> Result<Report> {
    run_with_ledger(root, opts, Ledger::for_run(opts)?)
}

fn run_with_ledger(root: &Path, opts: &ProbeOptions, mut ledger: Ledger) -> Result<Report> {
    let root = std::fs::canonicalize(root)
        .map_err(|e| VykarError::Config(format!("probe root {}: {e}", root.display())))?;
    if !root.is_dir() {
        return Err(VykarError::Config(format!(
            "probe root {} is not a directory",
            root.display()
        )));
    }
    let has_config = root
        .join("config")
        .try_exists()
        .map_err(|e| VykarError::Config(format!("probe root {}: {e}", root.display())))?;
    if has_config && !opts.allow_repo_root {
        return Err(VykarError::Config(format!(
            "probe root {} holds a 'config' (a repository?); pass --allow-repo-root to probe it",
            root.display()
        )));
    }
    let prefix = ledger.prefix().to_string();
    // Non-recursive: fails if the name exists, so the prefix is the probe's own.
    std::fs::create_dir(root.join(&prefix)).map_err(|e| {
        VykarError::Config(format!(
            "cannot create probe directory {}: {e}",
            root.join(&prefix).display()
        ))
    })?;
    ledger.record_dir(prefix);

    let (cases, race, notes) = run_cases(&root, opts, &mut ledger);
    let cleanup = cleanup(&root, &ledger);
    Ok(Report::finish(
        opts,
        "local",
        "fs".into(),
        "persist_noclobber",
        root.display().to_string(),
        &ledger,
        cases,
        race,
        notes,
        cleanup,
    ))
}

fn path_of(root: &Path, key: &str) -> PathBuf {
    key.split('/').fold(root.to_path_buf(), |p, c| p.join(c))
}

fn describe(e: &std::io::Error) -> String {
    match e.raw_os_error() {
        Some(n) => format!("{:?} (os error {n})", e.kind()),
        None => format!("{:?}: {e}", e.kind()),
    }
}

fn classify(e: &std::io::Error) -> Outcome {
    if e.kind() == std::io::ErrorKind::AlreadyExists {
        return Outcome::AlreadyExists {
            detail: describe(e),
        };
    }
    let detail = describe(e);
    if vykar_common::fs::is_no_clobber_unsupported(e) {
        Outcome::Rejected {
            detail: format!("{UNSUPPORTED_DETAIL}{detail}"),
        }
    } else {
        Outcome::Rejected { detail }
    }
}

/// A recorded temp file in `dir` holding `data`.
fn temp_with(
    dir: &Path,
    ledger: &mut Ledger,
    data: &[u8],
) -> std::result::Result<NamedTempFile, Outcome> {
    let reject = |what: &str, e: &std::io::Error| Outcome::Rejected {
        detail: format!("temp {what}: {}", describe(e)),
    };
    let mut tmp = tempfile::Builder::new()
        .prefix(vykar_protocol::TEMP_FILE_PREFIX)
        .rand_bytes(16)
        .tempfile_in(dir)
        .map_err(|e| reject("create", &e))?;
    let name = tmp
        .path()
        .file_name()
        .map(|n| n.to_string_lossy().into_owned())
        .unwrap_or_default();
    ledger.record_temp(&name).map_err(|e| Outcome::Rejected {
        detail: format!("temp name: {e}"),
    })?;
    tmp.write_all(data).map_err(|e| reject("write", &e))?;
    tmp.as_file().sync_all().map_err(|e| reject("sync", &e))?;
    Ok(tmp)
}

fn persist(tmp: NamedTempFile, target: &Path) -> Outcome {
    match tmp.persist_noclobber(target) {
        Ok(_) => Outcome::Created,
        // Dropping the returned temp file removes it.
        Err(e) => classify(&e.error),
    }
}

fn create_once(dir: &Path, ledger: &mut Ledger, target: &Path, data: &[u8]) -> Outcome {
    match temp_with(dir, ledger, data) {
        Ok(tmp) => persist(tmp, target),
        Err(o) => o,
    }
}

/// `Some(bytes == want)`, or `None` with a note if the file cannot be read.
fn content_is(target: &Path, want: &[u8], notes: &mut Vec<String>) -> Option<bool> {
    match std::fs::read(target) {
        Ok(got) => Some(got == want),
        Err(e) => {
            notes.push(format!("read {}: {}", target.display(), describe(&e)));
            None
        }
    }
}

/// Every case and the race. No early return between the first write and the
/// caller's cleanup: every failure becomes an [`Outcome`].
fn run_cases(
    root: &Path,
    opts: &ProbeOptions,
    ledger: &mut Ledger,
) -> (Vec<CaseResult>, Option<RaceResult>, Vec<String>) {
    let prefix = ledger.prefix().to_string();
    let dir = root.join(&prefix);
    let mut notes = Vec::new();
    let mut cases = Vec::new();
    let a = payload("A", &prefix, BODY_LEN);
    let b = payload("B", &prefix, BODY_LEN);

    let target = match ledger.key("k") {
        Ok(k) => path_of(root, &k),
        Err(e) => {
            notes.push(e.to_string());
            return (cases, None, notes);
        }
    };

    let created = create_once(&dir, ledger, &target, &a);
    let content = is_created(&created).then(|| content_is(&target, &a, &mut notes));
    let create = CaseResult::expect(
        CREATE,
        "created",
        is_created,
        created,
        content.flatten(),
        true,
    );
    let create_ok = create.ok;
    cases.push(create);

    // Repeats are judged against the bytes of create, so they need it fully ok.
    if create_ok {
        for (name, data) in [(REPEAT_SAME, &a), (REPEAT_OTHER, &b)] {
            let outcome = create_once(&dir, ledger, &target, data);
            let content = content_is(&target, &a, &mut notes);
            cases.push(CaseResult::expect(
                name,
                "already exists, bytes of create kept",
                is_exists,
                outcome,
                content,
                true,
            ));
        }
    } else {
        for name in [REPEAT_SAME, REPEAT_OTHER] {
            cases.push(CaseResult::skipped(
                name,
                "already exists, bytes of create kept",
                "create was not ok",
            ));
        }
    }

    cases.push(list_after_write(root, &dir, opts, ledger));

    let race = race(root, &dir, opts, ledger, &prefix, &mut notes);

    let mut note = format!("os {}", std::env::consts::OS);
    if cfg!(any(target_os = "linux", target_vendor = "apple")) {
        note.push_str(", tempfile tries renameat_with(NOREPLACE) first");
    }
    cases.push(CaseResult::info(
        "fs_note",
        Outcome::Info { detail: note },
        None,
    ));
    (cases, Some(race), notes)
}

/// Plain writes of fresh keys, each followed by a fresh `read_dir` of the
/// prefix directory, which must show it. A local filesystem has no second
/// handle beyond that new directory read.
fn list_after_write(
    root: &Path,
    dir: &Path,
    opts: &ProbeOptions,
    ledger: &mut Ledger,
) -> CaseResult {
    let n = opts.list_rounds();
    let result =
        |outcome| CaseResult::expect(LIST_AFTER_WRITE, "listed", is_listed, outcome, None, false);
    let mut keys = Vec::with_capacity(n);
    for r in 0..n {
        match ledger.key(&format!("list-{r}")) {
            Ok(k) => keys.push(k),
            Err(e) => {
                return result(Outcome::Rejected {
                    detail: e.to_string(),
                });
            }
        }
    }
    let mut missed = 0;
    for key in &keys {
        let path = path_of(root, key);
        if let Err(e) = std::fs::write(&path, b"l") {
            return result(Outcome::Rejected {
                detail: format!("write: {}", describe(&e)),
            });
        }
        let name = path.file_name().map(std::ffi::OsStr::to_os_string);
        let listed = match std::fs::read_dir(dir) {
            Ok(entries) => entries.flatten().any(|e| Some(e.file_name()) == name),
            Err(e) => {
                return result(Outcome::Rejected {
                    detail: format!("list: {}", describe(&e)),
                });
            }
        };
        if !listed {
            missed += 1;
        }
    }
    result(listing_outcome(missed, n))
}

fn race(
    root: &Path,
    dir: &Path,
    opts: &ProbeOptions,
    ledger: &mut Ledger,
    prefix: &str,
    notes: &mut Vec<String>,
) -> RaceResult {
    let writers = opts.writers();
    let mut result = RaceResult::new(writers);
    for r in 0..opts.rounds() {
        let target = match ledger.key(&format!("race-{r}")) {
            Ok(k) => path_of(root, &k),
            Err(e) => {
                notes.push(e.to_string());
                break;
            }
        };
        let payloads: Vec<Vec<u8>> = (0..writers)
            .map(|i| payload(&format!("r{r}w{i}"), prefix, BODY_LEN))
            .collect();
        let temps: Vec<std::result::Result<NamedTempFile, Outcome>> =
            payloads.iter().map(|p| temp_with(dir, ledger, p)).collect();
        let ready = temps.iter().filter(|t| t.is_ok()).count();
        let barrier = Barrier::new(ready.max(1));
        let outcomes: Vec<Outcome> = std::thread::scope(|s| {
            let handles: Vec<_> = temps
                .into_iter()
                .map(|t| match t {
                    Ok(tmp) => {
                        let barrier = &barrier;
                        let target = &target;
                        Ok(s.spawn(move || {
                            barrier.wait();
                            persist(tmp, target)
                        }))
                    }
                    Err(o) => Err(o),
                })
                .collect();
            handles
                .into_iter()
                .map(|h| match h {
                    Ok(h) => h.join().unwrap_or_else(|_| Outcome::Transport {
                        detail: "writer thread panicked".into(),
                    }),
                    Err(o) => o,
                })
                .collect()
        });
        if let Some(w) = result.add_round(&outcomes)
            && let Some(want) = payloads.get(w)
            && content_is(&target, want, notes) == Some(true)
        {
            result.content_matches_winner += 1;
        }
    }
    result
}

/// Remove every recorded key and temp, then the recorded directories,
/// non-recursively. Whatever is still in the prefix directory is listed, not removed.
fn cleanup(root: &Path, ledger: &Ledger) -> Cleanup {
    let mut out = Cleanup::default();
    for key in ledger.keys().iter().chain(ledger.temps()) {
        let path = path_of(root, key);
        match std::fs::remove_file(&path) {
            Ok(()) => out.removed += 1,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => out
                .errors
                .push(format!("remove {}: {}", path.display(), describe(&e))),
        }
    }
    for dir in ledger.dirs().iter().rev() {
        let path = path_of(root, dir);
        match std::fs::remove_dir(&path) {
            Ok(()) => out.removed += 1,
            Err(e)
                if matches!(
                    e.kind(),
                    std::io::ErrorKind::NotFound | std::io::ErrorKind::DirectoryNotEmpty
                ) => {}
            Err(e) => out
                .errors
                .push(format!("rmdir {}: {}", path.display(), describe(&e))),
        }
    }
    let prefix_dir = root.join(ledger.prefix());
    if prefix_dir.exists() {
        match std::fs::read_dir(&prefix_dir) {
            Ok(entries) => {
                for entry in entries.flatten() {
                    out.leftovers.push(format!(
                        "{}/{}",
                        ledger.prefix(),
                        entry.file_name().to_string_lossy()
                    ));
                }
                if out.leftovers.is_empty() {
                    out.leftovers.push(ledger.prefix().to_string());
                }
            }
            Err(e) => out
                .errors
                .push(format!("list {}: {}", prefix_dir.display(), describe(&e))),
        }
    }
    out.leftovers.sort();
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::probe::{PROBE_PREFIX, Verdict};

    fn opts() -> ProbeOptions {
        ProbeOptions {
            race_writers: 4,
            race_rounds: 3,
            ..ProbeOptions::default()
        }
    }

    fn probe_entries(dir: &Path) -> Vec<String> {
        std::fs::read_dir(dir)
            .unwrap()
            .map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
            .filter(|n| n.starts_with(PROBE_PREFIX))
            .collect()
    }

    #[test]
    fn local_probe_works_on_tempdir() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("keep.txt"), b"keep").unwrap();
        let report = run_local(dir.path(), &opts()).unwrap();
        assert_eq!(report.verdict, Verdict::Works, "{report:#?}");
        let list = report
            .cases
            .iter()
            .find(|c| c.name == LIST_AFTER_WRITE)
            .unwrap();
        assert!(is_listed(&list.outcome), "{list:?}");
        assert!(report.cleanup.leftovers.is_empty(), "{:?}", report.cleanup);
        assert!(report.cleanup.errors.is_empty(), "{:?}", report.cleanup);
        assert_eq!(report.exit_code(), 0);
        assert!(probe_entries(dir.path()).is_empty());
        assert_eq!(std::fs::read(dir.path().join("keep.txt")).unwrap(), b"keep");
        let race = report.race.unwrap();
        assert_eq!(race.rounds, 3);
        assert_eq!(race.writers, 4);
    }

    #[test]
    fn local_probe_refuses_repo_root() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("config"), b"repo").unwrap();
        assert!(run_local(dir.path(), &opts()).is_err());
        assert!(probe_entries(dir.path()).is_empty());
        let allowed = ProbeOptions {
            allow_repo_root: true,
            ..opts()
        };
        let report = run_local(dir.path(), &allowed).unwrap();
        assert_eq!(report.verdict, Verdict::Works);
        assert_eq!(std::fs::read(dir.path().join("config")).unwrap(), b"repo");
    }

    #[test]
    fn local_probe_refuses_existing_prefix() {
        let dir = tempfile::tempdir().unwrap();
        let fixed = dir.path().join("vykar-probe-fixed");
        std::fs::create_dir(&fixed).unwrap();
        std::fs::write(fixed.join("k"), b"theirs").unwrap();
        let ledger = Ledger::with_prefix("vykar-probe-fixed");
        assert!(run_with_ledger(dir.path(), &opts(), ledger).is_err());
        assert_eq!(std::fs::read(fixed.join("k")).unwrap(), b"theirs");
    }

    #[test]
    fn local_cleanup_leaves_foreign_file() {
        let dir = tempfile::tempdir().unwrap();
        let mut ledger = Ledger::with_prefix("vykar-probe-mixed");
        let prefix_dir = dir.path().join("vykar-probe-mixed");
        std::fs::create_dir(&prefix_dir).unwrap();
        ledger.record_dir("vykar-probe-mixed".into());
        let ours = ledger.key("k").unwrap();
        std::fs::write(path_of(dir.path(), &ours), b"ours").unwrap();
        std::fs::write(prefix_dir.join("foreign"), b"theirs").unwrap();

        let cleanup = cleanup(dir.path(), &ledger);
        assert!(!prefix_dir.join("k").exists());
        assert_eq!(
            std::fs::read(prefix_dir.join("foreign")).unwrap(),
            b"theirs"
        );
        assert_eq!(cleanup.leftovers, ["vykar-probe-mixed/foreign".to_string()]);
        assert!(cleanup.errors.is_empty(), "{:?}", cleanup.errors);
    }
}
