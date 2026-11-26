//! The record, in this machine's per-repository cache dir, that the last
//! compact here refused its mark because a live snapshot needs a chunk the
//! index lacks (`VykarError::MarkRefused` of
//! `VykarError::LiveChunkNotInIndex`; other causes are not recorded). Every
//! compact refuses until the cause is repaired, and the daemon and GUI
//! cycles compact each run, so `vykar info` and the GUI show it until a
//! compact marks again or `check --repair` applies. Local only: another
//! machine does not see it. Best effort: failures are logged.

use std::path::Path;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

const FILE: &str = "compact_refused.json";

/// A compact on this machine refused its mark, and none has marked since.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CompactRefusal {
    /// The first refusal since the record was last cleared.
    pub first: DateTime<Utc>,
    /// The latest refusal.
    pub last: DateTime<Utc>,
    /// The latest refusal's error text (it carries the advice).
    pub detail: String,
}

/// Record a refusal now: `first` is kept from an existing record.
pub(crate) fn record(dir: Option<&Path>, detail: &str) {
    let Some(dir) = dir else {
        return;
    };
    let now = Utc::now();
    let first = read(Some(dir)).map_or(now, |r| r.first);
    let refusal = CompactRefusal {
        first,
        last: now,
        detail: detail.to_string(),
    };
    if let Err(e) = std::fs::create_dir_all(dir) {
        tracing::warn!("could not create cache directory {}: {e}", dir.display());
        return;
    }
    let json = match serde_json::to_vec_pretty(&refusal) {
        Ok(j) => j,
        Err(e) => {
            tracing::warn!("could not serialize the compact refusal record: {e}");
            return;
        }
    };
    let path = dir.join(FILE);
    if let Err(e) = crate::repo::file_cache::atomic_write(&path, &json) {
        tracing::warn!(
            "could not write the compact refusal record to {}: {e}",
            path.display()
        );
    }
}

/// Remove the record (absent is fine).
pub(crate) fn clear(dir: Option<&Path>) {
    let Some(dir) = dir else {
        return;
    };
    let path = dir.join(FILE);
    match std::fs::remove_file(&path) {
        Ok(()) => {}
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
        Err(e) => tracing::warn!(
            "could not remove the compact refusal record {}: {e}",
            path.display()
        ),
    }
}

/// The record, if present and readable (a corrupt file reads as none).
pub fn read(dir: Option<&Path>) -> Option<CompactRefusal> {
    let bytes = std::fs::read(dir?.join(FILE)).ok()?;
    serde_json::from_slice(&bytes).ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_record_reads_back() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path().join("repo");
        assert_eq!(read(Some(&dir)), None);
        record(Some(&dir), "refused: x");
        let r = read(Some(&dir)).unwrap();
        assert_eq!(r.detail, "refused: x");
        assert_eq!(r.first, r.last);
    }

    #[test]
    fn a_second_record_keeps_the_first_time() {
        let tmp = tempfile::tempdir().unwrap();
        record(Some(tmp.path()), "one");
        let one = read(Some(tmp.path())).unwrap();
        std::thread::sleep(std::time::Duration::from_millis(5));
        record(Some(tmp.path()), "two");
        let two = read(Some(tmp.path())).unwrap();
        assert_eq!(two.first, one.first);
        assert!(two.last > one.last);
        assert_eq!(two.detail, "two");
    }

    #[test]
    fn clear_removes_the_record() {
        let tmp = tempfile::tempdir().unwrap();
        clear(Some(tmp.path()));
        record(Some(tmp.path()), "x");
        clear(Some(tmp.path()));
        assert_eq!(read(Some(tmp.path())), None);
        assert!(!tmp.path().join(FILE).exists());
    }

    #[test]
    fn a_corrupt_record_reads_as_none() {
        let tmp = tempfile::tempdir().unwrap();
        std::fs::write(tmp.path().join(FILE), b"{not json").unwrap();
        assert_eq!(read(Some(tmp.path())), None);
    }

    #[test]
    fn no_cache_dir_is_a_no_op() {
        record(None, "x");
        clear(None);
        assert_eq!(read(None), None);
    }
}
