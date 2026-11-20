//! Conditional GET of the repository `index` (`If-None-Match` -> 304).
//!
//! The digest is the BLAKE3 the server verified on the last `PUT /index`
//! (`X-Content-BLAKE3`, which REST clients send for every non-pack object to
//! a server that advertises `blake3`). A BLAKE2b-verified PUT, from an older
//! client, leaves no validator. It is held in memory
//! only, and bound to the identity of the file that PUT produced: the path is
//! stat'ed right after the rename, under the lock that also covers every
//! other server write of `index`. So an `index` replaced any other way never
//! matches.

/// Identity of one file version. The server only ever replaces `index` by
/// renaming a fresh temp file over it, so on Unix every version has its own
/// inode. Elsewhere, length and mtime have to do.
///
/// Not ctime (rename updates it on Linux), nor creation time (NTFS name
/// tunnelling can carry it over on rename-over).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct FileFingerprint {
    len: u64,
    modified: std::time::SystemTime,
    #[cfg(unix)]
    dev: u64,
    #[cfg(unix)]
    ino: u64,
}

impl FileFingerprint {
    /// `None` when the platform reports no modification time.
    pub(crate) fn of(meta: &std::fs::Metadata) -> Option<Self> {
        Some(Self {
            len: meta.len(),
            modified: meta.modified().ok()?,
            #[cfg(unix)]
            dev: std::os::unix::fs::MetadataExt::dev(meta),
            #[cfg(unix)]
            ino: std::os::unix::fs::MetadataExt::ino(meta),
        })
    }
}

#[derive(Clone, Debug)]
pub(crate) struct IndexValidator {
    /// 64 lowercase hex characters.
    pub(crate) digest_hex: String,
    pub(crate) fingerprint: FileFingerprint,
}

/// `key` names the repository `index` (leading/trailing slashes resolve to
/// the same file, see `AppState::file_path`).
pub(crate) fn is_index_key(key: &str) -> bool {
    key.trim_matches('/') == "index"
}

/// Whether an `If-None-Match` value names `digest_hex`: a comma-separated
/// list of entity tags, weak comparison (RFC 9110 §13.1.2), so `W/` is
/// ignored. `*` is not honoured.
pub(crate) fn if_none_match_hits(header: &str, digest_hex: &str) -> bool {
    header
        .split(',')
        .map(str::trim)
        .filter_map(|tag| {
            let tag = tag.trim_prefix("W/");
            tag.strip_prefix('"')?.strip_suffix('"')
        })
        .any(|opaque| opaque.eq_ignore_ascii_case(digest_hex))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn if_none_match_hits_named_tags() {
        let h = "ab12";
        assert!(if_none_match_hits("\"ab12\"", h));
        assert!(if_none_match_hits("W/\"ab12\"", h));
        assert!(if_none_match_hits("\"x\", \"ab12\"", h));
        assert!(if_none_match_hits("\"AB12\"", h));
    }

    #[test]
    fn if_none_match_misses_everything_else() {
        let h = "ab12";
        assert!(!if_none_match_hits("*", h));
        assert!(!if_none_match_hits("ab12", h));
        assert!(!if_none_match_hits("", h));
        assert!(!if_none_match_hits("\"\"", h));
        assert!(!if_none_match_hits("\"ab13\"", h));
    }

    #[test]
    fn is_index_key_ignores_slashes() {
        assert!(is_index_key("index"));
        assert!(is_index_key("/index/"));
        assert!(!is_index_key("index.gen"));
        assert!(!is_index_key("snapshots/index"));
    }
}
